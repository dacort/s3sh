use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::sync::Arc;

use super::{Command, ShellState};
use crate::archive::tar::TarHandler;
use crate::archive::zip::ZipHandler;
use crate::archive::ArchiveHandler;
use crate::vfs::{ArchiveType, VfsNode, VirtualPath};

pub struct CdCommand;

#[async_trait]
impl Command for CdCommand {
    fn name(&self) -> &str {
        "cd"
    }

    fn usage(&self) -> &str {
        "cd PATH - Change current directory"
    }

    async fn execute(&self, state: &mut ShellState, args: &[String]) -> Result<()> {
        if args.is_empty() {
            // cd with no args goes to root
            state.set_current_node(VfsNode::Root);
            return Ok(());
        }

        let path_str = &args[0];

        // Handle absolute vs relative paths
        let mut current = if path_str.starts_with('/') {
            // Start from root for absolute paths
            VfsNode::Root
        } else {
            // Start from current location for relative paths
            state.current_node().clone()
        };

        // Split path into segments and process each one
        let segments: Vec<&str> = path_str
            .trim_matches('/')
            .split('/')
            .filter(|s| !s.is_empty() && *s != ".")
            .collect();

        // Process each segment
        for segment in segments {
            current = if segment == ".." {
                // Go up one level
                self.navigate_up(&current)?
            } else {
                // Navigate to this segment
                self.navigate_to_segment(state, &current, segment).await?
            };
        }

        // Handle the special case of just "/"
        if path_str == "/" {
            current = VfsNode::Root;
        }

        // Verify the target is navigable
        if !current.is_navigable() {
            return Err(anyhow!("Not a directory: {}", path_str));
        }

        state.set_current_node(current);
        Ok(())
    }
}

impl CdCommand {
    /// Navigate up one level from the current node
    fn navigate_up(&self, current: &VfsNode) -> Result<VfsNode> {
        match current {
            VfsNode::Root => Ok(VfsNode::Root),

            VfsNode::Bucket { .. } => Ok(VfsNode::Root),

            VfsNode::Prefix { bucket, prefix } => {
                // Remove the last segment
                if prefix.trim_end_matches('/').contains('/') {
                    let parent_prefix = prefix
                        .trim_end_matches('/')
                        .rsplitn(2, '/')
                        .nth(1)
                        .unwrap();
                    Ok(VfsNode::Prefix {
                        bucket: bucket.clone(),
                        prefix: format!("{}/", parent_prefix),
                    })
                } else {
                    // At top of bucket
                    Ok(VfsNode::Bucket {
                        name: bucket.clone(),
                    })
                }
            }

            VfsNode::Object { bucket, key, .. } => {
                // Go to parent prefix or bucket
                if key.contains('/') {
                    let parent_prefix = key.rsplitn(2, '/').nth(1).unwrap();
                    Ok(VfsNode::Prefix {
                        bucket: bucket.clone(),
                        prefix: format!("{}/", parent_prefix),
                    })
                } else {
                    Ok(VfsNode::Bucket {
                        name: bucket.clone(),
                    })
                }
            }

            VfsNode::Archive { parent, .. } => Ok(*parent.clone()),

            VfsNode::ArchiveEntry { archive, path, .. } => {
                // Go up within the archive
                if path.trim_end_matches('/').contains('/') {
                    let parent_path = path
                        .trim_end_matches('/')
                        .rsplitn(2, '/')
                        .nth(1)
                        .unwrap()
                        .to_string();

                    Ok(VfsNode::ArchiveEntry {
                        archive: archive.clone(),
                        path: parent_path,
                        size: 0,
                        is_dir: true,
                    })
                } else {
                    Ok(*archive.clone())
                }
            }
        }
    }

    /// Navigate from current node to a named segment
    async fn navigate_to_segment(
        &self,
        state: &ShellState,
        current: &VfsNode,
        segment: &str,
    ) -> Result<VfsNode> {
        match current {
            VfsNode::Root => {
                // Segment is a bucket name
                Ok(VfsNode::Bucket {
                    name: segment.to_string(),
                })
            }

            VfsNode::Bucket { name } => {
                // Navigate within bucket
                // Try as object first
                if let Ok(metadata) = state.s3_client().head_object(name, segment).await {
                    let obj_node = VfsNode::Object {
                        bucket: name.clone(),
                        key: segment.to_string(),
                        size: metadata.size,
                    };
                    return self.try_archive_node(state, obj_node).await;
                }

                // Try as prefix
                let prefix_key = format!("{}/", segment);
                Ok(VfsNode::Prefix {
                    bucket: name.clone(),
                    prefix: prefix_key,
                })
            }

            VfsNode::Prefix { bucket, prefix } => {
                // Navigate within prefix
                let full_key = format!("{}{}", prefix, segment);

                // Try as object first
                if let Ok(metadata) = state.s3_client().head_object(bucket, &full_key).await {
                    let obj_node = VfsNode::Object {
                        bucket: bucket.clone(),
                        key: full_key.clone(),
                        size: metadata.size,
                    };
                    return self.try_archive_node(state, obj_node).await;
                }

                // Try as prefix
                let prefix_key = format!("{}/", full_key);
                Ok(VfsNode::Prefix {
                    bucket: bucket.clone(),
                    prefix: prefix_key,
                })
            }

            VfsNode::Archive { .. } => {
                // Navigate within archive
                let index = self.get_or_build_archive_index(state, current).await?;

                // Try both with and without trailing slash (tar archives often include the slash)
                let segment_with_slash = format!("{}/", segment);
                let entry = index.entries.get(segment)
                    .or_else(|| index.entries.get(&segment_with_slash));

                if let Some(entry) = entry {
                    if entry.is_dir {
                        // Store the path without trailing slash for consistency
                        let clean_path = entry.path.trim_end_matches('/').to_string();
                        return Ok(VfsNode::ArchiveEntry {
                            archive: Box::new(current.clone()),
                            path: clean_path,
                            size: entry.size,
                            is_dir: true,
                        });
                    }
                }

                Err(anyhow!("Path not found in archive: {}", segment))
            }

            VfsNode::ArchiveEntry { archive, path: current_path, .. } => {
                // Navigate within archive
                let index = self.get_or_build_archive_index(state, archive).await?;

                // Build target path
                let target_path = if current_path.is_empty() {
                    segment.to_string()
                } else {
                    format!("{}/{}", current_path.trim_end_matches('/'), segment)
                };

                // Try both with and without trailing slash
                let target_path_with_slash = format!("{}/", target_path);
                let entry = index.entries.get(&target_path)
                    .or_else(|| index.entries.get(&target_path_with_slash));

                if let Some(entry) = entry {
                    if entry.is_dir {
                        // Store the path without trailing slash for consistency
                        let clean_path = entry.path.trim_end_matches('/').to_string();
                        return Ok(VfsNode::ArchiveEntry {
                            archive: archive.clone(),
                            path: clean_path,
                            size: entry.size,
                            is_dir: true,
                        });
                    }
                }

                Err(anyhow!("Path not found in archive: {}", target_path))
            }

            VfsNode::Object { .. } => Err(anyhow!("Cannot cd from a file")),
        }
    }

    /// Resolve an absolute path from root
    async fn resolve_absolute_path(&self, state: &ShellState, path: &str) -> Result<VfsNode> {
        let vpath = VirtualPath::parse(path);
        let segments = vpath.segments();

        if segments.is_empty() {
            return Ok(VfsNode::Root);
        }

        // First segment is the bucket name
        let bucket = &segments[0];

        if segments.len() == 1 {
            // Just the bucket
            return Ok(VfsNode::Bucket {
                name: bucket.clone(),
            });
        }

        // Check if the path points to an object or prefix
        let key = segments[1..].join("/");

        // Try to get object metadata
        if let Ok(metadata) = state.s3_client().head_object(bucket, &key).await {
            // It's an object - check if it's an archive
            let obj_node = VfsNode::Object {
                bucket: bucket.clone(),
                key: key.clone(),
                size: metadata.size,
            };
            return self.try_archive_node(state, obj_node).await;
        }

        // Try as a prefix
        let prefix_key = if key.ends_with('/') {
            key
        } else {
            format!("{}/", key)
        };

        // Check if there are objects with this prefix
        let result = state
            .s3_client()
            .list_objects(bucket, &prefix_key, Some("/"))
            .await?;

        if !result.prefixes.is_empty() || !result.objects.is_empty() {
            Ok(VfsNode::Prefix {
                bucket: bucket.clone(),
                prefix: prefix_key,
            })
        } else {
            Err(anyhow!("Path not found: {}", path))
        }
    }

    /// Resolve a relative path from current node
    async fn resolve_relative_path(&self, state: &ShellState, path: &str) -> Result<VfsNode> {
        let current = state.current_node();

        match current {
            VfsNode::Root => {
                // Relative from root is just a bucket name
                Ok(VfsNode::Bucket {
                    name: path.to_string(),
                })
            }

            VfsNode::Bucket { name } => {
                // Navigate within bucket
                let key = path.to_string();

                // Try as object first
                if let Ok(metadata) = state.s3_client().head_object(name, &key).await {
                    let obj_node = VfsNode::Object {
                        bucket: name.clone(),
                        key: key.clone(),
                        size: metadata.size,
                    };
                    return self.try_archive_node(state, obj_node).await;
                }

                // Try as prefix
                let prefix_key = if key.ends_with('/') {
                    key
                } else {
                    format!("{}/", key)
                };

                Ok(VfsNode::Prefix {
                    bucket: name.clone(),
                    prefix: prefix_key,
                })
            }

            VfsNode::Prefix { bucket, prefix } => {
                // Navigate within prefix
                let full_key = format!("{}{}", prefix, path);

                // Try as object first
                if let Ok(metadata) = state.s3_client().head_object(bucket, &full_key).await {
                    let obj_node = VfsNode::Object {
                        bucket: bucket.clone(),
                        key: full_key.clone(),
                        size: metadata.size,
                    };
                    return self.try_archive_node(state, obj_node).await;
                }

                // Try as prefix
                let prefix_key = if full_key.ends_with('/') {
                    full_key
                } else {
                    format!("{}/", full_key)
                };

                Ok(VfsNode::Prefix {
                    bucket: bucket.clone(),
                    prefix: prefix_key,
                })
            }

            VfsNode::Object { .. } => Err(anyhow!("Cannot cd from a file")),

            VfsNode::Archive { .. } => {
                // Navigate within archive
                let index = self.get_or_build_archive_index(state, current).await?;

                // Check if path exists in the archive
                let normalized_path = path.trim_start_matches('/');

                if let Some(entry) = index.entries.get(normalized_path) {
                    if entry.is_dir {
                        return Ok(VfsNode::ArchiveEntry {
                            archive: Box::new(current.clone()),
                            path: normalized_path.to_string(),
                            size: entry.size,
                            is_dir: true,
                        });
                    }
                }

                Err(anyhow!("Path not found in archive: {}", path))
            }

            VfsNode::ArchiveEntry { archive, path: current_path, .. } => {
                // Navigate within archive
                let index = self.get_or_build_archive_index(state, archive).await?;

                // Check if path points to a subdirectory in the archive
                let target_path = if current_path.is_empty() {
                    path.to_string()
                } else {
                    format!("{}/{}", current_path.trim_end_matches('/'), path)
                };

                // Check if this path exists in the archive
                if let Some(entry) = index.entries.get(&target_path) {
                    if entry.is_dir {
                        return Ok(VfsNode::ArchiveEntry {
                            archive: archive.clone(),
                            path: target_path,
                            size: entry.size,
                            is_dir: true,
                        });
                    }
                }

                Err(anyhow!("Path not found in archive: {}", target_path))
            }
        }
    }

    /// Check if a node is an archive and convert it to an Archive node
    async fn try_archive_node(&self, state: &ShellState, node: VfsNode) -> Result<VfsNode> {
        match &node {
            VfsNode::Object { bucket, key, size } => {
                // Check if this is an archive by extension
                if let Some(archive_type) = ArchiveType::from_path(key) {
                    // Handle different archive types
                    let index = match &archive_type {
                        ArchiveType::Zip => {
                            let handler = ZipHandler::new();
                            handler.build_index(state.s3_client(), bucket, key).await?
                        }
                        ArchiveType::Tar | ArchiveType::TarGz | ArchiveType::TarBz2 => {
                            let handler = TarHandler::new(archive_type.clone());
                            handler.build_index(state.s3_client(), bucket, key).await?
                        }
                        _ => {
                            return Err(anyhow!(
                                "Archive type not yet supported: {:?}",
                                archive_type
                            ));
                        }
                    };

                    // Store in cache
                    let cache_key = format!("s3://{}/{}", bucket, key);
                    state.cache().put(cache_key, Arc::new(index.clone()));

                    return Ok(VfsNode::Archive {
                        parent: Box::new(VfsNode::Object {
                            bucket: bucket.clone(),
                            key: key.clone(),
                            size: *size,
                        }),
                        archive_type,
                        index: Some(Arc::new(index)),
                    });
                }
                Ok(node)
            }
            _ => Ok(node),
        }
    }

    /// Get or build archive index
    async fn get_or_build_archive_index(
        &self,
        state: &ShellState,
        archive_node: &VfsNode,
    ) -> Result<Arc<crate::vfs::ArchiveIndex>> {
        match archive_node {
            VfsNode::Archive {
                parent,
                archive_type,
                index,
            } => {
                // Check if we already have the index
                if let Some(idx) = index {
                    return Ok(Arc::clone(idx));
                }

                // Get bucket and key from parent
                let (bucket, key) = match parent.as_ref() {
                    VfsNode::Object { bucket, key, .. } => (bucket, key),
                    _ => return Err(anyhow!("Invalid archive parent node")),
                };

                // Check cache
                let cache_key = format!("s3://{}/{}", bucket, key);
                if let Some(cached) = state.cache().get(&cache_key) {
                    return Ok(cached);
                }

                // Build the index
                let idx = match archive_type {
                    ArchiveType::Zip => {
                        let handler = ZipHandler::new();
                        handler.build_index(state.s3_client(), bucket, key).await?
                    }
                    ArchiveType::Tar | ArchiveType::TarGz | ArchiveType::TarBz2 => {
                        let handler = TarHandler::new(archive_type.clone());
                        handler.build_index(state.s3_client(), bucket, key).await?
                    }
                    _ => return Err(anyhow!("Archive type not yet supported")),
                };
                let arc_idx = Arc::new(idx);
                state.cache().put(cache_key, Arc::clone(&arc_idx));
                Ok(arc_idx)
            }
            _ => Err(anyhow!("Not an archive node")),
        }
    }
}
