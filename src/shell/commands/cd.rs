use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::sync::Arc;

use super::{Command, ShellState};
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

        // Parse the path
        let target_node = if path_str == "/" {
            // Go to root
            VfsNode::Root
        } else if path_str == ".." {
            // Go up one level
            self.navigate_up(state.current_node())?
        } else if path_str.starts_with('/') {
            // Absolute path
            self.resolve_absolute_path(state, path_str).await?
        } else {
            // Relative path
            self.resolve_relative_path(state, path_str).await?
        };

        // Verify the target is navigable
        if !target_node.is_navigable() {
            return Err(anyhow!("Not a directory: {}", path_str));
        }

        state.set_current_node(target_node);
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
                    // For now, only handle Zip
                    if matches!(archive_type, ArchiveType::Zip) {
                        // Build the archive index
                        let handler = ZipHandler::new();
                        let index = handler
                            .build_index(state.s3_client(), bucket, key)
                            .await?;

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
                    } else {
                        return Err(anyhow!(
                            "Archive type not yet supported: {:?}",
                            archive_type
                        ));
                    }
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
                match archive_type {
                    ArchiveType::Zip => {
                        let handler = ZipHandler::new();
                        let idx = handler.build_index(state.s3_client(), bucket, key).await?;
                        let arc_idx = Arc::new(idx);
                        state.cache().put(cache_key, Arc::clone(&arc_idx));
                        Ok(arc_idx)
                    }
                    _ => Err(anyhow!("Archive type not yet supported")),
                }
            }
            _ => Err(anyhow!("Not an archive node")),
        }
    }
}
