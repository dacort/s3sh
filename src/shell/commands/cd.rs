use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::sync::Arc;

use super::{Command, ShellState};
use crate::archive::ArchiveHandler;
use crate::archive::tar::TarHandler;
use crate::archive::zip::ZipHandler;
use crate::ui::create_spinner;
use crate::vfs::{ArchiveType, VfsNode};

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
            return Err(anyhow!("Not a directory: {path_str}"));
        }

        state.set_current_node(current.clone());

        // Don't pre-populate completion cache here - let lazy loader fetch accurate is_dir info
        // This ensures cd only completes directories, cat completes everything

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
                    let parent_prefix = prefix.trim_end_matches('/').rsplit_once('/').unwrap().0;
                    Ok(VfsNode::Prefix {
                        bucket: bucket.clone(),
                        prefix: format!("{parent_prefix}/"),
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
                    let parent_prefix = key.rsplit_once('/').unwrap().0;
                    Ok(VfsNode::Prefix {
                        bucket: bucket.clone(),
                        prefix: format!("{parent_prefix}/"),
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
                        .rsplit_once('/')
                        .unwrap()
                        .0
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
                let prefix_key = format!("{segment}/");
                Ok(VfsNode::Prefix {
                    bucket: name.clone(),
                    prefix: prefix_key,
                })
            }

            VfsNode::Prefix { bucket, prefix } => {
                // Navigate within prefix
                let full_key = format!("{prefix}{segment}");

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
                let prefix_key = format!("{full_key}/");
                Ok(VfsNode::Prefix {
                    bucket: bucket.clone(),
                    prefix: prefix_key,
                })
            }

            VfsNode::Archive { .. } => {
                // Navigate within archive
                let index = self.get_or_build_archive_index(state, current).await?;

                if let Some(entry) = index.find_entry(segment)
                    && entry.is_dir
                {
                    // Store the path without trailing slash for consistency
                    let clean_path = entry.path.trim_end_matches('/').to_string();
                    return Ok(VfsNode::ArchiveEntry {
                        archive: Box::new(current.clone()),
                        path: clean_path,
                        size: entry.size,
                        is_dir: true,
                    });
                }

                Err(anyhow!("Path not found in archive: {segment}"))
            }

            VfsNode::ArchiveEntry {
                archive,
                path: current_path,
                ..
            } => {
                // Navigate within archive
                let index = self.get_or_build_archive_index(state, archive).await?;

                // Build target path
                let target_path = if current_path.is_empty() {
                    segment.to_string()
                } else {
                    format!("{}/{}", current_path.trim_end_matches('/'), segment)
                };

                if let Some(entry) = index.find_entry(&target_path)
                    && entry.is_dir
                {
                    // Store the path without trailing slash for consistency
                    let clean_path = entry.path.trim_end_matches('/').to_string();
                    return Ok(VfsNode::ArchiveEntry {
                        archive: archive.clone(),
                        path: clean_path,
                        size: entry.size,
                        is_dir: true,
                    });
                }

                Err(anyhow!("Path not found in archive: {target_path}"))
            }

            VfsNode::Object { .. } => Err(anyhow!("Cannot cd from a file")),
        }
    }

    /// Check if a node is an archive and convert it to an Archive node
    async fn try_archive_node(&self, state: &ShellState, node: VfsNode) -> Result<VfsNode> {
        match &node {
            VfsNode::Object { bucket, key, size } => {
                // Check if this is an archive by extension
                if let Some(archive_type) = ArchiveType::from_path(key) {
                    // Show spinner while building index
                    let filename = key.split('/').next_back().unwrap_or(key);
                    let spinner = create_spinner(&format!("Building index for {filename}..."));

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
                            spinner.finish_and_clear();
                            return Err(anyhow!(
                                "Archive type not yet supported: {archive_type:?}"
                            ));
                        }
                    };

                    spinner.finish_and_clear();

                    // Store in cache
                    let cache_key = format!("s3://{bucket}/{key}");
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
                let cache_key = format!("s3://{bucket}/{key}");
                if let Some(cached) = state.cache().get(&cache_key) {
                    return Ok(cached);
                }

                // Show spinner while building index
                let filename = key.split('/').next_back().unwrap_or(key);
                let spinner = create_spinner(&format!("Building index for {filename}..."));

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
                    _ => {
                        spinner.finish_and_clear();
                        return Err(anyhow!("Archive type not yet supported"));
                    }
                };

                spinner.finish_and_clear();

                let arc_idx = Arc::new(idx);
                state.cache().put(cache_key, Arc::clone(&arc_idx));
                Ok(arc_idx)
            }
            _ => Err(anyhow!("Not an archive node")),
        }
    }
}
