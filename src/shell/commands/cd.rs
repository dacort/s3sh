use anyhow::{anyhow, Result};
use async_trait::async_trait;

use super::{Command, ShellState};
use crate::vfs::{VfsNode, VirtualPath};

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
                if path.contains('/') {
                    let parent_path = path.rsplitn(2, '/').nth(1).unwrap();
                    // TODO: Create proper ArchiveEntry node for parent
                    Ok(*archive.clone())
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
            // It's an object
            return Ok(VfsNode::Object {
                bucket: bucket.clone(),
                key: key.clone(),
                size: metadata.size,
            });
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
                    return Ok(VfsNode::Object {
                        bucket: name.clone(),
                        key: key.clone(),
                        size: metadata.size,
                    });
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
                    return Ok(VfsNode::Object {
                        bucket: bucket.clone(),
                        key: full_key.clone(),
                        size: metadata.size,
                    });
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
                // Will implement in Phase 2
                Err(anyhow!("cd within archives not yet implemented"))
            }

            VfsNode::ArchiveEntry { .. } => {
                // Will implement in Phase 2
                Err(anyhow!("cd within archives not yet implemented"))
            }
        }
    }
}
