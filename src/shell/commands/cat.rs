use anyhow::{anyhow, Result};
use async_trait::async_trait;

use super::{Command, ShellState};
use crate::archive::zip::ZipHandler;
use crate::archive::ArchiveHandler;
use crate::vfs::{ArchiveType, VfsNode, VirtualPath};
use std::sync::Arc;

pub struct CatCommand;

#[async_trait]
impl Command for CatCommand {
    fn name(&self) -> &str {
        "cat"
    }

    fn usage(&self) -> &str {
        "cat FILE - Display file contents"
    }

    async fn execute(&self, state: &mut ShellState, args: &[String]) -> Result<()> {
        if args.is_empty() {
            return Err(anyhow!("Usage: cat FILE"));
        }

        let path_str = &args[0];

        // Resolve the path to a node
        let target_node = if path_str.starts_with('/') {
            self.resolve_absolute(state, path_str).await?
        } else {
            self.resolve_relative(state, path_str).await?
        };

        // Read the file
        match &target_node {
            VfsNode::Object { bucket, key, .. } => {
                let bytes = state.s3_client().get_object(bucket, key).await?;

                // Try to display as UTF-8 text
                match String::from_utf8(bytes.to_vec()) {
                    Ok(text) => print!("{}", text),
                    Err(_) => {
                        eprintln!("Warning: File contains binary data");
                        // Display first 1KB as hex
                        let display_len = bytes.len().min(1024);
                        for (i, byte) in bytes[..display_len].iter().enumerate() {
                            if i % 16 == 0 {
                                print!("\n{:08x}: ", i);
                            }
                            print!("{:02x} ", byte);
                        }
                        println!();
                        if bytes.len() > 1024 {
                            eprintln!("... ({} more bytes)", bytes.len() - 1024);
                        }
                    }
                }
            }

            VfsNode::ArchiveEntry {
                archive,
                path: file_path,
                is_dir,
                ..
            } => {
                if *is_dir {
                    return Err(anyhow!("Is a directory: {}", path_str));
                }

                // Get bucket and key from archive
                let (bucket, key, archive_type, index) = match archive.as_ref() {
                    VfsNode::Archive {
                        parent,
                        archive_type,
                        index,
                    } => {
                        let (b, k) = match parent.as_ref() {
                            VfsNode::Object { bucket, key, .. } => (bucket, key),
                            _ => return Err(anyhow!("Invalid archive parent")),
                        };
                        (b, k, archive_type, index)
                    }
                    _ => return Err(anyhow!("Not an archive")),
                };

                // Get or build index
                let idx = if let Some(i) = index {
                    Arc::clone(i)
                } else {
                    let cache_key = format!("s3://{}/{}", bucket, key);
                    if let Some(cached) = state.cache().get(&cache_key) {
                        cached
                    } else {
                        match archive_type {
                            ArchiveType::Zip => {
                                let handler = ZipHandler::new();
                                let built = handler.build_index(state.s3_client(), bucket, key).await?;
                                let arc = Arc::new(built);
                                state.cache().put(cache_key, Arc::clone(&arc));
                                arc
                            }
                            _ => return Err(anyhow!("Archive type not yet supported")),
                        }
                    }
                };

                // Extract the file
                let bytes = match archive_type {
                    ArchiveType::Zip => {
                        let handler = ZipHandler::new();
                        handler
                            .extract_file(state.s3_client(), bucket, key, &idx, file_path)
                            .await?
                    }
                    _ => return Err(anyhow!("Archive type not yet supported")),
                };

                // Try to display as UTF-8 text
                match String::from_utf8(bytes.to_vec()) {
                    Ok(text) => print!("{}", text),
                    Err(_) => {
                        eprintln!("Warning: File contains binary data");
                        // Display first 1KB as hex
                        let display_len = bytes.len().min(1024);
                        for (i, byte) in bytes[..display_len].iter().enumerate() {
                            if i % 16 == 0 {
                                print!("\n{:08x}: ", i);
                            }
                            print!("{:02x} ", byte);
                        }
                        println!();
                        if bytes.len() > 1024 {
                            eprintln!("... ({} more bytes)", bytes.len() - 1024);
                        }
                    }
                }
            }

            _ => {
                return Err(anyhow!("Not a file: {}", path_str));
            }
        }

        Ok(())
    }
}

impl CatCommand {
    /// Resolve absolute path to a VFS node
    async fn resolve_absolute(&self, state: &ShellState, path: &str) -> Result<VfsNode> {
        let vpath = VirtualPath::parse(path);
        let segments = vpath.segments();

        if segments.len() < 2 {
            return Err(anyhow!("Invalid file path: {}", path));
        }

        let bucket = &segments[0];
        let key = segments[1..].join("/");

        // Get object metadata
        let metadata = state.s3_client().head_object(bucket, &key).await?;

        Ok(VfsNode::Object {
            bucket: bucket.clone(),
            key,
            size: metadata.size,
        })
    }

    /// Resolve relative path to a VFS node
    async fn resolve_relative(&self, state: &ShellState, path: &str) -> Result<VfsNode> {
        let current = state.current_node();

        match current {
            VfsNode::Bucket { name } => {
                let metadata = state.s3_client().head_object(name, path).await?;
                Ok(VfsNode::Object {
                    bucket: name.clone(),
                    key: path.to_string(),
                    size: metadata.size,
                })
            }

            VfsNode::Prefix { bucket, prefix } => {
                let key = format!("{}{}", prefix, path);
                let metadata = state.s3_client().head_object(bucket, &key).await?;
                Ok(VfsNode::Object {
                    bucket: bucket.clone(),
                    key,
                    size: metadata.size,
                })
            }

            _ => Err(anyhow!("Cannot resolve relative path from current location")),
        }
    }
}
