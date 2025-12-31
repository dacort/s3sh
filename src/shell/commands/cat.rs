use anyhow::{anyhow, Result};
use async_trait::async_trait;

use super::{Command, ShellState};
use crate::vfs::{VfsNode, VirtualPath};

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

            VfsNode::ArchiveEntry { .. } => {
                // Will implement in Phase 2
                return Err(anyhow!("Reading from archives not yet implemented"));
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
