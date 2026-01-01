use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::sync::Arc;

use super::{Command, ShellState};
use crate::archive::ArchiveHandler;
use crate::archive::tar::TarHandler;
use crate::archive::zip::ZipHandler;
use crate::ui::create_spinner;
use crate::vfs::{ArchiveType, VfsNode, VirtualPath};

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
                    Ok(text) => print!("{text}"),
                    Err(_) => {
                        eprintln!("Warning: File contains binary data");
                        // Display first 1KB as hex
                        let display_len = bytes.len().min(1024);
                        for (i, byte) in bytes[..display_len].iter().enumerate() {
                            if i % 16 == 0 {
                                print!("\n{i:08x}: ");
                            }
                            print!("{byte:02x} ");
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
                    return Err(anyhow!("Is a directory: {path_str}"));
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
                    let cache_key = format!("s3://{bucket}/{key}");
                    if let Some(cached) = state.cache().get(&cache_key) {
                        cached
                    } else {
                        // Show spinner while building index
                        let filename = key.split('/').next_back().unwrap_or(key);
                        let spinner = create_spinner(&format!("Building index for {filename}..."));

                        let built = match archive_type {
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

                        let arc = Arc::new(built);
                        state.cache().put(cache_key, Arc::clone(&arc));
                        arc
                    }
                };

                // Show spinner while extracting file
                let filename = file_path.split('/').next_back().unwrap_or(file_path);
                let spinner = create_spinner(&format!("Extracting {filename}..."));

                // Extract the file
                let bytes = match archive_type {
                    ArchiveType::Zip => {
                        let handler = ZipHandler::new();
                        handler
                            .extract_file(state.s3_client(), bucket, key, &idx, file_path)
                            .await?
                    }
                    ArchiveType::Tar | ArchiveType::TarGz | ArchiveType::TarBz2 => {
                        let handler = TarHandler::new(archive_type.clone());
                        handler
                            .extract_file(state.s3_client(), bucket, key, &idx, file_path)
                            .await?
                    }
                    _ => {
                        spinner.finish_and_clear();
                        return Err(anyhow!("Archive type not yet supported"));
                    }
                };

                spinner.finish_and_clear();

                // Try to display as UTF-8 text
                match String::from_utf8(bytes.to_vec()) {
                    Ok(text) => print!("{text}"),
                    Err(_) => {
                        eprintln!("Warning: File contains binary data");
                        // Display first 1KB as hex
                        let display_len = bytes.len().min(1024);
                        for (i, byte) in bytes[..display_len].iter().enumerate() {
                            if i % 16 == 0 {
                                print!("\n{i:08x}: ");
                            }
                            print!("{byte:02x} ");
                        }
                        println!();
                        if bytes.len() > 1024 {
                            eprintln!("... ({} more bytes)", bytes.len() - 1024);
                        }
                    }
                }
            }

            _ => {
                return Err(anyhow!("Not a file: {path_str}"));
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
            return Err(anyhow!("Invalid file path: {path}"));
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
                let key = format!("{prefix}{path}");
                let metadata = state.s3_client().head_object(bucket, &key).await?;
                Ok(VfsNode::Object {
                    bucket: bucket.clone(),
                    key,
                    size: metadata.size,
                })
            }

            VfsNode::Archive { index, .. } => {
                // Resolve file at root of archive
                let idx = if let Some(i) = index {
                    Arc::clone(i)
                } else {
                    return Err(anyhow!("Archive index not available"));
                };

                let entry = idx
                    .find_entry(path)
                    .ok_or_else(|| anyhow!("File not found in archive: {path}"))?;

                Ok(VfsNode::ArchiveEntry {
                    archive: Box::new(current.clone()),
                    path: path.to_string(),
                    size: entry.size,
                    is_dir: entry.is_dir,
                })
            }

            VfsNode::ArchiveEntry {
                archive,
                path: current_path,
                ..
            } => {
                // Resolve file relative to current path in archive
                let idx = match archive.as_ref() {
                    VfsNode::Archive { index: Some(i), .. } => Arc::clone(i),
                    _ => return Err(anyhow!("Archive index not available")),
                };

                // Build full path
                let full_path = if current_path.is_empty() {
                    path.to_string()
                } else {
                    format!("{}/{}", current_path.trim_end_matches('/'), path)
                };

                let entry = idx
                    .find_entry(&full_path)
                    .ok_or_else(|| anyhow!("File not found in archive: {path}"))?;

                Ok(VfsNode::ArchiveEntry {
                    archive: archive.clone(),
                    path: full_path,
                    size: entry.size,
                    is_dir: entry.is_dir,
                })
            }

            _ => Err(anyhow!(
                "Cannot resolve relative path from current location"
            )),
        }
    }
}
