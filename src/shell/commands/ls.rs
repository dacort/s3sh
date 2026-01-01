use anyhow::{anyhow, Result};
use async_trait::async_trait;
use colored::*;

use super::{Command, ShellState};
use crate::archive::zip::ZipHandler;
use crate::archive::ArchiveHandler;
use crate::vfs::{ArchiveType, VfsNode};
use std::sync::Arc;

pub struct LsCommand;

#[async_trait]
impl Command for LsCommand {
    fn name(&self) -> &str {
        "ls"
    }

    fn usage(&self) -> &str {
        "ls [OPTIONS] - List directory contents"
    }

    async fn execute(&self, state: &mut ShellState, args: &[String]) -> Result<()> {
        // Parse flags
        let _recursive = args.contains(&"-R".to_string()) || args.contains(&"-r".to_string());
        let long_format = args.contains(&"-l".to_string());

        match state.current_node() {
            VfsNode::Root => {
                // List S3 buckets
                let buckets = state.s3_client().list_buckets().await?;

                if long_format {
                    println!("{:<30} {}", "NAME", "CREATED");
                    println!("{}", "-".repeat(60));
                    for bucket in buckets {
                        let created = bucket.creation_date.unwrap_or_else(|| "-".to_string());
                        println!("{:<30} {}", bucket.name.blue().bold(), created);
                    }
                } else {
                    for bucket in buckets {
                        println!("{}/", bucket.name.blue().bold());
                    }
                }
            }

            VfsNode::Bucket { name } => {
                // List objects in bucket (top level)
                let result = state.s3_client().list_objects(name, "", Some("/")).await?;

                if long_format {
                    println!("{:<50} {:>12} {}", "NAME", "SIZE", "MODIFIED");
                    println!("{}", "-".repeat(80));

                    // Print prefixes (directories)
                    for prefix in &result.prefixes {
                        let display_name = prefix.trim_end_matches('/').rsplit('/').next().unwrap_or(prefix);
                        println!("{:<50} {:>12} {}",
                            format!("{}/", display_name).blue().bold(),
                            "-",
                            "-"
                        );
                    }

                    // Print objects
                    for obj in &result.objects {
                        let display_name = obj.key.rsplit('/').next().unwrap_or(&obj.key);
                        let modified = obj.last_modified.as_deref().unwrap_or("-");
                        println!("{:<50} {:>12} {}",
                            display_name,
                            humansize::format_size(obj.size, humansize::BINARY),
                            modified
                        );
                    }
                } else {
                    // Print prefixes
                    for prefix in &result.prefixes {
                        let display_name = prefix.trim_end_matches('/').rsplit('/').next().unwrap_or(prefix);
                        println!("{}/", display_name.blue().bold());
                    }

                    // Print objects
                    for obj in &result.objects {
                        let display_name = obj.key.rsplit('/').next().unwrap_or(&obj.key);
                        println!("{}", display_name);
                    }
                }
            }

            VfsNode::Prefix { bucket, prefix } => {
                // List objects with this prefix
                let result = state
                    .s3_client()
                    .list_objects(bucket, prefix, Some("/"))
                    .await?;

                if long_format {
                    println!("{:<50} {:>12} {}", "NAME", "SIZE", "MODIFIED");
                    println!("{}", "-".repeat(80));

                    // Print prefixes (directories)
                    for p in &result.prefixes {
                        let display_name = p.trim_end_matches('/').rsplit('/').next().unwrap_or(p);
                        println!("{:<50} {:>12} {}",
                            format!("{}/", display_name).blue().bold(),
                            "-",
                            "-"
                        );
                    }

                    // Print objects
                    for obj in &result.objects {
                        let display_name = obj.key.rsplit('/').next().unwrap_or(&obj.key);
                        let modified = obj.last_modified.as_deref().unwrap_or("-");
                        println!("{:<50} {:>12} {}",
                            display_name,
                            humansize::format_size(obj.size, humansize::BINARY),
                            modified
                        );
                    }
                } else {
                    // Print prefixes
                    for p in &result.prefixes {
                        let display_name = p.trim_end_matches('/').rsplit('/').next().unwrap_or(p);
                        println!("{}/", display_name.blue().bold());
                    }

                    // Print objects
                    for obj in &result.objects {
                        let display_name = obj.key.rsplit('/').next().unwrap_or(&obj.key);
                        println!("{}", display_name);
                    }
                }
            }

            VfsNode::Archive {
                parent,
                archive_type,
                index,
            } => {
                // List contents of archive at root
                let idx = if let Some(i) = index {
                    Arc::clone(i)
                } else {
                    // Build index
                    let (bucket, key) = match parent.as_ref() {
                        VfsNode::Object { bucket, key, .. } => (bucket, key),
                        _ => return Err(anyhow!("Invalid archive parent")),
                    };

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

                // List entries at root
                let handler = ZipHandler::new();
                let entries = handler.list_entries(&idx, "");

                if long_format {
                    println!("{:<50} {:>12}", "NAME", "SIZE");
                    println!("{}", "-".repeat(65));

                    for entry in entries {
                        let display_name = if entry.is_dir {
                            format!("{}/", entry.path.trim_end_matches('/').rsplit('/').next().unwrap_or(&entry.path))
                        } else {
                            entry.path.rsplit('/').next().unwrap_or(&entry.path).to_string()
                        };

                        let size_str = if entry.is_dir {
                            "-".to_string()
                        } else {
                            humansize::format_size(entry.size, humansize::BINARY)
                        };

                        if entry.is_dir {
                            println!("{:<50} {:>12}", display_name.blue().bold(), size_str);
                        } else {
                            println!("{:<50} {:>12}", display_name, size_str);
                        }
                    }
                } else {
                    for entry in entries {
                        let display_name = if entry.is_dir {
                            format!("{}/", entry.path.trim_end_matches('/').rsplit('/').next().unwrap_or(&entry.path))
                        } else {
                            entry.path.rsplit('/').next().unwrap_or(&entry.path).to_string()
                        };

                        if entry.is_dir {
                            println!("{}", display_name.blue().bold());
                        } else {
                            println!("{}", display_name);
                        }
                    }
                }
            }

            VfsNode::ArchiveEntry {
                archive,
                path,
                is_dir,
                ..
            } => {
                if !is_dir {
                    return Err(anyhow!("Not a directory"));
                }

                // Get archive index
                let idx = match archive.as_ref() {
                    VfsNode::Archive { index: Some(i), .. } => Arc::clone(i),
                    VfsNode::Archive {
                        parent,
                        archive_type,
                        ..
                    } => {
                        let (bucket, key) = match parent.as_ref() {
                            VfsNode::Object { bucket, key, .. } => (bucket, key),
                            _ => return Err(anyhow!("Invalid archive parent")),
                        };

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
                    }
                    _ => return Err(anyhow!("Not an archive")),
                };

                // List entries at this path
                let handler = ZipHandler::new();
                let entries = handler.list_entries(&idx, path);

                if long_format {
                    println!("{:<50} {:>12}", "NAME", "SIZE");
                    println!("{}", "-".repeat(65));

                    for entry in entries {
                        let full_path = &entry.path;
                        let display_name = if entry.is_dir {
                            format!("{}/", full_path.trim_end_matches('/').rsplit('/').next().unwrap_or(full_path))
                        } else {
                            full_path.rsplit('/').next().unwrap_or(full_path).to_string()
                        };

                        let size_str = if entry.is_dir {
                            "-".to_string()
                        } else {
                            humansize::format_size(entry.size, humansize::BINARY)
                        };

                        if entry.is_dir {
                            println!("{:<50} {:>12}", display_name.blue().bold(), size_str);
                        } else {
                            println!("{:<50} {:>12}", display_name, size_str);
                        }
                    }
                } else {
                    for entry in entries {
                        let full_path = &entry.path;
                        let display_name = if entry.is_dir {
                            format!("{}/", full_path.trim_end_matches('/').rsplit('/').next().unwrap_or(full_path))
                        } else {
                            full_path.rsplit('/').next().unwrap_or(full_path).to_string()
                        };

                        if entry.is_dir {
                            println!("{}", display_name.blue().bold());
                        } else {
                            println!("{}", display_name);
                        }
                    }
                }
            }

            VfsNode::Object { .. } => {
                return Err(anyhow!("Not a directory"));
            }
        }

        Ok(())
    }
}
