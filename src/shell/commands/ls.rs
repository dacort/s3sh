use anyhow::{anyhow, Result};
use async_trait::async_trait;
use colored::*;

use super::{Command, ShellState};
use crate::vfs::VfsNode;

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

            VfsNode::Archive { .. } => {
                // Will implement in Phase 2
                return Err(anyhow!("Listing archives not yet implemented"));
            }

            VfsNode::ArchiveEntry { .. } => {
                // Will implement in Phase 2
                return Err(anyhow!("Listing archive entries not yet implemented"));
            }

            VfsNode::Object { .. } => {
                return Err(anyhow!("Not a directory"));
            }
        }

        Ok(())
    }
}
