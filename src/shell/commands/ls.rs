use anyhow::{Result, anyhow};
use async_trait::async_trait;
use colored::*;

use super::{Command, ShellState};
use crate::archive::ArchiveHandler;
use crate::archive::tar::TarHandler;
use crate::archive::zip::ZipHandler;
#[cfg(feature = "parquet")]
use crate::archive::ParquetHandler;
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
        // Parse flags and path
        let mut _recursive = false;
        let mut long_format = false;
        let mut path_arg: Option<String> = None;

        for arg in args {
            if arg == "-R" || arg == "-r" {
                _recursive = true;
            } else if arg == "-l" {
                long_format = true;
            } else if !arg.starts_with('-') {
                path_arg = Some(arg.clone());
                break; // Only take the first non-flag argument
            }
        }

        // Resolve the target node - either from path arg or current node
        // Also check if we need to filter by wildcard
        let (target_node, filter_pattern) = if let Some(path) = path_arg {
            // Check if path contains wildcards
            if path.contains('*') || path.contains('?') {
                // Extract parent path and pattern
                let (parent_path, pattern) = if let Some(pos) = path.rfind('/') {
                    (&path[..pos], &path[pos + 1..])
                } else {
                    // No slash, pattern is relative to current dir
                    ("", path.as_str())
                };

                let node = if parent_path.is_empty() {
                    state.current_node().clone()
                } else {
                    self.resolve_path(state, parent_path).await?
                };

                (node, Some(pattern.to_string()))
            } else {
                (self.resolve_path(state, &path).await?, None)
            }
        } else {
            (state.current_node().clone(), None)
        };

        match &target_node {
            VfsNode::Root => {
                // List S3 buckets
                let buckets = state.s3_client().list_buckets().await?;

                if long_format {
                    println!("{:<30} CREATED", "NAME");
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
                    println!("{:<50} {:>12} MODIFIED", "NAME", "SIZE");
                    println!("{}", "-".repeat(80));

                    // Print prefixes (directories)
                    for prefix in &result.prefixes {
                        let display_name = prefix
                            .trim_end_matches('/')
                            .rsplit('/')
                            .next()
                            .unwrap_or(prefix);
                        if Self::should_display(display_name, &filter_pattern) {
                            println!(
                                "{:<50} {:>12} -",
                                format!("{display_name}/").blue().bold(),
                                "-"
                            );
                        }
                    }

                    // Print objects
                    for obj in &result.objects {
                        let display_name = obj.key.rsplit('/').next().unwrap_or(&obj.key);
                        if Self::should_display(display_name, &filter_pattern) {
                            let modified = obj.last_modified.as_deref().unwrap_or("-");
                            println!(
                                "{:<50} {:>12} {}",
                                display_name,
                                humansize::format_size(obj.size, humansize::BINARY),
                                modified
                            );
                        }
                    }
                } else {
                    // Print prefixes
                    for prefix in &result.prefixes {
                        let display_name = prefix
                            .trim_end_matches('/')
                            .rsplit('/')
                            .next()
                            .unwrap_or(prefix);
                        if Self::should_display(display_name, &filter_pattern) {
                            println!("{}/", display_name.blue().bold());
                        }
                    }

                    // Print objects
                    for obj in &result.objects {
                        let display_name = obj.key.rsplit('/').next().unwrap_or(&obj.key);
                        if Self::should_display(display_name, &filter_pattern) {
                            println!("{display_name}");
                        }
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
                    println!("{:<50} {:>12} MODIFIED", "NAME", "SIZE");
                    println!("{}", "-".repeat(80));

                    // Print prefixes (directories)
                    for p in &result.prefixes {
                        let display_name = p.trim_end_matches('/').rsplit('/').next().unwrap_or(p);
                        if Self::should_display(display_name, &filter_pattern) {
                            println!(
                                "{:<50} {:>12} -",
                                format!("{display_name}/").blue().bold(),
                                "-"
                            );
                        }
                    }

                    // Print objects
                    for obj in &result.objects {
                        let display_name = obj.key.rsplit('/').next().unwrap_or(&obj.key);
                        if Self::should_display(display_name, &filter_pattern) {
                            let modified = obj.last_modified.as_deref().unwrap_or("-");
                            println!(
                                "{:<50} {:>12} {}",
                                display_name,
                                humansize::format_size(obj.size, humansize::BINARY),
                                modified
                            );
                        }
                    }
                } else {
                    // Print prefixes
                    for p in &result.prefixes {
                        let display_name = p.trim_end_matches('/').rsplit('/').next().unwrap_or(p);
                        if Self::should_display(display_name, &filter_pattern) {
                            println!("{}/", display_name.blue().bold());
                        }
                    }

                    // Print objects
                    for obj in &result.objects {
                        let display_name = obj.key.rsplit('/').next().unwrap_or(&obj.key);
                        if Self::should_display(display_name, &filter_pattern) {
                            println!("{display_name}");
                        }
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

                    let cache_key = format!("s3://{bucket}/{key}");
                    if let Some(cached) = state.cache().get(&cache_key) {
                        cached
                    } else {
                        let built = match archive_type {
                            ArchiveType::Zip => {
                                let handler = ZipHandler::new();
                                handler.build_index(state.s3_client(), bucket, key).await?
                            }
                            ArchiveType::Tar | ArchiveType::TarGz | ArchiveType::TarBz2 => {
                                let handler = TarHandler::new(archive_type.clone());
                                handler.build_index(state.s3_client(), bucket, key).await?
                            }
                            #[cfg(feature = "parquet")]
                            ArchiveType::Parquet => {
                                let handler = ParquetHandler::new();
                                handler.build_index(state.s3_client(), bucket, key).await?
                            }
                            _ => return Err(anyhow!("Archive type not yet supported")),
                        };
                        let arc = Arc::new(built);
                        state.cache().put(cache_key, Arc::clone(&arc));
                        arc
                    }
                };

                // List entries at root
                let entries: Vec<_> = match archive_type {
                    ArchiveType::Zip => {
                        let handler = ZipHandler::new();
                        handler.list_entries(&idx, "")
                    }
                    ArchiveType::Tar | ArchiveType::TarGz | ArchiveType::TarBz2 => {
                        let handler = TarHandler::new(archive_type.clone());
                        handler.list_entries(&idx, "")
                    }
                    #[cfg(feature = "parquet")]
                    ArchiveType::Parquet => {
                        let handler = ParquetHandler::new();
                        handler.list_entries(&idx, "")
                    }
                    _ => return Err(anyhow!("Archive type not yet supported")),
                };

                if long_format {
                    println!("{:<50} {:>12}", "NAME", "SIZE");
                    println!("{}", "-".repeat(65));

                    for entry in entries {
                        let base_name = entry
                            .path
                            .trim_end_matches('/')
                            .rsplit('/')
                            .next()
                            .unwrap_or(&entry.path);
                        if !Self::should_display(base_name, &filter_pattern) {
                            continue;
                        }

                        let display_name = if entry.is_dir {
                            format!("{base_name}/")
                        } else {
                            base_name.to_string()
                        };

                        let size_str = if entry.is_dir {
                            "-".to_string()
                        } else {
                            humansize::format_size(entry.size, humansize::BINARY)
                        };

                        if entry.is_dir {
                            println!("{:<50} {:>12}", display_name.blue().bold(), size_str);
                        } else {
                            println!("{display_name:<50} {size_str:>12}");
                        }
                    }
                } else {
                    for entry in entries {
                        let base_name = entry
                            .path
                            .trim_end_matches('/')
                            .rsplit('/')
                            .next()
                            .unwrap_or(&entry.path);
                        if !Self::should_display(base_name, &filter_pattern) {
                            continue;
                        }

                        let display_name = if entry.is_dir {
                            format!("{base_name}/")
                        } else {
                            base_name.to_string()
                        };

                        if entry.is_dir {
                            println!("{}", display_name.blue().bold());
                        } else {
                            println!("{display_name}");
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

                // Get archive type
                let archive_type = match archive.as_ref() {
                    VfsNode::Archive { archive_type, .. } => archive_type.clone(),
                    _ => return Err(anyhow!("Not an archive")),
                };

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

                        let cache_key = format!("s3://{bucket}/{key}");
                        if let Some(cached) = state.cache().get(&cache_key) {
                            cached
                        } else {
                            let built = match archive_type {
                                ArchiveType::Zip => {
                                    let handler = ZipHandler::new();
                                    handler.build_index(state.s3_client(), bucket, key).await?
                                }
                                ArchiveType::Tar | ArchiveType::TarGz | ArchiveType::TarBz2 => {
                                    let handler = TarHandler::new(archive_type.clone());
                                    handler.build_index(state.s3_client(), bucket, key).await?
                                }
                                #[cfg(feature = "parquet")]
                                ArchiveType::Parquet => {
                                    let handler = ParquetHandler::new();
                                    handler.build_index(state.s3_client(), bucket, key).await?
                                }
                                _ => return Err(anyhow!("Archive type not yet supported")),
                            };
                            let arc = Arc::new(built);
                            state.cache().put(cache_key, Arc::clone(&arc));
                            arc
                        }
                    }
                    _ => return Err(anyhow!("Not an archive")),
                };

                // List entries at this path
                let entries: Vec<_> = match &archive_type {
                    ArchiveType::Zip => {
                        let handler = ZipHandler::new();
                        handler.list_entries(&idx, path)
                    }
                    ArchiveType::Tar | ArchiveType::TarGz | ArchiveType::TarBz2 => {
                        let handler = TarHandler::new(archive_type.clone());
                        handler.list_entries(&idx, path)
                    }
                    #[cfg(feature = "parquet")]
                    ArchiveType::Parquet => {
                        let handler = ParquetHandler::new();
                        handler.list_entries(&idx, path)
                    }
                    _ => return Err(anyhow!("Archive type not yet supported")),
                };

                if long_format {
                    println!("{:<50} {:>12}", "NAME", "SIZE");
                    println!("{}", "-".repeat(65));

                    for entry in entries {
                        let full_path = &entry.path;
                        let base_name = full_path
                            .trim_end_matches('/')
                            .rsplit('/')
                            .next()
                            .unwrap_or(full_path);
                        if !Self::should_display(base_name, &filter_pattern) {
                            continue;
                        }

                        let display_name = if entry.is_dir {
                            format!("{base_name}/")
                        } else {
                            base_name.to_string()
                        };

                        let size_str = if entry.is_dir {
                            "-".to_string()
                        } else {
                            humansize::format_size(entry.size, humansize::BINARY)
                        };

                        if entry.is_dir {
                            println!("{:<50} {:>12}", display_name.blue().bold(), size_str);
                        } else {
                            println!("{display_name:<50} {size_str:>12}");
                        }
                    }
                } else {
                    for entry in entries {
                        let full_path = &entry.path;
                        let base_name = full_path
                            .trim_end_matches('/')
                            .rsplit('/')
                            .next()
                            .unwrap_or(full_path);
                        if !Self::should_display(base_name, &filter_pattern) {
                            continue;
                        }

                        let display_name = if entry.is_dir {
                            format!("{base_name}/")
                        } else {
                            base_name.to_string()
                        };

                        if entry.is_dir {
                            println!("{}", display_name.blue().bold());
                        } else {
                            println!("{display_name}");
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

impl LsCommand {
    /// Resolve a path (absolute or relative) to a VFS node
    async fn resolve_path(&self, state: &ShellState, path: &str) -> Result<VfsNode> {
        // Start from root for absolute paths, current for relative
        let mut current = if path.starts_with('/') {
            VfsNode::Root
        } else {
            state.current_node().clone()
        };

        // Split path into segments
        let segments: Vec<&str> = path
            .trim_matches('/')
            .split('/')
            .filter(|s| !s.is_empty() && *s != ".")
            .collect();

        // If path is just "/", return root
        if path == "/" || segments.is_empty() {
            return Ok(VfsNode::Root);
        }

        // Process each segment
        for segment in segments {
            current = if segment == ".." {
                self.navigate_up(&current)?
            } else {
                self.navigate_to_segment(state, &current, segment).await?
            };
        }

        Ok(current)
    }

    /// Navigate up one level
    fn navigate_up(&self, current: &VfsNode) -> Result<VfsNode> {
        match current {
            VfsNode::Root => Ok(VfsNode::Root),
            VfsNode::Bucket { .. } => Ok(VfsNode::Root),
            VfsNode::Prefix { bucket, prefix } => {
                if prefix.trim_end_matches('/').contains('/') {
                    let parent_prefix = prefix.trim_end_matches('/').rsplit_once('/').unwrap().0;
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
            VfsNode::Object { bucket, key, .. } => {
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
        }
    }

    /// Navigate to a single segment from current node
    async fn navigate_to_segment(
        &self,
        state: &ShellState,
        current: &VfsNode,
        segment: &str,
    ) -> Result<VfsNode> {
        match current {
            VfsNode::Root => Ok(VfsNode::Bucket {
                name: segment.to_string(),
            }),
            VfsNode::Bucket { name } => {
                if let Ok(metadata) = state.s3_client().head_object(name, segment).await {
                    return Ok(VfsNode::Object {
                        bucket: name.clone(),
                        key: segment.to_string(),
                        size: metadata.size,
                    });
                }
                Ok(VfsNode::Prefix {
                    bucket: name.clone(),
                    prefix: format!("{segment}/"),
                })
            }
            VfsNode::Prefix { bucket, prefix } => {
                let full_key = format!("{prefix}{segment}");
                if let Ok(metadata) = state.s3_client().head_object(bucket, &full_key).await {
                    return Ok(VfsNode::Object {
                        bucket: bucket.clone(),
                        key: full_key,
                        size: metadata.size,
                    });
                }
                Ok(VfsNode::Prefix {
                    bucket: bucket.clone(),
                    prefix: format!("{full_key}/"),
                })
            }
            VfsNode::Archive { index, .. } => {
                if let Some(idx) = index
                    && let Some(entry) = idx.find_entry(segment)
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
            VfsNode::ArchiveEntry { archive, path, .. } => {
                if let VfsNode::Archive {
                    index: Some(idx), ..
                } = archive.as_ref()
                {
                    let target_path = if path.is_empty() {
                        segment.to_string()
                    } else {
                        format!("{}/{}", path.trim_end_matches('/'), segment)
                    };

                    if let Some(entry) = idx.find_entry(&target_path)
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
                }
                Err(anyhow!("Path not found"))
            }
            VfsNode::Object { .. } => Err(anyhow!("Cannot navigate into a file")),
        }
    }

    /// Check if a filename should be displayed given an optional filter pattern
    fn should_display(filename: &str, filter_pattern: &Option<String>) -> bool {
        match filter_pattern {
            Some(pattern) => Self::matches_pattern(filename, pattern),
            None => true,
        }
    }

    /// Match a filename against a simple wildcard pattern (* and ?)
    fn matches_pattern(filename: &str, pattern: &str) -> bool {
        let mut name_chars = filename.chars().peekable();
        let mut pattern_chars = pattern.chars().peekable();

        loop {
            match (name_chars.peek(), pattern_chars.peek()) {
                (Some(_), Some('*')) => {
                    pattern_chars.next();
                    // If * is at the end, it matches everything
                    if pattern_chars.peek().is_none() {
                        return true;
                    }
                    // Try to match the rest of the pattern
                    while name_chars.peek().is_some() {
                        if Self::matches_pattern(
                            &name_chars.clone().collect::<String>(),
                            &pattern_chars.clone().collect::<String>(),
                        ) {
                            return true;
                        }
                        name_chars.next();
                    }
                    return false;
                }
                (Some(_n), Some('?')) => {
                    name_chars.next();
                    pattern_chars.next();
                }
                (Some(n), Some(p)) if n == p => {
                    name_chars.next();
                    pattern_chars.next();
                }
                (None, None) => return true,
                _ => return false,
            }
        }
    }
}
