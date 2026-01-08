use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use crate::s3::{S3Client, S3Stream};
use crate::vfs::{ArchiveEntry, ArchiveIndex, ArchiveType};

use super::ArchiveHandler;

pub struct TarHandler {
    archive_type: ArchiveType,
}

impl TarHandler {
    pub fn new(archive_type: ArchiveType) -> Self {
        TarHandler { archive_type }
    }
}

#[async_trait]
impl ArchiveHandler for TarHandler {
    async fn build_index(
        &self,
        s3_client: &Arc<S3Client>,
        bucket: &str,
        key: &str,
    ) -> Result<ArchiveIndex> {
        // Create S3 stream
        let stream =
            S3Stream::new(Arc::clone(s3_client), bucket.to_string(), key.to_string()).await?;

        // Use spawn_blocking to run sync tar operations in a blocking thread
        // This prevents the "cannot start runtime within runtime" error
        let archive_type = self.archive_type.clone();
        let entries =
            tokio::task::spawn_blocking(move || -> Result<HashMap<String, ArchiveEntry>> {
                // Create sync reader and decoder inside the blocking task
                let reader = stream.into_sync_reader();
                let reader: Box<dyn Read + Send> = match archive_type {
                    ArchiveType::Tar => Box::new(reader),
                    ArchiveType::TarGz => Box::new(flate2::read::GzDecoder::new(reader)),
                    ArchiveType::TarBz2 => Box::new(bzip2::read::BzDecoder::new(reader)),
                    _ => return Err(anyhow!("Unsupported tar archive type: {archive_type:?}")),
                };

                let mut archive = tar::Archive::new(reader);
                let mut entries = HashMap::new();
                let mut current_offset = 0u64;

                // Iterate through all entries in the tar
                for (index, entry_result) in archive.entries()?.enumerate() {
                    let entry =
                        entry_result.context(format!("Failed to read tar entry {index}"))?;

                    let path = entry
                        .path()
                        .context("Failed to get entry path")?
                        .to_string_lossy()
                        .to_string();

                    let size = entry.size();
                    let is_dir = entry.header().entry_type().is_dir();

                    // For compressed archives, we can't use real offsets
                    // Store the index instead (similar to zip approach)
                    let offset = match archive_type {
                        ArchiveType::Tar => current_offset,
                        _ => index as u64, // For compressed, store entry index
                    };

                    entries.insert(
                        path.clone(),
                        ArchiveEntry::physical(path, offset, size, is_dir),
                    );

                    // Update offset for next entry (512-byte aligned)
                    // Each header is 512 bytes, data is padded to 512-byte blocks
                    if archive_type == ArchiveType::Tar {
                        current_offset += 512; // Header
                        current_offset += size.div_ceil(512) * 512; // Data (rounded up)
                    }
                }

                Ok(entries)
            })
            .await
            .context("Failed to join blocking task")?
            .context("Failed to build tar index")?;

        Ok(ArchiveIndex {
            entries,
            metadata: std::collections::HashMap::new(),
            #[cfg(feature = "parquet")]
            parquet_store: None,
        })
    }

    async fn extract_file(
        &self,
        s3_client: &Arc<S3Client>,
        bucket: &str,
        key: &str,
        index: &ArchiveIndex,
        file_path: &str,
    ) -> Result<Bytes> {
        // Get the entry from the index
        let entry = index
            .entries
            .get(file_path)
            .ok_or_else(|| anyhow!("File not found in archive: {file_path}"))?;

        if entry.is_dir {
            return Err(anyhow!("Cannot extract directory: {file_path}"));
        }

        // Create S3 stream
        let stream =
            S3Stream::new(Arc::clone(s3_client), bucket.to_string(), key.to_string()).await?;

        // Store information needed for extraction
        let target_path = file_path.to_string();
        let archive_type = self.archive_type.clone();
        let entry_offset = match &entry.entry_type {
            crate::vfs::EntryType::Physical { offset } => *offset,
            crate::vfs::EntryType::ParquetVirtual { .. } => {
                unreachable!("Tar archives should never contain ParquetVirtual entries")
            }
        };

        // Use spawn_blocking for sync tar operations
        let buffer = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            // Create sync reader and decoder inside the blocking task
            let reader = stream.into_sync_reader();
            let reader: Box<dyn Read + Send> = match archive_type {
                ArchiveType::Tar => Box::new(reader),
                ArchiveType::TarGz => Box::new(flate2::read::GzDecoder::new(reader)),
                ArchiveType::TarBz2 => Box::new(bzip2::read::BzDecoder::new(reader)),
                _ => return Err(anyhow!("Unsupported tar archive type: {archive_type:?}")),
            };

            let mut archive = tar::Archive::new(reader);

            // For compressed archives, we need to iterate to find the entry
            // For uncompressed tar, we could seek but tar crate doesn't expose that well
            // So we'll iterate for all types for simplicity
            for (index, entry_result) in archive.entries()?.enumerate() {
                let mut entry = entry_result.context("Failed to read tar entry")?;

                let path = entry
                    .path()
                    .context("Failed to get entry path")?
                    .to_string_lossy()
                    .to_string();

                // Check if this is our target entry
                let is_match = match archive_type {
                    ArchiveType::Tar => entry.raw_file_position() == entry_offset,
                    _ => index as u64 == entry_offset,
                };

                if path == target_path || is_match {
                    // Found it! Read the entire file into memory
                    let mut buffer = Vec::new();
                    entry
                        .read_to_end(&mut buffer)
                        .context("Failed to read file from tar")?;
                    return Ok(buffer);
                }
            }

            Err(anyhow!("File not found in tar archive: {target_path}"))
        })
        .await
        .context("Failed to join blocking task")?
        .context("Failed to extract file from tar")?;

        Ok(Bytes::from(buffer))
    }

    fn list_entries<'a>(&self, index: &'a ArchiveIndex, path: &str) -> Vec<&'a ArchiveEntry> {
        let normalized_path = if path.is_empty() || path == "/" {
            ""
        } else {
            path.trim_start_matches('/').trim_end_matches('/')
        };

        let search_prefix = if normalized_path.is_empty() {
            String::new()
        } else {
            format!("{normalized_path}/")
        };

        let mut result = Vec::new();
        let mut seen_dirs = std::collections::HashSet::new();

        for (entry_path, entry) in &index.entries {
            // Skip if not in our directory
            if !search_prefix.is_empty() && !entry_path.starts_with(&search_prefix) {
                continue;
            }

            // Get the relative path from our search prefix
            let relative = if search_prefix.is_empty() {
                entry_path.as_str()
            } else {
                entry_path
                    .strip_prefix(&search_prefix)
                    .unwrap_or(entry_path)
            };

            // Skip if empty (shouldn't happen)
            if relative.is_empty() {
                continue;
            }

            // Check if this is a direct child or a nested entry
            if let Some(slash_pos) = relative.find('/') {
                // This is a nested entry - add the directory part
                let dir_name = &relative[..slash_pos];
                if seen_dirs.insert(dir_name.to_string()) {
                    // We haven't seen this directory yet
                    // Try to find if there's an actual directory entry for it
                    let dir_path = if search_prefix.is_empty() {
                        format!("{dir_name}/")
                    } else {
                        format!("{search_prefix}{dir_name}/")
                    };

                    if let Some(dir_entry) = index.entries.get(&dir_path) {
                        result.push(dir_entry);
                    }
                    // If no explicit directory entry, we could create a virtual one
                    // For now, we'll just skip it
                }
            } else {
                // This is a direct child
                result.push(entry);
            }
        }

        result
    }
}
