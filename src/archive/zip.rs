use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use crate::s3::{S3Client, S3Stream};
use crate::vfs::{ArchiveEntry, ArchiveIndex};

use super::ArchiveHandler;

pub struct ZipHandler;

#[async_trait]
impl ArchiveHandler for ZipHandler {
    async fn build_index(
        &self,
        s3_client: &Arc<S3Client>,
        bucket: &str,
        key: &str,
    ) -> Result<ArchiveIndex> {
        // Create S3 stream
        let stream =
            S3Stream::new(Arc::clone(s3_client), bucket.to_string(), key.to_string()).await?;
        let reader = stream.into_sync_reader();

        // Use spawn_blocking to run sync zip operations in a blocking thread
        // This prevents the "cannot start runtime within runtime" error
        let entries =
            tokio::task::spawn_blocking(move || -> Result<HashMap<String, ArchiveEntry>> {
                // Use the zip crate to read the archive
                let mut zip = zip::ZipArchive::new(reader).context("Failed to open zip archive")?;

                let mut entries = HashMap::new();

                // Iterate through all files in the zip
                for i in 0..zip.len() {
                    let file = zip
                        .by_index(i)
                        .context(format!("Failed to read zip entry {i}"))?;

                    let name = file.name().to_string();
                    let is_dir = file.is_dir();
                    let size = file.size();

                    // Get the offset of this entry
                    // The zip crate doesn't expose offsets directly, but we can work around it
                    // For now, we'll store the index and calculate offsets when extracting
                    entries.insert(
                        name.clone(),
                        ArchiveEntry::physical(name, i as u64, size, is_dir),
                    );
                }

                Ok(entries)
            })
            .await
            .context("Failed to join blocking task")?
            .context("Failed to build zip index")?;

        Ok(ArchiveIndex {
            entries,
            metadata: std::collections::HashMap::new(),
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

        // Create a new stream for extraction
        let stream =
            S3Stream::new(Arc::clone(s3_client), bucket.to_string(), key.to_string()).await?;
        let reader = stream.into_sync_reader();

        // Store the index for use in blocking task
        let index_num = match &entry.entry_type {
            crate::vfs::EntryType::Physical { offset } => *offset as usize,
            _ => return Err(anyhow!("Invalid entry type for zip handler")),
        };

        // Use spawn_blocking to run sync zip operations in a blocking thread
        let buffer = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            let mut zip = zip::ZipArchive::new(reader).context("Failed to open zip archive")?;

            // Extract by index (stored in offset field)
            let mut file = zip
                .by_index(index_num)
                .context(format!("Failed to read zip entry at index {index_num}"))?;

            // Read the entire file into memory
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)
                .context("Failed to read file from zip")?;

            Ok(buffer)
        })
        .await
        .context("Failed to join blocking task")?
        .context("Failed to extract file from zip")?;

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
                    } else {
                        // Directory entry doesn't exist explicitly, we could create a virtual one
                        // For now, skip it as we'll show the files
                    }
                }
            } else {
                // This is a direct child
                result.push(entry);
            }
        }

        result
    }
}

impl Default for ZipHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl ZipHandler {
    pub fn new() -> Self {
        ZipHandler
    }
}
