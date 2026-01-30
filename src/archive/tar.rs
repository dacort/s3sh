use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};
use async_compression::tokio::bufread::{GzipDecoder, BzDecoder};

use crate::s3::{S3Client, S3Stream};
use crate::vfs::{ArchiveEntry, ArchiveIndex, ArchiveType};

use super::ArchiveHandler;

const TAR_BLOCK: usize = 512;

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
        // Get streaming byte stream from S3
        let byte_stream = s3_client.get_object_stream(bucket, key).await?;

        // Convert ByteStream to AsyncRead
        let reader = byte_stream.into_async_read();

        // Wrap reader based on archive type
        let mut entries = match self.archive_type {
            ArchiveType::Tar => {
                // Uncompressed tar - stream directly
                stream_list_tar(reader).await?
            }
            ArchiveType::TarGz => {
                // Gzip compressed - use streaming decompression
                let gz = GzipDecoder::new(tokio::io::BufReader::new(reader));
                stream_list_tar(gz).await?
            }
            ArchiveType::TarBz2 => {
                // Bzip2 compressed - use streaming decompression
                let bz = BzDecoder::new(tokio::io::BufReader::new(reader));
                stream_list_tar(bz).await?
            }
            _ => return Err(anyhow!("Unsupported tar archive type: {:?}", self.archive_type)),
        };

        // Add virtual directory entries for any implied directories
        // Many tar files don't include explicit directory entries, only file entries
        let mut virtual_dirs = std::collections::HashSet::new();

        // Collect all directory paths that should exist based on file paths
        for (path, entry) in &entries {
            // For files, only process parent directories (not the file itself)
            // For directories, they already end with '/' so we process their parents
            let path_to_process = if entry.is_dir {
                // Directory paths end with '/', strip it to get the dir name
                path.trim_end_matches('/')
            } else {
                // For files, get the parent path
                if let Some(last_slash) = path.rfind('/') {
                    &path[..last_slash]
                } else {
                    // File is at root, no parent directories to add
                    continue;
                }
            };

            // Now build all parent directory paths
            let mut current_path = String::new();
            for component in path_to_process.split('/') {
                if component.is_empty() {
                    continue;
                }
                if !current_path.is_empty() {
                    current_path.push('/');
                }
                current_path.push_str(component);
                let dir_path = format!("{}/", current_path);

                // Only add if not already in entries
                if !entries.contains_key(&dir_path) {
                    virtual_dirs.insert(dir_path);
                }
            }
        }

        // Add virtual directory entries to the index
        for dir_path in virtual_dirs {
            entries.insert(
                dir_path.clone(),
                ArchiveEntry::physical(dir_path, 0, 0, true),
            );
        }

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
            #[cfg(feature = "parquet")]
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

// Helper functions for streaming tar parsing

/// Parse a null-terminated C string from a tar header field
fn parse_cstr(field: &[u8]) -> String {
    let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
    String::from_utf8_lossy(&field[..end]).trim().to_string()
}

/// Parse an octal number from a tar header field
fn parse_octal_u64(field: &[u8]) -> Option<u64> {
    let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
    let s = String::from_utf8_lossy(&field[..end]).trim().to_string();
    if s.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(&s, 8).ok()
}

/// Round up to next 512-byte boundary
fn round_up_512(n: u64) -> u64 {
    if n == 0 { 0 } else { ((n + 511) / 512) * 512 }
}

/// Skip exactly n bytes from an async reader
/// Note: We use read-and-discard instead of seek because:
/// 1. Compressed streams (gzip, bzip2) don't support seeking
/// 2. S3 ByteStream may not support efficient seeking
/// 3. This works consistently across all archive types
async fn skip_exact<R: AsyncRead + Unpin>(r: &mut R, mut n: u64) -> Result<()> {
    let mut buf = [0u8; 64 * 1024];
    while n > 0 {
        let want = std::cmp::min(n as usize, buf.len());
        r.read_exact(&mut buf[..want]).await
            .map_err(|e| anyhow!("EOF while skipping tar payload: {e}"))?;
        n -= want as u64;
    }
    Ok(())
}

/// Stream tar headers from an async reader without reading file contents
async fn stream_list_tar<R: AsyncRead + Unpin>(mut r: R) -> Result<HashMap<String, ArchiveEntry>> {
    let mut header = [0u8; TAR_BLOCK];
    let mut zero_blocks = 0u8;
    let mut entries = HashMap::new();
    let mut current_offset = 0u64;

    loop {
        // Read next 512-byte header
        if let Err(e) = r.read_exact(&mut header).await {
            // Handle incomplete archives gracefully:
            // While tar spec requires two zero blocks at the end, we allow incomplete
            // archives if we've already found entries. This improves robustness when
            // dealing with truncated or streaming archives.
            if !entries.is_empty() {
                break;
            }
            return Err(anyhow!("EOF while reading tar header: {e}"));
        }

        // Check for end-of-archive marker (two consecutive zero blocks)
        if header.iter().all(|&b| b == 0) {
            zero_blocks += 1;
            if zero_blocks >= 2 {
                break;
            }
            continue;
        } else {
            zero_blocks = 0;
        }

        // Parse header fields
        let name = parse_cstr(&header[0..100]);
        let prefix = parse_cstr(&header[345..500]);
        let path = if !prefix.is_empty() { 
            format!("{}/{}", prefix, name) 
        } else { 
            name 
        };

        let size = parse_octal_u64(&header[124..136])
            .ok_or_else(|| anyhow!("bad size field for entry {path:?}"))?;

        let typeflag = header[156] as char;
        let is_dir = typeflag == '5' || path.ends_with('/');

        // Store the entry
        entries.insert(
            path.clone(),
            ArchiveEntry::physical(path, current_offset, size, is_dir),
        );

        // Update offset for next entry (512-byte header + padded data)
        current_offset += 512; // Header
        current_offset += round_up_512(size); // Data (rounded up to 512 boundary)

        // Skip payload without reading it into memory
        let to_skip = round_up_512(size);
        skip_exact(&mut r, to_skip).await?;
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cstr() {
        let field = b"test.txt\0\0\0\0";
        assert_eq!(parse_cstr(field), "test.txt");
        
        let field = b"test\0";
        assert_eq!(parse_cstr(field), "test");
        
        let field = b"\0\0\0\0";
        assert_eq!(parse_cstr(field), "");
    }

    #[test]
    fn test_parse_octal_u64() {
        // Standard octal number
        let field = b"0000644\0";
        assert_eq!(parse_octal_u64(field), Some(420));
        
        // Empty field
        let field = b"\0\0\0\0";
        assert_eq!(parse_octal_u64(field), Some(0));
        
        // File size example (100 bytes)
        let field = b"0000144\0";
        assert_eq!(parse_octal_u64(field), Some(100));
        
        // Large file size (1MB = 1048576 bytes = 0o4000000)
        let field = b"04000000\0";
        assert_eq!(parse_octal_u64(field), Some(1048576));
    }

    #[test]
    fn test_round_up_512() {
        assert_eq!(round_up_512(0), 0);
        assert_eq!(round_up_512(1), 512);
        assert_eq!(round_up_512(512), 512);
        assert_eq!(round_up_512(513), 1024);
        assert_eq!(round_up_512(1000), 1024);
        assert_eq!(round_up_512(1024), 1024);
    }

    #[tokio::test]
    async fn test_stream_list_tar_empty() {
        // Create an empty tar (two zero blocks)
        let data = vec![0u8; 1024];
        let result = stream_list_tar(data.as_slice()).await;
        assert!(result.is_ok());
        let entries = result.unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_virtual_directories_with_explicit_dir_entry() {
        // Simulate an index with explicit directory entry (like wordpress-2.3.tar.gz)
        let mut entries = HashMap::new();
        entries.insert(
            "wordpress/".to_string(),
            ArchiveEntry::physical("wordpress/".to_string(), 0, 0, true),
        );
        entries.insert(
            "wordpress/index.php".to_string(),
            ArchiveEntry::physical("wordpress/index.php".to_string(), 512, 100, false),
        );

        let index = ArchiveIndex {
            entries,
            metadata: HashMap::new(),
            #[cfg(feature = "parquet")]
            parquet_store: None,
        };

        let handler = TarHandler::new(ArchiveType::Tar);
        let results = handler.list_entries(&index, "");

        // Should find the explicit wordpress/ directory
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].path, "wordpress/");
        assert!(results[0].is_dir);
    }

    #[test]
    fn test_virtual_directories_without_explicit_dir_entry() {
        // Simulate an index without explicit directory entry (like gallery-2.2.3-full.tar.gz)
        let mut entries = HashMap::new();
        entries.insert(
            "gallery2/index.php".to_string(),
            ArchiveEntry::physical("gallery2/index.php".to_string(), 0, 100, false),
        );
        entries.insert(
            "gallery2/themes/default.css".to_string(),
            ArchiveEntry::physical("gallery2/themes/default.css".to_string(), 1024, 200, false),
        );

        let index = ArchiveIndex {
            entries,
            metadata: HashMap::new(),
            #[cfg(feature = "parquet")]
            parquet_store: None,
        };

        let handler = TarHandler::new(ArchiveType::Tar);
        let results = handler.list_entries(&index, "");

        // Should find NO entries because there's no explicit directory entry
        // and we haven't added virtual directories yet
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_virtual_directories_added_during_build() {
        // Test that virtual directories are added when building the index
        use crate::vfs::ArchiveType;

        let mut entries = HashMap::new();
        // Files without explicit directory entries
        entries.insert(
            "gallery2/index.php".to_string(),
            ArchiveEntry::physical("gallery2/index.php".to_string(), 0, 100, false),
        );
        entries.insert(
            "gallery2/themes/default.css".to_string(),
            ArchiveEntry::physical("gallery2/themes/default.css".to_string(), 1024, 200, false),
        );

        // Simulate the virtual directory creation logic from build_index
        let mut virtual_dirs = std::collections::HashSet::new();
        for (path, entry) in &entries {
            let path_to_process = if entry.is_dir {
                path.trim_end_matches('/')
            } else {
                if let Some(last_slash) = path.rfind('/') {
                    &path[..last_slash]
                } else {
                    continue;
                }
            };

            let mut current_path = String::new();
            for component in path_to_process.split('/') {
                if component.is_empty() {
                    continue;
                }
                if !current_path.is_empty() {
                    current_path.push('/');
                }
                current_path.push_str(component);
                let dir_path = format!("{}/", current_path);

                if !entries.contains_key(&dir_path) {
                    virtual_dirs.insert(dir_path);
                }
            }
        }

        // Add virtual directories
        for dir_path in virtual_dirs {
            entries.insert(
                dir_path.clone(),
                ArchiveEntry::physical(dir_path, 0, 0, true),
            );
        }

        let index = ArchiveIndex {
            entries,
            metadata: HashMap::new(),
            #[cfg(feature = "parquet")]
            parquet_store: None,
        };

        let handler = TarHandler::new(ArchiveType::Tar);

        // List root - should now find gallery2/
        let results = handler.list_entries(&index, "");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].path, "gallery2/");
        assert!(results[0].is_dir);

        // List gallery2/ - should find themes/ and index.php
        let results = handler.list_entries(&index, "gallery2");
        assert_eq!(results.len(), 2);

        // Find the directory and file
        let has_themes = results.iter().any(|e| e.path == "gallery2/themes/" && e.is_dir);
        let has_index = results.iter().any(|e| e.path == "gallery2/index.php" && !e.is_dir);
        assert!(has_themes, "Should have gallery2/themes/ directory");
        assert!(has_index, "Should have gallery2/index.php file");
    }
}
