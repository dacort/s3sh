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
        let entries = match self.archive_type {
            ArchiveType::Tar => {
                // Uncompressed tar - use range requests to skip file content without downloading
                stream_list_tar_with_range_requests(s3_client, bucket, key).await?
            }
            ArchiveType::TarGz | ArchiveType::TarBz2 => {
                // Compressed archives must be fully downloaded and decompressed
                // because compression formats like gzip/bzip2 are not seekable
                let byte_stream = s3_client.get_object_stream(bucket, key).await?;
                let reader = byte_stream.into_async_read();

                match self.archive_type {
                    ArchiveType::TarGz => {
                        let gz = GzipDecoder::new(tokio::io::BufReader::new(reader));
                        stream_list_tar(gz).await?
                    }
                    ArchiveType::TarBz2 => {
                        let bz = BzDecoder::new(tokio::io::BufReader::new(reader));
                        stream_list_tar(bz).await?
                    }
                    _ => unreachable!(),
                }
            }
            _ => return Err(anyhow!("Unsupported tar archive type: {:?}", self.archive_type)),
        };

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

/// Stream tar headers from S3 using range requests to skip file content
/// This is more efficient for uncompressed tar files as it only downloads headers
async fn stream_list_tar_with_range_requests(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<HashMap<String, ArchiveEntry>> {
    let mut entries = HashMap::new();
    let mut current_offset = 0u64;
    let mut zero_blocks = 0u8;

    loop {
        // Read next 512-byte header using range request
        let header_bytes = s3_client
            .get_object_range(bucket, key, current_offset, TAR_BLOCK as u64)
            .await;

        // Handle end of file
        let header_bytes = match header_bytes {
            Ok(bytes) => bytes,
            Err(_) => {
                // EOF - if we have entries, that's ok
                if !entries.is_empty() {
                    break;
                }
                return Err(anyhow!("Failed to read tar header at offset {}", current_offset));
            }
        };

        // Check if we got a full header
        if header_bytes.len() < TAR_BLOCK {
            break;
        }

        let header: [u8; TAR_BLOCK] = header_bytes.as_ref()[..TAR_BLOCK]
            .try_into()
            .map_err(|_| anyhow!("Failed to convert header bytes"))?;

        // Check for end-of-archive marker (two consecutive zero blocks)
        if header.iter().all(|&b| b == 0) {
            zero_blocks += 1;
            if zero_blocks >= 2 {
                break;
            }
            current_offset += TAR_BLOCK as u64;
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

        // Move to next header (skip current header + file content)
        current_offset += TAR_BLOCK as u64; // Header
        current_offset += round_up_512(size); // File content (padded)
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
}
