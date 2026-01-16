use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use crate::s3::{S3Client, S3Stream};
use crate::vfs::{ArchiveEntry, ArchiveIndex};

use super::ArchiveHandler;

/// Maximum size to read for the End of Central Directory search (64KB should be enough)
const EOCD_SEARCH_SIZE: u64 = 65536;

/// Minimum signature for EOCD (4 bytes) + minimum EOCD size (18 more bytes)
const MIN_EOCD_SIZE: u64 = 22;

pub struct ZipHandler;

#[async_trait]
impl ArchiveHandler for ZipHandler {
    /// Build an index of all files in the ZIP archive using efficient range requests.
    ///
    /// This function uses only 2 HTTP requests:
    /// 1. Read the end of the file to find the End of Central Directory (EOCD)
    /// 2. Read the central directory to get the file list
    ///
    /// Previously, this used the zip crate which does many seek+read operations,
    /// each triggering a separate HTTP request - very slow for remote files!
    async fn build_index(
        &self,
        s3_client: &Arc<S3Client>,
        bucket: &str,
        key: &str,
    ) -> Result<ArchiveIndex> {
        // Get file size first
        let metadata = s3_client.head_object(bucket, key).await?;
        let size = metadata.size;

        if size < MIN_EOCD_SIZE {
            return Err(anyhow!("File too small to be a valid ZIP archive"));
        }

        // Step 1: Read the end of the file to locate the End of Central Directory (EOCD)
        // We read up to 64KB from the end, which should contain the EOCD even with comments
        let eocd_start = size.saturating_sub(EOCD_SEARCH_SIZE);
        let eocd_length = size - eocd_start;
        let eocd_data = s3_client
            .get_object_range(bucket, key, eocd_start, eocd_length)
            .await
            .context("Failed to read ZIP end of central directory")?;

        // Step 2: Parse the EOCD to find the central directory location
        let eocd_info = find_eocd(&eocd_data)?;

        // Step 3: Read the central directory
        let central_dir_data = s3_client
            .get_object_range(bucket, key, eocd_info.central_dir_offset, eocd_info.central_dir_size)
            .await
            .context("Failed to read ZIP central directory")?;

        // Step 4: Parse central directory headers manually
        let entries = parse_central_directory(&central_dir_data)?;

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

        // For extraction, we still need the zip crate to handle decompression.
        // We download the full file because we need access to both local file
        // headers and compressed data, and the zip crate validates structure.
        //
        // Future optimization: parse local file header at the offset from CD,
        // calculate compressed data location, download just that range,
        // and decompress manually using flate2/bzip2/etc.
        let stream =
            S3Stream::new(Arc::clone(s3_client), bucket.to_string(), key.to_string()).await?;
        let reader = stream.into_sync_reader();

        // Store the index for use in blocking task
        let index_num = match &entry.entry_type {
            crate::vfs::EntryType::Physical { offset } => *offset as usize,
            #[cfg(feature = "parquet")]
            crate::vfs::EntryType::ParquetVirtual { .. } => {
                unreachable!("Zip archives should never contain ParquetVirtual entries")
            }
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

/// Information extracted from the End of Central Directory record
#[derive(Debug)]
struct EocdInfo {
    central_dir_offset: u64,
    central_dir_size: u64,
}

/// Find the End of Central Directory record in the buffer.
fn find_eocd(data: &[u8]) -> Result<EocdInfo> {
    // EOCD signature: 0x06054b50 (little endian)
    const EOCD_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x05, 0x06];

    // Search backwards from the end for the EOCD signature
    // Use inclusive range to handle EOCD at exactly MIN_EOCD_SIZE from end
    for i in (0..=data.len().saturating_sub(MIN_EOCD_SIZE as usize)).rev() {
        if data[i..].starts_with(&EOCD_SIGNATURE) {
            // Found potential EOCD, parse it
            let eocd = &data[i..];

            // Parse EOCD fields (all little endian)
            // Offset 12: Size of central directory (4 bytes)
            // Offset 16: Offset of central directory (4 bytes)
            if eocd.len() < MIN_EOCD_SIZE as usize {
                continue;
            }

            let central_dir_size =
                u32::from_le_bytes([eocd[12], eocd[13], eocd[14], eocd[15]]) as u64;

            let central_dir_offset =
                u32::from_le_bytes([eocd[16], eocd[17], eocd[18], eocd[19]]) as u64;

            return Ok(EocdInfo {
                central_dir_offset,
                central_dir_size,
            });
        }
    }

    Err(anyhow!("Could not find End of Central Directory record"))
}

/// Parse central directory file headers to extract file entries.
///
/// Central Directory File Header format (46 bytes fixed + variable):
/// - 4 bytes: signature (0x02014b50)
/// - 2 bytes: version made by
/// - 2 bytes: version needed to extract
/// - 2 bytes: general purpose bit flag
/// - 2 bytes: compression method
/// - 2 bytes: last mod file time
/// - 2 bytes: last mod file date
/// - 4 bytes: crc-32
/// - 4 bytes: compressed size
/// - 4 bytes: uncompressed size
/// - 2 bytes: file name length
/// - 2 bytes: extra field length
/// - 2 bytes: file comment length
/// - 2 bytes: disk number start
/// - 2 bytes: internal file attributes
/// - 4 bytes: external file attributes
/// - 4 bytes: relative offset of local header
/// - (variable): file name
/// - (variable): extra field
/// - (variable): file comment
fn parse_central_directory(data: &[u8]) -> Result<HashMap<String, ArchiveEntry>> {
    const CDFH_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x01, 0x02];
    const CDFH_MIN_SIZE: usize = 46;

    let mut entries = HashMap::new();
    let mut pos = 0;
    let mut index = 0u64;

    while pos + CDFH_MIN_SIZE <= data.len() {
        // Check for CDFH signature
        if !data[pos..].starts_with(&CDFH_SIGNATURE) {
            // Reached end of central directory entries (could be EOCD or end of data)
            break;
        }

        // Parse fixed-size fields
        let uncompressed_size = u32::from_le_bytes([
            data[pos + 24],
            data[pos + 25],
            data[pos + 26],
            data[pos + 27],
        ]) as u64;

        let filename_len = u16::from_le_bytes([data[pos + 28], data[pos + 29]]) as usize;
        let extra_len = u16::from_le_bytes([data[pos + 30], data[pos + 31]]) as usize;
        let comment_len = u16::from_le_bytes([data[pos + 32], data[pos + 33]]) as usize;

        // Ensure we have enough data for the variable-length fields
        let total_entry_size = CDFH_MIN_SIZE + filename_len + extra_len + comment_len;
        if pos + total_entry_size > data.len() {
            return Err(anyhow!(
                "Truncated central directory entry at position {}",
                pos
            ));
        }

        // Extract filename
        let filename_bytes = &data[pos + CDFH_MIN_SIZE..pos + CDFH_MIN_SIZE + filename_len];
        let filename = String::from_utf8_lossy(filename_bytes).to_string();

        // Determine if it's a directory (ends with /)
        let is_dir = filename.ends_with('/');

        // Create ArchiveEntry - we use 'index' as the offset so extract_file
        // can use zip.by_index() to find the file
        let entry = ArchiveEntry::physical(filename.clone(), index, uncompressed_size, is_dir);

        entries.insert(filename, entry);

        // Move to next entry
        pos += total_entry_size;
        index += 1;
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_eocd_with_valid_signature() {
        // Create a minimal valid EOCD structure
        let mut data = vec![0u8; 100];

        // Place EOCD signature at position 50
        let eocd_pos = 50;
        data[eocd_pos..eocd_pos + 4].copy_from_slice(&[0x50, 0x4b, 0x05, 0x06]);

        // Set central directory size at offset 12 (1000 bytes)
        data[eocd_pos + 12..eocd_pos + 16].copy_from_slice(&1000u32.to_le_bytes());

        // Set central directory offset at offset 16 (5000 bytes from start)
        data[eocd_pos + 16..eocd_pos + 20].copy_from_slice(&5000u32.to_le_bytes());

        let result = find_eocd(&data);
        assert!(result.is_ok());
        let info = result.unwrap();
        assert_eq!(info.central_dir_size, 1000);
        assert_eq!(info.central_dir_offset, 5000);
    }

    #[test]
    fn test_find_eocd_not_found() {
        let data = vec![0u8; 100];
        let result = find_eocd(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_find_eocd_at_exact_min_size_boundary() {
        // Test edge case: EOCD at exactly MIN_EOCD_SIZE (22 bytes) from end
        let mut data = vec![0u8; MIN_EOCD_SIZE as usize];

        data[0..4].copy_from_slice(&[0x50, 0x4b, 0x05, 0x06]);
        data[12..16].copy_from_slice(&500u32.to_le_bytes());
        data[16..20].copy_from_slice(&1000u32.to_le_bytes());

        let result = find_eocd(&data);
        assert!(result.is_ok(), "Should find EOCD at exact MIN_EOCD_SIZE boundary");
        let info = result.unwrap();
        assert_eq!(info.central_dir_size, 500);
        assert_eq!(info.central_dir_offset, 1000);
    }

    #[test]
    fn test_parse_central_directory_single_entry() {
        // Create a minimal CDFH for "test.txt" (8 bytes)
        let mut data = vec![0u8; 46 + 8]; // CDFH header + filename

        // Signature
        data[0..4].copy_from_slice(&[0x50, 0x4b, 0x01, 0x02]);
        // Uncompressed size at offset 24 (100 bytes)
        data[24..28].copy_from_slice(&100u32.to_le_bytes());
        // Filename length at offset 28 (8 bytes)
        data[28..30].copy_from_slice(&8u16.to_le_bytes());
        // Extra field length at offset 30 (0)
        data[30..32].copy_from_slice(&0u16.to_le_bytes());
        // Comment length at offset 32 (0)
        data[32..34].copy_from_slice(&0u16.to_le_bytes());
        // Filename
        data[46..54].copy_from_slice(b"test.txt");

        let entries = parse_central_directory(&data).unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries.contains_key("test.txt"));
        let entry = entries.get("test.txt").unwrap();
        assert_eq!(entry.size, 100);
        assert!(!entry.is_dir);
    }

    #[test]
    fn test_parse_central_directory_directory_entry() {
        // Create a minimal CDFH for "folder/" (7 bytes)
        let mut data = vec![0u8; 46 + 7];

        data[0..4].copy_from_slice(&[0x50, 0x4b, 0x01, 0x02]);
        data[24..28].copy_from_slice(&0u32.to_le_bytes());
        data[28..30].copy_from_slice(&7u16.to_le_bytes());
        data[30..32].copy_from_slice(&0u16.to_le_bytes());
        data[32..34].copy_from_slice(&0u16.to_le_bytes());
        data[46..53].copy_from_slice(b"folder/");

        let entries = parse_central_directory(&data).unwrap();
        assert_eq!(entries.len(), 1);
        let entry = entries.get("folder/").unwrap();
        assert!(entry.is_dir);
    }
}
