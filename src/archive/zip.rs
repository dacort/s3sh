use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use flate2::read::DeflateDecoder;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use crate::s3::{S3Client, S3Stream};
use crate::vfs::{ArchiveEntry, ArchiveIndex, EntryType};

use super::ArchiveHandler;

/// Maximum size to read for the End of Central Directory search (64KB should be enough)
const EOCD_SEARCH_SIZE: u64 = 65536;

/// Minimum size for EOCD (4 bytes signature + 18 bytes data)
const MIN_EOCD_SIZE: usize = 22;

/// Central Directory File Header minimum size (fixed portion)
const CDFH_MIN_SIZE: usize = 46;

/// Local File Header minimum size (fixed portion)
const LOCAL_HEADER_MIN_SIZE: usize = 30;

/// ZIP compression methods
const COMPRESSION_STORED: u16 = 0;
const COMPRESSION_DEFLATE: u16 = 8;

pub struct ZipHandler;

/// Information extracted from the End of Central Directory record
#[derive(Debug)]
struct EocdInfo {
    central_dir_offset: u64,
    central_dir_size: u64,
}

#[async_trait]
impl ArchiveHandler for ZipHandler {
    async fn build_index(
        &self,
        s3_client: &Arc<S3Client>,
        bucket: &str,
        key: &str,
    ) -> Result<ArchiveIndex> {
        // Create S3 stream to get size and make range requests
        let stream =
            S3Stream::new(Arc::clone(s3_client), bucket.to_string(), key.to_string()).await?;

        let size = stream.size();
        if size < MIN_EOCD_SIZE as u64 {
            return Err(anyhow!("File too small to be a valid ZIP archive"));
        }

        // Step 1: Read the end of the file to locate the End of Central Directory (EOCD)
        let tail_size = EOCD_SEARCH_SIZE.min(size);
        let eocd_data = stream.read_tail(tail_size).await?;
        let eocd_start = size - tail_size;

        // Step 2: Parse the EOCD to find the central directory location
        let eocd_info = Self::find_eocd(&eocd_data, eocd_start)?;

        // Step 3: Read the central directory
        let central_dir_data = stream
            .read_range(eocd_info.central_dir_offset, eocd_info.central_dir_size)
            .await
            .context("Failed to read ZIP central directory")?;

        // Step 4: Parse central directory headers to build the index
        let entries = Self::parse_central_directory(&central_dir_data)?;

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

        // Extract ZIP-specific metadata
        let (local_header_offset, compressed_size, compression_method) = match &entry.entry_type {
            EntryType::ZipEntry {
                local_header_offset,
                compressed_size,
                compression_method,
            } => (*local_header_offset, *compressed_size, *compression_method),
            _ => {
                return Err(anyhow!(
                    "Invalid entry type for ZIP extraction: {file_path}"
                ));
            }
        };

        // Create S3 stream for range requests
        let stream =
            S3Stream::new(Arc::clone(s3_client), bucket.to_string(), key.to_string()).await?;

        // Read the local file header to get the actual data offset
        // Local header has variable-length filename and extra fields
        let local_header = stream
            .read_range(local_header_offset, LOCAL_HEADER_MIN_SIZE as u64)
            .await
            .context("Failed to read local file header")?;

        // Verify local header signature
        if local_header.len() < LOCAL_HEADER_MIN_SIZE
            || !local_header.starts_with(&[0x50, 0x4b, 0x03, 0x04])
        {
            return Err(anyhow!("Invalid local file header"));
        }

        // Get filename length (offset 26) and extra field length (offset 28)
        let filename_len = u16::from_le_bytes([local_header[26], local_header[27]]) as u64;
        let extra_len = u16::from_le_bytes([local_header[28], local_header[29]]) as u64;

        // Calculate actual data offset
        let data_offset =
            local_header_offset + LOCAL_HEADER_MIN_SIZE as u64 + filename_len + extra_len;

        // Read just the compressed data
        let compressed_data = if compressed_size > 0 {
            stream
                .read_range(data_offset, compressed_size)
                .await
                .context("Failed to read compressed file data")?
        } else {
            Bytes::new()
        };

        // Decompress based on compression method
        let decompressed = match compression_method {
            COMPRESSION_STORED => {
                // For stored entries, compressed and uncompressed sizes must match.
                if compressed_size != entry.size {
                    return Err(anyhow!(
                        "Invalid ZIP entry: stored file has mismatched sizes (compressed_size={}, entry.size={})",
                        compressed_size,
                        entry.size
                    ));
                }

                if compressed_data.len() as u64 != compressed_size {
                    return Err(anyhow!(
                        "Invalid ZIP entry: stored file data length ({}) does not match expected size ({})",
                        compressed_data.len(),
                        compressed_size
                    ));
                }

                compressed_data.to_vec()
            }
            COMPRESSION_DEFLATE => {
                let mut decoder = DeflateDecoder::new(&compressed_data[..]);
                let mut decompressed = Vec::with_capacity(entry.size as usize);
                decoder
                    .read_to_end(&mut decompressed)
                    .context("Failed to decompress deflate data")?;
                decompressed
            }
            other => {
                return Err(anyhow!(
                    "Unsupported compression method: {}. Only stored (0) and deflate (8) are supported.",
                    other
                ));
            }
        };

        Ok(Bytes::from(decompressed))
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

    /// Find the End of Central Directory record in the buffer.
    /// Returns information about the central directory location.
    fn find_eocd(data: &[u8], _buffer_start_offset: u64) -> Result<EocdInfo> {
        // EOCD signature: 0x06054b50 (little endian)
        const EOCD_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x05, 0x06];

        // Search backwards from the end for the EOCD signature
        // Use inclusive range to handle EOCD at exactly MIN_EOCD_SIZE from end
        for i in (0..=data.len().saturating_sub(MIN_EOCD_SIZE)).rev() {
            if data[i..].starts_with(&EOCD_SIGNATURE) {
                let eocd = &data[i..];

                if eocd.len() < MIN_EOCD_SIZE {
                    continue;
                }

                // Parse EOCD fields (all little endian)
                // Offset 12: Size of central directory (4 bytes)
                // Offset 16: Offset of central directory (4 bytes)
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

        let mut entries = HashMap::new();
        let mut pos = 0;

        while pos + CDFH_MIN_SIZE <= data.len() {
            // Check for CDFH signature
            if !data[pos..].starts_with(&CDFH_SIGNATURE) {
                // Reached end of central directory entries
                break;
            }

            // Parse compression method (offset 10)
            let compression_method = u16::from_le_bytes([data[pos + 10], data[pos + 11]]);

            // Parse compressed size (offset 20)
            let compressed_size = u32::from_le_bytes([
                data[pos + 20],
                data[pos + 21],
                data[pos + 22],
                data[pos + 23],
            ]) as u64;

            // Parse uncompressed size (offset 24)
            let uncompressed_size = u32::from_le_bytes([
                data[pos + 24],
                data[pos + 25],
                data[pos + 26],
                data[pos + 27],
            ]) as u64;

            // Parse lengths (offsets 28, 30, 32)
            let filename_len = u16::from_le_bytes([data[pos + 28], data[pos + 29]]) as usize;
            let extra_len = u16::from_le_bytes([data[pos + 30], data[pos + 31]]) as usize;
            let comment_len = u16::from_le_bytes([data[pos + 32], data[pos + 33]]) as usize;

            // Parse local header offset (offset 42)
            let local_header_offset = u32::from_le_bytes([
                data[pos + 42],
                data[pos + 43],
                data[pos + 44],
                data[pos + 45],
            ]) as u64;

            // Ensure we have enough data for the variable-length fields
            let total_entry_size = CDFH_MIN_SIZE
                .checked_add(filename_len)
                .and_then(|v| v.checked_add(extra_len))
                .and_then(|v| v.checked_add(comment_len))
                .ok_or_else(|| {
                    anyhow!(
                        "Central directory entry size overflow at position {}",
                        pos
                    )
                })?;

            let end = pos
                .checked_add(total_entry_size)
                .ok_or_else(|| {
                    anyhow!(
                        "Central directory entry position overflow at position {}",
                        pos
                    )
                })?;

            if end > data.len() {
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

            // Create the entry
            let entry = ArchiveEntry::zip_entry(
                filename.clone(),
                uncompressed_size,
                is_dir,
                local_header_offset,
                compressed_size,
                compression_method,
            );

            entries.insert(filename, entry);

            // Move to next entry
            pos += total_entry_size;
        }

        Ok(entries)
    }
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

        let result = ZipHandler::find_eocd(&data, 0);

        assert!(result.is_ok());
        let info = result.unwrap();
        assert_eq!(info.central_dir_size, 1000);
        assert_eq!(info.central_dir_offset, 5000);
    }

    #[test]
    fn test_find_eocd_not_found() {
        let data = vec![0u8; 100];
        let result = ZipHandler::find_eocd(&data, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_find_eocd_at_exact_min_size_boundary() {
        // Test edge case: EOCD at exactly MIN_EOCD_SIZE from end
        let mut data = vec![0u8; MIN_EOCD_SIZE];

        // Place EOCD signature at position 0
        data[0..4].copy_from_slice(&[0x50, 0x4b, 0x05, 0x06]);
        data[12..16].copy_from_slice(&500u32.to_le_bytes());
        data[16..20].copy_from_slice(&1000u32.to_le_bytes());

        let result = ZipHandler::find_eocd(&data, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_central_directory_single_entry() {
        // Create a minimal central directory with one entry
        let mut data = vec![0u8; 100];

        // CDFH signature
        data[0..4].copy_from_slice(&[0x50, 0x4b, 0x01, 0x02]);

        // Compression method (offset 10): deflate = 8
        data[10..12].copy_from_slice(&8u16.to_le_bytes());

        // Compressed size (offset 20): 500
        data[20..24].copy_from_slice(&500u32.to_le_bytes());

        // Uncompressed size (offset 24): 1000
        data[24..28].copy_from_slice(&1000u32.to_le_bytes());

        // Filename length (offset 28): 8
        data[28..30].copy_from_slice(&8u16.to_le_bytes());

        // Extra field length (offset 30): 0
        data[30..32].copy_from_slice(&0u16.to_le_bytes());

        // Comment length (offset 32): 0
        data[32..34].copy_from_slice(&0u16.to_le_bytes());

        // Local header offset (offset 42): 100
        data[42..46].copy_from_slice(&100u32.to_le_bytes());

        // Filename at offset 46: "test.txt"
        data[46..54].copy_from_slice(b"test.txt");

        let result = ZipHandler::parse_central_directory(&data);
        assert!(result.is_ok());

        let entries = result.unwrap();
        assert_eq!(entries.len(), 1);

        let entry = entries.get("test.txt").unwrap();
        assert_eq!(entry.path, "test.txt");
        assert_eq!(entry.size, 1000);
        assert!(!entry.is_dir);

        if let EntryType::ZipEntry {
            local_header_offset,
            compressed_size,
            compression_method,
        } = &entry.entry_type
        {
            assert_eq!(*local_header_offset, 100);
            assert_eq!(*compressed_size, 500);
            assert_eq!(*compression_method, 8);
        } else {
            panic!("Expected ZipEntry type");
        }
    }

    #[test]
    fn test_parse_central_directory_directory_entry() {
        let mut data = vec![0u8; 60];

        // CDFH signature
        data[0..4].copy_from_slice(&[0x50, 0x4b, 0x01, 0x02]);

        // Compression method: stored
        data[10..12].copy_from_slice(&0u16.to_le_bytes());

        // Sizes: 0
        data[20..24].copy_from_slice(&0u32.to_le_bytes());
        data[24..28].copy_from_slice(&0u32.to_le_bytes());

        // Filename length: 5 ("dir/")
        data[28..30].copy_from_slice(&4u16.to_le_bytes());
        data[30..32].copy_from_slice(&0u16.to_le_bytes());
        data[32..34].copy_from_slice(&0u16.to_le_bytes());

        // Local header offset
        data[42..46].copy_from_slice(&0u32.to_le_bytes());

        // Filename: "dir/"
        data[46..50].copy_from_slice(b"dir/");

        let entries = ZipHandler::parse_central_directory(&data).unwrap();
        let entry = entries.get("dir/").unwrap();
        assert!(entry.is_dir);
    }
}
