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

/// Maximum size to read for the End of Central Directory search.
/// Must be at least 65535 (max comment) + 22 (EOCD) = 65557 bytes.
const EOCD_SEARCH_SIZE: u64 = 66000;

/// Minimum size for EOCD (4 bytes signature + 18 bytes data)
const MIN_EOCD_SIZE: usize = 22;

/// Central Directory File Header minimum size (fixed portion)
const CDFH_MIN_SIZE: usize = 46;

/// Local File Header minimum size (fixed portion)
const LOCAL_HEADER_MIN_SIZE: usize = 30;

/// ZIP compression methods
const COMPRESSION_STORED: u16 = 0;
const COMPRESSION_DEFLATE: u16 = 8;

/// Maximum allowed decompressed size (1GB) to prevent zip bombs
const MAX_DECOMPRESSED_SIZE: u64 = 1024 * 1024 * 1024;

/// Maximum compression ratio allowed (1000:1) to detect zip bombs
const MAX_COMPRESSION_RATIO: u64 = 1000;

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

        // Step 2: Parse the EOCD to find the central directory location
        let eocd_info = Self::find_eocd(&eocd_data)?;

        // Validate central directory bounds
        let cd_end = eocd_info
            .central_dir_offset
            .checked_add(eocd_info.central_dir_size)
            .ok_or_else(|| anyhow!("Central directory bounds overflow"))?;

        if cd_end > size {
            return Err(anyhow!(
                "Central directory extends beyond file bounds (offset {} + size {} > file size {})",
                eocd_info.central_dir_offset,
                eocd_info.central_dir_size,
                size
            ));
        }

        // Step 3: Read the central directory
        let central_dir_data = stream
            .read_range(eocd_info.central_dir_offset, eocd_info.central_dir_size)
            .await
            .context("Failed to read ZIP central directory")?;

        // Step 4: Parse central directory headers to build the index
        let entries = Self::parse_central_directory(&central_dir_data, size)?;

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
        let (local_header_offset, compressed_size, compression_method, expected_crc32) =
            match &entry.entry_type {
                EntryType::ZipEntry {
                    local_header_offset,
                    compressed_size,
                    compression_method,
                    crc32,
                } => (*local_header_offset, *compressed_size, *compression_method, *crc32),
                _ => {
                    return Err(anyhow!(
                        "Invalid entry type for ZIP extraction: {file_path}"
                    ));
                }
            };

        // Zip bomb protection: check decompressed size limit
        if entry.size > MAX_DECOMPRESSED_SIZE {
            return Err(anyhow!(
                "File too large to extract safely: {} bytes exceeds {} byte limit",
                entry.size,
                MAX_DECOMPRESSED_SIZE
            ));
        }

        // Zip bomb protection: check compression ratio
        if compressed_size > 0 && entry.size / compressed_size > MAX_COMPRESSION_RATIO {
            return Err(anyhow!(
                "Suspicious compression ratio detected ({:.0}:1). File may be a zip bomb.",
                entry.size as f64 / compressed_size as f64
            ));
        }

        // Create S3 stream for range requests
        let stream =
            S3Stream::new(Arc::clone(s3_client), bucket.to_string(), key.to_string()).await?;

        let file_size = stream.size();

        // Validate local header offset is within bounds
        if local_header_offset >= file_size {
            return Err(anyhow!(
                "Invalid local header offset {} for file size {}",
                local_header_offset,
                file_size
            ));
        }

        // Read the local file header to get the actual data offset
        let local_header = stream
            .read_range(local_header_offset, LOCAL_HEADER_MIN_SIZE as u64)
            .await
            .context("Failed to read local file header")?;

        // Verify local header signature
        if local_header.len() < LOCAL_HEADER_MIN_SIZE
            || !local_header.starts_with(&[0x50, 0x4b, 0x03, 0x04])
        {
            return Err(anyhow!("Invalid local file header signature"));
        }

        // Get filename length (offset 26) and extra field length (offset 28)
        let filename_len = u16::from_le_bytes([local_header[26], local_header[27]]) as u64;
        let extra_len = u16::from_le_bytes([local_header[28], local_header[29]]) as u64;

        // Calculate actual data offset with overflow protection
        let data_offset = local_header_offset
            .checked_add(LOCAL_HEADER_MIN_SIZE as u64)
            .and_then(|v| v.checked_add(filename_len))
            .and_then(|v| v.checked_add(extra_len))
            .ok_or_else(|| anyhow!("Data offset calculation overflow"))?;

        // Validate data range is within file bounds
        let data_end = data_offset
            .checked_add(compressed_size)
            .ok_or_else(|| anyhow!("Data end calculation overflow"))?;

        if data_end > file_size {
            return Err(anyhow!(
                "Compressed data extends beyond file bounds (offset {} + size {} > file size {})",
                data_offset,
                compressed_size,
                file_size
            ));
        }

        // Handle empty files
        if compressed_size == 0 {
            if entry.size != 0 {
                return Err(anyhow!(
                    "Invalid ZIP entry: compressed size is 0 but uncompressed size is {}",
                    entry.size
                ));
            }
            // Empty file - verify CRC (should be 0 for empty data)
            if expected_crc32 != 0 {
                return Err(anyhow!(
                    "Invalid ZIP entry: empty file has non-zero CRC-32 (expected 0, got {:#010x})",
                    expected_crc32
                ));
            }
            return Ok(Bytes::new());
        }

        // Read the compressed data
        let compressed_data = stream
            .read_range(data_offset, compressed_size)
            .await
            .context("Failed to read compressed file data")?;

        // Decompress based on compression method
        let decompressed = match compression_method {
            COMPRESSION_STORED => {
                // For stored entries, compressed and uncompressed sizes must match
                if compressed_size != entry.size {
                    return Err(anyhow!(
                        "Invalid ZIP entry: stored file has mismatched sizes (compressed={}, uncompressed={})",
                        compressed_size,
                        entry.size
                    ));
                }
                compressed_data.to_vec()
            }
            COMPRESSION_DEFLATE => {
                let mut decoder = DeflateDecoder::new(&compressed_data[..]);
                // Use bounded reads to prevent memory exhaustion from malicious input
                let max_allowed = MAX_DECOMPRESSED_SIZE.min(entry.size);
                let capacity = max_allowed as usize;
                let mut decompressed = Vec::with_capacity(capacity);
                let mut buffer = [0u8; 8192];
                let mut total_decompressed: u64 = 0;

                loop {
                    let bytes_read = decoder
                        .read(&mut buffer)
                        .context("Failed to decompress deflate data")?;
                    if bytes_read == 0 {
                        break;
                    }

                    // Check against maximum allowed size before extending buffer
                    total_decompressed = total_decompressed
                        .checked_add(bytes_read as u64)
                        .ok_or_else(|| anyhow!("Decompressed data size overflow"))?;

                    if total_decompressed > max_allowed {
                        return Err(anyhow!(
                            "Decompressed data exceeds maximum allowed size of {} bytes",
                            max_allowed
                        ));
                    }

                    decompressed.extend_from_slice(&buffer[..bytes_read]);
                }
                decompressed
            }
            other => {
                return Err(anyhow!(
                    "Unsupported compression method: {}. Only stored (0) and deflate (8) are supported.",
                    other
                ));
            }
        };

        // Verify decompressed size matches expected
        if decompressed.len() as u64 != entry.size {
            return Err(anyhow!(
                "Decompressed size mismatch: expected {} bytes, got {} bytes",
                entry.size,
                decompressed.len()
            ));
        }

        // Verify CRC-32 checksum using SIMD-accelerated crc32fast
        let actual_crc32 = crc32fast::hash(&decompressed);
        if actual_crc32 != expected_crc32 {
            return Err(anyhow!(
                "CRC-32 checksum mismatch: expected {:#010x}, got {:#010x}. File may be corrupted.",
                expected_crc32,
                actual_crc32
            ));
        }

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
    fn find_eocd(data: &[u8]) -> Result<EocdInfo> {
        // EOCD signature: 0x06054b50 (little endian)
        const EOCD_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x05, 0x06];

        // Search backwards from the end for the EOCD signature
        for i in (0..=data.len().saturating_sub(MIN_EOCD_SIZE)).rev() {
            if data[i..].starts_with(&EOCD_SIGNATURE) {
                let eocd = &data[i..];

                if eocd.len() < MIN_EOCD_SIZE {
                    continue;
                }

                // Check for multi-disk archives (not supported)
                // Disk number (offset 4) and disk with CD start (offset 6)
                let disk_number = u16::from_le_bytes([eocd[4], eocd[5]]);
                let disk_with_cd = u16::from_le_bytes([eocd[6], eocd[7]]);

                if disk_number != 0 || disk_with_cd != 0 {
                    return Err(anyhow!(
                        "Multi-disk ZIP archives are not supported (disk {}, CD disk {})",
                        disk_number,
                        disk_with_cd
                    ));
                }

                // Parse sizes as raw u32 first to check for ZIP64
                let central_dir_size_raw =
                    u32::from_le_bytes([eocd[12], eocd[13], eocd[14], eocd[15]]);
                let central_dir_offset_raw =
                    u32::from_le_bytes([eocd[16], eocd[17], eocd[18], eocd[19]]);

                // ZIP64 uses 0xFFFFFFFF as a placeholder
                if central_dir_size_raw == u32::MAX || central_dir_offset_raw == u32::MAX {
                    return Err(anyhow!(
                        "ZIP64 archives are not supported (central directory fields use ZIP64 placeholder values)"
                    ));
                }

                return Ok(EocdInfo {
                    central_dir_offset: central_dir_offset_raw as u64,
                    central_dir_size: central_dir_size_raw as u64,
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
    fn parse_central_directory(
        data: &[u8],
        archive_size: u64,
    ) -> Result<HashMap<String, ArchiveEntry>> {
        const CDFH_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x01, 0x02];

        let mut entries = HashMap::new();
        let mut pos = 0;

        while pos + CDFH_MIN_SIZE <= data.len() {
            // Check for CDFH signature
            if !data[pos..].starts_with(&CDFH_SIGNATURE) {
                // Reached end of central directory entries
                break;
            }

            // Parse general purpose bit flag (offset 8)
            let general_purpose_flag = u16::from_le_bytes([data[pos + 8], data[pos + 9]]);

            // Check for data descriptor (bit 3) - we don't support this
            if general_purpose_flag & 0x0008 != 0 {
                return Err(anyhow!(
                    "ZIP entries with data descriptors (bit 3 set) are not supported"
                ));
            }

            // Parse compression method (offset 10)
            let compression_method = u16::from_le_bytes([data[pos + 10], data[pos + 11]]);

            // Parse CRC-32 (offset 16)
            let crc32 = u32::from_le_bytes([
                data[pos + 16],
                data[pos + 17],
                data[pos + 18],
                data[pos + 19],
            ]);

            // Parse compressed size (offset 20) - check for ZIP64
            let compressed_size_raw = u32::from_le_bytes([
                data[pos + 20],
                data[pos + 21],
                data[pos + 22],
                data[pos + 23],
            ]);

            // Parse uncompressed size (offset 24) - check for ZIP64
            let uncompressed_size_raw = u32::from_le_bytes([
                data[pos + 24],
                data[pos + 25],
                data[pos + 26],
                data[pos + 27],
            ]);

            // Parse local header offset (offset 42) - check for ZIP64
            let local_header_offset_raw = u32::from_le_bytes([
                data[pos + 42],
                data[pos + 43],
                data[pos + 44],
                data[pos + 45],
            ]);

            // Check for ZIP64 placeholder values
            if compressed_size_raw == u32::MAX
                || uncompressed_size_raw == u32::MAX
                || local_header_offset_raw == u32::MAX
            {
                return Err(anyhow!(
                    "ZIP64 entries are not supported (entry uses ZIP64 placeholder values)"
                ));
            }

            let compressed_size = compressed_size_raw as u64;
            let uncompressed_size = uncompressed_size_raw as u64;
            let local_header_offset = local_header_offset_raw as u64;

            // Validate local header offset
            if local_header_offset >= archive_size {
                return Err(anyhow!(
                    "Invalid local header offset {} for archive size {}",
                    local_header_offset,
                    archive_size
                ));
            }

            // Parse lengths (offsets 28, 30, 32) with overflow protection
            let filename_len = u16::from_le_bytes([data[pos + 28], data[pos + 29]]) as usize;
            let extra_len = u16::from_le_bytes([data[pos + 30], data[pos + 31]]) as usize;
            let comment_len = u16::from_le_bytes([data[pos + 32], data[pos + 33]]) as usize;

            // Calculate total entry size with overflow protection
            let total_entry_size = CDFH_MIN_SIZE
                .checked_add(filename_len)
                .and_then(|v| v.checked_add(extra_len))
                .and_then(|v| v.checked_add(comment_len))
                .ok_or_else(|| {
                    anyhow!("Central directory entry size overflow at position {}", pos)
                })?;

            let end = pos.checked_add(total_entry_size).ok_or_else(|| {
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

            // Extract filename with proper encoding handling
            let filename_bytes = &data[pos + CDFH_MIN_SIZE..pos + CDFH_MIN_SIZE + filename_len];
            let is_utf8 = (general_purpose_flag & (1 << 11)) != 0;

            let filename = if is_utf8 {
                // Filenames are explicitly marked as UTF-8
                String::from_utf8_lossy(filename_bytes).to_string()
            } else {
                // Legacy encoding - preserve byte values as chars
                // This handles CP437 and similar single-byte encodings
                filename_bytes.iter().map(|&b| b as char).collect()
            };

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
                crc32,
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
        let mut data = vec![0u8; 100];
        let eocd_pos = 50;

        // EOCD signature
        data[eocd_pos..eocd_pos + 4].copy_from_slice(&[0x50, 0x4b, 0x05, 0x06]);
        // Disk numbers (must be 0)
        data[eocd_pos + 4..eocd_pos + 8].copy_from_slice(&[0, 0, 0, 0]);
        // Central directory size
        data[eocd_pos + 12..eocd_pos + 16].copy_from_slice(&1000u32.to_le_bytes());
        // Central directory offset
        data[eocd_pos + 16..eocd_pos + 20].copy_from_slice(&5000u32.to_le_bytes());

        let result = ZipHandler::find_eocd(&data);
        assert!(result.is_ok());
        let info = result.unwrap();
        assert_eq!(info.central_dir_size, 1000);
        assert_eq!(info.central_dir_offset, 5000);
    }

    #[test]
    fn test_find_eocd_rejects_multi_disk() {
        let mut data = vec![0u8; MIN_EOCD_SIZE];

        // EOCD signature
        data[0..4].copy_from_slice(&[0x50, 0x4b, 0x05, 0x06]);
        // Non-zero disk number
        data[4..6].copy_from_slice(&1u16.to_le_bytes());

        let result = ZipHandler::find_eocd(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Multi-disk"));
    }

    #[test]
    fn test_find_eocd_rejects_zip64() {
        let mut data = vec![0u8; MIN_EOCD_SIZE];

        // EOCD signature
        data[0..4].copy_from_slice(&[0x50, 0x4b, 0x05, 0x06]);
        // Disk numbers = 0
        data[4..8].copy_from_slice(&[0, 0, 0, 0]);
        // Central directory size = 0xFFFFFFFF (ZIP64 marker)
        data[12..16].copy_from_slice(&u32::MAX.to_le_bytes());

        let result = ZipHandler::find_eocd(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ZIP64"));
    }

    #[test]
    fn test_find_eocd_not_found() {
        let data = vec![0u8; 100];
        let result = ZipHandler::find_eocd(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_crc32_empty() {
        assert_eq!(crc32fast::hash(&[]), 0x00000000);
    }

    #[test]
    fn test_crc32_known_value() {
        // "123456789" has a well-known CRC-32 value
        let data = b"123456789";
        assert_eq!(crc32fast::hash(data), 0xCBF43926);
    }

    #[test]
    fn test_parse_central_directory_rejects_data_descriptor() {
        let mut data = vec![0u8; 100];

        // CDFH signature
        data[0..4].copy_from_slice(&[0x50, 0x4b, 0x01, 0x02]);
        // General purpose flag with bit 3 set (data descriptor)
        data[8..10].copy_from_slice(&0x0008u16.to_le_bytes());

        let result = ZipHandler::parse_central_directory(&data, 10000);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("data descriptor"));
    }

    #[test]
    fn test_parse_central_directory_validates_offset() {
        let mut data = vec![0u8; 100];

        // CDFH signature
        data[0..4].copy_from_slice(&[0x50, 0x4b, 0x01, 0x02]);
        // No data descriptor flag
        data[8..10].copy_from_slice(&0u16.to_le_bytes());
        // Compression, CRC, sizes
        data[10..12].copy_from_slice(&8u16.to_le_bytes());
        data[16..20].copy_from_slice(&0u32.to_le_bytes());
        data[20..24].copy_from_slice(&100u32.to_le_bytes());
        data[24..28].copy_from_slice(&200u32.to_le_bytes());
        // Lengths
        data[28..30].copy_from_slice(&4u16.to_le_bytes());
        data[30..32].copy_from_slice(&0u16.to_le_bytes());
        data[32..34].copy_from_slice(&0u16.to_le_bytes());
        // Local header offset beyond archive size
        data[42..46].copy_from_slice(&50000u32.to_le_bytes());
        // Filename
        data[46..50].copy_from_slice(b"test");

        let result = ZipHandler::parse_central_directory(&data, 1000);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid local header offset"));
    }

    #[test]
    fn test_parse_central_directory_single_entry() {
        let mut data = vec![0u8; 100];

        // CDFH signature
        data[0..4].copy_from_slice(&[0x50, 0x4b, 0x01, 0x02]);
        // General purpose flag (no special bits)
        data[8..10].copy_from_slice(&0u16.to_le_bytes());
        // Compression method: deflate = 8
        data[10..12].copy_from_slice(&8u16.to_le_bytes());
        // CRC-32
        data[16..20].copy_from_slice(&0x12345678u32.to_le_bytes());
        // Compressed size: 500
        data[20..24].copy_from_slice(&500u32.to_le_bytes());
        // Uncompressed size: 1000
        data[24..28].copy_from_slice(&1000u32.to_le_bytes());
        // Filename length: 8
        data[28..30].copy_from_slice(&8u16.to_le_bytes());
        // Extra field length: 0
        data[30..32].copy_from_slice(&0u16.to_le_bytes());
        // Comment length: 0
        data[32..34].copy_from_slice(&0u16.to_le_bytes());
        // Local header offset: 100
        data[42..46].copy_from_slice(&100u32.to_le_bytes());
        // Filename: "test.txt"
        data[46..54].copy_from_slice(b"test.txt");

        let result = ZipHandler::parse_central_directory(&data, 10000);
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
            crc32,
        } = &entry.entry_type
        {
            assert_eq!(*local_header_offset, 100);
            assert_eq!(*compressed_size, 500);
            assert_eq!(*compression_method, 8);
            assert_eq!(*crc32, 0x12345678);
        } else {
            panic!("Expected ZipEntry type");
        }
    }

    #[test]
    fn test_parse_central_directory_utf8_filename() {
        let mut data = vec![0u8; 100];

        // CDFH signature
        data[0..4].copy_from_slice(&[0x50, 0x4b, 0x01, 0x02]);
        // General purpose flag with UTF-8 bit (bit 11) set
        data[8..10].copy_from_slice(&0x0800u16.to_le_bytes());
        // Compression, CRC, sizes
        data[10..12].copy_from_slice(&0u16.to_le_bytes());
        data[16..20].copy_from_slice(&0u32.to_le_bytes());
        data[20..24].copy_from_slice(&0u32.to_le_bytes());
        data[24..28].copy_from_slice(&0u32.to_le_bytes());
        // Filename length: 9 (日本.txt = 3 UTF-8 chars for 日本 + 4 for .txt)
        let filename = "日本.txt";
        let filename_bytes = filename.as_bytes();
        data[28..30].copy_from_slice(&(filename_bytes.len() as u16).to_le_bytes());
        data[30..32].copy_from_slice(&0u16.to_le_bytes());
        data[32..34].copy_from_slice(&0u16.to_le_bytes());
        data[42..46].copy_from_slice(&0u32.to_le_bytes());
        data[46..46 + filename_bytes.len()].copy_from_slice(filename_bytes);

        let result = ZipHandler::parse_central_directory(&data, 10000);
        assert!(result.is_ok());

        let entries = result.unwrap();
        let entry = entries.get("日本.txt").unwrap();
        assert_eq!(entry.path, "日本.txt");
    }
}
