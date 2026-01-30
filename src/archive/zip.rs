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

/// Compute CRC-32 checksum of data (IEEE polynomial)
fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFFFFFFu32;
    for &byte in data {
        let index = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = CRC32_TABLE[index] ^ (crc >> 8);
    }
    !crc
}

/// CRC-32 lookup table (IEEE polynomial 0xEDB88320)
const CRC32_TABLE: [u32; 256] = [
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
    0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
    0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
    0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
    0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
    0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
    0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
    0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
    0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
    0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
    0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
    0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
    0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
    0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
    0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
    0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
    0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
    0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7a9b, 0x5005713c, 0x270241aa,
    0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
    0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
    0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
    0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
    0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
    0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
    0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
    0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
    0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
    0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
    0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
    0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
    0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
    0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
    0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
    0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
    0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
    0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
    0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
    0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
    0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdede86c5, 0x47d7977f, 0x30d069e9,
    0xbdd3b106, 0xcad2f090, 0x73db802a, 0x04dc19bc, 0x9a91a61f, 0xedcc9989,
    0x7aa70e33, 0x0da01fa5, 0x9d48d534, 0xea4fe4a2, 0x73c50918, 0x04c2398e,
    0x9abfd32d, 0xedb8c3bb, 0x7407df01, 0x03000097,
];

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
                // Use checked capacity to avoid allocation panics on malicious input
                let capacity = (entry.size as usize).min(MAX_DECOMPRESSED_SIZE as usize);
                let mut decompressed = Vec::with_capacity(capacity);
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

        // Verify decompressed size matches expected
        if decompressed.len() as u64 != entry.size {
            return Err(anyhow!(
                "Decompressed size mismatch: expected {} bytes, got {} bytes",
                entry.size,
                decompressed.len()
            ));
        }

        // Verify CRC-32 checksum
        let actual_crc32 = crc32(&decompressed);
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
        assert_eq!(crc32(&[]), 0x00000000);
    }

    #[test]
    fn test_crc32_known_value() {
        // "123456789" has a well-known CRC-32 value
        let data = b"123456789";
        assert_eq!(crc32(data), 0xCBF43926);
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
