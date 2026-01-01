use anyhow::{Context, Result};
use bytes::Bytes;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use super::S3Client;

/// A streaming reader for S3 objects that supports range requests
/// This allows us to read specific parts of large files (like archives) without downloading everything
pub struct S3Stream {
    client: Arc<S3Client>,
    bucket: String,
    key: String,
    /// Total size of the object
    size: u64,
    /// Current position in the stream
    position: u64,
    /// Optional buffer for recently read data
    buffer: Option<Bytes>,
    /// Buffer position offset
    buffer_offset: u64,
}

impl S3Stream {
    /// Create a new S3 stream
    pub async fn new(client: Arc<S3Client>, bucket: String, key: String) -> Result<Self> {
        // Get object size
        let metadata = client.head_object(&bucket, &key).await?;

        Ok(S3Stream {
            client,
            bucket,
            key,
            size: metadata.size,
            position: 0,
            buffer: None,
            buffer_offset: 0,
        })
    }

    /// Get the total size of the object
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Get the current position
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Read a specific range of bytes
    pub async fn read_range(&self, offset: u64, length: u64) -> Result<Bytes> {
        if offset + length > self.size {
            return Err(anyhow::anyhow!(
                "Range out of bounds: {}+{} > {}",
                offset,
                length,
                self.size
            ));
        }

        self.client
            .get_object_range(&self.bucket, &self.key, offset, length)
            .await
    }

    /// Read the last N bytes of the object (useful for zip central directory)
    pub async fn read_tail(&self, length: u64) -> Result<Bytes> {
        let actual_length = length.min(self.size);
        let offset = self.size - actual_length;
        self.read_range(offset, actual_length).await
    }

    /// Read bytes into a buffer (implements Read trait behavior)
    async fn read_internal(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.position >= self.size {
            return Ok(0); // EOF
        }

        // Check if we have buffered data
        if let Some(ref buffer) = self.buffer {
            let buffer_pos = (self.position - self.buffer_offset) as usize;
            if buffer_pos < buffer.len() {
                // We have data in buffer
                let available = buffer.len() - buffer_pos;
                let to_copy = available.min(buf.len());
                buf[..to_copy].copy_from_slice(&buffer[buffer_pos..buffer_pos + to_copy]);
                self.position += to_copy as u64;
                return Ok(to_copy);
            }
        }

        // Need to fetch more data
        // Fetch in chunks of 64KB or remaining size
        let chunk_size = 65536u64;
        let remaining = self.size - self.position;
        let fetch_size = chunk_size.min(remaining).min(buf.len() as u64 * 2);

        let bytes = self
            .read_range(self.position, fetch_size)
            .await
            .context("Failed to read from S3")?;

        let to_copy = bytes.len().min(buf.len());
        buf[..to_copy].copy_from_slice(&bytes[..to_copy]);

        // Buffer the fetched data
        self.buffer = Some(bytes);
        self.buffer_offset = self.position;
        self.position += to_copy as u64;

        Ok(to_copy)
    }

    /// Create a synchronous reader wrapper
    pub fn into_sync_reader(self) -> SyncS3Reader {
        SyncS3Reader {
            stream: Arc::new(tokio::sync::Mutex::new(self)),
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

/// Synchronous wrapper around S3Stream for use with sync Read trait
pub struct SyncS3Reader {
    stream: Arc<tokio::sync::Mutex<S3Stream>>,
    runtime: tokio::runtime::Handle,
}

impl Read for SyncS3Reader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.runtime
            .block_on(async {
                let mut stream = self.stream.lock().await;
                stream.read_internal(buf).await
            })
            .map_err(std::io::Error::other)
    }
}

impl Seek for SyncS3Reader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.runtime.block_on(async {
            let mut stream = self.stream.lock().await;

            let new_pos = match pos {
                SeekFrom::Start(offset) => offset,
                SeekFrom::End(offset) => {
                    if offset >= 0 {
                        stream.size + offset as u64
                    } else {
                        stream.size.saturating_sub((-offset) as u64)
                    }
                }
                SeekFrom::Current(offset) => {
                    if offset >= 0 {
                        stream.position + offset as u64
                    } else {
                        stream.position.saturating_sub((-offset) as u64)
                    }
                }
            };

            if new_pos > stream.size {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Seek beyond end of file",
                ));
            }

            stream.position = new_pos;
            // Invalidate buffer if we seeked outside of it
            if let Some(ref buffer) = stream.buffer {
                let buffer_end = stream.buffer_offset + buffer.len() as u64;
                if new_pos < stream.buffer_offset || new_pos >= buffer_end {
                    stream.buffer = None;
                }
            }

            Ok(new_pos)
        })
    }
}

impl Clone for SyncS3Reader {
    fn clone(&self) -> Self {
        SyncS3Reader {
            stream: Arc::clone(&self.stream),
            runtime: self.runtime.clone(),
        }
    }
}
