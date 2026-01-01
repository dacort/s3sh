pub mod tar;
pub mod zip;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

use crate::s3::S3Client;
use crate::vfs::{ArchiveIndex, ArchiveEntry};

/// Trait for handling different archive formats
#[async_trait]
pub trait ArchiveHandler: Send + Sync {
    /// Build an index of the archive contents
    /// This reads the archive metadata to create a map of files and their locations
    async fn build_index(
        &self,
        s3_client: &Arc<S3Client>,
        bucket: &str,
        key: &str,
    ) -> Result<ArchiveIndex>;

    /// Extract a specific file from the archive
    async fn extract_file(
        &self,
        s3_client: &Arc<S3Client>,
        bucket: &str,
        key: &str,
        index: &ArchiveIndex,
        file_path: &str,
    ) -> Result<Bytes>;

    /// List entries at a specific path within the archive
    fn list_entries<'a>(
        &self,
        index: &'a ArchiveIndex,
        path: &str,
    ) -> Vec<&'a ArchiveEntry>;
}
