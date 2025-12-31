use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use aws_sdk_s3::types::Object;
use bytes::Bytes;

/// Wrapper around AWS S3 client
pub struct S3Client {
    client: Client,
}

impl S3Client {
    /// Create a new S3 client using default AWS configuration
    pub async fn new() -> Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = Client::new(&config);
        Ok(S3Client { client })
    }

    /// List all S3 buckets
    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let resp = self
            .client
            .list_buckets()
            .send()
            .await
            .context("Failed to list S3 buckets")?;

        let buckets = resp
            .buckets()
            .iter()
            .map(|b| BucketInfo {
                name: b.name().unwrap_or("").to_string(),
                creation_date: b.creation_date().and_then(|d| {
                    Some(d.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime).ok()?)
                }),
            })
            .collect();

        Ok(buckets)
    }

    /// List objects in a bucket with a given prefix and delimiter
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> Result<ListObjectsResult> {
        let mut req = self.client.list_objects_v2().bucket(bucket);

        if !prefix.is_empty() {
            req = req.prefix(prefix);
        }

        if let Some(delim) = delimiter {
            req = req.delimiter(delim);
        }

        let resp = req
            .send()
            .await
            .context(format!("Failed to list objects in bucket: {}", bucket))?;

        let prefixes = resp
            .common_prefixes()
            .iter()
            .filter_map(|p| p.prefix())
            .map(String::from)
            .collect();

        let objects = resp
            .contents()
            .iter()
            .map(|obj| ObjectInfo {
                key: obj.key().unwrap_or("").to_string(),
                size: obj.size().unwrap_or(0) as u64,
                last_modified: obj.last_modified().and_then(|d| {
                    Some(d.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime).ok()?)
                }),
            })
            .collect();

        Ok(ListObjectsResult { prefixes, objects })
    }

    /// Get an object's metadata
    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata> {
        let resp = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .context(format!("Failed to get metadata for s3://{}/{}", bucket, key))?;

        Ok(ObjectMetadata {
            size: resp.content_length().unwrap_or(0) as u64,
            content_type: resp.content_type().map(String::from),
            last_modified: resp.last_modified().and_then(|d| {
                Some(d.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime).ok()?)
            }),
        })
    }

    /// Get an entire object's contents
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Bytes> {
        let resp = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .context(format!("Failed to get object s3://{}/{}", bucket, key))?;

        let bytes = resp
            .body
            .collect()
            .await
            .context("Failed to read object body")?
            .into_bytes();

        Ok(bytes)
    }

    /// Get a range of bytes from an object (for streaming archives)
    pub async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        let range = format!("bytes={}-{}", offset, offset + length - 1);

        let resp = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(range)
            .send()
            .await
            .context(format!("Failed to get object range s3://{}/{}", bucket, key))?;

        let bytes = resp
            .body
            .collect()
            .await
            .context("Failed to read object body")?
            .into_bytes();

        Ok(bytes)
    }

    /// Check if an object exists
    pub async fn object_exists(&self, bucket: &str, key: &str) -> bool {
        self.head_object(bucket, key).await.is_ok()
    }
}

/// Information about an S3 bucket
#[derive(Debug, Clone)]
pub struct BucketInfo {
    pub name: String,
    pub creation_date: Option<String>,
}

/// Result of listing objects in a bucket
#[derive(Debug, Clone)]
pub struct ListObjectsResult {
    pub prefixes: Vec<String>,
    pub objects: Vec<ObjectInfo>,
}

/// Information about an S3 object
#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub last_modified: Option<String>,
}

/// Metadata about an S3 object
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub size: u64,
    pub content_type: Option<String>,
    pub last_modified: Option<String>,
}
