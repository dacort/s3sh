use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Wrapper around AWS S3 client with cross-region support
pub struct S3Client {
    default_client: Client,
    default_region: String,
    /// Cache of region-specific clients
    regional_clients: Arc<RwLock<HashMap<String, Client>>>,
}

impl S3Client {
    /// Create a new S3 client using default AWS configuration
    pub async fn new() -> Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let default_region = config
            .region()
            .map(|r| r.as_ref().to_string())
            .unwrap_or_else(|| "us-west-2".to_string());
        let client = Client::new(&config);

        Ok(S3Client {
            default_client: client,
            default_region,
            regional_clients: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Create an S3Client from an existing AWS SDK client (useful for testing)
    pub fn from_client(client: Client, region: String) -> Self {
        S3Client {
            default_client: client,
            default_region: region,
            regional_clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get or create a client for a specific region
    async fn get_regional_client(&self, region: &str) -> Result<Client> {
        // Check if we already have a client for this region
        {
            let clients = self.regional_clients.read().unwrap();
            if let Some(client) = clients.get(region) {
                return Ok(client.clone());
            }
        }

        // Create a new client for this region
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let region_provider = aws_sdk_s3::config::Region::new(region.to_string());
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .region(region_provider)
            .build();
        let client = Client::from_conf(s3_config);

        // Cache it
        {
            let mut clients = self.regional_clients.write().unwrap();
            clients.insert(region.to_string(), client.clone());
        }

        Ok(client)
    }

    /// Get the region of a bucket by making a head_bucket request
    async fn get_bucket_region(&self, bucket: &str) -> Result<String> {
        // Try with default client first
        match self
            .default_client
            .head_bucket()
            .bucket(bucket)
            .send()
            .await
        {
            Ok(resp) => {
                // Extract region from response headers
                if let Some(region) = resp.bucket_region() {
                    return Ok(region.to_string());
                }
                Ok(self.default_region.clone())
            }
            Err(e) => {
                // Check if error contains region information
                let error_msg = format!("{:?}", e);
                if error_msg.contains("PermanentRedirect") || error_msg.contains("301") {
                    // Try to extract region from error
                    // AWS returns the region in the error for redirects
                    // For now, we'll try common regions
                    for region in [
                        "us-east-1",
                        "us-west-1",
                        "us-west-2",
                        "eu-west-1",
                        "ap-southeast-1",
                    ] {
                        let client = self.get_regional_client(region).await?;
                        if let Ok(resp) = client.head_bucket().bucket(bucket).send().await {
                            if let Some(bucket_region) = resp.bucket_region() {
                                return Ok(bucket_region.to_string());
                            }
                            return Ok(region.to_string());
                        }
                    }
                }
                Err(anyhow::anyhow!(
                    "Failed to determine bucket region: {}",
                    bucket
                ))
            }
        }
    }

    /// Get the appropriate client for a bucket (handles cross-region)
    async fn get_client_for_bucket(&self, bucket: &str) -> Result<Client> {
        // Try default client first
        match self
            .default_client
            .head_bucket()
            .bucket(bucket)
            .send()
            .await
        {
            Ok(_) => Ok(self.default_client.clone()),
            Err(_) => {
                // Get the bucket's region and return appropriate client
                let region = self.get_bucket_region(bucket).await?;
                self.get_regional_client(&region).await
            }
        }
    }

    /// List all S3 buckets
    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let resp = self
            .default_client
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
                    Some(
                        d.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
                            .ok()?,
                    )
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
        let client = self.get_client_for_bucket(bucket).await?;
        let mut req = client.list_objects_v2().bucket(bucket);

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
                    Some(
                        d.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
                            .ok()?,
                    )
                }),
            })
            .collect();

        Ok(ListObjectsResult { prefixes, objects })
    }

    /// Get an object's metadata
    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata> {
        let client = self.get_client_for_bucket(bucket).await?;
        let resp = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .context(format!(
                "Failed to get metadata for s3://{}/{}",
                bucket, key
            ))?;

        Ok(ObjectMetadata {
            size: resp.content_length().unwrap_or(0) as u64,
        })
    }

    /// Get an entire object's contents
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Bytes> {
        let client = self.get_client_for_bucket(bucket).await?;
        let resp = client
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
        let client = self.get_client_for_bucket(bucket).await?;
        let range = format!("bytes={}-{}", offset, offset + length - 1);

        let resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(range)
            .send()
            .await
            .context(format!(
                "Failed to get object range s3://{}/{}",
                bucket, key
            ))?;

        let bytes = resp
            .body
            .collect()
            .await
            .context("Failed to read object body")?
            .into_bytes();

        Ok(bytes)
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
}
