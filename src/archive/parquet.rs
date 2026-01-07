use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::{
    as_boolean_array, as_generic_binary_array, as_primitive_array, as_string_array,
};
use arrow_array::types::*;
use arrow_array::*;
use arrow_schema::{DataType, Schema};
use chrono;
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, path::Path as ObjectPath};
use parquet::arrow::{
    ParquetRecordBatchStreamBuilder, ProjectionMask,
    async_reader::{AsyncFileReader, ParquetObjectReader},
};
use parquet::file::metadata::ParquetMetaData;

use aws_credential_types::provider::ProvideCredentials;

use crate::s3::S3Client;
use crate::vfs::{ArchiveEntry, ArchiveIndex, EntryType, ParquetEntryHandler};

use super::ArchiveHandler;

/// Handler for Parquet files - treats them as virtual directories
pub struct ParquetHandler;

// Timeout constants for operations
const METADATA_READ_TIMEOUT_SECS: u64 = 30; // Timeout for reading Parquet footer metadata
const DATA_READ_TIMEOUT_SECS: u64 = 60; // Timeout for reading column data

impl ParquetHandler {
    pub fn new() -> Self {
        ParquetHandler
    }

    /// Load AWS config once to be reused across operations
    /// This prevents duplicate credential loading
    async fn load_aws_config() -> aws_config::SdkConfig {
        aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await
    }

    /// Create an object_store S3 client from pre-loaded AWS config
    /// Accepts config as parameter to avoid redundant credential loading
    async fn create_object_store(
        config: &aws_config::SdkConfig,
        bucket: &str,
    ) -> Result<Arc<dyn ObjectStore>> {
        // Config is passed in to avoid duplicate aws_config::load_defaults() calls

        // Extract region with proper fallback chain
        let region = config
            .region()
            .map(|r| r.as_ref().to_string())
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
            .unwrap_or_else(|| {
                eprintln!(
                    "Warning: No AWS region configured. Using us-west-2 as fallback. \
                          Set AWS_DEFAULT_REGION or configure region in ~/.aws/config"
                );
                "us-west-2".to_string()
            });

        // Get credentials from aws-config - this supports the full AWS credential provider chain
        // including ~/.aws/credentials, environment variables, IAM roles, etc.
        let credentials = config
            .credentials_provider()
            .ok_or_else(|| anyhow!("No credentials provider found in AWS config"))?
            .provide_credentials()
            .await
            .context("Failed to load AWS credentials")?;

        // Build object_store S3 client with explicit credentials
        // Note: Timeouts are managed at the operation level using tokio::time::timeout
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(&region)
            .with_access_key_id(credentials.access_key_id())
            .with_secret_access_key(credentials.secret_access_key());

        // Add session token if present (for temporary credentials)
        if let Some(token) = credentials.session_token() {
            builder = builder.with_token(token);
        }

        let store = builder
            .build()
            .context("Failed to create object_store S3 client")?;

        Ok(Arc::new(store))
    }

    /// Read Parquet metadata (footer) from S3
    async fn read_metadata(
        config: &aws_config::SdkConfig,
        bucket: &str,
        key: &str,
    ) -> Result<(Arc<ParquetMetaData>, Arc<Schema>)> {
        // Create object store using pre-loaded config
        let store = Self::create_object_store(config, bucket).await?;

        // Create object path
        let object_path = ObjectPath::from(key);

        // Create Parquet reader with path (API changed in 57.x)
        let mut reader = ParquetObjectReader::new(Arc::clone(&store), object_path);

        // Get metadata (passing None for default ArrowReaderOptions)
        // Wrap in timeout to prevent indefinite hangs
        let metadata = tokio::time::timeout(
            std::time::Duration::from_secs(METADATA_READ_TIMEOUT_SECS),
            reader.get_metadata(None),
        )
        .await
        .context("Timeout reading Parquet metadata - operation took longer than 30 seconds")?
        .context("Failed to read Parquet metadata")?;

        // Extract Arrow schema
        let schema = parquet::arrow::parquet_to_arrow_schema(
            metadata.file_metadata().schema_descr(),
            metadata.file_metadata().key_value_metadata(),
        )
        .context("Failed to convert Parquet schema to Arrow")?;

        Ok((metadata, Arc::new(schema)))
    }

    /// Check if field is a nested type (struct, list, map)
    fn is_nested_type(field: &arrow_schema::Field) -> bool {
        matches!(
            field.data_type(),
            DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _)
        )
    }

    /// Estimate size of a field value for size calculation
    fn estimate_field_size(field: &arrow_schema::Field) -> usize {
        match field.data_type() {
            DataType::Int8 | DataType::UInt8 => 4,
            DataType::Int16 | DataType::UInt16 => 6,
            DataType::Int32 | DataType::UInt32 | DataType::Float32 => 12,
            DataType::Int64 | DataType::UInt64 | DataType::Float64 => 20,
            DataType::Utf8 | DataType::Binary => 50, // Average string length
            DataType::Boolean => 5,
            _ => 20, // Default
        }
    }

    /// Format Arrow data type as human-readable string
    fn format_data_type(dt: &DataType) -> String {
        match dt {
            DataType::Int8 => "INT8".to_string(),
            DataType::Int16 => "INT16".to_string(),
            DataType::Int32 => "INT32".to_string(),
            DataType::Int64 => "INT64".to_string(),
            DataType::UInt8 => "UINT8".to_string(),
            DataType::UInt16 => "UINT16".to_string(),
            DataType::UInt32 => "UINT32".to_string(),
            DataType::UInt64 => "UINT64".to_string(),
            DataType::Float32 => "FLOAT32".to_string(),
            DataType::Float64 => "FLOAT64".to_string(),
            DataType::Utf8 => "STRING".to_string(),
            DataType::Binary => "BINARY".to_string(),
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Timestamp(unit, tz) => {
                let tz_str = tz.as_ref().map(|s| s.as_ref()).unwrap_or("UTC");
                format!("TIMESTAMP({:?}, {})", unit, tz_str)
            }
            DataType::Date32 => "DATE".to_string(),
            DataType::Struct(_) => "STRUCT".to_string(),
            DataType::List(_) => "LIST".to_string(),
            DataType::Map(_, _) => "MAP".to_string(),
            _ => format!("{:?}", dt),
        }
    }

    /// Add _schema.txt virtual file
    fn add_schema_entry(
        entries: &mut HashMap<String, ArchiveEntry>,
        schema: &Schema,
    ) -> Result<()> {
        // Estimate size based on schema complexity
        let estimated_size = schema.fields().len() * 100;

        entries.insert(
            "_schema.txt".to_string(),
            ArchiveEntry::parquet_virtual(
                "_schema.txt".to_string(),
                estimated_size as u64,
                false, // is_file
                ParquetEntryHandler::Schema,
            ),
        );

        Ok(())
    }

    /// Add column data entries under columns/
    fn add_column_entries(
        entries: &mut HashMap<String, ArchiveEntry>,
        schema: &Schema,
    ) -> Result<()> {
        // Only handle flat schemas (defer nested types)
        for (i, field) in schema.fields().iter().enumerate() {
            // Skip nested types
            if Self::is_nested_type(field) {
                continue;
            }

            let column_name = field.name();
            let path = format!("columns/{}", column_name);

            // Estimate size: 100 rows * avg field size
            let estimated_size = 100 * Self::estimate_field_size(field);

            entries.insert(
                path.clone(),
                ArchiveEntry::parquet_virtual(
                    path,
                    estimated_size as u64,
                    false, // is_file
                    ParquetEntryHandler::ColumnData {
                        column_index: i,
                        column_name: column_name.to_string(),
                    },
                ),
            );
        }

        Ok(())
    }

    /// Add statistics entries under stats/
    fn add_stats_entries(
        entries: &mut HashMap<String, ArchiveEntry>,
        schema: &Schema,
    ) -> Result<()> {
        for (i, field) in schema.fields().iter().enumerate() {
            // Skip nested types
            if Self::is_nested_type(field) {
                continue;
            }

            let column_name = field.name();
            let path = format!("stats/{}", column_name);

            // Stats are small text summaries
            let estimated_size = 500;

            entries.insert(
                path.clone(),
                ArchiveEntry::parquet_virtual(
                    path,
                    estimated_size,
                    false, // is_file
                    ParquetEntryHandler::ColumnStats {
                        column_index: i,
                        column_name: column_name.to_string(),
                    },
                ),
            );
        }

        Ok(())
    }

    /// Render schema as human-readable text
    async fn render_schema(&self, index: &ArchiveIndex) -> Result<Bytes> {
        // Re-read metadata to get schema
        let bucket = index
            .metadata
            .get("bucket")
            .ok_or_else(|| anyhow!("Bucket not found in index metadata"))?;
        let key = index
            .metadata
            .get("key")
            .ok_or_else(|| anyhow!("Key not found in index metadata"))?;

        // Load AWS config once for this operation
        let config = Self::load_aws_config().await;
        let (_metadata, schema) = Self::read_metadata(&config, bucket, key).await?;

        // Build human-readable output
        let mut output = String::new();

        // Header
        output.push_str("Parquet Schema\n");
        output.push_str("==============\n\n");

        // File statistics
        if let Some(row_count) = index.metadata.get("row_count") {
            output.push_str(&format!("Rows: {}\n", row_count));
        }
        if let Some(num_row_groups) = index.metadata.get("num_row_groups") {
            output.push_str(&format!("Row Groups: {}\n", num_row_groups));
        }
        output.push('\n');

        // Schema fields
        output.push_str("Columns:\n");
        output.push_str("--------\n");
        for field in schema.fields() {
            let nullable = if field.is_nullable() {
                "nullable"
            } else {
                "required"
            };
            output.push_str(&format!(
                "  {} : {} ({})\n",
                field.name(),
                Self::format_data_type(field.data_type()),
                nullable
            ));
        }

        Ok(Bytes::from(output))
    }

    /// Format statistics value bytes as string
    fn format_stat_value(bytes_opt: Option<&[u8]>) -> String {
        match bytes_opt {
            Some(bytes) => {
                // Try UTF-8 first
                if let Ok(s) = std::str::from_utf8(bytes) {
                    format!("\"{}\"", s)
                } else {
                    // Fallback to hex for binary
                    format!("<binary: {} bytes>", bytes.len())
                }
            }
            None => "<not available>".to_string(),
        }
    }

    /// Render column statistics from Parquet footer metadata
    async fn render_column_stats(
        &self,
        bucket: &str,
        key: &str,
        _index: &ArchiveIndex,
        column_index: usize,
        column_name: &str,
    ) -> Result<Bytes> {
        // Load AWS config once for this operation
        let config = Self::load_aws_config().await;
        // Re-read metadata to get statistics
        let (metadata, schema) = Self::read_metadata(&config, bucket, key).await?;

        let mut output = String::new();

        // Header
        output.push_str(&format!("Column: {}\n", column_name));
        output.push_str(&"=".repeat(40 + column_name.len()));
        output.push_str("\n\n");

        // Type information
        let field = schema.field(column_index);
        output.push_str(&format!(
            "Type: {}\n",
            Self::format_data_type(field.data_type())
        ));
        output.push_str(&format!("Nullable: {}\n", field.is_nullable()));
        output.push('\n');

        // Statistics from row groups
        output.push_str("Statistics:\n");
        output.push_str("-----------\n");

        let mut total_null_count = 0u64;
        let mut total_rows = 0u64;

        // Iterate through row groups
        for (rg_idx, row_group) in metadata.row_groups().iter().enumerate() {
            if let Some(column_chunk) = row_group.columns().get(column_index)
                && let Some(stats) = column_chunk.statistics()
            {
                // Collect stats
                if rg_idx == 0 {
                    output.push_str(&format!(
                        "  Min Value: {}\n",
                        Self::format_stat_value(stats.min_bytes_opt())
                    ));
                    output.push_str(&format!(
                        "  Max Value: {}\n",
                        Self::format_stat_value(stats.max_bytes_opt())
                    ));
                }

                total_null_count += stats.null_count_opt().unwrap_or(0);
            }
            total_rows += row_group.num_rows() as u64;
        }

        output.push_str(&format!("  Total Rows: {}\n", total_rows));
        output.push_str(&format!("  Null Count: {}\n", total_null_count));
        if total_rows > 0 {
            output.push_str(&format!(
                "  Null %: {:.2}%\n",
                (total_null_count as f64 / total_rows as f64) * 100.0
            ));
        }

        Ok(Bytes::from(output))
    }

    /// Format a single array value as string
    fn format_array_value(array: &Arc<dyn Array>, index: usize) -> Result<String> {
        // Check if null
        if array.is_null(index) {
            return Ok("<NULL>".to_string());
        }

        // Dispatch based on type
        match array.data_type() {
            DataType::Int8 => Ok(as_primitive_array::<Int8Type>(array)
                .value(index)
                .to_string()),
            DataType::Int16 => Ok(as_primitive_array::<Int16Type>(array)
                .value(index)
                .to_string()),
            DataType::Int32 => Ok(as_primitive_array::<Int32Type>(array)
                .value(index)
                .to_string()),
            DataType::Int64 => Ok(as_primitive_array::<Int64Type>(array)
                .value(index)
                .to_string()),
            DataType::UInt8 => Ok(as_primitive_array::<UInt8Type>(array)
                .value(index)
                .to_string()),
            DataType::UInt16 => Ok(as_primitive_array::<UInt16Type>(array)
                .value(index)
                .to_string()),
            DataType::UInt32 => Ok(as_primitive_array::<UInt32Type>(array)
                .value(index)
                .to_string()),
            DataType::UInt64 => Ok(as_primitive_array::<UInt64Type>(array)
                .value(index)
                .to_string()),
            DataType::Float32 => Ok(as_primitive_array::<Float32Type>(array)
                .value(index)
                .to_string()),
            DataType::Float64 => Ok(as_primitive_array::<Float64Type>(array)
                .value(index)
                .to_string()),
            DataType::Boolean => Ok(as_boolean_array(array).value(index).to_string()),
            DataType::Utf8 => Ok(as_string_array(array).value(index).to_string()),
            DataType::Binary => {
                let bytes = as_generic_binary_array::<i32>(array).value(index);
                Ok(format!("<binary: {} bytes>", bytes.len()))
            }
            DataType::Timestamp(unit, tz) => {
                use arrow_schema::TimeUnit;
                let timestamp_micros = match unit {
                    TimeUnit::Second => {
                        as_primitive_array::<TimestampSecondType>(array).value(index) * 1_000_000
                    }
                    TimeUnit::Millisecond => {
                        as_primitive_array::<TimestampMillisecondType>(array).value(index) * 1_000
                    }
                    TimeUnit::Microsecond => {
                        as_primitive_array::<TimestampMicrosecondType>(array).value(index)
                    }
                    TimeUnit::Nanosecond => {
                        as_primitive_array::<TimestampNanosecondType>(array).value(index) / 1_000
                    }
                };

                if let Some(dt) = chrono::DateTime::from_timestamp_micros(timestamp_micros) {
                    let tz_str = tz.as_ref().map(|s| s.as_ref()).unwrap_or("UTC");
                    Ok(format!(
                        "{} ({})",
                        dt.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                        tz_str
                    ))
                } else {
                    Ok(format!("<invalid timestamp: {}>", timestamp_micros))
                }
            }
            DataType::Date32 => {
                let days = as_primitive_array::<Date32Type>(array).value(index);
                // Date32 is days since Unix epoch (1970-01-01)
                // Convert to seconds and then to date
                let timestamp = days as i64 * 86400; // days * seconds_per_day
                if let Some(dt) = chrono::DateTime::from_timestamp(timestamp, 0) {
                    Ok(dt.format("%Y-%m-%d").to_string())
                } else {
                    Ok(format!("<invalid date: {} days>", days))
                }
            }
            DataType::Date64 => {
                let millis = as_primitive_array::<Date64Type>(array).value(index);
                // Date64 is milliseconds since Unix epoch
                if let Some(dt) = chrono::DateTime::from_timestamp_millis(millis) {
                    Ok(dt.format("%Y-%m-%d").to_string())
                } else {
                    Ok(format!("<invalid date: {} ms>", millis))
                }
            }
            _ => Ok(format!("<unsupported type: {:?}>", array.data_type())),
        }
    }

    /// Read and render column data
    async fn render_column_data(
        &self,
        bucket: &str,
        key: &str,
        column_index: usize,
        _column_name: &str,
    ) -> Result<Bytes> {
        const DEFAULT_ROW_LIMIT: usize = 100;

        // Load AWS config once for this operation
        let config = Self::load_aws_config().await;
        // Create object store and reader
        let store = Self::create_object_store(&config, bucket).await?;
        let object_path = ObjectPath::from(key);

        // Get object metadata
        // Create Parquet reader with path (API changed in 57.x)
        let reader = ParquetObjectReader::new(store, object_path);

        // Build stream with column projection
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .context("Failed to create Parquet stream builder")?;

        // Create projection mask for single column
        let mask = ProjectionMask::roots(builder.parquet_schema(), vec![column_index]);

        let builder = builder
            .with_projection(mask)
            .with_batch_size(DEFAULT_ROW_LIMIT);

        let mut stream = builder.build().context("Failed to build Parquet stream")?;

        // Read first batch with timeout
        let batch_result = tokio::time::timeout(
            std::time::Duration::from_secs(DATA_READ_TIMEOUT_SECS),
            stream.next(),
        )
        .await
        .context("Timeout reading column data - operation took longer than 60 seconds")?
        .ok_or_else(|| anyhow!("No data in Parquet file - stream is empty"))?;

        let batch = batch_result.map_err(|e| {
            anyhow!(
                "Failed to read batch from Parquet stream: {}. \
                    This may indicate a permission issue or file format problem.",
                e
            )
        })?;

        // Extract column
        let column = batch.column(0).clone(); // First column (we projected only one)

        // Format as text (one value per line)
        let mut output = String::new();

        // Get actual number of rows (might be less than limit)
        let num_rows = column.len().min(DEFAULT_ROW_LIMIT);

        for row_idx in 0..num_rows {
            let value_str = Self::format_array_value(&column, row_idx)?;
            output.push_str(&value_str);
            output.push('\n');
        }

        Ok(Bytes::from(output))
    }
}

impl Default for ParquetHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ArchiveHandler for ParquetHandler {
    async fn build_index(
        &self,
        s3_client: &Arc<S3Client>,
        bucket: &str,
        key: &str,
    ) -> Result<ArchiveIndex> {
        // First verify the object exists using the regular S3Client
        s3_client
            .head_object(bucket, key)
            .await
            .context("Failed to verify Parquet file exists")?;

        // Load AWS config once for all operations in this method
        let config = Self::load_aws_config().await;

        // Read Parquet metadata
        let (metadata, schema) = Self::read_metadata(&config, bucket, key).await?;

        let mut entries = HashMap::new();

        // Create virtual files and directories from schema
        Self::add_schema_entry(&mut entries, &schema)?;

        // Add explicit directory entries for columns/ and stats/
        // These must be added BEFORE the child entries to avoid issues with list_entries
        entries.insert(
            "columns".to_string(),
            ArchiveEntry::parquet_virtual(
                "columns".to_string(),
                0,
                true,                        // is_dir
                ParquetEntryHandler::Schema, // Placeholder handler (not used for directories)
            ),
        );
        entries.insert(
            "stats".to_string(),
            ArchiveEntry::parquet_virtual(
                "stats".to_string(),
                0,
                true,                        // is_dir
                ParquetEntryHandler::Schema, // Placeholder handler (not used for directories)
            ),
        );

        Self::add_column_entries(&mut entries, &schema)?;
        Self::add_stats_entries(&mut entries, &schema)?;

        // Store metadata in index for later use
        let mut metadata_map = HashMap::new();
        metadata_map.insert(
            "row_count".to_string(),
            metadata.file_metadata().num_rows().to_string(),
        );
        metadata_map.insert(
            "num_row_groups".to_string(),
            metadata.num_row_groups().to_string(),
        );
        metadata_map.insert("bucket".to_string(), bucket.to_string());
        metadata_map.insert("key".to_string(), key.to_string());

        Ok(ArchiveIndex {
            entries,
            metadata: metadata_map,
        })
    }

    async fn extract_file(
        &self,
        _s3_client: &Arc<S3Client>,
        bucket: &str,
        key: &str,
        index: &ArchiveIndex,
        file_path: &str,
    ) -> Result<Bytes> {
        // Look up entry
        let entry = index
            .entries
            .get(file_path)
            .ok_or_else(|| anyhow!("File not found in Parquet archive: {}", file_path))?;

        if entry.is_dir {
            return Err(anyhow!("Cannot extract directory: {}", file_path));
        }

        // Dispatch based on entry type
        match &entry.entry_type {
            EntryType::ParquetVirtual { handler } => match handler {
                ParquetEntryHandler::Schema => self.render_schema(index).await,
                ParquetEntryHandler::ColumnStats {
                    column_index,
                    column_name,
                } => {
                    self.render_column_stats(bucket, key, index, *column_index, column_name)
                        .await
                }
                ParquetEntryHandler::ColumnData {
                    column_index,
                    column_name,
                } => {
                    self.render_column_data(bucket, key, *column_index, column_name)
                        .await
                }
            },
            _ => Err(anyhow!("Invalid entry type for Parquet handler")),
        }
    }

    fn list_entries<'a>(&self, index: &'a ArchiveIndex, path: &str) -> Vec<&'a ArchiveEntry> {
        // Normalize path (same as TarHandler)
        let normalized_path = if path.is_empty() || path == "/" {
            ""
        } else {
            path.trim_start_matches('/').trim_end_matches('/')
        };

        let search_prefix = if normalized_path.is_empty() {
            String::new()
        } else {
            format!("{}/", normalized_path)
        };

        let mut result = Vec::new();
        let mut seen_dirs = std::collections::HashSet::new();

        for (entry_path, entry) in &index.entries {
            // Skip if not in our directory
            if !search_prefix.is_empty() && !entry_path.starts_with(&search_prefix) {
                continue;
            }

            // Get relative path
            let relative = if search_prefix.is_empty() {
                entry_path.as_str()
            } else {
                entry_path
                    .strip_prefix(&search_prefix)
                    .unwrap_or(entry_path)
            };

            if relative.is_empty() {
                continue;
            }

            // Check if direct child or nested
            if let Some(slash_pos) = relative.find('/') {
                // Nested entry - show the parent directory
                let dir_name = &relative[..slash_pos];
                if seen_dirs.insert(dir_name.to_string()) {
                    let dir_path = if search_prefix.is_empty() {
                        dir_name.to_string()
                    } else {
                        format!("{}{}", search_prefix, dir_name)
                    };

                    // Look up the directory entry
                    if let Some(dir_entry) = index.entries.get(&dir_path) {
                        result.push(dir_entry);
                    }
                }
            } else {
                // Direct child
                if entry.is_dir {
                    // Only add directory if we haven't already added it from a nested entry
                    if seen_dirs.insert(relative.to_string()) {
                        result.push(entry);
                    }
                } else {
                    // Always add files
                    result.push(entry);
                }
            }
        }

        // Sort results for consistent output
        // Directories first (alphabetically), then files (alphabetically)
        result.sort_by(|a, b| match (a.is_dir, b.is_dir) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => a.path.cmp(&b.path),
        });

        result
    }
}
