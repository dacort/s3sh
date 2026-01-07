# Parquet Navigation Design

## Overview

Extend s3sh's archive navigation capability to support Apache Parquet files, treating them as explorable directory structures. Just as you can `cd` into a `.tar.gz` file to explore its contents, you should be able to `cd` into a `.parquet` file to explore its schema, statistics, columns, and data.

## Motivation

Parquet files are ubiquitous in data engineering and analytics, particularly in S3-based data lakes. Common use cases include:

- **Common Crawl indexes** - ~300GB Parquet files containing URL indexes for web crawl data
- **Data lake exploration** - Understanding schema and data distribution without downloading files
- **Column sampling** - Previewing specific columns from large datasets
- **Schema discovery** - Understanding nested structures and data types
- **Statistics inspection** - Checking min/max/null counts for columns

Current tools require downloading Parquet files or using batch query engines (Athena, Spark). s3sh could provide instant, interactive exploration using S3 range requests.

## User Experience

### Basic Navigation

```bash
s3sh:/ $ cd commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2024-10/subset=warc/
s3sh:/.../subset=warc/ $ ls
part-00000-tid-1234.parquet
part-00001-tid-1234.parquet
part-00002-tid-1234.parquet
...

# Navigate into a Parquet file like an archive
s3sh:/.../subset=warc/ $ cd part-00000-tid-1234.parquet
s3sh:/.../part-00000-tid-1234.parquet $ ls
columns/
stats/
row_groups/
_schema.txt
```

### Schema Exploration

```bash
s3sh:/.../part-00000.parquet $ cat _schema.txt
message schema {
  required binary url_surtkey (STRING);
  required binary url (STRING);
  required binary url_host_registered_domain (STRING);
  required binary url_host_tld (STRING);
  optional binary url_path (STRING);
  required binary warc_filename (STRING);
  required int64 warc_record_offset;
  required int32 warc_record_length;
  required int64 fetch_time (TIMESTAMP(MILLIS,true));
  optional int32 content_languages;
  ...
}

Rows: 1,234,567
Row Groups: 12
Compressed Size: 245 MB
Uncompressed Size: 890 MB
```

### Statistics Inspection

```bash
s3sh:/.../part-00000.parquet $ cd stats/
s3sh:/.../stats $ ls
url_surtkey
url
url_host_registered_domain
url_host_tld
url_path
warc_filename
warc_record_offset
fetch_time
...

s3sh:/.../stats $ cat url_host_registered_domain
Column: url_host_registered_domain
Type: String (Binary/UTF-8)
Encoding: PLAIN, RLE_DICTIONARY
Compression: SNAPPY

Statistics:
  Total Rows:        1,234,567
  Null Count:        0
  Distinct Count:    ~456,789 (estimated)

  Min Value:         "00000.com"
  Max Value:         "zzzz.com"

  Physical Size:     12.4 MB (compressed)
  Uncompressed Size: 45.6 MB

Row Group Statistics:
  RG 0: min="00000.com" max="bzzz.org" nulls=0 rows=102,880
  RG 1: min="caaa.net"  max="dzzz.com" nulls=0 rows=102,881
  ...
```

### Column Data Sampling

```bash
s3sh:/.../part-00000.parquet $ cd columns/
s3sh:/.../columns $ ls
url_surtkey
url
url_host_registered_domain
url_host_tld
url_path
warc_filename
warc_record_offset
fetch_time
...

# View first N rows of a column
s3sh:/.../columns $ cat url_host_registered_domain | head -10
example.com
example.org
wikipedia.org
github.com
stackoverflow.com
reddit.com
twitter.com
google.com
facebook.com
amazon.com

# View specific columns as a table
s3sh:/.../columns $ cat url_host_registered_domain warc_filename | head -5
┌──────────────────────────────┬─────────────────────────────────────────────────────┐
│ url_host_registered_domain   │ warc_filename                                       │
├──────────────────────────────┼─────────────────────────────────────────────────────┤
│ example.com                  │ CC-MAIN-20240101000000-20240101010000-00000.warc.gz │
│ example.org                  │ CC-MAIN-20240101000000-20240101010000-00000.warc.gz │
│ wikipedia.org                │ CC-MAIN-20240101000000-20240101010000-00001.warc.gz │
│ github.com                   │ CC-MAIN-20240101000000-20240101010000-00001.warc.gz │
│ stackoverflow.com            │ CC-MAIN-20240101000000-20240101010000-00002.warc.gz │
└──────────────────────────────┴─────────────────────────────────────────────────────┘

# Wildcards work too
s3sh:/.../columns $ cat warc_* | head -3
┌──────────────────────────────────────────────────────┬───────────────────┬─────────────────────┐
│ warc_filename                                        │ warc_record_offset│ warc_record_length  │
├──────────────────────────────────────────────────────┼───────────────────┼─────────────────────┤
│ CC-MAIN-20240101000000-20240101010000-00000.warc.gz  │ 0                 │ 1234                │
│ CC-MAIN-20240101000000-20240101010000-00000.warc.gz  │ 1234              │ 5678                │
│ CC-MAIN-20240101000000-20240101010000-00001.warc.gz  │ 0                 │ 2345                │
└──────────────────────────────────────────────────────┴───────────────────┴─────────────────────┘
```

### Nested Column Navigation

For complex/nested types (structs, lists, maps):

```bash
s3sh:/.../columns $ ls
user_profile/      # Nested struct
tags/              # List of strings
metadata/          # Map

s3sh:/.../columns $ cd user_profile/
s3sh:/.../user_profile $ ls
id
name
email
created_at

s3sh:/.../user_profile $ cat name | head -5
Alice Johnson
Bob Smith
Carol Williams
David Brown
Eve Davis
```

### Row Group Exploration

```bash
s3sh:/.../part-00000.parquet $ cd row_groups/
s3sh:/.../row_groups $ ls
row_group_0/   # Rows 0-102,880
row_group_1/   # Rows 102,881-205,761
row_group_2/   # Rows 205,762-308,642
...

s3sh:/.../row_groups $ cd row_group_0/
s3sh:/.../row_group_0 $ ls
columns/
_info.txt

s3sh:/.../row_group_0 $ cat _info.txt
Row Group: 0
Rows: 102,880
Compressed Size: 20.4 MB
Uncompressed Size: 74.2 MB

Columns: 26
Column Chunks:
  url_surtkey:         3.2 MB compressed
  url:                 4.1 MB compressed
  url_host_registered: 1.8 MB compressed
  ...
```

## Architecture

### Integration with Existing Archive System

Parquet files will use the same `ArchiveHandler` trait as tar/zip files:

```rust
// src/archive/parquet.rs

pub struct ParquetHandler;

#[async_trait]
impl ArchiveHandler for ParquetHandler {
    async fn build_index(
        &self,
        s3_client: Arc<S3Client>,
        bucket: &str,
        key: &str,
    ) -> Result<ArchiveIndex>;

    async fn extract_file(
        &self,
        s3_client: Arc<S3Client>,
        bucket: &str,
        key: &str,
        path: &str,
        index: &ArchiveIndex,
    ) -> Result<Bytes>;

    fn list_entries<'a>(
        &self,
        index: &'a ArchiveIndex,
        prefix: &str,
    ) -> Vec<&'a ArchiveEntry>;
}
```

### Virtual Directory Structure

The `build_index()` method will synthesize a virtual directory structure:

```
part-00000.parquet/
├── _schema.txt              # Virtual file (schema rendered as text)
├── columns/                 # Virtual directory
│   ├── url_surtkey         # Virtual file (column data accessor)
│   ├── url                 # Virtual file
│   ├── url_host_registered_domain/  # Nested struct becomes directory
│   │   ├── domain          # Virtual file
│   │   └── tld             # Virtual file
│   └── ...
├── stats/                   # Virtual directory
│   ├── url_surtkey         # Virtual file (stats for column)
│   ├── url                 # Virtual file
│   └── ...
└── row_groups/              # Virtual directory
    ├── row_group_0/        # Virtual directory
    │   ├── _info.txt       # Virtual file (row group metadata)
    │   └── columns/        # Virtual directory
    └── ...
```

### ArchiveIndex Structure

```rust
pub struct ArchiveIndex {
    pub entries: HashMap<String, ArchiveEntry>,
    pub metadata: HashMap<String, String>,  // Store Parquet metadata
}

pub struct ArchiveEntry {
    pub path: String,
    pub size: u64,
    pub is_dir: bool,
    pub entry_type: EntryType,  // New enum for different types
}

pub enum EntryType {
    TarFile { offset: u64 },
    ZipFile { local_header_offset: u64 },
    ParquetVirtual { handler: ParquetEntryHandler },  // New
}

pub enum ParquetEntryHandler {
    Schema,
    ColumnStats { column_name: String },
    ColumnData { column_name: String, column_path: Vec<String> },
    RowGroupInfo { row_group_id: usize },
}
```

### Arrow/Parquet Integration

Use the `arrow` and `parquet` crates with S3 object store integration:

```rust
use arrow::array::RecordBatch;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use object_store::aws::AmazonS3Builder;

pub struct ParquetReader {
    object_store: Arc<dyn ObjectStore>,
    metadata: Arc<ParquetMetaData>,
}

impl ParquetReader {
    pub async fn new(
        s3_client: Arc<S3Client>,
        bucket: &str,
        key: &str,
    ) -> Result<Self> {
        // Create object_store-compatible client
        let object_store = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .build()?;

        // Read Parquet footer metadata using range requests
        let meta = ParquetObjectReader::new(object_store, key)
            .get_metadata()
            .await?;

        Ok(Self {
            object_store: Arc::new(object_store),
            metadata: Arc::new(meta),
        })
    }

    pub async fn read_column(
        &self,
        column_name: &str,
        limit: Option<usize>,
    ) -> Result<RecordBatch> {
        let projection = vec![column_name];

        let builder = ParquetRecordBatchStreamBuilder::new(
            ParquetObjectReader::new(self.object_store.clone(), self.key)
        )
        .await?
        .with_projection(projection)
        .with_batch_size(limit.unwrap_or(8192));

        let mut stream = builder.build()?;

        // Read first batch
        stream.next().await.transpose()
    }

    pub fn get_column_stats(&self, column_name: &str) -> Result<ColumnStats> {
        // Extract from ParquetMetaData
        // Includes min/max/null_count from footer
    }
}
```

## Implementation Plan

### Phase 1: Core Infrastructure

1. **Add dependencies**
   ```toml
   arrow = "53.0"
   parquet = "53.0"
   object_store = { version = "0.11", features = ["aws"] }
   comfy-table = "7.0"  # For pretty table output
   ```

2. **Create ParquetHandler skeleton**
   - Implement `ArchiveHandler` trait
   - Register `.parquet` extension in `ArchiveCache`

3. **Basic metadata reading**
   - Use `ParquetObjectReader` to read footer
   - Extract schema, row group info, column stats

### Phase 2: Virtual Directory Structure

1. **Implement `build_index()`**
   - Create virtual entries for `_schema.txt`, `columns/`, `stats/`, `row_groups/`
   - Generate directory structure from schema (handle nested types)

2. **Implement `list_entries()`**
   - Return appropriate entries based on virtual path

### Phase 3: Data Extraction

1. **Implement `extract_file()` for virtual files**
   - `_schema.txt`: Render schema as human-readable text
   - `stats/<column>`: Format column statistics
   - `columns/<column>`: Read column data with limits

2. **Column data reading**
   - Use projection pushdown to read specific columns
   - Default limit (1000 rows?) for interactive use
   - Support for nested column paths

### Phase 4: Pretty Output

1. **Table formatting**
   - Use `comfy-table` for columnar output
   - Auto-detect column widths
   - Handle different data types (strings, numbers, timestamps)

2. **Column type rendering**
   - Strings: quoted
   - Numbers: right-aligned
   - Timestamps: formatted
   - Nulls: grayed out "NULL"
   - Binary: hex or `<binary>`

### Phase 5: Advanced Features

1. **Row group navigation**
   - Allow `cd row_groups/row_group_0/`
   - Read column data from specific row groups

2. **Nested type support**
   - Struct fields as subdirectories
   - Lists as arrays in output
   - Maps as key-value pairs

3. **Filtering/sampling**
   - Support for `cat columns/url | grep ".com$"`
   - Random sampling vs sequential

## Technical Considerations

### Memory Management

**Challenge**: Parquet files can be 100GB+, we can't load everything.

**Solution**:
- Use Arrow's streaming APIs
- Default to first N rows (configurable)
- Lazy evaluation - only fetch when `cat` is called
- Cache RecordBatch data in `ArchiveCache` (evict after use)

### S3 Range Requests

**Challenge**: Minimizing S3 API calls and data transfer.

**Solution**:
- `ParquetObjectReader` automatically optimizes range requests
- Footer metadata cached in `ArchiveIndex` (one-time read)
- Column chunks read on-demand
- Row group metadata included in footer (no extra requests)

### Performance

**Expected performance** (based on Arrow blog post):
- Metadata read: 1-2 requests, <1MB, ~100-200ms
- Schema display: Instant (cached in metadata)
- Stats display: Instant (from footer metadata)
- Column sample (1000 rows): 1-10 requests depending on row groups, ~500ms-2s

### Arrow/Object Store Integration

The `object_store` crate provides a unified interface for S3, but we already have an `S3Client`. Options:

1. **Use object_store directly** (recommended)
   - Create `AmazonS3Builder` from AWS config
   - Let `ParquetObjectReader` handle all I/O
   - Clean separation from existing S3Client

2. **Bridge S3Client to object_store**
   - Implement `object_store::ObjectStore` trait wrapping `S3Client`
   - More complex, but reuses existing infrastructure

**Recommendation**: Option 1 - use `object_store` for Parquet, keep `S3Client` for everything else.

### Nested Types Handling

Parquet supports complex nested types:

**Struct** → Directory:
```
columns/user_profile/
├── id
├── name
└── email
```

**List** → Single file with array output:
```bash
$ cat columns/tags
["rust", "programming"]
["aws", "cloud", "s3"]
["data", "analytics"]
```

**Map** → Single file with map output:
```bash
$ cat columns/metadata
{"key1": "value1", "key2": "value2"}
{"foo": "bar"}
```

## File Changes

### New Files

- `src/archive/parquet.rs` - ParquetHandler implementation
- `src/parquet/` (optional) - Parquet-specific utilities
  - `src/parquet/reader.rs` - Wrapper around Arrow ParquetObjectReader
  - `src/parquet/formatter.rs` - Pretty-printing for tables and stats
  - `src/parquet/virtual_fs.rs` - Virtual directory structure builder

### Modified Files

- `Cargo.toml` - Add arrow, parquet, object_store, comfy-table dependencies
- `src/archive/mod.rs` - Register ParquetHandler for `.parquet` extension
- `src/archive/mod.rs` - Update `ArchiveEntry` to support virtual files
- `src/shell/commands/cat.rs` - Handle multi-column output (table format)

### Unchanged Files

- `src/s3/client.rs` - Parquet uses `object_store` for S3 access
- `src/vfs/` - VFS abstraction already supports this
- `src/cache/` - Can reuse ArchiveCache with Parquet indexes

## Usage Examples

### Common Crawl Index Exploration

```bash
# Browse Common Crawl columnar indexes
s3sh --provider commoncrawl
s3sh:/ $ cd cc-index/table/cc-main/warc/crawl=CC-MAIN-2024-10/subset=warc/

# Pick a partition file
s3sh:/.../subset=warc/ $ ls
part-00000-tid-8788529717865660782-ad0d4022-4791-4228-9c2e-0a5965c520cd-3146-c000.snappy.parquet
part-00001-tid-8788529717865660782-ad0d4022-4791-4228-9c2e-0a5965c520cd-3147-c000.snappy.parquet
...

# Explore the structure
s3sh:/.../subset=warc/ $ cd part-00000-tid-8788529717865660782-ad0d4022-4791-4228-9c2e-0a5965c520cd-3146-c000.snappy.parquet
s3sh:/.../part-00000.parquet $ cat _schema.txt
message schema {
  required binary url_surtkey (STRING);
  required binary url (STRING);
  required binary url_host_name (STRING);
  required binary url_host_registered_domain (STRING);
  required binary url_host_tld (STRING);
  ...
}

# Check domain statistics
s3sh:/.../part-00000.parquet $ cat stats/url_host_registered_domain
Column: url_host_registered_domain
Type: String
Rows: 1,234,567
Min: "00000.com"
Max: "zzzz.com"
Distinct: ~456,789

# Sample some domains
s3sh:/.../part-00000.parquet $ cd columns/
s3sh:/.../columns $ cat url_host_registered_domain | head -20
example.com
wikipedia.org
github.com
stackoverflow.com
...

# Find WARC files for a specific domain
s3sh:/.../columns $ cat url_host_registered_domain warc_filename | grep "github.com" | head
github.com    CC-MAIN-20240101000000-20240101010000-00042.warc.gz
github.com    CC-MAIN-20240101000000-20240101010000-00043.warc.gz
github.com    CC-MAIN-20240101000000-20240101010000-00044.warc.gz
```

### Data Lake Exploration

```bash
# Explore a data lake
s3sh $ cd my-data-lake/events/year=2024/month=01/

# Check Parquet schema without downloading
s3sh:/.../month=01/ $ cd events_20240101.parquet
s3sh:/.../events_20240101.parquet $ cat _schema.txt

# Sample event data
s3sh:/.../events_20240101.parquet $ cd columns/
s3sh:/.../columns $ cat event_type user_id timestamp | head
┌─────────────┬──────────┬─────────────────────┐
│ event_type  │ user_id  │ timestamp           │
├─────────────┼──────────┼─────────────────────┤
│ page_view   │ 12345    │ 2024-01-01 00:00:12 │
│ click       │ 12345    │ 2024-01-01 00:00:15 │
│ page_view   │ 67890    │ 2024-01-01 00:00:23 │
└─────────────┴──────────┴─────────────────────┘
```

## Benefits

1. **No downloads required** - Explore 100GB+ Parquet files without downloading
2. **Instant schema discovery** - Understand data structure immediately
3. **Column sampling** - Preview specific columns interactively
4. **Statistics inspection** - Check data distribution before queries
5. **Reuses existing UX** - Same `cd`/`ls`/`cat` commands as archives
6. **Efficient S3 usage** - Arrow's optimized range requests minimize costs

## Future Enhancements

### Query Pushdown

Add filtering support:
```bash
s3sh:/.../columns $ cat url | where "contains(.com)"
s3sh:/.../columns $ cat fetch_time | where "> 2024-01-01"
```

### DuckDB Integration

Execute SQL queries on Parquet files:
```bash
s3sh:/.../part-00000.parquet $ query "SELECT url_host_registered_domain, COUNT(*) FROM columns GROUP BY 1 LIMIT 10"
```

### Column Histograms

Visualize data distribution:
```bash
s3sh:/.../stats $ cat url_host_tld/histogram
.com    ███████████████████████ 65%
.org    ████████ 18%
.net    ████ 9%
.edu    ██ 5%
...
```

### Export to CSV/JSON

```bash
s3sh:/.../columns $ cat url_host_registered_domain warc_filename --format csv > output.csv
s3sh:/.../columns $ cat * --format json | head -100 > sample.json
```

## Open Questions

1. **Default row limits**: What's a good default for `cat columns/foo`? 1000 rows? 10000?
2. **Large string columns**: Should we truncate long strings? Add `--width` flag?
3. **Binary columns**: How to display binary data? Hex dump? Base64? Just show `<binary>`?
4. **Column ordering**: Alphabetical or schema order?
5. **Nested JSON**: For map/struct types, pretty-print JSON or keep it compact?
6. **Pagination**: Should `cat` automatically paginate large outputs like `less`?
7. **S3 costs**: Should we warn when reading large columns? Show estimated cost?

## Success Metrics

The feature is successful if:

1. ✅ Can explore Common Crawl Parquet indexes without downloading
2. ✅ Schema display is instant (<100ms after metadata cached)
3. ✅ Column sampling (1000 rows) completes in <5 seconds
4. ✅ Statistics display is instant (from footer metadata)
5. ✅ Works with nested columns (structs, lists, maps)
6. ✅ Pretty table output is readable and DuckDB-like
7. ✅ Memory usage stays reasonable (<100MB for typical operations)
8. ✅ Reuses existing archive navigation UX seamlessly

## References

- [Apache Arrow Rust Documentation](https://arrow.apache.org/rust/parquet/index.html)
- [Parquet async_reader module](https://arrow.apache.org/rust/parquet/arrow/async_reader/index.html)
- [Querying Parquet with Millisecond Latency](https://arrow.apache.org/blog/2022/12/26/querying-parquet-with-millisecond-latency/)
- [Common Crawl Columnar Index Announcement](https://commoncrawl.org/blog/index-to-warc-files-and-urls-in-columnar-format)
- [Parquet Format Specification](https://parquet.apache.org/docs/file-format/)
