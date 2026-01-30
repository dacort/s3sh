# s3sh - The S3 Shell

## Overview
s3sh is an interactive S3 shell for exploring S3-compatible storage with Unix-like commands. Navigate S3 buckets and prefixes like directories, and seamlessly explore archive contents (tar, tar.gz, tar.bz2, zip, parquet) without downloading entire files. Supports multiple providers including AWS S3 and Source Cooperative for accessing public geospatial data.

## Key Features
- **Multi-Provider Support** - Access AWS S3, Source Coop, and other S3-compatible storage services
- **Unix-like Commands** - Use familiar `ls`, `cd`, `cat`, `pwd` commands to navigate S3
- **Archive Navigation** - `cd` directly into tar/zip/parquet files and explore their contents
- **Parquet Exploration** - Navigate parquet files like directories, view schemas and column data
- **Efficient Streaming** - Uses S3 range requests to access archive contents without full downloads
- **Interactive Shell** - Full command history and line editing via rustyline

## Installation
Available via crates.io:
```bash
# Basic installation (tar, zip support)
cargo install s3sh

# With parquet support (recommended for data files)
cargo install s3sh --features parquet
```

Or build from source:
```bash
git clone https://github.com/dacort/s3sh.git
cd s3sh

# Basic build
cargo build --release

# With parquet support
cargo build --release --features parquet
```

## Usage

Launch the interactive shell:
```bash
s3sh
```

### Providers

s3sh supports multiple S3-compatible storage providers through a plugin system. Use the `--provider` flag to select a provider:

```bash
# Use AWS S3 (default)
s3sh

# Use Source Coop for public geospatial data
s3sh --provider sourcecoop

# List available providers
s3sh --list-providers
```

#### AWS Provider (default)

Standard AWS S3 access with full cross-region support. Requires AWS credentials.

```bash
s3sh
# or explicitly
s3sh --provider aws
```

#### Source Coop Provider

Access public geospatial datasets from [Source Cooperative](https://source.coop) without credentials:

```bash
s3sh --provider sourcecoop

s3sh:/ $ cd kerner-lab/fields-of-the-world
s3sh:/kerner-lab/fields-of-the-world $ ls
cambodia/
croatia/
denmark/
...
README.md
ftw-sources.pmtiles

s3sh:/kerner-lab/fields-of-the-world $ cat README.md
```

Available Source Coop datasets include:
- **cholera** - Historical cholera data
- **kerner-lab** - Fields of the World agricultural field boundaries
- **gistemp** - NASA GISS Surface Temperature Analysis
- And many more public geospatial datasets

### Basic Commands

Navigate S3 like a filesystem:
```bash
# List buckets at root
s3sh:/ $ ls

# Navigate into a bucket
s3sh:/ $ cd my-bucket

# List objects and prefixes
s3sh:/my-bucket $ ls

# Navigate through prefixes
s3sh:/my-bucket $ cd logs/2024/

# View file contents
s3sh:/my-bucket/logs/2024 $ cat error.log

# Show current location
s3sh:/my-bucket/logs/2024 $ pwd
```

### Archive Navigation

Explore archives without downloading:
```bash
# Navigate into an archive
s3sh:/my-bucket $ cd backups/data.tar.gz

# List archive contents
s3sh:/my-bucket/backups/data.tar.gz $ ls

# Navigate within the archive
s3sh:/my-bucket/backups/data.tar.gz $ cd configs/

# View files from inside archives
s3sh:/my-bucket/backups/data.tar.gz/configs $ cat app.yml
```

### Parquet File Navigation

Explore parquet files as virtual directories (requires `--features parquet`):
```bash
# Navigate into a parquet file
s3sh:/my-bucket $ cd data/users.parquet

# View the schema
s3sh:/my-bucket/data/users.parquet $ cat _schema.txt
Parquet Schema
==============

Rows: 1000000
Row Groups: 10

Columns:
--------
  id : INT64 (required)
  name : STRING (nullable)
  email : STRING (nullable)
  created_at : TIMESTAMP(Microsecond, UTC) (nullable)

# Navigate to columns directory
s3sh:/my-bucket/data/users.parquet $ cd columns

# List available columns
s3sh:/my-bucket/data/users.parquet/columns $ ls
created_at
email
id
name

# View column data (first 100 rows)
s3sh:/my-bucket/data/users.parquet/columns $ cat name
Alice
Bob
Charlie
...

# View column statistics
s3sh:/my-bucket/data/users.parquet $ cd stats
s3sh:/my-bucket/data/users.parquet/stats $ cat email
Column: email
=============================================

Type: STRING
Nullable: true

Statistics:
-----------
  Min Value: "alice@example.com"
  Max Value: "zoe@example.com"
  Total Rows: 1000000
  Null Count: 42
  Null %: 0.00%
```

### Tab Completion

Smart completion based on context:
```bash
# Tab completes bucket names
s3sh:/ $ cd my-<TAB>

# Tab completes objects and prefixes
s3sh:/my-bucket $ cd log<TAB>

# cd only shows directories and navigable archives
s3sh:/my-bucket $ cd <TAB>
logs/  backups/  data.tar.gz  users.parquet  # Directories and archives

# Works inside archives too
s3sh:/my-bucket/users.parquet $ cd <TAB>
columns/  stats/

s3sh:/my-bucket/users.parquet/columns $ cd e<TAB>
email

# cat shows all files
s3sh:/my-bucket $ cat <TAB>
logs/  data.json  config.yml  # Files and directories

## Supported Archive Formats
- **Tar** - `.tar`
- **Gzip Tar** - `.tar.gz`, `.tgz`
- **Bzip2 Tar** - `.tar.bz2`, `.tbz2`
- **Zip** - `.zip`
- **Parquet** - `.parquet` (requires `--features parquet`)
  - View schema information
  - Browse columns as virtual files
  - Access column statistics
  - Preview column data (first 100 rows)

## Authentication

### AWS Provider

The AWS provider uses the AWS SDK for Rust and respects standard AWS credential configuration:
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- IAM instance profile (when running on EC2)

Required IAM permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:ListAllMyBuckets"
      ],
      "Resource": "*"
    }
  ]
}
```

### Source Coop Provider

No authentication required. The Source Coop provider accesses public datasets anonymously.

## Technical Details

### Architecture
- **VFS Abstraction** - Unified virtual filesystem for S3 objects and archive entries
- **Lazy Archive Indexing** - Archives are indexed on first access and cached
- **S3 Range Requests** - Efficient random access to archive contents
- **Async Runtime** - Built on Tokio for concurrent S3 operations

### Performance
- **LRU Caching** - Archive indexes are cached to avoid repeated S3 calls
- **Streaming** - Large files are streamed, not loaded into memory
- **Parallel Listings** - Tab completion fetches directory contents on-demand

## Development

### Running Regression Tests

The regression test suite validates performance and functionality against real S3 data. Configure the following environment variables:

```bash
export REGRESSION_BUCKET=your-bucket-name
export REGRESSION_ZIP_KEY=path/to/test-file.zip
export REGRESSION_TARGZ_KEY=path/to/test-file.tar.gz
```

Run the tests:
```bash
# Run all regression tests
cargo test --test regression_s3 -- --ignored --nocapture

# Run specific test categories
cargo test --test regression_s3 test_perf -- --ignored --nocapture     # Performance tests
cargo test --test regression_s3 test_func -- --ignored --nocapture     # Functionality tests
cargo test --test regression_s3 test_metrics -- --ignored --nocapture  # Metrics validation
```

The tests verify:
- **Performance** - Archive indexing completes within expected thresholds
- **Functionality** - Navigation (`cd`, `ls`, `cat`) works correctly in archives
- **Metrics** - Bytes transferred and request counts are accurately tracked

## Contributing
Contributions are welcome! Please feel free to submit issues or pull requests.

## License
MIT License - see LICENSE file for details

## Related Projects
- [s3grep](https://github.com/dacort/s3grep) - Fast parallel grep for S3 buckets
