# s3sh - The S3 Shell

## Overview
s3sh is an interactive S3 shell for exploring S3-compatible storage with Unix-like commands. Navigate S3 buckets and prefixes like directories, and seamlessly explore archive contents (tar, tar.gz, tar.bz2, zip) without downloading entire files. Supports multiple providers including AWS S3 and Source Cooperative for accessing public geospatial data.

## Key Features
- **Multi-Provider Support** - Access AWS S3, Source Coop, and other S3-compatible storage services
- **Unix-like Commands** - Use familiar `ls`, `cd`, `cat`, `pwd` commands to navigate S3
- **Archive Navigation** - `cd` directly into tar/zip files and explore their contents
- **Efficient Streaming** - Uses S3 range requests to access archive contents without full downloads
- **Interactive Shell** - Full command history and line editing via rustyline

## Installation
Available via crates.io:
```bash
cargo install s3sh
```

Or build from source:
```bash
git clone https://github.com/dacort/s3sh.git
cd s3sh
cargo build --release
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

### Tab Completion

Smart completion based on context:
```bash
# Tab completes bucket names
s3sh:/ $ cd my-<TAB>

# Tab completes objects and prefixes
s3sh:/my-bucket $ cd log<TAB>

# cd only shows directories
s3sh:/my-bucket $ cd <TAB>
logs/  backups/  configs/  # Only directories shown

# cat shows all files
s3sh:/my-bucket $ cat <TAB>
logs/  data.json  config.yml  # Files and directories

## Supported Archive Formats
- **Tar** - `.tar`
- **Gzip Tar** - `.tar.gz`, `.tgz`
- **Bzip2 Tar** - `.tar.bz2`, `.tbz2`
- **Zip** - `.zip`

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

## Contributing
Contributions are welcome! Please feel free to submit issues or pull requests.

## License
MIT License - see LICENSE file for details

## Related Projects
- [s3grep](https://github.com/dacort/s3grep) - Fast parallel grep for S3 buckets
