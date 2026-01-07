# Plugin System for s3sh

## Overview

Add a provider-based plugin system to s3sh that allows different S3-compatible services to be configured via command-line flags at startup. The first provider will be **Source Coop** (https://docs.source.coop/data-proxy), which provides public geospatial data via an S3-compatible proxy.

## Motivation

Users want to access S3-compatible services beyond AWS S3, such as:
- **Source Coop** - Public geospatial data at `https://data.source.coop`
- **MinIO** - Self-hosted object storage
- **GCS** - Google Cloud Storage (future)
- Custom S3-compatible endpoints

These services require:
- Custom endpoint URLs
- Different authentication methods (anonymous, different credential providers)
- Service-specific configurations (path-style addressing, etc.)

## Architecture

### Provider-as-Configuration Pattern

Instead of having providers create S3 clients directly, we use a **configuration-based approach**:

1. **Providers** supply configuration parameters (endpoint, credentials, region)
2. **Factory function** creates S3Client from provider configuration
3. **S3Client** remains unchanged (preserves existing cross-region logic)

**Benefits:**
- ✅ Zero changes to S3Client
- ✅ Preserves existing cross-region bucket support
- ✅ All providers use the same AWS SDK Client type
- ✅ Easy to test (mock configurations)
- ✅ Backward compatible

### Module Structure

```
src/
├── providers/
│   ├── mod.rs          # Core trait, factory, registry
│   ├── aws.rs          # AWS provider (default)
│   └── sourcecoop.rs   # Source Coop provider
├── s3/
│   └── client.rs       # UNCHANGED
├── shell/
│   └── mod.rs          # Add with_client() constructor
└── main.rs             # CLI parsing + provider selection
```

## Implementation Details

### 1. Provider Trait

```rust
// src/providers/mod.rs

#[derive(Debug, Clone)]
pub struct ProviderConfig {
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub anonymous: bool,
    pub default_region: Option<String>,
}

#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    async fn build_config(&self) -> Result<ProviderConfig>;
}
```

### 2. Factory Function

```rust
// src/providers/mod.rs

pub async fn create_s3_client(config: ProviderConfig) -> Result<(Client, String)> {
    let mut sdk_config = aws_config::from_env();

    // Handle anonymous access
    if config.anonymous {
        sdk_config = sdk_config.no_credentials();
    }

    let base_config = sdk_config.load().await;

    // Determine default region
    let default_region = config.default_region
        .or_else(|| base_config.region().map(|r| r.as_ref().to_string()))
        .unwrap_or_else(|| "us-west-2".to_string());

    // Build S3-specific config
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&base_config);

    if let Some(endpoint) = config.endpoint_url {
        s3_config_builder = s3_config_builder.endpoint_url(endpoint);
    }

    if config.force_path_style {
        s3_config_builder = s3_config_builder.force_path_style(true);
    }

    let s3_config = s3_config_builder.build();
    let client = Client::from_conf(s3_config);

    Ok((client, default_region))
}
```

### 3. AWS Provider (Default)

```rust
// src/providers/aws.rs

pub struct AwsProvider;

#[async_trait::async_trait]
impl Provider for AwsProvider {
    fn name(&self) -> &str { "aws" }
    fn description(&self) -> &str { "Amazon Web Services S3 (default)" }

    async fn build_config(&self) -> Result<ProviderConfig> {
        Ok(ProviderConfig {
            endpoint_url: None,
            force_path_style: false,
            anonymous: false,
            default_region: None,
        })
    }
}
```

### 4. Source Coop Provider

```rust
// src/providers/sourcecoop.rs

pub struct SourceCoopProvider;

#[async_trait::async_trait]
impl Provider for SourceCoopProvider {
    fn name(&self) -> &str { "sourcecoop" }
    fn description(&self) -> &str { "Source Cooperative - Public geospatial data" }

    async fn build_config(&self) -> Result<ProviderConfig> {
        Ok(ProviderConfig {
            endpoint_url: Some("https://data.source.coop".to_string()),
            force_path_style: true,
            anonymous: true,
            default_region: Some("us-west-2".to_string()),
        })
    }
}
```

### 5. Provider Registry

```rust
// src/providers/mod.rs

pub struct ProviderRegistry {
    providers: HashMap<String, Box<dyn Provider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            providers: HashMap::new(),
        };

        // Register built-in providers
        registry.register(Box::new(aws::AwsProvider::new()));
        registry.register(Box::new(sourcecoop::SourceCoopProvider::new()));

        registry
    }

    pub fn get(&self, name: &str) -> Option<&Box<dyn Provider>> {
        self.providers.get(name)
    }

    pub fn list(&self) -> Vec<&str> {
        let mut names: Vec<_> = self.providers.keys().map(|s| s.as_str()).collect();
        names.sort();
        names
    }
}
```

### 6. CLI Integration (main.rs)

```rust
use clap::Parser;

#[derive(Parser)]
#[command(name = "s3sh")]
#[command(about = "The S3 Shell - Navigate S3 buckets like a Unix shell")]
struct Args {
    /// S3 provider to use (aws, sourcecoop)
    #[arg(short, long, default_value = "aws")]
    provider: String,

    /// List available providers and exit
    #[arg(long)]
    list_providers: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.list_providers {
        print_available_providers();
        return Ok(());
    }

    let registry = providers::ProviderRegistry::new();
    let provider = registry.get(&args.provider)
        .ok_or_else(|| anyhow::anyhow!("Unknown provider: {}", args.provider))?;

    // Create S3 client from provider
    let provider_config = provider.build_config().await?;
    let (client, region) = providers::create_s3_client(provider_config).await?;

    // Wrap in S3Client and create shell state
    let s3_client = Arc::new(s3::S3Client::from_client(client, region));
    let mut state = shell::ShellState::with_client(s3_client).await?;

    // ... rest of REPL loop ...
}
```

### 7. ShellState Update

```rust
// src/shell/mod.rs

impl ShellState {
    // Existing method (unchanged)
    pub async fn new() -> Result<Self> {
        let s3_client = Arc::new(S3Client::new().await?);
        Self::with_client(s3_client).await
    }

    // New method for provider support
    pub async fn with_client(s3_client: Arc<S3Client>) -> Result<Self> {
        let cache = ArchiveCache::new(100);
        let completion_cache = CompletionCache::new(Arc::clone(&s3_client));

        let mut state = ShellState {
            current_node: VfsNode::Root,
            s3_client,
            cache,
            completion_cache,
            commands: HashMap::new(),
        };

        // Register commands
        state.register_command(Arc::new(commands::ls::LsCommand));
        state.register_command(Arc::new(commands::cd::CdCommand));
        state.register_command(Arc::new(commands::cat::CatCommand));

        Ok(state)
    }
}
```

## File Changes

### New Files
- `src/providers/mod.rs` - Core trait, factory function, registry
- `src/providers/aws.rs` - AWS provider implementation
- `src/providers/sourcecoop.rs` - Source Coop provider implementation

### Modified Files
- `src/lib.rs` - Add `pub mod providers;`
- `src/main.rs` - CLI parsing and provider integration
- `src/shell/mod.rs` - Add `with_client()` constructor
- `Cargo.toml` - Add `clap = { version = "4.5", features = ["derive"] }`

### Unchanged Files
- `src/s3/client.rs` - Already has `from_client()` method, no changes needed
- All other files remain unchanged

## Usage Examples

```bash
# Use default AWS provider (requires AWS credentials)
s3sh

# Use Source Coop provider (anonymous access to public data)
s3sh --provider sourcecoop

# List available providers
s3sh --list-providers
```

Example session with Source Coop:
```bash
$ s3sh --provider sourcecoop
============================================================
  s3sh - The S3 Shell
  Navigate S3 buckets like a Unix shell
============================================================
Provider: sourcecoop (Source Cooperative - Public geospatial data)

Type 'help' for available commands or 'exit' to quit

s3://> ls
cholera/
debian-security/
gistemp/
...
```

## Testing Strategy

### Unit Tests
```rust
#[tokio::test]
async fn test_aws_provider_config() {
    let provider = AwsProvider::new();
    let config = provider.build_config().await.unwrap();
    assert_eq!(config.anonymous, false);
}

#[tokio::test]
async fn test_sourcecoop_provider_config() {
    let provider = SourceCoopProvider::new();
    let config = provider.build_config().await.unwrap();
    assert_eq!(config.anonymous, true);
    assert_eq!(config.endpoint_url, Some("https://data.source.coop".to_string()));
}
```

### Integration Tests
```rust
#[tokio::test]
async fn test_sourcecoop_provider_creates_client() {
    let provider = SourceCoopProvider::new();
    let config = provider.build_config().await.unwrap();
    let result = create_s3_client(config).await;
    assert!(result.is_ok());
}
```

## Future Extensions

This architecture makes it easy to add:

### 1. Custom Endpoint Provider
```rust
pub struct CustomProvider {
    endpoint_url: String,
    anonymous: bool,
}
```

### 2. Environment Variable Support
```bash
export S3SH_PROVIDER=sourcecoop
s3sh  # Uses sourcecoop provider
```

### 3. Provider-Specific CLI Flags
```bash
s3sh --provider custom --endpoint-url http://localhost:9000 --anonymous
```

### 4. GCS Provider
```rust
pub struct GcsProvider;

impl Provider for GcsProvider {
    async fn build_config(&self) -> Result<ProviderConfig> {
        Ok(ProviderConfig {
            endpoint_url: Some("https://storage.googleapis.com".to_string()),
            force_path_style: false,
            anonymous: false,
            default_region: Some("us-central1".to_string()),
        })
    }
}
```

### 5. Configuration File Support
```yaml
# ~/.s3sh.yml
default_provider: sourcecoop
providers:
  my-minio:
    endpoint: http://localhost:9000
    anonymous: false
```

## Technical Notes

### Anonymous Authentication

The Source Coop provider uses AWS SDK's `no_credentials()` method:

```rust
let config = aws_config::from_env()
    .no_credentials()
    .load()
    .await;
```

This is available in `aws-config 1.5+` and creates an S3 client that doesn't sign requests, allowing access to public/anonymous endpoints.

### Cross-Region Support

The existing S3Client cross-region logic is preserved. For providers with custom endpoints (like Source Coop), the regional client cache may not be used, but the code path remains functional.

### Backward Compatibility

The existing `ShellState::new()` method is preserved, ensuring backward compatibility for tests and any code that doesn't need provider support.

## Dependencies

New dependency:
```toml
clap = { version = "4.5", features = ["derive"] }
```

Existing dependencies used:
- `aws-config = "1.5"` - Has `no_credentials()` method
- `aws-sdk-s3 = "1.68"` - S3 SDK
- `async-trait = "0.1"` - Already in dependencies

## References

- [Source Coop Data Proxy Documentation](https://docs.source.coop/data-proxy)
- [AWS SDK Rust Anonymous Credentials Discussion](https://github.com/awslabs/aws-sdk-rust/issues/425)
- [AWS SDK ConfigLoader::no_credentials()](https://docs.rs/aws-config/latest/aws_config/struct.ConfigLoader.html#method.no_credentials)
