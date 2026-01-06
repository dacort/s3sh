mod aws;
mod sourcecoop;

pub use aws::AwsProvider;
pub use sourcecoop::SourceCoopProvider;

use anyhow::Result;
use aws_sdk_s3::Client;
use std::collections::HashMap;

/// Configuration for creating an S3 client
#[derive(Debug, Clone)]
pub struct ProviderConfig {
    /// Optional custom endpoint URL
    pub endpoint_url: Option<String>,
    /// Whether to use path-style addressing (required for some S3-compatible services)
    pub force_path_style: bool,
    /// Whether to skip credentials (for anonymous/public access)
    pub anonymous: bool,
    /// Optional default region override
    pub default_region: Option<String>,
    /// Disable cross-region bucket support (for custom endpoints that don't support it)
    pub disable_cross_region: bool,
}

/// Trait for S3 provider implementations
/// Providers supply configuration for creating S3 clients
#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    /// Get the provider name
    fn name(&self) -> &str;

    /// Get provider description
    fn description(&self) -> &str;

    /// Build the provider configuration
    async fn build_config(&self) -> Result<ProviderConfig>;
}

/// Factory function to create S3Client from provider configuration
/// Returns (client, region, disable_cross_region)
pub async fn create_s3_client(config: ProviderConfig) -> Result<(Client, String, bool)> {
    let mut sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest());

    // Handle anonymous access
    if config.anonymous {
        sdk_config = sdk_config.no_credentials();
    }

    let base_config = sdk_config.load().await;

    // Determine default region
    let default_region = config
        .default_region
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

    Ok((client, default_region, config.disable_cross_region))
}

/// Registry of available providers
pub struct ProviderRegistry {
    providers: HashMap<String, Box<dyn Provider>>,
}

impl ProviderRegistry {
    /// Create a new registry with all built-in providers
    pub fn new() -> Self {
        let mut registry = Self {
            providers: HashMap::new(),
        };

        // Register built-in providers
        registry.register(Box::new(aws::AwsProvider::new()));
        registry.register(Box::new(sourcecoop::SourceCoopProvider::new()));

        registry
    }

    /// Register a provider
    pub fn register(&mut self, provider: Box<dyn Provider>) {
        self.providers.insert(provider.name().to_string(), provider);
    }

    /// Get a provider by name
    pub fn get(&self, name: &str) -> Option<&Box<dyn Provider>> {
        self.providers.get(name)
    }

    /// List all available providers
    pub fn list(&self) -> Vec<&str> {
        let mut names: Vec<_> = self.providers.keys().map(|s| s.as_str()).collect();
        names.sort();
        names
    }
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}
