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
    pub fn get(&self, name: &str) -> Option<&dyn Provider> {
        self.providers.get(name).map(|b| b.as_ref())
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

#[cfg(test)]
mod tests {
    use super::*;

    // Mock provider for testing
    struct MockProvider {
        name: String,
        description: String,
    }

    impl MockProvider {
        fn new(name: &str, description: &str) -> Self {
            Self {
                name: name.to_string(),
                description: description.to_string(),
            }
        }
    }

    #[async_trait::async_trait]
    impl Provider for MockProvider {
        fn name(&self) -> &str {
            &self.name
        }

        fn description(&self) -> &str {
            &self.description
        }

        async fn build_config(&self) -> Result<ProviderConfig> {
            Ok(ProviderConfig {
                endpoint_url: None,
                force_path_style: false,
                anonymous: false,
                default_region: None,
                disable_cross_region: false,
            })
        }
    }

    #[tokio::test]
    async fn test_create_s3_client_default() {
        let config = ProviderConfig {
            endpoint_url: None,
            force_path_style: false,
            anonymous: false,
            default_region: Some("us-east-1".to_string()),
            disable_cross_region: false,
        };

        let result = create_s3_client(config).await;
        assert!(result.is_ok());

        let (_client, region, disable_cross_region) = result.unwrap();
        assert_eq!(region, "us-east-1");
        assert_eq!(disable_cross_region, false);
    }

    #[tokio::test]
    async fn test_create_s3_client_anonymous() {
        let config = ProviderConfig {
            endpoint_url: None,
            force_path_style: false,
            anonymous: true,
            default_region: Some("us-west-2".to_string()),
            disable_cross_region: false,
        };

        let result = create_s3_client(config).await;
        assert!(result.is_ok());

        let (_, region, disable_cross_region) = result.unwrap();
        assert_eq!(region, "us-west-2");
        assert_eq!(disable_cross_region, false);
    }

    #[tokio::test]
    async fn test_create_s3_client_custom_endpoint() {
        let config = ProviderConfig {
            endpoint_url: Some("https://s3.custom.com".to_string()),
            force_path_style: false,
            anonymous: false,
            default_region: Some("custom-region".to_string()),
            disable_cross_region: false,
        };

        let result = create_s3_client(config).await;
        assert!(result.is_ok());

        let (_, region, _) = result.unwrap();
        assert_eq!(region, "custom-region");
    }

    #[tokio::test]
    async fn test_create_s3_client_force_path_style() {
        let config = ProviderConfig {
            endpoint_url: Some("https://s3.custom.com".to_string()),
            force_path_style: true,
            anonymous: true,
            default_region: Some("us-west-2".to_string()),
            disable_cross_region: true,
        };

        let result = create_s3_client(config).await;
        assert!(result.is_ok());

        let (_, region, disable_cross_region) = result.unwrap();
        assert_eq!(region, "us-west-2");
        assert_eq!(disable_cross_region, true);
    }

    #[tokio::test]
    async fn test_create_s3_client_default_region_fallback() {
        let config = ProviderConfig {
            endpoint_url: None,
            force_path_style: false,
            anonymous: false,
            default_region: None, // No default region set
            disable_cross_region: false,
        };

        let result = create_s3_client(config).await;
        assert!(result.is_ok());

        let (_, region, _) = result.unwrap();
        // Should fall back to us-west-2 if no region is configured
        assert!(!region.is_empty());
    }

    #[test]
    fn test_provider_registry_new() {
        let registry = ProviderRegistry::new();

        // Verify built-in providers are registered
        assert!(registry.get("aws").is_some());
        assert!(registry.get("sourcecoop").is_some());

        // Verify list contains both providers
        let providers = registry.list();
        assert_eq!(providers.len(), 2);
        assert!(providers.contains(&"aws"));
        assert!(providers.contains(&"sourcecoop"));
    }

    #[test]
    fn test_provider_registry_register() {
        let mut registry = ProviderRegistry::new();
        let initial_count = registry.list().len();

        // Register a custom provider
        let custom_provider = Box::new(MockProvider::new("custom", "Custom S3 Provider"));
        registry.register(custom_provider);

        // Verify provider was registered
        assert!(registry.get("custom").is_some());
        assert_eq!(registry.list().len(), initial_count + 1);
    }

    #[test]
    fn test_provider_registry_get_existing() {
        let registry = ProviderRegistry::new();

        // Test getting existing providers
        let aws_provider = registry.get("aws");
        assert!(aws_provider.is_some());
        assert_eq!(aws_provider.unwrap().name(), "aws");

        let sourcecoop_provider = registry.get("sourcecoop");
        assert!(sourcecoop_provider.is_some());
        assert_eq!(sourcecoop_provider.unwrap().name(), "sourcecoop");
    }

    #[test]
    fn test_provider_registry_get_non_existing() {
        let registry = ProviderRegistry::new();

        // Test getting non-existing provider
        let non_existing = registry.get("nonexistent");
        assert!(non_existing.is_none());
    }

    #[test]
    fn test_provider_registry_list_sorted() {
        let mut registry = ProviderRegistry::new();

        // Register providers in non-alphabetical order
        registry.register(Box::new(MockProvider::new("zebra", "Zebra Provider")));
        registry.register(Box::new(MockProvider::new("alpha", "Alpha Provider")));
        registry.register(Box::new(MockProvider::new("beta", "Beta Provider")));

        // Get list and verify it's sorted
        let providers = registry.list();

        // Check that providers are in alphabetical order
        for i in 1..providers.len() {
            assert!(providers[i - 1] <= providers[i]);
        }

        // Verify specific providers are included
        assert!(providers.contains(&"alpha"));
        assert!(providers.contains(&"aws"));
        assert!(providers.contains(&"beta"));
        assert!(providers.contains(&"sourcecoop"));
        assert!(providers.contains(&"zebra"));
    }

    #[test]
    fn test_provider_registry_default() {
        let registry = ProviderRegistry::default();

        // Default should behave the same as new()
        assert!(registry.get("aws").is_some());
        assert!(registry.get("sourcecoop").is_some());
        assert_eq!(registry.list().len(), 2);
    }
}
