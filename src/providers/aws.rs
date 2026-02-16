use super::{Provider, ProviderConfig};
use anyhow::Result;

/// AWS S3 provider (default)
pub struct AwsProvider;

impl Default for AwsProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl AwsProvider {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl Provider for AwsProvider {
    fn name(&self) -> &str {
        "aws"
    }

    fn description(&self) -> &str {
        "Amazon Web Services S3 (default)"
    }

    async fn build_config(&self) -> Result<ProviderConfig> {
        // Check if a custom endpoint is configured via environment variable
        // This allows using S3-compatible services like MinIO
        let endpoint_url = std::env::var("AWS_ENDPOINT_URL").ok();
        let has_custom_endpoint = endpoint_url.is_some();

        Ok(ProviderConfig {
            endpoint_url,
            force_path_style: has_custom_endpoint,
            anonymous: false,
            default_region: None,
            // Disable cross-region support when using custom endpoints
            // as S3-compatible services may not support region discovery
            disable_cross_region: has_custom_endpoint,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_aws_provider_config_default() {
        // Ensure AWS_ENDPOINT_URL is not set for this test
        unsafe {
            std::env::remove_var("AWS_ENDPOINT_URL");
        }

        let provider = AwsProvider::new();
        assert_eq!(provider.name(), "aws");
        assert_eq!(provider.description(), "Amazon Web Services S3 (default)");

        let config = provider.build_config().await.unwrap();
        assert_eq!(config.endpoint_url, None);
        assert_eq!(config.anonymous, false);
        assert_eq!(config.force_path_style, false);
        assert_eq!(config.default_region, None);
        assert_eq!(config.disable_cross_region, false);
    }

    #[tokio::test]
    async fn test_aws_provider_config_with_custom_endpoint() {
        // Set a custom endpoint URL
        unsafe {
            std::env::set_var("AWS_ENDPOINT_URL", "https://play.minio.io:9000");
        }

        let provider = AwsProvider::new();
        let config = provider.build_config().await.unwrap();

        assert_eq!(
            config.endpoint_url,
            Some("https://play.minio.io:9000".to_string())
        );
        assert_eq!(config.force_path_style, true);
        assert_eq!(config.disable_cross_region, true);

        // Clean up
        unsafe {
            std::env::remove_var("AWS_ENDPOINT_URL");
        }
    }
}
