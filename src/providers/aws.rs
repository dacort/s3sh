use super::{Provider, ProviderConfig};
use anyhow::Result;

/// AWS S3 provider (default)
pub struct AwsProvider;

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
        Ok(ProviderConfig {
            endpoint_url: None,
            force_path_style: false,
            anonymous: false,
            default_region: None,
            disable_cross_region: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_aws_provider_config() {
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
}
