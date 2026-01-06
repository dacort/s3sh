use super::{Provider, ProviderConfig};
use anyhow::Result;

/// Source Cooperative provider for public geospatial data
pub struct SourceCoopProvider;

impl Default for SourceCoopProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceCoopProvider {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl Provider for SourceCoopProvider {
    fn name(&self) -> &str {
        "sourcecoop"
    }

    fn description(&self) -> &str {
        "Source Cooperative - Public geospatial data"
    }

    async fn build_config(&self) -> Result<ProviderConfig> {
        Ok(ProviderConfig {
            endpoint_url: Some("https://data.source.coop".to_string()),
            force_path_style: true,
            anonymous: true,
            default_region: Some("us-west-2".to_string()),
            disable_cross_region: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sourcecoop_provider_config() {
        let provider = SourceCoopProvider::new();
        assert_eq!(provider.name(), "sourcecoop");
        assert_eq!(
            provider.description(),
            "Source Cooperative - Public geospatial data"
        );

        let config = provider.build_config().await.unwrap();
        assert_eq!(
            config.endpoint_url,
            Some("https://data.source.coop".to_string())
        );
        assert_eq!(config.anonymous, true);
        assert_eq!(config.force_path_style, true);
        assert_eq!(config.default_region, Some("us-west-2".to_string()));
        assert_eq!(config.disable_cross_region, true);
    }
}
