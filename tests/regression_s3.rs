//! Regression tests for s3sh archive performance and functionality.
//!
//! These tests run against real S3 buckets and require AWS authentication.
//! Configure via environment variables:
//! - REGRESSION_BUCKET: S3 bucket name (required)
//! - REGRESSION_ZIP_KEY: Path to large zip file for testing (optional)
//! - REGRESSION_TARGZ_KEY: Path to large tar.gz file for testing (optional)
//!
//! Run with: cargo test --test regression_s3 -- --ignored --nocapture

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use std::sync::Arc;
use std::time::{Duration, Instant};

use s3sh::cache::ArchiveCache;
use s3sh::s3::{S3Client, S3Metrics};
use s3sh::shell::commands::{Command, cat::CatCommand, cd::CdCommand, ls::LsCommand};
use s3sh::shell::{CompletionCache, ShellState};
use s3sh::vfs::VfsNode;

/// Test configuration from environment
struct RegressionConfig {
    bucket: String,
    zip_key: Option<String>,
    targz_key: Option<String>,
}

impl RegressionConfig {
    fn from_env() -> Option<Self> {
        let bucket = std::env::var("REGRESSION_BUCKET").ok()?;
        let zip_key = std::env::var("REGRESSION_ZIP_KEY").ok();
        let targz_key = std::env::var("REGRESSION_TARGZ_KEY").ok();

        // Require at least one archive key
        if zip_key.is_none() && targz_key.is_none() {
            return None;
        }

        Some(Self {
            bucket,
            zip_key,
            targz_key,
        })
    }

    fn skip_message() -> &'static str {
        "Skipping: REGRESSION_BUCKET and at least one of REGRESSION_ZIP_KEY or REGRESSION_TARGZ_KEY must be set"
    }
}

/// Performance thresholds
mod thresholds {
    use std::time::Duration;

    /// Maximum time for zip cd operation (central directory read)
    pub const ZIP_CD_MAX: Duration = Duration::from_secs(10);

    /// Calculate tar.gz threshold based on file size
    /// Assumes 1 Gbps connection = 125 MB/s theoretical max
    /// Use 50 MB/s as realistic threshold (40% efficiency)
    pub fn targz_cd_max(file_size_bytes: u64) -> Duration {
        let mb = file_size_bytes as f64 / (1024.0 * 1024.0);
        let seconds = mb / 50.0; // 50 MB/s
        Duration::from_secs_f64(seconds.max(5.0)) // At least 5 seconds
    }
}

/// Create S3 client with metrics enabled
async fn create_s3_client_with_metrics() -> (Arc<S3Client>, Arc<S3Metrics>) {
    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

    let s3_config = aws_sdk_s3::config::Builder::from(&config).build();
    let client = Client::from_conf(s3_config);

    let metrics = S3Metrics::new();
    let region = config
        .region()
        .map(|r| r.as_ref().to_string())
        .unwrap_or_else(|| "us-east-1".to_string());

    let s3_client = Arc::new(S3Client::from_client_with_metrics(
        client,
        region,
        false,
        Some(Arc::clone(&metrics)),
    ));

    (s3_client, metrics)
}

/// Create test shell with metrics-enabled client
async fn create_test_shell_with_metrics() -> (ShellState, Arc<S3Metrics>) {
    let (s3_client, metrics) = create_s3_client_with_metrics().await;
    let cache = ArchiveCache::new(100);
    let completion_cache = CompletionCache::new(Arc::clone(&s3_client), cache.clone());

    let mut state = ShellState::from_components(VfsNode::Root, s3_client, cache, completion_cache);

    // Register commands
    state.register_command_pub(Arc::new(CdCommand));
    state.register_command_pub(Arc::new(CatCommand));
    state.register_command_pub(Arc::new(LsCommand));

    (state, metrics)
}

/// Helper to print metrics summary
fn print_metrics_summary(metrics: &S3Metrics, operation: &str, elapsed: Duration) {
    let bytes = metrics.total_bytes();
    let requests = metrics.request_count();
    let request_time = metrics.total_request_time();

    println!("\n=== {operation} Metrics ===");
    println!("Total elapsed time: {:.2?}", elapsed);
    println!(
        "Total bytes transferred: {} ({:.2} MB)",
        bytes,
        bytes as f64 / (1024.0 * 1024.0)
    );
    println!("Request count: {}", requests);
    println!("Total request time: {:.2?}", request_time);
    if requests > 0 {
        println!(
            "Average request time: {:.2?}",
            request_time / requests as u32
        );
        println!(
            "Average bytes per request: {:.2} KB",
            bytes as f64 / requests as f64 / 1024.0
        );
    }

    // Print individual requests (first 10 and last 5)
    let all_requests = metrics.requests();
    if !all_requests.is_empty() {
        println!("\nRequest details (showing first 10 and last 5):");
        println!(
            "{:<8} {:>12} {:>12} {:>10}",
            "Request", "Offset", "Length", "Duration"
        );

        let show_first = all_requests.len().min(10);
        for (i, req) in all_requests.iter().take(show_first).enumerate() {
            println!(
                "{:<8} {:>12} {:>12} {:>10.2?}",
                i + 1,
                req.offset,
                req.length,
                req.duration
            );
        }

        if all_requests.len() > 15 {
            println!("... {} requests omitted ...", all_requests.len() - 15);
        }

        if all_requests.len() > 10 {
            let skip = all_requests.len().saturating_sub(5).max(10);
            for (i, req) in all_requests.iter().enumerate().skip(skip) {
                println!(
                    "{:<8} {:>12} {:>12} {:>10.2?}",
                    i + 1,
                    req.offset,
                    req.length,
                    req.duration
                );
            }
        }
    }
    println!("===========================\n");
}

// =============================================================================
// Performance Tests
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_perf_cd_into_zip() {
    let config = match RegressionConfig::from_env() {
        Some(c) => c,
        None => {
            println!("{}", RegressionConfig::skip_message());
            return;
        }
    };

    let zip_key = match &config.zip_key {
        Some(k) => k,
        None => {
            println!("Skipping: REGRESSION_ZIP_KEY not set");
            return;
        }
    };

    let (mut shell, metrics) = create_test_shell_with_metrics().await;

    // Navigate to bucket
    let cd_cmd = CdCommand;
    cd_cmd
        .execute(&mut shell, &[config.bucket.clone()])
        .await
        .expect("Failed to cd into bucket");

    // Reset metrics before timing
    metrics.reset();
    metrics.start_operation();

    let start = Instant::now();

    // CD into zip archive
    cd_cmd
        .execute(&mut shell, &[zip_key.clone()])
        .await
        .expect("Failed to cd into zip archive");

    let elapsed = start.elapsed();

    print_metrics_summary(&metrics, "ZIP cd", elapsed);

    // Assert performance threshold
    assert!(
        elapsed < thresholds::ZIP_CD_MAX,
        "ZIP cd took {:.2?}, expected < {:?}. \
            Bytes transferred: {} in {} requests",
        elapsed,
        thresholds::ZIP_CD_MAX,
        metrics.total_bytes(),
        metrics.request_count()
    );

    // Verify we're in the archive
    assert!(
        matches!(shell.current_node(), VfsNode::Archive { .. }),
        "Expected to be in archive node"
    );
}

#[tokio::test]
#[ignore]
async fn test_perf_cd_into_targz() {
    let config = match RegressionConfig::from_env() {
        Some(c) => c,
        None => {
            println!("{}", RegressionConfig::skip_message());
            return;
        }
    };

    let targz_key = match &config.targz_key {
        Some(k) => k,
        None => {
            println!("Skipping: REGRESSION_TARGZ_KEY not set");
            return;
        }
    };

    let (mut shell, metrics) = create_test_shell_with_metrics().await;

    // First, get file size to calculate threshold
    let file_size = shell
        .s3_client()
        .head_object(&config.bucket, targz_key)
        .await
        .expect("Failed to get tar.gz metadata")
        .size;

    println!(
        "tar.gz file size: {} bytes ({:.2} MB)",
        file_size,
        file_size as f64 / (1024.0 * 1024.0)
    );

    let threshold = thresholds::targz_cd_max(file_size);
    println!("Performance threshold: {:.2?}", threshold);

    // Navigate to bucket
    let cd_cmd = CdCommand;
    cd_cmd
        .execute(&mut shell, &[config.bucket.clone()])
        .await
        .expect("Failed to cd into bucket");

    // Reset metrics before timing
    metrics.reset();
    metrics.start_operation();

    let start = Instant::now();

    // CD into tar.gz archive
    cd_cmd
        .execute(&mut shell, &[targz_key.clone()])
        .await
        .expect("Failed to cd into tar.gz archive");

    let elapsed = start.elapsed();

    print_metrics_summary(&metrics, "TAR.GZ cd", elapsed);

    // For tar.gz, we expect to download the entire file
    // Verify reasonable efficiency (at least 25% of theoretical max)
    let bytes_per_sec = metrics.total_bytes() as f64 / elapsed.as_secs_f64();
    let mbps = bytes_per_sec / (1024.0 * 1024.0);
    println!("Effective throughput: {:.2} MB/s", mbps);

    // Assert performance threshold
    assert!(
        elapsed < threshold,
        "TAR.GZ cd took {:.2?}, expected < {:?}. \
         Bytes transferred: {} ({:.2} MB/s)",
        elapsed,
        threshold,
        metrics.total_bytes(),
        mbps
    );
}

// =============================================================================
// Functionality Tests
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_func_zip_cd_ls() {
    let config = match RegressionConfig::from_env() {
        Some(c) => c,
        None => {
            println!("{}", RegressionConfig::skip_message());
            return;
        }
    };

    let zip_key = match &config.zip_key {
        Some(k) => k,
        None => {
            println!("Skipping: REGRESSION_ZIP_KEY not set");
            return;
        }
    };

    let (mut shell, metrics) = create_test_shell_with_metrics().await;

    // cd into bucket
    let cd_cmd = CdCommand;
    cd_cmd
        .execute(&mut shell, &[config.bucket.clone()])
        .await
        .expect("Failed to cd into bucket");

    // cd into zip
    metrics.reset();
    cd_cmd
        .execute(&mut shell, &[zip_key.clone()])
        .await
        .expect("Failed to cd into zip archive");

    println!(
        "cd into zip: {} bytes, {} requests",
        metrics.total_bytes(),
        metrics.request_count()
    );

    // Verify we're in archive
    assert!(matches!(shell.current_node(), VfsNode::Archive { .. }));

    // ls at root of archive
    metrics.reset();
    let ls_cmd = LsCommand;
    ls_cmd
        .execute(&mut shell, &[])
        .await
        .expect("Failed to ls in archive");

    println!(
        "ls in archive: {} bytes, {} requests",
        metrics.total_bytes(),
        metrics.request_count()
    );

    // ls should not require additional S3 requests (index is cached)
    assert_eq!(
        metrics.request_count(),
        0,
        "ls should not make additional S3 requests"
    );

    // cd .. back out
    cd_cmd
        .execute(&mut shell, &["..".to_string()])
        .await
        .expect("Failed to cd ..");

    // Verify we're back in bucket or prefix (depending on where the archive was located)
    assert!(
        matches!(
            shell.current_node(),
            VfsNode::Bucket { .. } | VfsNode::Prefix { .. }
        ),
        "Expected to be out of archive, got {:?}",
        shell.current_node()
    );
}

#[tokio::test]
#[ignore]
async fn test_func_targz_cd_ls() {
    let config = match RegressionConfig::from_env() {
        Some(c) => c,
        None => {
            println!("{}", RegressionConfig::skip_message());
            return;
        }
    };

    let targz_key = match &config.targz_key {
        Some(k) => k,
        None => {
            println!("Skipping: REGRESSION_TARGZ_KEY not set");
            return;
        }
    };

    let (mut shell, metrics) = create_test_shell_with_metrics().await;

    // cd into bucket
    let cd_cmd = CdCommand;
    cd_cmd
        .execute(&mut shell, &[config.bucket.clone()])
        .await
        .expect("Failed to cd into bucket");

    // cd into tar.gz
    metrics.reset();
    cd_cmd
        .execute(&mut shell, &[targz_key.clone()])
        .await
        .expect("Failed to cd into tar.gz archive");

    println!(
        "cd into tar.gz: {} bytes, {} requests",
        metrics.total_bytes(),
        metrics.request_count()
    );

    // Verify we're in archive
    assert!(matches!(shell.current_node(), VfsNode::Archive { .. }));

    // ls at root of archive
    metrics.reset();
    let ls_cmd = LsCommand;
    ls_cmd
        .execute(&mut shell, &[])
        .await
        .expect("Failed to ls in archive");

    println!(
        "ls in archive: {} bytes, {} requests",
        metrics.total_bytes(),
        metrics.request_count()
    );

    // ls should not require additional S3 requests (index is cached)
    assert_eq!(
        metrics.request_count(),
        0,
        "ls should not make additional S3 requests"
    );

    // cd .. back out
    cd_cmd
        .execute(&mut shell, &["..".to_string()])
        .await
        .expect("Failed to cd ..");

    // Verify we're back in bucket or prefix (depending on where the archive was located)
    assert!(
        matches!(
            shell.current_node(),
            VfsNode::Bucket { .. } | VfsNode::Prefix { .. }
        ),
        "Expected to be out of archive, got {:?}",
        shell.current_node()
    );
}

#[tokio::test]
#[ignore]
async fn test_func_archive_navigation_roundtrip() {
    let config = match RegressionConfig::from_env() {
        Some(c) => c,
        None => {
            println!("{}", RegressionConfig::skip_message());
            return;
        }
    };

    // Use either zip or tar.gz
    let archive_key = config
        .zip_key
        .as_ref()
        .or(config.targz_key.as_ref())
        .unwrap();

    let (mut shell, _metrics) = create_test_shell_with_metrics().await;
    let cd_cmd = CdCommand;

    // Full navigation: / -> bucket -> archive -> .. -> /

    // Start at root
    assert!(matches!(shell.current_node(), VfsNode::Root));

    // cd bucket
    cd_cmd
        .execute(&mut shell, &[config.bucket.clone()])
        .await
        .expect("cd bucket");
    assert!(matches!(shell.current_node(), VfsNode::Bucket { .. }));

    // cd archive
    cd_cmd
        .execute(&mut shell, &[archive_key.clone()])
        .await
        .expect("cd archive");
    assert!(matches!(shell.current_node(), VfsNode::Archive { .. }));

    // cd .. (back to bucket or prefix, depending on archive location)
    cd_cmd
        .execute(&mut shell, &["..".to_string()])
        .await
        .expect("cd ..");
    assert!(
        matches!(
            shell.current_node(),
            VfsNode::Bucket { .. } | VfsNode::Prefix { .. }
        ),
        "Expected to be out of archive, got {:?}",
        shell.current_node()
    );

    // cd / (back to root)
    cd_cmd
        .execute(&mut shell, &["/".to_string()])
        .await
        .expect("cd /");
    assert!(matches!(shell.current_node(), VfsNode::Root));
}

// =============================================================================
// Metrics Verification Tests
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_metrics_bytes_tracking() {
    let config = match RegressionConfig::from_env() {
        Some(c) => c,
        None => {
            println!("{}", RegressionConfig::skip_message());
            return;
        }
    };

    let archive_key = config
        .zip_key
        .as_ref()
        .or(config.targz_key.as_ref())
        .unwrap();

    let (mut shell, metrics) = create_test_shell_with_metrics().await;

    // Navigate to archive
    let cd_cmd = CdCommand;
    cd_cmd
        .execute(&mut shell, &[config.bucket.clone()])
        .await
        .unwrap();

    metrics.reset();
    cd_cmd
        .execute(&mut shell, &[archive_key.clone()])
        .await
        .unwrap();

    // Verify metrics were collected
    assert!(metrics.total_bytes() > 0, "Should have transferred bytes");
    assert!(metrics.request_count() > 0, "Should have made requests");

    // Verify request details
    let requests = metrics.requests();
    assert!(!requests.is_empty(), "Should have request records");

    // Verify all bytes are accounted for
    let sum_bytes: u64 = requests.iter().map(|r| r.bytes).sum();
    assert_eq!(
        sum_bytes,
        metrics.total_bytes(),
        "Sum of request bytes should equal total"
    );

    // Verify timing makes sense
    for req in &requests {
        assert!(
            req.duration.as_nanos() > 0,
            "Request should have non-zero duration"
        );
        assert!(req.bytes > 0, "Request should have transferred bytes");
    }
}
