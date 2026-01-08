use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use bytes::Bytes;
use std::sync::Arc;

use s3sh::cache::ArchiveCache;
use s3sh::s3::S3Client;
use s3sh::shell::commands::{Command, cat::CatCommand, cd::CdCommand};
use s3sh::shell::{CompletionCache, ShellState};
use s3sh::vfs::VfsNode;

/// Test bucket name
const TEST_BUCKET: &str = "test-bucket";

/// Helper function to create an S3 client pointing to localstack
async fn create_localstack_client() -> Client {
    let endpoint_url =
        std::env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| "http://localhost:4566".to_string());

    let config = aws_config::defaults(BehaviorVersion::latest())
        .region("us-east-1")
        .load()
        .await;

    // Build S3-specific config with endpoint
    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .endpoint_url(&endpoint_url)
        .force_path_style(true) // Required for LocalStack
        .build();

    Client::from_conf(s3_config)
}

/// Helper function to create a test ShellState with localstack
async fn create_test_shell() -> ShellState {
    let endpoint_url =
        std::env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| "http://localhost:4566".to_string());

    let config = aws_config::defaults(BehaviorVersion::latest())
        .region("us-east-1")
        .load()
        .await;

    // Build S3-specific config with endpoint
    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .endpoint_url(&endpoint_url)
        .force_path_style(true) // Required for LocalStack
        .build();

    let client = Client::from_conf(s3_config);
    let s3_client = Arc::new(S3Client::from_client(client, "us-east-1".to_string()));
    let cache = ArchiveCache::new(100);
    let completion_cache = CompletionCache::new(Arc::clone(&s3_client), cache.clone());

    let mut state = ShellState::from_components(VfsNode::Root, s3_client, cache, completion_cache);

    // Register commands
    state.register_command_pub(Arc::new(CdCommand));
    state.register_command_pub(Arc::new(CatCommand));

    state
}

/// Setup test environment with bucket and test files
async fn setup_test_bucket(client: &Client) {
    // Create bucket
    client
        .create_bucket()
        .bucket(TEST_BUCKET)
        .send()
        .await
        .expect("Failed to create test bucket");

    // Upload a simple text file
    let test_content = "Hello from S3!\n";
    client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("test.txt")
        .body(Bytes::from(test_content).into())
        .send()
        .await
        .expect("Failed to upload test.txt");

    // Upload a file in a prefix (directory-like structure)
    let nested_content = "This is a nested file\n";
    client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("dir/nested.txt")
        .body(Bytes::from(nested_content).into())
        .send()
        .await
        .expect("Failed to upload nested file");

    // Create a simple tar.gz archive for testing
    let archive_bytes = create_test_targz();
    client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("test.tar.gz")
        .body(Bytes::from(archive_bytes).into())
        .send()
        .await
        .expect("Failed to upload test.tar.gz");
}

/// Create a simple tar.gz archive with test files
fn create_test_targz() -> Vec<u8> {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tar::Builder;

    let mut archive_data = Vec::new();
    {
        let encoder = GzEncoder::new(&mut archive_data, Compression::default());
        let mut tar = Builder::new(encoder);

        // Add a simple text file to the archive root
        let file_content = b"Content inside archive\n";
        let mut header = tar::Header::new_gnu();
        header.set_path("archive_file.txt").unwrap();
        header.set_size(file_content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();

        tar.append(&header, &file_content[..]).unwrap();

        // Add a directory
        let mut dir_header = tar::Header::new_gnu();
        dir_header.set_path("app/").unwrap();
        dir_header.set_size(0);
        dir_header.set_mode(0o755);
        dir_header.set_entry_type(tar::EntryType::Directory);
        dir_header.set_cksum();
        tar.append(&dir_header, &[][..]).unwrap();

        // Add a file inside the directory
        let nested_content = b"Nested file content\n";
        let mut nested_header = tar::Header::new_gnu();
        nested_header.set_path("app/nested.txt").unwrap();
        nested_header.set_size(nested_content.len() as u64);
        nested_header.set_mode(0o644);
        nested_header.set_cksum();
        tar.append(&nested_header, &nested_content[..]).unwrap();

        tar.finish().unwrap();
    }

    archive_data
}

#[tokio::test]
#[ignore] // Run with: cargo test -- --ignored --test-threads=1
async fn test_cd_command() {
    let client = create_localstack_client().await;

    // Verify localstack is running
    let resp = client.list_buckets().send().await;
    assert!(
        resp.is_ok(),
        "Failed to connect to Localstack S3. Is it running on localhost:4566?"
    );

    // Setup test environment
    setup_test_bucket(&client).await;

    // Create shell state
    let mut shell = create_test_shell().await;

    // Test: cd into bucket
    let cd_cmd = CdCommand;
    cd_cmd
        .execute(&mut shell, &[TEST_BUCKET.to_string()])
        .await
        .expect("Failed to cd into bucket");

    // Verify we're in the bucket
    match shell.current_node() {
        VfsNode::Bucket { name } => {
            assert_eq!(name, TEST_BUCKET);
        }
        _ => panic!("Expected to be in bucket node"),
    }

    // Test: cd into prefix (directory)
    cd_cmd
        .execute(&mut shell, &["dir".to_string()])
        .await
        .expect("Failed to cd into dir");

    // Verify we're in the prefix
    match shell.current_node() {
        VfsNode::Prefix { bucket, prefix } => {
            assert_eq!(bucket, TEST_BUCKET);
            assert_eq!(prefix.trim_end_matches('/'), "dir");
        }
        _ => panic!("Expected to be in prefix node"),
    }

    // Test: cd back to root
    cd_cmd
        .execute(&mut shell, &["/".to_string()])
        .await
        .expect("Failed to cd to root");

    assert!(
        matches!(shell.current_node(), VfsNode::Root),
        "Expected to be at root"
    );
}

#[tokio::test]
#[ignore] // Run with: cargo test -- --ignored --test-threads=1
async fn test_cat_command() {
    let client = create_localstack_client().await;

    // Verify localstack is running
    let resp = client.list_buckets().send().await;
    assert!(resp.is_ok(), "Failed to connect to Localstack S3");

    // Setup test environment
    setup_test_bucket(&client).await;

    // Create shell state
    let mut shell = create_test_shell().await;

    // Navigate to bucket
    let cd_cmd = CdCommand;
    cd_cmd
        .execute(&mut shell, &[TEST_BUCKET.to_string()])
        .await
        .expect("Failed to cd into bucket");

    // Test: cat a simple file
    let cat_cmd = CatCommand;
    let result = cat_cmd.execute(&mut shell, &["test.txt".to_string()]).await;
    assert!(result.is_ok(), "Failed to cat test.txt");

    // Test: cat a nested file with absolute path
    let result = cat_cmd
        .execute(&mut shell, &[format!("/{TEST_BUCKET}/dir/nested.txt")])
        .await;
    assert!(
        result.is_ok(),
        "Failed to cat nested file with absolute path"
    );
}

#[tokio::test]
#[ignore] // Run with: cargo test -- --ignored --test-threads=1
async fn test_cd_into_archive() {
    let client = create_localstack_client().await;

    // Verify localstack is running
    let resp = client.list_buckets().send().await;
    assert!(resp.is_ok(), "Failed to connect to Localstack S3");

    // Setup test environment
    setup_test_bucket(&client).await;

    // Create shell state
    let mut shell = create_test_shell().await;

    // Navigate to bucket
    let cd_cmd = CdCommand;
    cd_cmd
        .execute(&mut shell, &[TEST_BUCKET.to_string()])
        .await
        .expect("Failed to cd into bucket");

    // Test: cd into tar.gz archive
    let result = cd_cmd
        .execute(&mut shell, &["test.tar.gz".to_string()])
        .await;
    assert!(result.is_ok(), "Failed to cd into test.tar.gz archive");

    // Verify we're in the archive
    match shell.current_node() {
        VfsNode::Archive { .. } => {
            // Success - we're in the archive
        }
        _ => panic!("Expected to be in archive node after cd into tar.gz"),
    }
}

#[tokio::test]
#[ignore] // Run with: cargo test -- --ignored --test-threads=1
async fn test_cd_parent_from_archive() {
    let client = create_localstack_client().await;

    // Verify localstack is running
    let resp = client.list_buckets().send().await;
    assert!(resp.is_ok(), "Failed to connect to Localstack S3");

    // Setup test environment
    setup_test_bucket(&client).await;

    // Create shell state
    let mut shell = create_test_shell().await;

    // Navigate to bucket
    let cd_cmd = CdCommand;
    cd_cmd
        .execute(&mut shell, &[TEST_BUCKET.to_string()])
        .await
        .expect("Failed to cd into bucket");

    // Verify we're in the bucket
    match shell.current_node() {
        VfsNode::Bucket { name } => {
            assert_eq!(name, TEST_BUCKET);
        }
        _ => panic!("Expected to be in bucket node"),
    }

    // Test: cd into tar.gz archive
    cd_cmd
        .execute(&mut shell, &["test.tar.gz".to_string()])
        .await
        .expect("Failed to cd into test.tar.gz archive");

    // Verify we're in the archive
    match shell.current_node() {
        VfsNode::Archive { .. } => {
            // Success - we're in the archive
        }
        _ => panic!("Expected to be in archive node after cd into tar.gz"),
    }

    // Test: cd .. from archive root should go back to bucket
    cd_cmd
        .execute(&mut shell, &["..".to_string()])
        .await
        .expect("Failed to cd .. from archive root");

    // Verify we're back in the bucket
    match shell.current_node() {
        VfsNode::Bucket { name } => {
            assert_eq!(name, TEST_BUCKET);
        }
        _ => panic!("Expected to be in bucket after cd .. from archive"),
    }

    // Navigate back into archive and then into a subdirectory
    cd_cmd
        .execute(&mut shell, &["test.tar.gz".to_string()])
        .await
        .expect("Failed to cd into test.tar.gz archive");

    // cd into app directory within archive
    cd_cmd
        .execute(&mut shell, &["app".to_string()])
        .await
        .expect("Failed to cd into app directory in archive");

    // Verify we're in the archive entry
    match shell.current_node() {
        VfsNode::ArchiveEntry { path, is_dir, .. } => {
            assert_eq!(path, "app");
            assert!(is_dir, "Expected app to be a directory");
        }
        _ => panic!("Expected to be in archive entry node"),
    }

    // Test: cd .. from archive subdirectory should go back to archive root
    cd_cmd
        .execute(&mut shell, &["..".to_string()])
        .await
        .expect("Failed to cd .. from archive subdirectory");

    // Verify we're back at the archive root
    match shell.current_node() {
        VfsNode::Archive { .. } => {
            // Success - we're back at archive root
        }
        _ => panic!("Expected to be at archive root after cd .. from subdirectory"),
    }

    // Test: cd .. again from archive root should go back to bucket
    cd_cmd
        .execute(&mut shell, &["..".to_string()])
        .await
        .expect("Failed to cd .. from archive root (second time)");

    // Verify we're back in the bucket
    match shell.current_node() {
        VfsNode::Bucket { name } => {
            assert_eq!(name, TEST_BUCKET);
        }
        _ => panic!("Expected to be in bucket after cd .. from archive (second time)"),
    }
}
