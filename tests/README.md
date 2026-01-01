# Integration Tests

This directory contains integration tests for s3sh that use LocalStack to simulate AWS S3.

## Prerequisites

The integration tests require LocalStack to be running. You can start LocalStack using Docker:

```bash
docker run --rm -d -p 4566:4566 localstack/localstack
```

Alternatively, install and run LocalStack using the CLI:

```bash
pip install localstack
localstack start -d
```

Verify LocalStack is running:

```bash
curl http://localhost:4566/_localstack/health
```

## Running the Tests

The integration tests are marked with `#[ignore]` to prevent them from running without LocalStack.

To run the integration tests:

```bash
# Make sure LocalStack is running first
cargo test --test integration_s3 -- --ignored --test-threads=1
```

To run all tests (unit + integration):

```bash
# With LocalStack running
cargo test -- --ignored --test-threads=1
```

## Test Coverage

The integration tests cover the following core functionality:

1. **CD Command** (`test_cd_command`)
   - Navigate into S3 buckets
   - Navigate into prefixes (directories)
   - Navigate back to root

2. **Cat Command** (`test_cat_command`)
   - Read file contents from S3
   - Read nested files with absolute paths

3. **Archive Navigation** (`test_cd_into_archive`)
   - Navigate into tar.gz archives stored in S3

## CI/CD

The tests run automatically in GitHub Actions using the LocalStack action. See `.github/workflows/ci.yml` for the CI configuration.
