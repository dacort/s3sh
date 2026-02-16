# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1](https://github.com/dacort/s3sh/compare/v0.2.0...v0.2.1) - 2026-02-16

### Fixed

- custom S3 endpoint support by detecting AWS_ENDPOINT_URL ([#23](https://github.com/dacort/s3sh/pull/23))

## [0.2.0](https://github.com/dacort/s3sh/compare/v0.1.2...v0.2.0) - 2026-01-30

### Added

- Allow piping to the shell ([#13](https://github.com/dacort/s3sh/pull/13))
- Initial parquet support ([#11](https://github.com/dacort/s3sh/pull/11))

### Fixed

- zip performance ([#18](https://github.com/dacort/s3sh/pull/18))

### Other

- Improve tar listing memory efficiency with streaming ([#16](https://github.com/dacort/s3sh/pull/16))
- Add perf tests ([#17](https://github.com/dacort/s3sh/pull/17))

### Added
- Unix-style pipe support for streaming command output to external tools
  - Pipe `ls` output to `grep`, `sort`, `head`, etc.
  - Pipe `cat` output to `jq`, `less`, `wc`, and other utilities
  - Support for complex shell pipelines with multiple pipes
  - Uses file descriptor redirection for efficient streaming
  - Unix-only feature (requires Unix platform)

## [0.1.2](https://github.com/dacort/s3sh/compare/v0.1.1...v0.1.2) - 2026-01-07

### Other

- Add archive file auto-completion for cd command
- Add design docs to the repo :)
- Merge pull request #6 from dacort/copilot/sub-pr-5
- Address code review feedback: remove ineffective assertion and redundant variable
- Add comprehensive tests for create_s3_client and ProviderRegistry
- cargo clippy && fmt
- cargo clippy && fmt
- Add readme details about new provider system
- Add provider support, with source coop as the first example

## [0.1.1](https://github.com/dacort/s3sh/compare/v0.1.0...v0.1.1) - 2026-01-02

### Other

- Address code review feedback: improve comments and formatting
- Fix cd .. from archive root and add integration tests
- so many fmt
- Updated version of clippy
- cargo fmt again
- Fix cargo clippy errors
- Apply cargo clippy fixes
- cargo fmt
- Add basic integration tests
