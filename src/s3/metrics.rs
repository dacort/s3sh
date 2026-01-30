//! Metrics collection for S3 operations.
//!
//! This module provides thread-safe tracking of S3 request metrics including
//! bytes transferred, request count, and timing information.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Metrics for a single S3 request
#[derive(Debug, Clone)]
pub struct RequestMetric {
    /// Number of bytes transferred
    pub bytes: u64,
    /// Duration of the request
    pub duration: Duration,
    /// Byte offset of the request
    pub offset: u64,
    /// Requested length
    pub length: u64,
}

/// Collector for S3 operation metrics.
///
/// Thread-safe metrics collection for tracking S3 API calls,
/// bytes transferred, and timing information.
#[derive(Debug, Default)]
pub struct S3Metrics {
    /// Total bytes transferred
    total_bytes: AtomicU64,
    /// Total number of requests
    request_count: AtomicUsize,
    /// Total time spent in requests (nanoseconds)
    total_request_time_ns: AtomicU64,
    /// Individual request records (for detailed analysis)
    requests: RwLock<Vec<RequestMetric>>,
    /// Start time of the operation
    operation_start: RwLock<Option<Instant>>,
}

impl S3Metrics {
    /// Create a new metrics collector wrapped in Arc for sharing
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Start timing an operation
    pub fn start_operation(&self) {
        let mut start = self.operation_start.write().unwrap();
        *start = Some(Instant::now());
    }

    /// Record a completed request
    pub fn record_request(&self, bytes: u64, duration: Duration, offset: u64, length: u64) {
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.total_request_time_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);

        let mut requests = self.requests.write().unwrap();
        requests.push(RequestMetric {
            bytes,
            duration,
            offset,
            length,
        });
    }

    /// Get total bytes transferred
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Get request count
    pub fn request_count(&self) -> usize {
        self.request_count.load(Ordering::Relaxed)
    }

    /// Get total request time
    pub fn total_request_time(&self) -> Duration {
        Duration::from_nanos(self.total_request_time_ns.load(Ordering::Relaxed))
    }

    /// Get elapsed time since operation start
    pub fn operation_elapsed(&self) -> Option<Duration> {
        self.operation_start.read().unwrap().map(|s| s.elapsed())
    }

    /// Get all individual request metrics
    pub fn requests(&self) -> Vec<RequestMetric> {
        self.requests.read().unwrap().clone()
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.total_bytes.store(0, Ordering::Relaxed);
        self.request_count.store(0, Ordering::Relaxed);
        self.total_request_time_ns.store(0, Ordering::Relaxed);
        self.requests.write().unwrap().clear();
        *self.operation_start.write().unwrap() = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_tracking() {
        let metrics = S3Metrics::new();

        metrics.record_request(1000, Duration::from_millis(50), 0, 1000);
        metrics.record_request(2000, Duration::from_millis(100), 1000, 2000);

        assert_eq!(metrics.total_bytes(), 3000);
        assert_eq!(metrics.request_count(), 2);
        assert_eq!(metrics.total_request_time(), Duration::from_millis(150));

        let requests = metrics.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].bytes, 1000);
        assert_eq!(requests[1].bytes, 2000);
    }

    #[test]
    fn test_metrics_reset() {
        let metrics = S3Metrics::new();

        metrics.record_request(1000, Duration::from_millis(50), 0, 1000);
        assert_eq!(metrics.total_bytes(), 1000);

        metrics.reset();
        assert_eq!(metrics.total_bytes(), 0);
        assert_eq!(metrics.request_count(), 0);
        assert!(metrics.requests().is_empty());
    }

    #[test]
    fn test_operation_timing() {
        let metrics = S3Metrics::new();

        assert!(metrics.operation_elapsed().is_none());

        metrics.start_operation();
        std::thread::sleep(Duration::from_millis(10));

        let elapsed = metrics.operation_elapsed().unwrap();
        assert!(elapsed >= Duration::from_millis(10));
    }
}
