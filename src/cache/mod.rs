use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};

use crate::vfs::ArchiveIndex;

/// In-memory cache for archive indexes
pub struct ArchiveCache {
    /// LRU cache mapping S3 URIs to archive indexes
    cache: Arc<RwLock<LruCache<String, Arc<ArchiveIndex>>>>,
}

impl ArchiveCache {
    /// Create a new archive cache with a maximum number of entries
    pub fn new(capacity: usize) -> Self {
        let cache = LruCache::new(NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(100).unwrap()));
        ArchiveCache {
            cache: Arc::new(RwLock::new(cache)),
        }
    }

    /// Get an archive index from the cache
    pub fn get(&self, key: &str) -> Option<Arc<ArchiveIndex>> {
        let mut cache = self.cache.write().ok()?;
        cache.get(key).cloned()
    }

    /// Put an archive index into the cache
    pub fn put(&self, key: String, index: Arc<ArchiveIndex>) {
        if let Ok(mut cache) = self.cache.write() {
            cache.put(key, index);
        }
    }

    /// Clear the cache
    pub fn clear(&self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.clear();
        }
    }

    /// Get cache statistics
    pub fn len(&self) -> usize {
        self.cache.read().ok().map(|c| c.len()).unwrap_or(0)
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Clone for ArchiveCache {
    fn clone(&self) -> Self {
        ArchiveCache {
            cache: Arc::clone(&self.cache),
        }
    }
}
