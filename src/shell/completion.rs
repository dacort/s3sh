use rustyline::Context;
use rustyline::completion::{Completer, Pair};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::s3::S3Client;
use crate::vfs::VfsNode;

/// Entry in completion cache with metadata
#[derive(Clone, Debug)]
pub struct CompletionEntry {
    pub name: String,
    pub is_dir: bool,
}

/// Cache of available completions for different paths
#[derive(Clone)]
pub struct CompletionCache {
    /// Cached entries by path (path -> entries with metadata)
    entries: Arc<RwLock<HashMap<String, Vec<CompletionEntry>>>>,
    /// Available commands
    commands: Vec<String>,
    /// Current VFS node
    current_node: Arc<RwLock<VfsNode>>,
    /// S3 client for lazy loading
    s3_client: Arc<S3Client>,
}

impl CompletionCache {
    pub fn new(s3_client: Arc<S3Client>) -> Self {
        CompletionCache {
            entries: Arc::new(RwLock::new(HashMap::new())),
            commands: vec![
                "ls".to_string(),
                "cd".to_string(),
                "cat".to_string(),
                "pwd".to_string(),
                "help".to_string(),
                "exit".to_string(),
            ],
            current_node: Arc::new(RwLock::new(VfsNode::Root)),
            s3_client,
        }
    }

    /// Update the current node
    pub fn set_current_node(&self, node: VfsNode) {
        if let Ok(mut current) = self.current_node.write() {
            *current = node;
        }
    }

    /// Get the current node
    pub fn get_current_node(&self) -> VfsNode {
        self.current_node
            .read()
            .ok()
            .map(|n| n.clone())
            .unwrap_or(VfsNode::Root)
    }

    /// Update the cached entries for a specific path
    pub fn update_entries(&self, path: String, entries: Vec<CompletionEntry>) {
        if let Ok(mut cache) = self.entries.write() {
            cache.insert(path, entries);
        }
    }

    /// Get cached entries for a path
    pub fn get_entries(&self, path: &str) -> Option<Vec<CompletionEntry>> {
        self.entries
            .read()
            .ok()
            .and_then(|cache| cache.get(path).cloned())
    }

    /// Get available commands
    pub fn get_commands(&self) -> Vec<String> {
        self.commands.clone()
    }

    /// Get S3 client
    pub fn s3_client(&self) -> &Arc<S3Client> {
        &self.s3_client
    }
}

/// Tab completion helper for the shell
pub struct ShellCompleter {
    cache: CompletionCache,
}

impl ShellCompleter {
    pub fn new(cache: CompletionCache) -> Self {
        ShellCompleter { cache }
    }

    /// Complete a command at the start of the line
    fn complete_command(&self, line: &str) -> Vec<Pair> {
        self.cache
            .get_commands()
            .into_iter()
            .filter(|cmd| cmd.starts_with(line))
            .map(|cmd| Pair {
                display: cmd.clone(),
                replacement: cmd,
            })
            .collect()
    }

    /// Complete a path (file or directory)
    fn complete_path(&self, path: &str, command: &str) -> Vec<Pair> {
        // Determine which directory we're completing in
        let (dir_path, file_prefix) = if path.contains('/') {
            // Multi-segment path like "movies/" or "movies/id"
            let last_slash = path.rfind('/').unwrap();
            let dir = &path[..last_slash + 1]; // Include the trailing slash
            let prefix = &path[last_slash + 1..];
            (dir, prefix)
        } else {
            // Single segment in current directory
            ("", path)
        };

        // Get the cache key for this directory
        let cache_key = self.get_cache_key_for_path(dir_path);

        // Try to get cached entries
        let entries = if let Some(cached) = self.cache.get_entries(&cache_key) {
            cached
        } else {
            // Not cached - fetch it lazily
            match self.fetch_entries_for_path(dir_path) {
                Ok(entries) => {
                    // Cache for future use
                    self.cache
                        .update_entries(cache_key.clone(), entries.clone());
                    entries
                }
                Err(_) => return Vec::new(),
            }
        };

        // Filter and format completions
        entries
            .into_iter()
            .filter(|entry| {
                // Filter by prefix
                if !entry.name.starts_with(file_prefix) {
                    return false;
                }
                // Filter by command: cd only shows directories
                if command == "cd" && !entry.is_dir {
                    return false;
                }
                true
            })
            .map(|entry| {
                let replacement = if dir_path.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{}{}", dir_path, entry.name)
                };
                Pair {
                    display: entry.name,
                    replacement,
                }
            })
            .collect()
    }

    /// Get cache key for a path relative to current location
    fn get_cache_key_for_path(&self, rel_path: &str) -> String {
        let current = self.cache.get_current_node();

        if rel_path.is_empty() {
            // Current directory
            return self.node_to_cache_key(&current);
        }

        // Handle relative paths
        if rel_path.starts_with("..") {
            // Parent directory - compute from current
            return self.get_parent_cache_key(&current, rel_path);
        }

        // Child path - append to current
        let current_key = self.node_to_cache_key(&current);
        if current_key == "/" {
            format!("/{}", rel_path.trim_matches('/'))
        } else {
            format!("{}{}", current_key.trim_end_matches('/'), rel_path)
        }
    }

    /// Convert VfsNode to a cache key (path string)
    fn node_to_cache_key(&self, node: &VfsNode) -> String {
        match node {
            VfsNode::Root => "/".to_string(),
            VfsNode::Bucket { name } => format!("/{name}"),
            VfsNode::Prefix { bucket, prefix } => {
                format!("/{}/{}", bucket, prefix.trim_end_matches('/'))
            }
            _ => "/".to_string(), // Simplified for now
        }
    }

    /// Get parent directory cache key
    fn get_parent_cache_key(&self, current: &VfsNode, rel_path: &str) -> String {
        let mut key = self.node_to_cache_key(current);

        // Count how many levels up to go
        let up_count = rel_path.chars().filter(|&c| c == '.').count() / 2;

        for _ in 0..up_count {
            if let Some(pos) = key.trim_end_matches('/').rfind('/') {
                if pos == 0 {
                    key = "/".to_string();
                } else {
                    key = key[..pos].to_string();
                }
            }
        }

        // Add any remaining path after ../
        if let Some(suffix) = rel_path
            .strip_prefix("../")
            .or_else(|| rel_path.strip_prefix(".."))
        {
            if !suffix.is_empty() {
                key = format!("{}/{}", key.trim_end_matches('/'), suffix);
            }
        }

        key
    }

    /// Fetch entries for a path (blocks on async S3 call)
    fn fetch_entries_for_path(&self, rel_path: &str) -> Result<Vec<CompletionEntry>, ()> {
        let current = self.cache.get_current_node();
        let s3_client = self.cache.s3_client().clone();
        let rel_path = rel_path.to_string();

        // Use a channel to bridge sync completion with async S3 calls
        let (tx, rx) = std::sync::mpsc::channel();

        // Spawn task in existing tokio runtime
        let handle = tokio::runtime::Handle::try_current().map_err(|_| ())?;
        let current_clone = current.clone();

        handle.spawn(async move {
            let result =
                Self::fetch_entries_async_static(&s3_client, &current_clone, &rel_path).await;
            let _ = tx.send(result);
        });

        // Block on receiving result
        rx.recv().map_err(|_| ())?
    }

    /// Static async helper to fetch entries (can be called from spawned task)
    async fn fetch_entries_async_static(
        s3_client: &S3Client,
        current: &VfsNode,
        rel_path: &str,
    ) -> Result<Vec<CompletionEntry>, ()> {
        // Navigate to target node based on current + rel_path
        let target = Self::resolve_target_node_static(current, rel_path);

        match target {
            VfsNode::Root => {
                // List buckets
                let buckets = s3_client.list_buckets().await.map_err(|_| ())?;
                Ok(buckets
                    .into_iter()
                    .map(|b| CompletionEntry {
                        name: b.name,
                        is_dir: true,
                    })
                    .collect())
            }
            VfsNode::Bucket { ref name } => {
                // List in bucket root
                let result = s3_client
                    .list_objects(name, "", Some("/"))
                    .await
                    .map_err(|_| ())?;
                let mut entries = Vec::new();

                // Prefixes are directories
                for prefix in result.prefixes {
                    let name = prefix
                        .trim_end_matches('/')
                        .rsplit('/')
                        .next()
                        .unwrap_or(&prefix);
                    entries.push(CompletionEntry {
                        name: name.to_string(),
                        is_dir: true,
                    });
                }
                // Objects are files
                for obj in result.objects {
                    let name = obj.key.rsplit('/').next().unwrap_or(&obj.key);
                    entries.push(CompletionEntry {
                        name: name.to_string(),
                        is_dir: false,
                    });
                }

                Ok(entries)
            }
            VfsNode::Prefix {
                ref bucket,
                ref prefix,
            } => {
                // List at this prefix
                let result = s3_client
                    .list_objects(bucket, prefix, Some("/"))
                    .await
                    .map_err(|_| ())?;
                let mut entries = Vec::new();

                // Prefixes are directories
                for pfx in result.prefixes {
                    let name = pfx.trim_end_matches('/').rsplit('/').next().unwrap_or(&pfx);
                    entries.push(CompletionEntry {
                        name: name.to_string(),
                        is_dir: true,
                    });
                }
                // Objects are files
                for obj in result.objects {
                    let name = obj.key.rsplit('/').next().unwrap_or(&obj.key);
                    entries.push(CompletionEntry {
                        name: name.to_string(),
                        is_dir: false,
                    });
                }

                Ok(entries)
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Resolve target node from current + relative path (static version)
    fn resolve_target_node_static(current: &VfsNode, rel_path: &str) -> VfsNode {
        if rel_path.is_empty() {
            return current.clone();
        }

        // Handle parent navigation
        if rel_path.starts_with("..") {
            let parent = Self::navigate_up_static(current);
            let remaining = rel_path
                .strip_prefix("../")
                .or_else(|| rel_path.strip_prefix(".."))
                .unwrap_or("");
            if remaining.is_empty() {
                return parent;
            }
            // Continue from parent with remaining path
            return Self::resolve_child_node_static(&parent, remaining.trim_end_matches('/'));
        }

        // Navigate to child
        Self::resolve_child_node_static(current, rel_path.trim_end_matches('/'))
    }

    /// Navigate up one level (static version)
    fn navigate_up_static(current: &VfsNode) -> VfsNode {
        match current {
            VfsNode::Root => VfsNode::Root,
            VfsNode::Bucket { .. } => VfsNode::Root,
            VfsNode::Prefix { bucket, prefix } => {
                if prefix.trim_end_matches('/').contains('/') {
                    let parent_prefix =
                        prefix.trim_end_matches('/').rsplit_once('/').unwrap().0;
                    VfsNode::Prefix {
                        bucket: bucket.clone(),
                        prefix: format!("{parent_prefix}/"),
                    }
                } else {
                    VfsNode::Bucket {
                        name: bucket.clone(),
                    }
                }
            }
            _ => current.clone(),
        }
    }

    /// Resolve child node (static version)
    fn resolve_child_node_static(current: &VfsNode, name: &str) -> VfsNode {
        match current {
            VfsNode::Root => VfsNode::Bucket {
                name: name.to_string(),
            },
            VfsNode::Bucket { name: bucket } => VfsNode::Prefix {
                bucket: bucket.clone(),
                prefix: format!("{name}/"),
            },
            VfsNode::Prefix { bucket, prefix } => VfsNode::Prefix {
                bucket: bucket.clone(),
                prefix: format!("{prefix}{name}/"),
            },
            _ => current.clone(),
        }
    }
}

impl Completer for ShellCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let line = &line[..pos];

        // If line is empty or only whitespace, don't complete
        if line.trim().is_empty() {
            return Ok((0, Vec::new()));
        }

        // Split into words
        let words: Vec<&str> = line.split_whitespace().collect();

        if words.is_empty() {
            return Ok((0, Vec::new()));
        }

        // If we're on the first word, complete commands
        if words.len() == 1 && !line.ends_with(char::is_whitespace) {
            let completions = self.complete_command(words[0]);
            let start = line.len() - words[0].len();
            return Ok((start, completions));
        }

        // Otherwise, complete paths
        // The path is everything after the command
        let path_start = line.find(char::is_whitespace).unwrap_or(0);
        let path = line[path_start..].trim_start();

        // Get the command name for filtering
        let command = words[0];

        if path.is_empty() {
            // Just completed command, show all entries for current directory
            let current = self.cache.get_current_node();
            let cache_key = self.node_to_cache_key(&current);

            let entries = if let Some(cached) = self.cache.get_entries(&cache_key) {
                cached
            } else {
                // Try to fetch entries for current directory
                match self.fetch_entries_for_path("") {
                    Ok(entries) => {
                        self.cache.update_entries(cache_key, entries.clone());
                        entries
                    }
                    Err(_) => Vec::new(),
                }
            };

            let completions = entries
                .into_iter()
                .filter(|entry| {
                    // Filter by command: cd only shows directories
                    if command == "cd" && !entry.is_dir {
                        return false;
                    }
                    true
                })
                .map(|entry| Pair {
                    display: entry.name.clone(),
                    replacement: entry.name,
                })
                .collect();
            return Ok((pos, completions));
        }

        let completions = self.complete_path(path, command);
        let start = pos - path.split_whitespace().last().unwrap_or("").len();
        Ok((start, completions))
    }
}

impl rustyline::Helper for ShellCompleter {}
impl rustyline::highlight::Highlighter for ShellCompleter {}
impl rustyline::hint::Hinter for ShellCompleter {
    type Hint = String;
}
impl rustyline::validate::Validator for ShellCompleter {}
