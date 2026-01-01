pub mod commands;

use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;

use crate::cache::ArchiveCache;
use crate::s3::S3Client;
use crate::vfs::{PathResolver, VfsNode, VirtualPath};
use commands::Command;

/// Shell state - tracks current location and provides command execution
pub struct ShellState {
    /// Current node in the virtual filesystem
    current_node: VfsNode,
    /// S3 client
    s3_client: Arc<S3Client>,
    /// Archive cache
    cache: ArchiveCache,
    /// Path resolver
    resolver: PathResolver,
    /// Registered commands
    commands: HashMap<String, Arc<dyn Command>>,
}

impl ShellState {
    /// Create a new shell state
    pub async fn new() -> Result<Self> {
        let s3_client = Arc::new(S3Client::new().await?);
        let cache = ArchiveCache::new(100);
        let resolver = PathResolver::new();

        let mut state = ShellState {
            current_node: VfsNode::Root,
            s3_client,
            cache,
            resolver,
            commands: HashMap::new(),
        };

        // Register commands
        state.register_command(Arc::new(commands::ls::LsCommand));
        state.register_command(Arc::new(commands::cd::CdCommand));
        state.register_command(Arc::new(commands::cat::CatCommand));

        Ok(state)
    }

    /// Register a command
    fn register_command(&mut self, command: Arc<dyn Command>) {
        self.commands.insert(command.name().to_string(), command);
    }

    /// Execute a command line
    pub async fn execute(&mut self, line: &str) -> Result<()> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(());
        }

        // Simple parsing - split on whitespace
        let parts: Vec<String> = line
            .split_whitespace()
            .map(String::from)
            .collect();

        if parts.is_empty() {
            return Ok(());
        }

        let cmd_name = &parts[0];
        let args = &parts[1..];

        // Check for built-in commands first
        match cmd_name.as_str() {
            "exit" | "quit" => {
                return Err(anyhow!("exit"));
            }
            "help" => {
                self.print_help();
                return Ok(());
            }
            "pwd" => {
                println!("{}", self.current_path());
                return Ok(());
            }
            _ => {}
        }

        // Look up command
        if let Some(command) = self.commands.get(cmd_name) {
            let cmd = Arc::clone(command);
            cmd.execute(self, args).await
        } else {
            Err(anyhow!("Unknown command: {}", cmd_name))
        }
    }

    /// Get the current node
    pub fn current_node(&self) -> &VfsNode {
        &self.current_node
    }

    /// Set the current node
    pub fn set_current_node(&mut self, node: VfsNode) {
        self.current_node = node;
    }

    /// Get the S3 client
    pub fn s3_client(&self) -> &Arc<S3Client> {
        &self.s3_client
    }

    /// Get the cache
    pub fn cache(&self) -> &ArchiveCache {
        &self.cache
    }

    /// Get the resolver
    pub fn resolver(&self) -> &PathResolver {
        &self.resolver
    }

    /// Get the current virtual path
    pub fn current_path(&self) -> VirtualPath {
        self.node_to_path(&self.current_node)
    }

    /// Convert a VFS node to a virtual path
    fn node_to_path(&self, node: &VfsNode) -> VirtualPath {
        match node {
            VfsNode::Root => VirtualPath::parse("/"),
            VfsNode::Bucket { name } => VirtualPath::parse(&format!("/{}", name)),
            VfsNode::Prefix { bucket, prefix } => {
                VirtualPath::parse(&format!("/{}/{}", bucket, prefix.trim_end_matches('/')))
            }
            VfsNode::Object { bucket, key, .. } => {
                VirtualPath::parse(&format!("/{}/{}", bucket, key))
            }
            VfsNode::Archive { parent, .. } => self.node_to_path(parent),
            VfsNode::ArchiveEntry { archive, path, .. } => {
                let archive_path = self.node_to_path(archive);
                archive_path.join(path)
            }
        }
    }

    /// Print help message
    fn print_help(&self) {
        println!("Available commands:");
        println!("  ls [OPTIONS]   - List contents");
        println!("  cd PATH        - Change directory");
        println!("  cat FILE       - Display file contents");
        println!("  pwd            - Print working directory");
        println!("  help           - Show this help");
        println!("  exit/quit      - Exit the shell");
    }

    /// Get the prompt string
    pub fn prompt(&self) -> String {
        format!("3xplore:{} $ ", self.current_path())
    }
}
