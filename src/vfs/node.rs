use std::sync::Arc;

/// Represents different types of archives we can navigate into
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveType {
    Tar,
    TarGz,
    TarBz2,
    Zip,
    Gz,
    Bz2,
}

impl ArchiveType {
    /// Detect archive type from file extension
    pub fn from_path(path: &str) -> Option<Self> {
        let path_lower = path.to_lowercase();
        if path_lower.ends_with(".tar.gz") || path_lower.ends_with(".tgz") {
            Some(ArchiveType::TarGz)
        } else if path_lower.ends_with(".tar.bz2") || path_lower.ends_with(".tbz2") {
            Some(ArchiveType::TarBz2)
        } else if path_lower.ends_with(".tar") {
            Some(ArchiveType::Tar)
        } else if path_lower.ends_with(".zip") {
            Some(ArchiveType::Zip)
        } else if path_lower.ends_with(".gz") {
            Some(ArchiveType::Gz)
        } else if path_lower.ends_with(".bz2") {
            Some(ArchiveType::Bz2)
        } else {
            None
        }
    }
}

/// Archive index entry - cached metadata about files in an archive
#[derive(Debug, Clone)]
pub struct ArchiveEntry {
    pub path: String,
    pub offset: u64,
    pub size: u64,
    pub is_dir: bool,
}

/// Archive index - maps file paths to their metadata
#[derive(Debug, Clone)]
pub struct ArchiveIndex {
    pub entries: std::collections::HashMap<String, ArchiveEntry>,
}

impl ArchiveIndex {
    /// Look up an entry, trying both with and without trailing slash
    /// Tar archives often store directories with trailing slashes
    pub fn find_entry(&self, path: &str) -> Option<&ArchiveEntry> {
        self.entries.get(path)
            .or_else(|| self.entries.get(&format!("{}/", path)))
    }
}

/// Represents a node in the virtual filesystem
/// This is the core abstraction that unifies S3 objects and archive contents
#[derive(Debug, Clone)]
pub enum VfsNode {
    /// Root of the filesystem - lists S3 buckets
    Root,

    /// An S3 bucket
    Bucket {
        name: String,
    },

    /// A prefix within an S3 bucket (acts like a directory)
    Prefix {
        bucket: String,
        prefix: String,
    },

    /// An S3 object (file)
    Object {
        bucket: String,
        key: String,
        size: u64,
    },

    /// An archive file that can be navigated into
    Archive {
        /// The parent node (Object or ArchiveEntry)
        parent: Box<VfsNode>,
        /// Type of archive
        archive_type: ArchiveType,
        /// Cached index (lazy loaded)
        index: Option<Arc<ArchiveIndex>>,
    },

    /// A file or directory within an archive
    ArchiveEntry {
        /// The archive containing this entry
        archive: Box<VfsNode>,
        /// Path within the archive
        path: String,
        /// Size of the entry
        size: u64,
        /// Whether this is a directory
        is_dir: bool,
    },
}

impl VfsNode {
    /// Check if this node can be listed (like a directory)
    pub fn is_listable(&self) -> bool {
        matches!(
            self,
            VfsNode::Root | VfsNode::Bucket { .. } | VfsNode::Prefix { .. } |
            VfsNode::Archive { .. } | VfsNode::ArchiveEntry { is_dir: true, .. }
        )
    }

    /// Check if this node can be navigated into with cd
    pub fn is_navigable(&self) -> bool {
        self.is_listable()
    }
}
