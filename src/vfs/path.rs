/// Represents a path in the virtual filesystem
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualPath {
    /// Path segments (e.g., ["bucket", "prefix", "file.txt"])
    segments: Vec<String>,
    /// Whether this is an absolute path (starts with /)
    is_absolute: bool,
}

impl VirtualPath {
    /// Parse a path string into a VirtualPath
    pub fn parse(path: &str) -> Self {
        let is_absolute = path.starts_with('/');
        let segments: Vec<String> = path
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty() && *s != ".")
            .map(String::from)
            .collect();

        VirtualPath {
            segments,
            is_absolute,
        }
    }

    /// Create a new absolute path from segments
    pub fn from_segments(segments: Vec<String>) -> Self {
        VirtualPath {
            segments,
            is_absolute: true,
        }
    }

    /// Get the path segments
    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    /// Check if this is an absolute path
    pub fn is_absolute(&self) -> bool {
        self.is_absolute
    }

    /// Check if this path is empty (root)
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Get the parent path
    pub fn parent(&self) -> Option<Self> {
        if self.segments.is_empty() {
            None
        } else {
            let mut parent_segments = self.segments.clone();
            parent_segments.pop();
            Some(VirtualPath {
                segments: parent_segments,
                is_absolute: self.is_absolute,
            })
        }
    }

    /// Get the last segment (filename)
    pub fn filename(&self) -> Option<&str> {
        self.segments.last().map(|s| s.as_str())
    }

    /// Join this path with another
    pub fn join(&self, other: &str) -> Self {
        let mut new_segments = self.segments.clone();

        for segment in other.split('/') {
            if segment.is_empty() || segment == "." {
                continue;
            } else if segment == ".." {
                new_segments.pop();
            } else {
                new_segments.push(segment.to_string());
            }
        }

        VirtualPath {
            segments: new_segments,
            is_absolute: self.is_absolute,
        }
    }

    /// Convert to a string representation
    pub fn to_string(&self) -> String {
        if self.segments.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", self.segments.join("/"))
        }
    }
}

impl std::fmt::Display for VirtualPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_absolute() {
        let path = VirtualPath::parse("/bucket/prefix/file.txt");
        assert!(path.is_absolute());
        assert_eq!(path.segments(), &["bucket", "prefix", "file.txt"]);
    }

    #[test]
    fn test_parse_relative() {
        let path = VirtualPath::parse("prefix/file.txt");
        assert!(!path.is_absolute());
        assert_eq!(path.segments(), &["prefix", "file.txt"]);
    }

    #[test]
    fn test_parent() {
        let path = VirtualPath::parse("/bucket/prefix/file.txt");
        let parent = path.parent().unwrap();
        assert_eq!(parent.segments(), &["bucket", "prefix"]);
    }

    #[test]
    fn test_join() {
        let path = VirtualPath::parse("/bucket/prefix");
        let joined = path.join("subdir/file.txt");
        assert_eq!(joined.segments(), &["bucket", "prefix", "subdir", "file.txt"]);
    }

    #[test]
    fn test_join_with_dotdot() {
        let path = VirtualPath::parse("/bucket/prefix/subdir");
        let joined = path.join("../file.txt");
        assert_eq!(joined.segments(), &["bucket", "prefix", "file.txt"]);
    }
}
