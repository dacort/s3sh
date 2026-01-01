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

    /// Get the path segments
    pub fn segments(&self) -> &[String] {
        &self.segments
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
}

impl std::fmt::Display for VirtualPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.segments.is_empty() {
            write!(f, "/")
        } else {
            write!(f, "/{}", self.segments.join("/"))
        }
    }
}
