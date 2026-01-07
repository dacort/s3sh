# s3sh Development Plan

## Project Overview
s3sh is an interactive S3 shell that allows users to navigate S3 buckets using Unix-like commands (`ls`, `cd`, `cat`, `pwd`) and seamlessly explore archive contents (tar, zip) without downloading entire files.

## Core Architecture

### Virtual Filesystem (VFS) Abstraction
```rust
enum VfsNode {
    Root,                          // Lists S3 buckets
    Bucket { name },               // S3 bucket
    Prefix { bucket, prefix },     // S3 "directory" (prefix)
    Object { bucket, key, size },  // S3 object
    Archive { parent, type, index }, // Archive (tar, zip, etc.)
    ArchiveEntry { archive, path } // File/dir inside archive
}
```

### Archive Streaming Strategy
- **Zip files**: Read central directory at end using S3 range requests
- **Tar files**: Stream entire archive once to build header index
- **Caching**: LRU cache for archive indexes to avoid repeated S3 calls

---

## ✅ Phase 1: Foundation & S3 Navigation (COMPLETED)

**Goal:** Working shell that can navigate S3 buckets and read files

### Completed Features:
- ✅ Basic shell REPL with rustyline
- ✅ VFS types (VfsNode, VirtualPath)
- ✅ S3 client wrapper with AWS SDK
- ✅ Commands: `ls`, `cd`, `cat`, `pwd`, `help`, `exit`
- ✅ Path resolver for S3 navigation
- ✅ Error handling and colored output
- ✅ Cross-region bucket support (automatic regional client caching)

**Key Files:**
- `src/main.rs` - Entry point, REPL loop
- `src/shell/mod.rs` - Shell state, command dispatcher
- `src/shell/commands/` - Individual commands (ls.rs, cd.rs, cat.rs)
- `src/vfs/node.rs` - VfsNode enum
- `src/s3/client.rs` - AWS SDK wrapper

---

## ✅ Phase 2: Archive Support - Zip (COMPLETED)

**Goal:** Navigate and read zip files in S3

### Completed Features:
- ✅ S3 range request streaming (s3/stream.rs)
- ✅ ArchiveHandler trait
- ✅ ZipHandler implementation
  - EOCD (End of Central Directory) parsing via range request
  - Central directory parsing to build index
  - File extraction using range requests
- ✅ Archive detection by extension
- ✅ Updated `cd` to handle zip archives
- ✅ Updated `ls` to list zip contents
- ✅ Updated `cat` to read files from zip
- ✅ In-memory LRU cache for zip indexes

**Key Files:**
- `src/s3/stream.rs` - S3 range request streaming
- `src/archive/mod.rs` - ArchiveHandler trait
- `src/archive/zip.rs` - Zip support
- `src/cache/mod.rs` - LRU cache

---

## ✅ Phase 3: Archive Support - Tar (COMPLETED)

**Goal:** Navigate and read tar/tar.gz/tar.bz2 files

### Completed Features:
- ✅ TarHandler implementation
  - Uncompressed tar support
  - Gzip decompression layer for tar.gz
  - Bzip2 decompression layer for tar.bz2
- ✅ Tar header index building via streaming
- ✅ Archive type detection (tar, tar.gz, tgz, tar.bz2, tbz2)
- ✅ Updated resolver to detect tar formats
- ✅ Tar support in all commands
- ✅ Fixed trailing slash handling for tar directories

**Key Files:**
- `src/archive/tar.rs` - Tar support

---

## ✅ Phase 4: UX Improvements (COMPLETED)

**Goal:** Enhance user experience with better path handling and feedback

### Completed Features:
- ✅ Quote handling for paths with spaces
  - Single quotes, double quotes, backslash escaping
- ✅ Multiple `../` support for parent navigation
- ✅ `ls` with path arguments (`ls dir/subdir`)
- ✅ Wildcard support (`ls *.zip`, `ls *.tar.gz`)
  - Pattern matching with `*` (match any) and `?` (match one)
- ✅ Progress spinners for archive operations
  - Building archive indexes
  - Extracting files from archives
  - Visual feedback via indicatif crate

**Key Files:**
- `src/ui.rs` - UI utilities (spinner creation)
- `src/shell/mod.rs` - Quote parsing in parse_command_line

---

## ✅ Phase 5: Code Quality & Refactoring (COMPLETED)

**Goal:** Clean up code following YAGNI+SOLID+DRY+KISS principles

### Completed Refactoring:
- ✅ **Extract trailing slash helper** (~30 lines removed)
  - Created `find_entry()` method in ArchiveIndex
  - Handles tar directories with/without trailing slashes
- ✅ **Extract spinner creation helper** (~50 lines removed)
  - Created `ui.rs` module with `create_spinner()` function
  - Centralized spinner styling and configuration
- ✅ **Remove dead code** (~300 lines removed)
  - Removed unused methods from VfsNode (display_name, is_readable)
  - Removed unused methods from VirtualPath (from_segments, is_absolute, is_empty, parent, filename)
  - Deleted entire `vfs/resolver.rs` module (unused stub)
  - Removed unused methods from cd.rs (resolve_absolute_path, resolve_relative_path)
  - Removed unused S3Client methods (object_exists)
  - Removed unused struct fields (content_type, last_modified from ObjectMetadata)
  - Cleaned up unused imports
- ✅ **Cache utility methods restored**
  - Kept clear() and len() methods in ArchiveCache (useful for debugging)

---

## ✅ Phase 6: Smart Tab Completion (COMPLETED)

**Goal:** Intelligent tab completion with lazy loading and command awareness

### Completed Features:
- ✅ **Command Completion** - Tab completes `ls`, `cd`, `cat`, `pwd`, `help`, `exit`
- ✅ **Path Completion** - Completes file and directory names in current location
- ✅ **Multi-Segment Paths** - `cd movies/<TAB>` fetches subdirectory contents
- ✅ **Parent Directory Navigation** - `cd ../<TAB>` shows parent directory contents
- ✅ **Lazy Loading** - Fetches S3 listings on-demand during tab completion
  - First tab triggers S3 call (one-time cost per directory)
  - Results cached for instant subsequent completions
  - Uses channels to bridge sync completion with async S3 calls
- ✅ **Command-Aware Filtering**
  - `cd` command only shows directories (prefixes)
  - `cat` command shows both files and directories
  - CompletionEntry struct tracks name + is_dir metadata
- ✅ **Hierarchical Caching** - Stores entries by path for efficient lookups
- ✅ **Archive Support** - Works inside tar/zip files

**Key Files:**
- `src/shell/completion.rs` - Tab completion implementation
  - CompletionCache - Hierarchical cache (path → entries with metadata)
  - ShellCompleter - Implements rustyline's Completer trait
  - Lazy loading with tokio::spawn + channels
  - Command-aware filtering logic

**Technical Implementation:**
- Uses `std::sync::mpsc` channels to bridge synchronous completion with async S3 calls
- Spawns async tasks with `tokio::runtime::Handle::spawn()` to avoid runtime nesting
- Caches CompletionEntry (name, is_dir) for accurate filtering
- Resolves relative paths (., .., multi-segment) for completion

---

## 🔲 Phase 7: Remaining Features

### Recursive Listing (`ls -R`)
**Status:** Not Started
**Priority:** Medium

Implement recursive directory listing with tree-like display:
- Recursively traverse VFS nodes
- Include archive contents in output
- Format tree-like display (indent, branches)
- Handle depth limiting to avoid infinite recursion
- Show file sizes and types in recursive view

**Estimated Complexity:** Medium
- Need to implement recursive traversal logic
- Tree formatting for readable output
- Handle both S3 prefixes and archive entries
- Consider performance for large directories

---

## 🔲 Phase 8: Nested Archives (Future)

**Status:** Not Planned for v1.0
**Priority:** Low

Support for archives within archives:
- Detect when archive entry is itself an archive
- Extract inner archive to memory
- Support `cd` into nested archives
- Add depth limiting (max 3 levels)
- LRU cache for extracted nested archives

**Note:** Deferred to avoid over-engineering. Most use cases don't require nested archive navigation.

---

## 🔲 Phase 9: Standalone Compression (Future)

**Status:** Not Planned for v1.0
**Priority:** Low

Support for standalone compressed files:
- `.gz` files (gzip)
- `.bz2` files (bzip2)
- Decompress on-the-fly when `cat`'d
- No navigation (they're files, not archives)

---

## Technical Debt & Known Limitations

### Current Limitations:
1. **No nested archive support** - Can't `cd` into archives within archives
2. **No recursive listing** - `ls -R` not yet implemented
3. **No progress for large file cats** - Streaming has no progress indicator
4. **Archive indexes not persisted** - Rebuilt on each shell session

### Performance Considerations:
- Archive indexes are memory-only (LRU cache with 100 entry limit)
- First access to archive requires full stream (can be slow for large archives)
- Tab completion triggers S3 calls on first use per directory
- No parallel listing for large directories

### Future Optimizations:
- Persistent cache for archive indexes
- Progress indicators for large file operations
- Parallel S3 listing for faster tab completion
- Configurable cache size and TTL

---

## Project Statistics

### Lines of Code (Approximate):
- Core VFS: ~200 lines
- S3 Client: ~300 lines
- Commands: ~800 lines
- Archive Handlers: ~400 lines
- Tab Completion: ~500 lines
- Cache & Utilities: ~200 lines
- **Total: ~2,400 lines of Rust**

### Key Dependencies:
- **AWS SDK**: aws-sdk-s3, aws-config, tokio
- **Archive**: tar, zip, flate2, bzip2
- **CLI**: rustyline, colored, indicatif
- **Utilities**: anyhow, lru, humansize, dirs

---

## Success Criteria ✅

All core features implemented:
- ✅ Can list S3 buckets at root
- ✅ Can navigate into buckets and prefixes
- ✅ Can `cat` S3 objects
- ✅ Can `cd` into all supported archive types
- ✅ Can `cat` files from inside archives
- ✅ Tab completion with smart filtering
- ✅ Streaming works without downloading entire files
- ✅ Pleasant CLI with colors and history
- ✅ Progress indicators for slow operations
- ✅ Cross-region bucket support

---

## Release Checklist

### Pre-Release:
- ✅ README.md with examples
- ✅ LICENSE file (MIT)
- ✅ Cargo.toml metadata
- ✅ GitHub Actions workflow (release-plz)
- ✅ Rename to `s3sh` (avoiding unfortunate naming)
- ⬜ CHANGELOG.md (auto-generated by release-plz)

### v1.0.0 Release:
- ⬜ Tag and publish to crates.io
- ⬜ Create GitHub release with binaries
- ⬜ Update README with installation instructions
- ⬜ Consider adding to awesome-rust list

### Post v1.0.0:
- ⬜ Implement `ls -R` (recursive listing)
- ⬜ Add configuration file support
- ⬜ Consider persistent archive index cache
- ⬜ Add `find` and `grep` commands for searching
- ⬜ Support for additional archive formats (7z, rar)

---

## License

MIT License - See LICENSE file for details

**Built with:** Rust, AWS SDK, Tokio, and lots of ☕
