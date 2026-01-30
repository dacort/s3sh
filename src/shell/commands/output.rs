//! Output utilities for shell commands with BrokenPipe handling.
//!
//! These macros handle the common case where output is piped to a command like `head`
//! that closes the pipe early. Instead of erroring, we gracefully return Ok(()).

/// Print with newline, handling BrokenPipe gracefully.
///
/// Returns `Ok(())` early if BrokenPipe is encountered (e.g., when piped to `head`).
/// Propagates other IO errors.
#[macro_export]
macro_rules! print_line {
    ($($arg:tt)*) => {{
        use std::io::Write;
        match writeln!(std::io::stdout(), $($arg)*) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }
    }};
}

/// Print without newline, handling BrokenPipe gracefully.
///
/// Returns `Ok(())` early if BrokenPipe is encountered (e.g., when piped to `head`).
/// Propagates other IO errors.
#[macro_export]
macro_rules! print_str {
    ($($arg:tt)*) => {{
        use std::io::Write;
        match write!(std::io::stdout(), $($arg)*) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }
    }};
}

pub use print_line;
pub use print_str;
