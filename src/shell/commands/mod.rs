use anyhow::Result;
use async_trait::async_trait;

pub mod cat;
pub mod cd;
pub mod ls;

use super::ShellState;

/// Helper macro to print with BrokenPipe handling (with newline)
/// Returns Ok(()) early if BrokenPipe is encountered
#[macro_export]
macro_rules! print_line {
    ($($arg:tt)*) => {{
        use std::io::Write;
        let result = writeln!(std::io::stdout(), $($arg)*);
        match result {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }
    }};
}

/// Helper macro to print with BrokenPipe handling (no newline)
/// Returns Ok(()) early if BrokenPipe is encountered
#[macro_export]
macro_rules! print_str {
    ($($arg:tt)*) => {{
        use std::io::Write;
        let result = write!(std::io::stdout(), $($arg)*);
        match result {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }
    }};
}

/// Trait for shell commands
#[async_trait]
pub trait Command: Send + Sync {
    /// Get the command name
    fn name(&self) -> &str;

    /// Get command usage help
    fn usage(&self) -> &str;

    /// Execute the command
    async fn execute(&self, state: &mut ShellState, args: &[String]) -> Result<()>;
}
