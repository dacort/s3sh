use anyhow::Result;
use async_trait::async_trait;

pub mod ls;
pub mod cd;
pub mod cat;

use super::ShellState;

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
