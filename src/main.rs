use colored::*;
use rustyline::error::ReadlineError;
use s3sh::shell;
use rustyline::Editor;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Print welcome message
    println!("{}", "=".repeat(60).cyan());
    println!("{}", "  s3sh - The S3 Shell".bold().cyan());
    println!("{}", "  Navigate S3 buckets like a Unix shell".cyan());
    println!("{}", "=".repeat(60).cyan());
    println!();
    println!("Type 'help' for available commands or 'exit' to quit");
    println!();

    // Initialize shell state
    let mut state = match shell::ShellState::new().await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{} Failed to initialize S3 client: {}", "Error:".red().bold(), e);
            eprintln!("Make sure you have valid AWS credentials configured.");
            std::process::exit(1);
        }
    };

    // Don't pre-populate cache - let lazy loader fetch with accurate metadata
    // This ensures command-aware filtering (cd shows only dirs, cat shows all)

    // Create readline editor with tab completion
    let completer = shell::ShellCompleter::new(state.completion_cache().clone());
    let mut rl = Editor::new()?;
    rl.set_helper(Some(completer));

    // Load history if available
    let history_file = dirs::home_dir().map(|mut p| {
        p.push(".s3sh_history");
        p
    });

    if let Some(path) = &history_file {
        let _ = rl.load_history(path);
    }

    // REPL loop
    loop {
        let prompt = state.prompt();

        match rl.readline(&prompt) {
            Ok(line) => {
                // Add to history
                let _ = rl.add_history_entry(line.as_str());

                // Execute command
                match state.execute(&line).await {
                    Ok(_) => {}
                    Err(e) => {
                        if e.to_string() == "exit" {
                            break;
                        }
                        eprintln!("{} {}", "Error:".red().bold(), e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D
                println!("exit");
                break;
            }
            Err(err) => {
                eprintln!("{} {:?}", "Error:".red().bold(), err);
                break;
            }
        }
    }

    // Save history
    if let Some(path) = &history_file {
        let _ = rl.save_history(path);
    }

    println!("Goodbye!");
    Ok(())
}
