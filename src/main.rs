mod archive;
mod cache;
mod s3;
mod shell;
mod vfs;

use colored::*;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Print welcome message
    println!("{}", "=".repeat(60).cyan());
    println!("{}", "  3xplore - S3 Explorer Shell".bold().cyan());
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

    // Create readline editor
    let mut rl = DefaultEditor::new()?;

    // Load history if available
    let history_file = dirs::home_dir().map(|mut p| {
        p.push(".3xplore_history");
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
