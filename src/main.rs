use anyhow::Result;
use clap::Parser;
use colored::*;
use rustyline::Editor;
use rustyline::error::ReadlineError;
use std::sync::Arc;

use s3sh::{providers, s3, shell};

#[derive(Parser, Debug)]
#[command(name = "s3sh")]
#[command(about = "The S3 Shell - Navigate S3 buckets like a Unix shell", long_about = None)]
struct Args {
    /// S3 provider to use (aws, sourcecoop)
    #[arg(short, long, default_value = "aws")]
    provider: String,

    /// List available providers and exit
    #[arg(long)]
    list_providers: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Handle --list-providers
    if args.list_providers {
        print_available_providers();
        return Ok(());
    }

    // Initialize provider registry
    let registry = providers::ProviderRegistry::new();

    // Get the requested provider
    let provider = registry
        .get(&args.provider)
        .ok_or_else(|| anyhow::anyhow!("Unknown provider: {}", args.provider))?;

    // Create provider configuration first to check for custom endpoints
    let provider_config = provider.build_config().await?;

    // Print welcome message with provider info
    println!("{}", "=".repeat(60).cyan());
    println!("{}", "  s3sh - The S3 Shell".bold().cyan());
    println!("{}", "  Navigate S3 buckets like a Unix shell".cyan());
    println!("{}", "=".repeat(60).cyan());
    println!(
        "Provider: {} ({})",
        provider.name().bold(),
        provider.description()
    );
    if let Some(endpoint) = &provider_config.endpoint_url {
        println!("Endpoint: {}", endpoint.bold());
    }
    println!();
    println!("Type 'help' for available commands or 'exit' to quit");
    println!();

    // Create S3 client from provider
    let (client, region, disable_cross_region) =
        match providers::create_s3_client(provider_config).await {
            Ok(result) => result,
            Err(e) => {
                eprintln!(
                    "{} Failed to initialize S3 client: {}",
                    "Error:".red().bold(),
                    e
                );
                if args.provider == "aws" {
                    eprintln!("Make sure you have valid AWS credentials configured.");
                }
                std::process::exit(1);
            }
        };

    // Wrap client in S3Client wrapper
    let s3_client = Arc::new(s3::S3Client::from_client_with_options(
        client,
        region,
        disable_cross_region,
    ));

    // Initialize shell state with the client
    let mut state = shell::ShellState::with_client(s3_client).await?;

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
                let _ = rl.add_history_entry(line.as_str());

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
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
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

fn print_available_providers() {
    let registry = providers::ProviderRegistry::new();
    println!("Available S3 providers:");
    println!();

    for name in registry.list() {
        if let Some(provider) = registry.get(name) {
            println!("  {:12} - {}", name.cyan(), provider.description());
        }
    }

    println!();
    println!("Usage: s3sh --provider <name>");
}
