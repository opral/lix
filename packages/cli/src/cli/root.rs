use super::sql::SqlCommand;
use clap::{Parser, Subcommand, ValueHint};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "lix")]
#[command(about = "Lix command line interface")]
pub struct Cli {
    /// Path to the .lix file (required when multiple .lix files exist).
    #[arg(long, global = true, value_hint = ValueHint::FilePath)]
    pub path: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Execute raw SQL against a Lix database.
    Sql(SqlCommand),
}
