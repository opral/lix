use super::exp::ExpCommand;
use super::init::InitCommand;
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
    /// Experimental commands for benchmarking and diagnostics.
    Exp(ExpCommand),
    /// Initialize a Lix database file at the provided path.
    Init(InitCommand),
    /// Execute raw SQL against a Lix database.
    Sql(SqlCommand),
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command};
    use clap::Parser;
    use std::path::PathBuf;

    #[test]
    fn parses_init_command_path_argument() {
        let cli = Cli::try_parse_from(["lix", "init", "tmp/new.lix"]).expect("parse succeeds");
        match cli.command {
            Command::Init(init) => assert_eq!(init.path, PathBuf::from("tmp/new.lix")),
            _ => panic!("expected init command"),
        }
    }
}
