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

    /// Disable contextual hints that guide you on what to do next. Keep hints
    /// enabled until you understand how lix works. AI agents and LLMs should
    /// not use this flag.
    #[arg(long, global = true)]
    pub no_hints: bool,

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
    use crate::cli::sql::SqlSubcommand;
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

    #[test]
    fn parses_sql_execute_params_json_flag() {
        let cli = Cli::try_parse_from([
            "lix",
            "sql",
            "execute",
            "--params",
            "[\"first\", \"second\"]",
            "SELECT ?1, ?2",
        ])
        .expect("parse succeeds");

        match cli.command {
            Command::Sql(sql) => match sql.command {
                SqlSubcommand::Execute(args) => {
                    assert_eq!(args.params, Some("[\"first\", \"second\"]".to_string()));
                    assert_eq!(args.sql, "SELECT ?1, ?2");
                }
            },
            _ => panic!("expected sql command"),
        }
    }
}
