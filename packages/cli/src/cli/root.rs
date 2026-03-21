use super::exp::ExpCommand;
use super::init::InitCommand;
use super::redo::RedoCommand;
use super::sql::SqlCommand;
use super::undo::UndoCommand;
use super::version::VersionCommand;
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
    /// Reapply the most recently undone committed change unit.
    Redo(RedoCommand),
    /// Execute raw SQL against a Lix database.
    Sql(SqlCommand),
    /// Undo the most recent committed change unit.
    Undo(UndoCommand),
    /// Version operations such as merging branches.
    Version(VersionCommand),
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command};
    use crate::cli::sql::SqlSubcommand;
    use crate::cli::version::VersionSubcommand;
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

    #[test]
    fn parses_undo_command_version_flag() {
        let cli =
            Cli::try_parse_from(["lix", "undo", "--version", "branch-1"]).expect("parse succeeds");
        match cli.command {
            Command::Undo(command) => assert_eq!(command.version.as_deref(), Some("branch-1")),
            _ => panic!("expected undo command"),
        }
    }

    #[test]
    fn parses_redo_command_without_version() {
        let cli = Cli::try_parse_from(["lix", "redo"]).expect("parse succeeds");
        match cli.command {
            Command::Redo(command) => assert_eq!(command.version, None),
            _ => panic!("expected redo command"),
        }
    }

    #[test]
    fn parses_version_merge_command() {
        let cli = Cli::try_parse_from([
            "lix", "version", "merge", "--source", "draft-a", "--target", "main",
        ])
        .expect("parse succeeds");
        match cli.command {
            Command::Version(command) => match command.command {
                VersionSubcommand::Merge(args) => {
                    assert_eq!(args.source, "draft-a");
                    assert_eq!(args.target, "main");
                }
                _ => panic!("expected version merge command"),
            },
            _ => panic!("expected version command"),
        }
    }

    #[test]
    fn parses_version_create_command() {
        let cli = Cli::try_parse_from([
            "lix", "version", "create", "--id", "branch-a", "--name", "Branch A", "--hidden",
        ])
        .expect("parse succeeds");
        match cli.command {
            Command::Version(command) => match command.command {
                VersionSubcommand::Create(args) => {
                    assert_eq!(args.id.as_deref(), Some("branch-a"));
                    assert_eq!(args.name.as_deref(), Some("Branch A"));
                    assert!(args.hidden);
                }
                _ => panic!("expected version create command"),
            },
            _ => panic!("expected version command"),
        }
    }

    #[test]
    fn parses_version_switch_command() {
        let cli =
            Cli::try_parse_from(["lix", "version", "switch", "branch-a"]).expect("parse succeeds");
        match cli.command {
            Command::Version(command) => match command.command {
                VersionSubcommand::Switch(args) => {
                    assert_eq!(args.version_id, "branch-a");
                }
                _ => panic!("expected version switch command"),
            },
            _ => panic!("expected version command"),
        }
    }
}
