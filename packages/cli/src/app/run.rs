use super::context::AppContext;
use crate::cli::root::{Cli, Command};
use crate::commands;
use crate::error::CliError;
use clap::Parser;

pub fn run() -> Result<(), CliError> {
    let cli = Cli::parse();
    let context = AppContext {
        lix_path: cli.path,
    };

    match cli.command {
        Command::Sql(sql_command) => commands::sql::run(&context, sql_command),
    }
}
