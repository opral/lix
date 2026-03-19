use super::context::AppContext;
use crate::cli::root::{Cli, Command};
use crate::commands;
use crate::error::CliError;
use crate::hints;
use clap::Parser;

pub fn run() -> Result<(), CliError> {
    let cli = Cli::parse();
    let no_hints = cli.no_hints;
    let context = AppContext {
        lix_path: cli.path,
        no_hints,
    };

    let output = match cli.command {
        Command::Exp(exp_command) => commands::exp::run(&context, exp_command),
        Command::Init(init_command) => commands::init::run(init_command),
        Command::Redo(redo_command) => commands::redo::run(&context, redo_command),
        Command::Sql(sql_command) => commands::sql::run(&context, sql_command),
        Command::Undo(undo_command) => commands::undo::run(&context, undo_command),
    }?;

    if !no_hints {
        hints::render_hints(&output.hints);
    }

    Ok(())
}
