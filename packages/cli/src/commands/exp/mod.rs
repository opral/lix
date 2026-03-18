mod git_replay;

use crate::app::AppContext;
use crate::cli::exp::{ExpCommand, ExpSubcommand};
use crate::error::CliError;
use crate::hints::CommandOutput;

pub fn run(_context: &AppContext, command: ExpCommand) -> Result<CommandOutput, CliError> {
    match command.command {
        ExpSubcommand::GitReplay(args) => {
            git_replay::run(args)?;
            Ok(CommandOutput::empty())
        }
    }
}
