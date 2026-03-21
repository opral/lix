mod create;
mod merge;
mod switch;

use crate::app::AppContext;
use crate::cli::version::{VersionCommand, VersionSubcommand};
use crate::error::CliError;
use crate::hints::CommandOutput;

pub fn run(context: &AppContext, command: VersionCommand) -> Result<CommandOutput, CliError> {
    match command.command {
        VersionSubcommand::Create(command) => create::run(context, command),
        VersionSubcommand::Merge(command) => merge::run(context, command),
        VersionSubcommand::Switch(command) => switch::run(context, command),
    }
}
