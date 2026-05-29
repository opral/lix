use crate::app::AppContext;
use crate::cli::redo::RedoCommand;
use crate::error::CliError;
use crate::hints::CommandOutput;

pub fn run(_context: &AppContext, _command: RedoCommand) -> Result<CommandOutput, CliError> {
    Err(CliError::msg(
        "redo is not available in the current lix-sdk surface",
    ))
}
