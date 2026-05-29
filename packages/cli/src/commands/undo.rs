use crate::app::AppContext;
use crate::cli::undo::UndoCommand;
use crate::error::CliError;
use crate::hints::CommandOutput;

pub fn run(_context: &AppContext, _command: UndoCommand) -> Result<CommandOutput, CliError> {
    Err(CliError::msg(
        "undo is not available in the current lix-sdk surface",
    ))
}
