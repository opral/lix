use crate::app::AppContext;
use crate::cli::undo::UndoCommand;
use crate::db::{open_lix_at, resolve_db_path};
use crate::error::CliError;
use crate::hints::CommandOutput;
use lix_rs_sdk::UndoOptions;

pub fn run(context: &AppContext, command: UndoCommand) -> Result<CommandOutput, CliError> {
    let path = resolve_db_path(context)?;
    let lix = open_lix_at(&path)?;
    let result = pollster::block_on(lix.undo_with_options(UndoOptions {
        version_id: command.version,
    }))
    .map_err(|error| CliError::msg(error.to_string()))?;

    println!(
        "Undid commit {} in version {} with inverse commit {}",
        result.target_commit_id, result.version_id, result.inverse_commit_id
    );

    Ok(CommandOutput::empty())
}
