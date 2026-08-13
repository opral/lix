use crate::app::AppContext;
use crate::cli::undo::UndoCommand;
use crate::db::{open_lix_at, resolve_db_path};
use crate::error::CliError;
use crate::hints::CommandOutput;

pub fn run(context: &AppContext, command: UndoCommand) -> Result<CommandOutput, CliError> {
    let path = resolve_db_path(context)?;
    let lix = open_lix_at(&path)?;
    let receipt = if let Some(branch_id) = command.branch {
        crate::db::block_on(lix.switch_branch(lix::SwitchBranchOptions { branch_id }))
            .map_err(|error| CliError::msg(error.to_string()))?;
        crate::db::block_on(lix.undo())
    } else {
        crate::db::block_on(lix.undo())
    }
    .map_err(|error| CliError::msg(error.to_string()))?;
    println!(
        "Undid commit {} on branch {} as {}.",
        receipt.target_commit_id, receipt.branch_id, receipt.inverse_commit_id
    );
    Ok(CommandOutput::empty())
}
