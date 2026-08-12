use crate::app::AppContext;
use crate::cli::redo::RedoCommand;
use crate::db::{open_lix_at, resolve_db_path};
use crate::error::CliError;
use crate::hints::CommandOutput;

pub fn run(context: &AppContext, command: RedoCommand) -> Result<CommandOutput, CliError> {
    let path = resolve_db_path(context)?;
    let lix = open_lix_at(&path)?;
    let receipt = if let Some(branch_id) = command.branch {
        let branch = crate::db::block_on(lix.open_session_at(branch_id))
            .map_err(|error| CliError::msg(error.to_string()))?;
        crate::db::block_on(branch.redo())
    } else {
        crate::db::block_on(lix.redo())
    }
    .map_err(|error| CliError::msg(error.to_string()))?;
    println!(
        "Redid commit {} on branch {} as {}.",
        receipt.target_commit_id, receipt.branch_id, receipt.replay_commit_id
    );
    Ok(CommandOutput::empty())
}
