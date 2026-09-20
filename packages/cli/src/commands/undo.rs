use crate::app::AppContext;
use crate::cli::undo::UndoCommand;
use crate::db::{open_lix_at, resolve_db_path};
use crate::error::CliError;
use crate::hints::CommandOutput;

pub fn run(context: &AppContext, command: UndoCommand) -> Result<CommandOutput, CliError> {
    let path = resolve_db_path(context)?;
    let lix = open_lix_at(&path)?;
    if let Some(branch_id) = command.branch {
        crate::db::block_on(lix.switch_branch(lix::SwitchBranchOptions { branch_id }))
            .map_err(|error| CliError::msg(error.to_string()))?;
    }
    let result = crate::db::block_on(lix.execute("SELECT commit_id FROM lix_undo()", &[]))
        .map_err(|error| CliError::msg(error.to_string()))?;
    let commit_id = result
        .rows()
        .first()
        .and_then(|row| row.get::<String>("commit_id").ok());
    println!("{}", commit_id.unwrap_or_else(|| "null".to_string()));
    Ok(CommandOutput::empty())
}
