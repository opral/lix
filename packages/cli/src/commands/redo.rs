use crate::app::AppContext;
use crate::cli::redo::RedoCommand;
use crate::db::{open_lix_at, resolve_db_path};
use crate::error::CliError;
use crate::hints::CommandOutput;
use lix_rs_sdk::RedoOptions;

pub fn run(context: &AppContext, command: RedoCommand) -> Result<CommandOutput, CliError> {
    let path = resolve_db_path(context)?;
    let lix = open_lix_at(&path)?;
    let result = pollster::block_on(lix.redo_with_options(RedoOptions {
        version_id: command.version,
    }))
    .map_err(|error| CliError::msg(error.to_string()))?;

    println!(
        "Redid commit {} in version {} with replay commit {}",
        result.target_commit_id, result.version_id, result.replay_commit_id
    );

    Ok(CommandOutput::empty())
}
