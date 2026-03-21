use crate::app::AppContext;
use crate::cli::version::SwitchVersionCommand;
use crate::db::{open_lix_at, resolve_db_path};
use crate::error::CliError;
use crate::hints::CommandOutput;

pub fn run(context: &AppContext, command: SwitchVersionCommand) -> Result<CommandOutput, CliError> {
    let path = resolve_db_path(context)?;
    let lix = open_lix_at(&path)?;
    pollster::block_on(lix.switch_version(command.version_id.clone()))
        .map_err(|error| CliError::msg(error.to_string()))?;

    println!("Switched active version to {}", command.version_id);
    Ok(CommandOutput::empty())
}
