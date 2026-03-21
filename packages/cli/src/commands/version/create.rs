use crate::app::AppContext;
use crate::cli::version::CreateVersionCommand;
use crate::db::{open_lix_at, resolve_db_path};
use crate::error::CliError;
use crate::hints::CommandOutput;
use lix_rs_sdk::CreateVersionOptions;

pub fn run(context: &AppContext, command: CreateVersionCommand) -> Result<CommandOutput, CliError> {
    let path = resolve_db_path(context)?;
    let lix = open_lix_at(&path)?;
    let result = pollster::block_on(lix.create_version(CreateVersionOptions {
        id: command.id,
        name: command.name,
        hidden: command.hidden,
    }))
    .map_err(|error| CliError::msg(error.to_string()))?;

    println!("Created version {} ({})", result.id, result.name);
    Ok(CommandOutput::empty())
}
