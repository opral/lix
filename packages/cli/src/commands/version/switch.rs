use crate::app::AppContext;
use crate::cli::version::SwitchVersionCommand;
use crate::commands::version::{resolve_version_ref, VersionLookup};
use crate::db::{open_lix_at, resolve_db_path};
use crate::error::CliError;
use crate::hints::CommandOutput;

pub fn run(context: &AppContext, command: SwitchVersionCommand) -> Result<CommandOutput, CliError> {
    let path = resolve_db_path(context)?;
    let lix = open_lix_at(&path)?;
    let resolved = resolve_version_ref(
        &lix,
        match (command.id.as_deref(), command.name.as_deref()) {
            (Some(id), None) => VersionLookup::Id(id),
            (None, Some(name)) => VersionLookup::Name(name),
            _ => {
                return Err(CliError::msg(
                    "version switch requires exactly one of --id or --name",
                ));
            }
        },
    )?;
    pollster::block_on(lix.switch_version(resolved.id.clone()))
        .map_err(|error| CliError::msg(error.to_string()))?;

    println!(
        "Switched active version to {} ({})",
        resolved.name, resolved.id
    );
    Ok(CommandOutput::empty())
}
