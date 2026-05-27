use crate::app::AppContext;
use crate::cli::version::SwitchVersionCommand;
use crate::commands::version::{resolve_version_ref, VersionLookup};
use crate::db::{open_lix_at, resolve_db_path};
use crate::error::CliError;
use crate::hints::CommandOutput;
use lix_rs_sdk::SwitchBranchOptions;

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
    crate::db::block_on(lix.switch_branch(SwitchBranchOptions {
        branch_id: resolved.id.clone(),
    }))
    .map_err(|error| CliError::msg(error.to_string()))?;

    println!(
        "Switched active version to {} ({})",
        resolved.name, resolved.id
    );
    Ok(CommandOutput::empty())
}
