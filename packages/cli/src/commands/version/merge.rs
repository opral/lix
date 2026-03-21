use crate::app::AppContext;
use crate::cli::version::MergeVersionCommand;
use crate::commands::version::{resolve_version_ref, VersionLookup};
use crate::db::{open_lix_at, resolve_db_path};
use crate::error::CliError;
use crate::hints::CommandOutput;
use lix_rs_sdk::{MergeOutcome, MergeVersionOptions};

pub fn run(context: &AppContext, command: MergeVersionCommand) -> Result<CommandOutput, CliError> {
    let path = resolve_db_path(context)?;
    let lix = open_lix_at(&path)?;
    let source = resolve_version_ref(
        &lix,
        match (command.source_id.as_deref(), command.source_name.as_deref()) {
            (Some(id), None) => VersionLookup::Id(id),
            (None, Some(name)) => VersionLookup::Name(name),
            _ => {
                return Err(CliError::msg(
                    "version merge requires exactly one of --source-id or --source-name",
                ));
            }
        },
    )?;
    let target = resolve_version_ref(
        &lix,
        match (command.target_id.as_deref(), command.target_name.as_deref()) {
            (Some(id), None) => VersionLookup::Id(id),
            (None, Some(name)) => VersionLookup::Name(name),
            _ => {
                return Err(CliError::msg(
                    "version merge requires exactly one of --target-id or --target-name",
                ));
            }
        },
    )?;
    let result = pollster::block_on(lix.merge_version(MergeVersionOptions {
        source_version_id: source.id.clone(),
        target_version_id: target.id.clone(),
        expected_heads: None,
    }))
    .map_err(|error| CliError::msg(error.to_string()))?;

    match result.outcome {
        MergeOutcome::AlreadyUpToDate => {
            println!(
                "{} ({}) already contains {} ({})",
                target.name, target.id, source.name, source.id
            );
        }
        MergeOutcome::FastForwarded => {
            println!(
                "Fast-forwarded {} ({}) to {}",
                target.name, target.id, result.target_head_after_commit_id
            );
        }
        MergeOutcome::MergeCommitted => {
            let commit_id = result.created_merge_commit_id.ok_or_else(|| {
                CliError::msg("merge_version returned MergeCommitted without a merge commit id")
            })?;
            println!(
                "Merged {} ({}) into {} ({}) with commit {}",
                source.name, source.id, target.name, target.id, commit_id
            );
        }
    }

    Ok(CommandOutput::empty())
}
