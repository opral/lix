use crate::app::AppContext;
use crate::cli::version::MergeVersionCommand;
use crate::db::{open_lix_at, resolve_db_path};
use crate::error::CliError;
use crate::hints::CommandOutput;
use lix_rs_sdk::{MergeOutcome, MergeVersionOptions};

pub fn run(context: &AppContext, command: MergeVersionCommand) -> Result<CommandOutput, CliError> {
    let path = resolve_db_path(context)?;
    let lix = open_lix_at(&path)?;
    let result = pollster::block_on(lix.merge_version(MergeVersionOptions {
        source_version_id: command.source.clone(),
        target_version_id: command.target.clone(),
        expected_heads: None,
    }))
    .map_err(|error| CliError::msg(error.to_string()))?;

    match result.outcome {
        MergeOutcome::AlreadyUpToDate => {
            println!(
                "{} already contains {}",
                result.target_version_id, result.source_version_id
            );
        }
        MergeOutcome::FastForwarded => {
            println!(
                "Fast-forwarded {} to {}",
                result.target_version_id, result.target_head_after_commit_id
            );
        }
        MergeOutcome::MergeCommitted => {
            let commit_id = result.created_merge_commit_id.ok_or_else(|| {
                CliError::msg("merge_version returned MergeCommitted without a merge commit id")
            })?;
            println!(
                "Merged {} into {} with commit {}",
                result.source_version_id, result.target_version_id, commit_id
            );
        }
    }

    Ok(CommandOutput::empty())
}
