use crate::app::AppContext;
use crate::cli::version::CreateVersionCommand;
use crate::commands::version::{
    resolve_active_version_ref, resolve_version_ref, ResolvedVersionRef, VersionLookup,
};
use crate::db::{open_lix_at, resolve_db_path};
use crate::error::CliError;
use crate::hints::CommandOutput;
use lix_sdk::{CreateBranchOptions, CreateBranchResult, SwitchBranchOptions};

pub fn run(context: &AppContext, command: CreateVersionCommand) -> Result<CommandOutput, CliError> {
    let path = resolve_db_path(context)?;
    let lix = open_lix_at(&path)?;
    let source = match (command.from_id.as_deref(), command.from_name.as_deref()) {
        (Some(id), None) => Some(resolve_version_ref(&lix, VersionLookup::Id(id))?),
        (None, Some(name)) => Some(resolve_version_ref(&lix, VersionLookup::Name(name))?),
        (None, None) => None,
        _ => {
            return Err(CliError::msg(
                "version create accepts at most one of --from-id or --from-name",
            ));
        }
    };
    let original_active = resolve_active_version_ref(&lix)?;
    if let Some(source) = &source {
        crate::db::block_on(lix.switch_branch(SwitchBranchOptions {
            branch_id: source.id.clone(),
        }))
        .map_err(|error| CliError::msg(error.to_string()))?;
    }
    let name = command
        .name
        .clone()
        .or_else(|| command.id.clone())
        .ok_or_else(|| CliError::msg("version create requires --name when --id is omitted"))?;
    let result = crate::db::block_on(lix.create_branch(CreateBranchOptions {
        id: command.id,
        name,
        from_commit_id: None,
    }))
    .map_err(|error| CliError::msg(error.to_string()))?;
    if source.is_some() {
        crate::db::block_on(lix.switch_branch(SwitchBranchOptions {
            branch_id: original_active.id.clone(),
        }))
        .map_err(|error| CliError::msg(error.to_string()))?;
    }

    let parent = source.as_ref().unwrap_or(&original_active);
    let (created_line, active_line) = create_confirmation_lines(&result, parent, &original_active);
    println!("{created_line}");
    println!("{active_line}");
    Ok(CommandOutput::empty())
}

fn create_confirmation_lines(
    result: &CreateBranchResult,
    parent: &ResolvedVersionRef,
    active: &ResolvedVersionRef,
) -> (String, String) {
    (
        format!(
            "Created version {} from {} ({}).",
            result.id, parent.name, parent.id
        ),
        format!(
            "Active version is still {} ({}). Use `lix version switch --id {}` to work on it.",
            active.name, active.id, result.id
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::create_confirmation_lines;
    use crate::commands::version::ResolvedVersionRef;
    use lix_sdk::CreateBranchResult;

    #[test]
    fn create_confirmation_uses_active_version_not_parent_version() {
        let result = CreateBranchResult {
            id: "new-version".to_string(),
            name: "New Version".to_string(),
            hidden: false,
            commit_id: "commit-id".to_string(),
        };
        let parent = ResolvedVersionRef {
            id: "feature-b".to_string(),
            name: "Feature B".to_string(),
        };
        let active = ResolvedVersionRef {
            id: "feature-a".to_string(),
            name: "Feature A".to_string(),
        };

        let (_, active_line) = create_confirmation_lines(&result, &parent, &active);
        assert!(active_line.contains("Feature A (feature-a)"));
        assert!(!active_line.contains("Feature B (feature-b)"));
    }
}
