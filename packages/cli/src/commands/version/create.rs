use crate::app::AppContext;
use crate::cli::version::CreateVersionCommand;
use crate::commands::version::{
    resolve_active_version_ref, resolve_version_ref, ResolvedVersionRef, VersionLookup,
};
use crate::db::{open_lix_at, resolve_db_path};
use crate::error::CliError;
use crate::hints::CommandOutput;
use lix_rs_sdk::{CreateVersionOptions, CreateVersionResult};

pub fn run(context: &AppContext, command: CreateVersionCommand) -> Result<CommandOutput, CliError> {
    let path = resolve_db_path(context)?;
    let lix = open_lix_at(&path)?;
    let source_version_id = match (command.from_id.as_deref(), command.from_name.as_deref()) {
        (Some(id), None) => Some(resolve_version_ref(&lix, VersionLookup::Id(id))?.id),
        (None, Some(name)) => Some(resolve_version_ref(&lix, VersionLookup::Name(name))?.id),
        (None, None) => None,
        _ => {
            return Err(CliError::msg(
                "version create accepts at most one of --from-id or --from-name",
            ));
        }
    };
    let result = pollster::block_on(lix.create_version(CreateVersionOptions {
        id: command.id,
        name: command.name,
        source_version_id,
        hidden: command.hidden,
    }))
    .map_err(|error| CliError::msg(error.to_string()))?;
    let parent = resolve_version_ref(&lix, VersionLookup::Id(&result.parent_version_id))?;
    let active = resolve_active_version_ref(&lix)?;

    let (created_line, active_line) = create_confirmation_lines(&result, &parent, &active);
    println!("{created_line}");
    println!("{active_line}");
    Ok(CommandOutput::empty())
}

fn create_confirmation_lines(
    result: &CreateVersionResult,
    parent: &ResolvedVersionRef,
    active: &ResolvedVersionRef,
) -> (String, String) {
    (
        format!(
            "Created version {} ({}) with initial head from {} ({}) at commit {}.",
            result.name, result.id, parent.name, parent.id, result.parent_commit_id
        ),
        format!(
            "Active version is still {} ({}). Use `lix version switch --id {}` or `lix version switch --name {}` to work on it.",
            active.name, active.id, result.id, result.name
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::create_confirmation_lines;
    use crate::commands::version::ResolvedVersionRef;
    use lix_rs_sdk::CreateVersionResult;

    #[test]
    fn create_confirmation_uses_active_version_not_parent_version() {
        let result = CreateVersionResult {
            id: "new-version".to_string(),
            name: "New Version".to_string(),
            parent_version_id: "feature-b".to_string(),
            parent_commit_id: "commit-123".to_string(),
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
