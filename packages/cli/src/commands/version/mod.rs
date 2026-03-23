mod create;
mod merge;
mod switch;

use crate::app::AppContext;
use crate::cli::version::{VersionCommand, VersionSubcommand};
use crate::error::CliError;
use crate::hints::CommandOutput;
use lix_rs_sdk::{ExecuteResult, Lix, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VersionLookup<'a> {
    Id(&'a str),
    Name(&'a str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResolvedVersionRef {
    pub id: String,
    pub name: String,
}

pub fn run(context: &AppContext, command: VersionCommand) -> Result<CommandOutput, CliError> {
    match command.command {
        VersionSubcommand::Create(command) => create::run(context, command),
        VersionSubcommand::Merge(command) => merge::run(context, command),
        VersionSubcommand::Switch(command) => switch::run(context, command),
    }
}

pub(super) fn resolve_version_ref(
    lix: &Lix,
    lookup: VersionLookup<'_>,
) -> Result<ResolvedVersionRef, CliError> {
    match lookup {
        VersionLookup::Id(id) => resolve_version_by_id(lix, id),
        VersionLookup::Name(name) => resolve_version_by_name(lix, name),
    }
}

pub(super) fn resolve_active_version_ref(lix: &Lix) -> Result<ResolvedVersionRef, CliError> {
    let result = pollster::block_on(lix.execute(
        "SELECT v.id, v.name \
         FROM lix_active_version av \
         JOIN lix_version v ON v.id = av.version_id \
         ORDER BY av.id \
         LIMIT 1",
        &[],
    ))
    .map_err(|error| CliError::msg(error.to_string()))?;
    let rows = statement_rows(&result)?;
    let Some(row) = rows.first() else {
        return Err(CliError::msg("active version row is missing"));
    };

    Ok(ResolvedVersionRef {
        id: text_at(row, 0, "lix_version.id")?,
        name: text_at(row, 1, "lix_version.name")?,
    })
}

fn resolve_version_by_id(lix: &Lix, id: &str) -> Result<ResolvedVersionRef, CliError> {
    let result = pollster::block_on(lix.execute(
        "SELECT id, name FROM lix_version WHERE id = $1 LIMIT 1",
        &[Value::Text(id.to_string())],
    ))
    .map_err(|error| CliError::msg(error.to_string()))?;
    let rows = statement_rows(&result)?;
    let Some(row) = rows.first() else {
        return Err(CliError::msg(format!("no version exists with id '{id}'")));
    };

    Ok(ResolvedVersionRef {
        id: text_at(row, 0, "lix_version.id")?,
        name: text_at(row, 1, "lix_version.name")?,
    })
}

fn resolve_version_by_name(lix: &Lix, name: &str) -> Result<ResolvedVersionRef, CliError> {
    let result = pollster::block_on(lix.execute(
        "SELECT id, name FROM lix_version WHERE name = $1 ORDER BY id",
        &[Value::Text(name.to_string())],
    ))
    .map_err(|error| CliError::msg(error.to_string()))?;
    let rows = statement_rows(&result)?;
    match rows {
        [] => Err(CliError::msg(format!(
            "no version exists with name '{name}'"
        ))),
        [row] => Ok(ResolvedVersionRef {
            id: text_at(row, 0, "lix_version.id")?,
            name: text_at(row, 1, "lix_version.name")?,
        }),
        rows => {
            let matching_ids = rows
                .iter()
                .map(|row| text_at(row, 0, "lix_version.id"))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            Err(CliError::msg(format!(
                "version name '{name}' is ambiguous; matching ids: {matching_ids}"
            )))
        }
    }
}

fn statement_rows(result: &ExecuteResult) -> Result<&[Vec<Value>], CliError> {
    let [statement] = result.statements.as_slice() else {
        return Err(CliError::msg(format!(
            "expected one statement result, got {}",
            result.statements.len()
        )));
    };
    Ok(statement.rows.as_slice())
}

fn text_at(row: &[Value], index: usize, field: &str) -> Result<String, CliError> {
    match row.get(index) {
        Some(Value::Text(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Text(_)) => Err(CliError::msg(format!("{field} is empty"))),
        Some(Value::Integer(value)) => Ok(value.to_string()),
        Some(other) => Err(CliError::msg(format!(
            "expected text-like value for {field}, got {other:?}"
        ))),
        None => Err(CliError::msg(format!("missing {field}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::{create, merge, resolve_version_ref, switch, VersionLookup};
    use crate::app::AppContext;
    use crate::cli::version::{CreateVersionCommand, MergeVersionCommand, SwitchVersionCommand};
    use crate::db::{init_lix_at, open_lix_at};
    use lix_rs_sdk::{CreateVersionOptions, Value};
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_lix_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "lix-cli-version-{label}-{}-{nanos}.lix",
            std::process::id()
        ))
    }

    fn cleanup_lix_path(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
        let _ = std::fs::remove_file(format!("{}-journal", path.display()));
    }

    fn text_at(rows: &[Vec<Value>], row: usize, col: usize) -> String {
        match rows.get(row).and_then(|row| row.get(col)) {
            Some(Value::Text(value)) => value.clone(),
            Some(Value::Integer(value)) => value.to_string(),
            other => panic!("expected text-like value, got {other:?}"),
        }
    }

    #[test]
    fn fast_forward_merge_keeps_database_openable_across_fresh_opens() {
        std::thread::Builder::new()
            .name("fast_forward_merge_keeps_database_openable_across_fresh_opens".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                fast_forward_merge_keeps_database_openable_across_fresh_opens_inner();
            })
            .expect("test thread should spawn")
            .join()
            .expect("test thread should not panic");
    }

    fn fast_forward_merge_keeps_database_openable_across_fresh_opens_inner() {
        let path = temp_lix_path("fast-forward-openable");
        cleanup_lix_path(&path);

        init_lix_at(&path).expect("lix init should succeed");
        let context = AppContext {
            lix_path: Some(path.clone()),
            no_hints: true,
        };

        let lix = open_lix_at(&path).expect("initial open should succeed");
        pollster::block_on(lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('greeting', 'hello')",
            &[],
        ))
        .expect("main insert should succeed");

        create::run(
            &context,
            CreateVersionCommand {
                id: Some("feature".to_string()),
                name: Some("feature".to_string()),
                from_id: None,
                from_name: None,
                hidden: false,
            },
        )
        .expect("version create should succeed");

        switch::run(
            &context,
            SwitchVersionCommand {
                id: None,
                name: Some("feature".to_string()),
            },
        )
        .expect("version switch should succeed");

        let lix = open_lix_at(&path).expect("open on feature should succeed");
        pollster::block_on(lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('feature_key', 'feature_val')",
            &[],
        ))
        .expect("feature insert should succeed");

        let lix = open_lix_at(&path).expect("open for id lookup should succeed");
        let main_id_result = pollster::block_on(lix.execute(
            "SELECT id FROM lix_version WHERE name = 'main' LIMIT 1",
            &[],
        ))
        .expect("main id lookup should succeed");
        let main_id = text_at(&main_id_result.statements[0].rows, 0, 0);

        merge::run(
            &context,
            MergeVersionCommand {
                source_id: None,
                source_name: Some("feature".to_string()),
                target_id: Some(main_id.clone()),
                target_name: None,
            },
        )
        .expect("fast-forward merge should succeed");

        let reopened = open_lix_at(&path).expect("database should reopen after fast-forward merge");
        let select_result = pollster::block_on(reopened.execute("SELECT 1", &[]))
            .expect("reopened query should succeed");
        assert_eq!(text_at(&select_result.statements[0].rows, 0, 0), "1");

        switch::run(
            &context,
            SwitchVersionCommand {
                id: Some(main_id),
                name: None,
            },
        )
        .expect("switch back to main should succeed");
        let reopened = open_lix_at(&path).expect("main reopen should succeed");
        let feature_result = pollster::block_on(reopened.execute(
            "SELECT value FROM lix_key_value WHERE key = 'feature_key' LIMIT 1",
            &[],
        ))
        .expect("feature key query should succeed");
        assert_eq!(
            text_at(&feature_result.statements[0].rows, 0, 0),
            "feature_val"
        );

        cleanup_lix_path(&path);
    }

    #[test]
    fn resolve_version_ref_by_name_rejects_ambiguous_matches() {
        std::thread::Builder::new()
            .name("resolve_version_ref_by_name_rejects_ambiguous_matches".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(resolve_version_ref_by_name_rejects_ambiguous_matches_inner)
            .expect("test thread should spawn")
            .join()
            .expect("test thread should not panic");
    }

    fn resolve_version_ref_by_name_rejects_ambiguous_matches_inner() {
        let path = temp_lix_path("ambiguous-version-name");
        cleanup_lix_path(&path);

        init_lix_at(&path).expect("lix init should succeed");
        let lix = open_lix_at(&path).expect("open should succeed");
        pollster::block_on(lix.create_version(CreateVersionOptions {
            id: Some("feature-a".to_string()),
            name: Some("feature".to_string()),
            source_version_id: None,
            hidden: false,
        }))
        .expect("first version create should succeed");
        pollster::block_on(lix.create_version(CreateVersionOptions {
            id: Some("feature-b".to_string()),
            name: Some("feature".to_string()),
            source_version_id: None,
            hidden: false,
        }))
        .expect("second version create should succeed");

        let error = resolve_version_ref(&lix, VersionLookup::Name("feature"))
            .expect_err("ambiguous version name should fail");
        assert_eq!(
            error.to_string(),
            "version name 'feature' is ambiguous; matching ids: feature-a, feature-b"
        );

        cleanup_lix_path(&path);
    }

    #[test]
    fn resolve_version_ref_by_name_rejects_missing_match() {
        std::thread::Builder::new()
            .name("resolve_version_ref_by_name_rejects_missing_match".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(resolve_version_ref_by_name_rejects_missing_match_inner)
            .expect("test thread should spawn")
            .join()
            .expect("test thread should not panic");
    }

    fn resolve_version_ref_by_name_rejects_missing_match_inner() {
        let path = temp_lix_path("missing-version-name");
        cleanup_lix_path(&path);

        init_lix_at(&path).expect("lix init should succeed");
        let lix = open_lix_at(&path).expect("open should succeed");

        let error = resolve_version_ref(&lix, VersionLookup::Name("missing"))
            .expect_err("missing version name should fail");
        assert_eq!(error.to_string(), "no version exists with name 'missing'");

        cleanup_lix_path(&path);
    }
}
