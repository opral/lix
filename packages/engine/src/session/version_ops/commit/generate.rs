use std::collections::{BTreeMap, BTreeSet};

use crate::canonical::CanonicalChangeWrite;
use crate::schema::{builtin_schema_definition, decode_lixcol_literal};
use crate::{CanonicalJson, LixError};
use serde_json::json;

use super::types::{GenerateCommitArgs, GenerateCommitResult, StagedChange};
use super::UpdatedVersionRef;

const COMMIT_SCHEMA_KEY: &str = "lix_commit";
const CHANGE_SET_SCHEMA_KEY: &str = "lix_change_set";

#[derive(Debug, Clone)]
struct BuiltinSchemaMeta {
    schema_version: String,
    file_id: String,
    plugin_key: String,
}

#[derive(Debug, Clone)]
struct VersionMeta {
    commit_id: String,
    change_set_id: String,
    parent_commit_ids: Vec<String>,
}

pub fn generate_commit<F>(
    args: GenerateCommitArgs,
    mut generate_uuid: F,
) -> Result<GenerateCommitResult, LixError>
where
    F: FnMut() -> String,
{
    if args.versions.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "generate_commit: versions map is required".to_string(),
        });
    }

    // Ensure version snapshots are keyed correctly.
    for (version_id, info) in &args.versions {
        if info.snapshot.id.as_str() != version_id {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "generate_commit: versions['{version_id}'].snapshot.id must equal version id"
                ),
            });
        }
    }

    // Validate duplicate staged change ids.
    let mut seen_ids = BTreeSet::new();
    for change in &args.changes {
        let change_id = require_staged_change_id(change)?;
        if !seen_ids.insert(change_id.to_string()) {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("generate_commit: duplicate change id '{}'", change_id),
            });
        }
        validate_staged_change(change)?;
    }

    let commit_schema = builtin_schema_meta(COMMIT_SCHEMA_KEY)?;
    let change_set_schema = builtin_schema_meta(CHANGE_SET_SCHEMA_KEY)?;
    let effective_changes = collapse_staged_changes_last_wins(&args.changes);
    let mut output_changes: Vec<CanonicalChangeWrite> = effective_changes
        .iter()
        .map(|change| sanitize_staged_change(change))
        .collect();
    let mut updated_version_refs: Vec<UpdatedVersionRef> = Vec::new();

    let mut changes_by_version: BTreeMap<String, Vec<&StagedChange>> = BTreeMap::new();
    for change in &effective_changes {
        changes_by_version
            .entry(change.version_id.to_string())
            .or_default()
            .push(*change);
    }

    let versions_to_commit: BTreeSet<String> = changes_by_version
        .keys()
        .cloned()
        .chain(args.force_commit_versions.iter().cloned())
        .collect();
    let mut meta_by_version: BTreeMap<String, VersionMeta> = BTreeMap::new();
    for version_id in versions_to_commit {
        let version_info = args.versions.get(&version_id).ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "generate_commit: missing version context for '{}'",
                version_id
            ),
        })?;
        meta_by_version.insert(
            version_id,
            VersionMeta {
                commit_id: generate_uuid(),
                change_set_id: generate_uuid(),
                parent_commit_ids: version_info.parent_commit_ids.clone(),
            },
        );
    }

    let unique_active_accounts = dedupe_ordered(&args.active_accounts);
    let mut meta_changes: Vec<CanonicalChangeWrite> = Vec::new();
    let mut commit_row_index_by_version: BTreeMap<String, usize> = BTreeMap::new();

    for (version_id, meta) in &meta_by_version {
        let change_set_change_id = generate_uuid();
        meta_changes.push(CanonicalChangeWrite {
            id: change_set_change_id,
            entity_id: expect_identity(meta.change_set_id.clone(), "change_set entity_id"),
            schema_key: expect_identity(CHANGE_SET_SCHEMA_KEY.to_string(), "change_set schema_key"),
            schema_version: expect_identity(
                change_set_schema.schema_version.clone(),
                "change_set schema_version",
            ),
            file_id: expect_identity(change_set_schema.file_id.clone(), "change_set file_id"),
            plugin_key: expect_identity(
                change_set_schema.plugin_key.clone(),
                "change_set plugin_key",
            ),
            snapshot_content: Some(canonical_json(json!({
                "id": meta.change_set_id,
            }))?),
            metadata: None,
            created_at: args.timestamp.clone(),
        });

        let commit_change_id = generate_uuid();
        let commit_row_idx = meta_changes.len();
        commit_row_index_by_version.insert(version_id.clone(), commit_row_idx);
        meta_changes.push(CanonicalChangeWrite {
            id: commit_change_id,
            entity_id: expect_identity(meta.commit_id.clone(), "commit entity_id"),
            schema_key: expect_identity(COMMIT_SCHEMA_KEY.to_string(), "commit schema_key"),
            schema_version: expect_identity(
                commit_schema.schema_version.clone(),
                "commit schema_version",
            ),
            file_id: expect_identity(commit_schema.file_id.clone(), "commit file_id"),
            plugin_key: expect_identity(commit_schema.plugin_key.clone(), "commit plugin_key"),
            snapshot_content: Some(canonical_json(json!({
                "id": meta.commit_id,
                "change_set_id": meta.change_set_id,
            }))?),
            metadata: None,
            created_at: args.timestamp.clone(),
        });
    }

    // Update commit snapshots with membership metadata.
    for (version_id, meta) in &meta_by_version {
        let commit_row_idx =
            *commit_row_index_by_version
                .get(version_id)
                .ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "generate_commit: missing commit row index for version '{}'",
                        version_id
                    ),
                })?;
        let commit_row = meta_changes
            .get_mut(commit_row_idx)
            .ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "generate_commit: missing commit row for version '{}'",
                    version_id
                ),
            })?;

        let raw_snapshot = commit_row
            .snapshot_content
            .as_ref()
            .ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "generate_commit: commit row for version '{}' is missing snapshot_content",
                    version_id
                ),
            })?;
        let mut snapshot: serde_json::Value =
            raw_snapshot.to_value().map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "generate_commit: commit snapshot for version '{}' is invalid JSON: {}",
                    version_id, error.description
                ),
            })?;

        let member_change_ids: Vec<String> = changes_by_version
            .get(version_id)
            .into_iter()
            .flat_map(|changes| {
                changes
                    .iter()
                    .map(|change| require_staged_change_id(change).map(ToString::to_string))
            })
            .collect::<Result<Vec<_>, _>>()?;
        snapshot["change_ids"] = serde_json::Value::Array(
            member_change_ids
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect(),
        );
        snapshot["author_account_ids"] = serde_json::Value::Array(
            unique_active_accounts
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect(),
        );
        snapshot["parent_commit_ids"] = serde_json::Value::Array(
            meta.parent_commit_ids
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect(),
        );

        commit_row.snapshot_content = Some(canonical_json(snapshot)?);
    }

    for (version_id, meta) in &meta_by_version {
        updated_version_refs.push(UpdatedVersionRef {
            version_id: expect_identity(version_id.clone(), "version_ref version_id"),
            commit_id: meta.commit_id.clone(),
            created_at: args.timestamp.clone(),
        });
    }

    output_changes.extend(meta_changes);

    let affected_versions = effective_changes
        .iter()
        .map(|change| change.version_id.to_string())
        .chain(
            updated_version_refs
                .iter()
                .map(|update| update.version_id.to_string()),
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    Ok(GenerateCommitResult {
        canonical_changes: output_changes,
        updated_version_refs,
        affected_versions,
    })
}

fn sanitize_staged_change(change: &StagedChange) -> CanonicalChangeWrite {
    CanonicalChangeWrite {
        id: require_staged_change_id(change).unwrap().to_string(),
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        schema_version: change.schema_version.clone().unwrap(),
        file_id: change.file_id.clone().unwrap(),
        plugin_key: change.plugin_key.clone().unwrap(),
        snapshot_content: change
            .snapshot_content
            .as_deref()
            .map(CanonicalJson::from_text)
            .transpose()
            .unwrap(),
        metadata: change
            .metadata
            .as_deref()
            .map(CanonicalJson::from_text)
            .transpose()
            .unwrap(),
        created_at: require_staged_change_created_at(change)
            .unwrap()
            .to_string(),
    }
}

fn validate_staged_change(change: &StagedChange) -> Result<(), LixError> {
    let change_id = require_staged_change_id(change)?;
    let created_at = require_staged_change_created_at(change)?;
    let schema_version = require_staged_change_schema_version(change)?;
    let file_id = require_staged_change_file_id(change)?;
    let plugin_key = require_staged_change_plugin_key(change)?;

    let change_label = if change_id.is_empty() {
        "<empty change id>"
    } else {
        change_id
    };

    for (field, value) in [
        ("id", change_id),
        ("entity_id", change.entity_id.as_str()),
        ("schema_key", change.schema_key.as_str()),
        ("schema_version", schema_version),
        ("file_id", file_id),
        ("plugin_key", plugin_key),
        ("version_id", change.version_id.as_str()),
        ("created_at", created_at),
    ] {
        if value.is_empty() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "generate_commit: change '{change_label}' requires non-empty {field}"
                ),
            });
        }
    }

    Ok(())
}

fn require_staged_change_id(change: &StagedChange) -> Result<&str, LixError> {
    change.id.as_deref().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "generate_commit: staged change '{}:{}' requires id",
            change.schema_key, change.entity_id
        ),
    })
}

fn require_staged_change_created_at(change: &StagedChange) -> Result<&str, LixError> {
    change.created_at.as_deref().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "generate_commit: staged change '{}:{}' requires created_at",
            change.schema_key, change.entity_id
        ),
    })
}

fn require_staged_change_schema_version(change: &StagedChange) -> Result<&str, LixError> {
    change
        .schema_version
        .as_ref()
        .map(|value| value.as_str())
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "generate_commit: staged change '{}:{}' requires schema_version",
                change.schema_key, change.entity_id
            ),
        })
}

fn require_staged_change_file_id(change: &StagedChange) -> Result<&str, LixError> {
    change
        .file_id
        .as_ref()
        .map(|value| value.as_str())
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "generate_commit: staged change '{}:{}' requires file_id",
                change.schema_key, change.entity_id
            ),
        })
}

fn require_staged_change_plugin_key(change: &StagedChange) -> Result<&str, LixError> {
    change
        .plugin_key
        .as_ref()
        .map(|value| value.as_str())
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "generate_commit: staged change '{}:{}' requires plugin_key",
                change.schema_key, change.entity_id
            ),
        })
}

fn expect_identity<T>(value: impl Into<String>, context: &str) -> T
where
    T: TryFrom<String, Error = LixError>,
{
    T::try_from(value.into()).unwrap_or_else(|error| {
        panic!("{context}: {}", error.description);
    })
}

fn canonical_json(value: serde_json::Value) -> Result<CanonicalJson, LixError> {
    CanonicalJson::from_value(value).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "generate_commit: failed to encode canonical JSON payload: {}",
            error.description
        ),
    })
}

fn builtin_schema_meta(schema_key: &str) -> Result<BuiltinSchemaMeta, LixError> {
    let schema = builtin_schema_definition(schema_key).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("generate_commit: builtin schema '{}' not found", schema_key),
    })?;
    let schema_version = schema
        .get("x-lix-version")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "generate_commit: builtin schema '{}' is missing string x-lix-version",
                schema_key
            ),
        })?
        .to_string();
    let overrides = schema
        .get("x-lix-override-lixcols")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "generate_commit: builtin schema '{}' is missing x-lix-override-lixcols",
                schema_key
            ),
        })?;
    let file_id = overrides
        .get("lixcol_file_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "generate_commit: builtin schema '{}' is missing string lixcol_file_id",
                schema_key
            ),
        })?;
    let plugin_key = overrides
        .get("lixcol_plugin_key")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "generate_commit: builtin schema '{}' is missing string lixcol_plugin_key",
                schema_key
            ),
        })?;

    Ok(BuiltinSchemaMeta {
        schema_version,
        file_id: decode_lixcol_literal(file_id),
        plugin_key: decode_lixcol_literal(plugin_key),
    })
}

fn dedupe_ordered(values: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();
    for value in values {
        if seen.insert(value.clone()) {
            deduped.push(value.clone());
        }
    }
    deduped
}

fn collapse_staged_changes_last_wins(changes: &[StagedChange]) -> Vec<&StagedChange> {
    let mut latest_index_by_key: BTreeMap<(String, String, String, String), usize> =
        BTreeMap::new();
    for (index, change) in changes.iter().enumerate() {
        latest_index_by_key.insert(
            (
                change.version_id.to_string(),
                change.entity_id.to_string(),
                change.schema_key.to_string(),
                require_staged_change_file_id(change)
                    .expect("validated staged changes require file_id")
                    .to_string(),
            ),
            index,
        );
    }

    let mut kept_indexes = latest_index_by_key.into_values().collect::<Vec<_>>();
    kept_indexes.sort_unstable();
    kept_indexes
        .into_iter()
        .map(|index| &changes[index])
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::version_ops::{VersionInfo, VersionSnapshot};

    fn staged_change(
        id: &str,
        entity_id: &str,
        schema_key: &str,
        version_id: &str,
        writer_key: Option<&str>,
    ) -> StagedChange {
        StagedChange {
            id: Some(id.to_string()),
            entity_id: entity_id.try_into().unwrap(),
            schema_key: schema_key.try_into().unwrap(),
            schema_version: Some("1".try_into().unwrap()),
            file_id: Some("lix".try_into().unwrap()),
            plugin_key: Some("lix".try_into().unwrap()),
            snapshot_content: Some(
                CanonicalJson::from_text(format!(r#"{{"id":"{id}"}}"#))
                    .expect("test snapshot should be valid canonical json")
                    .as_str()
                    .to_string(),
            ),
            metadata: None,
            created_at: Some("2025-01-01T00:00:00.000Z".to_string()),
            version_id: version_id.try_into().unwrap(),
            writer_key: writer_key.map(ToString::to_string),
        }
    }

    fn version_info(id: &str, parent_commit_ids: &[&str]) -> VersionInfo {
        VersionInfo {
            parent_commit_ids: parent_commit_ids.iter().map(ToString::to_string).collect(),
            snapshot: VersionSnapshot {
                id: id.try_into().unwrap(),
            },
        }
    }

    fn counts_by_schema(result: &GenerateCommitResult) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for row in &result.canonical_changes {
            *counts.entry(row.schema_key.to_string()).or_insert(0) += 1;
        }
        counts
    }

    #[test]
    fn generates_commit_for_single_active_version_change() {
        let mut versions = BTreeMap::new();
        versions.insert(
            "version-main".to_string(),
            version_info("version-main", &["P_active"]),
        );
        versions.insert("global".to_string(), version_info("global", &["P_global"]));

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec!["acct-1".to_string()],
            changes: vec![staged_change(
                "chg_active",
                "kv_active",
                "lix_key_value",
                "version-main",
                Some("writer:test"),
            )],
            versions,
            force_commit_versions: BTreeSet::new(),
        };

        let mut n = 0u64;
        let result = generate_commit(args, || {
            let id = format!("uuid-{n}");
            n += 1;
            id
        })
        .expect("generate_commit should succeed");

        let counts = counts_by_schema(&result);
        assert_eq!(counts.get("lix_key_value"), Some(&1));
        assert_eq!(counts.get("lix_change_set"), Some(&1));
        assert_eq!(counts.get("lix_commit"), Some(&1));
        let commit_row = result
            .canonical_changes
            .iter()
            .find(|row| row.schema_key == "lix_commit")
            .expect("expected commit row");
        assert_eq!(commit_row.plugin_key, "lix");
        let commit_snapshot: serde_json::Value =
            serde_json::from_str(commit_row.snapshot_content.as_ref().unwrap()).unwrap();
        assert_eq!(
            commit_snapshot["change_ids"],
            serde_json::json!(["chg_active"])
        );
        assert_eq!(
            commit_snapshot["parent_commit_ids"],
            serde_json::json!(["P_active"])
        );
        assert_eq!(
            commit_snapshot["author_account_ids"],
            serde_json::json!(["acct-1"])
        );
        assert_eq!(result.updated_version_refs.len(), 1);
        assert_eq!(
            result.updated_version_refs[0].version_id.as_str(),
            "version-main"
        );
        assert_eq!(result.affected_versions, vec!["version-main".to_string()]);
    }

    #[test]
    fn generates_commit_for_global_change() {
        let mut versions = BTreeMap::new();
        versions.insert("global".to_string(), version_info("global", &["P_global"]));

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec!["acct-1".to_string()],
            changes: vec![staged_change(
                "chg_global",
                "kv_global",
                "lix_key_value",
                "global",
                None,
            )],
            versions,
            force_commit_versions: BTreeSet::new(),
        };

        let mut n = 0u64;
        let result = generate_commit(args, || {
            let id = format!("uuid-{n}");
            n += 1;
            id
        })
        .expect("generate_commit should succeed");

        let counts = counts_by_schema(&result);
        assert_eq!(counts.get("lix_key_value"), Some(&1));
        assert_eq!(counts.get("lix_change_set"), Some(&1));
        assert_eq!(counts.get("lix_commit"), Some(&1));
        assert_eq!(result.updated_version_refs.len(), 1);
        assert_eq!(result.updated_version_refs[0].version_id.as_str(), "global");
        assert_eq!(result.affected_versions, vec!["global".to_string()]);

        let commit_row = result
            .canonical_changes
            .iter()
            .find(|row| row.schema_key == "lix_commit")
            .expect("expected commit row");
        let commit_snapshot: serde_json::Value =
            serde_json::from_str(commit_row.snapshot_content.as_ref().unwrap()).unwrap();
        assert_eq!(
            commit_snapshot["author_account_ids"],
            serde_json::json!(["acct-1"])
        );
        assert_eq!(
            commit_snapshot["parent_commit_ids"],
            serde_json::json!(["P_global"])
        );
        assert_eq!(
            commit_snapshot["change_ids"],
            serde_json::json!(["chg_global"])
        );
    }

    #[test]
    fn generates_commits_for_multiple_versions() {
        let mut versions = BTreeMap::new();
        versions.insert("global".to_string(), version_info("global", &["P_global"]));
        versions.insert(
            "version-main".to_string(),
            version_info("version-main", &["P_main"]),
        );

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec!["acct-1".to_string(), "acct-2".to_string()],
            changes: vec![
                staged_change("chg_global", "kv_global", "lix_key_value", "global", None),
                staged_change("chg_main", "kv_main", "lix_key_value", "version-main", None),
            ],
            versions,
            force_commit_versions: BTreeSet::new(),
        };

        let mut n = 0u64;
        let result = generate_commit(args, || {
            let id = format!("uuid-{n}");
            n += 1;
            id
        })
        .expect("generate_commit should succeed");

        let counts = counts_by_schema(&result);
        assert_eq!(counts.get("lix_key_value"), Some(&2));
        assert_eq!(counts.get("lix_change_set"), Some(&2));
        assert_eq!(counts.get("lix_commit"), Some(&2));
        assert_eq!(result.updated_version_refs.len(), 2);

        let commit_rows: Vec<_> = result
            .canonical_changes
            .iter()
            .filter(|row| row.schema_key == "lix_commit")
            .collect();
        assert_eq!(commit_rows.len(), 2);
        for commit_row in commit_rows {
            let commit_snapshot: serde_json::Value =
                serde_json::from_str(commit_row.snapshot_content.as_ref().unwrap()).unwrap();
            assert_eq!(
                commit_snapshot["author_account_ids"],
                serde_json::json!(["acct-1", "acct-2"])
            );
            assert_eq!(commit_snapshot["change_ids"].as_array().unwrap().len(), 1);
        }
        let updated_versions = result
            .updated_version_refs
            .iter()
            .map(|update| update.version_id.to_string())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            updated_versions,
            BTreeSet::from(["global".to_string(), "version-main".to_string()])
        );
        assert_eq!(
            result.affected_versions,
            vec!["global".to_string(), "version-main".to_string()]
        );
    }

    #[test]
    fn collapses_staged_changes_per_entity_schema_file_with_last_wins() {
        let mut versions = BTreeMap::new();
        versions.insert("global".to_string(), version_info("global", &["P_global"]));

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec!["acct-1".to_string()],
            changes: vec![
                staged_change("chg_a1", "entity-a", "lix_key_value", "global", None),
                staged_change("chg_b1", "entity-b", "lix_key_value", "global", None),
                staged_change("chg_a2", "entity-a", "lix_key_value", "global", None),
            ],
            versions,
            force_commit_versions: BTreeSet::new(),
        };

        let mut n = 0u64;
        let result = generate_commit(args, || {
            let id = format!("uuid-{n}");
            n += 1;
            id
        })
        .expect("generate_commit should succeed");

        let change_ids = result
            .canonical_changes
            .iter()
            .filter(|row| row.schema_key == "lix_key_value")
            .map(|row| row.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(change_ids, vec!["chg_b1", "chg_a2"]);
        let commit_row = result
            .canonical_changes
            .iter()
            .find(|row| row.schema_key == "lix_commit")
            .expect("expected commit row");
        let commit_snapshot: serde_json::Value =
            serde_json::from_str(commit_row.snapshot_content.as_ref().unwrap()).unwrap();
        assert_eq!(
            commit_snapshot["change_ids"],
            serde_json::json!(["chg_b1", "chg_a2"])
        );
    }

    #[test]
    fn rejects_duplicate_change_ids() {
        let mut versions = BTreeMap::new();
        versions.insert("global".to_string(), version_info("global", &["P_global"]));

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec![],
            changes: vec![
                staged_change("dup", "entity-a", "lix_key_value", "global", None),
                staged_change("dup", "entity-b", "lix_key_value", "global", None),
            ],
            versions,
            force_commit_versions: BTreeSet::new(),
        };

        let error =
            generate_commit(args, || "id".to_string()).expect_err("expected duplicate id error");
        assert!(
            error.description.contains("duplicate change id"),
            "unexpected error: {}",
            error.description
        );
    }

    #[test]
    fn rejects_empty_change_entity_id() {
        let error = crate::EntityId::try_from("")
            .expect_err("expected empty entity_id to be rejected before commit generation");
        assert!(
            error.description.contains("entity_id must be non-empty"),
            "unexpected error: {}",
            error.description
        );
    }

    #[test]
    fn rejects_missing_version_context_for_change() {
        let mut versions = BTreeMap::new();
        versions.insert("global".to_string(), version_info("global", &["P_global"]));

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec![],
            changes: vec![staged_change(
                "chg-missing",
                "entity-a",
                "lix_key_value",
                "version-main",
                None,
            )],
            versions,
            force_commit_versions: BTreeSet::new(),
        };

        let error = generate_commit(args, || "id".to_string())
            .expect_err("expected missing version context error");
        assert!(
            error.description.contains("missing version context"),
            "unexpected error: {}",
            error.description
        );
    }

    #[test]
    fn generates_local_version_head_updates_for_forced_empty_commit() {
        let mut versions = BTreeMap::new();
        versions.insert(
            "version-main".to_string(),
            version_info("version-main", &["P_main"]),
        );

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec!["acct-1".to_string()],
            changes: Vec::new(),
            versions,
            force_commit_versions: BTreeSet::from(["version-main".to_string()]),
        };

        let mut n = 0u64;
        let result = generate_commit(args, || {
            let id = format!("uuid-{n}");
            n += 1;
            id
        })
        .expect("generate_commit should succeed");

        let counts = counts_by_schema(&result);
        assert_eq!(counts.get("lix_change_set"), Some(&1));
        assert_eq!(counts.get("lix_commit"), Some(&1));
        assert_eq!(counts.get("lix_key_value"), None);
        let commit_row = result
            .canonical_changes
            .iter()
            .find(|row| row.schema_key == "lix_commit")
            .expect("expected commit row");
        let commit_snapshot: serde_json::Value =
            serde_json::from_str(commit_row.snapshot_content.as_ref().unwrap()).unwrap();
        assert_eq!(commit_snapshot["change_ids"], serde_json::json!([]));
    }
}
