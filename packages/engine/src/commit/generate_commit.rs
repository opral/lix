use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;

use crate::builtin_schema::builtin_schema_definition;
use crate::commit::types::{
    ChangeRow, DomainChangeInput, GenerateCommitArgs, GenerateCommitResult, MaterializedStateRow,
};
use crate::LixError;

const GLOBAL_VERSION: &str = "global";
const COMMIT_SCHEMA_KEY: &str = "lix_commit";
const VERSION_TIP_SCHEMA_KEY: &str = "lix_version_tip";
const CHANGE_SET_ELEMENT_SCHEMA_KEY: &str = "lix_change_set_element";
const COMMIT_EDGE_SCHEMA_KEY: &str = "lix_commit_edge";
const CHANGE_AUTHOR_SCHEMA_KEY: &str = "lix_change_author";

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
            message: "generate_commit: versions map is required".to_string(),
        });
    }

    // Ensure version snapshots are keyed correctly.
    for (version_id, info) in &args.versions {
        if info.snapshot.id != *version_id {
            return Err(LixError {
                message: format!(
                    "generate_commit: versions['{version_id}'].snapshot.id must equal version id"
                ),
            });
        }
    }

    // Validate duplicate domain change ids.
    let mut seen_ids = BTreeSet::new();
    for change in &args.changes {
        if !seen_ids.insert(change.id.clone()) {
            return Err(LixError {
                message: format!("generate_commit: duplicate change id '{}'", change.id),
            });
        }
    }

    let commit_schema = builtin_schema_meta(COMMIT_SCHEMA_KEY)?;
    let version_tip_schema = builtin_schema_meta(VERSION_TIP_SCHEMA_KEY)?;
    let change_set_element_schema = builtin_schema_meta(CHANGE_SET_ELEMENT_SCHEMA_KEY)?;
    let commit_edge_schema = builtin_schema_meta(COMMIT_EDGE_SCHEMA_KEY)?;
    let change_author_schema = builtin_schema_meta(CHANGE_AUTHOR_SCHEMA_KEY)?;

    let mut output_changes: Vec<ChangeRow> =
        args.changes.iter().map(sanitize_domain_change).collect();
    let mut materialized_state: Vec<MaterializedStateRow> = Vec::new();

    let mut domain_by_version: BTreeMap<String, Vec<&DomainChangeInput>> = BTreeMap::new();
    for change in &args.changes {
        domain_by_version
            .entry(change.version_id.clone())
            .or_default()
            .push(change);
    }

    let versions_to_commit: BTreeSet<String> = domain_by_version.keys().cloned().collect();
    let mut meta_by_version: BTreeMap<String, VersionMeta> = BTreeMap::new();
    for version_id in versions_to_commit {
        let version_info = args.versions.get(&version_id).ok_or_else(|| LixError {
            message: format!(
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
    let mut meta_changes: Vec<ChangeRow> = Vec::new();
    let mut tip_change_id_by_version: BTreeMap<String, String> = BTreeMap::new();
    let mut commit_change_id_by_version: BTreeMap<String, String> = BTreeMap::new();
    let mut commit_row_index_by_version: BTreeMap<String, usize> = BTreeMap::new();

    for (version_id, meta) in &meta_by_version {
        let version_info = args.versions.get(version_id).ok_or_else(|| LixError {
            message: format!(
                "generate_commit: missing version context for '{}'",
                version_id
            ),
        })?;

        let version_tip_change_id = generate_uuid();
        tip_change_id_by_version.insert(version_id.clone(), version_tip_change_id.clone());

        meta_changes.push(ChangeRow {
            id: version_tip_change_id,
            entity_id: version_id.clone(),
            schema_key: VERSION_TIP_SCHEMA_KEY.to_string(),
            schema_version: version_tip_schema.schema_version.clone(),
            file_id: version_tip_schema.file_id.clone(),
            plugin_key: version_tip_schema.plugin_key.clone(),
            snapshot_content: Some(
                json!({
                    "id": version_id,
                    "commit_id": meta.commit_id,
                    "working_commit_id": version_info.snapshot.working_commit_id,
                })
                .to_string(),
            ),
            metadata: None,
            created_at: args.timestamp.clone(),
        });

        let commit_change_id = generate_uuid();
        commit_change_id_by_version.insert(version_id.clone(), commit_change_id.clone());
        let commit_row_idx = meta_changes.len();
        commit_row_index_by_version.insert(version_id.clone(), commit_row_idx);
        meta_changes.push(ChangeRow {
            id: commit_change_id,
            entity_id: meta.commit_id.clone(),
            schema_key: COMMIT_SCHEMA_KEY.to_string(),
            schema_version: commit_schema.schema_version.clone(),
            file_id: commit_schema.file_id.clone(),
            plugin_key: commit_schema.plugin_key.clone(),
            snapshot_content: Some(
                json!({
                    "id": meta.commit_id,
                    "change_set_id": meta.change_set_id,
                })
                .to_string(),
            ),
            metadata: None,
            created_at: args.timestamp.clone(),
        });
    }

    // Materialize domain rows and derived change_set_element rows.
    let global_commit_id = meta_by_version
        .get(GLOBAL_VERSION)
        .map(|meta| meta.commit_id.clone());
    for (version_id, domain_changes) in &domain_by_version {
        let meta = meta_by_version.get(version_id).ok_or_else(|| LixError {
            message: format!("generate_commit: missing version meta for '{}'", version_id),
        })?;
        let cse_commit_id = global_commit_id
            .clone()
            .unwrap_or_else(|| meta.commit_id.clone());

        for change in domain_changes {
            materialized_state.push(MaterializedStateRow {
                id: change.id.clone(),
                entity_id: change.entity_id.clone(),
                schema_key: change.schema_key.clone(),
                schema_version: change.schema_version.clone(),
                file_id: change.file_id.clone(),
                plugin_key: change.plugin_key.clone(),
                snapshot_content: change.snapshot_content.clone(),
                metadata: change.metadata.clone(),
                created_at: change.created_at.clone(),
                lixcol_version_id: version_id.clone(),
                lixcol_commit_id: meta.commit_id.clone(),
                writer_key: change.writer_key.clone(),
            });

            materialized_state.push(MaterializedStateRow {
                id: generate_uuid(),
                entity_id: format!("{}~{}", meta.change_set_id, change.id),
                schema_key: CHANGE_SET_ELEMENT_SCHEMA_KEY.to_string(),
                schema_version: change_set_element_schema.schema_version.clone(),
                file_id: change_set_element_schema.file_id.clone(),
                plugin_key: change_set_element_schema.plugin_key.clone(),
                snapshot_content: Some(
                    json!({
                        "change_set_id": meta.change_set_id,
                        "change_id": change.id,
                        "entity_id": change.entity_id,
                        "schema_key": change.schema_key,
                        "file_id": change.file_id,
                    })
                    .to_string(),
                ),
                metadata: None,
                created_at: args.timestamp.clone(),
                lixcol_version_id: GLOBAL_VERSION.to_string(),
                lixcol_commit_id: cse_commit_id.clone(),
                writer_key: None,
            });
        }
    }

    // Materialize derived per-change authors in global scope.
    for (version_id, domain_changes) in &domain_by_version {
        let meta = meta_by_version.get(version_id).ok_or_else(|| LixError {
            message: format!("generate_commit: missing version meta for '{}'", version_id),
        })?;
        let commit_change_id =
            commit_change_id_by_version
                .get(version_id)
                .ok_or_else(|| LixError {
                    message: format!(
                        "generate_commit: missing commit change id for version '{}'",
                        version_id
                    ),
                })?;

        for change in domain_changes {
            for account_id in &unique_active_accounts {
                materialized_state.push(MaterializedStateRow {
                    id: commit_change_id.clone(),
                    entity_id: format!("{}~{}", change.id, account_id),
                    schema_key: CHANGE_AUTHOR_SCHEMA_KEY.to_string(),
                    schema_version: change_author_schema.schema_version.clone(),
                    file_id: change_author_schema.file_id.clone(),
                    plugin_key: change_author_schema.plugin_key.clone(),
                    snapshot_content: Some(
                        json!({
                            "change_id": change.id,
                            "account_id": account_id,
                        })
                        .to_string(),
                    ),
                    metadata: None,
                    created_at: args.timestamp.clone(),
                    lixcol_version_id: GLOBAL_VERSION.to_string(),
                    lixcol_commit_id: meta.commit_id.clone(),
                    writer_key: None,
                });
            }
        }
    }

    // Update commit snapshots with membership metadata.
    for (version_id, meta) in &meta_by_version {
        let commit_row_idx =
            *commit_row_index_by_version
                .get(version_id)
                .ok_or_else(|| LixError {
                    message: format!(
                        "generate_commit: missing commit row index for version '{}'",
                        version_id
                    ),
                })?;
        let commit_row = meta_changes
            .get_mut(commit_row_idx)
            .ok_or_else(|| LixError {
                message: format!(
                    "generate_commit: missing commit row for version '{}'",
                    version_id
                ),
            })?;

        let raw_snapshot = commit_row
            .snapshot_content
            .as_ref()
            .ok_or_else(|| LixError {
                message: format!(
                    "generate_commit: commit row for version '{}' is missing snapshot_content",
                    version_id
                ),
            })?;
        let mut snapshot: serde_json::Value =
            serde_json::from_str(raw_snapshot).map_err(|error| LixError {
                message: format!(
                    "generate_commit: commit snapshot for version '{}' is invalid JSON: {}",
                    version_id, error
                ),
            })?;

        let domain_change_ids: Vec<String> = domain_by_version
            .get(version_id)
            .into_iter()
            .flat_map(|changes| changes.iter().map(|change| change.id.clone()))
            .collect();

        snapshot["change_ids"] = serde_json::Value::Array(
            domain_change_ids
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
        snapshot["meta_change_ids"] =
            serde_json::Value::Array(match tip_change_id_by_version.get(version_id) {
                Some(change_id) => vec![serde_json::Value::String(change_id.clone())],
                None => Vec::new(),
            });
        snapshot["parent_commit_ids"] = serde_json::Value::Array(
            meta.parent_commit_ids
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect(),
        );

        commit_row.snapshot_content = Some(snapshot.to_string());
    }

    // Materialize commit rows and version_tip rows so commit views can resolve immediately.
    for (version_id, meta) in &meta_by_version {
        materialized_state.push(MaterializedStateRow {
            id: generate_uuid(),
            entity_id: meta.commit_id.clone(),
            schema_key: COMMIT_SCHEMA_KEY.to_string(),
            schema_version: commit_schema.schema_version.clone(),
            file_id: commit_schema.file_id.clone(),
            plugin_key: commit_schema.plugin_key.clone(),
            snapshot_content: Some(
                json!({
                    "id": meta.commit_id,
                    "change_set_id": meta.change_set_id,
                })
                .to_string(),
            ),
            metadata: None,
            created_at: args.timestamp.clone(),
            lixcol_version_id: GLOBAL_VERSION.to_string(),
            lixcol_commit_id: meta.commit_id.clone(),
            writer_key: None,
        });

        let version_info = args.versions.get(version_id).ok_or_else(|| LixError {
            message: format!(
                "generate_commit: missing version context for '{}'",
                version_id
            ),
        })?;
        let tip_id = tip_change_id_by_version
            .get(version_id)
            .cloned()
            .unwrap_or_else(|| generate_uuid());
        materialized_state.push(MaterializedStateRow {
            id: tip_id,
            entity_id: version_id.clone(),
            schema_key: VERSION_TIP_SCHEMA_KEY.to_string(),
            schema_version: version_tip_schema.schema_version.clone(),
            file_id: version_tip_schema.file_id.clone(),
            plugin_key: version_tip_schema.plugin_key.clone(),
            snapshot_content: Some(
                json!({
                    "id": version_id,
                    "commit_id": meta.commit_id,
                    "working_commit_id": version_info.snapshot.working_commit_id,
                })
                .to_string(),
            ),
            metadata: None,
            created_at: args.timestamp.clone(),
            lixcol_version_id: GLOBAL_VERSION.to_string(),
            lixcol_commit_id: meta.commit_id.clone(),
            writer_key: None,
        });
    }

    // Materialize commit edges for commit graph topology.
    for meta in meta_by_version.values() {
        let edge_commit_id = global_commit_id
            .clone()
            .unwrap_or_else(|| meta.commit_id.clone());
        for parent_id in &meta.parent_commit_ids {
            materialized_state.push(MaterializedStateRow {
                id: generate_uuid(),
                entity_id: format!("{}~{}", parent_id, meta.commit_id),
                schema_key: COMMIT_EDGE_SCHEMA_KEY.to_string(),
                schema_version: commit_edge_schema.schema_version.clone(),
                file_id: commit_edge_schema.file_id.clone(),
                plugin_key: commit_edge_schema.plugin_key.clone(),
                snapshot_content: Some(
                    json!({
                        "parent_id": parent_id,
                        "child_id": meta.commit_id,
                    })
                    .to_string(),
                ),
                metadata: None,
                created_at: args.timestamp.clone(),
                lixcol_version_id: GLOBAL_VERSION.to_string(),
                lixcol_commit_id: edge_commit_id.clone(),
                writer_key: None,
            });
        }
    }

    output_changes.extend(meta_changes);

    Ok(GenerateCommitResult {
        changes: output_changes,
        materialized_state,
    })
}

fn sanitize_domain_change(change: &DomainChangeInput) -> ChangeRow {
    ChangeRow {
        id: change.id.clone(),
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        schema_version: change.schema_version.clone(),
        file_id: change.file_id.clone(),
        plugin_key: change.plugin_key.clone(),
        snapshot_content: change.snapshot_content.clone(),
        metadata: change.metadata.clone(),
        created_at: change.created_at.clone(),
    }
}

fn builtin_schema_meta(schema_key: &str) -> Result<BuiltinSchemaMeta, LixError> {
    let schema = builtin_schema_definition(schema_key).ok_or_else(|| LixError {
        message: format!("generate_commit: builtin schema '{}' not found", schema_key),
    })?;
    let schema_version = schema
        .get("x-lix-version")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| LixError {
            message: format!(
                "generate_commit: builtin schema '{}' is missing string x-lix-version",
                schema_key
            ),
        })?
        .to_string();
    let overrides = schema
        .get("x-lix-override-lixcols")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| LixError {
            message: format!(
                "generate_commit: builtin schema '{}' is missing x-lix-override-lixcols",
                schema_key
            ),
        })?;
    let file_id = overrides
        .get("lixcol_file_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| LixError {
            message: format!(
                "generate_commit: builtin schema '{}' is missing string lixcol_file_id",
                schema_key
            ),
        })?;
    let plugin_key = overrides
        .get("lixcol_plugin_key")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| LixError {
            message: format!(
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

fn decode_lixcol_literal(raw: &str) -> String {
    serde_json::from_str::<String>(raw).unwrap_or_else(|_| raw.trim_matches('\"').to_string())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::types::{VersionInfo, VersionSnapshot};

    fn domain_change(
        id: &str,
        entity_id: &str,
        schema_key: &str,
        version_id: &str,
        writer_key: Option<&str>,
    ) -> DomainChangeInput {
        DomainChangeInput {
            id: id.to_string(),
            entity_id: entity_id.to_string(),
            schema_key: schema_key.to_string(),
            schema_version: "1".to_string(),
            file_id: "lix".to_string(),
            plugin_key: "lix".to_string(),
            snapshot_content: Some(format!(r#"{{"id":"{id}"}}"#)),
            metadata: None,
            created_at: "2025-01-01T00:00:00.000Z".to_string(),
            version_id: version_id.to_string(),
            writer_key: writer_key.map(ToString::to_string),
        }
    }

    fn version_info(id: &str, working_commit_id: &str, parent_commit_ids: &[&str]) -> VersionInfo {
        VersionInfo {
            parent_commit_ids: parent_commit_ids.iter().map(ToString::to_string).collect(),
            snapshot: VersionSnapshot {
                id: id.to_string(),
                working_commit_id: working_commit_id.to_string(),
            },
        }
    }

    fn counts_by_schema(rows: &[MaterializedStateRow]) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for row in rows {
            *counts.entry(row.schema_key.clone()).or_insert(0) += 1;
        }
        counts
    }

    #[test]
    fn generates_commit_for_single_active_version_change() {
        let mut versions = BTreeMap::new();
        versions.insert(
            "version-main".to_string(),
            version_info("version-main", "work-main", &["P_active"]),
        );
        versions.insert(
            "global".to_string(),
            version_info("global", "work-global", &["P_global"]),
        );

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec!["acct-1".to_string()],
            changes: vec![domain_change(
                "chg_active",
                "kv_active",
                "lix_key_value",
                "version-main",
                Some("writer:test"),
            )],
            versions,
        };

        let mut n = 0u64;
        let result = generate_commit(args, || {
            let id = format!("uuid-{n}");
            n += 1;
            id
        })
        .expect("generate_commit should succeed");

        assert_eq!(result.changes.len(), 3);
        assert_eq!(
            result
                .changes
                .iter()
                .filter(|row| row.schema_key == "lix_commit")
                .count(),
            1
        );
        assert_eq!(
            result
                .changes
                .iter()
                .filter(|row| row.schema_key == "lix_version_tip")
                .count(),
            1
        );

        let commit_row = result
            .changes
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
            commit_snapshot["meta_change_ids"].as_array().unwrap().len(),
            1
        );
        assert_eq!(
            commit_snapshot["author_account_ids"],
            serde_json::json!(["acct-1"])
        );

        let materialized_counts = counts_by_schema(&result.materialized_state);
        assert_eq!(materialized_counts.get("lix_key_value"), Some(&1));
        assert_eq!(materialized_counts.get("lix_change_author"), Some(&1));
        assert_eq!(materialized_counts.get("lix_change_set_element"), Some(&1));
        assert_eq!(materialized_counts.get("lix_commit"), Some(&1));
        assert_eq!(materialized_counts.get("lix_version_tip"), Some(&1));
        assert_eq!(materialized_counts.get("lix_commit_edge"), Some(&1));
        assert_eq!(result.materialized_state.len(), 6);

        let domain_materialized = result
            .materialized_state
            .iter()
            .find(|row| row.schema_key == "lix_key_value")
            .expect("expected domain materialized row");
        assert_eq!(
            domain_materialized.writer_key.as_deref(),
            Some("writer:test")
        );
    }

    #[test]
    fn generates_commit_for_global_change() {
        let mut versions = BTreeMap::new();
        versions.insert(
            "global".to_string(),
            version_info("global", "work-global", &["P_global"]),
        );

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec!["acct-1".to_string()],
            changes: vec![domain_change(
                "chg_global",
                "kv_global",
                "lix_key_value",
                "global",
                None,
            )],
            versions,
        };

        let mut n = 0u64;
        let result = generate_commit(args, || {
            let id = format!("uuid-{n}");
            n += 1;
            id
        })
        .expect("generate_commit should succeed");

        assert_eq!(
            result
                .changes
                .iter()
                .filter(|row| row.schema_key == "lix_commit")
                .count(),
            1
        );
        assert_eq!(
            result
                .materialized_state
                .iter()
                .filter(|row| row.schema_key == "lix_change_author")
                .count(),
            1
        );
        assert_eq!(
            result
                .materialized_state
                .iter()
                .filter(|row| row.schema_key == "lix_change_set_element")
                .count(),
            1
        );
        assert_eq!(
            result
                .materialized_state
                .iter()
                .filter(|row| row.schema_key == "lix_commit_edge")
                .count(),
            1
        );
        assert_eq!(result.materialized_state.len(), 6);

        let commit_row = result
            .changes
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
            commit_snapshot["meta_change_ids"].as_array().unwrap().len(),
            1
        );

        let author_row = result
            .materialized_state
            .iter()
            .find(|row| row.schema_key == "lix_change_author")
            .expect("expected change_author row");
        assert_eq!(author_row.entity_id, "chg_global~acct-1");
        let author_snapshot: serde_json::Value =
            serde_json::from_str(author_row.snapshot_content.as_ref().unwrap()).unwrap();
        assert_eq!(
            author_snapshot,
            serde_json::json!({
                "change_id": "chg_global",
                "account_id": "acct-1",
            })
        );
    }

    #[test]
    fn generates_commits_for_multiple_versions() {
        let mut versions = BTreeMap::new();
        versions.insert(
            "global".to_string(),
            version_info("global", "work-global", &["P_global"]),
        );
        versions.insert(
            "version-main".to_string(),
            version_info("version-main", "work-main", &["P_main"]),
        );

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec!["acct-1".to_string(), "acct-2".to_string()],
            changes: vec![
                domain_change("chg_global", "kv_global", "lix_key_value", "global", None),
                domain_change("chg_main", "kv_main", "lix_key_value", "version-main", None),
            ],
            versions,
        };

        let mut n = 0u64;
        let result = generate_commit(args, || {
            let id = format!("uuid-{n}");
            n += 1;
            id
        })
        .expect("generate_commit should succeed");

        assert_eq!(result.changes.len(), 6);
        assert_eq!(
            result
                .changes
                .iter()
                .filter(|row| row.schema_key == "lix_commit")
                .count(),
            2
        );
        assert_eq!(
            result
                .changes
                .iter()
                .filter(|row| row.schema_key == "lix_version_tip")
                .count(),
            2
        );

        assert_eq!(
            result
                .materialized_state
                .iter()
                .filter(|row| row.schema_key == "lix_change_author")
                .count(),
            4
        );
        assert_eq!(result.materialized_state.len(), 14);

        let commit_rows: Vec<_> = result
            .changes
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
            assert_eq!(
                commit_snapshot["meta_change_ids"].as_array().unwrap().len(),
                1
            );
        }

        let change_author_entities: BTreeSet<String> = result
            .materialized_state
            .iter()
            .filter(|row| row.schema_key == "lix_change_author")
            .map(|row| row.entity_id.clone())
            .collect();
        assert_eq!(
            change_author_entities,
            BTreeSet::from([
                "chg_global~acct-1".to_string(),
                "chg_global~acct-2".to_string(),
                "chg_main~acct-1".to_string(),
                "chg_main~acct-2".to_string(),
            ])
        );

        let global_tip = result
            .materialized_state
            .iter()
            .find(|row| row.schema_key == "lix_version_tip" && row.entity_id == "global")
            .expect("global version_tip should exist");
        let global_tip_snapshot: serde_json::Value =
            serde_json::from_str(global_tip.snapshot_content.as_ref().unwrap()).unwrap();
        let global_commit_id = global_tip_snapshot["commit_id"]
            .as_str()
            .expect("commit_id should be string")
            .to_string();

        for cse in result
            .materialized_state
            .iter()
            .filter(|row| row.schema_key == "lix_change_set_element")
        {
            assert_eq!(cse.lixcol_version_id, "global");
            assert_eq!(cse.lixcol_commit_id, global_commit_id);
        }
    }

    #[test]
    fn rejects_duplicate_domain_change_ids() {
        let mut versions = BTreeMap::new();
        versions.insert(
            "global".to_string(),
            version_info("global", "work-global", &["P_global"]),
        );

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec![],
            changes: vec![
                domain_change("dup", "entity-a", "lix_key_value", "global", None),
                domain_change("dup", "entity-b", "lix_key_value", "global", None),
            ],
            versions,
        };

        let error =
            generate_commit(args, || "id".to_string()).expect_err("expected duplicate id error");
        assert!(
            error.message.contains("duplicate change id"),
            "unexpected error: {}",
            error.message
        );
    }

    #[test]
    fn rejects_missing_version_context_for_domain_change() {
        let mut versions = BTreeMap::new();
        versions.insert(
            "global".to_string(),
            version_info("global", "work-global", &["P_global"]),
        );

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec![],
            changes: vec![domain_change(
                "chg-missing",
                "entity-a",
                "lix_key_value",
                "version-main",
                None,
            )],
            versions,
        };

        let error = generate_commit(args, || "id".to_string())
            .expect_err("expected missing version context error");
        assert!(
            error.message.contains("missing version context"),
            "unexpected error: {}",
            error.message
        );
    }

    #[test]
    fn writer_key_is_propagated_only_to_domain_materialized_rows() {
        let mut versions = BTreeMap::new();
        versions.insert(
            "global".to_string(),
            version_info("global", "work-global", &["P_global"]),
        );
        versions.insert(
            "version-main".to_string(),
            version_info("version-main", "work-main", &["P_main"]),
        );

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec!["acct-1".to_string()],
            changes: vec![domain_change(
                "chg_writer",
                "entity_writer",
                "mock_schema",
                "version-main",
                Some("writer:test"),
            )],
            versions,
        };

        let mut n = 0u64;
        let result = generate_commit(args, || {
            let id = format!("uuid-{n}");
            n += 1;
            id
        })
        .expect("generate_commit should succeed");

        let domain_row = result
            .materialized_state
            .iter()
            .find(|row| row.schema_key == "mock_schema")
            .expect("expected materialized domain row");
        assert_eq!(domain_row.writer_key.as_deref(), Some("writer:test"));

        for row in result
            .materialized_state
            .iter()
            .filter(|row| row.schema_key != "mock_schema")
        {
            assert!(
                row.writer_key.is_none(),
                "meta row '{}' should have writer_key = None",
                row.schema_key
            );
        }
    }
}
