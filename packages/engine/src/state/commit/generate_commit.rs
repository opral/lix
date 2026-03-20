use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;

use crate::schema::builtin::{builtin_schema_definition, decode_lixcol_literal};
use crate::state::commit::types::{
    CanonicalCommitOutput, ChangeRow, DerivedCommitApplyInput, DomainChangeInput,
    GenerateCommitArgs, GenerateCommitResult, MaterializedStateRow,
};
use crate::{CanonicalJson, LixError};

const GLOBAL_VERSION: &str = "global";
const COMMIT_SCHEMA_KEY: &str = "lix_commit";
const CHANGE_SET_SCHEMA_KEY: &str = "lix_change_set";
const VERSION_REF_SCHEMA_KEY: &str = "lix_version_ref";
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

    // Validate duplicate domain change ids.
    let mut seen_ids = BTreeSet::new();
    for change in &args.changes {
        if !seen_ids.insert(change.id.clone()) {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("generate_commit: duplicate change id '{}'", change.id),
            });
        }
        validate_domain_change_identity(change)?;
    }

    let commit_schema = builtin_schema_meta(COMMIT_SCHEMA_KEY)?;
    let change_set_schema = builtin_schema_meta(CHANGE_SET_SCHEMA_KEY)?;
    let version_ref_schema = builtin_schema_meta(VERSION_REF_SCHEMA_KEY)?;
    let change_set_element_schema = builtin_schema_meta(CHANGE_SET_ELEMENT_SCHEMA_KEY)?;
    let commit_edge_schema = builtin_schema_meta(COMMIT_EDGE_SCHEMA_KEY)?;
    let change_author_schema = builtin_schema_meta(CHANGE_AUTHOR_SCHEMA_KEY)?;

    let effective_domain_changes = collapse_domain_changes_last_wins(&args.changes);
    let mut output_changes: Vec<ChangeRow> = effective_domain_changes
        .iter()
        .map(|change| sanitize_domain_change(change))
        .collect();
    let mut live_state_rows: Vec<MaterializedStateRow> = Vec::new();

    let mut domain_by_version: BTreeMap<String, Vec<&DomainChangeInput>> = BTreeMap::new();
    for change in &effective_domain_changes {
        domain_by_version
            .entry(change.version_id.to_string())
            .or_default()
            .push(*change);
    }

    let versions_to_commit: BTreeSet<String> = domain_by_version.keys().cloned().collect();
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
    let mut meta_changes: Vec<ChangeRow> = Vec::new();
    let mut change_set_change_id_by_version: BTreeMap<String, String> = BTreeMap::new();
    let mut commit_change_id_by_version: BTreeMap<String, String> = BTreeMap::new();
    let mut commit_row_index_by_version: BTreeMap<String, usize> = BTreeMap::new();

    for (version_id, meta) in &meta_by_version {
        let change_set_change_id = generate_uuid();
        change_set_change_id_by_version.insert(version_id.clone(), change_set_change_id.clone());
        meta_changes.push(ChangeRow {
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
        commit_change_id_by_version.insert(version_id.clone(), commit_change_id.clone());
        let commit_row_idx = meta_changes.len();
        commit_row_index_by_version.insert(version_id.clone(), commit_row_idx);
        meta_changes.push(ChangeRow {
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

    // Materialize domain rows and derived change_set_element rows.
    let global_commit_id = meta_by_version
        .get(GLOBAL_VERSION)
        .map(|meta| meta.commit_id.clone());
    for (version_id, domain_changes) in &domain_by_version {
        let meta = meta_by_version.get(version_id).ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("generate_commit: missing version meta for '{}'", version_id),
        })?;
        let cse_commit_id = global_commit_id
            .clone()
            .unwrap_or_else(|| meta.commit_id.clone());

        for change in domain_changes {
            live_state_rows.push(MaterializedStateRow {
                id: change.id.clone(),
                entity_id: change.entity_id.clone(),
                schema_key: change.schema_key.clone(),
                schema_version: change.schema_version.clone(),
                file_id: change.file_id.clone(),
                plugin_key: change.plugin_key.clone(),
                snapshot_content: change.snapshot_content.clone(),
                metadata: change.metadata.clone(),
                created_at: change.created_at.clone(),
                lixcol_version_id: expect_identity(
                    version_id.clone(),
                    "materialized_state lixcol_version_id",
                ),
                lixcol_commit_id: meta.commit_id.clone(),
                writer_key: change.writer_key.clone(),
            });

            live_state_rows.push(MaterializedStateRow {
                id: generate_uuid(),
                entity_id: expect_identity(
                    format!("{}~{}", meta.change_set_id, change.id),
                    "change_set_element entity_id",
                ),
                schema_key: expect_identity(
                    CHANGE_SET_ELEMENT_SCHEMA_KEY.to_string(),
                    "change_set_element schema_key",
                ),
                schema_version: expect_identity(
                    change_set_element_schema.schema_version.clone(),
                    "change_set_element schema_version",
                ),
                file_id: expect_identity(
                    change_set_element_schema.file_id.clone(),
                    "change_set_element file_id",
                ),
                plugin_key: expect_identity(
                    change_set_element_schema.plugin_key.clone(),
                    "change_set_element plugin_key",
                ),
                snapshot_content: Some(canonical_json(json!({
                    "change_set_id": meta.change_set_id,
                    "change_id": change.id,
                    "entity_id": change.entity_id,
                    "schema_key": change.schema_key,
                    "file_id": change.file_id,
                }))?),
                metadata: None,
                created_at: args.timestamp.clone(),
                lixcol_version_id: expect_identity(GLOBAL_VERSION.to_string(), "global version id"),
                lixcol_commit_id: cse_commit_id.clone(),
                writer_key: None,
            });
        }
    }

    // Materialize derived per-change authors in global scope.
    for (version_id, domain_changes) in &domain_by_version {
        let meta = meta_by_version.get(version_id).ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("generate_commit: missing version meta for '{}'", version_id),
        })?;
        let commit_change_id =
            commit_change_id_by_version
                .get(version_id)
                .ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "generate_commit: missing commit change id for version '{}'",
                        version_id
                    ),
                })?;

        for change in domain_changes {
            for account_id in &unique_active_accounts {
                live_state_rows.push(MaterializedStateRow {
                    id: commit_change_id.clone(),
                    entity_id: expect_identity(
                        format!("{}~{}", change.id, account_id),
                        "change_author entity_id",
                    ),
                    schema_key: expect_identity(
                        CHANGE_AUTHOR_SCHEMA_KEY.to_string(),
                        "change_author schema_key",
                    ),
                    schema_version: expect_identity(
                        change_author_schema.schema_version.clone(),
                        "change_author schema_version",
                    ),
                    file_id: expect_identity(
                        change_author_schema.file_id.clone(),
                        "change_author file_id",
                    ),
                    plugin_key: expect_identity(
                        change_author_schema.plugin_key.clone(),
                        "change_author plugin_key",
                    ),
                    snapshot_content: Some(canonical_json(json!({
                        "change_id": change.id,
                        "account_id": account_id,
                    }))?),
                    metadata: None,
                    created_at: args.timestamp.clone(),
                    lixcol_version_id: expect_identity(
                        GLOBAL_VERSION.to_string(),
                        "global version id",
                    ),
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

        let member_change_ids: Vec<String> = domain_by_version
            .get(version_id)
            .into_iter()
            .flat_map(|changes| changes.iter().map(|change| change.id.clone()))
            .collect();

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

    let mut commit_snapshot_by_version: BTreeMap<String, CanonicalJson> = BTreeMap::new();
    for version_id in meta_by_version.keys() {
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
        let commit_row = meta_changes.get(commit_row_idx).ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "generate_commit: missing commit row for version '{}'",
                version_id
            ),
        })?;
        let commit_snapshot = commit_row
            .snapshot_content
            .as_ref()
            .ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "generate_commit: commit row for version '{}' is missing snapshot_content",
                    version_id
                ),
            })?
            .clone();
        commit_snapshot_by_version.insert(version_id.clone(), commit_snapshot);
    }

    // Materialize commit rows and version_ref rows so commit views can resolve immediately.
    for (version_id, meta) in &meta_by_version {
        let change_set_change_id = change_set_change_id_by_version
            .get(version_id)
            .cloned()
            .unwrap_or_else(|| generate_uuid());
        live_state_rows.push(MaterializedStateRow {
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
            lixcol_version_id: expect_identity(GLOBAL_VERSION.to_string(), "global version id"),
            lixcol_commit_id: meta.commit_id.clone(),
            writer_key: None,
        });

        let commit_snapshot = commit_snapshot_by_version
            .get(version_id)
            .cloned()
            .ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "generate_commit: missing finalized commit snapshot for version '{}'",
                    version_id
                ),
            })?;
        live_state_rows.push(MaterializedStateRow {
            id: generate_uuid(),
            entity_id: expect_identity(meta.commit_id.clone(), "commit entity_id"),
            schema_key: expect_identity(COMMIT_SCHEMA_KEY.to_string(), "commit schema_key"),
            schema_version: expect_identity(
                commit_schema.schema_version.clone(),
                "commit schema_version",
            ),
            file_id: expect_identity(commit_schema.file_id.clone(), "commit file_id"),
            plugin_key: expect_identity(commit_schema.plugin_key.clone(), "commit plugin_key"),
            snapshot_content: Some(commit_snapshot),
            metadata: None,
            created_at: args.timestamp.clone(),
            lixcol_version_id: expect_identity(GLOBAL_VERSION.to_string(), "global version id"),
            lixcol_commit_id: meta.commit_id.clone(),
            writer_key: None,
        });

        live_state_rows.push(MaterializedStateRow {
            id: generate_uuid(),
            entity_id: expect_identity(version_id.clone(), "version_ref entity_id"),
            schema_key: expect_identity(
                VERSION_REF_SCHEMA_KEY.to_string(),
                "version_ref schema_key",
            ),
            schema_version: expect_identity(
                version_ref_schema.schema_version.clone(),
                "version_ref schema_version",
            ),
            file_id: expect_identity(version_ref_schema.file_id.clone(), "version_ref file_id"),
            plugin_key: expect_identity(
                version_ref_schema.plugin_key.clone(),
                "version_ref plugin_key",
            ),
            snapshot_content: Some(canonical_json(json!({
                "id": version_id,
                "commit_id": meta.commit_id,
            }))?),
            metadata: None,
            created_at: args.timestamp.clone(),
            lixcol_version_id: expect_identity(GLOBAL_VERSION.to_string(), "global version id"),
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
            live_state_rows.push(MaterializedStateRow {
                id: generate_uuid(),
                entity_id: expect_identity(
                    format!("{}~{}", parent_id, meta.commit_id),
                    "commit_edge entity_id",
                ),
                schema_key: expect_identity(
                    COMMIT_EDGE_SCHEMA_KEY.to_string(),
                    "commit_edge schema_key",
                ),
                schema_version: expect_identity(
                    commit_edge_schema.schema_version.clone(),
                    "commit_edge schema_version",
                ),
                file_id: expect_identity(commit_edge_schema.file_id.clone(), "commit_edge file_id"),
                plugin_key: expect_identity(
                    commit_edge_schema.plugin_key.clone(),
                    "commit_edge plugin_key",
                ),
                snapshot_content: Some(canonical_json(json!({
                    "parent_id": parent_id,
                    "child_id": meta.commit_id,
                }))?),
                metadata: None,
                created_at: args.timestamp.clone(),
                lixcol_version_id: expect_identity(GLOBAL_VERSION.to_string(), "global version id"),
                lixcol_commit_id: edge_commit_id.clone(),
                writer_key: None,
            });
        }
    }

    output_changes.extend(meta_changes);

    Ok(GenerateCommitResult {
        canonical_output: CanonicalCommitOutput {
            changes: output_changes,
        },
        derived_apply_input: DerivedCommitApplyInput {
            live_state_rows,
            live_layouts: BTreeMap::new(),
        },
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

fn validate_domain_change_identity(change: &DomainChangeInput) -> Result<(), LixError> {
    let change_label = if change.id.is_empty() {
        "<empty change id>"
    } else {
        change.id.as_str()
    };

    for (field, value) in [
        ("id", change.id.as_str()),
        ("entity_id", change.entity_id.as_str()),
        ("schema_key", change.schema_key.as_str()),
        ("schema_version", change.schema_version.as_str()),
        ("file_id", change.file_id.as_str()),
        ("plugin_key", change.plugin_key.as_str()),
        ("version_id", change.version_id.as_str()),
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

fn collapse_domain_changes_last_wins(changes: &[DomainChangeInput]) -> Vec<&DomainChangeInput> {
    let mut latest_index_by_key: BTreeMap<(String, String, String, String), usize> =
        BTreeMap::new();
    for (index, change) in changes.iter().enumerate() {
        latest_index_by_key.insert(
            (
                change.version_id.to_string(),
                change.entity_id.to_string(),
                change.schema_key.to_string(),
                change.file_id.to_string(),
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
    use crate::state::commit::types::{VersionInfo, VersionSnapshot};

    fn domain_change(
        id: &str,
        entity_id: &str,
        schema_key: &str,
        version_id: &str,
        writer_key: Option<&str>,
    ) -> DomainChangeInput {
        DomainChangeInput {
            id: id.to_string(),
            entity_id: entity_id.try_into().unwrap(),
            schema_key: schema_key.try_into().unwrap(),
            schema_version: "1".try_into().unwrap(),
            file_id: "lix".try_into().unwrap(),
            plugin_key: "lix".try_into().unwrap(),
            snapshot_content: Some(
                CanonicalJson::from_text(format!(r#"{{"id":"{id}"}}"#))
                    .expect("test snapshot should be valid canonical json"),
            ),
            metadata: None,
            created_at: "2025-01-01T00:00:00.000Z".to_string(),
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

    fn counts_by_schema(rows: &[MaterializedStateRow]) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for row in rows {
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

        assert_eq!(result.canonical_output.changes.len(), 3);
        assert_eq!(
            result
                .canonical_output
                .changes
                .iter()
                .filter(|row| row.schema_key == "lix_commit")
                .count(),
            1
        );
        assert_eq!(
            result
                .canonical_output
                .changes
                .iter()
                .filter(|row| row.schema_key == "lix_change_set")
                .count(),
            1
        );
        let commit_row = result
            .canonical_output
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
            commit_snapshot["author_account_ids"],
            serde_json::json!(["acct-1"])
        );

        let materialized_commit_row = result
            .derived_apply_input
            .live_state_rows
            .iter()
            .find(|row| row.schema_key == "lix_commit")
            .expect("expected materialized commit row");
        let materialized_commit_snapshot: serde_json::Value =
            serde_json::from_str(materialized_commit_row.snapshot_content.as_ref().unwrap())
                .unwrap();
        assert_eq!(materialized_commit_snapshot, commit_snapshot);

        let materialized_counts = counts_by_schema(&result.derived_apply_input.live_state_rows);
        assert_eq!(materialized_counts.get("lix_key_value"), Some(&1));
        assert_eq!(materialized_counts.get("lix_change_author"), Some(&1));
        assert_eq!(materialized_counts.get("lix_change_set_element"), Some(&1));
        assert_eq!(materialized_counts.get("lix_change_set"), Some(&1));
        assert_eq!(materialized_counts.get("lix_commit"), Some(&1));
        assert_eq!(materialized_counts.get("lix_version_ref"), Some(&1));
        assert_eq!(materialized_counts.get("lix_commit_edge"), Some(&1));
        assert_eq!(result.derived_apply_input.live_state_rows.len(), 7);

        let domain_materialized = result
            .derived_apply_input
            .live_state_rows
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
        versions.insert("global".to_string(), version_info("global", &["P_global"]));

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
                .canonical_output
                .changes
                .iter()
                .filter(|row| row.schema_key == "lix_commit")
                .count(),
            1
        );
        assert_eq!(
            result
                .derived_apply_input
                .live_state_rows
                .iter()
                .filter(|row| row.schema_key == "lix_change_author")
                .count(),
            1
        );
        assert_eq!(
            result
                .derived_apply_input
                .live_state_rows
                .iter()
                .filter(|row| row.schema_key == "lix_change_set_element")
                .count(),
            1
        );
        assert_eq!(
            result
                .derived_apply_input
                .live_state_rows
                .iter()
                .filter(|row| row.schema_key == "lix_commit_edge")
                .count(),
            1
        );
        assert_eq!(
            result
                .derived_apply_input
                .live_state_rows
                .iter()
                .filter(|row| row.schema_key == "lix_change_set")
                .count(),
            1
        );
        assert_eq!(
            result
                .derived_apply_input
                .live_state_rows
                .iter()
                .filter(|row| row.schema_key == "lix_version_ref")
                .count(),
            1
        );
        assert_eq!(result.derived_apply_input.live_state_rows.len(), 7);

        let commit_row = result
            .canonical_output
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
            commit_snapshot["change_ids"],
            serde_json::json!(["chg_global"])
        );

        let author_row = result
            .derived_apply_input
            .live_state_rows
            .iter()
            .find(|row| {
                row.schema_key == "lix_change_author" && row.entity_id == "chg_global~acct-1"
            })
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
        versions.insert("global".to_string(), version_info("global", &["P_global"]));
        versions.insert(
            "version-main".to_string(),
            version_info("version-main", &["P_main"]),
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

        assert_eq!(result.canonical_output.changes.len(), 6);
        assert_eq!(
            result
                .canonical_output
                .changes
                .iter()
                .filter(|row| row.schema_key == "lix_commit")
                .count(),
            2
        );
        assert_eq!(
            result
                .canonical_output
                .changes
                .iter()
                .filter(|row| row.schema_key == "lix_change_set")
                .count(),
            2
        );
        assert_eq!(
            result
                .derived_apply_input
                .live_state_rows
                .iter()
                .filter(|row| row.schema_key == "lix_change_author")
                .count(),
            4
        );
        assert_eq!(
            result
                .derived_apply_input
                .live_state_rows
                .iter()
                .filter(|row| row.schema_key == "lix_change_set")
                .count(),
            2
        );
        assert_eq!(
            result
                .derived_apply_input
                .live_state_rows
                .iter()
                .filter(|row| row.schema_key == "lix_change_set_element")
                .count(),
            2
        );
        assert_eq!(result.derived_apply_input.live_state_rows.len(), 16);

        let commit_rows: Vec<_> = result
            .canonical_output
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
            assert_eq!(commit_snapshot["change_ids"].as_array().unwrap().len(), 1);
        }

        let change_author_entities: BTreeSet<String> = result
            .derived_apply_input
            .live_state_rows
            .iter()
            .filter(|row| row.schema_key == "lix_change_author")
            .map(|row| row.entity_id.to_string())
            .collect();
        for entity in [
            "chg_global~acct-1",
            "chg_global~acct-2",
            "chg_main~acct-1",
            "chg_main~acct-2",
        ] {
            assert!(change_author_entities.contains(entity));
        }
        assert_eq!(
            result
                .derived_apply_input
                .live_state_rows
                .iter()
                .filter(|row| row.schema_key == "lix_change_author")
                .count(),
            4
        );

        let global_tip = result
            .derived_apply_input
            .live_state_rows
            .iter()
            .find(|row| row.schema_key == "lix_version_ref" && row.entity_id == "global")
            .expect("global version_ref should exist");
        let global_tip_snapshot: serde_json::Value =
            serde_json::from_str(global_tip.snapshot_content.as_ref().unwrap()).unwrap();
        let global_commit_id = global_tip_snapshot["commit_id"]
            .as_str()
            .expect("commit_id should be string")
            .to_string();

        for cse in result
            .derived_apply_input
            .live_state_rows
            .iter()
            .filter(|row| row.schema_key == "lix_change_set_element")
        {
            assert_eq!(cse.lixcol_version_id, "global");
            assert_eq!(cse.lixcol_commit_id, global_commit_id);
        }
    }

    #[test]
    fn collapses_domain_changes_per_entity_schema_file_with_last_wins() {
        let mut versions = BTreeMap::new();
        versions.insert("global".to_string(), version_info("global", &["P_global"]));

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec!["acct-1".to_string()],
            changes: vec![
                domain_change("chg_a1", "entity-a", "lix_key_value", "global", None),
                domain_change("chg_b1", "entity-b", "lix_key_value", "global", None),
                domain_change("chg_a2", "entity-a", "lix_key_value", "global", None),
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

        let domain_change_ids = result
            .canonical_output
            .changes
            .iter()
            .filter(|row| row.schema_key == "lix_key_value")
            .map(|row| row.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(domain_change_ids, vec!["chg_b1", "chg_a2"]);
        let commit_row = result
            .canonical_output
            .changes
            .iter()
            .find(|row| row.schema_key == "lix_commit")
            .expect("expected commit row");
        let commit_snapshot: serde_json::Value =
            serde_json::from_str(commit_row.snapshot_content.as_ref().unwrap()).unwrap();
        assert_eq!(
            commit_snapshot["change_ids"],
            serde_json::json!(["chg_b1", "chg_a2"])
        );

        let cse_change_ids = result
            .derived_apply_input
            .live_state_rows
            .iter()
            .filter(|row| row.schema_key == "lix_change_set_element")
            .map(|row| {
                let snapshot: serde_json::Value =
                    serde_json::from_str(row.snapshot_content.as_ref().unwrap())
                        .expect("cse snapshot should be valid JSON");
                snapshot["change_id"]
                    .as_str()
                    .expect("cse change_id should be string")
                    .to_string()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            cse_change_ids,
            BTreeSet::from(["chg_b1".to_string(), "chg_a2".to_string()])
        );

        let change_author_entities = result
            .derived_apply_input
            .live_state_rows
            .iter()
            .filter(|row| row.schema_key == "lix_change_author")
            .map(|row| row.entity_id.to_string())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            change_author_entities,
            BTreeSet::from(["chg_b1~acct-1".to_string(), "chg_a2~acct-1".to_string()])
        );
    }

    #[test]
    fn rejects_duplicate_domain_change_ids() {
        let mut versions = BTreeMap::new();
        versions.insert("global".to_string(), version_info("global", &["P_global"]));

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
            error.description.contains("duplicate change id"),
            "unexpected error: {}",
            error.description
        );
    }

    #[test]
    fn rejects_empty_domain_change_entity_id() {
        let mut versions = BTreeMap::new();
        versions.insert("global".to_string(), version_info("global", &["P_global"]));

        let args = GenerateCommitArgs {
            timestamp: "2025-01-01T00:00:00.000Z".to_string(),
            active_accounts: vec![],
            changes: vec![domain_change(
                "chg-empty-entity",
                "",
                "lix_key_value",
                "global",
                None,
            )],
            versions,
        };

        let error =
            generate_commit(args, || "id".to_string()).expect_err("expected empty entity_id error");
        assert!(
            error.description.contains("entity_id must be non-empty"),
            "unexpected error: {}",
            error.description
        );
    }

    #[test]
    fn rejects_missing_version_context_for_domain_change() {
        let mut versions = BTreeMap::new();
        versions.insert("global".to_string(), version_info("global", &["P_global"]));

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
            error.description.contains("missing version context"),
            "unexpected error: {}",
            error.description
        );
    }

    #[test]
    fn writer_key_is_propagated_only_to_domain_materialized_rows() {
        let mut versions = BTreeMap::new();
        versions.insert("global".to_string(), version_info("global", &["P_global"]));
        versions.insert(
            "version-main".to_string(),
            version_info("version-main", &["P_main"]),
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
            .derived_apply_input
            .live_state_rows
            .iter()
            .find(|row| row.schema_key == "mock_schema")
            .expect("expected materialized domain row");
        assert_eq!(domain_row.writer_key.as_deref(), Some("writer:test"));

        for row in result
            .derived_apply_input
            .live_state_rows
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
