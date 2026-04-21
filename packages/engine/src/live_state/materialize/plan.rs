use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;

use super::loader::{load_data_with_executor, LoadedData};
use super::types::{
    LatestVisibleWinnerDebugRow, LiveStateRebuildDebugMode, LiveStateRebuildDebugTrace,
    LiveStateRebuildPlan, LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning,
    LiveStateWrite, LiveStateWriteOp, StageStat, TraversedCommitDebugRow, TraversedEdgeDebugRow,
    VersionHeadDebugRow, VisibilityWinnerDebugRow,
};
use crate::canonical::{
    load_visible_state, CanonicalContentMode, CanonicalTombstoneMode, CanonicalVisibleStateFilter,
    CanonicalVisibleStateRequest, CanonicalVisibleStateRow,
};
use crate::live_state::store::{LiveStateBackendRef, LiveStateExecutorRef};
use crate::live_state::ReplayCursor;
use crate::schema::LixVersionRef;
use crate::schema::{builtin_schema_definition, builtin_schema_storage_defaults};
use crate::{CanonicalJson, LixError};

type VersionHeadMap = BTreeMap<String, Vec<String>>;

#[derive(Debug, Clone)]
struct VisibleRow {
    version_id: String,
    commit_id: String,
    replay_cursor: ReplayCursor,
    change_id: String,
    untracked: bool,
    entity_id: String,
    schema_key: String,
    schema_version: String,
    file_id: Option<String>,
    plugin_key: Option<String>,
    snapshot_content: Option<CanonicalJson>,
    metadata: Option<CanonicalJson>,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Clone)]
struct FinalStateRow {
    version_id: String,
    source: VisibleRow,
}

#[derive(Debug, Clone)]
struct ProjectionCandidate {
    depth: usize,
    row: VisibleRow,
}

#[derive(Debug, Clone)]
struct BuiltinProjectionSchemaMeta {
    schema_key: String,
    schema_version: String,
    file_id: Option<String>,
    plugin_key: Option<String>,
}

pub(crate) async fn live_state_rebuild_plan_internal(
    backend: LiveStateBackendRef<'_>,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    let mut executor = backend;
    live_state_rebuild_plan_with_executor(&mut executor, req).await
}

pub(crate) async fn live_state_rebuild_plan_with_executor(
    executor: LiveStateExecutorRef<'_>,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    let data = load_data_with_executor(executor).await?;
    let mut stats = Vec::new();
    let mut warnings = Vec::new();

    let mut visibility_projection_rows =
        build_untracked_visibility_projection_rows(&data, &mut warnings, &mut stats)?;
    visibility_projection_rows.sort_by(|a, b| {
        a.schema_key
            .cmp(&b.schema_key)
            .then_with(|| a.version_id.cmp(&b.version_id))
            .then_with(|| a.file_id.cmp(&b.file_id))
            .then_with(|| a.entity_id.cmp(&b.entity_id))
            .then_with(|| a.replay_cursor.cmp(&b.replay_cursor))
            .then_with(|| a.change_id.cmp(&b.change_id))
    });
    let version_refs =
        load_version_heads_from_canonical(&visibility_projection_rows, &mut warnings, &mut stats)?;
    let target_versions = resolve_target_versions(req, &version_refs, &data);
    let visible_state_rows =
        load_canonical_visible_state(executor, &version_refs, &target_versions, &mut stats).await?;

    let latest_visible_state = build_latest_visible_state(
        &data,
        &visible_state_rows,
        &version_refs,
        &visibility_projection_rows,
        &mut warnings,
        &mut stats,
    )?;

    let final_state = build_final_state(&latest_visible_state, &target_versions, &mut stats);
    let writes = build_writes(&final_state)?;

    let debug = build_debug_trace(
        req,
        &data,
        &version_refs,
        &visible_state_rows,
        &latest_visible_state,
        &final_state,
    )?;

    Ok(LiveStateRebuildPlan {
        run_id: format!("materialization::{:?}", req.scope),
        scope: resolved_scope(req, target_versions),
        stats,
        writes,
        warnings,
        debug,
    })
}

fn resolved_scope(
    req: &LiveStateRebuildRequest,
    target_versions: BTreeSet<String>,
) -> LiveStateRebuildScope {
    match &req.scope {
        LiveStateRebuildScope::Full => {
            let _ = target_versions;
            LiveStateRebuildScope::Full
        }
        LiveStateRebuildScope::Versions(_) => LiveStateRebuildScope::Versions(target_versions),
    }
}

fn build_latest_visible_state(
    data: &LoadedData,
    visible_state_rows: &[CanonicalVisibleStateRow],
    version_refs: &VersionHeadMap,
    visibility_projection_rows: &[VisibleRow],
    warnings: &mut Vec<LiveStateRebuildWarning>,
    stats: &mut Vec<StageStat>,
) -> Result<Vec<VisibleRow>, LixError> {
    let root_versions = root_versions_by_commit(version_refs);
    let mut winners = Vec::new();

    for row in visible_state_rows {
        if row.schema_key == "lix_version_descriptor" || row.schema_key == "lix_version_ref" {
            continue;
        }
        let Some(change) = data.changes.get(&row.source_change_id) else {
            warnings.push(LiveStateRebuildWarning {
                code: "missing_change".to_string(),
                message: format!(
                    "canonical visible state references missing change '{}'",
                    row.source_change_id
                ),
            });
            continue;
        };
        let Some(version_ids) = root_versions.get(&row.root_commit_id) else {
            warnings.push(LiveStateRebuildWarning {
                code: "missing_root_version".to_string(),
                message: format!(
                    "canonical visible state root '{}' does not map to a live version head",
                    row.root_commit_id
                ),
            });
            continue;
        };
        let snapshot_content = row
            .snapshot_content
            .as_ref()
            .map(|value| CanonicalJson::from_text(value.clone()))
            .transpose()?;
        let metadata = row
            .metadata
            .as_ref()
            .map(|value| CanonicalJson::from_text(value.clone()))
            .transpose()?;

        for version_id in version_ids {
            winners.push(VisibleRow {
                version_id: version_id.clone(),
                commit_id: row.source_commit_id.clone(),
                replay_cursor: change.replay_cursor.clone(),
                change_id: row.source_change_id.clone(),
                untracked: false,
                entity_id: row.entity_id.clone(),
                schema_key: row.schema_key.clone(),
                schema_version: row.schema_version.clone(),
                file_id: row.file_id.clone(),
                plugin_key: row.plugin_key.clone(),
                snapshot_content: snapshot_content.clone(),
                metadata: metadata.clone(),
                created_at: change.created_at.clone(),
                updated_at: change.created_at.clone(),
            });
        }
    }

    winners.extend(build_global_projection_rows(
        data,
        visible_state_rows,
        version_refs,
        warnings,
    ));
    winners.extend(visibility_projection_rows.iter().cloned());

    winners.sort_by(|a, b| {
        a.version_id
            .cmp(&b.version_id)
            .then_with(|| a.schema_key.cmp(&b.schema_key))
            .then_with(|| a.file_id.cmp(&b.file_id))
            .then_with(|| a.entity_id.cmp(&b.entity_id))
            .then_with(|| a.replay_cursor.cmp(&b.replay_cursor))
    });

    stats.push(StageStat {
        stage: "latest_visible_state".to_string(),
        input_rows: visible_state_rows.len() + visibility_projection_rows.len(),
        output_rows: winners.len(),
    });

    Ok(winners)
}

fn build_global_projection_rows(
    data: &LoadedData,
    visible_state_rows: &[CanonicalVisibleStateRow],
    version_refs: &VersionHeadMap,
    warnings: &mut Vec<LiveStateRebuildWarning>,
) -> Vec<VisibleRow> {
    let version_descriptor_schema = builtin_projection_schema_meta("lix_version_descriptor");
    let commit_schema = builtin_projection_schema_meta("lix_commit");
    let change_set_schema = builtin_projection_schema_meta("lix_change_set");
    let change_set_element_schema = builtin_projection_schema_meta("lix_change_set_element");
    let commit_edge_schema = builtin_projection_schema_meta("lix_commit_edge");
    let change_author_schema = builtin_projection_schema_meta("lix_change_author");
    let commit_depths = min_depth_by_commit_rows(visible_state_rows);

    let mut candidates: BTreeMap<
        (String, String, String, Option<String>),
        Vec<ProjectionCandidate>,
    > = BTreeMap::new();

    for descriptor in data.version_descriptors.values() {
        let effective_commit_id = version_refs
            .get(&descriptor.entity_id)
            .and_then(|tips| tips.first())
            .cloned()
            .unwrap_or_else(|| crate::version::GLOBAL_VERSION_ID.to_string());
        let depth = commit_depths
            .get(&effective_commit_id)
            .copied()
            .unwrap_or(usize::MAX / 4);

        let row = VisibleRow {
            version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
            commit_id: effective_commit_id,
            replay_cursor: descriptor.replay_cursor.clone(),
            change_id: descriptor.id.clone(),
            untracked: false,
            entity_id: descriptor.entity_id.clone(),
            schema_key: version_descriptor_schema.schema_key.clone(),
            schema_version: descriptor.schema_version.clone(),
            file_id: descriptor.file_id.clone(),
            plugin_key: descriptor.plugin_key.clone(),
            snapshot_content: Some(descriptor.snapshot_content.clone()),
            metadata: descriptor.metadata.clone(),
            created_at: descriptor.created_at.clone(),
            updated_at: descriptor.created_at.clone(),
        };
        let key = (
            row.version_id.clone(),
            row.entity_id.clone(),
            row.schema_key.clone(),
            row.file_id.clone(),
        );
        candidates
            .entry(key)
            .or_default()
            .push(ProjectionCandidate { depth, row });
    }

    // Emit tombstone writes for version_descriptors that are not in the descriptor map
    // (the descriptor map only contains upserts with valid snapshot_content).
    for change in data.changes.values() {
        if change.schema_key != "lix_version_descriptor" || change.snapshot_content.is_some() {
            continue;
        }
        let key = (
            crate::version::GLOBAL_VERSION_ID.to_string(),
            change.entity_id.clone(),
            version_descriptor_schema.schema_key.clone(),
            change.file_id.clone(),
        );
        let depth = usize::MAX / 4;
        let row = VisibleRow {
            version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
            commit_id: crate::version::GLOBAL_VERSION_ID.to_string(),
            replay_cursor: change.replay_cursor.clone(),
            change_id: change.id.clone(),
            untracked: false,
            entity_id: change.entity_id.clone(),
            schema_key: version_descriptor_schema.schema_key.clone(),
            schema_version: change.schema_version.clone(),
            file_id: change.file_id.clone(),
            plugin_key: change.plugin_key.clone(),
            snapshot_content: None,
            metadata: change.metadata.clone(),
            created_at: change.created_at.clone(),
            updated_at: change.created_at.clone(),
        };
        candidates
            .entry(key)
            .or_default()
            .push(ProjectionCandidate { depth, row });
    }

    for (commit_id, depth) in &commit_depths {
        let Some(commit) = data.commits.get(commit_id) else {
            continue;
        };
        let Some(commit_change) = data.changes.get(&commit.id) else {
            warnings.push(LiveStateRebuildWarning {
                code: "missing_commit_change".to_string(),
                message: format!(
                    "lix_commit '{}' references missing change row '{}'",
                    commit.entity_id, commit.id
                ),
            });
            continue;
        };

        let commit_row = VisibleRow {
            version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
            commit_id: commit.entity_id.clone(),
            replay_cursor: commit_change.replay_cursor.clone(),
            change_id: commit_change.id.clone(),
            untracked: false,
            entity_id: commit.entity_id.clone(),
            schema_key: commit_schema.schema_key.clone(),
            schema_version: commit_change.schema_version.clone(),
            file_id: commit_change.file_id.clone(),
            plugin_key: commit_change.plugin_key.clone(),
            snapshot_content: commit_change.snapshot_content.clone(),
            metadata: commit_change.metadata.clone(),
            created_at: commit_change.created_at.clone(),
            updated_at: commit_change.created_at.clone(),
        };
        let commit_key = (
            commit_row.version_id.clone(),
            commit_row.entity_id.clone(),
            commit_row.schema_key.clone(),
            commit_row.file_id.clone(),
        );
        candidates
            .entry(commit_key)
            .or_default()
            .push(ProjectionCandidate {
                depth: *depth,
                row: commit_row,
            });

        if let Some(change_set_id) = commit
            .snapshot
            .change_set_id
            .as_ref()
            .filter(|value| !value.is_empty())
        {
            let change_set_row = VisibleRow {
                version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
                commit_id: commit.entity_id.clone(),
                replay_cursor: commit_change.replay_cursor.clone(),
                change_id: commit_change.id.clone(),
                untracked: false,
                entity_id: change_set_id.clone(),
                schema_key: change_set_schema.schema_key.clone(),
                schema_version: change_set_schema.schema_version.clone(),
                file_id: change_set_schema.file_id.clone(),
                plugin_key: change_set_schema.plugin_key.clone(),
                snapshot_content: Some(canonical_json_value(json!({
                    "id": change_set_id,
                }))),
                metadata: commit_change.metadata.clone(),
                created_at: commit_change.created_at.clone(),
                updated_at: commit_change.created_at.clone(),
            };
            let change_set_key = (
                change_set_row.version_id.clone(),
                change_set_row.entity_id.clone(),
                change_set_row.schema_key.clone(),
                change_set_row.file_id.clone(),
            );
            candidates
                .entry(change_set_key)
                .or_default()
                .push(ProjectionCandidate {
                    depth: *depth,
                    row: change_set_row,
                });

            for change_id in &commit.snapshot.change_ids {
                let Some(change) = data.changes.get(change_id) else {
                    warnings.push(LiveStateRebuildWarning {
                        code: "missing_change".to_string(),
                        message: format!(
                            "lix_commit '{}' references missing change '{}'",
                            commit_id, change_id
                        ),
                    });
                    continue;
                };

                let cse_row = VisibleRow {
                    version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
                    commit_id: commit.entity_id.clone(),
                    replay_cursor: change.replay_cursor.clone(),
                    change_id: change.id.clone(),
                    untracked: false,
                    entity_id: format!("{}~{}", change_set_id, change.id),
                    schema_key: change_set_element_schema.schema_key.clone(),
                    schema_version: change_set_element_schema.schema_version.clone(),
                    file_id: change_set_element_schema.file_id.clone(),
                    plugin_key: change_set_element_schema.plugin_key.clone(),
                    snapshot_content: Some(canonical_json_value(json!({
                        "change_set_id": change_set_id,
                        "change_id": change.id,
                        "entity_id": change.entity_id,
                        "schema_key": change.schema_key,
                        "file_id": change.file_id,
                    }))),
                    metadata: change.metadata.clone(),
                    created_at: change.created_at.clone(),
                    updated_at: change.created_at.clone(),
                };
                let cse_key = (
                    cse_row.version_id.clone(),
                    cse_row.entity_id.clone(),
                    cse_row.schema_key.clone(),
                    cse_row.file_id.clone(),
                );
                candidates
                    .entry(cse_key)
                    .or_default()
                    .push(ProjectionCandidate {
                        depth: *depth,
                        row: cse_row,
                    });

                for account_id in &commit.snapshot.author_account_ids {
                    if account_id.is_empty() {
                        continue;
                    }
                    let author_row = VisibleRow {
                        version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
                        commit_id: commit.entity_id.clone(),
                        replay_cursor: commit_change.replay_cursor.clone(),
                        change_id: commit_change.id.clone(),
                        untracked: false,
                        entity_id: format!("{}~{}", change.id, account_id),
                        schema_key: change_author_schema.schema_key.clone(),
                        schema_version: change_author_schema.schema_version.clone(),
                        file_id: change_author_schema.file_id.clone(),
                        plugin_key: change_author_schema.plugin_key.clone(),
                        snapshot_content: Some(canonical_json_value(json!({
                            "change_id": change.id,
                            "account_id": account_id,
                        }))),
                        metadata: commit_change.metadata.clone(),
                        created_at: commit_change.created_at.clone(),
                        updated_at: commit_change.created_at.clone(),
                    };
                    let author_key = (
                        author_row.version_id.clone(),
                        author_row.entity_id.clone(),
                        author_row.schema_key.clone(),
                        author_row.file_id.clone(),
                    );
                    candidates
                        .entry(author_key)
                        .or_default()
                        .push(ProjectionCandidate {
                            depth: *depth,
                            row: author_row,
                        });
                }
            }
        }

        for parent_id in &commit.snapshot.parent_commit_ids {
            if parent_id.is_empty() {
                continue;
            }
            let edge_row = VisibleRow {
                version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
                commit_id: commit.entity_id.clone(),
                replay_cursor: commit_change.replay_cursor.clone(),
                change_id: commit_change.id.clone(),
                untracked: false,
                entity_id: format!("{}~{}", parent_id, commit.entity_id),
                schema_key: commit_edge_schema.schema_key.clone(),
                schema_version: commit_edge_schema.schema_version.clone(),
                file_id: commit_edge_schema.file_id.clone(),
                plugin_key: commit_edge_schema.plugin_key.clone(),
                snapshot_content: Some(canonical_json_value(json!({
                    "parent_id": parent_id,
                    "child_id": commit.entity_id,
                }))),
                metadata: commit_change.metadata.clone(),
                created_at: commit_change.created_at.clone(),
                updated_at: commit_change.created_at.clone(),
            };
            let edge_key = (
                edge_row.version_id.clone(),
                edge_row.entity_id.clone(),
                edge_row.schema_key.clone(),
                edge_row.file_id.clone(),
            );
            candidates
                .entry(edge_key)
                .or_default()
                .push(ProjectionCandidate {
                    depth: *depth,
                    row: edge_row,
                });
        }
    }

    resolve_projection_candidates(candidates)
}

fn build_untracked_visibility_projection_rows(
    data: &LoadedData,
    warnings: &mut Vec<LiveStateRebuildWarning>,
    stats: &mut Vec<StageStat>,
) -> Result<Vec<VisibleRow>, LixError> {
    let mut latest_by_identity = BTreeMap::new();
    let mut input_rows = 0usize;

    for scope in &data.untracked_visibility_rows {
        let Some(change) = data.changes.get(&scope.change_id) else {
            warnings.push(LiveStateRebuildWarning {
                code: "missing_visibility_change".to_string(),
                message: format!(
                    "untracked visibility '{}' references missing change '{}'",
                    scope.id, scope.change_id
                ),
            });
            continue;
        };

        let Some(row) = build_untracked_visibility_projection_row(scope, change, warnings)? else {
            continue;
        };
        input_rows += 1;

        let key = (
            row.version_id.clone(),
            row.entity_id.clone(),
            row.schema_key.clone(),
            row.file_id.clone(),
        );
        match latest_by_identity.get(&key) {
            Some((_, existing_append_seq)) if scope.append_seq <= *existing_append_seq => {}
            _ => {
                latest_by_identity.insert(key, (row, scope.append_seq));
            }
        }
    }

    let mut rows = latest_by_identity
        .into_values()
        .map(|(row, _)| row)
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        a.schema_key
            .cmp(&b.schema_key)
            .then_with(|| a.version_id.cmp(&b.version_id))
            .then_with(|| a.file_id.cmp(&b.file_id))
            .then_with(|| a.entity_id.cmp(&b.entity_id))
            .then_with(|| a.change_id.cmp(&b.change_id))
            .then_with(|| a.replay_cursor.cmp(&b.replay_cursor))
    });

    stats.push(StageStat {
        stage: "visibility_projection_rows".to_string(),
        input_rows,
        output_rows: rows.len(),
    });

    Ok(rows)
}

fn build_untracked_visibility_projection_row(
    visibility: &super::loader::UntrackedVisibilityRecord,
    change: &super::loader::ChangeRecord,
    warnings: &mut Vec<LiveStateRebuildWarning>,
) -> Result<Option<VisibleRow>, LixError> {
    if visibility.entity_id != change.entity_id
        || visibility.schema_key != change.schema_key
        || visibility.file_id != change.file_id
    {
        warnings.push(LiveStateRebuildWarning {
            code: "visibility_change_identity_mismatch".to_string(),
            message: format!(
                "untracked visibility '{}' identity does not match canonical change '{}'",
                visibility.id, change.id
            ),
        });
        return Ok(None);
    }

    let commit_id = if change.schema_key == "lix_version_ref" {
        parse_untracked_visibility_version_ref_commit_id(change, warnings)?
    } else {
        String::new()
    };

    Ok(Some(VisibleRow {
        version_id: visibility.version_id.clone(),
        commit_id,
        replay_cursor: change.replay_cursor.clone(),
        change_id: change.id.clone(),
        untracked: true,
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        schema_version: change.schema_version.clone(),
        file_id: change.file_id.clone(),
        plugin_key: change.plugin_key.clone(),
        snapshot_content: change.snapshot_content.clone(),
        metadata: change.metadata.clone(),
        created_at: change.created_at.clone(),
        updated_at: change.created_at.clone(),
    }))
}

fn parse_untracked_visibility_version_ref_commit_id(
    change: &super::loader::ChangeRecord,
    warnings: &mut Vec<LiveStateRebuildWarning>,
) -> Result<String, LixError> {
    let Some(snapshot_content) = change.snapshot_content.as_ref() else {
        return Ok(String::new());
    };

    let parsed: LixVersionRef = snapshot_content.parse().map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "materialization: invalid untracked visibility lix_version_ref snapshot JSON: {}",
            error.description
        ),
        hint: None,
    })?;

    if parsed.id.is_empty() || parsed.commit_id.is_empty() {
        warnings.push(LiveStateRebuildWarning {
            code: "invalid_visibility_version_ref".to_string(),
            message: format!(
                "untracked visibility lix_version_ref change '{}' is missing id or commit_id",
                change.id
            ),
        });
        return Ok(String::new());
    }

    if parsed.id != change.entity_id {
        warnings.push(LiveStateRebuildWarning {
            code: "visibility_version_ref_entity_mismatch".to_string(),
            message: format!(
                "untracked visibility lix_version_ref change '{}' snapshot id '{}' does not match entity_id '{}'",
                change.id, parsed.id, change.entity_id
            ),
        });
    }

    Ok(parsed.commit_id)
}

fn load_version_heads_from_canonical(
    visibility_projection_rows: &[VisibleRow],
    warnings: &mut Vec<LiveStateRebuildWarning>,
    stats: &mut Vec<StageStat>,
) -> Result<VersionHeadMap, LixError> {
    let root_version_refs = visibility_projection_rows
        .iter()
        .filter(|row| row.schema_key == "lix_version_ref")
        .filter_map(|row| {
            if row.commit_id.is_empty() {
                if row.snapshot_content.is_some() {
                    warnings.push(LiveStateRebuildWarning {
                        code: "invalid_visibility_version_ref_head".to_string(),
                        message: format!(
                            "untracked visibility lix_version_ref row '{}' is missing a rebuildable commit head",
                            row.change_id
                        ),
                    });
                }
                return None;
            }
            Some((row.entity_id.clone(), row.commit_id.clone()))
        })
        .collect::<Vec<_>>();
    let heads = build_version_head_map_local(&root_version_refs);

    stats.push(StageStat {
        stage: "version_ref_heads".to_string(),
        input_rows: root_version_refs.len(),
        output_rows: heads.values().map(|rows| rows.len()).sum(),
    });

    Ok(heads)
}

async fn load_canonical_visible_state(
    executor: LiveStateExecutorRef<'_>,
    version_refs: &VersionHeadMap,
    target_versions: &BTreeSet<String>,
    stats: &mut Vec<StageStat>,
) -> Result<Vec<CanonicalVisibleStateRow>, LixError> {
    let commit_ids = target_versions
        .iter()
        .filter_map(|version_id| version_refs.get(version_id))
        .flat_map(|heads| heads.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let rows = load_visible_state(
        executor,
        &CanonicalVisibleStateRequest {
            commit_ids,
            filter: CanonicalVisibleStateFilter::default(),
            content_mode: CanonicalContentMode::IncludeSnapshotContent,
            tombstones: CanonicalTombstoneMode::IncludeTombstones,
        },
    )
    .await?;

    stats.push(StageStat {
        stage: "canonical_visible_state".to_string(),
        input_rows: target_versions.len(),
        output_rows: rows.len(),
    });

    Ok(rows)
}

fn resolve_projection_candidates(
    candidates: BTreeMap<(String, String, String, Option<String>), Vec<ProjectionCandidate>>,
) -> Vec<VisibleRow> {
    let mut rows = Vec::new();
    for ((_version_id, _entity_id, _schema_key, _file_id), mut items) in candidates {
        items.sort_by(|a, b| {
            if uses_global_version_descriptor_replay_ordering(a) {
                b.row
                    .replay_cursor
                    .cmp(&a.row.replay_cursor)
                    .then_with(|| b.row.change_id.cmp(&a.row.change_id))
            } else {
                a.depth
                    .cmp(&b.depth)
                    .then_with(|| b.row.change_id.cmp(&a.row.change_id))
            }
        });
        if let Some(winner) = items.into_iter().next() {
            rows.push(winner.row);
        }
    }
    rows
}

fn uses_global_version_descriptor_replay_ordering(candidate: &ProjectionCandidate) -> bool {
    candidate.row.version_id == crate::version::GLOBAL_VERSION_ID
        && candidate.row.schema_key == "lix_version_descriptor"
}

fn canonical_json_value(value: serde_json::Value) -> CanonicalJson {
    CanonicalJson::from_value(value).expect("materialization plan should emit valid canonical JSON")
}

fn builtin_projection_schema_meta(schema_key: &str) -> BuiltinProjectionSchemaMeta {
    let schema = builtin_schema_definition(schema_key).unwrap_or_else(|| {
        panic!("builtin schema '{}' must exist", schema_key);
    });

    let parsed_schema_key = schema
        .get("x-lix-key")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("builtin schema '{}' must define x-lix-key", schema_key))
        .to_string();
    let schema_version = schema
        .get("x-lix-version")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("builtin schema '{}' must define x-lix-version", schema_key))
        .to_string();
    let defaults = builtin_schema_storage_defaults(schema_key).unwrap_or_else(|| {
        panic!(
            "builtin schema '{}' must define storage defaults",
            schema_key
        )
    });

    BuiltinProjectionSchemaMeta {
        schema_key: parsed_schema_key,
        schema_version,
        file_id: defaults.file_id.map(str::to_string),
        plugin_key: defaults.plugin_key.map(str::to_string),
    }
}

fn resolve_target_versions(
    req: &LiveStateRebuildRequest,
    version_refs: &VersionHeadMap,
    data: &LoadedData,
) -> BTreeSet<String> {
    match &req.scope {
        LiveStateRebuildScope::Versions(versions) => {
            let mut resolved = versions.clone();
            resolved.insert(crate::version::GLOBAL_VERSION_ID.to_string());
            resolved
        }
        LiveStateRebuildScope::Full => {
            let mut versions = BTreeSet::new();
            versions.insert(crate::version::GLOBAL_VERSION_ID.to_string());
            for version_id in version_refs.keys() {
                versions.insert(version_id.clone());
            }
            for version_id in data.version_descriptors.keys() {
                versions.insert(version_id.clone());
            }
            versions
        }
    }
}

fn build_final_state(
    latest_visible_state: &[VisibleRow],
    target_versions: &BTreeSet<String>,
    stats: &mut Vec<StageStat>,
) -> Vec<FinalStateRow> {
    let mut visible_by_version: BTreeMap<String, Vec<&VisibleRow>> = BTreeMap::new();
    for row in latest_visible_state {
        visible_by_version
            .entry(row.version_id.clone())
            .or_default()
            .push(row);
    }

    let mut rows = Vec::new();
    for version_id in target_versions {
        let Some(candidates) = visible_by_version.get(version_id) else {
            continue;
        };

        let mut chosen: BTreeMap<(String, String, Option<String>, bool), FinalStateRow> =
            BTreeMap::new();
        let mut sorted_candidates = candidates.clone();
        sorted_candidates.sort_by(|a, b| {
            a.schema_key
                .cmp(&b.schema_key)
                .then_with(|| a.file_id.cmp(&b.file_id))
                .then_with(|| a.entity_id.cmp(&b.entity_id))
                .then_with(|| a.change_id.cmp(&b.change_id))
        });

        for candidate in sorted_candidates {
            let key = (
                candidate.entity_id.clone(),
                candidate.schema_key.clone(),
                candidate.file_id.clone(),
                candidate.untracked,
            );
            if chosen.contains_key(&key) {
                continue;
            }
            chosen.insert(
                key,
                FinalStateRow {
                    version_id: version_id.clone(),
                    source: candidate.clone(),
                },
            );
        }

        rows.extend(chosen.into_values());
    }

    rows.sort_by(|a, b| {
        a.version_id
            .cmp(&b.version_id)
            .then_with(|| a.source.schema_key.cmp(&b.source.schema_key))
            .then_with(|| a.source.file_id.cmp(&b.source.file_id))
            .then_with(|| a.source.entity_id.cmp(&b.source.entity_id))
            .then_with(|| a.source.replay_cursor.cmp(&b.source.replay_cursor))
    });

    stats.push(StageStat {
        stage: "state_materializer".to_string(),
        input_rows: latest_visible_state.len(),
        output_rows: rows.len(),
    });

    rows
}

fn build_writes(final_state: &[FinalStateRow]) -> Result<Vec<LiveStateWrite>, LixError> {
    let mut writes = Vec::new();
    for row in final_state {
        let op = if row.source.snapshot_content.is_some() {
            LiveStateWriteOp::Upsert
        } else {
            LiveStateWriteOp::Tombstone
        };
        writes.push(LiveStateWrite {
            schema_key: require_identity(
                row.source.schema_key.clone(),
                "live-state write schema_key",
            )?,
            entity_id: require_identity(
                row.source.entity_id.clone(),
                "live-state write entity_id",
            )?,
            file_id: optional_identity(row.source.file_id.as_deref(), "live-state write file_id")?,
            version_id: require_identity(row.version_id.clone(), "live-state write version_id")?,
            global: row.version_id == crate::version::GLOBAL_VERSION_ID,
            untracked: row.source.untracked,
            op,
            snapshot_content: row.source.snapshot_content.clone(),
            metadata: row.source.metadata.clone(),
            schema_version: require_identity(
                row.source.schema_version.clone(),
                "live-state write schema_version",
            )?,
            plugin_key: optional_identity(
                row.source.plugin_key.as_deref(),
                "live-state write plugin_key",
            )?,
            change_id: row.source.change_id.clone(),
            created_at: row.source.created_at.clone(),
            updated_at: row.source.updated_at.clone(),
        });
    }

    writes.sort_by(|a, b| {
        a.schema_key
            .cmp(&b.schema_key)
            .then_with(|| a.untracked.cmp(&b.untracked))
            .then_with(|| a.version_id.cmp(&b.version_id))
            .then_with(|| a.file_id.cmp(&b.file_id))
            .then_with(|| a.entity_id.cmp(&b.entity_id))
            .then_with(|| a.change_id.cmp(&b.change_id))
    });

    Ok(writes)
}

fn build_debug_trace(
    req: &LiveStateRebuildRequest,
    data: &LoadedData,
    version_refs: &VersionHeadMap,
    visible_state_rows: &[CanonicalVisibleStateRow],
    latest_visible_state: &[VisibleRow],
    final_state: &[FinalStateRow],
) -> Result<Option<LiveStateRebuildDebugTrace>, LixError> {
    if matches!(req.debug, LiveStateRebuildDebugMode::Off) {
        return Ok(None);
    }

    let limit = req.debug_row_limit.max(1);

    let mut heads_by_version = Vec::new();
    for (version_id, tips) in version_refs {
        for tip in tips {
            heads_by_version.push(VersionHeadDebugRow {
                version_id: require_identity(version_id.clone(), "debug head version_id")?,
                commit_id: tip.clone(),
            });
        }
    }
    heads_by_version.sort_by(|a, b| {
        a.version_id
            .cmp(&b.version_id)
            .then_with(|| a.commit_id.cmp(&b.commit_id))
    });

    let root_versions = root_versions_by_commit(version_refs);
    let mut traversed_commit_set = BTreeSet::new();
    let mut traversed_commits = Vec::new();
    for row in visible_state_rows
        .iter()
        .filter(|row| row.schema_key == "lix_commit")
    {
        if let Some(version_ids) = root_versions.get(&row.root_commit_id) {
            for version_id in version_ids {
                if traversed_commit_set.insert((
                    version_id.clone(),
                    row.entity_id.clone(),
                    row.depth,
                )) {
                    traversed_commits.push(TraversedCommitDebugRow {
                        version_id: require_identity(
                            version_id.clone(),
                            "debug traversed commit version_id",
                        )?,
                        commit_id: row.entity_id.clone(),
                        depth: row.depth,
                    });
                }
            }
        }
    }
    traversed_commits.sort_by(|a, b| {
        a.version_id
            .cmp(&b.version_id)
            .then_with(|| a.depth.cmp(&b.depth))
            .then_with(|| a.commit_id.cmp(&b.commit_id))
    });

    let commit_depths = min_depth_by_commit_rows(visible_state_rows);
    let mut traversed_edge_set = BTreeSet::new();
    let mut traversed_edges = Vec::new();
    for row in visible_state_rows
        .iter()
        .filter(|row| row.schema_key == "lix_commit")
    {
        let Some(version_ids) = root_versions.get(&row.root_commit_id) else {
            continue;
        };
        let Some(commit) = data.commits.get(&row.entity_id) else {
            continue;
        };
        for parent_id in &commit.snapshot.parent_commit_ids {
            if !commit_depths.contains_key(parent_id) {
                continue;
            }
            for version_id in version_ids {
                if traversed_edge_set.insert((
                    version_id.clone(),
                    parent_id.clone(),
                    row.entity_id.clone(),
                )) {
                    traversed_edges.push(TraversedEdgeDebugRow {
                        version_id: version_id.clone(),
                        parent_id: parent_id.clone(),
                        child_id: row.entity_id.clone(),
                    });
                }
            }
        }
    }
    traversed_edges.sort_by(|a, b| {
        a.version_id
            .cmp(&b.version_id)
            .then_with(|| a.parent_id.cmp(&b.parent_id))
            .then_with(|| a.child_id.cmp(&b.child_id))
    });

    let latest_visible_winners = if matches!(req.debug, LiveStateRebuildDebugMode::Full) {
        let mut rows = latest_visible_state
            .iter()
            .map(|row| {
                Ok(LatestVisibleWinnerDebugRow {
                    version_id: require_identity(
                        row.version_id.clone(),
                        "debug latest-visible version_id",
                    )?,
                    entity_id: require_identity(
                        row.entity_id.clone(),
                        "debug latest-visible entity_id",
                    )?,
                    schema_key: require_identity(
                        row.schema_key.clone(),
                        "debug latest-visible schema_key",
                    )?,
                    file_id: optional_identity(
                        row.file_id.as_deref(),
                        "debug latest-visible file_id",
                    )?,
                    commit_id: row.commit_id.clone(),
                    change_id: row.change_id.clone(),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        rows.sort_by(|a, b| {
            a.version_id
                .cmp(&b.version_id)
                .then_with(|| a.schema_key.cmp(&b.schema_key))
                .then_with(|| a.file_id.cmp(&b.file_id))
                .then_with(|| a.entity_id.cmp(&b.entity_id))
                .then_with(|| a.commit_id.cmp(&b.commit_id))
                .then_with(|| a.change_id.cmp(&b.change_id))
        });
        rows.into_iter().take(limit).collect()
    } else {
        Vec::new()
    };

    let visibility_winners = if matches!(req.debug, LiveStateRebuildDebugMode::Full) {
        let mut rows = final_state
            .iter()
            .map(|row| {
                Ok(VisibilityWinnerDebugRow {
                    version_id: require_identity(row.version_id.clone(), "debug scope version_id")?,
                    entity_id: require_identity(
                        row.source.entity_id.clone(),
                        "debug scope entity_id",
                    )?,
                    schema_key: require_identity(
                        row.source.schema_key.clone(),
                        "debug scope schema_key",
                    )?,
                    file_id: optional_identity(
                        row.source.file_id.as_deref(),
                        "debug scope file_id",
                    )?,
                    global: row.version_id == crate::version::GLOBAL_VERSION_ID,
                    change_id: row.source.change_id.clone(),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        rows.sort_by(|a, b| {
            a.version_id
                .cmp(&b.version_id)
                .then_with(|| a.schema_key.cmp(&b.schema_key))
                .then_with(|| a.file_id.cmp(&b.file_id))
                .then_with(|| a.entity_id.cmp(&b.entity_id))
                .then_with(|| a.global.cmp(&b.global))
                .then_with(|| a.change_id.cmp(&b.change_id))
        });
        rows.into_iter().take(limit).collect()
    } else {
        Vec::new()
    };

    Ok(Some(LiveStateRebuildDebugTrace {
        heads_by_version: heads_by_version.into_iter().take(limit).collect(),
        traversed_commits: traversed_commits.into_iter().take(limit).collect(),
        traversed_edges: traversed_edges.into_iter().take(limit).collect(),
        latest_visible_winners,
        visibility_winners,
    }))
}

fn build_version_head_map_local(root_version_refs: &[(String, String)]) -> VersionHeadMap {
    let mut heads = BTreeMap::<String, Vec<String>>::new();
    for (version_id, commit_id) in root_version_refs {
        heads
            .entry(version_id.clone())
            .or_default()
            .push(commit_id.clone());
    }
    for commit_ids in heads.values_mut() {
        commit_ids.sort();
        commit_ids.dedup();
    }
    heads
}

fn root_versions_by_commit(version_refs: &VersionHeadMap) -> BTreeMap<String, Vec<String>> {
    let mut roots = BTreeMap::<String, Vec<String>>::new();
    for (version_id, commit_ids) in version_refs {
        for commit_id in commit_ids {
            roots
                .entry(commit_id.clone())
                .or_default()
                .push(version_id.clone());
        }
    }
    for version_ids in roots.values_mut() {
        version_ids.sort();
        version_ids.dedup();
    }
    roots
}

fn min_depth_by_commit_rows(
    visible_state_rows: &[CanonicalVisibleStateRow],
) -> BTreeMap<String, usize> {
    let mut depths = BTreeMap::new();
    for row in visible_state_rows
        .iter()
        .filter(|row| row.schema_key == "lix_commit")
    {
        let commit_id = row.entity_id.clone();
        match depths.get(&commit_id) {
            Some(existing) if *existing <= row.depth => {}
            _ => {
                depths.insert(commit_id, row.depth);
            }
        }
    }
    depths
}

fn require_identity<T>(value: impl Into<String>, context: &str) -> Result<T, LixError>
where
    T: TryFrom<String, Error = LixError>,
{
    let value = value.into();
    T::try_from(value.clone()).map_err(|_| {
        LixError::unknown(format!(
            "{context} must be a non-empty canonical identity, got '{}'",
            value
        ))
    })
}

fn optional_identity(value: Option<&str>, context: &str) -> Result<Option<String>, LixError> {
    value
        .map(|value| {
            if value.is_empty() {
                Err(LixError::unknown(format!("{context} must not be empty")))
            } else {
                Ok(value.to_string())
            }
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::{
        append_untracked_change_visibility_rows, CanonicalUntrackedVisibilityKind,
        CanonicalUntrackedVisibilityWrite,
    };
    use crate::live_state::materialize::loader;
    use crate::test_support::{
        init_test_backend_core, seed_canonical_change_row, seed_local_version_head,
        CanonicalChangeSeed, TestSqliteBackend,
    };
    use crate::{CanonicalSchemaKey, EntityId};

    async fn seed_scope_row(
        backend: &TestSqliteBackend,
        change_id: &str,
        version_id: &str,
        visibility_kind: CanonicalUntrackedVisibilityKind,
        entity_id: &str,
        schema_key: &str,
        created_at: &str,
    ) {
        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("visibility transaction should open");
        append_untracked_change_visibility_rows(
            transaction.as_mut(),
            &[CanonicalUntrackedVisibilityWrite {
                id: format!("visibility:{change_id}"),
                change_id: change_id.to_string(),
                version_id: version_id.to_string(),
                visibility_kind,
                entity_id: EntityId::new(entity_id).expect("valid entity id"),
                schema_key: CanonicalSchemaKey::new(schema_key).expect("valid schema key"),
                file_id: None,
                created_at: created_at.to_string(),
            }],
        )
        .await
        .expect("visibility row should seed");
        transaction
            .commit()
            .await
            .expect("visibility transaction should commit");
    }

    #[tokio::test]
    async fn rebuild_plan_ignores_unscoped_canonical_version_ref_changes() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");

        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-commit-local",
                entity_id: "commit-local",
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: None,
                plugin_key: None,
                snapshot_id: "snapshot-commit-local",
                snapshot_content: Some(
                    "{\"id\":\"commit-local\",\"change_set_id\":\"cs-local\",\"change_ids\":[],\"parent_commit_ids\":[]}",
                ),
                metadata: None,
                created_at: "2026-03-30T03:00:00Z",
            },
        )
        .await
        .expect("local commit should seed");
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-commit-legacy",
                entity_id: "commit-legacy",
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: None,
                plugin_key: None,
                snapshot_id: "snapshot-commit-legacy",
                snapshot_content: Some(
                    "{\"id\":\"commit-legacy\",\"change_set_id\":\"cs-legacy\",\"change_ids\":[],\"parent_commit_ids\":[]}",
                ),
                metadata: None,
                created_at: "2026-03-30T03:01:00Z",
            },
        )
        .await
        .expect("legacy commit should seed");
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-version-ref-legacy",
                entity_id: "main",
                schema_key: "lix_version_ref",
                schema_version: "1",
                file_id: None,
                plugin_key: None,
                snapshot_id: "snapshot-version-ref-legacy",
                snapshot_content: Some("{\"id\":\"main\",\"commit_id\":\"commit-legacy\"}"),
                metadata: None,
                created_at: "2026-03-30T03:02:00Z",
            },
        )
        .await
        .expect("legacy canonical version-ref row should seed");
        seed_local_version_head(&backend, "main", "commit-local", "2026-03-30T03:03:00Z")
            .await
            .expect("local version head should seed");

        let req = LiveStateRebuildRequest {
            scope: LiveStateRebuildScope::Full,
            debug: LiveStateRebuildDebugMode::Summary,
            debug_row_limit: 32,
        };
        let mut executor = &backend;
        let plan = live_state_rebuild_plan_with_executor(&mut executor, &req)
            .await
            .expect("canonical version-ref facts without untracked visibility should be ignored");
        assert!(
            !plan
                .debug
                .as_ref()
                .expect("summary debug trace should be present")
                .heads_by_version
                .iter()
                .any(|head| head.version_id == "main"),
            "expected canonical version-ref facts without untracked visibility to stay out of rebuild heads"
        );
        assert!(
            !plan.writes.iter().any(|write| {
                write.schema_key.to_string() == "lix_version_ref"
                    && write.entity_id.to_string() == "main"
            }),
            "expected canonical version-ref facts without untracked visibility to stay out of rebuild writes"
        );
    }

    #[tokio::test]
    async fn rebuild_plan_uses_untracked_visibility_version_refs() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");

        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-commit-local",
                entity_id: "commit-local",
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: None,
                plugin_key: None,
                snapshot_id: "snapshot-commit-local",
                snapshot_content: Some(
                    "{\"id\":\"commit-local\",\"change_set_id\":\"cs-local\",\"change_ids\":[],\"parent_commit_ids\":[]}",
                ),
                metadata: None,
                created_at: "2026-03-30T03:00:00Z",
            },
        )
        .await
        .expect("local commit should seed");
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-version-ref-main",
                entity_id: "main",
                schema_key: "lix_version_ref",
                schema_version: "1",
                file_id: None,
                plugin_key: None,
                snapshot_id: "snapshot-version-ref-main",
                snapshot_content: Some("{\"id\":\"main\",\"commit_id\":\"commit-local\"}"),
                metadata: None,
                created_at: "2026-03-30T03:01:00Z",
            },
        )
        .await
        .expect("untracked-visible canonical version-ref row should seed");
        seed_scope_row(
            &backend,
            "change-version-ref-main",
            crate::version::GLOBAL_VERSION_ID,
            CanonicalUntrackedVisibilityKind::Global,
            "main",
            "lix_version_ref",
            "2026-03-30T03:01:00Z",
        )
        .await;

        let req = LiveStateRebuildRequest {
            scope: LiveStateRebuildScope::Full,
            debug: LiveStateRebuildDebugMode::Summary,
            debug_row_limit: 32,
        };
        let mut executor = &backend;
        let plan = live_state_rebuild_plan_with_executor(&mut executor, &req)
            .await
            .expect("journaled version-ref rows should drive rebuild planning");

        assert!(
            plan.debug
                .as_ref()
                .expect("summary debug trace should be present")
                .heads_by_version
                .iter()
                .any(|head| head.version_id == "main" && head.commit_id == "commit-local"),
            "expected materialization heads to come from canonical visible version refs"
        );

        let version_ref_write = plan
            .writes
            .iter()
            .find(|write| {
                write.schema_key.to_string() == "lix_version_ref"
                    && write.entity_id.to_string() == "main"
            })
            .expect("rebuild plan should materialize main version ref");
        assert!(version_ref_write.untracked);
        assert_eq!(version_ref_write.change_id, "change-version-ref-main");
        assert_eq!(
            version_ref_write.version_id.to_string(),
            crate::version::GLOBAL_VERSION_ID
        );
    }

    #[tokio::test]
    async fn rebuild_plan_materializes_local_rows_from_untracked_visibility_relation() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");

        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-key-value-v1",
                entity_id: "pref-theme",
                schema_key: "lix_key_value",
                schema_version: "1",
                file_id: None,
                plugin_key: None,
                snapshot_id: "snapshot-key-value-v1",
                snapshot_content: Some("{\"key\":\"theme\",\"value\":\"dark\"}"),
                metadata: None,
                created_at: "2026-03-30T04:00:00Z",
            },
        )
        .await
        .expect("untracked-visible local key-value row should seed");
        seed_scope_row(
            &backend,
            "change-key-value-v1",
            "v1",
            CanonicalUntrackedVisibilityKind::Version,
            "pref-theme",
            "lix_key_value",
            "2026-03-30T04:00:00Z",
        )
        .await;

        let req = LiveStateRebuildRequest {
            scope: LiveStateRebuildScope::Versions([String::from("v1")].into_iter().collect()),
            debug: LiveStateRebuildDebugMode::Summary,
            debug_row_limit: 32,
        };
        let mut executor = &backend;
        let plan = live_state_rebuild_plan_with_executor(&mut executor, &req)
            .await
            .expect("untracked-visible local rows should rebuild");

        let key_value_write = plan
            .writes
            .iter()
            .find(|write| {
                write.schema_key.to_string() == "lix_key_value"
                    && write.entity_id.to_string() == "pref-theme"
            })
            .expect("rebuild plan should materialize untracked-visible local row");
        assert!(key_value_write.untracked);
        assert_eq!(key_value_write.version_id.to_string(), "v1");
        assert_eq!(key_value_write.change_id, "change-key-value-v1");
    }

    #[test]
    fn resolve_projection_candidates_prefers_newer_global_version_descriptor_tombstone() {
        let key = (
            crate::version::GLOBAL_VERSION_ID.to_string(),
            "version-deleted".to_string(),
            "lix_version_descriptor".to_string(),
            None,
        );
        let older_descriptor = ProjectionCandidate {
            depth: usize::MAX / 4,
            row: VisibleRow {
                version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
                commit_id: crate::version::GLOBAL_VERSION_ID.to_string(),
                replay_cursor: ReplayCursor::new("change-old", "2026-04-01T00:00:00Z"),
                change_id: "change-old".to_string(),
                untracked: false,
                entity_id: "version-deleted".to_string(),
                schema_key: "lix_version_descriptor".to_string(),
                schema_version: "1".to_string(),
                file_id: None,
                plugin_key: None,
                snapshot_content: Some(
                    CanonicalJson::from_text(
                        "{\"id\":\"version-deleted\",\"name\":\"Version Deleted\",\"hidden\":false}",
                    )
                    .expect("descriptor snapshot should parse"),
                ),
                metadata: None,
                created_at: "2026-04-01T00:00:00Z".to_string(),
                updated_at: "2026-04-01T00:00:00Z".to_string(),
            },
        };
        let newer_tombstone = ProjectionCandidate {
            depth: usize::MAX / 4,
            row: VisibleRow {
                version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
                commit_id: crate::version::GLOBAL_VERSION_ID.to_string(),
                replay_cursor: ReplayCursor::new("change-new", "2026-04-01T00:00:01Z"),
                change_id: "change-new".to_string(),
                untracked: false,
                entity_id: "version-deleted".to_string(),
                schema_key: "lix_version_descriptor".to_string(),
                schema_version: "1".to_string(),
                file_id: None,
                plugin_key: None,
                snapshot_content: None,
                metadata: None,
                created_at: "2026-04-01T00:00:01Z".to_string(),
                updated_at: "2026-04-01T00:00:01Z".to_string(),
            },
        };

        let rows = resolve_projection_candidates(BTreeMap::from([(
            key,
            vec![older_descriptor, newer_tombstone],
        )]));

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].change_id, "change-new");
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[test]
    fn build_untracked_visibility_projection_rows_prefers_visibility_append_seq() {
        let older_change = loader::ChangeRecord {
            id: "change-0001".to_string(),
            entity_id: "main".to_string(),
            schema_key: "lix_version_ref".to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(
                CanonicalJson::from_text("{\"id\":\"main\",\"commit_id\":\"commit-old\"}")
                    .expect("version ref snapshot should parse"),
            ),
            metadata: None,
            created_at: "2026-04-01T00:00:10Z".to_string(),
            replay_cursor: ReplayCursor::new("change-0001", "2026-04-01T00:00:10Z"),
        };
        let newer_visibility_to_older_change = loader::UntrackedVisibilityRecord {
            id: "visibility-0002".to_string(),
            append_seq: 2,
            change_id: "change-0001".to_string(),
            version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
            entity_id: "main".to_string(),
            schema_key: "lix_version_ref".to_string(),
            file_id: None,
        };
        let newer_change = loader::ChangeRecord {
            id: "change-0002".to_string(),
            entity_id: "main".to_string(),
            schema_key: "lix_version_ref".to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(
                CanonicalJson::from_text("{\"id\":\"main\",\"commit_id\":\"commit-newer-fact\"}")
                    .expect("version ref snapshot should parse"),
            ),
            metadata: None,
            created_at: "2026-04-01T00:00:01Z".to_string(),
            replay_cursor: ReplayCursor::new("change-0002", "2026-04-01T00:00:01Z"),
        };
        let older_visibility_to_newer_change = loader::UntrackedVisibilityRecord {
            id: "visibility-0001".to_string(),
            append_seq: 1,
            change_id: "change-0002".to_string(),
            version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
            entity_id: "main".to_string(),
            schema_key: "lix_version_ref".to_string(),
            file_id: None,
        };
        let data = loader::LoadedData {
            changes: BTreeMap::from([
                ("change-0001".to_string(), older_change),
                ("change-0002".to_string(), newer_change),
            ]),
            untracked_visibility_rows: vec![
                older_visibility_to_newer_change,
                newer_visibility_to_older_change,
            ],
            commits: BTreeMap::new(),
            version_descriptors: BTreeMap::new(),
        };
        let mut warnings = Vec::new();
        let mut stats = Vec::new();

        let rows = build_untracked_visibility_projection_rows(&data, &mut warnings, &mut stats)
            .expect("visibility projection should build");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].change_id, "change-0001");
        assert_eq!(rows[0].commit_id, "commit-old");
    }
}
