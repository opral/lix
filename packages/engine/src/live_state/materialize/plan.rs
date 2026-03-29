use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;

use super::loader::{load_data_with_executor, ChangeRecord, LoadedData};
use super::types::{
    LatestVisibleWinnerDebugRow, LiveStateRebuildDebugMode, LiveStateRebuildDebugTrace,
    LiveStateRebuildPlan, LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning,
    LiveStateWrite, LiveStateWriteOp, ScopeWinnerDebugRow, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionHeadDebugRow,
};
use crate::backend::QueryExecutor;
use crate::canonical::lineage::{
    build_version_commit_depth_map, build_version_head_map, collect_commit_edges,
    min_depth_by_commit, VersionCommitDepthMap, VersionHeadMap,
};
use crate::canonical::roots::load_all_version_head_commit_ids;
use crate::live_state::ReplayCursor;
use crate::schema::builtin::{builtin_schema_definition, decode_lixcol_literal};
use crate::version::GLOBAL_VERSION_ID;
use crate::{CanonicalJson, LixBackend, LixError};

#[derive(Debug, Clone)]
struct VisibleRow {
    version_id: String,
    commit_id: String,
    replay_cursor: ReplayCursor,
    change_id: String,
    entity_id: String,
    schema_key: String,
    schema_version: String,
    file_id: String,
    plugin_key: String,
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
struct VisibleCandidate {
    source_version_id: String,
    depth: usize,
    commit_id: String,
    change: ChangeRecord,
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
    file_id: String,
    plugin_key: String,
}

pub(crate) async fn live_state_rebuild_plan_internal(
    backend: &dyn LixBackend,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    let mut executor = backend;
    live_state_rebuild_plan_with_executor(&mut executor, req).await
}

pub(crate) async fn live_state_rebuild_plan_with_executor(
    executor: &mut dyn QueryExecutor,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    let data = load_data_with_executor(executor).await?;
    let mut stats = Vec::new();
    let mut warnings = Vec::new();

    let all_commit_edges = build_all_commit_edges(&data, &mut stats);
    let version_refs = load_version_heads_from_canonical(executor, &mut stats).await?;
    let commit_graph = build_version_commit_depth_map(&version_refs, &all_commit_edges);
    stats.push(StageStat {
        stage: "commit_graph".to_string(),
        input_rows: version_refs.values().map(|rows| rows.len()).sum::<usize>()
            + all_commit_edges.len(),
        output_rows: commit_graph.len(),
    });

    let latest_visible_state = build_latest_visible_state(
        &data,
        &commit_graph,
        &version_refs,
        &mut warnings,
        &mut stats,
    );

    let target_versions = resolve_target_versions(req, &version_refs, &data, &latest_visible_state);
    let final_state = build_final_state(&latest_visible_state, &target_versions, &mut stats);
    let writes = build_writes(&final_state)?;

    let debug = build_debug_trace(
        req,
        &version_refs,
        &commit_graph,
        &all_commit_edges,
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

fn build_all_commit_edges(
    data: &LoadedData,
    stats: &mut Vec<StageStat>,
) -> BTreeSet<(String, String)> {
    let edges = collect_commit_edges(
        data.commits.values().map(|commit| {
            (
                commit.entity_id.clone(),
                commit.snapshot.parent_commit_ids.clone(),
            )
        }),
        data.commit_edges.iter().map(|edge| {
            (
                edge.snapshot.parent_id.clone(),
                edge.snapshot.child_id.clone(),
            )
        }),
    );

    stats.push(StageStat {
        stage: "all_commit_edges".to_string(),
        input_rows: data.commits.len() + data.commit_edges.len(),
        output_rows: edges.len(),
    });

    edges
}

fn build_latest_visible_state(
    data: &LoadedData,
    commit_graph: &VersionCommitDepthMap,
    version_refs: &VersionHeadMap,
    warnings: &mut Vec<LiveStateRebuildWarning>,
    stats: &mut Vec<StageStat>,
) -> Vec<VisibleRow> {
    let mut candidates: BTreeMap<(String, String, String, String), Vec<VisibleCandidate>> =
        BTreeMap::new();

    for ((version_id, commit_id), depth) in commit_graph {
        let Some(commit) = data.commits.get(commit_id) else {
            warnings.push(LiveStateRebuildWarning {
                code: "missing_commit_snapshot".to_string(),
                message: format!(
                    "commit_graph references commit '{}' for version '{}' but no lix_commit snapshot exists",
                    commit_id, version_id
                ),
            });
            continue;
        };

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

            if change.schema_key == "lix_version_descriptor"
                || change.schema_key == "lix_version_ref"
            {
                continue;
            }

            let key = (
                version_id.clone(),
                change.entity_id.clone(),
                change.schema_key.clone(),
                change.file_id.clone(),
            );
            candidates.entry(key).or_default().push(VisibleCandidate {
                source_version_id: version_id.clone(),
                depth: *depth,
                commit_id: commit.entity_id.clone(),
                change: change.clone(),
            });
        }
    }

    let mut winners = Vec::new();
    for ((_version_id, _entity_id, _schema_key, _file_id), mut rows) in candidates {
        rows.sort_by(|a, b| {
            a.depth
                .cmp(&b.depth)
                .then_with(|| b.change.replay_cursor.cmp(&a.change.replay_cursor))
        });

        let Some(winner) = rows.first() else {
            continue;
        };

        let mut created_candidates = rows.clone();
        created_candidates.sort_by(|a, b| {
            b.depth
                .cmp(&a.depth)
                .then_with(|| a.change.replay_cursor.cmp(&b.change.replay_cursor))
        });
        let created_at = created_candidates
            .first()
            .map(|row| row.change.created_at.clone())
            .unwrap_or_else(|| winner.change.created_at.clone());

        winners.push(VisibleRow {
            version_id: winner.source_version_id.clone(),
            commit_id: winner.commit_id.clone(),
            replay_cursor: winner.change.replay_cursor.clone(),
            change_id: winner.change.id.clone(),
            entity_id: winner.change.entity_id.clone(),
            schema_key: winner.change.schema_key.clone(),
            schema_version: winner.change.schema_version.clone(),
            file_id: winner.change.file_id.clone(),
            plugin_key: winner.change.plugin_key.clone(),
            snapshot_content: winner.change.snapshot_content.clone(),
            metadata: winner.change.metadata.clone(),
            created_at,
            updated_at: winner.change.created_at.clone(),
        });
    }

    winners.extend(build_global_projection_rows(
        data,
        commit_graph,
        version_refs,
        warnings,
    ));

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
        input_rows: commit_graph.len(),
        output_rows: winners.len(),
    });

    winners
}

fn build_global_projection_rows(
    data: &LoadedData,
    commit_graph: &VersionCommitDepthMap,
    version_refs: &VersionHeadMap,
    warnings: &mut Vec<LiveStateRebuildWarning>,
) -> Vec<VisibleRow> {
    let version_descriptor_schema = builtin_projection_schema_meta("lix_version_descriptor");
    let version_ref_schema = builtin_projection_schema_meta("lix_version_ref");
    let commit_schema = builtin_projection_schema_meta("lix_commit");
    let change_set_element_schema = builtin_projection_schema_meta("lix_change_set_element");
    let commit_edge_schema = builtin_projection_schema_meta("lix_commit_edge");
    let change_author_schema = builtin_projection_schema_meta("lix_change_author");
    let commit_depths = min_depth_by_commit(commit_graph);

    let mut candidates: BTreeMap<(String, String, String, String), Vec<ProjectionCandidate>> =
        BTreeMap::new();

    for descriptor in data.version_descriptors.values() {
        let effective_commit_id = version_refs
            .get(&descriptor.entity_id)
            .and_then(|tips| tips.first())
            .cloned()
            .unwrap_or_else(|| GLOBAL_VERSION_ID.to_string());
        let depth = commit_depths
            .get(&effective_commit_id)
            .copied()
            .unwrap_or(usize::MAX / 4);

        let row = VisibleRow {
            version_id: GLOBAL_VERSION_ID.to_string(),
            commit_id: effective_commit_id,
            replay_cursor: descriptor.replay_cursor.clone(),
            change_id: descriptor.id.clone(),
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

    let mut latest_version_ref_changes: BTreeMap<String, &ChangeRecord> = BTreeMap::new();
    for change in data.changes.values() {
        if change.schema_key != "lix_version_ref" {
            continue;
        }
        match latest_version_ref_changes.get(&change.entity_id) {
            Some(existing) if existing.replay_cursor >= change.replay_cursor => {}
            _ => {
                latest_version_ref_changes.insert(change.entity_id.clone(), change);
            }
        }
    }

    for change in latest_version_ref_changes.into_values() {
        let effective_commit_id = version_refs
            .get(&change.entity_id)
            .and_then(|tips| tips.first())
            .cloned()
            .unwrap_or_default();
        let depth = commit_depths
            .get(&effective_commit_id)
            .copied()
            .unwrap_or(usize::MAX / 4);

        let row = VisibleRow {
            version_id: GLOBAL_VERSION_ID.to_string(),
            commit_id: effective_commit_id,
            replay_cursor: change.replay_cursor.clone(),
            change_id: change.id.clone(),
            entity_id: change.entity_id.clone(),
            schema_key: version_ref_schema.schema_key.clone(),
            schema_version: change.schema_version.clone(),
            file_id: change.file_id.clone(),
            plugin_key: change.plugin_key.clone(),
            snapshot_content: change.snapshot_content.clone(),
            metadata: change.metadata.clone(),
            created_at: change.created_at.clone(),
            updated_at: change.created_at.clone(),
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
            GLOBAL_VERSION_ID.to_string(),
            change.entity_id.clone(),
            version_descriptor_schema.schema_key.clone(),
            change.file_id.clone(),
        );
        let depth = usize::MAX / 4;
        let row = VisibleRow {
            version_id: GLOBAL_VERSION_ID.to_string(),
            commit_id: GLOBAL_VERSION_ID.to_string(),
            replay_cursor: change.replay_cursor.clone(),
            change_id: change.id.clone(),
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
            warnings.push(LiveStateRebuildWarning {
                code: "missing_commit_snapshot".to_string(),
                message: format!(
                    "commit_graph references commit '{}' but no lix_commit snapshot exists",
                    commit_id
                ),
            });
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
            version_id: GLOBAL_VERSION_ID.to_string(),
            commit_id: commit.entity_id.clone(),
            replay_cursor: commit_change.replay_cursor.clone(),
            change_id: commit_change.id.clone(),
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
                    version_id: GLOBAL_VERSION_ID.to_string(),
                    commit_id: commit.entity_id.clone(),
                    replay_cursor: change.replay_cursor.clone(),
                    change_id: change.id.clone(),
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
                        version_id: GLOBAL_VERSION_ID.to_string(),
                        commit_id: commit.entity_id.clone(),
                        replay_cursor: commit_change.replay_cursor.clone(),
                        change_id: commit_change.id.clone(),
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
                version_id: GLOBAL_VERSION_ID.to_string(),
                commit_id: commit.entity_id.clone(),
                replay_cursor: commit_change.replay_cursor.clone(),
                change_id: commit_change.id.clone(),
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

async fn load_version_heads_from_canonical(
    executor: &mut dyn QueryExecutor,
    stats: &mut Vec<StageStat>,
) -> Result<VersionHeadMap, LixError> {
    let root_version_refs = load_all_version_head_commit_ids(executor).await?;
    let heads = build_version_head_map(&root_version_refs);

    stats.push(StageStat {
        stage: "version_ref_heads".to_string(),
        input_rows: root_version_refs.len(),
        output_rows: heads.values().map(|rows| rows.len()).sum(),
    });

    Ok(heads)
}

fn resolve_projection_candidates(
    candidates: BTreeMap<(String, String, String, String), Vec<ProjectionCandidate>>,
) -> Vec<VisibleRow> {
    let mut rows = Vec::new();
    for ((_version_id, _entity_id, _schema_key, _file_id), mut items) in candidates {
        items.sort_by(|a, b| {
            a.depth
                .cmp(&b.depth)
                .then_with(|| b.row.change_id.cmp(&a.row.change_id))
        });
        if let Some(winner) = items.into_iter().next() {
            rows.push(winner.row);
        }
    }
    rows
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
    let overrides = schema
        .get("x-lix-override-lixcols")
        .and_then(serde_json::Value::as_object)
        .unwrap_or_else(|| {
            panic!(
                "builtin schema '{}' must define object x-lix-override-lixcols",
                schema_key
            )
        });
    let file_id = overrides
        .get("lixcol_file_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| {
            panic!(
                "builtin schema '{}' must define string lixcol_file_id",
                schema_key
            )
        });
    let plugin_key = overrides
        .get("lixcol_plugin_key")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| {
            panic!(
                "builtin schema '{}' must define string lixcol_plugin_key",
                schema_key
            )
        });

    BuiltinProjectionSchemaMeta {
        schema_key: parsed_schema_key,
        schema_version,
        file_id: decode_lixcol_literal(file_id),
        plugin_key: decode_lixcol_literal(plugin_key),
    }
}

fn resolve_target_versions(
    req: &LiveStateRebuildRequest,
    version_refs: &VersionHeadMap,
    data: &LoadedData,
    latest_visible_state: &[VisibleRow],
) -> BTreeSet<String> {
    match &req.scope {
        LiveStateRebuildScope::Versions(versions) => {
            let mut resolved = versions.clone();
            resolved.insert(GLOBAL_VERSION_ID.to_string());
            resolved
        }
        LiveStateRebuildScope::Full => {
            let mut versions = BTreeSet::new();
            for version_id in version_refs.keys() {
                versions.insert(version_id.clone());
            }
            for version_id in data.version_descriptors.keys() {
                versions.insert(version_id.clone());
            }
            for row in latest_visible_state {
                versions.insert(row.version_id.clone());
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

        let mut chosen: BTreeMap<(String, String, String), FinalStateRow> = BTreeMap::new();
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
            file_id: require_identity(row.source.file_id.clone(), "live-state write file_id")?,
            version_id: require_identity(row.version_id.clone(), "live-state write version_id")?,
            global: row.version_id == GLOBAL_VERSION_ID,
            op,
            snapshot_content: row.source.snapshot_content.clone(),
            metadata: row.source.metadata.clone(),
            schema_version: require_identity(
                row.source.schema_version.clone(),
                "live-state write schema_version",
            )?,
            plugin_key: require_identity(
                row.source.plugin_key.clone(),
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
            .then_with(|| a.version_id.cmp(&b.version_id))
            .then_with(|| a.file_id.cmp(&b.file_id))
            .then_with(|| a.entity_id.cmp(&b.entity_id))
            .then_with(|| a.change_id.cmp(&b.change_id))
    });

    Ok(writes)
}

fn build_debug_trace(
    req: &LiveStateRebuildRequest,
    version_refs: &VersionHeadMap,
    commit_graph: &VersionCommitDepthMap,
    all_commit_edges: &BTreeSet<(String, String)>,
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

    let mut traversed_commits = Vec::new();
    for ((version_id, commit_id), depth) in commit_graph {
        traversed_commits.push(TraversedCommitDebugRow {
            version_id: require_identity(version_id.clone(), "debug traversed commit version_id")?,
            commit_id: commit_id.clone(),
            depth: *depth,
        });
    }

    let mut traversed_edges = Vec::new();
    for (parent_id, child_id) in all_commit_edges {
        for (version_id, tips) in version_refs {
            if !tips.is_empty() {
                traversed_edges.push(TraversedEdgeDebugRow {
                    version_id: version_id.clone(),
                    parent_id: parent_id.clone(),
                    child_id: child_id.clone(),
                });
            }
        }
    }

    let latest_visible_winners = if matches!(req.debug, LiveStateRebuildDebugMode::Full) {
        latest_visible_state
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
                    file_id: require_identity(row.file_id.clone(), "debug latest-visible file_id")?,
                    commit_id: row.commit_id.clone(),
                    change_id: row.change_id.clone(),
                })
            })
            .take(limit)
            .collect::<Result<Vec<_>, LixError>>()?
    } else {
        Vec::new()
    };

    let scope_winners = if matches!(req.debug, LiveStateRebuildDebugMode::Full) {
        final_state
            .iter()
            .map(|row| {
                Ok(ScopeWinnerDebugRow {
                    version_id: require_identity(row.version_id.clone(), "debug scope version_id")?,
                    entity_id: require_identity(
                        row.source.entity_id.clone(),
                        "debug scope entity_id",
                    )?,
                    schema_key: require_identity(
                        row.source.schema_key.clone(),
                        "debug scope schema_key",
                    )?,
                    file_id: require_identity(row.source.file_id.clone(), "debug scope file_id")?,
                    global: row.version_id == GLOBAL_VERSION_ID,
                    change_id: row.source.change_id.clone(),
                })
            })
            .take(limit)
            .collect::<Result<Vec<_>, LixError>>()?
    } else {
        Vec::new()
    };

    Ok(Some(LiveStateRebuildDebugTrace {
        heads_by_version: heads_by_version.into_iter().take(limit).collect(),
        traversed_commits: traversed_commits.into_iter().take(limit).collect(),
        traversed_edges: traversed_edges.into_iter().take(limit).collect(),
        latest_visible_winners,
        scope_winners,
    }))
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
