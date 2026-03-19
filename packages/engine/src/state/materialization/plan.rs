use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde_json::json;

use crate::schema::builtin::types::LixVersionRef;
use crate::schema::builtin::{builtin_schema_definition, decode_lixcol_literal};
use crate::schema::live_store::load_untracked_live_rows_by_property_with_executor;
use crate::state::materialization::loader::{load_data, ChangeRecord, LoadedData};
use crate::state::materialization::types::{
    LatestVisibleWinnerDebugRow, LiveStateRebuildDebugMode, LiveStateRebuildDebugTrace,
    LiveStateRebuildPlan, LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning,
    LiveStateWrite, LiveStateWriteOp, ScopeWinnerDebugRow, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionAncestryDebugRow, VersionHeadDebugRow,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{CanonicalJson, LixBackend, LixError};

#[derive(Debug, Clone)]
struct VisibleRow {
    version_id: String,
    commit_id: String,
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

#[cfg(test)]
#[derive(Debug, Clone)]
struct ResolvedVersionRef {
    change: ChangeRecord,
    owner_commit_id: String,
    target_commit_id: Option<String>,
}

pub(crate) async fn live_state_rebuild_plan_internal(
    backend: &dyn LixBackend,
    req: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    let data = load_data(backend).await?;
    let mut stats = Vec::new();
    let mut warnings = Vec::new();

    let all_commit_edges = build_all_commit_edges(&data, &mut stats);
    let version_refs =
        load_version_heads_from_untracked(backend, &mut warnings, &mut stats).await?;
    let commit_graph = build_commit_graph(&version_refs, &all_commit_edges, &mut stats);

    let latest_visible_state = build_latest_visible_state(
        &data,
        &commit_graph,
        &version_refs,
        &mut warnings,
        &mut stats,
    );

    let target_versions = resolve_target_versions(req, &version_refs, &data, &latest_visible_state);
    let version_ancestry =
        build_version_ancestry(&data, &target_versions, &mut warnings, &mut stats);
    let final_state = build_final_state(
        &latest_visible_state,
        &version_ancestry,
        &target_versions,
        &mut stats,
    );
    let writes = build_writes(&final_state)?;

    let debug = build_debug_trace(
        req,
        &version_refs,
        &commit_graph,
        &all_commit_edges,
        &version_ancestry,
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
    let mut edges = BTreeSet::new();

    for commit in data.commits.values() {
        for parent_id in &commit.snapshot.parent_commit_ids {
            if parent_id.is_empty() || commit.entity_id.is_empty() {
                continue;
            }
            edges.insert((parent_id.clone(), commit.entity_id.clone()));
        }
    }

    for edge in &data.commit_edges {
        if edge.snapshot.parent_id.is_empty() || edge.snapshot.child_id.is_empty() {
            continue;
        }
        edges.insert((
            edge.snapshot.parent_id.clone(),
            edge.snapshot.child_id.clone(),
        ));
    }

    stats.push(StageStat {
        stage: "all_commit_edges".to_string(),
        input_rows: data.commits.len() + data.commit_edges.len(),
        output_rows: edges.len(),
    });

    edges
}

#[cfg(test)]
fn resolve_version_ref_candidates(
    data: &LoadedData,
    all_commit_edges: &BTreeSet<(String, String)>,
    commit_causal_rank: &BTreeMap<String, usize>,
    change_commit_by_change_id: &BTreeMap<String, String>,
    warnings: &mut Vec<LiveStateRebuildWarning>,
    stats: &mut Vec<StageStat>,
) -> BTreeMap<String, Vec<ResolvedVersionRef>> {
    let parents_by_child = parents_by_child(all_commit_edges);
    let mut ancestor_memo = BTreeMap::<String, BTreeSet<String>>::new();
    let mut by_version = BTreeMap::<String, Vec<ResolvedVersionRef>>::new();

    for change in data.changes.values() {
        if change.schema_key != "lix_version_ref" {
            continue;
        }

        let parsed = match change.snapshot_content.as_deref() {
            Some(snapshot_raw) => match serde_json::from_str::<LixVersionRef>(snapshot_raw) {
                Ok(parsed) => Some(parsed),
                Err(error) => {
                    warnings.push(LiveStateRebuildWarning {
                        code: "invalid_version_ref_snapshot".to_string(),
                        message: format!(
                            "lix_version_ref change '{}' has invalid snapshot JSON: {}",
                            change.id, error
                        ),
                    });
                    None
                }
            },
            None => None,
        };

        let version_id = parsed
            .as_ref()
            .map(|parsed| parsed.id.clone())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| change.entity_id.clone());

        let owner_commit_id = change_commit_by_change_id
            .get(&change.id)
            .cloned()
            .or_else(|| {
                parsed
                    .as_ref()
                    .map(|parsed| parsed.commit_id.clone())
                    .filter(|value| !value.is_empty())
            });

        let Some(owner_commit_id) = owner_commit_id else {
            warnings.push(LiveStateRebuildWarning {
                code: "orphan_version_ref_change".to_string(),
                message: format!(
                    "lix_version_ref change '{}' for version '{}' is not linked from any commit",
                    change.id, version_id
                ),
            });
            continue;
        };

        by_version
            .entry(version_id)
            .or_default()
            .push(ResolvedVersionRef {
                change: change.clone(),
                owner_commit_id,
                target_commit_id: parsed
                    .as_ref()
                    .map(|parsed| parsed.commit_id.clone())
                    .filter(|value| !value.is_empty()),
            });
    }

    let mut resolved = BTreeMap::new();
    for (version_id, candidates) in by_version {
        let owner_commits = candidates
            .iter()
            .map(|candidate| candidate.owner_commit_id.clone())
            .collect::<BTreeSet<_>>();
        let maximal_owner_commits = maximal_commits(
            &owner_commits,
            &parents_by_child,
            &commit_causal_rank,
            &mut ancestor_memo,
        );
        if maximal_owner_commits.len() > 1 {
            warnings.push(LiveStateRebuildWarning {
                code: "divergent_version_ref_history".to_string(),
                message: format!(
                    "version '{}' has multiple visible lix_version_ref winners: {}",
                    version_id,
                    maximal_owner_commits
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        }

        let mut winners = Vec::new();
        for owner_commit_id in maximal_owner_commits {
            let mut owner_candidates = candidates
                .iter()
                .filter(|candidate| candidate.owner_commit_id == owner_commit_id)
                .cloned()
                .collect::<Vec<_>>();
            owner_candidates.sort_by(|left, right| {
                right
                    .change
                    .created_at
                    .cmp(&left.change.created_at)
                    .then_with(|| right.change.id.cmp(&left.change.id))
            });
            if let Some(winner) = owner_candidates.into_iter().next() {
                winners.push(winner);
            }
        }
        if !winners.is_empty() {
            resolved.insert(version_id, winners);
        }
    }

    stats.push(StageStat {
        stage: "version_refs".to_string(),
        input_rows: data
            .changes
            .values()
            .filter(|change| change.schema_key == "lix_version_ref")
            .count(),
        output_rows: resolved.values().map(|rows| rows.len()).sum(),
    });

    resolved
}

#[cfg(test)]
fn build_version_refs(
    resolved_version_refs: &BTreeMap<String, Vec<ResolvedVersionRef>>,
    stats: &mut Vec<StageStat>,
) -> BTreeMap<String, Vec<String>> {
    let mut heads = BTreeMap::new();
    for (version_id, rows) in resolved_version_refs {
        let mut head_commit_ids = rows
            .iter()
            .filter_map(|row| row.target_commit_id.clone())
            .collect::<Vec<_>>();
        head_commit_ids.sort();
        head_commit_ids.dedup();
        if !head_commit_ids.is_empty() {
            heads.insert(version_id.clone(), head_commit_ids);
        }
    }

    stats.push(StageStat {
        stage: "version_ref_heads".to_string(),
        input_rows: resolved_version_refs.values().map(|rows| rows.len()).sum(),
        output_rows: heads.values().map(|rows| rows.len()).sum(),
    });

    heads
}

fn build_commit_graph(
    version_refs: &BTreeMap<String, Vec<String>>,
    all_commit_edges: &BTreeSet<(String, String)>,
    stats: &mut Vec<StageStat>,
) -> BTreeMap<(String, String), usize> {
    let parent_by_child = parents_by_child(all_commit_edges);

    let mut queue = VecDeque::new();
    for (version_id, tips) in version_refs {
        for tip in tips {
            queue.push_back((version_id.clone(), tip.clone(), 0usize));
        }
    }

    let mut min_depth: BTreeMap<(String, String), usize> = BTreeMap::new();
    while let Some((version_id, commit_id, depth)) = queue.pop_front() {
        let key = (version_id.clone(), commit_id.clone());
        if let Some(existing_depth) = min_depth.get(&key) {
            if *existing_depth <= depth {
                continue;
            }
        }
        min_depth.insert(key, depth);

        if let Some(parents) = parent_by_child.get(&commit_id) {
            for parent_id in parents {
                queue.push_back((version_id.clone(), parent_id.clone(), depth + 1));
            }
        }
    }

    stats.push(StageStat {
        stage: "commit_graph".to_string(),
        input_rows: version_refs.values().map(|rows| rows.len()).sum::<usize>()
            + all_commit_edges.len(),
        output_rows: min_depth.len(),
    });

    min_depth
}

fn build_latest_visible_state(
    data: &LoadedData,
    commit_graph: &BTreeMap<(String, String), usize>,
    version_refs: &BTreeMap<String, Vec<String>>,
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
                .then_with(|| b.change.created_at.cmp(&a.change.created_at))
                .then_with(|| b.change.id.cmp(&a.change.id))
        });

        let Some(winner) = rows.first() else {
            continue;
        };

        let mut created_candidates = rows.clone();
        created_candidates.sort_by(|a, b| {
            b.depth
                .cmp(&a.depth)
                .then_with(|| a.change.created_at.cmp(&b.change.created_at))
                .then_with(|| a.change.id.cmp(&b.change.id))
        });
        let created_at = created_candidates
            .first()
            .map(|row| row.change.created_at.clone())
            .unwrap_or_else(|| winner.change.created_at.clone());

        winners.push(VisibleRow {
            version_id: winner.source_version_id.clone(),
            commit_id: winner.commit_id.clone(),
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
            .then_with(|| a.change_id.cmp(&b.change_id))
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
    commit_graph: &BTreeMap<(String, String), usize>,
    version_refs: &BTreeMap<String, Vec<String>>,
    warnings: &mut Vec<LiveStateRebuildWarning>,
) -> Vec<VisibleRow> {
    let version_descriptor_schema = builtin_projection_schema_meta("lix_version_descriptor");
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

async fn load_version_heads_from_untracked(
    backend: &dyn LixBackend,
    warnings: &mut Vec<LiveStateRebuildWarning>,
    stats: &mut Vec<StageStat>,
) -> Result<BTreeMap<String, Vec<String>>, LixError> {
    let mut executor = backend;
    let rows = load_untracked_live_rows_by_property_with_executor(
        &mut executor,
        "lix_version_ref",
        "commit_id",
        &BTreeMap::new(),
        true,
        &["entity_id"],
    )
    .await?;

    let mut heads = BTreeMap::<String, Vec<String>>::new();
    for row in rows {
        let Some(snapshot_raw) = crate::schema::live_store::logical_snapshot_text(
            &crate::schema::live_layout::load_live_row_access_with_executor(
                &mut executor,
                "lix_version_ref",
            )
            .await?,
            &row,
        )?
        else {
            continue;
        };
        match serde_json::from_str::<LixVersionRef>(&snapshot_raw) {
            Ok(snapshot) if !snapshot.id.is_empty() && !snapshot.commit_id.is_empty() => {
                heads
                    .entry(snapshot.id)
                    .or_default()
                    .push(snapshot.commit_id);
            }
            Ok(_) => {}
            Err(error) => warnings.push(LiveStateRebuildWarning {
                code: "invalid_version_ref_snapshot".to_string(),
                message: format!(
                    "untracked lix_version_ref '{}' has invalid snapshot JSON: {}",
                    row.entity_id, error
                ),
            }),
        }
    }

    for values in heads.values_mut() {
        values.sort();
        values.dedup();
    }

    stats.push(StageStat {
        stage: "version_ref_heads".to_string(),
        input_rows: heads.values().map(|rows| rows.len()).sum(),
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
                .then_with(|| b.row.created_at.cmp(&a.row.created_at))
                .then_with(|| b.row.change_id.cmp(&a.row.change_id))
        });
        if let Some(winner) = items.into_iter().next() {
            rows.push(winner.row);
        }
    }
    rows
}

fn min_depth_by_commit(
    commit_graph: &BTreeMap<(String, String), usize>,
) -> BTreeMap<String, usize> {
    let mut min_depth = BTreeMap::new();
    for ((_, commit_id), depth) in commit_graph {
        min_depth
            .entry(commit_id.clone())
            .and_modify(|existing: &mut usize| {
                if *depth < *existing {
                    *existing = *depth;
                }
            })
            .or_insert(*depth);
    }
    min_depth
}

fn canonical_json_value(value: serde_json::Value) -> CanonicalJson {
    CanonicalJson::from_value(value).expect("materialization plan should emit valid canonical JSON")
}

fn parents_by_child(
    all_commit_edges: &BTreeSet<(String, String)>,
) -> BTreeMap<String, Vec<String>> {
    let mut parent_by_child = BTreeMap::new();
    for (parent, child) in all_commit_edges {
        parent_by_child
            .entry(child.clone())
            .or_insert_with(Vec::new)
            .push(parent.clone());
    }
    for parents in parent_by_child.values_mut() {
        parents.sort();
        parents.dedup();
    }
    parent_by_child
}

#[cfg(test)]
fn maximal_commits(
    candidate_commit_ids: &BTreeSet<String>,
    parents_by_child: &BTreeMap<String, Vec<String>>,
    commit_causal_rank: &BTreeMap<String, usize>,
    ancestor_memo: &mut BTreeMap<String, BTreeSet<String>>,
) -> BTreeSet<String> {
    let mut maximal = BTreeSet::new();
    for candidate in candidate_commit_ids {
        let dominated = candidate_commit_ids.iter().any(|other| {
            candidate != other
                && ancestors_for_commit(other, parents_by_child, ancestor_memo).contains(candidate)
        });
        if !dominated {
            maximal.insert(candidate.clone());
        }
    }

    if maximal.is_empty() {
        if let Some(last) = candidate_commit_ids.iter().max_by(|left, right| {
            let left_rank = commit_causal_rank.get(*left).copied().unwrap_or(0);
            let right_rank = commit_causal_rank.get(*right).copied().unwrap_or(0);
            left_rank.cmp(&right_rank).then_with(|| left.cmp(right))
        }) {
            maximal.insert(last.clone());
        }
    }

    maximal
}

#[cfg(test)]
fn ancestors_for_commit(
    commit_id: &str,
    parents_by_child: &BTreeMap<String, Vec<String>>,
    ancestor_memo: &mut BTreeMap<String, BTreeSet<String>>,
) -> BTreeSet<String> {
    if let Some(existing) = ancestor_memo.get(commit_id) {
        return existing.clone();
    }

    let mut ancestors = BTreeSet::new();
    if let Some(parents) = parents_by_child.get(commit_id) {
        for parent_id in parents {
            ancestors.insert(parent_id.clone());
            ancestors.extend(ancestors_for_commit(
                parent_id,
                parents_by_child,
                ancestor_memo,
            ));
        }
    }

    ancestor_memo.insert(commit_id.to_string(), ancestors.clone());
    ancestors
}

#[cfg(test)]
fn build_commit_causal_rank(
    data: &LoadedData,
    all_commit_edges: &BTreeSet<(String, String)>,
) -> BTreeMap<String, usize> {
    let mut parents_by_child = BTreeMap::<String, Vec<String>>::new();
    for commit_id in data.commits.keys() {
        parents_by_child.entry(commit_id.clone()).or_default();
    }
    for (parent_id, child_id) in all_commit_edges {
        parents_by_child
            .entry(child_id.clone())
            .or_default()
            .push(parent_id.clone());
    }
    for parents in parents_by_child.values_mut() {
        parents.sort();
        parents.dedup();
    }

    let mut memo = BTreeMap::<String, usize>::new();
    for commit_id in data.commits.keys() {
        let mut active = BTreeSet::new();
        let _ = commit_causal_rank_for_commit(commit_id, &parents_by_child, &mut memo, &mut active);
    }
    memo
}

#[cfg(test)]
fn commit_causal_rank_for_commit(
    commit_id: &str,
    parents_by_child: &BTreeMap<String, Vec<String>>,
    memo: &mut BTreeMap<String, usize>,
    active: &mut BTreeSet<String>,
) -> usize {
    if let Some(rank) = memo.get(commit_id).copied() {
        return rank;
    }
    if !active.insert(commit_id.to_string()) {
        return 0;
    }

    let rank = match parents_by_child.get(commit_id) {
        Some(parents) if !parents.is_empty() => {
            1 + parents
                .iter()
                .map(|parent_id| {
                    commit_causal_rank_for_commit(parent_id, parents_by_child, memo, active)
                })
                .max()
                .unwrap_or(0)
        }
        _ => 0,
    };

    active.remove(commit_id);
    memo.insert(commit_id.to_string(), rank);
    rank
}

#[cfg(test)]
fn build_change_commit_index(data: &LoadedData) -> BTreeMap<String, String> {
    let mut index = BTreeMap::new();
    for commit in data.commits.values() {
        for change_id in &commit.snapshot.change_ids {
            index
                .entry(change_id.clone())
                .or_insert_with(|| commit.entity_id.clone());
        }
    }
    index
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
    version_refs: &BTreeMap<String, Vec<String>>,
    data: &LoadedData,
    latest_visible_state: &[VisibleRow],
) -> BTreeSet<String> {
    match &req.scope {
        LiveStateRebuildScope::Versions(versions) => versions.clone(),
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

fn build_version_ancestry(
    data: &LoadedData,
    target_versions: &BTreeSet<String>,
    _warnings: &mut Vec<LiveStateRebuildWarning>,
    stats: &mut Vec<StageStat>,
) -> BTreeMap<String, Vec<(String, usize)>> {
    let mut ancestry: BTreeMap<String, Vec<(String, usize)>> = BTreeMap::new();

    for version_id in target_versions {
        ancestry.insert(version_id.clone(), vec![(version_id.clone(), 0usize)]);
    }

    stats.push(StageStat {
        stage: "version_ancestry".to_string(),
        input_rows: data.version_descriptors.len(),
        output_rows: ancestry.values().map(|rows| rows.len()).sum(),
    });

    ancestry
}

fn build_final_state(
    latest_visible_state: &[VisibleRow],
    version_ancestry: &BTreeMap<String, Vec<(String, usize)>>,
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
        let Some(ancestry) = version_ancestry.get(version_id) else {
            continue;
        };

        let mut chosen: BTreeMap<(String, String, String), FinalStateRow> = BTreeMap::new();
        for (ancestor_id, _depth) in ancestry {
            let Some(candidates) = visible_by_version.get(ancestor_id) else {
                continue;
            };

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
        }

        rows.extend(chosen.into_values());
    }

    rows.sort_by(|a, b| {
        a.version_id
            .cmp(&b.version_id)
            .then_with(|| a.source.schema_key.cmp(&b.source.schema_key))
            .then_with(|| a.source.file_id.cmp(&b.source.file_id))
            .then_with(|| a.source.entity_id.cmp(&b.source.entity_id))
            .then_with(|| a.source.change_id.cmp(&b.source.change_id))
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
            schema_key: require_identity(row.source.schema_key.clone(), "live-state write schema_key")?,
            entity_id: require_identity(row.source.entity_id.clone(), "live-state write entity_id")?,
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
            plugin_key: require_identity(row.source.plugin_key.clone(), "live-state write plugin_key")?,
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
    version_refs: &BTreeMap<String, Vec<String>>,
    commit_graph: &BTreeMap<(String, String), usize>,
    all_commit_edges: &BTreeSet<(String, String)>,
    version_ancestry: &BTreeMap<String, Vec<(String, usize)>>,
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

    let mut ancestry_rows = Vec::new();
    for (version_id, rows) in version_ancestry {
        for (ancestor_version_id, inheritance_depth) in rows {
            ancestry_rows.push(VersionAncestryDebugRow {
                version_id: require_identity(version_id.clone(), "debug ancestry version_id")?,
                ancestor_version_id: require_identity(
                    ancestor_version_id.clone(),
                    "debug ancestry ancestor_version_id",
                )?,
                inheritance_depth: *inheritance_depth,
            });
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
        version_ancestry: ancestry_rows.into_iter().take(limit).collect(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::builtin::types::LixCommit;
    use crate::state::materialization::loader::{
        CommitEdgeRecord, CommitRecord, VersionDescriptorRecord,
    };

    fn change_record(
        id: &str,
        entity_id: &str,
        schema_key: &str,
        snapshot_content: Option<CanonicalJson>,
    ) -> ChangeRecord {
        ChangeRecord {
            id: id.to_string(),
            entity_id: entity_id.to_string(),
            schema_key: schema_key.to_string(),
            schema_version: "1".to_string(),
            file_id: "lix".to_string(),
            plugin_key: "lix".to_string(),
            snapshot_content,
            metadata: None,
            created_at: "2025-01-01T00:00:00Z".to_string(),
        }
    }

    fn commit_record(id: &str, change_ids: &[&str], parent_commit_ids: &[&str]) -> CommitRecord {
        CommitRecord {
            id: format!("chg~{id}"),
            entity_id: id.to_string(),
            snapshot: LixCommit {
                id: id.to_string(),
                change_set_id: Some(format!("cs~{id}")),
                change_ids: change_ids.iter().map(|value| value.to_string()).collect(),
                author_account_ids: Vec::new(),
                parent_commit_ids: parent_commit_ids
                    .iter()
                    .map(|value| value.to_string())
                    .collect(),
            },
            created_at: "2025-01-01T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn version_ref_resolution_prefers_reset_owner_over_target_commit_maxima() {
        let data = LoadedData {
            changes: BTreeMap::from([
                (
                    "ref-child".to_string(),
                    change_record(
                        "ref-child",
                        "main",
                        "lix_version_ref",
                        Some(canonical_json_value(
                            json!({ "id": "main", "commit_id": "commit-child" }),
                        )),
                    ),
                ),
                (
                    "ref-reset".to_string(),
                    change_record(
                        "ref-reset",
                        "main",
                        "lix_version_ref",
                        Some(canonical_json_value(
                            json!({ "id": "main", "commit_id": "commit-root" }),
                        )),
                    ),
                ),
            ]),
            commits: BTreeMap::from([
                (
                    "commit-root".to_string(),
                    commit_record("commit-root", &[], &[]),
                ),
                (
                    "commit-child".to_string(),
                    commit_record("commit-child", &["ref-child"], &["commit-root"]),
                ),
                (
                    "commit-reset".to_string(),
                    commit_record("commit-reset", &["ref-reset"], &["commit-child"]),
                ),
            ]),
            version_descriptors: BTreeMap::<String, VersionDescriptorRecord>::new(),
            commit_edges: Vec::<CommitEdgeRecord>::new(),
        };

        let mut stats = Vec::new();
        let mut warnings = Vec::new();
        let all_commit_edges = build_all_commit_edges(&data, &mut stats);
        let commit_causal_rank = build_commit_causal_rank(&data, &all_commit_edges);
        let change_commit_by_change_id = build_change_commit_index(&data);
        let resolved = resolve_version_ref_candidates(
            &data,
            &all_commit_edges,
            &commit_causal_rank,
            &change_commit_by_change_id,
            &mut warnings,
            &mut stats,
        );
        let version_refs = build_version_refs(&resolved, &mut stats);

        assert!(warnings.is_empty());
        assert_eq!(
            version_refs.get("main"),
            Some(&vec!["commit-root".to_string()])
        );
    }
}
