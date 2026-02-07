use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde_json::json;

use crate::builtin_schema::builtin_schema_definition;
use crate::builtin_schema::types::LixVersionPointer;
use crate::materialization::loader::{load_data, ChangeRecord, LoadedData};
use crate::materialization::types::{
    InheritanceWinnerDebugRow, LatestVisibleWinnerDebugRow, MaterializationDebugMode,
    MaterializationDebugTrace, MaterializationPlan, MaterializationRequest, MaterializationScope,
    MaterializationWarning, MaterializationWrite, MaterializationWriteOp, StageStat,
    TraversedCommitDebugRow, TraversedEdgeDebugRow, VersionAncestryDebugRow,
    VersionPointerDebugRow,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError};

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
    snapshot_content: Option<String>,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Clone)]
struct FinalStateRow {
    version_id: String,
    inherited_from_version_id: Option<String>,
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

pub(crate) async fn materialization_plan_internal(
    backend: &dyn LixBackend,
    req: &MaterializationRequest,
) -> Result<MaterializationPlan, LixError> {
    let data = load_data(backend).await?;
    let mut stats = Vec::new();
    let mut warnings = Vec::new();

    let all_commit_edges = build_all_commit_edges(&data, &mut stats);
    let version_pointers = build_version_pointers(&data, &all_commit_edges, &mut stats);
    let commit_graph = build_commit_graph(&version_pointers, &all_commit_edges, &mut stats);

    let latest_visible_state = build_latest_visible_state(
        &data,
        &commit_graph,
        &version_pointers,
        &mut warnings,
        &mut stats,
    );

    let target_versions =
        resolve_target_versions(req, &version_pointers, &data, &latest_visible_state);
    let version_ancestry =
        build_version_ancestry(&data, &target_versions, &mut warnings, &mut stats);
    let final_state = build_final_state(
        &latest_visible_state,
        &version_ancestry,
        &target_versions,
        &mut stats,
    );
    let writes = build_writes(&final_state);

    let debug = build_debug_trace(
        req,
        &version_pointers,
        &commit_graph,
        &all_commit_edges,
        &version_ancestry,
        &latest_visible_state,
        &final_state,
    );

    Ok(MaterializationPlan {
        run_id: format!("materialization::{:?}", req.scope),
        scope: resolved_scope(req, target_versions),
        stats,
        writes,
        warnings,
        debug,
    })
}

fn resolved_scope(
    req: &MaterializationRequest,
    target_versions: BTreeSet<String>,
) -> MaterializationScope {
    match &req.scope {
        MaterializationScope::Full => MaterializationScope::Versions(target_versions),
        MaterializationScope::Versions(_) => MaterializationScope::Versions(target_versions),
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

fn build_version_pointers(
    data: &LoadedData,
    all_commit_edges: &BTreeSet<(String, String)>,
    stats: &mut Vec<StageStat>,
) -> BTreeMap<String, Vec<String>> {
    let mut version_commits: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for tip in &data.version_pointers {
        version_commits
            .entry(tip.snapshot.id.clone())
            .or_default()
            .insert(tip.snapshot.commit_id.clone());
    }

    let mut tips = BTreeMap::new();
    for (version_id, commits) in &version_commits {
        let mut non_tips = BTreeSet::new();
        for commit_id in commits {
            let has_child_in_same_version = all_commit_edges
                .iter()
                .any(|(parent, child)| parent == commit_id && commits.contains(child));
            if has_child_in_same_version {
                non_tips.insert(commit_id.clone());
            }
        }

        let mut tip_set: Vec<String> = commits
            .iter()
            .filter(|commit_id| !non_tips.contains(*commit_id))
            .cloned()
            .collect();
        tip_set.sort();

        if tip_set.is_empty() {
            // Fallback to the latest observed commit for this version.
            if let Some(last) = data
                .version_pointers
                .iter()
                .filter(|tip| tip.snapshot.id == *version_id)
                .max_by(|a, b| {
                    a.created_at
                        .cmp(&b.created_at)
                        .then_with(|| a.id.cmp(&b.id))
                })
            {
                tip_set.push(last.snapshot.commit_id.clone());
            }
        }

        if !tip_set.is_empty() {
            tips.insert(version_id.clone(), tip_set);
        }
    }

    stats.push(StageStat {
        stage: "version_pointers".to_string(),
        input_rows: data.version_pointers.len(),
        output_rows: tips.values().map(|rows| rows.len()).sum(),
    });

    tips
}

fn build_commit_graph(
    version_pointers: &BTreeMap<String, Vec<String>>,
    all_commit_edges: &BTreeSet<(String, String)>,
    stats: &mut Vec<StageStat>,
) -> BTreeMap<(String, String), usize> {
    let mut parent_by_child: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (parent, child) in all_commit_edges {
        parent_by_child
            .entry(child.clone())
            .or_default()
            .push(parent.clone());
    }
    for parents in parent_by_child.values_mut() {
        parents.sort();
        parents.dedup();
    }

    let mut queue = VecDeque::new();
    for (version_id, tips) in version_pointers {
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
        input_rows: version_pointers
            .values()
            .map(|rows| rows.len())
            .sum::<usize>()
            + all_commit_edges.len(),
        output_rows: min_depth.len(),
    });

    min_depth
}

fn build_latest_visible_state(
    data: &LoadedData,
    commit_graph: &BTreeMap<(String, String), usize>,
    version_pointers: &BTreeMap<String, Vec<String>>,
    warnings: &mut Vec<MaterializationWarning>,
    stats: &mut Vec<StageStat>,
) -> Vec<VisibleRow> {
    let mut candidates: BTreeMap<(String, String, String, String), Vec<VisibleCandidate>> =
        BTreeMap::new();

    for ((version_id, commit_id), depth) in commit_graph {
        let Some(commit) = data.commits.get(commit_id) else {
            warnings.push(MaterializationWarning {
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
                warnings.push(MaterializationWarning {
                    code: "missing_change".to_string(),
                    message: format!(
                        "lix_commit '{}' references missing change '{}'",
                        commit_id, change_id
                    ),
                });
                continue;
            };

            if change.schema_key == "lix_version_descriptor"
                || change.schema_key == "lix_version_pointer"
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
            created_at,
            updated_at: winner.change.created_at.clone(),
        });
    }

    winners.extend(build_global_projection_rows(
        data,
        commit_graph,
        version_pointers,
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
    version_pointers: &BTreeMap<String, Vec<String>>,
    warnings: &mut Vec<MaterializationWarning>,
) -> Vec<VisibleRow> {
    let commit_schema = builtin_projection_schema_meta("lix_commit");
    let version_pointer_schema = builtin_projection_schema_meta("lix_version_pointer");
    let change_set_element_schema = builtin_projection_schema_meta("lix_change_set_element");
    let commit_edge_schema = builtin_projection_schema_meta("lix_commit_edge");
    let change_author_schema = builtin_projection_schema_meta("lix_change_author");
    let commit_depths = min_depth_by_commit(commit_graph);
    let change_commit_by_change_id = build_change_commit_index(data);
    let latest_pointer_changes = latest_version_pointer_changes(data);

    let mut candidates: BTreeMap<(String, String, String, String), Vec<ProjectionCandidate>> =
        BTreeMap::new();

    for change in &latest_pointer_changes {
        let pointer_snapshot = change.snapshot_content.as_ref().and_then(|raw| {
            match serde_json::from_str::<LixVersionPointer>(raw) {
                Ok(parsed) => Some(parsed),
                Err(error) => {
                    warnings.push(MaterializationWarning {
                        code: "invalid_version_pointer_snapshot".to_string(),
                        message: format!(
                            "lix_version_pointer change '{}' has invalid snapshot JSON: {}",
                            change.id, error
                        ),
                    });
                    None
                }
            }
        });

        let effective_commit_id = pointer_snapshot
            .as_ref()
            .filter(|snapshot| !snapshot.commit_id.is_empty())
            .map(|snapshot| snapshot.commit_id.clone())
            .or_else(|| change_commit_by_change_id.get(&change.id).cloned())
            .unwrap_or_else(|| GLOBAL_VERSION_ID.to_string());
        let depth = commit_depths
            .get(&effective_commit_id)
            .copied()
            .unwrap_or(usize::MAX / 4);

        let snapshot_content = pointer_snapshot
            .as_ref()
            .filter(|snapshot| !snapshot.id.is_empty() && !snapshot.commit_id.is_empty())
            .map(|snapshot| {
                let working_commit_id = snapshot
                    .working_commit_id
                    .as_ref()
                    .filter(|value| !value.is_empty())
                    .cloned()
                    .unwrap_or_else(|| snapshot.commit_id.clone());
                json!({
                    "id": snapshot.id,
                    "commit_id": snapshot.commit_id,
                    "working_commit_id": working_commit_id,
                })
                .to_string()
            });

        let row = VisibleRow {
            version_id: GLOBAL_VERSION_ID.to_string(),
            commit_id: effective_commit_id,
            change_id: change.id.clone(),
            entity_id: change.entity_id.clone(),
            schema_key: version_pointer_schema.schema_key.clone(),
            schema_version: change.schema_version.clone(),
            file_id: change.file_id.clone(),
            plugin_key: change.plugin_key.clone(),
            snapshot_content,
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

    for (version_id, tip_commit_ids) in version_pointers {
        if latest_pointer_changes
            .iter()
            .any(|change| change.entity_id == *version_id)
        {
            continue;
        }
        for tip_commit_id in tip_commit_ids {
            let tip_depth = commit_depths
                .get(tip_commit_id)
                .copied()
                .unwrap_or(usize::MAX / 4);
            let fallback_created_at = data
                .commits
                .get(tip_commit_id)
                .map(|commit| commit.created_at.clone())
                .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string());

            let row = VisibleRow {
                version_id: GLOBAL_VERSION_ID.to_string(),
                commit_id: tip_commit_id.clone(),
                change_id: format!("syn~lix_version_pointer~{}~{}", version_id, tip_commit_id),
                entity_id: version_id.clone(),
                schema_key: version_pointer_schema.schema_key.clone(),
                schema_version: version_pointer_schema.schema_version.clone(),
                file_id: version_pointer_schema.file_id.clone(),
                plugin_key: version_pointer_schema.plugin_key.clone(),
                snapshot_content: Some(
                    json!({
                        "id": version_id,
                        "commit_id": tip_commit_id,
                        "working_commit_id": tip_commit_id,
                    })
                    .to_string(),
                ),
                created_at: fallback_created_at.clone(),
                updated_at: fallback_created_at,
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
                .push(ProjectionCandidate {
                    depth: tip_depth,
                    row,
                });
        }
    }

    for (commit_id, depth) in &commit_depths {
        let Some(commit) = data.commits.get(commit_id) else {
            warnings.push(MaterializationWarning {
                code: "missing_commit_snapshot".to_string(),
                message: format!(
                    "commit_graph references commit '{}' but no lix_commit snapshot exists",
                    commit_id
                ),
            });
            continue;
        };
        let Some(commit_change) = data.changes.get(&commit.id) else {
            warnings.push(MaterializationWarning {
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
                    warnings.push(MaterializationWarning {
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
                    snapshot_content: Some(
                        json!({
                            "change_set_id": change_set_id,
                            "change_id": change.id,
                            "entity_id": change.entity_id,
                            "schema_key": change.schema_key,
                            "file_id": change.file_id,
                        })
                        .to_string(),
                    ),
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
                        snapshot_content: Some(
                            json!({
                                "change_id": change.id,
                                "account_id": account_id,
                            })
                            .to_string(),
                        ),
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
                snapshot_content: Some(
                    json!({
                        "parent_id": parent_id,
                        "child_id": commit.entity_id,
                    })
                    .to_string(),
                ),
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

fn latest_version_pointer_changes(data: &LoadedData) -> Vec<ChangeRecord> {
    let mut latest_by_entity: BTreeMap<String, ChangeRecord> = BTreeMap::new();
    for change in data.changes.values() {
        if change.schema_key != "lix_version_pointer" {
            continue;
        }
        match latest_by_entity.get(&change.entity_id) {
            Some(existing)
                if existing.created_at > change.created_at
                    || (existing.created_at == change.created_at && existing.id >= change.id) => {}
            _ => {
                latest_by_entity.insert(change.entity_id.clone(), change.clone());
            }
        }
    }
    latest_by_entity.into_values().collect()
}

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

fn decode_lixcol_literal(raw: &str) -> String {
    serde_json::from_str::<String>(raw).unwrap_or_else(|_| raw.trim_matches('"').to_string())
}

fn resolve_target_versions(
    req: &MaterializationRequest,
    version_pointers: &BTreeMap<String, Vec<String>>,
    data: &LoadedData,
    latest_visible_state: &[VisibleRow],
) -> BTreeSet<String> {
    match &req.scope {
        MaterializationScope::Versions(versions) => versions.clone(),
        MaterializationScope::Full => {
            let mut versions = BTreeSet::new();
            for version_id in version_pointers.keys() {
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
    warnings: &mut Vec<MaterializationWarning>,
    stats: &mut Vec<StageStat>,
) -> BTreeMap<String, Vec<(String, usize)>> {
    let mut ancestry: BTreeMap<String, Vec<(String, usize)>> = BTreeMap::new();

    for version_id in target_versions {
        let mut rows = Vec::new();
        let mut seen = BTreeSet::new();
        let mut current = version_id.clone();
        let mut depth = 0usize;

        loop {
            if !seen.insert(current.clone()) {
                warnings.push(MaterializationWarning {
                    code: "version_inheritance_cycle".to_string(),
                    message: format!(
                        "cycle detected while resolving ancestry for version '{}'",
                        version_id
                    ),
                });
                break;
            }

            rows.push((current.clone(), depth));
            let next = data
                .version_descriptors
                .get(&current)
                .and_then(|descriptor| descriptor.snapshot.inherits_from_version_id.clone());

            let Some(next) = next else {
                break;
            };

            if depth >= 64 {
                warnings.push(MaterializationWarning {
                    code: "version_inheritance_depth_limit".to_string(),
                    message: format!(
                        "ancestry depth exceeded limit while resolving version '{}'",
                        version_id
                    ),
                });
                break;
            }

            current = next;
            depth += 1;
        }

        ancestry.insert(version_id.clone(), rows);
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
        for (ancestor_id, depth) in ancestry {
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
                        inherited_from_version_id: if *depth == 0 {
                            None
                        } else {
                            Some(ancestor_id.clone())
                        },
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

fn build_writes(final_state: &[FinalStateRow]) -> Vec<MaterializationWrite> {
    let mut writes = Vec::new();
    for row in final_state {
        let op = if row.source.snapshot_content.is_some() {
            MaterializationWriteOp::Upsert
        } else {
            MaterializationWriteOp::Tombstone
        };
        writes.push(MaterializationWrite {
            schema_key: row.source.schema_key.clone(),
            entity_id: row.source.entity_id.clone(),
            file_id: row.source.file_id.clone(),
            version_id: row.version_id.clone(),
            inherited_from_version_id: row.inherited_from_version_id.clone(),
            op,
            snapshot_content: row.source.snapshot_content.clone(),
            schema_version: row.source.schema_version.clone(),
            plugin_key: row.source.plugin_key.clone(),
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

    writes
}

fn build_debug_trace(
    req: &MaterializationRequest,
    version_pointers: &BTreeMap<String, Vec<String>>,
    commit_graph: &BTreeMap<(String, String), usize>,
    all_commit_edges: &BTreeSet<(String, String)>,
    version_ancestry: &BTreeMap<String, Vec<(String, usize)>>,
    latest_visible_state: &[VisibleRow],
    final_state: &[FinalStateRow],
) -> Option<MaterializationDebugTrace> {
    if matches!(req.debug, MaterializationDebugMode::Off) {
        return None;
    }

    let limit = req.debug_row_limit.max(1);

    let mut tips_by_version = Vec::new();
    for (version_id, tips) in version_pointers {
        for tip in tips {
            tips_by_version.push(VersionPointerDebugRow {
                version_id: version_id.clone(),
                tip_commit_id: tip.clone(),
            });
        }
    }

    let mut traversed_commits = Vec::new();
    for ((version_id, commit_id), depth) in commit_graph {
        traversed_commits.push(TraversedCommitDebugRow {
            version_id: version_id.clone(),
            commit_id: commit_id.clone(),
            depth: *depth,
        });
    }

    let mut traversed_edges = Vec::new();
    for (parent_id, child_id) in all_commit_edges {
        for (version_id, tips) in version_pointers {
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
                version_id: version_id.clone(),
                ancestor_version_id: ancestor_version_id.clone(),
                inheritance_depth: *inheritance_depth,
            });
        }
    }

    let latest_visible_winners = if matches!(req.debug, MaterializationDebugMode::Full) {
        latest_visible_state
            .iter()
            .map(|row| LatestVisibleWinnerDebugRow {
                version_id: row.version_id.clone(),
                entity_id: row.entity_id.clone(),
                schema_key: row.schema_key.clone(),
                file_id: row.file_id.clone(),
                commit_id: row.commit_id.clone(),
                change_id: row.change_id.clone(),
            })
            .take(limit)
            .collect()
    } else {
        Vec::new()
    };

    let inheritance_winners = if matches!(req.debug, MaterializationDebugMode::Full) {
        final_state
            .iter()
            .map(|row| InheritanceWinnerDebugRow {
                version_id: row.version_id.clone(),
                entity_id: row.source.entity_id.clone(),
                schema_key: row.source.schema_key.clone(),
                file_id: row.source.file_id.clone(),
                inherited_from_version_id: row.inherited_from_version_id.clone(),
                change_id: row.source.change_id.clone(),
            })
            .take(limit)
            .collect()
    } else {
        Vec::new()
    };

    Some(MaterializationDebugTrace {
        tips_by_version: tips_by_version.into_iter().take(limit).collect(),
        traversed_commits: traversed_commits.into_iter().take(limit).collect(),
        traversed_edges: traversed_edges.into_iter().take(limit).collect(),
        version_ancestry: ancestry_rows.into_iter().take(limit).collect(),
        latest_visible_winners,
        inheritance_winners,
    })
}
