use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use tracing::Instrument as _;

use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchReferenceRole};
use crate::changelog::ChangeRecordProjection;
use crate::entity_pk::EntityPk;
use crate::filesystem::DERIVED_FILE_REF_SCHEMA_KEY;
use crate::plugin::{
    PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginFileOwner, PluginRegistry, PluginRegistryEntry,
    inferred_media_type_for_path,
};
use crate::storage_adapter::Storage;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateKey, TrackedStateMergeConflict,
    TrackedStateStoreReader,
};
use crate::transaction::types::{
    TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteRow,
};

use super::analysis::{MergeCommits, MergeOutcome, analyze};
use super::conflicts::{
    MergeConflict as AnalysisMergeConflict,
    MergeConflictChangeKind as AnalysisMergeConflictChangeKind,
    MergeConflictKind as AnalysisMergeConflictKind, MergeConflictSide as AnalysisMergeConflictSide,
};
use super::stats::MergeStats;
use crate::common::{compose_directory_path, compose_file_path};
use crate::session::context::SessionContext;
use crate::tracked_state::TrackedStateMergePick;
use crate::transaction::types::StagedCommitChangeRef;
use crate::wasm::{
    WasmByteSource, WasmChangeEffect, WasmConflictResolution, WasmConflictTake, WasmEntityConflict,
    WasmEntityKey, WasmFileDescriptor, WasmHostBytes, WasmPluginSelection, WasmSourceRange,
    WasmSourceSlice,
};

/// Options for merging another branch into this session's active branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeBranchOptions {
    /// Branch whose changes should be merged into the active session branch.
    pub source_branch_id: String,
}

/// Options for previewing a merge from another branch into this session's
/// active branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeBranchPreviewOptions {
    /// Branch whose changes would be merged into the active session branch.
    pub source_branch_id: String,
}

/// Receipt returned after merging a branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeBranchReceipt {
    pub outcome: MergeBranchOutcome,
    pub target_branch_id: String,
    pub source_branch_id: String,
    pub base_commit_id: String,
    pub target_head_before_commit_id: String,
    pub source_head_before_commit_id: String,
    pub target_head_after_commit_id: String,
    pub created_merge_commit_id: Option<String>,
    pub change_stats: MergeChangeStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MergeChangeStats {
    pub total: usize,
    pub added: usize,
    pub modified: usize,
    pub removed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeBranchPreview {
    pub outcome: MergeBranchOutcome,
    pub target_branch_id: String,
    pub source_branch_id: String,
    pub base_commit_id: String,
    pub target_head_commit_id: String,
    pub source_head_commit_id: String,
    pub change_stats: MergeChangeStats,
    pub conflicts: Vec<MergeConflict>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeConflict {
    pub kind: MergeConflictKind,
    pub schema_key: String,
    pub entity_pk: JsonValue,
    pub file_id: Option<String>,
    pub target: MergeConflictSide,
    pub source: MergeConflictSide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeConflictKind {
    SameEntityChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeConflictSide {
    pub kind: MergeConflictChangeKind,
    pub before_change_id: Option<String>,
    pub after_change_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeConflictChangeKind {
    Added,
    Modified,
    Removed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeBranchOutcome {
    AlreadyUpToDate,
    FastForward,
    MergeCommitted,
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Previews merging `source_branch_id` into this session's active branch
    /// without advancing refs, staging changes, or creating commits.
    pub async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> Result<MergeBranchPreview, LixError> {
        let source_branch_id = options.source_branch_id;

        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                let active_branch_id = transaction.active_branch_id().to_string();
                if source_branch_id == active_branch_id {
                    return Err(LixError::invalid_self_merge(active_branch_id));
                }

                let (target_head, source_head) = {
                    let reader = transaction.branch_ref_reader().await;
                    let lifecycle = BranchLifecycle::new(&reader);
                    let target_head = lifecycle
                        .require_existing_commit_id(
                            &active_branch_id,
                            BranchOperation::MergeBranchPreview,
                            BranchReferenceRole::Target,
                        )
                        .await?;
                    let source_head = lifecycle
                        .require_existing_commit_id(
                            &source_branch_id,
                            BranchOperation::MergeBranchPreview,
                            BranchReferenceRole::Source,
                        )
                        .await?;
                    (target_head, source_head)
                };

                let merge_base = {
                    let mut reader = transaction.commit_graph_reader().await;
                    reader.merge_base(&target_head, &source_head).await?
                };

                let analysis = {
                    let mut reader = transaction.tracked_state_reader().await;
                    analyze(
                        &mut reader,
                        MergeCommits {
                            base_commit_id: merge_base,
                            target_commit_id: target_head,
                            source_commit_id: source_head,
                        },
                    )
                    .await?
                };
                let derived_blob_files = {
                    let mut reader = transaction.tracked_state_reader().await;
                    derived_plugin_blob_conflicts(&mut reader, &analysis).await?
                };

                let resolvable_plugin_conflicts = {
                    let mut reader = transaction.tracked_state_reader().await;
                    resolvable_plugin_conflict_keys(&mut reader, &analysis, &derived_blob_files)
                        .await?
                };

                Ok(preview_from_analysis(
                    &active_branch_id,
                    &source_branch_id,
                    &analysis,
                    &derived_blob_files,
                    &resolvable_plugin_conflicts,
                ))
            })
        })
        .await
    }

    /// Merges `source_branch_id` into this session's active branch.
    ///
    /// The generated target commit keeps the previous target head as its first
    /// parent and records the source head as an additional parent, so the
    /// commit graph preserves branch ancestry while tracked-state storage
    /// selects the planned source changes into the new target root.
    pub async fn merge_branch(
        &self,
        options: MergeBranchOptions,
    ) -> Result<MergeBranchReceipt, LixError> {
        let source_branch_id = options.source_branch_id;

        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                let active_branch_id = transaction.active_branch_id().to_string();
                if source_branch_id == active_branch_id {
                    return Err(LixError::invalid_self_merge(active_branch_id));
                }

                let (target_head, source_head) = async {
                    let reader = transaction.branch_ref_reader().await;
                    let lifecycle = BranchLifecycle::new(&reader);
                    let target_head = lifecycle
                        .require_existing_commit_id(
                            &active_branch_id,
                            BranchOperation::MergeBranch,
                            BranchReferenceRole::Target,
                        )
                        .await?;
                    let source_head = lifecycle
                        .require_existing_commit_id(
                            &source_branch_id,
                            BranchOperation::MergeBranch,
                            BranchReferenceRole::Source,
                        )
                        .await?;
                    Ok::<_, LixError>((target_head, source_head))
                }
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.merge_branch_refs"
                ))
                .await?;

                let merge_base = async {
                    let mut reader = transaction.commit_graph_reader().await;
                    reader.merge_base(&target_head, &source_head).await
                }
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.merge_base"
                ))
                .await?;
                let base_commit_id = merge_base;
                let analysis = async {
                    let mut reader = transaction.tracked_state_reader().await;
                    analyze(
                        &mut reader,
                        MergeCommits {
                            base_commit_id,
                            target_commit_id: target_head,
                            source_commit_id: source_head,
                        },
                    )
                    .await
                }
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.merge_analysis"
                ))
                .await?;
                let derived_blob_files = async {
                    let mut reader = transaction.tracked_state_reader().await;
                    derived_plugin_blob_conflicts(&mut reader, &analysis).await
                }
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.merge_derived_blob_detection"
                ))
                .await?;

                if analysis.outcome == MergeOutcome::AlreadyUpToDate {
                    return Ok(MergeBranchReceipt {
                        outcome: MergeBranchOutcome::AlreadyUpToDate,
                        target_branch_id: active_branch_id,
                        source_branch_id,
                        base_commit_id: analysis.commits.base_commit_id.to_string(),
                        target_head_after_commit_id: analysis.commits.target_commit_id.to_string(),
                        target_head_before_commit_id: analysis.commits.target_commit_id.to_string(),
                        source_head_before_commit_id: analysis.commits.source_commit_id.to_string(),
                        created_merge_commit_id: None,
                        change_stats: merge_change_stats_from_analysis(&analysis.stats),
                    });
                }

                if analysis.outcome == MergeOutcome::FastForward {
                    transaction
                        .advance_branch_ref(&active_branch_id, analysis.commits.source_commit_id)
                        .await?;

                    return Ok(MergeBranchReceipt {
                        outcome: MergeBranchOutcome::FastForward,
                        target_branch_id: active_branch_id,
                        source_branch_id,
                        base_commit_id: analysis.commits.base_commit_id.to_string(),
                        target_head_before_commit_id: analysis.commits.target_commit_id.to_string(),
                        source_head_before_commit_id: analysis.commits.source_commit_id.to_string(),
                        target_head_after_commit_id: analysis.commits.source_commit_id.to_string(),
                        created_merge_commit_id: None,
                        change_stats: merge_change_stats_from_analysis(&analysis.stats),
                    });
                }

                let merge_plan = analysis
                    .merge_plan()
                    .expect("merge analysis should include a plan for mergeCommitted");

                // Do the no-Wasm compatibility preflight and reject ordinary
                // conflicts before constructing a Component Store. A merge
                // with an unrelated unresolved row must not pay for (or be
                // masked by) a plugin resolver invocation.
                let resolvable_plugin_conflicts = async {
                    let mut reader = transaction.tracked_state_reader().await;
                    resolvable_plugin_conflict_keys(&mut reader, &analysis, &derived_blob_files)
                        .await
                }
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.merge_plugin_conflict_preflight"
                ))
                .await?;
                let effective_conflicts = analysis
                    .conflicts
                    .iter()
                    .filter(|conflict| {
                        !is_derived_blob_conflict(conflict, &derived_blob_files)
                            && !is_resolvable_plugin_semantic_conflict(
                                conflict,
                                &resolvable_plugin_conflicts,
                            )
                    })
                    .collect::<Vec<_>>();
                if !effective_conflicts.is_empty() {
                    return Err(merge_conflict_error(
                        &effective_conflicts
                            .into_iter()
                            .map(merge_conflict_from_analysis)
                            .collect::<Vec<_>>(),
                    )?);
                }

                let plugin_conflict_groups = async {
                    let mut reader = transaction.tracked_state_reader().await;
                    plugin_merge_conflict_groups(
                        &mut reader,
                        &analysis,
                        &derived_blob_files,
                        &resolvable_plugin_conflicts,
                    )
                    .await
                }
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.merge_plugin_conflict_inputs"
                ))
                .await?;
                let resolved_plugin_rows = resolve_plugin_merge_conflict_groups(
                    transaction,
                    plugin_conflict_groups,
                    &active_branch_id,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.merge_plugin_conflict_resolve"
                ))
                .await?;

                let semantic_rows = async {
                    let mut reader = transaction.tracked_state_reader().await;
                    materialized_plugin_merge_rows(
                        &mut reader,
                        &analysis,
                        &derived_blob_files,
                        &active_branch_id,
                        resolved_plugin_rows,
                    )
                    .await
                }
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.merge_materialized_rows"
                ))
                .await?;
                if !semantic_rows.is_empty() {
                    transaction
                        .stage_write(TransactionWrite::Rows {
                            mode: TransactionWriteMode::Replace,
                            rows: semantic_rows,
                        })
                        .instrument(tracing::debug_span!(
                            target: "lix_perf",
                            "lix.perf.merge_stage_semantic_rows"
                        ))
                        .await?;
                }
                let created_merge_commit_id = tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.merge_stage_commit"
                )
                .in_scope(|| {
                    let selected_changes = merge_plan
                        .picks
                        .iter()
                        .filter(|pick| !pick_is_derived_plugin_state(pick, &derived_blob_files))
                        .map(selected_change_from_merge_pick)
                        .collect::<Vec<_>>();
                    transaction.stage_merge_commit(
                        active_branch_id.clone(),
                        analysis.commits.source_commit_id,
                        selected_changes,
                    )
                })?;
                Ok(MergeBranchReceipt {
                    outcome: MergeBranchOutcome::MergeCommitted,
                    target_branch_id: active_branch_id,
                    source_branch_id,
                    base_commit_id: analysis.commits.base_commit_id.to_string(),
                    target_head_after_commit_id: created_merge_commit_id.clone(),
                    target_head_before_commit_id: analysis.commits.target_commit_id.to_string(),
                    source_head_before_commit_id: analysis.commits.source_commit_id.to_string(),
                    created_merge_commit_id: Some(created_merge_commit_id),
                    change_stats: merge_change_stats_from_analysis(&analysis.stats),
                })
            })
        })
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.merge_branch_total"
        ))
        .await
    }
}

fn selected_change_from_merge_pick(pick: &TrackedStateMergePick) -> StagedCommitChangeRef {
    StagedCommitChangeRef {
        schema_key: pick.selected_row.schema_key.clone(),
        file_id: pick.selected_row.file_id.clone(),
        entity_pk: pick.selected_row.entity_pk.clone(),
        change_id: pick.change_id,
        deleted: pick.selected_row.deleted,
        created_at: pick.selected_row.created_at,
        updated_at: pick.selected_row.updated_at,
    }
}

const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

async fn derived_plugin_blob_conflicts<S>(
    reader: &mut TrackedStateStoreReader<S>,
    analysis: &super::analysis::MergeAnalysis,
) -> Result<BTreeMap<String, PluginFileOwner>, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    // A derived blob conflict is the common signal, but it is not the
    // authority for plugin ownership. Start from every conflicted file and
    // prove one *live, identical* owner across all three historical roots.
    //
    // A missing/tombstoned owner is a file-lifecycle conflict (for example,
    // delete-vs-edit), not a semantic entity conflict. Letting a resolver
    // choose an entity value in that case could silently pair a live semantic
    // row with a deleted file owner. Keep the whole conflict visible until a
    // first-class lifecycle conflict model exists.
    let file_ids = analysis
        .conflicts
        .iter()
        .filter_map(|conflict| conflict.file_id.clone())
        .collect::<BTreeSet<_>>();
    if file_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let owner_keys = file_ids
        .iter()
        .map(|file_id| TrackedStateKey {
            schema_key: "lix_key_value".to_owned(),
            file_id: Some(file_id.clone()),
            entity_pk: EntityPk::single(PLUGIN_OWNER_KEY),
        })
        .collect::<Vec<_>>();
    let base_rows = reader
        .load_projected_rows_at_commit(
            &analysis.commits.base_commit_id.to_string(),
            &owner_keys,
            &ChangeRecordProjection::full(),
        )
        .await?;
    let target_rows = reader
        .load_projected_rows_at_commit(
            &analysis.commits.target_commit_id.to_string(),
            &owner_keys,
            &ChangeRecordProjection::full(),
        )
        .await?;
    let source_rows = reader
        .load_projected_rows_at_commit(
            &analysis.commits.source_commit_id.to_string(),
            &owner_keys,
            &ChangeRecordProjection::full(),
        )
        .await?;
    let mut common_owners = BTreeMap::new();
    for (index, file_id) in file_ids.into_iter().enumerate() {
        let Some(owner) = common_live_plugin_owner(
            base_rows[index].as_ref(),
            target_rows[index].as_ref(),
            source_rows[index].as_ref(),
        )?
        else {
            continue;
        };
        common_owners.insert(file_id, owner);
    }
    if common_owners.is_empty() {
        return Ok(BTreeMap::new());
    }

    // Semantic resolution regenerates the derived blob through the target
    // transaction's descriptor and component generation. Therefore it is
    // safe only when the complete file identity is common at all three roots:
    // the descriptor and ancestor path, plus the pinned registry entry. A
    // source-only rename may otherwise render CSV bytes while the merge
    // selects TSV metadata; a source-only compatible plugin upgrade may
    // otherwise render with the target component while committing the source
    // registry entry. Leave every such case as an ordinary conflict until
    // file-lifecycle and generation conflicts have first-class values.
    let candidate_file_ids = common_owners.keys().cloned().collect::<BTreeSet<_>>();
    let common_descriptors =
        historical_conflict_file_descriptors(reader, analysis, &candidate_file_ids).await?;
    let registry_key = TrackedStateKey {
        schema_key: "lix_key_value".to_owned(),
        file_id: None,
        entity_pk: EntityPk::single(PLUGIN_REGISTRY_KEY),
    };
    let base_registry = load_historical_plugin_registry(
        reader,
        &analysis.commits.base_commit_id.to_string(),
        &registry_key,
    )
    .await?;
    let target_registry = load_historical_plugin_registry(
        reader,
        &analysis.commits.target_commit_id.to_string(),
        &registry_key,
    )
    .await?;
    let source_registry = load_historical_plugin_registry(
        reader,
        &analysis.commits.source_commit_id.to_string(),
        &registry_key,
    )
    .await?;

    let mut derived = BTreeMap::new();
    for (file_id, owner) in common_owners {
        if common_descriptors
            .get(&file_id)
            .is_some_and(|(path, _)| path.is_some())
            && pinned_conflict_plugin_entry(
                &owner,
                &base_registry,
                &target_registry,
                &source_registry,
                &file_id,
            )
            .is_ok()
        {
            derived.insert(file_id, owner);
        }
    }
    Ok(derived)
}

/// A semantic resolver can run only within a single live file incarnation.
/// `PluginFileOwner::change_id` is the immutable incarnation key used by the
/// actor cache and ID namespace. Equal owner payloads alone are not enough:
/// a delete/recreate may intentionally reuse a file ID and plugin key.
fn common_live_plugin_owner(
    base: Option<&MaterializedTrackedStateRow>,
    target: Option<&MaterializedTrackedStateRow>,
    source: Option<&MaterializedTrackedStateRow>,
) -> Result<Option<PluginFileOwner>, LixError> {
    let Some(base) = base.filter(|row| !row.deleted) else {
        return Ok(None);
    };
    let Some(target) = target.filter(|row| !row.deleted) else {
        return Ok(None);
    };
    let Some(source) = source.filter(|row| !row.deleted) else {
        return Ok(None);
    };
    let Some(base_owner) = PluginFileOwner::from_tracked_state_row(base)? else {
        return Ok(None);
    };
    let Some(target_owner) = PluginFileOwner::from_tracked_state_row(target)? else {
        return Ok(None);
    };
    let Some(source_owner) = PluginFileOwner::from_tracked_state_row(source)? else {
        return Ok(None);
    };
    if base_owner == target_owner
        && base_owner == source_owner
        && base.change_id == target.change_id
        && base.change_id == source.change_id
    {
        Ok(Some(base_owner))
    } else {
        Ok(None)
    }
}

fn is_derived_blob_conflict(
    conflict: &AnalysisMergeConflict,
    derived_blob_files: &BTreeMap<String, PluginFileOwner>,
) -> bool {
    matches!(
        conflict.schema_key.as_str(),
        BLOB_REF_SCHEMA_KEY | DERIVED_FILE_REF_SCHEMA_KEY
    ) && conflict
        .file_id
        .as_ref()
        .is_some_and(|file_id| derived_blob_files.contains_key(file_id))
}

fn pick_is_derived_plugin_state(
    pick: &TrackedStateMergePick,
    derived_blob_files: &BTreeMap<String, PluginFileOwner>,
) -> bool {
    let Some(file_id) = pick.selected_row.file_id.as_ref() else {
        return false;
    };
    let Some(owner) = derived_blob_files.get(file_id) else {
        return false;
    };
    matches!(
        pick.selected_row.schema_key.as_str(),
        BLOB_REF_SCHEMA_KEY | DERIVED_FILE_REF_SCHEMA_KEY
    ) || owner
        .schema_keys()
        .iter()
        .any(|schema_key| schema_key == &pick.selected_row.schema_key)
}

/// One historical triple for a plugin-owned semantic entity. The row identity
/// remains host-owned; a Component can only choose or replace the aligned
/// value and cannot invent a different key during a merge.
#[derive(Debug, Clone)]
struct PluginMergeConflictRow {
    key: TrackedStateKey,
    base: Option<Arc<MaterializedTrackedStateRow>>,
    a: Option<Arc<MaterializedTrackedStateRow>>,
    b: Option<Arc<MaterializedTrackedStateRow>>,
}

#[derive(Debug, Clone)]
struct PluginMergeConflictGroup {
    plugin: PluginRegistryEntry,
    descriptor: WasmFileDescriptor,
    conflicts: Vec<PluginMergeConflictRow>,
}

fn conflict_tracked_state_key(conflict: &TrackedStateMergeConflict) -> TrackedStateKey {
    TrackedStateKey {
        schema_key: conflict.identity.schema_key.clone(),
        file_id: conflict.identity.file_id.clone(),
        entity_pk: conflict.identity.entity_pk.clone(),
    }
}

fn is_resolvable_plugin_semantic_conflict(
    conflict: &AnalysisMergeConflict,
    resolvable_plugin_conflicts: &BTreeSet<TrackedStateKey>,
) -> bool {
    let Ok(entity_pk) = EntityPk::from_json_array_value(&conflict.entity_pk) else {
        // Merge analysis always generates this value from a validated
        // `EntityPk`; keep a malformed public projection visible rather than
        // accidentally suppressing a conflict.
        return false;
    };
    resolvable_plugin_conflicts.contains(&TrackedStateKey {
        schema_key: conflict.schema_key.clone(),
        file_id: conflict.file_id.clone(),
        entity_pk,
    })
}

/// Returns exactly the semantic conflict identities that can be handed to a
/// static resolver. This deliberately does not execute Wasm: callers use it
/// both to make merge preview honest and to reject ordinary conflicts before
/// allocating a Component Store.
async fn resolvable_plugin_conflict_keys<S>(
    reader: &mut TrackedStateStoreReader<S>,
    analysis: &super::analysis::MergeAnalysis,
    derived_blob_files: &BTreeMap<String, PluginFileOwner>,
) -> Result<BTreeSet<TrackedStateKey>, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    let Some(merge_plan) = analysis.merge_plan() else {
        return Ok(BTreeSet::new());
    };
    let semantic_conflicts = merge_plan
        .conflicts
        .iter()
        .filter(|conflict| {
            let Some(file_id) = conflict.identity.file_id.as_ref() else {
                return false;
            };
            derived_blob_files.get(file_id).is_some_and(|owner| {
                owner
                    .schema_keys()
                    .iter()
                    .any(|schema_key| schema_key == &conflict.identity.schema_key)
            })
        })
        .collect::<Vec<_>>();
    if semantic_conflicts.is_empty() {
        return Ok(BTreeSet::new());
    }

    let registry_key = TrackedStateKey {
        schema_key: "lix_key_value".to_owned(),
        file_id: None,
        entity_pk: EntityPk::single(PLUGIN_REGISTRY_KEY),
    };
    let base_registry = load_historical_plugin_registry(
        reader,
        &analysis.commits.base_commit_id.to_string(),
        &registry_key,
    )
    .await?;
    let target_registry = load_historical_plugin_registry(
        reader,
        &analysis.commits.target_commit_id.to_string(),
        &registry_key,
    )
    .await?;
    let source_registry = load_historical_plugin_registry(
        reader,
        &analysis.commits.source_commit_id.to_string(),
        &registry_key,
    )
    .await?;

    let mut eligible = BTreeSet::new();
    for conflict in semantic_conflicts {
        let file_id = conflict
            .identity
            .file_id
            .as_deref()
            .expect("semantic plugin conflicts have a file id");
        let owner = derived_blob_files
            .get(file_id)
            .expect("semantic plugin conflicts have a derived owner");
        // An unavailable, mismatched, or upgraded historical entry is not a
        // fatal preflight error. Leave that row visible as an ordinary merge
        // conflict in both preview and merge instead of claiming it was
        // resolved. Corrupt registry snapshots were rejected while loading.
        if pinned_conflict_plugin_entry(
            owner,
            &base_registry,
            &target_registry,
            &source_registry,
            file_id,
        )
        .is_ok()
        {
            eligible.insert(conflict_tracked_state_key(conflict));
        }
    }
    Ok(eligible)
}

async fn plugin_merge_conflict_groups<S>(
    reader: &mut TrackedStateStoreReader<S>,
    analysis: &super::analysis::MergeAnalysis,
    derived_blob_files: &BTreeMap<String, PluginFileOwner>,
    resolvable_plugin_conflicts: &BTreeSet<TrackedStateKey>,
) -> Result<Vec<PluginMergeConflictGroup>, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    let merge_plan = analysis
        .merge_plan()
        .expect("plugin conflict resolution requires a merge plan");
    let semantic_conflicts = merge_plan
        .conflicts
        .iter()
        .filter(|conflict| {
            resolvable_plugin_conflicts.contains(&conflict_tracked_state_key(conflict))
        })
        .collect::<Vec<_>>();
    if semantic_conflicts.is_empty() {
        return Ok(Vec::new());
    }

    let semantic_file_ids = semantic_conflicts
        .iter()
        .filter_map(|conflict| conflict.identity.file_id.clone())
        .collect::<BTreeSet<_>>();
    let historical_descriptors =
        historical_conflict_file_descriptors(reader, analysis, &semantic_file_ids).await?;

    let registry_key = TrackedStateKey {
        schema_key: "lix_key_value".to_owned(),
        file_id: None,
        entity_pk: EntityPk::single(PLUGIN_REGISTRY_KEY),
    };
    let base_registry = load_historical_plugin_registry(
        reader,
        &analysis.commits.base_commit_id.to_string(),
        &registry_key,
    )
    .await?;
    let target_registry = load_historical_plugin_registry(
        reader,
        &analysis.commits.target_commit_id.to_string(),
        &registry_key,
    )
    .await?;
    let source_registry = load_historical_plugin_registry(
        reader,
        &analysis.commits.source_commit_id.to_string(),
        &registry_key,
    )
    .await?;

    let keys = semantic_conflicts
        .iter()
        .map(|conflict| TrackedStateKey {
            schema_key: conflict.identity.schema_key.clone(),
            file_id: conflict.identity.file_id.clone(),
            entity_pk: conflict.identity.entity_pk.clone(),
        })
        .collect::<Vec<_>>();
    let base_rows = reader
        .load_projected_rows_at_commit(
            &analysis.commits.base_commit_id.to_string(),
            &keys,
            &ChangeRecordProjection::full(),
        )
        .await?;
    let target_rows = reader
        .load_projected_rows_at_commit(
            &analysis.commits.target_commit_id.to_string(),
            &keys,
            &ChangeRecordProjection::full(),
        )
        .await?;
    let source_rows = reader
        .load_projected_rows_at_commit(
            &analysis.commits.source_commit_id.to_string(),
            &keys,
            &ChangeRecordProjection::full(),
        )
        .await?;

    let mut groups = BTreeMap::<String, PluginMergeConflictGroup>::new();
    for ((((conflict, key), base), target), source) in semantic_conflicts
        .into_iter()
        .zip(keys)
        .zip(base_rows)
        .zip(target_rows)
        .zip(source_rows)
    {
        let file_id = key
            .file_id
            .as_ref()
            .expect("semantic plugin conflicts have a file id")
            .clone();
        let owner = derived_blob_files
            .get(&file_id)
            .expect("semantic plugin conflicts have a derived owner");
        let plugin = pinned_conflict_plugin_entry(
            owner,
            &base_registry,
            &target_registry,
            &source_registry,
            &file_id,
        )?;
        verify_historical_conflict_row(base.as_ref(), conflict.target.before.as_ref(), "base")?;
        verify_historical_conflict_row(target.as_ref(), conflict.target.after.as_ref(), "target")?;
        verify_historical_conflict_row(source.as_ref(), conflict.source.after.as_ref(), "source")?;

        let (a, b) = canonical_conflict_variants(conflict, target, source)?;
        let row = PluginMergeConflictRow {
            key,
            base: historical_live_row(base),
            a,
            b,
        };
        let (path, media_type) = historical_descriptors
            .get(&file_id)
            .cloned()
            .unwrap_or((None, None));
        let descriptor = WasmFileDescriptor {
            // The historical file descriptor is not resolver authority, but
            // a common path/media identity lets one plugin support multiple
            // formats without guessing from a semantic row. Rename-divergent
            // or unavailable descriptors deliberately remain `None`.
            path,
            media_type,
            plugin: WasmPluginSelection {
                plugin_key: plugin.key().to_owned(),
                generation: plugin.archive_blob_hash().to_owned(),
            },
        };
        match groups.get_mut(&file_id) {
            Some(group) => {
                if group.plugin != plugin || group.descriptor != descriptor {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!(
                            "plugin-owned file '{file_id}' has inconsistent historical resolver generations"
                        ),
                    ));
                }
                group.conflicts.push(row);
            }
            None => {
                groups.insert(
                    file_id,
                    PluginMergeConflictGroup {
                        plugin,
                        descriptor,
                        conflicts: vec![row],
                    },
                );
            }
        }
    }
    let mut groups = groups.into_values().collect::<Vec<_>>();
    // Merge-plan order is currently key-stable, but make the packet invariant
    // host-owned rather than coupling an untrusted resolver input to that
    // implementation detail.
    for group in &mut groups {
        group
            .conflicts
            .sort_by(|left, right| left.key.cmp(&right.key));
    }
    Ok(groups)
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct HistoricalFileDescriptor {
    id: String,
    directory_id: Option<String>,
    name: String,
}

#[derive(Debug, Deserialize)]
struct HistoricalDirectoryDescriptor {
    id: String,
    parent_id: Option<String>,
    name: String,
}

/// Loads a path only when the three historical roots agree on a live file
/// descriptor. A resolver must not receive a branch-direction-dependent path:
/// divergent renames remain ordinary merge conflicts and missing/corrupt
/// descriptor metadata simply leaves the optional descriptor fields empty.
async fn historical_conflict_file_descriptors<S>(
    reader: &mut TrackedStateStoreReader<S>,
    analysis: &super::analysis::MergeAnalysis,
    file_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, (Option<String>, Option<String>)>, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    if file_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let keys = file_ids
        .iter()
        .map(|file_id| TrackedStateKey {
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_owned(),
            file_id: None,
            entity_pk: EntityPk::single(file_id),
        })
        .collect::<Vec<_>>();
    let base_commit_id = analysis.commits.base_commit_id.to_string();
    let target_commit_id = analysis.commits.target_commit_id.to_string();
    let source_commit_id = analysis.commits.source_commit_id.to_string();
    let base_rows = reader
        .load_projected_rows_at_commit(&base_commit_id, &keys, &ChangeRecordProjection::full())
        .await?;
    let target_rows = reader
        .load_projected_rows_at_commit(&target_commit_id, &keys, &ChangeRecordProjection::full())
        .await?;
    let source_rows = reader
        .load_projected_rows_at_commit(&source_commit_id, &keys, &ChangeRecordProjection::full())
        .await?;

    let mut descriptors = BTreeMap::new();
    for (((file_id, base), target), source) in file_ids
        .iter()
        .cloned()
        .zip(base_rows)
        .zip(target_rows)
        .zip(source_rows)
    {
        let Some((scope_file_id, descriptor)) =
            common_historical_file_descriptor(&file_id, base, target, source)
        else {
            descriptors.insert(file_id, (None, None));
            continue;
        };

        // A file descriptor can agree while one ancestor directory has been
        // renamed. Resolve all three full paths at their own historical roots
        // and only expose a path to the plugin when it is genuinely common.
        // A path-sensitive resolver must never receive a stale base path.
        let base_path = historical_file_path(
            reader,
            &base_commit_id,
            scope_file_id.as_deref(),
            &descriptor,
        )
        .await?;
        let target_path = historical_file_path(
            reader,
            &target_commit_id,
            scope_file_id.as_deref(),
            &descriptor,
        )
        .await?;
        let source_path = historical_file_path(
            reader,
            &source_commit_id,
            scope_file_id.as_deref(),
            &descriptor,
        )
        .await?;
        let Some(path) = common_historical_path(base_path, target_path, source_path) else {
            descriptors.insert(file_id, (None, None));
            continue;
        };
        let media_type = inferred_media_type_for_path(Some(&path)).map(str::to_owned);
        descriptors.insert(file_id, (Some(path), media_type));
    }
    Ok(descriptors)
}

fn historical_file_descriptor_row(
    row: Option<&MaterializedTrackedStateRow>,
    expected_file_id: &str,
) -> Option<(Option<String>, HistoricalFileDescriptor)> {
    let row = row.filter(|row| !row.deleted)?;
    let snapshot = row.snapshot_content.as_deref()?;
    let descriptor = serde_json::from_str::<HistoricalFileDescriptor>(snapshot).ok()?;
    (descriptor.id == expected_file_id).then_some((row.file_id.clone(), descriptor))
}

fn common_historical_file_descriptor(
    expected_file_id: &str,
    base: Option<MaterializedTrackedStateRow>,
    target: Option<MaterializedTrackedStateRow>,
    source: Option<MaterializedTrackedStateRow>,
) -> Option<(Option<String>, HistoricalFileDescriptor)> {
    let base = historical_file_descriptor_row(base.as_ref(), expected_file_id)?;
    let target = historical_file_descriptor_row(target.as_ref(), expected_file_id)?;
    let source = historical_file_descriptor_row(source.as_ref(), expected_file_id)?;
    (base == target && base == source).then_some(base)
}

fn common_historical_path(
    base: Option<String>,
    target: Option<String>,
    source: Option<String>,
) -> Option<String> {
    (base == target && base == source).then_some(base).flatten()
}

async fn historical_file_path<S>(
    reader: &mut TrackedStateStoreReader<S>,
    commit_id: &str,
    scope_file_id: Option<&str>,
    descriptor: &HistoricalFileDescriptor,
) -> Result<Option<String>, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    let mut ancestor_names = Vec::new();
    let mut directory_id = descriptor.directory_id.clone();
    let mut visited = BTreeSet::new();
    while let Some(id) = directory_id {
        if !visited.insert(id.clone()) {
            return Ok(None);
        }
        let key = TrackedStateKey {
            schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_owned(),
            file_id: scope_file_id.map(str::to_owned),
            entity_pk: EntityPk::single(&id),
        };
        let row = reader
            .load_projected_rows_at_commit(
                commit_id,
                std::slice::from_ref(&key),
                &ChangeRecordProjection::full(),
            )
            .await?
            .into_iter()
            .next()
            .flatten();
        let Some(row) = row.filter(|row| !row.deleted) else {
            return Ok(None);
        };
        let Some(snapshot) = row.snapshot_content.as_deref() else {
            return Ok(None);
        };
        let Ok(directory) = serde_json::from_str::<HistoricalDirectoryDescriptor>(snapshot) else {
            return Ok(None);
        };
        if directory.id != id {
            return Ok(None);
        }
        ancestor_names.push(directory.name);
        directory_id = directory.parent_id;
    }

    let mut parent_path = None;
    for name in ancestor_names.iter().rev() {
        let Ok(path) = compose_directory_path(parent_path.as_deref(), name) else {
            return Ok(None);
        };
        parent_path = Some(path);
    }
    Ok(compose_file_path(parent_path.as_deref(), &descriptor.name).ok())
}

async fn load_historical_plugin_registry<S>(
    reader: &mut TrackedStateStoreReader<S>,
    commit_id: &str,
    registry_key: &TrackedStateKey,
) -> Result<PluginRegistry, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    let rows = reader
        .load_projected_rows_at_commit(
            commit_id,
            std::slice::from_ref(registry_key),
            &ChangeRecordProjection::full(),
        )
        .await?;
    let row = rows.into_iter().next().flatten();
    let snapshot = match row {
        None => None,
        Some(row) if row.deleted || row.snapshot_content.is_none() => None,
        Some(row) => Some(
            serde_json::from_str(row.snapshot_content.as_deref().expect("checked")).map_err(
                |error| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!("historical plugin registry snapshot is invalid JSON: {error}"),
                    )
                },
            )?,
        ),
    };
    PluginRegistry::from_optional_snapshot(snapshot.as_ref())
}

fn pinned_conflict_plugin_entry(
    owner: &PluginFileOwner,
    base: &PluginRegistry,
    target: &PluginRegistry,
    source: &PluginRegistry,
    file_id: &str,
) -> Result<PluginRegistryEntry, LixError> {
    let target_entry = target.plugin(owner.plugin_key()).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!(
                "plugin-owned file '{}' references missing target plugin '{}'",
                file_id,
                owner.plugin_key()
            ),
        )
    })?;
    let base_entry = base.plugin(owner.plugin_key()).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!(
                "plugin-owned file '{}' has no base generation for plugin '{}'",
                file_id,
                owner.plugin_key()
            ),
        )
    })?;
    let source_entry = source.plugin(owner.plugin_key()).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!(
                "plugin-owned file '{}' references missing source plugin '{}'",
                file_id,
                owner.plugin_key()
            ),
        )
    })?;
    if target_entry != base_entry || target_entry != source_entry {
        return Err(LixError::new(
            LixError::CODE_MERGE_CONFLICT,
            format!("plugin-owned file '{file_id}' changed resolver generation across the merge"),
        )
        .with_hint(
            "merge the plugin generation change before merging semantic edits for this file",
        ));
    }
    if target_entry.schema_keys() != owner.schema_keys() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!(
                "plugin-owned file '{}' schema set does not match plugin '{}'",
                file_id,
                owner.plugin_key()
            ),
        ));
    }
    Ok(target_entry.clone())
}

fn verify_historical_conflict_row(
    row: Option<&MaterializedTrackedStateRow>,
    expected: Option<&crate::tracked_state::TrackedStateDiffRow>,
    side: &str,
) -> Result<(), LixError> {
    match (row, expected) {
        (None, None) => Ok(()),
        (Some(row), Some(expected)) if row.change_id == expected.change_id => Ok(()),
        _ => Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("historical {side} row did not match merge analysis"),
        )),
    }
}

fn historical_live_row(
    row: Option<MaterializedTrackedStateRow>,
) -> Option<Arc<MaterializedTrackedStateRow>> {
    row.filter(|row| !row.deleted).map(Arc::new)
}

fn canonical_conflict_variants(
    conflict: &TrackedStateMergeConflict,
    target: Option<MaterializedTrackedStateRow>,
    source: Option<MaterializedTrackedStateRow>,
) -> Result<
    (
        Option<Arc<MaterializedTrackedStateRow>>,
        Option<Arc<MaterializedTrackedStateRow>>,
    ),
    LixError,
> {
    let target_after = conflict.target.after.as_ref().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "merge conflict target side omitted its resulting row",
        )
    })?;
    let source_after = conflict.source.after.as_ref().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "merge conflict source side omitted its resulting row",
        )
    })?;
    let ordering = target_after
        .updated_at
        .cmp(&source_after.updated_at)
        .then_with(|| target_after.change_id.cmp(&source_after.change_id));
    if ordering.is_eq() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "distinct merge conflict sides share the same durable ordering key",
        ));
    }
    let target = historical_live_row(target);
    let source = historical_live_row(source);
    if ordering.is_lt() {
        Ok((target, source))
    } else {
        Ok((source, target))
    }
}

fn conflict_host_snapshot(
    row: Option<&Arc<MaterializedTrackedStateRow>>,
) -> Result<Option<WasmHostBytes>, LixError> {
    let Some(row) = row else {
        return Ok(None);
    };
    let snapshot = row.snapshot_content.as_ref().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            "live plugin semantic row is missing its complete snapshot",
        )
    })?;
    let length = u64::try_from(snapshot.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            "plugin semantic snapshot length exceeds Wasm source bounds",
        )
    })?;
    // Keep the historical row's owned String alive behind the source instead
    // of cloning its bytes into an intermediate Blob. The common `take(b)`
    // path then moves no multi-megabyte semantic snapshot through either guest
    // memory or a host-side staging copy; `read` copies only when a heuristic
    // explicitly asks for that range.
    let source = Arc::new(TrackedRowSnapshotSource {
        row: Arc::clone(row),
    });
    Ok(Some(WasmHostBytes::Source(WasmSourceSlice {
        source,
        range: WasmSourceRange { offset: 0, length },
    })))
}

#[derive(Debug)]
struct TrackedRowSnapshotSource {
    row: Arc<MaterializedTrackedStateRow>,
}

impl WasmByteSource for TrackedRowSnapshotSource {
    fn len(&self) -> u64 {
        self.row
            .snapshot_content
            .as_ref()
            .map_or(0, |snapshot| snapshot.len() as u64)
    }

    fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>, LixError> {
        if length == 0 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "v2 byte-source reads must request bytes",
            ));
        }
        let snapshot = self.row.snapshot_content.as_deref().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                "live plugin semantic row is missing its complete snapshot",
            )
        })?;
        let start = usize::try_from(offset).map_err(|_| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "v2 byte-source offset does not fit this host",
            )
        })?;
        let end = start.checked_add(length as usize).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "v2 byte-source range overflowed",
            )
        })?;
        let bytes = snapshot.as_bytes();
        if end > bytes.len() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "v2 byte-source range exceeds its snapshot",
            ));
        }
        Ok(bytes[start..end].to_vec())
    }
}

async fn resolve_plugin_merge_conflict_groups<StorageImpl>(
    transaction: &mut crate::transaction::Transaction<StorageImpl>,
    groups: Vec<PluginMergeConflictGroup>,
    target_branch_id: &str,
) -> Result<Vec<TransactionWriteRow>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut rows = Vec::new();
    for group in groups {
        let conflicts = group
            .conflicts
            .iter()
            .enumerate()
            .map(|(ordinal, conflict)| {
                Ok(WasmEntityConflict {
                    ordinal: u32::try_from(ordinal).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            "plugin conflict batch exceeds the u32 ordinal limit",
                        )
                    })?,
                    key: WasmEntityKey {
                        schema_key: conflict.key.schema_key.clone(),
                        entity_pk: conflict.key.entity_pk.clone().into_parts(),
                    },
                    base: conflict_host_snapshot(conflict.base.as_ref())?,
                    a: conflict_host_snapshot(conflict.a.as_ref())?,
                    b: conflict_host_snapshot(conflict.b.as_ref())?,
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let resolved = transaction
            .resolve_v2_plugin_conflicts(&group.plugin, group.descriptor, conflicts)
            .await?;
        if resolved.resolutions.len() != group.conflicts.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "validated plugin conflict output lost input alignment",
            ));
        }
        for (conflict, resolution) in group.conflicts.iter().zip(resolved.resolutions) {
            rows.push(transaction_row_from_conflict_resolution(
                conflict,
                resolution,
                target_branch_id,
            )?);
        }
    }
    Ok(rows)
}

fn transaction_row_from_conflict_resolution(
    conflict: &PluginMergeConflictRow,
    resolution: WasmConflictResolution<WasmHostBytes>,
    target_branch_id: &str,
) -> Result<TransactionWriteRow, LixError> {
    match resolution {
        WasmConflictResolution::Take(side) => {
            let (side_name, selected) = match side {
                WasmConflictTake::Base => ("base", &conflict.base),
                WasmConflictTake::A => ("a", &conflict.a),
                WasmConflictTake::B => ("b", &conflict.b),
            };
            let row = selected.as_deref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "plugin conflict resolver selected absent {side_name} snapshot; use delete for a tombstone"
                    ),
                )
            })?;
            transaction_row_from_tracked_row(row, target_branch_id)
        }
        WasmConflictResolution::Delete => {
            Ok(transaction_delete_row(&conflict.key, target_branch_id))
        }
        WasmConflictResolution::Replace {
            snapshot_content,
            effect,
        } => {
            let WasmHostBytes::Inline(snapshot) = snapshot_content else {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "validated conflict replacement retained an unresolved lazy snapshot",
                ));
            };
            let snapshot = String::from_utf8(snapshot).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!("plugin conflict replacement is not UTF-8 JSON: {error}"),
                )
            })?;
            // A replacement's effect is part of the v2 durable semantic
            // contract. Do not inherit the selected B row's metadata: that
            // could retain a stale format-only marker after a content merge,
            // or silently turn a declared format-only resolution into content.
            let metadata = match effect {
                WasmChangeEffect::Content => None,
                WasmChangeEffect::FormatOnly => Some(transaction_json(
                    r#"{"impact":"format"}"#,
                    "merge conflict format-only metadata",
                )?),
            };
            Ok(TransactionWriteRow {
                entity_pk: Some(conflict.key.entity_pk.clone()),
                schema_key: conflict.key.schema_key.clone(),
                file_id: conflict.key.file_id.clone(),
                snapshot: Some(transaction_json(&snapshot, "merge conflict replacement")?),
                metadata,
                origin: None,
                created_at: None,
                updated_at: None,
                global: false,
                change_id: None,
                commit_id: None,
                untracked: false,
                branch_id: target_branch_id.to_owned(),
            })
        }
    }
}

fn transaction_delete_row(key: &TrackedStateKey, target_branch_id: &str) -> TransactionWriteRow {
    TransactionWriteRow {
        entity_pk: Some(key.entity_pk.clone()),
        schema_key: key.schema_key.clone(),
        file_id: key.file_id.clone(),
        snapshot: None,
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: target_branch_id.to_owned(),
    }
}

fn transaction_row_from_tracked_row(
    row: &MaterializedTrackedStateRow,
    target_branch_id: &str,
) -> Result<TransactionWriteRow, LixError> {
    let snapshot = row
        .snapshot_content
        .as_deref()
        .map(|snapshot| transaction_json(snapshot, "merge snapshot"))
        .transpose()?;
    let metadata = row
        .metadata
        .as_deref()
        .map(|metadata| transaction_json(metadata, "merge metadata"))
        .transpose()?;
    Ok(TransactionWriteRow {
        entity_pk: Some(row.entity_pk.clone()),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot,
        metadata,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: target_branch_id.to_owned(),
    })
}

async fn materialized_plugin_merge_rows<S>(
    reader: &mut TrackedStateStoreReader<S>,
    analysis: &super::analysis::MergeAnalysis,
    derived_blob_files: &BTreeMap<String, PluginFileOwner>,
    target_branch_id: &str,
    resolved_plugin_rows: Vec<TransactionWriteRow>,
) -> Result<Vec<TransactionWriteRow>, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    let merge_plan = analysis
        .merge_plan()
        .expect("materialized merge rows require a merge plan");
    let semantic_picks = merge_plan
        .picks
        .iter()
        .filter(|pick| {
            let Some(file_id) = pick.selected_row.file_id.as_ref() else {
                return false;
            };
            derived_blob_files.get(file_id).is_some_and(|owner| {
                owner
                    .schema_keys()
                    .iter()
                    .any(|schema_key| schema_key == &pick.selected_row.schema_key)
            })
        })
        .collect::<Vec<_>>();
    if semantic_picks.is_empty() {
        return Ok(resolved_plugin_rows);
    }
    let keys = semantic_picks
        .iter()
        .map(|pick| TrackedStateKey {
            schema_key: pick.selected_row.schema_key.clone(),
            file_id: pick.selected_row.file_id.clone(),
            entity_pk: pick.selected_row.entity_pk.clone(),
        })
        .collect::<Vec<_>>();
    let rows = reader
        .load_projected_rows_at_commit(
            &analysis.commits.source_commit_id.to_string(),
            &keys,
            &ChangeRecordProjection::full(),
        )
        .await?;
    let mut rows = rows
        .into_iter()
        .zip(keys)
        .map(|(row, key)| {
            let row = row.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "source merge root omitted selected plugin row '{}' for file '{}'",
                        key.schema_key,
                        key.file_id.as_deref().unwrap_or_default()
                    ),
                )
            })?;
            transaction_row_from_tracked_row(&row, target_branch_id)
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    rows.extend(resolved_plugin_rows);
    Ok(rows)
}

fn transaction_json(value: &str, context: &str) -> Result<TransactionJson, LixError> {
    let parsed = serde_json::from_str(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("{context} is invalid JSON: {error}"),
        )
    })?;
    TransactionJson::from_value(parsed, context)
}

fn preview_from_analysis(
    target_branch_id: &str,
    source_branch_id: &str,
    analysis: &super::analysis::MergeAnalysis,
    derived_blob_files: &BTreeMap<String, PluginFileOwner>,
    resolvable_plugin_conflicts: &BTreeSet<TrackedStateKey>,
) -> MergeBranchPreview {
    MergeBranchPreview {
        outcome: merge_branch_outcome_from_analysis(analysis.outcome),
        target_branch_id: target_branch_id.to_string(),
        source_branch_id: source_branch_id.to_string(),
        base_commit_id: analysis.commits.base_commit_id.to_string(),
        target_head_commit_id: analysis.commits.target_commit_id.to_string(),
        source_head_commit_id: analysis.commits.source_commit_id.to_string(),
        change_stats: merge_change_stats_from_analysis(&analysis.stats),
        conflicts: analysis
            .conflicts
            .iter()
            .filter(|conflict| {
                !is_derived_blob_conflict(conflict, derived_blob_files)
                    && !is_resolvable_plugin_semantic_conflict(
                        conflict,
                        resolvable_plugin_conflicts,
                    )
            })
            .map(merge_conflict_from_analysis)
            .collect(),
    }
}

fn merge_branch_outcome_from_analysis(outcome: MergeOutcome) -> MergeBranchOutcome {
    match outcome {
        MergeOutcome::AlreadyUpToDate => MergeBranchOutcome::AlreadyUpToDate,
        MergeOutcome::FastForward => MergeBranchOutcome::FastForward,
        MergeOutcome::MergeCommitted => MergeBranchOutcome::MergeCommitted,
    }
}

fn merge_change_stats_from_analysis(stats: &MergeStats) -> MergeChangeStats {
    MergeChangeStats {
        total: stats.total,
        added: stats.added,
        modified: stats.modified,
        removed: stats.removed,
    }
}

fn merge_conflict_from_analysis(conflict: &AnalysisMergeConflict) -> MergeConflict {
    MergeConflict {
        kind: match conflict.kind {
            AnalysisMergeConflictKind::SameEntityChanged => MergeConflictKind::SameEntityChanged,
        },
        schema_key: conflict.schema_key.clone(),
        entity_pk: conflict.entity_pk.clone(),
        file_id: conflict.file_id.clone(),
        target: merge_conflict_side_from_analysis(&conflict.target),
        source: merge_conflict_side_from_analysis(&conflict.source),
    }
}

fn merge_conflict_side_from_analysis(side: &AnalysisMergeConflictSide) -> MergeConflictSide {
    MergeConflictSide {
        kind: match side.kind {
            AnalysisMergeConflictChangeKind::Added => MergeConflictChangeKind::Added,
            AnalysisMergeConflictChangeKind::Modified => MergeConflictChangeKind::Modified,
            AnalysisMergeConflictChangeKind::Removed => MergeConflictChangeKind::Removed,
        },
        before_change_id: side.before_change_id.clone(),
        after_change_id: side.after_change_id.clone(),
    }
}

#[expect(clippy::unnecessary_wraps)]
fn merge_conflict_error(conflicts: &[MergeConflict]) -> Result<LixError, LixError> {
    let conflict_count = conflicts.len();
    Ok(LixError::new(
        LixError::CODE_MERGE_CONFLICT,
        format!("merge_branch found {conflict_count} tracked-state conflict(s)"),
    )
    .with_hint("Resolve the conflicting entities in the target branch, then retry the merge.")
    .with_details(json!({
        "conflicts": conflicts.iter()
            .map(merge_conflict_details)
            .collect::<Vec<_>>(),
    })))
}

fn merge_conflict_details(conflict: &MergeConflict) -> serde_json::Value {
    json!({
        "kind": match conflict.kind {
            MergeConflictKind::SameEntityChanged => "sameEntityChanged",
        },
        "schemaKey": conflict.schema_key,
        "entityPk": conflict.entity_pk,
        "fileId": conflict.file_id,
        "target": merge_conflict_side_details(&conflict.target),
        "source": merge_conflict_side_details(&conflict.source),
    })
}

fn merge_conflict_side_details(side: &MergeConflictSide) -> serde_json::Value {
    json!({
        "kind": match side.kind {
            MergeConflictChangeKind::Added => "added",
            MergeConflictChangeKind::Modified => "modified",
            MergeConflictChangeKind::Removed => "removed",
        },
        "beforeChangeId": side.before_change_id,
        "afterChangeId": side.after_change_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::tracked_state::{TrackedStateDiffIdentity, TrackedStateDiffRow};

    fn descriptor_row(
        file_id: &str,
        directory_id: Option<&str>,
        name: &str,
    ) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            entity_pk: EntityPk::single(file_id),
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_owned(),
            file_id: None,
            snapshot_content: Some(
                json!({
                    "id": file_id,
                    "directory_id": directory_id,
                    "name": name,
                })
                .to_string(),
            ),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
            change_id: ChangeId::for_test_label("descriptor-change"),
            commit_id: CommitId::for_test_label("descriptor-commit"),
        }
    }

    fn owner_row(file_id: &str, incarnation: &str) -> MaterializedTrackedStateRow {
        let owner = PluginFileOwner::new(
            file_id,
            "plugin_csv_v2",
            vec!["csv_v2_row".to_owned(), "csv_v2_table".to_owned()],
        )
        .unwrap();
        MaterializedTrackedStateRow {
            entity_pk: EntityPk::single(PLUGIN_OWNER_KEY),
            schema_key: "lix_key_value".to_owned(),
            file_id: Some(file_id.to_owned()),
            snapshot_content: Some(owner.to_snapshot().unwrap().to_string()),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
            change_id: ChangeId::for_test_label(incarnation),
            commit_id: CommitId::for_test_label("owner-commit"),
        }
    }

    fn derived_file_ref_conflict(file_id: &str) -> AnalysisMergeConflict {
        AnalysisMergeConflict {
            kind: AnalysisMergeConflictKind::SameEntityChanged,
            schema_key: DERIVED_FILE_REF_SCHEMA_KEY.to_owned(),
            entity_pk: json!([file_id]),
            file_id: Some(file_id.to_owned()),
            target: AnalysisMergeConflictSide {
                kind: AnalysisMergeConflictChangeKind::Modified,
                before_change_id: Some("target-before".to_owned()),
                after_change_id: Some("target-after".to_owned()),
            },
            source: AnalysisMergeConflictSide {
                kind: AnalysisMergeConflictChangeKind::Modified,
                before_change_id: Some("source-before".to_owned()),
                after_change_id: Some("source-after".to_owned()),
            },
        }
    }

    fn derived_file_ref_pick(file_id: &str) -> TrackedStateMergePick {
        let change_id = ChangeId::for_test_label("derived-ref-change");
        TrackedStateMergePick {
            identity: TrackedStateDiffIdentity {
                schema_key: DERIVED_FILE_REF_SCHEMA_KEY.to_owned(),
                entity_pk: EntityPk::single(file_id),
                file_id: Some(file_id.to_owned()),
            },
            change_id,
            selected_row: TrackedStateDiffRow {
                entity_pk: EntityPk::single(file_id),
                schema_key: DERIVED_FILE_REF_SCHEMA_KEY.to_owned(),
                file_id: Some(file_id.to_owned()),
                deleted: false,
                created_at: crate::common::LixTimestamp::expect_parse(
                    "created_at",
                    "2026-01-01T00:00:00Z",
                ),
                updated_at: crate::common::LixTimestamp::expect_parse(
                    "updated_at",
                    "2026-01-01T00:00:00Z",
                ),
                change_id,
                commit_id: CommitId::for_test_label("derived-ref-commit"),
            },
        }
    }

    fn derived_file_owner(file_id: &str) -> BTreeMap<String, PluginFileOwner> {
        BTreeMap::from([(
            file_id.to_owned(),
            PluginFileOwner::new(
                file_id,
                "plugin_git_text_v2",
                vec!["git_text_line".to_owned()],
            )
            .unwrap(),
        )])
    }

    #[test]
    fn resolver_owner_requires_one_live_file_incarnation() {
        let base = owner_row("file-a", "incarnation-a");
        assert!(
            common_live_plugin_owner(Some(&base), Some(&base), Some(&base))
                .unwrap()
                .is_some()
        );

        let mut tombstone = base.clone();
        tombstone.deleted = true;
        assert!(
            common_live_plugin_owner(Some(&base), Some(&tombstone), Some(&base))
                .unwrap()
                .is_none()
        );

        let recreated = owner_row("file-a", "incarnation-b");
        assert!(
            common_live_plugin_owner(Some(&base), Some(&recreated), Some(&base))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn common_descriptor_requires_every_live_historical_root() {
        let base = descriptor_row("file-a", Some("directory-a"), "readme.md");
        assert!(
            common_historical_file_descriptor(
                "file-a",
                Some(base.clone()),
                Some(base.clone()),
                Some(base.clone()),
            )
            .is_some()
        );
        assert!(
            common_historical_file_descriptor(
                "file-a",
                Some(base.clone()),
                None,
                Some(base.clone()),
            )
            .is_none()
        );

        let mut tombstone = base.clone();
        tombstone.deleted = true;
        assert!(
            common_historical_file_descriptor(
                "file-a",
                Some(base.clone()),
                Some(tombstone),
                Some(base.clone()),
            )
            .is_none()
        );

        let renamed = descriptor_row("file-a", Some("directory-a"), "guide.md");
        assert!(
            common_historical_file_descriptor(
                "file-a",
                Some(base.clone()),
                Some(renamed),
                Some(base),
            )
            .is_none()
        );
    }

    #[test]
    fn resolver_take_requires_a_present_snapshot() {
        let conflict = PluginMergeConflictRow {
            key: TrackedStateKey {
                schema_key: "csv_v2_row".to_owned(),
                file_id: Some("file-a".to_owned()),
                entity_pk: EntityPk::single("row-a"),
            },
            base: None,
            a: None,
            b: None,
        };
        let error = transaction_row_from_conflict_resolution(
            &conflict,
            WasmConflictResolution::Take(WasmConflictTake::A),
            "main",
        )
        .expect_err("take(a) cannot silently turn an absent snapshot into deletion");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("absent a snapshot"));

        let deleted = transaction_row_from_conflict_resolution(
            &conflict,
            WasmConflictResolution::Delete,
            "main",
        )
        .expect("explicit delete remains the only tombstone operation");
        assert!(deleted.snapshot.is_none());
    }

    #[test]
    fn common_path_requires_identical_directory_ancestry_results() {
        assert_eq!(
            common_historical_path(
                Some("/docs/readme.md".to_owned()),
                Some("/guides/readme.md".to_owned()),
                Some("/docs/readme.md".to_owned()),
            ),
            None,
            "a parent-directory rename must not expose a stale base path"
        );
        assert_eq!(
            common_historical_path(
                Some("/docs/readme.md".to_owned()),
                Some("/docs/readme.md".to_owned()),
                Some("/docs/readme.md".to_owned()),
            ),
            Some("/docs/readme.md".to_owned())
        );
    }

    #[test]
    fn derived_file_ref_conflicts_are_reconciled_with_plugin_semantic_state() {
        let owners = derived_file_owner("file-a");

        assert!(is_derived_blob_conflict(
            &derived_file_ref_conflict("file-a"),
            &owners,
        ));
        assert!(!is_derived_blob_conflict(
            &derived_file_ref_conflict("file-other"),
            &owners,
        ));
    }

    #[test]
    fn derived_file_ref_picks_are_not_copied_over_semantic_merge_results() {
        let owners = derived_file_owner("file-a");

        assert!(pick_is_derived_plugin_state(
            &derived_file_ref_pick("file-a"),
            &owners,
        ));
        assert!(!pick_is_derived_plugin_state(
            &derived_file_ref_pick("file-other"),
            &owners,
        ));
    }
}
