use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use tracing::Instrument as _;

use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchReferenceRole};
use crate::changelog::ChangeRecordProjection;
use crate::plugin::runtime::{
    ConflictRank, PLUGIN_OWNER_KEY, PluginFileOwner, PluginRegistry, PluginRegistryEntry,
    ReconciledRow, ReconciledTypedRow, RowVersionRef,
    TypedColumnMergeResult as HostTypedColumnMergeResult, TypedRowVersionRef,
    WasmColumnMergeResult, WasmHostColumnMerge, load_plugin_registry_at_commit, reconcile_row,
    reconcile_typed_row, visit_typed_row_overlaps,
};
use crate::row_pk::RowPk;
use crate::storage_adapter::Storage;
#[cfg(test)]
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::tracked_state::{
    MaterializedTrackedStateRowRef, TrackedStateDiffIdentity, TrackedStateKey, TrackedStateKeyRef,
    TrackedStateMergeConflict, TrackedStateStoreReader,
};
use crate::transaction_types::{
    RawWriteBatch, TransactionJson, TransactionWrite, TransactionWriteMode,
};

use super::analysis::{MergeCommits, MergeOutcome, analyze};
use super::conflicts::{
    MergeConflictChangeKind as AnalysisMergeConflictChangeKind,
    MergeConflictKind as AnalysisMergeConflictKind, MergeConflictRow as AnalysisMergeConflict,
    MergeConflictSideRow as AnalysisMergeConflictSide,
};
use super::stats::MergeStats;
use crate::common::{SharedStr, compose_directory_path, compose_file_path};
use crate::plugin::runtime::{WasmRowKey, WasmTypedRow};
use crate::session::context::SessionContext;
use crate::tracked_state::TrackedStateMergePick;
use crate::transaction::StagedCommitChangeBatchBuilder;

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
    pub row_pk: JsonValue,
    pub file_id: Option<String>,
    pub target: MergeConflictSide,
    pub source: MergeConflictSide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeConflictKind {
    SameRowChanged,
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

        self.with_write_transaction_lending(async move |transaction| {
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
                    Ok::<_, LixError>((target_head, source_head))
                }
                .instrument(tracing::debug_span!(target: "lix_perf", "lix.perf.merge_branch_refs"))
                .await?;

                let merge_base = async {
                    let mut reader = transaction.commit_graph_reader().await;
                    reader.merge_base(&target_head, &source_head).await
                }
                .instrument(tracing::debug_span!(target: "lix_perf", "lix.perf.merge_base"))
                .await?;

                let analysis = async {
                    let mut reader = transaction.tracked_state_reader().await;
                    analyze(
                        &mut reader,
                        MergeCommits {
                            base_commit_id: merge_base,
                            target_commit_id: target_head,
                            source_commit_id: source_head,
                        },
                    )
                    .await
                }
                .instrument(tracing::debug_span!(target: "lix_perf", "lix.perf.merge_analysis"))
                .await?;
                let derived_blob_files = async {
                    let mut reader = transaction.tracked_state_reader().await;
                    derived_plugin_blob_conflicts(&mut reader, &analysis).await
                }
                .instrument(tracing::debug_span!(target: "lix_perf", "lix.perf.merge_derived_blob_detection"))
                .await?;

                let plugin_resolution_stats = if analysis.outcome == MergeOutcome::MergeCommitted {
                    let semantic_branch_id = SharedStr::from(active_branch_id.as_str());
                    let resolved_plugin_rows = resolve_row_merge_conflicts(
                        transaction,
                        &analysis,
                        &derived_blob_files,
                        &semantic_branch_id,
                    )
                    .instrument(tracing::debug_span!(target: "lix_perf", "lix.perf.merge_plugin_conflict_resolve"))
                    .await?;
                    async {
                        let mut reader = transaction.tracked_state_reader().await;
                        plugin_resolution_change_stats(&mut reader, &analysis, &resolved_plugin_rows).await
                    }
                    .instrument(tracing::debug_span!(target: "lix_perf", "lix.perf.merge_plugin_resolution_stats"))
                    .await?
                } else {
                    MergeChangeStats::default()
                };

                preview_from_analysis(
                    &active_branch_id,
                    &source_branch_id,
                    &analysis,
                    &derived_blob_files,
                    &plugin_resolution_stats,
                )
        })
        .instrument(tracing::debug_span!(target: "lix_perf", "lix.perf.merge_preview_total"))
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

        self.with_write_transaction_lending(async move |transaction| {
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

            let semantic_branch_id = SharedStr::from(active_branch_id.as_str());
            let resolved_plugin_rows = resolve_row_merge_conflicts(
                transaction,
                &analysis,
                &derived_blob_files,
                &semantic_branch_id,
            )
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.merge_plugin_conflict_resolve"
            ))
            .await?;
            let plugin_resolution_stats = async {
                let mut reader = transaction.tracked_state_reader().await;
                plugin_resolution_change_stats(&mut reader, &analysis, &resolved_plugin_rows).await
            }
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.merge_plugin_resolution_stats"
            ))
            .await?;

            let semantic_rows = async {
                let mut reader = transaction.tracked_state_reader().await;
                materialized_plugin_merge_rows(
                    &mut reader,
                    &analysis,
                    &derived_blob_files,
                    &semantic_branch_id,
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
                let mut selected_changes =
                    StagedCommitChangeBatchBuilder::with_capacity(merge_plan.picks.len());
                for pick in merge_plan
                    .picks
                    .iter()
                    .filter(|pick| !pick_is_derived_plugin_state(pick, &derived_blob_files))
                {
                    selected_changes.push(
                        pick.identity.clone(),
                        pick.selected_row.commit_id,
                        pick.change_id,
                        pick.selected_row.deleted,
                        pick.selected_row.created_at,
                        pick.selected_row.updated_at,
                    );
                }
                transaction.stage_merge_commit(
                    active_branch_id.clone(),
                    analysis.commits.source_commit_id,
                    selected_changes.finish(),
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
                change_stats: merge_change_stats_with_plugin_resolutions(
                    &analysis.stats,
                    &plugin_resolution_stats,
                ),
            })
        })
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.merge_branch_total"
        ))
        .await
    }
}

const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

#[derive(Debug, Clone, Default)]
struct DerivedPluginConflictIndex {
    owners: BTreeMap<String, PluginFileOwner>,
    files: BTreeSet<String>,
}

impl DerivedPluginConflictIndex {
    fn owner(&self, file_id: &str) -> Option<&PluginFileOwner> {
        self.owners.get(file_id)
    }

    fn contains_file(&self, file_id: &str) -> bool {
        self.files.contains(file_id)
    }
}

async fn derived_plugin_blob_conflicts<S>(
    reader: &mut TrackedStateStoreReader<S>,
    analysis: &super::analysis::MergeAnalysis,
) -> Result<DerivedPluginConflictIndex, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    // A derived blob conflict is the common signal, but it is not the
    // authority for plugin ownership. Start from every conflicted file and
    // prove one *live, identical* owner across all three historical roots.
    //
    // A missing/tombstoned owner is a file-lifecycle conflict (for example,
    // delete-vs-edit), not a semantic row conflict. Letting a resolver
    // choose a row value in that case could silently pair a live semantic
    // row with a deleted file owner. Keep the whole conflict visible until a
    // first-class lifecycle conflict model exists.
    let Some(conflicts) = analysis.conflict_batch() else {
        return Ok(DerivedPluginConflictIndex::default());
    };
    let mut conflict_indices_by_file = BTreeMap::<String, Vec<usize>>::new();
    for (index, conflict) in conflicts.iter().enumerate() {
        let Some(file_id) = conflict.file_id() else {
            continue;
        };
        conflict_indices_by_file
            .entry(file_id.to_owned())
            .or_default()
            .push(index);
    }
    let file_ids = conflict_indices_by_file
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if file_ids.is_empty() {
        return Ok(DerivedPluginConflictIndex::default());
    }

    let owner_keys = file_ids
        .iter()
        .map(|file_id| TrackedStateKey {
            schema_key: "lix_key_value".to_owned(),
            file_id: Some(file_id.clone()),
            row_pk: RowPk::single(PLUGIN_OWNER_KEY),
        })
        .collect::<Vec<_>>();
    let base_rows = reader
        .load_projected_batch_at_commit(
            &analysis.commits.base_commit_id.to_string(),
            &owner_keys,
            &ChangeRecordProjection::full(),
        )
        .await?;
    let target_rows = reader
        .load_projected_batch_at_commit(
            &analysis.commits.target_commit_id.to_string(),
            &owner_keys,
            &ChangeRecordProjection::full(),
        )
        .await?;
    let source_rows = reader
        .load_projected_batch_at_commit(
            &analysis.commits.source_commit_id.to_string(),
            &owner_keys,
            &ChangeRecordProjection::full(),
        )
        .await?;
    let mut common_owners = BTreeMap::new();
    for (index, file_id) in file_ids.into_iter().enumerate() {
        let Some(owner) = common_live_plugin_owner_ref(
            base_rows.row(index),
            target_rows.row(index),
            source_rows.row(index),
        )?
        else {
            continue;
        };
        common_owners.insert(file_id, owner);
    }
    if common_owners.is_empty() {
        return Ok(DerivedPluginConflictIndex::default());
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
    let base_registry =
        load_plugin_registry_at_commit(reader, &analysis.commits.base_commit_id.to_string())
            .await?;
    let target_registry =
        load_plugin_registry_at_commit(reader, &analysis.commits.target_commit_id.to_string())
            .await?;
    let source_registry =
        load_plugin_registry_at_commit(reader, &analysis.commits.source_commit_id.to_string())
            .await?;

    let mut derived = BTreeSet::new();
    let mut derived_owners = BTreeMap::new();
    for (file_id, owner) in common_owners {
        let Some(path @ Some(_)) = common_descriptors.get(&file_id).cloned() else {
            continue;
        };
        let _plugin = pinned_conflict_plugin_entry(
            &owner,
            &base_registry,
            &target_registry,
            &source_registry,
            &file_id,
        )?;
        let _ = path;
        derived.insert(file_id.clone());
        derived_owners.insert(file_id, owner);
    }
    Ok(DerivedPluginConflictIndex {
        owners: derived_owners,
        files: derived,
    })
}

fn common_live_plugin_owner_ref(
    base: Option<MaterializedTrackedStateRowRef<'_>>,
    target: Option<MaterializedTrackedStateRowRef<'_>>,
    source: Option<MaterializedTrackedStateRowRef<'_>>,
) -> Result<Option<PluginFileOwner>, LixError> {
    let Some(base) = base.filter(|row| !row.deleted()) else {
        return Ok(None);
    };
    let Some(target) = target.filter(|row| !row.deleted()) else {
        return Ok(None);
    };
    let Some(source) = source.filter(|row| !row.deleted()) else {
        return Ok(None);
    };
    if base.change_id() != target.change_id() || base.change_id() != source.change_id() {
        return Ok(None);
    }
    let Some(base_snapshot) = base.snapshot_content() else {
        return Ok(None);
    };
    if target.snapshot_content().map(SharedStr::as_str) != Some(base_snapshot.as_str())
        || source.snapshot_content().map(SharedStr::as_str) != Some(base_snapshot.as_str())
    {
        return Ok(None);
    }
    let Some(base_owner) = PluginFileOwner::from_tracked_state_row_ref(base)? else {
        return Ok(None);
    };
    Ok(Some(base_owner))
}

/// A semantic resolver can run only within a single live file incarnation.
/// `PluginFileOwner::change_id` is the immutable incarnation key used by the
/// actor cache and ID namespace. Equal owner payloads alone are not enough:
/// a delete/recreate may intentionally reuse a file ID and plugin key.
#[cfg(test)]
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
    conflict: &TrackedStateMergeConflict,
    derived_blob_files: &DerivedPluginConflictIndex,
) -> bool {
    matches!(conflict.identity.schema_key(), BLOB_REF_SCHEMA_KEY)
        && conflict
            .identity
            .file_id()
            .is_some_and(|file_id| derived_blob_files.contains_file(file_id))
}

fn pick_is_derived_plugin_state(
    pick: &TrackedStateMergePick,
    derived_blob_files: &DerivedPluginConflictIndex,
) -> bool {
    let Some(file_id) = pick.selected_row.file_id() else {
        return false;
    };
    let Some(owner) = derived_blob_files.owner(file_id) else {
        return false;
    };
    matches!(pick.selected_row.schema_key(), BLOB_REF_SCHEMA_KEY)
        || owner
            .schema_keys()
            .iter()
            .any(|schema_key| schema_key == pick.selected_row.schema_key())
}

#[derive(Debug, Clone)]
struct PluginMergeConflictPayload {
    snapshot: MergeConflictSnapshot,
    metadata: Option<SharedStr>,
}

#[derive(Debug, Clone)]
enum MergeConflictSnapshot {
    Json(SharedStr),
    Typed(Arc<WasmTypedRow>),
}
#[derive(Debug)]
struct RowMergeInput {
    identity: TrackedStateDiffIdentity,
    base: Option<PluginMergeConflictPayload>,
    a: Option<PluginMergeConflictPayload>,
    b: Option<PluginMergeConflictPayload>,
    typed: bool,
    merger: Option<PluginRegistryEntry>,
    primary_key_columns: BTreeSet<String>,
}

#[derive(Debug)]
struct ColumnMergeGroup {
    plugin: PluginRegistryEntry,
    merges: Vec<WasmHostColumnMerge>,
    destinations: Vec<(usize, String)>,
}

/// Resolves every same-row conflict through the universal row pipeline.
/// Host-native column LWW is always present. A schema-owning component is
/// called only for columns that both successors changed differently, and its
/// output is applied back to that exact column before file-backed rows proceed
/// through the ordinary projection staging path.
async fn resolve_row_merge_conflicts<StorageImpl>(
    transaction: &mut crate::transaction::Transaction<StorageImpl>,
    analysis: &super::analysis::MergeAnalysis,
    derived_blob_files: &DerivedPluginConflictIndex,
    target_branch_id: &SharedStr,
) -> Result<RawWriteBatch, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let merge_plan = analysis
        .merge_plan()
        .expect("row reconciliation requires a merge plan");
    let conflicts = merge_plan
        .conflicts
        .iter()
        .filter(|conflict| !is_derived_blob_conflict(conflict, derived_blob_files))
        .collect::<Vec<_>>();
    if conflicts.is_empty() {
        return Ok(RawWriteBatch::new());
    }

    let keys = conflicts
        .iter()
        .map(|conflict| TrackedStateKey {
            schema_key: conflict.identity.schema_key().to_owned(),
            file_id: conflict.identity.file_id().map(str::to_owned),
            row_pk: conflict.identity.row_pk().clone(),
        })
        .collect::<Vec<_>>();
    let primary_keys_by_schema = conflicts
        .iter()
        .map(|conflict| conflict.identity.schema_key())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|schema_key| {
            transaction
                .merge_primary_key_columns(schema_key)
                .map(|columns| (schema_key.to_owned(), columns))
        })
        .collect::<Result<BTreeMap<_, _>, LixError>>()?;

    // Historical reads finish before any Component is instantiated. All
    // merge input below owns or shares immutable buffers.
    let inputs = {
        let mut reader = transaction.tracked_state_reader().await;
        let base_rows = reader
            .load_projected_batch_at_commit(
                &analysis.commits.base_commit_id.to_string(),
                &keys,
                &ChangeRecordProjection::full(),
            )
            .await?;
        let target_rows = reader
            .load_projected_batch_at_commit(
                &analysis.commits.target_commit_id.to_string(),
                &keys,
                &ChangeRecordProjection::full(),
            )
            .await?;
        let source_rows = reader
            .load_projected_batch_at_commit(
                &analysis.commits.source_commit_id.to_string(),
                &keys,
                &ChangeRecordProjection::full(),
            )
            .await?;
        let base_registry = load_plugin_registry_at_commit(
            &mut reader,
            &analysis.commits.base_commit_id.to_string(),
        )
        .await?;
        let target_registry = load_plugin_registry_at_commit(
            &mut reader,
            &analysis.commits.target_commit_id.to_string(),
        )
        .await?;
        let source_registry = load_plugin_registry_at_commit(
            &mut reader,
            &analysis.commits.source_commit_id.to_string(),
        )
        .await?;

        let mut inputs = Vec::with_capacity(conflicts.len());
        for (index, conflict) in conflicts.iter().enumerate() {
            let base = base_rows.row(index);
            let target = target_rows.row(index);
            let source = source_rows.row(index);
            verify_historical_conflict_row_ref(base, conflict.target.before.as_ref(), "base")?;
            verify_historical_conflict_row_ref(target, conflict.target.after.as_ref(), "target")?;
            verify_historical_conflict_row_ref(source, conflict.source.after.as_ref(), "source")?;
            let (a, b) = canonical_conflict_variants_ref(conflict, target, source)?;
            let typed_ownership = [
                base_registry.owns_schema(conflict.identity.schema_key()),
                target_registry.owns_schema(conflict.identity.schema_key()),
                source_registry.owns_schema(conflict.identity.schema_key()),
            ];
            if typed_ownership.iter().any(|owned| *owned)
                && !typed_ownership.iter().all(|owned| *owned)
            {
                return Err(LixError::new(
                    LixError::CODE_MERGE_CONFLICT,
                    format!(
                        "plugin ownership for schema '{}' differs across the merge",
                        conflict.identity.schema_key()
                    ),
                ));
            }
            let typed = typed_ownership[0];
            let base = historical_live_payload_ref(base, typed)?;
            inputs.push(RowMergeInput {
                identity: conflict.identity.clone(),
                base,
                a: historical_live_payload_ref(a, typed)?,
                b: historical_live_payload_ref(b, typed)?,
                typed,
                merger: common_column_merger_for_schema(
                    conflict.identity.schema_key(),
                    &base_registry,
                    &target_registry,
                    &source_registry,
                )?,
                primary_key_columns: primary_keys_by_schema
                    .get(conflict.identity.schema_key())
                    .cloned()
                    .expect("schema primary keys were resolved above"),
            });
        }
        inputs
    };

    for input in inputs.iter().filter(|input| input.typed) {
        let expected = transaction
            .plugin_schema_plan(input.identity.schema_key())
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "typed row merge references unknown schema '{}'",
                        input.identity.schema_key()
                    ),
                )
            })?
            .fingerprint()
            .bytes();
        for payload in [input.base.as_ref(), input.a.as_ref(), input.b.as_ref()]
            .into_iter()
            .flatten()
        {
            if typed_merge_snapshot(payload)?.schema_fingerprint != expected {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "plugin merge row for schema '{}' has the wrong schema fingerprint",
                        input.identity.schema_key()
                    ),
                ));
            }
        }
    }

    let mut groups = BTreeMap::<(String, String), ColumnMergeGroup>::new();
    for (row_index, input) in inputs.iter().enumerate() {
        if !input.typed {
            continue;
        }
        let Some(plugin) = input.merger.as_ref() else {
            continue;
        };
        let (Some(base), Some(a), Some(b)) =
            (input.base.as_ref(), input.a.as_ref(), input.b.as_ref())
        else {
            continue;
        };
        let schema_plan = transaction
            .plugin_schema_plan(input.identity.schema_key())
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "column merge references unknown schema '{}'",
                        input.identity.schema_key()
                    ),
                )
            })?;
        let schema_fingerprint = schema_plan.fingerprint().bytes();
        let base_row = typed_merge_snapshot(base)?;
        let a_row = typed_merge_snapshot(a)?;
        let b_row = typed_merge_snapshot(b)?;
        if [base_row, a_row, b_row]
            .into_iter()
            .any(|row| row.schema_fingerprint != schema_fingerprint)
        {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "plugin merge row for schema '{}' has the wrong schema fingerprint",
                    input.identity.schema_key()
                ),
            ));
        }
        let group = groups
            .entry((
                plugin.key().to_owned(),
                plugin.archive_blob_hash().to_owned(),
            ))
            .or_insert_with(|| ColumnMergeGroup {
                plugin: plugin.clone(),
                merges: Vec::new(),
                destinations: Vec::new(),
            });
        visit_typed_row_overlaps(
            Some(typed_merge_payload_ref(base)?),
            Some(typed_merge_payload_ref(a)?),
            Some(typed_merge_payload_ref(b)?),
            &input.primary_key_columns,
            |overlap| {
                let ordinal = u32::try_from(group.merges.len()).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        "column merge batch exceeds the u32 ordinal limit",
                    )
                })?;
                group.merges.push(WasmHostColumnMerge {
                    ordinal,
                    key: WasmRowKey::from_typed_parts(
                        input.identity.schema_key().to_owned(),
                        schema_fingerprint,
                        base_row.row_pk.clone(),
                    )?,
                    file_id: input.identity.file_id().map(str::to_owned),
                    column: overlap.column.to_owned(),
                    schema_fingerprint,
                    base: base_row.row.get(overlap.column).cloned(),
                    a: overlap.a.cloned(),
                    b: overlap.b.cloned(),
                    base_row: Arc::clone(base_row),
                    a_row: Arc::clone(a_row),
                    b_row: Arc::clone(b_row),
                });
                group
                    .destinations
                    .push((row_index, overlap.column.to_owned()));
                Ok(())
            },
        )?;
    }

    let mut replacements = BTreeMap::<(usize, String), HostTypedColumnMergeResult>::new();
    for (_, group) in groups {
        let resolved = transaction
            .merge_plugin_columns(&group.plugin, group.merges)
            .await?;
        if resolved.results.len() != group.destinations.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "validated column merge output lost input alignment",
            ));
        }
        for (destination, result) in group.destinations.into_iter().zip(resolved.results) {
            let result = match result {
                WasmColumnMergeResult::UseLww => HostTypedColumnMergeResult::UseLww,
                WasmColumnMergeResult::Replace(value) => HostTypedColumnMergeResult::Replace(value),
            };
            replacements.insert(destination, result);
        }
    }

    let mut rows = RawWriteBatch::with_capacity(inputs.len());
    for (row_index, input) in inputs.iter().enumerate() {
        if input.typed {
            let resolved = reconcile_typed_row(
                input
                    .base
                    .as_ref()
                    .map(typed_merge_payload_ref)
                    .transpose()?,
                input.a.as_ref().map(typed_merge_payload_ref).transpose()?,
                input.b.as_ref().map(typed_merge_payload_ref).transpose()?,
                &input.primary_key_columns,
                |overlap| Ok(replacements.remove(&(row_index, overlap.column.to_owned()))),
            )?;
            push_reconciled_typed_transaction_row(&mut rows, input, resolved, target_branch_id);
        } else {
            let base = DecodedMergePayload::parse(input.base.as_ref())?;
            let a = DecodedMergePayload::parse(input.a.as_ref())?;
            let b = DecodedMergePayload::parse(input.b.as_ref())?;
            let resolved = reconcile_row(
                base.as_ref().map(DecodedMergePayload::borrowed),
                a.as_ref().map(DecodedMergePayload::borrowed),
                b.as_ref().map(DecodedMergePayload::borrowed),
                &input.primary_key_columns,
                |_| Ok(None),
            )?;
            push_reconciled_json_transaction_row(&mut rows, input, resolved, target_branch_id)?;
        }
    }
    Ok(rows)
}

fn common_column_merger_for_schema(
    schema_key: &str,
    base: &PluginRegistry,
    target: &PluginRegistry,
    source: &PluginRegistry,
) -> Result<Option<PluginRegistryEntry>, LixError> {
    fn merger<'a>(
        schema_key: &str,
        registry: &'a PluginRegistry,
    ) -> Option<&'a PluginRegistryEntry> {
        registry.plugins().iter().find(|plugin| {
            plugin.has_column_merger()
                && plugin
                    .schema_keys()
                    .binary_search_by(|key| key.as_str().cmp(schema_key))
                    .is_ok()
        })
    }
    match (
        merger(schema_key, base),
        merger(schema_key, target),
        merger(schema_key, source),
    ) {
        (None, None, None) => Ok(None),
        (Some(base), Some(target), Some(source)) if base == target && target == source => {
            Ok(Some(target.clone()))
        }
        _ => Err(LixError::new(
            LixError::CODE_MERGE_CONFLICT,
            format!("column merger generation for schema '{schema_key}' differs across the merge"),
        )
        .with_hint("merge the plugin generation change before merging semantic row edits")),
    }
}

fn typed_merge_snapshot(
    payload: &PluginMergeConflictPayload,
) -> Result<&Arc<WasmTypedRow>, LixError> {
    let MergeConflictSnapshot::Typed(snapshot) = &payload.snapshot else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "plugin-owned merge row carries a forbidden JSON snapshot",
        ));
    };
    Ok(snapshot)
}

fn typed_merge_payload_ref(
    payload: &PluginMergeConflictPayload,
) -> Result<TypedRowVersionRef<'_>, LixError> {
    Ok(TypedRowVersionRef {
        snapshot: typed_merge_snapshot(payload)?,
        metadata: payload.metadata.as_ref(),
    })
}

fn push_reconciled_typed_transaction_row(
    rows: &mut RawWriteBatch,
    input: &RowMergeInput,
    resolved: Option<ReconciledTypedRow>,
    target_branch_id: &SharedStr,
) {
    match resolved {
        Some(resolved) => {
            push_plugin_transaction_row(
                rows,
                &input.identity,
                Some(Arc::new(resolved.snapshot)),
                resolved.metadata.map(|metadata| {
                    TransactionJson::from_unvalidated_shared_normalized_content(metadata)
                }),
                target_branch_id,
            );
        }
        None => push_plugin_transaction_row(rows, &input.identity, None, None, target_branch_id),
    }
}

fn push_reconciled_json_transaction_row(
    rows: &mut RawWriteBatch,
    input: &RowMergeInput,
    resolved: Option<ReconciledRow>,
    target_branch_id: &SharedStr,
) -> Result<(), LixError> {
    match resolved {
        Some(resolved) => {
            let snapshot = serde_json::to_string(&resolved.snapshot).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("failed to encode reconciled row snapshot: {error}"),
                )
            })?;
            let metadata = resolved
                .metadata
                .map(|metadata| serde_json::to_string(&metadata))
                .transpose()
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("failed to encode reconciled row metadata: {error}"),
                    )
                })?;
            rows.push_parts(
                Some(input.identity.row_pk().clone()),
                input.identity.schema_key_shared(),
                input.identity.file_id_shared(),
                Some(TransactionJson::from_unvalidated_shared_normalized_content(
                    SharedStr::from(snapshot),
                )),
                metadata.map(|metadata| {
                    TransactionJson::from_unvalidated_shared_normalized_content(SharedStr::from(
                        metadata,
                    ))
                }),
                None,
                None,
                None,
                false,
                None,
                None,
                false,
                target_branch_id.clone(),
            );
        }
        None => rows.push_parts(
            Some(input.identity.row_pk().clone()),
            input.identity.schema_key_shared(),
            input.identity.file_id_shared(),
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            None,
            false,
            target_branch_id.clone(),
        ),
    }
    Ok(())
}

struct DecodedMergePayload {
    snapshot: JsonValue,
    metadata: Option<JsonValue>,
}

impl DecodedMergePayload {
    fn parse(payload: Option<&PluginMergeConflictPayload>) -> Result<Option<Self>, LixError> {
        payload
            .map(|payload| {
                let MergeConflictSnapshot::Json(snapshot) = &payload.snapshot else {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "generic merge row carries a native typed snapshot",
                    ));
                };
                let snapshot = serde_json::from_str(snapshot.as_str()).map_err(|error| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!("row merge snapshot is invalid JSON: {error}"),
                    )
                })?;
                let metadata = payload
                    .metadata
                    .as_ref()
                    .map(|metadata| serde_json::from_str(metadata.as_str()))
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_SCHEMA_VALIDATION,
                            format!("row merge metadata is invalid JSON: {error}"),
                        )
                    })?;
                Ok(Self { snapshot, metadata })
            })
            .transpose()
    }

    fn borrowed(&self) -> RowVersionRef<'_> {
        RowVersionRef {
            snapshot: &self.snapshot,
            metadata: self.metadata.as_ref(),
        }
    }
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
) -> Result<BTreeMap<String, Option<String>>, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    if file_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let keys = file_ids
        .iter()
        .map(|file_id| {
            Ok(TrackedStateKey {
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_owned(),
                file_id: Some(file_id.clone()),
                row_pk: RowPk::uuid_from_canonical(file_id).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("validated file ID is not a canonical UUID: {error}"),
                    )
                })?,
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let base_commit_id = analysis.commits.base_commit_id.to_string();
    let target_commit_id = analysis.commits.target_commit_id.to_string();
    let source_commit_id = analysis.commits.source_commit_id.to_string();
    let base_rows = reader
        .load_projected_batch_at_commit(&base_commit_id, &keys, &ChangeRecordProjection::full())
        .await?;
    let target_rows = reader
        .load_projected_batch_at_commit(&target_commit_id, &keys, &ChangeRecordProjection::full())
        .await?;
    let source_rows = reader
        .load_projected_batch_at_commit(&source_commit_id, &keys, &ChangeRecordProjection::full())
        .await?;

    let mut descriptors = BTreeMap::new();
    for (index, file_id) in file_ids.iter().cloned().enumerate() {
        let Some((scope_file_id, descriptor)) = common_historical_file_descriptor_ref(
            &file_id,
            base_rows.row(index),
            target_rows.row(index),
            source_rows.row(index),
        ) else {
            descriptors.insert(file_id, None);
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
            descriptors.insert(file_id, None);
            continue;
        };
        descriptors.insert(file_id, Some(path));
    }
    Ok(descriptors)
}

fn historical_file_descriptor_row_ref(
    row: Option<MaterializedTrackedStateRowRef<'_>>,
    expected_file_id: &str,
) -> Option<(Option<String>, HistoricalFileDescriptor)> {
    let row = row.filter(|row| !row.deleted())?;
    let snapshot = row.snapshot_content()?;
    let descriptor = serde_json::from_str::<HistoricalFileDescriptor>(snapshot.as_str()).ok()?;
    (descriptor.id == expected_file_id).then(|| (row.file_id().map(str::to_owned), descriptor))
}

fn common_historical_file_descriptor_ref(
    expected_file_id: &str,
    base: Option<MaterializedTrackedStateRowRef<'_>>,
    target: Option<MaterializedTrackedStateRowRef<'_>>,
    source: Option<MaterializedTrackedStateRowRef<'_>>,
) -> Option<(Option<String>, HistoricalFileDescriptor)> {
    let base = historical_file_descriptor_row_ref(base, expected_file_id)?;
    let target = historical_file_descriptor_row_ref(target, expected_file_id)?;
    let source = historical_file_descriptor_row_ref(source, expected_file_id)?;
    (base == target && base == source).then_some(base)
}

#[cfg(test)]
fn historical_file_descriptor_row(
    row: Option<&MaterializedTrackedStateRow>,
    expected_file_id: &str,
) -> Option<(Option<String>, HistoricalFileDescriptor)> {
    let row = row.filter(|row| !row.deleted)?;
    let snapshot = row.snapshot_content.as_deref()?;
    let descriptor = serde_json::from_str::<HistoricalFileDescriptor>(snapshot).ok()?;
    (descriptor.id == expected_file_id).then_some((row.file_id.clone(), descriptor))
}

#[cfg(test)]
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
            row_pk: RowPk::uuid_from_canonical(&id).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("validated directory ID is not a canonical UUID: {error}"),
                )
            })?,
        };
        let row = reader
            .load_projected_batch_at_commit(
                commit_id,
                std::slice::from_ref(&key),
                &ChangeRecordProjection::full(),
            )
            .await?
            .into_rows()
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
            format!("plugin-owned file '{file_id}' changed plugin generation across the merge"),
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

fn verify_historical_conflict_row_ref(
    row: Option<MaterializedTrackedStateRowRef<'_>>,
    expected: Option<&crate::tracked_state::TrackedStateDiffRow>,
    side: &str,
) -> Result<(), LixError> {
    match (row, expected) {
        (None, None) => Ok(()),
        (Some(row), Some(expected)) if row.change_id() == expected.change_id => Ok(()),
        _ => Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("historical {side} row did not match merge analysis"),
        )),
    }
}

fn historical_live_payload_ref(
    row: Option<MaterializedTrackedStateRowRef<'_>>,
    typed: bool,
) -> Result<Option<PluginMergeConflictPayload>, LixError> {
    row.filter(|row| !row.deleted())
        .map(|row| {
            let snapshot = if typed {
                let snapshot = row.typed_snapshot().cloned().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        "live plugin semantic row is missing its native typed snapshot",
                    )
                })?;
                if row.snapshot_content().is_some() {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        "live plugin semantic row carries a forbidden JSON snapshot",
                    ));
                }
                MergeConflictSnapshot::Typed(snapshot)
            } else {
                let snapshot = row.snapshot_content().cloned().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "live engine merge row is missing its JSON snapshot",
                    )
                })?;
                if row.typed_snapshot().is_some() {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "live engine merge row carries an unexpected typed snapshot",
                    ));
                }
                MergeConflictSnapshot::Json(snapshot)
            };
            Ok(PluginMergeConflictPayload {
                snapshot,
                metadata: row.metadata().cloned(),
            })
        })
        .transpose()
}

fn canonical_conflict_variants_ref<'a>(
    conflict: &TrackedStateMergeConflict,
    target: Option<MaterializedTrackedStateRowRef<'a>>,
    source: Option<MaterializedTrackedStateRowRef<'a>>,
) -> Result<
    (
        Option<MaterializedTrackedStateRowRef<'a>>,
        Option<MaterializedTrackedStateRowRef<'a>>,
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
    let ordering = ConflictRank::new(target_after.updated_at, target_after.change_id).cmp(
        &ConflictRank::new(source_after.updated_at, source_after.change_id),
    );
    if ordering.is_eq() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "distinct merge conflict sides share the same durable ordering key",
        ));
    }
    let target = target.filter(|row| !row.deleted());
    let source = source.filter(|row| !row.deleted());
    if ordering.is_lt() {
        Ok((target, source))
    } else {
        Ok((source, target))
    }
}

fn push_plugin_transaction_row(
    rows: &mut RawWriteBatch,
    identity: &TrackedStateDiffIdentity,
    snapshot: Option<Arc<WasmTypedRow>>,
    metadata: Option<TransactionJson>,
    target_branch_id: &SharedStr,
) {
    rows.push_typed_parts(
        Some(identity.row_pk().clone()),
        identity.schema_key_shared(),
        identity.file_id_shared(),
        snapshot,
        metadata,
        None,
        None,
        None,
        false,
        None,
        None,
        false,
        target_branch_id.clone(),
    );
}

fn push_transaction_row_from_tracked_row_ref(
    rows: &mut RawWriteBatch,
    row: MaterializedTrackedStateRowRef<'_>,
    target_branch_id: &SharedStr,
) {
    let metadata = row
        .metadata()
        .cloned()
        .map(TransactionJson::from_unvalidated_shared_normalized_content);
    if let Some(typed_snapshot) = row.typed_snapshot().cloned() {
        rows.push_typed_parts(
            Some(row.row_pk().clone()),
            row.schema_key_shared(),
            row.file_id_shared(),
            Some(typed_snapshot),
            metadata,
            None,
            None,
            None,
            false,
            None,
            None,
            false,
            target_branch_id.clone(),
        );
    } else {
        rows.push_parts(
            Some(row.row_pk().clone()),
            row.schema_key_shared(),
            row.file_id_shared(),
            row.snapshot_content()
                .cloned()
                .map(TransactionJson::from_unvalidated_shared_normalized_content),
            metadata,
            None,
            None,
            None,
            false,
            None,
            None,
            false,
            target_branch_id.clone(),
        );
    }
}

async fn materialized_plugin_merge_rows<S>(
    reader: &mut TrackedStateStoreReader<S>,
    analysis: &super::analysis::MergeAnalysis,
    derived_blob_files: &DerivedPluginConflictIndex,
    target_branch_id: &SharedStr,
    resolved_plugin_rows: RawWriteBatch,
) -> Result<RawWriteBatch, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    let merge_plan = analysis
        .merge_plan()
        .expect("materialized merge rows require a merge plan");
    let key_count = merge_plan
        .picks
        .iter()
        .filter(|pick| {
            pick.selected_row.file_id().is_some_and(|file_id| {
                derived_blob_files.owner(file_id).is_some_and(|owner| {
                    owner
                        .schema_keys()
                        .iter()
                        .any(|schema_key| schema_key == pick.selected_row.schema_key())
                })
            })
        })
        .count();
    let mut keys = Vec::with_capacity(key_count);
    for pick in &merge_plan.picks {
        let Some(file_id) = pick.selected_row.file_id() else {
            continue;
        };
        if !derived_blob_files.owner(file_id).is_some_and(|owner| {
            owner
                .schema_keys()
                .iter()
                .any(|schema_key| schema_key == pick.selected_row.schema_key())
        }) {
            continue;
        }
        keys.push(TrackedStateKeyRef {
            schema_key: pick.selected_row.schema_key(),
            file_id: pick.selected_row.file_id(),
            row_pk: pick.selected_row.row_pk(),
        });
    }
    debug_assert_eq!(keys.len(), key_count);
    if keys.is_empty() {
        return Ok(resolved_plugin_rows);
    }

    let materialized_rows = reader
        .load_projected_batch_at_commit_refs(
            &analysis.commits.source_commit_id.to_string(),
            &keys,
            &ChangeRecordProjection::full(),
        )
        .await?;
    let mut rows =
        RawWriteBatch::with_capacity(materialized_rows.len() + resolved_plugin_rows.len());
    for (slot, key) in keys.into_iter().enumerate() {
        let row = materialized_rows.row(slot).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "source merge root omitted selected plugin row '{}' for file '{}'",
                    key.schema_key,
                    key.file_id.unwrap_or_default()
                ),
            )
        })?;
        push_transaction_row_from_tracked_row_ref(&mut rows, row, target_branch_id);
    }
    rows.append(resolved_plugin_rows);
    Ok(rows)
}

async fn plugin_resolution_change_stats<S>(
    reader: &mut TrackedStateStoreReader<S>,
    analysis: &super::analysis::MergeAnalysis,
    resolved_rows: &RawWriteBatch,
) -> Result<MergeChangeStats, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    if resolved_rows.is_empty() {
        return Ok(MergeChangeStats::default());
    }
    let keys = resolved_rows
        .iter()
        .map(|row| {
            Ok(TrackedStateKeyRef {
                schema_key: row.schema_key.as_str(),
                file_id: row.file_id.map(SharedStr::as_str),
                row_pk: row.row_pk.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "plugin resolution row omitted its row identity",
                    )
                })?,
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let target_rows = reader
        .load_projected_batch_at_commit_refs(
            &analysis.commits.target_commit_id.to_string(),
            &keys,
            &ChangeRecordProjection::full(),
        )
        .await?;
    let mut stats = MergeChangeStats::default();
    for (index, resolved) in resolved_rows.iter().enumerate() {
        let target = target_rows.row(index).filter(|row| !row.deleted());
        let target_metadata = target
            .and_then(MaterializedTrackedStateRowRef::metadata)
            .map(SharedStr::as_str);
        let resolved_metadata = resolved.metadata.map(TransactionJson::normalized);
        let change = match (resolved.snapshot, resolved.typed_snapshot) {
            (Some(snapshot), None) => {
                let target_snapshot = target
                    .map(|row| {
                        if row.typed_snapshot().is_some() {
                            return Err(LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "live engine target row carries an unexpected typed snapshot",
                            ));
                        }
                        row.snapshot_content()
                            .map(SharedStr::as_str)
                            .ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    "live engine target row omitted its JSON snapshot",
                                )
                            })
                    })
                    .transpose()?;
                classify_plugin_resolution(
                    target_snapshot,
                    target_metadata,
                    Some(snapshot.normalized()),
                    resolved_metadata,
                )
            }
            (None, Some(snapshot)) => {
                let target_snapshot = target
                    .map(|row| {
                        if row.snapshot_content().is_some() {
                            return Err(LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "live plugin target row carries a forbidden JSON snapshot",
                            ));
                        }
                        row.typed_snapshot().map(Arc::as_ref).ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "live plugin target row omitted its native typed snapshot",
                            )
                        })
                    })
                    .transpose()?;
                classify_plugin_resolution(
                    target_snapshot,
                    target_metadata,
                    Some(snapshot.as_ref()),
                    resolved_metadata,
                )
            }
            (None, None) => target.map(|_| PluginResolutionChange::Removed),
            (Some(_), Some(_)) => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "resolved merge row carries both JSON and native typed snapshots",
                ));
            }
        };
        match change {
            Some(PluginResolutionChange::Added) => stats.added += 1,
            Some(PluginResolutionChange::Modified) => stats.modified += 1,
            Some(PluginResolutionChange::Removed) => stats.removed += 1,
            None => {}
        }
    }
    stats.total = stats.added + stats.modified + stats.removed;
    Ok(stats)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PluginResolutionChange {
    Added,
    Modified,
    Removed,
}

fn classify_plugin_resolution<T: PartialEq + ?Sized>(
    target_snapshot: Option<&T>,
    target_metadata: Option<&str>,
    resolved_snapshot: Option<&T>,
    resolved_metadata: Option<&str>,
) -> Option<PluginResolutionChange> {
    match (target_snapshot, resolved_snapshot) {
        (None, None) => None,
        (None, Some(_)) => Some(PluginResolutionChange::Added),
        (Some(_), None) => Some(PluginResolutionChange::Removed),
        (Some(target), Some(resolved)) => {
            if target == resolved && target_metadata == resolved_metadata {
                None
            } else {
                Some(PluginResolutionChange::Modified)
            }
        }
    }
}

fn preview_from_analysis(
    target_branch_id: &str,
    source_branch_id: &str,
    analysis: &super::analysis::MergeAnalysis,
    derived_blob_files: &DerivedPluginConflictIndex,
    plugin_resolution_stats: &MergeChangeStats,
) -> Result<MergeBranchPreview, LixError> {
    let conflicts = match analysis.merge_plan() {
        // Every same-row conflict has a deterministic host LWW result. An
        // optional merger improves overlapping columns but is not required
        // for the preview to be mergeable.
        Some(_) => Vec::new(),
        _ => analysis
            .conflict_batch()
            .map(|batch| {
                batch
                    .iter()
                    .filter(|conflict| {
                        !is_derived_blob_conflict(conflict.tracked(), derived_blob_files)
                    })
                    .map(merge_conflict_from_analysis)
                    .collect::<Result<Vec<_>, LixError>>()
            })
            .transpose()?
            .unwrap_or_default(),
    };
    Ok(MergeBranchPreview {
        outcome: merge_branch_outcome_from_analysis(analysis.outcome),
        target_branch_id: target_branch_id.to_string(),
        source_branch_id: source_branch_id.to_string(),
        base_commit_id: analysis.commits.base_commit_id.to_string(),
        target_head_commit_id: analysis.commits.target_commit_id.to_string(),
        source_head_commit_id: analysis.commits.source_commit_id.to_string(),
        change_stats: merge_change_stats_with_plugin_resolutions(
            &analysis.stats,
            plugin_resolution_stats,
        ),
        conflicts,
    })
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

fn merge_change_stats_with_plugin_resolutions(
    stats: &MergeStats,
    plugin_resolution_stats: &MergeChangeStats,
) -> MergeChangeStats {
    MergeChangeStats {
        total: stats.total + plugin_resolution_stats.total,
        added: stats.added + plugin_resolution_stats.added,
        modified: stats.modified + plugin_resolution_stats.modified,
        removed: stats.removed + plugin_resolution_stats.removed,
    }
}

fn merge_conflict_from_analysis(
    conflict: AnalysisMergeConflict<'_>,
) -> Result<MergeConflict, LixError> {
    Ok(MergeConflict {
        kind: match conflict.kind() {
            AnalysisMergeConflictKind::SameRowChanged => MergeConflictKind::SameRowChanged,
        },
        schema_key: conflict.schema_key().to_owned(),
        row_pk: conflict.row_pk().as_json_array_value()?,
        file_id: conflict.file_id().map(str::to_owned),
        target: merge_conflict_side_from_analysis(conflict.target()),
        source: merge_conflict_side_from_analysis(conflict.source()),
    })
}

fn merge_conflict_side_from_analysis(side: AnalysisMergeConflictSide<'_>) -> MergeConflictSide {
    MergeConflictSide {
        kind: match side.kind() {
            AnalysisMergeConflictChangeKind::Added => MergeConflictChangeKind::Added,
            AnalysisMergeConflictChangeKind::Modified => MergeConflictChangeKind::Modified,
            AnalysisMergeConflictChangeKind::Removed => MergeConflictChangeKind::Removed,
        },
        before_change_id: side.before_change_id().map(|id| id.to_string()),
        after_change_id: side.after_change_id().map(|id| id.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};

    fn descriptor_row(
        file_id: &str,
        directory_id: Option<&str>,
        name: &str,
    ) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            row_pk: RowPk::single(file_id),
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_owned(),
            file_id: Some(file_id.to_string()),
            snapshot_content: Some(
                json!({
                    "id": file_id,
                    "directory_id": directory_id,
                    "name": name,
                })
                .to_string()
                .into(),
            ),
            typed_snapshot: None,
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
            change_id: ChangeId::for_test_label("descriptor-change"),
            commit_id: CommitId::for_test_label("descriptor-commit"),
        }
    }

    #[test]
    fn plugin_resolution_stats_follow_actual_target_delta() {
        assert_eq!(
            classify_plugin_resolution(Some("target"), None, Some("target"), None),
            None,
            "taking the target is not a change"
        );
        assert_eq!(
            classify_plugin_resolution(Some("target"), None, None, None),
            Some(PluginResolutionChange::Removed),
            "deleting a modified source row is a removal from the target"
        );
        assert_eq!(
            classify_plugin_resolution(None, None, Some("replacement"), None),
            Some(PluginResolutionChange::Added)
        );
        assert_eq!(
            classify_plugin_resolution(
                Some("target"),
                Some("target-meta"),
                Some("replacement"),
                None,
            ),
            Some(PluginResolutionChange::Modified)
        );
        assert_eq!(
            classify_plugin_resolution(Some("target"), Some("target-meta"), Some("target"), None,),
            Some(PluginResolutionChange::Modified),
            "metadata-only resolver effects remain visible"
        );
    }

    fn owner_row(file_id: &str, incarnation: &str) -> MaterializedTrackedStateRow {
        let owner = PluginFileOwner::new(
            file_id,
            "plugin_csv",
            vec!["csv_row".to_owned(), "csv_table".to_owned()],
        )
        .unwrap();
        MaterializedTrackedStateRow {
            row_pk: RowPk::single(PLUGIN_OWNER_KEY),
            schema_key: "lix_key_value".to_owned(),
            file_id: Some(file_id.to_owned()),
            snapshot_content: Some(owner.to_snapshot().unwrap().to_string().into()),
            typed_snapshot: None,
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
            change_id: ChangeId::for_test_label(incarnation),
            commit_id: CommitId::for_test_label("owner-commit"),
        }
    }

    #[test]
    fn certified_rows_must_match_merge_analysis_roots() {
        let batch = crate::tracked_state::MaterializedTrackedStateBatch::from_rows(vec![
            MaterializedTrackedStateRow {
                row_pk: RowPk::single("certified-row"),
                schema_key: "certified_schema".to_owned(),
                file_id: Some("certified-file".to_owned()),
                snapshot_content: Some(r#"{"value":"base"}"#.into()),
                typed_snapshot: None,
                metadata: None,
                deleted: false,
                created_at: "2026-01-01T00:00:00Z".to_owned(),
                updated_at: "2026-01-01T00:00:00Z".to_owned(),
                change_id: ChangeId::for_test_label("certified-base-change"),
                commit_id: CommitId::for_test_label("certified-base-commit"),
            },
        ])
        .expect("certified base fixture should materialize");

        for side in ["base", "target", "source"] {
            assert!(
                verify_historical_conflict_row_ref(Some(batch.row(0)), None, side).is_err(),
                "{side} certified rows must have matching tracked-root entries"
            );
        }
    }

    #[test]
    fn resolver_owner_requires_one_live_file_incarnation() {
        let base = owner_row("01920000-0000-7000-8000-0000000000a2", "incarnation-a");
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

        let recreated = owner_row("01920000-0000-7000-8000-0000000000a2", "incarnation-b");
        assert!(
            common_live_plugin_owner(Some(&base), Some(&recreated), Some(&base))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn common_descriptor_requires_every_live_historical_root() {
        let base = descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            Some("directory-a"),
            "readme.md",
        );
        assert!(
            common_historical_file_descriptor(
                "01920000-0000-7000-8000-0000000000a2",
                Some(base.clone()),
                Some(base.clone()),
                Some(base.clone()),
            )
            .is_some()
        );
        assert!(
            common_historical_file_descriptor(
                "01920000-0000-7000-8000-0000000000a2",
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
                "01920000-0000-7000-8000-0000000000a2",
                Some(base.clone()),
                Some(tombstone),
                Some(base.clone()),
            )
            .is_none()
        );

        let renamed = descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            Some("directory-a"),
            "guide.md",
        );
        assert!(
            common_historical_file_descriptor(
                "01920000-0000-7000-8000-0000000000a2",
                Some(base.clone()),
                Some(renamed),
                Some(base),
            )
            .is_none()
        );
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
}
