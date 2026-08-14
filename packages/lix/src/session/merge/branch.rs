use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use tracing::Instrument as _;

use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchReferenceRole};
use crate::row_pk::RowPk;
use crate::forktree::{
    AuthenticatedHistoricalStateView, ForkTreeReadFacade, HistoricalStateRow, StateKey,
};
use crate::plugin::{
    ConflictRank, PLUGIN_OWNER_KEY, PluginFileOwner, PluginRegistry, PluginRegistryEntry,
    load_plugin_registry_on_historical_view,
};
use crate::storage_adapter::Storage;
use crate::transaction::types::{
    RawWriteBatch, TransactionJson, TransactionWrite, TransactionWriteMode,
};

use super::analysis::{MergeCommits, MergeOutcome};
use super::conflicts::{
    MergeConflictChangeKind as AnalysisMergeConflictChangeKind,
    MergeConflictKind as AnalysisMergeConflictKind, MergeConflictRow as AnalysisMergeConflict,
    MergeConflictSideRow as AnalysisMergeConflictSide,
};
use super::stats::MergeStats;
use crate::common::{SharedStr, compose_directory_path, compose_file_path};
use crate::session::context::SessionContext;
use crate::transaction::types::StagedCommitChangeBatchBuilder;
use crate::plugin::runtime::{
    WasmByteSource, WasmChangeEffect, WasmConflictResolution, WasmConflictTake, WasmRowConflict,
    WasmRowKey, WasmFileDescriptor, WasmHostBytes, WasmPluginSelection, WasmSourceRange,
    WasmSourceSlice,
};

use super::native::{MergeConflict as NativeMergeConflict, MergeKeyExt, MergePick, MergeRow};

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
                    let reader = transaction.branch_ref_reader_on_opening_read();
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
                    let mut reader = transaction.commit_graph_reader_on_opening_read();
                    reader.merge_base(&target_head, &source_head).await
                }
                .instrument(tracing::debug_span!(target: "lix_perf", "lix.perf.merge_base"))
                .await?;

                let facade = transaction.forktree_read_facade();
                let analysis = super::analysis::analyze(
                    &facade,
                    &active_branch_id,
                    transaction.sql_schema_snapshot(),
                    MergeCommits {
                        base_commit_id: merge_base,
                        target_commit_id: target_head,
                        source_commit_id: source_head,
                    },
                )
                .instrument(tracing::debug_span!(target: "lix_perf", "lix.perf.merge_analysis"))
                .await?;
                let historical = if analysis.conflict_batch().is_some() {
                    Some(MergeHistoricalState::open(&facade, &analysis).await?)
                } else {
                    None
                };
                let derived_blob_files = async {
                    derived_plugin_blob_conflicts(historical.as_ref(), &analysis).await
                }
                .instrument(tracing::debug_span!(target: "lix_perf", "lix.perf.merge_derived_blob_detection"))
                .await?;

                let resolvable_plugin_conflicts =
                    resolvable_plugin_conflict_keys(&analysis, &derived_blob_files);

                let plugin_resolution_stats = if analysis.outcome == MergeOutcome::MergeCommitted {
                    let plugin_conflict_groups = async {
                        plugin_merge_conflict_groups(
                            historical.as_ref().ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    "plugin conflict analysis omitted historical commit views",
                                )
                            })?,
                            &analysis,
                            &derived_blob_files,
                            &resolvable_plugin_conflicts,
                        )
                        .await
                    }
                    .instrument(tracing::debug_span!(target: "lix_perf", "lix.perf.merge_plugin_conflict_inputs"))
                    .await?;
                    let semantic_branch_id = SharedStr::from(active_branch_id.as_str());
                    let resolved_plugin_rows = resolve_plugin_merge_conflict_groups(
                        transaction,
                        plugin_conflict_groups,
                        &semantic_branch_id,
                    )
                    .instrument(tracing::debug_span!(target: "lix_perf", "lix.perf.merge_plugin_conflict_resolve"))
                    .await?;
                    async {
                        plugin_resolution_change_stats(
                            &historical.as_ref().ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    "plugin resolution omitted historical commit views",
                                )
                            })?.target,
                            &resolved_plugin_rows,
                        )
                        .await
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
                    &resolvable_plugin_conflicts,
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
    /// commit graph preserves branch ancestry while state storage
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
                let reader = transaction.branch_ref_reader_on_opening_read();
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
                let mut reader = transaction.commit_graph_reader_on_opening_read();
                reader.merge_base(&target_head, &source_head).await
            }
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.merge_base"
            ))
            .await?;
            let facade = transaction.forktree_read_facade();
            let analysis = super::analysis::analyze(
                &facade,
                &active_branch_id,
                transaction.sql_schema_snapshot(),
                MergeCommits {
                    base_commit_id: merge_base,
                    target_commit_id: target_head,
                    source_commit_id: source_head,
                },
            )
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.merge_analysis"
            ))
            .await?;
            let historical = if analysis.conflict_batch().is_some() {
                Some(MergeHistoricalState::open(&facade, &analysis).await?)
            } else {
                None
            };
            let derived_blob_files =
                async { derived_plugin_blob_conflicts(historical.as_ref(), &analysis).await }
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
            let resolvable_plugin_conflicts =
                resolvable_plugin_conflict_keys(&analysis, &derived_blob_files);
            let effective_conflicts = if resolvable_plugin_conflicts
                .covers_all_with_derived_blobs(merge_plan.conflicts.len(), &derived_blob_files)
            {
                Vec::new()
            } else {
                analysis
                    .conflict_batch()
                    .expect("mergeCommitted analysis should include a conflict batch")
                    .iter()
                    .enumerate()
                    .filter(|(index, conflict)| {
                        !is_derived_blob_conflict(conflict.tracked(), &derived_blob_files)
                            && !resolvable_plugin_conflicts.contains_index(*index)
                    })
                    .map(|(_, conflict)| conflict)
                    .collect::<Vec<_>>()
            };
            if !effective_conflicts.is_empty() {
                return Err(merge_conflict_error(
                    &effective_conflicts
                        .into_iter()
                        .map(merge_conflict_from_analysis)
                        .collect::<Result<Vec<_>, _>>()?,
                )?);
            }

            let plugin_conflict_groups = async {
                plugin_merge_conflict_groups(
                    historical.as_ref().ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "plugin conflict analysis omitted historical commit views",
                        )
                    })?,
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
            let semantic_branch_id = SharedStr::from(active_branch_id.as_str());
            let resolved_plugin_rows = resolve_plugin_merge_conflict_groups(
                transaction,
                plugin_conflict_groups,
                &semantic_branch_id,
            )
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.merge_plugin_conflict_resolve"
            ))
            .await?;

            let plugin_resolution_stats = async {
                plugin_resolution_change_stats(
                    &historical
                        .as_ref()
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "plugin resolution omitted historical commit views",
                            )
                        })?
                        .target,
                    &resolved_plugin_rows,
                )
                .await
            }
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.merge_plugin_resolution_stats"
            ))
            .await?;

            let semantic_rows = async {
                materialized_plugin_merge_rows(
                    &historical
                        .as_ref()
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "plugin materialization omitted historical commit views",
                            )
                        })?
                        .source,
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
                for pick in merge_plan.picks.iter().filter(|pick| {
                    !pick_is_derived_plugin_state(pick, &analysis.source_diff, &derived_blob_files)
                }) {
                    selected_changes.push(
                        pick.identity(&analysis.source_diff).clone(),
                        pick.selected_row(&analysis.source_diff).commit_id,
                        pick.change_id,
                        pick.selected_row(&analysis.source_diff).deleted,
                        pick.selected_row(&analysis.source_diff).created_at,
                        pick.selected_row(&analysis.source_diff).updated_at,
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

#[derive(Debug, Clone)]
struct DerivedPluginFileConflict {
    plugin: PluginRegistryEntry,
    descriptor: WasmFileDescriptor,
    conflict_indices: Vec<usize>,
    derived_blob_conflict_count: usize,
}

#[derive(Debug, Clone, Default)]
struct DerivedPluginConflictIndex {
    owners: BTreeMap<String, PluginFileOwner>,
    files: BTreeMap<String, DerivedPluginFileConflict>,
}

struct MergeHistoricalState<'a, R: ?Sized> {
    base: AuthenticatedHistoricalStateView<'a, R>,
    target: AuthenticatedHistoricalStateView<'a, R>,
    source: AuthenticatedHistoricalStateView<'a, R>,
}

impl<'a, R> MergeHistoricalState<'a, R>
where
    R: crate::storage_adapter::StorageAdapterRead,
{
    async fn open(
        facade: &'a ForkTreeReadFacade<R>,
        analysis: &super::analysis::MergeAnalysis,
    ) -> Result<Self, LixError> {
        let base = facade
            .historical_state_view(&analysis.commits.base_commit_id.to_string())
            .await?;
        let target = facade
            .historical_state_view(&analysis.commits.target_commit_id.to_string())
            .await?;
        let source = facade
            .historical_state_view(&analysis.commits.source_commit_id.to_string())
            .await?;
        Ok(Self {
            base,
            target,
            source,
        })
    }
}

impl DerivedPluginConflictIndex {
    fn context(&self, file_id: &str) -> Option<&DerivedPluginFileConflict> {
        self.files.get(file_id)
    }

    fn owner(&self, file_id: &str) -> Option<&PluginFileOwner> {
        self.owners.get(file_id)
    }

    fn contains_file(&self, file_id: &str) -> bool {
        self.owners.contains_key(file_id)
    }

    fn derived_blob_conflict_count(&self) -> usize {
        self.files
            .values()
            .map(|context| context.derived_blob_conflict_count)
            .sum()
    }
}

async fn derived_plugin_blob_conflicts<R>(
    historical: Option<&MergeHistoricalState<'_, R>>,
    analysis: &super::analysis::MergeAnalysis,
) -> Result<DerivedPluginConflictIndex, LixError>
where
    R: crate::storage_adapter::StorageAdapterRead,
{
    // A derived blob conflict is the common signal, but it is not the
    // authority for plugin ownership. Start from every conflicted file and
    // prove one *live, identical* owner across all three historical roots.
    //
    // A missing/tombstoned owner is a file-lifecycle conflict (for example,
    // delete-vs-edit), not a semantic row conflict. Letting a resolver
    // choose an row value in that case could silently pair a live semantic
    // row with a deleted file owner. Keep the whole conflict visible until a
    // first-class lifecycle conflict model exists.
    let Some(conflicts) = analysis.conflict_batch() else {
        return Ok(DerivedPluginConflictIndex::default());
    };
    let historical = historical.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "plugin conflict analysis omitted historical commit views",
        )
    })?;
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
        .map(|file_id| StateKey {
            schema_key: "lix_key_value".to_owned(),
            file_id: Some(file_id.clone()),
            row_pk: RowPk::single(PLUGIN_OWNER_KEY),
        })
        .collect::<Vec<_>>();
    let base_rows = historical.base.load_state_rows(&owner_keys).await?;
    let target_rows = historical.target.load_state_rows(&owner_keys).await?;
    let source_rows = historical.source.load_state_rows(&owner_keys).await?;
    let mut common_owners = BTreeMap::new();
    for (index, file_id) in file_ids.into_iter().enumerate() {
        let Some(owner) = common_live_plugin_owner_ref(
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
        historical_conflict_file_descriptors(historical, &candidate_file_ids).await?;
    let base_registry = load_plugin_registry_on_historical_view(
        &historical.base,
        &analysis.commits.base_commit_id.to_string(),
    )
    .await?;
    let target_registry = load_plugin_registry_on_historical_view(
        &historical.target,
        &analysis.commits.target_commit_id.to_string(),
    )
    .await?;
    let source_registry = load_plugin_registry_on_historical_view(
        &historical.source,
        &analysis.commits.source_commit_id.to_string(),
    )
    .await?;

    let mut derived = BTreeMap::new();
    let mut derived_owners = BTreeMap::new();
    for (file_id, owner) in common_owners {
        let Some(path @ Some(_)) = common_descriptors.get(&file_id).cloned() else {
            continue;
        };
        let Ok(plugin) = pinned_conflict_plugin_entry(
            &owner,
            &base_registry,
            &target_registry,
            &source_registry,
            &file_id,
        ) else {
            continue;
        };
        let descriptor = WasmFileDescriptor {
            path,
            plugin: WasmPluginSelection {
                plugin_key: plugin.key().to_owned(),
                generation: plugin.archive_blob_hash().to_owned(),
            },
        };
        let conflict_indices = conflict_indices_by_file
            .remove(&file_id)
            .unwrap_or_default();
        let merge_plan = analysis
            .merge_plan()
            .expect("derived plugin conflicts require a merge plan");
        let derived_blob_conflict_count = conflict_indices
            .iter()
            .filter(|&&index| {
                matches!(
                    merge_plan.conflicts[index].identity.schema_key(),
                    BLOB_REF_SCHEMA_KEY
                )
            })
            .count();
        derived.insert(
            file_id.clone(),
            DerivedPluginFileConflict {
                plugin,
                descriptor,
                conflict_indices,
                derived_blob_conflict_count,
            },
        );
        derived_owners.insert(file_id, owner);
    }
    Ok(DerivedPluginConflictIndex {
        owners: derived_owners,
        files: derived,
    })
}

fn common_live_plugin_owner_ref(
    base: Option<&HistoricalStateRow>,
    target: Option<&HistoricalStateRow>,
    source: Option<&HistoricalStateRow>,
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
    if base.change_id != target.change_id || base.change_id != source.change_id {
        return Ok(None);
    }
    let Some(base_snapshot) = base.snapshot_content.as_ref() else {
        return Ok(None);
    };
    if target.snapshot_content.as_ref().map(SharedStr::as_str) != Some(base_snapshot.as_str())
        || source.snapshot_content.as_ref().map(SharedStr::as_str) != Some(base_snapshot.as_str())
    {
        return Ok(None);
    }
    let Some(base_owner) = PluginFileOwner::from_historical_state_row(base)? else {
        return Ok(None);
    };
    Ok(Some(base_owner))
}

fn is_derived_blob_conflict(
    conflict: &NativeMergeConflict,
    derived_blob_files: &DerivedPluginConflictIndex,
) -> bool {
    matches!(conflict.identity.schema_key(), BLOB_REF_SCHEMA_KEY)
        && conflict
            .identity
            .file_id()
            .is_some_and(|file_id| derived_blob_files.contains_file(file_id))
}

fn pick_is_derived_plugin_state(
    pick: &MergePick,
    source_diff: &super::native::MergeDiff,
    derived_blob_files: &DerivedPluginConflictIndex,
) -> bool {
    let identity = pick.identity(source_diff);
    let Some(file_id) = identity.file_id() else {
        return false;
    };
    let Some(owner) = derived_blob_files.owner(file_id) else {
        return false;
    };
    matches!(identity.schema_key(), BLOB_REF_SCHEMA_KEY)
        || owner
            .schema_keys()
            .iter()
            .any(|schema_key| schema_key == identity.schema_key())
}

/// One historical triple for a plugin-owned semantic row. The row identity
/// remains host-owned; a Component can only choose or replace the aligned
/// value and cannot invent a different key during a merge.
#[derive(Debug, Clone)]
struct PluginMergeConflictRow {
    identity: StateKey,
    base: Option<PluginMergeConflictPayload>,
    a: Option<PluginMergeConflictPayload>,
    b: Option<PluginMergeConflictPayload>,
}

/// Payload-only conflict side. Identity lives once in the aligned batch row;
/// immutable JSON retains the shared read buffers loaded from the JSON store.
#[derive(Debug, Clone)]
struct PluginMergeConflictPayload {
    snapshot: SharedStr,
    metadata: Option<SharedStr>,
}

#[derive(Debug, Clone, Default)]
struct PluginMergeConflictBatch {
    rows: Vec<PluginMergeConflictRow>,
}

impl PluginMergeConflictBatch {
    fn push(&mut self, row: PluginMergeConflictRow) {
        self.rows.push(row);
    }

    fn sort_by_identity(&mut self) {
        self.rows
            .sort_by(|left, right| left.identity.cmp(&right.identity));
    }

    fn iter(&self) -> std::slice::Iter<'_, PluginMergeConflictRow> {
        self.rows.iter()
    }

    fn len(&self) -> usize {
        self.rows.len()
    }
}

#[derive(Debug, Clone)]
struct PluginMergeConflictGroup {
    plugin: PluginRegistryEntry,
    descriptor: WasmFileDescriptor,
    conflicts: PluginMergeConflictBatch,
}

#[derive(Debug, Clone, Default)]
struct ResolvablePluginConflicts {
    indices: Vec<usize>,
}

impl ResolvablePluginConflicts {
    fn contains_index(&self, index: usize) -> bool {
        self.indices.binary_search(&index).is_ok()
    }

    fn covers_all_with_derived_blobs(
        &self,
        conflict_count: usize,
        derived: &DerivedPluginConflictIndex,
    ) -> bool {
        self.indices
            .len()
            .checked_add(derived.derived_blob_conflict_count())
            == Some(conflict_count)
    }
}

/// Returns exactly the semantic conflict identitys that can be handed to a
/// static resolver. This deliberately does not execute Wasm: callers use it
/// both to make merge preview honest and to reject ordinary conflicts before
/// allocating a Component Store.
fn resolvable_plugin_conflict_keys(
    analysis: &super::analysis::MergeAnalysis,
    derived_blob_files: &DerivedPluginConflictIndex,
) -> ResolvablePluginConflicts {
    let Some(merge_plan) = analysis.merge_plan() else {
        return ResolvablePluginConflicts::default();
    };
    let mut eligible = ResolvablePluginConflicts::default();
    for (file_id, context) in &derived_blob_files.files {
        let owner = derived_blob_files
            .owner(file_id)
            .expect("derived plugin context has an owner");
        for &index in &context.conflict_indices {
            let conflict = &merge_plan.conflicts[index];
            if !matches!(conflict.identity.schema_key(), BLOB_REF_SCHEMA_KEY)
                && owner
                    .schema_keys()
                    .iter()
                    .any(|schema_key| schema_key == conflict.identity.schema_key())
            {
                eligible.indices.push(index);
            }
        }
    }
    eligible.indices.sort_unstable();
    eligible
}

async fn plugin_merge_conflict_groups<R>(
    historical: &MergeHistoricalState<'_, R>,
    analysis: &super::analysis::MergeAnalysis,
    derived_blob_files: &DerivedPluginConflictIndex,
    resolvable_plugin_conflicts: &ResolvablePluginConflicts,
) -> Result<Vec<PluginMergeConflictGroup>, LixError>
where
    R: crate::storage_adapter::StorageAdapterRead,
{
    let merge_plan = analysis
        .merge_plan()
        .expect("plugin conflict resolution requires a merge plan");
    let semantic_conflicts = resolvable_plugin_conflicts
        .indices
        .iter()
        .map(|&index| &merge_plan.conflicts[index])
        .collect::<Vec<_>>();
    if semantic_conflicts.is_empty() {
        return Ok(Vec::new());
    }

    let keys = semantic_conflicts
        .iter()
        .map(|conflict| StateKey {
            schema_key: conflict.identity.schema_key().to_owned(),
            file_id: conflict.identity.file_id().map(str::to_owned),
            row_pk: conflict.identity.row_pk().clone(),
        })
        .collect::<Vec<_>>();
    let base_rows = historical.base.load_state_rows(&keys).await?;
    let target_rows = historical.target.load_state_rows(&keys).await?;
    let source_rows = historical.source.load_state_rows(&keys).await?;
    if base_rows.iter().any(Option::is_none) {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "ForkTree merge base omitted a selected semantic row",
        ));
    }

    let mut groups = BTreeMap::<String, PluginMergeConflictGroup>::new();
    for (index, conflict) in semantic_conflicts.into_iter().enumerate() {
        let base = base_rows[index].as_ref();
        let target = target_rows[index].as_ref();
        let source = source_rows[index].as_ref();
        let file_id = conflict
            .identity
            .file_id()
            .expect("semantic plugin conflicts have a file id")
            .to_owned();
        let context = derived_blob_files
            .context(&file_id)
            .expect("semantic plugin conflicts have a derived owner");
        let plugin = context.plugin.clone();
        verify_historical_conflict_row_ref(base, conflict.target.before.as_ref(), "base")?;
        verify_historical_conflict_row_ref(target, conflict.target.after.as_ref(), "target")?;
        verify_historical_conflict_row_ref(source, conflict.source.after.as_ref(), "source")?;

        let (a, b) = canonical_conflict_variants_ref(conflict, target, source)?;
        let base = historical_live_payload_ref(base)?;
        let row = PluginMergeConflictRow {
            identity: conflict.identity.clone(),
            base,
            a: historical_live_payload_ref(a)?,
            b: historical_live_payload_ref(b)?,
        };
        let descriptor = context.descriptor.clone();
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
                        conflicts: PluginMergeConflictBatch { rows: vec![row] },
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
        group.conflicts.sort_by_identity();
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
async fn historical_conflict_file_descriptors<R>(
    historical: &MergeHistoricalState<'_, R>,
    file_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, Option<String>>, LixError>
where
    R: crate::storage_adapter::StorageAdapterRead,
{
    if file_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let keys = file_ids
        .iter()
        .map(|file_id| {
            Ok(StateKey {
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
    let base_rows = historical.base.load_state_rows(&keys).await?;
    let target_rows = historical.target.load_state_rows(&keys).await?;
    let source_rows = historical.source.load_state_rows(&keys).await?;

    let mut descriptors = BTreeMap::new();
    for (index, file_id) in file_ids.iter().cloned().enumerate() {
        let Some((scope_file_id, descriptor)) = common_historical_file_descriptor_ref(
            &file_id,
            base_rows[index].as_ref(),
            target_rows[index].as_ref(),
            source_rows[index].as_ref(),
        ) else {
            descriptors.insert(file_id, None);
            continue;
        };

        // A file descriptor can agree while one ancestor directory has been
        // renamed. Resolve all three full paths at their own historical roots
        // and only expose a path to the plugin when it is genuinely common.
        // A path-sensitive resolver must never receive a stale base path.
        let base_path =
            historical_file_path(&historical.base, scope_file_id.as_deref(), &descriptor).await?;
        let target_path =
            historical_file_path(&historical.target, scope_file_id.as_deref(), &descriptor).await?;
        let source_path =
            historical_file_path(&historical.source, scope_file_id.as_deref(), &descriptor).await?;
        let Some(path) = common_historical_path(base_path, target_path, source_path) else {
            descriptors.insert(file_id, None);
            continue;
        };
        descriptors.insert(file_id, Some(path));
    }
    Ok(descriptors)
}

fn historical_file_descriptor_row_ref(
    row: Option<&HistoricalStateRow>,
    expected_file_id: &str,
) -> Option<(Option<String>, HistoricalFileDescriptor)> {
    let row = row.filter(|row| !row.deleted)?;
    let snapshot = row.snapshot_content.as_ref()?;
    let descriptor = serde_json::from_str::<HistoricalFileDescriptor>(snapshot.as_str()).ok()?;
    (descriptor.id == expected_file_id).then(|| (row.key.file_id.clone(), descriptor))
}

fn common_historical_file_descriptor_ref(
    expected_file_id: &str,
    base: Option<&HistoricalStateRow>,
    target: Option<&HistoricalStateRow>,
    source: Option<&HistoricalStateRow>,
) -> Option<(Option<String>, HistoricalFileDescriptor)> {
    let base = historical_file_descriptor_row_ref(base, expected_file_id)?;
    let target = historical_file_descriptor_row_ref(target, expected_file_id)?;
    let source = historical_file_descriptor_row_ref(source, expected_file_id)?;
    (base == target && base == source).then_some(base)
}

fn common_historical_path(
    base: Option<String>,
    target: Option<String>,
    source: Option<String>,
) -> Option<String> {
    (base == target && base == source).then_some(base).flatten()
}

async fn historical_file_path<R>(
    historical: &AuthenticatedHistoricalStateView<'_, R>,
    scope_file_id: Option<&str>,
    descriptor: &HistoricalFileDescriptor,
) -> Result<Option<String>, LixError>
where
    R: crate::storage_adapter::StorageAdapterRead,
{
    let mut ancestor_names = Vec::new();
    let mut directory_id = descriptor.directory_id.clone();
    let mut visited = BTreeSet::new();
    while let Some(id) = directory_id {
        if !visited.insert(id.clone()) {
            return Ok(None);
        }
        let key = StateKey {
            schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_owned(),
            file_id: scope_file_id.map(str::to_owned),
            row_pk: RowPk::uuid_from_canonical(&id).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("validated directory ID is not a canonical UUID: {error}"),
                )
            })?,
        };
        let Some(row) = historical
            .load_state_rows(std::slice::from_ref(&key))
            .await?
            .into_iter()
            .next()
            .flatten()
        else {
            return Ok(None);
        };
        if row.deleted {
            return Ok(None);
        }
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

fn verify_historical_conflict_row_ref(
    row: Option<&HistoricalStateRow>,
    expected: Option<&MergeRow>,
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

fn historical_live_payload_ref(
    row: Option<&HistoricalStateRow>,
) -> Result<Option<PluginMergeConflictPayload>, LixError> {
    row.filter(|row| !row.deleted)
        .map(|row| {
            let snapshot = row.snapshot_content.clone().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "live plugin semantic row is missing its complete snapshot",
                )
            })?;
            Ok(PluginMergeConflictPayload {
                snapshot,
                metadata: row.metadata.clone(),
            })
        })
        .transpose()
}

fn canonical_conflict_variants_ref<'a>(
    conflict: &NativeMergeConflict,
    target: Option<&'a HistoricalStateRow>,
    source: Option<&'a HistoricalStateRow>,
) -> Result<
    (
        Option<&'a HistoricalStateRow>,
        Option<&'a HistoricalStateRow>,
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
    let target = target.filter(|row| !row.deleted);
    let source = source.filter(|row| !row.deleted);
    if ordering.is_lt() {
        Ok((target, source))
    } else {
        Ok((source, target))
    }
}

fn conflict_host_snapshot(
    payload: Option<&PluginMergeConflictPayload>,
) -> Result<Option<WasmHostBytes>, LixError> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    let snapshot = &payload.snapshot;
    let length = u64::try_from(snapshot.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            "plugin semantic snapshot length exceeds Wasm source bounds",
        )
    })?;
    // Keep the JSON-store buffer alive behind the source. The common `take(b)`
    // path moves neither the semantic bytes nor a materialized tracked row.
    let source = Arc::new(TrackedRowSnapshotSource {
        snapshot: snapshot.clone(),
    });
    Ok(Some(WasmHostBytes::Source(WasmSourceSlice {
        source,
        range: WasmSourceRange { offset: 0, length },
    })))
}

#[derive(Debug)]
struct TrackedRowSnapshotSource {
    snapshot: SharedStr,
}

impl WasmByteSource for TrackedRowSnapshotSource {
    fn len(&self) -> u64 {
        self.snapshot.len() as u64
    }

    fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>, LixError> {
        if length == 0 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "v2 byte-source reads must request bytes",
            ));
        }
        let snapshot = self.snapshot.as_str();
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
    target_branch_id: &SharedStr,
) -> Result<RawWriteBatch, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let row_count = groups.iter().try_fold(0usize, |row_count, group| {
        row_count.checked_add(group.conflicts.len()).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin conflict output row count overflowed this host",
            )
        })
    })?;
    let mut rows = RawWriteBatch::with_capacity(row_count);
    for group in groups {
        let conflicts = group
            .conflicts
            .iter()
            .enumerate()
            .map(|(ordinal, conflict)| {
                Ok(WasmRowConflict {
                    ordinal: u32::try_from(ordinal).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            "plugin conflict batch exceeds the u32 ordinal limit",
                        )
                    })?,
                    key: WasmRowKey::from_owned_parts(
                        conflict.identity.schema_key().to_owned(),
                        conflict.identity.row_pk().clone().into_parts(),
                    ),
                    base: conflict_host_snapshot(conflict.base.as_ref())?,
                    a: conflict_host_snapshot(conflict.a.as_ref())?,
                    b: conflict_host_snapshot(conflict.b.as_ref())?,
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let resolved = transaction
            .resolve_plugin_conflicts(&group.plugin, group.descriptor, conflicts)
            .await?;
        if resolved.resolutions.len() != group.conflicts.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "validated plugin conflict output lost input alignment",
            ));
        }
        for (conflict, resolution) in group.conflicts.iter().zip(resolved.resolutions) {
            push_transaction_row_from_conflict_resolution(
                &mut rows,
                conflict,
                resolution,
                target_branch_id,
            )?;
        }
    }
    Ok(rows)
}

fn push_transaction_row_from_conflict_resolution(
    rows: &mut RawWriteBatch,
    conflict: &PluginMergeConflictRow,
    resolution: WasmConflictResolution<WasmHostBytes>,
    target_branch_id: &SharedStr,
) -> Result<(), LixError> {
    match resolution {
        WasmConflictResolution::Take(side) => {
            let (side_name, selected) = match side {
                WasmConflictTake::Base => ("base", &conflict.base),
                WasmConflictTake::A => ("a", &conflict.a),
                WasmConflictTake::B => ("b", &conflict.b),
            };
            let row = selected.as_ref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "plugin conflict resolver selected absent {side_name} snapshot; use delete for a tombstone"
                    ),
                )
            })?;
            push_transaction_row_from_conflict_payload(
                rows,
                &conflict.identity,
                row,
                target_branch_id,
            );
            Ok(())
        }
        WasmConflictResolution::Delete => {
            push_plugin_transaction_row(rows, &conflict.identity, None, None, target_branch_id);
            Ok(())
        }
        WasmConflictResolution::Replace {
            snapshot_content,
            effect,
        } => {
            let WasmHostBytes::CanonicalJson(snapshot) = snapshot_content else {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "validated conflict replacement is not canonical JSON",
                ));
            };
            // A replacement's effect is part of the v2 durable semantic
            // contract. Do not inherit the selected B row's metadata: that
            // could retain a stale format-only marker after a content merge,
            // or silently turn a declared format-only resolution into content.
            let metadata = match effect {
                WasmChangeEffect::Content => None,
                WasmChangeEffect::FormatOnly => {
                    Some(TransactionJson::from_unvalidated_shared_normalized_content(
                        SharedStr::from_static(r#"{"impact":"format"}"#),
                    ))
                }
            };
            push_plugin_transaction_row(
                rows,
                &conflict.identity,
                Some(TransactionJson::from_canonical_batch(snapshot)),
                metadata,
                target_branch_id,
            );
            Ok(())
        }
    }
}

fn push_plugin_transaction_row(
    rows: &mut RawWriteBatch,
    identity: &StateKey,
    snapshot: Option<TransactionJson>,
    metadata: Option<TransactionJson>,
    target_branch_id: &SharedStr,
) {
    rows.push_parts(
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

fn push_transaction_row_from_conflict_payload(
    rows: &mut RawWriteBatch,
    identity: &StateKey,
    payload: &PluginMergeConflictPayload,
    target_branch_id: &SharedStr,
) {
    push_plugin_transaction_row(
        rows,
        identity,
        Some(TransactionJson::from_unvalidated_shared_normalized_content(
            payload.snapshot.clone(),
        )),
        payload
            .metadata
            .clone()
            .map(TransactionJson::from_unvalidated_shared_normalized_content),
        target_branch_id,
    );
}

fn push_transaction_row_from_tracked_row_ref(
    rows: &mut RawWriteBatch,
    row: &HistoricalStateRow,
    target_branch_id: &SharedStr,
) {
    let snapshot = row
        .snapshot_content
        .clone()
        .map(TransactionJson::from_unvalidated_shared_normalized_content);
    let metadata = row
        .metadata
        .clone()
        .map(TransactionJson::from_unvalidated_shared_normalized_content);
    rows.push_parts(
        Some(row.key.row_pk.clone()),
        row.key.schema_key.clone().into(),
        row.key.file_id.clone().map(Into::into),
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

async fn materialized_plugin_merge_rows<R>(
    source: &AuthenticatedHistoricalStateView<'_, R>,
    analysis: &super::analysis::MergeAnalysis,
    derived_blob_files: &DerivedPluginConflictIndex,
    target_branch_id: &SharedStr,
    resolved_plugin_rows: RawWriteBatch,
) -> Result<RawWriteBatch, LixError>
where
    R: crate::storage_adapter::StorageAdapterRead,
{
    let merge_plan = analysis
        .merge_plan()
        .expect("materialized merge rows require a merge plan");
    let key_count = merge_plan
        .picks
        .iter()
        .filter(|pick| {
            pick.identity(&analysis.source_diff)
                .file_id()
                .is_some_and(|file_id| {
                    derived_blob_files.owner(file_id).is_some_and(|owner| {
                        owner.schema_keys().iter().any(|schema_key| {
                            schema_key == pick.identity(&analysis.source_diff).schema_key()
                        })
                    })
                })
        })
        .count();
    let mut keys = Vec::with_capacity(key_count);
    for pick in &merge_plan.picks {
        let identity = pick.identity(&analysis.source_diff);
        let Some(file_id) = identity.file_id() else {
            continue;
        };
        if !derived_blob_files.owner(file_id).is_some_and(|owner| {
            owner
                .schema_keys()
                .iter()
                .any(|schema_key| schema_key == identity.schema_key())
        }) {
            continue;
        }
        keys.push(StateKey {
            schema_key: identity.schema_key().to_owned(),
            file_id: identity.file_id().map(str::to_owned),
            row_pk: identity.row_pk().clone(),
        });
    }
    debug_assert_eq!(keys.len(), key_count);
    if keys.is_empty() {
        return Ok(resolved_plugin_rows);
    }

    let materialized_rows = source.load_state_rows(&keys).await?;
    let mut rows =
        RawWriteBatch::with_capacity(materialized_rows.len() + resolved_plugin_rows.len());
    for (slot, key) in keys.into_iter().enumerate() {
        let row = materialized_rows[slot].as_ref().ok_or_else(|| {
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

async fn plugin_resolution_change_stats<R>(
    target: &AuthenticatedHistoricalStateView<'_, R>,
    resolved_rows: &RawWriteBatch,
) -> Result<MergeChangeStats, LixError>
where
    R: crate::storage_adapter::StorageAdapterRead,
{
    if resolved_rows.is_empty() {
        return Ok(MergeChangeStats::default());
    }
    let keys = resolved_rows
        .iter()
        .map(|row| {
            Ok(StateKey {
                schema_key: row.schema_key.to_string(),
                file_id: row.file_id.as_ref().map(ToString::to_string),
                row_pk: row
                    .row_pk
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "plugin resolution row omitted its row identity",
                        )
                    })?
                    .clone(),
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let target_rows = target.load_state_rows(&keys).await?;
    let mut stats = MergeChangeStats::default();
    for (index, resolved) in resolved_rows.iter().enumerate() {
        let target = target_rows[index].as_ref().filter(|row| !row.deleted);
        let target_snapshot = target
            .map(|row| {
                row.snapshot_content.as_ref().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "live plugin target row omitted its snapshot",
                    )
                })
            })
            .transpose()?;
        match classify_plugin_resolution(
            target_snapshot.map(SharedStr::as_str),
            target
                .and_then(|row| row.metadata.as_ref())
                .map(SharedStr::as_str),
            resolved.snapshot.map(TransactionJson::normalized),
            resolved.metadata.map(TransactionJson::normalized),
        ) {
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

fn classify_plugin_resolution(
    target_snapshot: Option<&str>,
    target_metadata: Option<&str>,
    resolved_snapshot: Option<&str>,
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
    resolvable_plugin_conflicts: &ResolvablePluginConflicts,
    plugin_resolution_stats: &MergeChangeStats,
) -> Result<MergeBranchPreview, LixError> {
    let conflicts = match analysis.merge_plan() {
        Some(plan)
            if resolvable_plugin_conflicts
                .covers_all_with_derived_blobs(plan.conflicts.len(), derived_blob_files) =>
        {
            Vec::new()
        }
        _ => analysis
            .conflict_batch()
            .map(|batch| {
                batch
                    .iter()
                    .enumerate()
                    .filter(|(index, conflict)| {
                        !is_derived_blob_conflict(conflict.tracked(), derived_blob_files)
                            && !resolvable_plugin_conflicts.contains_index(*index)
                    })
                    .map(|(_, conflict)| conflict)
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

#[expect(clippy::unnecessary_wraps)]
fn merge_conflict_error(conflicts: &[MergeConflict]) -> Result<LixError, LixError> {
    let conflict_count = conflicts.len();
    Ok(LixError::new(
        LixError::CODE_MERGE_CONFLICT,
        format!("merge_branch found {conflict_count} tracked-state conflict(s)"),
    )
    .with_hint("Resolve the conflicting rows in the target branch, then retry the merge.")
    .with_details(json!({
        "conflicts": conflicts.iter()
            .map(merge_conflict_details)
            .collect::<Vec<_>>(),
    })))
}

fn merge_conflict_details(conflict: &MergeConflict) -> serde_json::Value {
    json!({
        "kind": match conflict.kind {
            MergeConflictKind::SameRowChanged => "sameRowChanged",
        },
        "schemaKey": conflict.schema_key,
        "entityPk": conflict.row_pk,
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
