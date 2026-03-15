use std::collections::{BTreeMap, BTreeSet};

use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::filesystem::pending_file_writes::PendingFileWrite;
use crate::functions::SharedFunctionProvider;
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::shared_path::PreparedExecutionContext;
use crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch;
use crate::sql::public::runtime::{
    build_tracked_write_txn_plan, PublicWriteExecutionPartition, TrackedWriteTxnPlan,
    UntrackedWriteExecution,
};

use super::contracts::execution_plan::ExecutionPlan;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteTxnRunMode {
    Owned,
    Borrowed,
}

#[derive(Clone)]
pub(crate) struct PublicUntrackedWriteTxnPlan {
    pub(crate) execution: UntrackedWriteExecution,
    pub(crate) pending_file_writes: Vec<PendingFileWrite>,
    pub(crate) pending_file_delete_targets: BTreeSet<(String, String)>,
    pub(crate) functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    pub(crate) settings: DeterministicSettings,
    pub(crate) sequence_start: i64,
    pub(crate) writer_key: Option<String>,
}

#[derive(Clone)]
pub(crate) struct InternalWriteTxnPlan {
    pub(crate) plan: ExecutionPlan,
    pub(crate) pending_file_writes: Vec<PendingFileWrite>,
    pub(crate) pending_file_delete_targets: BTreeSet<(String, String)>,
    pub(crate) functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    pub(crate) settings: DeterministicSettings,
    pub(crate) sequence_start: i64,
    pub(crate) writer_key: Option<String>,
}

#[derive(Clone)]
pub(crate) enum WriteTxnUnit {
    PublicTracked(TrackedWriteTxnPlan),
    PublicUntracked(PublicUntrackedWriteTxnPlan),
    Internal(InternalWriteTxnPlan),
}

#[derive(Clone, Default)]
pub(crate) struct WriteTxnPlan {
    pub(crate) units: Vec<WriteTxnUnit>,
}

impl WriteTxnPlan {
    pub(crate) fn extend(&mut self, other: WriteTxnPlan) {
        self.units.extend(other.units);
        self.coalesce_filesystem_tracked_units();
    }

    pub(crate) fn bind_runtime(
        &mut self,
        settings: DeterministicSettings,
        sequence_start: i64,
        functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    ) {
        for unit in &mut self.units {
            match unit {
                WriteTxnUnit::PublicTracked(tracked) => {
                    tracked.functions = functions.clone();
                }
                WriteTxnUnit::PublicUntracked(untracked) => {
                    untracked.settings = settings;
                    untracked.sequence_start = sequence_start;
                    untracked.functions = functions.clone();
                }
                WriteTxnUnit::Internal(internal) => {
                    internal.settings = settings;
                    internal.sequence_start = sequence_start;
                    internal.functions = functions.clone();
                }
            }
        }
    }

    pub(crate) fn coalesce_filesystem_tracked_units(&mut self) {
        let mut coalesced = Vec::with_capacity(self.units.len());
        for unit in std::mem::take(&mut self.units) {
            match unit {
                WriteTxnUnit::PublicTracked(next)
                    if coalesced
                        .last_mut()
                        .and_then(|current| match current {
                            WriteTxnUnit::PublicTracked(current) => Some(current),
                            _ => None,
                        })
                        .is_some_and(|current| {
                            try_merge_filesystem_tracked_plans(current, &next)
                        }) => {}
                other => coalesced.push(other),
            }
        }
        self.units = coalesced;
    }
}

pub(crate) fn build_write_txn_plan(
    prepared: &PreparedExecutionContext,
    writer_key: Option<&str>,
) -> Option<WriteTxnPlan> {
    let mut units = Vec::new();

    if let Some(public_write) = prepared.public_write.as_ref() {
        if let Some(execution) = public_write.execution.as_ref() {
            for partition in &execution.partitions {
                match partition {
                    PublicWriteExecutionPartition::Tracked(tracked) => {
                        units.push(WriteTxnUnit::PublicTracked(build_tracked_write_txn_plan(
                            public_write,
                            tracked,
                            prepared,
                            writer_key,
                        )));
                    }
                    PublicWriteExecutionPartition::Untracked(untracked) => {
                        units.push(WriteTxnUnit::PublicUntracked(PublicUntrackedWriteTxnPlan {
                            execution: untracked.clone(),
                            pending_file_writes: prepared.intent.pending_file_writes.clone(),
                            pending_file_delete_targets: prepared
                                .intent
                                .pending_file_delete_targets
                                .clone(),
                            functions: prepared.functions.clone(),
                            settings: prepared.settings,
                            sequence_start: prepared.sequence_start,
                            writer_key: writer_key.map(str::to_string),
                        }));
                    }
                }
            }
        }
    } else if !prepared.plan.requirements.read_only_query {
        units.push(WriteTxnUnit::Internal(InternalWriteTxnPlan {
            plan: prepared.plan.clone(),
            pending_file_writes: prepared.intent.pending_file_writes.clone(),
            pending_file_delete_targets: prepared.intent.pending_file_delete_targets.clone(),
            functions: prepared.functions.clone(),
            settings: prepared.settings,
            sequence_start: prepared.sequence_start,
            writer_key: writer_key.map(str::to_string),
        }));
    }

    if units.is_empty() {
        None
    } else {
        let mut plan = WriteTxnPlan { units };
        plan.coalesce_filesystem_tracked_units();
        Some(plan)
    }
}

fn try_merge_filesystem_tracked_plans(
    current: &mut TrackedWriteTxnPlan,
    next: &TrackedWriteTxnPlan,
) -> bool {
    if !tracked_plan_is_coalescible_filesystem(current)
        || !tracked_plan_is_coalescible_filesystem(next)
    {
        return false;
    }
    if current
        .public_write
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        != next
            .public_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
        || current.execution.create_preconditions.write_lane
            != next.execution.create_preconditions.write_lane
        || !create_commit_expected_head_compatible(
            &current.execution.create_preconditions.expected_head,
            &next.execution.create_preconditions.expected_head,
        )
        || current.writer_key != next.writer_key
        || !tracked_plan_entity_targets_disjoint(current, next)
    {
        return false;
    }

    for requirement in &next.execution.schema_live_table_requirements {
        if !current
            .execution
            .schema_live_table_requirements
            .contains(requirement)
        {
            current
                .execution
                .schema_live_table_requirements
                .push(requirement.clone());
        }
    }
    current.execution.lazy_exact_file_updates = merge_lazy_exact_file_updates(
        &current.execution.lazy_exact_file_updates,
        &next.execution.lazy_exact_file_updates,
    );
    current.execution.persist_filesystem_payloads_before_write |=
        next.execution.persist_filesystem_payloads_before_write;
    current
        .execution
        .filesystem_payload_changes_committed_by_write |=
        next.execution.filesystem_payload_changes_committed_by_write;
    current.execution.domain_change_batch = merge_optional_domain_change_batches(
        current.execution.domain_change_batch.as_ref(),
        next.execution.domain_change_batch.as_ref(),
    );
    merge_plan_effects(
        &mut current.execution.semantic_effects,
        &next.execution.semantic_effects,
    );
    current.public_writes.extend(next.public_writes.clone());
    current
        .pending_file_writes
        .extend(next.pending_file_writes.clone());
    true
}

fn tracked_plan_is_coalescible_filesystem(plan: &TrackedWriteTxnPlan) -> bool {
    matches!(
        plan.public_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    )
}

fn create_commit_expected_head_compatible(
    left: &crate::state::commit::CreateCommitExpectedHead,
    right: &crate::state::commit::CreateCommitExpectedHead,
) -> bool {
    match (left, right) {
        (
            crate::state::commit::CreateCommitExpectedHead::CurrentHead,
            crate::state::commit::CreateCommitExpectedHead::CurrentHead,
        ) => true,
        (
            crate::state::commit::CreateCommitExpectedHead::CommitId(left),
            crate::state::commit::CreateCommitExpectedHead::CommitId(right),
        ) => left == right,
        (
            crate::state::commit::CreateCommitExpectedHead::CreateIfMissing,
            crate::state::commit::CreateCommitExpectedHead::CreateIfMissing,
        ) => true,
        _ => false,
    }
}

fn tracked_plan_entity_targets_disjoint(
    left: &TrackedWriteTxnPlan,
    right: &TrackedWriteTxnPlan,
) -> bool {
    let left_targets = tracked_plan_entity_targets(left);
    let right_targets = tracked_plan_entity_targets(right);
    left_targets.is_disjoint(&right_targets)
}

fn merge_plan_effects(current: &mut PlanEffects, next: &PlanEffects) {
    current
        .state_commit_stream_changes
        .extend(next.state_commit_stream_changes.clone());
    if next.next_active_version_id.is_some() {
        current.next_active_version_id = next.next_active_version_id.clone();
    }
    current
        .file_cache_refresh_targets
        .extend(next.file_cache_refresh_targets.clone());
}

fn tracked_plan_entity_targets(plan: &TrackedWriteTxnPlan) -> BTreeSet<(String, String, String)> {
    let mut targets = BTreeSet::new();
    if let Some(batch) = plan.execution.domain_change_batch.as_ref() {
        for change in &batch.changes {
            if change.schema_key == "lix_directory_descriptor" {
                continue;
            }
            targets.insert((
                change.entity_id.clone(),
                change.schema_key.clone(),
                change.version_id.clone(),
            ));
        }
    }
    for update in &plan.execution.lazy_exact_file_updates {
        match update {
            crate::sql::public::planner::ir::LazyExactFileUpdate::Metadata(update) => {
                targets.insert((
                    update.file_id.clone(),
                    "lix_file_descriptor".to_string(),
                    update.version_id.clone(),
                ));
            }
            crate::sql::public::planner::ir::LazyExactFileUpdate::Data(update) => {
                targets.insert((
                    update.file_id.clone(),
                    "lix_binary_blob_ref".to_string(),
                    update.version_id.clone(),
                ));
            }
            crate::sql::public::planner::ir::LazyExactFileUpdate::Delete(update) => {
                for file_id in &update.file_ids {
                    targets.insert((
                        file_id.clone(),
                        "lix_file_descriptor".to_string(),
                        update.version_id.clone(),
                    ));
                    targets.insert((
                        file_id.clone(),
                        "lix_binary_blob_ref".to_string(),
                        update.version_id.clone(),
                    ));
                }
            }
        }
    }
    targets
}

fn merge_optional_domain_change_batches(
    left: Option<&DomainChangeBatch>,
    right: Option<&DomainChangeBatch>,
) -> Option<DomainChangeBatch> {
    match (left, right) {
        (None, None) => None,
        (Some(batch), None) | (None, Some(batch)) => Some(batch.clone()),
        (Some(left), Some(right)) => {
            let mut merged = BTreeMap::new();
            for change in left.changes.iter().chain(right.changes.iter()) {
                merged.insert(
                    (
                        change.entity_id.clone(),
                        change.schema_key.clone(),
                        change.version_id.clone(),
                    ),
                    change.clone(),
                );
            }
            let mut semantic_effects = left.semantic_effects.clone();
            semantic_effects.extend(right.semantic_effects.clone());
            Some(DomainChangeBatch {
                changes: merged.into_values().collect(),
                write_lane: left.write_lane.clone(),
                writer_key: left.writer_key.clone().or_else(|| right.writer_key.clone()),
                semantic_effects,
            })
        }
    }
}

fn merge_lazy_exact_file_updates(
    left: &[crate::sql::public::planner::ir::LazyExactFileUpdate],
    right: &[crate::sql::public::planner::ir::LazyExactFileUpdate],
) -> Vec<crate::sql::public::planner::ir::LazyExactFileUpdate> {
    #[derive(Default)]
    struct FileMutationState {
        version_id: Option<String>,
        metadata: Option<crate::sql::public::planner::ir::OptionalTextPatch>,
        data: Option<Vec<u8>>,
        delete: bool,
    }

    let mut states: BTreeMap<String, FileMutationState> = BTreeMap::new();
    for update in left.iter().chain(right.iter()) {
        match update {
            crate::sql::public::planner::ir::LazyExactFileUpdate::Metadata(update) => {
                let state = states.entry(update.file_id.clone()).or_default();
                state.version_id = Some(update.version_id.clone());
                state.metadata = Some(update.metadata.clone());
                state.delete = false;
            }
            crate::sql::public::planner::ir::LazyExactFileUpdate::Data(update) => {
                let state = states.entry(update.file_id.clone()).or_default();
                state.version_id = Some(update.version_id.clone());
                state.data = Some(update.data.clone());
                state.delete = false;
            }
            crate::sql::public::planner::ir::LazyExactFileUpdate::Delete(update) => {
                for file_id in &update.file_ids {
                    let state = states.entry(file_id.clone()).or_default();
                    state.version_id = Some(update.version_id.clone());
                    state.metadata = None;
                    state.data = None;
                    state.delete = true;
                }
            }
        }
    }

    let mut merged = Vec::new();
    for (file_id, state) in states {
        let Some(version_id) = state.version_id else {
            continue;
        };
        if state.delete {
            merged.push(
                crate::sql::public::planner::ir::LazyExactFileUpdate::Delete(
                    crate::sql::public::planner::ir::LazyExactFileDelete {
                        file_ids: vec![file_id],
                        version_id,
                    },
                ),
            );
            continue;
        }
        if let Some(metadata) = state.metadata {
            merged.push(
                crate::sql::public::planner::ir::LazyExactFileUpdate::Metadata(
                    crate::sql::public::planner::ir::LazyExactFileMetadataUpdate {
                        file_id: file_id.clone(),
                        version_id: version_id.clone(),
                        metadata,
                    },
                ),
            );
        }
        if let Some(data) = state.data {
            merged.push(crate::sql::public::planner::ir::LazyExactFileUpdate::Data(
                crate::sql::public::planner::ir::LazyExactFileDataUpdate {
                    file_id,
                    version_id,
                    data,
                },
            ));
        }
    }
    merged
}
