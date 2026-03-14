use std::collections::BTreeSet;

use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::filesystem::pending_file_writes::PendingFileWrite;
use crate::functions::SharedFunctionProvider;
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::shared_path::PreparedExecutionContext;
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
    pub(crate) fn coalesce_exact_filesystem_tracked_units(&mut self) {
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
                            try_merge_exact_filesystem_tracked_plans(current, &next)
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
        plan.coalesce_exact_filesystem_tracked_units();
        Some(plan)
    }
}

fn try_merge_exact_filesystem_tracked_plans(
    current: &mut TrackedWriteTxnPlan,
    next: &TrackedWriteTxnPlan,
) -> bool {
    if !tracked_plan_is_coalescible_exact_filesystem(current)
        || !tracked_plan_is_coalescible_exact_filesystem(next)
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
        || current.public_write.planned_write.command.operation_kind
            != next.public_write.planned_write.command.operation_kind
        || current.execution.append_preconditions.write_lane
            != next.execution.append_preconditions.write_lane
        || !append_expected_tip_compatible(
            &current.execution.append_preconditions.expected_tip,
            &next.execution.append_preconditions.expected_tip,
        )
        || current.writer_key != next.writer_key
        || !lazy_exact_file_ids_disjoint(
            &current.execution.lazy_exact_file_updates,
            &next.execution.lazy_exact_file_updates,
        )
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
    current
        .execution
        .lazy_exact_file_updates
        .extend(next.execution.lazy_exact_file_updates.clone());
    current.execution.persist_filesystem_payloads_before_write |=
        next.execution.persist_filesystem_payloads_before_write;
    current
        .execution
        .filesystem_payload_changes_committed_by_write |=
        next.execution.filesystem_payload_changes_committed_by_write;
    merge_plan_effects(
        &mut current.execution.semantic_effects,
        &next.execution.semantic_effects,
    );
    current
        .pending_file_writes
        .extend(next.pending_file_writes.clone());
    true
}

fn tracked_plan_is_coalescible_exact_filesystem(plan: &TrackedWriteTxnPlan) -> bool {
    matches!(
        plan.public_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    ) && plan.execution.domain_change_batch.is_none()
        && !plan.execution.lazy_exact_file_updates.is_empty()
}

fn append_expected_tip_compatible(
    left: &crate::state::commit::AppendExpectedTip,
    right: &crate::state::commit::AppendExpectedTip,
) -> bool {
    match (left, right) {
        (
            crate::state::commit::AppendExpectedTip::CurrentTip,
            crate::state::commit::AppendExpectedTip::CurrentTip,
        ) => true,
        (
            crate::state::commit::AppendExpectedTip::CommitId(left),
            crate::state::commit::AppendExpectedTip::CommitId(right),
        ) => left == right,
        (
            crate::state::commit::AppendExpectedTip::CreateIfMissing,
            crate::state::commit::AppendExpectedTip::CreateIfMissing,
        ) => true,
        _ => false,
    }
}

fn lazy_exact_file_ids_disjoint(
    left: &[crate::sql::public::planner::ir::LazyExactFileUpdate],
    right: &[crate::sql::public::planner::ir::LazyExactFileUpdate],
) -> bool {
    let mut seen = BTreeSet::new();
    for update in left {
        for file_id in update.file_ids() {
            seen.insert(file_id.to_string());
        }
    }
    for update in right {
        for file_id in update.file_ids() {
            if seen.contains(file_id) {
                return false;
            }
        }
    }
    true
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
