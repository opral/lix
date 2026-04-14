use std::collections::BTreeSet;

use crate::sql::PlanEffects;
use crate::sql::SessionStateDelta;
use crate::streams::{
    state_commit_stream_changes_from_changes, StateChangeRecord, StateCommitStreamOperation,
    StateCommitStreamRuntimeMetadata,
};
use crate::transaction::pipeline::WriteExecutionOutcome;
use crate::transaction::{PendingCommitState, TrackedTxnUnit, WriteExecutionContext};
use crate::{LixBackendTransaction, LixError, QueryResult};

pub(crate) async fn execute_public_tracked_transaction_write_unit_with_transaction(
    execution_context: &dyn WriteExecutionContext,
    transaction: &mut dyn LixBackendTransaction,
    unit: &TrackedTxnUnit,
    pending_commit_state: Option<&mut Option<PendingCommitState>>,
) -> Result<Option<WriteExecutionOutcome>, LixError> {
    let tracked_write_outcome = execution_context
        .execute_public_tracked_append_txn_with_transaction(transaction, unit, pending_commit_state)
        .await?;

    let plan_effects_override = if tracked_write_outcome.plugin_changes_committed {
        if unit.has_compiler_only_filesystem_changes() {
            plan_effects_from_tracked_changes(
                &tracked_write_outcome.applied_changes,
                unit.public_write
                    .contract
                    .operation_kind
                    .state_commit_stream_operation(),
                unit.writer_key.as_deref(),
                tracked_write_outcome.next_active_version_id.clone(),
            )?
        } else {
            unit.execution.semantic_effects.clone()
        }
    } else {
        PlanEffects::default()
    };

    Ok(Some(WriteExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        direct_write_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: tracked_write_outcome.plugin_changes_committed,
        canonical_commit_receipt: tracked_write_outcome.receipt,
        plan_effects_override: Some(plan_effects_override),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: tracked_write_outcome.plugin_changes_committed
            && unit.should_emit_observe_tick(),
    }))
}

fn plan_effects_from_tracked_changes<Change: StateChangeRecord>(
    changes: &[Change],
    stream_operation: StateCommitStreamOperation,
    writer_key: Option<&str>,
    next_active_version_id: Option<String>,
) -> Result<PlanEffects, LixError> {
    Ok(PlanEffects {
        state_commit_stream_changes: state_commit_stream_changes_from_changes(
            changes,
            stream_operation,
            StateCommitStreamRuntimeMetadata::from_runtime_writer_key(writer_key),
        )?,
        session_delta: SessionStateDelta {
            next_active_version_id,
            next_active_account_ids: None,
            persist_workspace: false,
        },
        file_cache_refresh_targets: file_cache_refresh_targets_from_changes(changes),
    })
}

fn file_cache_refresh_targets_from_changes<Change: StateChangeRecord>(
    changes: &[Change],
) -> BTreeSet<(String, String)> {
    changes
        .iter()
        .filter(|change| change.file_id() != Some("lix"))
        .filter(|change| change.schema_key() != "lix_file_descriptor")
        .filter(|change| change.schema_key() != "lix_directory_descriptor")
        .filter_map(|change| {
            change
                .file_id()
                .map(|file_id| (file_id.to_string(), change.version_id().to_string()))
        })
        .collect()
}
