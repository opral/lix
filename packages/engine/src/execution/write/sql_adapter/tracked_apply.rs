use std::collections::BTreeSet;

use crate::contracts::artifacts::{PendingPublicCommitSession, PlanEffects, SessionStateDelta};
use crate::contracts::change::TrackedDomainChangeView;
use crate::contracts::state_commit_stream::{
    state_commit_stream_changes_from_domain_changes, StateCommitStreamRuntimeMetadata,
};
use crate::{LixBackendTransaction, LixError, QueryResult};

use super::runtime::{empty_public_write_execution_outcome, SqlExecutionOutcome};
use crate::execution::write::buffered::TrackedTxnUnit;
use crate::execution::write::WriteExecutionBindings;

pub(super) async fn run_public_tracked_append_txn_with_transaction(
    bindings: &dyn WriteExecutionBindings,
    transaction: &mut dyn LixBackendTransaction,
    unit: &TrackedTxnUnit,
    pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    if unit
        .execution
        .domain_change_batch
        .as_ref()
        .is_some_and(|batch| batch.changes.is_empty())
        && !unit.has_compiler_only_filesystem_changes()
    {
        return Ok(Some(empty_public_write_execution_outcome()));
    }

    let execution = bindings
        .execute_public_tracked_append_txn_with_transaction(
            transaction,
            unit,
            pending_commit_session,
        )
        .await?;

    let plan_effects_override = if execution.plugin_changes_committed {
        if unit.has_compiler_only_filesystem_changes() {
            plan_effects_from_tracked_domain_changes(
                &execution.applied_domain_changes,
                unit.public_write
                    .contract
                    .operation_kind
                    .state_commit_stream_operation(),
                unit.writer_key.as_deref(),
                execution.next_active_version_id.clone(),
            )?
        } else {
            unit.execution.semantic_effects.clone()
        }
    } else {
        PlanEffects::default()
    };

    Ok(Some(SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        internal_write_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: execution.plugin_changes_committed,
        canonical_commit_receipt: execution.receipt,
        plan_effects_override: Some(plan_effects_override),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: execution.plugin_changes_committed && unit.should_emit_observe_tick(),
    }))
}

fn plan_effects_from_tracked_domain_changes<Change: TrackedDomainChangeView>(
    changes: &[Change],
    stream_operation: crate::contracts::artifacts::StateCommitStreamOperation,
    writer_key: Option<&str>,
    next_active_version_id: Option<String>,
) -> Result<PlanEffects, LixError> {
    Ok(PlanEffects {
        state_commit_stream_changes: state_commit_stream_changes_from_domain_changes(
            changes,
            stream_operation,
            StateCommitStreamRuntimeMetadata::from_runtime_writer_key(writer_key),
        )?,
        session_delta: SessionStateDelta {
            next_active_version_id,
            next_active_account_ids: None,
            persist_workspace: false,
        },
        file_cache_refresh_targets: file_cache_refresh_targets_from_domain_changes(changes),
    })
}

fn file_cache_refresh_targets_from_domain_changes<Change: TrackedDomainChangeView>(
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
