use crate::canonical::compact_untracked_changes_for_touched_rows_in_transaction;
use crate::functions::LixFunctionProvider;
use crate::live_state::CanonicalCommitProjectionReceipt;
use crate::transaction::PendingCommitState;
use crate::transaction::{binary_blob_writes_from_filesystem_state, FilesystemTransactionState};
use crate::{LixBackendTransaction, LixError};

use super::create::create_commit;
pub(crate) use super::create::{
    CreateCommitAppliedOutput, CreateCommitArgs, CreateCommitDisposition, CreateCommitError,
    CreateCommitErrorKind, CreateCommitExpectedHead, CreateCommitIdempotencyKey,
    CreateCommitInvariantChecker, CreateCommitPreconditions, CreateCommitResult,
    CreateCommitWriteLane,
};
use super::pending::{
    build_pending_commit_state, create_commit_error_to_lix_error,
    merge_public_change_batch_into_pending_commit, pending_commit_state_matches_create_commit,
};
use super::types::{
    tracked_live_rows_from_staged_changes, untracked_live_rows_from_updated_version_refs,
    StagedChange,
};
pub(crate) struct BufferedTrackedAppendArgs {
    pub(crate) timestamp: Option<String>,
    pub(crate) changes: Vec<StagedChange>,
    pub(crate) filesystem_state: FilesystemTransactionState,
    pub(crate) preconditions: CreateCommitPreconditions,
    pub(crate) active_account_ids: Vec<String>,
    pub(crate) origin_key: Option<String>,
    pub(crate) should_emit_observe_tick: bool,
}

#[derive(Debug)]
pub(crate) struct BufferedTrackedAppendOutcome {
    pub(crate) disposition: CreateCommitDisposition,
    pub(crate) receipt: Option<CanonicalCommitProjectionReceipt>,
    pub(crate) applied_output: Option<CreateCommitAppliedOutput>,
    pub(crate) applied_changes: Vec<StagedChange>,
    pub(crate) merged_into_pending_session: bool,
}

pub(crate) async fn append_tracked(
    transaction: &mut dyn LixBackendTransaction,
    args: CreateCommitArgs,
    functions: &mut dyn LixFunctionProvider,
    invariant_checker: Option<&mut dyn CreateCommitInvariantChecker>,
) -> Result<CreateCommitResult, LixError> {
    append_tracked_unchecked(transaction, args, functions, invariant_checker).await
}

async fn append_tracked_unchecked(
    transaction: &mut dyn LixBackendTransaction,
    args: CreateCommitArgs,
    functions: &mut dyn LixFunctionProvider,
    invariant_checker: Option<&mut dyn CreateCommitInvariantChecker>,
) -> Result<CreateCommitResult, LixError> {
    // This helper intentionally composes multiple owners atomically:
    // canonical commit facts, replica-local version-head state, and
    // derived live-state rows. The owners
    // remain distinct even though the write unit commits them together.
    let result = create_commit(transaction, args, functions, invariant_checker)
        .await
        .map_err(create_commit_error_to_lix_error)?;

    if let Some(receipt) = result.receipt.as_ref() {
        let tracked_live_rows = tracked_live_rows_from_staged_changes(&result.applied_changes)?;
        let untracked_live_rows = untracked_live_rows_from_updated_version_refs(
            &receipt.canonical_receipt.updated_version_refs,
        );

        let mut live_rows = tracked_live_rows;
        live_rows.extend(untracked_live_rows);
        if !live_rows.is_empty() {
            crate::live_state::write_live_rows(transaction, &live_rows).await?;
        }
        crate::live_state::finalize_live_state_after_commit_write(transaction).await?;
        if result.applied_output.is_some() {
            let visibility_rows =
                crate::session::canonical_untracked_visibility_rows_from_updated_version_refs(
                    &receipt.canonical_receipt.updated_version_refs,
                )?;
            compact_untracked_changes_for_touched_rows_in_transaction(
                transaction,
                &visibility_rows,
            )
            .await?;
        }
    }

    Ok(result)
}

pub(crate) async fn append_tracked_with_pending_public_session(
    transaction: &mut dyn LixBackendTransaction,
    args: BufferedTrackedAppendArgs,
    functions: &mut dyn LixFunctionProvider,
    mut invariant_checker: Option<&mut dyn CreateCommitInvariantChecker>,
    mut pending_session: Option<&mut Option<PendingCommitState>>,
    allow_pending_session_merge: bool,
) -> Result<BufferedTrackedAppendOutcome, LixError> {
    if let Some(session_slot) = pending_session.as_deref_mut() {
        let can_merge = allow_pending_session_merge
            && session_slot.as_ref().is_some_and(|session| {
                pending_commit_state_matches_create_commit(session, &args.preconditions)
            });
        if can_merge {
            let binary_blob_writes =
                binary_blob_writes_from_filesystem_state(&args.filesystem_state);
            let timestamp = args
                .timestamp
                .clone()
                .unwrap_or_else(|| functions.timestamp());
            if let Some(checker) = invariant_checker.as_deref_mut() {
                checker
                    .recheck_invariants(transaction)
                    .await
                    .map_err(create_commit_error_to_lix_error)?;
            }
            let session = session_slot
                .as_mut()
                .expect("pending pending commit state should exist when merge preconditions match");
            let receipt = merge_public_change_batch_into_pending_commit(
                transaction,
                session,
                &args.changes,
                &binary_blob_writes,
                &args.active_account_ids,
                functions,
                &timestamp,
            )
            .await?;
            return Ok(BufferedTrackedAppendOutcome {
                disposition: CreateCommitDisposition::Applied,
                receipt: Some(receipt),
                applied_output: None,
                applied_changes: args.changes,
                merged_into_pending_session: true,
            });
        }
    }

    let write_lane = args.preconditions.write_lane.clone();
    let create_result = append_tracked_unchecked(
        transaction,
        CreateCommitArgs {
            timestamp: args.timestamp,
            changes: args.changes,
            filesystem_state: args.filesystem_state,
            preconditions: args.preconditions,
            active_account_ids: args.active_account_ids,
            lane_parent_commit_ids_override: None,
            allow_empty_commit: false,
            should_emit_observe_tick: args.should_emit_observe_tick,
            observe_tick_origin_key: args.origin_key.clone(),
            origin_key: args.origin_key,
        },
        functions,
        invariant_checker,
    )
    .await?;

    if let Some(session_slot) = pending_session {
        *session_slot = if matches!(create_result.disposition, CreateCommitDisposition::Applied) {
            if let Some(applied_output) = create_result.applied_output.as_ref() {
                Some(build_pending_commit_state(transaction, write_lane, applied_output).await?)
            } else {
                None
            }
        } else {
            None
        };
    }

    Ok(BufferedTrackedAppendOutcome {
        disposition: create_result.disposition,
        receipt: create_result.receipt,
        applied_output: create_result.applied_output,
        applied_changes: create_result.applied_changes,
        merged_into_pending_session: false,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        append_tracked, append_tracked_with_pending_public_session, BufferedTrackedAppendArgs,
        CreateCommitArgs, CreateCommitExpectedHead, CreateCommitIdempotencyKey,
        CreateCommitPreconditions, CreateCommitWriteLane, PendingCommitState,
    };
    use crate::functions::LixFunctionProvider;
    use crate::transaction::PendingCommitLane;
    use crate::{
        LixBackendTransaction, LixError, QueryResult, SqlDialect, TransactionBeginMode, Value,
    };
    use async_trait::async_trait;

    #[derive(Default)]
    struct NoopFunctionProvider;

    impl LixFunctionProvider for NoopFunctionProvider {
        fn uuid_v7(&mut self) -> String {
            "uuid-1".to_string()
        }

        fn timestamp(&mut self) -> String {
            "2026-03-06T14:22:00.000Z".to_string()
        }
    }

    #[derive(Default)]
    struct GateTransaction {
        live_state_mode: Option<String>,
    }

    #[async_trait(?Send)]
    impl LixBackendTransaction for GateTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> TransactionBeginMode {
            TransactionBeginMode::Write
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_live_state_status") {
                return Ok(QueryResult {
                    rows: vec![vec![
                        Value::Text(
                            self.live_state_mode
                                .clone()
                                .unwrap_or_else(|| "ready".to_string()),
                        ),
                        Value::Null,
                        Value::Null,
                        Value::Text(crate::live_state::LIVE_STATE_SCHEMA_EPOCH.to_string()),
                    ]],
                    columns: vec![
                        "mode".to_string(),
                        "latest_change_id".to_string(),
                        "latest_change_created_at".to_string(),
                        "schema_epoch".to_string(),
                    ],
                });
            }

            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn execute_batch(
            &mut self,
            _batch: &crate::backend::PreparedBatch,
        ) -> Result<QueryResult, LixError> {
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    fn sample_change() -> crate::session::version_ops::commit::StagedChange {
        crate::session::version_ops::commit::StagedChange {
            id: None,
            entity_id: "entity-1".try_into().unwrap(),
            schema_key: "lix_key_value".try_into().unwrap(),
            schema_version: Some("1".try_into().unwrap()),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some("{\"key\":\"hello\"}".to_string()),
            metadata: None,
            version_id: "version-a".try_into().unwrap(),
            origin_key: Some("writer-a".to_string()),
            created_at: None,
        }
    }

    #[tokio::test]
    async fn append_tracked_does_not_preemptively_reject_when_live_state_is_not_ready() {
        let mut transaction = GateTransaction {
            live_state_mode: Some("needs_rebuild".to_string()),
        };
        let mut functions = NoopFunctionProvider;

        let error = append_tracked(
            &mut transaction,
            CreateCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                filesystem_state: Default::default(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-1".to_string()),
                },
                active_account_ids: Vec::new(),
                lane_parent_commit_ids_override: None,
                allow_empty_commit: false,
                should_emit_observe_tick: false,
                observe_tick_origin_key: None,
                origin_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .expect_err("append_tracked should continue past live-state readiness");

        assert!(
            !error.description.contains("live state is not ready"),
            "unexpected error: {}",
            error.description
        );
    }

    #[tokio::test]
    async fn pending_public_append_does_not_preemptively_reject_when_live_state_is_not_ready() {
        let mut transaction = GateTransaction {
            live_state_mode: Some("needs_rebuild".to_string()),
        };
        let mut functions = NoopFunctionProvider;
        let mut pending_session = Some(PendingCommitState {
            lane: PendingCommitLane::Version("version-a".to_string()),
            commit_id: "commit-123".to_string(),
            commit_change_snapshot_id: "snapshot-1".to_string(),
            commit_snapshot: serde_json::json!({ "change_set_id": "change-set-1" }),
        });

        let outcome = append_tracked_with_pending_public_session(
            &mut transaction,
            BufferedTrackedAppendArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                filesystem_state: Default::default(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-1".to_string()),
                },
                active_account_ids: Vec::new(),
                origin_key: None,
                should_emit_observe_tick: false,
            },
            &mut functions,
            None,
            Some(&mut pending_session),
            true,
        )
        .await
        .expect("pending public append should continue past live-state readiness");

        assert!(
            outcome.merged_into_pending_session,
            "pending public append should still merge into the pending session",
        );
        assert!(
            outcome.receipt.is_some(),
            "pending public append should still produce a canonical commit receipt",
        );
    }
}
