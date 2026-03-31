use crate::filesystem::runtime::{
    binary_blob_writes_from_filesystem_state, FilesystemTransactionState,
};
use crate::functions::LixFunctionProvider;
use crate::{LixBackendTransaction, LixError};

use super::create::create_commit;
pub(crate) use super::create::{
    CreateCommitAppliedOutput, CreateCommitArgs, CreateCommitDisposition, CreateCommitError,
    CreateCommitErrorKind, CreateCommitExpectedHead, CreateCommitIdempotencyKey,
    CreateCommitInvariantChecker, CreateCommitPreconditions, CreateCommitResult,
    CreateCommitWriteLane,
};
use super::pending::{
    build_pending_public_commit_session, create_commit_error_to_lix_error,
    merge_public_domain_change_batch_into_pending_commit, pending_session_matches_create_commit,
    PendingPublicCommitSession,
};
use super::receipt::CanonicalCommitReceipt;
use super::types::ProposedDomainChange;

pub(crate) struct BufferedTrackedAppendArgs {
    pub(crate) timestamp: Option<String>,
    pub(crate) changes: Vec<ProposedDomainChange>,
    pub(crate) filesystem_state: FilesystemTransactionState,
    pub(crate) preconditions: CreateCommitPreconditions,
    pub(crate) active_account_ids: Vec<String>,
    pub(crate) writer_key: Option<String>,
    pub(crate) should_emit_observe_tick: bool,
}

#[derive(Debug)]
pub(crate) struct BufferedTrackedAppendOutcome {
    pub(crate) disposition: CreateCommitDisposition,
    pub(crate) receipt: Option<CanonicalCommitReceipt>,
    pub(crate) applied_output: Option<CreateCommitAppliedOutput>,
    pub(crate) applied_domain_changes: Vec<ProposedDomainChange>,
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
    // canonical commit facts, replica-local version-head state, workspace
    // writer-key annotations, and derived live-state projections. The owners
    // remain distinct even though the write unit commits them together.
    let execution_writer_key = args.writer_key.clone();
    let result = create_commit(transaction, args, functions, invariant_checker)
        .await
        .map_err(create_commit_error_to_lix_error)?;

    if let Some(receipt) = result.receipt.as_ref() {
        let tracked_writer_key_annotations =
            crate::workspace::writer_key::tracked_writer_key_annotations_from_changes(
                &result.applied_domain_changes,
                execution_writer_key.as_deref(),
            );
        let mut executor = &mut *transaction;
        crate::workspace::writer_key::apply_workspace_writer_key_annotations_with_executor(
            &mut executor,
            &tracked_writer_key_annotations,
        )
        .await?;
        crate::live_state::apply_commit_projections_best_effort_in_transaction(
            transaction,
            receipt,
            &tracked_writer_key_annotations,
        )
        .await?;
    }

    Ok(result)
}

pub(crate) async fn append_tracked_with_pending_public_session(
    transaction: &mut dyn LixBackendTransaction,
    args: BufferedTrackedAppendArgs,
    functions: &mut dyn LixFunctionProvider,
    mut invariant_checker: Option<&mut dyn CreateCommitInvariantChecker>,
    mut pending_session: Option<&mut Option<PendingPublicCommitSession>>,
    allow_pending_session_merge: bool,
) -> Result<BufferedTrackedAppendOutcome, LixError> {
    if let Some(session_slot) = pending_session.as_deref_mut() {
        let can_merge = allow_pending_session_merge
            && session_slot.as_ref().is_some_and(|session| {
                pending_session_matches_create_commit(session, &args.preconditions)
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
            let session = session_slot.as_mut().expect(
                "pending public commit session should exist when merge preconditions match",
            );
            let receipt = merge_public_domain_change_batch_into_pending_commit(
                transaction,
                session,
                &args.changes,
                &binary_blob_writes,
                &args.active_account_ids,
                args.writer_key.as_deref(),
                functions,
                &timestamp,
            )
            .await?;
            return Ok(BufferedTrackedAppendOutcome {
                disposition: CreateCommitDisposition::Applied,
                receipt: Some(receipt),
                applied_output: None,
                applied_domain_changes: args.changes,
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
            observe_tick_writer_key: args.writer_key.clone(),
            writer_key: args.writer_key,
        },
        functions,
        invariant_checker,
    )
    .await?;

    if let Some(session_slot) = pending_session {
        *session_slot = if matches!(create_result.disposition, CreateCommitDisposition::Applied) {
            if let Some(applied_output) = create_result.applied_output.as_ref() {
                Some(
                    build_pending_public_commit_session(transaction, write_lane, applied_output)
                        .await?,
                )
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
        applied_domain_changes: create_result.applied_domain_changes,
        merged_into_pending_session: false,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        append_tracked, append_tracked_with_pending_public_session, BufferedTrackedAppendArgs,
        CreateCommitArgs, CreateCommitExpectedHead, CreateCommitIdempotencyKey,
        CreateCommitPreconditions, CreateCommitWriteLane, PendingPublicCommitSession,
    };
    use crate::functions::LixFunctionProvider;
    use crate::{LixBackendTransaction, LixError, QueryResult, SqlDialect, TransactionMode, Value};
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

        fn mode(&self) -> TransactionMode {
            TransactionMode::Write
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
            _batch: &crate::backend::prepared::PreparedBatch,
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

    fn sample_change() -> crate::commit::ProposedDomainChange {
        crate::commit::ProposedDomainChange {
            entity_id: "entity-1".try_into().unwrap(),
            schema_key: "lix_key_value".try_into().unwrap(),
            schema_version: Some("1".try_into().unwrap()),
            file_id: Some("lix".try_into().unwrap()),
            plugin_key: Some("lix".try_into().unwrap()),
            snapshot_content: Some("{\"key\":\"hello\"}".to_string()),
            metadata: None,
            version_id: "version-a".try_into().unwrap(),
            writer_key: Some("writer-a".to_string()),
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
                observe_tick_writer_key: None,
                writer_key: None,
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
        let mut pending_session = Some(PendingPublicCommitSession {
            lane: CreateCommitWriteLane::Version("version-a".to_string()),
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
                writer_key: None,
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
