use crate::functions::LixFunctionProvider;
use crate::filesystem::runtime::{
    binary_blob_writes_from_filesystem_state, FilesystemTransactionState,
};
use crate::{LixBackendTransaction, LixError};

pub(crate) use super::create_commit::{
    CreateCommitAppliedOutput, CreateCommitArgs, CreateCommitDisposition, CreateCommitError,
    CreateCommitErrorKind, CreateCommitExpectedHead, CreateCommitIdempotencyKey,
    CreateCommitInvariantChecker, CreateCommitPreconditions, CreateCommitResult,
    CreateCommitWriteLane,
};
use super::create_commit::create_commit;
use super::pending_session::{
    build_pending_public_commit_session, create_commit_error_to_lix_error,
    merge_public_domain_change_batch_into_pending_commit, pending_session_matches_create_commit,
    PendingPublicCommitSession,
};
use super::ProposedDomainChange;

pub(crate) struct BufferedTrackedAppendArgs {
    pub(crate) timestamp: Option<String>,
    pub(crate) changes: Vec<ProposedDomainChange>,
    pub(crate) filesystem_state: FilesystemTransactionState,
    pub(crate) preconditions: CreateCommitPreconditions,
    pub(crate) writer_key: Option<String>,
    pub(crate) should_emit_observe_tick: bool,
}

pub(crate) struct BufferedTrackedAppendOutcome {
    pub(crate) disposition: CreateCommitDisposition,
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
    create_commit(transaction, args, functions, invariant_checker)
        .await
        .map_err(create_commit_error_to_lix_error)
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
            && session_slot
                .as_ref()
                .is_some_and(|session| pending_session_matches_create_commit(session, &args.preconditions));
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
                .expect("pending public commit session should exist when merge preconditions match");
            merge_public_domain_change_batch_into_pending_commit(
                transaction,
                session,
                &args.changes,
                &binary_blob_writes,
                functions,
                &timestamp,
            )
            .await?;
            return Ok(BufferedTrackedAppendOutcome {
                disposition: CreateCommitDisposition::Applied,
                applied_output: None,
                applied_domain_changes: args.changes,
                merged_into_pending_session: true,
            });
        }
    }

    let write_lane = args.preconditions.write_lane.clone();
    let create_result = append_tracked(
        transaction,
        CreateCommitArgs {
            timestamp: args.timestamp,
            changes: args.changes,
            filesystem_state: args.filesystem_state,
            preconditions: args.preconditions,
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
        applied_output: create_result.applied_output,
        applied_domain_changes: create_result.applied_domain_changes,
        merged_into_pending_session: false,
    })
}
