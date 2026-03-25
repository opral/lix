use crate::functions::LixFunctionProvider;
use crate::{LixBackendTransaction, LixError};

pub(crate) use super::create_commit::{
    CreateCommitAppliedOutput, CreateCommitArgs, CreateCommitDisposition, CreateCommitError,
    CreateCommitErrorKind, CreateCommitExpectedHead, CreateCommitIdempotencyKey,
    CreateCommitInvariantChecker, CreateCommitPreconditions, CreateCommitResult,
    CreateCommitWriteLane,
};
use super::create_commit::create_commit;
use super::pending_session::create_commit_error_to_lix_error;

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
