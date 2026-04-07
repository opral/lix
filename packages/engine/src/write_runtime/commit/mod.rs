//! Commit authoring and orchestration boundary.
//!
//! `write_runtime/commit` owns commit authoring, preflight, receipts,
//! idempotency, and the atomic write path that composes canonical facts with
//! local refs, workspace annotations, and narrow live-state hooks.

pub(crate) mod append;
pub(crate) mod create;
pub(crate) mod generate;
mod init;
pub(crate) mod pending;
pub(crate) mod preflight;
pub(crate) mod receipt;
pub(crate) mod types;

pub(crate) const COMMIT_IDEMPOTENCY_TABLE: &str = "lix_internal_commit_idempotency";

pub(crate) use crate::contracts::artifacts::{CanonicalCommitReceipt, UpdatedVersionRef};
pub(crate) use append::{
    append_tracked, append_tracked_with_pending_public_session, BufferedTrackedAppendArgs,
    CreateCommitAppliedOutput, CreateCommitArgs, CreateCommitDisposition, CreateCommitError,
    CreateCommitErrorKind, CreateCommitExpectedHead, CreateCommitIdempotencyKey,
    CreateCommitInvariantChecker, CreateCommitPreconditions, CreateCommitWriteLane,
};
pub(crate) use init::init;
pub(crate) use pending::PendingPublicCommitSession;
pub(crate) use types::ProposedDomainChange;
