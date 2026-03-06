#[allow(dead_code)]
mod append_commit;
mod generate_commit;
mod runtime;
mod types;

#[allow(unused_imports)]
pub(crate) use append_commit::{
    append_commit_if_preconditions_hold, AppendCommitArgs, AppendCommitDisposition,
    AppendCommitError, AppendCommitErrorKind, AppendCommitPreconditions, AppendCommitResult,
    AppendExpectedTip, AppendWriteLane,
};
pub use generate_commit::generate_commit;
pub(crate) use runtime::{
    bind_statement_batch_for_dialect, build_statement_batch_from_generate_commit_result,
    load_commit_active_accounts, load_version_info_for_versions, CommitQueryExecutor,
    StatementBatch,
};
pub(crate) use types::ProposedDomainChange;
pub use types::{
    ChangeRow, DomainChangeInput, GenerateCommitArgs, GenerateCommitResult, MaterializedStateRow,
    VersionInfo, VersionSnapshot,
};
