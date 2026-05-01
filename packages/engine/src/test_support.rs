use crate::backend::{LixBackend, TransactionBeginMode};
use crate::tracked_state::{TrackedStateContext, TrackedStateRow};
use crate::untracked_state::UntrackedStateContext;
use crate::version::GLOBAL_VERSION_ID;
use crate::version_ref::VersionRefContext;

pub(crate) const TEST_EMPTY_ROOT_COMMIT_ID: &str = "test-empty-root";
const TEST_TIMESTAMP: &str = "1970-01-01T00:00:00.000Z";

/// Seeds a version head and matching tracked root for unit tests.
///
/// A version ref that points at a commit without a tracked root is invalid for
/// the serving projection. This helper keeps that invariant in one place while
/// still letting low-level tests use synthetic commit ids.
pub(crate) async fn seed_version_head(
    backend: &(dyn LixBackend + Send + Sync),
    version_id: &str,
    commit_id: &str,
) {
    seed_version_head_with_rows(backend, version_id, commit_id, &[]).await;
}

/// Seeds the global version head to an empty tracked root for unit tests.
pub(crate) async fn seed_global_version_head(backend: &(dyn LixBackend + Send + Sync)) {
    seed_version_head(backend, GLOBAL_VERSION_ID, TEST_EMPTY_ROOT_COMMIT_ID).await;
}

/// Seeds a version head and writes the tracked root contents for its commit.
pub(crate) async fn seed_version_head_with_rows(
    backend: &(dyn LixBackend + Send + Sync),
    version_id: &str,
    commit_id: &str,
    rows: &[TrackedStateRow],
) {
    let mut transaction = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await
        .expect("seed transaction should open");
    VersionRefContext::new(Arc::new(UntrackedStateContext::new()))
        .writer(transaction.as_mut())
        .advance_head(version_id, commit_id, TEST_TIMESTAMP)
        .await
        .expect("version ref should write");
    TrackedStateContext::new()
        .writer(transaction.as_mut())
        .write_root(commit_id, None, rows)
        .await
        .expect("tracked root should write");
    transaction.commit().await.expect("seed should commit");
}
use std::sync::Arc;
