use std::sync::Arc;

use crate::json_store::JsonStoreContext;
use crate::storage::StorageContext;
use crate::storage::StorageWriteSet;
use crate::tracked_state::{TrackedStateContext, TrackedStateRow};
use crate::untracked_state::UntrackedStateContext;
use crate::version::VersionContext;
use crate::GLOBAL_VERSION_ID;

pub(crate) const TEST_EMPTY_ROOT_COMMIT_ID: &str = "test-empty-root";
const TEST_TIMESTAMP: &str = "1970-01-01T00:00:00.000Z";

/// Seeds a version head and matching tracked root for unit tests.
///
/// A version ref that points at a commit without a tracked root is invalid for
/// the serving projection. This helper keeps that invariant in one place while
/// still letting low-level tests use synthetic commit ids.
pub(crate) async fn seed_version_head(storage: StorageContext, version_id: &str, commit_id: &str) {
    seed_version_head_with_rows(storage, version_id, commit_id, &[]).await;
}

/// Seeds the global version head to an empty tracked root for unit tests.
pub(crate) async fn seed_global_version_head(storage: StorageContext) {
    seed_version_head(storage, GLOBAL_VERSION_ID, TEST_EMPTY_ROOT_COMMIT_ID).await;
}

/// Seeds a version head and writes the tracked root contents for its commit.
pub(crate) async fn seed_version_head_with_rows(
    storage: StorageContext,
    version_id: &str,
    commit_id: &str,
    rows: &[TrackedStateRow],
) {
    let mut transaction = storage
        .begin_write_transaction()
        .await
        .expect("seed transaction should open");
    let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
    let mut writes = StorageWriteSet::new();
    let canonical_row = {
        let mut json_writer = JsonStoreContext::new().writer();
        version_ctx
            .canonical_ref_row(
                &mut writes,
                &mut json_writer,
                version_id,
                commit_id,
                TEST_TIMESTAMP,
            )
            .expect("version ref should canonicalize")
    };
    version_ctx
        .stage_canonical_ref_rows(&mut writes, &[canonical_row])
        .expect("version ref should stage");
    writes
        .apply(&mut transaction.as_mut())
        .await
        .expect("version ref should write");
    let mut writes = StorageWriteSet::new();
    {
        let mut json_writer = JsonStoreContext::new().writer();
        TrackedStateContext::new()
            .writer()
            .stage_root(
                &mut transaction.as_mut(),
                &mut writes,
                &mut json_writer,
                commit_id,
                None,
                rows,
            )
            .await
            .expect("tracked root should write");
    }
    writes
        .apply(&mut transaction.as_mut())
        .await
        .expect("tracked root should write");
    transaction.commit().await.expect("seed should commit");
}
