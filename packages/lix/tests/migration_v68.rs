//! Golden repository-format coverage for the intentional pre-v72 hard cut.
//!
//! The fixtures remain useful to prove that old repositories can be inspected
//! without mutation and are rejected explicitly rather than mis-decoded as
//! complete-snapshot commits.

use futures_lite::io::Cursor;
use lix::{Memory, open_lix};

const V68_SNAPSHOT: &[u8] = include_bytes!("fixtures/v68_bundled_csv_history.lixsnap");
const V68_EXTERNAL_TOMBSTONES: &[u8] = include_bytes!("fixtures/v68_external_tombstones.lixsnap");

#[tokio::test]
async fn snapshot_open_rejects_v68_at_the_intentional_hard_cut() {
    for fixture in [V68_SNAPSHOT, V68_EXTERNAL_TOMBSTONES] {
        let storage = Memory::new();
        let Err(error) = open_lix()
            .with_storage(storage.clone())
            .from_snapshot(Cursor::new(fixture))
            .await
        else {
            panic!("automatic open must preserve the intentional v68 hard cut");
        };
        assert_eq!(error.code, "LIX_ERROR_MIGRATION_FAILED");
        assert!(
            error.message.contains("v68"),
            "unexpected rejection: {error:?}"
        );
        assert!(
            error.message.contains("v77"),
            "unexpected rejection: {error:?}"
        );
        let Err(retry) = open_lix()
            .with_storage(storage.clone())
            .from_snapshot(Cursor::new(fixture))
            .await
        else {
            panic!("retry unexpectedly opened an unsupported v68 snapshot");
        };
        assert_eq!(retry.code, "LIX_ERROR_MIGRATION_FAILED");
    }
}

#[tokio::test]
async fn automatic_open_rejects_v68_before_reading_commit_authority() {
    let Err(error) = open_lix().from_snapshot(Cursor::new(V68_SNAPSHOT)).await else {
        panic!("normal engine open must reject the old commit format");
    };
    assert_eq!(error.code, "LIX_ERROR_MIGRATION_FAILED");
    assert!(error.message.contains("v68"));
    assert!(error.message.contains("v77"));
}
