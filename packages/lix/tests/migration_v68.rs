//! Golden repository-format coverage for the intentional pre-v72 hard cut.
//!
//! The fixtures remain useful to prove that old repositories can be inspected
//! without mutation and are rejected explicitly rather than mis-decoded as
//! complete-snapshot commits.

use lix::Memory;
use lix::open_lix;

const V68_SNAPSHOT: &[u8] = include_bytes!("fixtures/v68_bundled_csv_history.snapshot");
const V68_EXTERNAL_TOMBSTONES: &[u8] = include_bytes!("fixtures/v68_external_tombstones.snapshot");

#[tokio::test]
async fn automatic_open_rejects_v68_without_mutation() {
    for fixture in [V68_SNAPSHOT, V68_EXTERNAL_TOMBSTONES] {
        let storage = Memory::from_snapshot(fixture).expect("v68 golden snapshot should decode");
        let before = storage.export_snapshot().expect("fixture should export");
        let Err(error) = open_lix().with_storage(storage.clone()).await else {
            panic!("automatic open must preserve the intentional v68 hard cut");
        };
        assert_eq!(error.code, "LIX_ERROR_MIGRATION_FAILED");
        assert!(error.message.contains("complete-snapshot commit format"));
        assert_eq!(
            storage.export_snapshot().expect("fixture should re-export"),
            before,
            "a rejected hard-cut migration must not modify storage"
        );
    }
}

#[tokio::test]
async fn automatic_open_rejects_v68_before_reading_commit_authority() {
    let storage = Memory::from_snapshot(V68_SNAPSHOT).expect("v68 golden snapshot should decode");
    let Err(error) = open_lix().with_storage(storage).await else {
        panic!("normal engine open must reject the old commit format");
    };
    assert_eq!(error.code, "LIX_ERROR_MIGRATION_FAILED");
    assert!(error.message.contains("v68"));
    assert!(error.message.contains("v76"));
}
