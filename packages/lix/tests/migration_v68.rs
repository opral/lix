//! Golden repository-format coverage for the intentional v75 hard cut.
//!
//! The fixtures remain useful to prove that old repositories can be inspected
//! without mutation and are rejected explicitly rather than mis-decoded as
//! complete-snapshot commits.

use lix::Memory;
use lix::migration::{MigrationOptions, MigrationStatus, inspect_lix, migrate_lix};
use lix::open_lix;

const V68_SNAPSHOT: &[u8] = include_bytes!("fixtures/v68_bundled_csv_history.snapshot");
const V68_EXTERNAL_TOMBSTONES: &[u8] = include_bytes!("fixtures/v68_external_tombstones.snapshot");

#[tokio::test]
async fn v68_is_inspectable_but_rejected_without_mutation() {
    for fixture in [V68_SNAPSHOT, V68_EXTERNAL_TOMBSTONES] {
        let storage = Memory::from_snapshot(fixture).expect("v68 golden snapshot should decode");
        let before = storage.export_snapshot().expect("fixture should export");
        assert_eq!(
            inspect_lix(&storage).await.unwrap(),
            MigrationStatus::Required {
                from_version: 68,
                to_version: 75,
            }
        );

        let error = migrate_lix(storage.clone(), MigrationOptions::default())
            .await
            .expect_err("v75 intentionally has no in-place migration from v68");
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
async fn normal_open_rejects_v68_before_reading_commit_authority() {
    let storage = Memory::from_snapshot(V68_SNAPSHOT).expect("v68 golden snapshot should decode");
    let Err(error) = open_lix().with_storage(storage).await else {
        panic!("normal engine open must reject the old commit format");
    };
    assert_eq!(error.code, "LIX_ERROR_REPOSITORY_MIGRATION_REQUIRED");
    assert!(error.message.contains("v68"));
    assert!(error.message.contains("v75"));
}
