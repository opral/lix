//! Golden repository-format fixture coverage for protocol v68.
//!
//! `fixtures/v68_bundled_csv_history.snapshot` is the raw, deterministic
//! `Memory::export_snapshot` output generated from `origin/main` commit
//! `89aea5d55773586ea60f77c1d9dddcfc8b394dd1`, whose repository protocol is
//! v68. The baseline helper was temporary because production v68 initialization
//! and revision fences use wall-clock UUIDs: it replaced `SystemFunctionProvider`
//! UUID/time calls and the seven direct revision/fence `Uuid::now_v7` call sites
//! with one fixed UUID sequence and timestamp. It then registered the bundled
//! `csv_row` schema, inserted one row, updated it once, exported, cold-reopened,
//! asserted one current row and two history rows, and asserted that re-export
//! after reopen was byte-identical. Two independent helper runs produced the
//! same 36,275-byte fixture with SHA-256
//! `00ca0adb275e77d9b76563501fae5158707c3aa18ef68ca766bef61297ea8a10`.
//! The temporary helper and baseline patches are intentionally not retained.

use lix::Memory;
use lix::migration::{MigrationOptions, MigrationStatus, inspect_repository, migrate_repository};
use lix::open_lix;

const V68_SNAPSHOT: &[u8] = include_bytes!("fixtures/v68_bundled_csv_history.snapshot");

#[tokio::test]
async fn current_format_inspection_recognizes_v68_golden_snapshot() {
    let storage = Memory::from_snapshot(V68_SNAPSHOT).expect("v68 golden snapshot should decode");

    assert_eq!(
        inspect_repository(&storage)
            .await
            .expect("v68 format inspection should succeed"),
        MigrationStatus::Required {
            from_version: 68,
            to_version: 69,
        }
    );
}

#[tokio::test]
async fn migrates_v68_fixture_and_reopens_with_current_and_history_rows() {
    let storage = Memory::from_snapshot(V68_SNAPSHOT).expect("v68 golden snapshot should decode");
    let Err(open_error) = open_lix().with_storage(storage.clone()).await else {
        panic!("normal engine open must require migration");
    };
    assert_eq!(open_error.code, "LIX_ERROR_REPOSITORY_MIGRATION_REQUIRED");
    let report = migrate_repository(storage.clone(), MigrationOptions::default())
        .await
        .expect("v68 fixture should migrate");
    assert_eq!(report.from_version, 68);
    assert_eq!(report.to_version, 69);
    assert!(report.changes_rewritten >= 2);
    assert!(report.commit_members_rewritten >= 2);
    assert!(report.hot_rows_rewritten >= 1);
    assert_eq!(
        inspect_repository(&storage).await.unwrap(),
        MigrationStatus::Current { version: 69 }
    );
    let retry = migrate_repository(storage.clone(), MigrationOptions::default())
        .await
        .expect("migration retry should be idempotent");
    assert_eq!(retry.from_version, 69);
    assert_eq!(retry.to_version, 69);
    assert_eq!(retry.changes_rewritten, 0);

    let migrated_snapshot = storage
        .export_snapshot()
        .expect("migrated repository should export");
    drop(storage);
    let reopened = Memory::from_snapshot(&migrated_snapshot)
        .expect("migrated repository should import into a fresh backend");
    let lix = open_lix()
        .with_storage(reopened)
        .await
        .expect("migrated repository should cold-open");
    let current = lix
        .execute("SELECT id, cells, order_key FROM csv_row", &[])
        .await
        .expect("migrated typed current row should read");
    assert_eq!(current.rows().len(), 1);
    let history = lix
        .execute(
            "SELECT id FROM lix_change WHERE schema_key = 'csv_row' ORDER BY created_at",
            &[],
        )
        .await
        .expect("migrated history should read");
    assert_eq!(history.rows().len(), 2);
}
