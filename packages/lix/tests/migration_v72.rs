//! Golden repository-format fixture coverage for the v72 account schema.
//!
//! `fixtures/v72_account_without_profile_uri.snapshot` is the raw
//! `Memory::export_snapshot` output generated from parent revision
//! `4816fdba591d7165ff1b0195e74471aa8fc73660`, before `profile_uri` was added.
//! It contains the v72 bootstrap account rows and persisted account schema.
//! SHA-256: `05503801b91c41d821897e76ee64c476851336932e3b9c9a6618210e66b60b29`.

use lix::migration::{MigrationOptions, MigrationStatus, inspect_lix, migrate_lix};
use lix::{Memory, open_lix};

const V72_ACCOUNT_SNAPSHOT: &[u8] =
    include_bytes!("fixtures/v72_account_without_profile_uri.snapshot");

#[tokio::test]
async fn v72_is_inspectable_but_rejected_by_the_v75_hard_cut() {
    let storage =
        Memory::from_snapshot(V72_ACCOUNT_SNAPSHOT).expect("v72 account fixture should decode");

    assert_eq!(
        inspect_lix(&storage)
            .await
            .expect("v72 format inspection should succeed"),
        MigrationStatus::Required {
            from_version: 72,
            to_version: 75,
        }
    );
    let Err(open_error) = open_lix().with_storage(storage.clone()).await else {
        panic!("normal engine open must require the v72 account-schema migration");
    };
    assert_eq!(open_error.code, "LIX_ERROR_REPOSITORY_MIGRATION_REQUIRED");

    let error = migrate_lix(storage.clone(), MigrationOptions::default())
        .await
        .expect_err("v75 intentionally has no in-place migration from v72");
    assert_eq!(error.code, "LIX_ERROR_MIGRATION_FAILED");
    assert!(error.message.contains("complete-snapshot commit format"));
    assert_eq!(
        inspect_lix(&storage).await.unwrap(),
        MigrationStatus::Required {
            from_version: 72,
            to_version: 75,
        },
        "the rejected migration must not modify the repository"
    );
}
