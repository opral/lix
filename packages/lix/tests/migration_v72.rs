//! Golden repository-format fixture coverage for the v72 account schema.
//!
//! `fixtures/v72_account_without_profile_uri.snapshot` is the raw
//! `Memory::export_snapshot` output generated from parent revision
//! `4816fdba591d7165ff1b0195e74471aa8fc73660`, before `profile_uri` was added.
//! It contains the v72 bootstrap account rows and persisted account schema.
//! SHA-256: `05503801b91c41d821897e76ee64c476851336932e3b9c9a6618210e66b60b29`.

use lix::migration::{MigrationOptions, MigrationStatus, inspect_lix, migrate_lix};
use lix::{ANONYMOUS_ACCOUNT_ID, Memory, Value, open_lix};

const V72_ACCOUNT_SNAPSHOT: &[u8] =
    include_bytes!("fixtures/v72_account_without_profile_uri.snapshot");

#[tokio::test]
async fn migrates_profile_uri_and_persists_updates_across_cold_reopen() {
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

    let report = migrate_lix(storage.clone(), MigrationOptions::default())
        .await
        .expect("v72 account schema should migrate");
    assert_eq!(report.from_version, 72);
    assert_eq!(report.to_version, 75);
    assert_eq!(
        inspect_lix(&storage).await.unwrap(),
        MigrationStatus::Current { version: 75 }
    );

    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("migrated v72 repository should open");
    let accounts = lix
        .execute("SELECT id, profile_uri FROM lix_account ORDER BY id", &[])
        .await
        .expect("migrated account rows should expose profile_uri");
    assert!(!accounts.rows().is_empty());
    assert!(
        accounts
            .rows()
            .iter()
            .all(|row| matches!(row.values(), [Value::Text(_), Value::Null])),
        "historical account rows must materialize the appended nullable column as NULL"
    );

    let profile_uri = "https://profiles.example/anonymous.json";
    let updated = lix
        .execute(
            "UPDATE lix_account SET profile_uri = $1 WHERE id = $2",
            &[
                Value::Text(profile_uri.to_owned()),
                Value::Text(ANONYMOUS_ACCOUNT_ID.to_owned()),
            ],
        )
        .await
        .expect("profile_uri update should succeed");
    assert_eq!(updated.rows_affected(), 1);
    lix.close().await.expect("migrated repository should close");
    drop(lix);

    let migrated = storage
        .export_snapshot()
        .expect("migrated repository should export");
    drop(storage);
    let reopened = Memory::from_snapshot(&migrated)
        .expect("migrated repository should import into a fresh backend");
    let lix = open_lix()
        .with_storage(reopened)
        .await
        .expect("migrated repository should cold-open");
    let result = lix
        .execute(
            "SELECT profile_uri FROM lix_account WHERE id = $1",
            &[Value::Text(ANONYMOUS_ACCOUNT_ID.to_owned())],
        )
        .await
        .expect("profile_uri should remain queryable after cold reopen");
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text(profile_uri.to_owned())]
    );
}
