use std::sync::atomic::Ordering;

use super::SessionContext;
use crate::catalog::{CatalogRevision, load_catalog_revision};
use crate::engine::Engine;
use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions};
use crate::{CreateBranchOptions, MergeBranchOptions, MergeBranchOutcome, Value};

async fn open() -> (
    StorageAdapter<Memory>,
    Engine<Memory>,
    SessionContext<Memory>,
) {
    let storage = Memory::new();
    let receipt = Engine::initialize(storage.clone()).await.unwrap();
    let engine = Engine::new(storage.clone()).await.unwrap();
    let session = engine
        .open_session_at(&receipt.main_branch_id)
        .await
        .unwrap();
    (StorageAdapter::new(storage), engine, session)
}

async fn catalog_stamp(
    storage: &StorageAdapter<Memory>,
    session: &SessionContext<Memory>,
) -> (CatalogRevision, usize) {
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .unwrap();
    (
        load_catalog_revision(&read).await.unwrap().unwrap(),
        session
            .catalog_context
            .committed_catalog_warms
            .load(Ordering::Relaxed),
    )
}

fn schema(key: &str, extra: bool) -> Value {
    let mut columns = vec![serde_json::json!({"name":"id", "type":"text", "nullable":false})];
    if extra {
        columns.push(serde_json::json!({"name":"extra", "type":"text", "nullable":true}));
    }
    Value::Jsonb(
        serde_json::json!({
            "$schema":"https://lix.dev/schema-v1.json", "key":key,
            "columns":columns, "primary_key":["id"]
        })
        .into(),
    )
}


#[tokio::test]
async fn unchanged_catalog_checkpoints_do_not_rotate_revision_or_warm() {
    let (storage, _engine, main) = open().await;
    main.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('a', 'one'), ('b', 'two')",
        &[],
    )
    .await
    .unwrap();
    let before = catalog_stamp(&storage, &main).await;
    main.create_checkpoint().await.unwrap();
    assert_eq!(catalog_stamp(&storage, &main).await, before);
    main.execute("UPDATE lix_key_value SET value = 'changed'", &[])
        .await
        .unwrap();
    main.execute(
        "SELECT commit_id FROM lix_create_checkpoint(ARRAY[lix_row_ref('lix_key_value', 'a')])",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(catalog_stamp(&storage, &main).await, before);
    main.create_checkpoint().await.unwrap();
    assert_eq!(catalog_stamp(&storage, &main).await, before);
}

#[tokio::test]
async fn unchanged_catalog_three_way_merge_does_not_rotate_revision_or_warm() {
    let (storage, engine, main) = open().await;
    let draft_id = "01920000-0000-7000-8000-0000000000d1";
    main.create_branch(CreateBranchOptions {
        id: Some(draft_id.to_owned()),
        name: "catalog unchanged".to_owned(),
        from_commit_id: None,
    })
    .await
    .unwrap();
    let draft = engine.open_session_at(draft_id).await.unwrap();
    main.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('target', 'one')",
        &[],
    )
    .await
    .unwrap();
    draft
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('source', 'two')",
            &[],
        )
        .await
        .unwrap();
    let before = catalog_stamp(&storage, &main).await;
    let receipt = main
        .merge_branch(MergeBranchOptions {
            source_branch_id: draft_id.to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(receipt.outcome, MergeBranchOutcome::MergeCommitted);
    assert_eq!(catalog_stamp(&storage, &main).await, before);
    assert_eq!(
        main.execute(
            "SELECT key FROM lix_key_value WHERE key IN ('target', 'source')",
            &[]
        )
        .await
        .unwrap()
        .rows()
        .len(),
        2
    );
}

#[tokio::test]
async fn unchanged_inherited_catalog_refresh_does_not_rotate_revision_or_warm() {
    let (storage, engine, main) = open().await;
    let global = engine
        .open_session_at(crate::GLOBAL_BRANCH_ID)
        .await
        .unwrap();
    // Exercise the initial global-root-backed branch, then an owned local root.
    for local_root in [false, true] {
        if local_root {
            main.execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('owned', 'local')",
                &[],
            )
            .await
            .unwrap();
        }
        global.execute("INSERT INTO lix_key_value (key, value, lixcol_global) VALUES ('global-data', $1, true) ON CONFLICT(key) DO UPDATE SET value = excluded.value", &[Value::Text(if local_root { "second" } else { "first" }.to_owned())]).await.unwrap();
        let before = catalog_stamp(&storage, &main).await;
        main.execute(
            "SELECT key FROM lix_key_value WHERE key = 'global-data'",
            &[],
        )
        .await
        .unwrap();
        assert_eq!(catalog_stamp(&storage, &main).await, before);
    }
}

#[tokio::test]
async fn inherited_catalog_changes_invalidate_but_local_overrides_mask_changes() {
    let (storage, engine, main) = open().await;
    let global = engine
        .open_session_at(crate::GLOBAL_BRANCH_ID)
        .await
        .unwrap();
    main.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('owned', 'local')",
        &[],
    )
    .await
    .unwrap();
    for key in ["inherited_probe", "overridden_probe"] {
        global
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global) VALUES ($1, true)",
                &[schema(key, false)],
            )
            .await
            .unwrap();
    }
    let before = catalog_stamp(&storage, &main).await;
    main.execute("SELECT id FROM inherited_probe", &[])
        .await
        .unwrap();
    let after = catalog_stamp(&storage, &main).await;
    assert_ne!(
        after.0, before.0,
        "newly inherited definitions change visibility"
    );
    assert_eq!(after.1, before.1 + 1);
    main.execute("INSERT INTO lix_registered_schema (value) VALUES ($1) ON CONFLICT(schema_key) DO UPDATE SET value = excluded.value", &[schema("overridden_probe", true)]).await.unwrap();

    // Replacing the global definition cannot change the local override.
    global
        .execute(
            "UPDATE lix_registered_schema SET value = $1 WHERE schema_key = 'overridden_probe'",
            &[schema("overridden_probe", true)],
        )
        .await
        .unwrap();
    let before = catalog_stamp(&storage, &main).await;
    main.execute("SELECT extra FROM overridden_probe", &[])
        .await
        .unwrap();
    assert_eq!(catalog_stamp(&storage, &main).await, before);

    global
        .execute(
            "UPDATE lix_registered_schema SET value = $1 WHERE schema_key = 'inherited_probe'",
            &[schema("inherited_probe", true)],
        )
        .await
        .unwrap();
    let before = catalog_stamp(&storage, &main).await;
    main.execute("SELECT extra FROM inherited_probe", &[])
        .await
        .unwrap();
    let after = catalog_stamp(&storage, &main).await;
    assert_ne!(after.0, before.0);
    assert_eq!(after.1, before.1 + 1);
    main.execute(
        "INSERT INTO inherited_probe (id, extra) VALUES ('inherited', 'validated')",
        &[],
    )
    .await
    .unwrap();
    main.execute(
        "INSERT INTO overridden_probe (id, extra) VALUES ('local', 'validated')",
        &[],
    )
    .await
    .unwrap();
}
