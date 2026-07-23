use lix_sdk::{Memory, OpenLixOptions, Value, open_lix};
use serde_json::{Value as JsonValue, json};

#[tokio::test]
async fn client_state_roundtrips_every_json_kind_and_upserts() {
    let lix = open_lix(OpenLixOptions::default())
        .await
        .expect("memory Lix opens");
    let client_state = lix.client_state();
    let values = [
        ("null", JsonValue::Null),
        ("boolean", json!(true)),
        ("integer", json!(42)),
        ("real", json!(42.5)),
        ("string", json!("value")),
        ("array", json!([1, "two", false, null])),
        ("object", json!({ "nested": { "enabled": true } })),
    ];

    for (key, value) in values {
        client_state
            .set(key, value.clone())
            .await
            .expect("client state value writes");
        assert_eq!(
            client_state.get(key).await.expect("client state reads"),
            Some(value)
        );
    }

    client_state
        .set("upsert", json!({ "version": 1 }))
        .await
        .expect("initial client state writes");
    client_state
        .set("upsert", json!({ "version": 2 }))
        .await
        .expect("client state upserts");
    assert_eq!(
        client_state
            .get("upsert")
            .await
            .expect("upserted state reads"),
        Some(json!({ "version": 2 }))
    );
    assert!(
        client_state
            .entries()
            .await
            .expect("client state entries read")
            .contains(&("upsert".to_string(), json!({ "version": 2 })))
    );

    let placement = lix
        .execute(
            "SELECT lixcol_branch_id, lixcol_global, lixcol_untracked \
             FROM lix_key_value_by_branch \
             WHERE key = 'lix_client_state:upsert' \
               AND lixcol_branch_id = 'global'",
            &[],
        )
        .await
        .expect("client state placement reads");
    assert_eq!(placement.len(), 1);
    assert_eq!(
        placement.rows()[0].values(),
        &[
            Value::Text("global".to_string()),
            Value::Boolean(true),
            Value::Boolean(true),
        ]
    );

    lix.close().await.expect("memory Lix closes");
}

#[tokio::test]
async fn client_state_delete_is_idempotent_and_builtin_keys_are_excluded() {
    let lix = open_lix(OpenLixOptions::default())
        .await
        .expect("memory Lix opens");
    let client_state = lix.client_state();

    let error = client_state
        .get("")
        .await
        .expect_err("empty client state keys must be rejected");
    assert_eq!(error.code, "LIX_INVALID_PARAM");

    assert_eq!(
        client_state
            .get("lix_id")
            .await
            .expect("client state read succeeds"),
        None,
        "the built-in lix_id KV row must not leak into client state"
    );
    client_state
        .set("lix_id", json!("client-owned"))
        .await
        .expect("prefixed logical key writes");
    assert_eq!(
        client_state
            .get("lix_id")
            .await
            .expect("prefixed logical key reads"),
        Some(json!("client-owned"))
    );

    let builtin = lix
        .execute(
            "SELECT value FROM lix_key_value_by_branch \
             WHERE key = 'lix_id' AND lixcol_branch_id = 'global'",
            &[],
        )
        .await
        .expect("built-in KV row reads");
    assert_eq!(builtin.len(), 1, "the built-in lix_id row remains intact");
    assert!(matches!(
        builtin.rows()[0].value("value"),
        Ok(Value::Json(_))
    ));

    client_state
        .delete("lix_id")
        .await
        .expect("existing client key deletes");
    client_state
        .delete("lix_id")
        .await
        .expect("missing client key delete is a no-op");
    assert_eq!(
        client_state
            .get("lix_id")
            .await
            .expect("deleted client key reads"),
        None
    );

    lix.close().await.expect("memory Lix closes");
}

#[tokio::test]
async fn client_state_survives_memory_snapshot_reopen() {
    let storage = Memory::new();
    let lix = open_lix(OpenLixOptions::new(storage.clone()))
        .await
        .expect("memory Lix opens");
    lix.client_state()
        .set(
            "atelier/ui-state/v1",
            json!({ "sidebar": "history", "width": 320 }),
        )
        .await
        .expect("client state writes");
    lix.close().await.expect("first memory Lix closes");

    let snapshot = storage.export_snapshot().expect("memory snapshot exports");
    let restored_storage =
        Memory::from_snapshot(&snapshot).expect("memory snapshot imports into new storage");
    let restored = open_lix(OpenLixOptions::new(restored_storage))
        .await
        .expect("snapshot Lix reopens");

    assert_eq!(
        restored
            .client_state()
            .get("atelier/ui-state/v1")
            .await
            .expect("restored client state reads"),
        Some(json!({ "sidebar": "history", "width": 320 }))
    );

    restored.close().await.expect("restored memory Lix closes");
}
