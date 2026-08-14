use lix::storage::Storage;
use lix::{Value, open_lix};

pub async fn stage_and_assert_registered_singleton<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("open registered singleton repository");
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "adapter_native_singleton",
        "columns": [
            { "name": "id", "type": "int8", "nullable": false },
            { "name": "value", "type": "text", "nullable": false },
            { "name": "payload", "type": "jsonb", "nullable": false }
        ],
        "primary_key": ["id"]
    });
    lix.execute(
        "INSERT INTO lix_registered_schema (schema_key, value) VALUES (CAST($1 AS JSONB) ->> 'key', CAST($1 AS JSONB))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .expect("register adapter singleton schema");
    lix.execute(
        "INSERT INTO adapter_native_singleton (id, value, payload) VALUES ($1, $2, CAST($3 AS JSONB))",
        &[
            Value::Integer(7),
            Value::Text("native".to_owned()),
            Value::Text(r#"{"adapter":true}"#.to_owned()),
        ],
    )
    .await
    .expect("insert adapter singleton");
    assert_registered_singleton(&lix).await;
}

pub async fn assert_reopened_registered_singleton<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("reopen registered singleton repository");
    assert_registered_singleton(&lix).await;
}

async fn assert_registered_singleton<S>(lix: &lix::Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let rows = lix
        .execute(
            "SELECT id, value, payload FROM adapter_native_singleton WHERE id = $1",
            &[Value::Integer(7)],
        )
        .await
        .expect("read adapter singleton");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.rows()[0].get::<i64>("id").expect("id"), 7);
    assert_eq!(
        rows.rows()[0].get::<String>("value").expect("value"),
        "native"
    );
    assert_eq!(
        rows.rows()[0].get::<String>("payload").expect("payload"),
        r#"{"adapter":true}"#
    );
}
