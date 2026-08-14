use lix::Value;
use lix::storage::Memory;
use lix::{engine::Engine, session::SessionContext};

const MULTI_ROW_SQL: &str = "SELECT b.id AS bundle_id, m.id AS message_id, \
    v.id AS variant_id FROM bundle b \
    LEFT JOIN message m ON m.\"bundle_id\" = b.id \
    LEFT JOIN variant v ON v.\"message_id\" = m.id WHERE b.id = $1";

#[tokio::test(flavor = "current_thread")]
async fn reusable_physical_read_plan_rebinds_snapshot_and_exact_parameters() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should open");
    let session = engine.open_session().await.expect("session should open");

    for schema in multi_row_schemas() {
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("schema should register");
    }
    for bundle in ["bundle-1", "bundle-2"] {
        session
            .execute(
                "INSERT INTO bundle (id, declarations) VALUES ($1, $2)",
                &[
                    Value::Text(bundle.to_string()),
                    Value::Json(serde_json::json!([]).into()),
                ],
            )
            .await
            .expect("bundle should insert");
    }
    insert_message_variant(&session, "bundle-1-en", "bundle-1").await;

    let bundle_1 = [Value::Text("bundle-1".to_string())];
    for _ in 0..2 {
        let rows = session
            .execute(MULTI_ROW_SQL, &bundle_1)
            .await
            .expect("warm cached query should execute");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows.rows()[0].value("message_id").expect("message column"),
            &Value::Text("bundle-1-en".to_string())
        );
    }

    // The physical operator template must never retain the prior provider or
    // snapshot. Reusing the exact SQL and parameter after a commit must bind a
    // fresh provider and expose the new row.
    insert_message_variant(&session, "bundle-1-de", "bundle-1").await;
    let rows = session
        .execute(MULTI_ROW_SQL, &bundle_1)
        .await
        .expect("cached query should rebind the current snapshot");
    assert_eq!(rows.len(), 2);

    // Parameter values, not only parameter types, are part of the physical
    // template identity. The left join for another bundle must not inherit the
    // cached bundle-1 predicate or rows.
    let bundle_2 = [Value::Text("bundle-2".to_string())];
    let rows = session
        .execute(MULTI_ROW_SQL, &bundle_2)
        .await
        .expect("different exact parameter should execute");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.rows()[0].value("message_id").expect("message column"),
        &Value::Null
    );

    // Aggregates remain on the ordinary DataFusion planner and retain exact
    // SQL semantics.
    for _ in 0..2 {
        let count = session
            .execute("SELECT COUNT(*) AS count FROM bundle", &[])
            .await
            .expect("aggregate fallback should execute");
        assert_eq!(
            count.rows()[0].value("count").expect("count column"),
            &Value::Integer(2)
        );
    }

    // Ordered reads are the dominant OLTP point-read shape, so their sort
    // operator is part of the reusable template. A rebuilt sort must never
    // inherit the previous execution's rows or ordering state, and it must see
    // rows committed after the template was captured.
    for _ in 0..2 {
        let ordered = session
            .execute("SELECT id FROM bundle ORDER BY id DESC", &[])
            .await
            .expect("ordered read should execute");
        assert_eq!(
            ordered
                .rows()
                .iter()
                .map(|row| row.value("id").expect("id column"))
                .collect::<Vec<_>>(),
            vec![
                &Value::Text("bundle-2".to_string()),
                &Value::Text("bundle-1".to_string())
            ]
        );
    }
    session
        .execute(
            "INSERT INTO bundle (id, declarations) VALUES ($1, $2)",
            &[
                Value::Text("bundle-3".to_string()),
                Value::Json(serde_json::json!([]).into()),
            ],
        )
        .await
        .expect("third bundle should insert");
    let ordered = session
        .execute("SELECT id FROM bundle ORDER BY id DESC", &[])
        .await
        .expect("ordered read should rebind the current snapshot");
    assert_eq!(
        ordered
            .rows()
            .iter()
            .map(|row| row.value("id").expect("id column"))
            .collect::<Vec<_>>(),
        vec![
            &Value::Text("bundle-3".to_string()),
            &Value::Text("bundle-2".to_string()),
            &Value::Text("bundle-1".to_string())
        ]
    );
    let limited = session
        .execute("SELECT id FROM bundle ORDER BY id DESC LIMIT 2", &[])
        .await
        .expect("top-k read should execute");
    assert_eq!(
        limited
            .rows()
            .iter()
            .map(|row| row.value("id").expect("id column"))
            .collect::<Vec<_>>(),
        vec![
            &Value::Text("bundle-3".to_string()),
            &Value::Text("bundle-2".to_string())
        ]
    );

    session.close().await.expect("session should close");
    let reopened = engine.open_session().await.expect("session should reopen");
    assert_eq!(
        reopened
            .execute(MULTI_ROW_SQL, &bundle_1)
            .await
            .expect("cached query should rebind after reopen")
            .len(),
        2
    );
}

async fn insert_message_variant(session: &SessionContext, message_id: &str, bundle_id: &str) {
    session
        .execute(
            "INSERT INTO message (id, \"bundle_id\", locale, selectors) VALUES ($1, $2, $3, $4)",
            &[
                Value::Text(message_id.to_string()),
                Value::Text(bundle_id.to_string()),
                Value::Text("locale".to_string()),
                Value::Json(serde_json::json!([]).into()),
            ],
        )
        .await
        .expect("message should insert");
    session
        .execute(
            "INSERT INTO variant (id, \"message_id\", matches, pattern) VALUES ($1, $2, $3, $4)",
            &[
                Value::Text(format!("{message_id}-0")),
                Value::Text(message_id.to_string()),
                Value::Json(serde_json::json!([]).into()),
                Value::Json(serde_json::json!([]).into()),
            ],
        )
        .await
        .expect("variant should insert");
}

fn multi_row_schemas() -> [serde_json::Value; 3] {
    [
        serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "bundle",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "declarations", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["id"],
        }),
        serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "message",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "bundle_id", "type": "text", "nullable": false },
                { "name": "locale", "type": "text", "nullable": false },
                { "name": "selectors", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["id"],
        }),
        serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "variant",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "message_id", "type": "text", "nullable": false },
                { "name": "matches", "type": "jsonb", "nullable": false },
                { "name": "pattern", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["id"],
        }),
    ]
}
