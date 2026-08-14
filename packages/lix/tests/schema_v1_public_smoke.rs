use lix::{Value, open_lix};
use serde_json::json;

#[tokio::test]
async fn seven_types_register_write_and_read_through_public_api() {
    let lix = open_lix().await.expect("memory Lix opens");
    let schema = json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "seven_type_probe",
        "columns": [
            {"name": "id", "type": "uuid", "nullable": false, "default_expression": "uuidv7()"},
            {"name": "label", "type": "text", "nullable": false},
            {"name": "count", "type": "int8", "nullable": false},
            {"name": "ratio", "type": "float8", "nullable": false},
            {"name": "active", "type": "boolean", "nullable": false},
            {"name": "metadata", "type": "jsonb", "nullable": false},
            {"name": "created_at", "type": "timestamptz", "nullable": false, "default_expression": "CURRENT_TIMESTAMP"}
        ],
        "primary_key": ["id"]
    });

    lix.execute(
        "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, CAST($2 AS JSONB))",
        &[
            Value::Text("seven_type_probe".into()),
            Value::Text(schema.to_string()),
        ],
    )
    .await
    .expect("Schema v1 registration succeeds");
    lix.execute(
        "INSERT INTO seven_type_probe (label, count, ratio, active, metadata) \
         VALUES ('ready', 42, 1.5, true, '{\"answer\":42}'::jsonb)",
        &[],
    )
    .await
    .expect("seven-type row writes");

    let result = lix
        .execute(
            "SELECT id, label, count, ratio, active, metadata, created_at \
             FROM seven_type_probe",
            &[],
        )
        .await
        .expect("seven-type row reads");
    let values = result.rows()[0].values();
    assert!(matches!(&values[0], Value::Text(id) if uuid::Uuid::parse_str(id).is_ok()));
    assert_eq!(values[1], Value::Text("ready".into()));
    assert_eq!(values[2], Value::Integer(42));
    assert_eq!(values[3], Value::Real(1.5));
    assert_eq!(values[4], Value::Boolean(true));
    assert_eq!(values[5], Value::Json(json!({"answer": 42}).into()));
    assert!(
        matches!(values[6], Value::Timestamp(_)),
        "timestamptz projection returned {:?}",
        values[6]
    );
    let Value::Text(id) = &values[0] else {
        unreachable!("UUID projection was asserted above")
    };
    let id = id.clone();

    let updated = lix
        .execute(
            "UPDATE seven_type_probe SET count = 43 WHERE id = $1 RETURNING id, count",
            &[Value::Text(id.clone())],
        )
        .await
        .expect("seven-type row updates");
    assert_eq!(updated.rows()[0].values()[1], Value::Integer(43));

    let point = lix
        .execute(
            "SELECT count, metadata FROM seven_type_probe WHERE id = $1",
            &[Value::Text(id.clone())],
        )
        .await
        .expect("seven-type point read");
    assert_eq!(point.rows()[0].values()[0], Value::Integer(43));
    let range = lix
        .execute(
            "SELECT id, label FROM seven_type_probe ORDER BY id LIMIT 10",
            &[],
        )
        .await
        .expect("seven-type range read");
    assert_eq!(range.rows().len(), 1);
    let full = lix
        .execute("SELECT * FROM seven_type_probe", &[])
        .await
        .expect("seven-type full projection");
    assert_eq!(full.rows().len(), 1);
    let deleted = lix
        .execute(
            "DELETE FROM seven_type_probe WHERE id = $1 RETURNING id",
            &[Value::Text(id)],
        )
        .await
        .expect("seven-type row deletes");
    assert_eq!(deleted.rows().len(), 1);
    assert_eq!(
        lix.execute("SELECT * FROM seven_type_probe", &[])
            .await
            .expect("deleted row remains absent")
            .rows()
            .len(),
        0
    );

    lix.close().await.expect("memory Lix closes");
}

#[tokio::test]
async fn dynamically_registered_rows_coexist_with_native_file_and_commit_surfaces() {
    let lix = open_lix().await.expect("memory Lix opens");
    let schema = json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "qualification_row",
        "columns": [
            {"name": "id", "type": "uuid", "nullable": false},
            {"name": "value", "type": "text", "nullable": false}
        ],
        "primary_key": ["id"]
    });
    lix.execute(
        "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, CAST($2 AS JSONB))",
        &[
            Value::Text("qualification_row".into()),
            Value::Text(schema.to_string()),
        ],
    )
    .await
    .expect("dynamic Schema-v1 registration succeeds");
    let id = "01920000-0000-7000-8000-0000000000a1";
    lix.execute(
        "INSERT INTO qualification_row (id, value) VALUES ($1, 'native')",
        &[Value::Text(id.into())],
    )
    .await
    .expect("dynamic native row writes");
    assert_eq!(
        lix.execute(
            "SELECT value FROM qualification_row WHERE id = $1",
            &[Value::Text(id.into())],
        )
        .await
        .expect("dynamic native row reads")
        .rows()[0]
            .values()[0],
        Value::Text("native".into())
    );

    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ('/carrier.txt', CAST('payload' AS BYTEA))",
        &[],
    )
    .await
    .expect("filesystem planning ignores unrelated dynamic state");
    assert_eq!(
        lix.execute("SELECT path FROM lix_file WHERE path = '/carrier.txt'", &[])
            .await
            .expect("native file row reads")
            .rows()
            .len(),
        1
    );
    assert!(
        !lix.execute("SELECT id FROM lix_commit ORDER BY id", &[])
            .await
            .expect("derived lix_commit rows use native tuples")
            .rows()
            .is_empty()
    );
    assert!(
        !lix.execute("SELECT id, commit_id FROM lix_branch_ref", &[])
            .await
            .expect("derived branch-ref rows use native tuples")
            .rows()
            .is_empty()
    );
    let branch_id = lix.active_branch_id().await.expect("active branch");
    let reopened = lix
        .open_session(branch_id)
        .await
        .expect("open retained repository session");
    assert_eq!(
        reopened
            .execute(
                "SELECT value FROM qualification_row WHERE id = $1",
                &[Value::Text(id.into())],
            )
            .await
            .expect("retained dynamic row reopens")
            .rows()
            .len(),
        1
    );
    lix.close().await.expect("memory Lix closes");
}

#[tokio::test]
async fn native_projection_preserves_null_json_filter_order_and_limit_semantics() {
    let lix = open_lix().await.expect("memory Lix opens");
    let schema = json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "native_projection_probe",
        "columns": [
            {"name": "id", "type": "uuid", "nullable": false},
            {"name": "note", "type": "text", "nullable": true},
            {"name": "enabled", "type": "boolean", "nullable": false},
            {"name": "payload", "type": "jsonb", "nullable": true}
        ],
        "primary_key": ["id"]
    });
    lix.execute(
        "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, CAST($2 AS JSONB))",
        &[
            Value::Text("native_projection_probe".into()),
            Value::Text(schema.to_string()),
        ],
    )
    .await
    .expect("projection schema registers");
    lix.execute(
        "INSERT INTO native_projection_probe (id, note, enabled, payload) VALUES \
         ('01920000-0000-7000-8000-000000000001', NULL, true, CAST('null' AS JSONB)), \
         ('01920000-0000-7000-8000-000000000002', 'keep', true, CAST('{\"z\":2,\"a\":1,\"z\":3}' AS JSONB)), \
         ('01920000-0000-7000-8000-000000000003', 'drop', false, CAST('[1,2]' AS JSONB))",
        &[],
    )
    .await
    .expect("native projection rows write");

    let rows = lix
        .execute(
            "SELECT id, note, payload FROM native_projection_probe \
             WHERE enabled = true ORDER BY id LIMIT 2",
            &[],
        )
        .await
        .expect("native residual filter and projection execute");
    assert_eq!(rows.rows().len(), 2);
    assert_eq!(rows.rows()[0].values()[1], Value::Null);
    // The Arrow jsonb cell contains the canonical text `null`; the public
    // result contract represents that JSON scalar as Value::Null.
    assert_eq!(rows.rows()[0].values()[2], Value::Null);
    assert_eq!(rows.rows()[1].values()[1], Value::Text("keep".into()));
    assert_eq!(
        rows.rows()[1].values()[2],
        Value::Json(json!({"a": 1, "z": 3}).into())
    );

    lix.execute(
        "DELETE FROM native_projection_probe WHERE id = \
         '01920000-0000-7000-8000-000000000001'",
        &[],
    )
    .await
    .expect("native row tombstones");
    let remaining = lix
        .execute(
            "SELECT id FROM native_projection_probe WHERE enabled = true ORDER BY id LIMIT 1",
            &[],
        )
        .await
        .expect("limit applies after tombstone visibility");
    assert_eq!(remaining.rows().len(), 1);
    assert_eq!(
        remaining.rows()[0].values()[0],
        Value::Text("01920000-0000-7000-8000-000000000002".into())
    );
    lix.close().await.expect("memory Lix closes");
}
