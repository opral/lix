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

    lix.close().await.expect("memory Lix closes");
}
