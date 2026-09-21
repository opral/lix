use lix::{ResultColumnType, Value};
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(text_cast_drops_jsonb_result_metadata, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    let json_value = Value::Jsonb(json!({"a": 1}).into());
    let json_null = Value::Jsonb(json!(null).into());

    for (value, expected) in [
        (&json_value, Value::Text("{\"a\":1}".into())),
        (&json_null, Value::Text("null".into())),
    ] {
        let direct = session
            .execute(
                "SELECT CAST($1 AS TEXT) AS explicit_text",
                std::slice::from_ref(value),
            )
            .await
            .expect("text cast projection should succeed");
        assert_eq!(direct.columns(), ["explicit_text"]);
        assert_eq!(direct.column_types(), [ResultColumnType::Text]);
        assert_rows_eq(direct, vec![vec![expected]]);
    }

    let nested = session
        .execute(
            "SELECT CASE WHEN true THEN CAST($1 AS TEXT) \
             ELSE COALESCE(CAST($1 AS TEXT), 'fallback') END AS nested_text",
            std::slice::from_ref(&json_value),
        )
        .await
        .expect("nested text casts should preserve expression scope");
    assert_eq!(nested.columns(), ["nested_text"]);
    assert_eq!(nested.column_types(), [ResultColumnType::Text]);
    assert_rows_eq(nested, vec![vec![Value::Text("{\"a\":1}".into())]]);

    let nested_json_cast = session
        .execute(
            "SELECT CAST(CAST($1 AS TEXT) AS JSONB) AS nested_json",
            std::slice::from_ref(&json_null),
        )
        .await
        .expect("nested text-to-JSONB cast should preserve JSON metadata");
    assert_eq!(nested_json_cast.columns(), ["nested_json"]);
    assert_eq!(nested_json_cast.column_types(), [ResultColumnType::Jsonb]);
    assert_rows_eq(
        nested_json_cast,
        vec![vec![Value::Jsonb(json!(null).into())]],
    );

    let malformed_json_cast = session
        .execute("SELECT CAST('not-json' AS JSONB)", &[])
        .await
        .expect_err("JSONB casts should reject malformed text");
    assert!(
        malformed_json_cast
            .message
            .to_ascii_lowercase()
            .contains("json")
    );

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('text-cast-metadata', $1)",
            std::slice::from_ref(&json_null),
        )
        .await
        .expect("cast metadata fixture should insert");

    let source_column = session
        .execute(
            "SELECT CAST(value AS TEXT) AS rendered_text \
             FROM lix_key_value WHERE key LIKE 'text-cast-metadata'",
            &[],
        )
        .await
        .expect("source-column text cast should succeed");
    assert_eq!(source_column.columns(), ["rendered_text"]);
    assert_eq!(source_column.column_types(), [ResultColumnType::Text]);
    assert_rows_eq(source_column, vec![vec![Value::Text("null".into())]]);

    let text_comparison = session
        .execute(
            "SELECT CAST(value AS TEXT) = 'null' AS matches_text \
             FROM lix_key_value WHERE key LIKE 'text-cast-metadata'",
            &[],
        )
        .await
        .expect("TEXT casts of JSONB columns should compare as text");
    assert_eq!(text_comparison.columns(), ["matches_text"]);
    assert_eq!(text_comparison.column_types(), [ResultColumnType::Boolean]);
    assert_rows_eq(text_comparison, vec![vec![Value::Boolean(true)]]);

    let returning = session
        .execute(
            "UPDATE lix_key_value SET value = value \
             WHERE key LIKE 'text-cast-metadata' \
             RETURNING CAST(value AS TEXT) AS rendered_text",
            &[],
        )
        .await
        .expect("source-column text cast in generic RETURNING should succeed");
    assert_eq!(returning.columns(), ["rendered_text"]);
    assert_eq!(returning.column_types(), [ResultColumnType::Text]);
    assert_rows_eq(returning, vec![vec![Value::Text("null".into())]]);

    let text_comparison_returning = session
        .execute(
            "UPDATE lix_key_value SET value = value \
             WHERE CAST(value AS TEXT) = 'null' \
             RETURNING CAST(value AS TEXT) AS rendered_text",
            &[],
        )
        .await
        .expect("generic RETURNING should compare TEXT casts as text");
    assert_eq!(
        text_comparison_returning.column_types(),
        [ResultColumnType::Text]
    );
    assert_rows_eq(
        text_comparison_returning,
        vec![vec![Value::Text("null".into())]],
    );

    let nested_returning = session
        .execute(
            "UPDATE lix_key_value SET value = value \
             WHERE key LIKE 'text-cast-metadata' \
             RETURNING COALESCE(CAST(value AS TEXT), 'fallback') AS rendered_text",
            &[],
        )
        .await
        .expect("nested text cast in generic RETURNING should succeed");
    assert_eq!(nested_returning.columns(), ["rendered_text"]);
    assert_eq!(nested_returning.column_types(), [ResultColumnType::Text]);
    assert_rows_eq(nested_returning, vec![vec![Value::Text("null".into())]]);

    let nested_json_returning = session
        .execute(
            "UPDATE lix_key_value SET value = value \
             WHERE key LIKE 'text-cast-metadata' \
             RETURNING CAST(CAST(value AS TEXT) AS JSONB) AS nested_json",
            &[],
        )
        .await
        .expect("nested text-to-JSONB RETURNING should preserve JSON metadata");
    assert_eq!(nested_json_returning.columns(), ["nested_json"]);
    assert_eq!(
        nested_json_returning.column_types(),
        [ResultColumnType::Jsonb]
    );
    assert_rows_eq(
        nested_json_returning,
        vec![vec![Value::Jsonb(json!(null).into())]],
    );

    let nested_json_update = session
        .execute(
            "UPDATE lix_key_value SET value = CAST(CAST(value AS TEXT) AS JSONB) \
             WHERE key LIKE 'text-cast-metadata' \
             RETURNING value",
            &[],
        )
        .await
        .expect("generic UPDATE should share TEXT-to-JSONB conversion semantics");
    assert_eq!(nested_json_update.column_types(), [ResultColumnType::Jsonb]);
    assert_rows_eq(
        nested_json_update,
        vec![vec![Value::Jsonb(json!(null).into())]],
    );

    let malformed_json_update = session
        .execute(
            "UPDATE lix_key_value SET value = CAST('not-json' AS JSONB) \
             WHERE key LIKE 'text-cast-metadata' \
             RETURNING value",
            &[],
        )
        .await
        .expect_err("generic UPDATE JSONB casts should reject malformed text");
    assert!(
        malformed_json_update
            .message
            .to_ascii_lowercase()
            .contains("json")
    );

    let uuid_schema = json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "uuid_cast_metadata",
        "columns": [
            {"name": "id", "type": "uuid", "nullable": false},
            {"name": "label", "type": "text", "nullable": false}
        ],
        "primary_key": ["id"]
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES ($1)",
            &[Value::Jsonb(uuid_schema.into())],
        )
        .await
        .expect("UUID cast metadata schema should register");
    let uuid = "01991b1d-6d8b-7000-8000-0000000000f1";
    let uuid_upper = uuid.to_uppercase();
    session
        .execute(
            "INSERT INTO uuid_cast_metadata (id, label) VALUES (CAST($1 AS UUID), 'row')",
            &[Value::Text(uuid.into())],
        )
        .await
        .expect("UUID row should insert");

    let uuid_read = session
        .execute(
            "SELECT CAST(id AS TEXT) AS rendered_id \
             FROM uuid_cast_metadata WHERE id = CAST($1 AS UUID)",
            &[Value::Text(uuid_upper.clone())],
        )
        .await
        .expect("explicit UUID casts should accept alternate case");
    assert_eq!(uuid_read.columns(), ["rendered_id"]);
    assert_eq!(uuid_read.column_types(), [ResultColumnType::Text]);
    assert_rows_eq(uuid_read, vec![vec![Value::Text(uuid.into())]]);

    let uuid_returning = session
        .execute(
            "UPDATE uuid_cast_metadata SET label = 'updated' \
             WHERE id = CAST($1 AS UUID) AND label LIKE '%' RETURNING CAST(id AS TEXT) AS rendered_id",
            &[Value::Text(uuid_upper)],
        )
        .await
        .expect("UUID comparison should work in generic UPDATE");
    assert_eq!(uuid_returning.columns(), ["rendered_id"]);
    assert_eq!(uuid_returning.column_types(), [ResultColumnType::Text]);
    assert_rows_eq(uuid_returning, vec![vec![Value::Text(uuid.into())]]);
});
