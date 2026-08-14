use serde_json::json;

use crate::{validate_lix_schema, validate_lix_schema_definition};

fn schema() -> serde_json::Value {
    json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "acme_note",
        "columns": [
            { "name": "id", "type": "uuid", "nullable": false, "default_expression": "uuidv7()" },
            { "name": "title", "type": "text", "nullable": false },
            { "name": "payload", "type": "jsonb", "nullable": false },
            { "name": "rank", "type": "int8", "nullable": false }
        ],
        "primary_key": ["id"],
        "unique": [["title"]]
    })
}

#[test]
fn accepts_schema_v1_and_rows() {
    let schema = schema();
    validate_lix_schema_definition(&schema).expect("valid Schema v1");
    validate_lix_schema(
        &schema,
        &json!({
            "id": "01920000-0000-7000-8000-000000000001",
            "title": "hello",
            "payload": { "nested": [1, null, true] },
            "rank": 1
        }),
    )
    .expect("valid row");
}

#[test]
fn rejects_json_schema_and_unknown_fields() {
    let legacy = json!({
        "x-lix-key": "acme_note",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "properties": { "id": { "type": "string" } }
    });
    assert!(validate_lix_schema_definition(&legacy).is_err());

    let mut unknown = schema();
    unknown["columns"][0]["format"] = json!("uuid");
    assert!(validate_lix_schema_definition(&unknown).is_err());
}

#[test]
fn distinguishes_sql_null_from_jsonb_null() {
    let schema = schema();
    validate_lix_schema(
        &schema,
        &json!({
            "id": "01920000-0000-7000-8000-000000000001",
            "title": "hello",
            "payload": null,
            "rank": 1
        }),
    )
    .expect("JSONB null is a non-NULL JSONB value");

    let error = validate_lix_schema(
        &schema,
        &json!({
            "id": "01920000-0000-7000-8000-000000000001",
            "title": null,
            "payload": {},
            "rank": 1
        }),
    )
    .expect_err("NOT NULL text rejects SQL NULL");
    assert!(error.message.contains("/title"));
}

#[test]
fn restricts_defaults_to_supported_postgresql_expression() {
    let mut invalid = schema();
    invalid["columns"][0]["default_expression"] = json!("agent_context() || random()");
    assert!(validate_lix_schema_definition(&invalid).is_err());
}

#[tokio::test]
async fn postgresql_jsonb_syntax_registers_queries_and_updates_entities() {
    use crate::Value;
    use crate::engine::Engine;
    use crate::storage::Memory;

    let storage = Memory::new();
    Engine::initialize(storage.clone()).await.unwrap();
    let engine = Engine::new(storage).await.unwrap();
    let session = engine.open_session().await.unwrap();
    let definition = json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "acme_jsonb_probe",
        "columns": [
            { "name": "id", "type": "text", "nullable": false },
            { "name": "payload", "type": "jsonb", "nullable": false }
        ],
        "primary_key": ["id"]
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, $2)",
            &[
                Value::Text("acme_jsonb_probe".into()),
                Value::Json(definition.into()),
            ],
        )
        .await
        .unwrap();
    session
        .execute(
            "INSERT INTO acme_jsonb_probe (id, payload) VALUES ('a', '{\"name\":\"Ada\",\"user\":{\"names\":[\"Ada\",\"Lin\"]}}'::jsonb)",
            &[],
        )
        .await
        .unwrap();
    let selected = session
        .execute(
            "SELECT payload ->> 'name' FROM acme_jsonb_probe WHERE id = 'a'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(selected.rows()[0].values(), &[Value::Text("Ada".into())]);

    let operators = session
        .execute(
            "SELECT payload #>> '{user,names,-1}', \
             payload @> '{\"name\":\"Ada\"}', \
             payload ? 'name' FROM acme_jsonb_probe WHERE id = 'a'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        operators.rows()[0].values(),
        &[
            Value::Text("Lin".into()),
            Value::Boolean(true),
            Value::Boolean(true),
        ]
    );

    let parameterized_exists = session
        .execute(
            "SELECT payload ? $1 FROM acme_jsonb_probe WHERE id = 'a'",
            &[Value::Text("name".into())],
        )
        .await
        .unwrap();
    assert_eq!(
        parameterized_exists.rows()[0].values(),
        &[Value::Boolean(true)]
    );

    let semantics = session
        .execute(
            "SELECT \
                '{\"b\":2,\"a\":1}'::jsonb = '{\"a\":1,\"b\":2}'::jsonb, \
                '{\"a\":1,\"a\":2}'::jsonb = '{\"a\":2}'::jsonb, \
                '1.0'::jsonb = '1'::jsonb, \
                '{\"a\":[1,2,3]}'::jsonb @> '{\"a\":[2]}'::jsonb, \
                '{\"a\":null}'::jsonb -> 'a', \
                '{\"a\":null}'::jsonb -> 'missing'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        semantics.rows()[0].values(),
        &[
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Json(json!(null).into()),
            Value::Null,
        ]
    );

    let nul_error = session
        .execute("SELECT '\"\\u0000\"'::jsonb", &[])
        .await
        .expect_err("PostgreSQL JSONB rejects Unicode NUL");
    assert!(nul_error.message.contains("NUL"), "{nul_error:?}");
}
