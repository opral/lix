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
async fn postgresql_jsonb_syntax_registers_queries_and_updates_rows() {
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
                Value::Jsonb(definition.into()),
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
            Value::Jsonb(json!(null).into()),
            Value::Null,
        ]
    );

    let nul_error = session
        .execute("SELECT '\"\\u0000\"'::jsonb", &[])
        .await
        .expect_err("PostgreSQL JSONB rejects Unicode NUL");
    assert!(nul_error.message.contains("NUL"), "{nul_error:?}");
}

// Paraglide creates three different row layouts per translation. 171 translations
// cross the 512-row native segment boundary; insertion order must not change
// whether those layouts can coexist in one commit.
async fn mixed_schema_segment_roundtrip(count: usize, order: &str) {
    use crate::{Value, engine::Engine, storage::Memory};

    let storage = Memory::new();
    Engine::initialize(storage.clone()).await.unwrap();
    let engine = Engine::new(storage.clone()).await.unwrap();
    let session = engine.open_session().await.unwrap();
    let layouts = [
        ("acme_bundle", json!([])),
        (
            "acme_message",
            json!([{ "name": "body", "type": "text", "nullable": false }]),
        ),
        (
            "acme_variant",
            json!([
                { "name": "rank", "type": "int8", "nullable": false },
                { "name": "payload", "type": "jsonb", "nullable": false }
            ]),
        ),
    ];
    for (key, extra) in layouts {
        let mut columns = vec![json!({ "name": "id", "type": "text", "nullable": false })];
        columns.extend(extra.as_array().unwrap().iter().cloned());
        let definition = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": key, "columns": columns, "primary_key": ["id"]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, $2)",
                &[Value::Text(key.into()), Value::Jsonb(definition.into())],
            )
            .await
            .unwrap();
    }
    let mut ids: Vec<_> = (0..count).collect();
    match order {
        "swapped" => ids.swap(0, 1),
        "reversed" => ids.reverse(),
        "sorted" => {}
        _ => unreachable!(),
    }
    let mut tx = session.begin_transaction().await.unwrap();
    for (table, columns) in [
        ("acme_bundle", "id"),
        ("acme_message", "id, body"),
        ("acme_variant", "id, rank, payload"),
    ] {
        let values = ids
            .iter()
            .map(|id| match table {
                "acme_bundle" => format!("('key_{id:04}')"),
                "acme_message" => format!("('key_{id:04}', 'message {id}')"),
                _ => format!("('key_{id:04}', {id}, '{{\"index\":{id}}}'::jsonb)"),
            })
            .collect::<Vec<_>>()
            .join(",");
        tx.execute(
            &format!("INSERT INTO {table} ({columns}) VALUES {values}"),
            &[],
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    let queries = [
        "SELECT id FROM acme_bundle ORDER BY id",
        "SELECT id, body FROM acme_message ORDER BY id",
        "SELECT id, rank, payload FROM acme_variant ORDER BY id",
    ];
    let mut expected = Vec::new();
    for (layout, query) in queries.iter().enumerate() {
        let result = session.execute(query, &[]).await.unwrap();
        assert_eq!(result.rows().len(), count);
        for (id, row) in result.rows().iter().enumerate() {
            let mut values = vec![Value::Text(format!("key_{id:04}"))];
            match layout {
                0 => {}
                1 => values.push(Value::Text(format!("message {id}"))),
                2 => values.extend([
                    Value::Integer(id as i64),
                    Value::Jsonb(json!({"index": id}).into()),
                ]),
                _ => unreachable!(),
            }
            assert_eq!(row.values(), values);
        }
        expected.push(result);
    }
    drop(session);
    drop(engine);
    let engine = Engine::new(storage.clone()).await.unwrap();
    let session = engine.open_session().await.unwrap();
    for (query, expected) in queries.into_iter().zip(&expected) {
        assert_eq!(
            session.execute(query, &[]).await.unwrap().rows(),
            expected.rows()
        );
    }
    // Rewrite a persisted mixed-layout root, including tombstones and JSONB.
    let mut tx = session.begin_transaction().await.unwrap();
    tx.execute("UPDATE acme_message SET body = 'updated'", &[])
        .await
        .unwrap();
    tx.execute(
        "UPDATE acme_variant SET payload = '{\"updated\":true}'::jsonb",
        &[],
    )
    .await
    .unwrap();
    tx.execute("DELETE FROM acme_bundle WHERE id = 'key_0000'", &[])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let updated_queries = [
        "SELECT id FROM acme_bundle ORDER BY id",
        "SELECT id FROM acme_message WHERE body = 'updated' ORDER BY id",
        "SELECT id FROM acme_variant WHERE payload ->> 'updated' = 'true' ORDER BY id",
    ];
    let mut updated = Vec::new();
    for (index, query) in updated_queries.iter().enumerate() {
        let result = session.execute(query, &[]).await.unwrap();
        assert_eq!(result.rows().len(), count - usize::from(index == 0));
        updated.push(result);
    }
    drop(session);
    drop(engine);
    let reopened = Engine::new(storage).await.unwrap();
    let session = reopened.open_session().await.unwrap();
    for (query, expected) in updated_queries.into_iter().zip(updated) {
        assert_eq!(
            session.execute(query, &[]).await.unwrap().rows(),
            expected.rows()
        );
    }
}

#[tokio::test]
async fn mixed_schema_segment_170_sorted() {
    mixed_schema_segment_roundtrip(170, "sorted").await;
}

#[tokio::test]
async fn mixed_schema_segment_171_sorted() {
    mixed_schema_segment_roundtrip(171, "sorted").await;
}

#[tokio::test]
async fn mixed_schema_segment_171_swapped() {
    mixed_schema_segment_roundtrip(171, "swapped").await;
}

#[tokio::test]
async fn mixed_schema_segment_171_reversed() {
    mixed_schema_segment_roundtrip(171, "reversed").await;
}

#[tokio::test]
async fn mixed_schema_segment_400_sorted() {
    mixed_schema_segment_roundtrip(400, "sorted").await;
}
