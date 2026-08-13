use std::collections::BTreeMap;

use lix_schema::{
    CompiledSchema, ErrorKind, SCHEMA_V1_JSON, SqlValue, from_json, validate_amendment,
};

fn schema_json(extra_columns: &str) -> String {
    format!(
        r#"{{
          "$schema":"https://lix.dev/schema-v1.json",
          "key":"example_task",
          "columns":[
            {{"name":"id","type":"uuid","nullable":false,"default_expression":"uuidv7()"}},
            {{"name":"metadata","type":"jsonb","nullable":true}}{extra_columns}
          ],
          "primary_key":["id"]
        }}"#
    )
}

#[test]
fn rejects_unknown_fields() {
    let input = schema_json("").replace("\"key\":", "\"unknown\":true,\"key\":");
    let error = from_json(&input).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Parse);
}

#[test]
fn preserves_sql_null_and_json_null_distinction() {
    let schema = from_json(&schema_json("")).unwrap();
    let compiled = CompiledSchema::compile(&schema).unwrap();
    let id = uuid::Uuid::parse_str("019c6b89-bb18-77a8-9164-84b8d46f7bb0").unwrap();
    let json_null = BTreeMap::from([
        ("id".to_owned(), SqlValue::Uuid(id)),
        (
            "metadata".to_owned(),
            SqlValue::Jsonb(serde_json::Value::Null),
        ),
    ]);
    compiled.validate_row(&json_null).unwrap();

    let sql_null = BTreeMap::from([
        ("id".to_owned(), SqlValue::Null),
        ("metadata".to_owned(), SqlValue::Null),
    ]);
    assert_eq!(compiled.validate_row(&sql_null).unwrap_err().path, "/id");

    compiled
        .validate(&serde_json::json!({
            "id": "019c6b89-bb18-77a8-9164-84b8d46f7bb0",
            "metadata": null
        }))
        .unwrap();
}

#[test]
fn allows_only_safe_append_amendments() {
    let previous = from_json(&schema_json("")).unwrap();
    let next = from_json(&schema_json(
        r#",{"name":"description","type":"text","nullable":true}"#,
    ))
    .unwrap();
    validate_amendment(&previous, &next).unwrap();

    let changed = from_json(&schema_json("").replace("\"jsonb\"", "\"text\"")).unwrap();
    assert_eq!(
        validate_amendment(&previous, &changed).unwrap_err().kind,
        ErrorKind::Amendment
    );
}

#[test]
fn official_migrations_all_validate() {
    let meta: serde_json::Value = serde_json::from_str(SCHEMA_V1_JSON).unwrap();
    let validator = jsonschema::JSONSchema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .compile(&meta)
        .unwrap();
    let mut count = 0;
    for entry in std::fs::read_dir("fixtures/current").unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let input = std::fs::read_to_string(entry.path()).unwrap();
        let value: serde_json::Value = serde_json::from_str(&input).unwrap();
        if let Err(errors) = validator.validate(&value) {
            panic!(
                "{}: {}",
                entry.path().display(),
                errors
                    .map(|error| error.to_string())
                    .collect::<Vec<_>>()
                    .join("; ")
            );
        }
        from_json(&input).unwrap();
        count += 1;
    }
    assert_eq!(count, 24);
}
