use lix_schema::{
    CompiledSchema, ErrorKind, Row, SCHEMA_V1_JSON, Value, from_json, validate_amendment,
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
    let json_null = Row::from([
        ("id".to_owned(), Value::Uuid(id)),
        (
            "metadata".to_owned(),
            Value::Jsonb(serde_json::Value::Null.into()),
        ),
    ]);
    compiled.validate_row(&json_null).unwrap();

    let sql_null = Row::from([
        ("id".to_owned(), Value::Null),
        ("metadata".to_owned(), Value::Null),
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
fn native_defaults_materialize_without_an_outer_json_row() {
    let schema = from_json(
        r#"{
          "$schema":"https://lix.dev/schema-v1.json",
          "key":"native_defaults",
          "columns":[
            {"name":"id","type":"uuid","nullable":false,"default_expression":"uuidv7()"},
            {"name":"label","type":"text","nullable":false,"default_value":"untitled"},
            {"name":"created_at","type":"timestamptz","nullable":false,"default_expression":"CURRENT_TIMESTAMP"},
            {"name":"payload","type":"jsonb","nullable":false,"default_value":{"ready":true}}
          ],
          "primary_key":["id"]
        }"#,
    )
    .unwrap();
    let compiled = CompiledSchema::compile(&schema).unwrap();
    let id = uuid::Uuid::parse_str("019c6b89-bb18-77a8-9164-84b8d46f7bb0").unwrap();
    let mut row = Row::new();

    assert!(
        compiled
            .apply_defaults(&mut row, || id, || 123_456)
            .unwrap()
    );
    assert_eq!(row["id"], Value::Uuid(id));
    assert_eq!(row["label"], Value::Text("untitled".to_owned()));
    assert_eq!(row["created_at"], Value::Timestamptz(123_456));
    assert_eq!(
        row["payload"],
        Value::Jsonb(serde_json::json!({"ready": true}).into())
    );
    compiled.validate_complete_row(&row).unwrap();
}

#[test]
fn create_row_allows_only_defaulted_primary_key_omissions() {
    let schema = from_json(&schema_json("")).unwrap();
    let compiled = CompiledSchema::compile(&schema).unwrap();
    let complete_except_generated_id = Row::from([("metadata".to_owned(), Value::Null)]);
    compiled
        .validate_create_row(&complete_except_generated_id)
        .unwrap();

    assert_eq!(
        compiled.validate_create_row(&Row::new()).unwrap_err().path,
        "/metadata",
        "nullable columns remain explicit SQL NULLs in a complete typed create"
    );
    let id = uuid::Uuid::parse_str("019c6b89-bb18-77a8-9164-84b8d46f7bb0").unwrap();
    assert_eq!(
        compiled
            .validate_create_row(&Row::from([
                ("id".to_owned(), Value::Uuid(id)),
                ("metadata".to_owned(), Value::Text("wrong type".to_owned())),
            ]))
            .unwrap_err()
            .path,
        "/metadata"
    );
}

#[test]
fn typed_row_body_round_trips_without_outer_json() {
    let schema = from_json(
        r#"{
          "$schema":"https://lix.dev/schema-v1.json",
          "key":"typed_row",
          "columns":[
            {"name":"id","type":"uuid","nullable":false},
            {"name":"count","type":"int8","nullable":false},
            {"name":"enabled","type":"boolean","nullable":false},
            {"name":"metadata","type":"jsonb","nullable":false},
            {"name":"note","type":"text","nullable":true}
          ],
          "primary_key":["id"]
        }"#,
    )
    .unwrap();
    let compiled = CompiledSchema::compile(&schema).unwrap();
    let id = uuid::Uuid::parse_str("019c6b89-bb18-77a8-9164-84b8d46f7bb0").unwrap();
    let row = Row::from([
        ("id".to_owned(), Value::Uuid(id)),
        ("count".to_owned(), Value::Int8(42)),
        ("enabled".to_owned(), Value::Boolean(true)),
        (
            "metadata".to_owned(),
            Value::Jsonb(serde_json::json!({"b": 2, "a": 1}).into()),
        ),
        ("note".to_owned(), Value::Null),
    ]);
    let body = compiled.encode_body(&row).unwrap();
    assert!(
        !body
            .windows(b"metadata".len())
            .any(|window| window == b"metadata")
    );
    assert_eq!(
        compiled
            .decode_body(&Row::from([("id".to_owned(), Value::Uuid(id))]), &body)
            .unwrap(),
        row
    );
}

#[test]
fn typed_row_body_rejects_noncanonical_jsonb_and_out_of_range_timestamps() {
    let schema = from_json(
        r#"{
          "$schema":"https://lix.dev/schema-v1.json",
          "key":"typed_values",
          "columns":[
            {"name":"id","type":"uuid","nullable":false},
            {"name":"metadata","type":"jsonb","nullable":false},
            {"name":"created_at","type":"timestamptz","nullable":false}
          ],
          "primary_key":["id"]
        }"#,
    )
    .unwrap();
    let compiled = CompiledSchema::compile(&schema).unwrap();
    let id = uuid::Uuid::nil();
    let row = Row::from([
        ("id".to_owned(), Value::Uuid(id)),
        (
            "metadata".to_owned(),
            Value::Jsonb(serde_json::json!({"a": 1, "b": 2}).into()),
        ),
        ("created_at".to_owned(), Value::Timestamptz(0)),
    ]);
    let mut body = compiled.encode_body(&row).unwrap();
    let canonical = br#"{"a":1,"b":2}"#;
    let replacement = br#"{"b":2,"a":1}"#;
    let offset = body
        .windows(canonical.len())
        .position(|window| window == canonical)
        .expect("body contains canonical JSONB bytes");
    body[offset..offset + replacement.len()].copy_from_slice(replacement);
    assert!(
        compiled
            .decode_body(&Row::from([("id".to_owned(), Value::Uuid(id))]), &body)
            .is_err()
    );

    let invalid_timestamp = Row::from([
        ("id".to_owned(), Value::Uuid(id)),
        (
            "metadata".to_owned(),
            Value::Jsonb(serde_json::json!({}).into()),
        ),
        ("created_at".to_owned(), Value::Timestamptz(i64::MAX)),
    ]);
    assert_eq!(
        compiled.validate_row(&invalid_timestamp).unwrap_err().path,
        "/created_at"
    );
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
fn bundled_native_jsonb_schemas_match_canonical_fixtures_and_fingerprints() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let plugin_dir = manifest_dir.join("../../plugins");
    let schemas = [
        (
            "json",
            "json_root",
            "041872f6589bbdf8187bccc3894451407073ba21a2a3d396503ac071f8402704",
        ),
        (
            "json",
            "json_object_member",
            "be012f960e381e167cc6da518891a6a68b47233eb67153bbba96bb4ba02f5734",
        ),
        (
            "json",
            "json_array_item",
            "a980f74baf5f9156853cd73cb3222f79ec5dbfaffa5da4d9d7f705c35357d662",
        ),
        (
            "excalidraw",
            "excalidraw_scene",
            "f65d9cc1904cd6279ec391790e83c198a23d053e5cbce7b0526200a4a8a2c601",
        ),
        (
            "excalidraw",
            "excalidraw_element",
            "9d4e6f076618c02e1b15e86c0d77bb36545b78330f967f9049985ac79968fd0f",
        ),
        (
            "excalidraw",
            "excalidraw_file",
            "f0cc6e729ea8d42f69558972e783e66087addfe13ed34435f10da51fa3694847",
        ),
    ];

    for (plugin, schema_key, expected_wire_fingerprint) in schemas {
        let fixture_path = manifest_dir
            .join("fixtures/current")
            .join(format!("{schema_key}.json"));
        let plugin_path = plugin_dir
            .join(plugin)
            .join("schema")
            .join(format!("{schema_key}.json"));
        let fixture_json = std::fs::read_to_string(&fixture_path).unwrap();
        let plugin_json = std::fs::read_to_string(&plugin_path).unwrap();

        assert_eq!(
            fixture_json, plugin_json,
            "canonical fixture drifted from bundled {plugin} schema {schema_key}"
        );

        let fixture = from_json(&fixture_json).unwrap();
        let bundled = from_json(&plugin_json).unwrap();
        assert_eq!(fixture.key, schema_key);
        assert_eq!(
            fixture.fingerprint().unwrap(),
            bundled.fingerprint().unwrap()
        );
        let fixture_wire_fingerprint = fixture.wire_fingerprint().unwrap();
        assert_eq!(
            fixture_wire_fingerprint,
            bundled.wire_fingerprint().unwrap(),
            "wire fingerprint drifted for bundled {plugin} schema {schema_key}"
        );
        assert_eq!(
            fixture_wire_fingerprint.to_hex().as_str(),
            expected_wire_fingerprint,
            "pinned wire fingerprint changed for bundled {plugin} schema {schema_key}"
        );
    }
}

#[test]
fn official_migrations_all_validate() {
    let _: serde_json::Value = serde_json::from_str(SCHEMA_V1_JSON).unwrap();
    let mut count = 0;
    for entry in std::fs::read_dir("fixtures/current").unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let input = std::fs::read_to_string(entry.path()).unwrap();
        from_json(&input).unwrap();
        count += 1;
    }
    assert_eq!(count, 24);
}
