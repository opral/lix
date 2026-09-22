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
            "54916ea6df172c6fe79b9a9f13e997dd3ef3cd141d505ec3ab612205c4ff1e49",
        ),
        (
            "json",
            "json_object_member",
            "b7e4f4671f04b6233702dfec9aeb4db70c653df78b7365d5772ccbb85a027c7f",
        ),
        (
            "json",
            "json_array_item",
            "b58a8f33fdb6c9846a81f877f7b6cb6ab44bccb979f86015fdaccf78547a040f",
        ),
        (
            "excalidraw",
            "excalidraw_scene",
            "77c887d6cd7dbc6119f655eb4952fa534bb34a7a5442d9c34ea000c8f7f7769a",
        ),
        (
            "excalidraw",
            "excalidraw_element",
            "3b8d46ade01074bae9f83d2347e7ccc499fb31d4de699abf52c3d5a91ab36c0c",
        ),
        (
            "excalidraw",
            "excalidraw_file",
            "40b14d5b53445e3b97ed909f6ccad57e07ddecd450d888678f4d73e821cc2e74",
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
    assert_eq!(count, 23);
}

#[test]
fn deletion_policy_defaults_roundtrips_and_is_not_a_safe_amendment() {
    let mut json = serde_json::json!({
        "$schema":"https://lix.dev/schema-v1.json", "key":"child",
        "columns":[{"name":"id","type":"text","nullable":false}], "primary_key":["id"],
        "foreign_keys":[{"columns":["id"],"references":{"schema_key":"parent","columns":["id"]}}]
    });
    let previous = from_json(&json.to_string()).unwrap();
    assert_eq!(
        previous.foreign_keys[0].on_delete,
        lix_schema::DeleteAction::NoAction
    );
    // Old repository schema JSON preserves its canonical representation.
    assert!(
        serde_json::to_value(&previous).unwrap()["foreign_keys"][0]
            .get("on_delete")
            .is_none()
    );
    json["foreign_keys"][0]["on_delete"] = "no_action".into();
    assert_eq!(previous, from_json(&json.to_string()).unwrap());
    json["foreign_keys"][0]["on_delete"] = "cascade".into();
    let cascade = from_json(&json.to_string()).unwrap();
    assert!(
        lix_schema::to_postgres_ddl(&cascade)
            .unwrap()
            .contains("REFERENCES parent (id) ON DELETE CASCADE")
    );
    assert!(validate_amendment(&previous, &cascade).is_err());
    assert_eq!(
        from_json(&serde_json::to_string(&cascade).unwrap()).unwrap(),
        cascade
    );
    for invalid in ["restrict", "set_null", "CASCADE", ""] {
        json["foreign_keys"][0]["on_delete"] = invalid.into();
        assert!(from_json(&json.to_string()).is_err());
    }
}

#[test]
fn row_reference_constraints_validate_and_preserve_default_policy_omission() {
    let json = serde_json::json!({
        "$schema":"https://lix.dev/schema-v1.json", "key":"row_ref_child",
        "columns":[
            {"name":"id","type":"uuid","nullable":false},
            {"name":"target","type":"text","nullable":false},
            {"name":"optional_target","type":"text","nullable":true}
        ],
        "primary_key":["id"],
        "row_refs":[
            {"column":"target"},
            {"column":"optional_target","on_delete":"cascade"}
        ]
    });
    let schema = from_json(&json.to_string()).unwrap();
    assert_eq!(schema.row_refs.len(), 2);
    assert_eq!(
        schema.row_refs[0].on_delete,
        lix_schema::DeleteAction::NoAction
    );
    assert_eq!(
        schema.row_refs[1].on_delete,
        lix_schema::DeleteAction::Cascade
    );
    let serialized = serde_json::to_value(&schema).unwrap();
    assert_eq!(
        serialized["row_refs"][0],
        serde_json::json!({"column":"target"})
    );
    assert_eq!(
        serialized["row_refs"][1],
        serde_json::json!({"column":"optional_target","on_delete":"cascade"})
    );
    let ddl_error = lix_schema::to_postgres_ddl(&schema).unwrap_err();
    assert_eq!(ddl_error.kind, ErrorKind::Serialization);
    assert!(ddl_error.message.contains("row-reference"));
}

#[test]
fn row_reference_constraints_require_unique_existing_text_columns() {
    let base = serde_json::json!({
        "$schema":"https://lix.dev/schema-v1.json", "key":"row_ref_child",
        "columns":[
            {"name":"id","type":"uuid","nullable":false},
            {"name":"target","type":"text","nullable":true},
            {"name":"count","type":"int8","nullable":true}
        ],
        "primary_key":["id"],
        "row_refs":[{"column":"target"}]
    });
    from_json(&base.to_string()).unwrap();

    for (column, expected_message) in [("missing", "unknown column"), ("count", "must use text")] {
        let mut invalid = base.clone();
        invalid["row_refs"][0]["column"] = column.into();
        let error = from_json(&invalid.to_string()).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Definition);
        assert_eq!(error.path, "/row_refs/0/column");
        assert!(error.message.contains(expected_message));
    }

    let mut duplicate = base;
    duplicate["row_refs"] = serde_json::json!([
        {"column":"target"},
        {"column":"target","on_delete":"cascade"}
    ]);
    let error = from_json(&duplicate.to_string()).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Definition);
    assert_eq!(error.path, "/row_refs/1/column");
    assert!(error.message.contains("duplicates"));
}

#[test]
fn row_reference_constraint_changes_are_not_safe_amendments() {
    let previous = from_json(
        r#"{
          "$schema":"https://lix.dev/schema-v1.json", "key":"row_ref_child",
          "columns":[
            {"name":"id","type":"uuid","nullable":false},
            {"name":"target","type":"text","nullable":true}
          ],
          "primary_key":["id"],
          "row_refs":[{"column":"target"}]
        }"#,
    )
    .unwrap();
    let mut changed = serde_json::to_value(&previous).unwrap();
    changed["row_refs"][0]["on_delete"] = "cascade".into();
    let changed = from_json(&changed.to_string()).unwrap();
    let error = validate_amendment(&previous, &changed).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Amendment);
    assert_eq!(error.path, "/");
}
