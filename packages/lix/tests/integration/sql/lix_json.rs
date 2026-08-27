use lix::{LixError, Value};
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(schema_v1_seven_types_have_a_runnable_schema_surface, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    let schema = serde_json::json!({
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
    session.execute(
        "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, CAST($2 AS JSONB))",
        &[Value::Text("seven_type_probe".into()), Value::Text(schema.to_string())],
    ).await.unwrap();
    session.execute(
        "INSERT INTO seven_type_probe (label, count, ratio, active, metadata) \
         VALUES ('ready', 42, 1.5, true, '{\"answer\":42}'::jsonb)",
        &[],
    ).await.unwrap();

    let result = session.execute(
        "SELECT id, label, count, ratio, active, metadata, created_at FROM seven_type_probe",
        &[],
    ).await.unwrap();
    let values = result.rows()[0].values();
    assert!(matches!(&values[0], Value::Text(id) if uuid::Uuid::parse_str(id).is_ok()));
    assert_eq!(values[1], Value::Text("ready".into()));
    assert_eq!(values[2], Value::Integer(42));
    assert_eq!(values[3], Value::Real(1.5));
    assert_eq!(values[4], Value::Boolean(true));
    assert_eq!(values[5], Value::Jsonb(json!({"answer": 42}).into()));
    assert!(matches!(values[6], Value::Timestamptz(_)));
});

simulation_test!(timestamptz_is_native_and_current_timestamp_is_stable, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "timestamp_probe",
        "columns": [
            {"name": "id", "type": "int8", "nullable": false},
            {
                "name": "created_at",
                "type": "timestamptz",
                "nullable": false,
                "default_expression": "CURRENT_TIMESTAMP"
            }
        ],
        "primary_key": ["id"]
    });
    session.execute(
        "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, CAST($2 AS JSONB))",
        &[Value::Text("timestamp_probe".into()), Value::Text(schema.to_string())],
    ).await.unwrap();
    session.execute("INSERT INTO timestamp_probe (id) VALUES (1)", &[])
        .await
        .unwrap();

    let row = session
        .execute(
            "SELECT created_at, CURRENT_TIMESTAMP AS first, CURRENT_TIMESTAMP AS second \
             FROM timestamp_probe WHERE id = 1",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(row.rows()[0].values()[0], Value::Timestamptz(_)));
    assert!(matches!(row.rows()[0].values()[1], Value::Timestamptz(_)));
    assert_eq!(row.rows()[0].values()[1], row.rows()[0].values()[2]);
});

simulation_test!(text_primary_keys_reject_jsonb_nul, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "nul_identity_probe",
        "columns": [{"name": "id", "type": "text", "nullable": false}],
        "primary_key": ["id"]
    });
    session.execute(
        "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, CAST($2 AS JSONB))",
        &[Value::Text("nul_identity_probe".into()), Value::Text(schema.to_string())],
    ).await.unwrap();
    let error = session.execute(
        "INSERT INTO nul_identity_probe (id) VALUES ($1)",
        &[Value::Text("a\0b".into())],
    ).await.expect_err("NUL cannot be represented by JSONB identity");
    assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
});

simulation_test!(
    lix_json_expression_results_are_semantic_json,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let result = session
            .execute(
                "SELECT \
                CAST('{\"name\":\"Ada\",\"tags\":[\"db\"]}' AS JSONB) AS document, \
                CAST(NULL AS JSONB) AS json_null, \
                '{\"name\":\"Ada\",\"tags\":[\"db\"]}'::jsonb -> 'tags' AS tags, \
                '{\"name\":\"Ada\"}'::jsonb -> 'missing' AS missing",
                &[],
            )
            .await
            .expect("select should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Jsonb(json!({"name": "Ada", "tags": ["db"]}).into()),
                Value::Null,
                Value::Jsonb(json!(["db"]).into()),
                Value::Null,
            ]],
        );
    }
);

simulation_test!(postgres_jsonb_path_operator_uses_text_array_path, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let result = session
            .execute(
                "SELECT '{\"user\":{\"names\":[\"Ada\"]}}'::jsonb #>> '{user,names,0}' AS name",
                &[],
            )
            .await
            .expect("select should succeed");

    assert_rows_eq(result, vec![vec![Value::Text("Ada".to_string())]]);
});

simulation_test!(postgres_jsonb_key_operator_treats_jsonpath_as_a_literal_key, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let result = session
        .execute(
            "SELECT '{\"path\":\"ok\"}'::jsonb ->> '$.path'",
            &[],
        )
        .await
        .expect("PostgreSQL key operands are literal keys");
    assert_rows_eq(result, vec![vec![Value::Null]]);
});

simulation_test!(
    json_column_predicates_reject_bare_text_literals,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "SELECT row_pk FROM lix_change WHERE row_pk = 'state-latest'",
                &[],
            )
            .await
            .expect_err("JSON column compared to text should fail loudly");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(
            error.hint().is_some_and(|hint| hint.contains("::jsonb")),
            "expected PostgreSQL JSONB hint: {error}"
        );
    }
);

simulation_test!(
    json_column_predicates_accept_jsonb_expressions,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "SELECT row_pk FROM lix_change WHERE row_pk = CAST('[\"state-latest\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("JSON column compared to lix_json expression should succeed");
    }
);

simulation_test!(
    typed_json_property_predicates_reject_bare_text_literals,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_json_predicate_schema\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"meta\",\"type\":\"jsonb\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                 false,\
                 false\
                 )",
                &[],
            )
            .await
            .expect("schema insert should succeed");

        session
            .execute(
                "INSERT INTO engine_json_predicate_schema (id, meta, lixcol_untracked) \
                 VALUES ('json-predicate-1', CAST('{\"flag\":true}' AS JSONB), false)",
                &[],
            )
            .await
            .expect("typed row insert should succeed");

        let error = session
            .execute(
                "SELECT id FROM engine_json_predicate_schema WHERE meta = '{\"flag\":true}'",
                &[],
            )
            .await
            .expect_err("typed JSON property compared to text should fail loudly");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);

        let result = session
            .execute(
                "SELECT id FROM engine_json_predicate_schema WHERE meta = CAST('{\"flag\":true}' AS JSONB)",
                &[],
            )
            .await
            .expect("typed JSON property compared to lix_json should succeed");

        assert_rows_eq(
            result,
            vec![vec![Value::Text("json-predicate-1".to_string())]],
        );
    }
);
