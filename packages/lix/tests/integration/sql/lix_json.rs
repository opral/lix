use lix::{LixError, Value};
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    schema_v1_seven_types_have_a_runnable_schema_surface,
    |sim| async move {
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
        session
            .execute(
                "INSERT INTO seven_type_probe (label, count, ratio, active, metadata) \
         VALUES ('ready', 42, 1.5, true, '{\"answer\":42}'::jsonb)",
                &[],
            )
            .await
            .unwrap();

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
    }
);

simulation_test!(
    timestamptz_is_native_and_current_timestamp_is_stable,
    |sim| async move {
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
        session
            .execute("INSERT INTO timestamp_probe (id) VALUES (1)", &[])
            .await
            .unwrap();

        let row = session
            .execute(
                "SELECT created_at, CURRENT_TIMESTAMP AS first, CURRENT_TIMESTAMP AS second, \
                        now() AS now_alias \
             FROM timestamp_probe WHERE id = 1",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(row.rows()[0].values()[0], Value::Timestamptz(_)));
        assert!(matches!(row.rows()[0].values()[1], Value::Timestamptz(_)));
        assert_eq!(row.rows()[0].values()[1], row.rows()[0].values()[2]);
        assert_eq!(row.rows()[0].values()[1], row.rows()[0].values()[3]);
    }
);

simulation_test!(text_primary_keys_reject_jsonb_nul, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "nul_identity_probe",
        "columns": [{"name": "id", "type": "text", "nullable": false}],
        "primary_key": ["id"]
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, CAST($2 AS JSONB))",
            &[
                Value::Text("nul_identity_probe".into()),
                Value::Text(schema.to_string()),
            ],
        )
        .await
        .unwrap();
    let error = session
        .execute(
            "INSERT INTO nul_identity_probe (id) VALUES ($1)",
            &[Value::Text("a\0b".into())],
        )
        .await
        .expect_err("NUL cannot be represented by JSONB identity");
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

simulation_test!(
    postgres_jsonb_path_operator_uses_text_array_path,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for (statement, expected) in [
            (
                "SELECT '{\"user\":{\"names\":[\"Ada\"]}}'::jsonb #>> '{user,names,0}' AS name",
                Value::Text("Ada".to_string()),
            ),
            (
                "SELECT '{\"user\":{\"names\":[\"Ada\"]}}'::jsonb #> ARRAY['user','names','0'] AS name",
                Value::Jsonb(json!("Ada").into()),
            ),
            (
                "SELECT doc #> path AS name FROM (VALUES ('{\"user\":{\"names\":[\"Ada\"]}}'::jsonb, ARRAY['user','names','0'])) t(doc, path)",
                Value::Jsonb(json!("Ada").into()),
            ),
        ] {
            let result = session
                .execute(statement, &[])
                .await
                .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
            assert_rows_eq(result, vec![vec![expected]]);
        }
    }
);

simulation_test!(
    postgres_jsonb_operator_rejects_wrong_operand_types,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        for statement in [
            "SELECT '[\"1\"]'::jsonb ? 1",
            "SELECT '{}'::jsonb ? true",
            "SELECT '{}'::jsonb ? '{}'::jsonb",
            "SELECT '{}'::jsonb @> '{\"a\":1}'::TEXT",
            "SELECT '{\"a\":1}'::jsonb -> '\"a\"'::jsonb",
            "SELECT '{}'::jsonb #> ARRAY['a'::jsonb]",
        ] {
            let error = session
                .execute(statement, &[])
                .await
                .expect_err("JSONB operators must enforce their PostgreSQL operand types");
            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH, "{statement}: {error:?}");
        }
    }
);

simulation_test!(
    postgres_jsonb_operator_infers_text_parameter_operands,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        assert_rows_eq(
            session
                .execute(
                    "SELECT '{\"a\":1}'::jsonb ? $1",
                    &[Value::Text("a".into())],
                )
                .await
                .expect("JSONB existence operator should accept a text parameter"),
            vec![vec![Value::Boolean(true)]],
        );

        assert_rows_eq(
            session
                .execute(
                    "SELECT '{\"a\":1}'::jsonb -> CAST('a' AS VARCHAR)",
                    &[],
                )
                .await
                .expect("JSONB path operators should consume DataFusion Utf8View values"),
            vec![vec![Value::Jsonb(json!(1).into())]],
        );

        let error = session
            .execute(
                "SELECT '{\"a\":1}'::jsonb ? $1",
                &[Value::Integer(1)],
            )
            .await
            .expect_err("JSONB existence operator should reject a non-text parameter");
        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
    }
);

simulation_test!(
    postgres_jsonb_path_operator_parses_quoted_array_elements,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        for (statement, expected) in [
            (
                "SELECT '{\"a,b\":42}'::jsonb #> '{\"a,b\"}'",
                Value::Jsonb(json!(42).into()),
            ),
            (
                "SELECT '{\"a,b\":42}'::jsonb #>> '{a\\,b}'",
                Value::Text("42".into()),
            ),
            (
                "SELECT '{\"a}b\":42}'::jsonb #>> '{\"a}b\"}'",
                Value::Text("42".into()),
            ),
            (
                "SELECT '{\"\":42}'::jsonb #>> '{\"\"}'",
                Value::Text("42".into()),
            ),
            (
                "SELECT '{\"a\\\"b\":42}'::jsonb #>> '{\"a\\\"b\"}'",
                Value::Text("42".into()),
            ),
            (
                "SELECT '{\"a\\\\b\":42}'::jsonb #>> '{\"a\\\\b\"}'",
                Value::Text("42".into()),
            ),
            (
                "SELECT '{\"NULL\":9}'::jsonb #> '{NULL}'",
                Value::Null,
            ),
            (
                "SELECT '{\"NULL\":9}'::jsonb #>> '{\"NULL\"}'",
                Value::Text("9".into()),
            ),
        ] {
            let result = session.execute(statement, &[]).await.unwrap();
            assert_rows_eq(result, vec![vec![expected]]);
        }

        for statement in [
            "SELECT '{\"a,b\":42}'::jsonb #> '{\"a,b}'",
            "SELECT '{\"a,b\":42}'::jsonb #> '{\"a\"junk}'",
        ] {
            session
                .execute(statement, &[])
                .await
                .expect_err("malformed PostgreSQL array path syntax must be rejected");
        }
    }
);

simulation_test!(
    jsonb_array_containment_preserves_nesting_levels,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let result = session
            .execute(
                "SELECT \
                    '[1,2,[1,3]]'::jsonb @> '[1,3]'::jsonb, \
                    '[[1,2]]'::jsonb @> '[1,2]'::jsonb, \
                    '[[1,2]]'::jsonb @> '1'::jsonb, \
                    '[1,2]'::jsonb @> '1'::jsonb, \
                    '[1,2,[1,3]]'::jsonb @> '[[1,3]]'::jsonb",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(
            result,
            vec![vec![
                Value::Boolean(false),
                Value::Boolean(false),
                Value::Boolean(false),
                Value::Boolean(true),
                Value::Boolean(true),
            ]],
        );
    }
);

simulation_test!(
    jsonb_numeric_equality_preserves_decimal_precision,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let result = session
            .execute(
                "SELECT \
                    '1.0000000000000000000000001'::jsonb = '1'::jsonb, \
                    '1e-1000'::jsonb = '0'::jsonb, \
                    '9007199254740993.0'::jsonb = '9007199254740993'::jsonb, \
                    '1.0'::jsonb = ANY(ARRAY['1'::jsonb]), \
                    '{\"n\":1.0}'::jsonb = '{\"n\":1}'::jsonb",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(
            result,
            vec![vec![
                Value::Boolean(false),
                Value::Boolean(false),
                Value::Boolean(true),
                Value::Boolean(true),
                Value::Boolean(true),
            ]],
        );

        let large_number = session
            .execute("SELECT '9007199254740993.0'::jsonb", &[])
            .await
            .unwrap();
        assert_rows_eq(
            large_number,
            vec![vec![Value::Jsonb(json!(9_007_199_254_740_993_u64).into())]],
        );

        let set_operations = session
            .execute(
                "SELECT \
                    (SELECT COUNT(*) FROM (SELECT '1'::jsonb AS v UNION SELECT '1.0'::jsonb) u) AS union_count, \
                    (SELECT COUNT(*) FROM (SELECT '1'::jsonb AS v INTERSECT SELECT '1.0'::jsonb) i) AS intersect_count, \
                    (SELECT COUNT(*) FROM (SELECT '1'::jsonb AS v EXCEPT SELECT '1.0'::jsonb) e) AS except_count",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(set_operations, vec![vec![Value::Integer(1), Value::Integer(1), Value::Integer(0)]]);
    }
);

simulation_test!(
    postgres_jsonb_key_operator_treats_jsonpath_as_a_literal_key,
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
            .execute("SELECT '{\"path\":\"ok\"}'::jsonb ->> '$.path'", &[])
            .await
            .expect("PostgreSQL key operands are literal keys");
        assert_rows_eq(result, vec![vec![Value::Null]]);
    }
);

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
                "SELECT snapshot_content FROM lix_change WHERE snapshot_content = 'state-latest'",
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
                "SELECT snapshot_content FROM lix_change \
                 WHERE snapshot_content = CAST('[\"state-latest\"]' AS JSONB)",
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

simulation_test!(
    json_null_remains_distinct_from_sql_null_across_typed_writes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "json_null_write_contract",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "nullable_value", "type": "jsonb", "nullable": true},
                {"name": "required_value", "type": "jsonb", "nullable": false}
            ],
            "primary_key": ["id"]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .expect("schema insert should succeed");

        let json_null = Value::Jsonb(json!(null).into());
        session
            .execute(
                "INSERT INTO json_null_write_contract (id, nullable_value, required_value) \
                 VALUES ($1, $2, $3)",
                &[
                    Value::Text("json-null".into()),
                    json_null.clone(),
                    json_null.clone(),
                ],
            )
            .await
            .expect("JSON null should satisfy both nullable and nonnullable JSONB columns");
        session
            .execute(
                "INSERT INTO json_null_write_contract (id, nullable_value, required_value) \
                 VALUES ($1, $2, $3)",
                &[
                    Value::Text("sql-null".into()),
                    Value::Null,
                    Value::Jsonb(json!({"present": true}).into()),
                ],
            )
            .await
            .expect("SQL NULL should be accepted by the nullable JSONB column");

        assert_rows_eq(
            session
                .execute(
                    "SELECT id, nullable_value, required_value \
                     FROM json_null_write_contract ORDER BY id",
                    &[],
                )
                .await
                .expect("initial JSON values should read"),
            vec![
                vec![
                    Value::Text("json-null".into()),
                    json_null.clone(),
                    json_null.clone(),
                ],
                vec![
                    Value::Text("sql-null".into()),
                    Value::Null,
                    Value::Jsonb(json!({"present": true}).into()),
                ],
            ],
        );

        session
            .execute(
                "UPDATE json_null_write_contract SET nullable_value = $1 WHERE id = 'sql-null'",
                &[json_null.clone()],
            )
            .await
            .expect("UPDATE should write JSON null into a nullable JSONB column");
        session
            .execute(
                "UPDATE json_null_write_contract \
                 SET nullable_value = COALESCE(nullable_value, CAST('null' AS JSONB)) \
                 WHERE id = 'json-null'",
                &[],
            )
            .await
            .expect("DataFusion UPDATE should preserve JSON null through COALESCE");
        session
            .execute(
                "UPDATE json_null_write_contract SET nullable_value = $1 WHERE id = 'json-null'",
                &[Value::Null],
            )
            .await
            .expect("UPDATE should write SQL NULL into a nullable JSONB column");

        let returned = session
            .execute(
                "UPDATE json_null_write_contract SET nullable_value = nullable_value \
                 WHERE id = 'json-null' \
                 RETURNING $1 AS sql_null, $2 AS json_null, \
                           CAST($2 AS JSONB) AS cast_json_null, $3 AS timestamp",
                &[
                    Value::Null,
                    json_null.clone(),
                    Value::Timestamptz(1_700_000_000_123_456),
                ],
            )
            .await
            .expect("RETURNING parameters should retain their native types");
        assert_rows_eq(
            returned,
            vec![vec![
                Value::Null,
                json_null.clone(),
                json_null.clone(),
                Value::Timestamptz(1_700_000_000_123_456),
            ]],
        );

        session
            .execute(
                "UPDATE json_null_write_contract SET required_value = $1 WHERE id = 'sql-null'",
                &[json_null.clone()],
            )
            .await
            .expect("UPDATE should write JSON null into a nonnullable JSONB column");

        let error = session
            .execute(
                "UPDATE json_null_write_contract SET required_value = $1 WHERE id = 'json-null'",
                &[Value::Null],
            )
            .await
            .expect_err("SQL NULL must remain invalid for a nonnullable JSONB column");
        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);

        let reopened_engine = sim
            .reboot_engine_from_current_snapshot()
            .await
            .expect("reopened engine should load the committed snapshot");
        let reopened = sim.wrap_session(
            reopened_engine
                .open_session()
                .await
                .expect("reopened session should open"),
            &reopened_engine,
        );
        assert_rows_eq(
            reopened
                .execute(
                    "SELECT id, nullable_value, required_value \
                     FROM json_null_write_contract ORDER BY id",
                    &[],
                )
                .await
                .expect("updated JSON values should read"),
            vec![
                vec![
                    Value::Text("json-null".into()),
                    Value::Null,
                    json_null.clone(),
                ],
                vec![
                    Value::Text("sql-null".into()),
                    json_null,
                    Value::Jsonb(json!(null).into()),
                ],
            ],
        );
    }
);

simulation_test!(
    json_null_equality_in_and_null_predicates_distinguish_sql_null,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "json_null_predicate_contract",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "payload", "type": "jsonb", "nullable": true}
            ],
            "primary_key": ["id"]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .expect("schema insert should succeed");
        let json_null = Value::Jsonb(json!(null).into());
        for (id, payload) in [
            ("json-null", json_null.clone()),
            ("sql-null", Value::Null),
            ("object", Value::Jsonb(json!({}).into())),
        ] {
            session
                .execute(
                    "INSERT INTO json_null_predicate_contract (id, payload) VALUES ($1, $2)",
                    &[Value::Text(id.into()), payload],
                )
                .await
                .expect("predicate fixture insert should succeed");
        }

        assert_rows_eq(
            session
                .execute(
                    "SELECT id, \
                            CAST(payload AS JSONB) = CAST('null' AS JSONB), \
                            CAST(payload AS JSONB) IN (CAST('null' AS JSONB), CAST('{}' AS JSONB)), \
                            payload IS NULL, \
                            payload IS NOT NULL \
                     FROM json_null_predicate_contract ORDER BY id",
                    &[],
                )
                .await
                .expect("JSON null predicates should read"),
            vec![
                vec![
                    Value::Text("json-null".into()),
                    Value::Boolean(true),
                    Value::Boolean(true),
                    Value::Boolean(false),
                    Value::Boolean(true),
                ],
                vec![
                    Value::Text("object".into()),
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::Boolean(false),
                    Value::Boolean(true),
                ],
                vec![
                    Value::Text("sql-null".into()),
                    Value::Null,
                    Value::Null,
                    Value::Boolean(true),
                    Value::Boolean(false),
                ],
            ],
        );

        assert_eq!(
            session
                .execute(
                    "UPDATE json_null_predicate_contract SET payload = payload \
                     WHERE payload = CAST($1 AS JSONB)",
                    &[json_null.clone()],
                )
                .await
                .expect("JSON equality should accept a parameter cast to JSONB")
                .rows_affected(),
            1
        );

        assert_eq!(
            session
                .execute(
                    "UPDATE json_null_predicate_contract SET payload = payload \
                     WHERE payload = CAST('null' AS JSONB)",
                    &[],
                )
                .await
                .expect("equality UPDATE should succeed")
                .rows_affected(),
            1
        );
        assert_eq!(
            session
                .execute(
                    "UPDATE json_null_predicate_contract SET payload = payload \
                     WHERE payload IN (CAST('null' AS JSONB), CAST('{}' AS JSONB))",
                    &[],
                )
                .await
                .expect("IN UPDATE should succeed")
                .rows_affected(),
            2
        );
        assert_eq!(
            session
                .execute(
                    "UPDATE json_null_predicate_contract SET payload = payload \
                     WHERE payload IS NULL",
                    &[],
                )
                .await
                .expect("IS NULL UPDATE should succeed")
                .rows_affected(),
            1
        );
        assert_eq!(
            session
                .execute(
                    "UPDATE json_null_predicate_contract SET payload = payload \
                     WHERE payload IS NOT NULL",
                    &[],
                )
                .await
                .expect("IS NOT NULL UPDATE should succeed")
                .rows_affected(),
            2
        );

        assert_eq!(
            session
                .execute(
                    "DELETE FROM json_null_predicate_contract \
                     WHERE payload = CAST('null' AS JSONB)",
                    &[],
                )
                .await
                .expect("equality DELETE should succeed")
                .rows_affected(),
            1
        );
        assert_eq!(
            session
                .execute(
                    "DELETE FROM json_null_predicate_contract \
                     WHERE payload IN (CAST('null' AS JSONB), CAST('{}' AS JSONB))",
                    &[],
                )
                .await
                .expect("IN DELETE should succeed")
                .rows_affected(),
            1
        );
        assert_eq!(
            session
                .execute(
                    "DELETE FROM json_null_predicate_contract WHERE payload IS NULL",
                    &[],
                )
                .await
                .expect("IS NULL DELETE should succeed")
                .rows_affected(),
            1
        );
        assert_eq!(
            session
                .execute(
                    "DELETE FROM json_null_predicate_contract WHERE payload IS NOT NULL",
                    &[],
                )
                .await
                .expect("IS NOT NULL DELETE should succeed")
                .rows_affected(),
            0
        );
    }
);

simulation_test!(
    jsonb_arrow_path_preserves_json_null_value,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "json_null_path_contract",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "payload", "type": "jsonb", "nullable": true}
            ],
            "primary_key": ["id"]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .expect("schema insert should succeed");
        session
            .execute(
                "INSERT INTO json_null_path_contract (id, payload) VALUES ($1, $2), ($3, $4), ($5, $6)",
                &[
                    Value::Text("child-json-null".into()),
                    Value::Jsonb(json!({"child": null}).into()),
                    Value::Text("root-json-null".into()),
                    Value::Jsonb(json!(null).into()),
                    Value::Text("sql-null".into()),
                    Value::Null,
                ],
            )
            .await
            .expect("path fixture insert should succeed");

        assert_rows_eq(
            session
                .execute(
                    "SELECT id, payload -> 'child' AS child, payload -> 'missing' AS missing \
                     FROM json_null_path_contract ORDER BY id",
                    &[],
                )
                .await
                .expect("JSON path extraction should succeed"),
            vec![
                vec![
                    Value::Text("child-json-null".into()),
                    Value::Jsonb(json!(null).into()),
                    Value::Null,
                ],
                vec![
                    Value::Text("root-json-null".into()),
                    Value::Null,
                    Value::Null,
                ],
                vec![Value::Text("sql-null".into()), Value::Null, Value::Null],
            ],
        );
    }
);

simulation_test!(
    jsonb_string_null_parameter_does_not_match_json_null,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "json_string_null_parameter_contract",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "payload", "type": "jsonb", "nullable": true}
            ],
            "primary_key": ["id"]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .expect("schema insert should succeed");
        session
            .execute(
                "INSERT INTO json_string_null_parameter_contract (id, payload) \
                 VALUES ($1, $2), ($3, $4), ($5, $6)",
                &[
                    Value::Text("json-null".into()),
                    Value::Jsonb(json!(null).into()),
                    Value::Text("json-string-null".into()),
                    Value::Jsonb(json!("null").into()),
                    Value::Text("object".into()),
                    Value::Jsonb(json!({}).into()),
                ],
            )
            .await
            .expect("predicate fixtures should insert");

        assert_rows_eq(
            session
                .execute(
                    "UPDATE json_string_null_parameter_contract SET payload = payload \
                     WHERE payload = $1 RETURNING id",
                    &[Value::Jsonb(json!("null").into())],
                )
                .await
                .expect("JSONB string equality should succeed"),
            vec![vec![Value::Text("json-string-null".into())]],
        );
        assert_rows_eq(
            session
                .execute(
                    "UPDATE json_string_null_parameter_contract SET payload = payload \
                     WHERE payload IN ($1) RETURNING id",
                    &[Value::Jsonb(json!("null").into())],
                )
                .await
                .expect("JSONB string IN predicate should succeed"),
            vec![vec![Value::Text("json-string-null".into())]],
        );
    }
);
