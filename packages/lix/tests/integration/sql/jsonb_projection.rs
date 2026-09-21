use lix::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    jsonb_projection_predicates_resolve_input_column_types,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "jsonb_projection_typecheck_contract",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "payload", "type": "jsonb", "nullable": false}
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

        for (id, payload) in [
            ("json-null", Value::Jsonb(json!(null).into())),
            ("object", Value::Jsonb(json!({}).into())),
            ("array", Value::Jsonb(json!([]).into())),
        ] {
            session
                .execute(
                    "INSERT INTO jsonb_projection_typecheck_contract (id, payload) \
                     VALUES ($1, $2)",
                    &[Value::Text(id.into()), payload],
                )
                .await
                .expect("projection fixture insert should succeed");
        }

        assert_rows_eq(
            session
                .execute(
                    "SELECT id, \
                            payload = CAST('null' AS JSONB), \
                            payload IN (CAST('null' AS JSONB), CAST('{}' AS JSONB)), \
                            (payload = CAST('null' AS JSONB)) OR \
                                payload IN (CAST('{}' AS JSONB)), \
                            CASE WHEN payload = CAST('null' AS JSONB) \
                                 THEN 'match' ELSE 'other' END \
                     FROM jsonb_projection_typecheck_contract ORDER BY id",
                    &[],
                )
                .await
                .expect("JSONB predicates in projections should resolve input types"),
            vec![
                vec![
                    Value::Text("array".into()),
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::Text("other".into()),
                ],
                vec![
                    Value::Text("json-null".into()),
                    Value::Boolean(true),
                    Value::Boolean(true),
                    Value::Boolean(true),
                    Value::Text("match".into()),
                ],
                vec![
                    Value::Text("object".into()),
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::Boolean(true),
                    Value::Text("other".into()),
                ],
            ],
        );

        assert_rows_eq(
            session
                .execute(
                    "SELECT id \
                     FROM jsonb_projection_typecheck_contract \
                     WHERE payload = CAST('null' AS JSONB)",
                    &[],
                )
                .await
                .expect("the same JSONB predicate in WHERE should succeed"),
            vec![vec![Value::Text("json-null".into())]],
        );

        let error = session
            .execute(
                "SELECT payload = $1 \
                 FROM jsonb_projection_typecheck_contract",
                &[Value::Text("null".into())],
            )
            .await
            .expect_err("bare text parameters must remain invalid in projections");
        assert_eq!(error.code, lix::LixError::CODE_TYPE_MISMATCH);

        let nested_error = session
            .execute(
                "SELECT payload IN ( \
                     SELECT payload FROM jsonb_projection_typecheck_contract \
                     WHERE payload = $1 \
                 ) FROM jsonb_projection_typecheck_contract",
                &[Value::Text("null".into())],
            )
            .await
            .expect_err("bare text parameters must remain invalid in nested predicates");
        assert_eq!(nested_error.code, lix::LixError::CODE_TYPE_MISMATCH);

        let nested_type_error = session
            .execute(
                "SELECT payload IN ( \
                     SELECT id FROM jsonb_projection_typecheck_contract \
                 ) FROM jsonb_projection_typecheck_contract",
                &[],
            )
            .await
            .expect_err("JSONB must not compare with a text subquery result");
        assert_eq!(nested_type_error.code, lix::LixError::CODE_TYPE_MISMATCH);

        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM jsonb_projection_typecheck_contract WHERE $1 IN ( \
                         SELECT payload FROM jsonb_projection_typecheck_contract \
                     ) ORDER BY id",
                    &[Value::Jsonb(json!(null).into())],
                )
                .await
                .expect("JSONB parameters should type-check against subquery results"),
            vec![
                vec![Value::Text("array".into())],
                vec![Value::Text("json-null".into())],
                vec![Value::Text("object".into())],
            ],
        );

        let parameter_subquery_error = session
            .execute(
                "SELECT id FROM jsonb_projection_typecheck_contract WHERE $1 IN ( \
                     SELECT payload FROM jsonb_projection_typecheck_contract \
                 )",
                &[Value::Text("null".into())],
            )
            .await
            .expect_err("text parameters must remain invalid against JSONB subqueries");
        assert_eq!(
            parameter_subquery_error.code,
            lix::LixError::CODE_TYPE_MISMATCH
        );
    }
);

simulation_test!(
    jsonb_numeric_parameter_matches_integral_decimal_across_paths,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "jsonb_numeric_comparison_contract",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "payload", "type": "jsonb", "nullable": false}
            ],
            "primary_key": ["id"]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .expect("numeric comparison schema should register");
        session
            .execute(
                "INSERT INTO jsonb_numeric_comparison_contract (id, payload) \
                 VALUES ('one', $1)",
                &[Value::Jsonb(json!(1).into())],
            )
            .await
            .expect("numeric JSONB fixture should insert");

        let decimal = Value::Jsonb(json!(1.0).into());
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM jsonb_numeric_comparison_contract \
                     WHERE payload = $1",
                    std::slice::from_ref(&decimal),
                )
                .await
                .expect("bare JSONB parameters should use canonical numeric equality"),
            vec![vec![Value::Text("one".into())]],
        );

        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM jsonb_numeric_comparison_contract \
                     WHERE payload = CAST($1 AS JSONB)",
                    std::slice::from_ref(&decimal),
                )
                .await
                .expect("JSONB cast should normalize integral decimal parameters"),
            vec![vec![Value::Text("one".into())]],
        );

        assert_rows_eq(
            session
                .execute(
                    "UPDATE jsonb_numeric_comparison_contract SET payload = payload \
                     WHERE payload = $1 AND id LIKE '%' RETURNING id",
                    std::slice::from_ref(&decimal),
                )
                .await
                .expect("generic JSONB equality should use canonical numeric parameters"),
            vec![vec![Value::Text("one".into())]],
        );

        assert_rows_eq(
            session
                .execute(
                    "UPDATE jsonb_numeric_comparison_contract SET payload = payload \
                     WHERE payload = $1 RETURNING id",
                    std::slice::from_ref(&decimal),
                )
                .await
                .expect("direct JSONB equality should use JSONB numeric semantics"),
            vec![vec![Value::Text("one".into())]],
        );

        let generic_cast_update = session
            .execute(
                "UPDATE jsonb_numeric_comparison_contract \
                 SET payload = CAST($1 AS JSONB) \
                 WHERE id LIKE 'one' RETURNING payload",
                std::slice::from_ref(&decimal),
            )
            .await
            .expect("generic JSONB UPDATE should normalize integral decimals");
        assert_eq!(
            generic_cast_update.column_types(),
            [lix::ResultColumnType::Jsonb]
        );
        assert_rows_eq(
            generic_cast_update,
            vec![vec![Value::Jsonb(json!(1).into())]],
        );
    }
);
