use lix::{ResultColumnType, Value};
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    case_returning_preserves_exact_bigint_comparisons_and_json_contract,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "case_returning_types",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "n", "type": "int8", "nullable": false},
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
            .expect("CASE type fixture schema should register");
        session
            .execute(
                "INSERT INTO case_returning_types (id, n, payload) \
                 VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9)",
                &[
                    Value::Text("wide".into()),
                    Value::Integer(9_007_199_254_740_993),
                    Value::Jsonb(json!({"a": 1}).into()),
                    Value::Text("neighbor".into()),
                    Value::Integer(9_007_199_254_740_992),
                    Value::Jsonb(json!({"a": 1}).into()),
                    Value::Text("json".into()),
                    Value::Integer(0),
                    Value::Jsonb(json!({"a": 1, "b": 2}).into()),
                ],
            )
            .await
            .expect("CASE type fixture rows should insert");

        for (operator, wide_matches, neighbor_matches) in [
            ("=", true, false),
            ("<>", false, true),
            ("<", false, true),
            ("<=", true, true),
            (">", false, false),
            (">=", true, false),
        ] {
            let sql = format!(
                "UPDATE case_returning_types SET n = n WHERE id = $1 \
                 RETURNING CASE WHEN n {operator} 9007199254740993.0 \
                                THEN 'hit' ELSE 'miss' END"
            );
            let wide = session
                .execute(&sql, &[Value::Text("wide".into())])
                .await
                .expect("wide BIGINT CASE comparison should execute");
            assert_rows_eq(
                wide,
                vec![vec![Value::Text(
                    (if wide_matches { "hit" } else { "miss" }).into(),
                )]],
            );
            let neighbor = session
                .execute(&sql, &[Value::Text("neighbor".into())])
                .await
                .expect("neighbor BIGINT CASE comparison should execute");
            assert_rows_eq(
                neighbor,
                vec![vec![Value::Text(
                    (if neighbor_matches { "hit" } else { "miss" }).into(),
                )]],
            );
        }

        for (expression, wide_matches, neighbor_matches) in [
            (
                "CASE n WHEN 9007199254740993.0 THEN 'hit' ELSE 'miss' END",
                true,
                false,
            ),
            (
                "CASE 9007199254740993.0 WHEN n THEN 'hit' ELSE 'miss' END",
                true,
                false,
            ),
        ] {
            let sql = format!(
                "UPDATE case_returning_types SET n = n WHERE id = $1 RETURNING {expression}"
            );
            let wide = session
                .execute(&sql, &[Value::Text("wide".into())])
                .await
                .expect("simple CASE BIGINT comparison should execute");
            assert_rows_eq(
                wide,
                vec![vec![Value::Text(
                    (if wide_matches { "hit" } else { "miss" }).into(),
                )]],
            );
            let neighbor = session
                .execute(&sql, &[Value::Text("neighbor".into())])
                .await
                .expect("simple CASE BIGINT comparison should execute");
            assert_rows_eq(
                neighbor,
                vec![vec![Value::Text(
                    (if neighbor_matches { "hit" } else { "miss" }).into(),
                )]],
            );
        }

        let images = session
            .execute(
                "UPDATE case_returning_types SET n = n WHERE id = 'wide' \
                 RETURNING CASE WHEN old.n = 9007199254740992.0 \
                                THEN 'old-hit' ELSE 'old-miss' END, \
                           CASE WHEN new.n > 9007199254740992.0 \
                                THEN 'new-hit' ELSE 'new-miss' END",
                &[],
            )
            .await
            .expect("OLD and NEW BIGINT CASE comparisons should execute");
        assert_rows_eq(
            images,
            vec![vec![
                Value::Text("old-miss".into()),
                Value::Text("new-hit".into()),
            ]],
        );

        for sql in [
            "UPDATE case_returning_types SET n = n WHERE id = 'missing' \
             RETURNING CASE WHEN TRUE THEN NULL ELSE payload END",
            "DELETE FROM case_returning_types WHERE id = 'missing' \
             RETURNING CASE WHEN TRUE THEN NULL ELSE payload END",
        ] {
            let empty = session
                .execute(sql, &[])
                .await
                .expect("empty CASE RETURNING should still expose its branch type");
            assert_eq!(empty.column_types(), &[ResultColumnType::Jsonb], "{sql}");
            assert!(empty.rows().is_empty(), "{sql}");
        }

        let typed_json = session
            .execute(
                "UPDATE case_returning_types SET n = n WHERE id = 'json' \
                 RETURNING CASE WHEN payload = $1 THEN 'hit' ELSE 'miss' END",
                &[Value::Jsonb(json!({"b": 2, "a": 1}).into())],
            )
            .await
            .expect("typed JSONB CASE parameter should execute");
        assert_rows_eq(typed_json, vec![vec![Value::Text("hit".into())]]);

        let canonical_json = session
            .execute(
                "UPDATE case_returning_types SET n = n WHERE id = 'json' \
                 RETURNING CASE WHEN payload = CAST('{\"b\":2,\"a\":1}' AS JSONB) \
                                THEN 'hit' ELSE 'miss' END",
                &[],
            )
            .await
            .expect("explicit JSONB CASE literal should execute");
        assert_rows_eq(canonical_json, vec![vec![Value::Text("hit".into())]]);

        for (sql, params) in [
            (
                "UPDATE case_returning_types SET n = n WHERE id = 'json' \
                 RETURNING CASE WHEN payload = '{\"a\":1}' THEN 'hit' ELSE 'miss' END",
                Vec::new(),
            ),
            (
                "UPDATE case_returning_types SET n = n WHERE id = 'json' \
                 RETURNING CASE WHEN payload = $1 THEN 'hit' ELSE 'miss' END",
                vec![Value::Text(r#"{"a":1}"#.into())],
            ),
        ] {
            let error = session
                .execute(sql, &params)
                .await
                .expect_err("bare text must not compare with JSONB in CASE");
            assert_eq!(error.code, lix::LixError::CODE_TYPE_MISMATCH, "{sql}");
        }
    }
);
