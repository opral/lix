use super::assert_rows_eq;
use lix::{ResultColumnType, Value};

simulation_test!(
    logical_conditional_types_survive_projection_and_returning,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session.execute("INSERT INTO lix_key_value(key,value) VALUES ('typed', '{\"a\":1}'::jsonb), ('missing', NULL), ('json-null', 'null'::jsonb)", &[]).await.unwrap();
        for expression in [
            "CASE WHEN key = 'typed' THEN value ELSE '{}'::jsonb END",
            "COALESCE(value, '{}'::jsonb)",
            "CASE key WHEN 'typed' THEN value ELSE NULL END",
        ] {
            for statement in [
                format!("SELECT {expression} AS v FROM lix_key_value WHERE key = 'typed'"),
                format!(
                    "UPDATE lix_key_value SET value = value WHERE key = 'typed' RETURNING {expression} AS v"
                ),
            ] {
                let rows = session.execute(&statement, &[]).await.unwrap();
                assert_eq!(
                    rows.column_types(),
                    &[ResultColumnType::Jsonb],
                    "{statement}"
                );
                assert_rows_eq(
                    rows,
                    vec![vec![Value::Jsonb(serde_json::json!({"a":1}).into())]],
                );
            }
        }
        let rows = session.execute("SELECT key FROM lix_key_value WHERE COALESCE(value, '{}'::jsonb) = '{\"a\":1}'::jsonb", &[]).await.unwrap();
        assert_rows_eq(rows, vec![vec![Value::Text("typed".into())]]);
        let rows = session.execute("SELECT COALESCE(value, '{}'::jsonb) AS v FROM lix_key_value WHERE key IN ('json-null', 'missing') ORDER BY key", &[]).await.unwrap();
        assert_rows_eq(
            rows,
            vec![
                vec![Value::Jsonb(serde_json::Value::Null.into())],
                vec![Value::Jsonb(serde_json::json!({}).into())],
            ],
        );
        for key in ["key", "'typed'"] {
            let expression = format!(
                "COALESCE(lix_row_ref('lix_key_value', NULL, {key}), lix_row_ref('lix_key_value', NULL, 'typed'))"
            );
            for statement in [
                format!("SELECT {expression} AS v FROM lix_key_value WHERE key = 'typed'"),
                format!(
                    "UPDATE lix_key_value SET value = value WHERE key = 'typed' RETURNING {expression} AS v"
                ),
            ] {
                let rows = session.execute(&statement, &[]).await.unwrap();
                assert_eq!(
                    rows.column_types(),
                    &[ResultColumnType::RowRef],
                    "{statement}"
                );
                assert!(matches!(&rows.rows()[0].values()[0], Value::RowRef(_)));
            }
        }
        for expression in [
            "CAST(COALESCE(value, '{}'::jsonb) AS TEXT)",
            "CASE WHEN key = 'typed' THEN value ELSE 'not-json' END",
            "COALESCE(value, 'not-json')",
        ] {
            let rows = session
                .execute(
                    &format!("SELECT {expression} AS v FROM lix_key_value WHERE key = 'typed'"),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(
                rows.column_types(),
                &[ResultColumnType::Text],
                "{expression}"
            );
            assert_rows_eq(rows, vec![vec![Value::Text("{\"a\":1}".into())]]);
        }

        let nested_case = session
            .execute(
                "SELECT q.v FROM (SELECT CASE WHEN key = 'typed' THEN value ELSE NULL END AS v FROM lix_key_value WHERE key = 'typed') q",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(nested_case.column_types(), &[ResultColumnType::Jsonb]);
        assert_rows_eq(
            nested_case,
            vec![vec![Value::Jsonb(serde_json::json!({"a":1}).into())]],
        );

        let joined_case = session
            .execute(
                "SELECT q.v FROM (SELECT key, CASE WHEN key = 'typed' THEN value ELSE NULL END AS v FROM lix_key_value WHERE key = 'typed') q JOIN lix_key_value r ON q.key = r.key",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(joined_case.column_types(), &[ResultColumnType::Jsonb]);
        assert_rows_eq(
            joined_case,
            vec![vec![Value::Jsonb(serde_json::json!({"a":1}).into())]],
        );

        session
            .execute(
                "INSERT INTO lix_key_value(key,value) VALUES ('number', '1'::jsonb)",
                &[],
            )
            .await
            .unwrap();
        let cast_value = session
            .execute(
                "SELECT CAST(value AS BIGINT) AS v FROM lix_key_value WHERE key = 'number'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(cast_value.column_types(), &[ResultColumnType::Integer]);
        assert_rows_eq(cast_value, vec![vec![Value::Integer(1)]]);

        let json_param = Value::Jsonb(serde_json::json!({}).into());
        let coalesce_param = session
            .execute(
                "SELECT key FROM lix_key_value WHERE COALESCE(value, $1) = '{}'::jsonb",
                std::slice::from_ref(&json_param),
            )
            .await
            .unwrap();
        assert_rows_eq(coalesce_param, vec![vec![Value::Text("missing".into())]]);
        let coalesce_null = session
            .execute(
                "SELECT key FROM lix_key_value WHERE COALESCE(value, $1) = '{}'::jsonb",
                &[Value::Null],
            )
            .await
            .unwrap();
        assert_rows_eq(coalesce_null, Vec::new());
        session
            .execute(
                "SELECT key FROM lix_key_value WHERE COALESCE(value, $1) = '{}'::jsonb",
                &[Value::Text("{}".into())],
            )
            .await
            .expect_err("bare text must not satisfy a JSONB coalesce predicate");

        let case_param = session
            .execute(
                "SELECT key FROM lix_key_value WHERE key = 'typed' AND (CASE WHEN $1 THEN $2 ELSE NULL END) = '{}'::jsonb",
                &[Value::Boolean(true), json_param.clone()],
            )
            .await
            .unwrap();
        assert_rows_eq(case_param, vec![vec![Value::Text("typed".into())]]);
        session
            .execute(
                "SELECT key FROM lix_key_value WHERE key = 'typed' AND (CASE WHEN $1 THEN $2 ELSE NULL END) = '{}'::jsonb",
                &[Value::Boolean(true), Value::Text("{}".into())],
            )
            .await
            .expect_err("bare text CASE branch must not satisfy a JSONB predicate");

        let json_param = Value::Jsonb(serde_json::json!({"a": 1}).into());
        let coalesce_subquery_sql = "SELECT key FROM lix_key_value \
            WHERE key = 'typed' \
              AND COALESCE($1, NULL) IN (\
                  SELECT value FROM lix_key_value WHERE key = 'typed'\
              )";
        let coalesce_subquery = session
            .execute(coalesce_subquery_sql, std::slice::from_ref(&json_param))
            .await
            .unwrap();
        assert_rows_eq(coalesce_subquery, vec![vec![Value::Text("typed".into())]]);
        session
            .execute(coalesce_subquery_sql, &[Value::Text("{\"a\":1}".into())])
            .await
            .expect_err("bare text COALESCE must not satisfy a JSONB subquery predicate");

        let case_subquery_sql = "SELECT key FROM lix_key_value \
            WHERE key = 'typed' \
              AND (CASE WHEN $1 THEN $2 ELSE NULL END) IN (\
                  SELECT value FROM lix_key_value WHERE key = 'typed'\
              )";
        let case_subquery = session
            .execute(
                case_subquery_sql,
                &[Value::Boolean(true), json_param.clone()],
            )
            .await
            .unwrap();
        assert_rows_eq(case_subquery, vec![vec![Value::Text("typed".into())]]);
        session
            .execute(
                case_subquery_sql,
                &[Value::Boolean(true), Value::Text("{\"a\":1}".into())],
            )
            .await
            .expect_err("bare text CASE branch must not satisfy a JSONB subquery predicate");

        let between_sql = "SELECT key FROM lix_key_value \
            WHERE key = 'typed' \
              AND $1 BETWEEN '{\"a\":0}'::jsonb AND '{\"z\":9}'::jsonb";
        let between = session
            .execute(between_sql, std::slice::from_ref(&json_param))
            .await
            .unwrap();
        assert_rows_eq(between, vec![vec![Value::Text("typed".into())]]);
        session
            .execute(between_sql, &[Value::Text("{\"a\":1}".into())])
            .await
            .expect_err("bare text BETWEEN operand must not satisfy JSONB bounds");

        let simple_case_sql = "SELECT key FROM lix_key_value \
            WHERE key = 'typed' \
              AND CASE value WHEN $1 THEN true ELSE false END";
        let simple_case = session
            .execute(simple_case_sql, std::slice::from_ref(&json_param))
            .await
            .unwrap();
        assert_rows_eq(simple_case, vec![vec![Value::Text("typed".into())]]);
        session
            .execute(simple_case_sql, &[Value::Text("{\"a\":1}".into())])
            .await
            .expect_err("bare text simple CASE operand must not satisfy JSONB comparison");
    }
);

simulation_test!(
    logical_union_types_ignore_sql_null_and_input_order,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        for (value, kind) in [
            ("42", ResultColumnType::Integer),
            ("'plain'", ResultColumnType::Text),
            ("'{\"a\":1}'::jsonb", ResultColumnType::Jsonb),
            (
                "lix_row_ref('lix_key_value', NULL, 'typed')",
                ResultColumnType::RowRef,
            ),
        ] {
            for empty in [false, true] {
                let filter = if empty { " WHERE false" } else { "" };
                for sql in [
                    format!("SELECT NULL AS v{filter} UNION ALL SELECT {value}{filter}"),
                    format!("SELECT {value} AS v{filter} UNION ALL SELECT NULL{filter}"),
                ] {
                    for statement in [sql.clone(), format!("SELECT v FROM ({sql}) q")] {
                        let rows = session.execute(&statement, &[]).await.unwrap();
                        assert_eq!(rows.column_types(), &[kind], "{statement}");
                        assert_eq!(rows.len(), if empty { 0 } else { 2 });
                        for row in rows.rows() {
                            let value = &row.values()[0];
                            assert!(
                                matches!(
                                    (kind, value),
                                    (_, Value::Null)
                                        | (ResultColumnType::Integer, Value::Integer(42))
                                        | (ResultColumnType::Text, Value::Text(_))
                                        | (ResultColumnType::Jsonb, Value::Jsonb(_))
                                        | (ResultColumnType::RowRef, Value::RowRef(_))
                                ),
                                "{statement}: {value:?}"
                            );
                        }
                    }
                }
            }
        }
        let mixed = session
            .execute(
                "SELECT '{\"a\":1}'::jsonb AS v UNION ALL SELECT 'not-json'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(mixed.column_types(), &[ResultColumnType::Text]);
        assert_rows_eq(
            mixed,
            vec![
                vec![Value::Text("{\"a\":1}".into())],
                vec![Value::Text("not-json".into())],
            ],
        );
    }
);
