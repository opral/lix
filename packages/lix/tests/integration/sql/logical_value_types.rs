use super::assert_rows_eq;
use lix::{ResultColumnType, Value};

simulation_test!(
    datafusion_array_functions_preserve_lix_value_contracts,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        let cardinality = session
            .execute("SELECT cardinality(ARRAY['{}'::jsonb]) AS n", &[])
            .await
            .expect("DataFusion cardinality should inspect an extension array's shape");
        assert_rows_eq(cardinality, vec![vec![Value::Integer(1)]]);

        let json_value = Value::Jsonb(serde_json::json!({}).into());
        for (sql, expected) in [
            (
                "SELECT unnest(array_reverse(ARRAY['{}'::jsonb])) AS v",
                vec![vec![json_value.clone()]],
            ),
            (
                "SELECT unnest(array_remove(ARRAY['{}'::jsonb, '{\"a\":1}'::jsonb], '{}'::jsonb)) AS v",
                vec![vec![Value::Jsonb(serde_json::json!({"a": 1}).into())]],
            ),
            (
                "SELECT unnest(array_append(ARRAY['{}'::jsonb], '{\"a\":1}'::jsonb)) AS v",
                vec![json_value.clone(), Value::Jsonb(serde_json::json!({"a": 1}).into())]
                    .into_iter()
                    .map(|value| vec![value])
                    .collect(),
            ),
        ] {
            let result = session
                .execute(sql, &[])
                .await
                .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
            assert_eq!(result.column_types(), &[ResultColumnType::Jsonb], "{sql}");
            assert_rows_eq(result, expected);
        }

        for (sql, expected) in [
            (
                "SELECT array_position(array_append(ARRAY['{}'::jsonb], '{}'::jsonb), '{}'::jsonb)",
                vec![vec![Value::Integer(1)]],
            ),
            (
                "SELECT array_position(ARRAY['{}'::jsonb, '{}'::jsonb], '{}'::jsonb, 2)",
                vec![vec![Value::Integer(2)]],
            ),
            (
                "SELECT unnest(array_positions(ARRAY['{}'::jsonb, '{}'::jsonb], '{}'::jsonb))",
                vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
            ),
            (
                "SELECT unnest(array_distinct(ARRAY['{}'::jsonb, '{}'::jsonb]))",
                vec![vec![Value::Jsonb(serde_json::json!({}).into())]],
            ),
            (
                "SELECT unnest(array_union(ARRAY['{}'::jsonb], ARRAY['{}'::jsonb, '1'::jsonb]))",
                vec![
                    vec![Value::Jsonb(serde_json::json!({}).into())],
                    vec![Value::Jsonb(serde_json::json!(1).into())],
                ],
            ),
            (
                "SELECT unnest(array_intersect(ARRAY['{}'::jsonb], ARRAY['{}'::jsonb, '1'::jsonb]))",
                vec![vec![Value::Jsonb(serde_json::json!({}).into())]],
            ),
            (
                "SELECT unnest(array_except(ARRAY['{}'::jsonb, '1'::jsonb], ARRAY['1'::jsonb]))",
                vec![vec![Value::Jsonb(serde_json::json!({}).into())]],
            ),
            (
                "SELECT array_has_any(ARRAY['{}'::jsonb], ARRAY['{}'::jsonb])",
                vec![vec![Value::Boolean(true)]],
            ),
            (
                "SELECT array_has_all(ARRAY['{}'::jsonb], ARRAY['{}'::jsonb])",
                vec![vec![Value::Boolean(true)]],
            ),
            (
                "SELECT cardinality(ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')])",
                vec![vec![Value::Integer(1)]],
            ),
            (
                "SELECT array_has_any(ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')], \
                                      ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')])",
                vec![vec![Value::Boolean(true)]],
            ),
        ] {
            let result = session
                .execute(sql, &[])
                .await
                .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
            if sql.contains("unnest(array_distinct")
                || sql.contains("unnest(array_union")
                || sql.contains("unnest(array_intersect")
                || sql.contains("unnest(array_except")
            {
                assert_eq!(result.column_types(), &[ResultColumnType::Jsonb], "{sql}");
            }
            assert_rows_eq(result, expected);
        }

        let json_text = session
            .execute(
                "SELECT array_to_string(array_reverse(ARRAY['{}'::jsonb, '1'::jsonb]), '|')",
                &[],
            )
            .await
            .expect("array_to_string should use JSONB's canonical text output");
        assert_rows_eq(json_text, vec![vec![Value::Text("1|{}".into())]]);

        for sql in [
            "SELECT array_position(ARRAY[lix_row_ref('lix_key_value', NULL, 'typed'), lix_row_ref('lix_key_value', NULL, 'typed')], lix_row_ref('lix_key_value', NULL, 'typed'), 2)",
            "SELECT unnest(array_positions(ARRAY[lix_row_ref('lix_key_value', NULL, 'typed'), lix_row_ref('lix_key_value', NULL, 'typed')], lix_row_ref('lix_key_value', NULL, 'typed')))",
            "SELECT unnest(array_distinct(ARRAY[lix_row_ref('lix_key_value', NULL, 'typed'), lix_row_ref('lix_key_value', NULL, 'typed')]))",
            "SELECT unnest(array_union(ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')], ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')]))",
            "SELECT unnest(array_intersect(ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')], ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')]))",
            "SELECT unnest(array_except(ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')], ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')]))",
        ] {
            let result = session
                .execute(sql, &[])
                .await
                .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
            if !sql.contains("unnest(array_positions") {
                assert_eq!(
                    result.column_types(),
                    &[if sql.contains("array_position(") {
                        ResultColumnType::Integer
                    } else {
                        ResultColumnType::RowRef
                    }],
                    "{sql}"
                );
            }
        }

        let row_ref_text = session
            .execute(
                "SELECT array_to_string(ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')], '|')",
                &[],
            )
            .await
            .expect_err("implicit array formatting must not expose opaque ROW_REF storage");
        assert_eq!(row_ref_text.code, lix::LixError::CODE_UNSUPPORTED_SQL);
        for sql in [
            "SELECT array_to_string(array_reverse(ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')]), '|')",
            "SELECT array_to_string(array_agg(v), '|') FROM (SELECT lix_row_ref('lix_key_value', NULL, 'typed') AS v) q",
            "SELECT array_to_string(ARRAY[ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')]], '|')",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("transformed and nested ROW_REF arrays stay opaque");
            assert_eq!(error.code, lix::LixError::CODE_UNSUPPORTED_SQL, "{sql}");
        }
    }
);

simulation_test!(
    logical_metadata_survives_group_keys_and_unnest,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        let grouped = session
            .execute(
                "SELECT COALESCE('{}'::jsonb, NULL) AS v, COUNT(*) \
                 FROM (VALUES (1), (2)) AS input(n) \
                 GROUP BY COALESCE('{}'::jsonb, NULL)",
                &[],
            )
            .await
            .expect("JSONB group keys should keep their logical result type");
        assert_eq!(
            grouped.column_types(),
            &[ResultColumnType::Jsonb, ResultColumnType::Integer]
        );
        assert_rows_eq(
            grouped,
            vec![vec![
                Value::Jsonb(serde_json::json!({}).into()),
                Value::Integer(2),
            ]],
        );

        for sql in [
            "SELECT CASE WHEN true THEN NULL ELSE NULL END AS v \
             UNION ALL SELECT '{}'::jsonb",
            "SELECT COALESCE(NULL, NULL) AS v UNION ALL SELECT '{}'::jsonb",
            "SELECT NULLIF(NULL, NULL) AS v UNION ALL SELECT '{}'::jsonb",
        ] {
            let union = session
                .execute(sql, &[])
                .await
                .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
            assert_eq!(union.column_types(), &[ResultColumnType::Jsonb], "{sql}");
            assert_rows_eq(
                union,
                vec![
                    vec![Value::Null],
                    vec![Value::Jsonb(serde_json::json!({}).into())],
                ],
            );
        }
        for sql in [
            "SELECT CAST(NULL AS VARCHAR) AS v UNION ALL SELECT '{}'::jsonb",
            "SELECT CAST(NULL AS TEXT) AS v UNION ALL SELECT '{}'::jsonb",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("explicit text casts establish a plain-text type identity");
            assert_eq!(error.code, lix::LixError::CODE_TYPE_MISMATCH, "{sql}");
        }

        let unnested = session
            .execute("SELECT unnest(ARRAY['{}'::jsonb]) AS v", &[])
            .await
            .expect("UNNEST should retain logical type metadata from its array");
        assert_eq!(unnested.column_types(), &[ResultColumnType::Jsonb]);
        assert_rows_eq(
            unnested,
            vec![vec![Value::Jsonb(serde_json::json!({}).into())]],
        );

        let nested_unnested = session
            .execute(
                "SELECT unnest(unnest(ARRAY[ARRAY['{}'::jsonb]])) AS v \
                 UNION ALL SELECT '{}'::jsonb AS v",
                &[],
            )
            .await
            .expect("nested UNNEST should retain the JSONB identity at its leaf");
        assert_eq!(nested_unnested.column_types(), &[ResultColumnType::Jsonb]);
        assert_rows_eq(
            nested_unnested,
            vec![
                vec![Value::Jsonb(serde_json::json!({}).into())],
                vec![Value::Jsonb(serde_json::json!({}).into())],
            ],
        );

        let json_value = Value::Jsonb(serde_json::json!({}).into());
        for (sql, expected) in [
            (
                "SELECT unnest(a) AS v FROM (SELECT ARRAY['{}'::jsonb] AS a UNION ALL SELECT NULL) q",
                vec![vec![json_value.clone()]],
            ),
            (
                "SELECT unnest(a) AS v FROM (SELECT ARRAY['{}'::jsonb] AS a UNION ALL SELECT ARRAY[NULL]) q",
                vec![vec![json_value.clone()], vec![Value::Null]],
            ),
            (
                "SELECT unnest(unnest(a)) AS v FROM (SELECT ARRAY[ARRAY['{}'::jsonb]] AS a UNION ALL SELECT ARRAY[ARRAY[NULL]]) q",
                vec![vec![json_value.clone()], vec![Value::Null]],
            ),
            (
                "SELECT unnest(unnest(a)) AS v FROM (SELECT ARRAY[ARRAY[NULL]] AS a UNION ALL SELECT ARRAY[ARRAY['{}'::jsonb]]) q",
                vec![vec![Value::Null], vec![json_value.clone()]],
            ),
        ] {
            let unnested = session
                .execute(sql, &[])
                .await
                .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
            assert_eq!(unnested.column_types(), &[ResultColumnType::Jsonb], "{sql}");
            assert_rows_eq(unnested, expected);
        }

        let aggregated = session
            .execute(
                "SELECT unnest(array_agg(v)) AS v FROM (VALUES ('{}'::jsonb)) t(v)",
                &[],
            )
            .await
            .expect("array_agg should preserve the extension identity of its elements");
        assert_eq!(aggregated.column_types(), &[ResultColumnType::Jsonb]);
        assert_rows_eq(aggregated, vec![vec![json_value]]);
    }
);

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
            let statement = format!("SELECT {expression} AS v FROM lix_key_value WHERE key = 'typed'");
            let result = session.execute(&statement, &[]).await;
            if expression.starts_with("CAST(") {
                let rows = result.unwrap();
                assert_eq!(rows.column_types(), &[ResultColumnType::Text]);
                assert_rows_eq(rows, vec![vec![Value::Text("{\"a\":1}".into())]]);
            } else {
                let error = result.expect_err("mixed JSONB and text results must not erase identity");
                assert_eq!(error.code, lix::LixError::CODE_TYPE_MISMATCH, "{statement}");
            }
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

        let equality_sql = "SELECT key FROM lix_key_value \
            WHERE key = 'typed' \
              AND $1 = '{\"a\":1}'::jsonb";
        let equality = session
            .execute(equality_sql, std::slice::from_ref(&json_param))
            .await
            .unwrap();
        assert_rows_eq(equality, vec![vec![Value::Text("typed".into())]]);
        session
            .execute(equality_sql, &[Value::Text("{\"a\":1}".into())])
            .await
            .expect_err("bare text operand must not satisfy a JSONB comparison");

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
                        let rows = session
                            .execute(&statement, &[])
                            .await
                            .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
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
            .expect_err("set operations must not erase JSONB identity into TEXT");
        assert_eq!(mixed.code, lix::LixError::CODE_TYPE_MISMATCH);
    }
);

simulation_test!(
    logical_metadata_survives_nullif_and_value_windows,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        let json_value = Value::Jsonb(serde_json::json!({"a": 1}).into());
        let nullif_json = session
            .execute(
                "SELECT NULLIF('{\"a\":1}'::jsonb, '{\"a\":2}'::jsonb) AS v",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(nullif_json.column_types(), &[ResultColumnType::Jsonb]);
        assert_rows_eq(nullif_json, vec![vec![json_value.clone()]]);

        let nullif_json_null = session
            .execute(
                "SELECT NULLIF('{\"a\":1}'::jsonb, '{\"a\":1}'::jsonb) AS v",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(nullif_json_null.column_types(), &[ResultColumnType::Jsonb]);
        assert_rows_eq(nullif_json_null, vec![vec![Value::Null]]);

        let nullif_untyped_null = session
            .execute("SELECT NULLIF(NULL, '{}'::jsonb) AS v", &[])
            .await
            .unwrap();
        assert_eq!(
            nullif_untyped_null.column_types(),
            &[ResultColumnType::Jsonb]
        );
        assert_rows_eq(nullif_untyped_null, vec![vec![Value::Null]]);

        let nullif_row_ref = session
            .execute(
                "SELECT NULLIF(\
                    lix_row_ref('lix_key_value', NULL, 'left'), \
                    lix_row_ref('lix_key_value', NULL, 'right')\
                ) AS v",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(nullif_row_ref.column_types(), &[ResultColumnType::RowRef]);
        assert!(matches!(
            nullif_row_ref.rows()[0].values()[0],
            Value::RowRef(_)
        ));

        let row_ref_null = session
            .execute(
                "SELECT NULLIF(\
                    lix_row_ref('lix_key_value', NULL, 'left'), \
                    lix_row_ref('lix_key_value', NULL, 'left')\
                ) AS v",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row_ref_null.column_types(), &[ResultColumnType::RowRef]);
        assert_rows_eq(row_ref_null, vec![vec![Value::Null]]);

        let row_ref_untyped_null = session
            .execute(
                "SELECT NULLIF(NULL, lix_row_ref('lix_key_value', NULL, 'left')) AS v",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            row_ref_untyped_null.column_types(),
            &[ResultColumnType::RowRef]
        );
        assert_rows_eq(row_ref_untyped_null, vec![vec![Value::Null]]);

        let windows = session
            .execute(
                "SELECT \
                    FIRST_VALUE(v) OVER (ORDER BY id) AS first_v, \
                    LAST_VALUE(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_v, \
                    NTH_VALUE(v, 1) OVER (ORDER BY id) AS nth_v, \
                    LAG(v) OVER (ORDER BY id) AS lag_v, \
                    LEAD(v) OVER (ORDER BY id) AS lead_v \
                 FROM (\
                    SELECT 1 AS id, '{\"a\":1}'::jsonb AS v \
                    UNION ALL \
                    SELECT 2 AS id, NULL::jsonb AS v\
                 ) q",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            windows.column_types(),
            &[
                ResultColumnType::Jsonb,
                ResultColumnType::Jsonb,
                ResultColumnType::Jsonb,
                ResultColumnType::Jsonb,
                ResultColumnType::Jsonb,
            ]
        );
        assert_rows_eq(
            windows,
            vec![
                vec![
                    json_value.clone(),
                    Value::Null,
                    json_value.clone(),
                    Value::Null,
                    Value::Null,
                ],
                vec![
                    json_value.clone(),
                    Value::Null,
                    json_value.clone(),
                    json_value,
                    Value::Null,
                ],
            ],
        );
    }
);

simulation_test!(
    logical_value_extensions_reject_implicit_utf8_operations,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        for statement in [
            "SELECT '{\"a\":1}'::jsonb < '{\"a\":2}'::jsonb",
            "SELECT MIN(v) FROM (SELECT '2'::jsonb AS v) q",
            "SELECT array_min(array_reverse(ARRAY['10'::jsonb, '2'::jsonb]))",
            "SELECT array_max(array_append(ARRAY['10'::jsonb], '2'::jsonb))",
            "SELECT array_sort(array_reverse(ARRAY['10'::jsonb, '2'::jsonb]))",
            "SELECT array_min(array_reverse(ARRAY[lix_row_ref('lix_key_value', NULL, 'a'), lix_row_ref('lix_key_value', NULL, 'b')]))",
            "SELECT array_max(array_append(ARRAY[lix_row_ref('lix_key_value', NULL, 'a')], lix_row_ref('lix_key_value', NULL, 'b')))" ,
            "SELECT array_sort(array_reverse(ARRAY[lix_row_ref('lix_key_value', NULL, 'a'), lix_row_ref('lix_key_value', NULL, 'b')]))",
            "SELECT array_agg(v ORDER BY v) FROM (VALUES ('10'::jsonb), ('2'::jsonb)) q(v)",
            "SELECT LOWER('{\"a\":1}'::jsonb)",
            "SELECT '{\"a\":1}'::jsonb || ''",
            "SELECT lix_row_ref('lix_key_value', NULL, 'typed') < lix_row_ref('lix_key_value', NULL, 'typed')",
            "SELECT array_agg(v ORDER BY v) FROM (SELECT lix_row_ref('lix_key_value', NULL, 'typed') AS v) q",
            "SELECT LOWER(lix_row_ref('lix_key_value', NULL, 'typed'))",
        ] {
            let error = session
                .execute(statement, &[])
                .await
                .expect_err("logical Lix values must not inherit UTF-8 ordering or functions");
            assert_eq!(error.code, lix::LixError::CODE_UNSUPPORTED_SQL, "{statement}");
        }

        let plain_text = session
            .execute("SELECT 'a' < 'b', LOWER('A'), 'a' || 'b'", &[])
            .await
            .unwrap();
        assert_rows_eq(
            plain_text,
            vec![vec![
                Value::Boolean(true),
                Value::Text("a".into()),
                Value::Text("ab".into()),
            ]],
        );

        let explicit_text = session
            .execute(
                "SELECT LOWER(CAST('{\"a\":1}'::jsonb AS TEXT)), \
                        LOWER(CAST(lix_row_ref('lix_key_value', NULL, 'typed') AS TEXT))",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            explicit_text.column_types(),
            &[ResultColumnType::Text, ResultColumnType::Text]
        );
    }
);

simulation_test!(
    row_ref_values_keep_identity_equality,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let row_ref = session
            .execute(
                "SELECT lix_row_ref('lix_key_value', NULL, 'typed') AS v",
                &[],
            )
            .await
            .unwrap();
        let row_ref_value = row_ref.rows()[0].values()[0].clone();
        assert_eq!(row_ref.column_types(), &[ResultColumnType::RowRef]);

        for sql in [
            "SELECT array_position(ARRAY['{}'::jsonb], '{}'::jsonb)",
            "SELECT array_position(a, '{}'::jsonb) FROM (SELECT ARRAY['{}'::jsonb] AS a) q",
        ] {
            let position = session.execute(sql, &[]).await.unwrap_or_else(|error| {
                panic!("DataFusion array_position should retain JSONB equality: {error:?}")
            });
            assert_rows_eq(position, vec![vec![Value::Integer(1)]]);
        }

        for sql in [
            "SELECT array_position(ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')], lix_row_ref('lix_key_value', NULL, 'typed'))",
            "SELECT array_position(a, lix_row_ref('lix_key_value', NULL, 'typed')) FROM (SELECT ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')] AS a) q",
        ] {
            let position = session.execute(sql, &[]).await.unwrap_or_else(|error| {
                panic!("DataFusion array_position should retain ROW_REF equality: {error:?}")
            });
            assert_rows_eq(position, vec![vec![Value::Integer(1)]]);
        }

        for sql in [
            "SELECT array_min(ARRAY['10'::jsonb, '2'::jsonb])",
            "SELECT array_min(a) FROM (SELECT ARRAY['10'::jsonb, '2'::jsonb] AS a) q",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("array_min must not apply DataFusion's UTF-8 order to JSONB");
            assert_eq!(error.code, lix::LixError::CODE_UNSUPPORTED_SQL, "{sql}");
        }

        let equal = session
            .execute("SELECT $1 = lix_row_ref('lix_key_value', NULL, 'typed')", std::slice::from_ref(&row_ref_value))
            .await
            .unwrap();
        assert_rows_eq(equal, vec![vec![Value::Boolean(true)]]);

        let null_comparison = session
            .execute(
                "SELECT lix_row_ref('lix_key_value', NULL, 'typed') IS DISTINCT FROM NULL",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(null_comparison, vec![vec![Value::Boolean(true)]]);

        let nullif_text = session
            .execute(
                "SELECT NULLIF(lix_row_ref('lix_key_value', NULL, 'typed'), $1)",
                &[Value::Text("text".into())],
            )
            .await
            .expect_err("NULLIF must not compare ROW_REF to plain text");
        assert_eq!(nullif_text.code, lix::LixError::CODE_TYPE_MISMATCH);

        for statement in [
            "SELECT lix_row_ref('lix_key_value', NULL, 'typed') = 'text'",
            "SELECT lix_row_ref('lix_key_value', NULL, 'typed') IN ('text')",
            "SELECT lix_row_ref('lix_key_value', NULL, 'typed') IS DISTINCT FROM 'text'",
            "SELECT CASE lix_row_ref('lix_key_value', NULL, 'typed') WHEN 'text' THEN true ELSE false END",
            "SELECT v FROM (SELECT lix_row_ref('lix_key_value', NULL, 'typed') AS v) q WHERE v = 'text'",
            "SELECT lix_row_ref('lix_key_value', NULL, 'typed') UNION ALL SELECT 'text'",
            "SELECT lix_row_ref('lix_key_value', NULL, 'typed') LIKE 'text%'",
            "SELECT lix_row_ref('lix_key_value', NULL, 'typed') = ANY(ARRAY['text'])",
        ] {
            let error = session
                .execute(statement, &[])
                .await
                .expect_err("ROW_REF identity must not be erased by UTF-8 coercion");
            assert!(
                error.code == lix::LixError::CODE_TYPE_MISMATCH
                    || error.code == lix::LixError::CODE_UNSUPPORTED_SQL,
                "{statement}: {error:?}"
            );
        }

        let bound_text = session
            .execute(
                "SELECT lix_row_ref('lix_key_value', NULL, 'typed') = $1",
                &[Value::Text("typed".into())],
            )
            .await
            .expect_err("ROW_REF identity must not be erased by text parameters");
        assert_eq!(bound_text.code, lix::LixError::CODE_TYPE_MISMATCH);

        let text_column = session
            .execute(
                "SELECT key FROM lix_key_value WHERE key = $1",
                std::slice::from_ref(&row_ref_value),
            )
            .await
            .expect_err("ROW_REF must not compare implicitly with a text column");
        assert_eq!(text_column.code, lix::LixError::CODE_TYPE_MISMATCH);

        let lower = session
            .execute("SELECT LOWER($1)", std::slice::from_ref(&row_ref_value))
            .await
            .expect_err("ROW_REF must be explicitly cast before text functions");
        assert_eq!(lower.code, lix::LixError::CODE_UNSUPPORTED_SQL);

        let explicit_text = session
            .execute(
                "SELECT LOWER(CAST($1 AS TEXT)) AS v",
                std::slice::from_ref(&row_ref_value),
            )
            .await
            .unwrap();
        assert_eq!(explicit_text.column_types(), &[ResultColumnType::Text]);
        assert!(matches!(explicit_text.rows()[0].values()[0], Value::Text(_)));

        let row_ref_parameter_comparison = session
            .execute("SELECT $1 = 'text'", std::slice::from_ref(&row_ref_value))
            .await
            .expect_err("ROW_REF parameters retain their logical identity");
        assert_eq!(
            row_ref_parameter_comparison.code,
            lix::LixError::CODE_TYPE_MISMATCH
        );

        let membership = session
            .execute(
                "SELECT $1 IN (lix_row_ref('lix_key_value', NULL, 'typed')), \
                        $1 = ANY(ARRAY[lix_row_ref('lix_key_value', NULL, 'typed')])",
                std::slice::from_ref(&row_ref_value),
            )
            .await
            .unwrap();
        assert_rows_eq(
            membership,
            vec![vec![Value::Boolean(true), Value::Boolean(true)]],
        );

        let explicit_text = session
            .execute("SELECT CAST($1 AS TEXT) = 'text'", std::slice::from_ref(&row_ref_value))
            .await
            .unwrap();
        assert_rows_eq(explicit_text, vec![vec![Value::Boolean(false)]]);
    }
);

simulation_test!(
    lag_lead_defaults_keep_only_value_metadata,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        let json = session
            .execute(
                "SELECT LAG(NULL, 1, '{}'::jsonb) OVER (ORDER BY x) AS v \
                 FROM (VALUES (1), (2)) q(x)",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(json.column_types(), &[ResultColumnType::Jsonb]);
        assert_rows_eq(json, vec![vec![Value::Jsonb(serde_json::json!({}).into())], vec![Value::Null]]);

        let row_ref = session
            .execute(
                "SELECT LEAD(NULL, 1, lix_row_ref('lix_key_value', NULL, 'typed')) OVER (ORDER BY x) AS v \
                 FROM (VALUES (1), (2)) q(x)",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row_ref.column_types(), &[ResultColumnType::RowRef]);
        assert!(matches!(row_ref.rows()[0].values()[0], Value::Null));
        assert!(matches!(row_ref.rows()[1].values()[0], Value::RowRef(_)));

        let mixed_default = session
            .execute(
                "SELECT LAG('{}'::jsonb, 1, 'fallback') OVER (ORDER BY x) \
                 FROM (VALUES (1), (2)) q(x)",
                &[],
            )
            .await
            .expect_err("window defaults must preserve the JSONB type identity");
        assert_eq!(mixed_default.code, lix::LixError::CODE_TYPE_MISMATCH);

        let mixed_nullif = session
            .execute("SELECT NULLIF('{}'::jsonb, 'text')", &[])
            .await
            .expect_err("NULLIF must not compare JSONB with plain text");
        assert_eq!(mixed_nullif.code, lix::LixError::CODE_TYPE_MISMATCH);
    }
);

simulation_test!(
    logical_value_parameters_preserve_custom_type_identity,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let json = Value::Jsonb(serde_json::json!({"a": 1}).into());

        let nullif = session
            .execute(
                "SELECT NULLIF('{\"a\":1}'::jsonb, '{}'::jsonb) = $1",
                std::slice::from_ref(&json),
            )
            .await
            .unwrap();
        assert_rows_eq(nullif, vec![vec![Value::Boolean(true)]]);

        let window = session
            .execute(
                "SELECT LAG(v) OVER (ORDER BY id) = $1 \
                 FROM (VALUES (1, '{\"a\":1}'::jsonb), (2, '{\"a\":1}'::jsonb)) q(id, v)",
                std::slice::from_ref(&json),
            )
            .await
            .unwrap();
        assert_rows_eq(window, vec![vec![Value::Null], vec![Value::Boolean(true)]]);

        let text_parameter = session
            .execute(
                "SELECT NULLIF('{\"a\":1}'::jsonb, '{}'::jsonb) = $1",
                &[Value::Text("{\"a\":1}".into())],
            )
            .await
            .expect_err("text parameters must not compare implicitly with JSONB");
        assert_eq!(text_parameter.code, lix::LixError::CODE_TYPE_MISMATCH);

        let text_literal = session
            .execute("SELECT NULLIF('{}'::jsonb, 'null'::jsonb) = '{}'", &[])
            .await
            .expect_err("text literals must not compare implicitly with JSONB");
        assert_eq!(text_literal.code, lix::LixError::CODE_TYPE_MISMATCH);

        let nullif_null = session
            .execute("SELECT NULLIF(NULL, '{}'::jsonb) = '{}'", &[])
            .await
            .expect_err("NULLIF must retain the type of its non-NULL operand");
        assert_eq!(nullif_null.code, lix::LixError::CODE_TYPE_MISMATCH);
    }
);

simulation_test!(
    explicit_casts_clear_lix_logical_metadata,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        let integer = session
            .execute(
                "SELECT n FROM (SELECT CAST('1'::jsonb AS BIGINT) AS n) q WHERE n = 1 ORDER BY n",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(integer.column_types(), &[ResultColumnType::Integer]);
        assert_rows_eq(integer, vec![vec![Value::Integer(1)]]);

        let varchar = session
            .execute("SELECT CAST('1'::jsonb AS VARCHAR) AS v", &[])
            .await
            .unwrap();
        assert_eq!(varchar.column_types(), &[ResultColumnType::Text]);
        assert_rows_eq(varchar, vec![vec![Value::Text("1".into())]]);
    }
);
