use lix::LixError;
use lix::Value;

simulation_test!(sql_missing_table_has_lix_error_code, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let error = session
        .execute("SELECT * FROM missing_table", &[])
        .await
        .expect_err("missing table should fail");

    assert_eq!(error.code, LixError::CODE_TABLE_NOT_FOUND);
    assert!(error.hint().is_some(), "expected discovery hint: {error}");
});

simulation_test!(sql_missing_column_has_lix_error_code, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let error = session
        .execute("SELECT missing_column FROM lix_file", &[])
        .await
        .expect_err("missing column should fail");

    assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);
});

simulation_test!(
    sql_duplicate_projection_name_is_parse_error,
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
            .execute("SELECT 1 AS x, 2 AS x", &[])
            .await
            .expect_err("duplicate projection names should fail during planning");

        assert_eq!(error.code, LixError::CODE_PARSE_ERROR);
    }
);

simulation_test!(
    sql_postgresql_numbered_placeholders_bind_positionally,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = engine
            .open_session()
            .await
            .expect("main session should open");

        let result = session
            .execute(
                "SELECT * FROM lix_file WHERE id = $1",
                &[Value::Text(
                    "6d697373-696e-872d-8669-6c6500000000".to_string(),
                )],
            )
            .await
            .expect("numbered placeholder should bind when no rows match");
        assert_eq!(result.len(), 0);

        let result = session
            .execute(
                "SELECT '?' AS literal, $1 AS first, $2 AS second",
                &[Value::Integer(10), Value::Text("second".to_string())],
            )
            .await
            .expect("numbered placeholders should bind by index");
        let row = result.rows().first().expect("query should return one row");
        assert_eq!(
            row.values(),
            &[
                Value::Text("?".to_string()),
                Value::Integer(10),
                Value::Text("second".to_string()),
            ]
        );

        let result = session
            .execute(
                "SELECT 'it''s ?' AS escaped_literal, $1 AS bound_value -- ? in comment\n",
                &[Value::Integer(42)],
            )
            .await
            .expect("normalization should preserve escaped literals and comments");
        let row = result.rows().first().expect("query should return one row");
        assert_eq!(
            row.values(),
            &[Value::Text("it's ?".to_string()), Value::Integer(42)]
        );

        let error = session
            .execute("SELECT $1 AS missing_param", &[])
            .await
            .expect_err("numbered placeholder without a value should fail");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }
);

simulation_test!(sql_anonymous_placeholders_are_rejected, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let error = session
        .execute("SELECT ? AS anonymous_value", &[Value::Integer(1)])
        .await
        .expect_err("anonymous placeholders should fail");

    assert_eq!(error.code, LixError::CODE_PARSE_ERROR);
});

simulation_test!(
    sql_transaction_execute_accepts_numbered_placeholders,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");

        let result = transaction
            .execute(
                "SELECT $1 AS first, $2 AS second",
                &[Value::Integer(1), Value::Text("two".to_string())],
            )
            .await
            .expect("numbered parameter read in transaction should succeed");
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Integer(1), Value::Text("two".to_string())]
        );

        transaction
            .execute(
                "INSERT INTO lix_file (id, path) VALUES ($1, $2)",
                &[
                    Value::Text("616e6f6e-796d-8f75-832d-7472616e7300".to_string()),
                    Value::Text("/numbered-transaction.txt".to_string()),
                ],
            )
            .await
            .expect("numbered parameter write in transaction should succeed");

        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");

        let result = session
            .execute(
                "SELECT path FROM lix_file WHERE id = $1",
                &[Value::Text(
                    "616e6f6e-796d-8f75-832d-7472616e7300".to_string(),
                )],
            )
            .await
            .expect("committed numbered parameter write should be visible");
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Text("/numbered-transaction.txt".to_string())]
        );
    }
);

simulation_test!(sql_explain_is_read_shaped, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let result = session
        .execute("EXPLAIN SELECT $1 AS explained_value", &[Value::Integer(1)])
        .await
        .expect("EXPLAIN SELECT should return explain rows");
    assert!(!result.columns().is_empty());
    assert!(!result.rows().is_empty());

    let error = session
        .execute(
            "EXPLAIN INSERT INTO lix_file (id, path) VALUES ($1, $2)",
            &[
                Value::Text("explained-write".to_string()),
                Value::Text("/explained-write.txt".to_string()),
            ],
        )
        .await
        .expect_err("EXPLAIN of write statements should not route through write execution");
    assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
});

simulation_test!(
    sql_json_function_miss_has_postgres_jsonb_hint,
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
            .execute("SELECT json_extract('{\"a\":1}', '$.a')", &[])
            .await
            .expect_err("non-Lix JSON UDF should fail with a targeted hint");

        assert_eq!(error.code, LixError::CODE_UDF_NOT_FOUND);
        assert!(
            error
                .hint()
                .is_some_and(|hint| hint.contains("PostgreSQL JSONB operators")),
            "expected PostgreSQL JSONB hint: {error}"
        );
    }
);

simulation_test!(
    sql_json_arrow_operator_uses_postgres_semantics,
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
            .execute("SELECT CAST('{\"a\":1}' AS JSONB) ->> 'a'", &[])
            .await
            .expect("PostgreSQL JSON arrow operator should be supported");
        assert_eq!(result.rows()[0].values(), &[Value::Text("1".into())]);
    }
);

simulation_test!(
    sql_udf_argument_mismatch_is_public_invalid_param,
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
            .execute("SELECT uuidv7('unexpected')", &[])
            .await
            .expect_err("wrong UDF arity should fail as public invalid input");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }
);

simulation_test!(
    sql_non_utf8_blob_parameter_has_targeted_error,
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
            .execute("SELECT length($1)", &[Value::Blob(vec![0xff].into())])
            .await
            .expect_err("non-UTF-8 blob should fail as text");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(
            error.message.contains("valid UTF-8 text"),
            "expected targeted UTF-8 message: {error}"
        );
        assert!(
            error
                .hint()
                .is_some_and(|hint| hint.contains("blob") && !hint.contains("lix_json")),
            "expected blob-specific hint without JSON detour: {error}"
        );
    }
);

simulation_test!(
    sql_blob_insert_into_json_row_has_targeted_error,
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
                "INSERT INTO lix_key_value (key, value) VALUES ('blob-value', $1)",
                &[Value::Blob(vec![1, 2, 3, 255, 0, 128].into())],
            )
            .await
            .expect_err("blob row insert should fail cleanly");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(
            error.message.contains("cannot store blob values directly"),
            "expected targeted blob-to-JSON message: {error}"
        );
        assert!(
            !error.message.contains("Binary("),
            "error should not expose Rust/DataFusion debug formatting: {error}"
        );
    }
);

simulation_test!(sql_create_table_returns_error, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let error = session
        .execute("CREATE TABLE scratch (id TEXT)", &[])
        .await
        .expect_err("CREATE TABLE should return an error, not panic");

    assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
});

simulation_test!(
    sql_recursive_cte_over_commit_views_returns_error,
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
                "WITH RECURSIVE commit_walk(id) AS ( \
                 SELECT id FROM lix_commit \
                 UNION ALL \
                 SELECT lix_commit_edge.child_id \
                 FROM lix_commit_edge \
                 JOIN commit_walk ON lix_commit_edge.parent_id = commit_walk.id \
                 ) \
                 SELECT id FROM commit_walk",
                &[],
            )
            .await
            .expect_err("recursive CTE should return an error, not panic");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL, "{error:?}");
    }
);
