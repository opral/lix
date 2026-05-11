use lix_engine::LixError;
use lix_engine::Value;

simulation_test!(sql_missing_table_has_lix_error_code, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
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
            .open_workspace_session()
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
                .open_workspace_session()
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

simulation_test!(sql_question_mark_placeholder_has_hint, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let error = session
        .execute("SELECT * FROM lix_file WHERE id = ?", &[])
        .await
        .expect_err("question mark placeholders should fail");

    assert_eq!(error.code, LixError::CODE_PARSE_ERROR);
    assert!(
        error.hint().is_some_and(|hint| hint.contains("$1")),
        "expected placeholder hint: {error}"
    );
});

simulation_test!(sql_json_function_miss_has_lix_udf_hint, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
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
            .is_some_and(|hint| hint.contains("lix_json_get")),
        "expected JSON UDF hint: {error}"
    );
});

simulation_test!(
    sql_json_arrow_operator_has_dialect_error,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute("SELECT lix_json('{\"a\":1}') ->> 'a'", &[])
            .await
            .expect_err("Postgres JSON arrow operator should fail with a dialect error");

        assert_eq!(error.code, LixError::CODE_DIALECT_UNSUPPORTED);
        assert!(
            error
                .hint()
                .is_some_and(|hint| hint.contains("lix_json_get_text")),
            "expected JSON dialect hint: {error}"
        );
    }
);

simulation_test!(
    sql_udf_argument_mismatch_is_public_invalid_param,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute("SELECT lix_uuid_v7('unexpected')", &[])
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
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute("SELECT length($1)", &[Value::Blob(vec![0xff])])
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
    sql_blob_insert_into_json_entity_has_targeted_error,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('blob-value', $1)",
                &[Value::Blob(vec![1, 2, 3, 255, 0, 128])],
            )
            .await
            .expect_err("blob entity insert should fail cleanly");

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
            .open_workspace_session()
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
                .open_workspace_session()
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
