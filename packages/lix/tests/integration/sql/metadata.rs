use lix::LixError;
use lix::Value;
use serde_json::json;

simulation_test!(
    metadata_rejects_invalid_json_on_lix_file_writes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "INSERT INTO lix_file (id, path, lixcol_metadata) \
                     VALUES ('6d657461-6461-8461-8d66-696c652d6900', '/6d657461-6461-8461-8d66-696c652d6900.txt', '{bad')",
                    &[],
                )
                .await
                .expect_err("invalid file metadata should be rejected on INSERT"),
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path) \
                 VALUES ('6d657461-6461-8461-8d66-696c652d7500', '/6d657461-6461-8461-8d66-696c652d7500.txt')",
                &[],
            )
            .await
            .expect("file insert should succeed");

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_file \
                     SET lixcol_metadata = '{bad' \
                     WHERE id = '6d657461-6461-8461-8d66-696c652d7500'",
                    &[],
                )
                .await
                .expect_err("invalid file metadata should be rejected on UPDATE"),
        );
    }
);

simulation_test!(
    metadata_rejects_invalid_json_on_lix_directory_writes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "INSERT INTO lix_directory (id, path, lixcol_metadata) \
                     VALUES ('6d657461-6461-8461-8d64-69722d696e00', '/6d657461-6461-8461-8d64-69722d696e00', '{bad')",
                    &[],
                )
                .await
                .expect_err("invalid directory metadata should be rejected on INSERT"),
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('6d657461-6461-8461-8d64-69722d757000', '/6d657461-6461-8461-8d64-69722d757000')",
                &[],
            )
            .await
            .expect("directory insert should succeed");

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_directory \
                     SET lixcol_metadata = '{bad' \
                     WHERE id = '6d657461-6461-8461-8d64-69722d757000'",
                    &[],
                )
                .await
                .expect_err("invalid directory metadata should be rejected on UPDATE"),
        );
    }
);

simulation_test!(
    metadata_narrow_filesystem_projections_keep_descriptor_rows,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let file_metadata = json!({"source": "66696c65-2d6e-8172-826f-770000000000"});
        session
            .execute(
                "INSERT INTO lix_file (id, path, lixcol_metadata) \
                 VALUES ('6d657461-6461-8461-8d6e-6172726f7700', '/6d657461-6461-8461-8d6e-6172726f7700.txt', $1)",
                &[Value::Json(file_metadata.clone().into())],
            )
            .await
            .expect("file insert should succeed");

        let file_result = session
            .execute(
                "SELECT id, lixcol_metadata \
                 FROM lix_file \
                 WHERE id = '6d657461-6461-8461-8d6e-6172726f7700'",
                &[],
            )
            .await
            .expect("narrow file metadata read should succeed");
        assert_metadata_value(file_result, "lixcol_metadata", &file_metadata);

        let directory_metadata = json!({"source": "64697265-6374-8f72-892d-6e6172726f00"});
        session
            .execute(
                "INSERT INTO lix_directory (id, path, lixcol_metadata) \
                 VALUES ('6d657461-6461-8461-8d6e-6172726f7700', '/6d657461-6461-8461-8d6e-6172726f7700', $1)",
                &[Value::Json(directory_metadata.clone().into())],
            )
            .await
            .expect("directory insert should succeed");

        let directory_result = session
            .execute(
                "SELECT path, lixcol_metadata \
                 FROM lix_directory \
                 WHERE id = '6d657461-6461-8461-8d6e-6172726f7700'",
                &[],
            )
            .await
            .expect("narrow directory metadata read should succeed");
        assert_metadata_value(directory_result, "lixcol_metadata", &directory_metadata);
    }
);

simulation_test!(
    metadata_rejects_invalid_json_on_typed_row_writes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "INSERT INTO lix_key_value (key, value, lixcol_metadata) \
                     VALUES ('metadata-row-insert', 'value', '{bad')",
                    &[],
                )
                .await
                .expect_err("invalid typed row metadata should be rejected on INSERT"),
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "INSERT INTO lix_key_value (key, value, lixcol_metadata) \
                     VALUES ('metadata-row-json-null-insert', 'value', CAST('null' AS JSONB))",
                    &[],
                )
                .await
                .expect_err("JSON null typed row metadata should be rejected on INSERT"),
        );

        session
            .execute(
                    "INSERT INTO lix_key_value (key, value, lixcol_metadata) \
                     VALUES ('metadata-row-lix-json-sql-null-insert', 'value', CAST(NULL AS JSONB))",
                    &[],
                )
                .await
                .expect("CAST(NULL AS JSONB) is SQL NULL metadata");

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('metadata-row-json-null-value', CAST(NULL AS JSONB))",
                &[],
            )
            .await
            .expect("CAST(NULL AS JSONB) should be accepted for JSON row columns");
        assert_metadata_null(
            session
                .execute(
                    "SELECT value \
                     FROM lix_key_value \
                     WHERE key = 'metadata-row-json-null-value'",
                    &[],
                )
                .await
                .expect("JSON null row value should read"),
            "value",
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('metadata-row-json-string-value', CAST('\"{\\\"source\\\":\\\"json-string\\\"}\"' AS JSONB))",
                &[],
            )
            .await
            .expect("JSON string row value should be accepted");
        assert_metadata_value(
            session
                .execute(
                    "SELECT value \
                     FROM lix_key_value \
                     WHERE key = 'metadata-row-json-string-value'",
                    &[],
                )
                .await
                .expect("JSON string row value should read"),
            "value",
            &json!("{\"source\":\"json-string\"}"),
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_metadata) \
                 VALUES ('metadata-row-sql-null-insert', 'value', NULL)",
                &[],
            )
            .await
            .expect("SQL NULL typed row metadata should be accepted on INSERT");
        assert_metadata_null(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = 'metadata-row-sql-null-insert'",
                    &[],
                )
                .await
                .expect("SQL NULL insert metadata should read as NULL"),
            "lixcol_metadata",
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "INSERT INTO lix_key_value (key, value, lixcol_metadata) \
                     VALUES ('metadata-row-json-null-param-insert', 'value', $1)",
                    &[Value::Json(json!(null).into())],
                )
                .await
                .expect_err("JSON null parameter metadata should be rejected on INSERT"),
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('metadata-row-update', NULL)",
                &[],
            )
            .await
            .expect("typed row insert should succeed");

        session
            .execute(
                "UPDATE lix_key_value \
                 SET lixcol_metadata = lixcol_metadata \
                 WHERE key = 'metadata-row-update'",
                &[],
            )
            .await
            .expect("metadata column SQL NULL should be preserved on UPDATE");
        assert_metadata_null(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = 'metadata-row-update'",
                    &[],
                )
                .await
                .expect("metadata column SQL NULL should read as NULL"),
            "lixcol_metadata",
        );

        session
            .execute(
                "UPDATE lix_key_value \
                 SET lixcol_metadata = CAST('{}' AS JSONB) -> 'missing' \
                 WHERE key = 'metadata-row-update'",
                &[],
            )
            .await
            .expect("metadata expression SQL NULL should be accepted on UPDATE");
        assert_metadata_null(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = 'metadata-row-update'",
                    &[],
                )
                .await
                .expect("metadata expression SQL NULL should read as NULL"),
            "lixcol_metadata",
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_key_value \
                     SET lixcol_metadata = CAST('{\"m\":null}' AS JSONB) -> 'm' \
                     WHERE key = 'metadata-row-update'",
                    &[],
                )
                .await
                .expect_err("JSON null from lix_json_get should be rejected as metadata"),
        );

        session
            .execute(
                "UPDATE lix_key_value \
                 SET lixcol_metadata = CAST('{\"m\":null}' AS JSONB) ->> 'm' \
                 WHERE key = 'metadata-row-update'",
                &[],
            )
            .await
            .expect("JSON null from lix_json_get_text should be accepted as SQL NULL metadata");
        assert_metadata_null(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = 'metadata-row-update'",
                    &[],
                )
                .await
                .expect("lix_json_get_text JSON null metadata assignment should read as NULL"),
            "lixcol_metadata",
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_key_value \
                     SET lixcol_metadata = value \
                     WHERE key = 'metadata-row-update'",
                    &[],
                )
                .await
                .expect_err("visible JSON null column should be rejected as metadata"),
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_key_value \
                     SET lixcol_metadata = CAST('{\"m\":\"{\\\"source\\\":\\\"json-string\\\"}\"}' AS JSONB) -> 'm' \
                     WHERE key = 'metadata-row-update'",
                    &[],
                )
                .await
                .expect_err("JSON string from lix_json_get should not be reparsed as metadata text"),
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('{\"source\":\"from-key\"}', 'metadata-source')",
                &[],
            )
            .await
            .expect("typed row insert with JSON-shaped string key should succeed");
        session
            .execute(
                "UPDATE lix_key_value \
                 SET lixcol_metadata = key \
                 WHERE key = '{\"source\":\"from-key\"}'",
                &[],
            )
            .await
            .expect("SQL text visible column should be parsed as metadata text");
        assert_metadata_value(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = '{\"source\":\"from-key\"}'",
                    &[],
                )
                .await
                .expect("metadata from SQL text column should read as JSON"),
            "lixcol_metadata",
            &json!({"source": "from-key"}),
        );

        session
            .execute(
                "UPDATE lix_key_value \
                 SET lixcol_metadata = CAST('\"{\\\"m\\\":{\\\"source\\\":\\\"json-string-root\\\"}}\"' AS JSONB) -> 'm' \
                 WHERE key = 'metadata-row-update'",
                &[],
            )
            .await
            .expect("JSON string root should not be reparsed by lix_json_get");
        assert_metadata_null(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = 'metadata-row-update'",
                    &[],
                )
                .await
                .expect("metadata should remain NULL when JSON string root is not reparsed"),
            "lixcol_metadata",
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_key_value \
                     SET lixcol_metadata = '{bad' \
                     WHERE key = 'metadata-row-update'",
                    &[],
                )
                .await
                .expect_err("invalid typed row metadata should be rejected on UPDATE"),
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_key_value \
                     SET lixcol_metadata = CAST('null' AS JSONB) \
                     WHERE key = 'metadata-row-update'",
                    &[],
                )
                .await
                .expect_err("JSON null typed row metadata should be rejected on UPDATE"),
        );

        session
            .execute(
                    "UPDATE lix_key_value \
                     SET lixcol_metadata = CAST(NULL AS JSONB) \
                     WHERE key = 'metadata-row-update'",
                    &[],
                )
                .await
                .expect("CAST(NULL AS JSONB) is SQL NULL metadata on UPDATE");

        session
            .execute(
                "UPDATE lix_key_value \
                 SET lixcol_metadata = $1 \
                 WHERE key = 'metadata-row-update'",
                &[Value::Null],
            )
            .await
            .expect("SQL NULL parameter metadata should be accepted on UPDATE");
        assert_metadata_null(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = 'metadata-row-update'",
                    &[],
                )
                .await
                .expect("SQL NULL parameter update metadata should read as NULL"),
            "lixcol_metadata",
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_key_value \
                     SET lixcol_metadata = $1 \
                     WHERE key = 'metadata-row-update'",
                    &[Value::Json(json!(null).into())],
                )
                .await
                .expect_err("JSON null parameter metadata should be rejected on UPDATE"),
        );
    }
);

simulation_test!(
    valid_object_metadata_survives_live_change_and_history_reads,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let expected = json!({
            "source": "metadata-regression",
            "nested": {"ok": true}
        });

        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_metadata) \
                 VALUES (\
                 'metadata-valid-object', \
                 'value', \
                 '{\"source\":\"metadata-regression\",\"nested\":{\"ok\":true}}'\
                 )",
                &[],
            )
            .await
            .expect("valid object metadata should write");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head commit should load")
            .expect("head commit should exist");

        assert_metadata_value(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = 'metadata-valid-object'",
                    &[],
                )
                .await
                .expect("typed row metadata should read"),
            "lixcol_metadata",
            &expected,
        );

        assert_metadata_value(
            session
                .execute(
                    "SELECT metadata \
                     FROM lix_change \
                     WHERE row_pk = CAST('[\"metadata-valid-object\"]' AS JSONB) \
                       AND schema_key = 'lix_key_value'",
                    &[],
                )
                .await
                .expect("lix_change metadata should read"),
            "metadata",
            &expected,
        );

        assert_metadata_value(
            session
                .execute(
                    &format!(
                        "SELECT lixcol_metadata \
                         FROM lix_key_value_history('{commit_id}') \
                           WHERE key = 'metadata-valid-object'"
                    ),
                    &[],
                )
                .await
                .expect("typed history metadata should read"),
            "lixcol_metadata",
            &expected,
        );
    }
);

fn assert_invalid_metadata_error(error: LixError) {
    assert!(
        matches!(
            error.code.as_str(),
            "LIX_ERROR_INVALID_JSON"
                | LixError::CODE_SCHEMA_VALIDATION
                | LixError::CODE_INVALID_PARAM
        ),
        "expected invalid metadata public error, got {error:?}"
    );
    assert!(
        error.message.contains("metadata") && error.message.contains("JSON"),
        "error should identify metadata JSON, got {error:?}"
    );
}

fn assert_metadata_value(result: lix::ExecuteResult, column: &str, expected: &serde_json::Value) {
    assert_eq!(result.len(), 1, "expected one metadata row");
    let value = result.rows()[0]
        .get::<Value>(column)
        .unwrap_or_else(|_| panic!("{column} should be present"));
    assert_eq!(value, Value::Json(expected.clone().into()));
}

fn assert_metadata_null(result: lix::ExecuteResult, column: &str) {
    assert_eq!(result.len(), 1, "expected one metadata row");
    let value = result.rows()[0]
        .get::<Value>(column)
        .unwrap_or_else(|_| panic!("{column} should be present"));
    assert_eq!(value, Value::Null);
}
