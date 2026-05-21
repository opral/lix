use lix_engine::LixError;
use lix_engine::Value;
use serde_json::json;

simulation_test!(
    metadata_rejects_invalid_json_on_lix_file_writes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "INSERT INTO lix_file (id, path, lixcol_metadata) \
                     VALUES ('metadata-file-insert', '/metadata-file-insert.txt', '{bad')",
                    &[],
                )
                .await
                .expect_err("invalid file metadata should be rejected on INSERT"),
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path) \
                 VALUES ('metadata-file-update', '/metadata-file-update.txt')",
                &[],
            )
            .await
            .expect("file insert should succeed");

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_file \
                     SET lixcol_metadata = '{bad' \
                     WHERE id = 'metadata-file-update'",
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
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "INSERT INTO lix_directory (id, path, lixcol_metadata) \
                     VALUES ('metadata-dir-insert', '/metadata-dir-insert/', '{bad')",
                    &[],
                )
                .await
                .expect_err("invalid directory metadata should be rejected on INSERT"),
        );

        session
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('metadata-dir-update', '/metadata-dir-update/')",
                &[],
            )
            .await
            .expect("directory insert should succeed");

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_directory \
                     SET lixcol_metadata = '{bad' \
                     WHERE id = 'metadata-dir-update'",
                    &[],
                )
                .await
                .expect_err("invalid directory metadata should be rejected on UPDATE"),
        );
    }
);

simulation_test!(
    metadata_rejects_invalid_json_on_typed_entity_writes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "INSERT INTO lix_key_value (key, value, lixcol_metadata) \
                     VALUES ('metadata-entity-insert', 'value', '{bad')",
                    &[],
                )
                .await
                .expect_err("invalid typed entity metadata should be rejected on INSERT"),
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "INSERT INTO lix_key_value (key, value, lixcol_metadata) \
                     VALUES ('metadata-entity-json-null-insert', 'value', lix_json('null'))",
                    &[],
                )
                .await
                .expect_err("JSON null typed entity metadata should be rejected on INSERT"),
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "INSERT INTO lix_key_value (key, value, lixcol_metadata) \
                     VALUES ('metadata-entity-lix-json-sql-null-insert', 'value', lix_json(NULL))",
                    &[],
                )
                .await
                .expect_err("lix_json(NULL) metadata should be rejected as JSON null on INSERT"),
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('metadata-entity-json-null-value', lix_json(NULL))",
                &[],
            )
            .await
            .expect("lix_json(NULL) should be accepted for JSON entity columns");
        assert_metadata_null(
            session
                .execute(
                    "SELECT value \
                     FROM lix_key_value \
                     WHERE key = 'metadata-entity-json-null-value'",
                    &[],
                )
                .await
                .expect("JSON null entity value should read"),
            "value",
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('metadata-entity-json-string-value', lix_json('\"{\\\"source\\\":\\\"json-string\\\"}\"'))",
                &[],
            )
            .await
            .expect("JSON string entity value should be accepted");
        assert_metadata_value(
            session
                .execute(
                    "SELECT value \
                     FROM lix_key_value \
                     WHERE key = 'metadata-entity-json-string-value'",
                    &[],
                )
                .await
                .expect("JSON string entity value should read"),
            "value",
            &json!("{\"source\":\"json-string\"}"),
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_metadata) \
                 VALUES ('metadata-entity-sql-null-insert', 'value', NULL)",
                &[],
            )
            .await
            .expect("SQL NULL typed entity metadata should be accepted on INSERT");
        assert_metadata_null(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = 'metadata-entity-sql-null-insert'",
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
                     VALUES ('metadata-entity-json-null-param-insert', 'value', $1)",
                    &[Value::Json(json!(null))],
                )
                .await
                .expect_err("JSON null parameter metadata should be rejected on INSERT"),
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('metadata-entity-update', NULL)",
                &[],
            )
            .await
            .expect("typed entity insert should succeed");

        session
            .execute(
                "UPDATE lix_key_value \
                 SET lixcol_metadata = lixcol_metadata \
                 WHERE key = 'metadata-entity-update'",
                &[],
            )
            .await
            .expect("metadata column SQL NULL should be preserved on UPDATE");
        assert_metadata_null(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = 'metadata-entity-update'",
                    &[],
                )
                .await
                .expect("metadata column SQL NULL should read as NULL"),
            "lixcol_metadata",
        );

        session
            .execute(
                "UPDATE lix_key_value \
                 SET lixcol_metadata = lix_json_get(lix_json('{}'), 'missing') \
                 WHERE key = 'metadata-entity-update'",
                &[],
            )
            .await
            .expect("metadata expression SQL NULL should be accepted on UPDATE");
        assert_metadata_null(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = 'metadata-entity-update'",
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
                     SET lixcol_metadata = lix_json_get(lix_json('{\"m\":null}'), 'm') \
                     WHERE key = 'metadata-entity-update'",
                    &[],
                )
                .await
                .expect_err("JSON null from lix_json_get should be rejected as metadata"),
        );

        session
            .execute(
                "UPDATE lix_key_value \
                 SET lixcol_metadata = lix_json_get_text(lix_json('{\"m\":null}'), 'm') \
                 WHERE key = 'metadata-entity-update'",
                &[],
            )
            .await
            .expect("JSON null from lix_json_get_text should be accepted as SQL NULL metadata");
        assert_metadata_null(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = 'metadata-entity-update'",
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
                     WHERE key = 'metadata-entity-update'",
                    &[],
                )
                .await
                .expect_err("visible JSON null column should be rejected as metadata"),
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_key_value \
                     SET lixcol_metadata = lix_json_get(lix_json('{\"m\":\"{\\\"source\\\":\\\"json-string\\\"}\"}'), 'm') \
                     WHERE key = 'metadata-entity-update'",
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
            .expect("typed entity insert with JSON-shaped string key should succeed");
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
                 SET lixcol_metadata = lix_json_get(lix_json('\"{\\\"m\\\":{\\\"source\\\":\\\"json-string-root\\\"}}\"'), 'm') \
                 WHERE key = 'metadata-entity-update'",
                &[],
            )
            .await
            .expect("JSON string root should not be reparsed by lix_json_get");
        assert_metadata_null(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = 'metadata-entity-update'",
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
                     WHERE key = 'metadata-entity-update'",
                    &[],
                )
                .await
                .expect_err("invalid typed entity metadata should be rejected on UPDATE"),
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_key_value \
                     SET lixcol_metadata = lix_json('null') \
                     WHERE key = 'metadata-entity-update'",
                    &[],
                )
                .await
                .expect_err("JSON null typed entity metadata should be rejected on UPDATE"),
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_key_value \
                     SET lixcol_metadata = lix_json(NULL) \
                     WHERE key = 'metadata-entity-update'",
                    &[],
                )
                .await
                .expect_err("lix_json(NULL) metadata should be rejected as JSON null on UPDATE"),
        );

        session
            .execute(
                "UPDATE lix_key_value \
                 SET lixcol_metadata = $1 \
                 WHERE key = 'metadata-entity-update'",
                &[Value::Null],
            )
            .await
            .expect("SQL NULL parameter metadata should be accepted on UPDATE");
        assert_metadata_null(
            session
                .execute(
                    "SELECT lixcol_metadata \
                     FROM lix_key_value \
                     WHERE key = 'metadata-entity-update'",
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
                     WHERE key = 'metadata-entity-update'",
                    &[Value::Json(json!(null))],
                )
                .await
                .expect_err("JSON null parameter metadata should be rejected on UPDATE"),
        );
    }
);

simulation_test!(
    metadata_rejects_invalid_json_on_lix_state_writes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        assert_invalid_metadata_error(
            session
                .execute(
                    "INSERT INTO lix_state (\
                     entity_pk, schema_key, file_id, snapshot_content, metadata\
                     ) VALUES (\
                     lix_json('[\"metadata-state-insert\"]'), 'lix_key_value', NULL, \
                     lix_json('{\"key\":\"metadata-state-insert\",\"value\":\"value\"}'), \
                     '{bad'\
                     )",
                    &[],
                )
                .await
                .expect_err("invalid lix_state metadata should be rejected on INSERT"),
        );

        session
            .execute(
                "INSERT INTO lix_state (\
                 entity_pk, schema_key, file_id, snapshot_content\
                 ) VALUES (\
                 lix_json('[\"metadata-state-update\"]'), 'lix_key_value', NULL, \
                 lix_json('{\"key\":\"metadata-state-update\",\"value\":\"value\"}')\
                 )",
                &[],
            )
            .await
            .expect("lix_state insert should succeed");

        assert_invalid_metadata_error(
            session
                .execute(
                    "UPDATE lix_state \
                     SET metadata = '{bad' \
                     WHERE entity_pk = lix_json('[\"metadata-state-update\"]') \
                       AND schema_key = 'lix_key_value'",
                    &[],
                )
                .await
                .expect_err("invalid lix_state metadata should be rejected on UPDATE"),
        );
    }
);

simulation_test!(
    valid_object_metadata_survives_live_change_and_history_reads,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
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
            .load_version_head_commit_id(sim.main_version_id())
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
                .expect("typed entity metadata should read"),
            "lixcol_metadata",
            &expected,
        );

        assert_metadata_value(
            session
                .execute(
                    "SELECT metadata \
                     FROM lix_state \
                     WHERE entity_pk = lix_json('[\"metadata-valid-object\"]') \
                       AND schema_key = 'lix_key_value'",
                    &[],
                )
                .await
                .expect("lix_state metadata should read"),
            "metadata",
            &expected,
        );

        assert_metadata_value(
            session
                .execute(
                    "SELECT metadata \
                     FROM lix_change \
                     WHERE entity_pk = lix_json('[\"metadata-valid-object\"]') \
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
                        "SELECT metadata \
                         FROM lix_state_history \
                         WHERE start_commit_id = '{commit_id}' \
                           AND entity_pk = lix_json('[\"metadata-valid-object\"]') \
                           AND schema_key = 'lix_key_value'"
                    ),
                    &[],
                )
                .await
                .expect("lix_state_history metadata should read"),
            "metadata",
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

fn assert_metadata_value(
    result: lix_engine::ExecuteResult,
    column: &str,
    expected: &serde_json::Value,
) {
    assert_eq!(result.len(), 1, "expected one metadata row");
    let value = result.rows()[0]
        .get::<Value>(column)
        .unwrap_or_else(|_| panic!("{column} should be present"));
    assert_eq!(value, Value::Json(expected.clone()));
}

fn assert_metadata_null(result: lix_engine::ExecuteResult, column: &str) {
    assert_eq!(result.len(), 1, "expected one metadata row");
    let value = result.rows()[0]
        .get::<Value>(column)
        .unwrap_or_else(|_| panic!("{column} should be present"));
    assert_eq!(value, Value::Null);
}
