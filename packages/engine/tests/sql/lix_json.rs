use lix_engine::{LixError, Value};
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_json_expression_results_are_semantic_json,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let result = session
            .execute(
                "SELECT \
                lix_json('{\"name\":\"Ada\",\"tags\":[\"db\"]}') AS document, \
                lix_json(NULL) AS json_null, \
                lix_json_get('{\"name\":\"Ada\",\"tags\":[\"db\"]}', 'tags') AS tags, \
                lix_json_get('{\"name\":\"Ada\"}', 'missing') AS missing",
                &[],
            )
            .await
            .expect("select should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Json(json!({"name": "Ada", "tags": ["db"]})),
                Value::Json(json!(null)),
                Value::Json(json!(["db"])),
                Value::Null,
            ]],
        );
    }
);

simulation_test!(lix_json_get_uses_variadic_path_segments, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let result = session
            .execute(
                "SELECT lix_json_get_text('{\"user\":{\"names\":[\"Ada\"]}}', 'user', 'names', 0) AS name",
                &[],
            )
            .await
            .expect("select should succeed");

    assert_rows_eq(result, vec![vec![Value::Text("Ada".to_string())]]);
});

simulation_test!(lix_json_get_rejects_jsonpath_strings, |sim| async move {
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
            "SELECT lix_json_get_text('{\"path\":\"ok\"}', '$.path')",
            &[],
        )
        .await
        .expect_err("JSONPath-looking strings should fail loudly");

    assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    assert!(
        error.message.contains("uses variadic path segments"),
        "expected path segment diagnostic: {error}"
    );
});

simulation_test!(
    json_column_predicates_reject_bare_text_literals,
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
                "SELECT entity_pk FROM lix_state WHERE entity_pk = 'state-latest'",
                &[],
            )
            .await
            .expect_err("JSON column compared to text should fail loudly");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(
            error.hint().is_some_and(|hint| hint.contains("lix_json")),
            "expected lix_json hint: {error}"
        );
    }
);

simulation_test!(
    json_identity_read_predicates_reject_parseable_bare_text_literals,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for sql in [
            "SELECT entity_pk FROM lix_state WHERE entity_pk = '[ \"state-latest\" ]'",
            "SELECT id FROM lix_file WHERE lixcol_entity_pk = '[ \"file-readme\" ]'",
            "SELECT id FROM lix_directory WHERE lixcol_entity_pk = '[ \"directory-root\" ]'",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("read predicates should not silently compare raw identity JSON text");

            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
            assert!(
                error.hint().is_some_and(|hint| hint.contains("lix_json")),
                "expected lix_json hint: {error}"
            );
        }

        for sql in [
            "SELECT entity_pk FROM lix_state WHERE entity_pk = $1",
            "SELECT id FROM lix_file WHERE lixcol_entity_pk = $1",
            "SELECT id FROM lix_directory WHERE lixcol_entity_pk = $1",
        ] {
            let error = session
                .execute(sql, &[Value::Text("[\"state-latest\"]".to_string())])
                .await
                .expect_err("read predicates should reject bare text identity JSON parameters");

            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
            assert!(
                error
                    .hint()
                    .is_some_and(|hint| hint.contains("JSON parameter")),
                "expected JSON parameter hint: {error}"
            );
        }
    }
);

simulation_test!(
    json_column_predicates_accept_lix_json_expressions,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "SELECT entity_pk FROM lix_state WHERE entity_pk = lix_json('[\"state-latest\"]')",
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
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"engine_json_predicate_schema\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"meta\":{\"type\":\"object\"}},\"required\":[\"id\",\"meta\"],\"additionalProperties\":false}'),\
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
                 VALUES ('json-predicate-1', lix_json('{\"flag\":true}'), false)",
                &[],
            )
            .await
            .expect("typed entity insert should succeed");

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
                "SELECT id FROM engine_json_predicate_schema WHERE meta = lix_json('{\"flag\":true}')",
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
    registered_schema_dml_rejects_bare_lixcol_entity_pk_text,
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
                "UPDATE lix_registered_schema \
                 SET value = lix_json('{\"x-lix-key\":\"engine_schema_update_history\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}') \
                 WHERE lixcol_entity_pk = 'engine_schema_update_history'",
                &[],
            )
            .await
            .expect_err("bare text lixcol_entity_pk update should fail before matching rows");

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);

        let error = session
            .execute(
                "DELETE FROM lix_registered_schema \
                 WHERE lixcol_entity_pk = 'engine_schema_update_history'",
                &[],
            )
            .await
            .expect_err("bare text lixcol_entity_pk delete should fail before matching rows");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
    }
);
