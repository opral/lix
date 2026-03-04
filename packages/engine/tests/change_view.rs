mod support;

use lix_engine::Value;

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

simulation_test!(
    lix_change_view_exposes_file_descriptor_change_rows,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('change-view-file-1', '/change-view.md', lix_text_encode('hello'))",
                &[],
            )
            .await
            .unwrap();

        let change_id_result = engine
            .execute(
                "SELECT change_id \
                 FROM lix_change_set_element \
                 WHERE entity_id = 'change-view-file-1' \
                   AND schema_key = 'lix_file_descriptor' \
                 ORDER BY change_id DESC \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(change_id_result.statements[0].rows.len(), 1);
        let change_id = match &change_id_result.statements[0].rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected change_id as text, got {other:?}"),
        };

        let result = engine
            .execute(
                "SELECT \
                   id, entity_id, schema_key, file_id, plugin_key, snapshot_content \
                 FROM lix_change \
                 WHERE id = $1",
                &[Value::Text(change_id.clone())],
            )
            .await
            .unwrap();

        assert_eq!(result.statements[0].rows.len(), 1);
        let row = &result.statements[0].rows[0];
        assert_text(&row[0], &change_id);
        assert_text(&row[1], "change-view-file-1");
        assert_text(&row[2], "lix_file_descriptor");
        assert_text(&row[3], "lix");
        assert_text(&row[4], "lix");
        match &row[5] {
            Value::Null | Value::Text(_) => {}
            other => panic!("expected snapshot_content as text or null, got {other:?}"),
        }
    }
);

simulation_test!(lix_change_rejects_insert_update_delete, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    let insert_err = engine
        .execute(
            "INSERT INTO lix_change (id, entity_id, schema_key, schema_version, file_id, plugin_key, created_at) \
             VALUES ('c1', 'e1', 's1', '1', 'lix', 'lix', '2026-01-01T00:00:00Z')", &[])
            .await
        .expect_err("INSERT on lix_change should fail");
    assert_eq!(insert_err.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");

    let update_err = engine
        .execute(
            "UPDATE lix_change SET schema_key = 'x' WHERE id = 'c1'",
            &[],
        )
        .await
        .expect_err("UPDATE on lix_change should fail");
    assert_eq!(update_err.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");

    let delete_err = engine
        .execute("DELETE FROM lix_change WHERE id = 'c1'", &[])
        .await
        .expect_err("DELETE on lix_change should fail");
    assert_eq!(delete_err.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");
});
