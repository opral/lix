use lix_engine::ExecuteResult;
use lix_engine::Value;

simulation_test!(lix_change_queries_tracked_changes, |sim| async move {
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
            "INSERT INTO lix_key_value (key, value) VALUES ('change-query', 'one')",
            &[],
        )
        .await
        .expect("tracked write should succeed");

    let result = session
        .execute(
            "SELECT entity_id, schema_key, snapshot_content \
             FROM lix_change \
             WHERE entity_id = 'change-query'",
            &[],
        )
        .await
        .expect("lix_change should read");
    let ExecuteResult::Rows(rows) = result else {
        panic!("SELECT should return rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.rows()[0].values(),
        &[
            Value::Text("change-query".to_string()),
            Value::Text("lix_key_value".to_string()),
            Value::Text("{\"key\":\"change-query\",\"value\":\"one\"}".to_string()),
        ]
    );
});
