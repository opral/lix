use lix_engine::Value;
use serde_json::json;

use super::select_rows;

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
    let rows = result;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.rows()[0].values(),
        &[
            Value::Text("change-query".to_string()),
            Value::Text("lix_key_value".to_string()),
            Value::Json(json!({"key": "change-query", "value": "one"})),
        ]
    );
});

simulation_test!(
    lix_change_count_handles_empty_projection,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let rows = select_rows(&session, "SELECT count(*) FROM lix_change").await;
        assert_single_count(rows);
    }
);

fn assert_single_count(rows: Vec<Vec<Value>>) {
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 1);
    let Value::Integer(count) = rows[0][0] else {
        panic!("expected integer count, got {:?}", rows[0][0]);
    };
    assert!(count >= 0);
}
