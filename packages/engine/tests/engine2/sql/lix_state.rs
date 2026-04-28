use crate::simulation_test2;
use lix_engine::engine2::ExecuteResult;
use lix_engine::Value;

simulation_test2!(lix_state_latest_update_wins, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim
        .open_main_session(&engine)
        .await
        .expect("main session should open");

    session
        .execute(
            "INSERT INTO lix_state (\
             entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version, global, untracked\
             ) VALUES (\
             'state-latest', 'lix_key_value', NULL, NULL, lix_json('{\"key\":\"state-latest\",\"value\":\"old\"}'), '1', false, false\
             )",
            &[],
        )
        .await
        .expect("lix_state insert should succeed");
    session
        .execute(
            "UPDATE lix_state \
             SET snapshot_content = lix_json('{\"key\":\"state-latest\",\"value\":\"new\"}') \
             WHERE entity_id = 'state-latest' AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("lix_state update should succeed");

    let result = session
        .execute(
            "SELECT snapshot_content \
             FROM lix_state \
             WHERE entity_id = 'state-latest' AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("lix_state read should succeed");
    assert_single_text(result, "{\"key\":\"state-latest\",\"value\":\"new\"}");
});

simulation_test2!(lix_state_delete_hides_row, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim
        .open_main_session(&engine)
        .await
        .expect("main session should open");

    session
        .execute(
            "INSERT INTO lix_state (\
             entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version, global, untracked\
             ) VALUES (\
             'state-delete', 'lix_key_value', NULL, NULL, lix_json('{\"key\":\"state-delete\",\"value\":\"delete-me\"}'), '1', false, false\
             )",
            &[],
        )
        .await
        .expect("lix_state insert should succeed");
    session
        .execute(
            "DELETE FROM lix_state \
             WHERE entity_id = 'state-delete' AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("lix_state delete should succeed");

    let result = session
        .execute(
            "SELECT entity_id \
             FROM lix_state \
             WHERE entity_id = 'state-delete' AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("lix_state read should succeed");
    let ExecuteResult::Rows(rows) = result else {
        panic!("SELECT should return rows");
    };
    assert_eq!(rows.len(), 0);
});

fn assert_single_text(result: ExecuteResult, expected: &str) {
    let ExecuteResult::Rows(row_set) = result else {
        panic!("SELECT should return rows");
    };
    assert_eq!(row_set.len(), 1);
    assert_eq!(
        row_set.rows()[0].values(),
        &[Value::Text(expected.to_string())]
    );
}
