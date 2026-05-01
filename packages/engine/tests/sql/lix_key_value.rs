use lix_engine::ExecuteResult;
use lix_engine::Value;

simulation_test!(lix_key_value_roundtrips_arbitrary_json, |sim| async move {
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
            "INSERT INTO lix_key_value (key, value) \
             VALUES ('kv-json', lix_json('{\"nested\":{\"flag\":true,\"items\":[1,\"two\",null]}}'))",
            &[],
        )
        .await
        .expect("insert should succeed");

    let result = session
        .execute("SELECT value FROM lix_key_value WHERE key = 'kv-json'", &[])
        .await
        .expect("select should succeed");
    assert_single_text(
        result,
        "{\"nested\":{\"flag\":true,\"items\":[1,\"two\",null]}}",
    );
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
