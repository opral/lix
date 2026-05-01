use lix_engine::ExecuteResult;
use lix_engine::LixError;
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

simulation_test!(lix_key_value_duplicate_insert_rejects, |sim| async move {
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
            "INSERT INTO lix_key_value (key, value) VALUES ('kv-duplicate', 'first')",
            &[],
        )
        .await
        .expect("initial insert should succeed");

    let error = session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('kv-duplicate', 'second')",
            &[],
        )
        .await
        .expect_err("plain INSERT should reject duplicate primary keys");
    assert_eq!(error.code, LixError::CODE_UNIQUE);

    session
        .execute(
            "UPDATE lix_key_value SET value = 'second' WHERE key = 'kv-duplicate'",
            &[],
        )
        .await
        .expect("explicit UPDATE should still replace existing state");

    let result = session
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'kv-duplicate'",
            &[],
        )
        .await
        .expect("select should succeed");
    assert_single_text(result, "\"second\"");
});

fn assert_single_text(result: ExecuteResult, expected: &str) {
    let row_set = result;
    assert_eq!(row_set.len(), 1);
    let expected_json = serde_json::from_str::<serde_json::Value>(expected)
        .expect("expected value should be valid JSON");
    assert_eq!(row_set.rows()[0].values(), &[Value::Json(expected_json)]);
}
