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

simulation_test!(
    lix_key_value_on_conflict_upserts_active_row,
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
                "INSERT INTO lix_key_value (key, value) \
             VALUES ('kv-upsert-active', 'first') \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                &[],
            )
            .await
            .expect("initial upsert should insert");

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
             VALUES ('kv-upsert-active', 'second') \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                &[],
            )
            .await
            .expect("second upsert should update");

        let result = session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'kv-upsert-active'",
                &[],
            )
            .await
            .expect("select should succeed");
        assert_single_text(result, "\"second\"");
    }
);

simulation_test!(
    lix_key_value_on_conflict_active_insert_does_not_mutate_global_projection,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let global_session = sim.wrap_session(
            engine
                .open_session("global")
                .await
                .expect("global session should open"),
            &engine,
        );

        global_session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_global) \
                 VALUES ('kv-upsert-global-shadow', 'global', true)",
                &[],
            )
            .await
            .expect("global insert should succeed");

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('kv-upsert-global-shadow', 'active') \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                &[],
            )
            .await
            .expect("active upsert should insert an active override");

        let active = session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'kv-upsert-global-shadow'",
                &[],
            )
            .await
            .expect("active select should succeed");
        assert_single_text(active, "\"active\"");

        let global = global_session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'kv-upsert-global-shadow'",
                &[],
            )
            .await
            .expect("global select should succeed");
        assert_single_text(global, "\"global\"");
    }
);

simulation_test!(
    lix_key_value_by_branch_on_conflict_upserts_global_row,
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
                "INSERT INTO lix_key_value_by_branch \
             (key, value, lixcol_branch_id, lixcol_global, lixcol_untracked) \
             VALUES ('kv-upsert-global', 'first', 'global', true, true) \
             ON CONFLICT(key, lixcol_branch_id) DO UPDATE SET value = excluded.value",
                &[],
            )
            .await
            .expect("initial global upsert should insert");

        session
            .execute(
                "INSERT INTO lix_key_value_by_branch \
             (key, value, lixcol_branch_id, lixcol_global, lixcol_untracked) \
             VALUES ('kv-upsert-global', 'second', 'global', true, true) \
             ON CONFLICT(key, lixcol_branch_id) DO UPDATE SET value = excluded.value",
                &[],
            )
            .await
            .expect("second global upsert should update");

        let result = session
            .execute(
                "SELECT value FROM lix_key_value_by_branch \
             WHERE key = 'kv-upsert-global' AND lixcol_branch_id = 'global'",
                &[],
            )
            .await
            .expect("select should succeed");
        assert_single_text(result, "\"second\"");
    }
);

fn assert_single_text(result: ExecuteResult, expected: &str) {
    let row_set = result;
    assert_eq!(row_set.len(), 1);
    let expected_json = serde_json::from_str::<serde_json::Value>(expected)
        .expect("expected value should be valid JSON");
    assert_eq!(row_set.rows()[0].values(), &[Value::Json(expected_json)]);
}
