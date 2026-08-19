use lix::ExecuteResult;
use lix::LixError;
use lix::Value;

simulation_test!(lix_key_value_roundtrips_arbitrary_json, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) \
             VALUES ('kv-json', CAST('{\"nested\":{\"flag\":true,\"items\":[1,\"two\",null]}}' AS JSONB))",
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

simulation_test!(
    lix_key_value_persisted_tracked_row_scan_preserves_canonical_json_through_global_branch_merge,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let global_session = sim.wrap_session(
            engine
                .open_session_at("ffffffff-ffff-7fff-bfff-ffffffffffff")
                .await
                .expect("global session should open"),
            &engine,
        );

        // Both writes auto-commit before the read. That forces normal tracked
        // TransactionJson bytes through the persisted global and active heads,
        // then through the active branch's global-fallback merge.
        global_session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_global) \
                 VALUES ('kv-canonical-persisted-global', \
                         CAST('{ \"z\": { \"b\": 2, \"a\": 1 }, \"a\": [3, 2] }' AS JSONB), true)",
                &[],
            )
            .await
            .expect("tracked global insert should succeed");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('kv-canonical-persisted-active', \
                         CAST('{ \"z\": { \"d\": 4, \"c\": 3 }, \"a\": [5, 4] }' AS JSONB))",
                &[],
            )
            .await
            .expect("tracked active insert should succeed");

        let result = session
            // LIKE deliberately routes through the broad SchemaSpec scan rather
            // than the exact-primary-key native executor.
            .execute(
                "SELECT key, CONCAT(value, '') AS value_text FROM lix_key_value \
                 WHERE key LIKE 'kv-canonical-persisted-%' ORDER BY key",
                &[],
            )
            .await
            .expect("broad tracked row scan should succeed");
        assert_eq!(
            result
                .rows()
                .iter()
                .map(lix::Row::values)
                .collect::<Vec<_>>(),
            vec![
                &[
                    Value::Text("kv-canonical-persisted-active".to_string()),
                    Value::Text(r#"{"a":[5,4],"z":{"c":3,"d":4}}"#.to_string()),
                ],
                &[
                    Value::Text("kv-canonical-persisted-global".to_string()),
                    Value::Text(r#"{"a":[3,2],"z":{"a":1,"b":2}}"#.to_string()),
                ],
            ],
            "persisted tracked JSON stays canonical through global/branch visibility merge"
        );
    }
);

simulation_test!(lix_key_value_duplicate_insert_rejects, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
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
    lix_key_value_update_preserves_created_at,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('kv-created-at', 'first')",
                &[],
            )
            .await
            .expect("initial insert should succeed");
        let first_created_at = session
            .execute(
                "SELECT lixcol_created_at FROM lix_key_value WHERE key = 'kv-created-at'",
                &[],
            )
            .await
            .expect("initial created timestamp should be readable")
            .rows()[0]
            .get::<String>("lixcol_created_at")
            .expect("created timestamp should be text");

        session
            .execute(
                "UPDATE lix_key_value SET value = 'second' WHERE key = 'kv-created-at'",
                &[],
            )
            .await
            .expect("update should succeed");
        let updated_created_at = session
            .execute(
                "SELECT lixcol_created_at FROM lix_key_value WHERE key = 'kv-created-at'",
                &[],
            )
            .await
            .expect("updated created timestamp should be readable")
            .rows()[0]
            .get::<String>("lixcol_created_at")
            .expect("updated created timestamp should be text");

        assert_eq!(updated_created_at, first_created_at);
    }
);

simulation_test!(
    lix_key_value_insert_after_delete_resurrects_key,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('kv-resurrect', 'first')",
                &[],
            )
            .await
            .expect("initial insert should succeed");
        session
            .execute("DELETE FROM lix_key_value WHERE key = 'kv-resurrect'", &[])
            .await
            .expect("delete should succeed");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('kv-resurrect', 'second')",
                &[],
            )
            .await
            .expect("a tombstone should not block reinsertion");

        let result = session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'kv-resurrect'",
                &[],
            )
            .await
            .expect("resurrected key should read");
        assert_single_text(result, "\"second\"");
    }
);

simulation_test!(
    lix_key_value_on_conflict_upserts_active_row,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
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
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let global_session = sim.wrap_session(
            engine
                .open_session_at("ffffffff-ffff-7fff-bfff-ffffffffffff")
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


fn assert_single_text(result: ExecuteResult, expected: &str) {
    let row_set = result;
    assert_eq!(row_set.len(), 1);
    let expected_json = serde_json::from_str::<serde_json::Value>(expected)
        .expect("expected value should be valid JSON");
    assert_eq!(
        row_set.rows()[0].values(),
        &[Value::Jsonb(expected_json.into())]
    );
}
