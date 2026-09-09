use super::assert_rows_eq;
use lix::Value;

simulation_test!(
    typed_history_keeps_before_after_native_column_types,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session.execute("INSERT INTO lix_registered_schema (value) VALUES ($1)", &[Value::Jsonb(serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"history_types","columns":[{"name":"id","type":"text","nullable":false},{"name":"count","type":"int8","nullable":false},{"name":"active","type":"boolean","nullable":false},{"name":"meta","type":"jsonb","nullable":false}],"primary_key":["id"]}).into())]).await.unwrap();
        session.execute("INSERT INTO history_types (id, count, active, meta) VALUES ('row', 1, true, CAST('{\"source\":\"insert\"}' AS JSONB))", &[]).await.unwrap();
        session.execute("UPDATE history_types SET count = 2, active = false, meta = CAST('{\"source\":\"update\"}' AS JSONB) WHERE id = 'row'", &[]).await.unwrap();
        assert_rows_eq(session.execute("SELECT id, from_count, to_count, from_active, to_active, from_meta, to_meta FROM lix_history('history_types') WHERE diff_type = 'modified'", &[]).await.unwrap(), vec![vec![Value::Text("row".into()), Value::Integer(1), Value::Integer(2), Value::Boolean(true), Value::Boolean(false), Value::Jsonb(serde_json::json!({"source":"insert"}).into()), Value::Jsonb(serde_json::json!({"source":"update"}).into())]]);
    }
);

simulation_test!(
    history_reads_schema_changes_at_exact_endpoints,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('pair', 'one')",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'pair'",
                &[],
            )
            .await
            .unwrap();
        let event = session.execute("SELECT lixcol_from_commit_id, lixcol_to_commit_id FROM lix_history('lix_key_value') WHERE key = 'pair' ORDER BY lixcol_position LIMIT 1", &[]).await.unwrap();
        let from = event.rows()[0]
            .get::<String>("lixcol_from_commit_id")
            .unwrap();
        let to = event.rows()[0]
            .get::<String>("lixcol_to_commit_id")
            .unwrap();
        for (commit, expected) in [(from, "one"), (to, "two")] {
            assert_rows_eq(
                session
                    .execute(
                        "SELECT value FROM lix_as_of('lix_key_value', $1) WHERE key = 'pair'",
                        &[Value::Text(commit)],
                    )
                    .await
                    .unwrap(),
                vec![vec![Value::Jsonb(serde_json::json!(expected).into())]],
            );
        }
    }
);

simulation_test!(history_excludes_untracked_rows, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    session.execute("INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ('tracked', 'yes', false), ('transient', 'no', true)", &[]).await.unwrap();
    assert_rows_eq(session.execute("SELECT key FROM lix_history('lix_key_value') WHERE key IN ('tracked', 'transient')", &[]).await.unwrap(), vec![vec![Value::Text("tracked".into())]]);
});
