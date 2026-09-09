use super::assert_rows_eq;
use lix::Value;

simulation_test!(
    history_functions_are_not_static_tables_and_expose_endpoint_contract,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        assert!(session.execute("SELECT table_name FROM information_schema.tables WHERE table_name IN ('lix_file_history', 'lix_directory_history')", &[]).await.unwrap().is_empty());
        for relation in ["lix_file", "lix_directory", "lix_key_value"] {
            let result = session
                .execute(
                    &format!("SELECT * FROM lix_history('{relation}') LIMIT 0"),
                    &[],
                )
                .await
                .unwrap();
            for field in [
                "diff_type",
                "lixcol_from_commit_id",
                "lixcol_to_commit_id",
                "lixcol_commit_is_checkpoint",
                "lixcol_commit_created_at",
                "lixcol_position",
            ] {
                assert!(
                    result.columns().iter().any(|column| column == field),
                    "{relation} missing {field}"
                );
            }
            for retired in [
                "lixcol_depth",
                "lixcol_observed_commit_id",
                "lixcol_source_changes",
                "lixcol_as_of_commit_id",
                "lixcol_is_deleted",
            ] {
                assert!(!result.columns().iter().any(|column| column == retired));
            }
        }
    }
);

simulation_test!(
    typed_history_deletion_keeps_key_and_before_value,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('history-key', 'old')",
                &[],
            )
            .await
            .unwrap();
        session
            .execute("DELETE FROM lix_key_value WHERE key = 'history-key'", &[])
            .await
            .unwrap();
        assert_rows_eq(session.execute("SELECT key, diff_type, from_value, to_value FROM lix_history('lix_key_value') WHERE key = 'history-key' ORDER BY lixcol_position", &[]).await.unwrap(), vec![
            vec![Value::Text("history-key".into()), Value::Text("removed".into()), Value::Jsonb(serde_json::json!("old").into()), Value::Null],
            vec![Value::Text("history-key".into()), Value::Text("added".into()), Value::Null, Value::Jsonb(serde_json::json!("old").into())],
        ]);
    }
);

simulation_test!(
    typed_history_preserves_composite_primary_keys_on_removal,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session.execute("INSERT INTO lix_registered_schema (value) VALUES ($1)", &[Value::Jsonb(serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"history_composite","columns":[{"name":"tenant","type":"text","nullable":false},{"name":"number","type":"int8","nullable":false},{"name":"value","type":"text","nullable":true}],"primary_key":["tenant","number"]}).into())]).await.unwrap();
        session
            .execute(
                "INSERT INTO history_composite (tenant, number, value) VALUES ('a', 42, 'before')",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "DELETE FROM history_composite WHERE tenant = 'a' AND number = 42",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(session.execute("SELECT tenant, number, from_value, to_value FROM lix_history('history_composite') WHERE diff_type = 'removed'", &[]).await.unwrap(), vec![vec![Value::Text("a".into()), Value::Integer(42), Value::Text("before".into()), Value::Null]]);
    }
);

simulation_test!(
    history_default_anchor_matches_explicit_head_and_old_anchor_stays_pinned,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('pinned', 'one')",
                &[],
            )
            .await
            .unwrap();
        let anchor = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .unwrap()
            .unwrap();
        let query = "SELECT key, diff_type, from_value, to_value, lixcol_to_commit_id FROM lix_history('lix_key_value', $1) WHERE key = 'pinned' ORDER BY lixcol_position";
        let before = session
            .execute(query, &[Value::Text(anchor.clone())])
            .await
            .unwrap();
        let default = session.execute("SELECT key, diff_type, from_value, to_value, lixcol_to_commit_id FROM lix_history('lix_key_value') WHERE key = 'pinned' ORDER BY lixcol_position", &[]).await.unwrap();
        assert_eq!(before.rows(), default.rows());
        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'pinned'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            session
                .execute(query, &[Value::Text(anchor)])
                .await
                .unwrap()
                .rows(),
            before.rows()
        );
    }
);

simulation_test!(
    history_residual_filters_and_limit_follow_sql_order,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('ordered', 'z')",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "UPDATE lix_key_value SET value = 'a' WHERE key = 'ordered'",
                &[],
            )
            .await
            .unwrap();
        session
            .execute(
                "UPDATE lix_key_value SET value = 'm' WHERE key = 'ordered'",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(session.execute("SELECT to_value FROM lix_history('lix_key_value') WHERE key = 'ordered' AND to_value <> '\"m\"'::jsonb ORDER BY to_value LIMIT 1", &[]).await.unwrap(), vec![vec![Value::Jsonb(serde_json::json!("a").into())]]);
        assert!(session.execute("SELECT key FROM lix_history('lix_key_value') WHERE key = 'ordered' AND to_value = '\"a\"'::jsonb AND to_value = '\"z\"'::jsonb", &[]).await.unwrap().is_empty());
    }
);

simulation_test!(
    history_joins_keep_relation_local_filters,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('left', 'one'), ('right', 'two')",
                &[],
            )
            .await
            .unwrap();
        let result = session.execute("SELECT a.key, b.key FROM lix_history('lix_key_value') a JOIN lix_history('lix_key_value') b ON a.lixcol_to_commit_id = b.lixcol_to_commit_id WHERE a.key = 'left' AND b.key = 'right'", &[]).await.unwrap();
        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("left".into()),
                Value::Text("right".into()),
            ]],
        );
    }
);

simulation_test!(history_projection_preserves_event_rows, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('projection', 'one')",
            &[],
        )
        .await
        .unwrap();
    session
        .execute(
            "UPDATE lix_key_value SET value = 'two' WHERE key = 'projection'",
            &[],
        )
        .await
        .unwrap();
    let keys = session.execute("SELECT row_ref FROM lix_history('lix_key_value') WHERE key = 'projection' ORDER BY lixcol_position", &[]).await.unwrap();
    let full = session.execute("SELECT * FROM lix_history('lix_key_value') WHERE key = 'projection' ORDER BY lixcol_position", &[]).await.unwrap();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys.len(), full.len());
    for (key, row) in keys.rows().iter().zip(full.rows()) {
        assert_eq!(
            key.get::<Value>("row_ref").unwrap(),
            row.get::<Value>("row_ref").unwrap()
        );
    }
});

simulation_test!(
    history_rejects_removed_names_and_invalid_anchors,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        for sql in [
            "SELECT * FROM lix_history(NULL)",
            "SELECT * FROM lix_history('missing_relation')",
            "SELECT * FROM lix_history('lix_file', 'not-a-commit')",
            "SELECT lixcol_depth FROM lix_history('lix_file')",
            "SELECT * FROM lix_file_history",
        ] {
            assert!(session.execute(sql, &[]).await.is_err(), "{sql}");
        }
        assert!(
            session
                .execute(
                    "SELECT * FROM lix_history($1)",
                    &[Value::Text("lix_file".into())]
                )
                .await
                .is_err()
        );
    }
);
