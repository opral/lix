mod support;

use lix_engine::Value;
use support::simulation_test::SimulationEngine;

async fn register_test_schema(engine: &SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .unwrap();
}

simulation_test!(
    untracked_state_routes_to_untracked_table,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine()
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
                 'lix_stored_schema',\
                 '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"untracked\"}', '1', 1\
             )",
                &[],
            )
            .await
            .unwrap();

        let initial = engine
            .execute(
                "SELECT snapshot_content FROM lix_internal_state_untracked WHERE entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        sim.expect_deterministic(initial.rows.clone());
        assert_eq!(initial.rows.len(), 1);
        assert_eq!(
            initial.rows[0][0],
            Value::Text("{\"key\":\"untracked\"}".to_string())
        );

        engine
            .execute(
                "UPDATE lix_internal_state_vtable SET snapshot_content = '{\"key\":\"updated\"}' \
             WHERE entity_id = 'entity-1' AND untracked = 1",
                &[],
            )
            .await
            .unwrap();

        let updated = engine
        .execute(
            "SELECT snapshot_content FROM lix_internal_state_untracked WHERE entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

        assert_eq!(
            updated.rows[0][0],
            Value::Text("{\"key\":\"updated\"}".to_string())
        );

        engine
            .execute(
                "INSERT INTO lix_internal_state_materialized_v1_test_schema (\
                 entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content, change_id, created_at, updated_at\
                 ) VALUES (\
                 'entity-1', 'test_schema', '1', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}', 'change-1', '1970-01-01T00:00:00Z', '1970-01-01T00:00:00Z'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let read = engine
            .execute(
                "SELECT snapshot_content, untracked FROM lix_internal_state_vtable \
             WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        sim.expect_deterministic(read.rows.clone());
        assert_eq!(read.rows.len(), 1);
        assert_eq!(
            read.rows[0][0],
            Value::Text("{\"key\":\"updated\"}".to_string())
        );
        assert_eq!(read.rows[0][1], Value::Integer(1));

        engine
            .execute(
                "DELETE FROM lix_internal_state_vtable WHERE entity_id = 'entity-1' AND untracked = 1",
                &[],
            )
            .await
            .unwrap();

        let remaining = engine
            .execute(
                "SELECT COUNT(*) FROM lix_internal_state_untracked WHERE entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(remaining.rows[0][0], Value::Integer(0));
    }
);

simulation_test!(untracked_state_change_id_is_untracked, |sim| async move {
    let engine = sim
        .boot_simulated_engine()
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
                 'lix_stored_schema',\
                 '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
                 )",
            &[],
        )
        .await
        .unwrap();

    engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"untracked\"}', '1', 1\
             )",
                &[],
            )
            .await
            .unwrap();

    let vtable = engine
        .execute(
            "SELECT change_id FROM lix_internal_state_vtable \
                 WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(vtable.rows.len(), 1);
    assert_eq!(vtable.rows[0][0], Value::Text("untracked".to_string()));
});

simulation_test!(
    tracked_state_creates_change_and_materialized_rows,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine()
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        register_test_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}', '1'\
             )",
                &[],
            )
            .await
            .unwrap();

        let changes = engine
            .execute(
                "SELECT snapshot_id FROM lix_internal_change WHERE entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(changes.rows.len(), 1);

        let snapshot_id = match &changes.rows[0][0] {
            Value::Text(value) => value.clone(),
            _ => panic!("expected snapshot id"),
        };

        let snapshots = engine
            .execute(
                &format!(
                    "SELECT content FROM lix_internal_snapshot WHERE id = '{}'",
                    snapshot_id
                ),
                &[],
            )
            .await
            .unwrap();

        assert_eq!(snapshots.rows.len(), 1);
        assert_eq!(
            snapshots.rows[0][0],
            Value::Text("{\"key\":\"tracked\"}".to_string())
        );

        let materialized = engine
            .execute(
                "SELECT snapshot_content FROM lix_internal_state_materialized_v1_test_schema \
                 WHERE entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(materialized.rows.len(), 1);
        assert_eq!(
            materialized.rows[0][0],
            Value::Text("{\"key\":\"tracked\"}".to_string())
        );
    }
);

simulation_test!(
    tracked_state_uses_no_content_snapshot_for_nulls,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine()
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        register_test_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-2', 'test_schema', 'file-1', 'version-1', 'lix', NULL, '1'\
             )",
                &[],
            )
            .await
            .unwrap();

        let changes = engine
            .execute(
                "SELECT snapshot_id FROM lix_internal_change WHERE entity_id = 'entity-2'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(changes.rows.len(), 1);
        assert_eq!(changes.rows[0][0], Value::Text("no-content".to_string()));
    }
);

simulation_test!(tracked_state_change_id_matches_vtable, |sim| async move {
    let engine = sim
        .boot_simulated_engine()
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    register_test_schema(&engine).await;

    engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}', '1'\
             )",
                &[],
            )
            .await
            .unwrap();

    let change = engine
        .execute(
            "SELECT id FROM lix_internal_change WHERE entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(change.rows.len(), 1);
    let change_id = match &change.rows[0][0] {
        Value::Text(value) => value.clone(),
        _ => panic!("expected change id"),
    };

    let vtable = engine
        .execute(
            "SELECT change_id FROM lix_internal_state_vtable \
                 WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(vtable.rows.len(), 1);
    assert_eq!(vtable.rows[0][0], Value::Text(change_id));
});

simulation_test!(tracked_update_creates_change_row, |sim| async move {
    let engine = sim
        .boot_simulated_engine()
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    register_test_schema(&engine).await;

    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}', '1'\
             )",
            &[],
        )
        .await
        .unwrap();

    engine
        .execute(
            "UPDATE lix_internal_state_vtable SET snapshot_content = '{\"key\":\"updated\"}' \
             WHERE entity_id = 'entity-1' AND schema_key = 'test_schema' AND file_id = 'file-1' AND version_id = 'version-1'",
            &[],
        )
        .await
        .unwrap();

    let vtable = engine
        .execute(
            "SELECT change_id, snapshot_content FROM lix_internal_state_vtable \
                 WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(vtable.rows.len(), 1);
    assert_eq!(
        vtable.rows[0][1],
        Value::Text("{\"key\":\"updated\"}".to_string())
    );

    let change_id = match &vtable.rows[0][0] {
        Value::Text(value) => value.clone(),
        _ => panic!("expected change id"),
    };

    let change = engine
        .execute(
            &format!(
                "SELECT snapshot_id FROM lix_internal_change WHERE id = '{}'",
                change_id
            ),
            &[],
        )
        .await
        .unwrap();

    assert_eq!(change.rows.len(), 1);
    let snapshot_id = match &change.rows[0][0] {
        Value::Text(value) => value.clone(),
        _ => panic!("expected snapshot id"),
    };

    let snapshot = engine
        .execute(
            &format!(
                "SELECT content FROM lix_internal_snapshot WHERE id = '{}'",
                snapshot_id
            ),
            &[],
        )
        .await
        .unwrap();

    assert_eq!(snapshot.rows.len(), 1);
    assert_eq!(
        snapshot.rows[0][0],
        Value::Text("{\"key\":\"updated\"}".to_string())
    );
});

simulation_test!(
    tracked_delete_creates_change_and_tombstone,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine()
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        register_test_schema(&engine).await;

        engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}', '1'\
             )",
            &[],
        )
        .await
        .unwrap();

        engine
        .execute(
            "DELETE FROM lix_internal_state_vtable \
             WHERE entity_id = 'entity-1' AND schema_key = 'test_schema' AND file_id = 'file-1' AND version_id = 'version-1'",
            &[],
        )
        .await
        .unwrap();

        let materialized = engine
            .execute(
                "SELECT is_tombstone, snapshot_content, change_id \
             FROM lix_internal_state_materialized_v1_test_schema \
             WHERE entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(materialized.rows.len(), 1);
        assert_eq!(materialized.rows[0][0], Value::Integer(1));
        assert_eq!(materialized.rows[0][1], Value::Null);

        let change_id = match &materialized.rows[0][2] {
            Value::Text(value) => value.clone(),
            _ => panic!("expected change id"),
        };

        let change = engine
            .execute(
                &format!(
                    "SELECT snapshot_id FROM lix_internal_change WHERE id = '{}'",
                    change_id
                ),
                &[],
            )
            .await
            .unwrap();

        assert_eq!(change.rows.len(), 1);
        assert_eq!(change.rows[0][0], Value::Text("no-content".to_string()));
    }
);

simulation_test!(tracked_multi_row_insert_creates_changes, |sim| async move {
    let engine = sim
        .boot_simulated_engine()
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    register_test_schema(&engine).await;

    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"one\"}', '1'\
             ), (\
             'entity-2', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"two\"}', '1'\
             )",
            &[],
        )
        .await
        .unwrap();

    let changes = engine
        .execute(
            "SELECT id, entity_id, snapshot_id FROM lix_internal_change \
             WHERE entity_id IN ('entity-1', 'entity-2')",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(changes.rows.len(), 2);

    let mut change_map: std::collections::HashMap<String, (String, String)> =
        std::collections::HashMap::new();
    for row in changes.rows {
        let change_id = match &row[0] {
            Value::Text(value) => value.clone(),
            _ => panic!("expected change id"),
        };
        let entity_id = match &row[1] {
            Value::Text(value) => value.clone(),
            _ => panic!("expected entity id"),
        };
        let snapshot_id = match &row[2] {
            Value::Text(value) => value.clone(),
            _ => panic!("expected snapshot id"),
        };
        change_map.insert(entity_id, (change_id, snapshot_id));
    }

    let expected_content = [
        ("entity-1", "{\"key\":\"one\"}"),
        ("entity-2", "{\"key\":\"two\"}"),
    ];

    for (entity_id, content) in expected_content {
        let snapshot_id = &change_map.get(entity_id).expect("missing change").1;
        let snapshot = engine
            .execute(
                &format!(
                    "SELECT content FROM lix_internal_snapshot WHERE id = '{}'",
                    snapshot_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(snapshot.rows.len(), 1);
        assert_eq!(snapshot.rows[0][0], Value::Text(content.to_string()));
    }

    let materialized = engine
        .execute(
            "SELECT entity_id, snapshot_content, change_id \
             FROM lix_internal_state_materialized_v1_test_schema \
             WHERE entity_id IN ('entity-1', 'entity-2')",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(materialized.rows.len(), 2);
    for row in materialized.rows {
        let entity_id = match &row[0] {
            Value::Text(value) => value.clone(),
            _ => panic!("expected entity id"),
        };
        let snapshot_content = match &row[1] {
            Value::Text(value) => value.clone(),
            _ => panic!("expected snapshot content"),
        };
        let change_id = match &row[2] {
            Value::Text(value) => value.clone(),
            _ => panic!("expected change id"),
        };
        let (expected_change_id, _) = change_map.get(&entity_id).expect("missing change");
        assert_eq!(&change_id, expected_change_id);
        let expected = expected_content
            .iter()
            .find(|(id, _)| *id == entity_id)
            .expect("missing expected")
            .1;
        assert_eq!(snapshot_content, expected);
    }
});

simulation_test!(
    tracked_update_null_uses_no_content_snapshot,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine()
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        register_test_schema(&engine).await;

        engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}', '1'\
             )",
            &[],
        )
        .await
        .unwrap();

        engine
        .execute(
            "UPDATE lix_internal_state_vtable SET snapshot_content = NULL \
             WHERE entity_id = 'entity-1' AND schema_key = 'test_schema' AND file_id = 'file-1' AND version_id = 'version-1'",
            &[],
        )
        .await
        .unwrap();

        let vtable = engine
            .execute(
                "SELECT change_id, snapshot_content FROM lix_internal_state_vtable \
                 WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(vtable.rows.len(), 1);
        assert_eq!(vtable.rows[0][1], Value::Null);

        let change_id = match &vtable.rows[0][0] {
            Value::Text(value) => value.clone(),
            _ => panic!("expected change id"),
        };

        let change = engine
            .execute(
                &format!(
                    "SELECT snapshot_id FROM lix_internal_change WHERE id = '{}'",
                    change_id
                ),
                &[],
            )
            .await
            .unwrap();

        assert_eq!(change.rows.len(), 1);
        assert_eq!(change.rows[0][0], Value::Text("no-content".to_string()));
    }
);

simulation_test!(
    mixed_tracked_and_untracked_insert_splits_writes,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine()
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        register_test_schema(&engine).await;

        engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
             ) VALUES (\
             'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}', '1', 0\
             ), (\
             'entity-2', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"untracked\"}', '1', 1\
             )",
            &[],
        )
        .await
        .unwrap();

        let change_count = engine
        .execute(
            "SELECT COUNT(*) FROM lix_internal_change WHERE entity_id IN ('entity-1', 'entity-2')",
            &[],
        )
        .await
        .unwrap();
        assert_eq!(change_count.rows[0][0], Value::Integer(1));

        let tracked_change = engine
            .execute(
                "SELECT snapshot_id FROM lix_internal_change WHERE entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(tracked_change.rows.len(), 1);
        let snapshot_id = match &tracked_change.rows[0][0] {
            Value::Text(value) => value.clone(),
            _ => panic!("expected snapshot id"),
        };

        let snapshot = engine
            .execute(
                &format!(
                    "SELECT content FROM lix_internal_snapshot WHERE id = '{}'",
                    snapshot_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(snapshot.rows.len(), 1);
        assert_eq!(
            snapshot.rows[0][0],
            Value::Text("{\"key\":\"tracked\"}".to_string())
        );

        let tracked_materialized = engine
            .execute(
                "SELECT snapshot_content FROM lix_internal_state_materialized_v1_test_schema \
             WHERE entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(tracked_materialized.rows.len(), 1);
        assert_eq!(
            tracked_materialized.rows[0][0],
            Value::Text("{\"key\":\"tracked\"}".to_string())
        );

        let untracked = engine
            .execute(
                "SELECT snapshot_content FROM lix_internal_state_untracked \
             WHERE entity_id = 'entity-2'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(untracked.rows.len(), 1);
        assert_eq!(
            untracked.rows[0][0],
            Value::Text("{\"key\":\"untracked\"}".to_string())
        );
    }
);
