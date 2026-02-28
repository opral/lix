mod support;
use std::collections::BTreeSet;

simulation_test!(init_creates_untracked_table, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    let result = engine
        .execute("SELECT 1 FROM lix_internal_state_untracked LIMIT 1", &[])
        .await
        .unwrap();

    sim.assert_deterministic(result.rows.clone());
});

simulation_test!(init_creates_snapshot_table, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    let result = engine
        .execute("SELECT 1 FROM lix_internal_snapshot LIMIT 1", &[])
        .await
        .unwrap();

    sim.assert_deterministic(result.rows.clone());
});

simulation_test!(init_creates_change_table, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    let result = engine
        .execute("SELECT 1 FROM lix_internal_change LIMIT 1", &[])
        .await
        .unwrap();

    sim.assert_deterministic(result.rows.clone());
});

simulation_test!(init_inserts_no_content_snapshot, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    let result = engine
        .execute(
            "SELECT content FROM lix_internal_snapshot WHERE id = 'no-content'",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(result.rows.clone());
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], lix_engine::Value::Null);
});

simulation_test!(
    init_creates_key_value_materialized_table,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        let result = engine
            .execute(
                "SELECT 1 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_key_value' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(result.rows.clone());
    }
);

simulation_test!(init_seeds_key_value_schema_definition, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    let result = engine
        .execute(
            "SELECT entity_id, snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE entity_id = 'lix_key_value~1' \
               AND schema_key = 'lix_stored_schema' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
             LIMIT 1",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(result.rows.clone());
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        lix_engine::Value::Text("lix_key_value~1".to_string())
    );

    let snapshot_content = match &result.rows[0][1] {
        lix_engine::Value::Text(value) => value,
        other => panic!("expected text snapshot_content, got {other:?}"),
    };
    let parsed: serde_json::Value = serde_json::from_str(snapshot_content).unwrap();
    assert_eq!(parsed["value"]["x-lix-key"], "lix_key_value");
    assert_eq!(parsed["value"]["x-lix-version"], "1");
});

simulation_test!(init_seeds_builtin_schema_definitions, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    let result = engine
        .execute(
            "SELECT entity_id, snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE entity_id IN (\
               'lix_stored_schema~1', \
               'lix_key_value~1', \
               'lix_change~1', \
               'lix_change_author~1', \
               'lix_change_set~1', \
               'lix_commit~1', \
               'lix_version_pointer~1', \
               'lix_change_set_element~1', \
               'lix_commit_edge~1'\
             ) \
               AND schema_key = 'lix_stored_schema' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
             ORDER BY entity_id",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(result.rows.clone());
    assert_eq!(result.rows.len(), 9);

    let mut seen_schema_keys = BTreeSet::new();
    for row in result.rows {
        let entity_id = match &row[0] {
            lix_engine::Value::Text(value) => value.clone(),
            other => panic!("expected text entity_id, got {other:?}"),
        };
        let snapshot_content = match &row[1] {
            lix_engine::Value::Text(value) => value.clone(),
            other => panic!("expected text snapshot_content, got {other:?}"),
        };
        let parsed: serde_json::Value = serde_json::from_str(&snapshot_content).unwrap();
        let schema = parsed
            .get("value")
            .expect("stored schema snapshot_content must include value");
        let schema_key = schema
            .get("x-lix-key")
            .and_then(serde_json::Value::as_str)
            .expect("schema must include x-lix-key");
        let schema_version = schema
            .get("x-lix-version")
            .and_then(serde_json::Value::as_str)
            .expect("schema must include x-lix-version");
        let plugin_key_override = schema
            .get("x-lix-override-lixcols")
            .and_then(serde_json::Value::as_object)
            .and_then(|overrides| overrides.get("lixcol_plugin_key"))
            .and_then(serde_json::Value::as_str)
            .expect("schema must include lixcol_plugin_key override");

        assert_eq!(schema_version, "1");
        assert_eq!(plugin_key_override, "\"lix\"");
        assert_eq!(entity_id, format!("{schema_key}~{schema_version}"));
        seen_schema_keys.insert(schema_key.to_string());
    }

    assert_eq!(
        seen_schema_keys,
        BTreeSet::from([
            "lix_change".to_string(),
            "lix_change_author".to_string(),
            "lix_change_set".to_string(),
            "lix_change_set_element".to_string(),
            "lix_commit".to_string(),
            "lix_commit_edge".to_string(),
            "lix_key_value".to_string(),
            "lix_stored_schema".to_string(),
            "lix_version_pointer".to_string(),
        ])
    );
});

simulation_test!(
    init_seeds_bootstrap_change_set_for_bootstrap_commit,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        let version_result = engine
            .execute(
                "SELECT commit_id \
             FROM lix_version \
             WHERE id = 'global' \
             LIMIT 1",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(version_result.rows.clone());
        assert_eq!(version_result.rows.len(), 1);
        let commit_id = match &version_result.rows[0][0] {
            lix_engine::Value::Text(value) => value.clone(),
            other => panic!("expected text commit_id for global version, got {other:?}"),
        };

        let change_set_result = engine
            .execute(
                "SELECT change_set_id \
             FROM lix_commit \
             WHERE id = $1 \
             LIMIT 1",
                &[lix_engine::Value::Text(commit_id)],
            )
            .await
            .unwrap();
        sim.assert_deterministic(change_set_result.rows.clone());
        assert_eq!(change_set_result.rows.len(), 1);
        let change_set_id = match &change_set_result.rows[0][0] {
            lix_engine::Value::Text(value) => value.clone(),
            other => panic!("expected text change_set_id for commit, got {other:?}"),
        };

        let change_set_exists = engine
            .execute(
                "SELECT 1 \
             FROM lix_change_set \
             WHERE id = $1 \
             LIMIT 1",
                &[lix_engine::Value::Text(change_set_id.clone())],
            )
            .await
            .unwrap();
        sim.assert_deterministic(change_set_exists.rows.clone());
        assert_eq!(change_set_exists.rows.len(), 1);
    }
);

simulation_test!(
    init_seeds_checkpoint_label_in_global_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        let result = engine
            .execute(
                "SELECT snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_label' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND snapshot_content IS NOT NULL",
                &[],
            )
            .await
            .unwrap();

        let mut has_checkpoint = false;
        for row in result.rows {
            let snapshot_content = match &row[0] {
                lix_engine::Value::Text(value) => value,
                other => panic!("expected text snapshot_content for lix_label, got {other:?}"),
            };
            let parsed: serde_json::Value =
                serde_json::from_str(snapshot_content).expect("lix_label snapshot must be JSON");
            if parsed.get("name").and_then(serde_json::Value::as_str) == Some("checkpoint") {
                has_checkpoint = true;
                break;
            }
        }

        assert!(
            has_checkpoint,
            "expected checkpoint label in global version"
        );
    }
);

simulation_test!(init_seeds_global_system_directories, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    let result = engine
        .execute(
            "SELECT path, hidden \
                 FROM lix_directory_by_version \
                 WHERE lixcol_version_id = 'global' \
                   AND path IN ('/.lix/', '/.lix/app_data/', '/.lix/plugins/') \
                 ORDER BY path",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(result.rows.clone());
    assert_eq!(result.rows.len(), 3);
    assert_eq!(
        result.rows[0][0],
        lix_engine::Value::Text("/.lix/".to_string())
    );
    let root_hidden = match &result.rows[0][1] {
        lix_engine::Value::Boolean(value) => *value,
        lix_engine::Value::Text(value) => value == "true",
        _ => false,
    };
    assert!(
        root_hidden,
        "expected hidden=true for /.lix/, got {:?}",
        result.rows[0][1]
    );
    assert_eq!(
        result.rows[1][0],
        lix_engine::Value::Text("/.lix/app_data/".to_string())
    );
    let app_data_hidden = match &result.rows[1][1] {
        lix_engine::Value::Boolean(value) => *value,
        lix_engine::Value::Text(value) => value == "true",
        _ => false,
    };
    assert!(
        app_data_hidden,
        "expected hidden=true for /.lix/app_data/, got {:?}",
        result.rows[1][1]
    );
    assert_eq!(
        result.rows[2][0],
        lix_engine::Value::Text("/.lix/plugins/".to_string())
    );
    let plugins_hidden = match &result.rows[2][1] {
        lix_engine::Value::Boolean(value) => *value,
        lix_engine::Value::Text(value) => value == "true",
        _ => false,
    };
    assert!(
        plugins_hidden,
        "expected hidden=true for /.lix/plugins/, got {:?}",
        result.rows[2][1]
    );

    let active_result = engine
        .execute(
            "SELECT path, hidden \
                 FROM lix_directory \
                 WHERE path IN ('/.lix/', '/.lix/app_data/', '/.lix/plugins/') \
                 ORDER BY path",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(active_result.rows.clone());
    assert_eq!(active_result.rows.len(), 3);
    assert_eq!(
        active_result.rows[0][0],
        lix_engine::Value::Text("/.lix/".to_string())
    );
    let active_root_hidden = match &active_result.rows[0][1] {
        lix_engine::Value::Boolean(value) => *value,
        lix_engine::Value::Text(value) => value == "true",
        _ => false,
    };
    assert!(
        active_root_hidden,
        "expected hidden=true for /.lix/ in lix_directory, got {:?}",
        active_result.rows[0][1]
    );
    assert_eq!(
        active_result.rows[1][0],
        lix_engine::Value::Text("/.lix/app_data/".to_string())
    );
    let active_app_data_hidden = match &active_result.rows[1][1] {
        lix_engine::Value::Boolean(value) => *value,
        lix_engine::Value::Text(value) => value == "true",
        _ => false,
    };
    assert!(
        active_app_data_hidden,
        "expected hidden=true for /.lix/app_data/ in lix_directory, got {:?}",
        active_result.rows[1][1]
    );
    assert_eq!(
        active_result.rows[2][0],
        lix_engine::Value::Text("/.lix/plugins/".to_string())
    );
    let active_plugins_hidden = match &active_result.rows[2][1] {
        lix_engine::Value::Boolean(value) => *value,
        lix_engine::Value::Text(value) => value == "true",
        _ => false,
    };
    assert!(
        active_plugins_hidden,
        "expected hidden=true for /.lix/plugins/ in lix_directory, got {:?}",
        active_result.rows[2][1]
    );
});
