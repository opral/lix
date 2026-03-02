mod support;
use std::collections::BTreeSet;

fn text_value(value: &lix_engine::Value, field: &str) -> String {
    match value {
        lix_engine::Value::Text(value) => value.clone(),
        other => panic!("expected text value for {field}, got {other:?}"),
    }
}

fn i64_value(value: &lix_engine::Value, field: &str) -> i64 {
    match value {
        lix_engine::Value::Integer(value) => *value,
        lix_engine::Value::Text(value) => value.parse::<i64>().unwrap_or_else(|error| {
            panic!("expected i64 text for {field}, got '{value}': {error}")
        }),
        other => panic!("expected i64 value for {field}, got {other:?}"),
    }
}

async fn active_version_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let active = engine
        .execute(
            "SELECT version_id \
             FROM lix_active_version \
             ORDER BY id \
             LIMIT 1",
            &[],
        )
        .await
        .expect("active version query should succeed");
    assert_eq!(active.rows.len(), 1);
    text_value(&active.rows[0][0], "active_version.version_id")
}

async fn checkpoint_label_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let rows = engine
        .execute(
            "SELECT id \
             FROM lix_label \
             WHERE name = 'checkpoint' \
             LIMIT 1",
            &[],
        )
        .await
        .expect("checkpoint label query should succeed");
    assert_eq!(rows.rows.len(), 1);
    text_value(&rows.rows[0][0], "lix_label.id")
}

async fn insert_untracked_snapshot(
    engine: &support::simulation_test::SimulationEngine,
    entity_id: &str,
    schema_key: &str,
    snapshot_content: &str,
    created_at: &str,
) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_untracked (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, created_at, updated_at\
             ) VALUES ($1, $2, 'lix', 'global', 'lix', $3, NULL, '1', $4, $4)",
            &[
                lix_engine::Value::Text(entity_id.to_string()),
                lix_engine::Value::Text(schema_key.to_string()),
                lix_engine::Value::Text(snapshot_content.to_string()),
                lix_engine::Value::Text(created_at.to_string()),
            ],
        )
        .await
        .expect("insert into lix_internal_state_untracked should succeed");
}

async fn insert_commit(
    engine: &support::simulation_test::SimulationEngine,
    commit_id: &str,
    change_set_id: &str,
    created_at: &str,
) {
    let snapshot = serde_json::json!({
        "id": commit_id,
        "change_set_id": change_set_id,
        "parent_commit_ids": [],
        "change_ids": [],
    })
    .to_string();
    insert_untracked_snapshot(engine, commit_id, "lix_commit", &snapshot, created_at).await;
}

async fn label_commit_as_checkpoint(
    engine: &support::simulation_test::SimulationEngine,
    commit_id: &str,
    checkpoint_label_id: &str,
    created_at: &str,
) {
    let entity_label_id = format!("{commit_id}~lix_commit~lix~{checkpoint_label_id}");
    let snapshot = serde_json::json!({
        "id": entity_label_id,
        "entity_id": commit_id,
        "schema_key": "lix_commit",
        "file_id": "lix",
        "label_id": checkpoint_label_id,
    })
    .to_string();
    insert_untracked_snapshot(
        engine,
        &entity_label_id,
        "lix_entity_label",
        &snapshot,
        created_at,
    )
    .await;
}

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
    init_seeds_per_version_commit_ids_and_last_checkpoint_pointers,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");

        engine.init().await.unwrap();

        let active_version = engine
            .execute(
                "SELECT version_id \
                 FROM lix_active_version \
                 ORDER BY id \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(active_version.rows.len(), 1);
        let main_version_id = text_value(&active_version.rows[0][0], "version_id");
        assert_ne!(main_version_id, "global");

        let versions = engine
            .execute(
                "SELECT id, commit_id \
                 FROM lix_version \
                 WHERE id IN ('global', $1) \
                 ORDER BY id",
                &[lix_engine::Value::Text(main_version_id.clone())],
            )
            .await
            .unwrap();
        sim.assert_deterministic(versions.rows.clone());
        assert_eq!(
            versions.rows.len(),
            2,
            "expected global + main version rows"
        );
        let versions_before_second_init = versions.rows.clone();

        let baselines = engine
            .execute(
                "SELECT version_id, checkpoint_commit_id \
                 FROM lix_internal_last_checkpoint \
                 WHERE version_id IN ('global', $1) \
                 ORDER BY version_id",
                &[lix_engine::Value::Text(main_version_id.clone())],
            )
            .await
            .unwrap();
        sim.assert_deterministic(baselines.rows.clone());
        assert_eq!(
            baselines.rows.len(),
            2,
            "expected baseline pointer rows for global + main"
        );
        let baselines_before_second_init = baselines.rows.clone();

        for version_row in &versions.rows {
            let version_id = text_value(&version_row[0], "id");
            let commit_id = text_value(&version_row[1], "commit_id");
            assert!(
                !commit_id.is_empty(),
                "version '{version_id}' must have commit_id"
            );

            let baseline_commit_id = baselines
                .rows
                .iter()
                .find_map(|row| {
                    if text_value(&row[0], "version_id") == version_id {
                        Some(text_value(&row[1], "checkpoint_commit_id"))
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| panic!("missing baseline pointer for version '{version_id}'"));

            assert_eq!(
                baseline_commit_id, commit_id,
                "seeded baseline must point to seeded tip commit for version '{version_id}'"
            );

            let commit_exists = engine
                .execute(
                    "SELECT COUNT(*) \
                     FROM lix_commit \
                     WHERE id = $1",
                    &[lix_engine::Value::Text(commit_id.clone())],
                )
                .await
                .unwrap();
            assert_eq!(
                i64_value(&commit_exists.rows[0][0], "commit_count"),
                1,
                "commit '{commit_id}' must exist exactly once"
            );
        }

        engine.init().await.unwrap();
        let versions_after_second_init = engine
            .execute(
                "SELECT id, commit_id \
                 FROM lix_version \
                 WHERE id IN ('global', $1) \
                 ORDER BY id",
                &[lix_engine::Value::Text(main_version_id.clone())],
            )
            .await
            .unwrap();
        sim.assert_deterministic(versions_after_second_init.rows.clone());
        assert_eq!(
            versions_after_second_init.rows, versions_before_second_init,
            "init() should be idempotent for seeded version pointers"
        );

        let baselines_after_second_init = engine
            .execute(
                "SELECT version_id, checkpoint_commit_id \
                 FROM lix_internal_last_checkpoint \
                 WHERE version_id IN ('global', $1) \
                 ORDER BY version_id",
                &[lix_engine::Value::Text(main_version_id)],
            )
            .await
            .unwrap();
        sim.assert_deterministic(baselines_after_second_init.rows.clone());
        assert_eq!(
            baselines_after_second_init.rows, baselines_before_second_init,
            "init() should be idempotent for seeded baseline pointers"
        );
    }
);

simulation_test!(
    init_rebuild_last_checkpoint_prefers_nearest_checkpoint_ancestor,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.unwrap();

        let version_id = active_version_id(&engine).await;
        let checkpoint_label_id = checkpoint_label_id(&engine).await;

        let tip = "rebuild-depth-tip";
        let near = "rebuild-depth-near";
        let far = "rebuild-depth-far";

        insert_commit(
            &engine,
            tip,
            "rebuild-depth-cs-tip",
            "2025-01-10T00:00:00.000Z",
        )
        .await;
        insert_commit(
            &engine,
            near,
            "rebuild-depth-cs-near",
            "2025-01-01T00:00:00.000Z",
        )
        .await;
        insert_commit(
            &engine,
            far,
            "rebuild-depth-cs-far",
            "2025-02-01T00:00:00.000Z",
        )
        .await;

        label_commit_as_checkpoint(
            &engine,
            near,
            &checkpoint_label_id,
            "2025-01-20T00:00:00.000Z",
        )
        .await;
        label_commit_as_checkpoint(
            &engine,
            far,
            &checkpoint_label_id,
            "2025-01-20T00:00:00.000Z",
        )
        .await;

        engine
            .execute(
                "UPDATE lix_version SET commit_id = $1 WHERE id = $2",
                &[
                    lix_engine::Value::Text(tip.to_string()),
                    lix_engine::Value::Text(version_id.clone()),
                ],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_commit_ancestry (commit_id, ancestor_id, depth) VALUES \
                 ($1, $1, 0), \
                 ($1, $2, 1), \
                 ($1, $3, 2)",
                &[
                    lix_engine::Value::Text(tip.to_string()),
                    lix_engine::Value::Text(near.to_string()),
                    lix_engine::Value::Text(far.to_string()),
                ],
            )
            .await
            .unwrap();

        engine.init().await.unwrap();

        let baseline = engine
            .execute(
                "SELECT checkpoint_commit_id \
                 FROM lix_internal_last_checkpoint \
                 WHERE version_id = $1",
                &[lix_engine::Value::Text(version_id)],
            )
            .await
            .unwrap();
        assert_eq!(baseline.rows.len(), 1);
        assert_eq!(
            text_value(&baseline.rows[0][0], "checkpoint_commit_id"),
            near,
            "nearest checkpoint ancestor should win over deeper ancestors even if deeper is newer"
        );
    }
);

simulation_test!(
    init_rebuild_last_checkpoint_tie_breaks_by_created_at_then_ancestor_id,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.unwrap();

        let version_id = active_version_id(&engine).await;
        let checkpoint_label_id = checkpoint_label_id(&engine).await;

        let tip = "rebuild-tiebreak-tip";
        let old = "rebuild-tiebreak-old";
        let tie_low = "rebuild-tiebreak-b";
        let tie_high = "rebuild-tiebreak-z";

        insert_commit(
            &engine,
            tip,
            "rebuild-tiebreak-cs-tip",
            "2025-03-01T00:00:00.000Z",
        )
        .await;
        insert_commit(
            &engine,
            old,
            "rebuild-tiebreak-cs-old",
            "2025-01-01T00:00:00.000Z",
        )
        .await;
        insert_commit(
            &engine,
            tie_low,
            "rebuild-tiebreak-cs-b",
            "2025-02-01T00:00:00.000Z",
        )
        .await;
        insert_commit(
            &engine,
            tie_high,
            "rebuild-tiebreak-cs-z",
            "2025-02-01T00:00:00.000Z",
        )
        .await;

        label_commit_as_checkpoint(
            &engine,
            old,
            &checkpoint_label_id,
            "2025-01-10T00:00:00.000Z",
        )
        .await;
        label_commit_as_checkpoint(
            &engine,
            tie_low,
            &checkpoint_label_id,
            "2025-02-10T00:00:00.000Z",
        )
        .await;
        label_commit_as_checkpoint(
            &engine,
            tie_high,
            &checkpoint_label_id,
            "2025-02-10T00:00:00.000Z",
        )
        .await;

        engine
            .execute(
                "UPDATE lix_version SET commit_id = $1 WHERE id = $2",
                &[
                    lix_engine::Value::Text(tip.to_string()),
                    lix_engine::Value::Text(version_id.clone()),
                ],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_commit_ancestry (commit_id, ancestor_id, depth) VALUES \
                 ($1, $1, 0), \
                 ($1, $2, 1), \
                 ($1, $3, 1), \
                 ($1, $4, 1)",
                &[
                    lix_engine::Value::Text(tip.to_string()),
                    lix_engine::Value::Text(old.to_string()),
                    lix_engine::Value::Text(tie_low.to_string()),
                    lix_engine::Value::Text(tie_high.to_string()),
                ],
            )
            .await
            .unwrap();

        engine.init().await.unwrap();

        let baseline = engine
            .execute(
                "SELECT checkpoint_commit_id \
                 FROM lix_internal_last_checkpoint \
                 WHERE version_id = $1",
                &[lix_engine::Value::Text(version_id)],
            )
            .await
            .unwrap();
        assert_eq!(baseline.rows.len(), 1);
        assert_eq!(
            text_value(&baseline.rows[0][0], "checkpoint_commit_id"),
            tie_high,
            "with equal depth, newer created_at wins; ties then resolve by ancestor_id DESC"
        );
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
