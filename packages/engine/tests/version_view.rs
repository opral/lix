mod support;

use lix_engine::Value;
use support::simulation_test::SimulationArgs;

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

fn assert_non_empty_text(value: &Value) {
    match value {
        Value::Text(actual) => assert!(
            !actual.is_empty(),
            "expected non-empty text value, got empty string"
        ),
        other => panic!("expected text value, got {other:?}"),
    }
}

fn assert_bool(value: &Value, expected: bool) {
    match value {
        Value::Boolean(actual) => assert_eq!(*actual, expected),
        Value::Integer(actual) => assert_eq!(*actual != 0, expected),
        Value::Text(actual) => {
            let normalized = actual.trim().to_ascii_lowercase();
            let parsed = match normalized.as_str() {
                "1" | "true" => true,
                "0" | "false" => false,
                _ => panic!("expected boolean-compatible text, got '{actual}'"),
            };
            assert_eq!(parsed, expected);
        }
        other => panic!("expected boolean-compatible value, got {other:?}"),
    }
}

async fn register_test_state_schema(engine: &support::simulation_test::SimulationEngine) {
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

async fn run_lix_version_seeded_main_id_deterministic(sim: SimulationArgs) {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.init().await.unwrap();

    let result = engine
        .execute(
            "SELECT id, name, inherits_from_version_id \
             FROM lix_version \
             WHERE name = 'main'",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    let id = match &row[0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected text id, got {other:?}"),
    };
    assert!(!id.is_empty());
    assert_ne!(id, "main");
    sim.assert_deterministic(id);
    assert_text(&row[1], "main");
    assert_text(&row[2], "global");
}

simulation_test!(
    lix_version_seeded_main_id_is_deterministic_across_backends,
    |sim| async move {
        run_lix_version_seeded_main_id_deterministic(sim).await;
    }
);

simulation_test!(
    lix_version_select_reads_seeded_global_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let result = engine
            .execute(
                "SELECT \
                 id, name, inherits_from_version_id, commit_id, \
                 schema_key, file_id, version_id, plugin_key, schema_version, untracked \
                 FROM lix_version \
                 WHERE id = 'global'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_text(&row[0], "global");
        assert_text(&row[1], "global");
        assert_eq!(row[2], Value::Null);
        assert_non_empty_text(&row[3]);
        assert_text(&row[4], "lix_version");
        assert_text(&row[5], "lix");
        assert_text(&row[6], "global");
        assert_text(&row[7], "lix");
        assert_text(&row[8], "1");
        assert_bool(&row[9], false);
    }
);

simulation_test!(
    lix_version_select_reads_seeded_main_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let result = engine
            .execute(
                "SELECT \
                 id, name, inherits_from_version_id \
                 FROM lix_version \
                 WHERE name = 'main'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_non_empty_text(&row[0]);
        assert_ne!(row[0], Value::Text("main".to_string()));
        assert_text(&row[1], "main");
        assert_text(&row[2], "global");
    }
);

simulation_test!(
    lix_version_insert_routes_to_descriptor_and_tip,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
             id, name, inherits_from_version_id, hidden, commit_id\
             ) VALUES (\
             'version-a', 'Version A', NULL, false, 'commit-a'\
             )",
                &[],
            )
            .await
            .unwrap();

        let result = engine
            .execute(
                "SELECT \
             id, name, inherits_from_version_id, hidden, commit_id \
             FROM lix_version \
             WHERE id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_text(&row[0], "version-a");
        assert_text(&row[1], "Version A");
        assert_eq!(row[2], Value::Null);
        assert_bool(&row[3], false);
        assert_text(&row[4], "commit-a");

        let vtable_rows = engine
            .execute(
                "SELECT schema_key \
             FROM lix_internal_state_vtable \
             WHERE entity_id = 'version-a' \
               AND schema_key IN ('lix_version_descriptor', 'lix_version_pointer') \
             ORDER BY schema_key",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(vtable_rows.rows.len(), 2);
        assert_text(&vtable_rows.rows[0][0], "lix_version_descriptor");
        assert_text(&vtable_rows.rows[1][0], "lix_version_pointer");
    }
);

simulation_test!(lix_version_insert_requires_commit_id, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    let error = engine
        .execute(
            "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden\
                 ) VALUES (\
                 'version-missing-tip', 'Version Missing Tip', NULL, false\
                 )",
            &[],
        )
        .await
        .expect_err("insert should require commit_id");

    assert!(
        error
            .description
            .contains("lix_version insert requires column 'commit_id'"),
        "unexpected error message: {}",
        error.description
    );
});

simulation_test!(
    lix_version_update_routes_to_descriptor_and_tip,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
             id, name, inherits_from_version_id, hidden, commit_id\
             ) VALUES (\
             'version-b', 'Version B', NULL, false, 'commit-b'\
             )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "UPDATE lix_version \
             SET name = 'Version B2', hidden = true, commit_id = 'commit-b2' \
             WHERE id = 'version-b'",
                &[],
            )
            .await
            .unwrap();

        let result = engine
            .execute(
                "SELECT \
             id, name, hidden, commit_id \
             FROM lix_version \
             WHERE id = 'version-b'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_text(&row[0], "version-b");
        assert_text(&row[1], "Version B2");
        assert_bool(&row[2], true);
        assert_text(&row[3], "commit-b2");
    }
);

simulation_test!(lix_version_update_allows_commit_id_only, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_version (\
             id, name, inherits_from_version_id, hidden, commit_id\
             ) VALUES (\
             'version-tip-contract', 'Version Tip Contract', NULL, false, 'commit-tip-0'\
             )",
            &[],
        )
        .await
        .unwrap();

    engine
        .execute(
            "UPDATE lix_version \
                 SET commit_id = 'commit-tip-1' \
                 WHERE id = 'version-tip-contract'",
            &[],
        )
        .await
        .expect("commit_id-only tip update should succeed");

    let rows = engine
        .execute(
            "SELECT commit_id FROM lix_version WHERE id = 'version-tip-contract'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_text(&rows.rows[0][0], "commit-tip-1");
});

simulation_test!(lix_version_update_supports_placeholders, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id\
                 ) VALUES (\
                 'version-ph', 'Version PH', NULL, false, 'commit-ph'\
                 )",
            &[],
        )
        .await
        .unwrap();

    engine
        .execute(
            "UPDATE lix_version \
                 SET name = ?, commit_id = ? \
                 WHERE id = ?",
            &[
                Value::Text("Version PH2".to_string()),
                Value::Text("commit-ph2".to_string()),
                Value::Text("version-ph".to_string()),
            ],
        )
        .await
        .unwrap();

    let result = engine
        .execute(
            "SELECT id, name, commit_id \
                 FROM lix_version \
                 WHERE id = 'version-ph'",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert_text(&row[0], "version-ph");
    assert_text(&row[1], "Version PH2");
    assert_text(&row[2], "commit-ph2");
});

simulation_test!(lix_version_delete_routes_to_tombstones, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_version (\
             id, name, inherits_from_version_id, hidden, commit_id\
             ) VALUES (\
             'version-c', 'Version C', NULL, false, 'commit-c'\
             )",
            &[],
        )
        .await
        .unwrap();

    engine
        .execute("DELETE FROM lix_version WHERE id = 'version-c'", &[])
        .await
        .unwrap();

    let version_rows = engine
        .execute("SELECT id FROM lix_version WHERE id = 'version-c'", &[])
        .await
        .unwrap();
    assert_eq!(version_rows.rows.len(), 0);

    let deleted_rows = engine
        .execute(
            "SELECT schema_key, snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE entity_id = 'version-c' \
               AND schema_key IN ('lix_version_descriptor', 'lix_version_pointer') \
             ORDER BY schema_key",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(deleted_rows.rows.len(), 2);
    assert_text(&deleted_rows.rows[0][0], "lix_version_descriptor");
    assert_eq!(deleted_rows.rows[0][1], Value::Null);
    assert_text(&deleted_rows.rows[1][0], "lix_version_pointer");
    assert_eq!(deleted_rows.rows[1][1], Value::Null);
});

simulation_test!(lix_version_delete_supports_placeholders, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_version (\
             id, name, inherits_from_version_id, hidden, commit_id\
             ) VALUES (\
             'version-pd', 'Version PD', NULL, false, 'commit-pd'\
             )",
            &[],
        )
        .await
        .unwrap();

    engine
        .execute(
            "DELETE FROM lix_version WHERE id = ?",
            &[Value::Text("version-pd".to_string())],
        )
        .await
        .unwrap();

    let result = engine
        .execute("SELECT id FROM lix_version WHERE id = 'version-pd'", &[])
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 0);
});

simulation_test!(
    lix_version_direct_mutation_does_not_duplicate_entries,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id\
                 ) VALUES (\
                 'version-direct', 'version-direct', NULL, false, 'commit-direct'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let before_version = engine
            .execute(
                "SELECT id, name, commit_id \
                 FROM lix_version \
                 WHERE id = 'version-direct'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(before_version.rows.len(), 1);

        let global_before = engine
            .execute("SELECT commit_id FROM lix_version WHERE id = 'global'", &[])
            .await
            .unwrap();
        assert_eq!(global_before.rows.len(), 1);
        let global_before_commit = match &global_before.rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected text commit_id, got {other:?}"),
        };

        engine
            .execute(
                "UPDATE lix_version SET name = 'version-direct-renamed' WHERE id = 'version-direct'",
                &[],
            )
            .await
            .unwrap();

        let after_version = engine
            .execute(
                "SELECT id, name, commit_id \
                 FROM lix_version \
                 WHERE id = 'version-direct'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(after_version.rows.len(), 1);
        assert_text(&after_version.rows[0][0], "version-direct");
        assert_text(&after_version.rows[0][1], "version-direct-renamed");
        assert_eq!(after_version.rows[0][2], before_version.rows[0][2]);

        let global_after = engine
            .execute("SELECT commit_id FROM lix_version WHERE id = 'global'", &[])
            .await
            .unwrap();
        assert_eq!(global_after.rows.len(), 1);
        let global_after_commit = match &global_after.rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected text commit_id, got {other:?}"),
        };
        assert_ne!(global_after_commit, global_before_commit);
    }
);

simulation_test!(
    lix_version_state_mutation_does_not_duplicate_entries,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id\
                 ) VALUES (\
                 'version-state', 'version-state', NULL, false, 'commit-state'\
                 )",
                &[],
            )
            .await
            .unwrap();
        register_test_state_schema(&engine).await;

        let before_version = engine
            .execute(
                "SELECT id, name, commit_id \
                 FROM lix_version \
                 WHERE id = 'version-state'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(before_version.rows.len(), 1);

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'state-entity-1', 'test_schema', 'file-state', 'version-state', 'lix', '{\"key\":\"value\"}', '1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let after_version = engine
            .execute(
                "SELECT id, name, commit_id \
                 FROM lix_version \
                 WHERE id = 'version-state'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(after_version.rows.len(), 1);
        assert_text(&after_version.rows[0][0], "version-state");
        assert_text(&after_version.rows[0][1], "version-state");
        assert_ne!(after_version.rows[0][2], before_version.rows[0][2]);
    }
);

simulation_test!(
    lix_version_duplicate_id_insert_overwrites_existing_row,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id\
                 ) VALUES (\
                 'v-unique-1', 'v-unique-1', NULL, false, 'commit-unique-1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id\
                 ) VALUES (\
                 'v-unique-1', 'v-unique-2', NULL, false, 'commit-unique-2'\
                 )",
                &[],
            )
            .await
            .expect("duplicate id insert should overwrite existing row");

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id\
                 ) VALUES (\
                 'v-unique-3', 'v-unique-3', NULL, false, 'commit-unique-3'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let versions = engine
            .execute(
                "SELECT id, name, commit_id \
                 FROM lix_version \
                 WHERE id IN ('v-unique-1', 'v-unique-2', 'v-unique-3') \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(versions.rows.len(), 2);
        assert_text(&versions.rows[0][0], "v-unique-1");
        assert_text(&versions.rows[0][1], "v-unique-2");
        assert_text(&versions.rows[0][2], "commit-unique-2");
        assert_text(&versions.rows[1][0], "v-unique-3");
    }
);

simulation_test!(
    lix_version_insert_commit_id_fk_is_lenient_materialized_mode,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id\
                 ) VALUES (\
                 'v-lenient', 'v-lenient', NULL, false, 'does_not_exist'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let inserted = engine
            .execute(
                "SELECT id, commit_id \
                 FROM lix_version \
                 WHERE id = 'v-lenient'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(inserted.rows.len(), 1);
        assert_text(&inserted.rows[0][0], "v-lenient");
        assert_text(&inserted.rows[0][1], "does_not_exist");
    }
);

simulation_test!(
    lix_version_sql_insert_creates_last_checkpoint_pointer,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id\
                 ) VALUES (\
                 'version-baseline-insert', 'version-baseline-insert', 'global', false, 'commit-baseline-insert'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let baseline = engine
            .execute(
                "SELECT checkpoint_commit_id \
                 FROM lix_internal_last_checkpoint \
                 WHERE version_id = 'version-baseline-insert'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(baseline.rows.len(), 1, "missing baseline pointer row");
        assert_text(&baseline.rows[0][0], "commit-baseline-insert");
    }
);

simulation_test!(
    lix_version_sql_delete_removes_last_checkpoint_pointer,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .create_version(lix_engine::CreateVersionOptions {
                id: Some("version-baseline-delete".to_string()),
                name: Some("version-baseline-delete".to_string()),
                inherits_from_version_id: Some("global".to_string()),
                hidden: false,
            })
            .await
            .unwrap();

        let baseline_before_delete = engine
            .execute(
                "SELECT checkpoint_commit_id \
                 FROM lix_internal_last_checkpoint \
                 WHERE version_id = 'version-baseline-delete'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            baseline_before_delete.rows.len(),
            1,
            "expected baseline row before SQL delete"
        );

        engine
            .execute(
                "DELETE FROM lix_version WHERE id = 'version-baseline-delete'",
                &[],
            )
            .await
            .unwrap();

        let baseline_after_delete = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_internal_last_checkpoint \
                 WHERE version_id = 'version-baseline-delete'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(baseline_after_delete.rows.len(), 1);
        let remaining = match &baseline_after_delete.rows[0][0] {
            Value::Integer(value) => *value,
            Value::Text(value) => value
                .parse::<i64>()
                .unwrap_or_else(|error| panic!("expected i64 text count, got '{value}': {error}")),
            other => panic!("expected integer count, got {other:?}"),
        };
        assert_eq!(remaining, 0, "baseline row should be deleted");
    }
);

simulation_test!(
    lix_version_sql_commit_id_update_does_not_move_last_checkpoint_pointer,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id\
                 ) VALUES (\
                 'version-baseline-frozen', 'version-baseline-frozen', 'global', false, 'commit-baseline-frozen-0'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let baseline_before = engine
            .execute(
                "SELECT checkpoint_commit_id \
                 FROM lix_internal_last_checkpoint \
                 WHERE version_id = 'version-baseline-frozen'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(baseline_before.rows.len(), 1, "missing baseline row");
        assert_text(&baseline_before.rows[0][0], "commit-baseline-frozen-0");

        engine
            .execute(
                "UPDATE lix_version \
                 SET commit_id = 'commit-baseline-frozen-1' \
                 WHERE id = 'version-baseline-frozen'",
                &[],
            )
            .await
            .unwrap();

        let version_tip = engine
            .execute(
                "SELECT commit_id \
                 FROM lix_version \
                 WHERE id = 'version-baseline-frozen'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(version_tip.rows.len(), 1);
        assert_text(&version_tip.rows[0][0], "commit-baseline-frozen-1");

        let baseline_after = engine
            .execute(
                "SELECT checkpoint_commit_id \
                 FROM lix_internal_last_checkpoint \
                 WHERE version_id = 'version-baseline-frozen'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(baseline_after.rows.len(), 1);
        assert_text(&baseline_after.rows[0][0], "commit-baseline-frozen-0");
    }
);
