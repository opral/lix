mod support;

use lix_engine::Value;
use support::simulation_test::{default_simulations, run_simulation_test, SimulationArgs};

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

#[tokio::test]
async fn lix_version_seeded_main_id_is_deterministic_across_backends() {
    run_simulation_test(
        default_simulations(),
        run_lix_version_seeded_main_id_deterministic,
    )
    .await;
}

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
                 id, name, inherits_from_version_id, commit_id, working_commit_id, \
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
        assert_non_empty_text(&row[4]);
        assert_text(&row[5], "lix_version");
        assert_text(&row[6], "lix");
        assert_text(&row[7], "global");
        assert_text(&row[8], "lix");
        assert_text(&row[9], "1");
        assert_eq!(row[10], Value::Integer(0));
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
             id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
             ) VALUES (\
             'version-a', 'Version A', NULL, 0, 'commit-a', 'working-a'\
             )",
                &[],
            )
            .await
            .unwrap();

        let result = engine
            .execute(
                "SELECT \
             id, name, inherits_from_version_id, hidden, commit_id, working_commit_id \
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
        assert_text(&row[5], "working-a");

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

simulation_test!(
    lix_version_insert_requires_explicit_tip_columns,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let error = engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id\
                 ) VALUES (\
                 'version-missing-tip', 'Version Missing Tip', NULL, 0, 'commit-missing-tip'\
                 )",
                &[],
            )
            .await
            .expect_err("insert should require working_commit_id");

        assert!(
            error
                .message
                .contains("lix_version insert requires column 'working_commit_id'"),
            "unexpected error message: {}",
            error.message
        );
    }
);

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
             id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
             ) VALUES (\
             'version-b', 'Version B', NULL, 0, 'commit-b', 'working-b'\
             )",
                &[],
            )
            .await
            .unwrap();

        engine
        .execute(
            "UPDATE lix_version \
             SET name = 'Version B2', hidden = 1, commit_id = 'commit-b2', working_commit_id = 'working-b2' \
             WHERE id = 'version-b'",
            &[],
        )
        .await
        .unwrap();

        let result = engine
            .execute(
                "SELECT \
             id, name, hidden, commit_id, working_commit_id \
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
        assert_text(&row[4], "working-b2");
    }
);

simulation_test!(
    lix_version_update_tip_requires_both_commit_and_working_commit,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
             id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
             ) VALUES (\
             'version-tip-contract', 'Version Tip Contract', NULL, 0, 'commit-tip-0', 'working-tip-0'\
             )",
                &[],
            )
            .await
            .unwrap();

        let error = engine
            .execute(
                "UPDATE lix_version \
                 SET commit_id = 'commit-tip-1' \
                 WHERE id = 'version-tip-contract'",
                &[],
            )
            .await
            .expect_err("tip update should require both commit fields");

        assert!(
            error
                .message
                .contains("must set both commit_id and working_commit_id together"),
            "unexpected error message: {}",
            error.message
        );
    }
);

simulation_test!(lix_version_update_supports_placeholders, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                 ) VALUES (\
                 'version-ph', 'Version PH', NULL, 0, 'commit-ph', 'working-ph'\
                 )",
            &[],
        )
        .await
        .unwrap();

    engine
        .execute(
            "UPDATE lix_version \
                 SET name = ?, commit_id = ?, working_commit_id = ? \
                 WHERE id = ?",
            &[
                Value::Text("Version PH2".to_string()),
                Value::Text("commit-ph2".to_string()),
                Value::Text("working-ph2".to_string()),
                Value::Text("version-ph".to_string()),
            ],
        )
        .await
        .unwrap();

    let result = engine
        .execute(
            "SELECT id, name, commit_id, working_commit_id \
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
    assert_text(&row[3], "working-ph2");
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
             id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
             ) VALUES (\
             'version-c', 'Version C', NULL, 0, 'commit-c', 'working-c'\
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
             id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
             ) VALUES (\
             'version-pd', 'Version PD', NULL, 0, 'commit-pd', 'working-pd'\
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
                 id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                 ) VALUES (\
                 'version-direct', 'version-direct', NULL, 0, 'commit-direct', 'working-direct'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let before_version = engine
            .execute(
                "SELECT id, name, commit_id, working_commit_id \
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
                "SELECT id, name, commit_id, working_commit_id \
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
        assert_eq!(after_version.rows[0][3], before_version.rows[0][3]);

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
                 id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                 ) VALUES (\
                 'version-state', 'version-state', NULL, 0, 'commit-state', 'working-state'\
                 )",
                &[],
            )
            .await
            .unwrap();
        register_test_state_schema(&engine).await;

        let before_version = engine
            .execute(
                "SELECT id, name, commit_id, working_commit_id \
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
                "SELECT id, name, commit_id, working_commit_id \
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
        assert_eq!(after_version.rows[0][3], before_version.rows[0][3]);
    }
);

simulation_test!(
    lix_version_enforces_unique_working_commit_id_on_insert,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                 ) VALUES (\
                 'v-unique-1', 'v-unique-1', NULL, 0, 'commit-unique-1', 'working-unique-1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let duplicate_error = engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                 ) VALUES (\
                 'v-unique-2', 'v-unique-2', NULL, 0, 'commit-unique-2', 'working-unique-1'\
                 )",
                &[],
            )
            .await
            .expect_err("duplicate working_commit_id should fail");
        assert!(
            duplicate_error
                .message
                .contains("Unique constraint violation"),
            "unexpected error message: {}",
            duplicate_error.message
        );
        assert!(
            duplicate_error.message.contains("working_commit_id"),
            "unexpected error message: {}",
            duplicate_error.message
        );
        assert!(
            duplicate_error.message.contains("working-unique-1"),
            "unexpected error message: {}",
            duplicate_error.message
        );

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                 ) VALUES (\
                 'v-unique-3', 'v-unique-3', NULL, 0, 'commit-unique-3', 'working-unique-3'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let versions = engine
            .execute(
                "SELECT id \
                 FROM lix_version \
                 WHERE id IN ('v-unique-1', 'v-unique-2', 'v-unique-3') \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(versions.rows.len(), 2);
        assert_text(&versions.rows[0][0], "v-unique-1");
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
                 id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                 ) VALUES (\
                 'v-lenient', 'v-lenient', NULL, 0, 'does_not_exist', 'working-lenient'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let inserted = engine
            .execute(
                "SELECT id, commit_id, working_commit_id \
                 FROM lix_version \
                 WHERE id = 'v-lenient'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(inserted.rows.len(), 1);
        assert_text(&inserted.rows[0][0], "v-lenient");
        assert_text(&inserted.rows[0][1], "does_not_exist");
        assert_text(&inserted.rows[0][2], "working-lenient");
    }
);
