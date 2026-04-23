use crate::support;

use lix_engine::Value;
use serde_json::json;
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

fn assert_non_text_bool(value: &Value, expected: bool) {
    match value {
        Value::Boolean(actual) => assert_eq!(*actual, expected),
        other => panic!("expected engine boolean value, got {other:?}"),
    }
}

async fn register_test_state_schema(engine: &support::simulation_test::SimulatedLix) {
    engine
        .register_schema(
            &serde_json::from_str::<serde_json::Value>(
                "{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}",
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn ensure_file_descriptor(
    engine: &support::simulation_test::SimulatedLix,
    version_id: &str,
    file_id: &str,
) {
    let existing = engine
        .execute(
            "SELECT entity_id \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_file_descriptor' \
               AND entity_id = $1 \
               AND version_id = $2 \
             LIMIT 1",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
            ],
        )
        .await
        .unwrap();
    if !existing.statements[0].rows.is_empty() {
        return;
    }

    let (name, extension) = file_id
        .rsplit_once('.')
        .map(|(name, extension)| (name, Some(extension)))
        .unwrap_or((file_id, None));
    let snapshot = json!({
        "id": file_id,
        "directory_id": null,
        "name": name,
        "extension": extension,
        "hidden": false
    })
    .to_string();

    engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             $1, 'lix_file_descriptor', NULL, $2, NULL, $3, '1'\
             )",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
                Value::Text(snapshot),
            ],
        )
        .await
        .unwrap();
}

async fn run_lix_version_seeded_main_id_deterministic(sim: SimulationArgs) {
    let engine = sim
        .boot_simulated_lix_deterministic()
        .await
        .expect("boot_simulated_lix_deterministic should succeed");
    engine.initialize().await.unwrap();

    let result = engine
        .execute(
            "SELECT id, name \
             FROM lix_version \
             WHERE name = 'main'",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(result.statements[0].rows.len(), 1);
    let row = &result.statements[0].rows[0];
    let id = match &row[0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected text id, got {other:?}"),
    };
    assert!(!id.is_empty());
    assert_ne!(id, "main");
    sim.assert_deterministic(id);
    assert_text(&row[1], "main");
}

simulation_test!(
    lix_version_seeded_main_id_is_deterministic_across_backends,
    |sim| async move {
        run_lix_version_seeded_main_id_deterministic(sim).await;
    }
);

simulation_test!(lix_version_hides_internal_global_lane, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.unwrap();

    let result = engine
        .execute(
            "SELECT id, name, hidden \
             FROM lix_version \
             WHERE id = 'global'",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(result.statements[0].rows.len(), 1);
    assert_text(&result.statements[0].rows[0][0], "global");
    assert_text(&result.statements[0].rows[0][1], "global");
    assert_bool(&result.statements[0].rows[0][2], true);
});

simulation_test!(
    lix_version_select_reads_seeded_main_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "SELECT \
                 id, name, hidden, commit_id \
                 FROM lix_version \
                 WHERE name = 'main'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(result.statements[0].rows.len(), 1);
        let row = &result.statements[0].rows[0];
        assert_non_empty_text(&row[0]);
        assert_ne!(row[0], Value::Text("main".to_string()));
        assert_text(&row[1], "main");
        assert_bool(&row[2], false);
        assert_non_empty_text(&row[3]);
    }
);

simulation_test!(
    lix_version_insert_routes_to_descriptor_and_tip,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
             id, name, hidden, commit_id\
             ) VALUES (\
             'version-a', 'Version A', false, 'commit-a'\
             )",
                &[],
            )
            .await
            .unwrap();

        let result = engine
            .execute(
                "SELECT \
             id, name, hidden, commit_id \
             FROM lix_version \
             WHERE id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(result.statements[0].rows.len(), 1);
        let row = &result.statements[0].rows[0];
        assert_text(&row[0], "version-a");
        assert_text(&row[1], "Version A");
        assert_bool(&row[2], false);
        assert_text(&row[3], "commit-a");

        let vtable_rows = engine
            .execute(
                "SELECT DISTINCT schema_key \
             FROM lix_state_by_version \
             WHERE entity_id = 'version-a' \
               AND schema_key IN ('lix_version_descriptor', 'lix_version_ref') \
               AND snapshot_content IS NOT NULL \
             ORDER BY schema_key",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(vtable_rows.statements[0].rows.len(), 2);
        assert_text(
            &vtable_rows.statements[0].rows[0][0],
            "lix_version_descriptor",
        );
        assert_text(&vtable_rows.statements[0].rows[1][0], "lix_version_ref");
    }
);

simulation_test!(lix_version_insert_requires_commit_id, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.unwrap();

    let error = engine
        .execute(
            "INSERT INTO lix_version (\
                 id, name, hidden\
                 ) VALUES (\
                 'version-missing-tip', 'Version Missing Tip', false\
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
             id, name, hidden, commit_id\
             ) VALUES (\
             'version-b', 'Version B', false, 'commit-b'\
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

        assert_eq!(result.statements[0].rows.len(), 1);
        let row = &result.statements[0].rows[0];
        assert_text(&row[0], "version-b");
        assert_text(&row[1], "Version B2");
        assert_bool(&row[2], true);
        assert_text(&row[3], "commit-b2");
    }
);

simulation_test!(lix_version_update_allows_commit_id_only, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_version (\
             id, name, hidden, commit_id\
             ) VALUES (\
             'version-tip-contract', 'Version Tip Contract', false, 'commit-tip-0'\
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
    assert_eq!(rows.statements[0].rows.len(), 1);
    assert_text(&rows.statements[0].rows[0][0], "commit-tip-1");
});

simulation_test!(lix_version_update_supports_placeholders, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_version (\
                 id, name, hidden, commit_id\
                 ) VALUES (\
                 'version-ph', 'Version PH', false, 'commit-ph'\
                 )",
            &[],
        )
        .await
        .unwrap();

    engine
        .execute(
            "UPDATE lix_version \
                 SET name = $1, commit_id = $2 \
                 WHERE id = $3",
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

    assert_eq!(result.statements[0].rows.len(), 1);
    let row = &result.statements[0].rows[0];
    assert_text(&row[0], "version-ph");
    assert_text(&row[1], "Version PH2");
    assert_text(&row[2], "commit-ph2");
});

simulation_test!(
    lix_version_update_supports_or_selector,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (id, name, hidden, commit_id) VALUES \
                 ('version-or-a', 'Version OR A', false, 'commit-or-a'), \
                 ('version-or-b', 'Version OR B', false, 'commit-or-b')",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "UPDATE lix_version \
                 SET hidden = true \
                 WHERE id = 'version-or-a' OR id = 'version-or-b'",
                &[],
            )
            .await
            .unwrap();

        let result = engine
            .execute(
                "SELECT id, hidden \
                 FROM lix_version \
                 WHERE id IN ('version-or-a', 'version-or-b') \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(result.statements[0].rows.len(), 2);
        assert_text(&result.statements[0].rows[0][0], "version-or-a");
        assert_bool(&result.statements[0].rows[0][1], true);
        assert_text(&result.statements[0].rows[1][0], "version-or-b");
        assert_bool(&result.statements[0].rows[1][1], true);
    }
);

simulation_test!(
    lix_version_hidden_projects_as_engine_boolean,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "SELECT hidden \
                 FROM lix_version \
                 WHERE name = 'main'",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(result.statements[0].rows.len(), 1);
        assert_non_text_bool(&result.statements[0].rows[0][0], false);
    }
);

simulation_test!(lix_version_delete_routes_to_tombstones, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_version (\
             id, name, hidden, commit_id\
             ) VALUES (\
             'version-c', 'Version C', false, 'commit-c'\
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
    assert_eq!(version_rows.statements[0].rows.len(), 0);

    let deleted_descriptor_rows = engine
        .execute(
            "SELECT schema_key, snapshot_content \
             FROM lix_change \
             WHERE entity_id = 'version-c' \
               AND schema_key = 'lix_version_descriptor' \
               AND snapshot_content IS NULL \
             ORDER BY created_at DESC, id DESC",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(deleted_descriptor_rows.statements[0].rows.len(), 1);
    assert_text(
        &deleted_descriptor_rows.statements[0].rows[0][0],
        "lix_version_descriptor",
    );
    assert_eq!(
        deleted_descriptor_rows.statements[0].rows[0][1],
        Value::Null
    );

    let version_ref_rows = engine
        .execute(
            "SELECT schema_key \
             FROM lix_state_by_version \
             WHERE entity_id = 'version-c' \
               AND schema_key = 'lix_version_ref'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(version_ref_rows.statements[0].rows.len(), 0);
});

simulation_test!(lix_version_delete_supports_placeholders, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.unwrap();

    engine
        .execute(
            "INSERT INTO lix_version (\
             id, name, hidden, commit_id\
             ) VALUES (\
             'version-pd', 'Version PD', false, 'commit-pd'\
             )",
            &[],
        )
        .await
        .unwrap();

    engine
        .execute(
            "DELETE FROM lix_version WHERE id = $1",
            &[Value::Text("version-pd".to_string())],
        )
        .await
        .unwrap();

    let result = engine
        .execute("SELECT id FROM lix_version WHERE id = 'version-pd'", &[])
        .await
        .unwrap();

    assert_eq!(result.statements[0].rows.len(), 0);
});

simulation_test!(
    lix_version_delete_supports_or_selector,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (id, name, hidden, commit_id) VALUES \
                 ('version-del-a', 'Version Del A', false, 'commit-del-a'), \
                 ('version-del-b', 'Version Del B', false, 'commit-del-b')",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "DELETE FROM lix_version \
                 WHERE id = 'version-del-a' OR id = 'version-del-b'",
                &[],
            )
            .await
            .unwrap();

        let result = engine
            .execute(
                "SELECT id \
                 FROM lix_version \
                 WHERE id IN ('version-del-a', 'version-del-b')",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(result.statements[0].rows.len(), 0);
    }
);

simulation_test!(
    lix_version_direct_mutation_does_not_duplicate_entries,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, hidden, commit_id\
                 ) VALUES (\
                 'version-direct', 'version-direct', false, 'commit-direct'\
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
        assert_eq!(before_version.statements[0].rows.len(), 1);

        engine
            .execute(
                "UPDATE lix_version SET name = 'version-direct-renamed' WHERE id = 'version-direct'", &[])
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
        assert_eq!(after_version.statements[0].rows.len(), 1);
        assert_text(&after_version.statements[0].rows[0][0], "version-direct");
        assert_text(
            &after_version.statements[0].rows[0][1],
            "version-direct-renamed",
        );
        assert_eq!(
            after_version.statements[0].rows[0][2],
            before_version.statements[0].rows[0][2]
        );
    }
);

simulation_test!(
    lix_version_state_mutation_does_not_duplicate_entries,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine.create_named_version("version-state").await.unwrap();
        register_test_state_schema(&engine).await;
        ensure_file_descriptor(&engine, "version-state", "file-state").await;

        let before_version = engine
            .execute(
                "SELECT id, name, commit_id \
                 FROM lix_version \
                 WHERE id = 'version-state'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(before_version.statements[0].rows.len(), 1);

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'state-entity-1', 'test_schema', 'file-state', 'version-state', NULL, '{\"key\":\"value\"}', '1'\
                 )", &[])
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
        assert_eq!(after_version.statements[0].rows.len(), 1);
        assert_text(&after_version.statements[0].rows[0][0], "version-state");
        assert_text(&after_version.statements[0].rows[0][1], "version-state");
        assert_ne!(
            after_version.statements[0].rows[0][2],
            before_version.statements[0].rows[0][2]
        );
    }
);

simulation_test!(
    lix_version_duplicate_id_insert_overwrites_existing_row,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, hidden, commit_id\
                 ) VALUES (\
                 'v-unique-1', 'v-unique-1', false, 'commit-unique-1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, hidden, commit_id\
                 ) VALUES (\
                 'v-unique-1', 'v-unique-2', false, 'commit-unique-2'\
                 )",
                &[],
            )
            .await
            .expect("duplicate id insert should overwrite existing row");

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, hidden, commit_id\
                 ) VALUES (\
                 'v-unique-3', 'v-unique-3', false, 'commit-unique-3'\
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
        assert_eq!(versions.statements[0].rows.len(), 2);
        assert_text(&versions.statements[0].rows[0][0], "v-unique-1");
        assert_text(&versions.statements[0].rows[0][1], "v-unique-2");
        assert_text(&versions.statements[0].rows[0][2], "commit-unique-2");
        assert_text(&versions.statements[0].rows[1][0], "v-unique-3");
    }
);

simulation_test!(
    lix_version_insert_commit_id_fk_is_lenient_materialized_mode,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, hidden, commit_id\
                 ) VALUES (\
                 'v-lenient', 'v-lenient', false, 'does_not_exist'\
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
        assert_eq!(inserted.statements[0].rows.len(), 1);
        assert_text(&inserted.statements[0].rows[0][0], "v-lenient");
        assert_text(&inserted.statements[0].rows[0][1], "does_not_exist");
    }
);
