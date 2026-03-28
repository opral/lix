use crate::support;

use lix_engine::{
    CreateVersionOptions, StateCommitStreamFilter, StateCommitStreamOperation, UndoOptions, Value,
};

const CHECKPOINT_LABEL_ID: &str = "lix_label_checkpoint";

fn as_text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.clone(),
        other => panic!("expected text value, got {other:?}"),
    }
}

async fn active_version_ref(
    engine: &support::simulation_test::SimulationEngine,
) -> (String, String) {
    let version_id = engine
        .active_version_id()
        .await
        .expect("active version query should succeed");
    let result = engine
        .execute(
            "SELECT id, commit_id \
             FROM lix_version \
             WHERE id = $1 \
             LIMIT 1",
            &[Value::Text(version_id)],
        )
        .await
        .expect("active version query should succeed");
    assert_eq!(result.statements[0].rows.len(), 1);
    (
        as_text(&result.statements[0].rows[0][0]),
        as_text(&result.statements[0].rows[0][1]),
    )
}

async fn key_value_value(
    engine: &support::simulation_test::SimulationEngine,
    key: &str,
) -> Option<String> {
    let result = engine
        .execute(
            "SELECT value FROM lix_key_value WHERE key = $1 LIMIT 1",
            &[Value::Text(key.to_string())],
        )
        .await
        .expect("key value query should succeed");
    result.statements[0]
        .rows
        .first()
        .and_then(|row| row.first())
        .map(as_text)
}

async fn checkpoint_label_id(
    engine: &support::simulation_test::SimulationEngine,
) -> Option<String> {
    let result = engine
        .execute(
            "SELECT id FROM lix_label WHERE id = $1 LIMIT 1",
            &[Value::Text(CHECKPOINT_LABEL_ID.to_string())],
        )
        .await
        .expect("checkpoint label query should succeed");
    result.statements[0]
        .rows
        .first()
        .and_then(|row| row.first())
        .map(as_text)
}

async fn plugins_directory_id(
    engine: &support::simulation_test::SimulationEngine,
) -> Option<String> {
    let result = engine
        .execute(
            "SELECT id FROM lix_directory WHERE path = '/.lix/plugins/' LIMIT 1",
            &[],
        )
        .await
        .expect("plugins directory query should succeed");
    result.statements[0]
        .rows
        .first()
        .and_then(|row| row.first())
        .map(as_text)
}

async fn directory_descriptor_exists(
    engine: &support::simulation_test::SimulationEngine,
    id: &str,
) -> Option<String> {
    let result = engine
        .execute(
            "SELECT id FROM lix_directory_descriptor WHERE id = $1 LIMIT 1",
            &[Value::Text(id.to_string())],
        )
        .await
        .expect("directory descriptor query should succeed");
    result.statements[0]
        .rows
        .first()
        .and_then(|row| row.first())
        .map(as_text)
}

async fn file_bytes(
    engine: &support::simulation_test::SimulationEngine,
    file_id: &str,
) -> Option<Vec<u8>> {
    let result = engine
        .execute(
            "SELECT data FROM lix_file WHERE id = $1 LIMIT 1",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .expect("file query should succeed");
    match result.statements[0]
        .rows
        .first()
        .and_then(|row| row.first())
    {
        Some(Value::Blob(bytes)) => Some(bytes.clone()),
        Some(other) => panic!("expected blob file data, got {other:?}"),
        None => None,
    }
}

simulation_test!(undo_redo_reverts_insert_and_replays_it, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.initialize().await.expect("init should succeed");

    let (version_id, _baseline_commit_id) = active_version_ref(&engine).await;
    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('undo-redo-insert', 'v1')",
            &[],
        )
        .await
        .expect("tracked insert should succeed");
    let (_version_id, inserted_commit_id) = active_version_ref(&engine).await;

    assert_eq!(
        key_value_value(&engine, "undo-redo-insert").await,
        Some("v1".to_string())
    );

    let undo = engine.undo().await.expect("undo should succeed");
    assert_eq!(undo.version_id, version_id);
    assert_eq!(undo.target_commit_id, inserted_commit_id);
    assert_ne!(undo.inverse_commit_id, undo.target_commit_id);
    assert_eq!(key_value_value(&engine, "undo-redo-insert").await, None);

    let redo = engine.redo().await.expect("redo should succeed");
    assert_eq!(redo.version_id, version_id);
    assert_eq!(redo.target_commit_id, inserted_commit_id);
    assert_ne!(redo.replay_commit_id, redo.target_commit_id);
    assert_ne!(redo.replay_commit_id, undo.inverse_commit_id);
    assert_eq!(
        key_value_value(&engine, "undo-redo-insert").await,
        Some("v1".to_string())
    );
});

simulation_test!(
    empty_key_insert_is_rejected_without_poisoning_undo_history,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        let (version_id, _baseline_commit_id) = active_version_ref(&engine).await;
        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('undo-safe-key', 'before')",
                &[],
            )
            .await
            .expect("tracked insert should succeed");
        let (_version_id, inserted_commit_id) = active_version_ref(&engine).await;

        let empty_insert = engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('', 'bad')",
                &[],
            )
            .await
            .expect_err("empty key insert should fail");
        assert!(
            empty_insert.to_string().contains("empty primary-key value")
                || empty_insert.to_string().contains("non-empty entity_id")
                || empty_insert.to_string().contains("non-empty key"),
            "unexpected error: {empty_insert}"
        );

        let undo = engine.undo().await.expect("undo should still succeed");
        assert_eq!(undo.version_id, version_id);
        assert_eq!(undo.target_commit_id, inserted_commit_id);
        assert_eq!(key_value_value(&engine, "undo-safe-key").await, None);
    }
);

simulation_test!(
    undo_rejects_bootstrap_boundary_on_fresh_project,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        let error = engine
            .undo()
            .await
            .expect_err("fresh project undo should stop at bootstrap boundary");
        assert_eq!(error.code, "LIX_ERROR_NOTHING_TO_UNDO");
        assert!(
            error.description.contains("nothing to undo"),
            "unexpected undo error: {}",
            error.description
        );
    }
);

simulation_test!(
    undo_redo_undo_cycle_creates_distinct_operation_commits,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('undo-redo-undo', 'v1')",
                &[],
            )
            .await
            .expect("tracked insert should succeed");

        let first_undo = engine.undo().await.expect("first undo should succeed");
        let redo = engine.redo().await.expect("redo should succeed");
        let second_undo = engine.undo().await.expect("second undo should succeed");

        assert_ne!(first_undo.inverse_commit_id, second_undo.inverse_commit_id);
        assert_eq!(first_undo.target_commit_id, redo.target_commit_id);
        assert_eq!(redo.target_commit_id, second_undo.target_commit_id);
        assert_eq!(key_value_value(&engine, "undo-redo-undo").await, None);
    }
);

simulation_test!(undo_stops_after_last_user_commit, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.initialize().await.expect("init should succeed");

    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('undo-boundary', 'v1')",
            &[],
        )
        .await
        .expect("tracked insert should succeed");

    engine.undo().await.expect("first undo should succeed");
    let error = engine
        .undo()
        .await
        .expect_err("second undo should stop at bootstrap boundary");
    assert_eq!(error.code, "LIX_ERROR_NOTHING_TO_UNDO");
    assert!(
        error.description.contains("nothing to undo"),
        "unexpected undo error: {}",
        error.description
    );
});

simulation_test!(checkpoint_label_delete_is_rejected, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.initialize().await.expect("init should succeed");

    let original_id = checkpoint_label_id(&engine)
        .await
        .expect("checkpoint label should exist after init");
    let error = engine
        .execute(
            "DELETE FROM lix_label WHERE id = $1",
            &[Value::Text(CHECKPOINT_LABEL_ID.to_string())],
        )
        .await
        .expect_err("checkpoint label delete should fail");
    assert!(
        error
            .description
            .contains("checkpoint label is system-managed"),
        "unexpected delete error: {error}"
    );
    assert_eq!(checkpoint_label_id(&engine).await, Some(original_id));
});

simulation_test!(
    undo_restores_deleted_bootstrap_plugins_directory,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        let original_id = plugins_directory_id(&engine)
            .await
            .expect("plugins directory should exist after init");
        engine
            .execute(
                "DELETE FROM lix_directory_descriptor WHERE id = $1",
                &[Value::Text(original_id.clone())],
            )
            .await
            .expect("bootstrap plugins directory delete should succeed");
        assert_eq!(
            directory_descriptor_exists(&engine, &original_id).await,
            None
        );

        let undo = engine
            .undo()
            .await
            .expect("undo should restore bootstrap directory");
        assert!(!undo.target_commit_id.is_empty());
        assert_eq!(
            directory_descriptor_exists(&engine, &original_id).await,
            Some(original_id)
        );
    }
);

simulation_test!(
    undo_redo_reverts_updates_to_prior_snapshot,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('undo-redo-update', 'before')",
                &[],
            )
            .await
            .expect("seed insert should succeed");
        engine
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'undo-redo-update'",
                &[],
            )
            .await
            .expect("tracked update should succeed");

        assert_eq!(
            key_value_value(&engine, "undo-redo-update").await,
            Some("after".to_string())
        );

        engine.undo().await.expect("undo should succeed");
        assert_eq!(
            key_value_value(&engine, "undo-redo-update").await,
            Some("before".to_string())
        );

        engine.redo().await.expect("redo should succeed");
        assert_eq!(
            key_value_value(&engine, "undo-redo-update").await,
            Some("after".to_string())
        );
    }
);

simulation_test!(
    undo_redo_reverts_deletes_to_restore_rows,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('undo-redo-delete', 'present')",
                &[],
            )
            .await
            .expect("seed insert should succeed");
        engine
            .execute(
                "DELETE FROM lix_key_value WHERE key = 'undo-redo-delete'",
                &[],
            )
            .await
            .expect("tracked delete should succeed");

        assert_eq!(key_value_value(&engine, "undo-redo-delete").await, None);

        engine.undo().await.expect("undo should succeed");
        assert_eq!(
            key_value_value(&engine, "undo-redo-delete").await,
            Some("present".to_string())
        );

        engine.redo().await.expect("redo should succeed");
        assert_eq!(key_value_value(&engine, "undo-redo-delete").await, None);
    }
);

simulation_test!(undo_redo_replays_file_inserts, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.initialize().await.expect("init should succeed");

    engine
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('undo-file', '/undo-file.txt', lix_text_encode('hello undo'))",
            &[],
        )
        .await
        .expect("file insert should succeed");
    assert_eq!(
        file_bytes(&engine, "undo-file").await,
        Some(b"hello undo".to_vec())
    );

    engine.undo().await.expect("undo should succeed");
    assert_eq!(file_bytes(&engine, "undo-file").await, None);

    engine.redo().await.expect("redo should succeed");
    assert_eq!(
        file_bytes(&engine, "undo-file").await,
        Some(b"hello undo".to_vec())
    );
});

simulation_test!(undo_clears_redo_after_new_tracked_write, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.initialize().await.expect("init should succeed");

    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('redo-a', 'a')",
            &[],
        )
        .await
        .expect("first insert should succeed");
    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('redo-b', 'b')",
            &[],
        )
        .await
        .expect("second insert should succeed");

    engine.undo().await.expect("undo should succeed");
    assert_eq!(key_value_value(&engine, "redo-b").await, None);

    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('redo-c', 'c')",
            &[],
        )
        .await
        .expect("replacement insert should succeed");

    let redo_error = engine.redo().await.expect_err("redo should be cleared");
    assert_eq!(redo_error.code, "LIX_ERROR_NOTHING_TO_REDO");
    assert!(
        redo_error.description.contains("nothing to redo"),
        "unexpected redo error: {}",
        redo_error.description
    );

    assert_eq!(
        key_value_value(&engine, "redo-a").await,
        Some("a".to_string())
    );
    assert_eq!(key_value_value(&engine, "redo-b").await, None);
    assert_eq!(
        key_value_value(&engine, "redo-c").await,
        Some("c".to_string())
    );
});

simulation_test!(
    undo_with_options_targets_non_active_version_without_switching_active_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        let (main_version_id, _main_commit_id) = active_version_ref(&engine).await;
        let branch = engine
            .create_version(CreateVersionOptions {
                id: Some("undo-target-branch".to_string()),
                name: Some("Undo Target Branch".to_string()),
                source_version_id: None,
                hidden: false,
            })
            .await
            .expect("create_version should succeed");

        engine
            .switch_version(branch.id.clone())
            .await
            .expect("switch_version should succeed");
        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('branch-only-undo', 'branch')",
                &[],
            )
            .await
            .expect("branch insert should succeed");

        engine
            .switch_version(main_version_id.clone())
            .await
            .expect("switch back to main should succeed");

        let undo = engine
            .undo_with_options(UndoOptions {
                version_id: Some(branch.id.clone()),
            })
            .await
            .expect("undo_with_options should succeed");
        assert_eq!(undo.version_id, branch.id);

        let (still_active_version_id, _still_active_commit_id) = active_version_ref(&engine).await;
        assert_eq!(still_active_version_id, main_version_id);
        assert_eq!(key_value_value(&engine, "branch-only-undo").await, None);

        engine
            .switch_version(undo.version_id)
            .await
            .expect("switch to branch should succeed");
        assert_eq!(key_value_value(&engine, "branch-only-undo").await, None);
    }
);

simulation_test!(
    undo_redo_emit_state_commit_stream_batches,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        let events = engine.state_commit_stream(StateCommitStreamFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            ..StateCommitStreamFilter::default()
        });

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('undo-stream', 'stream')",
                &[],
            )
            .await
            .expect("tracked insert should succeed");
        let _insert_batch = events
            .try_next()
            .expect("initial insert should emit a state commit event");

        engine.undo().await.expect("undo should succeed");
        let undo_batch = events.try_next().expect("undo should emit one event batch");
        assert!(undo_batch.changes.iter().any(|change| {
            change.entity_id == "undo-stream"
                && change.schema_key == "lix_key_value"
                && change.operation == StateCommitStreamOperation::Delete
        }));

        engine.redo().await.expect("redo should succeed");
        let redo_batch = events.try_next().expect("redo should emit one event batch");
        assert!(redo_batch.changes.iter().any(|change| {
            change.entity_id == "undo-stream"
                && change.schema_key == "lix_key_value"
                && change.operation == StateCommitStreamOperation::Insert
        }));
    }
);
