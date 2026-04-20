use lix_engine::streams::{StateCommitStreamFilter, StateCommitStreamOperation};
use lix_engine::{AdditionalSessionOptions, ExecuteOptions, LixError, Value};

fn insert_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{key}', 'lix_key_value', NULL, 'global', NULL, '{{\"key\":\"{key}\",\"value\":{value_json}}}', '1'\
         )"
    )
}

fn update_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "UPDATE lix_state_by_version \
         SET snapshot_content = '{{\"key\":\"{key}\",\"value\":{value_json}}}' \
         WHERE schema_key = 'lix_key_value' AND entity_id = '{key}' AND version_id = 'global'"
    )
}

fn delete_key_value_sql(key: &str) -> String {
    format!(
        "DELETE FROM lix_state_by_version \
         WHERE schema_key = 'lix_key_value' AND entity_id = '{key}' AND version_id = 'global'"
    )
}

fn insert_key_value_entity_sql(key: &str, value: &str) -> String {
    format!("INSERT INTO lix_key_value (key, value) VALUES ('{key}', '{value}')")
}

fn insert_untracked_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "INSERT INTO lix_state (\
         entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content, untracked\
         ) VALUES (\
         '{key}', NULL, 'lix_key_value', NULL, '1', \
         lix_json('{{\"key\":\"{key}\",\"value\":{value_json}}}'), true\
         )"
    )
}

fn update_untracked_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "UPDATE lix_state \
         SET snapshot_content = lix_json('{{\"key\":\"{key}\",\"value\":{value_json}}}') \
         WHERE entity_id = '{key}' \
           AND schema_key = 'lix_key_value' \
           AND file_id IS NULL \
           AND untracked = true"
    )
}

simulation_test!(
    untracked_write_finalization_compacts_superseded_journal_rows,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                &insert_untracked_key_value_sql("state-commit-untracked-compact", "\"v0\""),
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                &update_untracked_key_value_sql("state-commit-untracked-compact", "\"v1\""),
                &[],
            )
            .await
            .unwrap();

        let journal_rows = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_change \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'state-commit-untracked-compact' \
                   AND untracked = true",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(journal_rows.statements[0].rows.len(), 1);
        assert_eq!(
            journal_rows.statements[0].rows[0][0],
            Value::Text(
                "{\"key\":\"state-commit-untracked-compact\",\"value\":\"v1\"}".to_string()
            )
        );
    }
);

simulation_test!(
    state_commit_stream_emits_matching_batches,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let events = engine.state_commit_stream(StateCommitStreamFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            ..StateCommitStreamFilter::default()
        });

        engine
            .execute(
                &insert_key_value_sql("state-commit-events-a", "\"v0\""),
                &[],
            )
            .await
            .unwrap();

        let batch = events
            .try_next()
            .expect("expected a state commit event batch");
        assert!(
            batch
                .changes
                .iter()
                .any(|change| change.schema_key == "lix_key_value"
                    && change.entity_id == "state-commit-events-a"),
            "expected key_value mutation in batch: {:?}",
            batch.changes
        );
        assert!(
            events.try_next().is_none(),
            "expected a single batch for one execute() call"
        );
    }
);

simulation_test!(
    state_commit_stream_exclude_self_suppresses_workspace_origin_only,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let events = engine.state_commit_stream(StateCommitStreamFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            ..StateCommitStreamFilter::exclude_self()
        });

        engine
            .execute(
                &insert_key_value_sql("state-commit-events-b", "\"v0\""),
                &[],
            )
            .await
            .unwrap();
        assert!(
            events.try_next().is_none(),
            "exclude_self should suppress workspace-origin events"
        );

        let worker = engine
            .open_additional_session(AdditionalSessionOptions {
                origin_key: Some("ui-worker".to_string()),
                ..AdditionalSessionOptions::default()
            })
            .await
            .expect("additional session should open");

        worker
            .execute(
                &insert_key_value_sql("state-commit-events-c", "\"v0\""),
                &[],
            )
            .await
            .unwrap();
        let batch = events
            .try_next()
            .expect("expected event batch from a different origin");
        assert!(batch.changes.iter().any(|change| {
            change.schema_key == "lix_key_value" && change.entity_id == "state-commit-events-c"
        }));
    }
);

simulation_test!(
    state_commit_stream_include_untracked_filters_untracked_rows,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let all_events = engine.state_commit_stream(StateCommitStreamFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            ..StateCommitStreamFilter::default()
        });
        let tracked_only_events = engine.state_commit_stream(StateCommitStreamFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            include_untracked: false,
            ..StateCommitStreamFilter::default()
        });

        engine
            .execute(
                &insert_untracked_key_value_sql("state-commit-untracked-a", "\"v0\""),
                &[],
            )
            .await
            .unwrap();

        let batch = all_events
            .try_next()
            .expect("expected untracked event batch when include_untracked=true");
        assert!(batch.changes.iter().any(|change| {
            change.schema_key == "lix_key_value"
                && change.entity_id == "state-commit-untracked-a"
                && change.untracked
        }));
        assert!(
            tracked_only_events.try_next().is_none(),
            "include_untracked=false should suppress untracked stream batches"
        );
    }
);

simulation_test!(
    state_commit_stream_aggregates_changes_per_transaction_commit,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let events = engine.state_commit_stream(StateCommitStreamFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            ..StateCommitStreamFilter::default()
        });

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        &insert_key_value_sql("state-commit-events-d", "\"v0\""),
                        &[],
                    )
                    .await?;
                    tx.execute(
                        &insert_key_value_sql("state-commit-events-e", "\"v0\""),
                        &[],
                    )
                    .await?;
                    Ok::<(), LixError>(())
                })
            })
            .await
            .unwrap();

        let batch = events
            .try_next()
            .expect("expected a single batched event on transaction commit");
        assert!(batch
            .changes
            .iter()
            .any(|change| change.entity_id == "state-commit-events-d"));
        assert!(batch
            .changes
            .iter()
            .any(|change| change.entity_id == "state-commit-events-e"));
        assert!(
            events.try_next().is_none(),
            "transaction commit should emit one batch"
        );
    }
);

simulation_test!(
    state_commit_stream_does_not_emit_before_transaction_commit,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let events = engine.state_commit_stream(StateCommitStreamFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            ..StateCommitStreamFilter::default()
        });

        let events = engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    let events = events;
                    tx.execute(
                        &insert_key_value_sql("state-commit-events-f", "\"v0\""),
                        &[],
                    )
                    .await?;
                    assert!(
                        events.try_next().is_none(),
                        "no state commit batch should be visible before commit"
                    );
                    Ok::<_, LixError>(events)
                })
            })
            .await
            .unwrap();

        let committed = events
            .try_next()
            .expect("commit should flush one batched state commit event");
        assert!(committed
            .changes
            .iter()
            .any(|change| change.entity_id == "state-commit-events-f"));
        assert!(
            events.try_next().is_none(),
            "exactly one post-commit batch should be emitted"
        );
    }
);

simulation_test!(
    state_commit_stream_rollback_drops_pending_transaction_changes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let events = engine.state_commit_stream(StateCommitStreamFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            ..StateCommitStreamFilter::default()
        });

        let error = engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async {
                    tx.execute(
                        &insert_key_value_sql("state-commit-events-g", "\"v0\""),
                        &[],
                    )
                    .await?;
                    Err::<(), LixError>(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: "rollback state commit stream test".to_string(),
                        hint: None,
                    })
                })
            })
            .await
            .expect_err("transaction should roll back on callback error");
        assert!(
            error
                .description
                .contains("rollback state commit stream test"),
            "unexpected rollback error: {}",
            error.description
        );

        assert!(
            events.try_next().is_none(),
            "rollback must drop queued state commit batches"
        );
    }
);

simulation_test!(
    state_commit_stream_public_entity_insert_emits_one_semantic_change,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let events = engine.state_commit_stream(StateCommitStreamFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            entity_ids: vec!["state-commit-public-entity".to_string()],
            ..StateCommitStreamFilter::default()
        });

        engine
            .execute(
                &insert_key_value_entity_sql("state-commit-public-entity", "v0"),
                &[],
            )
            .await
            .unwrap();

        let batch = events
            .try_next()
            .expect("expected a state commit batch from the public entity write");
        assert_eq!(
            batch.changes.len(),
            1,
            "public live writes should emit one semantic change batch entry"
        );
        let change = &batch.changes[0];
        assert_eq!(change.operation, StateCommitStreamOperation::Insert);
        assert_eq!(change.entity_id, "state-commit-public-entity");
        assert_eq!(change.schema_key, "lix_key_value");
        assert!(
            events.try_next().is_none(),
            "public entity insert should emit exactly one batch"
        );
    }
);

simulation_test!(
    state_commit_stream_emits_update_operation_for_update_mutation,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let entity_id = "state-commit-events-update-op";
        let events = engine.state_commit_stream(StateCommitStreamFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            ..StateCommitStreamFilter::default()
        });

        engine
            .execute(&insert_key_value_sql(entity_id, "\"v0\""), &[])
            .await
            .unwrap();
        let _insert_batch = events
            .try_next()
            .expect("expected initial insert event batch");

        engine
            .execute(&update_key_value_sql(entity_id, "\"v1\""), &[])
            .await
            .unwrap();

        let update_batch = events
            .try_next()
            .expect("expected update event batch after UPDATE");

        assert!(
            update_batch
                .changes
                .iter()
                .any(|change| change.entity_id == entity_id
                    && change.operation == StateCommitStreamOperation::Update),
            "expected UPDATE operation for entity {}; got changes: {:?}",
            entity_id,
            update_batch.changes
        );
    }
);

simulation_test!(
    state_commit_stream_emits_delete_operation_for_delete_mutation,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let entity_id = "state-commit-events-delete-op";
        let events = engine.state_commit_stream(StateCommitStreamFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            ..StateCommitStreamFilter::default()
        });

        engine
            .execute(&insert_key_value_sql(entity_id, "\"v0\""), &[])
            .await
            .unwrap();
        let _insert_batch = events
            .try_next()
            .expect("expected initial insert event batch");

        engine
            .execute(&delete_key_value_sql(entity_id), &[])
            .await
            .unwrap();

        let delete_batch = events
            .try_next()
            .expect("expected delete event batch after DELETE");

        assert!(
            delete_batch
                .changes
                .iter()
                .any(|change| change.entity_id == entity_id
                    && change.operation == StateCommitStreamOperation::Delete),
            "expected DELETE operation for entity {}; got changes: {:?}",
            entity_id,
            delete_batch.changes
        );
    }
);
