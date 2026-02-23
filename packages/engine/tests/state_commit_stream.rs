mod support;

use lix_engine::{ExecuteOptions, LixError, StateCommitStreamFilter};

fn insert_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "INSERT INTO lix_internal_state_vtable (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{key}', 'lix_key_value', 'lix', 'global', 'lix', '{{\"key\":\"{key}\",\"value\":{value_json}}}', '1'\
         )"
    )
}

simulation_test!(
    state_commit_stream_emits_matching_batches,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

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
    state_commit_stream_respects_excluded_writer_keys,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let events = engine.state_commit_stream(StateCommitStreamFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            exclude_writer_keys: vec!["ui-writer".to_string()],
            ..StateCommitStreamFilter::default()
        });

        engine
            .execute_with_options(
                &insert_key_value_sql("state-commit-events-b", "\"v0\""),
                &[],
                ExecuteOptions {
                    writer_key: Some("ui-writer".to_string()),
                },
            )
            .await
            .unwrap();
        assert!(
            events.try_next().is_none(),
            "excluded writer should not receive events"
        );

        engine
            .execute(
                &insert_key_value_sql("state-commit-events-c", "\"v0\""),
                &[],
            )
            .await
            .unwrap();
        let batch = events
            .try_next()
            .expect("expected event batch from non-excluded writer");
        assert!(batch.changes.iter().any(|change| {
            change.schema_key == "lix_key_value" && change.entity_id == "state-commit-events-c"
        }));
    }
);

simulation_test!(
    state_commit_stream_aggregates_changes_per_transaction_commit,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

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
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

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
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

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
                        message: "rollback state commit stream test".to_string(),
                    })
                })
            })
            .await
            .expect_err("transaction should roll back on callback error");
        assert!(
            error.message.contains("rollback state commit stream test"),
            "unexpected rollback error: {}",
            error.message
        );

        assert!(
            events.try_next().is_none(),
            "rollback must drop queued state commit batches"
        );
    }
);
