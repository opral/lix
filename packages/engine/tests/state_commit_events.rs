mod support;

use lix_engine::{ExecuteOptions, LixError, StateCommitEventFilter};

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
    state_commit_events_emits_matching_batches,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let events = engine
            .raw_engine()
            .state_commit_events(StateCommitEventFilter {
                schema_keys: vec!["lix_key_value".to_string()],
                ..StateCommitEventFilter::default()
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
    state_commit_events_respects_excluded_writer_keys,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let events = engine
            .raw_engine()
            .state_commit_events(StateCommitEventFilter {
                schema_keys: vec!["lix_key_value".to_string()],
                exclude_writer_keys: vec!["ui-writer".to_string()],
                ..StateCommitEventFilter::default()
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
    state_commit_events_aggregates_changes_per_transaction_commit,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let events = engine
            .raw_engine()
            .state_commit_events(StateCommitEventFilter {
                schema_keys: vec!["lix_key_value".to_string()],
                ..StateCommitEventFilter::default()
            });

        engine
            .raw_engine()
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
