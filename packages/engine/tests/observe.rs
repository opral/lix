mod support;

use lix_engine::{ObserveQuery, Value};

fn insert_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "INSERT INTO lix_internal_state_vtable (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{key}', 'lix_key_value', 'lix', 'global', 'lix', '{{\"key\":\"{key}\",\"value\":{value_json}}}', '1'\
         )"
    )
}

fn update_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "UPDATE lix_internal_state_vtable \
         SET snapshot_content = '{{\"key\":\"{key}\",\"value\":{value_json}}}' \
         WHERE entity_id = '{key}' \
           AND schema_key = 'lix_key_value' \
           AND file_id = 'lix' \
           AND version_id = 'global'"
    )
}

simulation_test!(
    observe_emits_initial_and_followup_rows,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let mut observed = engine
            .raw_engine()
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' AND entity_id = ?1",
                vec![Value::Text("observe-key".to_string())],
            ))
            .expect("observe should succeed");

        let initial = observed
            .next()
            .await
            .expect("initial observe poll should succeed")
            .expect("initial observe event should exist");
        assert_eq!(initial.sequence, 0);
        assert!(initial.rows.rows.is_empty());
        assert_eq!(initial.state_commit_sequence, None);

        engine
            .execute(&insert_key_value_sql("observe-key", "\"v0\""), &[])
            .await
            .unwrap();

        let update = observed
            .next()
            .await
            .expect("follow-up observe poll should succeed")
            .expect("follow-up observe event should exist");
        assert_eq!(update.sequence, 1);
        assert!(update.state_commit_sequence.is_some());
        assert_eq!(update.rows.rows.len(), 1);
        assert_eq!(
            update.rows.rows[0][0],
            Value::Text("observe-key".to_string())
        );

        observed.close();
        let closed = observed
            .next()
            .await
            .expect("observe poll after close should succeed");
        assert!(closed.is_none());
    }
);

simulation_test!(
    observe_supports_multiple_subscribers,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let mut observed_a = engine
            .raw_engine()
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' AND entity_id = ?1 \
                 ORDER BY entity_id",
                vec![Value::Text("observe-multi".to_string())],
            ))
            .expect("observe should succeed");
        let mut observed_b = engine
            .raw_engine()
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' AND entity_id = ?1 \
                 ORDER BY entity_id",
                vec![Value::Text("observe-multi".to_string())],
            ))
            .expect("observe should succeed");

        let initial_a = observed_a.next().await.unwrap().unwrap();
        let initial_b = observed_b.next().await.unwrap().unwrap();
        assert!(initial_a.rows.rows.is_empty());
        assert!(initial_b.rows.rows.is_empty());

        engine
            .execute(&insert_key_value_sql("observe-multi", "\"v0\""), &[])
            .await
            .unwrap();

        let update_a = observed_a.next().await.unwrap().unwrap();
        let update_b = observed_b.next().await.unwrap().unwrap();

        assert_eq!(update_a.sequence, 1);
        assert_eq!(update_b.sequence, 1);
        assert_eq!(update_a.rows.rows.len(), 1);
        assert_eq!(update_b.rows.rows.len(), 1);
        assert_eq!(
            update_a.rows.rows[0][0],
            Value::Text("observe-multi".to_string())
        );
        assert_eq!(
            update_b.rows.rows[0][0],
            Value::Text("observe-multi".to_string())
        );
    }
);

simulation_test!(
    observe_skips_unrelated_commits_until_result_changes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let mut observed = engine
            .raw_engine()
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' AND entity_id = ?1 \
                 ORDER BY entity_id",
                vec![Value::Text("observe-target".to_string())],
            ))
            .expect("observe should succeed");

        let initial = observed.next().await.unwrap().unwrap();
        assert_eq!(initial.sequence, 0);
        assert!(initial.rows.rows.is_empty());

        engine
            .execute(&insert_key_value_sql("observe-unrelated", "\"v0\""), &[])
            .await
            .unwrap();
        engine
            .execute(&insert_key_value_sql("observe-target", "\"v1\""), &[])
            .await
            .unwrap();

        let update = observed.next().await.unwrap().unwrap();
        assert_eq!(update.sequence, 1);
        assert_eq!(update.rows.rows.len(), 1);
        assert_eq!(
            update.rows.rows[0][0],
            Value::Text("observe-target".to_string())
        );
    }
);

simulation_test!(
    observe_dedups_noop_result_reexecutions,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(&insert_key_value_sql("observe-dedup", "\"v0\""), &[])
            .await
            .unwrap();

        let mut observed = engine
            .raw_engine()
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' \
                 ORDER BY entity_id",
                vec![],
            ))
            .expect("observe should succeed");

        let initial = observed.next().await.unwrap().unwrap();
        assert_eq!(initial.sequence, 0);
        assert_eq!(initial.rows.rows.len(), 1);

        engine
            .execute(&update_key_value_sql("observe-dedup", "\"v1\""), &[])
            .await
            .unwrap();
        engine
            .execute(&insert_key_value_sql("observe-dedup-2", "\"v0\""), &[])
            .await
            .unwrap();

        let next = observed.next().await.unwrap().unwrap();
        assert_eq!(next.sequence, 1);
        assert_eq!(next.rows.rows.len(), 2);
        assert_eq!(
            next.rows.rows[0][0],
            Value::Text("observe-dedup".to_string())
        );
        assert_eq!(
            next.rows.rows[1][0],
            Value::Text("observe-dedup-2".to_string())
        );
    }
);

simulation_test!(
    observe_preserves_commit_order_across_multiple_updates,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let mut observed = engine
            .raw_engine()
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' \
                 ORDER BY entity_id",
                vec![],
            ))
            .expect("observe should succeed");

        let initial = observed.next().await.unwrap().unwrap();
        assert_eq!(initial.sequence, 0);
        assert!(initial.rows.rows.is_empty());

        engine
            .execute(&insert_key_value_sql("observe-order-a", "\"v0\""), &[])
            .await
            .unwrap();

        let first = observed.next().await.unwrap().unwrap();
        assert_eq!(first.sequence, 1);
        assert_eq!(first.rows.rows.len(), 1);

        engine
            .execute(&insert_key_value_sql("observe-order-b", "\"v0\""), &[])
            .await
            .unwrap();

        let second = observed.next().await.unwrap().unwrap();
        assert_eq!(second.sequence, 2);
        assert_eq!(second.rows.rows.len(), 2);
        assert!(
            first
                .state_commit_sequence
                .zip(second.state_commit_sequence)
                .is_some_and(|(left, right)| right > left),
            "expected monotonic state commit sequence order"
        );
    }
);

simulation_test!(
    observe_rejects_non_query_sql,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let result = engine.raw_engine().observe(ObserveQuery::new(
            "UPDATE lix_state SET schema_version = '1' WHERE 1 = 0",
            vec![],
        ));

        let error = match result {
            Ok(_) => panic!("observe should reject non-query SQL"),
            Err(error) => error,
        };
        assert!(
            error
                .message
                .contains("observe requires one or more SELECT statements"),
            "unexpected error message: {}",
            error.message
        );
    }
);
