use crate::support;

use lix_engine::wasm::NoopWasmRuntime;
use lix_engine::{CreateVersionOptions, ExecuteOptions, Lix, LixConfig};
use lix_engine::{ObserveQuery, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn insert_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{key}', 'lix_key_value', 'lix', 'global', 'lix', '{{\"key\":\"{key}\",\"value\":{value_json}}}', '1'\
         )"
    )
}

fn update_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "UPDATE lix_state_by_version \
         SET snapshot_content = '{{\"key\":\"{key}\",\"value\":{value_json}}}' \
         WHERE entity_id = '{key}' \
           AND schema_key = 'lix_key_value' \
           AND file_id = 'lix' \
           AND version_id = 'global'"
    )
}

fn insert_key_value_entity_sql(key: &str, value: &str) -> String {
    format!("INSERT INTO lix_key_value (key, value) VALUES ('{key}', '{value}')")
}

simulation_test!(
    observe_emits_initial_and_followup_rows,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let mut observed = engine
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let mut observed_a = engine
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' AND entity_id = ?1 \
                 ORDER BY entity_id",
                vec![Value::Text("observe-multi".to_string())],
            ))
            .expect("observe should succeed");
        let mut observed_b = engine
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
    observe_public_entity_view_emits_after_live_write,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let mut observed = engine
            .observe(ObserveQuery::new(
                "SELECT key, value \
                 FROM lix_key_value \
                 WHERE key = ?1",
                vec![Value::Text("observe-public-entity".to_string())],
            ))
            .expect("observe should succeed");

        let initial = observed.next().await.unwrap().unwrap();
        assert!(initial.rows.rows.is_empty());

        engine
            .execute(
                &insert_key_value_entity_sql("observe-public-entity", "v0"),
                &[],
            )
            .await
            .unwrap();

        let update = observed.next().await.unwrap().unwrap();
        assert!(update.state_commit_sequence.is_some());
        assert_eq!(update.rows.rows.len(), 1);
        assert_eq!(
            update.rows.rows[0],
            vec![
                Value::Text("observe-public-entity".to_string()),
                Value::Text("v0".to_string())
            ]
        );
    }
);

simulation_test!(
    observe_stream_recovers_after_transient_query_error,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let key = "observe-recover-json";
        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
                &[
                    Value::Text(key.to_string()),
                    Value::Text(r#"{"value":"ok-0"}"#.to_string()),
                ],
            )
            .await
            .expect("seed insert should succeed");

        let mut observed = engine
            .observe(ObserveQuery::new(
                "SELECT json_extract(value, '$.value') FROM lix_key_value WHERE key = ?1",
                vec![Value::Text(key.to_string())],
            ))
            .expect("observe should succeed");

        let initial = observed.next().await.unwrap().unwrap();
        assert_eq!(
            initial.rows.rows,
            vec![vec![Value::Text("ok-0".to_string())]]
        );

        let failing_next = observed.next();
        engine
            .execute(
                "UPDATE lix_key_value SET value = ?2 WHERE key = ?1",
                &[Value::Text(key.to_string()), Value::Text("{".to_string())],
            )
            .await
            .expect("malformed update should still commit");

        let query_error = failing_next
            .await
            .expect_err("observe follow-up should surface malformed json");
        assert!(
            query_error.description.contains("malformed")
                || query_error.description.contains("json")
                || query_error.description.contains("parse")
        );

        let recovered_next = observed.next();
        engine
            .execute(
                "UPDATE lix_key_value SET value = ?2 WHERE key = ?1",
                &[
                    Value::Text(key.to_string()),
                    Value::Text(r#"{"value":"ok-1"}"#.to_string()),
                ],
            )
            .await
            .expect("recovery update should succeed");

        let recovered = tokio::time::timeout(Duration::from_secs(2), recovered_next)
            .await
            .expect("observe recovery should not time out")
            .expect("observe recovery should succeed")
            .expect("observe recovery event should exist");
        assert_eq!(
            recovered.rows.rows,
            vec![vec![Value::Text("ok-1".to_string())]]
        );
    }
);

simulation_test!(
    observe_skips_unrelated_commits_until_result_changes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let mut observed = engine
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(&insert_key_value_sql("observe-dedup", "\"v0\""), &[])
            .await
            .unwrap();

        let mut observed = engine
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id != 'lix_id' \
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let mut observed = engine
            .observe(ObserveQuery::new(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id != 'lix_id' \
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
    observe_lix_state_emits_when_switching_from_global_visibility_to_local_shadow,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let branch = engine
            .create_version(CreateVersionOptions {
                ..Default::default()
            })
            .await
            .expect("create_version should succeed");

        let entity_id = "observe-switch-state";
        let insert_state_sql = "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)";

        engine
            .execute(
                insert_state_sql,
                &[
                    Value::Text(entity_id.to_string()),
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("lix".to_string()),
                    Value::Text("global".to_string()),
                    Value::Text("lix_sdk".to_string()),
                    Value::Text(format!(r#"{{"key":"{entity_id}","value":"global"}}"#)),
                    Value::Text("1".to_string()),
                ],
            )
            .await
            .expect("global state insert should succeed");
        engine
            .execute(
                insert_state_sql,
                &[
                    Value::Text(entity_id.to_string()),
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("lix".to_string()),
                    Value::Text(branch.id.clone()),
                    Value::Text("lix_sdk".to_string()),
                    Value::Text(format!(r#"{{"key":"{entity_id}","value":"branch"}}"#)),
                    Value::Text("1".to_string()),
                ],
            )
            .await
            .expect("branch state insert should succeed");

        let mut observed = engine
            .observe(ObserveQuery::new(
                "SELECT lix_json_extract(snapshot_content, 'value') \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' AND entity_id = ?1",
                vec![Value::Text(entity_id.to_string())],
            ))
            .expect("observe should succeed");

        let initial = observed.next().await.unwrap().unwrap();
        assert_eq!(initial.sequence, 0);
        assert_eq!(
            initial.rows.rows,
            vec![vec![Value::Text("global".to_string())]]
        );

        engine
            .switch_version(branch.id.clone())
            .await
            .expect("switch to branch should succeed");

        let debug_branch_version = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE entity_id = ?1 AND schema_key = 'lix_key_value' AND version_id = ?2",
                &[
                    Value::Text(entity_id.to_string()),
                    Value::Text(branch.id.clone()),
                ],
            )
            .await
            .expect("branch direct query should succeed");
        eprintln!(
            "debug branch by version rows: {:?}",
            debug_branch_version.statements[0].rows
        );
        let debug_active = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' AND entity_id = ?1",
                &[Value::Text(entity_id.to_string())],
            )
            .await
            .expect("active query after switch should succeed");
        eprintln!(
            "debug active rows after switch: {:?}",
            debug_active.statements[0].rows
        );
        let debug_version_head = engine
            .execute(
                "SELECT commit_id FROM lix_version WHERE id = ?1",
                &[Value::Text(branch.id.clone())],
            )
            .await
            .expect("version head query should succeed");
        eprintln!(
            "debug version head rows: {:?}",
            debug_version_head.statements[0].rows
        );

        let update = tokio::time::timeout(Duration::from_secs(2), observed.next())
            .await
            .expect("observe next should not time out")
            .expect("observe next should succeed")
            .expect("observe update event should exist");
        assert_eq!(update.sequence, 1);
        assert_eq!(
            update.rows.rows,
            vec![vec![Value::Text("branch".to_string())]]
        );
    }
);

simulation_test!(
    observe_lix_file_emits_when_switching_from_global_visibility_to_local_shadow,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let branch = engine
            .create_version(CreateVersionOptions {
                ..Default::default()
            })
            .await
            .expect("create_version should succeed");

        let path = "/observe-switch-file.txt";
        engine
            .execute(
                "INSERT INTO lix_file_by_version (path, data, lixcol_version_id) VALUES ($1, $2, $3)", &[
                    Value::Text(path.to_string()),
                    Value::Blob(vec![1]),
                    Value::Text("global".to_string()),
                ])
            .await
            .expect("global file insert should succeed");
        engine
            .execute(
                "UPDATE lix_file_by_version \
                 SET data = $1 \
                 WHERE path = $2 AND lixcol_version_id = $3",
                &[
                    Value::Blob(vec![2]),
                    Value::Text(path.to_string()),
                    Value::Text(branch.id.clone()),
                ],
            )
            .await
            .expect("branch file update should succeed");

        let mut observed = engine
            .observe(ObserveQuery::new(
                "SELECT data FROM lix_file WHERE path = ?1",
                vec![Value::Text(path.to_string())],
            ))
            .expect("observe should succeed");

        let initial = observed.next().await.unwrap().unwrap();
        assert_eq!(initial.sequence, 0);
        assert_eq!(initial.rows.rows, vec![vec![Value::Blob(vec![1])]]);

        engine
            .switch_version(branch.id.clone())
            .await
            .expect("switch to branch should succeed");

        let update = tokio::time::timeout(Duration::from_secs(2), observed.next())
            .await
            .expect("observe next should not time out")
            .expect("observe next should succeed")
            .expect("observe update event should exist");
        assert_eq!(update.sequence, 1);
        assert_eq!(update.rows.rows, vec![vec![Value::Blob(vec![2])]]);
    }
);

simulation_test!(
    observe_lix_state_mixed_tracked_and_untracked_changes_emit_only_on_local_shadow_delta,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let branch = engine
            .create_version(CreateVersionOptions {
                ..Default::default()
            })
            .await
            .expect("create_version should succeed");

        let entity_id = "observe-mixed-active-untracked";
        let insert_state_sql = "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)";

        engine
            .execute(
                insert_state_sql,
                &[
                    Value::Text(entity_id.to_string()),
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("lix".to_string()),
                    Value::Text("global".to_string()),
                    Value::Text("lix_sdk".to_string()),
                    Value::Text(format!(r#"{{"key":"{entity_id}","value":"global"}}"#)),
                    Value::Text("1".to_string()),
                ],
            )
            .await
            .expect("global state insert should succeed");
        engine
            .execute(
                insert_state_sql,
                &[
                    Value::Text(entity_id.to_string()),
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("lix".to_string()),
                    Value::Text(branch.id.clone()),
                    Value::Text("lix_sdk".to_string()),
                    Value::Text(format!(r#"{{"key":"{entity_id}","value":"branch-v1"}}"#)),
                    Value::Text("1".to_string()),
                ],
            )
            .await
            .expect("branch state insert should succeed");

        let mut observed = engine
            .observe(ObserveQuery::new(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' AND entity_id = ?1",
                vec![Value::Text(entity_id.to_string())],
            ))
            .expect("observe should succeed");

        let initial = observed.next().await.unwrap().unwrap();
        assert_eq!(initial.sequence, 0);
        assert_eq!(
            initial.rows.rows,
            vec![vec![Value::Text(format!(
                r#"{{"key":"{entity_id}","value":"global"}}"#
            ))]]
        );

        engine
            .execute(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = $1 \
                 WHERE entity_id = $2 \
                   AND schema_key = $3 \
                   AND version_id = $4",
                &[
                    Value::Text(format!(r#"{{"key":"{entity_id}","value":"branch-v2"}}"#)),
                    Value::Text(entity_id.to_string()),
                    Value::Text("lix_key_value".to_string()),
                    Value::Text(branch.id.clone()),
                ],
            )
            .await
            .expect("branch state update should succeed");

        let no_visible_change =
            tokio::time::timeout(Duration::from_millis(300), observed.next()).await;
        assert!(
            no_visible_change.is_err(),
            "tracked write in non-active version should not emit a visible observe update"
        );

        engine
            .switch_version(branch.id.clone())
            .await
            .expect("switch to branch should succeed");

        let update = tokio::time::timeout(Duration::from_secs(2), observed.next())
            .await
            .expect("observe next should not time out")
            .expect("observe next should succeed")
            .expect("observe update event should exist");
        assert_eq!(update.sequence, 1);
        assert_eq!(
            update.rows.rows,
            vec![vec![Value::Text(format!(
                r#"{{"key":"{entity_id}","value":"branch-v2"}}"#
            ))]]
        );
    }
);

simulation_test!(
    observe_rejects_non_query_sql,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let result = engine.observe(ObserveQuery::new(
            "UPDATE lix_state SET schema_version = '1' WHERE 1 = 0",
            vec![],
        ));

        let error = match result {
            Ok(_) => panic!("observe should reject non-query SQL"),
            Err(error) => error,
        };
        assert!(
            error
                .description
                .contains("observe requires one or more SELECT statements"),
            "unexpected error message: {}",
            error.description
        );
    }
);

#[test]
fn observe_sqlite_detects_external_insert_without_local_commit_stream_event() {
    run_local_observe_sqlite_case(
        "observe_sqlite_detects_external_insert_without_local_commit_stream_event",
        || async {
            let path = temp_sqlite_observe_path("external-insert");

            let engine_a = boot_sqlite_engine_at_path(path.clone());
            let engine_b = boot_sqlite_engine_at_path(path.clone());

            engine_a
                .initialize_if_needed()
                .await
                .expect("engine_a init should succeed");
            engine_b
                .initialize_if_needed()
                .await
                .expect("engine_b init should succeed");
            let session_a = Arc::clone(&engine_a);
            let session_b = Arc::clone(&engine_b);

            let mut observed = session_a
                .observe(ObserveQuery::new(
                    "SELECT path FROM lix_file WHERE path = '/observe-external.md'",
                    vec![],
                ))
                .expect("observe should succeed");

            let initial = observed
                .next()
                .await
                .expect("initial observe next should succeed")
                .expect("initial observe event should exist");
            assert!(initial.rows.rows.is_empty());

            session_b
                .execute(
                    "INSERT INTO lix_file (path, data) VALUES ('/observe-external.md', lix_text_encode('hello'))", &[])
                .await
                .expect("external insert should succeed");

            let update = tokio::time::timeout(Duration::from_secs(2), observed.next())
                .await
                .expect("observe next should not time out")
                .expect("observe next should succeed")
                .expect("observe update event should exist");
            assert_eq!(update.rows.rows.len(), 1);
            assert_eq!(
                update.rows.rows[0][0],
                Value::Text("/observe-external.md".to_string())
            );

            observed.close();
            drop(observed);
            drop(session_b);
            drop(session_a);
            drop(engine_b);
            drop(engine_a);
            cleanup_sqlite_path(&path);
        },
    );
}

#[test]
fn observe_sqlite_detects_external_untracked_state_insert() {
    run_local_observe_sqlite_case(
        "observe_sqlite_detects_external_untracked_state_insert",
        || async {
            let path = temp_sqlite_observe_path("external-untracked");

            let engine_a = boot_sqlite_engine_at_path(path.clone());
            let engine_b = boot_sqlite_engine_at_path(path.clone());

            engine_a
                .initialize_if_needed()
                .await
                .expect("engine_a init should succeed");
            engine_b
                .initialize_if_needed()
                .await
                .expect("engine_b init should succeed");
            let session_a = Arc::clone(&engine_a);
            let session_b = Arc::clone(&engine_b);

            let mut observed = session_a
                .observe(ObserveQuery::new(
                    "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'observe-untracked-external' \
                   AND untracked = true",
                    vec![],
                ))
                .expect("observe should succeed");

            let initial = observed
                .next()
                .await
                .expect("initial observe next should succeed")
                .expect("initial observe event should exist");
            assert!(initial.rows.rows.is_empty());

            session_b
                .execute(
                    "INSERT INTO lix_state (\
                 entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content, untracked\
                 ) VALUES (\
                 'observe-untracked-external', 'lix', 'lix_key_value', 'lix', '1', \
                 lix_json('{\"key\":\"observe-untracked-external\",\"value\":\"u1\"}'), true\
                 )", &[])
                .await
                .expect("external untracked insert should succeed");

            let update = tokio::time::timeout(Duration::from_secs(2), observed.next())
                .await
                .expect("observe next should not time out")
                .expect("observe next should succeed")
                .expect("observe update event should exist");
            assert_eq!(update.rows.rows.len(), 1);
            assert_eq!(
                update.rows.rows[0][0],
                Value::Text("observe-untracked-external".to_string())
            );

            observed.close();
            drop(observed);
            drop(session_b);
            drop(session_a);
            drop(engine_b);
            drop(engine_a);
            cleanup_sqlite_path(&path);
        },
    );
}

#[test]
fn observe_postgres_detects_external_insert_without_local_commit_stream_event() {
    run_local_observe_postgres_case(
        "observe_postgres_detects_external_insert_without_local_commit_stream_event",
        || async {
            let connection_string =
                support::simulations::create_postgres_test_database_url("observe-external-insert")
                    .await
                    .expect("postgres database url should be created");
            let engine_a = boot_postgres_engine_at_url(connection_string.clone());
            let engine_b = boot_postgres_engine_at_url(connection_string);

            engine_a
                .initialize_if_needed()
                .await
                .expect("engine_a init should succeed");
            engine_b
                .initialize_if_needed()
                .await
                .expect("engine_b init should succeed");
            let session_a = Arc::clone(&engine_a);
            let session_b = Arc::clone(&engine_b);

            let mut observed = session_a
                .observe(ObserveQuery::new(
                    "SELECT path FROM lix_file WHERE path = '/observe-external.md'",
                    vec![],
                ))
                .expect("observe should succeed");

            let initial = observed
                .next()
                .await
                .expect("initial observe next should succeed")
                .expect("initial observe event should exist");
            assert!(initial.rows.rows.is_empty());

            session_b
                .execute(
                    "INSERT INTO lix_file (path, data) VALUES ('/observe-external.md', lix_text_encode('hello'))", &[])
                .await
                .expect("external insert should succeed");

            let update = tokio::time::timeout(Duration::from_secs(2), observed.next())
                .await
                .expect("observe next should not time out")
                .expect("observe next should succeed")
                .expect("observe update event should exist");
            assert_eq!(update.rows.rows.len(), 1);
            assert_eq!(
                update.rows.rows[0][0],
                Value::Text("/observe-external.md".to_string())
            );
            assert_eq!(update.state_commit_sequence, None);
        },
    );
}

#[test]
fn observe_postgres_detects_external_untracked_state_insert() {
    run_local_observe_postgres_case(
        "observe_postgres_detects_external_untracked_state_insert",
        || async {
            let connection_string = support::simulations::create_postgres_test_database_url(
                "observe-external-untracked",
            )
            .await
            .expect("postgres database url should be created");
            let engine_a = boot_postgres_engine_at_url(connection_string.clone());
            let engine_b = boot_postgres_engine_at_url(connection_string);

            engine_a
                .initialize_if_needed()
                .await
                .expect("engine_a init should succeed");
            engine_b
                .initialize_if_needed()
                .await
                .expect("engine_b init should succeed");
            let session_a = Arc::clone(&engine_a);
            let session_b = Arc::clone(&engine_b);

            let mut observed = session_a
                .observe(ObserveQuery::new(
                    "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'observe-untracked-external' \
                   AND untracked = true",
                    vec![],
                ))
                .expect("observe should succeed");

            let initial = observed
                .next()
                .await
                .expect("initial observe next should succeed")
                .expect("initial observe event should exist");
            assert!(initial.rows.rows.is_empty());

            session_b
                .execute(
                    "INSERT INTO lix_state (\
                 entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content, untracked\
                 ) VALUES (\
                 'observe-untracked-external', 'lix', 'lix_key_value', 'lix', '1', \
                 lix_json('{\"key\":\"observe-untracked-external\",\"value\":\"u1\"}'), true\
                 )", &[])
                .await
                .expect("external untracked insert should succeed");

            let update = tokio::time::timeout(Duration::from_secs(2), observed.next())
                .await
                .expect("observe next should not time out")
                .expect("observe next should succeed")
                .expect("observe update event should exist");
            assert_eq!(update.rows.rows.len(), 1);
            assert_eq!(
                update.rows.rows[0][0],
                Value::Text("observe-untracked-external".to_string())
            );
            assert_eq!(update.state_commit_sequence, None);
        },
    );
}

#[test]
fn observe_external_same_writer_key_is_suppressed() {
    run_local_observe_postgres_case("observe_external_same_writer_key_is_suppressed", || async {
        let writer = "observe-external-writer";
        let connection_string =
            support::simulations::create_postgres_test_database_url("observe-same-writer")
                .await
                .expect("postgres database url should be created");
        let engine_a = boot_postgres_engine_at_url(connection_string.clone());
        let engine_b = boot_postgres_engine_at_url(connection_string);

        engine_a
            .initialize_if_needed()
            .await
            .expect("engine_a init should succeed");
        engine_b
            .initialize_if_needed()
            .await
            .expect("engine_b init should succeed");
        let session_a = Arc::clone(&engine_a);
        let session_b = Arc::clone(&engine_b);

        let mut observed = session_a
            .observe(ObserveQuery::new(
                "SELECT path \
                 FROM lix_file \
                 WHERE path = '/observe-writer.md' \
                   AND (lixcol_writer_key IS NULL OR lixcol_writer_key <> ?1)",
                vec![Value::Text(writer.to_string())],
            ))
            .expect("observe should succeed");
        let initial = observed
            .next()
            .await
            .expect("initial observe next should succeed")
            .expect("initial observe event should exist");
        assert!(initial.rows.rows.is_empty());

        session_b
            .execute_with_options(
                "INSERT INTO lix_file (path, data) VALUES ('/observe-writer.md', lix_text_encode('same-writer'))",
                &[],
                ExecuteOptions {
                    writer_key: Some(writer.to_string()),
                },
            )
            .await
            .expect("external insert should succeed");

        let timed = tokio::time::timeout(Duration::from_millis(800), observed.next()).await;
        assert!(
            timed.is_err(),
            "same writer key should suppress observe emission"
        );
    });
}

#[test]
fn observe_external_different_writer_key_emits() {
    run_local_observe_postgres_case("observe_external_different_writer_key_emits", || async {
        let connection_string =
            support::simulations::create_postgres_test_database_url("observe-different-writer")
                .await
                .expect("postgres database url should be created");
        let engine_a = boot_postgres_engine_at_url(connection_string.clone());
        let engine_b = boot_postgres_engine_at_url(connection_string);

        engine_a
            .initialize_if_needed()
            .await
            .expect("engine_a init should succeed");
        engine_b
            .initialize_if_needed()
            .await
            .expect("engine_b init should succeed");
        let session_a = Arc::clone(&engine_a);
        let session_b = Arc::clone(&engine_b);

        let mut observed = session_a
            .observe(ObserveQuery::new(
                "SELECT path \
                 FROM lix_file \
                 WHERE path = '/observe-writer-different.md' \
                   AND (lixcol_writer_key IS NULL OR lixcol_writer_key <> ?1)",
                vec![Value::Text("observer-writer".to_string())],
            ))
            .expect("observe should succeed");
        let initial = observed
            .next()
            .await
            .expect("initial observe next should succeed")
            .expect("initial observe event should exist");
        assert!(initial.rows.rows.is_empty());

        session_b
            .execute_with_options(
                "INSERT INTO lix_file (path, data) VALUES ('/observe-writer-different.md', lix_text_encode('different-writer'))",
                &[],
                ExecuteOptions {
                    writer_key: Some("different-writer".to_string()),
                },
            )
            .await
            .expect("external insert should succeed");

        let update = tokio::time::timeout(Duration::from_secs(2), observed.next())
            .await
            .expect("observe next should not time out")
            .expect("observe next should succeed")
            .expect("observe update event should exist");
        assert_eq!(update.rows.rows.len(), 1);
        assert_eq!(update.state_commit_sequence, None);
    });
}

#[test]
fn observe_external_null_writer_key_emits() {
    run_local_observe_postgres_case("observe_external_null_writer_key_emits", || async {
        let connection_string =
            support::simulations::create_postgres_test_database_url("observe-null-writer")
                .await
                .expect("postgres database url should be created");
        let engine_a = boot_postgres_engine_at_url(connection_string.clone());
        let engine_b = boot_postgres_engine_at_url(connection_string);

        engine_a
            .initialize_if_needed()
            .await
            .expect("engine_a init should succeed");
        engine_b
            .initialize_if_needed()
            .await
            .expect("engine_b init should succeed");
        let session_a = Arc::clone(&engine_a);
        let session_b = Arc::clone(&engine_b);

        let mut observed = session_a
            .observe(ObserveQuery::new(
                "SELECT path \
                 FROM lix_file \
                 WHERE path = '/observe-writer-null.md' \
                   AND (lixcol_writer_key IS NULL OR lixcol_writer_key <> ?1)",
                vec![Value::Text("observer-writer".to_string())],
            ))
            .expect("observe should succeed");
        let initial = observed
            .next()
            .await
            .expect("initial observe next should succeed")
            .expect("initial observe event should exist");
        assert!(initial.rows.rows.is_empty());

        session_b
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ('/observe-writer-null.md', lix_text_encode('null-writer'))", &[])
            .await
            .expect("external insert should succeed");

        let update = tokio::time::timeout(Duration::from_secs(2), observed.next())
            .await
            .expect("observe next should not time out")
            .expect("observe next should succeed")
            .expect("observe update event should exist");
        assert_eq!(update.rows.rows.len(), 1);
        assert_eq!(update.state_commit_sequence, None);
    });
}

#[test]
fn observe_external_read_only_transaction_does_not_emit() {
    run_local_observe_postgres_case(
        "observe_external_read_only_transaction_does_not_emit",
        || async {
            let connection_string =
                support::simulations::create_postgres_test_database_url("observe-read-only")
                    .await
                    .expect("postgres database url should be created");
            let engine_a = boot_postgres_engine_at_url(connection_string.clone());
            let engine_b = boot_postgres_engine_at_url(connection_string);

            engine_a
                .initialize_if_needed()
                .await
                .expect("engine_a init should succeed");
            engine_b
                .initialize_if_needed()
                .await
                .expect("engine_b init should succeed");
            let session_a = Arc::clone(&engine_a);
            let session_b = Arc::clone(&engine_b);

            let mut observed = session_a
                .observe(ObserveQuery::new(
                    "SELECT path FROM lix_file WHERE path = '/observe-read-only-tx.md'",
                    vec![],
                ))
                .expect("observe should succeed");
            let initial = observed
                .next()
                .await
                .expect("initial observe next should succeed")
                .expect("initial observe event should exist");
            assert!(initial.rows.rows.is_empty());

            let mut tx = session_b
                .begin_transaction_with_options(ExecuteOptions::default())
                .await
                .expect("begin transaction should succeed");
            tx.execute("SELECT 1", &[])
                .await
                .expect("read-only statement should succeed");
            tx.commit().await.expect("commit should succeed");

            let timed = tokio::time::timeout(Duration::from_millis(800), observed.next()).await;
            assert!(timed.is_err(), "read-only external tx should not emit");
        },
    );
}

#[test]
fn observe_external_mutating_transaction_emits_once_for_result_delta() {
    run_local_observe_postgres_case(
        "observe_external_mutating_transaction_emits_once_for_result_delta",
        || async {
            let connection_string =
                support::simulations::create_postgres_test_database_url("observe-mutating")
                    .await
                    .expect("postgres database url should be created");
            let engine_a = boot_postgres_engine_at_url(connection_string.clone());
            let engine_b = boot_postgres_engine_at_url(connection_string);

            engine_a
                .initialize_if_needed()
                .await
                .expect("engine_a init should succeed");
            engine_b
                .initialize_if_needed()
                .await
                .expect("engine_b init should succeed");
            let session_a = Arc::clone(&engine_a);
            let session_b = Arc::clone(&engine_b);

            let mut observed = session_a
                .observe(ObserveQuery::new(
                    "SELECT lix_text_decode(data) \
                     FROM lix_file \
                     WHERE path = '/observe-external-tx.md'",
                    vec![],
                ))
                .expect("observe should succeed");
            let initial = observed
                .next()
                .await
                .expect("initial observe next should succeed")
                .expect("initial observe event should exist");
            assert!(initial.rows.rows.is_empty());

            let mut tx = session_b
                .begin_transaction_with_options(ExecuteOptions::default())
                .await
                .expect("begin transaction should succeed");
            tx.execute(
                "INSERT INTO lix_file (path, data) VALUES ('/observe-external-tx.md', lix_text_encode('before'))",
                &[],
            )
            .await
            .expect("insert should succeed");
            tx.execute(
                "UPDATE lix_file SET data = lix_text_encode('after') WHERE path = '/observe-external-tx.md'",
                &[],
            )
            .await
            .expect("update should succeed");
            tx.commit().await.expect("commit should succeed");

            let update = tokio::time::timeout(Duration::from_secs(2), observed.next())
                .await
                .expect("observe next should not time out")
                .expect("observe next should succeed")
                .expect("observe update event should exist");
            assert_eq!(update.rows.rows.len(), 1);
            assert_eq!(update.rows.rows[0][0], Value::Text("after".to_string()));
            assert_eq!(update.state_commit_sequence, None);

            let timed = tokio::time::timeout(Duration::from_millis(600), observed.next()).await;
            assert!(
                timed.is_err(),
                "single external commit should not emit an extra duplicate event"
            );
        },
    );
}

#[test]
fn observe_external_unrelated_mutation_does_not_emit() {
    run_local_observe_postgres_case(
        "observe_external_unrelated_mutation_does_not_emit",
        || async {
            let connection_string =
                support::simulations::create_postgres_test_database_url("observe-unrelated")
                    .await
                    .expect("postgres database url should be created");
            let engine_a = boot_postgres_engine_at_url(connection_string.clone());
            let engine_b = boot_postgres_engine_at_url(connection_string);

            engine_a
                .initialize_if_needed()
                .await
                .expect("engine_a init should succeed");
            engine_b
                .initialize_if_needed()
                .await
                .expect("engine_b init should succeed");
            let session_a = Arc::clone(&engine_a);
            let session_b = Arc::clone(&engine_b);

            let mut observed = session_a
                .observe(ObserveQuery::new(
                    "SELECT path FROM lix_file WHERE path = '/observe-unrelated-target.md'",
                    vec![],
                ))
                .expect("observe should succeed");
            let initial = observed
                .next()
                .await
                .expect("initial observe next should succeed")
                .expect("initial observe event should exist");
            assert!(initial.rows.rows.is_empty());

            session_b
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ('/observe-unrelated-other.md', lix_text_encode('other'))", &[])
            .await
            .expect("external insert should succeed");

            let timed = tokio::time::timeout(Duration::from_millis(800), observed.next()).await;
            assert!(
                timed.is_err(),
                "unrelated external mutation should not emit when result stays identical"
            );
        },
    );
}

fn boot_sqlite_engine_at_path(path: PathBuf) -> Arc<Lix> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("sqlite test parent directory should be creatable");
    }
    let _ = std::fs::File::create(&path).expect("sqlite test file should be creatable");
    Arc::new(Lix::boot(LixConfig::new(
        support::simulations::sqlite_backend_with_filename(format!(
            "sqlite://{}",
            path.to_string_lossy()
        )),
        Arc::new(NoopWasmRuntime),
    )))
}

fn boot_postgres_engine_at_url(connection_string: String) -> Arc<Lix> {
    Arc::new(Lix::boot(LixConfig::new(
        support::simulations::postgres_backend_with_connection_string(connection_string),
        Arc::new(NoopWasmRuntime),
    )))
}

fn temp_sqlite_observe_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lix-observe-{label}-{}-{nanos}.sqlite",
        std::process::id()
    ))
}

fn cleanup_sqlite_path(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let wal = PathBuf::from(format!("{}-wal", path.to_string_lossy()));
    let shm = PathBuf::from(format!("{}-shm", path.to_string_lossy()));
    let journal = PathBuf::from(format!("{}-journal", path.to_string_lossy()));
    let _ = std::fs::remove_file(wal);
    let _ = std::fs::remove_file(shm);
    let _ = std::fs::remove_file(journal);
}

fn run_local_observe_sqlite_case<F, Fut>(name: &'static str, case: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let run_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build tokio runtime");
                runtime.block_on(case());
            }));
            let _ = result_tx.send(run_result);
        })
        .expect("failed to spawn observe sqlite test thread");

    match result_rx.recv_timeout(Duration::from_secs(120)) {
        Ok(Ok(())) => {
            thread.join().expect("observe sqlite test thread panicked");
        }
        Ok(Err(payload)) => {
            let _ = thread.join();
            std::panic::resume_unwind(payload);
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("observe sqlite case timed out after 120s: {name}");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            if let Err(payload) = thread.join() {
                std::panic::resume_unwind(payload);
            }
            panic!("observe sqlite case disconnected without result: {name}");
        }
    }
}

fn run_local_observe_postgres_case<F, Fut>(name: &'static str, case: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let run_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build tokio runtime");
                runtime.block_on(case());
            }));
            let _ = result_tx.send(run_result);
        })
        .expect("failed to spawn observe postgres test thread");

    match result_rx.recv_timeout(Duration::from_secs(120)) {
        Ok(Ok(())) => {
            thread
                .join()
                .expect("observe postgres test thread panicked");
        }
        Ok(Err(payload)) => {
            let _ = thread.join();
            std::panic::resume_unwind(payload);
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("observe postgres case timed out after 120s: {name}");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            if let Err(payload) = thread.join() {
                std::panic::resume_unwind(payload);
            }
            panic!("observe postgres case disconnected without result: {name}");
        }
    }
}
