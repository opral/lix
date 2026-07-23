use lix_engine::{LixError, Value};
use serde_json::json;

use super::select_rows;

simulation_test!(
    checkpoint_compacts_working_interval_and_projects_sql_surfaces,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );
        let initial_commit_id = sim.initial_commit_id().to_string();

        assert_eq!(
            select_rows(
                &session,
                "SELECT commit_id, lixcol_depth FROM lix_checkpoint ORDER BY lixcol_depth",
            )
            .await,
            vec![vec![
                Value::Text(initial_commit_id.clone()),
                Value::Integer(0),
            ]]
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-key', 'one')",
                &[],
            )
            .await
            .expect("tracked insert should succeed");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'checkpoint-key'",
                &[],
            )
            .await
            .expect("tracked update should succeed");
        let old_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist");

        assert_eq!(
            select_rows(
                &session,
                "SELECT entity_pk, schema_key, change_kind \
                 FROM lix_working_change ORDER BY schema_key, entity_pk",
            )
            .await,
            vec![vec![
                Value::Json(json!(["checkpoint-key"])),
                Value::Text("lix_key_value".to_string()),
                Value::Text("added".to_string()),
            ]]
        );

        let receipt = session
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");
        assert_ne!(receipt.commit_id, old_head);
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("head should load"),
            Some(receipt.commit_id.clone())
        );

        assert_eq!(
            select_rows(&session, "SELECT COUNT(*) FROM lix_working_change").await,
            vec![vec![Value::Integer(0)]]
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT commit_id, lixcol_depth FROM lix_checkpoint ORDER BY lixcol_depth",
            )
            .await,
            vec![
                vec![Value::Text(receipt.commit_id.clone()), Value::Integer(0),],
                vec![Value::Text(initial_commit_id.clone()), Value::Integer(1)],
            ]
        );
        assert_eq!(
            select_rows(
                &session,
                &format!(
                    "SELECT parent_id FROM lix_commit_edge \
                     WHERE child_id = '{}'",
                    receipt.commit_id
                ),
            )
            .await,
            vec![vec![Value::Text(initial_commit_id)]]
        );
        assert_eq!(
            select_rows(
                &session,
                "SELECT value FROM lix_key_value WHERE key = 'checkpoint-key'",
            )
            .await,
            vec![vec![Value::Json(json!("two"))]]
        );

        let timestamps_before_rebuild = select_rows(
            &session,
            "SELECT lixcol_created_at, lixcol_updated_at \
             FROM lix_key_value WHERE key = 'checkpoint-key'",
        )
        .await;
        assert_eq!(timestamps_before_rebuild.len(), 1);
        assert_eq!(
            timestamps_before_rebuild[0][0], timestamps_before_rebuild[0][1],
            "a newly added row must use the changelog's canonical timestamp"
        );

        engine
            .rebuild_tracked_state_for_branch(sim.main_branch_id())
            .await
            .expect("checkpoint tracked state should rebuild");
        assert_eq!(
            select_rows(
                &session,
                "SELECT lixcol_created_at, lixcol_updated_at \
                 FROM lix_key_value WHERE key = 'checkpoint-key'",
            )
            .await,
            timestamps_before_rebuild,
            "checkpoint timestamps must remain stable after tracked-state rebuild"
        );
    }
);

simulation_test!(
    checkpoint_surfaces_are_branch_explicit_and_read_only,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        let rows = select_rows(
            &session,
            "SELECT lixcol_branch_id, commit_id \
             FROM lix_checkpoint_by_branch \
             ORDER BY lixcol_branch_id, lixcol_depth",
        )
        .await;
        assert_eq!(
            rows,
            vec![vec![
                Value::Text(sim.main_branch_id().to_string()),
                Value::Text(sim.initial_commit_id().to_string()),
            ]]
        );

        for sql in [
            "INSERT INTO lix_checkpoint (commit_id, created_at, lixcol_depth) \
             VALUES ('fake', '2026-01-01T00:00:00Z', 0)",
            "UPDATE lix_checkpoint SET created_at = 'fake'",
            "DELETE FROM lix_working_change",
            "UPDATE lix_working_change_by_branch SET change_kind = 'fake'",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("checkpoint SQL surface should be read-only");
            assert_eq!(error.code, LixError::CODE_READ_ONLY);
        }
    }
);
