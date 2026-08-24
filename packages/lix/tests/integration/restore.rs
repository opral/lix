use lix::{CreateBranchOptions, LixError, SwitchBranchOptions, Value};
use serde_json::json;

use crate::support::simulation_test::engine::SimSession;

const RESTORE_SQL: &str =
    "INSERT INTO lix_restore (commit_id) VALUES ($1) RETURNING commit_id";

simulation_test!(
    restore_moves_only_the_active_branch_to_an_ancestor,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let initial_commit_id = sim.initial_commit_id().to_string();
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/a.txt', CAST('a' AS BYTEA))",
                &[],
            )
            .await
            .expect("first file should commit");
        let target_commit_id = head(&session).await;

        let other_branch = session
            .create_branch(CreateBranchOptions {
                id: None,
                name: "restore-control".to_string(),
                from_commit_id: None,
            })
            .await
            .expect("control branch should be created");

        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/b.txt', CAST('b' AS BYTEA))",
                &[],
            )
            .await
            .expect("second file should commit");
        let abandoned_head = head(&session).await;
        let commit_count_before = count(&session, "lix_commit").await;

        restore(&session, &target_commit_id)
            .await
            .expect("ancestor restore should succeed");
        assert_eq!(head(&session).await, target_commit_id);
        assert_eq!(count(&session, "lix_commit").await, commit_count_before);
        assert_eq!(count(&session, "lix_file").await, 1);
        let files = session
            .execute("SELECT path FROM lix_file ORDER BY path", &[])
            .await
            .expect("files should read");
        assert_eq!(files.rows()[0].get::<String>("path").unwrap(), "/a.txt");
        let abandoned_commit = session
            .execute(
                "SELECT COUNT(*) AS count FROM lix_commit WHERE id = $1",
                &[Value::Text(abandoned_head)],
            )
            .await
            .expect("abandoned commit should remain stored");
        assert_eq!(
            abandoned_commit.rows()[0].get::<i64>("count").unwrap(),
            1,
            "restore must not delete orphaned commits"
        );

        session
            .switch_branch(SwitchBranchOptions {
                branch_id: other_branch.id,
            })
            .await
            .expect("control branch should still exist");
        assert_eq!(head(&session).await, other_branch.commit_id);
        assert_eq!(count(&session, "lix_file").await, 1);

        restore(&session, &initial_commit_id)
            .await
            .expect("parentless commit should be restorable");
        assert_eq!(head(&session).await, initial_commit_id);
        assert_eq!(count(&session, "lix_file").await, 0);
    }
);

simulation_test!(
    restore_noop_and_errors_leave_head_unchanged,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('main', 'one')",
                &[],
            )
            .await
            .expect("main change should commit");
        let main_branch_id = session
            .active_branch_id()
            .await
            .expect("main branch id should read");
        let main_head = head(&session).await;
        let commit_count = count(&session, "lix_commit").await;

        restore(&session, &main_head)
            .await
            .expect("restoring HEAD should be a no-op");
        assert_eq!(head(&session).await, main_head);
        assert_eq!(count(&session, "lix_commit").await, commit_count);

        let missing = "01990000-0000-7000-8000-00000000dead";
        let missing_error = restore(&session, missing)
            .await
            .expect_err("missing target should fail");
        assert_eq!(missing_error.code, LixError::CODE_COMMIT_NOT_FOUND);
        assert_eq!(head(&session).await, main_head);

        let fork = session
            .create_branch(CreateBranchOptions {
                id: None,
                name: "unrelated-restore-target".to_string(),
                from_commit_id: None,
            })
            .await
            .expect("fork should be created");
        session
            .switch_branch(SwitchBranchOptions {
                branch_id: fork.id.clone(),
            })
            .await
            .expect("fork should be active");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'fork' WHERE key = 'main'",
                &[],
            )
            .await
            .expect("fork change should commit");
        let fork_head = head(&session).await;

        session
            .switch_branch(SwitchBranchOptions {
                branch_id: main_branch_id,
            })
            .await
            .expect("main should be active again");
        let commit_count_before_rejection = count(&session, "lix_commit").await;
        let error = restore(&session, &fork_head)
            .await
            .expect_err("non-ancestor target should fail");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert_eq!(head(&session).await, main_head);
        assert_eq!(
            count(&session, "lix_commit").await,
            commit_count_before_rejection
        );
    }
);

simulation_test!(
    restore_keeps_checkpoint_rows_for_orphaned_commits,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpointed', 'one')",
                &[],
            )
            .await
            .expect("first value should commit");
        let first_checkpoint = session
            .create_checkpoint()
            .await
            .expect("first checkpoint should commit");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'checkpointed'",
                &[],
            )
            .await
            .expect("second value should commit");
        let orphaned_checkpoint = session
            .create_checkpoint()
            .await
            .expect("second checkpoint should commit");
        let commit_count_before = count(&session, "lix_commit").await;

        restore(&session, &first_checkpoint.commit_id)
            .await
            .expect("earlier checkpoint should be restorable");

        assert_eq!(head(&session).await, first_checkpoint.commit_id);
        assert_eq!(count(&session, "lix_commit").await, commit_count_before);
        let value = session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'checkpointed'",
                &[],
            )
            .await
            .expect("restored value should read");
        assert_eq!(
            value.rows()[0].get::<serde_json::Value>("value").unwrap(),
            json!("one")
        );
        let checkpoint_row = session
            .execute(
                "SELECT COUNT(*) AS count FROM lix_checkpoint WHERE commit_id = $1",
                &[Value::Text(orphaned_checkpoint.commit_id)],
            )
            .await
            .expect("orphaned checkpoint row should read");
        assert_eq!(
            checkpoint_row.rows()[0].get::<i64>("count").unwrap(),
            1,
            "restore must retain checkpoint markers for orphaned commits"
        );

        let undo_error = session
            .undo()
            .await
            .expect_err("restore target should start a fresh undo interval");
        assert_eq!(undo_error.code, LixError::CODE_NOTHING_TO_UNDO);

        session
            .execute(
                "UPDATE lix_key_value SET value = 'after-restore' WHERE key = 'checkpointed'",
                &[],
            )
            .await
            .expect("tracked writes should work after restore");
        session
            .undo()
            .await
            .expect("a new edit after restore should be undoable");
        let after_undo = session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'checkpointed'",
                &[],
            )
            .await
            .expect("state after undo should read");
        assert_eq!(
            after_undo.rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            json!("one"),
            "undo after restore must reverse the new edit, not the restore"
        );
        session
            .create_checkpoint()
            .await
            .expect("checkpoint cursor should remain usable after restore");
    }
);

simulation_test!(
    restore_sql_shape_and_explicit_transaction_semantics,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('restore-sql', 'c')",
                &[],
            )
            .await
            .expect("C commits");
        let commit_c = head(&session).await;
        session
            .execute(
                "UPDATE lix_key_value SET value = 'd' WHERE key = 'restore-sql'",
                &[],
            )
            .await
            .expect("D commits");
        let commit_d = head(&session).await;

        for sql in [
            "SELECT lix_restore()",
            "SELECT upper(lix_restore($1))",
            "SELECT lix_restore($1)",
            "SELECT lix_restore($1), 1",
        ] {
            session
                .execute(sql, &[Value::Text(commit_c.clone())])
                .await
                .expect_err("removed scalar restore syntax must fail");
            assert_eq!(head(&session).await, commit_d, "{sql}");
        }

        for value in [Value::Null, Value::Integer(1)] {
            let error = session
                .execute(RESTORE_SQL, &[value])
                .await
                .expect_err("non-text restore target must fail");
            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
            assert_eq!(head(&session).await, commit_d);
        }
        let missing_param = session
            .execute(RESTORE_SQL, &[])
            .await
            .expect_err("missing restore parameter must fail");
        assert_eq!(missing_param.code, LixError::CODE_INVALID_PARAM);
        assert_eq!(head(&session).await, commit_d);

        let mut rolled_back = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        rolled_back
            .execute(RESTORE_SQL, &[Value::Text(commit_c.clone())])
            .await
            .expect("transaction restore should stage");
        rolled_back
            .rollback()
            .await
            .expect("rollback should succeed");
        assert_eq!(head(&session).await, commit_d);

        let mut write_first = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        write_first
            .execute(
                "UPDATE lix_key_value SET value = 'pending' WHERE key = 'restore-sql'",
                &[],
            )
            .await
            .expect("earlier write should stage");
        let restore_after_write = write_first
            .execute(RESTORE_SQL, &[Value::Text(commit_c.clone())])
            .await
            .expect_err("restore cannot follow another write");
        assert_eq!(restore_after_write.code, "LIX_INVALID_TRANSACTION_STATE");
        write_first
            .rollback()
            .await
            .expect("mixed transaction should roll back");
        assert_eq!(head(&session).await, commit_d);
        let durable_value = session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'restore-sql'",
                &[],
            )
            .await
            .expect("durable value should read");
        assert_eq!(
            durable_value.rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            json!("d")
        );

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(RESTORE_SQL, &[Value::Text(commit_c.clone())])
            .await
            .expect("first restore should stage");
        let stale_read_error = transaction
            .execute("SELECT lix_active_branch_commit_id()", &[])
            .await
            .expect_err("reads after restore must be rejected");
        assert_eq!(stale_read_error.code, "LIX_INVALID_TRANSACTION_STATE");
        let second_error = transaction
            .execute(RESTORE_SQL, &[Value::Text(commit_d.clone())])
            .await
            .expect_err("a second restore in one transaction should fail");
        assert_eq!(second_error.code, "LIX_INVALID_TRANSACTION_STATE");
        transaction
            .commit()
            .await
            .expect("first restore should commit");
        assert_eq!(head(&session).await, commit_c);
    }
);

simulation_test!(
    restore_preserves_branch_local_untracked_rows,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('tracked', 'target')",
                &[],
            )
            .await
            .expect("target state should commit");
        let target = head(&session).await;
        session
            .execute(
                "UPDATE lix_key_value SET value = 'later' WHERE key = 'tracked'",
                &[],
            )
            .await
            .expect("later state should commit");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ('local', 'kept', true)",
                &[],
            )
            .await
            .expect("branch-local untracked state should insert");

        restore(&session, &target)
            .await
            .expect("restore should retain untracked state");
        assert_eq!(head(&session).await, target);

        let values = session
            .execute(
                "SELECT key, value, lixcol_untracked FROM lix_key_value WHERE key IN ('local', 'tracked') ORDER BY key",
                &[],
            )
            .await
            .expect("restored state should read");
        assert_eq!(values.rows().len(), 2);
        assert_eq!(values.rows()[0].get::<String>("key").unwrap(), "local");
        assert_eq!(
            values.rows()[0].get::<serde_json::Value>("value").unwrap(),
            json!("kept")
        );
        assert!(values.rows()[0].get::<bool>("lixcol_untracked").unwrap());
        assert_eq!(values.rows()[1].get::<String>("key").unwrap(), "tracked");
        assert_eq!(
            values.rows()[1].get::<serde_json::Value>("value").unwrap(),
            json!("target")
        );
        assert!(!values.rows()[1].get::<bool>("lixcol_untracked").unwrap());
    }
);

async fn head(session: &SimSession) -> String {
    let result = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("HEAD should read");
    result.rows()[0]
        .get::<String>("commit_id")
        .expect("HEAD should be text")
}

async fn restore(session: &SimSession, commit_id: &str) -> Result<(), LixError> {
    let result = session
        .execute(RESTORE_SQL, &[Value::Text(commit_id.to_string())])
        .await?;
    assert_eq!(result.columns(), &["commit_id"]);
    assert_eq!(
        result.rows()[0].get::<String>("commit_id").unwrap(),
        commit_id
    );
    Ok(())
}

async fn count(session: &SimSession, table: &str) -> i64 {
    let result = session
        .execute(&format!("SELECT COUNT(*) AS count FROM {table}"), &[])
        .await
        .expect("count should read");
    result.rows()[0]
        .get::<i64>("count")
        .expect("count should be integer")
}
