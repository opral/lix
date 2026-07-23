#[macro_use]
mod support;

use lix_engine::{CreateBranchOptions, Value};
use serde_json::json;

simulation_test!(
    checkpoint_gc_keeps_one_recovery_interval_then_sweeps_it,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('gc-key', 'interval-one')",
                &[],
            )
            .await
            .expect("first interval write should succeed");
        let interval_one_first = branch_head(&engine, sim.main_branch_id()).await;
        session
            .fs
            .write_file("/gc/data.bin", b"interval-one-blob".to_vec())
            .await
            .expect("first interval blob write should succeed");
        let interval_one_second = branch_head(&engine, sim.main_branch_id()).await;

        let checkpoint_two = session
            .create_checkpoint()
            .await
            .expect("second checkpoint should succeed");
        assert_commits(&session, &[&interval_one_first, &interval_one_second], true).await;

        session
            .execute(
                "UPDATE lix_key_value SET value = 'interval-two' WHERE key = 'gc-key'",
                &[],
            )
            .await
            .expect("second interval state write should succeed");
        let interval_two_first = branch_head(&engine, sim.main_branch_id()).await;
        session
            .fs
            .write_file("/gc/data.bin", b"interval-two-blob".to_vec())
            .await
            .expect("second interval blob write should succeed");
        let interval_two_second = branch_head(&engine, sim.main_branch_id()).await;

        let checkpoint_three = session
            .create_checkpoint()
            .await
            .expect("third checkpoint should succeed");

        assert_commits(
            &session,
            &[&interval_one_first, &interval_one_second],
            false,
        )
        .await;
        assert_commits(&session, &[&interval_two_first, &interval_two_second], true).await;
        assert_commits(
            &session,
            &[&checkpoint_two.commit_id, &checkpoint_three.commit_id],
            true,
        )
        .await;

        let state = session
            .execute("SELECT value FROM lix_key_value WHERE key = 'gc-key'", &[])
            .await
            .expect("current state should remain readable after collection");
        assert_eq!(
            state.rows()[0].values(),
            &[Value::Json(json!("interval-two"))]
        );
        assert_eq!(
            session
                .fs
                .read_file("/gc/data.bin")
                .await
                .expect("current blob should remain readable after collection"),
            Some(b"interval-two-blob".to_vec())
        );
    }
);

simulation_test!(
    checkpoint_gc_keeps_commits_referenced_by_another_branch,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('branch-gc-key', 'protected')",
            &[],
        )
        .await
        .expect("protected interval write should succeed");
        let protected_first = branch_head(&engine, sim.main_branch_id()).await;
        main.execute(
            "UPDATE lix_key_value SET value = 'protected-head' WHERE key = 'branch-gc-key'",
            &[],
        )
        .await
        .expect("protected interval update should succeed");
        let protected_head = branch_head(&engine, sim.main_branch_id()).await;
        main.create_checkpoint()
            .await
            .expect("checkpoint retaining the first interval should succeed");

        main.create_branch(CreateBranchOptions {
            id: Some("gc-protected-branch".to_string()),
            name: "GC protected branch".to_string(),
            from_commit_id: Some(protected_head.clone()),
        })
        .await
        .expect("branch should be created from the recoverable auto-commit");
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('main-after-branch', 'main')",
            &[],
        )
        .await
        .expect("next main interval write should succeed");
        main.create_checkpoint()
            .await
            .expect("checkpoint rotating the main recovery root should succeed");

        assert_commits(&main, &[&protected_first, &protected_head], true).await;

        let protected = sim.wrap_session(
            engine
                .open_session("gc-protected-branch")
                .await
                .expect("protected branch session should open"),
            &engine,
        );
        let state = protected
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'branch-gc-key'",
                &[],
            )
            .await
            .expect("protected branch state should remain readable");
        assert_eq!(
            state.rows()[0].values(),
            &[Value::Json(json!("protected-head"))]
        );
    }
);

async fn branch_head(engine: &lix_engine::Engine, branch_id: &str) -> String {
    engine
        .load_branch_head_commit_id(branch_id)
        .await
        .expect("branch head should load")
        .expect("branch head should exist")
}

async fn assert_commits(
    session: &support::simulation_test::engine::SimSession,
    commit_ids: &[&str],
    expected_present: bool,
) {
    for commit_id in commit_ids {
        let result = session
            .execute(
                &format!("SELECT id FROM lix_commit WHERE id = '{commit_id}'"),
                &[],
            )
            .await
            .expect("commit existence query should succeed");
        assert_eq!(
            !result.is_empty(),
            expected_present,
            "unexpected reachability for commit {commit_id}"
        );
    }
}
