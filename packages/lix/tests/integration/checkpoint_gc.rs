use crate::support;
use lix::{CreateBranchOptions, Value};
use serde_json::json;
use tokio::time::{Duration, Instant};

const CHECKPOINT_GC_INTERVAL: u64 = 64;
const REPLAY_GC_SCHEMA_KEY: &str = "checkpoint_replay_gc_row";

simulation_test!(
    checkpoint_gc_keeps_one_recovery_interval_then_sweeps_it,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
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

        advance_to_next_gc(&session, 1).await;
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

        wait_for_commits(
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
            &[Value::Jsonb(json!("interval-two").into())]
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
    checkpoint_gc_keeps_then_reclaims_recovered_branch_interval,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
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
        let compacted_checkpoint = main
            .create_checkpoint()
            .await
            .expect("checkpoint retaining the first interval should succeed")
            .commit_id;

        main.create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-000000000510".to_string()),
            name: "GC protected branch".to_string(),
            from_commit_id: Some(protected_head.clone()),
        })
        .await
        .expect("branch should be created from the recoverable auto-commit");
        let protected = sim.wrap_session(
            engine
                .open_session_at("01920000-0000-7000-8000-000000000510")
                .await
                .expect("protected branch session should open"),
            &engine,
        );
        protected
            .execute(
                "UPDATE lix_key_value SET value = 'protected-source' WHERE key = 'branch-gc-key'",
                &[],
            )
            .await
            .expect("first source commit should consume the checkpoint bridge");
        let source_head = branch_head(&engine, "01920000-0000-7000-8000-000000000510").await;
        assert_eq!(
            commit_parent_edges(&main, &source_head).await,
            vec![(protected_head.clone(), 0), (compacted_checkpoint, 1),],
        );
        advance_to_next_gc(&main, 1).await;
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

        let state = protected
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'branch-gc-key'",
                &[],
            )
            .await
            .expect("protected branch state should remain readable");
        assert_eq!(
            state.rows()[0].values(),
            &[Value::Jsonb(json!("protected-source").into())]
        );

        drop(protected);
        main.execute(
            "DELETE FROM lix_branch WHERE id = '01920000-0000-7000-8000-000000000510'",
            &[],
        )
        .await
        .expect("protected branch should delete");
        for _ in 0..CHECKPOINT_GC_INTERVAL {
            main.create_checkpoint()
                .await
                .expect("post-delete GC checkpoint should succeed");
        }
        // This contract covers the recovered pre-checkpoint interval. The
        // deleted branch's final head has independent history/undo ownership.
        assert_commits(&main, &[&protected_first, &protected_head], true).await;
    }
);

simulation_test!(
    checkpoint_gc_aggregates_recovery_intervals_across_branches,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        main.create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-000000000511".to_string()),
            name: "GC other branch".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("other branch should be created");
        let other = sim.wrap_session(
            engine
                .open_session_at("01920000-0000-7000-8000-000000000511")
                .await
                .expect("other branch session should open"),
            &engine,
        );

        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('gc-main', 'main')",
            &[],
        )
        .await
        .expect("main interval write should succeed");
        let main_auto_commit = branch_head(&engine, sim.main_branch_id()).await;
        main.create_checkpoint()
            .await
            .expect("main checkpoint should retain its interval");
        main.create_checkpoint()
            .await
            .expect("main recovery root should rotate");

        other
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('gc-other', 'other')",
                &[],
            )
            .await
            .expect("other interval write should succeed");
        let other_auto_commit = branch_head(&engine, "01920000-0000-7000-8000-000000000511").await;
        let other_recovery_alias = other
            .create_checkpoint()
            .await
            .expect("other checkpoint should retain its interval")
            .commit_id;
        let other_serving_checkpoint = other
            .create_checkpoint()
            .await
            .expect("other recovery root should rotate")
            .commit_id;
        other
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('gc-other-active', 'active')",
                &[],
            )
            .await
            .expect("other active undo interval write should succeed");
        let other_active_history =
            branch_head(&engine, "01920000-0000-7000-8000-000000000511").await;

        assert_commits(
            &main,
            &[
                &main_auto_commit,
                &other_auto_commit,
                &other_recovery_alias,
                &other_serving_checkpoint,
                &other_active_history,
            ],
            true,
        )
        .await;
        for _ in 4..CHECKPOINT_GC_INTERVAL {
            main.create_checkpoint()
                .await
                .expect("global GC padding checkpoint should succeed");
        }
        assert_commits(&main, &[&main_auto_commit, &other_auto_commit], true).await;
        // Complete-snapshot history retains causal and state-base dependencies;
        // the branch intervals remain queryable alongside their recovery and
        // serving roots.
        assert_commits(
            &main,
            &[
                &other_recovery_alias,
                &other_serving_checkpoint,
                &other_active_history,
            ],
            true,
        )
        .await;
    }
);

simulation_test!(
    checkpoint_gc_retains_full_active_replay_interval,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        register_replay_gc_schema(&session).await;
        let mut seed = session
            .begin_transaction()
            .await
            .expect("replay fixture seed transaction should begin");
        for index in 0..100 {
            seed.execute(
                &format!(
                    "INSERT INTO {REPLAY_GC_SCHEMA_KEY} (id, indexed_value, note, generation) VALUES ($1, $2, $3, 0)"
                ),
                &[
                    Value::Text(format!("row-{index}")),
                    Value::Text(format!("indexed-{index}-0")),
                    Value::Text(format!("seed-{index}")),
                ],
            )
            .await
            .expect("replay fixture seed should succeed");
        }
        seed.commit()
            .await
            .expect("replay fixture seed should commit");
        let mut first = session
            .begin_transaction()
            .await
            .expect("first churn transaction should begin");
        first
            .execute(
                &format!(
                    "UPDATE {REPLAY_GC_SCHEMA_KEY} SET indexed_value = 'indexed-0-1', generation = 1 WHERE id = 'row-0'"
                ),
                &[],
            )
            .await
            .expect("first indexed churn should succeed");
        first
            .execute(
                &format!(
                    "UPDATE {REPLAY_GC_SCHEMA_KEY} SET note = 'one', generation = 1 WHERE id = 'row-1'"
                ),
                &[],
            )
            .await
            .expect("first nonindexed churn should succeed");
        first
            .execute(
                &format!("DELETE FROM {REPLAY_GC_SCHEMA_KEY} WHERE id = 'row-99'"),
                &[],
            )
            .await
            .expect("replacement delete should succeed");
        first.commit().await.expect("first churn should commit");

        let mut second = session
            .begin_transaction()
            .await
            .expect("second churn transaction should begin");
        second
            .execute(
                &format!(
                    "UPDATE {REPLAY_GC_SCHEMA_KEY} SET indexed_value = 'indexed-0-2', generation = 2 WHERE id = 'row-0'"
                ),
                &[],
            )
            .await
            .expect("second indexed churn should succeed");
        second
            .execute(
                &format!(
                    "UPDATE {REPLAY_GC_SCHEMA_KEY} SET note = 'two', generation = 2 WHERE id = 'row-1'"
                ),
                &[],
            )
            .await
            .expect("second nonindexed churn should succeed");
        second
            .execute(
                &format!(
                    "INSERT INTO {REPLAY_GC_SCHEMA_KEY} (id, indexed_value, note, generation) VALUES ('row-99', 'indexed-99-2', 'replacement', 2)"
                ),
                &[],
            )
            .await
            .expect("replacement insert should succeed");
        second.commit().await.expect("second churn should commit");
        let retired_head = branch_head(&engine, sim.main_branch_id()).await;

        session
            .create_checkpoint()
            .await
            .expect("compacting checkpoint should succeed");
        for _ in 1..CHECKPOINT_GC_INTERVAL {
            session
                .create_checkpoint()
                .await
                .expect("padding checkpoint should succeed");
        }
        wait_for_commits(&session, &[&retired_head], false).await;

        assert_replay_gc_state(&session).await;
        let history = session
            .execute(
                &format!(
                    "SELECT note FROM lix_history('{REPLAY_GC_SCHEMA_KEY}') WHERE id = 'row-1'"
                ),
                &[],
            )
            .await
            .expect("compacted history should remain readable after GC");
        assert!(!history.is_empty());

        let reopened_engine = sim
            .reboot_engine_from_current_snapshot()
            .await
            .expect("engine should reopen after replay GC");
        let reopened = sim.wrap_session(
            reopened_engine
                .open_session()
                .await
                .expect("reopened session should open"),
            &reopened_engine,
        );
        assert_replay_gc_state(&reopened).await;
        reopened
            .execute(
                &format!(
                    "SELECT note FROM lix_history('{REPLAY_GC_SCHEMA_KEY}') WHERE id = 'row-1'"
                ),
                &[],
            )
            .await
            .expect("compacted history should remain readable after cold reopen");
    }
);

async fn register_replay_gc_schema(session: &support::simulation_test::engine::SimSession) {
    let schema = json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": REPLAY_GC_SCHEMA_KEY,
        "columns": [
            { "name": "id", "type": "text", "nullable": false },
            { "name": "indexed_value", "type": "text", "nullable": false },
            { "name": "note", "type": "text", "nullable": false },
            { "name": "generation", "type": "int8", "nullable": false },
        ],
        "primary_key": ["id"],
        "unique": [["indexed_value"]],
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (CAST($1 AS JSONB), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("replay GC schema should register");
}

async fn assert_replay_gc_state(session: &support::simulation_test::engine::SimSession) {
    let state = session
        .execute(
            &format!("SELECT note FROM {REPLAY_GC_SCHEMA_KEY} WHERE id = 'row-1'"),
            &[],
        )
        .await
        .expect("current state should remain readable after replay GC");
    assert_eq!(state.rows()[0].values(), &[Value::Text("two".to_string())]);
}

async fn advance_to_next_gc(session: &support::simulation_test::engine::SimSession, sequence: u64) {
    for _ in (sequence + 1)..CHECKPOINT_GC_INTERVAL {
        session
            .create_checkpoint()
            .await
            .expect("padding checkpoint should succeed");
    }
}

async fn branch_head(engine: &lix::engine::Engine, branch_id: &str) -> String {
    engine
        .load_branch_head_commit_id(branch_id)
        .await
        .expect("branch head should load")
        .expect("branch head should exist")
}

async fn commit_parent_edges(
    session: &support::simulation_test::engine::SimSession,
    commit_id: &str,
) -> Vec<(String, i64)> {
    session
        .execute(
            &format!("SELECT parent_commit_ids FROM lix_commit WHERE id = '{commit_id}'"),
            &[],
        )
        .await
        .expect("ordered commit parents should read")
        .rows()
        .first()
        .and_then(|row| match &row.values()[0] {
            Value::Jsonb(parents) => Some(parents.to_value()),
            _ => None,
        })
        .and_then(|parents| parents.as_array().cloned())
        .expect("parent_commit_ids should be a JSON array")
        .into_iter()
        .enumerate()
        .map(|(order, parent)| {
            (
                parent.as_str().expect("parent id should be text").to_owned(),
                i64::try_from(order).expect("parent order should fit an integer"),
            )
        })
        .collect()
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

/// Checkpoint GC is deliberately asynchronous: the checkpoint/root
/// publication is the foreground guarantee, while collection may complete
/// after the API returns. Bound the wait so this test verifies eventual
/// collection without introducing timing-sensitive assertions.
async fn wait_for_commits(
    session: &support::simulation_test::engine::SimSession,
    commit_ids: &[&str],
    expected_present: bool,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut matches = true;
        for commit_id in commit_ids {
            let result = session
                .execute(
                    &format!("SELECT id FROM lix_commit WHERE id = '{commit_id}'"),
                    &[],
                )
                .await
                .expect("commit existence query should succeed");
            let present = !result.is_empty();
            if present != expected_present {
                matches = false;
                break;
            }
        }
        if matches {
            return;
        }
        if Instant::now() >= deadline {
            assert_commits(session, commit_ids, expected_present).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
