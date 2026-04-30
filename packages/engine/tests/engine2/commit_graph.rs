use crate::simulation_test2;
use lix_engine::engine2::ExecuteResult;
use lix_engine::Value;
use serde_json::Value as JsonValue;

simulation_test2!(
    version_ref_advances_after_tracked_commit,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let initial_head = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("version head should load")
            .expect("version head should exist");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('version-ref-advance', 'one')",
                &[],
            )
            .await
            .expect("tracked write should succeed");
        let advanced_head = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("version head should load")
            .expect("version head should exist");

        assert_ne!(
            advanced_head, initial_head,
            "tracked commit should advance the touched version ref"
        );
    }
);

simulation_test2!(
    tracked_write_creates_one_commit_without_advancing_global_ref,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let global_session = sim.wrap_session(
            engine
                .open_session("global")
                .await
                .expect("global session should open"),
            &engine,
        );
        let global_head_before = engine
            .load_version_head_commit_id("global")
            .await
            .expect("global head should load")
            .expect("global head should exist");
        let main_head_before = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('one-commit-model', 'ok')",
                &[],
            )
            .await
            .expect("tracked write should succeed");

        let global_head_after = engine
            .load_version_head_commit_id("global")
            .await
            .expect("global head should load")
            .expect("global head should exist");
        let main_head_after = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");

        assert_eq!(
            global_head_after, global_head_before,
            "non-global writes must not advance the global version ref"
        );
        assert_ne!(
            main_head_after, main_head_before,
            "tracked write should advance exactly the touched version ref"
        );

        let commit_snapshot = load_commit_snapshot(&global_session, &main_head_after).await;
        assert_eq!(
            commit_snapshot.get("id").and_then(JsonValue::as_str),
            Some(main_head_after.as_str()),
            "the touched-version commit should still be globally visible through lix_state"
        );
    }
);

simulation_test2!(
    second_commit_parents_previous_version_head,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let global_session = sim.wrap_session(
            engine
                .open_session("global")
                .await
                .expect("global session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('commit-parent', 'one')",
                &[],
            )
            .await
            .expect("first tracked write should succeed");
        let first_head = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("version head should load")
            .expect("version head should exist");

        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'commit-parent'",
                &[],
            )
            .await
            .expect("second tracked write should succeed");
        let second_head = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("version head should load")
            .expect("version head should exist");

        assert_ne!(second_head, first_head);

        let commit_snapshot = load_commit_snapshot(&global_session, &second_head).await;
        let parent_commit_ids = commit_snapshot
            .get("parent_commit_ids")
            .and_then(JsonValue::as_array)
            .expect("commit snapshot should contain parent_commit_ids");
        assert_eq!(
            parent_commit_ids,
            &vec![JsonValue::String(first_head)],
            "second commit should parent to the previous version head"
        );
    }
);

async fn load_commit_snapshot(
    session: &crate::support::simulation_test::engine2::SimSession,
    commit_id: &str,
) -> JsonValue {
    let result = session
        .execute(
            &format!(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE schema_key = 'lix_commit' AND entity_id = '{commit_id}'"
            ),
            &[],
        )
        .await
        .expect("commit row should read");
    let ExecuteResult::Rows(rows) = result else {
        panic!("SELECT should return rows");
    };
    assert_eq!(rows.len(), 1);
    let Value::Text(snapshot_content) = &rows.rows()[0].values()[0] else {
        panic!("commit snapshot should be text");
    };
    serde_json::from_str::<JsonValue>(snapshot_content).expect("commit snapshot should be JSON")
}
