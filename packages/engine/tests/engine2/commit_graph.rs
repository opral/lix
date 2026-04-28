use crate::simulation_test2;
use lix_engine::engine2::ExecuteResult;
use lix_engine::Value;
use serde_json::Value as JsonValue;

simulation_test2!(
    version_ref_advances_after_tracked_commit,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim
            .open_main_session(&engine)
            .await
            .expect("main session should open");
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
    second_commit_parents_previous_version_head,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim
            .open_main_session(&engine)
            .await
            .expect("main session should open");
        let global_session = sim
            .open_global_session(&engine)
            .await
            .expect("global session should open");

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
