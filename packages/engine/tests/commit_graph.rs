#[macro_use]
mod support;
use lix_engine::Value;

simulation_test!(branch_ref_advances_after_tracked_commit, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );
    let initial_head = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("branch head should load")
        .expect("branch head should exist");
    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('branch-ref-advance', 'one')",
            &[],
        )
        .await
        .expect("tracked write should succeed");
    let advanced_head = engine
        .load_branch_head_commit_id(sim.main_branch_id())
        .await
        .expect("branch head should load")
        .expect("branch head should exist");

    assert_ne!(
        advanced_head, initial_head,
        "tracked commit should advance the touched branch ref"
    );
});

simulation_test!(
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
            .load_branch_head_commit_id("global")
            .await
            .expect("global head should load")
            .expect("global head should exist");
        let main_head_before = engine
            .load_branch_head_commit_id(sim.main_branch_id())
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
            .load_branch_head_commit_id("global")
            .await
            .expect("global head should load")
            .expect("global head should exist");
        let main_head_after = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");

        assert_eq!(
            global_head_after, global_head_before,
            "non-global writes must not advance the global branch ref"
        );
        assert_ne!(
            main_head_after, main_head_before,
            "tracked write should advance exactly the touched branch ref"
        );

        assert_eq!(
            commit_ids(&global_session, &main_head_after).await,
            vec![main_head_after.clone()],
            "the touched-branch commit should still be globally visible through lix_state"
        );
    }
);

simulation_test!(
    second_commit_parents_previous_branch_head,
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
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("branch head should load")
            .expect("branch head should exist");

        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' WHERE key = 'commit-parent'",
                &[],
            )
            .await
            .expect("second tracked write should succeed");
        let second_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("branch head should load")
            .expect("branch head should exist");

        assert_ne!(second_head, first_head);

        assert_eq!(
            commit_parent_ids(&global_session, &second_head).await,
            vec![first_head],
            "second commit should parent to the previous branch head"
        );
    }
);

async fn commit_parent_ids(
    session: &support::simulation_test::engine::SimSession,
    commit_id: &str,
) -> Vec<String> {
    let result = session
        .execute(
            &format!(
                "SELECT parent_id \
                 FROM lix_commit_edge \
                 WHERE child_id = '{commit_id}' \
                 ORDER BY parent_id"
            ),
            &[],
        )
        .await
        .expect("commit edge rows should read");
    result
        .rows()
        .iter()
        .map(|row| match &row.values()[0] {
            Value::Text(parent_id) => parent_id.clone(),
            value => panic!("expected parent_id string, got {value:?}"),
        })
        .collect()
}

async fn commit_ids(
    session: &support::simulation_test::engine::SimSession,
    commit_id: &str,
) -> Vec<String> {
    let result = session
        .execute(
            &format!("SELECT id FROM lix_commit WHERE id = '{commit_id}'"),
            &[],
        )
        .await
        .expect("commit rows should read");
    result
        .rows()
        .iter()
        .map(|row| match &row.values()[0] {
            Value::Text(commit_id) => commit_id.clone(),
            value => panic!("expected commit id string, got {value:?}"),
        })
        .collect()
}
