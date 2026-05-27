use lix_engine::ExecuteResult;
use lix_engine::LixError;
use lix_engine::Value;

simulation_test!(lix_branch_lists_descriptors_with_refs, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session("global")
            .await
            .expect("global session should open"),
        &engine,
    );

    let result = session
        .execute(
            "SELECT id, name, hidden, commit_id FROM lix_branch ORDER BY id",
            &[],
        )
        .await
        .expect("lix_branch should read");
    let rows = result;
    assert_eq!(rows.len(), 2);

    let values = rows
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    assert!(values.contains(&vec![
        Value::Text("global".to_string()),
        Value::Text("global".to_string()),
        Value::Boolean(true),
        Value::Text(sim.initial_commit_id().to_string()),
    ]));
    assert!(values.contains(&vec![
        Value::Text(sim.main_branch_id().to_string()),
        Value::Text("main".to_string()),
        Value::Boolean(false),
        Value::Text(sim.initial_commit_id().to_string()),
    ]));
});

simulation_test!(
    lix_branch_count_star_handles_empty_projection,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session("global")
                .await
                .expect("global session should open"),
            &engine,
        );

        assert_eq!(
            count_rows(&session, "SELECT COUNT(*) FROM lix_branch").await,
            2
        );
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch WHERE name = 'main'",
            )
            .await,
            1
        );
    }
);

simulation_test!(
    lix_branch_insert_creates_descriptor_and_ref,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        let insert_result = session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('sql-branch-insert', 'SQL Insert')",
                &[],
            )
            .await
            .expect("lix_branch insert should create descriptor and ref");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        assert_single_branch_row(
            &session,
            "sql-branch-insert",
            "SQL Insert",
            false,
            sim.initial_commit_id(),
        )
        .await;
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch_descriptor WHERE id = 'sql-branch-insert'",
            )
            .await,
            1
        );
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch_ref WHERE id = 'sql-branch-insert'",
            )
            .await,
            1
        );
    }
);

simulation_test!(
    lix_branch_insert_accepts_explicit_hidden_and_commit_id,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        let insert_result = session
            .execute(
                &format!(
                    "INSERT INTO lix_branch (id, name, hidden, commit_id) \
                     VALUES ('sql-branch-explicit', 'Explicit', true, '{}')",
                    sim.initial_commit_id()
                ),
                &[],
            )
            .await
            .expect("lix_branch insert should accept hidden and commit_id");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        assert_single_branch_row(
            &session,
            "sql-branch-explicit",
            "Explicit",
            true,
            sim.initial_commit_id(),
        )
        .await;
    }
);

simulation_test!(
    lix_branch_update_splits_descriptor_and_ref_changes,
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
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('sql-branch-update', 'Before')",
                &[],
            )
            .await
            .expect("branch insert should succeed");

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('sql-branch-update-head', 'after')",
                &[],
            )
            .await
            .expect("tracked write should advance active branch head");
        let new_head = select_single_text(
            &session,
            &format!(
                "SELECT commit_id FROM lix_branch WHERE id = '{}'",
                sim.main_branch_id()
            ),
        )
        .await;

        let update_result = session
            .execute(
                &format!(
                    "UPDATE lix_branch \
                     SET name = 'After', hidden = true, commit_id = '{new_head}' \
                     WHERE id = 'sql-branch-update'"
                ),
                &[],
            )
            .await
            .expect("lix_branch update should split descriptor and ref changes");
        assert_eq!(update_result, ExecuteResult::from_rows_affected(1));

        assert_single_branch_row(&session, "sql-branch-update", "After", true, &new_head).await;
        assert_eq!(
            select_single_text(
                &session,
                "SELECT commit_id FROM lix_branch_ref WHERE id = 'sql-branch-update'",
            )
            .await,
            new_head
        );
    }
);

simulation_test!(
    lix_branch_delete_removes_descriptor_and_ref_atomically,
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
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('sql-branch-delete', 'Delete Me')",
                &[],
            )
            .await
            .expect("branch insert should succeed");

        let delete_result = session
            .execute("DELETE FROM lix_branch WHERE id = 'sql-branch-delete'", &[])
            .await
            .expect("lix_branch delete should remove descriptor and ref atomically");
        assert_eq!(delete_result, ExecuteResult::from_rows_affected(1));

        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch WHERE id = 'sql-branch-delete'",
            )
            .await,
            0
        );
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch_descriptor WHERE id = 'sql-branch-delete'",
            )
            .await,
            0
        );
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch_ref WHERE id = 'sql-branch-delete'",
            )
            .await,
            0
        );
    }
);

simulation_test!(
    lix_branch_delete_rejects_active_and_global_branches,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        let active_error = session
            .execute(
                &format!(
                    "DELETE FROM lix_branch WHERE id = '{}'",
                    sim.main_branch_id()
                ),
                &[],
            )
            .await
            .expect_err("delete should reject active branch");
        assert!(
            active_error.to_string().contains("active branch"),
            "active delete error should explain the restriction: {active_error:?}"
        );

        let global_error = session
            .execute("DELETE FROM lix_branch WHERE id = 'global'", &[])
            .await
            .expect_err("delete should reject global branch");
        assert!(
            global_error.to_string().contains("global branch"),
            "global delete error should explain the restriction: {global_error:?}"
        );

        assert_eq!(
            count_rows(
                &session,
                &format!(
                    "SELECT COUNT(*) FROM lix_branch WHERE id = '{}'",
                    sim.main_branch_id()
                ),
            )
            .await,
            1
        );
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch WHERE id = 'global'"
            )
            .await,
            1
        );
    }
);

simulation_test!(lix_branch_duplicate_insert_rejects, |sim| async move {
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
            "INSERT INTO lix_branch (id, name) \
             VALUES ('sql-branch-duplicate', 'First')",
            &[],
        )
        .await
        .expect("initial branch insert should succeed");

    let error = session
        .execute(
            "INSERT INTO lix_branch (id, name) \
             VALUES ('sql-branch-duplicate', 'Second')",
            &[],
        )
        .await
        .expect_err("duplicate branch id should be rejected");
    assert_eq!(error.code, LixError::CODE_UNIQUE);
    assert!(
        error.message.contains("table 'lix_branch'")
            && error.message.contains("id 'sql-branch-duplicate'")
            && !error.message.contains("lix_branch_descriptor")
            && !error.message.contains("lix_branch_ref"),
        "unexpected error: {error:?}"
    );
});

simulation_test!(lix_branch_duplicate_name_insert_rejects, |sim| async move {
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
            "INSERT INTO lix_branch (id, name) \
             VALUES ('sql-branch-name-a', 'Duplicate Name')",
            &[],
        )
        .await
        .expect("initial branch insert should succeed");

    let error = session
        .execute(
            "INSERT INTO lix_branch (id, name) \
             VALUES ('sql-branch-name-b', 'Duplicate Name')",
            &[],
        )
        .await
        .expect_err("duplicate branch name should be rejected");
    assert_eq!(error.code, LixError::CODE_UNIQUE);
    assert!(
        error.to_string().contains("/name"),
        "error should explain duplicate branch name: {error:?}"
    );
});

simulation_test!(lix_branch_duplicate_name_update_rejects, |sim| async move {
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
            "INSERT INTO lix_branch (id, name) \
             VALUES ('sql-branch-name-update-a', 'Name A')",
            &[],
        )
        .await
        .expect("first branch insert should succeed");
    session
        .execute(
            "INSERT INTO lix_branch (id, name) \
             VALUES ('sql-branch-name-update-b', 'Name B')",
            &[],
        )
        .await
        .expect("second branch insert should succeed");

    let error = session
        .execute(
            "UPDATE lix_branch \
             SET name = 'Name A' \
             WHERE id = 'sql-branch-name-update-b'",
            &[],
        )
        .await
        .expect_err("updating to a duplicate branch name should fail");
    assert_eq!(error.code, LixError::CODE_UNIQUE);
    assert!(
        error.to_string().contains("/name"),
        "error should explain duplicate branch name: {error:?}"
    );
});

simulation_test!(
    lix_branch_insert_rejects_invalid_commit_id,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_branch (id, name, commit_id) \
                 VALUES ('sql-branch-invalid-commit', 'Invalid Commit', 'missing-commit')",
                &[],
            )
            .await
            .expect_err("branch ref commit_id should reference an existing commit");
        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);

        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch WHERE id = 'sql-branch-invalid-commit'",
            )
            .await,
            0
        );
    }
);

simulation_test!(lix_branch_update_rejects_id_change, |sim| async move {
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
            "INSERT INTO lix_branch (id, name) \
             VALUES ('sql-branch-id-update', 'Before')",
            &[],
        )
        .await
        .expect("branch insert should succeed");

    let error = session
        .execute(
            "UPDATE lix_branch \
             SET id = 'sql-branch-id-update-renamed' \
             WHERE id = 'sql-branch-id-update'",
            &[],
        )
        .await
        .expect_err("branch id should be immutable through UPDATE");
    assert!(
        error.to_string().contains("immutable column 'id'"),
        "id update error should explain the restriction: {error:?}"
    );

    assert_eq!(
        count_rows(
            &session,
            "SELECT COUNT(*) FROM lix_branch WHERE id = 'sql-branch-id-update'",
        )
        .await,
        1
    );
    assert_eq!(
        count_rows(
            &session,
            "SELECT COUNT(*) FROM lix_branch WHERE id = 'sql-branch-id-update-renamed'",
        )
        .await,
        0
    );
});

simulation_test!(lix_branch_update_rejects_global_branch, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
        &engine,
    );

    let error = session
        .execute(
            "UPDATE lix_branch SET name = 'mutated-global' WHERE id = 'global'",
            &[],
        )
        .await
        .expect_err("global branch should be immutable through UPDATE");
    assert!(
        error.to_string().contains("global branch"),
        "global update error should explain the restriction: {error:?}"
    );

    assert_eq!(
        select_single_text(&session, "SELECT name FROM lix_branch WHERE id = 'global'").await,
        "global"
    );
});

simulation_test!(
    lix_branch_delete_missing_returns_zero_rows_affected,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        let delete_result = session
            .execute(
                "DELETE FROM lix_branch WHERE id = 'sql-branch-missing-delete'",
                &[],
            )
            .await
            .expect("missing branch delete should be a no-op");
        assert_eq!(delete_result, ExecuteResult::from_rows_affected(0));
    }
);

async fn assert_single_branch_row(
    session: &crate::support::simulation_test::engine::SimSession,
    branch_id: &str,
    name: &str,
    hidden: bool,
    commit_id: &str,
) {
    let result = session
        .execute(
            &format!(
                "SELECT id, name, hidden, commit_id \
                 FROM lix_branch \
                 WHERE id = '{branch_id}'"
            ),
            &[],
        )
        .await
        .expect("branch row should be selectable");
    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text(branch_id.to_string()),
            Value::Text(name.to_string()),
            Value::Boolean(hidden),
            Value::Text(commit_id.to_string()),
        ]
    );
}

async fn select_single_text(
    session: &crate::support::simulation_test::engine::SimSession,
    sql: &str,
) -> String {
    let result = session
        .execute(sql, &[])
        .await
        .expect("query should succeed");
    assert_eq!(result.len(), 1, "expected exactly one row for query: {sql}");
    match result.rows()[0].values()[0] {
        Value::Text(ref text) => text.clone(),
        ref other => panic!("expected text for query {sql}, got {other:?}"),
    }
}

async fn count_rows(
    session: &crate::support::simulation_test::engine::SimSession,
    sql: &str,
) -> i64 {
    let result = session
        .execute(sql, &[])
        .await
        .expect("count should succeed");
    assert_eq!(result.len(), 1, "expected exactly one row for query: {sql}");
    match result.rows()[0].values()[0] {
        Value::Integer(count) => count,
        ref other => panic!("expected integer count for query {sql}, got {other:?}"),
    }
}
