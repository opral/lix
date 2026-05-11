use lix_engine::ExecuteResult;
use lix_engine::LixError;
use lix_engine::Value;

simulation_test!(lix_version_lists_descriptors_with_refs, |sim| async move {
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
            "SELECT id, name, hidden, commit_id FROM lix_version ORDER BY id",
            &[],
        )
        .await
        .expect("lix_version should read");
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
        Value::Text(sim.main_version_id().to_string()),
        Value::Text("main".to_string()),
        Value::Boolean(false),
        Value::Text(sim.initial_commit_id().to_string()),
    ]));
});

simulation_test!(
    lix_version_count_star_handles_empty_projection,
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
            count_rows(&session, "SELECT COUNT(*) FROM lix_version").await,
            2
        );
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_version WHERE name = 'main'",
            )
            .await,
            1
        );
    }
);

simulation_test!(
    lix_version_insert_creates_descriptor_and_ref,
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
                "INSERT INTO lix_version (id, name) \
                 VALUES ('sql-version-insert', 'SQL Insert')",
                &[],
            )
            .await
            .expect("lix_version insert should create descriptor and ref");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        assert_single_version_row(
            &session,
            "sql-version-insert",
            "SQL Insert",
            false,
            sim.initial_commit_id(),
        )
        .await;
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_version_descriptor WHERE id = 'sql-version-insert'",
            )
            .await,
            1
        );
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_version_ref WHERE id = 'sql-version-insert'",
            )
            .await,
            1
        );
    }
);

simulation_test!(
    lix_version_insert_accepts_explicit_hidden_and_commit_id,
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
                    "INSERT INTO lix_version (id, name, hidden, commit_id) \
                     VALUES ('sql-version-explicit', 'Explicit', true, '{}')",
                    sim.initial_commit_id()
                ),
                &[],
            )
            .await
            .expect("lix_version insert should accept hidden and commit_id");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        assert_single_version_row(
            &session,
            "sql-version-explicit",
            "Explicit",
            true,
            sim.initial_commit_id(),
        )
        .await;
    }
);

simulation_test!(
    lix_version_update_splits_descriptor_and_ref_changes,
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
                "INSERT INTO lix_version (id, name) \
                 VALUES ('sql-version-update', 'Before')",
                &[],
            )
            .await
            .expect("version insert should succeed");

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('sql-version-update-head', 'after')",
                &[],
            )
            .await
            .expect("tracked write should advance active version head");
        let new_head = select_single_text(
            &session,
            &format!(
                "SELECT commit_id FROM lix_version WHERE id = '{}'",
                sim.main_version_id()
            ),
        )
        .await;

        let update_result = session
            .execute(
                &format!(
                    "UPDATE lix_version \
                     SET name = 'After', hidden = true, commit_id = '{new_head}' \
                     WHERE id = 'sql-version-update'"
                ),
                &[],
            )
            .await
            .expect("lix_version update should split descriptor and ref changes");
        assert_eq!(update_result, ExecuteResult::from_rows_affected(1));

        assert_single_version_row(&session, "sql-version-update", "After", true, &new_head).await;
        assert_eq!(
            select_single_text(
                &session,
                "SELECT commit_id FROM lix_version_ref WHERE id = 'sql-version-update'",
            )
            .await,
            new_head
        );
    }
);

simulation_test!(
    lix_version_delete_removes_descriptor_and_ref_atomically,
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
                "INSERT INTO lix_version (id, name) \
                 VALUES ('sql-version-delete', 'Delete Me')",
                &[],
            )
            .await
            .expect("version insert should succeed");

        let delete_result = session
            .execute(
                "DELETE FROM lix_version WHERE id = 'sql-version-delete'",
                &[],
            )
            .await
            .expect("lix_version delete should remove descriptor and ref atomically");
        assert_eq!(delete_result, ExecuteResult::from_rows_affected(1));

        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_version WHERE id = 'sql-version-delete'",
            )
            .await,
            0
        );
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_version_descriptor WHERE id = 'sql-version-delete'",
            )
            .await,
            0
        );
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_version_ref WHERE id = 'sql-version-delete'",
            )
            .await,
            0
        );
    }
);

simulation_test!(
    lix_version_delete_rejects_active_and_global_versions,
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
                    "DELETE FROM lix_version WHERE id = '{}'",
                    sim.main_version_id()
                ),
                &[],
            )
            .await
            .expect_err("delete should reject active version");
        assert!(
            active_error.to_string().contains("active version"),
            "active delete error should explain the restriction: {active_error:?}"
        );

        let global_error = session
            .execute("DELETE FROM lix_version WHERE id = 'global'", &[])
            .await
            .expect_err("delete should reject global version");
        assert!(
            global_error.to_string().contains("global version"),
            "global delete error should explain the restriction: {global_error:?}"
        );

        assert_eq!(
            count_rows(
                &session,
                &format!(
                    "SELECT COUNT(*) FROM lix_version WHERE id = '{}'",
                    sim.main_version_id()
                ),
            )
            .await,
            1
        );
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_version WHERE id = 'global'"
            )
            .await,
            1
        );
    }
);

simulation_test!(lix_version_duplicate_insert_rejects, |sim| async move {
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
            "INSERT INTO lix_version (id, name) \
             VALUES ('sql-version-duplicate', 'First')",
            &[],
        )
        .await
        .expect("initial version insert should succeed");

    let error = session
        .execute(
            "INSERT INTO lix_version (id, name) \
             VALUES ('sql-version-duplicate', 'Second')",
            &[],
        )
        .await
        .expect_err("duplicate version id should be rejected");
    assert_eq!(error.code, LixError::CODE_UNIQUE);
    assert!(
        error.message.contains("table 'lix_version'")
            && error.message.contains("id 'sql-version-duplicate'")
            && !error.message.contains("lix_version_descriptor")
            && !error.message.contains("lix_version_ref"),
        "unexpected error: {error:?}"
    );
});

simulation_test!(
    lix_version_duplicate_name_insert_rejects,
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
                "INSERT INTO lix_version (id, name) \
             VALUES ('sql-version-name-a', 'Duplicate Name')",
                &[],
            )
            .await
            .expect("initial version insert should succeed");

        let error = session
            .execute(
                "INSERT INTO lix_version (id, name) \
             VALUES ('sql-version-name-b', 'Duplicate Name')",
                &[],
            )
            .await
            .expect_err("duplicate version name should be rejected");
        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.to_string().contains("/name"),
            "error should explain duplicate version name: {error:?}"
        );
    }
);

simulation_test!(
    lix_version_duplicate_name_update_rejects,
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
                "INSERT INTO lix_version (id, name) \
             VALUES ('sql-version-name-update-a', 'Name A')",
                &[],
            )
            .await
            .expect("first version insert should succeed");
        session
            .execute(
                "INSERT INTO lix_version (id, name) \
             VALUES ('sql-version-name-update-b', 'Name B')",
                &[],
            )
            .await
            .expect("second version insert should succeed");

        let error = session
            .execute(
                "UPDATE lix_version \
             SET name = 'Name A' \
             WHERE id = 'sql-version-name-update-b'",
                &[],
            )
            .await
            .expect_err("updating to a duplicate version name should fail");
        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.to_string().contains("/name"),
            "error should explain duplicate version name: {error:?}"
        );
    }
);

simulation_test!(
    lix_version_insert_rejects_invalid_commit_id,
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
                "INSERT INTO lix_version (id, name, commit_id) \
                 VALUES ('sql-version-invalid-commit', 'Invalid Commit', 'missing-commit')",
                &[],
            )
            .await
            .expect_err("version ref commit_id should reference an existing commit");
        assert_eq!(error.code, LixError::CODE_VERSION_NOT_FOUND);

        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_version WHERE id = 'sql-version-invalid-commit'",
            )
            .await,
            0
        );
    }
);

simulation_test!(lix_version_update_rejects_id_change, |sim| async move {
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
            "INSERT INTO lix_version (id, name) \
             VALUES ('sql-version-id-update', 'Before')",
            &[],
        )
        .await
        .expect("version insert should succeed");

    let error = session
        .execute(
            "UPDATE lix_version \
             SET id = 'sql-version-id-update-renamed' \
             WHERE id = 'sql-version-id-update'",
            &[],
        )
        .await
        .expect_err("version id should be immutable through UPDATE");
    assert!(
        error.to_string().contains("immutable column 'id'"),
        "id update error should explain the restriction: {error:?}"
    );

    assert_eq!(
        count_rows(
            &session,
            "SELECT COUNT(*) FROM lix_version WHERE id = 'sql-version-id-update'",
        )
        .await,
        1
    );
    assert_eq!(
        count_rows(
            &session,
            "SELECT COUNT(*) FROM lix_version WHERE id = 'sql-version-id-update-renamed'",
        )
        .await,
        0
    );
});

simulation_test!(
    lix_version_delete_missing_returns_zero_rows_affected,
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
                "DELETE FROM lix_version WHERE id = 'sql-version-missing-delete'",
                &[],
            )
            .await
            .expect("missing version delete should be a no-op");
        assert_eq!(delete_result, ExecuteResult::from_rows_affected(0));
    }
);

async fn assert_single_version_row(
    session: &crate::support::simulation_test::engine::SimSession,
    version_id: &str,
    name: &str,
    hidden: bool,
    commit_id: &str,
) {
    let result = session
        .execute(
            &format!(
                "SELECT id, name, hidden, commit_id \
                 FROM lix_version \
                 WHERE id = '{version_id}'"
            ),
            &[],
        )
        .await
        .expect("version row should be selectable");
    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text(version_id.to_string()),
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
