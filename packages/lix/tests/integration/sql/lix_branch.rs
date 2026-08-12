use lix::ExecuteResult;
use lix::LixError;
use lix::Value;

simulation_test!(lix_branch_lists_descriptors_with_refs, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session_at("ffffffff-ffff-7fff-bfff-ffffffffffff")
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
        Value::Text("ffffffff-ffff-7fff-bfff-ffffffffffff".to_string()),
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
    lix_branch_exact_id_read_filters_preserve_semantics,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session_at("ffffffff-ffff-7fff-bfff-ffffffffffff")
                .await
                .expect("global session should open"),
            &engine,
        );
        let global_id = "ffffffff-ffff-7fff-bfff-ffffffffffff";
        let main_id = sim.main_branch_id().to_string();

        let exact = session
            .execute(
                "SELECT id, name FROM lix_branch WHERE id = $1",
                &[Value::Text(main_id.clone())],
            )
            .await
            .expect("exact branch ID should read");
        assert_eq!(exact.len(), 1);
        assert_eq!(
            exact.rows()[0].values(),
            &[
                Value::Text(main_id.clone()),
                Value::Text("main".to_string())
            ]
        );

        let in_list = session
            .execute(
                "SELECT id FROM lix_branch WHERE id IN ($1, $2) ORDER BY id",
                &[
                    Value::Text(global_id.to_string()),
                    Value::Text(main_id.clone()),
                ],
            )
            .await
            .expect("branch ID list should read");
        assert_eq!(in_list.len(), 2);
        assert_eq!(
            in_list
                .rows()
                .iter()
                .map(|row| row.get::<String>("id").expect("id should be text"))
                .collect::<Vec<_>>(),
            {
                let mut ids = vec![global_id.to_string(), main_id];
                ids.sort();
                ids
            }
        );

        let invalid = session
            .execute(
                "SELECT id FROM lix_branch WHERE id = $1",
                &[Value::Text("not-a-branch-id".to_string())],
            )
            .await
            .expect("a noncanonical branch ID should remain a no-match query");
        assert_eq!(invalid.len(), 0);
    }
);

simulation_test!(
    lix_branch_count_star_handles_empty_projection,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session_at("ffffffff-ffff-7fff-bfff-ffffffffffff")
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
    lix_branch_ref_control_preserves_public_row_metadata,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );
        let branch_id = sim.main_branch_id();
        let before = session
            .execute(
                &format!(
                    "SELECT lixcol_created_at, lixcol_updated_at, lixcol_change_id \
                     FROM lix_branch_ref WHERE id = '{branch_id}'"
                ),
                &[],
            )
            .await
            .expect("initial ref metadata should be readable");
        assert_eq!(before.len(), 1);
        let initial_created_at = before.rows()[0]
            .get::<String>("lixcol_created_at")
            .expect("created timestamp should be text");
        let initial_updated_at = before.rows()[0]
            .get::<String>("lixcol_updated_at")
            .expect("updated timestamp should be text");
        let initial_change_id = before.rows()[0]
            .get::<String>("lixcol_change_id")
            .expect("change id should be text");

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('6272616e-6368-8d72-8566-2d7075626c00', 'next-head')",
                &[],
            )
            .await
            .expect("tracked write should advance the workspace head");

        let after = session
            .execute(
                &format!(
                    "SELECT lixcol_created_at, lixcol_updated_at, lixcol_change_id \
                     FROM lix_branch_ref WHERE id = '{branch_id}'"
                ),
                &[],
            )
            .await
            .expect("advanced ref metadata should be readable");
        assert_eq!(after.len(), 1);
        let advanced_created_at = after.rows()[0]
            .get::<String>("lixcol_created_at")
            .expect("advanced created timestamp should be text");
        let advanced_updated_at = after.rows()[0]
            .get::<String>("lixcol_updated_at")
            .expect("advanced updated timestamp should be text");
        let advanced_change_id = after.rows()[0]
            .get::<String>("lixcol_change_id")
            .expect("advanced change id should be text");

        assert_eq!(advanced_created_at, initial_created_at);
        assert_ne!(advanced_updated_at, initial_updated_at);
        assert_ne!(advanced_change_id, initial_change_id);
        assert_eq!(
            count_rows(
                &session,
                &format!(
                    "SELECT COUNT(*) FROM lix_change \
                     WHERE id = '{advanced_change_id}' \
                     AND schema_key = 'lix_branch_ref'"
                ),
            )
            .await,
            1,
            "the synthesized current ref must retain its immutable public lix_change ledger fact"
        );
    }
);

simulation_test!(
    lix_branch_insert_creates_descriptor_and_ref,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        let insert_result = session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('73716c2d-6272-816e-8368-2d696e736500', 'SQL Insert')",
                &[],
            )
            .await
            .expect("lix_branch insert should create descriptor and ref");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        assert_single_branch_row(
            &session,
            "73716c2d-6272-816e-8368-2d696e736500",
            "SQL Insert",
            false,
            sim.initial_commit_id(),
        )
        .await;
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch_descriptor WHERE id = '73716c2d-6272-816e-8368-2d696e736500'",
            )
            .await,
            1
        );
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch_ref WHERE id = '73716c2d-6272-816e-8368-2d696e736500'",
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
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        let insert_result = session
            .execute(
                &format!(
                    "INSERT INTO lix_branch (id, name, hidden, commit_id) \
                     VALUES ('73716c2d-6272-816e-8368-2d6578706c00', 'Explicit', true, '{}')",
                    sim.initial_commit_id()
                ),
                &[],
            )
            .await
            .expect("lix_branch insert should accept hidden and commit_id");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        assert_single_branch_row(
            &session,
            "73716c2d-6272-816e-8368-2d6578706c00",
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
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('73716c2d-6272-816e-8368-2d7570646100', 'Before')",
                &[],
            )
            .await
            .expect("branch insert should succeed");

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('73716c2d-6272-816e-8368-2d7570646100', 'after')",
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
                     WHERE id = '73716c2d-6272-816e-8368-2d7570646100'"
                ),
                &[],
            )
            .await
            .expect("lix_branch update should split descriptor and ref changes");
        assert_eq!(update_result, ExecuteResult::from_rows_affected(1));

        assert_single_branch_row(
            &session,
            "73716c2d-6272-816e-8368-2d7570646100",
            "After",
            true,
            &new_head,
        )
        .await;
        assert_eq!(
            select_single_text(
                &session,
                "SELECT commit_id FROM lix_branch_ref WHERE id = '73716c2d-6272-816e-8368-2d7570646100'",
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
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('73716c2d-6272-816e-8368-2d64656c6500', 'Delete Me')",
                &[],
            )
            .await
            .expect("branch insert should succeed");

        let delete_result = session
            .execute(
                "DELETE FROM lix_branch WHERE id = '73716c2d-6272-816e-8368-2d64656c6500'",
                &[],
            )
            .await
            .expect("lix_branch delete should remove descriptor and ref atomically");
        assert_eq!(delete_result, ExecuteResult::from_rows_affected(1));

        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch WHERE id = '73716c2d-6272-816e-8368-2d64656c6500'",
            )
            .await,
            0
        );
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch_descriptor WHERE id = '73716c2d-6272-816e-8368-2d64656c6500'",
            )
            .await,
            0
        );
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch_ref WHERE id = '73716c2d-6272-816e-8368-2d64656c6500'",
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
                .open_session()
                .await
                .expect("session should open"),
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
            .execute(
                "DELETE FROM lix_branch WHERE id = 'ffffffff-ffff-7fff-bfff-ffffffffffff'",
                &[],
            )
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
                "SELECT COUNT(*) FROM lix_branch WHERE id = 'ffffffff-ffff-7fff-bfff-ffffffffffff'"
            )
            .await,
            1
        );
    }
);

simulation_test!(
    lix_branch_destructive_ref_changes_reject_branch_local_untracked_state,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('73716c2d-6272-816e-8368-2d6c6f636100', 'Local Untracked')",
                &[],
            )
            .await
            .expect("branch insert should succeed");
        let original_head = select_single_text(
            &session,
            "SELECT commit_id FROM lix_branch \
             WHERE id = '73716c2d-6272-816e-8368-2d6c6f636100'",
        )
        .await;
        session
            .execute(
                "INSERT INTO lix_key_value_by_branch \
                 (key, value, lixcol_branch_id, lixcol_global, lixcol_untracked) \
                 VALUES ('73716c2d-6272-816e-8368-2d6c6f636100', 'draft', \
                         '73716c2d-6272-816e-8368-2d6c6f636100', false, true)",
                &[],
            )
            .await
            .expect("branch-local untracked row should insert");

        session
            .execute(
                &format!(
                    "UPDATE lix_branch SET commit_id = '{original_head}' \
                     WHERE id = '73716c2d-6272-816e-8368-2d6c6f636100'"
                ),
                &[],
            )
            .await
            .expect("assigning the existing head should preserve local state");

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('73716c2d-6272-816e-8368-2d7265706f00', 'new-head')",
                &[],
            )
            .await
            .expect("tracked write should create another head");
        let new_head = select_single_text(
            &session,
            &format!(
                "SELECT commit_id FROM lix_branch WHERE id = '{}'",
                sim.main_branch_id()
            ),
        )
        .await;

        let repoint_error = session
            .execute(
                &format!(
                    "UPDATE lix_branch SET commit_id = '{new_head}' \
                     WHERE id = '73716c2d-6272-816e-8368-2d6c6f636100'"
                ),
                &[],
            )
            .await
            .expect_err("repoint should reject branch-local untracked state");
        assert_eq!(repoint_error.code, LixError::CODE_INVALID_PARAM);
        assert!(
            repoint_error
                .message
                .contains("cannot repoint branch '73716c2d-6272-816e-8368-2d6c6f636100'"),
            "unexpected repoint error: {repoint_error:?}"
        );

        let delete_error = session
            .execute(
                "DELETE FROM lix_branch WHERE id = '73716c2d-6272-816e-8368-2d6c6f636100'",
                &[],
            )
            .await
            .expect_err("delete should reject branch-local untracked state");
        assert_eq!(delete_error.code, LixError::CODE_INVALID_PARAM);
        assert!(
            delete_error
                .message
                .contains("cannot delete branch '73716c2d-6272-816e-8368-2d6c6f636100'"),
            "unexpected delete error: {delete_error:?}"
        );
        assert_eq!(
            select_single_text(
                &session,
                "SELECT commit_id FROM lix_branch \
                 WHERE id = '73716c2d-6272-816e-8368-2d6c6f636100'",
            )
            .await,
            original_head
        );
    }
);

simulation_test!(
    lix_branch_delete_then_recreate_does_not_restore_old_current_root,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('73716c2d-6272-816e-8368-2d7265637201', 'Before Delete')",
                &[],
            )
            .await
            .expect("branch insert should succeed");
        session
            .execute(
                "INSERT INTO lix_key_value_by_branch \
                 (key, value, lixcol_branch_id, lixcol_global, lixcol_untracked) \
                 VALUES ('73716c2d-6272-816e-8368-2d7265637201', 'old', \
                         '73716c2d-6272-816e-8368-2d7265637201', false, false)",
                &[],
            )
            .await
            .expect("tracked branch-local row should insert");
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_key_value_by_branch \
                 WHERE key = '73716c2d-6272-816e-8368-2d7265637201' \
                   AND lixcol_branch_id = '73716c2d-6272-816e-8368-2d7265637201'",
            )
            .await,
            1
        );

        session
            .execute(
                "DELETE FROM lix_branch WHERE id = '73716c2d-6272-816e-8368-2d7265637201'",
                &[],
            )
            .await
            .expect("branch without untracked local state should delete");
        session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('73716c2d-6272-816e-8368-2d7265637201', 'After Delete')",
                &[],
            )
            .await
            .expect("deleted branch id should be reusable");

        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_key_value_by_branch \
                 WHERE key = '73716c2d-6272-816e-8368-2d7265637201' \
                   AND lixcol_branch_id = '73716c2d-6272-816e-8368-2d7265637201'",
            )
            .await,
            0,
            "recreated branch must not reuse the deleted current-state root"
        );
    }
);

simulation_test!(lix_branch_duplicate_insert_rejects, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_branch (id, name) \
             VALUES ('73716c2d-6272-816e-8368-2d6475706c00', 'First')",
            &[],
        )
        .await
        .expect("initial branch insert should succeed");

    let error = session
        .execute(
            "INSERT INTO lix_branch (id, name) \
             VALUES ('73716c2d-6272-816e-8368-2d6475706c00', 'Second')",
            &[],
        )
        .await
        .expect_err("duplicate branch id should be rejected");
    assert_eq!(error.code, LixError::CODE_UNIQUE);
    assert!(
        error.message.contains("table 'lix_branch'")
            && error
                .message
                .contains("id '73716c2d-6272-816e-8368-2d6475706c00'")
            && !error.message.contains("lix_branch_descriptor")
            && !error.message.contains("lix_branch_ref"),
        "unexpected error: {error:?}"
    );
});

simulation_test!(lix_branch_duplicate_name_insert_rejects, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_branch (id, name) \
             VALUES ('73716c2d-6272-816e-8368-2d6e616d6504', 'Duplicate Name')",
            &[],
        )
        .await
        .expect("initial branch insert should succeed");

    let error = session
        .execute(
            "INSERT INTO lix_branch (id, name) \
             VALUES ('73716c2d-6272-816e-8368-2d6e616d6503', 'Duplicate Name')",
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
            .open_session()
            .await
            .expect("session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_branch (id, name) \
             VALUES ('73716c2d-6272-816e-8368-2d6e616d6505', 'Name A')",
            &[],
        )
        .await
        .expect("first branch insert should succeed");
    session
        .execute(
            "INSERT INTO lix_branch (id, name) \
             VALUES ('73716c2d-6272-816e-8368-2d6e616d6506', 'Name B')",
            &[],
        )
        .await
        .expect("second branch insert should succeed");

    let error = session
        .execute(
            "UPDATE lix_branch \
             SET name = 'Name A' \
             WHERE id = '73716c2d-6272-816e-8368-2d6e616d6506'",
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
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        let error = session
            .execute(
                "INSERT INTO lix_branch (id, name, commit_id) \
                 VALUES ('73716c2d-6272-816e-8368-2d696e766100', 'Invalid Commit', 'ffffffff-ffff-4fff-bfff-ffffffffffff')",
                &[],
            )
            .await
            .expect_err("branch ref commit_id should reference an existing commit");
        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);

        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch WHERE id = '73716c2d-6272-816e-8368-2d696e766100'",
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
            .open_session()
            .await
            .expect("session should open"),
        &engine,
    );

    session
        .execute(
            "INSERT INTO lix_branch (id, name) \
             VALUES ('73716c2d-6272-816e-8368-2d69642d7500', 'Before')",
            &[],
        )
        .await
        .expect("branch insert should succeed");

    let error = session
        .execute(
            "UPDATE lix_branch \
             SET id = '73716c2d-6272-816e-8368-2d69642d7501' \
             WHERE id = '73716c2d-6272-816e-8368-2d69642d7500'",
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
            "SELECT COUNT(*) FROM lix_branch WHERE id = '73716c2d-6272-816e-8368-2d69642d7500'",
        )
        .await,
        1
    );
    assert_eq!(
        count_rows(
            &session,
            "SELECT COUNT(*) FROM lix_branch WHERE id = '73716c2d-6272-816e-8368-2d69642d7501'",
        )
        .await,
        0
    );
});

simulation_test!(lix_branch_update_rejects_global_branch, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("session should open"),
        &engine,
    );

    let error = session
        .execute(
            "UPDATE lix_branch SET name = 'mutated-global' WHERE id = 'ffffffff-ffff-7fff-bfff-ffffffffffff'",
            &[],
        )
        .await
        .expect_err("global branch should be immutable through UPDATE");
    assert!(
        error.to_string().contains("global branch"),
        "global update error should explain the restriction: {error:?}"
    );

    assert_eq!(
        select_single_text(
            &session,
            "SELECT name FROM lix_branch WHERE id = 'ffffffff-ffff-7fff-bfff-ffffffffffff'"
        )
        .await,
        "global"
    );
});

simulation_test!(
    lix_branch_delete_missing_returns_zero_rows_affected,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        let delete_result = session
            .execute(
                "DELETE FROM lix_branch WHERE id = '73716c2d-6272-816e-8368-2d6d69737300'",
                &[],
            )
            .await
            .expect("missing branch delete should be a no-op");
        assert_eq!(delete_result, ExecuteResult::from_rows_affected(0));
    }
);

simulation_test!(
    lix_branch_insert_on_conflict_do_update_changes_name,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('73716c2d-6272-816e-8368-2d7570736501', 'Before')",
                &[],
            )
            .await
            .expect("initial branch insert should succeed");

        let upsert_result = session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('73716c2d-6272-816e-8368-2d7570736501', 'After') \
                 ON CONFLICT (id) DO UPDATE SET name = excluded.name",
                &[],
            )
            .await
            .expect("upsert DO UPDATE should succeed");
        assert_eq!(upsert_result, ExecuteResult::from_rows_affected(1));

        assert_single_branch_row(
            &session,
            "73716c2d-6272-816e-8368-2d7570736501",
            "After",
            false,
            sim.initial_commit_id(),
        )
        .await;
        assert_eq!(
            count_rows(
                &session,
                "SELECT COUNT(*) FROM lix_branch WHERE id = '73716c2d-6272-816e-8368-2d7570736501'",
            )
            .await,
            1
        );
    }
);

simulation_test!(
    lix_branch_insert_on_conflict_do_nothing_keeps_name,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('73716c2d-6272-816e-8368-2d7570736501', 'Keep')",
                &[],
            )
            .await
            .expect("initial branch insert should succeed");

        let upsert_result = session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('73716c2d-6272-816e-8368-2d7570736501', 'Discard') \
                 ON CONFLICT (id) DO NOTHING",
                &[],
            )
            .await
            .expect("upsert DO NOTHING should succeed");
        assert_eq!(upsert_result, ExecuteResult::from_rows_affected(0));

        assert_single_branch_row(
            &session,
            "73716c2d-6272-816e-8368-2d7570736501",
            "Keep",
            false,
            sim.initial_commit_id(),
        )
        .await;
    }
);

simulation_test!(
    lix_branch_insert_on_conflict_inserts_absent_id,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );

        let upsert_result = session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('73716c2d-6272-816e-8368-2d7570736501', 'Fresh') \
                 ON CONFLICT (id) DO UPDATE SET name = excluded.name",
                &[],
            )
            .await
            .expect("upsert on an absent id should insert");
        assert_eq!(upsert_result, ExecuteResult::from_rows_affected(1));

        assert_single_branch_row(
            &session,
            "73716c2d-6272-816e-8368-2d7570736501",
            "Fresh",
            false,
            sim.initial_commit_id(),
        )
        .await;
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
