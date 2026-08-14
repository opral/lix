use lix::Value;

simulation_test!(untracked_insert_is_current_state_only, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine.open_session().await.expect("session should open"),
        &engine,
    );
    let head_before = branch_head(&session, sim.main_branch_id()).await;

    session
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-current-insert', 'one', true)",
            &[],
        )
        .await
        .expect("untracked insert should succeed");

    assert_untracked_current_state(&session, "untracked-current-insert").await;
    assert_eq!(
        change_count_for_key(&session, "untracked-current-insert").await,
        0,
        "ordinary untracked state must not create a lix_change record"
    );
    assert_eq!(
        branch_head(&session, sim.main_branch_id()).await,
        head_before,
        "an untracked-only write must not advance the active branch head"
    );
});

simulation_test!(
    untracked_overwrite_and_delete_remain_history_free,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let head_before = branch_head(&session, sim.main_branch_id()).await;

        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-current-overwrite', 'one', true)",
                &[],
            )
            .await
            .expect("initial untracked insert should succeed");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'two' \
                 WHERE key = 'untracked-current-overwrite'",
                &[],
            )
            .await
            .expect("untracked overwrite should succeed");

        let visible = session
            .execute(
                "SELECT value FROM lix_key_value \
                 WHERE key = 'untracked-current-overwrite'",
                &[],
            )
            .await
            .expect("untracked row should read");
        assert_eq!(
            visible.rows()[0].values(),
            &[Value::Json(serde_json::json!("two").into())]
        );
        assert_untracked_current_state(&session, "untracked-current-overwrite").await;
        assert_eq!(
            change_count_for_key(&session, "untracked-current-overwrite").await,
            0,
            "untracked replacement must not create history"
        );

        session
            .execute(
                "DELETE FROM lix_key_value WHERE key = 'untracked-current-overwrite'",
                &[],
            )
            .await
            .expect("untracked delete should succeed");
        let visible = session
            .execute(
                "SELECT value FROM lix_key_value \
                 WHERE key = 'untracked-current-overwrite'",
                &[],
            )
            .await
            .expect("live state should remain readable");
        assert!(visible.is_empty());
        assert_eq!(
            change_count_for_key(&session, "untracked-current-overwrite").await,
            0,
            "untracked deletion must physically remove state without a tombstone change"
        );
        assert_eq!(
            branch_head(&session, sim.main_branch_id()).await,
            head_before,
            "untracked mutations must not advance the active branch head"
        );
    }
);

simulation_test!(
    tracked_and_untracked_inserts_cannot_share_an_identity,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('untracked-current-tracked-first', 'tracked')",
                &[],
            )
            .await
            .expect("tracked seed insert should succeed");
        let error = session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-current-tracked-first', 'untracked', true)",
                &[],
            )
            .await
            .expect_err("untracked insert must reject a tracked identity");
        assert_eq!(error.code, lix::LixError::CODE_UNIQUE);

        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-current-untracked-first', 'untracked', true)",
                &[],
            )
            .await
            .expect("untracked seed insert should succeed");
        let error = session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('untracked-current-untracked-first', 'tracked')",
                &[],
            )
            .await
            .expect_err("tracked insert must reject an untracked identity");
        assert_eq!(error.code, lix::LixError::CODE_UNIQUE);
    }
);

simulation_test!(
    normal_sql_reads_tracked_and_untracked_current_members_together,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('untracked-current-normal-tracked', 'tracked')",
                &[],
            )
            .await
            .expect("tracked insert should succeed");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-current-normal-untracked', 'untracked', true)",
                &[],
            )
            .await
            .expect("untracked insert should succeed");

        let rows = session
            .execute(
                "SELECT key, value FROM lix_key_value \
                 WHERE key IN ('untracked-current-normal-tracked', \
                               'untracked-current-normal-untracked') \
                 ORDER BY key",
                &[],
            )
            .await
            .expect("normal SQL should read unified current state");
        assert_eq!(
            rows.rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![
                    Value::Text("untracked-current-normal-tracked".to_string()),
                    Value::Json(serde_json::json!("tracked").into()),
                ],
                vec![
                    Value::Text("untracked-current-normal-untracked".to_string()),
                    Value::Json(serde_json::json!("untracked").into()),
                ],
            ],
            "ordinary SQL has no retention lane: it returns both current members"
        );
    }
);

simulation_test!(
    mixed_transaction_commits_only_its_tracked_member,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let initial_head = branch_head(&session, sim.main_branch_id()).await;

        let mut untracked_transaction = session
            .begin_transaction()
            .await
            .expect("untracked transaction should begin");
        untracked_transaction
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-current-tx-a', 'a', true)",
                &[],
            )
            .await
            .expect("first untracked transaction write should stage");
        untracked_transaction
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-current-tx-b', 'b', true)",
                &[],
            )
            .await
            .expect("second untracked transaction write should stage");
        untracked_transaction
            .commit()
            .await
            .expect("untracked transaction should commit");

        assert_eq!(
            branch_head(&session, sim.main_branch_id()).await,
            initial_head
        );
        assert_untracked_current_state(&session, "untracked-current-tx-a").await;
        assert_untracked_current_state(&session, "untracked-current-tx-b").await;

        let mut mixed_transaction = session
            .begin_transaction()
            .await
            .expect("mixed transaction should begin");
        mixed_transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('untracked-current-tx-tracked', 'tracked')",
                &[],
            )
            .await
            .expect("tracked mixed-transaction write should stage");
        mixed_transaction
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-current-tx-untracked', 'untracked', true)",
                &[],
            )
            .await
            .expect("untracked mixed-transaction write should stage");
        mixed_transaction
            .commit()
            .await
            .expect("mixed transaction should commit");

        let mixed_head = branch_head(&session, sim.main_branch_id()).await;
        assert_ne!(mixed_head, initial_head);
        let tracked_change =
            current_tracked_change_id(&session, "untracked-current-tx-tracked").await;
        assert!(change_exists(&session, &tracked_change).await);
        assert_untracked_current_state(&session, "untracked-current-tx-untracked").await;
        assert_eq!(
            change_count_for_key(&session, "untracked-current-tx-untracked").await,
            0
        );

        let tracked_history = session
            .execute(
                &format!(
                    "SELECT lixcol_change_id FROM lix_key_value_history('{mixed_head}') \
                       WHERE key = 'untracked-current-tx-tracked' \
                       AND lixcol_depth = 0"
                ),
                &[],
            )
            .await
            .expect("tracked commit membership should read");
        assert_eq!(tracked_history.len(), 1);
        let untracked_history = session
            .execute(
                &format!(
                    "SELECT lixcol_change_id FROM lix_key_value_history('{mixed_head}') \
                       WHERE key = 'untracked-current-tx-untracked'"
                ),
                &[],
            )
            .await
            .expect("untracked commit membership should read");
        assert!(untracked_history.is_empty());
    }
);

simulation_test!(
    same_identity_cannot_switch_retention_within_one_transaction,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-current-same-tx', 'draft', true)",
                &[],
            )
            .await
            .expect("untracked draft should stage");
        let error = transaction
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-current-same-tx', 'published', false)",
                &[],
            )
            .await
            .expect_err("same-transaction retention switch should fail");
        assert_eq!(error.code, lix::LixError::CODE_INVALID_PARAM);
        transaction
            .rollback()
            .await
            .expect("failed transaction should roll back");
    }
);

simulation_test!(
    untracked_mutations_never_enter_the_tracked_working_diff,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .create_checkpoint()
            .await
            .expect("empty checkpoint should succeed");

        for sql in [
            "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-current-diff', 'one', true)",
            "UPDATE lix_key_value SET value = 'two' \
                 WHERE key = 'untracked-current-diff'",
            "DELETE FROM lix_key_value WHERE key = 'untracked-current-diff'",
        ] {
            session
                .execute(sql, &[])
                .await
                .expect("history-free current-state mutation should succeed");
            let working_diff = session
                .execute("SELECT COUNT(*) FROM lix_working_diff", &[])
                .await
                .expect("working diff should read");
            assert_eq!(
                working_diff.rows()[0].values(),
                &[Value::Integer(0)],
                "untracked mutations must not enter a tracked working diff"
            );
        }
    }
);

simulation_test!(
    checkpoint_does_not_rehome_untracked_rows,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-across-checkpoint', 'one', true)",
                &[],
            )
            .await
            .expect("untracked insert should succeed");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('tracked-across-checkpoint', 'one')",
                &[],
            )
            .await
            .expect("tracked insert should succeed");

        // A checkpoint re-homes every live tracked row onto the checkpoint
        // commit. Tracked and untracked rows now share one serving
        // generation, so this is the fence that proves the checkpoint's
        // selection is driven by the working diff -- which untracked rows
        // never enter -- rather than by "everything in the generation".
        session
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        assert_untracked_current_state(&session, "untracked-across-checkpoint").await;
        assert_eq!(
            change_count_for_key(&session, "untracked-across-checkpoint").await,
            0,
            "a checkpoint must not give an untracked row a change record"
        );
        assert!(
            !current_tracked_change_id(&session, "tracked-across-checkpoint")
                .await
                .is_empty(),
            "the tracked row beside it must still be re-homed by the checkpoint"
        );
    }
);

async fn branch_head(
    session: &crate::support::simulation_test::engine::SimSession,
    branch_id: &str,
) -> String {
    let result = session
        .execute(
            &format!("SELECT commit_id FROM lix_branch WHERE id = '{branch_id}'"),
            &[],
        )
        .await
        .expect("branch head should read");
    let [row] = result.rows() else {
        panic!("expected exactly one branch row for '{branch_id}'");
    };
    let Value::Text(commit_id) = &row.values()[0] else {
        panic!("expected branch commit_id to be text");
    };
    commit_id.clone()
}

async fn assert_untracked_current_state(
    session: &crate::support::simulation_test::engine::SimSession,
    key: &str,
) {
    let result = session
        .execute(
            &format!(
                "SELECT lixcol_change_id, lixcol_commit_id, lixcol_untracked \
                 FROM lix_key_value WHERE key = '{key}'"
            ),
            &[],
        )
        .await
        .expect("untracked current state should read");
    let [row] = result.rows() else {
        panic!("expected exactly one current row for key '{key}'");
    };
    // Untracked state is identity-bearing but history-free: it exposes a real
    // change id and no commit id. The change id is asserted as a property
    // rather than as a literal on purpose — the value is a function of UUID
    // draw order, so pinning it here would turn any unrelated change in draw
    // order into a spurious failure that invites re-recording. History
    // freedom itself is asserted directly against the changelog and history
    // views elsewhere in this file, which is where it belongs.
    let [change_id, commit_id, untracked] = row.values() else {
        panic!("expected three projected columns for key '{key}'");
    };
    match change_id {
        Value::Text(value) => assert!(
            uuid::Uuid::parse_str(value).is_ok_and(|parsed| !parsed.is_nil()),
            "untracked state must expose a real change id, got '{value}' for key '{key}'"
        ),
        other => panic!("untracked state must expose a change id, got {other:?}"),
    }
    assert_eq!(
        (commit_id, untracked),
        (&Value::Null, &Value::Boolean(true)),
        "ordinary untracked state must expose no commit id and stay untracked"
    );
}

async fn current_tracked_change_id(
    session: &crate::support::simulation_test::engine::SimSession,
    key: &str,
) -> String {
    let result = session
        .execute(
            &format!("SELECT lixcol_change_id FROM lix_key_value WHERE key = '{key}'"),
            &[],
        )
        .await
        .expect("tracked change id should read");
    let [row] = result.rows() else {
        panic!("expected exactly one current row for key '{key}'");
    };
    let Value::Text(change_id) = &row.values()[0] else {
        panic!("expected a tracked current row to have a text change id");
    };
    change_id.clone()
}

async fn change_count_for_key(
    session: &crate::support::simulation_test::engine::SimSession,
    key: &str,
) -> usize {
    session
        .execute(
            &format!(
                "SELECT id FROM lix_change WHERE schema_key = 'lix_key_value' \
                 AND entity_pk ->> 0 = '{key}'"
            ),
            &[],
        )
        .await
        .expect("lix_change should read")
        .len()
}

async fn change_exists(
    session: &crate::support::simulation_test::engine::SimSession,
    change_id: &str,
) -> bool {
    session
        .execute(
            &format!("SELECT id FROM lix_change WHERE id = '{change_id}'"),
            &[],
        )
        .await
        .expect("lix_change should read")
        .len()
        == 1
}
