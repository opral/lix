use lix_engine::Value;

simulation_test!(
    untracked_insert_creates_change_without_advancing_branch_head,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );
        let head_before = branch_head(&session, sim.main_branch_id()).await;

        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-ledger-insert', 'one', true)",
                &[],
            )
            .await
            .expect("untracked insert should succeed");

        let change_id = current_change_id(&session, "untracked-ledger-insert").await;
        assert!(
            change_exists(&session, &change_id).await,
            "untracked current row should reference a visible lix_change row"
        );
        assert_eq!(
            branch_head(&session, sim.main_branch_id()).await,
            head_before,
            "an untracked-only write must not advance the active branch head"
        );
    }
);

simulation_test!(
    untracked_overwrite_replaces_compactable_change,
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
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-ledger-overwrite', 'one', true)",
                &[],
            )
            .await
            .expect("initial untracked insert should succeed");
        let first_change_id = current_change_id(&session, "untracked-ledger-overwrite").await;

        session
            .execute(
                "UPDATE lix_key_value \
                 SET value = 'two' \
                 WHERE key = 'untracked-ledger-overwrite'",
                &[],
            )
            .await
            .expect("untracked overwrite should succeed");
        let second_change_id = current_change_id(&session, "untracked-ledger-overwrite").await;

        assert_ne!(
            second_change_id, first_change_id,
            "each untracked mutation should receive a fresh change id"
        );
        assert!(
            change_exists(&session, &second_change_id).await,
            "the replacement untracked change should be visible in lix_change"
        );
        assert!(
            !change_exists(&session, &first_change_id).await,
            "the superseded untracked change should be compacted"
        );
    }
);

simulation_test!(
    deleting_untracked_current_row_does_not_reveal_tracked_history,
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
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('untracked-ledger-delete', 'tracked-history')",
                &[],
            )
            .await
            .expect("tracked seed insert should succeed");
        let tracked_head = branch_head(&session, sim.main_branch_id()).await;

        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-ledger-delete', 'untracked-current', true)",
                &[],
            )
            .await
            .expect("untracked update over tracked history should succeed");
        let pre_delete_change = current_change_id(&session, "untracked-ledger-delete").await;

        session
            .execute(
                "DELETE FROM lix_state \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_pk = lix_json('[\"untracked-ledger-delete\"]')",
                &[],
            )
            .await
            .expect("untracked delete should succeed");

        let visible = session
            .execute(
                "SELECT value FROM lix_key_value \
                 WHERE key = 'untracked-ledger-delete'",
                &[],
            )
            .await
            .expect("current state should remain readable");
        assert_eq!(
            visible.len(),
            0,
            "deleting untracked current state must not reveal the older tracked value"
        );
        assert_eq!(
            branch_head(&session, sim.main_branch_id()).await,
            tracked_head,
            "untracked update and delete must not advance the branch head"
        );
        let tombstones = session
            .execute(
                "SELECT id FROM lix_change \
                 WHERE schema_key = 'lix_key_value' \
                   AND lix_json_get_text(entity_pk, 0) = 'untracked-ledger-delete' \
                   AND snapshot_content IS NULL",
                &[],
            )
            .await
            .expect("deletion change should read");
        assert_eq!(
            tombstones.len(),
            1,
            "the deletion must remain as a ledger tombstone"
        );
        assert!(
            !change_exists(&session, &pre_delete_change).await,
            "the pre-deletion untracked value should be compacted"
        );
        let historical = session
            .execute(
                &format!(
                    "SELECT snapshot_content FROM lix_state_history \
                     WHERE start_commit_id = '{tracked_head}' \
                       AND schema_key = 'lix_key_value' \
                       AND lix_json_get_text(entity_pk, 0) = 'untracked-ledger-delete' \
                       AND depth = 0"
                ),
                &[],
            )
            .await
            .expect("tracked history should remain readable");
        assert_eq!(
            historical.len(),
            1,
            "tracked history must survive current-state replacement"
        );
    }
);

simulation_test!(
    tracked_write_after_untracked_becomes_canonical,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );
        let head_before = branch_head(&session, sim.main_branch_id()).await;

        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-ledger-promote', 'draft', true)",
                &[],
            )
            .await
            .expect("untracked insert should succeed");
        let untracked_change_id = current_change_id(&session, "untracked-ledger-promote").await;

        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-ledger-promote', 'tracked', false)",
                &[],
            )
            .await
            .expect("tracked write after untracked state should succeed");
        let tracked_change_id = current_change_id(&session, "untracked-ledger-promote").await;

        assert_ne!(tracked_change_id, untracked_change_id);
        assert!(change_exists(&session, &tracked_change_id).await);
        assert!(
            !change_exists(&session, &untracked_change_id).await,
            "the superseded untracked change should be compacted"
        );

        let row = session
            .execute(
                "SELECT value, lixcol_untracked FROM lix_key_value \
                 WHERE key = 'untracked-ledger-promote'",
                &[],
            )
            .await
            .expect("canonical row should read");
        assert_eq!(
            row.rows()[0].values(),
            &[
                Value::Json(serde_json::json!("tracked")),
                Value::Boolean(false)
            ]
        );
        assert_ne!(
            branch_head(&session, sim.main_branch_id()).await,
            head_before,
            "the tracked replacement should advance the branch head"
        );
    }
);

simulation_test!(
    mixed_and_untracked_only_transactions_have_expected_head_behavior,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
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
                 VALUES ('untracked-ledger-tx-a', 'a', true)",
                &[],
            )
            .await
            .expect("first untracked transaction write should stage");
        untracked_transaction
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-ledger-tx-b', 'b', true)",
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
            initial_head,
            "an untracked-only transaction must not advance the branch head"
        );
        let untracked_a = current_change_id(&session, "untracked-ledger-tx-a").await;
        let untracked_b = current_change_id(&session, "untracked-ledger-tx-b").await;
        assert!(change_exists(&session, &untracked_a).await);
        assert!(change_exists(&session, &untracked_b).await);

        let mut mixed_transaction = session
            .begin_transaction()
            .await
            .expect("mixed transaction should begin");
        mixed_transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('untracked-ledger-tx-tracked', 'tracked')",
                &[],
            )
            .await
            .expect("tracked mixed-transaction write should stage");
        mixed_transaction
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-ledger-tx-untracked', 'untracked', true)",
                &[],
            )
            .await
            .expect("untracked mixed-transaction write should stage");
        mixed_transaction
            .commit()
            .await
            .expect("mixed transaction should commit");

        let mixed_head = branch_head(&session, sim.main_branch_id()).await;
        assert_ne!(
            mixed_head, initial_head,
            "the tracked member of a mixed transaction should advance the branch head"
        );
        let tracked_change = current_change_id(&session, "untracked-ledger-tx-tracked").await;
        let untracked_change = current_change_id(&session, "untracked-ledger-tx-untracked").await;
        assert!(change_exists(&session, &tracked_change).await);
        assert!(change_exists(&session, &untracked_change).await);

        let tracked_history = session
            .execute(
                &format!(
                    "SELECT change_id FROM lix_state_history \
                     WHERE start_commit_id = '{mixed_head}' \
                       AND schema_key = 'lix_key_value' \
                       AND lix_json_get_text(entity_pk, 0) = 'untracked-ledger-tx-tracked' \
                       AND depth = 0"
                ),
                &[],
            )
            .await
            .expect("tracked commit membership should read");
        assert_eq!(tracked_history.len(), 1);
        let untracked_history = session
            .execute(
                &format!(
                    "SELECT change_id FROM lix_state_history \
                     WHERE start_commit_id = '{mixed_head}' \
                       AND schema_key = 'lix_key_value' \
                       AND lix_json_get_text(entity_pk, 0) = 'untracked-ledger-tx-untracked'"
                ),
                &[],
            )
            .await
            .expect("untracked commit membership should read");
        assert_eq!(
            untracked_history.len(),
            0,
            "the mixed transaction's untracked change must not become a commit member"
        );
    }
);

simulation_test!(
    same_identity_cannot_switch_durability_within_one_transaction,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("mixed transaction should begin");

        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-ledger-same-tx', 'draft', true)",
                &[],
            )
            .await
            .expect("untracked draft should stage");
        let error = transaction
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
                 VALUES ('untracked-ledger-same-tx', 'published', false)",
                &[],
            )
            .await
            .expect_err("same-transaction durability switch should fail");
        assert_eq!(error.code, lix_engine::LixError::CODE_INVALID_PARAM);
        assert!(
            error
                .message
                .contains("cannot mix tracked and untracked writes"),
            "error should explain the unsupported durability switch: {error:?}"
        );
        transaction
            .rollback()
            .await
            .expect("failed transaction should roll back");

        let current = session
            .execute(
                "SELECT key FROM lix_key_value \
                 WHERE key = 'untracked-ledger-same-tx'",
                &[],
            )
            .await
            .expect("rolled-back state should query");
        assert!(current.is_empty());
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

async fn current_change_id(
    session: &crate::support::simulation_test::engine::SimSession,
    key: &str,
) -> String {
    let result = session
        .execute(
            &format!("SELECT lixcol_change_id FROM lix_key_value WHERE key = '{key}'"),
            &[],
        )
        .await
        .expect("current change id should read");
    let [row] = result.rows() else {
        panic!("expected exactly one current row for key '{key}'");
    };
    let Value::Text(change_id) = &row.values()[0] else {
        panic!("expected a non-null text change id for key '{key}'");
    };
    change_id.clone()
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
