use lix_engine::Value;

const CHECKPOINT_LABEL_ID: &str = "lix_label_checkpoint";

simulation_test!(
    lix_commit_and_change_set_are_read_only_public_surfaces,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        let change_set_error = engine
            .execute("INSERT INTO lix_change_set (id) VALUES ('cs-blocked')", &[])
            .await
            .expect_err("INSERT on lix_change_set should fail");
        assert_eq!(
            change_set_error.code,
            "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED"
        );

        let commit_error = engine
            .execute(
                "INSERT INTO lix_commit (id, change_set_id) VALUES ('commit-blocked', 'cs-blocked')",
                &[],
            )
            .await
            .expect_err("INSERT on lix_commit should fail");
        assert_eq!(commit_error.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");
    }
);

simulation_test!(
    checkpoint_label_is_system_managed,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        let delete_error = engine
            .execute(
                "DELETE FROM lix_label WHERE id = $1",
                &[Value::Text(CHECKPOINT_LABEL_ID.to_string())],
            )
            .await
            .expect_err("checkpoint label delete should fail");
        assert!(
            delete_error
                .description
                .contains("checkpoint label is system-managed"),
            "unexpected delete error: {delete_error}"
        );

        let update_error = engine
            .execute(
                "UPDATE lix_label SET name = 'checkpoint-renamed' WHERE id = $1",
                &[Value::Text(CHECKPOINT_LABEL_ID.to_string())],
            )
            .await
            .expect_err("checkpoint label update should fail");
        assert!(
            update_error
                .description
                .contains("checkpoint label is system-managed"),
            "unexpected update error: {update_error}"
        );

        let insert_error = engine
            .execute(
                "INSERT INTO lix_label (id, name) VALUES ($1, 'not-checkpoint')",
                &[Value::Text(CHECKPOINT_LABEL_ID.to_string())],
            )
            .await
            .expect_err("checkpoint label insert should fail");
        assert!(
            insert_error
                .description
                .contains("checkpoint label is system-managed"),
            "unexpected insert error: {insert_error}"
        );
    }
);
