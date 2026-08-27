use lix::{CreateBranchOptions, LixError, Value};
use serde_json::json;

simulation_test!(
    lix_root_commit_id_returns_the_stable_repository_root,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let expected_root = sim.initial_global_commit_id().to_string();

        for value in ["one", "two", "three"] {
            session
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
                    &[
                        Value::Text(format!("root-{value}")),
                        Value::Text(value.to_owned()),
                    ],
                )
                .await
                .expect("tracked write should advance the active head");
        }

        let result = session
            .execute("SELECT lix_root_commit_id() AS root_commit_id", &[])
            .await
            .expect("root commit UDF should execute");
        assert_eq!(
            result.rows()[0].get::<String>("root_commit_id").unwrap(),
            expected_root
        );

        let parents = session
            .execute(
                "SELECT parent_commit_ids FROM lix_commit WHERE id = lix_root_commit_id()",
                &[],
            )
            .await
            .expect("the root commit should have no parents");
        assert_eq!(
            parents.rows()[0].values(),
            &[Value::Jsonb(json!([]).into())]
        );
    }
);

simulation_test!(
    lix_active_branch_commit_id_returns_active_head,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('active-head', 'one')",
                &[],
            )
            .await
            .expect("tracked write should succeed");
        let expected = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist");

        let result = session
            .execute("SELECT lix_active_branch_commit_id()", &[])
            .await
            .expect("active head UDF should execute");

        assert_eq!(
            result.rows()[0]
                .get::<String>("lix_active_branch_commit_id()")
                .unwrap(),
            expected
        );
    }
);

simulation_test!(
    lix_latest_checkpoint_commit_id_is_scoped_to_the_active_branch,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let root = sim.initial_commit_id().to_string();
        assert_eq!(latest_checkpoint_commit_id(&main).await, root);

        let invalid = main
            .execute("SELECT lix_latest_checkpoint_commit_id('extra')", &[])
            .await
            .expect_err("the checkpoint accessor must reject arguments");
        assert_eq!(invalid.code, LixError::CODE_INVALID_PARAM);

        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-scope', 'main')",
            &[],
        )
        .await
        .expect("main change should commit");
        let inherited = main
            .create_checkpoint()
            .await
            .expect("main checkpoint should commit");
        assert_eq!(
            latest_checkpoint_commit_id(&main).await,
            inherited.commit_id
        );

        let branch_id = "01930000-0000-7000-8000-0000000000c1";
        main.create_branch(CreateBranchOptions {
            id: Some(branch_id.to_string()),
            name: "Checkpoint UDF draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created");
        let draft = sim.wrap_session(
            engine
                .open_session_at(branch_id)
                .await
                .expect("draft session should open"),
            &engine,
        );
        assert_eq!(
            latest_checkpoint_commit_id(&draft).await,
            inherited.commit_id
        );

        main.execute(
            "UPDATE lix_key_value SET value = 'main-next' WHERE key = 'checkpoint-scope'",
            &[],
        )
        .await
        .expect("main should diverge");
        let main_checkpoint = main
            .create_checkpoint()
            .await
            .expect("newer main checkpoint should commit");
        assert_eq!(
            latest_checkpoint_commit_id(&draft).await,
            inherited.commit_id,
            "a newer global checkpoint on another branch must not leak into the draft"
        );

        draft
            .execute(
                "UPDATE lix_key_value SET value = 'draft' WHERE key = 'checkpoint-scope'",
                &[],
            )
            .await
            .expect("draft should diverge");
        let diff = draft
            .execute(
                "SELECT key FROM lix_diff(\
                 'lix_key_value', lix_latest_checkpoint_commit_id(), \
                 lix_active_branch_commit_id())",
                &[],
            )
            .await
            .expect("the checkpoint accessor should work directly as a diff argument");
        assert_eq!(diff.len(), 1);
        let draft_checkpoint = draft
            .create_checkpoint()
            .await
            .expect("draft checkpoint should commit");
        assert_eq!(
            latest_checkpoint_commit_id(&draft).await,
            draft_checkpoint.commit_id
        );
        assert_eq!(
            latest_checkpoint_commit_id(&main).await,
            main_checkpoint.commit_id,
            "a newer global draft checkpoint must not replace the main baseline"
        );

        let mut transaction = draft
            .begin_transaction()
            .await
            .expect("draft transaction should begin");
        let result = transaction
            .execute("SELECT lix_latest_checkpoint_commit_id() AS commit_id", &[])
            .await
            .expect("the branch checkpoint should resolve inside read transactions");
        assert_eq!(
            result.rows()[0].get::<String>("commit_id").unwrap(),
            draft_checkpoint.commit_id
        );
        transaction
            .rollback()
            .await
            .expect("read transaction should roll back");
    }
);

simulation_test!(
    lix_latest_checkpoint_commit_id_ignores_non_checkpoint_fork_and_restore_cursors,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );
        let root = sim.initial_commit_id().to_string();
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-fork', 'dirty-root')",
            &[],
        )
        .await
        .expect("an uncheckpointed main change should commit");

        let root_fork_id = "01930000-0000-7000-8000-0000000000c2";
        main.create_branch(CreateBranchOptions {
            id: Some(root_fork_id.to_string()),
            name: "Uncheckpointed fork".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("fork from an uncheckpointed commit should succeed");
        let root_fork = sim.wrap_session(
            engine
                .open_session_at(root_fork_id)
                .await
                .expect("uncheckpointed fork session should open"),
            &engine,
        );
        assert_eq!(
            latest_checkpoint_commit_id(&root_fork).await,
            root,
            "a fork's private baseline is not itself a checkpoint marker"
        );

        let checkpoint = main
            .create_checkpoint()
            .await
            .expect("main checkpoint should commit");
        main.execute(
            "UPDATE lix_key_value SET value = 'dirty-checkpoint' WHERE key = 'checkpoint-fork'",
            &[],
        )
        .await
        .expect("main should advance beyond its real checkpoint");
        let checkpoint_fork_id = "01930000-0000-7000-8000-0000000000c3";
        main.create_branch(CreateBranchOptions {
            id: Some(checkpoint_fork_id.to_string()),
            name: "Post-checkpoint dirty fork".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("fork from a post-checkpoint commit should succeed");
        let checkpoint_fork = sim.wrap_session(
            engine
                .open_session_at(checkpoint_fork_id)
                .await
                .expect("post-checkpoint fork session should open"),
            &engine,
        );
        assert_eq!(
            latest_checkpoint_commit_id(&checkpoint_fork).await,
            checkpoint.commit_id,
            "a dirty fork should inherit its nearest real mainline checkpoint"
        );

        let abandoned = main
            .create_checkpoint()
            .await
            .expect("later checkpoint should commit");
        main.execute(
            "INSERT INTO lix_restore (commit_id) VALUES ($1)",
            &[Value::Text(checkpoint.commit_id.clone())],
        )
        .await
        .expect("restoring the older checkpoint should succeed");
        assert_eq!(
            latest_checkpoint_commit_id(&main).await,
            checkpoint.commit_id,
            "an abandoned later global checkpoint must not remain active"
        );
        assert_ne!(abandoned.commit_id, checkpoint.commit_id);

        main.execute(
            "UPDATE lix_key_value SET value = 'restore-target' WHERE key = 'checkpoint-fork'",
            &[],
        )
        .await
        .expect("ordinary restore target should commit");
        let restore_target = main
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await
            .expect("restore target head should read")
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        main.execute(
            "UPDATE lix_key_value SET value = 'after-target' WHERE key = 'checkpoint-fork'",
            &[],
        )
        .await
        .expect("later ordinary change should commit");
        main.execute(
            "INSERT INTO lix_restore (commit_id) VALUES ($1)",
            &[Value::Text(restore_target)],
        )
        .await
        .expect("restoring an ordinary commit should succeed");
        assert_eq!(
            latest_checkpoint_commit_id(&main).await,
            checkpoint.commit_id,
            "a non-checkpoint restore target must not masquerade as a checkpoint"
        );
    }
);

async fn latest_checkpoint_commit_id(
    session: &crate::support::simulation_test::engine::SimSession,
) -> String {
    session
        .execute("SELECT lix_latest_checkpoint_commit_id() AS commit_id", &[])
        .await
        .expect("latest checkpoint accessor should execute")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("latest checkpoint accessor should return text")
}

simulation_test!(
    lix_active_branch_id_is_session_scoped_in_reads_transactions_and_writes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-000000000016".to_string()),
            name: "UDF draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created");
        let draft = sim.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-000000000016")
                .await
                .expect("draft session should open"),
            &engine,
        );

        let main_result = main
            .execute("SELECT lix_active_branch_id() AS branch_id", &[])
            .await
            .expect("main branch UDF should execute");
        assert_eq!(
            main_result.rows()[0].get::<String>("branch_id").unwrap(),
            sim.main_branch_id()
        );
        let draft_result = draft
            .execute("SELECT lix_active_branch_id() AS branch_id", &[])
            .await
            .expect("draft branch UDF should execute");
        assert_eq!(
            draft_result.rows()[0].get::<String>("branch_id").unwrap(),
            "01930000-0000-7000-8000-000000000016"
        );

        let mut transaction = draft
            .begin_transaction()
            .await
            .expect("draft transaction should begin");
        let transaction_result = transaction
            .execute("SELECT lix_active_branch_id() AS branch_id", &[])
            .await
            .expect("transaction branch UDF should execute");
        assert_eq!(
            transaction_result.rows()[0]
                .get::<String>("branch_id")
                .unwrap(),
            "01930000-0000-7000-8000-000000000016"
        );
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('61637469-7665-8d62-8261-6e63682d7500', lix_active_branch_id())",
                &[],
            )
            .await
            .expect("branch UDF should execute in a bound write");
        transaction
            .commit()
            .await
            .expect("draft transaction should commit");

        let stored = draft
            .execute(
                "SELECT value FROM lix_key_value WHERE key = '61637469-7665-8d62-8261-6e63682d7500'",
                &[],
            )
            .await
            .expect("stored branch should read");
        assert_eq!(
            stored.rows()[0].value("value").unwrap(),
            &Value::Jsonb(json!("01930000-0000-7000-8000-000000000016").into())
        );
    }
);

simulation_test!(
    lix_active_branch_commit_id_is_available_to_bound_writes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('active-head-write-seed', 'one')",
                &[],
            )
            .await
            .expect("tracked write should establish an active head");
        let expected = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head should load")
            .expect("head should exist");

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('active-head-write', lix_active_branch_commit_id())",
                &[],
            )
            .await
            .expect("bound write should evaluate active head UDF");

        let stored = session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'active-head-write'",
                &[],
            )
            .await
            .expect("stored active head should read");
        assert_eq!(
            stored.rows()[0].value("value").unwrap(),
            &Value::Jsonb(json!(expected.clone()).into())
        );
    }
);
