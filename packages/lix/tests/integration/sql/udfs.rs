use lix::{CreateBranchOptions, Value};
use serde_json::json;

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
