use lix_engine::{CreateBranchOptions, Value};
use serde_json::json;

simulation_test!(
    lix_active_branch_commit_id_returns_active_head,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
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
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.create_branch(CreateBranchOptions {
            id: Some("udf-draft".to_string()),
            name: "UDF draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created");
        let draft = sim.wrap_session(
            engine
                .open_session("udf-draft")
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
            "udf-draft"
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
            "udf-draft"
        );
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('active-branch-udf', lix_active_branch_id())",
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
                "SELECT value FROM lix_key_value WHERE key = 'active-branch-udf'",
                &[],
            )
            .await
            .expect("stored branch should read");
        assert_eq!(
            stored.rows()[0].value("value").unwrap(),
            &Value::Json(json!("udf-draft"))
        );
    }
);
