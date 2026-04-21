use lix_engine::{ExecuteOptions, Value};

simulation_test!(
    execute_before_init_returns_not_initialized,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        let err = engine
            .execute("SELECT 1 + 1", &[])
            .await
            .expect_err("execute before init should fail");
        assert_eq!(err.code, "LIX_ERROR_NOT_INITIALIZED");
    }
);

simulation_test!(select_works_after_init, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.unwrap();

    let result = engine.execute("SELECT 1 + 1", &[]).await.unwrap();
    sim.assert_deterministic(result.statements[0].rows.clone());
    assert_eq!(result.statements[0].rows.len(), 1);
    assert_eq!(result.statements[0].rows[0][0], Value::Integer(2));
});

simulation_test!(backend_json_extract_is_rejected, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.unwrap();

    let error = engine
        .execute("SELECT json_extract('{\"value\":1}', '$.value')", &[])
        .await
        .expect_err("backend-specific json_extract should be rejected");

    assert_eq!(error.code, "LIX_ERROR_SQL_UNSUPPORTED_FUNCTION");
    assert!(error.description.contains("json_extract"));
});

simulation_test!(
    dml_without_returning_returns_empty_public_rowset,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ('/contract-delete.md', lix_text_encode('hello'))", &[])
        .await
        .expect("file insert should succeed");

        let deleted = engine
            .execute(
                "DELETE FROM lix_file WHERE path = '/contract-delete.md'",
                &[],
            )
            .await
            .expect("delete should succeed");

        assert!(
            deleted.statements[0].columns.is_empty(),
            "DELETE without RETURNING must not expose internal columns"
        );
        assert!(
            deleted.statements[0].rows.is_empty(),
            "DELETE without RETURNING must not expose internal rows"
        );
    }
);

simulation_test!(
    sqlite_master_query_is_rejected,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let error = engine
            .execute("SELECT name FROM sqlite_master ORDER BY name LIMIT 1", &[])
            .await
            .expect_err("sqlite_master read should be rejected");

        assert!(
            !error.code.is_empty(),
            "sqlite_master read should fail with a normal compiler/runtime error"
        );
    }
);

simulation_test!(
    internal_table_read_returns_access_denied,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let error = engine
        .execute(
            "SELECT COUNT(*) FROM lix_internal_live_v1_lix_active_version WHERE untracked = true",
            &[],
        )
        .await
        .expect_err("internal table read should be rejected");
        assert!(!error.code.is_empty());
    }
);

simulation_test!(
    internal_table_drop_table_returns_access_denied,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let error = engine
            .execute("DROP TABLE lix_state_by_version", &[])
            .await
            .expect_err("internal table drop should be rejected");
        assert!(!error.code.is_empty());
    }
);

simulation_test!(
    internal_table_alter_table_returns_access_denied,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let error = engine
            .execute(
                "ALTER TABLE lix_state_by_version ADD COLUMN blocked INTEGER",
                &[],
            )
            .await
            .expect_err("internal table alter should be rejected");
        assert!(!error.code.is_empty());
    }
);

simulation_test!(
    internal_table_create_trigger_returns_access_denied,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let error = engine
            .execute(
                "CREATE TRIGGER lix_blocked_trigger AFTER INSERT ON lix_state_by_version BEGIN SELECT 1; END",
                &[],
            )
            .await
            .expect_err("internal table trigger creation should be rejected");
        assert!(!error.code.is_empty());
    }
);

simulation_test!(
    public_create_table_is_denied,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let error = engine
            .execute("CREATE TABLE user_data (id TEXT)", &[])
            .await
            .expect_err("public CREATE TABLE should be rejected");

        assert_eq!(error.code, "LIX_ERROR_PUBLIC_CREATE_TABLE_DENIED");
        assert!(error.description.contains("CREATE TABLE is not supported"));
        assert!(error.description.contains("lix_registered_schema"));
        assert!(error.description.contains("queryable entity views"));
    }
);

simulation_test!(
    public_create_table_in_transaction_is_denied,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.expect("init should succeed");

        let mut tx = engine
            .begin_transaction_with_options(ExecuteOptions::default())
            .await
            .expect("begin transaction should succeed");
        let error = tx
            .execute("CREATE TABLE user_data (id TEXT)", &[])
            .await
            .expect_err("public CREATE TABLE in transaction should be rejected");

        assert_eq!(error.code, "LIX_ERROR_PUBLIC_CREATE_TABLE_DENIED");
        tx.rollback().await.expect("rollback should succeed");
    }
);
