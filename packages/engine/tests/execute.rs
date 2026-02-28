mod support;

use lix_engine::Value;
use support::simulation_test::SimulationBootArgs;

simulation_test!(select_works, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    let result = engine.execute("SELECT 1 + 1", &[]).await.unwrap();
    sim.assert_deterministic(result.rows.clone());
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Integer(2));
});

simulation_test!(explain_lix_state_query_works, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    let result = engine
        .execute(
            "EXPLAIN SELECT COUNT(*) FROM lix_state WHERE file_id = 'missing' AND plugin_key = 'plugin_json'",
            &[],
        )
        .await
        .unwrap();

    assert!(
        !result.rows.is_empty(),
        "EXPLAIN over lix_state should return a plan"
    );
});

simulation_test!(
    dml_without_returning_returns_empty_public_rowset,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("init should succeed");

        engine
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ('/contract-delete.md', lix_text_encode('hello'))",
            &[],
        )
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
            deleted.columns.is_empty(),
            "DELETE without RETURNING must not expose internal columns"
        );
        assert!(
            deleted.rows.is_empty(),
            "DELETE without RETURNING must not expose internal rows"
        );
    }
);

simulation_test!(
    sqlite_master_query_returns_table_not_found,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        let error = engine
            .execute(
                "SELECT name FROM sqlite_master WHERE type = 'view' ORDER BY name",
                &[],
            )
            .await
            .expect_err("sqlite_master read should be rejected");

        assert_eq!(error.code, "LIX_ERROR_TABLE_NOT_FOUND");
    }
);

simulation_test!(
    internal_table_read_returns_access_denied,
    |sim| async move {
        let mut boot_args = SimulationBootArgs::default();
        boot_args.access_to_internal = false;
        let engine = sim
            .boot_simulated_engine(Some(boot_args))
            .await
            .expect("boot_simulated_engine should succeed");

        let error = engine
            .execute("SELECT * FROM lix_internal_state_vtable", &[])
            .await
            .expect_err("internal table read should be rejected");

        assert_eq!(error.code, "LIX_ERROR_INTERNAL_TABLE_ACCESS_DENIED");
    }
);
