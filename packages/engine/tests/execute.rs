mod support;

use lix_engine::Value;

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
