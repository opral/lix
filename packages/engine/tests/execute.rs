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
