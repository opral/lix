mod support;

simulation_test!(init_creates_untracked_table, |sim| async move {
    let engine = sim
        .boot_simulated_engine()
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    let result = engine
        .execute("SELECT 1 FROM lix_internal_state_untracked LIMIT 1", &[])
        .await
        .unwrap();

    sim.expect_deterministic(result.rows.clone());
});
