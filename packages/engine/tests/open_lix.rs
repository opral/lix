use lix_engine::simulation_test::{default_simulations, simulation_test};
use lix_engine::Value;

#[tokio::test]
async fn select_works() {
    simulation_test(default_simulations(), |sim| async move {
        let lix = sim
            .open_simulated_lix()
            .await
            .expect("open_lix should succeed");
        let result = lix.execute("SELECT 1 + 1", &[]).await.unwrap();
        sim.expect_deterministic(result.rows.clone());
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(2));
    })
    .await;
}
