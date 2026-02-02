use lix_engine::simulation_test::{default_simulations, simulation_test};
use lix_engine::{open_lix, LixBackend, OpenLixConfig, SqliteBackend, SqliteConfig, Value};

fn sqlite_backend() -> Box<dyn LixBackend + Send + Sync> {
    Box::new(SqliteBackend::new(SqliteConfig {
        filename: ":memory:".to_string(),
    }))
}

#[tokio::test]
async fn sqlite_select_works() {
    let lix = open_lix(OpenLixConfig {
        backend: sqlite_backend(),
    })
    .await
    .expect("open_lix should succeed");

    let result = lix.execute("SELECT 1 + 1", &[]).await.unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Integer(2));
}

#[tokio::test]
async fn simulation_select_works() {
    simulation_test("select", Some(default_simulations()), |sim| async move {
        let lix = sim
            .open_simulated_lix()
            .await
            .expect("open_lix should succeed");
        let result = lix.execute("SELECT 1 + 1", &[]).await.unwrap();
        sim.expect_deterministic(result.rows.clone());
    })
    .await;
}
