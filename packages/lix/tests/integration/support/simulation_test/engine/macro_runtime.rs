use std::future::Future;

use lix::LixError;
use lix::integration::{Engine, InitReceipt};
use lix::storage::Memory;

use super::mode::{SimulationMode, SimulationOptions};
use super::simulation::Simulation;

/// Runs one generated test entry for `simulation_test!`.
///
pub async fn run_simulation_test<F, Fut>(
    mode: SimulationMode,
    options: SimulationOptions,
    _case_id: &str,
    test_fn: F,
) where
    F: Fn(Simulation) -> Fut,
    Fut: Future<Output = ()>,
{
    let bootstrap = Bootstrap::create()
        .await
        .expect("simulation bootstrap should initialize");
    let sim = Simulation::from_bootstrap(mode, options, bootstrap.storage, bootstrap.receipt)
        .await
        .expect("simulation mode should boot");
    test_fn(sim).await;
}

#[derive(Clone)]
struct Bootstrap {
    storage: Memory,
    receipt: InitReceipt,
}

impl Bootstrap {
    async fn create() -> Result<Self, LixError> {
        let storage = Memory::new();
        let receipt = Engine::initialize(storage.clone()).await?;
        Ok(Self { storage, receipt })
    }
}

pub(crate) async fn enable_deterministic_mode(
    engine: &Engine,
    receipt: &InitReceipt,
) -> Result<(), LixError> {
    let timestamp_shuffle = false;
    let session = engine.open_session(receipt.main_branch_id.clone()).await?;
    match session
        .execute(&deterministic_mode_insert_sql(timestamp_shuffle), &[])
        .await
    {
        Ok(_) => {}
        Err(error) if error.code == "LIX_UNSUPPORTED_SQL" => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

fn deterministic_mode_insert_sql(timestamp_shuffle: bool) -> String {
    format!(
        "INSERT INTO lix_key_value (key, value, lixcol_global) \
         VALUES ('lix_deterministic_mode', \
         lix_json('{{\"enabled\":true,\"timestamp_shuffle\":{timestamp_shuffle}}}'), true)"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_mode_write_sql_carries_timestamp_shuffle_flag() {
        assert!(deterministic_mode_insert_sql(true).contains("\"timestamp_shuffle\":true"));
        assert!(deterministic_mode_insert_sql(false).contains("\"timestamp_shuffle\":false"));
    }
}
