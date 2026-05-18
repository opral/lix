use std::future::Future;

use lix_engine::backend::InMemoryBackend;
use lix_engine::LixError;
use lix_engine::{Engine, InitReceipt};

use super::expect_same::{SharedExpectSameRun, SharedExpectSameRunGuard, SimulationAssertions};
use super::mode::{SimulationMode, SimulationOptions};
use super::rebuild_tracked_state::deterministic_timestamp_shuffle_for;
use super::simulation::Simulation;

/// Runs one matrix entry for `simulation_test!`.
///
/// The macro generates one Rust test per mode. `assert_same` coordinates across
/// those test functions through shared state keyed by `case_id`.
pub async fn run_single_simulation_test<F, Fut>(
    mode: SimulationMode,
    options: SimulationOptions,
    case_id: &str,
    test_fn: F,
) where
    F: Fn(Simulation) -> Fut,
    Fut: Future<Output = ()>,
{
    let bootstrap = Bootstrap::create()
        .await
        .expect("simulation bootstrap should initialize");
    let expect_same = SharedExpectSameRun::new(case_id, mode);
    let _guard = SharedExpectSameRunGuard::new(expect_same.clone());
    let sim = Simulation::from_bootstrap(
        mode,
        options,
        bootstrap.backend,
        bootstrap.receipt,
        SimulationAssertions::shared(expect_same),
    )
    .await
    .expect("simulation mode should boot");
    test_fn(sim.clone()).await;
    sim.finish();
}

#[derive(Clone)]
struct Bootstrap {
    backend: InMemoryBackend,
    receipt: InitReceipt,
}

impl Bootstrap {
    async fn create() -> Result<Self, LixError> {
        let backend = InMemoryBackend::new();
        let receipt = Engine::initialize(backend.clone()).await?;
        Ok(Self { backend, receipt })
    }
}

pub(crate) async fn enable_deterministic_mode(
    engine: &Engine,
    receipt: &InitReceipt,
    mode: SimulationMode,
) -> Result<(), LixError> {
    let timestamp_shuffle = deterministic_timestamp_shuffle_for(mode);
    let session = engine.open_session(receipt.main_version_id.clone()).await?;
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
        "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
         VALUES ('lix_deterministic_mode', \
         lix_json('{{\"enabled\":true,\"timestamp_shuffle\":{timestamp_shuffle}}}'), true, true)"
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
