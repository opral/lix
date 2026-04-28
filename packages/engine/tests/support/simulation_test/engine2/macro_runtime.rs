use std::future::Future;

use lix_engine::engine2::{Engine, InitReceipt};
use lix_engine::LixError;

use super::expect_same::{
    Engine2SimulationAssertions, SharedExpectSameRun, SharedExpectSameRunGuard,
};
use super::kv_backend::{InMemoryKvBackend, KvMap};
use super::mode::{Engine2SimulationMode, Engine2SimulationOptions};
use super::rebuild_tracked_state::deterministic_timestamp_shuffle_for;
use super::simulation::Engine2Simulation;

/// Runs one test body against the engine2 simulation matrix.
pub async fn run_simulation_test<F, Fut>(options: Engine2SimulationOptions, test_fn: F)
where
    F: Fn(Engine2Simulation) -> Fut,
    Fut: Future<Output = ()>,
{
    let bootstrap = Engine2Bootstrap::create()
        .await
        .expect("engine2 simulation bootstrap should initialize");
    let assertions = Engine2SimulationAssertions::new_local();

    for mode in [
        Engine2SimulationMode::Base,
        Engine2SimulationMode::TrackedStateRebuild,
    ] {
        let sim = Engine2Simulation::from_bootstrap(
            mode,
            options,
            bootstrap.snapshot.clone(),
            bootstrap.receipt.clone(),
            assertions.clone(),
        )
        .await
        .expect("engine2 simulation mode should boot");
        test_fn(sim.clone()).await;
        sim.finish();
    }
}

/// Runs one matrix entry for `simulation_test2!`.
///
/// The macro generates one Rust test per mode. `assert_same` coordinates across
/// those test functions through shared state keyed by `case_id`.
pub async fn run_single_simulation_test<F, Fut>(
    mode: Engine2SimulationMode,
    options: Engine2SimulationOptions,
    case_id: &str,
    test_fn: F,
) where
    F: Fn(Engine2Simulation) -> Fut,
    Fut: Future<Output = ()>,
{
    let bootstrap = Engine2Bootstrap::create()
        .await
        .expect("engine2 simulation bootstrap should initialize");
    let expect_same = SharedExpectSameRun::new(case_id, mode);
    let _guard = SharedExpectSameRunGuard::new(expect_same.clone());
    let sim = Engine2Simulation::from_bootstrap(
        mode,
        options,
        bootstrap.snapshot,
        bootstrap.receipt,
        Engine2SimulationAssertions::shared(expect_same),
    )
    .await
    .expect("engine2 simulation mode should boot");
    test_fn(sim.clone()).await;
    sim.finish();
}

#[derive(Clone)]
struct Engine2Bootstrap {
    snapshot: KvMap,
    receipt: InitReceipt,
}

impl Engine2Bootstrap {
    async fn create() -> Result<Self, LixError> {
        let backend = InMemoryKvBackend::new();
        let receipt = Engine::initialize(Box::new(backend.clone())).await?;
        Ok(Self {
            snapshot: backend.snapshot(),
            receipt,
        })
    }
}

pub(crate) async fn enable_deterministic_mode(
    engine: &Engine,
    receipt: &InitReceipt,
    mode: Engine2SimulationMode,
) -> Result<(), LixError> {
    let timestamp_shuffle = deterministic_timestamp_shuffle_for(mode);
    let session = engine.open_session(receipt.main_version_id.clone()).await?;
    session
        .execute(&deterministic_mode_insert_sql(timestamp_shuffle), &[])
        .await?;
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
    fn deterministic_mode_sql_carries_timestamp_shuffle_flag() {
        assert!(deterministic_mode_insert_sql(true).contains("\"timestamp_shuffle\":true"));
        assert!(deterministic_mode_insert_sql(false).contains("\"timestamp_shuffle\":false"));
    }
}
