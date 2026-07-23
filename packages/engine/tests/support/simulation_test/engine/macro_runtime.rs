use std::future::Future;
use std::sync::Arc;

use lix_engine::LixError;
use lix_engine::storage::Memory;
use lix_engine::{Engine, InitReceipt};

use super::expect_same::{
    SharedExpectSameCase, SharedExpectSameRun, SharedExpectSameRunGuard, SimulationAssertions,
};
use super::mode::{SimulationMode, SimulationOptions};
use super::rebuild_tracked_state::deterministic_timestamp_shuffle_for;
use super::simulation::Simulation;

/// Runs one generated test entry for `simulation_test!`.
///
/// Non-base modes first execute the base mode in the same process. This keeps
/// simulation comparisons compatible with runners like nextest that isolate
/// every test case in a separate process.
pub async fn run_simulation_test<F, Fut>(
    mode: SimulationMode,
    options: SimulationOptions,
    case_id: &str,
    test_fn: F,
) where
    F: Fn(Simulation) -> Fut,
    Fut: Future<Output = ()>,
{
    if mode == SimulationMode::Base {
        run_single_simulation_test(mode, options, case_id, test_fn).await;
        return;
    }

    let shared_case = SharedExpectSameCase::new();
    run_single_simulation_test_with_case(
        SimulationMode::Base,
        options,
        case_id,
        shared_case.clone(),
        &test_fn,
    )
    .await;
    run_single_simulation_test_with_case(mode, options, case_id, shared_case, &test_fn).await;
}

/// Runs one simulation mode without bootstrapping a base comparison first.
async fn run_single_simulation_test<F, Fut>(
    mode: SimulationMode,
    options: SimulationOptions,
    case_id: &str,
    test_fn: F,
) where
    F: Fn(Simulation) -> Fut,
    Fut: Future<Output = ()>,
{
    run_single_simulation_test_with_case(
        mode,
        options,
        case_id,
        SharedExpectSameCase::new(),
        &test_fn,
    )
    .await;
}

async fn run_single_simulation_test_with_case<F, Fut>(
    mode: SimulationMode,
    options: SimulationOptions,
    case_id: &str,
    shared_case: Arc<SharedExpectSameCase>,
    test_fn: &F,
) where
    F: Fn(Simulation) -> Fut,
    Fut: Future<Output = ()>,
{
    let bootstrap = Bootstrap::create()
        .await
        .expect("simulation bootstrap should initialize");
    let expect_same = SharedExpectSameRun::with_case(case_id, mode, shared_case);
    let _guard = SharedExpectSameRunGuard::new(expect_same.clone());
    let sim = Box::pin(Simulation::from_bootstrap(
        mode,
        options,
        bootstrap.storage,
        bootstrap.receipt,
        SimulationAssertions::shared(expect_same),
    ))
    .await
    .expect("simulation mode should boot");
    test_fn(sim.clone()).await;
    sim.finish();
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
    mode: SimulationMode,
) -> Result<(), LixError> {
    let timestamp_shuffle = deterministic_timestamp_shuffle_for(mode);
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
