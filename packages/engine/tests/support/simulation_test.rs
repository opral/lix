use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use lix_engine::{boot, Engine, LixBackend, LixError, QueryResult, Value};

use super::simulations::default_simulations as default_simulations_impl;

pub struct Simulation {
    pub name: &'static str,
    pub backend_factory: Box<dyn Fn() -> Box<dyn LixBackend + Send + Sync> + Send + Sync>,
    pub setup: Option<Arc<dyn Fn() -> BoxFuture<'static, Result<(), LixError>> + Send + Sync>>,
}

pub struct SimulationArgs {
    backend_factory: Box<dyn Fn() -> Box<dyn LixBackend + Send + Sync> + Send + Sync>,
    setup: Option<Arc<dyn Fn() -> BoxFuture<'static, Result<(), LixError>> + Send + Sync>>,
    #[allow(dead_code)]
    expect: ExpectDeterministic,
}

pub struct SimulationEngine {
    engine: Engine,
}

impl SimulationEngine {
    #[allow(dead_code)]
    pub async fn init(&self) -> Result<(), LixError> {
        self.engine.init().await
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.engine.execute(sql, params).await
    }
}

impl SimulationArgs {
    pub async fn boot_simulated_engine(&self) -> Result<SimulationEngine, LixError> {
        if let Some(setup) = &self.setup {
            setup().await?;
        }
        Ok(SimulationEngine {
            engine: boot((self.backend_factory)()),
        })
    }

    #[allow(dead_code)]
    pub fn expect_deterministic<T>(&self, actual: T)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        self.expect.expect_deterministic(actual);
    }
}

#[derive(Clone)]
struct ExpectDeterministic {
    inner: Arc<Mutex<ExpectDeterministicState>>,
}

struct ExpectDeterministicState {
    #[allow(dead_code)]
    expected_values: Vec<Box<dyn Any + Send + Sync>>,
    is_first: bool,
    call_index: usize,
}

impl ExpectDeterministic {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ExpectDeterministicState {
                expected_values: Vec::new(),
                is_first: true,
                call_index: 0,
            })),
        }
    }

    fn start_simulation(&self, is_first: bool) {
        let mut state = self
            .inner
            .lock()
            .expect("expect_deterministic mutex poisoned");
        state.is_first = is_first;
        state.call_index = 0;
    }

    #[allow(dead_code)]
    fn expect_deterministic<T>(&self, actual: T)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        let mut state = self
            .inner
            .lock()
            .expect("expect_deterministic mutex poisoned");
        let idx = state.call_index;
        state.call_index += 1;

        if state.is_first {
            state.expected_values.push(Box::new(actual.clone()));
            return;
        }

        let expected_any = state
            .expected_values
            .get(idx)
            .expect("expect_deterministic called more times than in baseline");
        let expected = expected_any
            .downcast_ref::<T>()
            .expect("expect_deterministic type mismatch across simulations");

        if &actual != expected {
            panic!(
				"SIMULATION DETERMINISM VIOLATION\n\nCall #{}: values differ across simulations\nactual: {:?}\nexpected: {:?}",
				idx,
				actual,
				expected
			);
        }
    }
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub async fn run_simulation_test<F, Fut>(simulations: Vec<Simulation>, test_fn: F)
where
    F: Fn(SimulationArgs) -> Fut,
    Fut: Future<Output = ()>,
{
    let deterministic = ExpectDeterministic::new();

    for (index, simulation) in simulations.into_iter().enumerate() {
        deterministic.start_simulation(index == 0);
        let args = SimulationArgs {
            backend_factory: simulation.backend_factory,
            setup: simulation.setup,
            expect: deterministic.clone(),
        };
        test_fn(args).await;
    }
}

pub fn default_simulations() -> Vec<Simulation> {
    default_simulations_impl()
}
