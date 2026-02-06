#![allow(dead_code)]

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use lix_engine::{boot, BootArgs, BootKeyValue, Engine, LixBackend, LixError, QueryResult, Value};

use super::simulations::default_simulations as default_simulations_impl;

pub struct Simulation {
    pub name: &'static str,
    pub backend_factory: Box<dyn Fn() -> Box<dyn LixBackend + Send + Sync> + Send + Sync>,
    pub setup: Option<Arc<dyn Fn() -> BoxFuture<'static, Result<(), LixError>> + Send + Sync>>,
}

pub struct SimulationArgs {
    backend_factory: Box<dyn Fn() -> Box<dyn LixBackend + Send + Sync> + Send + Sync>,
    setup: Option<Arc<dyn Fn() -> BoxFuture<'static, Result<(), LixError>> + Send + Sync>>,
    expect: ExpectDeterministic,
}

#[derive(Default)]
pub struct SimulationBootArgs {
    pub key_values: Vec<BootKeyValue>,
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
    pub async fn boot_simulated_engine(
        &self,
        args: Option<SimulationBootArgs>,
    ) -> Result<SimulationEngine, LixError> {
        if let Some(setup) = &self.setup {
            setup().await?;
        }
        let args = args.unwrap_or_default();
        Ok(SimulationEngine {
            engine: boot(BootArgs {
                backend: (self.backend_factory)(),
                key_values: args.key_values,
            }),
        })
    }

    pub fn assert_deterministic<T>(&self, actual: T)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        self.expect.assert_deterministic(actual);
    }
}

#[derive(Clone)]
enum ExpectDeterministic {
    Local(LocalExpectDeterministic),
    Shared(SharedExpectDeterministic),
}

#[derive(Clone)]
struct LocalExpectDeterministic {
    inner: Arc<Mutex<LocalExpectDeterministicState>>,
}

struct LocalExpectDeterministicState {
    #[allow(dead_code)]
    expected_values: Vec<Box<dyn Any + Send + Sync>>,
    is_first: bool,
    call_index: usize,
}

impl LocalExpectDeterministic {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(LocalExpectDeterministicState {
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
    fn assert_deterministic<T>(&self, actual: T)
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

#[derive(Clone)]
struct SharedExpectDeterministic {
    run: Arc<SharedDeterministicRun>,
}

struct SharedDeterministicRun {
    backend_name: String,
    case_id: String,
    call_index: Mutex<usize>,
    state: Arc<SharedDeterministicCase>,
    is_baseline: bool,
}

struct SharedDeterministicCase {
    state: Mutex<SharedDeterministicCaseState>,
    condvar: Condvar,
}

struct SharedDeterministicCaseState {
    baseline_backend: Option<String>,
    baseline_finished: bool,
    baseline_failed: bool,
    baseline_call_count: Option<usize>,
    expected_values: Vec<Box<dyn Any + Send + Sync>>,
    role_by_backend: HashMap<String, bool>,
}

struct SharedDeterministicRunGuard {
    run: Arc<SharedDeterministicRun>,
}

impl SharedDeterministicRun {
    fn next_index(&self) -> usize {
        let mut idx = self
            .call_index
            .lock()
            .expect("deterministic run call_index mutex poisoned");
        let current = *idx;
        *idx += 1;
        current
    }

    fn call_count(&self) -> usize {
        *self
            .call_index
            .lock()
            .expect("deterministic run call_index mutex poisoned")
    }

    fn assert_deterministic<T>(&self, actual: T)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        let idx = self.next_index();
        if self.is_baseline {
            let mut state = self
                .state
                .state
                .lock()
                .expect("shared deterministic mutex poisoned");
            state.expected_values.push(Box::new(actual));
            self.state.condvar.notify_all();
            return;
        }

        let deadline = Instant::now() + Duration::from_secs(120);
        let mut state = self
            .state
            .state
            .lock()
            .expect("shared deterministic mutex poisoned");
        loop {
            if idx < state.expected_values.len() {
                break;
            }

            if state.baseline_finished {
                if state.baseline_failed {
                    panic!(
                        "SIMULATION DETERMINISM VIOLATION\n\nCase `{}` baseline backend `{}` failed; cannot compare call #{} for backend `{}`",
                        self.case_id,
                        state
                            .baseline_backend
                            .as_deref()
                            .unwrap_or("<unknown>"),
                        idx,
                        self.backend_name
                    );
                }
                panic!(
                    "SIMULATION DETERMINISM VIOLATION\n\nCase `{}` backend `{}` called assert_deterministic one extra time at call #{}",
                    self.case_id,
                    self.backend_name,
                    idx
                );
            }

            let now = Instant::now();
            if now >= deadline {
                panic!(
                    "SIMULATION DETERMINISM VIOLATION\n\nTimed out waiting for baseline values in case `{}` (backend `{}`, call #{})",
                    self.case_id,
                    self.backend_name,
                    idx
                );
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, _) = self
                .state
                .condvar
                .wait_timeout(state, remaining)
                .expect("shared deterministic condvar wait poisoned");
            state = next_state;
        }

        let expected_any = state
            .expected_values
            .get(idx)
            .expect("expected deterministic value missing");
        let expected = expected_any
            .downcast_ref::<T>()
            .expect("expect_deterministic type mismatch across simulations");
        if &actual != expected {
            panic!(
                "SIMULATION DETERMINISM VIOLATION\n\nCase `{}` call #{} differs for backend `{}`\nactual: {:?}\nexpected: {:?}",
                self.case_id,
                idx,
                self.backend_name,
                actual,
                expected
            );
        }
    }

    fn finish(&self, success: bool) -> Result<(), String> {
        if self.is_baseline {
            let mut state = self
                .state
                .state
                .lock()
                .expect("shared deterministic mutex poisoned");
            state.baseline_finished = true;
            state.baseline_failed = !success;
            state.baseline_call_count = Some(self.call_count());
            self.state.condvar.notify_all();
            return Ok(());
        }

        if !success {
            return Ok(());
        }

        let deadline = Instant::now() + Duration::from_secs(120);
        let mut state = self
            .state
            .state
            .lock()
            .expect("shared deterministic mutex poisoned");
        while !state.baseline_finished {
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "SIMULATION DETERMINISM VIOLATION\n\nTimed out waiting for baseline completion in case `{}` for backend `{}`",
                    self.case_id, self.backend_name
                ));
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, _) = self
                .state
                .condvar
                .wait_timeout(state, remaining)
                .expect("shared deterministic condvar wait poisoned");
            state = next_state;
        }

        if state.baseline_failed {
            return Err(format!(
                "SIMULATION DETERMINISM VIOLATION\n\nCase `{}` baseline backend `{}` failed; cannot validate backend `{}`",
                self.case_id,
                state
                    .baseline_backend
                    .as_deref()
                    .unwrap_or("<unknown>"),
                self.backend_name
            ));
        }

        let expected_calls = state
            .baseline_call_count
            .unwrap_or(state.expected_values.len());
        let actual_calls = self.call_count();
        if actual_calls != expected_calls {
            return Err(format!(
                "SIMULATION DETERMINISM VIOLATION\n\nCase `{}` backend `{}` called assert_deterministic {} times but baseline expected {}",
                self.case_id, self.backend_name, actual_calls, expected_calls
            ));
        }
        Ok(())
    }
}

impl Drop for SharedDeterministicRunGuard {
    fn drop(&mut self) {
        let success = !std::thread::panicking();
        if let Err(message) = self.run.finish(success) {
            if success {
                panic!("{message}");
            }
        }
    }
}

impl SharedExpectDeterministic {
    fn new(case_id: &str, backend_name: &str) -> (Self, SharedDeterministicRunGuard) {
        static REGISTRY: OnceLock<Mutex<HashMap<String, Arc<SharedDeterministicCase>>>> =
            OnceLock::new();
        let registry = REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));

        let case = {
            let mut lock = registry
                .lock()
                .expect("shared deterministic registry mutex poisoned");
            lock.entry(case_id.to_string())
                .or_insert_with(|| {
                    Arc::new(SharedDeterministicCase {
                        state: Mutex::new(SharedDeterministicCaseState {
                            baseline_backend: None,
                            baseline_finished: false,
                            baseline_failed: false,
                            baseline_call_count: None,
                            expected_values: Vec::new(),
                            role_by_backend: HashMap::new(),
                        }),
                        condvar: Condvar::new(),
                    })
                })
                .clone()
        };

        let is_baseline = {
            let mut state = case
                .state
                .lock()
                .expect("shared deterministic mutex poisoned");
            if let Some(existing) = state.role_by_backend.get(backend_name) {
                *existing
            } else if state.baseline_backend.is_none() {
                state.baseline_backend = Some(backend_name.to_string());
                state.role_by_backend.insert(backend_name.to_string(), true);
                true
            } else {
                state
                    .role_by_backend
                    .insert(backend_name.to_string(), false);
                false
            }
        };

        let run = Arc::new(SharedDeterministicRun {
            backend_name: backend_name.to_string(),
            case_id: case_id.to_string(),
            call_index: Mutex::new(0),
            state: case,
            is_baseline,
        });
        let guard = SharedDeterministicRunGuard { run: run.clone() };
        (Self { run }, guard)
    }

    fn assert_deterministic<T>(&self, actual: T)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        self.run.assert_deterministic(actual);
    }
}

impl ExpectDeterministic {
    fn new_local() -> Self {
        Self::Local(LocalExpectDeterministic::new())
    }

    fn new_shared(case_id: &str, backend_name: &str) -> (Self, SharedDeterministicRunGuard) {
        let (expect, guard) = SharedExpectDeterministic::new(case_id, backend_name);
        (Self::Shared(expect), guard)
    }

    fn start_simulation(&self, is_first: bool) {
        match self {
            Self::Local(local) => local.start_simulation(is_first),
            Self::Shared(_) => {
                panic!("start_simulation is only valid for local deterministic mode")
            }
        }
    }

    fn assert_deterministic<T>(&self, actual: T)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        match self {
            Self::Local(local) => local.assert_deterministic(actual),
            Self::Shared(shared) => shared.assert_deterministic(actual),
        }
    }
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub async fn run_simulation_test<F, Fut>(simulations: Vec<Simulation>, test_fn: F)
where
    F: Fn(SimulationArgs) -> Fut,
    Fut: Future<Output = ()>,
{
    let deterministic = ExpectDeterministic::new_local();

    for (index, simulation) in simulations.into_iter().enumerate() {
        deterministic.start_simulation(index == 0);
        let args = SimulationArgs {
            backend_factory: simulation.backend_factory,
            setup: simulation.setup,
            expect: deterministic.clone(),
        };
        Box::pin(test_fn(args)).await;
    }
}

pub async fn run_single_simulation_test<F, Fut>(simulation_name: &str, case_id: &str, test_fn: F)
where
    F: Fn(SimulationArgs) -> Fut,
    Fut: Future<Output = ()>,
{
    let simulation = default_simulations_impl()
        .into_iter()
        .find(|sim| sim.name == simulation_name)
        .unwrap_or_else(|| panic!("{} simulation missing", simulation_name));
    let (deterministic, _guard) = ExpectDeterministic::new_shared(case_id, simulation.name);
    let args = SimulationArgs {
        backend_factory: simulation.backend_factory,
        setup: simulation.setup,
        expect: deterministic,
    };
    Box::pin(test_fn(args)).await;
}

pub fn default_simulations() -> Vec<Simulation> {
    default_simulations_impl()
}
