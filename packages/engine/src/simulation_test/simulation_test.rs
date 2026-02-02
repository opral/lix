use std::any::Any;
use std::sync::{Arc, Mutex};

use crate::{open_lix, Lix, LixBackend, LixError, OpenLixConfig, SqliteBackend, SqliteConfig};

pub struct Simulation {
    pub name: &'static str,
    pub backend_factory: Box<dyn Fn() -> Box<dyn LixBackend + Send + Sync> + Send + Sync>,
}

pub struct SimulationArgs {
    pub name: &'static str,
    backend_factory: Box<dyn Fn() -> Box<dyn LixBackend + Send + Sync> + Send + Sync>,
    expect: ExpectDeterministic,
}

impl SimulationArgs {
    pub async fn open_simulated_lix(&self) -> Result<Lix, LixError> {
        open_lix(OpenLixConfig {
            backend: (self.backend_factory)(),
        })
        .await
    }

    pub fn expect_deterministic<T>(&self, actual: T)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        self.expect.expect_deterministic(actual);
    }

    pub fn expect<T, F>(&self, actual: T, diff: Option<F>)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
        F: FnOnce(&T, &T),
    {
        self.expect.expect(actual, diff);
    }
}

#[derive(Clone)]
struct ExpectDeterministic {
    inner: Arc<Mutex<ExpectDeterministicState>>,
}

struct ExpectDeterministicState {
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

    fn expect<T, F>(&self, actual: T, diff: Option<F>)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
        F: FnOnce(&T, &T),
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
            if let Some(diff_fn) = diff {
                diff_fn(&actual, expected);
            }
            panic!(
				"SIMULATION DETERMINISM VIOLATION\n\nCall #{}: values differ across simulations\nactual: {:?}\nexpected: {:?}",
				idx,
				actual,
				expected
			);
        }
    }

    fn expect_deterministic<T>(&self, actual: T)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        self.expect(actual, Option::<fn(&T, &T)>::None);
    }
}

pub fn default_simulations() -> Vec<Simulation> {
    vec![Simulation {
        name: "sqlite",
        backend_factory: Box::new(|| {
            Box::new(SqliteBackend::new(SqliteConfig {
                filename: ":memory:".to_string(),
            })) as Box<dyn LixBackend + Send + Sync>
        }),
    }]
}

pub async fn simulation_test<F, Fut>(simulations: Vec<Simulation>, test_fn: F)
where
    F: Fn(SimulationArgs) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let deterministic = ExpectDeterministic::new();

    for (index, simulation) in simulations.into_iter().enumerate() {
        deterministic.start_simulation(index == 0);
        let args = SimulationArgs {
            name: simulation.name,
            backend_factory: simulation.backend_factory,
            expect: deterministic.clone(),
        };
        test_fn(args).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{simulation_test, Simulation};
    use crate::{LixBackend, QueryResult, Value};
    use async_trait::async_trait;

    struct StaticBackend {
        value: i64,
    }

    #[async_trait]
    impl LixBackend for StaticBackend {
        async fn execute(
            &self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<QueryResult, crate::LixError> {
            Ok(QueryResult {
                rows: vec![vec![Value::Integer(self.value)]],
            })
        }
    }

    fn simulation_with_value(name: &'static str, value: i64) -> Simulation {
        Simulation {
            name,
            backend_factory: Box::new(move || {
                Box::new(StaticBackend { value }) as Box<dyn LixBackend + Send + Sync>
            }),
        }
    }

    #[tokio::test]
    async fn expect_deterministic_passes_with_same_values() {
        simulation_test(
            vec![
                simulation_with_value("sim-a", 1),
                simulation_with_value("sim-b", 1),
            ],
            |sim| async move {
                let lix = sim
                    .open_simulated_lix()
                    .await
                    .expect("open_lix should succeed");
                let result = lix.execute("SELECT 1", &[]).await.unwrap();
                sim.expect_deterministic(result.rows.clone());
            },
        )
        .await;
    }

    #[tokio::test]
    #[should_panic]
    async fn expect_deterministic_fails_on_mismatch() {
        simulation_test(
            vec![
                simulation_with_value("sim-a", 1),
                simulation_with_value("sim-b", 2),
            ],
            |sim| async move {
                let lix = sim
                    .open_simulated_lix()
                    .await
                    .expect("open_lix should succeed");
                let result = lix.execute("SELECT 1", &[]).await.unwrap();
                sim.expect_deterministic(result.rows.clone());
            },
        )
        .await;
    }
}
