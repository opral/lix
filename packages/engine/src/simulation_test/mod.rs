mod postgres;
mod simulation_test;

pub use postgres::postgres_simulation;
pub use simulation_test::{default_simulations, run_simulation_test, Simulation, SimulationArgs};

#[macro_export]
macro_rules! simulation_test {
    ($name:ident, |$sim:ident| $body:expr) => {
        paste::paste! {
            #[tokio::test]
            async fn [<$name _sqlite>]() {
                let $sim = $crate::simulation_test::default_simulations()
                    .into_iter()
                    .find(|sim| sim.name == "sqlite")
                    .expect("sqlite simulation missing");
                $crate::simulation_test::run_simulation_test(vec![$sim], |$sim| $body).await;
            }

            #[tokio::test]
            async fn [<$name _postgres>]() {
                let $sim = $crate::simulation_test::default_simulations()
                    .into_iter()
                    .find(|sim| sim.name == "postgres")
                    .expect("postgres simulation missing");
                $crate::simulation_test::run_simulation_test(vec![$sim], |$sim| $body).await;
            }
        }
    };
}
