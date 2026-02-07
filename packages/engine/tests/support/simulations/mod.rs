mod materialization;
mod postgres;
mod sqlite;

pub use materialization::materialization_simulation;
pub use postgres::postgres_simulation;
pub use sqlite::sqlite_simulation;

use crate::support::simulation_test::Simulation;

pub fn default_simulations() -> Vec<Simulation> {
    vec![
        sqlite_simulation(),
        postgres_simulation(),
        materialization_simulation(),
    ]
}
