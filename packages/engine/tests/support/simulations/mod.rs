mod materialization;
mod postgres;
mod sqlite;
mod timestamp_shuffle;

pub use materialization::materialization_simulation;
pub use postgres::postgres_simulation;
pub use sqlite::sqlite_simulation;
pub use timestamp_shuffle::timestamp_shuffle_simulation;

use crate::support::simulation_test::Simulation;

pub fn default_simulations() -> Vec<Simulation> {
    vec![
        sqlite_simulation(),
        postgres_simulation(),
        materialization_simulation(),
        timestamp_shuffle_simulation(),
    ]
}
