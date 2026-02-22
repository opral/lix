use crate::support::simulation_test::Simulation;

pub fn timestamp_shuffle_simulation() -> Simulation {
    let mut simulation = super::sqlite::sqlite_simulation();
    simulation.name = "timestamp_shuffle";
    simulation.behavior = crate::support::simulation_test::SimulationBehavior::TimestampShuffle;
    simulation
}
