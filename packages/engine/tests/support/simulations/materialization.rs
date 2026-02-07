use crate::support::simulation_test::Simulation;

pub fn materialization_simulation() -> Simulation {
    let mut simulation = super::sqlite::sqlite_simulation();
    simulation.name = "materialization";
    simulation.behavior = crate::support::simulation_test::SimulationBehavior::Rematerialization;
    simulation
}
