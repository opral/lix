//! Optimizer stage ownership.
//!
//! Named optimizer passes and their registries live here so heuristics stop
//! hiding inside runtime or lowering modules.

mod public_reads;
mod registry;
mod state_resolution;

pub(crate) use public_reads::{
    choose_specialized_public_read_strategy,
    optimize_broad_public_read_statement_with_known_live_layouts,
};
pub(crate) use registry::OptimizerPassTrace;
pub(crate) use state_resolution::optimize_state_resolution;
