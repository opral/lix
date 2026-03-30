//! Optimizer stage ownership.
//!
//! Named optimizer passes and their registries live here so heuristics stop
//! hiding inside runtime or lowering modules.

mod registry;
mod state_resolution;

pub(crate) use state_resolution::optimize_state_resolution;
