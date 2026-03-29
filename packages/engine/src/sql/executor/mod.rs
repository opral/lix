//! Executor stage ownership.
//!
//! This stage owns preparation, compiled execution artifacts, shared execution
//! routing, and public-surface execution.

pub(crate) mod compiled;
pub(crate) mod contracts;
pub(crate) mod dependency_spec;
pub(crate) mod derive_effects;
pub(crate) mod derive_requirements;
pub(crate) mod execute_prepared;
pub(crate) mod execution_program;
pub(crate) mod intent;
pub(crate) mod preprocess;
pub(crate) mod public_runtime;
pub(crate) mod runtime_functions;
pub(crate) mod runtime_state;
pub(crate) mod shared_path;
