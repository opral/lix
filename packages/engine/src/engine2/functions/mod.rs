//! Engine2 runtime function boundary.
//!
//! Sessions prepare one function context per execution. SQL, providers, and
//! transaction staging receive only a function provider; deterministic mode is
//! resolved privately inside this module.

mod context;
mod deterministic;
mod provider;
mod state;
mod types;

pub(crate) use context::FunctionContext;
pub(crate) use deterministic::DeterministicFunctionProvider;
pub(crate) use provider::{
    FunctionProvider, FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
};
pub(crate) use types::{DeterministicMode, DeterministicSequence};
