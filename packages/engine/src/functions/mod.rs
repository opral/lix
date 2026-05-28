//! Engine runtime function boundary.
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
pub(crate) use provider::{FunctionProvider, FunctionProviderHandle, SystemFunctionProvider};
pub(crate) use types::{DeterministicMode, DeterministicSequence};

pub(crate) type DeterministicRuntimeGuard = tokio::sync::OwnedMutexGuard<()>;

pub(crate) async fn deterministic_mode_enabled(
    live_state: &dyn crate::live_state::LiveStateReader,
) -> Result<bool, crate::LixError> {
    Ok(state::load_mode(live_state).await?.enabled)
}
