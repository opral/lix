use std::future::Future;

use crate::LixError;

/// Runs engine-owned background work without borrowing the embedding
/// application's async runtime.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) fn spawn<Factory, F>(name: &str, factory: Factory) -> Result<(), LixError>
where
    Factory: FnOnce() -> F + Send + 'static,
    F: Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || futures_lite::future::block_on(factory()))
        .map(|_| ())
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("start {name}: {error}"),
            )
        })
}

/// Browser Wasm has no threads; its embedding worker owns a local task queue.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) fn spawn<Factory, F>(_name: &str, factory: Factory) -> Result<(), LixError>
where
    Factory: FnOnce() -> F + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    wasm_bindgen_futures::spawn_local(factory());
    Ok(())
}
