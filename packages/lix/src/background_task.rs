use std::future::Future;

use crate::LixError;

/// Runs engine-owned background work without borrowing the embedding
/// application's async runtime.
///
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
