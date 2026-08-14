use std::future::Future;

use crate::LixError;

/// Runs engine-owned background work without borrowing the embedding
/// application's async runtime.
///
/// The temporary stack size preserves the existing execution envelope while
/// the separate default-stack hard cut removes deep poll stacks. Runtime
/// neutrality must not silently become a stack-size behavior change.
const BACKGROUND_TASK_STACK_BYTES: usize = 16 * 1024 * 1024;

pub(crate) fn spawn<Factory, F>(name: &str, factory: Factory) -> Result<(), LixError>
where
    Factory: FnOnce() -> F + Send + 'static,
    F: Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name(name.to_owned())
        .stack_size(BACKGROUND_TASK_STACK_BYTES)
        .spawn(move || futures_lite::future::block_on(factory()))
        .map(|_| ())
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("start {name}: {error}"),
            )
        })
}
