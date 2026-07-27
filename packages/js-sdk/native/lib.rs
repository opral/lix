#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(not(target_family = "wasm"))]
mod napi;
mod telemetry;
#[cfg(target_family = "wasm")]
mod wasm;
