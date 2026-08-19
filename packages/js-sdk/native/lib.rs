#![recursion_limit = "256"]

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(target_family = "wasm")]
mod browser_storage;
#[cfg(target_family = "wasm")]
mod js_storage;
#[cfg(not(target_family = "wasm"))]
mod napi;
mod telemetry;
#[cfg(target_family = "wasm")]
mod wasm;
