#[cfg(not(target_family = "wasm"))]
mod napi;
mod telemetry;
#[cfg(target_family = "wasm")]
mod wasm;
