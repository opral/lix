#[cfg(not(target_family = "wasm"))]
mod js_wasm_runtime;
#[cfg(not(target_family = "wasm"))]
mod napi;
mod telemetry;
#[cfg(target_family = "wasm")]
mod wasm;
