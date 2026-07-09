#[cfg(not(target_family = "wasm"))]
mod js_wasm_runtime;
#[cfg(not(target_family = "wasm"))]
mod napi;
#[cfg(target_family = "wasm")]
mod wasm;
