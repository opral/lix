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
mod session;
mod telemetry;
#[cfg(target_family = "wasm")]
mod wasm;

mod component_runtime;

#[cfg(not(target_family = "wasm"))]
pub(crate) mod component_runtime_napi;
#[cfg(target_family = "wasm")]
pub(crate) mod component_runtime_wasm;

fn parse_durability(value: Option<&str>) -> Result<lix::Durability, lix::LixError> {
    match value {
        None | Some("durable") => Ok(lix::Durability::Durable),
        Some("buffered") => Ok(lix::Durability::Buffered),
        Some(_) => Err(lix::LixError::new(
            lix::LixError::CODE_INVALID_PARAM,
            "durability must be durable or buffered",
        )),
    }
}
