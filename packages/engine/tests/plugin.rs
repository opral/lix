#[macro_use]
#[path = "support/mod.rs"]
mod support;
#[cfg(not(target_arch = "wasm32"))]
#[path = "support/wasmtime_runtime.rs"]
mod wasmtime_runtime;

#[cfg(not(target_arch = "wasm32"))]
#[path = "plugin/detect_changes.rs"]
mod detect_changes;
#[path = "plugin/fixture.rs"]
mod fixture;
#[path = "plugin/list_plugins.rs"]
mod list_plugins;
#[path = "plugin/register_plugin.rs"]
mod register_plugin;
