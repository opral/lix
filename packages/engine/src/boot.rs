use crate::deterministic_mode::DeterministicSettings;
use crate::engine::Engine;
use crate::json_truthiness::{loosely_false, loosely_true};
use crate::key_value::KEY_VALUE_GLOBAL_VERSION;
use crate::{LixBackend, WasmRuntime};
use serde_json::Value as JsonValue;
use std::sync::Arc;

const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";

#[derive(Debug, Clone)]
pub struct BootKeyValue {
    pub key: String,
    pub value: JsonValue,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BootAccount {
    pub id: String,
    pub name: String,
}

pub struct BootArgs {
    pub backend: Box<dyn LixBackend + Send + Sync>,
    pub wasm_runtime: Arc<dyn WasmRuntime>,
    pub key_values: Vec<BootKeyValue>,
    pub active_account: Option<BootAccount>,
    pub access_to_internal: bool,
}

impl BootArgs {
    pub fn new(
        backend: Box<dyn LixBackend + Send + Sync>,
        wasm_runtime: Arc<dyn WasmRuntime>,
    ) -> Self {
        Self {
            backend,
            wasm_runtime,
            key_values: Vec::new(),
            active_account: None,
            access_to_internal: false,
        }
    }
}

pub fn boot(args: BootArgs) -> Engine {
    let boot_deterministic_settings = infer_boot_deterministic_settings(&args.key_values);
    Engine::from_boot_args(args, boot_deterministic_settings)
}

pub(crate) fn infer_boot_deterministic_settings(
    key_values: &[BootKeyValue],
) -> Option<DeterministicSettings> {
    key_values.iter().rev().find_map(|key_value| {
        if key_value.key != DETERMINISTIC_MODE_KEY {
            return None;
        }
        if key_value
            .version_id
            .as_deref()
            .is_some_and(|version| version != KEY_VALUE_GLOBAL_VERSION)
        {
            return None;
        }
        let object = key_value.value.as_object()?;
        let enabled = object.get("enabled").map(loosely_true).unwrap_or(false);
        if !enabled {
            return None;
        }
        let uuid_v7_enabled = !object.get("uuid_v7").map(loosely_false).unwrap_or(false);
        let timestamp_enabled = !object.get("timestamp").map(loosely_false).unwrap_or(false);
        let timestamp_shuffle_enabled = object
            .get("timestamp_shuffle")
            .map(loosely_true)
            .unwrap_or(false);
        Some(DeterministicSettings {
            enabled,
            uuid_v7_enabled,
            timestamp_enabled,
            timestamp_shuffle_enabled,
        })
    })
}
