use crate::deterministic_mode::{parse_deterministic_settings_value, DeterministicSettings};
use crate::engine::Engine;
use crate::key_value::KEY_VALUE_GLOBAL_VERSION;
use crate::{LixBackend, LixError, WasmRuntime};
use serde_json::Value as JsonValue;
use std::sync::Arc;

const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";

#[derive(Debug, Clone)]
pub struct BootKeyValue {
    pub key: String,
    pub value: JsonValue,
    pub version_id: Option<String>,
    pub untracked: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct BootAccount {
    pub id: String,
    pub name: String,
}

pub struct EngineConfig {
    pub backend: Box<dyn LixBackend + Send + Sync>,
    pub wasm_runtime: Arc<dyn WasmRuntime>,
    pub key_values: Vec<BootKeyValue>,
    pub active_account: Option<BootAccount>,
    pub access_to_internal: bool,
}

impl EngineConfig {
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

impl Engine {
    pub(crate) async fn open(config: EngineConfig) -> Result<Self, LixError> {
        let engine = boot(config);
        engine.open_existing().await?;
        Ok(engine)
    }

    pub(crate) async fn open_or_init(config: EngineConfig) -> Result<bool, LixError> {
        let engine = boot(config);
        let initialized = engine.initialize_if_needed().await?;
        Ok(initialized)
    }
}

#[doc(hidden)]
pub type BootArgs = EngineConfig;

#[doc(hidden)]
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
        let settings = parse_deterministic_settings_value(&key_value.value);
        settings.enabled.then_some(settings)
    })
}
