use crate::builtin_schema::{builtin_schema_definition, builtin_schema_keys};
use crate::cel::CelEvaluator;
use crate::deterministic_mode::{
    load_persisted_sequence_next, load_settings, persist_sequence_highest, DeterministicSettings,
    RuntimeFunctionProvider,
};
use crate::functions::SharedFunctionProvider;
use crate::init::init_backend;
use crate::key_value::{
    key_value_file_id, key_value_plugin_key, key_value_schema_key, key_value_schema_version,
    KEY_VALUE_GLOBAL_VERSION,
};
use crate::schema_registry::register_schema;
use crate::sql::{
    build_delete_followup_sql, build_update_followup_sql, preprocess_sql_with_provider,
    PostprocessPlan,
};
use crate::validation::{validate_inserts, validate_updates, SchemaCache};
use crate::{LixBackend, LixError, QueryResult, Value};
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicBool, Ordering};

const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";

#[derive(Debug, Clone)]
pub struct BootKeyValue {
    pub key: String,
    pub value: JsonValue,
    pub version_id: Option<String>,
}

pub struct BootArgs {
    pub backend: Box<dyn LixBackend + Send + Sync>,
    pub key_values: Vec<BootKeyValue>,
}

impl BootArgs {
    pub fn new(backend: Box<dyn LixBackend + Send + Sync>) -> Self {
        Self {
            backend,
            key_values: Vec::new(),
        }
    }
}

pub struct Engine {
    backend: Box<dyn LixBackend + Send + Sync>,
    cel_evaluator: CelEvaluator,
    schema_cache: SchemaCache,
    boot_key_values: Vec<BootKeyValue>,
    boot_deterministic_settings: Option<DeterministicSettings>,
    deterministic_boot_pending: AtomicBool,
}

pub fn boot(args: BootArgs) -> Engine {
    let boot_deterministic_settings = infer_boot_deterministic_settings(&args.key_values);
    let deterministic_boot_pending = boot_deterministic_settings.is_some();
    Engine {
        backend: args.backend,
        cel_evaluator: CelEvaluator::new(),
        schema_cache: SchemaCache::new(),
        boot_key_values: args.key_values,
        boot_deterministic_settings,
        deterministic_boot_pending: AtomicBool::new(deterministic_boot_pending),
    }
}

impl Engine {
    pub async fn init(&self) -> Result<(), LixError> {
        let clear_boot_pending = self.deterministic_boot_pending.load(Ordering::SeqCst);
        let result = async {
            init_backend(self.backend.as_ref()).await?;
            self.ensure_builtin_schemas_installed().await?;
            self.seed_boot_key_values().await
        }
        .await;

        if clear_boot_pending {
            self.deterministic_boot_pending
                .store(false, Ordering::SeqCst);
        }

        result
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let mut settings = load_settings(self.backend.as_ref()).await?;
        if self.deterministic_boot_pending.load(Ordering::SeqCst) {
            if let Some(boot_settings) = self.boot_deterministic_settings {
                settings = boot_settings;
            }
        }
        let sequence_start = if settings.enabled {
            load_persisted_sequence_next(self.backend.as_ref()).await?
        } else {
            0
        };
        let functions =
            SharedFunctionProvider::new(RuntimeFunctionProvider::new(settings, sequence_start));

        let output = preprocess_sql_with_provider(
            self.backend.as_ref(),
            &self.cel_evaluator,
            sql,
            params,
            functions.clone(),
        )
        .await?;
        if !output.mutations.is_empty() {
            validate_inserts(self.backend.as_ref(), &self.schema_cache, &output.mutations).await?;
        }
        if !output.update_validations.is_empty() {
            validate_updates(
                self.backend.as_ref(),
                &self.schema_cache,
                &output.update_validations,
            )
            .await?;
        }
        for registration in output.registrations {
            register_schema(self.backend.as_ref(), &registration.schema_key).await?;
        }
        let result = match output.postprocess {
            None => self.backend.execute(&output.sql, &output.params).await,
            Some(PostprocessPlan::VtableUpdate(plan)) => {
                let result = self.backend.execute(&output.sql, &output.params).await?;
                let mut followup_functions = functions.clone();
                let followup_sql =
                    build_update_followup_sql(&plan, &result.rows, &mut followup_functions)?;
                if !followup_sql.is_empty() {
                    self.backend.execute(&followup_sql, &[]).await?;
                }
                Ok(result)
            }
            Some(PostprocessPlan::VtableDelete(plan)) => {
                let result = self.backend.execute(&output.sql, &output.params).await?;
                let mut followup_functions = functions.clone();
                let followup_sql =
                    build_delete_followup_sql(&plan, &result.rows, &mut followup_functions)?;
                if !followup_sql.is_empty() {
                    self.backend.execute(&followup_sql, &[]).await?;
                }
                Ok(result)
            }
        }?;

        if settings.enabled {
            let sequence_end = functions.with_lock(|provider| provider.next_sequence());
            if sequence_end > sequence_start {
                persist_sequence_highest(self.backend.as_ref(), sequence_end - 1).await?;
            }
        }

        Ok(result)
    }

    async fn ensure_builtin_schemas_installed(&self) -> Result<(), LixError> {
        for schema_key in builtin_schema_keys() {
            let schema = builtin_schema_definition(schema_key).ok_or_else(|| LixError {
                message: format!("builtin schema '{schema_key}' is not available"),
            })?;
            let entity_id = builtin_schema_entity_id(schema)?;

            let existing = self
                .execute(
                    "SELECT 1 FROM lix_internal_state_vtable \
                     WHERE schema_key = 'lix_stored_schema' \
                       AND entity_id = $1 \
                       AND version_id = 'global' \
                       AND snapshot_content IS NOT NULL \
                     LIMIT 1",
                    &[Value::Text(entity_id.clone())],
                )
                .await?;
            if !existing.rows.is_empty() {
                continue;
            }

            let snapshot_content = serde_json::json!({
                "value": schema
            })
            .to_string();
            self.execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) \
                 VALUES ('lix_stored_schema', $1)",
                &[Value::Text(snapshot_content)],
            )
            .await?;
        }

        Ok(())
    }

    async fn seed_boot_key_values(&self) -> Result<(), LixError> {
        for key_value in &self.boot_key_values {
            let version_id = key_value
                .version_id
                .as_deref()
                .unwrap_or(KEY_VALUE_GLOBAL_VERSION);
            let snapshot_content = serde_json::json!({
                "key": key_value.key,
                "value": key_value.value,
            })
            .to_string();

            self.execute(
                "INSERT INTO lix_internal_state_vtable (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, 1)",
                &[
                    Value::Text(key_value.key.clone()),
                    Value::Text(key_value_schema_key().to_string()),
                    Value::Text(key_value_file_id().to_string()),
                    Value::Text(version_id.to_string()),
                    Value::Text(key_value_plugin_key().to_string()),
                    Value::Text(snapshot_content),
                    Value::Text(key_value_schema_version().to_string()),
                ],
            )
            .await?;
        }

        Ok(())
    }
}

fn builtin_schema_entity_id(schema: &JsonValue) -> Result<String, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            message: "builtin schema must define string x-lix-key".to_string(),
        })?;
    let schema_version = schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            message: "builtin schema must define string x-lix-version".to_string(),
        })?;

    Ok(format!("{schema_key}~{schema_version}"))
}

fn infer_boot_deterministic_settings(key_values: &[BootKeyValue]) -> Option<DeterministicSettings> {
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
        Some(DeterministicSettings {
            enabled,
            uuid_v7_enabled,
            timestamp_enabled,
        })
    })
}

fn loosely_true(value: &JsonValue) -> bool {
    match value {
        JsonValue::Bool(boolean) => *boolean,
        JsonValue::Number(number) => {
            number.as_i64() == Some(1) || number.as_u64() == Some(1) || number.as_f64() == Some(1.0)
        }
        JsonValue::String(text) => text == "1",
        _ => false,
    }
}

fn loosely_false(value: &JsonValue) -> bool {
    match value {
        JsonValue::Bool(boolean) => !boolean,
        JsonValue::Number(number) => {
            number.as_i64() == Some(0) || number.as_u64() == Some(0) || number.as_f64() == Some(0.0)
        }
        JsonValue::String(text) => text.is_empty() || text == "0",
        JsonValue::Array(values) => values.is_empty(),
        _ => false,
    }
}
