use crate::account::{
    account_file_id, account_plugin_key, account_schema_key, account_schema_version,
    account_snapshot_content, account_storage_version_id, active_account_file_id,
    active_account_plugin_key, active_account_schema_key, active_account_schema_version,
    active_account_snapshot_content, active_account_storage_version_id,
};
use crate::builtin_schema::types::LixVersionDescriptor;
use crate::builtin_schema::{builtin_schema_definition, builtin_schema_keys};
use crate::cel::CelEvaluator;
use crate::deterministic_mode::{
    load_persisted_sequence_next, load_settings, persist_sequence_highest, DeterministicSettings,
    RuntimeFunctionProvider,
};
use crate::functions::SharedFunctionProvider;
use crate::init::init_backend;
use crate::json_truthiness::{loosely_false, loosely_true};
use crate::key_value::{
    key_value_file_id, key_value_plugin_key, key_value_schema_key, key_value_schema_version,
    KEY_VALUE_GLOBAL_VERSION,
};
use crate::materialization::{
    MaterializationApplyReport, MaterializationPlan, MaterializationReport, MaterializationRequest,
};
use crate::plugin::manifest::parse_plugin_manifest_json;
use crate::plugin::types::PluginManifest;
use crate::schema_registry::register_schema;
use crate::sql::{
    build_delete_followup_sql, build_update_followup_sql, escape_sql_string, parse_sql_statements,
    preprocess_sql_with_provider, resolve_values_rows, MutationRow, PostprocessPlan,
    UpdateValidationPlan,
};
use crate::validation::{validate_inserts, validate_updates, SchemaCache};
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id, parse_active_version_snapshot, version_descriptor_file_id,
    version_descriptor_plugin_key, version_descriptor_schema_key,
    version_descriptor_schema_version, version_descriptor_snapshot_content,
    version_descriptor_storage_version_id, version_pointer_file_id, version_pointer_plugin_key,
    version_pointer_schema_key, version_pointer_schema_version, version_pointer_snapshot_content,
    version_pointer_storage_version_id, DEFAULT_ACTIVE_VERSION_NAME, GLOBAL_VERSION_ID,
};
use crate::WasmRuntime;
use crate::{LixBackend, LixError, QueryResult, Value};
use serde_json::Value as JsonValue;
use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement, TableObject};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileWriteTarget {
    ActiveVersion,
    ExplicitVersion,
}

#[derive(Debug, Clone)]
struct PendingFileWrite {
    file_id: String,
    version_id: String,
    path: String,
    before_data: Option<Vec<u8>>,
    after_data: Vec<u8>,
}

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
    pub wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    pub key_values: Vec<BootKeyValue>,
    pub active_account: Option<BootAccount>,
}

impl BootArgs {
    pub fn new(backend: Box<dyn LixBackend + Send + Sync>) -> Self {
        Self {
            backend,
            wasm_runtime: None,
            key_values: Vec::new(),
            active_account: None,
        }
    }
}

pub struct Engine {
    backend: Box<dyn LixBackend + Send + Sync>,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    cel_evaluator: CelEvaluator,
    schema_cache: SchemaCache,
    boot_key_values: Vec<BootKeyValue>,
    boot_active_account: Option<BootAccount>,
    boot_deterministic_settings: Option<DeterministicSettings>,
    deterministic_boot_pending: AtomicBool,
    active_version_id: RwLock<String>,
}

pub fn boot(args: BootArgs) -> Engine {
    let boot_deterministic_settings = infer_boot_deterministic_settings(&args.key_values);
    let deterministic_boot_pending = boot_deterministic_settings.is_some();
    Engine {
        backend: args.backend,
        wasm_runtime: args.wasm_runtime,
        cel_evaluator: CelEvaluator::new(),
        schema_cache: SchemaCache::new(),
        boot_key_values: args.key_values,
        boot_active_account: args.active_account,
        boot_deterministic_settings,
        deterministic_boot_pending: AtomicBool::new(deterministic_boot_pending),
        active_version_id: RwLock::new(GLOBAL_VERSION_ID.to_string()),
    }
}

impl Engine {
    pub fn wasm_runtime(&self) -> Option<Arc<dyn WasmRuntime>> {
        self.wasm_runtime.clone()
    }

    pub async fn init(&self) -> Result<(), LixError> {
        let clear_boot_pending = self.deterministic_boot_pending.load(Ordering::SeqCst);
        let result = async {
            init_backend(self.backend.as_ref()).await?;
            self.ensure_builtin_schemas_installed().await?;
            let default_active_version_id = self.seed_default_versions().await?;
            self.seed_default_active_version(&default_active_version_id)
                .await?;
            self.seed_boot_key_values().await?;
            self.seed_boot_account().await?;
            self.load_and_cache_active_version().await
        }
        .await;

        if clear_boot_pending && result.is_ok() {
            self.deterministic_boot_pending
                .store(false, Ordering::SeqCst);
        }

        result
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let pending_file_writes = if self.wasm_runtime.is_some() {
            self.collect_pending_file_writes(sql, params)
                .unwrap_or_default()
        } else {
            Vec::new()
        };

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
        let next_active_version_id_from_mutations =
            active_version_from_mutations(&output.mutations)?;
        let next_active_version_id_from_updates =
            active_version_from_update_validations(&output.update_validations)?;
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
                let followup_sql = build_update_followup_sql(
                    self.backend.as_ref(),
                    &plan,
                    &result.rows,
                    &mut followup_functions,
                )
                .await?;
                if !followup_sql.is_empty() {
                    self.backend.execute(&followup_sql, &[]).await?;
                }
                Ok(result)
            }
            Some(PostprocessPlan::VtableDelete(plan)) => {
                let result = self.backend.execute(&output.sql, &output.params).await?;
                let mut followup_functions = functions.clone();
                let followup_sql = build_delete_followup_sql(
                    self.backend.as_ref(),
                    &plan,
                    &result.rows,
                    &mut followup_functions,
                )
                .await?;
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

        if let Some(version_id) =
            next_active_version_id_from_mutations.or(next_active_version_id_from_updates)
        {
            self.set_active_version_id(version_id);
        }

        if let Some(runtime) = self.wasm_runtime.as_ref() {
            if !pending_file_writes.is_empty() {
                let requests = pending_file_writes
                    .into_iter()
                    .map(|write| crate::plugin::runtime::FileChangeDetectionRequest {
                        file_id: write.file_id,
                        version_id: write.version_id,
                        path: write.path,
                        before_data: write.before_data,
                        after_data: write.after_data,
                    })
                    .collect::<Vec<_>>();

                let detected = crate::plugin::runtime::detect_file_changes_with_plugins(
                    self.backend.as_ref(),
                    runtime.as_ref(),
                    &requests,
                )
                .await?;
                self.persist_detected_file_changes(&detected).await?;
            }
        }

        Ok(result)
    }

    pub async fn install_plugin(
        &self,
        manifest_json: &str,
        wasm_bytes: &[u8],
    ) -> Result<(), LixError> {
        let validated = parse_plugin_manifest_json(manifest_json)?;
        ensure_valid_wasm_binary(wasm_bytes)?;
        let now = crate::functions::timestamp::timestamp();
        upsert_plugin_record(
            self.backend.as_ref(),
            &validated.manifest,
            &validated.normalized_json,
            wasm_bytes,
            &now,
        )
        .await
    }

    pub async fn materialization_plan(
        &self,
        req: &MaterializationRequest,
    ) -> Result<MaterializationPlan, LixError> {
        crate::materialization::materialization_plan(self.backend.as_ref(), req).await
    }

    pub async fn apply_materialization_plan(
        &self,
        plan: &MaterializationPlan,
    ) -> Result<MaterializationApplyReport, LixError> {
        crate::materialization::apply_materialization_plan(self.backend.as_ref(), plan).await
    }

    pub async fn materialize(
        &self,
        req: &MaterializationRequest,
    ) -> Result<MaterializationReport, LixError> {
        let plan = crate::materialization::materialization_plan(self.backend.as_ref(), req).await?;
        let apply =
            crate::materialization::apply_materialization_plan(self.backend.as_ref(), &plan)
                .await?;

        if let Some(runtime) = self.wasm_runtime.as_ref() {
            crate::plugin::runtime::materialize_file_data_with_plugins(
                self.backend.as_ref(),
                runtime.as_ref(),
                &plan,
            )
            .await?;
        }

        Ok(MaterializationReport { plan, apply })
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

    async fn seed_default_versions(&self) -> Result<String, LixError> {
        self.seed_materialized_version_descriptor(GLOBAL_VERSION_ID, GLOBAL_VERSION_ID, None)
            .await?;
        let main_version_id = match self
            .find_version_id_by_name(DEFAULT_ACTIVE_VERSION_NAME)
            .await?
        {
            Some(version_id) => version_id,
            None => {
                let generated_main_id = self.generate_runtime_uuid().await?;
                self.seed_materialized_version_descriptor(
                    &generated_main_id,
                    DEFAULT_ACTIVE_VERSION_NAME,
                    Some(GLOBAL_VERSION_ID),
                )
                .await?;
                generated_main_id
            }
        };

        let bootstrap_commit_id = self
            .load_latest_commit_id()
            .await?
            .unwrap_or_else(|| GLOBAL_VERSION_ID.to_string());
        self.seed_materialized_version_pointer(GLOBAL_VERSION_ID, &bootstrap_commit_id)
            .await?;
        self.seed_materialized_version_pointer(&main_version_id, &bootstrap_commit_id)
            .await?;

        Ok(main_version_id)
    }

    async fn seed_boot_account(&self) -> Result<(), LixError> {
        let Some(account) = &self.boot_active_account else {
            return Ok(());
        };

        let exists = self
            .execute(
                "SELECT 1 \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = $1 \
                   AND entity_id = $2 \
                   AND file_id = $3 \
                   AND version_id = $4 \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[
                    Value::Text(account_schema_key().to_string()),
                    Value::Text(account.id.clone()),
                    Value::Text(account_file_id().to_string()),
                    Value::Text(account_storage_version_id().to_string()),
                ],
            )
            .await?;
        if exists.rows.is_empty() {
            self.execute(
                "INSERT INTO lix_internal_state_vtable (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    Value::Text(account.id.clone()),
                    Value::Text(account_schema_key().to_string()),
                    Value::Text(account_file_id().to_string()),
                    Value::Text(account_storage_version_id().to_string()),
                    Value::Text(account_plugin_key().to_string()),
                    Value::Text(account_snapshot_content(&account.id, &account.name)),
                    Value::Text(account_schema_version().to_string()),
                ],
            )
            .await?;
        }

        self.execute(
            "DELETE FROM lix_internal_state_vtable \
             WHERE untracked = 1 \
               AND schema_key = $1 \
               AND file_id = $2 \
               AND version_id = $3",
            &[
                Value::Text(active_account_schema_key().to_string()),
                Value::Text(active_account_file_id().to_string()),
                Value::Text(active_account_storage_version_id().to_string()),
            ],
        )
        .await?;

        self.execute(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, 1)",
            &[
                Value::Text(account.id.clone()),
                Value::Text(active_account_schema_key().to_string()),
                Value::Text(active_account_file_id().to_string()),
                Value::Text(active_account_storage_version_id().to_string()),
                Value::Text(active_account_plugin_key().to_string()),
                Value::Text(active_account_snapshot_content(&account.id)),
                Value::Text(active_account_schema_version().to_string()),
            ],
        )
        .await?;

        Ok(())
    }

    async fn seed_materialized_version_descriptor(
        &self,
        entity_id: &str,
        name: &str,
        inherits_from_version_id: Option<&str>,
    ) -> Result<(), LixError> {
        let table = format!(
            "lix_internal_state_materialized_v1_{}",
            version_descriptor_schema_key()
        );
        let check_sql = format!(
            "SELECT 1 FROM {table} \
             WHERE schema_key = '{schema_key}' \
               AND entity_id = '{entity_id}' \
               AND file_id = '{file_id}' \
               AND version_id = '{version_id}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
             LIMIT 1",
            table = table,
            schema_key = escape_sql_string(version_descriptor_schema_key()),
            entity_id = escape_sql_string(entity_id),
            file_id = escape_sql_string(version_descriptor_file_id()),
            version_id = escape_sql_string(version_descriptor_storage_version_id()),
        );
        let existing = self.backend.execute(&check_sql, &[]).await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let snapshot_content =
            version_descriptor_snapshot_content(entity_id, name, inherits_from_version_id, false);
        let insert_sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content, \
             change_id, is_tombstone, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', '{plugin_key}', '{snapshot_content}', \
             'bootstrap', 0, '1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z'\
             ) \
             ON CONFLICT (entity_id, file_id, version_id) DO NOTHING",
            table = table,
            entity_id = escape_sql_string(entity_id),
            schema_key = escape_sql_string(version_descriptor_schema_key()),
            schema_version = escape_sql_string(version_descriptor_schema_version()),
            file_id = escape_sql_string(version_descriptor_file_id()),
            version_id = escape_sql_string(version_descriptor_storage_version_id()),
            plugin_key = escape_sql_string(version_descriptor_plugin_key()),
            snapshot_content = escape_sql_string(&snapshot_content),
        );
        self.backend.execute(&insert_sql, &[]).await?;

        Ok(())
    }

    async fn find_version_id_by_name(&self, name: &str) -> Result<Option<String>, LixError> {
        let table = format!(
            "lix_internal_state_materialized_v1_{}",
            version_descriptor_schema_key()
        );
        let sql = format!(
            "SELECT entity_id, snapshot_content \
             FROM {table} \
             WHERE schema_key = '{schema_key}' \
               AND file_id = '{file_id}' \
               AND version_id = '{version_id}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL",
            table = table,
            schema_key = escape_sql_string(version_descriptor_schema_key()),
            file_id = escape_sql_string(version_descriptor_file_id()),
            version_id = escape_sql_string(version_descriptor_storage_version_id()),
        );
        let result = self.backend.execute(&sql, &[]).await?;

        for row in result.rows {
            if row.len() < 2 {
                continue;
            }
            let entity_id = match &row[0] {
                Value::Text(value) => value,
                _ => continue,
            };
            let snapshot_content = match &row[1] {
                Value::Text(value) => value,
                _ => continue,
            };
            let snapshot: LixVersionDescriptor =
                serde_json::from_str(snapshot_content).map_err(|error| LixError {
                    message: format!("version descriptor snapshot_content invalid JSON: {error}"),
                })?;
            let Some(snapshot_name) = snapshot.name.as_deref() else {
                continue;
            };
            if snapshot_name != name {
                continue;
            }
            let snapshot_id = if snapshot.id.is_empty() {
                entity_id.as_str()
            } else {
                snapshot.id.as_str()
            };
            return Ok(Some(snapshot_id.to_string()));
        }

        Ok(None)
    }

    async fn seed_materialized_version_pointer(
        &self,
        entity_id: &str,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let table = format!(
            "lix_internal_state_materialized_v1_{}",
            version_pointer_schema_key()
        );
        let check_sql = format!(
            "SELECT 1 FROM {table} \
             WHERE schema_key = '{schema_key}' \
               AND entity_id = '{entity_id}' \
               AND file_id = '{file_id}' \
               AND version_id = '{version_id}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
             LIMIT 1",
            table = table,
            schema_key = escape_sql_string(version_pointer_schema_key()),
            entity_id = escape_sql_string(entity_id),
            file_id = escape_sql_string(version_pointer_file_id()),
            version_id = escape_sql_string(version_pointer_storage_version_id()),
        );
        let existing = self.backend.execute(&check_sql, &[]).await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let snapshot_content = version_pointer_snapshot_content(entity_id, commit_id, commit_id);
        let insert_sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content, \
             change_id, is_tombstone, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', '{plugin_key}', '{snapshot_content}', \
             'bootstrap', 0, '1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z'\
             ) \
             ON CONFLICT (entity_id, file_id, version_id) DO NOTHING",
            table = table,
            entity_id = escape_sql_string(entity_id),
            schema_key = escape_sql_string(version_pointer_schema_key()),
            schema_version = escape_sql_string(version_pointer_schema_version()),
            file_id = escape_sql_string(version_pointer_file_id()),
            version_id = escape_sql_string(version_pointer_storage_version_id()),
            plugin_key = escape_sql_string(version_pointer_plugin_key()),
            snapshot_content = escape_sql_string(&snapshot_content),
        );
        self.backend.execute(&insert_sql, &[]).await?;

        Ok(())
    }

    async fn seed_default_active_version(&self, version_id: &str) -> Result<(), LixError> {
        let check_sql = format!(
            "SELECT 1 \
             FROM lix_internal_state_untracked \
             WHERE schema_key = '{schema_key}' \
               AND file_id = '{file_id}' \
               AND version_id = '{storage_version_id}' \
               AND snapshot_content IS NOT NULL \
             LIMIT 1",
            schema_key = escape_sql_string(active_version_schema_key()),
            file_id = escape_sql_string(active_version_file_id()),
            storage_version_id = escape_sql_string(active_version_storage_version_id()),
        );
        let existing = self.backend.execute(&check_sql, &[]).await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let entity_id = self.generate_runtime_uuid().await?;
        let snapshot_content = active_version_snapshot_content(&entity_id, version_id);
        let insert_sql = format!(
            "INSERT INTO lix_internal_state_untracked (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{file_id}', '{storage_version_id}', '{plugin_key}', '{snapshot_content}', '{schema_version}', '1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z'\
             )",
            entity_id = escape_sql_string(&entity_id),
            schema_key = escape_sql_string(active_version_schema_key()),
            file_id = escape_sql_string(active_version_file_id()),
            storage_version_id = escape_sql_string(active_version_storage_version_id()),
            plugin_key = escape_sql_string(active_version_plugin_key()),
            snapshot_content = escape_sql_string(&snapshot_content),
            schema_version = escape_sql_string(active_version_schema_version()),
        );
        self.backend.execute(&insert_sql, &[]).await?;
        Ok(())
    }

    async fn load_latest_commit_id(&self) -> Result<Option<String>, LixError> {
        let result = self
            .backend
            .execute(
                "SELECT entity_id \
                 FROM lix_internal_change \
                 WHERE schema_key = 'lix_commit' \
                 ORDER BY created_at DESC, id DESC \
                 LIMIT 1",
                &[],
            )
            .await?;
        let Some(row) = result.rows.first() else {
            return Ok(None);
        };
        let Some(value) = row.first() else {
            return Ok(None);
        };
        match value {
            Value::Text(value) if !value.is_empty() => Ok(Some(value.clone())),
            _ => Ok(None),
        }
    }

    async fn generate_runtime_uuid(&self) -> Result<String, LixError> {
        let result = self.execute("SELECT lix_uuid_v7()", &[]).await?;
        let row = result.rows.first().ok_or_else(|| LixError {
            message: "lix_uuid_v7 query returned no rows".to_string(),
        })?;
        let value = row.first().ok_or_else(|| LixError {
            message: "lix_uuid_v7 query returned no columns".to_string(),
        })?;
        match value {
            Value::Text(text) => Ok(text.clone()),
            other => Err(LixError {
                message: format!("lix_uuid_v7 query returned non-text value: {other:?}"),
            }),
        }
    }

    async fn load_and_cache_active_version(&self) -> Result<(), LixError> {
        let result = self
            .backend
            .execute(
                "SELECT snapshot_content \
                 FROM lix_internal_state_untracked \
                 WHERE schema_key = $1 \
                   AND file_id = $2 \
                   AND version_id = $3 \
                   AND snapshot_content IS NOT NULL \
                 ORDER BY updated_at DESC \
                 LIMIT 1",
                &[
                    Value::Text(active_version_schema_key().to_string()),
                    Value::Text(active_version_file_id().to_string()),
                    Value::Text(active_version_storage_version_id().to_string()),
                ],
            )
            .await?;

        if let Some(row) = result.rows.first() {
            let snapshot_content = row.first().ok_or_else(|| LixError {
                message: "active version query row is missing snapshot_content".to_string(),
            })?;
            let snapshot_content = match snapshot_content {
                Value::Text(value) => value.as_str(),
                other => {
                    return Err(LixError {
                        message: format!(
                            "active version snapshot_content must be text, got {other:?}"
                        ),
                    })
                }
            };
            let active_version_id = parse_active_version_snapshot(snapshot_content)?;
            self.set_active_version_id(active_version_id);
            return Ok(());
        }

        self.set_active_version_id(GLOBAL_VERSION_ID.to_string());
        Ok(())
    }

    fn collect_pending_file_writes(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<PendingFileWrite>, LixError> {
        let statements = parse_sql_statements(sql)?;
        let active_version_id = self.active_version_id.read().unwrap().clone();
        let mut writes = Vec::new();

        for statement in statements {
            let Statement::Insert(insert) = statement else {
                continue;
            };

            let Some(target) = file_write_target_from_insert(&insert.table) else {
                continue;
            };

            let Some(source) = insert.source.as_ref() else {
                continue;
            };
            let SetExpr::Values(values) = source.body.as_ref() else {
                continue;
            };

            let data_index = insert
                .columns
                .iter()
                .position(|column| column.value.eq_ignore_ascii_case("data"));
            let id_index = insert
                .columns
                .iter()
                .position(|column| column.value.eq_ignore_ascii_case("id"));
            let path_index = insert
                .columns
                .iter()
                .position(|column| column.value.eq_ignore_ascii_case("path"));
            let version_index = insert.columns.iter().position(|column| {
                column.value.eq_ignore_ascii_case("lixcol_version_id")
                    || column.value.eq_ignore_ascii_case("version_id")
            });

            let (Some(data_index), Some(id_index), Some(path_index)) =
                (data_index, id_index, path_index)
            else {
                continue;
            };

            let resolved_rows = resolve_values_rows(&values.rows, params)?;
            for (row, resolved_row) in values.rows.iter().zip(resolved_rows.iter()) {
                if row.len() != insert.columns.len() {
                    continue;
                }

                let Some(file_id) = resolved_cell_text(resolved_row.get(id_index)) else {
                    continue;
                };
                let Some(path) = resolved_cell_text(resolved_row.get(path_index)) else {
                    continue;
                };
                let Some(after_data) =
                    resolved_cell_blob_or_text_bytes(resolved_row.get(data_index))
                else {
                    continue;
                };

                let version_id = match target {
                    FileWriteTarget::ActiveVersion => active_version_id.clone(),
                    FileWriteTarget::ExplicitVersion => {
                        let Some(version_index) = version_index else {
                            continue;
                        };
                        let Some(version_id) = resolved_cell_text(resolved_row.get(version_index))
                        else {
                            continue;
                        };
                        version_id
                    }
                };

                writes.push(PendingFileWrite {
                    file_id,
                    version_id,
                    path,
                    before_data: None,
                    after_data,
                });
            }
        }

        Ok(writes)
    }

    async fn persist_detected_file_changes(
        &self,
        changes: &[crate::plugin::runtime::DetectedFileChange],
    ) -> Result<(), LixError> {
        let mut latest_by_key: BTreeMap<(String, String, String, String), usize> = BTreeMap::new();
        for (index, change) in changes.iter().enumerate() {
            latest_by_key.insert(
                (
                    change.file_id.clone(),
                    change.version_id.clone(),
                    change.schema_key.clone(),
                    change.entity_id.clone(),
                ),
                index,
            );
        }

        for index in latest_by_key.into_values() {
            let change = &changes[index];
            match &change.snapshot_content {
                Some(snapshot_content) => {
                    Box::pin(self.execute(
                        "INSERT INTO lix_internal_state_vtable (\
                         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                         ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                        &[
                            Value::Text(change.entity_id.clone()),
                            Value::Text(change.schema_key.clone()),
                            Value::Text(change.file_id.clone()),
                            Value::Text(change.version_id.clone()),
                            Value::Text(change.plugin_key.clone()),
                            Value::Text(snapshot_content.clone()),
                            Value::Text(change.schema_version.clone()),
                        ],
                    ))
                    .await?;
                }
                None => {
                    Box::pin(self.execute(
                        "DELETE FROM lix_internal_state_vtable \
                         WHERE entity_id = $1 \
                           AND schema_key = $2 \
                           AND file_id = $3 \
                           AND version_id = $4",
                        &[
                            Value::Text(change.entity_id.clone()),
                            Value::Text(change.schema_key.clone()),
                            Value::Text(change.file_id.clone()),
                            Value::Text(change.version_id.clone()),
                        ],
                    ))
                    .await?;
                }
            }
        }

        Ok(())
    }

    fn set_active_version_id(&self, version_id: String) {
        let mut guard = self.active_version_id.write().unwrap();
        if *guard == version_id {
            return;
        }
        *guard = version_id;
    }
}

fn file_write_target_from_insert(table: &TableObject) -> Option<FileWriteTarget> {
    let TableObject::TableName(name) = table else {
        return None;
    };

    let table_name = name
        .0
        .last()
        .and_then(sqlparser::ast::ObjectNamePart::as_ident)
        .map(|ident| ident.value.as_str())?;

    if table_name.eq_ignore_ascii_case("lix_file") {
        Some(FileWriteTarget::ActiveVersion)
    } else if table_name.eq_ignore_ascii_case("lix_file_by_version") {
        Some(FileWriteTarget::ExplicitVersion)
    } else {
        None
    }
}

fn resolved_cell_text(cell: Option<&crate::sql::ResolvedCell>) -> Option<String> {
    match cell.and_then(|entry| entry.value.as_ref()) {
        Some(Value::Text(value)) => Some(value.clone()),
        _ => None,
    }
}

fn resolved_cell_blob_or_text_bytes(cell: Option<&crate::sql::ResolvedCell>) -> Option<Vec<u8>> {
    match cell.and_then(|entry| entry.value.as_ref()) {
        Some(Value::Blob(bytes)) => Some(bytes.clone()),
        Some(Value::Text(text)) => Some(text.as_bytes().to_vec()),
        _ => None,
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

async fn upsert_plugin_record(
    backend: &dyn LixBackend,
    manifest: &PluginManifest,
    manifest_json: &str,
    wasm_bytes: &[u8],
    timestamp: &str,
) -> Result<(), LixError> {
    backend
        .execute(
            "INSERT INTO lix_internal_plugin (\
             key, runtime, api_version, detect_changes_glob, entry, manifest_json, wasm, created_at, updated_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8) \
             ON CONFLICT (key) DO UPDATE SET \
             runtime = EXCLUDED.runtime, \
             api_version = EXCLUDED.api_version, \
             detect_changes_glob = EXCLUDED.detect_changes_glob, \
             entry = EXCLUDED.entry, \
             manifest_json = EXCLUDED.manifest_json, \
             wasm = EXCLUDED.wasm, \
             updated_at = EXCLUDED.updated_at",
            &[
                Value::Text(manifest.key.clone()),
                Value::Text(manifest.runtime.as_str().to_string()),
                Value::Text(manifest.api_version.clone()),
                Value::Text(manifest.detect_changes_glob.clone()),
                Value::Text(manifest.entry_or_default().to_string()),
                Value::Text(manifest_json.to_string()),
                Value::Blob(wasm_bytes.to_vec()),
                Value::Text(timestamp.to_string()),
            ],
        )
        .await?;

    Ok(())
}

fn ensure_valid_wasm_binary(wasm_bytes: &[u8]) -> Result<(), LixError> {
    if wasm_bytes.is_empty() {
        return Err(LixError {
            message: "Plugin wasm bytes must not be empty".to_string(),
        });
    }
    if wasm_bytes.len() < 8 || !wasm_bytes.starts_with(&[0x00, 0x61, 0x73, 0x6d]) {
        return Err(LixError {
            message: "Plugin wasm bytes must start with a valid wasm header".to_string(),
        });
    }
    Ok(())
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

fn active_version_from_mutations(mutations: &[MutationRow]) -> Result<Option<String>, LixError> {
    for mutation in mutations.iter().rev() {
        if !mutation.untracked {
            continue;
        }
        if mutation.schema_key != active_version_schema_key()
            || mutation.file_id != active_version_file_id()
            || mutation.version_id != active_version_storage_version_id()
        {
            continue;
        }

        let snapshot = mutation.snapshot_content.as_ref().ok_or_else(|| LixError {
            message: "active version mutation is missing snapshot_content".to_string(),
        })?;
        let snapshot_content = serde_json::to_string(snapshot).map_err(|error| LixError {
            message: format!("active version mutation snapshot_content invalid JSON: {error}"),
        })?;
        return parse_active_version_snapshot(&snapshot_content).map(Some);
    }

    Ok(None)
}

fn active_version_from_update_validations(
    plans: &[UpdateValidationPlan],
) -> Result<Option<String>, LixError> {
    for plan in plans.iter().rev() {
        if !plan
            .table
            .eq_ignore_ascii_case("lix_internal_state_untracked")
        {
            continue;
        }
        if !where_clause_targets_active_version(plan.where_clause.as_ref()) {
            continue;
        }
        let Some(snapshot) = plan.snapshot_content.as_ref() else {
            continue;
        };

        let snapshot_content = serde_json::to_string(snapshot).map_err(|error| LixError {
            message: format!("active version update snapshot_content invalid JSON: {error}"),
        })?;
        return parse_active_version_snapshot(&snapshot_content).map(Some);
    }

    Ok(None)
}

fn where_clause_targets_active_version(where_clause: Option<&Expr>) -> bool {
    let Some(where_clause) = where_clause else {
        return false;
    };
    let Some(schema_keys) = schema_keys_from_expr(where_clause) else {
        return false;
    };
    schema_keys
        .iter()
        .any(|value| value.eq_ignore_ascii_case(active_version_schema_key()))
}

fn schema_keys_from_expr(expr: &Expr) -> Option<Vec<String>> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if expr_is_schema_key_column(left) {
                return schema_key_literal_value(right).map(|value| vec![value]);
            }
            if expr_is_schema_key_column(right) {
                return schema_key_literal_value(left).map(|value| vec![value]);
            }
            None
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => match (schema_keys_from_expr(left), schema_keys_from_expr(right)) {
            (Some(left), Some(right)) => {
                let intersection = intersect_strings(&left, &right);
                if intersection.is_empty() {
                    None
                } else {
                    Some(intersection)
                }
            }
            (Some(keys), None) | (None, Some(keys)) => Some(keys),
            (None, None) => None,
        },
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => match (schema_keys_from_expr(left), schema_keys_from_expr(right)) {
            (Some(left), Some(right)) => Some(union_strings(&left, &right)),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if !expr_is_schema_key_column(expr) {
                return None;
            }
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                let value = schema_key_literal_value(item)?;
                values.push(value);
            }
            if values.is_empty() {
                None
            } else {
                Some(dedup_strings(values))
            }
        }
        Expr::Nested(inner) => schema_keys_from_expr(inner),
        _ => None,
    }
}

fn expr_is_schema_key_column(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("schema_key"),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case("schema_key"))
            .unwrap_or(false),
        _ => false,
    }
}

fn schema_key_literal_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(value) => value.value.clone().into_string(),
        Expr::Identifier(ident) if ident.quote_style == Some('"') => Some(ident.value.clone()),
        _ => None,
    }
}

fn dedup_strings(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for value in values {
        if !out.contains(&value) {
            out.push(value);
        }
    }
    out
}

fn union_strings(left: &[String], right: &[String]) -> Vec<String> {
    let mut out = left.to_vec();
    for value in right {
        if !out.contains(value) {
            out.push(value.clone());
        }
    }
    out
}

fn intersect_strings(left: &[String], right: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for value in left {
        if right.contains(value) && !out.contains(value) {
            out.push(value.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{active_version_from_update_validations, active_version_schema_key};
    use crate::sql::parse_sql_statements;
    use crate::sql::UpdateValidationPlan;
    use serde_json::json;
    use sqlparser::ast::{Expr, Statement};

    #[test]
    fn detects_active_version_update_with_single_quoted_schema_key() {
        let where_clause = parse_update_where_clause(&format!(
            "UPDATE lix_internal_state_untracked SET snapshot_content = 'x' WHERE schema_key = '{}' AND entity_id = 'main'",
            active_version_schema_key()
        ));
        let plan = update_validation_plan(where_clause, "v-single");

        let detected = active_version_from_update_validations(&[plan]).expect("detect version");
        assert_eq!(detected.as_deref(), Some("v-single"));
    }

    #[test]
    fn detects_active_version_update_with_double_quoted_schema_key() {
        let where_clause = parse_update_where_clause(&format!(
            "UPDATE lix_internal_state_untracked SET snapshot_content = 'x' WHERE schema_key = \"{}\" AND entity_id = 'main'",
            active_version_schema_key()
        ));
        let plan = update_validation_plan(where_clause, "v-double");

        let detected = active_version_from_update_validations(&[plan]).expect("detect version");
        assert_eq!(detected.as_deref(), Some("v-double"));
    }

    #[test]
    fn ignores_non_active_version_schema_key() {
        let where_clause = parse_update_where_clause(
            "UPDATE lix_internal_state_untracked SET snapshot_content = 'x' WHERE schema_key = 'other_schema' AND entity_id = 'main'",
        );
        let plan = update_validation_plan(where_clause, "v-other");

        let detected = active_version_from_update_validations(&[plan]).expect("detect version");
        assert_eq!(detected, None);
    }

    fn parse_update_where_clause(sql: &str) -> Expr {
        let mut statements = parse_sql_statements(sql).expect("parse sql");
        let statement = statements.remove(0);
        let Statement::Update(update) = statement else {
            panic!("expected update statement");
        };
        update.selection.expect("where clause")
    }

    fn update_validation_plan(where_clause: Expr, version_id: &str) -> UpdateValidationPlan {
        UpdateValidationPlan {
            table: "lix_internal_state_untracked".to_string(),
            where_clause: Some(where_clause),
            snapshot_content: Some(json!({
                "id": "main",
                "version_id": version_id
            })),
            snapshot_patch: None,
        }
    }
}
