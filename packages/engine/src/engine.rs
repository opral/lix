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
    MaterializationApplyReport, MaterializationDebugMode, MaterializationPlan,
    MaterializationReport, MaterializationRequest, MaterializationScope,
};
use crate::plugin::manifest::parse_plugin_manifest_json;
use crate::plugin::types::{InstalledPlugin, PluginManifest};
use crate::schema_registry::{register_schema, register_schema_sql_statements};
use crate::sql::{
    bind_sql_with_state, build_delete_followup_sql, build_update_followup_sql,
    coalesce_vtable_inserts_in_statement_list, escape_sql_string, expr_references_column_name,
    parse_sql_statements, preprocess_sql,
    preprocess_sql_with_provider_and_detected_file_domain_changes, ColumnReferenceOptions,
    DetectedFileDomainChange, MutationOperation, MutationRow, PlaceholderState, PostprocessPlan,
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
use crate::{LixBackend, LixError, LixTransaction, QueryResult, SqlDialect, Value};
use futures_util::FutureExt;
use serde_json::Value as JsonValue;
use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Expr, FromTable, ObjectName, ObjectNamePart, Query, Select,
    SetExpr, Statement, TableFactor, TableObject, TableWithJoins, Update, UpdateTableFromKind,
};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

#[derive(Debug, Clone, Default)]
pub struct ExecuteOptions {
    pub writer_key: Option<String>,
}

pub type EngineTransactionFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, LixError>> + 'a>>;

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
    installed_plugins_cache: RwLock<Option<Vec<InstalledPlugin>>>,
    plugin_component_cache: Mutex<BTreeMap<String, crate::plugin::runtime::CachedPluginComponent>>,
}

#[must_use = "EngineTransaction must be committed or rolled back"]
pub struct EngineTransaction<'a> {
    engine: &'a Engine,
    transaction: Option<Box<dyn LixTransaction + 'a>>,
    options: ExecuteOptions,
    active_version_id: String,
    active_version_changed: bool,
    installed_plugins_cache_invalidation_pending: bool,
}

impl<'a> EngineTransaction<'a> {
    pub async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let previous_active_version_id = self.active_version_id.clone();
        let transaction = self.transaction.as_mut().ok_or_else(|| LixError {
            message: "transaction is no longer active".to_string(),
        })?;
        let result = self
            .engine
            .execute_with_options_in_transaction(
                transaction.as_mut(),
                sql,
                params,
                &self.options,
                &mut self.active_version_id,
                None,
                false,
            )
            .await?;
        if self.active_version_id != previous_active_version_id {
            self.active_version_changed = true;
        }
        if should_invalidate_installed_plugins_cache_for_sql(sql) {
            self.installed_plugins_cache_invalidation_pending = true;
        }
        Ok(result)
    }

    pub async fn commit(mut self) -> Result<(), LixError> {
        let transaction = self.transaction.take().ok_or_else(|| LixError {
            message: "transaction is no longer active".to_string(),
        })?;
        transaction.commit().await?;
        if self.active_version_changed {
            self.engine
                .set_active_version_id(std::mem::take(&mut self.active_version_id));
        }
        if self.installed_plugins_cache_invalidation_pending {
            self.engine.invalidate_installed_plugins_cache()?;
        }
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<(), LixError> {
        let transaction = self.transaction.take().ok_or_else(|| LixError {
            message: "transaction is no longer active".to_string(),
        })?;
        transaction.rollback().await
    }
}

impl Drop for EngineTransaction<'_> {
    fn drop(&mut self) {
        if self.transaction.is_some() && !std::thread::panicking() {
            panic!("EngineTransaction dropped without commit() or rollback()");
        }
    }
}

struct TransactionBackendAdapter<'a> {
    dialect: crate::SqlDialect,
    transaction: Mutex<*mut (dyn LixTransaction + 'a)>,
    _lifetime: PhantomData<&'a ()>,
}

struct CollectedExecutionSideEffects {
    pending_file_writes: Vec<crate::filesystem::pending_file_writes::PendingFileWrite>,
    pending_file_delete_targets: BTreeSet<(String, String)>,
    detected_file_domain_changes_by_statement: Vec<Vec<DetectedFileDomainChange>>,
    detected_file_domain_changes: Vec<DetectedFileDomainChange>,
    untracked_filesystem_update_domain_changes: Vec<DetectedFileDomainChange>,
}

#[derive(Default)]
struct DeferredTransactionSideEffects {
    pending_file_writes: Vec<crate::filesystem::pending_file_writes::PendingFileWrite>,
    pending_file_delete_targets: BTreeSet<(String, String)>,
    detected_file_domain_changes: Vec<DetectedFileDomainChange>,
    untracked_filesystem_update_domain_changes: Vec<DetectedFileDomainChange>,
    file_cache_invalidation_targets: BTreeSet<(String, String)>,
}

// SAFETY: `TransactionBackendAdapter` is only used inside a single async execution flow.
// Internal access to the raw transaction pointer is serialized with a mutex.
unsafe impl<'a> Send for TransactionBackendAdapter<'a> {}
// SAFETY: see `Send` impl above.
unsafe impl<'a> Sync for TransactionBackendAdapter<'a> {}

impl<'a> TransactionBackendAdapter<'a> {
    fn new(transaction: &'a mut dyn LixTransaction) -> Self {
        Self {
            dialect: transaction.dialect(),
            transaction: Mutex::new(transaction as *mut (dyn LixTransaction + 'a)),
            _lifetime: PhantomData,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl<'a> LixBackend for TransactionBackendAdapter<'a> {
    fn dialect(&self) -> crate::SqlDialect {
        self.dialect
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let mut guard = self.transaction.lock().map_err(|_| LixError {
            message: "transaction adapter lock poisoned".to_string(),
        })?;
        // SAFETY: the pointer is created from a live `&mut dyn LixTransaction` and
        // this mutex serializes all calls so the mutable borrow is not aliased.
        unsafe { (&mut **guard).execute(sql, params).await }
    }

    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
        Err(LixError {
            message: "nested transactions are not supported".to_string(),
        })
    }
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
        installed_plugins_cache: RwLock::new(None),
        plugin_component_cache: Mutex::new(BTreeMap::new()),
    }
}

impl Engine {
    pub fn wasm_runtime(&self) -> Option<Arc<dyn WasmRuntime>> {
        self.wasm_runtime.clone()
    }

    async fn load_installed_plugins_with_backend(
        &self,
        backend: &dyn LixBackend,
        allow_cache: bool,
    ) -> Result<Vec<InstalledPlugin>, LixError> {
        if allow_cache {
            let cached = self
                .installed_plugins_cache
                .read()
                .map_err(|_| LixError {
                    message: "installed plugin cache lock poisoned".to_string(),
                })?
                .clone();
            if let Some(plugins) = cached {
                return Ok(plugins);
            }
        }

        let loaded = crate::plugin::runtime::load_installed_plugins(backend).await?;
        if allow_cache {
            let mut guard = self.installed_plugins_cache.write().map_err(|_| LixError {
                message: "installed plugin cache lock poisoned".to_string(),
            })?;
            *guard = Some(loaded.clone());
        }
        Ok(loaded)
    }

    fn invalidate_installed_plugins_cache(&self) -> Result<(), LixError> {
        let mut guard = self.installed_plugins_cache.write().map_err(|_| LixError {
            message: "installed plugin cache lock poisoned".to_string(),
        })?;
        *guard = None;
        let mut component_guard = self.plugin_component_cache.lock().map_err(|_| LixError {
            message: "plugin component cache lock poisoned".to_string(),
        })?;
        component_guard.clear();
        Ok(())
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

    pub async fn execute(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<QueryResult, LixError> {
        if let Some(statements) = extract_explicit_transaction_script(sql, params)? {
            return self
                .execute_transaction_script_with_options(statements, options)
                .await;
        }

        let active_version_id = self.active_version_id.read().unwrap().clone();
        let writer_key = options.writer_key.as_deref();
        self.maybe_materialize_reads_with_backend(self.backend.as_ref(), sql, &active_version_id)
            .await?;
        let read_only_query = is_query_only_sql(sql);
        let should_refresh_file_cache = !read_only_query && should_refresh_file_cache_for_sql(sql);
        let CollectedExecutionSideEffects {
            pending_file_writes,
            pending_file_delete_targets,
            detected_file_domain_changes_by_statement,
            detected_file_domain_changes,
            untracked_filesystem_update_domain_changes,
        } = if read_only_query {
            CollectedExecutionSideEffects {
                pending_file_writes: Vec::new(),
                pending_file_delete_targets: BTreeSet::new(),
                detected_file_domain_changes_by_statement: Vec::new(),
                detected_file_domain_changes: Vec::new(),
                untracked_filesystem_update_domain_changes: Vec::new(),
            }
        } else {
            self.collect_execution_side_effects_with_backend(
                self.backend.as_ref(),
                sql,
                params,
                &active_version_id,
                writer_key,
                true,
                true,
            )
            .await?
        };
        let (settings, sequence_start, functions) = self
            .prepare_runtime_functions_with_backend(self.backend.as_ref())
            .await?;

        let output = match preprocess_sql_with_provider_and_detected_file_domain_changes(
            self.backend.as_ref(),
            &self.cel_evaluator,
            sql,
            params,
            functions.clone(),
            &detected_file_domain_changes_by_statement,
            writer_key,
        )
        .await
        {
            Ok(output) => output,
            Err(error) if should_sequentialize_postprocess_multi_statement(sql, params, &error) => {
                return self
                    .execute_multi_statement_sequential_with_options(sql, params, &options)
                    .await;
            }
            Err(error) => return Err(error),
        };
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
                &output.params,
            )
            .await?;
        }
        for registration in output.registrations {
            register_schema(self.backend.as_ref(), &registration.schema_key).await?;
        }
        let mut postprocess_file_cache_targets = BTreeSet::new();
        let mut plugin_changes_committed = false;
        let result = match output.postprocess {
            None => {
                let result = self.backend.execute(&output.sql, &output.params).await?;
                let tracked_insert_mutation_present = output.mutations.iter().any(|mutation| {
                    mutation.operation == MutationOperation::Insert && !mutation.untracked
                });
                if tracked_insert_mutation_present && !detected_file_domain_changes.is_empty() {
                    plugin_changes_committed = true;
                }
                Ok(result)
            }
            Some(postprocess_plan) => {
                let mut transaction = self.backend.begin_transaction().await?;
                let result = match transaction.execute(&output.sql, &output.params).await {
                    Ok(result) => result,
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        return Err(error);
                    }
                };
                match &postprocess_plan {
                    PostprocessPlan::VtableUpdate(plan) => {
                        if should_refresh_file_cache {
                            let targets = match collect_postprocess_file_cache_targets(
                                &result.rows,
                                &plan.schema_key,
                            ) {
                                Ok(targets) => targets,
                                Err(error) => {
                                    let _ = transaction.rollback().await;
                                    return Err(error);
                                }
                            };
                            postprocess_file_cache_targets.extend(targets);
                        }
                    }
                    PostprocessPlan::VtableDelete(plan) => {
                        if should_refresh_file_cache {
                            let targets = match collect_postprocess_file_cache_targets(
                                &result.rows,
                                &plan.schema_key,
                            ) {
                                Ok(targets) => targets,
                                Err(error) => {
                                    let _ = transaction.rollback().await;
                                    return Err(error);
                                }
                            };
                            postprocess_file_cache_targets.extend(targets);
                        }
                    }
                }
                let additional_schema_keys = detected_file_domain_changes
                    .iter()
                    .map(|change| change.schema_key.clone())
                    .collect::<BTreeSet<_>>();
                for schema_key in additional_schema_keys {
                    for statement in
                        register_schema_sql_statements(&schema_key, transaction.dialect())
                    {
                        if let Err(error) = transaction.execute(&statement, &[]).await {
                            let _ = transaction.rollback().await;
                            return Err(error);
                        }
                    }
                }
                let mut followup_functions = functions.clone();
                let followup_sql = match postprocess_plan {
                    PostprocessPlan::VtableUpdate(plan) => match build_update_followup_sql(
                        transaction.as_mut(),
                        &plan,
                        &result.rows,
                        &detected_file_domain_changes,
                        writer_key,
                        &mut followup_functions,
                    )
                    .await
                    {
                        Ok(sql) => sql,
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            return Err(error);
                        }
                    },
                    PostprocessPlan::VtableDelete(plan) => match build_delete_followup_sql(
                        transaction.as_mut(),
                        &plan,
                        &result.rows,
                        &output.params,
                        &detected_file_domain_changes,
                        writer_key,
                        &mut followup_functions,
                    )
                    .await
                    {
                        Ok(sql) => sql,
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            return Err(error);
                        }
                    },
                };
                if !followup_sql.is_empty() {
                    if let Err(error) = transaction.execute(&followup_sql, &[]).await {
                        let _ = transaction.rollback().await;
                        return Err(error);
                    }
                }
                transaction.commit().await?;
                plugin_changes_committed = true;
                Ok(result)
            }
        }?;

        self.persist_runtime_sequence_with_backend(
            self.backend.as_ref(),
            settings,
            sequence_start,
            &functions,
        )
        .await?;

        if let Some(version_id) =
            next_active_version_id_from_mutations.or(next_active_version_id_from_updates)
        {
            self.set_active_version_id(version_id);
        }
        let file_cache_refresh_targets = if should_refresh_file_cache {
            let mut targets = direct_state_file_cache_refresh_targets(&output.mutations);
            targets.extend(postprocess_file_cache_targets);
            targets
        } else {
            BTreeSet::new()
        };
        let descriptor_cache_eviction_targets =
            file_descriptor_cache_eviction_targets(&output.mutations);
        let mut file_cache_invalidation_targets = file_cache_refresh_targets.clone();
        file_cache_invalidation_targets.extend(descriptor_cache_eviction_targets);
        file_cache_invalidation_targets.extend(pending_file_delete_targets);

        if !plugin_changes_committed && !detected_file_domain_changes.is_empty() {
            self.persist_detected_file_domain_changes(&detected_file_domain_changes)
                .await?;
        }
        if !untracked_filesystem_update_domain_changes.is_empty() {
            self.persist_untracked_file_domain_changes(&untracked_filesystem_update_domain_changes)
                .await?;
        }
        self.persist_pending_file_data_updates(&pending_file_writes)
            .await?;
        self.persist_pending_file_path_updates(&pending_file_writes)
            .await?;
        self.invalidate_file_data_cache_entries(&file_cache_invalidation_targets)
            .await?;
        self.invalidate_file_path_cache_entries(&file_cache_invalidation_targets)
            .await?;
        if self.wasm_runtime.is_some() {
            self.refresh_file_data_for_versions(file_cache_refresh_targets)
                .await?;
        }
        if should_invalidate_installed_plugins_cache_for_sql(sql) {
            self.invalidate_installed_plugins_cache()?;
        }

        Ok(result)
    }

    async fn execute_transaction_script_with_options(
        &self,
        statements: Vec<Statement>,
        options: ExecuteOptions,
    ) -> Result<QueryResult, LixError> {
        let mut transaction = self.backend.begin_transaction().await?;
        let mut last_result = QueryResult { rows: Vec::new() };
        let mut active_version_id = self.active_version_id.read().unwrap().clone();
        let mut active_version_changed = false;
        let mut installed_plugins_cache_invalidation_pending = false;
        let original_statements = statements;
        let coalesced_statements = coalesce_lix_file_transaction_statements(
            &original_statements,
            Some(transaction.dialect()),
        );
        let can_defer_side_effects = self.wasm_runtime.is_none() && coalesced_statements.is_some();
        let mut deferred_side_effects = if can_defer_side_effects {
            let original_sql = original_statements
                .iter()
                .map(Statement::to_string)
                .collect::<Vec<_>>()
                .join(";\n");
            let CollectedExecutionSideEffects {
                pending_file_writes,
                pending_file_delete_targets,
                detected_file_domain_changes: filesystem_tracked_domain_changes,
                untracked_filesystem_update_domain_changes,
                ..
            } = {
                let backend = TransactionBackendAdapter::new(transaction.as_mut());
                self.collect_execution_side_effects_with_backend(
                    &backend,
                    &original_sql,
                    &[],
                    &active_version_id,
                    options.writer_key.as_deref(),
                    false,
                    false,
                )
                .await?
            };
            Some(DeferredTransactionSideEffects {
                pending_file_writes,
                pending_file_delete_targets: pending_file_delete_targets.clone(),
                detected_file_domain_changes: filesystem_tracked_domain_changes,
                untracked_filesystem_update_domain_changes,
                file_cache_invalidation_targets: pending_file_delete_targets,
            })
        } else {
            None
        };
        let sql_statements = if let Some(coalesced) = coalesced_statements {
            coalesced
        } else {
            coalesce_vtable_inserts_in_statement_list(original_statements)?
                .into_iter()
                .map(|statement| statement.to_string())
                .collect::<Vec<_>>()
        };
        let skip_statement_side_effect_collection = deferred_side_effects.is_some();

        for sql in sql_statements {
            let previous_active_version_id = active_version_id.clone();
            let result = if skip_statement_side_effect_collection {
                self.execute_with_options_in_transaction(
                    transaction.as_mut(),
                    &sql,
                    &[],
                    &options,
                    &mut active_version_id,
                    deferred_side_effects.as_mut(),
                    true,
                )
                .await
            } else {
                self.execute_with_options_in_transaction(
                    transaction.as_mut(),
                    &sql,
                    &[],
                    &options,
                    &mut active_version_id,
                    None,
                    false,
                )
                .await
            };

            match result {
                Ok(query_result) => {
                    last_result = query_result;
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            }

            if active_version_id != previous_active_version_id {
                active_version_changed = true;
            }
            if should_invalidate_installed_plugins_cache_for_sql(&sql) {
                installed_plugins_cache_invalidation_pending = true;
            }
        }

        if let Some(side_effects) = deferred_side_effects.as_mut() {
            if let Err(error) = self
                .flush_deferred_transaction_side_effects_in_transaction(
                    transaction.as_mut(),
                    side_effects,
                    options.writer_key.as_deref(),
                )
                .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        }

        transaction.commit().await?;
        if active_version_changed {
            self.set_active_version_id(active_version_id);
        }
        if installed_plugins_cache_invalidation_pending {
            self.invalidate_installed_plugins_cache()?;
        }
        Ok(last_result)
    }

    async fn begin_transaction_with_options(
        &self,
        options: ExecuteOptions,
    ) -> Result<EngineTransaction<'_>, LixError> {
        let transaction = self.backend.begin_transaction().await?;
        Ok(EngineTransaction {
            engine: self,
            transaction: Some(transaction),
            options,
            active_version_id: self.active_version_id.read().unwrap().clone(),
            active_version_changed: false,
            installed_plugins_cache_invalidation_pending: false,
        })
    }

    pub async fn transaction<T, F>(&self, options: ExecuteOptions, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(&'tx mut EngineTransaction<'_>) -> EngineTransactionFuture<'tx, T>,
    {
        let mut transaction = self.begin_transaction_with_options(options).await?;
        match std::panic::AssertUnwindSafe(f(&mut transaction))
            .catch_unwind()
            .await
        {
            Ok(Ok(value)) => {
                transaction.commit().await?;
                Ok(value)
            }
            Ok(Err(error)) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
            Err(payload) => {
                let _ = transaction.rollback().await;
                std::panic::resume_unwind(payload);
            }
        }
    }

    async fn execute_with_options_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        sql: &str,
        params: &[Value],
        options: &ExecuteOptions,
        active_version_id: &mut String,
        deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
        skip_side_effect_collection: bool,
    ) -> Result<QueryResult, LixError> {
        let writer_key = options.writer_key.as_deref();
        let defer_side_effects = deferred_side_effects.is_some();
        let read_only_query = is_query_only_sql(sql);
        let should_refresh_file_cache = !read_only_query && should_refresh_file_cache_for_sql(sql);
        let (
            pending_file_writes,
            pending_file_delete_targets,
            detected_file_domain_changes,
            untracked_filesystem_update_domain_changes,
            settings,
            sequence_start,
            functions,
            output,
        ) = {
            let backend = TransactionBackendAdapter::new(transaction);
            self.maybe_materialize_reads_with_backend(&backend, sql, active_version_id)
                .await?;
            let CollectedExecutionSideEffects {
                pending_file_writes,
                pending_file_delete_targets,
                detected_file_domain_changes_by_statement,
                detected_file_domain_changes,
                untracked_filesystem_update_domain_changes,
            } = if skip_side_effect_collection || read_only_query {
                CollectedExecutionSideEffects {
                    pending_file_writes: Vec::new(),
                    pending_file_delete_targets: BTreeSet::new(),
                    detected_file_domain_changes_by_statement: Vec::new(),
                    detected_file_domain_changes: Vec::new(),
                    untracked_filesystem_update_domain_changes: Vec::new(),
                }
            } else {
                self.collect_execution_side_effects_with_backend(
                    &backend,
                    sql,
                    params,
                    active_version_id,
                    writer_key,
                    false,
                    !defer_side_effects,
                )
                .await?
            };
            let (settings, sequence_start, functions) = self
                .prepare_runtime_functions_with_backend(&backend)
                .await?;
            let output = match preprocess_sql_with_provider_and_detected_file_domain_changes(
                &backend,
                &self.cel_evaluator,
                sql,
                params,
                functions.clone(),
                &detected_file_domain_changes_by_statement,
                writer_key,
            )
            .await
            {
                Ok(output) => output,
                Err(error)
                    if should_sequentialize_postprocess_multi_statement(sql, params, &error) =>
                {
                    return Box::pin(
                        self.execute_multi_statement_sequential_with_options_in_transaction(
                            transaction,
                            sql,
                            params,
                            options,
                            active_version_id,
                        ),
                    )
                    .await;
                }
                Err(error) => return Err(error),
            };
            if !output.mutations.is_empty() {
                validate_inserts(&backend, &self.schema_cache, &output.mutations).await?;
            }
            if !output.update_validations.is_empty() {
                validate_updates(
                    &backend,
                    &self.schema_cache,
                    &output.update_validations,
                    &output.params,
                )
                .await?;
            }

            (
                pending_file_writes,
                pending_file_delete_targets,
                detected_file_domain_changes,
                untracked_filesystem_update_domain_changes,
                settings,
                sequence_start,
                functions,
                output,
            )
        };

        let next_active_version_id_from_mutations =
            active_version_from_mutations(&output.mutations)?;
        let next_active_version_id_from_updates =
            active_version_from_update_validations(&output.update_validations)?;
        for registration in output.registrations {
            for statement in
                register_schema_sql_statements(&registration.schema_key, transaction.dialect())
            {
                transaction.execute(&statement, &[]).await?;
            }
        }

        let mut postprocess_file_cache_targets = BTreeSet::new();
        let mut plugin_changes_committed = false;
        let result = match output.postprocess {
            None => {
                let result = transaction.execute(&output.sql, &output.params).await?;
                let tracked_insert_mutation_present = output.mutations.iter().any(|mutation| {
                    mutation.operation == MutationOperation::Insert && !mutation.untracked
                });
                if tracked_insert_mutation_present && !detected_file_domain_changes.is_empty() {
                    plugin_changes_committed = true;
                }
                result
            }
            Some(postprocess_plan) => {
                let result = transaction.execute(&output.sql, &output.params).await?;
                match &postprocess_plan {
                    PostprocessPlan::VtableUpdate(plan) => {
                        if should_refresh_file_cache {
                            postprocess_file_cache_targets.extend(
                                collect_postprocess_file_cache_targets(
                                    &result.rows,
                                    &plan.schema_key,
                                )?,
                            );
                        }
                    }
                    PostprocessPlan::VtableDelete(plan) => {
                        if should_refresh_file_cache {
                            postprocess_file_cache_targets.extend(
                                collect_postprocess_file_cache_targets(
                                    &result.rows,
                                    &plan.schema_key,
                                )?,
                            );
                        }
                    }
                }
                let additional_schema_keys = detected_file_domain_changes
                    .iter()
                    .map(|change| change.schema_key.clone())
                    .collect::<BTreeSet<_>>();
                for schema_key in additional_schema_keys {
                    for statement in
                        register_schema_sql_statements(&schema_key, transaction.dialect())
                    {
                        transaction.execute(&statement, &[]).await?;
                    }
                }
                let mut followup_functions = functions.clone();
                let followup_sql = match postprocess_plan {
                    PostprocessPlan::VtableUpdate(plan) => {
                        build_update_followup_sql(
                            transaction,
                            &plan,
                            &result.rows,
                            &detected_file_domain_changes,
                            writer_key,
                            &mut followup_functions,
                        )
                        .await?
                    }
                    PostprocessPlan::VtableDelete(plan) => {
                        build_delete_followup_sql(
                            transaction,
                            &plan,
                            &result.rows,
                            &output.params,
                            &detected_file_domain_changes,
                            writer_key,
                            &mut followup_functions,
                        )
                        .await?
                    }
                };
                if !followup_sql.is_empty() {
                    transaction.execute(&followup_sql, &[]).await?;
                }
                plugin_changes_committed = true;
                result
            }
        };

        if let Some(version_id) =
            next_active_version_id_from_mutations.or(next_active_version_id_from_updates)
        {
            *active_version_id = version_id;
        }

        let file_cache_refresh_targets = if should_refresh_file_cache {
            let mut targets = direct_state_file_cache_refresh_targets(&output.mutations);
            targets.extend(postprocess_file_cache_targets);
            targets
        } else {
            BTreeSet::new()
        };
        let descriptor_cache_eviction_targets =
            file_descriptor_cache_eviction_targets(&output.mutations);
        let mut file_cache_invalidation_targets = file_cache_refresh_targets;
        file_cache_invalidation_targets.extend(descriptor_cache_eviction_targets);
        file_cache_invalidation_targets.extend(pending_file_delete_targets);

        if let Some(deferred) = deferred_side_effects {
            deferred.pending_file_writes.extend(pending_file_writes);
            deferred
                .file_cache_invalidation_targets
                .extend(file_cache_invalidation_targets.iter().cloned());
            if !plugin_changes_committed {
                deferred
                    .detected_file_domain_changes
                    .extend(detected_file_domain_changes);
            }
            deferred
                .untracked_filesystem_update_domain_changes
                .extend(untracked_filesystem_update_domain_changes);
        } else {
            if !plugin_changes_committed && !detected_file_domain_changes.is_empty() {
                self.persist_detected_file_domain_changes_in_transaction(
                    transaction,
                    &detected_file_domain_changes,
                )
                .await?;
            }
            if !untracked_filesystem_update_domain_changes.is_empty() {
                self.persist_untracked_file_domain_changes_in_transaction(
                    transaction,
                    &untracked_filesystem_update_domain_changes,
                )
                .await?;
            }
            self.persist_pending_file_data_updates_in_transaction(
                transaction,
                &pending_file_writes,
            )
            .await?;
            self.persist_pending_file_path_updates_in_transaction(
                transaction,
                &pending_file_writes,
            )
            .await?;
            self.invalidate_file_data_cache_entries_in_transaction(
                transaction,
                &file_cache_invalidation_targets,
            )
            .await?;
            self.invalidate_file_path_cache_entries_in_transaction(
                transaction,
                &file_cache_invalidation_targets,
            )
            .await?;
        }
        self.persist_runtime_sequence_with_backend(
            &TransactionBackendAdapter::new(transaction),
            settings,
            sequence_start,
            &functions,
        )
        .await?;
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
        .await?;
        self.invalidate_installed_plugins_cache()?;
        Ok(())
    }

    /// Exports a portable snapshot as SQLite3 file bytes written via chunk stream.
    pub async fn export_snapshot(
        &self,
        writer: &mut dyn crate::SnapshotChunkWriter,
    ) -> Result<(), LixError> {
        self.backend.export_snapshot(writer).await
    }

    pub async fn restore_from_snapshot(
        &self,
        reader: &mut dyn crate::SnapshotChunkReader,
    ) -> Result<(), LixError> {
        self.backend.restore_from_snapshot(reader).await?;
        self.load_and_cache_active_version().await?;
        self.invalidate_installed_plugins_cache()?;
        Ok(())
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

    async fn maybe_materialize_reads_with_backend(
        &self,
        backend: &dyn LixBackend,
        sql: &str,
        active_version_id: &str,
    ) -> Result<(), LixError> {
        let Some(runtime) = self.wasm_runtime.as_ref() else {
            return Ok(());
        };

        if let Some(scope) = file_read_materialization_scope_for_sql(sql) {
            let versions = match scope {
                FileReadMaterializationScope::ActiveVersionOnly => {
                    let mut set = BTreeSet::new();
                    set.insert(active_version_id.to_string());
                    Some(set)
                }
                FileReadMaterializationScope::AllVersions => None,
            };
            crate::plugin::runtime::materialize_missing_file_data_with_plugins(
                backend,
                runtime.as_ref(),
                versions.as_ref(),
            )
            .await?;
        }
        if file_history_read_materialization_required_for_sql(sql) {
            crate::plugin::runtime::materialize_missing_file_history_data_with_plugins(
                backend,
                runtime.as_ref(),
            )
            .await?;
        }
        Ok(())
    }

    async fn collect_execution_side_effects_with_backend(
        &self,
        backend: &dyn LixBackend,
        sql: &str,
        params: &[Value],
        active_version_id: &str,
        writer_key: Option<&str>,
        allow_plugin_cache: bool,
        detect_plugin_file_changes: bool,
    ) -> Result<CollectedExecutionSideEffects, LixError> {
        let pending_file_write_collection =
            crate::filesystem::pending_file_writes::collect_pending_file_writes(
                backend,
                sql,
                params,
                active_version_id,
            )
            .await
            .map_err(|error| LixError {
                message: format!("pending file writes collection failed: {}", error.message),
            })?;
        let crate::filesystem::pending_file_writes::PendingFileWriteCollection {
            writes: pending_file_writes,
            writes_by_statement: pending_file_writes_by_statement,
        } = pending_file_write_collection;
        let pending_file_delete_targets =
            crate::filesystem::pending_file_writes::collect_pending_file_delete_targets(
                backend,
                sql,
                params,
                active_version_id,
            )
            .await
            .map_err(|error| LixError {
                message: format!("pending file delete collection failed: {}", error.message),
            })?;

        let detected_file_changes_by_statement = if detect_plugin_file_changes {
            self.detect_file_changes_for_pending_writes_by_statement_with_backend(
                backend,
                &pending_file_writes_by_statement,
                allow_plugin_cache,
            )
            .await?
        } else {
            vec![Vec::new(); pending_file_writes_by_statement.len()]
        };
        let mut detected_file_domain_changes_by_statement = detected_file_changes_by_statement
            .into_iter()
            .map(|changes| {
                detected_file_domain_changes_from_detected_file_changes(&changes, writer_key)
            })
            .collect::<Vec<_>>();

        let filesystem_update_domain_changes =
            collect_filesystem_update_detected_file_domain_changes(backend, sql, params)
                .await
                .map_err(|error| LixError {
                    message: format!(
                        "filesystem update side-effect detection failed: {}",
                        error.message
                    ),
                })?;
        let filesystem_update_tracked_changes_by_statement = filesystem_update_domain_changes
            .tracked_changes_by_statement
            .iter()
            .map(|changes| detected_file_domain_changes_with_writer_key(changes, writer_key))
            .collect::<Vec<_>>();
        let statement_count = detected_file_domain_changes_by_statement
            .len()
            .max(filesystem_update_tracked_changes_by_statement.len());
        detected_file_domain_changes_by_statement.resize_with(statement_count, Vec::new);
        for (index, tracked_changes) in filesystem_update_tracked_changes_by_statement
            .into_iter()
            .enumerate()
        {
            detected_file_domain_changes_by_statement[index].extend(tracked_changes);
            detected_file_domain_changes_by_statement[index] = dedupe_detected_file_domain_changes(
                &detected_file_domain_changes_by_statement[index],
            );
        }
        let mut detected_file_domain_changes = detected_file_domain_changes_by_statement
            .iter()
            .flat_map(|changes| changes.iter().cloned())
            .collect::<Vec<_>>();
        detected_file_domain_changes =
            dedupe_detected_file_domain_changes(&detected_file_domain_changes);
        let untracked_filesystem_update_domain_changes =
            dedupe_detected_file_domain_changes(&detected_file_domain_changes_with_writer_key(
                &filesystem_update_domain_changes.untracked_changes,
                writer_key,
            ));

        Ok(CollectedExecutionSideEffects {
            pending_file_writes,
            pending_file_delete_targets,
            detected_file_domain_changes_by_statement,
            detected_file_domain_changes,
            untracked_filesystem_update_domain_changes,
        })
    }

    async fn flush_deferred_transaction_side_effects_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        side_effects: &mut DeferredTransactionSideEffects,
        writer_key: Option<&str>,
    ) -> Result<(), LixError> {
        let collapsed_writes =
            collapse_pending_file_writes_for_transaction(&side_effects.pending_file_writes);
        side_effects.pending_file_writes = collapsed_writes;

        let mut detected_file_domain_changes =
            std::mem::take(&mut side_effects.detected_file_domain_changes);
        if !side_effects.pending_file_writes.is_empty() {
            let detected_by_statement = {
                let backend = TransactionBackendAdapter::new(transaction);
                self.detect_file_changes_for_pending_writes_by_statement_with_backend(
                    &backend,
                    &[side_effects.pending_file_writes.clone()],
                    false,
                )
                .await?
            };
            if let Some(detected) = detected_by_statement.into_iter().next() {
                detected_file_domain_changes.extend(
                    detected_file_domain_changes_from_detected_file_changes(&detected, writer_key),
                );
            }
        }
        detected_file_domain_changes =
            dedupe_detected_file_domain_changes(&detected_file_domain_changes);
        let untracked_filesystem_update_domain_changes = dedupe_detected_file_domain_changes(
            &std::mem::take(&mut side_effects.untracked_filesystem_update_domain_changes),
        );
        side_effects
            .file_cache_invalidation_targets
            .extend(std::mem::take(
                &mut side_effects.pending_file_delete_targets,
            ));

        if !detected_file_domain_changes.is_empty() {
            self.persist_detected_file_domain_changes_in_transaction(
                transaction,
                &detected_file_domain_changes,
            )
            .await?;
        }
        if !untracked_filesystem_update_domain_changes.is_empty() {
            self.persist_untracked_file_domain_changes_in_transaction(
                transaction,
                &untracked_filesystem_update_domain_changes,
            )
            .await?;
        }
        self.persist_pending_file_data_updates_in_transaction(
            transaction,
            &side_effects.pending_file_writes,
        )
        .await?;
        self.persist_pending_file_path_updates_in_transaction(
            transaction,
            &side_effects.pending_file_writes,
        )
        .await?;
        self.invalidate_file_data_cache_entries_in_transaction(
            transaction,
            &side_effects.file_cache_invalidation_targets,
        )
        .await?;
        self.invalidate_file_path_cache_entries_in_transaction(
            transaction,
            &side_effects.file_cache_invalidation_targets,
        )
        .await?;

        Ok(())
    }

    async fn prepare_runtime_functions_with_backend(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<
        (
            DeterministicSettings,
            i64,
            SharedFunctionProvider<RuntimeFunctionProvider>,
        ),
        LixError,
    > {
        let mut settings = load_settings(backend).await?;
        if self.deterministic_boot_pending.load(Ordering::SeqCst) {
            if let Some(boot_settings) = self.boot_deterministic_settings {
                settings = boot_settings;
            }
        }
        let sequence_start = if settings.enabled {
            load_persisted_sequence_next(backend).await?
        } else {
            0
        };
        let functions =
            SharedFunctionProvider::new(RuntimeFunctionProvider::new(settings, sequence_start));
        Ok((settings, sequence_start, functions))
    }

    async fn persist_runtime_sequence_with_backend(
        &self,
        backend: &dyn LixBackend,
        settings: DeterministicSettings,
        sequence_start: i64,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError> {
        if settings.enabled {
            let sequence_end = functions.with_lock(|provider| provider.next_sequence());
            if sequence_end > sequence_start {
                persist_sequence_highest(backend, sequence_end - 1).await?;
            }
        }
        Ok(())
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
                    ExecuteOptions::default(),
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
                ExecuteOptions::default(),
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
                ExecuteOptions::default(),
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
                ExecuteOptions::default(),
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
                ExecuteOptions::default(),
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
            ExecuteOptions::default(),
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
            ExecuteOptions::default(),
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
        let result = self
            .execute("SELECT lix_uuid_v7()", &[], ExecuteOptions::default())
            .await?;
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

    async fn detect_file_changes_for_pending_writes_by_statement_with_backend(
        &self,
        backend: &dyn LixBackend,
        writes_by_statement: &[Vec<crate::filesystem::pending_file_writes::PendingFileWrite>],
        allow_plugin_cache: bool,
    ) -> Result<Vec<Vec<crate::plugin::runtime::DetectedFileChange>>, LixError> {
        let Some(runtime) = self.wasm_runtime.as_ref() else {
            return Ok(vec![Vec::new(); writes_by_statement.len()]);
        };
        let installed_plugins = self
            .load_installed_plugins_with_backend(backend, allow_plugin_cache)
            .await?;
        if installed_plugins.is_empty() {
            return Ok(vec![Vec::new(); writes_by_statement.len()]);
        }

        let mut loaded_instances = {
            self.plugin_component_cache
                .lock()
                .map_err(|_| LixError {
                    message: "plugin component cache lock poisoned".to_string(),
                })?
                .clone()
        };
        let mut detected_by_statement = Vec::with_capacity(writes_by_statement.len());
        for writes in writes_by_statement {
            if writes.is_empty() {
                detected_by_statement.push(Vec::new());
                continue;
            }

            let requests = writes
                .iter()
                .map(|write| crate::plugin::runtime::FileChangeDetectionRequest {
                    file_id: write.file_id.clone(),
                    version_id: write.version_id.clone(),
                    before_path: write.before_path.clone(),
                    path: write.path.clone(),
                    before_data: write.before_data.clone(),
                    after_data: write.after_data.clone(),
                })
                .collect::<Vec<_>>();
            let detected = crate::plugin::runtime::detect_file_changes_with_plugins_with_cache(
                backend,
                runtime.as_ref(),
                &requests,
                &installed_plugins,
                &mut loaded_instances,
            )
            .await
            .map_err(|error| LixError {
                message: format!("file detect stage failed: {}", error.message),
            })?;
            detected_by_statement.push(dedupe_detected_file_changes(&detected));
        }
        {
            let mut guard = self.plugin_component_cache.lock().map_err(|_| LixError {
                message: "plugin component cache lock poisoned".to_string(),
            })?;
            *guard = loaded_instances;
        }

        Ok(detected_by_statement)
    }

    async fn persist_detected_file_domain_changes(
        &self,
        changes: &[DetectedFileDomainChange],
    ) -> Result<(), LixError> {
        self.persist_detected_file_domain_changes_with_untracked(changes, false)
            .await
    }

    async fn persist_untracked_file_domain_changes(
        &self,
        changes: &[DetectedFileDomainChange],
    ) -> Result<(), LixError> {
        self.persist_detected_file_domain_changes_with_untracked(changes, true)
            .await
    }

    async fn persist_detected_file_domain_changes_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        changes: &[DetectedFileDomainChange],
    ) -> Result<(), LixError> {
        self.persist_detected_file_domain_changes_with_untracked_in_transaction(
            transaction,
            changes,
            false,
        )
        .await
    }

    async fn persist_untracked_file_domain_changes_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        changes: &[DetectedFileDomainChange],
    ) -> Result<(), LixError> {
        self.persist_detected_file_domain_changes_with_untracked_in_transaction(
            transaction,
            changes,
            true,
        )
        .await
    }

    async fn persist_detected_file_domain_changes_with_untracked(
        &self,
        changes: &[DetectedFileDomainChange],
        untracked: bool,
    ) -> Result<(), LixError> {
        let deduped_changes = dedupe_detected_file_domain_changes(changes);
        if deduped_changes.is_empty() {
            return Ok(());
        }

        let mut params = Vec::with_capacity(deduped_changes.len() * 10);
        let mut rows = Vec::with_capacity(deduped_changes.len());
        for (row_index, change) in deduped_changes.iter().enumerate() {
            let base = row_index * 10;
            rows.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4,
                base + 5,
                base + 6,
                base + 7,
                base + 8,
                base + 9,
                base + 10
            ));
            params.push(Value::Text(change.entity_id.clone()));
            params.push(Value::Text(change.schema_key.clone()));
            params.push(Value::Text(change.file_id.clone()));
            params.push(Value::Text(change.version_id.clone()));
            params.push(Value::Text(change.plugin_key.clone()));
            params.push(match &change.snapshot_content {
                Some(snapshot_content) => Value::Text(snapshot_content.clone()),
                None => Value::Null,
            });
            params.push(Value::Text(change.schema_version.clone()));
            params.push(match &change.metadata {
                Some(metadata) => Value::Text(metadata.clone()),
                None => Value::Null,
            });
            params.push(match &change.writer_key {
                Some(writer_key) => Value::Text(writer_key.clone()),
                None => Value::Null,
            });
            params.push(Value::Integer(if untracked { 1 } else { 0 }));
        }

        let sql = format!(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, metadata, writer_key, untracked\
             ) VALUES {}",
            rows.join(", ")
        );
        Box::pin(self.execute(&sql, &params, ExecuteOptions::default())).await?;

        Ok(())
    }

    async fn persist_detected_file_domain_changes_with_untracked_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        changes: &[DetectedFileDomainChange],
        untracked: bool,
    ) -> Result<(), LixError> {
        let deduped_changes = dedupe_detected_file_domain_changes(changes);
        if deduped_changes.is_empty() {
            return Ok(());
        }

        let mut params = Vec::with_capacity(deduped_changes.len() * 10);
        let mut rows = Vec::with_capacity(deduped_changes.len());
        for (row_index, change) in deduped_changes.iter().enumerate() {
            let base = row_index * 10;
            rows.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4,
                base + 5,
                base + 6,
                base + 7,
                base + 8,
                base + 9,
                base + 10
            ));
            params.push(Value::Text(change.entity_id.clone()));
            params.push(Value::Text(change.schema_key.clone()));
            params.push(Value::Text(change.file_id.clone()));
            params.push(Value::Text(change.version_id.clone()));
            params.push(Value::Text(change.plugin_key.clone()));
            params.push(match &change.snapshot_content {
                Some(snapshot_content) => Value::Text(snapshot_content.clone()),
                None => Value::Null,
            });
            params.push(Value::Text(change.schema_version.clone()));
            params.push(match &change.metadata {
                Some(metadata) => Value::Text(metadata.clone()),
                None => Value::Null,
            });
            params.push(match &change.writer_key {
                Some(writer_key) => Value::Text(writer_key.clone()),
                None => Value::Null,
            });
            params.push(Value::Integer(if untracked { 1 } else { 0 }));
        }

        let sql = format!(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, metadata, writer_key, untracked\
             ) VALUES {}",
            rows.join(", ")
        );
        let backend = TransactionBackendAdapter::new(transaction);
        let output = preprocess_sql(&backend, &self.cel_evaluator, &sql, &params).await?;
        backend.execute(&output.sql, &output.params).await?;

        Ok(())
    }

    async fn persist_pending_file_data_updates(
        &self,
        writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
    ) -> Result<(), LixError> {
        let mut latest_by_key: BTreeMap<(String, String), usize> = BTreeMap::new();
        for (index, write) in writes.iter().enumerate() {
            if !write.data_is_authoritative {
                continue;
            }
            latest_by_key.insert((write.file_id.clone(), write.version_id.clone()), index);
        }

        for index in latest_by_key.into_values() {
            let write = &writes[index];
            self.backend
                .execute(
                    "INSERT INTO lix_internal_file_data_cache (file_id, version_id, data) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT (file_id, version_id) DO UPDATE SET \
                     data = EXCLUDED.data",
                    &[
                        Value::Text(write.file_id.clone()),
                        Value::Text(write.version_id.clone()),
                        Value::Blob(write.after_data.clone()),
                    ],
                )
                .await?;
        }

        Ok(())
    }

    async fn persist_pending_file_data_updates_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
    ) -> Result<(), LixError> {
        let mut latest_by_key: BTreeMap<(String, String), usize> = BTreeMap::new();
        for (index, write) in writes.iter().enumerate() {
            if !write.data_is_authoritative {
                continue;
            }
            latest_by_key.insert((write.file_id.clone(), write.version_id.clone()), index);
        }

        for index in latest_by_key.into_values() {
            let write = &writes[index];
            transaction
                .execute(
                    "INSERT INTO lix_internal_file_data_cache (file_id, version_id, data) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT (file_id, version_id) DO UPDATE SET \
                     data = EXCLUDED.data",
                    &[
                        Value::Text(write.file_id.clone()),
                        Value::Text(write.version_id.clone()),
                        Value::Blob(write.after_data.clone()),
                    ],
                )
                .await?;
        }

        Ok(())
    }

    async fn persist_pending_file_path_updates(
        &self,
        writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
    ) -> Result<(), LixError> {
        let mut latest_by_key: BTreeMap<(String, String), usize> = BTreeMap::new();
        for (index, write) in writes.iter().enumerate() {
            latest_by_key.insert((write.file_id.clone(), write.version_id.clone()), index);
        }

        for index in latest_by_key.into_values() {
            let write = &writes[index];
            let Some((name, extension)) = file_name_and_extension_from_path(&write.path) else {
                continue;
            };
            self.backend
                .execute(
                    "INSERT INTO lix_internal_file_path_cache \
                     (file_id, version_id, directory_id, name, extension, path) \
                     VALUES ($1, $2, NULL, $3, $4, $5) \
                     ON CONFLICT (file_id, version_id) DO UPDATE SET \
                     directory_id = EXCLUDED.directory_id, \
                     name = EXCLUDED.name, \
                     extension = EXCLUDED.extension, \
                     path = EXCLUDED.path",
                    &[
                        Value::Text(write.file_id.clone()),
                        Value::Text(write.version_id.clone()),
                        Value::Text(name),
                        match extension {
                            Some(value) => Value::Text(value),
                            None => Value::Null,
                        },
                        Value::Text(write.path.clone()),
                    ],
                )
                .await?;
        }
        Ok(())
    }

    async fn persist_pending_file_path_updates_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
    ) -> Result<(), LixError> {
        let mut latest_by_key: BTreeMap<(String, String), usize> = BTreeMap::new();
        for (index, write) in writes.iter().enumerate() {
            latest_by_key.insert((write.file_id.clone(), write.version_id.clone()), index);
        }

        for index in latest_by_key.into_values() {
            let write = &writes[index];
            let Some((name, extension)) = file_name_and_extension_from_path(&write.path) else {
                continue;
            };
            transaction
                .execute(
                    "INSERT INTO lix_internal_file_path_cache \
                     (file_id, version_id, directory_id, name, extension, path) \
                     VALUES ($1, $2, NULL, $3, $4, $5) \
                     ON CONFLICT (file_id, version_id) DO UPDATE SET \
                     directory_id = EXCLUDED.directory_id, \
                     name = EXCLUDED.name, \
                     extension = EXCLUDED.extension, \
                     path = EXCLUDED.path",
                    &[
                        Value::Text(write.file_id.clone()),
                        Value::Text(write.version_id.clone()),
                        Value::Text(name),
                        match extension {
                            Some(value) => Value::Text(value),
                            None => Value::Null,
                        },
                        Value::Text(write.path.clone()),
                    ],
                )
                .await?;
        }
        Ok(())
    }

    async fn invalidate_file_data_cache_entries(
        &self,
        targets: &BTreeSet<(String, String)>,
    ) -> Result<(), LixError> {
        if targets.is_empty() {
            return Ok(());
        }

        const PAIRS_PER_CHUNK: usize = 200;
        let keys = targets.iter().cloned().collect::<Vec<_>>();

        for chunk in keys.chunks(PAIRS_PER_CHUNK) {
            let mut params = Vec::with_capacity(chunk.len() * 2);
            let mut predicates = Vec::with_capacity(chunk.len());
            for (index, (file_id, version_id)) in chunk.iter().enumerate() {
                let file_param = index * 2 + 1;
                let version_param = file_param + 1;
                predicates.push(format!(
                    "(file_id = ${file_param} AND version_id = ${version_param})"
                ));
                params.push(Value::Text(file_id.clone()));
                params.push(Value::Text(version_id.clone()));
            }

            self.backend
                .execute(
                    &format!(
                        "DELETE FROM lix_internal_file_data_cache \
                         WHERE {}",
                        predicates.join(" OR ")
                    ),
                    &params,
                )
                .await?;
        }
        Ok(())
    }

    async fn invalidate_file_data_cache_entries_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        targets: &BTreeSet<(String, String)>,
    ) -> Result<(), LixError> {
        if targets.is_empty() {
            return Ok(());
        }

        const PAIRS_PER_CHUNK: usize = 200;
        let keys = targets.iter().cloned().collect::<Vec<_>>();

        for chunk in keys.chunks(PAIRS_PER_CHUNK) {
            let mut params = Vec::with_capacity(chunk.len() * 2);
            let mut predicates = Vec::with_capacity(chunk.len());
            for (index, (file_id, version_id)) in chunk.iter().enumerate() {
                let file_param = index * 2 + 1;
                let version_param = file_param + 1;
                predicates.push(format!(
                    "(file_id = ${file_param} AND version_id = ${version_param})"
                ));
                params.push(Value::Text(file_id.clone()));
                params.push(Value::Text(version_id.clone()));
            }

            transaction
                .execute(
                    &format!(
                        "DELETE FROM lix_internal_file_data_cache \
                         WHERE {}",
                        predicates.join(" OR ")
                    ),
                    &params,
                )
                .await?;
        }
        Ok(())
    }

    async fn invalidate_file_path_cache_entries(
        &self,
        targets: &BTreeSet<(String, String)>,
    ) -> Result<(), LixError> {
        if targets.is_empty() {
            return Ok(());
        }

        const PAIRS_PER_CHUNK: usize = 200;
        let keys = targets.iter().cloned().collect::<Vec<_>>();

        for chunk in keys.chunks(PAIRS_PER_CHUNK) {
            let mut params = Vec::with_capacity(chunk.len() * 2);
            let mut predicates = Vec::with_capacity(chunk.len());
            for (index, (file_id, version_id)) in chunk.iter().enumerate() {
                let file_param = index * 2 + 1;
                let version_param = file_param + 1;
                predicates.push(format!(
                    "(file_id = ${file_param} AND version_id = ${version_param})"
                ));
                params.push(Value::Text(file_id.clone()));
                params.push(Value::Text(version_id.clone()));
            }

            self.backend
                .execute(
                    &format!(
                        "DELETE FROM lix_internal_file_path_cache \
                         WHERE {}",
                        predicates.join(" OR ")
                    ),
                    &params,
                )
                .await?;
        }
        Ok(())
    }

    async fn invalidate_file_path_cache_entries_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        targets: &BTreeSet<(String, String)>,
    ) -> Result<(), LixError> {
        if targets.is_empty() {
            return Ok(());
        }

        const PAIRS_PER_CHUNK: usize = 200;
        let keys = targets.iter().cloned().collect::<Vec<_>>();

        for chunk in keys.chunks(PAIRS_PER_CHUNK) {
            let mut params = Vec::with_capacity(chunk.len() * 2);
            let mut predicates = Vec::with_capacity(chunk.len());
            for (index, (file_id, version_id)) in chunk.iter().enumerate() {
                let file_param = index * 2 + 1;
                let version_param = file_param + 1;
                predicates.push(format!(
                    "(file_id = ${file_param} AND version_id = ${version_param})"
                ));
                params.push(Value::Text(file_id.clone()));
                params.push(Value::Text(version_id.clone()));
            }

            transaction
                .execute(
                    &format!(
                        "DELETE FROM lix_internal_file_path_cache \
                         WHERE {}",
                        predicates.join(" OR ")
                    ),
                    &params,
                )
                .await?;
        }
        Ok(())
    }

    async fn refresh_file_data_for_versions(
        &self,
        targets: BTreeSet<(String, String)>,
    ) -> Result<(), LixError> {
        let versions = targets
            .into_iter()
            .map(|(_, version_id)| version_id)
            .collect::<BTreeSet<_>>();
        if versions.is_empty() {
            return Ok(());
        }

        self.materialize(&MaterializationRequest {
            scope: MaterializationScope::Versions(versions),
            debug: MaterializationDebugMode::Off,
            debug_row_limit: 1,
        })
        .await?;
        Ok(())
    }

    fn set_active_version_id(&self, version_id: String) {
        let mut guard = self.active_version_id.write().unwrap();
        if *guard == version_id {
            return;
        }
        *guard = version_id;
    }

    async fn execute_multi_statement_sequential_with_options(
        &self,
        sql: &str,
        params: &[Value],
        options: &ExecuteOptions,
    ) -> Result<QueryResult, LixError> {
        let statements = parse_sql_statements(sql)?;
        let mut last_result = QueryResult { rows: Vec::new() };
        for statement in statements {
            last_result =
                Box::pin(self.execute(&statement.to_string(), params, options.clone())).await?;
        }
        Ok(last_result)
    }

    async fn execute_multi_statement_sequential_with_options_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        sql: &str,
        params: &[Value],
        options: &ExecuteOptions,
        active_version_id: &mut String,
    ) -> Result<QueryResult, LixError> {
        let statements = parse_sql_statements(sql)?;
        let mut last_result = QueryResult { rows: Vec::new() };
        for statement in statements {
            last_result = Box::pin(self.execute_with_options_in_transaction(
                transaction,
                &statement.to_string(),
                params,
                options,
                active_version_id,
                None,
                false,
            ))
            .await?;
        }
        Ok(last_result)
    }
}

fn should_sequentialize_postprocess_multi_statement(
    sql: &str,
    params: &[Value],
    error: &LixError,
) -> bool {
    if !params.is_empty() || error.message != "postprocess rewrites require a single statement" {
        return false;
    }

    let Ok(statements) = parse_sql_statements(sql) else {
        return false;
    };
    if statements.len() <= 1 {
        return false;
    }

    !statements.iter().any(|statement| {
        matches!(
            statement,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
        )
    })
}

fn file_name_and_extension_from_path(path: &str) -> Option<(String, Option<String>)> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    let file_name = trimmed.rsplit('/').next()?;
    if file_name.is_empty() {
        return None;
    }
    let last_dot = file_name.rfind('.');
    let (name, extension) = match last_dot {
        Some(index) if index > 0 => {
            let name = file_name[..index].to_string();
            let extension = file_name[index + 1..].to_string();
            let extension = if extension.is_empty() {
                None
            } else {
                Some(extension)
            };
            (name, extension)
        }
        _ => (file_name.to_string(), None),
    };
    if name.is_empty() {
        return None;
    }
    Some((name, extension))
}

fn extract_explicit_transaction_script(
    sql: &str,
    params: &[Value],
) -> Result<Option<Vec<Statement>>, LixError> {
    if !params.is_empty() {
        return Ok(None);
    }

    let statements = parse_sql_statements(sql)?;
    if statements.len() < 2 {
        return Ok(None);
    }

    let first_is_begin = matches!(statements.first(), Some(Statement::StartTransaction { .. }));
    let last_is_commit = matches!(statements.last(), Some(Statement::Commit { .. }));
    if !first_is_begin || !last_is_commit {
        return Ok(None);
    }

    let middle = &statements[1..statements.len() - 1];
    if middle.iter().any(|statement| {
        matches!(
            statement,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
        )
    }) {
        return Err(LixError {
            message:
                "nested transaction statements are not supported inside BEGIN ... COMMIT scripts"
                    .to_string(),
        });
    }

    Ok(Some(middle.to_vec()))
}

#[derive(Debug, Clone)]
struct LixFileWriteRow {
    id: String,
    path_sql: String,
    data_sql: String,
}

fn coalesce_lix_file_transaction_statements(
    statements: &[Statement],
    dialect: Option<SqlDialect>,
) -> Option<Vec<String>> {
    if statements.is_empty() {
        return Some(Vec::new());
    }
    if !matches!(dialect, Some(SqlDialect::Sqlite)) {
        return None;
    }

    let mut delete_ids = Vec::new();
    let mut insert_rows = Vec::new();
    let mut update_rows = Vec::new();
    let mut seen_ids = BTreeSet::new();
    let mut saw_update = false;

    for statement in statements {
        if let Some(ids) = parse_lix_file_delete_ids(statement) {
            if saw_update {
                return None;
            }
            for id in ids {
                if !seen_ids.insert(id.clone()) {
                    return None;
                }
                delete_ids.push(id);
            }
            continue;
        }
        if let Some(rows) = parse_lix_file_insert_rows(statement) {
            if saw_update {
                return None;
            }
            for row in rows {
                if !seen_ids.insert(row.id.clone()) {
                    return None;
                }
                insert_rows.push(row);
            }
            continue;
        }
        if let Some(row) = parse_lix_file_update_row(statement) {
            if !seen_ids.insert(row.id.clone()) {
                return None;
            }
            saw_update = true;
            update_rows.push(row);
            continue;
        }
        return None;
    }

    let mut rewritten = Vec::new();

    if !delete_ids.is_empty() {
        let id_list = delete_ids
            .iter()
            .map(|id| format!("'{}'", escape_sql_string(id)))
            .collect::<Vec<_>>()
            .join(", ");
        rewritten.push(format!("DELETE FROM lix_file WHERE id IN ({id_list})"));
    }

    if !insert_rows.is_empty() {
        let values = insert_rows
            .iter()
            .map(|row| {
                format!(
                    "('{}', {}, {})",
                    escape_sql_string(&row.id),
                    row.path_sql,
                    row.data_sql
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        rewritten.push(format!(
            "INSERT INTO lix_file (id, path, data) VALUES {values}"
        ));
    }

    if !update_rows.is_empty() {
        let path_cases = update_rows
            .iter()
            .map(|row| {
                format!(
                    "WHEN '{}' THEN {}",
                    escape_sql_string(&row.id),
                    row.path_sql
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        let data_cases = update_rows
            .iter()
            .map(|row| {
                format!(
                    "WHEN '{}' THEN {}",
                    escape_sql_string(&row.id),
                    row.data_sql
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        let id_list = update_rows
            .iter()
            .map(|row| format!("'{}'", escape_sql_string(&row.id)))
            .collect::<Vec<_>>()
            .join(", ");
        rewritten.push(format!(
            "UPDATE lix_file \
             SET path = CASE id {path_cases} ELSE path END, \
                 data = CASE id {data_cases} ELSE data END \
             WHERE id IN ({id_list})"
        ));
    }

    Some(rewritten)
}

fn parse_lix_file_insert_rows(statement: &Statement) -> Option<Vec<LixFileWriteRow>> {
    let Statement::Insert(insert) = statement else {
        return None;
    };
    if !table_object_targets_table_name(&insert.table, "lix_file") {
        return None;
    }
    if insert.columns.is_empty() {
        return None;
    }
    if !insert.assignments.is_empty() || insert.on.is_some() || insert.returning.is_some() {
        return None;
    }
    let source = insert.source.as_deref()?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        return None;
    };

    if insert.columns.len() != 3 {
        return None;
    }

    let mut id_index = None;
    let mut path_index = None;
    let mut data_index = None;
    for (index, column) in insert.columns.iter().enumerate() {
        if column.value.eq_ignore_ascii_case("id") {
            if id_index.replace(index).is_some() {
                return None;
            }
        } else if column.value.eq_ignore_ascii_case("path") {
            if path_index.replace(index).is_some() {
                return None;
            }
        } else if column.value.eq_ignore_ascii_case("data") {
            if data_index.replace(index).is_some() {
                return None;
            }
        } else {
            // Coalescer only supports replay-shape inserts with exactly id/path/data.
            // Any extra column must fall back to normal statement execution.
            return None;
        }
    }
    let id_index = id_index?;
    let path_index = path_index?;
    let data_index = data_index?;

    let mut rows = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        let id = expr_as_string_literal(row.get(id_index)?)?;
        let path_sql = row.get(path_index)?.to_string();
        let data_sql = row.get(data_index)?.to_string();
        rows.push(LixFileWriteRow {
            id,
            path_sql,
            data_sql,
        });
    }
    Some(rows)
}

fn parse_lix_file_update_row(statement: &Statement) -> Option<LixFileWriteRow> {
    let Statement::Update(update) = statement else {
        return None;
    };
    if !table_with_joins_targets_table_name(&update.table, "lix_file") {
        return None;
    }
    if update.from.is_some() || update.returning.is_some() || update.limit.is_some() {
        return None;
    }

    let mut path_sql = None;
    let mut data_sql = None;
    for assignment in &update.assignments {
        let AssignmentTarget::ColumnName(target) = &assignment.target else {
            return None;
        };
        let column = object_name_last_ident_value(target)?;
        if column.eq_ignore_ascii_case("path") {
            path_sql = Some(assignment.value.to_string());
        } else if column.eq_ignore_ascii_case("data") {
            data_sql = Some(assignment.value.to_string());
        } else {
            return None;
        }
    }

    let id = parse_id_eq_selection(update.selection.as_ref()?)?;
    Some(LixFileWriteRow {
        id,
        path_sql: path_sql?,
        data_sql: data_sql?,
    })
}

fn parse_lix_file_delete_ids(statement: &Statement) -> Option<Vec<String>> {
    let Statement::Delete(delete) = statement else {
        return None;
    };
    if !delete.tables.is_empty()
        || delete.using.is_some()
        || delete.returning.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
    {
        return None;
    }

    let from = match &delete.from {
        FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
    };
    if from.len() != 1 || !table_with_joins_targets_table_name(&from[0], "lix_file") {
        return None;
    }

    parse_id_selection(delete.selection.as_ref()?)
}

fn parse_id_selection(selection: &Expr) -> Option<Vec<String>> {
    match selection {
        Expr::InList {
            expr,
            list,
            negated: false,
        } if expr_is_column_name(expr, "id") => list.iter().map(expr_as_string_literal).collect(),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if expr_is_column_name(left, "id") {
                return Some(vec![expr_as_string_literal(right)?]);
            }
            if expr_is_column_name(right, "id") {
                return Some(vec![expr_as_string_literal(left)?]);
            }
            None
        }
        _ => None,
    }
}

fn parse_id_eq_selection(selection: &Expr) -> Option<String> {
    let ids = parse_id_selection(selection)?;
    if ids.len() == 1 {
        return ids.into_iter().next();
    }
    None
}

fn expr_as_string_literal(expr: &Expr) -> Option<String> {
    let Expr::Value(value) = expr else {
        return None;
    };
    match &value.value {
        sqlparser::ast::Value::SingleQuotedString(text)
        | sqlparser::ast::Value::DoubleQuotedString(text)
        | sqlparser::ast::Value::NationalStringLiteral(text)
        | sqlparser::ast::Value::EscapedStringLiteral(text)
        | sqlparser::ast::Value::UnicodeStringLiteral(text) => Some(text.clone()),
        _ => None,
    }
}

fn expr_is_column_name(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(name),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case(name)),
        Expr::Nested(inner) => expr_is_column_name(inner, name),
        _ => false,
    }
}

fn object_name_last_ident_value(name: &ObjectName) -> Option<&str> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.as_str())
}

fn collapse_pending_file_writes_for_transaction(
    writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
) -> Vec<crate::filesystem::pending_file_writes::PendingFileWrite> {
    let mut collapsed =
        Vec::<crate::filesystem::pending_file_writes::PendingFileWrite>::with_capacity(
            writes.len(),
        );
    let mut index_by_key = BTreeMap::<(String, String), usize>::new();

    for write in writes {
        let key = (write.file_id.clone(), write.version_id.clone());
        if let Some(index) = index_by_key.get(&key).copied() {
            let existing = &mut collapsed[index];
            existing.path = write.path.clone();
            existing.after_data = write.after_data.clone();
            existing.data_is_authoritative =
                existing.data_is_authoritative || write.data_is_authoritative;
            if existing.before_path.is_none() {
                existing.before_path = write.before_path.clone();
            }
            if existing.before_data.is_none() {
                existing.before_data = write.before_data.clone();
            }
            continue;
        }

        index_by_key.insert(key, collapsed.len());
        collapsed.push(write.clone());
    }

    collapsed
}

fn direct_state_file_cache_refresh_targets(
    mutations: &[MutationRow],
) -> BTreeSet<(String, String)> {
    mutations
        .iter()
        .filter(|mutation| !mutation.untracked)
        .filter(|mutation| mutation.file_id != "lix")
        .filter(|mutation| mutation.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY)
        .filter(|mutation| mutation.schema_key != DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        .map(|mutation| (mutation.file_id.clone(), mutation.version_id.clone()))
        .collect()
}

fn file_descriptor_cache_eviction_targets(mutations: &[MutationRow]) -> BTreeSet<(String, String)> {
    mutations
        .iter()
        .filter(|mutation| !mutation.untracked)
        .filter(|mutation| mutation.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
        .filter(|mutation| {
            matches!(mutation.operation, MutationOperation::Delete)
                || mutation.snapshot_content.is_none()
        })
        .map(|mutation| (mutation.entity_id.clone(), mutation.version_id.clone()))
        .collect()
}

fn collect_postprocess_file_cache_targets(
    rows: &[Vec<Value>],
    schema_key: &str,
) -> Result<BTreeSet<(String, String)>, LixError> {
    if schema_key == FILE_DESCRIPTOR_SCHEMA_KEY || schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY {
        return Ok(BTreeSet::new());
    }

    let mut targets = BTreeSet::new();
    for row in rows {
        let Some(file_id) = row.get(1) else {
            return Err(LixError {
                message: "postprocess file cache refresh expected file_id column".to_string(),
            });
        };
        let Some(version_id) = row.get(2) else {
            return Err(LixError {
                message: "postprocess file cache refresh expected version_id column".to_string(),
            });
        };
        let Value::Text(file_id) = file_id else {
            return Err(LixError {
                message: format!(
                    "postprocess file cache refresh expected text file_id, got {file_id:?}"
                ),
            });
        };
        let Value::Text(version_id) = version_id else {
            return Err(LixError {
                message: format!(
                    "postprocess file cache refresh expected text version_id, got {version_id:?}"
                ),
            });
        };
        if file_id == "lix" {
            continue;
        }
        targets.insert((file_id.clone(), version_id.clone()));
    }

    Ok(targets)
}

trait DedupableDetectedFileChange {
    fn dedupe_key(&self) -> (&str, &str, &str, &str);
}

impl DedupableDetectedFileChange for crate::plugin::runtime::DetectedFileChange {
    fn dedupe_key(&self) -> (&str, &str, &str, &str) {
        (
            &self.file_id,
            &self.version_id,
            &self.schema_key,
            &self.entity_id,
        )
    }
}

impl DedupableDetectedFileChange for DetectedFileDomainChange {
    fn dedupe_key(&self) -> (&str, &str, &str, &str) {
        (
            &self.file_id,
            &self.version_id,
            &self.schema_key,
            &self.entity_id,
        )
    }
}

fn dedupe_detected_changes<T>(changes: &[T]) -> Vec<T>
where
    T: DedupableDetectedFileChange + Clone,
{
    let mut latest_by_key: BTreeMap<(&str, &str, &str, &str), usize> = BTreeMap::new();
    for (index, change) in changes.iter().enumerate() {
        latest_by_key.insert(change.dedupe_key(), index);
    }

    let mut ordered_indexes = latest_by_key.into_values().collect::<Vec<_>>();
    ordered_indexes.sort_unstable();
    ordered_indexes
        .into_iter()
        .filter_map(|index| changes.get(index).cloned())
        .collect()
}

fn dedupe_detected_file_changes(
    changes: &[crate::plugin::runtime::DetectedFileChange],
) -> Vec<crate::plugin::runtime::DetectedFileChange> {
    dedupe_detected_changes(changes)
}

fn dedupe_detected_file_domain_changes(
    changes: &[DetectedFileDomainChange],
) -> Vec<DetectedFileDomainChange> {
    dedupe_detected_changes(changes)
}

fn detected_file_domain_changes_from_detected_file_changes(
    changes: &[crate::plugin::runtime::DetectedFileChange],
    writer_key: Option<&str>,
) -> Vec<DetectedFileDomainChange> {
    changes
        .iter()
        .map(|change| DetectedFileDomainChange {
            entity_id: change.entity_id.clone(),
            schema_key: change.schema_key.clone(),
            schema_version: change.schema_version.clone(),
            file_id: change.file_id.clone(),
            version_id: change.version_id.clone(),
            plugin_key: change.plugin_key.clone(),
            snapshot_content: change.snapshot_content.clone(),
            metadata: None,
            writer_key: writer_key.map(ToString::to_string),
        })
        .collect()
}

fn detected_file_domain_changes_with_writer_key(
    changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
) -> Vec<DetectedFileDomainChange> {
    changes
        .iter()
        .map(|change| {
            let mut next = change.clone();
            next.writer_key = writer_key.map(ToString::to_string);
            next
        })
        .collect()
}

async fn collect_filesystem_update_detected_file_domain_changes(
    backend: &dyn LixBackend,
    sql: &str,
    params: &[Value],
) -> Result<FilesystemUpdateDomainChangeCollection, LixError> {
    let statements = parse_sql_statements(sql)?;
    let mut placeholder_state = PlaceholderState::new();
    let mut tracked_changes_by_statement = Vec::with_capacity(statements.len());
    let mut untracked_changes = Vec::new();
    for statement in statements {
        match statement {
            Statement::Update(update) => {
                let side_effects =
                    crate::filesystem::mutation_rewrite::update_side_effects_with_backend(
                        backend,
                        &update,
                        params,
                        &mut placeholder_state,
                    )
                    .await?;
                let statement_tracked_changes =
                    dedupe_detected_file_domain_changes(&side_effects.tracked_directory_changes);
                tracked_changes_by_statement.push(statement_tracked_changes);
                untracked_changes.extend(side_effects.untracked_directory_changes);
            }
            other => {
                tracked_changes_by_statement.push(Vec::new());
                advance_placeholder_state_for_statement(
                    &other,
                    params,
                    backend.dialect(),
                    &mut placeholder_state,
                )?;
            }
        }
    }

    Ok(FilesystemUpdateDomainChangeCollection {
        untracked_changes: dedupe_detected_file_domain_changes(&untracked_changes),
        tracked_changes_by_statement,
    })
}

struct FilesystemUpdateDomainChangeCollection {
    untracked_changes: Vec<DetectedFileDomainChange>,
    tracked_changes_by_statement: Vec<Vec<DetectedFileDomainChange>>,
}

fn advance_placeholder_state_for_statement(
    statement: &Statement,
    params: &[Value],
    dialect: crate::backend::SqlDialect,
    placeholder_state: &mut PlaceholderState,
) -> Result<(), LixError> {
    let statement_sql = statement.to_string();
    let bound = bind_sql_with_state(&statement_sql, params, dialect, *placeholder_state).map_err(
        |error| LixError {
            message: format!(
                "filesystem side-effect placeholder binding failed for '{}': {}",
                statement_sql, error.message
            ),
        },
    )?;
    *placeholder_state = bound.state;
    Ok(())
}

fn should_refresh_file_cache_for_sql(sql: &str) -> bool {
    let Ok(statements) = parse_sql_statements(sql) else {
        return false;
    };
    statements
        .iter()
        .any(statement_targets_file_cache_refresh_table)
}

fn is_query_only_sql(sql: &str) -> bool {
    let Ok(statements) = parse_sql_statements(sql) else {
        return false;
    };
    !statements.is_empty()
        && statements
            .iter()
            .all(|statement| matches!(statement, Statement::Query(_)))
}

fn statement_targets_file_cache_refresh_table(statement: &Statement) -> bool {
    statement_targets_table_name(statement, "lix_state")
        || statement_targets_table_name(statement, "lix_state_by_version")
}

fn should_invalidate_installed_plugins_cache_for_sql(sql: &str) -> bool {
    let Ok(statements) = parse_sql_statements(sql) else {
        return false;
    };
    statements
        .iter()
        .any(|statement| statement_targets_table_name(statement, "lix_internal_plugin"))
}

fn statement_targets_table_name(statement: &Statement, table_name: &str) -> bool {
    match statement {
        Statement::Insert(insert) => table_object_targets_table_name(&insert.table, table_name),
        Statement::Update(update) => table_with_joins_targets_table_name(&update.table, table_name),
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
            };
            tables
                .iter()
                .any(|table| table_with_joins_targets_table_name(table, table_name))
        }
        _ => false,
    }
}

fn table_object_targets_table_name(table: &TableObject, table_name: &str) -> bool {
    let TableObject::TableName(name) = table else {
        return false;
    };
    object_name_targets_table_name(name, table_name)
}

fn table_with_joins_targets_table_name(table: &TableWithJoins, table_name: &str) -> bool {
    let TableFactor::Table { name, .. } = &table.relation else {
        return false;
    };
    object_name_targets_table_name(name, table_name)
}

fn object_name_targets_table_name(name: &ObjectName, table_name: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(table_name))
        .unwrap_or(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileReadMaterializationScope {
    ActiveVersionOnly,
    AllVersions,
}

fn file_read_materialization_scope_for_sql(sql: &str) -> Option<FileReadMaterializationScope> {
    let Ok(statements) = parse_sql_statements(sql) else {
        return None;
    };

    let mut scope = None;
    for statement in &statements {
        let Some(statement_scope) = file_read_materialization_scope_for_statement(statement) else {
            continue;
        };
        match statement_scope {
            FileReadMaterializationScope::AllVersions => {
                return Some(FileReadMaterializationScope::AllVersions);
            }
            FileReadMaterializationScope::ActiveVersionOnly => {
                scope.get_or_insert(FileReadMaterializationScope::ActiveVersionOnly);
            }
        }
    }
    scope
}

fn file_history_read_materialization_required_for_sql(sql: &str) -> bool {
    let Ok(statements) = parse_sql_statements(sql) else {
        return false;
    };
    statements
        .iter()
        .any(file_history_read_materialization_required_for_statement)
}

fn file_history_read_materialization_required_for_statement(statement: &Statement) -> bool {
    statement_reads_table_name(statement, "lix_file_history")
}

fn file_read_materialization_scope_for_statement(
    statement: &Statement,
) -> Option<FileReadMaterializationScope> {
    let mentions_by_version = statement_reads_table_name(statement, "lix_file_by_version");
    if mentions_by_version {
        return Some(FileReadMaterializationScope::AllVersions);
    }
    if statement_reads_table_name(statement, "lix_file") {
        return Some(FileReadMaterializationScope::ActiveVersionOnly);
    }
    None
}

fn statement_reads_table_name(statement: &Statement, table_name: &str) -> bool {
    match statement {
        Statement::Query(query) => query_mentions_table_name(query, table_name),
        Statement::Insert(insert) => insert
            .source
            .as_deref()
            .is_some_and(|query| query_mentions_table_name(query, table_name)),
        Statement::Update(update) => {
            let target_matches = table_with_joins_mentions_table_name(&update.table, table_name);
            (target_matches && update_references_data_column(update))
                || update.from.as_ref().is_some_and(|from| match from {
                    UpdateTableFromKind::BeforeSet(from) | UpdateTableFromKind::AfterSet(from) => {
                        from.iter()
                            .any(|table| table_with_joins_mentions_table_name(table, table_name))
                    }
                })
                || update
                    .selection
                    .as_ref()
                    .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
                || update
                    .assignments
                    .iter()
                    .any(|assignment| expr_mentions_table_name(&assignment.value, table_name))
        }
        Statement::Delete(delete) => {
            delete.using.as_ref().is_some_and(|tables| {
                tables
                    .iter()
                    .any(|table| table_with_joins_mentions_table_name(table, table_name))
            }) || delete
                .selection
                .as_ref()
                .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
        }
        _ => false,
    }
}

fn update_references_data_column(update: &Update) -> bool {
    update
        .selection
        .as_ref()
        .is_some_and(expr_references_data_column)
        || update
            .assignments
            .iter()
            .any(|assignment| expr_references_data_column(&assignment.value))
}

fn expr_references_data_column(expr: &Expr) -> bool {
    expr_references_column_name(
        expr,
        "data",
        ColumnReferenceOptions {
            include_from_derived_subqueries: true,
        },
    )
}

fn query_mentions_table_name(query: &Query, table_name: &str) -> bool {
    if query_set_expr_mentions_table_name(query.body.as_ref(), table_name) {
        return true;
    }

    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if query_mentions_table_name(&cte.query, table_name) {
                return true;
            }
        }
    }

    if query
        .order_by
        .as_ref()
        .is_some_and(|order_by| order_by_mentions_table_name(order_by, table_name))
    {
        return true;
    }

    if query
        .limit_clause
        .as_ref()
        .is_some_and(|limit_clause| limit_clause_mentions_table_name(limit_clause, table_name))
    {
        return true;
    }

    if query
        .fetch
        .as_ref()
        .and_then(|fetch| fetch.quantity.as_ref())
        .is_some_and(|quantity| expr_mentions_table_name(quantity, table_name))
    {
        return true;
    }

    false
}

fn query_set_expr_mentions_table_name(expr: &SetExpr, table_name: &str) -> bool {
    match expr {
        SetExpr::Select(select) => select_mentions_table_name(select, table_name),
        SetExpr::Query(query) => query_mentions_table_name(query, table_name),
        SetExpr::SetOperation { left, right, .. } => {
            query_set_expr_mentions_table_name(left.as_ref(), table_name)
                || query_set_expr_mentions_table_name(right.as_ref(), table_name)
        }
        SetExpr::Values(values) => values
            .rows
            .iter()
            .flatten()
            .any(|expr| expr_mentions_table_name(expr, table_name)),
        SetExpr::Insert(statement)
        | SetExpr::Update(statement)
        | SetExpr::Delete(statement)
        | SetExpr::Merge(statement) => statement_reads_table_name(statement, table_name),
        SetExpr::Table(table) => table
            .table_name
            .as_ref()
            .is_some_and(|name| name.eq_ignore_ascii_case(table_name)),
    }
}

fn select_mentions_table_name(select: &Select, table_name: &str) -> bool {
    if select
        .from
        .iter()
        .any(|table| table_with_joins_mentions_table_name(table, table_name))
    {
        return true;
    }

    if select
        .projection
        .iter()
        .any(|item| select_item_mentions_table_name(item, table_name))
    {
        return true;
    }

    if select
        .prewhere
        .as_ref()
        .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select
        .selection
        .as_ref()
        .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if group_by_expr_mentions_table_name(&select.group_by, table_name) {
        return true;
    }

    if select
        .cluster_by
        .iter()
        .any(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select
        .distribute_by
        .iter()
        .any(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select
        .sort_by
        .iter()
        .any(|order_by_expr| order_by_expr_mentions_table_name(order_by_expr, table_name))
    {
        return true;
    }

    if select
        .having
        .as_ref()
        .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select
        .qualify
        .as_ref()
        .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select.connect_by.as_ref().is_some_and(|connect_by| {
        expr_mentions_table_name(&connect_by.condition, table_name)
            || connect_by
                .relationships
                .iter()
                .any(|expr| expr_mentions_table_name(expr, table_name))
    }) {
        return true;
    }

    false
}

fn table_with_joins_mentions_table_name(table: &TableWithJoins, table_name: &str) -> bool {
    if table_factor_mentions_table_name(&table.relation, table_name) {
        return true;
    }

    table.joins.iter().any(|join| {
        table_factor_mentions_table_name(&join.relation, table_name)
            || join_operator_mentions_table_name(&join.join_operator, table_name)
    })
}

fn table_factor_mentions_table_name(table: &TableFactor, table_name: &str) -> bool {
    match table {
        TableFactor::Table { name, .. } => object_name_matches_table_name(name, table_name),
        TableFactor::Derived { subquery, .. } => query_mentions_table_name(subquery, table_name),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => table_with_joins_mentions_table_name(table_with_joins, table_name),
        _ => false,
    }
}

fn select_item_mentions_table_name(item: &sqlparser::ast::SelectItem, table_name: &str) -> bool {
    match item {
        sqlparser::ast::SelectItem::UnnamedExpr(expr)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
            expr_mentions_table_name(expr, table_name)
        }
        sqlparser::ast::SelectItem::QualifiedWildcard(
            sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
            _,
        ) => expr_mentions_table_name(expr, table_name),
        _ => false,
    }
}

fn group_by_expr_mentions_table_name(
    group_by: &sqlparser::ast::GroupByExpr,
    table_name: &str,
) -> bool {
    match group_by {
        sqlparser::ast::GroupByExpr::All(_) => false,
        sqlparser::ast::GroupByExpr::Expressions(expressions, _) => expressions
            .iter()
            .any(|expr| expr_mentions_table_name(expr, table_name)),
    }
}

fn order_by_mentions_table_name(order_by: &sqlparser::ast::OrderBy, table_name: &str) -> bool {
    match &order_by.kind {
        sqlparser::ast::OrderByKind::All(_) => false,
        sqlparser::ast::OrderByKind::Expressions(expressions) => expressions
            .iter()
            .any(|expr| order_by_expr_mentions_table_name(expr, table_name)),
    }
}

fn order_by_expr_mentions_table_name(
    order_by_expr: &sqlparser::ast::OrderByExpr,
    table_name: &str,
) -> bool {
    if expr_mentions_table_name(&order_by_expr.expr, table_name) {
        return true;
    }

    order_by_expr.with_fill.as_ref().is_some_and(|with_fill| {
        with_fill
            .from
            .as_ref()
            .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
            || with_fill
                .to
                .as_ref()
                .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
            || with_fill
                .step
                .as_ref()
                .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    })
}

fn limit_clause_mentions_table_name(
    limit_clause: &sqlparser::ast::LimitClause,
    table_name: &str,
) -> bool {
    match limit_clause {
        sqlparser::ast::LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            limit
                .as_ref()
                .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
                || offset
                    .as_ref()
                    .is_some_and(|offset| expr_mentions_table_name(&offset.value, table_name))
                || limit_by
                    .iter()
                    .any(|expr| expr_mentions_table_name(expr, table_name))
        }
        sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit } => {
            expr_mentions_table_name(offset, table_name)
                || expr_mentions_table_name(limit, table_name)
        }
    }
}

fn join_operator_mentions_table_name(
    join_operator: &sqlparser::ast::JoinOperator,
    table_name: &str,
) -> bool {
    let (match_condition, constraint) = match join_operator {
        sqlparser::ast::JoinOperator::AsOf {
            match_condition,
            constraint,
        } => (Some(match_condition), Some(constraint)),
        sqlparser::ast::JoinOperator::Join(constraint)
        | sqlparser::ast::JoinOperator::Inner(constraint)
        | sqlparser::ast::JoinOperator::Left(constraint)
        | sqlparser::ast::JoinOperator::LeftOuter(constraint)
        | sqlparser::ast::JoinOperator::Right(constraint)
        | sqlparser::ast::JoinOperator::RightOuter(constraint)
        | sqlparser::ast::JoinOperator::FullOuter(constraint)
        | sqlparser::ast::JoinOperator::CrossJoin(constraint)
        | sqlparser::ast::JoinOperator::Semi(constraint)
        | sqlparser::ast::JoinOperator::LeftSemi(constraint)
        | sqlparser::ast::JoinOperator::RightSemi(constraint)
        | sqlparser::ast::JoinOperator::Anti(constraint)
        | sqlparser::ast::JoinOperator::LeftAnti(constraint)
        | sqlparser::ast::JoinOperator::RightAnti(constraint)
        | sqlparser::ast::JoinOperator::StraightJoin(constraint) => (None, Some(constraint)),
        sqlparser::ast::JoinOperator::CrossApply | sqlparser::ast::JoinOperator::OuterApply => {
            (None, None)
        }
    };

    match_condition.is_some_and(|expr| expr_mentions_table_name(expr, table_name))
        || constraint
            .is_some_and(|constraint| join_constraint_mentions_table_name(constraint, table_name))
}

fn join_constraint_mentions_table_name(
    constraint: &sqlparser::ast::JoinConstraint,
    table_name: &str,
) -> bool {
    match constraint {
        sqlparser::ast::JoinConstraint::On(expr) => expr_mentions_table_name(expr, table_name),
        _ => false,
    }
}

fn expr_mentions_table_name(expr: &Expr, table_name: &str) -> bool {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            expr_mentions_table_name(left, table_name)
                || expr_mentions_table_name(right, table_name)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => expr_mentions_table_name(expr, table_name),
        Expr::InList { expr, list, .. } => {
            expr_mentions_table_name(expr, table_name)
                || list
                    .iter()
                    .any(|item| expr_mentions_table_name(item, table_name))
        }
        Expr::InSubquery { expr, subquery, .. } => {
            expr_mentions_table_name(expr, table_name)
                || query_mentions_table_name(subquery, table_name)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_mentions_table_name(expr, table_name)
                || expr_mentions_table_name(low, table_name)
                || expr_mentions_table_name(high, table_name)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            expr_mentions_table_name(expr, table_name)
                || expr_mentions_table_name(pattern, table_name)
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            expr_mentions_table_name(expr, table_name)
                || expr_mentions_table_name(array_expr, table_name)
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            expr_mentions_table_name(left, table_name)
                || expr_mentions_table_name(right, table_name)
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => {
            query_mentions_table_name(subquery, table_name)
        }
        Expr::Function(function) => match &function.args {
            sqlparser::ast::FunctionArguments::List(list) => {
                list.args.iter().any(|arg| match arg {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(expr),
                    ) => expr_mentions_table_name(expr, table_name),
                    sqlparser::ast::FunctionArg::Named { arg, .. }
                    | sqlparser::ast::FunctionArg::ExprNamed { arg, .. } => match arg {
                        sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                            expr_mentions_table_name(expr, table_name)
                        }
                        _ => false,
                    },
                    _ => false,
                })
            }
            _ => false,
        },
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_some_and(|operand| expr_mentions_table_name(operand, table_name))
                || conditions.iter().any(|condition| {
                    expr_mentions_table_name(&condition.condition, table_name)
                        || expr_mentions_table_name(&condition.result, table_name)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|value| expr_mentions_table_name(value, table_name))
        }
        Expr::Tuple(items) => items
            .iter()
            .any(|item| expr_mentions_table_name(item, table_name)),
        _ => false,
    }
}

fn object_name_matches_table_name(name: &ObjectName, table_name: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(table_name))
        .unwrap_or(false)
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
    use super::{
        active_version_from_update_validations, active_version_schema_key,
        advance_placeholder_state_for_statement, boot, coalesce_lix_file_transaction_statements,
        detected_file_domain_changes_from_detected_file_changes,
        detected_file_domain_changes_with_writer_key, extract_explicit_transaction_script,
        file_descriptor_cache_eviction_targets, file_history_read_materialization_required_for_sql,
        file_read_materialization_scope_for_sql, is_query_only_sql,
        should_invalidate_installed_plugins_cache_for_sql, should_refresh_file_cache_for_sql,
        BootArgs, ExecuteOptions, FileReadMaterializationScope,
    };
    use crate::backend::{LixBackend, LixTransaction, SqlDialect};
    use crate::plugin::types::{InstalledPlugin, PluginRuntime};
    use crate::sql::{
        bind_sql_with_state, parse_sql_statements, MutationOperation, MutationRow, PlaceholderState,
    };
    use crate::sql::{DetectedFileDomainChange, UpdateValidationPlan};
    use crate::{LixError, QueryResult, SnapshotChunkReader, Value, WasmComponentInstance};
    use async_trait::async_trait;
    use serde_json::json;
    use sqlparser::ast::{Expr, Statement};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, RwLock};
    struct TestBackend {
        commit_called: Arc<AtomicBool>,
        rollback_called: Arc<AtomicBool>,
        active_version_snapshot: Arc<RwLock<String>>,
        restored_active_version_snapshot: String,
    }

    struct TestTransaction {
        commit_called: Arc<AtomicBool>,
        rollback_called: Arc<AtomicBool>,
    }

    #[derive(Default)]
    struct EmptySnapshotReader;

    struct NoopWasmComponentInstance;

    fn active_version_snapshot_json(version_id: &str) -> String {
        serde_json::json!({ "id": "main", "version_id": version_id }).to_string()
    }

    #[async_trait(?Send)]
    impl LixBackend for TestBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_state_untracked")
                && sql.contains("SELECT snapshot_content")
            {
                let snapshot = self
                    .active_version_snapshot
                    .read()
                    .expect("active_version_snapshot lock")
                    .clone();
                return Ok(QueryResult {
                    rows: vec![vec![Value::Text(snapshot)]],
                });
            }
            Ok(QueryResult { rows: Vec::new() })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Ok(Box::new(TestTransaction {
                commit_called: Arc::clone(&self.commit_called),
                rollback_called: Arc::clone(&self.rollback_called),
            }))
        }

        async fn restore_from_snapshot(
            &self,
            reader: &mut dyn SnapshotChunkReader,
        ) -> Result<(), LixError> {
            while reader.read_chunk().await?.is_some() {}
            let mut guard = self
                .active_version_snapshot
                .write()
                .expect("active_version_snapshot lock");
            *guard = self.restored_active_version_snapshot.clone();
            Ok(())
        }
    }

    #[async_trait(?Send)]
    impl LixTransaction for TestTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(
            &mut self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<QueryResult, LixError> {
            Ok(QueryResult { rows: Vec::new() })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            self.commit_called.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            self.rollback_called.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait(?Send)]
    impl SnapshotChunkReader for EmptySnapshotReader {
        async fn read_chunk(&mut self) -> Result<Option<Vec<u8>>, LixError> {
            Ok(None)
        }
    }

    #[async_trait(?Send)]
    impl WasmComponentInstance for NoopWasmComponentInstance {
        async fn call(&self, _export: &str, _input: &[u8]) -> Result<Vec<u8>, LixError> {
            Ok(Vec::new())
        }
    }

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

    #[test]
    fn refresh_cache_detection_matches_lix_state_writes() {
        assert!(should_refresh_file_cache_for_sql(
            "UPDATE lix_state SET snapshot_content = '{}' WHERE file_id = 'f'"
        ));
        assert!(should_refresh_file_cache_for_sql(
            "DELETE FROM lix_state_by_version WHERE file_id = 'f'"
        ));
        assert!(should_refresh_file_cache_for_sql(
            "INSERT INTO lix_state (entity_id, schema_key, file_id, version_id, snapshot_content) VALUES ('/x', 'json_pointer', 'f', 'v', '{}')"
        ));
    }

    #[test]
    fn refresh_cache_detection_ignores_non_target_tables() {
        assert!(!should_refresh_file_cache_for_sql(
            "SELECT * FROM lix_state WHERE file_id = 'f'"
        ));
        assert!(!should_refresh_file_cache_for_sql(
            "UPDATE lix_state_history SET snapshot_content = '{}' WHERE file_id = 'f'"
        ));
        assert!(!should_refresh_file_cache_for_sql(
            "UPDATE lix_internal_state_vtable SET snapshot_content = '{}' WHERE file_id = 'f'"
        ));
    }

    #[test]
    fn query_only_detection_matches_select_statements() {
        assert!(is_query_only_sql("SELECT path, data FROM lix_file"));
        assert!(is_query_only_sql(
            "SELECT path FROM lix_file; SELECT id FROM lix_version"
        ));
    }

    #[test]
    fn query_only_detection_rejects_mutations() {
        assert!(!is_query_only_sql(
            "SELECT path FROM lix_file; UPDATE lix_file SET path = '/x' WHERE id = 'f'"
        ));
        assert!(!is_query_only_sql("UPDATE lix_file SET path = '/x' WHERE id = 'f'"));
    }

    #[test]
    fn plugin_cache_invalidation_detects_internal_plugin_mutations_only() {
        assert!(should_invalidate_installed_plugins_cache_for_sql(
            "INSERT INTO lix_internal_plugin (key, runtime, api_version, detect_changes_glob, entry, manifest_json, wasm, created_at, updated_at) VALUES ('k', 'wasm-component-v1', '0.1.0', '*.json', 'plugin.wasm', '{}', X'00', '1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z')"
        ));
        assert!(should_invalidate_installed_plugins_cache_for_sql(
            "UPDATE lix_internal_plugin SET detect_changes_glob = '*.md' WHERE key = 'k'"
        ));
        assert!(should_invalidate_installed_plugins_cache_for_sql(
            "DELETE FROM lix_internal_plugin WHERE key = 'k'"
        ));
        assert!(!should_invalidate_installed_plugins_cache_for_sql(
            "SELECT * FROM lix_internal_plugin WHERE key = 'k'"
        ));
    }

    #[tokio::test]
    async fn transaction_plugin_cache_invalidation_happens_after_commit() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let engine = boot(BootArgs::new(Box::new(TestBackend {
            commit_called: Arc::clone(&commit_called),
            rollback_called: Arc::clone(&rollback_called),
            active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json("global"))),
            restored_active_version_snapshot: active_version_snapshot_json("global"),
        })));

        {
            let mut cache = engine
                .installed_plugins_cache
                .write()
                .expect("installed plugins cache lock");
            *cache = Some(Vec::new());
        }

        let mut tx = engine
            .begin_transaction_with_options(ExecuteOptions::default())
            .await
            .expect("begin transaction");
        tx.execute(
            "UPDATE lix_internal_plugin SET detect_changes_glob = '*.md' WHERE key = 'k'",
            &[],
        )
        .await
        .expect("transaction execute should succeed");

        assert!(
            engine
                .installed_plugins_cache
                .read()
                .expect("installed plugins cache lock")
                .is_some(),
            "cache should remain populated before commit"
        );

        tx.commit().await.expect("commit should succeed");
        assert!(commit_called.load(Ordering::SeqCst));
        assert!(!rollback_called.load(Ordering::SeqCst));
        assert!(
            engine
                .installed_plugins_cache
                .read()
                .expect("installed plugins cache lock")
                .is_none(),
            "cache should be invalidated after successful commit"
        );
    }

    #[tokio::test]
    async fn transaction_plugin_cache_invalidation_skips_rollback() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let engine = boot(BootArgs::new(Box::new(TestBackend {
            commit_called: Arc::clone(&commit_called),
            rollback_called: Arc::clone(&rollback_called),
            active_version_snapshot: Arc::new(RwLock::new(active_version_snapshot_json("global"))),
            restored_active_version_snapshot: active_version_snapshot_json("global"),
        })));

        {
            let mut cache = engine
                .installed_plugins_cache
                .write()
                .expect("installed plugins cache lock");
            *cache = Some(Vec::new());
        }

        let mut tx = engine
            .begin_transaction_with_options(ExecuteOptions::default())
            .await
            .expect("begin transaction");
        tx.execute(
            "UPDATE lix_internal_plugin SET detect_changes_glob = '*.md' WHERE key = 'k'",
            &[],
        )
        .await
        .expect("transaction execute should succeed");
        tx.rollback().await.expect("rollback should succeed");

        assert!(!commit_called.load(Ordering::SeqCst));
        assert!(rollback_called.load(Ordering::SeqCst));
        assert!(
            engine
                .installed_plugins_cache
                .read()
                .expect("installed plugins cache lock")
                .is_some(),
            "cache should remain populated after rollback"
        );
    }

    #[tokio::test]
    async fn restore_from_snapshot_refreshes_active_version_and_plugin_caches() {
        let commit_called = Arc::new(AtomicBool::new(false));
        let rollback_called = Arc::new(AtomicBool::new(false));
        let active_version_snapshot = Arc::new(RwLock::new(active_version_snapshot_json("before")));
        let engine = boot(BootArgs::new(Box::new(TestBackend {
            commit_called,
            rollback_called,
            active_version_snapshot: Arc::clone(&active_version_snapshot),
            restored_active_version_snapshot: active_version_snapshot_json("after"),
        })));

        {
            let mut cache = engine
                .installed_plugins_cache
                .write()
                .expect("installed plugins cache lock");
            *cache = Some(vec![InstalledPlugin {
                key: "k".to_string(),
                runtime: PluginRuntime::WasmComponentV1,
                api_version: "0.1.0".to_string(),
                detect_changes_glob: "*.json".to_string(),
                entry: "plugin.wasm".to_string(),
                manifest_json: "{}".to_string(),
                wasm: vec![0],
            }]);
        }
        {
            let mut components = engine
                .plugin_component_cache
                .lock()
                .expect("plugin component cache lock");
            components.insert(
                "k".to_string(),
                crate::plugin::runtime::CachedPluginComponent {
                    wasm: vec![0],
                    instance: Arc::new(NoopWasmComponentInstance) as Arc<dyn WasmComponentInstance>,
                },
            );
        }

        let mut reader = EmptySnapshotReader;
        engine
            .restore_from_snapshot(&mut reader)
            .await
            .expect("restore_from_snapshot should succeed");

        assert_eq!(
            engine
                .active_version_id
                .read()
                .expect("active_version_id lock")
                .as_str(),
            "after"
        );
        assert!(
            engine
                .installed_plugins_cache
                .read()
                .expect("installed plugins cache lock")
                .is_none(),
            "installed plugin cache should be invalidated after restore"
        );
        assert!(
            engine
                .plugin_component_cache
                .lock()
                .expect("plugin component cache lock")
                .is_empty(),
            "plugin component cache should be cleared after restore"
        );
    }

    #[test]
    fn filesystem_side_effect_scan_advances_placeholder_state_across_statements() {
        let mut statements = parse_sql_statements(
            "UPDATE lix_file SET path = ? WHERE id = 'file-a'; \
             UPDATE lix_file SET path = ? WHERE id = 'file-b'",
        )
        .expect("parse sql");
        assert_eq!(statements.len(), 2);

        let params = vec![
            Value::Text("/docs/a.json".to_string()),
            Value::Text("/archive/b.json".to_string()),
        ];
        let mut placeholder_state = PlaceholderState::new();
        advance_placeholder_state_for_statement(
            &statements.remove(0),
            &params,
            SqlDialect::Sqlite,
            &mut placeholder_state,
        )
        .expect("advance placeholder state for first statement");

        let bound = bind_sql_with_state("SELECT ?", &params, SqlDialect::Sqlite, placeholder_state)
            .expect("bind placeholder with carried state");
        assert_eq!(bound.params.len(), 1);
        assert_eq!(bound.params[0], Value::Text("/archive/b.json".to_string()));
    }

    #[test]
    fn file_read_materialization_scope_detects_insert_select_lix_file() {
        let scope = file_read_materialization_scope_for_sql(
            "INSERT INTO some_table (payload) \
             SELECT data FROM lix_file WHERE id = 'file-a'",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn file_read_materialization_scope_detects_insert_select_lix_file_by_version() {
        let scope = file_read_materialization_scope_for_sql(
            "INSERT INTO some_table (payload) \
             SELECT data FROM lix_file_by_version \
             WHERE id = 'file-a' AND lixcol_version_id = 'version-a'",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::AllVersions));
    }

    #[test]
    fn file_read_materialization_scope_detects_select_projection_subquery_lix_file_by_version() {
        let scope = file_read_materialization_scope_for_sql(
            "SELECT (\
                SELECT data FROM lix_file_by_version \
                WHERE id = 'file-a' AND lixcol_version_id = 'version-a'\
             ) AS payload",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::AllVersions));
    }

    #[test]
    fn file_read_materialization_scope_detects_select_where_exists_subquery_lix_file() {
        let scope = file_read_materialization_scope_for_sql(
            "SELECT 1 \
             WHERE EXISTS (\
                SELECT 1 FROM lix_file WHERE id = 'file-a'\
             )",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn file_read_materialization_scope_detects_select_join_on_subquery_lix_file() {
        let scope = file_read_materialization_scope_for_sql(
            "SELECT t.id \
             FROM some_table t \
             LEFT JOIN other_table o \
               ON EXISTS (SELECT 1 FROM lix_file WHERE id = 'file-a')",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn file_read_materialization_scope_detects_update_where_subquery_lix_file() {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE some_table \
             SET payload = 'x' \
             WHERE id IN (SELECT id FROM lix_file WHERE id = 'file-a')",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn file_read_materialization_scope_ignores_update_target_lix_file() {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file \
             SET path = '/renamed.json' \
             WHERE id = 'file-a'",
        );
        assert_eq!(scope, None);
    }

    #[test]
    fn file_read_materialization_scope_ignores_update_target_lix_file_by_version() {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file_by_version \
             SET path = '/renamed.json' \
             WHERE id = 'file-a' \
               AND lixcol_version_id = 'version-a'",
        );
        assert_eq!(scope, None);
    }

    #[test]
    fn regression_update_target_lix_file_with_data_predicate_requires_active_materialization_scope()
    {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file \
             SET path = '/renamed.json' \
             WHERE data IS NOT NULL",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn regression_update_target_lix_file_by_version_with_data_predicate_requires_all_versions_scope(
    ) {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file_by_version \
             SET path = '/renamed.json' \
             WHERE data IS NOT NULL \
               AND lixcol_version_id = 'version-a'",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::AllVersions));
    }

    #[test]
    fn regression_update_target_lix_file_string_literal_data_does_not_trigger_materialization() {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file \
             SET path = '/renamed.json' \
             WHERE metadata = 'data'",
        );
        assert_eq!(scope, None);
    }

    #[test]
    fn regression_update_target_lix_file_exists_subquery_data_requires_active_materialization_scope(
    ) {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file \
             SET path = '/renamed.json' \
             WHERE EXISTS (SELECT 1 WHERE data IS NOT NULL)",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn regression_update_target_lix_file_case_data_requires_active_materialization_scope() {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file \
             SET path = '/renamed.json' \
             WHERE CASE WHEN data IS NULL THEN 0 ELSE 1 END = 1",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn regression_update_target_lix_file_tuple_data_requires_active_materialization_scope() {
        let scope = file_read_materialization_scope_for_sql(
            "UPDATE lix_file \
             SET path = '/renamed.json' \
             WHERE (data, id) IN (('x', 'file-a'))",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::ActiveVersionOnly));
    }

    #[test]
    fn detected_file_domain_changes_apply_writer_key_fallback() {
        let detected = vec![crate::plugin::runtime::DetectedFileChange {
            entity_id: "entity-1".to_string(),
            schema_key: "schema-1".to_string(),
            schema_version: "1.0".to_string(),
            file_id: "file-1".to_string(),
            version_id: "version-1".to_string(),
            plugin_key: "plugin-1".to_string(),
            snapshot_content: Some("{\"a\":1}".to_string()),
        }];

        let with_writer_key =
            detected_file_domain_changes_from_detected_file_changes(&detected, Some("writer-123"));
        assert_eq!(with_writer_key[0].writer_key.as_deref(), Some("writer-123"));
    }

    #[test]
    fn detected_file_domain_changes_writer_key_is_overwritten_by_execution_writer() {
        let detected = vec![DetectedFileDomainChange {
            entity_id: "entity-1".to_string(),
            schema_key: "schema-1".to_string(),
            schema_version: "1.0".to_string(),
            file_id: "file-1".to_string(),
            version_id: "version-1".to_string(),
            plugin_key: "plugin-1".to_string(),
            snapshot_content: Some("{\"a\":1}".to_string()),
            metadata: None,
            writer_key: Some("writer-stale".to_string()),
        }];

        let with_writer_key = detected_file_domain_changes_with_writer_key(&detected, None);
        assert_eq!(with_writer_key[0].writer_key, None);

        let with_writer_key =
            detected_file_domain_changes_with_writer_key(&detected, Some("writer-current"));
        assert_eq!(
            with_writer_key[0].writer_key.as_deref(),
            Some("writer-current")
        );
    }

    #[test]
    fn file_read_materialization_scope_detects_delete_where_subquery_lix_file_by_version() {
        let scope = file_read_materialization_scope_for_sql(
            "DELETE FROM some_table \
             WHERE EXISTS (\
                 SELECT 1 FROM lix_file_by_version \
                 WHERE id = 'file-a' AND lixcol_version_id = 'version-a'\
             )",
        );
        assert_eq!(scope, Some(FileReadMaterializationScope::AllVersions));
    }

    #[test]
    fn file_history_materialization_detection_includes_insert_select_sources() {
        assert!(file_history_read_materialization_required_for_sql(
            "INSERT INTO some_table (payload) \
             SELECT data FROM lix_file_history \
             WHERE id = 'file-a' \
             LIMIT 1",
        ));
    }

    #[test]
    fn file_history_materialization_detection_includes_select_where_subquery_sources() {
        assert!(file_history_read_materialization_required_for_sql(
            "SELECT 1 \
             WHERE EXISTS (\
                SELECT 1 FROM lix_file_history WHERE id = 'file-a'\
             )",
        ));
    }

    #[test]
    fn file_history_materialization_detection_includes_update_where_subquery_sources() {
        assert!(file_history_read_materialization_required_for_sql(
            "UPDATE some_table \
             SET payload = 'x' \
             WHERE EXISTS (\
                 SELECT 1 FROM lix_file_history \
                 WHERE id = 'file-a' \
             )",
        ));
    }

    #[test]
    fn descriptor_delete_targets_cache_eviction_by_entity_id_and_version_id() {
        let targets = file_descriptor_cache_eviction_targets(&[
            MutationRow {
                operation: MutationOperation::Delete,
                entity_id: "file-a".to_string(),
                schema_key: "lix_file_descriptor".to_string(),
                schema_version: "1".to_string(),
                file_id: "lix".to_string(),
                version_id: "version-a".to_string(),
                plugin_key: "lix".to_string(),
                snapshot_content: None,
                untracked: false,
            },
            MutationRow {
                operation: MutationOperation::Delete,
                entity_id: "ignored-dir".to_string(),
                schema_key: "lix_directory_descriptor".to_string(),
                schema_version: "1".to_string(),
                file_id: "lix".to_string(),
                version_id: "version-a".to_string(),
                plugin_key: "lix".to_string(),
                snapshot_content: None,
                untracked: false,
            },
            MutationRow {
                operation: MutationOperation::Delete,
                entity_id: "ignored-untracked".to_string(),
                schema_key: "lix_file_descriptor".to_string(),
                schema_version: "1".to_string(),
                file_id: "lix".to_string(),
                version_id: "version-a".to_string(),
                plugin_key: "lix".to_string(),
                snapshot_content: None,
                untracked: true,
            },
        ]);

        assert_eq!(targets.len(), 1);
        assert!(targets.contains(&("file-a".to_string(), "version-a".to_string())));
    }

    #[test]
    fn extract_explicit_transaction_script_parses_begin_commit_wrapper() {
        let parsed = extract_explicit_transaction_script(
            "BEGIN; INSERT INTO lix_file (id, path, data) VALUES ('f1', '/a', x'01'); COMMIT;",
            &[],
        )
        .expect("parse transaction script");

        let statements = parsed.expect("expected explicit transaction script");
        assert_eq!(statements.len(), 1);
        assert!(matches!(statements[0], Statement::Insert(_)));
    }

    #[test]
    fn coalesce_lix_file_transaction_statements_rewrites_replay_shape() {
        let statements = parse_sql_statements(
            "DELETE FROM lix_file WHERE id IN ('d1');\
             INSERT INTO lix_file (id, path, data) VALUES ('i1', '/inserted.txt', x'01');\
             UPDATE lix_file SET path = '/updated-a.txt', data = x'0A' WHERE id = 'u1';\
             UPDATE lix_file SET path = '/updated-b.txt', data = x'0B' WHERE id = 'u2';",
        )
        .expect("parse");

        let rewritten =
            coalesce_lix_file_transaction_statements(&statements, Some(SqlDialect::Sqlite))
                .expect("expected coalesced rewrite");

        assert_eq!(rewritten.len(), 3);
        assert!(rewritten[0].starts_with("DELETE FROM lix_file WHERE id IN ('d1')"));
        assert!(rewritten[1].starts_with("INSERT INTO lix_file (id, path, data) VALUES "));
        assert!(rewritten[1].contains("('i1', '/inserted.txt', X'01')"));
        assert!(rewritten[2].starts_with("UPDATE lix_file SET path = CASE id "));
        assert!(rewritten[2].contains("WHEN 'u1' THEN '/updated-a.txt'"));
        assert!(rewritten[2].contains("WHEN 'u2' THEN '/updated-b.txt'"));
        assert!(rewritten[2].contains("WHEN 'u1' THEN X'0A'"));
        assert!(rewritten[2].contains("WHEN 'u2' THEN X'0B'"));
        assert!(rewritten[2].contains("WHERE id IN ('u1', 'u2')"));
    }

    #[test]
    fn coalesce_lix_file_transaction_statements_returns_none_for_duplicate_ids() {
        let statements = parse_sql_statements(
            "UPDATE lix_file SET path = '/updated-a.txt', data = x'0A' WHERE id = 'u1';\
             UPDATE lix_file SET path = '/updated-b.txt', data = x'0B' WHERE id = 'u1';",
        )
        .expect("parse");

        let rewritten =
            coalesce_lix_file_transaction_statements(&statements, Some(SqlDialect::Sqlite));
        assert!(rewritten.is_none());
    }

    #[test]
    fn coalesce_lix_file_transaction_statements_returns_none_for_insert_with_extra_columns() {
        let statements = parse_sql_statements(
            "INSERT INTO lix_file (id, path, data, metadata) \
             VALUES ('i1', '/inserted.txt', x'01', '{\"owner\":\"sam\"}');",
        )
        .expect("parse");

        let rewritten =
            coalesce_lix_file_transaction_statements(&statements, Some(SqlDialect::Sqlite));
        assert!(
            rewritten.is_none(),
            "coalescer must not drop non-replay columns from lix_file inserts"
        );
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
