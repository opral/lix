#![allow(dead_code)]

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::io::{Cursor, Write};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use lix_engine::{
    boot, BootAccount, BootArgs, BootKeyValue, CreateCheckpointResult, CreateVersionOptions,
    CreateVersionResult, Engine, ExecuteOptions, ExecuteResult, LiveStateApplyReport,
    LiveStateRebuildDebugMode, LiveStateRebuildPlan, LiveStateRebuildReport,
    LiveStateRebuildRequest, LiveStateRebuildScope, LixBackend, LixError, MergeVersionOptions,
    MergeVersionResult, ObserveEvents, ObserveQuery, RedoOptions, RedoResult, Session,
    SessionTransaction, UndoOptions, UndoResult, Value, WasmRuntime,
};
use serde_json::Value as JsonValue;
use tokio::sync::Mutex as TokioMutex;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

use super::simulations::default_simulations as default_simulations_impl;

pub struct Simulation {
    pub name: &'static str,
    pub backend_factory: Box<dyn Fn() -> Box<dyn LixBackend + Send + Sync> + Send + Sync>,
    pub setup: Option<Arc<dyn Fn() -> BoxFuture<'static, Result<(), LixError>> + Send + Sync>>,
    pub behavior: SimulationBehavior,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulationBehavior {
    Base,
    Rematerialization,
    TimestampShuffle,
}

pub struct SimulationArgs {
    backend_factory: Box<dyn Fn() -> Box<dyn LixBackend + Send + Sync> + Send + Sync>,
    setup: Option<Arc<dyn Fn() -> BoxFuture<'static, Result<(), LixError>> + Send + Sync>>,
    behavior: SimulationBehavior,
    expect: ExpectDeterministic,
}

pub struct SimulationBootArgs {
    pub key_values: Vec<BootKeyValue>,
    pub active_account: Option<BootAccount>,
    pub wasm_runtime: Arc<dyn WasmRuntime>,
    pub access_to_internal: bool,
}

impl Default for SimulationBootArgs {
    fn default() -> Self {
        default_simulation_boot_args()
    }
}

pub struct SimulationEngine {
    engine: Arc<Engine>,
    session: OnceLock<Arc<Session>>,
    behavior: SimulationBehavior,
    rematerialization_pending: AtomicBool,
    initialized: AtomicBool,
    rematerialization_lock: TokioMutex<()>,
}

impl SimulationEngine {
    #[allow(dead_code)]
    pub async fn init(&self) -> Result<(), LixError> {
        self.initialize().await
    }

    pub async fn initialize(&self) -> Result<(), LixError> {
        self.engine.initialize().await?;
        let session = Arc::new(self.engine.open_workspace_session().await?);
        let _ = self.session.set(session);
        self.initialized.store(true, Ordering::SeqCst);
        if self.behavior == SimulationBehavior::Rematerialization {
            // Re-materialize on first read after init, mirroring "cache cleared then repopulated"
            // simulation semantics from the JS test harness.
            self.rematerialization_pending.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.execute_with_options(sql, params, ExecuteOptions::default())
            .await
    }

    pub async fn execute_with_options(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        let session = self.opened_session().await?;
        match classify_statement(sql) {
            StatementKind::Read => {
                self.rematerialize_before_read_if_needed().await?;
                session.execute_with_options(sql, params, options).await
            }
            StatementKind::Write => {
                let result = session.execute_with_options(sql, params, options).await;
                if self.behavior == SimulationBehavior::Rematerialization && result.is_ok() {
                    self.rematerialization_pending.store(true, Ordering::SeqCst);
                }
                result
            }
            StatementKind::Other => session.execute_with_options(sql, params, options).await,
        }
    }

    pub async fn install_plugin(&self, archive_bytes: &[u8]) -> Result<(), LixError> {
        self.opened_session()
            .await?
            .install_plugin(archive_bytes)
            .await
    }

    pub async fn register_schema(&self, schema: &JsonValue) -> Result<(), LixError> {
        self.opened_session().await?.register_schema(schema).await
    }

    pub async fn create_version(
        &self,
        options: CreateVersionOptions,
    ) -> Result<CreateVersionResult, LixError> {
        self.opened_session().await?.create_version(options).await
    }

    pub async fn switch_version(&self, version_id: String) -> Result<(), LixError> {
        self.opened_session()
            .await?
            .switch_version(version_id)
            .await
    }

    pub async fn set_active_account_ids(
        &self,
        active_account_ids: Vec<String>,
    ) -> Result<(), LixError> {
        self.opened_session()
            .await?
            .set_active_account_ids(active_account_ids)
            .await
    }

    pub async fn merge_version(
        &self,
        options: MergeVersionOptions,
    ) -> Result<MergeVersionResult, LixError> {
        self.opened_session().await?.merge_version(options).await
    }

    pub async fn create_checkpoint(&self) -> Result<CreateCheckpointResult, LixError> {
        self.opened_session().await?.create_checkpoint().await
    }

    pub async fn undo(&self) -> Result<UndoResult, LixError> {
        self.opened_session().await?.undo().await
    }

    pub async fn undo_with_options(&self, options: UndoOptions) -> Result<UndoResult, LixError> {
        self.opened_session()
            .await?
            .undo_with_options(options)
            .await
    }

    pub async fn redo(&self) -> Result<RedoResult, LixError> {
        self.opened_session().await?.redo().await
    }

    pub async fn redo_with_options(&self, options: RedoOptions) -> Result<RedoResult, LixError> {
        self.opened_session()
            .await?
            .redo_with_options(options)
            .await
    }

    pub fn observe(&self, query: ObserveQuery) -> Result<ObserveEvents<'_>, LixError> {
        self.session()?.observe(query)
    }

    pub async fn begin_transaction_with_options(
        &self,
        options: ExecuteOptions,
    ) -> Result<SessionTransaction<'_>, LixError> {
        self.session()?
            .begin_transaction_with_options(options)
            .await
    }

    pub async fn transaction<T, F>(&self, options: ExecuteOptions, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut SessionTransaction<'_>,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
        self.session()?.transaction(options, f).await
    }

    pub async fn live_state_rebuild_plan(
        &self,
        req: &LiveStateRebuildRequest,
    ) -> Result<LiveStateRebuildPlan, LixError> {
        self.engine.live_state_rebuild_plan(req).await
    }

    pub async fn apply_live_state_rebuild_plan(
        &self,
        plan: &LiveStateRebuildPlan,
    ) -> Result<LiveStateApplyReport, LixError> {
        self.engine.apply_live_state_rebuild_plan(plan).await
    }

    pub async fn rebuild_live_state(
        &self,
        req: &LiveStateRebuildRequest,
    ) -> Result<LiveStateRebuildReport, LixError> {
        self.engine.rebuild_live_state(req).await
    }

    async fn rematerialize_before_read_if_needed(&self) -> Result<(), LixError> {
        if self.behavior != SimulationBehavior::Rematerialization
            || !self.initialized.load(Ordering::SeqCst)
            || !self.rematerialization_pending.load(Ordering::SeqCst)
        {
            return Ok(());
        }

        let _guard = self.rematerialization_lock.lock().await;
        if !self.initialized.load(Ordering::SeqCst)
            || !self.rematerialization_pending.load(Ordering::SeqCst)
        {
            return Ok(());
        }

        self.engine
            .rebuild_live_state(&LiveStateRebuildRequest {
                scope: LiveStateRebuildScope::Full,
                debug: LiveStateRebuildDebugMode::Off,
                debug_row_limit: 1,
            })
            .await?;
        self.rematerialization_pending
            .store(false, Ordering::SeqCst);
        Ok(())
    }

    fn session(&self) -> Result<&Session, LixError> {
        self.session
            .get()
            .map(Arc::as_ref)
            .ok_or_else(|| LixError::unknown("simulation session is not initialized"))
    }

    async fn opened_session(&self) -> Result<Arc<Session>, LixError> {
        if let Some(session) = self.session.get() {
            return Ok(Arc::clone(session));
        }
        let session = Arc::new(self.engine.open_workspace_session().await?);
        let _ = self.session.set(Arc::clone(&session));
        Ok(self.session.get().map(Arc::clone).unwrap_or(session))
    }
}

const DEFAULT_TEST_SCHEMA_PATH: &str = "schema/default.json";
const DEFAULT_TEST_SCHEMA_JSON: &str = r#"{
  "x-lix-key":"json_pointer",
  "x-lix-version":"1",
  "type":"object",
  "properties":{"path":{"type":"string"},"value":{}},
  "required":["path","value"],
  "additionalProperties":false
}"#;

pub fn build_test_plugin_archive(
    manifest_json: &str,
    wasm_bytes: &[u8],
) -> Result<Vec<u8>, LixError> {
    let mut manifest_value: serde_json::Value =
        serde_json::from_str(manifest_json).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("test plugin manifest must be valid JSON: {error}"),
        })?;
    {
        let manifest_object = manifest_value.as_object_mut().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "test plugin manifest must be a JSON object".to_string(),
        })?;

        if !manifest_object.contains_key("entry") {
            manifest_object.insert(
                "entry".to_string(),
                serde_json::Value::String("plugin.wasm".to_string()),
            );
        }
        if !manifest_object.contains_key("schemas") {
            manifest_object.insert(
                "schemas".to_string(),
                serde_json::Value::Array(vec![serde_json::Value::String(
                    DEFAULT_TEST_SCHEMA_PATH.to_string(),
                )]),
            );
        }
    }

    let normalized_manifest = serde_json::to_vec(&manifest_value).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("failed to normalize test plugin manifest JSON: {error}"),
    })?;
    let schemas = manifest_value
        .get("schemas")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "test plugin manifest schemas must be an array".to_string(),
        })?
        .iter()
        .map(|value| {
            value.as_str().map(str::to_string).ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "test plugin manifest schemas must contain string paths".to_string(),
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;

    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer
        .start_file("manifest.json", options)
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to start manifest.json in test plugin archive: {error}"),
        })?;
    writer
        .write_all(&normalized_manifest)
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to write manifest.json in test plugin archive: {error}"),
        })?;

    writer
        .start_file("plugin.wasm", options)
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to start plugin.wasm in test plugin archive: {error}"),
        })?;
    writer.write_all(wasm_bytes).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("failed to write plugin.wasm in test plugin archive: {error}"),
    })?;

    for schema_path in &schemas {
        writer
            .start_file(schema_path, options)
            .map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "failed to start schema '{schema_path}' in test plugin archive: {error}"
                ),
            })?;
        writer
            .write_all(DEFAULT_TEST_SCHEMA_JSON.as_bytes())
            .map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "failed to write schema '{schema_path}' in test plugin archive: {error}"
                ),
            })?;
    }

    writer
        .finish()
        .map(|cursor| cursor.into_inner())
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to finish test plugin archive: {error}"),
        })
}

impl Deref for SimulationEngine {
    type Target = Engine;

    fn deref(&self) -> &Self::Target {
        self.engine.as_ref()
    }
}

impl SimulationArgs {
    pub async fn boot_simulated_engine(
        &self,
        args: Option<SimulationBootArgs>,
    ) -> Result<SimulationEngine, LixError> {
        if let Some(setup) = &self.setup {
            setup().await?;
        }
        let mut args = args.unwrap_or_else(default_simulation_boot_args);
        if self.behavior == SimulationBehavior::TimestampShuffle {
            enable_timestamp_shuffle_mode(&mut args.key_values);
        }
        Ok(SimulationEngine {
            engine: Arc::new(boot(BootArgs {
                backend: (self.backend_factory)(),
                wasm_runtime: args.wasm_runtime,
                key_values: args.key_values,
                active_account: args.active_account,
                access_to_internal: args.access_to_internal,
            })),
            session: OnceLock::new(),
            behavior: self.behavior,
            rematerialization_pending: AtomicBool::new(false),
            initialized: AtomicBool::new(false),
            rematerialization_lock: TokioMutex::new(()),
        })
    }

    pub async fn boot_simulated_engine_deterministic(&self) -> Result<SimulationEngine, LixError> {
        self.boot_simulated_engine(Some(SimulationBootArgs {
            key_values: vec![BootKeyValue {
                key: "lix_deterministic_mode".to_string(),
                value: serde_json::json!({ "enabled": true }),
                lixcol_global: Some(true),
                lixcol_untracked: None,
            }],
            active_account: None,
            wasm_runtime: default_simulation_wasm_runtime(),
            access_to_internal: true,
        }))
        .await
    }

    pub fn assert_deterministic<T>(&self, actual: T)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        self.expect.assert_deterministic(actual);
    }

    pub fn assert_deterministic_normalized(&self, actual: Vec<Vec<Value>>) {
        self.expect.assert_deterministic_normalized(actual);
    }
}

fn default_simulation_boot_args() -> SimulationBootArgs {
    SimulationBootArgs {
        key_values: Vec::new(),
        active_account: None,
        wasm_runtime: default_simulation_wasm_runtime(),
        access_to_internal: true,
    }
}

fn enable_timestamp_shuffle_mode(key_values: &mut Vec<BootKeyValue>) {
    const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
    if let Some(existing) = key_values
        .iter_mut()
        .find(|entry| entry.key == DETERMINISTIC_MODE_KEY && entry.lixcol_global.unwrap_or(false))
    {
        let mut object = existing
            .value
            .as_object()
            .cloned()
            .unwrap_or_else(serde_json::Map::new);
        object.insert("enabled".to_string(), serde_json::Value::Bool(true));
        object.insert(
            "timestamp_shuffle".to_string(),
            serde_json::Value::Bool(true),
        );
        existing.value = serde_json::Value::Object(object);
        return;
    }

    key_values.push(BootKeyValue {
        key: DETERMINISTIC_MODE_KEY.to_string(),
        value: serde_json::json!({
            "enabled": true,
            "timestamp_shuffle": true
        }),
        lixcol_global: Some(true),
        lixcol_untracked: None,
    });
}

fn default_simulation_wasm_runtime() -> Arc<dyn WasmRuntime> {
    Arc::new(
        crate::support::wasmtime_runtime::TestWasmtimeRuntime::new()
            .expect("failed to initialize test wasmtime runtime"),
    ) as Arc<dyn WasmRuntime>
}

#[derive(Clone)]
enum ExpectDeterministic {
    Local(LocalExpectDeterministic),
    Shared(SharedExpectDeterministic),
}

#[derive(Clone)]
struct LocalExpectDeterministic {
    inner: Arc<Mutex<LocalExpectDeterministicState>>,
}

struct LocalExpectDeterministicState {
    #[allow(dead_code)]
    expected_values: Vec<Box<dyn Any + Send + Sync>>,
    is_first: bool,
    call_index: usize,
}

impl LocalExpectDeterministic {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(LocalExpectDeterministicState {
                expected_values: Vec::new(),
                is_first: true,
                call_index: 0,
            })),
        }
    }

    fn start_simulation(&self, is_first: bool) {
        let mut state = self
            .inner
            .lock()
            .expect("expect_deterministic mutex poisoned");
        state.is_first = is_first;
        state.call_index = 0;
    }

    #[allow(dead_code)]
    fn assert_deterministic<T>(&self, actual: T)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        let mut state = self
            .inner
            .lock()
            .expect("expect_deterministic mutex poisoned");
        let idx = state.call_index;
        state.call_index += 1;

        if state.is_first {
            state.expected_values.push(Box::new(actual.clone()));
            return;
        }

        let expected_any = state
            .expected_values
            .get(idx)
            .expect("expect_deterministic called more times than in baseline");
        let expected = expected_any
            .downcast_ref::<T>()
            .expect("expect_deterministic type mismatch across simulations");

        if &actual != expected {
            panic!(
				"SIMULATION DETERMINISM VIOLATION\n\nCall #{}: values differ across simulations\nactual: {:?}\nexpected: {:?}",
				idx,
				actual,
				expected
			);
        }
    }

    fn assert_deterministic_normalized(&self, actual: Vec<Vec<Value>>) {
        self.assert_deterministic(normalize_bool_like_rows(&actual));
    }
}

#[derive(Clone)]
struct SharedExpectDeterministic {
    run: Arc<SharedDeterministicRun>,
}

struct SharedDeterministicRun {
    backend_name: String,
    case_id: String,
    call_index: Mutex<usize>,
    state: Arc<SharedDeterministicCase>,
    is_baseline: bool,
}

struct SharedDeterministicCase {
    state: Mutex<SharedDeterministicCaseState>,
    condvar: Condvar,
}

struct SharedDeterministicCaseState {
    baseline_backend: Option<String>,
    baseline_finished: bool,
    baseline_failed: bool,
    baseline_call_count: Option<usize>,
    expected_values: Vec<Box<dyn Any + Send + Sync>>,
    role_by_backend: HashMap<String, bool>,
}

struct SharedDeterministicRunGuard {
    run: Arc<SharedDeterministicRun>,
}

impl SharedDeterministicRun {
    fn next_index(&self) -> usize {
        let mut idx = self
            .call_index
            .lock()
            .expect("deterministic run call_index mutex poisoned");
        let current = *idx;
        *idx += 1;
        current
    }

    fn call_count(&self) -> usize {
        *self
            .call_index
            .lock()
            .expect("deterministic run call_index mutex poisoned")
    }

    fn assert_deterministic<T>(&self, actual: T)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        let idx = self.next_index();
        if self.is_baseline {
            let mut state = self
                .state
                .state
                .lock()
                .expect("shared deterministic mutex poisoned");
            state.expected_values.push(Box::new(actual));
            self.state.condvar.notify_all();
            return;
        }

        let deadline = Instant::now() + Duration::from_secs(120);
        let mut state = self
            .state
            .state
            .lock()
            .expect("shared deterministic mutex poisoned");
        loop {
            if idx < state.expected_values.len() {
                break;
            }

            if state.baseline_finished {
                if state.baseline_failed {
                    panic!(
                        "SIMULATION DETERMINISM VIOLATION\n\nCase `{}` baseline backend `{}` failed; cannot compare call #{} for backend `{}`",
                        self.case_id,
                        state
                            .baseline_backend
                            .as_deref()
                            .unwrap_or("<unknown>"),
                        idx,
                        self.backend_name
                    );
                }
                panic!(
                    "SIMULATION DETERMINISM VIOLATION\n\nCase `{}` backend `{}` called assert_deterministic one extra time at call #{}",
                    self.case_id,
                    self.backend_name,
                    idx
                );
            }

            let now = Instant::now();
            if now >= deadline {
                panic!(
                    "SIMULATION DETERMINISM VIOLATION\n\nTimed out waiting for baseline values in case `{}` (backend `{}`, call #{})",
                    self.case_id,
                    self.backend_name,
                    idx
                );
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, _) = self
                .state
                .condvar
                .wait_timeout(state, remaining)
                .expect("shared deterministic condvar wait poisoned");
            state = next_state;
        }

        let expected_any = state
            .expected_values
            .get(idx)
            .expect("expected deterministic value missing");
        let expected = expected_any
            .downcast_ref::<T>()
            .expect("expect_deterministic type mismatch across simulations");
        if &actual != expected {
            panic!(
                "SIMULATION DETERMINISM VIOLATION\n\nCase `{}` call #{} differs for backend `{}`\nactual: {:?}\nexpected: {:?}",
                self.case_id,
                idx,
                self.backend_name,
                actual,
                expected
            );
        }
    }

    fn assert_deterministic_normalized(&self, actual: Vec<Vec<Value>>) {
        self.assert_deterministic(normalize_bool_like_rows(&actual));
    }

    fn finish(&self, success: bool) -> Result<(), String> {
        if self.is_baseline {
            let mut state = self
                .state
                .state
                .lock()
                .expect("shared deterministic mutex poisoned");
            state.baseline_finished = true;
            state.baseline_failed = !success;
            state.baseline_call_count = Some(self.call_count());
            self.state.condvar.notify_all();
            return Ok(());
        }

        if !success {
            return Ok(());
        }

        let deadline = Instant::now() + Duration::from_secs(120);
        let mut state = self
            .state
            .state
            .lock()
            .expect("shared deterministic mutex poisoned");
        while !state.baseline_finished {
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "SIMULATION DETERMINISM VIOLATION\n\nTimed out waiting for baseline completion in case `{}` for backend `{}`",
                    self.case_id, self.backend_name
                ));
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, _) = self
                .state
                .condvar
                .wait_timeout(state, remaining)
                .expect("shared deterministic condvar wait poisoned");
            state = next_state;
        }

        if state.baseline_failed {
            return Err(format!(
                "SIMULATION DETERMINISM VIOLATION\n\nCase `{}` baseline backend `{}` failed; cannot validate backend `{}`",
                self.case_id,
                state
                    .baseline_backend
                    .as_deref()
                    .unwrap_or("<unknown>"),
                self.backend_name
            ));
        }

        let expected_calls = state
            .baseline_call_count
            .unwrap_or(state.expected_values.len());
        let actual_calls = self.call_count();
        if actual_calls != expected_calls {
            return Err(format!(
                "SIMULATION DETERMINISM VIOLATION\n\nCase `{}` backend `{}` called assert_deterministic {} times but baseline expected {}",
                self.case_id, self.backend_name, actual_calls, expected_calls
            ));
        }
        Ok(())
    }
}

impl Drop for SharedDeterministicRunGuard {
    fn drop(&mut self) {
        let success = !std::thread::panicking();
        if let Err(message) = self.run.finish(success) {
            if success {
                panic!("{message}");
            }
        }
    }
}

impl SharedExpectDeterministic {
    fn new(case_id: &str, backend_name: &str) -> (Self, SharedDeterministicRunGuard) {
        static REGISTRY: OnceLock<Mutex<HashMap<String, Arc<SharedDeterministicCase>>>> =
            OnceLock::new();
        let registry = REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));

        let case = {
            let mut lock = registry
                .lock()
                .expect("shared deterministic registry mutex poisoned");
            lock.entry(case_id.to_string())
                .or_insert_with(|| {
                    Arc::new(SharedDeterministicCase {
                        state: Mutex::new(SharedDeterministicCaseState {
                            baseline_backend: None,
                            baseline_finished: false,
                            baseline_failed: false,
                            baseline_call_count: None,
                            expected_values: Vec::new(),
                            role_by_backend: HashMap::new(),
                        }),
                        condvar: Condvar::new(),
                    })
                })
                .clone()
        };

        let is_baseline = {
            let mut state = case
                .state
                .lock()
                .expect("shared deterministic mutex poisoned");
            if let Some(existing) = state.role_by_backend.get(backend_name) {
                *existing
            } else {
                let is_baseline = match state.baseline_backend.as_deref() {
                    Some(baseline) => baseline == backend_name,
                    None => {
                        state.baseline_backend = Some(backend_name.to_string());
                        true
                    }
                };
                state
                    .role_by_backend
                    .insert(backend_name.to_string(), is_baseline);
                is_baseline
            }
        };

        let run = Arc::new(SharedDeterministicRun {
            backend_name: backend_name.to_string(),
            case_id: case_id.to_string(),
            call_index: Mutex::new(0),
            state: case,
            is_baseline,
        });
        let guard = SharedDeterministicRunGuard { run: run.clone() };
        (Self { run }, guard)
    }

    fn assert_deterministic<T>(&self, actual: T)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        self.run.assert_deterministic(actual);
    }

    fn assert_deterministic_normalized(&self, actual: Vec<Vec<Value>>) {
        self.run.assert_deterministic_normalized(actual);
    }
}

impl ExpectDeterministic {
    fn new_local() -> Self {
        Self::Local(LocalExpectDeterministic::new())
    }

    fn new_shared(case_id: &str, backend_name: &str) -> (Self, SharedDeterministicRunGuard) {
        let (expect, guard) = SharedExpectDeterministic::new(case_id, backend_name);
        (Self::Shared(expect), guard)
    }

    fn start_simulation(&self, is_first: bool) {
        match self {
            Self::Local(local) => local.start_simulation(is_first),
            Self::Shared(_) => {
                panic!("start_simulation is only valid for local deterministic mode")
            }
        }
    }

    fn assert_deterministic<T>(&self, actual: T)
    where
        T: PartialEq + std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        match self {
            Self::Local(local) => local.assert_deterministic(actual),
            Self::Shared(shared) => shared.assert_deterministic(actual),
        }
    }

    fn assert_deterministic_normalized(&self, actual: Vec<Vec<Value>>) {
        match self {
            Self::Local(local) => local.assert_deterministic_normalized(actual),
            Self::Shared(shared) => shared.assert_deterministic_normalized(actual),
        }
    }
}

pub fn assert_boolean_like(value: &Value, expected: bool) {
    match value {
        Value::Boolean(actual) => assert_eq!(*actual, expected),
        Value::Integer(actual) => assert_eq!(*actual != 0, expected),
        Value::Text(actual) => {
            let normalized = actual.trim().to_ascii_lowercase();
            let parsed = match normalized.as_str() {
                "1" | "true" => true,
                "0" | "false" => false,
                _ => panic!("expected boolean-like text, got '{actual}'"),
            };
            assert_eq!(parsed, expected);
        }
        other => panic!("expected boolean-like value, got {other:?}"),
    }
}

fn normalize_bool_like_rows(rows: &[Vec<Value>]) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| row.iter().map(normalize_bool_like_value).collect())
        .collect()
}

fn normalize_bool_like_value(value: &Value) -> Value {
    match value {
        Value::Integer(0) => Value::Boolean(false),
        Value::Integer(1) => Value::Boolean(true),
        Value::Text(text) => {
            let normalized = text.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "0" | "false" => Value::Boolean(false),
                "1" | "true" => Value::Boolean(true),
                _ => value.clone(),
            }
        }
        _ => value.clone(),
    }
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub async fn run_simulation_test<F, Fut>(simulations: Vec<Simulation>, test_fn: F)
where
    F: Fn(SimulationArgs) -> Fut,
    Fut: Future<Output = ()>,
{
    let deterministic = ExpectDeterministic::new_local();

    for (index, simulation) in simulations.into_iter().enumerate() {
        deterministic.start_simulation(index == 0);
        let args = SimulationArgs {
            backend_factory: simulation.backend_factory,
            setup: simulation.setup,
            behavior: simulation.behavior,
            expect: deterministic.clone(),
        };
        Box::pin(test_fn(args)).await;
    }
}

pub async fn run_single_simulation_test<F, Fut>(simulation_name: &str, case_id: &str, test_fn: F)
where
    F: Fn(SimulationArgs) -> Fut,
    Fut: Future<Output = ()>,
{
    let simulation = default_simulations_impl()
        .into_iter()
        .find(|sim| sim.name == simulation_name)
        .unwrap_or_else(|| panic!("{} simulation missing", simulation_name));
    let (deterministic, _guard) = ExpectDeterministic::new_shared(case_id, simulation.name);
    let args = SimulationArgs {
        backend_factory: simulation.backend_factory,
        setup: simulation.setup,
        behavior: simulation.behavior,
        expect: deterministic,
    };
    Box::pin(test_fn(args)).await;
}

pub fn default_simulations() -> Vec<Simulation> {
    default_simulations_impl()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementKind {
    Read,
    Write,
    Other,
}

fn classify_statement(sql: &str) -> StatementKind {
    let Some(keyword) = first_keyword(sql) else {
        return StatementKind::Other;
    };
    let keyword = keyword.to_ascii_lowercase();

    match keyword.as_str() {
        "select" | "pragma" | "show" | "describe" | "desc" | "explain" => StatementKind::Read,
        "insert" | "update" | "delete" | "replace" | "merge" => StatementKind::Write,
        "with" => classify_with_statement(sql),
        _ => StatementKind::Other,
    }
}

fn classify_with_statement(sql: &str) -> StatementKind {
    let normalized = sql.to_ascii_lowercase();
    if normalized.contains(" insert ")
        || normalized.contains(" update ")
        || normalized.contains(" delete ")
        || normalized.contains(" replace ")
        || normalized.contains(" merge ")
    {
        StatementKind::Write
    } else {
        StatementKind::Read
    }
}

fn first_keyword(sql: &str) -> Option<&str> {
    let trimmed = sql.trim_start();
    let end = trimmed
        .char_indices()
        .find_map(|(idx, ch)| {
            if ch.is_whitespace() || ch == '(' {
                Some(idx)
            } else {
                None
            }
        })
        .unwrap_or(trimmed.len());
    let keyword = &trimmed[..end];
    if keyword.is_empty() {
        None
    } else {
        Some(keyword)
    }
}
