#[cfg(target_arch = "wasm32")]
mod wasm {
    use std::cell::Cell;
    use std::ops::Bound;

    use bytes::Bytes;
    use js_sys::{Array, Object, Reflect};
    use lix_rs_sdk::{
        open_lix_with_backend, run_backend_conformance, Backend, BackendConformanceStatus,
        BackendError, BackendFactory, BackendFixture, BackendRangeScan, BackendRead,
        BackendTestConfig, BackendWrite, CommitResult, CoreProjection, CreateBranchOptions,
        ExecuteResult, GetOptions, InMemoryBackend, Key, KeyRange, Lix as RsLix, LixError,
        LixTransaction as RsLixTransaction, MergeBranchOptions, MergeBranchPreviewOptions,
        PointVisitor, ProjectedValueRef, PutBatch, ReadOptions, ScanOptions, ScanResult,
        ScanVisitor, StoredValue, SwitchBranchOptions, Value, WriteOptions, WriteStats,
    };
    use serde::Serialize;
    use serde_json::json;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;

    #[wasm_bindgen(typescript_custom_section)]
    const LIX_TYPES: &str = r#"
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export type LixValue =
  | { kind: "null"; value: null }
  | { kind: "boolean"; value: boolean }
  | { kind: "integer"; value: number }
  | { kind: "real"; value: number }
  | { kind: "text"; value: string }
  | { kind: "json"; value: JsonValue }
  | { kind: "blob"; base64: string };

export type ExecuteResult = {
  columns: string[];
  rows: LixValue[][];
  rowsAffected: number;
  notices: LixNotice[];
};

export type LixNotice = {
  code: string;
  message: string;
  hint?: string;
};

export type BackendKvBound =
  | { kind: "included"; key: Uint8Array }
  | { kind: "excluded"; key: Uint8Array }
  | { kind: "unbounded" };

export type BackendKvScanRange = {
  lower: BackendKvBound;
  upper: BackendKvBound;
};

export type BackendKvGetRequest = {
  keys: Uint8Array[];
};

export type BackendKvValueBatch = {
  values: Array<Uint8Array | null>;
};

export type BackendKvScanRequest = {
  range: BackendKvScanRange;
  after?: Uint8Array | null;
  limit: number;
};

export type BackendKvEntryPage = {
  keys: Uint8Array[];
  values: Uint8Array[];
  resumeAfter?: Uint8Array | null;
};

export type BackendKvWriteOp =
  | { kind: "put"; key: Uint8Array; value: Uint8Array }
  | { kind: "delete"; key: Uint8Array }
  | { kind: "deleteRange"; range: BackendKvScanRange };

export type BackendKvWriteBatch = {
  ops: BackendKvWriteOp[];
};

export type BackendKvWriteStats = {
  puts: number;
  deletes: number;
  deleteRanges: number;
  bytesWritten: number;
};

export type BackendReadTransaction = {
  getValues(request: BackendKvGetRequest): BackendKvValueBatch;
  scanEntries(request: BackendKvScanRequest): BackendKvEntryPage;
  rollback(): void;
};

export type BackendWriteTransaction = BackendReadTransaction & {
  writeKvBatch(batch: BackendKvWriteBatch): BackendKvWriteStats;
  commit(): void;
};

export type Backend = {
  beginReadTransaction(): BackendReadTransaction;
  /**
   * Opens one backend-owned write transaction.
   *
   * Implementations are responsible for their own durability and write
   * serialization. A backend that cannot safely support concurrent write
   * transactions must reject or serialize a second call itself.
   */
  beginWriteTransaction(): BackendWriteTransaction;
  close?(): void;
};

export type BackendConformanceFactory = {
  createFixture(): BackendConformanceFixture;
  config?: Partial<BackendConformanceConfig>;
};

export type BackendConformanceFixture = {
  open(): Backend;
};

export type BackendConformanceConfig = {
  maxKeyLen: number;
  maxValueLen: number;
  ephemeral: boolean;
  supportsConcurrentWriters: boolean;
};

export type BackendConformanceReport = {
  tests: BackendConformanceTest[];
};

export type BackendConformanceTest = {
  name: string;
  status: "passed" | "failed" | "pending";
  error?: string;
};

export type OpenLixOptions = {
  backend?: Backend;
};

export type CreateBranchOptions = {
  id?: string;
  name: string;
  fromCommitId?: string;
};

export type CreateBranchResult = {
  id: string;
  name: string;
  hidden: boolean;
  commitId: string;
};

export type SwitchBranchOptions = {
  branchId: string;
};

export type SwitchBranchResult = {
  branchId: string;
};

export type MergeBranchOptions = {
  sourceBranchId: string;
};

export type MergeBranchOutcome =
  | "alreadyUpToDate"
  | "fastForward"
  | "mergeCommitted";

export type MergeBranchResult = {
  outcome: MergeBranchOutcome;
  targetBranchId: string;
  sourceBranchId: string;
  baseCommitId: string;
  targetHeadBeforeCommitId: string;
  sourceHeadBeforeCommitId: string;
  targetHeadAfterCommitId: string;
  createdMergeCommitId: string | null;
  changeStats: MergeChangeStats;
};

export type MergeBranchPreviewResult = {
  outcome: MergeBranchOutcome;
  targetBranchId: string;
  sourceBranchId: string;
  baseCommitId: string;
  targetHeadCommitId: string;
  sourceHeadCommitId: string;
  changeStats: MergeChangeStats;
  conflicts: MergeConflict[];
};

export type MergeChangeStats = {
  total: number;
  added: number;
  modified: number;
  removed: number;
};

export type MergeConflict = {
  kind: "sameEntityChanged";
  schemaKey: string;
  entityPk: string[];
  fileId: string | null;
  target: MergeConflictSide;
  source: MergeConflictSide;
};

export type MergeConflictSide = {
  kind: "added" | "modified" | "removed";
  beforeChangeId: string | null;
  afterChangeId: string | null;
};
"#;

    #[wasm_bindgen]
    pub struct Lix {
        inner: LixInner,
        backend: Option<JsValue>,
        backend_closed: Cell<bool>,
    }

    #[wasm_bindgen]
    pub struct LixTransaction {
        inner: Option<LixTransactionInner>,
    }

    enum LixInner {
        InMemory(RsLix),
        Js(RsLix<JsBackend>),
    }

    enum LixTransactionInner {
        InMemory(RsLixTransaction),
        Js(RsLixTransaction<JsBackend>),
    }

    impl LixInner {
        async fn execute(&self, sql: &str, values: &[Value]) -> Result<ExecuteResult, LixError> {
            match self {
                LixInner::InMemory(inner) => inner.execute(sql, values).await,
                LixInner::Js(inner) => inner.execute(sql, values).await,
            }
        }

        async fn begin_transaction(&self) -> Result<LixTransactionInner, LixError> {
            match self {
                LixInner::InMemory(inner) => inner
                    .begin_transaction()
                    .await
                    .map(LixTransactionInner::InMemory),
                LixInner::Js(inner) => inner.begin_transaction().await.map(LixTransactionInner::Js),
            }
        }

        async fn active_branch_id(&self) -> Result<String, LixError> {
            match self {
                LixInner::InMemory(inner) => inner.active_branch_id().await,
                LixInner::Js(inner) => inner.active_branch_id().await,
            }
        }

        async fn create_branch(
            &self,
            options: CreateBranchOptions,
        ) -> Result<lix_rs_sdk::CreateBranchReceipt, LixError> {
            match self {
                LixInner::InMemory(inner) => inner.create_branch(options).await,
                LixInner::Js(inner) => inner.create_branch(options).await,
            }
        }

        async fn switch_branch(
            &self,
            options: SwitchBranchOptions,
        ) -> Result<lix_rs_sdk::SwitchBranchReceipt, LixError> {
            match self {
                LixInner::InMemory(inner) => inner.switch_branch(options).await,
                LixInner::Js(inner) => inner.switch_branch(options).await,
            }
        }

        async fn merge_branch_preview(
            &self,
            options: MergeBranchPreviewOptions,
        ) -> Result<lix_rs_sdk::MergeBranchPreview, LixError> {
            match self {
                LixInner::InMemory(inner) => inner.merge_branch_preview(options).await,
                LixInner::Js(inner) => inner.merge_branch_preview(options).await,
            }
        }

        async fn merge_branch(
            &self,
            options: MergeBranchOptions,
        ) -> Result<lix_rs_sdk::MergeBranchReceipt, LixError> {
            match self {
                LixInner::InMemory(inner) => inner.merge_branch(options).await,
                LixInner::Js(inner) => inner.merge_branch(options).await,
            }
        }

        async fn close(&self) -> Result<(), LixError> {
            match self {
                LixInner::InMemory(inner) => inner.close().await,
                LixInner::Js(inner) => inner.close().await,
            }
        }
    }

    impl LixTransactionInner {
        async fn execute(
            &mut self,
            sql: &str,
            values: &[Value],
        ) -> Result<ExecuteResult, LixError> {
            match self {
                LixTransactionInner::InMemory(inner) => inner.execute(sql, values).await,
                LixTransactionInner::Js(inner) => inner.execute(sql, values).await,
            }
        }

        async fn commit(self) -> Result<(), LixError> {
            match self {
                LixTransactionInner::InMemory(inner) => inner.commit().await,
                LixTransactionInner::Js(inner) => inner.commit().await,
            }
        }

        async fn rollback(self) -> Result<(), LixError> {
            match self {
                LixTransactionInner::InMemory(inner) => inner.rollback().await,
                LixTransactionInner::Js(inner) => inner.rollback().await,
            }
        }
    }

    #[wasm_bindgen]
    impl Lix {
        /// Executes one DataFusion SQL statement against this Lix session.
        ///
        /// The SQL dialect is DataFusion SQL, not SQLite SQL. Positional
        /// placeholders use `?` or `$1`, `$2`, and so on. SQLite-specific catalog
        /// tables and transaction statements such as `sqlite_master`, `BEGIN`,
        /// and `COMMIT` are not part of this contract; use
        /// `information_schema` for catalog inspection.
        #[wasm_bindgen(js_name = execute)]
        pub async fn execute(&self, sql: JsValue, params: JsValue) -> Result<JsValue, JsValue> {
            let sql = sql
                .as_string()
                .ok_or_else(|| invalid_argument_error("execute", "sql", "string", &sql))
                .map_err(js_error)?;
            if !Array::is_array(&params) {
                return Err(js_error(invalid_argument_error(
                    "execute", "params", "array", &params,
                )));
            }
            let params = Array::from(&params);
            let values = params
                .iter()
                .map(value_from_js)
                .collect::<Result<Vec<_>, _>>()
                .map_err(js_error)?;
            let result = self.inner.execute(&sql, &values).await.map_err(js_error)?;
            execute_result_to_js(result).map_err(js_error)
        }

        #[wasm_bindgen(js_name = beginTransaction)]
        pub async fn begin_transaction(&self) -> Result<LixTransaction, JsValue> {
            let inner = self.inner.begin_transaction().await.map_err(js_error)?;
            Ok(LixTransaction { inner: Some(inner) })
        }

        #[wasm_bindgen(js_name = activeBranchId)]
        pub async fn active_branch_id(&self) -> Result<String, JsValue> {
            self.inner.active_branch_id().await.map_err(js_error)
        }

        #[wasm_bindgen(js_name = createBranch)]
        pub async fn create_branch(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let options = parse_create_branch_options(args).map_err(js_error)?;
            let result = self.inner.create_branch(options).await.map_err(js_error)?;
            let object = Object::new();
            set_string(&object, "id", &result.id).map_err(js_error)?;
            set_string(&object, "name", &result.name).map_err(js_error)?;
            Reflect::set(
                &object,
                &JsValue::from_str("hidden"),
                &JsValue::from_bool(result.hidden),
            )
            .map_err(|_| js_error(js_sdk_error("could not set hidden")))?;
            set_string(&object, "commitId", &result.commit_id).map_err(js_error)?;
            Ok(object.into())
        }

        #[wasm_bindgen(js_name = switchBranch)]
        pub async fn switch_branch(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let options = parse_switch_branch_options(args).map_err(js_error)?;
            let result = self.inner.switch_branch(options).await.map_err(js_error)?;
            let object = Object::new();
            set_string(&object, "branchId", &result.branch_id).map_err(js_error)?;
            Ok(object.into())
        }

        #[wasm_bindgen(js_name = mergeBranchPreview)]
        pub async fn merge_branch_preview(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let options = parse_merge_branch_preview_options(args).map_err(js_error)?;
            let result = self
                .inner
                .merge_branch_preview(options)
                .await
                .map_err(js_error)?;
            merge_branch_preview_to_js(result).map_err(js_error)
        }

        #[wasm_bindgen(js_name = mergeBranch)]
        pub async fn merge_branch(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let options = parse_merge_branch_options(args).map_err(js_error)?;
            let result = self.inner.merge_branch(options).await.map_err(js_error)?;
            let object = Object::new();
            let outcome = match result.outcome {
                lix_rs_sdk::MergeBranchOutcome::AlreadyUpToDate => "alreadyUpToDate",
                lix_rs_sdk::MergeBranchOutcome::FastForward => "fastForward",
                lix_rs_sdk::MergeBranchOutcome::MergeCommitted => "mergeCommitted",
            };
            set_string(&object, "outcome", outcome).map_err(js_error)?;
            set_string(&object, "targetBranchId", &result.target_branch_id).map_err(js_error)?;
            set_string(&object, "sourceBranchId", &result.source_branch_id).map_err(js_error)?;
            set_string(&object, "baseCommitId", &result.base_commit_id).map_err(js_error)?;
            set_string(
                &object,
                "targetHeadBeforeCommitId",
                &result.target_head_before_commit_id,
            )
            .map_err(js_error)?;
            set_string(
                &object,
                "sourceHeadBeforeCommitId",
                &result.source_head_before_commit_id,
            )
            .map_err(js_error)?;
            set_string(
                &object,
                "targetHeadAfterCommitId",
                &result.target_head_after_commit_id,
            )
            .map_err(js_error)?;
            set_optional_string(
                &object,
                "createdMergeCommitId",
                result.created_merge_commit_id.as_deref(),
            )
            .map_err(js_error)?;
            Reflect::set(
                &object,
                &JsValue::from_str("changeStats"),
                &merge_change_stats_to_js(&result.change_stats).map_err(js_error)?,
            )
            .map_err(|_| js_error(js_sdk_error("could not set changeStats")))?;
            Ok(object.into())
        }

        #[wasm_bindgen(js_name = close)]
        pub async fn close(&self) -> Result<(), JsValue> {
            self.inner.close().await.map_err(js_error)?;
            if let Some(backend) = &self.backend {
                if !self.backend_closed.replace(true) {
                    close_js_backend(backend).map_err(js_error)?;
                }
            }
            Ok(())
        }
    }

    #[wasm_bindgen]
    impl LixTransaction {
        #[wasm_bindgen(js_name = execute)]
        pub async fn execute(&mut self, sql: JsValue, params: JsValue) -> Result<JsValue, JsValue> {
            let sql = sql
                .as_string()
                .ok_or_else(|| invalid_argument_error("execute", "sql", "string", &sql))
                .map_err(js_error)?;
            if !Array::is_array(&params) {
                return Err(js_error(invalid_argument_error(
                    "execute", "params", "array", &params,
                )));
            }
            let params = Array::from(&params);
            let values = params
                .iter()
                .map(value_from_js)
                .collect::<Result<Vec<_>, _>>()
                .map_err(js_error)?;
            let inner = self
                .inner
                .as_mut()
                .ok_or_else(transaction_closed_error)
                .map_err(js_error)?;
            let result = inner.execute(&sql, &values).await.map_err(js_error)?;
            execute_result_to_js(result).map_err(js_error)
        }

        #[wasm_bindgen(js_name = commit)]
        pub async fn commit(&mut self) -> Result<(), JsValue> {
            let inner = self
                .inner
                .take()
                .ok_or_else(transaction_closed_error)
                .map_err(js_error)?;
            inner.commit().await.map_err(js_error)
        }

        #[wasm_bindgen(js_name = rollback)]
        pub async fn rollback(&mut self) -> Result<(), JsValue> {
            let inner = self
                .inner
                .take()
                .ok_or_else(transaction_closed_error)
                .map_err(js_error)?;
            inner.rollback().await.map_err(js_error)
        }
    }

    #[wasm_bindgen(js_name = openLix)]
    pub async fn open_lix(args: Option<JsValue>) -> Result<Lix, JsValue> {
        let options = parse_open_lix_options(args).map_err(js_error)?;
        let backend = options.backend;
        let backend_handle = options.backend_handle;
        Ok(Lix {
            inner: WasmOpenLixOptions::open_backend(backend)
                .await
                .map_err(js_error)?,
            backend: backend_handle,
            backend_closed: Cell::new(false),
        })
    }

    #[wasm_bindgen(js_name = runBackendConformance)]
    pub fn run_backend_conformance_js(factory: JsValue) -> Result<JsValue, JsValue> {
        let factory = JsBackendConformanceFactory::new(factory).map_err(js_error)?;
        let report = run_backend_conformance(&factory);
        backend_conformance_report_to_js(report).map_err(js_error)
    }

    struct WasmOpenLixOptions {
        backend: Option<JsBackend>,
        backend_handle: Option<JsValue>,
    }

    impl WasmOpenLixOptions {
        async fn open_backend(backend: Option<JsBackend>) -> Result<LixInner, LixError> {
            match backend {
                Some(backend) => open_lix_with_backend(backend).await.map(LixInner::Js),
                None => open_lix_with_backend(InMemoryBackend::new())
                    .await
                    .map(LixInner::InMemory),
            }
        }
    }

    fn parse_open_lix_options(args: Option<JsValue>) -> Result<WasmOpenLixOptions, LixError> {
        let Some(value) = args else {
            return Ok(WasmOpenLixOptions::default());
        };
        if value.is_undefined() || value.is_null() {
            return Ok(WasmOpenLixOptions::default());
        }
        if !value.is_object() {
            return Err(LixError::new(
                "LIX_ERROR_JS_SDK",
                "openLix() options must be an object",
            ));
        }
        let backend = Reflect::get(&value, &JsValue::from_str("backend"))
            .map_err(|_| js_sdk_error("openLix() could not read backend"))?;
        if backend.is_undefined() || backend.is_null() {
            return Ok(WasmOpenLixOptions::default());
        }
        if !backend.is_object() {
            return Err(LixError::new(
                "LIX_ERROR_JS_SDK",
                "openLix() backend must be an object",
            ));
        }
        Ok(WasmOpenLixOptions {
            backend: Some(JsBackend::new(backend.clone())),
            backend_handle: Some(backend),
        })
    }

    impl Default for WasmOpenLixOptions {
        fn default() -> Self {
            Self {
                backend: None,
                backend_handle: None,
            }
        }
    }

    #[derive(Clone)]
    struct JsBackend {
        inner: JsValue,
    }

    struct JsBackendConformanceFactory {
        inner: JsValue,
        config: BackendTestConfig,
    }

    struct JsBackendConformanceFixture {
        inner: JsValue,
    }

    struct JsConformanceBackend {
        inner: JsBackend,
        handle: JsValue,
    }

    impl JsBackend {
        fn new(inner: JsValue) -> Self {
            Self { inner }
        }

        fn begin_transaction(&self, method_name: &str) -> Result<JsValue, BackendError> {
            let transaction = call_method0(&self.inner, method_name).map_err(to_backend_error)?;
            if transaction.is_null() || transaction.is_undefined() || !transaction.is_object() {
                return Err(BackendError::Io(format!(
                    "backend.{method_name}() must return a transaction object"
                )));
            }
            Ok(transaction)
        }
    }

    impl JsBackendConformanceFactory {
        fn new(inner: JsValue) -> Result<Self, LixError> {
            if inner.is_null() || inner.is_undefined() || !inner.is_object() {
                return Err(js_sdk_error(
                    "runBackendConformance() factory must be an object",
                ));
            }
            let object = Object::from(inner.clone());
            Ok(Self {
                inner,
                config: backend_conformance_config_from_js(&object)?,
            })
        }
    }

    impl BackendFactory for JsBackendConformanceFactory {
        type Backend = JsConformanceBackend;
        type Fixture = JsBackendConformanceFixture;

        fn create_fixture(&self) -> Self::Fixture {
            let fixture = call_method0(&self.inner, "createFixture")
                .expect("runBackendConformance() factory.createFixture() failed");
            if fixture.is_null() || fixture.is_undefined() || !fixture.is_object() {
                panic!("runBackendConformance() factory.createFixture() must return an object");
            }
            JsBackendConformanceFixture { inner: fixture }
        }

        fn config(&self) -> BackendTestConfig {
            self.config.clone()
        }
    }

    impl BackendFixture for JsBackendConformanceFixture {
        type Backend = JsConformanceBackend;

        fn open(&self) -> Self::Backend {
            let backend = call_method0(&self.inner, "open")
                .expect("runBackendConformance() fixture.open() failed");
            if backend.is_null() || backend.is_undefined() || !backend.is_object() {
                panic!("runBackendConformance() fixture.open() must return a backend object");
            }
            JsConformanceBackend {
                inner: JsBackend::new(backend.clone()),
                handle: backend,
            }
        }
    }

    impl Backend for JsConformanceBackend {
        type Read<'a>
            = JsBackendRead
        where
            Self: 'a;

        type Write<'a>
            = JsBackendWrite
        where
            Self: 'a;

        fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
            self.inner.begin_read(opts)
        }

        fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
            self.inner.begin_write(opts)
        }
    }

    impl Drop for JsConformanceBackend {
        fn drop(&mut self) {
            let _ = close_js_backend(&self.handle);
        }
    }

    unsafe impl Send for JsBackend {}
    unsafe impl Sync for JsBackend {}

    impl Backend for JsBackend {
        type Read<'a>
            = JsBackendRead
        where
            Self: 'a;

        type Write<'a>
            = JsBackendWrite
        where
            Self: 'a;
        fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
            self.begin_transaction("beginReadTransaction")
                .map(|inner| JsBackendRead { inner })
        }

        fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
            self.begin_transaction("beginWriteTransaction")
                .map(|inner| JsBackendWrite {
                    inner,
                    stats: WriteStats::default(),
                })
        }
    }

    #[derive(Clone)]
    struct JsBackendRead {
        inner: JsValue,
    }

    struct JsBackendWrite {
        inner: JsValue,
        stats: WriteStats,
    }

    struct JsRangeScan {
        transaction: JsValue,
        range: KeyRange,
        projection: CoreProjection,
        resume_after: Option<Key>,
    }

    unsafe impl Send for JsBackendRead {}
    unsafe impl Sync for JsBackendRead {}
    unsafe impl Send for JsBackendWrite {}
    unsafe impl Send for JsRangeScan {}

    impl BackendRead for JsBackendRead {
        type RangeScan<'cursor> = JsRangeScan;

        fn visit_keys<V>(
            &self,
            keys: &[Key],
            opts: GetOptions<'_>,
            visitor: &mut V,
        ) -> Result<(), BackendError>
        where
            V: PointVisitor + ?Sized,
        {
            let values = js_get_values(&self.inner, keys)?;
            if values.len() != keys.len() {
                return Err(BackendError::Corruption(format!(
                    "transaction.getValues returned {} values for {} keys",
                    values.len(),
                    keys.len()
                )));
            }
            for (index, (key, value)) in keys.iter().zip(values.iter()).enumerate() {
                let projected = value.as_deref().map(|value| match opts.projection {
                    CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
                    CoreProjection::FullValue => ProjectedValueRef::FullValue(value),
                });
                visitor.visit(index, key, projected)?;
            }
            Ok(())
        }

        fn with_range_scan<T, F>(
            &self,
            range: KeyRange,
            opts: ScanOptions<'_>,
            f: F,
        ) -> Result<T, BackendError>
        where
            F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
        {
            let mut scan = JsRangeScan {
                transaction: self.inner.clone(),
                range,
                projection: opts.projection,
                resume_after: opts.resume_after.cloned(),
            };
            f(&mut scan)
        }

        fn close(self) -> Result<(), BackendError> {
            call_method0(&self.inner, "rollback")
                .map(|_| ())
                .map_err(to_backend_error)
        }
    }

    impl BackendRangeScan for JsRangeScan {
        fn visit_next<V>(
            &mut self,
            limit_rows: usize,
            visitor: &mut V,
        ) -> Result<ScanResult, BackendError>
        where
            V: ScanVisitor + ?Sized,
        {
            if limit_rows == 0 {
                return Ok(ScanResult::default());
            }
            let request =
                kv_scan_request_to_js(&self.range, self.resume_after.as_ref(), limit_rows)?;
            let page = js_value_to_entry_page(
                call_method1(&self.transaction, "scanEntries", &request)
                    .map_err(to_backend_error)?,
                "transaction.scanEntries",
            )?;
            for entry in &page.entries {
                let value = match (&entry.value, self.projection) {
                    (_, CoreProjection::KeyOnly) => ProjectedValueRef::KeyOnly,
                    (Some(value), CoreProjection::FullValue) => {
                        ProjectedValueRef::FullValue(value.as_ref())
                    }
                    (None, CoreProjection::FullValue) => {
                        return Err(BackendError::Corruption(
                            "transaction.scanEntries omitted values for full-value scan".into(),
                        ));
                    }
                };
                visitor.visit(entry.key.as_ref(), value)?;
            }
            self.resume_after = page.resume_after;
            Ok(ScanResult {
                emitted: page.entries.len(),
                has_more: self.resume_after.is_some(),
            })
        }
    }

    impl BackendWrite for JsBackendWrite {
        fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
            let mut ops = Vec::with_capacity(entries.entries.len());
            for entry in entries.entries {
                ops.push(JsWriteOp::Put {
                    key: entry.key,
                    value: entry.value,
                });
            }
            self.write_ops(ops)
        }

        fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
            self.write_ops(keys.iter().cloned().map(JsWriteOp::Delete).collect())
        }

        fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
            self.write_ops(vec![JsWriteOp::DeleteRange(range)])
        }

        fn commit(self) -> Result<CommitResult, BackendError> {
            call_method0(&self.inner, "commit")
                .map_err(to_backend_error)
                .map(|_| CommitResult {
                    commit_id: None,
                    stats: self.stats,
                })
        }

        fn rollback(self) -> Result<(), BackendError> {
            call_method0(&self.inner, "rollback")
                .map(|_| ())
                .map_err(to_backend_error)
        }
    }

    fn call_method0(receiver: &JsValue, method_name: &str) -> Result<JsValue, LixError> {
        let method = Reflect::get(receiver, &JsValue::from_str(method_name))
            .map_err(|_| js_sdk_error(format!("{method_name} could not be read")))?;
        call_function0(&method, receiver)
    }

    fn call_method1(
        receiver: &JsValue,
        method_name: &str,
        arg1: &JsValue,
    ) -> Result<JsValue, LixError> {
        let method = Reflect::get(receiver, &JsValue::from_str(method_name))
            .map_err(|_| js_sdk_error(format!("{method_name} could not be read")))?;
        call_function1(&method, receiver, arg1)
    }

    fn call_function0(function: &JsValue, receiver: &JsValue) -> Result<JsValue, LixError> {
        let function = function
            .dyn_ref::<js_sys::Function>()
            .ok_or_else(|| js_sdk_error("backend method must be a function"))?;
        reject_promise(function.call0(receiver).map_err(js_to_lix_error)?)
    }

    fn call_function1(
        function: &JsValue,
        receiver: &JsValue,
        arg1: &JsValue,
    ) -> Result<JsValue, LixError> {
        let function = function
            .dyn_ref::<js_sys::Function>()
            .ok_or_else(|| js_sdk_error("backend method must be a function"))?;
        reject_promise(function.call1(receiver, arg1).map_err(js_to_lix_error)?)
    }

    fn reject_promise(value: JsValue) -> Result<JsValue, LixError> {
        if value.is_instance_of::<js_sys::Promise>() {
            return Err(js_sdk_error(
                "JavaScript Backend methods must return synchronously",
            ));
        }
        Ok(value)
    }

    fn bytes_to_js(bytes: &[u8]) -> JsValue {
        js_sys::Uint8Array::from(bytes).into()
    }

    fn js_value_to_bytes(value: JsValue, context: &str) -> Result<Vec<u8>, LixError> {
        if !value.is_instance_of::<js_sys::Uint8Array>() {
            return Err(js_sdk_error(format!("{context} must return Uint8Array")));
        }
        Ok(js_sys::Uint8Array::from(value).to_vec())
    }

    fn usize_to_js(value: usize) -> JsValue {
        JsValue::from_f64(value as f64)
    }

    enum JsWriteOp {
        Put { key: Key, value: StoredValue },
        Delete(Key),
        DeleteRange(KeyRange),
    }

    struct JsEntryPage {
        entries: Vec<JsEntry>,
        resume_after: Option<Key>,
    }

    struct JsEntry {
        key: Key,
        value: Option<Vec<u8>>,
    }

    impl JsBackendWrite {
        fn write_ops(&mut self, ops: Vec<JsWriteOp>) -> Result<(), BackendError> {
            if ops.is_empty() {
                return Ok(());
            }
            let batch = kv_write_batch_to_js(ops)?;
            let stats = js_value_to_write_stats(
                call_method1(&self.inner, "writeKvBatch", &batch).map_err(to_backend_error)?,
                "transaction.writeKvBatch",
            )
            .map_err(to_backend_error)?;
            self.stats.put_entries += stats.put_entries;
            self.stats.deleted_entries += stats.deleted_entries;
            self.stats.deleted_ranges += stats.deleted_ranges;
            self.stats.written_bytes += stats.written_bytes;
            self.stats.backend_calls += 1;
            Ok(())
        }
    }

    fn close_js_backend(backend: &JsValue) -> Result<(), LixError> {
        let method = Reflect::get(backend, &JsValue::from_str("close"))
            .map_err(|_| js_sdk_error("backend.close could not be read"))?;
        if method.is_undefined() || method.is_null() {
            return Ok(());
        }
        call_function0(&method, backend)?;
        Ok(())
    }

    fn backend_conformance_config_from_js(
        factory: &Object,
    ) -> Result<BackendTestConfig, LixError> {
        let mut config = BackendTestConfig::default();
        let value = Reflect::get(factory, &JsValue::from_str("config"))
            .map_err(|_| js_sdk_error("runBackendConformance() could not read config"))?;
        if value.is_undefined() || value.is_null() {
            return Ok(config);
        }
        if !value.is_object() {
            return Err(js_sdk_error(
                "runBackendConformance() config must be an object when provided",
            ));
        }
        let object = Object::from(value);
        if let Some(max_key_len) = optional_usize(&object, "maxKeyLen", "config")? {
            config.max_key_len = max_key_len;
        }
        if let Some(max_value_len) = optional_usize(&object, "maxValueLen", "config")? {
            config.max_value_len = max_value_len;
        }
        if let Some(ephemeral) = optional_bool(&object, "ephemeral", "config")? {
            config.ephemeral = ephemeral;
        }
        if let Some(supports_concurrent_writers) =
            optional_bool(&object, "supportsConcurrentWriters", "config")?
        {
            config.supports_concurrent_writers = supports_concurrent_writers;
        }
        Ok(config)
    }

    fn backend_conformance_report_to_js(
        report: lix_rs_sdk::BackendConformanceReport,
    ) -> Result<JsValue, LixError> {
        let object = Object::new();
        let tests = Array::new();
        for test in report.tests {
            let test_object = Object::new();
            Reflect::set(
                &test_object,
                &JsValue::from_str("name"),
                &JsValue::from_str(test.name),
            )
            .map_err(|_| js_sdk_error("could not set conformance test name"))?;
            match test.status {
                BackendConformanceStatus::Passed => {
                    set_string(&test_object, "status", "passed")?;
                }
                BackendConformanceStatus::Pending => {
                    set_string(&test_object, "status", "pending")?;
                }
                BackendConformanceStatus::Failed(error) => {
                    set_string(&test_object, "status", "failed")?;
                    Reflect::set(
                        &test_object,
                        &JsValue::from_str("error"),
                        &JsValue::from_str(&error),
                    )
                    .map_err(|_| js_sdk_error("could not set conformance test error"))?;
                }
            }
            tests.push(&test_object);
        }
        Reflect::set(&object, &JsValue::from_str("tests"), &tests)
            .map_err(|_| js_sdk_error("could not set conformance tests"))?;
        Ok(object.into())
    }

    fn to_backend_error(error: LixError) -> BackendError {
        BackendError::Io(error.to_string())
    }

    fn js_get_values(
        transaction: &JsValue,
        keys: &[Key],
    ) -> Result<Vec<Option<Vec<u8>>>, BackendError> {
        let object = Object::new();
        let js_keys = Array::new();
        for key in keys {
            js_keys.push(&bytes_to_js(key.0.as_ref()));
        }
        Reflect::set(&object, &JsValue::from_str("keys"), &js_keys)
            .map_err(|_| to_backend_error(js_sdk_error("could not set get request keys")))?;
        let response =
            call_method1(transaction, "getValues", &object.into()).map_err(to_backend_error)?;
        js_value_to_values(response, "transaction.getValues").map_err(to_backend_error)
    }

    fn choose_scan_after(explicit_after: Option<&Key>, lower_after: Option<&Key>) -> Option<Key> {
        match (explicit_after, lower_after) {
            (Some(explicit_after), Some(lower_after)) => {
                Some(std::cmp::max(explicit_after, lower_after).clone())
            }
            (Some(explicit_after), None) => Some(explicit_after.clone()),
            (None, Some(lower_after)) => Some(lower_after.clone()),
            (None, None) => None,
        }
    }

    fn kv_bound_to_js(bound: &Bound<Key>) -> Result<JsValue, BackendError> {
        let object = Object::new();
        match bound {
            Bound::Included(key) => {
                set_string(&object, "kind", "included").map_err(to_backend_error)?;
                Reflect::set(
                    &object,
                    &JsValue::from_str("key"),
                    &bytes_to_js(key.0.as_ref()),
                )
                .map_err(|_| to_backend_error(js_sdk_error("could not set bound.key")))?;
            }
            Bound::Excluded(key) => {
                set_string(&object, "kind", "excluded").map_err(to_backend_error)?;
                Reflect::set(
                    &object,
                    &JsValue::from_str("key"),
                    &bytes_to_js(key.0.as_ref()),
                )
                .map_err(|_| to_backend_error(js_sdk_error("could not set bound.key")))?;
            }
            Bound::Unbounded => {
                set_string(&object, "kind", "unbounded").map_err(to_backend_error)?;
            }
        }
        Ok(object.into())
    }

    fn kv_scan_range_to_js(range: &KeyRange) -> Result<JsValue, BackendError> {
        let object = Object::new();
        Reflect::set(
            &object,
            &JsValue::from_str("lower"),
            &kv_bound_to_js(&range.lower)?,
        )
        .map_err(|_| to_backend_error(js_sdk_error("could not set range.lower")))?;
        Reflect::set(
            &object,
            &JsValue::from_str("upper"),
            &kv_bound_to_js(&range.upper)?,
        )
        .map_err(|_| to_backend_error(js_sdk_error("could not set range.upper")))?;
        Ok(object.into())
    }

    fn kv_scan_range_and_after_to_js(
        range: &KeyRange,
        after: Option<&Key>,
    ) -> Result<(JsValue, Option<Key>), BackendError> {
        let lower_after = match &range.lower {
            Bound::Excluded(start) => Some(start),
            _ => None,
        };
        Ok((
            kv_scan_range_to_js(range)?,
            choose_scan_after(after, lower_after),
        ))
    }

    fn kv_scan_request_to_js(
        range: &KeyRange,
        after: Option<&Key>,
        limit: usize,
    ) -> Result<JsValue, BackendError> {
        let object = Object::new();
        let (range, after) = kv_scan_range_and_after_to_js(range, after)?;
        Reflect::set(&object, &JsValue::from_str("range"), &range)
            .map_err(|_| to_backend_error(js_sdk_error("could not set scan request range")))?;
        let after = after
            .as_ref()
            .map(|key| bytes_to_js(key.0.as_ref()))
            .unwrap_or(JsValue::NULL);
        Reflect::set(&object, &JsValue::from_str("after"), &after)
            .map_err(|_| to_backend_error(js_sdk_error("could not set scan request after")))?;
        Reflect::set(&object, &JsValue::from_str("limit"), &usize_to_js(limit))
            .map_err(|_| to_backend_error(js_sdk_error("could not set scan request limit")))?;
        Ok(object.into())
    }

    fn kv_write_batch_to_js(ops: Vec<JsWriteOp>) -> Result<JsValue, BackendError> {
        let object = Object::new();
        let js_ops = Array::new();
        for op in ops {
            let op_object = Object::new();
            match op {
                JsWriteOp::Put { key, value } => {
                    set_string(&op_object, "kind", "put").map_err(to_backend_error)?;
                    Reflect::set(
                        &op_object,
                        &JsValue::from_str("key"),
                        &bytes_to_js(key.0.as_ref()),
                    )
                    .map_err(|_| to_backend_error(js_sdk_error("could not set write put key")))?;
                    Reflect::set(
                        &op_object,
                        &JsValue::from_str("value"),
                        &bytes_to_js(value.bytes.as_ref()),
                    )
                    .map_err(|_| to_backend_error(js_sdk_error("could not set write put value")))?;
                }
                JsWriteOp::Delete(key) => {
                    set_string(&op_object, "kind", "delete").map_err(to_backend_error)?;
                    Reflect::set(
                        &op_object,
                        &JsValue::from_str("key"),
                        &bytes_to_js(key.0.as_ref()),
                    )
                    .map_err(|_| {
                        to_backend_error(js_sdk_error("could not set write delete key"))
                    })?;
                }
                JsWriteOp::DeleteRange(range) => {
                    set_string(&op_object, "kind", "deleteRange").map_err(to_backend_error)?;
                    Reflect::set(
                        &op_object,
                        &JsValue::from_str("range"),
                        &kv_scan_range_to_js(&range)?,
                    )
                    .map_err(|_| {
                        to_backend_error(js_sdk_error("could not set write delete range"))
                    })?;
                }
            }
            js_ops.push(&op_object);
        }
        Reflect::set(&object, &JsValue::from_str("ops"), &js_ops)
            .map_err(|_| to_backend_error(js_sdk_error("could not set write ops")))?;
        Ok(object.into())
    }

    fn js_value_to_values(value: JsValue, context: &str) -> Result<Vec<Option<Vec<u8>>>, LixError> {
        let object = expect_backend_object(value, context)?;
        let values = required_array(&object, "values", context)?;
        let mut out = Vec::with_capacity(values.length() as usize);
        for value in values.iter() {
            if value.is_null() || value.is_undefined() {
                out.push(None);
            } else {
                out.push(Some(js_value_to_bytes(value, &format!("{context}.values"))?));
            }
        }
        Ok(out)
    }

    fn js_value_to_entry_page(value: JsValue, context: &str) -> Result<JsEntryPage, BackendError> {
        let object = expect_backend_object(value, context).map_err(to_backend_error)?;
        let keys = byte_array_property(&object, "keys", context)?;
        let values = byte_array_property(&object, "values", context)?;
        if keys.len() != values.len() {
            return Err(BackendError::Corruption(format!(
                "{context}.keys and {context}.values length mismatch"
            )));
        }
        let entries = keys
            .into_iter()
            .zip(values)
            .map(|(key, value)| JsEntry {
                key: Key(Bytes::from(key)),
                value: Some(value),
            })
            .collect();
        Ok(JsEntryPage {
            entries,
            resume_after: optional_bytes_property(&object, "resumeAfter", context)?
                .map(Bytes::from)
                .map(Key),
        })
    }

    fn js_value_to_write_stats(value: JsValue, context: &str) -> Result<WriteStats, LixError> {
        let object = expect_backend_object(value, context)?;
        Ok(WriteStats {
            put_entries: required_usize(&object, "puts", context)? as u64,
            deleted_entries: required_usize(&object, "deletes", context)? as u64,
            deleted_ranges: required_usize(&object, "deleteRanges", context)? as u64,
            written_bytes: required_usize(&object, "bytesWritten", context)? as u64,
            backend_calls: 1,
        })
    }

    fn byte_array_property(
        object: &Object,
        key: &str,
        context: &str,
    ) -> Result<Vec<Vec<u8>>, BackendError> {
        let array = required_array(object, key, context).map_err(to_backend_error)?;
        let mut page = Vec::with_capacity(array.length() as usize);
        for value in array.iter() {
            page.push(
                js_value_to_bytes(value, &format!("{context}.{key}")).map_err(to_backend_error)?,
            );
        }
        Ok(page)
    }

    fn optional_bytes_property(
        object: &Object,
        key: &str,
        context: &str,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let value = Reflect::get(object, &JsValue::from_str(key)).map_err(|_| {
            to_backend_error(js_sdk_error(format!("{context}.{key} could not be read")))
        })?;
        if value.is_undefined() || value.is_null() {
            return Ok(None);
        }
        js_value_to_bytes(value, &format!("{context}.{key}"))
            .map(Some)
            .map_err(to_backend_error)
    }

    fn expect_backend_object(value: JsValue, context: &str) -> Result<Object, LixError> {
        if value.is_null() || value.is_undefined() || !value.is_object() {
            return Err(js_sdk_error(format!("{context} must return an object")));
        }
        Ok(Object::from(value))
    }

    fn required_array(object: &Object, key: &str, context: &str) -> Result<Array, LixError> {
        let value = Reflect::get(object, &JsValue::from_str(key))
            .map_err(|_| js_sdk_error(format!("{context}.{key} could not be read")))?;
        if !Array::is_array(&value) {
            return Err(js_sdk_error(format!("{context}.{key} must be an array")));
        }
        Ok(Array::from(&value))
    }

    fn required_usize(object: &Object, key: &str, context: &str) -> Result<usize, LixError> {
        let value = Reflect::get(object, &JsValue::from_str(key))
            .map_err(|_| js_sdk_error(format!("{context}.{key} could not be read")))?;
        let number = value
            .as_f64()
            .ok_or_else(|| js_sdk_error(format!("{context}.{key} must be a number")))?;
        if !number.is_finite() || number < 0.0 || number.fract() != 0.0 {
            return Err(js_sdk_error(format!(
                "{context}.{key} must be a non-negative integer"
            )));
        }
        Ok(number as usize)
    }

    fn js_to_lix_error(value: JsValue) -> LixError {
        if let Some(message) = value.as_string() {
            return js_sdk_error(message);
        }
        let code = Reflect::get(&value, &JsValue::from_str("code"))
            .ok()
            .and_then(|code| code.as_string());
        let message = Reflect::get(&value, &JsValue::from_str("message"))
            .ok()
            .and_then(|message| message.as_string())
            .unwrap_or_else(|| "JavaScript backend error".to_string());
        let hint = Reflect::get(&value, &JsValue::from_str("hint"))
            .ok()
            .and_then(|hint| hint.as_string());
        let details = Reflect::get(&value, &JsValue::from_str("details"))
            .ok()
            .and_then(|details| {
                if details.is_undefined() || details.is_null() {
                    None
                } else {
                    serde_wasm_bindgen::from_value(details).ok()
                }
            });
        let mut error = LixError::new(
            code.unwrap_or_else(|| "LIX_ERROR_JS_SDK".to_string()),
            message,
        );
        if let Some(hint) = hint {
            error = error.with_hint(hint);
        }
        if let Some(details) = details {
            error = error.with_details(details);
        }
        error
    }

    fn parse_create_branch_options(value: JsValue) -> Result<CreateBranchOptions, LixError> {
        let object = expect_object(value, "createBranch")?;
        let id = optional_string(&object, "id", "createBranch")?;
        let name = required_string(&object, "name", "createBranch")?;
        let from_commit_id = optional_string(&object, "fromCommitId", "createBranch")?;
        Ok(CreateBranchOptions {
            id,
            name,
            from_commit_id,
        })
    }

    fn parse_switch_branch_options(value: JsValue) -> Result<SwitchBranchOptions, LixError> {
        let object = expect_object(value, "switchBranch")?;
        let branch_id = required_string(&object, "branchId", "switchBranch")?;
        Ok(SwitchBranchOptions { branch_id })
    }

    fn parse_merge_branch_options(value: JsValue) -> Result<MergeBranchOptions, LixError> {
        let object = expect_object(value, "mergeBranch")?;
        let source_branch_id = required_string(&object, "sourceBranchId", "mergeBranch")?;
        Ok(MergeBranchOptions { source_branch_id })
    }

    fn parse_merge_branch_preview_options(
        value: JsValue,
    ) -> Result<MergeBranchPreviewOptions, LixError> {
        let object = expect_object(value, "mergeBranchPreview")?;
        let source_branch_id = required_string(&object, "sourceBranchId", "mergeBranchPreview")?;
        Ok(MergeBranchPreviewOptions { source_branch_id })
    }

    fn expect_object(value: JsValue, method: &str) -> Result<Object, LixError> {
        if value.is_null() || value.is_undefined() || !value.is_object() {
            return Err(LixError::new(
                "LIX_ERROR_JS_SDK",
                format!("{method}() options must be an object"),
            ));
        }
        Ok(Object::from(value))
    }

    fn invalid_argument_error(
        operation: &str,
        argument: &str,
        expected: &str,
        actual_value: &JsValue,
    ) -> LixError {
        LixError::new(
            "LIX_INVALID_ARGUMENT",
            format!(
                "lix.{operation}() expected {argument} to be {} {expected}",
                expected_article(expected)
            ),
        )
        .with_details(json!({
            "operation": operation,
            "argument": argument,
            "expected": expected,
            "actual": js_type_name(actual_value),
        }))
    }

    fn expected_article(expected: &str) -> &'static str {
        match expected.chars().next().map(|c| c.to_ascii_lowercase()) {
            Some('a' | 'e' | 'i' | 'o' | 'u') => "an",
            _ => "a",
        }
    }

    fn js_type_name(value: &JsValue) -> &'static str {
        if value.is_null() {
            "null"
        } else if Array::is_array(value) {
            "array"
        } else if value.is_undefined() {
            "undefined"
        } else if value.is_string() {
            "string"
        } else if value.as_bool().is_some() {
            "boolean"
        } else if value.as_f64().is_some() {
            "number"
        } else if value.is_function() {
            "function"
        } else if value.is_object() {
            "object"
        } else {
            "unknown"
        }
    }

    fn required_string(object: &Object, key: &str, method: &str) -> Result<String, LixError> {
        let value = Reflect::get(object, &JsValue::from_str(key)).map_err(|_| {
            LixError::new(
                "LIX_ERROR_JS_SDK",
                format!("{method}() could not read {key}"),
            )
        })?;
        if let Some(value) = value.as_string() {
            if !value.is_empty() {
                return Ok(value);
            }
        }
        Err(LixError::new(
            "LIX_ERROR_JS_SDK",
            format!("{method}() requires non-empty string {key}"),
        ))
    }

    fn optional_string(
        object: &Object,
        key: &str,
        method: &str,
    ) -> Result<Option<String>, LixError> {
        let value = Reflect::get(object, &JsValue::from_str(key)).map_err(|_| {
            LixError::new(
                "LIX_ERROR_JS_SDK",
                format!("{method}() could not read {key}"),
            )
        })?;
        if value.is_undefined() || value.is_null() {
            return Ok(None);
        }
        if let Some(value) = value.as_string() {
            if !value.is_empty() {
                return Ok(Some(value));
            }
        }
        Err(LixError::new(
            "LIX_ERROR_JS_SDK",
            format!("{method}() requires {key} to be a non-empty string when provided"),
        ))
    }

    fn optional_bool(
        object: &Object,
        key: &str,
        context: &str,
    ) -> Result<Option<bool>, LixError> {
        let value = Reflect::get(object, &JsValue::from_str(key))
            .map_err(|_| js_sdk_error(format!("{context}.{key} could not be read")))?;
        if value.is_undefined() || value.is_null() {
            return Ok(None);
        }
        value
            .as_bool()
            .map(Some)
            .ok_or_else(|| js_sdk_error(format!("{context}.{key} must be a boolean")))
    }

    fn optional_usize(
        object: &Object,
        key: &str,
        context: &str,
    ) -> Result<Option<usize>, LixError> {
        let value = Reflect::get(object, &JsValue::from_str(key))
            .map_err(|_| js_sdk_error(format!("{context}.{key} could not be read")))?;
        if value.is_undefined() || value.is_null() {
            return Ok(None);
        }
        let number = value
            .as_f64()
            .ok_or_else(|| js_sdk_error(format!("{context}.{key} must be a number")))?;
        if !number.is_finite() || number < 0.0 || number.fract() != 0.0 {
            return Err(js_sdk_error(format!(
                "{context}.{key} must be a non-negative integer"
            )));
        }
        Ok(Some(number as usize))
    }

    fn value_from_js(value: JsValue) -> Result<Value, LixError> {
        if value.is_null() || value.is_undefined() || !value.is_object() {
            return Err(invalid_param(
                "parameter must be an explicit Lix value object",
                &value,
            ));
        }

        let object = Object::from(value.clone());
        let kind = Reflect::get(&object, &JsValue::from_str("kind"))
            .ok()
            .and_then(|value| value.as_string());
        match kind.as_deref() {
            Some("null") => Ok(Value::Null),
            Some("boolean") => Ok(Value::Boolean(
                Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_bool())
                    .ok_or_else(|| invalid_param("boolean value must be boolean", &value))?,
            )),
            Some("integer") => {
                let value = Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_f64())
                    .ok_or_else(|| invalid_param("integer value must be number", &value))?;
                if !value.is_finite() || value.fract() != 0.0 {
                    return Err(invalid_param_message(
                        "integer value must be a finite integer",
                    ));
                }
                Ok(Value::Integer(value as i64))
            }
            Some("real") => {
                let value = Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_f64())
                    .ok_or_else(|| invalid_param("real value must be number", &value))?;
                if !value.is_finite() {
                    return Err(invalid_param_message("real value must be a finite number"));
                }
                Ok(Value::Real(value))
            }
            Some("text") => Ok(Value::Text(
                Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_string())
                    .ok_or_else(|| invalid_param("text value must be string", &value))?,
            )),
            Some("json") => {
                let value = Reflect::get(&object, &JsValue::from_str("value"))
                    .map_err(|_| invalid_param("json value is missing", &value))?;
                let json = serde_wasm_bindgen::from_value(value).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("json value must be JSON-serializable: {error}"),
                    )
                })?;
                Ok(Value::Json(json))
            }
            Some("blob") => {
                let base64 = Reflect::get(&object, &JsValue::from_str("base64"))
                    .ok()
                    .and_then(|value| value.as_string())
                    .ok_or_else(|| invalid_param("blob base64 must be string", &value))?;
                let bytes =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, base64)
                        .map_err(|error| {
                            LixError::new(
                                LixError::CODE_INVALID_PARAM,
                                format!("blob base64 must be valid base64: {error}"),
                            )
                        })?;
                Ok(Value::Blob(bytes))
            }
            _ => Err(invalid_param(
                "parameter must be an explicit Lix value object",
                &value,
            )),
        }
    }

    fn execute_result_to_js(result: ExecuteResult) -> Result<JsValue, LixError> {
        let object = Object::new();
        let columns = Array::new();
        for column in result.columns() {
            columns.push(&JsValue::from_str(column));
        }
        Reflect::set(&object, &JsValue::from_str("columns"), &columns)
            .map_err(|_| js_sdk_error("could not set columns"))?;
        let values = Array::new();
        for row in result.rows() {
            let row_values = Array::new();
            for value in row.values() {
                row_values.push(&value_to_js(value)?);
            }
            values.push(&row_values);
        }
        Reflect::set(&object, &JsValue::from_str("rows"), &values)
            .map_err(|_| js_sdk_error("could not set rows"))?;
        set_number(&object, "rowsAffected", result.rows_affected() as f64)?;
        let notices = Array::new();
        for notice in result.notices() {
            let notice_object = Object::new();
            set_string(&notice_object, "code", &notice.code)?;
            set_string(&notice_object, "message", &notice.message)?;
            if let Some(hint) = &notice.hint {
                set_string(&notice_object, "hint", hint)?;
            }
            notices.push(&notice_object);
        }
        Reflect::set(&object, &JsValue::from_str("notices"), &notices)
            .map_err(|_| js_sdk_error("could not set notices"))?;
        Ok(object.into())
    }

    fn merge_branch_preview_to_js(
        result: lix_rs_sdk::MergeBranchPreview,
    ) -> Result<JsValue, LixError> {
        let object = Object::new();
        let outcome = match result.outcome {
            lix_rs_sdk::MergeBranchOutcome::AlreadyUpToDate => "alreadyUpToDate",
            lix_rs_sdk::MergeBranchOutcome::FastForward => "fastForward",
            lix_rs_sdk::MergeBranchOutcome::MergeCommitted => "mergeCommitted",
        };
        set_string(&object, "outcome", outcome)?;
        set_string(&object, "targetBranchId", &result.target_branch_id)?;
        set_string(&object, "sourceBranchId", &result.source_branch_id)?;
        set_string(&object, "baseCommitId", &result.base_commit_id)?;
        set_string(&object, "targetHeadCommitId", &result.target_head_commit_id)?;
        set_string(&object, "sourceHeadCommitId", &result.source_head_commit_id)?;
        Reflect::set(
            &object,
            &JsValue::from_str("changeStats"),
            &merge_change_stats_to_js(&result.change_stats)?,
        )
        .map_err(|_| js_sdk_error("could not set changeStats"))?;
        let conflicts = Array::new();
        for conflict in result.conflicts {
            conflicts.push(&merge_conflict_to_js(&conflict)?);
        }
        Reflect::set(&object, &JsValue::from_str("conflicts"), &conflicts)
            .map_err(|_| js_sdk_error("could not set conflicts"))?;
        Ok(object.into())
    }

    fn merge_change_stats_to_js(stats: &lix_rs_sdk::MergeChangeStats) -> Result<JsValue, LixError> {
        let object = Object::new();
        set_number(&object, "total", stats.total as f64)?;
        set_number(&object, "added", stats.added as f64)?;
        set_number(&object, "modified", stats.modified as f64)?;
        set_number(&object, "removed", stats.removed as f64)?;
        Ok(object.into())
    }

    fn merge_conflict_to_js(conflict: &lix_rs_sdk::MergeConflict) -> Result<JsValue, LixError> {
        let object = Object::new();
        let kind = match conflict.kind {
            lix_rs_sdk::MergeConflictKind::SameEntityChanged => "sameEntityChanged",
        };
        set_string(&object, "kind", kind)?;
        set_string(&object, "schemaKey", &conflict.schema_key)?;
        set_json(&object, "entityPk", &conflict.entity_pk)?;
        set_optional_string(&object, "fileId", conflict.file_id.as_deref())?;
        Reflect::set(
            &object,
            &JsValue::from_str("target"),
            &merge_conflict_side_to_js(&conflict.target)?,
        )
        .map_err(|_| js_sdk_error("could not set target conflict side"))?;
        Reflect::set(
            &object,
            &JsValue::from_str("source"),
            &merge_conflict_side_to_js(&conflict.source)?,
        )
        .map_err(|_| js_sdk_error("could not set source conflict side"))?;
        Ok(object.into())
    }

    fn merge_conflict_side_to_js(
        side: &lix_rs_sdk::MergeConflictSide,
    ) -> Result<JsValue, LixError> {
        let object = Object::new();
        let kind = match side.kind {
            lix_rs_sdk::MergeConflictChangeKind::Added => "added",
            lix_rs_sdk::MergeConflictChangeKind::Modified => "modified",
            lix_rs_sdk::MergeConflictChangeKind::Removed => "removed",
        };
        set_string(&object, "kind", kind)?;
        set_optional_string(&object, "beforeChangeId", side.before_change_id.as_deref())?;
        set_optional_string(&object, "afterChangeId", side.after_change_id.as_deref())?;
        Ok(object.into())
    }

    fn value_to_js(value: &Value) -> Result<JsValue, LixError> {
        let object = Object::new();
        match value {
            Value::Null => {
                set_string(&object, "kind", "null")?;
                Reflect::set(&object, &JsValue::from_str("value"), &JsValue::NULL)
                    .map_err(|_| js_sdk_error("could not set null value"))?;
            }
            Value::Boolean(value) => {
                set_string(&object, "kind", "boolean")?;
                Reflect::set(
                    &object,
                    &JsValue::from_str("value"),
                    &JsValue::from_bool(*value),
                )
                .map_err(|_| js_sdk_error("could not set boolean value"))?;
            }
            Value::Integer(value) => {
                set_string(&object, "kind", "integer")?;
                set_number(&object, "value", *value as f64)?;
            }
            Value::Real(value) => {
                set_string(&object, "kind", "real")?;
                set_number(&object, "value", *value)?;
            }
            Value::Text(value) => {
                set_string(&object, "kind", "text")?;
                set_string(&object, "value", value)?;
            }
            Value::Json(value) => {
                set_string(&object, "kind", "json")?;
                let serializer = serde_wasm_bindgen::Serializer::json_compatible();
                let value = value.serialize(&serializer).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_JS_SDK",
                        format!("could not serialize JSON value: {error}"),
                    )
                })?;
                Reflect::set(&object, &JsValue::from_str("value"), &value)
                    .map_err(|_| js_sdk_error("could not set json value"))?;
            }
            Value::Blob(value) => {
                set_string(&object, "kind", "blob")?;
                set_string(
                    &object,
                    "base64",
                    &base64::Engine::encode(&base64::engine::general_purpose::STANDARD, value),
                )?;
            }
        }
        Ok(object.into())
    }

    fn set_string(object: &Object, key: &str, value: &str) -> Result<(), LixError> {
        Reflect::set(object, &JsValue::from_str(key), &JsValue::from_str(value))
            .map(|_| ())
            .map_err(|_| js_sdk_error(format!("could not set {key}")))
    }

    fn set_optional_string(
        object: &Object,
        key: &str,
        value: Option<&str>,
    ) -> Result<(), LixError> {
        let value = value.map(JsValue::from_str).unwrap_or(JsValue::NULL);
        Reflect::set(object, &JsValue::from_str(key), &value)
            .map(|_| ())
            .map_err(|_| js_sdk_error(format!("could not set {key}")))
    }

    fn set_number(object: &Object, key: &str, value: f64) -> Result<(), LixError> {
        Reflect::set(object, &JsValue::from_str(key), &JsValue::from_f64(value))
            .map(|_| ())
            .map_err(|_| js_sdk_error(format!("could not set {key}")))
    }

    fn set_json(object: &Object, key: &str, value: &serde_json::Value) -> Result<(), LixError> {
        let serializer = serde_wasm_bindgen::Serializer::json_compatible();
        let value = value.serialize(&serializer).map_err(|error| {
            LixError::new(
                "LIX_ERROR_JS_SDK",
                format!("could not serialize JSON value for {key}: {error}"),
            )
        })?;
        Reflect::set(object, &JsValue::from_str(key), &value)
            .map(|_| ())
            .map_err(|_| js_sdk_error(format!("could not set {key}")))
    }

    fn invalid_param(message: impl Into<String>, value: &JsValue) -> LixError {
        LixError::new(LixError::CODE_INVALID_PARAM, message.into()).with_details(json!({
            "operation": "execute",
            "actual": js_type_name(value),
        }))
    }

    fn invalid_param_message(message: impl Into<String>) -> LixError {
        LixError::new(LixError::CODE_INVALID_PARAM, message.into()).with_details(json!({
            "operation": "execute",
        }))
    }

    fn js_sdk_error(message: impl Into<String>) -> LixError {
        LixError::new("LIX_ERROR_JS_SDK", message.into())
    }

    fn transaction_closed_error() -> LixError {
        LixError::new("LIX_INVALID_TRANSACTION_STATE", "Lix transaction is closed")
    }

    fn js_error(error: LixError) -> JsValue {
        let js_error = js_sys::Error::new(&error.message);
        let object: &Object = js_error.as_ref();
        let _ = Reflect::set(
            object,
            &JsValue::from_str("code"),
            &JsValue::from_str(&error.code),
        );
        if let Some(hint) = error.hint {
            let _ = Reflect::set(
                object,
                &JsValue::from_str("hint"),
                &JsValue::from_str(&hint),
            );
        }
        if let Some(details) = error.details {
            let serializer = serde_wasm_bindgen::Serializer::json_compatible();
            if let Ok(value) = details.serialize(&serializer) {
                let _ = Reflect::set(object, &JsValue::from_str("details"), &value);
            }
        }
        js_error.into()
    }
}
