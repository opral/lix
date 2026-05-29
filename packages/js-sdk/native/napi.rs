use lix_rs_sdk::{
    open_lix, open_lix_with_backend, CreateBranchOptions as RsCreateBranchOptions,
    CreateBranchReceipt, ExecuteResult as RsExecuteResult, InMemoryBackend, Lix as RsLix, LixError,
    LixTransaction as RsLixTransaction, MergeBranchOptions as RsMergeBranchOptions,
    MergeBranchOutcome, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    MergeChangeStats, MergeConflict, MergeConflictChangeKind, MergeConflictKind, MergeConflictSide,
    OpenLixOptions as RsOpenLixOptions, SqliteBackend, SqliteBackendOptions,
    SwitchBranchOptions as RsSwitchBranchOptions, SwitchBranchReceipt, Value,
};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::runtime::{Builder, Runtime};

#[napi(js_name = "Lix")]
pub struct NativeLix {
    rt: Runtime,
    lix: Option<NativeLixInner>,
}

enum NativeLixInner {
    Memory(RsLix<InMemoryBackend>),
    Sqlite(RsLix<SqliteBackend>),
}

enum NativeLixTransactionInner {
    Memory(RsLixTransaction<InMemoryBackend>),
    Sqlite(RsLixTransaction<SqliteBackend>),
}

impl NativeLixInner {
    async fn execute(
        &self,
        sql: &str,
        params: &[Value],
    ) -> std::result::Result<RsExecuteResult, LixError> {
        match self {
            Self::Memory(lix) => lix.execute(sql, params).await,
            Self::Sqlite(lix) => lix.execute(sql, params).await,
        }
    }

    async fn begin_transaction(&self) -> std::result::Result<NativeLixTransactionInner, LixError> {
        match self {
            Self::Memory(lix) => Ok(NativeLixTransactionInner::Memory(
                lix.begin_transaction().await?,
            )),
            Self::Sqlite(lix) => Ok(NativeLixTransactionInner::Sqlite(
                lix.begin_transaction().await?,
            )),
        }
    }

    async fn active_branch_id(&self) -> std::result::Result<String, LixError> {
        match self {
            Self::Memory(lix) => lix.active_branch_id().await,
            Self::Sqlite(lix) => lix.active_branch_id().await,
        }
    }

    async fn create_branch(
        &self,
        options: RsCreateBranchOptions,
    ) -> std::result::Result<CreateBranchReceipt, LixError> {
        match self {
            Self::Memory(lix) => lix.create_branch(options).await,
            Self::Sqlite(lix) => lix.create_branch(options).await,
        }
    }

    async fn switch_branch(
        &self,
        options: RsSwitchBranchOptions,
    ) -> std::result::Result<SwitchBranchReceipt, LixError> {
        match self {
            Self::Memory(lix) => lix.switch_branch(options).await,
            Self::Sqlite(lix) => lix.switch_branch(options).await,
        }
    }

    async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> std::result::Result<MergeBranchPreview, LixError> {
        match self {
            Self::Memory(lix) => lix.merge_branch_preview(options).await,
            Self::Sqlite(lix) => lix.merge_branch_preview(options).await,
        }
    }

    async fn merge_branch(
        &self,
        options: RsMergeBranchOptions,
    ) -> std::result::Result<MergeBranchReceipt, LixError> {
        match self {
            Self::Memory(lix) => lix.merge_branch(options).await,
            Self::Sqlite(lix) => lix.merge_branch(options).await,
        }
    }

    async fn close(&self) -> std::result::Result<(), LixError> {
        match self {
            Self::Memory(lix) => lix.close().await,
            Self::Sqlite(lix) => lix.close().await,
        }
    }
}

impl NativeLixTransactionInner {
    async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> std::result::Result<RsExecuteResult, LixError> {
        match self {
            Self::Memory(transaction) => transaction.execute(sql, params).await,
            Self::Sqlite(transaction) => transaction.execute(sql, params).await,
        }
    }

    async fn commit(self) -> std::result::Result<(), LixError> {
        match self {
            Self::Memory(transaction) => transaction.commit().await,
            Self::Sqlite(transaction) => transaction.commit().await,
        }
    }

    async fn rollback(self) -> std::result::Result<(), LixError> {
        match self {
            Self::Memory(transaction) => transaction.rollback().await,
            Self::Sqlite(transaction) => transaction.rollback().await,
        }
    }
}

#[napi]
impl NativeLix {
    #[napi(factory, js_name = "openMemory")]
    pub fn open_memory(env: Env) -> Result<Self> {
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(to_napi_error)?;
        let lix = rt
            .block_on(open_lix(RsOpenLixOptions::default()))
            .map_err(|error| throw_lix_error(&env, error))?;
        Ok(Self {
            rt,
            lix: Some(NativeLixInner::Memory(lix)),
        })
    }

    #[napi(factory, js_name = "openSqlite")]
    pub fn open_sqlite(env: Env, path: String) -> Result<Self> {
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(to_napi_error)?;
        let backend = SqliteBackend::new(SqliteBackendOptions { path: path.into() })
            .map_err(|error| throw_lix_error(&env, error.into()))?;
        let lix = rt
            .block_on(open_lix_with_backend(backend))
            .map_err(|error| throw_lix_error(&env, error))?;
        Ok(Self {
            rt,
            lix: Some(NativeLixInner::Sqlite(lix)),
        })
    }

    #[napi]
    pub fn execute(
        &self,
        env: Env,
        sql: String,
        params: Option<Vec<LixValue>>,
    ) -> Result<ExecuteResult> {
        let params = match params {
            Some(params) => params
                .into_iter()
                .map(Value::try_from)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| throw_lix_error(&env, error))?,
            None => Vec::new(),
        };
        let result = self
            .rt
            .block_on(
                self.lix()
                    .map_err(|error| throw_lix_error(&env, error))?
                    .execute(&sql, &params),
            )
            .map_err(|error| throw_lix_error(&env, error))?;
        ExecuteResult::try_from(result).map_err(|error| throw_lix_error(&env, error))
    }

    #[napi(js_name = "beginTransaction")]
    pub fn begin_transaction(&self, env: Env) -> Result<NativeLixTransaction> {
        let transaction = self
            .rt
            .block_on(
                self.lix()
                    .map_err(|error| throw_lix_error(&env, error))?
                    .begin_transaction(),
            )
            .map_err(|error| throw_lix_error(&env, error))?;
        NativeLixTransaction::new(transaction)
    }

    #[napi(js_name = "activeBranchId")]
    pub fn active_branch_id(&self, env: Env) -> Result<String> {
        self.rt
            .block_on(
                self.lix()
                    .map_err(|error| throw_lix_error(&env, error))?
                    .active_branch_id(),
            )
            .map_err(|error| throw_lix_error(&env, error))
    }

    #[napi(js_name = "createBranch")]
    pub fn create_branch(
        &self,
        env: Env,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceiptDto> {
        let receipt = self
            .rt
            .block_on(
                self.lix()
                    .map_err(|error| throw_lix_error(&env, error))?
                    .create_branch(options.into()),
            )
            .map_err(|error| throw_lix_error(&env, error))?;
        Ok(CreateBranchReceiptDto::from(receipt))
    }

    #[napi(js_name = "switchBranch")]
    pub fn switch_branch(
        &self,
        env: Env,
        options: SwitchBranchOptions,
    ) -> Result<SwitchBranchReceiptDto> {
        let receipt = self
            .rt
            .block_on(
                self.lix()
                    .map_err(|error| throw_lix_error(&env, error))?
                    .switch_branch(options.into()),
            )
            .map_err(|error| throw_lix_error(&env, error))?;
        Ok(SwitchBranchReceiptDto::from(receipt))
    }

    #[napi(js_name = "mergeBranchPreview")]
    pub fn merge_branch_preview(
        &self,
        env: Env,
        options: MergeBranchOptions,
    ) -> Result<MergeBranchPreviewDto> {
        let preview = self
            .rt
            .block_on(
                self.lix()
                    .map_err(|error| throw_lix_error(&env, error))?
                    .merge_branch_preview(options.into_preview()),
            )
            .map_err(|error| throw_lix_error(&env, error))?;
        Ok(MergeBranchPreviewDto::from(preview))
    }

    #[napi(js_name = "mergeBranch")]
    pub fn merge_branch(
        &self,
        env: Env,
        options: MergeBranchOptions,
    ) -> Result<MergeBranchReceiptDto> {
        let receipt = self
            .rt
            .block_on(
                self.lix()
                    .map_err(|error| throw_lix_error(&env, error))?
                    .merge_branch(options.into()),
            )
            .map_err(|error| throw_lix_error(&env, error))?;
        Ok(MergeBranchReceiptDto::from(receipt))
    }

    #[napi]
    pub fn close(&mut self, env: Env) -> Result<()> {
        let Some(lix) = self.lix.as_ref() else {
            return Ok(());
        };
        self.rt
            .block_on(lix.close())
            .map_err(|error| throw_lix_error(&env, error))?;
        self.lix = None;
        Ok(())
    }
}

impl NativeLix {
    fn lix(&self) -> std::result::Result<&NativeLixInner, LixError> {
        self.lix.as_ref().ok_or_else(|| {
            LixError::new(LixError::CODE_CLOSED, "Lix handle is closed")
                .with_hint("Open a new Lix handle before calling this method.")
        })
    }
}

#[napi(js_name = "LixTransaction")]
pub struct NativeLixTransaction {
    rt: Runtime,
    transaction: Option<NativeLixTransactionInner>,
}

#[napi]
impl NativeLixTransaction {
    fn new(transaction: NativeLixTransactionInner) -> Result<Self> {
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(to_napi_error)?;
        Ok(Self {
            rt,
            transaction: Some(transaction),
        })
    }

    #[napi]
    pub fn execute(
        &mut self,
        env: Env,
        sql: String,
        params: Option<Vec<LixValue>>,
    ) -> Result<ExecuteResult> {
        let params = match params {
            Some(params) => params
                .into_iter()
                .map(Value::try_from)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| throw_lix_error(&env, error))?,
            None => Vec::new(),
        };
        let mut transaction = self
            .take_transaction()
            .map_err(|error| throw_lix_error(&env, error))?;
        let result = self.rt.block_on(transaction.execute(&sql, &params));
        self.transaction = Some(transaction);
        let result = result.map_err(|error| throw_lix_error(&env, error))?;
        ExecuteResult::try_from(result).map_err(|error| throw_lix_error(&env, error))
    }

    #[napi]
    pub fn commit(&mut self, env: Env) -> Result<()> {
        let transaction = self
            .take_transaction()
            .map_err(|error| throw_lix_error(&env, error))?;
        self.rt
            .block_on(transaction.commit())
            .map_err(|error| throw_lix_error(&env, error))
    }

    #[napi]
    pub fn rollback(&mut self, env: Env) -> Result<()> {
        let transaction = self
            .take_transaction()
            .map_err(|error| throw_lix_error(&env, error))?;
        self.rt
            .block_on(transaction.rollback())
            .map_err(|error| throw_lix_error(&env, error))
    }
}

impl NativeLixTransaction {
    fn take_transaction(&mut self) -> std::result::Result<NativeLixTransactionInner, LixError> {
        self.transaction.take().ok_or_else(|| {
            LixError::new("LIX_INVALID_TRANSACTION_STATE", "Lix transaction is closed")
        })
    }
}

#[napi(object)]
pub struct CreateBranchOptions {
    pub id: Option<String>,
    pub name: String,
    pub from_commit_id: Option<String>,
}

impl From<CreateBranchOptions> for RsCreateBranchOptions {
    fn from(options: CreateBranchOptions) -> Self {
        Self {
            id: options.id,
            name: options.name,
            from_commit_id: options.from_commit_id,
        }
    }
}

#[napi(object)]
pub struct CreateBranchReceiptDto {
    pub id: String,
    pub name: String,
    pub hidden: bool,
    pub commit_id: String,
}

impl From<CreateBranchReceipt> for CreateBranchReceiptDto {
    fn from(receipt: CreateBranchReceipt) -> Self {
        Self {
            id: receipt.id,
            name: receipt.name,
            hidden: receipt.hidden,
            commit_id: receipt.commit_id,
        }
    }
}

#[napi(object)]
pub struct SwitchBranchOptions {
    pub branch_id: String,
}

impl From<SwitchBranchOptions> for RsSwitchBranchOptions {
    fn from(options: SwitchBranchOptions) -> Self {
        Self {
            branch_id: options.branch_id,
        }
    }
}

#[napi(object)]
pub struct SwitchBranchReceiptDto {
    pub branch_id: String,
}

impl From<SwitchBranchReceipt> for SwitchBranchReceiptDto {
    fn from(receipt: SwitchBranchReceipt) -> Self {
        Self {
            branch_id: receipt.branch_id,
        }
    }
}

#[napi(object)]
pub struct MergeBranchOptions {
    pub source_branch_id: String,
}

impl MergeBranchOptions {
    fn into_preview(self) -> MergeBranchPreviewOptions {
        MergeBranchPreviewOptions {
            source_branch_id: self.source_branch_id,
        }
    }
}

impl From<MergeBranchOptions> for RsMergeBranchOptions {
    fn from(options: MergeBranchOptions) -> Self {
        Self {
            source_branch_id: options.source_branch_id,
        }
    }
}

#[napi(object)]
pub struct MergeBranchReceiptDto {
    pub outcome: String,
    pub target_branch_id: String,
    pub source_branch_id: String,
    pub base_commit_id: String,
    pub target_head_before_commit_id: String,
    pub source_head_before_commit_id: String,
    pub target_head_after_commit_id: String,
    pub created_merge_commit_id: Option<String>,
    pub change_stats: MergeChangeStatsDto,
}

impl From<MergeBranchReceipt> for MergeBranchReceiptDto {
    fn from(receipt: MergeBranchReceipt) -> Self {
        Self {
            outcome: merge_branch_outcome_to_string(receipt.outcome),
            target_branch_id: receipt.target_branch_id,
            source_branch_id: receipt.source_branch_id,
            base_commit_id: receipt.base_commit_id,
            target_head_before_commit_id: receipt.target_head_before_commit_id,
            source_head_before_commit_id: receipt.source_head_before_commit_id,
            target_head_after_commit_id: receipt.target_head_after_commit_id,
            created_merge_commit_id: receipt.created_merge_commit_id,
            change_stats: receipt.change_stats.into(),
        }
    }
}

#[napi(object)]
pub struct MergeBranchPreviewDto {
    pub outcome: String,
    pub target_branch_id: String,
    pub source_branch_id: String,
    pub base_commit_id: String,
    pub target_head_commit_id: String,
    pub source_head_commit_id: String,
    pub change_stats: MergeChangeStatsDto,
    pub conflicts: Vec<MergeConflictDto>,
}

impl From<MergeBranchPreview> for MergeBranchPreviewDto {
    fn from(preview: MergeBranchPreview) -> Self {
        Self {
            outcome: merge_branch_outcome_to_string(preview.outcome),
            target_branch_id: preview.target_branch_id,
            source_branch_id: preview.source_branch_id,
            base_commit_id: preview.base_commit_id,
            target_head_commit_id: preview.target_head_commit_id,
            source_head_commit_id: preview.source_head_commit_id,
            change_stats: preview.change_stats.into(),
            conflicts: preview.conflicts.into_iter().map(Into::into).collect(),
        }
    }
}

fn merge_branch_outcome_to_string(outcome: MergeBranchOutcome) -> String {
    match outcome {
        MergeBranchOutcome::AlreadyUpToDate => "alreadyUpToDate",
        MergeBranchOutcome::FastForward => "fastForward",
        MergeBranchOutcome::MergeCommitted => "mergeCommitted",
    }
    .to_string()
}

#[napi(object)]
pub struct MergeChangeStatsDto {
    pub total: u32,
    pub added: u32,
    pub modified: u32,
    pub removed: u32,
}

impl From<MergeChangeStats> for MergeChangeStatsDto {
    fn from(stats: MergeChangeStats) -> Self {
        Self {
            total: stats.total as u32,
            added: stats.added as u32,
            modified: stats.modified as u32,
            removed: stats.removed as u32,
        }
    }
}

#[napi(object)]
pub struct MergeConflictDto {
    pub kind: String,
    pub schema_key: String,
    pub entity_pk: serde_json::Value,
    pub file_id: Option<String>,
    pub target: MergeConflictSideDto,
    pub source: MergeConflictSideDto,
}

impl From<MergeConflict> for MergeConflictDto {
    fn from(conflict: MergeConflict) -> Self {
        Self {
            kind: merge_conflict_kind_to_string(conflict.kind),
            schema_key: conflict.schema_key,
            entity_pk: conflict.entity_pk,
            file_id: conflict.file_id,
            target: conflict.target.into(),
            source: conflict.source.into(),
        }
    }
}

fn merge_conflict_kind_to_string(kind: MergeConflictKind) -> String {
    match kind {
        MergeConflictKind::SameEntityChanged => "sameEntityChanged",
    }
    .to_string()
}

#[napi(object)]
pub struct MergeConflictSideDto {
    pub kind: String,
    pub before_change_id: Option<String>,
    pub after_change_id: Option<String>,
}

impl From<MergeConflictSide> for MergeConflictSideDto {
    fn from(side: MergeConflictSide) -> Self {
        Self {
            kind: merge_conflict_change_kind_to_string(side.kind),
            before_change_id: side.before_change_id,
            after_change_id: side.after_change_id,
        }
    }
}

fn merge_conflict_change_kind_to_string(kind: MergeConflictChangeKind) -> String {
    match kind {
        MergeConflictChangeKind::Added => "added",
        MergeConflictChangeKind::Modified => "modified",
        MergeConflictChangeKind::Removed => "removed",
    }
    .to_string()
}

#[napi(object)]
pub struct LixValue {
    pub kind: String,
    pub value: Option<serde_json::Value>,
    pub blob: Option<Buffer>,
}

impl TryFrom<LixValue> for Value {
    type Error = LixError;

    fn try_from(value: LixValue) -> std::result::Result<Self, Self::Error> {
        match value.kind.as_str() {
            "null" => Ok(Value::Null),
            "boolean" => Ok(Value::Boolean(
                value.value.and_then(|v| v.as_bool()).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "boolean value must be a boolean",
                    )
                })?,
            )),
            "integer" => Ok(Value::Integer(
                value.value.and_then(|v| v.as_i64()).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "integer value must be an integer",
                    )
                })?,
            )),
            "real" => {
                let value = value.value.and_then(|v| v.as_f64()).ok_or_else(|| {
                    LixError::new(LixError::CODE_INVALID_PARAM, "real value must be a number")
                })?;
                if !value.is_finite() {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "real value must be a finite number",
                    ));
                }
                Ok(Value::Real(value))
            }
            "text" => Ok(Value::Text(
                value
                    .value
                    .and_then(|v| v.as_str().map(ToOwned::to_owned))
                    .ok_or_else(|| {
                        LixError::new(LixError::CODE_INVALID_PARAM, "text value must be a string")
                    })?,
            )),
            "json" => Ok(Value::Json(value.value.unwrap_or(serde_json::Value::Null))),
            "blob" => {
                let bytes = value.blob.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "blob value must include bytes",
                    )
                })?;
                Ok(Value::Blob(bytes.to_vec()))
            }
            other => Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("unsupported LixValue kind: {other}"),
            )),
        }
    }
}

impl TryFrom<&Value> for LixValue {
    type Error = LixError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Null => Ok(Self {
                kind: "null".to_string(),
                value: Some(serde_json::Value::Null),
                blob: None,
            }),
            Value::Boolean(value) => Ok(Self {
                kind: "boolean".to_string(),
                value: Some(serde_json::json!(value)),
                blob: None,
            }),
            Value::Integer(value) => Ok(Self {
                kind: "integer".to_string(),
                value: Some(serde_json::json!(value)),
                blob: None,
            }),
            Value::Real(value) => {
                if !value.is_finite() {
                    return Err(LixError::new(
                        "LIX_ERROR_JS_SDK_NATIVE",
                        "cannot encode non-finite real value",
                    ));
                }
                Ok(Self {
                    kind: "real".to_string(),
                    value: Some(serde_json::json!(value)),
                    blob: None,
                })
            }
            Value::Text(value) => Ok(Self {
                kind: "text".to_string(),
                value: Some(serde_json::json!(value)),
                blob: None,
            }),
            Value::Json(value) => Ok(Self {
                kind: "json".to_string(),
                value: Some(value.clone()),
                blob: None,
            }),
            Value::Blob(value) => Ok(Self {
                kind: "blob".to_string(),
                value: None,
                blob: Some(Buffer::from(value.clone())),
            }),
        }
    }
}

#[napi(object)]
pub struct ExecuteResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<LixValue>>,
    pub rows_affected: u32,
    pub notices: Vec<LixNotice>,
}

impl TryFrom<RsExecuteResult> for ExecuteResult {
    type Error = LixError;

    fn try_from(result: RsExecuteResult) -> std::result::Result<Self, Self::Error> {
        let mut rows = Vec::with_capacity(result.rows().len());
        for row in result.rows() {
            let mut values = Vec::with_capacity(row.values().len());
            for value in row.values() {
                values.push(LixValue::try_from(value)?);
            }
            rows.push(values);
        }
        Ok(Self {
            columns: result.columns().to_vec(),
            rows,
            rows_affected: result.rows_affected() as u32,
            notices: result
                .notices()
                .iter()
                .map(|notice| LixNotice {
                    code: notice.code.clone(),
                    message: notice.message.clone(),
                    hint: notice.hint.clone(),
                })
                .collect(),
        })
    }
}

#[napi(object)]
pub struct LixNotice {
    pub code: String,
    pub message: String,
    pub hint: Option<String>,
}

fn to_napi_error(error: impl std::fmt::Display) -> Error {
    Error::from_reason(error.to_string())
}

fn throw_lix_error(env: &Env, error: LixError) -> Error {
    let thrown = (|| -> Result<()> {
        let mut js_error = env.create_error(Error::new(Status::GenericFailure, &error.message))?;
        js_error.set_named_property("name", "LixError")?;
        js_error.set_named_property("code", error.code.clone())?;
        if let Some(hint) = &error.hint {
            js_error.set_named_property("hint", hint.clone())?;
        }
        if let Some(details) = &error.details {
            js_error.set_named_property("details", details.clone())?;
        }
        env.throw(js_error)?;
        Ok(())
    })();

    match thrown {
        Ok(()) => Error::new(Status::PendingException, ""),
        Err(error) => error,
    }
}
