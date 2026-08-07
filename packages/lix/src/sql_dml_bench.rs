//! Test-only SQL DML bridge for replacement-layout qualification.
//!
//! This module deliberately reuses Lix's parser, binder, logical write plan,
//! providers, and write executor. It exposes only the transaction-scoped
//! physical row capability needed by storage benchmarks. The supplied rows
//! are an ephemeral transaction snapshot; callers remain responsible for one
//! atomic publication into their authoritative test layout.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::binary_cas::{BlobBytesBatch, BlobId};
use crate::changelog::CommitId;
use crate::common::LixTimestamp;
use crate::functions::FunctionProviderHandle;
use crate::live_state::{
    LiveStateExactBatchRequest, LiveStateScanRequest, MaterializedLiveStateBatch,
    MaterializedLiveStateBatchBuilder, MaterializedLiveStateExactBatch, MaterializedLiveStateRow,
};
use crate::sql2::{PublicCatalog, SqlWriteExecutionContext};
use crate::transaction::types::{
    TransactionWrite, TransactionWriteOutcome, TransactionWriteRow, TypedMutationJournalBatch,
};
use crate::{ExecuteStatementMetadata, LixError, SqlQueryResult, Value};

const BENCH_CREATED_AT: &str = "2026-08-07T00:00:00Z";
const BENCH_UPDATED_AT: &str = "2026-08-07T00:00:01Z";

/// One semantic row read from or staged into the replacement-layout model.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SqlDmlBenchRow {
    pub entity_pk: String,
    pub schema_key: String,
    pub branch_id: String,
    pub file_id: Option<String>,
    pub snapshot: Option<String>,
    pub metadata: Option<String>,
    pub global: bool,
    pub untracked: bool,
    pub deleted: bool,
}

/// One statement routed through Lix's sole SQL write executor.
#[derive(Clone, Debug)]
pub struct SqlDmlBenchStatement {
    pub label: String,
    pub sql: String,
    pub params: Vec<Value>,
}

/// Public result identity retained by the qualification harness.
#[derive(Debug)]
pub struct SqlDmlBenchStatementResult {
    pub index: usize,
    pub label: String,
    pub rows_affected: u64,
    pub returning: Option<SqlQueryResult>,
}

/// A successful transaction-scoped lowering. `staged_rows` are the exact
/// physical postimages/tombstones emitted by Lix; `final_rows` is the ephemeral
/// read-your-writes snapshot used only to validate subsequent statements.
#[derive(Debug)]
pub struct SqlDmlBenchBatchResult {
    pub results: Vec<SqlDmlBenchStatementResult>,
    pub staged_rows: Vec<SqlDmlBenchRow>,
    pub final_rows: Vec<SqlDmlBenchRow>,
    pub live_scans: u64,
    pub exact_reads: u64,
}

/// Public, SQL-free row filter forwarded to a test-only physical target.
#[derive(Clone, Debug)]
pub enum SqlDmlBenchFileFilter {
    Any,
    Null,
    Value(String),
}

/// Lix-owned live-state request translated without exposing private storage
/// types to a replacement-layout benchmark.
#[derive(Clone, Debug)]
pub struct SqlDmlBenchScanRequest {
    pub rows_none: bool,
    pub schema_keys: Vec<String>,
    pub entity_pks: Vec<String>,
    pub branch_ids: Vec<String>,
    pub file_ids: Vec<SqlDmlBenchFileFilter>,
    pub untracked: Option<bool>,
    pub include_tombstones: bool,
    pub limit: Option<usize>,
}

/// One correlated identity in a Lix-owned exact read.
#[derive(Clone, Debug)]
pub struct SqlDmlBenchExactRowRequest {
    pub schema_key: String,
    pub entity_pk: String,
    pub branch_id: String,
    pub file_id: Option<String>,
}

/// Test-only physical read capability. Implementations return authenticated
/// semantic rows; they do not bind SQL, choose write behavior, or publish.
#[async_trait]
pub trait SqlDmlBenchReadTarget: Send {
    async fn scan_rows(
        &mut self,
        request: &SqlDmlBenchScanRequest,
    ) -> Result<Vec<SqlDmlBenchRow>, LixError>;

    async fn load_exact_rows(
        &mut self,
        rows: &[SqlDmlBenchExactRowRequest],
        untracked: Option<bool>,
        include_tombstones: bool,
    ) -> Result<Vec<Option<SqlDmlBenchRow>>, LixError>;
}

struct BenchWriteContext {
    active_branch_id: String,
    schemas: Vec<JsonValue>,
    rows: Vec<MaterializedLiveStateRow>,
    staged_rows: Vec<SqlDmlBenchRow>,
    live_scans: u64,
    exact_reads: u64,
}

struct DirectBenchWriteContext<'a, T> {
    active_branch_id: String,
    schemas: Vec<JsonValue>,
    target: &'a mut T,
    staged: BTreeMap<BenchRowIdentity, SqlDmlBenchRow>,
    staged_rows: Vec<SqlDmlBenchRow>,
    live_scans: u64,
    exact_reads: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BenchRowIdentity {
    schema_key: String,
    entity_pk: String,
    branch_id: String,
    file_id: Option<String>,
    global: bool,
    untracked: bool,
}

/// Execute a statement batch through the exact Lix binder/write executor.
///
/// A failing statement restores the complete transaction input before the
/// error is returned. No replacement-layout selector is published by this
/// function, so the model can preserve one atomic root transition.
pub async fn execute_sql_dml_batch_for_bench(
    active_branch_id: &str,
    schema_definitions: &[String],
    rows: Vec<SqlDmlBenchRow>,
    statements: &[SqlDmlBenchStatement],
) -> Result<SqlDmlBenchBatchResult, LixError> {
    let schemas = schema_definitions
        .iter()
        .map(|schema| {
            serde_json::from_str(schema).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("invalid benchmark schema definition: {error}"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let initial_rows = rows
        .into_iter()
        .map(materialized_row)
        .collect::<Result<Vec<_>, _>>()?;
    let mut ctx = BenchWriteContext {
        active_branch_id: active_branch_id.to_string(),
        schemas,
        rows: initial_rows.clone(),
        staged_rows: Vec::new(),
        live_scans: 0,
        exact_reads: 0,
    };
    let mut results = Vec::with_capacity(statements.len());
    for (index, statement) in statements.iter().enumerate() {
        let parsed = crate::sql2::parse_statement(&statement.sql)?;
        let catalog = ctx.public_catalog()?;
        let logical = crate::sql2::create_write_plan_template_from_parsed(
            &parsed,
            &catalog,
            ctx.active_branch_id(),
        )?;
        let plan = crate::sql2::create_write_logical_plan_from_template(logical);
        let execution = crate::sql2::execute_write_logical_plan_result_with_metadata(
            &mut ctx,
            plan,
            &statement.params,
            &ExecuteStatementMetadata::default(),
        )
        .await;
        let result = match execution {
            Ok(result) => result,
            Err(error) => {
                ctx.rows = initial_rows;
                ctx.staged_rows.clear();
                return Err(error);
            }
        };
        results.push(SqlDmlBenchStatementResult {
            index,
            label: statement.label.clone(),
            rows_affected: result.rows_affected,
            returning: result.returning,
        });
    }
    Ok(SqlDmlBenchBatchResult {
        results,
        staged_rows: ctx.staged_rows,
        final_rows: ctx
            .rows
            .into_iter()
            .map(bench_row_from_materialized)
            .collect::<Result<Vec<_>, _>>()?,
        live_scans: ctx.live_scans,
        exact_reads: ctx.exact_reads,
    })
}

/// Execute through Lix's sole binder/write executor while reading directly
/// from an authenticated test target. Only the transaction's coalesced
/// postimages are returned in `final_rows`; no full serving snapshot is
/// materialized or mirrored by this bridge.
pub async fn execute_sql_dml_batch_with_read_target_for_bench<T>(
    active_branch_id: &str,
    schema_definitions: &[String],
    target: &mut T,
    statements: &[SqlDmlBenchStatement],
) -> Result<SqlDmlBenchBatchResult, LixError>
where
    T: SqlDmlBenchReadTarget,
{
    let schemas = parse_schemas(schema_definitions)?;
    let mut ctx = DirectBenchWriteContext {
        active_branch_id: active_branch_id.to_string(),
        schemas,
        target,
        staged: BTreeMap::new(),
        staged_rows: Vec::new(),
        live_scans: 0,
        exact_reads: 0,
    };
    let mut results = Vec::with_capacity(statements.len());
    for (index, statement) in statements.iter().enumerate() {
        let parsed = crate::sql2::parse_statement(&statement.sql)?;
        let catalog = ctx.public_catalog()?;
        let logical = crate::sql2::create_write_plan_template_from_parsed(
            &parsed,
            &catalog,
            ctx.active_branch_id(),
        )?;
        let plan = crate::sql2::create_write_logical_plan_from_template(logical);
        let execution = crate::sql2::execute_write_logical_plan_result_with_metadata(
            &mut ctx,
            plan,
            &statement.params,
            &ExecuteStatementMetadata::default(),
        )
        .await;
        let result = match execution {
            Ok(result) => result,
            Err(error) => {
                ctx.staged.clear();
                ctx.staged_rows.clear();
                return Err(error);
            }
        };
        results.push(SqlDmlBenchStatementResult {
            index,
            label: statement.label.clone(),
            rows_affected: result.rows_affected,
            returning: result.returning,
        });
    }
    Ok(SqlDmlBenchBatchResult {
        results,
        staged_rows: ctx.staged_rows,
        final_rows: ctx.staged.into_values().collect(),
        live_scans: ctx.live_scans,
        exact_reads: ctx.exact_reads,
    })
}

fn parse_schemas(schema_definitions: &[String]) -> Result<Vec<JsonValue>, LixError> {
    schema_definitions
        .iter()
        .map(|schema| {
            serde_json::from_str(schema).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("invalid benchmark schema definition: {error}"),
                )
            })
        })
        .collect()
}

#[async_trait]
impl SqlWriteExecutionContext for BenchWriteContext {
    fn active_branch_id(&self) -> &str {
        &self.active_branch_id
    }

    fn functions(&self) -> FunctionProviderHandle {
        FunctionProviderHandle::system()
    }

    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        Ok(self.schemas.clone())
    }

    fn public_catalog(&self) -> Result<Arc<PublicCatalog>, LixError> {
        Ok(Arc::new(PublicCatalog::from_visible_schemas(
            &self.schemas,
        )?))
    }

    async fn load_bytes_many(&mut self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
        Ok(BlobBytesBatch::new(vec![None; hashes.len()]))
    }

    async fn scan_live_state_batch(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        self.live_scans = self.live_scans.saturating_add(1);
        Ok(filter_rows(&self.rows, request).into())
    }

    async fn load_exact_live_state_batch(
        &mut self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        self.exact_reads = self.exact_reads.saturating_add(1);
        let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(request.rows.len());
        let mut slots = Vec::with_capacity(request.rows.len());
        for identity in &request.rows {
            let row = self.rows.iter().find(|row| {
                row.schema_key == identity.schema_key
                    && row.branch_id.as_ref() == identity.branch_id
                    && row.entity_pk == identity.entity_pk
                    && row.file_id == identity.file_id
                    && request.untracked.is_none_or(|value| row.untracked == value)
                    && (request.include_tombstones || !row.deleted)
            });
            slots.push(row.map(|row| {
                let ordinal =
                    u32::try_from(builder.len()).expect("benchmark exact result should fit u32");
                builder.push_owned(row.clone());
                ordinal
            }));
        }
        MaterializedLiveStateExactBatch::new(builder.finish(), slots)
    }

    async fn load_branch_head(&mut self, branch_id: &str) -> Result<Option<CommitId>, LixError> {
        Ok(Some(CommitId::for_test_label(&format!(
            "forktree-bench-{branch_id}"
        ))))
    }

    async fn stage_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let (count, rows) = match write {
            TransactionWrite::Rows { rows, .. } => {
                let rows = rows.into_rows();
                (rows.len() as u64, rows)
            }
            TransactionWrite::RowsWithFileContent { rows, count, .. } => (count, rows.into_rows()),
        };
        for row in rows {
            let staged = bench_row_from_transaction(row)?;
            let materialized = materialized_row(staged.clone())?;
            if let Some(existing) = self
                .rows
                .iter_mut()
                .find(|existing| same_identity(existing, &materialized))
            {
                *existing = materialized;
            } else {
                self.rows.push(materialized);
            }
            self.staged_rows.push(staged);
        }
        Ok(TransactionWriteOutcome { count })
    }

    async fn stage_typed_mutation_journal_replace(
        &mut self,
        _rows: TypedMutationJournalBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "ForkTree SQL benchmark uses ordinary transaction row postimages",
        ))
    }

    async fn can_stage_typed_mutation_journal_replace(
        &mut self,
        _schema_key: &str,
        _live_count: u64,
        _ordered_identity_digest: [u8; 32],
    ) -> Result<bool, LixError> {
        Ok(false)
    }
}

#[async_trait]
impl<T> SqlWriteExecutionContext for DirectBenchWriteContext<'_, T>
where
    T: SqlDmlBenchReadTarget,
{
    fn active_branch_id(&self) -> &str {
        &self.active_branch_id
    }

    fn functions(&self) -> FunctionProviderHandle {
        FunctionProviderHandle::system()
    }

    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        Ok(self.schemas.clone())
    }

    fn public_catalog(&self) -> Result<Arc<PublicCatalog>, LixError> {
        Ok(Arc::new(PublicCatalog::from_visible_schemas(
            &self.schemas,
        )?))
    }

    async fn load_bytes_many(&mut self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
        Ok(BlobBytesBatch::new(vec![None; hashes.len()]))
    }

    async fn scan_live_state_batch(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        self.live_scans = self.live_scans.saturating_add(1);
        let mut target_request = public_scan_request(request)?;
        // Apply the limit only after transaction-local postimages replace the
        // target rows, preserving ordinary read-your-writes behavior.
        target_request.limit = None;
        let rows = self.target.scan_rows(&target_request).await?;
        let mut visible = rows
            .into_iter()
            .map(materialized_row)
            .collect::<Result<Vec<_>, _>>()?;
        for staged in self.staged.values() {
            let staged = materialized_row(staged.clone())?;
            if let Some(existing) = visible
                .iter_mut()
                .find(|existing| same_identity(existing, &staged))
            {
                *existing = staged;
            } else {
                visible.push(staged);
            }
        }
        Ok(filter_rows(&visible, request).into())
    }

    async fn load_exact_live_state_batch(
        &mut self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        self.exact_reads = self.exact_reads.saturating_add(1);
        let public = request
            .rows
            .iter()
            .map(|row| {
                Ok(SqlDmlBenchExactRowRequest {
                    schema_key: row.schema_key.clone(),
                    entity_pk: row.entity_pk.as_json_array_text()?,
                    branch_id: row.branch_id.clone(),
                    file_id: row.file_id.clone(),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let target_rows = self
            .target
            .load_exact_rows(&public, request.untracked, request.include_tombstones)
            .await?;
        if target_rows.len() != public.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "benchmark physical target returned misaligned exact-read slots",
            ));
        }
        let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(public.len());
        let mut slots = Vec::with_capacity(public.len());
        for (identity, target_row) in public.iter().zip(target_rows) {
            let staged = self.staged.values().find(|row| {
                row.schema_key == identity.schema_key
                    && row.entity_pk == identity.entity_pk
                    && row.branch_id == identity.branch_id
                    && row.file_id == identity.file_id
                    && request.untracked.is_none_or(|value| row.untracked == value)
            });
            let row = staged
                .cloned()
                .or(target_row)
                .filter(|row| request.include_tombstones || !row.deleted);
            slots.push(match row {
                Some(row) => {
                    let ordinal = u32::try_from(builder.len())
                        .expect("benchmark exact result should fit u32");
                    builder.push_owned(materialized_row(row)?);
                    Some(ordinal)
                }
                None => None,
            });
        }
        MaterializedLiveStateExactBatch::new(builder.finish(), slots)
    }

    async fn load_branch_head(&mut self, branch_id: &str) -> Result<Option<CommitId>, LixError> {
        Ok(Some(CommitId::for_test_label(&format!(
            "forktree-bench-{branch_id}"
        ))))
    }

    async fn stage_write(
        &mut self,
        write: TransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let (count, rows) = match write {
            TransactionWrite::Rows { rows, .. } => {
                let rows = rows.into_rows();
                (rows.len() as u64, rows)
            }
            TransactionWrite::RowsWithFileContent { rows, count, .. } => (count, rows.into_rows()),
        };
        for row in rows {
            let staged = bench_row_from_transaction(row)?;
            self.staged.insert(bench_identity(&staged), staged.clone());
            self.staged_rows.push(staged);
        }
        Ok(TransactionWriteOutcome { count })
    }

    async fn stage_typed_mutation_journal_replace(
        &mut self,
        _rows: TypedMutationJournalBatch,
    ) -> Result<TransactionWriteOutcome, LixError> {
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "ForkTree SQL benchmark uses ordinary transaction row postimages",
        ))
    }

    async fn can_stage_typed_mutation_journal_replace(
        &mut self,
        _schema_key: &str,
        _live_count: u64,
        _ordered_identity_digest: [u8; 32],
    ) -> Result<bool, LixError> {
        Ok(false)
    }
}

fn public_scan_request(request: &LiveStateScanRequest) -> Result<SqlDmlBenchScanRequest, LixError> {
    Ok(SqlDmlBenchScanRequest {
        rows_none: matches!(
            request.filter.rows,
            crate::live_state::LiveStateRowFilter::None
        ),
        schema_keys: request.filter.schema_keys.clone(),
        entity_pks: request
            .filter
            .entity_pks
            .iter()
            .map(crate::entity_pk::EntityPk::as_json_array_text)
            .collect::<Result<Vec<_>, _>>()?,
        branch_ids: request.filter.branch_ids.clone(),
        file_ids: request
            .filter
            .file_ids
            .iter()
            .map(|file_id| match file_id {
                crate::NullableKeyFilter::Any => SqlDmlBenchFileFilter::Any,
                crate::NullableKeyFilter::Null => SqlDmlBenchFileFilter::Null,
                crate::NullableKeyFilter::Value(value) => {
                    SqlDmlBenchFileFilter::Value(value.clone())
                }
            })
            .collect(),
        untracked: request.filter.untracked,
        include_tombstones: request.filter.include_tombstones,
        limit: request.limit,
    })
}

fn bench_identity(row: &SqlDmlBenchRow) -> BenchRowIdentity {
    BenchRowIdentity {
        schema_key: row.schema_key.clone(),
        entity_pk: row.entity_pk.clone(),
        branch_id: row.branch_id.clone(),
        file_id: row.file_id.clone(),
        global: row.global,
        untracked: row.untracked,
    }
}

fn filter_rows(
    rows: &[MaterializedLiveStateRow],
    request: &LiveStateScanRequest,
) -> Vec<MaterializedLiveStateRow> {
    if matches!(
        request.filter.rows,
        crate::live_state::LiveStateRowFilter::None
    ) {
        return Vec::new();
    }
    let mut selected = rows
        .iter()
        .filter(|row| {
            (request.filter.schema_keys.is_empty()
                || request.filter.schema_keys.contains(&row.schema_key))
                && (request.filter.entity_pks.is_empty()
                    || request.filter.entity_pks.contains(&row.entity_pk))
                && (request.filter.branch_ids.is_empty()
                    || request
                        .filter
                        .branch_ids
                        .iter()
                        .any(|branch| branch == row.branch_id.as_ref()))
                && request
                    .filter
                    .untracked
                    .is_none_or(|untracked| row.untracked == untracked)
                && (request.filter.include_tombstones || !row.deleted)
                && (request.filter.file_ids.is_empty()
                    || request.filter.file_ids.iter().any(|filter| match filter {
                        crate::NullableKeyFilter::Any => true,
                        crate::NullableKeyFilter::Null => row.file_id.is_none(),
                        crate::NullableKeyFilter::Value(file_id) => {
                            row.file_id.as_ref() == Some(file_id)
                        }
                    }))
        })
        .cloned()
        .collect::<Vec<_>>();
    if let Some(limit) = request.limit {
        selected.truncate(limit);
    }
    selected
}

fn same_identity(left: &MaterializedLiveStateRow, right: &MaterializedLiveStateRow) -> bool {
    left.schema_key == right.schema_key
        && left.entity_pk == right.entity_pk
        && left.branch_id == right.branch_id
        && left.file_id == right.file_id
        && left.global == right.global
        && left.untracked == right.untracked
}

fn materialized_row(row: SqlDmlBenchRow) -> Result<MaterializedLiveStateRow, LixError> {
    let entity_pk = crate::entity_pk::EntityPk::from_json_array_text(&row.entity_pk)
        .map_err(|error| LixError::new(LixError::CODE_INVALID_PARAM, error.to_string()))?;
    Ok(MaterializedLiveStateRow {
        entity_pk,
        schema_key: row.schema_key,
        file_id: row.file_id,
        snapshot_content: row.snapshot.map(Into::into),
        metadata: row.metadata.map(Into::into),
        deleted: row.deleted,
        created_at: LixTimestamp::expect_parse("benchmark created_at", BENCH_CREATED_AT),
        updated_at: LixTimestamp::expect_parse("benchmark updated_at", BENCH_UPDATED_AT),
        global: row.global,
        change_id: None,
        commit_id: None,
        untracked: row.untracked,
        branch_id: row.branch_id.into(),
    })
}

fn bench_row_from_transaction(row: TransactionWriteRow) -> Result<SqlDmlBenchRow, LixError> {
    let deleted = row.snapshot.is_none();
    Ok(SqlDmlBenchRow {
        entity_pk: row
            .entity_pk
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "bound benchmark write did not resolve its entity identity",
                )
            })?
            .as_json_array_text()?,
        schema_key: row.schema_key.into(),
        branch_id: row.branch_id.into(),
        file_id: row.file_id.map(Into::into),
        snapshot: row.snapshot.map(|snapshot| snapshot.to_string()),
        metadata: row.metadata.map(|metadata| metadata.to_string()),
        global: row.global,
        untracked: row.untracked,
        deleted,
    })
}

fn bench_row_from_materialized(row: MaterializedLiveStateRow) -> Result<SqlDmlBenchRow, LixError> {
    Ok(SqlDmlBenchRow {
        entity_pk: row.entity_pk.as_json_array_text()?,
        schema_key: row.schema_key,
        branch_id: row.branch_id.to_string(),
        file_id: row.file_id,
        snapshot: row.snapshot_content.map(Into::into),
        metadata: row.metadata.map(Into::into),
        global: row.global,
        untracked: row.untracked,
        deleted: row.deleted,
    })
}
