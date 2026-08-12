use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use futures_util::FutureExt;
use serde_json::Value as JsonValue;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::{
    BranchHead, BranchRefReader, branch_descriptor_stage_row, branch_descriptor_tombstone_row,
    branch_ref_stage_row, branch_ref_tombstone_row,
};
use crate::changelog::CommitId;
use crate::entity_pk::EntityPk;
use crate::hot_state::{
    HotStateExactBatchRequest, HotStateExactRowRequest, HotStateFilter, HotStateProjection,
    HotStateReader, HotStateScanRequest, MaterializedHotStateRowRef,
};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::write_normalization::{
    InsertCell, SqlCell, UpdateAssignmentValues, defaultable_bool_insert_value,
    defaultable_text_insert_value, insert_column_is_omitted,
};
use crate::sql2::{SqlWriteContext, WriteAccess, WriteContextHotStateReader};
use crate::transaction_types::{
    LogicalPrimaryKey, RawWriteBatch, TransactionWrite, TransactionWriteMode,
    TransactionWriteOperation, TransactionWriteOrigin, TransactionWriteRow,
};

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::spec::{
    DmlReturning, InsertApply, PlannedDml, PlannedScan, TableSpec, projected_schema,
    register_spec_table, row_source, scan_row_source, take_record_batch_rows,
};
use super::upsert::{StagedUpsert, UpsertReturningRow, UpsertSupport, materialize_omitted_column};
use super::values::{required_bool_value, required_string_value};

pub(super) async fn register_lix_branch_read_provider(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    hot_state: Arc<dyn HotStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(BranchSpec {
            hot_state,
            branch_ref,
            head_read_strategy: BranchHeadReadStrategy::Batch,
        }),
        WriteAccess::read_only(),
    )
}

pub(super) async fn register_write_provider(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    write_ctx: SqlWriteContext,
    branch_ref: Arc<dyn BranchRefReader>,
) -> Result<(), LixError> {
    let hot_state = Arc::new(WriteContextHotStateReader::new(write_ctx.clone()));
    register_spec_table(
        session,
        surface_name,
        Arc::new(BranchSpec {
            hot_state,
            branch_ref,
            head_read_strategy: BranchHeadReadStrategy::Point,
        }),
        WriteAccess::write(write_ctx),
    )
}

struct BranchSpec {
    hot_state: Arc<dyn HotStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    head_read_strategy: BranchHeadReadStrategy,
}

#[derive(Clone, Copy)]
enum BranchHeadReadStrategy {
    Batch,
    Point,
}

#[async_trait]
impl TableSpec for BranchSpec {
    #[expect(clippy::unnecessary_literal_bound)]
    fn table_name(&self) -> &str {
        "lix_branch"
    }

    fn schema(&self) -> SchemaRef {
        lix_branch_schema()
    }

    fn upsert_support(&self) -> Option<&dyn UpsertSupport> {
        Some(self)
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if exact_canonical_branch_ids_from_filters(std::slice::from_ref(filter)).is_some() {
            TableProviderFilterPushDown::Exact
        } else {
            TableProviderFilterPushDown::Unsupported
        }
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&lix_branch_schema(), projection);
        let descriptor_scope = BranchDescriptorScope::from_read_filters(filters);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
                (
                    Arc::clone(&self.hot_state),
                    Arc::clone(&self.branch_ref),
                    schema,
                    self.head_read_strategy,
                    descriptor_scope,
                ),
                |(hot_state, branch_ref, schema, head_read_strategy, descriptor_scope)| async move {
                    let rows = load_branch_rows_scoped(
                        hot_state,
                        branch_ref,
                        head_read_strategy,
                        descriptor_scope,
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                    LIX_BRANCH_COLS
                        .build(schema, &rows)
                        .map_err(branch_batch_error)
                },
            ),
        })
    }

    async fn stage_insert(
        &self,
        write_ctx: &SqlWriteContext,
        batches: Vec<RecordBatch>,
    ) -> Result<u64> {
        let default_commit_id = self
            .branch_ref
            .load_head(&write_ctx.active_branch_id())
            .await
            .map_err(lix_error_to_datafusion_error)?
            .map(|head| head.commit_id)
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "INSERT into lix_branch could not resolve active branch head".to_string(),
                )
            })?;
        let row_capacity = batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>()
            .saturating_mul(2);
        let mut rows = RawWriteBatch::with_capacity(row_capacity);
        let mut count = 0u64;
        for batch in batches {
            let branch_rows = branch_insert_rows_from_batch(&batch, &default_commit_id)?;
            count = count
                .checked_add(u64::try_from(branch_rows.len()).map_err(|_| {
                    DataFusionError::Execution("INSERT row count overflow".to_string())
                })?)
                .ok_or_else(|| DataFusionError::Execution("INSERT row count overflow".into()))?;
            for row in branch_rows {
                push_branch_stage_rows(&mut rows, row, TransactionWriteOperation::Insert, false);
            }
        }

        if !rows.is_empty() {
            write_ctx
                .stage_write(TransactionWrite::Rows {
                    mode: TransactionWriteMode::Insert,
                    rows,
                })
                .await
                .map_err(lix_error_to_datafusion_error)?;
        }

        Ok(count)
    }

    async fn plan_insert_with_returning(
        &self,
        write_ctx: SqlWriteContext,
        _input: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        returning: DmlReturning,
    ) -> Result<InsertApply> {
        let branch_ref = Arc::clone(&self.branch_ref);
        Ok(Arc::new(move |batches| {
            let write_ctx = write_ctx.clone();
            let branch_ref = Arc::clone(&branch_ref);
            let returning = returning.clone();
            async move {
                let default_commit_id = branch_ref
                    .load_head(&write_ctx.active_branch_id())
                    .await
                    .map_err(lix_error_to_datafusion_error)?
                    .map(|head| head.commit_id)
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "INSERT into lix_branch could not resolve active branch head"
                                .to_string(),
                        )
                    })?;
                let row_capacity = batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>()
                    .saturating_mul(2);
                let mut stage_rows = RawWriteBatch::with_capacity(row_capacity);
                let mut post_rows = Vec::new();
                let mut count = 0u64;
                for batch in batches {
                    let branch_rows = branch_insert_rows_from_batch(&batch, &default_commit_id)?;
                    count = count
                        .checked_add(u64::try_from(branch_rows.len()).map_err(|_| {
                            DataFusionError::Execution("INSERT row count overflow".to_string())
                        })?)
                        .ok_or_else(|| {
                            DataFusionError::Execution("INSERT row count overflow".to_string())
                        })?;
                    for row in &branch_rows {
                        push_branch_stage_rows(
                            &mut stage_rows,
                            row.clone(),
                            TransactionWriteOperation::Insert,
                            false,
                        );
                    }
                    post_rows.extend(branch_rows);
                }

                if !stage_rows.is_empty() {
                    write_ctx
                        .stage_write(TransactionWrite::Rows {
                            mode: TransactionWriteMode::Insert,
                            rows: stage_rows,
                        })
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                }

                let post_image = LIX_BRANCH_COLS
                    .build(lix_branch_schema(), &post_rows)
                    .map_err(branch_batch_error)?;
                returning.capture(returning.project(&post_image)?);
                Ok(count)
            }
            .boxed()
        }))
    }

    fn validate_update_assignments(&self, assignments: &[(String, Expr)]) -> Result<()> {
        validate_lix_branch_update_assignments(assignments)
    }

    async fn plan_delete(
        &self,
        write_ctx: SqlWriteContext,
        filters: &[Expr],
    ) -> Result<PlannedDml> {
        let active_branch_id = write_ctx.active_branch_id();
        Ok(PlannedDml {
            source: self.write_row_source(filters),
            apply: Arc::new(move |matched_batch| {
                let write_ctx = write_ctx.clone();
                let active_branch_id = active_branch_id.clone();
                async move {
                    let branch_rows = branch_rows_from_batch(&matched_batch)?;
                    let default_branch_id = load_default_branch_id(&write_ctx).await?;
                    reject_protected_branch_deletes(
                        &branch_rows,
                        &active_branch_id,
                        &default_branch_id,
                    )?;
                    let count = u64::try_from(branch_rows.len()).map_err(|_| {
                        DataFusionError::Execution("DELETE row count overflow".to_string())
                    })?;
                    let mut rows =
                        RawWriteBatch::with_capacity(branch_rows.len().saturating_mul(2));
                    for row in branch_rows {
                        push_branch_stage_rows(
                            &mut rows,
                            row,
                            TransactionWriteOperation::Delete,
                            true,
                        );
                    }

                    if !rows.is_empty() {
                        write_ctx
                            .stage_write(TransactionWrite::Rows {
                                mode: TransactionWriteMode::Replace,
                                rows,
                            })
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                    }

                    Ok(count)
                }
                .boxed()
            }),
        })
    }

    async fn plan_update(
        &self,
        write_ctx: SqlWriteContext,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: &[Expr],
    ) -> Result<PlannedDml> {
        let table_schema = lix_branch_schema();
        Ok(PlannedDml {
            source: self.write_row_source(filters),
            apply: Arc::new(move |matched_batch| {
                let write_ctx = write_ctx.clone();
                let assignments = assignments.clone();
                let table_schema = Arc::clone(&table_schema);
                async move {
                    let branch_rows =
                        branch_update_rows_from_batch(&matched_batch, &assignments, &table_schema)?;
                    reject_protected_branch_updates(&branch_rows)?;
                    let count = u64::try_from(branch_rows.len()).map_err(|_| {
                        DataFusionError::Execution("UPDATE row count overflow".to_string())
                    })?;
                    let mut rows =
                        RawWriteBatch::with_capacity(branch_rows.len().saturating_mul(2));
                    for row in branch_rows {
                        push_branch_stage_rows(
                            &mut rows,
                            row,
                            TransactionWriteOperation::Update,
                            false,
                        );
                    }

                    if !rows.is_empty() {
                        write_ctx
                            .stage_write(TransactionWrite::Rows {
                                mode: TransactionWriteMode::Replace,
                                rows,
                            })
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                    }

                    Ok(count)
                }
                .boxed()
            }),
        })
    }

    async fn plan_update_with_returning(
        &self,
        write_ctx: SqlWriteContext,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: &[Expr],
        returning: DmlReturning,
    ) -> Result<PlannedDml> {
        let table_schema = lix_branch_schema();
        Ok(PlannedDml {
            source: self.write_row_source(filters),
            apply: Arc::new(move |matched_batch| {
                let write_ctx = write_ctx.clone();
                let assignments = assignments.clone();
                let table_schema = Arc::clone(&table_schema);
                let returning = returning.clone();
                async move {
                    let branch_rows =
                        branch_update_rows_from_batch(&matched_batch, &assignments, &table_schema)?;
                    reject_protected_branch_updates(&branch_rows)?;
                    let count = u64::try_from(branch_rows.len()).map_err(|_| {
                        DataFusionError::Execution("UPDATE row count overflow".to_string())
                    })?;
                    let post_image = LIX_BRANCH_COLS
                        .build(lix_branch_schema(), &branch_rows)
                        .map_err(branch_batch_error)?;
                    let mut rows =
                        RawWriteBatch::with_capacity(branch_rows.len().saturating_mul(2));
                    for row in branch_rows {
                        push_branch_stage_rows(
                            &mut rows,
                            row,
                            TransactionWriteOperation::Update,
                            false,
                        );
                    }

                    if !rows.is_empty() {
                        write_ctx
                            .stage_write(TransactionWrite::Rows {
                                mode: TransactionWriteMode::Replace,
                                rows,
                            })
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                    }

                    returning.capture(returning.project(&post_image)?);
                    Ok(count)
                }
                .boxed()
            }),
        })
    }
}

impl BranchSpec {
    /// Unprojected row source used as the UPDATE/DELETE candidate set.
    fn write_row_source(&self, filters: &[Expr]) -> super::spec::RowSource {
        let descriptor_scope = BranchDescriptorScope::from_filters(filters);
        row_source(
            (
                Arc::clone(&self.hot_state),
                Arc::clone(&self.branch_ref),
                descriptor_scope,
            ),
            |(hot_state, branch_ref, descriptor_scope)| async move {
                let rows = load_branch_rows_scoped(
                    hot_state,
                    branch_ref,
                    BranchHeadReadStrategy::Point,
                    descriptor_scope,
                )
                .await
                .map_err(lix_error_to_datafusion_error)?;
                LIX_BRANCH_COLS
                    .build(lix_branch_schema(), &rows)
                    .map_err(branch_batch_error)
            },
        )
    }
}

/// Identity column the upsert driver matches conflicting rows on: a branch row
/// is uniquely its branch id.
const LIX_BRANCH_IDENTITY: &[&str] = &["id"];

#[async_trait]
impl UpsertSupport for BranchSpec {
    fn conflict_identity_columns(&self) -> &[&'static str] {
        LIX_BRANCH_IDENTITY
    }

    async fn insert_staged_rows(
        &self,
        write_ctx: &SqlWriteContext,
        batch: &RecordBatch,
    ) -> Result<StagedUpsert> {
        let default_commit_id = self
            .branch_ref
            .load_head(&write_ctx.active_branch_id())
            .await
            .map_err(lix_error_to_datafusion_error)?
            .map(|head| head.commit_id)
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "INSERT into lix_branch could not resolve active branch head".to_string(),
                )
            })?;
        let branch_rows = branch_insert_rows_from_batch(batch, &default_commit_id)?;
        let mut rows = RawWriteBatch::with_capacity(branch_rows.len().saturating_mul(2));
        for row in branch_rows {
            push_branch_stage_rows(&mut rows, row, TransactionWriteOperation::Insert, false);
        }
        Ok(StagedUpsert::rows(rows))
    }

    fn validate_proposed_batch(&self, batch: &RecordBatch) -> Result<()> {
        for row_index in 0..batch.num_rows() {
            defaultable_bool_insert_value(batch, row_index, "hidden", "INSERT into lix_branch")?;
            defaultable_text_insert_value(batch, row_index, "commit_id", "INSERT into lix_branch")?;
        }
        Ok(())
    }

    async fn materialize_excluded_defaults(
        &self,
        write_ctx: &SqlWriteContext,
        proposed: &RecordBatch,
    ) -> Result<RecordBatch> {
        let materialized = materialize_omitted_column(
            proposed,
            "hidden",
            Arc::new(BooleanArray::from(vec![false; proposed.num_rows()])),
        )?;
        if !insert_column_is_omitted(&materialized, "commit_id") {
            return Ok(materialized);
        }
        let default_commit_id = self
            .branch_ref
            .load_head(&write_ctx.active_branch_id())
            .await
            .map_err(lix_error_to_datafusion_error)?
            .map(|head| head.commit_id)
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "INSERT into lix_branch could not resolve active branch head".to_string(),
                )
            })?;
        let values = (0..materialized.num_rows())
            .map(|_| Some(default_commit_id.to_string()))
            .collect::<StringArray>();
        materialize_omitted_column(&materialized, "commit_id", Arc::new(values))
    }

    async fn materialize_returning_insert_defaults(
        &self,
        write_ctx: &SqlWriteContext,
        proposed: &RecordBatch,
    ) -> Result<RecordBatch> {
        // Branch identity is always caller-provided. Reuse the existing
        // excluded-default materialization so a conflict update and a fresh
        // insert observe the same hidden/head defaults.
        self.materialize_excluded_defaults(write_ctx, proposed)
            .await
    }

    async fn capture_upsert_returning(
        &self,
        write_ctx: &SqlWriteContext,
        affected_rows: Vec<UpsertReturningRow>,
        returning: DmlReturning,
    ) -> Result<()> {
        let keys = affected_rows
            .iter()
            .map(|row| {
                required_string_value(
                    row.batch(),
                    row.row_index(),
                    "id",
                    "INSERT ON CONFLICT RETURNING lix_branch",
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if keys.is_empty() {
            let empty = RecordBatch::new_empty(lix_branch_schema());
            returning.capture(returning.project(&empty)?);
            return Ok(());
        }

        // Build fresh readers rather than reuse the session's cached branch
        // ref: a conflict update may have just staged a new branch head.
        let hot_state: Arc<dyn HotStateReader> =
            Arc::new(WriteContextHotStateReader::new(write_ctx.clone()));
        let branch_ref: Arc<dyn BranchRefReader> = Arc::new(
            crate::sql2::WriteContextBranchRefReader::new(write_ctx.clone()),
        );
        let rows = load_branch_rows(hot_state, branch_ref, BranchHeadReadStrategy::Point)
            .await
            .map_err(lix_error_to_datafusion_error)?;
        let batch = LIX_BRANCH_COLS
            .build(lix_branch_schema(), &rows)
            .map_err(branch_batch_error)?;
        let mut post_rows = BTreeMap::new();
        for row_index in 0..batch.num_rows() {
            let id = required_string_value(
                &batch,
                row_index,
                "id",
                "INSERT ON CONFLICT RETURNING lix_branch",
            )?;
            let index = u32::try_from(row_index).map_err(|_| {
                DataFusionError::Execution("lix_branch RETURNING row index overflow".into())
            })?;
            if post_rows.insert(id.clone(), index).is_some() {
                return Err(DataFusionError::Execution(format!(
                    "lix_branch RETURNING post-image contains duplicate row for id '{id}'"
                )));
            }
        }
        let indices = keys
            .iter()
            .map(|id| {
                post_rows.get(id).copied().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "lix_branch RETURNING post-image is missing inserted or updated row '{id}'"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let post_image = take_record_batch_rows(&batch, &indices)?;
        returning.capture(returning.project(&post_image)?);
        Ok(())
    }

    async fn scan_conflict_candidates(
        &self,
        _write_ctx: &SqlWriteContext,
        _proposed: &RecordBatch,
        _target: &super::upsert::UpsertConflictTarget,
    ) -> Result<RecordBatch> {
        let rows = load_branch_rows(
            Arc::clone(&self.hot_state),
            Arc::clone(&self.branch_ref),
            BranchHeadReadStrategy::Point,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        LIX_BRANCH_COLS
            .build(lix_branch_schema(), &rows)
            .map_err(branch_batch_error)
    }

    async fn apply_conflict_update(
        &self,
        _write_ctx: &SqlWriteContext,
        augmented: &RecordBatch,
        assignments: &[(String, Arc<dyn PhysicalExpr>)],
    ) -> Result<StagedUpsert> {
        let branch_rows =
            branch_update_rows_from_batch(augmented, assignments, &lix_branch_schema())?;
        reject_protected_branch_updates(&branch_rows)?;
        let mut rows = RawWriteBatch::with_capacity(branch_rows.len().saturating_mul(2));
        for row in branch_rows {
            push_branch_stage_rows(&mut rows, row, TransactionWriteOperation::Update, false);
        }
        Ok(StagedUpsert::rows(rows))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BranchRow {
    id: String,
    name: String,
    hidden: bool,
    commit_id: CommitId,
}

static LIX_BRANCH_COLS: ColumnTable<BranchRow> = ColumnTable {
    columns: &[
        ("id", Col::Utf8(|row| Some(row.id.as_str()))),
        ("name", Col::Utf8(|row| Some(row.name.as_str()))),
        ("hidden", Col::Bool(|row| Some(row.hidden))),
        (
            "commit_id",
            Col::Utf8Owned(|row| Some(row.commit_id.to_string())),
        ),
    ],
};

fn branch_batch_error(error: ColumnTableError) -> DataFusionError {
    match error {
        ColumnTableError::UnsupportedColumn(column) => DataFusionError::Execution(format!(
            "sql2 does not support lix_branch column '{column}'"
        )),
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => {
            DataFusionError::Execution(format!("failed to build lix_branch batch: {error}"))
        }
        ColumnTableError::Row(error) => lix_error_to_datafusion_error(error),
    }
}

async fn load_branch_rows(
    hot_state: Arc<dyn HotStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    head_read_strategy: BranchHeadReadStrategy,
) -> Result<Vec<BranchRow>, LixError> {
    load_branch_rows_scoped(
        hot_state,
        branch_ref,
        head_read_strategy,
        BranchDescriptorScope::All,
    )
    .await
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BranchDescriptorScope {
    All,
    Ids(BTreeSet<String>),
}

impl BranchDescriptorScope {
    fn from_read_filters(filters: &[Expr]) -> Self {
        exact_canonical_branch_ids_from_filters(filters).map_or(Self::All, Self::Ids)
    }

    fn from_filters(filters: &[Expr]) -> Self {
        exact_branch_ids_from_filters(filters).map_or(Self::All, Self::Ids)
    }
}

async fn load_branch_rows_scoped(
    hot_state: Arc<dyn HotStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    head_read_strategy: BranchHeadReadStrategy,
    descriptor_scope: BranchDescriptorScope,
) -> Result<Vec<BranchRow>, LixError> {
    let entity_pks = match descriptor_scope {
        BranchDescriptorScope::All => Vec::new(),
        BranchDescriptorScope::Ids(ids) if ids.is_empty() => return Ok(Vec::new().into()),
        BranchDescriptorScope::Ids(ids) => ids
            .into_iter()
            .map(|id| {
                EntityPk::uuid_from_canonical(&id).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("branch id must be a canonical UUID: {error}"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    };
    let descriptor_rows = hot_state
        .scan_batch(&HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec!["lix_branch_descriptor".to_string()],
                branch_ids: vec![GLOBAL_BRANCH_ID.to_string()],
                entity_pks,
                ..HotStateFilter::default()
            },
            projection: HotStateProjection::default(),
            limit: None,
        })
        .await?;

    let descriptors = descriptor_rows
        .iter()
        .map(parse_descriptor)
        .collect::<Result<Vec<_>, _>>()?;

    match head_read_strategy {
        BranchHeadReadStrategy::Batch => {
            // A read session has already resolved and cached the active branch.
            // Keep the zero-to-two-descriptor case on point lookup: the active
            // head is already cached, so at most one storage read remains.
            // Batch once there is actual fanout to collapse.
            if descriptors.len() <= 2 {
                return load_branch_rows_with_point_lookups(descriptors, branch_ref).await;
            }
            match branch_ref.scan_heads().await {
                Ok(heads) => Ok(join_branch_descriptors_with_heads(descriptors, heads)),
                // A full scan can encounter a malformed ref unrelated to the
                // descriptors being listed. Preserve point-read semantics in
                // that case while keeping the one-scan fast path for valid
                // branch-ref state.
                Err(error)
                    if !matches!(
                        error.code.as_str(),
                        LixError::CODE_STORAGE_ERROR
                            | LixError::CODE_STORAGE_FENCED
                            | LixError::CODE_STORAGE_CLOSED
                    ) =>
                {
                    load_branch_rows_with_point_lookups(descriptors, branch_ref).await
                }
                Err(error) => Err(error),
            }
        }
        BranchHeadReadStrategy::Point => {
            load_branch_rows_with_point_lookups(descriptors, branch_ref).await
        }
    }
}

fn exact_branch_ids_from_filters(filters: &[Expr]) -> Option<BTreeSet<String>> {
    if filters.is_empty() {
        return None;
    }
    let mut ids = None::<BTreeSet<String>>;
    for filter in filters {
        let filter_ids = exact_branch_ids_from_filter(filter)?;
        ids = Some(match ids {
            Some(ids) => ids.intersection(&filter_ids).cloned().collect(),
            None => filter_ids,
        });
    }
    ids
}

fn exact_canonical_branch_ids_from_filters(filters: &[Expr]) -> Option<BTreeSet<String>> {
    let ids = exact_branch_ids_from_filters(filters)?;
    ids.iter()
        .all(|id| EntityPk::uuid_from_canonical(id).is_ok())
        .then_some(ids)
}

fn exact_branch_ids_from_filter(filter: &Expr) -> Option<BTreeSet<String>> {
    match filter {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
            let left = exact_branch_ids_from_filter(&binary_expr.left)?;
            let right = exact_branch_ids_from_filter(&binary_expr.right)?;
            Some(left.intersection(&right).cloned().collect())
        }
        // Even an OR made only from id equalities falls back to the full
        // candidate source. This keeps routing deliberately narrow and avoids
        // changing behavior for expression trees DataFusion must evaluate.
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => None,
        Expr::BinaryExpr(binary_expr) => {
            exact_branch_id_from_binary_filter(binary_expr).map(|id| BTreeSet::from([id]))
        }
        Expr::InList(in_list) => exact_branch_ids_from_in_list(in_list),
        _ => None,
    }
}

fn exact_branch_id_from_binary_filter(binary_expr: &BinaryExpr) -> Option<String> {
    if binary_expr.op != Operator::Eq {
        return None;
    }
    exact_branch_id_from_column_literal(&binary_expr.left, &binary_expr.right)
        .or_else(|| exact_branch_id_from_column_literal(&binary_expr.right, &binary_expr.left))
}

fn exact_branch_id_from_column_literal(column_expr: &Expr, literal_expr: &Expr) -> Option<String> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    if column.name != "id" {
        return None;
    }
    branch_id_string_literal(literal_expr)
}

fn exact_branch_ids_from_in_list(in_list: &InList) -> Option<BTreeSet<String>> {
    if in_list.negated {
        return None;
    }
    let Expr::Column(column) = in_list.expr.as_ref() else {
        return None;
    };
    if column.name != "id" {
        return None;
    }
    let ids = in_list
        .list
        .iter()
        .map(branch_id_string_literal)
        .collect::<Option<BTreeSet<_>>>()?;
    (!ids.is_empty()).then_some(ids)
}

fn branch_id_string_literal(expr: &Expr) -> Option<String> {
    let Expr::Literal(literal, _) = expr else {
        return None;
    };
    match literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(value.clone()),
        _ => None,
    }
}

async fn load_branch_rows_with_point_lookups(
    descriptors: Vec<BranchDescriptor>,
    branch_ref: Arc<dyn BranchRefReader>,
) -> Result<Vec<BranchRow>, LixError> {
    let mut out = Vec::new();
    for descriptor in descriptors {
        let Some(commit_id) = branch_ref.load_head_commit_id(&descriptor.id).await? else {
            continue;
        };
        out.push(BranchRow {
            commit_id,
            id: descriptor.id,
            name: descriptor.name,
            hidden: descriptor.hidden,
        });
    }
    Ok(out)
}

fn join_branch_descriptors_with_heads(
    descriptors: Vec<BranchDescriptor>,
    heads: Vec<BranchHead>,
) -> Vec<BranchRow> {
    let commit_ids_by_branch = heads
        .into_iter()
        .map(|head| (head.branch_id, head.commit_id))
        .collect::<HashMap<_, _>>();
    descriptors
        .into_iter()
        .filter_map(|descriptor| {
            let commit_id = commit_ids_by_branch.get(&descriptor.id).copied()?;
            Some(BranchRow {
                commit_id,
                id: descriptor.id,
                name: descriptor.name,
                hidden: descriptor.hidden,
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BranchDescriptor {
    id: String,
    name: String,
    hidden: bool,
}

fn parse_descriptor(row: MaterializedHotStateRowRef<'_>) -> Result<BranchDescriptor, LixError> {
    let snapshot = parse_snapshot(row, "lix_branch_descriptor")?;
    let id = snapshot
        .get("id")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "lix_branch_descriptor is missing id"))?
        .to_string();
    let name = snapshot
        .get("name")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "lix_branch_descriptor is missing name"))?
        .to_string();
    let hidden = snapshot
        .get("hidden")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    Ok(BranchDescriptor { id, name, hidden })
}

fn parse_snapshot(
    row: MaterializedHotStateRowRef<'_>,
    schema_key: &str,
) -> Result<JsonValue, LixError> {
    let snapshot_content = row
        .snapshot_content()
        .map(|content| content.as_str())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("{schema_key} row is missing snapshot_content"),
            )
        })?;
    serde_json::from_str(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{schema_key} snapshot_content is invalid JSON: {error}"),
        )
    })
}

fn validate_lix_branch_update_assignments(assignments: &[(String, Expr)]) -> Result<()> {
    for (column_name, _) in assignments {
        match column_name.as_str() {
            "name" | "hidden" | "commit_id" => {}
            "id" => {
                return Err(DataFusionError::Execution(
                    "UPDATE lix_branch cannot change immutable column 'id'".to_string(),
                ));
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "UPDATE lix_branch failed: column '{other}' does not exist"
                )));
            }
        }
    }
    Ok(())
}

fn branch_insert_rows_from_batch(
    batch: &RecordBatch,
    default_commit_id: &CommitId,
) -> Result<Vec<BranchRow>> {
    (0..batch.num_rows())
        .map(|row_index| {
            let id = required_string_value(batch, row_index, "id", "INSERT lix_branch")?;
            let name = required_string_value(batch, row_index, "name", "INSERT lix_branch")?;
            let hidden = defaultable_bool_insert_value(
                batch,
                row_index,
                "hidden",
                "INSERT into lix_branch",
            )?
            .unwrap_or(false);
            let commit_id = defaultable_text_insert_value(
                batch,
                row_index,
                "commit_id",
                "INSERT into lix_branch",
            )?
            .map(|commit_id| {
                parse_branch_row_commit_id(commit_id, TransactionWriteOperation::Insert)
            })
            .transpose()?
            .unwrap_or(*default_commit_id);
            Ok(BranchRow {
                id,
                name,
                hidden,
                commit_id,
            })
        })
        .collect()
}

fn branch_rows_from_batch(batch: &RecordBatch) -> Result<Vec<BranchRow>> {
    (0..batch.num_rows())
        .map(|row_index| {
            Ok(BranchRow {
                id: required_string_value(batch, row_index, "id", "DELETE lix_branch")?,
                name: required_string_value(batch, row_index, "name", "DELETE lix_branch")?,
                hidden: required_bool_value(batch, row_index, "hidden", "DELETE lix_branch")?,
                commit_id: parse_branch_row_commit_id(
                    required_string_value(batch, row_index, "commit_id", "DELETE lix_branch")?,
                    TransactionWriteOperation::Delete,
                )?,
            })
        })
        .collect()
}

async fn load_default_branch_id(write_ctx: &SqlWriteContext) -> Result<String> {
    let rows = write_ctx
        .load_exact_hot_state_batch(&HotStateExactBatchRequest {
            rows: vec![HotStateExactRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single(crate::init::DEFAULT_BRANCH_KEY),
                file_id: None,
            }],
            projection: HotStateProjection {
                columns: vec!["snapshot_content".to_string()],
            },
            untracked: Some(false),
            include_tombstones: false,
        })
        .await
        .map_err(lix_error_to_datafusion_error)?;
    let snapshot = rows
        .row(0)
        .and_then(MaterializedHotStateRowRef::snapshot_content)
        .ok_or_else(|| {
            DataFusionError::Execution("repository default branch is missing".to_string())
        })?;
    serde_json::from_str::<JsonValue>(snapshot)
        .ok()
        .and_then(|value| value.get("value").and_then(JsonValue::as_str).map(str::to_owned))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            DataFusionError::Execution("repository default branch is invalid".to_string())
        })
}

fn reject_protected_branch_deletes(
    rows: &[BranchRow],
    active_branch_id: &str,
    default_branch_id: &str,
) -> Result<()> {
    for row in rows {
        if row.id == GLOBAL_BRANCH_ID {
            return Err(DataFusionError::Execution(
                "DELETE FROM lix_branch cannot delete the global branch".to_string(),
            ));
        }
        if row.id == active_branch_id {
            return Err(DataFusionError::Execution(format!(
                "DELETE FROM lix_branch cannot delete active branch '{}'",
                row.id
            )));
        }
        if row.id == default_branch_id {
            return Err(DataFusionError::Execution(format!(
                "DELETE FROM lix_branch cannot delete repository default branch '{}'",
                row.id
            )));
        }
    }
    Ok(())
}

fn reject_protected_branch_updates(rows: &[BranchRow]) -> Result<()> {
    for row in rows {
        if row.id == GLOBAL_BRANCH_ID {
            return Err(DataFusionError::Execution(
                "UPDATE lix_branch cannot update the global branch".to_string(),
            ));
        }
    }
    Ok(())
}

fn branch_update_rows_from_batch(
    batch: &RecordBatch,
    assignments: &[(String, Arc<dyn PhysicalExpr>)],
    table_schema: &SchemaRef,
) -> Result<Vec<BranchRow>> {
    let assignment_values = UpdateAssignmentValues::evaluate(batch, assignments)?;
    (0..batch.num_rows())
        .map(|row_index| {
            Ok(BranchRow {
                id: required_string_value(batch, row_index, "id", "UPDATE lix_branch")?,
                name: update_string_value(
                    batch,
                    &assignment_values,
                    table_schema,
                    row_index,
                    "name",
                )?,
                hidden: update_bool_value(
                    batch,
                    &assignment_values,
                    table_schema,
                    row_index,
                    "hidden",
                )?,
                commit_id: parse_branch_row_commit_id(
                    update_string_value(
                        batch,
                        &assignment_values,
                        table_schema,
                        row_index,
                        "commit_id",
                    )?,
                    TransactionWriteOperation::Update,
                )?,
            })
        })
        .collect()
}

fn parse_branch_row_commit_id(
    commit_id: String,
    operation: TransactionWriteOperation,
) -> Result<CommitId> {
    let operation_name = match operation {
        TransactionWriteOperation::Insert => "INSERT",
        TransactionWriteOperation::Update => "UPDATE",
        TransactionWriteOperation::Delete => "DELETE",
    };
    CommitId::parse_lix(&commit_id, "lix_branch commit_id").map_err(|error| {
        DataFusionError::Execution(format!(
            "{operation_name} lix_branch received invalid commit_id '{commit_id}': {}",
            error.message
        ))
    })
}

fn push_branch_stage_rows(
    rows: &mut RawWriteBatch,
    row: BranchRow,
    operation: TransactionWriteOperation,
    tombstone: bool,
) {
    let origin = Some(lix_branch_origin(operation, &row.id));
    if tombstone {
        rows.push(with_origin(
            branch_descriptor_tombstone_row(&row.id),
            origin.clone(),
        ));
        rows.push(with_origin(branch_ref_tombstone_row(&row.id), origin));
    } else {
        rows.push(with_origin(
            branch_descriptor_stage_row(&row.id, &row.name, row.hidden),
            origin.clone(),
        ));
        rows.push(with_origin(
            branch_ref_stage_row(&row.id, &row.commit_id),
            origin,
        ));
    }
}

fn with_origin(
    mut row: TransactionWriteRow,
    origin: Option<TransactionWriteOrigin>,
) -> TransactionWriteRow {
    row.origin = origin;
    row
}

fn lix_branch_origin(action: TransactionWriteOperation, branch_id: &str) -> TransactionWriteOrigin {
    TransactionWriteOrigin {
        surface: crate::transaction_types::shared_origin_surface("lix_branch"),
        operation: action,
        primary_key: Some(Arc::new(LogicalPrimaryKey::single_id(branch_id))),
    }
}

fn update_string_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    table_schema: &SchemaRef,
    row_index: usize,
    column_name: &str,
) -> Result<String> {
    let column_index = table_schema.index_of(column_name)?;
    match assignment_values.assigned_or_existing_cell(batch, row_index, column_name)? {
        InsertCell::Omitted => {
            required_string_value(batch, row_index, column_name, "UPDATE lix_branch")
        }
        InsertCell::Provided(SqlCell::Value(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)),
        )) => Ok(value),
        InsertCell::Provided(SqlCell::Null) => Err(DataFusionError::Execution(format!(
            "UPDATE lix_branch requires non-null text column '{column_name}'"
        ))),
        InsertCell::Provided(SqlCell::Value(other)) => Err(DataFusionError::Execution(format!(
            "UPDATE lix_branch expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
    .map_err(|error| {
        if batch.column(column_index).is_null(row_index) {
            DataFusionError::Execution(format!(
                "UPDATE lix_branch requires non-null text column '{column_name}'"
            ))
        } else {
            error
        }
    })
}

fn update_bool_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    table_schema: &SchemaRef,
    row_index: usize,
    column_name: &str,
) -> Result<bool> {
    let column_index = table_schema.index_of(column_name)?;
    match assignment_values.assigned_or_existing_cell(batch, row_index, column_name)? {
        InsertCell::Omitted => {
            required_bool_value(batch, row_index, column_name, "UPDATE lix_branch")
        }
        InsertCell::Provided(SqlCell::Value(ScalarValue::Boolean(Some(value)))) => Ok(value),
        InsertCell::Provided(SqlCell::Null) => Err(DataFusionError::Execution(format!(
            "UPDATE lix_branch requires non-null boolean column '{column_name}'"
        ))),
        InsertCell::Provided(SqlCell::Value(other)) => Err(DataFusionError::Execution(format!(
            "UPDATE lix_branch expected boolean column '{column_name}', got {other:?}"
        ))),
    }
    .map_err(|error| {
        if batch.column(column_index).is_null(row_index) {
            DataFusionError::Execution(format!(
                "UPDATE lix_branch requires non-null boolean column '{column_name}'"
            ))
        } else {
            error
        }
    })
}

pub(super) fn lix_branch_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("hidden", DataType::Boolean, false),
        Field::new("commit_id", DataType::Utf8, false),
    ]))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::common::LixTimestamp;
    use crate::hot_state::MaterializedHotStateRow;
    use datafusion::common::Column;

    struct RowsHotStateReader {
        rows: Vec<MaterializedHotStateRow>,
    }

    #[async_trait]
    impl HotStateReader for RowsHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            _request: &HotStateScanRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateBatch, LixError> {
            Ok(self.rows.clone().into())
        }
    }

    struct CountingBranchRefReader {
        heads: Vec<BranchHead>,
        point_reads: AtomicUsize,
        scans: AtomicUsize,
        scan_error: Option<LixError>,
        point_error_branch: Option<String>,
    }

    struct RoutingHotStateReader {
        rows: Vec<MaterializedHotStateRow>,
        requests: StdMutex<Vec<HotStateScanRequest>>,
    }

    #[async_trait]
    impl HotStateReader for RoutingHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateBatch, LixError> {
            self.requests.lock().unwrap().push(request.clone());
            Ok(self
                .rows
                .iter()
                .filter(|row| {
                    request.filter.entity_pks.is_empty()
                        || request.filter.entity_pks.contains(&row.entity_pk)
                })
                .cloned()
                .collect::<Vec<_>>()
                .into())
        }
    }

    struct RoutingBranchRefReader {
        heads: Vec<BranchHead>,
        point_read_ids: StdMutex<Vec<String>>,
    }

    #[async_trait]
    impl BranchRefReader for RoutingBranchRefReader {
        async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
            self.point_read_ids
                .lock()
                .unwrap()
                .push(branch_id.to_string());
            Ok(self
                .heads
                .iter()
                .find(|head| head.branch_id == branch_id)
                .cloned())
        }

        async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
            panic!("branch write candidates must not scan all branch heads")
        }
    }

    #[async_trait]
    impl BranchRefReader for CountingBranchRefReader {
        async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
            self.point_reads.fetch_add(1, Ordering::Relaxed);
            if self.point_error_branch.as_deref() == Some(branch_id) {
                return Err(LixError::new(
                    LixError::CODE_UNKNOWN,
                    format!("branch ref for '{branch_id}' is malformed"),
                ));
            }
            Ok(self
                .heads
                .iter()
                .find(|head| head.branch_id == branch_id)
                .cloned())
        }

        async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
            self.scans.fetch_add(1, Ordering::Relaxed);
            if let Some(error) = &self.scan_error {
                return Err(error.clone());
            }
            Ok(self.heads.clone())
        }
    }

    fn descriptor_row(id: &str, name: &str) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            entity_pk: EntityPk::uuid_from_canonical(id).expect("fixture branch ID"),
            schema_key: "lix_branch_descriptor".to_string(),
            file_id: None,
            snapshot_content: Some(
                serde_json::json!({ "id": id, "name": name, "hidden": false })
                    .to_string()
                    .into(),
            ),
            metadata: None,
            deleted: false,
            created_at: LixTimestamp::expect_parse(
                "branch descriptor test created_at",
                "2026-07-12T00:00:00Z",
            ),
            updated_at: LixTimestamp::expect_parse(
                "branch descriptor test updated_at",
                "2026-07-12T00:00:00Z",
            ),
            global: true,
            change_id: None,
            commit_id: None,
            untracked: false,
            branch_id: GLOBAL_BRANCH_ID.into(),
        }
    }

    fn head(branch_id: &str) -> BranchHead {
        BranchHead {
            branch_id: branch_id.to_string(),
            commit_id: CommitId::for_test_label(&format!("commit-{branch_id}")),
        }
    }

    #[test]
    fn branch_multi_row_stage_uses_one_shared_metadata_dictionary() {
        let mut rows = RawWriteBatch::with_capacity(200);
        for index in 0..100 {
            let id = format!("01920000-0000-7000-8000-{index:012x}");
            push_branch_stage_rows(
                &mut rows,
                BranchRow {
                    name: format!("Branch {index}"),
                    commit_id: CommitId::for_test_label(&format!("commit-{index}")),
                    id,
                    hidden: false,
                },
                TransactionWriteOperation::Insert,
                false,
            );
        }

        assert_eq!(rows.len(), 200);
        assert_eq!(
            rows.shared_string_count(),
            3,
            "descriptor schema, ref schema, and global branch are batch-wide dictionary values"
        );
        assert!(std::ptr::eq(
            rows.row(0).branch_id,
            rows.row(rows.len() - 1).branch_id
        ));
        assert!(std::ptr::eq(rows.row(0).schema_key, rows.row(2).schema_key));
    }

    fn string_literal(value: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), None)
    }

    fn column(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }

    fn eq_filter(column_name: &str, value: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(column(column_name)),
            Operator::Eq,
            Box::new(string_literal(value)),
        ))
    }

    fn routing_spec() -> (
        BranchSpec,
        Arc<RoutingHotStateReader>,
        Arc<RoutingBranchRefReader>,
    ) {
        let hot_state = Arc::new(RoutingHotStateReader {
            rows: vec![
                descriptor_row("01920000-0000-7000-8000-0000000000a1", "Branch A"),
                descriptor_row("01920000-0000-7000-8000-0000000000b1", "Branch B"),
                descriptor_row("01920000-0000-7000-8000-0000000000c1", "Branch C"),
            ],
            requests: StdMutex::new(Vec::new()),
        });
        let branch_ref = Arc::new(RoutingBranchRefReader {
            heads: vec![
                head("01920000-0000-7000-8000-0000000000a1"),
                head("01920000-0000-7000-8000-0000000000b1"),
                head("01920000-0000-7000-8000-0000000000c1"),
            ],
            point_read_ids: StdMutex::new(Vec::new()),
        });
        let spec = BranchSpec {
            hot_state: hot_state.clone(),
            branch_ref: branch_ref.clone(),
            head_read_strategy: BranchHeadReadStrategy::Point,
        };
        (spec, hot_state, branch_ref)
    }

    #[tokio::test]
    async fn branch_write_id_filter_routes_descriptor_and_head_point_reads() {
        let (spec, hot_state, branch_ref) = routing_spec();
        let source =
            spec.write_row_source(&[eq_filter("id", "01920000-0000-7000-8000-0000000000b1")]);

        let batch = source().await.unwrap();

        assert_eq!(batch.num_rows(), 1);
        let requests = hot_state.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].filter.entity_pks,
            vec![
                EntityPk::uuid_from_canonical("01920000-0000-7000-8000-0000000000b1")
                    .expect("fixture branch ID")
            ]
        );
        assert_eq!(
            branch_ref.point_read_ids.lock().unwrap().as_slice(),
            &["01920000-0000-7000-8000-0000000000b1".to_string()]
        );
    }

    #[tokio::test]
    async fn branch_read_id_filter_routes_descriptor_and_head_point_reads() {
        let (mut spec, hot_state, branch_ref) = routing_spec();
        spec.head_read_strategy = BranchHeadReadStrategy::Batch;
        let filter = eq_filter("id", "01920000-0000-7000-8000-0000000000b1");
        assert_eq!(
            spec.filter_pushdown(&filter),
            TableProviderFilterPushDown::Exact
        );

        let planned = spec
            .plan_scan(None, &[filter], None, &ExecutionProps::new())
            .await
            .unwrap();
        let batch = planned.source.load_single_batch().await.unwrap();

        assert_eq!(batch.num_rows(), 1);
        let requests = hot_state.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].filter.entity_pks,
            vec![
                EntityPk::uuid_from_canonical("01920000-0000-7000-8000-0000000000b1")
                    .expect("fixture branch ID")
            ]
        );
        assert_eq!(
            branch_ref.point_read_ids.lock().unwrap().as_slice(),
            &["01920000-0000-7000-8000-0000000000b1".to_string()]
        );
    }

    #[test]
    fn branch_read_filter_pushdown_rejects_non_id_and_noncanonical_filters() {
        let (spec, _, _) = routing_spec();
        assert_eq!(
            spec.filter_pushdown(&eq_filter("name", "Branch A")),
            TableProviderFilterPushDown::Unsupported
        );
        assert_eq!(
            spec.filter_pushdown(&eq_filter("id", "not-a-branch-id")),
            TableProviderFilterPushDown::Unsupported
        );
    }

    #[tokio::test]
    async fn branch_write_or_filter_falls_back_to_full_candidate_source() {
        let (spec, hot_state, branch_ref) = routing_spec();
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "01920000-0000-7000-8000-0000000000a1")),
            Operator::Or,
            Box::new(eq_filter("id", "01920000-0000-7000-8000-0000000000b1")),
        ));
        let source = spec.write_row_source(&[filter]);

        let batch = source().await.unwrap();

        assert_eq!(batch.num_rows(), 3);
        let requests = hot_state.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].filter.entity_pks.is_empty());
        assert_eq!(
            branch_ref.point_read_ids.lock().unwrap().as_slice(),
            &[
                "01920000-0000-7000-8000-0000000000a1".to_string(),
                "01920000-0000-7000-8000-0000000000b1".to_string(),
                "01920000-0000-7000-8000-0000000000c1".to_string(),
            ]
        );
    }

    #[test]
    fn branch_write_filter_routing_accepts_exact_in_and_rejects_expressions() {
        let in_filter = Expr::InList(InList::new(
            Box::new(column("id")),
            vec![
                string_literal("01920000-0000-7000-8000-0000000000b1"),
                string_literal("01920000-0000-7000-8000-0000000000a1"),
            ],
            false,
        ));
        assert_eq!(
            exact_branch_ids_from_filters(&[in_filter]),
            Some(BTreeSet::from([
                "01920000-0000-7000-8000-0000000000a1".to_string(),
                "01920000-0000-7000-8000-0000000000b1".to_string(),
            ]))
        );

        let expression_filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(column("id")),
            Operator::Eq,
            Box::new(column("name")),
        ));
        assert_eq!(exact_branch_ids_from_filters(&[expression_filter]), None);
        assert_eq!(
            exact_branch_ids_from_filters(&[eq_filter("name", "Branch A")]),
            None
        );
    }

    #[tokio::test]
    async fn batch_head_read_joins_matching_descriptors_with_one_scan() {
        let hot_state = Arc::new(RowsHotStateReader {
            rows: vec![
                descriptor_row("01920000-0000-7000-8000-0000000000a1", "Branch A"),
                descriptor_row("01920000-0000-7000-8000-0000000000b1", "Branch B"),
                descriptor_row("01920000-0000-7000-8000-0000000000d1", "Descriptor only"),
            ],
        });
        let branch_ref = Arc::new(CountingBranchRefReader {
            heads: vec![
                head("01920000-0000-7000-8000-0000000000a1"),
                head("01920000-0000-7000-8000-0000000000b1"),
                head("01920000-0000-7000-8000-0000000000e1"),
            ],
            point_reads: AtomicUsize::new(0),
            scans: AtomicUsize::new(0),
            scan_error: None,
            point_error_branch: None,
        });

        let rows = load_branch_rows(
            hot_state,
            branch_ref.clone(),
            BranchHeadReadStrategy::Batch,
        )
        .await
        .unwrap();

        assert_eq!(
            rows,
            vec![
                BranchRow {
                    id: "01920000-0000-7000-8000-0000000000a1".to_string(),
                    name: "Branch A".to_string(),
                    hidden: false,
                    commit_id: head("01920000-0000-7000-8000-0000000000a1").commit_id,
                },
                BranchRow {
                    id: "01920000-0000-7000-8000-0000000000b1".to_string(),
                    name: "Branch B".to_string(),
                    hidden: false,
                    commit_id: head("01920000-0000-7000-8000-0000000000b1").commit_id,
                },
            ]
        );
        assert_eq!(branch_ref.scans.load(Ordering::Relaxed), 1);
        assert_eq!(branch_ref.point_reads.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn batch_head_read_avoids_scan_for_single_descriptor() {
        let hot_state = Arc::new(RowsHotStateReader {
            rows: vec![descriptor_row(
                "01920000-0000-7000-8000-0000000000a1",
                "Branch A",
            )],
        });
        let branch_ref = Arc::new(CountingBranchRefReader {
            heads: vec![head("01920000-0000-7000-8000-0000000000a1")],
            point_reads: AtomicUsize::new(0),
            scans: AtomicUsize::new(0),
            scan_error: None,
            point_error_branch: None,
        });

        let rows = load_branch_rows(
            hot_state,
            branch_ref.clone(),
            BranchHeadReadStrategy::Batch,
        )
        .await
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(branch_ref.scans.load(Ordering::Relaxed), 0);
        assert_eq!(branch_ref.point_reads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn batch_head_read_falls_back_to_point_reads_when_scan_fails() {
        let hot_state = Arc::new(RowsHotStateReader {
            rows: vec![
                descriptor_row("01920000-0000-7000-8000-0000000000a1", "Branch A"),
                descriptor_row("01920000-0000-7000-8000-0000000000b1", "Branch B"),
                descriptor_row("01920000-0000-7000-8000-0000000000c1", "Branch C"),
            ],
        });
        let branch_ref = Arc::new(CountingBranchRefReader {
            heads: vec![
                head("01920000-0000-7000-8000-0000000000a1"),
                head("01920000-0000-7000-8000-0000000000b1"),
                head("01920000-0000-7000-8000-0000000000c1"),
            ],
            point_reads: AtomicUsize::new(0),
            scans: AtomicUsize::new(0),
            scan_error: Some(LixError::new(
                LixError::CODE_UNKNOWN,
                "unrelated branch ref is malformed",
            )),
            point_error_branch: None,
        });

        let rows = load_branch_rows(
            hot_state,
            branch_ref.clone(),
            BranchHeadReadStrategy::Batch,
        )
        .await
        .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(branch_ref.scans.load(Ordering::Relaxed), 1);
        assert_eq!(branch_ref.point_reads.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn batch_head_read_still_rejects_a_malformed_selected_ref() {
        let hot_state = Arc::new(RowsHotStateReader {
            rows: vec![
                descriptor_row("01920000-0000-7000-8000-0000000000a1", "Branch A"),
                descriptor_row("01920000-0000-7000-8000-0000000000b1", "Branch B"),
                descriptor_row("01920000-0000-7000-8000-0000000000c1", "Branch C"),
            ],
        });
        let branch_ref = Arc::new(CountingBranchRefReader {
            heads: vec![
                head("01920000-0000-7000-8000-0000000000a1"),
                head("01920000-0000-7000-8000-0000000000b1"),
                head("01920000-0000-7000-8000-0000000000c1"),
            ],
            point_reads: AtomicUsize::new(0),
            scans: AtomicUsize::new(0),
            scan_error: Some(LixError::new(
                LixError::CODE_UNKNOWN,
                "a branch ref is malformed",
            )),
            point_error_branch: Some("01920000-0000-7000-8000-0000000000b1".to_string()),
        });

        let error = load_branch_rows(
            hot_state,
            branch_ref.clone(),
            BranchHeadReadStrategy::Batch,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .message
                .contains("01920000-0000-7000-8000-0000000000b1")
        );
        assert_eq!(branch_ref.scans.load(Ordering::Relaxed), 1);
        assert_eq!(branch_ref.point_reads.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn batch_head_read_does_not_amplify_storage_errors() {
        for code in [
            LixError::CODE_STORAGE_ERROR,
            LixError::CODE_STORAGE_FENCED,
            LixError::CODE_STORAGE_CLOSED,
        ] {
            let hot_state = Arc::new(RowsHotStateReader {
                rows: vec![
                    descriptor_row("01920000-0000-7000-8000-0000000000a1", "Branch A"),
                    descriptor_row("01920000-0000-7000-8000-0000000000b1", "Branch B"),
                    descriptor_row("01920000-0000-7000-8000-0000000000c1", "Branch C"),
                ],
            });
            let branch_ref = Arc::new(CountingBranchRefReader {
                heads: vec![
                    head("01920000-0000-7000-8000-0000000000a1"),
                    head("01920000-0000-7000-8000-0000000000b1"),
                    head("01920000-0000-7000-8000-0000000000c1"),
                ],
                point_reads: AtomicUsize::new(0),
                scans: AtomicUsize::new(0),
                scan_error: Some(LixError::new(code, "branch-ref scan failed")),
                point_error_branch: None,
            });

            let error = load_branch_rows(
                hot_state,
                branch_ref.clone(),
                BranchHeadReadStrategy::Batch,
            )
            .await
            .unwrap_err();

            assert_eq!(error.code, code);
            assert_eq!(branch_ref.scans.load(Ordering::Relaxed), 1);
            assert_eq!(branch_ref.point_reads.load(Ordering::Relaxed), 0);
        }
    }
}
