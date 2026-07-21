use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
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
use crate::live_state::{
    LiveStateFilter, LiveStateProjection, LiveStateReader, LiveStateScanRequest,
    MaterializedLiveStateRow,
};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::write_normalization::{InsertCell, SqlCell, UpdateAssignmentValues};
use crate::sql2::{SqlWriteContext, WriteAccess, WriteContextLiveStateReader};
use crate::transaction::types::{
    LogicalPrimaryKey, TransactionWrite, TransactionWriteMode, TransactionWriteOperation,
    TransactionWriteOrigin, TransactionWriteRow,
};

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::spec::{
    PlannedDml, PlannedScan, TableSpec, projected_schema, register_spec_table, row_source,
};
use super::upsert::{StagedUpsert, UpsertSupport};
use super::values::{
    optional_bool_value, optional_string_value, required_bool_value, required_string_value,
};

pub(super) async fn register_lix_branch_read_provider(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(BranchSpec {
            live_state,
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
    let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
    register_spec_table(
        session,
        surface_name,
        Arc::new(BranchSpec {
            live_state,
            branch_ref,
            head_read_strategy: BranchHeadReadStrategy::Point,
        }),
        WriteAccess::write(write_ctx),
    )
}

struct BranchSpec {
    live_state: Arc<dyn LiveStateReader>,
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

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&lix_branch_schema(), projection);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            load: row_source(
                (
                    Arc::clone(&self.live_state),
                    Arc::clone(&self.branch_ref),
                    schema,
                    self.head_read_strategy,
                ),
                |(live_state, branch_ref, schema, head_read_strategy)| async move {
                    let rows = load_branch_rows(live_state, branch_ref, head_read_strategy)
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
        let mut rows = Vec::new();
        let mut count = 0u64;
        for batch in batches {
            let branch_rows = branch_insert_rows_from_batch(&batch, &default_commit_id)?;
            count = count
                .checked_add(u64::try_from(branch_rows.len()).map_err(|_| {
                    DataFusionError::Execution("INSERT row count overflow".to_string())
                })?)
                .ok_or_else(|| DataFusionError::Execution("INSERT row count overflow".into()))?;
            rows.extend(branch_rows.into_iter().flat_map(branch_insert_stage_rows));
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
                    reject_protected_branch_deletes(&branch_rows, &active_branch_id)?;
                    let count = u64::try_from(branch_rows.len()).map_err(|_| {
                        DataFusionError::Execution("DELETE row count overflow".to_string())
                    })?;
                    let rows = branch_rows
                        .into_iter()
                        .flat_map(branch_tombstone_rows)
                        .collect::<Vec<_>>();

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
                    let rows = branch_rows
                        .into_iter()
                        .flat_map(branch_update_stage_rows)
                        .collect::<Vec<_>>();

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
}

impl BranchSpec {
    /// Unprojected row source used as the UPDATE/DELETE candidate set.
    fn write_row_source(&self, filters: &[Expr]) -> super::spec::RowSource {
        let descriptor_scope = BranchDescriptorScope::from_write_filters(filters);
        row_source(
            (
                Arc::clone(&self.live_state),
                Arc::clone(&self.branch_ref),
                descriptor_scope,
            ),
            |(live_state, branch_ref, descriptor_scope)| async move {
                let rows = load_branch_rows_scoped(
                    live_state,
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
        let rows = branch_rows
            .into_iter()
            .flat_map(branch_insert_stage_rows)
            .collect::<Vec<_>>();
        Ok(StagedUpsert::rows(rows))
    }

    async fn scan_conflict_candidates(
        &self,
        _write_ctx: &SqlWriteContext,
        _proposed: &RecordBatch,
        _target: &super::upsert::UpsertConflictTarget,
    ) -> Result<RecordBatch> {
        let rows = load_branch_rows(
            Arc::clone(&self.live_state),
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
        let rows = branch_rows
            .into_iter()
            .flat_map(branch_update_stage_rows)
            .collect::<Vec<_>>();
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
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    head_read_strategy: BranchHeadReadStrategy,
) -> Result<Vec<BranchRow>, LixError> {
    load_branch_rows_scoped(
        live_state,
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
    fn from_write_filters(filters: &[Expr]) -> Self {
        exact_branch_ids_from_write_filters(filters).map_or(Self::All, Self::Ids)
    }
}

async fn load_branch_rows_scoped(
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    head_read_strategy: BranchHeadReadStrategy,
    descriptor_scope: BranchDescriptorScope,
) -> Result<Vec<BranchRow>, LixError> {
    let entity_pks = match descriptor_scope {
        BranchDescriptorScope::All => Vec::new(),
        BranchDescriptorScope::Ids(ids) if ids.is_empty() => return Ok(Vec::new()),
        BranchDescriptorScope::Ids(ids) => ids.into_iter().map(EntityPk::single).collect(),
    };
    let descriptor_rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec!["lix_branch_descriptor".to_string()],
                branch_ids: vec![GLOBAL_BRANCH_ID.to_string()],
                entity_pks,
                ..LiveStateFilter::default()
            },
            projection: LiveStateProjection::default(),
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
                Err(error) if error.code != LixError::CODE_STORAGE_ERROR => {
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

fn exact_branch_ids_from_write_filters(filters: &[Expr]) -> Option<BTreeSet<String>> {
    if filters.is_empty() {
        return None;
    }
    let mut ids = None::<BTreeSet<String>>;
    for filter in filters {
        let filter_ids = exact_branch_ids_from_write_filter(filter)?;
        ids = Some(match ids {
            Some(ids) => ids.intersection(&filter_ids).cloned().collect(),
            None => filter_ids,
        });
    }
    ids
}

fn exact_branch_ids_from_write_filter(filter: &Expr) -> Option<BTreeSet<String>> {
    match filter {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
            let left = exact_branch_ids_from_write_filter(&binary_expr.left)?;
            let right = exact_branch_ids_from_write_filter(&binary_expr.right)?;
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

fn parse_descriptor(row: &MaterializedLiveStateRow) -> Result<BranchDescriptor, LixError> {
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

fn parse_snapshot(row: &MaterializedLiveStateRow, schema_key: &str) -> Result<JsonValue, LixError> {
    let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
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
            let hidden = optional_bool_value(batch, row_index, "hidden", "INSERT lix_branch")?
                .unwrap_or(false);
            let commit_id =
                optional_string_value(batch, row_index, "commit_id", "INSERT lix_branch")?
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

fn reject_protected_branch_deletes(rows: &[BranchRow], active_branch_id: &str) -> Result<()> {
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

fn branch_stage_rows(
    row: BranchRow,
    origin: Option<TransactionWriteOrigin>,
) -> Vec<TransactionWriteRow> {
    vec![
        with_origin(
            branch_descriptor_stage_row(&row.id, &row.name, row.hidden),
            origin.clone(),
        ),
        with_origin(branch_ref_stage_row(&row.id, &row.commit_id), origin),
    ]
}

fn branch_tombstone_rows(row: BranchRow) -> Vec<TransactionWriteRow> {
    let origin = Some(lix_branch_origin(
        TransactionWriteOperation::Delete,
        &row.id,
    ));
    vec![
        with_origin(branch_descriptor_tombstone_row(&row.id), origin.clone()),
        with_origin(branch_ref_tombstone_row(&row.id), origin),
    ]
}

fn branch_insert_stage_rows(row: BranchRow) -> Vec<TransactionWriteRow> {
    let origin = lix_branch_origin(TransactionWriteOperation::Insert, &row.id);
    branch_stage_rows(row, Some(origin))
}

fn branch_update_stage_rows(row: BranchRow) -> Vec<TransactionWriteRow> {
    let origin = lix_branch_origin(TransactionWriteOperation::Update, &row.id);
    branch_stage_rows(row, Some(origin))
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
        surface: "lix_branch".to_string(),
        operation: action,
        primary_key: Some(LogicalPrimaryKey {
            columns: vec!["id".to_string()],
            values: vec![branch_id.to_string()],
        }),
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
    use crate::live_state::LiveStateRowRequest;
    use datafusion::common::Column;

    struct RowsLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for RowsLiveStateReader {
        async fn load_exact_rows(
            &self,
            request: &crate::live_state::LiveStateExactBatchRequest,
        ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
            crate::live_state::load_exact_rows_via_scan_for_test(self, request).await
        }

        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    struct CountingBranchRefReader {
        heads: Vec<BranchHead>,
        point_reads: AtomicUsize,
        scans: AtomicUsize,
        scan_error: Option<LixError>,
        point_error_branch: Option<String>,
    }

    struct RoutingLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
        requests: StdMutex<Vec<LiveStateScanRequest>>,
    }

    #[async_trait]
    impl LiveStateReader for RoutingLiveStateReader {
        async fn load_exact_rows(
            &self,
            request: &crate::live_state::LiveStateExactBatchRequest,
        ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
            crate::live_state::load_exact_rows_via_scan_for_test(self, request).await
        }

        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            self.requests.lock().unwrap().push(request.clone());
            Ok(self
                .rows
                .iter()
                .filter(|row| {
                    request.filter.entity_pks.is_empty()
                        || request.filter.entity_pks.contains(&row.entity_pk)
                })
                .cloned()
                .collect())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
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

    fn descriptor_row(id: &str, name: &str) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: EntityPk::single(id),
            schema_key: "lix_branch_descriptor".to_string(),
            file_id: None,
            snapshot_content: Some(
                serde_json::json!({ "id": id, "name": name, "hidden": false }).to_string(),
            ),
            metadata: None,
            deleted: false,
            created_at: "2026-07-12T00:00:00Z".to_string(),
            updated_at: "2026-07-12T00:00:00Z".to_string(),
            global: true,
            change_id: None,
            commit_id: None,
            untracked: false,
            branch_id: GLOBAL_BRANCH_ID.to_string(),
        }
    }

    fn head(branch_id: &str) -> BranchHead {
        BranchHead {
            branch_id: branch_id.to_string(),
            commit_id: CommitId::for_test_label(&format!("commit-{branch_id}")),
        }
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
        Arc<RoutingLiveStateReader>,
        Arc<RoutingBranchRefReader>,
    ) {
        let live_state = Arc::new(RoutingLiveStateReader {
            rows: vec![
                descriptor_row("branch-a", "Branch A"),
                descriptor_row("branch-b", "Branch B"),
                descriptor_row("branch-c", "Branch C"),
            ],
            requests: StdMutex::new(Vec::new()),
        });
        let branch_ref = Arc::new(RoutingBranchRefReader {
            heads: vec![head("branch-a"), head("branch-b"), head("branch-c")],
            point_read_ids: StdMutex::new(Vec::new()),
        });
        let spec = BranchSpec {
            live_state: live_state.clone(),
            branch_ref: branch_ref.clone(),
            head_read_strategy: BranchHeadReadStrategy::Point,
        };
        (spec, live_state, branch_ref)
    }

    #[tokio::test]
    async fn branch_write_id_filter_routes_descriptor_and_head_point_reads() {
        let (spec, live_state, branch_ref) = routing_spec();
        let source = spec.write_row_source(&[eq_filter("id", "branch-b")]);

        let batch = source().await.unwrap();

        assert_eq!(batch.num_rows(), 1);
        let requests = live_state.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].filter.entity_pks,
            vec![EntityPk::single("branch-b")]
        );
        assert_eq!(
            branch_ref.point_read_ids.lock().unwrap().as_slice(),
            &["branch-b".to_string()]
        );
    }

    #[tokio::test]
    async fn branch_write_or_filter_falls_back_to_full_candidate_source() {
        let (spec, live_state, branch_ref) = routing_spec();
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "branch-a")),
            Operator::Or,
            Box::new(eq_filter("id", "branch-b")),
        ));
        let source = spec.write_row_source(&[filter]);

        let batch = source().await.unwrap();

        assert_eq!(batch.num_rows(), 3);
        let requests = live_state.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].filter.entity_pks.is_empty());
        assert_eq!(
            branch_ref.point_read_ids.lock().unwrap().as_slice(),
            &[
                "branch-a".to_string(),
                "branch-b".to_string(),
                "branch-c".to_string(),
            ]
        );
    }

    #[test]
    fn branch_write_filter_routing_accepts_exact_in_and_rejects_expressions() {
        let in_filter = Expr::InList(InList::new(
            Box::new(column("id")),
            vec![string_literal("branch-b"), string_literal("branch-a")],
            false,
        ));
        assert_eq!(
            exact_branch_ids_from_write_filters(&[in_filter]),
            Some(BTreeSet::from([
                "branch-a".to_string(),
                "branch-b".to_string(),
            ]))
        );

        let expression_filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(column("id")),
            Operator::Eq,
            Box::new(column("name")),
        ));
        assert_eq!(
            exact_branch_ids_from_write_filters(&[expression_filter]),
            None
        );
        assert_eq!(
            exact_branch_ids_from_write_filters(&[eq_filter("name", "Branch A")]),
            None
        );
    }

    #[tokio::test]
    async fn batch_head_read_joins_matching_descriptors_with_one_scan() {
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                descriptor_row("branch-a", "Branch A"),
                descriptor_row("branch-b", "Branch B"),
                descriptor_row("descriptor-only", "Descriptor only"),
            ],
        });
        let branch_ref = Arc::new(CountingBranchRefReader {
            heads: vec![head("branch-a"), head("branch-b"), head("ref-only")],
            point_reads: AtomicUsize::new(0),
            scans: AtomicUsize::new(0),
            scan_error: None,
            point_error_branch: None,
        });

        let rows = load_branch_rows(
            live_state,
            branch_ref.clone(),
            BranchHeadReadStrategy::Batch,
        )
        .await
        .unwrap();

        assert_eq!(
            rows,
            vec![
                BranchRow {
                    id: "branch-a".to_string(),
                    name: "Branch A".to_string(),
                    hidden: false,
                    commit_id: head("branch-a").commit_id,
                },
                BranchRow {
                    id: "branch-b".to_string(),
                    name: "Branch B".to_string(),
                    hidden: false,
                    commit_id: head("branch-b").commit_id,
                },
            ]
        );
        assert_eq!(branch_ref.scans.load(Ordering::Relaxed), 1);
        assert_eq!(branch_ref.point_reads.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn batch_head_read_avoids_scan_for_single_descriptor() {
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![descriptor_row("branch-a", "Branch A")],
        });
        let branch_ref = Arc::new(CountingBranchRefReader {
            heads: vec![head("branch-a")],
            point_reads: AtomicUsize::new(0),
            scans: AtomicUsize::new(0),
            scan_error: None,
            point_error_branch: None,
        });

        let rows = load_branch_rows(
            live_state,
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
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                descriptor_row("branch-a", "Branch A"),
                descriptor_row("branch-b", "Branch B"),
                descriptor_row("branch-c", "Branch C"),
            ],
        });
        let branch_ref = Arc::new(CountingBranchRefReader {
            heads: vec![head("branch-a"), head("branch-b"), head("branch-c")],
            point_reads: AtomicUsize::new(0),
            scans: AtomicUsize::new(0),
            scan_error: Some(LixError::new(
                LixError::CODE_UNKNOWN,
                "unrelated branch ref is malformed",
            )),
            point_error_branch: None,
        });

        let rows = load_branch_rows(
            live_state,
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
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                descriptor_row("branch-a", "Branch A"),
                descriptor_row("branch-b", "Branch B"),
                descriptor_row("branch-c", "Branch C"),
            ],
        });
        let branch_ref = Arc::new(CountingBranchRefReader {
            heads: vec![head("branch-a"), head("branch-b"), head("branch-c")],
            point_reads: AtomicUsize::new(0),
            scans: AtomicUsize::new(0),
            scan_error: Some(LixError::new(
                LixError::CODE_UNKNOWN,
                "a branch ref is malformed",
            )),
            point_error_branch: Some("branch-b".to_string()),
        });

        let error = load_branch_rows(
            live_state,
            branch_ref.clone(),
            BranchHeadReadStrategy::Batch,
        )
        .await
        .unwrap_err();

        assert!(error.message.contains("branch-b"));
        assert_eq!(branch_ref.scans.load(Ordering::Relaxed), 1);
        assert_eq!(branch_ref.point_reads.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn batch_head_read_does_not_amplify_storage_errors() {
        let live_state = Arc::new(RowsLiveStateReader {
            rows: vec![
                descriptor_row("branch-a", "Branch A"),
                descriptor_row("branch-b", "Branch B"),
                descriptor_row("branch-c", "Branch C"),
            ],
        });
        let branch_ref = Arc::new(CountingBranchRefReader {
            heads: vec![head("branch-a"), head("branch-b"), head("branch-c")],
            point_reads: AtomicUsize::new(0),
            scans: AtomicUsize::new(0),
            scan_error: Some(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "branch-ref scan failed",
            )),
            point_error_branch: None,
        });

        let error = load_branch_rows(
            live_state,
            branch_ref.clone(),
            BranchHeadReadStrategy::Batch,
        )
        .await
        .unwrap_err();

        assert_eq!(error.code, LixError::CODE_STORAGE_ERROR);
        assert_eq!(branch_ref.scans.load(Ordering::Relaxed), 1);
        assert_eq!(branch_ref.point_reads.load(Ordering::Relaxed), 0);
    }
}
