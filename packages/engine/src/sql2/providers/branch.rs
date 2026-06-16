use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::context::ExecutionProps;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalExpr;
use futures_util::FutureExt;
use serde_json::Value as JsonValue;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::{
    BranchRefReader, branch_descriptor_stage_row, branch_descriptor_tombstone_row,
    branch_ref_stage_row, branch_ref_tombstone_row,
};
use crate::changelog::CommitId;
use crate::live_state::{
    LiveStateFilter, LiveStateProjection, LiveStateReader, LiveStateScanRequest,
    MaterializedLiveStateRow,
};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::write_normalization::{InsertCell, SqlCell, UpdateAssignmentValues};
use crate::sql2::{
    SqlWriteContext, WriteAccess, WriteContextBranchRefReader, WriteContextLiveStateReader,
};
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
        }),
        WriteAccess::read_only(),
    )
}

pub(super) async fn register_write_provider(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    write_ctx: SqlWriteContext,
) -> Result<(), LixError> {
    let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
    let branch_ref = Arc::new(WriteContextBranchRefReader::new(write_ctx.clone()));
    register_spec_table(
        session,
        surface_name,
        Arc::new(BranchSpec {
            live_state,
            branch_ref,
        }),
        WriteAccess::write(write_ctx),
    )
}

struct BranchSpec {
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
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
            load: row_source(
                (Arc::clone(&self.live_state), Arc::clone(&self.branch_ref), schema),
                |(live_state, branch_ref, schema)| async move {
                    let rows = load_branch_rows(live_state, branch_ref)
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                    LIX_BRANCH_COLS.build(schema, &rows).map_err(branch_batch_error)
                },
            ),
        })
    }

    async fn stage_insert(
        &self,
        write_ctx: &SqlWriteContext,
        batches: Vec<RecordBatch>,
    ) -> Result<u64> {
        let default_commit_id = write_ctx
            .load_branch_head(&write_ctx.active_branch_id())
            .await
            .map_err(lix_error_to_datafusion_error)?
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
        _filters: &[Expr],
    ) -> Result<PlannedDml> {
        let active_branch_id = write_ctx.active_branch_id();
        Ok(PlannedDml {
            source: self.full_row_source(),
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
        _filters: &[Expr],
    ) -> Result<PlannedDml> {
        let table_schema = lix_branch_schema();
        Ok(PlannedDml {
            source: self.full_row_source(),
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
    fn full_row_source(&self) -> super::spec::RowSource {
        row_source(
            (Arc::clone(&self.live_state), Arc::clone(&self.branch_ref)),
            |(live_state, branch_ref)| async move {
                let rows = load_branch_rows(live_state, branch_ref)
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
        let default_commit_id = write_ctx
            .load_branch_head(&write_ctx.active_branch_id())
            .await
            .map_err(lix_error_to_datafusion_error)?
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
    ) -> Result<RecordBatch> {
        let rows = load_branch_rows(Arc::clone(&self.live_state), Arc::clone(&self.branch_ref))
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
) -> Result<Vec<BranchRow>, LixError> {
    let descriptor_rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec!["lix_branch_descriptor".to_string()],
                branch_ids: vec![GLOBAL_BRANCH_ID.to_string()],
                ..LiveStateFilter::default()
            },
            projection: LiveStateProjection::default(),
            limit: None,
        })
        .await?;

    let mut out = Vec::new();
    for descriptor_row in descriptor_rows {
        let descriptor = parse_descriptor(&descriptor_row)?;
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

