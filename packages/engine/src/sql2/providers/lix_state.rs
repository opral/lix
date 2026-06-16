#![allow(
    clippy::manual_let_else,
    clippy::missing_fields_in_debug,
    clippy::unnecessary_literal_bound
)]

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use futures_util::FutureExt;
use serde_json::Value as JsonValue;

use crate::GLOBAL_BRANCH_ID;
use crate::branch::BranchRefReader;
use crate::entity_pk::EntityPk;
use crate::live_state::{
    LiveStateFilter, LiveStateProjection, LiveStateReader, LiveStateRowFilter, LiveStateScanRequest,
};
use crate::sql2::branch_scope::{BranchBinding, resolve_provider_branch_ids};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::read_only::reject_read_only_stage_rows;
use crate::sql2::write_normalization::{InsertCell, SqlCell, UpdateAssignmentValues};
use crate::transaction::types::{TransactionJson, TransactionWriteRow};
use crate::{LixError, NullableKeyFilter, parse_row_metadata_value};

use crate::sql2::{
    SqlWriteContext, WriteAccess, WriteContextBranchRefReader, WriteContextLiveStateReader,
};
use crate::transaction::types::{TransactionWrite, TransactionWriteMode};

use crate::sql2::predicate_typecheck::{
    canonicalize_json_identity_text_filters, validate_json_predicate_filters,
};
use crate::sql2::result_metadata::json_field;

use super::columns::{ColumnTableError, LIVE_STATE_COLS};
use super::spec::{
    PlannedDml, PlannedScan, RowSource, TableSpec, projected_schema, register_spec_table,
    row_source,
};
use super::upsert::{StagedUpsert, UpsertSupport};
use super::values::string_expr_literal;

pub(super) async fn register_lix_state_active_provider(
    session: &SessionContext,
    surface_name: &str,
    active_branch_id: &str,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixStateSpec::active_branch(
            active_branch_id,
            live_state,
            branch_ref,
        )),
        WriteAccess::read_only(),
    )
}

pub(super) async fn register_lix_state_by_branch_provider(
    session: &SessionContext,
    surface_name: &str,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixStateSpec::by_branch(live_state, branch_ref)),
        WriteAccess::read_only(),
    )
}

pub(super) async fn register_lix_state_by_branch_write_provider(
    session: &SessionContext,
    surface_name: &str,
    write_ctx: SqlWriteContext,
) -> Result<(), LixError> {
    let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
    let branch_ref = Arc::new(WriteContextBranchRefReader::new(write_ctx.clone()));
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixStateSpec::by_branch(live_state, branch_ref)),
        WriteAccess::write(write_ctx),
    )
}

pub(super) async fn register_lix_state_active_write_provider(
    session: &SessionContext,
    surface_name: &str,
    write_ctx: SqlWriteContext,
) -> Result<(), LixError> {
    let active_branch_id = write_ctx.active_branch_id();
    let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
    let branch_ref = Arc::new(WriteContextBranchRefReader::new(write_ctx.clone()));
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixStateSpec::active_branch(
            active_branch_id,
            live_state,
            branch_ref,
        )),
        WriteAccess::write(write_ctx),
    )
}

struct LixStateSpec {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    branch_binding: BranchBinding,
}

impl LixStateSpec {
    fn active_branch(
        active_branch_id: impl Into<String>,
        live_state: Arc<dyn LiveStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
    ) -> Self {
        Self {
            schema: lix_state_schema(),
            live_state,
            branch_ref,
            branch_binding: BranchBinding::active(active_branch_id),
        }
    }

    fn by_branch(
        live_state: Arc<dyn LiveStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
    ) -> Self {
        Self {
            schema: lix_state_by_branch_schema(),
            live_state,
            branch_ref,
            branch_binding: BranchBinding::explicit(),
        }
    }

    /// Unprojected candidate-row source for UPDATE/DELETE, scoped by the
    /// branch route derived from the statement filters.
    fn dml_source(&self, write_ctx: &SqlWriteContext, filters: &[Expr]) -> RowSource {
        let route = LixStateByBranchRoute::from_filters(filters);
        let request = lix_state_scan_request(
            &self.schema,
            self.branch_binding.active_branch_id(),
            None,
            &route,
            None,
        );
        row_source(
            (write_ctx.clone(), request, Arc::clone(&self.schema)),
            |(write_ctx, request, table_schema)| async move {
                let rows = write_ctx
                    .scan_live_state(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                LIVE_STATE_COLS
                    .build(table_schema, &rows)
                    .map_err(lix_state_batch_error)
                    .map_err(lix_error_to_datafusion_error)
            },
        )
    }
}

/// Physical-identity columns the upsert driver matches conflicting rows on.
/// The active surface scopes by the active branch implicitly; the by-branch
/// surface carries `branch_id` as a column.
const LIX_STATE_ACTIVE_IDENTITY: &[&str] = &["entity_pk", "schema_key", "file_id"];
const LIX_STATE_BY_BRANCH_IDENTITY: &[&str] = &["entity_pk", "schema_key", "file_id", "branch_id"];

#[async_trait]
impl UpsertSupport for LixStateSpec {
    fn conflict_identity_columns(&self) -> &[&'static str] {
        match self.branch_binding {
            BranchBinding::Active { .. } => LIX_STATE_ACTIVE_IDENTITY,
            BranchBinding::Explicit => LIX_STATE_BY_BRANCH_IDENTITY,
        }
    }

    async fn insert_staged_rows(
        &self,
        _write_ctx: &SqlWriteContext,
        batch: &RecordBatch,
    ) -> Result<StagedUpsert> {
        let branch_binding = self.branch_binding.active_branch_id();
        let rows = lix_state_write_rows_from_batch(batch, branch_binding, "INSERT into lix_state")?;
        reject_read_only_stage_rows(&rows, "INSERT into lix_state")?;
        Ok(StagedUpsert::rows(rows))
    }

    async fn scan_conflict_candidates(
        &self,
        write_ctx: &SqlWriteContext,
        proposed: &RecordBatch,
        _target: &super::upsert::UpsertConflictTarget,
    ) -> Result<RecordBatch> {
        let request =
            lix_state_conflict_scan_request(&self.schema, &self.branch_binding, proposed)?;
        let rows = write_ctx
            .scan_live_state(&request)
            .await
            .map_err(lix_error_to_datafusion_error)?;
        LIVE_STATE_COLS
            .build(Arc::clone(&self.schema), &rows)
            .map_err(lix_state_batch_error)
            .map_err(lix_error_to_datafusion_error)
    }

    async fn apply_conflict_update(
        &self,
        _write_ctx: &SqlWriteContext,
        augmented: &RecordBatch,
        assignments: &[(String, Arc<dyn PhysicalExpr>)],
    ) -> Result<StagedUpsert> {
        let branch_binding = self.branch_binding.active_branch_id();
        let rows = lix_state_update_write_rows_from_batch(augmented, assignments, branch_binding)?;
        reject_read_only_stage_rows(&rows, "INSERT into lix_state")?;
        Ok(StagedUpsert::rows(rows))
    }
}

/// Scan request for the existing rows that could conflict with `proposed`:
/// the distinct (schema_key, entity_pk) identities present, scoped to the
/// active/explicit branch.
fn lix_state_conflict_scan_request(
    schema: &SchemaRef,
    branch_binding: &BranchBinding,
    proposed: &RecordBatch,
) -> Result<LiveStateScanRequest> {
    let mut schema_keys = BTreeSet::new();
    let mut entity_pks = Vec::new();
    for row_index in 0..proposed.num_rows() {
        schema_keys.insert(required_string_value(proposed, row_index, "schema_key")?);
        let entity_pk = required_string_value(proposed, row_index, "entity_pk")?;
        let entity_pk = EntityPk::from_json_array_text(&entity_pk).map_err(|error| {
            DataFusionError::Execution(format!("lix_state upsert has invalid entity_pk: {error}"))
        })?;
        entity_pks.push(entity_pk);
    }
    let branch_ids = match branch_binding {
        BranchBinding::Active { .. } => branch_binding
            .active_branch_id()
            .map(|id| vec![id.to_string()])
            .unwrap_or_default(),
        BranchBinding::Explicit => proposed_branch_ids(proposed)?,
    };
    Ok(LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: schema_keys.into_iter().collect(),
            entity_pks,
            branch_ids,
            ..LiveStateFilter::default()
        },
        projection: LiveStateProjection {
            columns: schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
        },
        limit: None,
    })
}

/// Distinct `branch_id` values present in the proposed batch (by-branch surface).
fn proposed_branch_ids(proposed: &RecordBatch) -> Result<Vec<String>> {
    let mut branch_ids = BTreeSet::new();
    for row_index in 0..proposed.num_rows() {
        if let Some(branch_id) = optional_string_value(proposed, row_index, "branch_id")? {
            branch_ids.insert(branch_id);
        }
    }
    Ok(branch_ids.into_iter().collect())
}

#[async_trait]
impl TableSpec for LixStateSpec {
    fn table_name(&self) -> &str {
        "lix_state"
    }

    fn upsert_support(&self) -> Option<&dyn UpsertSupport> {
        Some(self)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if parse_lix_state_filter(filter).is_some() {
            TableProviderFilterPushDown::Exact
        } else {
            TableProviderFilterPushDown::Unsupported
        }
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let route = LixStateByBranchRoute::from_filters(filters);
        let schema = projected_schema(&self.schema, projection);
        let mut request = lix_state_scan_request(
            &self.schema,
            self.branch_binding.active_branch_id(),
            projection,
            &route,
            limit,
        );
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;

        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            load: row_source(
                (Arc::clone(&self.live_state), schema, request),
                |(live_state, schema, request)| async move {
                    let rows = live_state
                        .scan_rows(&request)
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                    LIVE_STATE_COLS
                        .build(schema, &rows)
                        .map_err(lix_state_batch_error)
                        .map_err(lix_error_to_datafusion_error)
                },
            ),
        })
    }

    async fn stage_insert(
        &self,
        write_ctx: &SqlWriteContext,
        batches: Vec<RecordBatch>,
    ) -> Result<u64> {
        let branch_binding = self.branch_binding.active_branch_id().map(str::to_owned);
        let mut rows = Vec::new();
        for batch in batches {
            rows.extend(lix_state_write_rows_from_batch(
                &batch,
                branch_binding.as_deref(),
                "INSERT into lix_state",
            )?);
        }
        reject_read_only_stage_rows(&rows, "INSERT into lix_state")?;
        let count = u64::try_from(rows.len())
            .map_err(|_| DataFusionError::Execution("INSERT row count overflow".into()))?;

        write_ctx
            .stage_write(TransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows,
            })
            .await
            .map_err(lix_error_to_datafusion_error)?;

        Ok(count)
    }

    fn validate_update_assignments(&self, assignments: &[(String, Expr)]) -> Result<()> {
        validate_lix_state_update_assignments(&self.schema, assignments)
    }

    fn prepare_write_filters(&self, filters: Vec<Expr>) -> Result<Vec<Expr>> {
        let filters = canonicalize_json_identity_text_filters(self.schema.as_ref(), &filters)?;
        validate_json_predicate_filters(self.schema.as_ref(), &filters)?;
        Ok(filters)
    }

    async fn plan_delete(
        &self,
        write_ctx: SqlWriteContext,
        filters: &[Expr],
    ) -> Result<PlannedDml> {
        let branch_binding = self.branch_binding.active_branch_id().map(str::to_owned);
        Ok(PlannedDml {
            source: self.dml_source(&write_ctx, filters),
            apply: Arc::new(move |matched_batch| {
                let write_ctx = write_ctx.clone();
                let branch_binding = branch_binding.clone();
                async move {
                    let write_rows = lix_state_deletable_write_rows_from_batch(
                        &matched_batch,
                        branch_binding.as_deref(),
                    )?;
                    reject_read_only_stage_rows(&write_rows, "DELETE FROM lix_state")?;
                    let count = u64::try_from(write_rows.len()).map_err(|_| {
                        DataFusionError::Execution("DELETE row count overflow".to_string())
                    })?;

                    if count > 0 {
                        write_ctx
                            .stage_write(TransactionWrite::Rows {
                                mode: TransactionWriteMode::Replace,
                                rows: write_rows,
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
        let branch_binding = self.branch_binding.active_branch_id().map(str::to_owned);
        Ok(PlannedDml {
            source: self.dml_source(&write_ctx, filters),
            apply: Arc::new(move |matched_batch| {
                let write_ctx = write_ctx.clone();
                let branch_binding = branch_binding.clone();
                let assignments = assignments.clone();
                async move {
                    let write_rows = lix_state_update_write_rows_from_batch(
                        &matched_batch,
                        &assignments,
                        branch_binding.as_deref(),
                    )?;
                    reject_read_only_stage_rows(&write_rows, "UPDATE lix_state")?;
                    let count = u64::try_from(write_rows.len()).map_err(|_| {
                        DataFusionError::Execution("UPDATE row count overflow".to_string())
                    })?;

                    if count > 0 {
                        write_ctx
                            .stage_write(TransactionWrite::Rows {
                                mode: TransactionWriteMode::Replace,
                                rows: write_rows,
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

fn lix_state_stageable_write_rows_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    action: &str,
) -> Result<Vec<TransactionWriteRow>> {
    let mut rows = lix_state_write_rows_from_batch(batch, branch_binding, action)?;
    for row in &mut rows {
        row.created_at = None;
        row.updated_at = None;
        row.change_id = None;
        row.commit_id = None;
    }
    Ok(rows)
}

fn lix_state_update_write_rows_from_batch(
    batch: &RecordBatch,
    assignments: &[(String, Arc<dyn PhysicalExpr>)],
    branch_binding: Option<&str>,
) -> Result<Vec<TransactionWriteRow>> {
    let assignment_values = UpdateAssignmentValues::evaluate(batch, assignments)?;
    (0..batch.num_rows())
        .map(|row_index| {
            let global = optional_bool_value(batch, row_index, "global")?.unwrap_or(false);
            let branch_id =
                optional_string_value(batch, row_index, "branch_id")?.unwrap_or_else(|| {
                    if global {
                        GLOBAL_BRANCH_ID.to_string()
                    } else {
                        branch_binding.unwrap_or_default().to_string()
                    }
                });
            if !global && branch_id.is_empty() {
                return Err(DataFusionError::Execution(
                    "UPDATE lix_state_by_branch requires branch_id".to_string(),
                ));
            }

            Ok(TransactionWriteRow {
                entity_pk: Some(
                    EntityPk::from_json_array_text(&required_string_value(
                        batch,
                        row_index,
                        "entity_pk",
                    )?)
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "lix_state UPDATE has invalid entity_pk: {error}"
                        ))
                    })?,
                ),
                schema_key: required_string_value(batch, row_index, "schema_key")?,
                file_id: optional_string_value(batch, row_index, "file_id")?,
                snapshot: update_optional_json_value(
                    batch,
                    &assignment_values,
                    row_index,
                    "snapshot_content",
                )?,
                metadata: update_optional_metadata_value(
                    batch,
                    &assignment_values,
                    row_index,
                    "metadata",
                    "lix_state",
                )?,
                origin: None,
                created_at: None,
                updated_at: None,
                global,
                change_id: None,
                commit_id: None,
                untracked: optional_bool_value(batch, row_index, "untracked")?.unwrap_or(false),
                branch_id,
            })
        })
        .collect()
}

fn lix_state_deletable_write_rows_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
) -> Result<Vec<TransactionWriteRow>> {
    let mut rows =
        lix_state_stageable_write_rows_from_batch(batch, branch_binding, "DELETE FROM lix_state")?;
    for row in &mut rows {
        row.snapshot = None;
    }
    Ok(rows)
}

fn update_optional_string_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
) -> Result<Option<String>> {
    match assignment_values.assigned_or_existing_cell(batch, row_index, column_name)? {
        InsertCell::Omitted | InsertCell::Provided(SqlCell::Null) => Ok(None),
        InsertCell::Provided(SqlCell::Value(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)),
        )) => Ok(Some(value)),
        InsertCell::Provided(SqlCell::Value(other)) => Err(DataFusionError::Execution(format!(
            "UPDATE lix_state expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
}

fn update_optional_metadata_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<TransactionJson>> {
    update_optional_string_value(batch, assignment_values, row_index, column_name)?
        .map(|value| {
            let metadata =
                parse_row_metadata_value(&value, context).map_err(lix_error_to_datafusion_error)?;
            TransactionJson::from_value(metadata, &format!("{context} metadata"))
                .map_err(lix_error_to_datafusion_error)
        })
        .transpose()
}

fn update_optional_json_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
) -> Result<Option<TransactionJson>> {
    update_optional_string_value(batch, assignment_values, row_index, column_name)?
        .map(|value| parse_snapshot_json(&value, column_name))
        .transpose()
}

fn lix_state_write_rows_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    action: &str,
) -> Result<Vec<TransactionWriteRow>> {
    (0..batch.num_rows())
        .map(|row_index| {
            let global = optional_bool_value(batch, row_index, "global")?.unwrap_or(false);
            let branch_id =
                optional_string_value(batch, row_index, "branch_id")?.unwrap_or_else(|| {
                    if global {
                        GLOBAL_BRANCH_ID.to_string()
                    } else {
                        branch_binding.unwrap_or_default().to_string()
                    }
                });
            if !global && branch_id.is_empty() {
                return Err(DataFusionError::Execution(format!(
                    "{action} requires branch_id"
                )));
            }

            Ok(TransactionWriteRow {
                entity_pk: Some(
                    EntityPk::from_json_array_text(&required_string_value(
                        batch,
                        row_index,
                        "entity_pk",
                    )?)
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "lix_state INSERT has invalid entity_pk: {error}"
                        ))
                    })?,
                ),
                schema_key: required_string_value(batch, row_index, "schema_key")?,
                file_id: optional_string_value(batch, row_index, "file_id")?,
                snapshot: optional_json_value(batch, row_index, "snapshot_content")?,
                metadata: optional_metadata_value(batch, row_index, "metadata", "lix_state")?,
                origin: None,
                created_at: optional_string_value(batch, row_index, "created_at")?,
                updated_at: optional_string_value(batch, row_index, "updated_at")?,
                global,
                change_id: optional_string_value(batch, row_index, "change_id")?,
                commit_id: optional_string_value(batch, row_index, "commit_id")?,
                untracked: optional_bool_value(batch, row_index, "untracked")?.unwrap_or(false),
                branch_id,
            })
        })
        .collect()
}

fn validate_lix_state_update_assignments(
    schema: &SchemaRef,
    assignments: &[(String, Expr)],
) -> Result<()> {
    for (column_name, _) in assignments {
        schema.field_with_name(column_name).map_err(|_| {
            DataFusionError::Plan(format!(
                "UPDATE lix_state failed: column '{column_name}' does not exist"
            ))
        })?;
        if !matches!(
            column_name.as_str(),
            "snapshot_content" | "metadata" | "global" | "untracked"
        ) {
            return Err(DataFusionError::Execution(format!(
                "UPDATE lix_state cannot stage read-only column '{column_name}'"
            )));
        }
    }
    Ok(())
}

fn required_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<String> {
    optional_string_value(batch, row_index, column_name)?.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "INSERT into lix_state requires non-null text column '{column_name}'"
        ))
    })
}

fn optional_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<String>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        None
        | Some(
            ScalarValue::Null
            | ScalarValue::Utf8(None)
            | ScalarValue::Utf8View(None)
            | ScalarValue::LargeUtf8(None),
        ) => Ok(None),
        Some(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)),
        ) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Execution(format!(
            "INSERT into lix_state expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
}

fn optional_metadata_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<TransactionJson>> {
    optional_string_value(batch, row_index, column_name)?
        .map(|value| {
            let metadata =
                parse_row_metadata_value(&value, context).map_err(lix_error_to_datafusion_error)?;
            TransactionJson::from_value(metadata, &format!("{context} metadata"))
                .map_err(lix_error_to_datafusion_error)
        })
        .transpose()
}

fn optional_json_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<TransactionJson>> {
    optional_string_value(batch, row_index, column_name)?
        .map(|value| parse_snapshot_json(&value, column_name))
        .transpose()
}

fn parse_snapshot_json(value: &str, column_name: &str) -> Result<TransactionJson> {
    let parsed = serde_json::from_str::<JsonValue>(value).map_err(|error| {
        DataFusionError::Execution(format!(
            "lix_state expected valid JSON in column '{column_name}': {error}"
        ))
    })?;
    TransactionJson::from_value(parsed, &format!("lix_state {column_name}"))
        .map_err(lix_error_to_datafusion_error)
}

fn optional_bool_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<bool>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        Some(ScalarValue::Boolean(Some(value))) => Ok(Some(value)),
        None | Some(ScalarValue::Null | ScalarValue::Boolean(None)) => Ok(None),
        Some(other) => Err(DataFusionError::Execution(format!(
            "INSERT into lix_state expected boolean column '{column_name}', got {other:?}"
        ))),
    }
}

fn optional_scalar_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<ScalarValue>> {
    let schema = batch.schema();
    let column_index = match schema.index_of(column_name) {
        Ok(column_index) => column_index,
        Err(_) => return Ok(None),
    };

    if row_index >= batch.num_rows() {
        return Err(DataFusionError::Execution(format!(
            "row index {row_index} out of bounds for lix_state batch with {} rows",
            batch.num_rows()
        )));
    }

    ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)
        .map(Some)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "failed to decode lix_state column '{column_name}' at row {row_index}: {error}"
            ))
        })
}

pub(super) fn lix_state_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        json_field("entity_pk", false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        json_field("snapshot_content", true),
        json_field("metadata", true),
        Field::new("created_at", DataType::Utf8, true),
        Field::new("updated_at", DataType::Utf8, true),
        Field::new("global", DataType::Boolean, true),
        Field::new("change_id", DataType::Utf8, true),
        Field::new("commit_id", DataType::Utf8, true),
        Field::new("untracked", DataType::Boolean, true),
    ]))
}

pub(super) fn lix_state_by_branch_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        json_field("entity_pk", false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        json_field("snapshot_content", true),
        json_field("metadata", true),
        Field::new("created_at", DataType::Utf8, true),
        Field::new("updated_at", DataType::Utf8, true),
        Field::new("global", DataType::Boolean, true),
        Field::new("change_id", DataType::Utf8, true),
        Field::new("commit_id", DataType::Utf8, true),
        Field::new("untracked", DataType::Boolean, true),
        Field::new("branch_id", DataType::Utf8, false),
    ]))
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixStateByBranchRoute {
    schema_keys: Option<BTreeSet<String>>,
    branch_ids: Option<BTreeSet<String>>,
    entity_pks: Option<BTreeSet<String>>,
    file_id: Option<NullableKeyFilter<String>>,
    contradictory: bool,
}

impl LixStateByBranchRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            let Some(predicates) = parse_lix_state_filters(filter) else {
                continue;
            };
            for predicate in predicates {
                match predicate {
                    LixStateFilterPredicate::SchemaKeys(values) => {
                        merge_string_route_slot(
                            &mut route.schema_keys,
                            values,
                            &mut route.contradictory,
                        );
                    }
                    LixStateFilterPredicate::BranchIds(values) => {
                        merge_string_route_slot(
                            &mut route.branch_ids,
                            values,
                            &mut route.contradictory,
                        );
                    }
                    LixStateFilterPredicate::EntityPks(values) => {
                        merge_string_route_slot(
                            &mut route.entity_pks,
                            values,
                            &mut route.contradictory,
                        );
                    }
                    LixStateFilterPredicate::FileId(filter) => {
                        merge_nullable_key_route_slot(
                            &mut route.file_id,
                            filter,
                            &mut route.contradictory,
                        );
                    }
                }
            }
        }
        route
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LixStateFilterPredicate {
    SchemaKeys(BTreeSet<String>),
    BranchIds(BTreeSet<String>),
    EntityPks(BTreeSet<String>),
    FileId(NullableKeyFilter<String>),
}

fn lix_state_scan_request(
    schema: &SchemaRef,
    branch_binding: Option<&str>,
    projection: Option<&Vec<usize>>,
    route: &LixStateByBranchRoute,
    limit: Option<usize>,
) -> LiveStateScanRequest {
    let projection = LiveStateProjection {
        columns: projection_column_names(schema, projection),
    };
    let mut filter = LiveStateFilter {
        schema_keys: route
            .schema_keys
            .as_ref()
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default(),
        entity_pks: route
            .entity_pks
            .as_ref()
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| EntityPk::from_json_array_text(value).ok())
                    .collect()
            })
            .unwrap_or_default(),
        branch_ids: branch_binding
            .map(|value| vec![value.to_string()])
            .or_else(|| {
                route
                    .branch_ids
                    .as_ref()
                    .map(|values| values.iter().cloned().collect())
            })
            .unwrap_or_default(),
        ..LiveStateFilter::default()
    };
    if let Some(file_id) = route.file_id.clone() {
        filter.file_ids.push(file_id);
    }

    if route.contradictory {
        filter.rows = LiveStateRowFilter::None;
    }

    LiveStateScanRequest {
        filter,
        projection,
        limit,
    }
}

fn projection_column_names(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Vec<String> {
    projection
        .map(|indices| {
            indices
                .iter()
                .filter_map(|index| schema.fields().get(*index))
                .map(|field| field.name().clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn merge_string_route_slot(
    slot: &mut Option<BTreeSet<String>>,
    values: BTreeSet<String>,
    contradictory: &mut bool,
) {
    if values.is_empty() {
        return;
    }

    match slot {
        Some(existing) => {
            existing.retain(|value| values.contains(value));
            if existing.is_empty() {
                *contradictory = true;
            }
        }
        None => *slot = Some(values),
    }
}

fn merge_nullable_key_route_slot(
    slot: &mut Option<NullableKeyFilter<String>>,
    value: NullableKeyFilter<String>,
    contradictory: &mut bool,
) {
    match slot {
        Some(existing) if *existing != value => *contradictory = true,
        Some(_) => {}
        None => *slot = Some(value),
    }
}

fn parse_lix_state_filter(expr: &Expr) -> Option<LixStateFilterPredicate> {
    parse_lix_state_filters(expr)?.into_iter().next()
}

fn parse_lix_state_filters(expr: &Expr) -> Option<Vec<LixStateFilterPredicate>> {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
            let mut predicates = parse_lix_state_filters(&binary_expr.left)?;
            predicates.extend(parse_lix_state_filters(&binary_expr.right)?);
            Some(predicates)
        }
        Expr::BinaryExpr(binary_expr) => {
            parse_lix_state_binary_filter(binary_expr).map(|predicate| vec![predicate])
        }
        Expr::InList(in_list) => {
            parse_lix_state_in_list_filter(in_list).map(|predicate| vec![predicate])
        }
        Expr::IsNull(expr) => parse_lix_state_null_filter(expr).map(|predicate| vec![predicate]),
        _ => None,
    }
}

fn parse_lix_state_binary_filter(binary_expr: &BinaryExpr) -> Option<LixStateFilterPredicate> {
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_lix_state_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| parse_lix_state_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

fn parse_lix_state_in_list_filter(in_list: &InList) -> Option<LixStateFilterPredicate> {
    if in_list.negated {
        return None;
    }
    let Expr::Column(column) = in_list.expr.as_ref() else {
        return None;
    };

    let values = in_list
        .list
        .iter()
        .map(string_expr_literal)
        .collect::<Option<Vec<_>>>()?;
    if values.is_empty() {
        return None;
    }

    let values = values.into_iter().collect::<BTreeSet<_>>();
    match column.name.as_str() {
        "schema_key" => Some(LixStateFilterPredicate::SchemaKeys(values)),
        "branch_id" => Some(LixStateFilterPredicate::BranchIds(values)),
        "entity_pk" => canonical_entity_pk_values(values).map(LixStateFilterPredicate::EntityPks),
        _ => None,
    }
}

fn parse_lix_state_null_filter(expr: &Expr) -> Option<LixStateFilterPredicate> {
    let Expr::Column(column) = expr else {
        return None;
    };

    match column.name.as_str() {
        "file_id" => Some(LixStateFilterPredicate::FileId(NullableKeyFilter::Null)),
        _ => None,
    }
}

fn parse_lix_state_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<LixStateFilterPredicate> {
    let Expr::Column(column) = column_expr else {
        return None;
    };

    match column.name.as_str() {
        "schema_key" => string_expr_literal(literal_expr)
            .map(|value| LixStateFilterPredicate::SchemaKeys(BTreeSet::from([value]))),
        "branch_id" => string_expr_literal(literal_expr)
            .map(|value| LixStateFilterPredicate::BranchIds(BTreeSet::from([value]))),
        "entity_pk" => string_expr_literal(literal_expr)
            .and_then(|value| canonical_entity_pk_value(&value))
            .map(|value| LixStateFilterPredicate::EntityPks(BTreeSet::from([value]))),
        "file_id" => nullable_key_literal(literal_expr).map(LixStateFilterPredicate::FileId),
        _ => None,
    }
}

fn canonical_entity_pk_values(values: BTreeSet<String>) -> Option<BTreeSet<String>> {
    values
        .into_iter()
        .map(|value| canonical_entity_pk_value(&value))
        .collect()
}

fn canonical_entity_pk_value(value: &str) -> Option<String> {
    EntityPk::from_json_array_text(value)
        .ok()?
        .as_json_array_text()
        .ok()
}

fn nullable_key_literal(expr: &Expr) -> Option<NullableKeyFilter<String>> {
    if is_null_literal(expr) {
        return Some(NullableKeyFilter::Null);
    }
    string_expr_literal(expr).map(NullableKeyFilter::Value)
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Null, _))
}

/// Map [`ColumnTableError`] from [`LIVE_STATE_COLS`] builds onto the exact
/// error messages the hand-written `lix_state_record_batch` produced.
fn lix_state_batch_error(error: ColumnTableError) -> LixError {
    match error {
        ColumnTableError::UnsupportedColumn(other) => LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 does not support lix_state column '{other}'"),
        ),
        ColumnTableError::Arrow(error) => LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build lix_state_by_branch batch: {error}"),
        ),
        ColumnTableError::ArrowZeroColumn(error) => LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build zero-column lix_state batch: {error}"),
        ),
        ColumnTableError::Row(error) => error,
    }
}

#[cfg(test)]
#[expect(trivial_casts)]
mod tests {
    use super::super::spec::{SpecDmlExec, SpecTableProvider, TableSpec};
    use super::{
        LixStateByBranchRoute, LixStateFilterPredicate, LixStateSpec, lix_state_scan_request,
        lix_state_schema, lix_state_write_rows_from_batch, parse_lix_state_filter,
        register_lix_state_active_write_provider, register_lix_state_by_branch_write_provider,
    };
    use crate::binary_cas::BlobDataReader;
    use crate::branch::{BranchHead, BranchRefReader};
    use crate::changelog::{ChangeId, CommitId};
    use crate::functions::FunctionProviderHandle;
    use crate::sql2::dml::InsertExec;
    use crate::sql2::{
        SqlWriteContext, SqlWriteExecutionContext, WriteAccess, WriteContextBranchRefReader,
        WriteContextLiveStateReader,
    };
    use crate::transaction::types::{
        TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteOutcome,
        TransactionWriteRow,
    };
    use crate::{LixError, NullableKeyFilter};
    use crate::{
        entity_pk::EntityPk,
        live_state::{
            LiveStateReader, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
        },
    };
    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray, UInt64Array};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::TableProvider;
    use datafusion::common::{Column, DataFusionError};
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    };
    use datafusion::prelude::SessionContext;
    use datafusion::scalar::ScalarValue;
    use futures_util::stream;
    use serde_json::json;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    struct EmptyLiveStateReader;
    struct EmptyBranchRefReader;
    struct DummyBlobReader;

    #[derive(Default)]
    struct DummyWriteContext {
        rows: Vec<MaterializedLiveStateRow>,
    }

    #[derive(Default)]
    struct CapturingWriteContext {
        rows: Vec<MaterializedLiveStateRow>,
        writes: Vec<TransactionWrite>,
    }

    struct SingleBatchExec {
        batch: RecordBatch,
        properties: Arc<PlanProperties>,
    }

    impl std::fmt::Debug for SingleBatchExec {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SingleBatchExec").finish()
        }
    }

    impl SingleBatchExec {
        fn new(batch: RecordBatch) -> Self {
            let properties = PlanProperties::new(
                EquivalenceProperties::new(batch.schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self {
                batch,
                properties: Arc::new(properties),
            }
        }
    }

    impl DisplayAs for SingleBatchExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "SingleBatchExec")
        }
    }

    impl ExecutionPlan for SingleBatchExec {
        fn name(&self) -> &str {
            "SingleBatchExec"
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            Vec::new()
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            if !children.is_empty() {
                return Err(DataFusionError::Execution(
                    "SingleBatchExec does not accept children".to_string(),
                ));
            }
            Ok(self)
        }

        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            if partition != 0 {
                return Err(DataFusionError::Execution(format!(
                    "SingleBatchExec only exposes one partition, got {partition}"
                )));
            }

            let batch = self.batch.clone();
            let schema = batch.schema();
            let stream = stream::iter(vec![Ok(batch)]);
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }
    }

    #[async_trait]
    impl LiveStateReader for EmptyLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(vec![])
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl BranchRefReader for EmptyBranchRefReader {
        async fn load_head(&self, _branch_id: &str) -> Result<Option<BranchHead>, LixError> {
            Ok(None)
        }

        async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
            Ok(Vec::new())
        }
    }

    fn empty_branch_ref() -> Arc<dyn BranchRefReader> {
        Arc::new(EmptyBranchRefReader)
    }

    fn test_functions() -> FunctionProviderHandle {
        FunctionProviderHandle::system()
    }

    fn active_read_provider(active_branch_id: &str) -> SpecTableProvider {
        SpecTableProvider::new(
            Arc::new(LixStateSpec::active_branch(
                active_branch_id,
                Arc::new(EmptyLiveStateReader),
                empty_branch_ref(),
            )),
            WriteAccess::read_only(),
        )
    }

    fn active_write_spec(write_ctx: &SqlWriteContext) -> LixStateSpec {
        LixStateSpec::active_branch(
            write_ctx.active_branch_id(),
            Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
            Arc::new(WriteContextBranchRefReader::new(write_ctx.clone())),
        )
    }

    fn active_write_provider(write_ctx: SqlWriteContext) -> SpecTableProvider {
        SpecTableProvider::new(
            Arc::new(active_write_spec(&write_ctx)),
            WriteAccess::write(write_ctx),
        )
    }

    #[async_trait]
    impl BlobDataReader for DummyBlobReader {
        async fn load_bytes_many(
            &self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            Ok(crate::binary_cas::BlobBytesBatch::new(vec![
                None;
                hashes.len()
            ]))
        }
    }

    #[async_trait]
    impl SqlWriteExecutionContext for DummyWriteContext {
        fn active_branch_id(&self) -> &str {
            "branch-a"
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn list_visible_schemas(&self) -> Result<Vec<serde_json::Value>, LixError> {
            Ok(Vec::new())
        }

        async fn load_bytes_many(
            &mut self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            DummyBlobReader.load_bytes_many(hashes).await
        }

        async fn scan_live_state(
            &mut self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_branch_head(
            &mut self,
            branch_id: &str,
        ) -> Result<Option<CommitId>, LixError> {
            if branch_id == "ghost-branch" {
                return Ok(None);
            }
            Ok(Some(CommitId::for_test_label(&format!(
                "commit-{branch_id}"
            ))))
        }

        async fn stage_write(
            &mut self,
            _write: TransactionWrite,
        ) -> Result<TransactionWriteOutcome, LixError> {
            Ok(TransactionWriteOutcome { count: 0 })
        }
    }

    #[async_trait]
    impl SqlWriteExecutionContext for CapturingWriteContext {
        fn active_branch_id(&self) -> &str {
            "branch-a"
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn list_visible_schemas(&self) -> Result<Vec<serde_json::Value>, LixError> {
            Ok(Vec::new())
        }

        async fn load_bytes_many(
            &mut self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            DummyBlobReader.load_bytes_many(hashes).await
        }

        async fn scan_live_state(
            &mut self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_branch_head(
            &mut self,
            branch_id: &str,
        ) -> Result<Option<CommitId>, LixError> {
            if branch_id == "ghost-branch" {
                return Ok(None);
            }
            Ok(Some(CommitId::for_test_label(&format!(
                "commit-{branch_id}"
            ))))
        }

        async fn stage_write(
            &mut self,
            write: TransactionWrite,
        ) -> Result<TransactionWriteOutcome, LixError> {
            self.writes.push(write);
            Ok(TransactionWriteOutcome { count: 0 })
        }
    }

    fn col(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }

    fn str_lit(value: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), None)
    }

    fn json_lit(value: &str) -> Expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(value.to_string())),
            Some(datafusion::common::metadata::FieldMetadata::new(
                std::collections::BTreeMap::from([(
                    crate::sql2::result_metadata::LIX_VALUE_TYPE_METADATA_KEY.to_string(),
                    crate::sql2::result_metadata::LIX_VALUE_TYPE_JSON.to_string(),
                )]),
            )),
        )
    }

    fn string_column(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values)) as ArrayRef
    }

    fn one_row_lix_state_batch(global: bool) -> RecordBatch {
        RecordBatch::try_new(
            lix_state_schema(),
            vec![
                string_column(vec![Some("[\"entity-1\"]")]),
                string_column(vec![Some("lix_key_value")]),
                string_column(vec![None]),
                string_column(vec![Some("{\"key\":\"hello\",\"value\":\"world\"}")]),
                string_column(vec![Some("{\"source\":\"test\"}")]),
                string_column(vec![Some("2026-04-23T00:00:00Z")]),
                string_column(vec![Some("2026-04-23T01:00:00Z")]),
                Arc::new(BooleanArray::from(vec![global])) as ArrayRef,
                string_column(vec![Some("change-a")]),
                string_column(vec![None]),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ],
        )
        .expect("valid lix_state batch")
    }

    fn one_row_stageable_lix_state_batch() -> RecordBatch {
        RecordBatch::try_new(
            lix_state_schema(),
            vec![
                string_column(vec![Some("[\"entity-1\"]")]),
                string_column(vec![Some("lix_key_value")]),
                string_column(vec![None]),
                string_column(vec![Some("{\"key\":\"hello\",\"value\":\"world\"}")]),
                string_column(vec![None]),
                string_column(vec![None]),
                string_column(vec![None]),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
                string_column(vec![None]),
                string_column(vec![None]),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ],
        )
        .expect("valid stageable lix_state batch")
    }

    fn live_row(entity_pk: &str, metadata: Option<&str>) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: EntityPk::single(entity_pk),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some("{\"key\":\"hello\",\"value\":\"world\"}".to_string()),
            metadata: metadata.map(str::to_string),
            deleted: false,
            branch_id: "branch-a".to_string(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{entity_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{entity_pk}"))),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    #[test]
    fn parses_eq_filter_for_schema_key() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("schema_key")),
            Operator::Eq,
            Box::new(str_lit("profile")),
        ));

        assert_eq!(
            parse_lix_state_filter(&expr),
            Some(LixStateFilterPredicate::SchemaKeys(BTreeSet::from([
                "profile".to_string(),
            ])))
        );
    }

    #[test]
    fn parses_in_list_filter_for_branch_id() {
        let expr = Expr::InList(InList::new(
            Box::new(col("branch_id")),
            vec![str_lit("a"), str_lit("b")],
            false,
        ));

        assert_eq!(
            parse_lix_state_filter(&expr),
            Some(LixStateFilterPredicate::BranchIds(BTreeSet::from([
                "a".to_string(),
                "b".to_string(),
            ])))
        );
    }

    #[test]
    fn builds_scan_request_from_route_and_projection() {
        let schema = super::lix_state_by_branch_schema();
        let route = LixStateByBranchRoute::from_filters(&[
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("schema_key")),
                Operator::Eq,
                Box::new(str_lit("profile")),
            )),
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("branch_id")),
                Operator::Eq,
                Box::new(str_lit("v1")),
            )),
            Expr::IsNull(Box::new(col("file_id"))),
        ]);

        let request =
            lix_state_scan_request(&schema, None, Some(&vec![0, 1, 11]), &route, Some(10));

        assert_eq!(request.filter.schema_keys, vec!["profile".to_string()]);
        assert_eq!(request.filter.branch_ids, vec!["v1".to_string()]);
        assert_eq!(request.filter.file_ids, vec![NullableKeyFilter::Null]);
        assert_eq!(
            request.projection.columns,
            vec![
                "entity_pk".to_string(),
                "schema_key".to_string(),
                "branch_id".to_string()
            ]
        );
        assert_eq!(request.limit, Some(10));
    }

    #[test]
    fn builds_route_from_and_filter_tree() {
        let route = LixStateByBranchRoute::from_filters(&[Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("entity_pk")),
                Operator::Eq,
                Box::new(str_lit("[\"entity-a\"]")),
            ))),
            Operator::And,
            Box::new(Expr::InList(InList::new(
                Box::new(col("branch_id")),
                vec![str_lit("branch-a"), str_lit("global")],
                false,
            ))),
        ))]);

        assert_eq!(
            route.entity_pks,
            Some(BTreeSet::from(["[\"entity-a\"]".to_string()]))
        );
        assert_eq!(
            route.branch_ids,
            Some(BTreeSet::from([
                "global".to_string(),
                "branch-a".to_string()
            ]))
        );
    }

    #[test]
    fn contradictory_filters_turn_into_zero_limit_request() {
        let schema = super::lix_state_by_branch_schema();
        let route = LixStateByBranchRoute::from_filters(&[
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("schema_key")),
                Operator::Eq,
                Box::new(str_lit("a")),
            )),
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("schema_key")),
                Operator::Eq,
                Box::new(str_lit("b")),
            )),
        ]);

        let request = lix_state_scan_request(&schema, None, None, &route, None);

        assert_eq!(
            request.filter.rows,
            crate::live_state::LiveStateRowFilter::None
        );
        assert_eq!(request.limit, None);
        assert!(request.filter.schema_keys.is_empty());
    }

    #[tokio::test]
    async fn active_provider_contradictory_filters_still_validate_active_head() {
        let provider = active_read_provider("missing-branch");
        let session = SessionContext::new();
        let filters = vec![
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("schema_key")),
                Operator::Eq,
                Box::new(str_lit("a")),
            )),
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("schema_key")),
                Operator::Eq,
                Box::new(str_lit("b")),
            )),
        ];

        let error = provider
            .scan(&session.state(), None, &filters, None)
            .await
            .expect_err("missing active branch should be checked before zero-row scan");
        let error = crate::sql2::error::datafusion_error_to_lix_error(error);

        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
        assert!(
            error
                .message
                .contains("branch 'missing-branch' was not found")
        );
    }

    #[test]
    fn active_branch_view_pins_branch_filter() {
        let schema = lix_state_schema();
        let route = LixStateByBranchRoute::from_filters(&[Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("schema_key")),
            Operator::Eq,
            Box::new(str_lit("profile")),
        ))]);

        let request = lix_state_scan_request(&schema, Some("branch-a"), None, &route, None);

        assert_eq!(request.filter.schema_keys, vec!["profile".to_string()]);
        assert_eq!(request.filter.branch_ids, vec!["branch-a".to_string()]);
    }

    #[tokio::test]
    async fn registers_active_lix_state_with_write_context_only() {
        let session = SessionContext::new();
        let mut write_context = DummyWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);

        register_lix_state_active_write_provider(&session, "lix_state", write_ctx.clone())
            .await
            .expect("lix_state provider should register");
        register_lix_state_by_branch_write_provider(&session, "lix_state_by_branch", write_ctx)
            .await
            .expect("lix_state_by_branch provider should register");

        let lix_state = session
            .table_provider("lix_state")
            .await
            .expect("lix_state provider should exist");
        let lix_state = lix_state
            .as_any()
            .downcast_ref::<SpecTableProvider>()
            .expect("lix_state should be a SpecTableProvider");
        assert!(lix_state.is_write());

        let by_branch = session
            .table_provider("lix_state_by_branch")
            .await
            .expect("lix_state_by_branch provider should exist");
        let by_branch = by_branch
            .as_any()
            .downcast_ref::<SpecTableProvider>()
            .expect("lix_state_by_branch should be a SpecTableProvider");
        assert!(by_branch.is_write());
    }

    #[tokio::test]
    async fn insert_into_requires_write_transaction() {
        let session = SessionContext::new();
        let provider = active_read_provider("branch-a");
        let input = Arc::new(EmptyExec::new(provider.schema())) as Arc<dyn ExecutionPlan>;

        let error = provider
            .insert_into(&session.state(), input, InsertOp::Append)
            .await
            .expect_err("insert without a write context should fail");

        assert!(
            error.to_string().contains("requires a write transaction"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn update_requires_write_transaction() {
        let session = SessionContext::new();
        let provider = active_read_provider("branch-a");

        let error = provider
            .update(
                &session.state(),
                vec![("metadata".to_string(), str_lit("{\"source\":\"update\"}"))],
                vec![],
            )
            .await
            .expect_err("update without a write context should fail");

        assert!(
            error.to_string().contains("requires a write transaction"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn delete_requires_write_transaction() {
        let session = SessionContext::new();
        let provider = active_read_provider("branch-a");

        let error = provider
            .delete_from(&session.state(), vec![])
            .await
            .expect_err("delete without a write context should fail");

        assert!(
            error.to_string().contains("requires a write transaction"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn delete_returns_lix_state_delete_exec_with_write_ctx() {
        let session = SessionContext::new();
        let mut write_context = DummyWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let provider = active_write_provider(write_ctx);

        let plan = provider
            .delete_from(&session.state(), vec![])
            .await
            .expect("delete should produce a write plan");

        assert!(plan.as_any().is::<SpecDmlExec>());
    }

    #[tokio::test]
    async fn update_rejects_read_only_lix_state_columns() {
        let session = SessionContext::new();
        let mut write_context = DummyWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let provider = active_write_provider(write_ctx);

        let error = provider
            .update(
                &session.state(),
                vec![("entity_pk".to_string(), str_lit("entity-2"))],
                vec![],
            )
            .await
            .expect_err("updating a read-only field should fail");

        assert!(
            error.to_string().contains("read-only column 'entity_pk'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn update_returns_lix_state_update_exec_with_write_ctx() {
        let session = SessionContext::new();
        let mut write_context = DummyWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let provider = active_write_provider(write_ctx);

        let plan = provider
            .update(
                &session.state(),
                vec![("metadata".to_string(), str_lit("{\"source\":\"update\"}"))],
                vec![],
            )
            .await
            .expect("update should produce a write plan");

        assert!(plan.as_any().is::<SpecDmlExec>());
    }

    #[tokio::test]
    async fn insert_into_returns_data_sink_exec_with_write_ctx() {
        let session = SessionContext::new();
        let mut write_context = DummyWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let provider = active_write_provider(write_ctx);
        let input = Arc::new(EmptyExec::new(provider.schema())) as Arc<dyn ExecutionPlan>;

        let plan = provider
            .insert_into(&session.state(), input, InsertOp::Append)
            .await
            .expect("insert should produce a write plan");

        assert!(plan.as_any().is::<InsertExec>());
    }

    #[test]
    fn decodes_lix_state_batch_into_write_rows() {
        let rows = lix_state_write_rows_from_batch(
            &one_row_lix_state_batch(false),
            Some("branch-a"),
            "INSERT into lix_state",
        )
        .expect("batch should decode");

        assert_eq!(
            rows,
            vec![TransactionWriteRow {
                entity_pk: Some(EntityPk::single("entity-1")),
                schema_key: "lix_key_value".to_string(),
                file_id: None,
                snapshot: Some(TransactionJson::from_value_for_test(
                    json!({"key":"hello","value":"world"})
                )),
                metadata: Some(TransactionJson::from_value_for_test(
                    json!({"source": "test"})
                )),
                origin: None,
                created_at: Some("2026-04-23T00:00:00Z".to_string()),
                updated_at: Some("2026-04-23T01:00:00Z".to_string()),
                global: false,
                change_id: Some("change-a".to_string()),
                commit_id: None,
                untracked: false,
                branch_id: "branch-a".to_string(),
            }]
        );
    }

    #[test]
    fn decodes_global_lix_state_batch_into_global_branch() {
        let rows = lix_state_write_rows_from_batch(
            &one_row_lix_state_batch(true),
            Some("branch-a"),
            "INSERT into lix_state",
        )
        .expect("batch should decode");

        assert_eq!(rows[0].branch_id, "global");
        assert!(rows[0].global);
    }

    #[tokio::test]
    async fn insert_sink_stages_decoded_lix_state_rows() {
        let mut write_context = CapturingWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let spec = active_write_spec(&write_ctx);
        let batch = one_row_lix_state_batch(false);
        let count = spec
            .stage_insert(&write_ctx, vec![batch])
            .await
            .expect("sink should stage write");

        assert_eq!(count, 1);
        assert_eq!(
            write_context.writes.as_slice(),
            &[TransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: vec![TransactionWriteRow {
                    entity_pk: Some(EntityPk::single("entity-1")),
                    schema_key: "lix_key_value".to_string(),
                    file_id: None,
                    snapshot: Some(TransactionJson::from_value_for_test(
                        json!({"key":"hello","value":"world"})
                    )),
                    metadata: Some(TransactionJson::from_value_for_test(
                        json!({"source": "test"})
                    )),
                    origin: None,
                    created_at: Some("2026-04-23T00:00:00Z".to_string()),
                    updated_at: Some("2026-04-23T01:00:00Z".to_string()),
                    global: false,
                    change_id: Some("change-a".to_string()),
                    commit_id: None,
                    untracked: false,
                    branch_id: "branch-a".to_string(),
                }]
            }]
        );
    }

    #[tokio::test]
    async fn insert_plan_returns_datafusion_count_uint64() {
        let session = SessionContext::new();
        let mut write_context = CapturingWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let provider = active_write_provider(write_ctx);
        let input = Arc::new(SingleBatchExec::new(one_row_stageable_lix_state_batch()))
            as Arc<dyn ExecutionPlan>;

        let plan = provider
            .insert_into(&session.state(), input, InsertOp::Append)
            .await
            .expect("insert should produce a write plan");
        let batches = datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default()))
            .await
            .expect("insert write plan should execute");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "count");
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::UInt64);
        assert!(!batches[0].schema().field(0).is_nullable());

        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("count should be UInt64");
        assert_eq!(count.value(0), 1);
        assert_eq!(write_context.writes.len(), 1);
    }

    #[tokio::test]
    async fn update_plan_evaluates_filters_assignments_and_stages_rows() {
        let session = SessionContext::new();
        let mut write_context = CapturingWriteContext {
            rows: vec![
                live_row("entity-1", Some("{\"source\":\"match\"}")),
                live_row("entity-2", Some("{\"source\":\"skip\"}")),
            ],
            writes: Vec::new(),
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let provider = active_write_provider(write_ctx);

        let plan = provider
            .update(
                &session.state(),
                vec![
                    (
                        "snapshot_content".to_string(),
                        str_lit("{\"key\":\"hello\",\"value\":\"updated\"}"),
                    ),
                    (
                        "metadata".to_string(),
                        str_lit("{\"schema_key\":\"lix_key_value\"}"),
                    ),
                ],
                vec![Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(col("metadata")),
                    Operator::Eq,
                    Box::new(json_lit("{\"source\":\"match\"}")),
                ))],
            )
            .await
            .expect("update should produce a write plan");
        let batches = datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default()))
            .await
            .expect("update write plan should execute");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "count");
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::UInt64);
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("count should be UInt64");
        assert_eq!(count.value(0), 1);

        assert_eq!(
            write_context.writes.as_slice(),
            &[TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![TransactionWriteRow {
                    entity_pk: Some(EntityPk::single("entity-1")),
                    schema_key: "lix_key_value".to_string(),
                    file_id: None,
                    snapshot: Some(TransactionJson::from_value_for_test(
                        json!({"key":"hello","value":"updated"})
                    )),
                    metadata: Some(TransactionJson::from_value_for_test(
                        json!({"schema_key": "lix_key_value"})
                    )),
                    origin: None,
                    created_at: None,
                    updated_at: None,
                    global: false,
                    change_id: None,
                    commit_id: None,
                    untracked: false,
                    branch_id: "branch-a".to_string(),
                }]
            }]
        );
    }

    #[tokio::test]
    async fn delete_plan_with_empty_filters_stages_all_visible_rows() {
        let session = SessionContext::new();
        let mut write_context = CapturingWriteContext {
            rows: vec![
                live_row("entity-1", Some("{\"source\":\"one\"}")),
                live_row("entity-2", Some("{\"source\":\"two\"}")),
            ],
            writes: Vec::new(),
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let provider = active_write_provider(write_ctx);

        let plan = provider
            .delete_from(&session.state(), vec![])
            .await
            .expect("delete should produce a write plan");
        let batches = datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default()))
            .await
            .expect("delete write plan should execute");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "count");
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::UInt64);
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("count should be UInt64");
        assert_eq!(count.value(0), 2);

        assert_eq!(
            write_context.writes.as_slice(),
            &[TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![
                    TransactionWriteRow {
                        entity_pk: Some(EntityPk::single("entity-1")),
                        schema_key: "lix_key_value".to_string(),
                        file_id: None,
                        snapshot: None,
                        metadata: Some(TransactionJson::from_value_for_test(
                            json!({"source": "one"})
                        )),
                        origin: None,
                        created_at: None,
                        updated_at: None,
                        global: false,
                        change_id: None,
                        commit_id: None,
                        untracked: false,
                        branch_id: "branch-a".to_string(),
                    },
                    TransactionWriteRow {
                        entity_pk: Some(EntityPk::single("entity-2")),
                        schema_key: "lix_key_value".to_string(),
                        file_id: None,
                        snapshot: None,
                        metadata: Some(TransactionJson::from_value_for_test(
                            json!({"source": "two"})
                        )),
                        origin: None,
                        created_at: None,
                        updated_at: None,
                        global: false,
                        change_id: None,
                        commit_id: None,
                        untracked: false,
                        branch_id: "branch-a".to_string(),
                    },
                ]
            }]
        );
    }
}
