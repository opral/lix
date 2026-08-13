use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, LargeStringArray, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::logical_expr::type_coercion::binary::BinaryTypeCoercer;
use serde_json::Value as JsonValue;
use tracing::Instrument;

use crate::catalog::{SchemaPlanId, TypedJsonScalarRef};
use crate::changelog::CommitId;
use crate::common::{
    ExecuteStatementMetadata, RequestBlobSpliceProvenance, SharedStr, validate_row_metadata,
};
use crate::entity_pk::EntityPk;
use crate::hot_state::{
    HotStateFilter, HotStateProjection, HotStateRowFilter, HotStateScanRequest,
    MaterializedHotStateBatch, MaterializedHotStateRow, MaterializedHotStateRowRef,
};
use crate::sql2::SqlWriteExecutionContext;
use crate::sql2::bind::expr::{BoundBinaryOperator, BoundCastType, BoundExpr, BoundLiteral};
use crate::sql2::bind::write::{
    BoundAssignment, BoundConflictAction, BoundInsertConflict, BoundInsertValues, BoundWriteInput,
    BoundWriteOp, BoundWriteTarget, EntityWriteSurface, FileWriteSurface,
};
use crate::sql2::catalog::entity_surface::{
    EntitySurfaceColumn, arrow_data_type_for_entity_column_type,
};
use crate::sql2::catalog::{EntityColumnType, EntitySurfaceSpec};
use crate::sql2::plan::LogicalWritePlan;
use crate::sql2::plan::branch_scope::BranchScope;
use crate::sql2::plan::predicate::{BoundPredicate, FilterSet};
use crate::sql2::read_only::reject_read_only_entity_surface;
use crate::sql2::value_contract::{json_bigint_value, json_double_value};
use crate::sql2::write_normalization::LIX_FILE_CONTENT_CAST_HINT;
use crate::transaction_types::{
    CertifiedParameterInsertBatch, CertifiedParameterReplacementBatch,
    CertifiedRawWriteBatchPreparation, CompleteCollectionReplacementProof, PreparedRowFacts,
    RawWriteBatch, RawWriteRowRef, TransactionJson, TransactionWrite, TransactionWriteMode,
    TypedMutationJournalBatch,
};
use crate::plugin::runtime::WasmEntityKey;
use crate::{LixError, NullableKeyFilter, Value, parse_row_metadata_value};
use crate::{PreparedDmlParameterBatch, PreparedDmlValueRef};

use super::SqlWriteResult;

#[cfg(test)]
std::thread_local! {
    static ENTITY_UPDATE_PARAMETER_BATCH_EXECUTIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CERTIFIED_REPLACEMENT_PARAMETER_BATCH_EXECUTIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CERTIFIED_ENTITY_INSERT_BATCH_EXECUTIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_EXECUTIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CERTIFIED_GENERATION_IDENTITY_REPLACEMENTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CERTIFIED_SINGLE_PATH_VALUE_REPLACEMENTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static FAST_BINARY_ARITHMETIC_EVALUATIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn take_entity_update_parameter_batch_executions() -> usize {
    ENTITY_UPDATE_PARAMETER_BATCH_EXECUTIONS.with(|executions| executions.replace(0))
}

#[cfg(test)]
pub(crate) fn take_certified_replacement_parameter_batch_executions() -> usize {
    CERTIFIED_REPLACEMENT_PARAMETER_BATCH_EXECUTIONS.with(|executions| executions.replace(0))
}

#[cfg(test)]
pub(crate) fn take_certified_entity_insert_batch_executions() -> usize {
    CERTIFIED_ENTITY_INSERT_BATCH_EXECUTIONS.with(|executions| executions.replace(0))
}

#[cfg(test)]
pub(crate) fn take_certified_entity_insert_parameter_batch_executions() -> usize {
    CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_EXECUTIONS.with(|executions| executions.replace(0))
}

#[cfg(test)]
pub(crate) fn take_certified_generation_identity_replacements() -> usize {
    CERTIFIED_GENERATION_IDENTITY_REPLACEMENTS.with(|executions| executions.replace(0))
}

#[cfg(test)]
pub(crate) fn take_certified_single_path_value_replacements() -> usize {
    CERTIFIED_SINGLE_PATH_VALUE_REPLACEMENTS.with(|executions| executions.replace(0))
}

/// Counts evaluations that ran inside the fast writer's arithmetic arm.
///
/// The counter sits in `eval_binary_entity_value` itself, i.e. in the new
/// route, not at the router. A statement that is admitted but whose arithmetic
/// never evaluates (zero matched rows) therefore reads as zero.
#[cfg(test)]
pub(crate) fn take_fast_binary_arithmetic_evaluations() -> usize {
    FAST_BINARY_ARITHMETIC_EVALUATIONS.with(|evaluations| evaluations.replace(0))
}

#[cfg(test)]
pub(crate) fn supports_bound_public_write(plan: &LogicalWritePlan) -> bool {
    match &plan.bound.target {
        BoundWriteTarget::Entity(_) => bound_public_write_shape_supported(plan),
        BoundWriteTarget::File(surface) => {
            fast_file_path_write_shape(plan, surface).is_some()
                || fast_file_content_update_shape(plan, surface).is_some()
        }
        _ => false,
    }
}

pub(crate) enum BoundPublicWriteExecution {
    Executed(SqlWriteResult),
    Unsupported,
}

#[derive(Clone, Copy)]
enum EntityInsertParameterBatch<'a> {
    Arrow(&'a RecordBatch),
    Values(&'a [&'a [Value]]),
    Prepared(&'a PreparedDmlParameterBatch),
}

enum CertifiedEntityInsertParameterBatch {
    Typed(CertifiedParameterInsertBatch),
    Raw(RawWriteBatch),
}

const TYPED_CERTIFIED_INSERT_MIN_ROWS: usize = 32 * 1024;

fn use_typed_certified_insert(row_count: usize) -> bool {
    row_count >= TYPED_CERTIFIED_INSERT_MIN_ROWS
}

impl CertifiedEntityInsertParameterBatch {
    fn into_raw(self) -> Result<RawWriteBatch, LixError> {
        match self {
            Self::Typed(rows) => rows.into_raw(),
            Self::Raw(rows) => Ok(rows),
        }
    }
}

#[derive(Clone, Copy)]
enum DirectParameterValue<'a> {
    Null,
    String(&'a str),
    Boolean(bool),
}

impl<'a> EntityInsertParameterBatch<'a> {
    fn num_rows(self) -> usize {
        match self {
            Self::Arrow(batch) => batch.num_rows(),
            Self::Values(rows) => rows.len(),
            Self::Prepared(batch) => batch.row_count(),
        }
    }

    fn num_columns(self) -> usize {
        match self {
            Self::Arrow(batch) => batch.num_columns(),
            Self::Values(rows) => rows.first().map_or(0, |row| row.len()),
            Self::Prepared(batch) => batch.column_count(),
        }
    }

    fn column_matches(self, parameter_index: usize, column_type: EntityColumnType) -> bool {
        match self {
            Self::Arrow(batch) => {
                let Some(array) = batch.columns().get(parameter_index) else {
                    return false;
                };
                if crate::sql2::result_metadata::field_is_json(
                    batch.schema().field(parameter_index),
                ) {
                    return false;
                }
                match column_type {
                    EntityColumnType::String => {
                        array.as_any().is::<StringArray>()
                            || array.as_any().is::<LargeStringArray>()
                    }
                    EntityColumnType::Boolean => array.as_any().is::<BooleanArray>(),
                    _ => false,
                }
            }
            Self::Values(rows) => rows.iter().all(|row| {
                matches!(
                    (column_type, row.get(parameter_index)),
                    (EntityColumnType::String, Some(Value::Text(_) | Value::Null))
                        | (
                            EntityColumnType::Boolean,
                            Some(Value::Boolean(_) | Value::Null)
                        )
                )
            }),
            Self::Prepared(batch) => (0..batch.row_count()).all(|row| {
                matches!(
                    (column_type, batch.value(row, parameter_index)),
                    (
                        EntityColumnType::String,
                        PreparedDmlValueRef::Text(_) | PreparedDmlValueRef::Null
                    ) | (
                        EntityColumnType::Boolean,
                        PreparedDmlValueRef::Boolean(_) | PreparedDmlValueRef::Null
                    )
                )
            }),
        }
    }

    fn value(self, parameter_index: usize, row_index: usize) -> DirectParameterValue<'a> {
        match self {
            Self::Arrow(batch) => {
                let array = &batch.columns()[parameter_index];
                if array.is_null(row_index) {
                    return DirectParameterValue::Null;
                }
                if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
                    DirectParameterValue::String(array.value(row_index))
                } else if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
                    DirectParameterValue::String(array.value(row_index))
                } else if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
                    DirectParameterValue::Boolean(array.value(row_index))
                } else {
                    unreachable!("direct parameter column type was certified")
                }
            }
            Self::Values(rows) => match &rows[row_index][parameter_index] {
                Value::Null => DirectParameterValue::Null,
                Value::Text(value) => DirectParameterValue::String(value),
                Value::Boolean(value) => DirectParameterValue::Boolean(*value),
                _ => unreachable!("direct parameter value type was certified"),
            },
            Self::Prepared(batch) => match batch.value(row_index, parameter_index) {
                PreparedDmlValueRef::Null => DirectParameterValue::Null,
                PreparedDmlValueRef::Text(value) => DirectParameterValue::String(value),
                PreparedDmlValueRef::Boolean(value) => DirectParameterValue::Boolean(value),
                _ => unreachable!("direct parameter value type was certified"),
            },
        }
    }
}

/// Executes independent parameterized entity INSERT statements as one dense
/// transaction write. The public batch still returns one affected-row result
/// per logical statement, while parsing, binding, and transaction staging
/// happen once for the homogeneous batch.
pub(crate) async fn try_execute_entity_insert_parameter_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    parameter_batch: &RecordBatch,
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    try_execute_entity_insert_batch(
        ctx,
        plan,
        EntityInsertParameterBatch::Arrow(parameter_batch),
        true,
    )
    .await
}

pub(crate) async fn try_execute_entity_insert_prepared_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    parameter_batch: &PreparedDmlParameterBatch,
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    try_execute_entity_insert_batch(
        ctx,
        plan,
        EntityInsertParameterBatch::Prepared(parameter_batch),
        true,
    )
    .await
}

/// Executes the prepared single-row lix_file path shape in one provider batch.
/// This keeps Git replay on the same production parameter-page contract while
/// preserving its explicit marker barrier in the surrounding transaction.
pub(crate) async fn try_execute_file_prepared_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    parameter_batch: &PreparedDmlParameterBatch,
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    let BoundWriteTarget::File(surface) = &plan.bound.target else {
        return Ok(None);
    };
    let Some(shape) = fast_file_path_write_shape(plan, surface) else {
        return Ok(None);
    };
    let BoundWriteInput::Values(values) = &plan.bound.input else {
        return Ok(None);
    };
    if values.rows.len() != 1 || parameter_batch.column_count() != values.columns.len() {
        return Ok(None);
    }
    let metadata = ExecuteStatementMetadata::default();
    let mut writes = Vec::with_capacity(parameter_batch.row_count());
    for row_index in 0..parameter_batch.row_count() {
        let params = parameter_batch.row_values(row_index)?;
        let row = &values.rows[0];
        writes.push((
            shape
                .id_index
                .map(|index| eval_fast_file_text(&row[index], &params, "id"))
                .transpose()?,
            eval_fast_file_text(&row[shape.path_index], &params, "path")?,
            eval_fast_file_blob(&row[shape.data_index], &params, "content")?,
            shape
                .metadata_index
                .map(|index| eval_fast_file_metadata(&row[index], &params))
                .transpose()?
                .flatten(),
            fast_file_blob_expr_splice_provenance(&row[shape.data_index], &metadata),
        ));
    }
    let affected = crate::sql2::providers::execute_fast_lix_file_id_path_writes(
        ctx,
        writes,
        shape.conflict,
        metadata.mutation_identity(),
    )
    .await?;
    if affected != Some(parameter_batch.row_count() as u64) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "prepared lix_file batch affected {:?} rows, expected {}",
                affected,
                parameter_batch.row_count()
            ),
        ));
    }
    Ok(Some(
        (0..parameter_batch.row_count())
            .map(|_| SqlWriteResult::affected(1))
            .collect(),
    ))
}

pub(crate) async fn try_execute_entity_insert_value_batch<'a>(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    parameter_rows: &'a [&'a [Value]],
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    try_execute_entity_insert_batch(
        ctx,
        plan,
        EntityInsertParameterBatch::Values(parameter_rows),
        false,
    )
    .await
}

async fn try_execute_entity_insert_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    parameter_batch: EntityInsertParameterBatch<'_>,
    allow_generic_fallback: bool,
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    let BoundWriteTarget::Entity(EntityWriteSurface::Base { schema_key }) = &plan.bound.target
    else {
        return Ok(None);
    };
    let BoundWriteInput::Values(values) = &plan.bound.input else {
        return Ok(None);
    };
    if plan.bound.op != BoundWriteOp::Insert
        || values.rows.len() != 1
        || plan.bound.conflict.is_some()
        || plan.bound.returning.is_some()
        || !matches!(plan.bound.branch_scope, BranchScope::Active { .. })
    {
        return Ok(None);
    }

    let spec = entity_spec(ctx, schema_key)?;
    if spec.has_inter_row_constraints {
        return Ok(None);
    }
    validate_bound_write_supported(plan, &spec)?;
    let active_branch_commit_id = if plan_references_active_branch_commit_id(plan) {
        Some(load_active_branch_commit_id(ctx).await?)
    } else {
        None
    };
    let layout = InsertRowLayout::from_values(&spec, values)?;
    if layout.columns.iter().any(|target| {
        !matches!(
            target,
            InsertColumnTarget::Visible { .. } | InsertColumnTarget::Untracked
        )
    }) {
        return Ok(None);
    }
    let certification_span = tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.entity_insert_parameter_batch.certify"
    )
    .entered();
    let Some(mut write_rows) = certified_entity_insert_parameter_batch(
        ctx,
        plan,
        &spec,
        &layout,
        values,
        parameter_batch,
        allow_generic_fallback,
        active_branch_commit_id.as_ref(),
    )?
    else {
        return Ok(None);
    };
    drop(certification_span);
    let collection_empty = collection_is_certifiably_empty(ctx, &spec.schema_key).await?;
    let committed_conflict = if collection_empty {
        None
    } else {
        let raw_rows = write_rows.into_raw()?;
        let committed = scan_entity_conflict_candidates(ctx, &spec, &raw_rows)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.entity_insert_parameter_batch.conflict_scan"
            ))
            .await?;
        let conflict_attribution_span = tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.entity_insert_parameter_batch.conflict_attribution"
        )
        .entered();
        let committed_identities = committed
            .iter()
            .map(|row| {
                (
                    (
                        row.entity_pk().clone(),
                        row.file_id().map(SharedStr::from),
                        SharedStr::from(row.branch_id()),
                        row.global(),
                    ),
                    row.untracked(),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
        let mut conflict = None;
        for (row_index, row) in raw_rows.iter().enumerate() {
            let entity_pk = row
                .entity_pk
                .expect("certified parameter INSERT rows have explicit identities");
            let identity = (
                entity_pk.clone(),
                row.file_id.cloned(),
                row.branch_id.clone(),
                row.global,
            );
            let Some(existing_untracked) = committed_identities.get(&identity).copied() else {
                continue;
            };
            let error = if existing_untracked != row.untracked {
                let requested = if row.untracked {
                    "untracked"
                } else {
                    "tracked"
                };
                let existing = if existing_untracked {
                    "untracked"
                } else {
                    "tracked"
                };
                LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "cannot insert {requested} row for schema '{}' entity_pk {:?}: a canonical {existing} row already exists; delete it first",
                        row.schema_key, entity_pk,
                    ),
                )
            } else {
                LixError::new(
                    LixError::CODE_UNIQUE,
                    crate::transaction::duplicate_insert_identity_message(
                        row.schema_key,
                        entity_pk,
                        Some(row.branch_id),
                        row.origin,
                    ),
                )
            };
            conflict = Some(with_parameter_batch_statement_index(error, row_index));
            break;
        }
        drop(conflict_attribution_span);
        drop(committed);
        write_rows = CertifiedEntityInsertParameterBatch::Raw(raw_rows);
        conflict
    };
    match write_rows {
        CertifiedEntityInsertParameterBatch::Typed(rows) => {
            ctx.stage_certified_parameter_batch_insert(rows)
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.entity_insert_parameter_batch.stage_rows"
                ))
                .await?;
        }
        CertifiedEntityInsertParameterBatch::Raw(rows) => {
            ctx.stage_parameter_batch_insert(rows)
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.entity_insert_parameter_batch.stage_rows"
                ))
                .await?;
        }
    }
    if let Some(error) = committed_conflict {
        return Err(error);
    }
    if !spec.certifies_path_value_replacement {
        #[cfg(test)]
        CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_EXECUTIONS.with(|executions| {
            executions.set(executions.get().saturating_add(1));
        });
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_certified_entity_insert_parameter_batch_execution();
    }
    Ok(Some(
        (0..parameter_batch.num_rows())
            .map(|_| SqlWriteResult::affected(1))
            .collect(),
    ))
}

async fn collection_is_certifiably_empty(
    ctx: &mut dyn SqlWriteExecutionContext,
    schema_key: &str,
) -> Result<bool, LixError> {
    let branch_id = ctx.active_branch_id().to_string();
    let scope = crate::collection_generation::CollectionScopeRef {
        schema_key,
        file_id: None,
    };
    if ctx.has_staged_collection_rows(&branch_id, scope)? {
        return Ok(false);
    }
    Ok(ctx
        .load_collection_generation(&branch_id, scope)
        .await?
        .is_some_and(|generation| generation.live_count == 0))
}

/// Executes a certified ordered run of point updates as one physical
/// scan/stage operation.
///
/// `executeBatch` remains ordered at its public boundary. This route folds
/// repeated identities in statement order and lowers the resulting unique,
/// identity-sorted replacements in one physical scan/stage operation.
pub(crate) async fn try_execute_entity_update_value_batch<'a>(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    parameter_rows: &'a [&'a [Value]],
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    try_execute_direct_path_value_replacement_batch(
        ctx,
        plan,
        EntityInsertParameterBatch::Values(parameter_rows),
    )
    .await
}

pub(crate) async fn try_execute_entity_update_parameter_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    parameter_batch: &RecordBatch,
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    try_execute_entity_update_batch(
        ctx,
        plan,
        EntityInsertParameterBatch::Arrow(parameter_batch),
    )
    .await
}

async fn try_execute_entity_update_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    parameter_batch: EntityInsertParameterBatch<'_>,
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    if let Some(results) =
        try_execute_direct_path_value_replacement_batch(ctx, plan, parameter_batch).await?
    {
        return Ok(Some(results));
    }

    let BoundWriteTarget::Entity(EntityWriteSurface::Base { schema_key }) = &plan.bound.target
    else {
        return Ok(None);
    };
    if plan.bound.op != BoundWriteOp::Update {
        return Ok(None);
    }
    if !matches!(plan.bound.input, BoundWriteInput::None)
        || plan.bound.conflict.is_some()
        || plan.bound.returning.is_some()
        || !matches!(plan.bound.branch_scope, BranchScope::Active { .. })
        || !matches!(plan.filters.rows, FilterSet::All)
        || plan_references_active_branch_commit_id(plan)
    {
        return Ok(None);
    }

    let spec = entity_spec(ctx, schema_key)?;
    if spec.has_inter_row_constraints
        || plan.bound.assignments.iter().any(|assignment| {
            spec.primary_key_paths
                .iter()
                .any(|path| path.as_slice() == [assignment.column.name.as_str()])
        })
    {
        return Ok(None);
    }
    validate_bound_write_supported(plan, &spec)?;

    let direct_primary_key_param =
        bound_single_text_primary_key_param(&spec, &plan.bound.predicate);
    let direct_replacement = direct_path_value_replacement(&spec, plan, direct_primary_key_param);
    let borrowed_direct_parameters = match parameter_batch {
        EntityInsertParameterBatch::Arrow(batch) => {
            direct_replacement.as_ref().and_then(|replacement| {
                direct_replacement_text_columns(
                    batch,
                    direct_primary_key_param?,
                    replacement.value_param_index,
                )
            })
        }
        EntityInsertParameterBatch::Values(_) | EntityInsertParameterBatch::Prepared(_) => None,
    };
    let mut parameter_rows = Vec::with_capacity(parameter_batch.num_rows());
    let mut entity_pks = Vec::<EntityPk>::with_capacity(parameter_batch.num_rows());
    let mut entity_pks_strictly_ordered = true;
    for row_index in 0..parameter_batch.num_rows() {
        let entity_pk = if let Some(columns) = borrowed_direct_parameters {
            if columns.primary_keys.is_null(row_index) {
                return Ok(None);
            }
            EntityPk::single(columns.primary_keys.value(row_index).to_owned())
        } else {
            let params = parameter_batch_row_values(parameter_batch, row_index)
                .map_err(|error| with_parameter_batch_statement_index(error, row_index))?;
            let entity_pk = if let Some(param_index) = direct_primary_key_param {
                let Some(Value::Text(value)) = params.get(param_index) else {
                    return Ok(None);
                };
                EntityPk::single(value.clone())
            } else {
                let Some(mut row_entity_pks) = bound_entity_pks_from_primary_key_predicate(
                    &spec,
                    &plan.bound.predicate,
                    &params,
                ) else {
                    return Ok(None);
                };
                if row_entity_pks.len() != 1 {
                    return Ok(None);
                }
                row_entity_pks.pop().expect("one point-update identity")
            };
            parameter_rows.push(params);
            entity_pk
        };
        if let Some(previous) = entity_pks.last() {
            match previous.cmp(&entity_pk) {
                Ordering::Less => {}
                Ordering::Equal => {
                    // Repeated identities observe earlier staged writes and
                    // are not independent statements.
                    return Ok(None);
                }
                Ordering::Greater => entity_pks_strictly_ordered = false,
            }
        }
        entity_pks.push(entity_pk);
    }
    if !entity_pks_strictly_ordered {
        let mut unique_entity_pks = std::collections::BTreeSet::new();
        if entity_pks
            .iter()
            .any(|entity_pk| !unique_entity_pks.insert(entity_pk))
        {
            return Ok(None);
        }
    }
    if borrowed_direct_parameters.is_some() && !entity_pks_strictly_ordered {
        for row_index in 0..parameter_batch.num_rows() {
            parameter_rows.push(
                parameter_batch_row_values(parameter_batch, row_index)
                    .map_err(|error| with_parameter_batch_statement_index(error, row_index))?,
            );
        }
    }

    let direct_ordered = entity_pks_strictly_ordered && direct_replacement.is_some();
    let scan_entity_pks = if direct_ordered {
        std::mem::take(&mut entity_pks)
    } else {
        entity_pks.clone()
    };
    let candidates = scan_entity_candidates_for_pks(
        ctx,
        plan,
        &spec,
        scan_entity_pks,
        direct_replacement.is_some(),
    )
    .await?;
    if direct_replacement.is_some()
        && candidates
            .iter()
            .any(|candidate| candidate.untracked() || candidate.file_id().is_some())
    {
        // Retention and plugin-owned file rows retain the canonical semantic
        // preparation path. This certificate is only for ordinary tracked
        // entity replacements.
        return Ok(None);
    }
    let mut affected_by_statement = Vec::with_capacity(parameter_rows.len());
    let mut write_rows = RawWriteBatch::with_capacity(parameter_rows.len());
    if direct_ordered && let Some(replacement) = direct_replacement.as_ref() {
        // Exact scans return physical entity order. Merge the ordered request
        // and result streams directly instead of cloning every identity into
        // a second B-tree and allocating one candidate vector per row.
        let mut candidates = candidates.iter().peekable();
        let primary_key_param_index = direct_primary_key_param
            .expect("ordered direct replacement has one primary-key parameter");
        let mut normalized = Vec::with_capacity(parameter_rows.len().saturating_mul(64));
        let mut offsets = Vec::with_capacity(parameter_rows.len());
        let mut matched_candidates = Vec::with_capacity(parameter_rows.len());
        for row_index in 0..parameter_batch.num_rows() {
            let (expected_entity_pk, borrowed_value, params) = if let Some(columns) =
                borrowed_direct_parameters
            {
                (
                    columns.primary_keys.value(row_index),
                    (!columns.values.is_null(row_index)).then(|| columns.values.value(row_index)),
                    None,
                )
            } else {
                let params = &parameter_rows[row_index];
                let expected_entity_pk = match params.get(primary_key_param_index) {
                    Some(Value::Text(value)) => value.as_str(),
                    _ => unreachable!("direct replacement primary key was validated as text"),
                };
                (expected_entity_pk, None, Some(params.as_slice()))
            };
            let mut affected = 0;
            while let Some(candidate) = candidates.peek().copied() {
                if candidate
                    .entity_pk()
                    .as_single_string()
                    .map_err(|error| with_parameter_batch_statement_index(error, row_index))?
                    != expected_entity_pk
                {
                    break;
                }
                let candidate = EntityLiveRowRef::from(
                    candidates
                        .next()
                        .expect("peeked exact entity candidate remains available"),
                );
                let start = normalized.len();
                let result = if borrowed_direct_parameters.is_some() {
                    append_direct_path_value_replacement_json_text(
                        &mut normalized,
                        candidate,
                        borrowed_value,
                    )
                } else {
                    append_direct_path_value_replacement_json(
                        &mut normalized,
                        candidate,
                        params.expect("owned direct replacement has one parameter row"),
                        replacement,
                    )
                };
                result.map_err(|error| with_parameter_batch_statement_index(error, row_index))?;
                offsets.push((start, normalized.len()));
                matched_candidates.push((row_index, candidate));
                affected += 1;
            }
            affected_by_statement.push(affected);
        }
        // SAFETY: the arena contains only JSON syntax literals and text
        // emitted by the canonical JSON append helpers above.
        let snapshots = unsafe {
            TransactionJson::from_validated_certified_row_content_arena(normalized, offsets)?
        };
        for ((row_index, candidate), snapshot) in matched_candidates.into_iter().zip(snapshots) {
            append_direct_path_value_replacement_prepared_row(
                &mut write_rows,
                &spec,
                candidate,
                snapshot,
            )
            .map_err(|error| with_parameter_batch_statement_index(error, row_index))?;
        }
    } else {
        let mut candidates_by_pk = std::collections::BTreeMap::<EntityPk, Vec<_>>::new();
        for candidate in candidates.iter() {
            candidates_by_pk
                .entry(candidate.entity_pk().clone())
                .or_default()
                .push(candidate);
        }
        for (row_index, (entity_pk, params)) in
            entity_pks.into_iter().zip(&parameter_rows).enumerate()
        {
            let mut affected = 0;
            for candidate in candidates_by_pk.remove(&entity_pk).unwrap_or_default() {
                let appended = match direct_replacement.as_ref() {
                    Some(replacement) => append_direct_path_value_replacement_row(
                        &mut write_rows,
                        &spec,
                        candidate,
                        params,
                        replacement,
                    )
                    .map(|()| true),
                    None => append_entity_update_row(
                        &mut write_rows,
                        ctx,
                        plan,
                        &spec,
                        candidate,
                        params,
                        None,
                    ),
                }
                .map_err(|error| with_parameter_batch_statement_index(error, row_index))?;
                if appended {
                    affected += 1;
                }
            }
            affected_by_statement.push(affected);
        }
    }
    stage_rows(ctx, TransactionWriteMode::Replace, write_rows).await?;
    #[cfg(test)]
    {
        ENTITY_UPDATE_PARAMETER_BATCH_EXECUTIONS.with(|executions| {
            executions.set(executions.get() + 1);
        });
        if direct_replacement.is_some() {
            CERTIFIED_REPLACEMENT_PARAMETER_BATCH_EXECUTIONS.with(|executions| {
                executions.set(executions.get() + 1);
            });
        }
    }
    Ok(Some(
        affected_by_statement
            .into_iter()
            .map(SqlWriteResult::affected)
            .collect(),
    ))
}

pub(crate) async fn try_execute_entity_update_prepared_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    parameter_batch: &PreparedDmlParameterBatch,
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    try_execute_entity_update_batch(
        ctx,
        plan,
        EntityInsertParameterBatch::Prepared(parameter_batch),
    )
    .await
}

fn parameter_batch_row_values(
    parameter_batch: EntityInsertParameterBatch<'_>,
    row_index: usize,
) -> Result<Vec<Value>, LixError> {
    match parameter_batch {
        EntityInsertParameterBatch::Arrow(batch) => super::write::parameter_row(batch, row_index),
        EntityInsertParameterBatch::Prepared(batch) => batch.row_values(row_index),
        EntityInsertParameterBatch::Values(rows) => {
            rows.get(row_index).map(|row| row.to_vec()).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "SQL parameter row is outside the batch",
                )
            })
        }
    }
}

/// Lowers the dominant JSON-pointer replacement shape directly from borrowed
/// public parameters into one canonical row-content arena.
///
/// Statement order is preserved even when identities repeat or arrive out of
/// order: each live identity reports every matching statement as affected and
/// the last statement supplies the staged replacement. The canonical write
/// batch itself remains identity-sorted and contains one row per identity.
async fn try_execute_direct_path_value_replacement_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    parameter_batch: EntityInsertParameterBatch<'_>,
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    let BoundWriteTarget::Entity(EntityWriteSurface::Base { schema_key }) = &plan.bound.target
    else {
        return Ok(None);
    };
    if plan.bound.op != BoundWriteOp::Update {
        return Ok(None);
    }
    #[cfg(feature = "storage-benches")]
    let record_value_certificate = matches!(parameter_batch, EntityInsertParameterBatch::Values(_));
    #[cfg(feature = "storage-benches")]
    if record_value_certificate {
        crate::storage_bench::record_certified_entity_update_value_batch_attempt();
    }
    if !matches!(plan.bound.input, BoundWriteInput::None)
        || plan.bound.conflict.is_some()
        || plan.bound.returning.is_some()
        || !matches!(plan.bound.branch_scope, BranchScope::Active { .. })
        || !matches!(plan.filters.rows, FilterSet::All)
        || plan_references_active_branch_commit_id(plan)
    {
        return Ok(None);
    }

    let spec = entity_spec(ctx, schema_key)?;
    if spec.has_inter_row_constraints
        || plan.bound.assignments.iter().any(|assignment| {
            spec.primary_key_paths
                .iter()
                .any(|path| path.as_slice() == [assignment.column.name.as_str()])
        })
    {
        return Ok(None);
    }
    validate_bound_write_supported(plan, &spec)?;
    let Some(primary_key_param_index) =
        bound_single_text_primary_key_param(&spec, &plan.bound.predicate)
    else {
        return Ok(None);
    };
    let Some(replacement) =
        direct_path_value_replacement(&spec, plan, Some(primary_key_param_index))
    else {
        return Ok(None);
    };
    if !parameter_batch.column_matches(primary_key_param_index, EntityColumnType::String)
        || !parameter_batch.column_matches(replacement.value_param_index, EntityColumnType::String)
    {
        return Ok(None);
    }
    let Some(schema_catalog) = ctx.schema_catalog_snapshot() else {
        return Ok(None);
    };
    let Some((schema_plan_id, schema_plan)) = schema_catalog.plan_for_key(&spec.schema_key) else {
        return Ok(None);
    };
    if !schema_plan.accepts_canonical_certificate() {
        return Ok(None);
    }

    let row_count = parameter_batch.num_rows();
    let mut primary_key_arena = Vec::new();
    let mut primary_key_offsets = Vec::with_capacity(row_count);
    let mut previous_row = None;
    let mut primary_keys_strictly_ordered = true;
    let mut parameter_identity_hasher = blake3::Hasher::new();
    for statement_index in 0..row_count {
        let DirectParameterValue::String(primary_key) =
            parameter_batch.value(primary_key_param_index, statement_index)
        else {
            return Ok(None);
        };
        if let Some(previous_row) = previous_row {
            let DirectParameterValue::String(previous) =
                parameter_batch.value(primary_key_param_index, previous_row)
            else {
                unreachable!("the previous primary-key parameter was certified as text")
            };
            primary_keys_strictly_ordered &= previous < primary_key;
        }
        previous_row = Some(statement_index);
        parameter_identity_hasher.update(&(primary_key.len() as u64).to_le_bytes());
        parameter_identity_hasher.update(primary_key.as_bytes());
        let start = primary_key_arena.len();
        primary_key_arena.extend_from_slice(primary_key.as_bytes());
        primary_key_offsets.push((start, primary_key_arena.len()));
    }
    let parameter_identity_digest = *parameter_identity_hasher.finalize().as_bytes();
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_crud_ownership(
        crate::storage_bench::CRUD_OWNERSHIP_SQL_BOUND,
        row_count,
        primary_key_arena.len(),
        0,
        2,
        0,
        0,
    );
    let active_branch_id = ctx.active_branch_id().to_owned();
    let scope = crate::collection_generation::CollectionScopeRef {
        schema_key: &spec.schema_key,
        file_id: None,
    };
    // Generation controls describe committed HOT state. Any staged member can
    // change the effective identity set, so it must force the overlay-aware
    // candidate scan instead of certifying the committed digest.
    let has_staged_collection_rows = ctx.has_staged_collection_rows(&active_branch_id, scope)?;
    let collection_generation = ctx
        .load_collection_generation(&active_branch_id, scope)
        .await?;
    let certified_ordered_generation = primary_keys_strictly_ordered
        && !has_staged_collection_rows
        && collection_generation.is_some_and(|generation| {
            generation.live_count == row_count as u64
                && generation.ordered_identity_digest == Some(parameter_identity_digest)
        });
    let typed_journal_admitted = if certified_ordered_generation {
        ctx.can_stage_typed_mutation_journal_replace(
            &spec.schema_key,
            row_count as u64,
            parameter_identity_digest,
        )
        .await?
    } else {
        false
    };
    if typed_journal_admitted {
        let mut snapshots = Vec::with_capacity(
            primary_key_arena
                .len()
                .saturating_add(row_count.saturating_mul(32)),
        );
        let mut snapshot_offsets = Vec::with_capacity(row_count);
        for (statement_index, &(identity_start, identity_end)) in
            primary_key_offsets.iter().enumerate()
        {
            let primary_key = std::str::from_utf8(&primary_key_arena[identity_start..identity_end])
                .expect("borrowed SQL text remains UTF-8 in its mutation arena");
            let snapshot_start = snapshots.len();
            snapshots.extend_from_slice(b"{\"path\":");
            append_canonical_json_string(&mut snapshots, primary_key)
                .map_err(|error| with_parameter_batch_statement_index(error, statement_index))?;
            snapshots.extend_from_slice(b",\"value\":");
            match parameter_batch.value(replacement.value_param_index, statement_index) {
                DirectParameterValue::Null => snapshots.extend_from_slice(b"null"),
                DirectParameterValue::String(raw) => {
                    append_canonical_json_parameter(&mut snapshots, raw).map_err(|error| {
                        with_parameter_batch_statement_index(error, statement_index)
                    })?;
                }
                DirectParameterValue::Boolean(_) => {
                    unreachable!("the certified replacement value parameter is text")
                }
            }
            snapshots.push(b'}');
            snapshot_offsets.push((snapshot_start, snapshots.len()));
        }
        let journal = TypedMutationJournalBatch::new(
            schema_plan_id,
            spec.schema_key.as_str().into(),
            active_branch_id.into(),
            primary_key_arena,
            primary_key_offsets,
            snapshots,
            snapshot_offsets,
            parameter_identity_digest,
        )?;
        ctx.stage_typed_mutation_journal_replace(journal)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.entity_update_value_batch.stage_typed_journal",
                row_count
            ))
            .await?;
        #[cfg(test)]
        {
            ENTITY_UPDATE_PARAMETER_BATCH_EXECUTIONS.with(|executions| {
                executions.set(executions.get().saturating_add(1));
            });
            CERTIFIED_REPLACEMENT_PARAMETER_BATCH_EXECUTIONS.with(|executions| {
                executions.set(executions.get().saturating_add(1));
            });
            CERTIFIED_GENERATION_IDENTITY_REPLACEMENTS.with(|executions| {
                executions.set(executions.get().saturating_add(1));
            });
        }
        #[cfg(feature = "storage-benches")]
        if record_value_certificate {
            crate::storage_bench::record_certified_entity_update_value_batch_hit(row_count);
        }
        return Ok(Some(
            (0..row_count)
                .map(|_| SqlWriteResult::affected(1))
                .collect(),
        ));
    }
    let primary_key_arena = SharedStr::from_utf8(bytes::Bytes::from(primary_key_arena))
        .map_err(|_| LixError::unknown("certified replacement primary-key arena is not UTF-8"))?;
    let entity_pks = primary_key_offsets
        .into_iter()
        .map(|(start, end)| {
            EntityPk::from_validated_shared_string(
                primary_key_arena
                    .slice(start..end)
                    .expect("certified replacement primary-key offsets preserve UTF-8"),
            )
        })
        .collect::<Vec<_>>();

    // Keep the already-sorted case allocation-free. Otherwise sort statement
    // ordinals by identity and then ordinal so equal-identity groups retain
    // SQL's sequential last-write-wins semantics.
    let sorted_statement_ordinals = if primary_keys_strictly_ordered {
        None
    } else {
        let mut ordinals = (0..row_count).collect::<Vec<_>>();
        ordinals.sort_unstable_by(|left, right| {
            entity_pks[*left]
                .cmp(&entity_pks[*right])
                .then_with(|| left.cmp(right))
        });
        Some(ordinals)
    };
    let deduplicated_entity_pks = if let Some(ordinals) = &sorted_statement_ordinals {
        let mut unique = Vec::with_capacity(row_count);
        for &statement_index in ordinals {
            let entity_pk = &entity_pks[statement_index];
            if unique.last() != Some(entity_pk) {
                unique.push(entity_pk.clone());
            }
        }
        Some(unique)
    } else {
        None
    };
    let unique_entity_pks = deduplicated_entity_pks
        .as_deref()
        .unwrap_or(entity_pks.as_slice());
    let unique_row_count = unique_entity_pks.len();
    let ordered_identity_digest =
        crate::collection_generation::ordered_single_string_identity_digest(
            unique_entity_pks.iter(),
        );
    let certified_generation_identity = !has_staged_collection_rows
        && collection_generation.is_some_and(|generation| {
            generation.live_count == unique_row_count as u64
                && generation.ordered_identity_digest.is_some()
                && generation.ordered_identity_digest == ordered_identity_digest
        });
    #[cfg(test)]
    if certified_generation_identity {
        CERTIFIED_GENERATION_IDENTITY_REPLACEMENTS.with(|executions| {
            executions.set(executions.get().saturating_add(1));
        });
    }
    let candidates = if certified_generation_identity {
        MaterializedHotStateBatch::default()
    } else {
        let candidates =
            scan_entity_candidates_for_pks(ctx, plan, &spec, unique_entity_pks.to_vec(), true)
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.entity_update_value_batch.candidate_scan",
                    row_count,
                    unique_row_count
                ))
                .await?;
        if candidates.iter().any(|candidate| {
            candidate.untracked()
                || candidate.global()
                || candidate.file_id().is_some()
                || candidate.metadata().is_some()
        }) {
            return Ok(None);
        }
        candidates
    };
    // Sorting identities changes expression evaluation order. Once this route
    // is fully certified, validate JSON arguments for live rows in original
    // statement order so a later replacement cannot hide an earlier error.
    // Missing-row UPDATEs do not evaluate SET expressions, so first derive
    // liveness from the certified generation or the exact candidate scan.
    if !primary_keys_strictly_ordered {
        let live_unique_identities = if certified_generation_identity {
            None
        } else {
            let mut live = vec![false; unique_row_count];
            let mut candidate_index = 0;
            for (identity_index, entity_pk) in unique_entity_pks.iter().enumerate() {
                while candidate_index < candidates.len()
                    && candidates.row(candidate_index).entity_pk() < entity_pk
                {
                    candidate_index += 1;
                }
                live[identity_index] = candidate_index < candidates.len()
                    && candidates.row(candidate_index).entity_pk() == entity_pk;
            }
            Some(live)
        };
        let mut scratch = Vec::new();
        for (statement_index, entity_pk) in entity_pks.iter().enumerate() {
            let identity_index = unique_entity_pks
                .binary_search(entity_pk)
                .expect("every statement identity belongs to the unique identity set");
            if live_unique_identities
                .as_ref()
                .is_some_and(|live| !live[identity_index])
            {
                continue;
            }
            match parameter_batch.value(replacement.value_param_index, statement_index) {
                DirectParameterValue::Null => {}
                DirectParameterValue::String(raw) => {
                    scratch.clear();
                    append_canonical_json_parameter(&mut scratch, raw).map_err(|error| {
                        with_parameter_batch_statement_index(error, statement_index)
                    })?;
                }
                DirectParameterValue::Boolean(_) => {
                    unreachable!("the certified replacement value parameter is text")
                }
            }
        }
    }
    let replaces_complete_collection = certified_generation_identity
        || (candidates.len() == unique_row_count
            && collection_generation
                .is_some_and(|generation| generation.live_count == unique_row_count as u64));

    let (estimated_row_bytes, replacement_identity_replay_bytes) = unique_entity_pks
        .iter()
        .try_fold((0_usize, 0_usize), |(estimated, replay), entity_pk| {
            Some((
                estimated.checked_add(
                    entity_pk
                        .as_single_string()
                        .map_or(32, |path| path.len().saturating_add(32)),
                )?,
                replay.checked_add(
                    spec.schema_key
                        .len()
                        .checked_add(entity_pk.estimated_heap_bytes())?
                        .checked_add(128)?,
                )?,
            ))
        })
        .ok_or_else(|| LixError::unknown("certified replacement byte accounting overflowed"))?;
    let mut normalized = Vec::with_capacity(estimated_row_bytes);
    let replacement_capacity = if certified_generation_identity {
        unique_row_count
    } else {
        candidates.len()
    };
    let mut snapshot_offsets = Vec::with_capacity(replacement_capacity);
    let mut replacement_entity_pks = Vec::with_capacity(replacement_capacity);
    let mut replacement_predecessors = Vec::with_capacity(replacement_capacity);
    let mut affected_by_statement =
        (!certified_generation_identity).then(|| vec![0_u64; row_count]);
    let mut candidate_index = 0;
    let mut ordinal_index = 0;
    for (identity_index, entity_pk) in unique_entity_pks.iter().enumerate() {
        let (first_statement_index, last_statement_index) =
            if let Some(ordinals) = &sorted_statement_ordinals {
                let first = ordinal_index;
                while ordinal_index < ordinals.len()
                    && entity_pks[ordinals[ordinal_index]] == *entity_pk
                {
                    ordinal_index += 1;
                }
                (first, ordinal_index - 1)
            } else {
                (identity_index, identity_index)
            };
        if !certified_generation_identity {
            while candidate_index < candidates.len()
                && candidates.row(candidate_index).entity_pk() < entity_pk
            {
                candidate_index += 1;
            }
            if candidate_index == candidates.len()
                || candidates.row(candidate_index).entity_pk() != entity_pk
            {
                continue;
            }
        }

        let statement_index = sorted_statement_ordinals
            .as_ref()
            .map_or(last_statement_index, |ordinals| {
                ordinals[last_statement_index]
            });

        let start = normalized.len();
        normalized.extend_from_slice(b"{\"path\":");
        append_canonical_json_string(&mut normalized, entity_pk.as_single_string()?)
            .map_err(|error| with_parameter_batch_statement_index(error, statement_index))?;
        normalized.extend_from_slice(b",\"value\":");
        match parameter_batch.value(replacement.value_param_index, statement_index) {
            DirectParameterValue::Null => normalized.extend_from_slice(b"null"),
            DirectParameterValue::String(raw) => {
                append_canonical_json_parameter(&mut normalized, raw).map_err(|error| {
                    with_parameter_batch_statement_index(error, statement_index)
                })?;
            }
            DirectParameterValue::Boolean(_) => {
                unreachable!("the certified replacement value parameter is text")
            }
        }
        normalized.push(b'}');
        snapshot_offsets.push((start, normalized.len()));
        replacement_entity_pks.push(entity_pk.clone());
        replacement_predecessors.push(
            (!certified_generation_identity)
                .then(|| {
                    candidates
                        .row(candidate_index)
                        .durable_predecessor()
                        .cloned()
                })
                .flatten(),
        );
        if let Some(ordinals) = &sorted_statement_ordinals {
            for &affected_statement in &ordinals[first_statement_index..=last_statement_index] {
                if let Some(affected_by_statement) = &mut affected_by_statement {
                    affected_by_statement[affected_statement] = 1;
                }
            }
        } else if let Some(affected_by_statement) = &mut affected_by_statement {
            affected_by_statement[statement_index] = 1;
        }
        if !certified_generation_identity {
            candidate_index += 1;
        }
    }
    if !replacement_entity_pks.is_empty() {
        let normalized_len = normalized.len();
        // SAFETY: the arena contains only JSON syntax literals and text
        // emitted by the canonical JSON append helpers above.
        let snapshots = unsafe {
            TransactionJson::from_validated_certified_row_content_arena(
                normalized,
                snapshot_offsets,
            )?
        };
        let mut rows = CertifiedParameterReplacementBatch::new(
            replacement_entity_pks,
            snapshots,
            spec.schema_key.as_str().into(),
            active_branch_id.into(),
            CertifiedRawWriteBatchPreparation {
                schema_plan_id,
                facts: PreparedRowFacts {
                    row_content_validated: true,
                    requires_transaction_validation: false,
                },
                tracked_keys_strictly_ordered: true,
                complete_collection_replacement: replaces_complete_collection
                    .then(|| {
                        Some(CompleteCollectionReplacementProof {
                            ordered_identity_digest: ordered_identity_digest?,
                            replay_bytes: u64::try_from(
                                replacement_identity_replay_bytes.checked_add(normalized_len)?,
                            )
                            .ok()?,
                        })
                    })
                    .flatten(),
            },
        )?;
        for (index, predecessor) in replacement_predecessors.into_iter().enumerate() {
            rows.set_durable_predecessor(index, predecessor);
        }
        ctx.stage_certified_parameter_batch_replace(rows)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.entity_update_value_batch.stage_rows",
                row_count
            ))
            .await?;
    }

    #[cfg(test)]
    {
        ENTITY_UPDATE_PARAMETER_BATCH_EXECUTIONS.with(|executions| {
            executions.set(executions.get().saturating_add(1));
        });
        CERTIFIED_REPLACEMENT_PARAMETER_BATCH_EXECUTIONS.with(|executions| {
            executions.set(executions.get().saturating_add(1));
        });
    }
    #[cfg(feature = "storage-benches")]
    if record_value_certificate {
        crate::storage_bench::record_certified_entity_update_value_batch_hit(row_count);
    }
    Ok(Some(if certified_generation_identity {
        (0..row_count)
            .map(|_| SqlWriteResult::affected(1))
            .collect()
    } else {
        affected_by_statement
            .expect("uncertified replacement tracks statement effects")
            .into_iter()
            .map(SqlWriteResult::affected)
            .collect()
    }))
}

struct DirectPathValueReplacement {
    value_param_index: usize,
}

#[derive(Clone, Copy)]
struct DirectReplacementTextColumns<'a> {
    primary_keys: DirectTextColumn<'a>,
    values: DirectTextColumn<'a>,
}

#[derive(Clone, Copy)]
enum DirectTextColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
}

impl<'a> DirectTextColumn<'a> {
    fn is_null(self, row_index: usize) -> bool {
        match self {
            Self::Utf8(array) => array.is_null(row_index),
            Self::LargeUtf8(array) => array.is_null(row_index),
        }
    }

    fn value(self, row_index: usize) -> &'a str {
        match self {
            Self::Utf8(array) => array.value(row_index),
            Self::LargeUtf8(array) => array.value(row_index),
        }
    }
}

fn direct_text_column<'a>(array: &'a dyn Array) -> Option<DirectTextColumn<'a>> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        Some(DirectTextColumn::Utf8(array))
    } else {
        array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(DirectTextColumn::LargeUtf8)
    }
}

fn direct_replacement_text_columns<'a>(
    batch: &'a RecordBatch,
    primary_key_param_index: usize,
    value_param_index: usize,
) -> Option<DirectReplacementTextColumns<'a>> {
    if crate::sql2::result_metadata::field_is_json(batch.schema().field(value_param_index)) {
        return None;
    }
    Some(DirectReplacementTextColumns {
        primary_keys: direct_text_column(batch.column(primary_key_param_index).as_ref())?,
        values: direct_text_column(batch.column(value_param_index).as_ref())?,
    })
}

#[derive(Clone, Copy)]
enum EntityLiveRowRef<'a> {
    Owned(&'a MaterializedHotStateRow),
    Batch(MaterializedHotStateRowRef<'a>),
}

impl<'a> EntityLiveRowRef<'a> {
    fn entity_pk(self) -> &'a EntityPk {
        match self {
            Self::Owned(row) => &row.entity_pk,
            Self::Batch(row) => row.entity_pk(),
        }
    }

    fn schema_key(self) -> &'a str {
        match self {
            Self::Owned(row) => &row.schema_key,
            Self::Batch(row) => row.schema_key(),
        }
    }

    fn file_id(self) -> Option<&'a str> {
        match self {
            Self::Owned(row) => row.file_id.as_deref(),
            Self::Batch(row) => row.file_id(),
        }
    }

    fn snapshot_content(self) -> Option<&'a str> {
        match self {
            Self::Owned(row) => row.snapshot_content.as_deref(),
            Self::Batch(row) => row.snapshot_content().map(SharedStr::as_str),
        }
    }

    fn metadata(self) -> Option<&'a str> {
        match self {
            Self::Owned(row) => row.metadata.as_deref(),
            Self::Batch(row) => row.metadata().map(SharedStr::as_str),
        }
    }

    fn created_at(self) -> crate::common::LixTimestamp {
        match self {
            Self::Owned(row) => row.created_at,
            Self::Batch(row) => row.created_at(),
        }
    }

    fn updated_at(self) -> crate::common::LixTimestamp {
        match self {
            Self::Owned(row) => row.updated_at,
            Self::Batch(row) => row.updated_at(),
        }
    }

    fn global(self) -> bool {
        match self {
            Self::Owned(row) => row.global,
            Self::Batch(row) => row.global(),
        }
    }

    fn change_id(self) -> Option<crate::changelog::ChangeId> {
        match self {
            Self::Owned(row) => row.change_id,
            Self::Batch(row) => row.change_id(),
        }
    }

    fn commit_id(self) -> Option<CommitId> {
        match self {
            Self::Owned(row) => row.commit_id,
            Self::Batch(row) => row.commit_id(),
        }
    }

    fn untracked(self) -> bool {
        match self {
            Self::Owned(row) => row.untracked,
            Self::Batch(row) => row.untracked(),
        }
    }

    fn durable_predecessor(
        self,
    ) -> Option<&'a crate::hot_state::CertifiedCurrentStatePredecessor> {
        match self {
            Self::Owned(_) => None,
            Self::Batch(row) => row.durable_predecessor(),
        }
    }

    fn branch_id(self) -> &'a str {
        match self {
            Self::Owned(row) => row.branch_id.as_ref(),
            Self::Batch(row) => row.branch_id(),
        }
    }
}

impl<'a> From<&'a MaterializedHotStateRow> for EntityLiveRowRef<'a> {
    fn from(row: &'a MaterializedHotStateRow) -> Self {
        Self::Owned(row)
    }
}

impl<'a> From<MaterializedHotStateRowRef<'a>> for EntityLiveRowRef<'a> {
    fn from(row: MaterializedHotStateRowRef<'a>) -> Self {
        Self::Batch(row)
    }
}

fn direct_path_value_replacement(
    spec: &EntitySurfaceSpec,
    plan: &LogicalWritePlan,
    primary_key_param_index: Option<usize>,
) -> Option<DirectPathValueReplacement> {
    if !spec.certifies_path_value_replacement
        || primary_key_param_index.is_none()
        || spec.columns.len() != 2
        || plan.bound.assignments.len() != 1
    {
        return None;
    }
    let assignment = &plan.bound.assignments[0];
    if assignment.column.name != "value"
        || spec
            .visible_column("value")
            .is_none_or(|column| column.column_type != EntityColumnType::Json)
    {
        return None;
    }
    let BoundExpr::Function { name, args } = &assignment.value else {
        return None;
    };
    let [BoundExpr::Param(param)] = args.as_slice() else {
        return None;
    };
    (name == "lix_json").then(|| DirectPathValueReplacement {
        value_param_index: param.index.saturating_sub(1),
    })
}

fn append_direct_path_value_replacement_row<'a>(
    rows: &mut RawWriteBatch,
    spec: &EntitySurfaceSpec,
    candidate: impl Into<EntityLiveRowRef<'a>>,
    params: &[Value],
    replacement: &DirectPathValueReplacement,
) -> Result<(), LixError> {
    let candidate = candidate.into();
    let mut normalized = Vec::new();
    append_direct_path_value_replacement_json(&mut normalized, candidate, params, replacement)?;
    // SAFETY: the row is assembled from UTF-8 literals, existing row text,
    // and canonical JSON parameter text.
    let normalized = unsafe { SharedStr::from_utf8_unchecked(bytes::Bytes::from(normalized)) };
    append_direct_path_value_replacement_prepared_row(
        rows,
        spec,
        candidate,
        TransactionJson::from_certified_shared_normalized_row_content(normalized),
    )
}

fn append_direct_path_value_replacement_json(
    normalized: &mut Vec<u8>,
    candidate: EntityLiveRowRef<'_>,
    params: &[Value],
    replacement: &DirectPathValueReplacement,
) -> Result<(), LixError> {
    let raw = match params.get(replacement.value_param_index) {
        Some(Value::Null) => None,
        Some(Value::Text(raw)) => Some(raw.as_str()),
        Some(_) => {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "lix_json expects a text argument",
            ));
        }
        None => {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "missing SQL parameter ${}",
                    replacement.value_param_index + 1
                ),
            ));
        }
    };
    append_direct_path_value_replacement_json_text(normalized, candidate, raw)
}

fn append_direct_path_value_replacement_json_text(
    normalized: &mut Vec<u8>,
    candidate: EntityLiveRowRef<'_>,
    raw: Option<&str>,
) -> Result<(), LixError> {
    normalized.extend_from_slice(br#"{"path":"#);
    append_canonical_json_string(normalized, candidate.entity_pk().as_single_string()?)?;
    normalized.extend_from_slice(br#","value":"#);
    match raw {
        None => normalized.extend_from_slice(b"null"),
        Some(raw) => append_canonical_json_parameter(normalized, raw)?,
    }
    normalized.push(b'}');
    Ok(())
}

/// Appends a `lix_json` parameter in serde_json's stable compact form.
///
/// Public parameters do not carry a canonical-JSON type certificate. The
/// streaming recognizer proves already-canonical input in one allocation-free
/// scan. Noncanonical object order is reparsed into sorted compact bytes,
/// without the former redundant serde validation scan. Inputs outside this
/// deliberately narrow scalar grammar retain the canonical DOM fallback.
fn append_canonical_json_parameter(normalized: &mut Vec<u8>, raw: &str) -> Result<(), LixError> {
    if CanonicalJsonText::recognizes(raw) {
        normalized.extend_from_slice(raw.as_bytes());
        return Ok(());
    }
    let original_len = normalized.len();
    if CanonicalJsonText::append_normalized(raw, normalized) {
        return Ok(());
    }
    normalized.truncate(original_len);

    let value = serde_json::from_str::<JsonValue>(raw).map_err(|error| {
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("lix_json argument is not valid JSON: {error}"),
        )
    })?;
    serde_json::to_writer(normalized, &value).map_err(|error| {
        LixError::unknown(format!(
            "certified replacement value failed to serialize: {error}"
        ))
    })
}

struct CanonicalJsonText<'a> {
    bytes: &'a [u8],
    position: usize,
    remaining_depth: u8,
}

impl<'a> CanonicalJsonText<'a> {
    // serde_json's default recursion limit rejects the 128th nested container.
    const MAX_DEPTH: u8 = 127;

    fn recognizes(raw: &'a str) -> bool {
        let mut parser = Self {
            bytes: raw.as_bytes(),
            position: 0,
            remaining_depth: Self::MAX_DEPTH,
        };
        parser.value() && parser.position == parser.bytes.len()
    }

    fn append_normalized(raw: &'a str, output: &mut Vec<u8>) -> bool {
        let mut parser = Self {
            bytes: raw.as_bytes(),
            position: 0,
            remaining_depth: Self::MAX_DEPTH,
        };
        parser.write_value(output) && parser.position == parser.bytes.len()
    }

    fn value(&mut self) -> bool {
        match self.peek() {
            Some(b'n') => self.literal(b"null"),
            Some(b't') => self.literal(b"true"),
            Some(b'f') => self.literal(b"false"),
            Some(b'"') => self.string(false).is_some(),
            Some(b'[') => self.with_container_depth(Self::array),
            Some(b'{') => self.with_container_depth(Self::object),
            Some(b'-' | b'0'..=b'9') => self.integer(),
            _ => false,
        }
    }

    fn array(&mut self) -> bool {
        self.position += 1;
        if self.take(b']') {
            return true;
        }
        loop {
            if !self.value() {
                return false;
            }
            if self.take(b']') {
                return true;
            }
            if !self.take(b',') {
                return false;
            }
        }
    }

    fn object(&mut self) -> bool {
        self.position += 1;
        if self.take(b'}') {
            return true;
        }
        let mut previous_key: Option<&[u8]> = None;
        loop {
            let Some(key) = self.string(true) else {
                return false;
            };
            if previous_key.is_some_and(|previous| previous >= key) {
                return false;
            }
            previous_key = Some(key);
            if !self.take(b':') || !self.value() {
                return false;
            }
            if self.take(b'}') {
                return true;
            }
            if !self.take(b',') {
                return false;
            }
        }
    }

    fn write_value(&mut self, output: &mut Vec<u8>) -> bool {
        let start = self.position;
        match self.peek() {
            Some(b'n') if self.literal(b"null") => output.extend_from_slice(b"null"),
            Some(b't') if self.literal(b"true") => output.extend_from_slice(b"true"),
            Some(b'f') if self.literal(b"false") => output.extend_from_slice(b"false"),
            Some(b'"') if self.string(false).is_some() => {
                output.extend_from_slice(&self.bytes[start..self.position]);
            }
            Some(b'[') => return self.write_array(output),
            Some(b'{') => return self.write_object(output),
            Some(b'-' | b'0'..=b'9') if self.integer() => {
                output.extend_from_slice(&self.bytes[start..self.position]);
            }
            _ => return false,
        }
        true
    }

    fn write_array(&mut self, output: &mut Vec<u8>) -> bool {
        self.with_container_depth(|parser| parser.write_array_inner(output))
    }

    fn write_array_inner(&mut self, output: &mut Vec<u8>) -> bool {
        self.position += 1;
        output.push(b'[');
        if self.take(b']') {
            output.push(b']');
            return true;
        }
        let mut first = true;
        loop {
            if !first {
                output.push(b',');
            }
            first = false;
            if !self.write_value(output) {
                return false;
            }
            if self.take(b']') {
                output.push(b']');
                return true;
            }
            if !self.take(b',') {
                return false;
            }
        }
    }

    fn write_object(&mut self, output: &mut Vec<u8>) -> bool {
        self.with_container_depth(|parser| parser.write_object_inner(output))
    }

    fn write_object_inner(&mut self, output: &mut Vec<u8>) -> bool {
        self.position += 1;
        if self.take(b'}') {
            output.extend_from_slice(b"{}");
            return true;
        }
        let mut members = smallvec::SmallVec::<[(&'a [u8], &'a [u8]); 8]>::new();
        loop {
            let Some(key) = self.string(true) else {
                return false;
            };
            if !self.take(b':') {
                return false;
            }
            let value_start = self.position;
            if !self.skip_value() {
                return false;
            }
            members.push((key, &self.bytes[value_start..self.position]));
            if self.take(b'}') {
                break;
            }
            if !self.take(b',') {
                return false;
            }
        }
        members.sort_unstable_by(|left, right| left.0.cmp(right.0));
        if members.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return false;
        }

        output.push(b'{');
        for (index, (key, value)) in members.into_iter().enumerate() {
            if index != 0 {
                output.push(b',');
            }
            output.push(b'"');
            output.extend_from_slice(key);
            output.extend_from_slice(b"\":");
            let mut value_parser = Self {
                bytes: value,
                position: 0,
                remaining_depth: self.remaining_depth,
            };
            if !value_parser.write_value(output) || value_parser.position != value.len() {
                return false;
            }
        }
        output.push(b'}');
        true
    }

    fn skip_value(&mut self) -> bool {
        match self.peek() {
            Some(b'n') => self.literal(b"null"),
            Some(b't') => self.literal(b"true"),
            Some(b'f') => self.literal(b"false"),
            Some(b'"') => self.string(false).is_some(),
            Some(b'[') => self.skip_array(),
            Some(b'{') => self.skip_object(),
            Some(b'-' | b'0'..=b'9') => self.integer(),
            _ => false,
        }
    }

    fn skip_array(&mut self) -> bool {
        self.with_container_depth(Self::skip_array_inner)
    }

    fn skip_array_inner(&mut self) -> bool {
        self.position += 1;
        if self.take(b']') {
            return true;
        }
        loop {
            if !self.skip_value() {
                return false;
            }
            if self.take(b']') {
                return true;
            }
            if !self.take(b',') {
                return false;
            }
        }
    }

    fn skip_object(&mut self) -> bool {
        self.with_container_depth(Self::skip_object_inner)
    }

    fn skip_object_inner(&mut self) -> bool {
        self.position += 1;
        if self.take(b'}') {
            return true;
        }
        loop {
            if self.string(true).is_none() || !self.take(b':') || !self.skip_value() {
                return false;
            }
            if self.take(b'}') {
                return true;
            }
            if !self.take(b',') {
                return false;
            }
        }
    }

    fn with_container_depth(&mut self, parse: impl FnOnce(&mut Self) -> bool) -> bool {
        let Some(remaining_depth) = self.remaining_depth.checked_sub(1) else {
            return false;
        };
        self.remaining_depth = remaining_depth;
        let result = parse(self);
        self.remaining_depth = self.remaining_depth.saturating_add(1);
        result
    }

    fn string(&mut self, reject_escapes: bool) -> Option<&'a [u8]> {
        if !self.take(b'"') {
            return None;
        }
        let start = self.position;
        while let Some(byte) = self.peek() {
            match byte {
                b'"' => {
                    let end = self.position;
                    self.position += 1;
                    return Some(&self.bytes[start..end]);
                }
                b'\\' => {
                    if reject_escapes {
                        return None;
                    }
                    self.position += 1;
                    match self.peek() {
                        Some(b'"' | b'\\' | b'b' | b'f' | b'n' | b'r' | b't') => {
                            self.position += 1;
                        }
                        // `\/` and `\uXXXX` are valid but serde_json emits a
                        // different compact spelling after decoding them.
                        _ => return None,
                    }
                }
                0x00..=0x1f => return None,
                _ => self.position += 1,
            }
        }
        None
    }

    fn integer(&mut self) -> bool {
        let start = self.position;
        if self.take(b'-') && !matches!(self.peek(), Some(b'1'..=b'9')) {
            return false;
        }
        if self.take(b'0') {
            if matches!(self.peek(), Some(b'0'..=b'9' | b'.' | b'e' | b'E')) {
                return false;
            }
        } else {
            let digit_start = self.position;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.position += 1;
            }
            if self.position == digit_start || matches!(self.peek(), Some(b'.' | b'e' | b'E')) {
                return false;
            }
        }
        let Ok(text) = std::str::from_utf8(&self.bytes[start..self.position]) else {
            return false;
        };
        if text.starts_with('-') {
            text.parse::<i64>().is_ok()
        } else {
            text.parse::<u64>().is_ok()
        }
    }

    fn literal(&mut self, literal: &[u8]) -> bool {
        if self.bytes.get(self.position..self.position + literal.len()) != Some(literal) {
            return false;
        }
        self.position += literal.len();
        true
    }

    fn take(&mut self, expected: u8) -> bool {
        if self.peek() != Some(expected) {
            return false;
        }
        self.position += 1;
        true
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.position).copied()
    }
}

fn append_direct_path_value_replacement_prepared_row(
    rows: &mut RawWriteBatch,
    spec: &EntitySurfaceSpec,
    candidate: EntityLiveRowRef<'_>,
    snapshot: TransactionJson,
) -> Result<(), LixError> {
    let metadata = inherited_metadata(candidate, spec)?;
    rows.push_parts(
        Some(candidate.entity_pk().clone()),
        spec.schema_key.as_str().into(),
        candidate.file_id().map(Into::into),
        Some(snapshot),
        metadata,
        None,
        None,
        None,
        candidate.global(),
        None,
        None,
        candidate.untracked(),
        if candidate.global() {
            crate::GLOBAL_BRANCH_ID.into()
        } else {
            candidate.branch_id().into()
        },
    );
    let row_index = rows.len() - 1;
    rows.set_durable_predecessor(row_index, candidate.durable_predecessor().cloned());
    Ok(())
}

fn bound_single_text_primary_key_param(
    spec: &EntitySurfaceSpec,
    predicate: &BoundPredicate,
) -> Option<usize> {
    let [path] = spec.primary_key_paths.as_slice() else {
        return None;
    };
    let [primary_key_column] = path.as_slice() else {
        return None;
    };
    spec.visible_column(primary_key_column)
        .filter(|column| column.column_type == EntityColumnType::String)?;
    let BoundPredicate::Eq(left, right) = predicate else {
        return None;
    };
    match (left, right) {
        (BoundExpr::Column(column), BoundExpr::Param(param))
        | (BoundExpr::Param(param), BoundExpr::Column(column))
            if column.name == *primary_key_column =>
        {
            param.index.checked_sub(1)
        }
        _ => None,
    }
}

fn with_parameter_batch_statement_index(mut error: LixError, statement_index: usize) -> LixError {
    let mut details = match error.details.take() {
        Some(JsonValue::Object(details)) => details,
        Some(details) => {
            let mut wrapped = serde_json::Map::new();
            wrapped.insert("cause".to_string(), details);
            wrapped
        }
        None => serde_json::Map::new(),
    };
    details.insert(
        "statementIndex".to_string(),
        JsonValue::from(statement_index),
    );
    error.details = Some(JsonValue::Object(details));
    error
}

pub(crate) async fn try_execute_bound_public_write(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    params: &[Value],
    metadata: &ExecuteStatementMetadata,
) -> Result<BoundPublicWriteExecution, LixError> {
    match &plan.bound.target {
        BoundWriteTarget::Entity(surface) if bound_public_write_shape_supported(plan) => {
            execute_entity_write(ctx, plan, surface, params)
                .await
                .map(BoundPublicWriteExecution::Executed)
        }
        BoundWriteTarget::File(surface) => {
            if let Some(shape) = fast_file_path_write_shape(plan, surface) {
                Ok(execute_file_path_write(ctx, plan, params, metadata, shape)
                    .await?
                    .map_or(BoundPublicWriteExecution::Unsupported, |count| {
                        BoundPublicWriteExecution::Executed(SqlWriteResult::affected(count))
                    }))
            } else if let Some(shape) = fast_file_content_update_shape(plan, surface) {
                execute_file_content_update(ctx, params, metadata, &shape)
                    .await
                    .map(SqlWriteResult::affected)
                    .map(BoundPublicWriteExecution::Executed)
            } else {
                Ok(BoundPublicWriteExecution::Unsupported)
            }
        }
        _ => Ok(BoundPublicWriteExecution::Unsupported),
    }
}

struct FastFileContentUpdateShape {
    id: BoundExpr,
    data: BoundExpr,
    metadata: Option<BoundExpr>,
    data_parameter_index: Option<usize>,
}

async fn execute_file_content_update(
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    metadata: &ExecuteStatementMetadata,
    shape: &FastFileContentUpdateShape,
) -> Result<u64, LixError> {
    let id = eval_fast_file_nullable_text(&shape.id, params, "id")?;
    let data = eval_fast_file_blob(&shape.data, params, "content")?;
    let splice_provenance = fast_file_content_update_splice_provenance(shape, metadata);
    if let Some(metadata_expr) = &shape.metadata {
        let row_metadata = eval_fast_file_metadata(metadata_expr, params)?;
        crate::sql2::providers::execute_fast_lix_file_content_update_by_id_with_metadata(
            ctx,
            id,
            data,
            row_metadata,
            splice_provenance,
            metadata.mutation_identity(),
        )
        .await
    } else {
        crate::sql2::providers::execute_fast_lix_file_content_update_by_id(
            ctx,
            id,
            data,
            splice_provenance,
            metadata.mutation_identity(),
        )
        .await
    }
}

fn fast_file_content_update_splice_provenance(
    shape: &FastFileContentUpdateShape,
    metadata: &ExecuteStatementMetadata,
) -> Option<RequestBlobSpliceProvenance> {
    shape
        .data_parameter_index
        .and_then(|index| metadata.blob_splice_for_parameter(index))
        .cloned()
}

fn fast_file_content_update_shape(
    plan: &LogicalWritePlan,
    surface: &FileWriteSurface,
) -> Option<FastFileContentUpdateShape> {
    if !matches!(surface, FileWriteSurface::Base)
        || plan.bound.op != BoundWriteOp::Update
        || !matches!(plan.bound.input, BoundWriteInput::None)
        || plan.bound.conflict.is_some()
        || plan.bound.returning.is_some()
        || !matches!(plan.bound.branch_scope, BranchScope::Active { .. })
        || !(1..=2).contains(&plan.bound.assignments.len())
    {
        return None;
    }
    let assignment = plan
        .bound
        .assignments
        .iter()
        .find(|assignment| assignment.column.name == "content")?;
    let metadata = plan
        .bound
        .assignments
        .iter()
        .find(|assignment| assignment.column.name == "lixcol_metadata")
        .map(|assignment| assignment.value.clone());
    if !fast_file_blob_expr_supported(&assignment.value)
        || metadata
            .as_ref()
            .is_some_and(|expr| !fast_file_metadata_expr_supported(expr))
        || plan.bound.assignments.iter().any(|assignment| {
            assignment.column.name != "content" && assignment.column.name != "lixcol_metadata"
        })
    {
        return None;
    }
    let id = fast_file_id_predicate_value(&plan.bound.predicate)?;
    Some(FastFileContentUpdateShape {
        id: id.clone(),
        data: assignment.value.clone(),
        metadata,
        data_parameter_index: match &assignment.value {
            BoundExpr::Param(param) => Some(param.index),
            _ => None,
        },
    })
}

fn fast_file_id_predicate_value(predicate: &BoundPredicate) -> Option<&BoundExpr> {
    let BoundPredicate::Eq(left, right) = predicate else {
        return None;
    };
    fast_file_id_column_value(left, right).or_else(|| fast_file_id_column_value(right, left))
}

fn fast_file_id_column_value<'a>(
    column_expr: &BoundExpr,
    value_expr: &'a BoundExpr,
) -> Option<&'a BoundExpr> {
    let BoundExpr::Column(column) = column_expr else {
        return None;
    };
    if column.name == "id" && fast_file_text_expr_supported(value_expr) {
        Some(value_expr)
    } else {
        None
    }
}

fn fast_file_text_expr_supported(expr: &BoundExpr) -> bool {
    matches!(
        expr,
        BoundExpr::Param(_) | BoundExpr::Literal(BoundLiteral::Text(_))
    )
}

fn fast_file_blob_expr_supported(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::Param(_) => true,
        BoundExpr::Cast {
            expr,
            data_type: BoundCastType::Binary,
        } => fast_file_text_expr_supported(expr),
        _ => false,
    }
}

async fn execute_entity_write(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    surface: &EntityWriteSurface,
    params: &[Value],
) -> Result<SqlWriteResult, LixError> {
    let schema_key = match surface {
        EntityWriteSurface::Base { schema_key } | EntityWriteSurface::ByBranch { schema_key } => {
            schema_key
        }
    };
    reject_read_only_entity_surface(schema_key, entity_action(&plan.bound.op))
        .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;

    if schema_key == "lix_registered_schema" && plan.bound.op == BoundWriteOp::Delete {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "delete lix_registered_schema is not supported",
        ));
    }

    let catalog = ctx.public_catalog()?;
    let spec = catalog.entity_spec(schema_key).ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("entity surface '{schema_key}' is not visible"),
        )
    })?;
    validate_bound_write_supported(plan, spec)?;
    // Only `lix_active_branch_commit_id()` needs the current branch head.
    // Normal entity mutations already stage against the transaction's active
    // branch, so eagerly opening another read here makes the common write
    // path pay for a value it never observes.
    let active_branch_commit_id = if plan_references_active_branch_commit_id(plan) {
        Some(load_active_branch_commit_id(ctx).await?)
    } else {
        None
    };
    let no_op = matches!(plan.bound.branch_scope, BranchScope::Empty)
        || matches!(plan.filters.rows, FilterSet::None);
    match plan.bound.op {
        BoundWriteOp::Insert => {
            if no_op {
                entity_insert_batch(ctx, plan, spec, params, active_branch_commit_id.as_ref())?;
                return Ok(empty_entity_returning_result(plan));
            }
            if plan.bound.conflict.is_some() {
                entity_upsert(ctx, plan, spec, params, active_branch_commit_id.as_ref()).await
            } else {
                entity_insert(ctx, plan, spec, params, active_branch_commit_id.as_ref()).await
            }
        }
        BoundWriteOp::Update => {
            if no_op {
                return Ok(empty_entity_returning_result(plan));
            }
            entity_update(ctx, plan, spec, params, active_branch_commit_id.as_ref()).await
        }
        BoundWriteOp::Delete => {
            if no_op {
                return Ok(empty_entity_returning_result(plan));
            }
            if matches!(surface, EntityWriteSurface::Base { .. })
                && matches!(plan.bound.predicate, BoundPredicate::True)
                && plan.bound.returning.is_none()
                && let Some(result) = entity_delete_collection(ctx, &spec).await?
            {
                return Ok(result);
            }
            entity_delete(ctx, plan, spec, params, active_branch_commit_id.as_ref()).await
        }
    }
}

async fn entity_delete_collection(
    ctx: &mut dyn SqlWriteExecutionContext,
    spec: &EntitySurfaceSpec,
) -> Result<Option<SqlWriteResult>, LixError> {
    use crate::collection_generation::{CollectionScopeRef, collection_delete_stage_row};

    let scope = CollectionScopeRef {
        schema_key: &spec.schema_key,
        file_id: None,
    };
    let active_branch_id = ctx.active_branch_id().to_string();
    let global = ctx
        .load_collection_generation(crate::GLOBAL_BRANCH_ID, scope)
        .await?;
    // A visible active-branch collection can shadow global rows with the same
    // identity. Per-branch counts cannot recover that union cardinality
    // exactly, so preserve the row-wise route until the projection itself has
    // a certified count.
    if global.is_some_and(|global| global.live_count != 0) {
        return Ok(None);
    }
    // A generation control only counts committed HOT members. Ordinary staged
    // rows can add, replace, or remove members from that count, so let the
    // row-wise executor resolve the exact transaction overlay whenever any are
    // present.
    if ctx.has_staged_collection_rows(&active_branch_id, scope)? {
        return Ok(None);
    }
    let Some(mut previous) = ctx
        .load_collection_generation(&active_branch_id, scope)
        .await?
    else {
        return Ok(None);
    };
    if previous.live_count == crate::collection_generation::DEFERRED_LIVE_COUNT {
        let Some(live_count) = ctx
            .load_exact_collection_live_count(&active_branch_id, scope)
            .await?
        else {
            return Ok(None);
        };
        previous.live_count = live_count;
    }
    if previous.live_count == 0 {
        return Ok(Some(SqlWriteResult::affected(0)));
    }
    let mut rows = RawWriteBatch::with_capacity(1);
    rows.push(collection_delete_stage_row(&active_branch_id, scope));
    stage_rows(ctx, TransactionWriteMode::Replace, rows).await?;
    Ok(Some(SqlWriteResult::affected(previous.live_count)))
}

fn plan_references_active_branch_commit_id(plan: &LogicalWritePlan) -> bool {
    let input_references_head = match &plan.bound.input {
        BoundWriteInput::Values(values) => values
            .rows
            .iter()
            .flatten()
            .any(bound_expr_references_active_branch_commit_id),
        // Query input does not use this executor today. Keep the old eager
        // behavior if a future supported shape reaches it without a complete
        // expression traversal for `BoundRead`.
        BoundWriteInput::Query { .. } => true,
        BoundWriteInput::None => false,
    };
    input_references_head
        || bound_predicate_references_active_branch_commit_id(&plan.bound.predicate)
        || plan
            .bound
            .assignments
            .iter()
            .any(|assignment| bound_expr_references_active_branch_commit_id(&assignment.value))
        || plan.bound.conflict.as_ref().is_some_and(|conflict| {
            conflict
                .action
                .assignments()
                .iter()
                .any(|assignment| bound_expr_references_active_branch_commit_id(&assignment.value))
        })
        || plan.bound.returning.as_ref().is_some_and(|returning| {
            returning
                .items
                .iter()
                .any(|item| bound_expr_references_active_branch_commit_id(&item.expr))
        })
}

fn bound_predicate_references_active_branch_commit_id(predicate: &BoundPredicate) -> bool {
    match predicate {
        BoundPredicate::True | BoundPredicate::False => false,
        BoundPredicate::And(predicates) | BoundPredicate::Or(predicates) => predicates
            .iter()
            .any(bound_predicate_references_active_branch_commit_id),
        BoundPredicate::Eq(left, right) => {
            bound_expr_references_active_branch_commit_id(left)
                || bound_expr_references_active_branch_commit_id(right)
        }
        BoundPredicate::Like { expr, pattern, .. } => {
            bound_expr_references_active_branch_commit_id(expr)
                || bound_expr_references_active_branch_commit_id(pattern)
        }
        BoundPredicate::IsNull(expr) | BoundPredicate::IsNotNull(expr) => {
            bound_expr_references_active_branch_commit_id(expr)
        }
        BoundPredicate::In { expr, values } => {
            bound_expr_references_active_branch_commit_id(expr)
                || values
                    .iter()
                    .any(bound_expr_references_active_branch_commit_id)
        }
    }
}

fn bound_expr_references_active_branch_commit_id(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::Function { name, args } => {
            (name == "lix_active_branch_commit_id" && args.is_empty())
                || args
                    .iter()
                    .any(bound_expr_references_active_branch_commit_id)
        }
        BoundExpr::Cast { expr, .. } => bound_expr_references_active_branch_commit_id(expr),
        BoundExpr::Binary { left, right, .. } => {
            bound_expr_references_active_branch_commit_id(left)
                || bound_expr_references_active_branch_commit_id(right)
        }
        BoundExpr::Column(_)
        | BoundExpr::ExcludedColumn(_)
        | BoundExpr::Param(_)
        | BoundExpr::Literal(_) => false,
    }
}

#[cfg(test)]
mod active_branch_commit_id_reference_tests {
    use super::*;
    use crate::sql2::bind::expr::BoundColumnRef;
    use crate::sql2::bind::write::{
        BoundParamMap, BoundWrite, BoundWriteInput, BoundWriteOp, BoundWriteTarget,
        EntityWriteSurface,
    };
    use crate::sql2::plan::branch_scope::BranchScope;
    use crate::sql2::plan::write::PlannedWriteFilters;

    #[test]
    fn detects_active_branch_commit_id_in_nested_write_expressions() {
        let plan = update_plan(
            BoundPredicate::True,
            BoundExpr::Function {
                name: "lix_json".to_string(),
                args: vec![active_branch_commit_id()],
            },
        );

        assert!(plan_references_active_branch_commit_id(&plan));
    }

    #[test]
    fn detects_active_branch_commit_id_in_predicates_but_ignores_other_functions() {
        let plan = update_plan(
            BoundPredicate::Eq(
                BoundExpr::Column(BoundColumnRef {
                    table: "json_pointer".to_string(),
                    column_id: 0,
                    name: "value".to_string(),
                }),
                active_branch_commit_id(),
            ),
            BoundExpr::Function {
                name: "lix_timestamp".to_string(),
                args: Vec::new(),
            },
        );

        assert!(plan_references_active_branch_commit_id(&plan));

        let no_head_plan = update_plan(
            BoundPredicate::True,
            BoundExpr::Function {
                name: "lix_timestamp".to_string(),
                args: Vec::new(),
            },
        );
        assert!(!plan_references_active_branch_commit_id(&no_head_plan));
    }

    fn active_branch_commit_id() -> BoundExpr {
        BoundExpr::Function {
            name: "lix_active_branch_commit_id".to_string(),
            args: Vec::new(),
        }
    }

    fn update_plan(predicate: BoundPredicate, assignment_value: BoundExpr) -> LogicalWritePlan {
        LogicalWritePlan {
            bound: BoundWrite {
                target: BoundWriteTarget::Entity(EntityWriteSurface::Base {
                    schema_key: "json_pointer".to_string(),
                }),
                op: BoundWriteOp::Update,
                input: BoundWriteInput::None,
                predicate,
                assignments: vec![BoundAssignment {
                    column: BoundColumnRef {
                        table: "json_pointer".to_string(),
                        column_id: 1,
                        name: "value".to_string(),
                    },
                    value: assignment_value,
                }],
                conflict: None,
                returning: None,
                params: BoundParamMap::default(),
                branch_scope: BranchScope::Active {
                    branch_id: "main".to_string(),
                },
            },
            filters: PlannedWriteFilters {
                rows: FilterSet::All,
            },
        }
    }
}

async fn load_active_branch_commit_id(
    ctx: &mut dyn SqlWriteExecutionContext,
) -> Result<CommitId, LixError> {
    let active_branch_id = ctx.active_branch_id().to_string();
    ctx.load_branch_head(&active_branch_id)
        .await?
        .ok_or_else(|| {
            LixError::branch_not_found(
                active_branch_id,
                "execute bound public write",
                "active branch",
            )
        })
}

#[derive(Clone, Copy)]
struct FastFilePathWriteShape {
    id_index: Option<usize>,
    path_index: usize,
    data_index: usize,
    metadata_index: Option<usize>,
    conflict: crate::sql2::providers::FastLixFilePathWriteConflict,
}

async fn execute_file_path_write(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    params: &[Value],
    metadata: &ExecuteStatementMetadata,
    shape: FastFilePathWriteShape,
) -> Result<Option<u64>, LixError> {
    let BoundWriteInput::Values(values) = &plan.bound.input else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "bound lix_file fast write supports VALUES only",
        ));
    };
    let mut writes = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        let data_expr = &row[shape.data_index];
        writes.push((
            shape
                .id_index
                .map(|index| eval_fast_file_text(&row[index], params, "id"))
                .transpose()?,
            eval_fast_file_text(&row[shape.path_index], params, "path")?,
            eval_fast_file_blob(data_expr, params, "content")?,
            shape
                .metadata_index
                .map(|index| eval_fast_file_metadata(&row[index], params))
                .transpose()?
                .flatten(),
            fast_file_blob_expr_splice_provenance(data_expr, metadata),
        ));
    }
    crate::sql2::providers::execute_fast_lix_file_id_path_writes(
        ctx,
        writes,
        shape.conflict,
        metadata.mutation_identity(),
    )
    .await
}

fn fast_file_blob_expr_splice_provenance(
    expr: &BoundExpr,
    metadata: &ExecuteStatementMetadata,
) -> Option<RequestBlobSpliceProvenance> {
    let BoundExpr::Param(param) = expr else {
        return None;
    };
    metadata.blob_splice_for_parameter(param.index).cloned()
}

fn fast_file_path_write_shape(
    plan: &LogicalWritePlan,
    surface: &FileWriteSurface,
) -> Option<FastFilePathWriteShape> {
    if !matches!(surface, FileWriteSurface::Base)
        || plan.bound.op != BoundWriteOp::Insert
        || plan.bound.returning.is_some()
    {
        return None;
    }
    let BoundWriteInput::Values(values) = &plan.bound.input else {
        return None;
    };
    if values.rows.is_empty() || !(2..=4).contains(&values.columns.len()) {
        return None;
    }
    let id_index = values.column_index("id");
    let path_index = values.column_index("path")?;
    let data_index = values.column_index("content")?;
    let metadata_index = values.column_index("lixcol_metadata");
    if values.columns.len()
        != 2 + usize::from(id_index.is_some()) + usize::from(metadata_index.is_some())
    {
        return None;
    }
    if values.rows.iter().any(|row| {
        row.len() != values.columns.len()
            || !fast_file_text_expr_supported(&row[path_index])
            || !fast_file_blob_expr_supported(&row[data_index])
            || id_index.is_some_and(|index| !fast_file_text_expr_supported(&row[index]))
            || metadata_index.is_some_and(|index| !fast_file_metadata_expr_supported(&row[index]))
    }) {
        return None;
    }
    let conflict = match &plan.bound.conflict {
        None => crate::sql2::providers::FastLixFilePathWriteConflict::None,
        Some(conflict) => fast_file_path_conflict_shape(conflict)?,
    };
    if conflict.updates_metadata() && metadata_index.is_none() {
        return None;
    }
    if conflict.updates_data_only() && metadata_index.is_some() {
        return None;
    }
    // DataFusion ignores insert values for rows skipped by DO NOTHING. Keep
    // metadata-bearing variants there so invalid metadata on an existing row
    // retains that behavior without complicating the hot upsert path.
    if matches!(
        conflict,
        crate::sql2::providers::FastLixFilePathWriteConflict::DoNothing
            | crate::sql2::providers::FastLixFilePathWriteConflict::IdDoNothing
    ) && metadata_index.is_some()
    {
        return None;
    }
    Some(FastFilePathWriteShape {
        id_index,
        path_index,
        data_index,
        metadata_index,
        conflict,
    })
}

fn fast_file_path_conflict_shape(
    conflict: &BoundInsertConflict,
) -> Option<crate::sql2::providers::FastLixFilePathWriteConflict> {
    if conflict.target_columns.len() != 1 {
        return None;
    }
    let by_id = conflict.target_columns[0].name == "id";
    if !by_id && conflict.target_columns[0].name != "path" {
        return None;
    }
    match &conflict.action {
        BoundConflictAction::DoNothing => Some(if by_id {
            crate::sql2::providers::FastLixFilePathWriteConflict::IdDoNothing
        } else {
            crate::sql2::providers::FastLixFilePathWriteConflict::DoNothing
        }),
        BoundConflictAction::DoUpdate { assignments } => {
            let assigns_excluded_column = |assignment: &BoundAssignment, name: &str| {
                assignment.column.name == name
                    && matches!(
                        &assignment.value,
                        BoundExpr::ExcludedColumn(column) if column.name == name
                    )
            };
            if assignments.len() == 1 && assigns_excluded_column(&assignments[0], "content") {
                return Some(if by_id {
                    crate::sql2::providers::FastLixFilePathWriteConflict::IdUpdateContent
                } else {
                    crate::sql2::providers::FastLixFilePathWriteConflict::UpdateContent
                });
            }
            if assignments.len() == 2
                && assignments
                    .iter()
                    .any(|assignment| assigns_excluded_column(assignment, "content"))
                && assignments
                    .iter()
                    .any(|assignment| assigns_excluded_column(assignment, "lixcol_metadata"))
            {
                return Some(if by_id {
                    crate::sql2::providers::FastLixFilePathWriteConflict::IdUpdateContentAndMetadata
                } else {
                    crate::sql2::providers::FastLixFilePathWriteConflict::UpdateContentAndMetadata
                });
            }
            None
        }
    }
}

fn fast_file_metadata_expr_supported(expr: &BoundExpr) -> bool {
    matches!(
        expr,
        BoundExpr::Param(_)
            | BoundExpr::Literal(
                BoundLiteral::Null | BoundLiteral::Text(_) | BoundLiteral::Json(_)
            )
    )
}

fn eval_fast_file_text(
    expr: &BoundExpr,
    params: &[Value],
    column: &str,
) -> Result<String, LixError> {
    match expr {
        BoundExpr::Literal(BoundLiteral::Text(value)) => Ok(value.clone()),
        BoundExpr::Param(param) => match params.get(param.index.saturating_sub(1)) {
            Some(Value::Text(value)) => Ok(value.clone()),
            Some(_) => Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("lix_file fast write column '{column}' expects text"),
            )),
            None => Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("missing SQL parameter ${}", param.index),
            )),
        },
        _ => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!("lix_file fast write column '{column}' supports params and literals only"),
        )),
    }
}

fn eval_fast_file_nullable_text(
    expr: &BoundExpr,
    params: &[Value],
    column: &str,
) -> Result<Option<String>, LixError> {
    if let BoundExpr::Param(param) = expr
        && matches!(params.get(param.index.saturating_sub(1)), Some(Value::Null))
    {
        return Ok(None);
    }
    eval_fast_file_text(expr, params, column).map(Some)
}

fn eval_fast_file_blob(
    expr: &BoundExpr,
    params: &[Value],
    column: &str,
) -> Result<crate::Blob, LixError> {
    match expr {
        BoundExpr::Param(param) => match params.get(param.index.saturating_sub(1)) {
            Some(Value::Blob(value)) => Ok(value.clone()),
            Some(_) => Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("lix_file fast write column '{column}' expects blob content"),
            )
            .with_hint(LIX_FILE_CONTENT_CAST_HINT)),
            None => Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("missing SQL parameter ${}", param.index),
            )),
        },
        BoundExpr::Cast {
            expr,
            data_type: BoundCastType::Binary,
        } => Ok(eval_fast_file_text(expr, params, column)?
            .into_bytes()
            .into()),
        _ => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!("lix_file fast write column '{column}' supports blob parameters only"),
        )),
    }
}

fn eval_fast_file_metadata(
    expr: &BoundExpr,
    params: &[Value],
) -> Result<Option<TransactionJson>, LixError> {
    let value = match expr {
        BoundExpr::Literal(BoundLiteral::Null) => return Ok(None),
        BoundExpr::Literal(BoundLiteral::Text(value)) => {
            parse_row_metadata_value(value, "lix_file")?
        }
        BoundExpr::Literal(BoundLiteral::Json(value)) => {
            validate_row_metadata(value, "lix_file")?;
            value.clone()
        }
        BoundExpr::Param(param) => match params.get(param.index.saturating_sub(1)) {
            Some(Value::Null) => return Ok(None),
            Some(Value::Text(value)) => parse_row_metadata_value(value, "lix_file")?,
            Some(Value::Json(value)) => {
                let value = value.to_value();
                validate_row_metadata(&value, "lix_file")?;
                value
            }
            Some(_) => {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "lix_file fast write column 'lixcol_metadata' expects a JSON object",
                ));
            }
            None => {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("missing SQL parameter ${}", param.index),
                ));
            }
        },
        _ => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "lix_file fast write column 'lixcol_metadata' supports params and literals only",
            ));
        }
    };
    TransactionJson::from_value(value, "lix_file metadata").map(Some)
}

async fn entity_insert(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<SqlWriteResult, LixError> {
    let write_rows = entity_insert_batch(ctx, plan, spec, params, active_branch_commit_id)?;
    stage_entity_rows_with_postimage_returning(
        ctx,
        plan,
        spec,
        params,
        active_branch_commit_id,
        TransactionWriteMode::Insert,
        write_rows,
    )
    .await
}

async fn entity_upsert(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<SqlWriteResult, LixError> {
    let conflict = plan.bound.conflict.as_ref().ok_or_else(|| {
        LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "entity upsert requires an INSERT ON CONFLICT clause",
        )
    })?;
    validate_insert_conflict_target(plan, spec, conflict)?;

    let mut insert_rows = entity_insert_batch(ctx, plan, spec, params, active_branch_commit_id)?;
    let candidates = scan_entity_conflict_candidates(ctx, spec, &insert_rows).await?;
    let mut write_rows = RawWriteBatch::with_capacity(insert_rows.len());

    for index in 0..insert_rows.len() {
        let insert_row = insert_rows.row(index);
        let inserted_entity_pk = insert_row_entity_pk(insert_row, spec)?;
        let matching_candidate =
            find_conflict_candidate(insert_row, &inserted_entity_pk, &candidates);
        match (matching_candidate, &conflict.action) {
            // DO NOTHING on a conflicting row: leave the existing row untouched.
            (Some(_), BoundConflictAction::DoNothing) => {}
            (Some(candidate), BoundConflictAction::DoUpdate { assignments }) => {
                append_entity_conflict_update_row(
                    &mut write_rows,
                    ctx,
                    spec,
                    candidate,
                    insert_row,
                    assignments.as_slice(),
                    params,
                    active_branch_commit_id,
                )?;
            }
            (None, _) => write_rows.append_taken_row(&mut insert_rows, index),
        }
    }

    stage_entity_rows_with_postimage_returning(
        ctx,
        plan,
        spec,
        params,
        active_branch_commit_id,
        TransactionWriteMode::Replace,
        write_rows,
    )
    .await
}

fn entity_insert_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<RawWriteBatch, LixError> {
    let BoundWriteInput::Values(values) = &plan.bound.input else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "bound entity INSERT supports VALUES only",
        ));
    };
    let layout = InsertRowLayout::from_values(spec, values)?;
    if let Some(rows) = certified_entity_insert_batch(
        ctx,
        plan,
        spec,
        &layout,
        values,
        params,
        active_branch_commit_id,
    )? {
        return Ok(rows);
    }
    let mut write_rows = RawWriteBatch::with_capacity(values.rows.len());
    for row in &values.rows {
        append_entity_insert_row(
            &mut write_rows,
            ctx,
            plan,
            spec,
            &layout,
            row,
            params,
            active_branch_commit_id,
        )?;
    }
    Ok(write_rows)
}

async fn entity_update(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<SqlWriteResult, LixError> {
    if let Some(result) = try_execute_direct_path_value_replacement(ctx, plan, spec, params).await?
    {
        return Ok(result);
    }
    let constraints_unchanged = update_assignments_preserve_constraints(ctx, plan, spec);
    let candidates = scan_entity_candidates(ctx, plan, spec, params).await?;
    let mut write_rows = RawWriteBatch::with_capacity(candidates.len());
    for candidate in candidates.iter() {
        let appended = append_entity_update_row(
            &mut write_rows,
            ctx,
            plan,
            spec,
            candidate,
            params,
            active_branch_commit_id,
        )?;
        if appended && constraints_unchanged {
            write_rows.mark_last_constraints_unchanged();
        }
    }
    stage_entity_rows_with_postimage_returning(
        ctx,
        plan,
        spec,
        params,
        active_branch_commit_id,
        TransactionWriteMode::Replace,
        write_rows,
    )
    .await
}

/// Stages one ordinary `json_pointer(path, value)` point replacement through
/// the same canonical certificate used by dense parameter batches.
///
/// Explicit SQL transactions commonly issue one UPDATE per row because each
/// call must report its affected-row count immediately. The generic entity
/// route re-enters plugin reconciliation, schema normalization, and constraint
/// preparation for every such call even though this exact schema and plan
/// prove those passes redundant. A visible-row lookup still preserves
/// statement ordering and transaction-overlay semantics; only ordinary
/// tracked, branch-local, unfiled rows without metadata take the certificate.
async fn try_execute_direct_path_value_replacement(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
) -> Result<Option<SqlWriteResult>, LixError> {
    let Some(program) = prepare_path_value_replacement_program(ctx, plan, spec) else {
        return Ok(None);
    };
    execute_prepared_path_value_replacement(ctx, &program, params)
        .await
        .map(Some)
}

/// Immutable admission program for the ordinary tracked `path/value` point
/// replacement. The generic logical plan proves this shape once; explicit
/// transactions can then reuse these bound slots without reparsing, cloning,
/// resolving, and descending through the write executor for every row.
#[derive(Debug)]
pub(crate) struct PreparedPathValueReplacementProgram {
    pub(crate) schema_key: String,
    pub(crate) schema_plan_id: SchemaPlanId,
    primary_key_param_index: usize,
    value_param_index: usize,
}

pub(crate) struct PreparedPathValueReplacementRow {
    pub(crate) entity_pk: EntityPk,
    pub(crate) snapshot: TransactionJson,
}

impl PreparedPathValueReplacementProgram {
    pub(crate) fn parameter_count(&self) -> usize {
        self.primary_key_param_index
            .max(self.value_param_index)
            .saturating_add(1)
    }

    pub(crate) fn primary_key<'a>(&self, params: &'a [Value]) -> Result<&'a str, LixError> {
        let Some(Value::Text(primary_key)) = params.get(self.primary_key_param_index) else {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "prepared path replacement primary key must be text",
            ));
        };
        Ok(primary_key)
    }

    pub(crate) fn primary_key_text<'a>(
        &self,
        params: &'a [impl AsRef<str>],
    ) -> Result<&'a str, LixError> {
        params
            .get(self.primary_key_param_index)
            .map(AsRef::as_ref)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "prepared path replacement primary key text is missing",
                )
            })
    }

    pub(crate) fn replacement_value_text<'a>(
        &self,
        params: &'a [impl AsRef<str>],
    ) -> Result<&'a str, LixError> {
        params
            .get(self.value_param_index)
            .map(AsRef::as_ref)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "prepared path replacement value text is missing",
                )
            })
    }
}

pub(crate) fn prepare_path_value_replacement_program_from_logical(
    ctx: &dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
) -> Option<PreparedPathValueReplacementProgram> {
    let BoundWriteTarget::Entity(surface) = &plan.bound.target else {
        return None;
    };
    let schema_key = match surface {
        EntityWriteSurface::Base { schema_key } | EntityWriteSurface::ByBranch { schema_key } => {
            schema_key
        }
    };
    let catalog = ctx.public_catalog().ok()?;
    let spec = catalog.entity_spec(schema_key)?;
    prepare_path_value_replacement_program(ctx, plan, spec)
}

pub(crate) fn prepare_path_value_replacement_program(
    ctx: &dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
) -> Option<PreparedPathValueReplacementProgram> {
    if spec.has_inter_row_constraints
        || !matches!(plan.bound.input, BoundWriteInput::None)
        || plan.bound.conflict.is_some()
        || plan.bound.returning.is_some()
        || !matches!(plan.bound.branch_scope, BranchScope::Active { .. })
        || !matches!(plan.filters.rows, FilterSet::All)
        || plan_references_active_branch_commit_id(plan)
        || plan.bound.assignments.iter().any(|assignment| {
            spec.primary_key_paths
                .iter()
                .any(|path| path.as_slice() == [assignment.column.name.as_str()])
        })
    {
        return None;
    }
    let Some(primary_key_param_index) =
        bound_single_text_primary_key_param(spec, &plan.bound.predicate)
    else {
        return None;
    };
    let Some(replacement) =
        direct_path_value_replacement(spec, plan, Some(primary_key_param_index))
    else {
        return None;
    };
    let Some(schema_catalog) = ctx.schema_catalog_snapshot() else {
        return None;
    };
    let Some((schema_plan_id, schema_plan)) = schema_catalog.plan_for_key(&spec.schema_key) else {
        return None;
    };
    if !schema_plan.accepts_canonical_certificate() {
        return None;
    }

    Some(PreparedPathValueReplacementProgram {
        schema_key: spec.schema_key.clone(),
        schema_plan_id,
        primary_key_param_index,
        value_param_index: replacement.value_param_index,
    })
}

pub(crate) async fn execute_prepared_path_value_replacement(
    ctx: &mut dyn SqlWriteExecutionContext,
    program: &PreparedPathValueReplacementProgram,
    params: &[Value],
) -> Result<SqlWriteResult, LixError> {
    let Some(row) = prepare_path_value_replacement_row(ctx, program, params).await? else {
        return Ok(SqlWriteResult::affected(0));
    };
    let rows = CertifiedParameterReplacementBatch::new(
        vec![row.entity_pk],
        vec![row.snapshot],
        program.schema_key.as_str().into(),
        ctx.active_branch_id().into(),
        CertifiedRawWriteBatchPreparation {
            schema_plan_id: program.schema_plan_id,
            facts: PreparedRowFacts {
                row_content_validated: true,
                requires_transaction_validation: false,
            },
            tracked_keys_strictly_ordered: true,
            complete_collection_replacement: None,
        },
    )?;
    ctx.stage_certified_parameter_batch_replace(rows).await?;
    Ok(SqlWriteResult::affected(1))
}

pub(crate) async fn prepare_path_value_replacement_row(
    ctx: &mut dyn SqlWriteExecutionContext,
    program: &PreparedPathValueReplacementProgram,
    params: &[Value],
) -> Result<Option<PreparedPathValueReplacementRow>, LixError> {
    let primary_key = program.primary_key(params)?;
    let entity_pk = EntityPk::single(primary_key.to_owned());
    let candidates = ctx
        .scan_hot_state_batch(&HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![program.schema_key.clone()],
                entity_pks: vec![entity_pk.clone()],
                branch_ids: vec![ctx.active_branch_id().to_owned()],
                include_tombstones: false,
                ..HotStateFilter::default()
            },
            ..HotStateScanRequest::default()
        })
        .await?;
    if candidates.is_empty() {
        return Ok(None);
    }
    if candidates.len() != 1 {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "prepared path replacement resolved multiple visible rows",
        ));
    }
    let candidate = candidates.row(0);
    if candidate.untracked()
        || candidate.global()
        || candidate.file_id().is_some()
        || candidate.metadata().is_some()
        || candidate.branch_id() != ctx.active_branch_id()
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "prepared path replacement escaped its certified storage scope",
        ));
    }

    prepare_path_value_replacement_row_known_live(program, params).map(Some)
}

pub(crate) fn prepare_path_value_replacement_row_known_live(
    program: &PreparedPathValueReplacementProgram,
    params: &[Value],
) -> Result<PreparedPathValueReplacementRow, LixError> {
    let primary_key = program.primary_key(params)?;
    let entity_pk = EntityPk::single(primary_key.to_owned());
    let mut normalized = Vec::with_capacity(primary_key.len().saturating_add(32));
    let (start, end) =
        append_path_value_replacement_snapshot(program, primary_key, params, &mut normalized)?;
    // SAFETY: `append_path_value_replacement_snapshot` appends only UTF-8
    // literals, `str` identities, and canonical JSON parameter text.
    let snapshot = unsafe {
        TransactionJson::from_validated_certified_row_content_arena(normalized, vec![(start, end)])?
    }
    .pop()
    .expect("one certified replacement snapshot");
    Ok(PreparedPathValueReplacementRow {
        entity_pk,
        snapshot,
    })
}

pub(crate) fn append_path_value_replacement_snapshot(
    program: &PreparedPathValueReplacementProgram,
    primary_key: &str,
    params: &[Value],
    normalized: &mut Vec<u8>,
) -> Result<(usize, usize), LixError> {
    let replacement_value = match params.get(program.value_param_index) {
        Some(Value::Text(value)) => Some(value.as_str()),
        Some(Value::Null) => None,
        _ => {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "prepared path replacement value must be text or null",
            ));
        }
    };
    append_path_value_replacement_snapshot_text(primary_key, replacement_value, normalized)
}

pub(crate) fn append_path_value_replacement_snapshot_text(
    primary_key: &str,
    replacement_value: Option<&str>,
    normalized: &mut Vec<u8>,
) -> Result<(usize, usize), LixError> {
    let start = normalized.len();
    normalized.extend_from_slice(b"{\"path\":");
    if let Err(error) = append_canonical_json_string(normalized, primary_key) {
        normalized.truncate(start);
        return Err(error);
    }
    normalized.extend_from_slice(b",\"value\":");
    let result = match replacement_value {
        None => normalized.extend_from_slice(b"null"),
        Some(raw) => {
            if let Err(error) = append_canonical_json_parameter(normalized, raw) {
                normalized.truncate(start);
                return Err(error);
            }
        }
    };
    let () = result;
    normalized.push(b'}');
    #[cfg(test)]
    CERTIFIED_SINGLE_PATH_VALUE_REPLACEMENTS.with(|executions| {
        executions.set(executions.get().saturating_add(1));
    });
    Ok((start, normalized.len()))
}

/// Stage entity INSERT/UPDATE rows and, when requested, retain their final
/// write images for `RETURNING`.  The post-image is evaluated from the
/// normalized write row rather than the input expression so schema defaults
/// (including generated IDs) and conflict updates are visible to callers.
///
/// Evaluation happens before staging because staging consumes the batch, but
/// the query result is only constructed after the write is accepted. This
/// matches the existing DELETE path's all-or-error behavior while avoiding a
/// second lookup through the transaction overlay.
async fn stage_entity_rows_with_postimage_returning(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
    mode: TransactionWriteMode,
    write_rows: RawWriteBatch,
) -> Result<SqlWriteResult, LixError> {
    let returning_requires_staged_postimage =
        plan.bound.returning.as_ref().is_some_and(|returning| {
            returning
                .items
                .iter()
                .any(|item| returning_expr_requires_staged_postimage(&item.expr))
        });
    let returning_rows = if returning_requires_staged_postimage {
        None
    } else {
        entity_postimage_returning_rows(
            plan,
            spec,
            ctx,
            params,
            active_branch_commit_id,
            &write_rows,
        )?
    };
    // Transaction staging materializes audit fields such as the change and
    // commit IDs. Keep the direct write rows only for projections that need
    // those fields, then read their exact transaction-overlay post-images in
    // source order after staging succeeds.
    let staged_postimage_rows = returning_requires_staged_postimage.then(|| write_rows.clone());
    let rows_affected = stage_rows(ctx, mode, write_rows).await?;
    let returning_rows = match staged_postimage_rows {
        Some(write_rows) => {
            entity_staged_postimage_returning_rows(
                plan,
                spec,
                ctx,
                params,
                active_branch_commit_id,
                &write_rows,
            )
            .await?
        }
        None => returning_rows,
    };
    Ok(entity_returning_result(plan, rows_affected, returning_rows))
}

fn entity_postimage_returning_rows(
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
    write_rows: &RawWriteBatch,
) -> Result<Option<Vec<Vec<Value>>>, LixError> {
    let Some(returning) = plan.bound.returning.as_ref() else {
        return Ok(None);
    };
    let mut rows = Vec::with_capacity(write_rows.len());
    for row in write_rows.iter() {
        // Certified parameter batches intentionally retain only canonical
        // bytes. Decoding through `TransactionJson::value()` would panic for
        // those rows, so materialize the normalized representation here.
        let snapshot = row
            .snapshot
            .map(|snapshot| transaction_json_returning_value(snapshot, "entity post-image"))
            .transpose()?
            .unwrap_or(JsonValue::Null);
        let context = EntityEvalContext::staged(&snapshot, row, spec);
        rows.push(entity_returning_row(
            returning,
            &context,
            spec,
            ctx,
            params,
            active_branch_commit_id,
        )?);
    }
    Ok(Some(rows))
}

fn transaction_json_returning_value(
    value: &TransactionJson,
    context: &str,
) -> Result<JsonValue, LixError> {
    serde_json::from_str(value.normalized()).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("{context} contains invalid normalized JSON: {error}"),
        )
    })
}

async fn entity_staged_postimage_returning_rows(
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
    write_rows: &RawWriteBatch,
) -> Result<Option<Vec<Vec<Value>>>, LixError> {
    let Some(returning) = plan.bound.returning.as_ref() else {
        return Ok(None);
    };
    if write_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let candidates = scan_entity_conflict_candidates(ctx, spec, write_rows).await?;
    // A staged audit projection needs the transaction-visible row, but it
    // must not look through every candidate again for every write row. Aside
    // from making large `RETURNING *` writes quadratic, that repeated search
    // hid the fact that the match is a stable physical identity. Index once
    // and retain the previous ambiguity check.
    let mut candidates_by_identity = std::collections::HashMap::with_capacity(candidates.len());
    for candidate in candidates.iter() {
        let identity = entity_live_returning_identity(candidate);
        if candidates_by_identity.insert(identity, candidate).is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "staged entity post-image for schema '{}' is ambiguous in the transaction overlay",
                    spec.schema_key
                ),
            ));
        }
    }
    let mut rows = Vec::with_capacity(write_rows.len());
    for write_row in write_rows.iter() {
        let identity = entity_staged_returning_identity(write_row, spec)?;
        let candidate = candidates_by_identity.get(&identity).copied().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "staged entity post-image for schema '{}' is missing from the transaction overlay",
                    spec.schema_key
                ),
            )
        })?;
        let snapshot = candidate_snapshot(candidate)?.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "staged entity post-image for schema '{}' is unexpectedly a tombstone",
                    spec.schema_key
                ),
            )
        })?;
        let context = EntityEvalContext::live(&snapshot, candidate, spec);
        rows.push(entity_returning_row(
            returning,
            &context,
            spec,
            ctx,
            params,
            active_branch_commit_id,
        )?);
    }
    Ok(Some(rows))
}

type EntityReturningIdentity = (EntityPk, Option<String>, String, bool);

fn entity_staged_returning_identity(
    row: RawWriteRowRef<'_>,
    spec: &EntitySurfaceSpec,
) -> Result<EntityReturningIdentity, LixError> {
    Ok((
        insert_row_entity_pk(row, spec)?,
        row.file_id.map(|file_id| file_id.as_str().to_owned()),
        row.branch_id.as_str().to_owned(),
        row.global,
    ))
}

fn entity_live_returning_identity(row: MaterializedHotStateRowRef<'_>) -> EntityReturningIdentity {
    (
        row.entity_pk().clone(),
        row.file_id().map(ToOwned::to_owned),
        row.branch_id().to_owned(),
        row.global(),
    )
}

fn entity_returning_result(
    plan: &LogicalWritePlan,
    rows_affected: u64,
    rows: Option<Vec<Vec<Value>>>,
) -> SqlWriteResult {
    match (plan.bound.returning.as_ref(), rows) {
        (Some(returning), Some(rows)) => SqlWriteResult::returning(
            rows_affected,
            crate::SqlQueryResult {
                columns: returning
                    .items
                    .iter()
                    .map(|item| item.output_name.clone())
                    .collect(),
                rows,
                notices: Vec::new(),
            },
        ),
        _ => SqlWriteResult::affected(rows_affected),
    }
}

fn update_assignments_preserve_constraints(
    ctx: &dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
) -> bool {
    let Some(schema_catalog) = ctx.schema_catalog_snapshot() else {
        return false;
    };
    let Some((_, schema_plan)) = schema_catalog.plan_for_key(&spec.schema_key) else {
        return false;
    };
    let assigned = plan
        .bound
        .assignments
        .iter()
        .map(|assignment| assignment.column.name.as_str())
        .collect::<std::collections::HashSet<_>>();
    assigned_columns_preserve_constraints(schema_plan, &assigned)
}

fn assigned_columns_preserve_constraints(
    schema_plan: &crate::catalog::SchemaPlan,
    assigned: &std::collections::HashSet<&str>,
) -> bool {
    let touches = |path: &[String]| {
        path.first()
            .is_some_and(|property| assigned.contains(property.as_str()))
    };
    !schema_plan
        .primary_key
        .iter()
        .flatten()
        .any(|path| touches(path))
        && !schema_plan
            .uniques
            .iter()
            .flatten()
            .any(|path| touches(path))
        && !schema_plan
            .foreign_keys
            .iter()
            .flat_map(|foreign_key| &foreign_key.local_properties)
            .any(|path| touches(path))
}

fn append_entity_update_row<'a>(
    rows: &mut RawWriteBatch,
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    candidate: impl Into<EntityLiveRowRef<'a>>,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<bool, LixError> {
    let candidate = candidate.into();
    let Some(snapshot) = candidate_snapshot(candidate)? else {
        return Ok(false);
    };
    let original_context = EntityEvalContext::live(&snapshot, candidate, spec);
    if !predicate_matches(
        &plan.bound.predicate,
        &original_context,
        spec,
        ctx,
        params,
        active_branch_commit_id,
    )? {
        return Ok(false);
    }
    reject_projected_global_write(plan, candidate, "UPDATE")?;
    let mut updated = snapshot.clone();
    let mut visible_assignments = Vec::new();
    for assignment in &plan.bound.assignments {
        if let Some(column) = spec.visible_column(&assignment.column.name) {
            reject_direct_blob_json_value(&assignment.value, column.column_type, params)?;
            let value = eval_expr_value(
                &assignment.value,
                &original_context,
                ctx,
                params,
                active_branch_commit_id,
            )?;
            visible_assignments.push((
                column.name.clone(),
                entity_json_value(
                    &assignment.value,
                    value,
                    column.column_type,
                    &spec.schema_key,
                    &column.name,
                )?,
            ));
        } else if assignment.column.name == "lixcol_metadata" {
            // handled below from the assignment list
        } else {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                format!(
                    "bound entity UPDATE does not support assignment to '{}'",
                    assignment.column.name
                ),
            ));
        }
    }
    for (column_name, value) in visible_assignments {
        updated[&column_name] = value;
    }
    append_entity_replace_row_from_live(
        rows,
        ctx,
        spec,
        candidate,
        Some(updated),
        plan.bound.assignments.as_slice(),
        params,
        active_branch_commit_id,
    )?;
    Ok(true)
}

async fn entity_delete(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<SqlWriteResult, LixError> {
    let candidates = scan_entity_candidates(ctx, plan, spec, params).await?;
    let mut write_rows = RawWriteBatch::with_capacity(candidates.len());
    let mut returning_rows = plan.bound.returning.as_ref().map(|_| Vec::new());
    for candidate in candidates.iter() {
        let Some(snapshot) = candidate_snapshot(candidate)? else {
            continue;
        };
        let context = EntityEvalContext::live(&snapshot, candidate, spec);
        if predicate_matches(
            &plan.bound.predicate,
            &context,
            spec,
            ctx,
            params,
            active_branch_commit_id,
        )? {
            reject_projected_global_write(plan, candidate, "DELETE")?;
            if let (Some(returning), Some(rows)) =
                (plan.bound.returning.as_ref(), returning_rows.as_mut())
            {
                rows.push(entity_returning_row(
                    returning,
                    &context,
                    spec,
                    ctx,
                    params,
                    active_branch_commit_id,
                )?);
            }
            append_entity_replace_row_from_live(
                &mut write_rows,
                ctx,
                spec,
                candidate,
                None,
                plan.bound.assignments.as_slice(),
                params,
                active_branch_commit_id,
            )?;
        }
    }
    let rows_affected = stage_rows(ctx, TransactionWriteMode::Replace, write_rows).await?;
    match (plan.bound.returning.as_ref(), returning_rows) {
        (Some(returning), Some(rows)) => Ok(SqlWriteResult::returning(
            rows_affected,
            crate::SqlQueryResult {
                columns: returning
                    .items
                    .iter()
                    .map(|item| item.output_name.clone())
                    .collect(),
                rows,
                notices: Vec::new(),
            },
        )),
        _ => Ok(SqlWriteResult::affected(rows_affected)),
    }
}

fn empty_entity_returning_result(plan: &LogicalWritePlan) -> SqlWriteResult {
    entity_returning_result(plan, 0, plan.bound.returning.as_ref().map(|_| Vec::new()))
}

fn entity_returning_row(
    returning: &crate::sql2::bind::write::BoundReturning,
    context: &EntityEvalContext<'_>,
    spec: &EntitySurfaceSpec,
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<Vec<Value>, LixError> {
    returning
        .items
        .iter()
        .map(|item| {
            entity_returning_value(
                &item.expr,
                context,
                spec,
                ctx,
                params,
                active_branch_commit_id,
            )
        })
        .collect()
}

fn entity_returning_value(
    expr: &BoundExpr,
    context: &EntityEvalContext<'_>,
    spec: &EntitySurfaceSpec,
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<Value, LixError> {
    match expr {
        BoundExpr::Param(param)
            if params
                .get(param.index.saturating_sub(1))
                .is_some_and(|value| matches!(value, Value::Blob(_))) =>
        {
            let Value::Blob(value) = params
                .get(param.index.saturating_sub(1))
                .expect("checked SQL parameter exists")
            else {
                unreachable!("checked SQL parameter is a blob");
            };
            return Ok(Value::Blob(value.clone()));
        }
        _ => {}
    }

    let value = eval_expr_value(expr, context, ctx, params, active_branch_commit_id)?;
    if bound_expr_is_json(expr, spec) {
        return Ok(match value {
            EntityEvalValue::SqlNull => Value::Null,
            EntityEvalValue::Json(JsonValue::Null)
                if visible_entity_column(expr, spec)
                    .is_some_and(|column| column.column_type == EntityColumnType::Json) =>
            {
                Value::Null
            }
            EntityEvalValue::SqlText(value) => Value::Text(value),
            EntityEvalValue::Json(value) => Value::Json(value.into()),
        });
    }
    if let Some(column) = visible_entity_column(expr, spec) {
        if column.column_type == EntityColumnType::Integer {
            let value = value.into_json();
            return json_bigint_value(Some(&value), &spec.schema_key, &column.name)
                .map(|value| value.map_or(Value::Null, Value::Integer));
        }
        if column.column_type == EntityColumnType::Number {
            let value = value.into_json();
            return json_double_value(Some(&value), &spec.schema_key, &column.name)
                .map(|value| value.map_or(Value::Null, Value::Real));
        }
    }
    Ok(match value {
        EntityEvalValue::SqlNull | EntityEvalValue::Json(JsonValue::Null) => Value::Null,
        EntityEvalValue::SqlText(value) | EntityEvalValue::Json(JsonValue::String(value)) => {
            Value::Text(value)
        }
        EntityEvalValue::Json(JsonValue::Bool(value)) => Value::Boolean(value),
        EntityEvalValue::Json(JsonValue::Number(value)) => value
            .as_i64()
            .map(Value::Integer)
            .or_else(|| value.as_f64().map(Value::Real))
            .unwrap_or_else(|| Value::Text(value.to_string())),
        EntityEvalValue::Json(value @ (JsonValue::Array(_) | JsonValue::Object(_))) => {
            Value::Json(value.into())
        }
    })
}

fn visible_entity_column<'a>(
    expr: &BoundExpr,
    spec: &'a EntitySurfaceSpec,
) -> Option<&'a EntitySurfaceColumn> {
    let (BoundExpr::Column(column) | BoundExpr::ExcludedColumn(column)) = expr else {
        return None;
    };
    spec.visible_column(&column.name)
}

fn append_entity_conflict_update_row<'a>(
    rows: &mut RawWriteBatch,
    ctx: &mut dyn SqlWriteExecutionContext,
    spec: &EntitySurfaceSpec,
    candidate: impl Into<EntityLiveRowRef<'a>>,
    insert_row: RawWriteRowRef<'_>,
    assignments: &[BoundAssignment],
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<(), LixError> {
    let candidate = candidate.into();
    let snapshot = candidate_snapshot(candidate)?.ok_or_else(|| {
        LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "INSERT ON CONFLICT cannot update a tombstone row",
        )
    })?;
    let insert_snapshot = insert_row
        .snapshot
        .map(TransactionJson::value)
        .unwrap_or(&JsonValue::Null);
    let context =
        EntityEvalContext::conflict(&snapshot, candidate, insert_snapshot, insert_row, spec);
    let mut updated = snapshot.clone();
    let mut visible_assignments = Vec::new();
    for assignment in assignments {
        if let Some(column) = spec.visible_column(&assignment.column.name) {
            reject_direct_blob_json_value(&assignment.value, column.column_type, params)?;
            let value = eval_expr_value(
                &assignment.value,
                &context,
                ctx,
                params,
                active_branch_commit_id,
            )?;
            visible_assignments.push((
                column.name.clone(),
                entity_json_value(
                    &assignment.value,
                    value,
                    column.column_type,
                    &spec.schema_key,
                    &column.name,
                )?,
            ));
        } else if assignment.column.name == "lixcol_metadata" {
            // handled by append_entity_replace_row_from_live from the assignment list
        } else {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                format!(
                    "bound entity INSERT ON CONFLICT does not support assignment to '{}'",
                    assignment.column.name
                ),
            ));
        }
    }
    for (column_name, value) in visible_assignments {
        updated[&column_name] = value;
    }

    append_entity_replace_row_from_live(
        rows,
        ctx,
        spec,
        candidate,
        Some(updated),
        assignments,
        params,
        active_branch_commit_id,
    )
}

async fn stage_rows(
    ctx: &mut dyn SqlWriteExecutionContext,
    mode: TransactionWriteMode,
    rows: RawWriteBatch,
) -> Result<u64, LixError> {
    if rows.len() == 0 {
        return Ok(0);
    }
    let outcome = ctx
        .stage_write(TransactionWrite::Rows { mode, rows })
        .await?;
    Ok(outcome.count)
}

fn validate_insert_conflict_target(
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    conflict: &BoundInsertConflict,
) -> Result<(), LixError> {
    if spec.primary_key_paths.is_empty() {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "INSERT ON CONFLICT requires a schema primary key",
        ));
    }

    let mut expected = spec
        .primary_key_paths
        .iter()
        .map(|path| {
            if path.len() != 1 {
                return Err(LixError::new(
                    LixError::CODE_UNSUPPORTED_SQL,
                    "INSERT ON CONFLICT supports top-level primary-key columns only",
                ));
            }
            Ok(path[0].clone())
        })
        .collect::<Result<std::collections::BTreeSet<_>, LixError>>()?;
    if matches!(
        plan.bound.target,
        BoundWriteTarget::Entity(EntityWriteSurface::ByBranch { .. })
    ) {
        expected.insert("lixcol_branch_id".to_string());
    }

    let actual = conflict
        .target_columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<std::collections::BTreeSet<_>>();
    if actual != expected {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!(
                "INSERT ON CONFLICT target must match entity identity columns ({})",
                expected.into_iter().collect::<Vec<_>>().join(", ")
            ),
        ));
    }
    Ok(())
}

fn insert_row_entity_pk(
    row: RawWriteRowRef<'_>,
    spec: &EntitySurfaceSpec,
) -> Result<EntityPk, LixError> {
    if let Some(entity_pk) = row.entity_pk {
        return Ok(entity_pk.clone());
    }
    let snapshot = row.snapshot.ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "INSERT ON CONFLICT for schema '{}' requires snapshot_content",
                spec.schema_key
            ),
        )
    })?;
    EntityPk::from_primary_key_paths(snapshot.value(), &spec.primary_key_paths).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "INSERT ON CONFLICT failed to derive entity primary key for schema '{}': {error}",
                spec.schema_key
            ),
        )
    })
}

fn find_conflict_candidate<'a>(
    insert_row: RawWriteRowRef<'_>,
    inserted_entity_pk: &EntityPk,
    candidates: &'a MaterializedHotStateBatch,
) -> Option<MaterializedHotStateRowRef<'a>> {
    candidates.iter().find(|candidate| {
        candidate_matches_insert_identity(*candidate, insert_row, inserted_entity_pk)
    })
}

fn candidate_matches_insert_identity<'a>(
    candidate: impl Into<EntityLiveRowRef<'a>>,
    insert_row: RawWriteRowRef<'_>,
    inserted_entity_pk: &EntityPk,
) -> bool {
    let candidate = candidate.into();
    candidate.entity_pk() == inserted_entity_pk
        && candidate.file_id() == insert_row.file_id.map(SharedStr::as_str)
        && candidate.branch_id() == insert_row.branch_id.as_str()
        && candidate.global() == insert_row.global
}

async fn scan_entity_conflict_candidates(
    ctx: &mut dyn SqlWriteExecutionContext,
    spec: &EntitySurfaceSpec,
    insert_rows: &RawWriteBatch,
) -> Result<MaterializedHotStateBatch, LixError> {
    #[cfg(feature = "storage-benches")]
    let _phase =
        crate::storage_bench::enter_crud_phase(crate::storage_bench::CRUD_PHASE_WRITE_READ);
    let mut branch_ids = std::collections::BTreeSet::new();
    let mut entity_pks = std::collections::BTreeSet::new();
    let mut file_ids = std::collections::BTreeSet::new();
    for row in insert_rows.iter() {
        branch_ids.insert(row.branch_id.clone());
        entity_pks.insert(insert_row_entity_pk(row, spec)?);
        file_ids.insert(row.file_id.cloned());
    }
    let file_ids = file_ids
        .into_iter()
        .map(|file_id| {
            file_id.map_or(NullableKeyFilter::Null, |file_id| {
                NullableKeyFilter::Value(file_id.into())
            })
        })
        .collect::<Vec<_>>();

    // Retention is an attribute of the one canonical live identity, not part
    // of SQL conflict identity. A tracked INSERT therefore conflicts with an
    // existing untracked row (and vice versa); `DO UPDATE` then preserves the
    // existing row's retention through `append_entity_replace_row_from_live`.
    ctx.scan_hot_state_batch(&HotStateScanRequest {
        filter: HotStateFilter {
            schema_keys: vec![spec.schema_key.clone()],
            entity_pks: entity_pks.into_iter().collect(),
            branch_ids: branch_ids.into_iter().map(Into::into).collect(),
            file_ids,
            include_tombstones: false,
            ..HotStateFilter::default()
        },
        ..HotStateScanRequest::default()
    })
    .await
}

async fn scan_entity_candidates(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
) -> Result<MaterializedHotStateBatch, LixError> {
    #[cfg(feature = "storage-benches")]
    let _phase =
        crate::storage_bench::enter_crud_phase(crate::storage_bench::CRUD_PHASE_WRITE_READ);
    let branch_ids = scan_branch_ids(&plan.bound.branch_scope)?;
    let mut request = HotStateScanRequest {
        filter: HotStateFilter {
            schema_keys: vec![spec.schema_key.clone()],
            branch_ids,
            include_tombstones: false,
            ..HotStateFilter::default()
        },
        ..HotStateScanRequest::default()
    };
    if let Some(entity_pks) =
        bound_entity_pks_from_primary_key_predicate(spec, &plan.bound.predicate, params)
    {
        if entity_pks.is_empty() {
            request.filter.rows = HotStateRowFilter::None;
        }
        request.filter.entity_pks = entity_pks;
    }
    ctx.scan_hot_state_batch(&request).await
}

async fn scan_entity_candidates_for_pks(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    entity_pks: Vec<EntityPk>,
    metadata_only: bool,
) -> Result<MaterializedHotStateBatch, LixError> {
    #[cfg(feature = "storage-benches")]
    let _phase =
        crate::storage_bench::enter_crud_phase(crate::storage_bench::CRUD_PHASE_WRITE_READ);
    ctx.scan_hot_state_batch(&HotStateScanRequest {
        filter: HotStateFilter {
            schema_keys: vec![spec.schema_key.clone()],
            entity_pks,
            branch_ids: scan_branch_ids(&plan.bound.branch_scope)?,
            include_tombstones: false,
            ..HotStateFilter::default()
        },
        projection: if metadata_only {
            HotStateProjection {
                columns: vec!["metadata".to_string()],
            }
        } else {
            HotStateProjection::default()
        },
        ..HotStateScanRequest::default()
    })
    .await
}

fn bound_entity_pks_from_primary_key_predicate(
    spec: &EntitySurfaceSpec,
    predicate: &BoundPredicate,
    params: &[Value],
) -> Option<Vec<EntityPk>> {
    let primary_key_columns = spec
        .primary_key_paths
        .iter()
        .map(|path| {
            let [column_name] = path.as_slice() else {
                return None;
            };
            spec.visible_column(column_name)
                .map(|column| column.name.as_str())
        })
        .collect::<Option<Vec<_>>>()?;
    if primary_key_columns.is_empty() {
        return None;
    }
    let analyzer = BoundPrimaryKeyAnalyzer {
        primary_key_columns,
        primary_key_component_types: &spec.primary_key_component_types,
        params,
    };
    analyzer
        .analyze_conjunctive_constraint(predicate)?
        .into_entity_pks(
            &analyzer.primary_key_columns,
            analyzer.primary_key_component_types,
        )
        .map(|entity_pks| entity_pks.into_iter().collect())
}

struct BoundPrimaryKeyAnalyzer<'a> {
    primary_key_columns: Vec<&'a str>,
    primary_key_component_types: &'a [crate::entity_pk::EntityPkComponentType],
    params: &'a [Value],
}

#[derive(Clone)]
enum BoundPrimaryKeyConstraint {
    Full(std::collections::BTreeSet<EntityPk>),
    Parts(std::collections::BTreeMap<String, std::collections::BTreeSet<String>>),
}

impl BoundPrimaryKeyAnalyzer<'_> {
    /// Extracts identity constraints that are guaranteed conjuncts. A partial
    /// disjunction is never routed because doing so could omit matching rows.
    fn analyze_conjunctive_constraint(
        &self,
        predicate: &BoundPredicate,
    ) -> Option<BoundPrimaryKeyConstraint> {
        match predicate {
            BoundPredicate::And(predicates) => {
                let mut constraint: Option<BoundPrimaryKeyConstraint> = None;
                for predicate in predicates {
                    let Some(next) = self.analyze_conjunctive_constraint(predicate) else {
                        continue;
                    };
                    constraint = Some(match constraint {
                        Some(current) => current.intersect(next, &self.primary_key_columns),
                        None => next,
                    });
                }
                constraint
            }
            BoundPredicate::Or(predicates) => {
                let mut entity_pks = std::collections::BTreeSet::new();
                for predicate in predicates {
                    entity_pks.extend(
                        self.analyze_conjunctive_constraint(predicate)?
                            .into_entity_pks(
                                &self.primary_key_columns,
                                self.primary_key_component_types,
                            )?,
                    );
                }
                Some(BoundPrimaryKeyConstraint::Full(entity_pks))
            }
            BoundPredicate::Eq(left, right) => self
                .column_value_constraint(left, right)
                .or_else(|| self.column_value_constraint(right, left)),
            BoundPredicate::In { expr, values } => {
                let BoundExpr::Column(column) = expr else {
                    return None;
                };
                if !self.primary_key_columns.contains(&column.name.as_str()) {
                    return None;
                }
                let component_type = self.primary_key_component_type(&column.name)?;
                let values = values
                    .iter()
                    .map(|value| bound_primary_key_external(value, self.params, component_type))
                    .collect::<Option<std::collections::BTreeSet<_>>>()?;
                if values.is_empty() {
                    return None;
                }
                Some(BoundPrimaryKeyConstraint::Parts(
                    std::collections::BTreeMap::from([(column.name.clone(), values)]),
                ))
            }
            BoundPredicate::True
            | BoundPredicate::False
            | BoundPredicate::Like { .. }
            | BoundPredicate::IsNull(_)
            | BoundPredicate::IsNotNull(_) => None,
        }
    }

    fn column_value_constraint(
        &self,
        column_expr: &BoundExpr,
        value_expr: &BoundExpr,
    ) -> Option<BoundPrimaryKeyConstraint> {
        let BoundExpr::Column(column) = column_expr else {
            return None;
        };
        if !self.primary_key_columns.contains(&column.name.as_str()) {
            return None;
        }
        let component_type = self.primary_key_component_type(&column.name)?;
        let value = bound_primary_key_external(value_expr, self.params, component_type)?;
        Some(BoundPrimaryKeyConstraint::Parts(
            std::collections::BTreeMap::from([(
                column.name.clone(),
                std::collections::BTreeSet::from([value]),
            )]),
        ))
    }

    fn primary_key_component_type(
        &self,
        column: &str,
    ) -> Option<crate::entity_pk::EntityPkComponentType> {
        self.primary_key_columns
            .iter()
            .position(|candidate| *candidate == column)
            .and_then(|index| self.primary_key_component_types.get(index))
            .copied()
    }
}

impl BoundPrimaryKeyConstraint {
    fn intersect(self, other: Self, primary_key_columns: &[&str]) -> Self {
        match (self, other) {
            (Self::Full(left), Self::Full(right)) => {
                Self::Full(left.intersection(&right).cloned().collect())
            }
            (Self::Full(ids), Self::Parts(parts)) | (Self::Parts(parts), Self::Full(ids)) => {
                Self::Full(
                    ids.into_iter()
                        .filter(|identity| {
                            identity.components.len() == primary_key_columns.len()
                                && primary_key_columns
                                    .iter()
                                    .enumerate()
                                    .all(|(index, column)| {
                                        parts.get(*column).is_none_or(|values| {
                                            values.contains(
                                                &identity.components[index].external_string(),
                                            )
                                        })
                                    })
                        })
                        .collect(),
                )
            }
            (Self::Parts(mut left), Self::Parts(right)) => {
                for (column, right_values) in right {
                    left.entry(column)
                        .and_modify(|left_values| {
                            *left_values =
                                left_values.intersection(&right_values).cloned().collect();
                        })
                        .or_insert(right_values);
                }
                Self::Parts(left)
            }
        }
    }

    fn into_entity_pks(
        self,
        primary_key_columns: &[&str],
        component_types: &[crate::entity_pk::EntityPkComponentType],
    ) -> Option<std::collections::BTreeSet<EntityPk>> {
        match self {
            Self::Full(entity_pks) => Some(entity_pks),
            Self::Parts(parts) => {
                let mut combinations = vec![Vec::new()];
                for column in primary_key_columns {
                    let values = parts.get(*column)?;
                    let mut next = Vec::with_capacity(combinations.len() * values.len());
                    for prefix in &combinations {
                        for value in values {
                            let mut combination = prefix.clone();
                            combination.push(value.clone());
                            next.push(combination);
                        }
                    }
                    combinations = next;
                }
                combinations
                    .into_iter()
                    .map(|parts| EntityPk::from_external_parts(parts, component_types))
                    .collect::<Result<std::collections::BTreeSet<_>, _>>()
                    .ok()
            }
        }
    }
}

fn bound_primary_key_external(
    expr: &BoundExpr,
    params: &[Value],
    component_type: crate::entity_pk::EntityPkComponentType,
) -> Option<String> {
    use crate::entity_pk::EntityPkComponentType;

    match component_type {
        EntityPkComponentType::Integer => match expr {
            BoundExpr::Literal(BoundLiteral::Integer(value)) => Some(value.to_string()),
            BoundExpr::Param(param) => match params.get(param.index.saturating_sub(1)) {
                Some(Value::Integer(value)) => Some(value.to_string()),
                _ => None,
            },
            _ => None,
        },
        EntityPkComponentType::String
        | EntityPkComponentType::Uuid
        | EntityPkComponentType::Bytes => match expr {
            BoundExpr::Literal(BoundLiteral::Text(value)) => Some(value.clone()),
            BoundExpr::Param(param) => match params.get(param.index.saturating_sub(1)) {
                Some(Value::Text(value)) => Some(value.clone()),
                _ => None,
            },
            _ => None,
        },
    }
}

struct InsertRowLayout {
    schema_key: String,
    visible_columns: Vec<EntitySurfaceColumn>,
    snapshot_context: String,
    snapshot_capacity: usize,
    columns: Vec<InsertColumnTarget>,
}

#[derive(Clone)]
enum InsertColumnTarget {
    Visible {
        name: String,
        column_type: EntityColumnType,
        read_nullable: bool,
    },
    EntityPk,
    FileId,
    Metadata,
    Global,
    Untracked,
    BranchId,
}

impl InsertRowLayout {
    fn from_values(spec: &EntitySurfaceSpec, values: &BoundInsertValues) -> Result<Self, LixError> {
        let mut snapshot_capacity = 0;
        let mut seen_columns = std::collections::BTreeSet::new();
        let columns = values
            .columns
            .iter()
            .map(|column| {
                if !seen_columns.insert(column.name.clone()) {
                    return Err(LixError::new(
                        LixError::CODE_UNSUPPORTED_SQL,
                        format!("duplicate entity INSERT column '{}'", column.name),
                    ));
                }
                if let Some(surface_column) = spec.visible_column(&column.name) {
                    snapshot_capacity += 1;
                    return Ok(InsertColumnTarget::Visible {
                        name: surface_column.name.clone(),
                        column_type: surface_column.column_type,
                        read_nullable: surface_column.read_nullable,
                    });
                }
                Ok(match column.name.as_str() {
                    "lixcol_entity_pk" => InsertColumnTarget::EntityPk,
                    "lixcol_file_id" => InsertColumnTarget::FileId,
                    "lixcol_metadata" => InsertColumnTarget::Metadata,
                    "lixcol_global" => InsertColumnTarget::Global,
                    "lixcol_untracked" => InsertColumnTarget::Untracked,
                    "lixcol_branch_id" => InsertColumnTarget::BranchId,
                    _ => {
                        return Err(LixError::new(
                            LixError::CODE_UNSUPPORTED_SQL,
                            format!(
                                "bound entity INSERT does not support column '{}'",
                                column.name
                            ),
                        ));
                    }
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        Ok(Self {
            schema_key: spec.schema_key.clone(),
            visible_columns: spec.columns.clone(),
            snapshot_context: format!("{} insert snapshot_content", spec.schema_key),
            snapshot_capacity,
            columns,
        })
    }
}

struct CertifiedInsertRow {
    file_id: Option<SharedStr>,
    metadata: Option<TransactionJson>,
    global: bool,
    untracked: bool,
    branch_id: SharedStr,
}

enum CertifiedInsertParams<'a> {
    Borrowed(&'a [Value]),
    Owned(Vec<Value>),
}

impl CertifiedInsertParams<'_> {
    fn as_slice(&self) -> &[Value] {
        match self {
            Self::Borrowed(params) => params,
            Self::Owned(params) => params,
        }
    }
}

struct CertifiedInsertInput<'a> {
    row: &'a [BoundExpr],
    params: CertifiedInsertParams<'a>,
    statement_index: Option<usize>,
}

fn certified_entity_insert_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    layout: &InsertRowLayout,
    values: &BoundInsertValues,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<Option<RawWriteBatch>, LixError> {
    certified_entity_insert_rows(
        ctx,
        plan,
        spec,
        layout,
        values.rows.len(),
        values.rows.iter().map(|row| {
            Ok(CertifiedInsertInput {
                row,
                params: CertifiedInsertParams::Borrowed(params),
                statement_index: None,
            })
        }),
        active_branch_commit_id,
    )
}

fn certified_entity_insert_parameter_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    layout: &InsertRowLayout,
    values: &BoundInsertValues,
    parameter_batch: EntityInsertParameterBatch<'_>,
    allow_generic_fallback: bool,
    active_branch_commit_id: Option<&CommitId>,
) -> Result<Option<CertifiedEntityInsertParameterBatch>, LixError> {
    let [row] = values.rows.as_slice() else {
        return Ok(None);
    };
    if let Some(rows) =
        certified_direct_parameter_insert_batch(ctx, plan, spec, layout, row, parameter_batch)?
    {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_certified_entity_insert_parameter_batch_certification();
        // The dense lane has no per-row change column: it derives every UUID
        // from the commit-delta address space, which untracked rows are not
        // members of. Rather than reintroduce the million-row column that the
        // dense layout exists to avoid, untracked batches fall back to the
        // *raw* certified lane, whose per-row slot already carries a change id.
        //
        // Raw, not generic: the ordinary untracked insert (1k/10k rows) is far
        // below this threshold and already takes the raw lane, so nothing that
        // #1329 optimized changes route. Only untracked batches at or above
        // 32,768 rows move, and they move to raw rather than to the generic
        // path they would otherwise fall to.
        let dense_lane_supports_batch = !rows.untracked();
        return Ok(Some(
            if dense_lane_supports_batch && use_typed_certified_insert(rows.len()) {
                CertifiedEntityInsertParameterBatch::Typed(rows)
            } else {
                CertifiedEntityInsertParameterBatch::Raw(rows.into_raw()?)
            },
        ));
    }
    if !allow_generic_fallback {
        return Ok(None);
    }
    // The tracked/untracked lane is part of the registered schema domain, not
    // merely row payload. A parameterized batch can contain rows from both
    // lanes; committing that batch would defer the domain error until the
    // transaction boundary, where it no longer has a statement index. Keep
    // the established sequential route for this shape so the failing row is
    // validated and attributed before any batch staging occurs.
    if values.rows.iter().any(|row| {
        row.iter().zip(&layout.columns).any(|(expr, target)| {
            matches!(
                (expr, target),
                (BoundExpr::Param(_), InsertColumnTarget::Untracked)
            )
        })
    }) {
        return Ok(None);
    }
    certified_entity_insert_rows(
        ctx,
        plan,
        spec,
        layout,
        parameter_batch.num_rows(),
        (0..parameter_batch.num_rows()).map(|statement_index| {
            let params = match parameter_batch {
                EntityInsertParameterBatch::Arrow(batch) => {
                    super::write::parameter_row(batch, statement_index)
                }
                EntityInsertParameterBatch::Prepared(batch) => batch.row_values(statement_index),
                EntityInsertParameterBatch::Values(rows) => rows
                    .get(statement_index)
                    .map(|row| row.to_vec())
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "SQL parameter row is outside the batch",
                        )
                    }),
            };
            params
                .map(|params| CertifiedInsertInput {
                    row,
                    params: CertifiedInsertParams::Owned(params),
                    statement_index: Some(statement_index),
                })
                .map_err(|error| with_parameter_batch_statement_index(error, statement_index))
        }),
        active_branch_commit_id,
    )
    .map(|rows| rows.map(CertifiedEntityInsertParameterBatch::Raw))
}

struct DirectParameterInsertColumn {
    layout_index: usize,
    parameter_index: usize,
    name: String,
    name_prefix: Vec<u8>,
    column_type: EntityColumnType,
    read_nullable: bool,
}

fn direct_parameter_batch_needs_clustering(
    spec: &EntitySurfaceSpec,
    columns: &[DirectParameterInsertColumn],
    parameter_batch: EntityInsertParameterBatch<'_>,
) -> bool {
    let primary_key_roots = spec
        .primary_key_paths
        .iter()
        .filter_map(|path| path.first().map(String::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    for spec_column in &spec.columns {
        if spec_column.column_type == EntityColumnType::Boolean {
            return true;
        }
        if spec_column.column_type != EntityColumnType::String
            || primary_key_roots.contains(spec_column.name.as_str())
        {
            continue;
        }
        let Some(input) = columns
            .iter()
            .find(|column| column.name == spec_column.name)
        else {
            continue;
        };
        let mut values = std::collections::BTreeSet::new();
        for row_index in 0..parameter_batch.num_rows() {
            if let DirectParameterValue::String(value) =
                parameter_batch.value(input.parameter_index, row_index)
            {
                values.insert(value);
                if values.len() > crate::sql2::LOW_CARDINALITY_CLUSTER_MAX_VALUES {
                    break;
                }
            }
        }
        if (2..=crate::sql2::LOW_CARDINALITY_CLUSTER_MAX_VALUES).contains(&values.len()) {
            return true;
        }
    }
    false
}

fn append_canonical_json_string(output: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    if value
        .as_bytes()
        .iter()
        .all(|&byte| byte >= b' ' && byte != b'"' && byte != b'\\')
    {
        output.push(b'"');
        output.extend_from_slice(value.as_bytes());
        output.push(b'"');
        return Ok(());
    }
    serde_json::to_writer(output, value).map_err(|error| {
        LixError::unknown(format!(
            "certified INSERT value serialization failed: {error}"
        ))
    })
}

fn certified_direct_parameter_insert_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    layout: &InsertRowLayout,
    row: &[BoundExpr],
    parameter_batch: EntityInsertParameterBatch<'_>,
) -> Result<Option<CertifiedParameterInsertBatch>, LixError> {
    if plan.bound.conflict.is_some()
        || !spec.defaults.is_empty()
        || row.len() != layout.columns.len()
    {
        return Ok(None);
    }
    let Some(schema_catalog) = ctx.schema_catalog_snapshot() else {
        return Ok(None);
    };
    let Some((schema_plan_id, schema_plan)) = schema_catalog.plan_for_key(&layout.schema_key)
    else {
        return Ok(None);
    };
    if !schema_plan.accepts_canonical_certificate() {
        return Ok(None);
    }

    if let Some(rows) = certified_direct_path_value_insert_batch(
        ctx,
        spec,
        layout,
        row,
        parameter_batch,
        schema_plan_id,
    )? {
        return Ok(Some(rows));
    }

    let mut columns = Vec::with_capacity(layout.columns.len());
    for (layout_index, (expr, target)) in row.iter().zip(&layout.columns).enumerate() {
        let BoundExpr::Param(param) = expr else {
            return Ok(None);
        };
        let InsertColumnTarget::Visible {
            name,
            column_type,
            read_nullable,
        } = target
        else {
            return Ok(None);
        };
        if !matches!(
            column_type,
            EntityColumnType::String | EntityColumnType::Boolean
        ) {
            return Ok(None);
        }
        let parameter_index = param.index.saturating_sub(1);
        if parameter_index >= parameter_batch.num_columns() {
            return Err(LixError::unknown(format!(
                "SQL parameter ${} is outside a {} column batch",
                param.index,
                parameter_batch.num_columns()
            )));
        }
        if !parameter_batch.column_matches(parameter_index, *column_type) {
            return Ok(None);
        }
        let mut name_prefix = serde_json::to_vec(name).map_err(|error| {
            LixError::unknown(format!(
                "certified INSERT key serialization failed: {error}"
            ))
        })?;
        name_prefix.push(b':');
        columns.push(DirectParameterInsertColumn {
            layout_index,
            parameter_index,
            name: name.clone(),
            name_prefix,
            column_type: *column_type,
            read_nullable: *read_nullable,
        });
    }
    if spec.columns.iter().any(|column| {
        column.insert_required
            && !columns.iter().any(|candidate| {
                matches!(
                    &layout.columns[candidate.layout_index],
                    InsertColumnTarget::Visible { name, .. } if name == &column.name
                )
            })
    }) {
        return Ok(None);
    }
    columns.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    let primary_key_columns = spec
        .primary_key_paths
        .iter()
        .map(|path| {
            let [name] = path.as_slice() else {
                return None;
            };
            columns.iter().position(|column| {
                matches!(
                    &layout.columns[column.layout_index],
                    InsertColumnTarget::Visible {
                        name: candidate,
                        column_type: EntityColumnType::String,
                        ..
                    } if candidate == name
                )
            })
        })
        .collect::<Option<Vec<_>>>();
    let Some(primary_key_columns) = primary_key_columns else {
        return Ok(None);
    };

    let row_count = parameter_batch.num_rows();
    let estimated_row_bytes = columns
        .iter()
        .map(|column| column.name_prefix.len().saturating_add(34))
        .sum::<usize>()
        .saturating_add(columns.len().saturating_add(1));
    let mut normalized =
        Vec::with_capacity(row_count.checked_mul(estimated_row_bytes).ok_or_else(|| {
            LixError::unknown("certified parameter INSERT batch size overflowed")
        })?);
    let mut offsets = Vec::with_capacity(row_count);
    let mut entity_pks = Vec::with_capacity(row_count);
    let shared_string_primary_keys =
        spec.primary_key_component_types
            .iter()
            .all(|component_type| {
                matches!(
                    component_type,
                    crate::entity_pk::EntityPkComponentType::String
                )
            });
    let mut primary_key_arena = Vec::with_capacity(
        row_count
            .saturating_mul(primary_key_columns.len())
            .saturating_mul(16),
    );
    let mut primary_key_ranges =
        Vec::<(u32, u32)>::with_capacity(row_count.saturating_mul(primary_key_columns.len()));
    let mut previous_primary_key_row = None;
    let mut unordered_entity_pks = None::<std::collections::HashSet<EntityPk>>;
    let mut primary_key_parts = Vec::with_capacity(primary_key_columns.len());
    let field_names = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    let row_certificate =
        schema_plan.certify_typed_object_layout(&layout.schema_key, &field_names)?;
    let mut typed_values = Vec::with_capacity(columns.len());

    for statement_index in 0..row_count {
        let row_result = (|| -> Result<bool, LixError> {
            let start = normalized.len();
            typed_values.clear();
            normalized.push(b'{');
            for (field_index, column) in columns.iter().enumerate() {
                if field_index != 0 {
                    normalized.push(b',');
                }
                normalized.extend_from_slice(&column.name_prefix);
                let parameter_value =
                    parameter_batch.value(column.parameter_index, statement_index);
                if matches!(parameter_value, DirectParameterValue::Null) {
                    if !column.read_nullable {
                        let InsertColumnTarget::Visible { name, .. } =
                            &layout.columns[column.layout_index]
                        else {
                            unreachable!("direct parameter columns are visible");
                        };
                        return Err(LixError::new(
                            LixError::CODE_SCHEMA_VALIDATION,
                            format!(
                                "INSERT into {} column '{name}' does not allow explicit NULL",
                                layout.schema_key
                            ),
                        ));
                    }
                    normalized.extend_from_slice(b"null");
                    typed_values.push(TypedJsonScalarRef::Null);
                    continue;
                }
                match (column.column_type, parameter_value) {
                    (EntityColumnType::String, DirectParameterValue::String(value)) => {
                        append_canonical_json_string(&mut normalized, value)?;
                        typed_values.push(TypedJsonScalarRef::String(value));
                    }
                    (EntityColumnType::Boolean, DirectParameterValue::Boolean(value)) => {
                        normalized.extend_from_slice(if value {
                            b"true".as_slice()
                        } else {
                            b"false".as_slice()
                        });
                        typed_values.push(TypedJsonScalarRef::Boolean);
                    }
                    _ => unreachable!("direct parameter column type was certified"),
                }
            }
            normalized.push(b'}');

            primary_key_parts.clear();
            for &column_index in &primary_key_columns {
                let column = &columns[column_index];
                match parameter_batch.value(column.parameter_index, statement_index) {
                    DirectParameterValue::String(value) => primary_key_parts.push(value),
                    DirectParameterValue::Null => {
                        return Err(LixError::new(
                            LixError::CODE_SCHEMA_VALIDATION,
                            format!(
                                "INSERT failed to derive entity primary key for schema '{}': missing primary-key value",
                                layout.schema_key
                            ),
                        ));
                    }
                    DirectParameterValue::Boolean(_) => {
                        unreachable!("direct primary-key parameter is a string")
                    }
                }
            }
            let derived_entity_pk = if shared_string_primary_keys {
                None
            } else {
                Some(
                    EntityPk::from_shared_external_parts(
                        primary_key_parts.iter().map(|part| SharedStr::from(*part)),
                        &spec.primary_key_component_types,
                    )
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_SCHEMA_VALIDATION,
                            format!(
                                "INSERT failed to derive entity primary key for schema '{}': {error}",
                                layout.schema_key
                            ),
                        )
                    })?,
                )
            };
            row_certificate.certify_row(&typed_values, &primary_key_parts)?;
            if shared_string_primary_keys {
                if let Some(previous_row) = previous_primary_key_row {
                    let ordering = primary_key_columns
                        .iter()
                        .zip(&primary_key_parts)
                        .map(|(&column_index, current)| {
                            let column = &columns[column_index];
                            let DirectParameterValue::String(previous) =
                                parameter_batch.value(column.parameter_index, previous_row)
                            else {
                                unreachable!("direct primary-key parameter is a string")
                            };
                            previous.cmp(current)
                        })
                        .find(|ordering| !ordering.is_eq())
                        .unwrap_or(Ordering::Equal);
                    if ordering != Ordering::Less {
                        // The common bulk producer is already ordered. Keep
                        // this arena route allocation-free and let the
                        // established generic certified path preserve
                        // statement-attributed duplicate handling for the
                        // uncommon unordered batch.
                        return Ok(false);
                    }
                }
                previous_primary_key_row = Some(statement_index);
                for part in &primary_key_parts {
                    let start = u32::try_from(primary_key_arena.len()).map_err(|_| {
                        LixError::unknown("certified primary-key arena exceeds u32")
                    })?;
                    primary_key_arena.extend_from_slice(part.as_bytes());
                    let end = u32::try_from(primary_key_arena.len()).map_err(|_| {
                        LixError::unknown("certified primary-key arena exceeds u32")
                    })?;
                    primary_key_ranges.push((start, end));
                }
            } else {
                let entity_pk =
                    derived_entity_pk.expect("non-string primary key was materialized above");
                if let Some(unique) = &mut unordered_entity_pks {
                    if !unique.insert(entity_pk.clone()) {
                        return Ok(false);
                    }
                } else if let Some(previous) = entity_pks.last()
                    && previous >= &entity_pk
                {
                    // Ordered parameter batches prove uniqueness by adjacency
                    // and need no hash table. On the first disorder, seed the
                    // fallback index with exactly the already accepted prefix
                    // so duplicate detection and later validation retain
                    // statement order.
                    let mut unique = std::collections::HashSet::with_capacity(row_count);
                    unique.extend(entity_pks.iter().cloned());
                    if !unique.insert(entity_pk.clone()) {
                        return Ok(false);
                    }
                    unordered_entity_pks = Some(unique);
                }
                entity_pks.push(entity_pk);
            }
            offsets.push((start, normalized.len()));
            Ok(true)
        })();
        if !row_result
            .map_err(|error| with_parameter_batch_statement_index(error, statement_index))?
        {
            return Ok(None);
        }
    }
    if shared_string_primary_keys {
        let primary_key_arena = SharedStr::from_utf8(bytes::Bytes::from(primary_key_arena))
            .map_err(|_| LixError::unknown("certified primary-key arena is not valid UTF-8"))?;
        entity_pks.reserve(row_count);
        for ranges in primary_key_ranges.chunks_exact(primary_key_columns.len()) {
            if let [(start, end)] = ranges {
                entity_pks.push(EntityPk::from_validated_shared_string(
                    primary_key_arena
                        .slice(*start as usize..*end as usize)
                        .expect("certified primary-key ranges preserve UTF-8 boundaries"),
                ));
            } else {
                entity_pks.push(EntityPk::from_validated_shared_string_parts(
                    ranges.iter().map(|&(start, end)| {
                        primary_key_arena
                            .slice(start as usize..end as usize)
                            .expect("certified primary-key ranges preserve UTF-8 boundaries")
                    }),
                ));
            }
        }
    }
    let tracked_keys_strictly_ordered =
        shared_string_primary_keys || unordered_entity_pks.is_none();
    // SAFETY: each row is assembled from UTF-8 literals, validated text
    // parameters, and canonical JSON serializer output.
    let snapshots = unsafe {
        TransactionJson::from_validated_certified_row_content_arena(normalized, offsets)?
    };
    let schema_key: SharedStr = layout.schema_key.as_str().into();
    let branch_id: SharedStr = ctx.active_branch_id().into();
    let certificate = CertifiedRawWriteBatchPreparation {
        schema_plan_id,
        facts: PreparedRowFacts {
            row_content_validated: true,
            requires_transaction_validation: false,
        },
        tracked_keys_strictly_ordered,
        complete_collection_replacement: None,
    };
    let entity_columnar = if use_typed_certified_insert(row_count)
        && !direct_parameter_batch_needs_clustering(spec, &columns, parameter_batch)
    {
        let visible_columns = spec
            .columns
            .iter()
            .map(|spec_column| -> Option<ArrayRef> {
                let input = columns
                    .iter()
                    .find(|column| column.name == spec_column.name);
                Some(match (spec_column.column_type, input) {
                    (EntityColumnType::String, Some(input)) => {
                        Arc::new(StringArray::from_iter((0..row_count).map(|row_index| {
                            match parameter_batch.value(input.parameter_index, row_index) {
                                DirectParameterValue::Null => None,
                                DirectParameterValue::String(value) => Some(value),
                                DirectParameterValue::Boolean(_) => unreachable!(),
                            }
                        })))
                    }
                    (EntityColumnType::Boolean, Some(input)) => {
                        Arc::new(BooleanArray::from_iter((0..row_count).map(|row_index| {
                            match parameter_batch.value(input.parameter_index, row_index) {
                                DirectParameterValue::Null => None,
                                DirectParameterValue::Boolean(value) => Some(value),
                                DirectParameterValue::String(_) => unreachable!(),
                            }
                        })))
                    }
                    (EntityColumnType::String | EntityColumnType::Json, None) => {
                        Arc::new(StringArray::new_null(row_count))
                    }
                    (EntityColumnType::Boolean, None) => {
                        Arc::new(BooleanArray::new_null(row_count))
                    }
                    (EntityColumnType::Integer, None) => Arc::new(Int64Array::new_null(row_count)),
                    (EntityColumnType::Number, None) => Arc::new(Float64Array::new_null(row_count)),
                    (
                        EntityColumnType::Json
                        | EntityColumnType::Integer
                        | EntityColumnType::Number,
                        Some(_),
                    ) => {
                        return None;
                    }
                })
            })
            .collect::<Option<Vec<_>>>();
        visible_columns.and_then(|visible_columns| {
            let entity_pk_text = entity_pks
                .iter()
                .map(EntityPk::as_json_array_text)
                .collect::<Result<Vec<_>, _>>()
                .ok()?;
            crate::sql2::encode_unclustered_registered_entity_row_groups(
                spec,
                visible_columns,
                Arc::new(StringArray::from(entity_pk_text)),
            )
            .ok()
            .flatten()
        })
    } else {
        None
    };
    let mut rows = CertifiedParameterInsertBatch::new(
        entity_pks,
        snapshots,
        schema_key,
        branch_id,
        certificate,
    )?;
    if let Some(entity_columnar) = entity_columnar {
        rows = rows.with_entity_columnar(entity_columnar);
    }
    Ok(Some(rows))
}

fn certified_direct_path_value_insert_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    spec: &EntitySurfaceSpec,
    layout: &InsertRowLayout,
    row: &[BoundExpr],
    parameter_batch: EntityInsertParameterBatch<'_>,
    schema_plan_id: SchemaPlanId,
) -> Result<Option<CertifiedParameterInsertBatch>, LixError> {
    if !spec.certifies_path_value_replacement
        || !(row.len() == 2 || row.len() == 3)
        || layout.columns.len() != row.len()
    {
        return Ok(None);
    }
    let mut path_param_index = None;
    let mut value_param_index = None;
    let mut untracked = false;
    for (expr, target) in row.iter().zip(&layout.columns) {
        match (expr, target) {
            (
                BoundExpr::Param(param),
                InsertColumnTarget::Visible {
                    name,
                    column_type: EntityColumnType::String,
                    ..
                },
            ) if name == "path" => path_param_index = Some(param.index.saturating_sub(1)),
            (
                BoundExpr::Function { name, args },
                InsertColumnTarget::Visible {
                    name: column_name,
                    column_type: EntityColumnType::Json,
                    ..
                },
            ) if name == "lix_json" && column_name == "value" => {
                let [BoundExpr::Param(param)] = args.as_slice() else {
                    return Ok(None);
                };
                value_param_index = Some(param.index.saturating_sub(1));
            }
            (BoundExpr::Literal(BoundLiteral::Bool(true)), InsertColumnTarget::Untracked) => {
                untracked = true;
            }
            _ => return Ok(None),
        }
    }
    let (Some(path_param_index), Some(value_param_index)) = (path_param_index, value_param_index)
    else {
        return Ok(None);
    };
    if !parameter_batch.column_matches(path_param_index, EntityColumnType::String)
        || !parameter_batch.column_matches(value_param_index, EntityColumnType::String)
    {
        return Ok(None);
    }

    let row_count = parameter_batch.num_rows();
    let mut path_arena = Vec::new();
    let mut path_offsets = Vec::with_capacity(row_count);
    let mut normalized = Vec::new();
    let mut snapshot_offsets = Vec::with_capacity(row_count);
    let mut value_offsets = Vec::with_capacity(row_count);
    let mut previous_row = None;
    for statement_index in 0..row_count {
        let DirectParameterValue::String(path) =
            parameter_batch.value(path_param_index, statement_index)
        else {
            return Ok(None);
        };
        if let Some(previous_row) = previous_row {
            let DirectParameterValue::String(previous) =
                parameter_batch.value(path_param_index, previous_row)
            else {
                unreachable!("the previous certified path parameter was text")
            };
            if previous >= path {
                return Ok(None);
            }
        }
        previous_row = Some(statement_index);
        let path_start = path_arena.len();
        path_arena.extend_from_slice(path.as_bytes());
        path_offsets.push((path_start, path_arena.len()));

        let snapshot_start = normalized.len();
        normalized.extend_from_slice(b"{\"path\":");
        append_canonical_json_string(&mut normalized, path)
            .map_err(|error| with_parameter_batch_statement_index(error, statement_index))?;
        normalized.extend_from_slice(b",\"value\":");
        let DirectParameterValue::String(raw_value) =
            parameter_batch.value(value_param_index, statement_index)
        else {
            return Ok(None);
        };
        let value = serde_json::from_str::<JsonValue>(raw_value).map_err(|error| {
            with_parameter_batch_statement_index(
                LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    format!("lix_json argument is not valid JSON: {error}"),
                ),
                statement_index,
            )
        })?;
        let value_start = normalized.len();
        serde_json::to_writer(&mut normalized, &value).map_err(|error| {
            with_parameter_batch_statement_index(
                LixError::unknown(format!(
                    "certified INSERT value failed to serialize: {error}"
                )),
                statement_index,
            )
        })?;
        value_offsets.push((value_start, normalized.len()));
        normalized.push(b'}');
        snapshot_offsets.push((snapshot_start, normalized.len()));
    }

    let entity_columnar = if !untracked && use_typed_certified_insert(row_count) {
        let path_values = StringArray::from_iter(path_offsets.iter().map(|&(start, end)| {
            Some(
                std::str::from_utf8(&path_arena[start..end])
                    .expect("certified INSERT path arena is UTF-8"),
            )
        }));
        let json_values = StringArray::from_iter(value_offsets.iter().map(|&(start, end)| {
            Some(
                std::str::from_utf8(&normalized[start..end])
                    .expect("certified INSERT JSON arena is UTF-8"),
            )
        }));
        let entity_pk_values = StringArray::from_iter(path_offsets.iter().map(|&(start, end)| {
            let path = std::str::from_utf8(&path_arena[start..end])
                .expect("certified INSERT path arena is UTF-8");
            Some(format!(
                "[{}]",
                serde_json::to_string(path).expect("path should encode")
            ))
        }));
        crate::sql2::encode_unclustered_registered_entity_row_groups(
            spec,
            vec![Arc::new(path_values), Arc::new(json_values)],
            Arc::new(entity_pk_values),
        )
        .ok()
        .flatten()
    } else {
        None
    };
    let path_arena = SharedStr::from_utf8(bytes::Bytes::from(path_arena))
        .map_err(|_| LixError::unknown("certified INSERT path arena is not UTF-8"))?;
    let entity_pks = path_offsets
        .into_iter()
        .map(|(start, end)| {
            EntityPk::from_validated_shared_string(
                path_arena
                    .slice(start..end)
                    .expect("certified INSERT path offsets preserve UTF-8"),
            )
        })
        .collect::<Vec<_>>();
    // SAFETY: each row is assembled from UTF-8 literals, `str` paths, and
    // serde_json output.
    let snapshots = unsafe {
        TransactionJson::from_validated_certified_row_content_arena(normalized, snapshot_offsets)?
    };
    let mut rows = CertifiedParameterInsertBatch::new_with_lane(
        entity_pks,
        snapshots,
        layout.schema_key.as_str().into(),
        ctx.active_branch_id().into(),
        untracked,
        CertifiedRawWriteBatchPreparation {
            schema_plan_id,
            facts: PreparedRowFacts {
                row_content_validated: true,
                requires_transaction_validation: false,
            },
            tracked_keys_strictly_ordered: true,
            complete_collection_replacement: None,
        },
    )?;
    if let Some(entity_columnar) = entity_columnar {
        rows = rows.with_entity_columnar(entity_columnar);
    }
    Ok(Some(rows))
}

fn certified_entity_insert_rows<'a>(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    layout: &InsertRowLayout,
    row_count: usize,
    inputs: impl IntoIterator<Item = Result<CertifiedInsertInput<'a>, LixError>>,
    active_branch_commit_id: Option<&CommitId>,
) -> Result<Option<RawWriteBatch>, LixError> {
    if plan.bound.conflict.is_some() {
        return Ok(None);
    }
    if layout.columns.iter().any(|target| {
        matches!(
            target,
            InsertColumnTarget::FileId
                | InsertColumnTarget::Visible {
                    column_type: EntityColumnType::Json
                        | EntityColumnType::Integer
                        | EntityColumnType::Number,
                    ..
                }
        )
    }) {
        return Ok(None);
    }
    let Some(schema_catalog) = ctx.schema_catalog_snapshot() else {
        return Ok(None);
    };
    let Some((_, schema_plan)) = schema_catalog.plan_for_key(&layout.schema_key) else {
        return Ok(None);
    };
    if !schema_plan.accepts_canonical_certificate() || !spec.defaults.is_empty() {
        return Ok(None);
    }
    if spec.columns.iter().any(|column| {
        column.insert_required
            && !layout.columns.iter().any(|target| {
                matches!(
                    target,
                    InsertColumnTarget::Visible { name, .. } if name == &column.name
                )
            })
    }) {
        return Ok(None);
    }

    let mut visible_indices = layout
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, target)| match target {
            InsertColumnTarget::Visible { name, .. } => Some((index, name.as_str())),
            _ => None,
        })
        .collect::<Vec<_>>();
    visible_indices.sort_unstable_by_key(|(_, name)| *name);
    let primary_key_indices = spec
        .primary_key_paths
        .iter()
        .map(|path| {
            let [name] = path.as_slice() else {
                return None;
            };
            layout
                .columns
                .iter()
                .position(|target| matches!(target, InsertColumnTarget::Visible { name: candidate, .. } if candidate == name))
        })
        .collect::<Option<Vec<_>>>();
    let Some(primary_key_indices) = primary_key_indices else {
        return Ok(None);
    };

    let estimated_row_bytes = visible_indices
        .iter()
        .map(|(_, name)| name.len().saturating_add(35))
        .sum::<usize>()
        .saturating_add(2);
    let estimated_batch_bytes = row_count
        .checked_mul(estimated_row_bytes)
        .ok_or_else(|| LixError::unknown("certified INSERT batch size overflowed"))?;
    let mut normalized = Vec::with_capacity(estimated_batch_bytes);
    let mut offsets = Vec::with_capacity(row_count);
    let mut entity_pks = Vec::with_capacity(row_count);
    let mut row_parts = Vec::with_capacity(row_count);
    let mut unique_identities = std::collections::HashSet::with_capacity(row_count);
    let mut row_values = (0..layout.columns.len())
        .map(|_| None)
        .collect::<Vec<Option<JsonValue>>>();
    let context = EntityEvalContext::insert(&JsonValue::Null, &layout.visible_columns);

    for input in inputs {
        let input = input?;
        let row = input.row;
        let params = input.params.as_slice();
        let statement_index = input.statement_index;
        let row_result = (|| -> Result<bool, LixError> {
            if row.len() != layout.columns.len() {
                return Err(LixError::new(
                    LixError::CODE_UNSUPPORTED_SQL,
                    "entity INSERT rows must have a consistent column layout",
                ));
            }

            let mut explicit_entity_pk = None;
            let mut file_id = None;
            let mut metadata = None;
            let mut global = None;
            let mut untracked = None;
            let mut explicit_branch_id = None;
            for (index, (expr, target)) in row.iter().zip(layout.columns.iter()).enumerate() {
                if let InsertColumnTarget::Visible { column_type, .. } = target {
                    reject_direct_blob_json_value(expr, *column_type, params)?;
                }
                let eval_value =
                    eval_expr_value(expr, &context, ctx, params, active_branch_commit_id)?;
                if matches!(
                    target,
                    InsertColumnTarget::Global | InsertColumnTarget::Untracked
                ) && entity_eval_value_is_null(&eval_value)
                {
                    let column_name = match target {
                        InsertColumnTarget::Global => "lixcol_global",
                        InsertColumnTarget::Untracked => "lixcol_untracked",
                        _ => unreachable!("matched defaulted boolean system column"),
                    };
                    return Err(LixError::new(
                        LixError::CODE_TYPE_MISMATCH,
                        format!(
                            "INSERT into {} column '{column_name}' may be omitted to use its default, but explicit NULL is not allowed",
                            layout.schema_key
                        ),
                    ));
                }
                if matches!(target, InsertColumnTarget::Metadata) {
                    metadata = optional_metadata_from_eval_value(
                        eval_value,
                        "lixcol_metadata",
                        &layout.schema_key,
                    )?;
                    continue;
                }
                if let InsertColumnTarget::Visible {
                    name,
                    column_type,
                    read_nullable,
                } = target
                {
                    if !read_nullable && entity_eval_value_is_null(&eval_value) {
                        return Err(LixError::new(
                            LixError::CODE_SCHEMA_VALIDATION,
                            format!(
                                "INSERT into {} column '{name}' does not allow explicit NULL",
                                layout.schema_key
                            ),
                        ));
                    }
                    row_values[index] = Some(entity_json_value(
                        expr,
                        eval_value,
                        *column_type,
                        &layout.schema_key,
                        name,
                    )?);
                    continue;
                }
                let value = eval_value.into_json();
                match target {
                    InsertColumnTarget::Visible { .. } => {
                        unreachable!("visible columns handled above")
                    }
                    InsertColumnTarget::EntityPk => {
                        explicit_entity_pk =
                            Some(entity_pk_from_value(&value, "lixcol_entity_pk")?);
                    }
                    InsertColumnTarget::FileId => {
                        file_id = text_value(value, "lixcol_file_id")?;
                    }
                    InsertColumnTarget::Metadata => {
                        unreachable!("metadata handled before JSON value coercion")
                    }
                    InsertColumnTarget::Global => {
                        global = bool_value(value, "lixcol_global")?;
                    }
                    InsertColumnTarget::Untracked => {
                        untracked = bool_value(value, "lixcol_untracked")?;
                    }
                    InsertColumnTarget::BranchId => {
                        explicit_branch_id = text_value(value, "lixcol_branch_id")?;
                    }
                }
            }

            let primary_key_values = primary_key_indices
            .iter()
            .map(|index| {
                row_values[*index].as_ref().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!(
                            "INSERT failed to derive entity primary key for schema '{}': missing primary-key value",
                            layout.schema_key
                        ),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
            let derived_entity_pk = EntityPk::from_json_values(
                &primary_key_values
                    .iter()
                    .map(|value| (*value).clone())
                    .collect::<Vec<_>>(),
                &spec.primary_key_component_types,
            )
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "INSERT failed to derive entity primary key for schema '{}': {error}",
                        layout.schema_key
                    ),
                )
            })?;
            if explicit_entity_pk.as_ref().is_some_and(|explicit| {
                explicit.clone().into_parts() != derived_entity_pk.clone().into_parts()
            }) {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "INSERT into {} has lixcol_entity_pk that does not match its public primary-key columns",
                        layout.schema_key
                    ),
                ));
            }

            let start = normalized.len();
            normalized.push(b'{');
            for (field_index, (value_index, name)) in visible_indices.iter().enumerate() {
                if field_index != 0 {
                    normalized.push(b',');
                }
                serde_json::to_writer(&mut normalized, name).map_err(|error| {
                    LixError::unknown(format!(
                        "certified INSERT key serialization failed: {error}"
                    ))
                })?;
                normalized.push(b':');
                serde_json::to_writer(
                    &mut normalized,
                    row_values[*value_index]
                        .as_ref()
                        .expect("visible INSERT value was evaluated"),
                )
                .map_err(|error| {
                    LixError::unknown(format!(
                        "certified INSERT value serialization failed: {error}"
                    ))
                })?;
            }
            normalized.push(b'}');
            let key = WasmEntityKey::from_owned_parts(
                layout.schema_key.clone(),
                derived_entity_pk.clone().into_parts(),
            );
            let certified = schema_plan
                .certify_or_normalize_plugin_row(&normalized[start..], &key)?
                .ok_or_else(|| {
                    LixError::unknown(
                        "eligible certified INSERT row declined its schema certificate",
                    )
                })?;
            if let Some(canonical) = certified.normalized {
                normalized.truncate(start);
                normalized.extend_from_slice(&canonical);
            }
            let end = normalized.len();
            let global = global.unwrap_or(false);
            let row_parts_entry = CertifiedInsertRow {
                file_id: file_id.map(Into::into),
                metadata,
                global,
                untracked: untracked.unwrap_or(false),
                branch_id: entity_row_branch_id(plan, explicit_branch_id, global)?.into(),
            };
            if !unique_identities.insert((
                certified.entity_pk.clone(),
                row_parts_entry.file_id.clone(),
                row_parts_entry.branch_id.clone(),
                global,
            )) {
                return Ok(false);
            }
            offsets.push((start, end));
            entity_pks.push(certified.entity_pk);
            row_parts.push(row_parts_entry);
            for value in &mut row_values {
                *value = None;
            }
            Ok(true)
        })();
        let unique = row_result.map_err(|error| match statement_index {
            Some(index) => with_parameter_batch_statement_index(error, index),
            None => error,
        })?;
        if !unique {
            return Ok(None);
        }
    }

    let row_count = offsets.len();
    // SAFETY: each row is assembled from UTF-8 literals, validated text
    // parameters, and canonical JSON serializer output.
    let snapshots = unsafe {
        TransactionJson::from_validated_certified_row_content_arena(normalized, offsets)?
    };
    let mut rows = RawWriteBatch::with_capacity(row_count);
    for ((entity_pk, snapshot), row) in entity_pks.into_iter().zip(snapshots).zip(row_parts) {
        rows.push_parts(
            Some(entity_pk),
            layout.schema_key.as_str().into(),
            row.file_id,
            Some(snapshot),
            row.metadata,
            None,
            None,
            None,
            row.global,
            None,
            None,
            row.untracked,
            row.branch_id,
        );
    }
    #[cfg(test)]
    CERTIFIED_ENTITY_INSERT_BATCH_EXECUTIONS.with(|executions| {
        executions.set(executions.get().saturating_add(1));
    });
    Ok(Some(rows))
}

fn append_entity_insert_row(
    rows: &mut RawWriteBatch,
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    layout: &InsertRowLayout,
    row: &[BoundExpr],
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<(), LixError> {
    if row.len() != layout.columns.len() {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "entity INSERT rows must have a consistent column layout",
        ));
    }

    let mut snapshot = serde_json::Map::with_capacity(layout.snapshot_capacity);
    let mut entity_pk = None;
    let mut file_id = None;
    let mut metadata = None;
    let mut global = None;
    let mut untracked = None;
    let mut explicit_branch_id = None;
    let context = EntityEvalContext::insert(&JsonValue::Null, &layout.visible_columns);

    for (expr, target) in row.iter().zip(layout.columns.iter()) {
        if let InsertColumnTarget::Visible { column_type, .. } = target {
            reject_direct_blob_json_value(expr, *column_type, params)?;
        }
        let eval_value = eval_expr_value(expr, &context, ctx, params, active_branch_commit_id)?;
        if matches!(
            target,
            InsertColumnTarget::Global | InsertColumnTarget::Untracked
        ) && entity_eval_value_is_null(&eval_value)
        {
            let column_name = match target {
                InsertColumnTarget::Global => "lixcol_global",
                InsertColumnTarget::Untracked => "lixcol_untracked",
                _ => unreachable!("matched defaulted boolean system column"),
            };
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!(
                    "INSERT into {} column '{column_name}' may be omitted to use its default, but explicit NULL is not allowed",
                    layout.schema_key
                ),
            ));
        }
        if matches!(target, InsertColumnTarget::Metadata) {
            metadata = optional_metadata_from_eval_value(
                eval_value,
                "lixcol_metadata",
                &layout.schema_key,
            )?;
            continue;
        }
        if let InsertColumnTarget::Visible {
            name,
            column_type,
            read_nullable,
        } = target
        {
            if !read_nullable && entity_eval_value_is_null(&eval_value) {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "INSERT into {} column '{name}' does not allow explicit NULL",
                        layout.schema_key
                    ),
                ));
            }
            snapshot.insert(
                name.clone(),
                entity_json_value(expr, eval_value, *column_type, &layout.schema_key, name)?,
            );
            continue;
        }
        let value = eval_value.into_json();
        match target {
            InsertColumnTarget::Visible { .. } => unreachable!("visible columns handled above"),
            InsertColumnTarget::EntityPk => {
                entity_pk = Some(entity_pk_from_value(&value, "lixcol_entity_pk")?);
            }
            InsertColumnTarget::FileId => {
                file_id = text_value(value, "lixcol_file_id")?;
            }
            InsertColumnTarget::Metadata => {
                unreachable!("metadata handled before JSON value coercion")
            }
            InsertColumnTarget::Global => {
                global = bool_value(value, "lixcol_global")?;
            }
            InsertColumnTarget::Untracked => {
                untracked = bool_value(value, "lixcol_untracked")?;
            }
            InsertColumnTarget::BranchId => {
                explicit_branch_id = text_value(value, "lixcol_branch_id")?;
            }
        }
    }

    spec.defaults
        .apply(&mut snapshot, ctx.functions(), &layout.schema_key)?;
    let snapshot = JsonValue::Object(snapshot);
    if !spec.primary_key_paths.is_empty() {
        let derived_entity_pk = EntityPk::from_primary_key_plan(
            &snapshot,
            &spec.primary_key_paths,
            &spec.primary_key_component_types,
        )
        .map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "INSERT failed to derive entity primary key for schema '{}': {error}",
                    layout.schema_key
                ),
            )
        })?;
        if entity_pk
            .as_ref()
            .is_some_and(|explicit_entity_pk| explicit_entity_pk != &derived_entity_pk)
        {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "INSERT into {} has lixcol_entity_pk that does not match its public primary-key columns",
                    layout.schema_key
                ),
            ));
        }
        entity_pk = Some(derived_entity_pk);
    }
    let global = global.unwrap_or(false);
    let branch_id = entity_row_branch_id(plan, explicit_branch_id, global)?;
    rows.push_parts(
        entity_pk,
        layout.schema_key.as_str().into(),
        file_id.map(Into::into),
        Some(TransactionJson::from_value(
            snapshot,
            &layout.snapshot_context,
        )?),
        metadata,
        None,
        None,
        None,
        global,
        None,
        None,
        untracked.unwrap_or(false),
        branch_id.into(),
    );
    Ok(())
}

fn reject_projected_global_write<'a>(
    plan: &LogicalWritePlan,
    row: impl Into<EntityLiveRowRef<'a>>,
    action: &str,
) -> Result<(), LixError> {
    let row = row.into();
    let target_is_by_branch = matches!(
        &plan.bound.target,
        BoundWriteTarget::Entity(EntityWriteSurface::ByBranch { .. })
    );
    if target_is_by_branch && row.global() && row.branch_id() != crate::GLOBAL_BRANCH_ID {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!(
                "{action} through an entity by-branch surface cannot mutate a projected global row"
            ),
        ));
    }
    Ok(())
}

fn append_entity_replace_row_from_live<'a>(
    rows: &mut RawWriteBatch,
    ctx: &mut dyn SqlWriteExecutionContext,
    spec: &EntitySurfaceSpec,
    row: impl Into<EntityLiveRowRef<'a>>,
    snapshot: Option<JsonValue>,
    assignments: &[BoundAssignment],
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<(), LixError> {
    let row = row.into();
    let metadata = if let Some(expr) = assignment_value(assignments, "lixcol_metadata") {
        let snapshot_for_eval = candidate_snapshot(row)?.unwrap_or(JsonValue::Null);
        let context = EntityEvalContext::live(&snapshot_for_eval, row, spec);
        let value = eval_expr_value(expr, &context, ctx, params, active_branch_commit_id)?;
        optional_metadata_from_eval_value(value, "lixcol_metadata", &spec.schema_key)?
    } else {
        inherited_metadata(row, spec)?
    };

    let snapshot = snapshot
        .map(|snapshot| {
            TransactionJson::from_value(
                snapshot,
                &format!("{} update snapshot_content", spec.schema_key),
            )
        })
        .transpose()?;
    rows.push_parts(
        Some(row.entity_pk().clone()),
        spec.schema_key.as_str().into(),
        row.file_id().map(Into::into),
        snapshot,
        metadata,
        None,
        None,
        None,
        row.global(),
        None,
        None,
        row.untracked(),
        if row.global() {
            crate::GLOBAL_BRANCH_ID.into()
        } else {
            row.branch_id().into()
        },
    );
    Ok(())
}

fn inherited_metadata<'a>(
    row: impl Into<EntityLiveRowRef<'a>>,
    spec: &EntitySurfaceSpec,
) -> Result<Option<TransactionJson>, LixError> {
    row.into()
        .metadata()
        .map(|metadata| {
            let metadata = parse_row_metadata_value(metadata, &spec.schema_key)?;
            TransactionJson::from_value(metadata, &format!("{} metadata", spec.schema_key))
        })
        .transpose()
}

struct EntityEvalContext<'a> {
    snapshot: &'a JsonValue,
    row: Option<EntityEvalRowRef<'a>>,
    excluded_snapshot: Option<&'a JsonValue>,
    excluded_row: Option<RawWriteRowRef<'a>>,
    visible_columns: &'a [EntitySurfaceColumn],
}

#[derive(Clone, Copy)]
enum EntityEvalRowRef<'a> {
    Live(EntityLiveRowRef<'a>),
    Staged(RawWriteRowRef<'a>),
}

impl<'a> EntityEvalRowRef<'a> {
    fn entity_pk(self) -> Option<&'a EntityPk> {
        match self {
            Self::Live(row) => Some(row.entity_pk()),
            Self::Staged(row) => row.entity_pk,
        }
    }

    fn schema_key(self) -> &'a str {
        match self {
            Self::Live(row) => row.schema_key(),
            Self::Staged(row) => row.schema_key.as_str(),
        }
    }

    fn file_id(self) -> Option<&'a str> {
        match self {
            Self::Live(row) => row.file_id(),
            Self::Staged(row) => row.file_id.map(SharedStr::as_str),
        }
    }

    fn metadata(self) -> Result<Option<JsonValue>, LixError> {
        match self {
            Self::Live(row) => row
                .metadata()
                .map(|metadata| parse_row_metadata_value(metadata, row.schema_key()))
                .transpose(),
            Self::Staged(row) => row
                .metadata
                .map(|metadata| transaction_json_returning_value(metadata, "entity metadata"))
                .transpose(),
        }
    }

    fn created_at(self) -> Option<String> {
        match self {
            Self::Live(row) => Some(row.created_at().to_string()),
            Self::Staged(row) => row.created_at.map(str::to_owned),
        }
    }

    fn updated_at(self) -> Option<String> {
        match self {
            Self::Live(row) => Some(row.updated_at().to_string()),
            Self::Staged(row) => row.updated_at.map(str::to_owned),
        }
    }

    fn change_id(self) -> Option<String> {
        match self {
            Self::Live(row) => row.change_id().map(|value| value.to_string()),
            Self::Staged(row) => row.change_id.map(str::to_owned),
        }
    }

    fn commit_id(self) -> Option<String> {
        match self {
            Self::Live(row) => row.commit_id().map(|value| value.to_string()),
            Self::Staged(row) => row.commit_id.map(str::to_owned),
        }
    }

    fn global(self) -> bool {
        match self {
            Self::Live(row) => row.global(),
            Self::Staged(row) => row.global,
        }
    }

    fn untracked(self) -> bool {
        match self {
            Self::Live(row) => row.untracked(),
            Self::Staged(row) => row.untracked,
        }
    }

    fn branch_id(self) -> &'a str {
        match self {
            Self::Live(row) => row.branch_id(),
            Self::Staged(row) => row.branch_id.as_str(),
        }
    }
}

impl<'a> EntityEvalContext<'a> {
    fn insert(snapshot: &'a JsonValue, visible_columns: &'a [EntitySurfaceColumn]) -> Self {
        Self {
            snapshot,
            row: None,
            excluded_snapshot: None,
            excluded_row: None,
            visible_columns,
        }
    }

    fn live(
        snapshot: &'a JsonValue,
        row: impl Into<EntityLiveRowRef<'a>>,
        spec: &'a EntitySurfaceSpec,
    ) -> Self {
        Self {
            snapshot,
            row: Some(EntityEvalRowRef::Live(row.into())),
            excluded_snapshot: None,
            excluded_row: None,
            visible_columns: &spec.columns,
        }
    }

    fn staged(
        snapshot: &'a JsonValue,
        row: RawWriteRowRef<'a>,
        spec: &'a EntitySurfaceSpec,
    ) -> Self {
        Self {
            snapshot,
            row: Some(EntityEvalRowRef::Staged(row)),
            excluded_snapshot: None,
            excluded_row: None,
            visible_columns: &spec.columns,
        }
    }

    fn conflict(
        snapshot: &'a JsonValue,
        row: impl Into<EntityLiveRowRef<'a>>,
        excluded_snapshot: &'a JsonValue,
        excluded_row: RawWriteRowRef<'a>,
        spec: &'a EntitySurfaceSpec,
    ) -> Self {
        Self {
            snapshot,
            row: Some(EntityEvalRowRef::Live(row.into())),
            excluded_snapshot: Some(excluded_snapshot),
            excluded_row: Some(excluded_row),
            visible_columns: &spec.columns,
        }
    }
}

fn entity_spec(
    ctx: &dyn SqlWriteExecutionContext,
    schema_key: &str,
) -> Result<EntitySurfaceSpec, LixError> {
    ctx.public_catalog()?
        .entity_spec(schema_key)
        .cloned()
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("entity surface '{schema_key}' is not visible"),
            )
        })
}

#[derive(Clone, Debug)]
enum EntityEvalValue {
    SqlNull,
    SqlText(String),
    Json(JsonValue),
}

impl EntityEvalValue {
    fn into_json(self) -> JsonValue {
        match self {
            Self::SqlNull => JsonValue::Null,
            Self::SqlText(value) => JsonValue::String(value),
            Self::Json(value) => value,
        }
    }
}

fn entity_eval_value_is_null(value: &EntityEvalValue) -> bool {
    matches!(
        value,
        EntityEvalValue::SqlNull | EntityEvalValue::Json(JsonValue::Null)
    )
}

fn cast_entity_eval_value(
    value: EntityEvalValue,
    cast_type: BoundCastType,
) -> Result<EntityEvalValue, LixError> {
    if cast_type == BoundCastType::Binary {
        return Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            "BYTEA casts require a binary SQL column",
        )
        .with_hint(
            "Use BYTEA for lix_file.content; registered entity schemas expose no binary column type.",
        ));
    }

    let target_type = match cast_type {
        BoundCastType::Text => DataType::Utf8,
        BoundCastType::BigInt => DataType::Int64,
        BoundCastType::Double => DataType::Float64,
        BoundCastType::Boolean => DataType::Boolean,
        BoundCastType::Binary => unreachable!("binary entity casts rejected above"),
    };
    let scalar = scalar_from_entity_eval_value(value);
    let casted = scalar.cast_to(&target_type).map_err(|error| {
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("CAST AS {} failed: {error}", cast_type.canonical_sql_name()),
        )
    })?;
    entity_eval_value_from_cast_scalar(casted, cast_type)
}

fn scalar_from_entity_eval_value(value: EntityEvalValue) -> ScalarValue {
    match value {
        EntityEvalValue::SqlNull | EntityEvalValue::Json(JsonValue::Null) => ScalarValue::Null,
        EntityEvalValue::SqlText(value) | EntityEvalValue::Json(JsonValue::String(value)) => {
            ScalarValue::Utf8(Some(value))
        }
        EntityEvalValue::Json(JsonValue::Bool(value)) => ScalarValue::Boolean(Some(value)),
        EntityEvalValue::Json(JsonValue::Number(value)) => value.as_i64().map_or_else(
            || {
                value.as_u64().map_or_else(
                    || ScalarValue::Float64(value.as_f64()),
                    |value| ScalarValue::UInt64(Some(value)),
                )
            },
            |value| ScalarValue::Int64(Some(value)),
        ),
        EntityEvalValue::Json(value @ (JsonValue::Array(_) | JsonValue::Object(_))) => {
            ScalarValue::Utf8(Some(value.to_string()))
        }
    }
}

fn entity_eval_value_from_cast_scalar(
    value: ScalarValue,
    cast_type: BoundCastType,
) -> Result<EntityEvalValue, LixError> {
    if value.is_null() {
        return Ok(EntityEvalValue::SqlNull);
    }
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Ok(EntityEvalValue::SqlText(value)),
        ScalarValue::Int64(Some(value)) => {
            Ok(EntityEvalValue::Json(JsonValue::Number(value.into())))
        }
        ScalarValue::Float64(Some(value)) => serde_json::Number::from_f64(value)
            .map(JsonValue::Number)
            .map(EntityEvalValue::Json)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "DOUBLE PRECISION cast produced a non-finite number",
                )
            }),
        ScalarValue::Boolean(Some(value)) => Ok(EntityEvalValue::Json(JsonValue::Bool(value))),
        other => Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "CAST AS {} produced unexpected scalar {other:?}",
                cast_type.canonical_sql_name()
            ),
        )),
    }
}

fn eval_expr(
    expr: &BoundExpr,
    context: &EntityEvalContext<'_>,
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<JsonValue, LixError> {
    eval_expr_value(expr, context, ctx, params, active_branch_commit_id)
        .map(EntityEvalValue::into_json)
}

fn eval_expr_value(
    expr: &BoundExpr,
    context: &EntityEvalContext<'_>,
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<EntityEvalValue, LixError> {
    match expr {
        BoundExpr::Literal(BoundLiteral::Null) => Ok(EntityEvalValue::SqlNull),
        BoundExpr::Literal(BoundLiteral::Text(value)) => {
            Ok(EntityEvalValue::SqlText(value.clone()))
        }
        BoundExpr::Literal(literal) => Ok(EntityEvalValue::Json(literal_json(literal))),
        BoundExpr::Param(param) => params
            .get(param.index.saturating_sub(1))
            .map(value_eval)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("missing SQL parameter ${}", param.index),
                )
            }),
        BoundExpr::Column(column) => column_eval_value(context, &column.name),
        BoundExpr::ExcludedColumn(column) => excluded_column_eval_value(context, &column.name),
        BoundExpr::Cast { expr, data_type } => {
            let value = eval_expr_value(expr, context, ctx, params, active_branch_commit_id)?;
            cast_entity_eval_value(value, *data_type)
        }
        BoundExpr::Function { name, args } if name == "lix_json" && args.len() == 1 => {
            let raw = eval_expr_value(&args[0], context, ctx, params, active_branch_commit_id)?;
            let raw = match raw {
                EntityEvalValue::SqlNull => return Ok(EntityEvalValue::Json(JsonValue::Null)),
                EntityEvalValue::SqlText(value) => JsonValue::String(value),
                EntityEvalValue::Json(value) => value,
            };
            let JsonValue::String(raw) = raw else {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "lix_json expects a text argument",
                ));
            };
            serde_json::from_str(&raw)
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_TYPE_MISMATCH,
                        format!("lix_json argument is not valid JSON: {error}"),
                    )
                })
                .map(EntityEvalValue::Json)
        }
        BoundExpr::Function { name, args } if name == "lix_uuid_v7" && args.is_empty() => Ok(
            EntityEvalValue::SqlText(ctx.functions().call_uuid_v7().to_string()),
        ),
        BoundExpr::Function { name, args } if name == "lix_timestamp" && args.is_empty() => Ok(
            EntityEvalValue::SqlText(ctx.functions().call_timestamp().to_string()),
        ),
        BoundExpr::Function { name, args } if name == "lix_active_branch_id" && args.is_empty() => {
            Ok(EntityEvalValue::SqlText(ctx.active_branch_id().to_string()))
        }
        BoundExpr::Function { name, args }
            if name == "lix_active_branch_commit_id" && args.is_empty() =>
        {
            Ok(active_branch_commit_id
                .map(|commit_id| EntityEvalValue::SqlText(commit_id.to_string()))
                .unwrap_or(EntityEvalValue::SqlNull))
        }
        BoundExpr::Function { name, args }
            if (name == "lix_json_get" || name == "lix_json_get_text") && args.len() >= 2 =>
        {
            let root = eval_expr_value(&args[0], context, ctx, params, active_branch_commit_id)?;
            let mut current = match root {
                EntityEvalValue::SqlNull => return Ok(EntityEvalValue::SqlNull),
                EntityEvalValue::SqlText(raw) => {
                    serde_json::from_str::<JsonValue>(&raw).map_err(|error| {
                        LixError::new(
                            LixError::CODE_TYPE_MISMATCH,
                            format!(
                                "{name} expected valid JSON text in its first argument: {error}"
                            ),
                        )
                    })?
                }
                EntityEvalValue::Json(root) => match root {
                    JsonValue::Null => return Ok(EntityEvalValue::SqlNull),
                    value => value,
                },
            };
            for arg in &args[1..] {
                let segment = eval_expr(arg, context, ctx, params, active_branch_commit_id)?;
                let Some(next) = json_path_get(&current, &segment, name)? else {
                    return Ok(EntityEvalValue::SqlNull);
                };
                current = next;
            }
            if name == "lix_json_get_text" {
                if current.is_null() {
                    return Ok(EntityEvalValue::SqlNull);
                }
                Ok(EntityEvalValue::SqlText(json_text_value(&current)?))
            } else {
                Ok(EntityEvalValue::Json(current))
            }
        }
        BoundExpr::Function { name, .. } => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!("bound entity write does not support function '{name}' yet"),
        )),
        BoundExpr::Binary { left, op, right } => eval_binary_entity_value(
            left,
            *op,
            right,
            context,
            ctx,
            params,
            active_branch_commit_id,
        ),
    }
}

/// Evaluates a flat arithmetic expression the way the DataFusion writer would.
///
/// Operand types are coerced with DataFusion's own `BinaryTypeCoercer` and the
/// operation is applied with the same kernels `BinaryExpr::evaluate` selects:
/// wrapping for `+ - *` (the physical expression is built with
/// `fail_on_overflow` off) and checked for `/ %`. A column operand is first
/// cast to the Arrow type its surface presents, so the arithmetic sees the same
/// input value the DataFusion writer would read from the entity table rather
/// than whatever shape the JSON snapshot happens to hold.
fn eval_binary_entity_value(
    left: &BoundExpr,
    op: BoundBinaryOperator,
    right: &BoundExpr,
    context: &EntityEvalContext<'_>,
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<EntityEvalValue, LixError> {
    let left_scalar =
        binary_operand_scalar(left, context, ctx, params, active_branch_commit_id)?;
    let right_scalar =
        binary_operand_scalar(right, context, ctx, params, active_branch_commit_id)?;
    let operator = match op {
        BoundBinaryOperator::Add => Operator::Plus,
        BoundBinaryOperator::Subtract => Operator::Minus,
        BoundBinaryOperator::Multiply => Operator::Multiply,
        BoundBinaryOperator::Divide => Operator::Divide,
        BoundBinaryOperator::Modulo => Operator::Modulo,
    };
    let (left_type, right_type) = BinaryTypeCoercer::new(
        &left_scalar.data_type(),
        &operator,
        &right_scalar.data_type(),
    )
    .get_input_types()
    .map_err(|error| {
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("SQL arithmetic operands cannot be combined: {error}"),
        )
    })?;
    let left_scalar = left_scalar.cast_to(&left_type).map_err(|error| {
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("SQL arithmetic left operand is not numeric: {error}"),
        )
    })?;
    let right_scalar = right_scalar.cast_to(&right_type).map_err(|error| {
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("SQL arithmetic right operand is not numeric: {error}"),
        )
    })?;
    let result = match op {
        BoundBinaryOperator::Add => left_scalar.add(&right_scalar),
        BoundBinaryOperator::Subtract => left_scalar.sub(&right_scalar),
        BoundBinaryOperator::Multiply => left_scalar.mul(&right_scalar),
        BoundBinaryOperator::Divide => left_scalar.div(&right_scalar),
        BoundBinaryOperator::Modulo => left_scalar.rem(&right_scalar),
    }
    .map_err(|error| {
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("SQL arithmetic failed: {error}"),
        )
    })?;
    #[cfg(test)]
    FAST_BINARY_ARITHMETIC_EVALUATIONS
        .with(|evaluations| evaluations.set(evaluations.get().saturating_add(1)));
    entity_eval_value_from_arithmetic_scalar(result)
}

fn binary_operand_scalar(
    expr: &BoundExpr,
    context: &EntityEvalContext<'_>,
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<ScalarValue, LixError> {
    let value = eval_expr_value(expr, context, ctx, params, active_branch_commit_id)?;
    let scalar = scalar_from_entity_eval_value(value);
    let BoundExpr::Column(column) = expr else {
        return Ok(scalar);
    };
    let Some(surface_column) = context
        .visible_columns
        .iter()
        .find(|candidate| candidate.name == column.name)
    else {
        return Ok(scalar);
    };
    let target_type = arrow_data_type_for_entity_column_type(surface_column.column_type);
    if scalar.data_type() == target_type {
        return Ok(scalar);
    }
    scalar.cast_to(&target_type).map_err(|error| {
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!(
                "column '{}' does not hold a {target_type} value for SQL arithmetic: {error}",
                column.name
            ),
        )
    })
}

fn entity_eval_value_from_arithmetic_scalar(
    value: ScalarValue,
) -> Result<EntityEvalValue, LixError> {
    if value.is_null() {
        return Ok(EntityEvalValue::SqlNull);
    }
    match value {
        ScalarValue::Int64(Some(value)) => Ok(EntityEvalValue::Json(JsonValue::Number(
            value.into(),
        ))),
        ScalarValue::UInt64(Some(value)) => Ok(EntityEvalValue::Json(JsonValue::Number(
            value.into(),
        ))),
        ScalarValue::Float64(Some(value)) => serde_json::Number::from_f64(value)
            .map(JsonValue::Number)
            .map(EntityEvalValue::Json)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "SQL arithmetic produced a non-finite number",
                )
            }),
        other => Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("SQL arithmetic produced unsupported value {other:?}"),
        )),
    }
}

fn predicate_matches(
    predicate: &BoundPredicate,
    context: &EntityEvalContext<'_>,
    spec: &EntitySurfaceSpec,
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<bool, LixError> {
    use crate::sql2::plan::predicate::BoundPredicate;
    match predicate {
        BoundPredicate::True => Ok(true),
        BoundPredicate::False => Ok(false),
        BoundPredicate::And(predicates) => {
            for predicate in predicates {
                if !predicate_matches(
                    predicate,
                    context,
                    spec,
                    ctx,
                    params,
                    active_branch_commit_id,
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        BoundPredicate::Or(predicates) => {
            for predicate in predicates {
                if predicate_matches(
                    predicate,
                    context,
                    spec,
                    ctx,
                    params,
                    active_branch_commit_id,
                )? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        BoundPredicate::Eq(left, right) => {
            let left_value = eval_expr(left, context, ctx, params, active_branch_commit_id)?;
            let right_value = eval_expr(right, context, ctx, params, active_branch_commit_id)?;
            comparison_values_equal(left, left_value, right, right_value, spec)
        }
        BoundPredicate::Like { .. } => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "bound entity writes do not support LIKE predicates",
        )),
        BoundPredicate::IsNull(expr) => {
            let value = eval_expr(expr, context, ctx, params, active_branch_commit_id)?;
            Ok(value.is_null())
        }
        BoundPredicate::IsNotNull(expr) => {
            let value = eval_expr(expr, context, ctx, params, active_branch_commit_id)?;
            Ok(!value.is_null())
        }
        BoundPredicate::In { expr, values } => {
            let candidate = eval_expr(expr, context, ctx, params, active_branch_commit_id)?;
            if candidate.is_null() {
                return Ok(false);
            }
            for value_expr in values {
                let value = eval_expr(value_expr, context, ctx, params, active_branch_commit_id)?;
                if comparison_values_equal(expr, candidate.clone(), value_expr, value, spec)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum NumericComparisonValue {
    Signed(i64),
    Unsigned(u64),
    Double(f64),
}

fn comparison_values_equal(
    left_expr: &BoundExpr,
    mut left_value: JsonValue,
    right_expr: &BoundExpr,
    mut right_value: JsonValue,
    spec: &EntitySurfaceSpec,
) -> Result<bool, LixError> {
    normalize_bigint_comparison_literal(left_expr, right_expr, &mut right_value, spec)?;
    normalize_bigint_comparison_literal(right_expr, left_expr, &mut left_value, spec)?;
    let (left_value, right_value) =
        normalize_comparison_operands(left_expr, left_value, right_expr, right_value, spec)?;
    if left_value.is_null() || right_value.is_null() {
        return Ok(false);
    }

    let left_numeric = numeric_comparison_value(left_expr, &left_value, spec)?;
    let right_numeric = numeric_comparison_value(right_expr, &right_value, spec)?;
    match (left_numeric, right_numeric) {
        (Some(left), Some(right)) => Ok(numeric_values_equal(left, right)),
        _ => Ok(left_value == right_value),
    }
}

fn normalize_bigint_comparison_literal(
    column_expr: &BoundExpr,
    value_expr: &BoundExpr,
    value: &mut JsonValue,
    spec: &EntitySurfaceSpec,
) -> Result<(), LixError> {
    let Some(column) = visible_entity_column(column_expr, spec) else {
        return Ok(());
    };
    if column.column_type != EntityColumnType::Integer {
        return Ok(());
    }
    if let Some(exact) = bigint_number_literal(value_expr, &spec.schema_key, &column.name)? {
        *value = JsonValue::from(exact);
    }
    Ok(())
}

fn numeric_comparison_value(
    expr: &BoundExpr,
    value: &JsonValue,
    spec: &EntitySurfaceSpec,
) -> Result<Option<NumericComparisonValue>, LixError> {
    if let Some(column) = visible_entity_column(expr, spec) {
        return match column.column_type {
            EntityColumnType::Integer => {
                json_bigint_value(Some(value), &spec.schema_key, &column.name)
                    .map(|value| value.map(NumericComparisonValue::Signed))
            }
            EntityColumnType::Number => {
                json_double_value(Some(value), &spec.schema_key, &column.name)
                    .map(|value| value.map(NumericComparisonValue::Double))
            }
            EntityColumnType::String | EntityColumnType::Json | EntityColumnType::Boolean => {
                Ok(None)
            }
        };
    }

    let JsonValue::Number(number) = value else {
        return Ok(None);
    };
    if let Some(value) = number.as_i64() {
        return Ok(Some(NumericComparisonValue::Signed(value)));
    }
    if let Some(value) = number.as_u64() {
        return Ok(Some(NumericComparisonValue::Unsigned(value)));
    }
    Ok(number.as_f64().map(NumericComparisonValue::Double))
}

#[expect(
    clippy::cast_precision_loss,
    clippy::float_cmp,
    reason = "SQL numeric equality coerces mixed BIGINT/DOUBLE operands to DOUBLE PRECISION"
)]
fn numeric_values_equal(left: NumericComparisonValue, right: NumericComparisonValue) -> bool {
    match (left, right) {
        (NumericComparisonValue::Signed(left), NumericComparisonValue::Signed(right)) => {
            left == right
        }
        (NumericComparisonValue::Unsigned(left), NumericComparisonValue::Unsigned(right)) => {
            left == right
        }
        (NumericComparisonValue::Signed(left), NumericComparisonValue::Unsigned(right))
        | (NumericComparisonValue::Unsigned(right), NumericComparisonValue::Signed(left)) => {
            u64::try_from(left).is_ok_and(|left| left == right)
        }
        (NumericComparisonValue::Double(left), NumericComparisonValue::Double(right)) => {
            left == right
        }
        (NumericComparisonValue::Double(left), NumericComparisonValue::Signed(right))
        | (NumericComparisonValue::Signed(right), NumericComparisonValue::Double(left)) => {
            left == right as f64
        }
        (NumericComparisonValue::Double(left), NumericComparisonValue::Unsigned(right))
        | (NumericComparisonValue::Unsigned(right), NumericComparisonValue::Double(left)) => {
            left == right as f64
        }
    }
}

fn normalize_comparison_operands(
    left_expr: &BoundExpr,
    left_value: JsonValue,
    right_expr: &BoundExpr,
    right_value: JsonValue,
    spec: &EntitySurfaceSpec,
) -> Result<(JsonValue, JsonValue), LixError> {
    let left_is_json = bound_expr_is_json(left_expr, spec);
    let right_is_json = bound_expr_is_json(right_expr, spec);
    Ok((
        normalize_json_comparison_value(
            left_expr,
            left_value,
            right_is_json,
            is_identity_json_expr(right_expr),
        )?,
        normalize_json_comparison_value(
            right_expr,
            right_value,
            left_is_json,
            is_identity_json_expr(left_expr),
        )?,
    ))
}

fn normalize_json_comparison_value(
    expr: &BoundExpr,
    value: JsonValue,
    other_side_is_json: bool,
    other_side_is_identity_json: bool,
) -> Result<JsonValue, LixError> {
    if !other_side_is_json {
        return Ok(value);
    }
    let should_parse = matches!(expr, BoundExpr::Param(_))
        || (other_side_is_identity_json
            && matches!(expr, BoundExpr::Literal(BoundLiteral::Text(_))));
    if !should_parse {
        return Ok(value);
    }
    let JsonValue::String(raw) = value else {
        return Ok(value);
    };
    serde_json::from_str(&raw).map_err(|error| {
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("JSON comparison parameter is not valid JSON: {error}"),
        )
    })
}

fn validate_bound_write_supported(
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
) -> Result<(), LixError> {
    validate_predicate_supported(&plan.bound.predicate)?;
    validate_json_predicate_types(&plan.bound.predicate, spec)?;
    match &plan.bound.input {
        BoundWriteInput::Values(values) => {
            for row in &values.rows {
                for expr in row {
                    validate_expr_supported(expr)?;
                }
            }
        }
        BoundWriteInput::Query { .. } | BoundWriteInput::None => {}
    }
    for assignment in &plan.bound.assignments {
        validate_expr_supported(&assignment.value)?;
    }
    if let Some(conflict) = &plan.bound.conflict {
        for assignment in conflict.action.assignments() {
            validate_expr_supported(&assignment.value)?;
        }
    }
    if let Some(returning) = &plan.bound.returning {
        for item in &returning.items {
            validate_expr_supported(&item.expr)?;
        }
    }
    Ok(())
}

/// Whether a direct entity `RETURNING` projection is fully evaluated before
/// staging. The explicit-transaction boundary uses this to avoid snapshotting
/// a large journal for the common `RETURNING id, ...` case; audit fields still
/// require a rollback checkpoint because they are populated by staging.
pub(crate) fn entity_returning_projects_before_stage(plan: &LogicalWritePlan) -> bool {
    if !matches!(plan.bound.target, BoundWriteTarget::Entity(_))
        || !bound_public_write_shape_supported(plan)
    {
        return false;
    }
    match plan.bound.op {
        // Entity DELETE captures the preimage before it stages tombstones.
        BoundWriteOp::Delete => true,
        BoundWriteOp::Insert | BoundWriteOp::Update => {
            plan.bound.returning.as_ref().is_some_and(|returning| {
                returning
                    .items
                    .iter()
                    .all(|item| !returning_expr_requires_staged_postimage(&item.expr))
            })
        }
    }
}

fn returning_expr_requires_staged_postimage(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::Column(column)
            if matches!(
                column.name.as_str(),
                "lixcol_created_at" | "lixcol_updated_at" | "lixcol_change_id" | "lixcol_commit_id"
            ) =>
        {
            true
        }
        BoundExpr::Cast { expr, .. } => returning_expr_requires_staged_postimage(expr),
        BoundExpr::Function { args, .. } => {
            args.iter().any(returning_expr_requires_staged_postimage)
        }
        BoundExpr::Binary { left, right, .. } => {
            returning_expr_requires_staged_postimage(left)
                || returning_expr_requires_staged_postimage(right)
        }
        BoundExpr::Column(_)
        | BoundExpr::ExcludedColumn(_)
        | BoundExpr::Param(_)
        | BoundExpr::Literal(_) => false,
    }
}

fn bound_public_write_shape_supported(plan: &LogicalWritePlan) -> bool {
    let input_supported = match (&plan.bound.op, &plan.bound.input) {
        (BoundWriteOp::Insert, BoundWriteInput::Values(values)) => values
            .rows
            .iter()
            .flatten()
            .all(|expr| validate_expr_supported(expr).is_ok()),
        (BoundWriteOp::Update | BoundWriteOp::Delete, BoundWriteInput::None) => true,
        _ => false,
    };
    input_supported
        && validate_predicate_supported(&plan.bound.predicate).is_ok()
        && plan
            .bound
            .assignments
            .iter()
            .all(|assignment| validate_expr_supported(&assignment.value).is_ok())
        && plan.bound.conflict.as_ref().is_none_or(|conflict| {
            conflict
                .action
                .assignments()
                .iter()
                .all(|assignment| validate_expr_supported(&assignment.value).is_ok())
        })
        && plan.bound.returning.as_ref().is_none_or(|returning| {
            returning
                .items
                .iter()
                .all(|item| validate_expr_supported(&item.expr).is_ok())
        })
}

fn validate_predicate_supported(predicate: &BoundPredicate) -> Result<(), LixError> {
    use crate::sql2::plan::predicate::BoundPredicate;
    match predicate {
        BoundPredicate::True | BoundPredicate::False => Ok(()),
        BoundPredicate::And(predicates) | BoundPredicate::Or(predicates) => {
            for predicate in predicates {
                validate_predicate_supported(predicate)?;
            }
            Ok(())
        }
        BoundPredicate::Eq(left, right) => {
            validate_expr_supported(left)?;
            validate_expr_supported(right)
        }
        // Entity deletes with LIKE use the generic DataFusion write path so
        // the predicate has exactly the same Arrow/DataFusion semantics as
        // every other writable surface.
        BoundPredicate::Like { .. } => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "bound entity writes do not support LIKE predicates",
        )),
        BoundPredicate::IsNull(expr) | BoundPredicate::IsNotNull(expr) => {
            validate_expr_supported(expr)
        }
        BoundPredicate::In { expr, values } => {
            validate_expr_supported(expr)?;
            for value in values {
                validate_expr_supported(value)?;
            }
            Ok(())
        }
    }
}

fn validate_json_predicate_types(
    predicate: &BoundPredicate,
    spec: &EntitySurfaceSpec,
) -> Result<(), LixError> {
    use crate::sql2::plan::predicate::BoundPredicate;
    match predicate {
        BoundPredicate::True
        | BoundPredicate::False
        | BoundPredicate::Like { .. }
        | BoundPredicate::IsNull(_)
        | BoundPredicate::IsNotNull(_) => Ok(()),
        BoundPredicate::And(predicates) | BoundPredicate::Or(predicates) => {
            for predicate in predicates {
                validate_json_predicate_types(predicate, spec)?;
            }
            Ok(())
        }
        BoundPredicate::Eq(left, right) => validate_json_comparison_operands(left, right, spec),
        BoundPredicate::In { expr, values } => {
            if bound_expr_is_json(expr, spec) {
                for value in values {
                    if is_identity_json_expr(expr) && is_parseable_json_text_literal(value) {
                        continue;
                    }
                    require_json_comparison_operand(value, spec)?;
                }
            }
            for value in values {
                if bound_expr_is_json(value, spec) {
                    if is_identity_json_expr(value) && is_parseable_json_text_literal(expr) {
                        continue;
                    }
                    require_json_comparison_operand(expr, spec)?;
                }
            }
            Ok(())
        }
    }
}

fn validate_json_comparison_operands(
    left: &BoundExpr,
    right: &BoundExpr,
    spec: &EntitySurfaceSpec,
) -> Result<(), LixError> {
    if bound_expr_is_json(left, spec) {
        if is_identity_json_expr(left) && is_parseable_json_text_literal(right) {
            return Ok(());
        }
        require_json_comparison_operand(right, spec)?;
    }
    if bound_expr_is_json(right, spec) {
        if is_identity_json_expr(right) && is_parseable_json_text_literal(left) {
            return Ok(());
        }
        require_json_comparison_operand(left, spec)?;
    }
    Ok(())
}

fn require_json_comparison_operand(
    expr: &BoundExpr,
    spec: &EntitySurfaceSpec,
) -> Result<(), LixError> {
    if bound_expr_is_json(expr, spec)
        || matches!(expr, BoundExpr::Param(_))
        || matches!(expr, BoundExpr::Literal(BoundLiteral::Null))
    {
        return Ok(());
    }
    Err(LixError::new(
        LixError::CODE_TYPE_MISMATCH,
        "JSON columns can only be compared with JSON expressions",
    )
    .with_hint("Wrap JSON text with lix_json(...), use lix_json_get(...) for JSON values, or use IS NULL for null checks."))
}

fn is_identity_json_expr(expr: &BoundExpr) -> bool {
    matches!(
        expr,
        BoundExpr::Column(column) | BoundExpr::ExcludedColumn(column)
            if matches!(column.name.as_str(), "entity_pk" | "lixcol_entity_pk")
    )
}

fn is_parseable_json_text_literal(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::Literal(BoundLiteral::Text(value)) => {
            serde_json::from_str::<JsonValue>(value).is_ok()
        }
        _ => false,
    }
}

fn bound_expr_is_json(expr: &BoundExpr, spec: &EntitySurfaceSpec) -> bool {
    match expr {
        BoundExpr::Column(column) | BoundExpr::ExcludedColumn(column) => {
            spec.visible_column(&column.name)
                .is_some_and(|column| column.column_type == EntityColumnType::Json)
                || matches!(column.name.as_str(), "lixcol_entity_pk" | "lixcol_metadata")
        }
        BoundExpr::Literal(BoundLiteral::Json(_)) => true,
        BoundExpr::Function { name, .. } => matches!(name.as_str(), "lix_json" | "lix_json_get"),
        _ => false,
    }
}

fn validate_expr_supported(expr: &BoundExpr) -> Result<(), LixError> {
    match expr {
        BoundExpr::Column(_)
        | BoundExpr::ExcludedColumn(_)
        | BoundExpr::Param(_)
        | BoundExpr::Literal(_) => Ok(()),
        BoundExpr::Cast { expr, .. } => validate_expr_supported(expr),
        BoundExpr::Binary { left, right, .. } => {
            validate_binary_operand_supported(left)?;
            validate_binary_operand_supported(right)
        }
        BoundExpr::Function { name, args } => {
            match name.as_str() {
                "lix_json" if args.len() == 1 => {}
                "lix_uuid_v7"
                | "lix_timestamp"
                | "lix_active_branch_id"
                | "lix_active_branch_commit_id"
                    if args.is_empty() => {}
                "lix_json_get" | "lix_json_get_text" if args.len() >= 2 => {}
                _ => {
                    return Err(LixError::new(
                        LixError::CODE_UNSUPPORTED_SQL,
                        format!("bound entity write does not support function '{name}' yet"),
                    ));
                }
            }
            for arg in args {
                validate_expr_supported(arg)?;
            }
            Ok(())
        }
    }
}

/// Arithmetic admitted by the fast entity writer.
///
/// Only a flat `<operand> <arith op> <operand>` is admitted, where each operand
/// is a bare column, parameter, or literal -- the `SET k = k + 1` family. Any
/// nesting, cast, or function call inside the arithmetic keeps the whole
/// statement on the generic DataFusion writer, which evaluates arbitrary
/// expressions.
fn validate_binary_operand_supported(expr: &BoundExpr) -> Result<(), LixError> {
    match expr {
        BoundExpr::Column(_) | BoundExpr::Param(_) | BoundExpr::Literal(_) => Ok(()),
        BoundExpr::ExcludedColumn(_)
        | BoundExpr::Cast { .. }
        | BoundExpr::Function { .. }
        | BoundExpr::Binary { .. } => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "bound entity write evaluates compound binary expressions through DataFusion",
        )),
    }
}

fn candidate_snapshot<'a>(
    row: impl Into<EntityLiveRowRef<'a>>,
) -> Result<Option<JsonValue>, LixError> {
    row.into()
        .snapshot_content()
        .map(|snapshot| {
            serde_json::from_str(snapshot).map_err(|error| {
                LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    format!("entity row snapshot_content is not valid JSON: {error}"),
                )
            })
        })
        .transpose()
}

fn entity_json_value(
    expr: &BoundExpr,
    value: EntityEvalValue,
    column_type: EntityColumnType,
    schema_key: &str,
    column_name: &str,
) -> Result<JsonValue, LixError> {
    let exact_bigint_literal = if column_type == EntityColumnType::Integer {
        bigint_number_literal(expr, schema_key, column_name)?
    } else {
        None
    };
    let value = exact_bigint_literal.map_or_else(
        || match (value, column_type) {
            (EntityEvalValue::SqlNull, _) => JsonValue::Null,
            (EntityEvalValue::SqlText(value), EntityColumnType::Json) => {
                serde_json::from_str(&value).unwrap_or(JsonValue::String(value))
            }
            (EntityEvalValue::SqlText(value), _) => JsonValue::String(value),
            (EntityEvalValue::Json(JsonValue::String(value)), EntityColumnType::String) => {
                JsonValue::String(value)
            }
            (
                EntityEvalValue::Json(JsonValue::Number(value)),
                EntityColumnType::Number | EntityColumnType::Integer,
            ) => JsonValue::Number(value),
            (EntityEvalValue::Json(JsonValue::Bool(value)), EntityColumnType::Boolean) => {
                JsonValue::Bool(value)
            }
            (EntityEvalValue::Json(value), _) => value,
        },
        JsonValue::from,
    );
    match column_type {
        EntityColumnType::Integer => {
            json_bigint_value(Some(&value), schema_key, column_name)?;
        }
        EntityColumnType::Number => {
            json_double_value(Some(&value), schema_key, column_name)?;
        }
        EntityColumnType::String | EntityColumnType::Json | EntityColumnType::Boolean => {}
    }
    Ok(value)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BigintNumberLiteral {
    Exact(i64),
    NonIntegral,
}

fn bigint_number_literal(
    expr: &BoundExpr,
    schema_key: &str,
    column_name: &str,
) -> Result<Option<i64>, LixError> {
    let raw = match expr {
        BoundExpr::Literal(BoundLiteral::Number { raw, .. }) => raw,
        BoundExpr::Cast {
            expr,
            data_type: BoundCastType::BigInt,
        } => return bigint_number_literal(expr, schema_key, column_name),
        _ => return Ok(None),
    };
    let Some(BigintNumberLiteral::Exact(value)) = classify_bigint_literal(raw) else {
        return Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!(
                "typed SQL surface '{schema_key}' column '{column_name}' cannot represent SQL numeric literal {raw} as BIGINT"
            ),
        )
        .with_hint(
            "Use an exact integer between -9223372036854775808 and 9223372036854775807.",
        ));
    };
    Ok(Some(value))
}

fn classify_bigint_literal(raw: &str) -> Option<BigintNumberLiteral> {
    let (negative, unsigned) = raw.strip_prefix('-').map_or_else(
        || (false, raw.strip_prefix('+').unwrap_or(raw)),
        |unsigned| (true, unsigned),
    );
    let (mantissa, exponent) = if let Some((mantissa, exponent)) = unsigned.split_once(['e', 'E']) {
        if exponent.contains(['e', 'E']) {
            return None;
        }
        (mantissa, exponent.parse::<i64>().ok()?)
    } else {
        (unsigned, 0)
    };
    let (integer_digits, fractional_digits) =
        if let Some((integer_digits, fractional_digits)) = mantissa.split_once('.') {
            if fractional_digits.contains('.') {
                return None;
            }
            (integer_digits, fractional_digits)
        } else {
            (mantissa, "")
        };
    if integer_digits.is_empty() && fractional_digits.is_empty() {
        return None;
    }
    if !integer_digits.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional_digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let mut digits = String::with_capacity(integer_digits.len() + fractional_digits.len());
    digits.push_str(integer_digits);
    digits.push_str(fractional_digits);
    if digits.bytes().all(|byte| byte == b'0') {
        return Some(BigintNumberLiteral::Exact(0));
    }

    let fractional_len = i64::try_from(fractional_digits.len()).ok()?;
    let decimal_shift = exponent.checked_sub(fractional_len)?;
    if decimal_shift >= 0 {
        let significant = digits.trim_start_matches('0');
        let trailing_zeros = usize::try_from(decimal_shift).ok()?;
        if significant.len().checked_add(trailing_zeros)? > 19 {
            return None;
        }
        let mut magnitude = String::with_capacity(significant.len() + trailing_zeros);
        magnitude.push_str(significant);
        magnitude.extend(std::iter::repeat_n('0', trailing_zeros));
        return signed_bigint_magnitude(&magnitude, negative).map(BigintNumberLiteral::Exact);
    }

    let removed_digits = usize::try_from(decimal_shift.unsigned_abs()).ok()?;
    if removed_digits > digits.len() {
        return Some(BigintNumberLiteral::NonIntegral);
    }
    let split = digits.len() - removed_digits;
    let integer_magnitude = digits[..split].trim_start_matches('0');
    let fractional_is_zero = digits[split..].bytes().all(|byte| byte == b'0');
    if fractional_is_zero {
        let integer_magnitude = if integer_magnitude.is_empty() {
            "0"
        } else {
            integer_magnitude
        };
        return signed_bigint_magnitude(integer_magnitude, negative)
            .map(BigintNumberLiteral::Exact);
    }
    if non_integral_magnitude_is_in_bigint_range(integer_magnitude, negative) {
        Some(BigintNumberLiteral::NonIntegral)
    } else {
        None
    }
}

fn signed_bigint_magnitude(magnitude: &str, negative: bool) -> Option<i64> {
    let maximum = if negative {
        "9223372036854775808"
    } else {
        "9223372036854775807"
    };
    if magnitude.len() > maximum.len() || (magnitude.len() == maximum.len() && magnitude > maximum)
    {
        return None;
    }
    let magnitude = magnitude.parse::<u64>().ok()?;
    if negative {
        if magnitude == 9_223_372_036_854_775_808_u64 {
            Some(i64::MIN)
        } else {
            i64::try_from(magnitude).ok().map(|value| -value)
        }
    } else {
        i64::try_from(magnitude).ok()
    }
}

fn non_integral_magnitude_is_in_bigint_range(magnitude: &str, negative: bool) -> bool {
    let maximum_integer_part = if negative {
        "9223372036854775807"
    } else {
        "9223372036854775806"
    };
    magnitude.len() < maximum_integer_part.len()
        || (magnitude.len() == maximum_integer_part.len() && magnitude <= maximum_integer_part)
}

fn reject_direct_blob_json_value(
    expr: &BoundExpr,
    column_type: EntityColumnType,
    params: &[Value],
) -> Result<(), LixError> {
    if column_type != EntityColumnType::Json {
        return Ok(());
    }
    let is_blob = match expr {
        BoundExpr::Param(param) => params
            .get(param.index.saturating_sub(1))
            .is_some_and(|value| matches!(value, Value::Blob(_))),
        _ => false,
    };
    if is_blob {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "cannot store blob values directly in JSON entity columns",
        ));
    }
    Ok(())
}

fn literal_json(literal: &BoundLiteral) -> JsonValue {
    match literal {
        BoundLiteral::Null => JsonValue::Null,
        BoundLiteral::Bool(value) => JsonValue::Bool(*value),
        BoundLiteral::Integer(value) => JsonValue::from(*value),
        BoundLiteral::Number { value, .. } => JsonValue::Number(value.clone()),
        BoundLiteral::Text(value) => JsonValue::String(value.clone()),
        BoundLiteral::Json(value) => value.clone(),
    }
}

fn value_eval(value: &Value) -> EntityEvalValue {
    match value {
        Value::Null => EntityEvalValue::SqlNull,
        Value::Text(value) => EntityEvalValue::SqlText(value.clone()),
        _ => EntityEvalValue::Json(value_json(value)),
    }
}

fn value_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Boolean(value) => JsonValue::Bool(*value),
        Value::Integer(value) => JsonValue::from(*value),
        Value::Real(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Text(value) => JsonValue::String(value.clone()),
        Value::Json(value) => value.to_value(),
        Value::Blob(value) => {
            JsonValue::Array(value.iter().copied().map(JsonValue::from).collect())
        }
    }
}

fn json_path_get(
    value: &JsonValue,
    segment: &JsonValue,
    fn_name: &str,
) -> Result<Option<JsonValue>, LixError> {
    match segment {
        JsonValue::String(key) => {
            if key == "$" || key.starts_with("$.") || key.starts_with("$[") || key.starts_with('/')
            {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    format!(
                        "{fn_name}() uses variadic path segments, not JSONPath or JSON Pointer; got '{key}'"
                    ),
                ));
            }
            Ok(value.get(key).cloned())
        }
        JsonValue::Number(number) => {
            let Some(index) = number
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
            else {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    format!("{fn_name}() path indexes must be non-negative integers"),
                ));
            };
            Ok(value
                .as_array()
                .and_then(|values| values.get(index))
                .cloned())
        }
        JsonValue::Null => Ok(None),
        other => Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!(
                "{fn_name}() path arguments must be strings or non-negative integers, got {other}"
            ),
        )),
    }
}

fn json_text_value(value: &JsonValue) -> Result<String, LixError> {
    match value {
        JsonValue::String(text) => Ok(text.clone()),
        JsonValue::Number(number) => Ok(number.to_string()),
        JsonValue::Bool(boolean) => Ok(boolean.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            serde_json::to_string(value).map_err(|error| {
                LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    format!("lix_json_get_text() could not render JSON value: {error}"),
                )
            })
        }
        JsonValue::Null => Ok("null".to_string()),
    }
}

fn column_eval_value(
    context: &EntityEvalContext<'_>,
    column_name: &str,
) -> Result<EntityEvalValue, LixError> {
    if let Some(value) = context.snapshot.get(column_name) {
        return Ok(visible_column_eval_value(
            context
                .visible_columns
                .iter()
                .find(|column| column.name == column_name),
            value,
        ));
    }
    let Some(row) = context.row else {
        return Ok(EntityEvalValue::SqlNull);
    };
    match column_name {
        "lixcol_entity_pk" => row
            .entity_pk()
            .map(EntityPk::as_json_array_value)
            .transpose()
            .map(|value| {
                value
                    .map(EntityEvalValue::Json)
                    .unwrap_or(EntityEvalValue::SqlNull)
            }),
        "lixcol_schema_key" => Ok(EntityEvalValue::Json(JsonValue::String(
            row.schema_key().to_string(),
        ))),
        "lixcol_file_id" => Ok(row
            .file_id()
            .map(|value| EntityEvalValue::Json(JsonValue::String(value.to_string())))
            .unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_metadata" => row.metadata().map(|metadata| {
            metadata
                .map(EntityEvalValue::Json)
                .unwrap_or(EntityEvalValue::SqlNull)
        }),
        "lixcol_change_id" => Ok(row
            .change_id()
            .map(|value| EntityEvalValue::Json(JsonValue::String(value.to_string())))
            .unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_created_at" => Ok(row
            .created_at()
            .map(|value| EntityEvalValue::Json(JsonValue::String(value)))
            .unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_updated_at" => Ok(row
            .updated_at()
            .map(|value| EntityEvalValue::Json(JsonValue::String(value)))
            .unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_commit_id" => Ok(row
            .commit_id()
            .map(|value| EntityEvalValue::Json(JsonValue::String(value.to_string())))
            .unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_global" => Ok(EntityEvalValue::Json(JsonValue::Bool(row.global()))),
        "lixcol_untracked" => Ok(EntityEvalValue::Json(JsonValue::Bool(row.untracked()))),
        "lixcol_branch_id" => Ok(EntityEvalValue::Json(JsonValue::String(
            row.branch_id().to_string(),
        ))),
        _ => Ok(EntityEvalValue::SqlNull),
    }
}

fn excluded_column_eval_value(
    context: &EntityEvalContext<'_>,
    column_name: &str,
) -> Result<EntityEvalValue, LixError> {
    let Some(excluded_snapshot) = context.excluded_snapshot else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "excluded columns are only available in INSERT ON CONFLICT assignments",
        ));
    };
    if let Some(value) = excluded_snapshot.get(column_name) {
        return Ok(visible_column_eval_value(
            context
                .visible_columns
                .iter()
                .find(|column| column.name == column_name),
            value,
        ));
    }
    let Some(row) = context.excluded_row else {
        return Ok(EntityEvalValue::SqlNull);
    };
    match column_name {
        "lixcol_entity_pk" => row
            .entity_pk
            .map(|entity_pk| entity_pk.as_json_array_value().map(EntityEvalValue::Json))
            .transpose()
            .map(|value| value.unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_schema_key" => Ok(EntityEvalValue::Json(JsonValue::String(
            row.schema_key.to_string(),
        ))),
        "lixcol_file_id" => Ok(row
            .file_id
            .map(|value| EntityEvalValue::Json(JsonValue::String(value.to_string())))
            .unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_metadata" => row
            .metadata
            .map(|metadata| Ok(EntityEvalValue::Json(metadata.value().clone())))
            .transpose()
            .map(|metadata| metadata.unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_global" => Ok(EntityEvalValue::Json(JsonValue::Bool(row.global))),
        "lixcol_untracked" => Ok(EntityEvalValue::Json(JsonValue::Bool(row.untracked))),
        "lixcol_branch_id" => Ok(EntityEvalValue::Json(JsonValue::String(
            row.branch_id.to_string(),
        ))),
        _ => Ok(EntityEvalValue::SqlNull),
    }
}

fn visible_column_eval_value(
    column: Option<&EntitySurfaceColumn>,
    value: &JsonValue,
) -> EntityEvalValue {
    match (column.map(|column| column.column_type), value) {
        (Some(EntityColumnType::String), JsonValue::String(value)) => {
            EntityEvalValue::SqlText(value.clone())
        }
        _ => EntityEvalValue::Json(value.clone()),
    }
}

fn scan_branch_ids(scope: &BranchScope) -> Result<Vec<String>, LixError> {
    Ok(match scope {
        BranchScope::Active { branch_id } => vec![branch_id.clone()],
        BranchScope::Explicit { branch_ids } | BranchScope::ExplicitRequired { branch_ids } => {
            branch_ids.iter().cloned().collect()
        }
        BranchScope::ExplicitDynamic { .. } | BranchScope::ExplicitRequiredDynamic { .. } => {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "parameterized branch scope was not resolved before write execution",
            ));
        }
        BranchScope::Global => vec![crate::GLOBAL_BRANCH_ID.to_string()],
        BranchScope::Empty => Vec::new(),
    })
}

fn entity_row_branch_id(
    plan: &LogicalWritePlan,
    explicit_branch_id: Option<String>,
    global: bool,
) -> Result<String, LixError> {
    if global {
        let target_branch_ids = insert_target_branch_ids(&plan.bound.branch_scope);
        let target_is_by_branch = matches!(
            &plan.bound.target,
            BoundWriteTarget::Entity(EntityWriteSurface::ByBranch { .. })
        );
        if explicit_branch_id
            .as_deref()
            .is_some_and(|branch_id| branch_id != crate::GLOBAL_BRANCH_ID)
        {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "entity INSERT cannot combine lixcol_global = true with a non-global lixcol_branch_id",
            ));
        }
        if target_is_by_branch
            && target_branch_ids.iter().any(|branch_ids| {
                !branch_ids
                    .iter()
                    .any(|branch_id| branch_id == crate::GLOBAL_BRANCH_ID)
            })
        {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "entity INSERT cannot combine lixcol_global = true with a non-global target branch",
            ));
        }
        return Ok(crate::GLOBAL_BRANCH_ID.to_string());
    }
    if explicit_branch_id.as_deref() == Some(crate::GLOBAL_BRANCH_ID) {
        return Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            "entity INSERT with lixcol_branch_id = 'global' must also set lixcol_global = true",
        ));
    }
    let target_is_by_branch = matches!(
        &plan.bound.target,
        BoundWriteTarget::Entity(EntityWriteSurface::ByBranch { .. })
    );
    if target_is_by_branch && matches!(plan.bound.branch_scope, BranchScope::Global) {
        return Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            "entity INSERT into the global scope must set lixcol_global = true",
        ));
    }
    if let Some(branch_id) = explicit_branch_id {
        if target_is_by_branch {
            let target_branch_ids = insert_target_branch_ids(&plan.bound.branch_scope);
            if let Some(target_branch_ids) = &target_branch_ids {
                if !target_branch_ids.contains(&branch_id) {
                    return Err(LixError::new(
                        LixError::CODE_TYPE_MISMATCH,
                        format!(
                            "entity INSERT lixcol_branch_id '{branch_id}' does not match the target branch scope"
                        ),
                    ));
                }
            } else {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "entity INSERT has no target branch scope",
                ));
            }
        }
        return Ok(branch_id);
    }
    match &plan.bound.branch_scope {
        BranchScope::Active { branch_id } => Ok(branch_id.clone()),
        BranchScope::ExplicitRequired { branch_ids } | BranchScope::Explicit { branch_ids }
            if branch_ids.len() == 1 =>
        {
            Ok(branch_ids.iter().next().expect("len checked").clone())
        }
        BranchScope::ExplicitDynamic { .. } | BranchScope::ExplicitRequiredDynamic { .. } => {
            Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "parameterized branch scope was not resolved before write execution",
            ))
        }
        BranchScope::Global | BranchScope::Empty => Ok(crate::GLOBAL_BRANCH_ID.to_string()),
        _ => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "entity write requires exactly one target branch",
        )),
    }
}

fn insert_target_branch_ids(scope: &BranchScope) -> Option<Vec<String>> {
    match scope {
        BranchScope::Active { branch_id } => Some(vec![branch_id.clone()]),
        BranchScope::Explicit { branch_ids } | BranchScope::ExplicitRequired { branch_ids } => {
            Some(branch_ids.iter().cloned().collect())
        }
        BranchScope::ExplicitDynamic { .. } | BranchScope::ExplicitRequiredDynamic { .. } => None,
        BranchScope::Global => Some(vec![crate::GLOBAL_BRANCH_ID.to_string()]),
        BranchScope::Empty => Some(Vec::new()),
    }
}

fn assignment_value<'a>(
    assignments: &'a [BoundAssignment],
    column_name: &str,
) -> Option<&'a BoundExpr> {
    assignments
        .iter()
        .find(|assignment| assignment.column.name == column_name)
        .map(|assignment| &assignment.value)
}

fn optional_metadata_from_eval_value(
    value: EntityEvalValue,
    column_name: &str,
    context: &str,
) -> Result<Option<TransactionJson>, LixError> {
    let metadata = match value {
        EntityEvalValue::SqlNull => return Ok(None),
        EntityEvalValue::SqlText(value) => parse_row_metadata_value(&value, context)?,
        EntityEvalValue::Json(value) => {
            validate_row_metadata(&value, context)?;
            value
        }
    };
    TransactionJson::from_value(metadata, &format!("{context} {column_name}")).map(Some)
}

fn text_value(value: JsonValue, column_name: &str) -> Result<Option<String>, LixError> {
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::String(value) => Ok(Some(value)),
        other => Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("entity write expected text-compatible column '{column_name}', got {other}"),
        )),
    }
}

fn bool_value(value: JsonValue, column_name: &str) -> Result<Option<bool>, LixError> {
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::Bool(value) => Ok(Some(value)),
        other => Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("entity write expected boolean column '{column_name}', got {other}"),
        )),
    }
}

fn entity_pk_from_value(value: &JsonValue, column_name: &str) -> Result<EntityPk, LixError> {
    match value {
        JsonValue::String(value) => EntityPk::from_json_array_text(value).map_err(|error| {
            LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("entity write has invalid {column_name}: {error}"),
            )
        }),
        value => EntityPk::from_json_array_value(value).map_err(|error| {
            LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("entity write has invalid {column_name}: {error}"),
            )
        }),
    }
}

fn entity_action(op: &BoundWriteOp) -> &'static str {
    match op {
        BoundWriteOp::Insert => "INSERT into entity surface",
        BoundWriteOp::Update => "UPDATE entity surface",
        BoundWriteOp::Delete => "DELETE from entity surface",
    }
}

#[cfg(test)]
mod primary_key_route_tests {
    use super::*;
    use crate::sql2::bind::expr::{BoundColumnRef, BoundParamRef};

    #[test]
    fn typed_certified_insert_is_reserved_for_allocation_material_batches() {
        assert!(!use_typed_certified_insert(
            TYPED_CERTIFIED_INSERT_MIN_ROWS - 1
        ));
        assert!(use_typed_certified_insert(TYPED_CERTIFIED_INSERT_MIN_ROWS));
    }

    #[test]
    fn canonical_string_fast_path_matches_serde_for_safe_and_escaped_utf8() {
        for value in ["plain", "café", "quote\"", "slash\\", "line\nfeed", "nul\0"] {
            let mut actual = Vec::new();
            append_canonical_json_string(&mut actual, value)
                .expect("canonical string should serialize");
            assert_eq!(actual, serde_json::to_vec(value).unwrap());
        }
    }

    #[test]
    fn streaming_json_canonicalizer_matches_serde_for_supported_text() {
        let canonical = [
            "null",
            "true",
            "false",
            "0",
            "18446744073709551615",
            "-9223372036854775808",
            r#""plain""#,
            r#""quote\" and slash\\ and line\n""#,
            r#"[0,true,"café",{"a":1,"b":[2,3]}]"#,
            r#"{"a":1,"b":{"c":"value"},"z":null}"#,
        ];
        for raw in canonical {
            let mut actual = Vec::new();
            assert!(CanonicalJsonText::append_normalized(raw, &mut actual));
            let parsed: JsonValue = serde_json::from_str(raw).unwrap();
            assert_eq!(actual, serde_json::to_vec(&parsed).unwrap());
        }

        for raw in [
            " 0",
            "0 ",
            "-0",
            "1.0",
            "1e2",
            "18446744073709551616",
            "\"literal\ncontrol\"",
            r#""unicode \u0061""#,
            r#""escaped\/slash""#,
            r#"{"a":1,"a":2}"#,
            r#"{"escaped\u0061":1}"#,
        ] {
            let mut actual = Vec::new();
            assert!(!CanonicalJsonText::append_normalized(raw, &mut actual));
        }
    }

    #[test]
    fn streaming_json_recursion_limit_matches_serde_and_wide_canonical_objects_stay_direct() {
        for depth in [127, 128, 129] {
            let raw = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
            assert_eq!(
                CanonicalJsonText::recognizes(&raw),
                serde_json::from_str::<JsonValue>(&raw).is_ok(),
                "recognizer depth mismatch at {depth}"
            );
        }

        let wide = format!(
            "{{{}}}",
            (0..16)
                .map(|index| format!("\"k{index:02}\":{index}"))
                .collect::<Vec<_>>()
                .join(",")
        );
        assert!(CanonicalJsonText::recognizes(&wide));
        let mut actual = Vec::new();
        append_canonical_json_parameter(&mut actual, &wide).unwrap();
        assert_eq!(actual, wide.as_bytes());

        let too_deep = format!(
            "{}0{}",
            "[".repeat(usize::from(CanonicalJsonText::MAX_DEPTH) + 1),
            "]".repeat(usize::from(CanonicalJsonText::MAX_DEPTH) + 1)
        );
        assert!(!CanonicalJsonText::append_normalized(
            &too_deep,
            &mut Vec::new()
        ));
        assert!(append_canonical_json_parameter(&mut Vec::new(), &too_deep).is_err());
    }

    #[test]
    fn canonical_json_parameter_preserves_existing_normalization() {
        for raw in [
            " { \"b\" : 1, \"a\" : 2 } ",
            r#"{"value":1.0,"escaped":"\u0061"}"#,
            r#"{"ordinal":42,"lane":"scale","updated":true}"#,
            r#"[{"z":1,"a":{"d":4,"c":3}},2]"#,
        ] {
            let mut actual = Vec::new();
            append_canonical_json_parameter(&mut actual, raw).unwrap();
            let expected =
                serde_json::to_vec(&serde_json::from_str::<JsonValue>(raw).unwrap()).unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn compiles_single_text_primary_key_parameter_once() {
        let spec = crate::sql2::catalog::derive_entity_surface_spec_from_schema(
            &serde_json::json!({
                "x-lix-key": "entity",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "required": ["id", "value"],
                "properties": {
                    "id": { "type": "string" },
                    "value": {
                        "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
                    }
                },
                "additionalProperties": false
            }),
        )
        .expect("entity surface schema should compile");
        assert_eq!(
            bound_single_text_primary_key_param(
                &spec,
                &equals(column("id"), BoundExpr::Param(BoundParamRef { index: 2 }),),
            ),
            Some(1)
        );
        assert_eq!(
            bound_single_text_primary_key_param(
                &spec,
                &equals(
                    column("value"),
                    BoundExpr::Param(BoundParamRef { index: 2 }),
                ),
            ),
            None
        );
    }

    #[test]
    fn routes_literal_and_parameter_primary_keys() {
        let analyzer = BoundPrimaryKeyAnalyzer {
            primary_key_columns: vec!["id"],
            primary_key_component_types: &[crate::entity_pk::EntityPkComponentType::String],
            params: &[Value::Text("from-param".to_string())],
        };
        let predicate = BoundPredicate::Or(vec![
            equals(column("id"), text("literal")),
            equals(column("id"), BoundExpr::Param(BoundParamRef { index: 1 })),
        ]);

        assert_eq!(
            analyzer
                .analyze_conjunctive_constraint(&predicate)
                .expect("identity predicate should route")
                .into_entity_pks(
                    &analyzer.primary_key_columns,
                    analyzer.primary_key_component_types,
                )
                .expect("identity predicate should be complete"),
            std::collections::BTreeSet::from([
                EntityPk::single("from-param"),
                EntityPk::single("literal"),
            ])
        );
    }

    #[test]
    fn routes_typed_integer_primary_key_literals_and_parameters() {
        let component_types = [crate::entity_pk::EntityPkComponentType::Integer];
        let analyzer = BoundPrimaryKeyAnalyzer {
            primary_key_columns: vec!["id"],
            primary_key_component_types: &component_types,
            params: &[Value::Integer(42)],
        };
        let predicate = BoundPredicate::In {
            expr: column("id"),
            values: vec![
                BoundExpr::Literal(BoundLiteral::Integer(7)),
                BoundExpr::Param(BoundParamRef { index: 1 }),
            ],
        };
        let expected = ["7", "42"]
            .into_iter()
            .map(|value| {
                EntityPk::from_external_parts(vec![value.to_string()], &component_types)
                    .expect("integer identity should encode")
            })
            .collect();

        assert_eq!(
            analyzer
                .analyze_conjunctive_constraint(&predicate)
                .expect("typed integer predicate should route")
                .into_entity_pks(
                    &analyzer.primary_key_columns,
                    analyzer.primary_key_component_types,
                )
                .expect("typed integer predicate should be complete"),
            expected
        );
    }

    #[test]
    fn integer_primary_key_rejects_text_parameter_pushdown() {
        let analyzer = BoundPrimaryKeyAnalyzer {
            primary_key_columns: vec!["id"],
            primary_key_component_types: &[crate::entity_pk::EntityPkComponentType::Integer],
            params: &[Value::Text("42".to_string())],
        };

        assert!(
            analyzer
                .analyze_conjunctive_constraint(&equals(
                    column("id"),
                    BoundExpr::Param(BoundParamRef { index: 1 }),
                ))
                .is_none()
        );
    }

    #[test]
    fn routes_guaranteed_conjunct_but_not_partial_disjunction() {
        let analyzer = BoundPrimaryKeyAnalyzer {
            primary_key_columns: vec!["id"],
            primary_key_component_types: &[crate::entity_pk::EntityPkComponentType::String],
            params: &[],
        };
        let conjunct = BoundPredicate::And(vec![
            equals(column("id"), text("entity-a")),
            equals(column("kind"), text("note")),
        ]);
        assert_eq!(
            analyzer
                .analyze_conjunctive_constraint(&conjunct)
                .expect("guaranteed identity conjunct should route")
                .into_entity_pks(
                    &analyzer.primary_key_columns,
                    analyzer.primary_key_component_types,
                )
                .expect("identity conjunct should be complete"),
            std::collections::BTreeSet::from([EntityPk::single("entity-a")])
        );

        let disjunction = BoundPredicate::Or(vec![
            equals(column("id"), text("entity-a")),
            equals(column("kind"), text("note")),
        ]);
        assert!(
            analyzer
                .analyze_conjunctive_constraint(&disjunction)
                .is_none(),
            "a partially routable disjunction must retain the full scan"
        );
    }

    #[test]
    fn routes_composite_primary_key_in_declared_order() {
        let analyzer = BoundPrimaryKeyAnalyzer {
            primary_key_columns: vec!["namespace", "id"],
            primary_key_component_types: &[
                crate::entity_pk::EntityPkComponentType::String,
                crate::entity_pk::EntityPkComponentType::String,
            ],
            params: &[],
        };
        let predicate = BoundPredicate::And(vec![
            BoundPredicate::In {
                expr: column("id"),
                values: vec![text("one"), text("two")],
            },
            equals(column("namespace"), text("docs")),
        ]);

        assert_eq!(
            analyzer
                .analyze_conjunctive_constraint(&predicate)
                .expect("composite predicate should route")
                .into_entity_pks(
                    &analyzer.primary_key_columns,
                    analyzer.primary_key_component_types,
                )
                .expect("composite predicate should be complete"),
            std::collections::BTreeSet::from([
                EntityPk::from_parts(vec!["docs".to_string(), "one".to_string()])
                    .expect("valid entity pk"),
                EntityPk::from_parts(vec!["docs".to_string(), "two".to_string()])
                    .expect("valid entity pk"),
            ])
        );
    }

    #[test]
    fn contradictory_primary_key_conjunct_routes_empty() {
        let analyzer = BoundPrimaryKeyAnalyzer {
            primary_key_columns: vec!["id"],
            primary_key_component_types: &[crate::entity_pk::EntityPkComponentType::String],
            params: &[],
        };
        let predicate = BoundPredicate::And(vec![
            equals(column("id"), text("one")),
            equals(column("id"), text("two")),
        ]);

        assert!(
            analyzer
                .analyze_conjunctive_constraint(&predicate)
                .expect("contradictory identity should still route")
                .into_entity_pks(
                    &analyzer.primary_key_columns,
                    analyzer.primary_key_component_types,
                )
                .expect("identity predicate should be complete")
                .is_empty()
        );
    }

    #[test]
    fn only_constraint_source_assignments_require_commit_validation() {
        let catalog = crate::catalog::CatalogSnapshot::from_visible_schemas(&[
            serde_json::json!({
                "x-lix-key": "parent",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"],
                "additionalProperties": false
            }),
            serde_json::json!({
                "x-lix-key": "child",
                "x-lix-primary-key": ["/id"],
                "x-lix-unique": [["/slug"]],
                "x-lix-foreign-keys": [{
                    "properties": ["/parent_id"],
                    "references": { "schemaKey": "parent", "properties": ["/id"] }
                }],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "parent_id": { "type": "string" },
                    "slug": { "type": "string" },
                    "value": { "type": "string" }
                },
                "required": ["id", "parent_id", "slug", "value"],
                "additionalProperties": false
            }),
        ])
        .expect("constraint schemas should compile");
        let (_, plan) = catalog
            .plan_for_key("child")
            .expect("child schema plan should exist");

        assert!(assigned_columns_preserve_constraints(
            plan,
            &std::collections::HashSet::from(["value"]),
        ));
        for constrained in ["id", "slug", "parent_id"] {
            assert!(!assigned_columns_preserve_constraints(
                plan,
                &std::collections::HashSet::from([constrained]),
            ));
        }
    }

    fn equals(left: BoundExpr, right: BoundExpr) -> BoundPredicate {
        BoundPredicate::Eq(left, right)
    }

    fn column(name: &str) -> BoundExpr {
        BoundExpr::Column(BoundColumnRef {
            table: "entity".to_string(),
            column_id: 0,
            name: name.to_string(),
        })
    }

    fn text(value: &str) -> BoundExpr {
        BoundExpr::Literal(BoundLiteral::Text(value.to_string()))
    }
}
#[cfg(test)]
mod splice_provenance_tests {
    use super::{
        BoundExpr, FastFileContentUpdateShape, fast_file_blob_expr_splice_provenance,
        fast_file_content_update_splice_provenance,
    };
    use crate::common::{ExecuteStatementMetadata, MutationIdentity, RequestBlobSpliceProvenance};
    use crate::sql2::bind::expr::BoundParamRef;

    fn splice(label: &str) -> RequestBlobSpliceProvenance {
        let base = b"base";
        let result: crate::Blob = [base.as_slice(), label.as_bytes()].concat().into();
        RequestBlobSpliceProvenance::new_validated_for_test(
            base,
            &result,
            base.len(),
            0,
            label.as_bytes().to_vec(),
        )
    }

    #[test]
    fn fast_file_content_update_uses_the_bound_data_parameter_metadata() {
        let expected = splice("content");
        let metadata = ExecuteStatementMetadata {
            parameter_blob_splices: vec![Some(splice("unrelated")), None, Some(expected.clone())],
            ..ExecuteStatementMetadata::default()
        };
        let shape = FastFileContentUpdateShape {
            id: BoundExpr::Param(BoundParamRef { index: 2 }),
            data: BoundExpr::Param(BoundParamRef { index: 3 }),
            metadata: None,
            data_parameter_index: Some(3),
        };

        assert_eq!(
            fast_file_content_update_splice_provenance(&shape, &metadata),
            Some(expected)
        );
    }

    #[test]
    fn fast_file_content_update_has_no_provenance_for_full_blob_or_literal() {
        let full_blob_metadata = ExecuteStatementMetadata {
            parameter_blob_splices: vec![Some(splice("id")), None],
            ..ExecuteStatementMetadata::default()
        };
        let parameter_shape = FastFileContentUpdateShape {
            id: BoundExpr::Param(BoundParamRef { index: 1 }),
            data: BoundExpr::Param(BoundParamRef { index: 2 }),
            metadata: None,
            data_parameter_index: Some(2),
        };
        assert_eq!(
            fast_file_content_update_splice_provenance(&parameter_shape, &full_blob_metadata,),
            None
        );

        let literal_shape = FastFileContentUpdateShape {
            id: BoundExpr::Param(BoundParamRef { index: 1 }),
            data: BoundExpr::Literal(crate::sql2::bind::expr::BoundLiteral::Text(
                "literal".to_string(),
            )),
            metadata: None,
            data_parameter_index: None,
        };
        assert_eq!(
            fast_file_content_update_splice_provenance(&literal_shape, &full_blob_metadata),
            None
        );
    }

    #[test]
    fn fast_file_path_write_uses_each_rows_bound_data_parameter_metadata() {
        let first = splice("first-data");
        let second = splice("second-data");
        let metadata = ExecuteStatementMetadata {
            parameter_blob_splices: vec![
                Some(splice("first-path")),
                Some(first.clone()),
                None,
                Some(second.clone()),
            ],
            mutation_identity: Some(MutationIdentity {
                namespace_seed: [9; 16],
                operation_proof: [19; 32],
            }),
        };

        assert_eq!(
            fast_file_blob_expr_splice_provenance(
                &BoundExpr::Param(BoundParamRef { index: 2 }),
                &metadata,
            ),
            Some(first)
        );
        assert_eq!(
            fast_file_blob_expr_splice_provenance(
                &BoundExpr::Param(BoundParamRef { index: 4 }),
                &metadata,
            ),
            Some(second)
        );
        assert_eq!(
            fast_file_blob_expr_splice_provenance(
                &BoundExpr::Literal(crate::sql2::bind::expr::BoundLiteral::Text(
                    "literal".to_string(),
                )),
                &metadata,
            ),
            None
        );
    }
}

#[cfg(test)]
mod constraints_unchanged_tests {
    use std::collections::{BTreeMap, HashSet};

    use serde_json::{Value as JsonValue, json};

    use super::assigned_columns_preserve_constraints;
    use crate::catalog::{SchemaCatalogKey, SchemaPlan};
    use crate::sql2::derive_entity_surface_spec_from_schema;

    fn schema() -> JsonValue {
        json!({
            "x-lix-key": "constraint_probe",
            "x-lix-primary-key": ["/id"],
            "x-lix-unique": [["/slug"]],
            "x-lix-foreign-keys": [{
                "properties": ["/parentId"],
                "references": { "schemaKey": "constraint_probe_parent", "properties": ["/id"] }
            }],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "slug": { "type": "string" },
                "parentId": { "type": "string" },
                "payload": { "type": "string" }
            },
            "required": ["id", "slug", "parentId", "payload"],
            "additionalProperties": false
        })
    }

    /// `constraints_unchanged` is the one way a row of an indexed schema can
    /// reach commit with `requires_transaction_validation == false`, so the
    /// index's completeness rests on it never being granted to an UPDATE that
    /// touches an indexed column.
    ///
    /// It is granted through `assigned_columns_preserve_constraints`, which
    /// refuses whenever the assignment set touches the primary key, a unique
    /// group, or a foreign key's local properties — a strict superset of
    /// `indexed_columns`. This test states that superset relation directly:
    /// every indexed column, individually, revokes the certificate.
    #[test]
    fn every_indexed_column_revokes_the_constraints_unchanged_certificate() {
        let schema = schema();
        let spec = derive_entity_surface_spec_from_schema(&schema).expect("spec");
        assert_eq!(
            spec.indexed_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["parentId", "slug"],
            "the probe schema must actually declare indexed columns"
        );
        let parent = json!({
            "x-lix-key": "constraint_probe_parent",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
            "additionalProperties": false
        });
        let parent_key = SchemaCatalogKey {
            schema_key: "constraint_probe_parent".to_owned(),
        };
        let schema_index = BTreeMap::from([(parent_key.clone(), &parent)]);
        let key_index = BTreeMap::from([(parent_key, crate::catalog::SchemaPlanId::for_test(0))]);
        let plan = SchemaPlan::compile_standalone_for_test(
            SchemaCatalogKey {
                schema_key: "constraint_probe".to_owned(),
            },
            schema,
            &key_index,
            &schema_index,
        )
        .expect("schema should compile");

        for column in &spec.indexed_columns {
            let assigned = HashSet::from([column.name.as_str()]);
            assert!(
                !assigned_columns_preserve_constraints(&plan, &assigned),
                "assigning indexed column '{}' must revoke constraints_unchanged",
                column.name
            );
        }

        let untouched = HashSet::from(["payload"]);
        assert!(
            assigned_columns_preserve_constraints(&plan, &untouched),
            "an assignment touching no declared column keeps the certificate, \
             which is what makes skipping extraction for it sound"
        );
    }
}
