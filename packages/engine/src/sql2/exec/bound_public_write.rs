use bytes::Bytes;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use serde_json::Value as JsonValue;

use crate::changelog::CommitId;
use crate::common::{
    ExecuteStatementMetadata, RequestBlobSpliceProvenance, SharedStr, validate_row_metadata,
};
use crate::entity_pk::EntityPk;
use crate::live_state::{
    LiveStateFilter, LiveStateProjection, LiveStateRowFilter, LiveStateScanRequest,
    MaterializedLiveStateBatch, MaterializedLiveStateRow, MaterializedLiveStateRowRef,
};
use crate::sql2::SqlWriteExecutionContext;
use crate::sql2::bind::expr::{BoundCastType, BoundExpr, BoundLiteral};
use crate::sql2::bind::write::{
    BoundAssignment, BoundConflictAction, BoundInsertConflict, BoundInsertValues, BoundWriteInput,
    BoundWriteOp, BoundWriteTarget, EntityWriteSurface, FileWriteSurface,
};
use crate::sql2::catalog::entity_surface::EntitySurfaceColumn;
use crate::sql2::catalog::{EntityColumnType, EntitySurfaceSpec};
use crate::sql2::plan::LogicalWritePlan;
use crate::sql2::plan::branch_scope::BranchScope;
use crate::sql2::plan::predicate::{BoundPredicate, FilterSet};
use crate::sql2::read_only::reject_read_only_entity_surface;
use crate::sql2::value_contract::{json_bigint_value, json_double_value};
use crate::transaction::types::{
    RawWriteBatch, RawWriteRowRef, TransactionJson, TransactionWrite, TransactionWriteMode,
};
use crate::wasm::{WasmCanonicalJson, WasmEntityKey};
use crate::{LixError, NullableKeyFilter, Value, parse_row_metadata_value};

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
pub(crate) fn supports_bound_public_write(plan: &LogicalWritePlan) -> bool {
    match &plan.bound.target {
        BoundWriteTarget::Entity(_) => bound_public_write_shape_supported(plan),
        BoundWriteTarget::File(surface) => {
            fast_file_path_write_shape(plan, surface).is_some()
                || fast_file_data_update_shape(plan, surface).is_some()
        }
        _ => false,
    }
}

pub(crate) enum BoundPublicWriteExecution {
    Executed(SqlWriteResult),
    Unsupported,
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
    if layout
        .columns
        .iter()
        .any(|target| !matches!(target, InsertColumnTarget::Visible { .. }))
    {
        return Ok(None);
    }
    let mut write_rows = RawWriteBatch::with_capacity(parameter_batch.num_rows());
    let mut unique_identities =
        std::collections::HashSet::with_capacity(parameter_batch.num_rows());
    for row_index in 0..parameter_batch.num_rows() {
        let params = super::write::parameter_row(parameter_batch, row_index)
            .map_err(|error| with_parameter_batch_statement_index(error, row_index))?;
        let Some(mut row) = certified_entity_insert_batch(
            ctx,
            plan,
            &spec,
            &layout,
            values,
            &params,
            active_branch_commit_id.as_ref(),
        )
        .map_err(|error| with_parameter_batch_statement_index(error, row_index))?
        else {
            return Ok(None);
        };
        if row.len() != 1 {
            return Ok(None);
        }
        let candidate = row.row(0);
        let Some(entity_pk) = candidate.entity_pk else {
            return Ok(None);
        };
        if !unique_identities.insert((
            candidate.schema_key.clone(),
            entity_pk.clone(),
            candidate.file_id.cloned(),
            candidate.branch_id.clone(),
        )) {
            return Ok(None);
        }
        write_rows.append_taken_row(&mut row, 0);
    }
    let committed = scan_entity_conflict_candidates(ctx, &spec, &write_rows).await?;
    let committed_conflict = if committed.is_empty() {
        None
    } else {
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
        for (row_index, row) in write_rows.iter().enumerate() {
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
        conflict
    };
    stage_rows(ctx, TransactionWriteMode::Insert, write_rows).await?;
    if let Some(error) = committed_conflict {
        return Err(error);
    }
    #[cfg(test)]
    CERTIFIED_ENTITY_INSERT_PARAMETER_BATCH_EXECUTIONS.with(|executions| {
        executions.set(executions.get().saturating_add(1));
    });
    Ok(Some(
        (0..parameter_batch.num_rows())
            .map(|_| SqlWriteResult::affected(1))
            .collect(),
    ))
}

/// Executes a certified run of independent point updates as one physical
/// scan/stage operation.
///
/// `executeBatch` remains sequential at its public boundary. This route is
/// narrower: every logical statement must target a distinct primary key in
/// the same unconstrained entity surface, so evaluating them together is
/// observationally equivalent to evaluating them one at a time.
pub(crate) async fn try_execute_entity_update_parameter_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    parameter_batch: &RecordBatch,
) -> Result<Option<Vec<SqlWriteResult>>, LixError> {
    let BoundWriteTarget::Entity(EntityWriteSurface::Base { schema_key }) = &plan.bound.target
    else {
        return Ok(None);
    };
    if plan.bound.op != BoundWriteOp::Update
        || !matches!(plan.bound.input, BoundWriteInput::None)
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
    let mut parameter_rows = Vec::with_capacity(parameter_batch.num_rows());
    let mut entity_pks = Vec::with_capacity(parameter_batch.num_rows());
    let mut unique_entity_pks = std::collections::BTreeSet::new();
    for row_index in 0..parameter_batch.num_rows() {
        let params = super::write::parameter_row(parameter_batch, row_index)
            .map_err(|error| with_parameter_batch_statement_index(error, row_index))?;
        let entity_pk = if let Some(param_index) = direct_primary_key_param {
            let Some(Value::Text(value)) = params.get(param_index) else {
                return Ok(None);
            };
            EntityPk::single(value.clone())
        } else {
            let Some(mut row_entity_pks) =
                bound_entity_pks_from_primary_key_predicate(&spec, &plan.bound.predicate, &params)
            else {
                return Ok(None);
            };
            if row_entity_pks.len() != 1 {
                return Ok(None);
            }
            row_entity_pks.pop().expect("one point-update identity")
        };
        if !unique_entity_pks.insert(entity_pk.clone()) {
            // Repeated identities observe earlier staged writes and are not
            // independent statements.
            return Ok(None);
        }
        parameter_rows.push(params);
        entity_pks.push(entity_pk);
    }

    let candidates = scan_entity_candidates_for_pks(
        ctx,
        plan,
        &spec,
        unique_entity_pks.into_iter().collect(),
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
    let mut candidates_by_pk = std::collections::BTreeMap::<EntityPk, Vec<_>>::new();
    for candidate in candidates.iter() {
        candidates_by_pk
            .entry(candidate.entity_pk().clone())
            .or_default()
            .push(candidate);
    }

    let mut affected_by_statement = Vec::with_capacity(parameter_rows.len());
    let mut write_rows = RawWriteBatch::with_capacity(parameter_rows.len());
    for (row_index, (entity_pk, params)) in entity_pks.into_iter().zip(&parameter_rows).enumerate()
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

struct DirectPathValueReplacement {
    value_param_index: usize,
}

#[derive(Clone, Copy)]
enum EntityLiveRowRef<'a> {
    Owned(&'a MaterializedLiveStateRow),
    Batch(MaterializedLiveStateRowRef<'a>),
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

    fn branch_id(self) -> &'a str {
        match self {
            Self::Owned(row) => row.branch_id.as_ref(),
            Self::Batch(row) => row.branch_id(),
        }
    }
}

impl<'a> From<&'a MaterializedLiveStateRow> for EntityLiveRowRef<'a> {
    fn from(row: &'a MaterializedLiveStateRow) -> Self {
        Self::Owned(row)
    }
}

impl<'a> From<MaterializedLiveStateRowRef<'a>> for EntityLiveRowRef<'a> {
    fn from(row: MaterializedLiveStateRowRef<'a>) -> Self {
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
    let value = match params.get(replacement.value_param_index) {
        Some(Value::Null) => JsonValue::Null,
        Some(Value::Text(raw)) => serde_json::from_str(raw).map_err(|error| {
            LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("lix_json argument is not valid JSON: {error}"),
            )
        })?,
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
    let value = serde_json::to_string(&value).map_err(|error| {
        LixError::new(
            LixError::CODE_UNKNOWN,
            format!("certified replacement value failed to serialize: {error}"),
        )
    })?;
    let path =
        serde_json::to_string(candidate.entity_pk().as_single_string()?).map_err(|error| {
            LixError::new(
                LixError::CODE_UNKNOWN,
                format!("certified replacement identity failed to serialize: {error}"),
            )
        })?;
    let normalized = format!(r#"{{"path":{path},"value":{value}}}"#);
    let metadata = inherited_metadata(candidate, spec)?;
    rows.push_parts(
        Some(candidate.entity_pk().clone()),
        spec.schema_key.as_str().into(),
        candidate.file_id().map(Into::into),
        Some(TransactionJson::from_certified_normalized_row_content(
            normalized.into(),
        )),
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
            } else if let Some(shape) = fast_file_data_update_shape(plan, surface) {
                execute_file_data_update(ctx, params, metadata, &shape)
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

struct FastFileDataUpdateShape {
    id: BoundExpr,
    data: BoundExpr,
    metadata: Option<BoundExpr>,
    data_parameter_index: Option<usize>,
}

async fn execute_file_data_update(
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    metadata: &ExecuteStatementMetadata,
    shape: &FastFileDataUpdateShape,
) -> Result<u64, LixError> {
    let id = eval_fast_file_nullable_text(&shape.id, params, "id")?;
    let data = eval_fast_file_blob(&shape.data, params, "data")?;
    let splice_provenance = fast_file_data_update_splice_provenance(shape, metadata);
    if let Some(metadata_expr) = &shape.metadata {
        let row_metadata = eval_fast_file_metadata(metadata_expr, params)?;
        crate::sql2::providers::execute_fast_lix_file_data_update_by_id_with_metadata(
            ctx,
            id,
            data,
            row_metadata,
            splice_provenance,
            metadata.mutation_identity(),
        )
        .await
    } else {
        crate::sql2::providers::execute_fast_lix_file_data_update_by_id(
            ctx,
            id,
            data,
            splice_provenance,
            metadata.mutation_identity(),
        )
        .await
    }
}

fn fast_file_data_update_splice_provenance(
    shape: &FastFileDataUpdateShape,
    metadata: &ExecuteStatementMetadata,
) -> Option<RequestBlobSpliceProvenance> {
    shape
        .data_parameter_index
        .and_then(|index| metadata.blob_splice_for_parameter(index))
        .cloned()
}

fn fast_file_data_update_shape(
    plan: &LogicalWritePlan,
    surface: &FileWriteSurface,
) -> Option<FastFileDataUpdateShape> {
    if !matches!(surface, FileWriteSurface::Base)
        || plan.bound.op != BoundWriteOp::Update
        || !matches!(plan.bound.input, BoundWriteInput::None)
        || plan.bound.conflict.is_some()
        || !matches!(plan.bound.branch_scope, BranchScope::Active { .. })
        || !(1..=2).contains(&plan.bound.assignments.len())
    {
        return None;
    }
    let assignment = plan
        .bound
        .assignments
        .iter()
        .find(|assignment| assignment.column.name == "data")?;
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
            assignment.column.name != "data" && assignment.column.name != "lixcol_metadata"
        })
    {
        return None;
    }
    let id = fast_file_id_predicate_value(&plan.bound.predicate)?;
    Some(FastFileDataUpdateShape {
        id: id.clone(),
        data: assignment.value.clone(),
        metadata,
        data_parameter_index: match &assignment.value {
            BoundExpr::Param(param) => Some(param.index),
            BoundExpr::Literal(_) => None,
            _ => unreachable!("fast file data update accepts only params and blob literals"),
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
    matches!(
        expr,
        BoundExpr::Param(_) | BoundExpr::Literal(BoundLiteral::Blob(_))
    )
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

    let spec = entity_spec(ctx, schema_key)?;
    validate_bound_write_supported(plan, &spec)?;
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
                entity_insert_batch(ctx, plan, &spec, params, active_branch_commit_id.as_ref())?;
                return Ok(SqlWriteResult::affected(0));
            }
            if plan.bound.conflict.is_some() {
                entity_upsert(ctx, plan, &spec, params, active_branch_commit_id.as_ref())
                    .await
                    .map(SqlWriteResult::affected)
            } else {
                entity_insert(ctx, plan, &spec, params, active_branch_commit_id.as_ref())
                    .await
                    .map(SqlWriteResult::affected)
            }
        }
        BoundWriteOp::Update => {
            if no_op {
                return Ok(SqlWriteResult::affected(0));
            }
            entity_update(ctx, plan, &spec, params, active_branch_commit_id.as_ref())
                .await
                .map(SqlWriteResult::affected)
        }
        BoundWriteOp::Delete => {
            if no_op {
                return Ok(empty_entity_delete_returning_result(plan));
            }
            if matches!(surface, EntityWriteSurface::Base { .. })
                && matches!(plan.bound.predicate, BoundPredicate::True)
                && plan.bound.returning.is_none()
                && let Some(result) = entity_delete_collection(ctx, &spec).await?
            {
                return Ok(result);
            }
            entity_delete(ctx, plan, &spec, params, active_branch_commit_id.as_ref()).await
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
    let Some(previous) = ctx
        .load_collection_generation(&active_branch_id, scope)
        .await?
    else {
        return Ok(None);
    };
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
            eval_fast_file_blob(data_expr, params, "data")?,
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
    if !matches!(surface, FileWriteSurface::Base) || plan.bound.op != BoundWriteOp::Insert {
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
    let data_index = values.column_index("data")?;
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
    if conflict == crate::sql2::providers::FastLixFilePathWriteConflict::UpdateDataAndMetadata
        && metadata_index.is_none()
    {
        return None;
    }
    if conflict == crate::sql2::providers::FastLixFilePathWriteConflict::UpdateData
        && metadata_index.is_some()
    {
        return None;
    }
    // DataFusion ignores insert values for rows skipped by DO NOTHING. Keep
    // metadata-bearing variants there so invalid metadata on an existing row
    // retains that behavior without complicating the hot upsert path.
    if conflict == crate::sql2::providers::FastLixFilePathWriteConflict::DoNothing
        && metadata_index.is_some()
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
    if conflict.target_columns.len() != 1 || conflict.target_columns[0].name != "path" {
        return None;
    }
    match &conflict.action {
        BoundConflictAction::DoNothing => {
            Some(crate::sql2::providers::FastLixFilePathWriteConflict::DoNothing)
        }
        BoundConflictAction::DoUpdate { assignments } => {
            let assigns_excluded_column = |assignment: &BoundAssignment, name: &str| {
                assignment.column.name == name
                    && matches!(
                        &assignment.value,
                        BoundExpr::ExcludedColumn(column) if column.name == name
                    )
            };
            if assignments.len() == 1 && assigns_excluded_column(&assignments[0], "data") {
                return Some(crate::sql2::providers::FastLixFilePathWriteConflict::UpdateData);
            }
            if assignments.len() == 2
                && assignments
                    .iter()
                    .any(|assignment| assigns_excluded_column(assignment, "data"))
                && assignments
                    .iter()
                    .any(|assignment| assigns_excluded_column(assignment, "lixcol_metadata"))
            {
                return Some(
                    crate::sql2::providers::FastLixFilePathWriteConflict::UpdateDataAndMetadata,
                );
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
        BoundExpr::Literal(BoundLiteral::Blob(value)) => Ok(value.clone().into()),
        BoundExpr::Param(param) => match params.get(param.index.saturating_sub(1)) {
            Some(Value::Blob(value)) => Ok(value.clone()),
            Some(_) => Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("lix_file fast write column '{column}' expects blob data"),
            )),
            None => Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("missing SQL parameter ${}", param.index),
            )),
        },
        _ => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!("lix_file fast write column '{column}' supports params and blob literals only"),
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
                validate_row_metadata(value, "lix_file")?;
                value.clone()
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
) -> Result<u64, LixError> {
    let write_rows = entity_insert_batch(ctx, plan, spec, params, active_branch_commit_id)?;
    stage_rows(ctx, TransactionWriteMode::Insert, write_rows).await
}

async fn entity_upsert(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<u64, LixError> {
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

    stage_rows(ctx, TransactionWriteMode::Replace, write_rows).await
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
) -> Result<u64, LixError> {
    let candidates = scan_entity_candidates(ctx, plan, spec, params).await?;
    let mut write_rows = RawWriteBatch::with_capacity(candidates.len());
    for candidate in candidates.iter() {
        append_entity_update_row(
            &mut write_rows,
            ctx,
            plan,
            spec,
            candidate,
            params,
            active_branch_commit_id,
        )?;
    }
    stage_rows(ctx, TransactionWriteMode::Replace, write_rows).await
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

fn empty_entity_delete_returning_result(plan: &LogicalWritePlan) -> SqlWriteResult {
    plan.bound.returning.as_ref().map_or_else(
        || SqlWriteResult::affected(0),
        |returning| {
            SqlWriteResult::returning(
                0,
                crate::SqlQueryResult {
                    columns: returning
                        .items
                        .iter()
                        .map(|item| item.output_name.clone())
                        .collect(),
                    rows: Vec::new(),
                    notices: Vec::new(),
                },
            )
        },
    )
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
        BoundExpr::Literal(BoundLiteral::Blob(value)) => {
            return Ok(Value::Blob(value.clone().into()));
        }
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
            EntityEvalValue::Json(value) => Value::Json(value),
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
            Value::Json(value)
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
    candidates: &'a MaterializedLiveStateBatch,
) -> Option<MaterializedLiveStateRowRef<'a>> {
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
) -> Result<MaterializedLiveStateBatch, LixError> {
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
    ctx.scan_live_state_batch(&LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![spec.schema_key.clone()],
            entity_pks: entity_pks.into_iter().collect(),
            branch_ids: branch_ids.into_iter().map(Into::into).collect(),
            file_ids,
            include_tombstones: false,
            ..LiveStateFilter::default()
        },
        ..LiveStateScanRequest::default()
    })
    .await
}

async fn scan_entity_candidates(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
) -> Result<MaterializedLiveStateBatch, LixError> {
    let branch_ids = scan_branch_ids(&plan.bound.branch_scope)?;
    let mut request = LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![spec.schema_key.clone()],
            branch_ids,
            include_tombstones: false,
            ..LiveStateFilter::default()
        },
        ..LiveStateScanRequest::default()
    };
    if let Some(entity_pks) =
        bound_entity_pks_from_primary_key_predicate(spec, &plan.bound.predicate, params)
    {
        if entity_pks.is_empty() {
            request.filter.rows = LiveStateRowFilter::None;
        }
        request.filter.entity_pks = entity_pks;
    }
    ctx.scan_live_state_batch(&request).await
}

async fn scan_entity_candidates_for_pks(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    entity_pks: Vec<EntityPk>,
    metadata_only: bool,
) -> Result<MaterializedLiveStateBatch, LixError> {
    ctx.scan_live_state_batch(&LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![spec.schema_key.clone()],
            entity_pks,
            branch_ids: scan_branch_ids(&plan.bound.branch_scope)?,
            include_tombstones: false,
            ..LiveStateFilter::default()
        },
        projection: if metadata_only {
            LiveStateProjection {
                columns: vec!["metadata".to_string()],
            }
        } else {
            LiveStateProjection::default()
        },
        ..LiveStateScanRequest::default()
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
                .filter(|column| column.column_type == EntityColumnType::String)
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
                let values = values
                    .iter()
                    .map(|value| bound_primary_key_string(value, self.params))
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
        let value = bound_primary_key_string(value_expr, self.params)?;
        Some(BoundPrimaryKeyConstraint::Parts(
            std::collections::BTreeMap::from([(
                column.name.clone(),
                std::collections::BTreeSet::from([value]),
            )]),
        ))
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

fn bound_primary_key_string(expr: &BoundExpr, params: &[Value]) -> Option<String> {
    match expr {
        BoundExpr::Literal(BoundLiteral::Text(value)) => Some(value.clone()),
        BoundExpr::Param(param) => match params.get(param.index.saturating_sub(1)) {
            Some(Value::Text(value)) => Some(value.clone()),
            _ => None,
        },
        _ => None,
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

fn certified_entity_insert_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    layout: &InsertRowLayout,
    values: &BoundInsertValues,
    params: &[Value],
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
    if !schema_plan.accepts_v2_canonical_certificate() || !spec.defaults.is_empty() {
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
    let estimated_batch_bytes = values
        .rows
        .len()
        .checked_mul(estimated_row_bytes)
        .ok_or_else(|| LixError::unknown("certified INSERT batch size overflowed"))?;
    let mut normalized = Vec::with_capacity(estimated_batch_bytes);
    let mut offsets = Vec::with_capacity(values.rows.len());
    let mut entity_pks = Vec::with_capacity(values.rows.len());
    let mut row_parts = Vec::with_capacity(values.rows.len());
    let mut row_values = (0..layout.columns.len())
        .map(|_| None)
        .collect::<Vec<Option<JsonValue>>>();
    let context = EntityEvalContext::insert(&JsonValue::Null, &layout.visible_columns);

    for row in &values.rows {
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
                InsertColumnTarget::Visible { .. } => unreachable!("visible columns handled above"),
                InsertColumnTarget::EntityPk => {
                    explicit_entity_pk = Some(entity_pk_from_value(&value, "lixcol_entity_pk")?);
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
            .certify_or_normalize_v2_plugin_row(&normalized[start..], &key)?
            .ok_or_else(|| {
                LixError::unknown("eligible certified INSERT row declined its schema certificate")
            })?;
        if let Some(canonical) = certified.normalized {
            normalized.truncate(start);
            normalized.extend_from_slice(&canonical);
        }
        let end = normalized.len();
        offsets.push((start, end));
        entity_pks.push(certified.entity_pk);
        let global = global.unwrap_or(false);
        row_parts.push(CertifiedInsertRow {
            file_id: file_id.map(Into::into),
            metadata,
            global,
            untracked: untracked.unwrap_or(false),
            branch_id: entity_row_branch_id(plan, explicit_branch_id, global)?.into(),
        });
        for value in &mut row_values {
            *value = None;
        }
    }

    let normalized = Bytes::from(normalized);
    let normalized_rows = offsets
        .into_iter()
        .map(|(start, end)| {
            SharedStr::from_utf8(normalized.slice(start..end))
                .map_err(|_| LixError::unknown("certified INSERT row is not UTF-8"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let row_count = normalized_rows.len();
    let snapshots = WasmCanonicalJson::from_certified_batch_parts(
        normalized_rows,
        entity_pks.clone(),
        vec![schema_plan.shared_fingerprint()],
        vec![0; row_count],
        row_count,
    )?;
    let mut rows = RawWriteBatch::with_capacity(row_count);
    for ((entity_pk, snapshot), row) in entity_pks.into_iter().zip(snapshots).zip(row_parts) {
        rows.push_parts(
            Some(entity_pk),
            layout.schema_key.as_str().into(),
            row.file_id,
            Some(TransactionJson::from_canonical_batch(snapshot)),
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
        let derived_entity_pk =
            EntityPk::from_primary_key_paths(&snapshot, &spec.primary_key_paths).map_err(
                |error| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!(
                            "INSERT failed to derive entity primary key for schema '{}': {error}",
                            layout.schema_key
                        ),
                    )
                },
            )?;
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
    row: Option<EntityLiveRowRef<'a>>,
    excluded_snapshot: Option<&'a JsonValue>,
    excluded_row: Option<RawWriteRowRef<'a>>,
    visible_columns: &'a [EntitySurfaceColumn],
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
            row: Some(row.into()),
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
            row: Some(row.into()),
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
            "Use BYTEA for lix_file.data; registered entity schemas expose no binary column type.",
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
    if plan.bound.returning.is_some() && plan.bound.op != BoundWriteOp::Delete {
        let action = match plan.bound.op {
            BoundWriteOp::Insert => "INSERT",
            BoundWriteOp::Update => "UPDATE",
            BoundWriteOp::Delete => unreachable!("DELETE RETURNING is supported"),
        };
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!("{action} RETURNING is not supported for registered entity surfaces"),
        )
        .with_hint(format!(
            "Run the {action} without RETURNING, then SELECT the row explicitly."
        )));
    }
    Ok(())
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
                || matches!(
                    column.name.as_str(),
                    "lixcol_entity_pk" | "lixcol_metadata" | "lixcol_snapshot_content"
                )
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
        BoundExpr::Literal(BoundLiteral::Blob(_)) => true,
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
        BoundLiteral::Blob(value) => {
            JsonValue::Array(value.iter().copied().map(JsonValue::from).collect())
        }
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
        Value::Json(value) => value.clone(),
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
            .as_json_array_value()
            .map(EntityEvalValue::Json),
        "lixcol_schema_key" => Ok(EntityEvalValue::Json(JsonValue::String(
            row.schema_key().to_string(),
        ))),
        "lixcol_file_id" => Ok(row
            .file_id()
            .map(|value| EntityEvalValue::Json(JsonValue::String(value.to_string())))
            .unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_metadata" => row
            .metadata()
            .map(|metadata| parse_row_metadata_value(metadata, row.schema_key()))
            .transpose()
            .map(|metadata| {
                metadata
                    .map(EntityEvalValue::Json)
                    .unwrap_or(EntityEvalValue::SqlNull)
            }),
        "lixcol_change_id" => Ok(row
            .change_id()
            .map(|value| EntityEvalValue::Json(JsonValue::String(value.to_string())))
            .unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_created_at" => Ok(EntityEvalValue::Json(JsonValue::String(
            row.created_at().to_string(),
        ))),
        "lixcol_updated_at" => Ok(EntityEvalValue::Json(JsonValue::String(
            row.updated_at().to_string(),
        ))),
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
        BoundExpr, FastFileDataUpdateShape, fast_file_blob_expr_splice_provenance,
        fast_file_data_update_splice_provenance,
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
    fn fast_file_data_update_uses_the_bound_data_parameter_metadata() {
        let expected = splice("data");
        let metadata = ExecuteStatementMetadata {
            parameter_blob_splices: vec![Some(splice("unrelated")), None, Some(expected.clone())],
            ..ExecuteStatementMetadata::default()
        };
        let shape = FastFileDataUpdateShape {
            id: BoundExpr::Param(BoundParamRef { index: 2 }),
            data: BoundExpr::Param(BoundParamRef { index: 3 }),
            metadata: None,
            data_parameter_index: Some(3),
        };

        assert_eq!(
            fast_file_data_update_splice_provenance(&shape, &metadata),
            Some(expected)
        );
    }

    #[test]
    fn fast_file_data_update_has_no_provenance_for_full_blob_or_literal() {
        let full_blob_metadata = ExecuteStatementMetadata {
            parameter_blob_splices: vec![Some(splice("id")), None],
            ..ExecuteStatementMetadata::default()
        };
        let parameter_shape = FastFileDataUpdateShape {
            id: BoundExpr::Param(BoundParamRef { index: 1 }),
            data: BoundExpr::Param(BoundParamRef { index: 2 }),
            metadata: None,
            data_parameter_index: Some(2),
        };
        assert_eq!(
            fast_file_data_update_splice_provenance(&parameter_shape, &full_blob_metadata,),
            None
        );

        let literal_shape = FastFileDataUpdateShape {
            id: BoundExpr::Param(BoundParamRef { index: 1 }),
            data: BoundExpr::Literal(crate::sql2::bind::expr::BoundLiteral::Blob(vec![1])),
            metadata: None,
            data_parameter_index: None,
        };
        assert_eq!(
            fast_file_data_update_splice_provenance(&literal_shape, &full_blob_metadata),
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
                &BoundExpr::Literal(crate::sql2::bind::expr::BoundLiteral::Blob(vec![1])),
                &metadata,
            ),
            None
        );
    }
}
