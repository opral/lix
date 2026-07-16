use serde_json::Value as JsonValue;

use crate::changelog::CommitId;
use crate::common::validate_row_metadata;
use crate::entity_pk::EntityPk;
use crate::live_state::{LiveStateFilter, LiveStateScanRequest};
use crate::sql2::SqlWriteExecutionContext;
use crate::sql2::bind::expr::{BoundExpr, BoundLiteral};
use crate::sql2::bind::write::{
    BoundAssignment, BoundConflictAction, BoundInsertConflict, BoundInsertValues, BoundWriteInput,
    BoundWriteOp, BoundWriteTarget, EntityWriteSurface, FileWriteSurface,
};
use crate::sql2::catalog::entity_surface::EntitySurfaceColumn;
use crate::sql2::catalog::{
    EntityColumnType, EntitySurfaceSpec, derive_entity_surface_spec_from_schema,
};
use crate::sql2::plan::LogicalWritePlan;
use crate::sql2::plan::branch_scope::BranchScope;
use crate::sql2::plan::predicate::{BoundPredicate, FilterSet};
use crate::sql2::read_only::reject_read_only_entity_surface;
use crate::transaction::types::{
    TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteRow,
};
use crate::{LixError, NullableKeyFilter, Value, parse_row_metadata_value};

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
    Executed(u64),
    Unsupported,
}

pub(crate) async fn try_execute_bound_public_write(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<BoundPublicWriteExecution, LixError> {
    match &plan.bound.target {
        BoundWriteTarget::Entity(surface) if bound_public_write_shape_supported(plan) => {
            execute_entity_write(ctx, plan, surface, params)
                .await
                .map(BoundPublicWriteExecution::Executed)
        }
        BoundWriteTarget::File(surface) => {
            if let Some(shape) = fast_file_path_write_shape(plan, surface) {
                execute_file_path_write(ctx, plan, params, shape)
                    .await
                    .map(BoundPublicWriteExecution::Executed)
            } else if let Some(shape) = fast_file_data_update_shape(plan, surface) {
                execute_file_data_update(ctx, params, &shape)
                    .await
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
}

async fn execute_file_data_update(
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    shape: &FastFileDataUpdateShape,
) -> Result<u64, LixError> {
    let id = eval_fast_file_nullable_text(&shape.id, params, "id")?;
    let data = eval_fast_file_blob(&shape.data, params, "data")?;
    crate::sql2::providers::execute_fast_lix_file_data_update_by_id(ctx, id, data).await
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
        || plan.bound.assignments.len() != 1
    {
        return None;
    }
    let assignment = &plan.bound.assignments[0];
    if assignment.column.name != "data" || !fast_file_blob_expr_supported(&assignment.value) {
        return None;
    }
    let id = fast_file_id_predicate_value(&plan.bound.predicate)?;
    Some(FastFileDataUpdateShape {
        id: id.clone(),
        data: assignment.value.clone(),
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
) -> Result<u64, LixError> {
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
    let active_branch_commit_id = load_active_branch_commit_id(ctx).await?;
    let no_op = matches!(plan.bound.branch_scope, BranchScope::Empty)
        || matches!(plan.filters.rows, FilterSet::None);
    match plan.bound.op {
        BoundWriteOp::Insert => {
            if no_op {
                entity_insert_rows(ctx, plan, &spec, params, active_branch_commit_id.as_ref())?;
                return Ok(0);
            }
            if plan.bound.conflict.is_some() {
                entity_upsert(ctx, plan, &spec, params, active_branch_commit_id.as_ref()).await
            } else {
                entity_insert(ctx, plan, &spec, params, active_branch_commit_id.as_ref()).await
            }
        }
        BoundWriteOp::Update => {
            if no_op {
                return Ok(0);
            }
            entity_update(ctx, plan, &spec, params, active_branch_commit_id.as_ref()).await
        }
        BoundWriteOp::Delete => {
            if no_op {
                return Ok(0);
            }
            entity_delete(ctx, plan, &spec, params, active_branch_commit_id.as_ref()).await
        }
    }
}

async fn load_active_branch_commit_id(
    ctx: &mut dyn SqlWriteExecutionContext,
) -> Result<Option<CommitId>, LixError> {
    let active_branch_id = ctx.active_branch_id().to_string();
    ctx.load_branch_head(&active_branch_id)
        .await?
        .map(Some)
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
    path_index: usize,
    data_index: usize,
    conflict: crate::sql2::providers::FastLixFilePathWriteConflict,
}

async fn execute_file_path_write(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    params: &[Value],
    shape: FastFilePathWriteShape,
) -> Result<u64, LixError> {
    let BoundWriteInput::Values(values) = &plan.bound.input else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "bound lix_file fast write supports VALUES only",
        ));
    };
    let mut writes = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        writes.push((
            eval_fast_file_text(&row[shape.path_index], params, "path")?,
            eval_fast_file_blob(&row[shape.data_index], params, "data")?,
        ));
    }
    crate::sql2::providers::execute_fast_lix_file_path_writes(ctx, writes, shape.conflict).await
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
    if values.rows.is_empty() || values.columns.len() != 2 {
        return None;
    }
    let path_index = values.column_index("path")?;
    let data_index = values.column_index("data")?;
    if values.rows.iter().any(|row| {
        row.len() != values.columns.len()
            || !fast_file_value_expr_supported(&row[path_index])
            || !fast_file_value_expr_supported(&row[data_index])
    }) {
        return None;
    }
    let conflict = match &plan.bound.conflict {
        None => crate::sql2::providers::FastLixFilePathWriteConflict::None,
        Some(conflict) => fast_file_path_conflict_shape(conflict)?,
    };
    Some(FastFilePathWriteShape {
        path_index,
        data_index,
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
            if assignments.len() != 1 {
                return None;
            }
            let assignment = &assignments[0];
            if assignment.column.name != "data" {
                return None;
            }
            let BoundExpr::ExcludedColumn(column) = &assignment.value else {
                return None;
            };
            if column.name != "data" {
                return None;
            }
            Some(crate::sql2::providers::FastLixFilePathWriteConflict::UpdateData)
        }
    }
}

fn fast_file_value_expr_supported(expr: &BoundExpr) -> bool {
    matches!(
        expr,
        BoundExpr::Param(_) | BoundExpr::Literal(BoundLiteral::Text(_) | BoundLiteral::Blob(_))
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
) -> Result<Vec<u8>, LixError> {
    match expr {
        BoundExpr::Literal(BoundLiteral::Blob(value)) => Ok(value.clone()),
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

async fn entity_insert(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<u64, LixError> {
    let write_rows = entity_insert_rows(ctx, plan, spec, params, active_branch_commit_id)?;
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

    let insert_rows = entity_insert_rows(ctx, plan, spec, params, active_branch_commit_id)?;
    let candidates = scan_entity_conflict_candidates(ctx, spec, insert_rows.as_slice()).await?;
    let mut write_rows = Vec::with_capacity(insert_rows.len());

    for insert_row in insert_rows {
        let inserted_entity_pk = insert_row_entity_pk(&insert_row, spec)?;
        let matching_candidate =
            find_conflict_candidate(&insert_row, &inserted_entity_pk, candidates.as_slice());
        match (matching_candidate, &conflict.action) {
            // DO NOTHING on a conflicting row: leave the existing row untouched.
            (Some(_), BoundConflictAction::DoNothing) => {}
            (Some(candidate), BoundConflictAction::DoUpdate { assignments }) => {
                write_rows.push(entity_conflict_update_row(
                    ctx,
                    spec,
                    candidate,
                    &insert_row,
                    assignments.as_slice(),
                    params,
                    active_branch_commit_id,
                )?);
            }
            (None, _) => write_rows.push(insert_row),
        }
    }

    stage_rows(ctx, TransactionWriteMode::Replace, write_rows).await
}

fn entity_insert_rows(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<Vec<TransactionWriteRow>, LixError> {
    let BoundWriteInput::Values(values) = &plan.bound.input else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "bound entity INSERT supports VALUES only",
        ));
    };

    let layout = InsertRowLayout::from_values(spec, values)?;
    let mut write_rows = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        write_rows.push(entity_insert_row(
            ctx,
            plan,
            &layout,
            row,
            params,
            active_branch_commit_id,
        )?);
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
    let candidates = scan_entity_candidates(ctx, plan, spec).await?;
    let mut write_rows = Vec::new();
    for candidate in candidates {
        let Some(snapshot) = candidate_snapshot(&candidate)? else {
            continue;
        };
        let original_context = EntityEvalContext::live(&snapshot, &candidate, spec);
        if !predicate_matches(
            &plan.bound.predicate,
            &original_context,
            spec,
            ctx,
            params,
            active_branch_commit_id,
        )? {
            continue;
        }
        reject_projected_global_write(plan, &candidate, "UPDATE")?;
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
                    entity_json_value(value, column.column_type)?,
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
        write_rows.push(entity_replace_row_from_live(
            ctx,
            spec,
            &candidate,
            Some(updated),
            plan.bound.assignments.as_slice(),
            params,
            active_branch_commit_id,
        )?);
    }
    stage_rows(ctx, TransactionWriteMode::Replace, write_rows).await
}

async fn entity_delete(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<u64, LixError> {
    let candidates = scan_entity_candidates(ctx, plan, spec).await?;
    let mut write_rows = Vec::new();
    for candidate in candidates {
        let Some(snapshot) = candidate_snapshot(&candidate)? else {
            continue;
        };
        let context = EntityEvalContext::live(&snapshot, &candidate, spec);
        if predicate_matches(
            &plan.bound.predicate,
            &context,
            spec,
            ctx,
            params,
            active_branch_commit_id,
        )? {
            reject_projected_global_write(plan, &candidate, "DELETE")?;
            write_rows.push(entity_replace_row_from_live(
                ctx,
                spec,
                &candidate,
                None,
                plan.bound.assignments.as_slice(),
                params,
                active_branch_commit_id,
            )?);
        }
    }
    stage_rows(ctx, TransactionWriteMode::Replace, write_rows).await
}

fn entity_conflict_update_row(
    ctx: &mut dyn SqlWriteExecutionContext,
    spec: &EntitySurfaceSpec,
    candidate: &crate::live_state::MaterializedLiveStateRow,
    insert_row: &TransactionWriteRow,
    assignments: &[BoundAssignment],
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<TransactionWriteRow, LixError> {
    let snapshot = candidate_snapshot(candidate)?.ok_or_else(|| {
        LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "INSERT ON CONFLICT cannot update a tombstone row",
        )
    })?;
    let insert_snapshot = insert_row
        .snapshot
        .as_ref()
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
                entity_json_value(value, column.column_type)?,
            ));
        } else if assignment.column.name == "lixcol_metadata" {
            // handled by entity_replace_row_from_live from the assignment list
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

    entity_replace_row_from_live(
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
    rows: Vec<TransactionWriteRow>,
) -> Result<u64, LixError> {
    if rows.is_empty() {
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
    row: &TransactionWriteRow,
    spec: &EntitySurfaceSpec,
) -> Result<EntityPk, LixError> {
    if let Some(entity_pk) = &row.entity_pk {
        return Ok(entity_pk.clone());
    }
    let snapshot = row.snapshot.as_ref().ok_or_else(|| {
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
    insert_row: &TransactionWriteRow,
    inserted_entity_pk: &EntityPk,
    candidates: &'a [crate::live_state::MaterializedLiveStateRow],
) -> Option<&'a crate::live_state::MaterializedLiveStateRow> {
    candidates.iter().find(|candidate| {
        candidate_matches_insert_identity(candidate, insert_row, inserted_entity_pk)
    })
}

fn candidate_matches_insert_identity(
    candidate: &crate::live_state::MaterializedLiveStateRow,
    insert_row: &TransactionWriteRow,
    inserted_entity_pk: &EntityPk,
) -> bool {
    candidate.entity_pk == *inserted_entity_pk
        && candidate.file_id == insert_row.file_id
        && candidate.branch_id == insert_row.branch_id
        && candidate.global == insert_row.global
        && candidate.untracked == insert_row.untracked
}

async fn scan_entity_conflict_candidates(
    ctx: &mut dyn SqlWriteExecutionContext,
    spec: &EntitySurfaceSpec,
    insert_rows: &[TransactionWriteRow],
) -> Result<Vec<crate::live_state::MaterializedLiveStateRow>, LixError> {
    let mut branch_ids = std::collections::BTreeSet::new();
    let mut entity_pks = std::collections::BTreeSet::new();
    let mut file_ids = std::collections::BTreeSet::new();
    let mut untracked_values = std::collections::BTreeSet::new();
    for row in insert_rows {
        branch_ids.insert(row.branch_id.clone());
        entity_pks.insert(insert_row_entity_pk(row, spec)?);
        file_ids.insert(row.file_id.clone());
        untracked_values.insert(row.untracked);
    }
    let file_ids = file_ids
        .into_iter()
        .map(|file_id| file_id.map_or(NullableKeyFilter::Null, NullableKeyFilter::Value))
        .collect::<Vec<_>>();

    let mut candidates = Vec::new();
    for untracked in untracked_values {
        let rows = ctx
            .scan_live_state(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![spec.schema_key.clone()],
                    entity_pks: entity_pks.iter().cloned().collect(),
                    branch_ids: branch_ids.iter().cloned().collect(),
                    file_ids: file_ids.clone(),
                    untracked: Some(untracked),
                    include_tombstones: false,
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
            })
            .await?;
        candidates.extend(rows);
    }
    Ok(candidates)
}

async fn scan_entity_candidates(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
) -> Result<Vec<crate::live_state::MaterializedLiveStateRow>, LixError> {
    let branch_ids = scan_branch_ids(&plan.bound.branch_scope)?;
    let request = LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![spec.schema_key.clone()],
            branch_ids,
            include_tombstones: false,
            ..LiveStateFilter::default()
        },
        ..LiveStateScanRequest::default()
    };
    ctx.scan_live_state(&request).await
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

fn entity_insert_row(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    layout: &InsertRowLayout,
    row: &[BoundExpr],
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<TransactionWriteRow, LixError> {
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
        if matches!(target, InsertColumnTarget::Metadata) {
            metadata = optional_metadata_from_eval_value(
                eval_value,
                "lixcol_metadata",
                &layout.schema_key,
            )?;
            continue;
        }
        if let InsertColumnTarget::Visible { name, column_type } = target {
            snapshot.insert(name.clone(), entity_json_value(eval_value, *column_type)?);
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

    let snapshot = JsonValue::Object(snapshot);
    let global = global.unwrap_or(false);
    let branch_id = entity_row_branch_id(plan, explicit_branch_id, global)?;
    Ok(TransactionWriteRow {
        entity_pk,
        schema_key: layout.schema_key.clone(),
        file_id,
        snapshot: Some(TransactionJson::from_value(
            snapshot,
            &layout.snapshot_context,
        )?),
        metadata,
        origin: None,
        created_at: None,
        updated_at: None,
        global,
        change_id: None,
        commit_id: None,
        untracked: untracked.unwrap_or(false),
        branch_id,
    })
}

fn reject_projected_global_write(
    plan: &LogicalWritePlan,
    row: &crate::live_state::MaterializedLiveStateRow,
    action: &str,
) -> Result<(), LixError> {
    let target_is_by_branch = matches!(
        &plan.bound.target,
        BoundWriteTarget::Entity(EntityWriteSurface::ByBranch { .. })
    );
    if target_is_by_branch && row.global && row.branch_id != crate::GLOBAL_BRANCH_ID {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!(
                "{action} through an entity by-branch surface cannot mutate a projected global row"
            ),
        ));
    }
    Ok(())
}

fn entity_replace_row_from_live(
    ctx: &mut dyn SqlWriteExecutionContext,
    spec: &EntitySurfaceSpec,
    row: &crate::live_state::MaterializedLiveStateRow,
    snapshot: Option<JsonValue>,
    assignments: &[BoundAssignment],
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<TransactionWriteRow, LixError> {
    let metadata = if let Some(expr) = assignment_value(assignments, "lixcol_metadata") {
        let snapshot_for_eval = candidate_snapshot(row)?.unwrap_or(JsonValue::Null);
        let context = EntityEvalContext::live(&snapshot_for_eval, row, spec);
        let value = eval_expr_value(expr, &context, ctx, params, active_branch_commit_id)?;
        optional_metadata_from_eval_value(value, "lixcol_metadata", &spec.schema_key)?
    } else {
        inherited_metadata(row, spec)?
    };

    Ok(TransactionWriteRow {
        entity_pk: Some(row.entity_pk.clone()),
        schema_key: spec.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot: snapshot
            .map(|snapshot| {
                TransactionJson::from_value(
                    snapshot,
                    &format!("{} update snapshot_content", spec.schema_key),
                )
            })
            .transpose()?,
        metadata,
        origin: None,
        created_at: None,
        updated_at: None,
        global: row.global,
        change_id: None,
        commit_id: None,
        untracked: row.untracked,
        branch_id: if row.global {
            crate::GLOBAL_BRANCH_ID.to_string()
        } else {
            row.branch_id.clone()
        },
    })
}

fn inherited_metadata(
    row: &crate::live_state::MaterializedLiveStateRow,
    spec: &EntitySurfaceSpec,
) -> Result<Option<TransactionJson>, LixError> {
    row.metadata
        .as_ref()
        .map(|metadata| {
            let metadata = parse_row_metadata_value(metadata, &spec.schema_key)?;
            TransactionJson::from_value(metadata, &format!("{} metadata", spec.schema_key))
        })
        .transpose()
}

struct EntityEvalContext<'a> {
    snapshot: &'a JsonValue,
    row: Option<&'a crate::live_state::MaterializedLiveStateRow>,
    excluded_snapshot: Option<&'a JsonValue>,
    excluded_row: Option<&'a TransactionWriteRow>,
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
        row: &'a crate::live_state::MaterializedLiveStateRow,
        spec: &'a EntitySurfaceSpec,
    ) -> Self {
        Self {
            snapshot,
            row: Some(row),
            excluded_snapshot: None,
            excluded_row: None,
            visible_columns: &spec.columns,
        }
    }

    fn conflict(
        snapshot: &'a JsonValue,
        row: &'a crate::live_state::MaterializedLiveStateRow,
        excluded_snapshot: &'a JsonValue,
        excluded_row: &'a TransactionWriteRow,
        spec: &'a EntitySurfaceSpec,
    ) -> Self {
        Self {
            snapshot,
            row: Some(row),
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
    ctx.list_visible_schemas()?
        .into_iter()
        .filter_map(|schema| derive_entity_surface_spec_from_schema(&schema).ok())
        .find(|spec| spec.schema_key == schema_key)
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
        BoundExpr::Cast { .. } => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "bound entity writes do not support CAST expressions yet",
        )),
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
            let (left, right) = eval_comparison_operands(
                left,
                right,
                context,
                spec,
                ctx,
                params,
                active_branch_commit_id,
            )?;
            Ok(!left.is_null() && !right.is_null() && left == right)
        }
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
                let (candidate, value) = normalize_comparison_operands(
                    expr,
                    candidate.clone(),
                    value_expr,
                    value,
                    spec,
                )?;
                if !value.is_null() && candidate == value {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn eval_comparison_operands(
    left: &BoundExpr,
    right: &BoundExpr,
    context: &EntityEvalContext<'_>,
    spec: &EntitySurfaceSpec,
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    active_branch_commit_id: Option<&CommitId>,
) -> Result<(JsonValue, JsonValue), LixError> {
    let left_value = eval_expr(left, context, ctx, params, active_branch_commit_id)?;
    let right_value = eval_expr(right, context, ctx, params, active_branch_commit_id)?;
    normalize_comparison_operands(left, left_value, right, right_value, spec)
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
        BoundExpr::Cast { .. } => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "bound entity writes do not support CAST expressions yet",
        )),
        BoundExpr::Function { name, args } => {
            match name.as_str() {
                "lix_json" if args.len() == 1 => {}
                "lix_uuid_v7" | "lix_timestamp" | "lix_active_branch_commit_id"
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

fn candidate_snapshot(
    row: &crate::live_state::MaterializedLiveStateRow,
) -> Result<Option<JsonValue>, LixError> {
    row.snapshot_content
        .as_deref()
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

#[expect(clippy::unnecessary_wraps)]
fn entity_json_value(
    value: EntityEvalValue,
    column_type: EntityColumnType,
) -> Result<JsonValue, LixError> {
    Ok(match (value, column_type) {
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
    })
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
            .entity_pk
            .as_json_array_value()
            .map(EntityEvalValue::Json),
        "lixcol_schema_key" => Ok(EntityEvalValue::Json(JsonValue::String(
            row.schema_key.clone(),
        ))),
        "lixcol_file_id" => Ok(row
            .file_id
            .as_ref()
            .map(|value| EntityEvalValue::Json(JsonValue::String(value.clone())))
            .unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_metadata" => row
            .metadata
            .as_ref()
            .map(|metadata| parse_row_metadata_value(metadata, &row.schema_key))
            .transpose()
            .map(|metadata| {
                metadata
                    .map(EntityEvalValue::Json)
                    .unwrap_or(EntityEvalValue::SqlNull)
            }),
        "lixcol_change_id" => Ok(row
            .change_id
            .as_ref()
            .map(|value| EntityEvalValue::Json(JsonValue::String(value.to_string())))
            .unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_created_at" => Ok(EntityEvalValue::Json(JsonValue::String(
            row.created_at.clone(),
        ))),
        "lixcol_updated_at" => Ok(EntityEvalValue::Json(JsonValue::String(
            row.updated_at.clone(),
        ))),
        "lixcol_commit_id" => Ok(row
            .commit_id
            .as_ref()
            .map(|value| EntityEvalValue::Json(JsonValue::String(value.to_string())))
            .unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_global" => Ok(EntityEvalValue::Json(JsonValue::Bool(row.global))),
        "lixcol_untracked" => Ok(EntityEvalValue::Json(JsonValue::Bool(row.untracked))),
        "lixcol_branch_id" => Ok(EntityEvalValue::Json(JsonValue::String(
            row.branch_id.clone(),
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
            .as_ref()
            .map(|entity_pk| entity_pk.as_json_array_value().map(EntityEvalValue::Json))
            .transpose()
            .map(|value| value.unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_schema_key" => Ok(EntityEvalValue::Json(JsonValue::String(
            row.schema_key.clone(),
        ))),
        "lixcol_file_id" => Ok(row
            .file_id
            .as_ref()
            .map(|value| EntityEvalValue::Json(JsonValue::String(value.clone())))
            .unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_metadata" => row
            .metadata
            .as_ref()
            .map(|metadata| Ok(EntityEvalValue::Json(metadata.value().clone())))
            .transpose()
            .map(|metadata| metadata.unwrap_or(EntityEvalValue::SqlNull)),
        "lixcol_global" => Ok(EntityEvalValue::Json(JsonValue::Bool(row.global))),
        "lixcol_untracked" => Ok(EntityEvalValue::Json(JsonValue::Bool(row.untracked))),
        "lixcol_branch_id" => Ok(EntityEvalValue::Json(JsonValue::String(
            row.branch_id.clone(),
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
