use serde_json::Value as JsonValue;

use crate::entity_identity::EntityIdentity;
use crate::live_state::{LiveStateFilter, LiveStateScanRequest};
use crate::sql2::bind::expr::{BoundExpr, BoundLiteral};
use crate::sql2::bind::write::{
    BoundWriteInput, BoundWriteOp, BoundWriteTarget, EntityWriteSurface,
};
use crate::sql2::catalog::{
    derive_entity_surface_spec_from_schema, EntityColumnType, EntitySurfaceSpec,
};
use crate::sql2::plan::predicate::FilterSet;
use crate::sql2::plan::version_scope::VersionScope;
use crate::sql2::plan::LogicalWritePlan;
use crate::sql2::read_only::reject_read_only_entity_surface;
use crate::sql2::SqlWriteExecutionContext;
use crate::transaction::types::{
    TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteRow,
};
use crate::{parse_row_metadata_value, LixError, Value};

pub(crate) fn supports_bound_public_write(plan: &LogicalWritePlan) -> bool {
    matches!(plan.bound.target, BoundWriteTarget::Entity(_))
}

pub(crate) async fn execute_bound_public_write(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    params: &[Value],
) -> Result<u64, LixError> {
    match &plan.bound.target {
        BoundWriteTarget::Entity(surface) => execute_entity_write(ctx, plan, surface, params).await,
        _ => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "bound public write executor does not support this target yet",
        )),
    }
}

async fn execute_entity_write(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    surface: &EntityWriteSurface,
    params: &[Value],
) -> Result<u64, LixError> {
    let schema_key = match surface {
        EntityWriteSurface::Base { schema_key } | EntityWriteSurface::ByVersion { schema_key } => {
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
    let no_op = matches!(plan.bound.version_scope, VersionScope::Empty)
        || matches!(plan.filters.rows, FilterSet::None);
    match plan.bound.op {
        BoundWriteOp::Insert => {
            if no_op {
                entity_insert_rows(ctx, plan, &spec, params, None)?;
                return Ok(0);
            }
            let active_version_commit_id = load_active_version_commit_id(ctx).await?;
            entity_insert(
                ctx,
                plan,
                &spec,
                params,
                active_version_commit_id.as_deref(),
            )
            .await
        }
        BoundWriteOp::Update => {
            if no_op {
                return Ok(0);
            }
            let active_version_commit_id = load_active_version_commit_id(ctx).await?;
            entity_update(
                ctx,
                plan,
                &spec,
                params,
                active_version_commit_id.as_deref(),
            )
            .await
        }
        BoundWriteOp::Delete => {
            if no_op {
                return Ok(0);
            }
            let active_version_commit_id = load_active_version_commit_id(ctx).await?;
            entity_delete(
                ctx,
                plan,
                &spec,
                params,
                active_version_commit_id.as_deref(),
            )
            .await
        }
    }
}

async fn load_active_version_commit_id(
    ctx: &mut dyn SqlWriteExecutionContext,
) -> Result<Option<String>, LixError> {
    let active_version_id = ctx.active_version_id().to_string();
    ctx.load_version_head(&active_version_id).await
}

async fn entity_insert(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_version_commit_id: Option<&str>,
) -> Result<u64, LixError> {
    let write_rows = entity_insert_rows(ctx, plan, spec, params, active_version_commit_id)?;
    stage_rows(ctx, TransactionWriteMode::Insert, write_rows).await
}

fn entity_insert_rows(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_version_commit_id: Option<&str>,
) -> Result<Vec<TransactionWriteRow>, LixError> {
    let BoundWriteInput::Values(rows) = &plan.bound.input else {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "bound entity INSERT supports VALUES only",
        ));
    };

    let mut write_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let values = row
            .values
            .iter()
            .map(|(column, expr)| {
                Ok((
                    column.name.as_str(),
                    eval_expr(
                        expr,
                        &EntityEvalContext::insert(&JsonValue::Null),
                        ctx,
                        params,
                        active_version_commit_id,
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        write_rows.push(entity_insert_row(spec, plan, &values)?);
    }
    Ok(write_rows)
}

async fn entity_update(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_version_commit_id: Option<&str>,
) -> Result<u64, LixError> {
    let candidates = scan_entity_candidates(ctx, plan, spec).await?;
    let mut write_rows = Vec::new();
    for candidate in candidates {
        let Some(snapshot) = candidate_snapshot(&candidate)? else {
            continue;
        };
        let original_context = EntityEvalContext::live(&snapshot, &candidate);
        if !predicate_matches(
            &plan.bound.predicate,
            &original_context,
            spec,
            ctx,
            params,
            active_version_commit_id,
        )? {
            continue;
        }
        reject_projected_global_write(plan, &candidate, "UPDATE")?;
        let mut updated = snapshot.clone();
        let mut visible_assignments = Vec::new();
        for assignment in &plan.bound.assignments {
            if let Some(column) = spec.visible_column(&assignment.column.name) {
                let value = eval_expr(
                    &assignment.value,
                    &original_context,
                    ctx,
                    params,
                    active_version_commit_id,
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
            plan,
            params,
            active_version_commit_id,
        )?);
    }
    stage_rows(ctx, TransactionWriteMode::Replace, write_rows).await
}

async fn entity_delete(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
    params: &[Value],
    active_version_commit_id: Option<&str>,
) -> Result<u64, LixError> {
    let candidates = scan_entity_candidates(ctx, plan, spec).await?;
    let mut write_rows = Vec::new();
    for candidate in candidates {
        let Some(snapshot) = candidate_snapshot(&candidate)? else {
            continue;
        };
        let context = EntityEvalContext::live(&snapshot, &candidate);
        if predicate_matches(
            &plan.bound.predicate,
            &context,
            spec,
            ctx,
            params,
            active_version_commit_id,
        )? {
            reject_projected_global_write(plan, &candidate, "DELETE")?;
            write_rows.push(entity_replace_row_from_live(
                ctx,
                spec,
                &candidate,
                None,
                plan,
                params,
                active_version_commit_id,
            )?);
        }
    }
    stage_rows(ctx, TransactionWriteMode::Replace, write_rows).await
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

async fn scan_entity_candidates(
    ctx: &mut dyn SqlWriteExecutionContext,
    plan: &LogicalWritePlan,
    spec: &EntitySurfaceSpec,
) -> Result<Vec<crate::live_state::MaterializedLiveStateRow>, LixError> {
    let version_ids = scan_version_ids(&plan.bound.version_scope)?;
    let request = LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![spec.schema_key.clone()],
            version_ids,
            include_tombstones: false,
            ..LiveStateFilter::default()
        },
        ..LiveStateScanRequest::default()
    };
    ctx.scan_live_state(&request).await
}

fn entity_insert_row(
    spec: &EntitySurfaceSpec,
    plan: &LogicalWritePlan,
    values: &[(&str, JsonValue)],
) -> Result<TransactionWriteRow, LixError> {
    let mut snapshot = serde_json::Map::new();
    for column in &spec.columns {
        if let Some((_, value)) = values.iter().find(|(name, _)| *name == column.name) {
            snapshot.insert(
                column.name.clone(),
                entity_json_value(value.clone(), column.column_type)?,
            );
        }
    }
    let snapshot = JsonValue::Object(snapshot);
    let entity_id = explicit_entity_id(values)?;

    let global = bool_value(values, "lixcol_global")?.unwrap_or(false);
    let version_id = entity_row_version_id(plan, values, global)?;
    Ok(TransactionWriteRow {
        entity_id,
        schema_key: spec.schema_key.clone(),
        file_id: string_value(values, "lixcol_file_id")?,
        snapshot: Some(TransactionJson::from_value(
            snapshot,
            &format!("{} insert snapshot_content", spec.schema_key),
        )?),
        metadata: metadata_value(values, "lixcol_metadata", &spec.schema_key)?,
        origin: None,
        created_at: None,
        updated_at: None,
        global,
        change_id: None,
        commit_id: None,
        untracked: bool_value(values, "lixcol_untracked")?.unwrap_or(false),
        version_id,
    })
}

fn explicit_entity_id(values: &[(&str, JsonValue)]) -> Result<Option<EntityIdentity>, LixError> {
    let Some((_, value)) = values.iter().find(|(name, _)| *name == "lixcol_entity_id") else {
        return Ok(None);
    };
    let entity_id = entity_id_from_value(value, "lixcol_entity_id")?;
    Ok(Some(entity_id))
}

fn reject_projected_global_write(
    plan: &LogicalWritePlan,
    row: &crate::live_state::MaterializedLiveStateRow,
    action: &str,
) -> Result<(), LixError> {
    let target_is_by_version = matches!(
        &plan.bound.target,
        BoundWriteTarget::Entity(EntityWriteSurface::ByVersion { .. })
    );
    if target_is_by_version && row.global && row.version_id != crate::GLOBAL_VERSION_ID {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!(
                "{action} through an entity by-version surface cannot mutate a projected global row"
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
    plan: &LogicalWritePlan,
    params: &[Value],
    active_version_commit_id: Option<&str>,
) -> Result<TransactionWriteRow, LixError> {
    let metadata = assignment_value(plan, "lixcol_metadata")
        .map(|expr| {
            let snapshot_for_eval = candidate_snapshot(row)?.unwrap_or(JsonValue::Null);
            let context = EntityEvalContext::live(&snapshot_for_eval, row);
            eval_expr(expr, &context, ctx, params, active_version_commit_id).and_then(|value| {
                metadata_from_json_value(value, "lixcol_metadata", &spec.schema_key)
            })
        })
        .transpose()?
        .or_else(|| {
            row.metadata
                .as_ref()
                .and_then(|metadata| parse_row_metadata_value(metadata, &spec.schema_key).ok())
                .and_then(|metadata| {
                    TransactionJson::from_value(metadata, &format!("{} metadata", spec.schema_key))
                        .ok()
                })
        });

    Ok(TransactionWriteRow {
        entity_id: Some(row.entity_id.clone()),
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
        version_id: if row.global {
            crate::GLOBAL_VERSION_ID.to_string()
        } else {
            row.version_id.clone()
        },
    })
}

struct EntityEvalContext<'a> {
    snapshot: &'a JsonValue,
    row: Option<&'a crate::live_state::MaterializedLiveStateRow>,
}

impl<'a> EntityEvalContext<'a> {
    fn insert(snapshot: &'a JsonValue) -> Self {
        Self {
            snapshot,
            row: None,
        }
    }

    fn live(snapshot: &'a JsonValue, row: &'a crate::live_state::MaterializedLiveStateRow) -> Self {
        Self {
            snapshot,
            row: Some(row),
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

fn eval_expr(
    expr: &BoundExpr,
    context: &EntityEvalContext<'_>,
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    active_version_commit_id: Option<&str>,
) -> Result<JsonValue, LixError> {
    match expr {
        BoundExpr::Literal(literal) => Ok(literal_json(literal)),
        BoundExpr::Param(param) => params
            .get(param.index.saturating_sub(1))
            .map(value_json)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("missing SQL parameter ${}", param.index),
                )
            }),
        BoundExpr::Column(column) => column_json(context, &column.name),
        BoundExpr::Function { name, args } if name == "lix_json" && args.len() == 1 => {
            let raw = eval_expr(&args[0], context, ctx, params, active_version_commit_id)?;
            let JsonValue::String(raw) = raw else {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "lix_json expects a text argument",
                ));
            };
            serde_json::from_str(&raw).map_err(|error| {
                LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    format!("lix_json argument is not valid JSON: {error}"),
                )
            })
        }
        BoundExpr::Function { name, args } if name == "lix_uuid_v7" && args.is_empty() => {
            Ok(JsonValue::String(ctx.functions().call_uuid_v7()))
        }
        BoundExpr::Function { name, args } if name == "lix_timestamp" && args.is_empty() => {
            Ok(JsonValue::String(ctx.functions().call_timestamp()))
        }
        BoundExpr::Function { name, args } if name == "lix_empty_blob" && args.is_empty() => {
            Ok(JsonValue::Array(Vec::new()))
        }
        BoundExpr::Function { name, args }
            if name == "lix_active_version_commit_id" && args.is_empty() =>
        {
            Ok(active_version_commit_id
                .map(|commit_id| JsonValue::String(commit_id.to_string()))
                .unwrap_or(JsonValue::Null))
        }
        BoundExpr::Function { name, args }
            if (name == "lix_json_get" || name == "lix_json_get_text") && args.len() >= 2 =>
        {
            let root = eval_expr(&args[0], context, ctx, params, active_version_commit_id)?;
            let mut current =
                match root {
                    JsonValue::String(raw) => {
                        serde_json::from_str::<JsonValue>(&raw).map_err(|error| {
                            LixError::new(
                        LixError::CODE_TYPE_MISMATCH,
                        format!("{name} expected valid JSON text in its first argument: {error}"),
                    )
                        })?
                    }
                    JsonValue::Null => return Ok(JsonValue::Null),
                    value => value,
                };
            for arg in &args[1..] {
                let segment = eval_expr(arg, context, ctx, params, active_version_commit_id)?;
                current = json_path_get(&current, &segment, name)?.unwrap_or(JsonValue::Null);
                if current.is_null() {
                    return Ok(JsonValue::Null);
                }
            }
            if name == "lix_json_get_text" {
                Ok(JsonValue::String(json_text_value(&current)?))
            } else {
                Ok(current)
            }
        }
        BoundExpr::Function { name, args }
            if (name == "lix_text_encode" || name == "lix_text_decode")
                && (1..=2).contains(&args.len()) =>
        {
            if args.len() == 2 {
                validate_utf8_encoding(
                    eval_expr(&args[1], context, ctx, params, active_version_commit_id)?,
                    name,
                )?;
            }
            let value = eval_expr(&args[0], context, ctx, params, active_version_commit_id)?;
            if name == "lix_text_encode" {
                Ok(JsonValue::Array(
                    text_like_bytes(&value, name)?
                        .into_iter()
                        .map(JsonValue::from)
                        .collect(),
                ))
            } else {
                let bytes = binary_like_bytes(&value, name)?;
                String::from_utf8(bytes)
                    .map(JsonValue::String)
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_TYPE_MISMATCH,
                            format!("lix_text_decode() expected valid UTF8 bytes: {error}"),
                        )
                    })
            }
        }
        BoundExpr::Function { name, .. } => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!("bound entity write does not support function '{name}' yet"),
        )),
    }
}

fn predicate_matches(
    predicate: &crate::sql2::plan::predicate::BoundPredicate,
    context: &EntityEvalContext<'_>,
    spec: &EntitySurfaceSpec,
    ctx: &mut dyn SqlWriteExecutionContext,
    params: &[Value],
    active_version_commit_id: Option<&str>,
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
                    active_version_commit_id,
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        BoundPredicate::Eq(left, right) => {
            let (left, right) = eval_comparison_operands(
                left,
                right,
                context,
                spec,
                ctx,
                params,
                active_version_commit_id,
            )?;
            Ok(!left.is_null() && !right.is_null() && left == right)
        }
        BoundPredicate::In { expr, values } => {
            let candidate = eval_expr(expr, context, ctx, params, active_version_commit_id)?;
            if candidate.is_null() {
                return Ok(false);
            }
            for value_expr in values {
                let value = eval_expr(value_expr, context, ctx, params, active_version_commit_id)?;
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
    active_version_commit_id: Option<&str>,
) -> Result<(JsonValue, JsonValue), LixError> {
    let left_value = eval_expr(left, context, ctx, params, active_version_commit_id)?;
    let right_value = eval_expr(right, context, ctx, params, active_version_commit_id)?;
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
        normalize_json_param_value(left_expr, left_value, right_is_json)?,
        normalize_json_param_value(right_expr, right_value, left_is_json)?,
    ))
}

fn normalize_json_param_value(
    expr: &BoundExpr,
    value: JsonValue,
    other_side_is_json: bool,
) -> Result<JsonValue, LixError> {
    if !other_side_is_json || !matches!(expr, BoundExpr::Param(_)) {
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
        BoundWriteInput::Values(rows) => {
            for row in rows {
                for (_, expr) in &row.values {
                    validate_expr_supported(expr)?;
                }
            }
        }
        BoundWriteInput::Query(_) | BoundWriteInput::None => {}
    }
    for assignment in &plan.bound.assignments {
        validate_expr_supported(&assignment.value)?;
    }
    Ok(())
}

fn validate_predicate_supported(
    predicate: &crate::sql2::plan::predicate::BoundPredicate,
) -> Result<(), LixError> {
    use crate::sql2::plan::predicate::BoundPredicate;
    match predicate {
        BoundPredicate::True | BoundPredicate::False => Ok(()),
        BoundPredicate::And(predicates) => {
            for predicate in predicates {
                validate_predicate_supported(predicate)?;
            }
            Ok(())
        }
        BoundPredicate::Eq(left, right) => {
            validate_expr_supported(left)?;
            validate_expr_supported(right)
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
    predicate: &crate::sql2::plan::predicate::BoundPredicate,
    spec: &EntitySurfaceSpec,
) -> Result<(), LixError> {
    use crate::sql2::plan::predicate::BoundPredicate;
    match predicate {
        BoundPredicate::True | BoundPredicate::False => Ok(()),
        BoundPredicate::And(predicates) => {
            for predicate in predicates {
                validate_json_predicate_types(predicate, spec)?;
            }
            Ok(())
        }
        BoundPredicate::Eq(left, right) => validate_json_comparison_operands(left, right, spec),
        BoundPredicate::In { expr, values } => {
            if bound_expr_is_json(expr, spec) {
                for value in values {
                    require_json_comparison_operand(value, spec)?;
                }
            }
            for value in values {
                if bound_expr_is_json(value, spec) {
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
        require_json_comparison_operand(right, spec)?;
    }
    if bound_expr_is_json(right, spec) {
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

fn bound_expr_is_json(expr: &BoundExpr, spec: &EntitySurfaceSpec) -> bool {
    match expr {
        BoundExpr::Column(column) => {
            spec.visible_column(&column.name)
                .is_some_and(|column| column.column_type == EntityColumnType::Json)
                || matches!(
                    column.name.as_str(),
                    "lixcol_entity_id" | "lixcol_metadata" | "lixcol_snapshot_content"
                )
        }
        BoundExpr::Literal(BoundLiteral::Json(_)) => true,
        BoundExpr::Function { name, .. } => matches!(name.as_str(), "lix_json" | "lix_json_get"),
        _ => false,
    }
}

fn validate_expr_supported(expr: &BoundExpr) -> Result<(), LixError> {
    match expr {
        BoundExpr::Column(_) | BoundExpr::Param(_) | BoundExpr::Literal(_) => Ok(()),
        BoundExpr::Function { name, args } => {
            match name.as_str() {
                "lix_json" if args.len() == 1 => {}
                "lix_empty_blob"
                | "lix_uuid_v7"
                | "lix_timestamp"
                | "lix_active_version_commit_id"
                    if args.is_empty() => {}
                "lix_json_get" | "lix_json_get_text" if args.len() >= 2 => {}
                "lix_text_encode" | "lix_text_decode" if (1..=2).contains(&args.len()) => {}
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

fn entity_json_value(
    value: JsonValue,
    column_type: EntityColumnType,
) -> Result<JsonValue, LixError> {
    Ok(match (value, column_type) {
        (JsonValue::String(value), EntityColumnType::Json) => {
            serde_json::from_str(&value).unwrap_or(JsonValue::String(value))
        }
        (value, EntityColumnType::Json) => value,
        (JsonValue::String(value), EntityColumnType::String) => JsonValue::String(value),
        (JsonValue::Number(value), EntityColumnType::Integer) if value.is_i64() => {
            JsonValue::Number(value)
        }
        (JsonValue::Number(value), EntityColumnType::Number | EntityColumnType::Integer) => {
            JsonValue::Number(value)
        }
        (JsonValue::Bool(value), EntityColumnType::Boolean) => JsonValue::Bool(value),
        (JsonValue::Null, _) => JsonValue::Null,
        (value, _) => value,
    })
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

fn validate_utf8_encoding(value: JsonValue, fn_name: &str) -> Result<(), LixError> {
    let value = json_text_value(&value)?;
    let normalized = value.trim().to_ascii_uppercase().replace('-', "");
    if normalized == "UTF8" {
        Ok(())
    } else {
        Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("{fn_name}() only supports UTF8 encoding, got '{value}'"),
        ))
    }
}

fn text_like_bytes(value: &JsonValue, fn_name: &str) -> Result<Vec<u8>, LixError> {
    Ok(match value {
        JsonValue::String(value) => value.as_bytes().to_vec(),
        JsonValue::Number(value) => value.to_string().into_bytes(),
        JsonValue::Bool(value) => value.to_string().into_bytes(),
        JsonValue::Array(values) => values
            .iter()
            .map(byte_from_json_value)
            .collect::<Result<Vec<_>, _>>()?,
        JsonValue::Null => Vec::new(),
        other => {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("{fn_name}() expected text or binary-compatible input, got {other}"),
            ));
        }
    })
}

fn binary_like_bytes(value: &JsonValue, fn_name: &str) -> Result<Vec<u8>, LixError> {
    match value {
        JsonValue::Array(values) => values.iter().map(byte_from_json_value).collect(),
        JsonValue::String(value) => Ok(value.as_bytes().to_vec()),
        JsonValue::Null => Ok(Vec::new()),
        other => Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("{fn_name}() expected binary or text-compatible input, got {other}"),
        )),
    }
}

fn byte_from_json_value(value: &JsonValue) -> Result<u8, LixError> {
    value
        .as_u64()
        .and_then(|value| u8::try_from(value).ok())
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("binary value must contain integer bytes, got {value}"),
            )
        })
}

fn column_json(context: &EntityEvalContext<'_>, column_name: &str) -> Result<JsonValue, LixError> {
    if let Some(value) = context.snapshot.get(column_name) {
        return Ok(value.clone());
    }
    let Some(row) = context.row else {
        return Ok(JsonValue::Null);
    };
    match column_name {
        "lixcol_entity_id" => row.entity_id.as_json_array_value(),
        "lixcol_schema_key" => Ok(JsonValue::String(row.schema_key.clone())),
        "lixcol_file_id" => Ok(row
            .file_id
            .as_ref()
            .map(|value| JsonValue::String(value.clone()))
            .unwrap_or(JsonValue::Null)),
        "lixcol_metadata" => row
            .metadata
            .as_ref()
            .map(|metadata| parse_row_metadata_value(metadata, &row.schema_key))
            .transpose()
            .map(|metadata| metadata.unwrap_or(JsonValue::Null)),
        "lixcol_change_id" => Ok(row
            .change_id
            .as_ref()
            .map(|value| JsonValue::String(value.clone()))
            .unwrap_or(JsonValue::Null)),
        "lixcol_created_at" => Ok(JsonValue::String(row.created_at.clone())),
        "lixcol_updated_at" => Ok(JsonValue::String(row.updated_at.clone())),
        "lixcol_commit_id" => Ok(row
            .commit_id
            .as_ref()
            .map(|value| JsonValue::String(value.clone()))
            .unwrap_or(JsonValue::Null)),
        "lixcol_global" => Ok(JsonValue::Bool(row.global)),
        "lixcol_untracked" => Ok(JsonValue::Bool(row.untracked)),
        "lixcol_version_id" => Ok(JsonValue::String(row.version_id.clone())),
        _ => Ok(JsonValue::Null),
    }
}

fn scan_version_ids(scope: &VersionScope) -> Result<Vec<String>, LixError> {
    Ok(match scope {
        VersionScope::Active { version_id } => vec![version_id.clone()],
        VersionScope::Explicit { version_ids } | VersionScope::ExplicitRequired { version_ids } => {
            version_ids.iter().cloned().collect()
        }
        VersionScope::Global => vec![crate::GLOBAL_VERSION_ID.to_string()],
        VersionScope::Empty => Vec::new(),
    })
}

fn entity_row_version_id(
    plan: &LogicalWritePlan,
    values: &[(&str, JsonValue)],
    global: bool,
) -> Result<String, LixError> {
    let explicit_version_id = string_value(values, "lixcol_version_id")?;
    let target_version_ids = insert_target_version_ids(&plan.bound.version_scope);
    let target_is_by_version = matches!(
        &plan.bound.target,
        BoundWriteTarget::Entity(EntityWriteSurface::ByVersion { .. })
    );
    if global {
        if explicit_version_id
            .as_deref()
            .is_some_and(|version_id| version_id != crate::GLOBAL_VERSION_ID)
        {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "entity INSERT cannot combine lixcol_global = true with a non-global lixcol_version_id",
            ));
        }
        if target_is_by_version
            && target_version_ids.iter().any(|version_ids| {
                !version_ids
                    .iter()
                    .any(|version_id| version_id == crate::GLOBAL_VERSION_ID)
            })
        {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "entity INSERT cannot combine lixcol_global = true with a non-global target version",
            ));
        }
        return Ok(crate::GLOBAL_VERSION_ID.to_string());
    }
    if explicit_version_id.as_deref() == Some(crate::GLOBAL_VERSION_ID) {
        return Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            "entity INSERT with lixcol_version_id = 'global' must also set lixcol_global = true",
        ));
    }
    if target_is_by_version && matches!(plan.bound.version_scope, VersionScope::Global) {
        return Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            "entity INSERT into the global scope must set lixcol_global = true",
        ));
    }
    if let Some(version_id) = explicit_version_id {
        if target_is_by_version {
            if let Some(target_version_ids) = &target_version_ids {
                if !target_version_ids.contains(&version_id) {
                    return Err(LixError::new(
                        LixError::CODE_TYPE_MISMATCH,
                        format!(
                            "entity INSERT lixcol_version_id '{version_id}' does not match the target version scope"
                        ),
                    ));
                }
            } else {
                return Err(LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    "entity INSERT has no target version scope",
                ));
            }
        }
        return Ok(version_id);
    }
    match &plan.bound.version_scope {
        VersionScope::Active { version_id } => Ok(version_id.clone()),
        VersionScope::ExplicitRequired { version_ids } if version_ids.len() == 1 => {
            Ok(version_ids.iter().next().expect("len checked").clone())
        }
        VersionScope::Explicit { version_ids } if version_ids.len() == 1 => {
            Ok(version_ids.iter().next().expect("len checked").clone())
        }
        VersionScope::Global => Ok(crate::GLOBAL_VERSION_ID.to_string()),
        VersionScope::Empty => Ok(crate::GLOBAL_VERSION_ID.to_string()),
        _ => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "entity write requires exactly one target version",
        )),
    }
}

fn insert_target_version_ids(scope: &VersionScope) -> Option<Vec<String>> {
    match scope {
        VersionScope::Active { version_id } => Some(vec![version_id.clone()]),
        VersionScope::Explicit { version_ids } | VersionScope::ExplicitRequired { version_ids } => {
            Some(version_ids.iter().cloned().collect())
        }
        VersionScope::Global => Some(vec![crate::GLOBAL_VERSION_ID.to_string()]),
        VersionScope::Empty => Some(Vec::new()),
    }
}

fn assignment_value<'a>(plan: &'a LogicalWritePlan, column_name: &str) -> Option<&'a BoundExpr> {
    plan.bound
        .assignments
        .iter()
        .find(|assignment| assignment.column.name == column_name)
        .map(|assignment| &assignment.value)
}

fn string_value(
    values: &[(&str, JsonValue)],
    column_name: &str,
) -> Result<Option<String>, LixError> {
    match values
        .iter()
        .find(|(name, _)| *name == column_name)
        .map(|(_, value)| value)
    {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) => Ok(Some(value.clone())),
        Some(other) => Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("entity write expected text-compatible column '{column_name}', got {other}"),
        )),
    }
}

fn bool_value(values: &[(&str, JsonValue)], column_name: &str) -> Result<Option<bool>, LixError> {
    match values
        .iter()
        .find(|(name, _)| *name == column_name)
        .map(|(_, value)| value)
    {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Bool(value)) => Ok(Some(*value)),
        Some(other) => Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("entity write expected boolean column '{column_name}', got {other}"),
        )),
    }
}

fn metadata_value(
    values: &[(&str, JsonValue)],
    column_name: &str,
    context: &str,
) -> Result<Option<TransactionJson>, LixError> {
    values
        .iter()
        .find(|(name, _)| *name == column_name)
        .map(|(_, value)| metadata_from_json_value(value.clone(), column_name, context))
        .transpose()
}

fn metadata_from_json_value(
    value: JsonValue,
    column_name: &str,
    context: &str,
) -> Result<TransactionJson, LixError> {
    let metadata = match value {
        JsonValue::String(value) => parse_row_metadata_value(&value, context)?,
        JsonValue::Null => JsonValue::Null,
        other => other,
    };
    TransactionJson::from_value(metadata, &format!("{context} {column_name}"))
}

fn entity_id_from_value(value: &JsonValue, column_name: &str) -> Result<EntityIdentity, LixError> {
    match value {
        JsonValue::String(value) => EntityIdentity::from_json_array_text(value).map_err(|error| {
            LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("entity write has invalid {column_name}: {error}"),
            )
        }),
        value => EntityIdentity::from_json_array_value(value).map_err(|error| {
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
