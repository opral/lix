use crate::contracts::artifacts::{UpdateValidationInput, UpdateValidationInputRow};
use crate::contracts::traits::{LiveReadShapeContract, LiveStateQueryBackend};
use crate::runtime::SchemaCache;
use crate::sql::binder::bind_sql;
use crate::sql::prepare::contracts::planned_statement::UpdateValidationPlan;
use crate::sql::semantic_ir::validation::validate_update_inputs;
use crate::{LixBackend, LixError, Value};
use serde_json::Value as JsonValue;

pub(super) async fn validate_update_plans(
    backend: &dyn LixBackend,
    cache: &SchemaCache,
    plans: &[UpdateValidationPlan],
    params: &[Value],
) -> Result<(), LixError> {
    let mut inputs = Vec::with_capacity(plans.len());
    for plan in plans {
        inputs.push(load_update_validation_input(backend, plan, params).await?);
    }
    validate_update_inputs(backend, cache, &inputs).await
}

async fn load_update_validation_input(
    backend: &dyn LixBackend,
    plan: &UpdateValidationPlan,
    params: &[Value],
) -> Result<UpdateValidationInput, LixError> {
    let live_access = backend
        .load_live_read_shape_for_table_name(&plan.table)
        .await?;
    let snapshot_projection = if live_access.is_some() {
        String::new()
    } else {
        ", snapshot_content".to_string()
    };
    let normalized_projection = live_access
        .as_ref()
        .map(|access| access.normalized_projection_sql(None))
        .unwrap_or_default();
    let mut sql = format!(
        "SELECT entity_id, file_id, version_id, schema_key, schema_version{snapshot_projection}{normalized_projection} FROM {}",
        plan.table,
        snapshot_projection = snapshot_projection,
        normalized_projection = normalized_projection,
    );
    if let Some(where_clause) = &plan.where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clause.to_string());
    }

    let bound = bind_sql(&sql, params, backend.dialect())?;
    let result = backend.execute(&bound.sql, &bound.params).await?;

    let rows = result
        .rows
        .into_iter()
        .map(|row| decode_update_validation_row(live_access.as_deref(), &row))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(UpdateValidationInput {
        plan: plan.clone(),
        rows,
    })
}

fn decode_update_validation_row(
    live_access: Option<&dyn LiveReadShapeContract>,
    row: &[Value],
) -> Result<UpdateValidationInputRow, LixError> {
    let schema_key = value_to_string(&row[3], "schema_key")?;
    Ok(UpdateValidationInputRow {
        entity_id: value_to_string(&row[0], "entity_id")?,
        file_id: value_to_string(&row[1], "file_id")?,
        version_id: value_to_string(&row[2], "version_id")?,
        schema_key: schema_key.clone(),
        schema_version: value_to_string(&row[4], "schema_version")?,
        base_snapshot: required_projected_row_snapshot_json(
            live_access,
            schema_key.as_str(),
            row,
            5,
            5,
        )?,
    })
}

fn required_projected_row_snapshot_json(
    access: Option<&dyn LiveReadShapeContract>,
    schema_key: &str,
    row: &[Value],
    first_projected_column: usize,
    raw_snapshot_index: usize,
) -> Result<JsonValue, LixError> {
    let snapshot = match access {
        Some(access) => access.snapshot_from_projected_row(
            schema_key,
            row,
            first_projected_column,
            raw_snapshot_index,
        )?,
        None => value_snapshot_json(row.get(raw_snapshot_index), schema_key)?,
    };
    snapshot.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "projected row for schema '{}' did not contain a logical snapshot",
                schema_key
            ),
        )
    })
}

fn value_snapshot_json(
    value: Option<&Value>,
    schema_key: &str,
) -> Result<Option<JsonValue>, LixError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Json(json)) => Ok(Some(json.clone())),
        Some(Value::Text(text)) => serde_json::from_str::<JsonValue>(text)
            .map(Some)
            .map_err(|err| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "projected snapshot_content for schema '{}' is not valid JSON: {err}",
                        schema_key
                    ),
                )
            }),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "projected snapshot_content for schema '{}' must be JSON, text, or null, got {other:?}",
                schema_key
            ),
        )),
    }
}

fn value_to_string(value: &Value, name: &str) -> Result<String, LixError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected text value for {name}"),
        }),
    }
}
