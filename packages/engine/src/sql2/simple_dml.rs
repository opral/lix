use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, Delete, Expr, Ident, Insert, ObjectName, Query,
    SetExpr, Statement, TableFactor, TableObject, TableWithJoins, Update, Value as SqlValue,
};
use serde_json::Value as JsonValue;

use crate::entity_identity::EntityIdentity;
use crate::live_state::{LiveStateFilter, LiveStateScanRequest, MaterializedLiveStateRow};
use crate::sql2::entity_provider::{
    derive_entity_surface_spec_from_schema, EntityColumnType, EntitySurfaceSpec,
};
use crate::sql2::read_only::{reject_read_only_entity_surface, reject_read_only_stage_rows};
use crate::sql2::version_scope::resolve_write_version_scope;
use crate::transaction::types::{
    TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteRow,
};
use crate::{parse_row_metadata_value, LixError, Value, GLOBAL_VERSION_ID};

use super::SqlWriteExecutionContext;

enum SimpleDml<'ast> {
    Insert(SimpleInsert<'ast>),
    Update(SimpleUpdate<'ast>),
    Delete(SimpleDelete<'ast>),
}

struct SimpleInsert<'ast> {
    target: SimpleDmlTarget,
    columns: Vec<String>,
    value_rows: &'ast [Vec<Expr>],
}

struct SimpleUpdate<'ast> {
    target: SimpleDmlTarget,
    assignments: &'ast [Assignment],
    selection: &'ast Expr,
}

struct SimpleDelete<'ast> {
    target: SimpleDmlTarget,
    selection: &'ast Expr,
}

enum SimpleDmlTarget {
    LixState {
        active_version_id: String,
    },
    Entity {
        spec: EntitySurfaceSpec,
        version_binding: Option<String>,
    },
}

#[derive(Clone, Copy)]
enum SimpleDmlAction {
    Insert,
    Dml,
}

impl SimpleDmlAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Dml => "DML",
        }
    }
}

pub(crate) async fn try_execute_simple_write(
    ctx: &mut dyn SqlWriteExecutionContext,
    statement: &DataFusionStatement,
    params: &[Value],
) -> Result<Option<u64>, LixError> {
    let DataFusionStatement::Statement(statement) = statement else {
        return Ok(None);
    };

    let visible_schemas = ctx.list_visible_schemas()?;
    super::public_bind::validate_public_dml_statement(
        &DataFusionStatement::Statement(statement.clone()),
        &visible_schemas,
    )?;

    match classify_simple_dml(
        statement.as_ref(),
        ctx.active_version_id(),
        &visible_schemas,
    )? {
        Some(SimpleDml::Insert(insert)) => try_execute_insert(ctx, insert, params).await,
        Some(SimpleDml::Update(update)) => try_execute_update(ctx, update, params).await,
        Some(SimpleDml::Delete(delete)) => try_execute_delete(ctx, delete, params).await,
        None => Ok(None),
    }
}

fn classify_simple_dml<'ast>(
    statement: &'ast Statement,
    active_version_id: &str,
    visible_schemas: &[JsonValue],
) -> Result<Option<SimpleDml<'ast>>, LixError> {
    match statement {
        Statement::Insert(insert) => {
            classify_simple_insert(insert, active_version_id, visible_schemas)
                .map(|insert| insert.map(SimpleDml::Insert))
        }
        Statement::Update(update) => {
            classify_simple_update(update, active_version_id, visible_schemas)
                .map(|update| update.map(SimpleDml::Update))
        }
        Statement::Delete(delete) => {
            classify_simple_delete(delete, active_version_id, visible_schemas)
                .map(|delete| delete.map(SimpleDml::Delete))
        }
        _ => Ok(None),
    }
}

fn classify_simple_insert<'ast>(
    insert: &'ast Insert,
    active_version_id: &str,
    visible_schemas: &[JsonValue],
) -> Result<Option<SimpleInsert<'ast>>, LixError> {
    if insert.columns.is_empty()
        || insert.overwrite
        || insert.source.is_none()
        || !insert.assignments.is_empty()
        || insert.on.is_some()
        || insert.returning.is_some()
        || insert.replace_into
    {
        return Ok(None);
    }
    let target = match &insert.table {
        TableObject::TableName(name) => {
            simple_dml_target(name, active_version_id, visible_schemas)?
        }
        _ => None,
    };
    let Some(target) = target else {
        return Ok(None);
    };

    let Some(source) = &insert.source else {
        return Ok(None);
    };
    let Some(value_rows) = query_values(source) else {
        return Ok(None);
    };
    Ok(Some(SimpleInsert {
        target,
        columns: insert.columns.iter().map(ident_name).collect(),
        value_rows,
    }))
}

async fn try_execute_insert(
    ctx: &mut dyn SqlWriteExecutionContext,
    insert: SimpleInsert<'_>,
    params: &[Value],
) -> Result<Option<u64>, LixError> {
    let mut decoder = ParamDecoder::new(params);
    let rows = match insert.target {
        SimpleDmlTarget::LixState { active_version_id } => match decode_lix_state_insert(
            &active_version_id,
            &insert.columns,
            insert.value_rows,
            &mut decoder,
        ) {
            Ok(rows) => rows,
            Err(error) if is_fast_path_miss(&error) => return Ok(None),
            Err(error) => return Err(error),
        },
        SimpleDmlTarget::Entity {
            spec,
            version_binding,
        } => match decode_entity_insert(
            &spec,
            version_binding.as_deref(),
            &insert.columns,
            insert.value_rows,
            &mut decoder,
        ) {
            Ok(rows) => rows,
            Err(error) if is_fast_path_miss(&error) => return Ok(None),
            Err(error) => return Err(error),
        },
    };
    decoder.validate_count()?;

    let count = u64::try_from(rows.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_UNKNOWN,
            "simple INSERT row count overflow".to_string(),
        )
    })?;
    reject_read_only_stage_rows(&rows, "INSERT into lix_state").map_err(datafusion_to_lix_error)?;
    ctx.stage_write(TransactionWrite::Rows {
        mode: TransactionWriteMode::Insert,
        rows,
    })
    .await?;
    Ok(Some(count))
}

fn classify_simple_update<'ast>(
    update: &'ast Update,
    active_version_id: &str,
    visible_schemas: &[JsonValue],
) -> Result<Option<SimpleUpdate<'ast>>, LixError> {
    if update.from.is_some()
        || update.returning.is_some()
        || update.or.is_some()
        || update.limit.is_some()
        || !update.table.joins.is_empty()
    {
        return Ok(None);
    }
    let Some(target) = simple_update_target(&update.table, active_version_id, visible_schemas)?
    else {
        return Ok(None);
    };
    let Some(selection) = &update.selection else {
        return Ok(None);
    };
    Ok(Some(SimpleUpdate {
        target,
        assignments: &update.assignments,
        selection,
    }))
}

async fn try_execute_update(
    ctx: &mut dyn SqlWriteExecutionContext,
    update: SimpleUpdate<'_>,
    params: &[Value],
) -> Result<Option<u64>, LixError> {
    let mut decoder = ParamDecoder::new(params);
    let assignments = match simple_assignments(update.assignments, &mut decoder) {
        Ok(assignments) => assignments,
        Err(error) if is_fast_path_miss(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    match update.target {
        SimpleDmlTarget::LixState { active_version_id } => {
            let filter = match simple_lix_state_filter(
                update.selection,
                &mut decoder,
                Some(active_version_id.as_str()),
            ) {
                Ok(filter) => filter,
                Err(error) if is_fast_path_miss(&error) => return Ok(None),
                Err(error) => return Err(error),
            };
            decoder.validate_count()?;
            return execute_lix_state_update(ctx, filter, assignments)
                .await
                .map(Some);
        }
        SimpleDmlTarget::Entity {
            spec,
            version_binding,
        } => {
            let filter = match simple_entity_filter(
                update.selection,
                &mut decoder,
                &spec,
                version_binding.as_deref(),
                false,
            ) {
                Ok(filter) => filter,
                Err(error) if is_fast_path_miss(&error) => return Ok(None),
                Err(error) => return Err(error),
            };
            decoder.validate_count()?;
            return execute_entity_update(ctx, &spec, filter, assignments)
                .await
                .map(Some);
        }
    }
}

fn classify_simple_delete<'ast>(
    delete: &'ast Delete,
    active_version_id: &str,
    visible_schemas: &[JsonValue],
) -> Result<Option<SimpleDelete<'ast>>, LixError> {
    if !delete.tables.is_empty()
        || delete.using.is_some()
        || delete.returning.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
    {
        return Ok(None);
    }
    let Some(target) = simple_delete_target(delete, active_version_id, visible_schemas)? else {
        return Ok(None);
    };
    let Some(selection) = &delete.selection else {
        return Ok(None);
    };
    Ok(Some(SimpleDelete { target, selection }))
}

async fn try_execute_delete(
    ctx: &mut dyn SqlWriteExecutionContext,
    delete: SimpleDelete<'_>,
    params: &[Value],
) -> Result<Option<u64>, LixError> {
    match delete.target {
        SimpleDmlTarget::LixState { active_version_id } => {
            let mut decoder = ParamDecoder::new(params);
            let filter = match simple_lix_state_filter(
                delete.selection,
                &mut decoder,
                Some(active_version_id.as_str()),
            ) {
                Ok(filter) => filter,
                Err(error) if is_fast_path_miss(&error) => return Ok(None),
                Err(error) => return Err(error),
            };
            decoder.validate_count()?;
            return execute_lix_state_delete(ctx, filter).await.map(Some);
        }
        SimpleDmlTarget::Entity {
            spec,
            version_binding,
        } => {
            let require_version_filter = version_binding.is_none();
            let mut decoder = ParamDecoder::new(params);
            let filter = match simple_entity_filter(
                delete.selection,
                &mut decoder,
                &spec,
                version_binding.as_deref(),
                require_version_filter,
            ) {
                Ok(filter) => filter,
                Err(error) if is_fast_path_miss(&error) => return Ok(None),
                Err(error) => return Err(error),
            };
            decoder.validate_count()?;
            return execute_entity_delete(ctx, &spec, filter).await.map(Some);
        }
    }
}

fn query_values(query: &Query) -> Option<&[Vec<Expr>]> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || !query.locks.is_empty()
        || query.fetch.is_some()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return None;
    }
    match query.body.as_ref() {
        SetExpr::Values(values) => Some(values.rows.as_slice()),
        _ => None,
    }
}

fn decode_lix_state_insert(
    active_version_id: &str,
    columns: &[String],
    value_rows: &[Vec<Expr>],
    decoder: &mut ParamDecoder<'_>,
) -> Result<Vec<TransactionWriteRow>, LixError> {
    value_rows
        .iter()
        .map(|values| {
            let cells = row_cells(columns, values, decoder)?;
            let global = optional_bool(&cells, "global", "INSERT into lix_state")?.unwrap_or(false);
            let version_id = optional_string(&cells, "version_id", "INSERT into lix_state")?
                .unwrap_or_else(|| {
                    if global {
                        GLOBAL_VERSION_ID.to_string()
                    } else {
                        active_version_id.to_string()
                    }
                });
            let entity_id = required_string(&cells, "entity_id", "INSERT into lix_state")?;
            Ok(TransactionWriteRow {
                entity_id: Some(EntityIdentity::from_json_array_text(&entity_id).map_err(
                    |error| {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            format!("lix_state INSERT has invalid entity_id: {error}"),
                        )
                    },
                )?),
                schema_key: required_string(&cells, "schema_key", "INSERT into lix_state")?,
                file_id: optional_string(&cells, "file_id", "INSERT into lix_state")?,
                snapshot: optional_json(&cells, "snapshot_content", "lix_state")?,
                metadata: optional_metadata(&cells, "metadata", "lix_state")?,
                origin: None,
                created_at: optional_string(&cells, "created_at", "INSERT into lix_state")?,
                updated_at: optional_string(&cells, "updated_at", "INSERT into lix_state")?,
                global,
                change_id: optional_string(&cells, "change_id", "INSERT into lix_state")?,
                commit_id: optional_string(&cells, "commit_id", "INSERT into lix_state")?,
                untracked: optional_bool(&cells, "untracked", "INSERT into lix_state")?
                    .unwrap_or(false),
                version_id,
            })
        })
        .collect()
}

fn decode_entity_insert(
    spec: &EntitySurfaceSpec,
    version_binding: Option<&str>,
    columns: &[String],
    value_rows: &[Vec<Expr>],
    decoder: &mut ParamDecoder<'_>,
) -> Result<Vec<TransactionWriteRow>, LixError> {
    validate_entity_insert_columns(spec, version_binding, columns)?;
    value_rows
        .iter()
        .map(|values| {
            let cells = row_cells(columns, values, decoder)?;
            let scope = resolve_write_version_scope(
                optional_bool(&cells, "lixcol_global", "INSERT into entity surface")?,
                optional_string(&cells, "lixcol_version_id", "INSERT into entity surface")?,
                version_binding,
                &format!("INSERT into {}_by_version", spec.schema_key),
                &spec.schema_key,
            )
            .map_err(datafusion_to_lix_error)?;

            if let Some(schema_key) =
                optional_string(&cells, "lixcol_schema_key", "INSERT into entity surface")?
            {
                if schema_key != spec.schema_key {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!(
                            "INSERT into entity surface '{}' cannot set lixcol_schema_key to '{}'",
                            spec.schema_key, schema_key
                        ),
                    ));
                }
            }
            reject_present_entity_insert_field(&cells, "lixcol_snapshot_content")?;
            reject_present_entity_insert_field(&cells, "lixcol_created_at")?;
            reject_present_entity_insert_field(&cells, "lixcol_updated_at")?;
            reject_present_entity_insert_field(&cells, "lixcol_change_id")?;
            reject_present_entity_insert_field(&cells, "lixcol_commit_id")?;

            let explicit_entity_id =
                optional_string(&cells, "lixcol_entity_id", "INSERT into entity surface")?;
            let entity_id = if spec.primary_key_paths.is_empty() {
                let entity_id = explicit_entity_id.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!(
                            "INSERT into entity surface '{}' requires lixcol_entity_id because the schema has no x-lix-primary-key",
                            spec.schema_key
                        ),
                    )
                })?;
                Some(entity_identity(
                    &entity_id,
                    &spec.schema_key,
                    SimpleDmlAction::Insert,
                )?)
            } else {
                explicit_entity_id
                    .map(|entity_id| {
                        entity_identity(&entity_id, &spec.schema_key, SimpleDmlAction::Insert)
                    })
                    .transpose()?
            };

            let mut snapshot = serde_json::Map::new();
            for column in &spec.columns {
                let Some(value) = cells.get(&column.name) else {
                    continue;
                };
                snapshot.insert(
                    column.name.clone(),
                    entity_json_value(value, column.column_type)?,
                );
            }

            Ok(TransactionWriteRow {
                entity_id,
                schema_key: spec.schema_key.clone(),
                file_id: optional_string(&cells, "lixcol_file_id", "INSERT into entity surface")?,
                snapshot: Some(TransactionJson::from_value(
                    JsonValue::Object(snapshot),
                    &format!("{} insert snapshot_content", spec.schema_key),
                )?),
                metadata: optional_metadata(&cells, "lixcol_metadata", &spec.schema_key)?,
                origin: None,
                created_at: None,
                updated_at: None,
                global: scope.global,
                change_id: None,
                commit_id: None,
                untracked: optional_bool(&cells, "lixcol_untracked", "INSERT into entity surface")?
                    .unwrap_or(false),
                version_id: scope.version_id,
            })
        })
        .collect()
}

fn validate_entity_insert_columns(
    spec: &EntitySurfaceSpec,
    version_binding: Option<&str>,
    columns: &[String],
) -> Result<(), LixError> {
    for column in columns {
        let is_visible_column = spec.columns.iter().any(|field| field.name == *column);
        let is_public_system_column = matches!(
            column.as_str(),
            "lixcol_entity_id"
                | "lixcol_schema_key"
                | "lixcol_file_id"
                | "lixcol_snapshot_content"
                | "lixcol_metadata"
                | "lixcol_created_at"
                | "lixcol_updated_at"
                | "lixcol_global"
                | "lixcol_change_id"
                | "lixcol_commit_id"
                | "lixcol_untracked"
        );
        let is_by_version_column = column == "lixcol_version_id" && version_binding.is_none();
        if is_visible_column || is_public_system_column || is_by_version_column {
            continue;
        }
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!(
                "simple DML fast path does not support INSERT column '{column}' for entity surface '{}'",
                spec.schema_key
            ),
        ));
    }
    Ok(())
}

async fn execute_lix_state_delete(
    ctx: &mut dyn SqlWriteExecutionContext,
    filter: LiveStateFilter,
) -> Result<u64, LixError> {
    let rows = ctx
        .scan_live_state(&LiveStateScanRequest {
            filter,
            projection: Default::default(),
            limit: None,
        })
        .await?;
    let write_rows = rows
        .iter()
        .map(|row| live_lix_state_write_row(row, None, live_metadata(row, "lix_state")?))
        .collect::<Result<Vec<_>, _>>()?;
    let count = u64::try_from(write_rows.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_UNKNOWN,
            "simple DELETE row count overflow".to_string(),
        )
    })?;
    if count > 0 {
        reject_read_only_stage_rows(&write_rows, "DELETE FROM lix_state")
            .map_err(datafusion_to_lix_error)?;
        ctx.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows: write_rows,
        })
        .await?;
    }
    Ok(count)
}

async fn execute_lix_state_update(
    ctx: &mut dyn SqlWriteExecutionContext,
    filter: LiveStateFilter,
    assignments: std::collections::BTreeMap<String, Value>,
) -> Result<u64, LixError> {
    if assignments
        .keys()
        .any(|column| !matches!(column.as_str(), "snapshot_content" | "metadata"))
    {
        return Err(LixError::new(
            LixError::CODE_READ_ONLY,
            "UPDATE lix_state can only stage snapshot_content and metadata",
        ));
    }
    let rows = ctx
        .scan_live_state(&LiveStateScanRequest {
            filter,
            projection: Default::default(),
            limit: None,
        })
        .await?;
    let write_rows = rows
        .iter()
        .map(|row| {
            let snapshot = if assignments.contains_key("snapshot_content") {
                assigned_optional_json(&assignments, "snapshot_content", "lix_state")?
            } else {
                live_snapshot(row, "lix_state")?
            };
            let metadata = if assignments.contains_key("metadata") {
                assigned_optional_metadata(&assignments, "metadata", "lix_state")?
            } else {
                live_metadata(row, "lix_state")?
            };
            live_lix_state_write_row(row, snapshot, metadata)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let count = u64::try_from(write_rows.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_UNKNOWN,
            "simple UPDATE row count overflow".to_string(),
        )
    })?;
    if count > 0 {
        reject_read_only_stage_rows(&write_rows, "UPDATE lix_state")
            .map_err(datafusion_to_lix_error)?;
        ctx.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows: write_rows,
        })
        .await?;
    }
    Ok(count)
}

async fn execute_entity_delete(
    ctx: &mut dyn SqlWriteExecutionContext,
    spec: &EntitySurfaceSpec,
    filter: LiveStateFilter,
) -> Result<u64, LixError> {
    let rows = ctx
        .scan_live_state(&LiveStateScanRequest {
            filter,
            projection: Default::default(),
            limit: None,
        })
        .await?;
    let write_rows = rows
        .iter()
        .map(|row| live_entity_write_row(spec, row, None, live_metadata(row, &spec.schema_key)?))
        .collect::<Result<Vec<_>, _>>()?;
    let count = u64::try_from(write_rows.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_UNKNOWN,
            "simple DELETE row count overflow".to_string(),
        )
    })?;
    if count > 0 {
        ctx.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows: write_rows,
        })
        .await?;
    }
    Ok(count)
}

async fn execute_entity_update(
    ctx: &mut dyn SqlWriteExecutionContext,
    spec: &EntitySurfaceSpec,
    filter: LiveStateFilter,
    assignments: std::collections::BTreeMap<String, Value>,
) -> Result<u64, LixError> {
    if assignments.keys().any(|column| {
        column != "lixcol_metadata" && !spec.columns.iter().any(|field| field.name == *column)
    }) {
        return Err(LixError::new(
            LixError::CODE_READ_ONLY,
            format!(
                "UPDATE entity surface '{}' can only stage visible columns and lixcol_metadata",
                spec.schema_key
            ),
        ));
    }
    let rows = ctx
        .scan_live_state(&LiveStateScanRequest {
            filter,
            projection: Default::default(),
            limit: None,
        })
        .await?;
    let write_rows = rows
        .iter()
        .map(|row| {
            let mut snapshot = existing_snapshot(row, &spec.schema_key)?;
            for column in &spec.columns {
                let Some(value) = assignments.get(&column.name) else {
                    continue;
                };
                snapshot.insert(
                    column.name.clone(),
                    entity_json_value(value, column.column_type)?,
                );
            }
            let metadata = if assignments.contains_key("lixcol_metadata") {
                assigned_optional_metadata(&assignments, "lixcol_metadata", &spec.schema_key)?
            } else {
                live_metadata(row, &spec.schema_key)?
            };
            live_entity_write_row(spec, row, Some(JsonValue::Object(snapshot)), metadata)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let count = u64::try_from(write_rows.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_UNKNOWN,
            "simple UPDATE row count overflow".to_string(),
        )
    })?;
    if count > 0 {
        ctx.stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows: write_rows,
        })
        .await?;
    }
    Ok(count)
}

fn live_lix_state_write_row(
    row: &MaterializedLiveStateRow,
    snapshot: Option<TransactionJson>,
    metadata: Option<TransactionJson>,
) -> Result<TransactionWriteRow, LixError> {
    Ok(TransactionWriteRow {
        entity_id: Some(row.entity_id.clone()),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot,
        metadata,
        origin: None,
        created_at: None,
        updated_at: None,
        global: row.global,
        change_id: None,
        commit_id: None,
        untracked: row.untracked,
        version_id: row.version_id.clone(),
    })
}

fn live_entity_write_row(
    spec: &EntitySurfaceSpec,
    row: &MaterializedLiveStateRow,
    snapshot: Option<JsonValue>,
    metadata: Option<TransactionJson>,
) -> Result<TransactionWriteRow, LixError> {
    Ok(TransactionWriteRow {
        entity_id: Some(row.entity_id.clone()),
        schema_key: spec.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot: snapshot
            .map(|value| TransactionJson::from_value(value, &format!("{} update", spec.schema_key)))
            .transpose()?,
        metadata,
        origin: None,
        created_at: None,
        updated_at: None,
        global: row.global,
        change_id: None,
        commit_id: None,
        untracked: row.untracked,
        version_id: row.version_id.clone(),
    })
}

fn live_metadata(
    row: &MaterializedLiveStateRow,
    context: &str,
) -> Result<Option<TransactionJson>, LixError> {
    row.metadata
        .as_ref()
        .map(|value| {
            parse_row_metadata_value(value, context)
                .and_then(|value| TransactionJson::from_value(value, context))
        })
        .transpose()
}

fn live_snapshot(
    row: &MaterializedLiveStateRow,
    context: &str,
) -> Result<Option<TransactionJson>, LixError> {
    row.snapshot_content
        .as_ref()
        .map(|value| {
            serde_json::from_str::<JsonValue>(value)
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("{context} expected valid snapshot JSON: {error}"),
                    )
                })
                .and_then(|value| TransactionJson::from_value(value, context))
        })
        .transpose()
}

fn existing_snapshot(
    row: &MaterializedLiveStateRow,
    schema_key: &str,
) -> Result<serde_json::Map<String, JsonValue>, LixError> {
    let snapshot = row.snapshot_content.as_ref().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("UPDATE entity surface '{schema_key}' requires existing snapshot_content"),
        )
    })?;
    match serde_json::from_str::<JsonValue>(snapshot).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("UPDATE entity surface '{schema_key}' expected valid snapshot JSON: {error}"),
        )
    })? {
        JsonValue::Object(object) => Ok(object),
        other => Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("UPDATE entity surface '{schema_key}' expected object snapshot, got {other}"),
        )),
    }
}

fn assigned_optional_json(
    assignments: &std::collections::BTreeMap<String, Value>,
    column: &str,
    context: &str,
) -> Result<Option<TransactionJson>, LixError> {
    optional_json(assignments, column, context)
}

fn assigned_optional_metadata(
    assignments: &std::collections::BTreeMap<String, Value>,
    column: &str,
    context: &str,
) -> Result<Option<TransactionJson>, LixError> {
    optional_metadata(assignments, column, context)
}

fn simple_assignments(
    assignments: &[datafusion::sql::sqlparser::ast::Assignment],
    decoder: &mut ParamDecoder<'_>,
) -> Result<std::collections::BTreeMap<String, Value>, LixError> {
    let mut result = std::collections::BTreeMap::new();
    for assignment in assignments {
        let AssignmentTarget::ColumnName(name) = &assignment.target else {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "simple DML fast path does not support tuple assignments",
            ));
        };
        let Some(column) = object_name_leaf(name) else {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "simple DML fast path does not support dynamic assignment targets",
            ));
        };
        result.insert(column, decoder.expr_value(&assignment.value)?);
    }
    Ok(result)
}

fn simple_lix_state_filter(
    expr: &Expr,
    decoder: &mut ParamDecoder<'_>,
    active_version_id: Option<&str>,
) -> Result<LiveStateFilter, LixError> {
    let mut filter = LiveStateFilter {
        version_ids: active_version_id
            .map(|version_id| vec![version_id.to_string()])
            .unwrap_or_default(),
        ..LiveStateFilter::default()
    };
    apply_simple_filter(expr, decoder, &mut filter, None, true)?;
    if filter.schema_keys.is_empty() && filter.entity_ids.is_empty() && filter.file_ids.is_empty() {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "simple lix_state DML requires an identity predicate",
        ));
    }
    Ok(filter)
}

fn simple_entity_filter(
    expr: &Expr,
    decoder: &mut ParamDecoder<'_>,
    spec: &EntitySurfaceSpec,
    active_version_id: Option<&str>,
    require_version_filter: bool,
) -> Result<LiveStateFilter, LixError> {
    let mut filter = LiveStateFilter {
        schema_keys: vec![spec.schema_key.clone()],
        version_ids: active_version_id
            .map(|version_id| vec![version_id.to_string()])
            .unwrap_or_default(),
        ..LiveStateFilter::default()
    };
    apply_simple_filter(
        expr,
        decoder,
        &mut filter,
        Some(spec),
        active_version_id.is_none(),
    )?;
    if filter.entity_ids.is_empty() {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "simple entity DML requires a lixcol_entity_id predicate",
        ));
    }
    if require_version_filter && filter.version_ids.is_empty() {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "simple entity DML requires a lixcol_version_id predicate",
        ));
    }
    Ok(filter)
}

fn apply_simple_filter(
    expr: &Expr,
    decoder: &mut ParamDecoder<'_>,
    filter: &mut LiveStateFilter,
    spec: Option<&EntitySurfaceSpec>,
    allow_version_filter: bool,
) -> Result<(), LixError> {
    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            apply_simple_filter(left, decoder, filter, spec, allow_version_filter)?;
            apply_simple_filter(right, decoder, filter, spec, allow_version_filter)
        }
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
            let right_values = [right.as_ref()];
            let left_values = [left.as_ref()];
            apply_column_values(
                left,
                &right_values,
                decoder,
                filter,
                spec,
                allow_version_filter,
            )
            .or_else(|_| {
                apply_column_values(
                    right,
                    &left_values,
                    decoder,
                    filter,
                    spec,
                    allow_version_filter,
                )
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } if !negated => {
            let values = list.iter().collect::<Vec<_>>();
            apply_column_values(expr, &values, decoder, filter, spec, allow_version_filter)
        }
        _ => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "simple DML fast path only supports equality and IN predicates",
        )),
    }
}

fn apply_column_values(
    column_expr: &Expr,
    value_exprs: &[&Expr],
    decoder: &mut ParamDecoder<'_>,
    filter: &mut LiveStateFilter,
    spec: Option<&EntitySurfaceSpec>,
    allow_version_filter: bool,
) -> Result<(), LixError> {
    let column = column_name(column_expr).ok_or_else(|| {
        LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "simple DML predicate must compare a column to literals or params",
        )
    })?;
    let values = value_exprs
        .iter()
        .map(|expr| decoder.expr_value(expr))
        .collect::<Result<Vec<_>, _>>()?;
    match column.as_str() {
        "schema_key" if spec.is_none() => {
            if merge_string_filter(
                &mut filter.schema_keys,
                string_values(values, "schema_key")?,
            )? {
                filter.no_match = true;
            }
        }
        "version_id" | "lixcol_version_id" if allow_version_filter => {
            if merge_string_filter(&mut filter.version_ids, string_values(values, &column)?)? {
                filter.no_match = true;
            }
        }
        "entity_id" | "lixcol_entity_id" => {
            let entity_ids = string_values(values, &column)?
                .into_iter()
                .map(|value| {
                    entity_identity(
                        &value,
                        spec.map_or("lix_state", |spec| &spec.schema_key),
                        SimpleDmlAction::Dml,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            if merge_entity_filter(&mut filter.entity_ids, entity_ids)? {
                filter.no_match = true;
            }
        }
        _ => {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "simple DML fast path only supports identity predicates",
            ))
        }
    }
    Ok(())
}

fn merge_string_filter(target: &mut Vec<String>, values: Vec<String>) -> Result<bool, LixError> {
    if values.is_empty() {
        return Ok(true);
    }
    if target.is_empty() {
        *target = values;
        return Ok(false);
    }
    target.retain(|value| values.contains(value));
    if target.is_empty() {
        return Ok(true);
    }
    Ok(false)
}

fn merge_entity_filter(
    target: &mut Vec<EntityIdentity>,
    values: Vec<EntityIdentity>,
) -> Result<bool, LixError> {
    if values.is_empty() {
        return Ok(true);
    }
    if target.is_empty() {
        *target = values;
        return Ok(false);
    }
    target.retain(|value| values.contains(value));
    if target.is_empty() {
        return Ok(true);
    }
    Ok(false)
}

fn string_values(values: Vec<Value>, column: &str) -> Result<Vec<String>, LixError> {
    values
        .into_iter()
        .map(|value| match value {
            Value::Text(value) => Ok(value),
            other => Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("predicate column '{column}' expected text, got {other:?}"),
            )),
        })
        .collect()
}

fn row_cells(
    columns: &[String],
    values: &[Expr],
    decoder: &mut ParamDecoder<'_>,
) -> Result<std::collections::BTreeMap<String, Value>, LixError> {
    if columns.len() != values.len() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!(
                "INSERT expected {} value(s), got {}",
                columns.len(),
                values.len()
            ),
        ));
    }
    columns
        .iter()
        .zip(values)
        .map(|(column, value)| Ok((column.clone(), decoder.expr_value(value)?)))
        .collect()
}

fn simple_dml_target(
    target_name: &ObjectName,
    active_version_id: &str,
    visible_schemas: &[JsonValue],
) -> Result<Option<SimpleDmlTarget>, LixError> {
    let Some(target_name) = object_name_leaf(target_name) else {
        return Ok(None);
    };
    if target_name == "lix_state" {
        return Ok(Some(SimpleDmlTarget::LixState {
            active_version_id: active_version_id.to_string(),
        }));
    }
    for schema in visible_schemas {
        let Ok(spec) = derive_entity_surface_spec_from_schema(schema) else {
            continue;
        };
        if target_name == spec.schema_key {
            if reject_read_only_entity_surface(&spec.schema_key, "DML").is_err() {
                return Ok(None);
            }
            return Ok(Some(SimpleDmlTarget::Entity {
                spec,
                version_binding: Some(active_version_id.to_string()),
            }));
        }
        if target_name == format!("{}_by_version", spec.schema_key) {
            if reject_read_only_entity_surface(&spec.schema_key, "DML").is_err() {
                return Ok(None);
            }
            return Ok(Some(SimpleDmlTarget::Entity {
                spec,
                version_binding: None,
            }));
        }
    }
    Ok(None)
}

fn simple_update_target(
    update_table: &TableWithJoins,
    active_version_id: &str,
    visible_schemas: &[JsonValue],
) -> Result<Option<SimpleDmlTarget>, LixError> {
    let TableFactor::Table { name, .. } = &update_table.relation else {
        return Ok(None);
    };
    simple_dml_target(name, active_version_id, visible_schemas)
}

fn simple_delete_target(
    delete: &Delete,
    active_version_id: &str,
    visible_schemas: &[JsonValue],
) -> Result<Option<SimpleDmlTarget>, LixError> {
    let tables = match &delete.from {
        datafusion::sql::sqlparser::ast::FromTable::WithFromKeyword(tables)
        | datafusion::sql::sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return Ok(None);
    }
    simple_update_target(&tables[0], active_version_id, visible_schemas)
}

fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident_name(ident)),
        Expr::CompoundIdentifier(parts) => parts.last().map(ident_name),
        _ => None,
    }
}

fn optional_string(
    cells: &std::collections::BTreeMap<String, Value>,
    column: &str,
    context: &str,
) -> Result<Option<String>, LixError> {
    match cells.get(column) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Text(value)) => Ok(Some(value.clone())),
        Some(other) => Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("{context} expected text-compatible column '{column}', got {other:?}"),
        )),
    }
}

fn required_string(
    cells: &std::collections::BTreeMap<String, Value>,
    column: &str,
    context: &str,
) -> Result<String, LixError> {
    optional_string(cells, column, context)?.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("{context} requires non-null text column '{column}'"),
        )
    })
}

fn optional_bool(
    cells: &std::collections::BTreeMap<String, Value>,
    column: &str,
    context: &str,
) -> Result<Option<bool>, LixError> {
    match cells.get(column) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Boolean(value)) => Ok(Some(*value)),
        Some(other) => Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("{context} expected boolean column '{column}', got {other:?}"),
        )),
    }
}

fn optional_json(
    cells: &std::collections::BTreeMap<String, Value>,
    column: &str,
    context: &str,
) -> Result<Option<TransactionJson>, LixError> {
    optional_string(cells, column, context)?
        .map(|value| {
            serde_json::from_str::<JsonValue>(&value)
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("{context} expected valid JSON in column '{column}': {error}"),
                    )
                })
                .and_then(|value| TransactionJson::from_value(value, context))
        })
        .transpose()
}

fn optional_metadata(
    cells: &std::collections::BTreeMap<String, Value>,
    column: &str,
    context: &str,
) -> Result<Option<TransactionJson>, LixError> {
    optional_string(cells, column, context)?
        .map(|value| {
            parse_row_metadata_value(&value, context)
                .and_then(|value| TransactionJson::from_value(value, context))
        })
        .transpose()
}

fn reject_present_entity_insert_field(
    cells: &std::collections::BTreeMap<String, Value>,
    column: &str,
) -> Result<(), LixError> {
    if cells
        .get(column)
        .is_some_and(|value| !matches!(value, Value::Null))
    {
        return Err(LixError::new(
            LixError::CODE_READ_ONLY,
            format!("INSERT into entity surface cannot stage read-only column '{column}'"),
        ));
    }
    Ok(())
}

fn entity_json_value(value: &Value, column_type: EntityColumnType) -> Result<JsonValue, LixError> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Boolean(value) => Ok(JsonValue::Bool(*value)),
        Value::Integer(value) => Ok(JsonValue::from(*value)),
        Value::Real(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .ok_or_else(|| LixError::new(LixError::CODE_TYPE_MISMATCH, "invalid JSON number")),
        Value::Text(value) => match column_type {
            EntityColumnType::Json => Ok(
                serde_json::from_str(value).unwrap_or_else(|_| JsonValue::String(value.clone()))
            ),
            EntityColumnType::Integer => {
                value.parse::<i64>().map(JsonValue::from).map_err(|error| {
                    LixError::new(
                        LixError::CODE_TYPE_MISMATCH,
                        format!("entity integer column expected integer text: {error}"),
                    )
                })
            }
            EntityColumnType::Number => value
                .parse::<f64>()
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_TYPE_MISMATCH,
                        format!("entity number column expected number text: {error}"),
                    )
                })
                .and_then(|value| {
                    serde_json::Number::from_f64(value)
                        .map(JsonValue::Number)
                        .ok_or_else(|| {
                            LixError::new(LixError::CODE_TYPE_MISMATCH, "invalid JSON number")
                        })
                }),
            EntityColumnType::Boolean => {
                value.parse::<bool>().map(JsonValue::from).map_err(|error| {
                    LixError::new(
                        LixError::CODE_TYPE_MISMATCH,
                        format!("entity boolean column expected boolean text: {error}"),
                    )
                })
            }
            EntityColumnType::String => Ok(JsonValue::String(value.clone())),
        },
        Value::Json(value) => Ok(value.clone()),
        Value::Blob(_) => Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "entity JSON columns cannot store blob values directly",
        )),
    }
}

fn entity_identity(
    value: &str,
    schema_key: &str,
    action: SimpleDmlAction,
) -> Result<EntityIdentity, LixError> {
    EntityIdentity::from_json_array_text(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!(
                "{} entity surface '{schema_key}' has invalid lixcol_entity_id: {error}",
                action.as_str()
            ),
        )
    })
}

struct ParamDecoder<'a> {
    params: &'a [Value],
    max_placeholder: usize,
}

impl<'a> ParamDecoder<'a> {
    fn new(params: &'a [Value]) -> Self {
        Self {
            params,
            max_placeholder: 0,
        }
    }

    fn expr_value(&mut self, expr: &Expr) -> Result<Value, LixError> {
        match expr {
            Expr::Value(value) => self.sql_value(&value.value),
            Expr::Nested(expr) => self.expr_value(expr),
            _ => Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "simple DML fast path only supports literal values and bound parameters",
            )),
        }
    }

    fn sql_value(&mut self, value: &SqlValue) -> Result<Value, LixError> {
        Ok(match value {
            SqlValue::Null => Value::Null,
            SqlValue::Boolean(value) => Value::Boolean(*value),
            SqlValue::Number(raw, _) => raw
                .parse::<i64>()
                .map(Value::Integer)
                .or_else(|_| raw.parse::<f64>().map(Value::Real))
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("invalid numeric SQL literal '{raw}': {error}"),
                    )
                })?,
            SqlValue::Placeholder(name) => {
                let index = placeholder_index(name)?;
                self.max_placeholder = self.max_placeholder.max(index);
                self.params.get(index - 1).cloned().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("SQL expected at least {index} parameter(s)"),
                    )
                })?
            }
            SqlValue::SingleQuotedString(value)
            | SqlValue::DoubleQuotedString(value)
            | SqlValue::TripleSingleQuotedString(value)
            | SqlValue::TripleDoubleQuotedString(value)
            | SqlValue::EscapedStringLiteral(value)
            | SqlValue::UnicodeStringLiteral(value)
            | SqlValue::NationalStringLiteral(value) => Value::Text(value.clone()),
            SqlValue::HexStringLiteral(value) => Value::Text(value.clone()),
            _ => {
                return Err(LixError::new(
                    LixError::CODE_UNSUPPORTED_SQL,
                    "simple DML fast path only supports scalar SQL literals",
                ))
            }
        })
    }

    fn validate_count(&self) -> Result<(), LixError> {
        if self.params.len() == self.max_placeholder {
            return Ok(());
        }
        Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!(
                "SQL expected {} parameter(s), but {} parameter(s) were provided",
                self.max_placeholder,
                self.params.len()
            ),
        ))
    }
}

fn placeholder_index(id: &str) -> Result<usize, LixError> {
    id.strip_prefix('$')
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|index| *index > 0)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_PARSE_ERROR,
                format!("unsupported SQL parameter placeholder '{id}'"),
            )
        })
}

fn object_name_leaf(name: &ObjectName) -> Option<String> {
    name.0.last().and_then(|part| part.as_ident()).map(|ident| {
        if ident.quote_style.is_some() {
            ident.value.clone()
        } else {
            ident.value.to_ascii_lowercase()
        }
    })
}

fn ident_name(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

fn datafusion_to_lix_error(error: datafusion::error::DataFusionError) -> LixError {
    super::error::datafusion_error_to_lix_error(error)
}

fn is_fast_path_miss(error: &LixError) -> bool {
    error.code == LixError::CODE_UNSUPPORTED_SQL
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::json;

    use crate::binary_cas::{BlobBytesBatch, BlobHash};
    use crate::functions::{
        FunctionProvider, FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
    };
    use crate::live_state::{LiveStateScanRequest, MaterializedLiveStateRow};
    use crate::transaction::types::{TransactionWriteOutcome, TransactionWriteRow};

    use super::*;

    struct CapturingWriteContext {
        staged: Arc<Mutex<Vec<TransactionWriteRow>>>,
        schemas: Vec<JsonValue>,
    }

    #[async_trait]
    impl SqlWriteExecutionContext for CapturingWriteContext {
        fn active_version_id(&self) -> &str {
            "version-main"
        }

        fn functions(&self) -> FunctionProviderHandle {
            SharedFunctionProvider::new(
                Box::new(SystemFunctionProvider) as Box<dyn FunctionProvider + Send>
            )
        }

        fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
            Ok(self.schemas.clone())
        }

        async fn load_bytes_many(
            &mut self,
            _hashes: &[BlobHash],
        ) -> Result<BlobBytesBatch, LixError> {
            Ok(BlobBytesBatch::new(Vec::new()))
        }

        async fn scan_live_state(
            &mut self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(Vec::new())
        }

        async fn load_version_head(
            &mut self,
            _version_id: &str,
        ) -> Result<Option<String>, LixError> {
            Ok(Some("commit-main".to_string()))
        }

        async fn stage_write(
            &mut self,
            write: TransactionWrite,
        ) -> Result<TransactionWriteOutcome, LixError> {
            let TransactionWrite::Rows { rows, .. } = write else {
                return Ok(TransactionWriteOutcome { count: 0 });
            };
            let count = rows.len() as u64;
            self.staged.lock().expect("staged rows lock").extend(rows);
            Ok(TransactionWriteOutcome { count })
        }
    }

    #[tokio::test]
    async fn simple_entity_insert_stages_rows_without_datafusion_plan() {
        let staged = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = CapturingWriteContext {
            staged: Arc::clone(&staged),
            schemas: vec![test_schema()],
        };
        let statement =
            super::super::parse_statement("INSERT INTO test_schema (id, value) VALUES (?, ?)")
                .expect("statement should parse");

        let count = try_execute_simple_write(
            &mut ctx,
            &statement,
            &[
                Value::Text("entity-1".to_string()),
                Value::Text("hello".to_string()),
            ],
        )
        .await
        .expect("simple write should execute");

        assert_eq!(count, Some(1));
        let rows = staged.lock().expect("staged rows lock");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].schema_key, "test_schema");
        assert_eq!(rows[0].version_id, "version-main");
        assert_eq!(
            rows[0].snapshot.as_ref().unwrap().value(),
            &json!({"id": "entity-1", "value": "hello"})
        );
    }

    #[tokio::test]
    async fn insert_select_falls_back_to_datafusion() {
        let staged = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = CapturingWriteContext {
            staged: Arc::clone(&staged),
            schemas: vec![test_schema()],
        };
        let statement =
            super::super::parse_statement("INSERT INTO test_schema (id) SELECT 'entity-1'")
                .expect("statement should parse");

        let count = try_execute_simple_write(&mut ctx, &statement, &[])
            .await
            .expect("fallback should not error");

        assert_eq!(count, None);
        assert!(staged.lock().expect("staged rows lock").is_empty());
    }

    #[test]
    fn dynamic_assignment_target_is_fast_path_miss() {
        let assignment = Assignment {
            target: AssignmentTarget::ColumnName(ObjectName(vec![
                datafusion::sql::sqlparser::ast::ObjectNamePart::Function(
                    datafusion::sql::sqlparser::ast::ObjectNamePartFunction {
                        name: Ident::new("IDENTIFIER"),
                        args: vec![datafusion::sql::sqlparser::ast::FunctionArg::Unnamed(
                            datafusion::sql::sqlparser::ast::FunctionArgExpr::Expr(Expr::Value(
                                SqlValue::SingleQuotedString("value".to_string()).into(),
                            )),
                        )],
                    },
                ),
            ])),
            value: Expr::Value(SqlValue::Placeholder("$1".to_string()).into()),
        };
        let params = [Value::Text("second".to_string())];
        let mut decoder = ParamDecoder::new(&params);

        let error = simple_assignments(&[assignment], &mut decoder)
            .expect_err("dynamic assignment target should miss fast path");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
    }

    fn test_schema() -> JsonValue {
        json!({
            "x-lix-key": "test_schema",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "value": { "type": "string" }
            }
        })
    }
}
