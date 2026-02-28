use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;

use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ConflictTarget, Delete, Expr, FromTable, Ident,
    Insert, ObjectName, ObjectNamePart, OnConflictAction, OnInsert, Query, SelectItem, SetExpr,
    Statement, TableFactor, TableObject, TableWithJoins, Update, Value as AstValue, ValueWithSpan,
    Values,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::backend::SqlDialect;
use crate::cel::CelEvaluator;
use crate::engine::sql::planning::rewrite_engine::pipeline::query_engine::{
    rewrite_read_query, rewrite_read_query_with_backend,
};
use crate::engine::sql::planning::rewrite_engine::row_resolution::resolve_values_rows;
use crate::engine::sql::planning::rewrite_engine::{
    resolve_expr_cell_with_state, PlaceholderState, ResolvedCell,
};
use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::{errors, LixBackend, LixError, Value as EngineValue};

use super::target::{
    resolve_target_from_object_name, resolve_target_from_object_name_with_backend,
    EntityViewTarget, EntityViewVariant,
};

const LIX_STATE_VIEW_NAME: &str = "lix_state";
const LIX_STATE_BY_VERSION_VIEW_NAME: &str = "lix_state_by_version";

pub(crate) fn rewrite_insert(
    insert: Insert,
    params: &[EngineValue],
) -> Result<Option<Insert>, LixError> {
    let target = match &insert.table {
        TableObject::TableName(name) => resolve_target_from_object_name(name)?,
        _ => None,
    };
    let Some(target) = target else {
        return Ok(None);
    };
    if target.variant == EntityViewVariant::History {
        return Err(read_only_error(&target.view_name, "INSERT"));
    }
    let evaluator = CelEvaluator::new();
    let functions = SharedFunctionProvider::new(SystemFunctionProvider);
    Ok(Some(rewrite_insert_with_target(
        insert, params, &target, &evaluator, functions,
    )?))
}

pub(crate) async fn rewrite_insert_with_backend<P>(
    backend: &dyn LixBackend,
    insert: Insert,
    params: &[EngineValue],
    evaluator: &CelEvaluator,
    functions: SharedFunctionProvider<P>,
) -> Result<Option<Insert>, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let target = match &insert.table {
        TableObject::TableName(name) => {
            resolve_target_from_object_name_with_backend(backend, name).await?
        }
        _ => None,
    };
    let Some(target) = target else {
        return Ok(None);
    };
    if target.variant == EntityViewVariant::History {
        return Err(read_only_error(&target.view_name, "INSERT"));
    }
    Ok(Some(rewrite_insert_with_target(
        insert, params, &target, evaluator, functions,
    )?))
}

pub(crate) fn rewrite_update(
    update: Update,
    params: &[EngineValue],
) -> Result<Option<Update>, LixError> {
    let Some(target_name) = table_target_name(&update.table) else {
        return Ok(None);
    };
    let Some(target) = resolve_target_from_object_name(&target_name)? else {
        return Ok(None);
    };
    if target.variant == EntityViewVariant::History {
        return Err(read_only_error(&target.view_name, "UPDATE"));
    }
    let mut rewritten = rewrite_update_with_target(update, &target, SqlDialect::Sqlite, params)?;
    if let Some(selection) = rewritten.selection.take() {
        rewritten.selection = Some(rewrite_subquery_expressions(selection)?);
    }
    Ok(Some(rewritten))
}

pub(crate) async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    update: Update,
    params: &[EngineValue],
) -> Result<Option<Update>, LixError> {
    let Some(target_name) = table_target_name(&update.table) else {
        return Ok(None);
    };
    let Some(target) = resolve_target_from_object_name_with_backend(backend, &target_name).await?
    else {
        return Ok(None);
    };
    if target.variant == EntityViewVariant::History {
        return Err(read_only_error(&target.view_name, "UPDATE"));
    }
    let mut rewritten = rewrite_update_with_target(update, &target, backend.dialect(), params)?;
    if let Some(selection) = rewritten.selection.take() {
        rewritten.selection =
            Some(rewrite_subquery_expressions_with_backend(selection, backend).await?);
    }
    Ok(Some(rewritten))
}

pub(crate) fn rewrite_delete(delete: Delete) -> Result<Option<Delete>, LixError> {
    let Some(target_name) = delete_target_name(&delete) else {
        return Ok(None);
    };
    let Some(target) = resolve_target_from_object_name(&target_name)? else {
        return Ok(None);
    };
    if target.variant == EntityViewVariant::History {
        return Err(read_only_error(&target.view_name, "DELETE"));
    }
    let mut rewritten = rewrite_delete_with_target(delete, &target, SqlDialect::Sqlite)?;
    if let Some(selection) = rewritten.selection.take() {
        rewritten.selection = Some(rewrite_subquery_expressions(selection)?);
    }
    Ok(Some(rewritten))
}

pub(crate) async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    delete: Delete,
) -> Result<Option<Delete>, LixError> {
    let Some(target_name) = delete_target_name(&delete) else {
        return Ok(None);
    };
    let Some(target) = resolve_target_from_object_name_with_backend(backend, &target_name).await?
    else {
        return Ok(None);
    };
    if target.variant == EntityViewVariant::History {
        return Err(read_only_error(&target.view_name, "DELETE"));
    }
    let mut rewritten = rewrite_delete_with_target(delete, &target, backend.dialect())?;
    if let Some(selection) = rewritten.selection.take() {
        rewritten.selection =
            Some(rewrite_subquery_expressions_with_backend(selection, backend).await?);
    }
    Ok(Some(rewritten))
}

fn rewrite_insert_with_target<P>(
    mut insert: Insert,
    params: &[EngineValue],
    target: &EntityViewTarget,
    evaluator: &CelEvaluator,
    functions: SharedFunctionProvider<P>,
) -> Result<Insert, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let write_variant = mutation_variant(target);
    validate_and_strip_insert_on_conflict(&mut insert, &target.view_name)?;
    let is_default_values_insert = insert.columns.is_empty() && insert.source.is_none();
    if insert.columns.is_empty() && !is_default_values_insert {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("{} insert requires explicit columns", target.view_name),
        });
    }
    if is_default_values_insert {
        insert.source = Some(Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Values(Values {
                explicit_row: false,
                value_keyword: false,
                rows: vec![Vec::new()],
            })),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: Vec::new(),
            for_clause: None,
            settings: None,
            format_clause: None,
            pipe_operators: Vec::new(),
        }));
    }
    let source = insert.source.as_mut().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: format!("{} insert requires VALUES rows", target.view_name),
    })?;
    let SetExpr::Values(values) = source.body.as_mut() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("{} insert requires VALUES rows", target.view_name),
        });
    };

    let mut column_index = HashMap::new();
    for (index, column) in insert.columns.iter().enumerate() {
        column_index.insert(column.value.to_ascii_lowercase(), index);
    }
    if column_index.contains_key("schema_key") || column_index.contains_key("lixcol_schema_key") {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!(
                "{} insert cannot set schema_key; view schema is fixed",
                target.view_name
            ),
        });
    }

    let snapshot_index = find_first_column_index(
        &column_index,
        &["snapshot_content", "lixcol_snapshot_content"],
    );
    if snapshot_index.is_some() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!(
                "{} insert cannot set snapshot_content directly; set schema properties instead",
                target.view_name
            ),
        });
    }
    if target.variant == EntityViewVariant::Base
        && (column_index.contains_key("version_id")
            || column_index.contains_key("lixcol_version_id"))
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!(
                "{} insert cannot set version_id; version scope is resolved by the view/schema",
                target.view_name
            ),
        });
    }
    validate_insert_columns_known(target, &column_index)?;

    let entity_id_index =
        find_first_column_index(&column_index, &["entity_id", "lixcol_entity_id"]);
    let file_id_index = find_first_column_index(&column_index, &["file_id", "lixcol_file_id"]);
    let plugin_key_index =
        find_first_column_index(&column_index, &["plugin_key", "lixcol_plugin_key"]);
    let version_id_index =
        find_first_column_index(&column_index, &["version_id", "lixcol_version_id"]);
    let metadata_index = find_first_column_index(&column_index, &["metadata", "lixcol_metadata"]);
    let untracked_index =
        find_first_column_index(&column_index, &["untracked", "lixcol_untracked"]);
    let schema_version_index =
        find_first_column_index(&column_index, &["schema_version", "lixcol_schema_version"]);

    let property_index = build_property_index(&target.properties, &column_index);

    let resolved_rows = resolve_values_rows(&values.rows, params)?;
    let mut rewritten_rows = Vec::with_capacity(values.rows.len());
    for (row, resolved_row) in values.rows.iter().zip(resolved_rows.iter()) {
        if row.len() != insert.columns.len() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!(
                    "{} insert row length does not match column count",
                    target.view_name
                ),
            });
        }

        let snapshot_object = build_insert_snapshot_content_object(
            row,
            resolved_row,
            snapshot_index,
            &property_index,
            target,
            evaluator,
            functions.clone(),
        )?;
        let entity_id_expr = match entity_id_index {
            Some(index) => resolved_or_original_expr(row, resolved_row, index),
            None => derive_entity_id_expr(&snapshot_object, target)?,
        };
        let file_id_expr = match file_id_index {
            Some(index) => resolved_or_original_expr(row, resolved_row, index),
            None => match target.file_id_override.as_ref() {
                Some(value) => string_literal_expr(value),
                None => string_literal_expr("lix"),
            },
        };
        let plugin_key_expr = match plugin_key_index {
            Some(index) => resolved_or_original_expr(row, resolved_row, index),
            None => match target.plugin_key_override.as_ref() {
                Some(value) => string_literal_expr(value),
                None => string_literal_expr("lix"),
            },
        };
        let schema_version_expr = match schema_version_index {
            Some(index) => resolved_or_original_expr(row, resolved_row, index),
            None => string_literal_expr(&target.schema_version),
        };
        let metadata_expr = match metadata_index {
            Some(index) => resolved_or_original_expr(row, resolved_row, index),
            None => null_expr(),
        };
        let untracked_expr = match untracked_index {
            Some(index) => resolved_or_original_expr(row, resolved_row, index),
            None => boolean_expr(false),
        };
        let snapshot_content_expr =
            string_literal_expr(&JsonValue::Object(snapshot_object.clone()).to_string());
        let schema_key_expr = string_literal_expr(&target.schema_key);

        let row_exprs = match write_variant {
            EntityViewVariant::Base => vec![
                entity_id_expr,
                schema_key_expr,
                file_id_expr,
                plugin_key_expr,
                snapshot_content_expr,
                schema_version_expr,
                metadata_expr,
                untracked_expr,
            ],
            EntityViewVariant::ByVersion => {
                let version_expr = match version_id_index {
                    Some(index) => resolved_or_original_expr(row, resolved_row, index),
                    None => match target.version_id_override.as_ref() {
                        Some(value) => string_literal_expr(value),
                        None => {
                            return Err(LixError {
                                code: "LIX_ERROR_UNKNOWN".to_string(),
                                title: "Unknown error".to_string(),
                                description: format!(
                                "{} insert requires lixcol_version_id or schema default override",
                                target.view_name
                            ),
                            })
                        }
                    },
                };
                vec![
                    entity_id_expr,
                    schema_key_expr,
                    file_id_expr,
                    version_expr,
                    plugin_key_expr,
                    snapshot_content_expr,
                    schema_version_expr,
                    metadata_expr,
                    untracked_expr,
                ]
            }
            EntityViewVariant::History => {
                return Err(read_only_error(&target.view_name, "INSERT"));
            }
        };
        rewritten_rows.push(row_exprs);
    }

    insert.table = TableObject::TableName(ObjectName(vec![ObjectNamePart::Identifier(
        Ident::new(match write_variant {
            EntityViewVariant::Base => LIX_STATE_VIEW_NAME,
            EntityViewVariant::ByVersion => LIX_STATE_BY_VERSION_VIEW_NAME,
            EntityViewVariant::History => unreachable!(),
        }),
    )]));
    insert.columns = match write_variant {
        EntityViewVariant::Base => vec![
            Ident::new("entity_id"),
            Ident::new("schema_key"),
            Ident::new("file_id"),
            Ident::new("plugin_key"),
            Ident::new("snapshot_content"),
            Ident::new("schema_version"),
            Ident::new("metadata"),
            Ident::new("untracked"),
        ],
        EntityViewVariant::ByVersion => vec![
            Ident::new("entity_id"),
            Ident::new("schema_key"),
            Ident::new("file_id"),
            Ident::new("version_id"),
            Ident::new("plugin_key"),
            Ident::new("snapshot_content"),
            Ident::new("schema_version"),
            Ident::new("metadata"),
            Ident::new("untracked"),
        ],
        EntityViewVariant::History => Vec::new(),
    };
    values.rows = rewritten_rows;
    Ok(insert)
}

fn validate_and_strip_insert_on_conflict(
    insert: &mut Insert,
    view_name: &str,
) -> Result<(), LixError> {
    let Some(on_insert) = insert.on.take() else {
        return Ok(());
    };

    let OnInsert::OnConflict(on_conflict) = on_insert else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!(
                "{} insert only supports ON CONFLICT ... DO UPDATE",
                view_name
            ),
        });
    };

    match on_conflict.conflict_target {
        Some(ConflictTarget::Columns(columns)) if !columns.is_empty() => {}
        Some(_) => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!(
                    "{} insert ON CONFLICT only supports explicit column targets",
                    view_name
                ),
            })
        }
        None => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!(
                    "{} insert ON CONFLICT requires explicit conflict columns",
                    view_name
                ),
            })
        }
    }

    match on_conflict.action {
        OnConflictAction::DoUpdate(update) => {
            if update.selection.is_some() {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    title: "Unknown error".to_string(),
                    description: format!(
                        "{} insert ON CONFLICT DO UPDATE does not support WHERE",
                        view_name
                    ),
                });
            }
            Ok(())
        }
        OnConflictAction::DoNothing => {
            if view_name.eq_ignore_ascii_case("lix_stored_schema_by_version") {
                Ok(())
            } else {
                Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    title: "Unknown error".to_string(),
                    description: format!(
                        "{} insert ON CONFLICT DO NOTHING is not supported",
                        view_name
                    ),
                })
            }
        }
    }
}

fn resolved_or_original_expr(row: &[Expr], resolved_row: &[ResolvedCell], index: usize) -> Expr {
    match resolved_row.get(index).and_then(|cell| cell.value.as_ref()) {
        Some(EngineValue::Null) => Expr::Value(AstValue::Null.into()),
        Some(EngineValue::Boolean(value)) => Expr::Value(AstValue::Boolean(*value).into()),
        Some(EngineValue::Text(value)) => {
            Expr::Value(AstValue::SingleQuotedString(value.clone()).into())
        }
        Some(EngineValue::Integer(value)) => {
            Expr::Value(AstValue::Number(value.to_string(), false).into())
        }
        Some(EngineValue::Real(value)) => {
            Expr::Value(AstValue::Number(value.to_string(), false).into())
        }
        Some(EngineValue::Blob(value)) => Expr::Value(
            AstValue::SingleQuotedByteStringLiteral(String::from_utf8_lossy(value).to_string())
                .into(),
        ),
        None => row[index].clone(),
    }
}

fn rewrite_update_with_target(
    mut update: Update,
    target: &EntityViewTarget,
    dialect: SqlDialect,
    params: &[EngineValue],
) -> Result<Update, LixError> {
    let write_variant = mutation_variant(target);
    let derived_entity_id_predicate =
        derive_entity_id_predicate_from_where(update.selection.as_ref(), target);
    if !update.table.joins.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("{} update does not support JOIN targets", target.view_name),
        });
    }
    set_update_target_table(&mut update.table, write_variant)?;
    let property_names = property_name_set(target);
    let mut placeholder_state = PlaceholderState::new();
    let mut property_assignments = BTreeMap::new();
    let mut rewritten_assignments = Vec::with_capacity(update.assignments.len() + 1);
    for mut assignment in update.assignments {
        let AssignmentTarget::ColumnName(column_name) = &mut assignment.target else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!(
                    "{} update does not support tuple assignments",
                    target.view_name
                ),
            });
        };
        let Some(terminal) = column_name.0.last().and_then(ObjectNamePart::as_ident) else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!(
                    "strict rewrite violation: entity view update unknown assignment target in {}",
                    target.view_name
                ),
            });
        };
        let column = terminal.value.to_ascii_lowercase();

        let resolved =
            resolve_expr_cell_with_state(&assignment.value, params, &mut placeholder_state)?;
        if property_names.contains(&column) {
            let value = json_value_from_resolved_or_literal(
                Some(&resolved),
                Some(&assignment.value),
                &format!("{} update assignment '{}'", target.view_name, column),
            )?;
            let value = coerce_json_value_for_property(
                value,
                &column,
                target,
                &format!("{} update assignment '{}'", target.view_name, column),
            )?;
            property_assignments.insert(column, value);
            continue;
        }

        let Some(mapped) = rewrite_metadata_column_name(&column) else {
            return Err(unknown_entity_view_column_error(
                &target.view_name,
                "update assignment",
                terminal.value.as_str(),
                Some(&property_names),
            ));
        };
        if mapped == "schema_key" {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!(
                    "{} update cannot set schema_key; view schema is fixed",
                    target.view_name
                ),
            });
        }
        if mapped == "snapshot_content" {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!(
                    "{} update cannot set snapshot_content directly; set schema properties instead",
                    target.view_name
                ),
            });
        }
        if let Some(last) = column_name.0.last_mut() {
            *last = ObjectNamePart::Identifier(Ident::new(mapped));
        }
        rewritten_assignments.push(assignment);
    }
    if !property_assignments.is_empty() {
        rewritten_assignments.push(Assignment {
            target: AssignmentTarget::ColumnName(ObjectName(vec![ObjectNamePart::Identifier(
                Ident::new("snapshot_content"),
            )])),
            value: build_update_snapshot_patch_expr(&property_assignments, dialect)?,
        });
    }
    update.assignments = rewritten_assignments;
    let rewritten_selection = update
        .selection
        .take()
        .map(|expr| {
            rewrite_expression(
                expr,
                &property_names,
                dialect,
                &target.view_name,
                "update WHERE",
            )
        })
        .transpose()?;
    update.selection = Some(append_entity_scope_predicate(
        rewritten_selection,
        &target.schema_key,
        target,
        write_variant,
        derived_entity_id_predicate,
        "update",
    )?);
    Ok(update)
}

fn rewrite_delete_with_target(
    mut delete: Delete,
    target: &EntityViewTarget,
    dialect: SqlDialect,
) -> Result<Delete, LixError> {
    let write_variant = mutation_variant(target);
    let derived_entity_id_predicate =
        derive_entity_id_predicate_from_where(delete.selection.as_ref(), target);
    replace_delete_target_table(&mut delete, write_variant)?;
    let property_names = property_name_set(target);
    let rewritten_selection = delete
        .selection
        .take()
        .map(|expr| {
            rewrite_expression(
                expr,
                &property_names,
                dialect,
                &target.view_name,
                "delete WHERE",
            )
        })
        .transpose()?;
    delete.selection = Some(append_entity_scope_predicate(
        rewritten_selection,
        &target.schema_key,
        target,
        write_variant,
        derived_entity_id_predicate,
        "delete",
    )?);
    Ok(delete)
}

fn rewrite_expression(
    expr: Expr,
    property_names: &HashSet<String>,
    dialect: SqlDialect,
    view_name: &str,
    context: &str,
) -> Result<Expr, LixError> {
    Ok(match expr {
        Expr::Identifier(ident) => rewrite_column_reference_expr(
            ident.value,
            None,
            property_names,
            dialect,
            view_name,
            context,
        )?,
        Expr::CompoundIdentifier(idents) => {
            let Some(terminal) = idents.last() else {
                return Ok(Expr::CompoundIdentifier(idents));
            };
            let qualifier = if idents.len() > 1 {
                idents
                    .first()
                    .map(|ident| ident.value.clone())
                    .filter(|value| !value.is_empty())
            } else {
                None
            };
            rewrite_column_reference_expr(
                terminal.value.clone(),
                qualifier.as_deref(),
                property_names,
                dialect,
                view_name,
                context,
            )?
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(rewrite_expression(
                *left,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            op,
            right: Box::new(rewrite_expression(
                *right,
                property_names,
                dialect,
                view_name,
                context,
            )?),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op,
            expr: Box::new(rewrite_expression(
                *expr,
                property_names,
                dialect,
                view_name,
                context,
            )?),
        },
        Expr::Nested(inner) => Expr::Nested(Box::new(rewrite_expression(
            *inner,
            property_names,
            dialect,
            view_name,
            context,
        )?)),
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(rewrite_expression(
                *expr,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            list: list
                .into_iter()
                .map(|item| rewrite_expression(item, property_names, dialect, view_name, context))
                .collect::<Result<Vec<_>, _>>()?,
            negated,
        },
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(rewrite_expression(
                *expr,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            subquery,
            negated,
        },
        Expr::InUnnest {
            expr,
            array_expr,
            negated,
        } => Expr::InUnnest {
            expr: Box::new(rewrite_expression(
                *expr,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            array_expr: Box::new(rewrite_expression(
                *array_expr,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            negated,
        },
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(rewrite_expression(
                *expr,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            negated,
            low: Box::new(rewrite_expression(
                *low,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            high: Box::new(rewrite_expression(
                *high,
                property_names,
                dialect,
                view_name,
                context,
            )?),
        },
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => Expr::Like {
            negated,
            any,
            expr: Box::new(rewrite_expression(
                *expr,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            pattern: Box::new(rewrite_expression(
                *pattern,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            escape_char,
        },
        Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => Expr::ILike {
            negated,
            any,
            expr: Box::new(rewrite_expression(
                *expr,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            pattern: Box::new(rewrite_expression(
                *pattern,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            escape_char,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_expression(
            *inner,
            property_names,
            dialect,
            view_name,
            context,
        )?)),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rewrite_expression(
            *inner,
            property_names,
            dialect,
            view_name,
            context,
        )?)),
        Expr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => Expr::Cast {
            kind,
            expr: Box::new(rewrite_expression(
                *expr,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            data_type,
            format,
        },
        Expr::Function(function) => {
            let mut function = function;
            if let sqlparser::ast::FunctionArguments::List(list) = &mut function.args {
                for arg in &mut list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(expr),
                    ) = arg
                    {
                        *expr = rewrite_expression(
                            expr.clone(),
                            property_names,
                            dialect,
                            view_name,
                            context,
                        )?;
                    }
                }
            }
            Expr::Function(function)
        }
        Expr::AnyOp {
            left,
            compare_op,
            right,
            is_some,
        } => Expr::AnyOp {
            left: Box::new(rewrite_expression(
                *left,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            compare_op,
            right: Box::new(rewrite_expression(
                *right,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            is_some,
        },
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => Expr::AllOp {
            left: Box::new(rewrite_expression(
                *left,
                property_names,
                dialect,
                view_name,
                context,
            )?),
            compare_op,
            right: Box::new(rewrite_expression(
                *right,
                property_names,
                dialect,
                view_name,
                context,
            )?),
        },
        other => other,
    })
}

fn rewrite_subquery_expressions(expr: Expr) -> Result<Expr, LixError> {
    Ok(match expr {
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(rewrite_subquery_expressions(*left)?),
            op,
            right: Box::new(rewrite_subquery_expressions(*right)?),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op,
            expr: Box::new(rewrite_subquery_expressions(*expr)?),
        },
        Expr::Nested(inner) => Expr::Nested(Box::new(rewrite_subquery_expressions(*inner)?)),
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(rewrite_subquery_expressions(*expr)?),
            list: list
                .into_iter()
                .map(rewrite_subquery_expressions)
                .collect::<Result<Vec<_>, _>>()?,
            negated,
        },
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(rewrite_subquery_expressions(*expr)?),
            subquery: Box::new(rewrite_read_query(*subquery)?),
            negated,
        },
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(rewrite_subquery_expressions(*expr)?),
            negated,
            low: Box::new(rewrite_subquery_expressions(*low)?),
            high: Box::new(rewrite_subquery_expressions(*high)?),
        },
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => Expr::Like {
            negated,
            any,
            expr: Box::new(rewrite_subquery_expressions(*expr)?),
            pattern: Box::new(rewrite_subquery_expressions(*pattern)?),
            escape_char,
        },
        Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => Expr::ILike {
            negated,
            any,
            expr: Box::new(rewrite_subquery_expressions(*expr)?),
            pattern: Box::new(rewrite_subquery_expressions(*pattern)?),
            escape_char,
        },
        Expr::InUnnest {
            expr,
            array_expr,
            negated,
        } => Expr::InUnnest {
            expr: Box::new(rewrite_subquery_expressions(*expr)?),
            array_expr: Box::new(rewrite_subquery_expressions(*array_expr)?),
            negated,
        },
        Expr::AnyOp {
            left,
            compare_op,
            right,
            is_some,
        } => Expr::AnyOp {
            left: Box::new(rewrite_subquery_expressions(*left)?),
            compare_op,
            right: Box::new(rewrite_subquery_expressions(*right)?),
            is_some,
        },
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => Expr::AllOp {
            left: Box::new(rewrite_subquery_expressions(*left)?),
            compare_op,
            right: Box::new(rewrite_subquery_expressions(*right)?),
        },
        Expr::Exists { subquery, negated } => Expr::Exists {
            subquery: Box::new(rewrite_read_query(*subquery)?),
            negated,
        },
        Expr::Subquery(subquery) => Expr::Subquery(Box::new(rewrite_read_query(*subquery)?)),
        Expr::Function(function) => {
            let mut function = function;
            if let sqlparser::ast::FunctionArguments::List(list) = &mut function.args {
                for arg in &mut list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(expr),
                    ) = arg
                    {
                        *expr = rewrite_subquery_expressions(expr.clone())?;
                    }
                }
            }
            Expr::Function(function)
        }
        Expr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => Expr::Cast {
            kind,
            expr: Box::new(rewrite_subquery_expressions(*expr)?),
            data_type,
            format,
        },
        Expr::Case {
            case_token,
            end_token,
            operand,
            conditions,
            else_result,
        } => Expr::Case {
            case_token,
            end_token,
            operand: operand
                .map(|operand| rewrite_subquery_expressions(*operand))
                .transpose()?
                .map(Box::new),
            conditions: conditions
                .into_iter()
                .map(|condition| {
                    Ok(sqlparser::ast::CaseWhen {
                        condition: rewrite_subquery_expressions(condition.condition)?,
                        result: rewrite_subquery_expressions(condition.result)?,
                    })
                })
                .collect::<Result<Vec<_>, LixError>>()?,
            else_result: else_result
                .map(|value| rewrite_subquery_expressions(*value))
                .transpose()?
                .map(Box::new),
        },
        Expr::Tuple(items) => Expr::Tuple(
            items
                .into_iter()
                .map(rewrite_subquery_expressions)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        other => other,
    })
}

fn rewrite_subquery_expressions_with_backend<'a>(
    expr: Expr,
    backend: &'a dyn LixBackend,
) -> Pin<Box<dyn Future<Output = Result<Expr, LixError>> + 'a>> {
    Box::pin(async move {
        Ok(match expr {
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(rewrite_subquery_expressions_with_backend(*left, backend).await?),
                op,
                right: Box::new(rewrite_subquery_expressions_with_backend(*right, backend).await?),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(rewrite_subquery_expressions_with_backend(*expr, backend).await?),
            },
            Expr::Nested(inner) => Expr::Nested(Box::new(
                rewrite_subquery_expressions_with_backend(*inner, backend).await?,
            )),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut rewritten_list = Vec::with_capacity(list.len());
                for item in list {
                    rewritten_list
                        .push(rewrite_subquery_expressions_with_backend(item, backend).await?);
                }
                Expr::InList {
                    expr: Box::new(
                        rewrite_subquery_expressions_with_backend(*expr, backend).await?,
                    ),
                    list: rewritten_list,
                    negated,
                }
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => Expr::InSubquery {
                expr: Box::new(rewrite_subquery_expressions_with_backend(*expr, backend).await?),
                subquery: Box::new(rewrite_read_query_with_backend(backend, *subquery).await?),
                negated,
            },
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Expr::Between {
                expr: Box::new(rewrite_subquery_expressions_with_backend(*expr, backend).await?),
                negated,
                low: Box::new(rewrite_subquery_expressions_with_backend(*low, backend).await?),
                high: Box::new(rewrite_subquery_expressions_with_backend(*high, backend).await?),
            },
            Expr::Like {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => Expr::Like {
                negated,
                any,
                expr: Box::new(rewrite_subquery_expressions_with_backend(*expr, backend).await?),
                pattern: Box::new(
                    rewrite_subquery_expressions_with_backend(*pattern, backend).await?,
                ),
                escape_char,
            },
            Expr::ILike {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => Expr::ILike {
                negated,
                any,
                expr: Box::new(rewrite_subquery_expressions_with_backend(*expr, backend).await?),
                pattern: Box::new(
                    rewrite_subquery_expressions_with_backend(*pattern, backend).await?,
                ),
                escape_char,
            },
            Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => Expr::InUnnest {
                expr: Box::new(rewrite_subquery_expressions_with_backend(*expr, backend).await?),
                array_expr: Box::new(
                    rewrite_subquery_expressions_with_backend(*array_expr, backend).await?,
                ),
                negated,
            },
            Expr::AnyOp {
                left,
                compare_op,
                right,
                is_some,
            } => Expr::AnyOp {
                left: Box::new(rewrite_subquery_expressions_with_backend(*left, backend).await?),
                compare_op,
                right: Box::new(rewrite_subquery_expressions_with_backend(*right, backend).await?),
                is_some,
            },
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => Expr::AllOp {
                left: Box::new(rewrite_subquery_expressions_with_backend(*left, backend).await?),
                compare_op,
                right: Box::new(rewrite_subquery_expressions_with_backend(*right, backend).await?),
            },
            Expr::Exists { subquery, negated } => Expr::Exists {
                subquery: Box::new(rewrite_read_query_with_backend(backend, *subquery).await?),
                negated,
            },
            Expr::Subquery(subquery) => Expr::Subquery(Box::new(
                rewrite_read_query_with_backend(backend, *subquery).await?,
            )),
            Expr::Function(function) => {
                let mut function = function;
                if let sqlparser::ast::FunctionArguments::List(list) = &mut function.args {
                    for arg in &mut list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(expr),
                        ) = arg
                        {
                            *expr =
                                rewrite_subquery_expressions_with_backend(expr.clone(), backend)
                                    .await?;
                        }
                    }
                }
                Expr::Function(function)
            }
            Expr::Cast {
                kind,
                expr,
                data_type,
                format,
            } => Expr::Cast {
                kind,
                expr: Box::new(rewrite_subquery_expressions_with_backend(*expr, backend).await?),
                data_type,
                format,
            },
            Expr::Case {
                case_token,
                end_token,
                operand,
                conditions,
                else_result,
            } => {
                let operand = match operand {
                    Some(value) => Some(Box::new(
                        rewrite_subquery_expressions_with_backend(*value, backend).await?,
                    )),
                    None => None,
                };
                let mut rewritten_conditions = Vec::with_capacity(conditions.len());
                for condition in conditions {
                    rewritten_conditions.push(sqlparser::ast::CaseWhen {
                        condition: rewrite_subquery_expressions_with_backend(
                            condition.condition,
                            backend,
                        )
                        .await?,
                        result: rewrite_subquery_expressions_with_backend(
                            condition.result,
                            backend,
                        )
                        .await?,
                    });
                }
                let else_result = match else_result {
                    Some(value) => Some(Box::new(
                        rewrite_subquery_expressions_with_backend(*value, backend).await?,
                    )),
                    None => None,
                };
                Expr::Case {
                    case_token,
                    end_token,
                    operand,
                    conditions: rewritten_conditions,
                    else_result,
                }
            }
            Expr::Tuple(items) => {
                let mut rewritten_items = Vec::with_capacity(items.len());
                for item in items {
                    rewritten_items
                        .push(rewrite_subquery_expressions_with_backend(item, backend).await?);
                }
                Expr::Tuple(rewritten_items)
            }
            other => other,
        })
    })
}

fn rewrite_column_reference_expr(
    column_name: String,
    qualifier: Option<&str>,
    property_names: &HashSet<String>,
    dialect: SqlDialect,
    view_name: &str,
    context: &str,
) -> Result<Expr, LixError> {
    let lower = column_name.to_ascii_lowercase();
    if property_names.contains(&lower) {
        let expression_sql = match dialect {
            SqlDialect::Sqlite => format!(
                "json_extract({snapshot}, '$.\"{property}\"')",
                snapshot = snapshot_column_reference_sql(qualifier),
                property = column_name.replace('\'', "''")
            ),
            SqlDialect::Postgres => format!(
                "jsonb_extract_path_text(({snapshot})::jsonb, '{property}')",
                snapshot = snapshot_column_reference_sql(qualifier),
                property = column_name.replace('\'', "''")
            ),
        };
        return parse_expression_from_sql(&expression_sql);
    }
    if let Some(mapped) = rewrite_metadata_column_name(&lower) {
        if let Some(qualifier) = qualifier {
            return Ok(Expr::CompoundIdentifier(vec![
                Ident::new(qualifier),
                Ident::new(mapped),
            ]));
        }
        return Ok(Expr::Identifier(Ident::new(mapped)));
    }
    Err(unknown_entity_view_column_error(
        view_name,
        context,
        qualifier
            .map(|prefix| format!("{prefix}.{column_name}"))
            .unwrap_or(column_name),
        Some(property_names),
    ))
}

fn snapshot_column_reference_sql(qualifier: Option<&str>) -> String {
    match qualifier {
        Some(prefix) => format!("{prefix}.snapshot_content"),
        None => "snapshot_content".to_string(),
    }
}

fn append_schema_predicate(selection: Option<Expr>, schema_key: &str) -> Expr {
    let predicate = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("schema_key"))),
        op: BinaryOperator::Eq,
        right: Box::new(string_literal_expr(schema_key)),
    };
    match selection {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(existing),
            op: BinaryOperator::And,
            right: Box::new(predicate),
        },
        None => predicate,
    }
}

fn append_entity_scope_predicate(
    selection: Option<Expr>,
    schema_key: &str,
    target: &EntityViewTarget,
    write_variant: EntityViewVariant,
    derived_entity_id_predicate: Option<Expr>,
    operation: &str,
) -> Result<Expr, LixError> {
    let scoped = append_schema_predicate(selection, schema_key);
    let scoped = append_optional_and_predicate(scoped, derived_entity_id_predicate);
    let base_override_version_id = (target.variant == EntityViewVariant::Base
        && write_variant == EntityViewVariant::ByVersion)
        .then(|| target.version_id_override.as_deref())
        .flatten();
    if let Some(version_id) = base_override_version_id {
        return Ok(Expr::BinaryOp {
            left: Box::new(scoped),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("version_id"))),
                op: BinaryOperator::Eq,
                right: Box::new(string_literal_expr(version_id)),
            }),
        });
    }

    if write_variant == EntityViewVariant::ByVersion
        && !contains_column_reference(&scoped, "version_id")
    {
        let Some(version_id) = target.version_id_override.as_deref() else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!(
                    "{} {} requires explicit lixcol_version_id or schema default override",
                    target.view_name, operation
                ),
            });
        };
        return Ok(Expr::BinaryOp {
            left: Box::new(scoped),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("version_id"))),
                op: BinaryOperator::Eq,
                right: Box::new(string_literal_expr(version_id)),
            }),
        });
    }

    Ok(scoped)
}

fn append_optional_and_predicate(base: Expr, extra: Option<Expr>) -> Expr {
    match extra {
        Some(predicate) => Expr::BinaryOp {
            left: Box::new(base),
            op: BinaryOperator::And,
            right: Box::new(predicate),
        },
        None => base,
    }
}

fn derive_entity_id_predicate_from_where(
    selection: Option<&Expr>,
    target: &EntityViewTarget,
) -> Option<Expr> {
    let parts = collect_primary_key_equality_parts(selection?, target)?;
    let entity_id_expression = build_entity_id_expression_from_parts(&parts)?;
    Some(Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("entity_id"))),
        op: BinaryOperator::Eq,
        right: Box::new(entity_id_expression),
    })
}

fn collect_primary_key_equality_parts(
    selection: &Expr,
    target: &EntityViewTarget,
) -> Option<Vec<Expr>> {
    if target.primary_key_properties.is_empty() {
        return None;
    }
    let mut matched: HashMap<String, Expr> = HashMap::new();
    collect_primary_key_equalities_recursive(selection, target, &mut matched);

    let mut ordered = Vec::with_capacity(target.primary_key_properties.len());
    for property in &target.primary_key_properties {
        let key = property.to_ascii_lowercase();
        let value = matched.get(&key)?;
        ordered.push(value.clone());
    }
    Some(ordered)
}

fn collect_primary_key_equalities_recursive(
    expr: &Expr,
    target: &EntityViewTarget,
    matched: &mut HashMap<String, Expr>,
) {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if *op == BinaryOperator::And {
                collect_primary_key_equalities_recursive(left, target, matched);
                collect_primary_key_equalities_recursive(right, target, matched);
                return;
            }
            if *op != BinaryOperator::Eq {
                return;
            }

            if let Some(property) = expression_property_name(left) {
                let key = property.to_ascii_lowercase();
                if is_primary_key_property(target, &key)
                    && !matched.contains_key(&key)
                    && is_supported_entity_id_component_expression(right)
                {
                    matched.insert(key, strip_nested_expr((**right).clone()));
                    return;
                }
            }
            if let Some(property) = expression_property_name(right) {
                let key = property.to_ascii_lowercase();
                if is_primary_key_property(target, &key)
                    && !matched.contains_key(&key)
                    && is_supported_entity_id_component_expression(left)
                {
                    matched.insert(key, strip_nested_expr((**left).clone()));
                }
            }
        }
        Expr::Nested(inner) => collect_primary_key_equalities_recursive(inner, target, matched),
        _ => {}
    }
}

fn is_primary_key_property(target: &EntityViewTarget, property: &str) -> bool {
    target
        .primary_key_properties
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(property))
}

fn expression_property_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.clone()),
        Expr::Nested(inner) => expression_property_name(inner),
        _ => None,
    }
}

fn is_supported_entity_id_component_expression(expr: &Expr) -> bool {
    matches!(strip_nested_expr(expr.clone()), Expr::Value(_))
}

fn strip_nested_expr(expr: Expr) -> Expr {
    let mut current = expr;
    while let Expr::Nested(inner) = current {
        current = *inner;
    }
    current
}

fn build_entity_id_expression_from_parts(parts: &[Expr]) -> Option<Expr> {
    if parts.is_empty() {
        return None;
    }
    if parts.len() == 1 {
        return Some(parts[0].clone());
    }

    let mut iter = parts.iter();
    let mut combined = iter.next()?.clone();
    for part in iter {
        combined = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(combined),
                op: BinaryOperator::StringConcat,
                right: Box::new(string_literal_expr("~")),
            }),
            op: BinaryOperator::StringConcat,
            right: Box::new(part.clone()),
        };
    }
    Some(combined)
}

fn contains_column_reference(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(column),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case(column))
            .unwrap_or(false),
        Expr::BinaryOp { left, right, .. } => {
            contains_column_reference(left, column) || contains_column_reference(right, column)
        }
        Expr::UnaryOp { expr, .. } => contains_column_reference(expr, column),
        Expr::Nested(inner) => contains_column_reference(inner, column),
        Expr::InList { expr, list, .. } => {
            contains_column_reference(expr, column)
                || list
                    .iter()
                    .any(|item| contains_column_reference(item, column))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            contains_column_reference(expr, column)
                || contains_column_reference(low, column)
                || contains_column_reference(high, column)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            contains_column_reference(expr, column) || contains_column_reference(pattern, column)
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => contains_column_reference(inner, column),
        Expr::Cast { expr, .. } => contains_column_reference(expr, column),
        Expr::Function(function) => match &function.args {
            sqlparser::ast::FunctionArguments::List(list) => {
                list.args.iter().any(|arg| match arg {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(expr),
                    ) => contains_column_reference(expr, column),
                    sqlparser::ast::FunctionArg::Named { arg, .. }
                    | sqlparser::ast::FunctionArg::ExprNamed { arg, .. } => match arg {
                        sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                            contains_column_reference(expr, column)
                        }
                        _ => false,
                    },
                    _ => false,
                })
            }
            _ => false,
        },
        Expr::InSubquery { expr, .. } => contains_column_reference(expr, column),
        _ => false,
    }
}

fn build_insert_snapshot_content_object<P>(
    row: &[Expr],
    resolved_row: &[ResolvedCell],
    snapshot_index: Option<usize>,
    property_index: &HashMap<String, usize>,
    target: &EntityViewTarget,
    evaluator: &CelEvaluator,
    functions: SharedFunctionProvider<P>,
) -> Result<JsonMap<String, JsonValue>, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    if let Some(index) = snapshot_index {
        let snapshot = json_value_from_resolved_or_literal(
            resolved_row.get(index),
            row.get(index),
            &format!("{} insert snapshot_content", target.view_name),
        )?;
        let JsonValue::Object(object) = snapshot else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!(
                    "{} insert snapshot_content must be a JSON object",
                    target.view_name
                ),
            });
        };
        return Ok(object);
    }

    let mut object = JsonMap::new();
    for property in &target.properties {
        let Some(index) = property_index.get(&property.to_ascii_lowercase()) else {
            continue;
        };
        let value = json_value_from_resolved_or_literal(
            resolved_row.get(*index),
            row.get(*index),
            &format!("{} insert property '{}'", target.view_name, property),
        )?;
        let value = coerce_json_value_for_property(
            value,
            property,
            target,
            &format!("{} insert property '{}'", target.view_name, property),
        )?;
        object.insert(property.clone(), value);
    }
    apply_schema_defaults_to_snapshot(&mut object, target, evaluator, functions)?;
    Ok(object)
}

fn apply_schema_defaults_to_snapshot<P>(
    snapshot: &mut JsonMap<String, JsonValue>,
    target: &EntityViewTarget,
    evaluator: &CelEvaluator,
    functions: SharedFunctionProvider<P>,
) -> Result<(), LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let Some(properties) = target
        .schema
        .get("properties")
        .and_then(JsonValue::as_object)
    else {
        return Ok(());
    };

    for property in &target.properties {
        if snapshot.contains_key(property) {
            continue;
        }
        let value = evaluate_default_property_value(
            snapshot,
            property,
            properties.get(property),
            target,
            evaluator,
            functions.clone(),
        )?;
        snapshot.insert(property.clone(), value.unwrap_or(JsonValue::Null));
    }
    Ok(())
}

fn coerce_json_value_for_property(
    value: JsonValue,
    property: &str,
    target: &EntityViewTarget,
    context: &str,
) -> Result<JsonValue, LixError> {
    let value = if property_expects_boolean(target, property) {
        match value {
            JsonValue::Bool(_) => value,
            JsonValue::Number(number) => match number.as_i64() {
                Some(0) => JsonValue::Bool(false),
                Some(1) => JsonValue::Bool(true),
                _ => JsonValue::Number(number),
            },
            JsonValue::String(text) => {
                let normalized = text.trim().to_ascii_lowercase();
                match normalized.as_str() {
                    "true" | "1" => JsonValue::Bool(true),
                    "false" | "0" => JsonValue::Bool(false),
                    _ => JsonValue::String(text),
                }
            }
            other => other,
        }
    } else {
        value
    };

    enforce_property_type_constraints(&value, property, target, context)?;
    Ok(value)
}

fn property_expects_boolean(target: &EntityViewTarget, property: &str) -> bool {
    property_schema(target, property)
        .map(|schema| schema_allows_type(schema, "boolean"))
        .unwrap_or(false)
}

fn enforce_property_type_constraints(
    value: &JsonValue,
    property: &str,
    target: &EntityViewTarget,
    context: &str,
) -> Result<(), LixError> {
    let Some(schema) = property_schema(target, property) else {
        return Ok(());
    };
    let expected = expected_json_schema_types(schema);
    if expected.is_empty() {
        return Ok(());
    }
    if schema_accepts_json_value(schema, value) {
        return Ok(());
    }
    let hint = if matches!(value, JsonValue::String(_))
        && expected.iter().any(|ty| *ty == "object" || *ty == "array")
    {
        " Wrap JSON object/array input with lix_json(...)."
    } else {
        ""
    };
    Err(LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: format!(
            "{context} expects one of [{}], got {}.{}",
            expected.join(", "),
            json_value_type(value),
            hint
        )
        .trim_end_matches('.')
        .to_string(),
    })
}

fn expected_json_schema_types(schema: &JsonValue) -> Vec<&'static str> {
    let mut types = Vec::new();
    for candidate in [
        "null", "boolean", "integer", "number", "string", "object", "array",
    ] {
        if schema_allows_type(schema, candidate) {
            types.push(candidate);
        }
    }
    types
}

fn schema_accepts_json_value(schema: &JsonValue, value: &JsonValue) -> bool {
    match value {
        JsonValue::Null => schema_allows_type(schema, "null"),
        JsonValue::Bool(_) => schema_allows_type(schema, "boolean"),
        JsonValue::Number(number) => {
            if number.is_i64() || number.is_u64() {
                schema_allows_type(schema, "integer") || schema_allows_type(schema, "number")
            } else {
                schema_allows_type(schema, "number")
            }
        }
        JsonValue::String(_) => schema_allows_type(schema, "string"),
        JsonValue::Array(_) => schema_allows_type(schema, "array"),
        JsonValue::Object(_) => schema_allows_type(schema, "object"),
    }
}

fn json_value_type(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(number) => {
            if number.is_i64() || number.is_u64() {
                "integer"
            } else {
                "number"
            }
        }
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn property_schema<'a>(target: &'a EntityViewTarget, property: &str) -> Option<&'a JsonValue> {
    target
        .schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .and_then(|properties| properties.get(property))
}

fn schema_allows_type(schema: &JsonValue, expected: &str) -> bool {
    if schema
        .get("type")
        .and_then(JsonValue::as_str)
        .map(|value| value.eq_ignore_ascii_case(expected))
        .unwrap_or(false)
    {
        return true;
    }

    if schema
        .get("type")
        .and_then(JsonValue::as_array)
        .map(|values| {
            values.iter().any(|value| {
                value
                    .as_str()
                    .map(|value| value.eq_ignore_ascii_case(expected))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
    {
        return true;
    }

    for key in ["anyOf", "oneOf", "allOf"] {
        if schema
            .get(key)
            .and_then(JsonValue::as_array)
            .map(|variants| {
                variants
                    .iter()
                    .any(|variant| schema_allows_type(variant, expected))
            })
            .unwrap_or(false)
        {
            return true;
        }
    }

    false
}

fn evaluate_default_property_value<P>(
    snapshot: &JsonMap<String, JsonValue>,
    property: &str,
    property_schema: Option<&JsonValue>,
    target: &EntityViewTarget,
    evaluator: &CelEvaluator,
    functions: SharedFunctionProvider<P>,
) -> Result<Option<JsonValue>, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let Some(property_schema) = property_schema else {
        return Ok(None);
    };
    if let Some(expression) = property_schema
        .get("x-lix-default")
        .and_then(JsonValue::as_str)
    {
        let context = snapshot.clone();
        let value = evaluator
            .evaluate_with_functions(expression, &context, functions)
            .map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!(
                    "{} insert failed to evaluate x-lix-default for '{}.{}': {}",
                    target.view_name, target.schema_key, property, err.description
                ),
            })?;
        return Ok(Some(value));
    }
    Ok(property_schema.get("default").cloned())
}

fn build_update_snapshot_patch_expr(
    properties: &BTreeMap<String, JsonValue>,
    dialect: SqlDialect,
) -> Result<Expr, LixError> {
    let sql = match dialect {
        SqlDialect::Sqlite => {
            let mut args = vec!["COALESCE(snapshot_content, '{}')".to_string()];
            for (property, value) in properties {
                args.push(format!(
                    "'$.\"{property}\"'",
                    property = escape_single_quoted_literal(property)
                ));
                args.push(sqlite_json_value_expr(value));
            }
            format!("json_set({})", args.join(", "))
        }
        SqlDialect::Postgres => {
            let mut current = "CAST(COALESCE(snapshot_content, '{}') AS JSONB)".to_string();
            for (property, value) in properties {
                let json = escape_single_quoted_literal(&value.to_string());
                current = format!(
                    "jsonb_set({current}, '{{{property}}}', CAST('{json}' AS JSONB), true)",
                    property = escape_single_quoted_literal(property),
                );
            }
            format!("CAST({current} AS TEXT)")
        }
    };
    parse_expression_from_sql(&sql)
}

fn sqlite_json_value_expr(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(value) => {
            if *value {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        JsonValue::Number(number) => number.to_string(),
        JsonValue::String(text) => format!("'{}'", escape_single_quoted_literal(text)),
        JsonValue::Array(_) | JsonValue::Object(_) => format!(
            "json('{}')",
            escape_single_quoted_literal(&value.to_string())
        ),
    }
}

fn json_value_from_resolved_or_literal(
    cell: Option<&ResolvedCell>,
    expr: Option<&Expr>,
    context: &str,
) -> Result<JsonValue, LixError> {
    if let Some(expr) = expr {
        if let Some(arg_expr) = lix_json_argument_expr(expr)? {
            return json_value_from_lix_json_argument(cell, arg_expr, context);
        }
    }
    if let Some(cell) = cell {
        if let Some(value) = &cell.value {
            return json_value_from_engine_value(value, context);
        }
    }
    json_value_from_literal_expr(expr, context)
}

fn json_value_from_lix_json_argument(
    cell: Option<&ResolvedCell>,
    arg_expr: &Expr,
    context: &str,
) -> Result<JsonValue, LixError> {
    if let Some(cell) = cell {
        if cell.placeholder_index.is_none() {
            if let Ok(raw) = json_text_input_from_literal_expr(arg_expr, context) {
                return parse_json_value(&raw, context);
            }
        }
        if let Some(value) = &cell.value {
            let raw = json_text_input_from_engine_value(value, context)?;
            return parse_json_value(&raw, context);
        }
    }
    let raw = json_text_input_from_literal_expr(arg_expr, context)?;
    parse_json_value(&raw, context)
}

fn parse_json_value(raw: &str, context: &str) -> Result<JsonValue, LixError> {
    serde_json::from_str(raw).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: format!("{context} lix_json() argument must be valid JSON ({error})"),
    })
}

fn json_text_input_from_engine_value(
    value: &EngineValue,
    context: &str,
) -> Result<String, LixError> {
    match value {
        EngineValue::Null => Ok("null".to_string()),
        EngineValue::Boolean(value) => Ok(value.to_string()),
        EngineValue::Integer(value) => Ok(value.to_string()),
        EngineValue::Real(value) => {
            if value.is_finite() {
                Ok(value.to_string())
            } else {
                Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    title: "Unknown error".to_string(),
                    description: format!("{context} contains non-finite numeric value"),
                })
            }
        }
        EngineValue::Text(value) => Ok(value.clone()),
        EngineValue::Blob(_) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("{context} does not support blob values"),
        }),
    }
}

fn json_text_input_from_literal_expr(expr: &Expr, context: &str) -> Result<String, LixError> {
    let Expr::Value(ValueWithSpan { value, .. }) = expr else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("{context} requires literal or placeholder values"),
        });
    };
    match value {
        AstValue::Null => Ok("null".to_string()),
        AstValue::Boolean(value) => Ok(value.to_string()),
        AstValue::Number(value, _) => Ok(value.clone()),
        AstValue::SingleQuotedString(value)
        | AstValue::DoubleQuotedString(value)
        | AstValue::TripleSingleQuotedString(value)
        | AstValue::TripleDoubleQuotedString(value)
        | AstValue::EscapedStringLiteral(value)
        | AstValue::UnicodeStringLiteral(value)
        | AstValue::NationalStringLiteral(value)
        | AstValue::HexStringLiteral(value)
        | AstValue::SingleQuotedRawStringLiteral(value)
        | AstValue::DoubleQuotedRawStringLiteral(value)
        | AstValue::TripleSingleQuotedRawStringLiteral(value)
        | AstValue::TripleDoubleQuotedRawStringLiteral(value)
        | AstValue::SingleQuotedByteStringLiteral(value)
        | AstValue::DoubleQuotedByteStringLiteral(value)
        | AstValue::TripleSingleQuotedByteStringLiteral(value)
        | AstValue::TripleDoubleQuotedByteStringLiteral(value) => Ok(value.clone()),
        AstValue::DollarQuotedString(value) => Ok(value.value.clone()),
        AstValue::Placeholder(token) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("{context} unresolved placeholder '{token}'"),
        }),
    }
}

fn lix_json_argument_expr<'a>(expr: &'a Expr) -> Result<Option<&'a Expr>, LixError> {
    let Expr::Function(function) = expr else {
        return Ok(None);
    };
    if !function_name_matches(&function.name, "lix_json") {
        return Ok(None);
    }
    let args = match &function.args {
        sqlparser::ast::FunctionArguments::List(list) => {
            if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    title: "Unknown error".to_string(),
                    description: "lix_json() does not support DISTINCT/ALL/clauses".to_string(),
                });
            }
            &list.args
        }
        _ => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: "lix_json() requires a regular argument list".to_string(),
            });
        }
    };
    if args.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "lix_json() requires exactly 1 argument".to_string(),
        });
    }
    let arg = match &args[0] {
        sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) => expr,
        sqlparser::ast::FunctionArg::Unnamed(_) => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: "lix_json() arguments must be SQL expressions".to_string(),
            });
        }
        _ => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: "lix_json() does not support named arguments".to_string(),
            });
        }
    };
    Ok(Some(arg))
}

fn function_name_matches(name: &ObjectName, expected: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(expected))
        .unwrap_or(false)
}

fn json_value_from_engine_value(value: &EngineValue, context: &str) -> Result<JsonValue, LixError> {
    match value {
        EngineValue::Null => Ok(JsonValue::Null),
        EngineValue::Boolean(value) => Ok(JsonValue::Bool(*value)),
        EngineValue::Integer(value) => Ok(JsonValue::Number((*value).into())),
        EngineValue::Real(value) => JsonNumber::from_f64(*value)
            .map(JsonValue::Number)
            .ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!("{context} contains non-finite numeric value"),
            }),
        EngineValue::Text(value) => Ok(JsonValue::String(value.clone())),
        EngineValue::Blob(_) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("{context} does not support blob values"),
        }),
    }
}

fn json_value_from_literal_expr(expr: Option<&Expr>, context: &str) -> Result<JsonValue, LixError> {
    let expr = expr.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: format!("{context} is missing a value"),
    })?;
    let Expr::Value(ValueWithSpan { value, .. }) = expr else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("{context} requires literal or placeholder values"),
        });
    };

    match value {
        AstValue::Null => Ok(JsonValue::Null),
        AstValue::Boolean(value) => Ok(JsonValue::Bool(*value)),
        AstValue::Number(value, _) => {
            if let Ok(int) = value.parse::<i64>() {
                Ok(JsonValue::Number(int.into()))
            } else if let Ok(real) = value.parse::<f64>() {
                JsonNumber::from_f64(real)
                    .map(JsonValue::Number)
                    .ok_or_else(|| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        title: "Unknown error".to_string(),
                        description: format!("{context} contains non-finite numeric value"),
                    })
            } else {
                Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    title: "Unknown error".to_string(),
                    description: format!(
                        "{context} contains unsupported numeric literal '{value}'"
                    ),
                })
            }
        }
        AstValue::SingleQuotedString(value)
        | AstValue::DoubleQuotedString(value)
        | AstValue::TripleSingleQuotedString(value)
        | AstValue::TripleDoubleQuotedString(value)
        | AstValue::EscapedStringLiteral(value)
        | AstValue::UnicodeStringLiteral(value)
        | AstValue::NationalStringLiteral(value)
        | AstValue::HexStringLiteral(value)
        | AstValue::SingleQuotedRawStringLiteral(value)
        | AstValue::DoubleQuotedRawStringLiteral(value)
        | AstValue::TripleSingleQuotedRawStringLiteral(value)
        | AstValue::TripleDoubleQuotedRawStringLiteral(value)
        | AstValue::SingleQuotedByteStringLiteral(value)
        | AstValue::DoubleQuotedByteStringLiteral(value)
        | AstValue::TripleSingleQuotedByteStringLiteral(value)
        | AstValue::TripleDoubleQuotedByteStringLiteral(value) => {
            Ok(JsonValue::String(value.clone()))
        }
        AstValue::DollarQuotedString(value) => Ok(JsonValue::String(value.value.clone())),
        AstValue::Placeholder(token) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("{context} unresolved placeholder '{token}'"),
        }),
    }
}

fn escape_single_quoted_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn derive_entity_id_expr(
    snapshot: &JsonMap<String, JsonValue>,
    target: &EntityViewTarget,
) -> Result<Expr, LixError> {
    if target.primary_key_fields.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!(
                "{} insert requires entity_id (schema has no x-lix-primary-key)",
                target.view_name
            ),
        });
    }
    let mut parts = Vec::with_capacity(target.primary_key_fields.len());
    for field in &target.primary_key_fields {
        let Some(value) = json_pointer_get_from_snapshot(snapshot, &field.path) else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!(
                    "{} insert requires entity_id or all primary-key properties ({})",
                    target.view_name,
                    target
                        .primary_key_fields
                        .iter()
                        .map(|field| field.pointer.clone())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        };
        parts.push(entity_id_component_from_json_value(
            value,
            &field.pointer,
            target,
        )?);
    }
    let entity_id = if parts.len() == 1 {
        parts.remove(0)
    } else {
        parts.join("~")
    };
    Ok(string_literal_expr(&entity_id))
}

fn entity_id_component_from_json_value(
    value: &JsonValue,
    key_ref: &str,
    target: &EntityViewTarget,
) -> Result<String, LixError> {
    match value {
        JsonValue::Null => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!(
                "{} insert cannot derive entity_id from null primary-key property '{}'",
                target.view_name, key_ref
            ),
        }),
        JsonValue::String(text) => Ok(text.clone()),
        JsonValue::Bool(flag) => Ok(flag.to_string()),
        JsonValue::Number(number) => Ok(number.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Ok(value.to_string()),
    }
}

fn json_pointer_get_from_snapshot<'a>(
    snapshot: &'a JsonMap<String, JsonValue>,
    path: &[String],
) -> Option<&'a JsonValue> {
    let mut current: Option<&JsonValue> = None;
    for (index, segment) in path.iter().enumerate() {
        current = if index == 0 {
            snapshot.get(segment)
        } else {
            current?.as_object()?.get(segment)
        };
    }
    current
}

fn build_property_index(
    properties: &[String],
    columns: &HashMap<String, usize>,
) -> HashMap<String, usize> {
    let property_set = properties
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let mut out = HashMap::new();
    for (name, index) in columns {
        if property_set.contains(name) {
            out.insert(name.clone(), *index);
        }
    }
    out
}

fn validate_insert_columns_known(
    target: &EntityViewTarget,
    columns: &HashMap<String, usize>,
) -> Result<(), LixError> {
    let property_set = target
        .properties
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();

    for column in columns.keys() {
        if property_set.contains(column) {
            continue;
        }
        if is_known_insert_metadata_column(column) {
            continue;
        }
        return Err(unknown_entity_view_column_error(
            &target.view_name,
            "insert column",
            column.as_str(),
            Some(&property_set),
        ));
    }
    Ok(())
}

fn is_known_insert_metadata_column(column: &str) -> bool {
    matches!(
        column,
        "entity_id"
            | "lixcol_entity_id"
            | "file_id"
            | "lixcol_file_id"
            | "plugin_key"
            | "lixcol_plugin_key"
            | "version_id"
            | "lixcol_version_id"
            | "metadata"
            | "lixcol_metadata"
            | "untracked"
            | "lixcol_untracked"
            | "schema_version"
            | "lixcol_schema_version"
            | "schema_key"
            | "lixcol_schema_key"
            | "snapshot_content"
            | "lixcol_snapshot_content"
    )
}

fn unknown_entity_view_column_error(
    view_name: &str,
    context: &str,
    column: impl AsRef<str>,
    property_names: Option<&HashSet<String>>,
) -> LixError {
    let mut allowed = vec![
        "entity_id",
        "lixcol_entity_id",
        "schema_key",
        "lixcol_schema_key",
        "file_id",
        "lixcol_file_id",
        "version_id",
        "lixcol_version_id",
        "plugin_key",
        "lixcol_plugin_key",
        "snapshot_content",
        "lixcol_snapshot_content",
        "schema_version",
        "lixcol_schema_version",
        "metadata",
        "lixcol_metadata",
        "untracked",
        "lixcol_untracked",
        "created_at",
        "lixcol_created_at",
        "updated_at",
        "lixcol_updated_at",
        "change_id",
        "lixcol_change_id",
        "commit_id",
        "lixcol_commit_id",
        "root_commit_id",
        "lixcol_root_commit_id",
        "depth",
        "lixcol_depth",
        "inherited_from_version_id",
        "lixcol_inherited_from_version_id",
    ]
    .into_iter()
    .map(ToString::to_string)
    .collect::<Vec<_>>();
    if let Some(property_names) = property_names {
        allowed.extend(property_names.iter().cloned());
    }
    allowed.sort();
    allowed.dedup();

    LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!(
            "strict rewrite violation: entity view '{view_name}' {context} references unknown column '{}'; allowed columns: {}",
            column.as_ref(),
            allowed.join(", ")
        ),
    }
}

fn find_first_column_index(columns: &HashMap<String, usize>, candidates: &[&str]) -> Option<usize> {
    candidates
        .iter()
        .find_map(|candidate| columns.get(*candidate).copied())
}

fn mutation_variant(target: &EntityViewTarget) -> EntityViewVariant {
    if target.variant == EntityViewVariant::Base && target.version_id_override.is_some() {
        EntityViewVariant::ByVersion
    } else {
        target.variant
    }
}

fn set_update_target_table(
    table: &mut TableWithJoins,
    variant: EntityViewVariant,
) -> Result<(), LixError> {
    match &mut table.relation {
        TableFactor::Table { name, .. } => {
            *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                match variant {
                    EntityViewVariant::Base => LIX_STATE_VIEW_NAME,
                    EntityViewVariant::ByVersion => LIX_STATE_BY_VERSION_VIEW_NAME,
                    EntityViewVariant::History => unreachable!(),
                },
            ))]);
            Ok(())
        }
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "entity view update requires a table target".to_string(),
        }),
    }
}

fn replace_delete_target_table(
    delete: &mut Delete,
    variant: EntityViewVariant,
) -> Result<(), LixError> {
    let tables = match &mut delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "entity view delete requires a single table target".to_string(),
        });
    }
    let table = tables.first_mut().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: "entity view delete requires a table target".to_string(),
    })?;
    if !table.joins.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "entity view delete does not support JOIN targets".to_string(),
        });
    }
    match &mut table.relation {
        TableFactor::Table { name, .. } => {
            *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                match variant {
                    EntityViewVariant::Base => LIX_STATE_VIEW_NAME,
                    EntityViewVariant::ByVersion => LIX_STATE_BY_VERSION_VIEW_NAME,
                    EntityViewVariant::History => unreachable!(),
                },
            ))]);
            Ok(())
        }
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "entity view delete requires a table target".to_string(),
        }),
    }
}

fn table_target_name(table: &TableWithJoins) -> Option<ObjectName> {
    if !table.joins.is_empty() {
        return None;
    }
    match &table.relation {
        TableFactor::Table { name, .. } => Some(name.clone()),
        _ => None,
    }
}

fn delete_target_name(delete: &Delete) -> Option<ObjectName> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return None;
    }
    match &tables[0].relation {
        TableFactor::Table { name, .. } => Some(name.clone()),
        _ => None,
    }
}

fn rewrite_metadata_column_name(column: &str) -> Option<&'static str> {
    Some(match column {
        "entity_id" | "lixcol_entity_id" => "entity_id",
        "schema_key" | "lixcol_schema_key" => "schema_key",
        "file_id" | "lixcol_file_id" => "file_id",
        "version_id" | "lixcol_version_id" => "version_id",
        "plugin_key" | "lixcol_plugin_key" => "plugin_key",
        "snapshot_content" | "lixcol_snapshot_content" => "snapshot_content",
        "schema_version" | "lixcol_schema_version" => "schema_version",
        "created_at" | "lixcol_created_at" => "created_at",
        "updated_at" | "lixcol_updated_at" => "updated_at",
        "inherited_from_version_id" | "lixcol_inherited_from_version_id" => {
            "inherited_from_version_id"
        }
        "change_id" | "lixcol_change_id" => "change_id",
        "metadata" | "lixcol_metadata" => "metadata",
        "untracked" | "lixcol_untracked" => "untracked",
        "commit_id" | "lixcol_commit_id" => "commit_id",
        "root_commit_id" | "lixcol_root_commit_id" => "root_commit_id",
        "depth" | "lixcol_depth" => "depth",
        _ => return None,
    })
}

fn property_name_set(target: &EntityViewTarget) -> HashSet<String> {
    target
        .properties
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect()
}

fn read_only_error(view_name: &str, operation: &str) -> LixError {
    errors::read_only_view_write_error(view_name, operation)
}

fn string_literal_expr(value: &str) -> Expr {
    Expr::Value(AstValue::SingleQuotedString(value.to_string()).into())
}

fn boolean_expr(value: bool) -> Expr {
    Expr::Value(AstValue::Boolean(value).into())
}

fn null_expr() -> Expr {
    Expr::Value(AstValue::Null.into())
}

fn parse_expression_from_sql(sql: &str) -> Result<Expr, LixError> {
    let wrapper_sql = format!("SELECT {sql}");
    let mut statements =
        Parser::parse_sql(&GenericDialect {}, &wrapper_sql).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: error.to_string(),
        })?;
    if statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "expected a single expression statement".to_string(),
        });
    }
    let statement = statements.remove(0);
    let Statement::Query(query) = statement else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "expected SELECT expression statement".to_string(),
        });
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "expected SELECT expression".to_string(),
        });
    };
    let Some(item) = select.projection.first() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "missing projected expression".to_string(),
        });
    };
    match item {
        SelectItem::UnnamedExpr(expr) => Ok(expr.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "expected unnamed projected expression".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map as JsonMap};
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::{derive_entity_id_expr, rewrite_delete, rewrite_insert, rewrite_update};
    use crate::engine::sql::planning::rewrite_engine::entity_views::target::resolve_target_from_view_name;
    use crate::Value as EngineValue;

    #[test]
    fn rewrites_lix_key_value_by_version_insert_target() {
        let sql = "INSERT INTO lix_key_value_by_version (key, value, lixcol_file_id, lixcol_version_id, lixcol_plugin_key, lixcol_schema_version) VALUES ('e', 'x', 'lix', 'main', 'lix', '1')";
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        let statement = statements.remove(0);
        let Statement::Insert(insert) = statement else {
            panic!("expected insert statement");
        };
        let rewritten = rewrite_insert(insert, &[])
            .expect("rewrite should succeed")
            .expect("insert should rewrite");
        assert_eq!(rewritten.table.to_string(), "lix_state_by_version");
    }

    #[test]
    fn rejects_by_version_update_without_version_scope_or_override() {
        let sql = "UPDATE lix_key_value_by_version SET value = 'x' WHERE key = 'k'";
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        let statement = statements.remove(0);
        let Statement::Update(update) = statement else {
            panic!("expected update statement");
        };
        let err = rewrite_update(update, &[])
            .expect_err("update rewrite should require by-version scope");
        assert!(err
            .to_string()
            .contains("requires explicit lixcol_version_id or schema default override"));
    }

    #[test]
    fn rejects_by_version_delete_without_version_scope_or_override() {
        let sql = "DELETE FROM lix_key_value_by_version WHERE key = 'k'";
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        let statement = statements.remove(0);
        let Statement::Delete(delete) = statement else {
            panic!("expected delete statement");
        };
        let err =
            rewrite_delete(delete).expect_err("delete rewrite should require by-version scope");
        assert!(err
            .to_string()
            .contains("requires explicit lixcol_version_id or schema default override"));
    }

    #[test]
    fn by_version_delete_uses_schema_version_override_when_missing_in_where() {
        let sql = "DELETE FROM lix_change_set_by_version WHERE id = 'cs-1'";
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        let statement = statements.remove(0);
        let Statement::Delete(delete) = statement else {
            panic!("expected delete statement");
        };
        let rewritten = rewrite_delete(delete)
            .expect("delete rewrite should succeed")
            .expect("delete should be rewritten");
        let rendered = rewritten.to_string();
        assert!(rendered.contains("version_id = 'global'"));
        assert!(rendered.contains("schema_key = 'lix_change_set'"));
    }

    #[test]
    fn by_version_update_pushes_down_derived_entity_id_for_single_primary_key() {
        let sql =
            "UPDATE lix_key_value_by_version SET value = 'x' WHERE key = 'k' AND version_id = 'v1'";
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        let statement = statements.remove(0);
        let Statement::Update(update) = statement else {
            panic!("expected update statement");
        };
        let rewritten = rewrite_update(update, &[])
            .expect("update rewrite should succeed")
            .expect("update should rewrite");
        let rendered = rewritten.to_string();
        assert!(rendered.contains("entity_id = 'k'"));
    }

    #[test]
    fn by_version_delete_pushes_down_derived_entity_id_for_composite_primary_key() {
        let sql = "DELETE FROM lix_change_author_by_version \
                   WHERE change_id = 'change-1' AND account_id = 'account-1' AND version_id = 'global'";
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        let statement = statements.remove(0);
        let Statement::Delete(delete) = statement else {
            panic!("expected delete statement");
        };
        let rewritten = rewrite_delete(delete)
            .expect("delete rewrite should succeed")
            .expect("delete should rewrite");
        let rendered = rewritten.to_string();
        assert!(rendered.contains("entity_id ="));
        assert!(rendered.contains("change-1"));
        assert!(rendered.contains("account-1"));
        assert!(rendered.contains("||"));
    }

    #[test]
    fn derive_entity_id_supports_nested_primary_key_pointers_for_insert() {
        let target = resolve_target_from_view_name("lix_stored_schema_by_version")
            .expect("resolve should succeed")
            .expect("target should resolve");
        let mut snapshot = JsonMap::new();
        snapshot.insert(
            "value".to_string(),
            json!({
                "x-lix-key": "mock_schema",
                "x-lix-version": "1"
            }),
        );

        let entity_id = derive_entity_id_expr(&snapshot, &target).expect("entity id should derive");
        assert_eq!(entity_id.to_string(), "'mock_schema~1'");
    }

    #[test]
    fn rewrite_insert_derives_nested_primary_key_when_value_uses_lix_json() {
        let sql = "INSERT INTO lix_stored_schema_by_version (value, lixcol_version_id) VALUES (lix_json('{\"x-lix-key\":\"mock_schema\",\"x-lix-version\":\"1\"}'), 'global')";
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        let statement = statements.remove(0);
        let Statement::Insert(insert) = statement else {
            panic!("expected insert statement");
        };
        let rewritten = rewrite_insert(insert, &[])
            .expect("insert rewrite should succeed")
            .expect("insert should rewrite");
        let rendered = rewritten.to_string();
        assert!(rendered.contains("entity_id"));
        assert!(rendered.contains("mock_schema~1"));
    }

    #[test]
    fn rewrite_insert_keeps_bound_version_id_when_placeholders_are_reordered() {
        let sql =
            "INSERT INTO lix_stored_schema_by_version (value, lixcol_version_id) VALUES (lix_json(?), ?)";
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        let statement = statements.remove(0);
        let Statement::Insert(insert) = statement else {
            panic!("expected insert statement");
        };
        let rewritten = rewrite_insert(
            insert,
            &[
                EngineValue::Text(
                    "{\"x-lix-key\":\"mock_schema\",\"x-lix-version\":\"1\"}".to_string(),
                ),
                EngineValue::Text("global".to_string()),
            ],
        )
        .expect("insert rewrite should succeed")
        .expect("insert should rewrite");
        let rendered = rewritten.to_string();
        assert!(rendered.contains("version_id"));
        assert!(rendered.contains("'global'"));
    }

    #[test]
    fn rewrite_insert_rejects_json_text_without_lix_json() {
        let sql = "INSERT INTO lix_stored_schema_by_version (value, lixcol_version_id) VALUES ('{\"x-lix-key\":\"mock_schema\",\"x-lix-version\":\"1\"}', 'global')";
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        let statement = statements.remove(0);
        let Statement::Insert(insert) = statement else {
            panic!("expected insert statement");
        };
        let err = rewrite_insert(insert, &[]).expect_err("insert should require lix_json()");
        assert!(err
            .description
            .contains("Wrap JSON object/array input with lix_json(...)"));
    }

    #[test]
    fn rewrite_insert_allows_stored_schema_on_conflict_do_nothing() {
        let sql = "INSERT INTO lix_stored_schema_by_version (value, lixcol_version_id) \
                   VALUES (lix_json('{\"x-lix-key\":\"mock_schema\",\"x-lix-version\":\"1\"}'), 'global') \
                   ON CONFLICT (entity_id, file_id, version_id) DO NOTHING";
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        let statement = statements.remove(0);
        let Statement::Insert(insert) = statement else {
            panic!("expected insert statement");
        };
        let rewritten = rewrite_insert(insert, &[])
            .expect("insert rewrite should succeed")
            .expect("insert should rewrite");
        assert!(rewritten.to_string().contains("lix_state_by_version"));
    }
}
