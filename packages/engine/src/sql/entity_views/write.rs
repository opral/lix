use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;

use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, Delete, Expr, FromTable, Ident, Insert,
    ObjectName, ObjectNamePart, Query, SelectItem, SetExpr, Statement, TableFactor, TableObject,
    TableWithJoins, Update, Value as AstValue, ValueWithSpan, Values,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::backend::SqlDialect;
use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::sql::route::{rewrite_read_query, rewrite_read_query_with_backend};
use crate::sql::row_resolution::resolve_values_rows;
use crate::sql::{resolve_expr_cell_with_state, PlaceholderState, ResolvedCell};
use crate::{LixBackend, LixError, Value as EngineValue};

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
    let mut rewritten = rewrite_update_with_target(
        update,
        &target,
        SqlDialect::Sqlite,
        params,
    )?;
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
    let mut rewritten = rewrite_update_with_target(
        update,
        &target,
        backend.dialect(),
        params,
    )?;
    if let Some(selection) = rewritten.selection.take() {
        rewritten.selection = Some(
            rewrite_subquery_expressions_with_backend(selection, backend).await?,
        );
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
    let mut rewritten = rewrite_delete_with_target(
        delete,
        &target,
        SqlDialect::Sqlite,
    )?;
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
    let mut rewritten = rewrite_delete_with_target(
        delete,
        &target,
        backend.dialect(),
    )?;
    if let Some(selection) = rewritten.selection.take() {
        rewritten.selection = Some(
            rewrite_subquery_expressions_with_backend(selection, backend).await?,
        );
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
    if insert.on.is_some() {
        return Err(LixError {
            message: format!("{} insert does not support ON CONFLICT", target.view_name),
        });
    }
    let is_default_values_insert = insert.columns.is_empty() && insert.source.is_none();
    if insert.columns.is_empty() && !is_default_values_insert {
        return Err(LixError {
            message: format!("{} insert requires explicit columns", target.view_name),
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
        message: format!("{} insert requires VALUES rows", target.view_name),
    })?;
    let SetExpr::Values(values) = source.body.as_mut() else {
        return Err(LixError {
            message: format!("{} insert requires VALUES rows", target.view_name),
        });
    };

    let mut column_index = HashMap::new();
    for (index, column) in insert.columns.iter().enumerate() {
        column_index.insert(column.value.to_ascii_lowercase(), index);
    }
    if column_index.contains_key("schema_key") || column_index.contains_key("lixcol_schema_key") {
        return Err(LixError {
            message: format!(
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
            message: format!(
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
            message: format!(
                "{} insert cannot set version_id; version scope is resolved by the view/schema",
                target.view_name
            ),
        });
    }

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
                message: format!(
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
            Some(index) => row[index].clone(),
            None => derive_entity_id_expr(&snapshot_object, target)?,
        };
        let file_id_expr = match file_id_index {
            Some(index) => row[index].clone(),
            None => match target.file_id_override.as_ref() {
                Some(value) => string_literal_expr(value),
                None => string_literal_expr("lix"),
            },
        };
        let plugin_key_expr = match plugin_key_index {
            Some(index) => row[index].clone(),
            None => match target.plugin_key_override.as_ref() {
                Some(value) => string_literal_expr(value),
                None => string_literal_expr("lix"),
            },
        };
        let schema_version_expr = match schema_version_index {
            Some(index) => row[index].clone(),
            None => string_literal_expr(&target.schema_version),
        };
        let metadata_expr = match metadata_index {
            Some(index) => row[index].clone(),
            None => null_expr(),
        };
        let untracked_expr = match untracked_index {
            Some(index) => row[index].clone(),
            None => integer_expr(0),
        };
        let snapshot_content_expr = string_literal_expr(
            &JsonValue::Object(snapshot_object.clone()).to_string(),
        );
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
                    Some(index) => row[index].clone(),
                    None => match target.version_id_override.as_ref() {
                        Some(value) => string_literal_expr(value),
                        None => {
                            return Err(LixError {
                                message: format!(
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

fn rewrite_update_with_target(
    mut update: Update,
    target: &EntityViewTarget,
    dialect: SqlDialect,
    params: &[EngineValue],
) -> Result<Update, LixError> {
    let write_variant = mutation_variant(target);
    if !update.table.joins.is_empty() {
        return Err(LixError {
            message: format!("{} update does not support JOIN targets", target.view_name),
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
                message: format!(
                    "{} update does not support tuple assignments",
                    target.view_name
                ),
            });
        };
        let Some(terminal) = column_name.0.last().and_then(ObjectNamePart::as_ident) else {
            rewritten_assignments.push(assignment);
            continue;
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
            property_assignments.insert(column, value);
            continue;
        }

        let Some(mapped) = rewrite_metadata_column_name(&column) else {
            rewritten_assignments.push(assignment);
            continue;
        };
        if mapped == "schema_key" {
            return Err(LixError {
                message: format!(
                    "{} update cannot set schema_key; view schema is fixed",
                    target.view_name
                ),
            });
        }
        if mapped == "snapshot_content" {
            return Err(LixError {
                message: format!(
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
        .map(|expr| rewrite_expression(expr, &property_names, dialect))
        .transpose()?;
    update.selection = Some(append_entity_scope_predicate(
        rewritten_selection,
        &target.schema_key,
        target,
        write_variant,
    ));
    Ok(update)
}

fn rewrite_delete_with_target(
    mut delete: Delete,
    target: &EntityViewTarget,
    dialect: SqlDialect,
) -> Result<Delete, LixError> {
    let write_variant = mutation_variant(target);
    replace_delete_target_table(&mut delete, write_variant)?;
    let property_names = property_name_set(target);
    let rewritten_selection = delete
        .selection
        .take()
        .map(|expr| rewrite_expression(expr, &property_names, dialect))
        .transpose()?;
    delete.selection = Some(append_entity_scope_predicate(
        rewritten_selection,
        &target.schema_key,
        target,
        write_variant,
    ));
    Ok(delete)
}

fn rewrite_expression(
    expr: Expr,
    property_names: &HashSet<String>,
    dialect: SqlDialect,
) -> Result<Expr, LixError> {
    Ok(match expr {
        Expr::Identifier(ident) => {
            rewrite_column_reference_expr(ident.value, None, property_names, dialect)?
        }
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
            )?
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(rewrite_expression(*left, property_names, dialect)?),
            op,
            right: Box::new(rewrite_expression(*right, property_names, dialect)?),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op,
            expr: Box::new(rewrite_expression(*expr, property_names, dialect)?),
        },
        Expr::Nested(inner) => Expr::Nested(Box::new(rewrite_expression(
            *inner,
            property_names,
            dialect,
        )?)),
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(rewrite_expression(*expr, property_names, dialect)?),
            list: list
                .into_iter()
                .map(|item| rewrite_expression(item, property_names, dialect))
                .collect::<Result<Vec<_>, _>>()?,
            negated,
        },
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(rewrite_expression(*expr, property_names, dialect)?),
            subquery,
            negated,
        },
        Expr::InUnnest {
            expr,
            array_expr,
            negated,
        } => Expr::InUnnest {
            expr: Box::new(rewrite_expression(*expr, property_names, dialect)?),
            array_expr: Box::new(rewrite_expression(*array_expr, property_names, dialect)?),
            negated,
        },
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(rewrite_expression(*expr, property_names, dialect)?),
            negated,
            low: Box::new(rewrite_expression(*low, property_names, dialect)?),
            high: Box::new(rewrite_expression(*high, property_names, dialect)?),
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
            expr: Box::new(rewrite_expression(*expr, property_names, dialect)?),
            pattern: Box::new(rewrite_expression(*pattern, property_names, dialect)?),
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
            expr: Box::new(rewrite_expression(*expr, property_names, dialect)?),
            pattern: Box::new(rewrite_expression(*pattern, property_names, dialect)?),
            escape_char,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_expression(
            *inner,
            property_names,
            dialect,
        )?)),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rewrite_expression(
            *inner,
            property_names,
            dialect,
        )?)),
        Expr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => Expr::Cast {
            kind,
            expr: Box::new(rewrite_expression(*expr, property_names, dialect)?),
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
                        *expr = rewrite_expression(expr.clone(), property_names, dialect)?;
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
            left: Box::new(rewrite_expression(*left, property_names, dialect)?),
            compare_op,
            right: Box::new(rewrite_expression(*right, property_names, dialect)?),
            is_some,
        },
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => Expr::AllOp {
            left: Box::new(rewrite_expression(*left, property_names, dialect)?),
            compare_op,
            right: Box::new(rewrite_expression(*right, property_names, dialect)?),
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
                        result: rewrite_subquery_expressions_with_backend(condition.result, backend)
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
    if let Some(qualifier) = qualifier {
        return Ok(Expr::CompoundIdentifier(vec![
            Ident::new(qualifier),
            Ident::new(column_name),
        ]));
    }
    Ok(Expr::Identifier(Ident::new(column_name)))
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
) -> Expr {
    let scoped = append_schema_predicate(selection, schema_key);
    let base_override_version_id = (target.variant == EntityViewVariant::Base
        && write_variant == EntityViewVariant::ByVersion)
        .then(|| target.version_id_override.as_deref())
        .flatten();
    match base_override_version_id {
        Some(version_id) => Expr::BinaryOp {
            left: Box::new(scoped),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("version_id"))),
                op: BinaryOperator::Eq,
                right: Box::new(string_literal_expr(version_id)),
            }),
        },
        None => scoped,
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
                message: format!(
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
    let Some(properties) = target.schema.get("properties").and_then(JsonValue::as_object) else {
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
                message: format!(
                    "{} insert failed to evaluate x-lix-default for '{}.{}': {}",
                    target.view_name, target.schema_key, property, err.message
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
    if let Some(cell) = cell {
        if let Some(value) = &cell.value {
            return json_value_from_engine_value(value, context);
        }
    }
    json_value_from_literal_expr(expr, context)
}

fn json_value_from_engine_value(value: &EngineValue, context: &str) -> Result<JsonValue, LixError> {
    match value {
        EngineValue::Null => Ok(JsonValue::Null),
        EngineValue::Integer(value) => Ok(JsonValue::Number((*value).into())),
        EngineValue::Real(value) => JsonNumber::from_f64(*value)
            .map(JsonValue::Number)
            .ok_or_else(|| LixError {
                message: format!("{context} contains non-finite numeric value"),
            }),
        EngineValue::Text(value) => Ok(JsonValue::String(value.clone())),
        EngineValue::Blob(_) => Err(LixError {
            message: format!("{context} does not support blob values"),
        }),
    }
}

fn json_value_from_literal_expr(expr: Option<&Expr>, context: &str) -> Result<JsonValue, LixError> {
    let expr = expr.ok_or_else(|| LixError {
        message: format!("{context} is missing a value"),
    })?;
    let Expr::Value(ValueWithSpan { value, .. }) = expr else {
        return Err(LixError {
            message: format!("{context} requires literal or placeholder values"),
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
                        message: format!("{context} contains non-finite numeric value"),
                    })
            } else {
                Err(LixError {
                    message: format!("{context} contains unsupported numeric literal '{value}'"),
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
            message: format!("{context} unresolved placeholder '{token}'"),
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
    if target.primary_key_properties.is_empty() {
        return Err(LixError {
            message: format!(
                "{} insert requires entity_id (schema has no x-lix-primary-key)",
                target.view_name
            ),
        });
    }
    let mut parts = Vec::with_capacity(target.primary_key_properties.len());
    for property in &target.primary_key_properties {
        let Some(value) = snapshot.get(property) else {
            return Err(LixError {
                message: format!(
                    "{} insert requires entity_id or all primary-key properties ({})",
                    target.view_name,
                    target.primary_key_properties.join(", ")
                ),
            });
        };
        parts.push(entity_id_component_from_json_value(value, property, target)?);
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
    property: &str,
    target: &EntityViewTarget,
) -> Result<String, LixError> {
    match value {
        JsonValue::Null => Err(LixError {
            message: format!(
                "{} insert cannot derive entity_id from null primary-key property '{}'",
                target.view_name, property
            ),
        }),
        JsonValue::String(text) => Ok(text.clone()),
        JsonValue::Bool(flag) => Ok(flag.to_string()),
        JsonValue::Number(number) => Ok(number.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Ok(value.to_string()),
    }
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
            message: "entity view update requires a table target".to_string(),
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
            message: "entity view delete requires a single table target".to_string(),
        });
    }
    let table = tables.first_mut().ok_or_else(|| LixError {
        message: "entity view delete requires a table target".to_string(),
    })?;
    if !table.joins.is_empty() {
        return Err(LixError {
            message: "entity view delete does not support JOIN targets".to_string(),
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
            message: "entity view delete requires a table target".to_string(),
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
    LixError {
        message: format!("{view_name} is read-only; {operation} is not supported"),
    }
}

fn string_literal_expr(value: &str) -> Expr {
    Expr::Value(AstValue::SingleQuotedString(value.to_string()).into())
}

fn integer_expr(value: i64) -> Expr {
    Expr::Value(AstValue::Number(value.to_string(), false).into())
}

fn null_expr() -> Expr {
    Expr::Value(AstValue::Null.into())
}

fn parse_expression_from_sql(sql: &str) -> Result<Expr, LixError> {
    let wrapper_sql = format!("SELECT {sql}");
    let mut statements =
        Parser::parse_sql(&GenericDialect {}, &wrapper_sql).map_err(|error| LixError {
            message: error.to_string(),
        })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single expression statement".to_string(),
        });
    }
    let statement = statements.remove(0);
    let Statement::Query(query) = statement else {
        return Err(LixError {
            message: "expected SELECT expression statement".to_string(),
        });
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(LixError {
            message: "expected SELECT expression".to_string(),
        });
    };
    let Some(item) = select.projection.first() else {
        return Err(LixError {
            message: "missing projected expression".to_string(),
        });
    };
    match item {
        SelectItem::UnnamedExpr(expr) => Ok(expr.clone()),
        _ => Err(LixError {
            message: "expected unnamed projected expression".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::rewrite_insert;

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
}
