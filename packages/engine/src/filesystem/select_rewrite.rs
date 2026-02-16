use sqlparser::ast::{
    Expr, Ident, ObjectName, ObjectNamePart, Query, Select, SelectItem, SetExpr, Statement,
    TableAlias, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::filesystem::path::parse_file_path;
use crate::sql::escape_sql_string;
use crate::sql::{resolve_expr_cell_with_state, PlaceholderState};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
};
use crate::{LixError, Value};

const FILE_VIEW: &str = "lix_file";
const FILE_BY_VERSION_VIEW: &str = "lix_file_by_version";
const FILE_HISTORY_VIEW: &str = "lix_file_history";
const DIRECTORY_VIEW: &str = "lix_directory";
const DIRECTORY_BY_VERSION_VIEW: &str = "lix_directory_by_version";
const DIRECTORY_HISTORY_VIEW: &str = "lix_directory_history";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_params(query, &[])
}

pub fn rewrite_query_with_params(query: Query, params: &[Value]) -> Result<Option<Query>, LixError> {
    if let Some(rewritten) = try_rewrite_lix_file_path_data_fast_path(&query, params)? {
        return Ok(Some(rewritten));
    }

    let mut changed = false;
    let mut new_query = query.clone();
    new_query.body = Box::new(rewrite_set_expr(*query.body, &mut changed)?);

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

enum FilePathDataFastPathKind {
    Scan,
    PointPath(String),
}

fn try_rewrite_lix_file_path_data_fast_path(
    query: &Query,
    params: &[Value],
) -> Result<Option<Query>, LixError> {
    if query.with.is_some() || query.limit_clause.is_some() || query.fetch.is_some() {
        return Ok(None);
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    let select = select.as_ref();
    let Some(kind) = classify_simple_lix_file_path_data_select(select, params)? else {
        return Ok(None);
    };
    if !order_by_uses_only_path_or_data(query) {
        return Ok(None);
    }

    let order_by_sql = query.order_by.as_ref().map(ToString::to_string);
    let rewritten = match kind {
        FilePathDataFastPathKind::Scan => build_file_path_data_fast_query(order_by_sql)?,
        FilePathDataFastPathKind::PointPath(path) => {
            build_file_path_data_point_query(&path, order_by_sql)?
        }
    };
    Ok(Some(rewritten))
}

fn build_file_path_data_fast_query(order_by_sql: Option<String>) -> Result<Query, LixError> {
    let mut sql = format!(
        "WITH RECURSIVE directory_descriptor_rows AS (\
             SELECT \
                lix_json_text(snapshot_content, 'id') AS id, \
                lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                lix_json_text(snapshot_content, 'name') AS name, \
                version_id AS lixcol_version_id \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_directory_descriptor' \
               AND snapshot_content IS NOT NULL \
               AND {active_version_scope_descriptor}\
         ), \
         directory_paths AS (\
             SELECT \
                d.id, \
                d.lixcol_version_id, \
                '/' || d.name || '/' AS path \
             FROM directory_descriptor_rows d \
             WHERE d.parent_id IS NULL \
             UNION ALL \
             SELECT \
                child.id, \
                child.lixcol_version_id, \
                parent.path || child.name || '/' AS path \
             FROM directory_descriptor_rows child \
             JOIN directory_paths parent \
               ON parent.id = child.parent_id \
              AND parent.lixcol_version_id = child.lixcol_version_id\
         ), \
         file_descriptor_rows AS (\
             SELECT \
                lix_json_text(snapshot_content, 'id') AS id, \
                lix_json_text(snapshot_content, 'directory_id') AS directory_id, \
                lix_json_text(snapshot_content, 'name') AS name, \
                lix_json_text(snapshot_content, 'extension') AS extension, \
                version_id AS lixcol_version_id \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_file_descriptor' \
               AND snapshot_content IS NOT NULL \
               AND {active_version_scope_descriptor}\
         ) \
         SELECT \
            CASE \
                WHEN f.directory_id IS NULL THEN \
                    CASE \
                        WHEN f.extension IS NULL OR f.extension = '' THEN '/' || f.name \
                        ELSE '/' || f.name || '.' || f.extension \
                    END \
                WHEN dp.path IS NULL THEN NULL \
                ELSE \
                    CASE \
                        WHEN f.extension IS NULL OR f.extension = '' THEN dp.path || f.name \
                        ELSE dp.path || f.name || '.' || f.extension \
                    END \
            END AS path, \
            COALESCE(fd.data, lix_empty_blob()) AS data \
         FROM file_descriptor_rows f \
         LEFT JOIN directory_paths dp \
           ON dp.id = f.directory_id \
          AND dp.lixcol_version_id = f.lixcol_version_id \
         LEFT JOIN lix_internal_file_data_cache fd \
           ON fd.file_id = f.id \
          AND fd.version_id = f.lixcol_version_id \
         WHERE {active_version_scope}",
        active_version_scope = active_version_scope_predicate("f.lixcol_version_id"),
        active_version_scope_descriptor = active_version_scope_predicate("version_id")
    );
    if let Some(order_by) = order_by_sql {
        sql.push(' ');
        sql.push_str(&order_by);
    }
    parse_single_query(&sql)
}

fn build_file_path_data_point_query(
    normalized_path: &str,
    order_by_sql: Option<String>,
) -> Result<Query, LixError> {
    let parsed = parse_file_path(normalized_path)?;
    let mut ctes = Vec::new();

    if let Some(directory_path) = parsed.directory_path.as_ref() {
        let segments = directory_path
            .trim_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        if segments.is_empty() {
            return Err(LixError {
                message: "invalid directory path for point filesystem fast-path".to_string(),
            });
        }

        ctes.push(format!(
            "directory_descriptor_rows AS (\
                 SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
                    version_id AS lixcol_version_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_directory_descriptor' \
                   AND snapshot_content IS NOT NULL \
                   AND {active_version_scope_descriptor}\
             )",
            active_version_scope_descriptor = active_version_scope_predicate("version_id")
        ));

        for (depth, segment) in segments.iter().enumerate() {
            let segment = escape_sql_string(segment);
            let cte = if depth == 0 {
                format!(
                    "dir_level_0 AS (\
                         SELECT id, lixcol_version_id \
                         FROM directory_descriptor_rows \
                         WHERE parent_id IS NULL \
                           AND name = '{segment}'\
                     )",
                )
            } else {
                format!(
                    "dir_level_{depth} AS (\
                         SELECT child.id, child.lixcol_version_id \
                         FROM directory_descriptor_rows child \
                         JOIN dir_level_{parent_depth} parent \
                           ON parent.id = child.parent_id \
                          AND parent.lixcol_version_id = child.lixcol_version_id \
                         WHERE child.name = '{segment}'\
                     )",
                    parent_depth = depth - 1
                )
            };
            ctes.push(cte);
        }
    }

    let extension_predicate = if let Some(extension) = parsed.extension.as_ref() {
        format!(
            "lix_json_text(snapshot_content, 'extension') = '{}'",
            escape_sql_string(extension)
        )
    } else {
        "lix_json_text(snapshot_content, 'extension') IS NULL".to_string()
    };

    let directory_predicate = if parsed.directory_path.is_some() {
        let directory_depth = parsed
            .directory_path
            .as_ref()
            .map(|path| {
                path.trim_matches('/')
                    .split('/')
                    .filter(|segment| !segment.is_empty())
                    .count()
            })
            .unwrap_or(0);
        format!(
            "EXISTS (\
                 SELECT 1 \
                 FROM dir_level_{target_depth} target_dir \
                 WHERE target_dir.id = lix_json_text(snapshot_content, 'directory_id') \
                   AND target_dir.lixcol_version_id = version_id\
             )",
            target_depth = directory_depth - 1
        )
    } else {
        "lix_json_text(snapshot_content, 'directory_id') IS NULL".to_string()
    };

    ctes.push(format!(
        "file_descriptor_rows AS (\
             SELECT \
                lix_json_text(snapshot_content, 'id') AS id, \
                version_id AS lixcol_version_id \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_file_descriptor' \
               AND snapshot_content IS NOT NULL \
               AND {active_version_scope_descriptor} \
               AND lix_json_text(snapshot_content, 'name') = '{name}' \
               AND {extension_predicate} \
               AND {directory_predicate}\
         )",
        active_version_scope_descriptor = active_version_scope_predicate("version_id"),
        name = escape_sql_string(&parsed.name),
        extension_predicate = extension_predicate,
        directory_predicate = directory_predicate
    ));

    let mut sql = format!(
        "WITH {ctes} \
         SELECT \
            '{path}' AS path, \
            COALESCE(fd.data, lix_empty_blob()) AS data \
         FROM file_descriptor_rows f \
         LEFT JOIN lix_internal_file_data_cache fd \
           ON fd.file_id = f.id \
          AND fd.version_id = f.lixcol_version_id",
        ctes = ctes.join(", "),
        path = escape_sql_string(&parsed.normalized_path)
    );
    if let Some(order_by) = order_by_sql {
        sql.push(' ');
        sql.push_str(&order_by);
    }

    parse_single_query(&sql)
}

fn classify_simple_lix_file_path_data_select(
    select: &Select,
    params: &[Value],
) -> Result<Option<FilePathDataFastPathKind>, LixError> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Ok(None);
    }
    let TableFactor::Table { name, .. } = &select.from[0].relation else {
        return Ok(None);
    };
    let Some(table_name) = object_name_terminal(name) else {
        return Ok(None);
    };
    if !table_name.eq_ignore_ascii_case(FILE_VIEW) {
        return Ok(None);
    }

    if select.prewhere.is_some() {
        return Ok(None);
    }
    if !group_by_expr_is_empty(&select.group_by)
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || select.qualify.is_some()
        || select.connect_by.is_some()
    {
        return Ok(None);
    }

    let projection_matches = select.projection.len() == 2
        && select_item_is_column(&select.projection[0], "path")
        && select_item_is_column(&select.projection[1], "data");
    if !projection_matches {
        return Ok(None);
    }

    let Some(selection) = select.selection.as_ref() else {
        return Ok(Some(FilePathDataFastPathKind::Scan));
    };

    let Some(path) = extract_path_equality_value(selection, params)? else {
        return Ok(None);
    };
    let Ok(parsed) = parse_file_path(&path) else {
        return Ok(None);
    };
    Ok(Some(FilePathDataFastPathKind::PointPath(
        parsed.normalized_path,
    )))
}

fn extract_path_equality_value(selection: &Expr, params: &[Value]) -> Result<Option<String>, LixError> {
    let Expr::BinaryOp { left, op, right } = selection else {
        return Ok(None);
    };
    if *op != sqlparser::ast::BinaryOperator::Eq {
        return Ok(None);
    }

    let value_expr = if expr_is_column(left, "path") {
        right.as_ref()
    } else if expr_is_column(right, "path") {
        left.as_ref()
    } else {
        return Ok(None);
    };

    let mut placeholder_state = PlaceholderState::new();
    let resolved = resolve_expr_cell_with_state(value_expr, params, &mut placeholder_state)?;
    let Some(Value::Text(path)) = resolved.value else {
        return Ok(None);
    };
    Ok(Some(path))
}

fn group_by_expr_is_empty(group_by: &sqlparser::ast::GroupByExpr) -> bool {
    match group_by {
        sqlparser::ast::GroupByExpr::All(_) => false,
        sqlparser::ast::GroupByExpr::Expressions(expressions, _) => expressions.is_empty(),
    }
}

fn select_item_is_column(item: &SelectItem, column_name: &str) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) => expr_is_column(expr, column_name),
        SelectItem::ExprWithAlias { expr, alias } => {
            expr_is_column(expr, column_name) && alias.value.eq_ignore_ascii_case(column_name)
        }
        _ => false,
    }
}

fn expr_is_column(expr: &Expr, column_name: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(column_name),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|part| part.value.eq_ignore_ascii_case(column_name))
            .unwrap_or(false),
        _ => false,
    }
}

fn order_by_uses_only_path_or_data(query: &Query) -> bool {
    let Some(order_by) = query.order_by.as_ref() else {
        return true;
    };
    let sqlparser::ast::OrderByKind::Expressions(expressions) = &order_by.kind else {
        return false;
    };
    expressions.iter().all(|order_expr| {
        order_expr.with_fill.is_none()
            && (expr_is_column(&order_expr.expr, "path") || expr_is_column(&order_expr.expr, "data"))
    })
}

fn rewrite_set_expr(expr: SetExpr, changed: &mut bool) -> Result<SetExpr, LixError> {
    Ok(match expr {
        SetExpr::Select(select) => {
            let mut select = *select;
            rewrite_select(&mut select, changed)?;
            SetExpr::Select(Box::new(select))
        }
        SetExpr::Query(query) => {
            let mut query = *query;
            query.body = Box::new(rewrite_set_expr(*query.body, changed)?);
            SetExpr::Query(Box::new(query))
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => SetExpr::SetOperation {
            op,
            set_quantifier,
            left: Box::new(rewrite_set_expr(*left, changed)?),
            right: Box::new(rewrite_set_expr(*right, changed)?),
        },
        other => other,
    })
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_table_with_joins(table, changed)?;
    }
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(&mut table.relation, changed)?;
    for join in &mut table.joins {
        rewrite_table_factor(&mut join.relation, changed)?;
    }
    Ok(())
}

fn rewrite_table_factor(relation: &mut TableFactor, changed: &mut bool) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let Some(view_name) = object_name_terminal(name) else {
                return Ok(());
            };
            let Some(source_query) = build_filesystem_projection_query(&view_name)? else {
                return Ok(());
            };
            let derived_alias = alias.clone().or_else(|| Some(default_alias(&view_name)));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(source_query),
                alias: derived_alias,
            };
            *changed = true;
        }
        TableFactor::Derived { subquery, .. } => {
            if let Some(rewritten) = rewrite_query((**subquery).clone())? {
                *subquery = Box::new(rewritten);
                *changed = true;
            }
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_with_joins(table_with_joins, changed)?;
        }
        _ => {}
    }
    Ok(())
}

fn build_filesystem_projection_query(view_name: &str) -> Result<Option<Query>, LixError> {
    let sql = match view_name.to_ascii_lowercase().as_str() {
        FILE_VIEW => format!(
            "WITH RECURSIVE directory_descriptor_rows AS (\
                 SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
                    version_id AS lixcol_version_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_directory_descriptor' \
                   AND snapshot_content IS NOT NULL \
                   AND {active_version_scope_descriptor}\
             ), \
             directory_paths AS (\
                 SELECT \
                    d.id, \
                    d.lixcol_version_id, \
                    '/' || d.name || '/' AS path \
                 FROM directory_descriptor_rows d \
                 WHERE d.parent_id IS NULL \
                 UNION ALL \
                 SELECT \
                    child.id, \
                    child.lixcol_version_id, \
                    parent.path || child.name || '/' AS path \
                 FROM directory_descriptor_rows child \
                 JOIN directory_paths parent \
                   ON parent.id = child.parent_id \
                  AND parent.lixcol_version_id = child.lixcol_version_id\
             ), \
             file_descriptor_rows AS (\
                 SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'directory_id') AS directory_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
                    lix_json_text(snapshot_content, 'extension') AS extension, \
                    lix_json_text(snapshot_content, 'metadata') AS metadata, \
                    lix_json_text(snapshot_content, 'hidden') AS hidden, \
                    entity_id AS lixcol_entity_id, \
                    schema_key AS lixcol_schema_key, \
                    file_id AS lixcol_file_id, \
                    version_id AS lixcol_version_id, \
                    plugin_key AS lixcol_plugin_key, \
                    schema_version AS lixcol_schema_version, \
                    inherited_from_version_id AS lixcol_inherited_from_version_id, \
                    change_id AS lixcol_change_id, \
                    created_at AS lixcol_created_at, \
                    updated_at AS lixcol_updated_at, \
                    writer_key AS lixcol_writer_key, \
                    untracked AS lixcol_untracked, \
                    metadata AS lixcol_metadata \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_file_descriptor' \
                   AND snapshot_content IS NOT NULL \
                   AND {active_version_scope_descriptor}\
             ) \
             SELECT \
                f.id, \
                f.directory_id, \
                f.name, \
                f.extension, \
                CASE \
                    WHEN f.directory_id IS NULL THEN \
                        CASE \
                            WHEN f.extension IS NULL OR f.extension = '' THEN '/' || f.name \
                            ELSE '/' || f.name || '.' || f.extension \
                        END \
                    WHEN dp.path IS NULL THEN NULL \
                    ELSE \
                        CASE \
                            WHEN f.extension IS NULL OR f.extension = '' THEN dp.path || f.name \
                            ELSE dp.path || f.name || '.' || f.extension \
                        END \
                END AS path, \
                COALESCE(fd.data, lix_empty_blob()) AS data, \
                f.metadata, \
                f.hidden, \
                f.lixcol_entity_id, \
                f.lixcol_schema_key, \
                f.lixcol_file_id, \
                f.lixcol_plugin_key, \
                f.lixcol_schema_version, \
                f.lixcol_inherited_from_version_id, \
                f.lixcol_change_id, \
                f.lixcol_created_at, \
                f.lixcol_updated_at, \
                v.commit_id AS lixcol_commit_id, \
                f.lixcol_writer_key, \
                f.lixcol_untracked, \
                f.lixcol_metadata \
             FROM file_descriptor_rows f \
             LEFT JOIN directory_paths dp \
               ON dp.id = f.directory_id \
              AND dp.lixcol_version_id = f.lixcol_version_id \
             LEFT JOIN lix_version v \
               ON v.id = f.lixcol_version_id \
             LEFT JOIN lix_internal_file_data_cache fd \
               ON fd.file_id = f.id \
              AND fd.version_id = f.lixcol_version_id \
             WHERE {active_version_scope}",
            active_version_scope = active_version_scope_predicate("f.lixcol_version_id"),
            active_version_scope_descriptor = active_version_scope_predicate("version_id")
        ),
        FILE_BY_VERSION_VIEW => "WITH RECURSIVE directory_descriptor_rows AS (\
                 SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
                    version_id AS lixcol_version_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_directory_descriptor' \
                   AND snapshot_content IS NOT NULL\
             ), \
             directory_paths AS (\
                 SELECT \
                    d.id, \
                    d.lixcol_version_id, \
                    '/' || d.name || '/' AS path \
                 FROM directory_descriptor_rows d \
                 WHERE d.parent_id IS NULL \
                 UNION ALL \
                 SELECT \
                    child.id, \
                    child.lixcol_version_id, \
                    parent.path || child.name || '/' AS path \
                 FROM directory_descriptor_rows child \
                 JOIN directory_paths parent \
                   ON parent.id = child.parent_id \
                  AND parent.lixcol_version_id = child.lixcol_version_id\
             ), \
             file_descriptor_rows AS (\
                 SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'directory_id') AS directory_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
                    lix_json_text(snapshot_content, 'extension') AS extension, \
                    lix_json_text(snapshot_content, 'metadata') AS metadata, \
                    lix_json_text(snapshot_content, 'hidden') AS hidden, \
                    entity_id AS lixcol_entity_id, \
                    schema_key AS lixcol_schema_key, \
                    file_id AS lixcol_file_id, \
                    version_id AS lixcol_version_id, \
                    plugin_key AS lixcol_plugin_key, \
                    schema_version AS lixcol_schema_version, \
                    inherited_from_version_id AS lixcol_inherited_from_version_id, \
                    change_id AS lixcol_change_id, \
                    created_at AS lixcol_created_at, \
                    updated_at AS lixcol_updated_at, \
                    writer_key AS lixcol_writer_key, \
                    untracked AS lixcol_untracked, \
                    metadata AS lixcol_metadata \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_file_descriptor' \
                   AND snapshot_content IS NOT NULL\
             ) \
             SELECT \
                f.id, \
                f.directory_id, \
                f.name, \
                f.extension, \
                CASE \
                    WHEN f.directory_id IS NULL THEN \
                        CASE \
                            WHEN f.extension IS NULL OR f.extension = '' THEN '/' || f.name \
                            ELSE '/' || f.name || '.' || f.extension \
                        END \
                    WHEN dp.path IS NULL THEN NULL \
                    ELSE \
                        CASE \
                            WHEN f.extension IS NULL OR f.extension = '' THEN dp.path || f.name \
                            ELSE dp.path || f.name || '.' || f.extension \
                        END \
                END AS path, \
                COALESCE(fd.data, lix_empty_blob()) AS data, \
                f.metadata, \
                f.hidden, \
                f.lixcol_entity_id, \
                f.lixcol_schema_key, \
                f.lixcol_file_id, \
                f.lixcol_version_id, \
                f.lixcol_plugin_key, \
                f.lixcol_schema_version, \
                f.lixcol_inherited_from_version_id, \
                f.lixcol_change_id, \
                f.lixcol_created_at, \
                f.lixcol_updated_at, \
                v.commit_id AS lixcol_commit_id, \
                f.lixcol_writer_key, \
                f.lixcol_untracked, \
                f.lixcol_metadata \
             FROM file_descriptor_rows f \
             LEFT JOIN directory_paths dp \
               ON dp.id = f.directory_id \
              AND dp.lixcol_version_id = f.lixcol_version_id \
             LEFT JOIN lix_version v \
               ON v.id = f.lixcol_version_id \
             LEFT JOIN lix_internal_file_data_cache fd \
               ON fd.file_id = f.id \
              AND fd.version_id = f.lixcol_version_id"
            .to_string(),
        FILE_HISTORY_VIEW => "WITH RECURSIVE directory_history_base AS (\
                 SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
                    root_commit_id AS lixcol_root_commit_id, \
                    depth AS lixcol_depth \
                 FROM lix_state_history \
                 WHERE schema_key = 'lix_directory_descriptor' \
                   AND snapshot_content IS NOT NULL\
             ), \
             directory_history_path_walk AS (\
                 SELECT \
                    base.id AS target_id, \
                    base.lixcol_root_commit_id AS root_commit_id, \
                    base.lixcol_depth AS target_depth, \
                    base.parent_id AS next_parent_id, \
                    '/' || base.name || '/' AS path, \
                    0 AS step \
                 FROM directory_history_base base \
                 UNION ALL \
                 SELECT \
                    walk.target_id, \
                    walk.root_commit_id, \
                    walk.target_depth, \
                    parent.parent_id AS next_parent_id, \
                    '/' || parent.name || walk.path AS path, \
                    walk.step + 1 AS step \
                 FROM directory_history_path_walk walk \
                 JOIN directory_history_base parent \
                   ON parent.id = walk.next_parent_id \
                  AND parent.lixcol_root_commit_id = walk.root_commit_id \
                  AND parent.lixcol_depth = (\
                      SELECT MIN(candidate.lixcol_depth) \
                     FROM directory_history_base candidate \
                      WHERE candidate.id = walk.next_parent_id \
                        AND candidate.lixcol_root_commit_id = walk.root_commit_id \
                        AND candidate.lixcol_depth >= walk.target_depth\
                  ) \
                 WHERE walk.next_parent_id IS NOT NULL \
                   AND walk.step < 1024 \
             ), \
             directory_history_paths AS (\
                 SELECT \
                    walk.target_id, \
                    walk.root_commit_id, \
                    walk.target_depth, \
                    walk.path \
                 FROM directory_history_path_walk walk \
                 JOIN (\
                    SELECT \
                        target_id, \
                        root_commit_id, \
                        target_depth, \
                        MAX(step) AS max_step \
                    FROM directory_history_path_walk \
                    WHERE next_parent_id IS NULL \
                    GROUP BY target_id, root_commit_id, target_depth\
                 ) terminal \
                   ON terminal.target_id = walk.target_id \
                  AND terminal.root_commit_id = walk.root_commit_id \
                  AND terminal.target_depth = walk.target_depth \
                  AND terminal.max_step = walk.step \
             ), \
             file_history_descriptor_rows AS (\
                SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'directory_id') AS directory_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
                    lix_json_text(snapshot_content, 'extension') AS extension, \
                    lix_json_text(snapshot_content, 'metadata') AS metadata, \
                    lix_json_text(snapshot_content, 'hidden') AS hidden, \
                    entity_id AS lixcol_entity_id, \
                    schema_key AS lixcol_schema_key, \
                    file_id AS lixcol_file_id, \
                    version_id AS lixcol_version_id, \
                    plugin_key AS lixcol_plugin_key, \
                    schema_version AS lixcol_schema_version, \
                    change_id AS lixcol_change_id, \
                    metadata AS lixcol_metadata, \
                    commit_id AS lixcol_commit_id, \
                    root_commit_id AS lixcol_root_commit_id, \
                    depth AS lixcol_depth \
                FROM lix_state_history \
                WHERE schema_key = 'lix_file_descriptor' \
                  AND snapshot_content IS NOT NULL\
             ), \
             descriptor_depth_zero_roots AS (\
                SELECT \
                    id, \
                    lixcol_root_commit_id \
                FROM file_history_descriptor_rows \
                WHERE lixcol_depth = 0 \
                GROUP BY id, lixcol_root_commit_id\
             ), \
             content_only_roots AS (\
                SELECT \
                    ranked.id, \
                    ranked.lixcol_root_commit_id, \
                    ranked.lixcol_commit_id, \
                    ranked.lixcol_change_id \
                FROM (\
                    SELECT \
                        sh.file_id AS id, \
                        sh.root_commit_id AS lixcol_root_commit_id, \
                        sh.commit_id AS lixcol_commit_id, \
                        sh.change_id AS lixcol_change_id, \
                        ROW_NUMBER() OVER (\
                            PARTITION BY sh.file_id, sh.root_commit_id \
                            ORDER BY ic.created_at DESC, sh.change_id DESC\
                        ) AS row_num \
                    FROM lix_state_history sh \
                    JOIN lix_internal_change ic \
                      ON ic.id = sh.change_id \
                    LEFT JOIN descriptor_depth_zero_roots d0 \
                      ON d0.id = sh.file_id \
                     AND d0.lixcol_root_commit_id = sh.root_commit_id \
                    WHERE sh.depth = 0 \
                      AND sh.file_id IS NOT NULL \
                      AND sh.file_id != 'lix' \
                      AND sh.schema_key != 'lix_file_descriptor' \
                      AND sh.snapshot_content IS NOT NULL \
                      AND d0.id IS NULL\
                ) ranked \
                WHERE ranked.row_num = 1\
             ), \
             content_history_rows AS (\
                SELECT \
                    d.id, \
                    d.directory_id, \
                    d.name, \
                    d.extension, \
                    d.metadata, \
                    d.hidden, \
                    c.id AS lixcol_entity_id, \
                    d.lixcol_schema_key, \
                    c.id AS lixcol_file_id, \
                    d.lixcol_version_id, \
                    d.lixcol_plugin_key, \
                    d.lixcol_schema_version, \
                    c.lixcol_change_id, \
                    d.lixcol_metadata, \
                    c.lixcol_commit_id, \
                    c.lixcol_root_commit_id, \
                    0 AS lixcol_depth \
                FROM content_only_roots c \
                JOIN file_history_descriptor_rows d \
                  ON d.id = c.id \
                 AND d.lixcol_root_commit_id = c.lixcol_root_commit_id \
                 AND d.lixcol_depth = (\
                      SELECT MIN(candidate.lixcol_depth) \
                      FROM file_history_descriptor_rows candidate \
                      WHERE candidate.id = c.id \
                        AND candidate.lixcol_root_commit_id = c.lixcol_root_commit_id\
                 )\
             ), \
             file_history_rows AS (\
                SELECT * FROM file_history_descriptor_rows \
                UNION ALL \
                SELECT * FROM content_history_rows\
             ), \
             file_history_ranked_rows AS (\
                SELECT \
                    fhr.id, \
                    fhr.directory_id, \
                    fhr.name, \
                    fhr.extension, \
                    fhr.metadata, \
                    fhr.hidden, \
                    fhr.lixcol_entity_id, \
                    fhr.lixcol_schema_key, \
                    fhr.lixcol_file_id, \
                    fhr.lixcol_version_id, \
                    fhr.lixcol_plugin_key, \
                    fhr.lixcol_schema_version, \
                    fhr.lixcol_change_id, \
                    fhr.lixcol_metadata, \
                    fhr.lixcol_commit_id, \
                    fhr.lixcol_root_commit_id, \
                    fhr.lixcol_depth AS lixcol_raw_depth, \
                    ROW_NUMBER() OVER (\
                        PARTITION BY fhr.id, fhr.lixcol_root_commit_id \
                        ORDER BY \
                            fhr.lixcol_depth ASC, \
                            fhr.lixcol_commit_id DESC, \
                            fhr.lixcol_change_id DESC\
                    ) - 1 AS lixcol_depth \
                FROM file_history_rows fhr\
             ) \
             SELECT \
                f.id, \
                CASE \
                    WHEN f.directory_id IS NULL THEN \
                        CASE \
                            WHEN f.extension IS NULL OR f.extension = '' THEN '/' || f.name \
                            ELSE '/' || f.name || '.' || f.extension \
                        END \
                    WHEN dp.path IS NULL THEN NULL \
                    ELSE \
                        CASE \
                            WHEN f.extension IS NULL OR f.extension = '' THEN dp.path || f.name \
                            ELSE dp.path || f.name || '.' || f.extension \
                        END \
                END AS path, \
                COALESCE(fd.data, lix_empty_blob()) AS data, \
                f.metadata, \
                f.hidden, \
                f.lixcol_entity_id, \
                f.lixcol_schema_key, \
                f.lixcol_file_id, \
                f.lixcol_version_id, \
                f.lixcol_plugin_key, \
                f.lixcol_schema_version, \
                f.lixcol_change_id, \
                f.lixcol_metadata, \
                f.lixcol_commit_id, \
                f.lixcol_root_commit_id, \
                f.lixcol_depth \
             FROM file_history_ranked_rows f \
             LEFT JOIN directory_history_paths dp \
               ON dp.target_id = f.directory_id \
              AND dp.root_commit_id = f.lixcol_root_commit_id \
              AND dp.target_depth = (\
                  SELECT MIN(candidate.lixcol_depth) \
                  FROM directory_history_base candidate \
                  WHERE candidate.id = f.directory_id \
                    AND candidate.lixcol_root_commit_id = f.lixcol_root_commit_id \
                    AND candidate.lixcol_depth >= f.lixcol_raw_depth\
              ) \
             LEFT JOIN lix_internal_file_history_data_cache fd \
               ON fd.file_id = f.id \
              AND fd.root_commit_id = f.lixcol_root_commit_id \
              AND fd.depth = f.lixcol_depth"
            .to_string(),
        DIRECTORY_VIEW => format!(
            "WITH RECURSIVE directory_descriptor_rows AS (\
                 SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
                    lix_json_text(snapshot_content, 'hidden') AS hidden, \
                    entity_id AS lixcol_entity_id, \
                    schema_key AS lixcol_schema_key, \
                    schema_version AS lixcol_schema_version, \
                    version_id AS lixcol_version_id, \
                    inherited_from_version_id AS lixcol_inherited_from_version_id, \
                    change_id AS lixcol_change_id, \
                    metadata AS lixcol_metadata, \
                    created_at AS lixcol_created_at, \
                    updated_at AS lixcol_updated_at, \
                    untracked AS lixcol_untracked \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_directory_descriptor' \
                   AND snapshot_content IS NOT NULL\
             ), \
             directory_paths AS (\
                 SELECT \
                    d.id, \
                    d.lixcol_version_id, \
                    '/' || d.name || '/' AS path \
                 FROM directory_descriptor_rows d \
                 WHERE d.parent_id IS NULL \
                 UNION ALL \
                 SELECT \
                    child.id, \
                    child.lixcol_version_id, \
                    parent.path || child.name || '/' AS path \
                 FROM directory_descriptor_rows child \
                 JOIN directory_paths parent \
                   ON parent.id = child.parent_id \
                  AND parent.lixcol_version_id = child.lixcol_version_id\
             ) \
             SELECT \
                d.id, \
                d.parent_id, \
                d.name, \
                dp.path AS path, \
                d.hidden, \
                d.lixcol_entity_id, \
                d.lixcol_schema_key, \
                d.lixcol_schema_version, \
                d.lixcol_inherited_from_version_id, \
                d.lixcol_change_id, \
                d.lixcol_created_at, \
                d.lixcol_updated_at, \
                v.commit_id AS lixcol_commit_id, \
                d.lixcol_untracked, \
                d.lixcol_metadata \
             FROM directory_descriptor_rows d \
             LEFT JOIN directory_paths dp \
               ON dp.id = d.id \
              AND dp.lixcol_version_id = d.lixcol_version_id \
             LEFT JOIN lix_version v \
               ON v.id = d.lixcol_version_id \
             WHERE {active_version_scope}",
            active_version_scope = active_version_scope_predicate("d.lixcol_version_id")
        ),
        DIRECTORY_BY_VERSION_VIEW => "WITH RECURSIVE directory_descriptor_rows AS (\
                 SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
                    lix_json_text(snapshot_content, 'hidden') AS hidden, \
                    entity_id AS lixcol_entity_id, \
                    schema_key AS lixcol_schema_key, \
                    schema_version AS lixcol_schema_version, \
                    version_id AS lixcol_version_id, \
                    inherited_from_version_id AS lixcol_inherited_from_version_id, \
                    change_id AS lixcol_change_id, \
                    metadata AS lixcol_metadata, \
                    created_at AS lixcol_created_at, \
                    updated_at AS lixcol_updated_at, \
                    untracked AS lixcol_untracked \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_directory_descriptor' \
                   AND snapshot_content IS NOT NULL\
             ), \
             directory_paths AS (\
                 SELECT \
                    d.id, \
                    d.lixcol_version_id, \
                    '/' || d.name || '/' AS path \
                 FROM directory_descriptor_rows d \
                 WHERE d.parent_id IS NULL \
                 UNION ALL \
                 SELECT \
                    child.id, \
                    child.lixcol_version_id, \
                    parent.path || child.name || '/' AS path \
                 FROM directory_descriptor_rows child \
                 JOIN directory_paths parent \
                   ON parent.id = child.parent_id \
                  AND parent.lixcol_version_id = child.lixcol_version_id\
             ) \
             SELECT \
                d.id, \
                d.parent_id, \
                d.name, \
                dp.path AS path, \
                d.hidden, \
                d.lixcol_entity_id, \
                d.lixcol_schema_key, \
                d.lixcol_schema_version, \
                d.lixcol_version_id, \
                d.lixcol_inherited_from_version_id, \
                d.lixcol_change_id, \
                d.lixcol_created_at, \
                d.lixcol_updated_at, \
                v.commit_id AS lixcol_commit_id, \
                d.lixcol_untracked, \
                d.lixcol_metadata \
             FROM directory_descriptor_rows d \
             LEFT JOIN directory_paths dp \
               ON dp.id = d.id \
              AND dp.lixcol_version_id = d.lixcol_version_id \
             LEFT JOIN lix_version v \
               ON v.id = d.lixcol_version_id"
            .to_string(),
        DIRECTORY_HISTORY_VIEW => "WITH RECURSIVE directory_history_base AS (\
                 SELECT \
                    lix_json_text(snapshot_content, 'id') AS id, \
                    lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_text(snapshot_content, 'name') AS name, \
                    lix_json_text(snapshot_content, 'hidden') AS hidden, \
                    entity_id AS lixcol_entity_id, \
                    schema_key AS lixcol_schema_key, \
                    file_id AS lixcol_file_id, \
                    version_id AS lixcol_version_id, \
                    plugin_key AS lixcol_plugin_key, \
                    schema_version AS lixcol_schema_version, \
                    change_id AS lixcol_change_id, \
                    metadata AS lixcol_metadata, \
                    commit_id AS lixcol_commit_id, \
                    root_commit_id AS lixcol_root_commit_id, \
                    depth AS lixcol_depth \
                 FROM lix_state_history \
                 WHERE schema_key = 'lix_directory_descriptor' \
                   AND snapshot_content IS NOT NULL\
             ), \
             directory_history_path_walk AS (\
                 SELECT \
                    base.id AS target_id, \
                    base.lixcol_root_commit_id AS root_commit_id, \
                    base.lixcol_depth AS target_depth, \
                    base.parent_id AS next_parent_id, \
                    '/' || base.name || '/' AS path, \
                    0 AS step \
                 FROM directory_history_base base \
                 UNION ALL \
                 SELECT \
                    walk.target_id, \
                    walk.root_commit_id, \
                    walk.target_depth, \
                    parent.parent_id AS next_parent_id, \
                    '/' || parent.name || walk.path AS path, \
                    walk.step + 1 AS step \
                 FROM directory_history_path_walk walk \
                 JOIN directory_history_base parent \
                   ON parent.id = walk.next_parent_id \
                  AND parent.lixcol_root_commit_id = walk.root_commit_id \
                  AND parent.lixcol_depth = (\
                      SELECT MIN(candidate.lixcol_depth) \
                      FROM directory_history_base candidate \
                      WHERE candidate.id = walk.next_parent_id \
                        AND candidate.lixcol_root_commit_id = walk.root_commit_id \
                        AND candidate.lixcol_depth >= walk.target_depth\
                  ) \
                 WHERE walk.next_parent_id IS NOT NULL \
                   AND walk.step < 1024 \
             ), \
             directory_history_paths AS (\
                 SELECT \
                    walk.target_id, \
                    walk.root_commit_id, \
                    walk.target_depth, \
                    walk.path \
                 FROM directory_history_path_walk walk \
                 JOIN (\
                    SELECT \
                        target_id, \
                        root_commit_id, \
                        target_depth, \
                        MAX(step) AS max_step \
                    FROM directory_history_path_walk \
                    WHERE next_parent_id IS NULL \
                    GROUP BY target_id, root_commit_id, target_depth\
                 ) terminal \
                   ON terminal.target_id = walk.target_id \
                  AND terminal.root_commit_id = walk.root_commit_id \
                  AND terminal.target_depth = walk.target_depth \
                  AND terminal.max_step = walk.step \
             ) \
             SELECT \
                d.id, \
                d.parent_id, \
                d.name, \
                dp.path AS path, \
                d.hidden, \
                d.lixcol_entity_id, \
                d.lixcol_schema_key, \
                d.lixcol_file_id, \
                d.lixcol_version_id, \
                d.lixcol_plugin_key, \
                d.lixcol_schema_version, \
                d.lixcol_change_id, \
                d.lixcol_metadata, \
                d.lixcol_commit_id, \
                d.lixcol_root_commit_id, \
                d.lixcol_depth \
             FROM directory_history_base d \
             LEFT JOIN directory_history_paths dp \
               ON dp.target_id = d.id \
              AND dp.root_commit_id = d.lixcol_root_commit_id \
              AND dp.target_depth = d.lixcol_depth"
            .to_string(),
        _ => return Ok(None),
    };

    Ok(Some(parse_single_query(&sql)?))
}

fn active_version_scope_predicate(version_column: &str) -> String {
    format!(
        "{version_column} IN (\
         SELECT lix_json_text(snapshot_content, 'version_id') \
         FROM lix_internal_state_untracked \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{storage_version_id}' \
           AND snapshot_content IS NOT NULL\
         )",
        version_column = version_column,
        schema_key = escape_sql_string(active_version_schema_key()),
        file_id = escape_sql_string(active_version_file_id()),
        storage_version_id = escape_sql_string(active_version_storage_version_id()),
    )
}

fn parse_single_query(sql: &str) -> Result<Query, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        message: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single SELECT statement".to_string(),
        });
    }
    let statement = statements.remove(0);
    match statement {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            message: "expected SELECT statement".to_string(),
        }),
    }
}

fn object_name_terminal(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
}

fn default_alias(view_name: &str) -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(view_name),
        columns: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{rewrite_query, rewrite_query_with_params};
    use crate::sql::parse_sql_statements;
    use crate::Value;

    #[test]
    fn rewrites_file_view_reads_to_descriptor_projection() {
        let sql = "SELECT id, data FROM lix_file WHERE path = '/src/index.ts'";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query(query)
            .expect("rewrite")
            .expect("query should be rewritten")
            .to_string();

        assert!(rewritten.contains("COALESCE(fd.data, lix_empty_blob()) AS data"));
        assert!(rewritten.contains("FROM lix_state_by_version"));
        assert!(rewritten.contains("schema_key = 'lix_file_descriptor'"));
        assert!(rewritten.contains("FROM lix_internal_state_untracked"));
    }

    #[test]
    fn rewrites_simple_file_path_data_query_to_projection_fast_path() {
        let sql = "SELECT path, data FROM lix_file ORDER BY path";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query(query)
            .expect("rewrite")
            .expect("query should be rewritten")
            .to_string();

        assert!(rewritten.contains("COALESCE(fd.data, lix_empty_blob()) AS data"));
        assert!(rewritten.contains("FROM lix_state_by_version"));
        assert!(rewritten.contains("schema_key = 'lix_file_descriptor'"));
        assert!(
            !rewritten.contains("LEFT JOIN lix_version v"),
            "fast path should skip commit lookup join"
        );
    }

    #[test]
    fn rewrites_simple_file_path_data_point_query_with_param_fast_path() {
        let sql = "SELECT path, data FROM lix_file WHERE path = ?";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query_with_params(
            query,
            &[Value::Text("/bench/read/07/file-00007.txt".to_string())],
        )
        .expect("rewrite")
        .expect("query should be rewritten")
        .to_string();

        assert!(rewritten.contains("'/bench/read/07/file-00007.txt' AS path"));
        assert!(rewritten.contains("dir_level_0"));
        assert!(rewritten.contains("dir_level_1"));
        assert!(rewritten.contains("lix_json_text(snapshot_content, 'name') = 'file-00007'"));
        assert!(
            !rewritten.contains("directory_paths"),
            "point fast path should avoid recursive directory path projection"
        );
    }

    #[test]
    fn leaves_non_filesystem_reads_untouched() {
        let sql = "SELECT * FROM lix_state";
        let statements = parse_sql_statements(sql).expect("parse");
        let query = match statements.into_iter().next().expect("statement") {
            sqlparser::ast::Statement::Query(query) => *query,
            _ => panic!("expected query"),
        };
        let rewritten = rewrite_query(query).expect("rewrite");
        assert!(rewritten.is_none());
    }
}
