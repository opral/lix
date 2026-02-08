use serde_json::Value as JsonValue;
use sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, Query, Select, SetExpr, Statement, TableAlias, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::sql::escape_sql_string;
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    version_descriptor_file_id, version_descriptor_schema_key,
    version_descriptor_storage_version_id,
};
use crate::{LixBackend, LixError};

use super::target::{
    resolve_target_from_object_name, resolve_targets_with_backend, EntityViewTarget,
    EntityViewVariant,
};

pub(crate) fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_resolver(query, &mut |name| resolve_target_from_object_name(name))
}

pub(crate) async fn rewrite_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
) -> Result<Option<Query>, LixError> {
    let view_names = collect_table_view_names(&query);
    if view_names.is_empty() {
        return Ok(None);
    }
    let resolved = resolve_targets_with_backend(backend, &view_names).await?;
    rewrite_query_with_resolver(query, &mut |name| {
        let Some(view_name) = object_name_terminal(name) else {
            return Ok(None);
        };
        let key = view_name.to_ascii_lowercase();
        Ok(resolved.get(&key).cloned())
    })
}

fn rewrite_query_with_resolver(
    query: Query,
    resolver: &mut dyn FnMut(&ObjectName) -> Result<Option<EntityViewTarget>, LixError>,
) -> Result<Option<Query>, LixError> {
    let mut changed = false;
    let mut new_query = query.clone();
    new_query.body = Box::new(rewrite_set_expr(*query.body, resolver, &mut changed)?);

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

fn rewrite_set_expr(
    expr: SetExpr,
    resolver: &mut dyn FnMut(&ObjectName) -> Result<Option<EntityViewTarget>, LixError>,
    changed: &mut bool,
) -> Result<SetExpr, LixError> {
    Ok(match expr {
        SetExpr::Select(select) => {
            let mut select = *select;
            rewrite_select(&mut select, resolver, changed)?;
            SetExpr::Select(Box::new(select))
        }
        SetExpr::Query(query) => {
            let mut query = *query;
            query.body = Box::new(rewrite_set_expr(*query.body, resolver, changed)?);
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
            left: Box::new(rewrite_set_expr(*left, resolver, changed)?),
            right: Box::new(rewrite_set_expr(*right, resolver, changed)?),
        },
        other => other,
    })
}

fn rewrite_select(
    select: &mut Select,
    resolver: &mut dyn FnMut(&ObjectName) -> Result<Option<EntityViewTarget>, LixError>,
    changed: &mut bool,
) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_table_with_joins(table, resolver, changed)?;
    }
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    resolver: &mut dyn FnMut(&ObjectName) -> Result<Option<EntityViewTarget>, LixError>,
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(&mut table.relation, resolver, changed)?;
    for join in &mut table.joins {
        rewrite_table_factor(&mut join.relation, resolver, changed)?;
    }
    Ok(())
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    resolver: &mut dyn FnMut(&ObjectName) -> Result<Option<EntityViewTarget>, LixError>,
    changed: &mut bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let Some(target) = resolver(name)? else {
                return Ok(());
            };
            let derived_query = build_entity_view_query(&target)?;
            let derived_alias = alias
                .clone()
                .or_else(|| Some(default_alias(&target.view_name)));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            *changed = true;
        }
        TableFactor::Derived { subquery, .. } => {
            if let Some(rewritten) = rewrite_query_with_resolver((**subquery).clone(), resolver)? {
                *subquery = Box::new(rewritten);
                *changed = true;
            }
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_with_joins(table_with_joins, resolver, changed)?;
        }
        _ => {}
    }
    Ok(())
}

fn build_entity_view_query(target: &EntityViewTarget) -> Result<Query, LixError> {
    let (source_sql, extra_predicates) = match target.variant {
        EntityViewVariant::Base => (
            base_state_source_sql(target.version_id_override.as_deref()),
            vec!["1=1".to_string()],
        ),
        EntityViewVariant::ByVersion => {
            ("lix_state_by_version".to_string(), vec!["1=1".to_string()])
        }
        EntityViewVariant::History => ("lix_state_history".to_string(), vec!["1=1".to_string()]),
    };
    let mut select_parts = Vec::new();
    for property in &target.properties {
        select_parts.push(format!(
            "lix_json_text(snapshot_content, '{property}') AS {alias}",
            property = escape_sql_string(property),
            alias = quote_ident(property),
        ));
    }
    for (column, alias) in lixcol_aliases_for_variant(target.variant) {
        select_parts.push(format!("{column} AS {alias}"));
    }
    let mut predicates = vec![format!(
        "schema_key = '{schema_key}'",
        schema_key = escape_sql_string(&target.schema_key)
    )];
    predicates.extend(extra_predicates);
    predicates.extend(override_predicates(target));

    let sql = format!(
        "SELECT {projection} \
         FROM {source} \
         WHERE {predicate}",
        projection = select_parts.join(", "),
        source = source_sql,
        predicate = predicates.join(" AND "),
    );
    parse_single_query(&sql)
}

fn base_state_source_sql(version_id_override: Option<&str>) -> String {
    let descriptor_table = quote_ident(&format!(
        "lix_internal_state_materialized_v1_{}",
        version_descriptor_schema_key()
    ));
    let active_version_seed_sql = match version_id_override {
        Some(version_id) => format!(
            "SELECT '{version_id}' AS version_id",
            version_id = escape_sql_string(version_id)
        ),
        None => format!(
            "SELECT lix_json_text(snapshot_content, 'version_id') AS version_id \
             FROM lix_internal_state_untracked \
             WHERE schema_key = '{active_schema_key}' \
               AND file_id = '{active_file_id}' \
               AND version_id = '{active_storage_version_id}' \
               AND snapshot_content IS NOT NULL \
             ORDER BY updated_at DESC \
             LIMIT 1",
            active_schema_key = escape_sql_string(active_version_schema_key()),
            active_file_id = escape_sql_string(active_version_file_id()),
            active_storage_version_id = escape_sql_string(active_version_storage_version_id()),
        ),
    };
    format!(
        "(SELECT \
             ranked.entity_id AS entity_id, \
             ranked.schema_key AS schema_key, \
             ranked.file_id AS file_id, \
             ranked.version_id AS version_id, \
             ranked.plugin_key AS plugin_key, \
             ranked.snapshot_content AS snapshot_content, \
             ranked.schema_version AS schema_version, \
             ranked.created_at AS created_at, \
             ranked.updated_at AS updated_at, \
             ranked.inherited_from_version_id AS inherited_from_version_id, \
             ranked.change_id AS change_id, \
             ranked.untracked AS untracked, \
             ranked.metadata AS metadata \
         FROM ( \
           WITH RECURSIVE active_version AS ( \
             {active_version_seed_sql} \
           ), \
           version_chain(version_id, depth) AS ( \
             SELECT version_id, 0 AS depth \
             FROM active_version \
             UNION ALL \
             SELECT \
               lix_json_text(vd.snapshot_content, 'inherits_from_version_id') AS version_id, \
               vc.depth + 1 AS depth \
             FROM version_chain vc \
             JOIN {descriptor_table} vd \
               ON lix_json_text(vd.snapshot_content, 'id') = vc.version_id \
             WHERE vd.schema_key = '{descriptor_schema_key}' \
               AND vd.file_id = '{descriptor_file_id}' \
               AND vd.version_id = '{descriptor_storage_version_id}' \
               AND vd.is_tombstone = 0 \
               AND vd.snapshot_content IS NOT NULL \
               AND lix_json_text(vd.snapshot_content, 'inherits_from_version_id') IS NOT NULL \
               AND vc.depth < 64 \
           ) \
           SELECT \
             s.entity_id AS entity_id, \
             s.schema_key AS schema_key, \
             s.file_id AS file_id, \
             av.version_id AS version_id, \
             s.plugin_key AS plugin_key, \
             s.snapshot_content AS snapshot_content, \
             s.schema_version AS schema_version, \
             s.created_at AS created_at, \
             s.updated_at AS updated_at, \
             CASE \
               WHEN s.inherited_from_version_id IS NOT NULL THEN s.inherited_from_version_id \
               WHEN vc.depth = 0 THEN NULL \
               ELSE s.version_id \
             END AS inherited_from_version_id, \
             s.change_id AS change_id, \
             s.untracked AS untracked, \
             s.metadata AS metadata, \
             ROW_NUMBER() OVER ( \
               PARTITION BY s.entity_id, s.schema_key, s.file_id \
               ORDER BY vc.depth ASC \
             ) AS rn \
           FROM lix_internal_state_vtable s \
           JOIN version_chain vc \
             ON vc.version_id = s.version_id \
           CROSS JOIN active_version av \
         ) AS ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL) AS lix_state_base",
        active_version_seed_sql = active_version_seed_sql,
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
    )
}

fn override_predicates(target: &EntityViewTarget) -> Vec<String> {
    target
        .override_predicates
        .iter()
        .map(|predicate| match &predicate.value {
            JsonValue::Null => format!("{column} IS NULL", column = predicate.column),
            value => format!(
                "{column} = {literal}",
                column = predicate.column,
                literal = render_literal(value)
            ),
        })
        .collect()
}

fn render_literal(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(value) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        JsonValue::Number(value) => value.to_string(),
        JsonValue::String(value) => format!("'{}'", escape_sql_string(value)),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            format!("'{}'", escape_sql_string(&value.to_string()))
        }
    }
}

fn lixcol_aliases_for_variant(
    variant: EntityViewVariant,
) -> &'static [(&'static str, &'static str)] {
    match variant {
        EntityViewVariant::Base => &[
            ("entity_id", "lixcol_entity_id"),
            ("schema_key", "lixcol_schema_key"),
            ("file_id", "lixcol_file_id"),
            ("plugin_key", "lixcol_plugin_key"),
            ("schema_version", "lixcol_schema_version"),
            ("created_at", "lixcol_created_at"),
            ("updated_at", "lixcol_updated_at"),
            (
                "inherited_from_version_id",
                "lixcol_inherited_from_version_id",
            ),
            ("change_id", "lixcol_change_id"),
            ("untracked", "lixcol_untracked"),
            ("metadata", "lixcol_metadata"),
        ],
        EntityViewVariant::ByVersion => &[
            ("entity_id", "lixcol_entity_id"),
            ("schema_key", "lixcol_schema_key"),
            ("file_id", "lixcol_file_id"),
            ("version_id", "lixcol_version_id"),
            ("plugin_key", "lixcol_plugin_key"),
            ("schema_version", "lixcol_schema_version"),
            ("created_at", "lixcol_created_at"),
            ("updated_at", "lixcol_updated_at"),
            (
                "inherited_from_version_id",
                "lixcol_inherited_from_version_id",
            ),
            ("change_id", "lixcol_change_id"),
            ("untracked", "lixcol_untracked"),
            ("metadata", "lixcol_metadata"),
        ],
        EntityViewVariant::History => &[
            ("entity_id", "lixcol_entity_id"),
            ("schema_key", "lixcol_schema_key"),
            ("file_id", "lixcol_file_id"),
            ("version_id", "lixcol_version_id"),
            ("plugin_key", "lixcol_plugin_key"),
            ("schema_version", "lixcol_schema_version"),
            ("change_id", "lixcol_change_id"),
            ("metadata", "lixcol_metadata"),
            ("commit_id", "lixcol_commit_id"),
            ("root_commit_id", "lixcol_root_commit_id"),
            ("depth", "lixcol_depth"),
        ],
    }
}

fn default_alias(view_name: &str) -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(view_name),
        columns: Vec::new(),
    }
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

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn collect_table_view_names(query: &Query) -> Vec<String> {
    let mut out = Vec::new();
    collect_set_expr_view_names(query.body.as_ref(), &mut out);
    out
}

fn collect_set_expr_view_names(expr: &SetExpr, out: &mut Vec<String>) {
    match expr {
        SetExpr::Select(select) => {
            for table in &select.from {
                collect_table_factor_view_names(&table.relation, out);
                for join in &table.joins {
                    collect_table_factor_view_names(&join.relation, out);
                }
            }
        }
        SetExpr::Query(query) => collect_set_expr_view_names(query.body.as_ref(), out),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_view_names(left.as_ref(), out);
            collect_set_expr_view_names(right.as_ref(), out);
        }
        _ => {}
    }
}

fn collect_table_factor_view_names(relation: &TableFactor, out: &mut Vec<String>) {
    match relation {
        TableFactor::Table { name, .. } => {
            if let Some(view_name) = object_name_terminal(name) {
                out.push(view_name);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            collect_set_expr_view_names(subquery.body.as_ref(), out);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_table_factor_view_names(&table_with_joins.relation, out);
            for join in &table_with_joins.joins {
                collect_table_factor_view_names(&join.relation, out);
            }
        }
        _ => {}
    }
}

fn object_name_terminal(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
}
