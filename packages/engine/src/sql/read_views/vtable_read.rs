use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgumentList, FunctionArguments,
    GroupByExpr, Ident, ObjectName, ObjectNamePart, OrderByExpr, OrderByOptions, Query, Select,
    SelectFlavor, SelectItem, SetExpr, SetOperator, SetQuantifier, TableAlias, TableFactor,
    TableWithJoins, UnaryOperator, Value, ValueWithSpan, WindowSpec, WindowType,
};
use std::sync::OnceLock;

use crate::backend::SqlDialect;
use crate::sql::planner::catalog::PlannerCatalogSnapshot;
use crate::sql::read_views::state_pushdown::{
    select_projects_count_star, StatePushdown,
};
use crate::sql::{
    escape_sql_string, object_name_matches, parse_expression_with_dialect,
    parse_single_query_with_dialect, quote_ident,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    version_descriptor_file_id, version_descriptor_schema_key,
    version_descriptor_storage_version_id, GLOBAL_VERSION_ID,
};
use crate::LixError;

const VTABLE_NAME: &str = "lix_internal_state_vtable";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";
static EFFECTIVE_STATE_BY_VERSION_VIEW_TEMPLATE_WITH_COMMIT: OnceLock<Query> = OnceLock::new();
static EFFECTIVE_STATE_BY_VERSION_VIEW_TEMPLATE_NO_COMMIT: OnceLock<Query> = OnceLock::new();
static EFFECTIVE_STATE_BY_VERSION_COUNT_TEMPLATE: OnceLock<Query> = OnceLock::new();
static EFFECTIVE_STATE_ACTIVE_VIEW_TEMPLATE_WITH_COMMIT: OnceLock<Query> = OnceLock::new();
static EFFECTIVE_STATE_ACTIVE_VIEW_TEMPLATE_NO_COMMIT: OnceLock<Query> = OnceLock::new();
static EFFECTIVE_STATE_ACTIVE_COUNT_TEMPLATE: OnceLock<Query> = OnceLock::new();

pub(crate) fn build_effective_state_by_version_query(
    pushdown: &StatePushdown,
    count_only: bool,
    include_commit_mapping: bool,
) -> Result<Query, LixError> {
    if count_only {
        build_effective_state_by_version_count_query(pushdown)
    } else {
        build_effective_state_by_version_view_query(pushdown, include_commit_mapping)
    }
}

pub(crate) fn build_effective_state_active_query(
    pushdown: &StatePushdown,
    count_only: bool,
    include_commit_mapping: bool,
) -> Result<Query, LixError> {
    if count_only {
        build_effective_state_active_count_query(pushdown)
    } else {
        build_effective_state_active_view_query(pushdown, include_commit_mapping)
    }
}

fn build_effective_state_by_version_view_query(
    pushdown: &StatePushdown,
    include_commit_mapping: bool,
) -> Result<Query, LixError> {
    let mut query = effective_state_by_version_view_template(include_commit_mapping);
    append_effective_state_by_version_predicates(
        &mut query,
        &pushdown.source_predicates,
        &pushdown
            .ranked_predicates
            .iter()
            .map(|predicate| predicate.ranked_sql.clone())
            .collect::<Vec<_>>(),
    )?;
    Ok(query)
}

fn build_effective_state_by_version_count_query(
    pushdown: &StatePushdown,
) -> Result<Query, LixError> {
    let mut query = effective_state_by_version_count_template();
    append_effective_state_by_version_predicates(
        &mut query,
        &pushdown.source_predicates,
        &pushdown
            .ranked_predicates
            .iter()
            .map(|predicate| predicate.ranked_sql.clone())
            .collect::<Vec<_>>(),
    )?;
    Ok(query)
}

fn effective_state_by_version_view_template(include_commit_mapping: bool) -> Query {
    let template = if include_commit_mapping {
        EFFECTIVE_STATE_BY_VERSION_VIEW_TEMPLATE_WITH_COMMIT.get_or_init(|| {
            let sql = build_effective_state_by_version_view_template_sql(true);
            parse_single_query_with_dialect(&sql, SqlDialect::Sqlite)
                .expect("effective state by-version template with commit maps")
        })
    } else {
        EFFECTIVE_STATE_BY_VERSION_VIEW_TEMPLATE_NO_COMMIT.get_or_init(|| {
            let sql = build_effective_state_by_version_view_template_sql(false);
            parse_single_query_with_dialect(&sql, SqlDialect::Sqlite)
                .expect("effective state by-version template without commits")
        })
    };
    template.clone()
}

fn effective_state_by_version_count_template() -> Query {
    EFFECTIVE_STATE_BY_VERSION_COUNT_TEMPLATE
        .get_or_init(|| {
            let sql = build_effective_state_by_version_count_template_sql();
            parse_single_query_with_dialect(&sql, SqlDialect::Sqlite)
                .expect("effective state by-version count template")
        })
        .clone()
}

fn build_effective_state_by_version_view_template_sql(include_commit_mapping: bool) -> String {
    let descriptor_table = quote_ident(&format!(
        "lix_internal_state_materialized_v1_{}",
        version_descriptor_schema_key()
    ));
    let target_versions_cte = build_effective_state_all_target_versions_cte(VTABLE_NAME);
    let commit_ctes = if include_commit_mapping {
        format!(
            ", \
             commit_by_version AS ( \
               SELECT \
                 COALESCE(lix_json_text(snapshot_content, 'id'), entity_id) AS commit_id, \
                 lix_json_text(snapshot_content, 'change_set_id') AS change_set_id \
               FROM {vtable_name} \
               WHERE schema_key = 'lix_commit' \
                 AND version_id = '{global_version}' \
                 AND snapshot_content IS NOT NULL \
             ), \
             change_set_element_by_version AS ( \
               SELECT \
                 lix_json_text(snapshot_content, 'change_set_id') AS change_set_id, \
                 lix_json_text(snapshot_content, 'change_id') AS change_id \
               FROM {vtable_name} \
               WHERE schema_key = 'lix_change_set_element' \
                 AND version_id = '{global_version}' \
                 AND snapshot_content IS NOT NULL \
             ), \
             change_commit_by_change_id AS ( \
               SELECT \
                 cse.change_id AS change_id, \
                 MAX(cbv.commit_id) AS commit_id \
               FROM change_set_element_by_version cse \
               JOIN commit_by_version cbv \
                 ON cbv.change_set_id = cse.change_set_id \
               WHERE cse.change_id IS NOT NULL \
               GROUP BY cse.change_id \
             )",
            vtable_name = VTABLE_NAME,
            global_version = escape_sql_string(GLOBAL_VERSION_ID),
        )
    } else {
        String::new()
    };
    let commit_join = if include_commit_mapping {
        "LEFT JOIN change_commit_by_change_id cc \
             ON cc.change_id = s.change_id"
            .to_string()
    } else {
        String::new()
    };
    let commit_expr = if include_commit_mapping {
        "COALESCE(cc.commit_id, CASE WHEN s.untracked = 1 THEN 'untracked' ELSE NULL END) \
             AS commit_id"
            .to_string()
    } else {
        "CASE WHEN s.untracked = 1 THEN 'untracked' ELSE NULL END AS commit_id".to_string()
    };
    format!(
        "SELECT \
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
             ranked.commit_id AS commit_id, \
             ranked.untracked AS untracked, \
             ranked.writer_key AS writer_key, \
             ranked.metadata AS metadata \
         FROM ( \
           WITH RECURSIVE \
             version_descriptor AS ( \
               SELECT \
                 lix_json_text(snapshot_content, 'id') AS version_id, \
                 lix_json_text(snapshot_content, 'inherits_from_version_id') AS inherits_from_version_id \
               FROM {descriptor_table} \
               WHERE schema_key = '{descriptor_schema_key}' \
                 AND file_id = '{descriptor_file_id}' \
                 AND version_id = '{descriptor_storage_version_id}' \
                 AND is_tombstone = 0 \
                 AND snapshot_content IS NOT NULL \
             ), \
             {target_versions_cte}, \
             version_chain(target_version_id, ancestor_version_id, depth) AS ( \
               SELECT \
                 version_id AS target_version_id, \
                 version_id AS ancestor_version_id, \
                 0 AS depth \
               FROM target_versions \
               UNION ALL \
               SELECT \
                 vc.target_version_id, \
                 vd.inherits_from_version_id AS ancestor_version_id, \
                 vc.depth + 1 AS depth \
               FROM version_chain vc \
               JOIN version_descriptor vd \
                 ON vd.version_id = vc.ancestor_version_id \
               WHERE vd.inherits_from_version_id IS NOT NULL \
                 AND vc.depth < 64 \
             ) \
             {commit_ctes} \
           SELECT \
             s.entity_id AS entity_id, \
             s.schema_key AS schema_key, \
             s.file_id AS file_id, \
             vc.target_version_id AS version_id, \
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
             {commit_expr}, \
             s.untracked AS untracked, \
             s.writer_key AS writer_key, \
             s.metadata AS metadata, \
             ROW_NUMBER() OVER ( \
               PARTITION BY vc.target_version_id, s.entity_id, s.schema_key, s.file_id \
               ORDER BY vc.depth ASC \
             ) AS rn \
           FROM {vtable_name} s \
           JOIN version_chain vc \
             ON vc.ancestor_version_id = s.version_id \
           {commit_join} \
         ) AS ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL",
        descriptor_table = descriptor_table,
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
        target_versions_cte = target_versions_cte,
        vtable_name = VTABLE_NAME,
        commit_ctes = commit_ctes,
        commit_expr = commit_expr,
        commit_join = commit_join,
    )
}

fn build_effective_state_by_version_count_template_sql() -> String {
    let descriptor_table = quote_ident(&format!(
        "lix_internal_state_materialized_v1_{}",
        version_descriptor_schema_key()
    ));
    let target_versions_cte = build_effective_state_all_target_versions_cte(VTABLE_NAME);
    format!(
        "SELECT \
             ranked.entity_id AS entity_id \
         FROM ( \
           WITH RECURSIVE \
             version_descriptor AS ( \
               SELECT \
                 lix_json_text(snapshot_content, 'id') AS version_id, \
                 lix_json_text(snapshot_content, 'inherits_from_version_id') AS inherits_from_version_id \
               FROM {descriptor_table} \
               WHERE schema_key = '{descriptor_schema_key}' \
                 AND file_id = '{descriptor_file_id}' \
                 AND version_id = '{descriptor_storage_version_id}' \
                 AND is_tombstone = 0 \
                 AND snapshot_content IS NOT NULL \
             ), \
             {target_versions_cte}, \
             version_chain(target_version_id, ancestor_version_id, depth) AS ( \
               SELECT \
                 version_id AS target_version_id, \
                 version_id AS ancestor_version_id, \
                 0 AS depth \
               FROM target_versions \
               UNION ALL \
               SELECT \
                 vc.target_version_id, \
                 vd.inherits_from_version_id AS ancestor_version_id, \
                 vc.depth + 1 AS depth \
               FROM version_chain vc \
               JOIN version_descriptor vd \
                 ON vd.version_id = vc.ancestor_version_id \
               WHERE vd.inherits_from_version_id IS NOT NULL \
                 AND vc.depth < 64 \
             ) \
           SELECT \
             s.entity_id AS entity_id, \
             s.schema_key AS schema_key, \
             s.file_id AS file_id, \
             vc.target_version_id AS version_id, \
             s.plugin_key AS plugin_key, \
             s.snapshot_content AS snapshot_content, \
             ROW_NUMBER() OVER ( \
               PARTITION BY vc.target_version_id, s.entity_id, s.schema_key, s.file_id \
               ORDER BY vc.depth ASC \
             ) AS rn \
           FROM {vtable_name} s \
           JOIN version_chain vc \
             ON vc.ancestor_version_id = s.version_id \
         ) AS ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL",
        descriptor_table = descriptor_table,
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
        target_versions_cte = target_versions_cte,
        vtable_name = VTABLE_NAME,
    )
}

fn build_effective_state_all_target_versions_cte(vtable_name: &str) -> String {
    format!(
        "all_target_versions AS ( \
           SELECT version_id FROM version_descriptor \
           UNION \
           SELECT DISTINCT version_id FROM {vtable_name} \
         ), \
         target_versions AS ( \
           SELECT version_id \
           FROM all_target_versions \
         )",
        vtable_name = vtable_name
    )
}

fn append_effective_state_by_version_predicates(
    query: &mut Query,
    source_predicates: &[String],
    ranked_predicates: &[String],
) -> Result<(), LixError> {
    let outer_select = select_from_query_mut(query, "effective state by-version outer query")?;
    append_predicates_from_sql(&mut outer_select.selection, ranked_predicates)?;

    let Some(first_from) = outer_select.from.first_mut() else {
        return Err(LixError {
            message: "effective state by-version query missing outer FROM".to_string(),
        });
    };
    let TableFactor::Derived { subquery, .. } = &mut first_from.relation else {
        return Err(LixError {
            message: "effective state by-version query expected derived outer relation".to_string(),
        });
    };
    let inner_select = select_from_query_mut(
        subquery.as_mut(),
        "effective state by-version recursive inner query",
    )?;
    append_predicates_from_sql(&mut inner_select.selection, source_predicates)
}

fn select_from_query_mut<'a>(query: &'a mut Query, label: &str) -> Result<&'a mut Select, LixError> {
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err(LixError {
            message: format!("expected SELECT body for {label}"),
        });
    };
    Ok(select.as_mut())
}

fn append_predicates_from_sql(
    selection: &mut Option<Expr>,
    predicates: &[String],
) -> Result<(), LixError> {
    for predicate_sql in predicates {
        let predicate = parse_expression_with_dialect(predicate_sql, SqlDialect::Sqlite)?;
        let next = match selection.take() {
            Some(existing) => Expr::BinaryOp {
                left: Box::new(existing),
                op: BinaryOperator::And,
                right: Box::new(predicate),
            },
            None => predicate,
        };
        *selection = Some(next);
    }
    Ok(())
}

fn build_effective_state_active_view_query(
    pushdown: &StatePushdown,
    include_commit_mapping: bool,
) -> Result<Query, LixError> {
    let mut query = effective_state_active_view_template(include_commit_mapping);
    append_effective_state_read_predicates(
        &mut query,
        &pushdown.source_predicates,
        &pushdown
            .ranked_predicates
            .iter()
            .map(|predicate| predicate.ranked_sql.clone())
            .collect::<Vec<_>>(),
        "active",
    )?;
    Ok(query)
}

fn build_effective_state_active_view_template_sql(include_commit_mapping: bool) -> String {
    let descriptor_table = quote_ident(&format!(
        "lix_internal_state_materialized_v1_{}",
        version_descriptor_schema_key()
    ));
    let commit_ctes = if include_commit_mapping {
        format!(
            ", \
           commit_by_version AS ( \
             SELECT \
               COALESCE(lix_json_text(snapshot_content, 'id'), entity_id) AS commit_id, \
               lix_json_text(snapshot_content, 'change_set_id') AS change_set_id \
             FROM {vtable_name} \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND snapshot_content IS NOT NULL \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               lix_json_text(snapshot_content, 'change_set_id') AS change_set_id, \
               lix_json_text(snapshot_content, 'change_id') AS change_id \
             FROM {vtable_name} \
             WHERE schema_key = 'lix_change_set_element' \
               AND version_id = '{global_version}' \
               AND snapshot_content IS NOT NULL \
           ), \
           change_commit_by_change_id AS ( \
             SELECT \
               cse.change_id AS change_id, \
               MAX(cbv.commit_id) AS commit_id \
             FROM change_set_element_by_version cse \
             JOIN commit_by_version cbv \
               ON cbv.change_set_id = cse.change_set_id \
             WHERE cse.change_id IS NOT NULL \
             GROUP BY cse.change_id \
           )",
            vtable_name = VTABLE_NAME,
            global_version = escape_sql_string(GLOBAL_VERSION_ID),
        )
    } else {
        String::new()
    };
    let commit_join = if include_commit_mapping {
        "LEFT JOIN change_commit_by_change_id cc \
             ON cc.change_id = s.change_id"
            .to_string()
    } else {
        String::new()
    };
    let commit_expr = if include_commit_mapping {
        "COALESCE(cc.commit_id, CASE WHEN s.untracked = 1 THEN 'untracked' ELSE NULL END) \
             AS commit_id"
            .to_string()
    } else {
        "CASE WHEN s.untracked = 1 THEN 'untracked' ELSE NULL END AS commit_id".to_string()
    };
    format!(
        "SELECT \
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
             ranked.commit_id AS commit_id, \
             ranked.untracked AS untracked, \
             ranked.writer_key AS writer_key, \
             ranked.metadata AS metadata \
         FROM ( \
           WITH RECURSIVE active_version AS ( \
             SELECT lix_json_text(snapshot_content, 'version_id') AS version_id \
             FROM lix_internal_state_untracked \
             WHERE schema_key = '{active_schema_key}' \
               AND file_id = '{active_file_id}' \
               AND version_id = '{active_storage_version_id}' \
               AND snapshot_content IS NOT NULL \
             ORDER BY updated_at DESC \
             LIMIT 1 \
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
           {commit_ctes} \
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
             {commit_expr}, \
             s.untracked AS untracked, \
             s.writer_key AS writer_key, \
             s.metadata AS metadata, \
             ROW_NUMBER() OVER ( \
               PARTITION BY s.entity_id, s.schema_key, s.file_id \
               ORDER BY vc.depth ASC \
             ) AS rn \
           FROM {vtable_name} s \
           JOIN version_chain vc \
             ON vc.version_id = s.version_id \
           {commit_join} \
           CROSS JOIN active_version av \
         ) AS ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL",
        active_schema_key = escape_sql_string(active_version_schema_key()),
        active_file_id = escape_sql_string(active_version_file_id()),
        active_storage_version_id = escape_sql_string(active_version_storage_version_id()),
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
        vtable_name = VTABLE_NAME,
        commit_ctes = commit_ctes,
        commit_expr = commit_expr,
        commit_join = commit_join,
    )
}

fn effective_state_active_view_template(include_commit_mapping: bool) -> Query {
    let template = if include_commit_mapping {
        EFFECTIVE_STATE_ACTIVE_VIEW_TEMPLATE_WITH_COMMIT.get_or_init(|| {
            let sql = build_effective_state_active_view_template_sql(true);
            parse_single_query_with_dialect(&sql, SqlDialect::Sqlite)
                .expect("effective state active template with commit maps")
        })
    } else {
        EFFECTIVE_STATE_ACTIVE_VIEW_TEMPLATE_NO_COMMIT.get_or_init(|| {
            let sql = build_effective_state_active_view_template_sql(false);
            parse_single_query_with_dialect(&sql, SqlDialect::Sqlite)
                .expect("effective state active template without commits")
        })
    };
    template.clone()
}

fn build_effective_state_active_count_query(pushdown: &StatePushdown) -> Result<Query, LixError> {
    let mut query = effective_state_active_count_template();
    append_effective_state_read_predicates(
        &mut query,
        &pushdown.source_predicates,
        &pushdown
            .ranked_predicates
            .iter()
            .map(|predicate| predicate.ranked_sql.clone())
            .collect::<Vec<_>>(),
        "active count",
    )?;
    Ok(query)
}

fn build_effective_state_active_count_template_sql() -> String {
    let descriptor_table = quote_ident(&format!(
        "lix_internal_state_materialized_v1_{}",
        version_descriptor_schema_key()
    ));
    format!(
        "SELECT \
             ranked.entity_id AS entity_id \
         FROM ( \
           WITH RECURSIVE active_version AS ( \
             SELECT lix_json_text(snapshot_content, 'version_id') AS version_id \
             FROM lix_internal_state_untracked \
             WHERE schema_key = '{active_schema_key}' \
               AND file_id = '{active_file_id}' \
               AND version_id = '{active_storage_version_id}' \
               AND snapshot_content IS NOT NULL \
             ORDER BY updated_at DESC \
             LIMIT 1 \
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
             ROW_NUMBER() OVER ( \
               PARTITION BY s.entity_id, s.schema_key, s.file_id \
               ORDER BY vc.depth ASC \
             ) AS rn \
           FROM {vtable_name} s \
           JOIN version_chain vc \
             ON vc.version_id = s.version_id \
           CROSS JOIN active_version av \
         ) AS ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL",
        active_schema_key = escape_sql_string(active_version_schema_key()),
        active_file_id = escape_sql_string(active_version_file_id()),
        active_storage_version_id = escape_sql_string(active_version_storage_version_id()),
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
        vtable_name = VTABLE_NAME,
    )
}

fn effective_state_active_count_template() -> Query {
    EFFECTIVE_STATE_ACTIVE_COUNT_TEMPLATE
        .get_or_init(|| {
            let sql = build_effective_state_active_count_template_sql();
            parse_single_query_with_dialect(&sql, SqlDialect::Sqlite)
                .expect("effective state active count template")
        })
        .clone()
}

fn append_effective_state_read_predicates(
    query: &mut Query,
    source_predicates: &[String],
    ranked_predicates: &[String],
    label: &str,
) -> Result<(), LixError> {
    let outer_select = select_from_query_mut(query, "effective state outer query")?;
    append_predicates_from_sql(&mut outer_select.selection, ranked_predicates)?;

    let Some(first_from) = outer_select.from.first_mut() else {
        return Err(LixError {
            message: format!("effective state {label} query missing outer FROM"),
        });
    };
    let TableFactor::Derived { subquery, .. } = &mut first_from.relation else {
        return Err(LixError {
            message: format!("effective state {label} query expected derived outer relation"),
        });
    };
    let inner_select = select_from_query_mut(subquery.as_mut(), "effective state inner query")?;
    append_predicates_from_sql(&mut inner_select.selection, source_predicates)
}

pub fn rewrite_query_with_catalog(
    query: Query,
    catalog: &PlannerCatalogSnapshot,
) -> Result<Option<Query>, LixError> {
    let mut schema_keys = extract_schema_keys_from_query(&query).unwrap_or_default();
    if schema_keys.is_empty() {
        let plugin_keys = extract_plugin_keys_from_query(&query)
            .or_else(|| extract_plugin_keys_from_top_level_derived_subquery(&query))
            .unwrap_or_default();
        if !plugin_keys.is_empty() {
            schema_keys = catalog.schema_keys_for_plugins(&plugin_keys);
        }
    }
    if schema_keys.is_empty() {
        schema_keys = catalog.materialized_schema_keys.clone();
    }

    let mut changed = false;
    let mut new_query = query.clone();
    rewrite_query_inner(&mut new_query, &schema_keys, &mut changed)?;

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

fn rewrite_query_inner(
    query: &mut Query,
    schema_keys: &[String],
    changed: &mut bool,
) -> Result<(), LixError> {
    let query_schema_keys = resolve_schema_keys_for_query(query, schema_keys);
    let top_level_targets_vtable = query_targets_vtable(&query);
    let pushdown_predicate = if top_level_targets_vtable {
        extract_pushdown_predicate(&query)
    } else {
        None
    };

    if let Some(with) = query.with.as_mut() {
        for cte in &mut with.cte_tables {
            rewrite_query_inner(&mut cte.query, &query_schema_keys, changed)?;
        }
    }
    query.body = Box::new(rewrite_set_expr(
        (*query.body).clone(),
        &query_schema_keys,
        pushdown_predicate.as_ref(),
        changed,
    )?);
    Ok(())
}

fn rewrite_set_expr(
    expr: SetExpr,
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    changed: &mut bool,
) -> Result<SetExpr, LixError> {
    Ok(match expr {
        SetExpr::Select(select) => {
            let mut select = *select;
            rewrite_select(&mut select, schema_keys, pushdown_predicate, changed)?;
            SetExpr::Select(Box::new(select))
        }
        SetExpr::Query(query) => {
            let mut query = *query;
            rewrite_query_inner(&mut query, schema_keys, changed)?;
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
            left: Box::new(rewrite_set_expr(
                *left,
                schema_keys,
                pushdown_predicate,
                changed,
            )?),
            right: Box::new(rewrite_set_expr(
                *right,
                schema_keys,
                pushdown_predicate,
                changed,
            )?),
        },
        other => other,
    })
}

fn rewrite_select(
    select: &mut Select,
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    changed: &mut bool,
) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_table_with_joins(table, schema_keys, pushdown_predicate, changed)?;
    }
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(
        &mut table.relation,
        schema_keys,
        pushdown_predicate,
        changed,
    )?;
    for join in &mut table.joins {
        rewrite_table_factor(&mut join.relation, schema_keys, pushdown_predicate, changed)?;
    }
    Ok(())
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    changed: &mut bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if !schema_keys.is_empty() && object_name_matches(name, VTABLE_NAME) =>
        {
            let derived_query = build_untracked_union_query(schema_keys, pushdown_predicate)?;
            let derived_alias = alias.clone().or_else(|| Some(default_vtable_alias()));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            *changed = true;
        }
        TableFactor::Derived { subquery, .. } => {
            let mut subquery_changed = false;
            let mut rewritten_subquery = (**subquery).clone();
            rewrite_query_inner(&mut rewritten_subquery, schema_keys, &mut subquery_changed)?;
            if subquery_changed {
                *subquery = Box::new(rewritten_subquery);
                *changed = true;
            }
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_with_joins(table_with_joins, schema_keys, pushdown_predicate, changed)?;
        }
        _ => {}
    }
    Ok(())
}

fn build_untracked_union_query(
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
) -> Result<Query, LixError> {
    let stripped_predicate = pushdown_predicate.and_then(|expr| strip_qualifiers(expr.clone()));
    let predicate_schema_keys = stripped_predicate
        .as_ref()
        .and_then(|expr| extract_column_keys_from_expr(expr, expr_is_schema_key_column));
    let effective_schema_keys = narrow_schema_keys(schema_keys, predicate_schema_keys.as_deref());
    let mut union_branches = Vec::<Query>::new();

    let schema_filter_expr = if effective_schema_keys.is_empty() {
        None
    } else {
        Some(Expr::InList {
            expr: Box::new(expr_ident("schema_key")),
            list: effective_schema_keys
                .iter()
                .map(|key| expr_string(key))
                .collect::<Vec<_>>(),
            negated: false,
        })
    };

    let untracked_selection = and_exprs(
        schema_filter_expr
            .into_iter()
            .chain(stripped_predicate.clone())
            .collect(),
    )
    .unwrap_or_else(|| Expr::BinaryOp {
        left: Box::new(expr_int(1)),
        op: BinaryOperator::Eq,
        right: Box::new(expr_int(1)),
    });

    union_branches.push(select_query_from_parts(
        union_projection_untracked(),
        vec![table_with_joins_for(UNTRACKED_TABLE)],
        Some(untracked_selection),
    ));

    for key in &effective_schema_keys {
        let materialized_table = format!("{MATERIALIZED_PREFIX}{key}");
        let mut materialized_filters = vec![
            Expr::BinaryOp {
                left: Box::new(expr_ident("is_tombstone")),
                op: BinaryOperator::Eq,
                right: Box::new(expr_int(0)),
            },
            Expr::BinaryOp {
                left: Box::new(expr_ident("schema_key")),
                op: BinaryOperator::Eq,
                right: Box::new(expr_string(key)),
            },
            Expr::IsNotNull(Box::new(expr_ident("snapshot_content"))),
        ];
        if let Some(predicate) = stripped_predicate.as_ref() {
            materialized_filters.push(predicate.clone());
        }

        union_branches.push(select_query_from_parts(
            union_projection_materialized(),
            vec![table_with_joins_for(&materialized_table)],
            and_exprs(materialized_filters),
        ));
    }

    let mut union_expr = (*union_branches.remove(0).body).clone();
    for branch in union_branches {
        union_expr = SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: SetQuantifier::All,
            left: Box::new(union_expr),
            right: Box::new(*branch.body),
        };
    }

    let union_query = Query {
        with: None,
        body: Box::new(union_expr),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };

    let ranked_query = select_query_from_parts(
        ranked_projection_with_row_number(),
        vec![TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Box::new(union_query),
                alias: Some(explicit_alias("lix_state_union")),
            },
            joins: Vec::new(),
        }],
        None,
    );

    Ok(select_query_from_parts(
        ranked_projection_without_row_number(),
        vec![TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Box::new(ranked_query),
                alias: Some(explicit_alias("lix_state_ranked")),
            },
            joins: Vec::new(),
        }],
        Some(Expr::BinaryOp {
            left: Box::new(expr_ident("rn")),
            op: BinaryOperator::Eq,
            right: Box::new(expr_int(1)),
        }),
    ))
}

fn select_query_from_parts(
    projection: Vec<SelectItem>,
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
) -> Query {
    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection,
            exclude: None,
            into: None,
            from,
            lateral_views: Vec::new(),
            prewhere: None,
            selection,
            group_by: GroupByExpr::Expressions(Vec::new(), Vec::new()),
            cluster_by: Vec::new(),
            distribute_by: Vec::new(),
            sort_by: Vec::new(),
            having: None,
            named_window: Vec::new(),
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            connect_by: None,
            flavor: SelectFlavor::Standard,
        }))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    }
}

fn table_with_joins_for(table: &str) -> TableWithJoins {
    TableWithJoins {
        relation: TableFactor::Table {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(table))]),
            alias: None,
            args: None,
            with_hints: Vec::new(),
            version: None,
            with_ordinality: false,
            partitions: Vec::new(),
            json_path: None,
            sample: None,
            index_hints: Vec::new(),
        },
        joins: Vec::new(),
    }
}

fn union_projection_untracked() -> Vec<SelectItem> {
    vec![
        select_ident("entity_id"),
        select_ident("schema_key"),
        select_ident("file_id"),
        select_ident("version_id"),
        select_ident("plugin_key"),
        select_ident("snapshot_content"),
        select_ident("metadata"),
        select_ident("schema_version"),
        select_ident("created_at"),
        select_ident("updated_at"),
        select_alias(expr_null(), "inherited_from_version_id"),
        select_alias(expr_string("untracked"), "change_id"),
        select_alias(expr_null(), "writer_key"),
        select_alias(expr_int(1), "untracked"),
        select_alias(expr_int(1), "priority"),
    ]
}

fn union_projection_materialized() -> Vec<SelectItem> {
    vec![
        select_ident("entity_id"),
        select_ident("schema_key"),
        select_ident("file_id"),
        select_ident("version_id"),
        select_ident("plugin_key"),
        select_ident("snapshot_content"),
        select_ident("metadata"),
        select_ident("schema_version"),
        select_ident("created_at"),
        select_ident("updated_at"),
        select_ident("inherited_from_version_id"),
        select_ident("change_id"),
        select_ident("writer_key"),
        select_alias(expr_int(0), "untracked"),
        select_alias(expr_int(2), "priority"),
    ]
}

fn ranked_projection_with_row_number() -> Vec<SelectItem> {
    vec![
        select_ident("entity_id"),
        select_ident("schema_key"),
        select_ident("file_id"),
        select_ident("version_id"),
        select_ident("plugin_key"),
        select_ident("snapshot_content"),
        select_ident("metadata"),
        select_ident("schema_version"),
        select_ident("created_at"),
        select_ident("updated_at"),
        select_ident("inherited_from_version_id"),
        select_ident("change_id"),
        select_ident("writer_key"),
        select_ident("untracked"),
        select_alias(row_number_partitioned_expr(), "rn"),
    ]
}

fn ranked_projection_without_row_number() -> Vec<SelectItem> {
    vec![
        select_ident("entity_id"),
        select_ident("schema_key"),
        select_ident("file_id"),
        select_ident("version_id"),
        select_ident("plugin_key"),
        select_ident("snapshot_content"),
        select_ident("metadata"),
        select_ident("schema_version"),
        select_ident("created_at"),
        select_ident("updated_at"),
        select_ident("inherited_from_version_id"),
        select_ident("change_id"),
        select_ident("writer_key"),
        select_ident("untracked"),
    ]
}

fn row_number_partitioned_expr() -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("ROW_NUMBER"))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: Vec::<FunctionArg>::new(),
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: Some(WindowType::WindowSpec(WindowSpec {
            window_name: None,
            partition_by: vec![
                expr_ident("entity_id"),
                expr_ident("schema_key"),
                expr_ident("file_id"),
                expr_ident("version_id"),
            ],
            order_by: vec![OrderByExpr {
                expr: expr_ident("priority"),
                options: OrderByOptions::default(),
                with_fill: None,
            }],
            window_frame: None,
        })),
        within_group: Vec::new(),
    })
}

fn select_ident(name: &str) -> SelectItem {
    SelectItem::UnnamedExpr(expr_ident(name))
}

fn select_alias(expr: Expr, alias: &str) -> SelectItem {
    SelectItem::ExprWithAlias {
        expr,
        alias: Ident::new(alias),
    }
}

fn expr_ident(name: &str) -> Expr {
    Expr::Identifier(Ident::new(name))
}

fn expr_string(value: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(value.to_string()).into())
}

fn expr_int(value: i64) -> Expr {
    Expr::Value(Value::Number(value.to_string(), false).into())
}

fn expr_null() -> Expr {
    Expr::Value(Value::Null.into())
}

fn and_exprs(exprs: Vec<Expr>) -> Option<Expr> {
    let mut iter = exprs.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    }))
}

fn explicit_alias(name: &str) -> TableAlias {
    TableAlias {
        explicit: true,
        name: Ident::new(name),
        columns: Vec::new(),
    }
}

fn query_targets_vtable(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.from.iter().any(table_with_joins_targets_vtable)
}

fn table_with_joins_targets_vtable(table: &TableWithJoins) -> bool {
    table_factor_is_vtable(&table.relation)
        || table
            .joins
            .iter()
            .any(|join| table_factor_is_vtable(&join.relation))
}

fn table_factor_is_vtable(relation: &TableFactor) -> bool {
    matches!(
        relation,
        TableFactor::Table { name, .. } if object_name_matches(name, VTABLE_NAME)
    )
}

fn extract_schema_keys_from_query(query: &Query) -> Option<Vec<String>> {
    extract_column_keys_from_query(query, expr_is_schema_key_column)
}

#[cfg(test)]
fn extract_schema_keys_from_query_deep(query: &Query) -> Vec<String> {
    let mut keys = Vec::new();
    collect_schema_keys_from_query(query, &mut keys);
    dedup_strings(keys)
}

#[cfg(test)]
fn collect_schema_keys_from_query(query: &Query, keys: &mut Vec<String>) {
    if let Some(found) = extract_schema_keys_from_query(query) {
        keys.extend(found);
    }
    if let Some(with) = query.with.as_ref() {
        for cte in &with.cte_tables {
            collect_schema_keys_from_query(&cte.query, keys);
        }
    }
    collect_schema_keys_from_set_expr(&query.body, keys);
}

#[cfg(test)]
fn collect_schema_keys_from_set_expr(expr: &SetExpr, keys: &mut Vec<String>) {
    match expr {
        SetExpr::Select(select) => collect_schema_keys_from_select(select, keys),
        SetExpr::Query(query) => collect_schema_keys_from_query(query, keys),
        SetExpr::SetOperation { left, right, .. } => {
            collect_schema_keys_from_set_expr(left, keys);
            collect_schema_keys_from_set_expr(right, keys);
        }
        _ => {}
    }
}

#[cfg(test)]
fn collect_schema_keys_from_select(select: &Select, keys: &mut Vec<String>) {
    for table in &select.from {
        collect_schema_keys_from_table_factor(&table.relation, keys);
        for join in &table.joins {
            collect_schema_keys_from_table_factor(&join.relation, keys);
        }
    }
}

#[cfg(test)]
fn collect_schema_keys_from_table_factor(relation: &TableFactor, keys: &mut Vec<String>) {
    match relation {
        TableFactor::Derived { subquery, .. } => collect_schema_keys_from_query(subquery, keys),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_schema_keys_from_table_factor(&table_with_joins.relation, keys);
            for join in &table_with_joins.joins {
                collect_schema_keys_from_table_factor(&join.relation, keys);
            }
        }
        _ => {}
    }
}

fn resolve_schema_keys_for_query(query: &Query, inherited_schema_keys: &[String]) -> Vec<String> {
    extract_schema_keys_from_query(query).unwrap_or_else(|| inherited_schema_keys.to_vec())
}

fn extract_plugin_keys_from_query(query: &Query) -> Option<Vec<String>> {
    extract_column_keys_from_query(query, expr_is_plugin_key_column)
}

fn extract_plugin_keys_from_top_level_derived_subquery(query: &Query) -> Option<Vec<String>> {
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select,
        _ => return None,
    };
    if !select_projects_count_star(select) {
        return None;
    }
    if select.selection.is_some() {
        return None;
    }
    if select.from.len() != 1 {
        return None;
    }
    let table = select.from.first()?;
    if !table.joins.is_empty() {
        return None;
    }
    let TableFactor::Derived { subquery, .. } = &table.relation else {
        return None;
    };
    extract_plugin_keys_from_query(subquery)
}

fn extract_pushdown_predicate(query: &Query) -> Option<Expr> {
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select,
        _ => return None,
    };
    let selection = select.selection.as_ref()?;
    strip_qualifiers(selection.clone())
}

fn extract_column_keys_from_query(
    query: &Query,
    is_target_column: fn(&Expr) -> bool,
) -> Option<Vec<String>> {
    extract_column_keys_from_set_expr(&query.body, is_target_column)
}

fn extract_column_keys_from_set_expr(
    expr: &SetExpr,
    is_target_column: fn(&Expr) -> bool,
) -> Option<Vec<String>> {
    match expr {
        SetExpr::Select(select) => extract_column_keys_from_select(select, is_target_column),
        SetExpr::Query(query) => extract_column_keys_from_set_expr(&query.body, is_target_column),
        SetExpr::SetOperation { left, right, .. } => {
            extract_column_keys_from_set_expr(left, is_target_column)
                .or_else(|| extract_column_keys_from_set_expr(right, is_target_column))
        }
        _ => None,
    }
}

fn extract_column_keys_from_select(
    select: &Select,
    is_target_column: fn(&Expr) -> bool,
) -> Option<Vec<String>> {
    select
        .selection
        .as_ref()
        .and_then(|expr| extract_column_keys_from_expr(expr, is_target_column))
}

fn extract_column_keys_from_expr(
    expr: &Expr,
    is_target_column: fn(&Expr) -> bool,
) -> Option<Vec<String>> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if is_target_column(left) {
                return string_literal_value(right).map(|value| vec![value]);
            }
            if is_target_column(right) {
                return string_literal_value(left).map(|value| vec![value]);
            }
            None
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => match (
            extract_column_keys_from_expr(left, is_target_column),
            extract_column_keys_from_expr(right, is_target_column),
        ) {
            (Some(left), Some(right)) => {
                let intersection = intersect_strings(&left, &right);
                if intersection.is_empty() {
                    None
                } else {
                    Some(intersection)
                }
            }
            (Some(keys), None) | (None, Some(keys)) => Some(keys),
            (None, None) => None,
        },
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => match (
            extract_column_keys_from_expr(left, is_target_column),
            extract_column_keys_from_expr(right, is_target_column),
        ) {
            (Some(left), Some(right)) => Some(union_strings(&left, &right)),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if !is_target_column(expr) {
                return None;
            }
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                let value = string_literal_value(item)?;
                values.push(value);
            }
            if values.is_empty() {
                None
            } else {
                Some(dedup_strings(values))
            }
        }
        Expr::Nested(inner) => extract_column_keys_from_expr(inner, is_target_column),
        _ => None,
    }
}

fn expr_is_schema_key_column(expr: &Expr) -> bool {
    expr_last_identifier_eq(expr, "schema_key")
}

fn expr_is_plugin_key_column(expr: &Expr) -> bool {
    expr_last_identifier_eq(expr, "plugin_key")
}

fn expr_last_identifier_eq(expr: &Expr, target: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(target),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case(target))
            .unwrap_or(false),
        _ => false,
    }
}

fn string_literal_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        }) => Some(value.clone()),
        _ => None,
    }
}

fn strip_qualifiers(expr: Expr) -> Option<Expr> {
    match expr {
        Expr::Identifier(ident) => {
            if is_pushdown_column(&ident) {
                Some(Expr::Identifier(ident))
            } else {
                None
            }
        }
        Expr::CompoundIdentifier(_) => None,
        Expr::BinaryOp { left, op, right } => {
            if !is_simple_binary_op(&op) {
                return None;
            }
            let left = strip_qualifiers(*left)?;
            let right = strip_qualifiers(*right)?;
            Some(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        }
        Expr::Nested(inner) => strip_qualifiers(*inner).map(|inner| Expr::Nested(Box::new(inner))),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let expr = strip_qualifiers(*expr)?;
            let list = strip_in_list_values(list)?;
            Some(Expr::InList {
                expr: Box::new(expr),
                list,
                negated,
            })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let expr = strip_qualifiers(*expr)?;
            let low = strip_value_expr(*low)?;
            let high = strip_value_expr(*high)?;
            Some(Expr::Between {
                expr: Box::new(expr),
                negated,
                low: Box::new(low),
                high: Box::new(high),
            })
        }
        Expr::IsNull(inner) => {
            let inner = strip_qualifiers(*inner)?;
            Some(Expr::IsNull(Box::new(inner)))
        }
        Expr::IsNotNull(inner) => {
            let inner = strip_qualifiers(*inner)?;
            Some(Expr::IsNotNull(Box::new(inner)))
        }
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => {
            let expr = strip_qualifiers(*expr)?;
            Some(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            })
        }
        Expr::Like {
            expr,
            negated,
            pattern,
            escape_char,
            any,
        } => {
            let expr = strip_qualifiers(*expr)?;
            let pattern = strip_value_expr(*pattern)?;
            Some(Expr::Like {
                expr: Box::new(expr),
                negated,
                pattern: Box::new(pattern),
                escape_char,
                any,
            })
        }
        Expr::ILike {
            expr,
            negated,
            pattern,
            escape_char,
            any,
        } => {
            let expr = strip_qualifiers(*expr)?;
            let pattern = strip_value_expr(*pattern)?;
            Some(Expr::ILike {
                expr: Box::new(expr),
                negated,
                pattern: Box::new(pattern),
                escape_char,
                any,
            })
        }
        Expr::Value(_) => Some(expr),
        _ => None,
    }
}

fn strip_in_list_values(list: Vec<Expr>) -> Option<Vec<Expr>> {
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        let value = strip_value_expr(item)?;
        values.push(value);
    }
    Some(values)
}

fn strip_value_expr(expr: Expr) -> Option<Expr> {
    match expr {
        Expr::Value(_) => Some(expr),
        Expr::Nested(inner) => strip_value_expr(*inner).map(|inner| Expr::Nested(Box::new(inner))),
        _ => None,
    }
}

fn is_pushdown_column(ident: &Ident) -> bool {
    let value = ident.value.to_ascii_lowercase();
    matches!(
        value.as_str(),
        "entity_id"
            | "schema_key"
            | "schema_version"
            | "file_id"
            | "version_id"
            | "plugin_key"
            | "snapshot_content"
            | "metadata"
    )
}

fn is_simple_binary_op(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
    )
}

fn dedup_strings(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for value in values {
        if !out.contains(&value) {
            out.push(value);
        }
    }
    out
}

fn union_strings(left: &[String], right: &[String]) -> Vec<String> {
    let mut out = left.to_vec();
    for value in right {
        if !out.contains(value) {
            out.push(value.clone());
        }
    }
    out
}

fn intersect_strings(left: &[String], right: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for value in left {
        if right.contains(value) && !out.contains(value) {
            out.push(value.clone());
        }
    }
    out
}

fn narrow_schema_keys(
    schema_keys: &[String],
    predicate_schema_keys: Option<&[String]>,
) -> Vec<String> {
    let Some(predicate_schema_keys) = predicate_schema_keys else {
        return schema_keys.to_vec();
    };
    if schema_keys.is_empty() {
        return predicate_schema_keys.to_vec();
    }
    let intersection = intersect_strings(schema_keys, predicate_schema_keys);
    if intersection.is_empty() {
        schema_keys.to_vec()
    } else {
        intersection
    }
}

fn default_vtable_alias() -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(VTABLE_NAME),
        columns: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_untracked_union_query, extract_plugin_keys_from_query,
        extract_plugin_keys_from_top_level_derived_subquery, extract_pushdown_predicate,
        extract_schema_keys_from_query_deep,
    };
    use crate::sql::preprocess_sql_rewrite_only as preprocess_sql;
    use sqlparser::ast::{Query, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn compact_sql(sql: &str) -> String {
        sql.chars().filter(|c| !c.is_whitespace()).collect()
    }

    fn union_segment(sql: &str) -> &str {
        let end = sql
            .find(")ASlix_state_union")
            .expect("union segment end not found");
        let start = sql[..end]
            .rfind("FROM(")
            .expect("union segment start not found");
        &sql[start + 5..end]
    }

    fn assert_branch_contains_all(sql: &str, table_marker: &str, needles: &[&str]) {
        let union_sql = union_segment(sql);
        let start = union_sql
            .find(table_marker)
            .or_else(|| union_sql.find(&table_marker.replace('"', "")))
            .expect("table marker not found");
        let slice = &union_sql[start..];
        let end = slice.find("UNIONALL").unwrap_or(slice.len());
        let branch = &slice[..end];
        for needle in needles {
            assert!(
                branch.contains(needle),
                "expected branch for {table_marker} to contain {needle}, got: {branch}"
            );
        }
    }

    fn assert_branch_not_contains(sql: &str, table_marker: &str, needle: &str) {
        let union_sql = union_segment(sql);
        let start = union_sql
            .find(table_marker)
            .or_else(|| union_sql.find(&table_marker.replace('"', "")))
            .expect("table marker not found");
        let slice = &union_sql[start..];
        let end = slice.find("UNIONALL").unwrap_or(slice.len());
        let branch = &slice[..end];
        assert!(
            !branch.contains(needle),
            "expected branch for {table_marker} to not contain {needle}, got: {branch}"
        );
    }

    #[test]
    fn rewrite_pushes_down_predicates_for_schema_key_in() {
        let sql = "SELECT * FROM lix_internal_state_vtable WHERE schema_key IN ('schema_a', 'schema_b') AND entity_id = 'entity-1'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &[
                "schema_keyIN('schema_a','schema_b')",
                "entity_id='entity-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &[
                "is_tombstone=0",
                "schema_key='schema_a'",
                "snapshot_contentISNOTNULL",
                "schema_keyIN('schema_a','schema_b')",
                "entity_id='entity-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_b"#,
            &[
                "is_tombstone=0",
                "schema_key='schema_b'",
                "snapshot_contentISNOTNULL",
                "schema_keyIN('schema_a','schema_b')",
                "entity_id='entity-1'",
            ],
        );
    }

    #[test]
    fn rewrite_pushes_down_like_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND entity_id LIKE 'entity-%'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_key='schema_a'", "entity_idLIKE'entity-%'"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &[
                "is_tombstone=0",
                "schema_key='schema_a'",
                "snapshot_contentISNOTNULL",
                "entity_idLIKE'entity-%'",
            ],
        );
    }

    #[test]
    fn rewrite_pushes_down_or_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key IN ('schema_a', 'schema_b') \
            AND (entity_id = 'entity-1' OR file_id = 'file-1')";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &[
                "schema_keyIN('schema_a','schema_b')",
                "entity_id='entity-1'ORfile_id='file-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &[
                "is_tombstone=0",
                "schema_key='schema_a'",
                "snapshot_contentISNOTNULL",
                "entity_id='entity-1'ORfile_id='file-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_b"#,
            &[
                "is_tombstone=0",
                "schema_key='schema_b'",
                "snapshot_contentISNOTNULL",
                "entity_id='entity-1'ORfile_id='file-1'",
            ],
        );
    }

    #[test]
    fn rewrite_skips_or_with_non_schema_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' OR entity_id = 'entity-1'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert!(
            !compact.contains("lix_internal_state_untracked"),
            "expected no rewrite for OR with non-schema predicate, got: {compact}"
        );
    }

    #[test]
    fn rewrite_does_not_pushdown_qualified_identifiers() {
        let sql = "SELECT * FROM lix_internal_state_vtable AS a \
            WHERE a.schema_key = 'schema_a' AND a.entity_id = 'entity-1'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_keyIN('schema_a')"],
        );
        assert_branch_not_contains(
            &compact,
            "FROMlix_internal_state_untracked",
            "entity_id='entity-1'",
        );
        assert_branch_not_contains(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            "entity_id='entity-1'",
        );
    }

    #[test]
    fn rewrite_pushes_down_comparison_predicates() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND file_id >= 'file-2' AND entity_id <> 'entity-1'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &[
                "schema_key='schema_a'",
                "file_id>='file-2'",
                "entity_id<>'entity-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &[
                "is_tombstone=0",
                "schema_key='schema_a'",
                "snapshot_contentISNOTNULL",
                "file_id>='file-2'",
                "entity_id<>'entity-1'",
            ],
        );
    }

    #[test]
    fn rewrite_pushes_down_not_in_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND entity_id NOT IN ('entity-1', 'entity-2')";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &[
                "schema_key='schema_a'",
                "entity_idNOTIN('entity-1','entity-2')",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &[
                "is_tombstone=0",
                "schema_key='schema_a'",
                "snapshot_contentISNOTNULL",
                "entity_idNOTIN('entity-1','entity-2')",
            ],
        );
    }

    #[test]
    fn rewrite_pushes_down_is_null_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND snapshot_content IS NULL";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_key='schema_a'", "snapshot_contentISNULL"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &[
                "is_tombstone=0",
                "schema_key='schema_a'",
                "snapshot_contentISNOTNULL",
                "snapshot_contentISNULL",
            ],
        );
    }

    #[test]
    fn rewrite_pushes_down_between_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND entity_id BETWEEN 'a' AND 'm'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_key='schema_a'", "entity_idBETWEEN'a'AND'm'"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &[
                "is_tombstone=0",
                "schema_key='schema_a'",
                "snapshot_contentISNOTNULL",
                "entity_idBETWEEN'a'AND'm'",
            ],
        );
    }

    #[test]
    fn rewrite_pushes_down_not_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND NOT (entity_id = 'entity-1')";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_key='schema_a'", "NOT(entity_id='entity-1')"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &[
                "is_tombstone=0",
                "schema_key='schema_a'",
                "snapshot_contentISNOTNULL",
                "NOT(entity_id='entity-1')",
            ],
        );
    }

    #[test]
    fn extracts_plugin_keys_from_eq_and_in_list() {
        let query = parse_query(
            "SELECT * FROM lix_internal_state_vtable \
             WHERE plugin_key = 'plugin_json' OR plugin_key IN ('plugin_text', 'plugin_json')",
        );
        let keys = extract_plugin_keys_from_query(&query).expect("plugin keys should be extracted");
        assert_eq!(
            keys,
            vec!["plugin_json".to_string(), "plugin_text".to_string()]
        );
    }

    #[test]
    fn extracts_plugin_keys_from_qualified_identifier() {
        let query = parse_query(
            "SELECT * FROM lix_internal_state_vtable AS s WHERE s.plugin_key = 'plugin_json'",
        );
        let keys = extract_plugin_keys_from_query(&query).expect("plugin keys should be extracted");
        assert_eq!(keys, vec!["plugin_json".to_string()]);
    }

    #[test]
    fn extracts_plugin_keys_from_derived_subquery_filter() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM (SELECT * FROM lix_internal_state_vtable WHERE plugin_key = 'plugin_json') AS ranked",
        );
        let keys = extract_plugin_keys_from_top_level_derived_subquery(&query)
            .expect("plugin keys should be extracted");
        assert_eq!(keys, vec!["plugin_json".to_string()]);
    }

    #[test]
    fn plugin_key_extraction_skips_mixed_or_predicate() {
        let query = parse_query(
            "SELECT * FROM lix_internal_state_vtable \
             WHERE plugin_key = 'plugin_json' OR schema_key = 'json_pointer'",
        );
        assert!(
            extract_plugin_keys_from_query(&query).is_none(),
            "mixed OR should not produce a plugin-only key set"
        );
    }

    #[test]
    fn extracts_schema_keys_from_nested_derived_subquery_filter() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM (SELECT * FROM lix_internal_state_vtable WHERE schema_key = 'schema_a') AS ranked",
        );
        let keys = extract_schema_keys_from_query_deep(&query);
        assert_eq!(keys, vec!["schema_a".to_string()]);
    }

    #[test]
    fn narrows_materialized_union_to_schema_predicate_intersection() {
        let query = parse_query(
            "SELECT * FROM lix_internal_state_vtable \
             WHERE schema_key = 'schema_a' AND entity_id = 'entity-1'",
        );
        let predicate = extract_pushdown_predicate(&query).expect("predicate");
        let derived = build_untracked_union_query(
            &[
                "schema_a".to_string(),
                "schema_b".to_string(),
                "schema_c".to_string(),
            ],
            Some(&predicate),
        )
        .expect("derived query");
        let compact = compact_sql(&derived.to_string());

        assert!(compact.contains(r#"lix_internal_state_materialized_v1_schema_a"#));
        assert!(!compact.contains(r#"lix_internal_state_materialized_v1_schema_b"#));
        assert!(!compact.contains(r#"lix_internal_state_materialized_v1_schema_c"#));
    }

    fn parse_query(sql: &str) -> Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("valid SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            _ => panic!("expected query"),
        }
    }
}
