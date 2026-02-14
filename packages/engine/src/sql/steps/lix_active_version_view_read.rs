use sqlparser::ast::{Query, Select, TableFactor};

use crate::sql::{
    default_alias, escape_sql_string, object_name_matches, parse_single_query,
    rewrite_query_with_select_rewriter, rewrite_table_factors_in_select,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
};
use crate::LixError;

const LIX_ACTIVE_VERSION_VIEW_NAME: &str = "lix_active_version";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    if !top_level_select_targets_lix_active_version(&query) {
        return Ok(None);
    }
    rewrite_query_with_select_rewriter(query, &mut rewrite_select)
}

fn top_level_select_targets_lix_active_version(query: &Query) -> bool {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select
        .from
        .iter()
        .any(table_with_joins_targets_lix_active_version)
}

fn table_with_joins_targets_lix_active_version(table: &sqlparser::ast::TableWithJoins) -> bool {
    table_factor_is_lix_active_version(&table.relation)
        || table
            .joins
            .iter()
            .any(|join| table_factor_is_lix_active_version(&join.relation))
}

fn table_factor_is_lix_active_version(relation: &TableFactor) -> bool {
    matches!(
        relation,
        TableFactor::Table { name, .. } if object_name_matches(name, LIX_ACTIVE_VERSION_VIEW_NAME)
    )
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    rewrite_table_factors_in_select(select, &mut rewrite_table_factor, changed)
}

fn rewrite_table_factor(relation: &mut TableFactor, changed: &mut bool) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if object_name_matches(name, LIX_ACTIVE_VERSION_VIEW_NAME) =>
        {
            let derived_query = build_lix_active_version_view_query()?;
            let derived_alias = alias
                .clone()
                .or_else(|| Some(default_lix_active_version_alias()));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            *changed = true;
        }
        _ => {}
    }
    Ok(())
}

fn build_lix_active_version_view_query() -> Result<Query, LixError> {
    let sql = format!(
        "SELECT \
             entity_id AS id, \
             lix_json_text(snapshot_content, 'version_id') AS version_id, \
             schema_key, \
             file_id, \
             version_id AS lixcol_version_id, \
             schema_version, \
             untracked, \
             created_at, \
             updated_at, \
             change_id \
         FROM lix_internal_state_vtable \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{storage_version_id}' \
           AND untracked = 1 \
           AND snapshot_content IS NOT NULL",
        schema_key = escape_sql_string(active_version_schema_key()),
        file_id = escape_sql_string(active_version_file_id()),
        storage_version_id = escape_sql_string(active_version_storage_version_id()),
    );
    parse_single_query(&sql)
}

fn default_lix_active_version_alias() -> sqlparser::ast::TableAlias {
    default_alias(LIX_ACTIVE_VERSION_VIEW_NAME)
}
