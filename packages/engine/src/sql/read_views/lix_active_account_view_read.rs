use sqlparser::ast::{Query, Select, TableFactor};

use crate::account::{
    active_account_file_id, active_account_schema_key, active_account_storage_version_id,
};
use crate::sql::{
    default_alias, escape_sql_string, object_name_matches, parse_single_query,
    rewrite_query_selects, rewrite_table_factors_in_select_decision, RewriteDecision,
};
use crate::LixError;

const LIX_ACTIVE_ACCOUNT_VIEW_NAME: &str = "lix_active_account";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_selects(query, &mut rewrite_select)
}

fn rewrite_select(select: &mut Select) -> Result<RewriteDecision, LixError> {
    rewrite_table_factors_in_select_decision(select, &mut rewrite_table_factor)
}

fn rewrite_table_factor(relation: &mut TableFactor) -> Result<RewriteDecision, LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if object_name_matches(name, LIX_ACTIVE_ACCOUNT_VIEW_NAME) =>
        {
            let derived_query = build_lix_active_account_view_query()?;
            let derived_alias = alias
                .clone()
                .or_else(|| Some(default_lix_active_account_alias()));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            Ok(RewriteDecision::Changed)
        }
        _ => Ok(RewriteDecision::Unchanged),
    }
}

fn build_lix_active_account_view_query() -> Result<Query, LixError> {
    let sql = format!(
        "SELECT \
             lix_json_text(snapshot_content, 'account_id') AS account_id, \
             schema_key, \
             file_id, \
             version_id AS lixcol_version_id, \
             plugin_key, \
             schema_version, \
             untracked, \
             created_at, \
             updated_at, \
             change_id AS lixcol_change_id \
         FROM lix_internal_state_vtable \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{storage_version_id}' \
           AND untracked = 1 \
           AND snapshot_content IS NOT NULL",
        schema_key = escape_sql_string(active_account_schema_key()),
        file_id = escape_sql_string(active_account_file_id()),
        storage_version_id = escape_sql_string(active_account_storage_version_id()),
    );
    parse_single_query(&sql)
}

fn default_lix_active_account_alias() -> sqlparser::ast::TableAlias {
    default_alias(LIX_ACTIVE_ACCOUNT_VIEW_NAME)
}

#[cfg(test)]
mod tests {
    use super::rewrite_query;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            sqlparser::ast::Statement::Query(query) => *query,
            other => panic!("expected query, got {other:?}"),
        }
    }

    #[test]
    fn rewrites_lix_active_account_inside_exists_subquery() {
        let query = parse_query(
            "SELECT 1 \
             WHERE EXISTS (SELECT 1 FROM lix_active_account WHERE account_id = 'a')",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("FROM lix_active_account"));
        assert!(sql.contains("lix_internal_state_vtable"));
        assert!(sql.contains("untracked = 1"));
    }
}
