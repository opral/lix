use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, GroupByExpr, Ident, Join, JoinConstraint, JoinOperator, ObjectName,
    ObjectNamePart, Query, Select, SelectFlavor, SelectItem, SetExpr, TableFactor, TableWithJoins,
    Value,
};

use crate::sql::read_views::query_builder::{
    aliased_select_item, and_expr, column_eq_text, is_not_null_expr, lix_json_text_expr,
    select_query_from_table, unaliased_select_item,
};
use crate::sql::{
    default_alias, object_name_matches, rewrite_query_selects,
    rewrite_table_factors_in_select_decision, RewriteDecision,
};
use crate::version::{
    version_descriptor_schema_key, version_descriptor_storage_version_id,
    version_pointer_schema_key, version_pointer_storage_version_id,
};
use crate::LixError;

const LIX_VERSION_VIEW_NAME: &str = "lix_version";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_selects(query, &mut rewrite_select)
}

fn rewrite_select(select: &mut Select) -> Result<RewriteDecision, LixError> {
    rewrite_table_factors_in_select_decision(select, &mut rewrite_table_factor)
}

fn rewrite_table_factor(relation: &mut TableFactor) -> Result<RewriteDecision, LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if object_name_matches(name, LIX_VERSION_VIEW_NAME) =>
        {
            let derived_query = build_lix_version_view_query();
            let derived_alias = alias.clone().or_else(|| Some(default_lix_version_alias()));
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

fn build_lix_version_view_query() -> Query {
    let descriptor_selection = and_expr(
        and_expr(
            column_eq_text("schema_key", version_descriptor_schema_key()),
            column_eq_text("version_id", version_descriptor_storage_version_id()),
        ),
        is_not_null_expr("snapshot_content"),
    );
    let descriptor_query = select_query_from_table(
        vec![
            unaliased_select_item("entity_id"),
            unaliased_select_item("file_id"),
            unaliased_select_item("version_id"),
            unaliased_select_item("schema_version"),
            unaliased_select_item("change_id"),
            unaliased_select_item("created_at"),
            unaliased_select_item("updated_at"),
            aliased_select_item(lix_json_text_expr("snapshot_content", "id"), "id"),
            aliased_select_item(lix_json_text_expr("snapshot_content", "name"), "name"),
            aliased_select_item(
                lix_json_text_expr("snapshot_content", "inherits_from_version_id"),
                "inherits_from_version_id",
            ),
            aliased_select_item(lix_json_text_expr("snapshot_content", "hidden"), "hidden"),
        ],
        "lix_internal_state_vtable",
        descriptor_selection,
    );

    let pointer_selection = and_expr(
        and_expr(
            column_eq_text("schema_key", version_pointer_schema_key()),
            column_eq_text("version_id", version_pointer_storage_version_id()),
        ),
        is_not_null_expr("snapshot_content"),
    );
    let pointer_query = select_query_from_table(
        vec![
            unaliased_select_item("entity_id"),
            unaliased_select_item("change_id"),
            unaliased_select_item("created_at"),
            unaliased_select_item("updated_at"),
            aliased_select_item(lix_json_text_expr("snapshot_content", "id"), "id"),
            aliased_select_item(
                lix_json_text_expr("snapshot_content", "commit_id"),
                "commit_id",
            ),
            aliased_select_item(
                lix_json_text_expr("snapshot_content", "working_commit_id"),
                "working_commit_id",
            ),
        ],
        "lix_internal_state_vtable",
        pointer_selection,
    );

    let descriptor_relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(descriptor_query),
        alias: Some(default_alias("d")),
    };
    let pointer_relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(pointer_query),
        alias: Some(default_alias("t")),
    };

    let pointer_join = Join {
        relation: pointer_relation,
        global: false,
        join_operator: JoinOperator::LeftOuter(JoinConstraint::On(Expr::BinaryOp {
            left: Box::new(qualified_column_expr("t", "id")),
            op: BinaryOperator::Eq,
            right: Box::new(qualified_column_expr("d", "id")),
        })),
    };

    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: vec![
                aliased_select_item(qualified_column_expr("d", "id"), "id"),
                aliased_select_item(qualified_column_expr("d", "name"), "name"),
                aliased_select_item(
                    qualified_column_expr("d", "inherits_from_version_id"),
                    "inherits_from_version_id",
                ),
                aliased_select_item(qualified_column_expr("d", "hidden"), "hidden"),
                aliased_column_select_item_from_qualified("t", "commit_id", "commit_id"),
                aliased_column_select_item_from_qualified(
                    "t",
                    "working_commit_id",
                    "working_commit_id",
                ),
                aliased_select_item(qualified_column_expr("d", "entity_id"), "entity_id"),
                aliased_select_item(string_literal_expr("lix_version"), "schema_key"),
                aliased_select_item(qualified_column_expr("d", "file_id"), "file_id"),
                aliased_select_item(qualified_column_expr("d", "version_id"), "version_id"),
                aliased_select_item(string_literal_expr("lix"), "plugin_key"),
                aliased_select_item(
                    qualified_column_expr("d", "schema_version"),
                    "schema_version",
                ),
                aliased_select_item(
                    function_expr(
                        "COALESCE",
                        vec![
                            qualified_column_expr("t", "change_id"),
                            qualified_column_expr("d", "change_id"),
                        ],
                    ),
                    "change_id",
                ),
                aliased_select_item(
                    function_expr(
                        "COALESCE",
                        vec![
                            qualified_column_expr("d", "created_at"),
                            qualified_column_expr("t", "created_at"),
                        ],
                    ),
                    "created_at",
                ),
                aliased_select_item(
                    function_expr(
                        "COALESCE",
                        vec![
                            qualified_column_expr("t", "updated_at"),
                            qualified_column_expr("d", "updated_at"),
                        ],
                    ),
                    "updated_at",
                ),
                aliased_select_item(int_literal_expr(0), "untracked"),
            ],
            exclude: None,
            into: None,
            from: vec![TableWithJoins {
                relation: descriptor_relation,
                joins: vec![pointer_join],
            }],
            lateral_views: Vec::new(),
            prewhere: None,
            selection: None,
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

fn qualified_column_expr(relation: &str, column: &str) -> Expr {
    Expr::CompoundIdentifier(vec![Ident::new(relation), Ident::new(column)])
}

fn aliased_column_select_item_from_qualified(
    relation: &str,
    column: &str,
    alias: &str,
) -> SelectItem {
    aliased_select_item(qualified_column_expr(relation, column), alias)
}

fn function_expr(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)))
                .collect(),
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

fn string_literal_expr(value: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(value.to_string()).into())
}

fn int_literal_expr(value: i64) -> Expr {
    Expr::Value(Value::Number(value.to_string(), false).into())
}

fn default_lix_version_alias() -> sqlparser::ast::TableAlias {
    default_alias(LIX_VERSION_VIEW_NAME)
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
    fn rewrites_lix_version_to_descriptor_and_pointer_derived_join() {
        let query = parse_query(
            "SELECT id, commit_id \
             FROM lix_version \
             WHERE id = 'v1'",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("FROM lix_version"));
        assert!(sql.contains("LEFT OUTER JOIN"));
        assert!(sql.contains("lix_version_descriptor"));
        assert!(sql.contains("lix_version_pointer"));
        assert!(sql.contains("lix_internal_state_vtable"));
    }
}
