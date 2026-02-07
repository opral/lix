use sqlparser::ast::{
    BinaryOperator, Expr, Ident, ObjectName, Query, Select, SetExpr, Statement, TableAlias,
    TableFactor, TableWithJoins, UnaryOperator, Value, ValueWithSpan,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::backend::SqlDialect;
use crate::{LixBackend, LixError, Value as LixValue};

const VTABLE_NAME: &str = "lix_internal_state_vtable";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    let top_level_targets_vtable = query_targets_vtable(&query);
    let schema_keys = if top_level_targets_vtable {
        extract_schema_keys_from_query(&query).unwrap_or_default()
    } else {
        Vec::new()
    };
    let pushdown_predicate = if top_level_targets_vtable && !schema_keys.is_empty() {
        extract_pushdown_predicate(&query)
    } else {
        None
    };

    let mut changed = false;
    let mut new_query = query.clone();
    rewrite_query_inner(
        &mut new_query,
        &schema_keys,
        pushdown_predicate.as_ref(),
        &mut changed,
    )?;

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

pub async fn rewrite_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
) -> Result<Option<Query>, LixError> {
    let top_level_targets_vtable = query_targets_vtable(&query);
    let mut schema_keys = if top_level_targets_vtable {
        extract_schema_keys_from_query(&query).unwrap_or_default()
    } else {
        Vec::new()
    };
    if schema_keys.is_empty() {
        schema_keys = fetch_materialized_schema_keys(backend).await?;
    }
    let pushdown_predicate = if top_level_targets_vtable {
        extract_pushdown_predicate(&query)
    } else {
        None
    };

    let mut changed = false;
    let mut new_query = query.clone();
    rewrite_query_inner(
        &mut new_query,
        &schema_keys,
        pushdown_predicate.as_ref(),
        &mut changed,
    )?;

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

fn rewrite_query_inner(
    query: &mut Query,
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    changed: &mut bool,
) -> Result<(), LixError> {
    if let Some(with) = query.with.as_mut() {
        for cte in &mut with.cte_tables {
            rewrite_query_inner(&mut cte.query, schema_keys, None, changed)?;
        }
    }
    query.body = Box::new(rewrite_set_expr(
        (*query.body).clone(),
        schema_keys,
        pushdown_predicate,
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
            rewrite_query_inner(&mut query, schema_keys, pushdown_predicate, changed)?;
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
            if schema_keys.is_empty() {
                if let Some(rewritten) = rewrite_query((**subquery).clone())? {
                    *subquery = Box::new(rewritten);
                    *changed = true;
                }
            } else {
                let mut subquery_changed = false;
                let mut rewritten_subquery = (**subquery).clone();
                rewrite_query_inner(
                    &mut rewritten_subquery,
                    schema_keys,
                    pushdown_predicate,
                    &mut subquery_changed,
                )?;
                if subquery_changed {
                    *subquery = Box::new(rewritten_subquery);
                    *changed = true;
                }
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
    let dialect = GenericDialect {};
    let schema_list = schema_keys
        .iter()
        .map(|key| format!("'{}'", escape_string_literal(key)))
        .collect::<Vec<_>>()
        .join(", ");
    let predicate_sql = pushdown_predicate
        .and_then(|expr| strip_qualifiers(expr.clone()))
        .map(|expr| expr.to_string());
    let schema_filter = if schema_keys.is_empty() {
        None
    } else {
        Some(format!("schema_key IN ({schema_list})"))
    };
    let untracked_where = match (schema_filter.as_ref(), predicate_sql.as_ref()) {
        (Some(schema_filter), Some(predicate)) => {
            format!("{schema_filter} AND ({predicate})")
        }
        (Some(schema_filter), None) => schema_filter.clone(),
        (None, Some(predicate)) => format!("({predicate})"),
        (None, None) => "1=1".to_string(),
    };

    let mut union_parts = Vec::new();
    union_parts.push(format!(
        "SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                created_at, updated_at, NULL AS inherited_from_version_id, 'untracked' AS change_id, 1 AS untracked, 1 AS priority \
         FROM {untracked} \
         WHERE {untracked_where}",
        untracked = UNTRACKED_TABLE
    ));

    for key in schema_keys {
        let materialized_table = format!("{MATERIALIZED_PREFIX}{key}");
        let materialized_ident = quote_ident(&materialized_table);
        let materialized_where = predicate_sql
            .as_ref()
            .map(|predicate| format!(" WHERE ({predicate})"))
            .unwrap_or_default();
        union_parts.push(format!(
            "SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                    created_at, updated_at, inherited_from_version_id, change_id, 0 AS untracked, 2 AS priority \
             FROM {materialized}{materialized_where}",
            materialized = materialized_ident,
            materialized_where = materialized_where
        ));
    }

    let union_sql = union_parts.join(" UNION ALL ");

    let sql = format!(
        "SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                created_at, updated_at, inherited_from_version_id, change_id, untracked \
         FROM (\
             SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                    created_at, updated_at, inherited_from_version_id, change_id, untracked, \
                    ROW_NUMBER() OVER (PARTITION BY entity_id, schema_key, file_id, version_id ORDER BY priority) AS rn \
             FROM ({union_sql}) AS lix_state_union\
         ) AS lix_state_ranked \
         WHERE rn = 1",
    );

    let mut statements = Parser::parse_sql(&dialect, &sql).map_err(|err| LixError {
        message: err.to_string(),
    })?;

    if statements.len() != 1 {
        return Err(LixError {
            message: "expected single derived query statement".to_string(),
        });
    }

    match statements.remove(0) {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            message: "derived query did not parse as SELECT".to_string(),
        }),
    }
}

fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
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
    extract_schema_keys_from_set_expr(&query.body)
}

fn extract_pushdown_predicate(query: &Query) -> Option<Expr> {
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select,
        _ => return None,
    };
    let selection = select.selection.as_ref()?;
    strip_qualifiers(selection.clone())
}

fn extract_schema_keys_from_set_expr(expr: &SetExpr) -> Option<Vec<String>> {
    match expr {
        SetExpr::Select(select) => extract_schema_keys_from_select(select),
        SetExpr::Query(query) => extract_schema_keys_from_set_expr(&query.body),
        SetExpr::SetOperation { left, right, .. } => extract_schema_keys_from_set_expr(left)
            .or_else(|| extract_schema_keys_from_set_expr(right)),
        _ => None,
    }
}

fn extract_schema_keys_from_select(select: &Select) -> Option<Vec<String>> {
    select
        .selection
        .as_ref()
        .and_then(extract_schema_keys_from_expr)
}

fn extract_schema_keys_from_expr(expr: &Expr) -> Option<Vec<String>> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if expr_is_schema_key_column(left) {
                return string_literal_value(right).map(|value| vec![value]);
            }
            if expr_is_schema_key_column(right) {
                return string_literal_value(left).map(|value| vec![value]);
            }
            None
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => match (
            extract_schema_keys_from_expr(left),
            extract_schema_keys_from_expr(right),
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
            extract_schema_keys_from_expr(left),
            extract_schema_keys_from_expr(right),
        ) {
            (Some(left), Some(right)) => Some(union_strings(&left, &right)),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if !expr_is_schema_key_column(expr) {
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
        Expr::Nested(inner) => extract_schema_keys_from_expr(inner),
        _ => None,
    }
}

fn expr_is_schema_key_column(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("schema_key"),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case("schema_key"))
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

fn default_vtable_alias() -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(VTABLE_NAME),
        columns: Vec::new(),
    }
}

fn escape_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

async fn fetch_materialized_schema_keys(backend: &dyn LixBackend) -> Result<Vec<String>, LixError> {
    let sql = match backend.dialect() {
        SqlDialect::Sqlite => {
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'lix_internal_state_materialized_v1_%'"
        }
        SqlDialect::Postgres => {
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = current_schema() \
               AND table_type = 'BASE TABLE' \
               AND table_name LIKE 'lix_internal_state_materialized_v1_%'"
        }
    };
    let result = backend.execute(sql, &[]).await?;

    let mut keys = Vec::new();
    for row in &result.rows {
        let Some(LixValue::Text(name)) = row.first() else {
            continue;
        };
        let Some(schema_key) = name.strip_prefix(MATERIALIZED_PREFIX) else {
            continue;
        };
        if schema_key.is_empty() {
            continue;
        }
        if !keys.iter().any(|existing| existing == schema_key) {
            keys.push(schema_key.to_string());
        }
    }

    keys.sort();
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use crate::sql::preprocess_sql_rewrite_only as preprocess_sql;

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
                "schema_keyIN('schema_a','schema_b')",
                "entity_id='entity-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_b"#,
            &[
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
            &["entity_idLIKE'entity-%'"],
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
            &["entity_id='entity-1'ORfile_id='file-1'"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_b"#,
            &["entity_id='entity-1'ORfile_id='file-1'"],
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
            &["file_id>='file-2'", "entity_id<>'entity-1'"],
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
            &["entity_idNOTIN('entity-1','entity-2')"],
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
            &["snapshot_contentISNULL"],
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
            &["entity_idBETWEEN'a'AND'm'"],
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
            &["NOT(entity_id='entity-1')"],
        );
    }
}
