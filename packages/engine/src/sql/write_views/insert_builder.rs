use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    Expr, Ident, Insert, ObjectName, ObjectNamePart, Query, SetExpr, TableObject, Value, Values,
};

pub(crate) fn make_values_insert(
    table: &str,
    column_names: &[&str],
    rows: Vec<Vec<Expr>>,
) -> Insert {
    let query = Query {
        with: None,
        body: Box::new(SetExpr::Values(Values {
            explicit_row: false,
            value_keyword: false,
            rows,
        })),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };

    Insert {
        insert_token: AttachedToken::empty(),
        or: None,
        ignore: false,
        into: true,
        table: TableObject::TableName(ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            table,
        ))])),
        table_alias: None,
        columns: column_names.iter().map(|name| Ident::new(*name)).collect(),
        overwrite: false,
        source: Some(Box::new(query)),
        assignments: Vec::new(),
        partitioned: None,
        after_columns: Vec::new(),
        has_table_keyword: false,
        on: None,
        returning: None,
        replace_into: false,
        priority: None,
        insert_alias: None,
        settings: None,
        format_clause: None,
    }
}

pub(crate) fn string_expr(value: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(value.to_string()).into())
}

pub(crate) fn int_expr(value: i64) -> Expr {
    Expr::Value(Value::Number(value.to_string(), false).into())
}

pub(crate) fn null_expr() -> Expr {
    Expr::Value(Value::Null.into())
}
