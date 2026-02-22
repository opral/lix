use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, GroupByExpr, Ident, ObjectName, ObjectNamePart, Query, Select, SelectFlavor,
    SelectItem, SetExpr, TableFactor, TableWithJoins, Value,
};

pub(crate) fn select_query_from_table(
    projection: Vec<SelectItem>,
    table: &str,
    selection: Expr,
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
            from: vec![table_with_joins_for(table)],
            lateral_views: Vec::new(),
            prewhere: None,
            selection: Some(selection),
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

pub(crate) fn unaliased_select_item(column: &str) -> SelectItem {
    SelectItem::UnnamedExpr(column_expr(column))
}

pub(crate) fn aliased_select_item(expr: Expr, alias: &str) -> SelectItem {
    SelectItem::ExprWithAlias {
        expr,
        alias: Ident::new(alias),
    }
}

pub(crate) fn aliased_column_select_item(column: &str, alias: &str) -> SelectItem {
    aliased_select_item(column_expr(column), alias)
}

pub(crate) fn lix_json_text_expr(column: &str, field: &str) -> Expr {
    function_expr(
        "lix_json_text",
        vec![column_expr(column), string_literal_expr(field)],
    )
}

pub(crate) fn column_eq_text(column: &str, value: &str) -> Expr {
    eq_expr(column_expr(column), string_literal_expr(value))
}

pub(crate) fn column_eq_int(column: &str, value: i64) -> Expr {
    eq_expr(column_expr(column), int_literal_expr(value))
}

pub(crate) fn and_expr(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    }
}

pub(crate) fn is_not_null_expr(column: &str) -> Expr {
    Expr::IsNotNull(Box::new(column_expr(column)))
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

fn column_expr(name: &str) -> Expr {
    Expr::Identifier(Ident::new(name))
}

fn eq_expr(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::Eq,
        right: Box::new(right),
    }
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
