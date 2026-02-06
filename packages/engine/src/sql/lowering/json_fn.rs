use sqlparser::ast::{
    CastKind, DataType, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart, Value as AstValue,
};

use crate::backend::SqlDialect;
use crate::sql::lowering::logical_fn::LixJsonTextCall;

pub(crate) fn lower_lix_json_text(call: &LixJsonTextCall, dialect: SqlDialect) -> Expr {
    match dialect {
        SqlDialect::Sqlite => lower_sqlite_json_text(call),
        SqlDialect::Postgres => lower_postgres_json_text(call),
    }
}

fn lower_sqlite_json_text(call: &LixJsonTextCall) -> Expr {
    let mut json_path = "$".to_string();
    for segment in &call.path {
        json_path.push('.');
        json_path.push('"');
        json_path.push_str(&segment.replace('\\', "\\\\").replace('"', "\\\""));
        json_path.push('"');
    }
    function_expr(
        "json_extract",
        vec![call.json_expr.clone(), string_literal_expr(json_path)],
    )
}

fn lower_postgres_json_text(call: &LixJsonTextCall) -> Expr {
    let mut args = Vec::with_capacity(call.path.len() + 1);
    args.push(Expr::Cast {
        kind: CastKind::Cast,
        expr: Box::new(call.json_expr.clone()),
        data_type: DataType::JSONB,
        format: None,
    });
    args.extend(
        call.path
            .iter()
            .map(|segment| string_literal_expr(segment.clone())),
    );

    function_expr("jsonb_extract_path_text", args)
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

fn string_literal_expr(value: String) -> Expr {
    Expr::Value(AstValue::SingleQuotedString(value).into())
}
