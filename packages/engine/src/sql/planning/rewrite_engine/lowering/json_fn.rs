use sqlparser::ast::{
    helpers::attached_token::AttachedToken, CastKind, DataType, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart,
    Value as AstValue,
};

use crate::backend::SqlDialect;
use crate::engine::sql::planning::rewrite_engine::lowering::logical_fn::{
    LixJsonCall, LixJsonExtractCall, LixTextCodecCall,
};

pub(crate) fn lower_lix_json_extract(call: &LixJsonExtractCall, dialect: SqlDialect) -> Expr {
    match dialect {
        SqlDialect::Sqlite => lower_sqlite_json_text(call),
        SqlDialect::Postgres => lower_postgres_json_text(call),
    }
}

pub(crate) fn lower_lix_json(call: &LixJsonCall, dialect: SqlDialect) -> Expr {
    match dialect {
        SqlDialect::Sqlite => function_expr("json", vec![call.json_expr.clone()]),
        SqlDialect::Postgres => Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(call.json_expr.clone()),
            data_type: DataType::JSONB,
            format: None,
        },
    }
}

pub(crate) fn lower_lix_empty_blob(dialect: SqlDialect) -> Expr {
    match dialect {
        SqlDialect::Sqlite => function_expr("zeroblob", vec![integer_literal_expr(0)]),
        SqlDialect::Postgres => function_expr(
            "decode",
            vec![
                string_literal_expr("".to_string()),
                string_literal_expr("hex".to_string()),
            ],
        ),
    }
}

pub(crate) fn lower_lix_text_encode(call: &LixTextCodecCall, dialect: SqlDialect) -> Expr {
    match dialect {
        SqlDialect::Sqlite => Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(call.value_expr.clone()),
            data_type: DataType::Blob(None),
            format: None,
        },
        SqlDialect::Postgres => function_expr(
            "convert_to",
            vec![
                Expr::Cast {
                    kind: CastKind::Cast,
                    expr: Box::new(call.value_expr.clone()),
                    data_type: DataType::Text,
                    format: None,
                },
                string_literal_expr(call.encoding.clone()),
            ],
        ),
    }
}

pub(crate) fn lower_lix_text_decode(call: &LixTextCodecCall, dialect: SqlDialect) -> Expr {
    match dialect {
        SqlDialect::Sqlite => Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(call.value_expr.clone()),
            data_type: DataType::Text,
            format: None,
        },
        SqlDialect::Postgres => function_expr(
            "convert_from",
            vec![
                Expr::Cast {
                    kind: CastKind::Cast,
                    expr: Box::new(call.value_expr.clone()),
                    data_type: DataType::Bytea,
                    format: None,
                },
                string_literal_expr(call.encoding.clone()),
            ],
        ),
    }
}

fn lower_sqlite_json_text(call: &LixJsonExtractCall) -> Expr {
    let json_path = sqlite_json_path_literal(&call.path);
    let json_path_expr = string_literal_expr(json_path);
    let json_type_expr = function_expr(
        "json_type",
        vec![call.json_expr.clone(), json_path_expr.clone()],
    );
    let json_extract_expr =
        function_expr("json_extract", vec![call.json_expr.clone(), json_path_expr]);
    let sqlite_text_expr = Expr::BinaryOp {
        left: Box::new(json_extract_expr),
        op: sqlparser::ast::BinaryOperator::StringConcat,
        right: Box::new(string_literal_expr("".to_string())),
    };

    Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: Some(Box::new(json_type_expr)),
        conditions: vec![
            sqlparser::ast::CaseWhen {
                condition: string_literal_expr("true".to_string()),
                result: string_literal_expr("true".to_string()),
            },
            sqlparser::ast::CaseWhen {
                condition: string_literal_expr("false".to_string()),
                result: string_literal_expr("false".to_string()),
            },
        ],
        else_result: Some(Box::new(sqlite_text_expr)),
    }
}

fn lower_postgres_json_text(call: &LixJsonExtractCall) -> Expr {
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

fn sqlite_json_path_literal(path: &[String]) -> String {
    let mut json_path = "$".to_string();
    for segment in path {
        if sqlite_path_segment_is_array_index(segment) {
            json_path.push('[');
            json_path.push_str(segment);
            json_path.push(']');
        } else {
            json_path.push('.');
            json_path.push('"');
            json_path.push_str(&segment.replace('\\', "\\\\").replace('"', "\\\""));
            json_path.push('"');
        }
    }
    json_path
}

fn sqlite_path_segment_is_array_index(segment: &str) -> bool {
    !segment.is_empty() && segment.bytes().all(|byte| byte.is_ascii_digit())
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

fn integer_literal_expr(value: i64) -> Expr {
    Expr::Value(AstValue::Number(value.to_string(), false).into())
}
