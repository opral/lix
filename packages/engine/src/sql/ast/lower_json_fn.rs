use sqlparser::ast::{
    helpers::attached_token::AttachedToken, CastKind, DataType, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart,
    Value as AstValue,
};

use crate::backend::SqlDialect;

use super::lower_logical_fn::{
    LixJsonCall, LixJsonExtractCall, LixJsonPathSegment, LixTextCodecCall,
};

pub(crate) fn lower_lix_json_extract(call: &LixJsonExtractCall, dialect: SqlDialect) -> Expr {
    match dialect {
        SqlDialect::Sqlite => lower_sqlite_json_text(call),
        SqlDialect::Postgres => lower_postgres_json_text(call),
    }
}

pub(crate) fn lower_lix_json_extract_json(call: &LixJsonExtractCall, dialect: SqlDialect) -> Expr {
    match dialect {
        SqlDialect::Sqlite => lower_sqlite_json_value_text(call),
        SqlDialect::Postgres => lower_postgres_json_value_text(call),
    }
}

pub(crate) fn lower_lix_json_extract_boolean(
    call: &LixJsonExtractCall,
    dialect: SqlDialect,
) -> Expr {
    match dialect {
        SqlDialect::Sqlite => lower_sqlite_json_boolean(call),
        SqlDialect::Postgres => lower_postgres_json_boolean(call),
    }
}

pub(crate) fn lower_lix_json(call: &LixJsonCall, dialect: SqlDialect) -> Expr {
    let json_input_expr = lower_json_input_expr(&call.json_expr);
    match dialect {
        SqlDialect::Sqlite => function_expr("json", vec![json_input_expr]),
        SqlDialect::Postgres => Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(json_input_expr),
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
    args.extend(call.path.iter().map(postgres_json_path_segment_expr));

    function_expr("jsonb_extract_path_text", args)
}

fn lower_sqlite_json_value_text(call: &LixJsonExtractCall) -> Expr {
    let json_path = sqlite_json_path_literal(&call.path);
    let json_path_expr = string_literal_expr(json_path);
    let json_type_expr = function_expr(
        "json_type",
        vec![call.json_expr.clone(), json_path_expr.clone()],
    );
    let json_extract_expr = function_expr(
        "json_extract",
        vec![call.json_expr.clone(), json_path_expr.clone()],
    );
    let json_text_expr = Expr::BinaryOp {
        left: Box::new(json_extract_expr.clone()),
        op: sqlparser::ast::BinaryOperator::StringConcat,
        right: Box::new(string_literal_expr("".to_string())),
    };

    Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: Some(Box::new(json_type_expr)),
        conditions: vec![
            sqlparser::ast::CaseWhen {
                condition: Expr::Value(AstValue::Null.into()),
                result: Expr::Value(AstValue::Null.into()),
            },
            sqlparser::ast::CaseWhen {
                condition: string_literal_expr("null".to_string()),
                result: Expr::Value(AstValue::Null.into()),
            },
            sqlparser::ast::CaseWhen {
                condition: string_literal_expr("true".to_string()),
                result: string_literal_expr("true".to_string()),
            },
            sqlparser::ast::CaseWhen {
                condition: string_literal_expr("false".to_string()),
                result: string_literal_expr("false".to_string()),
            },
            sqlparser::ast::CaseWhen {
                condition: string_literal_expr("object".to_string()),
                result: json_text_expr.clone(),
            },
            sqlparser::ast::CaseWhen {
                condition: string_literal_expr("array".to_string()),
                result: json_text_expr,
            },
        ],
        else_result: Some(Box::new(function_expr(
            "json_quote",
            vec![json_extract_expr],
        ))),
    }
}

fn lower_postgres_json_value_text(call: &LixJsonExtractCall) -> Expr {
    let jsonb_cast_expr = Expr::Cast {
        kind: CastKind::Cast,
        expr: Box::new(call.json_expr.clone()),
        data_type: DataType::JSONB,
        format: None,
    };
    let mut jsonb_extract_args = Vec::with_capacity(call.path.len() + 1);
    jsonb_extract_args.push(jsonb_cast_expr.clone());
    jsonb_extract_args.extend(call.path.iter().map(postgres_json_path_segment_expr));
    let jsonb_extract_expr = function_expr("jsonb_extract_path", jsonb_extract_args);
    let jsonb_typeof_expr = function_expr("jsonb_typeof", vec![jsonb_extract_expr.clone()]);

    Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: None,
        conditions: vec![
            sqlparser::ast::CaseWhen {
                condition: Expr::IsNull(Box::new(jsonb_extract_expr.clone())),
                result: Expr::Value(AstValue::Null.into()),
            },
            sqlparser::ast::CaseWhen {
                condition: Expr::BinaryOp {
                    left: Box::new(jsonb_typeof_expr),
                    op: sqlparser::ast::BinaryOperator::Eq,
                    right: Box::new(string_literal_expr("null".to_string())),
                },
                result: Expr::Value(AstValue::Null.into()),
            },
        ],
        else_result: Some(Box::new(Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(jsonb_extract_expr),
            data_type: DataType::Text,
            format: None,
        })),
    }
}

fn lower_sqlite_json_boolean(call: &LixJsonExtractCall) -> Expr {
    let json_path = sqlite_json_path_literal(&call.path);
    let json_path_expr = string_literal_expr(json_path);
    let json_type_expr = function_expr(
        "json_type",
        vec![call.json_expr.clone(), json_path_expr.clone()],
    );

    Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: Some(Box::new(json_type_expr)),
        conditions: vec![
            sqlparser::ast::CaseWhen {
                condition: string_literal_expr("true".to_string()),
                result: integer_literal_expr(1),
            },
            sqlparser::ast::CaseWhen {
                condition: string_literal_expr("false".to_string()),
                result: integer_literal_expr(0),
            },
        ],
        else_result: Some(Box::new(Expr::Value(AstValue::Null.into()))),
    }
}

fn lower_postgres_json_boolean(call: &LixJsonExtractCall) -> Expr {
    let jsonb_cast_expr = Expr::Cast {
        kind: CastKind::Cast,
        expr: Box::new(call.json_expr.clone()),
        data_type: DataType::JSONB,
        format: None,
    };
    let mut jsonb_extract_args = Vec::with_capacity(call.path.len() + 1);
    jsonb_extract_args.push(jsonb_cast_expr.clone());
    jsonb_extract_args.extend(call.path.iter().map(postgres_json_path_segment_expr));

    let jsonb_extract_expr = function_expr("jsonb_extract_path", jsonb_extract_args.clone());
    let jsonb_extract_text_expr = function_expr("jsonb_extract_path_text", jsonb_extract_args);
    let boolean_text_expr = Expr::Cast {
        kind: CastKind::Cast,
        expr: Box::new(jsonb_extract_text_expr),
        data_type: DataType::Boolean,
        format: None,
    };

    Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: None,
        conditions: vec![sqlparser::ast::CaseWhen {
            condition: Expr::BinaryOp {
                left: Box::new(function_expr("jsonb_typeof", vec![jsonb_extract_expr])),
                op: sqlparser::ast::BinaryOperator::Eq,
                right: Box::new(string_literal_expr("boolean".to_string())),
            },
            result: boolean_text_expr,
        }],
        else_result: Some(Box::new(Expr::Value(AstValue::Null.into()))),
    }
}

fn lower_json_input_expr(expr: &Expr) -> Expr {
    if let Some(json_literal) = json_literal_text(expr) {
        return string_literal_expr(json_literal);
    }

    function_expr(
        "coalesce",
        vec![
            Expr::Cast {
                kind: CastKind::Cast,
                expr: Box::new(expr.clone()),
                data_type: DataType::Text,
                format: None,
            },
            string_literal_expr("null".to_string()),
        ],
    )
}

fn json_literal_text(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(value) => match &value.value {
            AstValue::Boolean(value) => Some(value.to_string()),
            AstValue::Null => Some("null".to_string()),
            _ => None,
        },
        _ => None,
    }
}

fn postgres_json_path_segment_expr(segment: &LixJsonPathSegment) -> Expr {
    match segment {
        LixJsonPathSegment::Key(key) => string_literal_expr(key.clone()),
        LixJsonPathSegment::Index(index) => string_literal_expr(index.to_string()),
    }
}

fn sqlite_json_path_literal(path: &[LixJsonPathSegment]) -> String {
    let mut json_path = "$".to_string();
    for segment in path {
        match segment {
            LixJsonPathSegment::Key(key) => {
                json_path.push('.');
                json_path.push('"');
                json_path.push_str(&key.replace('\\', "\\\\").replace('"', "\\\""));
                json_path.push('"');
            }
            LixJsonPathSegment::Index(index) => {
                json_path.push('[');
                json_path.push_str(&index.to_string());
                json_path.push(']');
            }
        }
    }
    json_path
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
