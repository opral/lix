use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, ObjectNamePart,
    Value as AstValue, ValueWithSpan,
};

use crate::LixError;

#[derive(Debug, Clone)]
pub(crate) struct LixJsonExtractCall {
    pub(crate) json_expr: Expr,
    pub(crate) path: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct LixJsonCall {
    pub(crate) json_expr: Expr,
}

pub(crate) fn parse_lix_json_extract(
    function: &Function,
) -> Result<Option<LixJsonExtractCall>, LixError> {
    if !function_name_matches(&function.name, "lix_json_extract") {
        return Ok(None);
    }

    let args = match &function.args {
        FunctionArguments::List(list) => {
            if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
                return Err(LixError {
                    message: "lix_json_extract() does not support DISTINCT/ALL/clauses".to_string(),
                });
            }
            &list.args
        }
        _ => {
            return Err(LixError {
                message: "lix_json_extract() requires a regular argument list".to_string(),
            })
        }
    };

    if args.len() < 2 {
        return Err(LixError {
            message: "lix_json_extract() requires at least 2 arguments".to_string(),
        });
    }

    let json_expr = function_arg_expr(&args[0], "lix_json_extract()")?;
    let mut path = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        let expr = function_arg_expr(arg, "lix_json_extract()")?;
        let key = string_literal(&expr).ok_or_else(|| LixError {
            message: "lix_json_extract() path arguments must be single-quoted strings".to_string(),
        })?;
        if key.is_empty() {
            return Err(LixError {
                message: "lix_json_extract() path segments must not be empty".to_string(),
            });
        }
        path.push(key.to_string());
    }

    Ok(Some(LixJsonExtractCall { json_expr, path }))
}

pub(crate) fn parse_lix_json(function: &Function) -> Result<Option<LixJsonCall>, LixError> {
    if !function_name_matches(&function.name, "lix_json") {
        return Ok(None);
    }
    let args = match &function.args {
        FunctionArguments::List(list) => {
            if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
                return Err(LixError {
                    message: "lix_json() does not support DISTINCT/ALL/clauses".to_string(),
                });
            }
            &list.args
        }
        _ => {
            return Err(LixError {
                message: "lix_json() requires a regular argument list".to_string(),
            });
        }
    };
    if args.len() != 1 {
        return Err(LixError {
            message: "lix_json() requires exactly 1 argument".to_string(),
        });
    }
    let json_expr = function_arg_expr(&args[0], "lix_json()")?;
    Ok(Some(LixJsonCall { json_expr }))
}

pub(crate) fn parse_lix_empty_blob(function: &Function) -> Result<Option<()>, LixError> {
    if !function_name_matches(&function.name, "lix_empty_blob") {
        return Ok(None);
    }
    match &function.args {
        FunctionArguments::List(list) => {
            if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
                return Err(LixError {
                    message: "lix_empty_blob() does not support DISTINCT/ALL/clauses".to_string(),
                });
            }
            if !list.args.is_empty() {
                return Err(LixError {
                    message: "lix_empty_blob() does not accept arguments".to_string(),
                });
            }
            Ok(Some(()))
        }
        FunctionArguments::None => Ok(Some(())),
        _ => Err(LixError {
            message: "lix_empty_blob() requires a regular argument list".to_string(),
        }),
    }
}

pub(crate) fn function_name_matches(name: &ObjectName, expected: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(expected))
        .unwrap_or(false)
}

fn function_arg_expr(arg: &FunctionArg, function_name: &str) -> Result<Expr, LixError> {
    let inner = match arg {
        FunctionArg::Unnamed(arg) => arg,
        _ => {
            return Err(LixError {
                message: format!("{function_name} does not support named arguments"),
            })
        }
    };

    match inner {
        FunctionArgExpr::Expr(expr) => Ok(expr.clone()),
        _ => Err(LixError {
            message: format!("{function_name} arguments must be SQL expressions"),
        }),
    }
}

fn string_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: AstValue::SingleQuotedString(value),
            ..
        }) => Some(value.as_str()),
        _ => None,
    }
}
