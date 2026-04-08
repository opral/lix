use crate::contracts::artifacts::{PreparedBatch, PreparedStatement};
use crate::sql::support::{parse_sql_statements, resolve_placeholder_index, PlaceholderState};
use crate::{LixError, SqlDialect, Value};
use sqlparser::ast::{Expr, Value as SqlValue, VisitMut, VisitorMut};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

pub fn collapse_prepared_batch_for_dialect(
    batch: &PreparedBatch,
    dialect: SqlDialect,
) -> Result<PreparedStatement, LixError> {
    let mut collapsed_sql = Vec::new();
    for step in &batch.steps {
        let collapsed = inline_prepared_statement_for_dialect(step, dialect)?;
        let sql = collapsed.sql.trim();
        if !sql.is_empty() {
            collapsed_sql.push(sql.to_string());
        }
    }
    Ok(PreparedStatement {
        sql: collapsed_sql.join("; "),
        params: Vec::new(),
    })
}

pub fn inline_prepared_statement_for_dialect(
    statement: &PreparedStatement,
    dialect: SqlDialect,
) -> Result<PreparedStatement, LixError> {
    if statement.params.is_empty() {
        return Ok(statement.clone());
    }

    let sql = inline_bound_statement(&statement.sql, &statement.params, dialect)?;
    Ok(PreparedStatement {
        sql,
        params: Vec::new(),
    })
}

fn inline_bound_statement(
    sql: &str,
    params: &[Value],
    dialect: SqlDialect,
) -> Result<String, LixError> {
    let mut statements = parse_sql_statements(sql)?;
    let mut state = PlaceholderState::new();

    for statement in &mut statements {
        let mut visitor = PlaceholderLiteralInliner {
            params,
            dialect,
            state: &mut state,
        };
        if let ControlFlow::Break(error) = statement.visit(&mut visitor) {
            return Err(error);
        }
    }

    Ok(statements
        .into_iter()
        .map(|statement| statement.to_string())
        .collect::<Vec<_>>()
        .join("; "))
}

struct PlaceholderLiteralInliner<'a> {
    params: &'a [Value],
    dialect: SqlDialect,
    state: &'a mut PlaceholderState,
}

impl VisitorMut for PlaceholderLiteralInliner<'_> {
    type Break = LixError;

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Value(value) = expr else {
            return ControlFlow::Continue(());
        };
        let SqlValue::Placeholder(token) = &value.value else {
            return ControlFlow::Continue(());
        };

        let source_index = match resolve_placeholder_index(token, self.params.len(), self.state) {
            Ok(index) => index,
            Err(error) => return ControlFlow::Break(error),
        };

        *expr = match engine_value_to_sql_expr(&self.params[source_index], self.dialect) {
            Ok(expr) => expr,
            Err(error) => return ControlFlow::Break(error),
        };

        ControlFlow::Continue(())
    }
}

fn engine_value_to_sql_expr(value: &Value, dialect: SqlDialect) -> Result<Expr, LixError> {
    match value {
        Value::Blob(value) if dialect == SqlDialect::Postgres => {
            let expr_sql = format!("decode('{}', 'hex')", encode_hex_upper(value));
            Parser::new(&GenericDialect {})
                .try_with_sql(&expr_sql)
                .map_err(|error| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: error.to_string(),
                })?
                .parse_expr()
                .map_err(|error| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: error.to_string(),
                })
        }
        _ => Ok(Expr::value(engine_value_to_sql_literal(value, dialect)?)),
    }
}

fn engine_value_to_sql_literal(value: &Value, dialect: SqlDialect) -> Result<SqlValue, LixError> {
    match value {
        Value::Null => Ok(SqlValue::Null),
        Value::Boolean(value) => Ok(SqlValue::Boolean(*value)),
        Value::Integer(value) => Ok(SqlValue::Number(value.to_string(), false)),
        Value::Real(value) => Ok(SqlValue::Number(value.to_string(), false)),
        Value::Text(value) => Ok(SqlValue::SingleQuotedString(value.clone())),
        Value::Json(value) => Ok(SqlValue::SingleQuotedString(value.to_string())),
        Value::Blob(value) => match dialect {
            SqlDialect::Sqlite => Ok(SqlValue::HexStringLiteral(encode_hex_upper(value))),
            SqlDialect::Postgres => Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "postgres blob literals require expression inlining",
            )),
        },
    }
}

fn encode_hex_upper(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
}
