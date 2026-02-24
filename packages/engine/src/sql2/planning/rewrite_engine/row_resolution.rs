use sqlparser::ast::{
    Expr, Insert, Query, SetExpr, Statement, TableObject, Value as SqlValue, Values,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::engine::sql2::planning::rewrite_engine::params::{resolve_placeholder_index, PlaceholderState};
use crate::engine::sql2::planning::rewrite_engine::read_pipeline::rewrite_read_query_with_backend;
use crate::engine::sql2::planning::rewrite_engine::{lower_statement, object_name_matches};
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedCell {
    pub value: Option<Value>,
    pub placeholder_index: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ResolvedInsertRowSource {
    pub rows: Vec<Vec<Expr>>,
    pub resolved_rows: Vec<Vec<ResolvedCell>>,
    pub values_layout: InsertValuesLayout,
}

pub struct RowSourceResolver<'a> {
    params: &'a [Value],
}

impl<'a> RowSourceResolver<'a> {
    pub fn new(params: &'a [Value]) -> Self {
        Self { params }
    }

    pub fn resolve_insert(
        &self,
        insert: &Insert,
    ) -> Result<Option<ResolvedInsertRowSource>, LixError> {
        let Some(source) = &insert.source else {
            return Ok(None);
        };
        let SetExpr::Values(values) = source.body.as_ref() else {
            return Ok(None);
        };

        let rows = values.rows.clone();
        let resolved_rows = resolve_values_rows(&rows, self.params)?;

        Ok(Some(ResolvedInsertRowSource {
            rows,
            resolved_rows,
            values_layout: InsertValuesLayout {
                explicit_row: values.explicit_row,
                value_keyword: values.value_keyword,
            },
        }))
    }

    pub fn resolve_insert_required(
        &self,
        insert: &Insert,
        operation: &str,
    ) -> Result<ResolvedInsertRowSource, LixError> {
        self.resolve_insert(insert)?.ok_or_else(|| LixError {
            message: format!("{operation} requires VALUES rows"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsertValuesLayout {
    pub explicit_row: bool,
    pub value_keyword: bool,
}

pub fn resolve_values_rows(
    rows: &[Vec<Expr>],
    params: &[Value],
) -> Result<Vec<Vec<ResolvedCell>>, LixError> {
    let mut state = PlaceholderState::default();
    let mut resolved_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let mut resolved = Vec::with_capacity(row.len());
        for expr in row {
            resolved.push(resolve_expr(expr, params, &mut state)?);
        }
        resolved_rows.push(resolved);
    }

    Ok(resolved_rows)
}

pub fn resolve_insert_rows(
    insert: &Insert,
    params: &[Value],
) -> Result<Option<Vec<Vec<ResolvedCell>>>, LixError> {
    Ok(RowSourceResolver::new(params)
        .resolve_insert(insert)?
        .map(|source| source.resolved_rows))
}

pub fn resolve_expr_cell_with_state(
    expr: &Expr,
    params: &[Value],
    state: &mut PlaceholderState,
) -> Result<ResolvedCell, LixError> {
    resolve_expr(expr, params, state)
}

fn insert_values_rows(insert: &Insert) -> Option<&[Vec<Expr>]> {
    let source = insert.source.as_ref()?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        return None;
    };
    Some(values.rows.as_slice())
}

pub fn insert_values_rows_mut(insert: &mut Insert) -> Option<&mut [Vec<Expr>]> {
    let source = insert.source.as_mut()?;
    let SetExpr::Values(values) = source.body.as_mut() else {
        return None;
    };
    Some(values.rows.as_mut_slice())
}

pub async fn materialize_vtable_insert_select_sources(
    backend: &dyn LixBackend,
    statements: &mut [Statement],
    params: &[Value],
) -> Result<(), LixError> {
    for statement in statements {
        let mut replace_with_noop = false;
        {
            let Statement::Insert(insert) = statement else {
                continue;
            };
            if !insert_targets_vtable(insert) {
                continue;
            }

            // VALUES sources are already represented as resolved rows directly.
            if insert_values_rows(insert).is_some() {
                continue;
            }

            let Some(source) = insert.source.as_ref() else {
                continue;
            };
            let source_query = (**source).clone();
            let rewritten_source = rewrite_read_query_with_backend(backend, source_query).await?;
            let lowered_source = lower_statement(
                Statement::Query(Box::new(rewritten_source)),
                backend.dialect(),
            )?;
            let select_sql = lowered_source.to_string();
            let result = backend.execute(&select_sql, params).await?;

            if result.rows.is_empty() {
                replace_with_noop = true;
            } else {
                if insert.columns.is_empty() {
                    return Err(LixError {
                        message: "vtable insert requires explicit columns".to_string(),
                    });
                }

                let expected_columns = insert.columns.len();
                let mut rows = Vec::with_capacity(result.rows.len());
                for row in result.rows {
                    if row.len() != expected_columns {
                        return Err(LixError {
                            message: format!(
                                "vtable insert SELECT returned {} columns but {} were expected",
                                row.len(),
                                expected_columns
                            ),
                        });
                    }
                    rows.push(
                        row.iter()
                            .map(engine_value_to_expr)
                            .collect::<Result<Vec<_>, _>>()?,
                    );
                }

                insert.source = Some(Box::new(Query {
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
                }));
            }
        }

        if replace_with_noop {
            *statement = no_op_statement()?;
        }
    }

    Ok(())
}

fn insert_targets_vtable(insert: &Insert) -> bool {
    match &insert.table {
        TableObject::TableName(name) => object_name_matches(name, "lix_internal_state_vtable"),
        _ => false,
    }
}

fn engine_value_to_expr(value: &Value) -> Result<Expr, LixError> {
    match value {
        Value::Null => Ok(Expr::Value(SqlValue::Null.into())),
        Value::Text(value) => Ok(Expr::Value(
            SqlValue::SingleQuotedString(value.clone()).into(),
        )),
        Value::Integer(value) => Ok(Expr::Value(
            SqlValue::Number(value.to_string(), false).into(),
        )),
        Value::Real(value) => Ok(Expr::Value(
            SqlValue::Number(value.to_string(), false).into(),
        )),
        Value::Blob(_) => Err(LixError {
            message: "blob values are not supported in vtable insert SELECT materialization"
                .to_string(),
        }),
    }
}

fn no_op_statement() -> Result<Statement, LixError> {
    let statements =
        Parser::parse_sql(&GenericDialect {}, "SELECT 1 WHERE 0 = 1").map_err(|err| LixError {
            message: format!("failed to build no-op statement: {err}"),
        })?;
    statements.into_iter().next().ok_or_else(|| LixError {
        message: "failed to build no-op statement".to_string(),
    })
}

fn resolve_expr(
    expr: &Expr,
    params: &[Value],
    state: &mut PlaceholderState,
) -> Result<ResolvedCell, LixError> {
    let Expr::Value(value) = expr else {
        return Ok(ResolvedCell {
            value: None,
            placeholder_index: None,
        });
    };

    match &value.value {
        SqlValue::Placeholder(token) => {
            let index = resolve_placeholder_index(token, params.len(), state)?;
            Ok(ResolvedCell {
                value: Some(params[index].clone()),
                placeholder_index: Some(index),
            })
        }
        other => Ok(ResolvedCell {
            value: Some(sql_literal_to_engine_value(other)?),
            placeholder_index: None,
        }),
    }
}

fn sql_literal_to_engine_value(value: &SqlValue) -> Result<Value, LixError> {
    match value {
        SqlValue::Number(raw, _) => {
            if let Ok(int) = raw.parse::<i64>() {
                Ok(Value::Integer(int))
            } else if let Ok(real) = raw.parse::<f64>() {
                Ok(Value::Real(real))
            } else {
                Err(LixError {
                    message: format!("unsupported numeric literal '{raw}'"),
                })
            }
        }
        SqlValue::SingleQuotedString(text)
        | SqlValue::DoubleQuotedString(text)
        | SqlValue::TripleSingleQuotedString(text)
        | SqlValue::TripleDoubleQuotedString(text)
        | SqlValue::EscapedStringLiteral(text)
        | SqlValue::UnicodeStringLiteral(text)
        | SqlValue::NationalStringLiteral(text)
        | SqlValue::SingleQuotedRawStringLiteral(text)
        | SqlValue::DoubleQuotedRawStringLiteral(text)
        | SqlValue::TripleSingleQuotedRawStringLiteral(text)
        | SqlValue::TripleDoubleQuotedRawStringLiteral(text)
        | SqlValue::SingleQuotedByteStringLiteral(text)
        | SqlValue::DoubleQuotedByteStringLiteral(text)
        | SqlValue::TripleSingleQuotedByteStringLiteral(text)
        | SqlValue::TripleDoubleQuotedByteStringLiteral(text) => Ok(Value::Text(text.clone())),
        SqlValue::HexStringLiteral(text) => Ok(Value::Blob(parse_hex_literal(text)?)),
        SqlValue::DollarQuotedString(text) => Ok(Value::Text(text.value.clone())),
        SqlValue::Boolean(value) => Ok(Value::Integer(if *value { 1 } else { 0 })),
        SqlValue::Null => Ok(Value::Null),
        SqlValue::Placeholder(token) => Err(LixError {
            message: format!("unexpected placeholder '{token}' while resolving row"),
        }),
    }
}

fn parse_hex_literal(text: &str) -> Result<Vec<u8>, LixError> {
    if text.len() % 2 != 0 {
        return Err(LixError {
            message: format!(
                "hex literal must contain an even number of digits, got {}",
                text.len()
            ),
        });
    }

    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut index = 0;
    while index < bytes.len() {
        let hi = hex_nibble(bytes[index])?;
        let lo = hex_nibble(bytes[index + 1])?;
        out.push((hi << 4) | lo);
        index += 2;
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8, LixError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(LixError {
            message: format!("invalid hex digit '{}'", char::from(byte)),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_values_rows, RowSourceResolver};
    use crate::engine::sql2::planning::rewrite_engine::parse_sql_statements;
    use crate::Value;
    use sqlparser::ast::{Insert, SetExpr, Statement};

    fn parse_values_rows(sql: &str) -> Vec<Vec<sqlparser::ast::Expr>> {
        let mut statements = parse_sql_statements(sql).expect("parse SQL");
        let statement = statements.remove(0);
        let Statement::Insert(insert) = statement else {
            panic!("expected insert");
        };
        let source = insert.source.expect("source");
        let SetExpr::Values(values) = *source.body else {
            panic!("expected values");
        };
        values.rows
    }

    fn parse_insert(sql: &str) -> Insert {
        let mut statements = parse_sql_statements(sql).expect("parse SQL");
        let statement = statements.remove(0);
        let Statement::Insert(insert) = statement else {
            panic!("expected insert");
        };
        insert
    }

    #[test]
    fn resolves_sequential_and_numbered_placeholders() {
        let rows = parse_values_rows("INSERT INTO t(a, b, c) VALUES (?, ?2, ?), ($1, ?4, ?)");
        let params = vec![
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
            Value::Text("c".to_string()),
            Value::Text("d".to_string()),
            Value::Text("e".to_string()),
        ];

        let resolved = resolve_values_rows(&rows, &params).expect("resolve");
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0][0].placeholder_index, Some(0));
        assert_eq!(resolved[0][1].placeholder_index, Some(1));
        assert_eq!(resolved[0][2].placeholder_index, Some(2));
        assert_eq!(resolved[1][0].placeholder_index, Some(0));
        assert_eq!(resolved[1][1].placeholder_index, Some(3));
        assert_eq!(resolved[1][2].placeholder_index, Some(4));
    }

    #[test]
    fn keeps_non_literal_expressions_unresolved() {
        let rows = parse_values_rows("INSERT INTO t(a) VALUES (1 + 2)");
        let resolved = resolve_values_rows(&rows, &[]).expect("resolve");
        assert_eq!(resolved[0][0].value, None);
        assert_eq!(resolved[0][0].placeholder_index, None);
    }

    #[test]
    fn row_source_resolver_resolves_values_rows() {
        let insert = parse_insert("INSERT INTO t(a, b) VALUES ($1, 'text')");
        let params = [Value::Text("value".to_string())];
        let resolver = RowSourceResolver::new(&params);
        let source = resolver
            .resolve_insert(&insert)
            .expect("resolve")
            .expect("values source");

        assert_eq!(source.rows.len(), 1);
        assert_eq!(source.resolved_rows.len(), 1);
        assert_eq!(
            source.resolved_rows[0][0].value,
            Some(Value::Text("value".to_string()))
        );
    }

    #[test]
    fn row_source_resolver_returns_none_for_non_values_source() {
        let insert = parse_insert("INSERT INTO t(a) SELECT 1");
        let resolver = RowSourceResolver::new(&[]);
        let source = resolver.resolve_insert(&insert).expect("resolve");

        assert!(source.is_none());
    }

    #[test]
    fn row_source_resolver_required_reports_operation_name() {
        let insert = parse_insert("INSERT INTO t(a) SELECT 1");
        let resolver = RowSourceResolver::new(&[]);
        let err = resolver
            .resolve_insert_required(&insert, "vtable insert")
            .expect_err("must error");

        assert!(err.message.contains("vtable insert requires VALUES rows"));
    }

    #[test]
    fn resolves_hex_literal_as_blob() {
        let rows = parse_values_rows("INSERT INTO t(a) VALUES (X'414243')");
        let resolved = resolve_values_rows(&rows, &[]).expect("resolve");
        assert_eq!(resolved[0][0].value, Some(Value::Blob(vec![65, 66, 67])));
    }
}
