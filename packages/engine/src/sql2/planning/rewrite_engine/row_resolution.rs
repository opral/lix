use sqlparser::ast::{Expr, Insert, SetExpr, Value as SqlValue};

use crate::engine::sql2::planning::rewrite_engine::params::{resolve_placeholder_index, PlaceholderState};
use crate::{LixError, Value};

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
