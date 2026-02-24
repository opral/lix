use sqlparser::ast::{
    Expr, Insert, Query, SetExpr, Statement, TableObject, Value as SqlValue, Values,
};

use crate::{LixBackend, LixError, Value};

use super::super::ast::lowering::lower_statement;
use super::super::ast::utils::parse_sql_statements;
use super::super::ast::walk::object_name_matches;
use super::rewrite_engine::{
    rewrite_read_query_with_backend_and_params_in_session, ReadRewriteSession,
};

pub(crate) async fn materialize_vtable_insert_select_sources(
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

            if insert_values_rows(insert).is_some() {
                continue;
            }

            let Some(source) = insert.source.as_ref() else {
                continue;
            };
            let source_query = (**source).clone();
            let mut session = ReadRewriteSession::default();
            let rewritten_source = Box::pin(rewrite_read_query_with_backend_and_params_in_session(
                backend,
                source_query,
                params,
                &mut session,
            ))
            .await?;
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

fn insert_values_rows(insert: &Insert) -> Option<&[Vec<Expr>]> {
    let source = insert.source.as_ref()?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        return None;
    };
    Some(values.rows.as_slice())
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
    let mut statements = parse_sql_statements("SELECT 1 WHERE 0 = 1")?;
    statements.pop().ok_or_else(|| LixError {
        message: "failed to build no-op statement".to_string(),
    })
}
