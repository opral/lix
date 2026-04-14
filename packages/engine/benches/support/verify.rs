use lix_engine::{Lix, Value};
use std::sync::Arc;
use tokio::runtime::Runtime;

#[allow(dead_code)]
pub fn assert_row_count(
    runtime: &Runtime,
    lix: &Arc<Lix>,
    sql: &str,
    params: &[Value],
    expected: i64,
) {
    let result = runtime
        .block_on(lix.execute(sql, params))
        .expect("verification query should succeed");
    let actual = match result
        .statements
        .first()
        .and_then(|statement| statement.rows.first())
        .and_then(|row| row.first())
    {
        Some(Value::Integer(value)) => *value,
        other => panic!("expected integer verification row, got {other:?}"),
    };
    assert_eq!(actual, expected, "verification row count mismatch");
}

pub fn scalar_count(runtime: &Runtime, lix: &Arc<Lix>, sql: &str, params: &[Value]) -> i64 {
    let result = runtime
        .block_on(lix.execute(sql, params))
        .expect("verification query should succeed");
    match result
        .statements
        .first()
        .and_then(|statement| statement.rows.first())
        .and_then(|row| row.first())
    {
        Some(Value::Integer(value)) => *value,
        other => panic!("expected integer verification row, got {other:?}"),
    }
}

pub fn scalar_text(runtime: &Runtime, lix: &Arc<Lix>, sql: &str, params: &[Value]) -> String {
    let result = runtime
        .block_on(lix.execute(sql, params))
        .expect("verification query should succeed");
    match result
        .statements
        .first()
        .and_then(|statement| statement.rows.first())
        .and_then(|row| row.first())
    {
        Some(Value::Text(value)) => value.clone(),
        Some(Value::Integer(value)) => value.to_string(),
        other => panic!("expected text-like verification row, got {other:?}"),
    }
}

pub fn scalar_blob(runtime: &Runtime, lix: &Arc<Lix>, sql: &str, params: &[Value]) -> Vec<u8> {
    let result = runtime
        .block_on(lix.execute(sql, params))
        .expect("verification query should succeed");
    match result
        .statements
        .first()
        .and_then(|statement| statement.rows.first())
        .and_then(|row| row.first())
    {
        Some(Value::Blob(value)) => value.clone(),
        other => panic!("expected blob verification row, got {other:?}"),
    }
}
