use crate::transaction::WriteBatch;
use crate::{LixBackendTransaction, LixError, SqlDialect, Value};

pub(crate) async fn append_observe_tick_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    origin_key: Option<&str>,
) -> Result<(), LixError> {
    let mut write_batch = WriteBatch::new();
    append_observe_tick_to_write_batch(&mut write_batch, transaction.dialect(), origin_key);
    crate::transaction::execute_write_batch_with_transaction(transaction, write_batch)
        .await
        .map(|_| ())
}

pub(crate) fn append_observe_tick_to_write_batch(
    write_batch: &mut WriteBatch,
    dialect: SqlDialect,
    origin_key: Option<&str>,
) {
    if let Some(origin_key) = origin_key {
        let sql = match dialect {
            SqlDialect::Sqlite => {
                "INSERT INTO lix_internal_observe_tick (created_at, origin_key) \
                 VALUES (CURRENT_TIMESTAMP, ?)"
            }
            SqlDialect::Postgres => {
                "INSERT INTO lix_internal_observe_tick (created_at, origin_key) \
                 VALUES (CURRENT_TIMESTAMP, $1)"
            }
        };
        write_batch.push_statement(sql, vec![Value::Text(origin_key.to_string())]);
    } else {
        write_batch.push_statement(
            "INSERT INTO lix_internal_observe_tick (created_at, origin_key) \
             VALUES (CURRENT_TIMESTAMP, NULL)",
            Vec::new(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::write_batch::WriteStep;

    #[test]
    fn append_observe_tick_to_write_batch_uses_dialect_aware_placeholders() {
        let mut sqlite_batch = WriteBatch::new();
        append_observe_tick_to_write_batch(
            &mut sqlite_batch,
            SqlDialect::Sqlite,
            Some("sqlite-origin"),
        );
        assert_eq!(sqlite_batch.steps.len(), 1);
        let WriteStep::Statement { sql, params } = &sqlite_batch.steps[0];
        assert_eq!(
            sql,
            "INSERT INTO lix_internal_observe_tick (created_at, origin_key) \
             VALUES (CURRENT_TIMESTAMP, ?)"
        );
        assert_eq!(params, &vec![Value::Text("sqlite-origin".to_string())]);

        let mut postgres_batch = WriteBatch::new();
        append_observe_tick_to_write_batch(
            &mut postgres_batch,
            SqlDialect::Postgres,
            Some("pg-origin"),
        );
        assert_eq!(postgres_batch.steps.len(), 1);
        let WriteStep::Statement { sql, params } = &postgres_batch.steps[0];
        assert_eq!(
            sql,
            "INSERT INTO lix_internal_observe_tick (created_at, origin_key) \
             VALUES (CURRENT_TIMESTAMP, $1)"
        );
        assert_eq!(params, &vec![Value::Text("pg-origin".to_string())]);
    }

    #[test]
    fn append_observe_tick_to_write_batch_handles_null_origin() {
        let mut write_batch = WriteBatch::new();
        append_observe_tick_to_write_batch(&mut write_batch, SqlDialect::Sqlite, None);
        assert_eq!(write_batch.steps.len(), 1);
        let WriteStep::Statement { sql, params } = &write_batch.steps[0];
        assert_eq!(
            sql,
            "INSERT INTO lix_internal_observe_tick (created_at, origin_key) \
             VALUES (CURRENT_TIMESTAMP, NULL)"
        );
        assert!(params.is_empty());
    }
}
