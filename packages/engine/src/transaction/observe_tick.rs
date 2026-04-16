use crate::{LixBackendTransaction, LixError, SqlDialect, Value};

pub(crate) async fn append_observe_tick_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    origin_key: Option<&str>,
) -> Result<(), LixError> {
    if let Some(origin_key) = origin_key {
        let sql = match transaction.dialect() {
            SqlDialect::Sqlite => {
                "INSERT INTO lix_internal_observe_tick (created_at, origin_key) \
                 VALUES (CURRENT_TIMESTAMP, ?)"
            }
            SqlDialect::Postgres => {
                "INSERT INTO lix_internal_observe_tick (created_at, origin_key) \
                 VALUES (CURRENT_TIMESTAMP, $1)"
            }
        };
        transaction
            .execute(sql, &[Value::Text(origin_key.to_string())])
            .await?;
    } else {
        transaction
            .execute(
                "INSERT INTO lix_internal_observe_tick (created_at, origin_key) \
                 VALUES (CURRENT_TIMESTAMP, NULL)",
                &[],
            )
            .await?;
    }
    Ok(())
}
