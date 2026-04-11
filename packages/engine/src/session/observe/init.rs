use crate::backend::execute_ddl_batch;
use crate::{LixBackend, LixError, SqlDialect};

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    let statement = match backend.dialect() {
        SqlDialect::Sqlite => format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             tick_seq INTEGER PRIMARY KEY AUTOINCREMENT,\
             created_at TEXT NOT NULL,\
             writer_key TEXT\
             )",
            super::OBSERVE_TICK_TABLE
        ),
        SqlDialect::Postgres => format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             tick_seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,\
             created_at TEXT NOT NULL,\
             writer_key TEXT\
             )",
            super::OBSERVE_TICK_TABLE
        ),
    };
    execute_ddl_batch(backend, "observe", &[statement.as_str()]).await
}
