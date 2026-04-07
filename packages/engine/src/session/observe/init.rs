use crate::ddl::execute_ddl_batch;
use crate::{LixBackend, LixError, SqlDialect};

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    let statement = match backend.dialect() {
        SqlDialect::Sqlite => {
            "CREATE TABLE IF NOT EXISTS lix_internal_observe_tick (\
             tick_seq INTEGER PRIMARY KEY AUTOINCREMENT,\
             created_at TEXT NOT NULL,\
             writer_key TEXT\
             )"
        }
        SqlDialect::Postgres => {
            "CREATE TABLE IF NOT EXISTS lix_internal_observe_tick (\
             tick_seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,\
             created_at TEXT NOT NULL,\
             writer_key TEXT\
             )"
        }
    };
    execute_ddl_batch(backend, "observe", &[statement]).await
}
