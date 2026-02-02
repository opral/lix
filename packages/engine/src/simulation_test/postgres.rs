use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use postgresql_embedded::PostgreSQL;
use tokio::sync::{Mutex as TokioMutex, OnceCell};

use crate::backends::{PostgresBackend, PostgresConfig};
use crate::{LixBackend, LixError};

use super::Simulation;

static POSTGRES: OnceCell<Arc<PostgresInstance>> = OnceCell::const_new();
static DB_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct PostgresInstance {
    postgresql: TokioMutex<PostgreSQL>,
    settings: postgresql_embedded::Settings,
}

async fn ensure_postgres() -> Result<Arc<PostgresInstance>, LixError> {
    POSTGRES
        .get_or_try_init(|| async {
            let mut pg = PostgreSQL::default();
            pg.setup().await.map_err(|err| LixError {
                message: err.to_string(),
            })?;
            pg.start().await.map_err(|err| LixError {
                message: err.to_string(),
            })?;
            let settings = pg.settings().clone();

            Ok(Arc::new(PostgresInstance {
                postgresql: TokioMutex::new(pg),
                settings,
            }))
        })
        .await
        .map(Arc::clone)
}

pub fn postgres_simulation() -> Simulation {
    let connection_string = Arc::new(Mutex::new(None::<String>));
    let setup_handle = connection_string.clone();

    Simulation {
        name: "postgres",
        setup: Some(Arc::new(move || {
            let connection_string = setup_handle.clone();
            Box::pin(async move {
                let instance = ensure_postgres().await?;
                let db_index = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
                let db_name = format!("lix_test_{}", db_index);

                {
                    let pg = instance.postgresql.lock().await;
                    pg.create_database(&db_name).await.map_err(|err| LixError {
                        message: err.to_string(),
                    })?;
                }

                let url = instance.settings.url(&db_name);

                *connection_string
                    .lock()
                    .expect("postgres connection string mutex poisoned") = Some(url);

                Ok(())
            })
        })),
        backend_factory: Box::new(move || {
            let url = connection_string
                .lock()
                .expect("postgres connection string mutex poisoned")
                .clone()
                .expect("postgres setup did not run");
            Box::new(PostgresBackend::new(PostgresConfig {
                connection_string: url,
            })) as Box<dyn LixBackend + Send + Sync>
        }),
    }
}
