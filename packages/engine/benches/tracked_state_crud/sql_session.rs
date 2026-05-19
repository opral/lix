use lix_engine::storage::InMemoryStorageBackend;
use lix_engine::{Engine, ExecuteResult, SessionContext, Value};
use tokio::runtime::Runtime;

use crate::workload::{sql_string, WorkloadRow};

const SQL_CHUNK_SIZE: usize = 500;

pub(crate) fn insert_all(runtime: &Runtime, rows: &[WorkloadRow]) -> usize {
    with_session(runtime, |runtime, session| {
        runtime.block_on(insert_rows(session, rows));
        rows.len()
    })
}

pub(crate) fn read_all(runtime: &Runtime, rows: &[WorkloadRow]) -> usize {
    with_seeded_session(runtime, rows, |runtime, session| {
        let result = runtime.block_on(execute(
            session,
            "SELECT path, value FROM json_pointer ORDER BY path",
        ));
        assert_eq!(result.len(), rows.len());
        result.len()
    })
}

pub(crate) fn read_all_by_pk(runtime: &Runtime, rows: &[WorkloadRow]) -> usize {
    with_seeded_session(runtime, rows, |runtime, session| {
        let mut total = 0;
        for chunk in rows.chunks(SQL_CHUNK_SIZE) {
            let result = runtime.block_on(execute(session, &select_by_pk_sql(chunk)));
            total += result.len();
        }
        assert_eq!(total, rows.len());
        total
    })
}

pub(crate) fn read_one_by_pk(runtime: &Runtime, rows: &[WorkloadRow]) -> usize {
    with_seeded_session(runtime, rows, |runtime, session| {
        let result = runtime.block_on(execute(
            session,
            &select_by_pk_sql(&rows[rows.len() / 2..][..1]),
        ));
        assert_eq!(result.len(), 1);
        result.len()
    })
}

pub(crate) fn update_all(runtime: &Runtime, rows: &[WorkloadRow]) -> usize {
    with_seeded_session(runtime, rows, |runtime, session| {
        let mut affected = 0;
        for chunk in rows.chunks(SQL_CHUNK_SIZE) {
            affected += runtime.block_on(update_rows(session, chunk));
        }
        assert_eq!(affected as usize, rows.len());
        affected as usize
    })
}

pub(crate) fn update_one_by_pk(runtime: &Runtime, rows: &[WorkloadRow]) -> usize {
    with_seeded_session(runtime, rows, |runtime, session| {
        let affected = runtime.block_on(update_rows(session, &rows[rows.len() / 2..][..1]));
        assert_eq!(affected, 1);
        affected as usize
    })
}

pub(crate) fn delete_all(runtime: &Runtime, rows: &[WorkloadRow]) -> usize {
    with_seeded_session(runtime, rows, |runtime, session| {
        let affected = runtime
            .block_on(execute(session, "DELETE FROM json_pointer"))
            .rows_affected();
        assert_eq!(affected as usize, rows.len());
        affected as usize
    })
}

pub(crate) fn delete_one_by_pk(runtime: &Runtime, rows: &[WorkloadRow]) -> usize {
    with_seeded_session(runtime, rows, |runtime, session| {
        let row = &rows[rows.len() / 2];
        let affected = runtime
            .block_on(execute(
                session,
                &format!(
                    "DELETE FROM json_pointer WHERE path = '{}'",
                    sql_string(row.path.as_str())
                ),
            ))
            .rows_affected();
        assert_eq!(affected, 1);
        affected as usize
    })
}

fn with_seeded_session<T>(
    runtime: &Runtime,
    rows: &[WorkloadRow],
    f: impl FnOnce(&Runtime, &SessionContext<InMemoryStorageBackend>) -> T,
) -> T {
    with_session(runtime, |runtime, session| {
        runtime.block_on(insert_rows(session, rows));
        f(runtime, session)
    })
}

fn with_session<T>(
    runtime: &Runtime,
    f: impl FnOnce(&Runtime, &SessionContext<InMemoryStorageBackend>) -> T,
) -> T {
    let session = runtime.block_on(prepare_session(InMemoryStorageBackend::new()));
    f(runtime, &session)
}

async fn prepare_session(
    backend: InMemoryStorageBackend,
) -> SessionContext<InMemoryStorageBackend> {
    Engine::initialize(backend.clone())
        .await
        .expect("initialize tracked-state crud backend");
    let engine = Engine::new(backend)
        .await
        .expect("open tracked-state crud engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open tracked-state crud session");
    register_json_pointer_schema(&session).await;
    session
}

async fn register_json_pointer_schema(session: &SessionContext<InMemoryStorageBackend>) {
    let schema = serde_json::json!({
        "x-lix-key": "json_pointer",
        "x-lix-primary-key": ["/path"],
        "type": "object",
        "required": ["path", "value"],
        "properties": {
            "path": { "type": "string" },
            "value": {
                "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
            }
        },
        "additionalProperties": false
    });
    let affected = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register json_pointer schema")
        .rows_affected();
    assert_eq!(affected, 1);
}

async fn insert_rows(session: &SessionContext<InMemoryStorageBackend>, rows: &[WorkloadRow]) {
    for chunk in rows.chunks(SQL_CHUNK_SIZE) {
        let affected = execute(session, &insert_rows_sql(chunk))
            .await
            .rows_affected();
        assert_eq!(affected as usize, chunk.len());
    }
}

async fn update_rows(
    session: &SessionContext<InMemoryStorageBackend>,
    rows: &[WorkloadRow],
) -> u64 {
    let mut affected = 0;
    for row in rows {
        affected += execute(session, &update_row_sql(row)).await.rows_affected();
    }
    affected
}

async fn execute(session: &SessionContext<InMemoryStorageBackend>, sql: &str) -> ExecuteResult {
    session
        .execute(sql, &[])
        .await
        .expect("execute tracked-state crud SQL")
}

fn insert_rows_sql(rows: &[WorkloadRow]) -> String {
    let mut sql = String::from("INSERT INTO json_pointer (path, value) VALUES ");
    for (index, row) in rows.iter().enumerate() {
        if index > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "('{}', lix_json('{}'))",
            sql_string(row.path.as_str()),
            sql_string(row.value_json.as_str())
        ));
    }
    sql
}

fn select_by_pk_sql(rows: &[WorkloadRow]) -> String {
    format!(
        "SELECT path, value FROM json_pointer WHERE path IN ({}) ORDER BY path",
        rows.iter()
            .map(|row| format!("'{}'", sql_string(row.path.as_str())))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn update_row_sql(row: &WorkloadRow) -> String {
    format!(
        "UPDATE json_pointer SET value = lix_json('{}') WHERE path = '{}'",
        sql_string(row.updated_value_json.as_str()),
        sql_string(row.path.as_str())
    )
}
