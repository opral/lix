use std::fmt::Write as _;

use lix_engine::storage::StorageBackend;
use lix_engine::{Engine, ExecuteResult, SessionContext, Value};

use crate::backends::{BackendProfile, ProfileBackend, RedbBackend, RocksDbBackend, SqliteBackend};
use crate::workload::{WorkloadRow, sql_string};

const SQL_CHUNK_SIZE: usize = 500;
const READ_MANY_PK_COUNT: usize = crate::READ_MANY_PK_COUNT;

pub(crate) enum SqlFixture {
    Sqlite(GenericSqlFixture<SqliteBackend>),
    RocksDb(GenericSqlFixture<RocksDbBackend>),
    Redb(GenericSqlFixture<RedbBackend>),
}

pub(crate) struct GenericSqlFixture<B: StorageBackend> {
    session: SessionContext<B>,
    row_count: usize,
    insert_sql_chunks: Vec<String>,
    select_all_sql: String,
    select_many_by_pk_sql: String,
    select_one_by_pk_sql: String,
    update_one_by_pk_sql: String,
    update_all_sql_rows: Vec<String>,
    delete_all_sql: String,
    delete_one_by_pk_sql: String,
}

pub(crate) async fn empty_fixture(profile: BackendProfile, rows: &[WorkloadRow]) -> SqlFixture {
    match profile.backend() {
        ProfileBackend::Sqlite(backend) => {
            SqlFixture::Sqlite(fixture_for_session(prepare_session(backend).await, rows))
        }
        ProfileBackend::RocksDb(backend) => {
            SqlFixture::RocksDb(fixture_for_session(prepare_session(backend).await, rows))
        }
        ProfileBackend::Redb(backend) => {
            SqlFixture::Redb(fixture_for_session(prepare_session(backend).await, rows))
        }
    }
}

pub(crate) async fn seeded_fixture(profile: BackendProfile, rows: &[WorkloadRow]) -> SqlFixture {
    let fixture = empty_fixture(profile, rows).await;
    fixture.insert_all().await;
    fixture
}

impl SqlFixture {
    #[expect(clippy::cast_possible_truncation)]
    pub(crate) async fn insert_all(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.insert_all().await,
            Self::RocksDb(fixture) => fixture.insert_all().await,
            Self::Redb(fixture) => fixture.insert_all().await,
        }
    }

    pub(crate) async fn read_all(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.read_all().await,
            Self::RocksDb(fixture) => fixture.read_all().await,
            Self::Redb(fixture) => fixture.read_all().await,
        }
    }

    pub(crate) async fn read_many_by_pk(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.read_many_by_pk().await,
            Self::RocksDb(fixture) => fixture.read_many_by_pk().await,
            Self::Redb(fixture) => fixture.read_many_by_pk().await,
        }
    }

    pub(crate) async fn read_one_by_pk(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.read_one_by_pk().await,
            Self::RocksDb(fixture) => fixture.read_one_by_pk().await,
            Self::Redb(fixture) => fixture.read_one_by_pk().await,
        }
    }

    pub(crate) async fn update_all(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.update_all().await,
            Self::RocksDb(fixture) => fixture.update_all().await,
            Self::Redb(fixture) => fixture.update_all().await,
        }
    }

    pub(crate) async fn update_one_by_pk(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.update_one_by_pk().await,
            Self::RocksDb(fixture) => fixture.update_one_by_pk().await,
            Self::Redb(fixture) => fixture.update_one_by_pk().await,
        }
    }

    pub(crate) async fn delete_all(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.delete_all().await,
            Self::RocksDb(fixture) => fixture.delete_all().await,
            Self::Redb(fixture) => fixture.delete_all().await,
        }
    }

    pub(crate) async fn delete_one_by_pk(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.delete_one_by_pk().await,
            Self::RocksDb(fixture) => fixture.delete_one_by_pk().await,
            Self::Redb(fixture) => fixture.delete_one_by_pk().await,
        }
    }
}

impl<B> GenericSqlFixture<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    #[expect(clippy::cast_possible_truncation)]
    async fn insert_all(&self) -> usize {
        let mut affected = 0;
        for sql in &self.insert_sql_chunks {
            affected += execute(&self.session, sql).await.rows_affected();
        }
        assert_eq!(affected as usize, self.row_count);
        affected as usize
    }

    async fn read_all(&self) -> usize {
        let result = execute(&self.session, &self.select_all_sql).await;
        assert_eq!(result.len(), self.row_count);
        result.len()
    }

    async fn read_many_by_pk(&self) -> usize {
        let result = execute(&self.session, &self.select_many_by_pk_sql).await;
        assert_eq!(result.len(), READ_MANY_PK_COUNT.min(self.row_count));
        result.len()
    }

    async fn read_one_by_pk(&self) -> usize {
        let result = execute(&self.session, &self.select_one_by_pk_sql).await;
        assert_eq!(result.len(), 1);
        result.len()
    }

    #[expect(clippy::cast_possible_truncation)]
    async fn update_all(&self) -> usize {
        let mut affected = 0;
        for sql in &self.update_all_sql_rows {
            affected += execute(&self.session, sql).await.rows_affected();
        }
        assert_eq!(affected as usize, self.row_count);
        affected as usize
    }

    #[expect(clippy::cast_possible_truncation)]
    async fn update_one_by_pk(&self) -> usize {
        let affected = execute(&self.session, &self.update_one_by_pk_sql)
            .await
            .rows_affected();
        assert_eq!(affected, 1);
        affected as usize
    }

    #[expect(clippy::cast_possible_truncation)]
    async fn delete_all(&self) -> usize {
        let affected = execute(&self.session, &self.delete_all_sql)
            .await
            .rows_affected();
        assert_eq!(affected as usize, self.row_count);
        affected as usize
    }

    #[expect(clippy::cast_possible_truncation)]
    async fn delete_one_by_pk(&self) -> usize {
        let affected = execute(&self.session, &self.delete_one_by_pk_sql)
            .await
            .rows_affected();
        assert_eq!(affected, 1);
        affected as usize
    }
}

fn fixture_for_session<B>(session: SessionContext<B>, rows: &[WorkloadRow]) -> GenericSqlFixture<B>
where
    B: StorageBackend,
{
    let mid = rows.len() / 2;
    GenericSqlFixture {
        session,
        row_count: rows.len(),
        insert_sql_chunks: rows.chunks(SQL_CHUNK_SIZE).map(insert_rows_sql).collect(),
        select_all_sql: "SELECT path, value FROM json_pointer ORDER BY path".to_string(),
        select_many_by_pk_sql: select_by_pk_sql(&rows[..READ_MANY_PK_COUNT.min(rows.len())]),
        select_one_by_pk_sql: select_by_pk_sql(&rows[mid..][..1]),
        update_one_by_pk_sql: update_row_sql(&rows[mid]),
        update_all_sql_rows: rows.iter().map(update_row_sql).collect(),
        delete_all_sql: "DELETE FROM json_pointer".to_string(),
        delete_one_by_pk_sql: format!(
            "DELETE FROM json_pointer WHERE path = '{}'",
            sql_string(rows[mid].path.as_str())
        ),
    }
}

async fn prepare_session<B>(backend: B) -> SessionContext<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
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

async fn register_json_pointer_schema<B>(session: &SessionContext<B>)
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
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

async fn execute<B>(session: &SessionContext<B>, sql: &str) -> ExecuteResult
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
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
        let _ = write!(
            sql,
            "('{}', lix_json('{}'))",
            sql_string(row.path.as_str()),
            sql_string(row.value_json.as_str())
        );
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
