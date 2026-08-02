use std::sync::Arc;

use lix_engine::{Engine, ExecuteBatchStatement, ExecuteResult, SessionContext, Storage, Value};

#[cfg(feature = "slatedb")]
use crate::storage::SlateDB;
use crate::storage::{ProfileStorage, RocksDB, SQLite, StorageProfile};
use crate::workload::{UpdateWorkloadRow, WorkloadRow, sql_string};

const READ_MANY_PK_COUNT: usize = crate::READ_MANY_PK_COUNT;
const BOUND_INSERT_ALL_SQL: &str = "INSERT INTO tracked_crud_insert (path, value) VALUES ($1, $2)";
const BOUND_SEED_JSON_SQL: &str =
    "INSERT INTO json_pointer (path, value) VALUES ($1, lix_json($2))";
const BOUND_UPDATE_ALL_SQL: &str = "UPDATE json_pointer SET value = lix_json($1) WHERE path = $2";
const UNTRACKED_PROBE_PATH: &str = "/__lix_untracked_probe";
type SharedParameterBatch = Arc<[Arc<[Value]>]>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UntrackedFixture {
    None,
    OneUnrelated,
    OneReadManyMember,
}

#[derive(Clone, Copy)]
enum FixtureShape {
    FullCrud,
    BoundUpdate,
}

enum LiteralUpdateWorkload {
    Transaction(Vec<String>),
    ExecuteBatch(Vec<ExecuteBatchStatement>),
}

impl UntrackedFixture {
    const fn has_untracked_row(self) -> bool {
        !matches!(self, Self::None)
    }
}

pub(crate) enum SqlFixture {
    SQLite(GenericSqlFixture<SQLite>),
    RocksDB(GenericSqlFixture<RocksDB>),
    #[cfg(feature = "slatedb")]
    SlateDB(GenericSqlFixture<SlateDB>),
}

pub(crate) struct GenericSqlFixture<StorageImpl: Storage + 'static> {
    session: SessionContext<StorageImpl>,
    /// Number of tracked fixture rows. In mixed mode the untracked probe
    /// replaces one of the requested rows rather than adding a 10,001st row.
    row_count: usize,
    visible_row_count: usize,
    untracked_fixture: UntrackedFixture,
    read_many_by_pk_count: usize,
    bound_insert_all_batch: SharedParameterBatch,
    bound_seed_json_batch: SharedParameterBatch,
    select_all_sql: String,
    select_many_by_pk_sql: String,
    select_one_by_pk_sql: String,
    update_one_by_pk_sql: String,
    update_all_workload: LiteralUpdateWorkload,
    bound_update_all_batch: SharedParameterBatch,
    delete_all_sql: String,
    delete_one_by_pk_sql: String,
    // Keep the storage path alive until after the session/storage is dropped.
    _dir: tempfile::TempDir,
}

pub(crate) async fn empty_fixture(profile: StorageProfile, rows: &[WorkloadRow]) -> SqlFixture {
    empty_fixture_with_read_many_pk_count(profile, rows, READ_MANY_PK_COUNT).await
}

/// Builds a fixture whose setup-excluded multi-point SQL has exactly
/// `read_many_by_pk_count` primary-key terms. Profile mode uses this to
/// measure read-many scaling without changing the Criterion workload shape.
pub(crate) async fn empty_fixture_with_read_many_pk_count(
    profile: StorageProfile,
    rows: &[WorkloadRow],
    read_many_by_pk_count: usize,
) -> SqlFixture {
    empty_fixture_with_shape(profile, rows, read_many_by_pk_count, FixtureShape::FullCrud).await
}

async fn empty_fixture_with_shape(
    profile: StorageProfile,
    rows: &[WorkloadRow],
    read_many_by_pk_count: usize,
    shape: FixtureShape,
) -> SqlFixture {
    assert!(
        (1..=rows.len()).contains(&read_many_by_pk_count),
        "read-many primary-key count must be between 1 and {}, got {read_many_by_pk_count}",
        rows.len()
    );
    let untracked_fixture = profile_untracked_fixture();
    match profile.storage() {
        ProfileStorage::SQLite { storage, _dir: dir } => SqlFixture::SQLite(fixture_for_session(
            prepare_session(storage).await,
            rows,
            read_many_by_pk_count,
            untracked_fixture,
            shape,
            dir,
        )),
        ProfileStorage::RocksDB { storage, _dir: dir } => SqlFixture::RocksDB(fixture_for_session(
            prepare_session(storage).await,
            rows,
            read_many_by_pk_count,
            untracked_fixture,
            shape,
            dir,
        )),
        #[cfg(feature = "slatedb")]
        ProfileStorage::SlateDB { storage, _dir: dir } => SqlFixture::SlateDB(fixture_for_session(
            prepare_session(storage).await,
            rows,
            read_many_by_pk_count,
            untracked_fixture,
            shape,
            dir,
        )),
    }
}

pub(crate) async fn seeded_fixture(profile: StorageProfile, rows: &[WorkloadRow]) -> SqlFixture {
    seeded_fixture_with_read_many_pk_count(profile, rows, READ_MANY_PK_COUNT).await
}

pub(crate) async fn seeded_fixture_with_read_many_pk_count(
    profile: StorageProfile,
    rows: &[WorkloadRow],
    read_many_by_pk_count: usize,
) -> SqlFixture {
    let fixture = empty_fixture_with_read_many_pk_count(profile, rows, read_many_by_pk_count).await;
    fixture.seed_json_rows().await;
    fixture.insert_untracked_probe().await;
    fixture
}

pub(crate) async fn seeded_bound_update_fixture_with_read_many_pk_count(
    profile: StorageProfile,
    rows: Vec<WorkloadRow>,
    read_many_by_pk_count: usize,
) -> SqlFixture {
    let row_count = rows.len();
    let mut fixture = empty_fixture_with_shape(
        profile,
        &rows,
        read_many_by_pk_count,
        FixtureShape::BoundUpdate,
    )
    .await;
    fixture.install_bound_seed_batch(rows);
    fixture.seed_json_rows().await;
    fixture.insert_untracked_probe().await;
    fixture.release_bound_update_setup();
    fixture.install_bound_update_batch(crate::workload::fixture_update_rows(row_count));
    fixture
}

impl SqlFixture {
    fn release_bound_update_setup(&mut self) {
        match self {
            Self::SQLite(fixture) => fixture.release_bound_update_setup(),
            Self::RocksDB(fixture) => fixture.release_bound_update_setup(),
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.release_bound_update_setup(),
        }
    }

    fn install_bound_seed_batch(&mut self, rows: Vec<WorkloadRow>) {
        match self {
            Self::SQLite(fixture) => fixture.install_bound_seed_batch(rows),
            Self::RocksDB(fixture) => fixture.install_bound_seed_batch(rows),
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.install_bound_seed_batch(rows),
        }
    }

    fn install_bound_update_batch(&mut self, rows: Vec<UpdateWorkloadRow>) {
        match self {
            Self::SQLite(fixture) => fixture.install_bound_update_batch(rows),
            Self::RocksDB(fixture) => fixture.install_bound_update_batch(rows),
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.install_bound_update_batch(rows),
        }
    }

    pub(crate) async fn insert_all(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.insert_all().await,
            Self::RocksDB(fixture) => fixture.insert_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.insert_all().await,
        }
    }

    async fn seed_json_rows(&self) {
        match self {
            Self::SQLite(fixture) => fixture.seed_json_rows().await,
            Self::RocksDB(fixture) => fixture.seed_json_rows().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.seed_json_rows().await,
        }
    }

    async fn insert_untracked_probe(&self) {
        match self {
            Self::SQLite(fixture) => fixture.insert_untracked_probe().await,
            Self::RocksDB(fixture) => fixture.insert_untracked_probe().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.insert_untracked_probe().await,
        }
    }

    pub(crate) async fn read_all(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.read_all().await,
            Self::RocksDB(fixture) => fixture.read_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_all().await,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) async fn read_all_result(&self) -> ExecuteResult {
        match self {
            Self::SQLite(fixture) => fixture.read_all_result().await,
            Self::RocksDB(fixture) => fixture.read_all_result().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_all_result().await,
        }
    }

    pub(crate) async fn read_many_by_pk(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.read_many_by_pk().await,
            Self::RocksDB(fixture) => fixture.read_many_by_pk().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_many_by_pk().await,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) async fn read_many_by_pk_result(&self) -> ExecuteResult {
        match self {
            Self::SQLite(fixture) => fixture.read_many_by_pk_result().await,
            Self::RocksDB(fixture) => fixture.read_many_by_pk_result().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_many_by_pk_result().await,
        }
    }

    pub(crate) async fn read_one_by_pk(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.read_one_by_pk().await,
            Self::RocksDB(fixture) => fixture.read_one_by_pk().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_one_by_pk().await,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) async fn read_one_by_pk_result(&self) -> ExecuteResult {
        match self {
            Self::SQLite(fixture) => fixture.read_one_by_pk_result().await,
            Self::RocksDB(fixture) => fixture.read_one_by_pk_result().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_one_by_pk_result().await,
        }
    }

    pub(crate) async fn update_all(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.update_all().await,
            Self::RocksDB(fixture) => fixture.update_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.update_all().await,
        }
    }

    /// Executes the tracked bulk-update workload through the public bound
    /// parameter surface. This is a profiling control for separating repeated
    /// literal SQL planning from the versioned write path.
    pub(crate) async fn update_all_bound(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.update_all_bound().await,
            Self::RocksDB(fixture) => fixture.update_all_bound().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.update_all_bound().await,
        }
    }

    pub(crate) async fn update_bound_rows(&self, row_count: usize) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.update_bound_rows(row_count).await,
            Self::RocksDB(fixture) => fixture.update_bound_rows(row_count).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.update_bound_rows(row_count).await,
        }
    }

    pub(crate) async fn update_spread_bound_rows(&self, row_count: usize) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.update_spread_bound_rows(row_count).await,
            Self::RocksDB(fixture) => fixture.update_spread_bound_rows(row_count).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.update_spread_bound_rows(row_count).await,
        }
    }

    pub(crate) async fn update_one_by_pk(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.update_one_by_pk().await,
            Self::RocksDB(fixture) => fixture.update_one_by_pk().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.update_one_by_pk().await,
        }
    }

    pub(crate) async fn delete_all(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.delete_all().await,
            Self::RocksDB(fixture) => fixture.delete_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.delete_all().await,
        }
    }

    pub(crate) async fn delete_one_by_pk(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.delete_one_by_pk().await,
            Self::RocksDB(fixture) => fixture.delete_one_by_pk().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.delete_one_by_pk().await,
        }
    }
}

impl<StorageImpl> GenericSqlFixture<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    fn release_bound_update_setup(&mut self) {
        self.bound_seed_json_batch = Arc::from([]);
    }

    fn install_bound_seed_batch(&mut self, rows: Vec<WorkloadRow>) {
        self.bound_seed_json_batch = rows
            .into_iter()
            .take(self.row_count)
            .map(|row| Arc::from(vec![Value::Text(row.path), Value::Text(row.value_json)]))
            .collect::<Vec<_>>()
            .into();
    }

    fn install_bound_update_batch(&mut self, rows: Vec<UpdateWorkloadRow>) {
        self.bound_update_all_batch = rows
            .into_iter()
            .take(self.row_count)
            .map(|row| {
                Arc::from(vec![
                    Value::Text(row.updated_value_json),
                    Value::Text(row.path),
                ])
            })
            .collect::<Vec<_>>()
            .into();
    }

    #[expect(clippy::cast_possible_truncation)]
    async fn insert_all(&self) -> usize {
        let _ =
            lix_engine::storage_bench::take_certified_entity_insert_parameter_batch_executions();
        let affected = self
            .session
            .execute_homogeneous_write_batch(
                Arc::from(BOUND_INSERT_ALL_SQL),
                Arc::clone(&self.bound_insert_all_batch),
            )
            .await
            .expect("execute tracked-state CRUD bound insert batch")
            .iter()
            .map(ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(affected as usize, self.row_count);
        assert_eq!(
            lix_engine::storage_bench::take_certified_entity_insert_parameter_batch_executions(),
            1,
            "tracked-state CRUD insert benchmark must execute one certified parameter batch"
        );
        affected as usize
    }

    async fn seed_json_rows(&self) {
        let affected = self
            .session
            .execute_homogeneous_write_batch(
                Arc::from(BOUND_SEED_JSON_SQL),
                Arc::clone(&self.bound_seed_json_batch),
            )
            .await
            .expect("execute tracked-state CRUD generated JSON seed batch")
            .iter()
            .map(ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(affected as usize, self.row_count);
    }

    async fn read_all(&self) -> usize {
        let result = std::hint::black_box(self.read_all_result().await);
        assert_eq!(result.len(), self.visible_row_count);
        result.len()
    }

    async fn insert_untracked_probe(&self) {
        if !self.untracked_fixture.has_untracked_row() {
            return;
        }
        let sql = format!(
            "INSERT INTO json_pointer (path, value, lixcol_untracked) VALUES ('{UNTRACKED_PROBE_PATH}', lix_json('{{\"lane\":\"untracked\"}}'), true)"
        );
        let affected = execute(&self.session, &sql).await.rows_affected();
        assert_eq!(affected, 1, "insert untracked overlay probe");
    }

    async fn read_all_result(&self) -> ExecuteResult {
        execute(&self.session, &self.select_all_sql).await
    }

    async fn read_many_by_pk(&self) -> usize {
        let result = std::hint::black_box(self.read_many_by_pk_result().await);
        assert_eq!(result.len(), self.read_many_by_pk_count);
        result.len()
    }

    async fn read_many_by_pk_result(&self) -> ExecuteResult {
        execute(&self.session, &self.select_many_by_pk_sql).await
    }

    async fn read_one_by_pk(&self) -> usize {
        let result = std::hint::black_box(self.read_one_by_pk_result().await);
        assert_eq!(result.len(), 1);
        result.len()
    }

    async fn read_one_by_pk_result(&self) -> ExecuteResult {
        execute(&self.session, &self.select_one_by_pk_sql).await
    }

    #[expect(clippy::cast_possible_truncation)]
    async fn update_all(&self) -> usize {
        let affected = match &self.update_all_workload {
            LiteralUpdateWorkload::Transaction(statements) => {
                Box::pin(execute_many_in_transaction(&self.session, statements)).await
            }
            LiteralUpdateWorkload::ExecuteBatch(statements) => self
                .session
                .execute_batch(statements)
                .await
                .expect("execute tracked-state CRUD SQL batch")
                .into_iter()
                .map(|result| result.rows_affected())
                .sum(),
        };
        assert_eq!(affected as usize, self.row_count);
        affected as usize
    }

    async fn update_all_bound(&self) -> usize {
        self.update_bound_rows(self.row_count).await
    }

    #[expect(clippy::cast_possible_truncation)]
    async fn update_bound_rows(&self, row_count: usize) -> usize {
        assert!(
            (1..=self.row_count).contains(&row_count),
            "bound update row count must be between 1 and {}, got {row_count}",
            self.row_count
        );
        let parameter_rows = if row_count == self.bound_update_all_batch.len() {
            Arc::clone(&self.bound_update_all_batch)
        } else {
            Arc::from(self.bound_update_all_batch[..row_count].to_vec())
        };
        let results = self
            .session
            .execute_homogeneous_write_batch(Arc::from(BOUND_UPDATE_ALL_SQL), parameter_rows)
            .await
            .expect("execute tracked-state CRUD bound update batch");
        let affected = results
            .iter()
            .map(ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(affected as usize, row_count);
        affected as usize
    }

    #[expect(clippy::cast_possible_truncation)]
    async fn update_spread_bound_rows(&self, row_count: usize) -> usize {
        assert!(
            (1..=self.row_count).contains(&row_count),
            "bound update row count must be between 1 and {}, got {row_count}",
            self.row_count
        );
        let last = self.row_count - 1;
        let parameter_rows = if row_count == 1 {
            vec![Arc::clone(&self.bound_update_all_batch[0])]
        } else {
            (0..row_count)
                .map(|index| {
                    Arc::clone(&self.bound_update_all_batch[index * last / (row_count - 1)])
                })
                .collect::<Vec<_>>()
        };
        let results = self
            .session
            .execute_homogeneous_write_batch(Arc::from(BOUND_UPDATE_ALL_SQL), parameter_rows.into())
            .await
            .expect("execute spread tracked-state CRUD bound update batch");
        let affected = results
            .iter()
            .map(ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(affected as usize, row_count);
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
        assert_eq!(affected as usize, self.visible_row_count);
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

fn fixture_for_session<StorageImpl>(
    session: SessionContext<StorageImpl>,
    rows: &[WorkloadRow],
    read_many_by_pk_count: usize,
    untracked_fixture: UntrackedFixture,
    shape: FixtureShape,
    dir: tempfile::TempDir,
) -> GenericSqlFixture<StorageImpl>
where
    StorageImpl: Storage,
{
    let tracked_rows = if untracked_fixture == UntrackedFixture::OneReadManyMember {
        // The untracked probe occupies one selected identity, so keep the
        // seeded and returned cardinality exactly equal to the baseline.
        &rows[..rows.len() - 1]
    } else {
        rows
    };
    let mid = tracked_rows.len() / 2;
    GenericSqlFixture {
        session,
        row_count: tracked_rows.len(),
        visible_row_count: tracked_rows.len() + usize::from(untracked_fixture.has_untracked_row()),
        untracked_fixture,
        read_many_by_pk_count,
        bound_insert_all_batch: if matches!(shape, FixtureShape::FullCrud) {
            tracked_rows
                .iter()
                .map(|row| {
                    Arc::from(vec![
                        Value::Text(row.path.clone()),
                        Value::Text(row.value_json.clone()),
                    ])
                })
                .collect::<Vec<_>>()
                .into()
        } else {
            Arc::from([])
        },
        bound_seed_json_batch: if matches!(shape, FixtureShape::FullCrud) {
            tracked_rows
                .iter()
                .map(|row| {
                    Arc::from(vec![
                        Value::Text(row.path.clone()),
                        Value::Text(row.value_json.clone()),
                    ])
                })
                .collect::<Vec<_>>()
                .into()
        } else {
            Arc::from([])
        },
        select_all_sql: "SELECT path, value FROM json_pointer ORDER BY path".to_string(),
        select_many_by_pk_sql: select_many_by_pk_sql(
            rows,
            read_many_by_pk_count,
            untracked_fixture,
        ),
        select_one_by_pk_sql: select_by_pk_sql(&tracked_rows[mid..][..1]),
        update_one_by_pk_sql: update_row_sql(&tracked_rows[mid]),
        update_all_workload: literal_update_workload(shape, tracked_rows),
        bound_update_all_batch: if matches!(shape, FixtureShape::FullCrud) {
            tracked_rows
                .iter()
                .map(|row| {
                    Arc::from(vec![
                        Value::Text(row.updated_value_json.clone()),
                        Value::Text(row.path.clone()),
                    ])
                })
                .collect::<Vec<_>>()
                .into()
        } else {
            Arc::from([])
        },
        delete_all_sql: "DELETE FROM json_pointer".to_string(),
        delete_one_by_pk_sql: format!(
            "DELETE FROM json_pointer WHERE path = '{}'",
            sql_string(tracked_rows[mid].path.as_str())
        ),
        _dir: dir,
    }
}

fn literal_update_workload(shape: FixtureShape, rows: &[WorkloadRow]) -> LiteralUpdateWorkload {
    if !matches!(shape, FixtureShape::FullCrud) {
        return LiteralUpdateWorkload::Transaction(Vec::new());
    }
    match std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_UPDATE_API").as_deref() {
        Ok("execute_batch") => LiteralUpdateWorkload::ExecuteBatch(
            rows.iter()
                .map(|row| ExecuteBatchStatement {
                    sql: update_row_sql(row),
                    params: Vec::new(),
                })
                .collect(),
        ),
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_UPDATE_API '{other}'; expected execute_batch"
        ),
        Err(_) => LiteralUpdateWorkload::Transaction(rows.iter().map(update_row_sql).collect()),
    }
}

/// Profile-only unified-current-state fixture switch. `one_unrelated` keeps
/// all selected rows tracked, proving an unrelated untracked row does not
/// change routing. `one_read_many_member` replaces one selected tracked
/// primary key with the untracked identity, proving normal SQL returns mixed
/// state without an explicit lane predicate.
fn profile_untracked_fixture() -> UntrackedFixture {
    match std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_UNTRACKED").as_deref() {
        Ok("one_unrelated") => UntrackedFixture::OneUnrelated,
        Ok("one_read_many_member") => UntrackedFixture::OneReadManyMember,
        Err(_) => UntrackedFixture::None,
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_UNTRACKED '{other}'; expected one_unrelated or one_read_many_member"
        ),
    }
}

async fn prepare_session<StorageImpl>(storage: StorageImpl) -> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    Engine::initialize(storage.clone())
        .await
        .expect("initialize tracked-state crud storage");
    let engine = Engine::new(storage)
        .await
        .expect("open tracked-state crud engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open tracked-state crud session");
    register_json_pointer_schema(&session).await;
    register_bulk_insert_schema(&session).await;
    session
}

async fn register_json_pointer_schema<StorageImpl>(session: &SessionContext<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
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

async fn register_bulk_insert_schema<StorageImpl>(session: &SessionContext<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "x-lix-key": "tracked_crud_insert",
        "x-lix-primary-key": ["/path"],
        "type": "object",
        "required": ["path", "value"],
        "properties": {
            "path": { "type": "string" },
            "value": { "type": "string" }
        },
        "additionalProperties": false
    });
    let affected = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register tracked_crud_insert schema")
        .rows_affected();
    assert_eq!(affected, 1);
}

async fn execute<StorageImpl>(session: &SessionContext<StorageImpl>, sql: &str) -> ExecuteResult
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(sql, &[])
        .await
        .expect("execute tracked-state crud SQL")
}

async fn execute_many_in_transaction<StorageImpl>(
    session: &SessionContext<StorageImpl>,
    statements: &[String],
) -> u64
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin tracked-state CRUD transaction");
    let mut affected = 0;
    for sql in statements {
        affected += transaction
            .execute(sql, &[])
            .await
            .expect("execute tracked-state CRUD transaction SQL")
            .rows_affected();
    }
    transaction
        .commit()
        .await
        .expect("commit tracked-state CRUD transaction");
    affected
}

fn select_by_pk_sql(rows: &[WorkloadRow]) -> String {
    select_by_paths_sql(rows.iter().map(|row| row.path.as_str()))
}

fn select_many_by_pk_sql(
    rows: &[WorkloadRow],
    read_many_by_pk_count: usize,
    untracked_fixture: UntrackedFixture,
) -> String {
    if untracked_fixture == UntrackedFixture::OneReadManyMember {
        assert!(
            read_many_by_pk_count >= 2,
            "one_read_many_member requires at least two selected primary keys"
        );
        return select_by_paths_sql(
            rows[..read_many_by_pk_count - 1]
                .iter()
                .map(|row| row.path.as_str())
                .chain(std::iter::once(UNTRACKED_PROBE_PATH)),
        );
    }
    select_by_pk_sql(&rows[..read_many_by_pk_count])
}

fn select_by_paths_sql<'a>(paths: impl IntoIterator<Item = &'a str>) -> String {
    format!(
        "SELECT path, value FROM json_pointer WHERE path IN ({}) ORDER BY path",
        paths
            .into_iter()
            .map(|path| format!("'{}'", sql_string(path)))
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
