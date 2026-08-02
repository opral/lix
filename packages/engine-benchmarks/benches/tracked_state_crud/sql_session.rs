use std::sync::Arc;
use std::{fmt::Write as _, ops::Range};

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
// These deliberately miss the native entity-read recognizer so profile mode
// can measure the general DataFusion execution path rather than a specialized
// public CRUD fast path.
const GENERAL_FILTER_SORT_SQL: &str =
    "SELECT path, value FROM json_pointer WHERE path IS NOT NULL ORDER BY value, path";
const GENERAL_AGGREGATE_SQL: &str = "SELECT COUNT(*) AS rows, MIN(path) AS first_path, MAX(path) AS last_path \
    FROM json_pointer WHERE path IS NOT NULL";
type SharedParameterBatch = Arc<[Arc<[Value]>]>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OlapReadShape {
    Scan,
    Filter,
    Sort,
    Group,
    Aggregate,
}

impl OlapReadShape {
    pub(crate) const ALL: [Self; 5] = [
        Self::Scan,
        Self::Filter,
        Self::Sort,
        Self::Group,
        Self::Aggregate,
    ];

    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Scan => "olap_scan",
            Self::Filter => "olap_filter",
            Self::Sort => "olap_sort",
            Self::Group => "olap_group",
            Self::Aggregate => "olap_aggregate",
        }
    }

    pub(crate) const fn sql(self) -> &'static str {
        match self {
            Self::Scan => {
                "WITH source AS (SELECT id, ordinal, lane, score, active FROM olap_row) \
                 SELECT id, ordinal, lane, score, active FROM source WHERE ordinal >= 0"
            }
            Self::Filter => {
                "WITH source AS (SELECT id, ordinal, lane, score, active FROM olap_row) \
                 SELECT ordinal, lane, score FROM source \
                 WHERE active = TRUE AND lane IN ('lane-07', 'lane-19') ORDER BY ordinal"
            }
            Self::Sort => {
                "WITH source AS (SELECT id, ordinal, lane, score, active FROM olap_row) \
                 SELECT id, ordinal, score FROM source WHERE active = TRUE \
                 ORDER BY score DESC, ordinal ASC LIMIT 10000"
            }
            Self::Group => {
                "WITH source AS (SELECT id, ordinal, lane, score, active FROM olap_row) \
                 SELECT lane, COUNT(*) AS rows, SUM(ordinal) AS ordinal_sum, \
                 AVG(score) AS score_avg, MIN(score) AS score_min, MAX(score) AS score_max \
                 FROM source WHERE active = TRUE GROUP BY lane ORDER BY lane"
            }
            Self::Aggregate => {
                "WITH source AS (SELECT id, ordinal, lane, score, active FROM olap_row) \
                 SELECT COUNT(*) AS rows, SUM(ordinal) AS ordinal_sum, AVG(score) AS score_avg, \
                 MIN(ordinal) AS min_ordinal, MAX(ordinal) AS max_ordinal \
                 FROM source WHERE active = TRUE"
            }
        }
    }

    pub(crate) fn from_label(label: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|shape| shape.label() == label)
    }
}

#[derive(Clone, Copy, Debug)]
struct OlapGroupExpected {
    rows: i64,
    ordinal_sum: i64,
    score_sum: f64,
    score_min: f64,
    score_max: f64,
}

#[derive(Clone, Debug)]
struct OlapExpected {
    active_rows: i64,
    active_ordinal_sum: i64,
    active_score_sum: f64,
    active_min_ordinal: i64,
    active_max_ordinal: i64,
    filtered_rows: usize,
    filtered_first_ordinal: i64,
    filtered_last_ordinal: i64,
    groups: [OlapGroupExpected; 32],
}

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
    Olap,
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
    olap_expected: Option<OlapExpected>,
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
    let fixture = if selected_olap_read_shape().is_some() {
        assert!(
            std::env::var_os("LIX_TRACKED_STATE_CRUD_PROFILE_UNTRACKED").is_none(),
            "typed OLAP profiles do not include the unrelated json_pointer overlay fixture"
        );
        empty_fixture_with_shape(profile, rows, read_many_by_pk_count, FixtureShape::Olap).await
    } else {
        empty_fixture_with_read_many_pk_count(profile, rows, read_many_by_pk_count).await
    };
    fixture.seed_rows().await;
    if selected_olap_read_shape().is_none() {
        fixture.insert_untracked_probe().await;
    }
    fixture
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) async fn seeded_olap_fixture(
    profile: StorageProfile,
    rows: &[WorkloadRow],
) -> SqlFixture {
    let fixture = empty_fixture_with_shape(
        profile,
        rows,
        READ_MANY_PK_COUNT.min(rows.len()),
        FixtureShape::Olap,
    )
    .await;
    fixture.seed_rows().await;
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
    fixture.seed_rows().await;
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

    async fn seed_rows(&self) {
        match self {
            Self::SQLite(fixture) => fixture.seed_rows().await,
            Self::RocksDB(fixture) => fixture.seed_rows().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.seed_rows().await,
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
        let shape = std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_READ_SHAPE");
        if let Ok(label) = shape.as_deref()
            && let Some(shape) = OlapReadShape::from_label(label)
        {
            return self.read_olap(shape).await;
        }
        match shape.as_deref() {
            Ok("aggregate_count") => return self.count_all().await,
            Ok("general_filter_sort") => return self.general_filter_sort_all().await,
            Ok("general_aggregate") => return self.general_aggregate().await,
            Ok("full_result") | Err(_) => {}
            Ok(other) => panic!(
                "unknown LIX_TRACKED_STATE_CRUD_PROFILE_READ_SHAPE '{other}'; expected full_result, aggregate_count, general_filter_sort, general_aggregate, olap_scan, olap_filter, olap_sort, olap_group, or olap_aggregate"
            ),
        }
        match self {
            Self::SQLite(fixture) => fixture.read_all().await,
            Self::RocksDB(fixture) => fixture.read_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_all().await,
        }
    }

    /// Opt-in OLAP profile shape. Keeping it behind the profile selector
    /// leaves the historical full-result CRUD benchmark unchanged while
    /// making the exact scan-vs-aggregate boundary reproducible at 1M rows.
    async fn count_all(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.count_all().await,
            Self::RocksDB(fixture) => fixture.count_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.count_all().await,
        }
    }

    async fn general_filter_sort_all(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.general_filter_sort_all().await,
            Self::RocksDB(fixture) => fixture.general_filter_sort_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.general_filter_sort_all().await,
        }
    }

    async fn general_aggregate(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.general_aggregate().await,
            Self::RocksDB(fixture) => fixture.general_aggregate().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.general_aggregate().await,
        }
    }

    pub(crate) async fn read_olap(&self, shape: OlapReadShape) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.read_olap(shape).await,
            Self::RocksDB(fixture) => fixture.read_olap(shape).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_olap(shape).await,
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

    async fn seed_rows(&self) {
        if self.olap_expected.is_some() {
            self.seed_olap_rows().await;
            return;
        }
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

    async fn seed_olap_rows(&self) {
        const ROWS_PER_STATEMENT: usize = 4_096;
        let mut transaction = self
            .session
            .begin_transaction()
            .await
            .expect("begin typed OLAP seed transaction");
        let mut affected = 0_u64;
        for start in (0..self.row_count).step_by(ROWS_PER_STATEMENT) {
            let end = (start + ROWS_PER_STATEMENT).min(self.row_count);
            affected += transaction
                .execute(&olap_insert_sql(start..end), &[])
                .await
                .expect("execute typed OLAP seed statement")
                .rows_affected();
        }
        transaction
            .commit()
            .await
            .expect("commit typed OLAP seed transaction");
        assert_eq!(
            usize::try_from(affected).expect("OLAP affected rows fit usize"),
            self.row_count
        );
    }

    async fn read_all(&self) -> usize {
        let result = std::hint::black_box(self.read_all_result().await);
        assert_eq!(result.len(), self.visible_row_count);
        result.len()
    }

    async fn count_all(&self) -> usize {
        let result = std::hint::black_box(
            execute(&self.session, "SELECT COUNT(*) AS count FROM json_pointer").await,
        );
        assert_eq!(result.columns(), ["count"]);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result.rows()[0].get_index(0),
            Some(&Value::Integer(self.visible_row_count as i64))
        );
        1
    }

    async fn general_filter_sort_all(&self) -> usize {
        let result = std::hint::black_box(execute(&self.session, GENERAL_FILTER_SORT_SQL).await);
        assert_eq!(result.len(), self.visible_row_count);
        result.len()
    }

    async fn general_aggregate(&self) -> usize {
        let result = std::hint::black_box(execute(&self.session, GENERAL_AGGREGATE_SQL).await);
        assert_eq!(result.columns(), ["rows", "first_path", "last_path"]);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result.rows()[0].get_index(0),
            Some(&Value::Integer(self.visible_row_count as i64))
        );
        1
    }

    async fn read_olap(&self, shape: OlapReadShape) -> usize {
        let expected = self
            .olap_expected
            .as_ref()
            .expect("typed OLAP query requires a typed OLAP fixture");
        let result = std::hint::black_box(execute(&self.session, shape.sql()).await);
        match shape {
            OlapReadShape::Scan => {
                assert_eq!(
                    result.columns(),
                    ["id", "ordinal", "lane", "score", "active"]
                );
                assert_eq!(result.len(), self.row_count);
            }
            OlapReadShape::Filter => {
                assert_eq!(result.columns(), ["ordinal", "lane", "score"]);
                assert_eq!(result.len(), expected.filtered_rows);
                assert_eq!(integer_at(&result, 0, 0), expected.filtered_first_ordinal);
                assert_eq!(
                    integer_at(&result, result.len() - 1, 0),
                    expected.filtered_last_ordinal
                );
            }
            OlapReadShape::Sort => assert_olap_sort(&result, expected),
            OlapReadShape::Group => assert_olap_group(&result, expected),
            OlapReadShape::Aggregate => assert_olap_aggregate(&result, expected),
        }
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
    let olap_expected = if matches!(shape, FixtureShape::Olap) {
        Some(olap_expected(tracked_rows.len()))
    } else {
        None
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
        olap_expected,
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

fn olap_expected(row_count: usize) -> OlapExpected {
    assert!(
        row_count >= 20,
        "typed OLAP fixture needs at least 20 rows to populate both selected lanes"
    );
    let mut active_rows = 0_i64;
    let mut active_ordinal_sum = 0_i64;
    let mut active_score_sum = 0.0;
    let mut active_min_ordinal = None;
    let mut active_max_ordinal = None;
    let mut filtered_rows = 0;
    let mut filtered_first_ordinal = None;
    let mut filtered_last_ordinal = None;
    let mut groups = [OlapGroupExpected {
        rows: 0,
        ordinal_sum: 0,
        score_sum: 0.0,
        score_min: f64::INFINITY,
        score_max: f64::NEG_INFINITY,
    }; 32];

    for index in 0..row_count {
        let ordinal = i64::try_from(index).expect("OLAP row ordinal must fit in i64");
        let lane_index = index % groups.len();
        #[expect(clippy::cast_precision_loss)]
        let score = (index % 10_000) as f64 / 8.0;
        let active = index % 3 != 0;
        if !active {
            continue;
        }

        active_rows += 1;
        active_ordinal_sum += ordinal;
        active_score_sum += score;
        active_min_ordinal.get_or_insert(ordinal);
        active_max_ordinal = Some(ordinal);
        let group = &mut groups[lane_index];
        group.rows += 1;
        group.ordinal_sum += ordinal;
        group.score_sum += score;
        group.score_min = group.score_min.min(score);
        group.score_max = group.score_max.max(score);
        if matches!(lane_index, 7 | 19) {
            filtered_rows += 1;
            filtered_first_ordinal.get_or_insert(ordinal);
            filtered_last_ordinal = Some(ordinal);
        }
    }

    OlapExpected {
        active_rows,
        active_ordinal_sum,
        active_score_sum,
        active_min_ordinal: active_min_ordinal.expect("OLAP fixture needs an active row"),
        active_max_ordinal: active_max_ordinal.expect("OLAP fixture needs an active row"),
        filtered_rows,
        filtered_first_ordinal: filtered_first_ordinal
            .expect("OLAP fixture needs a selected lane row"),
        filtered_last_ordinal: filtered_last_ordinal
            .expect("OLAP fixture needs a selected lane row"),
        groups,
    }
}

fn olap_insert_sql(ordinals: Range<usize>) -> String {
    let mut sql = String::from("INSERT INTO olap_row (id, ordinal, lane, score, active) VALUES ");
    for (position, ordinal) in ordinals.enumerate() {
        if position != 0 {
            sql.push(',');
        }
        let lane = ordinal % 32;
        #[expect(clippy::cast_precision_loss)]
        let score = (ordinal % 10_000) as f64 / 8.0;
        let active = if ordinal % 3 == 0 { "FALSE" } else { "TRUE" };
        write!(
            sql,
            "('/~lix-olap/{ordinal:09}', {ordinal}, 'lane-{lane:02}', {score}, {active})"
        )
        .expect("write typed OLAP INSERT SQL");
    }
    sql
}

fn assert_olap_sort(result: &ExecuteResult, expected: &OlapExpected) {
    assert_eq!(result.columns(), ["id", "ordinal", "score"]);
    assert_eq!(result.len(), 10_000.min(expected.active_rows as usize));
    let mut previous = None;
    for row_index in 0..result.len() {
        let ordinal = integer_at(result, row_index, 1);
        let score = real_at(result, row_index, 2);
        assert_ne!(ordinal % 3, 0, "sorted result must retain active rows only");
        assert_eq!(
            text_at(result, row_index, 0),
            format!("/~lix-olap/{ordinal:09}")
        );
        #[expect(clippy::cast_precision_loss)]
        let expected_score = (ordinal.rem_euclid(10_000)) as f64 / 8.0;
        assert_eq!(score, expected_score);
        if let Some((previous_score, previous_ordinal)) = previous {
            assert!(
                previous_score > score || (previous_score == score && previous_ordinal < ordinal),
                "OLAP top-k must be ordered by score DESC, ordinal ASC"
            );
        }
        previous = Some((score, ordinal));
    }
}

fn assert_olap_group(result: &ExecuteResult, expected: &OlapExpected) {
    assert_eq!(
        result.columns(),
        [
            "lane",
            "rows",
            "ordinal_sum",
            "score_avg",
            "score_min",
            "score_max"
        ]
    );
    assert_eq!(result.len(), expected.groups.len());
    let mut total_rows = 0;
    let mut total_ordinal_sum = 0;
    for (lane_index, group) in expected.groups.iter().enumerate() {
        assert_eq!(
            text_at(result, lane_index, 0),
            format!("lane-{lane_index:02}")
        );
        assert_eq!(integer_at(result, lane_index, 1), group.rows);
        assert_eq!(integer_at(result, lane_index, 2), group.ordinal_sum);
        assert_close(
            real_at(result, lane_index, 3),
            group.score_sum / group.rows as f64,
        );
        assert_eq!(real_at(result, lane_index, 4), group.score_min);
        assert_eq!(real_at(result, lane_index, 5), group.score_max);
        total_rows += group.rows;
        total_ordinal_sum += group.ordinal_sum;
    }
    assert_eq!(total_rows, expected.active_rows);
    assert_eq!(total_ordinal_sum, expected.active_ordinal_sum);
}

fn assert_olap_aggregate(result: &ExecuteResult, expected: &OlapExpected) {
    assert_eq!(
        result.columns(),
        [
            "rows",
            "ordinal_sum",
            "score_avg",
            "min_ordinal",
            "max_ordinal"
        ]
    );
    assert_eq!(result.len(), 1);
    assert_eq!(integer_at(result, 0, 0), expected.active_rows);
    assert_eq!(integer_at(result, 0, 1), expected.active_ordinal_sum);
    assert_close(
        real_at(result, 0, 2),
        expected.active_score_sum / expected.active_rows as f64,
    );
    assert_eq!(integer_at(result, 0, 3), expected.active_min_ordinal);
    assert_eq!(integer_at(result, 0, 4), expected.active_max_ordinal);
}

fn integer_at(result: &ExecuteResult, row: usize, column: usize) -> i64 {
    match result.rows()[row].get_index(column) {
        Some(Value::Integer(value)) => *value,
        value => panic!("expected integer at row {row}, column {column}, got {value:?}"),
    }
}

fn real_at(result: &ExecuteResult, row: usize, column: usize) -> f64 {
    match result.rows()[row].get_index(column) {
        Some(Value::Real(value)) => *value,
        value => panic!("expected real at row {row}, column {column}, got {value:?}"),
    }
}

fn text_at(result: &ExecuteResult, row: usize, column: usize) -> &str {
    match result.rows()[row].get_index(column) {
        Some(Value::Text(value)) => value,
        value => panic!("expected text at row {row}, column {column}, got {value:?}"),
    }
}

fn assert_close(actual: f64, expected: f64) {
    let tolerance = 1e-10 * expected.abs().max(1.0);
    assert!(
        (actual - expected).abs() <= tolerance,
        "expected {expected} within {tolerance}, got {actual}"
    );
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

pub(crate) fn selected_olap_read_shape() -> Option<OlapReadShape> {
    std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_READ_SHAPE")
        .ok()
        .as_deref()
        .and_then(OlapReadShape::from_label)
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
    register_olap_schema(&session).await;
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

async fn register_olap_schema<StorageImpl>(session: &SessionContext<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "x-lix-key": "olap_row",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "ordinal", "lane", "score", "active"],
        "properties": {
            "id": { "type": "string" },
            "ordinal": { "type": "integer" },
            "lane": { "type": "string" },
            "score": { "type": "number" },
            "active": { "type": "boolean" }
        },
        "additionalProperties": false
    });
    let affected = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register olap_row schema")
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
