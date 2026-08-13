use std::sync::Arc;
use std::{fmt::Write as _, ops::Range};

use lix::storage::Storage;
use lix::{ExecuteBatchStatement, ExecuteResult, LixError, Value};
use lix::{Lix, open_lix};

#[cfg(feature = "slatedb")]
use crate::storage::SlateDB;
use crate::storage::{ProfileStorage, RocksDB, StorageProfile};
use crate::workload::{UpdateWorkloadRow, WorkloadRow, sql_string};

const READ_MANY_PK_COUNT: usize = crate::READ_MANY_PK_COUNT;
const BOUND_INSERT_ALL_SQL: &str = "INSERT INTO tracked_crud_insert (path, value) VALUES ($1, $2)";
const BOUND_SEED_JSON_SQL: &str =
    "INSERT INTO json_pointer (path, value) VALUES ($1, lix_json($2))";
const BOUND_UPDATE_ALL_SQL: &str = "UPDATE json_pointer SET value = lix_json($1) WHERE path = $2";
const BOUND_OLAP_UPDATE_LANE_SQL: &str = "UPDATE olap_row SET lane = $1 WHERE id = $2";
const BOUND_OLAP_UPDATE_SCORE_SQL: &str = "UPDATE olap_row SET score = $1 WHERE id = $2";
const BOUND_OLAP_UPDATE_ACTIVE_SQL: &str = "UPDATE olap_row SET active = $1 WHERE id = $2";
const OLAP_ROWS_PER_STATEMENT: usize = 4_096;
const UNTRACKED_PROBE_PATH: &str = "/__lix_untracked_probe";
// These deliberately miss the native entity-read recognizer so profile mode
// can measure the general DataFusion execution path rather than a specialized
// public CRUD fast path.
const GENERAL_FILTER_SORT_SQL: &str =
    "SELECT path, value FROM json_pointer WHERE path IS NOT NULL ORDER BY value, path";
const GENERAL_AGGREGATE_SQL: &str = "SELECT COUNT(*) AS rows, MIN(path) AS first_path, MAX(path) AS last_path \
    FROM json_pointer WHERE path IS NOT NULL";
#[derive(Clone, Default)]
struct SharedParameterBatch {
    rows: Arc<[Vec<Value>]>,
}

impl SharedParameterBatch {
    fn from_rows(rows: impl IntoIterator<Item = Vec<Value>>) -> Result<Self, LixError> {
        let rows = rows.into_iter().collect::<Vec<_>>();
        if let Some(width) = rows.first().map(Vec::len)
            && rows.iter().any(|row| row.len() != width)
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "SQL parameter batch must be rectangular",
            ));
        }
        Ok(Self { rows: rows.into() })
    }

    fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn row_values(&self, index: usize) -> Result<Vec<Value>, LixError> {
        self.rows.get(index).cloned().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("SQL parameter row {index} is out of bounds"),
            )
        })
    }
}

async fn execute_parameter_batch<S>(
    lix: &Lix<S>,
    sql: Arc<str>,
    parameters: SharedParameterBatch,
) -> Result<Vec<ExecuteResult>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let statements = parameters
        .rows
        .iter()
        .map(|params| ExecuteBatchStatement {
            label: None,
            sql: sql.to_string(),
            params: params.clone(),
        })
        .collect::<Vec<_>>();
    lix.execute_batch(&statements).await
}

/// Folds one public cell into an accumulator, reading the whole payload.
///
/// Every variant is walked byte by byte so a value that only *looks*
/// materialized — a lazily decoded string, a JSON payload that still has to be
/// realized, a blob that has not been copied yet — has to pay its real cost
/// inside the timing window. The result is returned so nothing can be dropped
/// as dead code.
fn fold_value(accumulator: u64, value: &Value) -> u64 {
    fn fold_bytes(mut accumulator: u64, tag: u8, bytes: &[u8]) -> u64 {
        accumulator = accumulator.rotate_left(5) ^ u64::from(tag);
        for byte in bytes {
            accumulator = accumulator.wrapping_mul(0x0000_0100_0000_01b3) ^ u64::from(*byte);
        }
        accumulator
    }

    match value {
        Value::Null => fold_bytes(accumulator, 0, &[]),
        Value::Boolean(value) => fold_bytes(accumulator, 1, &[u8::from(*value)]),
        Value::Integer(value) => fold_bytes(accumulator, 2, &value.to_le_bytes()),
        Value::Real(value) => fold_bytes(accumulator, 3, &value.to_bits().to_le_bytes()),
        Value::Text(value) => fold_bytes(accumulator, 4, value.as_bytes()),
        Value::Json(value) => fold_bytes(accumulator, 5, value.as_bytes()),
        Value::Blob(value) => fold_bytes(accumulator, 6, value.as_bytes()),
    }
}

fn empty_parameter_batch() -> SharedParameterBatch {
    SharedParameterBatch::from_rows(std::iter::empty::<Vec<Value>>())
        .expect("empty prepared parameter batch is valid")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OlapReadShape {
    Scan,
    Filter,
    Sort,
    Group,
    Aggregate,
    Join,
}

impl OlapReadShape {
    pub(crate) const ALL: [Self; 6] = [
        Self::Scan,
        Self::Filter,
        Self::Sort,
        Self::Group,
        Self::Aggregate,
        Self::Join,
    ];

    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Scan => "olap_scan",
            Self::Filter => "olap_filter",
            Self::Sort => "olap_sort",
            Self::Group => "olap_group",
            Self::Aggregate => "olap_aggregate",
            Self::Join => "olap_join",
        }
    }

    pub(crate) const fn sql(self) -> &'static str {
        match self {
            Self::Scan => {
                "SELECT id, ordinal, lane, score, active FROM olap_row WHERE ordinal >= 0"
            }
            Self::Filter => {
                "SELECT ordinal, lane, score FROM olap_row \
                 WHERE active = TRUE AND lane IN ('lane-07', 'lane-19') ORDER BY ordinal"
            }
            Self::Sort => {
                "SELECT id, ordinal, score FROM olap_row WHERE active = TRUE \
                 ORDER BY score DESC, ordinal ASC LIMIT 10000"
            }
            Self::Group => {
                "SELECT lane, COUNT(*) AS rows, SUM(ordinal) AS ordinal_sum, \
                 AVG(score) AS score_avg, MIN(score) AS score_min, MAX(score) AS score_max \
                 FROM olap_row WHERE active = TRUE GROUP BY lane ORDER BY lane"
            }
            Self::Aggregate => {
                "SELECT COUNT(*) AS rows, SUM(ordinal) AS ordinal_sum, AVG(score) AS score_avg, \
                 MIN(ordinal) AS min_ordinal, MAX(ordinal) AS max_ordinal \
                 FROM olap_row WHERE active = TRUE"
            }
            Self::Join => {
                "SELECT left_row.id, right_row.score FROM olap_row AS left_row \
                 JOIN olap_row AS right_row ON left_row.id = right_row.id \
                 WHERE left_row.active = TRUE ORDER BY left_row.ordinal"
            }
        }
    }

    pub(crate) fn from_label(label: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|shape| shape.label() == label)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OlapMutationProfile {
    Pristine,
    Sparse,
    Moderate,
}

impl OlapMutationProfile {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Pristine => "pristine",
            Self::Sparse => "sparse",
            Self::Moderate => "moderate",
        }
    }

    const fn stride(self) -> Option<usize> {
        match self {
            Self::Pristine => None,
            Self::Sparse => Some(1_000),
            Self::Moderate => Some(10),
        }
    }
}

#[derive(Clone, Debug)]
struct OlapRowExpected {
    id: String,
    ordinal: i64,
    lane_index: usize,
    score: f64,
    active: bool,
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
    initial_row_count: usize,
    mutation_profile: OlapMutationProfile,
    visible_rows: usize,
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

enum UpdateWorkload {
    Transaction(Vec<String>),
    ExecuteBatch(Vec<ExecuteBatchStatement>),
    PreparedDml(SharedParameterBatch),
}

impl UntrackedFixture {
    const fn has_untracked_row(self) -> bool {
        !matches!(self, Self::None)
    }
}

pub(crate) enum SqlFixture {
    RocksDB(GenericSqlFixture<RocksDB>),
    #[cfg(feature = "slatedb")]
    SlateDB(GenericSqlFixture<SlateDB>),
}

pub(crate) struct GenericSqlFixture<StorageImpl: Storage + Clone + Send + Sync + 'static> {
    session: Lix<StorageImpl>,
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
    update_all_workload: UpdateWorkload,
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
    if selected_olap_read_shape().is_some() {
        fixture
            .seed_rows_with_olap_mutations(selected_olap_mutation_profile())
            .await;
    } else {
        fixture.seed_rows().await;
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
    seeded_olap_fixture_with_mutations(profile, rows, OlapMutationProfile::Pristine).await
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) async fn seeded_olap_fixture_with_mutations(
    profile: StorageProfile,
    rows: &[WorkloadRow],
    mutation_profile: OlapMutationProfile,
) -> SqlFixture {
    let mut fixture = empty_fixture_with_shape(
        profile,
        rows,
        READ_MANY_PK_COUNT.min(rows.len()),
        FixtureShape::Olap,
    )
    .await;
    fixture.install_olap_mutation_profile(mutation_profile);
    fixture
        .seed_rows_with_olap_mutations(mutation_profile)
        .await;
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
    fn install_olap_mutation_profile(&mut self, mutation_profile: OlapMutationProfile) {
        match self {
            Self::RocksDB(fixture) => fixture.install_olap_mutation_profile(mutation_profile),
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.install_olap_mutation_profile(mutation_profile),
        }
    }

    fn release_bound_update_setup(&mut self) {
        match self {
            Self::RocksDB(fixture) => fixture.release_bound_update_setup(),
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.release_bound_update_setup(),
        }
    }

    fn install_bound_seed_batch(&mut self, rows: Vec<WorkloadRow>) {
        match self {
            Self::RocksDB(fixture) => fixture.install_bound_seed_batch(rows),
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.install_bound_seed_batch(rows),
        }
    }

    fn install_bound_update_batch(&mut self, rows: Vec<UpdateWorkloadRow>) {
        match self {
            Self::RocksDB(fixture) => fixture.install_bound_update_batch(rows),
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.install_bound_update_batch(rows),
        }
    }

    pub(crate) async fn insert_all(&self) -> usize {
        match self {
            Self::RocksDB(fixture) => fixture.insert_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.insert_all().await,
        }
    }

    pub(crate) async fn active_commit_id(&self) -> String {
        match self {
            Self::RocksDB(fixture) => fixture.active_commit_id().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.active_commit_id().await,
        }
    }

    pub(crate) async fn columnar_history_count(&self, commit_id: &str) -> usize {
        match self {
            Self::RocksDB(fixture) => fixture.columnar_history_count(commit_id).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.columnar_history_count(commit_id).await,
        }
    }

    pub(crate) async fn columnar_diff_count(&self, before: &str, after: &str) -> usize {
        match self {
            Self::RocksDB(fixture) => fixture.columnar_diff_count(before, after).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.columnar_diff_count(before, after).await,
        }
    }

    async fn seed_rows(&self) {
        match self {
            Self::RocksDB(fixture) => fixture.seed_rows().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.seed_rows().await,
        }
    }

    async fn seed_rows_with_olap_mutations(&self, mutation_profile: OlapMutationProfile) {
        self.seed_rows().await;
        if !matches!(mutation_profile, OlapMutationProfile::Pristine) {
            self.apply_olap_mutations(mutation_profile).await;
        }
    }

    async fn apply_olap_mutations(&self, mutation_profile: OlapMutationProfile) {
        match self {
            Self::RocksDB(fixture) => fixture.apply_olap_mutations(mutation_profile).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.apply_olap_mutations(mutation_profile).await,
        }
    }

    async fn insert_untracked_probe(&self) {
        match self {
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
            return self.read_olap_timed(shape).await;
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
            Self::RocksDB(fixture) => fixture.count_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.count_all().await,
        }
    }

    async fn general_filter_sort_all(&self) -> usize {
        match self {
            Self::RocksDB(fixture) => fixture.general_filter_sort_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.general_filter_sort_all().await,
        }
    }

    async fn general_aggregate(&self) -> usize {
        match self {
            Self::RocksDB(fixture) => fixture.general_aggregate().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.general_aggregate().await,
        }
    }

    pub(crate) async fn read_olap(&self, shape: OlapReadShape) -> usize {
        match self {
            Self::RocksDB(fixture) => fixture.read_olap(shape).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_olap(shape).await,
        }
    }

    async fn read_olap_timed(&self, shape: OlapReadShape) -> usize {
        match self {
            Self::RocksDB(fixture) => fixture.read_olap_timed(shape).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_olap_timed(shape).await,
        }
    }

    pub(crate) async fn validate_selected_olap(&self) {
        if let Some(shape) = selected_olap_read_shape() {
            self.read_olap(shape).await;
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) async fn read_all_result(&self) -> ExecuteResult {
        match self {
            Self::RocksDB(fixture) => fixture.read_all_result().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_all_result().await,
        }
    }

    pub(crate) async fn read_all_rows_consumed(&self) -> u64 {
        match self {
            Self::RocksDB(fixture) => fixture.read_all_rows_consumed().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_all_rows_consumed().await,
        }
    }

    pub(crate) async fn read_many_by_pk(&self) -> usize {
        match self {
            Self::RocksDB(fixture) => fixture.read_many_by_pk().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_many_by_pk().await,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) async fn read_many_by_pk_result(&self) -> ExecuteResult {
        match self {
            Self::RocksDB(fixture) => fixture.read_many_by_pk_result().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_many_by_pk_result().await,
        }
    }

    pub(crate) async fn read_one_by_pk(&self) -> usize {
        match self {
            Self::RocksDB(fixture) => fixture.read_one_by_pk().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_one_by_pk().await,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) async fn read_one_by_pk_result(&self) -> ExecuteResult {
        match self {
            Self::RocksDB(fixture) => fixture.read_one_by_pk_result().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.read_one_by_pk_result().await,
        }
    }

    pub(crate) async fn update_all(&self) -> usize {
        match self {
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
            Self::RocksDB(fixture) => fixture.update_all_bound().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.update_all_bound().await,
        }
    }

    pub(crate) async fn update_bound_rows(&self, row_count: usize) -> usize {
        match self {
            Self::RocksDB(fixture) => fixture.update_bound_rows(row_count).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.update_bound_rows(row_count).await,
        }
    }

    pub(crate) async fn update_spread_bound_rows(&self, row_count: usize) -> usize {
        match self {
            Self::RocksDB(fixture) => fixture.update_spread_bound_rows(row_count).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.update_spread_bound_rows(row_count).await,
        }
    }

    pub(crate) async fn update_one_by_pk(&self) -> usize {
        match self {
            Self::RocksDB(fixture) => fixture.update_one_by_pk().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.update_one_by_pk().await,
        }
    }

    pub(crate) async fn delete_all(&self) -> usize {
        match self {
            Self::RocksDB(fixture) => fixture.delete_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(fixture) => fixture.delete_all().await,
        }
    }

    pub(crate) async fn delete_one_by_pk(&self) -> usize {
        match self {
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
    fn install_olap_mutation_profile(&mut self, mutation_profile: OlapMutationProfile) {
        assert!(self.olap_expected.is_some(), "typed OLAP fixture required");
        self.olap_expected = Some(olap_expected(self.row_count, mutation_profile));
    }

    fn release_bound_update_setup(&mut self) {
        self.bound_seed_json_batch = empty_parameter_batch();
    }

    fn install_bound_seed_batch(&mut self, rows: Vec<WorkloadRow>) {
        self.bound_seed_json_batch = SharedParameterBatch::from_rows(
            rows.into_iter()
                .take(self.row_count)
                .map(|row| vec![Value::Text(row.path), Value::Text(row.value_json)]),
        )
        .expect("seed parameter batch is rectangular");
    }

    fn install_bound_update_batch(&mut self, rows: Vec<UpdateWorkloadRow>) {
        self.bound_update_all_batch = SharedParameterBatch::from_rows(
            rows.into_iter()
                .take(self.row_count)
                .map(|row| vec![Value::Text(row.updated_value_json), Value::Text(row.path)]),
        )
        .expect("update parameter batch is rectangular");
    }

    #[expect(clippy::cast_possible_truncation)]
    async fn insert_all(&self) -> usize {
        let affected = execute_parameter_batch(
            &self.session,
            Arc::from(BOUND_INSERT_ALL_SQL),
            self.bound_insert_all_batch.clone(),
        )
        .await
        .expect("execute tracked-state CRUD bound insert batch")
        .iter()
        .map(ExecuteResult::rows_affected)
        .sum::<u64>();
        assert_eq!(affected as usize, self.row_count);
        affected as usize
    }

    async fn active_commit_id(&self) -> String {
        let result = execute(&self.session, "SELECT lix_active_branch_commit_id()").await;
        match result.rows()[0].get_index(0) {
            Some(Value::Text(commit_id)) => commit_id.clone(),
            value => panic!("active commit id should be text, got {value:?}"),
        }
    }

    async fn columnar_history_count(&self, commit_id: &str) -> usize {
        let result = execute(
            &self.session,
            &format!(
                "SELECT COUNT(*) AS entries FROM tracked_crud_insert_history('{commit_id}') \
                 WHERE lixcol_is_deleted = false"
            ),
        )
        .await;
        assert_eq!(
            result.rows()[0].get_index(0),
            Some(&Value::Integer(
                i64::try_from(self.row_count).expect("benchmark row count fits i64"),
            ))
        );
        self.row_count
    }

    async fn columnar_diff_count(&self, before: &str, after: &str) -> usize {
        let result = execute(
            &self.session,
            &format!(
                "SELECT COUNT(*) AS entries FROM lix_diff('{before}', '{after}') \
                 WHERE schema_key = 'tracked_crud_insert' AND diff_type = 'added'"
            ),
        )
        .await;
        assert_eq!(
            result.rows()[0].get_index(0),
            Some(&Value::Integer(
                i64::try_from(self.row_count).expect("benchmark row count fits i64"),
            ))
        );
        self.row_count
    }

    async fn seed_rows(&self) {
        if self.olap_expected.is_some() {
            self.seed_olap_rows().await;
            return;
        }
        let affected = execute_parameter_batch(
            &self.session,
            Arc::from(BOUND_SEED_JSON_SQL),
            self.bound_seed_json_batch.clone(),
        )
        .await
        .expect("execute tracked-state CRUD generated JSON seed batch")
        .iter()
        .map(ExecuteResult::rows_affected)
        .sum::<u64>();
        assert_eq!(affected as usize, self.row_count);
    }

    async fn seed_olap_rows(&self) {
        let mut transaction = self
            .session
            .begin_transaction()
            .await
            .expect("begin typed OLAP seed transaction");
        let mut affected = 0_u64;
        for start in (0..self.row_count).step_by(OLAP_ROWS_PER_STATEMENT) {
            let end = (start + OLAP_ROWS_PER_STATEMENT).min(self.row_count);
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

    async fn apply_olap_mutations(&self, mutation_profile: OlapMutationProfile) {
        let stride = mutation_profile
            .stride()
            .expect("pristine OLAP fixtures do not apply mutations");
        let updated_rows = (1..self.row_count)
            .step_by(stride)
            .map(|ordinal| {
                olap_expected_row(self.row_count, mutation_profile, ordinal)
                    .expect("updated OLAP row remains visible")
            })
            .collect::<Vec<_>>();
        let update_lane_rows = updated_rows
            .iter()
            .map(|row| {
                vec![
                    Value::Text(olap_lane(row.lane_index)),
                    Value::Text(row.id.clone()),
                ]
            })
            .collect::<Vec<_>>();
        let update_score_rows = updated_rows
            .iter()
            .map(|row| vec![Value::Real(row.score), Value::Text(row.id.clone())])
            .collect::<Vec<_>>();
        let update_active_rows = updated_rows
            .iter()
            .map(|row| vec![Value::Boolean(row.active), Value::Text(row.id.clone())])
            .collect::<Vec<_>>();
        let delete_ordinals = (2..self.row_count).step_by(stride).collect::<Vec<_>>();

        for (sql, parameter_rows, expected_affected) in [
            (
                BOUND_OLAP_UPDATE_LANE_SQL,
                update_lane_rows,
                mutation_count(self.row_count, 1, stride),
            ),
            (
                BOUND_OLAP_UPDATE_SCORE_SQL,
                update_score_rows,
                mutation_count(self.row_count, 1, stride),
            ),
            (
                BOUND_OLAP_UPDATE_ACTIVE_SQL,
                update_active_rows,
                mutation_count(self.row_count, 1, stride),
            ),
        ] {
            let affected = execute_parameter_batch(
                &self.session,
                Arc::from(sql),
                SharedParameterBatch::from_rows(parameter_rows)
                    .expect("OLAP parameter batch is rectangular"),
            )
            .await
            .unwrap_or_else(|error| panic!("execute typed OLAP mutation batch '{sql}': {error:?}"))
            .iter()
            .map(ExecuteResult::rows_affected)
            .sum::<u64>();
            assert_eq!(
                usize::try_from(affected).expect("OLAP mutation count fits usize"),
                expected_affected
            );
        }

        let mut transaction = self
            .session
            .begin_transaction()
            .await
            .expect("begin typed OLAP delete transaction");
        let mut deleted = 0_u64;
        for ordinals in delete_ordinals.chunks(OLAP_ROWS_PER_STATEMENT) {
            deleted += transaction
                .execute(&olap_delete_sql(ordinals), &[])
                .await
                .expect("execute typed OLAP delete statement")
                .rows_affected();
        }
        transaction
            .commit()
            .await
            .expect("commit typed OLAP delete transaction");
        assert_eq!(
            usize::try_from(deleted).expect("OLAP delete count fits usize"),
            mutation_count(self.row_count, 2, stride)
        );

        let mut transaction = self
            .session
            .begin_transaction()
            .await
            .expect("begin typed OLAP replacement insert transaction");
        let mut inserted = 0_u64;
        let insert_end = self.row_count + delete_ordinals.len();
        for start in (self.row_count..insert_end).step_by(OLAP_ROWS_PER_STATEMENT) {
            let end = (start + OLAP_ROWS_PER_STATEMENT).min(insert_end);
            inserted += transaction
                .execute(&olap_insert_sql(start..end), &[])
                .await
                .expect("execute typed OLAP replacement insert statement")
                .rows_affected();
        }
        transaction
            .commit()
            .await
            .expect("commit typed OLAP replacement insert transaction");
        assert_eq!(
            usize::try_from(inserted).expect("OLAP insert count fits usize"),
            mutation_count(self.row_count, 2, stride)
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
            OlapReadShape::Scan => assert_olap_scan(&result, expected),
            OlapReadShape::Filter => assert_olap_filter(&result, expected),
            OlapReadShape::Sort => assert_olap_sort(&result, expected),
            OlapReadShape::Group => assert_olap_group(&result, expected),
            OlapReadShape::Aggregate => assert_olap_aggregate(&result, expected),
            OlapReadShape::Join => assert_olap_join(&result, expected),
        }
        result.len()
    }

    async fn read_olap_timed(&self, shape: OlapReadShape) -> usize {
        let expected = self
            .olap_expected
            .as_ref()
            .expect("typed OLAP query requires a typed OLAP fixture");
        let result = std::hint::black_box(execute(&self.session, shape.sql()).await);
        let expected_len = match shape {
            OlapReadShape::Scan => expected.visible_rows,
            OlapReadShape::Filter => expected.filtered_rows,
            OlapReadShape::Sort => 10_000.min(expected.active_rows as usize),
            OlapReadShape::Group => expected.groups.len(),
            OlapReadShape::Aggregate => 1,
            OlapReadShape::Join => expected.active_rows as usize,
        };
        assert_eq!(result.len(), expected_len);
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

    /// `read_all` stops at `ExecuteResult::len()`. That does force row
    /// materialization, but nothing ever looks *inside* a row, so per-cell
    /// consumer costs stay unmeasured and a change that defers work past
    /// result construction could still show a win here. This variant walks
    /// every row and every cell and folds each one into an accumulator, so
    /// no deferred per-cell work can hide behind the timing window.
    async fn read_all_rows_consumed(&self) -> u64 {
        let result = self.read_all_result().await;
        assert_eq!(result.len(), self.visible_row_count);
        let columns = result.columns().len();
        let mut consumed = 0_u64;
        let mut cells = 0_usize;
        for row in result.rows() {
            let values = row.values();
            assert_eq!(values.len(), columns);
            for value in values {
                consumed = fold_value(consumed, value);
                cells += 1;
            }
        }
        assert_eq!(cells, self.visible_row_count * columns);
        std::hint::black_box(consumed)
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
        let expected_rows = match &self.update_all_workload {
            UpdateWorkload::Transaction(statements) => statements.len(),
            UpdateWorkload::ExecuteBatch(_) => self.row_count,
            UpdateWorkload::PreparedDml(rows) => rows.row_count(),
        };
        let affected: u64 = match &self.update_all_workload {
            UpdateWorkload::Transaction(statements) => self
                .session
                .execute_batch(
                    &statements
                        .iter()
                        .map(|sql| ExecuteBatchStatement {
                            label: None,
                            sql: sql.clone(),
                            params: Vec::new(),
                        })
                        .collect::<Vec<_>>(),
                )
                .await
                .expect("execute tracked-state CRUD scalar SQL batch")
                .into_iter()
                .map(|result| result.rows_affected())
                .sum(),
            UpdateWorkload::ExecuteBatch(statements) => self
                .session
                .execute_batch(statements)
                .await
                .expect("execute tracked-state CRUD SQL batch")
                .into_iter()
                .map(|result| result.rows_affected())
                .sum(),
            UpdateWorkload::PreparedDml(parameter_rows) => execute_parameter_batch(
                &self.session,
                Arc::from(BOUND_UPDATE_ALL_SQL),
                parameter_rows.clone(),
            )
            .await
            .expect("execute tracked-state CRUD SQL parameter batch")
            .into_iter()
            .map(|result| result.rows_affected())
            .sum(),
        };
        assert_eq!(affected as usize, expected_rows);
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
        let parameter_batch = if row_count == self.bound_update_all_batch.row_count() {
            self.bound_update_all_batch.clone()
        } else {
            SharedParameterBatch::from_rows(
                (0..row_count).map(|row| self.bound_update_all_batch.row_values(row).unwrap()),
            )
            .expect("bounded update parameter batch is rectangular")
        };
        let results = execute_parameter_batch(
            &self.session,
            Arc::from(BOUND_UPDATE_ALL_SQL),
            parameter_batch,
        )
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
        let indices = if row_count == 1 {
            vec![0]
        } else {
            (0..row_count)
                .map(|index| index * last / (row_count - 1))
                .collect::<Vec<_>>()
        };
        let parameter_batch = SharedParameterBatch::from_rows(
            indices
                .into_iter()
                .map(|row| self.bound_update_all_batch.row_values(row).unwrap()),
        )
        .expect("spread update parameter batch is rectangular");
        let results = execute_parameter_batch(
            &self.session,
            Arc::from(BOUND_UPDATE_ALL_SQL),
            parameter_batch,
        )
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
    session: Lix<StorageImpl>,
    rows: &[WorkloadRow],
    read_many_by_pk_count: usize,
    untracked_fixture: UntrackedFixture,
    shape: FixtureShape,
    dir: tempfile::TempDir,
) -> GenericSqlFixture<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let tracked_rows = if untracked_fixture == UntrackedFixture::OneReadManyMember {
        // The untracked probe occupies one selected identity, so keep the
        // seeded and returned cardinality exactly equal to the baseline.
        &rows[..rows.len() - 1]
    } else {
        rows
    };
    let olap_expected = if matches!(shape, FixtureShape::Olap) {
        Some(olap_expected(
            tracked_rows.len(),
            selected_olap_mutation_profile(),
        ))
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
            SharedParameterBatch::from_rows(tracked_rows.iter().map(|row| {
                vec![
                    Value::Text(row.path.clone()),
                    Value::Text(row.value_json.clone()),
                ]
            }))
            .expect("insert parameter batch is rectangular")
        } else {
            empty_parameter_batch()
        },
        bound_seed_json_batch: if matches!(shape, FixtureShape::FullCrud) {
            SharedParameterBatch::from_rows(tracked_rows.iter().map(|row| {
                vec![
                    Value::Text(row.path.clone()),
                    Value::Text(row.value_json.clone()),
                ]
            }))
            .expect("seed parameter batch is rectangular")
        } else {
            empty_parameter_batch()
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
        update_all_workload: update_workload(shape, tracked_rows),
        bound_update_all_batch: if matches!(shape, FixtureShape::FullCrud) {
            SharedParameterBatch::from_rows(tracked_rows.iter().map(|row| {
                vec![
                    Value::Text(row.updated_value_json.clone()),
                    Value::Text(row.path.clone()),
                ]
            }))
            .expect("update parameter batch is rectangular")
        } else {
            empty_parameter_batch()
        },
        delete_all_sql: "DELETE FROM json_pointer".to_string(),
        delete_one_by_pk_sql: format!(
            "DELETE FROM json_pointer WHERE path = '{}'",
            sql_string(tracked_rows[mid].path.as_str())
        ),
        _dir: dir,
    }
}

fn olap_expected(row_count: usize, mutation_profile: OlapMutationProfile) -> OlapExpected {
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

    let inserted_rows = mutation_profile
        .stride()
        .map_or(0, |stride| mutation_count(row_count, 2, stride));
    for row in (0..row_count + inserted_rows)
        .filter_map(|ordinal| olap_expected_row(row_count, mutation_profile, ordinal))
    {
        if !row.active {
            continue;
        }

        active_rows += 1;
        active_ordinal_sum += row.ordinal;
        active_score_sum += row.score;
        active_min_ordinal =
            Some(active_min_ordinal.map_or(row.ordinal, |value: i64| value.min(row.ordinal)));
        active_max_ordinal =
            Some(active_max_ordinal.map_or(row.ordinal, |value: i64| value.max(row.ordinal)));
        let group = &mut groups[row.lane_index];
        group.rows += 1;
        group.ordinal_sum += row.ordinal;
        group.score_sum += row.score;
        group.score_min = group.score_min.min(row.score);
        group.score_max = group.score_max.max(row.score);
        if matches!(row.lane_index, 7 | 19) {
            filtered_rows += 1;
            filtered_first_ordinal = Some(
                filtered_first_ordinal.map_or(row.ordinal, |value: i64| value.min(row.ordinal)),
            );
            filtered_last_ordinal = Some(
                filtered_last_ordinal.map_or(row.ordinal, |value: i64| value.max(row.ordinal)),
            );
        }
    }

    OlapExpected {
        initial_row_count: row_count,
        mutation_profile,
        visible_rows: row_count,
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

fn mutation_count(row_count: usize, residue: usize, stride: usize) -> usize {
    if row_count <= residue {
        0
    } else {
        (row_count - 1 - residue) / stride + 1
    }
}

fn olap_id(ordinal: usize) -> String {
    format!("/~lix-olap/{ordinal:09}")
}

fn olap_lane(lane_index: usize) -> String {
    format!("lane-{lane_index:02}")
}

fn olap_base_row(ordinal: usize) -> OlapRowExpected {
    #[expect(clippy::cast_precision_loss)]
    let score = (ordinal % 10_000) as f64 / 8.0;
    OlapRowExpected {
        id: olap_id(ordinal),
        ordinal: i64::try_from(ordinal).expect("OLAP row ordinal must fit in i64"),
        lane_index: ordinal % 32,
        score,
        active: ordinal % 3 != 0,
    }
}

fn olap_expected_row(
    initial_row_count: usize,
    mutation_profile: OlapMutationProfile,
    ordinal: usize,
) -> Option<OlapRowExpected> {
    let Some(stride) = mutation_profile.stride() else {
        return (ordinal < initial_row_count).then(|| olap_base_row(ordinal));
    };
    let inserted_rows = mutation_count(initial_row_count, 2, stride);
    if ordinal >= initial_row_count {
        return (ordinal < initial_row_count + inserted_rows).then(|| olap_base_row(ordinal));
    }
    if ordinal % stride == 2 {
        return None;
    }
    let mut row = olap_base_row(ordinal);
    if ordinal % stride == 1 {
        row.lane_index = (row.lane_index + 11) % 32;
        row.score += 2_048.0;
        row.active = !row.active;
    }
    Some(row)
}

fn olap_delete_sql(ordinals: &[usize]) -> String {
    assert!(
        !ordinals.is_empty(),
        "OLAP delete statement cannot be empty"
    );
    let mut sql = String::from("DELETE FROM olap_row WHERE id IN (");
    for (position, &ordinal) in ordinals.iter().enumerate() {
        if position != 0 {
            sql.push(',');
        }
        write!(sql, "'{}'", olap_id(ordinal)).expect("write typed OLAP DELETE SQL");
    }
    sql.push(')');
    sql
}

fn olap_insert_sql(ordinals: Range<usize>) -> String {
    let mut sql = String::from("INSERT INTO olap_row (id, ordinal, lane, score, active) VALUES ");
    for (position, ordinal) in ordinals.enumerate() {
        if position != 0 {
            sql.push(',');
        }
        let row = olap_base_row(ordinal);
        let active = if row.active { "TRUE" } else { "FALSE" };
        write!(
            sql,
            "('{}', {}, '{}', {}, {active})",
            row.id,
            row.ordinal,
            olap_lane(row.lane_index),
            row.score,
        )
        .expect("write typed OLAP INSERT SQL");
    }
    sql
}

fn expected_olap_rows(expected: &OlapExpected) -> impl Iterator<Item = OlapRowExpected> + '_ {
    let inserted_rows = expected.mutation_profile.stride().map_or(0, |stride| {
        mutation_count(expected.initial_row_count, 2, stride)
    });
    (0..expected.initial_row_count + inserted_rows).filter_map(|ordinal| {
        olap_expected_row(
            expected.initial_row_count,
            expected.mutation_profile,
            ordinal,
        )
    })
}

fn assert_olap_scan(result: &ExecuteResult, expected: &OlapExpected) {
    assert_eq!(
        result.columns(),
        ["id", "ordinal", "lane", "score", "active"]
    );
    assert_eq!(result.len(), expected.visible_rows);
    let max_ordinal = expected_olap_rows(expected)
        .map(|row| usize::try_from(row.ordinal).expect("nonnegative OLAP ordinal"))
        .max()
        .expect("typed OLAP fixture is not empty");
    let mut seen = vec![false; max_ordinal + 1];
    for row_index in 0..result.len() {
        let ordinal = usize::try_from(integer_at(result, row_index, 1))
            .expect("OLAP scan ordinal must be nonnegative");
        let row = olap_expected_row(
            expected.initial_row_count,
            expected.mutation_profile,
            ordinal,
        )
        .expect("OLAP scan returned a deleted or unknown row");
        assert!(
            !std::mem::replace(&mut seen[ordinal], true),
            "duplicate OLAP row"
        );
        assert_eq!(text_at(result, row_index, 0), row.id);
        assert_eq!(text_at(result, row_index, 2), olap_lane(row.lane_index));
        assert_eq!(real_at(result, row_index, 3), row.score);
        assert_eq!(boolean_at(result, row_index, 4), row.active);
    }
    for row in expected_olap_rows(expected) {
        assert!(
            seen[usize::try_from(row.ordinal).expect("nonnegative OLAP ordinal")],
            "OLAP scan omitted {}",
            row.id
        );
    }
}

fn assert_olap_filter(result: &ExecuteResult, expected: &OlapExpected) {
    assert_eq!(result.columns(), ["ordinal", "lane", "score"]);
    let rows = expected_olap_rows(expected)
        .filter(|row| row.active && matches!(row.lane_index, 7 | 19))
        .collect::<Vec<_>>();
    assert_eq!(result.len(), rows.len());
    assert_eq!(result.len(), expected.filtered_rows);
    for (row_index, row) in rows.iter().enumerate() {
        assert_eq!(integer_at(result, row_index, 0), row.ordinal);
        assert_eq!(text_at(result, row_index, 1), olap_lane(row.lane_index));
        assert_eq!(real_at(result, row_index, 2), row.score);
    }
    assert_eq!(
        rows.first().map(|row| row.ordinal),
        Some(expected.filtered_first_ordinal)
    );
    assert_eq!(
        rows.last().map(|row| row.ordinal),
        Some(expected.filtered_last_ordinal)
    );
}

fn assert_olap_sort(result: &ExecuteResult, expected: &OlapExpected) {
    assert_eq!(result.columns(), ["id", "ordinal", "score"]);
    let mut rows = expected_olap_rows(expected)
        .filter(|row| row.active)
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.ordinal.cmp(&right.ordinal))
    });
    rows.truncate(10_000);
    assert_eq!(result.len(), rows.len());
    for (row_index, row) in rows.iter().enumerate() {
        assert_eq!(text_at(result, row_index, 0), row.id);
        assert_eq!(integer_at(result, row_index, 1), row.ordinal);
        assert_eq!(real_at(result, row_index, 2), row.score);
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

fn assert_olap_join(result: &ExecuteResult, expected: &OlapExpected) {
    assert_eq!(result.len(), expected.active_rows as usize);
    assert_eq!(result.columns(), ["id", "score"]);
    for row_index in 0..result.len() {
        let id = text_at(result, row_index, 0);
        let score = real_at(result, row_index, 1);
        let ordinal = id
            .strip_prefix("/~lix-olap/")
            .and_then(|value| value.parse::<usize>().ok())
            .expect("join id should carry the fixture ordinal");
        let expected_row = olap_expected_row(
            expected.initial_row_count,
            expected.mutation_profile,
            ordinal,
        )
        .expect("join returned an unknown row");
        assert!(expected_row.active);
        assert_eq!(score, expected_row.score);
    }
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

fn boolean_at(result: &ExecuteResult, row: usize, column: usize) -> bool {
    match result.rows()[row].get_index(column) {
        Some(Value::Boolean(value)) => *value,
        value => panic!("expected boolean at row {row}, column {column}, got {value:?}"),
    }
}

fn assert_close(actual: f64, expected: f64) {
    let tolerance = 1e-10 * expected.abs().max(1.0);
    assert!(
        (actual - expected).abs() <= tolerance,
        "expected {expected} within {tolerance}, got {actual}"
    );
}

fn update_workload(shape: FixtureShape, rows: &[WorkloadRow]) -> UpdateWorkload {
    if !matches!(shape, FixtureShape::FullCrud) {
        return UpdateWorkload::Transaction(Vec::new());
    }
    match std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_UPDATE_API").as_deref() {
        Ok("execute_batch") => UpdateWorkload::ExecuteBatch(literal_update_batch(rows)),
        Ok("execute_batch_parameterized") => {
            UpdateWorkload::ExecuteBatch(parameterized_update_batch(rows))
        }
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_UPDATE_API '{other}'; expected execute_batch or execute_batch_parameterized"
        ),
        Err(_) => {
            let scalar_rows = std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_SCALAR_UPDATE_ROW_COUNT")
                .ok()
                .map(|value| {
                    value.parse::<usize>().unwrap_or_else(|_| {
                        panic!(
                            "LIX_TRACKED_STATE_CRUD_PROFILE_SCALAR_UPDATE_ROW_COUNT must be an integer between 1 and {}, got '{value}'",
                            rows.len()
                        )
                    })
                });
            if let Some(scalar_rows) = scalar_rows {
                assert!(
                    (1..=rows.len()).contains(&scalar_rows),
                    "LIX_TRACKED_STATE_CRUD_PROFILE_SCALAR_UPDATE_ROW_COUNT must be between 1 and {}, got {scalar_rows}",
                    rows.len()
                );
                UpdateWorkload::Transaction(
                    rows[..scalar_rows].iter().map(update_row_sql).collect(),
                )
            } else {
                UpdateWorkload::PreparedDml(prepared_update_rows(rows))
            }
        }
    }
}

fn prepared_update_rows(rows: &[WorkloadRow]) -> SharedParameterBatch {
    SharedParameterBatch::from_rows(rows.iter().map(|row| {
        vec![
            Value::Text(row.updated_value_json.clone()),
            Value::Text(row.path.clone()),
        ]
    }))
    .expect("prepared update parameter batch is rectangular")
}

fn parameterized_update_batch(rows: &[WorkloadRow]) -> Vec<ExecuteBatchStatement> {
    rows.iter()
        .map(|row| ExecuteBatchStatement {
            label: None,
            sql: BOUND_UPDATE_ALL_SQL.to_string(),
            params: vec![
                Value::Text(row.updated_value_json.clone()),
                Value::Text(row.path.clone()),
            ],
        })
        .collect()
}

fn literal_update_batch(rows: &[WorkloadRow]) -> Vec<ExecuteBatchStatement> {
    rows.iter()
        .map(|row| ExecuteBatchStatement {
            label: None,
            sql: update_row_sql(row),
            params: Vec::new(),
        })
        .collect()
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

pub(crate) fn selected_olap_mutation_profile() -> OlapMutationProfile {
    match std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_OLAP_STATE").as_deref() {
        Ok("sparse") => OlapMutationProfile::Sparse,
        Ok("moderate") => OlapMutationProfile::Moderate,
        Ok("pristine") | Err(_) => OlapMutationProfile::Pristine,
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_OLAP_STATE '{other}'; expected pristine, sparse, or moderate"
        ),
    }
}

async fn prepare_session<StorageImpl>(storage: StorageImpl) -> Lix<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize tracked-state crud storage");
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("open tracked-state crud lix");
    let session = lix
        .open_another_session()
        .await
        .expect("open tracked-state crud session");
    register_json_pointer_schema(&session).await;
    register_bulk_insert_schema(&session).await;
    register_olap_schema(&session).await;
    session
}

async fn register_json_pointer_schema<StorageImpl>(session: &Lix<StorageImpl>)
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

async fn register_bulk_insert_schema<StorageImpl>(session: &Lix<StorageImpl>)
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

async fn register_olap_schema<StorageImpl>(session: &Lix<StorageImpl>)
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

async fn execute<StorageImpl>(session: &Lix<StorageImpl>, sql: &str) -> ExecuteResult
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(sql, &[])
        .await
        .expect("execute tracked-state crud SQL")
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
    match std::env::var("LIX_TRACKED_STATE_CRUD_PROFILE_READ_MANY_DISTRIBUTION").as_deref() {
        Ok("spread") if read_many_by_pk_count > 1 => select_by_paths_sql(
            (0..read_many_by_pk_count)
                .map(|index| index * (rows.len() - 1) / (read_many_by_pk_count - 1))
                .map(|index| rows[index].path.as_str()),
        ),
        Ok("spread") | Ok("prefix") | Err(_) => select_by_pk_sql(&rows[..read_many_by_pk_count]),
        Ok(other) => panic!(
            "unknown LIX_TRACKED_STATE_CRUD_PROFILE_READ_MANY_DISTRIBUTION '{other}'; expected prefix or spread"
        ),
    }
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
