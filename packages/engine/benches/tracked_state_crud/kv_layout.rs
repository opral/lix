use bytes::Bytes;
use lix_engine::storage::{
    PointReadPlan, ScanPlan, StorageContext, StorageCoreProjection, StorageGetOptions,
    StoragePrefix, StorageReadOptions, StorageScanOptions, StorageSpace, StorageValue,
    StorageWriteOptions, StorageWriteSetStats,
};
use lix_engine::{Backend, Key, MAX_SCAN_PAGE_ROWS, ProjectedValue, SpaceId};

use crate::backends::{BackendProfile, ProfileBackend, RedbBackend, RocksDbBackend, SqliteBackend};
use crate::workload::{WorkloadRow, snapshot_value};

const ROW_SPACE: StorageSpace =
    StorageSpace::new(SpaceId(0x0002_0001), "tracked_state.crud.row.v1");

#[derive(Clone)]
struct BenchRow {
    key: Key,
    value: StorageValue,
    updated_value: StorageValue,
}

enum ProfileStorage {
    Sqlite(StorageContext<SqliteBackend>),
    RocksDb(StorageContext<RocksDbBackend>),
    Redb(StorageContext<RedbBackend>),
}

pub(crate) struct KvFixture {
    storage: ProfileStorage,
    rows: Vec<BenchRow>,
}

pub(crate) struct KvWriteAccounting {
    pub(crate) logical_rows: usize,
    pub(crate) staged_puts: u64,
    pub(crate) staged_deletes: u64,
    pub(crate) range_deletes: u64,
    pub(crate) touched_spaces: u64,
    pub(crate) backend_calls: u64,
    pub(crate) put_batches: u64,
    pub(crate) delete_batches: u64,
    pub(crate) written_bytes: u64,
}

pub(crate) struct KvLayoutAccounting {
    pub(crate) space_id: u32,
    pub(crate) space: &'static str,
    pub(crate) rows: u64,
    pub(crate) key_bytes: u64,
    pub(crate) value_bytes: u64,
}

struct KvWriteOutcome {
    logical_rows: usize,
    stats: StorageWriteSetStats,
    range_deletes: u64,
}

impl KvWriteOutcome {
    fn accounting(&self) -> KvWriteAccounting {
        KvWriteAccounting {
            logical_rows: self.logical_rows,
            staged_puts: self.stats.staged_puts,
            staged_deletes: self.stats.staged_deletes,
            range_deletes: self.range_deletes,
            touched_spaces: self.stats.touched_spaces,
            backend_calls: self.stats.backend_calls,
            put_batches: self.stats.put_batches,
            delete_batches: self.stats.delete_batches,
            written_bytes: self.stats.written_bytes,
        }
    }
}

pub(crate) async fn empty_fixture(profile: BackendProfile, rows: &[WorkloadRow]) -> KvFixture {
    let rows = bench_rows(rows);
    KvFixture {
        storage: profile_storage(profile),
        rows,
    }
}

pub(crate) async fn seeded_fixture(profile: BackendProfile, rows: &[WorkloadRow]) -> KvFixture {
    let fixture = empty_fixture(profile, rows).await;
    fixture.storage.insert_all(&fixture.rows).await;
    fixture
}

impl KvFixture {
    pub(crate) async fn insert_all(&mut self) -> usize {
        self.insert_all_accounting().await.logical_rows
    }

    #[expect(clippy::needless_pass_by_ref_mut)]
    pub(crate) async fn insert_all_accounting(&mut self) -> KvWriteAccounting {
        self.storage.insert_all(&self.rows).await.accounting()
    }

    pub(crate) async fn read_all(&self) -> usize {
        self.storage
            .read_all(self.rows.len(), StorageCoreProjection::FullValue)
            .await
    }

    pub(crate) async fn read_many_by_pk(&self, count: usize) -> usize {
        self.storage
            .read_points(&self.rows[..count.min(self.rows.len())])
            .await
    }

    pub(crate) async fn read_one_by_pk(&self) -> usize {
        self.storage
            .read_points(std::slice::from_ref(&self.rows[self.rows.len() / 2]))
            .await
    }

    pub(crate) async fn update_all(&mut self) -> usize {
        self.update_all_accounting().await.logical_rows
    }

    #[expect(clippy::needless_pass_by_ref_mut)]
    pub(crate) async fn update_all_accounting(&mut self) -> KvWriteAccounting {
        self.storage.update_all(&self.rows).await.accounting()
    }

    pub(crate) async fn update_one_by_pk(&mut self) -> usize {
        self.update_one_by_pk_accounting().await.logical_rows
    }

    #[expect(clippy::needless_pass_by_ref_mut)]
    pub(crate) async fn update_one_by_pk_accounting(&mut self) -> KvWriteAccounting {
        self.storage.update_all(&self.rows[..1]).await.accounting()
    }

    pub(crate) async fn delete_all(&mut self) -> usize {
        self.delete_all_accounting().await.logical_rows
    }

    #[expect(clippy::needless_pass_by_ref_mut)]
    pub(crate) async fn delete_all_accounting(&mut self) -> KvWriteAccounting {
        self.storage.delete_all(self.rows.len()).await.accounting()
    }

    pub(crate) async fn delete_one_by_pk(&mut self) -> usize {
        self.delete_one_by_pk_accounting().await.logical_rows
    }

    #[expect(clippy::needless_pass_by_ref_mut)]
    pub(crate) async fn delete_one_by_pk_accounting(&mut self) -> KvWriteAccounting {
        self.storage
            .delete_one(&self.rows[self.rows.len() / 2])
            .await
            .accounting()
    }

    pub(crate) async fn layout_accounting(&self) -> Vec<KvLayoutAccounting> {
        self.storage.layout_accounting().await
    }
}

impl ProfileStorage {
    async fn insert_all(&self, rows: &[BenchRow]) -> KvWriteOutcome {
        match self {
            Self::Sqlite(storage) => insert_all_storage(storage, rows).await,
            Self::RocksDb(storage) => insert_all_storage(storage, rows).await,
            Self::Redb(storage) => insert_all_storage(storage, rows).await,
        }
    }

    async fn update_all(&self, rows: &[BenchRow]) -> KvWriteOutcome {
        match self {
            Self::Sqlite(storage) => update_all_storage(storage, rows).await,
            Self::RocksDb(storage) => update_all_storage(storage, rows).await,
            Self::Redb(storage) => update_all_storage(storage, rows).await,
        }
    }

    async fn delete_all(&self, row_count: usize) -> KvWriteOutcome {
        match self {
            Self::Sqlite(storage) => delete_all_storage(storage, row_count).await,
            Self::RocksDb(storage) => delete_all_storage(storage, row_count).await,
            Self::Redb(storage) => delete_all_storage(storage, row_count).await,
        }
    }

    async fn delete_one(&self, row: &BenchRow) -> KvWriteOutcome {
        match self {
            Self::Sqlite(storage) => delete_one_storage(storage, row).await,
            Self::RocksDb(storage) => delete_one_storage(storage, row).await,
            Self::Redb(storage) => delete_one_storage(storage, row).await,
        }
    }

    async fn read_all(&self, expected_rows: usize, projection: StorageCoreProjection) -> usize {
        match self {
            Self::Sqlite(storage) => read_all_storage(storage, expected_rows, projection).await,
            Self::RocksDb(storage) => read_all_storage(storage, expected_rows, projection).await,
            Self::Redb(storage) => read_all_storage(storage, expected_rows, projection).await,
        }
    }

    async fn read_points(&self, rows: &[BenchRow]) -> usize {
        match self {
            Self::Sqlite(storage) => read_points_storage(storage, rows).await,
            Self::RocksDb(storage) => read_points_storage(storage, rows).await,
            Self::Redb(storage) => read_points_storage(storage, rows).await,
        }
    }

    async fn layout_accounting(&self) -> Vec<KvLayoutAccounting> {
        match self {
            Self::Sqlite(storage) => layout_accounting_storage(storage).await,
            Self::RocksDb(storage) => layout_accounting_storage(storage).await,
            Self::Redb(storage) => layout_accounting_storage(storage).await,
        }
    }
}

fn profile_storage(profile: BackendProfile) -> ProfileStorage {
    match profile.backend() {
        ProfileBackend::Sqlite(backend) => ProfileStorage::Sqlite(StorageContext::new(backend)),
        ProfileBackend::RocksDb(backend) => ProfileStorage::RocksDb(StorageContext::new(backend)),
        ProfileBackend::Redb(backend) => ProfileStorage::Redb(StorageContext::new(backend)),
    }
}

fn bench_rows(rows: &[WorkloadRow]) -> Vec<BenchRow> {
    rows.iter()
        .map(|row| {
            let value = snapshot_value(row.path.as_str(), row.value_json.as_str());
            let updated_value = snapshot_value(row.path.as_str(), row.updated_value_json.as_str());
            BenchRow {
                key: Key(Bytes::from(row_key(&row.path))),
                value: StorageValue {
                    bytes: Bytes::from(value),
                },
                updated_value: StorageValue {
                    bytes: Bytes::from(updated_value),
                },
            }
        })
        .collect()
}

fn row_key(entity_pk: &str) -> Vec<u8> {
    let mut out = Vec::new();
    push_component(&mut out, "main");
    push_component(&mut out, "json_pointer");
    push_component(&mut out, entity_pk);
    push_component(&mut out, "");
    out
}

fn push_component(out: &mut Vec<u8>, value: &str) {
    let len = u32::try_from(value.len()).expect("component length fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

async fn insert_all_storage<B>(storage: &StorageContext<B>, rows: &[BenchRow]) -> KvWriteOutcome
where
    B: Backend,
{
    let mut writes = storage.new_write_set();
    for row in rows {
        writes.put(ROW_SPACE, row.key.clone(), row.value.clone());
    }
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .expect("commit insert rows");
    assert_eq!(stats.staged_puts, rows.len() as u64);
    KvWriteOutcome {
        logical_rows: rows.len(),
        stats,
        range_deletes: 0,
    }
}

async fn update_all_storage<B>(storage: &StorageContext<B>, rows: &[BenchRow]) -> KvWriteOutcome
where
    B: Backend,
{
    let mut writes = storage.new_write_set();
    for row in rows {
        writes.put(ROW_SPACE, row.key.clone(), row.updated_value.clone());
    }
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .expect("commit update rows");
    assert_eq!(stats.staged_puts, rows.len() as u64);
    KvWriteOutcome {
        logical_rows: rows.len(),
        stats,
        range_deletes: 0,
    }
}

async fn delete_all_storage<B>(storage: &StorageContext<B>, row_count: usize) -> KvWriteOutcome
where
    B: Backend,
{
    let _commit = storage
        .clear_space(ROW_SPACE, StorageWriteOptions::default())
        .await
        .expect("clear tracked-state crud rows");
    let stats = StorageWriteSetStats {
        backend_calls: 1,
        delete_batches: 1,
        ..StorageWriteSetStats::default()
    };
    KvWriteOutcome {
        logical_rows: row_count,
        stats,
        range_deletes: 1,
    }
}

async fn delete_one_storage<B>(storage: &StorageContext<B>, row: &BenchRow) -> KvWriteOutcome
where
    B: Backend,
{
    let mut writes = storage.new_write_set();
    writes.delete(ROW_SPACE, row.key.clone());
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .expect("commit delete row");
    assert_eq!(stats.staged_deletes, 1);
    KvWriteOutcome {
        logical_rows: 1,
        stats,
        range_deletes: 0,
    }
}

async fn read_all_storage<B>(
    storage: &StorageContext<B>,
    expected_rows: usize,
    projection: StorageCoreProjection,
) -> usize
where
    B: Backend,
{
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("begin read");
    let plan = ScanPlan::prefix(
        ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut resume_after = None;
    let mut rows = 0usize;
    loop {
        let page = plan
            .collect(
                &read,
                StorageScanOptions {
                    projection,
                    limit_rows: MAX_SCAN_PAGE_ROWS,
                    resume_after,
                },
            )
            .await
            .expect("scan rows");
        rows += page.value.entries.len();
        if !page.value.has_more {
            break;
        }
        resume_after = Some(
            page.value
                .entries
                .last()
                .expect("non-terminal scan page must not be empty")
                .key
                .clone(),
        );
    }
    assert_eq!(rows, expected_rows);
    rows
}

async fn read_points_storage<B>(storage: &StorageContext<B>, rows: &[BenchRow]) -> usize
where
    B: Backend,
{
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("begin read");
    let keys = rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>();
    let result = PointReadPlan::new(ROW_SPACE, &keys)
        .materialize(&read, StorageGetOptions::default())
        .await
        .expect("point read rows");
    assert_eq!(result.value.len(), rows.len());
    assert!(result.value.iter().all(Option::is_some));
    result.value.len()
}

async fn layout_accounting_storage<B>(storage: &StorageContext<B>) -> Vec<KvLayoutAccounting>
where
    B: Backend,
{
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("begin kv layout accounting read");
    let plan = ScanPlan::prefix(
        ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut resume_after = None;
    let mut rows = 0u64;
    let mut key_bytes = 0u64;
    let mut value_bytes = 0u64;
    loop {
        let page = plan
            .collect(
                &read,
                StorageScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    limit_rows: MAX_SCAN_PAGE_ROWS,
                    resume_after,
                },
            )
            .await
            .expect("scan kv layout accounting");

        rows += page.value.entries.len() as u64;
        key_bytes += page
            .value
            .entries
            .iter()
            .map(|entry| entry.key.0.len() as u64 + 4)
            .sum::<u64>();
        value_bytes += page
            .value
            .entries
            .iter()
            .map(|entry| match &entry.value {
                ProjectedValue::KeyOnly => 0,
                ProjectedValue::FullValue(value) => value.len() as u64,
            })
            .sum::<u64>();

        if !page.value.has_more {
            break;
        }
        resume_after = Some(
            page.value
                .entries
                .last()
                .expect("non-terminal accounting page must not be empty")
                .key
                .clone(),
        );
    }

    if rows == 0 {
        return Vec::new();
    }

    vec![KvLayoutAccounting {
        space_id: ROW_SPACE.id.0,
        space: ROW_SPACE.name,
        rows,
        key_bytes,
        value_bytes,
    }]
}
