use bytes::Bytes;
use lix_engine::storage::{
    PointReadPlan, ScanPlan, StorageContext, StorageCoreProjection, StorageGetOptions,
    StoragePrefix, StorageReadOptions, StorageScanOptions, StorageSpace, StorageValue,
    StorageWriteOptions,
};
use lix_engine::{Backend, Key, SpaceId};

use crate::backends::{BackendProfile, ProfileBackend, RedbBackend, RocksDbBackend, SqliteBackend};
use crate::workload::{snapshot_value, WorkloadRow};

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

pub(crate) fn empty_fixture(profile: BackendProfile, rows: &[WorkloadRow]) -> KvFixture {
    let rows = bench_rows(rows);
    KvFixture {
        storage: profile_storage(profile),
        rows,
    }
}

pub(crate) fn seeded_fixture(profile: BackendProfile, rows: &[WorkloadRow]) -> KvFixture {
    let fixture = empty_fixture(profile, rows);
    fixture.storage.insert_all(&fixture.rows);
    fixture
}

impl KvFixture {
    pub(crate) fn insert_all(&self) -> usize {
        self.storage.insert_all(&self.rows)
    }

    pub(crate) fn read_all(&self) -> usize {
        self.storage
            .read_all(self.rows.len(), StorageCoreProjection::FullValue)
    }

    pub(crate) fn read_all_by_pk(&self) -> usize {
        self.storage.read_points(&self.rows)
    }

    pub(crate) fn read_one_by_pk(&self) -> usize {
        self.storage
            .read_points(std::slice::from_ref(&self.rows[self.rows.len() / 2]))
    }

    pub(crate) fn update_all(&self) -> usize {
        self.storage.update_all(&self.rows)
    }

    pub(crate) fn update_one_by_pk(&self) -> usize {
        self.storage.update_all(&self.rows[..1])
    }

    pub(crate) fn delete_all(&self) -> usize {
        self.storage.delete_all(self.rows.len())
    }

    pub(crate) fn delete_one_by_pk(&self) -> usize {
        self.storage.delete_one(&self.rows[self.rows.len() / 2])
    }
}

impl ProfileStorage {
    fn insert_all(&self, rows: &[BenchRow]) -> usize {
        match self {
            Self::Sqlite(storage) => insert_all_storage(storage, rows),
            Self::RocksDb(storage) => insert_all_storage(storage, rows),
            Self::Redb(storage) => insert_all_storage(storage, rows),
        }
    }

    fn update_all(&self, rows: &[BenchRow]) -> usize {
        match self {
            Self::Sqlite(storage) => update_all_storage(storage, rows),
            Self::RocksDb(storage) => update_all_storage(storage, rows),
            Self::Redb(storage) => update_all_storage(storage, rows),
        }
    }

    fn delete_all(&self, row_count: usize) -> usize {
        match self {
            Self::Sqlite(storage) => delete_all_storage(storage, row_count),
            Self::RocksDb(storage) => delete_all_storage(storage, row_count),
            Self::Redb(storage) => delete_all_storage(storage, row_count),
        }
    }

    fn delete_one(&self, row: &BenchRow) -> usize {
        match self {
            Self::Sqlite(storage) => delete_one_storage(storage, row),
            Self::RocksDb(storage) => delete_one_storage(storage, row),
            Self::Redb(storage) => delete_one_storage(storage, row),
        }
    }

    fn read_all(&self, expected_rows: usize, projection: StorageCoreProjection) -> usize {
        match self {
            Self::Sqlite(storage) => read_all_storage(storage, expected_rows, projection),
            Self::RocksDb(storage) => read_all_storage(storage, expected_rows, projection),
            Self::Redb(storage) => read_all_storage(storage, expected_rows, projection),
        }
    }

    fn read_points(&self, rows: &[BenchRow]) -> usize {
        match self {
            Self::Sqlite(storage) => read_points_storage(storage, rows),
            Self::RocksDb(storage) => read_points_storage(storage, rows),
            Self::Redb(storage) => read_points_storage(storage, rows),
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

fn row_key(entity_id: &str) -> Vec<u8> {
    let mut out = Vec::new();
    push_component(&mut out, "main");
    push_component(&mut out, "json_pointer");
    push_component(&mut out, entity_id);
    push_component(&mut out, "");
    out
}

fn push_component(out: &mut Vec<u8>, value: &str) {
    let len = u32::try_from(value.len()).expect("component length fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn insert_all_storage<B>(storage: &StorageContext<B>, rows: &[BenchRow]) -> usize
where
    B: Backend,
{
    let mut writes = storage.new_write_set();
    for row in rows {
        writes.put(ROW_SPACE, row.key.clone(), row.value.clone());
    }
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .expect("commit insert rows");
    assert_eq!(stats.staged_puts, rows.len() as u64);
    rows.len()
}

fn update_all_storage<B>(storage: &StorageContext<B>, rows: &[BenchRow]) -> usize
where
    B: Backend,
{
    let mut writes = storage.new_write_set();
    for row in rows {
        writes.put(ROW_SPACE, row.key.clone(), row.updated_value.clone());
    }
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .expect("commit update rows");
    assert_eq!(stats.staged_puts, rows.len() as u64);
    rows.len()
}

fn delete_all_storage<B>(storage: &StorageContext<B>, row_count: usize) -> usize
where
    B: Backend,
{
    storage
        .clear_space(ROW_SPACE, StorageWriteOptions::default())
        .expect("clear tracked-state crud rows");
    row_count
}

fn delete_one_storage<B>(storage: &StorageContext<B>, row: &BenchRow) -> usize
where
    B: Backend,
{
    let mut writes = storage.new_write_set();
    writes.delete(ROW_SPACE, row.key.clone());
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .expect("commit delete row");
    assert_eq!(stats.staged_deletes, 1);
    1
}

fn read_all_storage<B>(
    storage: &StorageContext<B>,
    expected_rows: usize,
    projection: StorageCoreProjection,
) -> usize
where
    B: Backend,
{
    let read = storage
        .begin_read(StorageReadOptions::default())
        .expect("begin read");
    let page = ScanPlan::prefix(
        ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    )
    .collect(
        &read,
        StorageScanOptions {
            projection,
            limit_rows: expected_rows + 1,
            ..StorageScanOptions::default()
        },
    )
    .expect("scan rows");
    assert_eq!(page.value.entries.len(), expected_rows);
    expected_rows
}

fn read_points_storage<B>(storage: &StorageContext<B>, rows: &[BenchRow]) -> usize
where
    B: Backend,
{
    let read = storage
        .begin_read(StorageReadOptions::default())
        .expect("begin read");
    let keys = rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>();
    let result = PointReadPlan::new(ROW_SPACE, &keys)
        .materialize(&read, StorageGetOptions::default())
        .expect("point read rows");
    assert_eq!(result.value.len(), rows.len());
    assert!(result.value.iter().all(Option::is_some));
    result.value.len()
}
