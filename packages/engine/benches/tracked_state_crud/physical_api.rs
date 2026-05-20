use lix_engine::storage::StorageContext;
use lix_engine::tracked_state::bench::{
    BenchLayoutAccounting, BenchTrackedFixture, BenchTrackedRow, BenchWriteAccounting,
};

use crate::backends::{BackendProfile, ProfileBackend, RedbBackend, RocksDbBackend, SqliteBackend};
use crate::workload::{snapshot_value, WorkloadRow};

pub(crate) enum PhysicalFixture {
    Sqlite(BenchTrackedFixture<SqliteBackend>),
    RocksDb(BenchTrackedFixture<RocksDbBackend>),
    Redb(BenchTrackedFixture<RedbBackend>),
}

pub(crate) type PhysicalWriteAccounting = BenchWriteAccounting;
pub(crate) type PhysicalLayoutAccounting = BenchLayoutAccounting;

pub(crate) fn empty_fixture(profile: BackendProfile, rows: &[WorkloadRow]) -> PhysicalFixture {
    let rows = bench_rows(rows);
    match profile.backend() {
        ProfileBackend::Sqlite(backend) => {
            PhysicalFixture::Sqlite(BenchTrackedFixture::new(StorageContext::new(backend), rows))
        }
        ProfileBackend::RocksDb(backend) => {
            PhysicalFixture::RocksDb(BenchTrackedFixture::new(StorageContext::new(backend), rows))
        }
        ProfileBackend::Redb(backend) => {
            PhysicalFixture::Redb(BenchTrackedFixture::new(StorageContext::new(backend), rows))
        }
    }
}

pub(crate) async fn seeded_fixture(
    profile: BackendProfile,
    rows: &[WorkloadRow],
) -> PhysicalFixture {
    let mut fixture = empty_fixture(profile, rows);
    fixture.seed().await;
    fixture
}

impl PhysicalFixture {
    pub(crate) async fn seed(&mut self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.seed().await,
            Self::RocksDb(fixture) => fixture.seed().await,
            Self::Redb(fixture) => fixture.seed().await,
        }
    }

    pub(crate) async fn insert_all(&mut self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.insert_all().await,
            Self::RocksDb(fixture) => fixture.insert_all().await,
            Self::Redb(fixture) => fixture.insert_all().await,
        }
    }

    pub(crate) async fn insert_all_accounting(&mut self) -> PhysicalWriteAccounting {
        match self {
            Self::Sqlite(fixture) => fixture.insert_all_accounting().await,
            Self::RocksDb(fixture) => fixture.insert_all_accounting().await,
            Self::Redb(fixture) => fixture.insert_all_accounting().await,
        }
    }

    pub(crate) async fn read_all(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.read_all().await,
            Self::RocksDb(fixture) => fixture.read_all().await,
            Self::Redb(fixture) => fixture.read_all().await,
        }
    }

    pub(crate) async fn read_all_by_pk(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.read_all_by_pk().await,
            Self::RocksDb(fixture) => fixture.read_all_by_pk().await,
            Self::Redb(fixture) => fixture.read_all_by_pk().await,
        }
    }

    pub(crate) async fn read_one_by_pk(&self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.read_one_by_pk().await,
            Self::RocksDb(fixture) => fixture.read_one_by_pk().await,
            Self::Redb(fixture) => fixture.read_one_by_pk().await,
        }
    }

    pub(crate) async fn update_all(&mut self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.update_all().await,
            Self::RocksDb(fixture) => fixture.update_all().await,
            Self::Redb(fixture) => fixture.update_all().await,
        }
    }

    pub(crate) async fn update_all_accounting(&mut self) -> PhysicalWriteAccounting {
        match self {
            Self::Sqlite(fixture) => fixture.update_all_accounting().await,
            Self::RocksDb(fixture) => fixture.update_all_accounting().await,
            Self::Redb(fixture) => fixture.update_all_accounting().await,
        }
    }

    pub(crate) async fn update_one_by_pk(&mut self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.update_one_by_pk().await,
            Self::RocksDb(fixture) => fixture.update_one_by_pk().await,
            Self::Redb(fixture) => fixture.update_one_by_pk().await,
        }
    }

    pub(crate) async fn update_one_by_pk_accounting(&mut self) -> PhysicalWriteAccounting {
        match self {
            Self::Sqlite(fixture) => fixture.update_one_by_pk_accounting().await,
            Self::RocksDb(fixture) => fixture.update_one_by_pk_accounting().await,
            Self::Redb(fixture) => fixture.update_one_by_pk_accounting().await,
        }
    }

    pub(crate) async fn delete_all(&mut self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.delete_all().await,
            Self::RocksDb(fixture) => fixture.delete_all().await,
            Self::Redb(fixture) => fixture.delete_all().await,
        }
    }

    pub(crate) async fn delete_all_accounting(&mut self) -> PhysicalWriteAccounting {
        match self {
            Self::Sqlite(fixture) => fixture.delete_all_accounting().await,
            Self::RocksDb(fixture) => fixture.delete_all_accounting().await,
            Self::Redb(fixture) => fixture.delete_all_accounting().await,
        }
    }

    pub(crate) async fn delete_one_by_pk(&mut self) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.delete_one_by_pk().await,
            Self::RocksDb(fixture) => fixture.delete_one_by_pk().await,
            Self::Redb(fixture) => fixture.delete_one_by_pk().await,
        }
    }

    pub(crate) fn layout_accounting(&self) -> Vec<PhysicalLayoutAccounting> {
        match self {
            Self::Sqlite(fixture) => fixture.layout_accounting(),
            Self::RocksDb(fixture) => fixture.layout_accounting(),
            Self::Redb(fixture) => fixture.layout_accounting(),
        }
    }

    pub(crate) async fn delete_one_by_pk_accounting(&mut self) -> PhysicalWriteAccounting {
        match self {
            Self::Sqlite(fixture) => fixture.delete_one_by_pk_accounting().await,
            Self::RocksDb(fixture) => fixture.delete_one_by_pk_accounting().await,
            Self::Redb(fixture) => fixture.delete_one_by_pk_accounting().await,
        }
    }
}

fn bench_rows(rows: &[WorkloadRow]) -> Vec<BenchTrackedRow> {
    rows.iter()
        .map(|row| BenchTrackedRow {
            schema_key: "json_pointer".to_string(),
            file_id: Some("pnpm-lock.fixture.json".to_string()),
            entity_id: row.path.clone(),
            value: snapshot_value(&row.path, &row.value_json).into_bytes(),
            updated_value: snapshot_value(&row.path, &row.updated_value_json).into_bytes(),
        })
        .collect()
}
