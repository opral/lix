use lix_engine::storage::StorageContext;
use lix_engine::transaction::bench::{
    BenchLayoutAccounting, BenchTransactionFixture, BenchTransactionRow, BenchWriteAccounting,
};

use crate::backends::{BackendProfile, ProfileBackend, RedbBackend, RocksDbBackend, SqliteBackend};
use crate::workload::{WorkloadRow, snapshot_value};

pub(crate) enum TransactionFixture {
    Sqlite(BenchTransactionFixture<SqliteBackend>),
    RocksDb(BenchTransactionFixture<RocksDbBackend>),
    Redb(BenchTransactionFixture<RedbBackend>),
}

pub(crate) type TransactionWriteAccounting = BenchWriteAccounting;
pub(crate) type TransactionLayoutAccounting = BenchLayoutAccounting;

pub(crate) async fn empty_fixture(
    profile: BackendProfile,
    rows: &[WorkloadRow],
) -> TransactionFixture {
    let rows = bench_rows(rows);
    match profile.backend() {
        ProfileBackend::Sqlite(backend) => TransactionFixture::Sqlite(
            BenchTransactionFixture::new(StorageContext::new(backend), rows).await,
        ),
        ProfileBackend::RocksDb(backend) => TransactionFixture::RocksDb(
            BenchTransactionFixture::new(StorageContext::new(backend), rows).await,
        ),
        ProfileBackend::Redb(backend) => TransactionFixture::Redb(
            BenchTransactionFixture::new(StorageContext::new(backend), rows).await,
        ),
    }
}

pub(crate) async fn seeded_fixture(
    profile: BackendProfile,
    rows: &[WorkloadRow],
) -> TransactionFixture {
    let mut fixture = empty_fixture(profile, rows).await;
    fixture.seed().await;
    fixture
}

impl TransactionFixture {
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

    pub(crate) async fn insert_all_accounting(&mut self) -> TransactionWriteAccounting {
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

    pub(crate) async fn read_many_by_pk(&self, count: usize) -> usize {
        match self {
            Self::Sqlite(fixture) => fixture.read_many_by_pk(count).await,
            Self::RocksDb(fixture) => fixture.read_many_by_pk(count).await,
            Self::Redb(fixture) => fixture.read_many_by_pk(count).await,
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

    pub(crate) async fn update_all_accounting(&mut self) -> TransactionWriteAccounting {
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

    pub(crate) async fn update_one_by_pk_accounting(&mut self) -> TransactionWriteAccounting {
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

    pub(crate) async fn delete_all_accounting(&mut self) -> TransactionWriteAccounting {
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

    pub(crate) async fn delete_one_by_pk_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::Sqlite(fixture) => fixture.delete_one_by_pk_accounting().await,
            Self::RocksDb(fixture) => fixture.delete_one_by_pk_accounting().await,
            Self::Redb(fixture) => fixture.delete_one_by_pk_accounting().await,
        }
    }

    pub(crate) async fn layout_accounting(&self) -> Vec<TransactionLayoutAccounting> {
        match self {
            Self::Sqlite(fixture) => fixture.layout_accounting().await,
            Self::RocksDb(fixture) => fixture.layout_accounting().await,
            Self::Redb(fixture) => fixture.layout_accounting().await,
        }
    }
}

fn bench_rows(rows: &[WorkloadRow]) -> Vec<BenchTransactionRow> {
    rows.iter()
        .map(|row| BenchTransactionRow {
            schema_key: "json_pointer".to_string(),
            file_id: None,
            entity_pk: row.path.clone(),
            value: serde_json::from_str(&snapshot_value(&row.path, &row.value_json))
                .expect("transaction bench value should parse"),
            updated_value: serde_json::from_str(&snapshot_value(
                &row.path,
                &row.updated_value_json,
            ))
            .expect("transaction bench updated value should parse"),
        })
        .collect()
}
