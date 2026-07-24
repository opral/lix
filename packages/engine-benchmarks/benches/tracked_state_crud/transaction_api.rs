use lix_engine::storage_adapter::StorageAdapter;
use lix_engine::transaction::bench::{
    BenchLayoutAccounting, BenchTransactionFixture, BenchTransactionRow, BenchWriteAccounting,
};

use crate::storage::{ProfileStorage, RocksDB, SQLite, StorageProfile};
use crate::workload::{WorkloadRow, snapshot_value};

pub(crate) enum TransactionFixture {
    SQLite(BenchTransactionFixture<SQLite>),
    RocksDB(BenchTransactionFixture<RocksDB>),
}

pub(crate) type TransactionWriteAccounting = BenchWriteAccounting;
pub(crate) type TransactionLayoutAccounting = BenchLayoutAccounting;

pub(crate) async fn empty_fixture(
    profile: StorageProfile,
    rows: &[WorkloadRow],
) -> TransactionFixture {
    let rows = bench_rows(rows);
    match profile.storage() {
        ProfileStorage::SQLite(storage) => TransactionFixture::SQLite(
            BenchTransactionFixture::new(StorageAdapter::new(storage), rows).await,
        ),
        ProfileStorage::RocksDB(storage) => TransactionFixture::RocksDB(
            BenchTransactionFixture::new(StorageAdapter::new(storage), rows).await,
        ),
    }
}

pub(crate) async fn seeded_fixture(
    profile: StorageProfile,
    rows: &[WorkloadRow],
) -> TransactionFixture {
    let mut fixture = empty_fixture(profile, rows).await;
    fixture.seed().await;
    fixture
}

impl TransactionFixture {
    pub(crate) async fn seed(&mut self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.seed().await,
            Self::RocksDB(fixture) => fixture.seed().await,
        }
    }

    pub(crate) async fn insert_all(&mut self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.insert_all().await,
            Self::RocksDB(fixture) => fixture.insert_all().await,
        }
    }

    pub(crate) async fn insert_all_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::SQLite(fixture) => fixture.insert_all_accounting().await,
            Self::RocksDB(fixture) => fixture.insert_all_accounting().await,
        }
    }

    pub(crate) async fn read_all(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.read_all().await,
            Self::RocksDB(fixture) => fixture.read_all().await,
        }
    }

    pub(crate) async fn read_many_by_pk(&self, count: usize) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.read_many_by_pk(count).await,
            Self::RocksDB(fixture) => fixture.read_many_by_pk(count).await,
        }
    }

    pub(crate) async fn read_one_by_pk(&self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.read_one_by_pk().await,
            Self::RocksDB(fixture) => fixture.read_one_by_pk().await,
        }
    }

    pub(crate) async fn update_all(&mut self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.update_all().await,
            Self::RocksDB(fixture) => fixture.update_all().await,
        }
    }

    pub(crate) async fn update_all_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::SQLite(fixture) => fixture.update_all_accounting().await,
            Self::RocksDB(fixture) => fixture.update_all_accounting().await,
        }
    }

    pub(crate) async fn update_one_by_pk(&mut self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.update_one_by_pk().await,
            Self::RocksDB(fixture) => fixture.update_one_by_pk().await,
        }
    }

    pub(crate) async fn update_one_by_pk_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::SQLite(fixture) => fixture.update_one_by_pk_accounting().await,
            Self::RocksDB(fixture) => fixture.update_one_by_pk_accounting().await,
        }
    }

    pub(crate) async fn delete_all(&mut self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.delete_all().await,
            Self::RocksDB(fixture) => fixture.delete_all().await,
        }
    }

    pub(crate) async fn delete_all_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::SQLite(fixture) => fixture.delete_all_accounting().await,
            Self::RocksDB(fixture) => fixture.delete_all_accounting().await,
        }
    }

    pub(crate) async fn delete_one_by_pk(&mut self) -> usize {
        match self {
            Self::SQLite(fixture) => fixture.delete_one_by_pk().await,
            Self::RocksDB(fixture) => fixture.delete_one_by_pk().await,
        }
    }

    pub(crate) async fn delete_one_by_pk_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::SQLite(fixture) => fixture.delete_one_by_pk_accounting().await,
            Self::RocksDB(fixture) => fixture.delete_one_by_pk_accounting().await,
        }
    }

    pub(crate) async fn layout_accounting(&self) -> Vec<TransactionLayoutAccounting> {
        match self {
            Self::SQLite(fixture) => fixture.layout_accounting().await,
            Self::RocksDB(fixture) => fixture.layout_accounting().await,
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
