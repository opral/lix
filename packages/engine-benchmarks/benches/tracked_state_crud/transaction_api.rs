use std::sync::Arc;

use lix::storage_adapter::StorageAdapter;
use lix::transaction::bench::{
    BenchLayoutAccounting, BenchTransactionFixture, BenchTransactionRow, BenchWriteAccounting,
};

#[cfg(feature = "slatedb")]
use crate::storage::SlateDB;
use crate::storage::{ProfileStorage, RocksDB, StorageProfile};
use crate::workload::{WorkloadRow, snapshot_value};

pub(crate) enum TransactionFixture {
    RocksDB {
        fixture: BenchTransactionFixture<RocksDB>,
        _dir: tempfile::TempDir,
    },
    #[cfg(feature = "slatedb")]
    SlateDB {
        fixture: BenchTransactionFixture<SlateDB>,
        _dir: tempfile::TempDir,
    },
}

pub(crate) type TransactionWriteAccounting = BenchWriteAccounting;
pub(crate) type TransactionLayoutAccounting = BenchLayoutAccounting;

pub(crate) async fn empty_fixture(
    profile: StorageProfile,
    rows: &[WorkloadRow],
) -> TransactionFixture {
    let rows = bench_rows(rows);
    match profile.storage() {
        ProfileStorage::RocksDB { storage, _dir: dir } => TransactionFixture::RocksDB {
            fixture: BenchTransactionFixture::new(StorageAdapter::new(storage), rows).await,
            _dir: dir,
        },
        #[cfg(feature = "slatedb")]
        ProfileStorage::SlateDB { storage, _dir: dir } => TransactionFixture::SlateDB {
            fixture: BenchTransactionFixture::new(StorageAdapter::new(storage), rows).await,
            _dir: dir,
        },
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
            Self::RocksDB { fixture, .. } => fixture.seed().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.seed().await,
        }
    }

    pub(crate) async fn insert_all(&mut self) -> usize {
        match self {
            Self::RocksDB { fixture, .. } => fixture.insert_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.insert_all().await,
        }
    }

    pub(crate) async fn insert_all_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::RocksDB { fixture, .. } => fixture.insert_all_accounting().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.insert_all_accounting().await,
        }
    }

    pub(crate) async fn read_all(&self) -> usize {
        match self {
            Self::RocksDB { fixture, .. } => fixture.read_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.read_all().await,
        }
    }

    pub(crate) async fn read_many_by_pk(&self, count: usize) -> usize {
        match self {
            Self::RocksDB { fixture, .. } => fixture.read_many_by_pk(count).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.read_many_by_pk(count).await,
        }
    }

    pub(crate) async fn read_one_by_pk(&self) -> usize {
        match self {
            Self::RocksDB { fixture, .. } => fixture.read_one_by_pk().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.read_one_by_pk().await,
        }
    }

    pub(crate) async fn update_all(&mut self) -> usize {
        match self {
            Self::RocksDB { fixture, .. } => fixture.update_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.update_all().await,
        }
    }

    pub(crate) async fn update_all_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::RocksDB { fixture, .. } => fixture.update_all_accounting().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.update_all_accounting().await,
        }
    }

    pub(crate) async fn update_one_by_pk(&mut self) -> usize {
        match self {
            Self::RocksDB { fixture, .. } => fixture.update_one_by_pk().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.update_one_by_pk().await,
        }
    }

    pub(crate) async fn update_one_by_pk_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::RocksDB { fixture, .. } => fixture.update_one_by_pk_accounting().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.update_one_by_pk_accounting().await,
        }
    }

    pub(crate) async fn delete_all(&mut self) -> usize {
        match self {
            Self::RocksDB { fixture, .. } => fixture.delete_all().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.delete_all().await,
        }
    }

    pub(crate) async fn delete_all_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::RocksDB { fixture, .. } => fixture.delete_all_accounting().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.delete_all_accounting().await,
        }
    }

    pub(crate) async fn delete_one_by_pk(&mut self) -> usize {
        match self {
            Self::RocksDB { fixture, .. } => fixture.delete_one_by_pk().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.delete_one_by_pk().await,
        }
    }

    pub(crate) async fn delete_one_by_pk_accounting(&mut self) -> TransactionWriteAccounting {
        match self {
            Self::RocksDB { fixture, .. } => fixture.delete_one_by_pk_accounting().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.delete_one_by_pk_accounting().await,
        }
    }

    pub(crate) async fn layout_accounting(&self) -> Vec<TransactionLayoutAccounting> {
        match self {
            Self::RocksDB { fixture, .. } => fixture.layout_accounting().await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { fixture, .. } => fixture.layout_accounting().await,
        }
    }
}

fn bench_rows(rows: &[WorkloadRow]) -> Vec<BenchTransactionRow> {
    rows.iter()
        .map(|row| BenchTransactionRow {
            schema_key: "json_pointer".to_string(),
            file_id: None,
            entity_pk: row.path.clone(),
            value: Arc::new(
                serde_json::from_str(&snapshot_value(&row.path, &row.value_json))
                    .expect("transaction bench value should parse"),
            ),
            updated_value: Arc::new(
                serde_json::from_str(&snapshot_value(&row.path, &row.updated_value_json))
                    .expect("transaction bench updated value should parse"),
            ),
        })
        .collect()
}
