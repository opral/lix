#![cfg(feature = "storage-benches")]

use async_trait::async_trait;
use lix_engine::storage_bench::{
    self, StorageBenchConfig, StorageBenchKeyPattern, StorageBenchSelectivity,
    StorageBenchUpdateFraction,
};
use lix_engine::{
    KvPair, KvScanRange, LixBackend, LixBackendTransaction, LixError, TransactionBeginMode,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

type Store = BTreeMap<(String, Vec<u8>), Vec<u8>>;

#[derive(Clone, Default)]
struct AccountingBackend {
    store: Arc<Mutex<Store>>,
}

#[derive(Debug, Clone, Copy, Default)]
struct AccountingSnapshot {
    entries: usize,
    key_bytes: usize,
    value_bytes: usize,
    tracked_chunk_entries: usize,
    tracked_chunk_value_bytes: usize,
    tracked_snapshot_entries: usize,
    tracked_snapshot_value_bytes: usize,
    tracked_root_entries: usize,
    tracked_by_file_root_entries: usize,
}

impl AccountingSnapshot {
    fn total_bytes(self) -> usize {
        self.key_bytes + self.value_bytes
    }

    fn bytes_per_row(self, rows: usize) -> usize {
        if rows == 0 {
            0
        } else {
            self.total_bytes() / rows
        }
    }

    fn saturating_sub(self, before: Self) -> Self {
        Self {
            entries: self.entries.saturating_sub(before.entries),
            key_bytes: self.key_bytes.saturating_sub(before.key_bytes),
            value_bytes: self.value_bytes.saturating_sub(before.value_bytes),
            tracked_chunk_entries: self
                .tracked_chunk_entries
                .saturating_sub(before.tracked_chunk_entries),
            tracked_chunk_value_bytes: self
                .tracked_chunk_value_bytes
                .saturating_sub(before.tracked_chunk_value_bytes),
            tracked_snapshot_entries: self
                .tracked_snapshot_entries
                .saturating_sub(before.tracked_snapshot_entries),
            tracked_snapshot_value_bytes: self
                .tracked_snapshot_value_bytes
                .saturating_sub(before.tracked_snapshot_value_bytes),
            tracked_root_entries: self
                .tracked_root_entries
                .saturating_sub(before.tracked_root_entries),
            tracked_by_file_root_entries: self
                .tracked_by_file_root_entries
                .saturating_sub(before.tracked_by_file_root_entries),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum AccountingWorkload {
    WriteRoot {
        label: &'static str,
        rows: usize,
        payload_bytes: usize,
    },
    UpdateOne {
        rows: usize,
    },
    AppendOne {
        rows: usize,
    },
    Update10Pct {
        rows: usize,
    },
}

#[tokio::test]
#[ignore = "prints deterministic storage accounting table"]
async fn storage_accounting() {
    let workloads = [
        AccountingWorkload::WriteRoot {
            label: "write_root_payload_small",
            rows: 10_000,
            payload_bytes: 0,
        },
        AccountingWorkload::WriteRoot {
            label: "write_root_payload_1k",
            rows: 10_000,
            payload_bytes: 1024,
        },
        AccountingWorkload::WriteRoot {
            label: "write_root_payload_16k",
            rows: 1_000,
            payload_bytes: 16 * 1024,
        },
        AccountingWorkload::WriteRoot {
            label: "write_root_payload_128k",
            rows: 100,
            payload_bytes: 128 * 1024,
        },
        AccountingWorkload::UpdateOne { rows: 100_000 },
        AccountingWorkload::AppendOne { rows: 100_000 },
        AccountingWorkload::Update10Pct { rows: 10_000 },
    ];

    println!(
        "{:<31} {:>7} {:>8} {:>12} {:>12} {:>10} {:>11} {:>11} {:>11} {:>11}",
        "workload",
        "rows",
        "entries",
        "value_bytes",
        "total_bytes",
        "bytes/row",
        "chunks",
        "snapshots",
        "roots",
        "file_roots"
    );

    for workload in workloads {
        let row = run_workload(workload)
            .await
            .expect("storage accounting workload should run");
        println!(
            "{:<31} {:>7} {:>8} {:>12} {:>12} {:>10} {:>11} {:>11} {:>11} {:>11}",
            workload_label(workload),
            row.rows,
            row.snapshot.entries,
            row.snapshot.value_bytes,
            row.snapshot.total_bytes(),
            row.snapshot.bytes_per_row(row.rows),
            row.snapshot.tracked_chunk_entries,
            row.snapshot.tracked_snapshot_entries,
            row.snapshot.tracked_root_entries,
            row.snapshot.tracked_by_file_root_entries,
        );
    }
}

#[tokio::test]
#[ignore = "prints max inline encoded value accounting table"]
async fn max_inline_encoded_value_accounting() {
    let workloads = [
        AccountingWorkload::WriteRoot {
            label: "write_root_payload_small",
            rows: 10_000,
            payload_bytes: 0,
        },
        AccountingWorkload::WriteRoot {
            label: "write_root_payload_1k",
            rows: 10_000,
            payload_bytes: 1024,
        },
        AccountingWorkload::WriteRoot {
            label: "write_root_payload_16k",
            rows: 1_000,
            payload_bytes: 16 * 1024,
        },
        AccountingWorkload::WriteRoot {
            label: "write_root_payload_128k",
            rows: 100,
            payload_bytes: 128 * 1024,
        },
    ];

    println!(
        "{:<10} {:<31} {:>7} {:>8} {:>12} {:>12} {:>10} {:>11} {:>11}",
        "max_inline",
        "workload",
        "rows",
        "entries",
        "value_bytes",
        "total_bytes",
        "bytes/row",
        "chunks",
        "snapshots"
    );

    for threshold in [512, 1024, 2048, 4096, 8192] {
        for workload in workloads {
            let row = run_write_root_workload_with_max_inline_encoded_value(workload, threshold)
                .await
                .expect("storage accounting max-inline workload should run");
            println!(
                "{:<10} {:<31} {:>7} {:>8} {:>12} {:>12} {:>10} {:>11} {:>11}",
                threshold,
                workload_label(workload),
                row.rows,
                row.snapshot.entries,
                row.snapshot.value_bytes,
                row.snapshot.total_bytes(),
                row.snapshot.bytes_per_row(row.rows),
                row.snapshot.tracked_chunk_entries,
                row.snapshot.tracked_snapshot_entries,
            );
        }
    }
}

struct AccountingRow {
    rows: usize,
    snapshot: AccountingSnapshot,
}

async fn run_workload(workload: AccountingWorkload) -> Result<AccountingRow, LixError> {
    let backend = AccountingBackend::default();
    let config = config_for(workload);
    let rows = workload_rows(workload);
    let snapshot = match workload {
        AccountingWorkload::WriteRoot { .. } => {
            let fixture = storage_bench::prepare_tracked_state_write_root(config).await?;
            storage_bench::tracked_state_write_root_prepared(&backend, &fixture).await?;
            backend.accounting()?
        }
        AccountingWorkload::UpdateOne { .. } => {
            let fixture =
                storage_bench::prepare_tracked_state_update_rows(&backend, config, 1).await?;
            let before = backend.accounting()?;
            storage_bench::tracked_state_update_existing_prepared(&backend, &fixture).await?;
            backend.accounting()?.saturating_sub(before)
        }
        AccountingWorkload::AppendOne { .. } => {
            let fixture =
                storage_bench::prepare_tracked_state_append_child_rows(&backend, config, 1).await?;
            let before = backend.accounting()?;
            storage_bench::tracked_state_update_existing_prepared(&backend, &fixture).await?;
            backend.accounting()?.saturating_sub(before)
        }
        AccountingWorkload::Update10Pct { rows } => {
            let fixture = storage_bench::prepare_tracked_state_update_rows(
                &backend,
                config,
                rows.div_ceil(10),
            )
            .await?;
            let before = backend.accounting()?;
            storage_bench::tracked_state_update_existing_prepared(&backend, &fixture).await?;
            backend.accounting()?.saturating_sub(before)
        }
    };
    Ok(AccountingRow { rows, snapshot })
}

async fn run_write_root_workload_with_max_inline_encoded_value(
    workload: AccountingWorkload,
    max_inline_encoded_value_bytes: usize,
) -> Result<AccountingRow, LixError> {
    let backend = AccountingBackend::default();
    let config = config_for(workload);
    let rows = workload_rows(workload);
    let fixture =
        storage_bench::prepare_tracked_state_write_root_with_max_inline_encoded_value_bytes(
            config,
            max_inline_encoded_value_bytes,
        )
        .await?;
    storage_bench::tracked_state_write_root_prepared(&backend, &fixture).await?;
    Ok(AccountingRow {
        rows,
        snapshot: backend.accounting()?,
    })
}

fn config_for(workload: AccountingWorkload) -> StorageBenchConfig {
    StorageBenchConfig {
        rows: workload_rows(workload),
        blob_bytes: 1024,
        state_payload_bytes: match workload {
            AccountingWorkload::WriteRoot { payload_bytes, .. } => payload_bytes,
            AccountingWorkload::UpdateOne { .. }
            | AccountingWorkload::AppendOne { .. }
            | AccountingWorkload::Update10Pct { .. } => 256,
        },
        key_pattern: StorageBenchKeyPattern::Sequential,
        selectivity: StorageBenchSelectivity::Percent100,
        update_fraction: StorageBenchUpdateFraction::Percent100,
    }
}

fn workload_rows(workload: AccountingWorkload) -> usize {
    match workload {
        AccountingWorkload::WriteRoot { rows, .. }
        | AccountingWorkload::UpdateOne { rows }
        | AccountingWorkload::AppendOne { rows }
        | AccountingWorkload::Update10Pct { rows } => rows,
    }
}

fn workload_label(workload: AccountingWorkload) -> String {
    match workload {
        AccountingWorkload::WriteRoot { label, rows, .. } => format!("{label}/{}", row_label(rows)),
        AccountingWorkload::UpdateOne { rows } => format!("update_1_existing/{}", row_label(rows)),
        AccountingWorkload::AppendOne { rows } => {
            format!("append_1_new_child_commit/{}", row_label(rows))
        }
        AccountingWorkload::Update10Pct { rows } => {
            format!("update_10pct_existing/{}", row_label(rows))
        }
    }
}

fn row_label(rows: usize) -> String {
    match rows {
        100_000 => "100k".to_string(),
        10_000 => "10k".to_string(),
        1_000 => "1k".to_string(),
        rows => rows.to_string(),
    }
}

impl AccountingBackend {
    fn lock_store(&self) -> Result<std::sync::MutexGuard<'_, Store>, LixError> {
        self.store
            .lock()
            .map_err(|_| LixError::new("LIX_ERROR_UNKNOWN", "accounting store mutex poisoned"))
    }

    fn accounting(&self) -> Result<AccountingSnapshot, LixError> {
        let store = self.lock_store()?;
        let mut snapshot = AccountingSnapshot::default();
        for ((namespace, key), value) in store.iter() {
            snapshot.entries += 1;
            snapshot.key_bytes += key.len();
            snapshot.value_bytes += value.len();
            match namespace.as_str() {
                "tracked_state.tree.chunk" => {
                    snapshot.tracked_chunk_entries += 1;
                    snapshot.tracked_chunk_value_bytes += value.len();
                }
                "tracked_state.tree.root" => {
                    snapshot.tracked_root_entries += 1;
                }
                "tracked_state.tree.root.by_file" => {
                    snapshot.tracked_by_file_root_entries += 1;
                }
                "tracked_state.snapshot" => {
                    snapshot.tracked_snapshot_entries += 1;
                    snapshot.tracked_snapshot_value_bytes += value.len();
                }
                _ => {}
            }
        }
        Ok(snapshot)
    }
}

#[async_trait]
impl LixBackend for AccountingBackend {
    async fn begin_transaction(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(AccountingTransaction {
            store: Arc::clone(&self.store),
            mode,
            finalized: false,
        }))
    }

    async fn kv_get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self
            .lock_store()?
            .get(&(namespace.to_string(), key.to_vec()))
            .cloned())
    }

    async fn kv_scan(
        &self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        let store = self.lock_store()?;
        Ok(scan_store(&store, namespace, range, limit))
    }
}

struct AccountingTransaction {
    store: Arc<Mutex<Store>>,
    mode: TransactionBeginMode,
    finalized: bool,
}

impl AccountingTransaction {
    fn lock_store(&self) -> Result<std::sync::MutexGuard<'_, Store>, LixError> {
        self.store
            .lock()
            .map_err(|_| LixError::new("LIX_ERROR_UNKNOWN", "accounting store mutex poisoned"))
    }
}

#[async_trait]
impl LixBackendTransaction for AccountingTransaction {
    fn mode(&self) -> TransactionBeginMode {
        self.mode
    }

    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self
            .lock_store()?
            .get(&(namespace.to_string(), key.to_vec()))
            .cloned())
    }

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        let store = self.lock_store()?;
        Ok(scan_store(&store, namespace, range, limit))
    }

    async fn kv_put(&mut self, namespace: &str, key: &[u8], value: &[u8]) -> Result<(), LixError> {
        self.lock_store()?
            .insert((namespace.to_string(), key.to_vec()), value.to_vec());
        Ok(())
    }

    async fn kv_delete(&mut self, namespace: &str, key: &[u8]) -> Result<(), LixError> {
        self.lock_store()?
            .remove(&(namespace.to_string(), key.to_vec()));
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        self.finalized = true;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        self.finalized = true;
        Ok(())
    }
}

fn scan_store(
    store: &Store,
    namespace: &str,
    range: KvScanRange,
    limit: Option<usize>,
) -> Vec<KvPair> {
    let mut pairs = Vec::new();
    for ((row_namespace, key), value) in store.iter() {
        if row_namespace != namespace {
            continue;
        }
        let matches = match &range {
            KvScanRange::Prefix(prefix) => key.starts_with(prefix),
            KvScanRange::Range { start, end } => key >= start && key < end,
        };
        if matches {
            pairs.push(KvPair::new(key.clone(), value.clone()));
        }
        if limit.is_some_and(|limit| pairs.len() >= limit) {
            break;
        }
    }
    pairs
}
