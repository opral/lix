#![cfg(feature = "storage-benches")]

use async_trait::async_trait;
use lix_engine::storage_bench::{
    self, JsonStorePayloadShape, StorageBenchConfig, StorageBenchKeyPattern,
    StorageBenchSelectivity, StorageBenchUpdateFraction,
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
    json_entries: usize,
    json_value_bytes: usize,
    json_chunk_entries: usize,
    json_chunk_value_bytes: usize,
    changelog_entries: usize,
    changelog_value_bytes: usize,
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
            json_entries: self.json_entries.saturating_sub(before.json_entries),
            json_value_bytes: self
                .json_value_bytes
                .saturating_sub(before.json_value_bytes),
            json_chunk_entries: self
                .json_chunk_entries
                .saturating_sub(before.json_chunk_entries),
            json_chunk_value_bytes: self
                .json_chunk_value_bytes
                .saturating_sub(before.json_chunk_value_bytes),
            changelog_entries: self
                .changelog_entries
                .saturating_sub(before.changelog_entries),
            changelog_value_bytes: self
                .changelog_value_bytes
                .saturating_sub(before.changelog_value_bytes),
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

#[derive(Debug, Clone, Copy)]
enum JsonAccountingWorkload {
    Raw1k { rows: usize },
    Structured16k { rows: usize },
    Structured128k { rows: usize },
    Array128k { rows: usize },
    DedupeSame16k { rows: usize },
    BaseUpdateObject1Of1000 { rows: usize },
    BaseUpdateArray1Of1000 { rows: usize },
}

#[derive(Debug, Clone, Copy)]
enum ChangelogAccountingWorkload {
    AppendSmall { rows: usize },
    Append1k { rows: usize },
    Append16k { rows: usize },
    Tombstones { rows: usize },
    Metadata1k { rows: usize },
    CompositeEntityIds { rows: usize },
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

#[tokio::test]
#[ignore = "prints deterministic json_store storage accounting table"]
async fn json_store_accounting() {
    let workloads = [
        JsonAccountingWorkload::Raw1k { rows: 1_000 },
        JsonAccountingWorkload::Structured16k { rows: 200 },
        JsonAccountingWorkload::Structured128k { rows: 50 },
        JsonAccountingWorkload::Array128k { rows: 50 },
        JsonAccountingWorkload::DedupeSame16k { rows: 1_000 },
        JsonAccountingWorkload::BaseUpdateObject1Of1000 { rows: 50 },
        JsonAccountingWorkload::BaseUpdateArray1Of1000 { rows: 50 },
    ];

    println!(
        "{:<37} {:>7} {:>8} {:>12} {:>12} {:>10} {:>11} {:>15}",
        "workload",
        "rows",
        "entries",
        "value_bytes",
        "total_bytes",
        "bytes/row",
        "json_refs",
        "json_chunks"
    );

    for workload in workloads {
        let row = run_json_workload(workload)
            .await
            .expect("json_store accounting workload should run");
        println!(
            "{:<37} {:>7} {:>8} {:>12} {:>12} {:>10} {:>11} {:>15}",
            json_workload_label(workload),
            row.rows,
            row.snapshot.entries,
            row.snapshot.value_bytes,
            row.snapshot.total_bytes(),
            row.snapshot.bytes_per_row(row.rows),
            row.snapshot.json_entries,
            row.snapshot.json_chunk_entries,
        );
    }
}

#[tokio::test]
#[ignore = "prints deterministic changelog storage accounting table"]
async fn changelog_accounting() {
    let workloads = [
        ChangelogAccountingWorkload::AppendSmall { rows: 10_000 },
        ChangelogAccountingWorkload::Append1k { rows: 10_000 },
        ChangelogAccountingWorkload::Append16k { rows: 1_000 },
        ChangelogAccountingWorkload::Tombstones { rows: 10_000 },
        ChangelogAccountingWorkload::Metadata1k { rows: 10_000 },
        ChangelogAccountingWorkload::CompositeEntityIds { rows: 10_000 },
    ];

    println!(
        "{:<31} {:>7} {:>8} {:>12} {:>12} {:>10} {:>11} {:>13}",
        "workload",
        "rows",
        "entries",
        "value_bytes",
        "total_bytes",
        "bytes/row",
        "changes",
        "change_bytes"
    );

    for workload in workloads {
        let row = run_changelog_workload(workload)
            .await
            .expect("changelog accounting workload should run");
        println!(
            "{:<31} {:>7} {:>8} {:>12} {:>12} {:>10} {:>11} {:>13}",
            changelog_workload_label(workload),
            row.rows,
            row.snapshot.entries,
            row.snapshot.value_bytes,
            row.snapshot.total_bytes(),
            row.snapshot.bytes_per_row(row.rows),
            row.snapshot.changelog_entries,
            row.snapshot.changelog_value_bytes,
        );
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

async fn run_json_workload(workload: JsonAccountingWorkload) -> Result<AccountingRow, LixError> {
    let backend = AccountingBackend::default();
    let rows = json_workload_rows(workload);
    let snapshot = match workload {
        JsonAccountingWorkload::Raw1k { rows } => {
            let fixture =
                storage_bench::prepare_json_store_write(JsonStorePayloadShape::SmallRaw1k, rows)
                    .await?;
            storage_bench::json_store_write_prepared(&backend, &fixture).await?;
            backend.accounting()?
        }
        JsonAccountingWorkload::Structured16k { rows } => {
            let fixture = storage_bench::prepare_json_store_write(
                JsonStorePayloadShape::MediumStructured16k,
                rows,
            )
            .await?;
            storage_bench::json_store_write_prepared(&backend, &fixture).await?;
            backend.accounting()?
        }
        JsonAccountingWorkload::Structured128k { rows } => {
            let fixture = storage_bench::prepare_json_store_write(
                JsonStorePayloadShape::LargeStructured128k,
                rows,
            )
            .await?;
            storage_bench::json_store_write_prepared(&backend, &fixture).await?;
            backend.accounting()?
        }
        JsonAccountingWorkload::Array128k { rows } => {
            let fixture = storage_bench::prepare_json_store_write(
                JsonStorePayloadShape::LargeArray128k,
                rows,
            )
            .await?;
            storage_bench::json_store_write_prepared(&backend, &fixture).await?;
            backend.accounting()?
        }
        JsonAccountingWorkload::DedupeSame16k { rows } => {
            let fixture = storage_bench::prepare_json_store_write_dedupe(
                JsonStorePayloadShape::MediumStructured16k,
                rows,
            )
            .await?;
            storage_bench::json_store_write_prepared(&backend, &fixture).await?;
            backend.accounting()?
        }
        JsonAccountingWorkload::BaseUpdateObject1Of1000 { rows } => {
            let fixture =
                storage_bench::prepare_json_store_base_update_object(&backend, rows).await?;
            let before = backend.accounting()?;
            storage_bench::json_store_write_against_base_object_prepared(&backend, &fixture)
                .await?;
            backend.accounting()?.saturating_sub(before)
        }
        JsonAccountingWorkload::BaseUpdateArray1Of1000 { rows } => {
            let fixture =
                storage_bench::prepare_json_store_base_update_array(&backend, rows).await?;
            let before = backend.accounting()?;
            storage_bench::json_store_write_against_base_array_prepared(&backend, &fixture).await?;
            backend.accounting()?.saturating_sub(before)
        }
    };
    Ok(AccountingRow { rows, snapshot })
}

async fn run_changelog_workload(
    workload: ChangelogAccountingWorkload,
) -> Result<AccountingRow, LixError> {
    let backend = AccountingBackend::default();
    let rows = changelog_workload_rows(workload);
    let config = changelog_config_for(workload);
    let fixture = match workload {
        ChangelogAccountingWorkload::AppendSmall { .. }
        | ChangelogAccountingWorkload::Append1k { .. }
        | ChangelogAccountingWorkload::Append16k { .. } => {
            storage_bench::prepare_changelog_append_changes(config).await?
        }
        ChangelogAccountingWorkload::Tombstones { .. } => {
            storage_bench::prepare_changelog_append_tombstones(config).await?
        }
        ChangelogAccountingWorkload::Metadata1k { .. } => {
            storage_bench::prepare_changelog_append_metadata(config).await?
        }
        ChangelogAccountingWorkload::CompositeEntityIds { .. } => {
            storage_bench::prepare_changelog_append_composite_entity_ids(config).await?
        }
    };
    storage_bench::changelog_append_changes_prepared(&backend, &fixture).await?;
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

fn json_workload_rows(workload: JsonAccountingWorkload) -> usize {
    match workload {
        JsonAccountingWorkload::Raw1k { rows }
        | JsonAccountingWorkload::Structured16k { rows }
        | JsonAccountingWorkload::Structured128k { rows }
        | JsonAccountingWorkload::Array128k { rows }
        | JsonAccountingWorkload::DedupeSame16k { rows }
        | JsonAccountingWorkload::BaseUpdateObject1Of1000 { rows }
        | JsonAccountingWorkload::BaseUpdateArray1Of1000 { rows } => rows,
    }
}

fn changelog_config_for(workload: ChangelogAccountingWorkload) -> StorageBenchConfig {
    StorageBenchConfig {
        rows: changelog_workload_rows(workload),
        blob_bytes: 1024,
        state_payload_bytes: match workload {
            ChangelogAccountingWorkload::AppendSmall { .. }
            | ChangelogAccountingWorkload::Tombstones { .. }
            | ChangelogAccountingWorkload::CompositeEntityIds { .. } => 0,
            ChangelogAccountingWorkload::Append1k { .. }
            | ChangelogAccountingWorkload::Metadata1k { .. } => 1024,
            ChangelogAccountingWorkload::Append16k { .. } => 16 * 1024,
        },
        key_pattern: StorageBenchKeyPattern::Sequential,
        selectivity: StorageBenchSelectivity::Percent100,
        update_fraction: StorageBenchUpdateFraction::Percent100,
    }
}

fn changelog_workload_rows(workload: ChangelogAccountingWorkload) -> usize {
    match workload {
        ChangelogAccountingWorkload::AppendSmall { rows }
        | ChangelogAccountingWorkload::Append1k { rows }
        | ChangelogAccountingWorkload::Append16k { rows }
        | ChangelogAccountingWorkload::Tombstones { rows }
        | ChangelogAccountingWorkload::Metadata1k { rows }
        | ChangelogAccountingWorkload::CompositeEntityIds { rows } => rows,
    }
}

fn changelog_workload_label(workload: ChangelogAccountingWorkload) -> String {
    match workload {
        ChangelogAccountingWorkload::AppendSmall { rows } => {
            format!("append_small/{}", row_label(rows))
        }
        ChangelogAccountingWorkload::Append1k { rows } => {
            format!("append_1k/{}", row_label(rows))
        }
        ChangelogAccountingWorkload::Append16k { rows } => {
            format!("append_16k/{}", row_label(rows))
        }
        ChangelogAccountingWorkload::Tombstones { rows } => {
            format!("tombstones/{}", row_label(rows))
        }
        ChangelogAccountingWorkload::Metadata1k { rows } => {
            format!("metadata_1k/{}", row_label(rows))
        }
        ChangelogAccountingWorkload::CompositeEntityIds { rows } => {
            format!("composite_entity_ids/{}", row_label(rows))
        }
    }
}

fn json_workload_label(workload: JsonAccountingWorkload) -> String {
    match workload {
        JsonAccountingWorkload::Raw1k { rows } => {
            format!("raw_1k/{}", row_label(rows))
        }
        JsonAccountingWorkload::Structured16k { rows } => {
            format!("structured_16k/{}", row_label(rows))
        }
        JsonAccountingWorkload::Structured128k { rows } => {
            format!("structured_128k/{}", row_label(rows))
        }
        JsonAccountingWorkload::Array128k { rows } => {
            format!("array_128k/{}", row_label(rows))
        }
        JsonAccountingWorkload::DedupeSame16k { rows } => {
            format!("dedupe_same_16k/{}", row_label(rows))
        }
        JsonAccountingWorkload::BaseUpdateObject1Of1000 { rows } => {
            format!("base_update_object_1_of_1000/{}", row_label(rows))
        }
        JsonAccountingWorkload::BaseUpdateArray1Of1000 { rows } => {
            format!("base_update_array_1_of_1000/{}", row_label(rows))
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
                "json_store.json" => {
                    snapshot.json_entries += 1;
                    snapshot.json_value_bytes += value.len();
                }
                "json_store.json_chunk" => {
                    snapshot.json_chunk_entries += 1;
                    snapshot.json_chunk_value_bytes += value.len();
                }
                "changelog.change" => {
                    snapshot.changelog_entries += 1;
                    snapshot.changelog_value_bytes += value.len();
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
