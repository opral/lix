#![cfg(feature = "storage-benches")]

use async_trait::async_trait;
use lix_engine::storage_bench::{
    self, JsonStorePayloadShape, StorageBenchConfig, StorageBenchKeyPattern,
    StorageBenchSelectivity, StorageBenchUpdateFraction,
};
use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup, BackendKvGetRequest,
    BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest, BackendKvValueBatch,
    BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch, BackendKvWriteOp,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, BytePageBuilder,
    LixError,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

type Store = BTreeMap<(String, Vec<u8>), Vec<u8>>;

fn byte_page_from_iter(values: impl IntoIterator<Item = Vec<u8>>) -> lix_engine::BytePage {
    let values = values.into_iter();
    let (lower_bound, _) = values.size_hint();
    let mut page = BytePageBuilder::with_capacity(lower_bound, 0);
    for value in values {
        page.push(&value);
    }
    page.finish()
}

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
    untracked_entries: usize,
    untracked_value_bytes: usize,
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
            untracked_entries: self
                .untracked_entries
                .saturating_sub(before.untracked_entries),
            untracked_value_bytes: self
                .untracked_value_bytes
                .saturating_sub(before.untracked_value_bytes),
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

#[derive(Debug, Clone, Copy)]
enum UntrackedAccountingWorkload {
    WriteRows {
        label: &'static str,
        rows: usize,
        payload_bytes: usize,
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
        "{:<31} {:>7} {:>8} {:>12} {:>12} {:>10} {:>11} {:>11} {:>11} {:>11} {:>9} {:>13}",
        "workload",
        "rows",
        "entries",
        "value_bytes",
        "total_bytes",
        "bytes/row",
        "chunks",
        "snapshots",
        "roots",
        "file_roots",
        "json",
        "json_bytes"
    );

    for workload in workloads {
        let row = run_workload(workload)
            .await
            .expect("storage accounting workload should run");
        println!(
            "{:<31} {:>7} {:>8} {:>12} {:>12} {:>10} {:>11} {:>11} {:>11} {:>11} {:>9} {:>13}",
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
            row.snapshot.json_entries,
            row.snapshot.json_value_bytes,
        );
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

#[tokio::test]
#[ignore = "prints deterministic untracked_state storage accounting table"]
async fn untracked_state_accounting() {
    let workloads = [
        UntrackedAccountingWorkload::WriteRows {
            label: "write_rows_payload_small",
            rows: 10_000,
            payload_bytes: 0,
        },
        UntrackedAccountingWorkload::WriteRows {
            label: "write_rows_payload_1k",
            rows: 10_000,
            payload_bytes: 1024,
        },
        UntrackedAccountingWorkload::WriteRows {
            label: "write_rows_payload_16k",
            rows: 1_000,
            payload_bytes: 16 * 1024,
        },
        UntrackedAccountingWorkload::WriteRows {
            label: "write_rows_payload_128k",
            rows: 100,
            payload_bytes: 128 * 1024,
        },
    ];

    println!(
        "{:<31} {:>7} {:>8} {:>12} {:>12} {:>10} {:>11} {:>13}",
        "workload",
        "rows",
        "entries",
        "value_bytes",
        "total_bytes",
        "bytes/row",
        "rows_ns",
        "row_bytes"
    );

    for workload in workloads {
        let row = run_untracked_workload(workload)
            .await
            .expect("untracked_state accounting workload should run");
        println!(
            "{:<31} {:>7} {:>8} {:>12} {:>12} {:>10} {:>11} {:>13}",
            untracked_workload_label(workload),
            row.rows,
            row.snapshot.entries,
            row.snapshot.value_bytes,
            row.snapshot.total_bytes(),
            row.snapshot.bytes_per_row(row.rows),
            row.snapshot.untracked_entries,
            row.snapshot.untracked_value_bytes,
        );
    }
}

struct AccountingRow {
    rows: usize,
    snapshot: AccountingSnapshot,
}

async fn run_workload(workload: AccountingWorkload) -> Result<AccountingRow, LixError> {
    let accounting_backend = AccountingBackend::default();
    let backend: Arc<dyn Backend + Send + Sync> = Arc::new(accounting_backend.clone());
    let config = config_for(workload);
    let rows = workload_rows(workload);
    let snapshot = match workload {
        AccountingWorkload::WriteRoot { .. } => {
            let fixture = storage_bench::prepare_tracked_state_write_root(config).await?;
            storage_bench::tracked_state_write_root_prepared(&backend, &fixture).await?;
            accounting_backend.accounting()?
        }
        AccountingWorkload::UpdateOne { .. } => {
            let fixture =
                storage_bench::prepare_tracked_state_update_rows(&backend, config, 1).await?;
            let before = accounting_backend.accounting()?;
            storage_bench::tracked_state_update_existing_prepared(&backend, &fixture).await?;
            accounting_backend.accounting()?.saturating_sub(before)
        }
        AccountingWorkload::AppendOne { .. } => {
            let fixture =
                storage_bench::prepare_tracked_state_append_child_rows(&backend, config, 1).await?;
            let before = accounting_backend.accounting()?;
            storage_bench::tracked_state_update_existing_prepared(&backend, &fixture).await?;
            accounting_backend.accounting()?.saturating_sub(before)
        }
        AccountingWorkload::Update10Pct { rows } => {
            let fixture = storage_bench::prepare_tracked_state_update_rows(
                &backend,
                config,
                rows.div_ceil(10),
            )
            .await?;
            let before = accounting_backend.accounting()?;
            storage_bench::tracked_state_update_existing_prepared(&backend, &fixture).await?;
            accounting_backend.accounting()?.saturating_sub(before)
        }
    };
    Ok(AccountingRow { rows, snapshot })
}

async fn run_json_workload(workload: JsonAccountingWorkload) -> Result<AccountingRow, LixError> {
    let accounting_backend = AccountingBackend::default();
    let backend: Arc<dyn Backend + Send + Sync> = Arc::new(accounting_backend.clone());
    let rows = json_workload_rows(workload);
    let snapshot = match workload {
        JsonAccountingWorkload::Raw1k { rows } => {
            let fixture =
                storage_bench::prepare_json_store_write(JsonStorePayloadShape::SmallRaw1k, rows)
                    .await?;
            storage_bench::json_store_write_prepared(&backend, &fixture).await?;
            accounting_backend.accounting()?
        }
        JsonAccountingWorkload::Structured16k { rows } => {
            let fixture = storage_bench::prepare_json_store_write(
                JsonStorePayloadShape::MediumStructured16k,
                rows,
            )
            .await?;
            storage_bench::json_store_write_prepared(&backend, &fixture).await?;
            accounting_backend.accounting()?
        }
        JsonAccountingWorkload::Structured128k { rows } => {
            let fixture = storage_bench::prepare_json_store_write(
                JsonStorePayloadShape::LargeStructured128k,
                rows,
            )
            .await?;
            storage_bench::json_store_write_prepared(&backend, &fixture).await?;
            accounting_backend.accounting()?
        }
        JsonAccountingWorkload::Array128k { rows } => {
            let fixture = storage_bench::prepare_json_store_write(
                JsonStorePayloadShape::LargeArray128k,
                rows,
            )
            .await?;
            storage_bench::json_store_write_prepared(&backend, &fixture).await?;
            accounting_backend.accounting()?
        }
        JsonAccountingWorkload::DedupeSame16k { rows } => {
            let fixture = storage_bench::prepare_json_store_write_dedupe(
                JsonStorePayloadShape::MediumStructured16k,
                rows,
            )
            .await?;
            storage_bench::json_store_write_prepared(&backend, &fixture).await?;
            accounting_backend.accounting()?
        }
        JsonAccountingWorkload::BaseUpdateObject1Of1000 { rows } => {
            let fixture =
                storage_bench::prepare_json_store_base_update_object(&backend, rows).await?;
            let before = accounting_backend.accounting()?;
            storage_bench::json_store_write_against_base_object_prepared(&backend, &fixture)
                .await?;
            accounting_backend.accounting()?.saturating_sub(before)
        }
        JsonAccountingWorkload::BaseUpdateArray1Of1000 { rows } => {
            let fixture =
                storage_bench::prepare_json_store_base_update_array(&backend, rows).await?;
            let before = accounting_backend.accounting()?;
            storage_bench::json_store_write_against_base_array_prepared(&backend, &fixture).await?;
            accounting_backend.accounting()?.saturating_sub(before)
        }
    };
    Ok(AccountingRow { rows, snapshot })
}

async fn run_changelog_workload(
    workload: ChangelogAccountingWorkload,
) -> Result<AccountingRow, LixError> {
    let accounting_backend = AccountingBackend::default();
    let backend: Arc<dyn Backend + Send + Sync> = Arc::new(accounting_backend.clone());
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
        snapshot: accounting_backend.accounting()?,
    })
}

async fn run_untracked_workload(
    workload: UntrackedAccountingWorkload,
) -> Result<AccountingRow, LixError> {
    let accounting_backend = AccountingBackend::default();
    let backend: Arc<dyn Backend + Send + Sync> = Arc::new(accounting_backend.clone());
    let rows = untracked_workload_rows(workload);
    let fixture =
        storage_bench::prepare_untracked_state_write_rows(untracked_config_for(workload)).await?;
    storage_bench::untracked_state_write_rows_prepared(&backend, &fixture).await?;
    Ok(AccountingRow {
        rows,
        snapshot: accounting_backend.accounting()?,
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

fn untracked_config_for(workload: UntrackedAccountingWorkload) -> StorageBenchConfig {
    StorageBenchConfig {
        rows: untracked_workload_rows(workload),
        blob_bytes: 1024,
        state_payload_bytes: match workload {
            UntrackedAccountingWorkload::WriteRows { payload_bytes, .. } => payload_bytes,
        },
        key_pattern: StorageBenchKeyPattern::Sequential,
        selectivity: StorageBenchSelectivity::Percent100,
        update_fraction: StorageBenchUpdateFraction::Percent100,
    }
}

fn untracked_workload_rows(workload: UntrackedAccountingWorkload) -> usize {
    match workload {
        UntrackedAccountingWorkload::WriteRows { rows, .. } => rows,
    }
}

fn untracked_workload_label(workload: UntrackedAccountingWorkload) -> String {
    match workload {
        UntrackedAccountingWorkload::WriteRows { label, rows, .. } => {
            format!("{label}/{}", row_label(rows))
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
                "u" => {
                    snapshot.untracked_entries += 1;
                    snapshot.untracked_value_bytes += value.len();
                }
                _ => {}
            }
        }
        Ok(snapshot)
    }
}

#[async_trait]
impl Backend for AccountingBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(AccountingTransaction {
            store: Arc::clone(&self.store),
            finalized: false,
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(AccountingTransaction {
            store: Arc::clone(&self.store),
            finalized: false,
        }))
    }
}

struct AccountingTransaction {
    store: Arc<Mutex<Store>>,
    finalized: bool,
}

impl AccountingTransaction {
    fn lock_store(&self) -> Result<std::sync::MutexGuard<'_, Store>, LixError> {
        self.store
            .lock()
            .map_err(|_| LixError::new("LIX_ERROR_UNKNOWN", "accounting store mutex poisoned"))
    }

    fn scan_filtered_pairs(
        &self,
        request: &BackendKvScanRequest,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, LixError> {
        let store = self.lock_store()?;
        let scan_limit = request
            .limit
            .checked_add(1 + usize::from(request.after.is_some()))
            .unwrap_or(request.limit);
        let mut pairs = scan_store(&store, &request.namespace, &request.range, Some(scan_limit));
        pairs.retain(|(key, _)| {
            request
                .after
                .as_deref()
                .is_none_or(|after| key.as_slice() > after)
        });
        Ok(pairs)
    }
}

#[async_trait]
impl BackendReadTransaction for AccountingTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        let store = self.lock_store()?;
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let mut values = BytePageBuilder::with_capacity(group.keys.len(), 0);
            let mut present = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                if let Some(value) = store.get(&(namespace.clone(), key)) {
                    values.push(value);
                    present.push(true);
                } else {
                    values.push([]);
                    present.push(false);
                }
            }
            groups.push(BackendKvValueGroup::new(
                namespace,
                values.finish(),
                present,
            ));
        }
        Ok(BackendKvValueBatch { groups })
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        let store = self.lock_store()?;
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let mut exists = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                exists.push(store.contains_key(&(namespace.clone(), key)));
            }
            groups.push(BackendKvExistsGroup { namespace, exists });
        }
        Ok(BackendKvExistsBatch { groups })
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        let pairs = self.scan_filtered_pairs(&request)?;
        let has_more = pairs.len() > request.limit;
        let resume_after = has_more
            .then(|| {
                pairs
                    .get(request.limit.saturating_sub(1))
                    .map(|(key, _)| key.clone())
            })
            .flatten();
        Ok(BackendKvKeyPage {
            keys: byte_page_from_iter(pairs.into_iter().take(request.limit).map(|(key, _)| key)),
            resume_after,
        })
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        let pairs = self.scan_filtered_pairs(&request)?;
        let has_more = pairs.len() > request.limit;
        let resume_after = has_more
            .then(|| {
                pairs
                    .get(request.limit.saturating_sub(1))
                    .map(|(key, _)| key.clone())
            })
            .flatten();
        Ok(BackendKvValuePage {
            values: byte_page_from_iter(
                pairs
                    .into_iter()
                    .take(request.limit)
                    .map(|(_, value)| value),
            ),
            resume_after,
        })
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        let pairs = self.scan_filtered_pairs(&request)?;
        let has_more = pairs.len() > request.limit;
        let resume_after = has_more
            .then(|| {
                pairs
                    .get(request.limit.saturating_sub(1))
                    .map(|(key, _)| key.clone())
            })
            .flatten();
        let mut keys = BytePageBuilder::with_capacity(request.limit.min(pairs.len()), 0);
        let mut values = BytePageBuilder::with_capacity(request.limit.min(pairs.len()), 0);
        for (key, value) in pairs.into_iter().take(request.limit) {
            keys.push(&key);
            values.push(&value);
        }
        Ok(BackendKvEntryPage {
            keys: keys.finish(),
            values: values.finish(),
            resume_after,
        })
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        self.finalized = true;
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for AccountingTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        let mut stats = BackendKvWriteStats::default();
        let mut store = self.lock_store()?;
        for group in batch.groups {
            let namespace = group.namespace().to_string();
            for op in group.ops() {
                match op {
                    BackendKvWriteOp::Put { key, value } => {
                        stats.puts += 1;
                        stats.bytes_written += key.len() + value.len();
                        store.insert((namespace.clone(), key.clone()), value.clone());
                    }
                    BackendKvWriteOp::Delete { key } => {
                        stats.deletes += 1;
                        stats.bytes_written += key.len();
                        store.remove(&(namespace.clone(), key.clone()));
                    }
                    BackendKvWriteOp::DeleteRange { range } => {
                        stats.delete_ranges += 1;
                        stats.bytes_written += delete_range_bytes(range);
                        store.retain(|(candidate_namespace, key), _| {
                            candidate_namespace != &namespace || !key_matches_range(key, range)
                        });
                    }
                }
            }
        }
        Ok(stats)
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        self.finalized = true;
        Ok(())
    }
}

fn scan_store(
    store: &Store,
    namespace: &str,
    range: &BackendKvScanRange,
    limit: Option<usize>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut pairs = Vec::new();
    for ((row_namespace, key), value) in store.iter() {
        if row_namespace != namespace {
            continue;
        }
        let matches = match range {
            BackendKvScanRange::Prefix(prefix) => key.starts_with(prefix),
            BackendKvScanRange::Range { start, end } => key >= start && key < end,
        };
        if matches {
            pairs.push((key.clone(), value.clone()));
        }
        if limit.is_some_and(|limit| pairs.len() >= limit) {
            break;
        }
    }
    pairs
}

fn key_matches_range(key: &[u8], range: &BackendKvScanRange) -> bool {
    match range {
        BackendKvScanRange::Prefix(prefix) => key.starts_with(prefix),
        BackendKvScanRange::Range { start, end } => key >= start.as_slice() && key < end.as_slice(),
    }
}

fn delete_range_bytes(range: &BackendKvScanRange) -> usize {
    match range {
        BackendKvScanRange::Prefix(prefix) => prefix.len(),
        BackendKvScanRange::Range { start, end } => start.len() + end.len(),
    }
}
