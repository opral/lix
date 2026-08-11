#![allow(clippy::large_futures)]

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};
use lix::storage::Storage;
use lix::storage_adapter::StorageAdapter;
use lix::transaction::bench::{BenchTransactionFixture, BenchTransactionRow};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use serde_json::json;
use tempfile::TempDir;

#[derive(Clone, Copy, Debug)]
struct Measurement {
    elapsed_ns: u64,
    written_bytes: u64,
    backend_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
enum Backend {
    RocksDB,
    SlateDB,
}

impl Backend {
    const fn name(self) -> &'static str {
        match self {
            Self::RocksDB => "rocksdb",
            Self::SlateDB => "slatedb",
        }
    }
}

fn rows(count: usize) -> Vec<BenchTransactionRow> {
    (0..count)
        .map(|index| {
            let row_pk = format!("01920000-0000-7000-8000-{index:012x}");
            BenchTransactionRow {
                schema_key: "lix_account".to_string(),
                file_id: None,
                row_pk: row_pk.clone(),
                value: Arc::new(json!({
                    "id": &row_pk,
                    "name": format!("account-{index}"),
                })),
                updated_value: Arc::new(json!({
                    "id": row_pk,
                    "name": format!("updated-account-{index}"),
                })),
            }
        })
        .collect()
}

async fn measure_storage<StorageImpl>(
    storage: StorageImpl,
    rows: &[BenchTransactionRow],
) -> (Measurement, Vec<(String, u64, u64, u64)>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let storage = StorageAdapter::new(storage);
    let mut fixture = BenchTransactionFixture::new(storage, rows.to_vec()).await;
    let started = Instant::now();
    let accounting = fixture.insert_all_accounting().await;
    let elapsed_ns =
        u64::try_from(started.elapsed().as_nanos()).expect("benchmark duration fits u64");
    assert_eq!(accounting.logical_rows, rows.len());
    let layout = fixture
        .layout_accounting()
        .await
        .into_iter()
        .map(|space| {
            (
                space.space.to_string(),
                space.rows,
                space.key_bytes,
                space.value_bytes,
            )
        })
        .collect();
    (
        Measurement {
            elapsed_ns,
            written_bytes: accounting.written_bytes,
            backend_bytes: 0,
        },
        layout,
    )
}

async fn measure(
    backend: Backend,
    rows: &[BenchTransactionRow],
) -> (Measurement, Vec<(String, u64, u64, u64)>) {
    let dir = TempDir::new().expect("create UUIDv7 storage benchmark tempdir");
    let storage_path = dir.path().join(backend.name());
    match backend {
        Backend::RocksDB => {
            let storage = RocksDB::open(&storage_path).expect("open RocksDB benchmark storage");
            let (mut measurement, layout) = measure_storage(storage.clone(), rows).await;
            storage.flush().expect("flush RocksDB benchmark storage");
            measurement.backend_bytes = directory_bytes(&storage_path);
            (measurement, layout)
        }
        Backend::SlateDB => {
            let storage = SlateDB::open(&storage_path).expect("open SlateDB benchmark storage");
            let (mut measurement, layout) = measure_storage(storage.clone(), rows).await;
            storage
                .flush()
                .await
                .expect("flush SlateDB benchmark storage");
            measurement.backend_bytes = directory_bytes(&storage_path);
            (measurement, layout)
        }
    }
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| {
            entry
                .metadata()
                .map(|metadata| {
                    if metadata.is_dir() {
                        directory_bytes(&entry.path())
                    } else {
                        metadata.len()
                    }
                })
                .unwrap_or(0)
        })
        .sum()
}

fn configured_backends() -> Vec<Backend> {
    std::env::var("LIX_UUID_V7_PHYSICAL_BACKENDS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|part| match part.trim() {
                    "rocksdb" => Backend::RocksDB,
                    "slatedb" => Backend::SlateDB,
                    other => panic!(
                        "unsupported LIX_UUID_V7_PHYSICAL_BACKENDS entry {other:?}; use rocksdb or slatedb"
                    ),
                })
                .collect()
        })
        .filter(|backends: &Vec<_>| !backends.is_empty())
        .unwrap_or_else(|| vec![Backend::RocksDB, Backend::SlateDB])
}

fn configured_sizes() -> Vec<usize> {
    std::env::var("LIX_UUID_V7_PHYSICAL_ROWS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|part| {
                    part.trim()
                        .parse::<usize>()
                        .expect("LIX_UUID_V7_PHYSICAL_ROWS must contain positive integers")
                })
                .filter(|count| *count > 0)
                .collect()
        })
        .filter(|sizes: &Vec<_>| !sizes.is_empty())
        .unwrap_or_else(|| vec![1_000, 10_000, 50_000, 200_000, 220_000])
}

fn configured_cycles() -> usize {
    std::env::var("LIX_UUID_V7_PHYSICAL_CYCLES")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|cycles| *cycles > 0)
        .unwrap_or(5)
}

fn median(values: &mut [u64]) -> u64 {
    values.sort_unstable();
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        u64::midpoint(values[middle - 1], values[middle])
    } else {
        values[middle]
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "benchmark rates are display-only"
)]
fn uuid_v7_physical_keys(_criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create benchmark runtime");
    for backend in configured_backends() {
        for count in configured_sizes() {
            let rows = rows(count);
            runtime.block_on(measure(backend, &rows[..10.min(rows.len())]));
            let cycles = configured_cycles();
            let mut samples = Vec::with_capacity(cycles);
            let mut last_layout = Vec::new();
            for cycle in 0..cycles {
                let (sample, layout) = runtime.block_on(measure(backend, &rows));
                eprintln!(
                    "uuid_v7_physical_sample backend={} rows={count} cycle={cycle} elapsed_ns={} written_bytes={} backend_bytes={}",
                    backend.name(),
                    sample.elapsed_ns,
                    sample.written_bytes,
                    sample.backend_bytes
                );
                samples.push(sample);
                last_layout = layout;
            }
            let mut elapsed = samples
                .iter()
                .map(|sample| sample.elapsed_ns)
                .collect::<Vec<_>>();
            let mut written = samples
                .iter()
                .map(|sample| sample.written_bytes)
                .collect::<Vec<_>>();
            let mut backend_sizes = samples
                .iter()
                .map(|sample| sample.backend_bytes)
                .collect::<Vec<_>>();
            let elapsed_ns = median(&mut elapsed);
            let written_bytes = median(&mut written);
            let backend_bytes = median(&mut backend_sizes);
            println!(
                "uuid_v7_physical backend={} rows={count} cycles={cycles} elapsed_median_ns={elapsed_ns} rows_per_second={:.1} written_bytes_median={written_bytes} backend_bytes_median={backend_bytes}",
                backend.name(),
                count as f64 * 1_000_000_000.0 / elapsed_ns as f64,
            );
            for (space, space_rows, key_bytes, value_bytes) in last_layout {
                println!(
                    "uuid_v7_physical_layout backend={} rows={count} space={space} space_rows={space_rows} key_bytes={key_bytes} value_bytes={value_bytes}",
                    backend.name()
                );
            }
        }
    }
}

criterion_group!(benches, uuid_v7_physical_keys);
criterion_main!(benches);
