use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};
use lix_engine::storage::Memory;
use lix_engine::storage_adapter::StorageAdapter;
use lix_engine::transaction::bench::{BenchTransactionFixture, BenchTransactionRow};
use serde_json::json;

#[derive(Clone, Copy, Debug)]
struct Measurement {
    elapsed_ns: u64,
    written_bytes: u64,
}

fn rows(count: usize) -> Vec<BenchTransactionRow> {
    (0..count)
        .map(|index| {
            let entity_pk = format!("01920000-0000-7000-8000-{index:012x}");
            BenchTransactionRow {
                schema_key: "json_pointer".to_string(),
                file_id: None,
                entity_pk: entity_pk.clone(),
                value: json!({
                    "path": entity_pk.clone(),
                    "value": {
                        "cells": ["alpha", "beta", "gamma", "delta"],
                        "ordinal": index,
                    }
                }),
                updated_value: json!({
                    "path": entity_pk,
                    "value": {
                        "cells": ["alpha", "beta", "gamma", "updated"],
                        "ordinal": index,
                    }
                }),
            }
        })
        .collect()
}

async fn measure(rows: &[BenchTransactionRow]) -> (Measurement, Vec<(String, u64, u64, u64)>) {
    let storage = StorageAdapter::new(Memory::new());
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
        },
        layout,
    )
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
    for count in configured_sizes() {
        let rows = rows(count);
        runtime.block_on(measure(&rows[..10.min(rows.len())]));
        let cycles = configured_cycles();
        let mut samples = Vec::with_capacity(cycles);
        let mut last_layout = Vec::new();
        for cycle in 0..cycles {
            let (sample, layout) = runtime.block_on(measure(&rows));
            eprintln!(
                "uuid_v7_physical_sample rows={count} cycle={cycle} elapsed_ns={} written_bytes={}",
                sample.elapsed_ns, sample.written_bytes
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
        let elapsed_ns = median(&mut elapsed);
        let written_bytes = median(&mut written);
        println!(
            "uuid_v7_physical rows={count} cycles={cycles} elapsed_median_ns={elapsed_ns} rows_per_second={:.1} written_bytes_median={written_bytes}",
            count as f64 * 1_000_000_000.0 / elapsed_ns as f64,
        );
        for (space, space_rows, key_bytes, value_bytes) in last_layout {
            println!(
                "uuid_v7_physical_layout rows={count} space={space} space_rows={space_rows} key_bytes={key_bytes} value_bytes={value_bytes}"
            );
        }
    }
}

criterion_group!(benches, uuid_v7_physical_keys);
criterion_main!(benches);
