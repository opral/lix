//! Profile the payload that lazy schema scoping can avoid transferring.
//!
//! The current eager path serializes every canonical event in a pull page. The
//! lazy path keeps the same cursor/event framing, skips commits that do not
//! touch the demanded schema, and retains a matching commit as one atomic pack.
//! This benchmark measures the representative wire-size and JSON work for a
//! mixed repository; it is a protocol baseline rather than a synthetic backend
//! throughput claim.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lix::sync::{SyncCanonicalEvent, SyncRowMutation, SyncTransactionPack};
use serde_json::json;

fn events(event_count: usize, rows_per_event: usize) -> Vec<SyncCanonicalEvent> {
    (0..event_count)
        .map(|event| SyncCanonicalEvent {
            cursor: event as u64 + 1,
            canonical_commit_id: format!("commit-{event}"),
            parent_commit_ids: Vec::new(),
            pack_fingerprint: String::new(),
            pack: SyncTransactionPack {
                operation_id: format!("server:{event}"),
                branch_id: "main".to_owned(),
                base_server_commit_id: "server-head".to_owned(),
                local_commit_id: format!("commit-{event}"),
                parent_commit_ids: Vec::new(),
                rows: (0..rows_per_event)
                    .map(|row| {
                        let schema = if event % 10 == 0 && row == 0 {
                            "needed_schema"
                        } else {
                            "unrelated_schema"
                        };
                        SyncRowMutation {
                            schema_key: schema.to_owned(),
                            file_id: None,
                            row_pk: json!([format!("{event}-{row}")]),
                            snapshot: Some(json!({
                                "id": format!("{event}-{row}"),
                                "payload": "lazy-sync-profile-row"
                            })),
                            metadata: None,
                            global: false,
                            untracked: false,
                        }
                    })
                    .collect(),
                files: Vec::new(),
            },
        })
        .collect()
}

fn filtered(events: &[SyncCanonicalEvent]) -> Vec<SyncCanonicalEvent> {
    events
        .iter()
        .cloned()
        .map(|mut event| {
            let touches_scope = event
                .pack
                .rows
                .iter()
                .any(|row| row.schema_key == "needed_schema");
            if !touches_scope {
                event.pack.rows.clear();
                event.pack.files.clear();
            }
            event
        })
        .collect()
}

fn sync_lazy_profile(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_lazy_profile");
    for (event_count, rows_per_event) in [(16, 64), (128, 64), (512, 128)] {
        let full = events(event_count, rows_per_event);
        let scoped = filtered(&full);
        let full_bytes = serde_json::to_vec(&full).expect("full event fixture serializes");
        let scoped_bytes = serde_json::to_vec(&scoped).expect("scoped event fixture serializes");
        let reduction = 100.0 * (1.0 - scoped_bytes.len() as f64 / full_bytes.len() as f64);
        eprintln!(
            "sync_lazy_profile events={event_count} rows_per_event={rows_per_event} full_bytes={} scoped_bytes={} reduction_pct={reduction:.1}",
            full_bytes.len(),
            scoped_bytes.len()
        );
        group.throughput(Throughput::Bytes(full_bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("serialize_full", format!("{event_count}x{rows_per_event}")),
            &full,
            |bench, input| {
                bench.iter(|| {
                    black_box(serde_json::to_vec(black_box(input)).expect("full serializes"));
                });
            },
        );
        group.throughput(Throughput::Bytes(scoped_bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::new(
                "serialize_scoped",
                format!("{event_count}x{rows_per_event}"),
            ),
            &scoped,
            |bench, input| {
                bench.iter(|| {
                    black_box(serde_json::to_vec(black_box(input)).expect("scoped serializes"));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, sync_lazy_profile);
criterion_main!(benches);
