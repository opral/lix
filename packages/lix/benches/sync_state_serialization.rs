//! Measures the serialization cost of the current durable sync outbox shape.
//!
//! `SyncClientState` intentionally remains private to the sync implementation,
//! so this probe mirrors its manifest and pending-pack fields while using the
//! public transaction-pack types. The manifest is intentionally small; pack
//! payloads are serialized independently and are not rewritten on cursor or
//! acknowledgement updates.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use lix::sync::{SyncRowMutation, SyncTransactionPack};
use serde::Serialize;
use serde_json::json;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SyncManifestProbe {
    version: u8,
    remote_id: String,
    branch_id: String,
    cursor: u64,
    server_commit_id: Option<String>,
    pending_operations: Vec<String>,
}

fn fixture(queue_len: usize) -> (SyncManifestProbe, Vec<SyncTransactionPack>) {
    let pending: Vec<SyncTransactionPack> = (0..queue_len)
        .map(|operation| SyncTransactionPack {
            operation_id: format!("client:{operation}"),
            branch_id: "main".to_owned(),
            base_server_commit_id: "server-head".to_owned(),
            local_commit_id: format!("local:{operation}"),
            parent_commit_ids: Vec::new(),
            rows: vec![SyncRowMutation {
                schema_key: "benchmark_row".to_owned(),
                file_id: Some("file-1".to_owned()),
                row_pk: json!([format!("row-{operation}")]),
                snapshot: Some(json!({
                    "id": format!("row-{operation}"),
                    "payload": "sync serialization fixture"
                })),
                metadata: None,
                global: false,
                untracked: false,
            }],
            files: Vec::new(),
        })
        .collect();

    let manifest = SyncManifestProbe {
        version: 2,
        remote_id: "https://example.test/repository".to_owned(),
        branch_id: "main".to_owned(),
        cursor: 42,
        server_commit_id: Some("server-head".to_owned()),
        pending_operations: pending
            .iter()
            .map(|pack| pack.operation_id.clone())
            .collect(),
    };
    (manifest, pending)
}

fn sync_state_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_state_serialization");
    for queue_len in [1, 8, 32, 128] {
        let (manifest, pending) = fixture(queue_len);
        let encoded_len = serde_json::to_vec(&manifest)
            .expect("sync fixture serializes")
            .len();
        group.bench_function(
            format!("manifest_pending_{queue_len}_bytes_{encoded_len}"),
            |b| {
                b.iter(|| {
                    let encoded =
                        serde_json::to_vec(black_box(&manifest)).expect("sync manifest serializes");
                    black_box(encoded);
                });
            },
        );
        let pack = pending.last().expect("sync fixture has a pack");
        let pack_encoded_len = serde_json::to_vec(pack)
            .expect("sync pack serializes")
            .len();
        group.bench_function(
            format!("pack_payload_{queue_len}_bytes_{pack_encoded_len}"),
            |b| {
                b.iter(|| {
                    let encoded =
                        serde_json::to_vec(black_box(pack)).expect("sync pack serializes");
                    black_box(encoded);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, sync_state_serialization);
criterion_main!(benches);
