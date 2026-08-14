use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::Bytes;
use lix::storage::{
    GetManyRequest, GetOptions, Key, PutBatch, PutEntry, ReadOptions, Storage, StorageRead,
    StorageSpace, StorageWrite, StoredValue, WriteOptions,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

const WARMUPS: usize = 3;
const SAMPLES: usize = 20;
const INNER: usize = 100;
const HISTORY: usize = 1_000;
const CURRENT_SPACE: StorageSpace =
    StorageSpace::benchmark_mutable(0x7f10_0001, "experiment.commit_pack.current");
const PACK_SPACE: StorageSpace =
    StorageSpace::benchmark_mutable(0x7f10_0002, "experiment.commit_pack.pack");

#[derive(Clone, Copy)]
struct Shape {
    name: &'static str,
    envelope: usize,
    page: usize,
    pack: usize,
}

const SHAPES: &[Shape] = &[
    Shape {
        name: "introduced_d1_p128",
        envelope: 361,
        page: 201,
        pack: 586,
    },
    Shape {
        name: "introduced_d10_p128",
        envelope: 361,
        page: 1_407,
        pack: 1_792,
    },
    Shape {
        name: "introduced_d100_p128",
        envelope: 361,
        page: 13_137,
        pack: 13_522,
    },
    Shape {
        name: "selected_d100",
        envelope: 361,
        page: 389,
        pack: 774,
    },
];

fn bytes(length: usize, salt: u8) -> Bytes {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64 ^ u64::from(salt);
    Bytes::from(
        (0..length)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>(),
    )
}

fn key(prefix: u8, ordinal: usize) -> Key {
    let mut value = Vec::with_capacity(9);
    value.push(prefix);
    value.extend_from_slice(&(ordinal as u64).to_be_bytes());
    Key(Bytes::from(value))
}

fn stats(mut samples: Vec<Duration>, iterations: usize) -> (u128, u128) {
    samples.sort_unstable();
    (
        samples[samples.len() / 2].as_nanos() / iterations as u128,
        samples[(samples.len() * 95).div_ceil(100) - 1].as_nanos() / iterations as u128,
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    for shape in SHAPES {
        let rocks = tempfile::tempdir().expect("rocks tempdir");
        run_backend(
            "rocksdb",
            RocksDB::open(rocks.path()).expect("open RocksDB"),
            None,
            *shape,
        )
        .await;

        let slate = tempfile::tempdir().expect("slate tempdir");
        let counters = SlateDBIoCounters::default();
        run_backend(
            "slatedb",
            SlateDB::open_with_io_counters(slate.path(), counters.clone()).expect("open SlateDB"),
            Some(counters),
            *shape,
        )
        .await;
    }
}

async fn run_backend<S>(
    backend: &str,
    storage: S,
    counters: Option<SlateDBIoCounters>,
    shape: Shape,
) where
    S: Storage,
{
    let envelope = bytes(shape.envelope, 1);
    let page = bytes(shape.page, 2);
    let pack = bytes(shape.pack, 3);
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin seed");
    let mut current = Vec::with_capacity(HISTORY * 2);
    let mut packed = Vec::with_capacity(HISTORY);
    for ordinal in 0..HISTORY {
        current.push(PutEntry {
            key: key(0, ordinal),
            value: StoredValue {
                bytes: envelope.clone(),
            },
        });
        current.push(PutEntry {
            key: key(1, ordinal),
            value: StoredValue {
                bytes: page.clone(),
            },
        });
        packed.push(PutEntry {
            key: key(2, ordinal),
            value: StoredValue {
                bytes: pack.clone(),
            },
        });
    }
    write
        .put_many(CURRENT_SPACE, PutBatch { entries: current })
        .await
        .expect("seed current geometry");
    write
        .put_many(PACK_SPACE, PutBatch { entries: packed })
        .await
        .expect("seed pack geometry");
    write.commit().await.expect("commit seed");

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin retained read");
    let one_envelope = [key(0, 0)];
    let one_page = [key(1, 0)];
    let one_pack = [key(2, 0)];
    let history_envelopes = (0..HISTORY).map(|i| key(0, i)).collect::<Vec<_>>();
    let history_pages = (0..HISTORY).map(|i| key(1, i)).collect::<Vec<_>>();
    let history_packs = (0..HISTORY).map(|i| key(2, i)).collect::<Vec<_>>();

    for operation in [
        "topology_one",
        "closure_one",
        "topology_h1000",
        "closure_h1000",
    ] {
        for layout in ["current", "pack"] {
            let inner = if operation.ends_with("h1000") {
                1
            } else {
                INNER
            };
            for _ in 0..WARMUPS {
                execute(
                    &read,
                    operation,
                    layout,
                    &one_envelope,
                    &one_page,
                    &one_pack,
                    &history_envelopes,
                    &history_pages,
                    &history_packs,
                )
                .await;
            }
            let before = counters.as_ref().map(SlateDBIoCounters::snapshot);
            let mut samples = Vec::with_capacity(SAMPLES);
            for _ in 0..SAMPLES {
                let started = Instant::now();
                for _ in 0..inner {
                    execute(
                        &read,
                        operation,
                        layout,
                        &one_envelope,
                        &one_page,
                        &one_pack,
                        &history_envelopes,
                        &history_pages,
                        &history_packs,
                    )
                    .await;
                }
                samples.push(started.elapsed());
            }
            let after = counters.as_ref().map(SlateDBIoCounters::snapshot);
            let (p50, p95) = stats(samples, inner);
            let (physical_calls, physical_bytes) = match (before, after) {
                (Some(before), Some(after)) => {
                    let delta = after.saturating_sub(before);
                    (delta.read_objects, delta.read_bytes)
                }
                _ => (0, 0),
            };
            println!(
                "commit_pack_20,backend={backend},shape={},operation={operation},layout={layout},p50_ns={p50},p95_ns={p95},logical_iterations={},physical_calls={physical_calls},physical_bytes={physical_bytes}",
                shape.name,
                SAMPLES * inner,
            );
        }
    }
}

#[expect(clippy::too_many_arguments)]
async fn execute<R: StorageRead>(
    read: &R,
    operation: &str,
    layout: &str,
    one_envelope: &[Key],
    one_page: &[Key],
    one_pack: &[Key],
    history_envelopes: &[Key],
    history_pages: &[Key],
    history_packs: &[Key],
) {
    let request = |space, keys| GetManyRequest {
        space,
        keys,
        opts: GetOptions::default(),
    };
    match (operation, layout) {
        ("topology_one", "current") => {
            black_box(
                read.get_many(&[request(CURRENT_SPACE, one_envelope)])
                    .await
                    .unwrap(),
            );
        }
        ("topology_one", "pack") => {
            black_box(
                read.get_many(&[request(PACK_SPACE, one_pack)])
                    .await
                    .unwrap(),
            );
        }
        ("closure_one", "current") => {
            black_box(
                read.get_many(&[request(CURRENT_SPACE, one_envelope)])
                    .await
                    .unwrap(),
            );
            black_box(
                read.get_many(&[request(CURRENT_SPACE, one_page)])
                    .await
                    .unwrap(),
            );
        }
        ("closure_one", "pack") => {
            black_box(
                read.get_many(&[request(PACK_SPACE, one_pack)])
                    .await
                    .unwrap(),
            );
        }
        ("topology_h1000", "current") => {
            black_box(
                read.get_many(&[request(CURRENT_SPACE, history_envelopes)])
                    .await
                    .unwrap(),
            );
        }
        ("topology_h1000", "pack") => {
            black_box(
                read.get_many(&[request(PACK_SPACE, history_packs)])
                    .await
                    .unwrap(),
            );
        }
        ("closure_h1000", "current") => {
            black_box(
                read.get_many(&[request(CURRENT_SPACE, history_envelopes)])
                    .await
                    .unwrap(),
            );
            black_box(
                read.get_many(&[request(CURRENT_SPACE, history_pages)])
                    .await
                    .unwrap(),
            );
        }
        ("closure_h1000", "pack") => {
            black_box(
                read.get_many(&[request(PACK_SPACE, history_packs)])
                    .await
                    .unwrap(),
            );
        }
        _ => unreachable!(),
    }
}
