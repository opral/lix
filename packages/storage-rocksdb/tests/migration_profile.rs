//! Reproducible native-backend profile for snapshot copy-and-activate.
//!
//! `LIX_MIGRATION_PROFILE_MIB=256 cargo test -p lix-storage-rocksdb \
//!   --features storage-benches --test migration_profile --release -- --ignored --nocapture`

use std::{fs, ops::Bound, path::Path, time::Instant};

use bytes::Bytes;
use futures_lite::io::Cursor;
use lix::storage::{
    BeginScanOptions, CoreProjection, Key, KeyRange, ProjectedValue, Storage, StoredValue,
    WriteOptions,
};
use lix::{
    open_lix, registered_spaces,
    storage_adapter::{StorageAdapter, StorageAdapterRead as _},
};
use lix_storage_rocksdb::RocksDB;

const V75_RELEASED_SNAPSHOT: &[u8] =
    include_bytes!("../../lix/tests/fixtures/v75_released_repository.lixsnap");
const ENTRY_BYTES: usize = 64 * 1024;

#[tokio::test]
#[ignore = "manual repository migration capacity profile"]
async fn profile_snapshot_copy_and_activate_on_rocksdb() {
    let payload_mib = std::env::var("LIX_MIGRATION_PROFILE_MIB")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64);
    assert!(payload_mib > 0, "profile payload must be larger than zero");
    let source = open_lix()
        .from_snapshot(Cursor::new(V75_RELEASED_SNAPSHOT))
        .await
        .expect("open and upgrade released fixture");
    let (expected_entries, expected_value) =
        seed_payload(&source.storage_adapter(), payload_mib).await;
    let mut snapshot = Vec::new();
    source
        .export_snapshot()
        .write_to(&mut snapshot)
        .await
        .expect("export profile snapshot");
    source.close().await.expect("close profile source");
    let snapshot_bytes = snapshot.len();

    let directory = tempfile::tempdir().expect("create profile directory");
    let database_path = directory.path().join("lix.rocksdb");
    let storage = RocksDB::open(&database_path).expect("open RocksDB profile repository");

    let started = Instant::now();
    let lix = open_lix()
        .with_storage(storage.clone())
        .from_snapshot(Cursor::new(snapshot))
        .await
        .expect("restore profile snapshot");
    let open_ms = started.elapsed().as_secs_f64() * 1_000.0;
    assert_eq!(lix.open_report().migration, None);
    verify_seeded_payload(&lix.storage_adapter(), expected_entries, &expected_value).await;
    lix.close().await.expect("close profiled repository");
    storage.flush().expect("flush migrated repository");
    let after_bytes = directory_bytes(&database_path);

    println!(
        "snapshot_profile backend=rocksdb payload_mib={payload_mib} snapshot_bytes={snapshot_bytes} \
         after_bytes={after_bytes} storage_to_snapshot_ratio={:.3} open_ms={open_ms:.3} \
         logical_mib_per_s={:.3}",
        after_bytes as f64 / snapshot_bytes as f64,
        payload_mib as f64 / (open_ms / 1_000.0),
    );
}

async fn seed_payload<S>(storage: &StorageAdapter<S>, payload_mib: usize) -> (usize, Bytes)
where
    S: Storage,
{
    let entry_count = payload_mib.saturating_mul(1024 * 1024) / ENTRY_BYTES;
    let mut state = 0x243f_6a88_85a3_08d3_u64;
    let mut payload = vec![0_u8; ENTRY_BYTES];
    for chunk in payload.chunks_exact_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    let value = Bytes::from(payload);
    let mut writes = storage.new_write_set();
    for index in 0..entry_count {
        writes.put(
            registered_spaces::JSON_SPACE,
            Key(Bytes::from(format!("profile-{index:08}"))),
            StoredValue {
                bytes: value.clone(),
            },
        );
    }
    storage
        .commit_write_set(
            writes,
            WriteOptions {
                await_durable: true,
                batch_capacity_hint_bytes: payload_mib.saturating_mul(1024 * 1024),
                ..WriteOptions::default()
            },
        )
        .await
        .expect("commit profile payload");
    (entry_count, value)
}

async fn verify_seeded_payload<S>(
    storage: &StorageAdapter<S>,
    expected_entries: usize,
    expected_value: &Bytes,
) where
    S: Storage,
{
    let read = storage
        .begin_read(Default::default())
        .await
        .expect("begin migrated payload read");
    let mut cursor = read
        .begin_scan(
            registered_spaces::JSON_SPACE,
            KeyRange {
                lower: Bound::Included(Key(Bytes::from_static(b"profile-"))),
                upper: Bound::Excluded(Key(Bytes::from_static(b"profile."))),
            },
            BeginScanOptions {
                projection: CoreProjection::FullValue,
                ..BeginScanOptions::default()
            },
        )
        .await
        .expect("begin migrated payload scan");
    let mut actual_entries = 0;
    while let Some(entries) = cursor
        .next_chunk()
        .await
        .expect("scan migrated payload chunk")
    {
        for entry in entries {
            assert_eq!(
                entry.value,
                ProjectedValue::FullValue(expected_value.clone()),
                "migrated profile payload bytes changed for {:?}",
                entry.key
            );
            actual_entries += 1;
        }
    }
    assert_eq!(
        actual_entries, expected_entries,
        "migrated profile payload entry count changed"
    );
}

fn directory_bytes(path: &Path) -> u64 {
    fs::read_dir(path)
        .expect("read RocksDB directory")
        .map(|entry| {
            let entry = entry.expect("read RocksDB entry");
            let metadata = entry.metadata().expect("read RocksDB metadata");
            if metadata.is_dir() {
                directory_bytes(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
}
