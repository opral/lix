//! Reproducible native-backend profile for repository copy-and-activate.
//!
//! `LIX_MIGRATION_PROFILE_MIB=256 cargo test -p lix-storage-rocksdb \
//!   --features storage-benches --test migration_profile --release -- --ignored --nocapture`

use std::{fs, ops::Bound, path::Path, time::Instant};

use bytes::Bytes;
use lix::storage::{
    BeginScanOptions, CoreProjection, Key, KeyRange, MAX_SCAN_PAGE_ROWS, Memory, ProjectedValue,
    PutBatch, PutEntry, Storage, StorageRead, StorageSpace, StorageWrite, StoredValue,
    WriteOptions,
};
use lix::{
    open_lix, registered_spaces,
    storage_adapter::{StorageAdapter, StorageAdapterRead as _},
};
use lix_storage_rocksdb::RocksDB;

const V75_RELEASED_SNAPSHOT: &[u8] =
    include_bytes!("../../lix/tests/fixtures/v75_released_repository.snapshot");
const ENTRY_BYTES: usize = 64 * 1024;

#[tokio::test]
#[ignore = "manual repository migration capacity profile"]
async fn profile_released_v75_copy_and_activate_on_rocksdb() {
    let payload_mib = std::env::var("LIX_MIGRATION_PROFILE_MIB")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64);
    assert!(payload_mib > 0, "profile payload must be larger than zero");
    let source = Memory::from_snapshot(V75_RELEASED_SNAPSHOT).expect("decode released v75");
    let (expected_entries, expected_value) = seed_payload(&source, payload_mib).await;

    let directory = tempfile::tempdir().expect("create profile directory");
    let database_path = directory.path().join("repository.rocksdb");
    let storage = RocksDB::open(&database_path).expect("open RocksDB profile repository");
    for &space in registered_spaces::ALL_STORAGE_SPACES {
        copy_space(&source, &storage, space).await;
    }
    storage.flush().expect("flush pre-migration repository");
    let before_bytes = directory_bytes(&database_path);
    drop(storage);
    let storage = RocksDB::open(&database_path).expect("cold-reopen profile repository");

    let started = Instant::now();
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("auto-upgrade released v75 repository");
    let open_ms = started.elapsed().as_secs_f64() * 1_000.0;
    assert_eq!(lix.open_report().migration.unwrap().from_format, 75);
    verify_seeded_payload(&lix.storage_adapter(), expected_entries, &expected_value).await;
    lix.close().await.expect("close profiled repository");
    storage.flush().expect("flush migrated repository");
    let after_bytes = directory_bytes(&database_path);

    println!(
        "migration_profile backend=rocksdb payload_mib={payload_mib} before_bytes={before_bytes} \
         after_bytes={after_bytes} retained_storage_ratio={:.3} open_ms={open_ms:.3} \
         logical_mib_per_s={:.3}",
        after_bytes as f64 / before_bytes as f64,
        payload_mib as f64 / (open_ms / 1_000.0),
    );
}

async fn seed_payload(storage: &Memory, payload_mib: usize) -> (usize, Bytes) {
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
    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            batch_capacity_hint_bytes: payload_mib.saturating_mul(1024 * 1024),
            ..WriteOptions::default()
        })
        .await
        .expect("begin profile seed write");
    write
        .put_many(
            registered_spaces::JSON_SPACE,
            PutBatch {
                entries: (0..entry_count)
                    .map(|index| PutEntry {
                        key: Key(Bytes::from(format!("profile-{index:08}"))),
                        value: StoredValue {
                            bytes: value.clone(),
                        },
                    })
                    .collect(),
            },
        )
        .await
        .expect("seed profile payload");
    write.commit().await.expect("commit profile payload");
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

async fn copy_space(source: &Memory, target: &RocksDB, space: StorageSpace) {
    let read = source
        .begin_read(Default::default())
        .await
        .expect("begin fixture read");
    let mut cursor = read
        .begin_scan(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions {
                projection: CoreProjection::FullValue,
                ..BeginScanOptions::default()
            },
        )
        .await
        .expect("begin fixture scan");
    loop {
        let (page, has_more) = cursor
            .next_page(MAX_SCAN_PAGE_ROWS)
            .await
            .expect("scan fixture page")
            .into_parts();
        if !page.is_empty() {
            let entries = page
                .into_iter()
                .map(|entry| PutEntry {
                    key: entry.key,
                    value: StoredValue {
                        bytes: match entry.value {
                            ProjectedValue::FullValue(value) => value,
                            ProjectedValue::KeyOnly => unreachable!("requested full values"),
                        },
                    },
                })
                .collect();
            let mut write = target
                .begin_write(WriteOptions {
                    await_durable: true,
                    ..WriteOptions::default()
                })
                .await
                .expect("begin RocksDB fixture write");
            write
                .put_many(space, PutBatch { entries })
                .await
                .expect("copy fixture page");
            write.commit().await.expect("commit fixture page");
        }
        if !has_more {
            break;
        }
    }
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
