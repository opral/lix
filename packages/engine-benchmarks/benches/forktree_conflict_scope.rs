//! Test-only model for separating ForkTree's global publication order from
//! owner-local conflict detection. This does not wire or alter production.

use std::alloc::GlobalAlloc;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use futures_util::future::join_all;
use lix::storage::{
    CoreProjection, GetManyRequest, GetOptions, Key, Precondition, ProjectedValue, PutBatch,
    PutEntry, ReadOptions, Storage, StorageError, StorageRead, StorageWrite, StoredValue,
    ValueSemantics, WriteOptions,
};
use lix::storage_bench::synthetic_space_for_bench;
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;

static PROFILE_ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static PROFILE_ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static PROFILE_ALLOCATION_ENABLED: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && PROFILE_ALLOCATION_ENABLED.load(Ordering::Relaxed) {
            PROFILE_ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            PROFILE_ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(
        &self,
        pointer: *mut u8,
        layout: std::alloc::Layout,
        new_size: usize,
    ) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, new_size) };
        if !replacement.is_null()
            && new_size >= layout.size()
            && PROFILE_ALLOCATION_ENABLED.load(Ordering::Relaxed)
        {
            PROFILE_ALLOCATED_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            PROFILE_ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

const SELECTOR_SPACE: lix::storage::StorageSpace =
    synthetic_space_for_bench(70, ValueSemantics::Mutable);
const OBJECT_SPACE: lix::storage::StorageSpace =
    synthetic_space_for_bench(71, ValueSemantics::Immutable);
const GLOBAL_KEY: &[u8] = b"global";
const PREP_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Current,
    Scoped,
    Handoff,
}

impl Mode {
    fn parse(value: &str) -> Self {
        match value {
            "current" => Self::Current,
            "scoped" => Self::Scoped,
            "handoff" => Self::Handoff,
            other => panic!("unknown mode '{other}'"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Scoped => "scoped",
            Self::Handoff => "handoff",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OwnerKind {
    Branch,
    Catalog,
    Upload,
}

impl OwnerKind {
    fn parse(value: &str) -> Self {
        match value {
            "branch" => Self::Branch,
            "catalog" => Self::Catalog,
            "upload" => Self::Upload,
            other => panic!("unknown owner kind '{other}'"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Branch => "branch",
            Self::Catalog => "catalog",
            Self::Upload => "upload",
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::Branch => b'b',
            Self::Catalog => b'c',
            Self::Upload => b'u',
        }
    }
}

#[derive(Clone, Debug, Default)]
struct LogicalIo {
    get_calls: u64,
    get_keys: u64,
    get_bytes: u64,
    commit_attempts: u64,
    put_entries: u64,
    put_bytes: u64,
}

impl std::ops::AddAssign for LogicalIo {
    fn add_assign(&mut self, rhs: Self) {
        self.get_calls += rhs.get_calls;
        self.get_keys += rhs.get_keys;
        self.get_bytes += rhs.get_bytes;
        self.commit_attempts += rhs.commit_attempts;
        self.put_entries += rhs.put_entries;
        self.put_bytes += rhs.put_bytes;
    }
}

#[derive(Clone, Debug)]
struct Snapshot {
    global: Bytes,
    owner: Option<Bytes>,
}

#[derive(Clone, Debug)]
struct Prepared {
    owner_key: Key,
    expected_owner: Option<Bytes>,
    payload: Bytes,
    object_key: Key,
}

#[derive(Clone, Debug, Default)]
struct WriterResult {
    success: bool,
    stale: bool,
    global_retries: u64,
    prepare_calls: u64,
    io: LogicalIo,
}

#[derive(Clone, Debug, Default)]
struct CohortResult {
    successes: u64,
    stale: u64,
    global_retries: u64,
    prepare_calls: u64,
    io: LogicalIo,
}

fn key(bytes: impl Into<Bytes>) -> Key {
    Key(bytes.into())
}

fn global_value(epoch: u64, gc_watermark: u64) -> Bytes {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&epoch.to_be_bytes());
    bytes.extend_from_slice(&gc_watermark.to_be_bytes());
    Bytes::from(bytes)
}

fn decode_global(value: &Bytes) -> (u64, u64) {
    assert_eq!(value.len(), 16, "global authority must be exactly 16 bytes");
    let epoch = u64::from_be_bytes(value[..8].try_into().expect("epoch bytes"));
    let watermark = u64::from_be_bytes(value[8..].try_into().expect("watermark bytes"));
    (epoch, watermark)
}

fn owner_key(kind: OwnerKind, cohort: u64, owner: usize) -> Key {
    let mut bytes = Vec::with_capacity(17);
    bytes.push(kind.tag());
    bytes.extend_from_slice(&cohort.to_be_bytes());
    bytes.extend_from_slice(&(owner as u64).to_be_bytes());
    key(Bytes::from(bytes))
}

fn prepare(kind: OwnerKind, cohort: u64, owner: usize, expected_owner: Option<Bytes>) -> Prepared {
    let mut payload = vec![kind.tag(); PREP_BYTES];
    payload[..8].copy_from_slice(&cohort.to_be_bytes());
    payload[8..16].copy_from_slice(&(owner as u64).to_be_bytes());
    let digest = blake3::hash(&payload);
    let object_key = key(Bytes::copy_from_slice(digest.as_bytes()));
    Prepared {
        owner_key: owner_key(kind, cohort, owner),
        expected_owner,
        payload: Bytes::from(payload),
        object_key,
    }
}

fn owner_value(previous: Option<&Bytes>, object_key: &Key) -> Bytes {
    let generation = previous.map_or(1, |bytes| {
        assert_eq!(bytes.len(), 40, "owner selector encoding");
        u64::from_be_bytes(bytes[..8].try_into().expect("owner generation")) + 1
    });
    let mut value = Vec::with_capacity(40);
    value.extend_from_slice(&generation.to_be_bytes());
    value.extend_from_slice(&object_key.0);
    Bytes::from(value)
}

async fn read_snapshot<S: Storage>(storage: &S, owner_key: &Key) -> (Snapshot, LogicalIo) {
    let keys = [key(Bytes::from_static(GLOBAL_KEY)), owner_key.clone()];
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("open coherent model read");
    let result = read
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("read coherent global/owner selector");
    assert_eq!(result.values.len(), 2);
    let values = result
        .values
        .into_iter()
        .map(|value| match value {
            Some(ProjectedValue::FullValue(bytes)) => Some(bytes),
            None => None,
            Some(ProjectedValue::KeyOnly) => unreachable!("full-value projection"),
        })
        .collect::<Vec<_>>();
    let global = values[0].clone().expect("seeded global selector");
    let owner = values[1].clone();
    let get_bytes = global.len() as u64 + owner.as_ref().map_or(0, |value| value.len() as u64);
    (
        Snapshot { global, owner },
        LogicalIo {
            get_calls: 1,
            get_keys: 2,
            get_bytes,
            ..LogicalIo::default()
        },
    )
}

async fn seed_global<S: Storage>(storage: &S) {
    let mut write = storage
        .begin_write(WriteOptions {
            preconditions: vec![Precondition::KeyAbsent {
                space: SELECTOR_SPACE,
                key: key(Bytes::from_static(GLOBAL_KEY)),
            }],
            ..WriteOptions::default()
        })
        .await
        .expect("begin global seed");
    write
        .put_many(
            SELECTOR_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: key(Bytes::from_static(GLOBAL_KEY)),
                    value: StoredValue {
                        bytes: global_value(1, 0),
                    },
                }],
            },
        )
        .await
        .expect("stage global seed");
    write.commit().await.expect("commit global seed");
}

async fn publish_once<S: Storage>(
    storage: &S,
    snapshot: &Snapshot,
    prepared: &Prepared,
) -> Result<LogicalIo, StorageError> {
    let (epoch, watermark) = decode_global(&snapshot.global);
    let next_global = global_value(
        epoch.checked_add(1).expect("global epoch overflow"),
        watermark,
    );
    let next_owner = owner_value(prepared.expected_owner.as_ref(), &prepared.object_key);
    let mut preconditions = vec![Precondition::KeyValueEquals {
        space: SELECTOR_SPACE,
        key: key(Bytes::from_static(GLOBAL_KEY)),
        expected: snapshot.global.clone(),
    }];
    preconditions.push(match &prepared.expected_owner {
        Some(expected) => Precondition::KeyValueEquals {
            space: SELECTOR_SPACE,
            key: prepared.owner_key.clone(),
            expected: expected.clone(),
        },
        None => Precondition::KeyAbsent {
            space: SELECTOR_SPACE,
            key: prepared.owner_key.clone(),
        },
    });
    let mut write = storage
        .begin_write(WriteOptions {
            preconditions,
            batch_capacity_hint_bytes: prepared.payload.len() + 128,
            ..WriteOptions::default()
        })
        .await?;
    write
        .put_many(
            OBJECT_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: prepared.object_key.clone(),
                    value: StoredValue {
                        bytes: prepared.payload.clone(),
                    },
                }],
            },
        )
        .await?;
    write
        .put_many(
            SELECTOR_SPACE,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: key(Bytes::from_static(GLOBAL_KEY)),
                        value: StoredValue {
                            bytes: next_global.clone(),
                        },
                    },
                    PutEntry {
                        key: prepared.owner_key.clone(),
                        value: StoredValue {
                            bytes: next_owner.clone(),
                        },
                    },
                ],
            },
        )
        .await?;
    let io = LogicalIo {
        commit_attempts: 1,
        put_entries: 3,
        put_bytes: (prepared.object_key.0.len()
            + prepared.payload.len()
            + GLOBAL_KEY.len()
            + next_global.len()
            + prepared.owner_key.0.len()
            + next_owner.len()) as u64,
        ..LogicalIo::default()
    };
    write.commit().await?;
    Ok(io)
}

async fn run_writer<S: Storage>(
    storage: S,
    mode: Mode,
    kind: OwnerKind,
    cohort: u64,
    owner: usize,
    initial: Snapshot,
) -> WriterResult {
    let mut result = WriterResult::default();
    let mut snapshot = initial.clone();
    let mut prepared = prepare(kind, cohort, owner, initial.owner.clone());
    result.prepare_calls = 1;
    loop {
        if mode == Mode::Current && result.global_retries > 0 {
            prepared = prepare(kind, cohort, owner, initial.owner.clone());
            result.prepare_calls += 1;
        }
        let attempted_bytes = (prepared.payload.len() + prepared.object_key.0.len() + 128) as u64;
        result.io.commit_attempts += 1;
        result.io.put_entries += 3;
        result.io.put_bytes += attempted_bytes;
        match publish_once(&storage, &snapshot, &prepared).await {
            Ok(_) => {
                result.success = true;
                return result;
            }
            Err(StorageError::PreconditionFailed(failures)) => {
                if failures.iter().any(|failure| failure.index == 1) {
                    result.stale = true;
                    return result;
                }
                assert!(
                    failures.iter().any(|failure| failure.index == 0),
                    "only global or owner preconditions exist"
                );
                result.global_retries += 1;
                let (latest, io) = read_snapshot(&storage, &prepared.owner_key).await;
                result.io += io;
                if latest.owner != initial.owner {
                    result.stale = true;
                    return result;
                }
                snapshot = latest;
                tokio::task::yield_now().await;
            }
            Err(StorageError::WriteConflict) => {
                result.global_retries += 1;
                let (latest, io) = read_snapshot(&storage, &prepared.owner_key).await;
                result.io += io;
                if latest.owner != initial.owner {
                    result.stale = true;
                    return result;
                }
                snapshot = latest;
                tokio::task::yield_now().await;
            }
            Err(error) => panic!("publication failed unexpectedly: {error}"),
        }
    }
}

async fn run_cohort<S: Storage + Clone>(
    storage: &S,
    mode: Mode,
    kind: OwnerKind,
    cohort: u64,
    owners: usize,
    same_owner: bool,
) -> CohortResult {
    if mode == Mode::Handoff {
        return run_handoff_cohort(storage, kind, cohort, owners, same_owner).await;
    }
    let mut initial = Vec::with_capacity(owners);
    for writer in 0..owners {
        let owner = if same_owner { 0 } else { writer };
        let (snapshot, _) = read_snapshot(storage, &owner_key(kind, cohort, owner)).await;
        initial.push((owner, snapshot));
    }
    let results =
        join_all(initial.into_iter().map(|(owner, snapshot)| {
            run_writer(storage.clone(), mode, kind, cohort, owner, snapshot)
        }))
        .await;
    let mut cohort_result = CohortResult::default();
    for result in results {
        cohort_result.successes += u64::from(result.success);
        cohort_result.stale += u64::from(result.stale);
        cohort_result.global_retries += result.global_retries;
        cohort_result.prepare_calls += result.prepare_calls;
        cohort_result.io += result.io;
    }
    cohort_result
}

/// Models one process-local reservation queue over the sole durable global
/// selector. A successful atomic global+owner publication hands its exact
/// resulting global bytes to the next waiter, avoiding a doomed CAS and
/// reread. The handoff is disposable scheduling state: an external writer or
/// crash merely forces the normal global CAS/reread path.
async fn run_handoff_cohort<S: Storage + Clone>(
    storage: &S,
    kind: OwnerKind,
    cohort: u64,
    owners: usize,
    same_owner: bool,
) -> CohortResult {
    let mut initial = Vec::with_capacity(owners);
    for writer in 0..owners {
        let owner = if same_owner { 0 } else { writer };
        let (snapshot, _) = read_snapshot(storage, &owner_key(kind, cohort, owner)).await;
        initial.push((owner, snapshot));
    }
    let mut handoff_global = initial
        .first()
        .expect("handoff cohort must not be empty")
        .1
        .global
        .clone();
    assert!(
        initial
            .iter()
            .all(|(_, snapshot)| snapshot.global == handoff_global),
        "same-instant cohort must reserve from one global version"
    );

    let mut cohort_result = CohortResult::default();
    for (owner, initial_snapshot) in initial {
        let prepared = prepare(kind, cohort, owner, initial_snapshot.owner.clone());
        cohort_result.prepare_calls += 1;
        let mut snapshot = Snapshot {
            global: handoff_global.clone(),
            owner: initial_snapshot.owner.clone(),
        };
        loop {
            cohort_result.io.commit_attempts += 1;
            cohort_result.io.put_entries += 3;
            cohort_result.io.put_bytes +=
                (prepared.payload.len() + prepared.object_key.0.len() + 128) as u64;
            match publish_once(storage, &snapshot, &prepared).await {
                Ok(_) => {
                    cohort_result.successes += 1;
                    let (epoch, watermark) = decode_global(&snapshot.global);
                    handoff_global = global_value(
                        epoch.checked_add(1).expect("global epoch overflow"),
                        watermark,
                    );
                    break;
                }
                Err(StorageError::PreconditionFailed(failures)) => {
                    if failures.iter().any(|failure| failure.index == 1) {
                        cohort_result.stale += 1;
                        break;
                    }
                    assert!(
                        failures.iter().any(|failure| failure.index == 0),
                        "only global or owner preconditions exist"
                    );
                    cohort_result.global_retries += 1;
                    let (latest, io) = read_snapshot(storage, &prepared.owner_key).await;
                    cohort_result.io += io;
                    if latest.owner != initial_snapshot.owner {
                        cohort_result.stale += 1;
                        break;
                    }
                    handoff_global = latest.global.clone();
                    snapshot = latest;
                }
                Err(StorageError::WriteConflict) => {
                    cohort_result.global_retries += 1;
                    let (latest, io) = read_snapshot(storage, &prepared.owner_key).await;
                    cohort_result.io += io;
                    if latest.owner != initial_snapshot.owner {
                        cohort_result.stale += 1;
                        break;
                    }
                    handoff_global = latest.global.clone();
                    snapshot = latest;
                }
                Err(error) => panic!("handoff publication failed unexpectedly: {error}"),
            }
        }
    }
    cohort_result
}

async fn advance_gc<S: Storage>(storage: &S, expected_global: Bytes) -> Result<(), StorageError> {
    let (epoch, watermark) = decode_global(&expected_global);
    let next = global_value(epoch + 1, watermark.max(epoch));
    let mut write = storage
        .begin_write(WriteOptions {
            preconditions: vec![Precondition::KeyValueEquals {
                space: SELECTOR_SPACE,
                key: key(Bytes::from_static(GLOBAL_KEY)),
                expected: expected_global,
            }],
            ..WriteOptions::default()
        })
        .await?;
    write
        .put_many(
            SELECTOR_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: key(Bytes::from_static(GLOBAL_KEY)),
                    value: StoredValue { bytes: next },
                }],
            },
        )
        .await?;
    write.commit().await?;
    Ok(())
}

async fn run_oracle<S: Storage + Clone>(backend: &str, storage: &S) {
    seed_global(storage).await;

    for (offset, kind) in [OwnerKind::Branch, OwnerKind::Catalog, OwnerKind::Upload]
        .into_iter()
        .enumerate()
    {
        let distinct =
            run_cohort(storage, Mode::Handoff, kind, 10 + offset as u64, 10, false).await;
        assert_eq!(distinct.successes, 10, "unrelated owners must all publish");
        assert_eq!(distinct.stale, 0, "unrelated owners are not stale");

        let same = run_cohort(storage, Mode::Handoff, kind, 20 + offset as u64, 10, true).await;
        assert_eq!(same.successes, 1, "one same-owner writer wins");
        assert_eq!(same.stale, 9, "same-owner stale writers reject");
    }

    // GC first: the prepared logical publication may retry only the global
    // authority because its owner selector remains unchanged.
    let pub_key = owner_key(OwnerKind::Branch, 30, 0);
    let (prepared_snapshot, _) = read_snapshot(storage, &pub_key).await;
    advance_gc(storage, prepared_snapshot.global.clone())
        .await
        .expect("GC-first global fence");
    let gc_first = run_writer(
        storage.clone(),
        Mode::Handoff,
        OwnerKind::Branch,
        30,
        0,
        prepared_snapshot,
    )
    .await;
    assert!(gc_first.success && gc_first.global_retries >= 1);

    // Publication first: a GC plan bound to the old global authority rejects.
    let gc_key = owner_key(OwnerKind::Catalog, 31, 0);
    let (gc_snapshot, _) = read_snapshot(storage, &gc_key).await;
    let publication = run_writer(
        storage.clone(),
        Mode::Handoff,
        OwnerKind::Catalog,
        31,
        0,
        gc_snapshot.clone(),
    )
    .await;
    assert!(publication.success);
    assert!(matches!(
        advance_gc(storage, gc_snapshot.global).await,
        Err(StorageError::PreconditionFailed(_))
    ));

    // A reader pin and an open upload are independent owner selectors and
    // remain present across unrelated publication and GC fencing.
    let pins = run_cohort(storage, Mode::Handoff, OwnerKind::Branch, 40, 1, false).await;
    let uploads = run_cohort(storage, Mode::Handoff, OwnerKind::Upload, 41, 1, false).await;
    assert_eq!((pins.successes, uploads.successes), (1, 1));
    let pin_key = owner_key(OwnerKind::Branch, 40, 0);
    let upload_key = owner_key(OwnerKind::Upload, 41, 0);
    let (pin, _) = read_snapshot(storage, &pin_key).await;
    let (upload, _) = read_snapshot(storage, &upload_key).await;
    assert!(pin.owner.is_some() && upload.owner.is_some());
    advance_gc(storage, upload.global)
        .await
        .expect("GC uses exact latest global fence");
    let (pin_after, _) = read_snapshot(storage, &pin_key).await;
    let (upload_after, _) = read_snapshot(storage, &upload_key).await;
    assert_eq!(pin.owner, pin_after.owner);
    assert_eq!(upload.owner, upload_after.owner);

    // A local reservation has no durable row. Losing it before publication
    // changes neither global nor owner authority; a committed successor does.
    let crash_key = owner_key(OwnerKind::Branch, 42, 0);
    let (before_crash, _) = read_snapshot(storage, &crash_key).await;
    let _lost_preparation = prepare(OwnerKind::Branch, 42, 0, before_crash.owner.clone());
    let (after_crash, _) = read_snapshot(storage, &crash_key).await;
    assert_eq!(before_crash.global, after_crash.global);
    assert_eq!(before_crash.owner, after_crash.owner);
    let committed_after_crash =
        run_cohort(storage, Mode::Handoff, OwnerKind::Branch, 43, 1, false).await;
    assert_eq!(committed_after_crash.successes, 1);
    println!(
        "forktree_global_handoff_oracle,backend={backend},unrelated_branch_catalog_upload=pass,same_owner_successes=1,same_owner_stale=9,gc_first_publication_retry=pass,publication_first_stale_gc_reject=pass,reader_pin_preserved=pass,open_upload_preserved=pass,crash_before_commit_no_authority=pass,crash_after_commit_durable=pass,global_authorities=1"
    );
}

async fn assert_reopen<S: Storage>(storage: &S) {
    for (kind, cohort) in [
        (OwnerKind::Branch, 40),
        (OwnerKind::Upload, 41),
        (OwnerKind::Catalog, 31),
        (OwnerKind::Branch, 43),
    ] {
        let (snapshot, _) = read_snapshot(storage, &owner_key(kind, cohort, 0)).await;
        assert!(snapshot.owner.is_some(), "owner selector survives reopen");
    }
    let (lost_reservation, _) = read_snapshot(storage, &owner_key(OwnerKind::Branch, 42, 0)).await;
    assert!(
        lost_reservation.owner.is_none(),
        "crash-before-commit reservation must not become authority"
    );
    let (global, _) = read_snapshot(storage, &owner_key(OwnerKind::Catalog, 999, 0)).await;
    let (epoch, watermark) = decode_global(&global.global);
    assert!(epoch > 1 && watermark > 0 && watermark < epoch);
}

fn begin_profile() {
    PROFILE_ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_ENABLED.store(true, Ordering::Relaxed);
}

fn end_profile() -> (u64, u64) {
    PROFILE_ALLOCATION_ENABLED.store(false, Ordering::Relaxed);
    (
        PROFILE_ALLOCATED_BYTES.load(Ordering::Relaxed),
        PROFILE_ALLOCATION_CALLS.load(Ordering::Relaxed),
    )
}

fn process_resident_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").expect("read process statm");
    let resident_pages = statm
        .split_whitespace()
        .nth(1)
        .expect("resident pages")
        .parse::<u64>()
        .expect("parse resident pages");
    resident_pages * 4096
}

fn process_cpu_ticks() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("read process stat");
    let tail = stat.rsplit_once(')').expect("process stat command").1;
    let fields = tail.split_whitespace().collect::<Vec<_>>();
    fields[11].parse::<u64>().expect("user CPU ticks")
        + fields[12].parse::<u64>().expect("system CPU ticks")
}

fn directory_bytes(path: &Path) -> u64 {
    fn visit(path: &Path) -> u64 {
        let Ok(metadata) = std::fs::symlink_metadata(path) else {
            return 0;
        };
        if metadata.is_file() {
            return metadata.len();
        }
        if !metadata.is_dir() {
            return 0;
        }
        std::fs::read_dir(path).map_or(0, |entries| {
            entries.flatten().map(|entry| visit(&entry.path())).sum()
        })
    }
    visit(path)
}

fn print_result(
    backend: &str,
    mode: Mode,
    kind: OwnerKind,
    owners: usize,
    rounds: usize,
    cohort: &CohortResult,
    wall_us: f64,
    cpu_us: f64,
    allocated: u64,
    allocation_calls: u64,
    rss_before: u64,
    rss_after: u64,
    disk_before: u64,
    disk_after: u64,
    physical: SlateDBIoSnapshot,
) {
    let publications = cohort.successes as f64;
    println!(
        "forktree_conflict_scope,backend={backend},mode={},kind={},owners={owners},rounds={rounds},successes={},stale={},global_retries={},surfaced_false_conflicts={},prepare_calls={},prep_bytes={},wall_us_total={wall_us:.3},wall_us_per_publication={:.3},cpu_us_total={cpu_us:.3},cpu_us_per_publication={:.3},alloc_bytes={allocated},alloc_bytes_per_publication={:.1},alloc_calls={allocation_calls},rss_before={rss_before},rss_after={rss_after},get_calls={},get_keys={},get_bytes={},commit_attempts={},put_entries={},put_bytes={},disk_before={disk_before},disk_after={disk_after},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},slate_list_operations={},slate_deleted_objects={}",
        mode.name(),
        kind.name(),
        cohort.successes,
        cohort.stale,
        cohort.global_retries,
        if mode == Mode::Current {
            cohort.global_retries
        } else {
            0
        },
        cohort.prepare_calls,
        PREP_BYTES,
        wall_us / publications,
        cpu_us / publications,
        allocated as f64 / publications,
        cohort.io.get_calls,
        cohort.io.get_keys,
        cohort.io.get_bytes,
        cohort.io.commit_attempts,
        cohort.io.put_entries,
        cohort.io.put_bytes,
        physical.read_objects,
        physical.read_bytes,
        physical.write_objects,
        physical.write_bytes,
        physical.list_operations,
        physical.deleted_objects,
    );
}

async fn measure<S: Storage + Clone>(
    backend: &str,
    storage: &S,
    path: &Path,
    counters: Option<&SlateDBIoCounters>,
    mode: Mode,
    kind: OwnerKind,
    owners: usize,
    rounds: usize,
) {
    let physical_before =
        counters.map_or_else(SlateDBIoSnapshot::default, |value| value.snapshot());
    let disk_before = directory_bytes(path);
    let rss_before = process_resident_bytes();
    let cpu_before = process_cpu_ticks();
    begin_profile();
    let started = Instant::now();
    let mut cohort = CohortResult::default();
    for round in 0..rounds {
        let result = run_cohort(storage, mode, kind, 1000 + round as u64, owners, false).await;
        cohort.successes += result.successes;
        cohort.stale += result.stale;
        cohort.global_retries += result.global_retries;
        cohort.prepare_calls += result.prepare_calls;
        cohort.io += result.io;
    }
    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (allocated, allocation_calls) = end_profile();
    let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_before);
    let cpu_us = cpu_ticks as f64 * 10_000.0;
    let rss_after = process_resident_bytes();
    let disk_after = directory_bytes(path);
    let physical = counters.map_or_else(SlateDBIoSnapshot::default, |value| {
        value.snapshot().saturating_sub(physical_before)
    });
    assert_eq!(cohort.successes, (owners * rounds) as u64);
    assert_eq!(cohort.stale, 0);
    let (global, _) = read_snapshot(storage, &owner_key(kind, u64::MAX, 0)).await;
    let (epoch, _) = decode_global(&global.global);
    assert!(
        epoch > (owners * rounds) as u64,
        "one global version per publication"
    );
    print_result(
        backend,
        mode,
        kind,
        owners,
        rounds,
        &cohort,
        wall_us,
        cpu_us,
        allocated,
        allocation_calls,
        rss_before,
        rss_after,
        disk_before,
        disk_after,
        physical,
    );
}

async fn run_rocks(mode: Mode, kind: OwnerKind, owners: usize, rounds: usize) {
    let directory = tempfile::tempdir().expect("create RocksDB model directory");
    let path = directory.path().join("rocksdb");
    let storage = RocksDB::open(&path).expect("open RocksDB model");
    run_oracle("rocksdb", &storage).await;
    storage.flush().expect("flush RocksDB oracle");
    measure("rocksdb", &storage, &path, None, mode, kind, owners, rounds).await;
    storage.flush().expect("flush RocksDB measurement");
    println!(
        "forktree_conflict_scope_settled,backend=rocksdb,mode={},kind={},owners={owners},rounds={rounds},disk_bytes={}",
        mode.name(),
        kind.name(),
        directory_bytes(&path),
    );
    drop(storage);
    let reopened = RocksDB::open(&path).expect("reopen RocksDB model");
    assert_reopen(&reopened).await;
}

async fn run_slate(mode: Mode, kind: OwnerKind, owners: usize, rounds: usize) {
    let directory = tempfile::tempdir().expect("create SlateDB model directory");
    let path = directory.path().join("slatedb");
    let counters = SlateDBIoCounters::default();
    let storage =
        SlateDB::open_with_io_counters(&path, counters.clone()).expect("open SlateDB model");
    run_oracle("slatedb", &storage).await;
    storage.flush().await.expect("flush SlateDB oracle");
    measure(
        "slatedb",
        &storage,
        &path,
        Some(&counters),
        mode,
        kind,
        owners,
        rounds,
    )
    .await;
    storage.flush().await.expect("flush SlateDB measurement");
    println!(
        "forktree_conflict_scope_settled,backend=slatedb,mode={},kind={},owners={owners},rounds={rounds},disk_bytes={},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={}",
        mode.name(),
        kind.name(),
        directory_bytes(&path),
        counters.snapshot().read_objects,
        counters.snapshot().read_bytes,
        counters.snapshot().write_objects,
        counters.snapshot().write_bytes,
    );
    drop(storage);
    let reopened = SlateDB::open(&path).expect("reopen SlateDB model");
    assert_reopen(&reopened).await;
    reopened
        .flush()
        .await
        .expect("close reopened SlateDB model");
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let backend = args.get(1).map(String::as_str).unwrap_or("rocksdb");
    let mode = Mode::parse(args.get(2).map(String::as_str).unwrap_or("current"));
    let kind = OwnerKind::parse(args.get(3).map(String::as_str).unwrap_or("branch"));
    let owners = args
        .get(4)
        .map_or(1, |value| value.parse::<usize>().expect("owner count"));
    let rounds = args
        .get(5)
        .map_or(5, |value| value.parse::<usize>().expect("round count"));
    assert!(matches!(owners, 1 | 10 | 100));
    assert!(rounds > 0);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("create conflict-scope runtime")
        .block_on(async {
            match backend {
                "rocksdb" => run_rocks(mode, kind, owners, rounds).await,
                "slatedb" => run_slate(mode, kind, owners, rounds).await,
                other => panic!("unknown backend '{other}'"),
            }
        });
}
