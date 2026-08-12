#![allow(
    clippy::manual_async_fn,
    reason = "explicit future signatures mirror Storage traits and keep Send guarantees visible"
)]

use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::ops::{Bound, Range};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::FutureExt;
use futures_util::stream::{self, BoxStream, StreamExt, TryStreamExt};
use lix::storage::conformance::{StorageFactory, StorageFixture, StorageTestConfig};
use lix::storage::immutable::{
    ImmutableSegment, ImmutableSegmentWriter, ImmutableValueLocator, decode_immutable_locator,
    decode_immutable_value, encode_immutable_locator,
};
use lix::storage::{
    BeginScanOptions, Capability, CommitResult, CoreProjection, GetManyRequest, GetManyResult, Key,
    KeyRange, Precondition, PreconditionFailure, ProjectedValue, PutBatch, ReadDurability,
    ReadEntry, ReadOptions, ScanChunk, ScanCursor as StorageScanCursor, ScanOrder, SpaceId,
    Storage, StorageError, StorageRead, StorageScanSource, StorageSpace, StorageWrite, StoredValue,
    ValueSemantics, WriteOptions, WriteStats,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    Attributes, CopyOptions, Extensions, GetOptions as ObjectStoreGetOptions, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use slatedb::admin::AdminBuilder;
use slatedb::config::{
    CompressionCodec, DurabilityLevel, FlushOptions, FlushType, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, ObjectStoreCacheOptions, ReadOptions as SlateDBReadOptions,
    ScanOptions as SlateDBScanOptions, Settings, WriteOptions as SlateDBWriteOptions,
};
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};
use slatedb::filter_policy::BloomFilterPolicy;
use slatedb::prefix_extractor::{PrefixExtractor, PrefixTarget};
use slatedb::{
    CloseReason, Db, DbIterator, DbSnapshot, DbStatus, GarbageCollectorBuilder, KeyValue,
    WriteBatch,
};
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};
use tempfile::TempDir;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard, oneshot};

#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom};

const DB_PATH: &str = "db";
const SEGMENTED_FORMAT_PATH: &str = "lix-space-segments-v2";
const IMMUTABLE_VALUE_PATH: &str = "lix-immutable-value-segment-v1";
const IMMUTABLE_VALUE_CACHE_PATH: &str = "lix-immutable-value-segment-v1";
const IMMUTABLE_CACHE_VALUE_MAGIC: &[u8; 8] = b"LIXICV5\0";
const IMMUTABLE_VALUE_IO_CONCURRENCY: usize = 32;
const IMMUTABLE_CACHE_EXTENT_BYTES: usize = 8 * 1024 * 1024;
const IMMUTABLE_GC_GRACE: Duration = Duration::from_secs(60 * 60);
const SPACE_PREFIX_LEN: usize = 4;
const SPACE_PREFIX_EXTRACTOR_NAME: &str = "lix-storage-space-be32-v1";
const MAX_SLATEDB_KEY_LEN: usize = u16::MAX as usize;
const RUNTIME_WORKER_THREADS: usize = 2;
const POINT_READ_CONCURRENCY: usize = 64;
// The engine-level exact-point cache already absorbs the dominant hot-read
// workload. Keep this snapshot cache as a bounded coherence aid rather than a
// second 16 MiB copy of the same values.
const SNAPSHOT_POINT_CACHE_BYTES: usize = 2 * 1024 * 1024;
const SNAPSHOT_POINT_CACHE_ENTRIES: usize = 512;
const SNAPSHOT_POINT_CACHE_MAX_VALUE_BYTES: usize = 64 * 1024;
const DEFAULT_BLOCK_CACHE_BYTES: u64 = 4 * 1024 * 1024;
// Keep the commit, change, and reverse change-ID indexes' Bloom filters
// resident across batched point validation. At 10M commits, 64 MiB thrashed
// these multi-megabyte filters and turned 1,001 five-key probes into 353 MiB
// of repeated compacted-object reads.
const DEFAULT_METADATA_CACHE_BYTES: u64 = 256 * 1024 * 1024;
// Large repositories probe several identity spaces for new absent IDs on
// every append. Sixteen bits keeps aggregate false positives bounded across
// the L0 and sorted-run fan-out without changing the filter encoding.
const FILTER_BITS_PER_KEY: u32 = 16;
const MAX_UNFLUSHED_BYTES: usize = 128 * 1024 * 1024;
const SCAN_READ_AHEAD_BYTES: usize = 2 * 1024 * 1024;
const SCAN_MAX_FETCH_TASKS: usize = 16;
const SCAN_CACHE_BLOCKS: bool = true;
const OBJECT_STORE_CACHE_PART_SIZE_BYTES: usize = 2 * 1024 * 1024;
const COMPACTOR_COMMIT_INTERVAL: Duration = Duration::from_secs(5);
// SlateDB 0.14.1 hard-codes this lifetime for the unnamed checkpoints that
// protect compaction inputs from concurrent readers while a manifest changes.
const COMPACTOR_SAFETY_CHECKPOINT_LIFETIME: Duration = Duration::from_secs(15 * 60);
const WRITE_PIPELINE_MAX_PENDING_ENTRIES: usize = 1024 * 1024;
const WRITE_PIPELINE_MAX_PENDING_BYTES: usize = 128 * 1024 * 1024;
const LOCAL_SST_FILE_CACHE_ENTRIES: usize = 256;
// Local direct reads do not pay object-store request latency. An 8 MiB content
// budget preserves small-SST reuse without retaining a second 32 MiB tier next
// to SlateDB's block cache.
const LOCAL_SST_CONTENT_CACHE_BYTES: usize = 4 * 1024 * 1024;
const LOCAL_SST_CONTENT_MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug)]
struct StorageSpacePrefixExtractor;

impl PrefixExtractor for StorageSpacePrefixExtractor {
    fn name(&self) -> &str {
        SPACE_PREFIX_EXTRACTOR_NAME
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let len = match target {
            PrefixTarget::Point(bytes) | PrefixTarget::Prefix(bytes) => bytes.len(),
        };
        (len >= SPACE_PREFIX_LEN).then_some(SPACE_PREFIX_LEN)
    }
}

#[derive(Debug)]
pub struct SlateDBFactory {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct SlateDBFixture {
    path: PathBuf,
}

/// Immutable large values do not benefit from an LSM tree: they are never
/// updated in place, their content hash is already the lookup key, and their
/// presence is published atomically by a separate small marker row. Store the
/// encoded bytes once in the backing object store while SlateDB retains the
/// transactional marker, snapshot visibility, and range index.
#[derive(Clone)]
#[allow(missing_debug_implementations)]
struct ImmutableValueStore {
    object_store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    cache: Option<ImmutableValueCache>,
    counters: Option<SlateDBIoCounters>,
    process_segments: Arc<AsyncMutex<HashSet<Key>>>,
}

impl ImmutableValueStore {
    fn new(
        db_path: &str,
        object_store: Arc<dyn ObjectStore>,
        cache: Option<&SlateDBCacheOptions>,
        counters: Option<SlateDBIoCounters>,
    ) -> Self {
        Self {
            object_store,
            prefix: ObjectPath::from(join_db_path(db_path, IMMUTABLE_VALUE_PATH)),
            cache: cache.map(|options| ImmutableValueCache::new(options, counters.clone())),
            counters,
            process_segments: Arc::new(AsyncMutex::new(HashSet::new())),
        }
    }

    fn location(&self, key: &Key) -> Result<ObjectPath, StorageError> {
        let hash: [u8; 32] = key
            .0
            .as_ref()
            .try_into()
            .map_err(|_| StorageError::InvalidKey)?;
        Ok(self
            .prefix
            .clone()
            .join(blake3::Hash::from_bytes(hash).to_hex().as_str()))
    }

    async fn put_segments(&self, segments: Vec<ImmutableSegment>) -> Result<(), StorageError> {
        {
            let mut process_segments = self.process_segments.lock().await;
            process_segments.extend(segments.iter().map(|segment| segment.id.clone()));
        }
        tokio::task::yield_now().await;
        let results = stream::iter(segments)
            .map(|segment| {
                let store = Arc::clone(&self.object_store);
                let location = self.location(&segment.id);
                async move {
                    let location = location?;
                    let payload = segment.frames.into_iter().collect::<PutPayload>();
                    let comparison_payload = payload.clone();
                    match store
                        .put_opts(
                            &location,
                            payload,
                            PutOptions {
                                mode: PutMode::Create,
                                ..PutOptions::default()
                            },
                        )
                        .await
                    {
                        Ok(_) => return Ok::<(), StorageError>(()),
                        Err(object_store::Error::AlreadyExists { .. }) => {
                            let expected = Bytes::from(comparison_payload);
                            let existing = store
                                .get(&location)
                                .await
                                .map_err(object_store_error)?
                                .bytes()
                                .await
                                .map_err(object_store_error)?;
                            if existing != expected {
                                return Err(StorageError::Corruption(
                                    "immutable segment identity was assigned different bytes"
                                        .to_string(),
                                ));
                            }
                        }
                        Err(error) => return Err(object_store_error(error)),
                    }
                    Ok(())
                }
            })
            .buffer_unordered(IMMUTABLE_VALUE_IO_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        let mut first_error = None;
        for result in results {
            match result {
                Ok(()) => {}
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn get_many(&self, markers: Vec<Bytes>) -> Result<Vec<Bytes>, StorageError> {
        let mut requests_by_segment = BTreeMap::<Key, (usize, Vec<(usize, Range<usize>)>)>::new();
        for (index, marker) in markers.into_iter().enumerate() {
            let locator = decode_immutable_locator(&marker)?;
            let (segment_len, requests) = requests_by_segment
                .entry(locator.segment_id)
                .or_insert_with(|| (locator.segment_len, Vec::new()));
            if *segment_len != locator.segment_len {
                return Err(StorageError::Corruption(
                    "immutable locators disagree on segment length".to_string(),
                ));
            }
            requests.push((index, locator.range));
        }
        let request_count = requests_by_segment
            .values()
            .map(|(_, requests)| requests.len())
            .sum::<usize>();
        let batches = stream::iter(requests_by_segment)
            .map(|(segment_key, (segment_len, requests))| {
                let store = Arc::clone(&self.object_store);
                let location = self.location(&segment_key);
                let cache = self.cache.clone();
                async move {
                    let location = location?;
                    let requested_ranges = requests
                        .iter()
                        .map(|(_, range)| range.clone())
                        .collect::<Vec<_>>();
                    let plan = if cache.is_some() {
                        plan_immutable_extents(&requested_ranges, segment_len)?
                    } else {
                        plan_coalesced_immutable_ranges(&requested_ranges, segment_len)?
                    };
                    let probes = stream::iter(plan.extents.iter().cloned().enumerate())
                        .map(|(span_index, range)| {
                            let cache = cache.clone();
                            let segment_key = segment_key.clone();
                            async move {
                                let cache_key = immutable_range_cache_key(&segment_key, &range)?;
                                let value = match &cache {
                                    Some(cache) => cache.get(&cache_key).await,
                                    None => None,
                                };
                                Ok::<_, StorageError>((span_index, range, cache_key, value))
                            }
                        })
                        .buffer_unordered(IMMUTABLE_VALUE_IO_CONCURRENCY)
                        .try_collect::<Vec<_>>()
                        .await?;
                    let mut spans = vec![None; probes.len()];
                    let mut misses = Vec::new();
                    for (span_index, range, cache_key, value) in probes {
                        if let Some(value) = value {
                            spans[span_index] = Some(value);
                        } else {
                            misses.push((span_index, range, cache_key));
                        }
                    }
                    let mut fetch_guards = Vec::new();
                    if let Some(cache) = &cache {
                        let cache_keys = misses
                            .iter()
                            .map(|(_, _, cache_key)| cache_key.clone())
                            .collect::<Vec<_>>();
                        fetch_guards = cache.lock_fetches(&cache_keys).await;
                        let mut locked_misses = Vec::with_capacity(misses.len());
                        for (span_index, range, cache_key) in misses {
                            if let Some(value) = cache.get(&cache_key).await {
                                spans[span_index] = Some(value);
                            } else {
                                locked_misses.push((span_index, range, cache_key));
                            }
                        }
                        misses = locked_misses;
                    }
                    let remote_ranges = misses
                        .iter()
                        .map(|(_, range, _)| {
                            Ok(u64::try_from(range.start).map_err(|_| {
                                StorageError::Corruption(
                                    "immutable segment range start exceeds u64".to_string(),
                                )
                            })?
                                ..u64::try_from(range.end).map_err(|_| {
                                    StorageError::Corruption(
                                        "immutable segment range end exceeds u64".to_string(),
                                    )
                                })?)
                        })
                        .collect::<Result<Vec<_>, StorageError>>()?;
                    let remote_spans = if remote_ranges.is_empty() {
                        Vec::new()
                    } else {
                        store
                            .get_ranges(&location, &remote_ranges)
                            .await
                            .map_err(object_store_error)?
                    };
                    let mut cache_writes = Vec::with_capacity(misses.len());
                    for ((span_index, range, cache_key), value) in
                        misses.into_iter().zip(remote_spans)
                    {
                        if value.len() != range.len() {
                            return Err(StorageError::Corruption(
                                "immutable segment read omitted a coalesced span".to_string(),
                            ));
                        }
                        cache_writes.push((cache_key, value.clone()));
                        spans[span_index] = Some(value);
                    }
                    if let Some(cache) = cache {
                        stream::iter(cache_writes)
                            .for_each_concurrent(
                                IMMUTABLE_VALUE_IO_CONCURRENCY,
                                |(cache_key, value)| {
                                    let cache = cache.clone();
                                    async move { cache.put(&cache_key, value).await }
                                },
                            )
                            .await;
                    }
                    drop(fetch_guards);
                    requests
                        .into_iter()
                        .zip(plan.placements)
                        .map(|((index, requested), fragments)| {
                            materialize_immutable_request(&spans, &requested, &fragments)
                                .map(|value| (index, value))
                        })
                        .collect::<Result<Vec<_>, StorageError>>()
                }
            })
            .buffer_unordered(IMMUTABLE_VALUE_IO_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let mut encoded = vec![None; request_count];
        for (index, value) in batches.into_iter().flatten() {
            let slot = encoded.get_mut(index).ok_or_else(|| {
                StorageError::Corruption("immutable segment result index is invalid".to_string())
            })?;
            *slot = Some(value);
        }
        encoded
            .into_iter()
            .map(|value| {
                value
                    .ok_or_else(|| {
                        StorageError::Corruption(
                            "immutable segment read omitted a requested value".to_string(),
                        )
                    })
                    .and_then(decode_immutable_value)
            })
            .collect()
    }

    async fn collect_unreachable(
        &self,
        reachable: HashSet<Key>,
        cutoff: SystemTime,
    ) -> Result<(), StorageError> {
        let reachable = reachable
            .iter()
            .map(|key| self.location(key))
            .collect::<Result<HashSet<_>, _>>()?;
        let mut objects = self.object_store.list(Some(&self.prefix));
        let mut candidates = Vec::new();
        while let Some(object) = objects.next().await {
            let object = object.map_err(object_store_error)?;
            if !reachable.contains(&object.location)
                && SystemTime::from(object.last_modified) <= cutoff
            {
                candidates.push(object.location);
            }
        }
        // Segments uploaded or reused by this process are protected until
        // restart. Hold registration across the short candidate sweep so a
        // concurrent upload cannot pass the check and then lose its object.
        let process_segments = self.process_segments.lock().await;
        let process_locations = process_segments
            .iter()
            .map(|key| self.location(key))
            .collect::<Result<HashSet<_>, _>>()?;
        for location in candidates {
            if !process_locations.contains(&location) {
                self.object_store
                    .delete(&location)
                    .await
                    .map_err(object_store_error)?;
            }
        }
        Ok(())
    }
}

fn materialize_immutable_request(
    spans: &[Option<Bytes>],
    requested: &Range<usize>,
    fragments: &[(usize, Range<usize>)],
) -> Result<Bytes, StorageError> {
    if let [(span_index, range)] = fragments {
        let span = spans
            .get(*span_index)
            .and_then(Option::as_ref)
            .ok_or_else(|| {
                StorageError::Corruption(
                    "immutable segment read omitted a requested extent".to_string(),
                )
            })?;
        if range.end > span.len() || range.len() != requested.len() {
            return Err(StorageError::Corruption(
                "immutable segment extent is truncated".to_string(),
            ));
        }
        return Ok(span.slice(range.clone()));
    }

    let mut encoded = BytesMut::with_capacity(requested.len());
    for (span_index, range) in fragments {
        let span = spans
            .get(*span_index)
            .and_then(Option::as_ref)
            .ok_or_else(|| {
                StorageError::Corruption(
                    "immutable segment read omitted a requested extent".to_string(),
                )
            })?;
        if range.end > span.len() {
            return Err(StorageError::Corruption(
                "immutable segment extent is truncated".to_string(),
            ));
        }
        encoded.extend_from_slice(&span[range.clone()]);
    }
    if encoded.len() != requested.len() {
        return Err(StorageError::Corruption(
            "immutable extents did not reconstruct the requested value".to_string(),
        ));
    }
    Ok(encoded.freeze())
}

#[derive(Debug, PartialEq, Eq)]
struct ImmutableExtentPlan {
    extents: Vec<Range<usize>>,
    placements: Vec<Vec<(usize, Range<usize>)>>,
}

fn plan_coalesced_immutable_ranges(
    ranges: &[Range<usize>],
    segment_len: usize,
) -> Result<ImmutableExtentPlan, StorageError> {
    let mut order = (0..ranges.len()).collect::<Vec<_>>();
    order.sort_unstable_by_key(|index| (ranges[*index].start, ranges[*index].end));
    let mut extents = Vec::<Range<usize>>::new();
    let mut placements = vec![Vec::new(); ranges.len()];
    for index in order {
        let range = &ranges[index];
        if range.start >= range.end || range.end > segment_len {
            return Err(StorageError::Corruption(
                "immutable value range is outside its segment".to_string(),
            ));
        }
        let extent_index = match extents.last_mut() {
            Some(extent)
                if range.start <= extent.end
                    && range.end.saturating_sub(extent.start) <= IMMUTABLE_CACHE_EXTENT_BYTES =>
            {
                extent.end = extent.end.max(range.end);
                extents.len() - 1
            }
            _ => {
                extents.push(range.clone());
                extents.len() - 1
            }
        };
        let extent = &extents[extent_index];
        placements[index].push((
            extent_index,
            range.start - extent.start..range.end - extent.start,
        ));
    }
    Ok(ImmutableExtentPlan {
        extents,
        placements,
    })
}

fn plan_immutable_extents(
    ranges: &[Range<usize>],
    segment_len: usize,
) -> Result<ImmutableExtentPlan, StorageError> {
    let mut starts = BTreeMap::<usize, ()>::new();
    for range in ranges {
        if range.start >= range.end || range.end > segment_len {
            return Err(StorageError::Corruption(
                "immutable value range is outside its segment".to_string(),
            ));
        }
        let mut start = range.start - range.start % IMMUTABLE_CACHE_EXTENT_BYTES;
        while start < range.end {
            starts.insert(start, ());
            start = start
                .checked_add(IMMUTABLE_CACHE_EXTENT_BYTES)
                .ok_or_else(|| {
                    StorageError::Corruption("immutable extent range overflows usize".to_string())
                })?;
        }
    }
    let extents = starts
        .into_keys()
        .map(|start| {
            start
                ..start
                    .saturating_add(IMMUTABLE_CACHE_EXTENT_BYTES)
                    .min(segment_len)
        })
        .collect::<Vec<_>>();
    let extent_indexes = extents
        .iter()
        .enumerate()
        .map(|(index, extent)| (extent.start, index))
        .collect::<HashMap<_, _>>();
    let placements = ranges
        .iter()
        .map(|range| {
            let mut fragments = Vec::new();
            let mut position = range.start;
            while position < range.end {
                let extent_start = position - position % IMMUTABLE_CACHE_EXTENT_BYTES;
                let extent_index = *extent_indexes.get(&extent_start).ok_or_else(|| {
                    StorageError::Corruption("immutable extent plan omitted a range".to_string())
                })?;
                let extent_end = extent_start
                    .saturating_add(IMMUTABLE_CACHE_EXTENT_BYTES)
                    .min(segment_len);
                let fragment_end = range.end.min(extent_end);
                fragments.push((
                    extent_index,
                    position - extent_start..fragment_end - extent_start,
                ));
                position = fragment_end;
            }
            Ok(fragments)
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    Ok(ImmutableExtentPlan {
        extents,
        placements,
    })
}

fn immutable_range_cache_key(segment_key: &Key, range: &Range<usize>) -> Result<Key, StorageError> {
    let start = u64::try_from(range.start).map_err(|_| {
        StorageError::Corruption("immutable cache range start exceeds u64".to_string())
    })?;
    let end = u64::try_from(range.end).map_err(|_| {
        StorageError::Corruption("immutable cache range end exceeds u64".to_string())
    })?;
    let mut hash = blake3::Hasher::new();
    hash.update(&segment_key.0);
    hash.update(&start.to_le_bytes());
    hash.update(&end.to_le_bytes());
    Ok(Key(Bytes::copy_from_slice(hash.finalize().as_bytes())))
}

/// Bounded whole-object cache for the immutable sidecar namespace.
///
/// SlateDB's internal cached object store is intentionally private upstream,
/// so sidecar reads use an adjacent cache rooted in the caller's configured
/// disk-cache directory. Cache failures are soft: the authoritative object
/// store remains readable and immutable content makes stale validation
/// unnecessary.
#[derive(Clone, Debug)]
struct ImmutableValueCache {
    root: PathBuf,
    max_bytes: usize,
    current_bytes: Arc<AtomicU64>,
    counters: Option<SlateDBIoCounters>,
    fetch_locks: Arc<[Arc<AsyncMutex<()>>]>,
}

impl ImmutableValueCache {
    fn new(options: &SlateDBCacheOptions, counters: Option<SlateDBIoCounters>) -> Self {
        let root = options.root_folder.join(IMMUTABLE_VALUE_CACHE_PATH);
        let (_, immutable_max_bytes) = disk_cache_budgets(options.max_disk_cache_bytes);
        let current_bytes = if root.is_dir() {
            prune_immutable_value_cache(&root, immutable_max_bytes)
                .unwrap_or_else(|_| immutable_value_cache_bytes(&root))
        } else {
            0
        };
        Self {
            current_bytes: Arc::new(AtomicU64::new(current_bytes)),
            root,
            max_bytes: immutable_max_bytes,
            counters,
            fetch_locks: Arc::from(
                (0..64)
                    .map(|_| Arc::new(AsyncMutex::new(())))
                    .collect::<Vec<_>>(),
            ),
        }
    }

    fn fetch_lock_index(&self, key: &Key) -> usize {
        let mut prefix = [0_u8; 8];
        let copy_len = key.0.len().min(prefix.len());
        prefix[..copy_len].copy_from_slice(&key.0[..copy_len]);
        u64::from_le_bytes(prefix) as usize % self.fetch_locks.len()
    }

    async fn lock_fetch_index(&self, index: usize) -> OwnedMutexGuard<()> {
        Arc::clone(&self.fetch_locks[index]).lock_owned().await
    }

    async fn lock_fetches(&self, keys: &[Key]) -> Vec<OwnedMutexGuard<()>> {
        let mut indexes = keys
            .iter()
            .map(|key| self.fetch_lock_index(key))
            .collect::<Vec<_>>();
        indexes.sort_unstable();
        indexes.dedup();
        let mut guards = Vec::with_capacity(indexes.len());
        for index in indexes {
            guards.push(self.lock_fetch_index(index).await);
        }
        guards
    }

    fn path(&self, key: &Key) -> Option<PathBuf> {
        let hash: [u8; 32] = key.0.as_ref().try_into().ok()?;
        Some(
            self.root
                .join(blake3::Hash::from_bytes(hash).to_hex().as_str()),
        )
    }

    async fn get(&self, key: &Key) -> Option<Bytes> {
        if let Some(counters) = &self.counters {
            counters
                .inner
                .cache_filesystem_reads
                .fetch_add(1, Ordering::Relaxed);
        }
        let path = self.path(key)?;
        let cached = tokio::task::spawn_blocking(move || std::fs::read(path).ok().map(Bytes::from))
            .await
            .ok()
            .flatten()?;
        if let Some(value) = decode_immutable_cache_value(cached) {
            return Some(value);
        }
        self.remove(key).await;
        None
    }

    async fn put(&self, key: &Key, value: Bytes) {
        if let Some(counters) = &self.counters {
            counters
                .inner
                .cache_filesystem_writes
                .fetch_add(1, Ordering::Relaxed);
        }
        let value = encode_immutable_cache_value(value);
        if value.len() > self.max_bytes {
            return;
        }
        let Some(path) = self.path(key) else {
            return;
        };
        let root = self.root.clone();
        let max_bytes = self.max_bytes;
        let current_bytes = Arc::clone(&self.current_bytes);
        let _ = tokio::task::spawn_blocking(move || {
            use std::io::Write;

            std::fs::create_dir_all(&root)?;
            if !path.exists() {
                let mut temporary = tempfile::NamedTempFile::new_in(&root)?;
                temporary.write_all(&value)?;
                match temporary.persist_noclobber(&path) {
                    Ok(_) => {
                        current_bytes.fetch_add(value.len() as u64, Ordering::Relaxed);
                    }
                    Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error.error),
                }
            }
            if current_bytes.load(Ordering::Relaxed) > max_bytes as u64 {
                current_bytes.store(
                    prune_immutable_value_cache(&root, max_bytes)?,
                    Ordering::Relaxed,
                );
            }
            Ok(())
        })
        .await;
    }

    async fn remove(&self, key: &Key) {
        if let Some(counters) = &self.counters {
            counters
                .inner
                .cache_filesystem_removes
                .fetch_add(1, Ordering::Relaxed);
        }
        let Some(path) = self.path(key) else {
            return;
        };
        let current_bytes = Arc::clone(&self.current_bytes);
        let _ = tokio::task::spawn_blocking(move || {
            let bytes = std::fs::metadata(&path)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            if std::fs::remove_file(path).is_ok() {
                let _ =
                    current_bytes.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        Some(current.saturating_sub(bytes))
                    });
            }
        })
        .await;
    }
}

fn encode_immutable_cache_value(value: Bytes) -> Bytes {
    let mut encoded = Vec::with_capacity(IMMUTABLE_CACHE_VALUE_MAGIC.len() + 32 + value.len());
    encoded.extend_from_slice(IMMUTABLE_CACHE_VALUE_MAGIC);
    encoded.extend_from_slice(blake3::hash(&value).as_bytes());
    encoded.extend_from_slice(&value);
    Bytes::from(encoded)
}

fn decode_immutable_cache_value(encoded: Bytes) -> Option<Bytes> {
    let digest_start = IMMUTABLE_CACHE_VALUE_MAGIC.len();
    let value_start = digest_start.checked_add(32)?;
    if encoded.get(..digest_start)? != IMMUTABLE_CACHE_VALUE_MAGIC {
        return None;
    }
    let expected = encoded.get(digest_start..value_start)?;
    let value = encoded.slice(value_start..);
    (blake3::hash(&value).as_bytes() == expected).then_some(value)
}

fn immutable_value_cache_bytes(root: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .filter(|entry| is_immutable_value_cache_object(&entry.path()))
        .filter_map(|entry| entry.metadata().ok())
        .map(|metadata| metadata.len())
        .fold(0_u64, u64::saturating_add)
}

fn is_immutable_value_cache_object(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

fn prune_immutable_value_cache(root: &Path, max_bytes: usize) -> std::io::Result<u64> {
    let mut total = 0_u64;
    let mut files = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if !is_immutable_value_cache_object(&entry.path()) {
            continue;
        }
        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            continue;
        }
        total = total.saturating_add(metadata.len());
        files.push((
            metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            metadata.len(),
            entry.path(),
        ));
    }
    if total <= max_bytes as u64 {
        return Ok(total);
    }
    files.sort_unstable_by_key(|(modified, _, path)| (*modified, path.clone()));
    for (_, bytes, path) in files {
        match std::fs::remove_file(path) {
            Ok(()) => total = total.saturating_sub(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if total <= max_bytes as u64 {
            break;
        }
    }
    Ok(total)
}

#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub struct SlateDB {
    path: PathBuf,
    worker: SlateDBWorker,
    immutable_value_store: ImmutableValueStore,
    write_gate: WriteGate,
    write_pipeline: WritePipeline,
    point_cache: SnapshotPointCache,
    startup_immutable_gc: StartupImmutableGc,
}

#[derive(Clone, Default)]
struct StartupImmutableGc {
    state: Arc<StartupImmutableGcState>,
}

#[derive(Default)]
struct StartupImmutableGcState {
    scheduled: AtomicBool,
    result: Mutex<Option<Result<(), StorageError>>>,
}

impl StartupImmutableGc {
    fn schedule(&self, worker: &SlateDBWorker, store: &ImmutableValueStore) {
        let cutoff = SystemTime::now()
            .checked_sub(IMMUTABLE_GC_GRACE)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        self.schedule_with_cutoff(worker, store, cutoff);
    }

    fn schedule_with_cutoff(
        &self,
        worker: &SlateDBWorker,
        store: &ImmutableValueStore,
        cutoff: SystemTime,
    ) {
        if self
            .state
            .scheduled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let state = Arc::clone(&self.state);
        let store = store.clone();
        worker.spawn_reclamation(move |database| async move {
            let result =
                collect_startup_immutable_garbage_from_database(database, &store, cutoff).await;
            *state
                .result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
        });
    }

    fn completed_result(&self) -> Result<(), StorageError> {
        self.state
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or(Ok(()))
    }
}

#[derive(Clone, Debug, Default)]
pub struct SlateDBObjectStoreOptions {
    pub cache: Option<SlateDBCacheOptions>,
}

#[derive(Clone, Debug)]
pub struct SlateDBCacheOptions {
    pub root_folder: PathBuf,
    pub max_disk_cache_bytes: usize,
    pub block_cache_bytes: u64,
    pub metadata_cache_bytes: u64,
}

#[derive(Clone)]
pub struct SlateDBIoCounters {
    inner: Arc<SlateDBIoCounterValues>,
    metrics: Arc<DefaultMetricsRecorder>,
}

impl fmt::Debug for SlateDBIoCounters {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("SlateDBIoCounters").finish()
    }
}

impl Default for SlateDBIoCounters {
    fn default() -> Self {
        Self {
            inner: Arc::default(),
            metrics: Arc::new(DefaultMetricsRecorder::new()),
        }
    }
}

#[derive(Debug, Default)]
struct SlateDBIoCounterValues {
    read_objects: AtomicU64,
    read_bytes: AtomicU64,
    write_objects: AtomicU64,
    write_bytes: AtomicU64,
    list_operations: AtomicU64,
    listed_objects: AtomicU64,
    deleted_objects: AtomicU64,
    copied_objects: AtomicU64,
    immutable_locator_rows: AtomicU64,
    cache_filesystem_reads: AtomicU64,
    cache_filesystem_writes: AtomicU64,
    cache_filesystem_removes: AtomicU64,
    writer_gate_acquisitions: AtomicU64,
    writer_gate_wait_nanos: AtomicU64,
    wal: SlateDBIoCategoryCounters,
    compacted: SlateDBIoCategoryCounters,
    manifest: SlateDBIoCategoryCounters,
    compactions: SlateDBIoCategoryCounters,
    other: SlateDBIoCategoryCounters,
}

#[derive(Debug, Default)]
struct SlateDBIoCategoryCounters {
    read_objects: AtomicU64,
    read_bytes: AtomicU64,
    write_objects: AtomicU64,
    write_bytes: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SlateDBIoSnapshot {
    pub read_objects: u64,
    pub read_bytes: u64,
    pub write_objects: u64,
    pub write_bytes: u64,
    pub list_operations: u64,
    pub listed_objects: u64,
    pub deleted_objects: u64,
    pub copied_objects: u64,
    pub immutable_locator_rows: u64,
    pub cache_filesystem_reads: u64,
    pub cache_filesystem_writes: u64,
    pub cache_filesystem_removes: u64,
    pub writer_gate_acquisitions: u64,
    pub writer_gate_wait_nanos: u64,
    pub wal: SlateDBIoCategorySnapshot,
    pub compacted: SlateDBIoCategorySnapshot,
    pub manifest: SlateDBIoCategorySnapshot,
    pub compactions: SlateDBIoCategorySnapshot,
    pub other: SlateDBIoCategorySnapshot,
    pub main: SlateDBIoComponentSnapshot,
    pub reader: SlateDBIoComponentSnapshot,
    pub compactor: SlateDBIoComponentSnapshot,
    pub gc: SlateDBIoComponentSnapshot,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SlateDBIoCategorySnapshot {
    pub read_objects: u64,
    pub read_bytes: u64,
    pub write_objects: u64,
    pub write_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SlateDBIoComponentSnapshot {
    pub read_requests: u64,
    pub write_requests: u64,
}

impl SlateDBIoCounters {
    pub fn snapshot(&self) -> SlateDBIoSnapshot {
        let metrics = self.metrics.snapshot();
        SlateDBIoSnapshot {
            read_objects: self.inner.read_objects.load(Ordering::Relaxed),
            read_bytes: self.inner.read_bytes.load(Ordering::Relaxed),
            write_objects: self.inner.write_objects.load(Ordering::Relaxed),
            write_bytes: self.inner.write_bytes.load(Ordering::Relaxed),
            list_operations: self.inner.list_operations.load(Ordering::Relaxed),
            listed_objects: self.inner.listed_objects.load(Ordering::Relaxed),
            deleted_objects: self.inner.deleted_objects.load(Ordering::Relaxed),
            copied_objects: self.inner.copied_objects.load(Ordering::Relaxed),
            immutable_locator_rows: self.inner.immutable_locator_rows.load(Ordering::Relaxed),
            cache_filesystem_reads: self.inner.cache_filesystem_reads.load(Ordering::Relaxed),
            cache_filesystem_writes: self.inner.cache_filesystem_writes.load(Ordering::Relaxed),
            cache_filesystem_removes: self.inner.cache_filesystem_removes.load(Ordering::Relaxed),
            writer_gate_acquisitions: self.inner.writer_gate_acquisitions.load(Ordering::Relaxed),
            writer_gate_wait_nanos: self.inner.writer_gate_wait_nanos.load(Ordering::Relaxed),
            wal: self.inner.wal.snapshot(),
            compacted: self.inner.compacted.snapshot(),
            manifest: self.inner.manifest.snapshot(),
            compactions: self.inner.compactions.snapshot(),
            other: self.inner.other.snapshot(),
            main: component_snapshot(&metrics, "db"),
            reader: component_snapshot(&metrics, "reader"),
            compactor: component_snapshot(&metrics, "compactor"),
            gc: component_snapshot(&metrics, "gc"),
        }
    }
}

fn component_snapshot(
    metrics: &slatedb_common::metrics::Metrics,
    component: &str,
) -> SlateDBIoComponentSnapshot {
    let mut snapshot = SlateDBIoComponentSnapshot::default();
    for metric in metrics.by_name("slatedb.object_store.request_count") {
        let label = |name: &str| {
            metric
                .labels
                .iter()
                .find_map(|(key, value)| (key == name).then_some(value.as_str()))
        };
        if label("component") != Some(component) {
            continue;
        }
        let MetricValue::Counter(value) = metric.value else {
            continue;
        };
        match label("op") {
            Some("get") => snapshot.read_requests = snapshot.read_requests.saturating_add(value),
            Some("put") => snapshot.write_requests = snapshot.write_requests.saturating_add(value),
            _ => {}
        }
    }
    snapshot
}

impl SlateDBIoCounterValues {
    fn category(&self, location: &ObjectPath) -> &SlateDBIoCategoryCounters {
        let path = location.as_ref();
        if path.split('/').any(|part| part == "wal") {
            &self.wal
        } else if path.split('/').any(|part| part == "compacted") {
            &self.compacted
        } else if path.split('/').any(|part| part == "manifest") {
            &self.manifest
        } else if path.split('/').any(|part| part == "compactions") {
            &self.compactions
        } else {
            &self.other
        }
    }
}

impl SlateDBIoCategoryCounters {
    fn snapshot(&self) -> SlateDBIoCategorySnapshot {
        SlateDBIoCategorySnapshot {
            read_objects: self.read_objects.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_objects: self.write_objects.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
        }
    }
}

impl SlateDBIoSnapshot {
    pub fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            read_objects: self.read_objects.saturating_sub(earlier.read_objects),
            read_bytes: self.read_bytes.saturating_sub(earlier.read_bytes),
            write_objects: self.write_objects.saturating_sub(earlier.write_objects),
            write_bytes: self.write_bytes.saturating_sub(earlier.write_bytes),
            list_operations: self.list_operations.saturating_sub(earlier.list_operations),
            listed_objects: self.listed_objects.saturating_sub(earlier.listed_objects),
            deleted_objects: self.deleted_objects.saturating_sub(earlier.deleted_objects),
            copied_objects: self.copied_objects.saturating_sub(earlier.copied_objects),
            immutable_locator_rows: self
                .immutable_locator_rows
                .saturating_sub(earlier.immutable_locator_rows),
            cache_filesystem_reads: self
                .cache_filesystem_reads
                .saturating_sub(earlier.cache_filesystem_reads),
            cache_filesystem_writes: self
                .cache_filesystem_writes
                .saturating_sub(earlier.cache_filesystem_writes),
            cache_filesystem_removes: self
                .cache_filesystem_removes
                .saturating_sub(earlier.cache_filesystem_removes),
            writer_gate_acquisitions: self
                .writer_gate_acquisitions
                .saturating_sub(earlier.writer_gate_acquisitions),
            writer_gate_wait_nanos: self
                .writer_gate_wait_nanos
                .saturating_sub(earlier.writer_gate_wait_nanos),
            wal: self.wal.saturating_sub(earlier.wal),
            compacted: self.compacted.saturating_sub(earlier.compacted),
            manifest: self.manifest.saturating_sub(earlier.manifest),
            compactions: self.compactions.saturating_sub(earlier.compactions),
            other: self.other.saturating_sub(earlier.other),
            main: self.main.saturating_sub(earlier.main),
            reader: self.reader.saturating_sub(earlier.reader),
            compactor: self.compactor.saturating_sub(earlier.compactor),
            gc: self.gc.saturating_sub(earlier.gc),
        }
    }
}

impl SlateDBIoCategorySnapshot {
    fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            read_objects: self.read_objects.saturating_sub(earlier.read_objects),
            read_bytes: self.read_bytes.saturating_sub(earlier.read_bytes),
            write_objects: self.write_objects.saturating_sub(earlier.write_objects),
            write_bytes: self.write_bytes.saturating_sub(earlier.write_bytes),
        }
    }
}

impl SlateDBIoComponentSnapshot {
    fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            read_requests: self.read_requests.saturating_sub(earlier.read_requests),
            write_requests: self.write_requests.saturating_sub(earlier.write_requests),
        }
    }
}

/// Reads local table ranges on the caller executor.
///
/// `LocalFileSystem::get_opts` schedules file open and metadata work on the
/// blocking pool, and `GetResult::bytes` schedules the actual range read as a
/// second task. `SlateDB::open` already permits local reads on the caller
/// because it owns a dedicated current-thread runtime; doing the short local
/// syscalls there removes both dispatches while preserving the upstream path,
/// range, metadata, ETag, and precondition contracts.
#[derive(Debug)]
struct DirectLocalReads {
    inner: LocalFileSystem,
    files: Mutex<DirectLocalFileCache>,
}

#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counters: SlateDBIoCounters,
}

#[derive(Debug)]
struct CountingMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    counters: SlateDBIoCounters,
    location: ObjectPath,
    uploaded_bytes: Arc<AtomicU64>,
}

#[async_trait]
impl MultipartUpload for CountingMultipartUpload {
    fn put_part(&mut self, payload: PutPayload) -> object_store::UploadPart {
        let bytes = payload.content_length() as u64;
        let uploaded_bytes = Arc::clone(&self.uploaded_bytes);
        self.inner
            .put_part(payload)
            .map(move |result| {
                if result.is_ok() {
                    uploaded_bytes.fetch_add(bytes, Ordering::Relaxed);
                }
                result
            })
            .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let result = self.inner.complete().await?;
        let bytes = self.uploaded_bytes.load(Ordering::Relaxed);
        let category = self.counters.inner.category(&self.location);
        self.counters
            .inner
            .write_objects
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .inner
            .write_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        category.write_objects.fetch_add(1, Ordering::Relaxed);
        category.write_bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(result)
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

impl fmt::Display for CountingObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "counting {}", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let bytes = payload.content_length() as u64;
        let result = self.inner.put_opts(location, payload, options).await?;
        let category = self.counters.inner.category(location);
        self.counters
            .inner
            .write_objects
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .inner
            .write_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        category.write_objects.fetch_add(1, Ordering::Relaxed);
        category.write_bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, options).await?;
        Ok(Box::new(CountingMultipartUpload {
            inner,
            counters: self.counters.clone(),
            location: location.clone(),
            uploaded_bytes: Arc::new(AtomicU64::new(0)),
        }))
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: ObjectStoreGetOptions,
    ) -> object_store::Result<GetResult> {
        let mut result = self.inner.get_opts(location, options).await?;
        let category = self.counters.inner.category(location);
        self.counters
            .inner
            .read_objects
            .fetch_add(1, Ordering::Relaxed);
        category.read_objects.fetch_add(1, Ordering::Relaxed);
        if let GetResultPayload::Stream(payload) = result.payload {
            let counters = self.counters.clone();
            let location = location.clone();
            result.payload = GetResultPayload::Stream(
                payload
                    .map(move |item| {
                        if let Ok(bytes) = &item {
                            counters
                                .inner
                                .read_bytes
                                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                            counters
                                .inner
                                .category(&location)
                                .read_bytes
                                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        }
                        item
                    })
                    .boxed(),
            );
        }
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let result = self.inner.get_ranges(location, ranges).await?;
        let bytes = result.iter().map(|bytes| bytes.len() as u64).sum::<u64>();
        let category = self.counters.inner.category(location);
        self.counters
            .inner
            .read_objects
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .inner
            .read_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        category.read_objects.fetch_add(1, Ordering::Relaxed);
        category.read_bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        let counters = self.counters.clone();
        self.inner
            .delete_stream(locations)
            .map(move |item| {
                if item.is_ok() {
                    counters
                        .inner
                        .deleted_objects
                        .fetch_add(1, Ordering::Relaxed);
                }
                item
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.counters
            .inner
            .list_operations
            .fetch_add(1, Ordering::Relaxed);
        let counters = self.counters.clone();
        self.inner
            .list(prefix)
            .map(move |item| {
                if item.is_ok() {
                    counters
                        .inner
                        .listed_objects
                        .fetch_add(1, Ordering::Relaxed);
                }
                item
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.counters
            .inner
            .list_operations
            .fetch_add(1, Ordering::Relaxed);
        let result = self.inner.list_with_delimiter(prefix).await?;
        self.counters.inner.listed_objects.fetch_add(
            (result.objects.len() + result.common_prefixes.len()) as u64,
            Ordering::Relaxed,
        );
        Ok(result)
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await?;
        self.counters
            .inner
            .copied_objects
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct DirectLocalFileCache {
    entries: HashMap<ObjectPath, DirectLocalFile>,
    eviction_order: VecDeque<ObjectPath>,
    content_bytes: usize,
}

#[derive(Clone, Debug)]
struct DirectLocalFile {
    file: Arc<std::fs::File>,
    size: u64,
    modified: SystemTime,
    e_tag: String,
    contents: Option<Bytes>,
}

impl fmt::Display for DirectLocalReads {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "direct-read {}", self.inner)
    }
}

#[async_trait]
impl ObjectStore for DirectLocalReads {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: ObjectStoreGetOptions,
    ) -> object_store::Result<GetResult> {
        let local_file = self.local_file(location)?;
        let size = local_file.size;
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: local_file.modified.into(),
            size,
            e_tag: Some(local_file.e_tag.clone()),
            version: None,
        };
        options.check_preconditions(&meta)?;
        let range = match options.range {
            Some(range) => range
                .as_range(size)
                .map_err(|source| object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(source),
                })?,
            None => 0..size,
        };
        let bytes = if options.head || range.is_empty() {
            Bytes::new()
        } else if let Some(contents) = &local_file.contents {
            let start =
                usize::try_from(range.start).map_err(|source| object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(source),
                })?;
            let end =
                usize::try_from(range.end).map_err(|source| object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(source),
                })?;
            contents.slice(start..end)
        } else {
            let length = usize::try_from(range.end - range.start).map_err(|source| {
                object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(source),
                }
            })?;
            let mut bytes = vec![0; length];
            direct_local_read_exact_at(&local_file.file, &mut bytes, range.start)
                .map_err(|source| direct_local_io_error(location, source))?;
            Bytes::from(bytes)
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta,
            range,
            attributes: Attributes::default(),
            extensions: Extensions::default(),
        })
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let mut result = Vec::with_capacity(ranges.len());
        for range in ranges {
            let options = ObjectStoreGetOptions {
                range: Some(object_store::GetRange::Bounded(range.clone())),
                ..ObjectStoreGetOptions::default()
            };
            result.push(self.get_opts(location, options).await?.bytes().await?);
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

impl DirectLocalReads {
    fn local_file(&self, location: &ObjectPath) -> object_store::Result<DirectLocalFile> {
        let cacheable = Path::new(location.as_ref())
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"));
        if cacheable
            && let Some(file) = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .get(location)
                .cloned()
        {
            return Ok(file);
        }

        let filesystem_path = self.inner.path_to_filesystem(location)?;
        let file = std::fs::File::open(&filesystem_path)
            .map_err(|source| direct_local_io_error(location, source))?;
        let metadata = file
            .metadata()
            .map_err(|source| direct_local_io_error(location, source))?;
        if metadata.is_dir() {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "object is a directory".into(),
            });
        }
        let modified = metadata
            .modified()
            .map_err(|source| direct_local_io_error(location, source))?;
        let contents = if cacheable
            && LOCAL_SST_CONTENT_CACHE_BYTES > 0
            && metadata.len() <= LOCAL_SST_CONTENT_MAX_FILE_BYTES
            && metadata.len() <= LOCAL_SST_CONTENT_CACHE_BYTES as u64
        {
            let length =
                usize::try_from(metadata.len()).map_err(|source| object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(source),
                })?;
            let mut bytes = vec![0; length];
            direct_local_read_exact_at(&file, &mut bytes, 0)
                .map_err(|source| direct_local_io_error(location, source))?;
            Some(Bytes::from(bytes))
        } else {
            None
        };
        let file = DirectLocalFile {
            file: Arc::new(file),
            size: metadata.len(),
            modified,
            e_tag: direct_local_etag(&metadata, modified),
            contents,
        };
        if cacheable {
            let mut cache = self
                .files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(existing) = cache.entries.get(location) {
                return Ok(existing.clone());
            }
            let content_bytes = file.contents.as_ref().map_or(0, Bytes::len);
            while cache.entries.len() >= LOCAL_SST_FILE_CACHE_ENTRIES
                || cache.content_bytes.saturating_add(content_bytes) > LOCAL_SST_CONTENT_CACHE_BYTES
            {
                let Some(evicted) = cache.eviction_order.pop_front() else {
                    break;
                };
                if let Some(evicted) = cache.entries.remove(&evicted) {
                    cache.content_bytes = cache
                        .content_bytes
                        .saturating_sub(evicted.contents.as_ref().map_or(0, Bytes::len));
                }
            }
            cache.eviction_order.push_back(location.clone());
            cache.content_bytes = cache.content_bytes.saturating_add(content_bytes);
            cache.entries.insert(location.clone(), file.clone());
        }
        Ok(file)
    }
}

fn direct_local_io_error(location: &ObjectPath, source: std::io::Error) -> object_store::Error {
    if source.kind() == std::io::ErrorKind::NotFound {
        object_store::Error::NotFound {
            path: location.to_string(),
            source: Box::new(source),
        }
    } else {
        object_store::Error::Generic {
            store: "LocalFileSystem",
            source: Box::new(source),
        }
    }
}

#[cfg(unix)]
fn direct_local_read_exact_at(
    file: &std::fs::File,
    bytes: &mut [u8],
    offset: u64,
) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(bytes, offset)
}

#[cfg(not(unix))]
fn direct_local_read_exact_at(
    file: &std::fs::File,
    bytes: &mut [u8],
    offset: u64,
) -> std::io::Result<()> {
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(bytes)
}

fn direct_local_etag(metadata: &std::fs::Metadata, modified: SystemTime) -> String {
    #[cfg(unix)]
    let inode = {
        use std::os::unix::fs::MetadataExt;
        metadata.ino()
    };
    #[cfg(not(unix))]
    let inode = 0;
    let modified_micros = modified
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    format!("{inode:x}-{modified_micros:x}-{:x}", metadata.len())
}

#[allow(missing_debug_implementations)]
pub struct SlateDBRead {
    worker: SlateDBWorker,
    immutable_value_store: ImmutableValueStore,
    write_pipeline: WritePipeline,
    snapshot: Arc<DbSnapshot>,
    publication_view: Option<PublicationView>,
    durability: ReadDurability,
    point_cache: SnapshotPointCache,
    #[cfg(test)]
    scan_worker_gate: Option<Arc<ScanTestGate>>,
    #[cfg(test)]
    scan_hydration_gate: Option<Arc<ScanTestGate>>,
}

#[cfg(test)]
struct ScanTestGate {
    entered: AtomicBool,
    entered_notify: Notify,
    release: Notify,
}

#[cfg(test)]
impl ScanTestGate {
    fn new() -> Self {
        Self {
            entered: AtomicBool::new(false),
            entered_notify: Notify::new(),
            release: Notify::new(),
        }
    }

    async fn wait_until_entered(&self) {
        while !self.entered.load(Ordering::Acquire) {
            self.entered_notify.notified().await;
        }
    }
}

#[allow(missing_debug_implementations)]
pub struct SlateDBWrite {
    worker: SlateDBWorker,
    immutable_value_store: ImmutableValueStore,
    write_pipeline: WritePipeline,
    point_cache: SnapshotPointCache,
    write_gate: WriteGate,
    writer_permit: Option<OwnedMutexGuard<()>>,
    preconditions: Vec<Precondition>,
    await_durable: bool,
    base: Option<Arc<DbSnapshot>>,
    overlay: BTreeMap<Key, Option<Bytes>>,
    immutable_values: HashMap<Key, Bytes>,
    stats: WriteStats,
}

/// Bounded values from immutable visible snapshots.
///
/// SlateDB's snapshot sequence is the last committed sequence it exposes, so
/// the pair `(sequence, key)` identifies one point-read view even after newer
/// writes become visible. Keeping values under that key lets independently
/// opened reads reuse hot points without weakening snapshot isolation.
#[derive(Clone)]
struct SnapshotPointCache {
    state: Arc<Mutex<SnapshotPointCacheState>>,
}

#[derive(Default)]
struct SnapshotPointCacheState {
    entries: HashMap<u64, HashMap<Key, SnapshotPointCacheValue>>,
    eviction_order: VecDeque<SnapshotPointCacheKey>,
    used_bytes: usize,
    current_sequence: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SnapshotPointCacheKey {
    sequence: u64,
    key: Key,
}

#[derive(Clone)]
struct SnapshotPointCacheValue {
    value: Option<Bytes>,
    weight: usize,
}

#[derive(Clone, Default)]
struct WritePipeline {
    state: Arc<Mutex<WritePipelineState>>,
}

#[derive(Default)]
struct WritePipelineState {
    tail: Option<Arc<WriteCompletion>>,
    queued: VecDeque<QueuedWrite>,
    draining: bool,
    visible: VecDeque<Arc<PublishedWrite>>,
    active_views: BTreeMap<(u64, u64), usize>,
    snapshot_fetches: usize,
    newest_snapshot_sequence: u64,
    next_publication_id: u64,
    pending_entries: usize,
    pending_bytes: usize,
    terminal_error: Option<StorageError>,
    latest_snapshot: Option<Arc<DbSnapshot>>,
}

struct QueuedWrite {
    overlay: Arc<BTreeMap<Key, Option<Bytes>>>,
    published: Arc<PublishedWrite>,
    completion: Arc<WriteCompletion>,
    await_durable: bool,
    weight_bytes: usize,
}

struct PublishedWrite {
    publication_id: u64,
    overlay: Arc<BTreeMap<Key, Option<Bytes>>>,
    persisted_sequence: AtomicU64,
}

struct PublicationView {
    pipeline: WritePipeline,
    worker: Option<SlateDBWorker>,
    snapshot_sequence: u64,
    publication_id: u64,
}

struct SnapshotFetch {
    pipeline: WritePipeline,
    worker: SlateDBWorker,
}

struct WriteCompletion {
    done: AtomicBool,
    result: Mutex<Option<Result<u64, StorageError>>>,
    notify: Notify,
}

const PENDING_WRITE_SEQUENCE: u64 = u64::MAX;

impl WriteCompletion {
    fn new() -> Self {
        Self {
            done: AtomicBool::new(false),
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    fn complete(&self, result: Result<u64, StorageError>) {
        *self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn wait(&self) -> Result<u64, StorageError> {
        loop {
            let notified = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return self
                    .result
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                    .expect("completed SlateDB write has a result");
            }
            notified.await;
        }
    }
}

impl WritePipeline {
    fn new() -> Self {
        Self::default()
    }

    async fn wait_for_visible(&self) -> Result<(), StorageError> {
        let tail = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tail
            .clone();
        if let Some(tail) = tail {
            tail.wait().await?;
        }
        self.terminal_error()
    }

    fn terminal_error(&self) -> Result<(), StorageError> {
        let error = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .terminal_error
            .clone();
        error.map_or(Ok(()), Err)
    }

    /// Obtains a snapshot together with the retirement guard its reader must
    /// hold until it captures a publication view.
    ///
    /// Both paths — cached and freshly fetched — return a `SnapshotFetch`, so
    /// the guard cannot be omitted by construction. `cleanup_publications`
    /// retires a publication only while `snapshot_fetches == 0`, and a reader
    /// holding a snapshot it has not yet captured a view against still depends
    /// on every publication newer than that snapshot. The cached fast path
    /// used to return no guard, which let cleanup retire a publication in
    /// exactly that gap; the reader then observed neither the snapshot value
    /// nor the overlay value, i.e. a stale point read or scan.
    async fn snapshot(
        &self,
        worker: &SlateDBWorker,
    ) -> Result<(Arc<DbSnapshot>, SnapshotFetch), StorageError> {
        let (cached, publication_id) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.snapshot_fetches = state
                .snapshot_fetches
                .checked_add(1)
                .expect("SlateDB snapshot fetch overflow");
            (state.latest_snapshot.clone(), state.next_publication_id)
        };
        // Constructed immediately after the increment so that every early
        // return below balances the counter through `SnapshotFetch::drop`.
        let fetch = SnapshotFetch {
            pipeline: self.clone(),
            worker: worker.clone(),
        };
        if let Some(snapshot) = cached {
            worker.check_open_fast()?;
            return Ok((snapshot, fetch));
        }
        let snapshot = worker
            .call_read(|db| async move { db.snapshot().await.map_err(slatedb_error) })
            .await?;
        if !self.try_install_snapshot(publication_id, &snapshot) {
            worker.check_open_fast()?;
        }
        Ok((snapshot, fetch))
    }

    /// Publishes `snapshot` as the cached snapshot when it is still current,
    /// reporting whether it was installed.
    ///
    /// The freshness test and the install happen under **one** acquisition of
    /// the pipeline lock. They used to be two, and a commit landing in the gap
    /// was silently undone: `commit` clears `latest_snapshot` and bumps
    /// `next_publication_id`, then the later install found `latest_snapshot`
    /// empty, passed its monotonic guard vacuously, and reinstated a snapshot
    /// that predated the write that commit had just published.
    fn try_install_snapshot(&self, publication_id: u64, snapshot: &Arc<DbSnapshot>) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cacheable = state.next_publication_id == publication_id
            && state
                .tail
                .as_ref()
                .is_none_or(|tail| tail.done.load(Ordering::Acquire))
            && snapshot_covers_persisted_publications(&state, snapshot.seq());
        if !cacheable {
            return false;
        }
        if state
            .latest_snapshot
            .as_ref()
            .is_none_or(|current| current.seq() <= snapshot.seq())
        {
            state.latest_snapshot = Some(Arc::clone(snapshot));
        }
        true
    }

    fn capture_with_worker(
        &self,
        worker: SlateDBWorker,
        snapshot_sequence: u64,
    ) -> PublicationView {
        self.capture_inner(Some(worker), snapshot_sequence)
    }

    #[cfg(test)]
    fn capture(&self, snapshot_sequence: u64) -> PublicationView {
        self.capture_inner(None, snapshot_sequence)
    }

    fn capture_inner(
        &self,
        worker: Option<SlateDBWorker>,
        snapshot_sequence: u64,
    ) -> PublicationView {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.newest_snapshot_sequence = state.newest_snapshot_sequence.max(snapshot_sequence);
        let retired = cleanup_publications(&mut state, snapshot_sequence);
        let publication_id = state.next_publication_id;
        *state
            .active_views
            .entry((snapshot_sequence, publication_id))
            .or_default() += 1;
        drop(state);
        if let Some(worker) = &worker {
            worker.defer_publication_drop(retired);
        }
        PublicationView {
            pipeline: self.clone(),
            worker,
            snapshot_sequence,
            publication_id,
        }
    }

    fn visible_writes(
        &self,
        snapshot_sequence: u64,
        publication_id: u64,
    ) -> Vec<Arc<PublishedWrite>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .visible
            .iter()
            .filter(|write| {
                let persisted = write.persisted_sequence.load(Ordering::Acquire);
                write.publication_id <= publication_id
                    && (persisted == PENDING_WRITE_SEQUENCE || persisted > snapshot_sequence)
            })
            .cloned()
            .collect()
    }

    fn point_value(
        &self,
        snapshot_sequence: u64,
        publication_id: u64,
        key: &Key,
    ) -> Option<Option<Bytes>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .visible
            .iter()
            .rev()
            .find_map(|write| {
                publication_visible_to_view(write, snapshot_sequence, publication_id)
                    .then(|| write.overlay.get(key).cloned())
                    .flatten()
            })
    }
}

impl Drop for PublicationView {
    fn drop(&mut self) {
        let mut state = self
            .pipeline
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = (self.snapshot_sequence, self.publication_id);
        let remove = state.active_views.get_mut(&key).is_some_and(|count| {
            *count -= 1;
            *count == 0
        });
        if remove {
            state.active_views.remove(&key);
        }
        let newest_snapshot_sequence = state.newest_snapshot_sequence;
        let retired = cleanup_publications(&mut state, newest_snapshot_sequence);
        drop(state);
        if let Some(worker) = &self.worker {
            worker.defer_publication_drop(retired);
        }
    }
}

impl Drop for SnapshotFetch {
    fn drop(&mut self) {
        let mut state = self
            .pipeline
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.snapshot_fetches = state
            .snapshot_fetches
            .checked_sub(1)
            .expect("SlateDB snapshot fetches should be balanced");
        let newest_snapshot_sequence = state.newest_snapshot_sequence;
        let retired = cleanup_publications(&mut state, newest_snapshot_sequence);
        drop(state);
        self.worker.defer_publication_drop(retired);
    }
}

fn publication_visible_to_view(
    write: &PublishedWrite,
    snapshot_sequence: u64,
    publication_id: u64,
) -> bool {
    let persisted = write.persisted_sequence.load(Ordering::Acquire);
    write.publication_id <= publication_id
        && (persisted == PENDING_WRITE_SEQUENCE || persisted > snapshot_sequence)
}

fn cleanup_publications(
    state: &mut WritePipelineState,
    newest_snapshot_sequence: u64,
) -> Vec<Arc<PublishedWrite>> {
    let mut retired = Vec::new();
    while state.visible.front().is_some_and(|write| {
        let persisted = write.persisted_sequence.load(Ordering::Acquire);
        state.snapshot_fetches == 0
            && persisted != PENDING_WRITE_SEQUENCE
            && persisted <= newest_snapshot_sequence
            && !state
                .active_views
                .keys()
                .any(|(snapshot_sequence, publication_id)| {
                    publication_visible_to_view(write, *snapshot_sequence, *publication_id)
                })
    }) {
        retired.push(
            state
                .visible
                .pop_front()
                .expect("front publication passed the retirement predicate"),
        );
    }
    retired
}

fn snapshot_covers_persisted_publications(
    state: &WritePipelineState,
    snapshot_sequence: u64,
) -> bool {
    state.visible.iter().all(|write| {
        let persisted = write.persisted_sequence.load(Ordering::Acquire);
        persisted != PENDING_WRITE_SEQUENCE && persisted <= snapshot_sequence
    })
}

fn write_pipeline_should_backpressure(pending_entries: usize, pending_bytes: usize) -> bool {
    pending_entries >= WRITE_PIPELINE_MAX_PENDING_ENTRIES
        || pending_bytes >= WRITE_PIPELINE_MAX_PENDING_BYTES
}

impl SnapshotPointCache {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(SnapshotPointCacheState::default())),
        }
    }

    /// `Some(None)` is a cached missing point; outer `None` is a cache miss.
    fn get(&self, sequence: u64, key: &Key) -> Option<Option<Bytes>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(&sequence)
            .and_then(|entries| entries.get(key))
            .map(|entry| entry.value.clone())
    }

    /// `Some(None)` is a cached missing point; outer `None` is a cache miss.
    ///
    /// A multi-key read does not mutate recency on hits, so inspect its whole
    /// snapshot-key set under one lock instead of acquiring the cache mutex
    /// once for every requested key.
    fn get_many(&self, sequence: u64, keys: &[Key], values: &mut [Option<Option<Bytes>>]) {
        debug_assert_eq!(keys.len(), values.len());
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entries = state.entries.get(&sequence);
        for (key, value) in keys.iter().zip(values) {
            *value = entries
                .and_then(|entries| entries.get(key))
                .map(|entry| entry.value.clone());
        }
    }

    fn insert(&self, sequence: u64, key: Key, value: Option<Bytes>) {
        // SlateDB values can retain an entire backing block. Copy cacheable
        // values so the cache's byte bound reflects the memory it owns.
        let value = value.map(|value| Bytes::copy_from_slice(&value));
        let value_bytes = value.as_ref().map_or(0, Bytes::len);
        if value_bytes > SNAPSHOT_POINT_CACHE_MAX_VALUE_BYTES {
            return;
        }
        let weight = key.0.len().saturating_add(value_bytes);
        if weight > SNAPSHOT_POINT_CACHE_BYTES {
            return;
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .entries
            .get(&sequence)
            .is_some_and(|entries| entries.contains_key(&key))
        {
            return;
        }
        while state.used_bytes.saturating_add(weight) > SNAPSHOT_POINT_CACHE_BYTES
            || state.eviction_order.len() >= SNAPSHOT_POINT_CACHE_ENTRIES
        {
            let Some(evicted_key) = state.eviction_order.pop_front() else {
                break;
            };
            let (evicted_weight, remove_sequence) = state
                .entries
                .get_mut(&evicted_key.sequence)
                .map_or((None, false), |entries| {
                    let evicted_weight = entries.remove(&evicted_key.key).map(|entry| entry.weight);
                    (evicted_weight, entries.is_empty())
                });
            if let Some(weight) = evicted_weight {
                state.used_bytes = state.used_bytes.saturating_sub(weight);
            }
            if remove_sequence {
                state.entries.remove(&evicted_key.sequence);
            }
        }
        state.used_bytes = state.used_bytes.saturating_add(weight);
        state.eviction_order.push_back(SnapshotPointCacheKey {
            sequence,
            key: key.clone(),
        });
        state
            .entries
            .entry(sequence)
            .or_default()
            .insert(key, SnapshotPointCacheValue { value, weight });
    }

    fn observe_snapshot(&self, sequence: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.current_sequence == Some(sequence) {
            return;
        }
        if !state.entries.contains_key(&sequence) {
            state.entries.clear();
            state.eviction_order.clear();
            state.used_bytes = 0;
        }
        state.current_sequence = Some(sequence);
    }

    fn advance_local_write(&self, sequence: u64, overlays: &[Arc<BTreeMap<Key, Option<Bytes>>>]) {
        let mut latest_values = BTreeMap::new();
        for overlay in overlays {
            latest_values.extend(
                overlay
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone())),
            );
        }

        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match state.current_sequence {
                Some(current) if current.checked_add(1) == Some(sequence) => {
                    if let Some(entries) = state.entries.remove(&current) {
                        state.entries.insert(sequence, entries);
                    }
                    for key in &mut state.eviction_order {
                        if key.sequence == current {
                            key.sequence = sequence;
                        }
                    }
                }
                Some(current) if sequence == current => {}
                _ => {
                    state.entries.clear();
                    state.eviction_order.clear();
                    state.used_bytes = 0;
                }
            }
            state.current_sequence = Some(sequence);

            for key in latest_values.keys() {
                let removed_weight = state
                    .entries
                    .get_mut(&sequence)
                    .and_then(|entries| entries.remove(key))
                    .map(|entry| entry.weight);
                if let Some(weight) = removed_weight {
                    state.used_bytes = state.used_bytes.saturating_sub(weight);
                }
            }
            state.eviction_order.retain(|entry| {
                entry.sequence != sequence || !latest_values.contains_key(&entry.key)
            });
            if state.entries.get(&sequence).is_some_and(HashMap::is_empty) {
                state.entries.remove(&sequence);
            }
        }

        for (key, value) in latest_values {
            self.insert(sequence, key, value);
        }
    }
}

impl Default for SlateDBFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SlateDBFactory {
    pub fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create slatedb storage temp dir"),
            next_database_id: AtomicU64::new(0),
        }
    }
}

impl StorageFactory for SlateDBFactory {
    type Storage = SlateDB;
    type Fixture = SlateDBFixture;

    fn create_fixture(&self) -> Self::Fixture {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        let path = self
            .temp_dir
            .path()
            .join(format!("storage-{database_id}.slatedb"));
        SlateDBFixture { path }
    }

    fn config(&self) -> StorageTestConfig {
        StorageTestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..StorageTestConfig::default()
        }
    }
}

impl StorageFixture for SlateDBFixture {
    type Storage = SlateDB;

    fn open(&self) -> impl Future<Output = Self::Storage> + Send {
        async move { SlateDB::open(&self.path).expect("open slatedb storage") }
    }
}

impl SlateDB {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        Self::open_local(path.into(), None)
    }

    #[doc(hidden)]
    pub fn open_with_io_counters(
        path: impl Into<PathBuf>,
        counters: SlateDBIoCounters,
    ) -> Result<Self, StorageError> {
        Self::open_local(path.into(), Some(counters))
    }

    fn open_local(
        path: PathBuf,
        counters: Option<SlateDBIoCounters>,
    ) -> Result<Self, StorageError> {
        std::fs::create_dir_all(&path).map_err(|error| {
            StorageError::Io(format!(
                "create slatedb storage directory {}: {error}",
                path.display()
            ))
        })?;
        let mut object_store: Arc<dyn ObjectStore> = Arc::new(DirectLocalReads {
            inner: LocalFileSystem::new_with_prefix(&path).map_err(object_store_error)?,
            files: Mutex::new(DirectLocalFileCache::default()),
        });
        let metrics = counters
            .as_ref()
            .map(|counters| Arc::clone(&counters.metrics));
        let diagnostics = counters.clone();
        if let Some(counters) = counters {
            object_store = Arc::new(CountingObjectStore {
                inner: object_store,
                counters,
            });
        }
        Self::open_object_store_with_read_dispatch(
            DB_PATH,
            object_store,
            SlateDBObjectStoreOptions::default(),
            true,
            metrics,
            diagnostics,
        )
        .map(|mut storage| {
            storage.path = path;
            storage
        })
    }

    pub fn open_object_store_with_options(
        db_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        options: SlateDBObjectStoreOptions,
    ) -> Result<Self, StorageError> {
        Self::open_object_store_with_read_dispatch(
            db_path,
            object_store,
            options,
            false,
            None,
            None,
        )
    }

    #[doc(hidden)]
    pub fn open_object_store_with_options_and_io_counters(
        db_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        options: SlateDBObjectStoreOptions,
        counters: SlateDBIoCounters,
    ) -> Result<Self, StorageError> {
        let metrics = Arc::clone(&counters.metrics);
        let object_store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore {
            inner: object_store,
            counters: counters.clone(),
        });
        Self::open_object_store_with_read_dispatch(
            db_path,
            object_store,
            options,
            false,
            Some(metrics),
            Some(counters),
        )
    }

    /// Opens SlateDB with a private current-thread read-dispatch choice.
    ///
    /// `SlateDB::open` is the only caller that enables it: LocalFileSystem
    /// moves filesystem work to Tokio's blocking pool before it can block a
    /// current-thread runtime. Generic ObjectStore implementations do not
    /// promise that property.
    fn open_object_store_with_read_dispatch(
        db_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        options: SlateDBObjectStoreOptions,
        read_on_caller_current_thread: bool,
        metrics: Option<Arc<DefaultMetricsRecorder>>,
        counters: Option<SlateDBIoCounters>,
    ) -> Result<Self, StorageError> {
        validate_object_store_options(&options)?;
        let db_path = db_path.into();
        let immutable_value_store = ImmutableValueStore::new(
            &db_path,
            Arc::clone(&object_store),
            options.cache.as_ref(),
            counters,
        );
        Ok(Self {
            worker: SlateDBWorker::start(
                db_path.clone(),
                object_store,
                options,
                read_on_caller_current_thread,
                metrics,
            )?,
            immutable_value_store,
            path: PathBuf::from(db_path),
            write_gate: WriteGate::new(),
            write_pipeline: WritePipeline::new(),
            point_cache: SnapshotPointCache::new(),
            startup_immutable_gc: StartupImmutableGc::default(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn flush(&self) -> Result<(), StorageError> {
        self.write_pipeline.wait_for_visible().await?;
        self.worker.wait_for_reclamation().await?;
        self.startup_immutable_gc.completed_result()?;
        let snapshot_sequence = self
            .worker
            .call(|db| async move {
                db.flush().await.map_err(slatedb_error)?;
                db.snapshot()
                    .await
                    .map(|snapshot| snapshot.seq())
                    .map_err(slatedb_error)
            })
            .await?;
        drop(
            self.write_pipeline
                .capture_with_worker(self.worker.clone(), snapshot_sequence),
        );
        self.worker.wait_for_reclamation().await?;
        self.startup_immutable_gc.completed_result()
    }

    /// Forces the active and immutable memtables into SSTs for storage-layout
    /// diagnostics.
    ///
    /// [`Self::flush`] intentionally follows upstream durability semantics and
    /// flushes only the WAL while WAL is enabled. Benchmarks must opt into this
    /// stronger lifecycle boundary when comparing memory-resident and SST
    /// scan shapes.
    #[doc(hidden)]
    pub async fn flush_memtable_for_diagnostics(&self) -> Result<(), StorageError> {
        self.write_pipeline.wait_for_visible().await?;
        self.worker.wait_for_reclamation().await?;
        self.startup_immutable_gc.completed_result()?;
        let snapshot_sequence = self
            .worker
            .call(|db| async move {
                db.flush_with_options(FlushOptions {
                    flush_type: FlushType::MemTable,
                })
                .await
                .map_err(slatedb_error)?;
                db.snapshot()
                    .await
                    .map(|snapshot| snapshot.seq())
                    .map_err(slatedb_error)
            })
            .await?;
        drop(
            self.write_pipeline
                .capture_with_worker(self.worker.clone(), snapshot_sequence),
        );
        self.worker.wait_for_reclamation().await?;
        self.startup_immutable_gc.completed_result()
    }
}

impl Storage for SlateDB {
    type Read<'a>
        = SlateDBRead
    where
        Self: 'a;

    type Write<'a>
        = SlateDBWrite
    where
        Self: 'a;

    fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        async move {
            self.write_pipeline.terminal_error()?;
            let (snapshot, snapshot_fetch) = self.write_pipeline.snapshot(&self.worker).await?;
            self.point_cache.observe_snapshot(snapshot.seq());
            let publication_view = if opts.durability == ReadDurability::Visible {
                Some(
                    self.write_pipeline
                        .capture_with_worker(self.worker.clone(), snapshot.seq()),
                )
            } else {
                None
            };
            drop(snapshot_fetch);
            self.write_pipeline.terminal_error()?;
            Ok(SlateDBRead {
                worker: self.worker.clone(),
                immutable_value_store: self.immutable_value_store.clone(),
                write_pipeline: self.write_pipeline.clone(),
                snapshot,
                publication_view,
                durability: opts.durability,
                point_cache: self.point_cache.clone(),
                #[cfg(test)]
                scan_worker_gate: None,
                #[cfg(test)]
                scan_hydration_gate: None,
            })
        }
    }

    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        async move {
            self.startup_immutable_gc.completed_result()?;
            self.startup_immutable_gc
                .schedule(&self.worker, &self.immutable_value_store);
            Ok(SlateDBWrite {
                worker: self.worker.clone(),
                immutable_value_store: self.immutable_value_store.clone(),
                write_pipeline: self.write_pipeline.clone(),
                point_cache: self.point_cache.clone(),
                write_gate: self.write_gate.clone(),
                writer_permit: None,
                preconditions: opts.preconditions,
                // The engine sets this only for the atomic mutation plus
                // idempotency-receipt commit. Its replay contract requires a
                // durable receipt before the request can be acknowledged.
                await_durable: opts.await_durable || opts.idempotency_key.is_some(),
                base: None,
                overlay: BTreeMap::new(),
                immutable_values: HashMap::new(),
                stats: WriteStats::default(),
            })
        }
    }
}

#[cfg(test)]
async fn collect_startup_immutable_garbage(
    worker: &SlateDBWorker,
    store: &ImmutableValueStore,
    cutoff: SystemTime,
) -> Result<(), StorageError> {
    let store = store.clone();
    worker
        .call_read(move |database| async move {
            collect_startup_immutable_garbage_from_database(database, &store, cutoff).await
        })
        .await
}

async fn collect_startup_immutable_garbage_from_database(
    database: Arc<Db>,
    store: &ImmutableValueStore,
    cutoff: SystemTime,
) -> Result<(), StorageError> {
    let snapshot = database.snapshot().await.map_err(slatedb_error)?;
    let scan_options = slatedb_scan_options(ReadDurability::Visible);
    let mut rows = snapshot
        .scan_with_options(.., &scan_options)
        .await
        .map_err(slatedb_error)?;
    let mut reachable = HashSet::new();
    while let Some(row) = rows.next().await.map_err(slatedb_error)? {
        if let Ok(locator) = decode_immutable_locator(&row.value) {
            reachable.insert(locator.segment_id);
        }
    }
    store.collect_unreachable(reachable, cutoff).await
}

async fn check_preconditions(
    worker: &SlateDBWorker,
    write_pipeline: &WritePipeline,
    point_cache: &SnapshotPointCache,
    immutable_value_store: &ImmutableValueStore,
    preconditions: &[Precondition],
) -> Result<(), StorageError> {
    if preconditions.is_empty() {
        return Ok(());
    }
    let preconditions = preconditions.to_vec();
    let write_pipeline = write_pipeline.clone();
    let read_pipeline = write_pipeline.clone();
    let point_cache = point_cache.clone();
    let immutable_value_store = immutable_value_store.clone();
    let (snapshot, snapshot_fetch) = write_pipeline.snapshot(worker).await?;
    let snapshot_sequence = snapshot.seq();
    point_cache.observe_snapshot(snapshot_sequence);
    let publication_view = read_pipeline.capture_with_worker(worker.clone(), snapshot_sequence);
    // Keep the fetch guard alive until the publication view is registered.
    // Otherwise a concurrent drainer can retire an overlay between obtaining
    // the snapshot and capturing the view that makes that overlay visible.
    drop(snapshot_fetch);
    let matches = worker
        .call_read(move |_db| async move {
            let publication_id = publication_view.publication_id;
            let mut matches = Vec::with_capacity(preconditions.len());
            let mut index = 0;
            while index < preconditions.len() {
                let start = index;
                let mut point_keys = Vec::new();
                while index < preconditions.len() {
                    let Some(key) = point_precondition_physical_key(&preconditions[index])? else {
                        break;
                    };
                    point_keys.push(key);
                    index += 1;
                }

                if !point_keys.is_empty() {
                    // A tracked mutation normally supplies a branch-head and a
                    // revision predicate (and idempotent mutations add a
                    // receipt predicate). Evaluate each contiguous point run
                    // against this snapshot in one read operation rather than
                    // serializing a worker entry for every predicate.
                    let mut values = get_cached_snapshot_values(
                        Arc::clone(&snapshot),
                        point_keys.clone(),
                        &point_cache,
                    )
                    .await?;
                    for (index, key) in point_keys.iter().enumerate() {
                        if let Some(value) =
                            read_pipeline.point_value(snapshot_sequence, publication_id, key)
                        {
                            values[index] = value;
                        }
                    }
                    let immutable_targets = preconditions[start..index]
                        .iter()
                        .enumerate()
                        .filter_map(|(offset, precondition)| match precondition {
                            Precondition::KeyValueHashEquals { space, .. }
                            | Precondition::KeyValueEquals { space, .. }
                                if space.value_semantics == ValueSemantics::Immutable =>
                            {
                                values[offset]
                                    .as_ref()
                                    .map(|marker| (offset, marker.clone()))
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    if !immutable_targets.is_empty() {
                        let immutable_values = immutable_value_store
                            .get_many(
                                immutable_targets
                                    .iter()
                                    .map(|(_, marker)| marker.clone())
                                    .collect(),
                            )
                            .await?;
                        for ((offset, _), value) in
                            immutable_targets.into_iter().zip(immutable_values)
                        {
                            values[offset] = Some(value);
                        }
                    }
                    matches.extend(values.iter().enumerate().map(|(offset, value)| {
                        point_precondition_matches(&preconditions[start + offset], value.as_ref())
                    }));
                    continue;
                }

                let matches_precondition = match &preconditions[index] {
                    Precondition::RangeEmpty { space, range } => {
                        let range = physical_range(space.id, range.clone())?;
                        let bounds = EncodedBounds::new(range.clone());
                        let mut keys = collect_snapshot_keys(Arc::clone(&snapshot), bounds).await?;
                        let visible_writes =
                            read_pipeline.visible_writes(snapshot_sequence, publication_id);
                        for write in &visible_writes {
                            for (key, value) in &*write.overlay {
                                if range_contains_key(&range, key) {
                                    if value.is_some() {
                                        if !keys.contains(key) {
                                            keys.push(key.clone());
                                        }
                                    } else {
                                        keys.retain(|candidate| candidate != key);
                                    }
                                }
                            }
                        }
                        keys.is_empty()
                    }
                    Precondition::BranchEquals { .. } => false,
                    Precondition::KeyAbsent { .. }
                    | Precondition::KeyPresent { .. }
                    | Precondition::KeyValueHashEquals { .. }
                    | Precondition::KeyValueEquals { .. } => {
                        unreachable!("point preconditions are collected above")
                    }
                };
                matches.push(matches_precondition);
                index += 1;
            }
            Ok(matches)
        })
        .await?;
    write_pipeline.terminal_error()?;
    let failures = matches
        .into_iter()
        .enumerate()
        .filter_map(|(index, matches)| (!matches).then_some(PreconditionFailure { index }))
        .collect::<Vec<_>>();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(StorageError::PreconditionFailed(failures))
    }
}

async fn get_cached_snapshot_values(
    snapshot: Arc<DbSnapshot>,
    keys: Vec<Key>,
    point_cache: &SnapshotPointCache,
) -> Result<Vec<Option<Bytes>>, StorageError> {
    let sequence = snapshot.seq();
    let mut values = vec![None; keys.len()];
    point_cache.get_many(sequence, &keys, &mut values);
    let missing = keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| values[index].is_none().then_some((index, key.clone())))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        let missing_keys = missing
            .iter()
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>();
        let fetched = get_snapshot_values(snapshot, missing_keys, ReadDurability::Visible).await?;
        for ((index, key), value) in missing.into_iter().zip(fetched) {
            point_cache.insert(sequence, key, value.clone());
            values[index] = Some(value);
        }
    }
    Ok(values
        .into_iter()
        .map(|value| value.expect("all SlateDB point-cache misses are filled"))
        .collect())
}

fn point_precondition_physical_key(
    precondition: &Precondition,
) -> Result<Option<Key>, StorageError> {
    match precondition {
        Precondition::KeyAbsent { space, key }
        | Precondition::KeyPresent { space, key }
        | Precondition::KeyValueHashEquals { space, key, .. }
        | Precondition::KeyValueEquals { space, key, .. } => physical_key(space.id, key).map(Some),
        Precondition::RangeEmpty { .. } | Precondition::BranchEquals { .. } => Ok(None),
    }
}

fn point_precondition_matches(precondition: &Precondition, value: Option<&Bytes>) -> bool {
    match precondition {
        Precondition::KeyAbsent { .. } => value.is_none(),
        Precondition::KeyPresent { .. } => value.is_some(),
        Precondition::KeyValueHashEquals { hash, .. } => {
            value.is_some_and(|value| blake3::hash(value.as_ref()).as_bytes() == hash)
        }
        Precondition::KeyValueEquals { expected, .. } => {
            value.is_some_and(|value| value == expected)
        }
        Precondition::RangeEmpty { .. } | Precondition::BranchEquals { .. } => {
            unreachable!("only point preconditions have batched snapshot values")
        }
    }
}

/// `ValueIntegrity::ContentAddressed` is a no-op on this adapter, deliberately.
///
/// The declaration tells a backend it *may* skip its own value checksum,
/// because the engine recomputes the value's BLAKE3-256 digest from its key on
/// every full-value read. Acting on it is an optimisation, never an
/// obligation, and SlateDB has nothing to act with: `slatedb::config::ReadOptions`
/// exposes `durability_filter`, `dirty`, `cache_blocks` and `filter_context`
/// and no checksum control at any level. Immutable values also leave the LSM
/// entirely for `db/lix-immutable-value-segment-v1`, where integrity is the
/// object store's rather than a per-read setting.
///
/// So SlateDB keeps verifying exactly as before and stays correct by doing
/// nothing. This comment exists so the asymmetry with `packages/rocksdb-storage`
/// reads as a decision rather than as a missed call site.
impl StorageRead for SlateDBRead {
    fn snapshot_cache_key(&self) -> Option<u128> {
        let publication_id = self
            .publication_view
            .as_ref()
            .map_or(0, |view| view.publication_id);
        Some((u128::from(self.snapshot.seq()) << 64) | u128::from(publication_id))
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        async move {
            self.write_pipeline.terminal_error()?;
            if let [request] = requests
                && let [key] = request.keys
            {
                let key = physical_key(request.space.id, key)?;
                let snapshot = Arc::clone(&self.snapshot);
                let durability = self.durability;
                let mut value = if durability == ReadDurability::Visible {
                    let sequence = snapshot.seq();
                    let cache = self.point_cache.clone();
                    if let Some(value) = cache.get(sequence, &key) {
                        self.worker.check_open_fast()?;
                        value
                    } else {
                        let fetched_key = key.clone();
                        let value = self
                            .worker
                            .call_read(move |_db| {
                                get_snapshot_value(snapshot, fetched_key, durability)
                            })
                            .await?;
                        cache.insert(sequence, key.clone(), value.clone());
                        value
                    }
                } else {
                    let read_key = key.clone();
                    self.worker
                        .call_read(move |_db| get_snapshot_value(snapshot, read_key, durability))
                        .await?
                };
                if let Some(view) = &self.publication_view
                    && let Some(published) = self.write_pipeline.point_value(
                        view.snapshot_sequence,
                        view.publication_id,
                        &key,
                    )
                {
                    value = published;
                }
                let mut results =
                    vec![value.map(|value| project_value(value, request.opts.projection))];
                hydrate_immutable_value_gets(&self.immutable_value_store, requests, &mut results)
                    .await?;
                return Ok(GetManyResult::new(results));
            }

            let mut physical_keys = Vec::with_capacity(
                requests
                    .iter()
                    .map(|request| request.keys.len())
                    .sum::<usize>(),
            );
            for request in requests {
                for key in request.keys {
                    physical_keys.push(physical_key(request.space.id, key)?);
                }
            }
            if physical_keys.is_empty() {
                return Ok(GetManyResult::new(Vec::new()));
            }

            let snapshot = Arc::clone(&self.snapshot);
            let durability = self.durability;
            let mut values = if durability == ReadDurability::Visible {
                let sequence = snapshot.seq();
                let cache = self.point_cache.clone();
                let mut values = vec![None; physical_keys.len()];
                let mut missing = Vec::new();
                cache.get_many(sequence, &physical_keys, &mut values);
                for (index, key) in physical_keys.iter().enumerate() {
                    if values[index].is_none() {
                        missing.push((index, key.clone()));
                    }
                }
                if missing.is_empty() {
                    self.worker.check_open_fast()?;
                } else {
                    let missing_keys = missing
                        .iter()
                        .map(|(_, key)| key.clone())
                        .collect::<Vec<_>>();
                    let fetched = self
                        .worker
                        .call_read(move |_db| {
                            get_snapshot_values(snapshot, missing_keys, durability)
                        })
                        .await?;
                    for ((index, key), value) in missing.into_iter().zip(fetched) {
                        cache.insert(sequence, key, value.clone());
                        values[index] = Some(value);
                    }
                }
                values
                    .into_iter()
                    .map(|value| value.expect("all SlateDB batch point-cache misses are filled"))
                    .collect::<Vec<_>>()
            } else {
                let read_keys = physical_keys.clone();
                self.worker
                    .call_read(move |_db| get_snapshot_values(snapshot, read_keys, durability))
                    .await?
            };
            for (key, value) in physical_keys.iter().zip(&mut values) {
                if let Some(view) = &self.publication_view
                    && let Some(published) = self.write_pipeline.point_value(
                        view.snapshot_sequence,
                        view.publication_id,
                        key,
                    )
                {
                    *value = published;
                }
            }

            let mut values = values.into_iter();
            let mut results = Vec::with_capacity(physical_keys.len());
            for request in requests {
                results.extend(
                    values.by_ref().take(request.keys.len()).map(|value| {
                        value.map(|value| project_value(value, request.opts.projection))
                    }),
                );
            }
            let unexpected_value = values.next();
            debug_assert!(unexpected_value.is_none());
            hydrate_immutable_value_gets(&self.immutable_value_store, requests, &mut results)
                .await?;
            Ok(GetManyResult::new(results))
        }
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<StorageScanCursor<'_>, StorageError>> + Send {
        async move {
            self.write_pipeline.terminal_error()?;
            StorageScanCursor::validate_range(&range)?;
            if opts.order == ScanOrder::Descending {
                return Err(StorageError::Unsupported(Capability::ReverseScan));
            }
            let bounds = EncodedBounds::new(physical_range(space.id, range.clone())?);
            let visible_writes = self
                .publication_view
                .as_ref()
                .map_or_else(Vec::new, |view| {
                    self.write_pipeline
                        .visible_writes(view.snapshot_sequence, view.publication_id)
                });
            let state = if bounds.is_empty() {
                SlateStreamingScanState::empty(bounds, visible_writes)
            } else {
                let snapshot = Arc::clone(&self.snapshot);
                let durability = self.durability;
                let scan_bounds = bounds.clone();
                self.worker
                    .call_read(move |_db| async move {
                        let iter = open_snapshot_scan(snapshot, scan_bounds, durability).await?;
                        Ok(SlateStreamingScanState::new(iter, bounds, visible_writes))
                    })
                    .await?
            };
            StorageScanCursor::from_source(
                range,
                opts.order,
                SlateDBScanSource {
                    worker: self.worker.clone(),
                    immutable_value_store: self.immutable_value_store.clone(),
                    write_pipeline: self.write_pipeline.clone(),
                    space,
                    projection: opts.projection,
                    state: Some(state),
                    #[cfg(test)]
                    worker_gate: self.scan_worker_gate.clone(),
                    #[cfg(test)]
                    hydration_gate: self.scan_hydration_gate.clone(),
                },
            )
        }
    }
}

struct SlateDBScanSource {
    worker: SlateDBWorker,
    immutable_value_store: ImmutableValueStore,
    write_pipeline: WritePipeline,
    space: StorageSpace,
    projection: CoreProjection,
    state: Option<SlateStreamingScanState>,
    #[cfg(test)]
    worker_gate: Option<Arc<ScanTestGate>>,
    #[cfg(test)]
    hydration_gate: Option<Arc<ScanTestGate>>,
}

impl StorageScanSource for SlateDBScanSource {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            self.write_pipeline.terminal_error()?;
            let state = self.state.take().ok_or(StorageError::InvalidCursor)?;
            let projection = self.projection;
            let space_id = self.space.id;
            #[cfg(test)]
            let worker_gate = self.worker_gate.clone();
            let (state, chunk) = self
                .worker
                .call_read(move |_db| async move {
                    #[cfg(test)]
                    if let Some(gate) = worker_gate {
                        gate.entered.store(true, Ordering::Release);
                        gate.entered_notify.notify_waiters();
                        gate.release.notified().await;
                    }
                    streaming_scan_page(state, limit_rows, projection, space_id).await
                })
                .await?;
            self.state = Some(state);
            #[cfg(test)]
            if let Some(gate) = &self.hydration_gate {
                gate.entered.store(true, Ordering::Release);
                gate.entered_notify.notify_waiters();
                gate.release.notified().await;
            }
            let (mut entries, has_more) = chunk.into_parts();
            hydrate_immutable_value_scan(
                &self.immutable_value_store,
                self.space,
                self.projection,
                &mut entries,
            )
            .await?;
            Ok(ScanChunk::new(entries, has_more))
        })
    }
}

struct SlateStreamingScanState {
    iter: Option<DbIterator>,
    base_pending: Option<KeyValue>,
    overlays: StreamingOverlayCursor,
    output_pending: Option<(Key, Bytes)>,
}

impl SlateStreamingScanState {
    fn new(
        iter: DbIterator,
        bounds: EncodedBounds,
        visible_writes: Vec<Arc<PublishedWrite>>,
    ) -> Self {
        Self {
            iter: Some(iter),
            base_pending: None,
            overlays: StreamingOverlayCursor::new(bounds, visible_writes),
            output_pending: None,
        }
    }

    fn empty(bounds: EncodedBounds, visible_writes: Vec<Arc<PublishedWrite>>) -> Self {
        Self {
            iter: None,
            base_pending: None,
            overlays: StreamingOverlayCursor::new(bounds, visible_writes),
            output_pending: None,
        }
    }
}

struct StreamingOverlayCursor {
    writes: Vec<Arc<PublishedWrite>>,
    upper: Bound<Key>,
    heads: BinaryHeap<OverlayHeapEntry>,
    pending: Option<(Key, Option<Bytes>)>,
}

#[derive(Eq, PartialEq)]
struct OverlayHeapEntry {
    key: Key,
    publication_id: u64,
    write_index: usize,
}

impl Ord for OverlayHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| self.publication_id.cmp(&other.publication_id))
            .then_with(|| self.write_index.cmp(&other.write_index))
    }
}

impl PartialOrd for OverlayHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl StreamingOverlayCursor {
    fn new(bounds: EncodedBounds, writes: Vec<Arc<PublishedWrite>>) -> Self {
        let lower = bound_vec_to_key(&bounds.lower);
        let mut cursor = Self {
            writes,
            upper: bound_vec_to_key(&bounds.upper),
            heads: BinaryHeap::new(),
            pending: None,
        };
        for write_index in 0..cursor.writes.len() {
            cursor.push_next(write_index, lower.clone());
        }
        cursor
    }

    fn peek(&mut self) -> Option<&(Key, Option<Bytes>)> {
        if self.pending.is_none() {
            let next = self.find_next();
            self.pending = next;
        }
        self.pending.as_ref()
    }

    fn take(&mut self) -> Option<(Key, Option<Bytes>)> {
        let _ = self.peek();
        self.pending.take()
    }

    fn find_next(&mut self) -> Option<(Key, Option<Bytes>)> {
        let first = self.heads.pop()?;
        let key = first.key.clone();
        let mut equal_heads = vec![first];
        while self.heads.peek().is_some_and(|head| head.key == key) {
            equal_heads.push(self.heads.pop().expect("peeked overlay head exists"));
        }
        let winner = equal_heads
            .iter()
            .max_by_key(|head| head.publication_id)
            .expect("at least one overlay head exists")
            .write_index;
        let value = self.writes[winner]
            .overlay
            .get(&key)
            .cloned()
            .expect("overlay head remains bound to its immutable publication");
        for head in equal_heads {
            self.push_next(head.write_index, Bound::Excluded(key.clone()));
        }
        Some((key, value))
    }

    fn push_next(&mut self, write_index: usize, lower: Bound<Key>) {
        if key_bounds_are_empty(&lower, &self.upper) {
            return;
        }
        let Some((key, _)) = self.writes[write_index]
            .overlay
            .range((lower, self.upper.clone()))
            .next()
        else {
            return;
        };
        self.heads.push(OverlayHeapEntry {
            key: key.clone(),
            publication_id: self.writes[write_index].publication_id,
            write_index,
        });
    }
}

fn key_bounds_are_empty(lower: &Bound<Key>, upper: &Bound<Key>) -> bool {
    match (lower, upper) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(lower), Bound::Included(upper)) => lower > upper,
        (Bound::Included(lower) | Bound::Excluded(lower), Bound::Excluded(upper))
        | (Bound::Excluded(lower), Bound::Included(upper)) => lower >= upper,
    }
}

fn bound_vec_to_key(bound: &Bound<Vec<u8>>) -> Bound<Key> {
    match bound {
        Bound::Included(key) => Bound::Included(Key(Bytes::copy_from_slice(key))),
        Bound::Excluded(key) => Bound::Excluded(Key(Bytes::copy_from_slice(key))),
        Bound::Unbounded => Bound::Unbounded,
    }
}

async fn streaming_scan_page(
    mut state: SlateStreamingScanState,
    limit_rows: usize,
    projection: CoreProjection,
    space_id: SpaceId,
) -> Result<(SlateStreamingScanState, ScanChunk), StorageError> {
    let mut rows = Vec::with_capacity(limit_rows);
    if let Some(row) = state.output_pending.take() {
        rows.push(row);
    }
    while rows.len() < limit_rows {
        let Some(row) = next_streaming_visible_row(&mut state).await? else {
            break;
        };
        rows.push(row);
    }
    state.output_pending = next_streaming_visible_row(&mut state).await?;
    let has_more = state.output_pending.is_some();
    let entries = rows
        .into_iter()
        .map(|(key, value)| {
            if key.0.len() < SPACE_PREFIX_LEN
                || key.0[..SPACE_PREFIX_LEN] != space_id.0.to_be_bytes()
            {
                return Err(StorageError::Corruption(format!(
                    "slatedb scan key escaped its storage space: {:?}",
                    key.0
                )));
            }
            Ok(ReadEntry {
                key: Key(key.0.slice(SPACE_PREFIX_LEN..)),
                value: project_value(value, projection),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((state, ScanChunk::new(entries, has_more)))
}

async fn next_streaming_visible_row(
    state: &mut SlateStreamingScanState,
) -> Result<Option<(Key, Bytes)>, StorageError> {
    loop {
        if state.base_pending.is_none()
            && let Some(iter) = &mut state.iter
        {
            state.base_pending = iter.next().await.map_err(slatedb_error)?;
        }
        let decision = match (state.base_pending.as_ref(), state.overlays.peek()) {
            (Some(base), Some((overlay_key, _))) => match base.key.cmp(&overlay_key.0) {
                std::cmp::Ordering::Less => StreamingMergeDecision::Base,
                std::cmp::Ordering::Equal => StreamingMergeDecision::OverlayAndBase,
                std::cmp::Ordering::Greater => StreamingMergeDecision::Overlay,
            },
            (Some(_), None) => StreamingMergeDecision::Base,
            (None, Some(_)) => StreamingMergeDecision::Overlay,
            (None, None) => return Ok(None),
        };
        let row = match decision {
            StreamingMergeDecision::Base => state
                .base_pending
                .take()
                .map(|row| (Key(row.key), Some(row.value))),
            StreamingMergeDecision::Overlay => state.overlays.take(),
            StreamingMergeDecision::OverlayAndBase => {
                state.base_pending.take();
                state.overlays.take()
            }
        };
        if let Some((key, Some(value))) = row {
            return Ok(Some((key, value)));
        }
    }
}

#[derive(Clone, Copy)]
enum StreamingMergeDecision {
    Base,
    Overlay,
    OverlayAndBase,
}

async fn hydrate_immutable_value_gets(
    immutable_value_store: &ImmutableValueStore,
    requests: &[GetManyRequest<'_>],
    results: &mut [Option<ProjectedValue>],
) -> Result<(), StorageError> {
    let mut targets = Vec::new();
    let mut result_index = 0usize;
    for request in requests {
        for _ in request.keys {
            if request.space.value_semantics == ValueSemantics::Immutable
                && request.opts.projection == CoreProjection::FullValue
                && let Some(Some(ProjectedValue::FullValue(marker))) = results.get(result_index)
            {
                targets.push((result_index, marker.clone()));
            }
            result_index += 1;
        }
    }
    if targets.is_empty() {
        return Ok(());
    }
    let values = immutable_value_store
        .get_many(targets.iter().map(|(_, marker)| marker.clone()).collect())
        .await?;
    for ((result_index, _), value) in targets.into_iter().zip(values) {
        results[result_index] = Some(ProjectedValue::FullValue(value));
    }
    Ok(())
}

async fn hydrate_immutable_value_scan(
    immutable_value_store: &ImmutableValueStore,
    space: StorageSpace,
    projection: CoreProjection,
    entries: &mut [ReadEntry],
) -> Result<(), StorageError> {
    if space.value_semantics != ValueSemantics::Immutable || projection != CoreProjection::FullValue
    {
        return Ok(());
    }
    let values = immutable_value_store
        .get_many(
            entries
                .iter()
                .map(|entry| match &entry.value {
                    ProjectedValue::FullValue(marker) => Ok(marker.clone()),
                    ProjectedValue::KeyOnly => Err(StorageError::Corruption(
                        "immutable full-value scan returned a key-only marker".to_string(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .await?;
    for (entry, value) in entries.iter_mut().zip(values) {
        entry.value = ProjectedValue::FullValue(value);
    }
    Ok(())
}

async fn read_visible_immutable_values(
    worker: &SlateDBWorker,
    write_pipeline: &WritePipeline,
    point_cache: &SnapshotPointCache,
    immutable_value_store: &ImmutableValueStore,
    keys: Vec<Key>,
) -> Result<Vec<Option<Bytes>>, StorageError> {
    let (snapshot, snapshot_fetch) = write_pipeline.snapshot(worker).await?;
    let snapshot_sequence = snapshot.seq();
    point_cache.observe_snapshot(snapshot_sequence);
    let publication_view = write_pipeline.capture_with_worker(worker.clone(), snapshot_sequence);
    drop(snapshot_fetch);
    let point_cache = point_cache.clone();
    let write_pipeline = write_pipeline.clone();
    let immutable_value_store = immutable_value_store.clone();
    worker
        .call_read(move |_db| async move {
            let mut values =
                get_cached_snapshot_values(snapshot, keys.clone(), &point_cache).await?;
            for (index, key) in keys.iter().enumerate() {
                if let Some(published) = write_pipeline.point_value(
                    snapshot_sequence,
                    publication_view.publication_id,
                    key,
                ) {
                    values[index] = published;
                }
            }
            let targets = values
                .iter()
                .enumerate()
                .filter_map(|(index, marker)| marker.clone().map(|marker| (index, marker)))
                .collect::<Vec<_>>();
            if !targets.is_empty() {
                let hydrated = immutable_value_store
                    .get_many(targets.iter().map(|(_, marker)| marker.clone()).collect())
                    .await?;
                for ((index, _), value) in targets.into_iter().zip(hydrated) {
                    values[index] = Some(value);
                }
            }
            Ok(values)
        })
        .await
}

impl SlateDBWrite {
    async fn serialize_publication(&mut self) -> Result<(), StorageError> {
        if self.writer_permit.is_some() {
            return Ok(());
        }
        let wait_started = Instant::now();
        let permit = self.write_gate.acquire().await;
        if let Some(counters) = &self.immutable_value_store.counters {
            counters
                .inner
                .writer_gate_acquisitions
                .fetch_add(1, Ordering::Relaxed);
            counters.inner.writer_gate_wait_nanos.fetch_add(
                wait_started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                Ordering::Relaxed,
            );
        }
        check_preconditions(
            &self.worker,
            &self.write_pipeline,
            &self.point_cache,
            &self.immutable_value_store,
            &self.preconditions,
        )
        .await?;
        if !self.immutable_values.is_empty() {
            let keys = self.immutable_values.keys().cloned().collect::<Vec<_>>();
            let existing = read_visible_immutable_values(
                &self.worker,
                &self.write_pipeline,
                &self.point_cache,
                &self.immutable_value_store,
                keys.clone(),
            )
            .await?;
            for (key, existing) in keys.into_iter().zip(existing) {
                let Some(existing) = existing else {
                    continue;
                };
                if self.immutable_values.get(&key) != Some(&existing) {
                    return Err(StorageError::Corruption(
                        "immutable identity was assigned different bytes".to_string(),
                    ));
                }
                // An identical immutable assignment is idempotent. Keep the
                // existing locator rather than publishing a replacement.
                self.overlay.remove(&key);
            }
        }
        self.writer_permit = Some(permit);
        Ok(())
    }
}

impl StorageWrite for SlateDBWrite {
    fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            if space.value_semantics == ValueSemantics::Immutable {
                let put_entries = entries.entries.len() as u64;
                let mut segment_writer = ImmutableSegmentWriter::default();
                let mut written_bytes = 0_u64;
                let staged_entries = entries
                    .entries
                    .into_iter()
                    .map(|entry| {
                        Ok((
                            physical_key(space.id, &entry.key)?,
                            stored_value_bytes(entry.value),
                        ))
                    })
                    .collect::<Result<Vec<_>, StorageError>>()?;
                let visible_values = if self.writer_permit.is_some() {
                    read_visible_immutable_values(
                        &self.worker,
                        &self.write_pipeline,
                        &self.point_cache,
                        &self.immutable_value_store,
                        staged_entries.iter().map(|(key, _)| key.clone()).collect(),
                    )
                    .await?
                    .into_iter()
                    .collect::<Vec<_>>()
                } else {
                    vec![None; staged_entries.len()]
                };
                for ((physical_key, value), visible) in
                    staged_entries.into_iter().zip(visible_values)
                {
                    written_bytes = written_bytes.saturating_add(value.len() as u64);
                    if let Some(existing) = visible {
                        if existing != value {
                            return Err(StorageError::Corruption(
                                "immutable identity was assigned different bytes".to_string(),
                            ));
                        }
                        self.overlay.remove(&physical_key);
                        continue;
                    }
                    if let Some(existing) = self.immutable_values.get(&physical_key) {
                        if existing != &value {
                            return Err(StorageError::Corruption(
                                "immutable identity was assigned different bytes".to_string(),
                            ));
                        }
                        continue;
                    }
                    self.immutable_values
                        .insert(physical_key.clone(), value.clone());
                    segment_writer.insert(physical_key, value)?;
                }
                let immutable_segments = segment_writer.finish(|_| true)?;
                let immutable_locators = immutable_segments
                    .iter()
                    .flat_map(|segment| {
                        let segment_len = segment.values.last().map_or(0, |(_, range)| range.end);
                        segment.values.iter().map(move |(physical_key, range)| {
                            let locator = ImmutableValueLocator {
                                segment_id: segment.id.clone(),
                                segment_len,
                                range: range.clone(),
                            };
                            encode_immutable_locator(&locator)
                                .map(|locator| (physical_key.clone(), locator))
                        })
                    })
                    .collect::<Result<Vec<_>, StorageError>>()?;
                if let Some(counters) = &self.immutable_value_store.counters {
                    counters
                        .inner
                        .immutable_locator_rows
                        .fetch_add(immutable_locators.len() as u64, Ordering::Relaxed);
                }
                self.immutable_value_store
                    .put_segments(immutable_segments)
                    .await?;
                for (physical_key, locator) in immutable_locators {
                    self.overlay.insert(physical_key, Some(locator));
                }
                self.stats.put_entries += put_entries;
                self.stats.written_bytes += written_bytes;
                self.stats.storage_calls += 1;
                return Ok(());
            }
            let physical_key_bytes = entries.entries.iter().try_fold(0_usize, |total, entry| {
                let key_len = SPACE_PREFIX_LEN
                    .checked_add(entry.key.0.len())
                    .ok_or(StorageError::InvalidKey)?;
                if key_len > MAX_SLATEDB_KEY_LEN {
                    return Err(StorageError::InvalidKey);
                }
                total.checked_add(key_len).ok_or(StorageError::InvalidKey)
            })?;
            let space_prefix = space.id.0.to_be_bytes();
            let mut physical_keys = Vec::with_capacity(physical_key_bytes);
            for entry in &entries.entries {
                physical_keys.extend_from_slice(&space_prefix);
                physical_keys.extend_from_slice(&entry.key.0);
            }
            let physical_keys = Bytes::from(physical_keys);
            let mut key_start = 0;
            for entry in entries.entries {
                let key_end = key_start + SPACE_PREFIX_LEN + entry.key.0.len();
                let key = Key(physical_keys.slice(key_start..key_end));
                key_start = key_end;
                let value = stored_value_bytes(entry.value);
                self.stats.put_entries += 1;
                self.stats.written_bytes += value.len() as u64;
                self.overlay.insert(key, Some(value));
            }
            self.stats.storage_calls += 1;
            Ok(())
        }
    }

    fn delete_many(
        &mut self,
        space: StorageSpace,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            let physical_key_bytes = keys.iter().try_fold(0_usize, |total, key| {
                let key_len = SPACE_PREFIX_LEN
                    .checked_add(key.0.len())
                    .ok_or(StorageError::InvalidKey)?;
                if key_len > MAX_SLATEDB_KEY_LEN {
                    return Err(StorageError::InvalidKey);
                }
                total.checked_add(key_len).ok_or(StorageError::InvalidKey)
            })?;
            let space_prefix = space.id.0.to_be_bytes();
            let mut physical_keys = Vec::with_capacity(physical_key_bytes);
            for key in keys {
                physical_keys.extend_from_slice(&space_prefix);
                physical_keys.extend_from_slice(&key.0);
            }
            let physical_keys = Bytes::from(physical_keys);
            let mut key_start = 0;
            for key in keys {
                let key_end = key_start + SPACE_PREFIX_LEN + key.0.len();
                self.overlay
                    .insert(Key(physical_keys.slice(key_start..key_end)), None);
                key_start = key_end;
            }
            self.stats.deleted_entries += keys.len() as u64;
            self.stats.storage_calls += 1;
            Ok(())
        }
    }

    fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        async move {
            self.serialize_publication().await?;
            let range = physical_range(space.id, range)?;
            let bounds = EncodedBounds::new(range.clone());
            if bounds.is_empty() {
                self.stats.deleted_ranges += 1;
                self.stats.storage_calls += 1;
                return Ok(());
            }

            // Snapshot discovery is read-only until both awaits complete and
            // the overlay is updated below, so a cancelled caller can safely
            // release this work instead of holding worker shutdown open.
            if self.base.is_none() {
                self.write_pipeline.wait_for_visible().await?;
                self.base = Some(
                    self.worker
                        .call_read(|db| async move { db.snapshot().await.map_err(slatedb_error) })
                        .await?,
                );
            }
            let base = Arc::clone(
                self.base
                    .as_ref()
                    .expect("SlateDB write base snapshot is initialized"),
            );
            let base_keys = self
                .worker
                .call_read(move |_db| collect_snapshot_keys(base, bounds))
                .await?;

            let overlay_keys = self
                .overlay
                .keys()
                .filter(|key| range_contains_key(&range, key))
                .cloned()
                .collect::<Vec<_>>();
            let staged_puts_in_range = overlay_keys
                .iter()
                .filter(|key| self.overlay.get(*key).is_some_and(Option::is_some))
                .count();

            for key in overlay_keys.into_iter().chain(base_keys.iter().cloned()) {
                self.overlay.insert(key, None);
            }

            self.stats.deleted_entries += (base_keys.len() + staged_puts_in_range) as u64;
            self.stats.deleted_ranges += 1;
            self.stats.storage_calls += 1;
            Ok(())
        }
    }

    fn commit(self) -> impl Future<Output = Result<CommitResult, StorageError>> + Send {
        async move {
            let mut this = self;
            this.serialize_publication().await?;
            let Self {
                worker,
                write_pipeline,
                point_cache,
                writer_permit,
                await_durable,
                overlay,
                stats,
                ..
            } = this;
            let writer_permit = writer_permit.expect("SlateDB commit owns the writer permit");
            if overlay.is_empty() {
                return Ok(CommitResult {
                    commit_id: None,
                    stats,
                });
            }

            worker.check_open()?;
            write_pipeline.terminal_error()?;
            let overlay_entries = overlay.len();
            let overlay_bytes = overlay
                .iter()
                .map(|(key, value)| {
                    key.0
                        .len()
                        .saturating_add(value.as_ref().map_or(0, Bytes::len))
                })
                .sum::<usize>();
            let overlay = Arc::new(overlay);
            let completion = Arc::new(WriteCompletion::new());
            let (start_drainer, apply_backpressure) = {
                let mut state = write_pipeline
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // A cached snapshot remains logically correct through the
                // publication overlay, but it must not outlive that overlay
                // after persistence. Force the next reader to fetch a current
                // snapshot; `snapshot` caches it only when no publication
                // raced the fetch and the write tail is complete.
                state.latest_snapshot = None;
                state.next_publication_id = state
                    .next_publication_id
                    .checked_add(1)
                    .expect("SlateDB publication id overflow");
                let publication_id = state.next_publication_id;
                let published = Arc::new(PublishedWrite {
                    publication_id,
                    overlay: Arc::clone(&overlay),
                    persisted_sequence: AtomicU64::new(PENDING_WRITE_SEQUENCE),
                });
                state.tail = Some(Arc::clone(&completion));
                state.visible.push_back(Arc::clone(&published));
                state.pending_entries = state.pending_entries.saturating_add(overlay_entries);
                state.pending_bytes = state.pending_bytes.saturating_add(overlay_bytes);
                state.queued.push_back(QueuedWrite {
                    overlay,
                    published,
                    completion: Arc::clone(&completion),
                    await_durable,
                    weight_bytes: overlay_bytes,
                });
                let start_drainer = !state.draining;
                state.draining = true;
                let apply_backpressure =
                    write_pipeline_should_backpressure(state.pending_entries, state.pending_bytes);
                (start_drainer, apply_backpressure)
            };

            if start_drainer {
                let task_pipeline = write_pipeline.clone();
                let publication_reclaimer = worker.publication_reclaimer();
                worker.spawn(move |db| {
                    drain_write_queue(db, task_pipeline, point_cache, publication_reclaimer)
                });
            }

            // The writer gate protects precondition evaluation plus publication
            // into the ordered adapter pipeline. Once published, later writers
            // observe this overlay without waiting for SlateDB's task rendezvous,
            // except at the high-water mark where the gate bounds queued memory.
            if apply_backpressure {
                completion.wait().await?;
            }
            drop(writer_permit);
            if await_durable && !apply_backpressure {
                completion.wait().await?;
            }
            Ok(CommitResult {
                commit_id: None,
                stats,
            })
        }
    }

    fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send {
        async { Ok(()) }
    }
}

async fn drain_write_queue(
    db: Arc<Db>,
    pipeline: WritePipeline,
    point_cache: SnapshotPointCache,
    publication_reclaimer: PublicationReclaimer,
) {
    loop {
        let writes = {
            let mut state = pipeline
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.queued.is_empty() {
                state.draining = false;
                return;
            }
            state.queued.drain(..).collect::<Vec<_>>()
        };

        let prior_error = pipeline.terminal_error();
        let result = if let Err(error) = prior_error {
            Err(error)
        } else {
            let mut batch = WriteBatch::new();
            let await_durable = writes.iter().any(|write| write.await_durable);
            for write in &writes {
                for (key, value) in &*write.overlay {
                    match value {
                        Some(value) => batch.put_bytes(key.0.clone(), value.clone()),
                        None => batch.delete(key.0.clone()),
                    }
                }
            }
            // SlateDB's own `await_durable` write option does not ask for a WAL
            // flush; it only parks on the WAL buffer's durability watcher, which
            // fires either when the buffer exceeds `l0_sst_size_bytes` (64 MiB)
            // or when the periodic `flush_interval` ticker (100 ms) elapses.
            // Every durable commit under that ceiling therefore paid up to a
            // full 100 ms tick of pure idle latency for a WAL write that costs
            // microseconds. Enqueue the batch without waiting, then ask the
            // batch writer to flush now. The flush message is ordered behind
            // this batch on the same writer channel, so it carries exactly the
            // same durability guarantee — the WAL SST is in the object store
            // before the commit is acknowledged — and one flush covers every
            // durable write in the drained group, amortizing the barrier
            // across a concurrent window instead of serializing on the ticker.
            async {
                let handle = db
                    .write_with_options(
                        batch,
                        &SlateDBWriteOptions {
                            await_durable: false,
                            ..SlateDBWriteOptions::default()
                        },
                    )
                    .await?;
                if await_durable {
                    db.flush().await?;
                }
                Ok::<_, slatedb::Error>(handle.seqnum())
            }
            .await
            .map_err(slatedb_error)
            .map_err(commit_outcome_unknown)
        };

        if let Err(error) = &result {
            pipeline
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .terminal_error = Some(error.clone());
        }
        if let Ok(sequence) = &result {
            let overlays = writes
                .iter()
                .map(|write| Arc::clone(&write.overlay))
                .collect::<Vec<_>>();
            point_cache.advance_local_write(*sequence, &overlays);
            for write in &writes {
                write
                    .published
                    .persisted_sequence
                    .store(*sequence, Ordering::Release);
            }
        }
        let completed_entries = writes
            .iter()
            .map(|write| write.overlay.len())
            .sum::<usize>();
        let completed_bytes = writes.iter().map(|write| write.weight_bytes).sum::<usize>();
        let retired = {
            let mut state = pipeline
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.pending_entries = state
                .pending_entries
                .checked_sub(completed_entries)
                .expect("SlateDB pending write entries should be balanced");
            state.pending_bytes = state
                .pending_bytes
                .checked_sub(completed_bytes)
                .expect("SlateDB pending write bytes should be balanced");
            if let Ok(sequence) = &result {
                // A completed write is covered by every future SlateDB
                // snapshot. Retain its publication only while an already
                // active older view still needs the overlay.
                state.newest_snapshot_sequence = state.newest_snapshot_sequence.max(*sequence);
                cleanup_publications(&mut state, *sequence)
            } else {
                Vec::new()
            }
        };
        publication_reclaimer.defer(retired);
        for write in writes {
            if let Ok(sequence) = &result {
                write.completion.complete(Ok(*sequence));
            } else {
                write.completion.complete(result.clone());
            }
        }
    }
}

#[derive(Clone)]
#[allow(missing_debug_implementations)]
struct SlateDBWorker {
    inner: Arc<SlateDBWorkerInner>,
}

#[allow(missing_debug_implementations)]
struct SlateDBWorkerInner {
    runtime: Handle,
    db: Arc<Db>,
    status: tokio::sync::watch::Receiver<DbStatus>,
    read_on_caller_current_thread: bool,
    in_flight: InFlightTracker,
    reclamation: InFlightTracker,
    shutdown: mpsc::Sender<()>,
    manager: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone, Debug, Default)]
struct InFlightTracker {
    state: Arc<(Mutex<usize>, Condvar)>,
}

struct InFlightGuard {
    state: Arc<(Mutex<usize>, Condvar)>,
}

#[derive(Clone)]
struct PublicationReclaimer {
    runtime: Handle,
    shutdown_in_flight: InFlightTracker,
    reclamation: InFlightTracker,
}

impl PublicationReclaimer {
    fn defer(&self, retired: Vec<Arc<PublishedWrite>>) {
        if retired.is_empty() {
            return;
        }
        let shutdown_in_flight = self.shutdown_in_flight.enter();
        let reclamation = self.reclamation.enter();
        self.runtime.spawn_blocking(move || {
            let _shutdown_in_flight = shutdown_in_flight;
            let _reclamation = reclamation;
            drop(retired);
        });
    }
}

impl InFlightTracker {
    fn enter(&self) -> InFlightGuard {
        let mut active = self
            .state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active += 1;
        drop(active);
        InFlightGuard {
            state: Arc::clone(&self.state),
        }
    }

    fn wait_until_idle(&self) {
        let (active, idle) = &*self.state;
        let mut active = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *active != 0 {
            active = idle
                .wait(active)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let (active, idle) = &*self.state;
        let mut active = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active = active
            .checked_sub(1)
            .expect("SlateDB in-flight operation count should be balanced");
        if *active == 0 {
            idle.notify_all();
        }
    }
}

impl SlateDBWorker {
    fn start(
        db_path: String,
        object_store: Arc<dyn ObjectStore>,
        options: SlateDBObjectStoreOptions,
        local_filesystem: bool,
        metrics: Option<Arc<DefaultMetricsRecorder>>,
    ) -> Result<Self, StorageError> {
        let in_flight = InFlightTracker::default();
        let reclamation = InFlightTracker::default();
        let manager_in_flight = in_flight.clone();
        let (shutdown, shutdown_rx) = mpsc::channel();
        let (opened_tx, opened_rx) = mpsc::channel::<Result<(Handle, Arc<Db>), StorageError>>();
        let thread = std::thread::Builder::new()
            .name("lix-slatedb-manager".to_string())
            .spawn(move || {
                run_slatedb_manager(
                    db_path,
                    object_store,
                    options,
                    metrics,
                    shutdown_rx,
                    opened_tx,
                    manager_in_flight,
                    local_filesystem,
                );
            })
            .map_err(|error| StorageError::Io(format!("spawn slatedb worker: {error}")))?;

        match opened_rx
            .recv()
            .map_err(|error| StorageError::Io(format!("slatedb worker did not open: {error}")))?
        {
            Ok((runtime, db)) => {
                let status = db.subscribe();
                Ok(Self {
                    inner: Arc::new(SlateDBWorkerInner {
                        runtime,
                        db,
                        status,
                        read_on_caller_current_thread: local_filesystem,
                        in_flight,
                        reclamation,
                        shutdown,
                        manager: Mutex::new(Some(thread)),
                    }),
                })
            }
            Err(error) => {
                let _ = thread.join();
                Err(error)
            }
        }
    }

    fn spawn<F, Fut>(&self, operation: F)
    where
        F: FnOnce(Arc<Db>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let in_flight = self.inner.in_flight.enter();
        let db = Arc::clone(&self.inner.db);
        self.inner.runtime.spawn(async move {
            let _in_flight = in_flight;
            operation(db).await;
        });
    }

    fn spawn_reclamation<F, Fut>(&self, operation: F)
    where
        F: FnOnce(Arc<Db>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let in_flight = self.inner.in_flight.enter();
        let reclamation = self.inner.reclamation.enter();
        let db = Arc::clone(&self.inner.db);
        self.inner.runtime.spawn(async move {
            let _in_flight = in_flight;
            let _reclamation = reclamation;
            operation(db).await;
        });
    }

    fn defer_publication_drop(&self, retired: Vec<Arc<PublishedWrite>>) {
        self.publication_reclaimer().defer(retired);
    }

    fn publication_reclaimer(&self) -> PublicationReclaimer {
        PublicationReclaimer {
            runtime: self.inner.runtime.clone(),
            shutdown_in_flight: self.inner.in_flight.clone(),
            reclamation: self.inner.reclamation.clone(),
        }
    }

    async fn wait_for_reclamation(&self) -> Result<(), StorageError> {
        let reclamation = self.inner.reclamation.clone();
        tokio::task::spawn_blocking(move || reclamation.wait_until_idle())
            .await
            .map_err(|error| StorageError::Io(format!("join SlateDB reclamation task: {error}")))
    }

    fn check_open(&self) -> Result<(), StorageError> {
        match self.inner.db.status().close_reason {
            None => Ok(()),
            Some(CloseReason::Fenced) => Err(StorageError::Fenced),
            Some(reason) => Err(StorageError::Closed(format!("slatedb closed: {reason:?}"))),
        }
    }

    fn check_open_fast(&self) -> Result<(), StorageError> {
        let status = self.inner.status.borrow();
        match status.close_reason.as_ref() {
            None => Ok(()),
            Some(&CloseReason::Fenced) => Err(StorageError::Fenced),
            Some(reason) => Err(StorageError::Closed(format!("slatedb closed: {reason:?}"))),
        }
    }

    /// Runs an operation that must retain completion semantics after its caller
    /// is dropped.
    ///
    /// Mutating operations and flushes use this path so a cancelled caller
    /// cannot turn an already-started publication or durability operation into
    /// an ambiguous outcome. Read-only work uses [`Self::call_read`] instead.
    async fn call<R, F, Fut>(&self, operation: F) -> Result<R, StorageError>
    where
        R: Send + 'static,
        F: FnOnce(Arc<Db>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, StorageError>> + Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        // Manager shutdown waits for this guard. The guard is deliberately
        // independent of `SlateDBWorkerInner`: keeping the inner Arc in a task
        // running on its own runtime would make its synchronous manager join
        // self-deadlock when the task released the final Arc.
        let in_flight = self.inner.in_flight.enter();
        let db = Arc::clone(&self.inner.db);
        self.inner.runtime.spawn(async move {
            let _in_flight = in_flight;
            let result = operation(db).await;
            let _ = reply_tx.send(result);
        });
        reply_rx
            .await
            .map_err(|error| StorageError::Io(format!("receive slatedb worker reply: {error}")))?
    }

    /// Runs a read operation which can be safely abandoned with its caller.
    ///
    /// Writes and flushes deliberately continue through [`Self::call`]: after
    /// a caller drops its future, letting a mutating operation finish preserves
    /// a single, well-defined publication and durability outcome. Reads have
    /// no such side effects, so run them on the caller's multithreaded
    /// executor. That keeps SlateDB's own async work local to the request and
    /// avoids a manager-task spawn plus oneshot round trip for every snapshot,
    /// point read, and scan page. A current-thread runtime uses the same path
    /// only for [`SlateDB::open`]'s LocalFileSystem, which moves filesystem
    /// work to Tokio's blocking pool before it can block the executor. Generic
    /// ObjectStores may synchronously work before their first yield, so they
    /// keep using the manager there.
    /// Canceling either path drops the read future and the in-flight guard that
    /// manager shutdown waits on.
    async fn call_read<R, F, Fut>(&self, operation: F) -> Result<R, StorageError>
    where
        R: Send + 'static,
        F: FnOnce(Arc<Db>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, StorageError>> + Send + 'static,
    {
        let caller_can_run_read = matches!(
            Handle::try_current(),
            Ok(handle)
                if self.inner.read_on_caller_current_thread
                    || handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
        );
        if !caller_can_run_read {
            return self.call_read_on_manager(operation).await;
        }
        // Manager shutdown waits for this guard. The guard is deliberately
        // independent of `SlateDBWorkerInner`: the operation may retain only
        // the database Arc while the last storage handle is being dropped.
        // Keeping the guard in this caller future prevents the synchronous
        // manager close from racing that operation.
        let _in_flight = self.inner.in_flight.enter();
        operation(Arc::clone(&self.inner.db)).await
    }

    async fn call_read_on_manager<R, F, Fut>(&self, operation: F) -> Result<R, StorageError>
    where
        R: Send + 'static,
        F: FnOnce(Arc<Db>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, StorageError>> + Send + 'static,
    {
        let (mut reply_tx, reply_rx) = oneshot::channel();
        // Manager shutdown waits for this guard. The guard is deliberately
        // independent of `SlateDBWorkerInner`: keeping the inner Arc in a task
        // running on its own runtime would make its synchronous manager join
        // self-deadlock when the task released the final Arc.
        let in_flight = self.inner.in_flight.enter();
        let db = Arc::clone(&self.inner.db);
        self.inner.runtime.spawn(async move {
            let _in_flight = in_flight;
            let result = tokio::select! {
                biased;
                () = reply_tx.closed() => None,
                result = operation(db) => Some(result),
            };
            if let Some(result) = result {
                let _ = reply_tx.send(result);
            }
        });
        reply_rx
            .await
            .map_err(|error| StorageError::Io(format!("receive slatedb worker reply: {error}")))?
    }
}

impl Drop for SlateDBWorkerInner {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
        let Ok(mut manager) = self.manager.lock() else {
            return;
        };
        if let Some(manager) = manager.take() {
            let _ = manager.join();
        }
    }
}

fn run_slatedb_manager(
    db_path: String,
    object_store: Arc<dyn ObjectStore>,
    options: SlateDBObjectStoreOptions,
    metrics: Option<Arc<DefaultMetricsRecorder>>,
    shutdown: mpsc::Receiver<()>,
    opened: mpsc::Sender<Result<(Handle, Arc<Db>), StorageError>>,
    in_flight: InFlightTracker,
    collect_local_garbage_on_close: bool,
) {
    let runtime = match Builder::new_multi_thread()
        .worker_threads(RUNTIME_WORKER_THREADS)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = opened.send(Err(StorageError::Io(format!(
                "create slatedb runtime: {error}"
            ))));
            return;
        }
    };

    let db = match open_slatedb(
        &runtime,
        db_path.clone(),
        Arc::clone(&object_store),
        options,
        metrics,
    ) {
        Ok(db) => db,
        Err(error) => {
            let _ = opened.send(Err(error));
            return;
        }
    };

    let db = Arc::new(db);
    if opened
        .send(Ok((runtime.handle().clone(), Arc::clone(&db))))
        .is_err()
    {
        let _ = runtime.block_on(db.close());
        return;
    }
    let _ = shutdown.recv();
    // This is the last local handle: every Lix read/write has drained, and
    // Db::close flushes the memtable and stops the compactor before collection.
    in_flight.wait_until_idle();
    if runtime.block_on(db.close()).is_ok() && collect_local_garbage_on_close {
        runtime.block_on(collect_local_garbage(db_path, object_store));
    }
}

async fn collect_local_garbage(db_path: String, object_store: Arc<dyn ObjectStore>) {
    let physical_db_path = join_db_path(&db_path, SEGMENTED_FORMAT_PATH);
    release_local_compactor_checkpoints(&physical_db_path, Arc::clone(&object_store)).await;
    GarbageCollectorBuilder::new(physical_db_path, object_store)
        .with_options(local_close_gc_options())
        .build()
        .run_gc_once()
        .await;
}

async fn release_local_compactor_checkpoints(
    physical_db_path: &str,
    object_store: Arc<dyn ObjectStore>,
) {
    let admin = AdminBuilder::new(physical_db_path, object_store).build();
    let checkpoints = match admin.list_checkpoints(None).await {
        Ok(checkpoints) => checkpoints,
        Err(_) => return,
    };
    for checkpoint in checkpoints {
        // Preserve named user checkpoints and reader checkpoints (which use a
        // different lifetime). The exact unnamed 15-minute shape is internal
        // to SlateDB's compactor and is redundant once the local DB is closed.
        if !is_compactor_safety_checkpoint(&checkpoint) {
            continue;
        }
        let _ = admin.delete_checkpoint(checkpoint.id).await;
    }
}

fn is_compactor_safety_checkpoint(checkpoint: &slatedb::Checkpoint) -> bool {
    checkpoint.name.is_none()
        && checkpoint.expire_time.is_some_and(|expire_time| {
            expire_time
                .signed_duration_since(checkpoint.create_time)
                .to_std()
                == Ok(COMPACTOR_SAFETY_CHECKPOINT_LIFETIME)
        })
}

fn local_close_gc_options() -> GarbageCollectorOptions {
    let immediate = || GarbageCollectorDirectoryOptions {
        interval: None,
        min_age: Duration::ZERO,
        dry_run: false,
    };
    GarbageCollectorOptions {
        manifest_options: None,
        wal_options: Some(immediate()),
        wal_fence_options: None,
        compacted_options: Some(immediate()),
        compactions_options: None,
        detach_options: None,
        metric_level: None,
    }
}

fn open_slatedb(
    runtime: &Runtime,
    db_path: String,
    object_store: Arc<dyn ObjectStore>,
    options: SlateDBObjectStoreOptions,
    metrics: Option<Arc<DefaultMetricsRecorder>>,
) -> Result<Db, StorageError> {
    runtime.block_on(async move {
        let physical_db_path = join_db_path(&db_path, SEGMENTED_FORMAT_PATH);
        let mut builder = Db::builder(physical_db_path, object_store)
            .with_segment_extractor(Arc::new(StorageSpacePrefixExtractor))
            .with_filter_policies(vec![Arc::new(BloomFilterPolicy::new(FILTER_BITS_PER_KEY))]);
        if let Some(metrics) = metrics {
            builder = builder.with_metrics_recorder(metrics);
        }
        let mut settings = slatedb_settings();
        if let Some(cache) = options.cache {
            let (slatedb_max_bytes, _) = disk_cache_budgets(cache.max_disk_cache_bytes);
            settings.object_store_cache_options = ObjectStoreCacheOptions {
                root_folder: Some(cache.root_folder),
                max_cache_size_bytes: Some(slatedb_max_bytes),
                part_size_bytes: OBJECT_STORE_CACHE_PART_SIZE_BYTES,
                cache_puts: true,
                preload_disk_cache_on_startup: None,
                scan_interval: None,
                ..ObjectStoreCacheOptions::default()
            };
            builder = builder.with_settings(settings).with_db_cache(db_cache(
                cache.block_cache_bytes,
                cache.metadata_cache_bytes,
            ));
        } else {
            // Keep the default bounded instead of accepting SlateDB's much
            // larger cache defaults. This captures hot SST blocks and
            // metadata for normal default reads without enabling the optional
            // disk object cache.
            builder = builder.with_settings(settings).with_db_cache(db_cache(
                DEFAULT_BLOCK_CACHE_BYTES,
                DEFAULT_METADATA_CACHE_BYTES,
            ));
        }
        builder.build().await.map_err(slatedb_error)
    })
}

fn disk_cache_budgets(total_bytes: usize) -> (usize, usize) {
    let immutable_bytes = total_bytes / 2;
    (total_bytes.saturating_sub(immutable_bytes), immutable_bytes)
}

fn join_db_path(db_path: &str, child: &str) -> String {
    let db_path = db_path.trim_end_matches('/');
    if db_path.is_empty() {
        child.to_string()
    } else {
        format!("{db_path}/{child}")
    }
}

fn slatedb_settings() -> Settings {
    let mut settings = Settings {
        compression_codec: Some(CompressionCodec::Zstd),
        ..Settings::default()
    };
    settings
        .compactor_options
        .as_mut()
        .expect("default SlateDB settings enable compaction")
        .commit_compacted_interval = COMPACTOR_COMMIT_INTERVAL;
    settings.max_unflushed_bytes = MAX_UNFLUSHED_BYTES;
    settings
}

fn validate_object_store_options(options: &SlateDBObjectStoreOptions) -> Result<(), StorageError> {
    let Some(cache) = &options.cache else {
        return Ok(());
    };
    if cache.root_folder.as_os_str().is_empty() {
        return Err(StorageError::Io(
            "slatedb cache root folder must not be empty".to_string(),
        ));
    }
    if cache.max_disk_cache_bytes == 0 {
        return Err(StorageError::Io(
            "slatedb disk cache size must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn moka_cache(capacity: u64) -> Option<Arc<dyn DbCache>> {
    if capacity == 0 {
        return None;
    }
    Some(Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
        max_capacity: capacity,
        time_to_live: None,
        time_to_idle: None,
    })))
}

fn db_cache(block_cache_bytes: u64, metadata_cache_bytes: u64) -> Arc<dyn DbCache> {
    Arc::new(
        SplitCache::new()
            .with_block_cache(moka_cache(block_cache_bytes))
            .with_meta_cache(moka_cache(metadata_cache_bytes))
            .build(),
    )
}

fn physical_key(space: SpaceId, key: &Key) -> Result<Key, StorageError> {
    let len = SPACE_PREFIX_LEN + key.0.len();
    if len > MAX_SLATEDB_KEY_LEN {
        return Err(StorageError::InvalidKey);
    }
    let mut bytes = Vec::with_capacity(len);
    bytes.extend_from_slice(&space.0.to_be_bytes());
    bytes.extend_from_slice(&key.0);
    Ok(Key(Bytes::from(bytes)))
}

fn physical_range(space: SpaceId, range: KeyRange) -> Result<KeyRange, StorageError> {
    let map = |bound: Bound<Key>, unbounded: Bound<Key>| -> Result<Bound<Key>, StorageError> {
        Ok(match bound {
            Bound::Included(key) => Bound::Included(physical_key(space, &key)?),
            Bound::Excluded(key) => Bound::Excluded(physical_key(space, &key)?),
            Bound::Unbounded => unbounded,
        })
    };
    Ok(KeyRange {
        lower: map(
            range.lower,
            Bound::Included(Key(Bytes::copy_from_slice(&space.0.to_be_bytes()))),
        )?,
        upper: map(
            range.upper,
            space.0.checked_add(1).map_or(Bound::Unbounded, |next| {
                Bound::Excluded(Key(Bytes::copy_from_slice(&next.to_be_bytes())))
            }),
        )?,
    })
}

#[derive(Clone, Debug)]
struct EncodedBounds {
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
}

impl EncodedBounds {
    fn new(range: KeyRange) -> Self {
        let lower = match range.lower {
            Bound::Included(key) => Bound::Included(key.0.to_vec()),
            Bound::Excluded(key) => Bound::Included(exclusive_successor(&key)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper = match range.upper {
            Bound::Included(key) => Bound::Excluded(exclusive_successor(&key)),
            Bound::Excluded(key) => Bound::Excluded(key.0.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Self { lower, upper }
    }

    fn is_empty(&self) -> bool {
        bounds_are_empty(&self.lower, &self.upper)
    }

    fn range(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        (self.lower.clone(), self.upper.clone())
    }
}

fn exclusive_successor(key: &Key) -> Vec<u8> {
    // SlateDB scans are half-open. Appending the smallest byte produces the
    // immediate lexicographic successor, so `> key` becomes `>= key || 0` and
    // `<= key` becomes `< key || 0` without relying on non-half-open bounds.
    let mut successor = Vec::with_capacity(key.0.len() + 1);
    successor.extend_from_slice(&key.0);
    successor.push(0);
    successor
}

async fn get_snapshot_values(
    snapshot: Arc<DbSnapshot>,
    keys: Vec<Key>,
    durability: ReadDurability,
) -> Result<Vec<Option<Bytes>>, StorageError> {
    let read_options = slatedb_read_options(durability);
    stream::iter(keys)
        .map(|key| {
            let snapshot = Arc::clone(&snapshot);
            let read_options = read_options.clone();
            async move {
                snapshot
                    .get_with_options(key.0, &read_options)
                    .await
                    .map_err(slatedb_error)
            }
        })
        .buffered(POINT_READ_CONCURRENCY)
        .try_collect()
        .await
}

async fn get_snapshot_value(
    snapshot: Arc<DbSnapshot>,
    key: Key,
    durability: ReadDurability,
) -> Result<Option<Bytes>, StorageError> {
    let read_options = slatedb_read_options(durability);
    snapshot
        .get_with_options(key.0, &read_options)
        .await
        .map_err(slatedb_error)
}

async fn open_snapshot_scan(
    snapshot: Arc<DbSnapshot>,
    bounds: EncodedBounds,
    durability: ReadDurability,
) -> Result<DbIterator, StorageError> {
    let scan_options = slatedb_scan_options(durability);
    snapshot
        .scan_with_options(bounds.range(), &scan_options)
        .await
        .map_err(slatedb_error)
}

async fn collect_snapshot_keys(
    snapshot: Arc<DbSnapshot>,
    bounds: EncodedBounds,
) -> Result<Vec<Key>, StorageError> {
    let scan_options = slatedb_scan_options(ReadDurability::Visible);
    let mut iter = snapshot
        .scan_with_options(bounds.range(), &scan_options)
        .await
        .map_err(slatedb_error)?;
    let mut keys = Vec::new();
    while let Some(row) = iter.next().await.map_err(slatedb_error)? {
        keys.push(Key(row.key));
    }
    Ok(keys)
}

fn slatedb_read_options(durability: ReadDurability) -> SlateDBReadOptions {
    SlateDBReadOptions::new().with_durability_filter(slatedb_durability_filter(durability))
}

fn slatedb_scan_options(durability: ReadDurability) -> SlateDBScanOptions {
    // SlateDB's default scan options fetch one block at a time. Keep iteration
    // ordered, but let SlateDB prefetch remote SST blocks behind the iterator.
    SlateDBScanOptions::default()
        .with_durability_filter(slatedb_durability_filter(durability))
        .with_read_ahead_bytes(SCAN_READ_AHEAD_BYTES)
        .with_max_fetch_tasks(SCAN_MAX_FETCH_TASKS)
        .with_cache_blocks(SCAN_CACHE_BLOCKS)
}

fn slatedb_durability_filter(durability: ReadDurability) -> DurabilityLevel {
    match durability {
        ReadDurability::Visible => DurabilityLevel::Memory,
        ReadDurability::Durable => DurabilityLevel::Remote,
    }
}

fn bounds_are_empty(lower: &Bound<Vec<u8>>, upper: &Bound<Vec<u8>>) -> bool {
    match (lower, upper) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(lower), Bound::Included(upper)) => lower > upper,
        (Bound::Included(lower) | Bound::Excluded(lower), Bound::Excluded(upper))
        | (Bound::Excluded(lower), Bound::Included(upper)) => lower >= upper,
    }
}

fn range_contains_key(range: &KeyRange, key: &Key) -> bool {
    let lower_matches = match &range.lower {
        Bound::Included(lower) => key >= lower,
        Bound::Excluded(lower) => key > lower,
        Bound::Unbounded => true,
    };
    let upper_matches = match &range.upper {
        Bound::Included(upper) => key <= upper,
        Bound::Excluded(upper) => key < upper,
        Bound::Unbounded => true,
    };
    lower_matches && upper_matches
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
}

fn project_value(value: Bytes, projection: CoreProjection) -> ProjectedValue {
    match projection {
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        CoreProjection::FullValue => ProjectedValue::FullValue(value),
    }
}

fn slatedb_error(error: slatedb::Error) -> StorageError {
    match error.kind() {
        slatedb::ErrorKind::Closed(CloseReason::Fenced) => StorageError::Fenced,
        // SlateDB's public contract requires a new instance after *any*
        // Closed reason. Keep fencing distinct for callers, while making
        // background-task failures and future close reasons terminal too.
        slatedb::ErrorKind::Closed(_) => StorageError::Closed(format!("slatedb storage: {error}")),
        _ => StorageError::Io(format!("slatedb storage: {error}")),
    }
}

/// Errors from an accepted SlateDB write cannot prove the batch was not
/// applied: SlateDB can fail after its atomic WAL/memtable publication and
/// before returning the durability watcher. Preserve the known terminal
/// states, but make every other attempted commit outcome explicit so callers
/// do not blindly replay it.
fn commit_outcome_unknown(error: StorageError) -> StorageError {
    match error {
        StorageError::Fenced | StorageError::Closed(_) => error,
        error => StorageError::CommitOutcomeUnknown(error.to_string()),
    }
}

fn object_store_error(error: object_store::Error) -> StorageError {
    StorageError::Io(format!("slatedb object store: {error}"))
}

#[derive(Clone, Default)]
#[allow(missing_debug_implementations)]
struct WriteGate {
    state: Arc<AsyncMutex<()>>,
}

impl WriteGate {
    fn new() -> Self {
        Self::default()
    }

    async fn acquire(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.state).lock_owned().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_IMMUTABLE_SPACE: StorageSpace =
        StorageSpace::immutable(SpaceId(0x00ff_0001), "test.immutable");
    use async_trait::async_trait;
    use futures_util::stream::BoxStream;
    use lix::storage::{
        GetOptions, ProjectedValue, PutEntry, ReadOptions, Storage, StorageRead, StorageWrite,
        StoredValue, WriteOptions,
    };
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{
        CopyOptions, Error as ObjectStoreError, GetOptions as ObjectStoreGetOptions, GetResult,
        ListResult, MultipartUpload, ObjectMeta, ObjectStoreExt, PutMultipartOptions, PutOptions,
        PutPayload, PutResult, RenameOptions, Result as ObjectStoreResult,
    };
    use slatedb::config::{CheckpointOptions, FlushOptions, FlushType};
    use std::collections::BTreeSet;
    use std::ops::Range;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::time::{Duration, Instant};

    tokio::task_local! {
        static CALLER_READ_MARKER: ();
    }

    fn immutable_test_segment(key: Key, value: Bytes) -> (Key, Bytes, Range<usize>) {
        let mut writer = ImmutableSegmentWriter::default();
        writer
            .insert(key, value)
            .expect("stage immutable test value");
        let segment = writer
            .finish(|_| true)
            .expect("finish immutable test segment")
            .remove(0);
        let range = segment.values[0].1.clone();
        let bytes = Bytes::from(segment.frames.into_iter().collect::<PutPayload>());
        (segment.id, bytes, range)
    }

    #[test]
    fn single_immutable_extent_reuses_the_fetched_span() {
        let span = Bytes::from_static(b"0123456789");
        let expected_ptr = span.slice(2..5).as_ptr();
        let value = materialize_immutable_request(&[Some(span)], &(100..103), &[(0, 2..5)])
            .expect("materialize one immutable extent");

        assert_eq!(value, Bytes::from_static(b"234"));
        assert_eq!(value.as_ptr(), expected_ptr);
    }

    #[test]
    fn fragmented_immutable_extents_still_reconstruct_one_value() {
        let value = materialize_immutable_request(
            &[
                Some(Bytes::from_static(b"abc")),
                Some(Bytes::from_static(b"def")),
            ],
            &(100..104),
            &[(0, 1..3), (1, 0..2)],
        )
        .expect("materialize fragmented immutable extents");

        assert_eq!(value, Bytes::from_static(b"bcde"));
    }

    #[test]
    fn immutable_values_pack_into_one_segment_with_exact_locators() {
        let values = [
            (Key(Bytes::from(vec![0x31; 32])), Bytes::from_static(b"one")),
            (Key(Bytes::from(vec![0x12; 32])), Bytes::from_static(b"two")),
            (
                Key(Bytes::from(vec![0x23; 32])),
                Bytes::from_static(b"three"),
            ),
        ];
        let mut writer = ImmutableSegmentWriter::default();
        for (key, value) in values.clone() {
            writer.insert(key, value).expect("stage immutable value");
        }
        let segments = writer.finish(|_| true).expect("pack immutable values");
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].values.len(), values.len());
        let segment_key = segments[0].id.clone();
        let segment_bytes = Bytes::from(
            segments[0]
                .frames
                .clone()
                .into_iter()
                .collect::<PutPayload>(),
        );
        for (key, expected) in values {
            let range = segments[0]
                .values
                .iter()
                .find_map(|(candidate, range)| (candidate == &key).then(|| range.clone()))
                .expect("packed value has an exact locator");
            assert_eq!(segments[0].id, segment_key);
            let encoded = segment_bytes.slice(range);
            assert_eq!(
                decode_immutable_value(encoded).expect("decode packed value"),
                expected
            );
        }
    }

    #[test]
    fn immutable_extent_plan_preserves_order_and_splits_boundary_crossings() {
        let mib = 1024 * 1024;
        let plan = plan_immutable_extents(&[9 * mib..10 * mib, 7 * mib..9 * mib, 0..mib], 12 * mib)
            .expect("plan immutable extents");
        assert_eq!(plan.extents, [0..8 * mib, 8 * mib..12 * mib]);
        assert_eq!(
            plan.placements,
            [
                vec![(1, mib..2 * mib)],
                vec![(0, 7 * mib..8 * mib), (1, 0..mib)],
                vec![(0, 0..mib)]
            ]
        );
    }

    #[test]
    fn immutable_cache_plans_fixed_aligned_extents_for_sequential_and_random_reads() {
        let mib = 1024 * 1024;
        let ranges = (0..17)
            .map(|index| index * mib..(index + 1) * mib)
            .collect::<Vec<_>>();
        let sequential =
            plan_immutable_extents(&ranges, 17 * mib).expect("plan sequential immutable extents");
        assert_eq!(
            sequential.extents,
            [0..8 * mib, 8 * mib..16 * mib, 16 * mib..17 * mib]
        );
        assert_eq!(sequential.placements.len(), ranges.len());

        let random = plan_immutable_extents(
            &[mib..2 * mib, 17 * mib..18 * mib, 41 * mib..42 * mib],
            64 * mib,
        )
        .expect("plan random immutable extents");
        assert_eq!(
            random.extents,
            [0..8 * mib, 16 * mib..24 * mib, 40 * mib..48 * mib]
        );
        assert!(
            random
                .extents
                .iter()
                .all(|extent| extent.len() <= IMMUTABLE_CACHE_EXTENT_BYTES)
        );
    }

    #[tokio::test]
    async fn immutable_cache_reuses_extents_across_sequential_requests_and_bounds_random_seeks() {
        let mib = 1024 * 1024;
        let mut writer = ImmutableSegmentWriter::default();
        let mut expected = Vec::new();
        for index in 0_u8..32 {
            let value = Bytes::from(vec![index; mib]);
            writer
                .insert(Key(Bytes::from(vec![index; 32])), value.clone())
                .expect("stage extent-cache value");
            expected.push(value);
        }
        let segment = writer
            .finish(|_| true)
            .expect("finish extent-cache segment")
            .remove(0);
        let segment_len = segment
            .values
            .last()
            .expect("extent-cache segment has values")
            .1
            .end;
        let markers = segment
            .values
            .iter()
            .map(|(_, range)| {
                encode_immutable_locator(&ImmutableValueLocator {
                    segment_id: segment.id.clone(),
                    segment_len,
                    range: range.clone(),
                })
                .expect("encode extent-cache locator")
            })
            .collect::<Vec<_>>();
        let counters = SlateDBIoCounters::default();
        let object_store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore {
            inner: Arc::new(InMemory::new()),
            counters: counters.clone(),
        });
        let sequential_cache = tempfile::tempdir().expect("create sequential extent cache");
        let sequential_store = ImmutableValueStore::new(
            "cross-request-extents",
            Arc::clone(&object_store),
            Some(&SlateDBCacheOptions {
                root_folder: sequential_cache.path().to_path_buf(),
                max_disk_cache_bytes: 128 * 1024 * 1024,
                block_cache_bytes: 0,
                metadata_cache_bytes: 0,
            }),
            None,
        );
        sequential_store
            .put_segments(vec![segment])
            .await
            .expect("store extent-cache segment");

        let before_sequential = counters.snapshot();
        for index in 0..6 {
            let value = sequential_store
                .get_many(vec![markers[index].clone()])
                .await
                .expect("read sequential cached value");
            assert_eq!(value, [expected[index].clone()]);
        }
        let sequential_io = counters.snapshot().saturating_sub(before_sequential);
        assert_eq!(
            sequential_io.read_objects, 1,
            "six separate sequential requests should share one remote extent"
        );
        assert_eq!(
            sequential_io.read_bytes,
            IMMUTABLE_CACHE_EXTENT_BYTES as u64
        );

        let random_cache = tempfile::tempdir().expect("create random-seek extent cache");
        let random_store = ImmutableValueStore::new(
            "cross-request-extents",
            object_store,
            Some(&SlateDBCacheOptions {
                root_folder: random_cache.path().to_path_buf(),
                max_disk_cache_bytes: 128 * 1024 * 1024,
                block_cache_bytes: 0,
                metadata_cache_bytes: 0,
            }),
            None,
        );
        let before_random = counters.snapshot();
        for index in [1_usize, 9, 17, 25] {
            let value = random_store
                .get_many(vec![markers[index].clone()])
                .await
                .expect("read random cached value");
            assert_eq!(value, [expected[index].clone()]);
        }
        let random_io = counters.snapshot().saturating_sub(before_random);
        assert_eq!(random_io.read_objects, 4);
        assert_eq!(
            random_io.read_bytes,
            (4 * IMMUTABLE_CACHE_EXTENT_BYTES) as u64,
            "each cold random seek should fetch at most one fixed extent"
        );
    }

    #[tokio::test]
    async fn immutable_cache_singleflights_concurrent_extent_reads() {
        let mib = 1024 * 1024;
        let mut writer = ImmutableSegmentWriter::default();
        writer
            .insert(Key(Bytes::from(vec![7; 32])), Bytes::from(vec![0x77; mib]))
            .expect("stage singleflight value");
        let segment = writer
            .finish(|_| true)
            .expect("finish singleflight segment")
            .remove(0);
        let marker = encode_immutable_locator(&ImmutableValueLocator {
            segment_id: segment.id.clone(),
            segment_len: segment.values[0].1.end,
            range: segment.values[0].1.clone(),
        })
        .expect("encode singleflight locator");
        let counters = SlateDBIoCounters::default();
        let object_store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore {
            inner: Arc::new(InMemory::new()),
            counters: counters.clone(),
        });
        let cache = tempfile::tempdir().expect("create singleflight cache");
        let store = ImmutableValueStore::new(
            "singleflight-extents",
            object_store,
            Some(&SlateDBCacheOptions {
                root_folder: cache.path().to_path_buf(),
                max_disk_cache_bytes: 64 * 1024 * 1024,
                block_cache_bytes: 0,
                metadata_cache_bytes: 0,
            }),
            None,
        );
        store
            .put_segments(vec![segment])
            .await
            .expect("store singleflight segment");
        let before = counters.snapshot();
        let (left, right) = tokio::join!(
            store.get_many(vec![marker.clone()]),
            store.get_many(vec![marker])
        );
        assert_eq!(
            left.expect("first extent read"),
            right.expect("second extent read")
        );
        let io = counters.snapshot().saturating_sub(before);
        assert_eq!(io.read_objects, 1, "one cold extent should issue one read");
    }

    #[tokio::test]
    async fn immutable_cache_acquires_colliding_stripes_once() {
        let root = tempfile::tempdir().expect("create stripe-lock cache");
        let cache = ImmutableValueCache::new(
            &SlateDBCacheOptions {
                root_folder: root.path().to_path_buf(),
                max_disk_cache_bytes: 1024,
                block_cache_bytes: 0,
                metadata_cache_bytes: 0,
            },
            None,
        );
        let first = Key(Bytes::from(vec![1; 32]));
        let mut colliding = vec![1; 32];
        colliding[8] = 2;
        let second = Key(Bytes::from(colliding));
        assert_eq!(
            cache.fetch_lock_index(&first),
            cache.fetch_lock_index(&second)
        );
        let guards =
            tokio::time::timeout(Duration::from_secs(1), cache.lock_fetches(&[first, second]))
                .await
                .expect("colliding stripes must not self-deadlock");
        assert_eq!(guards.len(), 1);
    }

    #[tokio::test]
    async fn startup_gc_protects_recent_and_reused_segments() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage = SlateDB::open_object_store_with_options(
            "startup-immutable-gc",
            Arc::clone(&object_store),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open startup GC storage");
        let mut writer = ImmutableSegmentWriter::default();
        writer
            .insert(Key(Bytes::from(vec![8; 32])), Bytes::from_static(b"orphan"))
            .expect("stage orphan");
        let segment = writer.finish(|_| true).expect("finish orphan").remove(0);
        let orphan_location = storage
            .immutable_value_store
            .location(&segment.id)
            .expect("locate orphan");
        object_store
            .put(
                &orphan_location,
                segment.frames.clone().into_iter().collect::<PutPayload>(),
            )
            .await
            .expect("upload orphan");
        collect_startup_immutable_garbage(
            &storage.worker,
            &storage.immutable_value_store,
            SystemTime::UNIX_EPOCH,
        )
        .await
        .expect("run grace-protected GC");
        assert_eq!(
            object_store
                .list(Some(&storage.immutable_value_store.prefix))
                .try_collect::<Vec<_>>()
                .await
                .expect("list grace-protected objects")
                .len(),
            1,
            "new in-flight segments must survive the GC grace window"
        );
        storage
            .immutable_value_store
            .put_segments(vec![segment])
            .await
            .expect("reuse old orphan before publication");
        collect_startup_immutable_garbage(
            &storage.worker,
            &storage.immutable_value_store,
            SystemTime::now() + Duration::from_secs(1),
        )
        .await
        .expect("run expired-orphan GC");
        let objects = object_store
            .list(Some(&storage.immutable_value_store.prefix))
            .try_collect::<Vec<_>>()
            .await
            .expect("list immutable objects");
        assert_eq!(
            objects.len(),
            1,
            "GC must not delete an old segment reused by an in-flight write"
        );
        let mut stale_writer = ImmutableSegmentWriter::default();
        stale_writer
            .insert(Key(Bytes::from(vec![9; 32])), Bytes::from_static(b"stale"))
            .expect("stage stale orphan");
        let stale = stale_writer
            .finish(|_| true)
            .expect("finish stale orphan")
            .remove(0);
        object_store
            .put(
                &storage
                    .immutable_value_store
                    .location(&stale.id)
                    .expect("locate stale orphan"),
                stale.frames.into_iter().collect::<PutPayload>(),
            )
            .await
            .expect("upload stale orphan");
        collect_startup_immutable_garbage(
            &storage.worker,
            &storage.immutable_value_store,
            SystemTime::now() + Duration::from_secs(1),
        )
        .await
        .expect("collect stale orphan");
        assert_eq!(
            object_store
                .list(Some(&storage.immutable_value_store.prefix))
                .try_collect::<Vec<_>>()
                .await
                .expect("list after stale collection")
                .len(),
            1,
            "GC must remove unreferenced segments not used by this process"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn first_commit_does_not_wait_for_startup_immutable_gc() {
        let object_store = Arc::new(InMemory::new());
        let store = BlockingStore::new(Arc::clone(&object_store));
        let storage = SlateDB::open_object_store_with_options(
            "background-startup-immutable-gc",
            Arc::new(store.clone()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open background startup-GC storage");
        object_store
            .put(
                &storage.immutable_value_store.prefix.clone().join("orphan"),
                PutPayload::from_static(b"orphan"),
            )
            .await
            .expect("seed immutable object for blocked listing");
        let blocked = store.block_immutable_lists();

        let mutable_space = StorageSpace::mutable(SpaceId(7), "test.mutable");
        let mut write = tokio::time::timeout(
            Duration::from_secs(1),
            storage.begin_write(WriteOptions::default()),
        )
        .await
        .expect("begin_write must not wait for immutable listing")
        .expect("begin first foreground write");
        blocked.wait_for_entries(1, "background immutable listing");
        write
            .put_many(
                mutable_space,
                PutBatch {
                    entries: vec![PutEntry {
                        key: Key(Bytes::from_static(b"key")),
                        value: StoredValue {
                            bytes: Bytes::from_static(b"value"),
                        },
                    }],
                },
            )
            .await
            .expect("stage first foreground write");
        tokio::time::timeout(Duration::from_secs(1), write.commit())
            .await
            .expect("first commit must not wait for immutable listing")
            .expect("commit first foreground write");

        drop(blocked);
        storage
            .flush()
            .await
            .expect("flush waits for background immutable GC");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn background_startup_gc_reclaims_stale_orphan_and_retains_reachable_value() {
        let directory = tempfile::tempdir().expect("create startup-GC lifecycle directory");
        let immutable_key = Key(Bytes::from(vec![3; 32]));
        let immutable_value = Bytes::from_static(b"reachable immutable value");
        let orphan_location = {
            let storage = SlateDB::open(directory.path()).expect("open seed storage");
            let mut write = storage
                .begin_write(WriteOptions::default())
                .await
                .expect("begin reachable immutable write");
            write
                .put_many(
                    TEST_IMMUTABLE_SPACE,
                    PutBatch {
                        entries: vec![PutEntry {
                            key: immutable_key.clone(),
                            value: StoredValue {
                                bytes: immutable_value.clone(),
                            },
                        }],
                    },
                )
                .await
                .expect("stage reachable immutable value");
            write
                .commit()
                .await
                .expect("publish reachable immutable value");
            storage.flush().await.expect("flush seed storage");

            let mut orphan_writer = ImmutableSegmentWriter::default();
            orphan_writer
                .insert(Key(Bytes::from(vec![4; 32])), Bytes::from_static(b"orphan"))
                .expect("stage stale orphan");
            let orphan = orphan_writer
                .finish(|_| true)
                .expect("finish stale orphan")
                .remove(0);
            let orphan_location = storage
                .immutable_value_store
                .location(&orphan.id)
                .expect("locate stale orphan");
            storage
                .immutable_value_store
                .object_store
                .put(
                    &orphan_location,
                    orphan.frames.into_iter().collect::<PutPayload>(),
                )
                .await
                .expect("upload stale orphan");
            orphan_location
        };
        let orphan_path = directory.path().join(orphan_location.as_ref());
        let orphan = std::fs::OpenOptions::new()
            .write(true)
            .open(&orphan_path)
            .expect("open stale orphan file");
        orphan
            .set_times(
                std::fs::FileTimes::new()
                    .set_modified(SystemTime::UNIX_EPOCH)
                    .set_accessed(SystemTime::UNIX_EPOCH),
            )
            .expect("age stale orphan file");

        let storage = SlateDB::open(directory.path()).expect("reopen lifecycle storage");
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .expect("begin first write after reopen");
        write
            .put_many(
                StorageSpace::mutable(SpaceId(8), "test.mutable"),
                PutBatch {
                    entries: vec![PutEntry {
                        key: Key(Bytes::from_static(b"trigger")),
                        value: StoredValue {
                            bytes: Bytes::from_static(b"gc"),
                        },
                    }],
                },
            )
            .await
            .expect("stage first write after reopen");
        write
            .commit()
            .await
            .expect("commit first write after reopen");
        storage
            .flush()
            .await
            .expect("wait for reopened background GC");
        assert!(!orphan_path.exists(), "stale orphan must be reclaimed");

        let read = storage
            .begin_read(ReadOptions::default())
            .await
            .expect("read retained immutable value");
        assert_eq!(
            read.get_many(&[GetManyRequest {
                space: TEST_IMMUTABLE_SPACE,
                keys: std::slice::from_ref(&immutable_key),
                opts: GetOptions::default(),
            }])
            .await
            .expect("load retained immutable value")
            .values,
            vec![Some(ProjectedValue::FullValue(immutable_value))]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn background_startup_gc_failure_surfaces_on_flush_and_later_writes() {
        let store = BlockingStore::new(Arc::new(InMemory::new()));
        let storage = SlateDB::open_object_store_with_options(
            "failed-background-startup-immutable-gc",
            Arc::new(store.clone()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open failed startup-GC storage");
        store.fail_immutable_lists();

        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .expect("first write starts before background failure");
        write
            .put_many(
                StorageSpace::mutable(SpaceId(9), "test.mutable"),
                PutBatch {
                    entries: vec![PutEntry {
                        key: Key(Bytes::from_static(b"key")),
                        value: StoredValue {
                            bytes: Bytes::from_static(b"value"),
                        },
                    }],
                },
            )
            .await
            .expect("stage write before background failure");
        write
            .commit()
            .await
            .expect("foreground commit precedes background failure");

        let flush_error = storage
            .flush()
            .await
            .expect_err("flush must surface background GC failure");
        assert!(
            flush_error
                .to_string()
                .contains("injected immutable list failure")
        );
        let write_error = match storage.begin_write(WriteOptions::default()).await {
            Ok(_) => panic!("later write must surface background GC failure"),
            Err(error) => error,
        };
        assert!(
            write_error
                .to_string()
                .contains("injected immutable list failure")
        );
    }

    #[tokio::test]
    async fn diagnostic_object_store_counters_measure_completed_io() {
        let counters = SlateDBIoCounters::default();
        let store = CountingObjectStore {
            inner: Arc::new(InMemory::new()),
            counters: counters.clone(),
        };
        let path = ObjectPath::from("table.sst");
        store
            .put_opts(
                &path,
                PutPayload::from_static(b"payload"),
                PutOptions::default(),
            )
            .await
            .expect("write counted object");
        let bytes = store
            .get_opts(&path, ObjectStoreGetOptions::default())
            .await
            .expect("open counted object")
            .bytes()
            .await
            .expect("read counted object");
        assert_eq!(bytes, Bytes::from_static(b"payload"));
        let ranges = store
            .get_ranges(&path, &[0..2, 5..7])
            .await
            .expect("read counted object ranges");
        assert_eq!(
            ranges,
            vec![Bytes::from_static(b"pa"), Bytes::from_static(b"ad")]
        );
        let multipart_path = ObjectPath::from("multipart.sst");
        let mut multipart = store
            .put_multipart_opts(&multipart_path, PutMultipartOptions::default())
            .await
            .expect("open counted multipart object");
        multipart
            .put_part(PutPayload::from_static(b"multi"))
            .await
            .expect("write counted multipart part");
        multipart
            .put_part(PutPayload::from_static(b"part"))
            .await
            .expect("write second counted multipart part");
        multipart
            .complete()
            .await
            .expect("complete counted multipart object");
        let listed = store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list counted object");
        assert_eq!(listed.len(), 2);
        store.delete(&path).await.expect("delete counted object");

        assert_eq!(
            counters.snapshot(),
            SlateDBIoSnapshot {
                read_objects: 2,
                read_bytes: 11,
                write_objects: 2,
                write_bytes: 16,
                list_operations: 1,
                listed_objects: 2,
                deleted_objects: 1,
                copied_objects: 0,
                other: SlateDBIoCategorySnapshot {
                    read_objects: 2,
                    read_bytes: 11,
                    write_objects: 2,
                    write_bytes: 16,
                },
                ..SlateDBIoSnapshot::default()
            }
        );
    }

    #[test]
    fn diagnostic_object_store_counters_classify_slatedb_paths() {
        let counters = SlateDBIoCounterValues::default();
        for (path, expected) in [
            ("db/wal/1.sst", &counters.wal),
            ("db/compacted/1.sst", &counters.compacted),
            ("db/manifest/1.manifest", &counters.manifest),
            ("db/compactions/1.compactions", &counters.compactions),
            ("db/other/file", &counters.other),
        ] {
            assert!(std::ptr::eq(
                counters.category(&ObjectPath::from(path)),
                expected
            ));
        }
    }

    #[test]
    fn uses_zstd_compression_by_default() {
        assert_eq!(
            slatedb_settings().compression_codec,
            Some(CompressionCodec::Zstd)
        );
    }

    #[test]
    fn local_close_collects_only_immediately_safe_data_files() {
        let options = local_close_gc_options();
        assert!(options.manifest_options.is_none());
        assert!(options.wal_fence_options.is_none());
        assert!(options.compactions_options.is_none());
        assert!(options.detach_options.is_none());
        for directory in [options.wal_options, options.compacted_options] {
            let directory = directory.expect("local close enables data-file collection");
            assert_eq!(directory.min_age, Duration::ZERO);
            assert!(!directory.dry_run);
        }
    }

    #[test]
    fn local_close_releases_only_unnamed_compactor_safety_checkpoints() {
        let store = Arc::new(InMemory::new());
        let db_path = "test-local-close-compactor-checkpoints";
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("create checkpoint fixture");
        drop(storage);
        let physical_db_path = join_db_path(db_path, SEGMENTED_FORMAT_PATH);
        let admin = AdminBuilder::new(physical_db_path.clone(), store.clone()).build();
        let compactor = block_on(admin.create_detached_checkpoint(&CheckpointOptions {
            lifetime: Some(COMPACTOR_SAFETY_CHECKPOINT_LIFETIME),
            ..CheckpointOptions::default()
        }))
        .expect("create compactor-shaped checkpoint");
        let named = block_on(admin.create_detached_checkpoint(&CheckpointOptions {
            lifetime: Some(COMPACTOR_SAFETY_CHECKPOINT_LIFETIME),
            name: Some("keep".to_string()),
            ..CheckpointOptions::default()
        }))
        .expect("create named checkpoint");
        let reader = block_on(admin.create_detached_checkpoint(&CheckpointOptions {
            lifetime: Some(Duration::from_secs(10 * 60)),
            ..CheckpointOptions::default()
        }))
        .expect("create reader-shaped checkpoint");

        block_on(release_local_compactor_checkpoints(
            &physical_db_path,
            store,
        ));

        let remaining = block_on(admin.list_checkpoints(None))
            .expect("list checkpoints after local release")
            .into_iter()
            .map(|checkpoint| checkpoint.id)
            .collect::<BTreeSet<_>>();
        assert!(!remaining.contains(&compactor.id));
        assert!(remaining.contains(&named.id));
        assert!(remaining.contains(&reader.id));
    }

    #[test]
    fn bounds_unflushed_memory_to_two_l0_tables() {
        let settings = slatedb_settings();
        assert_eq!(settings.max_unflushed_bytes, MAX_UNFLUSHED_BYTES);
        assert_eq!(settings.max_unflushed_bytes, settings.l0_sst_size_bytes * 2);
    }

    #[test]
    fn strengthens_point_filters_for_large_history() {
        assert_eq!(FILTER_BITS_PER_KEY, 16);
        assert_eq!(
            BloomFilterPolicy::new(FILTER_BITS_PER_KEY).bits_per_key(),
            FILTER_BITS_PER_KEY
        );
    }

    #[test]
    fn storage_space_extractor_uses_the_four_byte_physical_prefix() {
        let extractor = StorageSpacePrefixExtractor;
        assert_eq!(extractor.name(), SPACE_PREFIX_EXTRACTOR_NAME);
        assert_eq!(
            extractor.prefix_len(&PrefixTarget::Point(Bytes::from_static(b"\0\0\0\x07key"))),
            Some(SPACE_PREFIX_LEN)
        );
        assert_eq!(
            extractor.prefix_len(&PrefixTarget::Prefix(Bytes::from_static(b"\0\0\0"))),
            None
        );
    }

    #[test]
    fn disk_cache_parts_match_scan_read_ahead() {
        assert_eq!(OBJECT_STORE_CACHE_PART_SIZE_BYTES, SCAN_READ_AHEAD_BYTES);
    }

    #[test]
    fn disk_cache_budget_is_shared_without_exceeding_the_configured_total() {
        for total in [1, 2, 3, 1024, 8 * 1024 * 1024] {
            let (slatedb, immutable) = disk_cache_budgets(total);
            assert_eq!(slatedb.saturating_add(immutable), total);
            assert!(slatedb >= immutable);
        }
    }

    #[test]
    fn encoded_bounds_normalize_to_half_open_ranges() {
        let key = Key(Bytes::from_static(b"key"));
        let bounds = EncodedBounds::new(KeyRange {
            lower: Bound::Excluded(key.clone()),
            upper: Bound::Included(key.clone()),
        });
        let successor = b"key\0".to_vec();
        assert_eq!(bounds.lower, Bound::Included(successor.clone()));
        assert_eq!(bounds.upper, Bound::Excluded(successor));

        let bounds = EncodedBounds::new(KeyRange {
            lower: Bound::Excluded(key),
            upper: Bound::Unbounded,
        });
        assert_eq!(bounds.lower, Bound::Included(b"key\0".to_vec()));
        assert_eq!(bounds.upper, Bound::Unbounded);
    }

    #[test]
    fn streaming_overlay_cursor_advances_heads_and_newest_duplicate_wins() {
        let published = |publication_id, rows: &[(&'static [u8], Option<&'static [u8]>)]| {
            Arc::new(PublishedWrite {
                publication_id,
                overlay: Arc::new(
                    rows.iter()
                        .map(|(key, value)| {
                            (
                                Key(Bytes::copy_from_slice(key)),
                                value.map(Bytes::copy_from_slice),
                            )
                        })
                        .collect(),
                ),
                persisted_sequence: AtomicU64::new(PENDING_WRITE_SEQUENCE),
            })
        };
        let older = published(
            7,
            &[(b"a", Some(b"old-a")), (b"b", None), (b"d", Some(b"old-d"))],
        );
        let newer = published(9, &[(b"a", Some(b"new-a")), (b"c", Some(b"new-c"))]);
        let mut cursor = StreamingOverlayCursor::new(
            EncodedBounds::new(KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            }),
            vec![newer, older],
        );
        assert_eq!(
            std::iter::from_fn(|| cursor.take()).collect::<Vec<_>>(),
            vec![
                (
                    Key(Bytes::from_static(b"a")),
                    Some(Bytes::from_static(b"new-a")),
                ),
                (Key(Bytes::from_static(b"b")), None),
                (
                    Key(Bytes::from_static(b"c")),
                    Some(Bytes::from_static(b"new-c")),
                ),
                (
                    Key(Bytes::from_static(b"d")),
                    Some(Bytes::from_static(b"old-d")),
                ),
            ]
        );
    }

    #[test]
    fn batches_completed_compactions_on_the_compactor_poll_interval() {
        let settings = slatedb_settings();
        let compactor = settings
            .compactor_options
            .as_ref()
            .expect("Lix enables SlateDB compaction");
        let default_settings = Settings::default();
        let default_compactor = default_settings
            .compactor_options
            .as_ref()
            .expect("default SlateDB settings enable compaction");
        assert_eq!(
            compactor.commit_compacted_interval,
            COMPACTOR_COMMIT_INTERVAL
        );
        assert_eq!(compactor.commit_compacted_interval, compactor.poll_interval);
        assert_eq!(compactor.poll_interval, default_compactor.poll_interval);
    }

    #[test]
    fn bulk_write_pipeline_applies_entry_and_byte_backpressure() {
        assert!(!write_pipeline_should_backpressure(
            WRITE_PIPELINE_MAX_PENDING_ENTRIES - 1,
            WRITE_PIPELINE_MAX_PENDING_BYTES - 1,
        ));
        assert!(write_pipeline_should_backpressure(
            WRITE_PIPELINE_MAX_PENDING_ENTRIES,
            0,
        ));
        assert!(write_pipeline_should_backpressure(
            0,
            WRITE_PIPELINE_MAX_PENDING_BYTES,
        ));
    }

    #[test]
    fn opens_fresh_local_versioned_storage() {
        let directory = tempfile::tempdir().expect("create fresh local storage directory");
        let storage = SlateDB::open(directory.path()).expect("open fresh local LZ4 storage");
        assert_eq!(storage.path(), directory.path());
    }

    #[test]
    fn immutable_binary_chunks_publish_through_markers_and_roundtrip_after_reopen() {
        let directory = tempfile::tempdir().expect("create immutable chunk storage directory");
        let key = Key(Bytes::from(vec![0x2a; 32]));
        let value = Bytes::from(vec![0x5c; 2 * 1024 * 1024]);
        let value_hash = *blake3::hash(&value).as_bytes();
        let storage = SlateDB::open(directory.path()).expect("open immutable chunk storage");

        let rolled_back_key = Key(Bytes::from(vec![0x19; 32]));
        let mut rolled_back = block_on(storage.begin_write(WriteOptions::default()))
            .expect("begin rolled-back immutable chunk write");
        block_on(rolled_back.put_many(
            TEST_IMMUTABLE_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: rolled_back_key.clone(),
                    value: StoredValue {
                        bytes: value.clone(),
                    },
                }],
            },
        ))
        .expect("stage rolled-back immutable chunk");
        block_on(rolled_back.rollback()).expect("roll back immutable chunk");

        let mut write = block_on(storage.begin_write(WriteOptions::default()))
            .expect("begin immutable chunk write");
        block_on(write.put_many(
            TEST_IMMUTABLE_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: value.clone(),
                    },
                }],
            },
        ))
        .expect("stage immutable chunk");
        block_on(write.commit()).expect("publish immutable chunk marker");
        block_on(storage.flush()).expect("flush immutable chunk marker");
        drop(storage);

        let immutable_directory = directory.path().join(DB_PATH).join(IMMUTABLE_VALUE_PATH);
        let physical_key =
            physical_key(TEST_IMMUTABLE_SPACE.id, &key).expect("derive physical immutable key");
        let (segment_key, encoded_value, _) = immutable_test_segment(physical_key, value.clone());
        let segment_hash: [u8; 32] = segment_key.0.as_ref().try_into().expect("segment hash");
        let stored_path =
            immutable_directory.join(blake3::Hash::from_bytes(segment_hash).to_hex().as_str());
        let stored_bytes = std::fs::read(&stored_path).expect("read immutable chunk object");
        assert_eq!(stored_bytes, encoded_value);
        assert_eq!(
            std::fs::read_dir(&immutable_directory)
                .expect("list immutable segments")
                .filter_map(Result::ok)
                .filter(|entry| entry.path().is_file())
                .count(),
            2,
            "rollback leaves one unreachable immutable segment for reachability GC"
        );

        let storage = SlateDB::open(directory.path()).expect("reopen immutable chunk storage");
        let read = block_on(storage.begin_read(ReadOptions::default()))
            .expect("read immutable chunk storage");
        let result = block_on(read.get_many(&[GetManyRequest {
            space: TEST_IMMUTABLE_SPACE,
            keys: std::slice::from_ref(&key),
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }]))
        .expect("load immutable chunk");
        assert_eq!(
            result.values,
            vec![Some(ProjectedValue::FullValue(value.clone()))]
        );
        let key_only = block_on(read.get_many(&[GetManyRequest {
            space: TEST_IMMUTABLE_SPACE,
            keys: std::slice::from_ref(&key),
            opts: GetOptions {
                projection: CoreProjection::KeyOnly,
            },
        }]))
        .expect("load immutable chunk key only");
        assert_eq!(key_only.values, vec![Some(ProjectedValue::KeyOnly)]);
        let mut cursor = block_on(read.begin_scan(
            TEST_IMMUTABLE_SPACE,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions {
                projection: CoreProjection::FullValue,
                ..BeginScanOptions::default()
            },
        ))
        .expect("begin immutable chunk scan");
        let (scan, _scan_has_more) = block_on(cursor.next_page(16))
            .expect("scan immutable chunk")
            .into_parts();
        assert_eq!(scan.len(), 1);
        assert_eq!(scan[0].key, key);
        assert_eq!(scan[0].value, ProjectedValue::FullValue(value));

        let matching = block_on(storage.begin_write(WriteOptions {
            preconditions: vec![Precondition::KeyValueHashEquals {
                space: TEST_IMMUTABLE_SPACE,
                key: key.clone(),
                hash: value_hash,
            }],
            ..WriteOptions::default()
        }))
        .expect("begin immutable full-value hash precondition");
        block_on(matching.commit()).expect("immutable full-value hash precondition should match");
        let mismatch = block_on(storage.begin_write(WriteOptions {
            preconditions: vec![Precondition::KeyValueEquals {
                space: TEST_IMMUTABLE_SPACE,
                key,
                expected: Bytes::from_static(b"not the immutable value"),
            }],
            ..WriteOptions::default()
        }))
        .expect("begin immutable full-value equality precondition");
        let mismatch = block_on(mismatch.commit())
            .err()
            .expect("immutable full-value equality precondition should inspect sidecar bytes");
        assert!(matches!(mismatch, StorageError::PreconditionFailed(_)));

        std::fs::remove_file(stored_path).expect("remove immutable object for corruption probe");
        let error = block_on(read.get_many(&[GetManyRequest {
            space: TEST_IMMUTABLE_SPACE,
            keys: std::slice::from_ref(&Key(Bytes::from(vec![0x2a; 32]))),
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }]))
        .expect_err("a published marker must not hide a missing immutable object");
        assert!(matches!(error, StorageError::Io(_)));
    }

    #[tokio::test]
    async fn cancellation_during_worker_poisons_streaming_cursor_fail_closed() {
        let storage = SlateDB::open_object_store_with_options(
            "cancel-during-worker",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open worker-cancellation storage");
        let space = StorageSpace::mutable(SpaceId(0x77), "test.worker-cancellation");
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .expect("begin worker-cancellation seed");
        write
            .put_many(
                space,
                PutBatch {
                    entries: vec![
                        PutEntry {
                            key: Key(Bytes::from_static(b"a")),
                            value: StoredValue {
                                bytes: Bytes::from_static(b"A"),
                            },
                        },
                        PutEntry {
                            key: Key(Bytes::from_static(b"b")),
                            value: StoredValue {
                                bytes: Bytes::from_static(b"B"),
                            },
                        },
                    ],
                },
            )
            .await
            .expect("stage worker-cancellation seed");
        write
            .commit()
            .await
            .expect("commit worker-cancellation seed");

        let mut read = storage
            .begin_read(ReadOptions::default())
            .await
            .expect("begin worker-cancellation read");
        let gate = Arc::new(ScanTestGate::new());
        read.scan_worker_gate = Some(Arc::clone(&gate));
        let mut cursor = read
            .begin_scan(
                space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                BeginScanOptions::default(),
            )
            .await
            .expect("begin worker-cancellation cursor");
        let mut page = Box::pin(cursor.next_page(1));
        tokio::select! {
            () = gate.wait_until_entered() => {}
            result = &mut page => panic!("scan page completed before worker suspension: {result:?}"),
        }
        drop(page);
        gate.release.notify_waiters();
        assert!(matches!(
            cursor.next_page(1).await,
            Err(StorageError::InvalidCursor)
        ));
        drop(cursor);
        drop(read);

        let restart_read = storage
            .begin_read(ReadOptions::default())
            .await
            .expect("begin fresh worker-cancellation read");
        let mut restart = restart_read
            .begin_scan(
                space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                BeginScanOptions::default(),
            )
            .await
            .expect("begin fresh worker-cancellation cursor");
        let (restarted, _restarted_has_more) = restart
            .next_page(2)
            .await
            .expect("drain fresh worker-cancellation cursor").into_parts();
        assert_eq!(
            restarted
                .iter()
                .map(|entry| entry.key.0.as_ref())
                .collect::<Vec<_>>(),
            vec![b"a".as_slice(), b"b".as_slice()]
        );
    }

    #[tokio::test]
    async fn cancelled_immutable_hydration_poisons_streaming_cursor_without_skipping() {
        let directory = tempfile::tempdir().expect("create cancelled-scan storage directory");
        let storage = SlateDB::open(directory.path()).expect("open cancelled-scan storage");
        let first_key = Key(Bytes::from(vec![0x31; 32]));
        let second_key = Key(Bytes::from(vec![0x32; 32]));
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .expect("begin cancelled-scan seed");
        write
            .put_many(
                TEST_IMMUTABLE_SPACE,
                PutBatch {
                    entries: vec![
                        PutEntry {
                            key: first_key.clone(),
                            value: StoredValue {
                                bytes: Bytes::from_static(b"first-value"),
                            },
                        },
                        PutEntry {
                            key: second_key.clone(),
                            value: StoredValue {
                                bytes: Bytes::from_static(b"second-value"),
                            },
                        },
                    ],
                },
            )
            .await
            .expect("stage cancelled-scan seed");
        write.commit().await.expect("commit cancelled-scan seed");

        let mut read = storage
            .begin_read(ReadOptions::default())
            .await
            .expect("begin cancelled-scan read");
        let gate = Arc::new(ScanTestGate::new());
        read.scan_hydration_gate = Some(Arc::clone(&gate));
        let mut cursor = read
            .begin_scan(
                TEST_IMMUTABLE_SPACE,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                BeginScanOptions::default(),
            )
            .await
            .expect("begin cancelled immutable scan");
        let mut page = Box::pin(cursor.next_page(1));
        tokio::select! {
            () = gate.wait_until_entered() => {}
            result = &mut page => panic!("scan page completed before hydration suspension: {result:?}"),
        }
        drop(page);
        assert!(matches!(
            cursor.next_page(1).await,
            Err(StorageError::InvalidCursor)
        ));
        drop(cursor);
        drop(read);

        let restart_read = storage
            .begin_read(ReadOptions::default())
            .await
            .expect("begin explicit restart read");
        let mut restart = restart_read
            .begin_scan(
                TEST_IMMUTABLE_SPACE,
                KeyRange {
                    lower: Bound::Excluded(first_key),
                    upper: Bound::Unbounded,
                },
                BeginScanOptions::default(),
            )
            .await
            .expect("begin explicit exclusive restart");
        let (restarted, _restarted_has_more) = restart
            .next_page(1)
            .await
            .expect("read explicit exclusive restart page").into_parts();
        assert_eq!(restarted.len(), 1);
        assert_eq!(restarted[0].key, second_key);
        assert_eq!(
            restarted[0].value,
            ProjectedValue::FullValue(Bytes::from_static(b"second-value"))
        );
    }

    #[test]
    fn configured_disk_cache_serves_immutable_value_store_after_remote_removal() {
        let object_store = Arc::new(InMemory::new());
        let db_path = "immutable-chunk-cache";
        let key = Key(Bytes::from(vec![0x37; 32]));
        let value = Bytes::from(vec![0x8d; 2 * 1024 * 1024]);
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            object_store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open immutable chunk cache seed");
        let mut write = block_on(storage.begin_write(WriteOptions::default()))
            .expect("begin immutable chunk cache seed");
        block_on(write.put_many(
            TEST_IMMUTABLE_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: value.clone(),
                    },
                }],
            },
        ))
        .expect("stage immutable chunk cache seed");
        block_on(write.commit()).expect("commit immutable chunk cache seed");
        block_on(storage.flush()).expect("flush immutable chunk cache seed");
        drop(storage);

        let cache = tempfile::tempdir().expect("create immutable chunk disk cache");
        let cache_root = cache.path().join("object-cache");
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            object_store.clone(),
            SlateDBObjectStoreOptions {
                cache: Some(SlateDBCacheOptions {
                    root_folder: cache_root.clone(),
                    max_disk_cache_bytes: 8 * 1024 * 1024,
                    block_cache_bytes: 1024 * 1024,
                    metadata_cache_bytes: 1024 * 1024,
                }),
            },
        )
        .expect("open cached immutable chunk storage");
        let read = block_on(storage.begin_read(ReadOptions::default()))
            .expect("begin cached immutable chunk read");
        let request = [GetManyRequest {
            space: TEST_IMMUTABLE_SPACE,
            keys: std::slice::from_ref(&key),
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }];
        let first = block_on(read.get_many(&request)).expect("populate immutable chunk disk cache");
        assert_eq!(
            first.values,
            vec![Some(ProjectedValue::FullValue(value.clone()))]
        );
        let physical_key =
            physical_key(TEST_IMMUTABLE_SPACE.id, &key).expect("derive physical immutable key");
        let (segment_key, encoded_value, _range) =
            immutable_test_segment(physical_key, value.clone());
        let segment_hash: [u8; 32] = segment_key.0.as_ref().try_into().expect("segment hash");
        let cache_key = immutable_range_cache_key(&segment_key, &(0..encoded_value.len()))
            .expect("derive immutable range cache key");
        let cache_path = cache_root.join(IMMUTABLE_VALUE_CACHE_PATH).join(
            blake3::Hash::from_bytes(
                cache_key
                    .0
                    .as_ref()
                    .try_into()
                    .expect("range cache key is a hash"),
            )
            .to_hex()
            .as_str(),
        );
        assert!(cache_path.is_file());

        let mut corrupt = std::fs::read(&cache_path).expect("read immutable disk cache entry");
        *corrupt
            .last_mut()
            .expect("cached immutable value should not be empty") ^= 0x80;
        std::fs::write(&cache_path, corrupt).expect("corrupt cached payload without truncating it");
        let repaired = block_on(read.get_many(&request))
            .expect("replace corrupt immutable cache entry from remote");
        assert_eq!(
            repaired.values,
            vec![Some(ProjectedValue::FullValue(value.clone()))]
        );

        let remote_path = ObjectPath::from(join_db_path(db_path, IMMUTABLE_VALUE_PATH))
            .join(blake3::Hash::from_bytes(segment_hash).to_hex().as_str());
        block_on(object_store.delete(&remote_path)).expect("remove remote immutable chunk");
        let cached = block_on(read.get_many(&request))
            .expect("serve immutable chunk from configured disk cache");
        assert_eq!(cached.values, vec![Some(ProjectedValue::FullValue(value))]);
    }

    #[test]
    fn configured_immutable_value_cache_prunes_to_its_byte_bound() {
        let directory = tempfile::tempdir().expect("create bounded immutable chunk cache");
        let cache = ImmutableValueCache::new(
            &SlateDBCacheOptions {
                root_folder: directory.path().to_path_buf(),
                max_disk_cache_bytes: 2048,
                block_cache_bytes: 0,
                metadata_cache_bytes: 0,
            },
            None,
        );
        block_on(cache.put(&Key(Bytes::from(vec![0x11; 32])), Bytes::from(vec![1; 700])));
        block_on(cache.put(&Key(Bytes::from(vec![0x22; 32])), Bytes::from(vec![2; 700])));
        assert!(immutable_value_cache_bytes(&cache.root) <= cache.max_bytes as u64);
    }

    #[test]
    fn local_close_reclaims_a_flushed_wal() {
        let directory = tempfile::tempdir().expect("create local GC storage directory");
        let storage = SlateDB::open(directory.path()).expect("open local GC storage");
        for key in [b"a", b"b", b"c", b"d"] {
            let mut write = block_on(storage.begin_write(WriteOptions::default()))
                .expect("begin local GC write");
            block_on(write.put_many(
                StorageSpace::mutable(SpaceId(7), "test.mutable"),
                PutBatch {
                    entries: vec![PutEntry {
                        key: Key(Bytes::copy_from_slice(key)),
                        value: StoredValue {
                            bytes: Bytes::from_static(b"value"),
                        },
                    }],
                },
            ))
            .expect("stage local GC row");
            block_on(write.commit()).expect("commit local GC row");
            block_on(storage.flush_memtable_for_diagnostics()).expect("flush local GC memtable");
        }
        let wal = directory
            .path()
            .join(DB_PATH)
            .join(SEGMENTED_FORMAT_PATH)
            .join("wal");
        let before = std::fs::read_dir(&wal)
            .expect("list flushed local GC WAL")
            .filter_map(Result::ok)
            .filter(|entry| entry.path().is_file())
            .count();
        assert!(before > 2, "the fixture should create obsolete WAL SSTs");

        drop(storage);

        let after = std::fs::read_dir(&wal)
            .expect("list local GC WAL after close")
            .filter_map(Result::ok)
            .filter(|entry| entry.path().is_file())
            .count();
        assert!(
            after < before,
            "local close should collect obsolete WAL SSTs"
        );
    }

    #[test]
    fn completed_write_reclaims_publication_without_a_read_or_flush() {
        let storage = SlateDB::open_object_store_with_options(
            "test-flush-reclaims-publications",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open publication reclamation storage");
        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin publication");
        block_on(write.put_many(
            StorageSpace::mutable(SpaceId(7), "test.mutable"),
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(Bytes::from_static(b"key")),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"value"),
                    },
                }],
            },
        ))
        .expect("stage publication");
        block_on(write.commit()).expect("commit publication");
        block_on(storage.write_pipeline.wait_for_visible()).expect("wait for publication");
        block_on(storage.worker.wait_for_reclamation()).expect("wait for publication reclamation");

        let state = storage
            .write_pipeline
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            state.visible.is_empty(),
            "persisted overlays must not wait for the next foreground read to be reclaimed"
        );
        drop(state);
        let active_reclaimers = storage
            .worker
            .inner
            .reclamation
            .state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            *active_reclaimers, 0,
            "an explicit flush is a publication-reclamation lifecycle barrier"
        );
    }

    #[test]
    fn cached_open_does_not_preload_ssts() {
        let cache_dir = tempfile::tempdir().expect("create disk-cache directory");
        assert_open_does_not_preload_ssts(
            "test-on-demand-disk-cache",
            SlateDBObjectStoreOptions {
                cache: Some(SlateDBCacheOptions {
                    root_folder: cache_dir.path().join("object-cache"),
                    max_disk_cache_bytes: 16 * 1024 * 1024,
                    block_cache_bytes: 0,
                    metadata_cache_bytes: 0,
                }),
            },
        );
    }

    #[test]
    fn default_memory_cache_does_not_preload_ssts() {
        assert_open_does_not_preload_ssts(
            "test-on-demand-memory-cache",
            SlateDBObjectStoreOptions::default(),
        );
    }

    fn assert_open_does_not_preload_ssts(db_path: &str, options: SlateDBObjectStoreOptions) {
        let inner = Arc::new(InMemory::new());
        let db_path = db_path.to_string();
        seed_compacted_sst(inner.clone(), &db_path);

        let store = Arc::new(BlockingStore::new(inner));
        let blocked_reads = store.block_compacted_reads();
        let (opened_tx, opened_rx) = mpsc::channel();
        let opener = std::thread::spawn(move || {
            opened_tx
                .send(SlateDB::open_object_store_with_options(
                    db_path, store, options,
                ))
                .expect("send cached open result");
        });

        let opened = opened_rx.recv_timeout(Duration::from_secs(5));
        drop(blocked_reads);
        opener.join().expect("join cached opener");
        let storage = opened
            .expect("cached open must not wait for SST reads")
            .expect("open cached SlateDB");
        drop(storage);
    }

    #[test]
    fn default_memory_cache_serves_a_warm_sst_read_without_object_store_access() {
        let inner = Arc::new(InMemory::new());
        let db_path = "test-default-memory-cache-hit";
        seed_compacted_sst(inner.clone(), db_path);

        let store = Arc::new(BlockingStore::new(inner));
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open default-memory-cache storage");

        let first = block_on(storage.worker.call_read(|db| async move {
            let snapshot = db.snapshot().await.map_err(slatedb_error)?;
            snapshot.get(b"\0\0\0\x07key").await.map_err(slatedb_error)
        }))
        .expect("warm raw SlateDB SST read");
        assert_eq!(first, Some(Bytes::from_static(b"value")));

        let blocked_reads = store.block_sst_reads();
        let reader_storage = storage;
        let (result_tx, result_rx) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            let result = block_on(reader_storage.worker.call_read(|db| async move {
                let snapshot = db.snapshot().await.map_err(slatedb_error)?;
                snapshot.get(b"\0\0\0\x07key").await.map_err(slatedb_error)
            }));
            result_tx.send(result).expect("send warm raw read result");
        });

        let second = match result_rx.recv_timeout(Duration::from_secs(2)) {
            Ok(result) => result.expect("warm raw read should succeed"),
            Err(error) => {
                drop(blocked_reads);
                reader.join().expect("join blocked raw reader");
                panic!("warm raw read touched the object store: {error}");
            }
        };
        drop(blocked_reads);
        reader.join().expect("join warm raw reader");
        assert_eq!(second, Some(Bytes::from_static(b"value")));
    }

    fn seed_compacted_sst(inner: Arc<InMemory>, db_path: &str) {
        let physical_db_path = join_db_path(db_path, SEGMENTED_FORMAT_PATH);
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build raw SlateDB test runtime")
            .block_on(async {
                let db = Db::builder(physical_db_path, inner.clone())
                    .with_segment_extractor(Arc::new(StorageSpacePrefixExtractor))
                    .with_settings(slatedb_settings())
                    .with_db_cache_disabled()
                    .build()
                    .await
                    .expect("open raw SlateDB");
                let mut batch = WriteBatch::new();
                batch.put(b"\0\0\0\x07key", b"value");
                db.write_with_options(
                    batch,
                    &SlateDBWriteOptions {
                        await_durable: false,
                        ..SlateDBWriteOptions::default()
                    },
                )
                .await
                .expect("write raw SlateDB row");
                db.flush().await.expect("flush raw SlateDB WAL");
                db.flush_with_options(FlushOptions {
                    flush_type: FlushType::MemTable,
                })
                .await
                .expect("flush raw SlateDB memtable");
                db.close().await.expect("close raw SlateDB");
            });
    }

    #[test]
    fn fresh_storage_uses_versioned_segmented_format() {
        let store = Arc::new(InMemory::new());
        let db_path = "test-zstd-physical-format";
        let space = StorageSpace::mutable(SpaceId(7), "test.mutable");
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open fresh Zstd storage");

        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin Zstd write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(Bytes::from_static(b"zstd-key")),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"zstd-value"),
                    },
                }],
            },
        ))
        .expect("stage Zstd row");
        block_on(write.commit()).expect("commit Zstd row");
        block_on(storage.flush()).expect("flush Zstd row");
        block_on(storage.worker.call(move |db| async move {
            db.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .map_err(slatedb_error)?;
            assert_eq!(
                db.manifest().segment_extractor_name(),
                Some(SPACE_PREFIX_EXTRACTOR_NAME),
                "new physical database must persist the Lix storage-space extractor"
            );
            assert!(
                db.manifest()
                    .segment(&space.id.0.to_be_bytes())
                    .is_some_and(|segment| !segment.l0().is_empty()),
                "the row must be isolated in its storage-space segment"
            );
            assert!(
                db.manifest()
                    .segments()
                    .iter()
                    .flat_map(|segment| segment.l0())
                    .any(|view| view.sst.info.compression_codec == Some(CompressionCodec::Zstd)),
                "new physical SST must record the Zstd codec"
            );
            Ok(())
        }))
        .expect("flush and inspect Zstd SST");
        drop(storage);

        let physical_prefix = format!("{db_path}/{SEGMENTED_FORMAT_PATH}/");
        let object_paths = block_on(async {
            let mut objects = store.list(None);
            let mut paths = Vec::new();
            while let Some(object) = objects.next().await {
                paths.push(
                    object
                        .expect("list fresh Zstd storage object")
                        .location
                        .to_string(),
                );
            }
            paths
        });
        assert!(!object_paths.is_empty(), "fresh storage must write objects");
        assert!(
            object_paths
                .iter()
                .all(|path| path.starts_with(&physical_prefix)),
            "all objects must use the versioned segmented namespace: {object_paths:?}"
        );
    }

    #[test]
    fn open_object_store_round_trips_with_memory_store() {
        let storage = SlateDB::open_object_store_with_options(
            "test-db",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open memory object-store slatedb storage");

        let space = StorageSpace::mutable(SpaceId(7), "test.mutable");
        let key = Key(Bytes::from_static(b"hello"));
        let value = Bytes::from_static(b"world");

        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: value.clone(),
                    },
                }],
            },
        ))
        .expect("put row");
        block_on(write.commit()).expect("commit row");

        let read = block_on(storage.begin_read(ReadOptions::default())).expect("begin read");
        let result = block_on(read.get_many(&[GetManyRequest {
            space,
            keys: std::slice::from_ref(&key),
            opts: GetOptions::default(),
        }]))
        .expect("read row");

        assert_eq!(result.values, vec![Some(ProjectedValue::FullValue(value))]);
        assert_eq!(
            block_on(read.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions {
                    projection: CoreProjection::KeyOnly,
                },
            }]))
            .expect("read singleton key only")
            .values,
            vec![Some(ProjectedValue::KeyOnly)]
        );
        assert_eq!(
            block_on(read.get_many(&[GetManyRequest {
                space,
                keys: &[Key(Bytes::from_static(b"missing"))],
                opts: GetOptions::default(),
            }]))
            .expect("read singleton missing key")
            .values,
            vec![None]
        );
    }

    #[test]
    fn snapshot_scan_cursor_preserves_lookahead_and_falls_back_safely() {
        let storage = SlateDB::open_object_store_with_options(
            "test-snapshot-scan-cursor",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open cursor-test storage");
        let space = StorageSpace::mutable(SpaceId(7), "test.mutable");
        let keys = [b"a", b"b", b"c"].map(|bytes| Key(Bytes::from_static(bytes)));
        let mut write = block_on(storage.begin_write(WriteOptions::default()))
            .expect("begin cursor-test write");
        block_on(
            write.put_many(
                space,
                PutBatch {
                    entries: keys
                        .iter()
                        .cloned()
                        .map(|key| PutEntry {
                            value: StoredValue {
                                bytes: key.0.clone(),
                            },
                            key,
                        })
                        .collect(),
                },
            ),
        )
        .expect("stage cursor-test rows");
        block_on(write.commit()).expect("commit cursor-test rows");

        let read =
            block_on(storage.begin_read(ReadOptions::default())).expect("begin cursor-test read");
        let range = KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        };
        let mut cursor = block_on(read.begin_scan(
            space,
            range.clone(),
            BeginScanOptions {
                projection: CoreProjection::FullValue,
                ..BeginScanOptions::default()
            },
        ))
        .expect("begin cursor-test scan");
        let (first, first_has_more) = block_on(cursor.next_page(1))
            .expect("scan first cursor-test page")
            .into_parts();
        assert_eq!(first[0].key, keys[0]);
        assert!(first_has_more);
        let (second, second_has_more) = block_on(cursor.next_page(1))
            .expect("scan second cursor-test page")
            .into_parts();
        assert_eq!(second[0].key, keys[1]);
        assert!(second_has_more);

        // A changed projection requires a new cursor with an explicit exclusive
        // authenticated restart boundary.
        let mut key_cursor = block_on(read.begin_scan(
            space,
            KeyRange {
                lower: Bound::Excluded(keys[1].clone()),
                upper: Bound::Unbounded,
            },
            BeginScanOptions {
                projection: CoreProjection::KeyOnly,
                ..BeginScanOptions::default()
            },
        ))
        .expect("begin projected restart scan");
        let (third, third_has_more) = block_on(key_cursor.next_page(1))
            .expect("scan projected restart page")
            .into_parts();
        assert_eq!(third[0].key, keys[2]);
        assert_eq!(third[0].value, ProjectedValue::KeyOnly);
        assert!(!third_has_more);

        let mut restarted_cursor = block_on(read.begin_scan(
            space,
            range,
            BeginScanOptions {
                projection: CoreProjection::FullValue,
                ..BeginScanOptions::default()
            },
        ))
        .expect("begin restarted cursor-test scan");
        let (restarted, _restarted_has_more) = block_on(restarted_cursor.next_page(1))
            .expect("scan restarted first page")
            .into_parts();
        assert_eq!(restarted[0].key, keys[0]);
        let (restarted_second, __restarted_second_has_more) =
            block_on(restarted_cursor.next_page(1))
                .expect("scan restarted second page")
                .into_parts();
        assert_eq!(restarted_second[0].key, keys[1]);
    }

    #[test]
    fn batched_point_preconditions_preserve_duplicate_and_mixed_failure_indexes() {
        let storage = SlateDB::open_object_store_with_options(
            "test-batched-point-preconditions",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open memory object-store slatedb storage");
        let space = StorageSpace::mutable(SpaceId(7), "test.mutable");
        let present = Key(Bytes::from_static(b"present"));
        let missing = Key(Bytes::from_static(b"missing"));
        let value = Bytes::from_static(b"value");
        let value_hash = *blake3::hash(&value).as_bytes();

        let mut seed =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin seed write");
        block_on(seed.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: present.clone(),
                    value: StoredValue {
                        bytes: value.clone(),
                    },
                }],
            },
        ))
        .expect("stage seed value");
        block_on(seed.commit()).expect("commit seed value");

        let passing = block_on(storage.begin_write(WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space,
                    key: present.clone(),
                    expected: value.clone(),
                },
                Precondition::KeyPresent {
                    space,
                    key: present.clone(),
                },
                Precondition::KeyAbsent {
                    space,
                    key: missing.clone(),
                },
            ],
            ..WriteOptions::default()
        }))
        .expect("begin passing batched point preconditions");
        block_on(passing.commit()).expect("all batched point preconditions pass");

        let failing = block_on(storage.begin_write(WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space,
                    key: present.clone(),
                    expected: value,
                },
                Precondition::KeyAbsent {
                    space,
                    key: present.clone(),
                },
                Precondition::RangeEmpty {
                    space,
                    range: KeyRange {
                        lower: Bound::Included(present.clone()),
                        upper: Bound::Included(present.clone()),
                    },
                },
                Precondition::KeyPresent {
                    space,
                    key: missing,
                },
                Precondition::KeyAbsent {
                    space,
                    key: present.clone(),
                },
                Precondition::BranchEquals {
                    ref_key: Key(Bytes::from_static(b"branch-ref")),
                    expected: Bytes::from_static(b"ignored"),
                },
                Precondition::KeyValueHashEquals {
                    space,
                    key: present,
                    hash: value_hash,
                },
            ],
            ..WriteOptions::default()
        }))
        .expect("begin mixed failed preconditions");
        let error = block_on(failing.commit())
            .err()
            .expect("mixed failed preconditions report every original index");

        assert_eq!(
            error,
            StorageError::PreconditionFailed(vec![
                PreconditionFailure { index: 1 },
                PreconditionFailure { index: 2 },
                PreconditionFailure { index: 3 },
                PreconditionFailure { index: 4 },
                PreconditionFailure { index: 5 },
            ])
        );
    }

    #[test]
    fn pending_publication_is_visible_to_points_scans_and_preconditions() {
        let storage = SlateDB::open_object_store_with_options(
            "test-pending-publication-overlay",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open pending-publication storage");
        let blocker = Arc::new(WriteCompletion::new());
        storage
            .write_pipeline
            .state
            .lock()
            .expect("lock write pipeline")
            .tail = Some(Arc::clone(&blocker));

        let space = StorageSpace::mutable(SpaceId(7), "test.mutable");
        let key = Key(Bytes::from_static(b"pending"));
        let value = Bytes::from_static(b"value");
        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin pending write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: value.clone(),
                    },
                }],
            },
        ))
        .expect("stage pending write");
        block_on(write.commit()).expect("publish pending write");

        let read =
            block_on(storage.begin_read(ReadOptions::default())).expect("begin overlay read");
        assert_eq!(
            block_on(read.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions::default(),
            }]))
            .expect("read pending point")
            .values,
            vec![Some(ProjectedValue::FullValue(value.clone()))]
        );
        let mut cursor = block_on(read.begin_scan(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions::default(),
        ))
        .expect("begin pending point scan");
        assert_eq!(
            block_on(cursor.next_page(usize::MAX))
                .expect("scan pending point")
                .into_parts()
                .0,
            vec![ReadEntry {
                key: key.clone(),
                value: ProjectedValue::FullValue(value.clone()),
            }]
        );

        let checked = block_on(storage.begin_write(WriteOptions {
            preconditions: vec![Precondition::KeyValueEquals {
                space,
                key,
                expected: value,
            }],
            ..WriteOptions::default()
        }))
        .expect("begin pending point precondition");
        block_on(checked.commit()).expect("pending point satisfies the next writer precondition");

        blocker.complete(Ok(0));
        block_on(storage.flush()).expect("flush released pending write");
    }

    #[test]
    fn snapshot_point_cache_advances_only_across_contiguous_local_sequences() {
        let cache = SnapshotPointCache::new();
        let unchanged = Key(Bytes::from_static(b"unchanged"));
        let updated = Key(Bytes::from_static(b"updated"));
        let deleted = Key(Bytes::from_static(b"deleted"));
        cache.observe_snapshot(7);
        cache.insert(7, unchanged.clone(), Some(Bytes::from_static(b"stable")));
        cache.insert(7, updated.clone(), Some(Bytes::from_static(b"old")));
        cache.insert(7, deleted.clone(), Some(Bytes::from_static(b"present")));

        let overlay = Arc::new(BTreeMap::from([
            (updated.clone(), Some(Bytes::from_static(b"new"))),
            (deleted.clone(), None),
        ]));
        cache.advance_local_write(8, &[overlay]);

        assert_eq!(
            cache.get(8, &unchanged),
            Some(Some(Bytes::from_static(b"stable")))
        );
        assert_eq!(
            cache.get(8, &updated),
            Some(Some(Bytes::from_static(b"new")))
        );
        assert_eq!(cache.get(8, &deleted), Some(None));
        assert_eq!(cache.get(7, &unchanged), None);
    }

    #[test]
    fn snapshot_point_cache_clears_unchanged_values_on_external_sequence_jump() {
        let cache = SnapshotPointCache::new();
        let stale = Key(Bytes::from_static(b"stale"));
        let locally_written = Key(Bytes::from_static(b"local"));
        cache.observe_snapshot(11);
        cache.insert(11, stale.clone(), Some(Bytes::from_static(b"old")));

        cache.observe_snapshot(15);
        assert_eq!(cache.get(15, &stale), None);

        cache.advance_local_write(
            16,
            &[Arc::new(BTreeMap::from([(
                locally_written.clone(),
                Some(Bytes::from_static(b"new")),
            )]))],
        );
        assert_eq!(cache.get(16, &stale), None);
        assert_eq!(
            cache.get(16, &locally_written),
            Some(Some(Bytes::from_static(b"new")))
        );
    }

    #[test]
    fn visible_point_cache_isolated_by_snapshot_sequence() {
        let storage = SlateDB::open_object_store_with_options(
            "test-snapshot-point-cache",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open snapshot cache storage");
        let space = StorageSpace::mutable(SpaceId(7), "test.mutable");
        let key = Key(Bytes::from_static(b"versioned-key"));

        let mut initial =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin initial write");
        block_on(initial.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"first"),
                    },
                }],
            },
        ))
        .expect("stage initial value");
        block_on(initial.commit()).expect("commit initial value");

        let before_update =
            block_on(storage.begin_read(ReadOptions::default())).expect("begin old snapshot");
        let before_update_cache_key = before_update
            .snapshot_cache_key()
            .expect("SlateDB read should expose a snapshot cache key");
        assert_eq!(
            block_on(before_update.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions::default(),
            }]))
            .expect("read old snapshot")
            .values,
            vec![Some(ProjectedValue::FullValue(Bytes::from_static(
                b"first"
            )))]
        );

        let mut update =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin update write");
        block_on(update.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"second"),
                    },
                }],
            },
        ))
        .expect("stage updated value");
        block_on(update.commit()).expect("commit updated value");

        let after_update =
            block_on(storage.begin_read(ReadOptions::default())).expect("begin new snapshot");
        assert_ne!(
            before_update_cache_key,
            after_update
                .snapshot_cache_key()
                .expect("updated SlateDB read should expose a snapshot cache key")
        );
        assert_eq!(
            block_on(after_update.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions::default(),
            }]))
            .expect("read new snapshot")
            .values,
            vec![Some(ProjectedValue::FullValue(Bytes::from_static(
                b"second"
            )))]
        );
        assert_eq!(
            block_on(before_update.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions::default(),
            }]))
            .expect("reread old snapshot")
            .values,
            vec![Some(ProjectedValue::FullValue(Bytes::from_static(
                b"first"
            )))],
            "an old snapshot must not observe the value cached for a newer sequence"
        );
    }

    #[test]
    fn snapshot_point_cache_batch_preserves_hits_misses_and_duplicates() {
        let cache = SnapshotPointCache::new();
        let present = Key(Bytes::from_static(b"present"));
        let missing = Key(Bytes::from_static(b"cached-missing"));
        let unseen = Key(Bytes::from_static(b"unseen"));
        let value = Bytes::from_static(b"value");
        cache.insert(7, present.clone(), Some(value.clone()));
        cache.insert(7, missing.clone(), None);

        let keys = [present.clone(), missing.clone(), unseen, present.clone()];
        let mut values = vec![None; keys.len()];
        cache.get_many(7, &keys, &mut values);
        assert_eq!(
            values,
            vec![
                Some(Some(value.clone())),
                Some(None),
                None,
                Some(Some(value))
            ]
        );
        let keys = [present, missing];
        let mut values = vec![None; keys.len()];
        cache.get_many(8, &keys, &mut values);
        assert_eq!(values, vec![None, None]);
    }

    #[test]
    fn snapshot_point_cache_limits_entries_with_one_snapshot_bucket() {
        let cache = SnapshotPointCache::new();
        let first = Key(Bytes::from_static(b"cache-entry-0000"));
        for index in 0..=SNAPSHOT_POINT_CACHE_ENTRIES {
            cache.insert(
                7,
                Key(Bytes::from(format!("cache-entry-{index:04}"))),
                Some(Bytes::from_static(b"value")),
            );
        }

        let keys = [first];
        let mut values = [None];
        cache.get_many(7, &keys, &mut values);
        assert_eq!(values, [None]);
        let keys = [Key(Bytes::from(format!(
            "cache-entry-{SNAPSHOT_POINT_CACHE_ENTRIES:04}"
        )))];
        let mut values = [None];
        cache.get_many(7, &keys, &mut values);
        assert_eq!(values, [Some(Some(Bytes::from_static(b"value")))]);
    }

    #[test]
    fn fenced_writer_reports_a_terminal_error_after_slatedb_closes_it() {
        let object_store = Arc::new(InMemory::new());
        let db_path = "test-fenced-writer";
        let first = SlateDB::open_object_store_with_options(
            db_path,
            object_store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open first SlateDB writer");
        let space = StorageSpace::mutable(SpaceId(10), "test.mutable");

        let mut seed =
            block_on(first.begin_write(WriteOptions::default())).expect("begin seed write");
        block_on(seed.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(Bytes::from_static(b"before-fence")),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"value"),
                    },
                }],
            },
        ))
        .expect("stage seed write");
        block_on(seed.commit()).expect("commit seed write");
        block_on(first.flush()).expect("durably flush seed write");

        let _second = SlateDB::open_object_store_with_options(
            db_path,
            object_store,
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open newer SlateDB writer");

        // A newer writer fences this one asynchronously through SlateDB's
        // manifest poll. Wait for that terminal state before asserting that a
        // subsequent commit is rejected.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match block_on(first.begin_read(ReadOptions::default())) {
                Err(StorageError::Fenced) => break,
                Ok(read) => drop(read),
                Err(error) => panic!("old writer returned the wrong error after fencing: {error}"),
            }
            assert!(
                Instant::now() < deadline,
                "SlateDB did not close the fenced writer within the test deadline"
            );
            std::thread::sleep(Duration::from_millis(10));
        }

        let mut fenced =
            block_on(first.begin_write(WriteOptions::default())).expect("begin fenced write");
        block_on(fenced.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(Bytes::from_static(b"after-fence")),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"value"),
                    },
                }],
            },
        ))
        .expect("stage fenced write");
        let error = block_on(fenced.commit()).expect_err("fenced writer must reject commits");

        assert_eq!(error, StorageError::Fenced);
    }

    #[test]
    fn closed_slatedb_panic_is_a_distinct_terminal_storage_error() {
        let error =
            slatedb::Error::closed("background worker panicked".to_string(), CloseReason::Panic);

        assert!(matches!(slatedb_error(error), StorageError::Closed(_)));
    }

    #[test]
    fn commit_is_visible_while_background_wal_flush_is_blocked() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let storage = SlateDB::open_object_store_with_options(
            "test-commit-visibility",
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open commit visibility storage");
        let space = StorageSpace::mutable(SpaceId(8), "test.mutable");
        let key = Key(Bytes::from_static(b"visible-before-durable"));
        let queued_key = Key(Bytes::from_static(b"visible-while-draining"));

        let blocked_write = store.block_next_write();
        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin visibility write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"value"),
                    },
                }],
            },
        ))
        .expect("stage visibility write");
        block_on(write.commit()).expect("publish visibility value");

        // The request has returned, but SlateDB's first background WAL upload
        // is still in flight.
        blocked_write.wait_for_entries(1, "SlateDB WAL write");

        let mut queued =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin queued write");
        block_on(queued.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: queued_key.clone(),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"queued"),
                    },
                }],
            },
        ))
        .expect("stage queued write");
        block_on(queued.commit()).expect("publish queued value");

        let read = block_on(storage.begin_read(ReadOptions::default()))
            .expect("begin visible in-memory read");
        let values = block_on(read.get_many(&[GetManyRequest {
            space,
            keys: &[key, queued_key],
            opts: GetOptions::default(),
        }]))
        .expect("read visible in-memory value")
        .values;
        assert_eq!(
            values,
            vec![
                Some(ProjectedValue::FullValue(Bytes::from_static(b"value"))),
                Some(ProjectedValue::FullValue(Bytes::from_static(b"queued"))),
            ]
        );

        drop(blocked_write);
        block_on(storage.flush()).expect("flush visible value");
    }

    #[test]
    fn persisted_publication_remains_visible_to_an_older_active_view() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let storage = SlateDB::open_object_store_with_options(
            "test-active-publication-view",
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open active-publication storage");
        let space = StorageSpace::mutable(SpaceId(8), "test.mutable");
        let key = Key(Bytes::from_static(b"active-view"));
        let value = Bytes::from_static(b"value");

        let blocked_write = store.block_next_write();
        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin blocked write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: value.clone(),
                    },
                }],
            },
        ))
        .expect("stage blocked value");
        block_on(write.commit()).expect("publish blocked value");
        blocked_write.wait_for_entries(1, "SlateDB WAL write");

        let older = block_on(storage.begin_read(ReadOptions::default()))
            .expect("capture pre-persistence view");
        drop(blocked_write);
        block_on(storage.flush()).expect("persist publication");
        let newer =
            block_on(storage.begin_read(ReadOptions::default())).expect("capture persisted view");

        assert_eq!(
            block_on(older.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions::default(),
            }]))
            .expect("read publication through older point view")
            .values,
            vec![Some(ProjectedValue::FullValue(value.clone()))]
        );
        let mut cursor = block_on(older.begin_scan(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions::default(),
        ))
        .expect("begin publication scan through older view");
        assert_eq!(
            block_on(cursor.next_page(usize::MAX))
                .expect("read publication through older scan view")
                .into_parts()
                .0,
            vec![ReadEntry {
                key,
                value: ProjectedValue::FullValue(value),
            }]
        );

        drop(cursor);
        drop(older);
        assert!(
            storage
                .write_pipeline
                .state
                .lock()
                .expect("lock publication state")
                .visible
                .is_empty(),
            "publication is reclaimed after its last dependent view"
        );
        drop(newer);
    }

    #[test]
    fn dropping_unrelated_view_does_not_reclaim_publication_a_stale_fetch_needs() {
        let pipeline = WritePipeline::new();
        let unrelated = pipeline.capture(1);
        let key = Key(Bytes::from_static(b"stale-fetch"));
        let value = Bytes::from_static(b"published");
        let published = Arc::new(PublishedWrite {
            publication_id: 1,
            overlay: Arc::new(BTreeMap::from([(key.clone(), Some(value.clone()))])),
            persisted_sequence: AtomicU64::new(2),
        });
        {
            let mut state = pipeline.state.lock().expect("lock publication state");
            state.next_publication_id = 1;
            state.newest_snapshot_sequence = 2;
            state.snapshot_fetches = 1;
            state.visible.push_back(published);
        }

        // Model a snapshot fetch that started at sequence 1 before the
        // publication persisted. Dropping an older unrelated view in the gap
        // must not discard the overlay before that fetch captures its view.
        drop(unrelated);
        let stale_fetch = pipeline.capture(1);
        {
            let mut state = pipeline.state.lock().expect("complete snapshot fetch");
            state.snapshot_fetches = 0;
            let retired = cleanup_publications(&mut state, 2);
            assert!(
                retired.is_empty(),
                "the newly registered stale view still needs the publication"
            );
        }

        assert_eq!(
            pipeline.point_value(
                stale_fetch.snapshot_sequence,
                stale_fetch.publication_id,
                &key,
            ),
            Some(Some(value))
        );
    }

    fn cached_snapshot_test_storage(name: &'static str) -> SlateDB {
        SlateDB::open_object_store_with_options(
            name,
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open cached-snapshot coherence storage")
    }

    async fn cached_snapshot_commit(storage: &SlateDB, space: StorageSpace, value: &'static str) {
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .expect("begin cached-snapshot write");
        write
            .put_many(
                space,
                PutBatch {
                    entries: vec![PutEntry {
                        key: Key(Bytes::from_static(b"a")),
                        value: StoredValue {
                            bytes: Bytes::from_static(value.as_bytes()),
                        },
                    }],
                },
            )
            .await
            .expect("stage cached-snapshot write");
        write.commit().await.expect("commit cached-snapshot write");
    }

    fn snapshot_fetches_of(storage: &SlateDB) -> usize {
        storage
            .write_pipeline
            .state
            .lock()
            .expect("lock publication state")
            .snapshot_fetches
    }

    /// D2. Companion to
    /// `dropping_unrelated_view_does_not_reclaim_publication_a_stale_fetch_needs`,
    /// which proves the `snapshot_fetches` guard protects a reader in the gap
    /// between obtaining a snapshot and capturing its publication view — but
    /// only ever exercises the fetch path, pinning `snapshot_fetches = 1`.
    ///
    /// The cached fast path used to return no guard, so a cached reader sat in
    /// the identical gap with the protection switched off and
    /// `cleanup_publications` could retire the publication its older snapshot
    /// still needed. Both paths must register the guard.
    #[tokio::test]
    async fn cached_snapshot_path_registers_a_retirement_guard() {
        let storage = cached_snapshot_test_storage("test-cached-snapshot-guard");
        let space = StorageSpace::mutable(SpaceId(0x91), "test.cached-snapshot-guard");
        cached_snapshot_commit(&storage, space, "A").await;

        let (snapshot, fetch) = storage
            .write_pipeline
            .snapshot(&storage.worker)
            .await
            .expect("fetch a snapshot");
        drop(fetch);
        assert_eq!(snapshot_fetches_of(&storage), 0);

        // Force the cached fast path deterministically.
        storage
            .write_pipeline
            .state
            .lock()
            .expect("lock publication state")
            .latest_snapshot = Some(Arc::clone(&snapshot));

        let (_cached, cached_fetch) = storage
            .write_pipeline
            .snapshot(&storage.worker)
            .await
            .expect("take the cached snapshot path");
        assert_eq!(
            snapshot_fetches_of(&storage),
            1,
            "the cached snapshot path must register the same retirement guard as the fetch path"
        );
        drop(cached_fetch);
        assert_eq!(
            snapshot_fetches_of(&storage),
            0,
            "dropping the guard must balance the counter"
        );
    }

    /// D1. `commit` clears `latest_snapshot` and bumps `next_publication_id`
    /// so the next reader refetches. That invalidation must not be undone by an
    /// install decided before the commit landed.
    ///
    /// The state below is arranged so that **only** the publication-id term can
    /// reject the install: the racing publication has already persisted and
    /// been retired from `visible`, so `snapshot_covers_persisted_publications`
    /// holds, the tail is complete, and `latest_snapshot` is empty, so the
    /// monotonic guard passes vacuously. That is precisely the state the old
    /// two-phase structure reached by the time it called `install_snapshot`.
    #[tokio::test]
    async fn commit_is_not_undone_by_an_install_decided_before_it() {
        let storage = cached_snapshot_test_storage("test-cached-snapshot-install-race");
        let space = StorageSpace::mutable(SpaceId(0x92), "test.cached-snapshot-install-race");
        cached_snapshot_commit(&storage, space, "A").await;

        let (stale_snapshot, fetch) = storage
            .write_pipeline
            .snapshot(&storage.worker)
            .await
            .expect("fetch the pre-commit snapshot");
        let publication_id = storage
            .write_pipeline
            .state
            .lock()
            .expect("lock publication state")
            .next_publication_id;
        drop(fetch);

        // The commit that races the in-flight install, then fully settles:
        // its publication persists and retires, leaving only the publication
        // id to witness that the snapshot is stale.
        cached_snapshot_commit(&storage, space, "B").await;
        {
            let mut state = storage
                .write_pipeline
                .state
                .lock()
                .expect("lock publication state");
            state.visible.clear();
            state.tail = None;
            state.latest_snapshot = None;
            assert_ne!(
                state.next_publication_id, publication_id,
                "the racing commit must advance the publication id"
            );
            assert!(
                snapshot_covers_persisted_publications(&state, stale_snapshot.seq()),
                "every other cacheability term must hold, isolating the publication-id check"
            );
        }

        assert!(
            !storage
                .write_pipeline
                .try_install_snapshot(publication_id, &stale_snapshot),
            "an install decided before the commit must be rejected"
        );
        assert!(
            storage
                .write_pipeline
                .state
                .lock()
                .expect("lock publication state")
                .latest_snapshot
                .is_none(),
            "the commit's cache invalidation must not be undone by a stale install"
        );
    }

    #[test]
    fn stale_snapshot_is_not_cacheable_after_publication_persists() {
        let mut state = WritePipelineState::default();
        state.visible.push_back(Arc::new(PublishedWrite {
            publication_id: 1,
            overlay: Arc::new(BTreeMap::new()),
            persisted_sequence: AtomicU64::new(2),
        }));

        assert!(!snapshot_covers_persisted_publications(&state, 1));
        assert!(snapshot_covers_persisted_publications(&state, 2));
    }

    #[test]
    fn durable_reads_exclude_writes_awaiting_wal_upload() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let storage = SlateDB::open_object_store_with_options(
            "test-durable-read-filter",
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open durable read storage");
        let space = StorageSpace::mutable(SpaceId(8), "test.mutable");
        let key = Key(Bytes::from_static(b"visible-before-remote-durable"));
        let value = Bytes::from_static(b"value");

        let blocked_write = store.block_next_write();
        let committer_storage = storage.clone();
        let committer_key = key.clone();
        let committer_value = value.clone();
        let (commit_tx, commit_rx) = mpsc::channel();
        let committer = std::thread::spawn(move || {
            let mut write = block_on(committer_storage.begin_write(WriteOptions::default()))
                .expect("begin durable read write");
            block_on(write.put_many(
                space,
                PutBatch {
                    entries: vec![PutEntry {
                        key: committer_key,
                        value: StoredValue {
                            bytes: committer_value,
                        },
                    }],
                },
            ))
            .expect("stage durable read row");
            commit_tx
                .send(block_on(write.commit()))
                .expect("send durable read commit result");
        });

        blocked_write.wait_for_entries(1, "SlateDB WAL write");
        commit_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("commit should complete after local publication")
            .expect("commit visible durable read row");
        committer.join().expect("join durable read committer");
        let visible =
            block_on(storage.begin_read(ReadOptions::default())).expect("begin visible read");
        assert_eq!(
            block_on(visible.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions::default(),
            }]))
            .expect("read visible value")
            .values,
            vec![Some(ProjectedValue::FullValue(value.clone()))],
            "the ordinary read tier may include published in-memory state"
        );
        let durable = block_on(storage.begin_read(ReadOptions {
            durability: ReadDurability::Durable,
            ..ReadOptions::default()
        }))
        .expect("begin remote-durable read");
        assert_eq!(
            block_on(durable.get_many(&[GetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: GetOptions::default(),
            }]))
            .expect("read remote-durable value")
            .values,
            vec![None],
            "a remote-durable read must not claim a blocked WAL upload persisted"
        );

        drop(blocked_write);
        block_on(storage.flush()).expect("flush published durable read row");

        let durable = block_on(storage.begin_read(ReadOptions {
            durability: ReadDurability::Durable,
            ..ReadOptions::default()
        }))
        .expect("begin completed remote-durable read");
        assert_eq!(
            block_on(durable.get_many(&[GetManyRequest {
                space,
                keys: &[key],
                opts: GetOptions::default(),
            }]))
            .expect("read completed remote-durable value")
            .values,
            vec![Some(ProjectedValue::FullValue(value))]
        );
    }

    #[test]
    fn await_durable_write_does_not_acknowledge_a_blocked_wal_upload() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let storage = SlateDB::open_object_store_with_options(
            "test-await-durable-write",
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open await-durable storage");
        let blocked_write = store.block_next_write();
        let committer_storage = storage.clone();
        let (commit_tx, commit_rx) = mpsc::channel();
        let committer = std::thread::spawn(move || {
            let mut write = block_on(committer_storage.begin_write(WriteOptions {
                await_durable: true,
                ..WriteOptions::default()
            }))
            .expect("begin await-durable write");
            block_on(write.put_many(
                StorageSpace::mutable(SpaceId(8), "test.mutable"),
                PutBatch {
                    entries: vec![PutEntry {
                        key: Key(Bytes::from_static(b"durable-before-ack")),
                        value: StoredValue {
                            bytes: Bytes::from_static(b"value"),
                        },
                    }],
                },
            ))
            .expect("stage await-durable row");
            commit_tx
                .send(block_on(write.commit()))
                .expect("send await-durable result");
        });

        blocked_write.wait_for_entries(1, "await-durable SlateDB WAL write");
        assert!(
            commit_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "durable write must not acknowledge before its WAL upload completes",
        );
        drop(blocked_write);
        commit_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("durable write should finish after WAL upload")
            .expect("await-durable commit");
        committer.join().expect("join await-durable committer");
    }

    #[test]
    fn explicit_flush_reports_background_durability_failure() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let storage = SlateDB::open_object_store_with_options(
            "test-failed-commit",
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open failed commit storage");
        let space = StorageSpace::mutable(SpaceId(9), "test.mutable");
        let key = Key(Bytes::from_static(b"rejected"));

        let blocked_write = store.block_next_write();
        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin buffered write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key,
                    value: StoredValue {
                        bytes: Bytes::from_static(b"not-durable"),
                    },
                }],
            },
        ))
        .expect("stage buffered write");
        block_on(write.commit()).expect("publish buffered write");

        blocked_write.wait_for_entries(1, "failing background WAL write");
        store.fail_writes();
        drop(blocked_write);
        let flush_error = block_on(storage.flush()).expect_err("WAL flush must fail");
        assert!(
            matches!(flush_error, StorageError::Io(message) if message.contains("injected write failure")),
            "flush should preserve the SlateDB write error"
        );
    }

    #[test]
    fn dropping_last_handle_waits_for_background_flush() {
        let store = Arc::new(BlockingStore::new(Arc::new(InMemory::new())));
        let db_path = "test-close-background-durability";
        let space = StorageSpace::mutable(SpaceId(8), "test.mutable");
        let key = Key(Bytes::from_static(b"background-commit"));
        let value = Bytes::from_static(b"durable");
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open close-test storage");
        let mut write =
            block_on(storage.begin_write(WriteOptions::default())).expect("begin close-test write");
        block_on(write.put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: value.clone(),
                    },
                }],
            },
        ))
        .expect("stage close-test value");

        let blocked_write = store.block_next_write();
        block_on(write.commit()).expect("publish close-test value");
        blocked_write.wait_for_entries(1, "background commit WAL write");

        let (closed_tx, closed_rx) = mpsc::channel();
        let closer = std::thread::spawn(move || {
            drop(storage);
            let _ = closed_tx.send(());
        });
        assert!(
            matches!(
                closed_rx.recv_timeout(Duration::from_millis(50)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "close must wait for the background WAL flush"
        );
        drop(blocked_write);
        closed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("close should finish after WAL durability");
        closer.join().expect("join close-test closer");

        let reopened = SlateDB::open_object_store_with_options(
            db_path,
            store,
            SlateDBObjectStoreOptions::default(),
        )
        .expect("reopen close-test storage");
        let read =
            block_on(reopened.begin_read(ReadOptions::default())).expect("begin close-test read");
        let result = block_on(read.get_many(&[GetManyRequest {
            space,
            keys: &[key],
            opts: GetOptions::default(),
        }]))
        .expect("read close-test value");
        assert_eq!(result.values, vec![Some(ProjectedValue::FullValue(value))]);
    }

    #[test]
    fn cloned_snapshot_reads_overlap() {
        let inner = Arc::new(InMemory::new());
        let db_path = "test-concurrent-reads";
        let space = StorageSpace::mutable(SpaceId(9), "test.mutable");
        let left_key = Key(Bytes::from_static(b"left"));
        let right_key = Key(Bytes::from_static(b"right"));
        let value = Bytes::from(vec![b'x'; 128 * 1024]);

        {
            let storage = SlateDB::open_object_store_with_options(
                db_path,
                inner.clone(),
                SlateDBObjectStoreOptions::default(),
            )
            .expect("open concurrent-read seed storage");
            let mut write = block_on(storage.begin_write(WriteOptions::default()))
                .expect("begin concurrent-read seed write");
            block_on(write.put_many(
                space,
                PutBatch {
                    entries: vec![
                        PutEntry {
                            key: left_key.clone(),
                            value: StoredValue {
                                bytes: value.clone(),
                            },
                        },
                        PutEntry {
                            key: right_key.clone(),
                            value: StoredValue {
                                bytes: value.clone(),
                            },
                        },
                    ],
                },
            ))
            .expect("stage concurrent-read seed values");
            block_on(write.commit()).expect("commit concurrent-read seed values");
        }

        let store = Arc::new(BlockingStore::new(inner));
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("reopen concurrent-read storage");
        let read = Arc::new(
            block_on(storage.begin_read(ReadOptions::default()))
                .expect("begin shared snapshot read"),
        );
        let left_read = Arc::clone(&read);
        let right_read = Arc::clone(&read);
        let blocked_reads = store.block_sst_reads();

        let left = std::thread::spawn(move || {
            block_on(left_read.get_many(&[GetManyRequest {
                space,
                keys: &[left_key],
                opts: GetOptions::default(),
            }]))
        });
        blocked_reads.wait_for_entries(1, "first SST read");
        let right = std::thread::spawn(move || {
            block_on(right_read.get_many(&[GetManyRequest {
                space,
                keys: &[right_key],
                opts: GetOptions::default(),
            }]))
        });
        blocked_reads.wait_for_entries(2, "second concurrent SST read");
        drop(blocked_reads);

        assert_eq!(
            left.join()
                .expect("join left read")
                .expect("read left value")
                .values,
            vec![Some(ProjectedValue::FullValue(value.clone()))]
        );
        assert_eq!(
            right
                .join()
                .expect("join right read")
                .expect("read right value")
                .values,
            vec![Some(ProjectedValue::FullValue(value))]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_object_store_read_yields_to_executor() {
        let inner = Arc::new(InMemory::new());
        let db_path = "test-async-read-yields";
        let space = StorageSpace::mutable(SpaceId(10), "test.mutable");
        let key = Key(Bytes::from_static(b"remote-key"));
        let value = Bytes::from(vec![b'x'; 128 * 1024]);

        {
            let storage = SlateDB::open_object_store_with_options(
                db_path,
                inner.clone(),
                SlateDBObjectStoreOptions::default(),
            )
            .expect("open async-read seed storage");
            let mut write = storage
                .begin_write(WriteOptions::default())
                .await
                .expect("begin async-read seed write");
            write
                .put_many(
                    space,
                    PutBatch {
                        entries: vec![PutEntry {
                            key: key.clone(),
                            value: StoredValue {
                                bytes: value.clone(),
                            },
                        }],
                    },
                )
                .await
                .expect("stage async-read seed value");
            write.commit().await.expect("commit async-read seed value");
        }

        let store = Arc::new(BlockingStore::new(inner));
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("reopen async-read storage");
        let read = storage
            .begin_read(ReadOptions::default())
            .await
            .expect("begin async object-store read");
        let blocked_read = store.block_sst_reads();

        let (release_tx, release_rx) = mpsc::channel();
        let releaser = std::thread::spawn(move || {
            blocked_read.wait_for_entries(1, "pending async SST read");
            let _ = release_rx.recv_timeout(Duration::from_secs(2));
            drop(blocked_read);
        });

        let (task_tx, task_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = task_tx.send(());
        });

        let keys = [key];
        let requests = [GetManyRequest {
            space,
            keys: &keys,
            opts: GetOptions::default(),
        }];
        let point_read = read.get_many(&requests);
        tokio::pin!(point_read);
        tokio::select! {
            biased;
            result = &mut point_read => {
                panic!("blocked object-store read completed before independent task: {result:?}");
            }
            result = task_rx => {
                result.expect("independent Tokio task should run while read is pending");
            }
        }

        release_tx.send(()).expect("release pending SST read");
        let result = point_read.await.expect("finish async object-store read");
        assert_eq!(result.values, vec![Some(ProjectedValue::FullValue(value))]);
        releaser.join().expect("join SST read releaser");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_pending_read_cancels_it_before_storage_close() {
        let storage = SlateDB::open_object_store_with_options(
            "test-cancel-pending-read",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open cancellable read storage");
        assert_dropping_pending_read_cancels_before_storage_close(storage).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_pending_local_filesystem_read_cancels_before_storage_close() {
        let directory = tempfile::tempdir().expect("create local cancellable read storage");
        let storage = SlateDB::open(directory.path()).expect("open local cancellable read storage");
        assert_dropping_pending_read_cancels_before_storage_close(storage).await;
    }

    async fn assert_dropping_pending_read_cancels_before_storage_close(storage: SlateDB) {
        let release = Arc::new(Notify::new());
        let release_for_read = Arc::clone(&release);
        let (started_tx, started_rx) = oneshot::channel();
        let worker = storage.worker.clone();
        let pending_read = tokio::spawn(async move {
            worker
                .call_read(move |_db| {
                    let release = Arc::clone(&release_for_read);
                    async move {
                        let _ = started_tx.send(());
                        release.notified().await;
                        Ok::<(), StorageError>(())
                    }
                })
                .await
        });

        started_rx
            .await
            .expect("pending read operation should start before cancellation");
        pending_read.abort();
        let error = pending_read
            .await
            .expect_err("dropping the caller should cancel its read future");
        assert!(
            error.is_cancelled(),
            "pending read task should be cancelled"
        );

        let (closed_tx, closed_rx) = mpsc::channel();
        let closer = std::thread::spawn(move || {
            drop(storage);
            let _ = closed_tx.send(());
        });
        if let Err(error) = closed_rx.recv_timeout(Duration::from_secs(2)) {
            // Keep the regression test self-cleaning if read cancellation ever
            // regresses: the old detached operation can finish before joining.
            release.notify_one();
            closed_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("close should finish after releasing pending read");
            closer.join().expect("join fallback closer");
            panic!("storage close should wait only for the cancelled read to drain: {error:?}");
        }
        closer.join().expect("join storage closer");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_operation_stays_on_the_callers_executor() {
        let storage = SlateDB::open_object_store_with_options(
            "test-caller-runtime-read",
            Arc::new(InMemory::new()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open caller-runtime read storage");

        CALLER_READ_MARKER
            .scope((), async {
                storage
                    .worker
                    .call_read(|_db| async move {
                        assert!(
                            CALLER_READ_MARKER.try_with(|()| ()).is_ok(),
                            "read work must retain the caller task context"
                        );
                        Ok::<(), StorageError>(())
                    })
                    .await
                    .expect("run read on caller executor");
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_filesystem_read_operation_stays_on_the_callers_executor() {
        let directory = tempfile::tempdir().expect("create local caller-runtime storage");
        let storage = SlateDB::open(directory.path()).expect("open local caller-runtime storage");

        CALLER_READ_MARKER
            .scope((), async {
                storage
                    .worker
                    .call_read(|db| async move {
                        assert!(
                            CALLER_READ_MARKER.try_with(|()| ()).is_ok(),
                            "local filesystem read work must retain the caller task context"
                        );
                        db.snapshot().await.map_err(slatedb_error)?;
                        Ok::<(), StorageError>(())
                    })
                    .await
                    .expect("run local read on caller executor");
            })
            .await;
    }

    #[test]
    fn timed_out_operation_block_releases_without_poisoning_cleanup() {
        let enabled = Arc::new(AtomicBool::new(false));
        let block = Arc::new(OperationBlock::default());
        let blocked = OperationBlockGuard::arm(Arc::clone(&enabled), Arc::clone(&block));

        assert_eq!(
            block.wait_for_entries_with_timeout(1, Duration::ZERO),
            Err(0)
        );
        drop(blocked);

        assert!(!enabled.load(Ordering::Acquire));
        assert!(block.released.load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn immutable_uploads_overlap_without_blocking_mutable_publication() {
        let store = BlockingStore::new(Arc::new(InMemory::new()));
        let storage = SlateDB::open_object_store_with_options(
            "test-overlapping-immutable-uploads",
            Arc::new(store.clone()),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open overlapping immutable upload storage");
        storage
            .begin_write(WriteOptions::default())
            .await
            .expect("complete startup immutable GC")
            .rollback()
            .await
            .expect("rollback startup GC write");
        let blocked = store.block_immutable_writes();

        let uploads = (0_u8..4)
            .map(|index| {
                let storage = storage.clone();
                tokio::spawn(async move {
                    let mut write = storage
                        .begin_write(WriteOptions::default())
                        .await
                        .expect("begin immutable upload");
                    write
                        .put_many(
                            TEST_IMMUTABLE_SPACE,
                            PutBatch {
                                entries: vec![PutEntry {
                                    key: Key(Bytes::from(vec![index; 32])),
                                    value: StoredValue {
                                        bytes: Bytes::from(vec![index; 1024]),
                                    },
                                }],
                            },
                        )
                        .await
                        .expect("upload immutable segment");
                    write.commit().await.expect("publish immutable locator");
                })
            })
            .collect::<Vec<_>>();
        blocked.wait_for_entries(4, "four concurrent immutable uploads");

        let mutable_space = StorageSpace::mutable(SpaceId(7), "test.mutable");
        let mutable_key = Key(Bytes::from_static(b"project-save"));
        let mut save = storage
            .begin_write(WriteOptions::default())
            .await
            .expect("begin project save while uploads are blocked");
        save.put_many(
            mutable_space,
            PutBatch {
                entries: vec![PutEntry {
                    key: mutable_key.clone(),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"saved"),
                    },
                }],
            },
        )
        .await
        .expect("stage project save while uploads are blocked");
        save.commit()
            .await
            .expect("publish project save while uploads are blocked");

        let read = storage
            .begin_read(ReadOptions::default())
            .await
            .expect("read project save");
        assert_eq!(
            read.get_many(&[GetManyRequest {
                space: mutable_space,
                keys: std::slice::from_ref(&mutable_key),
                opts: GetOptions::default(),
            }])
            .await
            .expect("load project save")
            .values,
            vec![Some(ProjectedValue::FullValue(Bytes::from_static(
                b"saved"
            )))]
        );

        drop(blocked);
        for upload in uploads {
            upload.await.expect("join immutable upload");
        }
    }

    fn block_on<T>(future: impl Future<Output = T>) -> T {
        Builder::new_current_thread()
            .build()
            .expect("build test runtime")
            .block_on(future)
    }

    #[derive(Clone, Debug)]
    struct BlockingStore {
        inner: Arc<InMemory>,
        next_write: Arc<AtomicBool>,
        immutable_writes: Arc<AtomicBool>,
        fail_writes: Arc<AtomicBool>,
        writes: Arc<OperationBlock>,
        block_reads: Arc<AtomicBool>,
        block_compacted_reads: Arc<AtomicBool>,
        reads: Arc<OperationBlock>,
        block_immutable_lists: Arc<AtomicBool>,
        fail_immutable_lists: Arc<AtomicBool>,
        lists: Arc<OperationBlock>,
    }

    impl BlockingStore {
        fn new(inner: Arc<InMemory>) -> Self {
            Self {
                inner,
                next_write: Arc::new(AtomicBool::new(false)),
                immutable_writes: Arc::new(AtomicBool::new(false)),
                fail_writes: Arc::new(AtomicBool::new(false)),
                writes: Arc::new(OperationBlock::default()),
                block_reads: Arc::new(AtomicBool::new(false)),
                block_compacted_reads: Arc::new(AtomicBool::new(false)),
                reads: Arc::new(OperationBlock::default()),
                block_immutable_lists: Arc::new(AtomicBool::new(false)),
                fail_immutable_lists: Arc::new(AtomicBool::new(false)),
                lists: Arc::new(OperationBlock::default()),
            }
        }

        fn block_next_write(&self) -> OperationBlockGuard {
            OperationBlockGuard::arm(Arc::clone(&self.next_write), Arc::clone(&self.writes))
        }

        fn block_immutable_writes(&self) -> OperationBlockGuard {
            OperationBlockGuard::arm(Arc::clone(&self.immutable_writes), Arc::clone(&self.writes))
        }

        fn fail_writes(&self) {
            self.fail_writes.store(true, Ordering::Release);
        }

        fn block_sst_reads(&self) -> OperationBlockGuard {
            OperationBlockGuard::arm(Arc::clone(&self.block_reads), Arc::clone(&self.reads))
        }

        fn block_compacted_reads(&self) -> OperationBlockGuard {
            OperationBlockGuard::arm(
                Arc::clone(&self.block_compacted_reads),
                Arc::clone(&self.reads),
            )
        }

        fn block_immutable_lists(&self) -> OperationBlockGuard {
            OperationBlockGuard::arm(
                Arc::clone(&self.block_immutable_lists),
                Arc::clone(&self.lists),
            )
        }

        fn fail_immutable_lists(&self) {
            self.fail_immutable_lists.store(true, Ordering::Release);
        }

        async fn maybe_block_write(&self, location: &ObjectPath) {
            let immutable = self.immutable_writes.load(Ordering::Acquire)
                && location.as_ref().contains(IMMUTABLE_VALUE_PATH);
            if self.next_write.swap(false, Ordering::AcqRel) || immutable {
                self.writes.enter().await;
            }
        }

        fn maybe_fail_write(&self) -> ObjectStoreResult<()> {
            if self.fail_writes.load(Ordering::Acquire) {
                Err(ObjectStoreError::NotSupported {
                    source: Box::new(std::io::Error::other("injected write failure")),
                })
            } else {
                Ok(())
            }
        }

        async fn maybe_block_read(&self, location: &ObjectPath) {
            let is_sst = location
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"));
            let block_all_ssts = self.block_reads.load(Ordering::Acquire);
            let block_compacted = self.block_compacted_reads.load(Ordering::Acquire)
                && location.as_ref().contains("/compacted/");
            if is_sst && (block_all_ssts || block_compacted) {
                self.reads.enter().await;
            }
        }
    }

    impl fmt::Display for BlockingStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("BlockingStore")
        }
    }

    #[derive(Debug, Default)]
    struct OperationBlock {
        entries: AtomicUsize,
        released: AtomicBool,
        entry_state: Mutex<()>,
        entered: Condvar,
        release: Notify,
    }

    impl OperationBlock {
        fn reset(&self) {
            let _state = self.entry_state.lock().expect("lock operation block");
            self.entries.store(0, Ordering::Release);
            self.released.store(false, Ordering::Release);
        }

        async fn enter(&self) {
            let state = self.entry_state.lock().expect("lock operation block");
            self.entries.fetch_add(1, Ordering::AcqRel);
            self.entered.notify_all();
            drop(state);
            loop {
                let released = self.release.notified();
                tokio::pin!(released);
                released.as_mut().enable();
                if self.released.load(Ordering::Acquire) {
                    return;
                }
                released.await;
            }
        }

        fn release(&self) {
            self.released.store(true, Ordering::Release);
            self.release.notify_waiters();
        }

        fn wait_for_entries_with_timeout(
            &self,
            expected: usize,
            timeout: Duration,
        ) -> Result<(), usize> {
            let deadline = Instant::now() + timeout;
            let mut state = self.entry_state.lock().expect("lock operation block");
            while self.entries.load(Ordering::Acquire) < expected {
                let now = Instant::now();
                if now >= deadline {
                    let observed = self.entries.load(Ordering::Acquire);
                    drop(state);
                    return Err(observed);
                }
                (state, _) = self
                    .entered
                    .wait_timeout(state, deadline - now)
                    .expect("wait for blocked operation");
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct OperationBlockGuard {
        enabled: Arc<AtomicBool>,
        block: Arc<OperationBlock>,
    }

    impl OperationBlockGuard {
        fn arm(enabled: Arc<AtomicBool>, block: Arc<OperationBlock>) -> Self {
            block.reset();
            enabled.store(true, Ordering::Release);
            Self { enabled, block }
        }

        fn wait_for_entries(&self, expected: usize, description: &str) {
            if let Err(observed) = self
                .block
                .wait_for_entries_with_timeout(expected, Duration::from_secs(10))
            {
                panic!("timed out waiting for {description}; observed {observed}");
            }
        }
    }

    impl Drop for OperationBlockGuard {
        fn drop(&mut self) {
            self.enabled.store(false, Ordering::Release);
            self.block.release();
        }
    }

    #[async_trait]
    impl ObjectStore for BlockingStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.maybe_block_write(location).await;
            self.maybe_fail_write()?;
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            options: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: ObjectStoreGetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.maybe_block_read(location).await;
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &ObjectPath,
            ranges: &[Range<u64>],
        ) -> ObjectStoreResult<Vec<Bytes>> {
            self.maybe_block_read(location).await;
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            let immutable =
                prefix.is_some_and(|prefix| prefix.as_ref().contains(IMMUTABLE_VALUE_PATH));
            if immutable && self.fail_immutable_lists.load(Ordering::Acquire) {
                return stream::once(async {
                    Err(ObjectStoreError::NotSupported {
                        source: Box::new(std::io::Error::other("injected immutable list failure")),
                    })
                })
                .boxed();
            }
            let inner = self.inner.list(prefix);
            if !immutable || !self.block_immutable_lists.load(Ordering::Acquire) {
                return inner;
            }
            let lists = Arc::clone(&self.lists);
            inner
                .then(move |result| {
                    let lists = Arc::clone(&lists);
                    async move {
                        lists.enter().await;
                        result
                    }
                })
                .boxed()
        }

        fn list_with_offset(
            &self,
            prefix: Option<&ObjectPath>,
            offset: &ObjectPath,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: RenameOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }
}
