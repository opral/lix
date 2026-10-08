use super::super::partial_state::stage_partial_replica_state;
use super::*;
use crate::binary_cas::{BinaryCasContext, BlobDataReader, BlobManifestsRequired, load_metadata_many};
use crate::storage::{StorageRead, StorageWrite};
use crate::{Memory, open_lix};
use base64::Engine as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
struct BlobProfileStorage {
    memory: Memory,
    begin_reads: Arc<AtomicUsize>,
    point_reads: Arc<AtomicUsize>,
    bounded_page_calls: Arc<AtomicUsize>,
    max_bounded_page_bytes: Arc<AtomicUsize>,
    durable_commits: Arc<AtomicUsize>,
    max_prefix_slots: usize,
    delay: std::time::Duration,
}

struct BlobProfileWrite {
    inner: crate::storage_adapter::MemoryWrite,
    durable: bool,
    durable_commits: Arc<AtomicUsize>,
    delay: std::time::Duration,
}

struct BlobProfileRead<R> {
    inner: R,
    point_reads: Arc<AtomicUsize>,
    bounded_page_calls: Arc<AtomicUsize>,
    max_bounded_page_bytes: Arc<AtomicUsize>,
    max_prefix_slots: usize,
}

impl<R> BlobProfileRead<R> {
    fn record_bounded_page(
        &self,
        values: &[Option<StorageProjectedValue>],
    ) {
        let bytes = values
            .iter()
            .flatten()
            .map(|value| match value {
                StorageProjectedValue::FullValue(bytes) => bytes.len(),
                StorageProjectedValue::KeyOnly => 0,
            })
            .sum::<usize>();
        self.bounded_page_calls.fetch_add(1, Ordering::SeqCst);
        self.max_bounded_page_bytes
            .fetch_max(bytes, Ordering::SeqCst);
    }
}

impl<R: StorageRead> StorageRead for BlobProfileRead<R> {
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }

    async fn get_many(
        &self,
        requests: &[StorageGetManyRequest<'_>],
    ) -> Result<crate::storage_adapter::StorageGetManyResult, crate::storage_adapter::StorageError>
    {
        self.point_reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get_many(requests).await
    }

    async fn get_many_bounded(
        &self,
        requests: &[StorageGetManyRequest<'_>],
        budget: crate::storage_adapter::ReadBudget,
    ) -> Result<crate::storage_adapter::StorageGetManyResult, crate::storage_adapter::StorageError>
    {
        self.point_reads.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.get_many_bounded(requests, budget).await?;
        self.record_bounded_page(&result.values);
        Ok(result)
    }

    async fn get_many_bounded_prefix(
        &self,
        requests: &[StorageGetManyRequest<'_>],
        offset: usize,
        max_slots: usize,
        budget: crate::storage_adapter::ReadBudget,
    ) -> Result<
        crate::storage_adapter::GetManyPrefixResult,
        crate::storage_adapter::StorageError,
    > {
        self.point_reads.fetch_add(1, Ordering::SeqCst);
        let result = self
            .inner
            .get_many_bounded_prefix(
                requests,
                offset,
                max_slots.min(self.max_prefix_slots),
                budget,
            )
            .await?;
        self.record_bounded_page(&result.values);
        Ok(result)
    }

    async fn begin_scan(
        &self,
        space: crate::storage_adapter::StorageSpace,
        range: crate::storage_adapter::StorageKeyRange,
        opts: crate::storage_adapter::StorageBeginScanOptions,
    ) -> Result<crate::storage_adapter::StorageScanCursor<'_>, crate::storage_adapter::StorageError>
    {
        self.inner.begin_scan(space, range, opts).await
    }
}

impl Storage for BlobProfileStorage {
    type Read<'a>
        = BlobProfileRead<crate::storage_adapter::MemoryRead>
    where
        Self: 'a;
    type Write<'a>
        = BlobProfileWrite
    where
        Self: 'a;

    async fn acquire_session(
        &self,
    ) -> Result<crate::storage_adapter::StorageSessionToken, crate::storage_adapter::StorageError>
    {
        self.memory.acquire_session().await
    }

    async fn begin_read(
        &self,
        opts: crate::storage_adapter::StorageReadOptions,
    ) -> Result<Self::Read<'_>, crate::storage_adapter::StorageError> {
        self.begin_reads.fetch_add(1, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        Ok(BlobProfileRead {
            inner: self.memory.begin_read(opts).await?,
            point_reads: Arc::clone(&self.point_reads),
            bounded_page_calls: Arc::clone(&self.bounded_page_calls),
            max_bounded_page_bytes: Arc::clone(&self.max_bounded_page_bytes),
            max_prefix_slots: self.max_prefix_slots,
        })
    }

    async fn begin_write(
        &self,
        opts: StorageWriteOptions,
    ) -> Result<Self::Write<'_>, crate::storage_adapter::StorageError> {
        let durable = opts.await_durable;
        Ok(BlobProfileWrite {
            inner: self.memory.begin_write(opts).await?,
            durable,
            durable_commits: Arc::clone(&self.durable_commits),
            delay: self.delay,
        })
    }
}

impl StorageWrite for BlobProfileWrite {
    async fn put_many(
        &mut self,
        space: crate::storage_adapter::StorageSpace,
        entries: crate::storage_adapter::PutBatch,
    ) -> Result<(), crate::storage_adapter::StorageError> {
        self.inner.put_many(space, entries).await
    }

    async fn replace_many(
        &mut self,
        space: crate::storage_adapter::StorageSpace,
        entries: crate::storage_adapter::PutBatch,
    ) -> Result<(), crate::storage_adapter::StorageError> {
        self.inner.replace_many(space, entries).await
    }

    async fn delete_many(
        &mut self,
        space: crate::storage_adapter::StorageSpace,
        keys: &[StorageKey],
    ) -> Result<(), crate::storage_adapter::StorageError> {
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: crate::storage_adapter::StorageSpace,
        range: crate::storage_adapter::StorageKeyRange,
    ) -> Result<(), crate::storage_adapter::StorageError> {
        self.inner.delete_range(space, range).await
    }

    async fn commit(
        self,
    ) -> Result<crate::storage_adapter::StorageCommitResult, crate::storage_adapter::StorageError>
    {
        let Self {
            inner,
            durable,
            durable_commits,
            delay,
        } = self;
        if durable && !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let result = inner.commit().await;
        if durable && result.is_ok() {
            durable_commits.fetch_add(1, Ordering::SeqCst);
        }
        result
    }

    async fn rollback(self) -> Result<(), crate::storage_adapter::StorageError> {
        self.inner.rollback().await
    }
}

async fn profiled_fixture() -> (
    StorageAdapter<BlobProfileStorage>,
    PartialReplicaState,
    BlobProfileStorage,
) {
    profiled_fixture_with_prefix_slots(usize::MAX).await
}

async fn profiled_fixture_with_prefix_slots(
    max_prefix_slots: usize,
) -> (
    StorageAdapter<BlobProfileStorage>,
    PartialReplicaState,
    BlobProfileStorage,
) {
    let authority = open_lix().await.unwrap();
    let state = PartialReplicaState::new(
        format!("https://example.test/lix/{}", authority.lix_id()),
        authority.active_account_id().into(),
        "00000000-0000-7000-8000-000000000399".into(),
        authority.partial_replica_descriptor(None).await.unwrap(),
    )
    .unwrap();
    let profiled = BlobProfileStorage {
        memory: Memory::new(),
        begin_reads: Arc::new(AtomicUsize::new(0)),
        point_reads: Arc::new(AtomicUsize::new(0)),
        bounded_page_calls: Arc::new(AtomicUsize::new(0)),
        max_bounded_page_bytes: Arc::new(AtomicUsize::new(0)),
        durable_commits: Arc::new(AtomicUsize::new(0)),
        max_prefix_slots: max_prefix_slots.max(1),
        delay: std::time::Duration::from_millis(5),
    };
    let storage = StorageAdapter::new(profiled.clone());
    let mut writes = storage.new_write_set();
    let condition = stage_partial_replica_state(&mut writes, &state, None).unwrap();
    let mut raw = storage
        .begin_migration_write(StorageWriteOptions {
            preconditions: vec![condition],
            ..Default::default()
        })
        .await
        .unwrap();
    writes.lower_into(&mut raw).await.unwrap();
    raw.commit().await.unwrap();
    authority.close().await.unwrap();
    (storage, state, profiled)
}

const PROFILED_CHUNKS: usize = 16;
const PROFILED_CHUNK_BYTES: usize = 128 * 1024;
const PROFILED_NETWORK_DELAY: std::time::Duration = std::time::Duration::from_millis(5);

fn profiled_chunks() -> Vec<(ChunkHash, Vec<u8>)> {
    (0..PROFILED_CHUNKS)
        .map(|index| {
            let bytes = vec![index as u8 + 1; PROFILED_CHUNK_BYTES];
            (ChunkHash::from_content(&bytes), bytes)
        })
        .collect()
}

async fn seed_profiled_demands(
    storage: &StorageAdapter<BlobProfileStorage>,
    chunks: &[(ChunkHash, Vec<u8>)],
) {
    let mut demand_markers = storage.new_write_set();
    for (hash, _) in chunks {
        demand_markers.put(
            BINARY_CAS_CHUNK_DEMAND_SPACE,
            StorageKey(Bytes::copy_from_slice(hash.as_bytes())),
            Vec::<u8>::new(),
        );
    }
    storage
        .commit_partial_replica_write_set(
            super::super::partial_replica_write_capability(),
            demand_markers,
            Default::default(),
        )
        .await
        .unwrap();
}

#[derive(Debug)]
struct BlobProfileResult {
    begin_reads: usize,
    point_reads: usize,
    durable_commits: usize,
    elapsed: std::time::Duration,
}

async fn run_scalar_blob_profile() -> BlobProfileResult {
    let (storage, state, profile) = profiled_fixture().await;
    let chunks = profiled_chunks();
    seed_profiled_demands(&storage, &chunks).await;
    profile.begin_reads.store(0, Ordering::SeqCst);
    profile.point_reads.store(0, Ordering::SeqCst);
    profile.durable_commits.store(0, Ordering::SeqCst);
    let started = std::time::Instant::now();
    for page in chunks.chunks(super::super::transfer::CHUNK_CONCURRENCY) {
        for (hash, _) in page {
            assert!(!scalar_chunk_is_resident(&storage, &state, *hash).await.unwrap());
        }
        tokio::time::sleep(PROFILED_NETWORK_DELAY).await;
        for (hash, bytes) in page {
            scalar_install_chunk(&storage, &state, *hash, bytes).await.unwrap();
        }
    }
    BlobProfileResult {
        begin_reads: profile.begin_reads.load(Ordering::SeqCst),
        point_reads: profile.point_reads.load(Ordering::SeqCst),
        durable_commits: profile.durable_commits.load(Ordering::SeqCst),
        elapsed: started.elapsed(),
    }
}

async fn run_paged_blob_profile() -> BlobProfileResult {
    let (storage, state, profile) = profiled_fixture().await;
    let all_chunks = profiled_chunks();
    seed_profiled_demands(&storage, &all_chunks).await;
    let mut chunks = all_chunks.into_iter();
    profile.begin_reads.store(0, Ordering::SeqCst);
    profile.point_reads.store(0, Ordering::SeqCst);
    profile.durable_commits.store(0, Ordering::SeqCst);
    let started = std::time::Instant::now();
    loop {
        let page = chunks
            .by_ref()
            .take(super::super::transfer::CHUNK_CONCURRENCY)
            .collect::<Vec<_>>();
        if page.is_empty() {
            break;
        }
        let hashes = page.iter().map(|(hash, _)| *hash).collect::<Vec<_>>();
        assert_eq!(
            chunks_are_resident(&storage, &state, &hashes)
                .await
                .unwrap(),
            vec![false; page.len()]
        );
        tokio::time::sleep(PROFILED_NETWORK_DELAY).await;
        install_chunk_page(&storage, &state, page).await.unwrap();
    }
    BlobProfileResult {
        begin_reads: profile.begin_reads.load(Ordering::SeqCst),
        point_reads: profile.point_reads.load(Ordering::SeqCst),
        durable_commits: profile.durable_commits.load(Ordering::SeqCst),
        elapsed: started.elapsed(),
    }
}

async fn fixture() -> (StorageAdapter<Memory>, PartialReplicaState) {
    let authority = open_lix().await.unwrap();
    let state = PartialReplicaState::new(
        format!("https://example.test/lix/{}", authority.lix_id()),
        authority.active_account_id().into(),
        "00000000-0000-7000-8000-000000000399".into(),
        authority.partial_replica_descriptor(None).await.unwrap(),
    )
    .unwrap();
    let storage = StorageAdapter::new(Memory::new());
    let mut writes = storage.new_write_set();
    let condition = stage_partial_replica_state(&mut writes, &state, None).unwrap();
    let mut raw = storage
        .begin_migration_write(StorageWriteOptions {
            preconditions: vec![condition],
            ..Default::default()
        })
        .await
        .unwrap();
    writes.lower_into(&mut raw).await.unwrap();
    raw.commit().await.unwrap();
    (storage, state)
}

/// Compares the scalar and page publication paths across the preflight
/// residency lookup, the network boundary, and strict durable publication.
/// Five milliseconds on read/commit boundaries and per network page approximate
/// a modest remote-backed storage RTT; assertions use operation counts, not time.
#[tokio::test]
async fn scalar_and_paged_sixteen_chunk_install_profile() {
    let scalar = run_scalar_blob_profile().await;
    let paged = run_paged_blob_profile().await;
    assert_eq!(scalar.begin_reads, PROFILED_CHUNKS * 2);
    assert_eq!(scalar.durable_commits, PROFILED_CHUNKS);
    let pages = PROFILED_CHUNKS.div_ceil(super::super::transfer::CHUNK_CONCURRENCY);
    assert_eq!(paged.begin_reads, pages * 2);
    assert_eq!(paged.durable_commits, pages);
    assert!(paged.point_reads < scalar.point_reads);
    eprintln!(
        "partial blob profile: chunks={PROFILED_CHUNKS}, bytes={}, simulated_adapter_delay_ms=5, simulated_fetch_page_delay_ms=5, scalar_ms={}, paged_ms={}, scalar={scalar:?}, paged={paged:?}",
        PROFILED_CHUNKS * PROFILED_CHUNK_BYTES,
        scalar.elapsed.as_secs_f64() * 1000.0,
        paged.elapsed.as_secs_f64() * 1000.0,
    );
}

fn wire(bytes: &[u8]) -> SyncBlobManifest {
    SyncBlobManifest {
        blob_id: BlobId::from_content(bytes).to_hex(),
        size_bytes: bytes.len() as u64,
        chunks: vec![super::super::SyncBlobChunk {
            chunk_id: ChunkHash::from_content(bytes).to_hex(),
            size_bytes: bytes.len() as u64,
        }],
        inline_bytes_base64: None,
    }
}

fn inline_wire(bytes: &[u8]) -> SyncBlobManifest {
    let canonical = CanonicalBlobManifest::from_bytes(bytes);
    SyncBlobManifest {
        blob_id: canonical.blob_id.to_hex(),
        size_bytes: canonical.size_bytes,
        chunks: canonical
            .chunks
            .iter()
            .map(|chunk| super::super::SyncBlobChunk {
                chunk_id: chunk.hash.to_hex(),
                size_bytes: chunk.size_bytes,
            })
            .collect(),
        inline_bytes_base64: Some(base64::engine::general_purpose::STANDARD.encode(bytes)),
    }
}

fn maximum_receipt_manifest() -> (SyncBlobManifest, ChunkHash) {
    maximum_receipt_manifest_from(b"receipt inventory placeholder")
}

fn maximum_receipt_manifest_from(seed: &[u8]) -> (SyncBlobManifest, ChunkHash) {
    let hash = ChunkHash::from_content(seed);
    let count = 16_384usize;
    let chunk_size = crate::binary_cas::raw_chunk_transfer_bounds().max_payload_bytes as u64;
    let size_bytes = (count as u64) * chunk_size;
    let blob_id = BlobId::from_chunks(
        size_bytes,
        std::iter::repeat((hash, chunk_size)).take(count),
    );
    (
        SyncBlobManifest {
            blob_id: blob_id.to_hex(),
            size_bytes,
            chunks: vec![
                super::super::SyncBlobChunk {
                    chunk_id: hash.to_hex(),
                    size_bytes: chunk_size,
                };
                count
            ],
            inline_bytes_base64: None,
        },
        hash,
    )
}

fn wire_chunks(chunks: &[&[u8]]) -> SyncBlobManifest {
    let receipts = chunks
        .iter()
        .map(|bytes| (ChunkHash::from_content(bytes), bytes.len() as u64))
        .collect::<Vec<_>>();
    let size_bytes = receipts.iter().map(|(_, size)| *size).sum::<u64>();
    SyncBlobManifest {
        blob_id: BlobId::from_chunks(
            size_bytes,
            receipts.iter().map(|(hash, size)| (*hash, *size)),
        )
        .to_hex(),
        size_bytes,
        chunks: receipts
            .into_iter()
            .map(|(hash, size_bytes)| super::super::SyncBlobChunk {
                chunk_id: hash.to_hex(),
                size_bytes,
            })
            .collect(),
        inline_bytes_base64: None,
    }
}

#[tokio::test]
async fn partial_blob_manifest_and_chunk_pages_preserve_slots_and_verified_bytes() {
    let (storage, state) = fixture().await;
    let shared = b"shared page chunk";
    let left = b"left page chunk";
    let right = b"right page chunk";
    let manifests = [wire_chunks(&[shared, left]), wire_chunks(&[shared, right])];
    let requested = manifests
        .iter()
        .map(|manifest| BlobId::from_hex(&manifest.blob_id).unwrap())
        .collect::<Vec<_>>();
    let registrations = install_manifest_page(&storage, &state, &requested, &manifests)
        .await
        .unwrap();
    assert_eq!(registrations.len(), 2);
    assert_eq!(
        registrations[0].missing_chunk_ids,
        vec![
            ChunkHash::from_content(left).to_hex(),
            ChunkHash::from_content(shared).to_hex(),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
    );
    assert_eq!(
        registrations[1].missing_chunk_ids,
        vec![
            ChunkHash::from_content(right).to_hex(),
            ChunkHash::from_content(shared).to_hex(),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
    );
    assert_eq!(
        manifests_are_resident(
            &storage,
            &state,
            &[requested[0], requested[1], requested[0]]
        )
        .await
        .unwrap(),
        vec![true, true, true]
    );

    let hashes = [
        ChunkHash::from_content(shared),
        ChunkHash::from_content(left),
        ChunkHash::from_content(right),
    ];
    assert_eq!(
        chunks_are_resident(&storage, &state, &hashes)
            .await
            .unwrap(),
        vec![false, false, false]
    );
    install_chunk_page(
        &storage,
        &state,
        vec![
            (hashes[0], shared.to_vec()),
            (hashes[1], left.to_vec()),
            (hashes[2], right.to_vec()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        chunks_are_resident(&storage, &state, &hashes)
            .await
            .unwrap(),
        vec![true, true, true]
    );
    for (hash, expected_bytes) in hashes.into_iter().zip([shared.as_slice(), left.as_slice(), right.as_slice()]) {
        let read = storage.begin_read(Default::default()).await.unwrap();
        assert_eq!(
            crate::binary_cas::load_verified_chunk(&read, hash).await.unwrap().as_deref(),
            Some(expected_bytes)
        );
    }
}

#[tokio::test]
async fn manifest_page_keeps_inline_publication_self_contained() {
    let (storage, state) = fixture().await;
    let bytes = b"inline page content";
    let mut manifest = wire(bytes);
    manifest.inline_bytes_base64 =
        Some(base64::engine::general_purpose::STANDARD.encode(bytes));
    let requested = BlobId::from_content(bytes);
    let registration = install_manifest_page(&storage, &state, &[requested], &[manifest])
        .await
        .unwrap();
    assert_eq!(registration.len(), 1);
    assert!(registration[0].missing_chunk_ids.is_empty());
    assert_eq!(
        manifests_are_resident(&storage, &state, &[requested])
            .await
            .unwrap(),
        vec![true]
    );
    assert_eq!(
        chunks_are_resident(&storage, &state, &[ChunkHash::from_content(bytes)])
            .await
            .unwrap(),
        vec![true]
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        crate::binary_cas::load_bytes_many(&read, &[requested])
            .await
            .unwrap()
            .into_vec(),
        vec![Some(bytes.to_vec())]
    );
}

#[tokio::test]
async fn manifest_pages_partition_sixteen_max_inline_bodies() {
    let (storage, state) = fixture().await;
    let payloads = (0..16)
        .map(|index| vec![index as u8 + 1; super::super::blob::MAX_INLINE_SYNC_BLOB_BYTES])
        .collect::<Vec<_>>();
    let manifests = payloads
        .iter()
        .map(|payload| inline_wire(payload))
        .collect::<Vec<_>>();
    let requested = manifests
        .iter()
        .map(|manifest| BlobId::from_hex(&manifest.blob_id).unwrap())
        .collect::<Vec<_>>();

    let pages = super::super::transfer::partition_manifest_pages(&manifests).unwrap();
    assert!(pages.len() > 1);
    assert_eq!(
        pages.iter().map(|page| page.len()).sum::<usize>(),
        manifests.len()
    );
    for page in &pages {
        super::super::transfer::validate_manifest_page(&manifests[page.clone()]).unwrap();
    }

    let registrations = install_manifest_pages(&storage, &state, &requested, &manifests)
        .await
        .unwrap();
    assert_eq!(registrations.len(), manifests.len());
    assert!(
        registrations
            .iter()
            .all(|registration| registration.missing_chunk_ids.is_empty())
    );
    assert_eq!(
        manifests_are_resident(&storage, &state, &requested)
            .await
            .unwrap(),
        vec![true; manifests.len()]
    );
}

#[tokio::test]
async fn maximum_manifest_receipt_singleton_fits_but_oversized_page_is_rejected() {
    let (storage, state, profile) = profiled_fixture().await;
    let (manifest, hash) = maximum_receipt_manifest();
    let requested = BlobId::from_hex(&manifest.blob_id).unwrap();
    super::super::transfer::validate_manifest_page(std::slice::from_ref(&manifest)).unwrap();

    let registrations = install_manifest_pages(
        &storage,
        &state,
        &[requested, requested],
        &[manifest.clone(), manifest.clone()],
    )
    .await
    .unwrap();
    assert_eq!(registrations.len(), 2);
    assert_eq!(registrations[0].missing_chunk_ids, vec![hash.to_hex()]);
    assert_eq!(registrations[1], registrations[0]);
    assert!(
        manifest_is_resident(&storage, &state, requested)
            .await
            .unwrap()
    );

    profile.begin_reads.store(0, Ordering::SeqCst);
    assert!(
        install_manifest_page(
            &storage,
            &state,
            &[requested, requested],
            &[manifest.clone(), manifest],
        )
        .await
        .is_err()
    );
    assert_eq!(profile.begin_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn resident_maximum_manifest_inventories_use_bounded_prefix_pages() {
    let (storage, state, profile) = profiled_fixture_with_prefix_slots(1).await;
    // Chunked CAS metadata stores a receipt count rather than the full wire
    // inventory, so force one provider slot per prefix to exercise paging.
    let (first, _) = maximum_receipt_manifest_from(b"first maximum inventory");
    let (second, _) = maximum_receipt_manifest_from(b"second maximum inventory");
    let first_id = BlobId::from_hex(&first.blob_id).unwrap();
    let second_id = BlobId::from_hex(&second.blob_id).unwrap();
    let absent_id = BlobId::from_content(b"absent maximum inventory");
    install_manifest_pages(&storage, &state, &[first_id, second_id], &[first, second])
        .await
        .unwrap();

    profile.bounded_page_calls.store(0, Ordering::SeqCst);
    profile.max_bounded_page_bytes.store(0, Ordering::SeqCst);
    assert_eq!(
        manifests_are_resident(
            &storage,
            &state,
            &[second_id, absent_id, first_id, second_id]
        )
        .await
        .unwrap(),
        vec![true, false, true, true]
    );
    assert_eq!(profile.bounded_page_calls.load(Ordering::SeqCst), 3);
    assert!(profile.max_bounded_page_bytes.load(Ordering::SeqCst) <= 2 * 1024 * 1024);
}

#[tokio::test]
async fn duplicate_chunk_page_publication_is_single_mutation_and_race_safe() {
    let (storage, state) = fixture().await;
    let bytes = b"racing page content";
    let manifest = wire(bytes);
    let hash = ChunkHash::from_content(bytes);
    install_manifest_page(
        &storage,
        &state,
        &[BlobId::from_content(bytes), BlobId::from_content(bytes)],
        &[manifest.clone(), manifest],
    )
    .await
    .unwrap();
    assert_eq!(
        chunks_are_resident(&storage, &state, &[hash, hash])
            .await
            .unwrap(),
        vec![false, false]
    );

    let payloads = || vec![(hash, bytes.to_vec()), (hash, bytes.to_vec())];
    let (first, second) = futures_util::join!(
        install_chunk_page(&storage, &state, payloads()),
        install_chunk_page(&storage, &state, payloads()),
    );
    first.unwrap();
    second.unwrap();
    assert_eq!(
        chunks_are_resident(&storage, &state, &[hash, hash])
            .await
            .unwrap(),
        vec![true, true]
    );
}

#[tokio::test]
async fn chunk_page_rejects_corrupt_rows_and_oversized_admission_before_read() {
    let (storage, state, profile) = profiled_fixture().await;
    let bytes = b"corrupt row target";
    let hash = ChunkHash::from_content(bytes);
    let mut writes = storage.new_write_set();
    writes.put(
        BINARY_CAS_CHUNK_SPACE,
        StorageKey(Bytes::copy_from_slice(hash.as_bytes())),
        b"not an encoded CAS chunk".as_slice(),
    );
    storage
        .commit_partial_replica_write_set(
            super::super::partial_replica_write_capability(),
            writes,
            Default::default(),
        )
        .await
        .unwrap();
    assert!(
        chunks_are_resident(&storage, &state, &[hash])
            .await
            .is_err()
    );

    profile.begin_reads.store(0, Ordering::SeqCst);
    let too_many = vec![hash; super::super::transfer::CHUNK_CONCURRENCY + 1];
    assert!(
        chunks_are_resident(&storage, &state, &too_many)
            .await
            .is_err()
    );
    assert_eq!(profile.begin_reads.load(Ordering::SeqCst), 0);

    let authentic = b"requested hash content";
    let requested = ChunkHash::from_content(authentic);
    let mut demand = storage.new_write_set();
    demand.put(
        BINARY_CAS_CHUNK_DEMAND_SPACE,
        StorageKey(Bytes::copy_from_slice(requested.as_bytes())),
        Vec::<u8>::new(),
    );
    storage
        .commit_partial_replica_write_set(
            super::super::partial_replica_write_capability(),
            demand,
            Default::default(),
        )
        .await
        .unwrap();
    assert!(
        install_chunk_page(
            &storage,
            &state,
            vec![(requested, b"wrong content bytes".to_vec())],
        )
        .await
        .is_err()
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(
        crate::binary_cas::load_verified_chunk(&read, requested)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn referenced_partial_read_demands_manifest_then_chunk_and_stays_warm() {
    let (storage, state) = fixture().await;
    let bytes = b"partial file content";
    let hash = BlobId::from_content(bytes);
    let context = BinaryCasContext::new();
    let read = storage.begin_read(Default::default()).await.unwrap();
    // Generic existence probes and ordinary readers keep absent semantics.
    context
        .reader(&read)
        .require_referenced_manifests(&[hash])
        .await
        .unwrap();
    assert!(
        crate::binary_cas::load_bytes_many(&read, &[hash])
            .await
            .unwrap()
            .into_vec()[0]
            .is_none()
    );
    context.enable_referenced_manifest_demands();
    let error = context
        .reader(&read)
        .require_referenced_manifests(&[hash])
        .await
        .unwrap_err();
    assert_eq!(
        BlobManifestsRequired::from_error(&error).unwrap(),
        Some(BlobManifestsRequired(vec![hash]))
    );
    drop(read);
    let result = install_manifest(&storage, &state, hash, &wire(bytes))
        .await
        .unwrap();
    assert_eq!(
        result.missing_chunk_ids,
        vec![ChunkHash::from_content(bytes).to_hex()]
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        crate::binary_cas::load_bytes_many(&read, &[hash])
            .await
            .unwrap_err()
            .code,
        "LIX_SYNC_CHUNKS_REQUIRED"
    );
    drop(read);
    install_chunk(&storage, &state, ChunkHash::from_content(bytes), bytes)
        .await
        .unwrap();
    for _ in 0..3 {
        let read = storage.begin_read(Default::default()).await.unwrap();
        context
            .reader(&read)
            .require_referenced_manifests(&[hash])
            .await
            .unwrap();
        assert_eq!(
            crate::binary_cas::load_bytes_many(&read, &[hash])
                .await
                .unwrap()
                .into_vec(),
            vec![Some(bytes.to_vec())]
        );
    }
}

#[tokio::test]
async fn referenced_content_batches_missing_manifests_and_chunk_closure() {
    let (storage, state) = fixture().await;
    let first_bytes = b"first independent executable";
    let second_bytes = b"second independent executable";
    let first = BlobId::from_content(first_bytes);
    let second = BlobId::from_content(second_bytes);
    let context = BinaryCasContext::new();
    context.enable_referenced_manifest_demands();

    let read = storage.begin_read(Default::default()).await.unwrap();
    let error = context
        .reader(&read)
        .require_referenced_content(&[second, first, first])
        .await
        .unwrap_err();
    assert_eq!(
        BlobManifestsRequired::from_error(&error).unwrap(),
        Some(BlobManifestsRequired(vec![first.min(second), first.max(second)]))
    );
    drop(read);

    install_manifest(&storage, &state, first, &wire(first_bytes))
        .await
        .unwrap();
    install_manifest(&storage, &state, second, &wire(second_bytes))
        .await
        .unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let error = context
        .reader(&read)
        .require_referenced_content(&[first, second])
        .await
        .unwrap_err();
    let chunk_ids = error
        .details
        .as_ref()
        .and_then(|details| details.get("chunkIds"))
        .and_then(serde_json::Value::as_array)
        .unwrap();
    let mut expected_chunk_ids = vec![
        ChunkHash::from_content(first_bytes).to_hex(),
        ChunkHash::from_content(second_bytes).to_hex(),
    ];
    expected_chunk_ids.sort();
    let actual_chunk_ids = chunk_ids
        .iter()
        .map(|id| id.as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(actual_chunk_ids, expected_chunk_ids);
    drop(read);

    install_chunk(
        &storage,
        &state,
        ChunkHash::from_content(first_bytes),
        first_bytes,
    )
    .await
    .unwrap();
    install_chunk(
        &storage,
        &state,
        ChunkHash::from_content(second_bytes),
        second_bytes,
    )
    .await
    .unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    context
        .reader(&read)
        .require_referenced_content(&[first, second])
        .await
        .unwrap();
}

#[tokio::test]
async fn bad_hash_and_old_epoch_never_publish_blob_bytes() {
    let (storage, state) = fixture().await;
    let bytes = b"validated content";
    let hash = BlobId::from_content(bytes);
    let mut wrong = wire(bytes);
    wrong.blob_id = BlobId::from_content(b"other").to_hex();
    assert!(
        install_manifest(&storage, &state, hash, &wrong)
            .await
            .is_err()
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(load_metadata_many(&read, &[hash]).await.unwrap().into_vec()[0].is_none());
    drop(read);
    install_manifest(&storage, &state, hash, &wire(bytes))
        .await
        .unwrap();
    assert!(
        install_chunk(&storage, &state, ChunkHash::from_content(bytes), b"corrupt")
            .await
            .is_err()
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (_, raw) = load_partial_replica_state(&read).await.unwrap().unwrap();
    assert!(
        crate::binary_cas::load_verified_chunk(&read, ChunkHash::from_content(bytes))
            .await
            .unwrap()
            .is_none()
    );
    drop(read);
    let replacement = PartialReplicaState::new(
        state.remote_id().into(),
        state.active_account_id().into(),
        "00000000-0000-7000-8000-000000000499".into(),
        state.descriptor().clone(),
    )
    .unwrap();
    let mut writes = storage.new_write_set();
    let condition = stage_partial_replica_state(&mut writes, &replacement, Some(raw)).unwrap();
    let mut raw = storage
        .begin_migration_write(StorageWriteOptions {
            preconditions: vec![condition],
            ..Default::default()
        })
        .await
        .unwrap();
    writes.lower_into(&mut raw).await.unwrap();
    raw.commit().await.unwrap();
    assert_eq!(
        install_chunk(&storage, &state, ChunkHash::from_content(bytes), bytes)
            .await
            .unwrap_err()
            .code,
        "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH"
    );
}

#[tokio::test]
async fn duplicate_manifest_and_chunk_installs_are_idempotent_without_overwrite() {
    let (storage, state) = fixture().await;
    let bytes = b"same concurrent content";
    let hash = BlobId::from_content(bytes);
    let manifest = wire(bytes);
    let (first, second) = futures_util::join!(
        install_manifest(&storage, &state, hash, &manifest),
        install_manifest(&storage, &state, hash, &manifest),
    );
    first.unwrap();
    second.unwrap();
    let chunk = ChunkHash::from_content(bytes);
    let (first, second) = futures_util::join!(
        install_chunk(&storage, &state, chunk, bytes),
        install_chunk(&storage, &state, chunk, bytes),
    );
    first.unwrap();
    second.unwrap();
    assert!(manifest_is_resident(&storage, &state, hash).await.unwrap());
    assert!(chunk_is_resident(&storage, &state, chunk).await.unwrap());
}

#[tokio::test]
async fn deferred_manifest_replans_when_chunk_arrives_after_missing_snapshot() {
    let (storage, state) = fixture().await;
    let bytes = b"shared content that arrived during manifest admission";
    let manifest = super::super::blob::decode_manifest(&wire(bytes)).unwrap();
    let requested = manifest.blob_id;
    let chunk = ChunkHash::from_content(bytes);

    // An existing deferred reference supplied the demand that authorizes the
    // concurrent chunk installer. A second manifest is planned while absent.
    let mut demand = storage.new_write_set();
    demand.put(
        BINARY_CAS_CHUNK_DEMAND_SPACE,
        StorageKey(Bytes::copy_from_slice(chunk.as_bytes())),
        Vec::<u8>::new(),
    );
    storage
        .commit_partial_replica_write_set(
            super::super::partial_replica_write_capability(),
            demand,
            Default::default(),
        )
        .await
        .unwrap();

    let read = storage.begin_read(Default::default()).await.unwrap();
    let (_, receipt) = load_partial_replica_state(&read).await.unwrap().unwrap();
    let mut stale_writes = storage.new_write_set();
    let (preconditions, missing) = prepare_manifest_install(
        &read,
        &mut stale_writes,
        requested,
        receipt,
        &manifest,
        None,
    )
    .await
    .unwrap();
    assert_eq!(missing, vec![chunk]);
    drop(read);

    install_chunk(&storage, &state, chunk, bytes).await.unwrap();
    let stale_commit = storage
        .commit_partial_replica_write_set(
            super::super::partial_replica_write_capability(),
            stale_writes,
            StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(
        stale_commit,
        Err(crate::storage_adapter::StorageWriteSetError::Storage(
            crate::storage_adapter::StorageError::PreconditionFailed(_)
        ))
    ));

    let registration = install_manifest(&storage, &state, requested, &wire(bytes))
        .await
        .unwrap();
    assert!(registration.missing_chunk_ids.is_empty());
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(
        load_metadata_many(&read, &[requested])
            .await
            .unwrap()
            .into_vec()[0]
            .is_some()
    );
    assert!(
        PointReadPlan::new(
            BINARY_CAS_CHUNK_DEMAND_SPACE,
            &[StorageKey(Bytes::copy_from_slice(chunk.as_bytes()))],
        )
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value[0]
            .is_none()
    );
}

#[tokio::test]
async fn corrupt_resident_manifest_is_not_a_demand_or_a_repair() {
    let (storage, state) = fixture().await;
    let hash = BlobId::from_content(b"corrupted manifest");
    let mut writes = storage.new_write_set();
    writes.put(
        BINARY_CAS_MANIFEST_SPACE,
        StorageKey(Bytes::copy_from_slice(hash.as_bytes())),
        b"invalid manifest".as_slice(),
    );
    storage
        .commit_partial_replica_write_set(
            super::super::partial_replica_write_capability(),
            writes,
            Default::default(),
        )
        .await
        .unwrap();
    let context = BinaryCasContext::new();
    context.enable_referenced_manifest_demands();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let error = context
        .reader(&read)
        .require_referenced_manifests(&[hash])
        .await
        .unwrap_err();
    assert!(BlobManifestsRequired::from_error(&error).unwrap().is_none());
    drop(read);
    assert!(
        install_manifest(&storage, &state, hash, &wire(b"corrupted manifest"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn preconnect_chunk_check_requires_explicit_demand_or_verified_presence() {
    let (storage, state) = fixture().await;
    let bytes = b"content requested by authenticated manifest";
    let hash = BlobId::from_content(bytes);
    let chunk = ChunkHash::from_content(bytes);
    let error = chunk_is_resident(&storage, &state, chunk)
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_STORAGE_ERROR);
    assert!(error.message.contains("demand marker"));
    // A bare demand error cannot authorize fetching an arbitrary missing key.
    assert!(install_chunk(&storage, &state, chunk, bytes).await.is_err());
    install_manifest(&storage, &state, hash, &wire(bytes))
        .await
        .unwrap();
    assert!(!chunk_is_resident(&storage, &state, chunk).await.unwrap());
    install_chunk(&storage, &state, chunk, bytes).await.unwrap();
    assert!(chunk_is_resident(&storage, &state, chunk).await.unwrap());
}
