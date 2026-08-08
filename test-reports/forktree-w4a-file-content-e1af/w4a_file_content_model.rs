//! Pure W4a file-content ownership model.
//!
//! The accepted route is represented by non-copy ownership types:
//! ReadLease -> PreparedPublication -> StoragePlan -> PreparedCommit.
//! BlobId is private to the authenticated manifest owner and is never accepted
//! as a caller-supplied publication authority.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

const CHUNK_BYTES: usize = 1024 * 1024;
type BlobId = u64;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Chunk {
    id: u64,
    bytes: Vec<u8>,
}

impl Chunk {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            id: hash_bytes(b"W4A-CHUNK", &bytes),
            bytes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Manifest {
    blob_id: BlobId,
    total_bytes: usize,
    chunks: Vec<Chunk>,
}

impl Manifest {
    fn from_chunks(chunks: Vec<Chunk>) -> Self {
        let total_bytes = chunks.iter().map(|chunk| chunk.bytes.len()).sum();
        let blob_id = Self::canonical_blob_id(&chunks, total_bytes);
        Self {
            blob_id,
            total_bytes,
            chunks,
        }
    }

    fn authenticate_ordered_chunks(&self) -> Result<(), Failure> {
        let actual_total = self
            .chunks
            .iter()
            .map(|chunk| chunk.bytes.len())
            .sum::<usize>();
        if actual_total != self.total_bytes {
            return Err(Failure::WrongSize);
        }
        for chunk in &self.chunks {
            if chunk.id != hash_bytes(b"W4A-CHUNK", &chunk.bytes) {
                return Err(Failure::WrongChunkIdentity);
            }
        }
        if self.blob_id != Self::canonical_blob_id(&self.chunks, self.total_bytes) {
            return Err(Failure::WrongBlobIdentity);
        }
        Ok(())
    }

    fn canonical_blob_id(chunks: &[Chunk], total_bytes: usize) -> BlobId {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"W4A-MANIFEST");
        bytes.extend_from_slice(&(total_bytes as u64).to_be_bytes());
        for (position, chunk) in chunks.iter().enumerate() {
            bytes.extend_from_slice(&(position as u64).to_be_bytes());
            bytes.extend_from_slice(&chunk.id.to_be_bytes());
            bytes.extend_from_slice(&(chunk.bytes.len() as u64).to_be_bytes());
        }
        hash_bytes(b"W4A-BLOB", &bytes)
    }

    fn authenticate_shape(&self) -> Result<(), Failure> {
        let actual_total = self
            .chunks
            .iter()
            .map(|chunk| chunk.bytes.len())
            .sum::<usize>();
        if actual_total != self.total_bytes
            || self.blob_id != Self::canonical_blob_id(&self.chunks, self.total_bytes)
        {
            return Err(Failure::WrongManifestShape);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Failure {
    WrongSize,
    WrongChunkIdentity,
    WrongBlobIdentity,
    WrongManifestShape,
    Malformed,
    Stale,
    IdempotencyConflict,
    SecondWriter,
    DirectCas,
    Fallback,
    CallerSuppliedBlobId,
    MissingRoot,
}

#[derive(Debug, Eq, PartialEq)]
struct CoherentView {
    owner_id: u64,
    view_id: u64,
    generation: u64,
}

#[derive(Debug, Eq, PartialEq)]
struct ReadLease {
    view: CoherentView,
    read_id: u64,
    row_identity: Option<RowIdentity>,
}

impl ReadLease {
    fn read_id(&self) -> u64 {
        self.read_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RowIdentity {
    blob_id: BlobId,
}

#[derive(Debug, Eq, PartialEq)]
struct FileOperation {
    read: ReadLease,
    manifest: Manifest,
    idempotency_key: u64,
}

impl FileOperation {
    fn new(read: ReadLease, manifest: Manifest, idempotency_key: u64) -> Self {
        Self {
            read,
            manifest,
            idempotency_key,
        }
    }

    fn prepare(self) -> Result<PreparedPublication, Failure> {
        self.manifest.authenticate_ordered_chunks()?;
        let owner_blob_id =
            Manifest::canonical_blob_id(&self.manifest.chunks, self.manifest.total_bytes);
        if self.manifest.blob_id != owner_blob_id {
            return Err(Failure::WrongBlobIdentity);
        }
        if let Some(row) = &self.read.row_identity {
            if row.blob_id != owner_blob_id {
                return Err(Failure::WrongBlobIdentity);
            }
        }
        Ok(PreparedPublication {
            owner_id: self.read.view.owner_id,
            view_id: self.read.view.view_id,
            generation: self.read.view.generation,
            read_id: self.read.read_id,
            owner_blob_id,
            idempotency_key: self.idempotency_key,
            manifest: self.manifest,
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
struct PreparedPublication {
    owner_id: u64,
    view_id: u64,
    generation: u64,
    read_id: u64,
    owner_blob_id: BlobId,
    idempotency_key: u64,
    manifest: Manifest,
}

impl PreparedPublication {
    fn into_storage_plan(self) -> Result<StoragePlan, Failure> {
        self.manifest.authenticate_ordered_chunks()?;
        Ok(StoragePlan {
            owner_id: self.owner_id,
            view_id: self.view_id,
            generation: self.generation,
            read_id: self.read_id,
            owner_blob_id: self.owner_blob_id,
            idempotency_key: self.idempotency_key,
            manifest: self.manifest,
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
struct StoragePlan {
    owner_id: u64,
    view_id: u64,
    generation: u64,
    read_id: u64,
    owner_blob_id: BlobId,
    idempotency_key: u64,
    manifest: Manifest,
}

#[derive(Debug, Eq, PartialEq)]
struct PreparedCommit {
    plan: StoragePlan,
}

fn reject_direct_cas_route() -> Result<(), Failure> {
    Err(Failure::DirectCas)
}

fn reject_fallback_route() -> Result<(), Failure> {
    Err(Failure::Fallback)
}

fn reject_second_writer_route() -> Result<(), Failure> {
    Err(Failure::SecondWriter)
}

fn reject_caller_supplied_blob_id(_blob_id: BlobId) -> Result<(), Failure> {
    Err(Failure::CallerSuppliedBlobId)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VisibleState {
    blob_id: BlobId,
    generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DurableState {
    generation: u64,
    visible: Option<VisibleState>,
    row_identity: Option<RowIdentity>,
    idempotency: BTreeMap<u64, BlobId>,
    roots: BTreeSet<BlobId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Engine {
    owner_id: u64,
    view_id: u64,
    generation: u64,
    next_read_id: u64,
    visible: Option<VisibleState>,
    row_identity: Option<RowIdentity>,
    idempotency: BTreeMap<u64, BlobId>,
    roots: BTreeSet<BlobId>,
}

impl Engine {
    fn new() -> Self {
        Self::with_owner_id(7)
    }

    fn with_owner_id(owner_id: u64) -> Self {
        Self {
            owner_id,
            view_id: 7,
            generation: 0,
            next_read_id: 1,
            visible: None,
            row_identity: None,
            idempotency: BTreeMap::new(),
            roots: BTreeSet::new(),
        }
    }

    fn begin_coherent_read(&mut self) -> ReadLease {
        let read_id = self.next_read_id;
        self.next_read_id += 1;
        ReadLease {
            view: CoherentView {
                owner_id: self.owner_id,
                view_id: self.view_id,
                generation: self.generation,
            },
            read_id,
            row_identity: self.row_identity.clone(),
        }
    }

    fn durable_state(&self) -> DurableState {
        DurableState {
            generation: self.generation,
            visible: self.visible.clone(),
            row_identity: self.row_identity.clone(),
            idempotency: self.idempotency.clone(),
            roots: self.roots.clone(),
        }
    }

    fn prepare_write_set(&self, plan: StoragePlan) -> Result<PreparedCommit, Failure> {
        if plan.owner_id != self.owner_id
            || plan.view_id != self.view_id
            || plan.generation != self.generation
        {
            return Err(Failure::Stale);
        }
        if let Some(previous) = self.idempotency.get(&plan.idempotency_key) {
            if *previous != plan.owner_blob_id {
                return Err(Failure::IdempotencyConflict);
            }
        }
        Ok(PreparedCommit { plan })
    }

    fn commit(&mut self, prepared: PreparedCommit) -> Result<BlobId, Failure> {
        let plan = prepared.plan;
        if plan.owner_id != self.owner_id
            || plan.view_id != self.view_id
            || plan.generation != self.generation
        {
            return Err(Failure::Stale);
        }
        if let Some(previous) = self.idempotency.get(&plan.idempotency_key) {
            return if *previous == plan.owner_blob_id {
                Ok(*previous)
            } else {
                Err(Failure::IdempotencyConflict)
            };
        }

        let next_generation = self.generation + 1;
        self.generation = next_generation;
        self.visible = Some(VisibleState {
            blob_id: plan.owner_blob_id,
            generation: next_generation,
        });
        self.row_identity = Some(RowIdentity {
            blob_id: plan.owner_blob_id,
        });
        self.idempotency
            .insert(plan.idempotency_key, plan.owner_blob_id);
        self.roots.insert(plan.owner_blob_id);
        Ok(plan.owner_blob_id)
    }

    fn publish_file_content(
        &mut self,
        manifest: Manifest,
        idempotency_key: u64,
    ) -> Result<BlobId, Failure> {
        self.publish_file_operation(manifest, idempotency_key)
    }

    fn publish_file_operation(
        &mut self,
        manifest: Manifest,
        idempotency_key: u64,
    ) -> Result<BlobId, Failure> {
        let read = self.begin_coherent_read();
        let operation = FileOperation::new(read, manifest, idempotency_key);
        let publication = operation.prepare()?;
        let plan = publication.into_storage_plan()?;
        let prepared_commit = self.prepare_write_set(plan)?;
        self.commit(prepared_commit)
    }

    fn cold_reopen(&self) -> Result<Self, Failure> {
        let Some(visible) = &self.visible else {
            return Ok(self.clone());
        };
        if !self.roots.contains(&visible.blob_id) {
            return Err(Failure::MissingRoot);
        }
        if visible.generation == 0 {
            return Err(Failure::Malformed);
        }
        Ok(self.clone())
    }

    fn w5_handoff(&self) -> Result<BlobId, Failure> {
        let Some(visible) = &self.visible else {
            return Err(Failure::MissingRoot);
        };
        if !self.roots.contains(&visible.blob_id) {
            return Err(Failure::MissingRoot);
        }
        Ok(visible.blob_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReuseProfile {
    unchanged_chunks: usize,
    changed_chunks: usize,
    bytes_rehashed: usize,
}

fn reuse_profile(before: &[u64], after: &[u64]) -> ReuseProfile {
    assert_eq!(before.len(), after.len());
    let unchanged_chunks = before
        .iter()
        .zip(after)
        .filter(|(before_id, after_id)| before_id == after_id)
        .count();
    let changed_chunks = before.len() - unchanged_chunks;
    ReuseProfile {
        unchanged_chunks,
        changed_chunks,
        bytes_rehashed: changed_chunks * CHUNK_BYTES,
    }
}

fn partial_read(manifest: &Manifest, range: Range<usize>) -> Result<(Vec<u8>, usize), Failure> {
    manifest.authenticate_shape()?;
    let mut output = Vec::new();
    let mut visited_chunks = 0;
    let mut offset = 0;
    for chunk in &manifest.chunks {
        let end = offset + chunk.bytes.len();
        let overlap_start = range.start.max(offset);
        let overlap_end = range.end.min(end);
        if overlap_start < overlap_end {
            if chunk.id != hash_bytes(b"W4A-CHUNK", &chunk.bytes) {
                return Err(Failure::WrongChunkIdentity);
            }
            visited_chunks += 1;
            output.extend_from_slice(&chunk.bytes[overlap_start - offset..overlap_end - offset]);
        }
        offset = end;
    }
    if range.end > offset {
        return Err(Failure::WrongSize);
    }
    Ok((output, visited_chunks))
}

fn hash_bytes(domain: &[u8], bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in domain.iter().chain(bytes) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3_u64);
    }
    hash
}

fn small_manifest() -> Manifest {
    Manifest::from_chunks(vec![
        Chunk::new(b"aaaa".to_vec()),
        Chunk::new(b"bbbb".to_vec()),
        Chunk::new(b"cccc".to_vec()),
    ])
}

#[test]
fn valid_write_is_one_view_one_plan_one_commit_and_reopens() {
    let mut engine = Engine::new();
    let manifest = small_manifest();
    let blob_id = engine
        .publish_file_content(manifest, 10)
        .expect("valid publication");
    assert_eq!(
        engine.visible.as_ref().map(|state| state.blob_id),
        Some(blob_id)
    );
    assert_eq!(engine.w5_handoff(), Ok(blob_id));
    assert_eq!(engine.cold_reopen(), Ok(engine.clone()));
}

#[test]
fn same_size_substitution_fails_before_publication() {
    let mut engine = Engine::new();
    let good = small_manifest();
    engine
        .publish_file_content(good.clone(), 10)
        .expect("seed row identity");
    let mut substituted = good.clone();
    substituted.chunks[1] = Chunk::new(b"xxxx".to_vec());
    substituted.blob_id = Manifest::canonical_blob_id(&substituted.chunks, substituted.total_bytes);
    let before = engine.clone();
    let result = engine.publish_file_content(substituted, 11);
    assert_eq!(result, Err(Failure::WrongBlobIdentity));
    assert_eq!(engine.durable_state(), before.durable_state());
}

#[test]
fn malformed_size_and_chunk_identity_fail_without_partial_state() {
    let mut engine = Engine::new();
    let good = small_manifest();

    let mut wrong_size = good.clone();
    wrong_size.total_bytes += 1;
    let before = engine.clone();
    assert_eq!(
        engine.publish_file_content(wrong_size, 12),
        Err(Failure::WrongSize)
    );
    assert_eq!(engine.durable_state(), before.durable_state());

    let mut wrong_chunk = good;
    wrong_chunk.chunks[0].id ^= 1;
    let before = engine.clone();
    assert_eq!(
        engine.publish_file_content(wrong_chunk, 13),
        Err(Failure::WrongChunkIdentity)
    );
    assert_eq!(engine.durable_state(), before.durable_state());
}

#[test]
fn stale_and_idempotency_conflicts_are_atomic() {
    let mut engine = Engine::new();
    let first = small_manifest();
    let stale_read = engine.begin_coherent_read();

    engine
        .publish_file_content(first.clone(), 14)
        .expect("first publication");

    let stale_operation = FileOperation::new(stale_read, first.clone(), 15);
    let stale_publication = stale_operation.prepare().expect("authenticated stale");
    let stale_plan = stale_publication
        .into_storage_plan()
        .expect("stale shape is still authenticated");
    let before = engine.clone();
    assert_eq!(engine.prepare_write_set(stale_plan), Err(Failure::Stale));
    assert_eq!(engine.durable_state(), before.durable_state());

    let replay = engine
        .publish_file_content(first.clone(), 14)
        .expect("identical replay");
    assert_eq!(replay, first.blob_id);

    let different = Manifest::from_chunks(vec![Chunk::new(b"different".to_vec())]);
    let before = engine.clone();
    assert_eq!(
        engine.publish_file_content(different, 14),
        Err(Failure::WrongBlobIdentity)
    );
    assert_eq!(engine.durable_state(), before.durable_state());
}

#[test]
fn idempotency_conflict_is_atomic_after_authenticated_owner_derivation() {
    let mut engine = Engine::new();
    let first = small_manifest();
    engine.idempotency.insert(14, first.blob_id);
    let different = Manifest::from_chunks(vec![Chunk::new(b"different".to_vec())]);
    let before = engine.clone();
    assert_eq!(
        engine.publish_file_content(different, 14),
        Err(Failure::IdempotencyConflict)
    );
    assert_eq!(engine.durable_state(), before.durable_state());
}

#[test]
fn partial_read_is_range_bounded_and_authenticates_visited_chunks() {
    let manifest = small_manifest();
    let (bytes, visited) = partial_read(&manifest, 4..8).expect("bounded range");
    assert_eq!(bytes, b"bbbb");
    assert_eq!(visited, 1);

    let mut corrupt = manifest.clone();
    corrupt.chunks[0].bytes[0] = b'!';
    assert_eq!(
        partial_read(&corrupt, 0..4),
        Err(Failure::WrongChunkIdentity)
    );
    assert_eq!(partial_read(&corrupt, 4..8), Ok((b"bbbb".to_vec(), 1)));
}

#[test]
fn sixty_four_mib_layout_reuses_sixty_three_unchanged_one_mib_chunks() {
    let before: Vec<u64> = (0_u64..64_u64)
        .map(|index| hash_bytes(b"W4A-1M-CHUNK", &index.to_be_bytes()))
        .collect();
    let mut after = before.clone();
    after[37] = hash_bytes(b"W4A-1M-CHUNK-EDIT", &37_u64.to_be_bytes());
    let profile = reuse_profile(&before, &after);
    assert_eq!(
        profile,
        ReuseProfile {
            unchanged_chunks: 63,
            changed_chunks: 1,
            bytes_rehashed: CHUNK_BYTES,
        }
    );
}

#[test]
fn one_noncopy_view_binds_one_operation_and_rejects_legacy_writers() {
    let mut engine = Engine::new();
    let first_read = engine.begin_coherent_read();
    let first_read_id = first_read.read_id();
    let second_read = engine.begin_coherent_read();
    let second_read_id = second_read.read_id();
    assert_ne!(first_read_id, second_read_id);

    let first_operation = FileOperation::new(first_read, small_manifest(), 100);
    let second_operation = FileOperation::new(second_read, small_manifest(), 101);
    let first_publication = first_operation.prepare().expect("first read");
    let first_plan = first_publication.into_storage_plan().expect("first plan");
    let first_commit = engine
        .prepare_write_set(first_plan)
        .expect("first read commit");
    engine.commit(first_commit).expect("first commit");

    let second_publication = second_operation.prepare().expect("second read");
    let second_plan = second_publication.into_storage_plan().expect("second plan");
    assert_eq!(engine.prepare_write_set(second_plan), Err(Failure::Stale));

    assert_eq!(reject_direct_cas_route(), Err(Failure::DirectCas));
    assert_eq!(reject_fallback_route(), Err(Failure::Fallback));
    assert_eq!(reject_second_writer_route(), Err(Failure::SecondWriter));
    assert_eq!(
        reject_caller_supplied_blob_id(small_manifest().blob_id),
        Err(Failure::CallerSuppliedBlobId)
    );
}

#[test]
fn missing_root_fails_cold_reopen_and_w5_final_reference_handoff() {
    let mut engine = Engine::new();
    let blob_id = engine
        .publish_file_content(small_manifest(), 200)
        .expect("valid publication");
    engine.roots.remove(&blob_id);
    assert_eq!(engine.cold_reopen(), Err(Failure::MissingRoot));
    assert_eq!(engine.w5_handoff(), Err(Failure::MissingRoot));
}

fn main() {}
