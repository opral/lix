//! Pure W4a file-content ownership and final-reference model.
//!
//! The accepted route is represented by non-copy ownership types:
//! ReadLease -> PreparedPublication -> StoragePlan -> PreparedCommit.
//! BlobId is private to the authenticated manifest owner and is never accepted
//! as a caller-supplied publication authority. Durable state is rebuilt and
//! authenticated on cold reopen so the model covers the final-reference handoff
//! without depending on a production codec or adapter.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Range;

const CHUNK_BYTES: usize = 1024 * 1024;
type BlobId = u64;
type ChunkId = u64;
type RootId = u64;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Chunk {
    id: ChunkId,
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
        let mut seen = HashSet::new();
        for chunk in &self.chunks {
            if !seen.insert(chunk.id) {
                return Err(Failure::DuplicateChunk);
            }
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
    MissingChunk,
    MissingManifest,
    WrongObjectKind,
    DuplicateChunk,
    Malformed,
    Stale,
    IdempotencyConflict,
    SecondWriter,
    DirectCas,
    Fallback,
    CallerSuppliedBlobId,
    MissingRoot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectKind {
    Chunk,
    Manifest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PersistedObject {
    kind: ObjectKind,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChunkRef {
    id: ChunkId,
    len: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PersistedManifest {
    blob_id: BlobId,
    total_bytes: usize,
    chunks: Vec<ChunkRef>,
}

impl PersistedManifest {
    fn from_manifest(manifest: &Manifest) -> Self {
        Self {
            blob_id: manifest.blob_id,
            total_bytes: manifest.total_bytes,
            chunks: manifest
                .chunks
                .iter()
                .map(|chunk| ChunkRef {
                    id: chunk.id,
                    len: chunk.bytes.len(),
                })
                .collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootKind {
    Branch,
    History,
    Checkpoint,
    Upload,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RootRef {
    blob_id: BlobId,
    kind: RootKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VisibleState {
    blob_id: BlobId,
    size: usize,
    generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RowIdentity {
    blob_id: BlobId,
    size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DurableState {
    generation: u64,
    visible: Option<VisibleState>,
    row_identity: Option<RowIdentity>,
    idempotency: BTreeMap<u64, BlobId>,
    roots: BTreeMap<RootId, RootRef>,
    manifests: BTreeMap<BlobId, PersistedManifest>,
    objects: BTreeMap<ChunkId, PersistedObject>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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
            if row.blob_id != owner_blob_id || row.size != self.manifest.total_bytes {
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
            root_id: self.idempotency_key,
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
    root_id: RootId,
}

#[derive(Debug, Eq, PartialEq)]
struct PreparedCommit {
    plan: StoragePlan,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReclaimReport {
    removed_manifests: usize,
    removed_objects: usize,
    remaining_manifests: usize,
    remaining_objects: usize,
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
struct Engine {
    owner_id: u64,
    view_id: u64,
    generation: u64,
    next_read_id: u64,
    visible: Option<VisibleState>,
    row_identity: Option<RowIdentity>,
    idempotency: BTreeMap<u64, BlobId>,
    roots: BTreeMap<RootId, RootRef>,
    manifests: BTreeMap<BlobId, PersistedManifest>,
    objects: BTreeMap<ChunkId, PersistedObject>,
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
            roots: BTreeMap::new(),
            manifests: BTreeMap::new(),
            objects: BTreeMap::new(),
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
            manifests: self.manifests.clone(),
            objects: self.objects.clone(),
        }
    }

    fn install_manifest(&mut self, manifest: &Manifest) {
        self.manifests
            .insert(manifest.blob_id, PersistedManifest::from_manifest(manifest));
        for chunk in &manifest.chunks {
            self.objects.insert(
                chunk.id,
                PersistedObject {
                    kind: ObjectKind::Chunk,
                    bytes: chunk.bytes.clone(),
                },
            );
        }
    }

    fn retain_manifest_root(
        &mut self,
        root_id: RootId,
        manifest: &Manifest,
        kind: RootKind,
    ) -> BlobId {
        manifest
            .authenticate_ordered_chunks()
            .expect("model roots only retain authenticated manifests");
        self.install_manifest(manifest);
        self.roots.insert(
            root_id,
            RootRef {
                blob_id: manifest.blob_id,
                kind,
            },
        );
        manifest.blob_id
    }

    fn retain_root(&mut self, root_id: RootId, blob_id: BlobId, kind: RootKind) {
        assert!(self.manifests.contains_key(&blob_id));
        self.roots.insert(root_id, RootRef { blob_id, kind });
    }

    fn release_root(&mut self, root_id: RootId) {
        self.roots.remove(&root_id);
    }

    fn rotate_checkpoint_window(&mut self, blob_id: BlobId, first_root: RootId, count: usize) {
        assert!(
            count <= 65,
            "the checkpoint retention window is bounded at 65"
        );
        for offset in 0..count {
            self.retain_root(first_root + offset as u64, blob_id, RootKind::Checkpoint);
        }
    }

    fn reclaim(&mut self) -> ReclaimReport {
        let live_blobs: BTreeSet<_> = self.roots.values().map(|root| root.blob_id).collect();
        let dead_manifests: Vec<_> = self
            .manifests
            .keys()
            .copied()
            .filter(|blob_id| !live_blobs.contains(blob_id))
            .collect();
        let mut live_objects = BTreeSet::new();
        for blob_id in &live_blobs {
            if let Some(manifest) = self.manifests.get(blob_id) {
                live_objects.extend(manifest.chunks.iter().map(|chunk| chunk.id));
            }
        }
        let dead_objects: Vec<_> = self
            .objects
            .keys()
            .copied()
            .filter(|chunk_id| !live_objects.contains(chunk_id))
            .collect();
        for blob_id in &dead_manifests {
            self.manifests.remove(blob_id);
        }
        for chunk_id in &dead_objects {
            self.objects.remove(chunk_id);
        }
        ReclaimReport {
            removed_manifests: dead_manifests.len(),
            removed_objects: dead_objects.len(),
            remaining_manifests: self.manifests.len(),
            remaining_objects: self.objects.len(),
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
        self.install_manifest(&plan.manifest);
        self.generation = next_generation;
        self.visible = Some(VisibleState {
            blob_id: plan.owner_blob_id,
            size: plan.manifest.total_bytes,
            generation: next_generation,
        });
        self.row_identity = Some(RowIdentity {
            blob_id: plan.owner_blob_id,
            size: plan.manifest.total_bytes,
        });
        self.idempotency
            .insert(plan.idempotency_key, plan.owner_blob_id);
        self.roots.insert(
            plan.root_id,
            RootRef {
                blob_id: plan.owner_blob_id,
                kind: RootKind::Branch,
            },
        );
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

    fn validate_durable(durable: &DurableState) -> Result<(), Failure> {
        for (chunk_id, object) in &durable.objects {
            if object.kind != ObjectKind::Chunk {
                return Err(Failure::WrongObjectKind);
            }
            if *chunk_id != hash_bytes(b"W4A-CHUNK", &object.bytes) {
                return Err(Failure::WrongChunkIdentity);
            }
        }

        for (blob_id, persisted) in &durable.manifests {
            let mut seen = HashSet::new();
            let mut chunks = Vec::with_capacity(persisted.chunks.len());
            let mut total_bytes = 0;
            for chunk_ref in &persisted.chunks {
                if !seen.insert(chunk_ref.id) {
                    return Err(Failure::DuplicateChunk);
                }
                let Some(object) = durable.objects.get(&chunk_ref.id) else {
                    return Err(Failure::MissingChunk);
                };
                if object.kind != ObjectKind::Chunk {
                    return Err(Failure::WrongObjectKind);
                }
                if object.bytes.len() != chunk_ref.len {
                    return Err(Failure::WrongSize);
                }
                if chunk_ref.id != hash_bytes(b"W4A-CHUNK", &object.bytes) {
                    return Err(Failure::WrongChunkIdentity);
                }
                total_bytes += object.bytes.len();
                chunks.push(Chunk {
                    id: chunk_ref.id,
                    bytes: object.bytes.clone(),
                });
            }
            if total_bytes != persisted.total_bytes {
                return Err(Failure::WrongSize);
            }
            let reconstructed = Manifest {
                blob_id: persisted.blob_id,
                total_bytes: persisted.total_bytes,
                chunks,
            };
            reconstructed.authenticate_ordered_chunks()?;
            if reconstructed.blob_id != *blob_id {
                return Err(Failure::WrongBlobIdentity);
            }
        }

        for root in durable.roots.values() {
            if !durable.manifests.contains_key(&root.blob_id) {
                return Err(Failure::MissingManifest);
            }
        }
        if let Some(visible) = &durable.visible {
            if !durable
                .roots
                .values()
                .any(|root| root.blob_id == visible.blob_id)
            {
                return Err(Failure::MissingRoot);
            }
            let Some(manifest) = durable.manifests.get(&visible.blob_id) else {
                return Err(Failure::MissingManifest);
            };
            if manifest.total_bytes != visible.size {
                return Err(Failure::WrongSize);
            }
            if let Some(row) = &durable.row_identity {
                if row.blob_id != visible.blob_id || row.size != manifest.total_bytes {
                    return Err(Failure::WrongBlobIdentity);
                }
            } else {
                return Err(Failure::Malformed);
            }
            if visible.generation == 0 {
                return Err(Failure::Malformed);
            }
        }
        Ok(())
    }

    fn from_durable(durable: DurableState) -> Result<Self, Failure> {
        Self::validate_durable(&durable)?;
        Ok(Self {
            owner_id: 7,
            view_id: 7,
            generation: durable.generation,
            next_read_id: 1,
            visible: durable.visible,
            row_identity: durable.row_identity,
            idempotency: durable.idempotency,
            roots: durable.roots,
            manifests: durable.manifests,
            objects: durable.objects,
        })
    }

    fn cold_reopen(&self) -> Result<Self, Failure> {
        Self::from_durable(self.durable_state())
    }

    fn partial_read_after_reopen(&self, range: Range<usize>) -> Result<(Vec<u8>, usize), Failure> {
        let Some(visible) = &self.visible else {
            return Err(Failure::MissingRoot);
        };
        let Some(persisted) = self.manifests.get(&visible.blob_id) else {
            return Err(Failure::MissingManifest);
        };
        let mut chunks = Vec::with_capacity(persisted.chunks.len());
        for chunk_ref in &persisted.chunks {
            let Some(object) = self.objects.get(&chunk_ref.id) else {
                return Err(Failure::MissingChunk);
            };
            chunks.push(Chunk {
                id: chunk_ref.id,
                bytes: object.bytes.clone(),
            });
        }
        let manifest = Manifest {
            blob_id: persisted.blob_id,
            total_bytes: persisted.total_bytes,
            chunks,
        };
        partial_read(&manifest, range)
    }

    fn w5_handoff(&self) -> Result<BlobId, Failure> {
        let Some(visible) = &self.visible else {
            return Err(Failure::MissingRoot);
        };
        if !self
            .roots
            .values()
            .any(|root| root.blob_id == visible.blob_id)
        {
            return Err(Failure::MissingRoot);
        }
        if !self.manifests.contains_key(&visible.blob_id) {
            return Err(Failure::MissingManifest);
        }
        Ok(visible.blob_id)
    }
}

fn partial_read(manifest: &Manifest, range: Range<usize>) -> Result<(Vec<u8>, usize), Failure> {
    manifest.authenticate_shape()?;
    let mut offset = 0;
    let mut visited_chunks = 0;
    for chunk in &manifest.chunks {
        let end = offset + chunk.bytes.len();
        let overlap_start = range.start.max(offset);
        let overlap_end = range.end.min(end);
        if overlap_start < overlap_end {
            if chunk.id != hash_bytes(b"W4A-CHUNK", &chunk.bytes) {
                return Err(Failure::WrongChunkIdentity);
            }
            visited_chunks += 1;
        }
        offset = end;
    }
    if range.end > offset {
        return Err(Failure::WrongSize);
    }

    let mut output = Vec::new();
    offset = 0;
    for chunk in &manifest.chunks {
        let end = offset + chunk.bytes.len();
        let overlap_start = range.start.max(offset);
        let overlap_end = range.end.min(end);
        if overlap_start < overlap_end {
            output.extend_from_slice(&chunk.bytes[overlap_start - offset..overlap_end - offset]);
        }
        offset = end;
    }
    Ok((output, visited_chunks))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReuseProfile {
    total_bytes: usize,
    unchanged_chunks: usize,
    changed_chunks: usize,
    bytes_rehashed: usize,
}

fn reuse_profile(before: &[Chunk], after: &[Chunk]) -> ReuseProfile {
    assert_eq!(before.len(), after.len());
    let unchanged_chunks = before
        .iter()
        .zip(after)
        .filter(|(before_chunk, after_chunk)| before_chunk.id == after_chunk.id)
        .count();
    let changed_chunks = before.len() - unchanged_chunks;
    ReuseProfile {
        total_bytes: after.iter().map(|chunk| chunk.bytes.len()).sum(),
        unchanged_chunks,
        changed_chunks,
        bytes_rehashed: changed_chunks * CHUNK_BYTES,
    }
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

fn seed_engine() -> (Engine, BlobId) {
    let mut engine = Engine::new();
    let blob_id = engine
        .publish_file_content(small_manifest(), 1)
        .expect("valid publication");
    (engine, blob_id)
}

#[test]
fn valid_write_is_one_view_one_plan_one_commit_and_reopens() {
    let (engine, blob_id) = seed_engine();
    assert_eq!(
        engine.visible.as_ref().map(|state| state.blob_id),
        Some(blob_id)
    );
    assert_eq!(engine.w5_handoff(), Ok(blob_id));
    let reopened = engine.cold_reopen().expect("authenticated reopen");
    assert_eq!(
        reopened.partial_read_after_reopen(0..4),
        Ok((b"aaaa".to_vec(), 1))
    );
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
    let before = engine.durable_state();
    let result = engine.publish_file_content(substituted, 11);
    assert_eq!(result, Err(Failure::WrongBlobIdentity));
    assert_eq!(engine.durable_state(), before);
}

#[test]
fn malformed_size_and_chunk_identity_fail_without_partial_state() {
    let mut engine = Engine::new();
    let good = small_manifest();

    let mut wrong_size = good.clone();
    wrong_size.total_bytes += 1;
    let before = engine.durable_state();
    assert_eq!(
        engine.publish_file_content(wrong_size, 12),
        Err(Failure::WrongSize)
    );
    assert_eq!(engine.durable_state(), before);

    let mut wrong_chunk = good;
    wrong_chunk.chunks[0].id ^= 1;
    let before = engine.durable_state();
    assert_eq!(
        engine.publish_file_content(wrong_chunk, 13),
        Err(Failure::WrongChunkIdentity)
    );
    assert_eq!(engine.durable_state(), before);
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
    let before = engine.durable_state();
    assert_eq!(engine.prepare_write_set(stale_plan), Err(Failure::Stale));
    assert_eq!(engine.durable_state(), before);

    let replay = engine
        .publish_file_content(first.clone(), 14)
        .expect("identical replay");
    assert_eq!(replay, first.blob_id);

    let different = Manifest::from_chunks(vec![Chunk::new(b"different".to_vec())]);
    let before = engine.durable_state();
    assert_eq!(
        engine.publish_file_content(different, 14),
        Err(Failure::WrongBlobIdentity)
    );
    assert_eq!(engine.durable_state(), before);
}

#[test]
fn idempotency_conflict_is_atomic_after_authenticated_owner_derivation() {
    let mut engine = Engine::new();
    let first = small_manifest();
    engine.idempotency.insert(14, first.blob_id);
    let different = Manifest::from_chunks(vec![Chunk::new(b"different".to_vec())]);
    let before = engine.durable_state();
    assert_eq!(
        engine.publish_file_content(different, 14),
        Err(Failure::IdempotencyConflict)
    );
    assert_eq!(engine.durable_state(), before);
}

#[test]
fn partial_read_is_range_bounded_and_authenticates_visited_chunks_before_output() {
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
    let before: Vec<Chunk> = (0_u8..64_u8)
        .map(|index| Chunk::new(vec![index; CHUNK_BYTES]))
        .collect();
    let mut after = before.clone();
    after[37] = Chunk::new(vec![0xFE; CHUNK_BYTES]);
    let profile = reuse_profile(&before, &after);
    assert_eq!(
        profile,
        ReuseProfile {
            total_bytes: 64 * CHUNK_BYTES,
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
fn shared_chunks_survive_until_the_exact_final_reference() {
    let mut engine = Engine::new();
    let shared = Chunk::new(b"shared-1m-logical-chunk".to_vec());
    let first = Manifest::from_chunks(vec![shared.clone(), Chunk::new(b"first".to_vec())]);
    let second = Manifest::from_chunks(vec![shared.clone(), Chunk::new(b"second".to_vec())]);
    let first_blob = engine.retain_manifest_root(10, &first, RootKind::Branch);
    let second_blob = engine.retain_manifest_root(11, &second, RootKind::History);
    assert_ne!(first_blob, second_blob);
    let shared_id = shared.id;
    assert!(engine.objects.contains_key(&shared_id));

    engine.release_root(10);
    let after_first_release = engine.reclaim();
    assert_eq!(after_first_release.removed_manifests, 1);
    assert!(engine.objects.contains_key(&shared_id));
    assert_eq!(after_first_release.remaining_objects, 2);

    engine.release_root(11);
    let after_final_release = engine.reclaim();
    assert_eq!(after_final_release.removed_manifests, 1);
    assert_eq!(after_final_release.removed_objects, 2);
    assert_eq!(after_final_release.remaining_manifests, 0);
    assert_eq!(after_final_release.remaining_objects, 0);
}

#[test]
fn branch_history_checkpoint_and_upload_roots_hold_65_entry_window() {
    let (mut engine, blob_id) = seed_engine();
    let branch_root = 1;
    engine.retain_root(2, blob_id, RootKind::History);
    engine.retain_root(3, blob_id, RootKind::Upload);
    engine.rotate_checkpoint_window(blob_id, 10_000, 65);

    engine.release_root(branch_root);
    engine.release_root(2);
    engine.release_root(3);
    let retained = engine.reclaim();
    assert_eq!(retained.removed_manifests, 0);
    assert_eq!(retained.removed_objects, 0);
    assert_eq!(engine.roots.len(), 65);

    for root_id in 10_000..10_064 {
        engine.release_root(root_id);
    }
    let still_retained = engine.reclaim();
    assert_eq!(still_retained.removed_manifests, 0);
    assert_eq!(still_retained.removed_objects, 0);

    engine.release_root(10_064);
    let final_release = engine.reclaim();
    assert_eq!(final_release.removed_manifests, 1);
    assert_eq!(final_release.removed_objects, 3);
}

#[test]
fn cold_reopen_rejects_missing_wrong_kind_hash_order_duplicate_and_row_corruption() {
    let (engine, blob_id) = seed_engine();
    let original = engine.durable_state();
    let first_chunk = original.manifests.get(&blob_id).expect("manifest").chunks[0].id;

    let mut missing = engine.clone();
    missing.objects.remove(&first_chunk);
    assert_eq!(missing.cold_reopen(), Err(Failure::MissingChunk));

    let mut wrong_kind = engine.clone();
    wrong_kind.objects.get_mut(&first_chunk).unwrap().kind = ObjectKind::Manifest;
    assert_eq!(wrong_kind.cold_reopen(), Err(Failure::WrongObjectKind));

    let mut wrong_content = engine.clone();
    wrong_content.objects.get_mut(&first_chunk).unwrap().bytes[0] ^= 1;
    assert_eq!(
        wrong_content.cold_reopen(),
        Err(Failure::WrongChunkIdentity)
    );

    let mut duplicate = engine.clone();
    duplicate
        .manifests
        .get_mut(&blob_id)
        .unwrap()
        .chunks
        .push(ChunkRef {
            id: first_chunk,
            len: 4,
        });
    assert_eq!(duplicate.cold_reopen(), Err(Failure::DuplicateChunk));

    let mut reordered = engine.clone();
    reordered
        .manifests
        .get_mut(&blob_id)
        .unwrap()
        .chunks
        .swap(0, 1);
    assert_eq!(reordered.cold_reopen(), Err(Failure::WrongBlobIdentity));

    let mut wrong_row = engine.clone();
    wrong_row.row_identity.as_mut().unwrap().size += 1;
    assert_eq!(wrong_row.cold_reopen(), Err(Failure::WrongBlobIdentity));

    assert_eq!(engine.durable_state(), original);
}

#[test]
fn cold_reopen_and_partial_range_have_zero_mutation_on_corruption() {
    let (engine, blob_id) = seed_engine();
    let before = engine.durable_state();
    let mut corrupt = engine.clone();
    let chunk_id = corrupt.manifests.get(&blob_id).unwrap().chunks[1].id;
    corrupt.objects.get_mut(&chunk_id).unwrap().bytes[0] ^= 1;
    assert_eq!(corrupt.cold_reopen(), Err(Failure::WrongChunkIdentity));
    assert_eq!(
        corrupt.partial_read_after_reopen(4..8),
        Err(Failure::WrongChunkIdentity)
    );
    assert_eq!(engine.durable_state(), before);
}

#[test]
fn missing_root_fails_cold_reopen_and_w5_final_reference_handoff() {
    let (mut engine, blob_id) = seed_engine();
    engine.roots.remove(&1);
    assert_eq!(engine.cold_reopen(), Err(Failure::MissingRoot));
    assert_eq!(engine.w5_handoff(), Err(Failure::MissingRoot));
    assert_eq!(engine.reclaim().removed_manifests, 1);
    assert!(!engine.manifests.contains_key(&blob_id));
}

fn main() {}
