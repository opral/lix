//! Pure W4a file-content ownership model.
//!
//! This is intentionally independent of the production crate.  Unlike the
//! rejected model, the accepted route is represented by ownership types:
//! CoherentView -> PreparedPublication -> StoragePlan -> PreparedCommit.
//! No caller-supplied read/plan/commit counters can make a legacy route pass.

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
        let blob_id = manifest_id(&chunks, total_bytes);
        Self {
            blob_id,
            total_bytes,
            chunks,
        }
    }

    fn authenticate(&self) -> Result<(), Failure> {
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
        if self.blob_id != manifest_id(&self.chunks, self.total_bytes) {
            return Err(Failure::WrongBlobIdentity);
        }
        Ok(())
    }

    fn authenticate_shape(&self) -> Result<(), Failure> {
        let actual_total = self
            .chunks
            .iter()
            .map(|chunk| chunk.bytes.len())
            .sum::<usize>();
        if actual_total != self.total_bytes
            || self.blob_id != manifest_id(&self.chunks, self.total_bytes)
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
    SecondRead,
    SecondWriter,
    DirectCas,
    Fallback,
    MissingRoot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CoherentView {
    view_id: u64,
    generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedPublication {
    view: CoherentView,
    idempotency_key: u64,
    manifest: Manifest,
}

impl PreparedPublication {
    fn into_storage_plan(self) -> Result<StoragePlan, Failure> {
        self.manifest.authenticate()?;
        Ok(StoragePlan {
            view: self.view,
            idempotency_key: self.idempotency_key,
            manifest: self.manifest,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoragePlan {
    view: CoherentView,
    idempotency_key: u64,
    manifest: Manifest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedCommit {
    plan: StoragePlan,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LegacyRoute {
    SecondRead,
    SecondWriter,
    DirectCas,
    Fallback,
}

fn reject_legacy_route(route: LegacyRoute) -> Result<(), Failure> {
    Err(match route {
        LegacyRoute::SecondRead => Failure::SecondRead,
        LegacyRoute::SecondWriter => Failure::SecondWriter,
        LegacyRoute::DirectCas => Failure::DirectCas,
        LegacyRoute::Fallback => Failure::Fallback,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VisibleState {
    blob_id: BlobId,
    generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Engine {
    view_id: u64,
    generation: u64,
    visible: Option<VisibleState>,
    idempotency: BTreeMap<u64, BlobId>,
    roots: BTreeSet<BlobId>,
}

impl Engine {
    fn new() -> Self {
        Self {
            view_id: 7,
            generation: 0,
            visible: None,
            idempotency: BTreeMap::new(),
            roots: BTreeSet::new(),
        }
    }

    fn begin_coherent_read(&self) -> CoherentView {
        CoherentView {
            view_id: self.view_id,
            generation: self.generation,
        }
    }

    fn prepare_file_content(
        &self,
        view: CoherentView,
        manifest: Manifest,
        idempotency_key: u64,
    ) -> PreparedPublication {
        PreparedPublication {
            view,
            idempotency_key,
            manifest,
        }
    }

    fn prepare_write_set(&self, plan: StoragePlan) -> Result<PreparedCommit, Failure> {
        if plan.view.view_id != self.view_id || plan.view.generation != self.generation {
            return Err(Failure::Stale);
        }
        if let Some(previous) = self.idempotency.get(&plan.idempotency_key) {
            if *previous != plan.manifest.blob_id {
                return Err(Failure::IdempotencyConflict);
            }
        }
        Ok(PreparedCommit { plan })
    }

    fn commit(&mut self, prepared: PreparedCommit) -> Result<BlobId, Failure> {
        let plan = prepared.plan;
        if plan.view.view_id != self.view_id || plan.view.generation != self.generation {
            return Err(Failure::Stale);
        }
        if let Some(previous) = self.idempotency.get(&plan.idempotency_key) {
            return if *previous == plan.manifest.blob_id {
                Ok(*previous)
            } else {
                Err(Failure::IdempotencyConflict)
            };
        }

        let next_generation = self.generation + 1;
        self.generation = next_generation;
        self.visible = Some(VisibleState {
            blob_id: plan.manifest.blob_id,
            generation: next_generation,
        });
        self.idempotency
            .insert(plan.idempotency_key, plan.manifest.blob_id);
        self.roots.insert(plan.manifest.blob_id);
        Ok(plan.manifest.blob_id)
    }

    fn publish_file_content(
        &mut self,
        manifest: Manifest,
        idempotency_key: u64,
    ) -> Result<BlobId, Failure> {
        let view = self.begin_coherent_read();
        let publication = self.prepare_file_content(view, manifest, idempotency_key);
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

fn manifest_id(chunks: &[Chunk], total_bytes: usize) -> BlobId {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"W4A-MANIFEST");
    bytes.extend_from_slice(&(total_bytes as u64).to_be_bytes());
    for chunk in chunks {
        bytes.extend_from_slice(&chunk.id.to_be_bytes());
        bytes.extend_from_slice(&(chunk.bytes.len() as u64).to_be_bytes());
    }
    hash_bytes(b"W4A-BLOB", &bytes)
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
    let mut substituted = good.clone();
    substituted.chunks[1] = Chunk::new(b"xxxx".to_vec());
    let before = engine.clone();
    let result = engine.publish_file_content(substituted, 11);
    assert_eq!(result, Err(Failure::WrongBlobIdentity));
    assert_eq!(engine, before);
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
    assert_eq!(engine, before);

    let mut wrong_chunk = good;
    wrong_chunk.chunks[0].id ^= 1;
    let before = engine.clone();
    assert_eq!(
        engine.publish_file_content(wrong_chunk, 13),
        Err(Failure::WrongChunkIdentity)
    );
    assert_eq!(engine, before);
}

#[test]
fn stale_and_idempotency_conflicts_are_atomic() {
    let mut engine = Engine::new();
    let first = small_manifest();
    let stale_view = engine.begin_coherent_read();

    engine
        .publish_file_content(first.clone(), 14)
        .expect("first publication");

    let stale_publication = engine.prepare_file_content(stale_view, first.clone(), 15);
    let stale_plan = stale_publication
        .into_storage_plan()
        .expect("stale shape is still authenticated");
    let before = engine.clone();
    assert_eq!(engine.prepare_write_set(stale_plan), Err(Failure::Stale));
    assert_eq!(engine, before);

    let replay = engine
        .publish_file_content(first.clone(), 14)
        .expect("identical replay");
    assert_eq!(replay, first.blob_id);

    let different = Manifest::from_chunks(vec![Chunk::new(b"different".to_vec())]);
    let before = engine.clone();
    assert_eq!(
        engine.publish_file_content(different, 14),
        Err(Failure::IdempotencyConflict)
    );
    assert_eq!(engine, before);
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
fn second_read_writer_direct_cas_and_fallback_fixtures_are_rejected() {
    let engine = Engine::new();
    for route in [
        LegacyRoute::SecondRead,
        LegacyRoute::SecondWriter,
        LegacyRoute::DirectCas,
        LegacyRoute::Fallback,
    ] {
        assert!(reject_legacy_route(route).is_err());
    }

    let manifest = small_manifest();
    let mut published = engine.clone();
    published
        .publish_file_content(manifest, 100)
        .expect("transaction route");
    assert!(published.visible.is_some());
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
