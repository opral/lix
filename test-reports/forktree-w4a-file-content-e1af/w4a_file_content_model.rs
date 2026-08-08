//! Pure W4a model. It is intentionally independent of the Lix production crate.
//! The model uses deterministic fingerprints, not a production codec or storage
//! adapter. It proves the ownership/corruption/rollback contract before wiring.

use std::collections::{BTreeMap, BTreeSet};

const CHUNK_BYTES: usize = 1024 * 1024;

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
    blob_id: u64,
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct Attempt {
    view_id: u64,
    expected_generation: u64,
    idempotency_key: u64,
    manifest: Manifest,
    reads: u8,
    plans: u8,
    commits: u8,
    direct_cas: bool,
    fallback: bool,
}

impl Attempt {
    fn valid(engine: &Engine, manifest: Manifest, idempotency_key: u64) -> Self {
        Self {
            view_id: engine.view_id,
            expected_generation: engine.generation,
            idempotency_key,
            manifest,
            reads: 1,
            plans: 1,
            commits: 1,
            direct_cas: false,
            fallback: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VisibleState {
    blob_id: u64,
    generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Engine {
    view_id: u64,
    generation: u64,
    visible: Option<VisibleState>,
    idempotency: BTreeMap<u64, u64>,
    roots: BTreeSet<u64>,
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

    fn publish(&mut self, attempt: Attempt) -> Result<u64, Failure> {
        if attempt.view_id != self.view_id {
            return Err(Failure::Stale);
        }
        if attempt.reads != 1 {
            return Err(Failure::SecondRead);
        }
        if attempt.plans != 1 || attempt.commits != 1 {
            return Err(Failure::SecondWriter);
        }
        if attempt.direct_cas {
            return Err(Failure::DirectCas);
        }
        if attempt.fallback {
            return Err(Failure::Fallback);
        }
        if attempt.expected_generation != self.generation {
            return Err(Failure::Stale);
        }
        attempt.manifest.authenticate()?;
        if let Some(previous) = self.idempotency.get(&attempt.idempotency_key) {
            return if *previous == attempt.manifest.blob_id {
                Ok(*previous)
            } else {
                Err(Failure::IdempotencyConflict)
            };
        }

        let next_generation = self.generation + 1;
        self.generation = next_generation;
        self.visible = Some(VisibleState {
            blob_id: attempt.manifest.blob_id,
            generation: next_generation,
        });
        self.idempotency
            .insert(attempt.idempotency_key, attempt.manifest.blob_id);
        self.roots.insert(attempt.manifest.blob_id);
        Ok(attempt.manifest.blob_id)
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

    fn w5_handoff(&self) -> Result<u64, Failure> {
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

fn partial_read(manifest: &Manifest, range: std::ops::Range<usize>) -> Result<(Vec<u8>, usize), Failure> {
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

fn manifest_id(chunks: &[Chunk], total_bytes: usize) -> u64 {
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
        .publish(Attempt::valid(&engine, manifest, 10))
        .expect("valid publication");
    assert_eq!(engine.visible.as_ref().map(|state| state.blob_id), Some(blob_id));
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
    let result = engine.publish(Attempt::valid(&engine, substituted, 11));
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
        engine.publish(Attempt::valid(&engine, wrong_size, 12)),
        Err(Failure::WrongSize)
    );
    assert_eq!(engine, before);

    let mut wrong_chunk = good;
    wrong_chunk.chunks[0].id ^= 1;
    let before = engine.clone();
    assert_eq!(
        engine.publish(Attempt::valid(&engine, wrong_chunk, 13)),
        Err(Failure::WrongChunkIdentity)
    );
    assert_eq!(engine, before);
}

#[test]
fn stale_and_idempotency_conflicts_are_atomic() {
    let mut engine = Engine::new();
    let first = small_manifest();
    let attempt = Attempt::valid(&engine, first.clone(), 14);
    engine.publish(attempt).expect("first publication");

    let mut stale = Attempt::valid(&engine, first.clone(), 15);
    stale.expected_generation -= 1;
    let before = engine.clone();
    assert_eq!(engine.publish(stale), Err(Failure::Stale));
    assert_eq!(engine, before);

    let replay = Attempt::valid(&engine, first.clone(), 14);
    assert_eq!(engine.publish(replay), Ok(first.blob_id));

    let different = Manifest::from_chunks(vec![Chunk::new(b"different".to_vec())]);
    let conflict = Attempt::valid(&engine, different, 14);
    let before = engine.clone();
    assert_eq!(engine.publish(conflict), Err(Failure::IdempotencyConflict));
    assert_eq!(engine, before);
}

#[test]
fn partial_read_is_range_bounded_and_authenticates_visited_chunks() {
    let manifest = small_manifest();
    let (bytes, visited) = partial_read(&manifest, 4..8).expect("bounded range");
    assert_eq!(bytes, b"bbbb");
    assert_eq!(visited, 1);

    let mut corrupt = manifest;
    corrupt.chunks[0].bytes[0] = b'!';
    assert_eq!(partial_read(&corrupt, 4..8), Ok((b"bbbb".to_vec(), 1)));
    assert_eq!(partial_read(&corrupt, 0..4), Err(Failure::WrongChunkIdentity));
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
    let mut engine = Engine::new();
    let manifest = small_manifest();
    for (name, expected) in [
        ("second-read", Failure::SecondRead),
        ("second-writer", Failure::SecondWriter),
        ("direct-cas", Failure::DirectCas),
        ("fallback", Failure::Fallback),
    ] {
        let mut attempt =
            Attempt::valid(&engine, manifest.clone(), 100 + u64::from(name.len() as u8));
        match name {
            "second-read" => attempt.reads = 2,
            "second-writer" => attempt.plans = 2,
            "direct-cas" => attempt.direct_cas = true,
            "fallback" => attempt.fallback = true,
            _ => unreachable!("fixture list is closed"),
        }
        let before = engine.clone();
        assert_eq!(engine.publish(attempt), Err(expected), "fixture {name}");
        assert_eq!(engine, before, "fixture {name} changed state");
    }
}

fn main() {}
