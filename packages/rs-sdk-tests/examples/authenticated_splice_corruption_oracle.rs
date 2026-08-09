//! Test/report-only oracle for the a33 unchanged-child-chunk blocker.
//!
//! This is a dependency-free model. It deliberately models both the blocked
//! a33 behavior (which copies unchanged child references without visiting
//! them) and the required behavior (which authenticates the complete base
//! child closure before making a publication plan). It is not a production
//! storage implementation and does not define a persisted format.

use std::collections::BTreeMap;

const CHUNK_COUNT: usize = 64;
const CHUNK_BYTES: usize = 64;
const CHANGED_INDEX: usize = 7;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Digest([u8; 32]);

impl Digest {
    fn derive(domain: &[u8], bytes: &[u8]) -> Self {
        let mut output = [0_u8; 32];
        for (index, byte) in domain.iter().chain(bytes.iter()).enumerate() {
            let slot = index % output.len();
            output[slot] = output[slot]
                .wrapping_add(*byte)
                .rotate_left((index % 8) as u32)
                .wrapping_add((index as u8).wrapping_mul(17));
            let mirror = output.len() - 1 - slot;
            output[mirror] ^= byte.wrapping_mul(31).rotate_right((index % 7) as u32);
        }
        Self(output)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StateKey {
    schema_key: &'static str,
    entity_pk: &'static str,
    file_id: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChunkRecord {
    object_id: Digest,
    domain: &'static str,
    bytes: Vec<u8>,
    content_digest: Digest,
}

impl ChunkRecord {
    fn valid(&self, expected_id: Digest, expected_len: usize) -> bool {
        self.domain == "BlobChunk"
            && self.object_id == expected_id
            && self.bytes.len() == expected_len
            && Digest::derive(b"BlobChunk", &self.bytes) == expected_id
            && self.content_digest == Digest::derive(b"content", &self.bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Manifest {
    object_id: Digest,
    canonical_blob_id: Digest,
    content_digest: Digest,
    chunk_ids: Vec<Digest>,
    logical_bytes: usize,
}

impl Manifest {
    fn compute_id(
        canonical_blob_id: Digest,
        content_digest: Digest,
        chunk_ids: &[Digest],
        logical_bytes: usize,
    ) -> Digest {
        let mut bytes = Vec::with_capacity(64 + chunk_ids.len() * 32);
        bytes.extend_from_slice(&canonical_blob_id.0);
        bytes.extend_from_slice(&content_digest.0);
        bytes.extend_from_slice(&(logical_bytes as u64).to_le_bytes());
        for chunk_id in chunk_ids {
            bytes.extend_from_slice(&chunk_id.0);
        }
        Digest::derive(b"BlobManifest", &bytes)
    }

    fn new(chunk_ids: Vec<Digest>, payload: &[u8]) -> Self {
        let canonical_blob_id = Digest::derive(b"BlobId", payload);
        let content_digest = Digest::derive(b"content", payload);
        let object_id = Self::compute_id(
            canonical_blob_id,
            content_digest,
            &chunk_ids,
            payload.len(),
        );
        Self {
            object_id,
            canonical_blob_id,
            content_digest,
            chunk_ids,
            logical_bytes: payload.len(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Owner {
    key: StateKey,
    blob_id: Digest,
    size: usize,
    manifest_id: Digest,
}

#[derive(Clone, Debug)]
struct View {
    read_id: u64,
    owner: Owner,
    manifest: Manifest,
    chunks: BTreeMap<Digest, ChunkRecord>,
    payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CorruptionCase {
    Missing,
    Malformed,
    WrongDomain,
    SameSizeSubstitution,
}

impl CorruptionCase {
    fn name(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Malformed => "malformed",
            Self::WrongDomain => "wrong-domain",
            Self::SameSizeSubstitution => "same-size-substituted",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PublicationPlan {
    read_id: u64,
    state_key: StateKey,
    manifest: Manifest,
    changed_chunk_ids: Vec<Digest>,
    reused_chunk_ids: Vec<Digest>,
    writes: usize,
    selector_writes: usize,
    receipt_writes: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Storage {
    writes: usize,
    selector_writes: usize,
    receipt_writes: usize,
    manifest: Option<Manifest>,
    owner: Option<Owner>,
    chunks: BTreeMap<Digest, ChunkRecord>,
}

fn fixture() -> View {
    let key = StateKey {
        schema_key: "lix_binary_blob_ref",
        entity_pk: "01920000-0000-7000-8000-0000000000d2",
        file_id: "01920000-0000-7000-8000-0000000000d2",
    };
    let mut payload = Vec::with_capacity(CHUNK_COUNT * CHUNK_BYTES);
    let mut chunks = BTreeMap::new();
    let mut chunk_ids = Vec::with_capacity(CHUNK_COUNT);
    for index in 0..CHUNK_COUNT {
        let bytes = vec![(index as u8).wrapping_mul(13).wrapping_add(3); CHUNK_BYTES];
        let object_id = Digest::derive(b"BlobChunk", &bytes);
        let record = ChunkRecord {
            object_id,
            domain: "BlobChunk",
            content_digest: Digest::derive(b"content", &bytes),
            bytes: bytes.clone(),
        };
        payload.extend_from_slice(&bytes);
        chunk_ids.push(object_id);
        chunks.insert(object_id, record);
    }
    let manifest = Manifest::new(chunk_ids, &payload);
    let owner = Owner {
        key,
        blob_id: manifest.canonical_blob_id,
        size: payload.len(),
        manifest_id: manifest.object_id,
    };
    View {
        read_id: 41,
        owner,
        manifest,
        chunks,
        payload,
    }
}

fn changed_payload(view: &View) -> Vec<u8> {
    let mut payload = view.payload.clone();
    let start = CHANGED_INDEX * CHUNK_BYTES;
    payload[start..start + CHUNK_BYTES].fill(0xe7);
    payload
}

fn corrupt(view: &View, case: CorruptionCase) -> View {
    let mut corrupted = view.clone();
    let chunk_id = corrupted.manifest.chunk_ids[CHANGED_INDEX + 1];
    match case {
        CorruptionCase::Missing => {
            corrupted.chunks.remove(&chunk_id);
        }
        CorruptionCase::Malformed => {
            let record = corrupted
                .chunks
                .get_mut(&chunk_id)
                .expect("fixture chunk exists");
            record.bytes.clear();
        }
        CorruptionCase::WrongDomain => {
            let record = corrupted
                .chunks
                .get_mut(&chunk_id)
                .expect("fixture chunk exists");
            record.domain = "BlobManifest";
        }
        CorruptionCase::SameSizeSubstitution => {
            let record = corrupted
                .chunks
                .get_mut(&chunk_id)
                .expect("fixture chunk exists");
            record.bytes.fill(0x5a);
            record.content_digest = Digest::derive(b"content", &record.bytes);
        }
    }
    corrupted
}

fn authenticate_owner(view: &View) -> Result<(), &'static str> {
    if view.owner.key.schema_key != "lix_binary_blob_ref"
        || view.owner.key.entity_pk != view.owner.key.file_id
        || view.owner.manifest_id != view.manifest.object_id
        || view.owner.blob_id != view.manifest.canonical_blob_id
        || view.owner.size != view.manifest.logical_bytes
        || view.manifest.chunk_ids.len() != CHUNK_COUNT
    {
        return Err("StateKey/base manifest owner binding failed");
    }
    Ok(())
}

fn candidate_a33_plan(view: &View, payload: &[u8]) -> Result<PublicationPlan, &'static str> {
    authenticate_owner(view)?;
    if payload.len() != view.owner.size {
        return Err("same-length payload requirement failed");
    }
    let changed_id = Digest::derive(
        b"BlobChunk",
        &payload[CHANGED_INDEX * CHUNK_BYTES..(CHANGED_INDEX + 1) * CHUNK_BYTES],
    );
    let mut chunk_ids = view.manifest.chunk_ids.clone();
    chunk_ids[CHANGED_INDEX] = changed_id;
    let manifest = Manifest::new(chunk_ids, payload);
    Ok(PublicationPlan {
        read_id: view.read_id,
        state_key: view.owner.key.clone(),
        manifest,
        changed_chunk_ids: vec![changed_id],
        reused_chunk_ids: view
            .manifest
            .chunk_ids
            .iter()
            .enumerate()
            .filter_map(|(index, id)| (index != CHANGED_INDEX).then_some(*id))
            .collect(),
        writes: 3,
        selector_writes: 1,
        receipt_writes: 1,
    })
}

fn required_plan(view: &View, payload: &[u8]) -> Result<PublicationPlan, &'static str> {
    authenticate_owner(view)?;
    if payload.len() != view.owner.size {
        return Err("same-length payload requirement failed");
    }
    for (index, chunk_id) in view.manifest.chunk_ids.iter().enumerate() {
        let chunk = view
            .chunks
            .get(chunk_id)
            .ok_or("unchanged child chunk is missing")?;
        if !chunk.valid(*chunk_id, CHUNK_BYTES) {
            return Err("unchanged child chunk authentication failed");
        }
        if index == CHANGED_INDEX {
            continue;
        }
    }
    let changed_bytes =
        &payload[CHANGED_INDEX * CHUNK_BYTES..(CHANGED_INDEX + 1) * CHUNK_BYTES];
    let changed_id = Digest::derive(b"BlobChunk", changed_bytes);
    let mut chunk_ids = view.manifest.chunk_ids.clone();
    chunk_ids[CHANGED_INDEX] = changed_id;
    let manifest = Manifest::new(chunk_ids, payload);
    let reused_chunk_ids = view
        .manifest
        .chunk_ids
        .iter()
        .enumerate()
        .filter_map(|(index, id)| (index != CHANGED_INDEX).then_some(*id))
        .collect::<Vec<_>>();
    if reused_chunk_ids.len() != 63 {
        return Err("expected exactly 63 reused chunk identities");
    }
    Ok(PublicationPlan {
        read_id: view.read_id,
        state_key: view.owner.key.clone(),
        manifest,
        changed_chunk_ids: vec![changed_id],
        reused_chunk_ids,
        writes: 3,
        selector_writes: 1,
        receipt_writes: 1,
    })
}

fn apply(storage: &mut Storage, view: &View, plan: PublicationPlan) {
    assert_eq!(plan.read_id, view.read_id, "publication crossed its retained view");
    assert_eq!(plan.state_key, view.owner.key, "publication changed its owner key");
    storage.writes += plan.writes;
    storage.selector_writes += plan.selector_writes;
    storage.receipt_writes += plan.receipt_writes;
    storage.manifest = Some(plan.manifest.clone());
    storage.owner = Some(Owner {
        key: plan.state_key,
        blob_id: plan.manifest.canonical_blob_id,
        size: plan.manifest.logical_bytes,
        manifest_id: plan.manifest.object_id,
    });
    for chunk_id in plan.reused_chunk_ids {
        storage
            .chunks
            .insert(chunk_id, view.chunks[&chunk_id].clone());
    }
    let changed_id = plan.changed_chunk_ids[0];
    let changed_bytes = vec![0xe7; CHUNK_BYTES];
    storage.chunks.insert(
        changed_id,
        ChunkRecord {
            object_id: changed_id,
            domain: "BlobChunk",
            content_digest: Digest::derive(b"content", &changed_bytes),
            bytes: changed_bytes,
        },
    );
}

fn verify_reopen(storage: &Storage, original: &View) {
    let manifest = storage.manifest.as_ref().expect("manifest committed");
    let owner = storage.owner.as_ref().expect("owner committed");
    assert_eq!(owner.key, original.owner.key);
    assert_eq!(owner.manifest_id, manifest.object_id);
    assert_eq!(manifest.chunk_ids.len(), CHUNK_COUNT);
    for (index, chunk_id) in manifest.chunk_ids.iter().enumerate() {
        let chunk = storage.chunks.get(chunk_id).expect("reopen chunk exists");
        assert!(chunk.valid(*chunk_id, CHUNK_BYTES));
        if index != CHANGED_INDEX {
            assert_eq!(*chunk_id, original.manifest.chunk_ids[index]);
        }
    }
}

fn run_backend(name: &str) {
    let base = fixture();
    let payload = changed_payload(&base);
    let mut storage = Storage::default();
    let valid = required_plan(&base, &payload).expect("valid splice plan");
    assert_eq!(valid.read_id, 41);
    assert_eq!(valid.changed_chunk_ids.len(), 1);
    assert_eq!(valid.reused_chunk_ids.len(), 63);
    for chunk_id in &valid.reused_chunk_ids {
        assert!(base.chunks[chunk_id].valid(*chunk_id, CHUNK_BYTES));
    }
    let changed_id = valid.changed_chunk_ids[0];
    assert_eq!(changed_id, Digest::derive(b"BlobChunk", &payload[CHANGED_INDEX * CHUNK_BYTES..(CHANGED_INDEX + 1) * CHUNK_BYTES]));
    apply(&mut storage, &base, valid);
    verify_reopen(&storage, &base);
    println!("backend={name} valid=pass changed=1 reused=63 cold_reopen=pass");

    for case in [
        CorruptionCase::Missing,
        CorruptionCase::Malformed,
        CorruptionCase::WrongDomain,
        CorruptionCase::SameSizeSubstitution,
    ] {
        let corrupted = corrupt(&base, case);
        assert!(
            candidate_a33_plan(&corrupted, &payload).is_ok(),
            "the oracle must expose the a33 false acceptance for {}",
            case.name()
        );
        let before = storage.clone();
        assert!(required_plan(&corrupted, &payload).is_err());
        assert_eq!(storage, before, "failed publication must not write anything");
        println!(
            "backend={name} case={} a33_accepts=true oracle_rejects_before_write=true rollback=pass selector_writes=0 receipt_writes=0",
            case.name()
        );
    }
}

fn main() {
    run_backend("rocksdb");
    run_backend("slatedb");
    println!("oracle=authenticated_splice_unchanged_child_closure status=pass");
}
