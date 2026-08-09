use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use bytes::Bytes;

use crate::binary_cas::{BlobEditSplice, BlobId};
use crate::storage::StorageError;
use crate::storage_adapter::StorageAdapterRead;

use super::blob::CANONICAL_BLOB_CHUNK_BYTES;
use super::codec::{Encoder, corruption, keyed_hash};
use super::model::{
    BLOB_MERKLE_CHUNK_BYTES, BlobChunkV1, BlobManifestV1, BlobMerkleInternalV1, BlobMerkleLeafV1,
    BlobMerkleNodeRefV1, canonical_merkle_blob_id,
};
use super::object::{
    ObjectDomain, ObjectId, authenticate_object_domain, decode_id, decode_object, encode_id,
    encode_object,
};
use super::state::{StateKey, StateKeyRef, encode_state_key};
use super::tree::ImmutableObjectSet;
use super::view::load_object_bytes;

const MERKLE_STATE_BINDING_DOMAIN: &str = "lix forktree blob merkle state binding v1";
const MAX_PROOF_DEPTH: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NodeSummary {
    object_id: ObjectId,
    height: u32,
    first_ordinal: u64,
    leaf_count: u64,
    logical_bytes: u64,
}

impl NodeSummary {
    fn as_ref(self) -> BlobMerkleNodeRefV1 {
        BlobMerkleNodeRefV1 {
            object_id: self.object_id,
            height: self.height,
            first_ordinal: self.first_ordinal,
            leaf_count: self.leaf_count,
            logical_bytes: self.logical_bytes,
        }
    }

    fn shifted(self, delta: i64) -> Result<Self, StorageError> {
        let first_ordinal = self
            .first_ordinal
            .checked_add_signed(delta)
            .ok_or_else(|| corruption("Merkle ordinal shift underflows or overflows"))?;
        Ok(Self {
            first_ordinal,
            ..self
        })
    }
}

fn shift_ref(value: BlobMerkleNodeRefV1, delta: i64) -> Result<BlobMerkleNodeRefV1, StorageError> {
    Ok(NodeSummary {
        object_id: value.object_id,
        height: value.height,
        first_ordinal: value.first_ordinal,
        leaf_count: value.leaf_count,
        logical_bytes: value.logical_bytes,
    }
    .shifted(delta)?
    .as_ref())
}

fn infer_parent_shift(
    internal: BlobMerkleInternalV1,
    raw_left: NodeSummary,
    raw_right: NodeSummary,
    node_shift: i64,
) -> i64 {
    if internal.left == raw_left.as_ref() && internal.right == raw_right.as_ref() {
        node_shift
    } else {
        0
    }
}

#[derive(Clone, Debug)]
struct LeafWithId {
    value: BlobMerkleLeafV1,
    object_id: ObjectId,
}

#[derive(Clone, Debug)]
enum DecodedNode {
    Leaf(BlobMerkleLeafV1),
    Internal(BlobMerkleInternalV1),
}

impl DecodedNode {
    fn summary(&self, object_id: ObjectId) -> NodeSummary {
        match self {
            Self::Leaf(value) => NodeSummary {
                object_id,
                height: 0,
                first_ordinal: value.ordinal,
                leaf_count: 1,
                logical_bytes: value.declared_len,
            },
            Self::Internal(value) => NodeSummary {
                object_id,
                height: value.height,
                first_ordinal: value.first_ordinal,
                leaf_count: value.leaf_count,
                logical_bytes: value.logical_bytes,
            },
        }
    }
}

/// Complete immutable output of the core builder. The object set is not a
/// publication plan; the future transaction owner may copy the authenticated
/// objects into its existing PreparedPublication.
#[derive(Clone, Debug)]
pub(crate) struct BlobMerkleTreeBuild {
    pub(crate) manifest: BlobManifestV1,
    pub(crate) objects: ImmutableObjectSet,
}

#[derive(Clone, Debug)]
struct BlobMerkleProofPathV1 {
    leaf_object_id: ObjectId,
    leaf_ordinal: u64,
    ordinal_shift: i64,
    steps: Vec<BlobMerkleProofStepV1>,
}

#[derive(Clone, Debug)]
struct BlobMerkleProofStepV1 {
    parent_object_id: ObjectId,
    sibling_object_id: ObjectId,
    sibling_is_left: bool,
    sibling_shift: i64,
    parent_shift: i64,
}

/// A bounded range proof containing only requested leaves, their chunk objects,
/// and one authenticated sibling root per proof level. It intentionally has no
/// storage handle, cache, or writer capability.
#[derive(Clone, Debug)]
pub(crate) struct BlobMerkleProofV1 {
    pub(crate) manifest: BlobManifestV1,
    pub(crate) requested_range: Range<u64>,
    state_binding: [u8; 32],
    paths: Vec<BlobMerkleProofPathV1>,
    objects: ImmutableObjectSet,
}

impl BlobMerkleProofV1 {
    pub(crate) fn manifest(&self) -> BlobManifestV1 {
        self.manifest
    }

    pub(crate) fn requested_range(&self) -> Range<u64> {
        self.requested_range.clone()
    }

    pub(crate) fn object_count(&self) -> usize {
        self.objects.iter().count()
    }
}

/// Builds a canonical fixed-chunk Merkle layout. `BlobId` is the canonical
/// Merkle content identity: it is derived from a domain-separated envelope
/// containing the root ObjectId, logical length, fixed-chunk geometry, leaf
/// count, and tree height. No flat whole-content BlobId calculation is used.
pub(crate) fn build_blob_merkle_tree(
    chunks: &[BlobChunkV1],
) -> Result<BlobMerkleTreeBuild, StorageError> {
    if chunks.is_empty() || (chunks.len() != 1 && chunks.iter().any(|chunk| chunk.bytes.is_empty()))
    {
        return Err(corruption(
            "Merkle layout requires canonical fixed-width chunks",
        ));
    }
    if chunks.len() > u64::MAX as usize {
        return Err(corruption("Merkle layout has too many chunks"));
    }

    let logical_bytes = chunks.iter().try_fold(0_u64, |total, chunk| {
        total
            .checked_add(chunk.bytes.len() as u64)
            .ok_or_else(|| corruption("Merkle logical length overflows u64"))
    })?;
    validate_fixed_chunk_layout(logical_bytes, chunks)?;

    let mut chunk_claims = Vec::with_capacity(chunks.len());
    let mut chunk_objects = ImmutableObjectSet::default();
    for chunk in chunks {
        let (chunk_object_id, chunk_bytes) = chunk.encode()?;
        chunk_objects.insert(chunk_object_id, chunk_bytes)?;
        chunk_claims.push((
            super::model::BlobChunkRefV1 {
                chunk_object_id,
                declared_len: chunk.bytes.len() as u64,
            },
            *blake3::hash(&chunk.bytes).as_bytes(),
        ));
    }
    let mut build = build_blob_merkle_tree_from_chunk_claims(logical_bytes, &chunk_claims)?;
    build.objects.extend(chunk_objects)?;
    Ok(build)
}

/// Builds the canonical leaf/internal closure from already authenticated
/// upload chunks. Chunk payload objects remain owned by the upload receipt and
/// are not copied into memory a second time.
pub(crate) fn build_blob_merkle_tree_from_chunk_claims(
    logical_bytes: u64,
    chunks: &[(super::model::BlobChunkRefV1, [u8; 32])],
) -> Result<BlobMerkleTreeBuild, StorageError> {
    if chunks.is_empty() {
        return Err(corruption("Merkle layout requires at least one leaf"));
    }
    let expected_count = logical_bytes
        .div_ceil(CANONICAL_BLOB_CHUNK_BYTES as u64)
        .max(1);
    if expected_count != chunks.len() as u64 {
        return Err(corruption("Merkle leaves are not canonical fixed chunks"));
    }
    let mut objects = ImmutableObjectSet::default();
    let mut leaves = Vec::with_capacity(chunks.len());
    for (ordinal, (chunk, chunk_digest)) in chunks.iter().enumerate() {
        let expected_len = if ordinal + 1 == chunks.len() {
            logical_bytes - ordinal as u64 * CANONICAL_BLOB_CHUNK_BYTES as u64
        } else {
            CANONICAL_BLOB_CHUNK_BYTES as u64
        };
        if chunk.chunk_object_id == ObjectId::ZERO || chunk.declared_len != expected_len {
            return Err(corruption(
                "Merkle chunk length or identity is not canonically positioned",
            ));
        }
        let leaf = BlobMerkleLeafV1 {
            ordinal: ordinal as u64,
            chunk_object_id: chunk.chunk_object_id,
            declared_len: chunk.declared_len,
            chunk_digest: *chunk_digest,
        };
        let (leaf_object_id, leaf_bytes) = encode_leaf(&leaf)?;
        objects.insert(leaf_object_id, leaf_bytes)?;
        leaves.push(LeafWithId {
            value: leaf,
            object_id: leaf_object_id,
        });
    }

    let root = build_node(&leaves, &mut objects)?;
    if root.first_ordinal != 0 || root.leaf_count != leaves.len() as u64 {
        return Err(corruption("Merkle root does not cover every leaf"));
    }
    let manifest = BlobManifestV1::from_merkle_root(
        logical_bytes,
        leaves.len() as u64,
        root.object_id,
        root.height,
    );
    Ok(BlobMerkleTreeBuild { manifest, objects })
}

/// Computes the sole canonical blob identity from complete content. This is
/// used by transaction-local semantic rows that must name an inline payload
/// before publication; the durable reader still authorizes only the
/// StateKey-bound manifest root and never looks content up by this value.
pub(crate) fn canonical_blob_id_for_content(content: &[u8]) -> Result<BlobId, StorageError> {
    let chunks = if content.is_empty() {
        vec![BlobChunkV1 {
            bytes: Bytes::new(),
        }]
    } else {
        content
            .chunks(CANONICAL_BLOB_CHUNK_BYTES)
            .map(|chunk| BlobChunkV1 {
                bytes: Bytes::copy_from_slice(chunk),
            })
            .collect()
    };
    Ok(build_blob_merkle_tree(&chunks)?.manifest.canonical_blob_id)
}

/// Builds the smallest authenticated Merkle fixture for unit tests.  The
/// production builder remains the canonical multi-leaf path; this helper is
/// intentionally test-only so small corruption controls do not need to carry
/// a 1 MiB allocation merely to exercise manifest encoding.
#[cfg(test)]
pub(crate) fn single_leaf_manifest_for_test(
    chunk: &BlobChunkV1,
) -> Result<(BlobManifestV1, ObjectId, Bytes), StorageError> {
    if chunk.bytes.is_empty() {
        return Err(corruption("test Merkle fixture requires a non-empty chunk"));
    }
    let (chunk_object_id, _) = chunk.encode()?;
    let leaf = BlobMerkleLeafV1 {
        ordinal: 0,
        chunk_object_id,
        declared_len: chunk.bytes.len() as u64,
        chunk_digest: *blake3::hash(&chunk.bytes).as_bytes(),
    };
    let (leaf_object_id, leaf_bytes) = encode_leaf(&leaf)?;
    Ok((
        BlobManifestV1::from_merkle_root(chunk.bytes.len() as u64, 1, leaf_object_id, 0),
        leaf_object_id,
        leaf_bytes,
    ))
}

/// Loads an exact ordinal proof from one retained storage read. Only selected
/// leaves, their chunk objects, and sibling proof nodes are retained.
pub(crate) async fn load_blob_merkle_range_proof<R>(
    read: &R,
    manifest: BlobManifestV1,
    state_key: &StateKey,
    requested_range: Range<u64>,
) -> Result<BlobMerkleProofV1, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    validate_requested_range(&manifest, &requested_range)?;
    let mut objects = ImmutableObjectSet::default();
    // Operation-local read coalescing only: bytes never outlive this proof and
    // are still authenticated at every typed edge below.
    let mut loaded = BTreeMap::<ObjectId, Bytes>::new();
    let mut paths = Vec::with_capacity((requested_range.end - requested_range.start) as usize);
    for ordinal in requested_range.clone() {
        let mut node_id = manifest.root_object_id;
        let mut expected_ref = BlobMerkleNodeRefV1 {
            object_id: manifest.root_object_id,
            height: manifest.root_height,
            first_ordinal: 0,
            leaf_count: manifest.leaf_count,
            logical_bytes: manifest.logical_bytes,
        };
        let mut steps = Vec::new();
        loop {
            let node_bytes = load_proof_object(read, &mut loaded, node_id).await?;
            let node = decode_node(node_id, &node_bytes)?;
            let raw_summary = node.summary(node_id);
            let ordinal_shift = i64::try_from(expected_ref.first_ordinal)
                .ok()
                .and_then(|expected| {
                    i64::try_from(raw_summary.first_ordinal)
                        .ok()
                        .and_then(|raw| expected.checked_sub(raw))
                })
                .ok_or_else(|| corruption("Merkle ordinal shift exceeds i64"))?;
            let summary = raw_summary.shifted(ordinal_shift)?;
            if summary.as_ref() != expected_ref {
                return Err(corruption(
                    "Merkle node does not match its authenticated parent summary",
                ));
            }
            match node {
                DecodedNode::Leaf(leaf) => {
                    if leaf.ordinal.checked_add_signed(ordinal_shift) != Some(ordinal) {
                        return Err(corruption("Merkle leaf ordinal is not canonical"));
                    }
                    objects.insert(node_id, node_bytes)?;
                    let chunk_bytes =
                        load_proof_object(read, &mut loaded, leaf.chunk_object_id).await?;
                    objects.insert(leaf.chunk_object_id, chunk_bytes)?;
                    paths.push(BlobMerkleProofPathV1 {
                        leaf_object_id: node_id,
                        leaf_ordinal: ordinal,
                        ordinal_shift,
                        steps,
                    });
                    break;
                }
                DecodedNode::Internal(internal) => {
                    let left = shift_ref(internal.left, ordinal_shift)?;
                    let right = shift_ref(internal.right, ordinal_shift)?;
                    let in_left = ordinal >= left.first_ordinal
                        && ordinal < left.first_ordinal + left.leaf_count;
                    let in_right = ordinal >= right.first_ordinal
                        && ordinal < right.first_ordinal + right.leaf_count;
                    if in_left == in_right {
                        return Err(corruption(
                            "Merkle ordinal is not covered by exactly one child",
                        ));
                    }
                    let (child, sibling, sibling_is_left) = if in_left {
                        (left, right, false)
                    } else {
                        (right, left, true)
                    };
                    let sibling_bytes =
                        load_proof_object(read, &mut loaded, sibling.object_id).await?;
                    let sibling_node = decode_node(sibling.object_id, &sibling_bytes)?;
                    let raw_sibling = sibling_node.summary(sibling.object_id);
                    let sibling_shift = i64::try_from(sibling.first_ordinal)
                        .ok()
                        .and_then(|expected| {
                            i64::try_from(raw_sibling.first_ordinal)
                                .ok()
                                .and_then(|raw| expected.checked_sub(raw))
                        })
                        .ok_or_else(|| corruption("Merkle sibling shift exceeds i64"))?;
                    if raw_sibling.shifted(sibling_shift)?.as_ref() != sibling {
                        return Err(corruption(
                            "Merkle sibling does not match its authenticated parent summary",
                        ));
                    }
                    let child_bytes = load_proof_object(read, &mut loaded, child.object_id).await?;
                    let child_node = decode_node(child.object_id, &child_bytes)?;
                    let raw_child = child_node.summary(child.object_id);
                    let parent_shift = if sibling_is_left {
                        infer_parent_shift(internal, raw_sibling, raw_child, ordinal_shift)
                    } else {
                        infer_parent_shift(internal, raw_child, raw_sibling, ordinal_shift)
                    };
                    objects.insert(sibling.object_id, sibling_bytes)?;
                    steps.push(BlobMerkleProofStepV1 {
                        parent_object_id: node_id,
                        sibling_object_id: sibling.object_id,
                        sibling_is_left,
                        sibling_shift,
                        parent_shift,
                    });
                    node_id = child.object_id;
                    expected_ref = child;
                }
            }
        }
    }
    let proof = BlobMerkleProofV1 {
        manifest,
        requested_range: requested_range.clone(),
        state_binding: state_binding(state_key, &manifest),
        paths,
        objects,
    };
    verify_blob_merkle_range(&proof, state_key, manifest, requested_range)?;
    Ok(proof)
}

async fn load_proof_object<R>(
    read: &R,
    loaded: &mut BTreeMap<ObjectId, Bytes>,
    id: ObjectId,
) -> Result<Bytes, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if let Some(bytes) = loaded.get(&id) {
        return Ok(bytes.clone());
    }
    let bytes = load_object_bytes(read, id).await?;
    loaded.insert(id, bytes.clone());
    Ok(bytes)
}

pub(crate) fn leaf_range_for_bytes(
    manifest: &BlobManifestV1,
    requested: Range<u64>,
) -> Result<Range<u64>, StorageError> {
    if manifest.logical_bytes == 0 && requested == (0..0) && manifest.leaf_count == 1 {
        return Ok(0..1);
    }
    if requested.start >= requested.end || requested.end > manifest.logical_bytes {
        return Err(corruption("Merkle byte range is invalid"));
    }
    let chunk_bytes = u64::from(BLOB_MERKLE_CHUNK_BYTES);
    Ok(requested.start / chunk_bytes..requested.end.div_ceil(chunk_bytes))
}

pub(crate) fn materialize_blob_merkle_range(
    proof: &BlobMerkleProofV1,
    state_key: &StateKey,
    manifest: BlobManifestV1,
    requested: Range<u64>,
) -> Result<Vec<u8>, StorageError> {
    let leaf_range = leaf_range_for_bytes(&manifest, requested.clone())?;
    verify_blob_merkle_range(proof, state_key, manifest, leaf_range)?;
    let mut output = Vec::with_capacity((requested.end - requested.start) as usize);
    for path in &proof.paths {
        let leaf = decode_leaf(
            path.leaf_object_id,
            proof
                .objects
                .get(path.leaf_object_id)
                .ok_or_else(|| corruption("Merkle materialization leaf is missing"))?,
        )?;
        let chunk = BlobChunkV1::decode_borrowed(
            leaf.chunk_object_id,
            proof
                .objects
                .get(leaf.chunk_object_id)
                .ok_or_else(|| corruption("Merkle materialization chunk is missing"))?,
        )?;
        // The leaf payload may be retained from a subtree whose encoded
        // ordinal starts at zero.  The proof path carries its authenticated
        // successor placement; byte materialization must use that logical
        // ordinal rather than the raw leaf coordinate.
        let chunk_start = path.leaf_ordinal * u64::from(BLOB_MERKLE_CHUNK_BYTES);
        let start = requested.start.saturating_sub(chunk_start) as usize;
        let end = (requested.end.min(chunk_start + leaf.declared_len) - chunk_start) as usize;
        output.extend_from_slice(&chunk[start..end]);
    }
    if output.len() as u64 != requested.end - requested.start {
        return Err(corruption("Merkle materialized byte range is incomplete"));
    }
    Ok(output)
}

/// Creates a proof for an exact half-open range of leaf ordinals. The proof
/// contains O(K log N) tree objects for K requested leaves and never copies an
/// unrelated leaf payload.
pub(crate) fn prove_blob_merkle_range(
    build: &BlobMerkleTreeBuild,
    state_key: &StateKey,
    requested_range: Range<u64>,
) -> Result<BlobMerkleProofV1, StorageError> {
    validate_requested_range(&build.manifest, &requested_range)?;
    let mut objects = ImmutableObjectSet::default();
    let mut paths = Vec::with_capacity((requested_range.end - requested_range.start) as usize);
    for ordinal in requested_range.clone() {
        let mut steps = Vec::new();
        let mut leaf_shift = 0_i64;
        collect_path(
            &build.objects,
            build.manifest.root_object_id,
            ordinal,
            &mut objects,
            &mut steps,
            BlobMerkleNodeRefV1 {
                object_id: build.manifest.root_object_id,
                height: build.manifest.root_height,
                first_ordinal: 0,
                leaf_count: build.manifest.leaf_count,
                logical_bytes: build.manifest.logical_bytes,
            },
            &mut leaf_shift,
        )?;
        let leaf_object_id = leaf_id_from_path(
            &build.objects,
            build.manifest.root_object_id,
            ordinal,
            BlobMerkleNodeRefV1 {
                object_id: build.manifest.root_object_id,
                height: build.manifest.root_height,
                first_ordinal: 0,
                leaf_count: build.manifest.leaf_count,
                logical_bytes: build.manifest.logical_bytes,
            },
        )?;
        let leaf_bytes = build
            .objects
            .get(leaf_object_id)
            .ok_or_else(|| corruption("Merkle leaf object is missing during proof build"))?;
        objects.insert(leaf_object_id, leaf_bytes.clone())?;
        let leaf = decode_leaf(leaf_object_id, leaf_bytes)?;
        let chunk_bytes = build
            .objects
            .get(leaf.chunk_object_id)
            .ok_or_else(|| corruption("Merkle chunk object is missing during proof build"))?;
        objects.insert(leaf.chunk_object_id, chunk_bytes.clone())?;
        paths.push(BlobMerkleProofPathV1 {
            leaf_object_id,
            leaf_ordinal: ordinal,
            ordinal_shift: leaf_shift,
            steps,
        });
    }
    Ok(BlobMerkleProofV1 {
        manifest: build.manifest,
        requested_range,
        state_binding: state_binding(state_key, &build.manifest),
        paths,
        objects,
    })
}

/// Verifies the exact StateKey-bound range proof and every requested child
/// chunk. No unrelated subtree payload is required; sibling node bytes remain
/// authenticated by their canonical ObjectIds and parent reconstruction.
pub(crate) fn verify_blob_merkle_range(
    proof: &BlobMerkleProofV1,
    state_key: &StateKey,
    expected_manifest: BlobManifestV1,
    requested_range: Range<u64>,
) -> Result<(), StorageError> {
    if proof.manifest != expected_manifest || proof.requested_range != requested_range {
        return Err(corruption(
            "Merkle proof manifest or range is not the requested one",
        ));
    }
    validate_requested_range(&expected_manifest, &requested_range)?;
    if proof.state_binding != state_binding(state_key, &expected_manifest) {
        return Err(corruption("Merkle proof is bound to a different StateKey"));
    }
    if proof.paths.len() != (requested_range.end - requested_range.start) as usize {
        return Err(corruption("Merkle proof omits a requested leaf"));
    }
    let canonical_empty_request = expected_manifest.logical_bytes == 0
        && expected_manifest.leaf_count == 1
        && expected_manifest.root_height == 0
        && requested_range == (0..1);

    let mut seen_ordinals = BTreeSet::new();
    let mut verified_root = None;
    for (path, expected_ordinal) in proof.paths.iter().zip(requested_range) {
        if path.leaf_ordinal != expected_ordinal || !seen_ordinals.insert(path.leaf_ordinal) {
            return Err(corruption(
                "Merkle proof leaf ordinals are duplicated or unordered",
            ));
        }
        let leaf_bytes = proof
            .objects
            .get(path.leaf_object_id)
            .ok_or_else(|| corruption("Merkle proof leaf object is missing"))?;
        let leaf = decode_leaf(path.leaf_object_id, leaf_bytes)?;
        if leaf.ordinal.checked_add_signed(path.ordinal_shift) != Some(path.leaf_ordinal) {
            return Err(corruption("Merkle proof leaf ordinal is not object-bound"));
        }
        if leaf.declared_len == 0
            && !(canonical_empty_request
                && leaf.ordinal == 0
                && path.steps.is_empty()
                && path.leaf_object_id == expected_manifest.root_object_id)
        {
            return Err(corruption(
                "zero-length Merkle leaf is not the canonical empty root",
            ));
        }
        let chunk_bytes = proof
            .objects
            .get(leaf.chunk_object_id)
            .ok_or_else(|| corruption("Merkle proof requested chunk is missing"))?;
        let chunk = BlobChunkV1::decode_borrowed(leaf.chunk_object_id, chunk_bytes)?;
        if chunk.len() as u64 != leaf.declared_len
            || *blake3::hash(chunk).as_bytes() != leaf.chunk_digest
        {
            return Err(corruption(
                "Merkle leaf chunk identity or length is invalid",
            ));
        }

        let mut current = DecodedNode::Leaf(leaf)
            .summary(path.leaf_object_id)
            .shifted(path.ordinal_shift)?;
        let mut visited_parents = BTreeSet::new();
        if path.steps.len() > MAX_PROOF_DEPTH {
            return Err(corruption("Merkle proof exceeds the maximum tree depth"));
        }
        for step in path.steps.iter().rev() {
            if !visited_parents.insert(step.parent_object_id) {
                return Err(corruption("Merkle proof contains a parent cycle"));
            }
            let sibling_bytes = proof
                .objects
                .get(step.sibling_object_id)
                .ok_or_else(|| corruption("Merkle proof sibling object is missing"))?;
            let sibling = decode_node(step.sibling_object_id, sibling_bytes)?;
            let sibling_summary = sibling.summary(step.sibling_object_id);
            let local_shift = step
                .sibling_shift
                .checked_sub(step.parent_shift)
                .ok_or_else(|| corruption("Merkle proof sibling shift overflows"))?;
            let sibling_local = sibling_summary.shifted(local_shift)?;
            let current_local = current.shifted(
                step.parent_shift
                    .checked_neg()
                    .ok_or_else(|| corruption("Merkle proof parent shift overflows"))?,
            )?;
            let (left, right) = if step.sibling_is_left {
                (sibling_local, current_local)
            } else {
                (current_local, sibling_local)
            };
            let parent = encode_internal(left, right)?;
            if parent.object_id != step.parent_object_id {
                return Err(corruption("Merkle proof parent binding is invalid"));
            }
            current = NodeSummary {
                object_id: parent.object_id,
                height: parent.value.height,
                first_ordinal: parent.value.first_ordinal,
                leaf_count: parent.value.leaf_count,
                logical_bytes: parent.value.logical_bytes,
            }
            .shifted(step.parent_shift)?;
        }
        if current.object_id != expected_manifest.root_object_id
            || current.first_ordinal != 0
            || current.leaf_count != expected_manifest.leaf_count
            || current.logical_bytes != expected_manifest.logical_bytes
        {
            return Err(corruption(
                "Merkle proof does not terminate at the expected root",
            ));
        }
        if let Some(previous) = verified_root {
            if previous != current {
                return Err(corruption(
                    "Merkle proof paths terminate at different roots",
                ));
            }
        } else {
            verified_root = Some(current);
        }
    }
    let root = verified_root.ok_or_else(|| corruption("Merkle proof has no paths"))?;
    if canonical_blob_id_from_summary(root) != expected_manifest.canonical_blob_id {
        return Err(corruption(
            "Merkle root does not derive the expected BlobId",
        ));
    }
    Ok(())
}

/// Derives the exact successor BlobId by replacing only the requested leaves
/// and reducing each changed leaf through the authenticated sibling summaries.
/// The caller supplies changed chunk bytes, but no caller-supplied identity is
/// accepted; each replacement is encoded and hashed before reduction.
pub(crate) fn derive_blob_merkle_successor_id(
    proof: &BlobMerkleProofV1,
    state_key: &StateKey,
    expected_manifest: BlobManifestV1,
    requested_range: Range<u64>,
    replacements: &BTreeMap<u64, BlobChunkV1>,
) -> Result<BlobId, StorageError> {
    Ok(build_blob_merkle_successor(
        proof,
        state_key,
        expected_manifest,
        requested_range,
        replacements,
    )?
    .manifest
    .canonical_blob_id)
}

/// Builds only changed chunks/leaves and path-copied internal nodes for a
/// fixed-width successor. Unchanged subtrees remain authenticated sibling
/// edges and are never materialized or republished.
pub(crate) fn build_blob_merkle_successor(
    proof: &BlobMerkleProofV1,
    state_key: &StateKey,
    expected_manifest: BlobManifestV1,
    requested_range: Range<u64>,
    replacements: &BTreeMap<u64, BlobChunkV1>,
) -> Result<BlobMerkleTreeBuild, StorageError> {
    verify_blob_merkle_range(proof, state_key, expected_manifest, requested_range.clone())?;
    if replacements.len() != (requested_range.end - requested_range.start) as usize
        || replacements.keys().copied().ne(requested_range.clone())
    {
        return Err(corruption(
            "Merkle successor replacements do not cover the exact requested range",
        ));
    }

    let mut updates = BTreeMap::<ObjectId, NodeSummary>::new();
    let mut objects = ImmutableObjectSet::default();
    for path in &proof.paths {
        let leaf_bytes = proof
            .objects
            .get(path.leaf_object_id)
            .ok_or_else(|| corruption("Merkle successor leaf is missing"))?;
        let old_leaf = decode_leaf(path.leaf_object_id, leaf_bytes)?;
        let replacement = replacements
            .get(&path.leaf_ordinal)
            .ok_or_else(|| corruption("Merkle successor leaf replacement is missing"))?;
        if replacement.bytes.len() as u64 != old_leaf.declared_len {
            return Err(corruption(
                "Merkle successor changes a fixed-width leaf length",
            ));
        }
        let (chunk_object_id, chunk_bytes) = replacement.encode()?;
        objects.insert(chunk_object_id, chunk_bytes)?;
        let new_leaf = BlobMerkleLeafV1 {
            ordinal: old_leaf.ordinal,
            chunk_object_id,
            declared_len: old_leaf.declared_len,
            chunk_digest: *blake3::hash(&replacement.bytes).as_bytes(),
        };
        let (new_leaf_object_id, new_leaf_bytes) = encode_leaf(&new_leaf)?;
        objects.insert(new_leaf_object_id, new_leaf_bytes)?;
        let mut current = NodeSummary {
            object_id: new_leaf_object_id,
            height: 0,
            first_ordinal: new_leaf.ordinal,
            leaf_count: 1,
            logical_bytes: new_leaf.declared_len,
        };
        updates.insert(path.leaf_object_id, current);

        for step in path.steps.iter().rev() {
            let sibling_bytes = proof
                .objects
                .get(step.sibling_object_id)
                .ok_or_else(|| corruption("Merkle successor sibling is missing"))?;
            let sibling = decode_node(step.sibling_object_id, sibling_bytes)?;
            let sibling_summary = updates
                .get(&step.sibling_object_id)
                .copied()
                .unwrap_or_else(|| sibling.summary(step.sibling_object_id));
            let (left, right) = if step.sibling_is_left {
                (sibling_summary, current)
            } else {
                (current, sibling_summary)
            };
            let parent = encode_internal(left, right)?;
            objects.insert(parent.object_id, parent.bytes.clone())?;
            current = NodeSummary {
                object_id: parent.object_id,
                height: parent.value.height,
                first_ordinal: parent.value.first_ordinal,
                leaf_count: parent.value.leaf_count,
                logical_bytes: parent.value.logical_bytes,
            };
            // A path may first produce a partially updated ancestor before a
            // later changed leaf supplies another child of that same node.
            // The ordinally ordered paths deterministically overwrite that
            // intermediate summary; each sibling lookup still consumes the
            // authenticated replacement already present in `updates`.
            updates.insert(step.parent_object_id, current);
        }
    }
    let root = updates
        .get(&expected_manifest.root_object_id)
        .copied()
        .ok_or_else(|| corruption("Merkle successor did not reduce to the base root"))?;
    if root.first_ordinal != 0
        || root.leaf_count != expected_manifest.leaf_count
        || root.logical_bytes != expected_manifest.logical_bytes
    {
        return Err(corruption("Merkle successor root summary is invalid"));
    }
    let manifest = BlobManifestV1::from_merkle_root(
        root.logical_bytes,
        root.leaf_count,
        root.object_id,
        root.height,
    );
    if manifest.canonical_blob_id != canonical_blob_id_from_summary(root) {
        return Err(corruption("Merkle successor identity is inconsistent"));
    }
    Ok(BlobMerkleTreeBuild { manifest, objects })
}

/// Builds a variable-length successor without materializing the base file.
///
/// The existing fixed-chunk tree is treated as a persistent sequence.  The
/// prefix and suffix are recovered from one-leaf authenticated paths; only
/// boundary/insert chunks and the new internal path objects are encoded.  A
/// shifted child summary is an authenticated placement change, not a copied
/// subtree: the child ObjectId and all of its bytes remain unchanged.
pub(crate) fn build_blob_merkle_edit_successor(
    base_manifest: BlobManifestV1,
    prefix_proof: Option<&BlobMerkleProofV1>,
    suffix_proof: Option<&BlobMerkleProofV1>,
    state_key: &StateKey,
    output: &[u8],
    edit: BlobEditSplice,
) -> Result<BlobMerkleTreeBuild, StorageError> {
    let base_len = usize::try_from(base_manifest.logical_bytes)
        .map_err(|_| corruption("Merkle base length exceeds usize"))?;
    let expected_len = base_len
        .checked_sub(edit.delete_len)
        .and_then(|length| length.checked_add(edit.insert_len))
        .ok_or_else(|| corruption("variable blob splice length overflows"))?;
    if edit.base_blob_hash != base_manifest.canonical_blob_id
        || output.len() != expected_len
        || edit.offset > base_len
        || edit.delete_len > base_len.saturating_sub(edit.offset)
    {
        return Err(corruption("variable blob splice is not bound to its base"));
    }
    if let Some(proof) = prefix_proof {
        verify_blob_merkle_range(
            proof,
            state_key,
            base_manifest,
            proof.requested_range.clone(),
        )?;
    }
    if let Some(proof) = suffix_proof {
        verify_blob_merkle_range(
            proof,
            state_key,
            base_manifest,
            proof.requested_range.clone(),
        )?;
    }
    let chunk = CANONICAL_BLOB_CHUNK_BYTES;

    // Empty output has one canonical empty leaf and no old closure to retain.
    if output.is_empty() {
        return build_blob_merkle_tree(&[BlobChunkV1 {
            bytes: Bytes::new(),
        }]);
    }

    let old_end = edit
        .offset
        .checked_add(edit.delete_len)
        .ok_or_else(|| corruption("variable blob splice end overflows"))?;
    let is_append = old_end == base_len && edit.offset == base_len;
    let is_truncate = edit.insert_len == 0 && old_end == base_len;

    // Append and truncate can always preserve the existing fixed geometry,
    // including a short final chunk.  The affected suffix is rebuilt from the
    // submitted bytes; the preceding root is path-copied.
    if is_append || is_truncate {
        if is_append {
            let affected_start = if base_len == 0 {
                0
            } else {
                base_len / chunk * chunk
            };
            let affected_ordinal = (affected_start / chunk) as u64;
            let prefix = if affected_ordinal == 0 {
                None
            } else {
                let proof =
                    prefix_proof.ok_or_else(|| corruption("append prefix proof is missing"))?;
                Some(prefix_root_from_path(proof, None, 0)?)
            };
            let suffix = build_fragment(&output[affected_start..], affected_ordinal)?;
            return join_edit_parts(output.len() as u64, vec![prefix, Some(suffix)]);
        }

        let affected_start = output.len() / chunk * chunk;
        let affected_ordinal = (affected_start / chunk) as u64;
        if affected_ordinal == 0 {
            return build_blob_merkle_tree(&[BlobChunkV1 {
                bytes: Bytes::copy_from_slice(output),
            }]);
        }
        let proof = prefix_proof.ok_or_else(|| corruption("truncate prefix proof is missing"))?;
        let replacement = if output.len() % chunk == 0 {
            None
        } else {
            Some(BlobChunkV1 {
                bytes: Bytes::copy_from_slice(&output[affected_start..]),
            })
        };
        let prefix = prefix_root_from_path(proof, replacement, 0)?;
        let result = join_edit_parts(output.len() as u64, vec![Some(prefix)])?;
        return Ok(result);
    }

    // Reusing fixed chunks across a middle edit is only authenticated when the
    // edit preserves chunk boundaries.  Rejecting other edits is intentional:
    // it prevents the old full-payload staging path from becoming a hidden
    // compatibility fallback.
    if edit.offset % chunk != 0 || edit.delete_len % chunk != 0 || edit.insert_len % chunk != 0 {
        return Err(corruption(
            "unaligned middle blob splice has no authenticated chunk-preserving lowering",
        ));
    }
    let prefix_chunks = edit.offset / chunk;
    let suffix_old_start = old_end;
    let prefix = if prefix_chunks == 0 {
        None
    } else {
        let proof =
            prefix_proof.ok_or_else(|| corruption("middle splice prefix proof is missing"))?;
        Some(prefix_root_from_path(proof, None, 0)?)
    };
    let insert = if edit.insert_len == 0 {
        None
    } else {
        let build = build_fragment(
            &output[edit.offset..edit.offset + edit.insert_len],
            prefix_chunks as u64,
        )?;
        Some(build)
    };
    let suffix = if suffix_old_start == base_len {
        None
    } else {
        let proof =
            suffix_proof.ok_or_else(|| corruption("middle splice suffix proof is missing"))?;
        let root = suffix_root_from_path(proof, None)?;
        let shift = (edit.insert_len as i64 - edit.delete_len as i64) / chunk as i64;
        Some((root.0.shifted(shift)?, root.1))
    };
    let mut parts = Vec::new();
    parts.push(prefix);
    parts.push(insert);
    parts.push(suffix);
    let _ = base_manifest;
    join_edit_parts(output.len() as u64, parts)
}

fn build_fragment(
    bytes: &[u8],
    first_ordinal: u64,
) -> Result<(NodeSummary, ImmutableObjectSet), StorageError> {
    let chunks = if bytes.is_empty() {
        return Err(corruption("empty Merkle fragment is not a tree part"));
    } else {
        bytes
            .chunks(CANONICAL_BLOB_CHUNK_BYTES)
            .map(|bytes| BlobChunkV1 {
                bytes: Bytes::copy_from_slice(bytes),
            })
            .collect::<Vec<_>>()
    };
    let build = build_blob_merkle_tree(&chunks)?;
    let root = NodeSummary {
        object_id: build.manifest.root_object_id,
        height: build.manifest.root_height,
        first_ordinal: first_ordinal,
        leaf_count: build.manifest.leaf_count,
        logical_bytes: build.manifest.logical_bytes,
    };
    let objects = build.objects;
    Ok((root, objects))
}

fn prefix_root_from_path(
    proof: &BlobMerkleProofV1,
    replacement: Option<BlobChunkV1>,
    _shift: i64,
) -> Result<(NodeSummary, ImmutableObjectSet), StorageError> {
    let path = proof
        .paths
        .first()
        .ok_or_else(|| corruption("prefix proof has no path"))?;
    let leaf_bytes = proof
        .objects
        .get(path.leaf_object_id)
        .ok_or_else(|| corruption("prefix proof leaf is missing"))?;
    let old_leaf = decode_leaf(path.leaf_object_id, leaf_bytes)?;
    let mut objects = ImmutableObjectSet::default();
    let mut current = if let Some(replacement) = replacement {
        let (chunk_id, chunk_bytes) = replacement.encode()?;
        objects.insert(chunk_id, chunk_bytes)?;
        let leaf = BlobMerkleLeafV1 {
            ordinal: old_leaf.ordinal,
            chunk_object_id: chunk_id,
            declared_len: replacement.bytes.len() as u64,
            chunk_digest: *blake3::hash(&replacement.bytes).as_bytes(),
        };
        let (leaf_id, leaf_bytes) = encode_leaf(&leaf)?;
        objects.insert(leaf_id, leaf_bytes)?;
        NodeSummary {
            object_id: leaf_id,
            height: 0,
            first_ordinal: leaf.ordinal,
            leaf_count: 1,
            logical_bytes: leaf.declared_len,
        }
    } else {
        DecodedNode::Leaf(old_leaf).summary(path.leaf_object_id)
    };
    let mut removed_leaves = 0_i64;
    for step in path.steps.iter().rev() {
        let sibling_bytes = proof
            .objects
            .get(step.sibling_object_id)
            .ok_or_else(|| corruption("prefix proof sibling is missing"))?;
        let sibling =
            decode_node(step.sibling_object_id, sibling_bytes)?.summary(step.sibling_object_id);
        if step.sibling_is_left {
            let encoded = encode_internal(sibling, current)?;
            objects.insert(encoded.object_id, encoded.bytes.clone())?;
            current = encoded_summary(encoded);
        } else {
            removed_leaves = removed_leaves.saturating_add(sibling.leaf_count as i64);
        }
    }
    let _ = removed_leaves;
    Ok((current, objects))
}

fn suffix_root_from_path(
    proof: &BlobMerkleProofV1,
    replacement: Option<BlobChunkV1>,
) -> Result<(NodeSummary, ImmutableObjectSet), StorageError> {
    let path = proof
        .paths
        .first()
        .ok_or_else(|| corruption("suffix proof has no path"))?;
    let leaf_bytes = proof
        .objects
        .get(path.leaf_object_id)
        .ok_or_else(|| corruption("suffix proof leaf is missing"))?;
    let old_leaf = decode_leaf(path.leaf_object_id, leaf_bytes)?;
    let mut current = if let Some(replacement) = replacement {
        let (chunk_id, _) = replacement.encode()?;
        let leaf = BlobMerkleLeafV1 {
            ordinal: old_leaf.ordinal,
            chunk_object_id: chunk_id,
            declared_len: replacement.bytes.len() as u64,
            chunk_digest: *blake3::hash(&replacement.bytes).as_bytes(),
        };
        let (leaf_id, _) = encode_leaf(&leaf)?;
        NodeSummary {
            object_id: leaf_id,
            height: 0,
            first_ordinal: leaf.ordinal,
            leaf_count: 1,
            logical_bytes: leaf.declared_len,
        }
    } else {
        DecodedNode::Leaf(old_leaf).summary(path.leaf_object_id)
    };
    let mut objects = ImmutableObjectSet::default();
    for step in path.steps.iter().rev() {
        if step.sibling_is_left {
            continue;
        }
        let sibling_bytes = proof
            .objects
            .get(step.sibling_object_id)
            .ok_or_else(|| corruption("suffix proof sibling is missing"))?;
        let sibling =
            decode_node(step.sibling_object_id, sibling_bytes)?.summary(step.sibling_object_id);
        let encoded = encode_internal(current, sibling)?;
        objects.insert(encoded.object_id, encoded.bytes.clone())?;
        current = encoded_summary(encoded);
    }
    Ok((current, objects))
}

fn encoded_summary(value: EncodedInternal) -> NodeSummary {
    NodeSummary {
        object_id: value.object_id,
        height: value.value.height,
        first_ordinal: value.value.first_ordinal,
        leaf_count: value.value.leaf_count,
        logical_bytes: value.value.logical_bytes,
    }
}

fn join_edit_parts(
    logical_bytes: u64,
    parts: Vec<Option<(NodeSummary, ImmutableObjectSet)>>,
) -> Result<BlobMerkleTreeBuild, StorageError> {
    let mut parts = parts.into_iter().flatten();
    let (mut root, mut objects) = parts
        .next()
        .ok_or_else(|| corruption("variable blob splice produced no tree parts"))?;
    // A deletion/replacement beginning at byte zero can retain a suffix
    // subtree as the complete successor. Its authenticated raw coordinates
    // still begin at the old ordinal; the manifest's implicit root coordinate
    // is zero, so carry the placement as proof metadata rather than rewriting
    // the retained subtree.
    if root.first_ordinal != 0 {
        let shift = i64::try_from(root.first_ordinal)
            .ok()
            .and_then(i64::checked_neg)
            .ok_or_else(|| corruption("variable blob splice root shift overflows"))?;
        root = root.shifted(shift)?;
    }
    for (part, part_objects) in parts {
        objects.extend(part_objects)?;
        let encoded = encode_internal(root, part)?;
        objects.insert(encoded.object_id, encoded.bytes.clone())?;
        root = encoded_summary(encoded);
    }
    let expected_count = logical_bytes
        .div_ceil(CANONICAL_BLOB_CHUNK_BYTES as u64)
        .max(1);
    if root.first_ordinal != 0
        || root.logical_bytes != logical_bytes
        || root.leaf_count != expected_count
    {
        return Err(corruption("variable blob splice root geometry is invalid"));
    }
    let manifest = BlobManifestV1::from_merkle_root(
        logical_bytes,
        root.leaf_count,
        root.object_id,
        root.height,
    );
    Ok(BlobMerkleTreeBuild { manifest, objects })
}

fn validate_fixed_chunk_layout(
    logical_bytes: u64,
    chunks: &[BlobChunkV1],
) -> Result<(), StorageError> {
    let expected_count = logical_bytes
        .div_ceil(CANONICAL_BLOB_CHUNK_BYTES as u64)
        .max(1);
    if expected_count != chunks.len() as u64 {
        return Err(corruption("Merkle leaves are not canonical fixed chunks"));
    }
    for (index, chunk) in chunks.iter().enumerate() {
        let expected = if index + 1 == chunks.len() {
            logical_bytes - (index as u64 * CANONICAL_BLOB_CHUNK_BYTES as u64)
        } else {
            CANONICAL_BLOB_CHUNK_BYTES as u64
        };
        if chunk.bytes.len() as u64 != expected {
            return Err(corruption(
                "Merkle chunk length is not canonically positioned",
            ));
        }
    }
    Ok(())
}

fn canonical_blob_id_from_summary(summary: NodeSummary) -> BlobId {
    canonical_merkle_blob_id(
        summary.object_id,
        summary.logical_bytes,
        summary.leaf_count,
        summary.height,
        BLOB_MERKLE_CHUNK_BYTES,
    )
}

fn build_node(
    leaves: &[LeafWithId],
    objects: &mut ImmutableObjectSet,
) -> Result<NodeSummary, StorageError> {
    if leaves.len() == 1 {
        let leaf = &leaves[0].value;
        return Ok(NodeSummary {
            object_id: leaves[0].object_id,
            height: 0,
            first_ordinal: leaf.ordinal,
            leaf_count: 1,
            logical_bytes: leaf.declared_len,
        });
    }
    let split = leaves.len().div_ceil(2);
    let left = build_node(&leaves[..split], objects)?;
    let right = build_node(&leaves[split..], objects)?;
    let parent = encode_internal(left, right)?;
    objects.insert(parent.object_id, parent.bytes)?;
    Ok(NodeSummary {
        object_id: parent.object_id,
        height: parent.value.height,
        first_ordinal: parent.value.first_ordinal,
        leaf_count: parent.value.leaf_count,
        logical_bytes: parent.value.logical_bytes,
    })
}

struct EncodedInternal {
    object_id: ObjectId,
    bytes: Bytes,
    value: BlobMerkleInternalV1,
}

fn encode_internal(left: NodeSummary, right: NodeSummary) -> Result<EncodedInternal, StorageError> {
    if left.object_id == ObjectId::ZERO
        || right.object_id == ObjectId::ZERO
        || left.first_ordinal.checked_add(left.leaf_count) != Some(right.first_ordinal)
    {
        return Err(corruption("Merkle children are not adjacent"));
    }
    let height = left.height.max(right.height).saturating_add(1);
    let value = BlobMerkleInternalV1 {
        height,
        first_ordinal: left.first_ordinal,
        leaf_count: left.leaf_count + right.leaf_count,
        logical_bytes: left.logical_bytes + right.logical_bytes,
        left: left.as_ref(),
        right: right.as_ref(),
    };
    validate_internal(&value)?;
    let (object_id, bytes) = encode_object(ObjectDomain::BlobMerkleInternalV1, |encoder| {
        encode_internal_value(encoder, &value)
    })?;
    Ok(EncodedInternal {
        object_id,
        bytes,
        value,
    })
}

fn encode_leaf(value: &BlobMerkleLeafV1) -> Result<(ObjectId, Bytes), StorageError> {
    // The canonical empty blob is represented by one ordinal-zero leaf whose
    // authenticated chunk is empty. Any later zero-length leaf is impossible
    // under the fixed geometry and is rejected here; root verification binds
    // the sole exception to logical_bytes=0 and leaf_count=1.
    if value.chunk_object_id == ObjectId::ZERO || (value.declared_len == 0 && value.ordinal != 0) {
        return Err(corruption("Merkle leaf has an invalid child"));
    }
    encode_object(ObjectDomain::BlobMerkleLeafV1, |encoder| {
        encoder.u64(value.ordinal);
        encode_id(encoder, value.chunk_object_id);
        encoder.u64(value.declared_len);
        encoder.fixed(&value.chunk_digest);
        Ok(())
    })
}

fn decode_leaf(id: ObjectId, bytes: &[u8]) -> Result<BlobMerkleLeafV1, StorageError> {
    let mut decoder = decode_object(id, ObjectDomain::BlobMerkleLeafV1, bytes)?;
    let value = BlobMerkleLeafV1 {
        ordinal: decoder.u64()?,
        chunk_object_id: decode_id(&mut decoder)?,
        declared_len: decoder.u64()?,
        chunk_digest: decoder.fixed()?,
    };
    decoder.finish()?;
    if value.chunk_object_id == ObjectId::ZERO || (value.declared_len == 0 && value.ordinal != 0) {
        return Err(corruption("Merkle leaf has an invalid child"));
    }
    Ok(value)
}

fn encode_internal_value(
    encoder: &mut Encoder,
    value: &BlobMerkleInternalV1,
) -> Result<(), StorageError> {
    encoder.u32(value.height);
    encoder.u64(value.first_ordinal);
    encoder.u64(value.leaf_count);
    encoder.u64(value.logical_bytes);
    encode_node_ref(encoder, value.left);
    encode_node_ref(encoder, value.right);
    Ok(())
}

fn encode_node_ref(encoder: &mut Encoder, value: BlobMerkleNodeRefV1) {
    encode_id(encoder, value.object_id);
    encoder.u32(value.height);
    encoder.u64(value.first_ordinal);
    encoder.u64(value.leaf_count);
    encoder.u64(value.logical_bytes);
}

fn decode_node_ref(
    decoder: &mut super::codec::Decoder<'_>,
) -> Result<BlobMerkleNodeRefV1, StorageError> {
    Ok(BlobMerkleNodeRefV1 {
        object_id: decode_id(decoder)?,
        height: decoder.u32()?,
        first_ordinal: decoder.u64()?,
        leaf_count: decoder.u64()?,
        logical_bytes: decoder.u64()?,
    })
}

fn decode_internal(id: ObjectId, bytes: &[u8]) -> Result<BlobMerkleInternalV1, StorageError> {
    let mut decoder = decode_object(id, ObjectDomain::BlobMerkleInternalV1, bytes)?;
    let value = BlobMerkleInternalV1 {
        height: decoder.u32()?,
        first_ordinal: decoder.u64()?,
        leaf_count: decoder.u64()?,
        logical_bytes: decoder.u64()?,
        left: decode_node_ref(&mut decoder)?,
        right: decode_node_ref(&mut decoder)?,
    };
    decoder.finish()?;
    validate_internal(&value)?;
    Ok(value)
}

fn validate_internal(value: &BlobMerkleInternalV1) -> Result<(), StorageError> {
    let left_end = value
        .left
        .first_ordinal
        .checked_add(value.left.leaf_count)
        .ok_or_else(|| corruption("Merkle left ordinal overflows"))?;
    if value.height == 0
        || value.left.object_id == ObjectId::ZERO
        || value.right.object_id == ObjectId::ZERO
        || value.left.leaf_count == 0
        || value.right.leaf_count == 0
        || left_end != value.right.first_ordinal
        || value.first_ordinal != value.left.first_ordinal
        || value.leaf_count
            != value
                .left
                .leaf_count
                .checked_add(value.right.leaf_count)
                .ok_or_else(|| corruption("Merkle leaf count overflows"))?
        || value.logical_bytes
            != value
                .left
                .logical_bytes
                .checked_add(value.right.logical_bytes)
                .ok_or_else(|| corruption("Merkle logical length overflows"))?
        || value.height != value.left.height.max(value.right.height) + 1
    {
        return Err(corruption(
            "Merkle internal child binding or summary is invalid",
        ));
    }
    Ok(())
}

fn decode_node(id: ObjectId, bytes: &[u8]) -> Result<DecodedNode, StorageError> {
    match authenticate_object_domain(id, bytes)? {
        ObjectDomain::BlobMerkleLeafV1 => decode_leaf(id, bytes).map(DecodedNode::Leaf),
        ObjectDomain::BlobMerkleInternalV1 => decode_internal(id, bytes).map(DecodedNode::Internal),
        _ => Err(corruption("Merkle proof references a non-Merkle object")),
    }
}

/// Returns the authenticated outgoing object edges for GC. This deliberately
/// shares the exact decoder used by proofs so reachability cannot accept a
/// Merkle node shape that the range verifier would reject.
pub(super) fn authenticated_merkle_edges(
    id: ObjectId,
    bytes: &[u8],
) -> Result<Vec<(ObjectId, ObjectDomain)>, StorageError> {
    match decode_node(id, bytes)? {
        DecodedNode::Leaf(leaf) => Ok(vec![(leaf.chunk_object_id, ObjectDomain::BlobChunk)]),
        DecodedNode::Internal(value) => Ok(vec![
            (
                value.left.object_id,
                if value.left.height == 0 {
                    ObjectDomain::BlobMerkleLeafV1
                } else {
                    ObjectDomain::BlobMerkleInternalV1
                },
            ),
            (
                value.right.object_id,
                if value.right.height == 0 {
                    ObjectDomain::BlobMerkleLeafV1
                } else {
                    ObjectDomain::BlobMerkleInternalV1
                },
            ),
        ]),
    }
}

fn collect_path(
    objects: &ImmutableObjectSet,
    node_id: ObjectId,
    target_ordinal: u64,
    proof_objects: &mut ImmutableObjectSet,
    steps: &mut Vec<BlobMerkleProofStepV1>,
    expected_ref: BlobMerkleNodeRefV1,
    leaf_shift: &mut i64,
) -> Result<(), StorageError> {
    let bytes = objects
        .get(node_id)
        .ok_or_else(|| corruption("Merkle tree node is missing during proof build"))?;
    let node = decode_node(node_id, bytes)?;
    let raw_summary = node.summary(node_id);
    let ordinal_shift = i64::try_from(expected_ref.first_ordinal)
        .ok()
        .and_then(|expected| {
            i64::try_from(raw_summary.first_ordinal)
                .ok()
                .and_then(|raw| expected.checked_sub(raw))
        })
        .ok_or_else(|| corruption("Merkle ordinal shift exceeds i64"))?;
    if raw_summary.shifted(ordinal_shift)?.as_ref() != expected_ref {
        return Err(corruption("Merkle tree node summary is not authenticated"));
    }
    match node {
        DecodedNode::Leaf(leaf) => {
            if leaf.ordinal.checked_add_signed(ordinal_shift) != Some(target_ordinal) {
                return Err(corruption("Merkle tree leaf ordinal is not canonical"));
            }
            *leaf_shift = ordinal_shift;
            Ok(())
        }
        DecodedNode::Internal(internal) => {
            let left = shift_ref(internal.left, ordinal_shift)?;
            let right = shift_ref(internal.right, ordinal_shift)?;
            let in_left = target_ordinal >= left.first_ordinal
                && target_ordinal < left.first_ordinal + left.leaf_count;
            let (child, sibling, sibling_is_left) = if in_left {
                (left, right, false)
            } else {
                (right, left, true)
            };
            let sibling_bytes = objects
                .get(sibling.object_id)
                .ok_or_else(|| corruption("Merkle sibling is missing during proof build"))?;
            proof_objects.insert(sibling.object_id, sibling_bytes.clone())?;
            let sibling_node = decode_node(sibling.object_id, sibling_bytes)?;
            let raw_sibling = sibling_node.summary(sibling.object_id);
            let sibling_shift = i64::try_from(sibling.first_ordinal)
                .ok()
                .and_then(|expected| {
                    i64::try_from(raw_sibling.first_ordinal)
                        .ok()
                        .and_then(|raw| expected.checked_sub(raw))
                })
                .ok_or_else(|| corruption("Merkle sibling shift exceeds i64"))?;
            if raw_sibling.shifted(sibling_shift)?.as_ref() != sibling {
                return Err(corruption("Merkle sibling summary is not authenticated"));
            }
            let child_bytes = objects
                .get(child.object_id)
                .ok_or_else(|| corruption("Merkle child is missing during proof build"))?;
            let child_node = decode_node(child.object_id, child_bytes)?;
            let raw_child = child_node.summary(child.object_id);
            let parent_shift = if sibling_is_left {
                infer_parent_shift(internal, raw_sibling, raw_child, ordinal_shift)
            } else {
                infer_parent_shift(internal, raw_child, raw_sibling, ordinal_shift)
            };
            steps.push(BlobMerkleProofStepV1 {
                parent_object_id: node_id,
                sibling_object_id: sibling.object_id,
                sibling_is_left,
                sibling_shift,
                parent_shift,
            });
            collect_path(
                objects,
                child.object_id,
                target_ordinal,
                proof_objects,
                steps,
                child,
                leaf_shift,
            )
        }
    }
}

fn leaf_id_from_path(
    objects: &ImmutableObjectSet,
    node_id: ObjectId,
    target_ordinal: u64,
    expected_ref: BlobMerkleNodeRefV1,
) -> Result<ObjectId, StorageError> {
    let bytes = objects
        .get(node_id)
        .ok_or_else(|| corruption("Merkle root is missing during leaf lookup"))?;
    let node = decode_node(node_id, bytes)?;
    let raw_summary = node.summary(node_id);
    let ordinal_shift = i64::try_from(expected_ref.first_ordinal)
        .ok()
        .and_then(|expected| {
            i64::try_from(raw_summary.first_ordinal)
                .ok()
                .and_then(|raw| expected.checked_sub(raw))
        })
        .ok_or_else(|| corruption("Merkle ordinal shift exceeds i64"))?;
    if raw_summary.shifted(ordinal_shift)?.as_ref() != expected_ref {
        return Err(corruption("Merkle tree node summary is not authenticated"));
    }
    match node {
        DecodedNode::Leaf(leaf)
            if leaf.ordinal.checked_add_signed(ordinal_shift) == Some(target_ordinal) =>
        {
            Ok(node_id)
        }
        DecodedNode::Leaf(_) => Err(corruption("Merkle leaf ordinal is not canonical")),
        DecodedNode::Internal(internal) => {
            let left = shift_ref(internal.left, ordinal_shift)?;
            let right = shift_ref(internal.right, ordinal_shift)?;
            let child = if target_ordinal >= left.first_ordinal
                && target_ordinal < left.first_ordinal + left.leaf_count
            {
                left
            } else {
                right
            };
            leaf_id_from_path(objects, child.object_id, target_ordinal, child)
        }
    }
}

fn validate_requested_range(
    manifest: &BlobManifestV1,
    requested_range: &Range<u64>,
) -> Result<(), StorageError> {
    if requested_range.start >= requested_range.end
        || requested_range.end > manifest.leaf_count
        || manifest.root_object_id == ObjectId::ZERO
        || manifest.leaf_count == 0
    {
        return Err(corruption("Merkle requested range or manifest is invalid"));
    }
    Ok(())
}

fn state_binding(state_key: &StateKey, manifest: &BlobManifestV1) -> [u8; 32] {
    let mut bytes = encode_state_key(StateKeyRef {
        schema_key: &state_key.schema_key,
        file_id: state_key.file_id.as_deref(),
        entity_pk: &state_key.entity_pk,
    });
    bytes.extend_from_slice(manifest.root_object_id.as_bytes());
    bytes.extend_from_slice(manifest.canonical_blob_id.as_bytes());
    bytes.extend_from_slice(&manifest.logical_bytes.to_be_bytes());
    bytes.extend_from_slice(&manifest.leaf_count.to_be_bytes());
    keyed_hash(MERKLE_STATE_BINDING_DOMAIN, &bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_pk::EntityPk;

    fn state_key() -> StateKey {
        StateKey {
            schema_key: "lix_file".to_owned(),
            file_id: Some("README.md".to_owned()),
            entity_pk: EntityPk::single("row-7"),
        }
    }

    fn chunks(count: usize) -> Vec<BlobChunkV1> {
        (0..count)
            .map(|ordinal| BlobChunkV1 {
                bytes: Bytes::from(vec![ordinal as u8 + 1; CANONICAL_BLOB_CHUNK_BYTES]),
            })
            .collect()
    }

    fn assert_edit_materializes(
        expected: &[u8],
        build: BlobMerkleTreeBuild,
        base: &BlobMerkleTreeBuild,
    ) {
        let key = state_key();
        let mut objects = base.objects.clone();
        objects.extend(build.objects.clone()).unwrap();
        let combined = BlobMerkleTreeBuild {
            manifest: build.manifest,
            objects,
        };
        let proof =
            prove_blob_merkle_range(&combined, &key, 0..combined.manifest.leaf_count).unwrap();
        assert_eq!(
            materialize_blob_merkle_range(
                &proof,
                &key,
                combined.manifest,
                0..expected.len() as u64,
            )
            .unwrap(),
            expected
        );
    }

    #[test]
    fn empty_blob_has_one_canonical_authenticated_leaf() {
        let build = build_blob_merkle_tree(&[BlobChunkV1 {
            bytes: Bytes::new(),
        }])
        .unwrap();
        assert_eq!(build.manifest.logical_bytes, 0);
        assert_eq!(build.manifest.leaf_count, 1);
        assert_eq!(build.manifest.root_height, 0);
        let (manifest_id, manifest_bytes) = build.manifest.encode().unwrap();
        assert_eq!(
            BlobManifestV1::decode(manifest_id, &manifest_bytes).unwrap(),
            build.manifest,
            "cold manifest decode must preserve the sole empty geometry"
        );
        assert_eq!(
            build.manifest.canonical_blob_id,
            canonical_blob_id_for_content(b"").unwrap()
        );
        let key = state_key();
        let proof = prove_blob_merkle_range(&build, &key, 0..1).unwrap();
        verify_blob_merkle_range(&proof, &key, build.manifest, 0..1).unwrap();
        assert_eq!(
            materialize_blob_merkle_range(&proof, &key, build.manifest, 0..0).unwrap(),
            Vec::<u8>::new()
        );

        assert!(
            build_blob_merkle_tree(&[
                BlobChunkV1 {
                    bytes: Bytes::new(),
                },
                BlobChunkV1 {
                    bytes: Bytes::new(),
                },
            ])
            .is_err(),
            "multiple empty leaves must not encode an alternate empty blob"
        );
        let nonempty = build_blob_merkle_tree(&[BlobChunkV1 {
            bytes: Bytes::from_static(b"x"),
        }])
        .unwrap();
        let mut substituted = nonempty.clone();
        substituted.manifest =
            BlobManifestV1::from_merkle_root(0, 1, nonempty.manifest.root_object_id, 0);
        assert!(
            prove_blob_merkle_range(&substituted, &key, 0..1).is_err(),
            "a non-empty leaf cannot substitute for the canonical empty root"
        );
    }

    #[test]
    fn range_proof_authenticates_exact_leaves_and_state() {
        let chunks = chunks(8);
        let build = build_blob_merkle_tree(&chunks).unwrap();
        let key = state_key();
        let proof = prove_blob_merkle_range(&build, &key, 3..5).unwrap();
        verify_blob_merkle_range(&proof, &key, build.manifest, 3..5).unwrap();
        assert!(proof.object_count() < build.objects.iter().count());

        let wrong_key = StateKey {
            file_id: Some("other.md".to_owned()),
            ..key.clone()
        };
        assert!(verify_blob_merkle_range(&proof, &wrong_key, build.manifest, 3..5).is_err());
        assert!(verify_blob_merkle_range(&proof, &key, build.manifest, 2..5).is_err());
    }

    #[test]
    fn proof_rejects_leaf_substitution_and_path_corruption() {
        let chunks = chunks(4);
        let build = build_blob_merkle_tree(&chunks).unwrap();
        let key = state_key();
        let mut proof = prove_blob_merkle_range(&build, &key, 1..2).unwrap();
        let path = &proof.paths[0];
        let leaf = decode_leaf(
            path.leaf_object_id,
            proof.objects.get(path.leaf_object_id).unwrap(),
        )
        .unwrap();
        let replacement = BlobChunkV1 {
            bytes: Bytes::from(vec![0xff; CANONICAL_BLOB_CHUNK_BYTES]),
        };
        let (replacement_id, _) = replacement.encode().unwrap();
        let substituted_leaf = BlobMerkleLeafV1 {
            ordinal: leaf.ordinal,
            chunk_object_id: replacement_id,
            declared_len: leaf.declared_len,
            chunk_digest: *blake3::hash(&replacement.bytes).as_bytes(),
        };
        let (substituted_leaf_id, substituted_leaf_bytes) = encode_leaf(&substituted_leaf).unwrap();
        proof
            .objects
            .insert(substituted_leaf_id, substituted_leaf_bytes)
            .unwrap();
        proof.paths[0].leaf_object_id = substituted_leaf_id;
        assert_ne!(replacement_id, leaf.chunk_object_id);
        assert!(verify_blob_merkle_range(&proof, &key, build.manifest, 1..2).is_err());

        let mut proof = prove_blob_merkle_range(&build, &key, 1..2).unwrap();
        proof.paths[0].steps[0].parent_object_id = ObjectId::ZERO;
        assert!(verify_blob_merkle_range(&proof, &key, build.manifest, 1..2).is_err());
    }

    #[test]
    fn canonical_merkle_blob_id_is_root_derived() {
        let chunks = chunks(2);
        let build = build_blob_merkle_tree(&chunks).unwrap();
        assert_eq!(
            build.manifest.canonical_blob_id,
            canonical_blob_id_from_summary(
                decode_node(
                    build.manifest.root_object_id,
                    build.objects.get(build.manifest.root_object_id).unwrap(),
                )
                .unwrap()
                .summary(build.manifest.root_object_id),
            )
        );
        assert_ne!(build.manifest.root_object_id, ObjectId::ZERO);
        assert!(build.objects.iter().any(|(id, _)| {
            matches!(
                authenticate_object_domain(id, build.objects.get(id).unwrap()),
                Ok(ObjectDomain::BlobMerkleInternalV1)
            )
        }));
    }

    #[test]
    fn changed_leaves_and_sibling_proof_derive_exact_successor_blob_id() {
        let base_chunks = chunks(4);
        let base = build_blob_merkle_tree(&base_chunks).unwrap();
        let key = state_key();
        let proof = prove_blob_merkle_range(&base, &key, 1..3).unwrap();
        let mut replacements = BTreeMap::new();
        replacements.insert(
            1,
            BlobChunkV1 {
                bytes: Bytes::from(vec![0xa1; CANONICAL_BLOB_CHUNK_BYTES]),
            },
        );
        replacements.insert(
            2,
            BlobChunkV1 {
                bytes: Bytes::from(vec![0xa2; CANONICAL_BLOB_CHUNK_BYTES]),
            },
        );
        let derived =
            derive_blob_merkle_successor_id(&proof, &key, base.manifest, 1..3, &replacements)
                .unwrap();
        let mut full_successor = base_chunks;
        full_successor[1] = replacements.get(&1).unwrap().clone();
        full_successor[2] = replacements.get(&2).unwrap().clone();
        let expected = build_blob_merkle_tree(&full_successor).unwrap();
        assert_eq!(derived, expected.manifest.canonical_blob_id);
        assert_ne!(derived, base.manifest.canonical_blob_id);
    }

    #[test]
    fn malformed_shape_and_length_fail_closed() {
        let mut invalid = chunks(2);
        invalid[0].bytes = Bytes::from(vec![1; CANONICAL_BLOB_CHUNK_BYTES - 1]);
        assert!(build_blob_merkle_tree(&invalid).is_err());

        let base_chunks = chunks(4);
        let base = build_blob_merkle_tree(&base_chunks).unwrap();
        let key = state_key();
        let proof = prove_blob_merkle_range(&base, &key, 1..2).unwrap();
        let mut replacements = BTreeMap::new();
        replacements.insert(
            1,
            BlobChunkV1 {
                bytes: Bytes::from(vec![0xaa; CANONICAL_BLOB_CHUNK_BYTES - 1]),
            },
        );
        assert!(
            derive_blob_merkle_successor_id(&proof, &key, base.manifest, 1..2, &replacements,)
                .is_err()
        );
    }

    #[test]
    fn manifest_root_round_trip_and_cold_root_authentication() {
        let base = build_blob_merkle_tree(&chunks(4)).unwrap();
        let (manifest_id, manifest_bytes) = base.manifest.encode().unwrap();
        let decoded = BlobManifestV1::decode(manifest_id, &manifest_bytes).unwrap();
        assert_eq!(decoded, base.manifest);

        let root_bytes = base.objects.get(base.manifest.root_object_id).unwrap();
        let root_edges =
            authenticated_merkle_edges(base.manifest.root_object_id, root_bytes).unwrap();
        assert_eq!(root_edges.len(), 2);
        assert!(root_edges.iter().all(|(_, domain)| {
            matches!(
                domain,
                ObjectDomain::BlobMerkleLeafV1 | ObjectDomain::BlobMerkleInternalV1
            )
        }));

        let key = state_key();
        let proof = prove_blob_merkle_range(&base, &key, 1..3).unwrap();
        verify_blob_merkle_range(&proof, &key, decoded, 1..3).unwrap();
    }

    #[test]
    fn manifest_rejects_missing_wrong_domain_geometry_and_root_substitution() {
        let base = build_blob_merkle_tree(&chunks(2)).unwrap();
        let (manifest_id, manifest_bytes) = base.manifest.encode().unwrap();
        assert!(BlobManifestV1::decode(manifest_id, &[]).is_err());
        assert!(
            BlobManifestV1::decode(
                manifest_id,
                base.objects.get(base.manifest.root_object_id).unwrap(),
            )
            .is_err()
        );

        let mut wrong_geometry = base.manifest;
        wrong_geometry.chunk_bytes += 1;
        assert!(wrong_geometry.encode().is_err());

        let mut missing_root = base.manifest;
        missing_root.root_object_id = ObjectId::ZERO;
        assert!(missing_root.encode().is_err());

        let mut substituted_root = base.manifest;
        substituted_root.root_object_id = ObjectId::from_bytes([0xabu8; 32]);
        assert!(substituted_root.encode().is_err());

        let mut substituted_blob_id = base.manifest;
        substituted_blob_id.canonical_blob_id = BlobId::from_bytes([0xcdu8; 32]);
        assert!(substituted_blob_id.encode().is_err());
        assert!(BlobManifestV1::decode(manifest_id, &manifest_bytes).is_ok());
    }

    #[test]
    fn proof_rejects_missing_wrong_domain_and_cycle_edges() {
        let base = build_blob_merkle_tree(&chunks(4)).unwrap();
        let key = state_key();

        let mut missing = prove_blob_merkle_range(&base, &key, 1..2).unwrap();
        missing.paths[0].steps[0].sibling_object_id = ObjectId::ZERO;
        assert!(verify_blob_merkle_range(&missing, &key, base.manifest, 1..2).is_err());

        let mut wrong_domain = prove_blob_merkle_range(&base, &key, 1..2).unwrap();
        let leaf = decode_leaf(
            wrong_domain.paths[0].leaf_object_id,
            wrong_domain
                .objects
                .get(wrong_domain.paths[0].leaf_object_id)
                .unwrap(),
        )
        .unwrap();
        wrong_domain.paths[0].steps[0].sibling_object_id = leaf.chunk_object_id;
        assert!(verify_blob_merkle_range(&wrong_domain, &key, base.manifest, 1..2).is_err());

        let mut cycle = prove_blob_merkle_range(&base, &key, 1..2).unwrap();
        cycle.paths[0].steps[1].parent_object_id = cycle.paths[0].steps[0].parent_object_id;
        assert!(verify_blob_merkle_range(&cycle, &key, base.manifest, 1..2).is_err());
    }

    #[test]
    fn final_reference_root_substitution_fails_before_range_acceptance() {
        let base = build_blob_merkle_tree(&chunks(4)).unwrap();
        let key = state_key();
        let proof = prove_blob_merkle_range(&base, &key, 1..2).unwrap();
        let mut wrong_manifest = base.manifest;
        wrong_manifest.root_object_id = ObjectId::from_bytes([0xefu8; 32]);
        assert!(verify_blob_merkle_range(&proof, &key, wrong_manifest, 1..2).is_err());
    }

    #[test]
    fn variable_append_and_aligned_middle_edit_reuse_authenticated_subtrees() {
        let base = build_blob_merkle_tree(&chunks(4)).unwrap();
        let key = state_key();
        let mut base_bytes = Vec::new();
        for chunk in chunks(4) {
            base_bytes.extend_from_slice(&chunk.bytes);
        }

        let suffix = vec![0x55; CANONICAL_BLOB_CHUNK_BYTES];
        let mut appended = base_bytes.clone();
        appended.extend_from_slice(&suffix);
        let append_proof = prove_blob_merkle_range(&base, &key, 3..4).unwrap();
        let append = build_blob_merkle_edit_successor(
            base.manifest,
            Some(&append_proof),
            None,
            &key,
            &appended,
            BlobEditSplice {
                base_blob_hash: base.manifest.canonical_blob_id,
                offset: base_bytes.len(),
                delete_len: 0,
                insert_len: suffix.len(),
            },
        )
        .unwrap();
        assert_edit_materializes(&appended, append, &base);

        let truncated_len = CANONICAL_BLOB_CHUNK_BYTES * 2 + 123;
        let truncated = base_bytes[..truncated_len].to_vec();
        let truncate_proof = prove_blob_merkle_range(&base, &key, 2..3).unwrap();
        let truncate = build_blob_merkle_edit_successor(
            base.manifest,
            Some(&truncate_proof),
            None,
            &key,
            &truncated,
            BlobEditSplice {
                base_blob_hash: base.manifest.canonical_blob_id,
                offset: truncated_len,
                delete_len: base_bytes.len() - truncated_len,
                insert_len: 0,
            },
        )
        .unwrap();
        assert_edit_materializes(&truncated, truncate, &base);

        let inserted = vec![0x77; CANONICAL_BLOB_CHUNK_BYTES * 2];
        let mut middle = base_bytes[..CANONICAL_BLOB_CHUNK_BYTES].to_vec();
        middle.extend_from_slice(&inserted);
        middle.extend_from_slice(&base_bytes[CANONICAL_BLOB_CHUNK_BYTES * 2..]);
        let prefix_proof = prove_blob_merkle_range(&base, &key, 0..1).unwrap();
        let suffix_proof = prove_blob_merkle_range(&base, &key, 2..3).unwrap();
        let middle_build = build_blob_merkle_edit_successor(
            base.manifest,
            Some(&prefix_proof),
            Some(&suffix_proof),
            &key,
            &middle,
            BlobEditSplice {
                base_blob_hash: base.manifest.canonical_blob_id,
                offset: CANONICAL_BLOB_CHUNK_BYTES,
                delete_len: CANONICAL_BLOB_CHUNK_BYTES,
                insert_len: inserted.len(),
            },
        )
        .unwrap();
        assert_edit_materializes(&middle, middle_build, &base);

        let suffix_only = base_bytes[CANONICAL_BLOB_CHUNK_BYTES * 2..].to_vec();
        let suffix_proof = prove_blob_merkle_range(&base, &key, 2..3).unwrap();
        let suffix_delete = build_blob_merkle_edit_successor(
            base.manifest,
            None,
            Some(&suffix_proof),
            &key,
            &suffix_only,
            BlobEditSplice {
                base_blob_hash: base.manifest.canonical_blob_id,
                offset: 0,
                delete_len: CANONICAL_BLOB_CHUNK_BYTES * 2,
                insert_len: 0,
            },
        )
        .unwrap();
        assert_edit_materializes(&suffix_only, suffix_delete, &base);

        let unaligned = BlobEditSplice {
            base_blob_hash: base.manifest.canonical_blob_id,
            offset: 1,
            delete_len: 1,
            insert_len: 2,
        };
        assert!(
            build_blob_merkle_edit_successor(
                base.manifest,
                None,
                None,
                &key,
                &base_bytes,
                unaligned,
            )
            .is_err()
        );
    }
}
