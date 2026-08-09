use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use bytes::Bytes;

use crate::binary_cas::BlobId;
use crate::storage::StorageError;

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
    steps: Vec<BlobMerkleProofStepV1>,
}

#[derive(Clone, Debug)]
struct BlobMerkleProofStepV1 {
    parent_object_id: ObjectId,
    sibling_object_id: ObjectId,
    sibling_is_left: bool,
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
    if chunks.is_empty() || chunks.iter().any(|chunk| chunk.bytes.is_empty()) {
        return Err(corruption("Merkle layout requires non-empty chunks"));
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

    let mut objects = ImmutableObjectSet::default();
    let mut leaves = Vec::with_capacity(chunks.len());
    for (ordinal, chunk) in chunks.iter().enumerate() {
        let (chunk_object_id, chunk_bytes) = chunk.encode()?;
        objects.insert(chunk_object_id, chunk_bytes)?;
        let leaf = BlobMerkleLeafV1 {
            ordinal: ordinal as u64,
            chunk_object_id,
            declared_len: chunk.bytes.len() as u64,
            chunk_digest: *blake3::hash(&chunk.bytes).as_bytes(),
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
        collect_path(
            &build.objects,
            build.manifest.root_object_id,
            ordinal,
            &mut objects,
            &mut steps,
        )?;
        let leaf_object_id =
            leaf_id_from_path(&build.objects, build.manifest.root_object_id, ordinal)?;
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
        if leaf.ordinal != path.leaf_ordinal {
            return Err(corruption("Merkle proof leaf ordinal is not object-bound"));
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

        let mut current = DecodedNode::Leaf(leaf).summary(path.leaf_object_id);
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
            let (left, right) = if step.sibling_is_left {
                (sibling_summary, current)
            } else {
                (current, sibling_summary)
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
            };
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
    verify_blob_merkle_range(proof, state_key, expected_manifest, requested_range.clone())?;
    if replacements.len() != (requested_range.end - requested_range.start) as usize
        || replacements.keys().copied().ne(requested_range.clone())
    {
        return Err(corruption(
            "Merkle successor replacements do not cover the exact requested range",
        ));
    }

    let mut updates = BTreeMap::<ObjectId, NodeSummary>::new();
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
        let (chunk_object_id, _) = replacement.encode()?;
        let new_leaf = BlobMerkleLeafV1 {
            ordinal: old_leaf.ordinal,
            chunk_object_id,
            declared_len: old_leaf.declared_len,
            chunk_digest: *blake3::hash(&replacement.bytes).as_bytes(),
        };
        let (new_leaf_object_id, _) = encode_leaf(&new_leaf)?;
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
    Ok(canonical_blob_id_from_summary(root))
}

fn validate_fixed_chunk_layout(
    logical_bytes: u64,
    chunks: &[BlobChunkV1],
) -> Result<(), StorageError> {
    let expected_count = logical_bytes.div_ceil(CANONICAL_BLOB_CHUNK_BYTES as u64);
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
    if value.chunk_object_id == ObjectId::ZERO || value.declared_len == 0 {
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
    if value.chunk_object_id == ObjectId::ZERO || value.declared_len == 0 {
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
) -> Result<(), StorageError> {
    let bytes = objects
        .get(node_id)
        .ok_or_else(|| corruption("Merkle tree node is missing during proof build"))?;
    match decode_node(node_id, bytes)? {
        DecodedNode::Leaf(leaf) => {
            if leaf.ordinal != target_ordinal {
                return Err(corruption("Merkle tree leaf ordinal is not canonical"));
            }
            Ok(())
        }
        DecodedNode::Internal(internal) => {
            let in_left = target_ordinal >= internal.left.first_ordinal
                && target_ordinal < internal.left.first_ordinal + internal.left.leaf_count;
            let (child, sibling, sibling_is_left) = if in_left {
                (internal.left, internal.right, false)
            } else {
                (internal.right, internal.left, true)
            };
            let sibling_bytes = objects
                .get(sibling.object_id)
                .ok_or_else(|| corruption("Merkle sibling is missing during proof build"))?;
            proof_objects.insert(sibling.object_id, sibling_bytes.clone())?;
            steps.push(BlobMerkleProofStepV1 {
                parent_object_id: node_id,
                sibling_object_id: sibling.object_id,
                sibling_is_left,
            });
            collect_path(
                objects,
                child.object_id,
                target_ordinal,
                proof_objects,
                steps,
            )
        }
    }
}

fn leaf_id_from_path(
    objects: &ImmutableObjectSet,
    node_id: ObjectId,
    target_ordinal: u64,
) -> Result<ObjectId, StorageError> {
    let bytes = objects
        .get(node_id)
        .ok_or_else(|| corruption("Merkle root is missing during leaf lookup"))?;
    match decode_node(node_id, bytes)? {
        DecodedNode::Leaf(leaf) if leaf.ordinal == target_ordinal => Ok(node_id),
        DecodedNode::Leaf(_) => Err(corruption("Merkle leaf ordinal is not canonical")),
        DecodedNode::Internal(internal) => {
            let child = if target_ordinal >= internal.left.first_ordinal
                && target_ordinal < internal.left.first_ordinal + internal.left.leaf_count
            {
                internal.left.object_id
            } else {
                internal.right.object_id
            };
            leaf_id_from_path(objects, child, target_ordinal)
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
        || manifest.logical_bytes == 0
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
}
