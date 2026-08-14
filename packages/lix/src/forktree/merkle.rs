use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use bytes::Bytes;

use crate::binary_cas::BlobId;
use crate::storage::{
    CoreProjection, GetManyRequest, GetOptions, Key, ProjectedValue, StorageError,
};
use crate::storage_adapter::StorageAdapterRead;

use super::blob::CANONICAL_BLOB_CHUNK_BYTES;
use super::codec::{Encoder, corruption, keyed_hash};
use super::model::{
    BLOB_MERKLE_CHUNK_BYTES, BlobChunkRefV1, BlobChunkV1, BlobManifestV1, BlobMerkleInternalV1,
    BlobMerkleLeafV1, BlobMerkleNodeRefV1, canonical_merkle_blob_id,
};
use super::object::{
    ObjectDomain, ObjectId, authenticate_object_domain, decode_id, decode_object, encode_id,
    encode_object, hash_object_parts,
};
use super::state::{StateKey, StateKeyRef, encode_state_key};
use super::tree::ImmutableObjectSet;
use super::view::load_object_bytes;

const MERKLE_STATE_BINDING_DOMAIN: &str = "lix forktree blob merkle state binding v1";
const MAX_PROOF_DEPTH: usize = 128;
const MAX_SHARED_RANGE_RETENTION_FACTOR: u64 = 4;

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
pub(super) struct BlobMerkleTreeBuild {
    pub(super) manifest: BlobManifestV1,
    pub(super) objects: ImmutableObjectSet,
}

#[derive(Debug)]
struct AuthenticatedBlobMerkleBase {
    chunk_claims: Vec<(BlobChunkRefV1, [u8; 32])>,
    node_object_ids: BTreeSet<ObjectId>,
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
pub(super) struct BlobMerkleProofV1 {
    manifest: BlobManifestV1,
    requested_range: Range<u64>,
    state_binding: [u8; 32],
    paths: Vec<BlobMerkleProofPathV1>,
    objects: ImmutableObjectSet,
}

impl BlobMerkleProofV1 {
    #[cfg(test)]
    fn object_count(&self) -> usize {
        self.objects.iter().count()
    }
}

/// Builds a canonical fixed-chunk Merkle layout. `BlobId` is the canonical
/// Merkle content identity: it is derived from a domain-separated envelope
/// containing the root ObjectId, logical length, fixed-chunk geometry, leaf
/// count, and tree height. No flat whole-content BlobId calculation is used.
pub(super) fn build_blob_merkle_tree(
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
            BlobChunkRefV1 {
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
pub(super) fn build_blob_merkle_tree_from_chunk_claims(
    logical_bytes: u64,
    chunks: &[(BlobChunkRefV1, [u8; 32])],
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

/// Builds a variable-width successor from one authenticated retained Merkle
/// closure. Unchanged canonical chunks remain referenced by ObjectId; only
/// chunks whose bytes or fixed-width positions changed are encoded. Existing
/// leaf/internal ObjectIds are removed from the returned object set, so the
/// caller publishes only the new chunk and path-copy closure.
pub(super) async fn build_blob_merkle_edit_successor<R>(
    read: &R,
    manifest: BlobManifestV1,
    payload: &[u8],
    offset: usize,
    delete_len: usize,
    insert_len: usize,
) -> Result<BlobMerkleTreeBuild, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let base_len = usize::try_from(manifest.logical_bytes)
        .map_err(|_| corruption("Merkle edit base length does not fit usize"))?;
    let delete_end = offset
        .checked_add(delete_len)
        .filter(|end| *end <= base_len)
        .ok_or_else(|| corruption("Merkle edit delete range is invalid"))?;
    let insert_end = offset
        .checked_add(insert_len)
        .ok_or_else(|| corruption("Merkle edit insert range overflows"))?;
    let successor_len = base_len
        .checked_sub(delete_len)
        .and_then(|len| len.checked_add(insert_len))
        .ok_or_else(|| corruption("Merkle edit successor length overflows"))?;
    if successor_len != payload.len() || (delete_len == 0 && insert_len == 0) {
        return Err(corruption(
            "Merkle edit payload length or changed range is invalid",
        ));
    }

    let base = load_authenticated_blob_merkle_base(read, manifest).await?;
    build_blob_merkle_edit_from_base(manifest, &base, payload, offset, delete_end, insert_end)
}

fn build_blob_merkle_edit_from_base(
    manifest: BlobManifestV1,
    base: &AuthenticatedBlobMerkleBase,
    payload: &[u8],
    offset: usize,
    delete_end: usize,
    insert_end: usize,
) -> Result<BlobMerkleTreeBuild, StorageError> {
    let chunk_bytes = CANONICAL_BLOB_CHUNK_BYTES;
    let successor_leaf_count = payload.len().div_ceil(chunk_bytes).max(1);
    let old_chunk_ids = base
        .chunk_claims
        .iter()
        .map(|(chunk, _)| chunk.chunk_object_id)
        .collect::<BTreeSet<_>>();
    let mut chunk_claims = Vec::with_capacity(successor_leaf_count);
    let mut new_chunks = ImmutableObjectSet::default();

    for ordinal in 0..successor_leaf_count {
        let start = ordinal * chunk_bytes;
        let end = start.saturating_add(chunk_bytes).min(payload.len());
        let len = end - start;
        let mapped_old_start = if end <= offset {
            Some(start)
        } else if start >= insert_end {
            delete_end.checked_add(start - insert_end)
        } else {
            None
        };
        let reused = mapped_old_start.and_then(|old_start| {
            let old_end = old_start.checked_add(len)?;
            if old_start % chunk_bytes != 0 || old_end > manifest.logical_bytes as usize {
                return None;
            }
            let (chunk, digest) = base.chunk_claims.get(old_start / chunk_bytes)?;
            (chunk.declared_len == len as u64).then(|| (chunk.clone(), *digest))
        });
        if let Some(claim) = reused {
            chunk_claims.push(claim);
            continue;
        }

        let chunk = BlobChunkV1 {
            bytes: Bytes::copy_from_slice(&payload[start..end]),
        };
        let (chunk_object_id, chunk_encoding) = chunk.encode()?;
        if !old_chunk_ids.contains(&chunk_object_id) {
            new_chunks.insert(chunk_object_id, chunk_encoding)?;
        }
        chunk_claims.push((
            BlobChunkRefV1 {
                chunk_object_id,
                declared_len: len as u64,
            },
            *blake3::hash(&chunk.bytes).as_bytes(),
        ));
    }

    let mut successor =
        build_blob_merkle_tree_from_chunk_claims(payload.len() as u64, &chunk_claims)?;
    for id in &base.node_object_ids {
        successor.objects.remove(*id);
    }
    successor.objects.extend(new_chunks)?;
    Ok(successor)
}

async fn load_authenticated_blob_merkle_base<R>(
    read: &R,
    manifest: BlobManifestV1,
) -> Result<AuthenticatedBlobMerkleBase, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let root = NodeSummary {
        object_id: manifest.root_object_id,
        height: manifest.root_height,
        first_ordinal: 0,
        leaf_count: manifest.leaf_count,
        logical_bytes: manifest.logical_bytes,
    };
    if canonical_blob_id_from_summary(root) != manifest.canonical_blob_id {
        return Err(corruption(
            "Merkle edit manifest identity does not match its root summary",
        ));
    }
    let mut claims_by_ordinal = BTreeMap::new();
    let mut node_object_ids = BTreeSet::new();
    let mut frontier = vec![root];
    while !frontier.is_empty() {
        let ids = frontier
            .iter()
            .map(|summary| summary.object_id)
            .collect::<Vec<_>>();
        let objects = load_merkle_objects_many(read, &ids).await?;
        let mut next = Vec::with_capacity(frontier.len().saturating_mul(2));
        for expected in frontier {
            if !node_object_ids.insert(expected.object_id) {
                return Err(corruption("Merkle edit base contains a node cycle"));
            }
            let bytes = objects
                .get(&expected.object_id)
                .ok_or_else(|| corruption("Merkle edit base node is absent"))?;
            let node = decode_node(expected.object_id, bytes)?;
            if node.summary(expected.object_id) != expected {
                return Err(corruption(
                    "Merkle edit base node does not match its authenticated parent summary",
                ));
            }
            match node {
                DecodedNode::Leaf(leaf) => {
                    if claims_by_ordinal
                        .insert(
                            leaf.ordinal,
                            (
                                BlobChunkRefV1 {
                                    chunk_object_id: leaf.chunk_object_id,
                                    declared_len: leaf.declared_len,
                                },
                                leaf.chunk_digest,
                            ),
                        )
                        .is_some()
                    {
                        return Err(corruption("Merkle edit base repeats a leaf ordinal"));
                    }
                }
                DecodedNode::Internal(internal) => {
                    next.push(NodeSummary {
                        object_id: internal.left.object_id,
                        height: internal.left.height,
                        first_ordinal: internal.left.first_ordinal,
                        leaf_count: internal.left.leaf_count,
                        logical_bytes: internal.left.logical_bytes,
                    });
                    next.push(NodeSummary {
                        object_id: internal.right.object_id,
                        height: internal.right.height,
                        first_ordinal: internal.right.first_ordinal,
                        leaf_count: internal.right.leaf_count,
                        logical_bytes: internal.right.logical_bytes,
                    });
                }
            }
        }
        frontier = next;
    }
    let chunk_claims = claims_by_ordinal
        .into_iter()
        .enumerate()
        .map(|(expected, (ordinal, claim))| {
            if ordinal != expected as u64 {
                Err(corruption("Merkle edit base leaf order is invalid"))
            } else {
                Ok(claim)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if chunk_claims.len() as u64 != manifest.leaf_count {
        return Err(corruption("Merkle edit base leaf count is inconsistent"));
    }
    for (ordinal, (chunk, _)) in chunk_claims.iter().enumerate() {
        let expected_len = if ordinal + 1 == chunk_claims.len() {
            manifest.logical_bytes - ordinal as u64 * CANONICAL_BLOB_CHUNK_BYTES as u64
        } else {
            CANONICAL_BLOB_CHUNK_BYTES as u64
        };
        if chunk.declared_len != expected_len {
            return Err(corruption(
                "Merkle edit base chunks do not have canonical fixed geometry",
            ));
        }
    }
    Ok(AuthenticatedBlobMerkleBase {
        chunk_claims,
        node_object_ids,
    })
}

async fn load_merkle_objects_many<R>(
    read: &R,
    ids: &[ObjectId],
) -> Result<BTreeMap<ObjectId, Bytes>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let keys = ids
        .iter()
        .map(|id| Key(Bytes::copy_from_slice(id.as_bytes())))
        .collect::<Vec<_>>();
    let loaded = read
        .get_many(&[GetManyRequest {
            space: super::object::OBJECT_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    if loaded.values.len() != ids.len() {
        return Err(corruption(
            "Merkle edit base read returned the wrong slot count",
        ));
    }
    ids.iter()
        .copied()
        .zip(loaded.values)
        .map(|(id, value)| match value {
            Some(ProjectedValue::FullValue(bytes)) => Ok((id, bytes)),
            Some(ProjectedValue::KeyOnly) => Err(corruption(
                "Merkle edit base read returned key-only projection",
            )),
            None => Err(corruption(format!(
                "Merkle edit base object {id} is absent"
            ))),
        })
        .collect()
}

/// Computes the sole canonical blob identity from complete content. This is
/// used by transaction-local semantic rows that must name an inline payload
/// before publication; the durable reader still authorizes only the
/// StateKey-bound manifest root and never looks content up by this value.
pub(crate) fn canonical_blob_id_for_content(content: &[u8]) -> Result<BlobId, StorageError> {
    let logical_bytes = u64::try_from(content.len())
        .map_err(|_| corruption("canonical blob content exceeds u64 length"))?;
    let leaf_count = content.len().div_ceil(CANONICAL_BLOB_CHUNK_BYTES).max(1);
    if leaf_count == 1 {
        return Ok(canonical_blob_id_from_summary(summary_leaf_for_content(
            0, content,
        )?));
    }
    let mut leaves = Vec::with_capacity(leaf_count);

    for (ordinal, chunk) in content.chunks(CANONICAL_BLOB_CHUNK_BYTES).enumerate() {
        let ordinal =
            u64::try_from(ordinal).map_err(|_| corruption("canonical blob has too many chunks"))?;
        leaves.push(summary_leaf_for_content(ordinal, chunk)?);
    }

    let root = build_summary_node(&leaves)?;
    debug_assert_eq!(root.logical_bytes, logical_bytes);
    debug_assert_eq!(root.leaf_count, leaf_count as u64);
    Ok(canonical_blob_id_from_summary(root))
}

/// Builds the smallest authenticated Merkle fixture for unit tests.  The
/// production builder remains the canonical multi-leaf path; this helper is
/// intentionally test-only so small corruption controls do not need to carry
/// a 1 MiB allocation merely to exercise manifest encoding.
#[cfg(test)]
pub(super) fn single_leaf_manifest_for_test(
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
pub(super) async fn load_blob_merkle_range_proof<R>(
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
            let summary = node.summary(node_id);
            if summary.as_ref() != expected_ref {
                return Err(corruption(
                    "Merkle node does not match its authenticated parent summary",
                ));
            }
            match node {
                DecodedNode::Leaf(leaf) => {
                    if leaf.ordinal != ordinal {
                        return Err(corruption("Merkle leaf ordinal is not canonical"));
                    }
                    objects.insert(node_id, node_bytes)?;
                    let chunk_bytes =
                        load_proof_object(read, &mut loaded, leaf.chunk_object_id).await?;
                    objects.insert(leaf.chunk_object_id, chunk_bytes)?;
                    paths.push(BlobMerkleProofPathV1 {
                        leaf_object_id: node_id,
                        leaf_ordinal: ordinal,
                        steps,
                    });
                    break;
                }
                DecodedNode::Internal(internal) => {
                    let in_left = ordinal >= internal.left.first_ordinal
                        && ordinal < internal.left.first_ordinal + internal.left.leaf_count;
                    let in_right = ordinal >= internal.right.first_ordinal
                        && ordinal < internal.right.first_ordinal + internal.right.leaf_count;
                    if in_left == in_right {
                        return Err(corruption(
                            "Merkle ordinal is not covered by exactly one child",
                        ));
                    }
                    let (child, sibling, sibling_is_left) = if in_left {
                        (internal.left, internal.right, false)
                    } else {
                        (internal.right, internal.left, true)
                    };
                    let sibling_bytes =
                        load_proof_object(read, &mut loaded, sibling.object_id).await?;
                    let sibling_node = decode_node(sibling.object_id, &sibling_bytes)?;
                    if sibling_node.summary(sibling.object_id).as_ref() != sibling {
                        return Err(corruption(
                            "Merkle sibling does not match its authenticated parent summary",
                        ));
                    }
                    objects.insert(sibling.object_id, sibling_bytes)?;
                    steps.push(BlobMerkleProofStepV1 {
                        parent_object_id: node_id,
                        sibling_object_id: sibling.object_id,
                        sibling_is_left,
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

pub(super) fn leaf_range_for_bytes(
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

pub(super) fn materialize_blob_merkle_range(
    proof: &BlobMerkleProofV1,
    state_key: &StateKey,
    manifest: BlobManifestV1,
    requested: Range<u64>,
) -> Result<Bytes, StorageError> {
    let leaf_range = leaf_range_for_bytes(&manifest, requested.clone())?;
    verify_blob_merkle_range(proof, state_key, manifest, leaf_range)?;

    if requested.start == requested.end {
        return Ok(Bytes::new());
    }

    // The authenticated object is already a shared `Bytes` buffer. For a
    // range contained in one leaf, retain a slice of that exact object rather
    // than copying the payload into an intermediate Vec. The object header
    // remains part of the authenticated buffer; only the payload subrange is
    // exposed to the caller.
    if proof.paths.len() == 1 {
        let path = &proof.paths[0];
        let leaf_bytes = proof
            .objects
            .get(path.leaf_object_id)
            .ok_or_else(|| corruption("Merkle materialization leaf is missing"))?;
        let leaf = decode_leaf(path.leaf_object_id, leaf_bytes)?;
        let chunk_bytes = proof
            .objects
            .get(leaf.chunk_object_id)
            .ok_or_else(|| corruption("Merkle materialization chunk is missing"))?;
        let chunk = BlobChunkV1::decode_borrowed(leaf.chunk_object_id, chunk_bytes)?;
        let chunk_start = leaf
            .ordinal
            .checked_mul(u64::from(BLOB_MERKLE_CHUNK_BYTES))
            .ok_or_else(|| corruption("Merkle chunk offset overflowed"))?;
        let start = requested
            .start
            .checked_sub(chunk_start)
            .ok_or_else(|| corruption("Merkle materialization starts before its leaf"))?;
        let end = requested
            .end
            .min(
                chunk_start
                    .checked_add(leaf.declared_len)
                    .ok_or_else(|| corruption("Merkle chunk end overflowed"))?,
            )
            .checked_sub(chunk_start)
            .ok_or_else(|| corruption("Merkle materialization ends before its leaf"))?;
        let start = usize::try_from(start)
            .map_err(|_| corruption("Merkle materialization start exceeds usize"))?;
        let end = usize::try_from(end)
            .map_err(|_| corruption("Merkle materialization end exceeds usize"))?;
        let payload_start = chunk.as_ptr() as usize;
        let object_start = chunk_bytes.as_ptr() as usize;
        let payload_offset = payload_start
            .checked_sub(object_start)
            .ok_or_else(|| corruption("Merkle chunk payload is outside its object"))?;
        let payload_end = payload_offset
            .checked_add(chunk.len())
            .ok_or_else(|| corruption("Merkle chunk payload offset overflowed"))?;
        let object_end = object_start
            .checked_add(chunk_bytes.len())
            .ok_or_else(|| corruption("Merkle chunk object address overflowed"))?;
        if payload_start < object_start
            || payload_end > object_end
            || end > chunk.len()
            || start > end
        {
            return Err(corruption("Merkle materialization chunk range is invalid"));
        }
        let requested_len = u64::try_from(end - start)
            .map_err(|_| corruption("Merkle materialization range exceeds u64"))?;
        let chunk_len = u64::try_from(chunk.len())
            .map_err(|_| corruption("Merkle chunk length exceeds u64"))?;
        if requested_len
            .checked_mul(MAX_SHARED_RANGE_RETENTION_FACTOR)
            .is_none_or(|bounded_len| bounded_len < chunk_len)
        {
            return Ok(Bytes::copy_from_slice(&chunk[start..end]));
        }
        return Ok(chunk_bytes.slice(payload_offset + start..payload_offset + end));
    }

    // Multi-leaf ranges have no single backing allocation. Allocate exactly
    // the requested size once and append authenticated leaf slices directly;
    // this is the sole coalescing copy before the final consumer.
    let mut output = Vec::with_capacity(
        usize::try_from(requested.end - requested.start)
            .map_err(|_| corruption("Merkle materialization range exceeds usize"))?,
    );
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
        let chunk_start = leaf.ordinal * u64::from(BLOB_MERKLE_CHUNK_BYTES);
        let start = requested.start.saturating_sub(chunk_start) as usize;
        let chunk_end = chunk_start
            .checked_add(leaf.declared_len)
            .ok_or_else(|| corruption("Merkle chunk end overflowed"))?;
        let end = (requested.end.min(chunk_end) - chunk_start) as usize;
        output.extend_from_slice(&chunk[start..end]);
    }
    if output.len() as u64 != requested.end - requested.start {
        return Err(corruption("Merkle materialized byte range is incomplete"));
    }
    Ok(Bytes::from(output))
}

/// Creates a proof for an exact half-open range of leaf ordinals. The proof
/// contains O(K log N) tree objects for K requested leaves and never copies an
/// unrelated leaf payload.
#[cfg(test)]
fn prove_blob_merkle_range(
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
fn verify_blob_merkle_range(
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
        if leaf.ordinal != path.leaf_ordinal {
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
#[cfg(test)]
fn derive_blob_merkle_successor_id(
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
pub(super) fn build_blob_merkle_successor(
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

fn summary_leaf_for_content(ordinal: u64, chunk: &[u8]) -> Result<NodeSummary, StorageError> {
    let chunk_len = u32::try_from(chunk.len())
        .map_err(|_| corruption("canonical blob chunk exceeds u32 length"))?;
    let chunk_len_bytes = chunk_len.to_be_bytes();
    let chunk_object_id = hash_object_parts(ObjectDomain::BlobChunk, &[&chunk_len_bytes, chunk]);
    let declared_len = u64::try_from(chunk.len())
        .map_err(|_| corruption("canonical blob chunk exceeds u64 length"))?;
    let chunk_digest = *blake3::hash(chunk).as_bytes();
    let ordinal_bytes = ordinal.to_be_bytes();
    let declared_len_bytes = declared_len.to_be_bytes();
    let leaf_object_id = hash_object_parts(
        ObjectDomain::BlobMerkleLeafV1,
        &[
            &ordinal_bytes,
            chunk_object_id.as_bytes(),
            &declared_len_bytes,
            &chunk_digest,
        ],
    );
    if leaf_object_id == ObjectId::ZERO {
        return Err(corruption("canonical blob leaf has a zero object id"));
    }
    Ok(NodeSummary {
        object_id: leaf_object_id,
        height: 0,
        first_ordinal: ordinal,
        leaf_count: 1,
        logical_bytes: declared_len,
    })
}

fn build_summary_node(leaves: &[NodeSummary]) -> Result<NodeSummary, StorageError> {
    if leaves.is_empty() {
        return Err(corruption("canonical blob has no Merkle leaves"));
    }
    if leaves.len() == 1 {
        return Ok(leaves[0]);
    }

    let split = leaves
        .len()
        .checked_next_power_of_two()
        .ok_or_else(|| corruption("Merkle leaf count cannot be represented"))?
        / 2;
    let left = build_summary_node(&leaves[..split])?;
    let right = build_summary_node(&leaves[split..])?;
    let value = BlobMerkleInternalV1 {
        height: left.height.max(right.height).saturating_add(1),
        first_ordinal: left.first_ordinal,
        leaf_count: left.leaf_count + right.leaf_count,
        logical_bytes: left.logical_bytes + right.logical_bytes,
        left: left.as_ref(),
        right: right.as_ref(),
    };
    validate_internal(&value)?;

    let height_bytes = value.height.to_be_bytes();
    let first_ordinal_bytes = value.first_ordinal.to_be_bytes();
    let leaf_count_bytes = value.leaf_count.to_be_bytes();
    let logical_bytes = value.logical_bytes.to_be_bytes();
    let left_height_bytes = value.left.height.to_be_bytes();
    let left_first_ordinal_bytes = value.left.first_ordinal.to_be_bytes();
    let left_leaf_count_bytes = value.left.leaf_count.to_be_bytes();
    let left_logical_bytes = value.left.logical_bytes.to_be_bytes();
    let right_height_bytes = value.right.height.to_be_bytes();
    let right_first_ordinal_bytes = value.right.first_ordinal.to_be_bytes();
    let right_leaf_count_bytes = value.right.leaf_count.to_be_bytes();
    let right_logical_bytes = value.right.logical_bytes.to_be_bytes();
    let object_id = hash_object_parts(
        ObjectDomain::BlobMerkleInternalV1,
        &[
            &height_bytes,
            &first_ordinal_bytes,
            &leaf_count_bytes,
            &logical_bytes,
            value.left.object_id.as_bytes(),
            &left_height_bytes,
            &left_first_ordinal_bytes,
            &left_leaf_count_bytes,
            &left_logical_bytes,
            value.right.object_id.as_bytes(),
            &right_height_bytes,
            &right_first_ordinal_bytes,
            &right_leaf_count_bytes,
            &right_logical_bytes,
        ],
    );
    if object_id == ObjectId::ZERO {
        return Err(corruption(
            "canonical blob internal node has a zero object id",
        ));
    }
    Ok(NodeSummary {
        object_id,
        height: value.height,
        first_ordinal: value.first_ordinal,
        leaf_count: value.leaf_count,
        logical_bytes: value.logical_bytes,
    })
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
    // Canonical left-complete shape: the left subtree is the largest complete
    // power-of-two prefix, except that an already-complete tree splits evenly.
    // This is content-canonical and lets appending one leaf path-copy only the
    // right frontier instead of repartitioning every retained leaf.
    let split = leaves
        .len()
        .checked_next_power_of_two()
        .ok_or_else(|| corruption("Merkle leaf count cannot be represented"))?
        / 2;
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

    fn authenticated_base(build: &BlobMerkleTreeBuild) -> AuthenticatedBlobMerkleBase {
        fn collect(
            objects: &ImmutableObjectSet,
            id: ObjectId,
            claims: &mut Vec<(BlobChunkRefV1, [u8; 32])>,
            node_ids: &mut BTreeSet<ObjectId>,
        ) {
            assert!(node_ids.insert(id));
            match decode_node(id, objects.get(id).unwrap()).unwrap() {
                DecodedNode::Leaf(leaf) => claims.push((
                    BlobChunkRefV1 {
                        chunk_object_id: leaf.chunk_object_id,
                        declared_len: leaf.declared_len,
                    },
                    leaf.chunk_digest,
                )),
                DecodedNode::Internal(internal) => {
                    collect(objects, internal.left.object_id, claims, node_ids);
                    collect(objects, internal.right.object_id, claims, node_ids);
                }
            }
        }

        let mut chunk_claims = Vec::new();
        let mut node_object_ids = BTreeSet::new();
        collect(
            &build.objects,
            build.manifest.root_object_id,
            &mut chunk_claims,
            &mut node_object_ids,
        );
        AuthenticatedBlobMerkleBase {
            chunk_claims,
            node_object_ids,
        }
    }

    fn canonical_chunks(bytes: &[u8]) -> Vec<BlobChunkV1> {
        if bytes.is_empty() {
            return vec![BlobChunkV1 {
                bytes: Bytes::new(),
            }];
        }
        bytes
            .chunks(CANONICAL_BLOB_CHUNK_BYTES)
            .map(|bytes| BlobChunkV1 {
                bytes: Bytes::copy_from_slice(bytes),
            })
            .collect()
    }

    fn staged_chunk_count(build: &BlobMerkleTreeBuild) -> usize {
        build
            .objects
            .iter()
            .filter(|(id, bytes)| {
                authenticate_object_domain(*id, bytes) == Ok(ObjectDomain::BlobChunk)
            })
            .count()
    }

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
    fn summary_only_canonical_blob_id_matches_full_builder() {
        for size in [
            0,
            1,
            4 * 1024,
            CANONICAL_BLOB_CHUNK_BYTES - 1,
            CANONICAL_BLOB_CHUNK_BYTES,
            CANONICAL_BLOB_CHUNK_BYTES + 1,
            3 * CANONICAL_BLOB_CHUNK_BYTES + 123,
        ] {
            let content = (0..size)
                .map(|index| (index as u8).wrapping_mul(31).wrapping_add(7))
                .collect::<Vec<_>>();
            let expected = build_blob_merkle_tree(&canonical_chunks(&content))
                .unwrap()
                .manifest
                .canonical_blob_id;
            assert_eq!(
                canonical_blob_id_for_content(&content).unwrap(),
                expected,
                "summary-only canonical identity diverged at {size} bytes"
            );
        }
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
            Bytes::new()
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
        let substituted_proof = prove_blob_merkle_range(&substituted, &key, 0..1).unwrap();
        assert!(
            verify_blob_merkle_range(&substituted_proof, &key, substituted.manifest, 0..1,)
                .is_err(),
            "a non-empty leaf cannot substitute for the canonical empty root"
        );
    }

    #[test]
    fn single_leaf_materialization_reuses_authenticated_chunk_bytes() {
        let build = build_blob_merkle_tree(&[BlobChunkV1 {
            bytes: Bytes::from_static(b"authenticated chunk"),
        }])
        .unwrap();
        let key = state_key();
        let proof = prove_blob_merkle_range(&build, &key, 0..1).unwrap();
        let leaf = decode_leaf(
            proof.paths[0].leaf_object_id,
            proof.objects.get(proof.paths[0].leaf_object_id).unwrap(),
        )
        .unwrap();
        let encoded_chunk = proof.objects.get(leaf.chunk_object_id).unwrap();
        let materialized =
            materialize_blob_merkle_range(&proof, &key, build.manifest, 3..13).unwrap();

        assert_eq!(materialized.as_ref(), b"henticated".as_slice());
        let encoded_start = encoded_chunk.as_ptr() as usize;
        let encoded_end = encoded_start + encoded_chunk.len();
        let materialized_start = materialized.as_ptr() as usize;
        assert!(
            materialized_start >= encoded_start
                && materialized_start + materialized.len() <= encoded_end,
            "single-leaf reads must retain a slice of the authenticated chunk object"
        );
    }

    #[test]
    fn tiny_single_leaf_ranges_do_not_retain_the_full_chunk_object() {
        let build = build_blob_merkle_tree(&chunks(1)).unwrap();
        let key = state_key();
        let proof = prove_blob_merkle_range(&build, &key, 0..1).unwrap();
        let leaf = decode_leaf(
            proof.paths[0].leaf_object_id,
            proof.objects.get(proof.paths[0].leaf_object_id).unwrap(),
        )
        .unwrap();
        let encoded_chunk = proof.objects.get(leaf.chunk_object_id).unwrap();
        let materialized =
            materialize_blob_merkle_range(&proof, &key, build.manifest, 0..1).unwrap();

        assert_eq!(materialized.as_ref(), [1].as_slice());
        let encoded_start = encoded_chunk.as_ptr() as usize;
        let encoded_end = encoded_start + encoded_chunk.len();
        let materialized_start = materialized.as_ptr() as usize;
        assert!(
            materialized_start < encoded_start
                || materialized_start >= encoded_end
                || materialized_start + materialized.len() > encoded_end,
            "tiny ranges must copy instead of retaining a full authenticated chunk"
        );
    }

    #[test]
    fn multi_leaf_materialization_copies_only_the_requested_cross_chunk_range() {
        let build = build_blob_merkle_tree(&chunks(2)).unwrap();
        let key = state_key();
        let proof = prove_blob_merkle_range(&build, &key, 0..2).unwrap();
        let chunk_size = CANONICAL_BLOB_CHUNK_BYTES as u64;
        let start = chunk_size - 3;
        let end = chunk_size + 4;
        let materialized =
            materialize_blob_merkle_range(&proof, &key, build.manifest, start..end).unwrap();

        assert_eq!(materialized.len(), 7);
        assert_eq!(materialized.as_ref(), [1, 1, 1, 2, 2, 2, 2].as_slice());
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
        assert!(
            materialize_blob_merkle_range(&proof, &wrong_key, build.manifest, 3..5).is_err(),
            "a wrong StateKey must fail before materialization"
        );
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
        assert!(materialize_blob_merkle_range(&proof, &key, build.manifest, 1..2).is_err());

        let mut proof = prove_blob_merkle_range(&build, &key, 1..2).unwrap();
        proof.paths[0].steps[0].parent_object_id = ObjectId::ZERO;
        assert!(verify_blob_merkle_range(&proof, &key, build.manifest, 1..2).is_err());
        assert!(materialize_blob_merkle_range(&proof, &key, build.manifest, 1..2).is_err());
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
    fn variable_width_edits_reuse_authenticated_chunks_and_match_full_rebuild() {
        let base_chunks = chunks(4);
        let base_bytes = base_chunks
            .iter()
            .flat_map(|chunk| chunk.bytes.iter().copied())
            .collect::<Vec<_>>();
        let base = build_blob_merkle_tree(&base_chunks).unwrap();
        let authenticated = authenticated_base(&base);

        let inserted = vec![0xf5; CANONICAL_BLOB_CHUNK_BYTES];
        let mut appended = base_bytes.clone();
        appended.extend_from_slice(&inserted);
        let append = build_blob_merkle_edit_from_base(
            base.manifest,
            &authenticated,
            &appended,
            base_bytes.len(),
            base_bytes.len(),
            appended.len(),
        )
        .unwrap();
        let expected_append = build_blob_merkle_tree(&canonical_chunks(&appended)).unwrap();
        assert_eq!(append.manifest, expected_append.manifest);
        assert_eq!(staged_chunk_count(&append), 1);
        assert!(
            append
                .objects
                .iter()
                .all(|(id, _)| { !authenticated.node_object_ids.contains(&id) })
        );

        let truncated = base_bytes[..2 * CANONICAL_BLOB_CHUNK_BYTES].to_vec();
        let truncate = build_blob_merkle_edit_from_base(
            base.manifest,
            &authenticated,
            &truncated,
            truncated.len(),
            base_bytes.len(),
            truncated.len(),
        )
        .unwrap();
        let expected_truncate = build_blob_merkle_tree(&canonical_chunks(&truncated)).unwrap();
        assert_eq!(truncate.manifest, expected_truncate.manifest);
        assert_eq!(staged_chunk_count(&truncate), 0);

        let offset = CANONICAL_BLOB_CHUNK_BYTES;
        let mut middle_insert = Vec::with_capacity(base_bytes.len() + inserted.len());
        middle_insert.extend_from_slice(&base_bytes[..offset]);
        middle_insert.extend_from_slice(&inserted);
        middle_insert.extend_from_slice(&base_bytes[offset..]);
        let insert = build_blob_merkle_edit_from_base(
            base.manifest,
            &authenticated,
            &middle_insert,
            offset,
            offset,
            offset + inserted.len(),
        )
        .unwrap();
        let expected_insert = build_blob_merkle_tree(&canonical_chunks(&middle_insert)).unwrap();
        assert_eq!(insert.manifest, expected_insert.manifest);
        assert_eq!(staged_chunk_count(&insert), 1);

        let delete_end = offset + CANONICAL_BLOB_CHUNK_BYTES;
        let mut middle_delete = Vec::with_capacity(base_bytes.len() - CANONICAL_BLOB_CHUNK_BYTES);
        middle_delete.extend_from_slice(&base_bytes[..offset]);
        middle_delete.extend_from_slice(&base_bytes[delete_end..]);
        let delete = build_blob_merkle_edit_from_base(
            base.manifest,
            &authenticated,
            &middle_delete,
            offset,
            delete_end,
            offset,
        )
        .unwrap();
        let expected_delete = build_blob_merkle_tree(&canonical_chunks(&middle_delete)).unwrap();
        assert_eq!(delete.manifest, expected_delete.manifest);
        assert_eq!(staged_chunk_count(&delete), 0);
    }

    #[test]
    fn unaligned_variable_edit_rebuilds_only_the_necessarily_shifted_suffix() {
        let base_chunks = chunks(3);
        let base_bytes = base_chunks
            .iter()
            .flat_map(|chunk| chunk.bytes.iter().copied())
            .collect::<Vec<_>>();
        let base = build_blob_merkle_tree(&base_chunks).unwrap();
        let authenticated = authenticated_base(&base);
        let offset = CANONICAL_BLOB_CHUNK_BYTES / 2;
        let delete_len = 11;
        let insert = [0x5a; 17];
        let mut successor = Vec::new();
        successor.extend_from_slice(&base_bytes[..offset]);
        successor.extend_from_slice(&insert);
        successor.extend_from_slice(&base_bytes[offset + delete_len..]);
        let edit = build_blob_merkle_edit_from_base(
            base.manifest,
            &authenticated,
            &successor,
            offset,
            offset + delete_len,
            offset + insert.len(),
        )
        .unwrap();
        let expected = build_blob_merkle_tree(&canonical_chunks(&successor)).unwrap();
        assert_eq!(edit.manifest, expected.manifest);
        assert_eq!(staged_chunk_count(&edit), 4);
    }
}
