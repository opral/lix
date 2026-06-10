use musli::{Decode, Encode};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::LixError;
use crate::storage_codec;
use crate::tracked_state::types::{
    TRACKED_STATE_HASH_BYTES, TrackedSchemaFilePrefixRef, TrackedSchemaKeyPrefixRef,
    TrackedStateIndexValue, TrackedStateIndexValueRef, TrackedStateKey, TrackedStateKeyRef,
};

#[cfg(test)]
use crate::json_store::JsonRef;

const WEIBULL_K: i32 = 4;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) struct EncodedLeafEntry {
    #[musli(bytes)]
    pub(crate) key: Vec<u8>,
    #[musli(bytes)]
    pub(crate) value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Encode, Decode)]
#[musli(packed)]
pub(crate) struct EncodedLeafEntryRef<'a> {
    #[musli(bytes)]
    pub(crate) key: &'a [u8],
    #[musli(bytes)]
    pub(crate) value: &'a [u8],
}

impl EncodedLeafEntry {
    pub(crate) fn as_ref(&self) -> EncodedLeafEntryRef<'_> {
        EncodedLeafEntryRef {
            key: &self.key,
            value: &self.value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) struct PendingChunkWrite {
    pub(crate) hash: [u8; TRACKED_STATE_HASH_BYTES],
    #[musli(bytes)]
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) struct ChildSummary {
    #[musli(bytes)]
    pub(crate) first_key: Vec<u8>,
    #[musli(bytes)]
    pub(crate) last_key: Vec<u8>,
    pub(crate) child_hash: [u8; TRACKED_STATE_HASH_BYTES],
    pub(crate) subtree_count: u64,
}

#[derive(Debug, Clone, Copy, Encode, Decode)]
#[musli(packed)]
pub(crate) struct ChildSummaryRef<'a> {
    #[musli(bytes)]
    pub(crate) first_key: &'a [u8],
    #[musli(bytes)]
    pub(crate) last_key: &'a [u8],
    pub(crate) child_hash: [u8; TRACKED_STATE_HASH_BYTES],
    pub(crate) subtree_count: u64,
}

impl ChildSummary {
    pub(crate) fn as_ref(&self) -> ChildSummaryRef<'_> {
        ChildSummaryRef {
            first_key: &self.first_key,
            last_key: &self.last_key,
            child_hash: self.child_hash,
            subtree_count: self.subtree_count,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum DecodedNode {
    Leaf(DecodedLeafNode),
    Internal(DecodedInternalNode),
}

#[derive(Debug, Clone)]
pub(crate) enum DecodedNodeRef<'a> {
    Leaf(DecodedLeafNodeRef<'a>),
    Internal(DecodedInternalNode),
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedLeafNode {
    entries: Vec<EncodedLeafEntry>,
}

impl DecodedLeafNode {
    pub(crate) fn entries(&self) -> &[EncodedLeafEntry] {
        &self.entries
    }
}

/// Decoded view of a leaf node.
///
/// Keys are stored front-coded on disk and reconstructed into one arena at
/// decode time; values stay borrowed from the chunk bytes verbatim.
#[derive(Debug, Clone)]
pub(crate) struct DecodedLeafNodeRef<'a> {
    key_arena: Vec<u8>,
    entries: Vec<LeafEntrySpan<'a>>,
}

#[derive(Debug, Clone, Copy)]
struct LeafEntrySpan<'a> {
    key_start: usize,
    key_end: usize,
    value: &'a [u8],
}

impl DecodedLeafNodeRef<'_> {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    #[expect(clippy::unnecessary_wraps)]
    pub(crate) fn entry(&self, index: usize) -> Result<Option<EncodedLeafEntryRef<'_>>, LixError> {
        Ok(self.entries.get(index).map(|span| EncodedLeafEntryRef {
            key: &self.key_arena[span.key_start..span.key_end],
            value: span.value,
        }))
    }

    #[expect(clippy::unnecessary_wraps)]
    pub(crate) fn key(&self, index: usize) -> Result<Option<&[u8]>, LixError> {
        Ok(self
            .entries
            .get(index)
            .map(|span| &self.key_arena[span.key_start..span.key_end]))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedInternalNode {
    children: Vec<ChildSummary>,
}

impl DecodedInternalNode {
    pub(crate) fn children(&self) -> &[ChildSummary] {
        &self.children
    }
}

const NODE_KIND_LEAF_V2: u8 = 1;
const NODE_KIND_INTERNAL: u8 = 2;

#[derive(Encode, Decode)]
struct StorageInternalNode<'a> {
    children: Vec<ChildSummaryRef<'a>>,
}

pub(crate) fn hash_bytes(bytes: &[u8]) -> [u8; TRACKED_STATE_HASH_BYTES] {
    *blake3::hash(bytes).as_bytes()
}

pub(crate) fn encode_key(key: &TrackedStateKey) -> Vec<u8> {
    storage_codec::encode("tracked-state key", key)
        .expect("tracked-state key storage encoding should not fail")
}

pub(crate) fn encode_key_ref(key: TrackedStateKeyRef<'_>) -> Vec<u8> {
    storage_codec::encode("tracked-state key", &key)
        .expect("tracked-state key storage encoding should not fail")
}

pub(crate) fn encode_schema_key_prefix(schema_key: &str) -> Vec<u8> {
    storage_codec::encode(
        "tracked-state schema key prefix",
        &TrackedSchemaKeyPrefixRef { schema_key },
    )
    .expect("tracked-state schema key prefix storage encoding should not fail")
}

pub(crate) fn encode_schema_file_prefix(schema_key: &str, file_id: Option<&str>) -> Vec<u8> {
    storage_codec::encode(
        "tracked-state schema/file prefix",
        &TrackedSchemaFilePrefixRef {
            schema_key,
            file_id,
        },
    )
    .expect("tracked-state schema/file prefix storage encoding should not fail")
}

pub(crate) fn decode_key(bytes: &[u8]) -> Result<TrackedStateKey, LixError> {
    storage_codec::decode("tracked-state key", bytes)
}

/// Decodes a key after the caller has already proven the schema/file prefix.
///
/// This is for scan paths that have matched an encoded prefix range and only
/// need to materialize the entity suffix plus the selected columns.
pub(crate) fn decode_key_with_trusted_prefix(
    bytes: &[u8],
    schema_key: &str,
    file_id: Option<&str>,
    prefix_len: usize,
) -> Result<TrackedStateKey, LixError> {
    let suffix = bytes.get(prefix_len..).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state tree trusted key prefix is longer than encoded key",
        )
    })?;
    let entity_pk = storage_codec::decode("tracked-state key entity primary key", suffix)?;
    Ok(TrackedStateKey {
        schema_key: schema_key.to_string(),
        file_id: file_id.map(str::to_string),
        entity_pk,
    })
}

#[cfg(test)]
pub(crate) fn encode_value(value: &TrackedStateIndexValue) -> Vec<u8> {
    storage_codec::encode("tracked-state value", value)
        .expect("tracked-state value storage encoding should not fail")
}

pub(crate) fn encode_value_ref(value: TrackedStateIndexValueRef) -> Vec<u8> {
    storage_codec::encode("tracked-state value", &value)
        .expect("tracked-state value storage encoding should not fail")
}

#[cfg(test)]
pub(crate) fn encoded_value_len(value: &TrackedStateIndexValue) -> usize {
    encode_value(value).len()
}

pub(crate) fn decode_value(bytes: &[u8]) -> Result<TrackedStateIndexValue, LixError> {
    decode_value_view(bytes).map(tracked_value_from_storage)
}

pub(crate) fn decode_visible_value(
    bytes: &[u8],
    include_tombstones: bool,
) -> Result<Option<TrackedStateIndexValue>, LixError> {
    let view = decode_value_view(bytes)?;
    if view.deleted && !include_tombstones {
        return Ok(None);
    }
    Ok(Some(tracked_value_from_storage(view)))
}

fn decode_value_view(bytes: &[u8]) -> Result<TrackedStateIndexValueRef, LixError> {
    storage_codec::decode("tracked-state value", bytes)
}

fn tracked_value_from_storage(value: TrackedStateIndexValueRef) -> TrackedStateIndexValue {
    let TrackedStateIndexValueRef {
        change_id,
        commit_id,
        deleted,
        snapshot_ref,
        metadata_ref,
        created_at,
        updated_at,
    } = value;
    TrackedStateIndexValue {
        change_id,
        commit_id,
        deleted,
        snapshot_ref,
        metadata_ref,
        created_at,
        updated_at,
    }
}

pub(crate) fn encode_leaf_node(entries: &[EncodedLeafEntry]) -> Vec<u8> {
    let entries = entries
        .iter()
        .map(EncodedLeafEntry::as_ref)
        .collect::<Vec<_>>();
    encode_leaf_node_refs(&entries)
}

/// Leaf node wire format (v2):
///
/// ```text
/// [NODE_KIND_LEAF_V2]
/// varint entry_count
/// per entry:
///   varint shared_key_len   bytes shared with the previous entry's key
///   varint key_suffix_len ++ key suffix bytes
///   varint value_len ++ value bytes (verbatim standard value encoding)
/// ```
///
/// Entries within a node are sorted by key, so consecutive keys share the
/// encoded schema-key/file-id prefix and most of the entity-pk; front-coding
/// removes that redundancy. Values are untouched: byte equality and the
/// value codec stay exactly as before. Sortedness is a caller invariant
/// (the tree builder always produces sorted entries); it is asserted in
/// debug builds but not enforced in release, where unsorted input would
/// round-trip faithfully and only confuse downstream binary search.
pub(crate) fn encode_leaf_node_refs(entries: &[EncodedLeafEntryRef<'_>]) -> Vec<u8> {
    debug_assert!(
        entries.windows(2).all(|pair| pair[0].key < pair[1].key),
        "leaf entries must be strictly sorted by key"
    );
    let mut out = Vec::with_capacity(64 + entries.len() * 24);
    out.push(NODE_KIND_LEAF_V2);
    write_varint(&mut out, entries.len() as u64);
    let mut previous_key: &[u8] = &[];
    for entry in entries {
        let shared = shared_prefix_len(previous_key, entry.key);
        write_varint(&mut out, shared as u64);
        write_varint(&mut out, (entry.key.len() - shared) as u64);
        out.extend_from_slice(&entry.key[shared..]);
        write_varint(&mut out, entry.value.len() as u64);
        out.extend_from_slice(entry.value);
        previous_key = entry.key;
    }
    #[cfg(debug_assertions)]
    verify_leaf_round_trip(&out, entries);
    out
}

#[cfg(debug_assertions)]
fn verify_leaf_round_trip(encoded: &[u8], entries: &[EncodedLeafEntryRef<'_>]) {
    let decoded = match decode_node_ref(encoded) {
        Ok(DecodedNodeRef::Leaf(leaf)) => leaf,
        other => panic!("leaf round trip decoded unexpectedly: {other:?}"),
    };
    assert_eq!(decoded.len(), entries.len(), "leaf round trip entry count");
    for (index, entry) in entries.iter().enumerate() {
        let round_tripped = decoded
            .entry(index)
            .expect("leaf round trip entry should read")
            .expect("leaf round trip entry should exist");
        assert_eq!(round_tripped.key, entry.key, "leaf round trip key {index}");
        assert_eq!(
            round_tripped.value, entry.value,
            "leaf round trip value {index}"
        );
    }
}

fn decode_leaf_v2(body: &[u8]) -> Result<DecodedLeafNodeRef<'_>, LixError> {
    fn usize_from(value: u64, what: &str) -> Result<usize, LixError> {
        usize::try_from(value).map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("tracked-state leaf node {what} does not fit in usize"),
            )
        })
    }
    fn slice<'b>(body: &'b [u8], offset: &mut usize, len: usize) -> Result<&'b [u8], LixError> {
        let end = offset.checked_add(len).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state leaf node length overflow",
            )
        })?;
        let bytes = body.get(*offset..end).ok_or_else(|| {
            LixError::new("LIX_ERROR_UNKNOWN", "tracked-state leaf node is truncated")
        })?;
        *offset = end;
        Ok(bytes)
    }

    let mut offset = 0usize;
    let entry_count = usize_from(read_varint(body, &mut offset)?, "entry count")?;
    // Reconstructed keys total the suffix bytes plus every re-expanded
    // shared prefix, so heavy sharing can exceed the body length; the body
    // length is a cheap reservation that avoids re-allocation for typical
    // value-dominated chunks.
    let mut key_arena = Vec::with_capacity(body.len());
    let mut entries = Vec::with_capacity(entry_count.min(body.len()));
    let mut previous_key_start = 0usize;
    for _ in 0..entry_count {
        let shared = usize_from(read_varint(body, &mut offset)?, "shared key length")?;
        let suffix_len = usize_from(read_varint(body, &mut offset)?, "key suffix length")?;
        let previous_key_len = key_arena.len() - previous_key_start;
        if shared > previous_key_len {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state leaf node shares more key bytes than the previous key holds",
            ));
        }
        let key_start = key_arena.len();
        key_arena.extend_from_within(previous_key_start..previous_key_start + shared);
        let suffix = slice(body, &mut offset, suffix_len)?;
        key_arena.extend_from_slice(suffix);
        let value_len = usize_from(read_varint(body, &mut offset)?, "value length")?;
        let value = slice(body, &mut offset, value_len)?;
        entries.push(LeafEntrySpan {
            key_start,
            key_end: key_arena.len(),
            value,
        });
        previous_key_start = key_start;
    }
    if offset != body.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state leaf node has trailing bytes",
        ));
    }
    Ok(DecodedLeafNodeRef { key_arena, entries })
}

fn shared_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right.iter())
        .take_while(|(a, b)| a == b)
        .count()
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn read_varint(bytes: &[u8], offset: &mut usize) -> Result<u64, LixError> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*offset).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state leaf node varint is truncated",
            )
        })?;
        *offset += 1;
        if shift >= 64 || (shift == 63 && byte > 1) {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state leaf node varint overflows u64",
            ));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

pub(crate) fn encode_internal_node(children: &[ChildSummary]) -> Vec<u8> {
    let children = children
        .iter()
        .map(ChildSummary::as_ref)
        .collect::<Vec<_>>();
    encode_internal_node_refs(&children)
}

pub(crate) fn encode_internal_node_refs(children: &[ChildSummaryRef<'_>]) -> Vec<u8> {
    let mut out = vec![NODE_KIND_INTERNAL];
    out.extend_from_slice(
        &storage_codec::encode(
            "tracked-state internal node",
            &StorageInternalNode {
                children: children.to_vec(),
            },
        )
        .expect("tracked-state internal node storage encoding should not fail"),
    );
    out
}

pub(crate) fn decode_node(bytes: &[u8]) -> Result<DecodedNode, LixError> {
    match decode_node_ref(bytes)? {
        DecodedNodeRef::Leaf(leaf) => {
            let mut entries = Vec::with_capacity(leaf.len());
            for index in 0..leaf.len() {
                let entry = leaf.entry(index)?.ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "tracked-state leaf entry disappeared during owned decode",
                    )
                })?;
                entries.push(EncodedLeafEntry {
                    key: entry.key.to_vec(),
                    value: entry.value.to_vec(),
                });
            }
            Ok(DecodedNode::Leaf(DecodedLeafNode { entries }))
        }
        DecodedNodeRef::Internal(internal) => Ok(DecodedNode::Internal(internal)),
    }
}

pub(crate) fn decode_node_ref(bytes: &[u8]) -> Result<DecodedNodeRef<'_>, LixError> {
    let (&kind, body) = bytes
        .split_first()
        .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "tracked-state tree node is empty"))?;
    match kind {
        NODE_KIND_LEAF_V2 => Ok(DecodedNodeRef::Leaf(decode_leaf_v2(body)?)),
        NODE_KIND_INTERNAL => {
            let node: StorageInternalNode<'_> =
                storage_codec::decode("tracked-state internal node", body)?;
            Ok(DecodedNodeRef::Internal(DecodedInternalNode {
                children: node
                    .children
                    .into_iter()
                    .map(|child| ChildSummary {
                        first_key: child.first_key.to_vec(),
                        last_key: child.last_key.to_vec(),
                        child_hash: child.child_hash,
                        subtree_count: child.subtree_count,
                    })
                    .collect(),
            }))
        }
        other => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state tree node has unknown kind byte {other}"),
        )),
    }
}

pub(crate) fn child_summary_from_node(
    node_bytes: Vec<u8>,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    subtree_count: u64,
) -> (PendingChunkWrite, ChildSummary) {
    let hash = hash_bytes(&node_bytes);
    (
        PendingChunkWrite {
            hash,
            data: node_bytes,
        },
        ChildSummary {
            first_key,
            last_key,
            child_hash: hash,
            subtree_count,
        },
    )
}

#[expect(clippy::cast_precision_loss)]
pub(crate) fn boundary_trigger(
    encoded_key: &[u8],
    level: usize,
    chunk_size: usize,
    item_size: usize,
    target_chunk_bytes: usize,
) -> bool {
    if item_size == 0 || target_chunk_bytes == 0 {
        return false;
    }

    let start =
        weibull_cdf(chunk_size.saturating_sub(item_size) as f64 / target_chunk_bytes as f64);
    let end = weibull_cdf(chunk_size as f64 / target_chunk_bytes as f64);
    let remaining = 1.0 - start;
    if remaining <= 0.0 {
        return true;
    }

    let split_probability = ((end - start) / remaining).clamp(0.0, 1.0);
    let hash = xxh3_64_with_seed(encoded_key, level_salt(level));
    (hash as f64) < split_probability * (u64::MAX as f64)
}

fn weibull_cdf(normalized_size: f64) -> f64 {
    if normalized_size <= 0.0 {
        return 0.0;
    }
    -f64::exp_m1(-normalized_size.powi(WEIBULL_K))
}

fn level_salt(level: usize) -> u64 {
    let mut value = (level as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::{
        DecodedNodeRef as NodeRefForLeafTests, decode_node_ref as decode_node_ref_for_leaf_tests,
        encode_leaf_node_refs as encode_leaf_refs_for_tests,
    };

    fn leaf_entries_round_trip(entries: &[(Vec<u8>, Vec<u8>)]) {
        let refs = entries
            .iter()
            .map(|(key, value)| EncodedLeafEntryRef {
                key: key.as_slice(),
                value: value.as_slice(),
            })
            .collect::<Vec<_>>();
        let encoded = encode_leaf_refs_for_tests(&refs);
        let NodeRefForLeafTests::Leaf(decoded) =
            decode_node_ref_for_leaf_tests(&encoded).expect("leaf should decode")
        else {
            panic!("leaf encoded bytes decoded as non-leaf");
        };
        assert_eq!(decoded.len(), entries.len());
        for (index, (key, value)) in entries.iter().enumerate() {
            let entry = decoded
                .entry(index)
                .expect("entry should read")
                .expect("entry should exist");
            assert_eq!(entry.key, key.as_slice(), "key {index}");
            assert_eq!(entry.value, value.as_slice(), "value {index}");
        }
    }

    #[test]
    fn leaf_v2_round_trips_representative_shapes() {
        leaf_entries_round_trip(&[]);
        leaf_entries_round_trip(&[(b"only".to_vec(), b"value".to_vec())]);
        leaf_entries_round_trip(&[
            (b"a".to_vec(), Vec::new()),
            (b"ab".to_vec(), vec![0u8; 300]),
            (b"abc/0001".to_vec(), b"x".to_vec()),
            (b"abc/0002".to_vec(), b"y".to_vec()),
            (b"zzz".to_vec(), vec![0xff; 7]),
        ]);
        // No shared prefixes at all (front-coding worst case).
        leaf_entries_round_trip(&[
            (vec![0x00], b"a".to_vec()),
            (vec![0x80, 0x01], b"b".to_vec()),
            (vec![0xff, 0xff, 0xff], b"c".to_vec()),
        ]);
    }

    #[test]
    fn leaf_v2_round_trips_generated_sorted_keys() {
        // Deterministic pseudo-random keys with heavy shared prefixes,
        // mimicking encoded (schema_key, file_id, entity_pk) keys.
        let mut entries = (0..512usize)
            .map(|index| {
                let mut state = (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
                state ^= state >> 31;
                let key = format!(
                    "json_pointer\u{0}/packages/{:04}/{}",
                    index % 7,
                    state % 1000
                )
                .into_bytes();
                let value = state.to_be_bytes().repeat(1 + (index % 5));
                (key, value)
            })
            .collect::<Vec<_>>();
        entries.sort();
        entries.dedup_by(|a, b| a.0 == b.0);
        leaf_entries_round_trip(&entries);
    }

    #[test]
    fn leaf_v2_round_trips_multibyte_varint_fields() {
        // Keys long enough that shared and suffix lengths need two-byte
        // varints, with >127 entries so the count does too.
        let mut entries = Vec::new();
        let prefix = "p".repeat(160);
        for index in 0..200usize {
            let key = format!("{prefix}/{index:05}").into_bytes();
            let value = vec![index.to_le_bytes()[0]; 130];
            entries.push((key, value));
        }
        entries.sort();
        leaf_entries_round_trip(&entries);
    }

    /// Pins the exact wire bytes. Tree chunks are content-addressed, so any
    /// accidental format drift changes chunk hashes; this golden test makes
    /// such drift fail a unit test instead of only surfacing in storage.
    #[test]
    fn leaf_v2_wire_format_is_pinned() {
        let entries = [
            (b"shared/aaaa".to_vec(), b"v1".to_vec()),
            (b"shared/aabb".to_vec(), b"v2".to_vec()),
            (b"shared/bbbb".to_vec(), b"v3".to_vec()),
        ];
        let refs = entries
            .iter()
            .map(|(key, value)| EncodedLeafEntryRef {
                key: key.as_slice(),
                value: value.as_slice(),
            })
            .collect::<Vec<_>>();
        let encoded = encode_leaf_refs_for_tests(&refs);
        let expected: &[u8] = &[
            1, // NODE_KIND_LEAF_V2
            3, // entry count
            0, 11, b's', b'h', b'a', b'r', b'e', b'd', b'/', b'a', b'a', b'a', b'a', 2, b'v',
            b'1', // entry 0: shared=0, suffix "shared/aaaa", value "v1"
            9, 2, b'b', b'b', 2, b'v', b'2', // entry 1: shared=9, suffix "bb"
            7, 4, b'b', b'b', b'b', b'b', 2, b'v', b'3', // entry 2: shared=7, suffix "bbbb"
        ];
        assert_eq!(encoded, expected, "leaf v2 wire bytes must stay stable");
    }

    #[test]
    fn leaf_v2_rejects_malformed_bytes() {
        let entries = [(b"key-a".to_vec(), b"value-a".to_vec())];
        let encoded = encode_leaf_refs_for_tests(
            &entries
                .iter()
                .map(|(key, value)| EncodedLeafEntryRef {
                    key: key.as_slice(),
                    value: value.as_slice(),
                })
                .collect::<Vec<_>>(),
        );

        let mut unknown_kind = encoded.clone();
        unknown_kind[0] = 0x7f;
        assert!(decode_node_ref_for_leaf_tests(&unknown_kind).is_err());

        let truncated = &encoded[..encoded.len() - 1];
        assert!(decode_node_ref_for_leaf_tests(truncated).is_err());

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(decode_node_ref_for_leaf_tests(&trailing).is_err());

        // shared_len larger than the previous key reconstructs nothing valid.
        let mut bogus_share = vec![NODE_KIND_LEAF_V2];
        bogus_share.extend_from_slice(&[1, 9, 1, b'k', 1, b'v']);
        assert!(decode_node_ref_for_leaf_tests(&bogus_share).is_err());

        assert!(decode_node_ref_for_leaf_tests(&[]).is_err());
    }

    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;

    fn timestamp(field: &str, value: &str) -> LixTimestamp {
        LixTimestamp::expect_parse(field, value)
    }

    fn test_value(commit_id: &str, change_id: &str) -> TrackedStateIndexValue {
        TrackedStateIndexValue {
            change_id: ChangeId::for_test_label(change_id),
            commit_id: CommitId::for_test_label(commit_id),
            deleted: false,
            snapshot_ref: None,
            metadata_ref: None,
            created_at: timestamp("created_at", "2026-01-01T00:00:00Z"),
            updated_at: timestamp("updated_at", "2026-01-02T00:00:00Z"),
        }
    }

    fn set_timestamps(value: &mut TrackedStateIndexValue, created_at: &str, updated_at: &str) {
        value.created_at = timestamp("created_at", created_at);
        value.updated_at = timestamp("updated_at", updated_at);
    }

    #[test]
    fn key_codec_distinguishes_null_and_value_file_id() {
        let null_key = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk::single("entity"),
        });
        let file_key = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: Some("file".to_string()),
            entity_pk: EntityPk::single("entity"),
        });

        assert_ne!(null_key, file_key);
        assert_eq!(
            decode_key(&null_key).expect("null key"),
            TrackedStateKey {
                schema_key: "schema".to_string(),
                file_id: None,
                entity_pk: EntityPk::single("entity"),
            }
        );
        assert_eq!(
            decode_key(&file_key).expect("file key"),
            TrackedStateKey {
                schema_key: "schema".to_string(),
                file_id: Some("file".to_string()),
                entity_pk: EntityPk::single("entity"),
            }
        );
    }

    #[test]
    fn key_codec_encodes_composite_identity_as_string_tuple_parts() {
        let key = TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk {
                parts: vec![
                    "namespace".to_string(),
                    "true".to_string(),
                    "42".to_string(),
                ],
            },
        };

        let encoded = encode_key(&key);

        assert_eq!(decode_key(&encoded).expect("key should decode"), key);
    }

    #[test]
    fn key_codec_decodes_entity_suffix_with_trusted_prefix() {
        let key = TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: Some("file".to_string()),
            entity_pk: EntityPk {
                parts: vec!["namespace".to_string(), "id".to_string()],
            },
        };
        let encoded = encode_key(&key);
        let prefix = encode_schema_file_prefix("schema", Some("file"));

        assert_eq!(
            decode_key_with_trusted_prefix(&encoded, "schema", Some("file"), prefix.len())
                .expect("key suffix should decode"),
            key
        );
    }

    #[test]
    fn key_codec_rejects_malformed_storage_bytes() {
        let mut encoded = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk {
                parts: vec!["true".to_string()],
            },
        });
        encoded.truncate(encoded.len() - 1);

        let error = decode_key(&encoded).expect_err("truncated key should reject");
        assert!(
            error
                .to_string()
                .contains("failed to decode tracked-state key")
        );
    }

    #[test]
    fn key_codec_rejects_empty_entity_pk() {
        let encoded = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk { parts: Vec::new() },
        });

        let error = decode_key(&encoded).expect_err("empty entity pk should reject");

        assert!(
            error
                .message
                .contains("entity primary key decoded from storage is invalid")
        );
    }

    #[test]
    fn key_codec_preserves_tuple_prefix_ordering() {
        let prefix = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk {
                parts: vec!["a".to_string()],
            },
        });
        let extended = encode_key(&TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: None,
            entity_pk: EntityPk {
                parts: vec!["a".to_string(), "b".to_string()],
            },
        });

        assert!(prefix < extended);
    }

    #[test]
    fn value_codec_roundtrips_change_ref_value() {
        let value = TrackedStateIndexValue {
            change_id: ChangeId::for_test_label("change"),
            commit_id: CommitId::for_test_label("commit"),
            deleted: false,
            snapshot_ref: Some(JsonRef::from_hash_bytes([1; 32])),
            metadata_ref: Some(JsonRef::from_hash_bytes([2; 32])),
            created_at: timestamp("created_at", "2026-01-01T00:00:00Z"),
            updated_at: timestamp("updated_at", "2026-01-02T00:00:00Z"),
        };

        let encoded = encode_value(&value);
        assert_eq!(decode_value(&encoded).expect("value"), value);
    }

    #[test]
    fn value_codec_roundtrips_second_change_ref_value() {
        let value = TrackedStateIndexValue {
            change_id: ChangeId::for_test_label("other-change"),
            commit_id: CommitId::for_test_label("other-commit"),
            deleted: true,
            snapshot_ref: None,
            metadata_ref: None,
            created_at: timestamp("created_at", "2026-01-01T00:00:00Z"),
            updated_at: timestamp("updated_at", "2026-01-02T00:00:00Z"),
        };

        let encoded = encode_value(&value);
        assert_eq!(decode_value(&encoded).expect("value"), value);
    }

    #[test]
    fn value_codec_stores_fixed_width_timestamps() {
        let mut matching = test_value("commit", "change");
        set_timestamps(
            &mut matching,
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        );
        let matching_len = encode_value(&matching).len();
        assert_eq!(
            decode_value(&encode_value(&matching)).expect("value"),
            matching
        );

        set_timestamps(
            &mut matching,
            "2026-01-01T00:00:00Z",
            "2026-01-02T00:00:00Z",
        );
        let distinct_len = encode_value(&matching).len();

        assert_eq!(matching_len, distinct_len);
    }

    #[test]
    fn owned_value_codec_matches_borrowed_value_codec() {
        let mut compact = test_value("commit", "change");
        set_timestamps(&mut compact, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z");

        let compact_owned = storage_codec::encode("tracked-state owned value", &compact)
            .expect("owned value should encode");
        assert_eq!(compact_owned, encode_value(&compact));
        let compact_decoded: TrackedStateIndexValue =
            storage_codec::decode("tracked-state owned value", &compact_owned)
                .expect("owned value should decode");
        assert_eq!(compact_decoded, compact);

        let mut distinct = compact.clone();
        set_timestamps(
            &mut distinct,
            "2026-01-01T00:00:00Z",
            "2026-01-02T00:00:00Z",
        );

        let distinct_owned = storage_codec::encode("tracked-state owned value", &distinct)
            .expect("owned value should encode");
        assert_eq!(distinct_owned, encode_value(&distinct));
        let distinct_decoded: TrackedStateIndexValue =
            storage_codec::decode("tracked-state owned value", &distinct_owned)
                .expect("owned value should decode");
        assert_eq!(distinct_decoded, distinct);
    }

    #[test]
    fn encoded_value_len_matches_encoded_value_bytes() {
        let values = [
            TrackedStateIndexValue {
                change_id: ChangeId::for_test_label("change"),
                commit_id: CommitId::for_test_label("commit"),
                deleted: false,
                snapshot_ref: None,
                metadata_ref: None,
                created_at: timestamp("created_at", "2026-01-01T00:00:00Z"),
                updated_at: timestamp("updated_at", "2026-01-02T00:00:00Z"),
            },
            TrackedStateIndexValue {
                change_id: ChangeId::for_test_label("change-2"),
                commit_id: CommitId::for_test_label("commit"),
                deleted: true,
                snapshot_ref: Some(JsonRef::from_hash_bytes([3; 32])),
                metadata_ref: None,
                created_at: timestamp("created_at", "2026-01-01T00:00:00Z"),
                updated_at: timestamp("updated_at", "2026-01-02T00:00:00Z"),
            },
            TrackedStateIndexValue {
                change_id: ChangeId::for_test_label("change-3"),
                commit_id: CommitId::for_test_label("other"),
                deleted: false,
                snapshot_ref: None,
                metadata_ref: Some(JsonRef::from_hash_bytes([4; 32])),
                created_at: timestamp("created_at", "2026-01-01T00:00:00Z"),
                updated_at: timestamp("updated_at", "2026-01-02T00:00:00Z"),
            },
        ];

        for value in values {
            assert_eq!(encoded_value_len(&value), encode_value(&value).len());
        }
    }

    #[test]
    fn leaf_node_codec_roundtrips_borrowed_entries() {
        let entries = vec![
            EncodedLeafEntry {
                key: b"alpha".to_vec(),
                value: b"one".to_vec(),
            },
            EncodedLeafEntry {
                key: b"bravo".to_vec(),
                value: b"two-two".to_vec(),
            },
        ];

        let encoded = encode_leaf_node(&entries);
        let DecodedNodeRef::Leaf(leaf) = decode_node_ref(&encoded).expect("leaf ref") else {
            panic!("expected leaf node");
        };
        assert_eq!(leaf.len(), 2);
        assert_eq!(leaf.key(1).expect("second key"), Some(b"bravo".as_slice()));
        let second = leaf
            .entry(1)
            .expect("second entry")
            .expect("second entry exists");
        assert_eq!(second.key, b"bravo");
        assert_eq!(second.value, b"two-two");

        let DecodedNode::Leaf(owned) = decode_node(&encoded).expect("owned leaf") else {
            panic!("expected owned leaf node");
        };
        assert_eq!(owned.entries(), entries.as_slice());
    }

    #[test]
    fn leaf_node_codec_roundtrips_empty_leaf() {
        let encoded = encode_leaf_node(&[]);

        let DecodedNodeRef::Leaf(leaf) = decode_node_ref(&encoded).expect("leaf ref") else {
            panic!("expected leaf node");
        };
        assert_eq!(leaf.len(), 0);
        assert!(leaf.entry(0).expect("missing entry").is_none());
    }

    #[test]
    fn leaf_node_codec_rejects_malformed_storage_bytes() {
        let entries = vec![
            EncodedLeafEntry {
                key: b"alpha".to_vec(),
                value: b"one".to_vec(),
            },
            EncodedLeafEntry {
                key: b"bravo".to_vec(),
                value: b"two".to_vec(),
            },
        ];
        let mut encoded = encode_leaf_node(&entries);
        encoded.truncate(encoded.len() - 1);

        let error = decode_node_ref(&encoded).expect_err("truncated leaf should reject");

        assert!(
            error.to_string().contains("tracked-state leaf node"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn content_hash_is_blake3() {
        assert_eq!(hash_bytes(b"abc"), *blake3::hash(b"abc").as_bytes());
    }

    #[test]
    fn boundary_decisions_are_xxh3_based_and_deterministic() {
        let left = boundary_trigger(b"key", 0, 4096, 128, 4096);
        let right = boundary_trigger(b"key", 0, 4096, 128, 4096);
        assert_eq!(left, right);
    }
}
