//! Authenticated append-only semantic-history storage.
//!
//! The mutable root is only a selector/CAS fence.  Its content-addressed
//! directory nodes form a persistent ordered fanout tree whose leaves are
//! bounded semantic-history segments.  A mutation copies the paths to its
//! touched leaves; untouched leaves and subtrees are reused by digest.
//!
//! The directory coordinate is `(record kind, UUID bytes)`. Commit chronology
//! remains the authenticated parent/generation relation in each commit record;
//! it is never inferred from this identity-order coordinate or duplicated as a
//! second physical index.

use crate::LixError;
use crate::storage_codec;

pub(crate) const FORMAT_VERSION: u8 = 2;
pub(crate) const ROOT_KEY: &[u8] = b"\0semantic-history-root";
pub(crate) const LEAF_KEY_PREFIX: u8 = 1;
pub(crate) const NODE_KEY_PREFIX: u8 = 2;
pub(crate) const COMMIT_RECORD_KIND: u8 = 1;
pub(crate) const CHANGE_RECORD_KIND: u8 = 2;
pub(crate) const MEMBERSHIP_RECORD_KIND: u8 = 3;
pub(crate) const DIRECTORY_FANOUT: usize = 32;
pub(crate) const LEAF_MAX_RECORDS: usize = 128;
pub(crate) const LEAF_MAX_BYTES: usize = 64 * 1024;
pub(crate) const HISTORY_KEY_BYTES: usize = 17;

#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct ChildSelector {
    pub(crate) first: [u8; HISTORY_KEY_BYTES],
    pub(crate) last: [u8; HISTORY_KEY_BYTES],
    pub(crate) key: Vec<u8>,
    pub(crate) record_count: u32,
    pub(crate) byte_len: u32,
    pub(crate) digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct SemanticHistoryRoot {
    pub(crate) format_version: u8,
    pub(crate) generation: u64,
    pub(crate) target: Option<ChildSelector>,
    pub(crate) record_count: u64,
    pub(crate) digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct SemanticHistoryDirectory {
    pub(crate) format_version: u8,
    pub(crate) level: u8,
    pub(crate) children: Vec<ChildSelector>,
    pub(crate) digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct SemanticHistoryRecord {
    pub(crate) kind: u8,
    pub(crate) id: [u8; 16],
    pub(crate) value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct SemanticHistorySegment {
    pub(crate) format_version: u8,
    pub(crate) generation: u64,
    pub(crate) records: Vec<SemanticHistoryRecord>,
    pub(crate) min_key: [u8; HISTORY_KEY_BYTES],
    pub(crate) max_key: [u8; HISTORY_KEY_BYTES],
    pub(crate) digest: [u8; 32],
}

impl SemanticHistoryRoot {
    pub(crate) fn empty() -> Self {
        Self {
            format_version: FORMAT_VERSION,
            generation: 0,
            target: None,
            record_count: 0,
            digest: [0; 32],
        }
    }

    pub(crate) fn seal(mut self) -> Result<Self, LixError> {
        self.validate_without_digest()?;
        self.digest = root_digest(&self)?;
        Ok(self)
    }

    pub(crate) fn validate(&self) -> Result<(), LixError> {
        self.validate_without_digest()?;
        if root_digest(self)? != self.digest {
            return Err(corruption("semantic-history root digest"));
        }
        Ok(())
    }

    fn validate_without_digest(&self) -> Result<(), LixError> {
        if self.format_version != FORMAT_VERSION {
            return Err(corruption("semantic-history root format"));
        }
        if self.target.is_none() && self.record_count != 0 {
            return Err(corruption("semantic-history empty root count"));
        }
        if let Some(target) = &self.target {
            validate_selector(target, NODE_KEY_PREFIX)?;
            if target.record_count as u64 != self.record_count {
                return Err(corruption("semantic-history root count"));
            }
        }
        Ok(())
    }
}

impl SemanticHistoryDirectory {
    pub(crate) fn seal(level: u8, children: Vec<ChildSelector>) -> Result<Self, LixError> {
        if children.is_empty() || children.len() > DIRECTORY_FANOUT {
            return Err(corruption("semantic-history directory fanout"));
        }
        validate_children(&children)?;
        let expected = if level == 0 {
            LEAF_KEY_PREFIX
        } else {
            NODE_KEY_PREFIX
        };
        if children.iter().any(|child| child.key[0] != expected) {
            return Err(corruption("semantic-history directory child level"));
        }
        let mut node = Self {
            format_version: FORMAT_VERSION,
            level,
            children,
            digest: [0; 32],
        };
        node.digest = directory_digest(&node)?;
        Ok(node)
    }

    pub(crate) fn validate(&self) -> Result<(), LixError> {
        if self.format_version != FORMAT_VERSION {
            return Err(corruption("semantic-history directory format"));
        }
        if self.children.is_empty() || self.children.len() > DIRECTORY_FANOUT {
            return Err(corruption("semantic-history directory fanout"));
        }
        validate_children(&self.children)?;
        let expected = if self.level == 0 {
            LEAF_KEY_PREFIX
        } else {
            NODE_KEY_PREFIX
        };
        if self.children.iter().any(|child| child.key[0] != expected) {
            return Err(corruption("semantic-history directory child level"));
        }
        if directory_digest(self)? != self.digest {
            return Err(corruption("semantic-history directory digest"));
        }
        Ok(())
    }
}

impl SemanticHistorySegment {
    pub(crate) fn seal(
        generation: u64,
        mut records: Vec<SemanticHistoryRecord>,
    ) -> Result<Self, LixError> {
        records.sort_by_key(record_sort_key);
        if records.len() > LEAF_MAX_RECORDS || records.is_empty() {
            return Err(corruption("semantic-history leaf record bound"));
        }
        for pair in records.windows(2) {
            if record_sort_key(&pair[0]) >= record_sort_key(&pair[1]) {
                return Err(corruption("semantic-history duplicate or unordered record"));
            }
        }
        let min_key = record_sort_key(&records[0]);
        let max_key = record_sort_key(records.last().expect("non-empty leaf"));
        let mut segment = Self {
            format_version: FORMAT_VERSION,
            generation,
            records,
            min_key,
            max_key,
            digest: [0; 32],
        };
        if encoded_size(&segment)? > LEAF_MAX_BYTES {
            return Err(corruption("semantic-history leaf byte bound"));
        }
        segment.digest = segment_digest(&segment)?;
        Ok(segment)
    }

    pub(crate) fn validate(&self) -> Result<(), LixError> {
        if self.format_version != FORMAT_VERSION || self.records.is_empty() {
            return Err(corruption("semantic-history leaf format"));
        }
        if self.records.len() > LEAF_MAX_RECORDS
            || record_sort_key(&self.records[0]) != self.min_key
            || record_sort_key(self.records.last().expect("non-empty leaf")) != self.max_key
        {
            return Err(corruption("semantic-history leaf bounds"));
        }
        for record in &self.records {
            if !matches!(
                record.kind,
                COMMIT_RECORD_KIND | CHANGE_RECORD_KIND | MEMBERSHIP_RECORD_KIND
            ) {
                return Err(corruption("semantic-history leaf record kind"));
            }
        }
        for pair in self.records.windows(2) {
            if record_sort_key(&pair[0]) >= record_sort_key(&pair[1]) {
                return Err(corruption("semantic-history leaf ordering"));
            }
        }
        if encoded_size(self)? > LEAF_MAX_BYTES || segment_digest(self)? != self.digest {
            return Err(corruption("semantic-history leaf digest or size"));
        }
        Ok(())
    }
}

pub(crate) fn record_sort_key(record: &SemanticHistoryRecord) -> [u8; HISTORY_KEY_BYTES] {
    let mut key = [0; HISTORY_KEY_BYTES];
    key[0] = record.kind;
    key[1..].copy_from_slice(&record.id);
    key
}

pub(crate) fn record_key(kind: u8, id: [u8; 16]) -> [u8; HISTORY_KEY_BYTES] {
    let mut key = [0; HISTORY_KEY_BYTES];
    key[0] = kind;
    key[1..].copy_from_slice(&id);
    key
}

pub(crate) fn leaf_key(digest: [u8; 32]) -> Vec<u8> {
    content_key(LEAF_KEY_PREFIX, digest)
}

pub(crate) fn node_key(digest: [u8; 32]) -> Vec<u8> {
    content_key(NODE_KEY_PREFIX, digest)
}

pub(crate) fn content_key(prefix: u8, digest: [u8; 32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(33);
    key.push(prefix);
    key.extend_from_slice(&digest);
    key
}

pub(crate) fn encode_root(root: &SemanticHistoryRoot) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("semantic-history root", root)
}

pub(crate) fn decode_root(bytes: &[u8]) -> Result<SemanticHistoryRoot, LixError> {
    let root: SemanticHistoryRoot = storage_codec::decode("semantic-history root", bytes)?;
    root.validate()?;
    Ok(root)
}

pub(crate) fn encode_directory(node: &SemanticHistoryDirectory) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("semantic-history directory", node)
}

pub(crate) fn decode_directory(bytes: &[u8]) -> Result<SemanticHistoryDirectory, LixError> {
    let node: SemanticHistoryDirectory =
        storage_codec::decode("semantic-history directory", bytes)?;
    node.validate()?;
    Ok(node)
}

pub(crate) fn encode_segment(segment: &SemanticHistorySegment) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("semantic-history leaf", segment)
}

pub(crate) fn decode_segment(bytes: &[u8]) -> Result<SemanticHistorySegment, LixError> {
    let segment: SemanticHistorySegment = storage_codec::decode("semantic-history leaf", bytes)?;
    segment.validate()?;
    Ok(segment)
}

pub(crate) fn selector_for_leaf(
    segment: &SemanticHistorySegment,
) -> Result<ChildSelector, LixError> {
    Ok(ChildSelector {
        first: segment.min_key,
        last: segment.max_key,
        key: leaf_key(segment.digest),
        record_count: u32::try_from(segment.records.len())
            .map_err(|_| corruption("semantic-history leaf count overflow"))?,
        byte_len: u32::try_from(encoded_size(segment)?)
            .map_err(|_| corruption("semantic-history leaf byte count overflow"))?,
        digest: segment.digest,
    })
}

pub(crate) fn selector_for_node(
    node: &SemanticHistoryDirectory,
) -> Result<ChildSelector, LixError> {
    let first = node
        .children
        .first()
        .expect("sealed node has children")
        .first;
    let last = node.children.last().expect("sealed node has children").last;
    let record_count = node
        .children
        .iter()
        .try_fold(0u64, |sum, child| {
            sum.checked_add(child.record_count as u64)
        })
        .ok_or_else(|| corruption("semantic-history node count overflow"))?;
    let byte_len = node
        .children
        .iter()
        .try_fold(0u64, |sum, child| sum.checked_add(child.byte_len as u64))
        .ok_or_else(|| corruption("semantic-history node byte overflow"))?;
    Ok(ChildSelector {
        first,
        last,
        key: node_key(node.digest),
        record_count: u32::try_from(record_count)
            .map_err(|_| corruption("semantic-history node count overflow"))?,
        byte_len: u32::try_from(byte_len)
            .map_err(|_| corruption("semantic-history node byte count overflow"))?,
        digest: node.digest,
    })
}

fn validate_selector(selector: &ChildSelector, expected_prefix: u8) -> Result<(), LixError> {
    if selector.first > selector.last
        || selector.record_count == 0
        || selector.key.len() != 33
        || selector.key[0] != expected_prefix
        || selector.key[1..] != selector.digest
        || selector.byte_len == 0
    {
        return Err(corruption("semantic-history selector"));
    }
    Ok(())
}

fn validate_children(children: &[ChildSelector]) -> Result<(), LixError> {
    for child in children {
        if child.key.first() != Some(&LEAF_KEY_PREFIX)
            && child.key.first() != Some(&NODE_KEY_PREFIX)
        {
            return Err(corruption("semantic-history child key"));
        }
        validate_selector(child, child.key[0])?;
    }
    for pair in children.windows(2) {
        if pair[0].last >= pair[1].first {
            return Err(corruption("semantic-history child ordering or overlap"));
        }
    }
    Ok(())
}

fn encoded_size<T: musli::Encode<musli::mode::Binary>>(value: &T) -> Result<usize, LixError> {
    Ok(storage_codec::encode("semantic-history size", value)?.len())
}

fn root_digest(root: &SemanticHistoryRoot) -> Result<[u8; 32], LixError> {
    let mut copy = root.clone();
    copy.digest = [0; 32];
    Ok(*blake3::hash(&storage_codec::encode(
        "semantic-history root digest",
        &copy,
    )?)
    .as_bytes())
}

fn directory_digest(node: &SemanticHistoryDirectory) -> Result<[u8; 32], LixError> {
    let mut copy = node.clone();
    copy.digest = [0; 32];
    Ok(*blake3::hash(&storage_codec::encode(
        "semantic-history directory digest",
        &copy,
    )?)
    .as_bytes())
}

fn segment_digest(segment: &SemanticHistorySegment) -> Result<[u8; 32], LixError> {
    let mut copy = segment.clone();
    copy.digest = [0; 32];
    Ok(*blake3::hash(&storage_codec::encode(
        "semantic-history leaf digest",
        &copy,
    )?)
    .as_bytes())
}

fn corruption(message: &str) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authenticated_leaf_directory_round_trip() {
        let segment = SemanticHistorySegment::seal(
            3,
            vec![
                SemanticHistoryRecord {
                    kind: CHANGE_RECORD_KIND,
                    id: [3; 16],
                    value: vec![1, 2],
                },
                SemanticHistoryRecord {
                    kind: COMMIT_RECORD_KIND,
                    id: [3; 16],
                    value: vec![4],
                },
            ],
        )
        .expect("leaf should seal");
        let leaf = selector_for_leaf(&segment).expect("leaf selector");
        let node = SemanticHistoryDirectory::seal(0, vec![leaf]).expect("directory should seal");
        let node_bytes = encode_directory(&node).expect("directory should encode");
        assert_eq!(
            decode_directory(&node_bytes).expect("directory should decode"),
            node
        );
        let target = selector_for_node(&node).expect("root selector");
        let root = SemanticHistoryRoot {
            format_version: FORMAT_VERSION,
            generation: 1,
            target: Some(target),
            record_count: 2,
            digest: [0; 32],
        }
        .seal()
        .expect("root should seal");
        assert_eq!(
            decode_root(&encode_root(&root).expect("root encode")).unwrap(),
            root
        );
    }

    #[test]
    fn malformed_bounds_digest_and_key_fail_closed() {
        let segment = SemanticHistorySegment::seal(
            1,
            vec![SemanticHistoryRecord {
                kind: COMMIT_RECORD_KIND,
                id: [1; 16],
                value: vec![9],
            }],
        )
        .expect("leaf should seal");
        let mut bytes = encode_segment(&segment).expect("leaf encode");
        *bytes.last_mut().unwrap() ^= 1;
        assert!(decode_segment(&bytes).is_err());
        let mut selector = selector_for_leaf(&segment).unwrap();
        selector.key[0] = NODE_KEY_PREFIX;
        assert!(SemanticHistoryDirectory::seal(0, vec![selector]).is_err());
    }

    #[test]
    fn leaf_and_directory_bounds_are_hard_limits() {
        let records = (0..=LEAF_MAX_RECORDS)
            .map(|index| SemanticHistoryRecord {
                kind: COMMIT_RECORD_KIND,
                id: [u8::try_from(index).unwrap_or(u8::MAX); 16],
                value: vec![1],
            })
            .collect::<Vec<_>>();
        assert!(SemanticHistorySegment::seal(1, records).is_err());
        let leaf = SemanticHistorySegment::seal(
            1,
            vec![SemanticHistoryRecord {
                kind: COMMIT_RECORD_KIND,
                id: [1; 16],
                value: vec![1],
            }],
        )
        .unwrap();
        let selector = selector_for_leaf(&leaf).unwrap();
        assert!(SemanticHistoryDirectory::seal(0, vec![selector; DIRECTORY_FANOUT + 1]).is_err());
    }
}
