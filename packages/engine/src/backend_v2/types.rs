use std::ops::Bound;

use bytes::Bytes;

use crate::backend_v2::{BackendError, BackendPredicate, ReadSupport};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SpaceId(pub u32);

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key(pub Bytes);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value(pub Bytes);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadEntry {
    pub key: Key,
    pub value: ProjectedValue,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadBatch {
    pub entries: Vec<ReadEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutEntry {
    pub key: Key,
    pub value: StoredValue,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PutBatch {
    pub entries: Vec<PutEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoredValue {
    FullValue(Bytes),
    Envelope {
        header: Bytes,
        refs: Bytes,
        payload: Bytes,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRange {
    pub lower: Bound<Key>,
    pub upper: Bound<Key>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Prefix {
    pub bytes: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor(pub Bytes);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GetOptions<'a> {
    pub projection: ValueProjection,
    pub order: PointOrder,
    pub preserve_duplicates: bool,
    pub predicates: &'a [BackendPredicate],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PointOrder {
    Caller,
    KeyAsc,
    Unordered,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanOptions<'a> {
    pub projection: ValueProjection,
    pub direction: ScanDirection,
    pub limit_rows: Option<usize>,
    pub limit_bytes: Option<usize>,
    pub cursor: Option<&'a Cursor>,
    pub predicates: &'a [BackendPredicate],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Reverse,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanPage {
    pub entries: ReadBatch,
    pub next_cursor: Option<Cursor>,
    pub support: ReadSupport,
    pub stats: ReadStats,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetManyResult {
    pub entries: Vec<GetSlot>,
    pub support: ReadSupport,
    pub stats: ReadStats,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetSlot {
    pub requested_index: Option<usize>,
    pub key: Key,
    pub value: Option<ProjectedValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueProjection {
    KeyOnly,
    Header,
    Refs,
    HeaderAndRefs,
    Payload,
    FullValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectedValue {
    KeyOnly,
    Header(Bytes),
    Refs(Bytes),
    HeaderAndRefs { header: Bytes, refs: Bytes },
    Payload(Bytes),
    FullValue(Bytes),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadOptions {
    pub snapshot: Option<SnapshotRef>,
    pub consistency: ReadConsistency,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadConsistency {
    #[default]
    Snapshot,
    StaleOk,
    Latest,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteOptions {
    pub base_snapshot: Option<SnapshotRef>,
    pub durability: Durability,
    pub idempotency_key: Option<Bytes>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRef(pub Bytes);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Durability {
    #[default]
    Default,
    Durable,
    Relaxed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadStats {
    pub scanned_entries: u64,
    pub emitted_entries: u64,
    pub skipped_by_backend: u64,
    pub decoded_bytes: u64,
    pub payload_bytes: u64,
    pub backend_calls: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteStats {
    pub put_entries: u64,
    pub deleted_entries: u64,
    pub deleted_ranges: u64,
    pub written_bytes: u64,
    pub backend_calls: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitResult {
    pub commit_id: Option<Bytes>,
    pub stats: WriteStats,
}

impl Prefix {
    pub fn to_range(&self) -> Result<KeyRange, BackendError> {
        let lower = Key(self.bytes.clone());
        let mut upper = self.bytes.to_vec();

        while let Some(last) = upper.last_mut() {
            if *last == u8::MAX {
                upper.pop();
            } else {
                *last += 1;
                return Ok(KeyRange {
                    lower: Bound::Included(lower),
                    upper: Bound::Excluded(Key(Bytes::from(upper))),
                });
            }
        }

        Ok(KeyRange {
            lower: Bound::Included(lower),
            upper: Bound::Unbounded,
        })
    }
}

impl Default for GetOptions<'_> {
    fn default() -> Self {
        Self {
            projection: ValueProjection::FullValue,
            order: PointOrder::Caller,
            preserve_duplicates: true,
            predicates: &[],
        }
    }
}

impl Default for ScanOptions<'_> {
    fn default() -> Self {
        Self {
            projection: ValueProjection::FullValue,
            direction: ScanDirection::Forward,
            limit_rows: None,
            limit_bytes: None,
            cursor: None,
            predicates: &[],
        }
    }
}
