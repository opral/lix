use std::{fmt, ops::Bound};

use bytes::Bytes;

use crate::storage::{Precondition, StorageError};

/// Maximum number of owned rows returned by one storage scan page.
pub const MAX_SCAN_PAGE_ROWS: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SpaceId(pub u32);

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key(pub Bytes);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadEntry {
    pub key: Key,
    pub value: ProjectedValue,
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

/// A validated slice inside one immutable encoded mutation buffer.
///
/// Storage write planning uses these compact descriptors to keep encoded keys
/// and values in batch-wide buffers until the final backend boundary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BufferRange {
    offset: usize,
    length: usize,
}

impl BufferRange {
    pub const fn new(offset: usize, length: usize) -> Self {
        Self { offset, length }
    }

    pub const fn offset(self) -> usize {
        self.offset
    }

    pub const fn len(self) -> usize {
        self.length
    }

    pub const fn is_empty(self) -> bool {
        self.length == 0
    }

    fn checked_end(self) -> Option<usize> {
        self.offset.checked_add(self.length)
    }
}

/// One put whose key and value are ranges in an [`EncodedMutationBatch`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodedPut {
    pub key: BufferRange,
    pub value: BufferRange,
}

/// Contiguous encoded storage mutations for one storage space.
///
/// This is the zero-copy ingress for domain lowerers which already encode a
/// complete batch. `key_bytes` contains every put and delete key; `value_bytes`
/// contains every put value. Construction validates every descriptor once, so
/// [`crate::storage_adapter::StorageWriteSet`] can retain the two buffers and
/// carry only ranges through sorting and duplicate validation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EncodedMutationBatch {
    key_bytes: Bytes,
    value_bytes: Bytes,
    puts: Vec<EncodedPut>,
    deletes: Vec<BufferRange>,
}

impl EncodedMutationBatch {
    pub fn try_new(
        key_bytes: Bytes,
        value_bytes: Bytes,
        puts: Vec<EncodedPut>,
        deletes: Vec<BufferRange>,
    ) -> Result<Self, EncodedMutationBatchError> {
        for (index, put) in puts.iter().enumerate() {
            validate_buffer_range(put.key, key_bytes.len()).map_err(|()| {
                EncodedMutationBatchError::PutKeyOutOfBounds {
                    index,
                    range: put.key,
                    buffer_len: key_bytes.len(),
                }
            })?;
            validate_buffer_range(put.value, value_bytes.len()).map_err(|()| {
                EncodedMutationBatchError::PutValueOutOfBounds {
                    index,
                    range: put.value,
                    buffer_len: value_bytes.len(),
                }
            })?;
        }
        for (index, range) in deletes.iter().copied().enumerate() {
            validate_buffer_range(range, key_bytes.len()).map_err(|()| {
                EncodedMutationBatchError::DeleteKeyOutOfBounds {
                    index,
                    range,
                    buffer_len: key_bytes.len(),
                }
            })?;
        }
        Ok(Self {
            key_bytes,
            value_bytes,
            puts,
            deletes,
        })
    }

    pub fn put_count(&self) -> usize {
        self.puts.len()
    }

    pub fn delete_count(&self) -> usize {
        self.deletes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.puts.is_empty() && self.deletes.is_empty()
    }

    pub fn key_bytes(&self) -> &Bytes {
        &self.key_bytes
    }

    pub fn value_bytes(&self) -> &Bytes {
        &self.value_bytes
    }

    pub fn puts(&self) -> &[EncodedPut] {
        &self.puts
    }

    pub fn deletes(&self) -> &[BufferRange] {
        &self.deletes
    }

    pub(crate) fn into_parts(self) -> (Bytes, Bytes, Vec<EncodedPut>, Vec<BufferRange>) {
        (self.key_bytes, self.value_bytes, self.puts, self.deletes)
    }
}

fn validate_buffer_range(range: BufferRange, buffer_len: usize) -> Result<(), ()> {
    match range.checked_end() {
        Some(end) if end <= buffer_len => Ok(()),
        Some(_) | None => Err(()),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodedMutationBatchError {
    PutKeyOutOfBounds {
        index: usize,
        range: BufferRange,
        buffer_len: usize,
    },
    PutValueOutOfBounds {
        index: usize,
        range: BufferRange,
        buffer_len: usize,
    },
    DeleteKeyOutOfBounds {
        index: usize,
        range: BufferRange,
        buffer_len: usize,
    },
}

impl fmt::Display for EncodedMutationBatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PutKeyOutOfBounds {
                index,
                range,
                buffer_len,
            } => write!(
                formatter,
                "encoded put {index} key range {range:?} exceeds key buffer length {buffer_len}"
            ),
            Self::PutValueOutOfBounds {
                index,
                range,
                buffer_len,
            } => write!(
                formatter,
                "encoded put {index} value range {range:?} exceeds value buffer length {buffer_len}"
            ),
            Self::DeleteKeyOutOfBounds {
                index,
                range,
                buffer_len,
            } => write!(
                formatter,
                "encoded delete {index} key range {range:?} exceeds key buffer length {buffer_len}"
            ),
        }
    }
}

impl std::error::Error for EncodedMutationBatchError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredValue {
    pub bytes: Bytes,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GetOptions {
    pub projection: CoreProjection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanOptions {
    pub projection: CoreProjection,
    pub limit_rows: usize,
    pub resume_after: Option<Key>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanChunk {
    pub entries: Vec<ReadEntry>,
    pub has_more: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetManyResult {
    /// One slot per key passed to `get_many`, flattened in request order and
    /// then key order within each request.
    ///
    /// Duplicates are preserved. `None` means the requested key was missing.
    pub values: Vec<Option<ProjectedValue>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GetManyRequest<'a> {
    pub space: SpaceId,
    pub keys: &'a [Key],
    pub opts: GetOptions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreProjection {
    KeyOnly,
    FullValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectedValue {
    KeyOnly,
    FullValue(Bytes),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadOptions {
    pub snapshot: Option<SnapshotRef>,
    pub consistency: ReadConsistency,
    /// Minimum persistence boundary for rows returned by this read.
    ///
    /// Most engine reads use [`ReadDurability::Visible`], which permits a
    /// backend to serve its latest committed in-memory state. Callers that
    /// must distinguish a published write from one that has crossed the
    /// backend's documented durable boundary use [`ReadDurability::Durable`].
    /// Backends that cannot prove that boundary must return an explicit
    /// storage error instead of silently downgrading the read.
    pub durability: ReadDurability,
}

/// Persistence boundary required from a storage read.
///
/// This is deliberately separate from [`ReadConsistency`]: consistency
/// chooses a snapshot/view, while durability controls which persistence tier
/// may satisfy that view.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadDurability {
    /// The backend's normal committed view, which may include data awaiting a
    /// background durability step.
    #[default]
    Visible,
    /// Only data which the backend can prove has crossed its durable
    /// persistence boundary.
    Durable,
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
    pub idempotency_key: Option<Bytes>,
    /// Do not acknowledge the commit until the backend has crossed its
    /// durable persistence boundary.
    pub await_durable: bool,
    /// Conditions evaluated atomically against the state immediately before
    /// this write becomes visible.
    pub preconditions: Vec<Precondition>,
    /// Lower bound for the backend's encoded write-batch capacity.
    ///
    /// Correctness never depends on this hint. The storage adapter fills it
    /// from the complete canonical write set so backends with contiguous batch
    /// encodings can allocate their large buffer once.
    pub batch_capacity_hint_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRef(pub Bytes);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteStats {
    pub put_entries: u64,
    pub deleted_entries: u64,
    pub deleted_ranges: u64,
    pub written_bytes: u64,
    pub storage_calls: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitResult {
    pub commit_id: Option<Bytes>,
    pub stats: WriteStats,
}

impl Prefix {
    pub fn to_range(&self) -> Result<KeyRange, StorageError> {
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

impl Default for GetOptions {
    fn default() -> Self {
        Self {
            projection: CoreProjection::FullValue,
        }
    }
}

impl GetManyResult {
    pub fn new(values: Vec<Option<ProjectedValue>>) -> Self {
        Self { values }
    }

    pub fn entries_for_requested_keys(&self, keys: &[Key]) -> Vec<ReadEntry> {
        keys.iter()
            .cloned()
            .zip(self.values.iter().cloned())
            .filter_map(|(key, value)| value.map(|value| ReadEntry { key, value }))
            .collect()
    }
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            projection: CoreProjection::FullValue,
            limit_rows: 1024,
            resume_after: None,
        }
    }
}

impl ScanOptions {
    pub fn page_size(&self) -> usize {
        self.limit_rows.min(MAX_SCAN_PAGE_ROWS)
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_SCAN_PAGE_ROWS, ScanOptions};

    #[test]
    fn scan_page_size_is_bounded() {
        assert_eq!(
            ScanOptions {
                limit_rows: usize::MAX,
                ..ScanOptions::default()
            }
            .page_size(),
            MAX_SCAN_PAGE_ROWS
        );
        assert_eq!(
            ScanOptions {
                limit_rows: 17,
                ..ScanOptions::default()
            }
            .page_size(),
            17
        );
    }
}
