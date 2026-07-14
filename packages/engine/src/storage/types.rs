use std::ops::Bound;

use bytes::Bytes;

use crate::storage::StorageError;

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
    /// One slot per key passed to `get_many`, in caller order.
    ///
    /// Duplicates are preserved. `None` means the requested key was missing.
    pub values: Vec<Option<ProjectedValue>>,
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
    Durable,
}

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
