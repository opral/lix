use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;

use crate::storage::{
    BeginScanOptions, Key, KeyRange, ScanChunk, ScanOptions, ScanOrder, StorageError, StorageRead,
    StorageSpace,
};

/// Ephemeral iterator state owned by a coherent storage read view.
///
/// Implementations keep their native iterator here. This state is never a
/// durable continuation token and must not be used as an authorization or
/// garbage-collection authority.
#[doc(hidden)]
pub trait StorageScanSource: Send {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>>;
}

/// One storage-owned range cursor tied to the read view that created it.
///
/// A cursor validates ordering, bounds, and page cardinality before returning
/// adapter results. Drop cancels the scan and releases the native iterator.
#[expect(missing_debug_implementations)]
pub struct ScanCursor<'a> {
    source: Box<dyn StorageScanSource + 'a>,
    range: KeyRange,
    order: ScanOrder,
    last_key: Option<Key>,
    finished: bool,
}

impl<'a> ScanCursor<'a> {
    #[doc(hidden)]
    pub fn from_source(
        range: KeyRange,
        order: ScanOrder,
        source: impl StorageScanSource + 'a,
    ) -> Result<Self, StorageError> {
        if !range_is_valid(&range) {
            return Err(StorageError::InvalidCursor);
        }
        Ok(Self {
            source: Box::new(source),
            range,
            order,
            last_key: None,
            finished: false,
        })
    }

    /// Advances the live native iterator by at most `limit_rows` entries.
    ///
    /// The requested limit is capped by [`crate::storage::MAX_SCAN_PAGE_ROWS`].
    /// A zero limit ends the cursor without touching the backend.
    pub async fn next_page(&mut self, limit_rows: usize) -> Result<ScanChunk, StorageError> {
        if self.finished || limit_rows == 0 {
            self.finished = true;
            return Ok(ScanChunk {
                entries: Vec::new(),
                has_more: false,
            });
        }
        let page_size = limit_rows.min(crate::storage::MAX_SCAN_PAGE_ROWS);
        let chunk = self.source.next_page(page_size).await?;
        if chunk.entries.len() > page_size
            || (chunk.has_more && chunk.entries.is_empty())
            || !self.valid_entries(&chunk)
        {
            return Err(StorageError::InvalidCursor);
        }
        self.last_key = chunk.entries.last().map(|entry| entry.key.clone());
        self.finished = !chunk.has_more;
        Ok(chunk)
    }

    fn valid_entries(&self, chunk: &ScanChunk) -> bool {
        let mut previous = self.last_key.as_ref();
        for entry in &chunk.entries {
            if !range_contains(&self.range, &entry.key) {
                return false;
            }
            if let Some(previous) = previous {
                let ordered = match self.order {
                    ScanOrder::Ascending => previous < &entry.key,
                    ScanOrder::Descending => previous > &entry.key,
                };
                if !ordered {
                    return false;
                }
            }
            previous = Some(&entry.key);
        }
        true
    }
}

pub(crate) fn legacy_scan_cursor<'a, R>(
    read: &'a R,
    space: StorageSpace,
    range: KeyRange,
    opts: BeginScanOptions,
) -> Result<ScanCursor<'a>, StorageError>
where
    R: StorageRead + ?Sized,
{
    if opts.order == ScanOrder::Descending {
        return Err(StorageError::Unsupported(
            crate::storage::Capability::ReverseScan,
        ));
    }
    ScanCursor::from_source(
        range.clone(),
        opts.order,
        RestartingScanSource {
            read,
            space,
            range,
            projection: opts.projection,
            resume_after: None,
            finished: false,
        },
    )
}

struct RestartingScanSource<'a, R>
where
    R: StorageRead + ?Sized,
{
    read: &'a R,
    space: StorageSpace,
    range: KeyRange,
    projection: crate::storage::CoreProjection,
    resume_after: Option<Key>,
    finished: bool,
}

impl<R> StorageScanSource for RestartingScanSource<'_, R>
where
    R: StorageRead + ?Sized,
{
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            if self.finished {
                return Ok(ScanChunk {
                    entries: Vec::new(),
                    has_more: false,
                });
            }
            let chunk = self
                .read
                .scan(
                    self.space,
                    self.range.clone(),
                    ScanOptions {
                        projection: self.projection,
                        limit_rows,
                        resume_after: self.resume_after.clone(),
                    },
                )
                .await?;
            self.resume_after = chunk.entries.last().map(|entry| entry.key.clone());
            self.finished = !chunk.has_more;
            Ok(chunk)
        })
    }
}

fn range_is_valid(range: &KeyRange) -> bool {
    match (&range.lower, &range.upper) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
        (Bound::Included(lower), Bound::Included(upper)) => lower <= upper,
        (Bound::Included(lower) | Bound::Excluded(lower), Bound::Excluded(upper))
        | (Bound::Excluded(lower), Bound::Included(upper)) => lower <= upper,
    }
}

fn range_contains(range: &KeyRange, key: &Key) -> bool {
    let after_lower = match &range.lower {
        Bound::Included(lower) => key >= lower,
        Bound::Excluded(lower) => key > lower,
        Bound::Unbounded => true,
    };
    let before_upper = match &range.upper {
        Bound::Included(upper) => key <= upper,
        Bound::Excluded(upper) => key < upper,
        Bound::Unbounded => true,
    };
    after_lower && before_upper
}
