use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;

use crate::storage::{Key, KeyRange, ReadEntry, ScanChunk, ScanOrder, StorageError};

/// Ephemeral iterator state owned by a coherent storage read view.
///
/// Production range-backed adapters keep their native iterator here. This
/// state is never a durable continuation token and must not be used as an
/// authorization or garbage-collection authority.
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
    poisoned: bool,
}

impl<'a> ScanCursor<'a> {
    #[doc(hidden)]
    pub fn validate_range(range: &KeyRange) -> Result<(), StorageError> {
        if range_is_valid(range) {
            Ok(())
        } else {
            Err(StorageError::InvalidCursor)
        }
    }

    #[doc(hidden)]
    pub fn from_source(
        range: KeyRange,
        order: ScanOrder,
        source: impl StorageScanSource + 'a,
    ) -> Result<Self, StorageError> {
        Self::validate_range(&range)?;
        Ok(Self {
            source: Box::new(source),
            range,
            order,
            last_key: None,
            finished: false,
            poisoned: false,
        })
    }

    /// Advances the live native iterator by at most `limit_rows` entries.
    ///
    /// **This is the deliberately bounded read.** It stops at the page boundary
    /// even when the range holds more rows, and the returned [`ScanChunk`] only
    /// yields its entries through
    /// [`into_parts`](ScanChunk::into_parts) so that the `has_more` flag is
    /// bound at the call site. Reach for it only when a bound is the intent —
    /// an existence probe, a `LIMIT` pushed down from SQL, or a byte budget.
    ///
    /// **To read every row in the range, use [`Self::collect_all`] or
    /// [`Self::next_chunk`] instead.** Those cannot truncate, and they are
    /// shorter to write than a hand-rolled page loop.
    ///
    /// The requested limit is capped by [`crate::storage::MAX_SCAN_PAGE_ROWS`].
    /// A zero limit ends the cursor without touching the backend.
    ///
    /// Dropping a `next_page` future before it completes permanently poisons
    /// this cursor. A later call returns [`StorageError::InvalidCursor`]; it
    /// never restarts an iterator or refreshes the coherent read view. Durable
    /// maintenance resumes by opening a new cursor whose range has an explicit
    /// exclusive authenticated lower bound.
    pub async fn next_page(&mut self, limit_rows: usize) -> Result<ScanChunk, StorageError> {
        if self.poisoned {
            return Err(StorageError::InvalidCursor);
        }
        if self.finished || limit_rows == 0 {
            self.finished = true;
            return Ok(ScanChunk::new(Vec::new(), false));
        }
        let page_size = limit_rows.min(crate::storage::MAX_SCAN_PAGE_ROWS);
        self.poisoned = true;
        let (entries, has_more) = self.source.next_page(page_size).await?.into_parts();
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_checkpoint_scan_page(entries.len());
        if entries.len() > page_size
            || (has_more && entries.is_empty())
            || !self.valid_entries(&entries)
        {
            return Err(StorageError::InvalidCursor);
        }
        self.last_key = entries.last().map(|entry| entry.key.clone());
        self.finished = !has_more;
        self.poisoned = false;
        Ok(ScanChunk::new(entries, has_more))
    }

    /// Yields the next chunk of rows, or `None` once the range is drained.
    ///
    /// This is the streaming drain:
    /// `while let Some(entries) = cursor.next_chunk().await? { … }` visits
    /// every row in the range without materializing all of them at once, and
    /// there is no continuation flag for a caller to forget. Prefer it over a
    /// hand-written `next_page` loop — a hand-written loop is exactly the shape
    /// that truncated certified-manifest scans at one page.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<ReadEntry>>, StorageError> {
        if self.finished {
            return Ok(None);
        }
        let (entries, has_more) = self
            .next_page(crate::storage::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        if entries.is_empty() && !has_more {
            return Ok(None);
        }
        Ok(Some(entries))
    }

    /// Reads every remaining row in the range into one vector.
    ///
    /// This is the ordinary read. It cannot truncate, and it is shorter than
    /// the bounded form, which is the intended asymmetry: a call site that
    /// wants everything says so in one call, and a call site that wants a bound
    /// has to reach for [`Self::next_page`] and bind `has_more` explicitly.
    ///
    /// The cost is O(rows in range) memory. A caller scanning an unbounded
    /// range over a large space should stream with [`Self::next_chunk`].
    pub async fn collect_all(&mut self) -> Result<Vec<ReadEntry>, StorageError> {
        let mut all = Vec::new();
        while let Some(entries) = self.next_chunk().await? {
            all.extend(entries);
        }
        Ok(all)
    }

    fn valid_entries(&self, entries: &[ReadEntry]) -> bool {
        let mut previous = self.last_key.as_ref();
        for entry in entries {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ProjectedValue;
    use bytes::Bytes;
    use std::task::{Context, Poll};

    struct ReadySource;

    impl StorageScanSource for ReadySource {
        fn next_page(
            &mut self,
            _limit_rows: usize,
        ) -> Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
            Box::pin(async {
                Ok(ScanChunk::new(
                    vec![ReadEntry {
                        key: Key(Bytes::from_static(b"a")),
                        value: ProjectedValue::KeyOnly,
                    }],
                    false,
                ))
            })
        }
    }

    struct PendingSource;

    impl StorageScanSource for PendingSource {
        fn next_page(
            &mut self,
            _limit_rows: usize,
        ) -> Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
            Box::pin(std::future::pending())
        }
    }

    struct ErrorSource;

    impl StorageScanSource for ErrorSource {
        fn next_page(
            &mut self,
            _limit_rows: usize,
        ) -> Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
            Box::pin(async { Err(StorageError::Io("injected scan failure".to_string())) })
        }
    }

    struct DuplicateSource;

    impl StorageScanSource for DuplicateSource {
        fn next_page(
            &mut self,
            _limit_rows: usize,
        ) -> Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
            Box::pin(async {
                let entry = ReadEntry {
                    key: Key(Bytes::from_static(b"a")),
                    value: ProjectedValue::KeyOnly,
                };
                Ok(ScanChunk::new(vec![entry.clone(), entry], false))
            })
        }
    }

    fn full_range() -> KeyRange {
        KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        }
    }

    #[tokio::test]
    async fn cancellation_before_first_poll_leaves_cursor_usable() {
        let mut cursor = ScanCursor::from_source(full_range(), ScanOrder::Ascending, ReadySource)
            .expect("valid cursor");
        let future = cursor.next_page(1);
        drop(future);

        let (entries, _has_more) = cursor
            .next_page(1)
            .await
            .expect("cursor remains usable")
            .into_parts();
        assert_eq!(entries[0].key.0.as_ref(), b"a");
    }

    #[tokio::test]
    async fn cancellation_after_pending_poll_poisons_cursor_fail_closed() {
        let mut cursor = ScanCursor::from_source(full_range(), ScanOrder::Ascending, PendingSource)
            .expect("valid cursor");
        let mut future = Box::pin(cursor.next_page(1));
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        drop(future);

        assert!(matches!(
            cursor.next_page(1).await,
            Err(StorageError::InvalidCursor)
        ));
    }

    #[tokio::test]
    async fn backend_error_poisons_cursor_fail_closed() {
        let mut cursor = ScanCursor::from_source(full_range(), ScanOrder::Ascending, ErrorSource)
            .expect("valid cursor");
        assert!(matches!(
            cursor.next_page(1).await,
            Err(StorageError::Io(_))
        ));
        assert!(matches!(
            cursor.next_page(1).await,
            Err(StorageError::InvalidCursor)
        ));
    }

    #[tokio::test]
    async fn malformed_page_poisons_cursor_fail_closed() {
        let mut cursor =
            ScanCursor::from_source(full_range(), ScanOrder::Ascending, DuplicateSource)
                .expect("valid cursor");
        assert!(matches!(
            cursor.next_page(2).await,
            Err(StorageError::InvalidCursor)
        ));
        assert!(matches!(
            cursor.next_page(1).await,
            Err(StorageError::InvalidCursor)
        ));
    }
}
