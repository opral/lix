use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;

use crate::storage::{Key, KeyRange, ScanChunk, ScanOrder, StorageError};

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
            return Ok(ScanChunk {
                entries: Vec::new(),
                has_more: false,
            });
        }
        let page_size = limit_rows.min(crate::storage::MAX_SCAN_PAGE_ROWS);
        self.poisoned = true;
        let chunk = self.source.next_page(page_size).await?;
        if chunk.entries.len() > page_size
            || (chunk.has_more && chunk.entries.is_empty())
            || !self.valid_entries(&chunk)
        {
            return Err(StorageError::InvalidCursor);
        }
        self.last_key = chunk.entries.last().map(|entry| entry.key.clone());
        self.finished = !chunk.has_more;
        self.poisoned = false;
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
    use crate::storage::{ProjectedValue, ReadEntry};
    use bytes::Bytes;
    use std::task::{Context, Poll};

    struct ReadySource;

    impl StorageScanSource for ReadySource {
        fn next_page(
            &mut self,
            _limit_rows: usize,
        ) -> Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
            Box::pin(async {
                Ok(ScanChunk {
                    entries: vec![ReadEntry {
                        key: Key(Bytes::from_static(b"a")),
                        value: ProjectedValue::KeyOnly,
                    }],
                    has_more: false,
                })
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
                Ok(ScanChunk {
                    entries: vec![entry.clone(), entry],
                    has_more: false,
                })
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

        let page = cursor.next_page(1).await.expect("cursor remains usable");
        assert_eq!(page.entries[0].key.0.as_ref(), b"a");
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
