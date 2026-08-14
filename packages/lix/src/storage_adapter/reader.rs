#![allow(
    clippy::manual_async_fn,
    reason = "test readers mirror explicit Send future signatures from StorageRead"
)]

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bytes::Bytes;

    use crate::storage::{
        BeginScanOptions, CoreProjection, GetManyRequest, GetManyResult, GetOptions, Key, KeyRange,
        Memory, Prefix, ProjectedValue, ReadOptions, ScanChunk, ScanCursor, StorageError,
        StorageRead, StorageScanSource, StoredValue, ValueSemantics, WriteOptions,
    };
    use crate::storage_adapter::{
        PointReadPlan, SharedStorageAdapterRead, StorageAdapter, StorageAdapterRead,
        StorageAdapterReadScope, StorageSpace, exact_get_many,
    };

    fn key(bytes: &'static str) -> Key {
        Key(Bytes::from_static(bytes.as_bytes()))
    }

    fn key_bytes(bytes: &'static [u8]) -> Key {
        Key(Bytes::from_static(bytes))
    }

    fn value(bytes: &'static str) -> StoredValue {
        StoredValue {
            bytes: Bytes::from_static(bytes.as_bytes()),
        }
    }

    fn space() -> StorageSpace {
        StorageSpace::engine_declared(1, "test.space", ValueSemantics::Mutable)
    }

    #[derive(Clone, Default)]
    struct SpyRead {
        seen: Arc<Mutex<Vec<Key>>>,
        scan_ranges: Arc<Mutex<Vec<KeyRange>>>,
        scan_calls: Arc<AtomicUsize>,
    }

    impl StorageRead for SpyRead {
        fn get_many(
            &self,
            requests: &[GetManyRequest<'_>],
        ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
            async move {
                let keys = requests
                    .iter()
                    .flat_map(|request| request.keys.iter().cloned())
                    .collect::<Vec<_>>();
                *self.seen.lock().expect("spy lock") = keys.clone();
                Ok(GetManyResult::new(
                    keys.iter()
                        .map(|key| Some(ProjectedValue::FullValue(key.0.clone())))
                        .collect(),
                ))
            }
        }

        fn begin_scan(
            &self,
            _space: StorageSpace,
            range: KeyRange,
            opts: BeginScanOptions,
        ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
            async move {
                self.scan_calls.fetch_add(1, Ordering::Relaxed);
                self.scan_ranges
                    .lock()
                    .expect("spy lock")
                    .push(range.clone());
                ScanCursor::from_source(range, opts.order, EmptyScanSource)
            }
        }
    }

    struct EmptyScanSource;

    impl StorageScanSource for EmptyScanSource {
        fn next_page(
            &mut self,
            _limit_rows: usize,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>>
        {
            Box::pin(async { Ok(ScanChunk::new(Vec::new(), false)) })
        }
    }

    #[derive(Clone)]
    struct OverlapRead {
        entered: Arc<tokio::sync::Barrier>,
        release: Arc<tokio::sync::Semaphore>,
    }

    #[derive(Clone, Copy)]
    struct WrongCardinalityRead {
        returned: usize,
    }

    impl StorageRead for WrongCardinalityRead {
        async fn get_many(
            &self,
            _requests: &[GetManyRequest<'_>],
        ) -> Result<GetManyResult, StorageError> {
            Ok(GetManyResult::new(vec![None; self.returned]))
        }

        async fn begin_scan(
            &self,
            _space: StorageSpace,
            _range: KeyRange,
            _opts: BeginScanOptions,
        ) -> Result<ScanCursor<'_>, StorageError> {
            unreachable!("wrong-cardinality test does not scan")
        }
    }

    impl StorageRead for OverlapRead {
        async fn get_many(
            &self,
            _requests: &[GetManyRequest<'_>],
        ) -> Result<GetManyResult, StorageError> {
            self.entered.wait().await;
            self.release
                .acquire()
                .await
                .expect("overlap test semaphore should stay open")
                .forget();
            Ok(GetManyResult::new(Vec::new()))
        }

        async fn begin_scan(
            &self,
            _space: StorageSpace,
            _range: KeyRange,
            _opts: BeginScanOptions,
        ) -> Result<ScanCursor<'_>, StorageError> {
            unreachable!("overlap test does not scan")
        }
    }

    #[tokio::test]
    async fn cloned_shared_reads_overlap_on_one_snapshot() {
        let entered = Arc::new(tokio::sync::Barrier::new(3));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let shared = SharedStorageAdapterRead::new(StorageAdapterReadScope::new(OverlapRead {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        }));

        let left = shared.clone();
        let left_task = tokio::spawn(async move {
            left.get_many(&[GetManyRequest {
                space: StorageSpace::engine_declared(1, "test.mutable", ValueSemantics::Mutable),
                keys: &[],
                opts: GetOptions::default(),
            }])
            .await
        });
        let right = shared.clone();
        let right_task = tokio::spawn(async move {
            right
                .get_many(&[GetManyRequest {
                    space: StorageSpace::engine_declared(
                        1,
                        "test.mutable",
                        ValueSemantics::Mutable,
                    ),
                    keys: &[],
                    opts: GetOptions::default(),
                }])
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), entered.wait())
            .await
            .expect("both reads should enter without serializing");
        release.add_permits(2);
        left_task.await.expect("join left read").expect("left read");
        right_task
            .await
            .expect("join right read")
            .expect("right read");
        shared.finish().expect("finish shared read");
    }

    #[tokio::test]
    async fn point_reads_dedupe_and_reconstruct_caller_order() {
        let spy = SpyRead::default();
        let seen = Arc::clone(&spy.seen);
        let read = StorageAdapterReadScope::new(spy);
        let result = PointReadPlan::new(space(), &[key("b"), key("a"), key("b"), key("missing")])
            .materialize(&read, GetOptions::default())
            .await
            .expect("point read");

        assert_eq!(
            *seen.lock().expect("spy lock"),
            [key("b"), key("a"), key("missing")]
        );
        assert_eq!(
            result.value,
            vec![
                Some(ProjectedValue::FullValue(key("b").0)),
                Some(ProjectedValue::FullValue(key("a").0)),
                Some(ProjectedValue::FullValue(key("b").0)),
                Some(ProjectedValue::FullValue(key("missing").0)),
            ]
        );
    }

    #[tokio::test]
    async fn known_unique_point_plan_keeps_identity_and_is_reusable() {
        let spy = SpyRead::default();
        let seen = Arc::clone(&spy.seen);
        let read = StorageAdapterReadScope::new(spy);
        let plan = PointReadPlan::from_unique_keys(space(), vec![key("a"), key("b")]);

        assert_eq!(plan.requested_to_unique().to_vec(), [0, 1]);
        for _ in 0..2 {
            let result = plan
                .collect(&read, GetOptions::default())
                .await
                .expect("known-unique point read");
            assert_eq!(result.value.requested_to_unique.to_vec(), [0, 1]);
            assert_eq!(result.value.unique_values.len(), 2);
            assert_eq!(result.stats.requested_keys, 2);
            assert_eq!(result.stats.unique_storage_keys, 2);
            assert_eq!(result.stats.storage_calls, 1);
        }
        assert_eq!(*seen.lock().expect("spy lock"), [key("a"), key("b")]);
    }

    #[tokio::test]
    async fn point_reads_reject_short_and_long_backend_results() {
        let plan = PointReadPlan::from_unique_keys(space(), vec![key("a"), key("b")]);
        for returned in [1, 3] {
            let read = StorageAdapterReadScope::new(WrongCardinalityRead { returned });
            let error = plan
                .materialize(&read, GetOptions::default())
                .await
                .expect_err("malformed backend result must fail");
            assert!(matches!(error, StorageError::Corruption(_)));
            assert!(
                error
                    .to_string()
                    .contains("2 requested keys in a 1-request batch")
            );
        }
    }

    #[tokio::test]
    async fn exact_multi_request_reads_reject_short_and_long_backend_results() {
        let left_keys = [key("a")];
        let right_keys = [key("b"), key("c")];
        let requests = [
            GetManyRequest {
                space: space(),
                keys: &left_keys,
                opts: GetOptions::default(),
            },
            GetManyRequest {
                space: space(),
                keys: &right_keys,
                opts: GetOptions::default(),
            },
        ];
        for returned in [2, 4] {
            let read = StorageAdapterReadScope::new(WrongCardinalityRead { returned });
            let error = exact_get_many(&read, &requests)
                .await
                .expect_err("malformed multi-request backend result must fail");
            assert!(matches!(error, StorageError::Corruption(_)));
            assert!(
                error
                    .to_string()
                    .contains("3 requested keys in a 2-request batch")
            );
        }
    }

    #[tokio::test]
    async fn prefix_plans_lower_edge_case_bounds() {
        let spy = SpyRead::default();
        let ranges = Arc::clone(&spy.scan_ranges);
        let read = StorageAdapterReadScope::new(spy);

        for prefix in [b"".as_slice(), b"\xff".as_slice(), b"\x00\xff".as_slice()] {
            let range = Prefix {
                bytes: Bytes::copy_from_slice(prefix),
            }
            .to_range()
            .expect("valid prefix range");
            let mut cursor = read
                .begin_scan(space(), range, BeginScanOptions::default())
                .await
                .expect("begin prefix scan");
            let _ = cursor.next_page(1).await.expect("prefix scan");
        }

        assert_eq!(
            *ranges.lock().expect("spy lock"),
            [
                KeyRange {
                    lower: Bound::Included(key_bytes(b"")),
                    upper: Bound::Unbounded,
                },
                KeyRange {
                    lower: Bound::Included(key_bytes(b"\xff")),
                    upper: Bound::Unbounded,
                },
                KeyRange {
                    lower: Bound::Included(key_bytes(b"\x00\xff")),
                    upper: Bound::Excluded(key_bytes(b"\x01")),
                },
            ]
        );
    }

    #[tokio::test]
    async fn zero_limit_prefix_cursor_cancels_without_fetching_a_page() {
        let spy = SpyRead::default();
        let scan_calls = Arc::clone(&spy.scan_calls);
        let read = StorageAdapterReadScope::new(spy);
        let range = Prefix {
            bytes: Bytes::from_static(b"a"),
        }
        .to_range()
        .expect("valid prefix range");
        let mut cursor = read
            .begin_scan(
                space(),
                range,
                BeginScanOptions {
                    projection: CoreProjection::KeyOnly,
                    ..BeginScanOptions::default()
                },
            )
            .await
            .expect("begin zero-limit prefix scan");
        let (result, result_has_more) = cursor
            .next_page(0)
            .await
            .expect("zero-limit prefix scan")
            .into_parts();

        assert!(result.is_empty());
        assert!(!result_has_more);
        assert_eq!(scan_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn scan_plan_returns_owned_pages_and_honors_resume_key() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        for (key_bytes, value_bytes) in [("a", "A"), ("b", "B"), ("c", "C")] {
            writes.put(space(), key(key_bytes), value(value_bytes));
        }
        storage
            .commit_write_set(writes, WriteOptions::default())
            .await
            .expect("seed");
        let read = storage
            .begin_read(ReadOptions::default())
            .await
            .expect("begin read");
        let mut cursor = read
            .begin_scan(
                space(),
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                BeginScanOptions::default(),
            )
            .await
            .expect("begin cursor");
        let (page, page_has_more) = cursor.next_page(1).await.expect("first page").into_parts();
        assert_eq!(page[0].key, key("a"));
        assert!(page_has_more);

        let (next, _next_has_more) = cursor
            .next_page(crate::storage::MAX_SCAN_PAGE_ROWS)
            .await
            .expect("second page")
            .into_parts();
        assert_eq!(
            next.into_iter().map(|entry| entry.key).collect::<Vec<_>>(),
            [key("b"), key("c")]
        );
    }
}
