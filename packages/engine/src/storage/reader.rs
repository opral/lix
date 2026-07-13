#![allow(
    clippy::manual_async_fn,
    reason = "test readers mirror explicit Send future signatures from BackendRead"
)]

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bytes::Bytes;

    use crate::backend::{
        BackendError, BackendRead, CoreProjection, GetManyResult, GetOptions, InMemoryBackend, Key,
        KeyRange, Prefix, ProjectedValue, ReadOptions, ScanChunk, ScanOptions, SpaceId,
        StoredValue, WriteOptions,
    };
    use crate::storage::{
        PointReadPlan, ScanPlan, SharedStorageRead, StorageContext, StorageRead, StorageReadScope,
        StorageReadStats, StorageSpace,
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
        StorageSpace::new(SpaceId(1), "test.space")
    }

    #[derive(Clone, Default)]
    struct SpyRead {
        seen: Arc<Mutex<Vec<Key>>>,
        scan_ranges: Arc<Mutex<Vec<KeyRange>>>,
        scan_calls: Arc<AtomicUsize>,
    }

    impl BackendRead for SpyRead {
        fn get_many(
            &self,
            _space: SpaceId,
            keys: &[Key],
            _opts: GetOptions,
        ) -> impl Future<Output = Result<GetManyResult, BackendError>> + Send {
            async move {
                *self.seen.lock().expect("spy lock") = keys.to_vec();
                Ok(GetManyResult::new(
                    keys.iter()
                        .map(|key| Some(ProjectedValue::FullValue(key.0.clone())))
                        .collect(),
                ))
            }
        }

        fn scan(
            &self,
            _space: SpaceId,
            range: KeyRange,
            _opts: ScanOptions,
        ) -> impl Future<Output = Result<ScanChunk, BackendError>> + Send {
            async move {
                self.scan_calls.fetch_add(1, Ordering::Relaxed);
                self.scan_ranges.lock().expect("spy lock").push(range);
                Ok(ScanChunk {
                    entries: Vec::new(),
                    has_more: false,
                })
            }
        }
    }

    #[derive(Clone)]
    struct OverlapRead {
        entered: Arc<tokio::sync::Barrier>,
        release: Arc<tokio::sync::Semaphore>,
    }

    impl BackendRead for OverlapRead {
        async fn get_many(
            &self,
            _space: SpaceId,
            _keys: &[Key],
            _opts: GetOptions,
        ) -> Result<GetManyResult, BackendError> {
            self.entered.wait().await;
            self.release
                .acquire()
                .await
                .expect("overlap test semaphore should stay open")
                .forget();
            Ok(GetManyResult::new(Vec::new()))
        }

        async fn scan(
            &self,
            _space: SpaceId,
            _range: KeyRange,
            _opts: ScanOptions,
        ) -> Result<ScanChunk, BackendError> {
            unreachable!("overlap test does not scan")
        }
    }

    #[tokio::test]
    async fn cloned_shared_reads_overlap_on_one_snapshot() {
        let entered = Arc::new(tokio::sync::Barrier::new(3));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let shared = SharedStorageRead::new(StorageReadScope::new(OverlapRead {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        }));

        let left = shared.clone();
        let left_task =
            tokio::spawn(
                async move { left.get_many(SpaceId(1), &[], GetOptions::default()).await },
            );
        let right = shared.clone();
        let right_task =
            tokio::spawn(
                async move { right.get_many(SpaceId(1), &[], GetOptions::default()).await },
            );

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
        let read = StorageReadScope::new(spy);
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
        let read = StorageReadScope::new(spy);
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
            assert_eq!(result.stats.unique_backend_keys, 2);
            assert_eq!(result.stats.backend_calls, 1);
        }
        assert_eq!(*seen.lock().expect("spy lock"), [key("a"), key("b")]);
    }

    #[tokio::test]
    async fn prefix_plans_lower_edge_case_bounds() {
        let spy = SpyRead::default();
        let ranges = Arc::clone(&spy.scan_ranges);
        let read = StorageReadScope::new(spy);

        for prefix in [b"".as_slice(), b"\xff".as_slice(), b"\x00\xff".as_slice()] {
            ScanPlan::prefix(
                space(),
                Prefix {
                    bytes: Bytes::copy_from_slice(prefix),
                },
            )
            .collect(&read, ScanOptions::default())
            .await
            .expect("prefix scan");
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
    async fn zero_limit_prefix_scan_skips_backend_and_reports_shape() {
        let spy = SpyRead::default();
        let scan_calls = Arc::clone(&spy.scan_calls);
        let read = StorageReadScope::new(spy);
        let result = ScanPlan::prefix(
            space(),
            Prefix {
                bytes: Bytes::from_static(b"a"),
            },
        )
        .collect(
            &read,
            ScanOptions {
                projection: CoreProjection::KeyOnly,
                limit_rows: 0,
                resume_after: Some(key("resume")),
            },
        )
        .await
        .expect("zero-limit prefix scan");

        assert!(result.value.entries.is_empty());
        assert!(!result.value.has_more);
        assert_eq!(scan_calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            result.stats,
            StorageReadStats {
                prefix_lowered: 1,
                prefix_scan_chunks: 1,
                scan_key_only_chunks: 1,
                scan_resume_after: 1,
                ..StorageReadStats::default()
            }
        );
    }

    #[tokio::test]
    async fn scan_plan_returns_owned_pages_and_honors_resume_key() {
        let storage = StorageContext::new(InMemoryBackend::new());
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
        let plan = ScanPlan::range(
            space(),
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
        );

        let page = plan
            .collect(
                &read,
                ScanOptions {
                    limit_rows: 1,
                    ..ScanOptions::default()
                },
            )
            .await
            .expect("first page");
        assert_eq!(page.value.entries[0].key, key("a"));
        assert!(page.value.has_more);

        let next = plan
            .collect(
                &read,
                ScanOptions {
                    resume_after: Some(page.value.entries[0].key.clone()),
                    ..ScanOptions::default()
                },
            )
            .await
            .expect("second page");
        assert_eq!(
            next.value
                .entries
                .into_iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>(),
            [key("b"), key("c")]
        );
    }
}
