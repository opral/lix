#![allow(dead_code)]

mod frozen_oracle {
    include!(concat!(
        env!("OUT_DIR"),
        "/forktree_stage2_recovery_no_lease_includable.rs"
    ));

    #[cfg(test)]
    mod adversarial {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
        use std::sync::{Arc, Mutex};
        use tokio::sync::Notify;
        use tokio::time::{Duration, timeout};

        struct DeletePageGate {
            target: Key,
            committed: Notify,
            release: Notify,
        }

        struct GatedStorage<S> {
            inner: S,
            gate: Arc<DeletePageGate>,
        }

        struct GatedWrite<W> {
            inner: W,
            gate: Arc<DeletePageGate>,
            deletes_target: bool,
        }

        struct ReadBoundary {
            armed: AtomicBool,
            reads: AtomicUsize,
            reached: Notify,
            release: Notify,
        }

        struct ReadBoundaryStorage<S> {
            inner: S,
            boundary: Arc<ReadBoundary>,
        }

        impl<S: Storage> Storage for ReadBoundaryStorage<S> {
            type Read<'a>
                = S::Read<'a>
            where
                Self: 'a;
            type Write<'a>
                = S::Write<'a>
            where
                Self: 'a;

            async fn begin_read(
                &self,
                options: ReadOptions,
            ) -> Result<Self::Read<'_>, StorageError> {
                if self.boundary.armed.load(AtomicOrdering::Acquire)
                    && self.boundary.reads.fetch_add(1, AtomicOrdering::AcqRel) == 1
                {
                    self.boundary.reached.notify_one();
                    self.boundary.release.notified().await;
                }
                self.inner.begin_read(options).await
            }

            async fn begin_write(
                &self,
                options: WriteOptions,
            ) -> Result<Self::Write<'_>, StorageError> {
                self.inner.begin_write(options).await
            }
        }

        impl<S: Storage> Storage for GatedStorage<S> {
            type Read<'a>
                = S::Read<'a>
            where
                Self: 'a;
            type Write<'a>
                = GatedWrite<S::Write<'a>>
            where
                Self: 'a;

            async fn begin_read(
                &self,
                options: ReadOptions,
            ) -> Result<Self::Read<'_>, StorageError> {
                self.inner.begin_read(options).await
            }

            async fn begin_write(
                &self,
                options: WriteOptions,
            ) -> Result<Self::Write<'_>, StorageError> {
                Ok(GatedWrite {
                    inner: self.inner.begin_write(options).await?,
                    gate: Arc::clone(&self.gate),
                    deletes_target: false,
                })
            }
        }

        impl<W: StorageWrite> StorageWrite for GatedWrite<W> {
            async fn put_many(
                &mut self,
                space: lix::storage::StorageSpace,
                entries: PutBatch,
            ) -> Result<(), StorageError> {
                self.inner.put_many(space, entries).await
            }

            async fn delete_many(
                &mut self,
                space: lix::storage::StorageSpace,
                keys: &[Key],
            ) -> Result<(), StorageError> {
                if space == OBJECTS && keys.contains(&self.gate.target) {
                    self.deletes_target = true;
                }
                self.inner.delete_many(space, keys).await
            }

            async fn delete_range(
                &mut self,
                space: lix::storage::StorageSpace,
                range: KeyRange,
            ) -> Result<(), StorageError> {
                self.inner.delete_range(space, range).await
            }

            async fn commit(self) -> Result<lix::storage::CommitResult, StorageError> {
                let deletes_target = self.deletes_target;
                let gate = Arc::clone(&self.gate);
                let result = self.inner.commit().await;
                if deletes_target && result.is_ok() {
                    gate.committed.notify_one();
                    gate.release.notified().await;
                }
                result
            }

            async fn rollback(self) -> Result<(), StorageError> {
                self.inner.rollback().await
            }
        }

        async fn deletion_page_first_must_fence_prepared_publish<S: Storage>(storage: S) -> Id {
            let mut setup_metrics = Metrics::default();
            let initial = seed(&storage, &mut setup_metrics).await.expect("seed");
            let prepared_gc = prepare_gc(&storage, &mut setup_metrics)
                .await
                .expect("prepare GC");
            start_gc(&storage, &prepared_gc, &mut setup_metrics)
                .await
                .expect("start GC");

            let successor = graph(101);
            stage(&storage, &successor, &mut setup_metrics)
                .await
                .expect("stage successor after mark");
            let prepared_publish = prepare_publish(
                &storage,
                initial.root.id,
                successor.root.id,
                &mut setup_metrics,
            )
            .await
            .expect("prepare publication against initial GC progress");

            let gate = Arc::new(DeletePageGate {
                target: successor.objects[0].key.clone(),
                committed: Notify::new(),
                release: Notify::new(),
            });
            let storage = GatedStorage {
                inner: storage,
                gate: Arc::clone(&gate),
            };
            let gc_metrics = Mutex::new(Metrics::default());
            let publish_metrics = Mutex::new(Metrics::default());

            let resume = async {
                let mut metrics = Metrics::default();
                let result = resume_gc(&storage, &mut metrics).await;
                *gc_metrics.lock().expect("GC metrics") = metrics;
                result
            };
            let publish = async {
                timeout(Duration::from_secs(10), gate.committed.notified())
                    .await
                    .expect("real GC deletion page did not commit");
                let mut metrics = Metrics::default();
                let result = commit_publish(&storage, &prepared_publish, &mut metrics).await;
                *publish_metrics.lock().expect("publish metrics") = metrics;
                gate.release.notify_one();
                result
            };
            let (resume_result, publish_result) = tokio::join!(resume, publish);

            assert!(
                matches!(publish_result, Err(StorageError::PreconditionFailed(_))),
                "a publication prepared at p0 must fail after a real deletion page commits and rotates p0 to p1"
            );
            resume_result.expect("GC must finish after winning the deletion-page race");
            let mut verify_metrics = Metrics::default();
            verify_active(&storage, initial.root.id, &mut verify_metrics)
                .await
                .expect("failed publication cannot change active authority");
            graph_present(&storage, &successor, false, &mut verify_metrics)
                .await
                .expect("GC must reclaim the unpublished successor");

            // The losing writer must restage bytes before retrying; a stale
            // publication cannot resurrect the objects reclaimed by GC.
            stage(&storage, &successor, &mut verify_metrics)
                .await
                .expect("restage successor after losing deletion-page race");
            let retry = prepare_publish(
                &storage,
                initial.root.id,
                successor.root.id,
                &mut verify_metrics,
            )
            .await
            .expect("prepare retry after restage");
            commit_publish(&storage, &retry, &mut verify_metrics)
                .await
                .expect("retry after restage");
            verify_active(&storage, successor.root.id, &mut verify_metrics)
                .await
                .expect("retried publication is active");

            // Reverse race: publication wins p0 and removes progress before
            // the already-prepared deletion transaction reaches commit.
            let prepared_gc = prepare_gc(&storage, &mut verify_metrics)
                .await
                .expect("prepare reverse-order GC");
            start_gc(&storage, &prepared_gc, &mut verify_metrics)
                .await
                .expect("start reverse-order GC");
            let winner = graph(102);
            stage(&storage, &winner, &mut verify_metrics)
                .await
                .expect("stage publication-first winner");
            let prepared_publish = prepare_publish(
                &storage,
                successor.root.id,
                winner.root.id,
                &mut verify_metrics,
            )
            .await
            .expect("prepare publication-first winner");
            let (raw_authority, _) = load_authority(&storage, &mut verify_metrics)
                .await
                .expect("load deletion-page authority");
            let raw_progress = progress_value(&storage, &mut verify_metrics)
                .await
                .expect("load deletion-page progress")
                .expect("active deletion-page progress");
            let mut progress = Progress::decode(&raw_progress).expect("decode deletion progress");
            commit_publish(&storage, &prepared_publish, &mut verify_metrics)
                .await
                .expect("publication wins reverse race");
            assert!(matches!(
                commit_gc_deletion_page(
                    &storage,
                    &raw_authority,
                    &raw_progress,
                    &mut progress,
                    OBJECTS,
                    vec![winner.objects[0].key.clone()],
                    &mut verify_metrics,
                )
                .await,
                Err(StorageError::PreconditionFailed(_))
            ));
            verify_active(&storage, winner.root.id, &mut verify_metrics)
                .await
                .expect("publication-first winner remains active");
            graph_present(&storage, &winner, true, &mut verify_metrics)
                .await
                .expect("losing deletion page cannot remove winner objects");
            assert_no_persisted_reader_state(&storage, &mut verify_metrics)
                .await
                .expect("no reader authority after either race order");
            winner.root.id
        }

        async fn same_root_distinct_reads_reject_cursor<S: Storage>(storage: &S) {
            let mut metrics = Metrics::default();
            seed(storage, &mut metrics).await.expect("seed");
            let first = open_pinned(storage, &mut metrics)
                .await
                .expect("first view");
            let second = open_pinned(storage, &mut metrics)
                .await
                .expect("second view");
            assert_eq!(first.root, second.root, "control requires the same root");
            let (_, cursor) = page(&first, None, None, PAGE_ROWS, &mut metrics)
                .await
                .expect("first page");
            let cursor = cursor.expect("continuation");
            assert!(
                matches!(
                    page(&second, Some(&cursor), None, PAGE_ROWS, &mut metrics).await,
                    Err(StorageError::InvalidCursor)
                ),
                "a cursor is scoped to one StorageRead instance even when root and authority bytes are identical"
            );
        }

        async fn deletion_between_validation_and_fence_capture_must_fail<S: Storage>(storage: S) {
            let boundary = Arc::new(ReadBoundary {
                armed: AtomicBool::new(false),
                reads: AtomicUsize::new(0),
                reached: Notify::new(),
                release: Notify::new(),
            });
            let storage = ReadBoundaryStorage {
                inner: storage,
                boundary: Arc::clone(&boundary),
            };
            let mut metrics = Metrics::default();
            let initial = seed(&storage, &mut metrics).await.expect("seed");
            let prepared_gc = prepare_gc(&storage, &mut metrics)
                .await
                .expect("prepare GC");
            start_gc(&storage, &prepared_gc, &mut metrics)
                .await
                .expect("start GC");
            let successor = graph(301);
            stage(&storage, &successor, &mut metrics)
                .await
                .expect("stage successor after mark");
            let (raw_authority, _) = load_authority(&storage, &mut metrics)
                .await
                .expect("load GC authority");
            let raw_progress = progress_value(&storage, &mut metrics)
                .await
                .expect("load GC progress")
                .expect("active GC progress");
            let mut progress = Progress::decode(&raw_progress).expect("decode GC progress");

            boundary.reads.store(0, AtomicOrdering::Release);
            boundary.armed.store(true, AtomicOrdering::Release);
            let prepare = async {
                let mut prepare_metrics = Metrics::default();
                prepare_publish(
                    &storage,
                    initial.root.id,
                    successor.root.id,
                    &mut prepare_metrics,
                )
                .await
            };
            let delete = async {
                timeout(Duration::from_secs(10), boundary.reached.notified())
                    .await
                    .expect("publisher did not reach post-validation authority read");
                let mut delete_metrics = Metrics::default();
                let result = commit_gc_deletion_page(
                    &storage,
                    &raw_authority,
                    &raw_progress,
                    &mut progress,
                    OBJECTS,
                    vec![successor.objects[0].key.clone()],
                    &mut delete_metrics,
                )
                .await;
                boundary.release.notify_one();
                result
            };
            let (prepared, deleted) = tokio::join!(prepare, delete);
            deleted.expect("delete page between validation and fence capture");
            let prepared = prepared.expect("publisher captures post-delete progress");
            let result = commit_publish(&storage, &prepared, &mut metrics).await;
            let (_, authority) = load_authority(&storage, &mut metrics)
                .await
                .expect("load authority after publication attempt");
            let root_validation = validate_root(
                &storage,
                Root {
                    kind: Kind::Catalog,
                    id: successor.root.id,
                },
                &mut metrics,
            )
            .await;
            assert!(
                matches!(&result, Err(StorageError::PreconditionFailed(_))),
                "root validation and progress capture must be one coherent fenced observation: publication={result:?}, active_is_deleted_successor={}, successor_validation={root_validation:?}",
                authority.active == successor.root.id,
            );
        }

        async fn real_page_error_poisons_held_view<S: Storage>(storage: &S) {
            let mut metrics = Metrics::default();
            let initial = seed(storage, &mut metrics).await.expect("seed");
            let mut malformed = graph(202);
            let raw = &mut malformed.rows[PAGE_ROWS].value.bytes;
            let mut corrupted = raw.to_vec();
            *corrupted.last_mut().expect("row checksum") ^= 1;
            *raw = Bytes::from(corrupted);
            stage(storage, &malformed, &mut metrics)
                .await
                .expect("stage malformed later page");
            let prepared =
                prepare_publish(storage, initial.root.id, malformed.root.id, &mut metrics)
                    .await
                    .expect("prepare malformed-row graph publication");
            commit_publish(storage, &prepared, &mut metrics)
                .await
                .expect("publish graph whose later row is malformed");

            let held = open_pinned(storage, &mut metrics).await.expect("held view");
            let (first_page, cursor) = page(&held, None, None, PAGE_ROWS, &mut metrics)
                .await
                .expect("first page remains valid");
            let last_authenticated = first_page.last().expect("first page row").0.clone();
            let cursor = cursor.expect("continuation into malformed page");
            assert!(matches!(
                page(&held, Some(&cursor), None, PAGE_ROWS, &mut metrics).await,
                Err(StorageError::Corruption(_))
            ));
            assert!(
                matches!(
                    page(&held, Some(&cursor), None, PAGE_ROWS, &mut metrics).await,
                    Err(StorageError::ReadExpired)
                ),
                "the real page error must poison continuation on the same still-held view"
            );

            let repaired = graph(203);
            stage(storage, &repaired, &mut metrics)
                .await
                .expect("stage repaired successor");
            let prepared =
                prepare_publish(storage, malformed.root.id, repaired.root.id, &mut metrics)
                    .await
                    .expect("prepare repaired publication");
            commit_publish(storage, &prepared, &mut metrics)
                .await
                .expect("publish repaired graph");
            let fresh = open_pinned(storage, &mut metrics)
                .await
                .expect("fresh view");
            let (restarted, _) = page(
                &fresh,
                None,
                Some(&last_authenticated),
                PAGE_ROWS,
                &mut metrics,
            )
            .await
            .expect("fresh restart after last authenticated key");
            assert_eq!(restarted.len(), PAGE_ROWS);
            assert!(restarted[0].0 > last_authenticated);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn rocks_deletion_page_first_fences_prepared_publish() {
            let directory = tempfile::tempdir().expect("RocksDB directory");
            let path = directory.path().to_path_buf();
            let active = deletion_page_first_must_fence_prepared_publish(
                RocksDB::open(directory.path()).expect("open RocksDB"),
            )
            .await;
            let reopened = RocksDB::open(path).expect("cold reopen RocksDB");
            let mut metrics = Metrics::default();
            verify_active(&reopened, active, &mut metrics)
                .await
                .expect("retried RocksDB publication survives cold reopen");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn slate_deletion_page_first_fences_prepared_publish() {
            let directory = tempfile::tempdir().expect("SlateDB directory");
            let path = directory.path().to_path_buf();
            let active = deletion_page_first_must_fence_prepared_publish(
                SlateDB::open_with_io_counters(directory.path(), SlateDBIoCounters::default())
                    .expect("open SlateDB"),
            )
            .await;
            let reopened = SlateDB::open_with_io_counters(path, SlateDBIoCounters::default())
                .expect("cold reopen SlateDB");
            let mut metrics = Metrics::default();
            verify_active(&reopened, active, &mut metrics)
                .await
                .expect("retried SlateDB publication survives cold reopen");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn rocks_same_root_distinct_reads_reject_cursor() {
            let directory = tempfile::tempdir().expect("RocksDB directory");
            let storage = RocksDB::open(directory.path()).expect("open RocksDB");
            same_root_distinct_reads_reject_cursor(&storage).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn slate_same_root_distinct_reads_reject_cursor() {
            let directory = tempfile::tempdir().expect("SlateDB directory");
            let storage =
                SlateDB::open_with_io_counters(directory.path(), SlateDBIoCounters::default())
                    .expect("open SlateDB");
            same_root_distinct_reads_reject_cursor(&storage).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn rocks_deletion_between_validation_and_fence_capture_fails() {
            let directory = tempfile::tempdir().expect("RocksDB directory");
            deletion_between_validation_and_fence_capture_must_fail(
                RocksDB::open(directory.path()).expect("open RocksDB"),
            )
            .await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn slate_deletion_between_validation_and_fence_capture_fails() {
            let directory = tempfile::tempdir().expect("SlateDB directory");
            deletion_between_validation_and_fence_capture_must_fail(
                SlateDB::open_with_io_counters(directory.path(), SlateDBIoCounters::default())
                    .expect("open SlateDB"),
            )
            .await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn rocks_real_page_error_poisons_held_view() {
            let directory = tempfile::tempdir().expect("RocksDB directory");
            let storage = RocksDB::open(directory.path()).expect("open RocksDB");
            real_page_error_poisons_held_view(&storage).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn slate_real_page_error_poisons_held_view() {
            let directory = tempfile::tempdir().expect("SlateDB directory");
            let storage =
                SlateDB::open_with_io_counters(directory.path(), SlateDBIoCounters::default())
                    .expect("open SlateDB");
            real_page_error_poisons_held_view(&storage).await;
        }
    }
}
