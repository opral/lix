//! Migration control/target commits may revoke physical reads (notably OPFS).
//! Reopen bounded read units while checking the exact epoch fence and the
//! bank mutation revision. This supports immutable source inspection and
//! candidate read-only planning between writes. Candidate publication must
//! retain its revision precondition; a planner never spans its own writes.
use super::*;
use crate::storage_adapter::StorageScanOrder as ScanOrder;

pub(crate) struct MigrationPlanningRead<S> {
    inner: Arc<MigrationReadState<S>>,
}

struct MigrationReadState<S> {
    source: StorageAdapter<S>,
    revision: Option<Bytes>,
}

impl<S> Clone for MigrationPlanningRead<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S: Storage + Clone + Send + Sync + 'static> MigrationPlanningRead<S> {
    pub(crate) async fn new(source: &StorageAdapter<S>) -> Result<Self, StorageError> {
        let revision = retry_read(|| source.load_mutation_revision()).await?;
        Ok(Self {
            inner: Arc::new(MigrationReadState {
                source: source.clone(),
                revision,
            }),
        })
    }

    pub(crate) fn finish(self) -> Result<(), StorageError> {
        Arc::try_unwrap(self.inner).map_err(|read| {
            StorageError::Io(format!(
                "migration planning read still has {} active handles",
                Arc::strong_count(&read) - 1
            ))
        })?;
        Ok(())
    }

    async fn read(
        &self,
    ) -> Result<crate::storage_adapter::StorageAdapterReadScope<S::Read<'_>>, StorageError> {
        let read = self.inner.source.begin_read(ReadOptions::default()).await?;
        if StorageAdapter::<S>::load_mutation_revision_from_read(&read).await?
            != self.inner.revision
        {
            return Err(StorageError::Fenced);
        }
        Ok(read)
    }
}

impl<S: Storage + Clone + Send + Sync + 'static> crate::storage_adapter::StorageAdapterRead
    for MigrationPlanningRead<S>
{
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        if requests
            .iter()
            .any(|request| request.space == REPOSITORY_EPOCH_SPACE)
        {
            return Err(StorageError::Fenced);
        }
        retry_read(|| async {
            let read = self.read().await?;
            read.get_many(requests).await
        })
        .await
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        if space == REPOSITORY_EPOCH_SPACE {
            return Err(StorageError::Fenced);
        }
        ScanCursor::from_source(
            range.clone(),
            opts.order,
            FrozenScan {
                source: self,
                space,
                range,
                opts,
            },
        )
    }
}

struct FrozenScan<'a, S> {
    source: &'a MigrationPlanningRead<S>,
    space: StorageSpace,
    range: KeyRange,
    opts: BeginScanOptions,
}
impl<S: Storage + Clone + Send + Sync + 'static> StorageScanSource for FrozenScan<'_, S> {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let page = retry_read(|| async {
                let read = self.source.read().await?;
                let mut cursor = read
                    .begin_scan(self.space, self.range.clone(), self.opts)
                    .await?;
                cursor.next_page(limit_rows.min(MAX_SCAN_PAGE_ROWS)).await
            })
            .await?;
            let (entries, has_more) = page.into_parts();
            if let Some(last) = entries.last() {
                match self.opts.order {
                    ScanOrder::Ascending => self.range.lower = Bound::Excluded(last.key.clone()),
                    ScanOrder::Descending => self.range.upper = Bound::Excluded(last.key.clone()),
                }
            }
            Ok(ScanChunk::new(entries, has_more))
        })
    }
}

async fn retry_read<T, F, Fut>(mut operation: F) -> Result<T, StorageError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    let mut retry = crate::common::ExpiredReadRetryState::default();
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(StorageError::ReadExpired) => {
                let error = LixError::from(StorageError::ReadExpired);
                let Some(delay) = retry.next_delay(&error) else {
                    return Err(StorageError::ReadExpired);
                };
                tokio::task::yield_now().await;
                if !delay.is_zero() {
                    crate::sync::sleep(delay).await;
                }
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::CommitExpiringStorage;
    use super::*;
    const SPACE: StorageSpace = crate::storage_spaces::RETIRED_JSON_SPACE;

    async fn fixture() -> (
        CommitExpiringStorage,
        StorageAdapter<CommitExpiringStorage>,
        Bytes,
    ) {
        let storage = CommitExpiringStorage::new();
        let seed = StorageAdapter::for_epoch_unfenced(storage.clone(), EpochBank::A);
        let mut writes = seed.new_write_set();
        for i in 0_u8..6 {
            writes.put(SPACE, vec![i], vec![i]);
        }
        seed.commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let claim = encode_pointer(PointerState::Migrating {
            source: EpochBank::A,
            source_format: crate::init::CURRENT_FORMAT_VERSION,
            target: EpochBank::B,
            generation: 1,
            attempt: uuid::Uuid::now_v7(),
        });
        let mut write = storage.begin_write(Default::default()).await.unwrap();
        write
            .put_many(
                REPOSITORY_EPOCH_SPACE,
                PutBatch {
                    entries: vec![
                        PutEntry {
                            key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
                            value: StoredValue {
                                bytes: claim.clone(),
                            },
                        },
                        PutEntry {
                            key: Key(Bytes::from_static(REPOSITORY_EPOCH_LEASE_KEY)),
                            value: StoredValue {
                                bytes: Bytes::from_static(b"0"),
                            },
                        },
                    ],
                },
            )
            .await
            .unwrap();
        write.commit().await.unwrap();
        let source =
            StorageAdapter::for_epoch_migration(storage.clone(), EpochBank::A, claim.clone());
        (storage, source, claim)
    }

    #[tokio::test]
    async fn planning_read_finish_rejects_live_clones() {
        let (_, source, _) = fixture().await;
        let read = MigrationPlanningRead::new(&source).await.unwrap();
        let clone = read.clone();
        assert!(matches!(read.finish(), Err(StorageError::Io(_))));
        clone.finish().unwrap();
    }

    #[tokio::test]
    async fn frozen_source_survives_heartbeat_and_target_commits_between_scan_pages() {
        for projection in [CoreProjection::FullValue, CoreProjection::KeyOnly] {
            let (storage, source, claim) = fixture().await;
            let ordinary = source.begin_read(Default::default()).await.unwrap();
            let frozen = MigrationPlanningRead::new(&source).await.unwrap();
            let keys = [Key(Bytes::from_static(&[2]))];
            let requests = [GetManyRequest {
                space: SPACE,
                keys: &keys,
                opts: GetOptions::default(),
            }];
            let expected = frozen.get_many(&requests).await.unwrap().values;
            let range = KeyRange {
                lower: Bound::Excluded(Key(Bytes::from_static(&[0]))),
                upper: Bound::Included(Key(Bytes::from_static(&[4]))),
            };
            let mut scan = frozen
                .begin_scan(
                    SPACE,
                    range,
                    BeginScanOptions {
                        order: ScanOrder::Ascending,
                        projection,
                    },
                )
                .await
                .unwrap();
            let (first, more) = scan.next_page(2).await.unwrap().into_parts();
            assert!(more);
            advance_lease(
                &storage,
                &claim,
                &Bytes::from_static(b"0"),
                &Bytes::from_static(b"1"),
            )
            .await
            .unwrap();
            let target = StorageAdapter::for_epoch_migration(storage.clone(), EpochBank::B, claim);
            let mut writes = target.new_write_set();
            writes.put(SPACE, vec![2], vec![99]);
            target
                .commit_write_set(writes, Default::default())
                .await
                .unwrap();
            assert!(matches!(
                ordinary.get_many(&requests).await,
                Err(StorageError::ReadExpired)
            ));
            assert_eq!(frozen.get_many(&requests).await.unwrap().values, expected);
            storage.expire_next_page();
            let (second, more) = scan.next_page(2).await.unwrap().into_parts();
            assert!(!more);
            let keys = first
                .into_iter()
                .chain(second)
                .map(|entry| entry.key.0[0])
                .collect::<Vec<_>>();
            assert_eq!(keys, vec![1, 2, 3, 4]);
        }
    }

    #[tokio::test]
    async fn frozen_source_rejects_changed_source_revision_and_replaced_migration_claim() {
        for change_source in [true, false] {
            let (storage, source, claim) = fixture().await;
            let frozen = MigrationPlanningRead::new(&source).await.unwrap();
            let mut scan = frozen
                .begin_scan(
                    SPACE,
                    KeyRange {
                        lower: Bound::Unbounded,
                        upper: Bound::Unbounded,
                    },
                    Default::default(),
                )
                .await
                .unwrap();
            let (first, more) = scan.next_page(1).await.unwrap().into_parts();
            assert_eq!(first.len(), 1);
            assert!(more);
            if change_source {
                let mut writes = source.new_write_set();
                writes.put(SPACE, vec![2], vec![99]);
                source
                    .commit_write_set(writes, Default::default())
                    .await
                    .unwrap();
            } else {
                let active = encode_pointer(PointerState::Active {
                    bank: EpochBank::B,
                    generation: 1,
                    format: crate::init::CURRENT_FORMAT_VERSION,
                    publication: None,
                });
                replace_pointer(&storage, &claim, &active).await.unwrap();
            }
            assert!(matches!(scan.next_page(1).await, Err(StorageError::Fenced)));
            let keys = [Key(Bytes::from_static(&[2]))];
            assert!(matches!(
                frozen
                    .get_many(&[GetManyRequest {
                        space: SPACE,
                        keys: &keys,
                        opts: GetOptions::default()
                    }])
                    .await,
                Err(StorageError::Fenced)
            ));
        }
    }
    #[tokio::test]
    async fn frozen_source_preserves_unsupported_scan_errors() {
        let (_, source, _) = fixture().await;
        let frozen = MigrationPlanningRead::new(&source).await.unwrap();
        let mut scan = frozen
            .begin_scan(
                SPACE,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                BeginScanOptions {
                    order: ScanOrder::Descending,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            scan.next_page(1).await,
            Err(StorageError::Unsupported(
                crate::storage_adapter::StorageCapability::ReverseScan
            ))
        ));
    }
}
