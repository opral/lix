use std::ops::Bound;

use futures_io::AsyncRead;
use futures_lite::io::sink;

use super::format::{
    SnapshotDecoder, SnapshotEncoder, SnapshotEntry, SnapshotTrailer, invalid_snapshot,
};
use crate::migration::{MigrationStatus, begin_fresh_epoch_import, inspect_lix_with_adapter};
use crate::storage_adapter::{
    MAX_SCAN_PAGE_ROWS, PutBatch, PutEntry, StorageAdapter, StorageAdapterRead as _,
    StorageBeginScanOptions, StorageCoreProjection, StorageKey, StorageKeyRange,
    StorageProjectedValue, StorageReadOptions, StorageSession, StorageSpaceId, StorageValue,
    Storage,
};
use crate::LixError;

pub(crate) async fn restore_snapshot<S, R>(
    storage: StorageSession<S>,
    source: R,
) -> Result<StorageSession<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    R: AsyncRead + Unpin + Send + 'static,
{
    let import = begin_fresh_epoch_import(storage).await?;
    let restored = restore_into_candidate(&import, source).await;
    match restored {
        Ok(format) => import.publish(format).await,
        Err(error) => match import.abort().await {
            Ok(()) => Err(error),
            Err(abort_error) => Err(LixError::new(
                abort_error.code,
                format!(
                    "{error}; snapshot restore cleanup also failed: {}",
                    abort_error.message
                ),
            )),
        },
    }
}

async fn restore_into_candidate<S, R>(
    import: &crate::migration::FreshEpochImport<StorageSession<S>>,
    source: R,
) -> Result<u32, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    R: AsyncRead + Unpin,
{
    let (header, mut decoder) = SnapshotDecoder::new(source).await?;
    if header.lix_format_version == 0
        || header.lix_format_version > crate::init::CURRENT_FORMAT_VERSION
    {
        return Err(invalid_snapshot(format!(
            "snapshot Lix format version {} is not supported by this engine (current {})",
            header.lix_format_version,
            crate::init::CURRENT_FORMAT_VERSION
        )));
    }
    let mut batch_space: Option<StorageSpaceId> = None;
    let mut batch = Vec::with_capacity(MAX_SCAN_PAGE_ROWS);
    let mut batch_bytes = 0_usize;
    while let Some(entry) = decoder.next_entry().await? {
        let space = super::snapshot_space(entry.space_id).ok_or_else(|| {
            invalid_snapshot(format!(
                "snapshot contains unknown logical space 0x{:08x}",
                entry.space_id
            ))
        })?;
        let entry_bytes = entry
            .key
            .len()
            .checked_add(entry.value.len())
            .ok_or_else(|| invalid_snapshot("snapshot entry byte length overflowed"))?;
        const MAX_IMPORT_BATCH_BYTES: usize = 8 * 1024 * 1024;
        if batch_space.is_some_and(|current| current != space.id)
            || batch.len() == MAX_SCAN_PAGE_ROWS
            || (!batch.is_empty()
                && batch_bytes
                    .checked_add(entry_bytes)
                    .is_none_or(|bytes| bytes > MAX_IMPORT_BATCH_BYTES))
        {
            let current = batch_space.expect("a nonempty snapshot batch has a space");
            let current = super::snapshot_space(current.0)
                .expect("a validated snapshot space remains registered");
            import
                .write_exact_batch(current, PutBatch { entries: batch })
                .await?;
            batch = Vec::with_capacity(MAX_SCAN_PAGE_ROWS);
            batch_bytes = 0;
        }
        batch_space = Some(space.id);
        batch_bytes = batch_bytes
            .checked_add(entry_bytes)
            .ok_or_else(|| invalid_snapshot("snapshot import batch byte length overflowed"))?;
        batch.push(PutEntry {
            key: StorageKey(entry.key),
            value: StorageValue { bytes: entry.value },
        });
    }
    if !batch.is_empty() {
        let current = batch_space.expect("a nonempty snapshot batch has a space");
        let current = super::snapshot_space(current.0)
            .expect("a validated snapshot space remains registered");
        import
            .write_exact_batch(current, PutBatch { entries: batch })
            .await?;
    }
    let expected = decoder
        .trailer()
        .ok_or_else(|| invalid_snapshot("snapshot has no verified trailer"))?;

    validate_protocol_version(import.candidate(), header.lix_format_version).await?;
    let actual = candidate_digest(import.candidate(), header.lix_format_version).await?;
    if actual != expected {
        return Err(invalid_snapshot(
            "restored snapshot does not match the verified source payload",
        ));
    }
    // Decode and verify the complete artifact before reporting that its
    // embedded Lix format cannot be upgraded. This keeps integrity checking
    // independent from engine compatibility and makes old fixtures useful as
    // immutable wire evidence.
    if header.lix_format_version < crate::init::CURRENT_FORMAT_VERSION
        && !crate::migration::has_complete_migration_path(
            header.lix_format_version,
            crate::init::CURRENT_FORMAT_VERSION,
        )
    {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_FAILED",
            format!(
                "Lix snapshot format v{} has no registered upgrade path to v{}",
                header.lix_format_version,
                crate::init::CURRENT_FORMAT_VERSION
            ),
        ));
    }
    Ok(header.lix_format_version)
}

async fn validate_protocol_version<S>(
    candidate: &StorageAdapter<S>,
    expected: u32,
) -> Result<(), LixError>
where
    S: Storage,
{
    let observed = inspect_lix_with_adapter(candidate).await?;
    let observed_version = match observed {
        MigrationStatus::Current { version } => version,
        MigrationStatus::Required { from_version, .. } => from_version,
        MigrationStatus::TooNew { found_version, .. } => found_version,
        MigrationStatus::Missing | MigrationStatus::Malformed => {
            return Err(invalid_snapshot(
                "snapshot has no valid Lix format marker",
            ));
        }
    };
    if observed_version != expected {
        return Err(invalid_snapshot(format!(
            "snapshot header declares Lix format {expected}, but its state contains format {observed_version}"
        )));
    }
    Ok(())
}

async fn candidate_digest<S>(
    candidate: &StorageAdapter<S>,
    format: u32,
) -> Result<SnapshotTrailer, LixError>
where
    S: Storage,
{
    let mut output = sink();
    let mut encoder = SnapshotEncoder::new(&mut output, format).await?;
    // The wire registry is append-only. Rehash spaces retired by a future
    // engine too, so an old snapshot is verified exactly before migration.
    for space in crate::storage_spaces::SNAPSHOT_STORAGE_SPACES
        .iter()
        .copied()
    {
        let mut lower = Bound::Unbounded;
        loop {
            // Heartbeat commits may expire an OPFS read generation. Reopen one
            // bounded read per page; the hidden candidate itself is immutable.
            // Every candidate batch already completed an await-durable
            // commit. Rehash through a normal coherent read so adapters such
            // as Memory, which have no distinct durable-read boundary, remain
            // valid restore destinations.
            let read = candidate
                .begin_read(StorageReadOptions::default())
                .await?;
            let mut cursor = read
                .begin_scan(
                    space,
                    StorageKeyRange {
                        lower: lower.clone(),
                        upper: Bound::Unbounded,
                    },
                    StorageBeginScanOptions {
                        projection: StorageCoreProjection::FullValue,
                        ..StorageBeginScanOptions::default()
                    },
                )
                .await?;
            let (entries, has_more) = cursor
                .next_page(MAX_SCAN_PAGE_ROWS)
                .await?
                .into_parts();
            if entries.is_empty() {
                break;
            }
            lower = Bound::Excluded(
                entries
                    .last()
                    .expect("a nonempty page has a last key")
                    .key
                    .clone(),
            );
            for entry in entries {
                let StorageProjectedValue::FullValue(value) = entry.value else {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "full-value snapshot verification returned a key-only entry",
                    ));
                };
                encoder
                    .write_entry(&SnapshotEntry {
                        space_id: space.id.0,
                        key: entry.key.0,
                        value,
                    })
                    .await?;
            }
            if !has_more {
                break;
            }
        }
    }
    encoder.finish().await
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future as _,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        time::Duration,
    };

    use bytes::Bytes;
    use futures_io::AsyncRead;
    use futures_lite::io::Cursor;

    use crate::storage_adapter::{
        Memory, MemoryRead, MemoryWrite, PutBatch, REPOSITORY_EPOCH_KEY,
        REPOSITORY_EPOCH_SPACE, Storage, StorageCommitResult, StorageCoreProjection, StorageError,
        StorageGetManyRequest, StorageGetOptions, StorageKey, StorageKeyRange,
        StorageProjectedValue, StorageRead as _, StorageReadOptions, StorageSession,
        StorageSessionToken, StorageSpace, StorageWriteOptions,
    };
    use crate::open_lix;

    struct PendingSnapshotReader;

    #[derive(Clone)]
    struct DelayedFirstCommitStorage {
        inner: Memory,
        gate: Arc<DelayedFirstCommitGate>,
    }

    struct DelayedFirstCommitGate {
        delay_first: AtomicBool,
        commit_attempts: AtomicUsize,
        first_started: tokio::sync::Semaphore,
        release_first: tokio::sync::Semaphore,
        first_finished: tokio::sync::Semaphore,
    }

    impl DelayedFirstCommitStorage {
        fn new() -> Self {
            Self {
                inner: Memory::new(),
                gate: Arc::new(DelayedFirstCommitGate {
                    delay_first: AtomicBool::new(true),
                    commit_attempts: AtomicUsize::new(0),
                    first_started: tokio::sync::Semaphore::new(0),
                    release_first: tokio::sync::Semaphore::new(0),
                    first_finished: tokio::sync::Semaphore::new(0),
                }),
            }
        }
    }

    impl Storage for DelayedFirstCommitStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;

        type Write<'a>
            = DelayedFirstCommitWrite
        where
            Self: 'a;

        async fn acquire_session(&self) -> Result<StorageSessionToken, StorageError> {
            self.inner.acquire_session().await
        }

        async fn begin_read(
            &self,
            options: StorageReadOptions,
        ) -> Result<Self::Read<'_>, StorageError> {
            self.inner.begin_read(options).await
        }

        async fn begin_write(
            &self,
            options: StorageWriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            Ok(DelayedFirstCommitWrite {
                inner: self.inner.begin_write(options).await?,
                gate: Arc::clone(&self.gate),
            })
        }
    }

    struct DelayedFirstCommitWrite {
        inner: MemoryWrite,
        gate: Arc<DelayedFirstCommitGate>,
    }

    impl crate::storage::StorageWrite for DelayedFirstCommitWrite {
        async fn put_many(
            &mut self,
            space: StorageSpace,
            entries: PutBatch,
        ) -> Result<(), StorageError> {
            self.inner.put_many(space, entries).await
        }

        async fn replace_many(
            &mut self,
            space: StorageSpace,
            entries: PutBatch,
        ) -> Result<(), StorageError> {
            self.inner.replace_many(space, entries).await
        }

        async fn delete_many(
            &mut self,
            space: StorageSpace,
            keys: &[StorageKey],
        ) -> Result<(), StorageError> {
            self.inner.delete_many(space, keys).await
        }

        async fn delete_range(
            &mut self,
            space: StorageSpace,
            range: StorageKeyRange,
        ) -> Result<(), StorageError> {
            self.inner.delete_range(space, range).await
        }

        async fn commit(self) -> Result<StorageCommitResult, StorageError> {
            self.gate.commit_attempts.fetch_add(1, Ordering::SeqCst);
            if !self.gate.delay_first.swap(false, Ordering::SeqCst) {
                return self.inner.commit().await;
            }

            let gate = Arc::clone(&self.gate);
            let (report, result) = tokio::sync::oneshot::channel();
            crate::background_task::spawn("lix-delayed-snapshot-claim-test", move || async move {
                gate.first_started.add_permits(1);
                gate.release_first
                    .acquire()
                    .await
                    .expect("delayed claim release stays open")
                    .forget();
                let committed = self.inner.commit().await;
                gate.first_finished.add_permits(1);
                let _ = report.send(committed);
            })
            .map_err(|error| StorageError::Io(error.to_string()))?;
            result.await.map_err(|_| {
                StorageError::Io("delayed snapshot claim task stopped".to_string())
            })?
        }

        async fn rollback(self) -> Result<(), StorageError> {
            self.inner.rollback().await
        }
    }

    impl AsyncRead for PendingSnapshotReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Pending
        }
    }

    async fn epoch_pointer<S>(storage: &StorageSession<S>) -> Option<Bytes>
    where
        S: Storage,
    {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read epoch pointer");
        let keys = [StorageKey(Bytes::from_static(REPOSITORY_EPOCH_KEY))];
        let values = read
            .get_many(&[StorageGetManyRequest {
                space: REPOSITORY_EPOCH_SPACE,
                keys: &keys,
                opts: StorageGetOptions {
                    projection: StorageCoreProjection::FullValue,
                },
            }])
            .await
            .expect("load epoch pointer");
        values
            .values
            .into_iter()
            .next()
            .flatten()
            .and_then(|value| match value {
                StorageProjectedValue::FullValue(bytes) => Some(bytes),
                StorageProjectedValue::KeyOnly => None,
            })
    }

    #[tokio::test]
    async fn dropping_restore_cleans_claim_and_allows_prompt_retry() {
        let source = open_lix().await.expect("open snapshot source");
        let mut snapshot = Vec::new();
        source
            .export_snapshot()
            .write_to(&mut snapshot)
            .await
            .expect("export retry snapshot");

        let storage = Memory::new();
        let session = StorageSession::acquire(storage)
            .await
            .expect("acquire restore storage session");
        let mut cancelled = Box::pin(super::restore_snapshot(
            session.clone(),
            PendingSnapshotReader,
        ));
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(
            cancelled.as_mut().poll(&mut context),
            Poll::Pending
        ));
        let claim = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(claim) = epoch_pointer(&session).await {
                    break claim;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending restore did not publish its hidden claim");
        assert!(claim.starts_with(b"lix.repository-epoch.v1|migrating|"));

        drop(cancelled);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if epoch_pointer(&session).await.is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("cancelled restore claim was not cleaned promptly");

        tokio::time::timeout(
            Duration::from_secs(1),
            super::restore_snapshot(session, Cursor::new(snapshot)),
        )
        .await
        .expect("retry waited for lease recovery")
        .expect("retry restore succeeds after cancellation cleanup");
    }

    #[tokio::test]
    async fn dropping_restore_before_claim_visibility_cleans_settled_claim() {
        let source = open_lix().await.expect("open snapshot source");
        let mut snapshot = Vec::new();
        source
            .export_snapshot()
            .write_to(&mut snapshot)
            .await
            .expect("export retry snapshot");

        let storage = DelayedFirstCommitStorage::new();
        let gate = Arc::clone(&storage.gate);
        let session = StorageSession::acquire(storage)
            .await
            .expect("acquire delayed restore storage session");
        let mut cancelled = Box::pin(super::restore_snapshot(
            session.clone(),
            PendingSnapshotReader,
        ));
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(
            cancelled.as_mut().poll(&mut context),
            Poll::Pending
        ));
        tokio::time::timeout(Duration::from_secs(1), gate.first_started.acquire())
            .await
            .expect("fresh claim commit did not start")
            .expect("fresh claim start signal stays open")
            .forget();
        assert!(
            epoch_pointer(&session).await.is_none(),
            "delayed fresh claim must not be visible before cancellation"
        );

        drop(cancelled);

        // Give an incorrectly eager cleanup owner time to observe the absent
        // pointer and exit before the detached commit is released. The fixed
        // owner is still awaiting that commit, so it cannot clean prematurely.
        let _ = tokio::time::timeout(Duration::from_millis(100), async {
            while gate.commit_attempts.load(Ordering::SeqCst) < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        gate.release_first.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), gate.first_finished.acquire())
            .await
            .expect("delayed fresh claim did not finish")
            .expect("fresh claim finish signal stays open")
            .forget();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if epoch_pointer(&session).await.is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("cancelled pre-visibility claim was orphaned");

        tokio::time::timeout(
            Duration::from_secs(1),
            super::restore_snapshot(session, Cursor::new(snapshot)),
        )
        .await
        .expect("retry waited for lease recovery")
        .expect("retry restore succeeds after pre-visibility cancellation");
    }
}
