use super::context::SessionContext;
use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchReferenceRole};
use crate::storage_adapter::{SharedStorageAdapterRead, Storage, StorageReadOptions};

/// Options for switching a session to another branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchBranchOptions {
    pub branch_id: String,
}

/// Receipt returned after switching to another branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchBranchReceipt {
    pub branch_id: String,
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Switches this session's active branch.
    ///
    /// Clones of this session observe the switch in place. Independently
    /// opened sessions and the repository's default branch are unchanged.
    pub async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<SwitchBranchReceipt, LixError> {
        let branch_id = options.branch_id;
        // Keep the existing session/collaboration lease so branch deletion
        // cannot race target validation. A switch is normally session-local;
        // when the selected local branch pins an older global head, the lazy
        // auto-rebase below also publishes one metadata-only commit.
        let write_access = self.begin_session_write_access().await?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let reader = self.branch_ctx.ref_reader(&read);
        BranchLifecycle::new(&reader)
            .require_existing_commit_id(
                &branch_id,
                BranchOperation::SwitchBranch,
                BranchReferenceRole::Target,
            )
            .await?;
        self.ensure_open()?;
        let previous_branch_id = self.bound_branch_id()?;
        self.branch.set(branch_id.clone())?;
        self.observe_invalidation.bump();
        drop(reader);
        drop(read);
        drop(write_access);
        // Refresh at the explicit checkout boundary. This keeps observers and
        // other read helpers from discovering that they need an internal write
        // while already evaluating a stable snapshot. The selector must move
        // before the bump so the refresh's staleness gate sees the switch;
        // an error therefore restores the previous selector — a failed
        // switch must not leave the session silently on the target branch.
        if let Err(error) = self.refresh_active_branch_base_if_stale().await {
            self.branch.set(previous_branch_id)?;
            self.observe_invalidation.bump();
            return Err(error);
        }

        Ok(SwitchBranchReceipt { branch_id })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::CreateBranchOptions;
    use crate::engine::Engine;
    use crate::storage::{
        BeginScanOptions, GetManyRequest, GetManyResult, KeyRange, Memory, MemoryRead, MemoryWrite,
        ReadOptions, ScanCursor, Storage, StorageError, StorageRead, WriteOptions,
    };

    use super::*;

    #[derive(Clone)]
    struct CountingStorage {
        inner: Memory,
        counters: Arc<Counters>,
    }

    struct CountingRead {
        inner: MemoryRead,
        counters: Arc<Counters>,
    }

    #[derive(Default)]
    struct Counters {
        begin_reads: AtomicU64,
        begin_writes: AtomicU64,
        get_many_calls: AtomicU64,
        get_many_keys: AtomicU64,
        scan_calls: AtomicU64,
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    struct CounterSnapshot {
        begin_reads: u64,
        begin_writes: u64,
        get_many_calls: u64,
        get_many_keys: u64,
        scan_calls: u64,
    }

    impl CountingStorage {
        fn new() -> Self {
            Self {
                inner: Memory::new(),
                counters: Arc::new(Counters::default()),
            }
        }

        fn snapshot(&self) -> CounterSnapshot {
            CounterSnapshot {
                begin_reads: self.counters.begin_reads.load(Ordering::Relaxed),
                begin_writes: self.counters.begin_writes.load(Ordering::Relaxed),
                get_many_calls: self.counters.get_many_calls.load(Ordering::Relaxed),
                get_many_keys: self.counters.get_many_keys.load(Ordering::Relaxed),
                scan_calls: self.counters.scan_calls.load(Ordering::Relaxed),
            }
        }
    }

    impl CounterSnapshot {
        fn delta_since(self, earlier: Self) -> Self {
            Self {
                begin_reads: self.begin_reads - earlier.begin_reads,
                begin_writes: self.begin_writes - earlier.begin_writes,
                get_many_calls: self.get_many_calls - earlier.get_many_calls,
                get_many_keys: self.get_many_keys - earlier.get_many_keys,
                scan_calls: self.scan_calls - earlier.scan_calls,
            }
        }
    }

    impl Storage for CountingStorage {
        type Read<'a>
            = CountingRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            self.counters.begin_reads.fetch_add(1, Ordering::Relaxed);
            Ok(CountingRead {
                inner: self.inner.begin_read(options).await?,
                counters: Arc::clone(&self.counters),
            })
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            self.counters.begin_writes.fetch_add(1, Ordering::Relaxed);
            self.inner.begin_write(options).await
        }
    }

    impl StorageRead for CountingRead {
        async fn get_many(
            &self,
            requests: &[GetManyRequest<'_>],
        ) -> Result<GetManyResult, StorageError> {
            self.counters.get_many_calls.fetch_add(1, Ordering::Relaxed);
            self.counters.get_many_keys.fetch_add(
                requests
                    .iter()
                    .map(|request| request.keys.len() as u64)
                    .sum(),
                Ordering::Relaxed,
            );
            self.inner.get_many(requests).await
        }

        async fn begin_scan(
            &self,
            space: crate::storage::StorageSpace,
            range: KeyRange,
            options: BeginScanOptions,
        ) -> Result<ScanCursor<'_>, StorageError> {
            self.counters.scan_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.begin_scan(space, range, options).await
        }
    }

    #[tokio::test]
    async fn switching_a_stale_branch_publishes_one_bounded_base_refresh() {
        let storage = CountingStorage::new();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("initialize switch benchmark storage");
        let engine = Engine::new(storage.clone())
            .await
            .expect("open switch benchmark engine");
        let session = engine
            .open_session_at(&receipt.main_branch_id)
            .await
            .expect("open pinned main session");
        let branch = session
            .create_branch(CreateBranchOptions {
                id: Some("01990000-0000-7000-8000-00000000c001".to_owned()),
                name: "switch-control-read-test".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("create switch target");

        let before = storage.snapshot();
        let switched = session
            .switch_branch(SwitchBranchOptions {
                branch_id: branch.id.clone(),
            })
            .await
            .expect("switch pinned session");
        let delta = storage.snapshot().delta_since(before);

        assert_eq!(switched.branch_id, branch.id);
        assert_eq!(delta.begin_writes, 1, "stale checkout needs one commit");
        assert!(
            delta.begin_reads <= 4
                && delta.get_many_calls <= 80
                && delta.get_many_keys <= 96
                && delta.scan_calls <= 20,
            "metadata-only auto-rebase must remain bounded, saw {delta:?}"
        );
    }

    /// Fails every `begin_write` while armed; reads pass through.
    #[derive(Clone)]
    struct WriteFailStorage {
        inner: Memory,
        fail_writes: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Storage for WriteFailStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            self.inner.begin_read(options).await
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            if self.fail_writes.load(Ordering::SeqCst) {
                return Err(StorageError::Corruption("injected write failure".into()));
            }
            self.inner.begin_write(options).await
        }
    }

    #[tokio::test]
    async fn failed_boundary_refresh_rolls_back_the_branch_selector() {
        // A stale checkout's boundary refresh publishes one commit. When that
        // write fails, switch_branch returns Err — and must leave the session
        // on its previous branch, not silently on the target.
        let fail_writes = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let storage = WriteFailStorage {
            inner: Memory::new(),
            fail_writes: Arc::clone(&fail_writes),
        };
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("initialize storage");
        let engine = Engine::new(storage).await.expect("open engine");
        let session = engine
            .open_session_at(&receipt.main_branch_id)
            .await
            .expect("open pinned main session");
        let branch = session
            .create_branch(CreateBranchOptions {
                id: Some("01990000-0000-7000-8000-00000000c003".to_owned()),
                name: "refresh-rollback".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("create switch target");

        fail_writes.store(true, Ordering::SeqCst);
        let result = session
            .switch_branch(SwitchBranchOptions {
                branch_id: branch.id.clone(),
            })
            .await;
        fail_writes.store(false, Ordering::SeqCst);

        // This setup is the bounded-refresh test's: the stale checkout needs
        // exactly one commit, so the injected write failure must surface.
        result.expect_err("the boundary refresh write was injected to fail");
        assert_eq!(
            session
                .active_branch_id()
                .await
                .expect("read active branch"),
            receipt.main_branch_id,
            "a failed switch must leave the session on its previous branch"
        );
        // The session stays fully usable and can complete the switch.
        let switched = session
            .switch_branch(SwitchBranchOptions {
                branch_id: branch.id.clone(),
            })
            .await
            .expect("retry succeeds once writes recover");
        assert_eq!(switched.branch_id, branch.id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn switch_branch_completes_against_an_armed_observation() {
        // Regression: switch_branch bumps the observer invalidation
        // generation, so an armed observation re-evaluates under its own
        // waitable-operation guard while the switch waits for observations
        // to settle. A lazy base refresh inside that guarded execute needs
        // session write access, which drains waitable operations — a
        // deadlock on the observation's own guard. The observe loop
        // refreshes before taking its guard instead.
        let storage = Memory::new();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("initialize storage");
        let engine = Engine::new(storage).await.expect("open engine");
        let session = engine
            .open_session_at(&receipt.main_branch_id)
            .await
            .expect("open pinned main session");
        session
            .execute(
                "INSERT INTO lix_file (path, content) \
                 VALUES ('/armed.md', CAST('Hello' AS BYTEA))",
                &[],
            )
            .await
            .expect("seed a file");
        let mut events = session
            .observe("SELECT content FROM lix_file", &[])
            .expect("observation opens");
        events.next().await.expect("initial evaluation");
        let armed = tokio::spawn(async move { events.next().await });

        let branch = session
            .create_branch(CreateBranchOptions {
                id: Some("01990000-0000-7000-8000-00000000c002".to_owned()),
                name: "armed-observation-switch".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("create switch target");
        let switched = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            session.switch_branch(SwitchBranchOptions {
                branch_id: branch.id.clone(),
            }),
        )
        .await
        .expect("switch_branch must not deadlock against the armed observation")
        .expect("switch succeeds");
        assert_eq!(switched.branch_id, branch.id);
        armed.abort();
    }
}
