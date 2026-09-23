use super::context::SessionContext;
use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchRefReader, BranchReferenceRole};
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
        // Serialize switches across session clones while the target is
        // prepared and the shared selector is published.
        let _switch_serial = self.branch.begin_switch().await;
        self.ensure_open()?;
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
        drop(reader);
        drop(read);
        self.ensure_open()?;

        loop {
            // Refresh a private candidate bound to the target. A failure leaves
            // the branch shared by session clones untouched, so ordinary writes
            // can continue on the old branch and cannot commit to a target that
            // this switch later abandons.
            let candidate = self.branch_switch_candidate(branch_id.clone())?;
            candidate.refresh_active_branch_base_if_stale().await?;
            let prepared_global_head = candidate
                .observed_global_head
                .read()
                .map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "session global-head observation is poisoned",
                    )
                })?
                .clone();

            // Take write access only at publication. This drains operations
            // still using the old selector, then the final validation fences
            // branch deletion before the new selector becomes visible.
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

            // A global commit can race the gap between target refresh and
            // selector publication. Recheck the session's freshness watermark
            // under write access and prepare again if global advanced. The
            // target's pinned base can intentionally predate the observed head;
            // comparing the two directly would create an unnecessary commit
            // every time the session switches back to that branch.
            let target_is_current = if branch_id == crate::GLOBAL_BRANCH_ID
                || self.sync_mode.role() == crate::sync::SyncRole::PartialReplica
            {
                true
            } else {
                let global_head = reader.load_head_commit_id(crate::GLOBAL_BRANCH_ID).await?;
                match (prepared_global_head.as_ref(), global_head.as_ref()) {
                    (Some(prepared), Some(current)) => prepared == current,
                    // A missing observation means the candidate had no global
                    // head to refresh against. The refresh path treats an
                    // absent target/global head as a no-op.
                    _ => true,
                }
            };

            if !target_is_current {
                drop(reader);
                drop(read);
                drop(write_access);
                continue;
            }

            let mut observed_global_head = self.observed_global_head.write().map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "session global-head observation is poisoned",
                )
            })?;
            self.branch.set(branch_id.clone())?;
            *observed_global_head = prepared_global_head;
            self.observe_invalidation.bump();
            drop(observed_global_head);
            drop(reader);
            drop(read);
            drop(write_access);
            break;
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

        async fn acquire_session(
            &self,
        ) -> Result<crate::storage::StorageSessionToken, StorageError> {
            self.inner.acquire_session().await
        }

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
    async fn branch_creation_shares_immutable_rows_and_refresh_keeps_its_generation() {
        use crate::branch::BranchHeadControlContext;
        use crate::hot_state::{ROOT_CURRENT_BASE_SPACE, ROW_SPACE, hot_generation_scope_prefix};
        use crate::storage_adapter::{
            StorageAdapterRead as _, StorageBeginScanOptions, StoragePrefix,
        };

        for rows in [8, 1024] {
            let storage = CountingStorage::new();
            let initialized = Engine::initialize(storage.clone())
                .await
                .expect("initialize");
            let engine = Engine::new(storage.clone()).await.expect("engine");
            let session = engine
                .open_session_at(&initialized.main_branch_id)
                .await
                .expect("session");
            let values = (0..rows)
                .map(|i| format!("('row-{i}', 'value-{i}')"))
                .collect::<Vec<_>>()
                .join(",");
            session
                .execute(
                    &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
                    &[],
                )
                .await
                .expect("seed");
            session
                .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
                .await
                .expect("checkpoint");
            let branch = session
                .create_branch(CreateBranchOptions {
                    id: None,
                    name: "shared-root".to_owned(),
                    from_commit_id: None,
                })
                .await
                .expect("create");
            let read = engine
                .storage()
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read");
            let control = BranchHeadControlContext::new()
                .reader(&read)
                .load(&branch.id)
                .await
                .expect("control")
                .expect("branch");
            let range = StoragePrefix {
                bytes: hot_generation_scope_prefix(&branch.id, control.tracked_generation).into(),
            }
            .to_range()
            .expect("scope");
            let roots = read
                .begin_scan(
                    ROOT_CURRENT_BASE_SPACE,
                    range.clone(),
                    StorageBeginScanOptions::default(),
                )
                .await
                .expect("root scan")
                .collect_all()
                .await
                .expect("roots");
            assert_eq!(
                roots.len(),
                1,
                "new branch must share its immutable root ({rows} rows)"
            );
            let hot = read
                .begin_scan(ROW_SPACE, range, StorageBeginScanOptions::default())
                .await
                .expect("hot scan")
                .collect_all()
                .await
                .expect("hot rows");
            assert!(
                hot.len() < 64,
                "only the serving catalog may be copied, not {rows} owned rows: {}",
                hot.len()
            );
            drop(read);
            session
                .switch_branch(SwitchBranchOptions {
                    branch_id: branch.id.clone(),
                })
                .await
                .expect("stale checkout");
            let read = engine
                .storage()
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read");
            let refreshed = BranchHeadControlContext::new()
                .reader(&read)
                .load(&branch.id)
                .await
                .expect("control")
                .expect("branch");
            assert_ne!(
                control.head_commit_id, refreshed.head_commit_id,
                "stale checkout publishes a base refresh"
            );
            assert_eq!(
                control.tracked_generation, refreshed.tracked_generation,
                "base refresh must retain the local serving generation"
            );
            assert_eq!(
                control.working_diff_checkpoint_commit_id,
                refreshed.working_diff_checkpoint_commit_id
            );
            drop(read);
            let count = session
                .execute(
                    "SELECT COUNT(*) AS n FROM lix_key_value WHERE key LIKE 'row-%'",
                    &[],
                )
                .await
                .expect("shared rows");
            assert_eq!(count.rows()[0].get::<i64>("n").expect("count"), rows);
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
            // In-place publication authenticates catalog/root ownership and
            // the private epoch. Its catalog revision also warms the new
            // catalog in one fresh read. These are fixed metadata costs,
            // independent of the number of branch-owned application rows.
            delta.begin_reads <= 5
                && delta.get_many_calls <= 144
                && delta.get_many_keys <= 160
                && delta.scan_calls <= 20,
            "metadata-only auto-rebase must remain bounded, saw {delta:?}"
        );
    }

    #[tokio::test]
    async fn switching_to_a_direct_global_commit_does_not_retry_forever() {
        let storage = Memory::new();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("initialize storage");
        let engine = Engine::new(storage).await.expect("open engine");
        let global = engine
            .open_session_at(crate::GLOBAL_BRANCH_ID.to_owned())
            .await
            .expect("open global branch");
        let global_head = global
            .execute("SELECT lix_active_branch_commit_id()", &[])
            .await
            .expect("read global head");
        let crate::Value::Text(global_head) = &global_head.rows()[0].values()[0] else {
            panic!("global head should be text");
        };

        let session = engine
            .open_session_at(&receipt.main_branch_id)
            .await
            .expect("open main session");
        let branch = session
            .create_branch(CreateBranchOptions {
                id: Some("01990000-0000-7000-8000-00000000c005".to_owned()),
                name: "direct-global-head".to_owned(),
                from_commit_id: Some(global_head.clone()),
            })
            .await
            .expect("create branch at global commit");

        let switched = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            session.switch_branch(SwitchBranchOptions {
                branch_id: branch.id.clone(),
            }),
        )
        .await
        .expect("direct global-root switch should terminate")
        .expect("switch succeeds");
        assert_eq!(switched.branch_id, branch.id);
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

        async fn acquire_session(
            &self,
        ) -> Result<crate::storage::StorageSessionToken, StorageError> {
            self.inner.acquire_session().await
        }

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

    #[derive(Clone)]
    struct RefreshReadFailStorage {
        inner: Memory,
        gate: Arc<RefreshReadGate>,
    }

    #[derive(Default)]
    struct RefreshReadGate {
        armed: std::sync::atomic::AtomicBool,
        reads_after_arm: AtomicU64,
        blocked: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl RefreshReadGate {
        fn arm(&self) {
            self.reads_after_arm.store(0, Ordering::SeqCst);
            self.armed.store(true, Ordering::SeqCst);
        }

        async fn wait_until_blocked(&self) {
            self.blocked.notified().await;
        }

        fn release_with_failure(&self) {
            self.release.notify_one();
        }
    }

    impl Storage for RefreshReadFailStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn acquire_session(
            &self,
        ) -> Result<crate::storage::StorageSessionToken, StorageError> {
            self.inner.acquire_session().await
        }

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            if self.gate.armed.load(Ordering::SeqCst)
                && self.gate.reads_after_arm.fetch_add(1, Ordering::SeqCst) == 1
            {
                self.gate.armed.store(false, Ordering::SeqCst);
                self.gate.blocked.notify_one();
                self.gate.release.notified().await;
                return Err(StorageError::Corruption(
                    "injected branch refresh read failure".into(),
                ));
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
    async fn cloned_write_during_failed_switch_stays_on_published_branch() {
        let storage = RefreshReadFailStorage {
            inner: Memory::new(),
            gate: Arc::new(RefreshReadGate::default()),
        };
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("initialize storage");
        let engine = Engine::new(storage.clone()).await.expect("open engine");
        let session = engine
            .open_session_at(&receipt.main_branch_id)
            .await
            .expect("open pinned main session");
        let branch = session
            .create_branch(CreateBranchOptions {
                id: Some("01990000-0000-7000-8000-00000000c004".to_owned()),
                name: "refresh-write-race".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("create switch target");

        storage.gate.arm();
        let switch_session = session.clone();
        let switch_branch_id = branch.id.clone();
        let switch = tokio::spawn(async move {
            switch_session
                .switch_branch(SwitchBranchOptions {
                    branch_id: switch_branch_id,
                })
                .await
        });
        storage.gate.wait_until_blocked().await;

        // The refresh is paused before it can fail. A cloned handle remains
        // bound to the currently published branch and can commit normally.
        let write_session = session.clone();
        let write = tokio::spawn(async move {
            write_session
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ('switch-race', 'kept')",
                    &[],
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            write
                .await
                .expect("cloned write task")
                .expect("cloned write");
        })
        .await
        .expect("write can finish against the still-published branch");

        storage.gate.release_with_failure();
        let switch_result = tokio::time::timeout(std::time::Duration::from_secs(30), switch)
            .await
            .expect("switch finishes after refresh fails")
            .expect("switch task");
        switch_result.expect_err("refresh read was injected to fail");

        assert_eq!(
            session.active_branch_id().await.expect("active branch"),
            receipt.main_branch_id
        );
        let main_count = session
            .execute(
                "SELECT COUNT(*) AS n FROM lix_key_value WHERE key = 'switch-race'",
                &[],
            )
            .await
            .expect("read write from main")
            .rows()[0]
            .get::<i64>("n")
            .expect("main count");
        assert_eq!(main_count, 1);

        let target_session = engine
            .open_session_at(&branch.id)
            .await
            .expect("open target session");
        let target_count = target_session
            .execute(
                "SELECT COUNT(*) AS n FROM lix_key_value WHERE key = 'switch-race'",
                &[],
            )
            .await
            .expect("read target")
            .rows()[0]
            .get::<i64>("n")
            .expect("target count");
        assert_eq!(
            target_count, 0,
            "failed switch must not leak writes to target"
        );
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
