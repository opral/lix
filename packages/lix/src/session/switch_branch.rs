use serde_json::json;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchRefStoreReader, BranchReferenceRole};
use crate::storage_adapter::{SharedStorageAdapterRead, Storage, StorageReadOptions};
use crate::transaction::types::{RawWriteBatch, TransactionJson, TransactionWriteRow};

use super::context::{SessionContext, SessionMode, WORKSPACE_BRANCH_KEY};

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";

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
    /// Switches the session's active branch selector.
    ///
    /// Pinned sessions update their in-memory selector. Workspace sessions
    /// additionally persist the workspace selector. Clones of this session
    /// observe the switch in place; independently opened sessions retain the
    /// branch snapshot they opened with.
    pub async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<SwitchBranchReceipt, LixError> {
        let branch_id = options.branch_id;
        let receipt_branch_id = branch_id.clone();
        let current_mode = self.mode.clone();
        let selector = match &self.mode {
            SessionMode::Pinned { branch_id } | SessionMode::Workspace { branch_id } => {
                branch_id.clone()
            }
        };
        let observe_invalidation = self.observe_invalidation.clone();
        match current_mode {
            SessionMode::Pinned { .. } => {
                let _operation_guard = self.begin_waitable_session_operation().await?;
                let read = SharedStorageAdapterRead::new(
                    self.storage
                        .begin_read(StorageReadOptions::default())
                        .await?,
                );
                let reader = BranchRefStoreReader::new(read);
                BranchLifecycle::new(&reader)
                    .require_existing_commit_id(
                        &branch_id,
                        BranchOperation::SwitchBranch,
                        BranchReferenceRole::Target,
                    )
                    .await?;
                self.ensure_open()?;
                *selector.write().map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "session branch selector is poisoned",
                    )
                })? = receipt_branch_id.clone();
                observe_invalidation.bump();
            }
            SessionMode::Workspace { .. } => {
                let write_access = self.begin_session_write_access().await?;
                self.with_write_transaction_reserved_lending(
                    write_access,
                    async move |transaction| {
                        {
                            let reader = transaction.branch_ref_reader_on_opening_read();
                            BranchLifecycle::new(&reader)
                                .require_existing_commit_id(
                                    &branch_id,
                                    BranchOperation::SwitchBranch,
                                    BranchReferenceRole::Target,
                                )
                                .await?
                        };
                        let mut rows = RawWriteBatch::with_capacity(1);
                        rows.push(workspace_branch_stage_row(&branch_id)?);
                        transaction.stage_rows(rows).await?;
                        Ok(())
                    },
                    |()| {
                        *selector.write().map_err(|_| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "session branch selector is poisoned",
                            )
                        })? = receipt_branch_id.clone();
                        observe_invalidation.bump();
                        Ok(())
                    },
                )
                .await?;
            }
        }

        Ok(SwitchBranchReceipt {
            branch_id: receipt_branch_id,
        })
    }
}

#[expect(clippy::unnecessary_wraps)]
fn workspace_branch_stage_row(branch_id: &str) -> Result<TransactionWriteRow, LixError> {
    Ok(TransactionWriteRow {
        row_pk: Some(crate::row_pk::RowPk::single(WORKSPACE_BRANCH_KEY)),
        schema_key: KEY_VALUE_SCHEMA_KEY.into(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "key": WORKSPACE_BRANCH_KEY,
            "value": branch_id,
        }))),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: true,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: GLOBAL_BRANCH_ID.into(),
    })
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
    async fn pinned_switch_reads_only_the_authoritative_target_control() {
        let storage = CountingStorage::new();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("initialize switch benchmark storage");
        let engine = Engine::new(storage.clone())
            .await
            .expect("open switch benchmark engine");
        let session = engine
            .open_session(&receipt.main_branch_id)
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
        assert_eq!(delta.begin_reads, 1);
        assert_eq!(delta.begin_writes, 0);
        assert_eq!(delta.scan_calls, 0);
        assert_eq!(
            (delta.get_many_calls, delta.get_many_keys),
            (7, 13),
            "pinned switching must authenticate the target and its catalog owner through one retained read; the native loader may deduplicate authenticated closure keys"
        );
    }
}
