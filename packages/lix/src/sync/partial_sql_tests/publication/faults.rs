//! Acceptance/acknowledgement faults over canonical Memory. This wrapper owns
//! the Engine from construction; it never submits a proof to another engine.
use super::*;
use crate::storage_adapter::{
    MemoryRead, MemoryWrite, PutBatch, Storage, StorageCommitResult, StorageError, StorageKey,
    StorageKeyRange, StorageReadOptions, StorageSessionToken, StorageSpace, StorageWrite,
    StorageWriteOptions,
};
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

#[derive(Clone, Default)]
struct AcceptanceFault {
    mode: Arc<AtomicU8>,
    owner_gate: Arc<crate::storage_adapter::StorageOwnerGate>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
#[derive(Clone, Default)]
struct AcceptanceStorage {
    memory: Memory,
    fault: AcceptanceFault,
}
struct AcceptanceWrite {
    inner: MemoryWrite,
    fault: AcceptanceFault,
}
impl Storage for AcceptanceStorage {
    type Read<'a>
        = MemoryRead
    where
        Self: 'a;
    type Write<'a>
        = AcceptanceWrite
    where
        Self: 'a;
    async fn acquire_session(&self) -> Result<StorageSessionToken, StorageError> {
        self.memory.acquire_session().await
    }
    async fn acquire_partial_replica_owner(
        &self,
        token: StorageSessionToken,
    ) -> Result<crate::storage::StorageOwnerLease, StorageError> {
        self.memory.acquire_partial_replica_owner(token).await
    }
    async fn begin_read(&self, opts: StorageReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.memory.begin_read(opts).await
    }
    async fn begin_write(
        &self,
        opts: StorageWriteOptions,
    ) -> Result<Self::Write<'_>, StorageError> {
        Ok(AcceptanceWrite {
            inner: self.memory.begin_write(opts).await?,
            fault: self.fault.clone(),
        })
    }
}
impl StorageWrite for AcceptanceWrite {
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
    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
    async fn commit(self) -> Result<StorageCommitResult, StorageError> {
        let result = self.inner.commit().await?;
        let mode = self.fault.mode.swap(0, Ordering::SeqCst);
        if mode != 0 {
            self.fault.entered.notify_one();
            self.fault.release.notified().await;
            if mode == 2 {
                return Err(StorageError::CommitOutcomeUnknown(
                    "injected after Memory accepted publication".into(),
                ));
            }
        }
        Ok(result)
    }
}

async fn prepared_fault_fixture() -> (
    Arc<Engine<AcceptanceStorage>>,
    SessionContext<AcceptanceStorage>,
    Arc<PartialReplicaState>,
    Arc<PartialReplicaState>,
    PreparedPartialPublication,
    AcceptanceFault,
) {
    prepared_fault_fixture_with_deadline(None).await
}
async fn prepared_fault_fixture_with_deadline(
    short: Option<Duration>,
) -> (
    Arc<Engine<AcceptanceStorage>>,
    SessionContext<AcceptanceStorage>,
    Arc<PartialReplicaState>,
    Arc<PartialReplicaState>,
    PreparedPartialPublication,
    AcceptanceFault,
) {
    let authority = open_lix().await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('resident','before')",
            &[],
        )
        .await
        .unwrap();
    let old = Arc::new(
        PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            authority.active_account_id().into(),
            uuid::Uuid::now_v7().to_string(),
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap(),
    );
    let backend = AcceptanceStorage::default();
    let fault = backend.fault.clone();
    let storage = StorageAdapter::new(backend);
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let preconditions = stage_partial_bootstrap(&read, &mut writes, &old).unwrap();
    crate::init::stage_partial_repository_protocol(&mut writes);
    drop(read);
    storage
        .commit_write_set(
            writes,
            StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (mut engine, session) =
        Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &old)
            .await
            .unwrap();
    engine.install_partial_owner(crate::engine::PartialOwnerLifetime::install(
        fault.owner_gate.try_acquire().unwrap(),
    ));
    let engine = Arc::new(engine);
    engine
        .sync_mode()
        .admit_partial_replica(old.clone(), crate::sync::partial_replica_write_capability());
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "SELECT value FROM lix_key_value WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    authority
        .execute(
            "UPDATE lix_key_value SET value='remote' WHERE key='resident'",
            &[],
        )
        .await
        .unwrap();
    let next = Arc::new(
        old.with_descriptor_and_fresh_generations(
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap(),
    );
    let prepared = prepare_hydrating(&engine, &old, next.clone(), &authority).await;
    let prepared = if let Some(duration) = short {
        drop(prepared);
        let deadline = crate::sync::http::CandidateBaselineDeadline::for_test(
            &next.baseline_lease().lease_id,
            duration,
        );
        prepare_hydrating_with_deadline(&engine, &old, next.clone(), &authority, deadline).await
    } else {
        prepared
    };
    (engine, session, old, next, prepared, fault)
}

#[tokio::test]
async fn cancelled_publication_caller_retains_gates_until_acknowledgement() {
    tokio::time::timeout(Duration::from_secs(10),async {
        let (engine,session,old,next,prepared,fault)=prepared_fault_fixture().await;
        fault.mode.store(1,Ordering::SeqCst);
        let mut caller=Box::pin(publish_prepared_partial(engine.clone(),prepared));
        tokio::select! {
            _=fault.entered.notified()=>{},
            result=&mut caller=>panic!("publication acknowledged before injected boundary: {result:?}"),
        }
        drop(caller);
        engine.partial_owner().close();
        assert!(matches!(fault.owner_gate.try_acquire(), Err(StorageError::InUse)),
            "cancelled caller and closed owner must not release accepted publication");
        assert_eq!(engine.sync_mode().partial_admission().as_deref(),Some(old.as_ref()));
        let storage=engine.storage();
        let read=storage.begin_read(Default::default()).await.unwrap();
        assert_eq!(crate::sync::partial_state::load_partial_replica_state(&read).await.unwrap().unwrap().0,*next);
        drop(read);
        let mut query=Box::pin(session.execute("SELECT value FROM lix_key_value WHERE key='resident'",&[]));
        assert!(tokio::time::timeout(Duration::from_millis(30),&mut query).await.is_err(),"direct SQL escaped publication gate before acknowledgement");
        fault.release.notify_one();
        assert!(value(query.await.unwrap()).contains("remote"));
        // Gate acquisition by SQL establishes that the owned publisher finished
        // its ACK and state swap. Its final guard drop may be scheduled next.
        let _new_owner = loop {
            match fault.owner_gate.try_acquire() {
                Ok(owner) => break owner,
                Err(StorageError::InUse) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected owner acquisition error: {error}"),
            }
        };
        assert_eq!(engine.sync_mode().partial_admission().as_deref(),Some(next.as_ref()));
        let (reopened,fresh)=Engine::new_partial_replica(engine.storage(),EngineOptions::new(),&next).await.unwrap();
        reopened.sync_mode().admit_partial_replica(next,crate::sync::partial_replica_write_capability());
        assert!(value(fresh.execute("SELECT value FROM lix_key_value WHERE key='resident'",&[]).await.unwrap()).contains("remote"));
    }).await.expect("publication cancellation test timed out");
}

#[tokio::test]
async fn accepted_unknown_publication_poison_blocks_sql_until_durable_reopen() {
    tokio::time::timeout(Duration::from_secs(10),async {
        let (engine,session,_,next,prepared,fault)=prepared_fault_fixture().await;
        fault.mode.store(2,Ordering::SeqCst);
        let mut caller=Box::pin(publish_prepared_partial(engine.clone(),prepared));
        tokio::select! {
            _=fault.entered.notified()=>{},
            result=&mut caller=>panic!("publication acknowledged before injected boundary: {result:?}"),
        }
        fault.release.notify_one();
        let error=caller.await.unwrap_err();
        assert_eq!(error.code,LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN);
        for sql in ["SELECT value FROM lix_key_value WHERE key='resident'","UPDATE lix_key_value SET value='unsafe' WHERE key='resident'"] {
            assert_eq!(session.execute(sql,&[]).await.unwrap_err().code,LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN);
        }
        let storage=engine.storage();
        let read=storage.begin_read(Default::default()).await.unwrap();
        let durable=crate::sync::partial_state::load_partial_replica_state(&read).await.unwrap().unwrap().0;
        assert_eq!(durable,*next);
        drop(read);
        let (reopened,fresh)=Engine::new_partial_replica(storage,EngineOptions::new(),&durable).await.unwrap();
        reopened.sync_mode().admit_partial_replica(Arc::new(durable),crate::sync::partial_replica_write_capability());
        assert!(value(fresh.execute("SELECT value FROM lix_key_value WHERE key='resident'",&[]).await.unwrap()).contains("remote"));
    }).await.expect("unknown publication test timed out");
}

#[tokio::test]
async fn expired_candidate_waiting_for_write_gate_never_publishes_even_if_caller_cancelled() {
    for cancel in [false, true] {
        let (engine, _session, old, _next, prepared, _fault) =
            prepared_fault_fixture_with_deadline(Some(Duration::from_secs(2))).await;
        let held = engine.collaboration_write_gate().lock_owned().await;
        let mut caller = Box::pin(publish_prepared_partial(engine.clone(), prepared));
        // Poll the caller so it spawns its owned task, then establish that the
        // publisher has acquired exclusive interests and is waiting on held.
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut caller)
                .await
                .is_err()
        );
        let registry = engine.sync_mode().read_interests().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), registry.begin_operation())
                .await
                .is_err()
        );
        if cancel {
            drop(caller);
            tokio::time::sleep(Duration::from_millis(2100)).await;
        } else {
            let error = tokio::time::timeout(Duration::from_secs(3), caller)
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, "LIX_PARTIAL_CANDIDATE_EXPIRED");
        }
        // Still hold the write gate: expiration must cancel the owned wait and
        // release the exclusive interest gate independently of caller lifetime.
        let operation = tokio::time::timeout(Duration::from_secs(1), registry.begin_operation())
            .await
            .unwrap();
        drop(operation);
        assert_eq!(
            engine.sync_mode().partial_admission().as_deref(),
            Some(old.as_ref())
        );
        let storage = engine.storage();
        let read = storage.begin_read(Default::default()).await.unwrap();
        assert_eq!(
            crate::sync::partial_state::load_partial_replica_state(&read)
                .await
                .unwrap()
                .unwrap()
                .0,
            *old
        );
        drop(read);
        drop(held);
        engine
            .sync_mode()
            .ensure_partial_admission_healthy()
            .unwrap();
    }
}

#[tokio::test]
async fn candidate_expiring_after_storage_acceptance_poison_blocks_live_sql() {
    let (engine, session, old, next, prepared, fault) =
        prepared_fault_fixture_with_deadline(Some(Duration::from_secs(2))).await;
    fault.mode.store(1, Ordering::SeqCst);
    let mut caller = Box::pin(publish_prepared_partial(engine.clone(), prepared));
    tokio::select! {
        _ = fault.entered.notified() => {},
        result = &mut caller => panic!("publication did not enter acceptance boundary: {result:?}"),
    }
    tokio::time::sleep(Duration::from_millis(2100)).await;
    fault.release.notify_one();
    let error = caller.await.unwrap_err();
    assert_eq!(error.code, "LIX_PARTIAL_CANDIDATE_EXPIRED");
    assert_eq!(
        engine.sync_mode().partial_admission().as_deref(),
        Some(old.as_ref())
    );
    for sql in [
        "SELECT value FROM lix_key_value WHERE key='resident'",
        "UPDATE lix_key_value SET value='unsafe' WHERE key='resident'",
    ] {
        assert_eq!(
            session.execute(sql, &[]).await.unwrap_err().code,
            "LIX_PARTIAL_CANDIDATE_EXPIRED"
        );
    }
    let storage = engine.storage();
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        crate::sync::partial_state::load_partial_replica_state(&read)
            .await
            .unwrap()
            .unwrap()
            .0,
        *next,
        "known accepted state remains the recovery source; it must not be rolled back"
    );
}
