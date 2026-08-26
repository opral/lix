use std::{future::Future, sync::Arc};

use tokio::sync::watch;

use crate::observe_coordinator::{
    ObserveQueryEvaluation, ObserveQueryKey, ObserveQueryState, ObserveSessionScope,
    ObserveSharedContent,
};
use crate::observe_invalidation::ObserveInvalidationEvent;
use crate::storage_adapter::Memory;
use crate::storage_adapter::Storage;
use crate::{ExecuteResult, LixError, Value, sql2};

use super::SessionContext;

#[derive(Debug, Clone)]
struct ObserveQuery {
    scope: ObserveSessionScope,
    sql: String,
    params: Vec<Value>,
    shared_state: Option<Arc<ObserveQueryState>>,
}

impl ObserveQuery {
    fn new(
        scope: ObserveSessionScope,
        sql: impl Into<String>,
        params: Vec<Value>,
        shared_state: Option<Arc<ObserveQueryState>>,
    ) -> Self {
        Self {
            scope,
            sql: sql.into(),
            params,
            shared_state,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObserveEvent {
    pub sequence: u64,
    pub mutation_sequence: u64,
    pub rows: ExecuteResult,
}

#[expect(missing_debug_implementations)]
pub struct ObserveEvents<StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    session: SessionContext<StorageImpl>,
    query: ObserveQuery,
    receiver: Option<watch::Receiver<ObserveInvalidationEvent>>,
    sequence: u64,
    last_rows: Option<ExecuteResult>,
    last_shared_content: Option<ObserveSharedContent>,
    sync_demand_tx: Option<tokio::sync::mpsc::Sender<crate::sync::SyncDemand>>,
    closed: bool,
}

impl<StorageImpl> ObserveEvents<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) fn with_sync_demand_sender(
        mut self,
        sync_demand_tx: Option<tokio::sync::mpsc::Sender<crate::sync::SyncDemand>>,
    ) -> Self {
        self.sync_demand_tx = sync_demand_tx;
        self
    }

    pub fn next(
        &mut self,
    ) -> impl Future<Output = Result<Option<ObserveEvent>, LixError>> + Send + '_ {
        // SAFETY: ObserveEvents is exclusively borrowed for the whole future;
        // its session/storage handles are Send and its shared references target
        // Sync coordinator, catalog, and immutable query state.
        unsafe { super::AssumeSendFuture::new(self.next_inner()) }
    }

    async fn next_inner(&mut self) -> Result<Option<ObserveEvent>, LixError> {
        if self.closed || self.session.is_closed() {
            self.close();
            return Ok(None);
        }
        if self.last_rows.is_none() {
            let Some((mutation_sequence, evaluation)) =
                Box::pin(self.evaluate_stable_snapshot()).await?
            else {
                return Ok(None);
            };
            let rows = evaluation.rows;
            self.acknowledge_delivered_file_views(&rows);
            self.last_rows = Some(rows.clone());
            self.last_shared_content = evaluation.shared_content;
            return Ok(Some(ObserveEvent {
                sequence: self.sequence,
                mutation_sequence,
                rows,
            }));
        }

        loop {
            if self.closed || self.session.is_closed() {
                self.close();
                return Ok(None);
            }

            if !Box::pin(self.wait_for_invalidation()).await? {
                self.close();
                return Ok(None);
            }

            if self.session.is_closed() {
                self.close();
                return Ok(None);
            }

            let Some((mutation_sequence, evaluation)) =
                Box::pin(self.evaluate_stable_snapshot()).await?
            else {
                return Ok(None);
            };
            let changed =
                evaluation.rows_changed_since(self.last_rows.as_ref(), self.last_shared_content);
            self.last_shared_content = evaluation.shared_content;
            if changed {
                let rows = evaluation.rows;
                self.acknowledge_delivered_file_views(&rows);
                self.sequence += 1;
                self.last_rows = Some(rows.clone());
                return Ok(Some(ObserveEvent {
                    sequence: self.sequence,
                    mutation_sequence,
                    rows,
                }));
            }
        }
    }

    pub fn close(&mut self) {
        self.closed = true;
        self.receiver.take();
    }

    fn acknowledge_delivered_file_views(&self, rows: &ExecuteResult) {
        self.session
            .file_views
            .apply_mutations(rows.file_view_mutations().iter().cloned());
    }

    async fn wait_for_invalidation(&mut self) -> Result<bool, LixError> {
        let Some(receiver) = self.receiver.as_mut() else {
            return Ok(false);
        };
        if receiver.changed().await.is_err() {
            return Ok(false);
        }
        self.invalidation_generation().map(|_| true)
    }

    fn invalidation_generation(&mut self) -> Result<u64, LixError> {
        let event = self
            .receiver
            .as_mut()
            .expect("open observer retains its invalidation receiver")
            .borrow_and_update()
            .clone();
        match event {
            ObserveInvalidationEvent::Generation(generation) => Ok(generation),
            ObserveInvalidationEvent::TerminalError(error) => Err(error),
        }
    }

    async fn evaluate_stable_snapshot(
        &mut self,
    ) -> Result<Option<(u64, ObserveQueryEvaluation)>, LixError> {
        loop {
            let operation_guard = self.session.begin_waitable_session_operation().await?;
            self.session
                .observe_invalidation
                .ensure_external_watcher(self.session.storage.clone())
                .await?;
            let before = self.invalidation_generation()?;
            drop(operation_guard);
            // Keep shared-query leadership across demand hydration, while the
            // query helper releases its own session guard before the sync
            // worker writes the requested history or chunks.
            let rows = Box::pin(self.execute_or_share(before)).await;
            // A follower can wait on another session's shared leader. Recheck
            // this session after that wait so an explicit transaction cannot
            // consume the shared cached result.
            let operation_guard = match self.session.begin_waitable_session_operation().await {
                Err(error) if error.code == LixError::CODE_CLOSED => {
                    self.close();
                    return Ok(None);
                }
                result => result?,
            };
            // Closing transitions the shared session to `Closing` before it
            // waits for active operations. Do not publish a snapshot that
            // completed concurrently with that lifecycle boundary.
            if self.session.is_closed() {
                self.close();
                return Ok(None);
            }
            let rows = match rows {
                Ok(rows) => rows,
                Err(error) if error.code == LixError::CODE_CLOSED => {
                    self.close();
                    return Ok(None);
                }
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        LixError::CODE_STORAGE_READ_EXPIRED
                            | LixError::CODE_TRANSACTION_CONFLICT
                    ) =>
                {
                    // A concurrent commit invalidated this evaluation — an
                    // expired coherent read past its bounded in-execute
                    // retries, or a conflicting base-refresh write. That
                    // commit also bumps the invalidation generation, so the
                    // loop's own response is the right one: yield to the
                    // committing side and re-evaluate on a fresh snapshot.
                    // A long-lived observation must never surface a
                    // retryable transient as a stream error.
                    drop(operation_guard);
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let after = self.invalidation_generation()?;
            if before == after {
                return Ok(Some((after, rows)));
            }
            drop(operation_guard);
        }
    }

    async fn execute_or_share(
        &mut self,
        generation: u64,
    ) -> Result<ObserveQueryEvaluation, LixError> {
        let scope = self.session.observe_scope();
        if self.query.scope != scope {
            let key = ObserveQueryKey::new(scope.clone(), &self.query.sql, &self.query.params)?;
            self.query.scope = scope;
            self.query.shared_state = Some(self.session.observe_coordinator.state_for(&key));
            self.last_shared_content = None;
        }
        let Some(shared_state) = &self.query.shared_state else {
            return Self::execute_with_sync_demands(
                &self.session,
                &self.query.sql,
                &self.query.params,
                self.sync_demand_tx.as_ref(),
            )
            .await
            .map(ObserveQueryEvaluation::unshared);
        };

        shared_state
            .evaluate(generation, Arc::strong_count(shared_state) > 1, || {
                Self::execute_with_sync_demands(
                    &self.session,
                    &self.query.sql,
                    &self.query.params,
                    self.sync_demand_tx.as_ref(),
                )
            })
            .await
    }

    async fn execute_with_sync_demands(
        session: &SessionContext<StorageImpl>,
        sql: &str,
        params: &[Value],
        sync_demand_tx: Option<&tokio::sync::mpsc::Sender<crate::sync::SyncDemand>>,
    ) -> Result<ExecuteResult, LixError> {
        let mut retry = crate::sync::SyncDemandRetry::default();
        loop {
            // Refresh a stale base before entering the operation guard: the
            // refresh may need session write access, which drains waitable
            // operations — taking it while holding this observation's own
            // guard would self-deadlock.
            session.refresh_active_branch_base_if_stale().await?;
            let operation_guard = session.begin_waitable_session_operation().await?;
            let rows = Box::pin(session.execute_for_observe(sql, params)).await;
            drop(operation_guard);
            match rows {
                Err(error) => retry.hydrate_for_retry(sync_demand_tx, error).await?,
                result => return result,
            }
        }
    }
}

impl<StorageImpl> Drop for ObserveEvents<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.close();
    }
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub fn observe(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<ObserveEvents<StorageImpl>, LixError> {
        self.ensure_observe_registration_allowed()?;
        if sql.trim().is_empty() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "observe requires a non-empty SQL string",
            ));
        }
        let statement = self.sql_planning_cache.parse_statement(sql)?;
        if sql2::bind_statement_route(&statement)? == sql2::BoundStatementRoute::Write {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "observe only supports read statements",
            ));
        }
        if sql2::statement_has_durable_runtime_function(&statement) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "observe does not support durable runtime functions",
            ));
        }
        let scope = self.observe_scope();
        let key = ObserveQueryKey::new(scope.clone(), sql, params)?;
        let shared_state = Some(self.observe_coordinator.state_for(&key));

        Ok(ObserveEvents {
            session: self.clone(),
            query: ObserveQuery::new(scope, sql, params.to_vec(), shared_state),
            receiver: Some(self.observe_invalidation.subscribe()),
            sequence: 0,
            last_rows: None,
            last_shared_content: None,
            sync_demand_tx: None,
            closed: false,
        })
    }

    fn observe_scope(&self) -> ObserveSessionScope {
        ObserveSessionScope::Branch(
            self.branch
                .get()
                .expect("session branch selector should be readable"),
        )
    }
}

/// See `session::execute::assume_send_future_proofs`.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::storage::{
        Memory, MemoryRead, MemoryWrite, ReadOptions, StorageError, WriteOptions,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Fails the next `expire_reads` `begin_read` calls with `ReadExpired`,
    /// the way a cross-context store invalidates a coherent read while a
    /// concurrent commit lands.
    #[derive(Clone)]
    struct ExpiringStorage {
        inner: Memory,
        expire_reads: Arc<AtomicU64>,
    }

    impl Storage for ExpiringStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            if self
                .expire_reads
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(StorageError::ReadExpired);
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
    async fn transient_expired_read_reevaluates_instead_of_erroring_the_stream() {
        let expire_reads = Arc::new(AtomicU64::new(0));
        let storage = ExpiringStorage {
            inner: Memory::new(),
            expire_reads: Arc::clone(&expire_reads),
        };
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("initialize storage");
        let engine = Engine::new(storage).await.expect("open engine");
        let session = engine
            .open_session_at(&receipt.main_branch_id)
            .await
            .expect("open pinned main session");
        let mut events = session
            .observe("SELECT value FROM lix_key_value WHERE key = 'observed'", &[])
            .expect("observation opens");
        events.next().await.expect("initial evaluation");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('observed', 'yes')",
                &[],
            )
            .await
            .expect("insert commits");
        // The insert bumped the invalidation generation; make the armed
        // observation lose its next coherent read to a concurrent commit.
        expire_reads.store(1, Ordering::SeqCst);
        let event = events
            .next()
            .await
            .expect("a retryable expired read must re-evaluate, never error the stream")
            .expect("insert event exists");
        assert_eq!(event.rows.rows().len(), 1);
    }
}

#[cfg(test)]
mod assume_send_future_proofs {
    use super::*;

    // session/observe.rs -- ObserveEvents::next
    #[allow(dead_code)]
    fn next_inner_is_send(events: &mut ObserveEvents<Memory>) {
        fn is_send<T: Send>(_: &T) {}
        is_send(&events.next_inner());
    }

    #[allow(dead_code)]
    fn observe_events_is_send_for_every_storage<S>()
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        fn assert_send<T: Send>() {}
        assert_send::<ObserveEvents<S>>();
    }
}
