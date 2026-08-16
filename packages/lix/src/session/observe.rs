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
    sync_scopes: Vec<String>,
    shared_state: Option<Arc<ObserveQueryState>>,
}

impl ObserveQuery {
    fn new(
        scope: ObserveSessionScope,
        sql: impl Into<String>,
        params: Vec<Value>,
        sync_scopes: Vec<String>,
        shared_state: Option<Arc<ObserveQueryState>>,
    ) -> Self {
        Self {
            scope,
            sql: sql.into(),
            params,
            sync_scopes,
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
    closed: bool,
}

impl<StorageImpl> ObserveEvents<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
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
            self.session
                .wait_for_sync_scope_hydration(&self.query.sync_scopes)
                .await?;
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
            #[cfg(not(target_family = "wasm"))]
            ObserveInvalidationEvent::TerminalStorageError(error) => Err(error),
        }
    }

    async fn evaluate_stable_snapshot(
        &mut self,
    ) -> Result<Option<(u64, ObserveQueryEvaluation)>, LixError> {
        loop {
            let operation_guard = self.session.begin_waitable_session_operation().await?;
            #[cfg(not(target_family = "wasm"))]
            self.session
                .observe_invalidation
                .ensure_external_watcher(self.session.storage.clone())
                .await?;
            let before = self.invalidation_generation()?;
            let rows = Box::pin(self.execute_or_share(before)).await;
            drop(operation_guard);
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
                Err(error) => return Err(error),
            };
            let after = self.invalidation_generation()?;
            if before == after {
                return Ok(Some((after, rows)));
            }
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
            return Box::pin(
                self.session
                    .execute_for_observe(&self.query.sql, &self.query.params),
            )
            .await
            .map(ObserveQueryEvaluation::unshared);
        };

        shared_state
            .evaluate(generation, Arc::strong_count(shared_state) > 1, || {
                Box::pin(async {
                    Box::pin(
                        self.session
                            .execute_for_observe(&self.query.sql, &self.query.params),
                    )
                    .await
                })
            })
            .await
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
        let sync_scopes = self.register_sync_sql_scope(sql);
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
            query: ObserveQuery::new(scope, sql, params.to_vec(), sync_scopes, shared_state),
            receiver: Some(self.observe_invalidation.subscribe()),
            sequence: 0,
            last_rows: None,
            last_shared_content: None,
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
