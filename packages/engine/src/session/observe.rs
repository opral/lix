use std::sync::Arc;

use tokio::sync::watch;

use crate::observe_coordinator::{ObserveQueryKey, ObserveQueryState, ObserveSessionScope};
use crate::storage_adapter::Memory;
use crate::storage_adapter::Storage;
use crate::{ExecuteResult, LixError, Value, sql2};

use super::{SessionContext, SessionMode};

#[derive(Debug, Clone)]
struct ObserveQuery {
    sql: String,
    params: Vec<Value>,
    shared_state: Option<Arc<ObserveQueryState>>,
}

impl ObserveQuery {
    fn new(
        sql: impl Into<String>,
        params: Vec<Value>,
        shared_state: Option<Arc<ObserveQueryState>>,
    ) -> Self {
        Self {
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
    receiver: watch::Receiver<u64>,
    sequence: u64,
    last_rows: Option<ExecuteResult>,
    closed: bool,
}

impl<StorageImpl> ObserveEvents<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub async fn next(&mut self) -> Result<Option<ObserveEvent>, LixError> {
        if self.closed || self.session.is_closed() {
            self.closed = true;
            return Ok(None);
        }
        if self.last_rows.is_none() {
            let Some((mutation_sequence, rows)) = self.evaluate_stable_snapshot().await? else {
                return Ok(None);
            };
            self.last_rows = Some(rows.clone());
            return Ok(Some(ObserveEvent {
                sequence: self.sequence,
                mutation_sequence,
                rows,
            }));
        }

        loop {
            if self.closed || self.session.is_closed() {
                self.closed = true;
                return Ok(None);
            }

            if !self.wait_for_invalidation().await? {
                self.closed = true;
                return Ok(None);
            }

            if self.session.is_closed() {
                self.closed = true;
                return Ok(None);
            }

            let Some((mutation_sequence, rows)) = self.evaluate_stable_snapshot().await? else {
                return Ok(None);
            };
            if self
                .last_rows
                .as_ref()
                .is_none_or(|last_rows| *last_rows != rows)
            {
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
    }

    async fn wait_for_invalidation(&mut self) -> Result<bool, LixError> {
        Ok(self.receiver.changed().await.is_ok())
    }

    async fn evaluate_stable_snapshot(&mut self) -> Result<Option<(u64, ExecuteResult)>, LixError> {
        loop {
            let operation_guard = self.session.begin_waitable_session_operation().await?;
            #[cfg(not(target_family = "wasm"))]
            self.session
                .observe_invalidation
                .ensure_external_watcher(self.session.storage.clone())
                .await?;
            let before = *self.receiver.borrow_and_update();
            let rows = self.execute_or_share(before).await;
            drop(operation_guard);
            let rows = match rows {
                Ok(rows) => rows,
                Err(error) if error.code == LixError::CODE_CLOSED => {
                    self.closed = true;
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            let after = *self.receiver.borrow_and_update();
            if before == after {
                return Ok(Some((after, rows)));
            }
        }
    }

    async fn execute_or_share(&self, generation: u64) -> Result<ExecuteResult, LixError> {
        let Some(shared_state) = &self.query.shared_state else {
            return self
                .session
                .execute(&self.query.sql, &self.query.params)
                .await;
        };

        shared_state
            .evaluate(generation, || async {
                self.session
                    .execute(&self.query.sql, &self.query.params)
                    .await
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
        let statement = sql2::parse_statement(sql)?;
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
        let key = ObserveQueryKey::new(self.observe_scope(), sql, params)?;
        let shared_state = Some(self.observe_coordinator.state_for(&key));

        Ok(ObserveEvents {
            session: self.clone(),
            query: ObserveQuery::new(sql, params.to_vec(), shared_state),
            receiver: self.observe_invalidation.subscribe(),
            sequence: 0,
            last_rows: None,
            closed: false,
        })
    }

    fn observe_scope(&self) -> ObserveSessionScope {
        match &self.mode {
            SessionMode::Workspace => ObserveSessionScope::Workspace,
            SessionMode::Pinned { branch_id } => ObserveSessionScope::Pinned(branch_id.clone()),
        }
    }
}
