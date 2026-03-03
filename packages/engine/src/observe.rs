use crate::engine::sql::ast::utils::parse_sql_statements;
use crate::engine::sql::planning::dependency_spec::{
    dependency_spec_to_state_commit_stream_filter, derive_dependency_spec_from_statements,
};
use crate::engine::{Engine, ExecuteOptions};
use crate::state_commit_stream::StateCommitStream;
use crate::{LixError, QueryResult, SqlDialect, Value};
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use std::sync::Arc;
use std::time::Duration;

const SQLITE_EXTERNAL_OBSERVE_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObserveQuery {
    pub sql: String,
    pub params: Vec<Value>,
}

impl ObserveQuery {
    pub fn new(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObserveEvent {
    pub sequence: u64,
    pub rows: QueryResult,
    pub state_commit_sequence: Option<u64>,
}

pub struct ObserveEvents<'a> {
    engine: &'a Engine,
    state: ObserveState,
}

pub struct ObserveEventsOwned {
    engine: Arc<Engine>,
    state: ObserveState,
}

struct ObserveState {
    query: ObserveQuery,
    state_commits: StateCommitStream,
    last_result: Option<QueryResult>,
    sqlite_external_polling: bool,
    sqlite_last_data_version: Option<i64>,
    emitted_initial: bool,
    next_sequence: u64,
    closed: bool,
}

impl ObserveEvents<'_> {
    pub async fn next(&mut self) -> Result<Option<ObserveEvent>, LixError> {
        self.state.next_with_engine(self.engine).await
    }

    pub fn close(&mut self) {
        self.state.close();
    }
}

impl Drop for ObserveEvents<'_> {
    fn drop(&mut self) {
        self.close();
    }
}

impl ObserveEventsOwned {
    pub async fn next(&mut self) -> Result<Option<ObserveEvent>, LixError> {
        self.state.next_with_engine(self.engine.as_ref()).await
    }

    pub fn close(&mut self) {
        self.state.close();
    }
}

impl Drop for ObserveEventsOwned {
    fn drop(&mut self) {
        self.close();
    }
}

impl ObserveState {
    async fn next_with_engine(
        &mut self,
        engine: &Engine,
    ) -> Result<Option<ObserveEvent>, LixError> {
        if self.closed {
            return Ok(None);
        }

        if !self.emitted_initial {
            self.emitted_initial = true;
            let rows = execute_observe_query(engine, &self.query).await?;
            self.last_result = Some(rows.clone());
            if self.sqlite_external_polling {
                self.sqlite_last_data_version = Some(sqlite_data_version(engine).await?);
            }
            return Ok(Some(self.make_event(rows, None)));
        }

        loop {
            if self.sqlite_external_polling {
                if let Some(batch) = self.state_commits.try_next() {
                    let rows = execute_observe_query(engine, &self.query).await?;

                    if self
                        .last_result
                        .as_ref()
                        .is_some_and(|previous| *previous == rows)
                    {
                        continue;
                    }

                    self.last_result = Some(rows.clone());
                    return Ok(Some(self.make_event(rows, Some(batch.sequence))));
                }

                observe_poll_sleep(SQLITE_EXTERNAL_OBSERVE_POLL_INTERVAL).await;
                if self.closed {
                    return Ok(None);
                }
                if !self.sqlite_data_version_advanced(engine).await? {
                    continue;
                }

                let rows = execute_observe_query(engine, &self.query).await?;
                if self
                    .last_result
                    .as_ref()
                    .is_some_and(|previous| *previous == rows)
                {
                    continue;
                }

                self.last_result = Some(rows.clone());
                return Ok(Some(self.make_event(rows, None)));
            }

            let Some(batch) = self.state_commits.next().await else {
                self.closed = true;
                return Ok(None);
            };

            let rows = execute_observe_query(engine, &self.query).await?;

            if self
                .last_result
                .as_ref()
                .is_some_and(|previous| *previous == rows)
            {
                continue;
            }

            self.last_result = Some(rows.clone());
            return Ok(Some(self.make_event(rows, Some(batch.sequence))));
        }
    }

    async fn sqlite_data_version_advanced(&mut self, engine: &Engine) -> Result<bool, LixError> {
        let current = sqlite_data_version(engine).await?;
        match self.sqlite_last_data_version {
            Some(previous) if previous == current => Ok(false),
            _ => {
                self.sqlite_last_data_version = Some(current);
                Ok(true)
            }
        }
    }

    fn make_event(
        &mut self,
        rows: QueryResult,
        state_commit_sequence: Option<u64>,
    ) -> ObserveEvent {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        ObserveEvent {
            sequence,
            rows,
            state_commit_sequence,
        }
    }

    fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.state_commits.close();
    }
}

async fn execute_observe_query(
    engine: &Engine,
    query: &ObserveQuery,
) -> Result<QueryResult, LixError> {
    engine
        .execute(&query.sql, &query.params, ExecuteOptions::default())
        .await
}

impl Engine {
    pub fn observe(&self, query: ObserveQuery) -> Result<ObserveEvents<'_>, LixError> {
        let state = build_observe_state(self, query)?;
        Ok(ObserveEvents {
            engine: self,
            state,
        })
    }
}

pub fn observe_owned(
    engine: Arc<Engine>,
    query: ObserveQuery,
) -> Result<ObserveEventsOwned, LixError> {
    let state = build_observe_state(engine.as_ref(), query)?;
    Ok(ObserveEventsOwned { engine, state })
}

fn build_observe_state(engine: &Engine, query: ObserveQuery) -> Result<ObserveState, LixError> {
    let statements = parse_sql_statements(&query.sql)?;
    if statements.is_empty()
        || !statements
            .iter()
            .all(|statement| matches!(statement, Statement::Query(_)))
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "observe requires one or more SELECT statements".to_string(),
        });
    }

    let dependency_spec = derive_dependency_spec_from_statements(&statements, &query.params)?;
    let filter = dependency_spec_to_state_commit_stream_filter(&dependency_spec);
    let state_commits = engine.state_commit_stream(filter);

    Ok(ObserveState {
        query,
        state_commits,
        last_result: None,
        sqlite_external_polling: engine.sql_dialect() == SqlDialect::Sqlite,
        sqlite_last_data_version: None,
        emitted_initial: false,
        next_sequence: 0,
        closed: false,
    })
}

#[cfg(not(target_arch = "wasm32"))]
async fn observe_poll_sleep(duration: Duration) {
    futures_timer::Delay::new(duration).await;
}

#[cfg(target_arch = "wasm32")]
async fn observe_poll_sleep(duration: Duration) {
    let millis = u32::try_from(duration.as_millis()).unwrap_or(u32::MAX);
    gloo_timers::future::TimeoutFuture::new(millis).await;
}

async fn sqlite_data_version(engine: &Engine) -> Result<i64, LixError> {
    let result = engine
        .execute_backend_sql("PRAGMA data_version", &[])
        .await?;
    let value = result
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "failed to read sqlite data_version: pragma returned no rows".to_string(),
        })?;
    match value {
        Value::Integer(value) => Ok(*value),
        Value::Real(value) => Ok(*value as i64),
        Value::Text(value) => value.parse::<i64>().map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to parse sqlite data_version text: {error}"),
        }),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to parse sqlite data_version value: {other:?}"),
        }),
    }
}
