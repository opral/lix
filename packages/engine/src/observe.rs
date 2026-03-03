use crate::engine::sql::ast::utils::parse_sql_statements;
use crate::engine::sql::planning::dependency_spec::{
    dependency_spec_to_state_commit_stream_filter, derive_dependency_spec_from_statements,
};
use crate::engine::{Engine, ExecuteOptions};
use crate::state_commit_stream::StateCommitStream;
use crate::{LixError, QueryResult, Value};
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

const OBSERVE_TICK_POLL_INTERVAL: Duration = Duration::from_millis(250);

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
    writer_key_filter: ObserveWriterKeyFilter,
    last_seen_tick_seq: Option<i64>,
    emitted_initial: bool,
    next_sequence: u64,
    closed: bool,
}

#[derive(Default)]
struct ObserveWriterKeyFilter {
    include: BTreeSet<String>,
    exclude: BTreeSet<String>,
}

struct ObserveTickRow {
    tick_seq: i64,
    writer_key: Option<String>,
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
            self.last_seen_tick_seq = latest_observe_tick_seq(engine).await?;
            return Ok(Some(self.make_event(rows, None)));
        }

        loop {
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

            observe_poll_sleep(OBSERVE_TICK_POLL_INTERVAL).await;
            if self.closed {
                return Ok(None);
            }

            let observed_ticks = observe_ticks_since(engine, self.last_seen_tick_seq).await?;
            if observed_ticks.is_empty() {
                continue;
            }

            let mut should_reexecute = false;
            for tick in observed_ticks {
                self.last_seen_tick_seq = Some(tick.tick_seq);
                if self
                    .writer_key_filter
                    .matches_external_tick(tick.writer_key.as_deref())
                {
                    should_reexecute = true;
                }
            }

            if !should_reexecute {
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

impl ObserveWriterKeyFilter {
    fn matches_external_tick(&self, writer_key: Option<&str>) -> bool {
        if !self.include.is_empty() {
            let Some(writer_key) = writer_key else {
                return false;
            };
            if !self.include.contains(writer_key) {
                return false;
            }
        }

        if let Some(writer_key) = writer_key {
            if self.exclude.contains(writer_key) {
                return false;
            }
        }

        true
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
    let writer_key_filter = ObserveWriterKeyFilter {
        include: filter.writer_keys.iter().cloned().collect(),
        exclude: filter.exclude_writer_keys.iter().cloned().collect(),
    };
    let state_commits = engine.state_commit_stream(filter);

    Ok(ObserveState {
        query,
        state_commits,
        last_result: None,
        writer_key_filter,
        last_seen_tick_seq: None,
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

async fn latest_observe_tick_seq(engine: &Engine) -> Result<Option<i64>, LixError> {
    let result = engine
        .execute_backend_sql(
            "SELECT tick_seq \
             FROM lix_internal_observe_tick \
             ORDER BY tick_seq DESC \
             LIMIT 1",
            &[],
        )
        .await?;
    let Some(first_row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(first_value) = first_row.first() else {
        return Ok(None);
    };
    Ok(Some(parse_observe_tick_seq(first_value)?))
}

async fn observe_ticks_since(
    engine: &Engine,
    last_seen_tick_seq: Option<i64>,
) -> Result<Vec<ObserveTickRow>, LixError> {
    let result = if let Some(last_seen) = last_seen_tick_seq {
        engine
            .execute_backend_sql(
                "SELECT tick_seq, writer_key \
                 FROM lix_internal_observe_tick \
                 WHERE tick_seq > $1 \
                 ORDER BY tick_seq ASC",
                &[Value::Integer(last_seen)],
            )
            .await?
    } else {
        engine
            .execute_backend_sql(
                "SELECT tick_seq, writer_key \
                 FROM lix_internal_observe_tick \
                 ORDER BY tick_seq ASC",
                &[],
            )
            .await?
    };

    let mut ticks = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        let tick_seq = parse_observe_tick_seq(row.first().ok_or(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description:
                "failed to read observe tick sequence: row has no tick_seq column".to_string(),
        })?)?;

        let writer_key =
            parse_observe_tick_writer_key(
                row.get(1).ok_or(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description:
                        "failed to read observe tick writer key: row has no writer_key column"
                            .to_string(),
                })?,
            )?;

        ticks.push(ObserveTickRow {
            tick_seq,
            writer_key,
        });
    }
    Ok(ticks)
}

fn parse_observe_tick_seq(value: &Value) -> Result<i64, LixError> {
    match value {
        Value::Integer(value) => Ok(*value),
        Value::Real(value) => Ok(*value as i64),
        Value::Text(value) => value.parse::<i64>().map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to parse observe tick sequence text: {error}"),
        }),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to parse observe tick sequence value: {other:?}"),
        }),
    }
}

fn parse_observe_tick_writer_key(value: &Value) -> Result<Option<String>, LixError> {
    match value {
        Value::Null => Ok(None),
        Value::Text(value) => Ok(Some(value.clone())),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to parse observe tick writer key value: {other:?}"),
        }),
    }
}
