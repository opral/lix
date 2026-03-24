use crate::errors::classification::is_missing_relation_error;
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, Value};

pub(crate) const LIVE_STATE_SCHEMA_EPOCH: &str = "1";
pub(crate) const LIVE_STATE_STATUS_TABLE: &str = "lix_internal_live_state_status";
const LIVE_STATE_STATUS_SINGLETON_ID: i64 = 1;

pub(crate) const LIVE_STATE_STATUS_CREATE_TABLE_SQL: &str =
    "CREATE TABLE lix_internal_live_state_status (\
     singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),\
     mode TEXT NOT NULL,\
     latest_change_id TEXT,\
     latest_change_created_at TEXT,\
     schema_epoch TEXT NOT NULL,\
     updated_at TEXT NOT NULL\
     )";

pub(crate) const LIVE_STATE_STATUS_SEED_ROW_SQL: &str =
    "INSERT INTO lix_internal_live_state_status (\
     singleton_id, mode, latest_change_id, latest_change_created_at, schema_epoch, updated_at\
     ) \
     SELECT 1, 'uninitialized', NULL, NULL, '1', '1970-01-01T00:00:00Z' \
     WHERE NOT EXISTS (\
       SELECT 1 FROM lix_internal_live_state_status WHERE singleton_id = 1\
     )";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveStateMode {
    Uninitialized,
    Bootstrapping,
    Ready,
    NeedsRebuild,
    Rebuilding,
}

impl LiveStateMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Uninitialized => "uninitialized",
            Self::Bootstrapping => "bootstrapping",
            Self::Ready => "ready",
            Self::NeedsRebuild => "needs_rebuild",
            Self::Rebuilding => "rebuilding",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "uninitialized" => Some(Self::Uninitialized),
            "bootstrapping" => Some(Self::Bootstrapping),
            "ready" => Some(Self::Ready),
            "needs_rebuild" => Some(Self::NeedsRebuild),
            "rebuilding" => Some(Self::Rebuilding),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalWatermark {
    pub(crate) change_id: String,
    pub(crate) created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveStateStatusRow {
    mode: LiveStateMode,
    schema_epoch: String,
    watermark: Option<CanonicalWatermark>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveStateSnapshot {
    status: Option<LiveStateStatusRow>,
    pub(crate) latest_canonical_watermark: Option<CanonicalWatermark>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveStateReadiness {
    Uninitialized,
    Ready,
    NeedsRebuild,
}

pub(crate) fn evaluate_live_state_transaction_eligibility(
    snapshot: &LiveStateSnapshot,
) -> LiveStateReadiness {
    let Some(status) = snapshot.status.as_ref() else {
        return if snapshot.latest_canonical_watermark.is_some() {
            LiveStateReadiness::NeedsRebuild
        } else {
            LiveStateReadiness::Uninitialized
        };
    };

    match status.mode {
        LiveStateMode::Uninitialized => {
            if snapshot.latest_canonical_watermark.is_some() {
                LiveStateReadiness::NeedsRebuild
            } else {
                LiveStateReadiness::Uninitialized
            }
        }
        LiveStateMode::Bootstrapping | LiveStateMode::Rebuilding | LiveStateMode::NeedsRebuild => {
            LiveStateReadiness::NeedsRebuild
        }
        // Inside an open write transaction the canonical change head may advance
        // before the transaction stamps the live-state watermark at commit time.
        // Transaction eligibility therefore validates owner state and schema epoch,
        // not watermark equality.
        LiveStateMode::Ready => {
            if status.schema_epoch == LIVE_STATE_SCHEMA_EPOCH {
                LiveStateReadiness::Ready
            } else {
                LiveStateReadiness::NeedsRebuild
            }
        }
    }
}

pub(crate) fn evaluate_live_state_snapshot(snapshot: &LiveStateSnapshot) -> LiveStateReadiness {
    let Some(status) = snapshot.status.as_ref() else {
        return if snapshot.latest_canonical_watermark.is_some() {
            LiveStateReadiness::NeedsRebuild
        } else {
            LiveStateReadiness::Uninitialized
        };
    };

    match status.mode {
        LiveStateMode::Uninitialized => {
            if snapshot.latest_canonical_watermark.is_some() {
                LiveStateReadiness::NeedsRebuild
            } else {
                LiveStateReadiness::Uninitialized
            }
        }
        LiveStateMode::Bootstrapping | LiveStateMode::Rebuilding | LiveStateMode::NeedsRebuild => {
            LiveStateReadiness::NeedsRebuild
        }
        LiveStateMode::Ready => {
            let ready = status.schema_epoch == LIVE_STATE_SCHEMA_EPOCH
                && status.watermark == snapshot.latest_canonical_watermark;
            if ready {
                LiveStateReadiness::Ready
            } else {
                LiveStateReadiness::NeedsRebuild
            }
        }
    }
}

pub(crate) async fn load_live_state_snapshot(
    backend: &dyn LixBackend,
) -> Result<LiveStateSnapshot, LixError> {
    load_live_state_snapshot_with_backend(backend).await
}

pub(crate) async fn ensure_live_state_ready(backend: &dyn LixBackend) -> Result<(), LixError> {
    let snapshot = load_live_state_snapshot(backend).await?;
    match evaluate_live_state_snapshot(&snapshot) {
        LiveStateReadiness::Ready => Ok(()),
        LiveStateReadiness::Uninitialized => Err(crate::errors::not_initialized_error()),
        LiveStateReadiness::NeedsRebuild => Err(crate::errors::live_state_not_ready_error()),
    }
}

pub(crate) async fn ensure_live_state_ready_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<(), LixError> {
    let snapshot = load_live_state_snapshot_in_transaction(transaction).await?;
    match evaluate_live_state_transaction_eligibility(&snapshot) {
        LiveStateReadiness::Ready => Ok(()),
        LiveStateReadiness::Uninitialized => Err(crate::errors::not_initialized_error()),
        LiveStateReadiness::NeedsRebuild => Err(crate::errors::live_state_not_ready_error()),
    }
}

pub(crate) async fn mark_live_state_mode_with_backend(
    backend: &dyn LixBackend,
    mode: LiveStateMode,
) -> Result<(), LixError> {
    backend
        .execute(&build_set_live_state_mode_sql(mode), &[])
        .await?;
    Ok(())
}

pub(crate) async fn mark_live_state_ready_with_backend(
    backend: &dyn LixBackend,
    watermark: &CanonicalWatermark,
) -> Result<(), LixError> {
    backend
        .execute(&build_mark_live_state_ready_sql(watermark), &[])
        .await?;
    Ok(())
}

pub(crate) async fn load_live_state_mode_with_backend(
    backend: &dyn LixBackend,
) -> Result<LiveStateMode, LixError> {
    Ok(load_live_state_status_row_with_backend(backend).await?.mode)
}

pub(crate) async fn try_claim_live_state_bootstrap_with_backend(
    backend: &dyn LixBackend,
) -> Result<bool, LixError> {
    let result = backend
        .execute(
            "UPDATE lix_internal_live_state_status \
             SET mode = 'bootstrapping', \
                 latest_change_id = NULL, \
                 latest_change_created_at = NULL, \
                 schema_epoch = $1, \
                 updated_at = CURRENT_TIMESTAMP \
             WHERE singleton_id = 1 \
               AND mode = 'uninitialized' \
             RETURNING singleton_id",
            &[Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string())],
        )
        .await?;
    Ok(result.rows.first().is_some())
}

pub(crate) async fn load_latest_canonical_watermark(
    backend: &dyn LixBackend,
) -> Result<Option<CanonicalWatermark>, LixError> {
    let result = backend
        .execute(
            "SELECT id, created_at \
             FROM lix_internal_change \
             ORDER BY created_at DESC, id DESC \
             LIMIT 1",
            &[],
        )
        .await?;
    parse_latest_canonical_watermark(&result)
}

pub(crate) async fn load_latest_canonical_watermark_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<Option<CanonicalWatermark>, LixError> {
    let result = transaction
        .execute(
            "SELECT id, created_at \
             FROM lix_internal_change \
             ORDER BY created_at DESC, id DESC \
             LIMIT 1",
            &[],
        )
        .await?;
    parse_latest_canonical_watermark(&result)
}

fn parse_latest_canonical_watermark(
    result: &QueryResult,
) -> Result<Option<CanonicalWatermark>, LixError> {
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    Ok(Some(CanonicalWatermark {
        change_id: text_value(row.first(), "lix_internal_change.id")?,
        created_at: text_value(row.get(1), "lix_internal_change.created_at")?,
    }))
}

pub(crate) fn build_set_live_state_mode_sql(mode: LiveStateMode) -> String {
    format!(
        "INSERT INTO {table} (\
         singleton_id, mode, latest_change_id, latest_change_created_at, schema_epoch, updated_at\
         ) VALUES (\
         {singleton_id}, '{mode}', NULL, NULL, '{schema_epoch}', CURRENT_TIMESTAMP\
         ) ON CONFLICT (singleton_id) DO UPDATE SET \
         mode = excluded.mode, \
         latest_change_id = excluded.latest_change_id, \
         latest_change_created_at = excluded.latest_change_created_at, \
         schema_epoch = excluded.schema_epoch, \
         updated_at = excluded.updated_at",
        table = LIVE_STATE_STATUS_TABLE,
        singleton_id = LIVE_STATE_STATUS_SINGLETON_ID,
        mode = mode.as_str(),
        schema_epoch = LIVE_STATE_SCHEMA_EPOCH,
    )
}

pub(crate) fn build_mark_live_state_ready_sql(watermark: &CanonicalWatermark) -> String {
    format!(
        "INSERT INTO {table} (\
         singleton_id, mode, latest_change_id, latest_change_created_at, schema_epoch, updated_at\
         ) VALUES (\
         {singleton_id}, 'ready', '{change_id}', '{created_at}', '{schema_epoch}', CURRENT_TIMESTAMP\
         ) ON CONFLICT (singleton_id) DO UPDATE SET \
         mode = excluded.mode, \
         latest_change_id = excluded.latest_change_id, \
         latest_change_created_at = excluded.latest_change_created_at, \
         schema_epoch = excluded.schema_epoch, \
         updated_at = excluded.updated_at",
        table = LIVE_STATE_STATUS_TABLE,
        singleton_id = LIVE_STATE_STATUS_SINGLETON_ID,
        change_id = escape_sql_string(&watermark.change_id),
        created_at = escape_sql_string(&watermark.created_at),
        schema_epoch = LIVE_STATE_SCHEMA_EPOCH,
    )
}

async fn load_live_state_status_row_with_backend(
    backend: &dyn LixBackend,
) -> Result<LiveStateStatusRow, LixError> {
    let result = backend
        .execute(
            "SELECT mode, latest_change_id, latest_change_created_at, schema_epoch \
             FROM lix_internal_live_state_status \
             WHERE singleton_id = 1 \
             LIMIT 1",
            &[],
        )
        .await;
    parse_live_state_status_result(result)
}

async fn load_live_state_snapshot_with_backend(
    backend: &dyn LixBackend,
) -> Result<LiveStateSnapshot, LixError> {
    let result = backend
        .execute(&build_load_live_state_snapshot_sql(), &[])
        .await;
    parse_live_state_snapshot_result(result)
}

async fn load_live_state_snapshot_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<LiveStateSnapshot, LixError> {
    let result = transaction
        .execute(&build_load_live_state_snapshot_sql(), &[])
        .await;
    parse_live_state_snapshot_result(result)
}

fn parse_live_state_status_result(
    result: Result<QueryResult, LixError>,
) -> Result<LiveStateStatusRow, LixError> {
    let result = match result {
        Ok(result) => result,
        Err(error) if is_missing_relation_error(&error) => return Ok(default_live_state_status()),
        Err(error) => return Err(error),
    };
    let Some(row) = result.rows.first() else {
        return Ok(default_live_state_status());
    };
    live_state_status_row_from_values(row)
}

fn parse_live_state_snapshot_result(
    result: Result<QueryResult, LixError>,
) -> Result<LiveStateSnapshot, LixError> {
    let result = match result {
        Ok(result) => result,
        Err(error) if is_missing_relation_error(&error) => return Ok(default_live_state_snapshot()),
        Err(error) => return Err(error),
    };
    let Some(row) = result.rows.first() else {
        return Ok(default_live_state_snapshot());
    };

    let status = parse_nullable_live_state_status(row)?;
    let latest_canonical_watermark = parse_nullable_canonical_watermark(row.get(4), row.get(5))?;
    Ok(LiveStateSnapshot {
        status,
        latest_canonical_watermark,
    })
}

fn live_state_status_row_from_values(row: &[Value]) -> Result<LiveStateStatusRow, LixError> {
    let mode_text = text_value(row.first(), "lix_internal_live_state_status.mode")?;
    let Some(mode) = LiveStateMode::parse(&mode_text) else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("invalid live state mode '{mode_text}'"),
        ));
    };
    let latest_change_id = optional_text_value(row.get(1))?;
    let latest_change_created_at = optional_text_value(row.get(2))?;
    let watermark = match (latest_change_id, latest_change_created_at) {
        (Some(change_id), Some(created_at)) => Some(CanonicalWatermark {
            change_id,
            created_at,
        }),
        (None, None) => None,
        _ => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "live state watermark is partially populated",
            ))
        }
    };

    Ok(LiveStateStatusRow {
        mode,
        schema_epoch: text_value(row.get(3), "lix_internal_live_state_status.schema_epoch")?,
        watermark,
    })
}

fn default_live_state_status() -> LiveStateStatusRow {
    LiveStateStatusRow {
        mode: LiveStateMode::Uninitialized,
        schema_epoch: LIVE_STATE_SCHEMA_EPOCH.to_string(),
        watermark: None,
    }
}

fn default_live_state_snapshot() -> LiveStateSnapshot {
    LiveStateSnapshot {
        status: None,
        latest_canonical_watermark: None,
    }
}

fn parse_nullable_live_state_status(row: &[Value]) -> Result<Option<LiveStateStatusRow>, LixError> {
    if row.is_empty() {
        return Ok(None);
    }
    match optional_text_value(row.first())? {
        None => {
            let latest_change_id = optional_text_value(row.get(1))?;
            let latest_change_created_at = optional_text_value(row.get(2))?;
            let schema_epoch = optional_text_value(row.get(3))?;
            if latest_change_id.is_some()
                || latest_change_created_at.is_some()
                || schema_epoch.is_some()
            {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "live state status row is partially populated",
                ));
            }
            Ok(None)
        }
        Some(mode_text) => {
            let Some(mode) = LiveStateMode::parse(&mode_text) else {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    &format!("invalid live state mode '{mode_text}'"),
                ));
            };
            let latest_change_id = optional_text_value(row.get(1))?;
            let latest_change_created_at = optional_text_value(row.get(2))?;
            let watermark = match (latest_change_id, latest_change_created_at) {
                (Some(change_id), Some(created_at)) => Some(CanonicalWatermark {
                    change_id,
                    created_at,
                }),
                (None, None) => None,
                _ => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "live state watermark is partially populated",
                    ))
                }
            };

            Ok(Some(LiveStateStatusRow {
                mode,
                schema_epoch: text_value(
                    row.get(3),
                    "lix_internal_live_state_status.schema_epoch",
                )?,
                watermark,
            }))
        }
    }
}

fn parse_nullable_canonical_watermark(
    change_id: Option<&Value>,
    created_at: Option<&Value>,
) -> Result<Option<CanonicalWatermark>, LixError> {
    match (
        optional_text_value(change_id)?,
        optional_text_value(created_at)?,
    ) {
        (Some(change_id), Some(created_at)) => Ok(Some(CanonicalWatermark {
            change_id,
            created_at,
        })),
        (None, None) => Ok(None),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "canonical live-state watermark is partially populated",
        )),
    }
}

fn text_value(value: Option<&Value>, field: &str) -> Result<String, LixError> {
    match value {
        Some(Value::Text(text)) if !text.is_empty() => Ok(text.clone()),
        Some(Value::Text(_)) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("{field} is empty"),
        )),
        Some(Value::Integer(number)) => Ok(number.to_string()),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("expected text-like value for {field}, got {other:?}"),
        )),
        None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("{field} is missing"),
        )),
    }
}

fn optional_text_value(value: Option<&Value>) -> Result<Option<String>, LixError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Text(text)) => Ok(Some(text.clone())),
        Some(Value::Integer(number)) => Ok(Some(number.to_string())),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("expected nullable text-like live state field, got {other:?}"),
        )),
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn build_load_live_state_snapshot_sql() -> String {
    "WITH status AS (\
       SELECT mode, latest_change_id, latest_change_created_at, schema_epoch \
       FROM lix_internal_live_state_status \
       WHERE singleton_id = 1 \
       LIMIT 1\
     ), canonical AS (\
       SELECT id, created_at \
       FROM lix_internal_change \
       ORDER BY created_at DESC, id DESC \
       LIMIT 1\
     ) \
     SELECT \
       (SELECT mode FROM status), \
       (SELECT latest_change_id FROM status), \
       (SELECT latest_change_created_at FROM status), \
       (SELECT schema_epoch FROM status), \
       (SELECT id FROM canonical), \
       (SELECT created_at FROM canonical)"
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::SqlDialect;
    use async_trait::async_trait;

    #[derive(Default)]
    struct FakeBackend {
        status_row: Option<Vec<Value>>,
        latest_watermark: Option<(String, String)>,
        executed_sql: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.executed_sql.lock().unwrap().push(sql.to_string());
            if sql.contains("WITH status AS")
                && sql.contains("lix_internal_live_state_status")
                && sql.contains("lix_internal_change")
            {
                let mut row = self
                    .status_row
                    .clone()
                    .unwrap_or_else(|| vec![Value::Null, Value::Null, Value::Null, Value::Null]);
                match &self.latest_watermark {
                    Some((id, created_at)) => {
                        row.push(Value::Text(id.clone()));
                        row.push(Value::Text(created_at.clone()));
                    }
                    None => {
                        row.push(Value::Null);
                        row.push(Value::Null);
                    }
                }
                return Ok(QueryResult {
                    rows: vec![row],
                    columns: vec![
                        "mode".to_string(),
                        "latest_change_id".to_string(),
                        "latest_change_created_at".to_string(),
                        "schema_epoch".to_string(),
                        "id".to_string(),
                        "created_at".to_string(),
                    ],
                });
            }
            if sql.contains("FROM lix_internal_live_state_status") {
                return Ok(QueryResult {
                    rows: self.status_row.clone().into_iter().collect(),
                    columns: vec![
                        "mode".to_string(),
                        "latest_change_id".to_string(),
                        "latest_change_created_at".to_string(),
                        "schema_epoch".to_string(),
                    ],
                });
            }
            if sql.contains("FROM lix_internal_change")
                && sql.contains("ORDER BY created_at DESC, id DESC")
            {
                return Ok(QueryResult {
                    rows: self
                        .latest_watermark
                        .clone()
                        .into_iter()
                        .map(|(id, created_at)| vec![Value::Text(id), Value::Text(created_at)])
                        .collect(),
                    columns: vec!["id".to_string(), "created_at".to_string()],
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "transactions not used in fake backend",
            ))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "begin_savepoint not supported in test backend",
            ))
        }
    }

    struct FakeTransaction {
        status_row: Option<Vec<Value>>,
        latest_watermark: Option<(String, String)>,
    }

    #[async_trait(?Send)]
    impl LixBackendTransaction for FakeTransaction {
        fn dialect(&self) -> crate::SqlDialect {
            crate::SqlDialect::Sqlite
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("WITH status AS")
                && sql.contains("lix_internal_live_state_status")
                && sql.contains("lix_internal_change")
            {
                let mut row = self
                    .status_row
                    .clone()
                    .unwrap_or_else(|| vec![Value::Null, Value::Null, Value::Null, Value::Null]);
                match &self.latest_watermark {
                    Some((id, created_at)) => {
                        row.push(Value::Text(id.clone()));
                        row.push(Value::Text(created_at.clone()));
                    }
                    None => {
                        row.push(Value::Null);
                        row.push(Value::Null);
                    }
                }
                return Ok(QueryResult {
                    rows: vec![row],
                    columns: vec![
                        "mode".to_string(),
                        "latest_change_id".to_string(),
                        "latest_change_created_at".to_string(),
                        "schema_epoch".to_string(),
                        "id".to_string(),
                        "created_at".to_string(),
                    ],
                });
            }
            if sql.contains("FROM lix_internal_live_state_status") {
                return Ok(QueryResult {
                    rows: self.status_row.clone().into_iter().collect(),
                    columns: vec![
                        "mode".to_string(),
                        "latest_change_id".to_string(),
                        "latest_change_created_at".to_string(),
                        "schema_epoch".to_string(),
                    ],
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn readiness_is_uninitialized_without_canonical_state() {
        let backend = FakeBackend::default();
        assert_eq!(
            evaluate_live_state_snapshot(&load_live_state_snapshot(&backend).await.unwrap()),
            LiveStateReadiness::Uninitialized
        );
    }

    #[tokio::test]
    async fn readiness_is_ready_when_status_matches_latest_canonical_change() {
        let backend = FakeBackend {
            status_row: Some(vec![
                Value::Text("ready".to_string()),
                Value::Text("change-2".to_string()),
                Value::Text("2026-03-15T01:02:03Z".to_string()),
                Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string()),
            ]),
            latest_watermark: Some(("change-2".to_string(), "2026-03-15T01:02:03Z".to_string())),
            executed_sql: std::sync::Mutex::new(Vec::new()),
        };

        assert_eq!(
            evaluate_live_state_snapshot(&load_live_state_snapshot(&backend).await.unwrap()),
            LiveStateReadiness::Ready
        );
    }

    #[tokio::test]
    async fn readiness_mismatch_is_observed_without_mutating_status() {
        let backend = FakeBackend {
            status_row: Some(vec![
                Value::Text("ready".to_string()),
                Value::Text("change-1".to_string()),
                Value::Text("2026-03-15T01:02:02Z".to_string()),
                Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string()),
            ]),
            latest_watermark: Some(("change-2".to_string(), "2026-03-15T01:02:03Z".to_string())),
            executed_sql: std::sync::Mutex::new(Vec::new()),
        };

        let snapshot = load_live_state_snapshot(&backend).await.unwrap();
        assert_eq!(
            evaluate_live_state_snapshot(&snapshot),
            LiveStateReadiness::NeedsRebuild
        );

        let executed_sql = backend.executed_sql.lock().unwrap().clone();
        assert_eq!(executed_sql.len(), 1);
        assert!(
            !executed_sql[0]
                .to_ascii_lowercase()
                .contains("insert into lix_internal_live_state_status"),
            "observer path must not mutate live-state status"
        );
        assert!(
            !executed_sql[0]
                .to_ascii_lowercase()
                .contains("update lix_internal_live_state_status"),
            "observer path must not mutate live-state status"
        );
    }

    #[tokio::test]
    async fn transaction_ready_check_rejects_needs_rebuild() {
        let mut transaction = FakeTransaction {
            status_row: Some(vec![
                Value::Text("needs_rebuild".to_string()),
                Value::Null,
                Value::Null,
                Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string()),
            ]),
            latest_watermark: None,
        };

        let error = ensure_live_state_ready_in_transaction(&mut transaction)
            .await
            .expect_err("needs_rebuild should fail");
        assert_eq!(
            error.code,
            crate::errors::ErrorCode::LiveStateNotReady.as_str()
        );
    }

    #[tokio::test]
    async fn readiness_without_status_but_with_canonical_state_requires_rebuild() {
        let backend = FakeBackend {
            status_row: None,
            latest_watermark: Some(("change-2".to_string(), "2026-03-15T01:02:03Z".to_string())),
            executed_sql: std::sync::Mutex::new(Vec::new()),
        };

        let snapshot = load_live_state_snapshot(&backend).await.unwrap();
        assert_eq!(
            evaluate_live_state_snapshot(&snapshot),
            LiveStateReadiness::NeedsRebuild
        );
    }

    #[tokio::test]
    async fn transaction_ready_check_allows_inflight_watermark_drift() {
        let mut transaction = FakeTransaction {
            status_row: Some(vec![
                Value::Text("ready".to_string()),
                Value::Text("change-1".to_string()),
                Value::Text("2026-03-15T01:02:02Z".to_string()),
                Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string()),
            ]),
            latest_watermark: Some(("change-2".to_string(), "2026-03-15T01:02:03Z".to_string())),
        };

        ensure_live_state_ready_in_transaction(&mut transaction)
            .await
            .expect("inflight watermark drift inside transaction should be allowed");
    }
}
