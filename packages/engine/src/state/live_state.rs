use crate::errors::classification::is_missing_relation_error;
use crate::{LixBackend, LixError, LixTransaction, QueryResult, Value};

pub(crate) const LIVE_STATE_SCHEMA_EPOCH: &str = "1";
pub(crate) const LIVE_STATE_STATUS_TABLE: &str = "lix_internal_live_state_status";
const LIVE_STATE_STATUS_SINGLETON_ID: i64 = 1;

pub(crate) const LIVE_STATE_STATUS_CREATE_TABLE_SQL: &str =
    "CREATE TABLE IF NOT EXISTS lix_internal_live_state_status (\
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
     SELECT 1, 'needs_rebuild', NULL, NULL, '1', '1970-01-01T00:00:00Z' \
     WHERE NOT EXISTS (\
       SELECT 1 FROM lix_internal_live_state_status WHERE singleton_id = 1\
     )";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveStateMode {
    Ready,
    NeedsRebuild,
    Rebuilding,
}

impl LiveStateMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::NeedsRebuild => "needs_rebuild",
            Self::Rebuilding => "rebuilding",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveStateReadiness {
    Uninitialized,
    Ready,
    NeedsRebuild,
}

pub(crate) async fn canonical_state_exists(backend: &dyn LixBackend) -> Result<bool, LixError> {
    match backend
        .execute("SELECT 1 FROM lix_internal_change LIMIT 1", &[])
        .await
    {
        Ok(result) => Ok(result.rows.first().is_some()),
        Err(error) if is_missing_relation_error(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

pub(crate) async fn refresh_live_state_readiness(
    backend: &dyn LixBackend,
) -> Result<LiveStateReadiness, LixError> {
    if !canonical_state_exists(backend).await? {
        return Ok(LiveStateReadiness::Uninitialized);
    }

    let status = load_live_state_status_row_with_backend(backend).await?;
    let latest = load_latest_canonical_watermark(backend).await?;
    let ready = status.mode == LiveStateMode::Ready
        && status.schema_epoch == LIVE_STATE_SCHEMA_EPOCH
        && status.watermark == latest;
    if ready {
        return Ok(LiveStateReadiness::Ready);
    }
    if status.mode == LiveStateMode::Rebuilding {
        return Ok(LiveStateReadiness::NeedsRebuild);
    }

    mark_live_state_mode_with_backend(backend, LiveStateMode::NeedsRebuild).await?;
    Ok(LiveStateReadiness::NeedsRebuild)
}

pub(crate) async fn ensure_live_state_ready(backend: &dyn LixBackend) -> Result<(), LixError> {
    match refresh_live_state_readiness(backend).await? {
        LiveStateReadiness::Ready => Ok(()),
        LiveStateReadiness::Uninitialized => Err(crate::errors::not_initialized_error()),
        LiveStateReadiness::NeedsRebuild => Err(crate::errors::live_state_not_ready_error()),
    }
}

pub(crate) async fn ensure_live_state_ready_in_transaction(
    transaction: &mut dyn LixTransaction,
) -> Result<(), LixError> {
    let status = load_live_state_status_row_in_transaction(transaction).await?;
    let ready =
        status.mode == LiveStateMode::Ready && status.schema_epoch == LIVE_STATE_SCHEMA_EPOCH;
    if ready {
        return Ok(());
    }
    Err(crate::errors::live_state_not_ready_error())
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
    transaction: &mut dyn LixTransaction,
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

async fn load_live_state_status_row_in_transaction(
    transaction: &mut dyn LixTransaction,
) -> Result<LiveStateStatusRow, LixError> {
    let result = transaction
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
        mode: LiveStateMode::NeedsRebuild,
        schema_epoch: LIVE_STATE_SCHEMA_EPOCH.to_string(),
        watermark: None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::SqlDialect;
    use async_trait::async_trait;

    #[derive(Default)]
    struct FakeBackend {
        canonical_exists: bool,
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
            if sql.contains("SELECT 1 FROM lix_internal_change") {
                return Ok(QueryResult {
                    rows: if self.canonical_exists {
                        vec![vec![Value::Integer(1)]]
                    } else {
                        Vec::new()
                    },
                    columns: vec!["1".to_string()],
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

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "transactions not used in fake backend",
            ))
        }
    }

    struct FakeTransaction {
        status_row: Vec<Value>,
    }

    #[async_trait(?Send)]
    impl LixTransaction for FakeTransaction {
        fn dialect(&self) -> crate::SqlDialect {
            crate::SqlDialect::Sqlite
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_live_state_status") {
                return Ok(QueryResult {
                    rows: vec![self.status_row.clone()],
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
            refresh_live_state_readiness(&backend).await.unwrap(),
            LiveStateReadiness::Uninitialized
        );
    }

    #[tokio::test]
    async fn readiness_is_ready_when_status_matches_latest_canonical_change() {
        let backend = FakeBackend {
            canonical_exists: true,
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
            refresh_live_state_readiness(&backend).await.unwrap(),
            LiveStateReadiness::Ready
        );
    }

    #[tokio::test]
    async fn transaction_ready_check_rejects_needs_rebuild() {
        let mut transaction = FakeTransaction {
            status_row: vec![
                Value::Text("needs_rebuild".to_string()),
                Value::Null,
                Value::Null,
                Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string()),
            ],
        };

        let error = ensure_live_state_ready_in_transaction(&mut transaction)
            .await
            .expect_err("needs_rebuild should fail");
        assert_eq!(
            error.code,
            crate::errors::ErrorCode::LiveStateNotReady.as_str()
        );
    }
}
