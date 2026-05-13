use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvAccessSegment, BackendKvEntryPage, BackendKvExistsBatch,
    BackendKvExistsGroup, BackendKvGetRequest, BackendKvKeyPage, BackendKvRead4Order,
    BackendKvRead4Page, BackendKvRead4Projection, BackendKvRead4ValuePart, BackendKvReadV3Presence,
    BackendKvScanRange, BackendKvScanRequest, BackendKvTableReadRequest, BackendKvValueBatch,
    BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch, BackendKvWriteOp,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, BytePageBuilder,
    LixError,
};
use rusqlite::{params, params_from_iter, types::Value as SqlValue, Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const UNTRACKED_NAMESPACE: &str = "u3";

#[derive(Clone)]
pub(crate) struct SqliteBenchBackend {
    connection: Arc<Mutex<Connection>>,
    #[allow(dead_code)]
    path: Option<Arc<PathBuf>>,
    _temp_dir: Option<Arc<TempDir>>,
}

pub(crate) struct SqliteBenchTransaction {
    connection: Arc<Mutex<Connection>>,
    finalized: bool,
}

impl SqliteBenchBackend {
    pub(crate) fn tempfile() -> Result<Self, LixError> {
        let temp_dir = Arc::new(TempDir::new().map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("sqlite bench tempdir: {error}"),
            )
        })?);
        let path = Arc::new(temp_dir.path().join("bench.sqlite"));
        let connection = Connection::open(path.as_path()).map_err(sqlite_error)?;
        configure_connection(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            path: Some(path),
            _temp_dir: Some(temp_dir),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn path(&self) -> Option<&Path> {
        self.path.as_deref().map(PathBuf::as_path)
    }

    fn lock_connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, LixError> {
        self.connection
            .lock()
            .map_err(|_| LixError::new("LIX_ERROR_UNKNOWN", "sqlite bench connection poisoned"))
    }
}

fn configure_connection(connection: &Connection) -> Result<(), LixError> {
    connection
        .execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA temp_store = MEMORY;
            PRAGMA foreign_keys = ON;
            CREATE TABLE kv (
                namespace TEXT NOT NULL,
                key BLOB NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY (namespace, key)
            ) WITHOUT ROWID;
            CREATE TABLE kv_u (
                key BLOB NOT NULL PRIMARY KEY,
                value BLOB NOT NULL
            ) WITHOUT ROWID;
            ",
        )
        .map_err(sqlite_error)?;
    Ok(())
}

#[async_trait]
impl Backend for SqliteBenchBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        let connection = self.lock_connection()?;
        connection
            .execute_batch("BEGIN DEFERRED")
            .map_err(sqlite_error)?;
        drop(connection);
        Ok(Box::new(SqliteBenchTransaction {
            connection: Arc::clone(&self.connection),
            finalized: false,
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        let connection = self.lock_connection()?;
        connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(sqlite_error)?;
        drop(connection);
        Ok(Box::new(SqliteBenchTransaction {
            connection: Arc::clone(&self.connection),
            finalized: false,
        }))
    }
}

#[async_trait]
impl BackendReadTransaction for SqliteBenchTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        let connection = self.lock_connection()?;
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let key_count = group.keys.len();
            let keys = group.keys;
            let mut values = BytePageBuilder::with_capacity(key_count, 0);
            let mut present = Vec::with_capacity(key_count);
            if keys.is_empty() {
                groups.push(BackendKvValueGroup::new(
                    namespace,
                    values.finish(),
                    present,
                ));
                continue;
            }

            let untracked = namespace == UNTRACKED_NAMESPACE;
            let key_placeholders = std::iter::repeat_n("?", key_count)
                .collect::<Vec<_>>()
                .join(", ");
            let sql = if untracked {
                format!(
                    "
                    SELECT key, value
                    FROM kv_u
                    WHERE key IN ({key_placeholders})
                    "
                )
            } else {
                format!(
                    "
                    SELECT key, value
                    FROM kv
                    WHERE namespace = ? AND key IN ({key_placeholders})
                    "
                )
            };
            let mut parameters = Vec::with_capacity(usize::from(!untracked) + key_count);
            if !untracked {
                parameters.push(SqlValue::Text(namespace.clone()));
            }
            parameters.extend(keys.iter().cloned().map(SqlValue::Blob));

            let mut statement = connection.prepare(&sql).map_err(sqlite_error)?;
            let mut rows = statement
                .query(params_from_iter(parameters))
                .map_err(sqlite_error)?;
            let mut values_by_key = HashMap::with_capacity(key_count);
            while let Some(row) = rows.next().map_err(sqlite_error)? {
                values_by_key.insert(
                    row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?,
                    row.get::<_, Vec<u8>>(1).map_err(sqlite_error)?,
                );
            }

            for key in keys {
                if let Some(value) = values_by_key.get(&key) {
                    values.push(value);
                    present.push(true);
                } else {
                    values.push([]);
                    present.push(false);
                }
            }
            if present.len() != key_count {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "sqlite get_values returned {} rows for {key_count} requested keys",
                        present.len()
                    ),
                ));
            }
            groups.push(BackendKvValueGroup::new(
                namespace,
                values.finish(),
                present,
            ));
        }
        Ok(BackendKvValueBatch { groups })
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        let connection = self.lock_connection()?;
        let mut statement = connection
            .prepare_cached("SELECT 1 FROM kv WHERE namespace = ?1 AND key = ?2")
            .map_err(sqlite_error)?;
        let mut untracked_statement = connection
            .prepare_cached("SELECT 1 FROM kv_u WHERE key = ?1")
            .map_err(sqlite_error)?;
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let mut exists = Vec::with_capacity(group.keys.len());
            if namespace == UNTRACKED_NAMESPACE {
                for key in group.keys {
                    exists.push(
                        untracked_statement
                            .query_row(params![key.as_slice()], |_| Ok(()))
                            .optional()
                            .map_err(sqlite_error)?
                            .is_some(),
                    );
                }
            } else {
                for key in group.keys {
                    exists.push(
                        statement
                            .query_row(params![namespace.as_str(), key.as_slice()], |_| Ok(()))
                            .optional()
                            .map_err(sqlite_error)?
                            .is_some(),
                    );
                }
            }
            groups.push(BackendKvExistsGroup { namespace, exists });
        }
        Ok(BackendKvExistsBatch { groups })
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        let connection = self.lock_connection()?;
        if request.namespace == UNTRACKED_NAMESPACE {
            sqlite_scan_untracked_keys(&connection, request)
        } else {
            sqlite_scan_keys(&connection, request)
        }
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        let connection = self.lock_connection()?;
        if request.namespace == UNTRACKED_NAMESPACE {
            sqlite_scan_untracked_values(&connection, request)
        } else {
            sqlite_scan_values(&connection, request)
        }
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        let connection = self.lock_connection()?;
        if request.namespace == UNTRACKED_NAMESPACE {
            sqlite_scan_untracked_entries(&connection, request)
        } else {
            sqlite_scan_entries(&connection, request)
        }
    }

    async fn read4(
        &mut self,
        request: BackendKvTableReadRequest,
    ) -> Result<BackendKvRead4Page, LixError> {
        let connection = self.lock_connection()?;
        sqlite_read4(&connection, request)
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        self.lock_connection()?
            .execute_batch("ROLLBACK")
            .map_err(sqlite_error)?;
        self.finalized = true;
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for SqliteBenchTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        let connection = self.lock_connection()?;
        let mut put_statement = connection
            .prepare_cached(
                "
                INSERT OR REPLACE INTO kv (namespace, key, value)
                VALUES (?1, ?2, ?3)
                ",
            )
            .map_err(sqlite_error)?;
        let mut delete_statement = connection
            .prepare_cached("DELETE FROM kv WHERE namespace = ?1 AND key = ?2")
            .map_err(sqlite_error)?;
        let mut put_untracked_statement = connection
            .prepare_cached(
                "
                INSERT OR REPLACE INTO kv_u (key, value)
                VALUES (?1, ?2)
                ",
            )
            .map_err(sqlite_error)?;
        let mut delete_untracked_statement = connection
            .prepare_cached("DELETE FROM kv_u WHERE key = ?1")
            .map_err(sqlite_error)?;
        let mut stats = BackendKvWriteStats::default();
        for group in batch.groups {
            let namespace = group.namespace().to_string();
            for op in group.ops() {
                match op {
                    BackendKvWriteOp::Put { key, value } => {
                        if namespace == UNTRACKED_NAMESPACE {
                            put_untracked_statement
                                .raw_bind_parameter(1, key.as_slice())
                                .map_err(sqlite_error)?;
                            put_untracked_statement
                                .raw_bind_parameter(2, value.as_slice())
                                .map_err(sqlite_error)?;
                            put_untracked_statement
                                .raw_execute()
                                .map_err(sqlite_error)?;
                        } else {
                            put_statement
                                .raw_bind_parameter(1, namespace.as_str())
                                .map_err(sqlite_error)?;
                            put_statement
                                .raw_bind_parameter(2, key.as_slice())
                                .map_err(sqlite_error)?;
                            put_statement
                                .raw_bind_parameter(3, value.as_slice())
                                .map_err(sqlite_error)?;
                            put_statement.raw_execute().map_err(sqlite_error)?;
                        }
                        stats.puts += 1;
                        stats.bytes_written += key.len() + value.len();
                    }
                    BackendKvWriteOp::Delete { key } => {
                        if namespace == UNTRACKED_NAMESPACE {
                            delete_untracked_statement
                                .raw_bind_parameter(1, key.as_slice())
                                .map_err(sqlite_error)?;
                            delete_untracked_statement
                                .raw_execute()
                                .map_err(sqlite_error)?;
                        } else {
                            delete_statement
                                .raw_bind_parameter(1, namespace.as_str())
                                .map_err(sqlite_error)?;
                            delete_statement
                                .raw_bind_parameter(2, key.as_slice())
                                .map_err(sqlite_error)?;
                            delete_statement.raw_execute().map_err(sqlite_error)?;
                        }
                        stats.deletes += 1;
                        stats.bytes_written += key.len();
                    }
                    BackendKvWriteOp::DeleteRange { range } => {
                        sqlite_delete_range(&connection, namespace.as_str(), range)?;
                        stats.delete_ranges += 1;
                        stats.bytes_written += delete_range_bytes(range);
                    }
                }
            }
        }
        Ok(stats)
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        self.lock_connection()?
            .execute_batch("COMMIT")
            .map_err(sqlite_error)?;
        self.finalized = true;
        Ok(())
    }
}

impl SqliteBenchTransaction {
    fn lock_connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, LixError> {
        self.connection
            .lock()
            .map_err(|_| LixError::new("LIX_ERROR_UNKNOWN", "sqlite bench connection poisoned"))
    }
}

impl Drop for SqliteBenchTransaction {
    fn drop(&mut self) {
        if !self.finalized {
            if let Ok(connection) = self.connection.lock() {
                let _ = connection.execute_batch("ROLLBACK");
            }
        }
    }
}

fn sqlite_read4(
    connection: &Connection,
    request: BackendKvTableReadRequest,
) -> Result<BackendKvRead4Page, LixError> {
    if request.residual_filter.is_some() {
        return Err(LixError::unknown(
            "sqlite bench read4 cannot apply residual filters",
        ));
    }
    if request.session.is_some() {
        return Err(LixError::unknown(
            "sqlite bench read4 does not support read sessions",
        ));
    }

    let mut keyed = Vec::new();
    let mut run_spans = Vec::new();
    let mut spans = Vec::new();
    for segment in request.access {
        match segment {
            BackendKvAccessSegment::Points {
                keys,
                request_indexes,
            } => sqlite_read4_push_indexed_keys(&mut keyed, keys, request_indexes)?,
            BackendKvAccessSegment::Run {
                lower,
                upper,
                keys,
                request_indexes,
            } => {
                run_spans.push((lower, upper));
                sqlite_read4_push_indexed_keys(&mut keyed, keys, request_indexes)?;
            }
            BackendKvAccessSegment::Span { lower, upper } => spans.push((lower, upper)),
        }
    }

    if !keyed.is_empty() && !spans.is_empty() {
        return Err(LixError::unknown(
            "sqlite bench read4 cannot mix point/run and span access",
        ));
    }
    if !keyed.is_empty() || spans.is_empty() {
        if request.after.is_some() {
            return Err(LixError::unknown(
                "sqlite bench read4 point/run access does not support after cursors",
            ));
        }
        if run_spans.is_empty() {
            return sqlite_read4_points(
                connection,
                request.table.namespace,
                keyed,
                request.projection,
                request.output_order,
            );
        }
        return sqlite_read4_runs(
            connection,
            request.table.namespace,
            keyed,
            run_spans,
            request.projection,
            request.output_order,
        );
    }

    if request.output_order != BackendKvRead4Order::KeyOrder {
        return Err(LixError::unknown(
            "sqlite bench read4 span access requires key order output",
        ));
    }
    sqlite_read4_spans(
        connection,
        request.table.namespace,
        spans,
        request.after,
        request.limit.unwrap_or(usize::MAX),
        request.projection,
    )
}

fn sqlite_read4_push_indexed_keys(
    output: &mut Vec<(u32, Vec<u8>)>,
    keys: Vec<Vec<u8>>,
    request_indexes: Vec<u32>,
) -> Result<(), LixError> {
    if keys.len() != request_indexes.len() {
        return Err(LixError::unknown("sqlite bench read4 key/index mismatch"));
    }
    output.extend(request_indexes.into_iter().zip(keys));
    Ok(())
}

fn sqlite_read4_points(
    connection: &Connection,
    namespace: String,
    mut keyed: Vec<(u32, Vec<u8>)>,
    projection: BackendKvRead4Projection,
    order: BackendKvRead4Order,
) -> Result<BackendKvRead4Page, LixError> {
    match order {
        BackendKvRead4Order::RequestOrder => keyed.sort_by_key(|(index, _)| *index),
        BackendKvRead4Order::KeyOrder => keyed.sort_by(|left, right| left.1.cmp(&right.1)),
    }

    let request_indexes = match order {
        BackendKvRead4Order::RequestOrder => None,
        BackendKvRead4Order::KeyOrder => Some(keyed.iter().map(|(index, _)| *index).collect()),
    };
    let mut keys = BytePageBuilder::with_capacity(keyed.len(), 0);
    for (_, key) in &keyed {
        keys.push(key);
    }
    let value_count = sqlite_read4_projection_len(&projection);
    let mut value_builders = (0..value_count)
        .map(|_| BytePageBuilder::with_capacity(keyed.len(), 0))
        .collect::<Vec<_>>();

    if keyed.is_empty() {
        return Ok(BackendKvRead4Page {
            keys: keys.finish(),
            presence: BackendKvReadV3Presence::bitmap(Vec::new()),
            values: value_builders
                .into_iter()
                .map(BytePageBuilder::finish)
                .collect(),
            request_indexes,
            resume_after: None,
        });
    }

    let values_by_key =
        sqlite_read4_select_points(connection, namespace.as_str(), &keyed, &projection)?;
    let mut present = Vec::with_capacity(keyed.len());
    for (_, key) in keyed {
        if let Some(values) = values_by_key.get(&key) {
            present.push(true);
            for (builder, value) in value_builders.iter_mut().zip(values) {
                builder.push(value);
            }
        } else {
            present.push(false);
            for builder in &mut value_builders {
                builder.push([]);
            }
        }
    }

    Ok(BackendKvRead4Page {
        keys: keys.finish(),
        presence: BackendKvReadV3Presence::bitmap(present),
        values: value_builders
            .into_iter()
            .map(BytePageBuilder::finish)
            .collect(),
        request_indexes,
        resume_after: None,
    })
}

fn sqlite_read4_runs(
    connection: &Connection,
    namespace: String,
    mut keyed: Vec<(u32, Vec<u8>)>,
    run_spans: Vec<(Vec<u8>, Vec<u8>)>,
    projection: BackendKvRead4Projection,
    order: BackendKvRead4Order,
) -> Result<BackendKvRead4Page, LixError> {
    match order {
        BackendKvRead4Order::RequestOrder => keyed.sort_by_key(|(index, _)| *index),
        BackendKvRead4Order::KeyOrder => keyed.sort_by(|left, right| left.1.cmp(&right.1)),
    }

    let request_indexes = match order {
        BackendKvRead4Order::RequestOrder => None,
        BackendKvRead4Order::KeyOrder => Some(keyed.iter().map(|(index, _)| *index).collect()),
    };
    let values_by_key =
        sqlite_read4_collect_spans(connection, namespace.as_str(), run_spans, &projection)?;
    let mut keys = BytePageBuilder::with_capacity(keyed.len(), 0);
    let mut present = Vec::with_capacity(keyed.len());
    let mut value_builders = (0..sqlite_read4_projection_len(&projection))
        .map(|_| BytePageBuilder::with_capacity(keyed.len(), 0))
        .collect::<Vec<_>>();
    for (_, key) in keyed {
        keys.push(&key);
        if let Some(values) = values_by_key.get(&key) {
            present.push(true);
            for (builder, value) in value_builders.iter_mut().zip(values) {
                builder.push(value);
            }
        } else {
            present.push(false);
            for builder in &mut value_builders {
                builder.push([]);
            }
        }
    }
    Ok(BackendKvRead4Page {
        keys: keys.finish(),
        presence: BackendKvReadV3Presence::bitmap(present),
        values: value_builders
            .into_iter()
            .map(BytePageBuilder::finish)
            .collect(),
        request_indexes,
        resume_after: None,
    })
}

fn sqlite_read4_collect_spans(
    connection: &Connection,
    namespace: &str,
    mut spans: Vec<(Vec<u8>, Vec<u8>)>,
    projection: &BackendKvRead4Projection,
) -> Result<HashMap<Vec<u8>, Vec<Vec<u8>>>, LixError> {
    spans.retain(|(lower, upper)| upper.is_empty() || lower < upper);
    spans.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let mut values_by_key = HashMap::new();
    for (lower, upper) in spans {
        let page = sqlite_read4_span_page(
            connection,
            namespace,
            &lower,
            &upper,
            None,
            usize::MAX,
            projection,
        )?;
        for (index, key) in page.keys.iter().enumerate() {
            let values = page
                .values
                .iter()
                .map(|values| {
                    values.get(index).map(<[u8]>::to_vec).ok_or_else(|| {
                        LixError::unknown("sqlite bench read4 run value page is misaligned")
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            values_by_key.insert(key.to_vec(), values);
        }
    }
    Ok(values_by_key)
}

fn sqlite_read4_select_points(
    connection: &Connection,
    namespace: &str,
    keyed: &[(u32, Vec<u8>)],
    projection: &BackendKvRead4Projection,
) -> Result<HashMap<Vec<u8>, Vec<Vec<u8>>>, LixError> {
    let untracked = namespace == UNTRACKED_NAMESPACE;
    let key_placeholders = std::iter::repeat_n("?", keyed.len())
        .collect::<Vec<_>>()
        .join(", ");
    let value_columns = sqlite_read4_projection_sql(projection);
    let select_columns = if value_columns.is_empty() {
        "key".to_string()
    } else {
        format!("key, {}", value_columns.join(", "))
    };
    let sql = if untracked {
        format!(
            "
            SELECT {select_columns}
            FROM kv_u
            WHERE key IN ({key_placeholders})
            "
        )
    } else {
        format!(
            "
            SELECT {select_columns}
            FROM kv
            WHERE namespace = ? AND key IN ({key_placeholders})
            "
        )
    };
    let mut parameters = Vec::with_capacity(usize::from(!untracked) + keyed.len());
    if !untracked {
        parameters.push(SqlValue::Text(namespace.to_string()));
    }
    parameters.extend(keyed.iter().map(|(_, key)| SqlValue::Blob(key.clone())));

    let mut statement = connection.prepare_cached(&sql).map_err(sqlite_error)?;
    let mut rows = statement
        .query(params_from_iter(parameters))
        .map_err(sqlite_error)?;
    let value_count = sqlite_read4_projection_len(projection);
    let mut values_by_key = HashMap::with_capacity(keyed.len());
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let key = row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?;
        let mut values = Vec::with_capacity(value_count);
        for index in 0..value_count {
            values.push(row.get::<_, Vec<u8>>(index + 1).map_err(sqlite_error)?);
        }
        values_by_key.insert(key, values);
    }
    Ok(values_by_key)
}

fn sqlite_read4_spans(
    connection: &Connection,
    namespace: String,
    mut spans: Vec<(Vec<u8>, Vec<u8>)>,
    mut after: Option<Vec<u8>>,
    limit: usize,
    projection: BackendKvRead4Projection,
) -> Result<BackendKvRead4Page, LixError> {
    spans.retain(|(lower, upper)| upper.is_empty() || lower < upper);
    spans.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    let mut keys = BytePageBuilder::new();
    let mut value_builders = (0..sqlite_read4_projection_len(&projection))
        .map(|_| BytePageBuilder::new())
        .collect::<Vec<_>>();
    let mut resume_after = None;
    for (span_index, (lower, upper)) in spans.iter().enumerate() {
        let remaining = limit.saturating_sub(keys.len());
        if remaining == 0 {
            resume_after = keys
                .len()
                .checked_sub(1)
                .and_then(|index| keys.get(index))
                .map(<[u8]>::to_vec);
            break;
        }
        let page = sqlite_read4_span_page(
            connection,
            namespace.as_str(),
            lower,
            upper,
            after.as_deref(),
            remaining,
            &projection,
        )?;
        for key in page.keys.iter() {
            keys.push(key);
        }
        for (target, source) in value_builders.iter_mut().zip(page.values) {
            for value in source.iter() {
                target.push(value);
            }
        }
        resume_after = page.resume_after;
        if resume_after.is_some() {
            break;
        }
        if keys.len() == limit && span_index + 1 < spans.len() {
            resume_after = keys
                .len()
                .checked_sub(1)
                .and_then(|index| keys.get(index))
                .map(<[u8]>::to_vec);
            break;
        }
        after = None;
    }

    Ok(BackendKvRead4Page {
        keys: keys.finish(),
        presence: BackendKvReadV3Presence::All,
        values: value_builders
            .into_iter()
            .map(BytePageBuilder::finish)
            .collect(),
        request_indexes: None,
        resume_after,
    })
}

fn sqlite_read4_span_page(
    connection: &Connection,
    namespace: &str,
    lower: &[u8],
    upper: &[u8],
    after: Option<&[u8]>,
    limit: usize,
    projection: &BackendKvRead4Projection,
) -> Result<BackendKvRead4Page, LixError> {
    let untracked = namespace == UNTRACKED_NAMESPACE;
    let value_columns = sqlite_read4_projection_sql(projection);
    let select_columns = if value_columns.is_empty() {
        "key".to_string()
    } else {
        format!("key, {}", value_columns.join(", "))
    };
    let fetch_limit = sqlite_fetch_limit(limit)?;
    let sql = if untracked {
        format!(
            "
            SELECT {select_columns}
            FROM kv_u
            WHERE (?1 IS NULL OR key > ?1)
              AND key >= ?2
              AND (?3 IS NULL OR key < ?3)
            ORDER BY key
            LIMIT ?4
            "
        )
    } else {
        format!(
            "
            SELECT {select_columns}
            FROM kv
            WHERE namespace = ?1
              AND (?2 IS NULL OR key > ?2)
              AND key >= ?3
              AND (?4 IS NULL OR key < ?4)
            ORDER BY key
            LIMIT ?5
            "
        )
    };
    let upper_param = (!upper.is_empty()).then_some(upper);
    let mut statement = connection.prepare_cached(&sql).map_err(sqlite_error)?;
    let mut rows = if untracked {
        statement
            .query(params![after, lower, upper_param, fetch_limit])
            .map_err(sqlite_error)?
    } else {
        statement
            .query(params![namespace, after, lower, upper_param, fetch_limit])
            .map_err(sqlite_error)?
    };
    let value_count = sqlite_read4_projection_len(projection);
    let mut keys = BytePageBuilder::new();
    let mut value_builders = (0..value_count)
        .map(|_| BytePageBuilder::new())
        .collect::<Vec<_>>();
    let mut count = 0;
    let mut resume_after_candidate = None;
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let key = row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?;
        if count < limit {
            resume_after_candidate = Some(key.clone());
            keys.push(&key);
            for (value_index, builder) in value_builders.iter_mut().enumerate() {
                let value = row
                    .get::<_, Vec<u8>>(value_index + 1)
                    .map_err(sqlite_error)?;
                builder.push(&value);
            }
        }
        count += 1;
    }
    let resume_after = (count > limit).then_some(resume_after_candidate).flatten();
    Ok(BackendKvRead4Page {
        keys: keys.finish(),
        presence: BackendKvReadV3Presence::All,
        values: value_builders
            .into_iter()
            .map(BytePageBuilder::finish)
            .collect(),
        request_indexes: None,
        resume_after,
    })
}

fn sqlite_read4_projection_len(projection: &BackendKvRead4Projection) -> usize {
    match projection {
        BackendKvRead4Projection::KeysOnly => 0,
        BackendKvRead4Projection::Parts(parts) => parts.len(),
    }
}

fn sqlite_read4_projection_sql(projection: &BackendKvRead4Projection) -> Vec<&'static str> {
    match projection {
        BackendKvRead4Projection::KeysOnly => Vec::new(),
        BackendKvRead4Projection::Parts(parts) => parts
            .iter()
            .map(|part| match part {
                BackendKvRead4ValuePart::Header => {
                    "substr(value, 26, CAST(substr(value, 6, 10) AS INTEGER))"
                }
                BackendKvRead4ValuePart::PayloadRef | BackendKvRead4ValuePart::Payload => {
                    "substr(value, 26 + CAST(substr(value, 6, 10) AS INTEGER), CAST(substr(value, 16, 10) AS INTEGER))"
                }
                BackendKvRead4ValuePart::FullValue => "value",
            })
            .collect(),
    }
}

fn sqlite_scan_keys(
    connection: &Connection,
    request: BackendKvScanRequest,
) -> Result<BackendKvKeyPage, LixError> {
    let start = scan_start_key(&request);
    let end = scan_end_key(&request.range);
    let limit = sqlite_fetch_limit(request.limit)?;
    let mut statement = connection
        .prepare_cached(
            "
            SELECT key FROM kv
            WHERE namespace = ?1
              AND (?2 IS NULL OR key > ?2)
              AND key >= ?3
              AND (?4 IS NULL OR key < ?4)
            ORDER BY key
            LIMIT ?5
            ",
        )
        .map_err(sqlite_error)?;
    let mut cursor = statement
        .query(params![
            request.namespace.as_str(),
            request.after.as_deref(),
            start.as_slice(),
            end.as_deref(),
            limit,
        ])
        .map_err(sqlite_error)?;
    let mut keys = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    while let Some(row) = cursor.next().map_err(sqlite_error)? {
        let key = row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?;
        if count < request.limit {
            resume_after_candidate = Some(key.clone());
            keys.push(&key);
        }
        count += 1;
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvKeyPage {
        keys: keys.finish(),
        resume_after,
    })
}

fn sqlite_scan_untracked_keys(
    connection: &Connection,
    request: BackendKvScanRequest,
) -> Result<BackendKvKeyPage, LixError> {
    let start = scan_start_key(&request);
    let end = scan_end_key(&request.range);
    let limit = sqlite_fetch_limit(request.limit)?;
    let mut statement = connection
        .prepare_cached(
            "
            SELECT key FROM kv_u
            WHERE (?1 IS NULL OR key > ?1)
              AND key >= ?2
              AND (?3 IS NULL OR key < ?3)
            ORDER BY key
            LIMIT ?4
            ",
        )
        .map_err(sqlite_error)?;
    let mut cursor = statement
        .query(params![
            request.after.as_deref(),
            start.as_slice(),
            end.as_deref(),
            limit
        ])
        .map_err(sqlite_error)?;
    let mut keys = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    while let Some(row) = cursor.next().map_err(sqlite_error)? {
        let key = row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?;
        if count < request.limit {
            resume_after_candidate = Some(key.clone());
            keys.push(&key);
        }
        count += 1;
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvKeyPage {
        keys: keys.finish(),
        resume_after,
    })
}

fn sqlite_scan_values(
    connection: &Connection,
    request: BackendKvScanRequest,
) -> Result<BackendKvValuePage, LixError> {
    let start = scan_start_key(&request);
    let end = scan_end_key(&request.range);
    let limit = sqlite_fetch_limit(request.limit)?;
    let mut statement = connection
        .prepare_cached(
            "
            SELECT key, value FROM kv
            WHERE namespace = ?1
              AND (?2 IS NULL OR key > ?2)
              AND key >= ?3
              AND (?4 IS NULL OR key < ?4)
            ORDER BY key
            LIMIT ?5
            ",
        )
        .map_err(sqlite_error)?;
    let mut cursor = statement
        .query(params![
            request.namespace.as_str(),
            request.after.as_deref(),
            start.as_slice(),
            end.as_deref(),
            limit,
        ])
        .map_err(sqlite_error)?;
    let mut values = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    while let Some(row) = cursor.next().map_err(sqlite_error)? {
        if count < request.limit {
            resume_after_candidate = Some(row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?);
            let value = row.get::<_, Vec<u8>>(1).map_err(sqlite_error)?;
            values.push(&value);
        }
        count += 1;
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvValuePage {
        values: values.finish(),
        resume_after,
    })
}

fn sqlite_scan_untracked_values(
    connection: &Connection,
    request: BackendKvScanRequest,
) -> Result<BackendKvValuePage, LixError> {
    let start = scan_start_key(&request);
    let end = scan_end_key(&request.range);
    let limit = sqlite_fetch_limit(request.limit)?;
    let mut statement = connection
        .prepare_cached(
            "
            SELECT key, value FROM kv_u
            WHERE (?1 IS NULL OR key > ?1)
              AND key >= ?2
              AND (?3 IS NULL OR key < ?3)
            ORDER BY key
            LIMIT ?4
            ",
        )
        .map_err(sqlite_error)?;
    let mut cursor = statement
        .query(params![
            request.after.as_deref(),
            start.as_slice(),
            end.as_deref(),
            limit
        ])
        .map_err(sqlite_error)?;
    let mut values = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    while let Some(row) = cursor.next().map_err(sqlite_error)? {
        if count < request.limit {
            resume_after_candidate = Some(row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?);
            let value = row.get::<_, Vec<u8>>(1).map_err(sqlite_error)?;
            values.push(&value);
        }
        count += 1;
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvValuePage {
        values: values.finish(),
        resume_after,
    })
}

fn sqlite_scan_entries(
    connection: &Connection,
    request: BackendKvScanRequest,
) -> Result<BackendKvEntryPage, LixError> {
    let start = scan_start_key(&request);
    let end = scan_end_key(&request.range);
    let limit = sqlite_fetch_limit(request.limit)?;
    let mut statement = connection
        .prepare_cached(
            "
            SELECT key, value FROM kv
            WHERE namespace = ?1
              AND (?2 IS NULL OR key > ?2)
              AND key >= ?3
              AND (?4 IS NULL OR key < ?4)
            ORDER BY key
            LIMIT ?5
            ",
        )
        .map_err(sqlite_error)?;
    let mut cursor = statement
        .query(params![
            request.namespace.as_str(),
            request.after.as_deref(),
            start.as_slice(),
            end.as_deref(),
            limit,
        ])
        .map_err(sqlite_error)?;
    let mut keys = BytePageBuilder::new();
    let mut values = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    while let Some(row) = cursor.next().map_err(sqlite_error)? {
        let key = row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?;
        if count < request.limit {
            let value = row.get::<_, Vec<u8>>(1).map_err(sqlite_error)?;
            resume_after_candidate = Some(key.clone());
            keys.push(&key);
            values.push(&value);
        }
        count += 1;
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvEntryPage {
        keys: keys.finish(),
        values: values.finish(),
        resume_after,
    })
}

fn sqlite_scan_untracked_entries(
    connection: &Connection,
    request: BackendKvScanRequest,
) -> Result<BackendKvEntryPage, LixError> {
    let start = scan_start_key(&request);
    let end = scan_end_key(&request.range);
    let limit = sqlite_fetch_limit(request.limit)?;
    let mut statement = connection
        .prepare_cached(
            "
            SELECT key, value FROM kv_u
            WHERE (?1 IS NULL OR key > ?1)
              AND key >= ?2
              AND (?3 IS NULL OR key < ?3)
            ORDER BY key
            LIMIT ?4
            ",
        )
        .map_err(sqlite_error)?;
    let mut cursor = statement
        .query(params![
            request.after.as_deref(),
            start.as_slice(),
            end.as_deref(),
            limit
        ])
        .map_err(sqlite_error)?;
    let mut keys = BytePageBuilder::new();
    let mut values = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    while let Some(row) = cursor.next().map_err(sqlite_error)? {
        let key = row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?;
        if count < request.limit {
            let value = row.get::<_, Vec<u8>>(1).map_err(sqlite_error)?;
            resume_after_candidate = Some(key.clone());
            keys.push(&key);
            values.push(&value);
        }
        count += 1;
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvEntryPage {
        keys: keys.finish(),
        values: values.finish(),
        resume_after,
    })
}

fn sqlite_fetch_limit(limit: usize) -> Result<i64, LixError> {
    if limit == usize::MAX {
        return Ok(i64::MAX);
    }
    let fetch_limit = limit.checked_add(1).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "storage scan limit overflow while checking for next page",
        )
    })?;
    i64::try_from(fetch_limit).map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "storage scan limit does not fit into sqlite i64",
        )
    })
}

fn scan_start_key(request: &BackendKvScanRequest) -> Vec<u8> {
    let range_start = match &request.range {
        BackendKvScanRange::Prefix(prefix) => prefix.as_slice(),
        BackendKvScanRange::Range { start, .. } => start.as_slice(),
    };
    match request.after.as_deref() {
        Some(after) if after > range_start => after.to_vec(),
        _ => range_start.to_vec(),
    }
}

fn scan_end_key(range: &BackendKvScanRange) -> Option<Vec<u8>> {
    match range {
        BackendKvScanRange::Prefix(prefix) => prefix_end(prefix),
        BackendKvScanRange::Range { end, .. } => Some(end.clone()),
    }
}

fn sqlite_delete_range(
    connection: &Connection,
    namespace: &str,
    range: &BackendKvScanRange,
) -> Result<(), LixError> {
    if namespace == UNTRACKED_NAMESPACE {
        return sqlite_delete_untracked_range(connection, range);
    }

    if matches!(range, BackendKvScanRange::Prefix(prefix) if prefix.is_empty())
        && sqlite_namespace_is_exclusive(connection, namespace)?
    {
        return connection
            .execute("DELETE FROM kv", [])
            .map(|_| ())
            .map_err(sqlite_error);
    }

    let start = match range {
        BackendKvScanRange::Prefix(prefix) => prefix.as_slice(),
        BackendKvScanRange::Range { start, .. } => start.as_slice(),
    };
    match scan_end_key(range) {
        Some(end) => connection
            .execute(
                "DELETE FROM kv WHERE namespace = ?1 AND key >= ?2 AND key < ?3",
                params![namespace, start, end],
            )
            .map(|_| ())
            .map_err(sqlite_error),
        None => connection
            .execute(
                "DELETE FROM kv WHERE namespace = ?1 AND key >= ?2",
                params![namespace, start],
            )
            .map(|_| ())
            .map_err(sqlite_error),
    }
}

fn sqlite_delete_untracked_range(
    connection: &Connection,
    range: &BackendKvScanRange,
) -> Result<(), LixError> {
    let start = match range {
        BackendKvScanRange::Prefix(prefix) => prefix.as_slice(),
        BackendKvScanRange::Range { start, .. } => start.as_slice(),
    };
    match scan_end_key(range) {
        Some(end) => connection
            .execute(
                "DELETE FROM kv_u WHERE key >= ?1 AND key < ?2",
                params![start, end],
            )
            .map(|_| ())
            .map_err(sqlite_error),
        None => {
            if start.is_empty() {
                connection
                    .execute("DELETE FROM kv_u", [])
                    .map(|_| ())
                    .map_err(sqlite_error)
            } else {
                connection
                    .execute("DELETE FROM kv_u WHERE key >= ?1", params![start])
                    .map(|_| ())
                    .map_err(sqlite_error)
            }
        }
    }
}

fn sqlite_namespace_is_exclusive(
    connection: &Connection,
    namespace: &str,
) -> Result<bool, LixError> {
    let first_namespace = sqlite_first_namespace(connection)?;
    let Some(first_namespace) = first_namespace else {
        return Ok(true);
    };
    if first_namespace != namespace {
        return Ok(false);
    }

    let last_namespace = sqlite_last_namespace(connection)?;
    Ok(last_namespace.as_deref() == Some(namespace))
}

fn sqlite_first_namespace(connection: &Connection) -> Result<Option<String>, LixError> {
    connection
        .query_row(
            "SELECT namespace FROM kv ORDER BY namespace ASC LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sqlite_error)
}

fn sqlite_last_namespace(connection: &Connection) -> Result<Option<String>, LixError> {
    connection
        .query_row(
            "SELECT namespace FROM kv ORDER BY namespace DESC LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sqlite_error)
}

fn delete_range_bytes(range: &BackendKvScanRange) -> usize {
    match range {
        BackendKvScanRange::Prefix(prefix) => prefix.len(),
        BackendKvScanRange::Range { start, end } => start.len() + end.len(),
    }
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

fn sqlite_error(error: rusqlite::Error) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sqlite bench backend: {error}"),
    )
}
