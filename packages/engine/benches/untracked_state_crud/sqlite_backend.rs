use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup, BackendKvGetRequest,
    BackendKvKeyPage, BackendKvKeySpan, BackendKvReadV3Order, BackendKvReadV3Page,
    BackendKvReadV3Presence, BackendKvReadV3Projection, BackendKvReadV3Request,
    BackendKvReadV3Source, BackendKvReadV3Strategy, BackendKvReadV3ValuePart,
    BackendKvScanPlanV3Page, BackendKvScanPlanV3Projection, BackendKvScanPlanV3Request,
    BackendKvScanPlanV3ValuePart, BackendKvScanRange, BackendKvScanRequest, BackendKvValueBatch,
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

    async fn scan_plan_v3(
        &mut self,
        request: BackendKvScanPlanV3Request,
    ) -> Result<BackendKvScanPlanV3Page, LixError> {
        let connection = self.lock_connection()?;
        sqlite_scan_plan_v3(&connection, request)
    }

    async fn read_v3(
        &mut self,
        request: BackendKvReadV3Request,
    ) -> Result<BackendKvReadV3Page, LixError> {
        let connection = self.lock_connection()?;
        sqlite_read_v3(&connection, request)
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

fn sqlite_scan_plan_v3(
    connection: &Connection,
    request: BackendKvScanPlanV3Request,
) -> Result<BackendKvScanPlanV3Page, LixError> {
    let spans = normalize_scan_plan_v3_spans(request.spans);
    if spans.is_empty() {
        return Ok(BackendKvScanPlanV3Page {
            keys: BytePageBuilder::new().finish(),
            values: match request.projection {
                BackendKvScanPlanV3Projection::KeysOnly => Vec::new(),
                BackendKvScanPlanV3Projection::ValueParts(parts) => parts
                    .iter()
                    .map(|_| BytePageBuilder::new().finish())
                    .collect(),
            },
            resume_after: None,
        });
    }

    let mut parameters = Vec::new();
    let from_clause = if request.namespace == UNTRACKED_NAMESPACE {
        "kv_u AS p"
    } else {
        parameters.push(SqlValue::Text(request.namespace.clone()));
        "kv AS p"
    };
    let namespace_filter = if request.namespace == UNTRACKED_NAMESPACE {
        ""
    } else {
        "p.namespace = ? AND"
    };

    parameters.push(nullable_blob_value(request.after.as_deref()));
    parameters.push(nullable_blob_value(request.after.as_deref()));

    let mut span_clauses = Vec::with_capacity(spans.len());
    for span in &spans {
        parameters.push(SqlValue::Blob(span.start.clone()));
        parameters.push(nullable_blob_value(non_empty_slice(&span.end)));
        parameters.push(nullable_blob_value(non_empty_slice(&span.end)));
        span_clauses.push("(p.key >= ? AND (? IS NULL OR p.key < ?))");
    }
    parameters.push(SqlValue::Integer(sqlite_fetch_limit(request.page_size)?));

    let (value_parts, projection_sql) = match request.projection {
        BackendKvScanPlanV3Projection::KeysOnly => (Vec::new(), String::new()),
        BackendKvScanPlanV3Projection::ValueParts(parts) => {
            let mut projection_sql = String::new();
            for part in &parts {
                projection_sql.push_str(", ");
                projection_sql.push_str(sqlite_scan_plan_v3_value_part_expr(*part));
            }
            (parts, projection_sql)
        }
    };

    let sql = format!(
        "
        SELECT p.key{projection_sql}
        FROM {from_clause}
        WHERE {namespace_filter}
          (? IS NULL OR p.key > ?)
          AND ({})
        ORDER BY p.key
        LIMIT ?
        ",
        span_clauses.join(" OR ")
    );

    let mut statement = connection.prepare(&sql).map_err(sqlite_error)?;
    let mut cursor = statement
        .query(params_from_iter(parameters))
        .map_err(sqlite_error)?;
    let mut keys = BytePageBuilder::new();
    let mut value_builders = value_parts
        .iter()
        .map(|_| BytePageBuilder::new())
        .collect::<Vec<_>>();
    let mut count = 0;
    let mut resume_after_candidate = None;
    while let Some(row) = cursor.next().map_err(sqlite_error)? {
        let key = row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?;
        if count < request.page_size {
            resume_after_candidate = Some(key.clone());
            keys.push(&key);
            for (part_index, values) in value_builders.iter_mut().enumerate() {
                let value = row
                    .get::<_, Vec<u8>>(part_index + 1)
                    .map_err(sqlite_error)?;
                values.push(&value);
            }
        }
        count += 1;
    }

    let resume_after = (count > request.page_size)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvScanPlanV3Page {
        keys: keys.finish(),
        values: value_builders
            .into_iter()
            .map(BytePageBuilder::finish)
            .collect(),
        resume_after,
    })
}

const SQLITE_READ3_DENSE_SCAN_THRESHOLD: usize = 512;

fn sqlite_read_v3(
    connection: &Connection,
    request: BackendKvReadV3Request,
) -> Result<BackendKvReadV3Page, LixError> {
    match request.source {
        BackendKvReadV3Source::Spans { spans, after } => {
            let page = sqlite_scan_plan_v3(
                connection,
                BackendKvScanPlanV3Request {
                    namespace: request.namespace,
                    spans,
                    after,
                    page_size: request.page_size.unwrap_or(usize::MAX),
                    projection: read_v3_scan_plan_v3_projection(request.projection),
                },
            )?;
            Ok(BackendKvReadV3Page {
                keys: page.keys,
                presence: BackendKvReadV3Presence::All,
                values: page.values,
                request_indexes: None,
                resume_after: page.resume_after,
            })
        }
        BackendKvReadV3Source::Keys { keys } => sqlite_read_v3_keys(
            connection,
            request.namespace,
            keys,
            request.projection,
            request.order,
        ),
        BackendKvReadV3Source::KeysOrSpans { keys, spans } => {
            let strategy = effective_read_v3_strategy(request.strategy);
            let use_scan = match strategy {
                BackendKvReadV3Strategy::Scan => !spans.is_empty(),
                BackendKvReadV3Strategy::Points => false,
                BackendKvReadV3Strategy::Auto => {
                    request.order == BackendKvReadV3Order::RequestOrder
                        && keys.len() >= SQLITE_READ3_DENSE_SCAN_THRESHOLD
                        && !spans.is_empty()
                }
            };
            if use_scan {
                sqlite_read_v3_dense_scan(
                    connection,
                    request.namespace,
                    keys,
                    spans,
                    request.projection,
                )
            } else {
                sqlite_read_v3_keys(
                    connection,
                    request.namespace,
                    keys,
                    request.projection,
                    request.order,
                )
            }
        }
    }
}

fn effective_read_v3_strategy(strategy: BackendKvReadV3Strategy) -> BackendKvReadV3Strategy {
    if strategy != BackendKvReadV3Strategy::Auto {
        return strategy;
    }
    match std::env::var("LIX_READ_V3_STRATEGY").as_deref() {
        Ok("points") => BackendKvReadV3Strategy::Points,
        Ok("scan") => BackendKvReadV3Strategy::Scan,
        _ => BackendKvReadV3Strategy::Auto,
    }
}

fn sqlite_read_v3_keys(
    connection: &Connection,
    namespace: String,
    keys: Vec<Vec<u8>>,
    projection: BackendKvReadV3Projection,
    order: BackendKvReadV3Order,
) -> Result<BackendKvReadV3Page, LixError> {
    if keys.is_empty() {
        return Ok(empty_read_v3_page(projection, 0));
    }

    let key_count = keys.len();
    let untracked = namespace == UNTRACKED_NAMESPACE;
    let key_placeholders = std::iter::repeat_n("?", key_count)
        .collect::<Vec<_>>()
        .join(", ");
    let from_clause = if untracked { "kv_u AS p" } else { "kv AS p" };
    let namespace_filter = if untracked { "" } else { "p.namespace = ? AND" };
    let mut parameters = Vec::with_capacity(usize::from(!untracked) + key_count);
    if !untracked {
        parameters.push(SqlValue::Text(namespace));
    }
    parameters.extend(keys.iter().cloned().map(SqlValue::Blob));

    let (parts, projection_sql) = match projection {
        BackendKvReadV3Projection::KeysOnly => (Vec::new(), String::new()),
        BackendKvReadV3Projection::ValueParts(parts) => {
            let mut projection_sql = String::new();
            for part in &parts {
                projection_sql.push_str(", ");
                projection_sql.push_str(sqlite_read_v3_value_part_expr(*part));
            }
            (parts, projection_sql)
        }
    };
    let sql = format!(
        "
        SELECT p.key{projection_sql}
        FROM {from_clause}
        WHERE {namespace_filter} p.key IN ({key_placeholders})
        "
    );
    let mut statement = connection.prepare(&sql).map_err(sqlite_error)?;
    let mut cursor = statement
        .query(params_from_iter(parameters))
        .map_err(sqlite_error)?;
    let mut values_by_key = HashMap::with_capacity(key_count);
    while let Some(row) = cursor.next().map_err(sqlite_error)? {
        let key = row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?;
        let mut values = Vec::with_capacity(parts.len());
        for part_index in 0..parts.len() {
            values.push(
                row.get::<_, Vec<u8>>(part_index + 1)
                    .map_err(sqlite_error)?,
            );
        }
        values_by_key.insert(key, values);
    }

    assemble_read_v3_from_key_map(keys, parts.len(), order, |key| {
        values_by_key.get(key).map(Vec::as_slice)
    })
}

fn sqlite_read_v3_dense_scan(
    connection: &Connection,
    namespace: String,
    keys: Vec<Vec<u8>>,
    spans: Vec<BackendKvKeySpan>,
    projection: BackendKvReadV3Projection,
) -> Result<BackendKvReadV3Page, LixError> {
    let part_count = match &projection {
        BackendKvReadV3Projection::KeysOnly => 0,
        BackendKvReadV3Projection::ValueParts(parts) => parts.len(),
    };
    let page = sqlite_scan_plan_v3(
        connection,
        BackendKvScanPlanV3Request {
            namespace,
            spans,
            after: None,
            page_size: usize::MAX,
            projection: read_v3_scan_plan_v3_projection(projection),
        },
    )?;
    let mut values_by_key = HashMap::with_capacity(page.keys.len());
    for (index, key) in page.keys.iter().enumerate() {
        let mut values = Vec::with_capacity(part_count);
        for values_page in &page.values {
            values.push(
                values_page
                    .get(index)
                    .ok_or_else(|| LixError::unknown("sqlite read_v3 dense scan value missing"))?
                    .to_vec(),
            );
        }
        values_by_key.insert(key.to_vec(), values);
    }
    assemble_read_v3_from_key_map(
        keys,
        part_count,
        BackendKvReadV3Order::RequestOrder,
        |key| values_by_key.get(key).map(Vec::as_slice),
    )
}

fn assemble_read_v3_from_key_map<'a>(
    keys: Vec<Vec<u8>>,
    part_count: usize,
    order: BackendKvReadV3Order,
    mut lookup: impl FnMut(&[u8]) -> Option<&'a [Vec<u8>]>,
) -> Result<BackendKvReadV3Page, LixError> {
    let mut key_builder = BytePageBuilder::new();
    let mut present = Vec::new();
    let mut value_builders = (0..part_count)
        .map(|_| BytePageBuilder::new())
        .collect::<Vec<_>>();
    let mut request_indexes = match order {
        BackendKvReadV3Order::RequestOrder => None,
        BackendKvReadV3Order::KeyOrder => Some(Vec::new()),
    };

    for (index, key) in keys.into_iter().enumerate() {
        let values = lookup(&key);
        match (order, values) {
            (BackendKvReadV3Order::RequestOrder, Some(values)) => {
                key_builder.push(&key);
                present.push(true);
                for (value, builder) in values.iter().zip(value_builders.iter_mut()) {
                    builder.push(value);
                }
            }
            (BackendKvReadV3Order::RequestOrder, None) => {
                key_builder.push(&key);
                present.push(false);
                for builder in &mut value_builders {
                    builder.push([]);
                }
            }
            (BackendKvReadV3Order::KeyOrder, Some(values)) => {
                key_builder.push(&key);
                present.push(true);
                request_indexes
                    .as_mut()
                    .expect("request indexes exist")
                    .push(
                        u32::try_from(index).map_err(|_| {
                            LixError::unknown("sqlite read_v3 request index overflow")
                        })?,
                    );
                for (value, builder) in values.iter().zip(value_builders.iter_mut()) {
                    builder.push(value);
                }
            }
            (BackendKvReadV3Order::KeyOrder, None) => {}
        }
    }
    Ok(BackendKvReadV3Page {
        keys: key_builder.finish(),
        presence: BackendKvReadV3Presence::bitmap(present),
        values: value_builders
            .into_iter()
            .map(BytePageBuilder::finish)
            .collect(),
        request_indexes,
        resume_after: None,
    })
}

fn empty_read_v3_page(
    projection: BackendKvReadV3Projection,
    key_count: usize,
) -> BackendKvReadV3Page {
    let value_count = match projection {
        BackendKvReadV3Projection::KeysOnly => 0,
        BackendKvReadV3Projection::ValueParts(parts) => parts.len(),
    };
    BackendKvReadV3Page {
        keys: BytePageBuilder::new().finish(),
        presence: BackendKvReadV3Presence::Bitmap(Vec::with_capacity(key_count)),
        values: (0..value_count)
            .map(|_| BytePageBuilder::new().finish())
            .collect(),
        request_indexes: None,
        resume_after: None,
    }
}

fn read_v3_scan_plan_v3_projection(
    projection: BackendKvReadV3Projection,
) -> BackendKvScanPlanV3Projection {
    match projection {
        BackendKvReadV3Projection::KeysOnly => BackendKvScanPlanV3Projection::KeysOnly,
        BackendKvReadV3Projection::ValueParts(parts) => BackendKvScanPlanV3Projection::ValueParts(
            parts
                .into_iter()
                .map(BackendKvScanPlanV3ValuePart::from)
                .collect(),
        ),
    }
}

fn sqlite_scan_plan_v3_value_part_expr(part: BackendKvScanPlanV3ValuePart) -> &'static str {
    match part {
        BackendKvScanPlanV3ValuePart::Header => {
            "substr(p.value, 26, CAST(substr(p.value, 6, 10) AS INTEGER))"
        }
        BackendKvScanPlanV3ValuePart::Payload => {
            "substr(p.value, 26 + CAST(substr(p.value, 6, 10) AS INTEGER), CAST(substr(p.value, 16, 10) AS INTEGER))"
        }
        BackendKvScanPlanV3ValuePart::FullValue => "p.value",
    }
}

fn sqlite_read_v3_value_part_expr(part: BackendKvReadV3ValuePart) -> &'static str {
    sqlite_scan_plan_v3_value_part_expr(part.into())
}

fn nullable_blob_value(value: Option<&[u8]>) -> SqlValue {
    value.map_or(SqlValue::Null, |value| SqlValue::Blob(value.to_vec()))
}

fn non_empty_slice(value: &[u8]) -> Option<&[u8]> {
    (!value.is_empty()).then_some(value)
}

fn normalize_scan_plan_v3_spans(mut spans: Vec<BackendKvKeySpan>) -> Vec<BackendKvKeySpan> {
    spans.retain(|span| span.end.is_empty() || span.start < span.end);
    spans.sort_by(|left, right| {
        left.start.cmp(&right.start).then_with(|| {
            scan_plan_v3_span_end_for_order(left).cmp(scan_plan_v3_span_end_for_order(right))
        })
    });

    let mut normalized: Vec<BackendKvKeySpan> = Vec::new();
    for span in spans {
        let Some(last) = normalized.last_mut() else {
            normalized.push(span);
            continue;
        };
        if last.end.is_empty() || last.end >= span.start {
            if last.end.is_empty() || span.end.is_empty() {
                last.end.clear();
            } else if span.end > last.end {
                last.end = span.end;
            }
        } else {
            normalized.push(span);
        }
    }
    normalized
}

fn scan_plan_v3_span_end_for_order(span: &BackendKvKeySpan) -> &[u8] {
    if span.end.is_empty() {
        &[u8::MAX]
    } else {
        &span.end
    }
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
