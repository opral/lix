use super::sqlite_backend::BenchSqliteBackend;
use async_trait::async_trait;
use lix_engine::{
    LixBackend, LixBackendTransaction, LixError, PreparedBatch, QueryResult, SqlDialect,
    TransactionBeginMode, Value,
};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct SqlTraceOperation {
    pub sequence: u64,
    pub kind: &'static str,
    pub sql: Option<String>,
    pub duration_ms: f64,
}

#[derive(Debug, Default)]
pub struct SqlTraceCollector {
    next_sequence: AtomicU64,
    operations: Mutex<Vec<SqlTraceOperation>>,
}

impl SqlTraceCollector {
    pub fn push(&self, kind: &'static str, sql: Option<&str>, duration_ms: f64) {
        let mut guard = self
            .operations
            .lock()
            .expect("sql trace collector mutex should not be poisoned");
        guard.push(SqlTraceOperation {
            sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
            kind,
            sql: sql.map(ToOwned::to_owned),
            duration_ms,
        });
    }

    pub fn snapshot(&self) -> Vec<SqlTraceOperation> {
        self.operations
            .lock()
            .expect("sql trace collector mutex should not be poisoned")
            .clone()
    }

    pub fn clear(&self) {
        self.operations
            .lock()
            .expect("sql trace collector mutex should not be poisoned")
            .clear();
        self.next_sequence.store(0, Ordering::Relaxed);
    }
}

pub fn file_backed_backend(
    path: &Path,
    trace_collector: Option<Arc<SqlTraceCollector>>,
) -> Result<Box<dyn LixBackend + Send + Sync>, LixError> {
    let backend = BenchSqliteBackend::file_backed(path)?;
    Ok(match trace_collector {
        Some(collector) => Box::new(TracingBenchBackend {
            inner: backend,
            collector,
        }),
        None => Box::new(backend),
    })
}

pub fn trace_flag_enabled(var_name: &str) -> bool {
    std::env::var(var_name)
        .map(|raw| {
            let normalized = raw.trim().to_ascii_lowercase();
            !normalized.is_empty() && normalized != "0" && normalized != "false"
        })
        .unwrap_or(false)
}

pub fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn summarize_sql(sql: &str, max_sql_chars: usize) -> String {
    if sql.len() <= max_sql_chars {
        return sql.to_string();
    }
    format!("{}...", &sql[..max_sql_chars])
}

#[derive(Debug)]
pub struct TraceSummary {
    pub kind: &'static str,
    pub count: usize,
    pub total_ms: f64,
    pub max_ms: f64,
    pub first_sequence: u64,
}

impl Default for TraceSummary {
    fn default() -> Self {
        Self {
            kind: "unknown",
            count: 0,
            total_ms: 0.0,
            max_ms: 0.0,
            first_sequence: u64::MAX,
        }
    }
}

struct TracingBenchBackend {
    inner: BenchSqliteBackend,
    collector: Arc<SqlTraceCollector>,
}

struct TracingBenchTransaction<'a> {
    inner: Box<dyn LixBackendTransaction + 'a>,
    collector: Arc<SqlTraceCollector>,
}

#[async_trait(?Send)]
impl LixBackend for TracingBenchBackend {
    fn dialect(&self) -> SqlDialect {
        self.inner.dialect()
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let started = std::time::Instant::now();
        let result = self.inner.execute(sql, params).await;
        self.collector.push(
            "backend_execute",
            Some(sql),
            started.elapsed().as_secs_f64() * 1000.0,
        );
        result
    }

    async fn begin_transaction(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        let started = std::time::Instant::now();
        let tx = self.inner.begin_transaction(mode).await?;
        self.collector.push(
            "begin_transaction",
            Some(match mode {
                TransactionBeginMode::Read => "read",
                TransactionBeginMode::Write => "write",
                TransactionBeginMode::Deferred => "deferred",
            }),
            started.elapsed().as_secs_f64() * 1000.0,
        );
        Ok(Box::new(TracingBenchTransaction {
            inner: tx,
            collector: Arc::clone(&self.collector),
        }))
    }

    async fn begin_savepoint(
        &self,
        name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        let started = std::time::Instant::now();
        let tx = self.inner.begin_savepoint(name).await?;
        self.collector.push(
            "begin_savepoint",
            Some(name),
            started.elapsed().as_secs_f64() * 1000.0,
        );
        Ok(Box::new(TracingBenchTransaction {
            inner: tx,
            collector: Arc::clone(&self.collector),
        }))
    }
}

#[async_trait(?Send)]
impl LixBackendTransaction for TracingBenchTransaction<'_> {
    fn dialect(&self) -> SqlDialect {
        self.inner.dialect()
    }

    fn mode(&self) -> TransactionBeginMode {
        self.inner.mode()
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let started = std::time::Instant::now();
        let result = self.inner.execute(sql, params).await;
        self.collector.push(
            "transaction_execute",
            Some(sql),
            started.elapsed().as_secs_f64() * 1000.0,
        );
        result
    }

    async fn execute_batch(&mut self, batch: &PreparedBatch) -> Result<QueryResult, LixError> {
        let started = std::time::Instant::now();
        let result = self.inner.execute_batch(batch).await;
        let collapsed =
            lix_engine::collapse_prepared_batch_for_dialect(batch, self.inner.dialect())?;
        self.collector.push(
            "transaction_execute_batch",
            Some(&collapsed.sql),
            started.elapsed().as_secs_f64() * 1000.0,
        );
        result
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        let Self { inner, collector } = *self;
        let started = std::time::Instant::now();
        let result = inner.commit().await;
        collector.push(
            "transaction_commit",
            None,
            started.elapsed().as_secs_f64() * 1000.0,
        );
        result
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        let Self { inner, collector } = *self;
        let started = std::time::Instant::now();
        let result = inner.rollback().await;
        collector.push(
            "transaction_rollback",
            None,
            started.elapsed().as_secs_f64() * 1000.0,
        );
        result
    }
}
