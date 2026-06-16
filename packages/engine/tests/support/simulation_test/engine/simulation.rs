#![expect(dead_code)]

use lix_engine::backend::InMemoryBackend;
use lix_engine::{
    CreateBranchOptions, CreateBranchReceipt, Engine, ExecuteResult, FsDirEntry, FsMkdirOptions,
    FsRmOptions, FsWriteOptions, InitReceipt, MergeBranchOptions, MergeBranchPreview,
    MergeBranchPreviewOptions, MergeBranchReceipt, SessionContext, SessionTransaction,
    SwitchBranchOptions, SwitchBranchReceipt,
};
use lix_engine::{LixError, Value};

use super::expect_same::SimulationAssertions;
use super::mode::{SimulationMode, SimulationOptions};
use super::rebuild_tracked_state::RebuildTrackedStateSimulation;

/// Per-mode handle exposed to tests using `simulation_test!`.
#[derive(Clone)]
pub struct Simulation {
    mode: SimulationMode,
    backend: InMemoryBackend,
    engine: Engine,
    receipt: InitReceipt,
    rebuild_tracked_state: RebuildTrackedStateSimulation,
    assertions: SimulationAssertions,
}

impl Simulation {
    pub(super) async fn from_bootstrap(
        mode: SimulationMode,
        options: SimulationOptions,
        backend: InMemoryBackend,
        receipt: InitReceipt,
        assertions: SimulationAssertions,
    ) -> Result<Self, LixError> {
        let engine = Engine::new(backend.clone()).await?;
        if options.deterministic {
            super::macro_runtime::enable_deterministic_mode(&engine, &receipt, mode).await?;
        }
        assertions.start_mode(mode);
        Ok(Self {
            mode,
            backend,
            engine,
            receipt,
            rebuild_tracked_state: RebuildTrackedStateSimulation::new(mode),
            assertions,
        })
    }

    /// Returns the normal engine runtime for this simulation run.
    pub async fn boot_engine(&self) -> Engine {
        self.engine.clone()
    }

    /// Boots a fresh engine from the current backend snapshot.
    ///
    /// This is the simulation equivalent of closing the app and reopening the
    /// same repository. It lets tests distinguish persisted workspace state
    /// from in-memory session state.
    pub async fn reboot_engine_from_current_snapshot(&self) -> Result<Engine, LixError> {
        Engine::new(self.backend.clone()).await
    }

    /// Wraps a normal engine session with simulation hooks.
    pub fn wrap_session(&self, session: SessionContext, engine: &Engine) -> SimSession {
        SimSession {
            sim: self.clone(),
            engine: engine.clone(),
            fs: SimFs::new(self.clone(), engine.clone(), session.clone()),
            session,
        }
    }

    /// Returns a fresh, empty backend for lifecycle tests.
    #[expect(clippy::unused_self)]
    pub fn uninitialized_backend(&self) -> InMemoryBackend {
        InMemoryBackend::new()
    }

    /// Returns the initialized Lix id.
    pub fn lix_id(&self) -> &str {
        &self.receipt.lix_id
    }

    /// Returns the initial commit id.
    pub fn initial_commit_id(&self) -> &str {
        &self.receipt.initial_commit_id
    }

    /// Returns the initialized main branch id.
    pub fn main_branch_id(&self) -> &str {
        &self.receipt.main_branch_id
    }

    pub(crate) fn finish(&self) {
        self.assertions.finish_mode(self.mode);
    }
}

/// Session wrapper that injects simulation behavior around normal execution.
pub struct SimSession {
    sim: Simulation,
    engine: Engine,
    session: SessionContext,
    pub fs: SimFs,
}

impl SimSession {
    pub fn wrap_session(&self, session: SessionContext, engine: &Engine) -> Self {
        Self {
            sim: self.sim.clone(),
            engine: engine.clone(),
            fs: SimFs::new(self.sim.clone(), engine.clone(), session.clone()),
            session,
        }
    }

    pub async fn active_branch_id(&self) -> Result<String, LixError> {
        self.session.active_branch_id().await
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        let statement_kind = classify_statement(sql);
        if statement_kind == StatementKind::Read {
            let active_branch_id = self.session.active_branch_id().await?;
            self.sim
                .rebuild_tracked_state
                .before_read(&self.engine, &active_branch_id)
                .await?;
        }

        let result = self.session.execute(sql, params).await;
        if let Ok(result) = &result {
            if statement_kind == StatementKind::Write || execute_result_looks_like_write(result) {
                self.sim.rebuild_tracked_state.after_successful_write();
            }
        }
        result
    }

    pub async fn begin_transaction(&self) -> Result<SimTransaction, LixError> {
        let active_branch_id = self.session.active_branch_id().await?;
        self.sim
            .rebuild_tracked_state
            .before_read(&self.engine, &active_branch_id)
            .await?;
        let transaction = self.session.begin_transaction().await?;
        Ok(SimTransaction {
            sim: self.sim.clone(),
            engine: self.engine.clone(),
            session: self.session.clone(),
            transaction,
            saw_write: false,
        })
    }

    pub async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError> {
        let result = self.session.create_branch(options).await;
        if result.is_ok() {
            self.sim.rebuild_tracked_state.after_successful_write();
        }
        result
    }

    pub async fn merge_branch(
        &self,
        options: MergeBranchOptions,
    ) -> Result<MergeBranchReceipt, LixError> {
        let result = self.session.merge_branch(options).await;
        if result.is_ok() {
            self.sim.rebuild_tracked_state.after_successful_write();
        }
        result
    }

    pub async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> Result<MergeBranchPreview, LixError> {
        self.session.merge_branch_preview(options).await
    }

    pub async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<(Self, SwitchBranchReceipt), LixError> {
        let (session, receipt) = self.session.switch_branch(options).await?;
        Ok((
            Self {
                sim: self.sim.clone(),
                engine: self.engine.clone(),
                fs: SimFs::new(self.sim.clone(), self.engine.clone(), session.clone()),
                session,
            },
            receipt,
        ))
    }
}

#[derive(Clone)]
pub struct SimFs {
    sim: Simulation,
    engine: Engine,
    session: SessionContext,
}

impl SimFs {
    fn new(sim: Simulation, engine: Engine, session: SessionContext) -> Self {
        Self {
            sim,
            engine,
            session,
        }
    }

    pub async fn write_file(
        &self,
        path: &str,
        data: Vec<u8>,
        options: FsWriteOptions,
    ) -> Result<(), LixError> {
        let result = self.session.fs().write_file(path, data, options).await;
        if result.is_ok() {
            self.sim.rebuild_tracked_state.after_successful_write();
        }
        result
    }

    pub async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, LixError> {
        let active_branch_id = self.session.active_branch_id().await?;
        self.sim
            .rebuild_tracked_state
            .before_read(&self.engine, &active_branch_id)
            .await?;
        self.session.fs().read_file(path).await
    }

    pub async fn mkdir(&self, path: &str, options: FsMkdirOptions) -> Result<(), LixError> {
        let result = self.session.fs().mkdir(path, options).await;
        if result.is_ok() {
            self.sim.rebuild_tracked_state.after_successful_write();
        }
        result
    }

    pub async fn readdir(&self, path: &str) -> Result<Option<Vec<FsDirEntry>>, LixError> {
        let active_branch_id = self.session.active_branch_id().await?;
        self.sim
            .rebuild_tracked_state
            .before_read(&self.engine, &active_branch_id)
            .await?;
        self.session.fs().readdir(path).await
    }

    pub async fn rm(&self, path: &str, options: FsRmOptions) -> Result<(), LixError> {
        let result = self.session.fs().rm(path, options).await;
        if result.is_ok() {
            self.sim.rebuild_tracked_state.after_successful_write();
        }
        result
    }
}

/// Transaction wrapper that injects simulation behavior around normal execution.
pub struct SimTransaction {
    sim: Simulation,
    engine: Engine,
    session: SessionContext,
    transaction: SessionTransaction,
    saw_write: bool,
}

impl SimTransaction {
    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        let statement_kind = classify_statement(sql);
        match statement_kind {
            StatementKind::Read => {
                let active_branch_id = self.transaction.active_branch_id()?.to_string();
                self.sim
                    .rebuild_tracked_state
                    .before_read(&self.engine, &active_branch_id)
                    .await?;
            }
            StatementKind::Write | StatementKind::Utility => {}
        }
        let result = self.transaction.execute(sql, params).await;
        if let Ok(result) = &result {
            if statement_kind == StatementKind::Write || execute_result_looks_like_write(result) {
                self.saw_write = true;
            }
        }
        result
    }

    pub async fn commit(self) -> Result<(), LixError> {
        let result = self.transaction.commit().await;
        if result.is_ok() && self.saw_write {
            self.sim.rebuild_tracked_state.after_successful_write();
        }
        result
    }

    pub async fn rollback(self) -> Result<(), LixError> {
        self.transaction.rollback().await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementKind {
    Read,
    Write,
    Utility,
}

fn classify_statement(sql: &str) -> StatementKind {
    let sql = skip_leading_sql_trivia(sql);
    if let Some(inner) = sql.strip_prefix('(') {
        return classify_statement(inner);
    }

    let (keyword, _rest) = first_keyword_and_rest(sql);
    match keyword.as_str() {
        "SELECT" | "WITH" | "VALUES" | "FROM" | "TABLE" | "EXPLAIN" => StatementKind::Read,
        "INSERT" | "UPDATE" | "DELETE" => StatementKind::Write,
        _ => StatementKind::Utility,
    }
}

fn first_keyword_and_rest(sql: &str) -> (String, &str) {
    let sql = skip_leading_sql_trivia(sql);
    let end_index = sql
        .char_indices()
        .find_map(|(index, ch)| {
            if !(ch.is_ascii_alphanumeric() || ch == '_') {
                Some(index)
            } else {
                None
            }
        })
        .unwrap_or(sql.len());
    (sql[..end_index].to_ascii_uppercase(), &sql[end_index..])
}

fn skip_leading_sql_trivia(mut sql: &str) -> &str {
    loop {
        let trimmed = sql.trim_start();
        if trimmed.len() != sql.len() {
            sql = trimmed;
            continue;
        }

        if let Some(comment_body) = sql.strip_prefix("--") {
            let Some(newline_index) = comment_body.find('\n') else {
                return "";
            };
            sql = &comment_body[newline_index + 1..];
            continue;
        }

        if let Some(comment_body) = sql.strip_prefix("/*") {
            let Some(end_index) = end_of_nested_block_comment(comment_body) else {
                return "";
            };
            sql = &comment_body[end_index..];
            continue;
        }

        return sql;
    }
}

fn end_of_nested_block_comment(comment_body: &str) -> Option<usize> {
    let mut depth = 1usize;
    let mut index = 0usize;
    while index < comment_body.len() {
        let rest = &comment_body[index..];
        if rest.starts_with("/*") {
            depth += 1;
            index += 2;
            continue;
        }
        if rest.starts_with("*/") {
            depth -= 1;
            index += 2;
            if depth == 0 {
                return Some(index);
            }
            continue;
        }
        index += rest.chars().next().map(char::len_utf8).unwrap_or_default();
    }
    None
}

fn execute_result_looks_like_write(result: &ExecuteResult) -> bool {
    result.columns().is_empty() && result.rows().is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_statement_splits_reads_writes_and_utility() {
        assert_eq!(classify_statement("SELECT 1"), StatementKind::Read);
        assert_eq!(
            classify_statement("  WITH x AS (...) SELECT 1"),
            StatementKind::Read
        );
        assert_eq!(
            classify_statement("-- leading comment\nSELECT 1"),
            StatementKind::Read
        );
        assert_eq!(
            classify_statement("/* leading block */ INSERT INTO t VALUES (1)"),
            StatementKind::Write
        );
        assert_eq!(
            classify_statement("/* outer /* inner */ outer */ SELECT 1"),
            StatementKind::Read
        );
        assert_eq!(
            classify_statement("INSERT INTO t VALUES (1)"),
            StatementKind::Write
        );
        assert_eq!(
            classify_statement("UPDATE t SET a = 1"),
            StatementKind::Write
        );
        assert_eq!(classify_statement("DELETE FROM t"), StatementKind::Write);
        assert_eq!(classify_statement("EXPLAIN SELECT 1"), StatementKind::Read);
        assert_eq!(
            classify_statement("EXPLAIN ANALYZE INSERT INTO t VALUES (1)"),
            StatementKind::Read
        );
        assert_eq!(
            classify_statement("EXPLAIN FORMAT INDENT SELECT 1"),
            StatementKind::Read
        );
        assert_eq!(
            classify_statement("EXPLAIN FORMAT 'INDENT' SELECT 1"),
            StatementKind::Read
        );
        assert_eq!(
            classify_statement("EXPLAIN FORMAT \"INDENT\" SELECT 1"),
            StatementKind::Read
        );
        assert_eq!(
            classify_statement("SELECT/* comment */ 1"),
            StatementKind::Read
        );
        assert_eq!(
            classify_statement("EXPLAIN/* comment */ SELECT 1"),
            StatementKind::Read
        );
        assert_eq!(
            classify_statement("EXPLAIN (SELECT 1)"),
            StatementKind::Read
        );
        assert_eq!(classify_statement("VALUES (1), (2)"), StatementKind::Read);
        assert_eq!(
            classify_statement("FROM lix_file SELECT id"),
            StatementKind::Read
        );
        assert_eq!(classify_statement("(SELECT 1)"), StatementKind::Read);
        assert_eq!(classify_statement("((SELECT 1))"), StatementKind::Read);
    }
}
