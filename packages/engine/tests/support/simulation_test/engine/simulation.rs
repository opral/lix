use lix_engine::storage::Memory;
use lix_engine::{
    CreateBranchOptions, CreateBranchReceipt, Engine, ExecuteResult, InitReceipt,
    MergeBranchOptions, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    SessionContext, SessionTransaction, SwitchBranchOptions, SwitchBranchReceipt,
};
use lix_engine::{LixError, Value};

use super::expect_same::SimulationAssertions;
use super::mode::{SimulationMode, SimulationOptions};
use super::rebuild_tracked_state::RebuildTrackedStateSimulation;

/// Per-mode handle exposed to tests using `simulation_test!`.
#[derive(Clone)]
#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
pub struct Simulation {
    mode: SimulationMode,
    storage: Memory,
    engine: Engine,
    receipt: InitReceipt,
    rebuild_tracked_state: RebuildTrackedStateSimulation,
    assertions: SimulationAssertions,
}

#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
impl Simulation {
    pub(super) async fn from_bootstrap(
        mode: SimulationMode,
        options: SimulationOptions,
        storage: Memory,
        receipt: InitReceipt,
        assertions: SimulationAssertions,
    ) -> Result<Self, LixError> {
        let engine = Engine::new(storage.clone()).await?;
        if options.deterministic {
            super::macro_runtime::enable_deterministic_mode(&engine, &receipt, mode).await?;
        }
        assertions.start_mode(mode);
        Ok(Self {
            mode,
            storage,
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

    /// Boots a fresh engine from the current storage snapshot.
    ///
    /// This is the simulation equivalent of closing the app and reopening the
    /// same repository. It lets tests distinguish persisted workspace state
    /// from in-memory session state.
    pub async fn reboot_engine_from_current_snapshot(&self) -> Result<Engine, LixError> {
        Engine::new(self.storage.clone()).await
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

    /// Returns a fresh, empty storage for lifecycle tests.
    #[expect(clippy::unused_self)]
    pub fn uninitialized_storage(&self) -> Memory {
        Memory::new()
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
#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
pub struct SimSession {
    sim: Simulation,
    engine: Engine,
    session: SessionContext,
    pub fs: SimFs,
}

#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
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
#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
pub struct SimFs {
    sim: Simulation,
    engine: Engine,
    session: SessionContext,
}

#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
impl SimFs {
    fn new(sim: Simulation, engine: Engine, session: SessionContext) -> Self {
        Self {
            sim,
            engine,
            session,
        }
    }

    pub async fn write_file(&self, path: &str, data: Vec<u8>) -> Result<(), LixError> {
        let result = self
            .session
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
                 ON CONFLICT (path) DO UPDATE SET data = excluded.data",
                &[Value::Text(path.to_string()), Value::Blob(data.into())],
            )
            .await
            .map(|_| ());
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
        let result = self
            .session
            .execute(
                "SELECT data FROM lix_file WHERE path = $1",
                &[Value::Text(path.to_string())],
            )
            .await?;
        Ok(result
            .rows()
            .first()
            .and_then(|row| row.get::<Vec<u8>>("data").ok()))
    }

    pub async fn mkdir(&self, path: &str) -> Result<(), LixError> {
        let result = self
            .session
            .execute(
                "INSERT INTO lix_directory (path) VALUES ($1) \
                 ON CONFLICT (path) DO NOTHING",
                &[Value::Text(path.to_string())],
            )
            .await
            .map(|_| ());
        if result.is_ok() {
            self.sim.rebuild_tracked_state.after_successful_write();
        }
        result
    }

    pub async fn readdir(&self, path: &str) -> Result<Option<Vec<String>>, LixError> {
        let active_branch_id = self.session.active_branch_id().await?;
        self.sim
            .rebuild_tracked_state
            .before_read(&self.engine, &active_branch_id)
            .await?;
        let result = self
            .session
            .execute(
                "SELECT path FROM lix_file WHERE path LIKE $1 \
                 UNION ALL \
                 SELECT path FROM lix_directory WHERE path LIKE $1 AND path != $2 \
                 ORDER BY path",
                &[
                    Value::Text(format!("{path}%")),
                    Value::Text(path.to_string()),
                ],
            )
            .await?;
        let mut entries = Vec::new();
        for row in result.rows() {
            let child_path = row.get::<String>("path")?;
            let Some(name) = direct_child_name(path, &child_path) else {
                continue;
            };
            entries.push(name);
        }
        if entries.is_empty() {
            Ok(None)
        } else {
            entries.sort();
            entries.dedup();
            Ok(Some(entries))
        }
    }

    pub async fn rm(&self, path: &str) -> Result<(), LixError> {
        let result = async {
            self.session
                .execute(
                    "DELETE FROM lix_file WHERE path = $1",
                    &[Value::Text(path.to_string())],
                )
                .await?;
            self.session
                .execute(
                    "DELETE FROM lix_directory WHERE path = $1",
                    &[Value::Text(path.to_string())],
                )
                .await?;
            Ok(())
        }
        .await;
        if result.is_ok() {
            self.sim.rebuild_tracked_state.after_successful_write();
        }
        result
    }
}

fn direct_child_name(parent: &str, child: &str) -> Option<String> {
    let remainder = child.strip_prefix(parent)?;
    if remainder.is_empty() {
        return None;
    }
    let trimmed = remainder.trim_end_matches('/');
    if trimmed.is_empty() || trimmed.contains('/') {
        return None;
    }
    Some(trimmed.to_string())
}

/// Transaction wrapper that injects simulation behavior around normal execution.
#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
pub struct SimTransaction {
    sim: Simulation,
    engine: Engine,
    session: SessionContext,
    transaction: SessionTransaction,
    saw_write: bool,
}

#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
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
