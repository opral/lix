use lix_engine::{Backend, LixError, Value};
use lix_engine::{
    CreateVersionOptions, CreateVersionReceipt, Engine, ExecuteResult, InitReceipt,
    MergeVersionOptions, MergeVersionPreview, MergeVersionPreviewOptions, MergeVersionReceipt,
    SessionContext, SwitchVersionOptions, SwitchVersionReceipt,
};

use super::expect_same::SimulationAssertions;
use super::mode::{SimulationMode, SimulationOptions};
use super::rebuild_tracked_state::RebuildTrackedStateSimulation;
use crate::support::kv_backend::{InMemoryKvBackend, KvMap};

/// Per-mode handle exposed to tests using `simulation_test!`.
#[derive(Clone)]
pub struct Simulation {
    mode: SimulationMode,
    #[allow(dead_code)]
    backend: InMemoryKvBackend,
    engine: Engine,
    receipt: InitReceipt,
    rebuild_tracked_state: RebuildTrackedStateSimulation,
    assertions: SimulationAssertions,
}

#[allow(dead_code)]
impl Simulation {
    pub(super) async fn from_bootstrap(
        mode: SimulationMode,
        options: SimulationOptions,
        snapshot: KvMap,
        receipt: InitReceipt,
        assertions: SimulationAssertions,
    ) -> Result<Self, LixError> {
        let backend = InMemoryKvBackend::from_snapshot(snapshot);
        let engine = Engine::new(Box::new(backend.clone())).await?;
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
        Engine::new(Box::new(InMemoryKvBackend::from_snapshot(
            self.backend.snapshot(),
        )))
        .await
    }

    /// Wraps a normal engine session with simulation hooks.
    pub fn wrap_session(&self, session: SessionContext, engine: &Engine) -> SimSession {
        SimSession {
            sim: self.clone(),
            engine: engine.clone(),
            session,
        }
    }

    /// Returns a fresh, empty backend for lifecycle tests.
    pub fn uninitialized_backend(&self) -> Box<dyn Backend + Send + Sync> {
        Box::new(InMemoryKvBackend::new())
    }

    /// Returns the initialized Lix id.
    pub fn lix_id(&self) -> &str {
        &self.receipt.lix_id
    }

    /// Returns the initial commit id.
    pub fn initial_commit_id(&self) -> &str {
        &self.receipt.initial_commit_id
    }

    /// Returns the initialized main version id.
    pub fn main_version_id(&self) -> &str {
        &self.receipt.main_version_id
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
}

#[allow(dead_code)]
impl SimSession {
    pub fn wrap_session(&self, session: SessionContext, engine: &Engine) -> SimSession {
        SimSession {
            sim: self.sim.clone(),
            engine: engine.clone(),
            session,
        }
    }

    pub async fn active_version_id(&self) -> Result<String, LixError> {
        self.session.active_version_id().await
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        match classify_statement(sql) {
            StatementKind::Read => {
                let active_version_id = self.session.active_version_id().await?;
                self.sim
                    .rebuild_tracked_state
                    .before_read(&self.engine, &active_version_id)
                    .await?;
                self.session.execute(sql, params).await
            }
            StatementKind::Write => {
                let result = self.session.execute(sql, params).await;
                if result.is_ok() {
                    self.sim.rebuild_tracked_state.after_successful_write();
                }
                result
            }
            StatementKind::Utility => self.session.execute(sql, params).await,
        }
    }

    pub async fn create_version(
        &self,
        options: CreateVersionOptions,
    ) -> Result<CreateVersionReceipt, LixError> {
        let result = self.session.create_version(options).await;
        if result.is_ok() {
            self.sim.rebuild_tracked_state.after_successful_write();
        }
        result
    }

    pub async fn merge_version(
        &self,
        options: MergeVersionOptions,
    ) -> Result<MergeVersionReceipt, LixError> {
        let result = self.session.merge_version(options).await;
        if result.is_ok() {
            self.sim.rebuild_tracked_state.after_successful_write();
        }
        result
    }

    pub async fn merge_version_preview(
        &self,
        options: MergeVersionPreviewOptions,
    ) -> Result<MergeVersionPreview, LixError> {
        self.session.merge_version_preview(options).await
    }

    pub async fn switch_version(
        &self,
        options: SwitchVersionOptions,
    ) -> Result<(SimSession, SwitchVersionReceipt), LixError> {
        let (session, receipt) = self.session.switch_version(options).await?;
        Ok((
            SimSession {
                sim: self.sim.clone(),
                engine: self.engine.clone(),
                session,
            },
            receipt,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementKind {
    Read,
    Write,
    Utility,
}

fn classify_statement(sql: &str) -> StatementKind {
    let keyword = sql
        .trim_start()
        .split(|ch: char| ch.is_whitespace() || ch == '(')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    match keyword.as_str() {
        "SELECT" | "WITH" => StatementKind::Read,
        "INSERT" | "UPDATE" | "DELETE" => StatementKind::Write,
        _ => StatementKind::Utility,
    }
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
            classify_statement("INSERT INTO t VALUES (1)"),
            StatementKind::Write
        );
        assert_eq!(
            classify_statement("UPDATE t SET a = 1"),
            StatementKind::Write
        );
        assert_eq!(classify_statement("DELETE FROM t"), StatementKind::Write);
        assert_eq!(
            classify_statement("EXPLAIN SELECT 1"),
            StatementKind::Utility
        );
    }
}
