use lix::storage::Memory;
use lix::{
    CreateBranchOptions, CreateBranchReceipt, CreateCheckpointReceipt, ExecuteResult,
    MergeBranchOptions, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    SessionTransaction, SwitchBranchOptions, SwitchBranchReceipt,
};
use lix::{LixError, Value};
use lix::{engine::Engine, init::InitReceipt, session::SessionContext};

use super::mode::{SimulationMode, SimulationOptions};

/// Per-mode handle exposed to tests using `simulation_test!`.
#[derive(Clone)]
#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
pub struct Simulation {
    storage: Memory,
    engine: Engine,
    receipt: InitReceipt,
}

#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
impl Simulation {
    pub(super) async fn from_bootstrap(
        _mode: SimulationMode,
        options: SimulationOptions,
        storage: Memory,
        receipt: InitReceipt,
    ) -> Result<Self, LixError> {
        let engine = Engine::new(storage.clone()).await?;
        if options.deterministic {
            super::macro_runtime::enable_deterministic_mode(&engine, &receipt).await?;
        }
        Ok(Self {
            storage,
            engine,
            receipt,
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
    pub fn wrap_session(&self, session: SessionContext, _engine: &Engine) -> SimSession {
        SimSession {
            sim: self.clone(),
            fs: SimFs::new(self.clone(), session.clone()),
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
}

/// Session wrapper that injects simulation behavior around normal execution.
#[derive(Clone)]
#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
pub struct SimSession {
    sim: Simulation,
    session: SessionContext,
    pub fs: SimFs,
}

#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
impl SimSession {
    pub fn wrap_session(&self, session: SessionContext, _engine: &Engine) -> Self {
        Self {
            sim: self.sim.clone(),
            fs: SimFs::new(self.sim.clone(), session.clone()),
            session,
        }
    }

    pub async fn active_branch_id(&self) -> Result<String, LixError> {
        self.session.active_branch_id().await
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        let result = self.session.execute(sql, params).await;
        result
    }

    pub async fn begin_transaction(&self) -> Result<SimTransaction, LixError> {
        let transaction = self.session.begin_transaction().await?;
        Ok(SimTransaction { transaction })
    }

    pub async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError> {
        self.session.create_branch(options).await
    }

    pub async fn create_checkpoint(&self) -> Result<CreateCheckpointReceipt, LixError> {
        self.session.create_checkpoint().await
    }

    pub async fn merge_branch(
        &self,
        options: MergeBranchOptions,
    ) -> Result<MergeBranchReceipt, LixError> {
        self.session.merge_branch(options).await
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
    ) -> Result<SwitchBranchReceipt, LixError> {
        self.session.switch_branch(options).await
    }
}

#[derive(Clone)]
#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
pub struct SimFs {
    sim: Simulation,
    session: SessionContext,
}

#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
impl SimFs {
    fn new(sim: Simulation, session: SessionContext) -> Self {
        Self { sim, session }
    }

    pub async fn write_file(&self, path: &str, data: Vec<u8>) -> Result<(), LixError> {
        let result = self
            .session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                 ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                &[Value::Text(path.to_string()), Value::Blob(data.into())],
            )
            .await
            .map(|_| ());
        result
    }

    pub async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, LixError> {
        let result = self
            .session
            .execute(
                "SELECT content FROM lix_file WHERE path = $1",
                &[Value::Text(path.to_string())],
            )
            .await?;
        Ok(result
            .rows()
            .first()
            .and_then(|row| row.get::<Vec<u8>>("content").ok()))
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
        result
    }

    pub async fn readdir(&self, path: &str) -> Result<Option<Vec<String>>, LixError> {
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
        result
    }
}

fn direct_child_name(parent: &str, child: &str) -> Option<String> {
    let child_prefix = if parent == "/" {
        "/".to_string()
    } else {
        format!("{parent}/")
    };
    let remainder = child.strip_prefix(&child_prefix)?;
    if remainder.is_empty() {
        return None;
    }
    if remainder.contains('/') {
        return None;
    }
    Some(remainder.to_string())
}

/// Transaction wrapper that injects simulation behavior around normal execution.
#[allow(
    dead_code,
    reason = "shared integration-test harness is compiled once per test target"
)]
pub struct SimTransaction {
    transaction: SessionTransaction,
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
        self.transaction.execute(sql, params).await
    }

    pub async fn commit(self) -> Result<(), LixError> {
        self.transaction.commit().await
    }

    pub async fn rollback(self) -> Result<(), LixError> {
        self.transaction.rollback().await
    }
}
