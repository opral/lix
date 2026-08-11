use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use datafusion::common::{DataFusionError, Result};
use datafusion::prelude::SessionContext;

use crate::common::LixTimestamp;
use crate::functions::FunctionProviderHandle;

/// Per-session storage for the per-statement facts the execution UDFs report.
///
/// The five execution functions (`lix_active_account_id`,
/// `lix_active_branch_id`, `lix_active_branch_commit_id`, `uuidv7`,
/// `CURRENT_TIMESTAMP`) used to be registered and deregistered on every statement so
/// each one could capture that statement's values in its own fields. They are
/// now registered once, when the session is created, and read this slot at
/// invocation time instead.
///
/// The slot is what makes that safe: a pooled session never carries a value from
/// an earlier statement, because [`Self::bind`] overwrites every field before
/// planning starts and the UDFs never hold a copy of their own.
#[derive(Default)]
pub(crate) struct ExecutionSlots {
    values: Mutex<ExecutionSlotValues>,
}

#[derive(Default)]
struct ExecutionSlotValues {
    active_account_id: Option<String>,
    active_branch_id: Option<String>,
    active_branch_commit_id: Option<String>,
    functions: Option<FunctionProviderHandle>,
    current_timestamp: Option<LixTimestamp>,
}

impl std::fmt::Debug for ExecutionSlots {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionSlots")
            .finish_non_exhaustive()
    }
}

impl ExecutionSlots {
    /// Installs the current statement's execution facts.
    ///
    /// Every field is written unconditionally: leaving a field untouched would
    /// be exactly the staleness this type exists to prevent. Existing
    /// allocations are reused when the text is unchanged, which is the steady
    /// state for a session that keeps serving the same branch and account.
    pub(crate) fn bind(
        &self,
        functions: FunctionProviderHandle,
        active_account_id: &str,
        active_branch_id: Option<&str>,
        active_branch_commit_id: Option<&str>,
    ) {
        let mut values = self.lock();
        assign(&mut values.active_account_id, Some(active_account_id));
        assign(&mut values.active_branch_id, active_branch_id);
        assign(&mut values.active_branch_commit_id, active_branch_commit_id);
        values.functions = Some(functions);
        values.current_timestamp = None;
    }

    pub(crate) fn active_account_id(&self) -> Option<String> {
        self.lock().active_account_id.clone()
    }

    pub(crate) fn active_branch_id(&self) -> Option<String> {
        self.lock().active_branch_id.clone()
    }

    pub(crate) fn active_branch_commit_id(&self) -> Option<String> {
        self.lock().active_branch_commit_id.clone()
    }

    /// The statement's function provider.
    ///
    /// An unbound slot is an engine bug rather than a user error, so it fails
    /// the statement instead of silently falling back to the system provider —
    /// a deterministic-uuid test harness must never be replaced by wall-clock
    /// randomness without saying so.
    pub(crate) fn functions(&self) -> Result<FunctionProviderHandle> {
        self.lock().functions.clone().ok_or_else(|| {
            DataFusionError::Internal(
                "Lix SQL execution functions were invoked on an unbound session".to_string(),
            )
        })
    }

    /// PostgreSQL `CURRENT_TIMESTAMP`: one value fixed for this statement's
    /// implicit transaction. The provider is invoked lazily so statements that
    /// do not use a timestamp do not consume deterministic sequence state.
    pub(crate) fn current_timestamp(&self) -> Result<LixTimestamp> {
        let mut values = self.lock();
        if let Some(timestamp) = values.current_timestamp {
            return Ok(timestamp);
        }
        let functions = values.functions.clone().ok_or_else(|| {
            DataFusionError::Internal(
                "Lix SQL execution functions were invoked on an unbound session".to_string(),
            )
        })?;
        let timestamp = functions.call_timestamp();
        values.current_timestamp = Some(timestamp);
        Ok(timestamp)
    }

    fn lock(&self) -> MutexGuard<'_, ExecutionSlotValues> {
        self.values.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn assign(slot: &mut Option<String>, value: Option<&str>) {
    match (slot.as_mut(), value) {
        (Some(existing), Some(value)) => {
            if existing != value {
                existing.clear();
                existing.push_str(value);
            }
        }
        (_, Some(value)) => *slot = Some(value.to_string()),
        (_, None) => *slot = None,
    }
}

/// Recovers the slots a Lix SQL session was created with.
///
/// The slots travel in the session's `SessionConfig` extensions so that every
/// holder of a `SessionContext` — read sessions, write sessions and transaction
/// sessions alike — reaches the same instance the session's UDFs were
/// registered against.
pub(crate) fn execution_slots(session: &SessionContext) -> Arc<ExecutionSlots> {
    session
        .state_ref()
        .read()
        .config()
        .get_extension::<ExecutionSlots>()
        .expect("Lix SQL sessions are created with execution-function slots")
}
