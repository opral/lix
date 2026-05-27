use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use lix_engine::Engine;
use lix_engine::LixError;

use super::mode::SimulationMode;

/// Returns whether a simulation mode should shuffle deterministic timestamps.
///
/// Rebuild mode intentionally shuffles timestamps so tests do not encode
/// assumptions that tracked-state rebuild order and write-time order match.
pub(crate) fn deterministic_timestamp_shuffle_for(mode: SimulationMode) -> bool {
    matches!(mode, SimulationMode::TrackedStateRebuild)
}

/// Mode-specific read/write hook for tracked-state rebuild simulation.
#[derive(Clone)]
pub(crate) struct RebuildTrackedStateSimulation {
    mode: SimulationMode,
    pending: Arc<AtomicBool>,
}

impl RebuildTrackedStateSimulation {
    pub(crate) fn new(mode: SimulationMode) -> Self {
        Self {
            mode,
            pending: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn after_successful_write(&self) {
        if self.mode == SimulationMode::TrackedStateRebuild {
            self.pending.store(true, Ordering::SeqCst);
        }
    }

    pub(crate) async fn before_read(
        &self,
        engine: &Engine,
        branch_id: &str,
    ) -> Result<(), LixError> {
        if self.mode != SimulationMode::TrackedStateRebuild {
            return Ok(());
        }
        if !self.pending.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        engine.rebuild_tracked_state_for_branch(branch_id).await
    }

    #[cfg(test)]
    fn pending_for_test(&self) -> bool {
        self.pending.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_shuffle_is_only_enabled_for_rebuild_mode() {
        assert!(!deterministic_timestamp_shuffle_for(SimulationMode::Base));
        assert!(deterministic_timestamp_shuffle_for(
            SimulationMode::TrackedStateRebuild
        ));
    }

    #[test]
    fn successful_write_marks_rebuild_pending_only_in_rebuild_mode() {
        let base = RebuildTrackedStateSimulation::new(SimulationMode::Base);
        let rebuild = RebuildTrackedStateSimulation::new(SimulationMode::TrackedStateRebuild);

        base.after_successful_write();
        rebuild.after_successful_write();

        assert!(!base.pending_for_test());
        assert!(rebuild.pending_for_test());
    }
}
