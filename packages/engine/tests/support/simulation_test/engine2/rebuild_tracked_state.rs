use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use lix_engine::engine2::Engine;
use lix_engine::LixError;

use super::mode::Engine2SimulationMode;

/// Returns whether a simulation mode should shuffle deterministic timestamps.
///
/// Rebuild mode intentionally shuffles timestamps so tests do not encode
/// assumptions that tracked-state rebuild order and write-time order match.
pub(crate) fn deterministic_timestamp_shuffle_for(mode: Engine2SimulationMode) -> bool {
    matches!(mode, Engine2SimulationMode::TrackedStateRebuild)
}

/// Mode-specific read/write hook for tracked-state rebuild simulation.
#[derive(Clone)]
pub(crate) struct RebuildTrackedStateSimulation {
    mode: Engine2SimulationMode,
    pending: Arc<AtomicBool>,
}

impl RebuildTrackedStateSimulation {
    pub(crate) fn new(mode: Engine2SimulationMode) -> Self {
        Self {
            mode,
            pending: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn after_successful_write(&self) {
        if self.mode == Engine2SimulationMode::TrackedStateRebuild {
            self.pending.store(true, Ordering::SeqCst);
        }
    }

    pub(crate) async fn before_read(
        &self,
        engine: &Engine,
        version_id: &str,
    ) -> Result<(), LixError> {
        if self.mode != Engine2SimulationMode::TrackedStateRebuild {
            return Ok(());
        }
        if !self.pending.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        engine.rebuild_tracked_state_for_version(version_id).await
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
        assert!(!deterministic_timestamp_shuffle_for(
            Engine2SimulationMode::Base
        ));
        assert!(deterministic_timestamp_shuffle_for(
            Engine2SimulationMode::TrackedStateRebuild
        ));
    }

    #[test]
    fn successful_write_marks_rebuild_pending_only_in_rebuild_mode() {
        let base = RebuildTrackedStateSimulation::new(Engine2SimulationMode::Base);
        let rebuild =
            RebuildTrackedStateSimulation::new(Engine2SimulationMode::TrackedStateRebuild);

        base.after_successful_write();
        rebuild.after_successful_write();

        assert!(!base.pending_for_test());
        assert!(rebuild.pending_for_test());
    }
}
