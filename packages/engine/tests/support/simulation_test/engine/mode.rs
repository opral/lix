/// Runtime mode for the simulation harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulationMode {
    Base,
    TrackedStateRebuild,
}

impl SimulationMode {
    pub fn name(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::TrackedStateRebuild => "tracked_state_rebuild",
        }
    }
}

/// Options for `simulation_test!`.
///
/// Deterministic mode is enabled by default so the base and rebuild runs can be
/// compared exactly without per-storage result normalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimulationOptions {
    pub deterministic: bool,
}

impl Default for SimulationOptions {
    fn default() -> Self {
        Self {
            deterministic: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_names_are_stable_for_generated_test_names() {
        assert_eq!(SimulationMode::Base.name(), "base");
        assert_eq!(
            SimulationMode::TrackedStateRebuild.name(),
            "tracked_state_rebuild"
        );
    }

    #[test]
    fn deterministic_mode_is_enabled_by_default() {
        assert!(SimulationOptions::default().deterministic);
    }
}
