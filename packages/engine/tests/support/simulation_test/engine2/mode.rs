/// Runtime mode for the engine2 simulation harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine2SimulationMode {
    Base,
    TrackedStateRebuild,
}

impl Engine2SimulationMode {
    pub fn name(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::TrackedStateRebuild => "tracked_state_rebuild",
        }
    }
}

/// Options for `simulation_test2!`.
///
/// Deterministic mode is enabled by default so the base and rebuild runs can be
/// compared exactly without per-backend result normalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Engine2SimulationOptions {
    pub deterministic: bool,
}

impl Default for Engine2SimulationOptions {
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
        assert_eq!(Engine2SimulationMode::Base.name(), "base");
        assert_eq!(
            Engine2SimulationMode::TrackedStateRebuild.name(),
            "tracked_state_rebuild"
        );
    }

    #[test]
    fn deterministic_mode_is_enabled_by_default() {
        assert!(Engine2SimulationOptions::default().deterministic);
    }
}
