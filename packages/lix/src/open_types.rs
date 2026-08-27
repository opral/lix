use std::sync::Arc;

/// A non-blocking observer for Lix open and automatic migration.
///
/// Implementations must return promptly. Progress is observational: panics
/// are isolated and cannot change whether opening succeeds.
pub trait OpenProgressSink: Send + Sync {
    fn report(&self, progress: OpenProgress);
}

/// Coarse, stable phases suitable for a CLI, loading screen, or logs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OpenPhase {
    Inspecting,
    Migrating,
    Validating,
    Opening,
    Complete,
}

/// One Lix-open progress snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenProgress {
    pub phase: OpenPhase,
    pub from_format: Option<u32>,
    pub to_format: u32,
    pub completed: Option<u64>,
    pub total: Option<u64>,
}

/// The migration performed by this open, if any.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenMigrationReport {
    pub from_format: u32,
    pub to_format: u32,
}

/// Immutable facts about how this handle was opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenReport {
    pub format: u32,
    pub initialized: bool,
    pub migration: Option<OpenMigrationReport>,
}

pub(crate) fn emit_open_progress(
    sink: Option<&Arc<dyn OpenProgressSink>>,
    progress: OpenProgress,
) {
    let Some(sink) = sink else { return };
    let sink = Arc::clone(sink);
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink.report(progress)));
}
