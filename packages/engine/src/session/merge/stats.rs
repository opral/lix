use crate::tracked_state::{
    TrackedStateDiff, TrackedStateDiffKind, TrackedStateMergePatch, TrackedStateMergePlan,
};
use crate::LixError;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct MergeStats {
    pub(crate) total: usize,
    pub(crate) added: usize,
    pub(crate) modified: usize,
    pub(crate) removed: usize,
}

pub(crate) fn stats_from_diff(diff: &TrackedStateDiff) -> MergeStats {
    let mut stats = MergeStats::default();
    for entry in &diff.entries {
        stats.add(entry.kind);
    }
    stats
}

pub(crate) fn stats_from_plan(
    plan: &TrackedStateMergePlan,
    source_diff: &TrackedStateDiff,
) -> Result<MergeStats, LixError> {
    let mut stats = MergeStats::default();
    for patch in &plan.patches {
        let identity = patch_identity(patch);
        let Some(entry) = source_diff
            .entries
            .iter()
            .find(|entry| &entry.identity == identity)
        else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "merge analysis could not find source diff entry for adopted schema '{}' entity '{}'",
                    identity.schema_key,
                    identity.entity_id.as_json_array_text()?
                ),
            ));
        };
        stats.add(entry.kind);
    }
    Ok(stats)
}

impl MergeStats {
    fn add(&mut self, kind: TrackedStateDiffKind) {
        self.total += 1;
        match kind {
            TrackedStateDiffKind::Added => self.added += 1,
            TrackedStateDiffKind::Modified => self.modified += 1,
            TrackedStateDiffKind::Removed => self.removed += 1,
        }
    }
}

fn patch_identity(
    patch: &TrackedStateMergePatch,
) -> &crate::tracked_state::TrackedStateDiffIdentity {
    match patch {
        TrackedStateMergePatch::Adopt { identity, .. } => identity,
    }
}
