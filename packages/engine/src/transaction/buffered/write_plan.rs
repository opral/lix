use std::collections::BTreeMap;

use crate::live_state::{LiveWriteRow, RowIdentity};
use crate::transaction::overlay::PendingRowOverlay;
use crate::transaction::TransactionDelta;
use crate::LixError;

#[derive(Debug, Clone, Default)]
pub(crate) struct WritePlan {
    pub(crate) writes: Vec<LiveWriteRow>,
}

impl WritePlan {
    pub(crate) fn from_public_delta(delta: &TransactionDelta) -> Self {
        Self {
            writes: delta.writes.clone(),
        }
    }

    pub(crate) fn extend(&mut self, other: WritePlan) {
        self.writes.extend(other.writes);
        self.coalesce_writes();
    }

    fn coalesce_writes(&mut self) {
        self.writes = coalesce_by_identity(std::mem::take(&mut self.writes));
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WriteDelta {
    public_delta: TransactionDelta,
    materialization_plan: WritePlan,
    pending_row_overlay: PendingRowOverlay,
}

impl WriteDelta {
    pub(crate) fn from_public_delta(delta: TransactionDelta) -> Result<Self, LixError> {
        let public_delta = coalesce_public_delta(delta);
        let pending_row_overlay = PendingRowOverlay::from_delta(&public_delta)?;
        let materialization_plan = WritePlan::from_public_delta(&public_delta);
        Ok(Self {
            public_delta,
            materialization_plan,
            pending_row_overlay,
        })
    }

    pub(crate) fn materialization_plan(&self) -> &WritePlan {
        &self.materialization_plan
    }

    pub(crate) fn pending_row_overlay(&self) -> &PendingRowOverlay {
        &self.pending_row_overlay
    }

    pub(crate) fn extend(&mut self, incoming: WriteDelta) {
        self.public_delta = coalesce_public_delta(merge_public_deltas(
            self.public_delta.clone(),
            incoming.public_delta,
        ));
        self.materialization_plan
            .extend(incoming.materialization_plan);
        self.pending_row_overlay.merge(incoming.pending_row_overlay);
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct WriteJournal {
    staged_delta: Option<WriteDelta>,
    staged_count: usize,
    continuation_safe: bool,
}

impl WriteJournal {
    pub(crate) fn is_empty(&self) -> bool {
        self.staged_delta.is_none()
    }

    pub(crate) fn staged_count(&self) -> usize {
        self.staged_count
    }

    pub(crate) fn continuation_safe(&self) -> bool {
        self.continuation_safe
    }

    pub(crate) fn stage_delta(&mut self, incoming: WriteDelta) -> Result<(), LixError> {
        if !self.is_empty() && !self.can_stage_delta(&incoming) {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "cannot stage conflicting tracked and untracked identities in one isolated transaction batch",
            ));
        }

        match self.staged_delta.as_mut() {
            Some(current) => current.extend(incoming),
            None => self.staged_delta = Some(incoming),
        }
        self.staged_count += 1;
        self.continuation_safe = true;
        Ok(())
    }

    pub(crate) fn aggregated_public_delta(&self) -> TransactionDelta {
        self.staged_delta
            .as_ref()
            .map(|delta| delta.public_delta.clone())
            .unwrap_or_default()
    }

    pub(crate) fn materialization_plan(&self) -> Option<&WritePlan> {
        self.staged_delta
            .as_ref()
            .map(WriteDelta::materialization_plan)
    }

    pub(crate) fn pending_row_overlay(&self) -> Option<&PendingRowOverlay> {
        self.staged_delta
            .as_ref()
            .map(WriteDelta::pending_row_overlay)
    }

    fn can_stage_delta(&self, incoming: &WriteDelta) -> bool {
        let Some(current) = self.pending_row_overlay() else {
            return true;
        };

        let current_tracked = current.tracked_identities();
        let current_untracked = current.untracked_identities();
        let incoming_tracked = incoming.pending_row_overlay().tracked_identities();
        let incoming_untracked = incoming.pending_row_overlay().untracked_identities();

        current_tracked.is_disjoint(&incoming_untracked)
            && current_untracked.is_disjoint(&incoming_tracked)
    }
}

fn merge_public_deltas(current: TransactionDelta, incoming: TransactionDelta) -> TransactionDelta {
    let mut merged = current;
    merged.writes.extend(incoming.writes);
    merged
}

fn coalesce_public_delta(delta: TransactionDelta) -> TransactionDelta {
    TransactionDelta {
        writes: coalesce_by_identity(delta.writes),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct WriteIdentity {
    row: RowIdentity,
    untracked: bool,
}

fn coalesce_by_identity(writes: Vec<LiveWriteRow>) -> Vec<LiveWriteRow> {
    let mut latest = BTreeMap::<WriteIdentity, LiveWriteRow>::new();
    for write in writes {
        latest.insert(
            WriteIdentity {
                row: RowIdentity::from_live_write(&write),
                untracked: write.untracked,
            },
            write,
        );
    }
    latest.into_values().collect()
}
