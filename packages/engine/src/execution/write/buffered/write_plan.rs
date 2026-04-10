use std::collections::BTreeMap;

use crate::contracts::{RowIdentity, TrackedWriteRow, UntrackedWriteRow};
use crate::execution::write::overlay::PendingWriteOverlay;
use crate::execution::write::TransactionDelta;
use crate::LixError;

#[derive(Debug, Clone)]
pub(crate) enum WriteUnit {
    ApplyTracked { writes: Vec<TrackedWriteRow> },
    ApplyUntracked { writes: Vec<UntrackedWriteRow> },
}

#[derive(Debug, Clone, Default)]
pub(crate) struct WritePlan {
    pub(crate) units: Vec<WriteUnit>,
}

impl WritePlan {
    pub(crate) fn from_public_delta(delta: &TransactionDelta) -> Self {
        let mut units = Vec::new();
        if !delta.tracked_writes.is_empty() {
            units.push(WriteUnit::ApplyTracked {
                writes: delta.tracked_writes.clone(),
            });
        }
        if !delta.untracked_writes.is_empty() {
            units.push(WriteUnit::ApplyUntracked {
                writes: delta.untracked_writes.clone(),
            });
        }
        Self { units }
    }

    pub(crate) fn extend(&mut self, other: WritePlan) {
        self.units.extend(other.units);
        self.coalesce_units();
    }

    fn coalesce_units(&mut self) {
        let mut coalesced = Vec::with_capacity(self.units.len());
        for unit in std::mem::take(&mut self.units) {
            match unit {
                WriteUnit::ApplyTracked { writes } => {
                    if let Some(WriteUnit::ApplyTracked { writes: current }) = coalesced.last_mut()
                    {
                        current.extend(writes);
                    } else {
                        coalesced.push(WriteUnit::ApplyTracked { writes });
                    }
                }
                WriteUnit::ApplyUntracked { writes } => {
                    if let Some(WriteUnit::ApplyUntracked { writes: current }) =
                        coalesced.last_mut()
                    {
                        current.extend(writes);
                    } else {
                        coalesced.push(WriteUnit::ApplyUntracked { writes });
                    }
                }
            }
        }
        self.units = coalesced;
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WriteDelta {
    public_delta: TransactionDelta,
    materialization_plan: WritePlan,
    pending_write_overlay: PendingWriteOverlay,
}

impl WriteDelta {
    pub(crate) fn from_public_delta(delta: TransactionDelta) -> Result<Self, LixError> {
        let public_delta = coalesce_public_delta(delta);
        let pending_write_overlay = PendingWriteOverlay::from_delta(&public_delta)?;
        let materialization_plan = WritePlan::from_public_delta(&public_delta);
        Ok(Self {
            public_delta,
            materialization_plan,
            pending_write_overlay,
        })
    }

    pub(crate) fn materialization_plan(&self) -> &WritePlan {
        &self.materialization_plan
    }

    pub(crate) fn pending_write_overlay(&self) -> &PendingWriteOverlay {
        &self.pending_write_overlay
    }

    pub(crate) fn extend(&mut self, incoming: WriteDelta) {
        self.public_delta = coalesce_public_delta(merge_public_deltas(
            self.public_delta.clone(),
            incoming.public_delta,
        ));
        self.materialization_plan
            .extend(incoming.materialization_plan);
        self.pending_write_overlay
            .merge(incoming.pending_write_overlay);
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

    pub(crate) fn pending_write_overlay(&self) -> Option<&PendingWriteOverlay> {
        self.staged_delta
            .as_ref()
            .map(WriteDelta::pending_write_overlay)
    }

    fn can_stage_delta(&self, incoming: &WriteDelta) -> bool {
        let Some(current) = self.pending_write_overlay() else {
            return true;
        };

        let current_tracked = current.tracked_identities();
        let current_untracked = current.untracked_identities();
        let incoming_tracked = incoming.pending_write_overlay().tracked_identities();
        let incoming_untracked = incoming.pending_write_overlay().untracked_identities();

        current_tracked.is_disjoint(&incoming_untracked)
            && current_untracked.is_disjoint(&incoming_tracked)
    }
}

fn merge_public_deltas(current: TransactionDelta, incoming: TransactionDelta) -> TransactionDelta {
    let mut merged = current;
    merged.tracked_writes.extend(incoming.tracked_writes);
    merged.untracked_writes.extend(incoming.untracked_writes);
    merged
}

fn coalesce_public_delta(delta: TransactionDelta) -> TransactionDelta {
    TransactionDelta {
        tracked_writes: coalesce_by_identity(delta.tracked_writes, RowIdentity::from_tracked_write),
        untracked_writes: coalesce_by_identity(
            delta.untracked_writes,
            RowIdentity::from_untracked_write,
        ),
    }
}

fn coalesce_by_identity<T, F>(writes: Vec<T>, identity: F) -> Vec<T>
where
    T: Clone,
    F: Fn(&T) -> RowIdentity,
{
    let mut latest = BTreeMap::<RowIdentity, T>::new();
    for write in writes {
        latest.insert(identity(&write), write);
    }
    latest.into_values().collect()
}
