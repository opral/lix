use std::collections::{BTreeMap, BTreeSet};

use crate::live_tracked_state::TrackedWriteRow;
use crate::live_untracked_state::UntrackedWriteRow;
use crate::LixError;

use super::contracts::TransactionDelta;
use super::participants::{PendingTxnParticipants, RowIdentity};

#[derive(Debug, Clone)]
pub(crate) enum TxnMaterializationUnit {
    EnsureUntrackedStorage { schema_keys: Vec<String> },
    ApplyTracked { writes: Vec<TrackedWriteRow> },
    ApplyUntracked { writes: Vec<UntrackedWriteRow> },
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TxnMaterializationPlan {
    pub(crate) units: Vec<TxnMaterializationUnit>,
}

impl TxnMaterializationPlan {
    pub(crate) fn from_public_delta(delta: &TransactionDelta) -> Self {
        let mut units = Vec::new();
        if !delta.untracked_writes.is_empty() {
            let schema_keys = delta
                .untracked_writes
                .iter()
                .map(|row| row.schema_key.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            units.push(TxnMaterializationUnit::EnsureUntrackedStorage { schema_keys });
        }
        if !delta.tracked_writes.is_empty() {
            units.push(TxnMaterializationUnit::ApplyTracked {
                writes: delta.tracked_writes.clone(),
            });
        }
        if !delta.untracked_writes.is_empty() {
            units.push(TxnMaterializationUnit::ApplyUntracked {
                writes: delta.untracked_writes.clone(),
            });
        }
        Self { units }
    }

    pub(crate) fn extend(&mut self, other: TxnMaterializationPlan) {
        self.units.extend(other.units);
        self.coalesce_units();
    }

    fn coalesce_units(&mut self) {
        let mut coalesced = Vec::with_capacity(self.units.len());
        for unit in std::mem::take(&mut self.units) {
            match unit {
                TxnMaterializationUnit::EnsureUntrackedStorage { schema_keys } => {
                    if let Some(TxnMaterializationUnit::EnsureUntrackedStorage {
                        schema_keys: current,
                    }) = coalesced.last_mut()
                    {
                        current.extend(schema_keys);
                        current.sort();
                        current.dedup();
                    } else {
                        coalesced.push(TxnMaterializationUnit::EnsureUntrackedStorage {
                            schema_keys,
                        });
                    }
                }
                TxnMaterializationUnit::ApplyTracked { writes } => {
                    if let Some(TxnMaterializationUnit::ApplyTracked { writes: current }) =
                        coalesced.last_mut()
                    {
                        current.extend(writes);
                    } else {
                        coalesced.push(TxnMaterializationUnit::ApplyTracked { writes });
                    }
                }
                TxnMaterializationUnit::ApplyUntracked { writes } => {
                    if let Some(TxnMaterializationUnit::ApplyUntracked { writes: current }) =
                        coalesced.last_mut()
                    {
                        current.extend(writes);
                    } else {
                        coalesced.push(TxnMaterializationUnit::ApplyUntracked { writes });
                    }
                }
            }
        }
        self.units = coalesced;
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TxnDelta {
    public_delta: TransactionDelta,
    materialization_plan: TxnMaterializationPlan,
    pending_txn_participants: PendingTxnParticipants,
}

impl TxnDelta {
    pub(crate) fn from_public_delta(delta: TransactionDelta) -> Result<Self, LixError> {
        let public_delta = coalesce_public_delta(delta);
        let pending_txn_participants = PendingTxnParticipants::from_delta(&public_delta)?;
        let materialization_plan = TxnMaterializationPlan::from_public_delta(&public_delta);
        Ok(Self {
            public_delta,
            materialization_plan,
            pending_txn_participants,
        })
    }

    pub(crate) fn materialization_plan(&self) -> &TxnMaterializationPlan {
        &self.materialization_plan
    }

    pub(crate) fn pending_txn_participants(&self) -> &PendingTxnParticipants {
        &self.pending_txn_participants
    }

    pub(crate) fn extend(&mut self, incoming: TxnDelta) {
        self.public_delta = coalesce_public_delta(merge_public_deltas(
            self.public_delta.clone(),
            incoming.public_delta,
        ));
        self.materialization_plan.extend(incoming.materialization_plan);
        self.pending_txn_participants
            .merge(incoming.pending_txn_participants);
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MutationJournal {
    staged_delta: Option<TxnDelta>,
    staged_count: usize,
    continuation_safe: bool,
}

impl MutationJournal {
    pub(crate) fn is_empty(&self) -> bool {
        self.staged_delta.is_none()
    }

    pub(crate) fn staged_count(&self) -> usize {
        self.staged_count
    }

    pub(crate) fn continuation_safe(&self) -> bool {
        self.continuation_safe
    }

    pub(crate) fn stage_delta(&mut self, incoming: TxnDelta) -> Result<(), LixError> {
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

    pub(crate) fn materialization_plan(&self) -> Option<&TxnMaterializationPlan> {
        self.staged_delta.as_ref().map(TxnDelta::materialization_plan)
    }

    pub(crate) fn pending_txn_participants(&self) -> Option<&PendingTxnParticipants> {
        self.staged_delta
            .as_ref()
            .map(TxnDelta::pending_txn_participants)
    }

    fn can_stage_delta(&self, incoming: &TxnDelta) -> bool {
        let Some(current) = self.pending_txn_participants() else {
            return true;
        };

        let current_tracked = current.tracked_identities();
        let current_untracked = current.untracked_identities();
        let incoming_tracked = incoming.pending_txn_participants().tracked_identities();
        let incoming_untracked = incoming.pending_txn_participants().untracked_identities();

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
