//! Concrete committed and staged state views.
//!
//! This is the Wave 2 native read boundary. `ForkTreeStateView` owns one
//! authenticated retained ForkTree view; `TransactionStateView` adds an
//! ordered staged-row overlay. Neither boundary exposes a generic reader
//! trait or a compatibility request/batch vocabulary.

use std::cmp::Ordering;

use crate::LixError;
use crate::forktree::{
    CanonicalBranchId, ForkTreeReadFacade, StateCell, StateKey, StateSource, StateValue,
    UntrackedValue, VisibleStateRow,
};
use crate::storage::StorageError;
use crate::storage_adapter::StorageAdapterRead;

/// One logical row returned by a concrete state view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StateRow {
    pub(crate) key: Vec<u8>,
    pub(crate) value: StateValue,
    pub(crate) source: StateRowSource,
}

/// One authenticated untracked row, retaining the owner branch identity so a
/// consumer cannot republish a global value as branch-local state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UntrackedStateRow {
    pub(crate) owner: CanonicalBranchId,
    pub(crate) key: StateKey,
    pub(crate) value: UntrackedValue,
}

impl StateRow {
    fn from_committed(row: VisibleStateRow) -> Self {
        Self {
            key: row.encoded_key,
            value: row.value,
            source: match row.source {
                StateSource::Global => StateRowSource::Global,
                StateSource::Branch => StateRowSource::Branch,
            },
        }
    }

    fn from_staged(row: &StagedStateRow) -> Self {
        Self {
            key: row.key.clone(),
            value: row.value.clone(),
            source: StateRowSource::Staged,
        }
    }
}

/// Provenance of a row after the committed/staged overlay is resolved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StateRowSource {
    Global,
    Branch,
    Staged,
}

/// One already-authenticated staged state cell. Rows must be supplied in
/// strict canonical key order; the transaction overlay never builds a map.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StagedStateRow {
    pub(crate) key: Vec<u8>,
    pub(crate) value: StateValue,
}

/// A staged untracked cell. The owner is explicit because untracked overlay
/// resolution must preserve branch-over-global precedence and tombstones.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StagedUntrackedStateRow {
    pub(crate) owner: CanonicalBranchId,
    pub(crate) key: StateKey,
    pub(crate) value: UntrackedValue,
}

impl StagedUntrackedStateRow {
    pub(crate) fn new(owner: CanonicalBranchId, key: StateKey, value: UntrackedValue) -> Self {
        Self { owner, key, value }
    }
}

impl StagedStateRow {
    pub(crate) fn new(key: Vec<u8>, value: StateValue) -> Self {
        Self { key, value }
    }
}

/// Authenticated committed state over one retained ForkTree read.
pub(crate) struct ForkTreeStateView<R> {
    view: crate::forktree::CoherentView<R>,
}

impl<R> ForkTreeStateView<R>
where
    R: StorageAdapterRead,
{
    pub(crate) fn new(view: crate::forktree::CoherentView<R>) -> Self {
        Self { view }
    }

    pub(crate) async fn from_facade(
        facade: ForkTreeReadFacade<R>,
        branch_id: &str,
    ) -> Result<Self, LixError> {
        Ok(Self::new(facade.into_branch(branch_id).await?))
    }

    /// Resolves exact keys through the native authenticated ordered-tree
    /// primitive. Result slots preserve request order and duplicates.
    pub(crate) async fn points(
        &self,
        keys: &[Vec<u8>],
        include_tombstones: bool,
    ) -> Result<Vec<Option<StateRow>>, StorageError> {
        self.view
            .points(keys, include_tombstones)
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| row.map(StateRow::from_committed))
                    .collect()
            })
    }

    /// Resolves one canonical half-open byte range through the native
    /// authenticated range primitive.
    pub(crate) async fn range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
        include_tombstones: bool,
    ) -> Result<Vec<StateRow>, StorageError> {
        self.view
            .range(lower, upper, limit, include_tombstones)
            .await
            .map(|rows| rows.into_iter().map(StateRow::from_committed).collect())
    }

    /// Resolves exact untracked state keys through the same retained view.
    /// Returned slots preserve request order and duplicates, while each
    /// result retains the authenticated local/global owner.
    pub(crate) async fn untracked_points(
        &self,
        state_keys: &[Vec<u8>],
    ) -> Result<Vec<Option<UntrackedStateRow>>, LixError> {
        Ok(self
            .view
            .load_untracked_overlay_points(state_keys)
            .await?
            .into_iter()
            .map(|row| row.map(|(owner, key, value)| UntrackedStateRow { owner, key, value }))
            .collect())
    }

    /// Scans the authenticated untracked local/global overlay through the
    /// retained view and keeps the owner attached to every result.
    pub(crate) async fn untracked_overlay_rows(&self) -> Result<Vec<UntrackedStateRow>, LixError> {
        Ok(self
            .view
            .scan_untracked_overlay_rows()
            .await?
            .into_iter()
            .map(|(owner, key, value)| UntrackedStateRow { owner, key, value })
            .collect())
    }

    /// Scans one authenticated branch's untracked key range without opening
    /// another read or traversing unrelated owners.
    pub(crate) async fn untracked_branch_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<UntrackedStateRow>, LixError> {
        let owner = self.view.branch_id();
        Ok(self
            .view
            .scan_untracked_branch_range(lower, upper, limit)
            .await?
            .into_iter()
            .map(|(key, value)| UntrackedStateRow { owner, key, value })
            .collect())
    }
}

/// One transaction's committed retained view plus an ordered staged overlay.
/// Staged rows are validated once at construction; points and ranges use
/// linear overlay merges and never materialize a full-key map.
pub(crate) struct TransactionStateView<R> {
    committed: ForkTreeStateView<R>,
    staged: Vec<StagedStateRow>,
    staged_untracked: Vec<StagedUntrackedStateRow>,
}

impl<R> TransactionStateView<R>
where
    R: StorageAdapterRead,
{
    pub(crate) fn new(
        committed: ForkTreeStateView<R>,
        staged: Vec<StagedStateRow>,
    ) -> Result<Self, LixError> {
        if staged.windows(2).any(|rows| rows[0].key >= rows[1].key) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "staged state rows are not strictly ordered",
            ));
        }
        Ok(Self {
            committed,
            staged,
            staged_untracked: Vec::new(),
        })
    }

    /// Constructs a transaction view with explicitly owned staged untracked
    /// cells. The untracked overlay is kept ordered and linear just like the
    /// tracked staged rows; no full-key map is introduced.
    pub(crate) fn new_with_untracked(
        committed: ForkTreeStateView<R>,
        staged: Vec<StagedStateRow>,
        staged_untracked: Vec<StagedUntrackedStateRow>,
    ) -> Result<Self, LixError> {
        let view = Self::new(committed, staged)?;
        if staged_untracked
            .windows(2)
            .any(|rows| rows[0].key >= rows[1].key)
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "staged untracked state rows are not strictly ordered",
            ));
        }
        Ok(Self {
            staged_untracked,
            ..view
        })
    }

    /// Resolves every requested key before applying visibility. A staged
    /// tombstone masks a committed value even when tombstones are omitted
    /// from the returned slots; duplicate request slots retain their order.
    pub(crate) async fn points(
        &self,
        keys: &[Vec<u8>],
        include_tombstones: bool,
    ) -> Result<Vec<Option<StateRow>>, StorageError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut order = (0..keys.len()).collect::<Vec<_>>();
        order.sort_by(|left, right| keys[*left].cmp(&keys[*right]).then(left.cmp(right)));
        let sorted_keys = order
            .iter()
            .map(|index| keys[*index].clone())
            .collect::<Vec<_>>();
        let committed = self.committed.points(&sorted_keys, true).await?;
        let mut staged_index = 0;
        let mut sorted_rows = Vec::with_capacity(keys.len());
        for (key, committed_row) in sorted_keys.iter().zip(committed) {
            while staged_index < self.staged.len()
                && self.staged[staged_index].key.as_slice() < key.as_slice()
            {
                staged_index += 1;
            }
            let row = if staged_index < self.staged.len()
                && self.staged[staged_index].key.as_slice() == key.as_slice()
            {
                visible_staged_row(&self.staged[staged_index], include_tombstones)
            } else {
                committed_row.and_then(|row| visible_committed_row(row, include_tombstones))
            };
            sorted_rows.push(row);
        }

        let mut output = (0..keys.len()).map(|_| None).collect::<Vec<_>>();
        for (sorted_slot, original_slot) in order.into_iter().enumerate() {
            output[original_slot] = sorted_rows[sorted_slot].take();
        }
        Ok(output)
    }

    /// Merges committed and staged rows in key order, applying the limit only
    /// after staged replacement and tombstone visibility are resolved.
    pub(crate) async fn range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
        include_tombstones: bool,
    ) -> Result<Vec<StateRow>, StorageError> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let committed = self.committed.range(lower, upper, None, true).await?;
        let mut committed_index = 0;
        let mut staged_index = self
            .staged
            .partition_point(|row| lower.is_some_and(|bound| row.key.as_slice() < bound));
        let mut output = Vec::new();

        while committed_index < committed.len() || staged_index < self.staged.len() {
            let committed_key = committed.get(committed_index).map(|row| row.key.as_slice());
            let staged_key = self.staged.get(staged_index).map(|row| row.key.as_slice());
            let choice = match (committed_key, staged_key) {
                (None, None) => break,
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (Some(committed), Some(staged)) => committed.cmp(staged),
            };

            let row = match choice {
                Ordering::Less => {
                    let row = committed[committed_index].clone();
                    committed_index += 1;
                    visible_committed_row(row, include_tombstones)
                }
                Ordering::Equal => {
                    let row = visible_staged_row(&self.staged[staged_index], include_tombstones);
                    committed_index += 1;
                    staged_index += 1;
                    row
                }
                Ordering::Greater => {
                    let key = self.staged[staged_index].key.as_slice();
                    if upper.is_some_and(|bound| key >= bound) {
                        break;
                    }
                    let row = visible_staged_row(&self.staged[staged_index], include_tombstones);
                    staged_index += 1;
                    row
                }
            };
            if let Some(row) = row {
                output.push(row);
                if limit.is_some_and(|limit| output.len() >= limit) {
                    break;
                }
            }
        }
        Ok(output)
    }

    /// Resolves untracked exact points with staged rows taking precedence.
    /// A staged tombstone suppresses the committed local/global answer even
    /// when the caller omits tombstones from its result projection.
    pub(crate) async fn untracked_points(
        &self,
        state_keys: &[Vec<u8>],
        include_tombstones: bool,
    ) -> Result<Vec<Option<UntrackedStateRow>>, LixError> {
        let committed = self.committed.untracked_points(state_keys).await?;
        let mut output = Vec::with_capacity(state_keys.len());
        for (key, committed_row) in state_keys.iter().zip(committed) {
            let decoded_key = crate::forktree::decode_state_key(key)?;
            let staged = self
                .staged_untracked
                .iter()
                .find(|row| row.key == decoded_key);
            let row = staged
                .map(|row| {
                    if !include_tombstones && row.value.cell.deleted() {
                        None
                    } else {
                        Some(UntrackedStateRow {
                            owner: row.owner,
                            key: row.key.clone(),
                            value: row.value.clone(),
                        })
                    }
                })
                .unwrap_or(committed_row);
            output.push(row);
        }
        Ok(output)
    }
}

fn visible_committed_row(row: StateRow, include_tombstones: bool) -> Option<StateRow> {
    if !include_tombstones && matches!(row.value.cell, StateCell::Tombstone) {
        None
    } else {
        Some(row)
    }
}

fn visible_staged_row(row: &StagedStateRow, include_tombstones: bool) -> Option<StateRow> {
    if !include_tombstones && matches!(row.value.cell, StateCell::Tombstone) {
        None
    } else {
        Some(StateRow::from_staged(row))
    }
}
