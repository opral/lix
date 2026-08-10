//! Concrete committed and staged state views.
//!
//! This is the Wave 2 native read boundary. `ForkTreeStateView` owns one
//! authenticated retained ForkTree view; `TransactionStateView` adds an
//! ordered staged-row overlay. Neither boundary exposes a generic reader
//! trait or a compatibility request/batch vocabulary.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::LixError;
use crate::common::LixTimestamp;
use crate::forktree::{
    CanonicalBranchId, ForkTreeReadFacade, ObjectId, StateCell, StateKey, StateSource, StateValue,
    UntrackedValue, VisibleStateRow,
};
use crate::storage::StorageError;
use crate::storage_adapter::StorageAdapterRead;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// Durable predecessor evidence authenticated by a retained ForkTree view.
/// Only the timestamp needed by publication is carried; row identity and
/// payload remain owned by the native state view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CertifiedStatePredecessor {
    created_at: LixTimestamp,
}

impl CertifiedStatePredecessor {
    pub(crate) fn new(created_at: LixTimestamp) -> Self {
        Self { created_at }
    }

    pub(crate) fn created_at(&self) -> Result<LixTimestamp, LixError> {
        Ok(self.created_at)
    }
}

const DIFF_ID_PREFIX: &str = "d1.";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DiffSides {
    pub(crate) before: Option<crate::changelog::ChangeId>,
    pub(crate) after: Option<crate::changelog::ChangeId>,
}

pub(crate) fn encode_diff_id(
    before: Option<crate::changelog::ChangeId>,
    after: Option<crate::changelog::ChangeId>,
) -> Result<String, LixError> {
    if before.is_none() && after.is_none() {
        return Err(LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            "invalid diff_id: a diff must contain at least one side",
        ));
    }
    let mut bytes = Vec::with_capacity(33);
    bytes.push(u8::from(before.is_some()) + u8::from(after.is_some()) * 2);
    if let Some(change_id) = before {
        bytes.extend_from_slice(change_id.as_uuid().as_bytes());
    }
    if let Some(change_id) = after {
        bytes.extend_from_slice(change_id.as_uuid().as_bytes());
    }
    Ok(format!("{DIFF_ID_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)))
}

pub(crate) fn decode_diff_id(value: &str) -> Result<DiffSides, LixError> {
    let invalid = |message: &str| {
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("invalid diff_id: {message}"),
        )
    };
    let payload = value
        .strip_prefix(DIFF_ID_PREFIX)
        .ok_or_else(|| invalid("unsupported or missing version"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| invalid("payload is not valid base64url"))?;
    let Some(flags) = bytes.first().copied() else {
        return Err(invalid("payload is empty"));
    };
    if flags == 0 || flags & !3 != 0 {
        return Err(invalid("side flags are invalid"));
    }
    let expected = 1 + usize::from(flags & 1 != 0) * 16 + usize::from(flags & 2 != 0) * 16;
    if bytes.len() != expected {
        return Err(invalid("payload length does not match its side flags"));
    }
    let mut offset = 1;
    let mut take = |present: bool| {
        if !present {
            return None;
        }
        let uuid = uuid::Uuid::from_slice(&bytes[offset..offset + 16])
            .expect("validated diff id UUID slice length");
        offset += 16;
        Some(crate::changelog::ChangeId::new(uuid))
    };
    Ok(DiffSides {
        before: take(flags & 1 != 0),
        after: take(flags & 2 != 0),
    })
}

fn canonical_branch_id(branch_id: &str) -> Result<CanonicalBranchId, LixError> {
    let uuid = uuid::Uuid::parse_str(branch_id).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("branch ID must be a UUID: {error}"),
        )
    })?;
    Ok(CanonicalBranchId::from_bytes(*uuid.as_bytes()))
}

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

/// Authenticated global/local state roots for one historical view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StateRoots {
    pub(crate) global: ObjectId,
    pub(crate) local: Option<ObjectId>,
}

/// One native state value selected at a diff endpoint, retaining its source
/// root provenance and the complete authenticated value identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StateDiffValue {
    pub(crate) value: StateValue,
    pub(crate) source: StateSource,
}

/// Ordered native diff entry between two explicit authenticated root pairs.
/// The key order is inherited from the ordered-tree diff and is never rebuilt
/// through a map or sorted after the fact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StateDiffEntry {
    pub(crate) key: StateKey,
    pub(crate) before: Option<StateDiffValue>,
    pub(crate) after: Option<StateDiffValue>,
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
#[derive(Clone)]
pub(crate) struct ForkTreeStateView<R> {
    view: Arc<crate::forktree::CoherentView<R>>,
}

impl<R> ForkTreeStateView<R>
where
    R: StorageAdapterRead,
{
    pub(crate) fn new(view: crate::forktree::CoherentView<R>) -> Self {
        Self {
            view: Arc::new(view),
        }
    }

    pub(crate) async fn from_facade(
        facade: ForkTreeReadFacade<R>,
        branch_id: &str,
    ) -> Result<Self, LixError> {
        Ok(Self::new(facade.into_branch(branch_id).await?))
    }

    /// Loads authenticated change records through this view's retained read.
    /// Merge analysis uses this only to bind native state-diff values to their
    /// immutable change payloads; it never opens a second read.
    pub(crate) async fn load_change_records(
        &self,
        ids: &[crate::changelog::ChangeId],
    ) -> Result<Vec<Option<crate::changelog::ChangeRecord>>, LixError> {
        crate::forktree::load_change_records(self.view.retained_read(), ids).await
    }

    pub(crate) fn branch_id_matches(&self, branch_id: &str) -> Result<bool, LixError> {
        Ok(self.view.branch_id() == canonical_branch_id(branch_id)?)
    }

    /// Returns the UUID text for the branch bound to this retained view.
    /// Selector identity remains owned and authenticated by the coherent
    /// ForkTree view; this is only row provenance for native consumers.
    pub(crate) fn branch_id(&self) -> String {
        uuid::Uuid::from_bytes(*self.view.branch_id().as_bytes()).to_string()
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

    /// Resolves exact keys from another branch through a borrowed coherent
    /// view. The branch view reuses this view's retained read; it never
    /// refreshes the selector or opens a second storage operation.
    pub(crate) async fn branch_points(
        &self,
        branch_id: &str,
        keys: &[Vec<u8>],
        include_tombstones: bool,
    ) -> Result<Vec<Option<StateRow>>, LixError> {
        let branch_id = canonical_branch_id(branch_id)?;
        self.view
            .branch_view(branch_id)
            .await?
            .points(keys, include_tombstones)
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| row.map(StateRow::from_committed))
                    .collect()
            })
            .map_err(LixError::from)
    }

    /// Resolves one bounded range from another branch through a borrowed
    /// coherent view. The native ForkTree range remains the only traversal;
    /// visibility and tombstones are authenticated by that branch view.
    pub(crate) async fn branch_range(
        &self,
        branch_id: &str,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
        include_tombstones: bool,
    ) -> Result<Vec<StateRow>, LixError> {
        let branch_id = canonical_branch_id(branch_id)?;
        self.view
            .branch_view(branch_id)
            .await?
            .range(lower, upper, limit, include_tombstones)
            .await
            .map(|rows| rows.into_iter().map(StateRow::from_committed).collect())
            .map_err(LixError::from)
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

    /// Diffs two explicit authenticated root pairs through this retained
    /// ForkTree view. Equal roots short-circuit before any tree descent;
    /// changed keys are merged from the two intrinsically ordered native
    /// root diffs and resolved at both endpoints with local-over-global
    /// precedence.
    pub(crate) async fn diff_roots(
        &self,
        before: StateRoots,
        after: StateRoots,
    ) -> Result<Vec<StateDiffEntry>, LixError> {
        if before == after {
            return Ok(Vec::new());
        }

        let local_changes =
            crate::forktree::diff_roots(before.local, after.local, self.view.retained_read())
                .await?;
        let global_changes = crate::forktree::diff_roots(
            Some(before.global),
            Some(after.global),
            self.view.retained_read(),
        )
        .await?;
        let keys = merge_sorted_state_keys(local_changes, global_changes);
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let encoded = keys
            .iter()
            .map(|key| {
                crate::forktree::encode_state_key(crate::forktree::StateKeyRef {
                    schema_key: &key.schema_key,
                    file_id: key.file_id.as_deref(),
                    entity_pk: &key.entity_pk,
                })
            })
            .collect::<Vec<_>>();
        let before_rows = crate::forktree::state_points_on_read(
            before.global,
            before.local,
            &encoded,
            true,
            self.view.retained_read(),
        )
        .await?;
        let after_rows = crate::forktree::state_points_on_read(
            after.global,
            after.local,
            &encoded,
            true,
            self.view.retained_read(),
        )
        .await?;
        let mut output = Vec::new();
        for ((key, before), after) in keys.into_iter().zip(before_rows).zip(after_rows) {
            let before = before.map(|(value, source)| StateDiffValue { value, source });
            let after = after.map(|(value, source)| StateDiffValue { value, source });
            if before != after {
                output.push(StateDiffEntry { key, before, after });
            }
        }
        Ok(output)
    }
}

fn merge_sorted_state_keys(left: Vec<StateKey>, right: Vec<StateKey>) -> Vec<StateKey> {
    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();
    let mut merged = Vec::new();
    loop {
        match (left.peek(), right.peek()) {
            (None, None) => break,
            (Some(_), None) => merged.push(left.next().expect("peeked left key")),
            (None, Some(_)) => merged.push(right.next().expect("peeked right key")),
            (Some(left_key), Some(right_key)) => match left_key.cmp(right_key) {
                Ordering::Less => merged.push(left.next().expect("peeked left key")),
                Ordering::Greater => merged.push(right.next().expect("peeked right key")),
                Ordering::Equal => {
                    merged.push(left.next().expect("peeked left key"));
                    right.next();
                }
            },
        }
    }
    merged
}

/// One transaction's committed retained view plus an ordered staged overlay.
/// Staged rows are validated once at construction; points and ranges use
/// linear overlay merges and never materialize a full-key map.
#[derive(Clone)]
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

    pub(crate) async fn branch_points(
        &self,
        branch_id: &str,
        keys: &[Vec<u8>],
        include_tombstones: bool,
    ) -> Result<Vec<Option<StateRow>>, LixError> {
        if self.committed.branch_id_matches(branch_id)? {
            return self
                .points(keys, include_tombstones)
                .await
                .map_err(LixError::from);
        }
        self.committed
            .branch_points(branch_id, keys, include_tombstones)
            .await
    }

    pub(crate) async fn branch_range(
        &self,
        branch_id: &str,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
        include_tombstones: bool,
    ) -> Result<Vec<StateRow>, LixError> {
        if self.committed.branch_id_matches(branch_id)? {
            return self
                .range(lower, upper, limit, include_tombstones)
                .await
                .map_err(LixError::from);
        }
        self.committed
            .branch_range(branch_id, lower, upper, limit, include_tombstones)
            .await
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

    /// Resolves one bounded untracked branch range through the retained
    /// ForkTree view and merges the ordered staged overlay. The caller must
    /// supply a schema/entity prefix range; this method never widens it to a
    /// whole-owner scan.
    pub(crate) async fn untracked_branch_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<UntrackedStateRow>, LixError> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let committed = self
            .committed
            .untracked_branch_range(lower, upper, None)
            .await?;
        let owner = uuid::Uuid::parse_str(&self.committed.branch_id())
            .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?;
        let encode = |owner: CanonicalBranchId, key: &StateKey| {
            crate::forktree::encode_untracked_key(
                owner,
                crate::forktree::StateKeyRef {
                    schema_key: &key.schema_key,
                    file_id: key.file_id.as_deref(),
                    entity_pk: &key.entity_pk,
                },
            )
        };
        let staged = self
            .staged_untracked
            .iter()
            .filter(|row| row.owner == CanonicalBranchId::from_bytes(*owner.as_bytes()))
            .filter(|row| {
                let key = encode(row.owner, &row.key);
                lower.is_none_or(|bound| key.as_slice() >= bound)
                    && upper.is_none_or(|bound| key.as_slice() < bound)
            })
            .map(|row| {
                (
                    encode(row.owner, &row.key),
                    UntrackedStateRow {
                        owner: row.owner,
                        key: row.key.clone(),
                        value: row.value.clone(),
                    },
                )
            })
            .collect::<Vec<_>>();
        let mut committed_index = 0;
        let mut staged_index = 0;
        let mut output = Vec::new();
        while committed_index < committed.len() || staged_index < staged.len() {
            let committed_key = committed
                .get(committed_index)
                .map(|row| encode(row.owner, &row.key));
            let staged_key = staged.get(staged_index).map(|(key, _)| key.clone());
            let choice = match (committed_key.as_deref(), staged_key.as_deref()) {
                (None, None) => break,
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (Some(left), Some(right)) => left.cmp(right),
            };
            let row = match choice {
                Ordering::Less => {
                    let row = committed[committed_index].clone();
                    committed_index += 1;
                    row
                }
                Ordering::Equal => {
                    committed_index += 1;
                    let row = staged[staged_index].1.clone();
                    staged_index += 1;
                    row
                }
                Ordering::Greater => {
                    let row = staged[staged_index].1.clone();
                    staged_index += 1;
                    row
                }
            };
            output.push(row);
            if limit.is_some_and(|bound| output.len() >= bound) {
                break;
            }
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
