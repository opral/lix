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
use crate::entity_pk::EntityPk;
use crate::forktree::{
    CanonicalBranchId, ForkTreeReadFacade, ObjectId, StateCell, StateKey, StateSource, StateValue,
    UntrackedValue, VisibleStateRow,
};
use crate::storage_adapter::{StorageAdapterRead, StorageError};
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

    /// Checks authorship against the authenticated ForkTree ChangeCatalog.
    /// Changes are topology-owned rather than state rows, so account deletion
    /// must consult this retained view directly instead of inventing a second
    /// changelog/state authority.
    pub(crate) async fn has_authored_change(&self, account_id: &str) -> Result<bool, LixError> {
        const PAGE_SIZE: usize = 256;
        let mut start_after = None;
        loop {
            let records = crate::forktree::scan_change_records(
                self.view.retained_read(),
                start_after,
                PAGE_SIZE,
            )
            .await?;
            if records.is_empty() {
                return Ok(false);
            }
            if records.iter().any(|record| record.account_id == account_id) {
                return Ok(true);
            }
            let Some(last) = records.last() else {
                return Ok(false);
            };
            start_after = Some(last.change_id);
            if records.len() < PAGE_SIZE {
                return Ok(false);
            }
        }
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
        if self.view.branch_id() == branch_id {
            return self
                .points(keys, include_tombstones)
                .await
                .map_err(LixError::from);
        }
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
        #[cfg(feature = "storage-benches")]
        crate::sql_profile::record_scan_owner(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        let branch_id = canonical_branch_id(branch_id)?;
        if self.view.branch_id() == branch_id {
            return self
                .range(lower, upper, limit, include_tombstones)
                .await
                .map_err(LixError::from);
        }
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

    /// Resolves exact untracked keys for an explicitly selected branch using
    /// this view's retained read. The branch view is borrowed from the same
    /// authenticated coherent read; it does not refresh selectors or acquire
    /// another storage read.
    pub(crate) async fn untracked_points_for_branch(
        &self,
        branch_id: &str,
        state_keys: &[Vec<u8>],
    ) -> Result<Vec<Option<UntrackedStateRow>>, LixError> {
        let branch_id = canonical_branch_id(branch_id)?;
        if self.view.branch_id() == branch_id {
            return self.untracked_points(state_keys).await;
        }
        Ok(self
            .view
            .branch_view(branch_id)
            .await?
            .load_untracked_overlay_points(state_keys)
            .await?
            .into_iter()
            .map(|row| row.map(|(owner, key, value)| UntrackedStateRow { owner, key, value }))
            .collect())
    }

    /// Resolves exact untracked rows from one authenticated owner without
    /// applying the sibling-owner overlay. TransactionStateView uses this
    /// primitive to apply staged mutations per owner before local-over-global
    /// precedence, so a staged global row cannot mask a committed local row.
    pub(crate) async fn untracked_points_for_owner(
        &self,
        owner: CanonicalBranchId,
        state_keys: &[Vec<u8>],
    ) -> Result<Vec<Option<UntrackedStateRow>>, LixError> {
        let owner_view = self.view.branch_view(owner).await?;
        let mut rows = Vec::with_capacity(state_keys.len());
        for state_key in state_keys {
            let row = owner_view
                .load_untracked_owner_point(owner, state_key)
                .await?
                .map(|(key, value)| UntrackedStateRow { owner, key, value });
            rows.push(row);
        }
        Ok(rows)
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

    /// Scans one authenticated untracked owner over a bounded canonical
    /// state-key range. The owner is selected by the retained coherent view;
    /// no sibling owner is implicitly substituted.
    pub(crate) async fn untracked_owner_range(
        &self,
        owner: CanonicalBranchId,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<UntrackedStateRow>, LixError> {
        let owner_view = self.view.branch_view(owner).await?;
        Ok(owner_view
            .scan_untracked_branch_range(lower, upper, limit)
            .await?
            .into_iter()
            .map(|(key, value)| UntrackedStateRow { owner, key, value })
            .collect())
    }

    /// Scans the selected branch's authenticated untracked overlay together
    /// with the global owner through this same retained read. Branch values
    /// win for equal state keys, including tombstones, before visibility and
    /// LIMIT are applied. Each owner is traversed with the supplied bounds;
    /// this never widens a bounded entity scan to a whole-owner enumeration.
    pub(crate) async fn untracked_overlay_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
        include_tombstones: bool,
    ) -> Result<Vec<UntrackedStateRow>, LixError> {
        Ok(self
            .view
            .scan_untracked_overlay_branch_range(lower, upper, limit, include_tombstones)
            .await?
            .into_iter()
            .map(|(owner, key, value)| UntrackedStateRow { owner, key, value })
            .collect())
    }

    /// Scans the bounded untracked overlay for an explicitly selected branch.
    /// The local and global owner ranges are merged by logical state key so a
    /// local row (including a tombstone) masks the global row before the
    /// caller's limit is applied.
    pub(crate) async fn untracked_overlay_branch_range_for_branch(
        &self,
        branch_id: &str,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
        include_tombstones: bool,
    ) -> Result<Vec<UntrackedStateRow>, LixError> {
        #[cfg(feature = "storage-benches")]
        crate::sql_profile::record_scan_owner(0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let branch_id = canonical_branch_id(branch_id)?;
        if self.view.branch_id() == branch_id {
            return self
                .untracked_overlay_range(lower, upper, limit, include_tombstones)
                .await;
        }
        let branch_view = self.view.branch_view(branch_id).await?;
        Ok(branch_view
            .scan_untracked_overlay_branch_range(lower, upper, limit, include_tombstones)
            .await?
            .into_iter()
            .map(|(owner, key, value)| UntrackedStateRow { owner, key, value })
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

fn canonical_untracked_state_key(key: &StateKey) -> Vec<u8> {
    crate::forktree::encode_state_key(crate::forktree::StateKeyRef {
        schema_key: &key.schema_key,
        file_id: key.file_id.as_deref(),
        entity_pk: &key.entity_pk,
    })
}

fn state_key_in_range(key: &StateKey, lower: Option<&[u8]>, upper: Option<&[u8]>) -> bool {
    let encoded = canonical_untracked_state_key(key);
    lower.is_none_or(|bound| encoded.as_slice() >= bound)
        && upper.is_none_or(|bound| encoded.as_slice() < bound)
}

fn merge_untracked_owner_rows(
    global: Vec<UntrackedStateRow>,
    local: Vec<UntrackedStateRow>,
) -> Vec<UntrackedStateRow> {
    let mut global = global.into_iter().peekable();
    let mut local = local.into_iter().peekable();
    let mut merged = Vec::new();
    loop {
        match (global.peek(), local.peek()) {
            (None, None) => break,
            (Some(_), None) => merged.push(global.next().expect("peeked global row")),
            (None, Some(_)) => merged.push(local.next().expect("peeked local row")),
            (Some(global_row), Some(local_row)) => match global_row.key.cmp(&local_row.key) {
                Ordering::Less => merged.push(global.next().expect("peeked global row")),
                Ordering::Greater => merged.push(local.next().expect("peeked local row")),
                // A local row, including a tombstone, masks the global row.
                Ordering::Equal => {
                    global.next();
                    merged.push(local.next().expect("peeked local row"));
                }
            },
        }
    }
    merged
}

/// Applies one owner's staged rows over that owner's committed rows. Both
/// inputs are ordered by canonical state-key bytes, so this remains a bounded
/// linear merge and never compares owner-prefixed storage keys.
fn merge_untracked_owner_overlay(
    committed: Vec<UntrackedStateRow>,
    staged: Vec<UntrackedStateRow>,
) -> Vec<UntrackedStateRow> {
    let mut committed = committed.into_iter().peekable();
    let mut staged = staged.into_iter().peekable();
    let mut merged = Vec::new();
    loop {
        match (committed.peek(), staged.peek()) {
            (None, None) => break,
            (Some(_), None) => merged.push(committed.next().expect("peeked committed row")),
            (None, Some(_)) => merged.push(staged.next().expect("peeked staged row")),
            (Some(committed_row), Some(staged_row)) => match committed_row.key.cmp(&staged_row.key)
            {
                Ordering::Less => merged.push(committed.next().expect("peeked committed row")),
                Ordering::Greater => merged.push(staged.next().expect("peeked staged row")),
                Ordering::Equal => {
                    committed.next();
                    merged.push(staged.next().expect("peeked staged row"));
                }
            },
        }
    }
    merged
}

/// Applies staged owner rows to a committed effective local/global overlay.
/// The committed input has already resolved its own local-over-global choice;
/// this merge retains a committed local row when only a staged global row is
/// present, while still allowing staged local rows to replace it. This keeps
/// exact points and ranges on the same owner-aware precedence rule.
fn merge_untracked_overlay_rows(
    committed: Vec<UntrackedStateRow>,
    staged_global: Vec<UntrackedStateRow>,
    staged_local: Vec<UntrackedStateRow>,
    local_owner: CanonicalBranchId,
) -> Vec<UntrackedStateRow> {
    let staged = merge_untracked_owner_rows(staged_global, staged_local);
    let mut committed = committed.into_iter().peekable();
    let mut staged = staged.into_iter().peekable();
    let mut merged = Vec::new();
    loop {
        match (committed.peek(), staged.peek()) {
            (None, None) => break,
            (Some(_), None) => merged.push(committed.next().expect("peeked committed row")),
            (None, Some(_)) => merged.push(staged.next().expect("peeked staged row")),
            (Some(committed_row), Some(staged_row)) => match committed_row.key.cmp(&staged_row.key)
            {
                Ordering::Less => merged.push(committed.next().expect("peeked committed row")),
                Ordering::Greater => merged.push(staged.next().expect("peeked staged row")),
                Ordering::Equal => {
                    let committed_row = committed.next().expect("peeked committed row");
                    let staged_row = staged.next().expect("peeked staged row");
                    if staged_row.owner == local_owner || committed_row.owner != local_owner {
                        merged.push(staged_row);
                    } else {
                        merged.push(committed_row);
                    }
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
    pub(crate) fn branch_id(&self) -> String {
        self.committed.branch_id()
    }

    /// Revalidates the author against this transaction's current authenticated
    /// committed view. Session admission is intentionally insufficient: an
    /// already-open session must not publish after its account is deleted or
    /// disabled by another operation.
    pub(crate) async fn validate_active_account(&self, account_id: &str) -> Result<(), LixError> {
        let account_pk = EntityPk::uuid_from_canonical(account_id).map_err(|_| {
            LixError::new(
                "LIX_INVALID_ACCOUNT_ID",
                format!("active account id '{account_id}' is not a canonical UUID"),
            )
        })?;
        let key = crate::forktree::encode_state_key(crate::forktree::StateKeyRef {
            schema_key: "lix_account",
            file_id: None,
            entity_pk: &account_pk,
        });
        let row = self
            .points(&[key], true)
            .await
            .map_err(LixError::from)?
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ACCOUNT_NOT_FOUND",
                    format!("active account '{account_id}' does not exist"),
                )
            })?;
        let snapshot = match row.value.cell {
            StateCell::Value(value) => value,
            StateCell::Null | StateCell::Tombstone => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("account '{account_id}' has no snapshot"),
                ));
            }
        };
        let value: serde_json::Value =
            serde_json::from_str(snapshot.as_str()).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("account '{account_id}' has invalid JSON: {error}"),
                )
            })?;
        if value.get("status").and_then(serde_json::Value::as_str) != Some("active") {
            return Err(LixError::new(
                "LIX_ACCOUNT_DISABLED",
                format!("active account '{account_id}' is disabled"),
            ));
        }
        Ok(())
    }

    pub(crate) async fn has_authored_change(&self, account_id: &str) -> Result<bool, LixError> {
        self.committed.has_authored_change(account_id).await
    }

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
        for rows in staged_untracked.windows(2) {
            let left_key = canonical_untracked_state_key(&rows[0].key);
            let right_key = canonical_untracked_state_key(&rows[1].key);
            match left_key.cmp(&right_key) {
                Ordering::Less => {}
                Ordering::Greater => {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "staged untracked state rows are not ordered by canonical state key",
                    ));
                }
                Ordering::Equal if rows[0].owner == rows[1].owner => {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "duplicate staged untracked state identity",
                    ));
                }
                Ordering::Equal => {}
            }
        }
        Ok(Self {
            staged_untracked,
            ..view
        })
    }

    /// Rebinds the same retained committed view to the latest transaction
    /// buffer snapshot. This is the only mutable part of an explicit
    /// transaction read: no new storage read or committed-only facade is
    /// acquired between statements.
    pub(crate) fn with_staged_rows(
        &self,
        staged: Vec<StagedStateRow>,
        staged_untracked: Vec<StagedUntrackedStateRow>,
    ) -> Result<Self, LixError>
    where
        R: Clone,
    {
        Self::new_with_untracked(self.committed.clone(), staged, staged_untracked)
    }

    fn staged_untracked_rows_for_owner(
        &self,
        owner: CanonicalBranchId,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Vec<UntrackedStateRow> {
        self.staged_untracked
            .iter()
            .filter(|row| row.owner == owner && state_key_in_range(&row.key, lower, upper))
            .map(|row| UntrackedStateRow {
                owner: row.owner,
                key: row.key.clone(),
                value: row.value.clone(),
            })
            .collect()
    }

    fn staged_untracked_row_for_owner(
        &self,
        owner: CanonicalBranchId,
        key: &StateKey,
    ) -> Option<UntrackedStateRow> {
        self.staged_untracked
            .iter()
            .find(|row| row.owner == owner && row.key == *key)
            .map(|row| UntrackedStateRow {
                owner: row.owner,
                key: row.key.clone(),
                value: row.value.clone(),
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
        let active_owner = canonical_branch_id(self.committed.branch_id().as_str())?;
        let global_owner = canonical_branch_id(crate::GLOBAL_BRANCH_ID)?;
        let committed_local = self
            .committed
            .untracked_points_for_owner(active_owner, state_keys)
            .await?;
        let committed_global = if active_owner == global_owner {
            vec![None; state_keys.len()]
        } else {
            self.committed
                .untracked_points_for_owner(global_owner, state_keys)
                .await?
        };
        let mut output = Vec::with_capacity(state_keys.len());
        for ((key, committed_local), committed_global) in
            state_keys.iter().zip(committed_local).zip(committed_global)
        {
            let decoded_key = crate::forktree::decode_state_key(key)?;
            let staged_local = self.staged_untracked_row_for_owner(active_owner, &decoded_key);
            let staged_global = (active_owner != global_owner)
                .then(|| self.staged_untracked_row_for_owner(global_owner, &decoded_key))
                .flatten();
            // Resolve each owner independently before applying local-over-
            // global precedence. In particular, a staged global row must
            // never replace an authenticated committed local row, while a
            // local staged tombstone must still mask the global owner.
            let local = staged_local.or(committed_local);
            let global = staged_global.or(committed_global);
            let row = local.or(global);
            let row = row.filter(|row| include_tombstones || !row.value.cell.deleted());
            output.push(row);
        }
        Ok(output)
    }

    /// Resolves exact untracked keys for another branch through the same
    /// transaction-owned retained read. Staged rows are applied only when
    /// the requested branch is the transaction's branch; other branches use
    /// their authenticated committed overlay.
    pub(crate) async fn untracked_points_for_branch(
        &self,
        branch_id: &str,
        state_keys: &[Vec<u8>],
        include_tombstones: bool,
    ) -> Result<Vec<Option<UntrackedStateRow>>, LixError> {
        if self.committed.branch_id_matches(branch_id)? {
            return self.untracked_points(state_keys, include_tombstones).await;
        }
        let rows = self
            .committed
            .untracked_points_for_branch(branch_id, state_keys)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| row.filter(|row| include_tombstones || !row.value.cell.deleted()))
            .collect())
    }

    /// Returns the effective local/global untracked overlay after applying
    /// staged owners. The returned rows are canonical-key ordered and retain
    /// their authenticated owner identity for filesystem projections.
    pub(crate) async fn untracked_overlay_rows(&self) -> Result<Vec<UntrackedStateRow>, LixError> {
        let committed = self.committed.untracked_overlay_rows().await?;
        let active_owner = canonical_branch_id(self.committed.branch_id().as_str())?;
        let global_owner = canonical_branch_id(crate::GLOBAL_BRANCH_ID)?;
        let staged_global = self.staged_untracked_rows_for_owner(global_owner, None, None);
        let staged_local = if active_owner == global_owner {
            Vec::new()
        } else {
            self.staged_untracked_rows_for_owner(active_owner, None, None)
        };
        Ok(merge_untracked_overlay_rows(
            committed,
            staged_global,
            staged_local,
            active_owner,
        ))
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
        let owner = CanonicalBranchId::from_bytes(*owner.as_bytes());
        let staged = self.staged_untracked_rows_for_owner(owner, lower, upper);
        Ok(
            merge_untracked_overlay_rows(committed, Vec::new(), staged, owner)
                .into_iter()
                .take(limit.unwrap_or(usize::MAX))
                .collect(),
        )
    }

    /// Resolves the global+branch untracked range through the committed
    /// retained view, then applies this transaction's staged untracked rows
    /// as an ordered overlay. Staged tombstones suppress committed values
    /// before visibility and LIMIT, matching exact-point semantics.
    pub(crate) async fn untracked_overlay_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
        include_tombstones: bool,
    ) -> Result<Vec<UntrackedStateRow>, LixError> {
        let active_owner = canonical_branch_id(self.committed.branch_id().as_str())?;
        let global_owner = canonical_branch_id(crate::GLOBAL_BRANCH_ID)?;
        let committed_local = self
            .committed
            .untracked_owner_range(active_owner, lower, upper, None)
            .await?;
        let committed_global = if active_owner == global_owner {
            Vec::new()
        } else {
            self.committed
                .untracked_owner_range(global_owner, lower, upper, None)
                .await?
        };
        let staged_global = self.staged_untracked_rows_for_owner(global_owner, lower, upper);
        let staged_local = if active_owner == global_owner {
            Vec::new()
        } else {
            self.staged_untracked_rows_for_owner(active_owner, lower, upper)
        };
        let effective_local = merge_untracked_owner_overlay(committed_local, staged_local);
        let effective_global = merge_untracked_owner_overlay(committed_global, staged_global);
        let rows = merge_untracked_owner_rows(effective_global, effective_local)
            .into_iter()
            .filter(|row| include_tombstones || !row.value.cell.deleted())
            .take(limit.unwrap_or(usize::MAX))
            .collect();
        Ok(rows)
    }

    /// Resolves one bounded untracked overlay range for an explicit branch,
    /// retaining the transaction's staged overlay when that branch is the
    /// transaction branch and otherwise using only the committed retained
    /// view. The merge remains bounded to the requested range.
    pub(crate) async fn untracked_overlay_branch_range_for_branch(
        &self,
        branch_id: &str,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
        include_tombstones: bool,
    ) -> Result<Vec<UntrackedStateRow>, LixError> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        if self.committed.branch_id_matches(branch_id)? {
            return self
                .untracked_overlay_range(lower, upper, limit, include_tombstones)
                .await;
        }
        let target = canonical_branch_id(branch_id)?;
        let global = canonical_branch_id(crate::GLOBAL_BRANCH_ID)?;
        let (staged_global, staged_local) = if self.committed.branch_id_matches(branch_id)? {
            if target == global {
                (
                    Vec::new(),
                    self.staged_untracked_rows_for_owner(target, lower, upper),
                )
            } else {
                (
                    self.staged_untracked_rows_for_owner(global, lower, upper),
                    self.staged_untracked_rows_for_owner(target, lower, upper),
                )
            }
        } else {
            (Vec::new(), Vec::new())
        };
        let staged_empty = staged_global.is_empty() && staged_local.is_empty();
        let committed = self
            .committed
            .untracked_overlay_branch_range_for_branch(
                branch_id,
                lower,
                upper,
                staged_empty.then_some(limit).flatten(),
                staged_empty && include_tombstones,
            )
            .await?;
        Ok(
            merge_untracked_overlay_rows(committed, staged_global, staged_local, target)
                .into_iter()
                .filter(|row| include_tombstones || !row.value.cell.deleted())
                .take(limit.unwrap_or(usize::MAX))
                .collect(),
        )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_row(owner: CanonicalBranchId, value: &str) -> UntrackedStateRow {
        UntrackedStateRow {
            owner,
            key: StateKey {
                schema_key: "app.overlay".into(),
                file_id: None,
                entity_pk: EntityPk::single("same"),
            },
            value: UntrackedValue {
                created_at: LixTimestamp::expect_parse("created_at", "2026-01-01T00:00:00.000Z"),
                updated_at: LixTimestamp::expect_parse("updated_at", "2026-01-01T00:00:00.001Z"),
                cell: StateCell::Value(value.into()),
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: Vec::new(),
            },
        }
    }

    #[test]
    fn staged_global_does_not_mask_committed_local() {
        let local = CanonicalBranchId::from_bytes([1; 16]);
        let global = CanonicalBranchId::from_bytes([2; 16]);
        let merged = merge_untracked_overlay_rows(
            vec![test_row(local, "committed-local")],
            vec![test_row(global, "staged-global")],
            Vec::new(),
            local,
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].owner, local);
        assert_eq!(
            merged[0].value.cell,
            StateCell::Value("committed-local".into())
        );
    }

    #[test]
    fn staged_local_wins_over_staged_global_and_tombstone_masks() {
        let local = CanonicalBranchId::from_bytes([1; 16]);
        let global = CanonicalBranchId::from_bytes([2; 16]);
        let merged = merge_untracked_overlay_rows(
            Vec::new(),
            vec![test_row(global, "staged-global")],
            vec![UntrackedStateRow {
                value: UntrackedValue {
                    cell: StateCell::Tombstone,
                    ..test_row(local, "unused").value
                },
                ..test_row(local, "unused")
            }],
            local,
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].owner, local);
        assert!(merged[0].value.cell.deleted());
    }

    #[test]
    fn owner_staging_precedes_local_over_global_for_exact_and_range() {
        let local = CanonicalBranchId::from_bytes([1; 16]);
        let global = CanonicalBranchId::from_bytes([2; 16]);
        let local_tombstone = UntrackedStateRow {
            value: UntrackedValue {
                cell: StateCell::Tombstone,
                ..test_row(local, "unused").value
            },
            ..test_row(local, "unused")
        };
        let staged_global = test_row(global, "staged-global");
        let committed_global = test_row(global, "committed-global");

        let effective_local = merge_untracked_owner_overlay(
            vec![test_row(local, "committed-local")],
            vec![local_tombstone.clone()],
        );
        let effective_global =
            merge_untracked_owner_overlay(vec![committed_global], vec![staged_global]);
        let exact = effective_local
            .first()
            .cloned()
            .or_else(|| effective_global.first().cloned());
        assert_eq!(exact.as_ref().map(|row| row.owner), Some(local));
        assert!(exact.is_some_and(|row| row.value.cell.deleted()));

        let range = merge_untracked_owner_rows(effective_global, effective_local);
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].owner, local);
        assert!(range[0].value.cell.deleted());
    }
}
