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
    VisibleStateRow,
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
    pub(crate) fn seed_logical_snapshot(
        &self,
        _active_branch_id: &str,
    ) -> Result<Option<crate::common::SharedStr>, LixError> {
        let key = crate::forktree::decode_state_key(&self.key)?;
        let global = match self.source {
            StateRowSource::Global => true,
            StateRowSource::Branch | StateRowSource::StagedBranch => false,
            StateRowSource::StagedGlobal => true,
        };
        self.value.cell.seed_logical_text(&key, global)
    }

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
            source: if row.global {
                StateRowSource::StagedGlobal
            } else {
                StateRowSource::StagedBranch
            },
        }
    }
}

/// Provenance of a row after the committed/staged overlay is resolved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StateRowSource {
    Global,
    Branch,
    StagedGlobal,
    StagedBranch,
}

/// One already-authenticated staged state cell. Rows must be supplied in
/// strict canonical key order; the transaction overlay never builds a map.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StagedStateRow {
    pub(crate) key: Vec<u8>,
    pub(crate) value: StateValue,
    pub(crate) global: bool,
}

impl StagedStateRow {
    pub(crate) fn new(key: Vec<u8>, value: StateValue, global: bool) -> Self {
        Self { key, value, global }
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

    pub(crate) async fn live_count(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<u64, StorageError> {
        self.view.live_count(lower, upper).await
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
                .map_err(Into::into);
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
        let branch_id = canonical_branch_id(branch_id)?;
        if self.view.branch_id() == branch_id {
            return self
                .range(lower, upper, limit, include_tombstones)
                .await
                .map_err(Into::into);
        }
        self.view
            .branch_view(branch_id)
            .await?
            .range(lower, upper, limit, include_tombstones)
            .await
            .map(|rows| rows.into_iter().map(StateRow::from_committed).collect())
            .map_err(LixError::from)
    }

    /// Resolves disjoint ranges from one branch through a single retained
    /// authenticated traversal. Active-branch requests reuse this exact view;
    /// alternate branches borrow one coherent view on the same storage read.
    pub(crate) async fn branch_ranges(
        &self,
        branch_id: &str,
        ranges: &[(Vec<u8>, Option<Vec<u8>>)],
        include_tombstones: bool,
    ) -> Result<Vec<Vec<StateRow>>, LixError> {
        let branch_id = canonical_branch_id(branch_id)?;
        let rows = if self.view.branch_id() == branch_id {
            self.view.ranges(ranges, include_tombstones).await?
        } else {
            self.view
                .branch_view(branch_id)
                .await?
                .ranges(ranges, include_tombstones)
                .await?
        };
        Ok(rows
            .into_iter()
            .map(|rows| rows.into_iter().map(StateRow::from_committed).collect())
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
    // Native tree diffs are ordered by durable encoded bytes
    // (schema, entity_pk, file_id). `StateKey::Ord` follows struct field order
    // (schema, file_id, entity_pk), so it is not an authority for this merge.
    // Encode each input exactly once and preserve the two linear streams.
    let encode = |key: &StateKey| {
        crate::forktree::encode_state_key(crate::forktree::StateKeyRef {
            schema_key: &key.schema_key,
            file_id: key.file_id.as_deref(),
            entity_pk: &key.entity_pk,
        })
    };
    let mut left = left
        .into_iter()
        .map(|key| (encode(&key), key))
        .peekable();
    let mut right = right
        .into_iter()
        .map(|key| (encode(&key), key))
        .peekable();
    let mut merged = Vec::new();
    loop {
        match (left.peek(), right.peek()) {
            (None, None) => break,
            (Some(_), None) => merged.push(left.next().expect("peeked left key").1),
            (None, Some(_)) => merged.push(right.next().expect("peeked right key").1),
            (Some((left_encoded, _)), Some((right_encoded, _))) => {
                match left_encoded.cmp(right_encoded) {
                Ordering::Less => merged.push(left.next().expect("peeked left key").1),
                Ordering::Greater => merged.push(right.next().expect("peeked right key").1),
                Ordering::Equal => {
                    merged.push(left.next().expect("peeked left key").1);
                    right.next();
                }
                }
            }
        }
    }
    merged
}

#[cfg(test)]
mod key_order_tests {
    use super::{StateKey, merge_sorted_state_keys};
    use crate::entity_pk::EntityPk;

    fn key(entity_pk: &str, file_id: &str) -> StateKey {
        StateKey {
            schema_key: "app.row".to_owned(),
            file_id: Some(file_id.to_owned()),
            entity_pk: EntityPk::single(entity_pk),
        }
    }

    #[test]
    fn root_diff_merge_uses_canonical_pk_before_file_order_and_dedupes() {
        let duplicate = key("a", "z-file");
        let merged = merge_sorted_state_keys(
            vec![duplicate.clone(), key("c", "a-file")],
            vec![duplicate, key("b", "b-file")],
        );
        assert_eq!(
            merged,
            vec![key("a", "z-file"), key("b", "b-file"), key("c", "a-file")]
        );
    }
}

/// One transaction's committed retained view plus an ordered staged overlay.
/// Staged rows are validated once at construction; points and ranges use
/// linear overlay merges and never materialize a full-key map.
#[derive(Clone)]
pub(crate) struct TransactionStateView<R> {
    committed: ForkTreeStateView<R>,
    staged: Vec<StagedStateRow>,
    removed_local_ranges: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl<R> TransactionStateView<R>
where
    R: StorageAdapterRead,
{
    pub(crate) async fn live_count_if_unmodified(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<Option<u64>, StorageError> {
        let staged_in_range = self.staged.iter().any(|row| {
            !lower.is_some_and(|lower| row.key.as_slice() < lower)
                && !upper.is_some_and(|upper| row.key.as_slice() >= upper)
        });
        let removed_overlap = self.removed_local_ranges.iter().any(|(removed_lower, removed_upper)| {
            !upper.is_some_and(|upper| removed_lower.as_slice() >= upper)
                && !removed_upper
                    .as_deref()
                    .is_some_and(|removed_upper| lower.is_some_and(|lower| removed_upper <= lower))
        });
        if staged_in_range || removed_overlap {
            return Ok(None);
        }
        self.committed.live_count(lower, upper).await.map(Some)
    }

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
        let snapshot = row
            .seed_logical_snapshot(crate::GLOBAL_BRANCH_ID)?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("account '{account_id}' has no snapshot"),
                )
            })?;
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
        let removed_local_ranges = collection_delete_ranges(&staged, &committed.branch_id())?;
        Ok(Self {
            committed,
            staged,
            removed_local_ranges,
        })
    }

    /// Rebinds the same retained committed view to the latest transaction
    /// buffer snapshot. This is the only mutable part of an explicit
    /// transaction read: no new storage read or committed-only facade is
    /// acquired between statements.
    pub(crate) fn with_staged_rows(&self, staged: Vec<StagedStateRow>) -> Result<Self, LixError>
    where
        R: Clone,
    {
        Self::new(self.committed.clone(), staged)
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
                committed_row.and_then(|row| {
                    (!self.removes_committed_row(&row))
                        .then(|| visible_committed_row(row, include_tombstones))
                        .flatten()
                })
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
                    (!self.removes_committed_row(&row))
                        .then(|| visible_committed_row(row, include_tombstones))
                        .flatten()
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

    fn removes_committed_row(&self, row: &StateRow) -> bool {
        row.source == StateRowSource::Branch
            && self.removed_local_ranges.iter().any(|(lower, upper)| {
                row.key.as_slice() >= lower.as_slice()
                    && upper
                        .as_ref()
                        .is_none_or(|upper| row.key.as_slice() < upper.as_slice())
            })
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

    /// Resolves disjoint ranges with the transaction's ordered staged overlay.
    /// Each range is independently merged in linear key order after one
    /// committed ForkTree traversal.
    pub(crate) async fn branch_ranges(
        &self,
        branch_id: &str,
        ranges: &[(Vec<u8>, Option<Vec<u8>>)],
        include_tombstones: bool,
    ) -> Result<Vec<Vec<StateRow>>, LixError> {
        if !self.committed.branch_id_matches(branch_id)? {
            return self
                .committed
                .branch_ranges(branch_id, ranges, include_tombstones)
                .await;
        }
        let committed = self
            .committed
            .branch_ranges(branch_id, ranges, true)
            .await?;
        Ok(ranges
            .iter()
            .zip(committed)
            .map(|((lower, upper), committed)| {
                let committed = committed
                    .into_iter()
                    .filter(|row| !self.removes_committed_row(row))
                    .collect();
                merge_staged_range(
                    committed,
                    &self.staged,
                    lower,
                    upper.as_deref(),
                    include_tombstones,
                )
            })
            .collect())
    }
}

fn collection_delete_ranges(
    staged: &[StagedStateRow],
    _active_branch_id: &str,
) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>, LixError> {
    let mut ranges = Vec::new();
    for row in staged {
        let key = crate::forktree::decode_state_key(&row.key)?;
        if key.schema_key != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY {
            continue;
        }
        let Some(snapshot) = row.value.cell.seed_logical_text(
            &key,
            row.global,
        )? else {
            continue;
        };
        let value: serde_json::Value =
            serde_json::from_str(snapshot.as_str()).map_err(|error| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!("collection-generation marker snapshot is malformed: {error}"),
                )
            })?;
        if value.get("live_count").and_then(serde_json::Value::as_u64) != Some(0) {
            continue;
        }
        let (schema_key, file_id) =
            crate::collection_generation::collection_scope_from_entity_pk(&key.entity_pk)?;
        if file_id.is_some() {
            continue;
        }
        let bounds =
            crate::forktree::encode_state_entity_prefix_bounds(&schema_key, &EntityPk::empty());
        ranges.push((bounds.lower, bounds.upper));
    }
    ranges.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(ranges)
}

fn merge_staged_range(
    committed: Vec<StateRow>,
    staged: &[StagedStateRow],
    lower: &[u8],
    upper: Option<&[u8]>,
    include_tombstones: bool,
) -> Vec<StateRow> {
    let mut committed_index = 0;
    let mut staged_index = staged.partition_point(|row| row.key.as_slice() < lower);
    let mut output = Vec::new();
    while committed_index < committed.len() || staged_index < staged.len() {
        let committed_key = committed.get(committed_index).map(|row| row.key.as_slice());
        let staged_key = staged.get(staged_index).and_then(|row| {
            (!upper.is_some_and(|upper| row.key.as_slice() >= upper)).then_some(row.key.as_slice())
        });
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
                let row = visible_staged_row(&staged[staged_index], include_tombstones);
                committed_index += 1;
                staged_index += 1;
                row
            }
            Ordering::Greater => {
                let row = visible_staged_row(&staged[staged_index], include_tombstones);
                staged_index += 1;
                row
            }
        };
        if let Some(row) = row {
            output.push(row);
        }
    }
    output
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
