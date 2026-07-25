use std::{collections::BTreeSet, fmt, ops::Deref, sync::Arc};

use crate::LixError;
use crate::binary_cas::{BlobHash, BlobPayload, BlobSameLengthSplice};
use crate::catalog::SchemaPlanId;
use crate::changelog::{ChangeId, CommitId};
use crate::common::{LixTimestamp, MutationIdentity, RequestBlobSpliceProvenance};
use crate::entity_pk::EntityPk;
use crate::json_store::JsonRef;
use crate::live_state::MaterializedLiveStateRow;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransactionJson {
    value: Arc<JsonValue>,
    normalized: Arc<str>,
}

impl TransactionJson {
    pub(crate) fn from_value(value: JsonValue, context: &str) -> Result<Self, LixError> {
        let normalized: Arc<str> = serde_json::to_string(&value)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_UNKNOWN,
                    format!("{context} failed to serialize as normalized JSON: {error}"),
                )
            })?
            .into();
        Ok(Self {
            value: Arc::new(value),
            normalized,
        })
    }

    pub(crate) fn from_value_unchecked(value: JsonValue) -> Self {
        Self::from_value(value, "transaction JSON")
            .expect("serializing serde_json::Value should not fail")
    }

    #[cfg(test)]
    pub(crate) fn from_value_for_test(value: JsonValue) -> Self {
        Self::from_value(value, "test transaction JSON").expect("test JSON should normalize")
    }

    pub(crate) fn from_parts(value: Arc<JsonValue>, normalized: Arc<str>) -> Self {
        Self { value, normalized }
    }

    pub(crate) fn value(&self) -> &JsonValue {
        self.value.as_ref()
    }

    pub(crate) fn normalized(&self) -> &str {
        self.normalized.as_ref()
    }

    pub(crate) fn into_parts(self) -> (Arc<JsonValue>, Arc<str>) {
        (self.value, self.normalized)
    }
}

impl Deref for TransactionJson {
    type Target = JsonValue;

    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

impl PartialEq<JsonValue> for TransactionJson {
    fn eq(&self, other: &JsonValue) -> bool {
        self.value() == other
    }
}

impl PartialEq<TransactionJson> for JsonValue {
    fn eq(&self, other: &TransactionJson) -> bool {
        self == other.value()
    }
}

impl fmt::Display for TransactionJson {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.normalized())
    }
}

impl Serialize for TransactionJson {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TransactionJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = JsonValue::deserialize(deserializer)?;
        Self::from_value(value, "transaction JSON").map_err(serde::de::Error::custom)
    }
}

/// State row accepted at the transaction write boundary.
///
/// External SQL/provider code must parse any textual JSON before constructing
/// this type. The transaction receives `TransactionJson`, applies schema
/// defaults and identity derivation, then prepares JSON refs in
/// `PreparedStateRow` without serializing already-normalized JSON again.
///
/// SQL providers stage semantic rows, not final storage rows. INSERT providers
/// may omit defaulted snapshot fields and leave `entity_pk` unset when the
/// target schema has an `x-lix-primary-key`; transaction normalization applies
/// schema defaults and derives the final identity. Typed UPDATE providers must
/// stage full rewritten snapshots after applying column assignments to the
/// existing row.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TransactionWriteRow {
    pub(crate) entity_pk: Option<EntityPk>,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot: Option<TransactionJson>,
    pub(crate) metadata: Option<TransactionJson>,
    pub(crate) origin: Option<TransactionWriteOrigin>,
    pub(crate) created_at: Option<String>,
    pub(crate) updated_at: Option<String>,
    pub(crate) global: bool,
    pub(crate) change_id: Option<String>,
    pub(crate) commit_id: Option<String>,
    pub(crate) untracked: bool,
    pub(crate) branch_id: String,
}

impl TransactionWriteRow {
    pub(crate) fn schema_scope_branch_id(&self) -> &str {
        if self.global {
            crate::GLOBAL_BRANCH_ID
        } else {
            self.branch_id.as_str()
        }
    }
}

/// User-facing write operation that produced one physical staged row.
///
/// Composite SQL surfaces such as `lix_file` lower one logical row into
/// multiple state rows. The transaction layer owns final constraint validation,
/// but error messages should stay in the vocabulary of the logical operation
/// when the caller did not write the physical state schema directly.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TransactionWriteOrigin {
    pub(crate) surface: String,
    pub(crate) operation: TransactionWriteOperation,
    pub(crate) primary_key: Option<LogicalPrimaryKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum TransactionWriteOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogicalPrimaryKey {
    pub(crate) columns: Vec<String>,
    pub(crate) values: Vec<String>,
}

/// Incoming file payload paired with transaction write rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransactionFileData {
    pub(crate) file_id: String,
    pub(crate) path: Option<String>,
    pub(crate) filename: Option<String>,
    pub(crate) branch_id: String,
    pub(crate) global: bool,
    pub(crate) untracked: bool,
    /// Whether the visible pre-write file had a binary blob reference.
    ///
    /// File providers already know this while lowering an UPDATE. Carrying the
    /// fact through the transaction boundary avoids rediscovering it by
    /// scanning the filesystem during plugin reconciliation. Inserts and
    /// callers without a prior row leave this false.
    pub(crate) had_blob_ref: bool,
    /// Content hash of the visible pre-write blob when the SQL lowerer had it
    /// available without an additional read. This is transient write
    /// provenance only; it lets a verified v2 same-length edit reuse durable
    /// CAS chunk references at commit.
    base_blob_hash: Option<BlobHash>,
    /// A fixed-width replacement proven by the v2 transition against the
    /// exact accepted document. The complete output bytes remain authoritative;
    /// this only permits an internal CAS staging fast path.
    same_length_blob_splice: Option<BlobSameLengthSplice>,
    /// Validated transport splice that produced `payload`, when the ordinary
    /// SQL blob parameter arrived through the remote splice optimization.
    /// This is transient execution provenance and is never persisted as file
    /// or plugin state.
    splice_provenance: Option<RequestBlobSpliceProvenance>,
    /// Optional mutation identity supplied by execution metadata. It is
    /// transient; only a bounded reservation proof derived from it can become
    /// durable plugin state. The current HTTP protocol does not expose replay
    /// identity.
    mutation_identity: Option<MutationIdentity>,
    payload: BlobPayload,
    /// Content-addressed payloads produced while validating this file write.
    /// Plugin installation uses this for the extracted WASM component so
    /// steady-state reads can load it directly without reopening the archive.
    auxiliary_payloads: Vec<BlobPayload>,
}

impl TransactionFileData {
    pub(crate) fn new(
        file_id: String,
        path: Option<String>,
        filename: Option<String>,
        branch_id: String,
        global: bool,
        untracked: bool,
        data: impl Into<crate::Blob>,
    ) -> Self {
        Self {
            file_id,
            path,
            filename,
            branch_id,
            global,
            untracked,
            had_blob_ref: false,
            base_blob_hash: None,
            same_length_blob_splice: None,
            splice_provenance: None,
            mutation_identity: None,
            payload: BlobPayload::from_bytes(data),
            auxiliary_payloads: Vec::new(),
        }
    }

    pub(crate) fn with_had_blob_ref(mut self, had_blob_ref: bool) -> Self {
        self.had_blob_ref = had_blob_ref;
        self
    }

    pub(crate) fn with_base_blob_hash(mut self, base_blob_hash: Option<BlobHash>) -> Self {
        self.had_blob_ref |= base_blob_hash.is_some();
        self.base_blob_hash = base_blob_hash;
        self
    }

    pub(crate) fn set_splice_provenance(
        &mut self,
        splice_provenance: Option<RequestBlobSpliceProvenance>,
    ) {
        self.splice_provenance = splice_provenance;
    }

    pub(crate) fn splice_provenance(&self) -> Option<&RequestBlobSpliceProvenance> {
        self.splice_provenance.as_ref()
    }

    pub(crate) fn set_mutation_identity(&mut self, mutation_identity: Option<MutationIdentity>) {
        self.mutation_identity = mutation_identity;
    }

    pub(crate) fn mutation_identity(&self) -> Option<MutationIdentity> {
        self.mutation_identity
    }

    pub(crate) fn add_auxiliary_payload(&mut self, data: impl Into<crate::Blob>) {
        self.auxiliary_payloads.push(BlobPayload::from_bytes(data));
    }

    pub(crate) fn data(&self) -> &[u8] {
        self.payload.bytes()
    }

    pub(crate) fn replace_data(&mut self, data: impl Into<crate::Blob>) {
        self.payload = BlobPayload::from_bytes(data);
        // Transport provenance describes the replaced request payload. Once a
        // plugin renderer materializes merged bytes, it no longer applies.
        self.splice_provenance = None;
        self.base_blob_hash = None;
        self.same_length_blob_splice = None;
    }

    /// Marks a single fixed-width replacement that was independently verified
    /// by the v2 file transition. Invalid/unknown base state intentionally
    /// leaves this unset so CAS staging follows the normal full path.
    pub(crate) fn set_verified_same_length_blob_splice(
        &mut self,
        visible_base_blob_hash: BlobHash,
        offset: usize,
        length: usize,
    ) {
        let Some(base_blob_hash) = self.base_blob_hash else {
            return;
        };
        if base_blob_hash != visible_base_blob_hash {
            return;
        }
        if length == 0
            || offset
                .checked_add(length)
                .is_none_or(|end| end > self.payload.len())
        {
            return;
        }
        self.same_length_blob_splice =
            Some(BlobSameLengthSplice::new(base_blob_hash, offset, length));
    }

    pub(crate) fn blob_hash(&self) -> Option<BlobHash> {
        self.payload.hash()
    }

    pub(crate) fn len(&self) -> usize {
        self.payload.len()
    }

    pub(crate) fn payload(&self) -> &BlobPayload {
        &self.payload
    }

    pub(crate) fn same_length_blob_splice(&self) -> Option<BlobSameLengthSplice> {
        self.same_length_blob_splice
    }

    #[cfg(test)]
    pub(crate) fn base_blob_hash(&self) -> Option<BlobHash> {
        self.base_blob_hash
    }

    pub(crate) fn auxiliary_payloads(&self) -> &[BlobPayload] {
        &self.auxiliary_payloads
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }
}

/// One decoded write batch accepted by the transaction boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TransactionWrite {
    Rows {
        mode: TransactionWriteMode,
        rows: Vec<TransactionWriteRow>,
    },
    RowsWithFileData {
        mode: TransactionWriteMode,
        rows: Vec<TransactionWriteRow>,
        file_data: Vec<TransactionFileData>,
        count: u64,
    },
}

/// One decoded write batch after semantic normalization and JSON preparation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparedTransactionWrite {
    Rows {
        mode: TransactionWriteMode,
        rows: Vec<PreparedStateRow>,
    },
    RowsWithFileData {
        mode: TransactionWriteMode,
        rows: Vec<PreparedStateRow>,
        file_data: Vec<TransactionFileData>,
        count: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionWriteMode {
    Insert,
    Replace,
}

/// Result returned after the transaction accepts a write batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransactionWriteOutcome {
    pub(crate) count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StageJson {
    pub(crate) value: Arc<serde_json::Value>,
    pub(crate) normalized: Arc<str>,
    pub(crate) json_ref: JsonRef,
}

impl StageJson {
    pub(crate) fn materialize(&self) -> String {
        self.normalized.as_ref().to_string()
    }

    /// Whether this payload inlines into values instead of the json store.
    pub(crate) fn is_inline(&self) -> bool {
        self.normalized.len() <= crate::json_store::JSON_INLINE_MAX_BYTES
    }

    pub(crate) fn slot_ref(&self) -> crate::json_store::JsonSlotRef<'_> {
        if self.is_inline() {
            crate::json_store::JsonSlotRef::Inline(&self.normalized)
        } else {
            crate::json_store::JsonSlotRef::Ref(&self.json_ref)
        }
    }
}

#[expect(clippy::unnecessary_wraps)]
pub(crate) fn stage_json_from_value(
    value: TransactionJson,
    _context: &str,
) -> Result<StageJson, LixError> {
    let (value, normalized) = value.into_parts();
    let json_ref = JsonRef::for_content(normalized.as_bytes());
    Ok(StageJson {
        value,
        normalized,
        json_ref,
    })
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PreparedRowFacts {
    /// Row-local schema, metadata, and primary-key validation completed
    /// against `schema_plan_id` during semantic normalization.
    ///
    /// Commit validation still owns transaction-wide and committed-state
    /// constraints. Keeping this certificate on the prepared row prevents the
    /// immutable JSON payload from being validated a second time at commit.
    pub(crate) row_content_validated: bool,
    /// This prepared operation participates in a cross-row constraint that
    /// requires transaction-wide or committed-state validation.
    pub(crate) requires_transaction_validation: bool,
}

/// Prepared state row owned by the transaction write buffer.
///
/// This is the first boundary that owns `StageJson`: JSON has been normalized
/// and assigned a content-addressed `JsonRef`. Durable placement belongs to the
/// JSON store at batch staging time, not row preparation time.
/// Storage owners must receive only the ref-backed row forms derived from this
/// type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedStateRow {
    pub(crate) schema_plan_id: SchemaPlanId,
    pub(crate) facts: PreparedRowFacts,
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot: Option<StageJson>,
    pub(crate) metadata: Option<StageJson>,
    pub(crate) origin: Option<TransactionWriteOrigin>,
    pub(crate) origin_key: Option<String>,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) global: bool,
    pub(crate) change_id: Option<ChangeId>,
    pub(crate) commit_id: Option<CommitId>,
    pub(crate) untracked: bool,
    pub(crate) branch_id: String,
}

impl From<PreparedStateRow> for MaterializedLiveStateRow {
    fn from(row: PreparedStateRow) -> Self {
        let deleted = row.snapshot.is_none();
        Self {
            entity_pk: row.entity_pk,
            schema_key: row.schema_key,
            file_id: row.file_id,
            snapshot_content: row.snapshot.map(|snapshot| snapshot.materialize()),
            metadata: row.metadata.map(|metadata| metadata.materialize()),
            deleted,
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            change_id: row.change_id,
            commit_id: row.commit_id,
            untracked: row.untracked,
            branch_id: row.branch_id.into(),
        }
    }
}

impl From<&PreparedStateRow> for MaterializedLiveStateRow {
    fn from(row: &PreparedStateRow) -> Self {
        Self {
            entity_pk: row.entity_pk.clone(),
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            snapshot_content: row.snapshot.as_ref().map(StageJson::materialize),
            metadata: row.metadata.as_ref().map(StageJson::materialize),
            deleted: row.snapshot.is_none(),
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            change_id: row.change_id,
            commit_id: row.commit_id,
            untracked: row.untracked,
            branch_id: row.branch_id.clone().into(),
        }
    }
}

/// Transaction-local commit change refs accumulated while rows are staged.
///
/// Final commit row materialization owns commit ids, parent heads, and commit
/// row timestamps. Staging only tracks which hydrated tracked changes the
/// future commit introduces for a branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedCommitChangeRefs {
    pub(crate) commit_id: CommitId,
    pub(crate) commit_change_id: ChangeId,
    /// Every normal branch-head publication has a distinct public
    /// `lixcol_change_id`, even though v6 stores the moving ref directly
    /// rather than as a flat current row.
    pub(crate) branch_ref_change_id: ChangeId,
    pub(crate) created_at: LixTimestamp,
    /// Normal prepared rows are already the authoritative commit-membership
    /// sequence. Keeping every fresh row change id in another BTreeSet only
    /// duplicates 10k+ UUIDs on bulk commits, so staging tracks their count.
    pub(crate) tracked_change_count: usize,
    /// Selected historical refs are a distinct, irregular path and need
    /// change-id deduplication before they are appended beside row changes.
    selected_change_ids: BTreeSet<ChangeId>,
    pub(crate) selected_change_refs: Vec<StagedCommitChangeRef>,
    pub(crate) allow_empty: bool,
}

impl Default for StagedCommitChangeRefs {
    fn default() -> Self {
        Self {
            commit_id: CommitId::default(),
            commit_change_id: ChangeId::default(),
            branch_ref_change_id: ChangeId::default(),
            created_at: LixTimestamp::expect_parse("created_at", "1970-01-01T00:00:00.000Z"),
            tracked_change_count: 0,
            selected_change_ids: BTreeSet::new(),
            selected_change_refs: Vec::new(),
            allow_empty: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedCommitChangeRef {
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) entity_pk: EntityPk,
    pub(crate) change_id: ChangeId,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
}

impl StagedCommitChangeRefs {
    pub(crate) fn new(
        commit_id: CommitId,
        commit_change_id: ChangeId,
        branch_ref_change_id: ChangeId,
        created_at: LixTimestamp,
    ) -> Self {
        Self {
            commit_id,
            commit_change_id,
            branch_ref_change_id,
            created_at,
            tracked_change_count: 0,
            selected_change_ids: BTreeSet::new(),
            selected_change_refs: Vec::new(),
            allow_empty: false,
        }
    }

    pub(crate) fn add_change_id(&mut self, _change_id: ChangeId) {
        self.tracked_change_count += 1;
    }

    pub(crate) fn add_change_count(&mut self, count: usize) {
        self.tracked_change_count += count;
    }

    pub(crate) fn add_selected_change_ref(&mut self, change_ref: StagedCommitChangeRef) {
        if self.selected_change_ids.insert(change_ref.change_id) {
            self.selected_change_refs.push(change_ref);
        }
    }

    pub(crate) fn remove_change_id(&mut self, _change_id: &ChangeId) {
        self.tracked_change_count = self
            .tracked_change_count
            .checked_sub(1)
            .expect("staged tracked change count must not underflow");
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tracked_change_count == 0 && self.selected_change_refs.is_empty()
    }

    pub(crate) fn allow_empty(&mut self) {
        self.allow_empty = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verified_same_length_splice_requires_the_visible_blob_base() {
        let base = BlobHash::from_content(b"before");
        let wrong_base = BlobHash::from_content(b"other!");
        let mut write = TransactionFileData::new(
            "file".to_string(),
            None,
            None,
            "main".to_string(),
            false,
            false,
            b"after!".to_vec(),
        )
        .with_base_blob_hash(Some(base));

        write.set_verified_same_length_blob_splice(wrong_base, 2, 1);
        assert_eq!(
            write.same_length_blob_splice(),
            None,
            "a same-sized but different visible blob must not donate chunk references"
        );

        write.set_verified_same_length_blob_splice(base, 2, 1);
        assert_eq!(
            write.same_length_blob_splice(),
            Some(BlobSameLengthSplice::new(base, 2, 1))
        );
    }
}
