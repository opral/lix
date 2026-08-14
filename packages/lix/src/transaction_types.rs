use std::{
    collections::{HashMap, HashSet},
    fmt,
    io::{self, Write as _},
    ops::Deref,
    sync::{Arc, OnceLock},
};

use crate::LixError;
use crate::binary_cas::{
    BlobEditSplice, BlobId, BlobPayload, BlobSameLengthSplice, BlobWriteReceipt,
};
use crate::catalog::SchemaPlanId;
use crate::changelog::{ChangeId, CommitId};
use crate::common::{LixTimestamp, MutationIdentity, RequestBlobSpliceProvenance, SharedStr};
use crate::row_pk::RowPk;
use crate::functions::FunctionProviderHandle;
use crate::hot_state::{CertifiedCurrentStatePredecessor, MaterializedHotStateRow};
use crate::json_store::JsonRef;
use crate::tracked_state::OrderedAddressableCommitDeltaStage;
use crate::plugin::runtime::{WasmCanonicalJson, WasmCanonicalJsonCertificateRef, WasmCertifiedRowBatch};
use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone)]
pub(crate) struct TransactionJson {
    storage: TransactionJsonStorage,
}

#[derive(Debug, Clone)]
enum TransactionJsonStorage {
    Decoded {
        value: Arc<JsonValue>,
        normalized: OnceLock<Arc<str>>,
    },
    #[cfg_attr(not(test), allow(dead_code))]
    Certified {
        normalized: Arc<str>,
    },
    CertifiedShared {
        normalized: SharedStr,
        certificate: TransactionJsonCertificate,
    },
    CanonicalShared {
        value: OnceLock<Arc<JsonValue>>,
        normalized: SharedStr,
    },
    CanonicalBatch(WasmCanonicalJson),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionJsonCertificate {
    RowContent,
    Metadata,
}

impl TransactionJson {
    pub(crate) fn from_value(value: JsonValue, context: &str) -> Result<Self, LixError> {
        Self::from_shared_value(Arc::new(value), context)
    }

    pub(crate) fn from_shared_value(
        value: Arc<JsonValue>,
        context: &str,
    ) -> Result<Self, LixError> {
        let _ = context;
        Ok(Self {
            storage: TransactionJsonStorage::Decoded {
                value,
                normalized: OnceLock::new(),
            },
        })
    }

    pub(crate) fn from_value_unchecked(value: JsonValue) -> Self {
        Self::from_value(value, "transaction JSON")
            .expect("serializing serde_json::Value should not fail")
    }

    #[cfg(feature = "storage-benches")]
    pub(crate) fn from_shared_value_unchecked(value: Arc<JsonValue>) -> Self {
        Self::from_shared_value(value, "transaction JSON")
            .expect("serializing serde_json::Value should not fail")
    }

    #[cfg(test)]
    pub(crate) fn from_value_for_test(value: JsonValue) -> Self {
        Self::from_value(value, "test transaction JSON").expect("test JSON should normalize")
    }

    /// Reuses one row of a bounded canonical JSON cursor-page batch.
    ///
    /// The parsed value column and normalized byte arena remain owned once by
    /// the batch. Moving this handle through transaction preparation and
    /// staging clones neither representation.
    pub(crate) fn from_canonical_batch(value: WasmCanonicalJson) -> Self {
        Self {
            storage: TransactionJsonStorage::CanonicalBatch(value),
        }
    }

    /// Constructs canonical row content whose schema semantics and identity
    /// were proven by a typed lowerer.
    ///
    /// The decoded JSON stays lazy because ordinary staging and durable
    /// placement only consume canonical bytes. Callers may issue this
    /// certificate only for complete replacement rows whose unchanged
    /// identity and row-local schema constraints were already established.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn from_certified_normalized_row_content(normalized: Arc<str>) -> Self {
        Self {
            storage: TransactionJsonStorage::Certified { normalized },
        }
    }

    /// Retains plan-bound certified canonical JSON in a shared buffer.
    ///
    /// This constructor is reserved for callers that have proven row-local
    /// schema semantics and identity against the transaction's current plan.
    pub(crate) fn from_certified_shared_normalized_row_content(normalized: SharedStr) -> Self {
        Self {
            storage: TransactionJsonStorage::CertifiedShared {
                normalized,
                certificate: TransactionJsonCertificate::RowContent,
            },
        }
    }

    /// Constructs certified row content as zero-copy views over one canonical
    /// UTF-8 arena.
    ///
    /// Typed engine producers have already proven row identity and schema
    /// semantics before reaching this boundary. Retaining those rows directly
    /// avoids constructing a decoded/certificate transport batch only for
    /// transaction normalization to immediately dismantle it again.
    pub(crate) fn from_certified_row_content_arena(
        normalized: Vec<u8>,
        offsets: Vec<(usize, usize)>,
    ) -> Result<Vec<Self>, LixError> {
        let invalid_arena = |message| LixError::new(LixError::CODE_INVALID_PARAM, message);
        let arena = SharedStr::from_utf8(Bytes::from(normalized))
            .map_err(|_| invalid_arena("certified transaction JSON arena is not UTF-8"))?;
        Self::from_certified_row_content_shared_arena(arena, offsets)
    }

    /// Constructs certified row content from a UTF-8 arena assembled from
    /// already-valid string fragments by an engine-owned producer.
    ///
    /// Offsets remain checked for full, contiguous coverage and UTF-8 scalar
    /// boundaries. Only the redundant complete-buffer validation pass is
    /// skipped.
    /// # Safety
    ///
    /// `normalized` must contain valid UTF-8. Offset coverage and character
    /// boundaries are still checked before any row view is constructed.
    pub(crate) unsafe fn from_validated_certified_row_content_arena(
        normalized: Vec<u8>,
        offsets: Vec<(usize, usize)>,
    ) -> Result<Vec<Self>, LixError> {
        // SAFETY: upheld by the caller contract above.
        let arena = unsafe { SharedStr::from_utf8_unchecked(Bytes::from(normalized)) };
        Self::from_certified_row_content_shared_arena(arena, offsets)
    }

    fn from_certified_row_content_shared_arena(
        arena: SharedStr,
        offsets: Vec<(usize, usize)>,
    ) -> Result<Vec<Self>, LixError> {
        let invalid_arena = |message| LixError::new(LixError::CODE_INVALID_PARAM, message);
        let arena_len = u32::try_from(arena.len())
            .map_err(|_| invalid_arena("certified transaction JSON arena exceeds u32"))?;
        let mut previous_end = 0_u32;
        let mut rows = Vec::with_capacity(offsets.len());
        for (start, end) in offsets {
            let start = u32::try_from(start)
                .map_err(|_| invalid_arena("certified transaction JSON row offset exceeds u32"))?;
            let end = u32::try_from(end)
                .map_err(|_| invalid_arena("certified transaction JSON row offset exceeds u32"))?;
            if start != previous_end || end < start || end > arena_len {
                return Err(invalid_arena(
                    "certified transaction JSON arena offsets are invalid or non-contiguous",
                ));
            }
            let normalized = arena.slice(start as usize..end as usize).ok_or_else(|| {
                invalid_arena("certified transaction JSON arena offsets split a UTF-8 scalar")
            })?;
            rows.push(Self::from_certified_shared_normalized_row_content(
                normalized,
            ));
            previous_end = end;
        }
        if previous_end != arena_len {
            return Err(invalid_arena(
                "certified transaction JSON arena offsets do not cover the arena",
            ));
        }
        Ok(rows)
    }

    /// Retains canonical engine-owned metadata whose semantic validity is
    /// established by the typed producer.
    pub(crate) fn from_certified_shared_normalized_metadata(normalized: SharedStr) -> Self {
        Self {
            storage: TransactionJsonStorage::CertifiedShared {
                normalized,
                certificate: TransactionJsonCertificate::Metadata,
            },
        }
    }

    /// Retains canonical JSON bytes whose schema semantics have not been
    /// proven against the transaction's current catalog.
    ///
    /// Historical merge/pick payloads use this representation: normalization
    /// parses the shared bytes lazily at most once for current-plan validation,
    /// while unchanged canonical bytes continue into staging without a second
    /// serialization or row-owned copy.
    pub(crate) fn from_unvalidated_shared_normalized_content(normalized: SharedStr) -> Self {
        Self {
            storage: TransactionJsonStorage::CanonicalShared {
                value: OnceLock::new(),
                normalized,
            },
        }
    }

    pub(crate) fn row_content_certified(&self) -> bool {
        matches!(self.storage, TransactionJsonStorage::Certified { .. })
            || matches!(
                self.storage,
                TransactionJsonStorage::CertifiedShared {
                    certificate: TransactionJsonCertificate::RowContent,
                    ..
                }
            )
    }

    pub(crate) fn metadata_content_certified(&self) -> bool {
        matches!(
            self.storage,
            TransactionJsonStorage::CertifiedShared {
                certificate: TransactionJsonCertificate::Metadata,
                ..
            }
        )
    }

    /// Revokes a row-content proof while retaining its canonical bytes.
    ///
    /// A typed frontend can certify against its pinned catalog before an
    /// explicit transaction stages a schema amendment. In that case the
    /// transport remains useful, but transaction normalization must decode
    /// and validate it against the amended plan.
    fn revoke_row_content_certificate(self) -> Self {
        match self.storage {
            TransactionJsonStorage::CertifiedShared {
                normalized,
                certificate: TransactionJsonCertificate::RowContent,
            } => Self::from_unvalidated_shared_normalized_content(normalized),
            TransactionJsonStorage::Certified { normalized } => {
                Self::from_unvalidated_shared_normalized_content(normalized.as_ref().into())
            }
            storage => Self { storage },
        }
    }

    pub(crate) fn canonical_batch_certificate(
        &self,
    ) -> Option<WasmCanonicalJsonCertificateRef<'_>> {
        match &self.storage {
            TransactionJsonStorage::CanonicalBatch(value) => value.certificate(),
            TransactionJsonStorage::Decoded { .. }
            | TransactionJsonStorage::Certified { .. }
            | TransactionJsonStorage::CertifiedShared { .. }
            | TransactionJsonStorage::CanonicalShared { .. } => None,
        }
    }

    pub(crate) fn canonical_batch_normalized_shared(&self) -> Option<SharedStr> {
        match &self.storage {
            TransactionJsonStorage::CanonicalBatch(value) => Some(value.normalized_shared()),
            TransactionJsonStorage::Decoded { .. }
            | TransactionJsonStorage::Certified { .. }
            | TransactionJsonStorage::CertifiedShared { .. }
            | TransactionJsonStorage::CanonicalShared { .. } => None,
        }
    }

    pub(crate) fn requires_batch_canonicalization(&self) -> bool {
        matches!(self.storage, TransactionJsonStorage::Decoded { .. })
    }

    pub(crate) fn value(&self) -> &JsonValue {
        match &self.storage {
            TransactionJsonStorage::Decoded { value, .. } => value.as_ref(),
            TransactionJsonStorage::CanonicalBatch(value) => value.value(),
            TransactionJsonStorage::CanonicalShared { value, normalized } => value
                .get_or_init(|| {
                    Arc::new(
                        serde_json::from_str(normalized.as_str())
                            .expect("shared canonical transaction JSON must remain valid JSON"),
                    )
                })
                .as_ref(),
            TransactionJsonStorage::Certified { .. }
            | TransactionJsonStorage::CertifiedShared { .. } => {
                panic!("certified transaction JSON must be prepared before decoding is requested")
            }
        }
    }

    pub(crate) fn normalized(&self) -> &str {
        match &self.storage {
            TransactionJsonStorage::Decoded { value, normalized } => normalized
                .get_or_init(|| {
                    serde_json::to_string(value.as_ref())
                        .expect("serializing serde_json::Value should not fail")
                        .into()
                })
                .as_ref(),
            TransactionJsonStorage::Certified { normalized } => normalized.as_ref(),
            TransactionJsonStorage::CertifiedShared { normalized, .. } => normalized.as_str(),
            TransactionJsonStorage::CanonicalShared { normalized, .. } => normalized.as_str(),
            TransactionJsonStorage::CanonicalBatch(value) => value.normalized(),
        }
    }

    #[cfg(test)]
    pub(crate) fn canonical_batch_row(&self) -> Option<&WasmCanonicalJson> {
        match &self.storage {
            TransactionJsonStorage::CanonicalBatch(value) => Some(value),
            TransactionJsonStorage::Decoded { .. }
            | TransactionJsonStorage::Certified { .. }
            | TransactionJsonStorage::CertifiedShared { .. }
            | TransactionJsonStorage::CanonicalShared { .. } => None,
        }
    }

    /// Materializes a mutable value only for a semantic rewrite such as
    /// applying a missing schema default. The batch-backed happy path never
    /// calls this method.
    pub(crate) fn into_value_for_mutation(self) -> JsonValue {
        match self.storage {
            TransactionJsonStorage::Decoded { value, .. } => {
                Arc::try_unwrap(value).unwrap_or_else(|value| value.as_ref().clone())
            }
            TransactionJsonStorage::CanonicalBatch(value) => value.value().clone(),
            TransactionJsonStorage::CanonicalShared { value, normalized } => {
                let value = value.into_inner().unwrap_or_else(|| {
                    Arc::new(
                        serde_json::from_str(normalized.as_str())
                            .expect("shared canonical transaction JSON must remain valid JSON"),
                    )
                });
                Arc::try_unwrap(value).unwrap_or_else(|value| value.as_ref().clone())
            }
            TransactionJsonStorage::Certified { .. }
            | TransactionJsonStorage::CertifiedShared { .. } => {
                panic!("certified transaction JSON must bypass semantic normalization")
            }
        }
    }
}

impl PartialEq for TransactionJson {
    fn eq(&self, other: &Self) -> bool {
        match (&self.storage, &other.storage) {
            (
                TransactionJsonStorage::Decoded { value: left, .. },
                TransactionJsonStorage::Decoded { value: right, .. },
            ) => left == right,
            (
                TransactionJsonStorage::CanonicalBatch(left),
                TransactionJsonStorage::CanonicalBatch(right),
            ) => left.normalized() == right.normalized(),
            _ => self.normalized() == other.normalized(),
        }
    }
}

impl Eq for TransactionJson {}

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
        self.value().serialize(serializer)
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
/// defaults and identity derivation, then prepares JSON refs directly in a
/// `PreparedStateBatch` without serializing already-normalized JSON again.
///
/// SQL providers stage semantic rows, not final storage rows. INSERT providers
/// may omit defaulted snapshot fields and leave `row_pk` unset when the
/// target schema has a `primary_key`; transaction normalization applies
/// schema defaults and derives the final identity. Typed UPDATE providers must
/// stage full rewritten snapshots after applying column assignments to the
/// existing row.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TransactionWriteRow {
    pub(crate) row_pk: Option<RowPk>,
    pub(crate) schema_key: SharedStr,
    pub(crate) file_id: Option<SharedStr>,
    pub(crate) snapshot: Option<TransactionJson>,
    pub(crate) metadata: Option<TransactionJson>,
    pub(crate) origin: Option<TransactionWriteOrigin>,
    pub(crate) created_at: Option<String>,
    pub(crate) updated_at: Option<String>,
    pub(crate) global: bool,
    pub(crate) change_id: Option<String>,
    pub(crate) commit_id: Option<String>,
    pub(crate) untracked: bool,
    pub(crate) branch_id: SharedStr,
}

const RAW_WRITE_NONE: u32 = u32::MAX;
const RAW_WRITE_GLOBAL: u8 = 1;
const RAW_WRITE_UNTRACKED: u8 = 1 << 1;
const RAW_WRITE_CONSTRAINTS_UNCHANGED: u8 = 1 << 2;

/// Compact row topology for an incoming transaction write batch.
///
/// Variable-width owners live in typed columns or batch dictionaries. Slots
/// contain only ordinals and flags, so repeated schema, file, branch, origin,
/// timestamp, and identifier values are retained once per batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RawWriteSlot {
    schema_key: u32,
    file_id: u32,
    origin: u32,
    created_at: u32,
    updated_at: u32,
    change_id: u32,
    commit_id: u32,
    branch_id: u32,
    flags: u8,
}

/// Mutable Arrow-style ingress representation shared by SQL and plugin writes.
///
/// Row identities and JSON payloads are aligned typed columns because
/// normalization mutates them in place. Repeated strings and origins use
/// dictionary ordinals. `TransactionJson` values retain canonical page arenas,
/// so moving, selecting, or extracting rows clones neither parsed snapshots
/// nor normalized byte buffers.
#[derive(Debug, Clone)]
pub(crate) struct RawWriteBatch {
    slots: Vec<RawWriteSlot>,
    row_pks: Vec<Option<RowPk>>,
    snapshots: Vec<Option<TransactionJson>>,
    metadata: Vec<Option<TransactionJson>>,
    durable_predecessors: Vec<Option<CertifiedCurrentStatePredecessor>>,
    strings: Vec<SharedStr>,
    string_index: Option<HashMap<SharedStr, u32>>,
    expected_rows: usize,
    origins: Vec<TransactionWriteOrigin>,
    origin_index: Option<HashMap<TransactionWriteOrigin, u32>>,
    /// Exact homogeneous row-normalization proof produced by a typed
    /// frontend. Any operation that changes row contents clears this proof.
    certified_preparation: Option<CertifiedRawWriteBatchPreparation>,
    #[cfg(test)]
    string_promotions: usize,
    #[cfg(test)]
    origin_promotions: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CertifiedRawWriteBatchPreparation {
    pub(crate) schema_plan_id: SchemaPlanId,
    pub(crate) facts: PreparedRowFacts,
    pub(crate) tracked_keys_strictly_ordered: bool,
    pub(crate) complete_collection_replacement: Option<CompleteCollectionReplacementProof>,
}

/// Ephemeral proof used to construct an authoritative immutable replacement
/// manifest. Absence means an ordinary mutation batch; there is no separate
/// legacy certification bit or partially-populated proof state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompleteCollectionReplacementProof {
    pub(crate) ordered_identity_digest: [u8; 32],
    pub(crate) replay_bytes: u64,
}

/// Frontend-neutral columns for one certified ordered replacement journal.
///
/// SQL, plugins, and programmatic APIs can produce this carrier without
/// allocating row-owned transaction DTOs. The transaction assigns lifecycle
/// and commit identity while retaining these arenas as its immutable journal.
/// Mixed mutation lanes are rejected instead of lowering this representation
/// into generic prepared rows.
pub(crate) struct TypedMutationJournalBatch {
    pub(crate) schema_plan_id: SchemaPlanId,
    pub(crate) schema_key: SharedStr,
    pub(crate) branch_id: SharedStr,
    pub(crate) identity_arena: Vec<u8>,
    pub(crate) identity_offsets: Vec<(usize, usize)>,
    pub(crate) snapshot_arena: Vec<u8>,
    pub(crate) snapshot_offsets: Vec<(usize, usize)>,
    pub(crate) expected_ordered_identity_digest: [u8; 32],
}

impl TypedMutationJournalBatch {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        schema_plan_id: SchemaPlanId,
        schema_key: SharedStr,
        branch_id: SharedStr,
        identity_arena: Vec<u8>,
        identity_offsets: Vec<(usize, usize)>,
        snapshot_arena: Vec<u8>,
        snapshot_offsets: Vec<(usize, usize)>,
        expected_ordered_identity_digest: [u8; 32],
    ) -> Result<Self, LixError> {
        if identity_offsets.is_empty() || identity_offsets.len() != snapshot_offsets.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "typed mutation journal columns are empty or misaligned",
            ));
        }
        Ok(Self {
            schema_plan_id,
            schema_key,
            branch_id,
            identity_arena,
            identity_offsets,
            snapshot_arena,
            snapshot_offsets,
            expected_ordered_identity_digest,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.identity_offsets.len()
    }
}

/// Dense, typed columns produced by certified SQL parameter-batch paths.
///
/// Keeping this transport separate from [`RawWriteBatch`] avoids allocating
/// raw nullable/system columns only for transaction preparation to dismantle
/// them immediately. The transaction either lowers these columns directly or
/// explicitly converts them to raw rows when a transaction-local schema
/// change invalidates the certificate.
pub(crate) struct CertifiedParameterBatch {
    row_pks: Vec<RowPk>,
    snapshots: Vec<TransactionJson>,
    durable_predecessors: Vec<Option<CertifiedCurrentStatePredecessor>>,
    schema_key: SharedStr,
    branch_id: SharedStr,
    untracked: bool,
    certificate: CertifiedRawWriteBatchPreparation,
    row_columnar: Option<crate::sql2::EncodedRowGroups>,
}

impl CertifiedParameterBatch {
    pub(crate) fn new(
        row_pks: Vec<RowPk>,
        snapshots: Vec<TransactionJson>,
        schema_key: SharedStr,
        branch_id: SharedStr,
        certificate: CertifiedRawWriteBatchPreparation,
    ) -> Result<Self, LixError> {
        Self::new_with_lane(
            row_pks,
            snapshots,
            schema_key,
            branch_id,
            false,
            certificate,
        )
    }

    pub(crate) fn new_with_lane(
        row_pks: Vec<RowPk>,
        snapshots: Vec<TransactionJson>,
        schema_key: SharedStr,
        branch_id: SharedStr,
        untracked: bool,
        certificate: CertifiedRawWriteBatchPreparation,
    ) -> Result<Self, LixError> {
        if row_pks.len() != snapshots.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified replacement columns are not aligned",
            ));
        }
        if row_pks.len() >= RAW_WRITE_NONE as usize {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "certified replacement row count exceeds u32",
            ));
        }
        Ok(Self {
            durable_predecessors: std::iter::repeat_with(|| None)
                .take(row_pks.len())
                .collect(),
            row_pks,
            snapshots,
            schema_key,
            branch_id,
            untracked,
            certificate,
            row_columnar: None,
        })
    }

    pub(crate) fn with_row_columnar(
        mut self,
        row_columnar: crate::sql2::EncodedRowGroups,
    ) -> Self {
        self.row_columnar = Some(row_columnar);
        self
    }

    pub(crate) fn len(&self) -> usize {
        self.row_pks.len()
    }

    pub(crate) fn schema_scope_branch_id(&self) -> &str {
        self.branch_id.as_str()
    }

    pub(crate) fn schema_key(&self) -> &str {
        self.schema_key.as_str()
    }

    pub(crate) fn untracked(&self) -> bool {
        self.untracked
    }

    pub(crate) fn set_durable_predecessor(
        &mut self,
        index: usize,
        value: Option<CertifiedCurrentStatePredecessor>,
    ) {
        self.durable_predecessors[index] = value;
    }

    pub(crate) fn into_raw(self) -> Result<RawWriteBatch, LixError> {
        let mut rows = RawWriteBatch::from_certified_parameter_rows(
            self.row_pks,
            self.snapshots,
            self.schema_key,
            self.branch_id,
            self.untracked,
            self.certificate,
        )?;
        for (index, predecessor) in self.durable_predecessors.into_iter().enumerate() {
            rows.set_durable_predecessor(index, predecessor);
        }
        Ok(rows)
    }

    pub(crate) fn into_dense_prepared(
        self,
        origin_key: Option<&SharedStr>,
        timestamp: LixTimestamp,
    ) -> Result<PreparedStateBatch, LixError> {
        // The dense certified representation is tracked-only *by construction*,
        // and this assertion is where that invariant is stated. Fixing the two
        // projections below to agree on the lane does not make the dense lane
        // correct for untracked rows: it derives every change id from the
        // commit-delta address space and reports `addressable_change_id: true`
        // unconditionally, neither of which holds for untracked state. The
        // routing guard lives at `sql2::exec::bound_public_write`
        // (`dense_lane_supports_batch`), which sends untracked certified
        // batches to the raw lane before they can reach this constructor.
        //
        // That guard is a call-site predicate one edit away from deletion, so
        // restate it here at the construction boundary — where a future caller
        // would actually violate it — rather than leaving it to a comment.
        // Verified to hold today: this assertion was run as a hard `assert!`
        // over the full CI-scope suite with zero hits.
        debug_assert!(
            !self.untracked,
            "the dense certified lane is tracked-only; untracked certified \
             batches take the raw lane"
        );
        self.into_dense_prepared_timestamps(origin_key, DenseParameterTimestamps::Scalar(timestamp))
    }

    fn into_dense_prepared_timestamps(
        self,
        origin_key: Option<&SharedStr>,
        timestamps: DenseParameterTimestamps,
    ) -> Result<PreparedStateBatch, LixError> {
        let Self {
            row_pks,
            snapshots,
            durable_predecessors,
            schema_key,
            branch_id,
            untracked,
            certificate,
            row_columnar,
        } = self;
        let row_count = row_pks.len();
        let predecessor_count = durable_predecessors
            .iter()
            .filter(|value| value.is_some())
            .count();
        let dense_durable_predecessors = if predecessor_count == row_count && row_count != 0 {
            Some(
                durable_predecessors
                    .iter()
                    .cloned()
                    .map(|value| value.expect("every certified predecessor is present"))
                    .collect::<Vec<_>>()
                    .into(),
            )
        } else {
            None
        };
        let (mut strings, schema_key_ordinal, branch_id_ordinal) = if schema_key == branch_id {
            (vec![schema_key], 0_u32, 0_u32)
        } else {
            (vec![schema_key, branch_id], 0_u32, 1_u32)
        };
        let origin_key = origin_key.map(|value| {
            if let Some(ordinal) = strings.iter().position(|candidate| candidate == value) {
                return u32::try_from(ordinal)
                    .expect("certified replacement string dictionary must fit u32");
            }
            let ordinal = u32::try_from(strings.len())
                .expect("certified replacement string dictionary must fit u32");
            strings.push(value.clone());
            ordinal
        });
        let mut json = Vec::with_capacity(row_count);
        for snapshot in snapshots {
            json.push(stage_json_from_value(
                snapshot,
                "certified parameter snapshot_content",
            )?);
        }
        let string_index = strings
            .iter()
            .cloned()
            .enumerate()
            .map(|(ordinal, value)| {
                (
                    value,
                    u32::try_from(ordinal)
                        .expect("certified replacement string dictionary must fit u32"),
                )
            })
            .collect();
        let mut prepared = PreparedStateBatch {
            slots: Vec::new(),
            dense_certified_parameter: Some(DenseCertifiedParameterSlots {
                len: row_count,
                schema_plan_id: certificate.schema_plan_id,
                facts: certificate.facts,
                schema_key: schema_key_ordinal,
                origin_key,
                timestamps,
                commit_id: None,
                branch_id: branch_id_ordinal,
                untracked,
                direct_change_ids: None,
                row_columnar,
                durable_predecessors: dense_durable_predecessors,
            }),
            row_pks,
            strings,
            string_index,
            json,
            durable_predecessors: Vec::new(),
            origins: Vec::new(),
            origin_index: HashMap::new(),
            origin_column_sets: Vec::new(),
            origin_column_index: HashMap::new(),
            certified_tracked_keys_strictly_ordered: certificate.tracked_keys_strictly_ordered,
            complete_collection_replacement: certificate.complete_collection_replacement,
            staged_index_values: StagedIndexValues::default(),
        };
        if predecessor_count != 0 && predecessor_count != row_count {
            for (index, predecessor) in durable_predecessors.into_iter().enumerate() {
                prepared.set_durable_predecessor(index, predecessor);
            }
        }
        Ok(prepared)
    }
}

pub(crate) type CertifiedParameterInsertBatch = CertifiedParameterBatch;
pub(crate) type CertifiedParameterReplacementBatch = CertifiedParameterBatch;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RawWriteRowRef<'a> {
    pub(crate) row_pk: Option<&'a RowPk>,
    pub(crate) schema_key: &'a SharedStr,
    pub(crate) file_id: Option<&'a SharedStr>,
    pub(crate) snapshot: Option<&'a TransactionJson>,
    pub(crate) metadata: Option<&'a TransactionJson>,
    pub(crate) origin: Option<&'a TransactionWriteOrigin>,
    pub(crate) created_at: Option<&'a str>,
    pub(crate) updated_at: Option<&'a str>,
    pub(crate) global: bool,
    pub(crate) change_id: Option<&'a str>,
    pub(crate) commit_id: Option<&'a str>,
    pub(crate) untracked: bool,
    pub(crate) constraints_unchanged: bool,
    pub(crate) branch_id: &'a SharedStr,
}

impl RawWriteRowRef<'_> {
    pub(crate) fn schema_scope_branch_id(&self) -> &str {
        if self.global {
            crate::GLOBAL_BRANCH_ID
        } else {
            self.branch_id.as_str()
        }
    }
}

#[cfg(feature = "storage-benches")]
fn record_raw_row_ownership(
    row_pk: Option<&RowPk>,
    schema_key: &SharedStr,
    file_id: Option<&SharedStr>,
    snapshot: Option<&TransactionJson>,
    metadata: Option<&TransactionJson>,
    created_at: Option<&SharedStr>,
    updated_at: Option<&SharedStr>,
    change_id: Option<&SharedStr>,
    commit_id: Option<&SharedStr>,
    branch_id: &SharedStr,
) {
    let key_bytes = row_pk.map_or(0, RowPk::estimated_heap_bytes)
        + schema_key.len()
        + file_id.map_or(0, |value| value.len())
        + created_at.map_or(0, |value| value.len())
        + updated_at.map_or(0, |value| value.len())
        + change_id.map_or(0, |value| value.len())
        + commit_id.map_or(0, |value| value.len())
        + branch_id.len();
    let value_bytes = snapshot.map_or(0, |value| value.normalized().len())
        + metadata.map_or(0, |value| value.normalized().len());
    crate::storage_bench::record_crud_ownership(
        crate::storage_bench::CRUD_OWNERSHIP_RAW_BATCH,
        1,
        key_bytes,
        value_bytes,
        5,
        1 + usize::from(file_id.is_some())
            + usize::from(created_at.is_some())
            + usize::from(updated_at.is_some())
            + usize::from(change_id.is_some())
            + usize::from(commit_id.is_some())
            + 1,
        2,
    );
}

#[cfg(feature = "storage-benches")]
fn record_prepared_row_ownership(
    row_pk: &RowPk,
    schema_key: &SharedStr,
    file_id: Option<&SharedStr>,
    snapshot: Option<&StageJson>,
    metadata: Option<&StageJson>,
    origin_key: Option<&SharedStr>,
    branch_id: &SharedStr,
    stage: usize,
) {
    let key_bytes = row_pk.estimated_heap_bytes()
        + schema_key.len()
        + file_id.map_or(0, |value| value.len())
        + origin_key.map_or(0, |value| value.len())
        + branch_id.len();
    let value_bytes = snapshot.map_or(0, |value| value.normalized().len())
        + metadata.map_or(0, |value| value.normalized().len());
    crate::storage_bench::record_crud_ownership(
        stage,
        1,
        key_bytes,
        value_bytes,
        6,
        2 + usize::from(file_id.is_some()) + usize::from(origin_key.is_some()),
        2,
    );
}

pub(crate) struct RawWriteRows<'a> {
    batch: &'a RawWriteBatch,
    range: std::ops::Range<usize>,
}

impl RawWriteBatch {
    pub(crate) fn new() -> Self {
        Self::with_capacity(0)
    }

    pub(crate) fn with_capacity(row_capacity: usize) -> Self {
        const INLINE_DICTIONARY_LIMIT: usize = 32;
        Self {
            slots: Vec::with_capacity(row_capacity),
            row_pks: Vec::with_capacity(row_capacity),
            snapshots: Vec::with_capacity(row_capacity),
            metadata: Vec::with_capacity(row_capacity),
            durable_predecessors: Vec::with_capacity(row_capacity),
            // File ids are the only commonly row-cardinal strings. Schema,
            // branch, timestamp, and origin metadata normally collapse to a
            // handful of dictionary entries.
            strings: Vec::with_capacity(row_capacity.min(INLINE_DICTIONARY_LIMIT)),
            string_index: None,
            expected_rows: row_capacity,
            origins: Vec::with_capacity(row_capacity.min(INLINE_DICTIONARY_LIMIT)),
            origin_index: None,
            certified_preparation: None,
            #[cfg(test)]
            string_promotions: 0,
            #[cfg(test)]
            origin_promotions: 0,
        }
    }

    fn from_certified_parameter_rows(
        row_pks: Vec<RowPk>,
        snapshots: Vec<TransactionJson>,
        schema_key: SharedStr,
        branch_id: SharedStr,
        untracked: bool,
        certificate: CertifiedRawWriteBatchPreparation,
    ) -> Result<Self, LixError> {
        if row_pks.len() != snapshots.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified parameter row columns are not aligned",
            ));
        }
        let row_count = row_pks.len();
        if row_count >= RAW_WRITE_NONE as usize {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "certified parameter row count exceeds u32",
            ));
        }
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_crud_ownership(
            crate::storage_bench::CRUD_OWNERSHIP_RAW_BATCH,
            row_count,
            row_pks
                .iter()
                .map(RowPk::estimated_heap_bytes)
                .sum::<usize>()
                + schema_key.len()
                + branch_id.len(),
            snapshots
                .iter()
                .map(|value| value.normalized().len())
                .sum::<usize>(),
            row_count.saturating_mul(5),
            2,
            2,
        );
        let (strings, schema_key_ordinal, branch_id_ordinal) = if schema_key == branch_id {
            (vec![schema_key], 0, 0)
        } else {
            (vec![schema_key, branch_id], 0, 1)
        };
        let slot = RawWriteSlot {
            schema_key: schema_key_ordinal,
            file_id: RAW_WRITE_NONE,
            origin: RAW_WRITE_NONE,
            created_at: RAW_WRITE_NONE,
            updated_at: RAW_WRITE_NONE,
            change_id: RAW_WRITE_NONE,
            commit_id: RAW_WRITE_NONE,
            branch_id: branch_id_ordinal,
            flags: if untracked { RAW_WRITE_UNTRACKED } else { 0 },
        };
        Ok(Self {
            slots: vec![slot; row_count],
            row_pks: row_pks.into_iter().map(Some).collect(),
            snapshots: snapshots.into_iter().map(Some).collect(),
            metadata: std::iter::repeat_with(|| None).take(row_count).collect(),
            durable_predecessors: std::iter::repeat_with(|| None).take(row_count).collect(),
            strings,
            string_index: None,
            expected_rows: row_count,
            origins: Vec::new(),
            origin_index: None,
            certified_preparation: Some(certificate),
            #[cfg(test)]
            string_promotions: 0,
            #[cfg(test)]
            origin_promotions: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn from_test_rows(rows: Vec<TransactionWriteRow>) -> Self {
        let mut batch = Self::with_capacity(rows.len());
        for row in rows {
            batch.push(row);
        }
        batch
    }

    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub(crate) fn row(&self, index: usize) -> RawWriteRowRef<'_> {
        self.get(index)
            .expect("raw write row index must be inside the batch")
    }

    pub(crate) fn get(&self, index: usize) -> Option<RawWriteRowRef<'_>> {
        let slot = self.slots.get(index)?;
        Some(RawWriteRowRef {
            row_pk: self.row_pks[index].as_ref(),
            schema_key: &self.strings[slot.schema_key as usize],
            file_id: self.optional_string(slot.file_id),
            snapshot: self.snapshots[index].as_ref(),
            metadata: self.metadata[index].as_ref(),
            origin: (slot.origin != RAW_WRITE_NONE).then(|| &self.origins[slot.origin as usize]),
            created_at: self.optional_string(slot.created_at).map(SharedStr::as_str),
            updated_at: self.optional_string(slot.updated_at).map(SharedStr::as_str),
            global: slot.flags & RAW_WRITE_GLOBAL != 0,
            change_id: self.optional_string(slot.change_id).map(SharedStr::as_str),
            commit_id: self.optional_string(slot.commit_id).map(SharedStr::as_str),
            untracked: slot.flags & RAW_WRITE_UNTRACKED != 0,
            constraints_unchanged: slot.flags & RAW_WRITE_CONSTRAINTS_UNCHANGED != 0,
            branch_id: &self.strings[slot.branch_id as usize],
        })
    }

    pub(crate) fn iter(&self) -> RawWriteRows<'_> {
        RawWriteRows {
            batch: self,
            range: 0..self.len(),
        }
    }

    pub(crate) fn push(&mut self, row: TransactionWriteRow) {
        self.certified_preparation = None;
        self.push_parts(
            row.row_pk,
            row.schema_key,
            row.file_id,
            row.snapshot,
            row.metadata,
            row.origin,
            row.created_at.map(SharedStr::from),
            row.updated_at.map(SharedStr::from),
            row.global,
            row.change_id.map(SharedStr::from),
            row.commit_id.map(SharedStr::from),
            row.untracked,
            row.branch_id,
        );
    }

    /// Certifies that the row just appended is a replacement whose primary
    /// key, unique-key, and foreign-key source properties are unchanged.
    pub(crate) fn mark_last_constraints_unchanged(&mut self) {
        let slot = self
            .slots
            .last_mut()
            .expect("constraint certificate requires an appended row");
        slot.flags |= RAW_WRITE_CONSTRAINTS_UNCHANGED;
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) fn push_parts(
        &mut self,
        row_pk: Option<RowPk>,
        schema_key: SharedStr,
        file_id: Option<SharedStr>,
        snapshot: Option<TransactionJson>,
        metadata: Option<TransactionJson>,
        origin: Option<TransactionWriteOrigin>,
        created_at: Option<SharedStr>,
        updated_at: Option<SharedStr>,
        global: bool,
        change_id: Option<SharedStr>,
        commit_id: Option<SharedStr>,
        untracked: bool,
        branch_id: SharedStr,
    ) {
        #[cfg(feature = "storage-benches")]
        record_raw_row_ownership(
            row_pk.as_ref(),
            &schema_key,
            file_id.as_ref(),
            snapshot.as_ref(),
            metadata.as_ref(),
            created_at.as_ref(),
            updated_at.as_ref(),
            change_id.as_ref(),
            commit_id.as_ref(),
            &branch_id,
        );
        self.certified_preparation = None;
        assert!(
            self.slots.len() < RAW_WRITE_NONE as usize,
            "raw write row count must fit a non-null u32 ordinal"
        );
        let schema_key = self.intern_string(schema_key);
        let file_id = self.intern_optional_string(file_id);
        let origin = self.intern_optional_origin(origin);
        let created_at = self.intern_optional_string(created_at);
        let updated_at = self.intern_optional_string(updated_at);
        let change_id = self.intern_optional_string(change_id);
        let commit_id = self.intern_optional_string(commit_id);
        let branch_id = self.intern_string(branch_id);
        let flags =
            (u8::from(global) * RAW_WRITE_GLOBAL) | (u8::from(untracked) * RAW_WRITE_UNTRACKED);
        self.slots.push(RawWriteSlot {
            schema_key,
            file_id,
            origin,
            created_at,
            updated_at,
            change_id,
            commit_id,
            branch_id,
            flags,
        });
        self.row_pks.push(row_pk);
        self.snapshots.push(snapshot);
        self.metadata.push(metadata);
        self.durable_predecessors.push(None);
        self.debug_assert_aligned();
    }

    pub(crate) fn append(&mut self, mut other: Self) {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_crud_ownership(
            crate::storage_bench::CRUD_OWNERSHIP_RAW_TRANSFER,
            other.len(),
            0,
            0,
            other.len().saturating_mul(5),
            0,
            0,
        );
        self.certified_preparation = None;
        self.reserve(other.len());
        for index in 0..other.len() {
            other.move_row_into(index, self);
        }
    }

    /// Moves one source ordinal into this batch without materializing a row DTO.
    ///
    /// The caller owns source ordinal selection, so the moved source slot is
    /// intentionally left as a hole and must not be read again.
    pub(crate) fn append_taken_row(&mut self, source: &mut Self, index: usize) {
        self.certified_preparation = None;
        source.move_row_into(index, self);
    }

    pub(crate) fn certified_preparation(&self) -> Option<CertifiedRawWriteBatchPreparation> {
        self.certified_preparation
    }

    /// Consumes a homogeneous certified ingress batch into its commit-ready
    /// typed columns.
    ///
    /// A certified producer has already established row shape, identity, and
    /// current-plan semantics. Re-reading every row through the generic
    /// preparation API only clones identities, re-interns the same schema and
    /// branch strings, and keeps both dense batches live at once. This lowering
    /// transfers the ingress dictionaries and aligned owners directly while
    /// constructing only the final fixed-width slots and staged JSON handles.
    pub(crate) fn into_certified_prepared(
        self,
        certificate: CertifiedRawWriteBatchPreparation,
        origin_key: Option<&SharedStr>,
        timestamp: LixTimestamp,
        functions: &FunctionProviderHandle,
    ) -> Result<PreparedStateBatch, LixError> {
        if self.certified_preparation != Some(certificate) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified transaction batch proof changed before direct preparation",
            ));
        }
        let RawWriteBatch {
            slots,
            row_pks,
            snapshots,
            metadata,
            durable_predecessors,
            mut strings,
            string_index: _,
            expected_rows: _,
            origins,
            origin_index: _,
            certified_preparation: _,
            #[cfg(test)]
                string_promotions: _,
            #[cfg(test)]
                origin_promotions: _,
        } = self;
        let row_count = slots.len();
        if row_pks.len() != row_count
            || snapshots.len() != row_count
            || metadata.len() != row_count
            || durable_predecessors.len() != row_count
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified transaction batch columns are not aligned",
            ));
        }
        if !origins.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified transaction batch unexpectedly contains logical origins",
            ));
        }
        if durable_predecessors.iter().any(Option::is_some) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified INSERT batch unexpectedly contains durable predecessors",
            ));
        }

        let mut string_index = HashMap::with_capacity(strings.len().saturating_add(1));
        for (ordinal, value) in strings.iter().cloned().enumerate() {
            string_index.insert(
                value,
                u32::try_from(ordinal)
                    .expect("certified transaction string dictionary must fit u32"),
            );
        }
        let origin_key = origin_key.map(|value| {
            if let Some(&ordinal) = string_index.get(value) {
                return ordinal;
            }
            let ordinal = u32::try_from(strings.len())
                .expect("certified transaction string dictionary must fit u32");
            strings.push(value.clone());
            string_index.insert(value.clone(), ordinal);
            ordinal
        });

        let mut prepared_slots = Vec::with_capacity(row_count);
        let mut prepared_row_pks = Vec::with_capacity(row_count);
        let mut json = Vec::with_capacity(row_count);
        for (row_index, (((slot, row_pk), snapshot), metadata)) in slots
            .into_iter()
            .zip(row_pks)
            .zip(snapshots)
            .zip(metadata)
            .enumerate()
        {
            if slot.file_id != RAW_WRITE_NONE
                || slot.origin != RAW_WRITE_NONE
                || slot.created_at != RAW_WRITE_NONE
                || slot.updated_at != RAW_WRITE_NONE
                || slot.change_id != RAW_WRITE_NONE
                || slot.commit_id != RAW_WRITE_NONE
                || slot.flags & !RAW_WRITE_UNTRACKED != 0
                || metadata.is_some()
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "certified transaction batch contains unsupported system columns",
                ));
            }
            let row_pk = row_pk.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "certified transaction row is missing row_pk",
                )
            })?;
            let snapshot = snapshot.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "certified transaction row is missing snapshot_content",
                )
            })?;
            let snapshot =
                stage_json_from_value(snapshot, "certified prepared row snapshot_content")?;
            prepared_row_pks.push(row_pk);
            json.push(snapshot);
            let untracked = slot.flags & RAW_WRITE_UNTRACKED != 0;
            // Tracked rows keep the nil placeholder: `addressable_change_id`
            // means commit planning replaces it with the row's commit-delta
            // address, so minting here would be a wasted UUID draw per row on
            // the flagship tracked path. Untracked rows are outside that
            // address space, so nothing would ever replace the placeholder —
            // they are the only rows that must be minted here.
            let change_id = if untracked {
                ChangeId::from(functions.call_uuid_v7())
            } else {
                ChangeId::default()
            };
            prepared_slots.push(PreparedStateSlot {
                schema_plan_id: certificate.schema_plan_id,
                facts: certificate.facts,
                row_pk: u32::try_from(row_index)
                    .expect("certified transaction row ordinal must fit u32"),
                schema_key: slot.schema_key,
                file_id: None,
                snapshot: Some(
                    u32::try_from(row_index)
                        .expect("certified transaction JSON ordinal must fit u32"),
                ),
                metadata: None,
                origin: None,
                origin_key,
                created_at: timestamp,
                updated_at: timestamp,
                global: false,
                change_id: Some(change_id),
                addressable_change_id: true,
                commit_id: None,
                untracked,
                branch_id: slot.branch_id,
                durable_predecessor: None,
            });
        }
        Ok(PreparedStateBatch {
            slots: prepared_slots,
            dense_certified_parameter: None,
            row_pks: prepared_row_pks,
            strings,
            string_index,
            json,
            durable_predecessors: Vec::new(),
            origins: Vec::new(),
            origin_index: HashMap::new(),
            origin_column_sets: Vec::new(),
            origin_column_index: HashMap::new(),
            certified_tracked_keys_strictly_ordered: certificate.tracked_keys_strictly_ordered,
            complete_collection_replacement: certificate.complete_collection_replacement,
            staged_index_values: StagedIndexValues::default(),
        })
    }

    /// Downgrades a batch proof to canonical transport after its schema plan
    /// has been invalidated by transaction-local catalog changes.
    pub(crate) fn revoke_certified_preparation(&mut self) {
        self.certified_preparation = None;
        for snapshot in &mut self.snapshots {
            if let Some(value) = snapshot.take() {
                *snapshot = Some(value.revoke_row_content_certificate());
            }
        }
    }

    pub(crate) fn reserve(&mut self, additional: usize) {
        const INLINE_DICTIONARY_LIMIT: usize = 32;
        self.expected_rows = self
            .expected_rows
            .max(self.len().saturating_add(additional));
        self.slots.reserve(additional);
        self.row_pks.reserve(additional);
        self.snapshots.reserve(additional);
        self.metadata.reserve(additional);
        self.durable_predecessors.reserve(additional);
        if let Some(index) = &mut self.string_index {
            // One row-cardinal file id plus the inline schema/branch/timestamp
            // set is the bulk-write happy path. Preserve headroom for those
            // shared entries so a zero-capacity append does not immediately
            // outgrow its one promotion near the end of the batch.
            let expected_strings = self.expected_rows.saturating_add(INLINE_DICTIONARY_LIMIT);
            let additional_strings = expected_strings.saturating_sub(self.strings.len());
            self.strings.reserve(additional_strings);
            index.reserve(expected_strings.saturating_sub(index.len()));
        }
        if let Some(index) = &mut self.origin_index {
            let additional_origins = self.expected_rows.saturating_sub(self.origins.len());
            self.origins.reserve(additional_origins);
            index.reserve(self.expected_rows.saturating_sub(index.len()));
        }
    }

    pub(crate) fn row_pk_mut(&mut self, index: usize) -> &mut Option<RowPk> {
        self.certified_preparation = None;
        &mut self.row_pks[index]
    }

    pub(crate) fn take_row_pk(&mut self, index: usize) -> Option<RowPk> {
        self.row_pks[index].take()
    }

    pub(crate) fn take_snapshot(&mut self, index: usize) -> Option<TransactionJson> {
        self.snapshots[index].take()
    }

    pub(crate) fn set_snapshot(&mut self, index: usize, value: Option<TransactionJson>) {
        self.certified_preparation = None;
        self.snapshots[index] = value;
    }

    pub(crate) fn take_metadata(&mut self, index: usize) -> Option<TransactionJson> {
        self.metadata[index].take()
    }

    pub(crate) fn take_durable_predecessor(
        &mut self,
        index: usize,
    ) -> Option<CertifiedCurrentStatePredecessor> {
        self.durable_predecessors[index].take()
    }

    pub(crate) fn set_durable_predecessor(
        &mut self,
        index: usize,
        value: Option<CertifiedCurrentStatePredecessor>,
    ) {
        self.durable_predecessors[index] = value;
    }

    pub(crate) fn snapshot_slots_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut Option<TransactionJson>> {
        self.snapshots.iter_mut()
    }

    pub(crate) fn metadata_slots_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut Option<TransactionJson>> {
        self.metadata.iter_mut()
    }

    pub(crate) fn set_origin(&mut self, index: usize, origin: Option<TransactionWriteOrigin>) {
        self.certified_preparation = None;
        let ordinal = self.intern_optional_origin(origin);
        self.slots[index].origin = ordinal;
    }

    pub(crate) fn set_change_id(&mut self, index: usize, change_id: Option<SharedStr>) {
        self.certified_preparation = None;
        let ordinal = self.intern_optional_string(change_id);
        self.slots[index].change_id = ordinal;
    }

    pub(crate) fn set_file_id(&mut self, index: usize, file_id: Option<SharedStr>) {
        self.certified_preparation = None;
        let ordinal = self.intern_optional_string(file_id);
        self.slots[index].file_id = ordinal;
    }

    /// Retains rows in source order and compacts the aligned mutable columns.
    ///
    /// Dictionaries remain shared until more than half their slots are dead;
    /// the uncommon compaction rebuild keeps repeated metadata interned.
    pub(crate) fn retain(&mut self, mut keep: impl FnMut(RawWriteRowRef<'_>) -> bool) {
        self.certified_preparation = None;
        let mut destination = 0usize;
        for source in 0..self.len() {
            if !keep(self.row(source)) {
                continue;
            }
            if destination != source {
                self.slots.swap(destination, source);
                self.row_pks.swap(destination, source);
                self.snapshots.swap(destination, source);
                self.metadata.swap(destination, source);
                self.durable_predecessors.swap(destination, source);
            }
            destination += 1;
        }
        self.slots.truncate(destination);
        self.row_pks.truncate(destination);
        self.snapshots.truncate(destination);
        self.metadata.truncate(destination);
        self.durable_predecessors.truncate(destination);
        self.debug_assert_aligned();
    }

    /// Moves selected source ordinals into a new typed batch and leaves holes.
    ///
    /// Reconciled mixed slots own the source order and never expose a moved
    /// ordinal again, so no row-owned sentinel is required.
    pub(crate) fn take_rows(&mut self, selected: &[usize]) -> Self {
        let mut extracted = Self::with_capacity(selected.len());
        for &index in selected {
            self.move_row_into(index, &mut extracted);
        }
        extracted
    }

    #[cfg(test)]
    pub(crate) fn into_rows(self) -> Vec<TransactionWriteRow> {
        let mut batch = self;
        let mut rows = Vec::with_capacity(batch.len());
        for index in 0..batch.len() {
            rows.push(batch.take_owned_row(index));
        }
        rows
    }

    #[cfg(test)]
    fn take_owned_row(&mut self, index: usize) -> TransactionWriteRow {
        let slot = self.slots[index];
        TransactionWriteRow {
            row_pk: self.row_pks[index].take(),
            schema_key: self.strings[slot.schema_key as usize].clone(),
            file_id: self.optional_string(slot.file_id).cloned(),
            snapshot: self.snapshots[index].take(),
            metadata: self.metadata[index].take(),
            origin: (slot.origin != RAW_WRITE_NONE)
                .then(|| self.origins[slot.origin as usize].clone()),
            created_at: self
                .optional_string(slot.created_at)
                .map(ToString::to_string),
            updated_at: self
                .optional_string(slot.updated_at)
                .map(ToString::to_string),
            global: slot.flags & RAW_WRITE_GLOBAL != 0,
            change_id: self
                .optional_string(slot.change_id)
                .map(ToString::to_string),
            commit_id: self
                .optional_string(slot.commit_id)
                .map(ToString::to_string),
            untracked: slot.flags & RAW_WRITE_UNTRACKED != 0,
            branch_id: self.strings[slot.branch_id as usize].clone(),
        }
    }

    fn move_row_into(&mut self, index: usize, destination: &mut Self) {
        let slot = self.slots[index];
        let durable_predecessor = self.durable_predecessors[index].take();
        destination.push_parts(
            self.row_pks[index].take(),
            self.strings[slot.schema_key as usize].clone(),
            self.optional_string(slot.file_id).cloned(),
            self.snapshots[index].take(),
            self.metadata[index].take(),
            (slot.origin != RAW_WRITE_NONE).then(|| self.origins[slot.origin as usize].clone()),
            self.optional_string(slot.created_at).cloned(),
            self.optional_string(slot.updated_at).cloned(),
            slot.flags & RAW_WRITE_GLOBAL != 0,
            self.optional_string(slot.change_id).cloned(),
            self.optional_string(slot.commit_id).cloned(),
            slot.flags & RAW_WRITE_UNTRACKED != 0,
            self.strings[slot.branch_id as usize].clone(),
        );
        let destination_index = destination.len() - 1;
        destination.set_durable_predecessor(destination_index, durable_predecessor);
    }

    fn intern_optional_string(&mut self, value: Option<SharedStr>) -> u32 {
        value
            .map(|value| self.intern_string(value))
            .unwrap_or(RAW_WRITE_NONE)
    }

    fn intern_string(&mut self, value: SharedStr) -> u32 {
        const INLINE_DICTIONARY_LIMIT: usize = 32;
        if let Some(index) = self
            .string_index
            .as_ref()
            .and_then(|index| index.get(&value))
        {
            return *index;
        }
        if self.string_index.is_none()
            && let Some(index) = self
                .strings
                .iter()
                .position(|candidate| candidate == &value)
        {
            return u32::try_from(index).expect("inline raw string ordinal must fit u32");
        }
        if self.string_index.is_none() && self.strings.len() == INLINE_DICTIONARY_LIMIT {
            let promoted_capacity = self
                .expected_rows
                .saturating_add(INLINE_DICTIONARY_LIMIT)
                .max(INLINE_DICTIONARY_LIMIT + 1);
            self.strings
                .reserve(promoted_capacity.saturating_sub(self.strings.len()));
            let mut index = HashMap::with_capacity(promoted_capacity);
            for (ordinal, existing) in self.strings.iter().cloned().enumerate() {
                index.insert(
                    existing,
                    u32::try_from(ordinal).expect("raw write string ordinal must fit u32"),
                );
            }
            self.string_index = Some(index);
            #[cfg(test)]
            {
                self.string_promotions += 1;
            }
        }
        let index =
            u32::try_from(self.strings.len()).expect("raw write string dictionary must fit u32");
        assert_ne!(
            index, RAW_WRITE_NONE,
            "raw write string dictionary must leave the null ordinal unused"
        );
        self.strings.push(value.clone());
        if let Some(string_index) = &mut self.string_index {
            string_index.insert(value, index);
        }
        index
    }

    fn intern_optional_origin(&mut self, value: Option<TransactionWriteOrigin>) -> u32 {
        const INLINE_DICTIONARY_LIMIT: usize = 32;
        let Some(value) = value else {
            return RAW_WRITE_NONE;
        };
        if let Some(index) = self
            .origin_index
            .as_ref()
            .and_then(|index| index.get(&value))
        {
            return *index;
        }
        if self.origin_index.is_none()
            && let Some(index) = self
                .origins
                .iter()
                .position(|candidate| candidate == &value)
        {
            return u32::try_from(index).expect("inline raw origin ordinal must fit u32");
        }
        if self.origin_index.is_none() && self.origins.len() == INLINE_DICTIONARY_LIMIT {
            let promoted_capacity = self.expected_rows.max(INLINE_DICTIONARY_LIMIT + 1);
            self.origins
                .reserve(promoted_capacity.saturating_sub(self.origins.len()));
            let mut index = HashMap::with_capacity(promoted_capacity);
            for (ordinal, existing) in self.origins.iter().cloned().enumerate() {
                index.insert(
                    existing,
                    u32::try_from(ordinal).expect("raw write origin ordinal must fit u32"),
                );
            }
            self.origin_index = Some(index);
            #[cfg(test)]
            {
                self.origin_promotions += 1;
            }
        }
        let index =
            u32::try_from(self.origins.len()).expect("raw write origin dictionary must fit u32");
        assert_ne!(
            index, RAW_WRITE_NONE,
            "raw write origin dictionary must leave the null ordinal unused"
        );
        self.origins.push(value.clone());
        if let Some(origin_index) = &mut self.origin_index {
            origin_index.insert(value, index);
        }
        index
    }

    fn optional_string(&self, ordinal: u32) -> Option<&SharedStr> {
        (ordinal != RAW_WRITE_NONE).then(|| &self.strings[ordinal as usize])
    }

    fn debug_assert_aligned(&self) {
        debug_assert_eq!(self.row_pks.len(), self.slots.len());
        debug_assert_eq!(self.snapshots.len(), self.slots.len());
        debug_assert_eq!(self.metadata.len(), self.slots.len());
        debug_assert_eq!(self.durable_predecessors.len(), self.slots.len());
    }

    #[cfg(test)]
    pub(crate) fn shared_string_count(&self) -> usize {
        self.strings.len()
    }

    #[cfg(test)]
    pub(crate) fn shared_origin_count(&self) -> usize {
        self.origins.len()
    }

    #[cfg(test)]
    pub(crate) fn aligned_owner_allocation_ptrs(&self) -> [*const (); 4] {
        [
            self.slots.as_ptr().cast(),
            self.row_pks.as_ptr().cast(),
            self.snapshots.as_ptr().cast(),
            self.metadata.as_ptr().cast(),
        ]
    }

    #[cfg(test)]
    pub(crate) fn aligned_owner_capacities(&self) -> [usize; 4] {
        [
            self.slots.capacity(),
            self.row_pks.capacity(),
            self.snapshots.capacity(),
            self.metadata.capacity(),
        ]
    }

    #[cfg(test)]
    pub(crate) fn string_dictionary_is_promoted(&self) -> bool {
        self.string_index.is_some()
    }

    #[cfg(test)]
    pub(crate) fn origin_dictionary_is_promoted(&self) -> bool {
        self.origin_index.is_some()
    }

    #[cfg(test)]
    pub(crate) fn dictionary_capacities(&self) -> [usize; 4] {
        [
            self.strings.capacity(),
            self.string_index.as_ref().map_or(0, HashMap::capacity),
            self.origins.capacity(),
            self.origin_index.as_ref().map_or(0, HashMap::capacity),
        ]
    }

    #[cfg(test)]
    pub(crate) fn dictionary_promotion_counts(&self) -> [usize; 2] {
        [self.string_promotions, self.origin_promotions]
    }
}

impl Default for RawWriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> Iterator for RawWriteRows<'a> {
    type Item = RawWriteRowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().and_then(|index| self.batch.get(index))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl DoubleEndedIterator for RawWriteRows<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.range
            .next_back()
            .and_then(|index| self.batch.get(index))
    }
}

impl ExactSizeIterator for RawWriteRows<'_> {}

impl<'a> IntoIterator for &'a RawWriteBatch {
    type Item = RawWriteRowRef<'a>;
    type IntoIter = RawWriteRows<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl PartialEq for RawWriteBatch {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other).all(|(left, right)| left == right)
    }
}

impl Eq for RawWriteBatch {}

impl PartialEq for RawWriteRowRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.row_pk == other.row_pk
            && self.schema_key == other.schema_key
            && self.file_id == other.file_id
            && self.snapshot == other.snapshot
            && self.metadata == other.metadata
            && self.origin == other.origin
            && self.created_at == other.created_at
            && self.updated_at == other.updated_at
            && self.global == other.global
            && self.change_id == other.change_id
            && self.commit_id == other.commit_id
            && self.untracked == other.untracked
            && self.constraints_unchanged == other.constraints_unchanged
            && self.branch_id == other.branch_id
    }
}

impl Eq for RawWriteRowRef<'_> {}

/// User-facing write operation that produced one physical staged row.
///
/// Composite SQL surfaces such as `lix_file` lower one logical row into
/// multiple state rows. The transaction layer owns final constraint validation,
/// but error messages should stay in the vocabulary of the logical operation
/// when the caller did not write the physical state schema directly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(crate) struct TransactionWriteOrigin {
    /// Logical write surface retained once and cheaply shared by every
    /// physical row produced for the operation.
    pub(crate) surface: SharedStr,
    pub(crate) operation: TransactionWriteOperation,
    /// Logical primary-key metadata retained once across every physical row
    /// lowered from the same public write.
    #[serde(
        serialize_with = "serialize_shared_logical_primary_key",
        deserialize_with = "deserialize_shared_logical_primary_key"
    )]
    pub(crate) primary_key: Option<Arc<LogicalPrimaryKey>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(crate) enum TransactionWriteOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogicalPrimaryKey {
    /// Invariant logical-key descriptor shared across every row in a batch.
    pub(crate) columns: Arc<[String]>,
    pub(crate) values: Vec<String>,
}

impl LogicalPrimaryKey {
    pub(crate) fn single_id(value: impl Into<String>) -> Self {
        static ID_COLUMNS: OnceLock<Arc<[String]>> = OnceLock::new();
        Self {
            columns: Arc::clone(ID_COLUMNS.get_or_init(|| vec!["id".to_string()].into())),
            values: vec![value.into()],
        }
    }
}

/// Reuses static storage for the public write surfaces on the bulk happy path.
pub(crate) fn shared_origin_surface(surface: &str) -> SharedStr {
    match surface {
        "lix_file" => SharedStr::from_static("lix_file"),
        "lix_directory" => SharedStr::from_static("lix_directory"),
        "lix_branch" => SharedStr::from_static("lix_branch"),
        "filesystem path parent" => SharedStr::from_static("filesystem path parent"),
        "plugin_reconciliation" => SharedStr::from_static("plugin_reconciliation"),
        _ => surface.into(),
    }
}

fn serialize_shared_logical_primary_key<S>(
    primary_key: &Option<Arc<LogicalPrimaryKey>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    primary_key.as_deref().serialize(serializer)
}

fn deserialize_shared_logical_primary_key<'de, D>(
    deserializer: D,
) -> Result<Option<Arc<LogicalPrimaryKey>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<LogicalPrimaryKey>::deserialize(deserializer)
        .map(|primary_key| primary_key.map(Arc::new))
}

/// File content accepted by the ordinary transaction write abstraction.
///
/// Prepared CAS content is already durable; publishing it is a metadata-only
/// transaction operation. Keeping that state in the content enum prevents a
/// prepared blob from masquerading as an empty inline payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FileContent {
    Inline(BlobPayload),
    PreparedCas(BlobWriteReceipt),
}

impl FileContent {
    pub(crate) fn inline(data: impl Into<crate::Blob>) -> Self {
        Self::Inline(BlobPayload::from_bytes(data))
    }

    pub(crate) fn blob_id(&self) -> Option<BlobId> {
        match self {
            Self::Inline(payload) => payload.hash(),
            Self::PreparedCas(receipt) => Some(receipt.hash),
        }
    }

    pub(crate) fn len(&self) -> u64 {
        match self {
            Self::Inline(payload) => payload.len() as u64,
            Self::PreparedCas(receipt) => receipt.size_bytes,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Inline(payload) => payload.is_empty(),
            Self::PreparedCas(receipt) => receipt.size_bytes == 0,
        }
    }

    pub(crate) fn inline_payload(&self) -> Option<&BlobPayload> {
        match self {
            Self::Inline(payload) => Some(payload),
            Self::PreparedCas(_) => None,
        }
    }

    pub(crate) fn inline_bytes(&self) -> Option<&[u8]> {
        self.inline_payload().map(BlobPayload::bytes)
    }
}

impl From<crate::Blob> for FileContent {
    fn from(data: crate::Blob) -> Self {
        Self::inline(data)
    }
}

impl From<Vec<u8>> for FileContent {
    fn from(data: Vec<u8>) -> Self {
        Self::inline(data)
    }
}

/// Incoming file content paired with transaction write rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransactionFileContent {
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
    base_blob_hash: Option<BlobId>,
    /// A fixed-width replacement proven by the v2 transition against the
    /// exact accepted document. The complete output bytes remain authoritative;
    /// this only permits an internal CAS staging fast path.
    same_length_blob_splice: Option<BlobSameLengthSplice>,
    edit_blob_splice: Option<BlobEditSplice>,
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
    content: FileContent,
    /// Content-addressed payloads produced while validating this file write.
    /// Plugin installation uses this for the extracted WASM component so
    /// steady-state reads can load it directly without reopening the archive.
    auxiliary_payloads: Vec<BlobPayload>,
    plugin_checkpoint: Option<PluginCheckpointWrite>,
    /// Reconciliation may retain this record only as the owner of certified
    /// semantic batches after its file payload has already been materialized
    /// through the ordinary plugin path.
    stage_payload_at_commit: bool,
    /// Certified v3 semantic owners which remain encoded through commit.
    certified_row_batches: Vec<WasmCertifiedRowBatch>,
}

impl TransactionFileContent {
    pub(crate) fn new(
        file_id: String,
        path: Option<String>,
        filename: Option<String>,
        branch_id: String,
        global: bool,
        untracked: bool,
        content: impl Into<FileContent>,
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
            edit_blob_splice: None,
            splice_provenance: None,
            mutation_identity: None,
            content: content.into(),
            auxiliary_payloads: Vec::new(),
            plugin_checkpoint: None,
            stage_payload_at_commit: true,
            certified_row_batches: Vec::new(),
        }
    }

    pub(crate) fn with_had_blob_ref(mut self, had_blob_ref: bool) -> Self {
        self.had_blob_ref = had_blob_ref;
        self
    }

    pub(crate) fn with_base_blob_hash(mut self, base_blob_hash: Option<BlobId>) -> Self {
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

    pub(crate) fn set_plugin_checkpoint(
        &mut self,
        generation: String,
        semantic_root: String,
        runtime: impl Into<crate::Blob>,
        authority: impl Into<crate::Blob>,
    ) {
        self.plugin_checkpoint = Some(PluginCheckpointWrite {
            generation,
            semantic_root,
            runtime: runtime.into(),
            authority: authority.into(),
        });
    }

    pub(crate) fn plugin_checkpoint(&self) -> Option<&PluginCheckpointWrite> {
        self.plugin_checkpoint.as_ref()
    }

    pub(crate) fn set_certified_row_batches(&mut self, batches: Vec<WasmCertifiedRowBatch>) {
        self.certified_row_batches = batches;
    }

    pub(crate) fn certified_row_batches(&self) -> &[WasmCertifiedRowBatch] {
        &self.certified_row_batches
    }

    pub(crate) fn retain_certified_batches_only(&mut self) {
        self.stage_payload_at_commit = false;
    }

    pub(crate) fn stage_payload_at_commit(&self) -> bool {
        self.stage_payload_at_commit && matches!(self.content, FileContent::Inline(_))
    }

    pub(crate) fn inline_data(&self) -> Option<&[u8]> {
        self.content.inline_bytes()
    }

    #[cfg(test)]
    pub(crate) fn content(&self) -> &[u8] {
        self.inline_data()
            .expect("test fixture expected inline file content")
    }

    pub(crate) fn replace_data(&mut self, data: impl Into<crate::Blob>) {
        self.content = FileContent::inline(data);
        // Transport provenance describes the replaced request payload. Once a
        // plugin renderer materializes merged bytes, it no longer applies.
        self.splice_provenance = None;
        self.base_blob_hash = None;
        self.same_length_blob_splice = None;
        self.edit_blob_splice = None;
    }

    /// Marks a single fixed-width replacement that was independently verified
    /// by the v2 file transition. Invalid/unknown base state intentionally
    /// leaves this unset so CAS staging follows the normal full path.
    pub(crate) fn set_verified_same_length_blob_splice(
        &mut self,
        visible_base_blob_hash: BlobId,
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
                .is_none_or(|end| end as u64 > self.content.len())
        {
            return;
        }
        self.same_length_blob_splice =
            Some(BlobSameLengthSplice::new(base_blob_hash, offset, length));
    }

    pub(crate) fn set_verified_blob_edit_splice(
        &mut self,
        visible_base_blob_hash: BlobId,
        offset: usize,
        delete_len: usize,
        insert_len: usize,
    ) {
        let Some(base_blob_hash) = self.base_blob_hash else {
            return;
        };
        if base_blob_hash != visible_base_blob_hash
            || (delete_len == 0 && insert_len == 0)
            || offset
                .checked_add(insert_len)
                .is_none_or(|end| end as u64 > self.content.len())
        {
            return;
        }
        self.edit_blob_splice = Some(BlobEditSplice {
            base_blob_hash,
            offset,
            delete_len,
            insert_len,
        });
    }

    pub(crate) fn blob_hash(&self) -> Option<BlobId> {
        self.content.blob_id()
    }

    pub(crate) fn len(&self) -> u64 {
        self.content.len()
    }

    pub(crate) fn inline_payload(&self) -> Option<&BlobPayload> {
        self.content.inline_payload()
    }

    pub(crate) fn same_length_blob_splice(&self) -> Option<BlobSameLengthSplice> {
        self.same_length_blob_splice
    }

    pub(crate) fn edit_blob_splice(&self) -> Option<BlobEditSplice> {
        self.edit_blob_splice
    }

    #[cfg(test)]
    pub(crate) fn base_blob_hash(&self) -> Option<BlobId> {
        self.base_blob_hash
    }

    pub(crate) fn auxiliary_payloads(&self) -> &[BlobPayload] {
        &self.auxiliary_payloads
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.content.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginCheckpointWrite {
    pub(crate) generation: String,
    pub(crate) semantic_root: String,
    pub(crate) runtime: crate::Blob,
    pub(crate) authority: crate::Blob,
}

/// One decoded write batch accepted by the transaction boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TransactionWrite {
    Rows {
        mode: TransactionWriteMode,
        rows: RawWriteBatch,
    },
    RowsWithFileContent {
        mode: TransactionWriteMode,
        rows: RawWriteBatch,
        file_content: Vec<TransactionFileContent>,
        count: u64,
    },
}

/// One decoded write batch after semantic normalization and JSON preparation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparedTransactionWrite {
    Rows {
        mode: TransactionWriteMode,
        rows: PreparedStateBatch,
    },
    RowsWithFileContent {
        mode: TransactionWriteMode,
        rows: PreparedStateBatch,
        file_content: Vec<TransactionFileContent>,
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

#[derive(Debug, Clone)]
pub(crate) struct StageJson {
    storage: StageJsonStorage,
    pub(crate) json_ref: JsonRef,
}

#[derive(Debug, Clone)]
enum StageJsonStorage {
    Owned {
        value: OnceLock<Arc<JsonValue>>,
        normalized: Arc<str>,
    },
    CertifiedShared {
        value: OnceLock<Arc<JsonValue>>,
        normalized: SharedStr,
    },
    /// Canonical bytes retained after transaction validation has completed.
    ///
    /// The parsed batch column is deliberately gone at this boundary. Commit
    /// materialization and storage lowering consume only the certified bytes,
    /// so making decoded access impossible prevents an accidental second parse.
    ValidatedShared {
        normalized: SharedStr,
    },
    /// Row-owned certified bytes retained after transaction validation.
    ///
    /// Direct row writes already arrive in an `Arc<str>`. Keeping that
    /// owner avoids allocating and copying the full JSON payload merely to
    /// discard an empty (or no-longer-needed) decoded-value cache.
    ValidatedOwned {
        normalized: Arc<str>,
    },
    CanonicalBatch(WasmCanonicalJson),
}

impl StageJson {
    pub(crate) fn value(&self) -> &serde_json::Value {
        match &self.storage {
            StageJsonStorage::Owned { value, normalized } => value
                .get_or_init(|| {
                    Arc::new(
                        serde_json::from_str(normalized)
                            .expect("prepared normalized JSON must remain valid JSON"),
                    )
                })
                .as_ref(),
            StageJsonStorage::CertifiedShared { value, normalized } => value
                .get_or_init(|| {
                    Arc::new(
                        serde_json::from_str(normalized.as_str())
                            .expect("prepared normalized JSON must remain valid JSON"),
                    )
                })
                .as_ref(),
            StageJsonStorage::ValidatedShared { .. } | StageJsonStorage::ValidatedOwned { .. } => {
                panic!("validated staged JSON must not be decoded after transaction validation")
            }
            StageJsonStorage::CanonicalBatch(value) => value.value(),
        }
    }

    pub(crate) fn normalized(&self) -> &str {
        match &self.storage {
            StageJsonStorage::Owned { normalized, .. } => normalized.as_ref(),
            StageJsonStorage::CertifiedShared { normalized, .. } => normalized.as_str(),
            StageJsonStorage::ValidatedShared { normalized } => normalized.as_str(),
            StageJsonStorage::ValidatedOwned { normalized } => normalized.as_ref(),
            StageJsonStorage::CanonicalBatch(value) => value.normalized(),
        }
    }

    #[cfg(test)]
    pub(crate) fn canonical_batch_row(&self) -> Option<&WasmCanonicalJson> {
        match &self.storage {
            StageJsonStorage::CanonicalBatch(value) => Some(value),
            StageJsonStorage::Owned { .. }
            | StageJsonStorage::CertifiedShared { .. }
            | StageJsonStorage::ValidatedShared { .. }
            | StageJsonStorage::ValidatedOwned { .. } => None,
        }
    }

    /// Drops the parsed values column after all semantic validation succeeds.
    ///
    /// Every row retains only a cheap slice of the canonical batch arena. The
    /// final row releases the shared DOM and offset columns before commit
    /// materialization allocates its storage buffers.
    pub(crate) fn release_validated_canonical_value_column(&mut self) -> bool {
        let storage = match &self.storage {
            StageJsonStorage::CanonicalBatch(value) => StageJsonStorage::ValidatedShared {
                normalized: value.normalized_shared(),
            },
            StageJsonStorage::CertifiedShared { normalized, .. } => {
                StageJsonStorage::ValidatedShared {
                    normalized: normalized.clone(),
                }
            }
            StageJsonStorage::Owned { normalized, .. } => StageJsonStorage::ValidatedOwned {
                normalized: Arc::clone(normalized),
            },
            StageJsonStorage::ValidatedShared { .. } | StageJsonStorage::ValidatedOwned { .. } => {
                return false;
            }
        };
        self.storage = storage;
        true
    }

    #[cfg(test)]
    pub(crate) fn retains_decoded_value_for_tests(&self) -> bool {
        match &self.storage {
            StageJsonStorage::Owned { value, .. }
            | StageJsonStorage::CertifiedShared { value, .. } => value.get().is_some(),
            StageJsonStorage::CanonicalBatch(_) => true,
            StageJsonStorage::ValidatedShared { .. } | StageJsonStorage::ValidatedOwned { .. } => {
                false
            }
        }
    }

    /// Retains canonical or already-shared normalized JSON without copying.
    ///
    /// Row-owned `Arc<str>` payloads still require one conversion into the
    /// shared byte-backed representation. Batch-backed and certified-shared
    /// payloads only clone an owner and range.
    pub(crate) fn materialize_shared(&self) -> SharedStr {
        match &self.storage {
            StageJsonStorage::Owned { normalized, .. } => SharedStr::from(normalized.as_ref()),
            StageJsonStorage::CertifiedShared { normalized, .. } => normalized.clone(),
            StageJsonStorage::ValidatedShared { normalized } => normalized.clone(),
            StageJsonStorage::ValidatedOwned { normalized } => SharedStr::from(normalized.as_ref()),
            StageJsonStorage::CanonicalBatch(value) => value.normalized_shared(),
        }
    }

    /// Whether this payload inlines into values instead of the json store.
    pub(crate) fn is_inline(&self) -> bool {
        self.normalized().len() <= crate::json_store::JSON_INLINE_MAX_BYTES
    }

    pub(crate) fn slot_ref(&self) -> crate::json_store::JsonSlotRef<'_> {
        if self.is_inline() {
            crate::json_store::JsonSlotRef::Inline(self.normalized())
        } else {
            crate::json_store::JsonSlotRef::Ref(&self.json_ref)
        }
    }
}

impl PartialEq for StageJson {
    fn eq(&self, other: &Self) -> bool {
        self.normalized() == other.normalized()
            && (self.is_inline() || other.is_inline() || self.json_ref == other.json_ref)
    }
}

impl Eq for StageJson {}

#[expect(clippy::unnecessary_wraps)]
pub(crate) fn stage_json_from_value(
    value: TransactionJson,
    _context: &str,
) -> Result<StageJson, LixError> {
    // Inline values carry their bytes as the authoritative durable payload.
    // Computing and retaining a content hash for every small row only to
    // discard it at `JsonSlotRef::Inline` doubled the canonical-byte walk on
    // bulk inserts. Out-of-band values still require the exact content ref.
    let json_ref = if value.normalized().len() <= crate::json_store::JSON_INLINE_MAX_BYTES {
        JsonRef::default()
    } else {
        JsonRef::for_content(value.normalized().as_bytes())
    };
    let storage = match value.storage {
        TransactionJsonStorage::Decoded { value, normalized } => StageJsonStorage::Owned {
            value: OnceLock::from(value),
            normalized: normalized.into_inner().unwrap_or_else(|| {
                panic!("transaction JSON was normalized while computing its JSON ref")
            }),
        },
        TransactionJsonStorage::Certified { normalized } => StageJsonStorage::Owned {
            value: OnceLock::new(),
            normalized,
        },
        TransactionJsonStorage::CertifiedShared {
            normalized,
            certificate:
                TransactionJsonCertificate::RowContent | TransactionJsonCertificate::Metadata,
        } => StageJsonStorage::CertifiedShared {
            value: OnceLock::new(),
            normalized,
        },
        TransactionJsonStorage::CanonicalShared { value, normalized } => {
            StageJsonStorage::CertifiedShared { value, normalized }
        }
        TransactionJsonStorage::CanonicalBatch(value) => StageJsonStorage::CanonicalBatch(value),
    };
    Ok(StageJson { storage, json_ref })
}

/// Coalesces decoded JSON values into one Arrow-style values column plus one
/// offset-addressed canonical UTF-8 arena.
///
/// Plugin-produced rows already carry a bounded canonical page handle and are
/// left untouched. SQL-produced values reach this boundary without a
/// per-row serialized `String`; the batch appends every row into one amortized
/// arena, serializes each row once, and replaces the row-owned values with
/// cheap batch handles. Avoiding an exact-size structural prepass matters for
/// bulk writes: walking every JSON tree twice costs more than the bounded
/// geometric growth of one shared arena.
pub(crate) fn canonicalize_transaction_json_batch<'a>(
    slots: impl IntoIterator<Item = &'a mut Option<TransactionJson>>,
    context: &str,
) -> Result<(), LixError> {
    enum DecodedCanonicalValue {
        Owned(JsonValue),
        Shared(Arc<JsonValue>),
    }

    impl DecodedCanonicalValue {
        fn as_value(&self) -> &JsonValue {
            match self {
                Self::Owned(value) => value,
                Self::Shared(value) => value.as_ref(),
            }
        }

        fn into_shared(self) -> Arc<JsonValue> {
            match self {
                Self::Owned(value) => Arc::new(value),
                Self::Shared(value) => value,
            }
        }
    }

    let mut slots = slots.into_iter().collect::<Vec<_>>();
    let decoded_count = slots
        .iter()
        .filter(|slot| {
            let slot: &Option<TransactionJson> = slot;
            slot.as_ref()
                .is_some_and(TransactionJson::requires_batch_canonicalization)
        })
        .count();
    let mut values = Vec::with_capacity(decoded_count);
    let mut all_values_uniquely_owned = true;
    let mut cached_normalized = Vec::with_capacity(decoded_count);
    let mut positions = Vec::with_capacity(decoded_count);
    for (position, slot) in slots.iter_mut().enumerate() {
        let Some(json) = slot.take() else {
            continue;
        };
        match json.storage {
            TransactionJsonStorage::Decoded { value, normalized } => {
                let value = match Arc::try_unwrap(value) {
                    Ok(value) => DecodedCanonicalValue::Owned(value),
                    Err(value) => {
                        all_values_uniquely_owned = false;
                        DecodedCanonicalValue::Shared(value)
                    }
                };
                positions.push(position);
                values.push(value);
                cached_normalized.push(normalized.into_inner());
            }
            TransactionJsonStorage::Certified { normalized } => {
                **slot = Some(TransactionJson {
                    storage: TransactionJsonStorage::Certified { normalized },
                });
            }
            TransactionJsonStorage::CertifiedShared {
                normalized,
                certificate,
            } => {
                **slot = Some(TransactionJson {
                    storage: TransactionJsonStorage::CertifiedShared {
                        normalized,
                        certificate,
                    },
                });
            }
            TransactionJsonStorage::CanonicalShared { value, normalized } => {
                **slot = Some(TransactionJson {
                    storage: TransactionJsonStorage::CanonicalShared { value, normalized },
                });
            }
            TransactionJsonStorage::CanonicalBatch(value) => {
                **slot = Some(TransactionJson::from_canonical_batch(value));
            }
        }
    }
    if values.is_empty() {
        return Ok(());
    }

    let cached_capacity = cached_normalized
        .iter()
        .flatten()
        .try_fold(0_usize, |total, cached| total.checked_add(cached.len()))
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_UNKNOWN,
                format!("{context} canonical JSON batch size overflowed"),
            )
        })?;
    let decoded_capacity_hint = values
        .len()
        .checked_mul(256)
        .and_then(|capacity| capacity.checked_add(cached_capacity))
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_UNKNOWN,
                format!("{context} canonical JSON batch size overflowed"),
            )
        })?;
    if u32::try_from(cached_capacity).is_err() {
        return Err(LixError::new(
            LixError::CODE_UNKNOWN,
            format!("{context} canonical JSON batch exceeds u32"),
        ));
    }

    let mut offsets = Vec::with_capacity(values.len());
    let mut normalized = CanonicalJsonArena::new(decoded_capacity_hint, context)?;
    let mut serialize_count = 0usize;
    for (value, cached) in values.iter().zip(&cached_normalized) {
        let start = u32::try_from(normalized.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_UNKNOWN,
                format!("{context} canonical JSON batch exceeds u32"),
            )
        })?;
        if let Some(cached) = cached {
            normalized
                .append(cached.as_bytes())
                .map_err(|failure| canonical_json_arena_error(context, failure))?;
        } else {
            serde_json::to_writer(&mut normalized, value.as_value()).map_err(|error| {
                normalized.failure().map_or_else(
                    || {
                        LixError::new(
                            LixError::CODE_UNKNOWN,
                            format!("{context} failed to serialize normalized JSON: {error}"),
                        )
                    },
                    |failure| canonical_json_arena_error(context, failure),
                )
            })?;
            serialize_count += 1;
        }
        let end = u32::try_from(normalized.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_UNKNOWN,
                format!("{context} canonical JSON batch exceeds u32"),
            )
        })?;
        offsets.push((start, end));
    }
    if all_values_uniquely_owned {
        let values = values
            .into_iter()
            .map(|value| {
                let DecodedCanonicalValue::Owned(value) = value else {
                    unreachable!("unique canonicalization retained a shared decoded JSON owner")
                };
                value
            })
            .collect();
        let rows = WasmCanonicalJson::from_batch_parts(
            values,
            normalized.into_bytes(),
            offsets,
            positions.len(),
            serialize_count,
        )?;
        for ((position, row), expected_row) in positions.into_iter().zip(rows).zip(0..) {
            debug_assert_eq!(row.row_index(), expected_row);
            *slots[position] = Some(TransactionJson::from_canonical_batch(row));
        }
    } else {
        // SAFETY: every uncached row was written by serde_json and every
        // cached row came from a previously validated TransactionJson.
        let normalized =
            unsafe { SharedStr::from_utf8_unchecked(Bytes::from(normalized.into_bytes())) };
        for ((position, value), (start, end)) in positions.into_iter().zip(values).zip(offsets) {
            let normalized = normalized
                .slice(start as usize..end as usize)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_UNKNOWN,
                        format!("{context} canonical JSON batch offsets are invalid"),
                    )
                })?;
            *slots[position] = Some(TransactionJson {
                storage: TransactionJsonStorage::CanonicalShared {
                    value: OnceLock::from(value.into_shared()),
                    normalized,
                },
            });
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CanonicalJsonArenaFailure {
    ExceedsU32,
    Allocation,
}

struct CanonicalJsonArena {
    bytes: Vec<u8>,
    limit: usize,
    failure: Option<CanonicalJsonArenaFailure>,
}

impl CanonicalJsonArena {
    fn new(capacity_hint: usize, context: &str) -> Result<Self, LixError> {
        Self::with_limit(capacity_hint, u32::MAX as usize)
            .map_err(|failure| canonical_json_arena_error(context, failure))
    }

    fn with_limit(capacity_hint: usize, limit: usize) -> Result<Self, CanonicalJsonArenaFailure> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity_hint.min(limit))
            .map_err(|_| CanonicalJsonArenaFailure::Allocation)?;
        Ok(Self {
            bytes,
            limit,
            failure: None,
        })
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn failure(&self) -> Option<CanonicalJsonArenaFailure> {
        self.failure
    }

    fn append(&mut self, bytes: &[u8]) -> Result<(), CanonicalJsonArenaFailure> {
        self.write_all(bytes).map_err(|_| {
            self.failure
                .unwrap_or(CanonicalJsonArenaFailure::Allocation)
        })
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl io::Write for CanonicalJsonArena {
    fn write(&mut self, source: &[u8]) -> io::Result<usize> {
        let required = self
            .bytes
            .len()
            .checked_add(source.len())
            .filter(|&required| required <= self.limit)
            .ok_or_else(|| {
                self.failure = Some(CanonicalJsonArenaFailure::ExceedsU32);
                io::Error::other("canonical JSON arena exceeds its wire-format limit")
            })?;
        if required > self.bytes.capacity() {
            let doubled = self
                .bytes
                .capacity()
                .max(1)
                .saturating_mul(2)
                .min(self.limit);
            let target = required.max(doubled);
            if self
                .bytes
                .try_reserve_exact(target - self.bytes.len())
                .is_err()
            {
                self.failure = Some(CanonicalJsonArenaFailure::Allocation);
                return Err(io::Error::other("canonical JSON arena allocation failed"));
            }
        }
        self.bytes.extend_from_slice(source);
        Ok(source.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn canonical_json_arena_error(context: &str, failure: CanonicalJsonArenaFailure) -> LixError {
    let message = match failure {
        CanonicalJsonArenaFailure::ExceedsU32 => {
            format!("{context} canonical JSON batch exceeds u32")
        }
        CanonicalJsonArenaFailure::Allocation => {
            format!("{context} canonical JSON batch allocation failed")
        }
    };
    LixError::new(LixError::CODE_UNKNOWN, message)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
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

/// Row-shaped fixture used only to keep transaction tests concise.
///
/// Production preparation writes directly into [`PreparedStateBatch`] and
/// never creates this row owner.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TestPreparedStateRow {
    pub(crate) schema_plan_id: SchemaPlanId,
    pub(crate) facts: PreparedRowFacts,
    pub(crate) row_pk: RowPk,
    pub(crate) schema_key: SharedStr,
    pub(crate) file_id: Option<SharedStr>,
    pub(crate) snapshot: Option<StageJson>,
    pub(crate) metadata: Option<StageJson>,
    pub(crate) origin: Option<TransactionWriteOrigin>,
    /// Execution-scoped provenance shared by all rows in the prepared batch.
    pub(crate) origin_key: Option<SharedStr>,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) global: bool,
    pub(crate) change_id: Option<ChangeId>,
    pub(crate) commit_id: Option<CommitId>,
    pub(crate) untracked: bool,
    pub(crate) branch_id: SharedStr,
}

#[cfg(test)]
impl TestPreparedStateRow {
    /// Borrows this row-shaped fixture through the same view consumed by
    /// production validation and staging code.
    pub(crate) fn borrowed(&self) -> PreparedStateRowRef<'_> {
        PreparedStateRowRef {
            schema_plan_id: self.schema_plan_id,
            facts: self.facts,
            row_pk: &self.row_pk,
            schema_key: &self.schema_key,
            file_id: self.file_id.as_ref(),
            snapshot: self.snapshot.as_ref(),
            metadata: self.metadata.as_ref(),
            origin: self.origin.as_ref(),
            origin_key: self.origin_key.as_ref(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            global: self.global,
            change_id: self.change_id,
            addressable_change_id: false,
            commit_id: self.commit_id,
            untracked: self.untracked,
            branch_id: &self.branch_id,
            durable_predecessor: None,
        }
    }
}

/// One staged row's indexed-column values, lifted out of the snapshot while
/// transaction validation still held it parsed.
///
/// This exists so the commit-time index hook never decodes a snapshot again.
/// `StageJson::value()` panics once validation releases the decoded column,
/// and reaching around it with a second `serde_json::from_str` is exactly the
/// cost this carrier removes.
#[derive(Debug, Clone)]
pub(crate) struct StagedIndexRow {
    pub(crate) branch_id: SharedStr,
    pub(crate) schema_key: SharedStr,
    pub(crate) row_pk: RowPk,
    pub(crate) file_id: Option<SharedStr>,
    /// **Every** indexed ordinal the row's schema declares, carrying `None`
    /// where this row has no indexable value for it.
    ///
    /// Commit earns a completeness witness per `(schema, ordinal)` whether or
    /// not extraction found a value, so dropping the `None` entries would
    /// silently narrow witness coverage and leave a column permanently
    /// unwitnessed — a slow read, not a wrong one, but an invisible one.
    pub(crate) columns: Vec<(u16, Option<crate::hot_state::HotIndexValue>)>,
}

/// Everything the commit-time hot index hook needs, produced by validation.
///
/// Empty is the safe value: no rows means no entries, and no registered
/// collections means no witnesses, so a batch that never reached extraction
/// publishes nothing and every read of that collection keeps scanning.
#[derive(Debug, Clone, Default)]
pub(crate) struct StagedIndexValues {
    pub(crate) rows: Vec<StagedIndexRow>,
    /// `(schema_key, ordinal)` pairs whose collection provably begins at this
    /// commit because the commit registers the schema itself.
    pub(crate) registered_collections: std::collections::BTreeSet<(String, u16)>,
}

impl StagedIndexValues {
    /// Folds one schema scope's extraction into the transaction-wide result.
    pub(crate) fn absorb(&mut self, other: Self) {
        self.rows.extend(other.rows);
        self.registered_collections
            .extend(other.registered_collections);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.registered_collections.is_empty()
    }
}

/// Compact, typed owner for prepared transaction state.
///
/// Rows are represented by ordinals into typed owner columns. Repeated
/// branch, schema, file, origin-key, and logical-origin values are interned
/// once per batch instead of being cloned into every row. Canonical JSON
/// entries remain ordinal views over their cursor-page owner, so the batch
/// retains one underlying canonical arena while preserving row-local refs.
#[derive(Debug, Clone)]
pub(crate) struct PreparedStateBatch {
    slots: Vec<PreparedStateSlot>,
    /// Fixed-shape certified parameter writes keep batch-common facts once and
    /// derive identity/JSON ordinals from row position. Any operation that
    /// needs row-local topology expands this representation into `slots`.
    dense_certified_parameter: Option<DenseCertifiedParameterSlots>,
    row_pks: Vec<RowPk>,
    strings: Vec<SharedStr>,
    string_index: HashMap<SharedStr, u32>,
    json: Vec<StageJson>,
    durable_predecessors: Vec<CertifiedCurrentStatePredecessor>,
    origins: Vec<TransactionWriteOrigin>,
    origin_index: HashMap<TransactionWriteOrigin, u32>,
    origin_column_sets: Vec<Arc<[String]>>,
    origin_column_index: HashMap<Arc<[String]>, u32>,
    /// The typed producer proved strictly ordered, unique tracked keys. Any
    /// row topology change clears this proof.
    certified_tracked_keys_strictly_ordered: bool,
    /// Proof material consumed when publishing the immutable replacement
    /// manifest. Row-topology changes clear the proof as one atomic value.
    complete_collection_replacement: Option<CompleteCollectionReplacementProof>,
    /// Indexed-column values extracted during transaction validation, which is
    /// the last place the snapshots are already parsed. Derived data, not row
    /// content: [`PartialEq`] deliberately ignores it.
    staged_index_values: StagedIndexValues,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedStateSlot {
    schema_plan_id: SchemaPlanId,
    facts: PreparedRowFacts,
    row_pk: u32,
    schema_key: u32,
    file_id: Option<u32>,
    snapshot: Option<u32>,
    metadata: Option<u32>,
    origin: Option<u32>,
    origin_key: Option<u32>,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    global: bool,
    change_id: Option<ChangeId>,
    /// The engine generated this fresh tracked identity and may replace it
    /// with its final commit-delta address during commit planning.
    addressable_change_id: bool,
    commit_id: Option<CommitId>,
    untracked: bool,
    branch_id: u32,
    durable_predecessor: Option<u32>,
}

#[derive(Debug, Clone)]
struct DenseCertifiedParameterSlots {
    len: usize,
    schema_plan_id: SchemaPlanId,
    facts: PreparedRowFacts,
    schema_key: u32,
    origin_key: Option<u32>,
    timestamps: DenseParameterTimestamps,
    commit_id: Option<CommitId>,
    branch_id: u32,
    untracked: bool,
    /// Absent until commit-delta publication assigns direct addresses. The
    /// compact segment map derives every UUID without a million-row column.
    direct_change_ids: Option<OrderedAddressableCommitDeltaStage>,
    /// Frontend-built row groups over the same certified typed columns.
    /// Topology-changing operations drop this derived accelerator.
    row_columnar: Option<crate::sql2::EncodedRowGroups>,
    /// Exact authenticated predecessor evidence is row-aligned with the
    /// certified identity column. Ordinary replacement batches therefore
    /// retain their compact representation through publication.
    durable_predecessors: Option<Arc<[CertifiedCurrentStatePredecessor]>>,
}

#[derive(Debug, Clone)]
enum DenseParameterTimestamps {
    Scalar(LixTimestamp),
    PerRow(Vec<LixTimestamp>),
}

impl DenseParameterTimestamps {
    fn get(&self, row_index: usize) -> LixTimestamp {
        match self {
            Self::Scalar(timestamp) => *timestamp,
            Self::PerRow(timestamps) => timestamps[row_index],
        }
    }

    fn append(&mut self, left_len: usize, right_len: usize, right: Self) {
        if let (Self::Scalar(left), Self::Scalar(right)) = (&*self, &right)
            && left == right
        {
            return;
        }
        let mut timestamps = match self {
            Self::Scalar(timestamp) => {
                let mut timestamps = Vec::with_capacity(left_len.saturating_add(right_len));
                timestamps.resize(left_len, *timestamp);
                timestamps
            }
            Self::PerRow(timestamps) => std::mem::take(timestamps),
        };
        match right {
            Self::Scalar(timestamp) => {
                timestamps.resize(timestamps.len().saturating_add(right_len), timestamp);
            }
            Self::PerRow(mut right) => timestamps.append(&mut right),
        }
        *self = Self::PerRow(timestamps);
    }
}

/// Borrowed row projection over a [`PreparedStateBatch`].
///
/// This is intentionally a view, not an owner. Identifiers, JSON payloads,
/// and logical write metadata stay in the batch columns.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PreparedStateRowRef<'a> {
    pub(crate) schema_plan_id: SchemaPlanId,
    pub(crate) facts: PreparedRowFacts,
    pub(crate) row_pk: &'a RowPk,
    pub(crate) schema_key: &'a SharedStr,
    pub(crate) file_id: Option<&'a SharedStr>,
    pub(crate) snapshot: Option<&'a StageJson>,
    pub(crate) metadata: Option<&'a StageJson>,
    pub(crate) origin: Option<&'a TransactionWriteOrigin>,
    pub(crate) origin_key: Option<&'a SharedStr>,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) global: bool,
    pub(crate) change_id: Option<ChangeId>,
    pub(crate) addressable_change_id: bool,
    pub(crate) commit_id: Option<CommitId>,
    pub(crate) untracked: bool,
    pub(crate) branch_id: &'a SharedStr,
    pub(crate) durable_predecessor: Option<&'a CertifiedCurrentStatePredecessor>,
}

pub(crate) struct PreparedStateRows<'a> {
    batch: &'a PreparedStateBatch,
    range: std::ops::Range<usize>,
}

impl PreparedStateBatch {
    pub(crate) fn new() -> Self {
        Self::with_capacity(0)
    }

    pub(crate) fn with_capacity(row_capacity: usize) -> Self {
        let json_capacity = row_capacity
            .checked_mul(2)
            .expect("prepared JSON column capacity overflowed");
        Self::with_column_capacities(row_capacity, json_capacity, row_capacity)
    }

    /// Reserves exact dense row/JSON columns while keeping interned strings
    /// bounded by a small initial dictionary. Plugin imports commonly contain
    /// hundreds of thousands of rows but only one branch, file, schema, and
    /// origin key; reserving one hash bucket and string slot per row creates a
    /// large empty representation of those repeated values.
    pub(crate) fn with_dense_capacity(row_capacity: usize, json_capacity: usize) -> Self {
        Self::with_column_capacities(row_capacity, json_capacity, row_capacity.min(1_024))
    }

    fn with_column_capacities(
        row_capacity: usize,
        json_capacity: usize,
        string_capacity: usize,
    ) -> Self {
        Self {
            slots: Vec::with_capacity(row_capacity),
            dense_certified_parameter: None,
            row_pks: Vec::with_capacity(row_capacity),
            // Most bulk batches share schema, branch, and origin descriptors;
            // file ids are the only commonly row-cardinal string. Reserving
            // five entries per row made the empty dictionary dominate peak
            // memory on the happy path. Pathological all-distinct columns can
            // grow this bounded hint a fixed handful of times.
            strings: Vec::with_capacity(string_capacity),
            string_index: HashMap::with_capacity(string_capacity),
            json: Vec::with_capacity(json_capacity),
            durable_predecessors: Vec::new(),
            // Origin dictionaries are absent for ordinary SQL/direct writes
            // and typically contain only a few shared descriptors. Allocate
            // them lazily instead of paying two N-sized buffers up front.
            origins: Vec::new(),
            origin_index: HashMap::new(),
            origin_column_sets: Vec::new(),
            origin_column_index: HashMap::new(),
            certified_tracked_keys_strictly_ordered: false,
            complete_collection_replacement: None,
            staged_index_values: StagedIndexValues::default(),
        }
    }

    /// Publishes validation's indexed-column extraction onto the batch that
    /// commit will materialize.
    pub(crate) fn set_staged_index_values(&mut self, values: StagedIndexValues) {
        self.staged_index_values = values;
    }

    pub(crate) fn staged_index_values(&self) -> &StagedIndexValues {
        &self.staged_index_values
    }

    pub(crate) fn len(&self) -> usize {
        self.dense_certified_parameter
            .as_ref()
            .map_or_else(|| self.slots.len(), |dense| dense.len)
    }

    #[cfg(feature = "storage-benches")]
    pub(crate) fn record_ownership(&self, stage: usize) {
        let mut key_bytes = 0usize;
        let mut value_bytes = 0usize;
        let mut string_entries = 0usize;
        for row in self.iter() {
            key_bytes = key_bytes
                .saturating_add(row.row_pk.estimated_heap_bytes())
                .saturating_add(row.schema_key.len())
                .saturating_add(row.file_id.map_or(0, |value| value.len()))
                .saturating_add(row.origin_key.map_or(0, |value| value.len()))
                .saturating_add(row.branch_id.len());
            value_bytes = value_bytes
                .saturating_add(row.snapshot.map_or(0, |value| value.normalized().len()))
                .saturating_add(row.metadata.map_or(0, |value| value.normalized().len()));
            string_entries = string_entries
                .saturating_add(2)
                .saturating_add(usize::from(row.file_id.is_some()))
                .saturating_add(usize::from(row.origin_key.is_some()));
        }
        crate::storage_bench::record_crud_ownership(
            stage,
            self.len(),
            key_bytes,
            value_bytes,
            self.len().saturating_mul(6),
            string_entries,
            self.len().saturating_mul(2),
        );
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn certified_tracked_keys_strictly_ordered(&self) -> bool {
        self.certified_tracked_keys_strictly_ordered
    }

    pub(crate) fn complete_collection_replacement_proof(
        &self,
    ) -> Option<CompleteCollectionReplacementProof> {
        self.complete_collection_replacement
    }

    pub(crate) fn certify_complete_collection_replacement(
        &mut self,
        expected_schema_key: &str,
        expected_branch_id: &str,
        expected_live_count: u64,
        expected_ordered_identity_digest: [u8; 32],
    ) -> bool {
        if u64::try_from(self.len()).ok() != Some(expected_live_count) || self.is_empty() {
            return false;
        }
        let Some(actual_digest) =
            crate::collection_generation::ordered_single_string_identity_digest(
                self.iter().map(|row| row.row_pk),
            )
        else {
            return false;
        };
        if actual_digest != expected_ordered_identity_digest {
            return false;
        }
        let Some(replay_bytes) = self.iter().try_fold(0_u64, |bytes, row| {
            if row.schema_key.as_str() != expected_schema_key
                || row.branch_id.as_str() != expected_branch_id
                || row.snapshot.is_none()
                || row.file_id.is_some()
                || row.origin.is_some()
                || row.origin_key.is_some()
                || row.untracked
                || row.global
            {
                return None;
            }
            let row_bytes = row
                .schema_key
                .len()
                .checked_add(row.row_pk.estimated_heap_bytes())?
                .checked_add(128)?
                .checked_add(row.snapshot?.normalized().len())?;
            bytes.checked_add(u64::try_from(row_bytes).ok()?)
        }) else {
            return false;
        };
        // A complete replacement is one immutable post-image generation, not
        // a sequence of row statements. Canonicalize its lifecycle boundary
        // exactly where the complete-set proof becomes authoritative so the
        // durable part encoder never inherits legacy per-call timestamps.
        let replacement_timestamp = self.row(0).updated_at;
        if let Some(dense) = &mut self.dense_certified_parameter {
            dense.timestamps = DenseParameterTimestamps::Scalar(replacement_timestamp);
        } else {
            for slot in &mut self.slots {
                slot.updated_at = replacement_timestamp;
            }
        }
        self.complete_collection_replacement = Some(CompleteCollectionReplacementProof {
            ordered_identity_digest: expected_ordered_identity_digest,
            replay_bytes,
        });
        self.certified_tracked_keys_strictly_ordered = true;
        true
    }

    pub(crate) fn iter(&self) -> PreparedStateRows<'_> {
        PreparedStateRows {
            batch: self,
            range: 0..self.len(),
        }
    }

    pub(crate) fn row(&self, index: usize) -> PreparedStateRowRef<'_> {
        self.get(index)
            .expect("prepared state batch row ordinal is in bounds")
    }

    pub(crate) fn get(&self, index: usize) -> Option<PreparedStateRowRef<'_>> {
        if let Some(dense) = &self.dense_certified_parameter {
            if index >= dense.len {
                return None;
            }
            return Some(PreparedStateRowRef {
                schema_plan_id: dense.schema_plan_id,
                facts: dense.facts,
                row_pk: &self.row_pks[index],
                schema_key: &self.strings[dense.schema_key as usize],
                file_id: None,
                snapshot: Some(&self.json[index]),
                metadata: None,
                origin: None,
                origin_key: dense.origin_key.map(|index| &self.strings[index as usize]),
                created_at: dense.timestamps.get(index),
                updated_at: dense.timestamps.get(index),
                global: false,
                change_id: Some(dense.direct_change_ids.as_ref().map_or_else(
                    ChangeId::default,
                    |assignment| {
                        assignment
                            .change_id_at(index)
                            .expect("dense direct change assignment covers every row")
                    },
                )),
                addressable_change_id: true,
                commit_id: dense.commit_id,
                untracked: dense.untracked,
                branch_id: &self.strings[dense.branch_id as usize],
                durable_predecessor: dense
                    .durable_predecessors
                    .as_ref()
                    .map(|values| &values[index]),
            });
        }
        let slot = *self.slots.get(index)?;
        Some(PreparedStateRowRef {
            schema_plan_id: slot.schema_plan_id,
            facts: slot.facts,
            row_pk: &self.row_pks[slot.row_pk as usize],
            schema_key: &self.strings[slot.schema_key as usize],
            file_id: slot.file_id.map(|index| &self.strings[index as usize]),
            snapshot: slot.snapshot.map(|index| &self.json[index as usize]),
            metadata: slot.metadata.map(|index| &self.json[index as usize]),
            origin: slot.origin.map(|index| &self.origins[index as usize]),
            origin_key: slot.origin_key.map(|index| &self.strings[index as usize]),
            created_at: slot.created_at,
            updated_at: slot.updated_at,
            global: slot.global,
            change_id: slot.change_id,
            addressable_change_id: slot.addressable_change_id,
            commit_id: slot.commit_id,
            untracked: slot.untracked,
            branch_id: &self.strings[slot.branch_id as usize],
            durable_predecessor: slot
                .durable_predecessor
                .map(|index| &self.durable_predecessors[index as usize]),
        })
    }

    pub(crate) fn first(&self) -> Option<PreparedStateRowRef<'_>> {
        self.get(0)
    }

    pub(crate) fn last(&self) -> Option<PreparedStateRowRef<'_>> {
        self.len().checked_sub(1).and_then(|index| self.get(index))
    }

    #[cfg(test)]
    pub(crate) fn push_test_row(&mut self, row: TestPreparedStateRow) {
        self.push_parts(
            row.schema_plan_id,
            row.facts,
            row.row_pk,
            row.schema_key,
            row.file_id,
            row.snapshot,
            row.metadata,
            row.origin,
            row.origin_key.as_ref(),
            row.created_at,
            row.updated_at,
            row.global,
            row.change_id,
            row.commit_id,
            row.untracked,
            row.branch_id,
        );
    }

    #[cfg(test)]
    pub(crate) fn from_test_rows(rows: Vec<TestPreparedStateRow>) -> Self {
        let mut batch = Self::with_capacity(rows.len());
        for row in rows {
            batch.push_test_row(row);
        }
        batch
    }

    #[cfg(test)]
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn push_parts(
        &mut self,
        schema_plan_id: SchemaPlanId,
        facts: PreparedRowFacts,
        row_pk: RowPk,
        schema_key: SharedStr,
        file_id: Option<SharedStr>,
        snapshot: Option<StageJson>,
        metadata: Option<StageJson>,
        origin: Option<TransactionWriteOrigin>,
        origin_key: Option<&SharedStr>,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
        global: bool,
        change_id: Option<ChangeId>,
        commit_id: Option<CommitId>,
        untracked: bool,
        branch_id: SharedStr,
    ) {
        self.push_parts_with_change_addressability(
            schema_plan_id,
            facts,
            row_pk,
            schema_key,
            file_id,
            snapshot,
            metadata,
            origin,
            origin_key,
            created_at,
            updated_at,
            global,
            change_id,
            false,
            commit_id,
            untracked,
            branch_id,
        );
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) fn push_parts_with_change_addressability(
        &mut self,
        schema_plan_id: SchemaPlanId,
        facts: PreparedRowFacts,
        row_pk: RowPk,
        schema_key: SharedStr,
        file_id: Option<SharedStr>,
        snapshot: Option<StageJson>,
        metadata: Option<StageJson>,
        origin: Option<TransactionWriteOrigin>,
        origin_key: Option<&SharedStr>,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
        global: bool,
        change_id: Option<ChangeId>,
        addressable_change_id: bool,
        commit_id: Option<CommitId>,
        untracked: bool,
        branch_id: SharedStr,
    ) {
        #[cfg(feature = "storage-benches")]
        record_prepared_row_ownership(
            &row_pk,
            &schema_key,
            file_id.as_ref(),
            snapshot.as_ref(),
            metadata.as_ref(),
            origin_key,
            &branch_id,
            crate::storage_bench::CRUD_OWNERSHIP_PREPARED_BATCH,
        );
        self.expand_dense_certified_parameter();
        let row_pk = self.push_row_pk(row_pk);
        let schema_key = self.intern_string(schema_key);
        let file_id = file_id.map(|value| self.intern_string(value));
        let snapshot = snapshot.map(|value| self.push_json(value));
        let metadata = metadata.map(|value| self.push_json(value));
        let origin = origin.map(|value| self.intern_origin(value));
        let origin_key = origin_key.map(|value| self.intern_string_ref(value));
        let branch_id = self.intern_string(branch_id);
        self.slots.push(PreparedStateSlot {
            schema_plan_id,
            facts,
            row_pk,
            schema_key,
            file_id,
            snapshot,
            metadata,
            origin,
            origin_key,
            created_at,
            updated_at,
            global,
            change_id,
            addressable_change_id,
            commit_id,
            untracked,
            branch_id,
            durable_predecessor: None,
        });
    }

    fn expand_dense_certified_parameter(&mut self) {
        let Some(dense) = self.dense_certified_parameter.take() else {
            return;
        };
        debug_assert!(self.slots.is_empty());
        debug_assert_eq!(self.row_pks.len(), dense.len);
        debug_assert_eq!(self.json.len(), dense.len);
        self.slots.reserve(dense.len);
        for row_index in 0..dense.len {
            let ordinal = u32::try_from(row_index)
                .expect("dense certified parameter row ordinal must fit u32");
            let durable_predecessor = dense.durable_predecessors.as_ref().map(|values| {
                let index = u32::try_from(self.durable_predecessors.len())
                    .expect("prepared durable predecessor column must fit u32");
                self.durable_predecessors.push(values[row_index].clone());
                index
            });
            self.slots.push(PreparedStateSlot {
                schema_plan_id: dense.schema_plan_id,
                facts: dense.facts,
                row_pk: ordinal,
                schema_key: dense.schema_key,
                file_id: None,
                snapshot: Some(ordinal),
                metadata: None,
                origin: None,
                origin_key: dense.origin_key,
                created_at: dense.timestamps.get(row_index),
                updated_at: dense.timestamps.get(row_index),
                global: false,
                change_id: Some(dense.direct_change_ids.as_ref().map_or_else(
                    ChangeId::default,
                    |assignment| {
                        assignment
                            .change_id_at(row_index)
                            .expect("dense direct change assignment covers every row")
                    },
                )),
                addressable_change_id: true,
                commit_id: dense.commit_id,
                // The durability lane is a batch-common fact carried by the
                // dense header, exactly like `commit_id` and `branch_id` above.
                // Reading it from anywhere else would let the same batch report
                // one lane through `get()` and another through its row slots.
                untracked: dense.untracked,
                branch_id: dense.branch_id,
                durable_predecessor,
            });
        }
    }

    pub(crate) fn set_durable_predecessor(
        &mut self,
        row: usize,
        value: Option<CertifiedCurrentStatePredecessor>,
    ) {
        self.expand_dense_certified_parameter();
        self.slots[row].durable_predecessor = value.map(|value| {
            let index = u32::try_from(self.durable_predecessors.len())
                .expect("prepared durable predecessor column must fit u32");
            self.durable_predecessors.push(value);
            index
        });
    }

    /// Extends one fixed-shape certified parameter journal without expanding
    /// either side into row slots. Repeated `Transaction::execute` calls
    /// arrive as singleton batches; compatible ordered writes are one logical
    /// columnar morsel and only need row-cardinal identity, JSON, and timestamp
    /// columns.
    fn try_append_dense_certified_parameter(&mut self, other: &mut Self) -> bool {
        let compatible = match (
            self.dense_certified_parameter.as_ref(),
            other.dense_certified_parameter.as_ref(),
        ) {
            (Some(left), Some(right)) => {
                let same_origin_key = match (left.origin_key, right.origin_key) {
                    (None, None) => true,
                    (Some(left), Some(right)) => {
                        self.strings[left as usize] == other.strings[right as usize]
                    }
                    (None, Some(_)) | (Some(_), None) => false,
                };
                left.schema_plan_id == right.schema_plan_id
                    && left.facts == right.facts
                    && self.strings[left.schema_key as usize]
                        == other.strings[right.schema_key as usize]
                    && self.strings[left.branch_id as usize]
                        == other.strings[right.branch_id as usize]
                    && same_origin_key
                    && left.commit_id == right.commit_id
                    // The merged cohort keeps one dense header, so every
                    // batch-common fact must already agree. The durability
                    // lane is one of them: without this, a cohort silently
                    // adopts the leader's lane.
                    && left.untracked == right.untracked
                    && left.direct_change_ids.is_none()
                    && right.direct_change_ids.is_none()
                    && left.row_columnar.is_none()
                    && right.row_columnar.is_none()
                    && left.durable_predecessors.is_none()
                    && right.durable_predecessors.is_none()
                    && self.durable_predecessors.is_empty()
                    && other.durable_predecessors.is_empty()
                    && self.origins.is_empty()
                    && other.origins.is_empty()
                    && self.origin_column_sets.is_empty()
                    && other.origin_column_sets.is_empty()
                    && self.certified_tracked_keys_strictly_ordered
                    && other.certified_tracked_keys_strictly_ordered
                    && self
                        .row_pks
                        .last()
                        .zip(other.row_pks.first())
                        .is_some_and(|(left, right)| left < right)
            }
            (None, _) | (_, None) => false,
        };
        if !compatible {
            return false;
        }

        let right = other
            .dense_certified_parameter
            .take()
            .expect("compatible dense parameter batch");
        let left = self
            .dense_certified_parameter
            .as_mut()
            .expect("compatible dense parameter batch");
        left.timestamps
            .append(left.len, right.len, right.timestamps);
        left.len = left
            .len
            .checked_add(right.len)
            .expect("prepared dense parameter row count overflowed");
        self.row_pks.append(&mut other.row_pks);
        self.json.append(&mut other.json);
        self.complete_collection_replacement = None;
        true
    }

    /// Appends another batch without materializing row-owned intermediates.
    pub(crate) fn append(&mut self, mut other: Self) {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_crud_ownership(
            crate::storage_bench::CRUD_OWNERSHIP_PREPARED_CLONE,
            other.len(),
            0,
            0,
            other.len().saturating_mul(6),
            0,
            0,
        );
        if other.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = other;
            return;
        }
        if self.try_append_dense_certified_parameter(&mut other) {
            return;
        }
        self.expand_dense_certified_parameter();
        other.expand_dense_certified_parameter();
        self.certified_tracked_keys_strictly_ordered = false;
        self.complete_collection_replacement = None;
        let row_base =
            u32::try_from(self.row_pks.len()).expect("prepared row column must fit u32");
        let json_base = u32::try_from(self.json.len()).expect("prepared JSON column must fit u32");
        let predecessor_base = u32::try_from(self.durable_predecessors.len())
            .expect("prepared predecessor column must fit u32");
        self.slots.reserve(other.slots.len());
        self.row_pks.reserve(other.row_pks.len());
        self.json.reserve(other.json.len());
        self.durable_predecessors
            .reserve(other.durable_predecessors.len());
        self.strings.reserve(other.strings.len());
        self.string_index.reserve(other.strings.len());
        self.origins.reserve(other.origins.len());
        self.origin_index.reserve(other.origins.len());
        self.origin_column_sets
            .reserve(other.origin_column_sets.len());
        self.origin_column_index
            .reserve(other.origin_column_sets.len());
        let string_remap = other
            .strings
            .drain(..)
            .map(|value| self.intern_string(value))
            .collect::<Vec<_>>();
        let origin_remap = other
            .origins
            .drain(..)
            .map(|value| self.intern_origin(value))
            .collect::<Vec<_>>();
        self.row_pks.append(&mut other.row_pks);
        self.json.append(&mut other.json);
        self.durable_predecessors
            .append(&mut other.durable_predecessors);
        self.slots.extend(other.slots.into_iter().map(|slot| {
            let remap_string = |index: u32| string_remap[index as usize];
            PreparedStateSlot {
                row_pk: slot
                    .row_pk
                    .checked_add(row_base)
                    .expect("prepared row ordinal overflowed"),
                schema_key: remap_string(slot.schema_key),
                file_id: slot.file_id.map(remap_string),
                snapshot: slot.snapshot.map(|index| {
                    index
                        .checked_add(json_base)
                        .expect("prepared JSON ordinal overflowed")
                }),
                metadata: slot.metadata.map(|index| {
                    index
                        .checked_add(json_base)
                        .expect("prepared JSON ordinal overflowed")
                }),
                durable_predecessor: slot.durable_predecessor.map(|index| {
                    index
                        .checked_add(predecessor_base)
                        .expect("prepared predecessor ordinal overflowed")
                }),
                origin: slot.origin.map(|index| origin_remap[index as usize]),
                origin_key: slot.origin_key.map(remap_string),
                branch_id: remap_string(slot.branch_id),
                ..slot
            }
        }));
    }

    pub(crate) fn swap_rows(&mut self, left: usize, right: usize) {
        self.expand_dense_certified_parameter();
        self.certified_tracked_keys_strictly_ordered = false;
        self.complete_collection_replacement = None;
        self.slots.swap(left, right);
    }

    /// Selects unique source ordinals into the requested destination order.
    ///
    /// Only compact slots move; typed owner columns remain stable and are
    /// still shared by the selected row ordinals.
    pub(crate) fn select_rows(&mut self, source_by_destination: &[usize]) {
        if self.dense_certified_parameter.is_some()
            && source_by_destination.len() == self.len()
            && source_by_destination
                .iter()
                .enumerate()
                .all(|(destination, &source)| destination == source)
        {
            return;
        }
        self.expand_dense_certified_parameter();
        self.certified_tracked_keys_strictly_ordered = false;
        self.complete_collection_replacement = None;
        debug_assert!(
            source_by_destination
                .iter()
                .all(|source| *source < self.len())
        );
        let old_len = self.len();
        let retained_len = source_by_destination.len();
        // Sparse replacements keep slot updates cheap. Rebuild owner columns
        // only once dead row owners exceed the live row count, bounding
        // retained arenas to roughly 2x live data while making compaction
        // amortized O(1) across repeated point updates.
        let should_compact_owners = self.should_compact_owner_columns(retained_len);
        if retained_len < old_len && should_compact_owners {
            let mut compacted = Self::with_capacity(source_by_destination.len());
            for &source in source_by_destination {
                compacted.push_borrowed_row(self.row(source));
            }
            *self = compacted;
            return;
        }
        let mut original_at_position = (0..old_len).collect::<Vec<_>>();
        let mut position_of_original = original_at_position.clone();
        for (destination, &source) in source_by_destination.iter().enumerate() {
            let source_position = position_of_original[source];
            if source_position == destination {
                continue;
            }
            let displaced_original = original_at_position[destination];
            self.slots.swap(destination, source_position);
            original_at_position.swap(destination, source_position);
            position_of_original[source] = destination;
            position_of_original[displaced_original] = source_position;
        }
        self.slots.truncate(source_by_destination.len());
    }

    /// Truncates a slot prefix after callers have placed final rows directly.
    ///
    /// Unlike `select_rows`, this path allocates no O(live_rows) permutation
    /// buffers. Generic staging uses it for sparse point replacements.
    pub(crate) fn truncate_rows(&mut self, retained_len: usize) {
        if self.dense_certified_parameter.is_some() && retained_len == self.len() {
            return;
        }
        self.expand_dense_certified_parameter();
        if retained_len != self.slots.len() {
            self.certified_tracked_keys_strictly_ordered = false;
            self.complete_collection_replacement = None;
        }
        self.slots.truncate(retained_len);
        if !self.should_compact_owner_columns(retained_len) {
            return;
        }
        let mut compacted = Self::with_capacity(retained_len);
        for index in 0..retained_len {
            compacted.push_borrowed_row(self.row(index));
        }
        *self = compacted;
    }

    pub(crate) fn set_commit_id(&mut self, index: usize, commit_id: Option<CommitId>) {
        self.expand_dense_certified_parameter();
        self.slots[index].commit_id = commit_id;
    }

    pub(crate) fn set_commit_id_all(&mut self, commit_id: CommitId) {
        if let Some(dense) = &mut self.dense_certified_parameter {
            dense.commit_id = Some(commit_id);
            return;
        }
        for slot in &mut self.slots {
            slot.commit_id = Some(commit_id);
        }
    }

    pub(crate) fn set_change_id(&mut self, index: usize, change_id: Option<ChangeId>) {
        self.expand_dense_certified_parameter();
        self.slots[index].change_id = change_id;
    }

    pub(crate) fn set_ordered_addressable_change_ids(
        &mut self,
        row_indices: &[usize],
        assignment: OrderedAddressableCommitDeltaStage,
    ) -> Result<(), LixError> {
        if assignment.row_count() != row_indices.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "ordered commit-delta assignment count changed during staging",
            ));
        }
        if let Some(dense) = &mut self.dense_certified_parameter
            && row_indices.len() == dense.len
            && row_indices
                .iter()
                .enumerate()
                .all(|(index, &row_index)| index == row_index)
        {
            dense.direct_change_ids = Some(assignment);
            return Ok(());
        }
        for (&row_index, change_id) in row_indices.iter().zip(assignment.assigned_change_ids()) {
            self.set_change_id(row_index, Some(change_id));
        }
        Ok(())
    }

    pub(crate) fn release_validated_canonical_value_columns(&mut self) {
        if let Some(dense) = &self.dense_certified_parameter {
            debug_assert_eq!(dense.len, self.json.len());
        } else {
            let live_json_count = self
                .slots
                .iter()
                .map(|slot| {
                    usize::from(slot.snapshot.is_some()) + usize::from(slot.metadata.is_some())
                })
                .sum::<usize>();
            #[cfg(debug_assertions)]
            if live_json_count == self.json.len() {
                let mut referenced = vec![false; self.json.len()];
                for ordinal in self
                    .slots
                    .iter()
                    .flat_map(|slot| [slot.snapshot, slot.metadata].into_iter().flatten())
                {
                    assert!(
                        !std::mem::replace(&mut referenced[ordinal as usize], true),
                        "prepared JSON ordinals must remain unique"
                    );
                }
            }
            if live_json_count != self.json.len() {
                let old_json = std::mem::take(&mut self.json);
                let mut ordinal_remap = vec![u32::MAX; old_json.len()];
                let mut live_json = Vec::with_capacity(live_json_count);
                for slot in &mut self.slots {
                    for ordinal in [&mut slot.snapshot, &mut slot.metadata] {
                        let Some(old_ordinal) = *ordinal else {
                            continue;
                        };
                        let remapped = &mut ordinal_remap[old_ordinal as usize];
                        if *remapped == u32::MAX {
                            *remapped = u32::try_from(live_json.len())
                                .expect("prepared live JSON ordinal must fit u32");
                            live_json.push(old_json[old_ordinal as usize].clone());
                        }
                        *ordinal = Some(*remapped);
                    }
                }
                self.json = live_json;
            }
        }
        if self.json.is_empty() {
            return;
        }
        for value in &mut self.json {
            value.release_validated_canonical_value_column();
        }
        let mut normalized = self
            .json
            .iter()
            .enumerate()
            .filter_map(|(ordinal, value)| match &value.storage {
                StageJsonStorage::ValidatedShared { normalized } => {
                    Some((ordinal, normalized.clone()))
                }
                StageJsonStorage::ValidatedOwned { .. } => None,
                StageJsonStorage::Owned { .. }
                | StageJsonStorage::CertifiedShared { .. }
                | StageJsonStorage::CanonicalBatch(_) => {
                    unreachable!("all live staged JSON must be released before arena accounting")
                }
            })
            .collect::<Vec<_>>();
        let live_bytes = normalized
            .iter()
            .map(|(_, value)| value.len())
            .sum::<usize>();
        let mut source_buffers = HashSet::with_capacity(normalized.len());
        let retained_bytes = normalized
            .iter()
            .filter_map(|(_, value)| {
                source_buffers
                    .insert(value.retained_buffer_identity())
                    .then_some(value.retained_buffer_len())
            })
            .sum::<usize>();
        if retained_bytes > live_bytes.saturating_mul(2) {
            let mut arena = Vec::with_capacity(live_bytes);
            for (_, value) in &normalized {
                arena.extend_from_slice(value.as_bytes());
            }
            // SAFETY: this arena is the concatenation of validated SharedStr
            // values and therefore remains UTF-8.
            let arena = unsafe { SharedStr::from_utf8_unchecked(Bytes::from(arena)) };
            let mut offset = 0usize;
            for (_, value) in &mut normalized {
                let end = offset + value.len();
                *value = arena
                    .slice(offset..end)
                    .expect("validated canonical JSON row boundary remains UTF-8");
                offset = end;
            }
        }
        for (ordinal, normalized) in normalized {
            self.json[ordinal].storage = StageJsonStorage::ValidatedShared { normalized };
        }
    }

    #[cfg(test)]
    pub(crate) fn slot_allocation_ptr(&self) -> *const () {
        self.slots.as_ptr().cast()
    }

    pub(crate) fn dense_certified_parameter_summary(
        &self,
    ) -> Option<(PreparedRowFacts, &str, &str)> {
        let dense = self.dense_certified_parameter.as_ref()?;
        Some((
            dense.facts,
            self.strings[dense.schema_key as usize].as_str(),
            self.strings[dense.branch_id as usize].as_str(),
        ))
    }

    /// Returns the contiguous snapshot column for a single certified row
    /// generation. Commit-time derived indexes can consume this column
    /// directly instead of first allocating row ordinals and projecting the
    /// same fixed metadata through `PreparedStateRowRef` for every row.
    pub(crate) fn dense_row_columnar_input(&self) -> Option<(CommitId, &str, &[StageJson])> {
        let dense = self.dense_certified_parameter.as_ref()?;
        let commit_id = dense.commit_id?;
        Some((
            commit_id,
            self.strings[dense.schema_key as usize].as_str(),
            &self.json[..dense.len],
        ))
    }

    pub(crate) fn take_dense_row_columnar(
        &mut self,
    ) -> Option<(CommitId, String, crate::sql2::EncodedRowGroups)> {
        let dense = self.dense_certified_parameter.as_mut()?;
        let commit_id = dense.commit_id?;
        let encoded = dense.row_columnar.take()?;
        Some((
            commit_id,
            self.strings[dense.schema_key as usize].to_string(),
            encoded,
        ))
    }

    #[cfg(test)]
    pub(crate) fn is_dense_certified_parameter(&self) -> bool {
        self.dense_certified_parameter.is_some()
    }

    #[cfg(test)]
    pub(crate) fn shared_string_count(&self) -> usize {
        self.strings.len()
    }

    #[cfg(test)]
    pub(crate) fn shared_origin_count(&self) -> usize {
        self.origins.len()
    }

    pub(crate) fn set_requires_transaction_validation(
        &mut self,
        index: usize,
        requires_transaction_validation: bool,
    ) {
        self.expand_dense_certified_parameter();
        self.slots[index].facts.requires_transaction_validation = requires_transaction_validation;
    }

    fn push_row_pk(&mut self, value: RowPk) -> u32 {
        let index =
            u32::try_from(self.row_pks.len()).expect("prepared row column must fit u32");
        self.row_pks.push(value);
        index
    }

    fn push_json(&mut self, value: StageJson) -> u32 {
        let index = u32::try_from(self.json.len()).expect("prepared JSON column must fit u32");
        self.json.push(value);
        index
    }

    fn intern_string(&mut self, value: SharedStr) -> u32 {
        if let Some(index) = self.string_index.get(&value) {
            return *index;
        }
        let index =
            u32::try_from(self.strings.len()).expect("prepared string dictionary must fit u32");
        self.strings.push(value.clone());
        self.string_index.insert(value, index);
        index
    }

    fn intern_string_ref(&mut self, value: &SharedStr) -> u32 {
        if let Some(index) = self.string_index.get(value) {
            return *index;
        }
        self.intern_string(value.clone())
    }

    fn intern_origin(&mut self, mut value: TransactionWriteOrigin) -> u32 {
        let surface = self.intern_string(value.surface);
        value.surface = self.strings[surface as usize].clone();
        if let Some(primary_key) = value.primary_key.take() {
            let primary_key = match Arc::try_unwrap(primary_key) {
                Ok(mut primary_key) => {
                    primary_key.columns = self.intern_origin_columns(primary_key.columns);
                    Arc::new(primary_key)
                }
                Err(primary_key) => {
                    let columns = self.intern_origin_columns(primary_key.columns.clone());
                    if Arc::ptr_eq(&columns, &primary_key.columns) {
                        primary_key
                    } else {
                        Arc::new(LogicalPrimaryKey {
                            columns,
                            values: primary_key.values.clone(),
                        })
                    }
                }
            };
            value.primary_key = Some(primary_key);
        }
        if let Some(index) = self.origin_index.get(&value) {
            return *index;
        }
        let index =
            u32::try_from(self.origins.len()).expect("prepared origin dictionary must fit u32");
        self.origins.push(value.clone());
        self.origin_index.insert(value, index);
        index
    }

    fn intern_origin_columns(&mut self, columns: Arc<[String]>) -> Arc<[String]> {
        if let Some(index) = self.origin_column_index.get(&columns) {
            return Arc::clone(&self.origin_column_sets[*index as usize]);
        }
        let index = u32::try_from(self.origin_column_sets.len())
            .expect("prepared origin descriptor dictionary must fit u32");
        self.origin_column_sets.push(Arc::clone(&columns));
        self.origin_column_index.insert(Arc::clone(&columns), index);
        columns
    }

    fn should_compact_owner_columns(&self, retained_len: usize) -> bool {
        retained_len == 0 || self.row_pks.len() > retained_len.saturating_mul(2)
    }

    fn push_borrowed_row(&mut self, row: PreparedStateRowRef<'_>) {
        #[cfg(feature = "storage-benches")]
        record_prepared_row_ownership(
            row.row_pk,
            row.schema_key,
            row.file_id,
            row.snapshot,
            row.metadata,
            row.origin_key,
            row.branch_id,
            crate::storage_bench::CRUD_OWNERSHIP_PREPARED_CLONE,
        );
        self.push_parts_with_change_addressability(
            row.schema_plan_id,
            row.facts,
            row.row_pk.clone(),
            row.schema_key.clone(),
            row.file_id.cloned(),
            row.snapshot.cloned(),
            row.metadata.cloned(),
            row.origin.cloned(),
            row.origin_key,
            row.created_at,
            row.updated_at,
            row.global,
            row.change_id,
            row.addressable_change_id,
            row.commit_id,
            row.untracked,
            row.branch_id.clone(),
        );
        let index = self.len() - 1;
        self.set_durable_predecessor(index, row.durable_predecessor.cloned());
    }
}

impl Default for PreparedStateBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> Iterator for PreparedStateRows<'a> {
    type Item = PreparedStateRowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(|index| self.batch.row(index))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl DoubleEndedIterator for PreparedStateRows<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.range.next_back().map(|index| self.batch.row(index))
    }
}

impl ExactSizeIterator for PreparedStateRows<'_> {}

impl<'a> IntoIterator for &'a PreparedStateBatch {
    type Item = PreparedStateRowRef<'a>;
    type IntoIter = PreparedStateRows<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl PartialEq for PreparedStateBatch {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .zip(other.iter())
                .all(|(left, right)| left == right)
    }
}

impl Eq for PreparedStateBatch {}

impl PartialEq for PreparedStateRowRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.schema_plan_id == other.schema_plan_id
            && self.facts == other.facts
            && self.row_pk == other.row_pk
            && self.schema_key == other.schema_key
            && self.file_id == other.file_id
            && self.snapshot == other.snapshot
            && self.metadata == other.metadata
            && self.origin == other.origin
            && self.origin_key == other.origin_key
            && self.created_at == other.created_at
            && self.updated_at == other.updated_at
            && self.global == other.global
            && self.change_id == other.change_id
            && self.commit_id == other.commit_id
            && self.untracked == other.untracked
            && self.branch_id == other.branch_id
    }
}

impl Eq for PreparedStateRowRef<'_> {}

impl From<PreparedStateRowRef<'_>> for MaterializedHotStateRow {
    fn from(row: PreparedStateRowRef<'_>) -> Self {
        Self {
            row_pk: row.row_pk.clone(),
            schema_key: row.schema_key.to_string(),
            file_id: row.file_id.map(ToString::to_string),
            snapshot_content: row.snapshot.map(StageJson::materialize_shared),
            metadata: row.metadata.map(StageJson::materialize_shared),
            deleted: row.snapshot.is_none(),
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            change_id: row.change_id,
            commit_id: row.commit_id,
            untracked: row.untracked,
            branch_id: Arc::from(row.branch_id.as_str()),
        }
    }
}

#[cfg(test)]
impl From<TestPreparedStateRow> for MaterializedHotStateRow {
    fn from(row: TestPreparedStateRow) -> Self {
        let deleted = row.snapshot.is_none();
        Self {
            row_pk: row.row_pk,
            schema_key: row.schema_key.into(),
            file_id: row.file_id.map(Into::into),
            snapshot_content: row.snapshot.map(|snapshot| snapshot.materialize_shared()),
            metadata: row.metadata.map(|metadata| metadata.materialize_shared()),
            deleted,
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            change_id: row.change_id,
            commit_id: row.commit_id,
            untracked: row.untracked,
            branch_id: Arc::from(row.branch_id.as_str()),
        }
    }
}

#[cfg(test)]
impl From<&TestPreparedStateRow> for MaterializedHotStateRow {
    fn from(row: &TestPreparedStateRow) -> Self {
        Self {
            row_pk: row.row_pk.clone(),
            schema_key: row.schema_key.to_string(),
            file_id: row.file_id.as_ref().map(ToString::to_string),
            snapshot_content: row.snapshot.as_ref().map(StageJson::materialize_shared),
            metadata: row.metadata.as_ref().map(StageJson::materialize_shared),
            deleted: row.snapshot.is_none(),
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            change_id: row.change_id,
            commit_id: row.commit_id,
            untracked: row.untracked,
            branch_id: Arc::from(row.branch_id.as_str()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expdl_probe_certificate(plan: u32) -> CertifiedRawWriteBatchPreparation {
        CertifiedRawWriteBatchPreparation {
            schema_plan_id: SchemaPlanId::for_test(plan),
            facts: PreparedRowFacts {
                row_content_validated: true,
                requires_transaction_validation: false,
            },
            tracked_keys_strictly_ordered: true,
            complete_collection_replacement: None,
        }
    }

    fn expdl_probe_batch(keys: &[&str], untracked: bool) -> PreparedStateBatch {
        let row_pks = keys.iter().map(|key| RowPk::single(*key)).collect();
        let snapshots = keys
            .iter()
            .map(|key| {
                TransactionJson::from_certified_shared_normalized_row_content(
                    format!(r#"{{"id":"{key}"}}"#).into(),
                )
            })
            .collect::<Vec<_>>();
        CertifiedParameterInsertBatch::new_with_lane(
            row_pks,
            snapshots,
            "expdl_probe".into(),
            "main".into(),
            untracked,
            expdl_probe_certificate(7),
        )
        .expect("certified rows should construct")
        // Deliberately the inner constructor. `into_dense_prepared` asserts
        // that no untracked batch reaches the dense lane, which is the
        // invariant these probes are the backstop for: they prove that if the
        // routing guard ever goes, the projections still agree on the lane
        // instead of silently flipping it.
        .into_dense_prepared_timestamps(
            None,
            DenseParameterTimestamps::Scalar(LixTimestamp::expect_parse(
                "timestamp",
                "2026-08-02T00:00:00.000Z",
            )),
        )
        .expect("certified rows should prepare")
    }

    /// A dense certified batch must report the same durability lane through
    /// both of its projections. The borrowed accessor reads
    /// `dense.untracked`; expansion into row slots must not invent one.
    #[test]
    fn expdl_dense_certified_expansion_preserves_untracked_lane() {
        let mut prepared = expdl_probe_batch(&["a", "b"], true);

        assert!(prepared.is_dense_certified_parameter());
        assert!(
            prepared.iter().all(|row| row.untracked),
            "dense projection must report the untracked lane"
        );

        prepared.set_durable_predecessor(0, None);

        assert!(!prepared.is_dense_certified_parameter());
        assert!(
            prepared.iter().all(|row| row.untracked),
            "expansion must not move untracked rows into the tracked lane"
        );
    }

    /// Two dense cohorts only merge when every batch-common fact matches.
    /// The durability lane is batch-common, so a tracked cohort must never
    /// absorb an untracked one.
    #[test]
    fn expdl_dense_certified_cohort_merge_keeps_lanes_apart() {
        let mut tracked = expdl_probe_batch(&["a", "b"], false);
        let untracked = expdl_probe_batch(&["c", "d"], true);

        tracked.append(untracked);

        assert_eq!(tracked.len(), 4);
        assert_eq!(
            tracked.iter().map(|row| row.untracked).collect::<Vec<_>>(),
            vec![false, false, true, true],
            "merging cohorts must preserve each row's durability lane"
        );
    }

    #[test]
    fn certified_parameter_rows_keep_batch_common_prepared_facts_dense() {
        let row_pks = vec![RowPk::single("a"), RowPk::single("b")];
        let snapshots = [r#"{"id":"a"}"#, r#"{"id":"b"}"#]
            .into_iter()
            .map(|value| {
                TransactionJson::from_certified_shared_normalized_row_content(value.into())
            })
            .collect::<Vec<_>>();
        let certificate = CertifiedRawWriteBatchPreparation {
            schema_plan_id: SchemaPlanId::for_test(7),
            facts: PreparedRowFacts {
                row_content_validated: true,
                requires_transaction_validation: false,
            },
            tracked_keys_strictly_ordered: true,
            complete_collection_replacement: None,
        };
        let timestamp = LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:00.000Z");
        let mut prepared = CertifiedParameterInsertBatch::new(
            row_pks,
            snapshots,
            "dense_probe".into(),
            "main".into(),
            certificate,
        )
        .expect("certified rows should construct")
        .into_dense_prepared(None, timestamp)
        .expect("certified rows should prepare");

        assert!(prepared.is_dense_certified_parameter());
        assert!(prepared.slots.is_empty());
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared.first().unwrap().row_pk, &RowPk::single("a"));
        assert_eq!(
            prepared.last().unwrap().snapshot.unwrap().normalized(),
            r#"{"id":"b"}"#
        );
        assert!(prepared.iter().all(|row| {
            row.schema_key.as_str() == "dense_probe"
                && row.branch_id.as_str() == "main"
                && row.created_at == timestamp
                && row.updated_at == timestamp
                && row.change_id == Some(ChangeId::default())
                && row.addressable_change_id
        }));

        let commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_2468_0000_0000,
        ));
        prepared.set_commit_id_all(commit_id);
        assert!(prepared.iter().all(|row| row.commit_id == Some(commit_id)));
        let assignment = OrderedAddressableCommitDeltaStage::for_test_dense(commit_id, 2);
        let assigned = assignment.assigned_change_ids().collect::<Vec<_>>();
        prepared
            .set_ordered_addressable_change_ids(&[0, 1], assignment)
            .expect("dense direct addresses should assign");
        assert_eq!(
            prepared
                .iter()
                .map(|row| row.change_id.unwrap())
                .collect::<Vec<_>>(),
            assigned
        );

        prepared.set_requires_transaction_validation(0, true);
        assert!(!prepared.is_dense_certified_parameter());
        assert_eq!(prepared.slots.len(), 2);
        assert!(prepared.row(0).facts.requires_transaction_validation);
        assert!(!prepared.row(1).facts.requires_transaction_validation);
        assert_eq!(prepared.row(1).commit_id, Some(commit_id));
        assert_eq!(prepared.row(0).change_id, Some(assigned[0]));
        assert_eq!(prepared.row(1).change_id, Some(assigned[1]));
    }

    #[test]
    fn certified_replacement_keeps_dense_complete_collection_proof() {
        let snapshots = [r#"{"id":"a"}"#, r#"{"id":"b"}"#]
            .into_iter()
            .map(|value| {
                TransactionJson::from_certified_shared_normalized_row_content(value.into())
            })
            .collect::<Vec<_>>();
        let mut prepared = CertifiedParameterReplacementBatch::new(
            vec![RowPk::single("a"), RowPk::single("b")],
            snapshots,
            "dense_replacement".into(),
            "main".into(),
            CertifiedRawWriteBatchPreparation {
                schema_plan_id: SchemaPlanId::for_test(8),
                facts: PreparedRowFacts {
                    row_content_validated: true,
                    requires_transaction_validation: false,
                },
                tracked_keys_strictly_ordered: true,
                complete_collection_replacement: Some(CompleteCollectionReplacementProof {
                    ordered_identity_digest: [7; 32],
                    replay_bytes: 321,
                }),
            },
        )
        .expect("certified replacement should construct")
        .into_dense_prepared(
            None,
            LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:00.000Z"),
        )
        .expect("certified replacement should prepare");

        assert!(prepared.is_dense_certified_parameter());
        assert_eq!(
            prepared.complete_collection_replacement_proof(),
            Some(CompleteCollectionReplacementProof {
                ordered_identity_digest: [7; 32],
                replay_bytes: 321,
            })
        );
        assert!(prepared.certified_tracked_keys_strictly_ordered());
        prepared.truncate_rows(2);
        assert!(prepared.is_dense_certified_parameter());
        assert!(prepared.complete_collection_replacement_proof().is_some());

        prepared.truncate_rows(1);
        assert!(!prepared.is_dense_certified_parameter());
        assert_eq!(prepared.complete_collection_replacement_proof(), None);
        assert!(!prepared.certified_tracked_keys_strictly_ordered());
        assert_eq!(prepared.row(0).row_pk, &RowPk::single("a"));
    }

    #[test]
    fn certified_replacement_keeps_authenticated_predecessors_dense_and_through_expansion() {
        let snapshots = [r#"{"id":"a"}"#, r#"{"id":"b"}"#]
            .into_iter()
            .map(|value| {
                TransactionJson::from_certified_shared_normalized_row_content(value.into())
            })
            .collect::<Vec<_>>();
        let mut rows = CertifiedParameterReplacementBatch::new(
            vec![RowPk::single("a"), RowPk::single("b")],
            snapshots,
            "dense_replacement".into(),
            "main".into(),
            CertifiedRawWriteBatchPreparation {
                schema_plan_id: SchemaPlanId::for_test(10),
                facts: PreparedRowFacts {
                    row_content_validated: true,
                    requires_transaction_validation: false,
                },
                tracked_keys_strictly_ordered: true,
                complete_collection_replacement: None,
            },
        )
        .expect("certified replacement should construct");
        rows.set_durable_predecessor(
            0,
            Some(CertifiedCurrentStatePredecessor::Encoded(
                Bytes::from_static(b"predecessor-a"),
            )),
        );
        rows.set_durable_predecessor(
            1,
            Some(CertifiedCurrentStatePredecessor::Encoded(
                Bytes::from_static(b"predecessor-b"),
            )),
        );

        let mut prepared = rows
            .into_dense_prepared(
                None,
                LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:00.000Z"),
            )
            .expect("certified replacement should prepare");
        assert!(prepared.is_dense_certified_parameter());
        assert!(matches!(
            prepared.row(1).durable_predecessor,
            Some(CertifiedCurrentStatePredecessor::Encoded(bytes))
                if bytes.as_ref() == b"predecessor-b"
        ));

        prepared.set_requires_transaction_validation(0, true);
        assert!(!prepared.is_dense_certified_parameter());
        assert!(matches!(
            prepared.row(0).durable_predecessor,
            Some(CertifiedCurrentStatePredecessor::Encoded(bytes))
                if bytes.as_ref() == b"predecessor-a"
        ));
        assert!(matches!(
            prepared.row(1).durable_predecessor,
            Some(CertifiedCurrentStatePredecessor::Encoded(bytes))
                if bytes.as_ref() == b"predecessor-b"
        ));
    }

    #[test]
    fn compatible_dense_replacements_coalesce_with_per_row_timestamps() {
        let certificate = CertifiedRawWriteBatchPreparation {
            schema_plan_id: SchemaPlanId::for_test(9),
            facts: PreparedRowFacts {
                row_content_validated: true,
                requires_transaction_validation: false,
            },
            tracked_keys_strictly_ordered: true,
            complete_collection_replacement: None,
        };
        let prepare = |key: &str, timestamp| {
            CertifiedParameterReplacementBatch::new(
                vec![RowPk::single(key)],
                vec![
                    TransactionJson::from_certified_shared_normalized_row_content(
                        format!(r#"{{"id":"{key}"}}"#).into(),
                    ),
                ],
                "dense_replacement".into(),
                "main".into(),
                certificate,
            )
            .expect("certified replacement should construct")
            .into_dense_prepared(None, timestamp)
            .expect("certified replacement should prepare")
        };
        let first_timestamp = LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:00.000Z");
        let second_timestamp = LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:01.000Z");
        let commit_id = CommitId::for_test_label("dense-coalesced-replacements");
        let mut prepared = prepare("a", first_timestamp);
        prepared.set_commit_id_all(commit_id);
        let mut second = prepare("b", second_timestamp);
        second.set_commit_id_all(commit_id);

        prepared.append(second);

        assert!(prepared.is_dense_certified_parameter());
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared.row(0).created_at, first_timestamp);
        assert_eq!(prepared.row(1).created_at, second_timestamp);
        assert_eq!(prepared.row(0).commit_id, Some(commit_id));
        assert_eq!(prepared.row(1).commit_id, Some(commit_id));
        assert!(prepared.certified_tracked_keys_strictly_ordered());
        let ordered_identity_digest =
            crate::collection_generation::ordered_single_string_identity_digest(
                prepared.iter().map(|row| row.row_pk),
            )
            .expect("single-string identities should hash");
        assert!(prepared.certify_complete_collection_replacement(
            "dense_replacement",
            "main",
            2,
            ordered_identity_digest,
        ));
        assert_eq!(prepared.row(0).updated_at, first_timestamp);
        assert_eq!(prepared.row(1).updated_at, first_timestamp);
        let (columnar_commit_id, schema_key, snapshots) = prepared
            .dense_row_columnar_input()
            .expect("coalesced replacement retains contiguous columnar input");
        assert_eq!(columnar_commit_id, commit_id);
        assert_eq!(schema_key, "dense_replacement");
        assert_eq!(snapshots.len(), 2);

        prepared.append(prepare("a", second_timestamp));
        assert!(!prepared.is_dense_certified_parameter());
        assert!(!prepared.certified_tracked_keys_strictly_ordered());
        assert_eq!(prepared.len(), 3);
    }

    #[test]
    fn complete_replacement_certificate_rejects_rows_outside_collection_scope() {
        let certificate = CertifiedRawWriteBatchPreparation {
            schema_plan_id: SchemaPlanId::for_test(10),
            facts: PreparedRowFacts {
                row_content_validated: true,
                requires_transaction_validation: false,
            },
            tracked_keys_strictly_ordered: true,
            complete_collection_replacement: None,
        };
        let prepare = |schema_key: &str, key: &str, timestamp| {
            CertifiedParameterReplacementBatch::new(
                vec![RowPk::single(key)],
                vec![
                    TransactionJson::from_certified_shared_normalized_row_content(
                        format!(r#"{{"id":"{key}"}}"#).into(),
                    ),
                ],
                schema_key.into(),
                "main".into(),
                certificate,
            )
            .expect("certified replacement should construct")
            .into_dense_prepared(None, timestamp)
            .expect("certified replacement should prepare")
        };
        let first_timestamp = LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:00.000Z");
        let second_timestamp = LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:01.000Z");
        let mut prepared = prepare("other_schema", "a", first_timestamp);
        prepared.append(prepare("target_schema", "b", second_timestamp));
        let ordered_identity_digest =
            crate::collection_generation::ordered_single_string_identity_digest(
                prepared.iter().map(|row| row.row_pk),
            )
            .expect("single-string identities should hash");

        assert!(!prepared.certify_complete_collection_replacement(
            "target_schema",
            "main",
            2,
            ordered_identity_digest,
        ));
        assert_eq!(prepared.row(0).updated_at, first_timestamp);
        assert_eq!(prepared.row(1).updated_at, second_timestamp);
        assert!(prepared.complete_collection_replacement_proof().is_none());
    }

    fn prepared_fixture_row(
        row: &str,
        snapshot: Option<StageJson>,
        operation: TransactionWriteOperation,
        origin_key: &SharedStr,
    ) -> TestPreparedStateRow {
        let timestamp = LixTimestamp::expect_parse("timestamp", "2026-07-28T00:00:00.000Z");
        TestPreparedStateRow {
            schema_plan_id: SchemaPlanId::for_test(0),
            facts: PreparedRowFacts::default(),
            row_pk: RowPk::single(row),
            schema_key: "shared_schema".into(),
            file_id: Some("shared_file".into()),
            snapshot,
            metadata: None,
            origin: Some(TransactionWriteOrigin {
                surface: "shared_surface".into(),
                operation,
                primary_key: None,
            }),
            origin_key: Some(origin_key.clone()),
            created_at: timestamp,
            updated_at: timestamp,
            global: false,
            change_id: Some(ChangeId::for_test_label(row)),
            commit_id: None,
            untracked: false,
            branch_id: "shared_branch".into(),
        }
    }

    #[test]
    fn ten_thousand_raw_rows_share_dictionaries_arenas_and_retain_in_place() {
        const ROW_COUNT: usize = 10_000;
        let mut values = Vec::with_capacity(ROW_COUNT);
        let mut normalized = Vec::new();
        let mut offsets = Vec::with_capacity(ROW_COUNT);
        for index in 0..ROW_COUNT {
            let value = serde_json::json!({"id": index});
            let encoded = serde_json::to_vec(&value).expect("fixture should serialize");
            let start = u32::try_from(normalized.len()).expect("fixture offset");
            normalized.extend_from_slice(&encoded);
            let end = u32::try_from(normalized.len()).expect("fixture offset");
            values.push(value);
            offsets.push((start, end));
        }
        let canonical =
            WasmCanonicalJson::from_batch_parts(values, normalized, offsets, ROW_COUNT, ROW_COUNT)
                .expect("canonical raw fixture");
        let mut rows = RawWriteBatch::with_capacity(ROW_COUNT);
        let owner_pointers = rows.aligned_owner_allocation_ptrs();
        let owner_capacities = rows.aligned_owner_capacities();
        let schema_key = SharedStr::from("bulk_schema");
        let file_id = SharedStr::from("bulk_file");
        let timestamp = SharedStr::from("2026-07-28T00:00:00.000Z");
        let branch_id = SharedStr::from("bulk_branch");
        let origin = TransactionWriteOrigin {
            surface: SharedStr::from("bulk_surface"),
            operation: TransactionWriteOperation::Update,
            primary_key: None,
        };
        for (index, snapshot) in canonical.into_iter().enumerate() {
            rows.push_parts(
                Some(RowPk::single(index.to_string())),
                schema_key.clone(),
                Some(file_id.clone()),
                Some(TransactionJson::from_canonical_batch(snapshot)),
                None,
                Some(origin.clone()),
                Some(timestamp.clone()),
                Some(timestamp.clone()),
                false,
                None,
                None,
                false,
                branch_id.clone(),
            );
        }

        assert_eq!(rows.aligned_owner_allocation_ptrs(), owner_pointers);
        assert_eq!(rows.aligned_owner_capacities(), owner_capacities);
        assert_eq!(rows.shared_string_count(), 4);
        assert_eq!(rows.shared_origin_count(), 1);
        assert!(
            !rows.string_dictionary_is_promoted(),
            "four shared values must stay in the inline dictionary"
        );
        assert!(
            !rows.origin_dictionary_is_promoted(),
            "one shared origin must stay in the inline dictionary"
        );
        let first = rows
            .row(0)
            .snapshot
            .and_then(TransactionJson::canonical_batch_row)
            .expect("first canonical raw row");
        let last = rows
            .row(ROW_COUNT - 1)
            .snapshot
            .and_then(TransactionJson::canonical_batch_row)
            .expect("last canonical raw row");
        assert!(first.shares_batch_with(last));

        let mut source_index = 0usize;
        rows.retain(|_| {
            let keep = source_index % 2 == 0;
            source_index += 1;
            keep
        });
        assert_eq!(rows.len(), ROW_COUNT / 2);
        assert_eq!(rows.aligned_owner_allocation_ptrs(), owner_pointers);
        assert_eq!(rows.aligned_owner_capacities(), owner_capacities);
        for (destination, row) in rows.iter().enumerate() {
            assert_eq!(
                row.row_pk
                    .expect("retained raw identity")
                    .as_single_string()
                    .expect("single raw identity"),
                (destination * 2).to_string()
            );
        }
        let first = rows
            .row(0)
            .snapshot
            .and_then(TransactionJson::canonical_batch_row)
            .expect("retained first canonical row");
        let last = rows
            .row(rows.len() - 1)
            .snapshot
            .and_then(TransactionJson::canonical_batch_row)
            .expect("retained last canonical row");
        assert!(first.shares_batch_with(last));
    }

    #[test]
    fn zero_capacity_append_reserves_ten_thousand_row_cardinal_dictionaries_once() {
        const ROW_COUNT: usize = 10_000;
        let mut source = RawWriteBatch::with_capacity(ROW_COUNT);
        let schema_key = SharedStr::from("bulk_schema");
        let branch_id = SharedStr::from("bulk_branch");
        for index in 0..ROW_COUNT {
            source.push_parts(
                Some(RowPk::single(index.to_string())),
                schema_key.clone(),
                Some(SharedStr::from(format!("file-{index:05}"))),
                None,
                None,
                Some(TransactionWriteOrigin {
                    surface: SharedStr::from(format!("surface-{index:05}")),
                    operation: TransactionWriteOperation::Insert,
                    primary_key: None,
                }),
                None,
                None,
                false,
                None,
                None,
                false,
                branch_id.clone(),
            );
        }

        let mut rows = RawWriteBatch::new();
        rows.append(source);

        assert_eq!(rows.len(), ROW_COUNT);
        assert_eq!(
            rows.dictionary_promotion_counts(),
            [1, 1],
            "each row-cardinal dictionary promotes exactly once"
        );
        let [string_values, string_index, origin_values, origin_index] =
            rows.dictionary_capacities();
        assert!(string_values >= ROW_COUNT + 2);
        assert!(string_index >= ROW_COUNT + 2);
        assert!(
            string_values < ROW_COUNT * 2,
            "the string values buffer must not geometrically grow after promotion"
        );
        assert!(
            string_index < ROW_COUNT * 2,
            "the string index must not geometrically grow after promotion"
        );
        assert!(origin_values >= ROW_COUNT);
        assert!(origin_index >= ROW_COUNT);
        assert!(
            rows.aligned_owner_capacities()
                .into_iter()
                .all(|capacity| capacity >= ROW_COUNT),
            "zero-capacity destination reserves every aligned owner once before moving"
        );
        assert_eq!(
            rows.row(0).file_id.map(SharedStr::as_str),
            Some("file-00000")
        );
        assert_eq!(
            rows.row(ROW_COUNT - 1).file_id.map(SharedStr::as_str),
            Some("file-09999")
        );
    }

    #[test]
    fn prepared_batch_interns_metadata_and_exposes_borrowed_crud_views() {
        let normalized = br#"{"id":"a"}{"id":"b"}"#.to_vec();
        let first_end = u32::try_from(br#"{"id":"a"}"#.len()).expect("fixture length");
        let end = u32::try_from(normalized.len()).expect("fixture length");
        let mut canonical = WasmCanonicalJson::from_batch_parts(
            vec![
                serde_json::json!({"id": "a"}),
                serde_json::json!({"id": "b"}),
            ],
            normalized,
            vec![(0, first_end), (first_end, end)],
            2,
            2,
        )
        .expect("canonical batch");
        let second = canonical.pop().expect("second canonical row");
        let first = canonical.pop().expect("first canonical row");
        let origin_key: SharedStr = "one-execution".into();
        let batch = PreparedStateBatch::from_test_rows(vec![
            prepared_fixture_row(
                "a",
                Some(
                    stage_json_from_value(
                        TransactionJson::from_canonical_batch(first),
                        "insert fixture",
                    )
                    .expect("insert fixture should stage"),
                ),
                TransactionWriteOperation::Insert,
                &origin_key,
            ),
            prepared_fixture_row(
                "b",
                Some(
                    stage_json_from_value(
                        TransactionJson::from_canonical_batch(second),
                        "update fixture",
                    )
                    .expect("update fixture should stage"),
                ),
                TransactionWriteOperation::Update,
                &origin_key,
            ),
            prepared_fixture_row("c", None, TransactionWriteOperation::Delete, &origin_key),
        ]);

        assert_eq!(batch.shared_string_count(), 5);
        assert_eq!(batch.shared_origin_count(), 3);
        let rows = batch.iter().collect::<Vec<_>>();
        assert_eq!(
            rows.iter()
                .map(|row| row.origin.expect("origin").operation)
                .collect::<Vec<_>>(),
            [
                TransactionWriteOperation::Insert,
                TransactionWriteOperation::Update,
                TransactionWriteOperation::Delete,
            ]
        );
        assert!(rows[0].snapshot.is_some());
        assert!(rows[1].snapshot.is_some());
        assert!(rows[2].snapshot.is_none());
        assert!(std::ptr::eq(rows[0].schema_key, rows[2].schema_key));
        assert!(std::ptr::eq(
            rows[0].origin_key.expect("origin key"),
            rows[2].origin_key.expect("origin key")
        ));
        let first_canonical = rows[0]
            .snapshot
            .and_then(StageJson::canonical_batch_row)
            .expect("first canonical view");
        let second_canonical = rows[1]
            .snapshot
            .and_then(StageJson::canonical_batch_row)
            .expect("second canonical view");
        assert!(first_canonical.shares_batch_with(second_canonical));
    }

    #[test]
    fn prepared_batch_compacts_superseded_owner_columns() {
        let origin_key: SharedStr = "one-execution".into();
        let staged_snapshot = |value: String| {
            stage_json_from_value(
                TransactionJson::from_certified_shared_normalized_row_content(value.into()),
                "compaction fixture",
            )
            .expect("fixture should stage")
        };
        let mut batch = PreparedStateBatch::from_test_rows(vec![
            prepared_fixture_row(
                "a",
                Some(staged_snapshot(r#"{"id":"a","version":0}"#.to_string())),
                TransactionWriteOperation::Update,
                &origin_key,
            ),
            prepared_fixture_row(
                "b",
                Some(staged_snapshot(r#"{"id":"b"}"#.to_string())),
                TransactionWriteOperation::Update,
                &origin_key,
            ),
        ]);

        for version in 1..=64 {
            batch.append(PreparedStateBatch::from_test_rows(vec![
                prepared_fixture_row(
                    "a",
                    Some(staged_snapshot(format!(
                        r#"{{"id":"a","version":{version}}}"#
                    ))),
                    TransactionWriteOperation::Update,
                    &origin_key,
                ),
            ]));
            batch.select_rows(&[2, 1]);
            assert_eq!(batch.len(), 2);
            assert!(batch.row_pks.len() <= 4);
            assert!(batch.json.len() <= 4);
            assert!(batch.strings.len() <= 8);
            assert!(batch.origins.len() <= 2);
        }

        assert_eq!(
            batch
                .row(0)
                .snapshot
                .expect("replacement snapshot")
                .normalized(),
            r#"{"id":"a","version":64}"#
        );
        assert_eq!(
            batch
                .row(1)
                .row_pk
                .as_single_string()
                .expect("scalar pk"),
            "b"
        );
    }

    #[test]
    fn prepared_batch_interns_origin_descriptors_across_unique_primary_keys() {
        let origin_key: SharedStr = "one-execution".into();
        let mut first =
            prepared_fixture_row("a", None, TransactionWriteOperation::Insert, &origin_key);
        first.origin = Some(TransactionWriteOrigin {
            surface: String::from("lix_file").into(),
            operation: TransactionWriteOperation::Insert,
            primary_key: Some(Arc::new(LogicalPrimaryKey {
                columns: vec!["id".to_string()].into(),
                values: vec!["file-a".to_string()],
            })),
        });
        let mut second =
            prepared_fixture_row("b", None, TransactionWriteOperation::Insert, &origin_key);
        second.origin = Some(TransactionWriteOrigin {
            surface: String::from("lix_file").into(),
            operation: TransactionWriteOperation::Insert,
            primary_key: Some(Arc::new(LogicalPrimaryKey {
                columns: vec!["id".to_string()].into(),
                values: vec!["file-b".to_string()],
            })),
        });

        let batch = PreparedStateBatch::from_test_rows(vec![first, second]);
        let first = batch.row(0).origin.expect("first origin");
        let second = batch.row(1).origin.expect("second origin");
        let first_key = first.primary_key.as_ref().expect("first logical key");
        let second_key = second.primary_key.as_ref().expect("second logical key");
        assert_eq!(batch.origins.len(), 2, "row-local PK values stay distinct");
        assert_eq!(batch.origin_column_sets.len(), 1);
        assert!(first.surface.shares_buffer_with(&second.surface));
        assert!(Arc::ptr_eq(&first_key.columns, &second_key.columns));
        assert_eq!(first_key.values, ["file-a"]);
        assert_eq!(second_key.values, ["file-b"]);
    }

    #[test]
    fn prepared_batch_reserves_dense_columns_without_overreserving_sparse_dictionaries() {
        let row_count = 64;
        let mut batch = PreparedStateBatch::with_capacity(row_count);
        let initial_dense_capacities = (
            batch.slots.capacity(),
            batch.row_pks.capacity(),
            batch.json.capacity(),
        );
        let initial_dense_pointers = (
            batch.slots.as_ptr(),
            batch.row_pks.as_ptr(),
            batch.json.as_ptr(),
        );
        assert!(batch.strings.capacity() >= row_count);
        assert!(batch.strings.capacity() < row_count * 5);
        assert!(batch.string_index.capacity() >= row_count);
        assert!(batch.string_index.capacity() < row_count * 5);
        assert_eq!(batch.origins.capacity(), 0);
        assert_eq!(batch.origin_index.capacity(), 0);
        assert_eq!(batch.origin_column_sets.capacity(), 0);
        assert_eq!(batch.origin_column_index.capacity(), 0);
        for index in 0..row_count {
            let timestamp = LixTimestamp::expect_parse("timestamp", "2026-07-28T00:00:00.000Z");
            let snapshot = stage_json_from_value(
                TransactionJson::from_certified_shared_normalized_row_content(
                    format!(r#"{{"id":"row-{index}"}}"#).into(),
                ),
                "capacity snapshot",
            )
            .expect("snapshot should stage");
            let metadata = stage_json_from_value(
                TransactionJson::from_certified_shared_normalized_row_content(
                    format!(r#"{{"row":{index}}}"#).into(),
                ),
                "capacity metadata",
            )
            .expect("metadata should stage");
            batch.push_test_row(TestPreparedStateRow {
                schema_plan_id: SchemaPlanId::for_test(0),
                facts: PreparedRowFacts::default(),
                row_pk: RowPk::single(format!("row-{index}")),
                schema_key: format!("schema-{index}").into(),
                file_id: Some(format!("file-{index}").into()),
                snapshot: Some(snapshot),
                metadata: Some(metadata),
                origin: Some(TransactionWriteOrigin {
                    surface: format!("surface-{index}").into(),
                    operation: TransactionWriteOperation::Update,
                    primary_key: None,
                }),
                origin_key: Some(format!("origin-key-{index}").into()),
                created_at: timestamp,
                updated_at: timestamp,
                global: false,
                change_id: Some(ChangeId::for_test_label(&format!("change-{index}"))),
                commit_id: None,
                untracked: false,
                branch_id: format!("branch-{index}").into(),
            });
        }
        assert_eq!(
            (
                batch.slots.capacity(),
                batch.row_pks.capacity(),
                batch.json.capacity(),
            ),
            initial_dense_capacities
        );
        assert_eq!(
            (
                batch.slots.as_ptr(),
                batch.row_pks.as_ptr(),
                batch.json.as_ptr(),
            ),
            initial_dense_pointers
        );
        assert_eq!(batch.strings.len(), row_count * 5);
        assert_eq!(batch.origins.len(), row_count);
    }

    #[test]
    fn ten_thousand_write_row_clones_retain_identifier_buffers() {
        let schema_key = SharedStr::from("bulk_schema");
        let file_id = SharedStr::from("01920000-0000-7000-8000-0000000000a2");
        let branch_id = SharedStr::from("01920000-0000-7000-8000-0000000000a1");
        let origin_surface = SharedStr::from("bulk_surface");
        let logical_primary_key = Arc::new(LogicalPrimaryKey {
            columns: vec!["id".to_string()].into(),
            values: vec!["logical-row-1".to_string()],
        });
        let origin = TransactionWriteOrigin {
            surface: origin_surface.clone(),
            operation: TransactionWriteOperation::Insert,
            primary_key: Some(Arc::clone(&logical_primary_key)),
        };
        let row = TransactionWriteRow {
            row_pk: Some(RowPk::single("row-1")),
            schema_key: schema_key.clone(),
            file_id: Some(file_id.clone()),
            snapshot: None,
            metadata: None,
            origin: Some(origin.clone()),
            created_at: None,
            updated_at: None,
            global: false,
            change_id: None,
            commit_id: None,
            untracked: false,
            branch_id: branch_id.clone(),
        };

        let rows = vec![row; 10_000];
        for row in &rows {
            assert!(row.schema_key.shares_buffer_with(&schema_key));
            assert!(
                row.file_id
                    .as_ref()
                    .expect("fixture file id")
                    .shares_buffer_with(&file_id)
            );
            assert!(row.branch_id.shares_buffer_with(&branch_id));
            let row_origin = row.origin.as_ref().expect("fixture origin");
            assert!(row_origin.surface.shares_buffer_with(&origin_surface));
            assert!(Arc::ptr_eq(
                row_origin
                    .primary_key
                    .as_ref()
                    .expect("fixture logical primary key"),
                &logical_primary_key
            ));
        }

        let encoded = serde_json::to_vec(&origin).expect("origin should serialize");
        let decoded: TransactionWriteOrigin =
            serde_json::from_slice(&encoded).expect("origin should deserialize");
        assert_eq!(decoded, origin);
    }

    #[test]
    fn certified_normalized_content_can_materialize_from_the_prepared_boundary() {
        let transaction_json = TransactionJson::from_certified_normalized_row_content(
            r#"{"path":"/a","value":{"nested":true}}"#.into(),
        );
        assert!(transaction_json.row_content_certified());

        let staged = stage_json_from_value(transaction_json, "certified test row")
            .expect("certified JSON should prepare");
        assert_eq!(
            staged.value(),
            &serde_json::json!({"path": "/a", "value": {"nested": true}})
        );
        assert_eq!(
            staged.json_ref,
            JsonRef::default(),
            "inline JSON must not pay for an unused content hash"
        );
    }

    #[test]
    fn out_of_band_json_retains_its_content_hash() {
        let normalized = format!(
            r#"{{"value":"{}"}}"#,
            "x".repeat(crate::json_store::JSON_INLINE_MAX_BYTES)
        );
        let expected = JsonRef::for_content(normalized.as_bytes());
        let staged = stage_json_from_value(
            TransactionJson::from_certified_normalized_row_content(normalized.into()),
            "large certified test row",
        )
        .expect("large certified JSON should prepare");

        assert!(!staged.is_inline());
        assert_eq!(staged.json_ref, expected);
    }

    #[test]
    fn canonical_batch_row_stays_shared_at_the_prepared_boundary() {
        let normalized = br#"{"id":"row-1","value":"hello"}"#.to_vec();
        let end = u32::try_from(normalized.len()).expect("fixture length");
        let mut batch = WasmCanonicalJson::from_batch_parts(
            vec![serde_json::json!({"id": "row-1", "value": "hello"})],
            normalized,
            vec![(0, end)],
            1,
            1,
        )
        .expect("canonical batch");
        let batch_row = batch.pop().expect("canonical row");
        let transaction_json = TransactionJson::from_canonical_batch(batch_row.clone());

        let staged =
            stage_json_from_value(transaction_json, "canonical batch row").expect("stage JSON");
        let staged_batch_row = staged
            .canonical_batch_row()
            .expect("staging must retain canonical batch ownership");
        assert!(batch_row.shares_batch_with(staged_batch_row));
        assert_eq!(staged.normalized(), r#"{"id":"row-1","value":"hello"}"#);
        assert_eq!(staged.value(), batch_row.value());
        assert_eq!(staged_batch_row.validation_counts(), (1, 1));
        let source = batch_row.normalized_shared();
        let materialized = staged.materialize_shared();
        assert!(source.shares_buffer_with(&materialized));
        assert_eq!(
            source.retained_buffer_len(),
            materialized.retained_buffer_len()
        );
    }

    #[test]
    fn validated_stage_releases_the_parsed_column_and_keeps_the_canonical_arena() {
        let normalized = br#"{"id":"row-1","value":"hello"}"#.to_vec();
        let end = u32::try_from(normalized.len()).expect("fixture length");
        let mut batch = WasmCanonicalJson::from_batch_parts(
            vec![serde_json::json!({"id": "row-1", "value": "hello"})],
            normalized,
            vec![(0, end)],
            1,
            1,
        )
        .expect("canonical batch");
        let batch_row = batch.pop().expect("canonical row");
        let source = batch_row.normalized_shared();
        let mut staged = stage_json_from_value(
            TransactionJson::from_canonical_batch(batch_row),
            "validated canonical batch row",
        )
        .expect("stage JSON");

        assert!(staged.release_validated_canonical_value_column());
        assert!(!staged.release_validated_canonical_value_column());
        assert!(staged.canonical_batch_row().is_none());
        assert_eq!(staged.normalized(), r#"{"id":"row-1","value":"hello"}"#);
        let materialized = staged.materialize_shared();
        assert!(source.shares_buffer_with(&materialized));
    }

    #[test]
    fn validated_stage_releases_a_parsed_shared_canonical_value() {
        let source: SharedStr = r#"{"id":"row-1","value":"hello"}"#.into();
        let transaction_json =
            TransactionJson::from_unvalidated_shared_normalized_content(source.clone());
        assert_eq!(
            transaction_json.value(),
            &serde_json::json!({"id": "row-1", "value": "hello"})
        );
        let mut staged = stage_json_from_value(transaction_json, "shared canonical row")
            .expect("shared canonical JSON should stage");
        assert!(staged.retains_decoded_value_for_tests());

        assert!(staged.release_validated_canonical_value_column());
        assert!(!staged.retains_decoded_value_for_tests());
        assert_eq!(staged.normalized(), source.as_str());
        assert!(source.shares_buffer_with(&staged.materialize_shared()));
        assert!(
            std::panic::catch_unwind(|| staged.value()).is_err(),
            "validated shared JSON must not retain or reparse a decoded value"
        );
    }

    #[test]
    fn validated_batch_keeps_certified_owned_json_zero_copy() {
        const ROW_COUNT: usize = 32;
        let sources = (0..ROW_COUNT)
            .map(|index| {
                Arc::<str>::from(format!(
                    r#"{{"id":"row-{index}","padding":"{}"}}"#,
                    "x".repeat(128)
                ))
            })
            .collect::<Vec<_>>();
        let origin_key: SharedStr = "direct-owned".into();
        let mut batch = PreparedStateBatch::from_test_rows(
            sources
                .iter()
                .enumerate()
                .map(|(index, source)| {
                    prepared_fixture_row(
                        &format!("row-{index}"),
                        Some(
                            stage_json_from_value(
                                TransactionJson::from_certified_normalized_row_content(Arc::clone(
                                    source,
                                )),
                                "certified direct replacement",
                            )
                            .expect("certified direct JSON should stage"),
                        ),
                        TransactionWriteOperation::Update,
                        &origin_key,
                    )
                })
                .collect(),
        );

        batch.release_validated_canonical_value_columns();

        for (row, source) in batch.iter().zip(&sources) {
            let snapshot = row.snapshot.expect("direct replacement snapshot");
            assert_eq!(
                snapshot.normalized().as_ptr(),
                source.as_ptr(),
                "validation release must retain the incoming Arc<str> allocation"
            );
            assert!(!snapshot.retains_decoded_value_for_tests());
        }
        assert!(
            std::panic::catch_unwind(|| {
                batch
                    .row(0)
                    .snapshot
                    .expect("direct replacement snapshot")
                    .value()
            })
            .is_err(),
            "validated owned JSON must not retain or reparse a decoded value"
        );
    }

    #[test]
    fn dense_certified_transaction_arena_releases_in_place() {
        const ROW_COUNT: usize = 32;
        let mut normalized = Vec::new();
        let mut offsets = Vec::with_capacity(ROW_COUNT);
        for index in 0..ROW_COUNT {
            let start = normalized.len();
            serde_json::to_writer(
                &mut normalized,
                &serde_json::json!({"id": format!("row-{index}")}),
            )
            .expect("fixture should serialize");
            offsets.push((start, normalized.len()));
        }
        let snapshots = TransactionJson::from_certified_row_content_arena(normalized, offsets)
            .expect("certified transaction arena");
        assert!(snapshots.iter().all(TransactionJson::row_content_certified));
        let origin_key: SharedStr = "dense-certified-arena".into();
        let mut batch = PreparedStateBatch::from_test_rows(
            snapshots
                .into_iter()
                .enumerate()
                .map(|(index, snapshot)| {
                    prepared_fixture_row(
                        &format!("row-{index}"),
                        Some(
                            stage_json_from_value(snapshot, "certified transaction arena")
                                .expect("certified JSON should stage"),
                        ),
                        TransactionWriteOperation::Insert,
                        &origin_key,
                    )
                })
                .collect(),
        );
        let json_allocation = batch.json.as_ptr();
        let first = batch
            .row(0)
            .snapshot
            .expect("first snapshot")
            .materialize_shared();
        assert!(batch.iter().skip(1).all(|row| {
            first.shares_buffer_with(&row.snapshot.expect("snapshot").materialize_shared())
        }));

        batch.release_validated_canonical_value_columns();

        assert_eq!(
            batch.json.as_ptr(),
            json_allocation,
            "a dense JSON owner column must not be cloned during validation release"
        );
        assert!(batch.iter().all(|row| {
            !row.snapshot
                .expect("snapshot")
                .retains_decoded_value_for_tests()
        }));
    }

    #[test]
    fn certified_transaction_arena_preserves_utf8_and_offset_validation() {
        assert!(
            TransactionJson::from_certified_row_content_arena(vec![0xff], vec![(0, 1)]).is_err(),
            "the fallible constructor must reject invalid producer bytes"
        );

        let normalized = "é".as_bytes().to_vec();
        let split_offsets = vec![(0, 1), (1, normalized.len())];
        assert!(
            TransactionJson::from_certified_row_content_arena(
                normalized.clone(),
                split_offsets.clone(),
            )
            .is_err(),
            "the fallible constructor must reject offsets inside a UTF-8 scalar"
        );
        // SAFETY: the complete arena is valid UTF-8. This intentionally gives
        // the trusted constructor bad offsets to verify it still checks them.
        assert!(
            unsafe {
                TransactionJson::from_validated_certified_row_content_arena(
                    normalized,
                    split_offsets,
                )
            }
            .is_err(),
            "the trusted constructor must retain UTF-8 boundary checks"
        );
    }

    #[test]
    fn certified_row_content_remains_decodable_until_validation_release() {
        let normalized = br#"{"id":"row-1"}"#.to_vec();
        let normalized_len = normalized.len();
        let snapshot = TransactionJson::from_certified_row_content_arena(
            normalized,
            vec![(0, normalized_len)],
        )
        .expect("certified transaction arena")
        .pop()
        .expect("certified row");
        let mut staged =
            stage_json_from_value(snapshot, "certified transaction arena").expect("staged JSON");

        assert!(!staged.retains_decoded_value_for_tests());
        assert_eq!(staged.value()["id"], "row-1");
        assert!(staged.retains_decoded_value_for_tests());
        assert!(staged.release_validated_canonical_value_column());
        assert!(!staged.retains_decoded_value_for_tests());
        assert_eq!(staged.normalized(), r#"{"id":"row-1"}"#);
    }

    #[test]
    fn validated_batch_repacks_a_sparse_canonical_source_arena() {
        const ROW_COUNT: usize = 32;
        let mut values = Vec::with_capacity(ROW_COUNT);
        let mut normalized = Vec::new();
        let mut offsets = Vec::with_capacity(ROW_COUNT);
        for index in 0..ROW_COUNT {
            let value = serde_json::json!({
                "id": format!("row-{index}"),
                "padding": "x".repeat(256),
            });
            let encoded = serde_json::to_vec(&value).expect("fixture should serialize");
            let start = u32::try_from(normalized.len()).expect("fixture offset");
            normalized.extend_from_slice(&encoded);
            let end = u32::try_from(normalized.len()).expect("fixture offset");
            offsets.push((start, end));
            values.push(value);
        }
        let source_arena_len = normalized.len();
        let canonical =
            WasmCanonicalJson::from_batch_parts(values, normalized, offsets, ROW_COUNT, ROW_COUNT)
                .expect("canonical fixture");
        let origin_key: SharedStr = "sparse-arena".into();
        let mut batch = PreparedStateBatch::from_test_rows(
            canonical
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    prepared_fixture_row(
                        &format!("row-{index}"),
                        Some(
                            stage_json_from_value(
                                TransactionJson::from_canonical_batch(value),
                                "sparse canonical fixture",
                            )
                            .expect("fixture should stage"),
                        ),
                        TransactionWriteOperation::Update,
                        &origin_key,
                    )
                })
                .collect(),
        );
        batch.select_rows(&[0]);
        let live_len = batch
            .row(0)
            .snapshot
            .expect("selected snapshot")
            .normalized()
            .len();
        assert_eq!(
            batch
                .row(0)
                .snapshot
                .expect("selected snapshot")
                .materialize_shared()
                .retained_buffer_len(),
            source_arena_len,
            "slot compaction alone still retains the source page"
        );

        batch.release_validated_canonical_value_columns();
        let retained = batch
            .row(0)
            .snapshot
            .expect("selected snapshot")
            .materialize_shared();
        assert_eq!(retained.len(), live_len);
        assert_eq!(
            retained.retained_buffer_len(),
            live_len,
            "post-validation compaction must not retain dead canonical page bytes"
        );
    }

    #[test]
    fn validated_batch_prunes_skewed_dead_json_before_arena_accounting() {
        const ROW_COUNT: usize = 100;
        const LIVE_COUNT: usize = 51;
        let mut values = Vec::with_capacity(ROW_COUNT);
        let mut normalized = Vec::new();
        let mut offsets = Vec::with_capacity(ROW_COUNT);
        for index in 0..ROW_COUNT {
            let value = if index < LIVE_COUNT {
                serde_json::json!({"id": format!("live-{index}")})
            } else {
                serde_json::json!({
                    "id": format!("dead-{index}"),
                    "padding": "x".repeat(2048),
                })
            };
            let encoded = serde_json::to_vec(&value).expect("fixture should serialize");
            let start = u32::try_from(normalized.len()).expect("fixture offset");
            normalized.extend_from_slice(&encoded);
            let end = u32::try_from(normalized.len()).expect("fixture offset");
            offsets.push((start, end));
            values.push(value);
        }
        let source_arena_len = normalized.len();
        let canonical =
            WasmCanonicalJson::from_batch_parts(values, normalized, offsets, ROW_COUNT, ROW_COUNT)
                .expect("canonical fixture");
        let origin_key: SharedStr = "skewed-arena".into();
        let mut batch = PreparedStateBatch::from_test_rows(
            canonical
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    prepared_fixture_row(
                        &format!("row-{index}"),
                        Some(
                            stage_json_from_value(
                                TransactionJson::from_canonical_batch(value),
                                "skewed canonical fixture",
                            )
                            .expect("fixture should stage"),
                        ),
                        TransactionWriteOperation::Update,
                        &origin_key,
                    )
                })
                .collect(),
        );
        let selected = (0..LIVE_COUNT).collect::<Vec<_>>();
        batch.select_rows(&selected);
        assert_eq!(batch.len(), LIVE_COUNT);
        assert_eq!(
            batch.json.len(),
            ROW_COUNT,
            "the lazy owner threshold intentionally defers a near-half compaction"
        );

        batch.release_validated_canonical_value_columns();
        assert_eq!(batch.json.len(), LIVE_COUNT);
        let retained = batch
            .iter()
            .map(|row| row.snapshot.expect("live snapshot").materialize_shared())
            .collect::<Vec<_>>();
        let live_bytes = retained.iter().map(|value| value.len()).sum::<usize>();
        assert!(source_arena_len > live_bytes.saturating_mul(2));
        assert_eq!(retained[0].retained_buffer_len(), live_bytes);
        assert!(
            retained[1..]
                .iter()
                .all(|value| retained[0].shares_buffer_with(value)),
            "all live JSON slices must report one common compact arena owner"
        );
    }

    #[test]
    fn decoded_sql_rows_canonicalize_into_one_exact_batch_arena() {
        let mut rows = vec![
            Some(TransactionJson::from_value_for_test(
                serde_json::json!({"id": "a", "value": "first"}),
            )),
            Some(TransactionJson::from_value_for_test(
                serde_json::json!({"id": "b", "value": "second"}),
            )),
        ];

        canonicalize_transaction_json_batch(rows.iter_mut(), "SQL fixture")
            .expect("canonicalize SQL batch");
        let first = rows[0]
            .as_ref()
            .and_then(TransactionJson::canonical_batch_row)
            .expect("first batch row");
        let second = rows[1]
            .as_ref()
            .and_then(TransactionJson::canonical_batch_row)
            .expect("second batch row");

        assert!(first.shares_batch_with(second));
        assert_eq!(first.batch_row_count(), 2);
        assert_eq!(
            first.batch_arena_len(),
            first.normalized().len() + second.normalized().len()
        );
        assert_eq!(first.validation_counts(), (2, 2));
    }

    #[test]
    fn shared_decoded_rows_retain_source_values_in_one_canonical_arena() {
        let first_source = Arc::new(serde_json::json!({"id": "a", "value": "first"}));
        let second_source = Arc::new(serde_json::json!({"id": "b", "value": "second"}));
        let mut rows = vec![
            Some(
                TransactionJson::from_shared_value(Arc::clone(&first_source), "shared SQL fixture")
                    .expect("shared first row"),
            ),
            Some(
                TransactionJson::from_shared_value(
                    Arc::clone(&second_source),
                    "shared SQL fixture",
                )
                .expect("shared second row"),
            ),
        ];

        canonicalize_transaction_json_batch(rows.iter_mut(), "shared SQL fixture")
            .expect("canonicalize shared SQL batch");
        let (first_value, first_normalized) = match &rows[0].as_ref().expect("first row").storage {
            TransactionJsonStorage::CanonicalShared { value, normalized } => (
                value.get().expect("first decoded owner"),
                normalized.clone(),
            ),
            _ => panic!("shared decoded row must retain its source owner"),
        };
        let (second_value, second_normalized) = match &rows[1].as_ref().expect("second row").storage
        {
            TransactionJsonStorage::CanonicalShared { value, normalized } => (
                value.get().expect("second decoded owner"),
                normalized.clone(),
            ),
            _ => panic!("shared decoded row must retain its source owner"),
        };

        assert!(Arc::ptr_eq(first_value, &first_source));
        assert!(Arc::ptr_eq(second_value, &second_source));
        assert!(first_normalized.shares_buffer_with(&second_normalized));
        assert_eq!(
            first_normalized.retained_buffer_len(),
            first_normalized.len() + second_normalized.len()
        );
    }

    #[test]
    fn cached_normalized_json_is_copied_into_the_batch_without_reserializing() {
        let mut rows = vec![
            Some(TransactionJson::from_value_for_test(
                serde_json::json!({"id": "cached", "value": "first"}),
            )),
            Some(TransactionJson::from_value_for_test(
                serde_json::json!({"id": "fresh", "value": "second"}),
            )),
        ];
        let cached = rows[0]
            .as_ref()
            .expect("cached row")
            .normalized()
            .to_string();

        canonicalize_transaction_json_batch(rows.iter_mut(), "cached SQL fixture")
            .expect("canonicalize cached SQL batch");
        let first = rows[0]
            .as_ref()
            .and_then(TransactionJson::canonical_batch_row)
            .expect("first batch row");
        let second = rows[1]
            .as_ref()
            .and_then(TransactionJson::canonical_batch_row)
            .expect("second batch row");
        assert_eq!(first.normalized(), cached);
        assert!(first.shares_batch_with(second));
        assert_eq!(
            first.validation_counts(),
            (2, 1),
            "only the uncached row should invoke canonical serialization"
        );
    }

    #[test]
    fn canonical_json_arena_rejects_uncached_bytes_before_exceeding_its_limit() {
        let mut arena =
            CanonicalJsonArena::with_limit(0, 8).expect("create bounded canonical JSON arena");
        let error = serde_json::to_writer(
            &mut arena,
            &serde_json::json!("payload larger than the test wire limit"),
        )
        .expect_err("oversized uncached JSON must fail through the bounded writer");

        assert!(error.is_io());
        assert_eq!(arena.failure(), Some(CanonicalJsonArenaFailure::ExceedsU32));
        assert!(
            arena.len() <= 8,
            "the arena must reject a write before growing beyond its wire limit"
        );
    }

    #[test]
    fn verified_same_length_splice_requires_the_visible_blob_base() {
        let base = BlobId::from_content(b"before");
        let wrong_base = BlobId::from_content(b"other!");
        let mut write = TransactionFileContent::new(
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

    #[test]
    fn verified_edit_splice_is_format_neutral_and_cleared_by_rematerialization() {
        let base = BlobId::from_content(b"before");
        let wrong_base = BlobId::from_content(b"other!");
        let mut write = TransactionFileContent::new(
            "file".to_string(),
            None,
            None,
            "main".to_string(),
            false,
            false,
            b"after-more".to_vec(),
        )
        .with_base_blob_hash(Some(base));

        write.set_verified_blob_edit_splice(wrong_base, 2, 1, 5);
        assert_eq!(write.edit_blob_splice(), None);

        write.set_verified_blob_edit_splice(base, 2, 1, 5);
        assert_eq!(
            write.edit_blob_splice(),
            Some(BlobEditSplice {
                base_blob_hash: base,
                offset: 2,
                delete_len: 1,
                insert_len: 5,
            })
        );

        write.replace_data(b"plugin-rendered".to_vec());
        assert_eq!(
            write.edit_blob_splice(),
            None,
            "a plugin-rendered replacement invalidates the submitted-byte proof"
        );
    }
}
