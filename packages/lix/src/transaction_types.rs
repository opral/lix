use std::{
    collections::HashMap,
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
use crate::functions::FunctionProviderHandle;
use crate::hot_state::{CertifiedCurrentStatePredecessor, MaterializedHotStateRow};
use crate::json_store::JsonRef;
use crate::plugin::runtime::{WasmCertifiedRowBatch, WasmTypedRow};
use crate::row_pk::RowPk;
use crate::tracked_state::OrderedAddressableCommitDeltaStage;
use bytes::Bytes;
use lix_schema::Jsonb;
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
    CertifiedShared {
        normalized: SharedStr,
        certificate: TransactionJsonCertificate,
    },
    CanonicalShared {
        value: OnceLock<Arc<JsonValue>>,
        normalized: SharedStr,
    },
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
        matches!(
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
            storage => Self { storage },
        }
    }

    pub(crate) fn requires_batch_canonicalization(&self) -> bool {
        matches!(self.storage, TransactionJsonStorage::Decoded { .. })
    }

    pub(crate) fn value(&self) -> &JsonValue {
        match &self.storage {
            TransactionJsonStorage::Decoded { value, .. } => value.as_ref(),
            TransactionJsonStorage::CanonicalShared { value, normalized } => value
                .get_or_init(|| {
                    Arc::new(
                        serde_json::from_str(normalized.as_str())
                            .expect("shared canonical transaction JSON must remain valid JSON"),
                    )
                })
                .as_ref(),
            TransactionJsonStorage::CertifiedShared { .. } => {
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
            TransactionJsonStorage::CertifiedShared { normalized, .. } => normalized.as_str(),
            TransactionJsonStorage::CanonicalShared { normalized, .. } => normalized.as_str(),
        }
    }

    /// Materializes a mutable value only for a semantic rewrite such as
    /// applying a missing schema default.
    pub(crate) fn into_value_for_mutation(self) -> JsonValue {
        match self.storage {
            TransactionJsonStorage::Decoded { value, .. } => {
                Arc::try_unwrap(value).unwrap_or_else(|value| value.as_ref().clone())
            }
            TransactionJsonStorage::CanonicalShared { value, normalized } => {
                let value = value.into_inner().unwrap_or_else(|| {
                    Arc::new(
                        serde_json::from_str(normalized.as_str())
                            .expect("shared canonical transaction JSON must remain valid JSON"),
                    )
                });
                Arc::try_unwrap(value).unwrap_or_else(|value| value.as_ref().clone())
            }
            TransactionJsonStorage::CertifiedShared { .. } => {
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
const RAW_WRITE_PLUGIN_OWNED: u8 = 1 << 3;

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
    snapshots: Vec<Option<RawSnapshot>>,
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

/// The sole pre-schema-bound snapshot owner.
///
/// JSON ingress is replaced in place with its decoded Schema v1 row during
/// normalization, so a raw row cannot carry competing snapshot forms.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RawSnapshot {
    Json(TransactionJson),
    Row(Arc<WasmTypedRow>),
}

impl Eq for RawSnapshot {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CertifiedRawWriteBatchPreparation {
    pub(crate) schema_plan_id: SchemaPlanId,
    pub(crate) facts: PreparedRowFacts,
    pub(crate) tracked_keys_strictly_ordered: bool,
    pub(crate) complete_collection_replacement: Option<CompleteCollectionReplacementProof>,
    /// The SQL lowerer produced one homogeneous native row collection with no
    /// file scope or lifecycle/control rows. Plugin reconciliation can only
    /// classify ownership for this shape; it cannot change its semantics.
    pub(crate) fileless_typed_sql_rows: bool,
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
        let mut previous_end = 0usize;
        for &(start, end) in &snapshot_offsets {
            if start != previous_end || end <= start || end > snapshot_arena.len() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "typed mutation journal payload offsets are invalid",
                ));
            }
            previous_end = end;
        }
        if previous_end != snapshot_arena.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "typed mutation journal payload offsets do not cover the arena",
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
    certified_snapshots: Option<CertifiedSnapshots>,
    durable_predecessors: Vec<Option<CertifiedCurrentStatePredecessor>>,
    schema_key: SharedStr,
    branch_id: SharedStr,
    untracked: bool,
    certificate: CertifiedRawWriteBatchPreparation,
    row_columnar: Option<crate::sql2::EncodedRowGroups>,
}

struct CertifiedSnapshots {
    arena: Bytes,
    offsets: Vec<(u32, u32)>,
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
            certified_snapshots: None,
            schema_key,
            branch_id,
            untracked,
            certificate,
            row_columnar: None,
        })
    }

    pub(crate) fn new_typed(
        row_pks: Vec<RowPk>,
        snapshot_arena: Vec<u8>,
        snapshot_offsets: Vec<(usize, usize)>,
        schema_key: SharedStr,
        branch_id: SharedStr,
        mut certificate: CertifiedRawWriteBatchPreparation,
    ) -> Result<Self, LixError> {
        if row_pks.is_empty() || row_pks.len() != snapshot_offsets.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified typed replacement columns are not aligned",
            ));
        }
        if row_pks.len() >= RAW_WRITE_NONE as usize {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "certified typed replacement row count exceeds u32",
            ));
        }
        let arena_len = snapshot_arena.len();
        let mut previous_end = 0usize;
        let mut offsets = Vec::with_capacity(snapshot_offsets.len());
        for (start, end) in snapshot_offsets {
            if start != previous_end || end <= start || end > arena_len {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "certified typed replacement offsets are invalid",
                ));
            }
            offsets.push((
                u32::try_from(start).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "certified typed replacement arena exceeds u32",
                    )
                })?,
                u32::try_from(end).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "certified typed replacement arena exceeds u32",
                    )
                })?,
            ));
            previous_end = end;
        }
        if previous_end != arena_len {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified typed replacement offsets do not cover their arena",
            ));
        }
        certificate.fileless_typed_sql_rows = true;
        Ok(Self {
            durable_predecessors: std::iter::repeat_with(|| None)
                .take(row_pks.len())
                .collect(),
            row_pks,
            snapshots: Vec::new(),
            certified_snapshots: Some(CertifiedSnapshots {
                arena: Bytes::from(snapshot_arena),
                offsets,
            }),
            schema_key,
            branch_id,
            untracked: false,
            certificate,
            row_columnar: None,
        })
    }

    pub(crate) fn with_row_columnar(mut self, row_columnar: crate::sql2::EncodedRowGroups) -> Self {
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

    pub(crate) fn is_fileless_typed_sql_rows(&self) -> bool {
        self.certificate.fileless_typed_sql_rows
    }

    pub(crate) fn set_durable_predecessor(
        &mut self,
        index: usize,
        value: Option<CertifiedCurrentStatePredecessor>,
    ) {
        self.durable_predecessors[index] = value;
    }

    pub(crate) fn convert_to_typed(
        &mut self,
        schema_plan: &crate::catalog::SchemaPlan,
    ) -> Result<(), LixError> {
        if self.certified_snapshots.is_some() {
            return Ok(());
        }
        let snapshots = std::mem::take(&mut self.snapshots);
        let mut arena = Vec::new();
        let mut offsets = Vec::with_capacity(self.row_pks.len());
        for (row_pk, snapshot) in self.row_pks.iter().zip(snapshots) {
            let value = serde_json::from_str(snapshot.normalized()).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("certified typed row is not canonical JSON: {error}"),
                )
            })?;
            let typed = WasmTypedRow::from_normalized_json(schema_plan, row_pk, &value)?;
            let payload = typed.durable_payload().map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("cannot encode certified parameter typed payload: {error:?}"),
                )
            })?;
            let start = u32::try_from(arena.len()).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "certified typed payload arena exceeds u32",
                )
            })?;
            arena.extend_from_slice(payload.as_ref());
            let end = u32::try_from(arena.len()).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "certified typed payload arena exceeds u32",
                )
            })?;
            offsets.push((start, end));
        }
        self.certified_snapshots = Some(CertifiedSnapshots {
            arena: Bytes::from(arena),
            offsets,
        });
        self.certificate.fileless_typed_sql_rows = true;
        Ok(())
    }

    pub(crate) fn into_raw(self) -> Result<RawWriteBatch, LixError> {
        if let Some(certified_snapshots) = self.certified_snapshots {
            let mut rows = RawWriteBatch::with_capacity(self.row_pks.len());
            for (row_pk, (start, end)) in self.row_pks.into_iter().zip(certified_snapshots.offsets)
            {
                let decoded_snapshot = WasmTypedRow::decode_durable_payload(
                    Arc::from(&certified_snapshots.arena[start as usize..end as usize]),
                    self.schema_key.as_str(),
                    &row_pk,
                )
                .map(Arc::new)?;
                rows.push_typed_parts(
                    Some(row_pk),
                    self.schema_key.clone(),
                    None,
                    Some(decoded_snapshot),
                    None,
                    None,
                    None,
                    None,
                    false,
                    None,
                    None,
                    self.untracked,
                    self.branch_id.clone(),
                );
            }
            rows.certified_preparation = Some(self.certificate);
            for (index, predecessor) in self.durable_predecessors.into_iter().enumerate() {
                rows.set_durable_predecessor(index, predecessor);
            }
            return Ok(rows);
        }
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

    pub(crate) fn into_prepared(
        self,
        origin_key: Option<&SharedStr>,
        timestamp: LixTimestamp,
        functions: &FunctionProviderHandle,
    ) -> Result<PreparedStateBatch, LixError> {
        if self.untracked {
            return self.into_snapshot_prepared(origin_key, timestamp, functions);
        }
        self.into_dense_prepared(origin_key, timestamp)
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
            certified_snapshots,
            durable_predecessors,
            schema_key,
            branch_id,
            untracked,
            certificate,
            row_columnar,
        } = self;
        debug_assert!(snapshots.is_empty());
        let certified_snapshots = certified_snapshots.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified parameter rows are missing typed snapshots",
            )
        })?;
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
        let prepared_metadata = Vec::new();
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
            metadata: prepared_metadata,
            snapshot_arenas: Vec::new(),
            snapshot_refs: std::iter::repeat_with(|| None).take(row_count).collect(),
            durable_predecessors: Vec::new(),
            origins: Vec::new(),
            origin_index: HashMap::new(),
            origin_column_sets: Vec::new(),
            origin_column_index: HashMap::new(),
            certified_tracked_keys_strictly_ordered: certificate.tracked_keys_strictly_ordered,
            complete_collection_replacement: certificate.complete_collection_replacement,
            staged_index_values: StagedIndexValues::default(),
        };
        prepared.set_snapshot_arena(certified_snapshots.arena, certified_snapshots.offsets)?;
        if predecessor_count != 0 && predecessor_count != row_count {
            for (index, predecessor) in durable_predecessors.into_iter().enumerate() {
                prepared.set_durable_predecessor(index, predecessor);
            }
        }
        Ok(prepared)
    }

    fn into_snapshot_prepared(
        self,
        origin_key: Option<&SharedStr>,
        timestamp: LixTimestamp,
        functions: &FunctionProviderHandle,
    ) -> Result<PreparedStateBatch, LixError> {
        let Self {
            row_pks,
            snapshots,
            certified_snapshots,
            durable_predecessors,
            schema_key,
            branch_id,
            untracked,
            certificate,
            row_columnar: _,
        } = self;
        debug_assert!(snapshots.is_empty());
        let certified_snapshots = certified_snapshots.expect("snapshot conversion was checked");
        let mut prepared = PreparedStateBatch::with_capacity(row_pks.len());
        for row_pk in row_pks {
            let change_id = if untracked {
                ChangeId::from(functions.call_uuid_v7())
            } else {
                ChangeId::default()
            };
            prepared.push_parts_with_change_addressability(
                certificate.schema_plan_id,
                certificate.facts,
                row_pk,
                schema_key.clone(),
                None,
                None,
                None,
                None,
                origin_key,
                timestamp,
                timestamp,
                false,
                Some(change_id),
                true,
                None,
                untracked,
                branch_id.clone(),
            );
        }
        prepared.set_snapshot_arena(certified_snapshots.arena, certified_snapshots.offsets)?;
        for (index, predecessor) in durable_predecessors.into_iter().enumerate() {
            prepared.set_durable_predecessor(index, predecessor);
        }
        prepared.certified_tracked_keys_strictly_ordered =
            certificate.tracked_keys_strictly_ordered;
        prepared.complete_collection_replacement = certificate.complete_collection_replacement;
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
    pub(crate) snapshot: Option<&'a RawSnapshot>,
    pub(crate) metadata: Option<&'a TransactionJson>,
    pub(crate) origin: Option<&'a TransactionWriteOrigin>,
    pub(crate) created_at: Option<&'a str>,
    pub(crate) updated_at: Option<&'a str>,
    pub(crate) global: bool,
    pub(crate) change_id: Option<&'a str>,
    pub(crate) commit_id: Option<&'a str>,
    pub(crate) untracked: bool,
    pub(crate) constraints_unchanged: bool,
    pub(crate) plugin_owned: bool,
    pub(crate) branch_id: &'a SharedStr,
}

impl<'a> RawWriteRowRef<'a> {
    pub(crate) fn snapshot_json(&self) -> Option<&'a TransactionJson> {
        match self.snapshot? {
            RawSnapshot::Json(snapshot) => Some(snapshot),
            RawSnapshot::Row(_) => None,
        }
    }

    pub(crate) fn decoded_snapshot(&self) -> Option<&'a Arc<WasmTypedRow>> {
        match self.snapshot? {
            RawSnapshot::Json(_) => None,
            RawSnapshot::Row(snapshot) => Some(snapshot),
        }
    }

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
    snapshot: Option<&[u8]>,
    metadata: Option<&Jsonb>,
    origin_key: Option<&SharedStr>,
    branch_id: &SharedStr,
    stage: usize,
) {
    let key_bytes = row_pk.estimated_heap_bytes()
        + schema_key.len()
        + file_id.map_or(0, |value| value.len())
        + origin_key.map_or(0, |value| value.len())
        + branch_id.len();
    let value_bytes = snapshot.map_or(0, <[u8]>::len)
        + metadata.map_or(0, |value| value.binary_len().unwrap_or(0));
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
            snapshots: snapshots
                .into_iter()
                .map(|snapshot| Some(RawSnapshot::Json(snapshot)))
                .collect(),
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

    #[inline(always)]
    pub(crate) fn row(&self, index: usize) -> RawWriteRowRef<'_> {
        self.get(index)
            .expect("raw write row index must be inside the batch")
    }

    #[inline(always)]
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
            plugin_owned: slot.flags & RAW_WRITE_PLUGIN_OWNED != 0,
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

    /// Marks a row whose schema is owned by the active plugin registry.
    /// Normalization uses this proof to reject removed JSON ingress and require
    /// the native typed payload lane used by component-authored rows.
    pub(crate) fn mark_plugin_owned(&mut self, index: usize) {
        self.certified_preparation = None;
        self.slots[index].flags |= RAW_WRITE_PLUGIN_OWNED;
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
        self.snapshots.push(snapshot.map(RawSnapshot::Json));
        self.metadata.push(metadata);
        self.durable_predecessors.push(None);
        self.debug_assert_aligned();
    }

    /// Appends a plugin row whose Schema v1 values have already been
    /// validated at the component boundary. The typed owner remains aligned
    /// with the ordinary raw-row columns and is never lowered to an outer JSON
    /// object.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn push_typed_parts(
        &mut self,
        row_pk: Option<RowPk>,
        schema_key: SharedStr,
        file_id: Option<SharedStr>,
        decoded_snapshot: Option<Arc<WasmTypedRow>>,
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
        self.push_parts(
            row_pk, schema_key, file_id, None, metadata, origin, created_at, updated_at, global,
            change_id, commit_id, untracked, branch_id,
        );
        *self
            .snapshots
            .last_mut()
            .expect("typed row was just appended") = decoded_snapshot.map(RawSnapshot::Row);
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

    /// Replaces transient canonical JSON row sources with their sole durable
    /// Schema v1 representation before certified preparation. This keeps the
    /// fast SQL batch builders allocation-friendly without allowing their
    /// temporary JSON arena to become a second physical payload lane.
    pub(crate) fn convert_json_snapshots_to_typed(
        &mut self,
        schema_plan: &crate::catalog::SchemaPlan,
    ) -> Result<(), LixError> {
        for index in 0..self.len() {
            let Some(RawSnapshot::Json(snapshot)) = self.snapshots[index].as_ref() else {
                continue;
            };
            let value = serde_json::from_str(snapshot.normalized()).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("certified typed row is not canonical JSON: {error}"),
                )
            })?;
            let row_pk = self.row_pks[index].as_ref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "certified typed row is missing its primary key",
                )
            })?;
            let typed = WasmTypedRow::from_normalized_json(schema_plan, row_pk, &value)?;
            self.snapshots[index] = Some(RawSnapshot::Row(Arc::new(typed)));
        }
        Ok(())
    }

    pub(crate) fn certify_fileless_typed_sql_rows(
        &mut self,
        schema_plan_id: SchemaPlanId,
        facts: PreparedRowFacts,
    ) -> Result<(), LixError> {
        let Some(first) = self.get(0) else {
            return Ok(());
        };
        let schema_key = first.schema_key;
        let branch_id = first.branch_id;
        let untracked = first.untracked;
        let mut previous_row_pk = None;
        let mut strictly_ordered = true;
        for row in self.iter() {
            if row.schema_key != schema_key
                || row.branch_id != branch_id
                || row.untracked != untracked
                || row.global
                || row.file_id.is_some()
                || row.snapshot_json().is_some()
                || row.metadata.is_some()
                || (row.decoded_snapshot().is_none() && row.row_pk.is_none())
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "fileless typed SQL certificate received an ineligible row",
                ));
            }
            if let (Some(previous), Some(current)) = (previous_row_pk, row.row_pk) {
                strictly_ordered &= previous < current;
            }
            previous_row_pk = row.row_pk;
        }
        self.certified_preparation = Some(CertifiedRawWriteBatchPreparation {
            schema_plan_id,
            facts,
            tracked_keys_strictly_ordered: strictly_ordered,
            complete_collection_replacement: None,
            fileless_typed_sql_rows: true,
        });
        Ok(())
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
        let prepared_metadata = Vec::new();
        let mut snapshot_arenas = Vec::with_capacity(row_count);
        let mut snapshot_refs = Vec::with_capacity(row_count);
        let mut prepared_durable_predecessors = Vec::new();
        for (row_index, ((((slot, row_pk), snapshot), metadata), predecessor)) in slots
            .into_iter()
            .zip(row_pks)
            .zip(snapshots)
            .zip(metadata)
            .zip(durable_predecessors)
            .enumerate()
        {
            if slot.file_id != RAW_WRITE_NONE
                || slot.origin != RAW_WRITE_NONE
                || slot.created_at != RAW_WRITE_NONE
                || slot.updated_at != RAW_WRITE_NONE
                || slot.change_id != RAW_WRITE_NONE
                || slot.commit_id != RAW_WRITE_NONE
                || slot.flags & !(RAW_WRITE_UNTRACKED | RAW_WRITE_CONSTRAINTS_UNCHANGED) != 0
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
            match snapshot {
                Some(RawSnapshot::Row(decoded_snapshot)) => {
                    let payload = decoded_snapshot.durable_payload().map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("cannot encode certified prepared snapshot: {error:?}"),
                        )
                    })?;
                    if payload.is_empty() {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "certified transaction row has an empty native typed snapshot",
                        ));
                    }
                    let arena = u32::try_from(snapshot_arenas.len())
                        .expect("prepared snapshot arena count must fit u32");
                    let end = u32::try_from(payload.len())
                        .expect("prepared snapshot payload must fit u32");
                    snapshot_arenas.push(Bytes::from_owner(payload));
                    snapshot_refs.push(Some(PreparedSnapshotRef {
                        arena,
                        start: 0,
                        end,
                    }));
                }
                Some(RawSnapshot::Json(_)) => {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "certified preparation requires a normalized snapshot row",
                    ));
                }
                // Protocol v69 encodes deletes as the absence of a snapshot.
                None => snapshot_refs.push(None),
            }
            prepared_row_pks.push(row_pk);
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
            let durable_predecessor = predecessor.map(|predecessor| {
                let ordinal = u32::try_from(prepared_durable_predecessors.len())
                    .expect("certified predecessor column must fit u32");
                prepared_durable_predecessors.push(predecessor);
                ordinal
            });
            prepared_slots.push(PreparedStateSlot {
                schema_plan_id: certificate.schema_plan_id,
                facts: certificate.facts,
                row_pk: u32::try_from(row_index)
                    .expect("certified transaction row ordinal must fit u32"),
                schema_key: slot.schema_key,
                file_id: None,
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
                durable_predecessor,
            });
        }
        Ok(PreparedStateBatch {
            slots: prepared_slots,
            dense_certified_parameter: None,
            row_pks: prepared_row_pks,
            strings,
            string_index,
            metadata: prepared_metadata,
            snapshot_arenas,
            snapshot_refs,
            durable_predecessors: prepared_durable_predecessors,
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
                *snapshot = Some(match value {
                    RawSnapshot::Json(value) => {
                        RawSnapshot::Json(value.revoke_row_content_certificate())
                    }
                    RawSnapshot::Row(value) => RawSnapshot::Row(value),
                });
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
        match self.snapshots[index].take() {
            Some(RawSnapshot::Json(snapshot)) => Some(snapshot),
            value => {
                self.snapshots[index] = value;
                None
            }
        }
    }

    pub(crate) fn take_decoded_snapshot(&mut self, index: usize) -> Option<Arc<WasmTypedRow>> {
        match self.snapshots[index].take() {
            Some(RawSnapshot::Row(snapshot)) => Some(snapshot),
            value => {
                self.snapshots[index] = value;
                None
            }
        }
    }

    pub(crate) fn set_snapshot(&mut self, index: usize, value: Option<TransactionJson>) {
        self.certified_preparation = None;
        self.snapshots[index] = value.map(RawSnapshot::Json);
    }

    pub(crate) fn set_decoded_snapshot(&mut self, index: usize, value: Option<Arc<WasmTypedRow>>) {
        self.certified_preparation = None;
        self.snapshots[index] = value.map(RawSnapshot::Row);
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
        let snapshot = self.snapshots[index].take().map(|snapshot| match snapshot {
            RawSnapshot::Json(snapshot) => snapshot,
            RawSnapshot::Row(snapshot) => TransactionJson::from_value_for_test(
                snapshot
                    .to_json_value()
                    .expect("typed test row should project to JSON"),
            ),
        });
        TransactionWriteRow {
            row_pk: self.row_pks[index].take(),
            schema_key: self.strings[slot.schema_key as usize].clone(),
            file_id: self.optional_string(slot.file_id).cloned(),
            snapshot,
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
        let row_pk = self.row_pks[index].take();
        let schema_key = self.strings[slot.schema_key as usize].clone();
        let file_id = self.optional_string(slot.file_id).cloned();
        let snapshot = self.snapshots[index].take();
        let metadata = self.metadata[index].take();
        let origin =
            (slot.origin != RAW_WRITE_NONE).then(|| self.origins[slot.origin as usize].clone());
        let created_at = self.optional_string(slot.created_at).cloned();
        let updated_at = self.optional_string(slot.updated_at).cloned();
        let global = slot.flags & RAW_WRITE_GLOBAL != 0;
        let change_id = self.optional_string(slot.change_id).cloned();
        let commit_id = self.optional_string(slot.commit_id).cloned();
        let untracked = slot.flags & RAW_WRITE_UNTRACKED != 0;
        let branch_id = self.strings[slot.branch_id as usize].clone();
        match snapshot {
            Some(RawSnapshot::Row(snapshot)) => destination.push_typed_parts(
                row_pk,
                schema_key,
                file_id,
                Some(snapshot),
                metadata,
                origin,
                created_at,
                updated_at,
                global,
                change_id,
                commit_id,
                untracked,
                branch_id,
            ),
            snapshot => destination.push_parts(
                row_pk,
                schema_key,
                file_id,
                snapshot.map(|snapshot| match snapshot {
                    RawSnapshot::Json(snapshot) => snapshot,
                    RawSnapshot::Row(_) => unreachable!("decoded snapshot matched above"),
                }),
                metadata,
                origin,
                created_at,
                updated_at,
                global,
                change_id,
                commit_id,
                untracked,
                branch_id,
            ),
        }
        let destination_index = destination.len() - 1;
        destination.set_durable_predecessor(destination_index, durable_predecessor);
        destination.slots[destination_index].flags |=
            slot.flags & (RAW_WRITE_CONSTRAINTS_UNCHANGED | RAW_WRITE_PLUGIN_OWNED);
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

    #[inline(always)]
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
    stage_payload_at_commit: bool,
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

    pub(crate) fn plugin_checkpoint(&self) -> Option<&PluginCheckpointWrite> {
        self.plugin_checkpoint.as_ref()
    }

    pub(crate) fn certified_row_batches(&self) -> &[WasmCertifiedRowBatch] {
        &self.certified_row_batches
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
    ValidatedShared { normalized: SharedStr },
    /// Row-owned certified bytes retained after transaction validation.
    ///
    /// Direct row writes already arrive in an `Arc<str>`. Keeping that
    /// owner avoids allocating and copying the full JSON payload merely to
    /// discard an empty (or no-longer-needed) decoded-value cache.
    ValidatedOwned { normalized: Arc<str> },
}

impl StageJson {
    /// Crosses the preparation boundary into the protocol's native metadata
    /// representation. Prepared rows never retain a JSON text slot.
    pub(crate) fn into_jsonb(self) -> Jsonb {
        let parse = |normalized: &str| {
            serde_json::from_str(normalized)
                .expect("prepared normalized metadata must remain valid JSON")
        };
        let value = match self.storage {
            StageJsonStorage::Owned { value, normalized } => value
                .into_inner()
                .map(|value| Arc::try_unwrap(value).unwrap_or_else(|value| (*value).clone()))
                .unwrap_or_else(|| parse(&normalized)),
            StageJsonStorage::CertifiedShared { value, normalized } => value
                .into_inner()
                .map(|value| Arc::try_unwrap(value).unwrap_or_else(|value| (*value).clone()))
                .unwrap_or_else(|| parse(normalized.as_str())),
            StageJsonStorage::ValidatedShared { normalized } => parse(normalized.as_str()),
            StageJsonStorage::ValidatedOwned { normalized } => parse(&normalized),
        };
        Jsonb::from_value(value)
    }
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
        }
    }

    pub(crate) fn normalized(&self) -> &str {
        match &self.storage {
            StageJsonStorage::Owned { normalized, .. } => normalized.as_ref(),
            StageJsonStorage::CertifiedShared { normalized, .. } => normalized.as_str(),
            StageJsonStorage::ValidatedShared { normalized } => normalized.as_str(),
            StageJsonStorage::ValidatedOwned { normalized } => normalized.as_ref(),
        }
    }

    /// Drops the parsed values column after all semantic validation succeeds.
    ///
    /// Every row retains only a cheap slice of the canonical batch arena. The
    /// final row releases the shared DOM and offset columns before commit
    /// materialization allocates its storage buffers.
    pub(crate) fn release_validated_canonical_value_column(&mut self) -> bool {
        let storage = match &self.storage {
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
            StageJsonStorage::ValidatedShared { .. } | StageJsonStorage::ValidatedOwned { .. } => {
                false
            }
        }
    }

    /// Whether this payload inlines into values instead of the json store.
    pub(crate) fn is_inline(&self) -> bool {
        self.normalized().len() <= crate::json_store::JSON_INLINE_MAX_BYTES
    }
}

impl PartialEq for StageJson {
    fn eq(&self, other: &Self) -> bool {
        self.normalized() == other.normalized()
            && (self.is_inline() || other.is_inline() || self.json_ref == other.json_ref)
    }
}

impl Eq for StageJson {}

pub(crate) fn stage_json_from_value(value: TransactionJson) -> StageJson {
    // Inline values carry their bytes as the authoritative durable payload.
    // Computing and retaining a content hash for every small row only to
    // discard it at the inline-storage boundary doubled the canonical-byte walk on
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
    };
    StageJson { storage, json_ref }
}

/// Coalesces decoded engine JSON values into one canonical UTF-8 arena.
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
                    Err(value) => DecodedCanonicalValue::Shared(value),
                };
                positions.push(position);
                values.push(value);
                cached_normalized.push(normalized.into_inner());
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
        }
        let end = u32::try_from(normalized.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_UNKNOWN,
                format!("{context} canonical JSON batch exceeds u32"),
            )
        })?;
        offsets.push((start, end));
    }
    // SAFETY: every uncached row was written by serde_json and every cached
    // row came from a previously validated TransactionJson.
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
    pub(crate) snapshot: Option<Bytes>,
    pub(crate) metadata: Option<Jsonb>,
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
            snapshot: self.snapshot.as_deref(),
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
    metadata: Vec<Jsonb>,
    /// Canonical typed snapshot owners. Row-aligned refs borrow ranges from
    /// these arenas; `None` is the sole tombstone representation.
    snapshot_arenas: Vec<Bytes>,
    snapshot_refs: Vec<Option<PreparedSnapshotRef>>,
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
struct PreparedSnapshotRef {
    arena: u32,
    start: u32,
    end: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedStateSlot {
    schema_plan_id: SchemaPlanId,
    facts: PreparedRowFacts,
    row_pk: u32,
    schema_key: u32,
    file_id: Option<u32>,
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
    pub(crate) snapshot: Option<&'a [u8]>,
    pub(crate) metadata: Option<&'a Jsonb>,
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

impl PreparedStateRowRef<'_> {
    #[inline]
    pub(crate) fn is_deleted(self) -> bool {
        self.snapshot.is_none()
    }

    #[inline]
    pub(crate) fn has_payload(self) -> bool {
        !self.is_deleted()
    }

    pub(crate) fn materialize_decoded_snapshot(
        self,
    ) -> Result<Option<Arc<WasmTypedRow>>, LixError> {
        self.snapshot
            .map(|payload| {
                WasmTypedRow::decode_durable_payload(
                    Arc::from(payload),
                    self.schema_key.as_str(),
                    self.row_pk,
                )
                .map(Arc::new)
            })
            .transpose()
    }
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
            metadata: Vec::with_capacity(json_capacity),
            snapshot_arenas: Vec::new(),
            snapshot_refs: Vec::with_capacity(row_capacity),
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
                .saturating_add(row.snapshot.map_or(0, <[u8]>::len))
                .saturating_add(
                    row.metadata
                        .map_or(0, |value| value.binary_len().unwrap_or(0)),
                );
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

    /// Rebinds addressable rows in a fully certified fresh-file import to the
    /// owning commit's direct mutation address space.
    ///
    /// The fresh-file certificate has already proved that this is one live,
    /// tracked, exact-file batch with no selected history. Preparation marks
    /// engine-generated placeholder identities as addressable, so they can be
    /// replaced here without touching authored IDs used by durable row
    /// references, materialization roots, or actor observations.
    pub(crate) fn certify_fresh_file_direct_addresses(
        &mut self,
        expected_file_id: &str,
    ) -> Result<(), LixError> {
        self.expand_dense_certified_parameter();
        if self.slots.iter().enumerate().any(|(index, slot)| {
            slot.untracked
                || slot
                    .file_id
                    .map(|index| self.strings[index as usize].as_str())
                    != Some(expected_file_id)
                || self.snapshot_refs[index].is_none()
        }) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "fresh-file direct-address certificate no longer matches prepared rows",
            ));
        }
        for slot in &mut self.slots {
            if slot.addressable_change_id {
                slot.change_id = Some(ChangeId::default());
            }
        }
        Ok(())
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
            let payload_bytes = row.snapshot.map(<[u8]>::len)?;
            let row_bytes = row
                .schema_key
                .len()
                .checked_add(row.row_pk.estimated_heap_bytes())?
                .checked_add(128)?
                .checked_add(payload_bytes)?;
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

    #[inline(always)]
    pub(crate) fn row(&self, index: usize) -> PreparedStateRowRef<'_> {
        self.get(index)
            .expect("prepared state batch row ordinal is in bounds")
    }

    #[inline(always)]
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
                snapshot: self.snapshot_at(index),
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
            snapshot: self.snapshot_at(index),
            metadata: slot.metadata.map(|index| &self.metadata[index as usize]),
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
        self.push_parts_with_change_addressability_native(
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
            false,
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
    pub(crate) fn hard_cut_test_payloads_to_typed(&mut self) -> Result<(), LixError> {
        debug_assert!(
            self.iter()
                .all(|row| row.snapshot.is_some() || row.is_deleted())
        );
        Ok(())
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) fn push_parts_with_change_addressability(
        &mut self,
        schema_plan_id: SchemaPlanId,
        facts: PreparedRowFacts,
        row_pk: RowPk,
        schema_key: SharedStr,
        file_id: Option<SharedStr>,
        snapshot: Option<Bytes>,
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
        self.push_parts_with_change_addressability_native(
            schema_plan_id,
            facts,
            row_pk,
            schema_key,
            file_id,
            snapshot,
            metadata.map(StageJson::into_jsonb),
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
        );
    }

    #[expect(clippy::too_many_arguments)]
    fn push_parts_with_change_addressability_native(
        &mut self,
        schema_plan_id: SchemaPlanId,
        facts: PreparedRowFacts,
        row_pk: RowPk,
        schema_key: SharedStr,
        file_id: Option<SharedStr>,
        snapshot: Option<Bytes>,
        metadata: Option<Jsonb>,
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
            snapshot.as_deref(),
            metadata.as_ref(),
            origin_key,
            &branch_id,
            crate::storage_bench::CRUD_OWNERSHIP_PREPARED_BATCH,
        );
        self.expand_dense_certified_parameter();
        let row_pk = self.push_row_pk(row_pk);
        let schema_key = self.intern_string(schema_key);
        let file_id = file_id.map(|value| self.intern_string(value));
        let metadata = metadata.map(|value| self.push_metadata(value));
        let origin = origin.map(|value| self.intern_origin(value));
        let origin_key = origin_key.map(|value| self.intern_string_ref(value));
        let branch_id = self.intern_string(branch_id);
        self.slots.push(PreparedStateSlot {
            schema_plan_id,
            facts,
            row_pk,
            schema_key,
            file_id,
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
        self.snapshot_refs.push(None);
        if let Some(snapshot) = snapshot {
            let index = self.slots.len() - 1;
            self.set_snapshot_bytes(index, snapshot);
        }
    }

    fn expand_dense_certified_parameter(&mut self) {
        let Some(dense) = self.dense_certified_parameter.take() else {
            return;
        };
        debug_assert!(self.slots.is_empty());
        debug_assert_eq!(self.row_pks.len(), dense.len);
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
                    && self.snapshot_arenas.is_empty()
                    && other.snapshot_arenas.is_empty()
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
        self.metadata.append(&mut other.metadata);
        let arena_base = u32::try_from(self.snapshot_arenas.len())
            .expect("prepared snapshot arena count must fit u32");
        self.snapshot_arenas.append(&mut other.snapshot_arenas);
        self.snapshot_refs
            .extend(other.snapshot_refs.drain(..).map(|snapshot| {
                snapshot.map(|snapshot| PreparedSnapshotRef {
                    arena: snapshot
                        .arena
                        .checked_add(arena_base)
                        .expect("prepared snapshot arena ordinal overflowed"),
                    ..snapshot
                })
            }));
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
        let row_base = u32::try_from(self.row_pks.len()).expect("prepared row column must fit u32");
        let metadata_base =
            u32::try_from(self.metadata.len()).expect("prepared metadata column must fit u32");
        let predecessor_base = u32::try_from(self.durable_predecessors.len())
            .expect("prepared predecessor column must fit u32");
        self.slots.reserve(other.slots.len());
        self.row_pks.reserve(other.row_pks.len());
        self.metadata.reserve(other.metadata.len());
        self.snapshot_refs.reserve(other.snapshot_refs.len());
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
        self.metadata.append(&mut other.metadata);
        let payload_arena_base = u32::try_from(self.snapshot_arenas.len())
            .expect("prepared snapshot arena count must fit u32");
        self.snapshot_arenas.append(&mut other.snapshot_arenas);
        self.snapshot_refs
            .extend(other.snapshot_refs.into_iter().map(|snapshot| {
                snapshot.map(|snapshot| PreparedSnapshotRef {
                    arena: snapshot
                        .arena
                        .checked_add(payload_arena_base)
                        .expect("prepared snapshot arena ordinal overflowed"),
                    ..snapshot
                })
            }));
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
                metadata: slot.metadata.map(|index| {
                    index
                        .checked_add(metadata_base)
                        .expect("prepared metadata ordinal overflowed")
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
        self.snapshot_refs.swap(left, right);
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
            self.snapshot_refs.swap(destination, source_position);
            original_at_position.swap(destination, source_position);
            position_of_original[source] = destination;
            position_of_original[displaced_original] = source_position;
        }
        self.slots.truncate(source_by_destination.len());
        self.snapshot_refs.truncate(source_by_destination.len());
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
        self.snapshot_refs.truncate(retained_len);
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
        // Metadata crossed into native JSONB during preparation; there is no
        // decoded/text column left to release after semantic validation.
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
    pub(crate) fn set_origin_for_test(
        &mut self,
        index: usize,
        origin: Option<TransactionWriteOrigin>,
    ) {
        self.expand_dense_certified_parameter();
        let origin = origin.map(|origin| self.intern_origin(origin));
        self.slots[index].origin = origin;
    }

    pub(crate) fn set_requires_transaction_validation(
        &mut self,
        index: usize,
        requires_transaction_validation: bool,
    ) {
        self.expand_dense_certified_parameter();
        self.slots[index].facts.requires_transaction_validation = requires_transaction_validation;
    }

    pub(crate) fn set_decoded_snapshot(&mut self, index: usize, value: Option<Arc<WasmTypedRow>>) {
        self.expand_dense_certified_parameter();
        self.snapshot_refs[index] = None;
        let Some(value) = value else {
            return;
        };
        let payload = value
            .durable_payload()
            .expect("validated prepared typed row must encode");
        self.set_snapshot_bytes(index, Bytes::from_owner(payload));
    }

    fn snapshot_at(&self, index: usize) -> Option<&[u8]> {
        let snapshot = self.snapshot_refs.get(index).copied().flatten()?;
        self.snapshot_arenas
            .get(snapshot.arena as usize)?
            .get(snapshot.start as usize..snapshot.end as usize)
    }

    fn set_snapshot_bytes(&mut self, index: usize, snapshot: Bytes) {
        let arena = u32::try_from(self.snapshot_arenas.len())
            .expect("prepared snapshot arena count must fit u32");
        let end = u32::try_from(snapshot.len()).expect("prepared snapshot payload must fit u32");
        self.snapshot_arenas.push(snapshot);
        self.snapshot_refs[index] = Some(PreparedSnapshotRef {
            arena,
            start: 0,
            end,
        });
    }

    /// Attaches one certified payload arena to the current row topology.
    /// Every range is validated before ownership is published, so subsequent
    /// row operations only need to keep the compact refs aligned.
    pub(crate) fn set_snapshot_arena(
        &mut self,
        arena: Bytes,
        offsets: Vec<(u32, u32)>,
    ) -> Result<(), LixError> {
        if offsets.len() != self.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified typed payload ranges are not row-aligned",
            ));
        }
        if offsets.iter().enumerate().any(|(index, &(start, end))| {
            start >= end || end as usize > arena.len() || self.snapshot_refs[index].is_some()
        }) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified typed payload range or row payload invariant is invalid",
            ));
        }
        let arena_index = u32::try_from(self.snapshot_arenas.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "prepared typed payload arena count exceeds u32",
            )
        })?;
        self.snapshot_arenas.push(arena);
        for (slot, (start, end)) in self.snapshot_refs.iter_mut().zip(offsets) {
            *slot = Some(PreparedSnapshotRef {
                arena: arena_index,
                start,
                end,
            });
        }
        Ok(())
    }

    fn push_row_pk(&mut self, value: RowPk) -> u32 {
        let index = u32::try_from(self.row_pks.len()).expect("prepared row column must fit u32");
        self.row_pks.push(value);
        index
    }

    fn push_metadata(&mut self, value: Jsonb) -> u32 {
        let index =
            u32::try_from(self.metadata.len()).expect("prepared metadata column must fit u32");
        self.metadata.push(value);
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
        self.push_parts_with_change_addressability_native(
            row.schema_plan_id,
            row.facts,
            row.row_pk.clone(),
            row.schema_key.clone(),
            row.file_id.cloned(),
            row.snapshot.map(Bytes::copy_from_slice),
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

    #[inline(always)]
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

pub(crate) fn materialize_jsonb_shared(value: &Jsonb) -> SharedStr {
    SharedStr::from(
        serde_json::to_string(value.as_value())
            .expect("validated prepared metadata must serialize"),
    )
}

impl From<PreparedStateRowRef<'_>> for MaterializedHotStateRow {
    fn from(row: PreparedStateRowRef<'_>) -> Self {
        Self {
            row_pk: row.row_pk.clone(),
            schema_key: row.schema_key.to_string(),
            file_id: row.file_id.map(ToString::to_string),
            snapshot_content: None,
            metadata: row.metadata.map(materialize_jsonb_shared),
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

/// Builds the legacy owned DTO at an explicit JSON projection boundary.
/// Durable and batch-native paths keep the typed sidecar instead; persistent
/// filesystem indexes currently own this DTO and therefore request the
/// transient projection deliberately.
pub(crate) fn materialized_hot_state_row_with_snapshot_projection(
    row: PreparedStateRowRef<'_>,
) -> Result<MaterializedHotStateRow, LixError> {
    let mut materialized = MaterializedHotStateRow::from(row);
    materialized.snapshot_content = row
        .snapshot
        .map(|payload| {
            WasmTypedRow::decode_durable_payload(
                Arc::from(payload),
                row.schema_key.as_str(),
                row.row_pk,
            )?
            .to_json_shared()
        })
        .transpose()?;
    Ok(materialized)
}

#[cfg(test)]
impl From<TestPreparedStateRow> for MaterializedHotStateRow {
    fn from(row: TestPreparedStateRow) -> Self {
        let deleted = row.snapshot.is_none();
        let snapshot_content = row.snapshot.as_deref().map(|snapshot| {
            WasmTypedRow::decode_durable_payload(
                Arc::from(snapshot),
                row.schema_key.as_str(),
                &row.row_pk,
            )
            .and_then(|typed| typed.to_json_shared())
            .expect("test prepared snapshot should decode")
        });
        Self {
            row_pk: row.row_pk,
            schema_key: row.schema_key.into(),
            file_id: row.file_id.map(Into::into),
            snapshot_content,
            metadata: row
                .metadata
                .map(|metadata| materialize_jsonb_shared(&metadata)),
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
            snapshot_content: row.snapshot.as_deref().map(|snapshot| {
                WasmTypedRow::decode_durable_payload(
                    Arc::from(snapshot),
                    row.schema_key.as_str(),
                    &row.row_pk,
                )
                .and_then(|typed| typed.to_json_shared())
                .expect("test prepared snapshot should decode")
            }),
            metadata: row.metadata.as_ref().map(materialize_jsonb_shared),
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

    #[test]
    fn fileless_typed_sql_certificate_rejects_metadata_rows() {
        let mut rows = RawWriteBatch::new();
        rows.push_typed_parts(
            Some(RowPk::single("row")),
            "typed_sql_probe".into(),
            None,
            None,
            Some(TransactionJson::from_value_for_test(serde_json::json!({
                "source": "test"
            }))),
            None,
            None,
            None,
            false,
            None,
            None,
            false,
            "main".into(),
        );

        let error = rows
            .certify_fileless_typed_sql_rows(
                SchemaPlanId::for_test(1),
                PreparedRowFacts {
                    row_content_validated: true,
                    requires_transaction_validation: false,
                },
            )
            .expect_err("metadata rows must remain on generic preparation");
        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(rows.certified_preparation().is_none());
    }

    #[test]
    fn fresh_file_direct_addressing_preserves_authored_authorities() {
        let timestamp = LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:00.000Z");
        let file_id = SharedStr::from_static("01920000-0000-7000-8000-0000000000a2");
        let authored = ChangeId::for_test_label("materialization-root");
        let mut rows = PreparedStateBatch::with_capacity(2);
        for (key, change_id, addressable) in [
            (
                "ordinary",
                Some(ChangeId::for_test_label("placeholder")),
                true,
            ),
            ("authority", Some(authored), false),
        ] {
            let row_pk = RowPk::single(key);
            let snapshot =
                WasmTypedRow::from_test_json_unchecked(&row_pk, &serde_json::json!({"id": key}))
                    .expect("test snapshot should become typed")
                    .durable_payload()
                    .map(Bytes::from_owner)
                    .expect("typed test snapshot should encode");
            rows.push_parts_with_change_addressability(
                SchemaPlanId::for_test(1),
                PreparedRowFacts::default(),
                row_pk,
                "plugin_state".into(),
                Some(file_id.clone()),
                Some(snapshot),
                None,
                None,
                None,
                timestamp,
                timestamp,
                false,
                change_id,
                addressable,
                None,
                false,
                "main".into(),
            );
        }

        rows.certify_fresh_file_direct_addresses(file_id.as_str())
            .expect("fresh file should certify");

        assert_eq!(rows.row(0).change_id, Some(ChangeId::default()));
        assert!(rows.row(0).addressable_change_id);
        assert_eq!(rows.row(1).change_id, Some(authored));
        assert!(!rows.row(1).addressable_change_id);
    }

    fn expdl_probe_certificate(plan: u32) -> CertifiedRawWriteBatchPreparation {
        CertifiedRawWriteBatchPreparation {
            schema_plan_id: SchemaPlanId::for_test(plan),
            facts: PreparedRowFacts {
                row_content_validated: true,
                requires_transaction_validation: false,
            },
            tracked_keys_strictly_ordered: true,
            complete_collection_replacement: None,
            fileless_typed_sql_rows: false,
        }
    }

    fn typed_certified_probe_batch(
        keys: &[&str],
        schema_key: &str,
        untracked: bool,
        certificate: CertifiedRawWriteBatchPreparation,
        timestamp: LixTimestamp,
    ) -> PreparedStateBatch {
        let row_pks = keys
            .iter()
            .map(|key| RowPk::single(*key))
            .collect::<Vec<_>>();
        let mut arena = Vec::new();
        let mut offsets = Vec::with_capacity(keys.len());
        for (row_pk, key) in row_pks.iter().zip(keys) {
            let typed =
                WasmTypedRow::from_test_json_unchecked(row_pk, &serde_json::json!({ "id": key }))
                    .expect("test row should become typed");
            let payload = typed
                .durable_payload()
                .expect("test typed row should encode");
            let start = arena.len();
            arena.extend_from_slice(payload.as_ref());
            offsets.push((start, arena.len()));
        }
        let mut rows = CertifiedParameterInsertBatch::new_typed(
            row_pks,
            arena,
            offsets,
            schema_key.into(),
            "main".into(),
            certificate,
        )
        .expect("certified typed rows should construct");
        rows.untracked = untracked;
        rows.into_prepared(None, timestamp, &FunctionProviderHandle::system())
            .expect("certified typed rows should prepare")
    }

    fn expdl_probe_batch(keys: &[&str], untracked: bool) -> PreparedStateBatch {
        typed_certified_probe_batch(
            keys,
            "expdl_probe",
            untracked,
            expdl_probe_certificate(7),
            LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:00.000Z"),
        )
    }

    /// Certified typed rows must retain their durability lane after preparation
    /// and after later row-local mutations.
    #[test]
    fn typed_certified_expansion_preserves_untracked_lane() {
        let mut prepared = expdl_probe_batch(&["a", "b"], true);

        assert!(!prepared.is_dense_certified_parameter());
        assert!(
            prepared.iter().all(|row| row.untracked),
            "prepared rows must report the untracked lane"
        );

        prepared.set_durable_predecessor(0, None);
        assert!(
            prepared.iter().all(|row| row.untracked),
            "expansion must not move untracked rows into the tracked lane"
        );
    }

    /// Appending certified typed rows must preserve each row's durability lane.
    #[test]
    fn typed_certified_cohort_merge_keeps_lanes_apart() {
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
    fn certified_parameter_rows_keep_batch_common_prepared_facts_with_native_snapshots() {
        let certificate = CertifiedRawWriteBatchPreparation {
            schema_plan_id: SchemaPlanId::for_test(7),
            facts: PreparedRowFacts {
                row_content_validated: true,
                requires_transaction_validation: false,
            },
            tracked_keys_strictly_ordered: true,
            complete_collection_replacement: None,
            fileless_typed_sql_rows: false,
        };
        let timestamp = LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:00.000Z");
        let mut prepared =
            typed_certified_probe_batch(&["a", "b"], "dense_probe", false, certificate, timestamp);

        assert!(prepared.is_dense_certified_parameter());
        assert_eq!(prepared.slots.len(), 0);
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared.first().unwrap().row_pk, &RowPk::single("a"));
        let last = prepared.last().unwrap();
        let decoded = WasmTypedRow::decode_durable_payload(
            Arc::from(last.snapshot.unwrap()),
            last.schema_key.as_str(),
            last.row_pk,
        )
        .expect("prepared snapshot should decode");
        assert_eq!(decoded.to_json_shared().unwrap().as_str(), r#"{"id":"b"}"#);
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
        assert_eq!(prepared.slots.len(), 2);
        assert!(prepared.row(0).facts.requires_transaction_validation);
        assert!(!prepared.row(1).facts.requires_transaction_validation);
        assert_eq!(prepared.row(1).commit_id, Some(commit_id));
        assert_eq!(prepared.row(0).change_id, Some(assigned[0]));
        assert_eq!(prepared.row(1).change_id, Some(assigned[1]));
    }

    #[test]
    fn certified_typed_replacement_keeps_complete_collection_proof() {
        let mut prepared = typed_certified_probe_batch(
            &["a", "b"],
            "dense_replacement",
            false,
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
                fileless_typed_sql_rows: false,
            },
            LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:00.000Z"),
        );

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
    fn certified_typed_replacement_keeps_authenticated_predecessors_through_mutation() {
        let timestamp = LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:00.000Z");
        let mut prepared = typed_certified_probe_batch(
            &["a", "b"],
            "dense_replacement",
            false,
            CertifiedRawWriteBatchPreparation {
                schema_plan_id: SchemaPlanId::for_test(10),
                facts: PreparedRowFacts {
                    row_content_validated: true,
                    requires_transaction_validation: false,
                },
                tracked_keys_strictly_ordered: true,
                complete_collection_replacement: None,
                fileless_typed_sql_rows: false,
            },
            timestamp,
        );
        prepared.set_durable_predecessor(
            0,
            Some(CertifiedCurrentStatePredecessor::Encoded(
                Bytes::from_static(b"predecessor-a"),
            )),
        );
        prepared.set_durable_predecessor(
            1,
            Some(CertifiedCurrentStatePredecessor::Encoded(
                Bytes::from_static(b"predecessor-b"),
            )),
        );

        assert!(!prepared.is_dense_certified_parameter());
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
    fn typed_replacements_require_explicit_recertification_after_append() {
        let certificate = CertifiedRawWriteBatchPreparation {
            schema_plan_id: SchemaPlanId::for_test(9),
            facts: PreparedRowFacts {
                row_content_validated: true,
                requires_transaction_validation: false,
            },
            tracked_keys_strictly_ordered: true,
            complete_collection_replacement: None,
            fileless_typed_sql_rows: false,
        };
        let prepare = |key: &str, timestamp| {
            typed_certified_probe_batch(&[key], "dense_replacement", false, certificate, timestamp)
        };
        let first_timestamp = LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:00.000Z");
        let second_timestamp = LixTimestamp::expect_parse("timestamp", "2026-08-02T00:00:01.000Z");
        let commit_id = CommitId::for_test_label("dense-coalesced-replacements");
        let mut prepared = prepare("a", first_timestamp);
        prepared.set_commit_id_all(commit_id);
        let mut second = prepare("b", second_timestamp);
        second.set_commit_id_all(commit_id);

        prepared.append(second);

        assert!(!prepared.is_dense_certified_parameter());
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared.row(0).created_at, first_timestamp);
        assert_eq!(prepared.row(1).created_at, second_timestamp);
        assert_eq!(prepared.row(0).commit_id, Some(commit_id));
        assert_eq!(prepared.row(1).commit_id, Some(commit_id));
        assert!(
            !prepared.certified_tracked_keys_strictly_ordered(),
            "append must invalidate the ordered-set proof until the combined rows are certified"
        );
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
        assert!(prepared.certified_tracked_keys_strictly_ordered());
        assert_eq!(prepared.row(0).updated_at, first_timestamp);
        assert_eq!(prepared.row(1).updated_at, first_timestamp);
        assert!(prepared.iter().all(|row| row.snapshot.is_some()));

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
            fileless_typed_sql_rows: false,
        };
        let prepare = |schema_key: &str, key: &str, timestamp| {
            typed_certified_probe_batch(&[key], schema_key, false, certificate, timestamp)
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
        snapshot: Option<TransactionJson>,
        operation: TransactionWriteOperation,
        origin_key: &SharedStr,
    ) -> TestPreparedStateRow {
        let timestamp = LixTimestamp::expect_parse("timestamp", "2026-07-28T00:00:00.000Z");
        let row_pk = RowPk::single(row);
        let snapshot = snapshot.map(|snapshot| {
            let value = serde_json::from_str(snapshot.normalized())
                .expect("test JSON ingress should remain valid");
            let typed = WasmTypedRow::from_test_json_unchecked(&row_pk, &value)
                .expect("test snapshot should become typed");
            Bytes::from_owner(
                typed
                    .durable_payload()
                    .expect("test native snapshot should encode"),
            )
        });
        TestPreparedStateRow {
            schema_plan_id: SchemaPlanId::for_test(0),
            facts: PreparedRowFacts::default(),
            row_pk,
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
    fn prepared_batch_compacts_superseded_owner_columns() {
        let origin_key: SharedStr = "one-execution".into();
        let staged_snapshot = |value: String| {
            TransactionJson::from_certified_shared_normalized_row_content(value.into())
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
            assert!(batch.metadata.len() <= 4);
            assert!(batch.strings.len() <= 8);
            assert!(batch.origins.len() <= 2);
        }

        let row = batch.row(0);
        let decoded = WasmTypedRow::decode_durable_payload(
            Arc::from(row.snapshot.expect("replacement snapshot")),
            row.schema_key.as_str(),
            row.row_pk,
        )
        .expect("replacement snapshot should decode");
        assert_eq!(
            decoded.to_json_shared().unwrap().as_str(),
            r#"{"id":"a","version":64}"#
        );
        assert_eq!(
            batch.row(1).row_pk.as_single_string().expect("scalar pk"),
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
    fn certified_shared_content_can_materialize_from_the_prepared_boundary() {
        let transaction_json = TransactionJson::from_certified_shared_normalized_row_content(
            r#"{"path":"/a","value":{"nested":true}}"#.into(),
        );
        assert!(transaction_json.row_content_certified());

        let staged = stage_json_from_value(transaction_json);
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
            TransactionJson::from_certified_shared_normalized_row_content(normalized.into()),
        );

        assert!(!staged.is_inline());
        assert_eq!(staged.json_ref, expected);
    }





    #[test]
    fn certified_transaction_rows_release_without_moving_native_columns() {
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
                        Some(snapshot),
                        TransactionWriteOperation::Insert,
                        &origin_key,
                    )
                })
                .collect(),
        );
        let metadata_allocation = batch.metadata.as_ptr();
        let snapshot_arenas = batch.snapshot_arenas.as_ptr();

        batch.release_validated_canonical_value_columns();

        assert_eq!(
            batch.metadata.as_ptr(),
            metadata_allocation,
            "native metadata owners must not move during validation release"
        );
        assert_eq!(batch.snapshot_arenas.as_ptr(), snapshot_arenas);
        assert!(batch.iter().all(|row| row.snapshot.is_some()));
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
        let mut staged = stage_json_from_value(snapshot);

        assert!(!staged.retains_decoded_value_for_tests());
        assert_eq!(staged.value()["id"], "row-1");
        assert!(staged.retains_decoded_value_for_tests());
        assert!(staged.release_validated_canonical_value_column());
        assert!(!staged.retains_decoded_value_for_tests());
        assert_eq!(staged.normalized(), r#"{"id":"row-1"}"#);
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
        let first = match &rows[0].as_ref().expect("first row").storage {
            TransactionJsonStorage::CanonicalShared { normalized, .. } => normalized,
            _ => panic!("first row must retain canonical shared bytes"),
        };
        let second = match &rows[1].as_ref().expect("second row").storage {
            TransactionJsonStorage::CanonicalShared { normalized, .. } => normalized,
            _ => panic!("second row must retain canonical shared bytes"),
        };
        assert!(first.shares_buffer_with(second));
        assert_eq!(first.retained_buffer_len(), first.len() + second.len());
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
        let first = match &rows[0].as_ref().expect("first row").storage {
            TransactionJsonStorage::CanonicalShared { normalized, .. } => normalized,
            _ => panic!("first row must retain canonical shared bytes"),
        };
        let second = match &rows[1].as_ref().expect("second row").storage {
            TransactionJsonStorage::CanonicalShared { normalized, .. } => normalized,
            _ => panic!("second row must retain canonical shared bytes"),
        };
        assert_eq!(first.as_str(), cached);
        assert!(first.shares_buffer_with(second));
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
