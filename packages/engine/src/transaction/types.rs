use std::{collections::BTreeSet, fmt, ops::Deref, sync::Arc};

use crate::entity_identity::EntityIdentity;
use crate::json_store::{JsonRef, JsonStoreWriter, NormalizedJson};
use crate::live_state::MaterializedLiveStateRow;
use crate::schema_catalog::SchemaPlanId;
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::untracked_state::MaterializedUntrackedStateRow;
use crate::LixError;
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
/// may omit defaulted snapshot fields and leave `entity_id` unset when the
/// target schema has an `x-lix-primary-key`; transaction normalization applies
/// schema defaults and derives the final identity. Typed UPDATE providers must
/// stage full rewritten snapshots after applying column assignments to the
/// existing row. Raw `lix_state` snapshot updates are replacement writes, not
/// implicit patches.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TransactionWriteRow {
    pub(crate) entity_id: Option<EntityIdentity>,
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
    pub(crate) version_id: String,
}

impl TransactionWriteRow {
    pub(crate) fn schema_scope_version_id(&self) -> &str {
        if self.global {
            crate::GLOBAL_VERSION_ID
        } else {
            self.version_id.as_str()
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
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) data: Vec<u8>,
}

/// Existing canonical change adopted into another version's tracked projection.
///
/// Merges use this path when the source side already owns the canonical
/// changelog fact. The target commit references that existing change id and
/// writes a target-version projection row without appending a copied change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransactionAdoptedChange {
    pub(crate) version_id: String,
    pub(crate) change_id: String,
    pub(crate) projected_row: MaterializedTrackedStateRow,
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
    AdoptedChanges {
        changes: Vec<TransactionAdoptedChange>,
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
    AdoptedChanges {
        rows: Vec<PreparedAdoptedStateRow>,
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
}

pub(crate) fn stage_json_from_value(
    json_writer: &mut JsonStoreWriter,
    value: TransactionJson,
    _context: &str,
) -> Result<StageJson, LixError> {
    let (value, normalized) = value.into_parts();
    let json_ref =
        json_writer.prepare_json(NormalizedJson::from_arc_unchecked(Arc::clone(&normalized)))?;
    Ok(StageJson {
        value,
        normalized,
        json_ref,
    })
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PreparedRowFacts {
    /// Placeholder for the next cut: row-derived constraint facts will be
    /// computed once during normalization and consumed by validation.
    pub(crate) _sealed: (),
}

/// Prepared state row owned by the transaction write buffer.
///
/// This is the first boundary that owns `StageJson`: JSON has been normalized,
/// assigned a `JsonRef`, and staged in the transaction-local `JsonStoreWriter`.
/// Storage owners must receive only the ref-backed row forms derived from this
/// type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedStateRow {
    pub(crate) schema_plan_id: SchemaPlanId,
    pub(crate) facts: PreparedRowFacts,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot: Option<StageJson>,
    pub(crate) metadata: Option<StageJson>,
    pub(crate) origin: Option<TransactionWriteOrigin>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) change_id: Option<String>,
    pub(crate) commit_id: Option<String>,
    pub(crate) untracked: bool,
    pub(crate) version_id: String,
}

/// Transaction-hydrated projection for an adopted canonical change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedAdoptedStateRow {
    pub(crate) schema_plan_id: SchemaPlanId,
    pub(crate) facts: PreparedRowFacts,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot: Option<StageJson>,
    pub(crate) metadata: Option<StageJson>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) change_id: String,
    pub(crate) commit_id: String,
    pub(crate) version_id: String,
}

impl From<PreparedStateRow> for MaterializedLiveStateRow {
    fn from(row: PreparedStateRow) -> Self {
        MaterializedLiveStateRow {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            file_id: row.file_id,
            snapshot_content: row.snapshot.map(|snapshot| snapshot.materialize()),
            metadata: row.metadata.map(|metadata| metadata.materialize()),
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            change_id: row.change_id,
            commit_id: row.commit_id,
            untracked: row.untracked,
            version_id: row.version_id,
        }
    }
}

impl From<&PreparedStateRow> for MaterializedLiveStateRow {
    fn from(row: &PreparedStateRow) -> Self {
        MaterializedLiveStateRow {
            entity_id: row.entity_id.clone(),
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            snapshot_content: row.snapshot.as_ref().map(StageJson::materialize),
            metadata: row.metadata.as_ref().map(StageJson::materialize),
            created_at: row.created_at.clone(),
            updated_at: row.updated_at.clone(),
            global: row.global,
            change_id: row.change_id.clone(),
            commit_id: row.commit_id.clone(),
            untracked: row.untracked,
            version_id: row.version_id.clone(),
        }
    }
}

impl From<PreparedAdoptedStateRow> for MaterializedLiveStateRow {
    fn from(row: PreparedAdoptedStateRow) -> Self {
        MaterializedLiveStateRow {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            file_id: row.file_id,
            snapshot_content: row.snapshot.map(|snapshot| snapshot.materialize()),
            metadata: row.metadata.map(|metadata| metadata.materialize()),
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            change_id: Some(row.change_id),
            commit_id: Some(row.commit_id),
            untracked: false,
            version_id: row.version_id,
        }
    }
}

impl From<&PreparedAdoptedStateRow> for MaterializedLiveStateRow {
    fn from(row: &PreparedAdoptedStateRow) -> Self {
        MaterializedLiveStateRow {
            entity_id: row.entity_id.clone(),
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            snapshot_content: row.snapshot.as_ref().map(StageJson::materialize),
            metadata: row.metadata.as_ref().map(StageJson::materialize),
            created_at: row.created_at.clone(),
            updated_at: row.updated_at.clone(),
            global: row.global,
            change_id: Some(row.change_id.clone()),
            commit_id: Some(row.commit_id.clone()),
            untracked: false,
            version_id: row.version_id.clone(),
        }
    }
}

impl From<PreparedStateRow> for MaterializedUntrackedStateRow {
    fn from(row: PreparedStateRow) -> Self {
        MaterializedUntrackedStateRow {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            file_id: row.file_id,
            snapshot_content: row.snapshot.map(|snapshot| snapshot.materialize()),
            metadata: row.metadata.map(|metadata| metadata.materialize()),
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            version_id: row.version_id,
        }
    }
}

/// Transaction-local introduced-change membership accumulated while rows are staged.
///
/// Final commit row materialization owns commit ids, parent heads, and commit
/// row timestamps. Staging only tracks which hydrated tracked changes the
/// future commit introduces for a version.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StagedCommitMembers {
    pub(crate) commit_id: String,
    pub(crate) commit_change_id: String,
    pub(crate) change_set_id: String,
    pub(crate) created_at: String,
    pub(crate) change_ids: BTreeSet<String>,
    pub(crate) allow_empty: bool,
}

impl StagedCommitMembers {
    pub(crate) fn new(
        commit_id: String,
        commit_change_id: String,
        change_set_id: String,
        created_at: String,
    ) -> Self {
        Self {
            commit_id,
            commit_change_id,
            change_set_id,
            created_at,
            change_ids: BTreeSet::new(),
            allow_empty: false,
        }
    }

    pub(crate) fn add_change_id(&mut self, change_id: String) {
        self.change_ids.insert(change_id);
    }

    pub(crate) fn remove_change_id(&mut self, change_id: &str) {
        self.change_ids.remove(change_id);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.change_ids.is_empty()
    }

    pub(crate) fn allow_empty(&mut self) {
        self.allow_empty = true;
    }
}
