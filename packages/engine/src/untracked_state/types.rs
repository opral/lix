use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::NullableKeyFilter;

/// Durable local row excluded from changelog and commit membership.
///
/// This is the canonical physical shape: identity/header fields are stored
/// directly, and mutable JSON payloads are stored inline in the sidecar row.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
pub(crate) struct UntrackedStateRow {
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) file_id: Option<String>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) snapshot_content: Option<String>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) global: bool,
    pub(crate) branch_id: String,
}

impl UntrackedStateRow {
    pub(crate) fn created_at(&self) -> LixTimestamp {
        self.created_at
    }

    pub(crate) fn updated_at(&self) -> LixTimestamp {
        self.updated_at
    }

    pub(crate) fn as_ref(&self) -> UntrackedStateRowRef<'_> {
        UntrackedStateRowRef {
            entity_pk: &self.entity_pk,
            schema_key: &self.schema_key,
            file_id: self.file_id.as_deref(),
            snapshot_content: self.snapshot_content.as_deref(),
            metadata: self.metadata.as_deref(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            global: self.global,
            branch_id: &self.branch_id,
        }
    }
}

/// Zero-copy view of untracked-state write row.
///
/// Untracked state owns this storage-facing write shape. Callers adapt into it
/// without making untracked_state depend on transaction or live-state types.
#[derive(Debug, Clone, Copy, musli::Encode)]
pub(crate) struct UntrackedStateRowRef<'a> {
    pub(crate) entity_pk: &'a EntityPk,
    pub(crate) schema_key: &'a str,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) file_id: Option<&'a str>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) snapshot_content: Option<&'a str>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) metadata: Option<&'a str>,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) global: bool,
    pub(crate) branch_id: &'a str,
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct UntrackedPayloadRef<'a> {
    #[musli(with = crate::storage_codec::option)]
    pub(crate) snapshot_content: Option<&'a str>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) metadata: Option<&'a str>,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) global: bool,
}

/// Hydrated boundary shape for callers that still work with JSON payloads.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MaterializedUntrackedStateRow {
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) deleted: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) branch_id: String,
}

/// Stable identity for one local untracked overlay row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct UntrackedStateIdentity {
    pub(crate) branch_id: String,
    pub(crate) schema_key: String,
    pub(crate) entity_pk: EntityPk,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) file_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, musli::Encode)]
#[musli(packed)]
pub(crate) struct UntrackedStateIdentityRef<'a> {
    pub(crate) branch_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) entity_pk: &'a EntityPk,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) file_id: Option<&'a str>,
}

impl UntrackedStateIdentity {
    pub(crate) fn as_ref(&self) -> UntrackedStateIdentityRef<'_> {
        UntrackedStateIdentityRef {
            branch_id: &self.branch_id,
            schema_key: &self.schema_key,
            entity_pk: &self.entity_pk,
            file_id: self.file_id.as_deref(),
        }
    }
}

impl<'a> From<UntrackedStateRowRef<'a>> for UntrackedStateIdentityRef<'a> {
    fn from(row: UntrackedStateRowRef<'a>) -> Self {
        Self {
            branch_id: row.branch_id,
            schema_key: row.schema_key,
            entity_pk: row.entity_pk,
            file_id: row.file_id,
        }
    }
}

/// Identity-centered filter for untracked local overlay scans.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct UntrackedStateFilter {
    #[serde(default)]
    pub(crate) schema_keys: Vec<String>,
    #[serde(default)]
    pub(crate) entity_pks: Vec<EntityPk>,
    #[serde(default)]
    pub(crate) branch_ids: Vec<String>,
    #[serde(default)]
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
}

/// Requested property set for an untracked-state scan.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct UntrackedStateProjection {
    #[serde(default)]
    pub(crate) columns: Vec<String>,
}

/// Scan request for local untracked overlay rows.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct UntrackedStateScanRequest {
    #[serde(default)]
    pub(crate) filter: UntrackedStateFilter,
    #[serde(default)]
    pub(crate) projection: UntrackedStateProjection,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

/// Point lookup request for one untracked local overlay row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UntrackedStateRowRequest {
    pub(crate) schema_key: String,
    pub(crate) branch_id: String,
    pub(crate) entity_pk: EntityPk,
    pub(crate) file_id: NullableKeyFilter<String>,
}

#[derive(musli::Encode)]
#[musli(packed)]
pub(crate) struct UntrackedBranchPrefixRef<'a> {
    pub(crate) branch_id: &'a str,
}

#[derive(musli::Encode)]
#[musli(packed)]
pub(crate) struct UntrackedBranchSchemaPrefixRef<'a> {
    pub(crate) branch_id: &'a str,
    pub(crate) schema_key: &'a str,
}

#[derive(musli::Encode)]
#[musli(packed)]
pub(crate) struct UntrackedBranchSchemaEntityPrefixRef<'a> {
    pub(crate) branch_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) entity_pk: &'a EntityPk,
}

#[derive(musli::Encode)]
#[musli(packed)]
pub(crate) struct UntrackedBranchSchemaEntityFilePrefixRef<'a> {
    pub(crate) branch_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) entity_pk: &'a EntityPk,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) file_id: Option<&'a str>,
}
