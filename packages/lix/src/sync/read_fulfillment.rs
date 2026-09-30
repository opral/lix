//! Operation-scoped discovery of immutable read dependencies at the authority.
//!
//! Recipes describe native reads, never remote SQL or result rows. A fulfilled
//! recipe only warms the cache: local evaluation still decides membership and
//! absence, including pending local changes.
use super::partial_state::PartialReplicaState;
use crate::storage_adapter::*;
use crate::tracked_state::{NativeMetadataRef, NativeObjectRef};
use crate::{
    LixError,
    hot_state::{InterestDomain, LogicalReadInterest, ReadInterestRegistry},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

const MAX_INPUT_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_RESPONSE_BYTES: usize = MAX_INPUT_BYTES.div_ceil(3) * 4 + 2 * 1024 * 1024;
const PAGE_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
const MAX_PAYLOAD_BYTES: usize = 256 * 1024 * 1024;
// Greedy pages are at least half full, except the final page. Oversize
// individual inputs consume more than a page by themselves.
const MAX_PAGES: usize = 2 * MAX_PAYLOAD_BYTES / PAGE_PAYLOAD_BYTES + 1;
const MAX_RECORDS: usize = 16384;
const MAX_READ_CALLS: usize = 65536;
const MAX_READ_BYTES: usize = 1024 * 1024 * 1024;
const MAX_RECIPE_BYTES: usize = 512 * 1024;
const MARKER: &str = "readFulfillment";

fn invalid(message: &str) -> LixError {
    LixError::new("LIX_READ_FULFILLMENT_INVALID", message)
}

#[derive(Clone, Copy)]
pub(crate) enum ClientFailurePhase {
    Validation,
    Installation,
}

impl ClientFailurePhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Validation => "read_fulfillment_validation",
            Self::Installation => "read_fulfillment_install",
        }
    }
}

/// Add bounded context where a client receives or installs a validly framed
/// read-fulfillment response. Never copy parser text, addresses, row values,
/// or storage diagnostics into these stable telemetry fields.
pub(crate) fn annotate_client_failure(mut error: LixError, phase: ClientFailurePhase) -> LixError {
    if error.code != "LIX_READ_FULFILLMENT_INVALID" {
        return error;
    }
    let known_reason = error
        .details
        .as_ref()
        .and_then(|details| details.get("payloadFailureReason"))
        .and_then(|value| value.as_str())
        .filter(|reason| {
            matches!(
                *reason,
                "selected_change_payload_identity_or_lifetime_mismatch"
                    | "selected_change_payload_recipe_mismatch"
                    | "selected_change_payload_locator_missing"
                    | "selected_change_payload_source_mismatch"
                    | "selected_change_payload_outside_descriptor_scope"
                    | "read_fulfillment_resident_input_conflict"
            )
        })
        .map(str::to_owned);
    let failure_reason = known_reason.as_deref().or(match error.message.as_str() {
        "canonical selected change payload identity or lifetime mismatch" => {
            Some("selected_change_payload_identity_or_lifetime_mismatch")
        }
        "canonical change payload has no matching row recipe" => {
            Some("selected_change_payload_recipe_mismatch")
        }
        "canonical change payload has no selected source locator" => {
            Some("selected_change_payload_locator_missing")
        }
        "canonical change payload source disagrees with its selected locator" => {
            Some("selected_change_payload_source_mismatch")
        }
        "canonical change payload is outside the descriptor branch scope" => {
            Some("selected_change_payload_outside_descriptor_scope")
        }
        "read fulfillment conflicts with resident input" => {
            Some("read_fulfillment_resident_input_conflict")
        }
        _ => Some(match phase {
            ClientFailurePhase::Validation => "read_fulfillment_validation_failed",
            ClientFailurePhase::Installation => "read_fulfillment_install_failed",
        }),
    });
    let details = error
        .details
        .get_or_insert_with(|| Box::new(serde_json::json!({})));
    if let Some(details) = details.as_object_mut() {
        details.insert("payloadPhase".into(), serde_json::json!(phase.as_str()));
        if let Some(failure_reason) = failure_reason {
            details.insert(
                "payloadFailureReason".into(),
                serde_json::json!(failure_reason),
            );
        }
    }
    error
}

fn preserves_local_mutable_native_overlay(address: &ReadInputAddress) -> bool {
    matches!(
        address,
        ReadInputAddress::Metadata(
            NativeMetadataRef::CommitGraphRecord(_) | NativeMetadataRef::ChangeLocator(_)
        ) | ReadInputAddress::ChangeRecord { .. }
    )
}

/// A commit's header, mutation catalog, and directly addressed parts share a
/// physical owner key.  A partial replica may retain a locally selected
/// representation for that owner after an upload/adoption transition while
/// the authority's optional closure names a newer representation.  Keep the
/// local representation coherent as a unit; content-addressed nodes continue
/// to require their exact digest.
#[derive(Clone)]
struct LocalOwnerAuthority {
    manifest: crate::tracked_state::CommitStateManifest,
    catalog_digest: [u8; 32],
}

fn owner_commit_id(address: &ReadInputAddress) -> Option<crate::changelog::CommitId> {
    match address {
        ReadInputAddress::Metadata(NativeMetadataRef::CommitStateHeader(id)) => {
            crate::changelog::CommitId::parse(id).ok()
        }
        ReadInputAddress::Object(NativeObjectRef::MutationCatalog { commit_id, .. })
        | ReadInputAddress::Object(NativeObjectRef::CommitDeltaPart { commit_id, .. }) => Some(
            crate::changelog::CommitId::new(uuid::Uuid::from_bytes(*commit_id)),
        ),
        _ => None,
    }
}

fn owner_input_matches_local_authority(
    authority: &LocalOwnerAuthority,
    address: &ReadInputAddress,
) -> bool {
    let Some(local_digest) = owner_input_local_digest(authority, address) else {
        return false;
    };
    match address {
        // The header itself is the row that selected the local representation;
        // a differing optional authority header is always retained locally.
        ReadInputAddress::Metadata(NativeMetadataRef::CommitStateHeader(_)) => false,
        ReadInputAddress::Object(NativeObjectRef::MutationCatalog {
            expected_digest, ..
        }) => local_digest == *expected_digest,
        ReadInputAddress::Object(NativeObjectRef::CommitDeltaPart {
            expected_digest, ..
        }) => local_digest == *expected_digest,
        _ => false,
    }
}

fn owner_input_local_digest(
    authority: &LocalOwnerAuthority,
    address: &ReadInputAddress,
) -> Option<[u8; 32]> {
    match address {
        ReadInputAddress::Object(NativeObjectRef::MutationCatalog { .. }) => {
            Some(authority.catalog_digest)
        }
        ReadInputAddress::Object(NativeObjectRef::CommitDeltaPart {
            part_index,
            replacement,
            ..
        }) => {
            let index = usize::try_from(*part_index).ok()?;
            if *replacement {
                authority
                    .manifest
                    .mutations
                    .parts
                    .get(index)
                    .and_then(|part| part.replacement_part.as_ref())
                    .map(|part| part.content_digest)
                    .or_else(|| {
                        authority
                            .manifest
                            .mutations
                            .replacement_part_digests
                            .get(index)
                            .copied()
                    })
            } else {
                authority
                    .manifest
                    .mutations
                    .parts
                    .get(index)
                    .filter(|part| part.replacement_part.is_none())
                    .map(|part| part.content_digest)
            }
        }
        _ => None,
    }
}

fn validate_local_owner_input(
    authority: &LocalOwnerAuthority,
    address: &ReadInputAddress,
    bytes: &[u8],
) -> Result<(), LixError> {
    match address {
        ReadInputAddress::Metadata(NativeMetadataRef::CommitStateHeader(_)) => {
            address.validate(bytes)
        }
        ReadInputAddress::Object(NativeObjectRef::MutationCatalog { commit_id, .. }) => {
            NativeObjectRef::MutationCatalog {
                commit_id: *commit_id,
                expected_digest: authority.catalog_digest,
            }
            .validate(bytes)
        }
        ReadInputAddress::Object(NativeObjectRef::CommitDeltaPart {
            commit_id,
            part_index,
            replacement,
            ..
        }) => {
            let expected_digest = owner_input_local_digest(authority, address)
                .ok_or_else(|| invalid("retained owner has no matching mutation part"))?;
            NativeObjectRef::CommitDeltaPart {
                commit_id: *commit_id,
                part_index: *part_index,
                expected_digest,
                replacement: *replacement,
            }
            .validate(bytes)
        }
        _ => Err(invalid("invalid retained owner input")),
    }
}

pub(crate) fn annotate_capture(
    mut error: LixError,
    capture: Option<&ReadInterestRegistry>,
) -> LixError {
    if error.automatic_retry_is_forbidden()
        || error
            .details
            .as_ref()
            .is_some_and(|d| d.get("nativeHistoryDemand").is_some())
    {
        return error;
    }
    let Some(capture) = capture else {
        return error;
    };
    let snapshot = match capture.snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => return error,
    };
    if snapshot.interests.is_empty()
        || snapshot
            .interests
            .iter()
            .any(|interest| matches!(interest.as_ref(), LogicalReadInterest::Diff { .. }))
    {
        // Historical diffs may name local pending commits. Their specialized
        // demand path owns those endpoints; the authority cannot replay the
        // client's private history as an admitted current-state recipe.
        return error;
    }
    if snapshot.serialized_bytes > MAX_RECIPE_BYTES {
        return invalid("read operation recipe byte limit exceeded");
    }
    // Keep the native missing diagnostic intact for corruption handling and
    // pinned transaction admission. Failed captures are never published.
    let interests: Vec<_> = snapshot
        .interests
        .iter()
        .map(|value| value.as_ref())
        .collect();
    let details = error
        .details
        .get_or_insert_with(|| Box::new(serde_json::json!({})));
    if let Some(details) = details.as_object_mut() {
        details.insert(MARKER.into(), serde_json::json!({"interests": interests}));
        details.insert("nativeReadRecipeCount".into(), interests.len().into());
        let recipe_mask = interests.iter().fold(0_u16, |mask, interest| {
            mask | payload_recipe_mask(std::slice::from_ref(*interest))
        });
        details.insert("nativeReadRecipeMask".into(), recipe_mask.into());
    }
    error
}

/// A selected live change can be absent from a sparse replica even when all
/// immutable index nodes are present. Demand its canonical locator through
/// the captured recipe so the authority returns the paired change payload.
/// Full replicas never use this recovery path.
pub(crate) fn selected_change_payload_locator(error: &LixError) -> Option<NativeMetadataRef> {
    if error.code != LixError::CODE_INTERNAL_ERROR {
        return None;
    }
    let details = error.details.as_ref()?;
    if details.get("payloadFailureReason")?.as_str()? != "selected_change_payload_unavailable" {
        return None;
    }
    let change_id = details.get("changeId")?.as_str()?;
    let parsed = uuid::Uuid::parse_str(change_id).ok()?;
    if parsed.to_string() != change_id {
        return None;
    }
    Some(NativeMetadataRef::ChangeLocator(change_id.to_owned()))
}

pub(super) fn interests_for_error(
    error: &LixError,
) -> Result<Option<Vec<LogicalReadInterest>>, LixError> {
    let Some(marker) = error.details.as_ref().and_then(|d| d.get(MARKER)) else {
        return Ok(None);
    };
    let interests = marker
        .get("interests")
        .ok_or_else(|| invalid("missing read recipes"))?;
    if serde_json::to_vec(interests)
        .map_err(|_| invalid("invalid recipes"))?
        .len()
        > MAX_RECIPE_BYTES
    {
        return Err(invalid("read recipe byte limit exceeded"));
    }
    let interests: Vec<LogicalReadInterest> =
        serde_json::from_value(interests.clone()).map_err(|_| invalid("invalid read recipes"))?;
    if interests.is_empty() || interests.len() > 4096 {
        return Err(invalid("read recipe count limit exceeded"));
    }
    Ok(Some(interests))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ReadFulfillmentRequest {
    pub(crate) epoch_id: String,
    pub(crate) descriptor: super::PartialReplicaDescriptor,
    pub(crate) interests: Vec<LogicalReadInterest>,
    pub(crate) required: Vec<ReadInputAddress>,
    pub(crate) continuation: Option<ReadContinuation>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ReadContinuation {
    pub(crate) next_input: usize,
    pub(crate) closure_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "address",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum ReadInputAddress {
    Metadata(NativeMetadataRef),
    Object(NativeObjectRef),
    /// A canonical changelog payload selected by one descriptor-scoped row
    /// identity. The payload itself is mutable in CHANGE_SPACE, so its exact
    /// row lifetime and source owner travel with the typed input.
    ChangeRecord {
        change_id: String,
        source_commit_id: String,
        branch_id: String,
        schema_key: String,
        file_id: Option<String>,
        #[serde(with = "row_pk_payload")]
        row_pk: crate::row_pk::RowPk,
        updated_at: String,
        payload_digest: [u8; 32],
    },
    BlobManifest([u8; 32]),
    BlobChunk([u8; 32]),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ReadInput {
    pub(crate) address: ReadInputAddress,
    #[serde(with = "payload")]
    pub(crate) bytes: Vec<u8>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DiscoveryProfile {
    pub(crate) storage_calls: usize,
    pub(crate) storage_bytes: usize,
    pub(crate) payload_bytes: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ReadFulfillmentResponse {
    pub(crate) lix_id: String,
    pub(crate) epoch_id: String,
    pub(crate) request_digest: String,
    pub(crate) inputs: Vec<ReadInput>,
    pub(crate) profile: DiscoveryProfile,
    pub(crate) closure_digest: String,
    pub(crate) continuation: Option<ReadContinuation>,
}
mod payload {
    use super::*;
    use base64::Engine;
    pub(super) fn serialize<S: serde::Serializer>(
        bytes: &[u8],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        if encoded.len() > MAX_INPUT_BYTES.div_ceil(3) * 4 {
            return Err(serde::de::Error::custom("read input exceeds bound"));
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(serde::de::Error::custom)?;
        if bytes.len() > MAX_INPUT_BYTES {
            return Err(serde::de::Error::custom("read input exceeds bound"));
        }
        Ok(bytes)
    }
}

/// `RowPk`'s ordinary JSON form intentionally erases UUID-vs-text component
/// types for SQL-facing values. Read-fulfillment identities cross the wire and
/// must preserve that distinction so clients can revalidate the canonical row
/// payload against the selected identity.
mod row_pk_payload {
    use super::*;

    pub(super) fn serialize<S: serde::Serializer>(
        row_pk: &crate::row_pk::RowPk,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        row_pk
            .as_typed_json_array_value()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<crate::row_pk::RowPk, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        crate::row_pk::RowPk::from_typed_json_array_value(&value).map_err(serde::de::Error::custom)
    }
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
impl ReadFulfillmentRequest {
    pub(crate) fn validate(&self, repository: &str) -> Result<(), LixError> {
        self.descriptor
            .validate(repository, Some(&self.descriptor.selected_branch.branch_id))?;
        if uuid::Uuid::parse_str(&self.epoch_id).is_err()
            || self.required.is_empty()
            || self.required.len() > 32
            || self.interests.is_empty()
            || self.interests.len() > 4096
            || serde_json::to_vec(&self.interests)
                .map_err(|_| invalid("invalid recipes"))?
                .len()
                > MAX_RECIPE_BYTES
        {
            return Err(invalid("invalid read fulfillment request"));
        }
        // Historical endpoints may name private or unleased commits. Their
        // specialized discovery protocol owns that scope; do not accept them
        // merely because the foreground capture normally filters them out.
        if self
            .interests
            .iter()
            .any(|interest| matches!(interest, LogicalReadInterest::Diff { .. }))
        {
            return Err(invalid("historical diff recipes require history discovery"));
        }
        for interest in &self.interests {
            let LogicalReadInterest::Scan { request, domain } = interest else {
                continue;
            };
            if request.is_catalog_identity_only_scan() {
                let branch_id = request.filter.branch_ids.first();
                if *domain != InterestDomain::Tracked
                    || branch_id.is_none_or(|branch_id| {
                        branch_id != &self.descriptor.selected_branch.branch_id
                            && branch_id != &self.descriptor.global_branch.branch_id
                    })
                {
                    return Err(invalid(
                        "schema catalog identity scans are restricted to descriptor branches",
                    ));
                }
            }
        }
        let mut required = BTreeSet::new();
        for address in &self.required {
            if matches!(address, ReadInputAddress::ChangeRecord { .. }) {
                return Err(invalid(
                    "canonical change payloads are selected by the read recipe",
                ));
            }
            if !required.insert(address.coordinate()?) {
                return Err(invalid("duplicate required read input"));
            }
        }
        if let Some(cursor) = &self.continuation {
            if cursor.next_input == 0
                || cursor.next_input >= MAX_RECORDS
                || !valid_digest(&cursor.closure_digest)
            {
                return Err(invalid("invalid read continuation"));
            }
        }
        Ok(())
    }
    fn digest(&self) -> Result<String, LixError> {
        let mut basis = self.clone();
        basis.continuation = None;
        Ok(
            blake3::hash(&serde_json::to_vec(&basis).map_err(|_| invalid("invalid request"))?)
                .to_hex()
                .to_string(),
        )
    }
}

impl ReadInputAddress {
    pub(crate) fn coordinate(&self) -> Result<(StorageSpace, StorageKey), LixError> {
        use crate::binary_cas::*;
        Ok(match self {
            Self::Metadata(address) => (
                super::native_metadata::space(address),
                super::native_metadata::key(address)?,
            ),
            Self::Object(address) => (
                address.space(),
                StorageKey(Bytes::from(address.storage_key())),
            ),
            Self::ChangeRecord { change_id, .. } => {
                let change_id = canonical_change_id(change_id)?;
                (
                    crate::changelog::CHANGE_SPACE,
                    StorageKey(Bytes::copy_from_slice(change_id.as_uuid().as_bytes())),
                )
            }
            Self::BlobManifest(hash) => (
                BINARY_CAS_MANIFEST_SPACE,
                StorageKey(Bytes::copy_from_slice(hash)),
            ),
            Self::BlobChunk(hash) => (
                BINARY_CAS_CHUNK_SPACE,
                StorageKey(Bytes::copy_from_slice(hash)),
            ),
        })
    }
    fn validate(&self, bytes: &[u8]) -> Result<(), LixError> {
        match self {
            Self::Metadata(address) => super::native_metadata::validate_bytes(address, bytes),
            Self::Object(address) => address.validate(bytes),
            Self::ChangeRecord {
                change_id,
                source_commit_id,
                schema_key,
                file_id,
                row_pk,
                updated_at,
                payload_digest,
                ..
            } => {
                let change_id = canonical_change_id(change_id)?;
                canonical_commit_id(source_commit_id)?;
                let updated_at = canonical_timestamp(updated_at)?;
                let record = crate::changelog::decode_change_record(bytes, change_id)
                    .map_err(|_| invalid("invalid canonical selected change payload"))?;
                if record.schema_key != *schema_key
                    || record.file_id != *file_id
                    || record.row_pk != *row_pk
                    || record.snapshot.is_none()
                    || record.created_at != updated_at
                    || blake3::hash(bytes).as_bytes() != payload_digest
                {
                    return Err(invalid(
                        "canonical selected change payload identity or lifetime mismatch",
                    ));
                }
                Ok(())
            }
            Self::BlobManifest(hash) => {
                let wire: super::SyncBlobManifest = serde_json::from_slice(bytes)
                    .map_err(|_| invalid("invalid canonical blob input"))?;
                let manifest = super::blob::decode_manifest(&wire)?;
                if manifest.blob_id.as_bytes() != hash || wire.inline_bytes_base64.is_some() {
                    return Err(invalid("canonical blob input identity mismatch"));
                }
                Ok(())
            }
            Self::BlobChunk(hash) => {
                if bytes.is_empty()
                    || bytes.len() > 4 * 1024 * 1024
                    || blake3::hash(bytes).as_bytes() != hash
                {
                    return Err(invalid("blob chunk input digest mismatch"));
                }
                Ok(())
            }
        }
    }

    fn validate_existing_mutable_value(
        &self,
        bytes: &[u8],
        canonical_bytes: &[u8],
    ) -> Result<(), LixError> {
        match self {
            Self::ChangeRecord { change_id, .. } => {
                let change_id = canonical_change_id(change_id)?;
                self.validate(canonical_bytes)?;
                let existing = crate::changelog::decode_change_record(bytes, change_id)
                    .map_err(|_| invalid("invalid resident mutable change payload"))?;
                let canonical = crate::changelog::decode_change_record(canonical_bytes, change_id)
                    .map_err(|_| invalid("invalid canonical mutable change payload"))?;
                if existing != canonical || existing.snapshot.is_none() {
                    return Err(invalid(
                        "resident mutable change payload does not match canonical identity or lifetime",
                    ));
                }
                Ok(())
            }
            _ => self.validate(bytes),
        }
    }
}

fn canonical_change_id(value: &str) -> Result<crate::changelog::ChangeId, LixError> {
    let id = crate::changelog::ChangeId::parse(value)
        .map_err(|_| invalid("change payload ID must be a canonical UUID"))?;
    if id.to_string() != value {
        return Err(invalid("change payload ID must be a canonical UUID"));
    }
    Ok(id)
}

fn canonical_commit_id(value: &str) -> Result<crate::changelog::CommitId, LixError> {
    let id = crate::changelog::CommitId::parse(value)
        .map_err(|_| invalid("change payload source must be a canonical UUID"))?;
    if id.to_string() != value {
        return Err(invalid("change payload source must be a canonical UUID"));
    }
    Ok(id)
}

fn canonical_timestamp(value: &str) -> Result<crate::common::LixTimestamp, LixError> {
    let timestamp = crate::common::LixTimestamp::parse(value)
        .map_err(|_| invalid("change payload timestamp must be canonical"))?;
    if timestamp.to_string() != value {
        return Err(invalid("change payload timestamp must be canonical"));
    }
    Ok(timestamp)
}

fn append_receipt_input(
    receipt: &mut super::runtime::HydratedInputs,
    address: &ReadInputAddress,
    coordinate: (StorageSpace, StorageKey),
) -> Result<(), LixError> {
    match address {
        ReadInputAddress::Metadata(_)
        | ReadInputAddress::Object(_)
        | ReadInputAddress::ChangeRecord { .. } => {
            receipt.keys.push(coordinate);
        }
        ReadInputAddress::BlobChunk(hash) => {
            let key = StorageKey(Bytes::copy_from_slice(hash));
            receipt
                .keys
                .push((crate::binary_cas::BINARY_CAS_CHUNK_SPACE, key.clone()));
            receipt
                .keys
                .push((crate::binary_cas::BINARY_CAS_CHUNK_PRESENCE_SPACE, key));
        }
        ReadInputAddress::BlobManifest(hash) => {
            let blob = crate::binary_cas::BlobId::from_bytes(*hash);
            if !receipt.blob_manifests.contains(&blob) {
                receipt.blob_manifests.push(blob);
            }
        }
    }
    Ok(())
}

// Raw physical observations never cross the protocol boundary. Only the
// explicit immutable input algebra above can become installable wire data.
#[derive(Default)]
struct Observations {
    values: BTreeMap<(StorageSpace, StorageKey), Bytes>,
    profile: DiscoveryProfile,
}
impl Observations {
    fn charge_call(&mut self) -> Result<(), StorageError> {
        self.profile.storage_calls += 1;
        if self.profile.storage_calls > MAX_READ_CALLS {
            return Err(StorageError::Io(
                "read discovery work limit exceeded".into(),
            ));
        }
        Ok(())
    }
    fn observe(
        &mut self,
        space: StorageSpace,
        key: &StorageKey,
        value: &StorageProjectedValue,
    ) -> Result<(), StorageError> {
        let StorageProjectedValue::FullValue(bytes) = value else {
            return Ok(());
        };
        self.profile.storage_bytes = self.profile.storage_bytes.saturating_add(bytes.len());
        if self.profile.storage_bytes > MAX_READ_BYTES {
            return Err(StorageError::Io(
                "read discovery byte limit exceeded".into(),
            ));
        }
        if !is_input_space(space) {
            return Ok(());
        }
        if !self.values.contains_key(&(space, key.clone())) {
            self.profile.payload_bytes = self.profile.payload_bytes.saturating_add(bytes.len());
            if self.profile.payload_bytes > MAX_PAYLOAD_BYTES || self.values.len() >= MAX_RECORDS {
                return Err(StorageError::Io(
                    "read discovery payload limit exceeded".into(),
                ));
            }
            self.values.insert((space, key.clone()), bytes.clone());
        }
        Ok(())
    }
}
fn is_input_space(space: StorageSpace) -> bool {
    use crate::tracked_state::*;
    [
        TRACKED_STATE_TREE_CHUNK_SPACE,
        SCOPED_RANGE_NODE_SPACE,
        MUTATION_DIRECTORY_NODE_SPACE,
        TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
        TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        TRACKED_STATE_CHANGE_LOCATOR_SPACE,
        crate::changelog::COMMIT_SPACE,
    ]
    .contains(&space)
}
#[derive(Clone)]
pub(crate) struct DependencyRead<R> {
    base: R,
    observations: Arc<Mutex<Observations>>,
}
impl<R: StorageAdapterRead> StorageAdapterRead for DependencyRead<R> {
    fn requires_physical_reads(&self) -> bool {
        true
    }
    async fn get_many(
        &self,
        requests: &[StorageGetManyRequest<'_>],
    ) -> Result<StorageGetManyResult, StorageError> {
        self.observations.lock().unwrap().charge_call()?;
        let result = self.base.get_many(requests).await?;
        let mut values = result.values.iter();
        let mut observations = self.observations.lock().unwrap();
        for request in requests {
            for key in request.keys {
                let value = values.next().ok_or_else(|| {
                    StorageError::Io("discovery storage cardinality mismatch".into())
                })?;
                if let Some(value) = value {
                    observations.observe(request.space, key, value)?;
                }
            }
        }
        if values.next().is_some() {
            return Err(StorageError::Io(
                "discovery storage cardinality mismatch".into(),
            ));
        }
        Ok(result)
    }
    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: StorageKeyRange,
        opts: StorageBeginScanOptions,
    ) -> Result<StorageScanCursor<'_>, StorageError> {
        self.observations.lock().unwrap().charge_call()?;
        let cursor = self
            .base
            .begin_scan(space, range.clone(), opts.clone())
            .await?;
        StorageScanCursor::from_source(
            range,
            opts.order,
            DependencyScan {
                base: cursor,
                space,
                observations: self.observations.clone(),
            },
        )
    }
}
struct DependencyScan<'a> {
    base: StorageScanCursor<'a>,
    space: StorageSpace,
    observations: Arc<Mutex<Observations>>,
}
impl StorageScanSource for DependencyScan<'_> {
    fn next_page(
        &mut self,
        limit: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<StorageScanChunk, StorageError>> + Send + '_>>
    {
        Box::pin(async move {
            self.observations.lock().unwrap().charge_call()?;
            let (rows, more) = self
                .base
                .next_page(limit.min(MAX_SCAN_PAGE_ROWS))
                .await?
                .into_parts();
            let mut observations = self.observations.lock().unwrap();
            for row in &rows {
                observations.observe(self.space, &row.key, &row.value)?;
            }
            Ok(StorageScanChunk::new(rows, more))
        })
    }
}

fn fixed<const N: usize>(key: &[u8]) -> Result<[u8; N], LixError> {
    key.try_into()
        .map_err(|_| invalid("invalid immutable input key"))
}
async fn typed_input(
    read: &impl StorageAdapterRead,
    space: StorageSpace,
    key: &[u8],
    bytes: Bytes,
) -> Result<ReadInput, LixError> {
    use crate::tracked_state::*;
    let id = || -> Result<String, LixError> { Ok(uuid::Uuid::from_bytes(fixed(key)?).to_string()) };
    let address = if space == TRACKED_STATE_TREE_CHUNK_SPACE {
        ReadInputAddress::Object(NativeObjectRef::TrackedStateTreeChunk(fixed(key)?))
    } else if space == SCOPED_RANGE_NODE_SPACE {
        ReadInputAddress::Object(NativeObjectRef::ScopedRangeNode(fixed(key)?))
    } else if space == MUTATION_DIRECTORY_NODE_SPACE {
        ReadInputAddress::Object(NativeObjectRef::MutationDirectoryNode(fixed(key)?))
    } else if space == TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE {
        ReadInputAddress::Metadata(NativeMetadataRef::CommitStateHeader(id()?))
    } else if space == TRACKED_STATE_CHANGE_LOCATOR_SPACE {
        ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(id()?))
    } else if space == crate::changelog::COMMIT_SPACE {
        ReadInputAddress::Metadata(NativeMetadataRef::CommitGraphRecord(id()?))
    } else if space == TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE {
        let commit = crate::changelog::CommitId::parse_lix(&id()?, "read input owner")?;
        let header = PointReadPlan::new(
            TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            &[StorageKey(Bytes::copy_from_slice(key))],
        )
        .materialize(read, Default::default())
        .await?
        .value
        .pop()
        .flatten();
        let Some(StorageProjectedValue::FullValue(header)) = header else {
            return Err(invalid("catalog input has no owner header"));
        };
        ReadInputAddress::Object(commit_state_catalog_address(commit, &header)?)
    } else if space == TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE {
        if key.len() != 20 && key.len() != 52 {
            return Err(invalid("invalid native part address"));
        }
        ReadInputAddress::Object(NativeObjectRef::CommitDeltaPart {
            commit_id: fixed(&key[..16])?,
            part_index: u32::from_be_bytes(fixed(&key[16..20])?),
            expected_digest: if key.len() == 52 {
                fixed(&key[20..])?
            } else {
                *blake3::hash(&bytes).as_bytes()
            },
            replacement: key.len() == 52,
        })
    } else {
        return Err(invalid("unsupported read dependency"));
    };
    address.validate(&bytes)?;
    Ok(ReadInput {
        address,
        bytes: bytes.to_vec(),
    })
}

// Compile the native recipe evaluator once across storage backends. This is a
// read-only dynamic boundary; the adapter still owns snapshot consistency.
trait RecipeReadSource: Send + Sync {
    fn get_many<'a>(
        &'a self,
        requests: &'a [StorageGetManyRequest<'a>],
    ) -> futures_util::future::BoxFuture<'a, Result<StorageGetManyResult, StorageError>>;
    fn begin_scan(
        &self,
        space: StorageSpace,
        range: StorageKeyRange,
        opts: StorageBeginScanOptions,
    ) -> futures_util::future::BoxFuture<'_, Result<StorageScanCursor<'_>, StorageError>>;
}
struct ReadBackend<R>(R);
impl<R: StorageAdapterRead> RecipeReadSource for ReadBackend<R> {
    fn get_many<'a>(
        &'a self,
        requests: &'a [StorageGetManyRequest<'a>],
    ) -> futures_util::future::BoxFuture<'a, Result<StorageGetManyResult, StorageError>> {
        Box::pin(StorageAdapterRead::get_many(&self.0, requests))
    }
    fn begin_scan(
        &self,
        space: StorageSpace,
        range: StorageKeyRange,
        opts: StorageBeginScanOptions,
    ) -> futures_util::future::BoxFuture<'_, Result<StorageScanCursor<'_>, StorageError>> {
        Box::pin(StorageAdapterRead::begin_scan(&self.0, space, range, opts))
    }
}
#[derive(Clone)]
struct RecipeRead(Arc<dyn RecipeReadSource>);
impl StorageAdapterRead for RecipeRead {
    async fn get_many(
        &self,
        requests: &[StorageGetManyRequest<'_>],
    ) -> Result<StorageGetManyResult, StorageError> {
        self.0.get_many(requests).await
    }
    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: StorageKeyRange,
        opts: StorageBeginScanOptions,
    ) -> Result<StorageScanCursor<'_>, StorageError> {
        self.0.begin_scan(space, range, opts).await
    }
}

pub(crate) async fn discover<R>(
    base: R,
    repository: &str,
    account: &str,
    lease_id: &str,
    request: &ReadFulfillmentRequest,
    hot: crate::hot_state::HotStateContext,
) -> Result<ReadFulfillmentResponse, LixError>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    discover_with_read(
        RecipeRead(Arc::new(ReadBackend(base))),
        repository,
        account,
        lease_id,
        request,
        hot,
    )
    .await
}
async fn discover_with_read(
    base: RecipeRead,
    repository: &str,
    account: &str,
    lease_id: &str,
    request: &ReadFulfillmentRequest,
    hot: crate::hot_state::HotStateContext,
) -> Result<ReadFulfillmentResponse, LixError> {
    request.validate(repository)?;
    let lease = crate::gc::require_native_baseline_lease(
        &base,
        lease_id,
        account,
        crate::telemetry::unix_time_ms(),
    )
    .await?;
    lease.validate_for_roots(
        account,
        &super::leased_descriptor::descriptor_roots(&request.descriptor)?,
    )?;
    let observations = Arc::new(Mutex::new(Observations::default()));
    let read = DependencyRead {
        base,
        observations: observations.clone(),
    };
    let blobs = Arc::new(BlobReadCapture::default());
    let mut required_chunks = Vec::new();
    for address in &request.required {
        if let ReadInputAddress::BlobChunk(hash) = address {
            let bytes = crate::binary_cas::load_verified_chunk(
                &read,
                crate::binary_cas::ChunkHash::from_bytes(*hash),
            )
            .await?
            .ok_or_else(|| invalid("authority lacks required chunk"))?;
            address.validate(&bytes)?;
            required_chunks.push(ReadInput {
                address: address.clone(),
                bytes,
            });
            continue;
        }
        let (space, key) = address.coordinate()?;
        // Change locators are logical metadata addresses. Directly authored
        // changes intentionally have no locator row at this key; use the same
        // canonical authority resolver as the native-metadata endpoint.
        let is_change_locator = matches!(
            address,
            ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(_))
        );
        let value =
            if let ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(id)) = address {
                let commit = crate::changelog::CommitId::parse(id)
                    .map_err(|_| invalid("native metadata ID must be a canonical UUID"))?;
                let change = crate::changelog::ChangeId::new(*commit.as_uuid());
                crate::tracked_state::load_canonical_change_locator(&read, change)
                    .await?
                    .map(|locator| {
                        StorageProjectedValue::FullValue(Bytes::from(
                            crate::tracked_state::encode_change_locator(locator),
                        ))
                    })
            } else {
                PointReadPlan::new(space, std::slice::from_ref(&key))
                    .materialize(&read, Default::default())
                    .await?
                    .value
                    .pop()
                    .flatten()
            };
        let Some(StorageProjectedValue::FullValue(bytes)) = value else {
            return Err(invalid("authority lacks required read input")
                .with_details(serde_json::json!({"address":address})));
        };
        if let ReadInputAddress::BlobManifest(hash) = address {
            crate::binary_cas::decode_binary_cas_manifest(&bytes)?;
            blobs.record(crate::binary_cas::BlobId::from_bytes(*hash), false, None)?;
        } else {
            address.validate(&bytes)?;
        }
        if is_change_locator {
            let observed = StorageProjectedValue::FullValue(bytes.clone());
            observations
                .lock()
                .map_err(|_| invalid("dependency observations poisoned"))?
                .observe(space, &key, &observed)
                .map_err(LixError::from)?;
        }
    }
    for branch in [
        &request.descriptor.selected_branch,
        &request.descriptor.global_branch,
    ] {
        for roots in [&branch.head, &branch.checkpoint] {
            let commit =
                crate::changelog::CommitId::parse_lix(&roots.commit_id, "fulfillment root")?;
            if super::partial_replica::commit_roots(&read, commit).await? != *roots {
                return Err(invalid("requested roots disagree with immutable authority"));
            }
            // An admitted checkpoint can have no selected rows and therefore
            // no returned-row owner to close its mutation inventory. Local
            // publication still reads that inventory, including an empty one.
            // Retain it with the root header rather than depending on an
            // incidental per-pointer metadata bundle during read warmup.
            let _ = crate::tracked_state::load_commit_state_manifest(&read, commit).await?;
        }
    }
    let state = PartialReplicaState::from_leased(
        "https://read-fulfillment.invalid".into(),
        account.into(),
        request.epoch_id.clone(),
        super::LeasedPartialReplicaDescriptor {
            descriptor: request.descriptor.clone(),
            lease,
        },
    )?
    .for_read_fulfillment();
    if super::partial_write_frontier::next_missing_candidate_write_frontier(&read, &state)
        .await?
        .is_some()
    {
        return Err(invalid("authority baseline graph is incomplete"));
    }
    let mut staged = StorageWriteSet::new();
    let branches = if request.descriptor.selected_branch.branch_id
        == request.descriptor.global_branch.branch_id
    {
        vec![&request.descriptor.selected_branch]
    } else {
        vec![
            &request.descriptor.selected_branch,
            &request.descriptor.global_branch,
        ]
    };
    for branch in branches {
        crate::branch::stage_branch_head_control(
            &mut staged,
            &branch.branch_id,
            super::partial_bootstrap::partial_branch_control(&state, branch)?,
        )?;
        crate::hot_state::TrackedHeadContext::new()
            .writer(&read, &mut staged)
            .stage_root_current_base(
                &branch.branch_id,
                state.serving_generation(&branch.branch_id)?,
                crate::changelog::CommitId::parse_lix(&branch.head.commit_id, "fulfillment head")?,
            );
    }
    let hot = hot.with_partial_scope_policy(
        &request.descriptor.selected_branch.branch_id,
        &request.descriptor.global_branch.branch_id,
    );
    let scoped = super::partial_candidate_prepare::CandidateRead {
        base: read.clone(),
        staged: Arc::new(staged),
    };
    let interests = crate::hot_state::ReadInterestSnapshot {
        revision: 0,
        serialized_bytes: 0,
        interests: request.interests.iter().cloned().map(Arc::new).collect(),
    };
    let logical_inputs = super::read_interest_prepare::prepare_native_read_interests_authority(
        scoped,
        &request.descriptor,
        &interests,
        account,
        hot,
        blobs.clone(),
    )
    .await?;
    // Some immutable metadata addresses are canonical derivations rather than
    // physical rows (notably direct ChangeLocator values). Merge those typed
    // inputs into the same observation map as physical reads so one operation
    // closes its complete native dependency graph.
    let mut explicit_selected_inputs = Vec::new();
    for input in logical_inputs {
        input.address.validate(&input.bytes)?;
        if matches!(&input.address, ReadInputAddress::ChangeRecord { .. }) {
            explicit_selected_inputs.push(input);
            continue;
        }
        let (space, key) = input.address.coordinate()?;
        let observed = StorageProjectedValue::FullValue(Bytes::from(input.bytes));
        observations
            .lock()
            .map_err(|_| invalid("dependency observations poisoned"))?
            .observe(space, &key, &observed)
            .map_err(LixError::from)?;
    }
    let mut typed = BTreeMap::new();
    loop {
        let pending = observations
            .lock()
            .map_err(|_| invalid("dependency observations poisoned"))?
            .values
            .iter()
            .filter(|(coordinate, _)| !typed.contains_key(*coordinate))
            .map(|(coordinate, bytes)| (coordinate.clone(), bytes.clone()))
            .collect::<Vec<_>>();
        if pending.is_empty() {
            break;
        }
        for ((space, key), bytes) in pending {
            let input = typed_input(&read, space, &key.0, bytes).await?;
            typed.insert((space, key), input);
        }
    }
    let mut inputs = typed.into_values().collect::<Vec<_>>();
    for input in explicit_selected_inputs {
        let coordinate = input.address.coordinate()?;
        if let Some(existing) = inputs
            .iter()
            .find(|existing| existing.address.coordinate().ok().as_ref() == Some(&coordinate))
        {
            if existing.bytes != input.bytes {
                return Err(invalid(
                    "selected change payload conflicts with observed input",
                ));
            }
            continue;
        }
        inputs.push(input);
    }
    for input in required_chunks {
        if !inputs
            .iter()
            .any(|existing| existing.address == input.address)
        {
            inputs.push(input);
        }
    }
    let native_bytes = inputs.iter().map(|input| input.bytes.len()).sum::<usize>();
    if native_bytes > MAX_PAYLOAD_BYTES || inputs.len() > MAX_RECORDS {
        return Err(invalid("native discovery payload limit exceeded"));
    }
    inputs.extend(
        export_blob_inputs(
            &read,
            &blobs,
            MAX_PAYLOAD_BYTES - native_bytes,
            MAX_RECORDS - inputs.len(),
            &inputs,
        )
        .await?,
    );

    let mut profile = observations
        .lock()
        .map_err(|_| invalid("read discovery accounting poisoned"))?
        .profile
        .clone();
    profile.payload_bytes = inputs.iter().map(|input| input.bytes.len()).sum();
    let closure_digest = input_digest(request, &inputs)?;
    let mut response = ReadFulfillmentResponse {
        lix_id: repository.into(),
        epoch_id: request.epoch_id.clone(),
        request_digest: request.digest()?,
        inputs,
        profile,
        closure_digest: closure_digest.clone(),
        continuation: None,
    };
    validate_complete(request, &response)?;
    let start = if let Some(cursor) = &request.continuation {
        if cursor.closure_digest != closure_digest
            || cursor.next_input == 0
            || cursor.next_input >= response.inputs.len()
        {
            return Err(invalid("invalid or changed read continuation"));
        }
        cursor.next_input
    } else {
        0
    };
    let mut end = start;
    let mut bytes = 0usize;
    while end < response.inputs.len() {
        let next = response.inputs[end].bytes.len();
        if end > start && bytes.saturating_add(next) > PAGE_PAYLOAD_BYTES {
            break;
        }
        bytes += next;
        end += 1;
    }
    response.continuation = (end < response.inputs.len()).then_some(ReadContinuation {
        next_input: end,
        closure_digest,
    });
    response.inputs = response
        .inputs
        .into_iter()
        .skip(start)
        .take(end - start)
        .collect();
    validate_response(request, &response)?;
    Ok(response)
}

fn input_digest(
    request: &ReadFulfillmentRequest,
    inputs: &[ReadInput],
) -> Result<String, LixError> {
    let mut digest = blake3::Hasher::new();
    digest.update(request.digest()?.as_bytes());
    for input in inputs {
        let address =
            serde_json::to_vec(&input.address).map_err(|_| invalid("invalid input address"))?;
        digest.update(&(address.len() as u64).to_be_bytes());
        digest.update(&address);
        digest.update(&(input.bytes.len() as u64).to_be_bytes());
        digest.update(&input.bytes);
    }
    Ok(digest.finalize().to_hex().to_string())
}
pub(crate) fn validate_response(
    request: &ReadFulfillmentRequest,
    response: &ReadFulfillmentResponse,
) -> Result<(), LixError> {
    request.validate(&response.lix_id)?;
    if response.epoch_id != request.epoch_id
        || response.request_digest != request.digest()?
        || response.inputs.len() > MAX_RECORDS
        || !valid_digest(&response.closure_digest)
    {
        return Err(invalid(
            "read fulfillment belongs to another request or exceeds its bound",
        ));
    }
    let mut bytes = 0usize;
    let mut seen = BTreeSet::new();
    for input in &response.inputs {
        bytes = bytes
            .checked_add(input.bytes.len())
            .ok_or_else(|| invalid("read fulfillment size overflow"))?;
        if input.bytes.len() > MAX_INPUT_BYTES
            || bytes > MAX_PAYLOAD_BYTES
            || !seen.insert(input.address.coordinate()?)
        {
            return Err(invalid(
                "read fulfillment exceeds bound or repeats an input",
            ));
        }
        input.address.validate(&input.bytes)?;
    }
    if response.inputs.len() > 1 && bytes > PAGE_PAYLOAD_BYTES {
        return Err(invalid("read page exceeds byte bound"));
    }
    let start = request
        .continuation
        .as_ref()
        .map_or(0, |cursor| cursor.next_input);
    if request
        .continuation
        .as_ref()
        .is_some_and(|cursor| cursor.closure_digest != response.closure_digest)
    {
        return Err(invalid("read continuation changed its closure"));
    }
    if let Some(next) = &response.continuation {
        if response.inputs.is_empty()
            || next.next_input != start + response.inputs.len()
            || next.next_input >= MAX_RECORDS
            || next.closure_digest != response.closure_digest
        {
            return Err(invalid("read continuation did not advance exactly"));
        }
    }
    Ok(())
}
/// A scan can select a canonical row payload when its identity satisfies the
/// recipe's exact filters. An empty `row_pks` list is the native wildcard
/// used by ordinary SQL scans; it still constrains the identity through the
/// schema, branch, file, and row-bound filters below. The server may return a
/// typed payload only for an identity actually selected while replaying that
/// scan, not for arbitrary mutable CHANGE_SPACE values.
fn scan_selects_change_identity(
    scan: &crate::hot_state::HotStateScanRequest,
    domain: InterestDomain,
    branch_id: &str,
    schema_key: &str,
    file_id: Option<&str>,
    row_pk: &crate::row_pk::RowPk,
) -> bool {
    let filter = &scan.filter;
    if scan.limit.is_some()
        || domain == InterestDomain::Untracked
        || filter.untracked == Some(true)
        || filter.rows != crate::hot_state::HotStateRowFilter::All
        || !filter
            .schema_keys
            .iter()
            .any(|candidate| candidate == schema_key)
        || !filter
            .branch_ids
            .iter()
            .any(|candidate| candidate == branch_id)
        || (!filter.row_pks.is_empty()
            && !filter.row_pks.iter().any(|candidate| candidate == row_pk))
        || !crate::tracked_state::row_pk_satisfies_bounds(
            row_pk,
            filter.row_pk_lower.as_ref(),
            filter.row_pk_upper.as_ref(),
        )
        || (!filter.file_ids.is_empty()
            && !filter.file_ids.iter().any(|candidate| match candidate {
                crate::NullableKeyFilter::Any => true,
                crate::NullableKeyFilter::Null => file_id.is_none(),
                crate::NullableKeyFilter::Value(expected) => file_id == Some(expected.as_str()),
            }))
    {
        return false;
    }

    // These predicates need row contents, indexed values, or tombstone state
    // that the wire identity alone cannot prove. Reject them here instead of
    // trying to approximate their selection semantics.
    filter.global.is_none()
        && filter.constraints.is_empty()
        && filter.declared_column_eq.is_none()
        && filter.declared_column_range.is_none()
        && !filter.include_tombstones
}

fn scan_recipe_selects_change_identity(
    interest: &LogicalReadInterest,
    branch_id: &str,
    schema_key: &str,
    file_id: Option<&str>,
    row_pk: &crate::row_pk::RowPk,
) -> bool {
    match interest {
        LogicalReadInterest::Exact {
            rows, untracked, ..
        } => {
            *untracked != Some(true)
                && rows.iter().any(|row| {
                    row.branch_id == branch_id
                        && row.schema_key == schema_key
                        && row.file_id.as_deref() == file_id
                        && row.row_pk == *row_pk
                })
        }
        LogicalReadInterest::Scan { request, domain } => {
            scan_selects_change_identity(request, *domain, branch_id, schema_key, file_id, row_pk)
        }
        LogicalReadInterest::FilesystemMetadata {
            directory,
            branch_ids,
            file_ids,
            directory_ids,
            ..
        } => {
            if file_ids.as_ref().is_some_and(Vec::is_empty)
                || directory_ids.as_ref().is_some_and(Vec::is_empty)
            {
                return false;
            }
            // Metadata replay constructs this path index before applying
            // output predicates. Its ancestor descriptors are dependencies
            // even when they are absent from the final SQL result.
            let scope = if *directory {
                crate::filesystem::FilesystemPathIndexScope::All
            } else {
                file_ids.clone().map_or(
                    crate::filesystem::FilesystemPathIndexScope::All,
                    crate::filesystem::FilesystemPathIndexScope::FileIds,
                )
            };
            scan_recipe_selects_change_identity(
                &LogicalReadInterest::FilesystemPaths {
                    scope,
                    branch_ids: branch_ids.clone(),
                    include_blob_refs: false,
                    cache_small_blob_data: false,
                },
                branch_id,
                schema_key,
                file_id,
                row_pk,
            )
        }
        LogicalReadInterest::FilesystemPaths {
            scope,
            branch_ids,
            include_blob_refs,
            cache_small_blob_data,
        } => {
            // Use the same native schema/branch recipe as path-index replay.
            // File-scoped consumers still read directory descriptors to resolve
            // ancestors, but cannot select another file's descriptor or blob.
            let scan = crate::filesystem::FilesystemPathIndexRequest::new(branch_ids.clone())
                .with_scope(scope.clone())
                .with_blob_refs(*include_blob_refs || *cache_small_blob_data)
                .hot_state_request();
            if !scan_selects_change_identity(
                &scan,
                InterestDomain::Combined,
                branch_id,
                schema_key,
                file_id,
                row_pk,
            ) {
                return false;
            }
            match scope {
                crate::filesystem::FilesystemPathIndexScope::FileIds(ids)
                    if schema_key != "lix_directory_descriptor" =>
                {
                    ids.iter().any(|id| {
                        crate::row_pk::RowPk::uuid_from_canonical(id)
                            .is_ok_and(|expected| expected == *row_pk)
                            && file_id.is_none_or(|file| file == id)
                    })
                }
                _ => true,
            }
        }
        _ => false,
    }
}

fn payload_schema_kind(schema: &str) -> &'static str {
    match schema {
        "lix_file_descriptor" => "file_descriptor",
        "lix_directory_descriptor" => "directory_descriptor",
        "lix_binary_blob_ref" => "binary_blob_ref",
        "lix_plugin_registry" => "plugin_registry",
        "lix_account" => "account",
        "lix_schema" => "schema",
        "lix_key_value" => "key_value",
        _ => "other",
    }
}

fn payload_recipe_mask(interests: &[LogicalReadInterest]) -> u16 {
    interests.iter().fold(0, |mask, interest| {
        mask | match interest {
            LogicalReadInterest::Exact { .. } => 1,
            LogicalReadInterest::Scan { .. } => 2,
            LogicalReadInterest::FileContent { .. } => 4,
            LogicalReadInterest::FilesystemMetadata { .. } => 8,
            LogicalReadInterest::FilesystemPaths { .. } => 16,
            LogicalReadInterest::CollectionGeneration { .. } => 32,
            LogicalReadInterest::PackedIdentityMembership { .. } => 64,
            LogicalReadInterest::Diff { .. } => 128,
        }
    })
}

fn validate_complete(
    request: &ReadFulfillmentRequest,
    response: &ReadFulfillmentResponse,
) -> Result<(), LixError> {
    request.validate(&response.lix_id)?;
    if response.epoch_id != request.epoch_id
        || response.request_digest != request.digest()?
        || response.continuation.is_some()
        || response.inputs.len() > MAX_RECORDS
        || input_digest(request, &response.inputs)? != response.closure_digest
    {
        return Err(invalid(
            "read fulfillment closure is incomplete or mismatched",
        ));
    }
    let mut bytes = 0usize;
    let mut seen = BTreeSet::new();
    for input in &response.inputs {
        bytes = bytes
            .checked_add(input.bytes.len())
            .ok_or_else(|| invalid("closure size overflow"))?;
        if bytes > MAX_PAYLOAD_BYTES
            || input.bytes.len() > MAX_INPUT_BYTES
            || !seen.insert(input.address.coordinate()?)
        {
            return Err(invalid("invalid read closure bounds or repeated input"));
        }
        input.address.validate(&input.bytes)?;
    }
    for required in &request.required {
        if !response
            .inputs
            .iter()
            .any(|input| &input.address == required)
        {
            return Err(invalid("read fulfillment omitted a required input"));
        }
    }
    for input in &response.inputs {
        let ReadInputAddress::ChangeRecord {
            change_id,
            source_commit_id,
            branch_id,
            schema_key,
            file_id,
            row_pk,
            ..
        } = &input.address
        else {
            continue;
        };
        if branch_id != &request.descriptor.selected_branch.branch_id
            && branch_id != &request.descriptor.global_branch.branch_id
        {
            return Err(invalid(
                "canonical change payload is outside the descriptor branch scope",
            ));
        }
        let selected_by_recipe = request.interests.iter().any(|interest| {
            scan_recipe_selects_change_identity(
                interest,
                branch_id,
                schema_key,
                file_id.as_deref(),
                row_pk,
            )
        });
        if !selected_by_recipe {
            return Err(
                invalid("canonical change payload has no matching row recipe").with_details(
                    serde_json::json!({
                        "payloadFailureReason": "selected_change_payload_recipe_mismatch",
                        "payloadPhase": "read_fulfillment_validation",
                        "payloadSchemaKind": payload_schema_kind(schema_key),
                        "payloadRecipeMask": payload_recipe_mask(&request.interests),
                        "payloadRecipeCount": request.interests.len(),
                        "payloadRequiredInputCount": request.required.len(),
                        "branchId": branch_id,
                        "changeId": change_id,
                        "sourceCommitId": source_commit_id
                    }),
                ),
            );
        }
        let locator_address =
            ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(change_id.clone()));
        let locator = response
            .inputs
            .iter()
            .find(|candidate| candidate.address == locator_address)
            .ok_or_else(|| invalid("canonical change payload has no selected source locator"))?;
        let parsed_change = canonical_change_id(change_id)?;
        let parsed_source = canonical_commit_id(source_commit_id)?;
        let source = crate::tracked_state::decode_change_locator(parsed_change, &locator.bytes)?;
        if source.commit_id != parsed_source {
            return Err(invalid(
                "canonical change payload source disagrees with its selected locator",
            ));
        }
    }
    Ok(())
}

pub(super) async fn fetch<C: super::http::RawHttpClient>(
    transport: &super::http::HttpSyncTransport<C>,
    request: &ReadFulfillmentRequest,
) -> Result<ReadFulfillmentResponse, LixError> {
    let mut page_request = request.clone();
    let mut response = transport.fulfill_read(&page_request).await?;
    let mut payload_bytes = response
        .inputs
        .iter()
        .map(|input| input.bytes.len())
        .sum::<usize>();
    for _ in 0..MAX_PAGES {
        let Some(next) = response.continuation.take() else {
            validate_complete(request, &response)?;
            return Ok(response);
        };
        page_request.continuation = Some(next);
        let page = transport.fulfill_read(&page_request).await?;
        payload_bytes = payload_bytes.saturating_add(
            page.inputs
                .iter()
                .map(|input| input.bytes.len())
                .sum::<usize>(),
        );
        if payload_bytes > MAX_PAYLOAD_BYTES
            || response.inputs.len() + page.inputs.len() > MAX_RECORDS
        {
            return Err(invalid("read assembly exceeds bound"));
        }
        response.inputs.extend(page.inputs);
        response.continuation = page.continuation;
    }
    Err(invalid("read continuation count exceeds bound"))
}

pub(super) async fn install<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    request: &ReadFulfillmentRequest,
    response: &ReadFulfillmentResponse,
) -> Result<super::runtime::HydratedInputs, LixError> {
    validate_complete(request, response)?;
    if request.epoch_id != state.epoch_id() || request.descriptor != *state.descriptor() {
        return Err(invalid("read fulfillment basis changed"));
    }
    let payload_change_ids = response
        .inputs
        .iter()
        .filter_map(|input| match &input.address {
            ReadInputAddress::ChangeRecord { change_id, .. } => Some(change_id.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let paired_change_locator_ids = response
        .inputs
        .iter()
        .filter_map(|input| match &input.address {
            ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(change_id))
                if payload_change_ids.contains(change_id) =>
            {
                Some(change_id.clone())
            }
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let mut stale_mutable_observations = Vec::<(StorageSpace, StorageKey, Bytes)>::new();
    loop {
        let read = storage.begin_read(Default::default()).await?;
        let Some((actual, receipt)) =
            super::partial_state::load_partial_replica_state(&read).await?
        else {
            return Err(invalid("partial admission is missing"));
        };
        if actual != *state {
            return Err(LixError::new(
                super::runtime::PARTIAL_ADMISSION_CHANGED_CODE,
                "read fulfillment admission changed",
            ));
        }
        let mut writes = storage.new_write_set();
        let mut preconditions = vec![StoragePrecondition::KeyValueEquals {
            space: super::PARTIAL_REPLICA_STATE_SPACE,
            key: super::partial_state::partial_replica_state_key(),
            expected: receipt,
        }];
        let mut hydrated = super::runtime::HydratedInputs::default();
        let mut local_owner_authorities = BTreeMap::new();
        // Detect a locally selected owner representation before processing
        // bundled catalog/part inputs.  The header is immutable under normal
        // publication, but a local upload/adoption can leave a valid owner
        // bundle whose bytes differ from the authority's optional closure.
        // Load and validate the paired inventory here so preserving it cannot
        // turn a corrupt local header into a permissive cache hit.
        for input in response.inputs.iter().filter(|input| {
            matches!(
                input.address,
                ReadInputAddress::Metadata(NativeMetadataRef::CommitStateHeader(_))
            ) && !request.required.contains(&input.address)
        }) {
            let Some(owner) = owner_commit_id(&input.address) else {
                continue;
            };
            let (space, key) = input.address.coordinate()?;
            let value = PointReadPlan::new(space, std::slice::from_ref(&key))
                .materialize(&read, Default::default())
                .await?
                .value
                .pop()
                .flatten();
            let Some(StorageProjectedValue::FullValue(local)) = value else {
                continue;
            };
            if local.as_ref() == input.bytes.as_slice() {
                continue;
            }
            input.address.validate(&local)?;
            let manifest = crate::tracked_state::load_commit_state_manifest(&read, owner)
                .await?
                .ok_or_else(|| invalid("local owner header has no mutation authority"))?;
            let inventory_key = StorageKey(Bytes::copy_from_slice(owner.as_uuid().as_bytes()));
            let inventory = PointReadPlan::new(
                crate::tracked_state::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
                std::slice::from_ref(&inventory_key),
            )
            .materialize(&read, Default::default())
            .await?
            .value
            .pop()
            .flatten()
            .and_then(|value| match value {
                StorageProjectedValue::FullValue(bytes) => Some(bytes),
                StorageProjectedValue::KeyOnly => None,
            })
            .ok_or_else(|| invalid("local owner header has no mutation catalog"))?;
            // Fence both halves of the retained owner bundle, even when a
            // particular response page does not name the catalog itself.
            // This prevents a concurrent retirement/replacement from making
            // the preserved header and catalog disagree after publication.
            preconditions.push(StoragePrecondition::KeyValueEquals {
                space,
                key: key.clone(),
                expected: local,
            });
            preconditions.push(StoragePrecondition::KeyValueEquals {
                space: crate::tracked_state::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
                key: inventory_key,
                expected: inventory.clone(),
            });
            local_owner_authorities.insert(
                owner,
                LocalOwnerAuthority {
                    manifest,
                    catalog_digest: *blake3::hash(&inventory).as_bytes(),
                },
            );
        }
        let mut manifests = Vec::new();
        let mut chunks = Vec::new();
        for input in &response.inputs {
            if let Some(owner) = owner_commit_id(&input.address)
                && let Some(authority) = local_owner_authorities.get(&owner)
            {
                if request.required.contains(&input.address)
                    && !owner_input_matches_local_authority(authority, &input.address)
                {
                    return Err(invalid("required read input conflicts with retained owner"));
                }
                if !request.required.contains(&input.address)
                    && !owner_input_matches_local_authority(authority, &input.address)
                {
                    if matches!(
                        input.address,
                        ReadInputAddress::Object(NativeObjectRef::CommitDeltaPart {
                            replacement: true,
                            ..
                        })
                    ) {
                        // Replacement parts include their digest in the
                        // physical key. They are independently content
                        // addressed and are not part of the retained direct
                        // owner representation; omit them from this receipt.
                        continue;
                    }
                    // The authority's optional input belongs to a different
                    // owner representation.  Do not install just that row and
                    // create a hybrid header/catalog/part bundle.
                    let (space, key) = input.address.coordinate()?;
                    let value = PointReadPlan::new(space, std::slice::from_ref(&key))
                        .materialize(&read, Default::default())
                        .await?
                        .value
                        .pop()
                        .flatten();
                    if let Some(StorageProjectedValue::FullValue(bytes)) = value {
                        validate_local_owner_input(authority, &input.address, &bytes)?;
                        if !matches!(
                            input.address,
                            ReadInputAddress::Metadata(NativeMetadataRef::CommitStateHeader(_))
                        ) {
                            preconditions.push(StoragePrecondition::KeyValueEquals {
                                space,
                                key: key.clone(),
                                expected: bytes,
                            });
                        }
                        append_receipt_input(&mut hydrated, &input.address, (space, key))?;
                    }
                    continue;
                }
            }
            let (space, key) = input.address.coordinate()?;
            if let ReadInputAddress::BlobManifest(hash) = input.address {
                let blob = crate::binary_cas::BlobId::from_bytes(hash);
                if crate::binary_cas::load_metadata_many(&read, &[blob])
                    .await?
                    .into_vec()[0]
                    .is_none()
                {
                    let wire: super::SyncBlobManifest = serde_json::from_slice(&input.bytes)
                        .map_err(|_| invalid("invalid canonical blob input"))?;
                    let manifest = super::blob::decode_manifest(&wire)?;
                    super::partial_blob::check_manifest_chunk_presence(&read, &manifest).await?;
                    manifests.push(manifest);
                    preconditions.push(StoragePrecondition::KeyAbsent {
                        space,
                        key: key.clone(),
                    });
                }
                append_receipt_input(&mut hydrated, &input.address, (space, key))?;
                continue;
            }
            if let ReadInputAddress::BlobChunk(hash) = input.address {
                let chunk = crate::binary_cas::ChunkHash::from_bytes(hash);
                let existing = crate::binary_cas::load_verified_chunk(&read, chunk).await?;
                let presence = crate::binary_cas::chunk_presence_many(&read, &[chunk]).await?[0];
                if existing.is_some() != presence {
                    return Err(invalid("resident chunk presence is inconsistent"));
                }
                match existing {
                    Some(bytes) if bytes == input.bytes => {
                        append_receipt_input(&mut hydrated, &input.address, (space, key))?;
                    }
                    Some(_) => return Err(invalid("blob input conflicts with resident chunk")),
                    None => {
                        preconditions.push(StoragePrecondition::KeyAbsent {
                            space,
                            key: key.clone(),
                        });
                        chunks.push(crate::binary_cas::CanonicalBlobChunk {
                            receipt: crate::binary_cas::BlobChunkReceipt {
                                hash: chunk,
                                size_bytes: input.bytes.len() as u64,
                            },
                            bytes: input.bytes.clone(),
                        });
                        append_receipt_input(&mut hydrated, &input.address, (space, key))?;
                    }
                }
                continue;
            }
            let value = PointReadPlan::new(space, std::slice::from_ref(&key))
                .materialize(&read, Default::default())
                .await?
                .value
                .pop()
                .flatten();
            match value {
                None => {
                    preconditions.push(StoragePrecondition::KeyAbsent {
                        space,
                        key: key.clone(),
                    });
                    writes.put(
                        space,
                        key.clone(),
                        StorageValue {
                            bytes: Bytes::copy_from_slice(&input.bytes),
                        },
                    );
                    append_receipt_input(&mut hydrated, &input.address, (space, key))?;
                }
                Some(StorageProjectedValue::FullValue(bytes)) if bytes.as_ref() == input.bytes => {
                    preconditions.push(StoragePrecondition::KeyValueEquals {
                        space,
                        key: key.clone(),
                        expected: bytes.clone(),
                    });
                    append_receipt_input(&mut hydrated, &input.address, (space, key))?;
                }
                Some(StorageProjectedValue::FullValue(bytes))
                    if matches!(&input.address, ReadInputAddress::ChangeRecord { .. }) =>
                {
                    if request.required.contains(&input.address) {
                        return Err(invalid(
                            "canonical selected change payload cannot replace a required input",
                        ));
                    }
                    // CHANGE_SPACE is a rebuildable projection. This payload
                    // has already been bound to an exact row recipe and its
                    // canonical physical source by `validate_complete`; heal
                    // a stale decodable resident value with an exact compare
                    // so a concurrent local write is never overwritten.
                    input.address.validate(&input.bytes)?;
                    if let Some((_, _, initially_observed)) = stale_mutable_observations
                        .iter()
                        .find(|(observed_space, observed_key, _)| {
                            observed_space == &space && observed_key == &key
                        })
                    {
                        if initially_observed != &bytes {
                            return Err(invalid(
                                "resident change payload changed during canonical repair",
                            ));
                        }
                    } else {
                        stale_mutable_observations.push((space, key.clone(), bytes.clone()));
                    }
                    preconditions.push(StoragePrecondition::KeyValueEquals {
                        space,
                        key: key.clone(),
                        expected: bytes,
                    });
                    writes.put(
                        space,
                        key.clone(),
                        StorageValue {
                            bytes: Bytes::copy_from_slice(&input.bytes),
                        },
                    );
                    append_receipt_input(&mut hydrated, &input.address, (space, key))?;
                }
                Some(StorageProjectedValue::FullValue(bytes))
                    if matches!(
                        &input.address,
                        ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(change_id))
                            if paired_change_locator_ids.contains(change_id)
                    ) =>
                {
                    // A locator paired with this exact selected-row payload
                    // is part of the validated canonical closure. Heal a
                    // stale but decodable projection with CAS even when the
                    // locator was explicitly required: its paired payload and
                    // source were already checked against the exact recipe.
                    input.address.validate(&input.bytes)?;
                    if let Some((_, _, initially_observed)) = stale_mutable_observations
                        .iter()
                        .find(|(observed_space, observed_key, _)| {
                            observed_space == &space && observed_key == &key
                        })
                    {
                        if initially_observed != &bytes {
                            return Err(invalid(
                                "resident mutable payload changed during canonical repair",
                            ));
                        }
                    } else {
                        stale_mutable_observations.push((space, key.clone(), bytes.clone()));
                    }
                    preconditions.push(StoragePrecondition::KeyValueEquals {
                        space,
                        key: key.clone(),
                        expected: bytes,
                    });
                    writes.put(
                        space,
                        key.clone(),
                        StorageValue {
                            bytes: Bytes::copy_from_slice(&input.bytes),
                        },
                    );
                    append_receipt_input(&mut hydrated, &input.address, (space, key))?;
                }
                Some(StorageProjectedValue::FullValue(bytes))
                    if preserves_local_mutable_native_overlay(&input.address)
                        && !request.required.contains(&input.address) =>
                {
                    // These UUID-keyed metadata planes are mutable because a
                    // local pending commit/change may already own this key.
                    // The closure warms absent immutable inputs; it must not
                    // overwrite that local overlay when the authority's
                    // optional observation has a different value.
                    input
                        .address
                        .validate_existing_mutable_value(&bytes, &input.bytes)?;
                    preconditions.push(StoragePrecondition::KeyValueEquals {
                        space,
                        key: key.clone(),
                        expected: bytes.clone(),
                    });
                    if !matches!(&input.address, ReadInputAddress::ChangeRecord { .. }) {
                        append_receipt_input(&mut hydrated, &input.address, (space, key))?;
                    }
                    continue;
                }
                Some(_) => {
                    return Err(invalid("read fulfillment conflicts with resident input")
                        .with_details(serde_json::json!({
                            "address": &input.address,
                            "space": space.name,
                            "key": key.0.to_vec(),
                        })));
                }
            }
        }
        let missing_chunks = crate::binary_cas::stage_deferred_canonical_manifests_with_chunks(
            &read,
            &mut writes,
            &manifests,
            &chunks,
        )
        .await?;
        // A demand marker is a mutable hint. Fence every absent payload row
        // that caused one against a concurrent chunk installer; otherwise a
        // chunk commit between the read snapshot and this commit could be
        // followed by a stale demand put from this transaction.
        for hash in missing_chunks {
            preconditions.push(StoragePrecondition::KeyAbsent {
                space: crate::binary_cas::BINARY_CAS_CHUNK_SPACE,
                key: StorageKey(Bytes::copy_from_slice(hash.as_bytes())),
            });
        }
        crate::binary_cas::stage_transfer_publication_fence(&read, &mut writes, &mut preconditions)
            .await?;
        drop(read);
        match storage
            .commit_partial_replica_write_set(
                super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
        {
            Err(StorageWriteSetError::Storage(StorageError::PreconditionFailed(_))) => continue,
            result => return result.map(|_| hydrated).map_err(Into::into),
        }
    }
}

/// Logical blob requests are captured before physical delta/base traversal.
/// This preserves range selection even when the authority's physical layout
/// differs from the canonical layout installed by a partial replica.
#[derive(Default)]
pub(crate) struct BlobReadCapture(Mutex<BTreeMap<crate::binary_cas::BlobId, BlobSelection>>);
#[derive(Clone, Default)]
struct BlobSelection {
    full: bool,
    ranges: Vec<std::ops::Range<u64>>,
}
impl BlobReadCapture {
    pub(crate) fn wrap(
        self: &Arc<Self>,
        reader: impl crate::binary_cas::BlobDataReader + 'static,
    ) -> Arc<dyn crate::binary_cas::BlobDataReader> {
        Arc::new(RecordingBlobReader {
            inner: Arc::new(reader),
            capture: self.clone(),
        })
    }
    fn record(
        &self,
        blob: crate::binary_cas::BlobId,
        full: bool,
        range: Option<std::ops::Range<u64>>,
    ) -> Result<(), LixError> {
        let mut requests = self
            .0
            .lock()
            .map_err(|_| invalid("blob read capture poisoned"))?;
        if requests.len() >= 4096 && !requests.contains_key(&blob) {
            return Err(invalid("blob discovery count limit exceeded"));
        }
        let selection = requests.entry(blob).or_default();
        selection.full |= full;
        if selection.full {
            selection.ranges.clear();
        } else if let Some(range) = range
            && !selection.ranges.contains(&range)
        {
            if selection.ranges.len() >= 4096 {
                return Err(invalid("blob range count limit exceeded"));
            }
            selection.ranges.push(range);
        }
        Ok(())
    }
}
struct RecordingBlobReader {
    inner: Arc<dyn crate::binary_cas::BlobDataReader>,
    capture: Arc<BlobReadCapture>,
}
#[async_trait::async_trait]
impl crate::binary_cas::BlobDataReader for RecordingBlobReader {
    fn requires_referenced_content_preparation(&self) -> bool {
        self.inner.requires_referenced_content_preparation()
    }
    async fn require_referenced_manifests(
        &self,
        hashes: &[crate::binary_cas::BlobId],
    ) -> Result<(), LixError> {
        for hash in hashes {
            self.capture.record(*hash, false, None)?;
        }
        self.inner.require_referenced_manifests(hashes).await
    }
    async fn require_referenced_content(
        &self,
        hashes: &[crate::binary_cas::BlobId],
    ) -> Result<(), LixError> {
        for hash in hashes {
            self.capture.record(*hash, true, None)?;
        }
        self.inner.require_referenced_content(hashes).await
    }
    async fn load_bytes_many(
        &self,
        hashes: &[crate::binary_cas::BlobId],
    ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
        for hash in hashes {
            self.capture.record(*hash, true, None)?;
        }
        self.inner.load_bytes_many(hashes).await
    }
    async fn load_ranges_many(
        &self,
        requests: &[(crate::binary_cas::BlobId, std::ops::Range<u64>)],
    ) -> Result<crate::binary_cas::BlobRangeBytesBatch, LixError> {
        for (hash, range) in requests {
            self.capture.record(*hash, false, Some(range.clone()))?;
        }
        self.inner.load_ranges_many(requests).await
    }
}

async fn export_blob_inputs(
    read: &impl StorageAdapterRead,
    capture: &BlobReadCapture,
    remaining_bytes: usize,
    remaining_records: usize,
    existing: &[ReadInput],
) -> Result<Vec<ReadInput>, LixError> {
    use crate::binary_cas::*;
    let selections = capture
        .0
        .lock()
        .map_err(|_| invalid("blob read capture poisoned"))?
        .clone();
    let mut inputs = Vec::new();
    let mut chunks = existing
        .iter()
        .filter_map(|input| {
            if let ReadInputAddress::BlobChunk(hash) = input.address {
                Some(ChunkHash::from_bytes(hash))
            } else {
                None
            }
        })
        .collect::<BTreeSet<_>>();
    let mut payload_bytes = 0usize;
    for (blob, selection) in selections {
        let metadata = load_metadata_many(read, &[blob])
            .await?
            .into_vec()
            .pop()
            .flatten()
            .ok_or_else(|| invalid("authority lacks selected blob"))?;
        let manifest = load_streaming_canonical_manifest(read, &metadata).await?;
        let wire = super::SyncBlobManifest {
            blob_id: blob.to_hex(),
            size_bytes: manifest.size_bytes,
            chunks: manifest
                .chunks
                .iter()
                .map(|chunk| super::SyncBlobChunk {
                    chunk_id: chunk.hash.to_hex(),
                    size_bytes: chunk.size_bytes,
                })
                .collect(),
            inline_bytes_base64: None,
        };
        let bytes = serde_json::to_vec(&wire).map_err(|_| invalid("invalid canonical manifest"))?;
        payload_bytes = payload_bytes.saturating_add(bytes.len());
        if payload_bytes > remaining_bytes || inputs.len() >= remaining_records {
            return Err(invalid("blob discovery payload limit exceeded"));
        }
        inputs.push(ReadInput {
            address: ReadInputAddress::BlobManifest(*blob.as_bytes()),
            bytes,
        });
        let mut offset = 0u64;
        let mut selected = BTreeSet::new();
        let mut anchors = BTreeSet::new();
        for chunk in &manifest.chunks {
            let end = offset + chunk.size_bytes;
            if selection.full
                || selection
                    .ranges
                    .iter()
                    .any(|range| range.start < end && offset < range.end)
            {
                selected.insert(chunk.hash);
                anchors.insert(offset / (CHUNK_ANCHOR_BYTES as u64) * (CHUNK_ANCHOR_BYTES as u64));
            }
            offset = end;
        }
        for offset in anchors {
            for chunk in load_canonical_blob_anchor(read, &metadata, offset).await? {
                if selected.contains(&chunk.receipt.hash) && chunks.insert(chunk.receipt.hash) {
                    payload_bytes = payload_bytes.saturating_add(chunk.bytes.len());
                    if payload_bytes > remaining_bytes || inputs.len() >= remaining_records {
                        return Err(invalid("blob discovery payload limit exceeded"));
                    }
                    inputs.push(ReadInput {
                        address: ReadInputAddress::BlobChunk(*chunk.receipt.hash.as_bytes()),
                        bytes: chunk.bytes,
                    });
                }
            }
        }
    }
    Ok(inputs)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fixture() -> (ReadFulfillmentRequest, ReadFulfillmentResponse) {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let bytes = b"a verified immutable chunk".to_vec();
        let address = ReadInputAddress::BlobChunk(*blake3::hash(&bytes).as_bytes());
        let request = ReadFulfillmentRequest {
            epoch_id: uuid::Uuid::now_v7().to_string(),
            interests: vec![LogicalReadInterest::FilesystemMetadata {
                directory: false,
                branch_ids: vec![descriptor.selected_branch.branch_id.clone()],
                file_ids: None,
                directory_ids: None,
                root_directory: false,
                path_predicate: crate::hot_state::FilePathInterest::All,
            }],
            descriptor,
            required: vec![address.clone()],
            continuation: None,
        };
        let inputs = vec![ReadInput { address, bytes }];
        let response = ReadFulfillmentResponse {
            lix_id: request.descriptor.lix_id.clone(),
            epoch_id: request.epoch_id.clone(),
            request_digest: request.digest().unwrap(),
            closure_digest: input_digest(&request, &inputs).unwrap(),
            inputs,
            profile: Default::default(),
            continuation: None,
        };
        authority.close().await.unwrap();
        (request, response)
    }

    #[test]
    fn scan_change_payload_recipe_requires_a_safe_identity_match() {
        let row_pk = crate::row_pk::RowPk::single("row");
        let make_scan = || crate::hot_state::HotStateScanRequest {
            filter: crate::hot_state::HotStateFilter {
                schema_keys: vec!["schema".to_owned()],
                row_pks: vec![row_pk.clone()],
                branch_ids: vec!["branch".to_owned()],
                file_ids: vec![crate::NullableKeyFilter::Null],
                ..Default::default()
            },
            ..Default::default()
        };
        let selects = |scan: &crate::hot_state::HotStateScanRequest, domain| {
            scan_selects_change_identity(scan, domain, "branch", "schema", None, &row_pk)
        };

        assert!(selects(&make_scan(), InterestDomain::Combined));
        assert!(selects(&make_scan(), InterestDomain::Tracked));
        assert!(!selects(&make_scan(), InterestDomain::Untracked));

        let mut broad_scan = make_scan();
        broad_scan.filter.row_pks.clear();
        assert!(
            selects(&broad_scan, InterestDomain::Combined),
            "an empty row key list is a wildcard when schema and branch remain scoped"
        );

        let mut broad_without_schema = broad_scan.clone();
        broad_without_schema.filter.schema_keys.clear();
        assert!(!selects(&broad_without_schema, InterestDomain::Combined));

        let mut broad_wrong_schema = broad_scan.clone();
        broad_wrong_schema.filter.schema_keys[0] = "other_schema".to_owned();
        assert!(!selects(&broad_wrong_schema, InterestDomain::Combined));

        let mut broad_without_branch = broad_scan.clone();
        broad_without_branch.filter.branch_ids.clear();
        assert!(!selects(&broad_without_branch, InterestDomain::Combined));

        let mut broad_wrong_branch = broad_scan.clone();
        broad_wrong_branch.filter.branch_ids[0] = "other_branch".to_owned();
        assert!(!selects(&broad_wrong_branch, InterestDomain::Combined));

        let mut wrong_schema = make_scan();
        wrong_schema.filter.schema_keys[0] = "other_schema".to_owned();
        assert!(!selects(&wrong_schema, InterestDomain::Combined));

        let mut wrong_branch = make_scan();
        wrong_branch.filter.branch_ids[0] = "other_branch".to_owned();
        assert!(!selects(&wrong_branch, InterestDomain::Combined));

        let mut wrong_row = make_scan();
        wrong_row.filter.row_pks[0] = crate::row_pk::RowPk::single("other_row");
        assert!(!selects(&wrong_row, InterestDomain::Combined));

        let mut wrong_file = make_scan();
        wrong_file.filter.file_ids[0] = crate::NullableKeyFilter::Value("file".to_owned());
        assert!(!selects(&wrong_file, InterestDomain::Combined));

        let mut untracked_only = make_scan();
        untracked_only.filter.untracked = Some(true);
        assert!(!selects(&untracked_only, InterestDomain::Combined));

        let mut content_predicate = make_scan();
        content_predicate
            .filter
            .constraints
            .push(crate::hot_state::ScanConstraint {
                field: crate::hot_state::ScanField::RowPk,
                operator: crate::hot_state::ScanOperator::Eq(crate::Value::Text("row".to_owned())),
            });
        assert!(!selects(&content_predicate, InterestDomain::Combined));

        let mut global_scope = make_scan();
        global_scope.filter.global = Some(false);
        assert!(!selects(&global_scope, InterestDomain::Combined));

        let mut tombstones = make_scan();
        tombstones.filter.include_tombstones = true;
        assert!(!selects(&tombstones, InterestDomain::Combined));

        let mut limited = make_scan();
        limited.limit = Some(1);
        assert!(!selects(&limited, InterestDomain::Combined));
    }

    #[tokio::test]
    async fn read_closure_rejects_corruption_omission_and_wrong_admission() {
        let (request, response) = fixture().await;
        validate_response(&request, &response).unwrap();
        validate_complete(&request, &response).unwrap();
        let mut corrupt = response.clone();
        corrupt.inputs[0].bytes[0] ^= 1;
        // Even a self-consistent transport digest cannot authorize corrupt content.
        corrupt.closure_digest = input_digest(&request, &corrupt.inputs).unwrap();
        assert!(validate_complete(&request, &corrupt).is_err());
        let mut omitted = response.clone();
        omitted.inputs.clear();
        omitted.closure_digest = input_digest(&request, &omitted.inputs).unwrap();
        assert!(validate_complete(&request, &omitted).is_err());
        let mut repeated = response.clone();
        repeated.inputs.push(repeated.inputs[0].clone());
        repeated.closure_digest = input_digest(&request, &repeated.inputs).unwrap();
        assert!(validate_complete(&request, &repeated).is_err());
        let mut other = request.clone();
        other.epoch_id = uuid::Uuid::now_v7().to_string();
        assert!(validate_complete(&other, &response).is_err());
    }

    #[tokio::test]
    async fn fulfillment_rejects_fixed_history_endpoints_outside_its_recipe_scope() {
        let (mut request, _) = fixture().await;
        request.interests = vec![LogicalReadInterest::Diff {
            branch_id: Some(request.descriptor.selected_branch.branch_id.clone()),
            relation: "lix_state_diff".into(),
            from: crate::hot_state::DiffInterestEndpoint::Fixed(uuid::Uuid::now_v7().to_string()),
            to: crate::hot_state::DiffInterestEndpoint::ActiveHead,
            filter: Default::default(),
            retain_payloads: true,
            projected_columns: vec![],
            limit: None,
        }];
        let error = request.validate(&request.descriptor.lix_id).unwrap_err();
        assert!(error.to_string().contains("historical diff recipes"));
    }

    #[tokio::test]
    async fn catalog_identity_scan_recipes_are_limited_to_descriptor_branches() {
        let (mut request, _) = fixture().await;
        let catalog_scan = crate::hot_state::HotStateScanRequest {
            filter: crate::hot_state::HotStateFilter {
                schema_keys: vec!["lix_registered_schema".into()],
                branch_ids: vec![request.descriptor.selected_branch.branch_id.clone()],
                file_ids: vec![crate::NullableKeyFilter::Null],
                untracked: Some(false),
                ..Default::default()
            },
            projection: crate::hot_state::HotStateProjection {
                columns: vec!["row_pk".into()],
            },
            ..Default::default()
        };
        request.interests = vec![LogicalReadInterest::Scan {
            request: catalog_scan.clone(),
            domain: InterestDomain::Tracked,
        }];
        request.validate(&request.descriptor.lix_id).unwrap();

        let mut wrong_domain = request.clone();
        wrong_domain.interests = vec![LogicalReadInterest::Scan {
            request: catalog_scan.clone(),
            domain: InterestDomain::Untracked,
        }];
        assert!(
            wrong_domain
                .validate(&wrong_domain.descriptor.lix_id)
                .is_err()
        );

        let mut wrong_branch = request.clone();
        let LogicalReadInterest::Scan {
            request: scan_request,
            ..
        } = &mut wrong_branch.interests[0]
        else {
            unreachable!();
        };
        scan_request.filter.branch_ids = vec!["unleased-branch".into()];
        assert!(
            wrong_branch
                .validate(&wrong_branch.descriptor.lix_id)
                .is_err()
        );
    }

    #[tokio::test]
    async fn read_continuation_rejects_replay_skips_and_closure_changes() {
        let (mut request, mut response) = fixture().await;
        request.continuation = Some(ReadContinuation {
            next_input: 1,
            closure_digest: response.closure_digest.clone(),
        });
        response.continuation = Some(ReadContinuation {
            next_input: 2,
            closure_digest: response.closure_digest.clone(),
        });
        validate_response(&request, &response).unwrap();
        response.continuation.as_mut().unwrap().next_input = 1;
        assert!(validate_response(&request, &response).is_err());
        response.continuation.as_mut().unwrap().next_input = 3;
        assert!(validate_response(&request, &response).is_err());
        response.continuation.as_mut().unwrap().next_input = 2;
        response.closure_digest = "a".repeat(64);
        assert!(validate_response(&request, &response).is_err());
        request.continuation.as_mut().unwrap().next_input = usize::MAX;
        assert!(request.validate(&request.descriptor.lix_id).is_err());
    }

    #[test]
    fn canonical_change_overlay_preserves_only_the_exact_live_payload() {
        let change_id = crate::changelog::ChangeId::for_test_label("overlay-change");
        let source_commit_id = crate::changelog::CommitId::for_test_label("overlay-owner");
        let row_pk = crate::row_pk::RowPk::single("row");
        let created_at = crate::common::LixTimestamp::from_unix_millis_utc_lossy(7);
        let record = crate::changelog::ChangeRecord {
            format_version: 2,
            change_id,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            schema_key: "lix_key_value".to_owned(),
            row_pk: row_pk.clone(),
            file_id: None,
            snapshot: Some(vec![1, 2, 3]),
            metadata: None,
            created_at,
            origin_key: None,
        };
        let canonical = crate::changelog::encode_change_record(&record).unwrap();
        let address = ReadInputAddress::ChangeRecord {
            change_id: change_id.to_string(),
            source_commit_id: source_commit_id.to_string(),
            branch_id: "branch".to_owned(),
            schema_key: record.schema_key.clone(),
            file_id: None,
            row_pk: row_pk.clone(),
            updated_at: created_at.to_string(),
            payload_digest: *blake3::hash(&canonical).as_bytes(),
        };

        address
            .validate_existing_mutable_value(&canonical, &canonical)
            .expect("an exact canonical live overlay is safe to preserve");

        let mut stale_identity = record.clone();
        stale_identity.schema_key = "other_schema".to_owned();
        let stale_identity = crate::changelog::encode_change_record(&stale_identity).unwrap();
        assert!(
            address
                .validate_existing_mutable_value(&stale_identity, &canonical)
                .is_err()
        );

        let mut stale_lifetime = record.clone();
        stale_lifetime.created_at = crate::common::LixTimestamp::from_unix_millis_utc_lossy(8);
        let stale_lifetime = crate::changelog::encode_change_record(&stale_lifetime).unwrap();
        assert!(
            address
                .validate_existing_mutable_value(&stale_lifetime, &canonical)
                .is_err()
        );

        let mut stale_tombstone = record;
        stale_tombstone.snapshot = None;
        let stale_tombstone = crate::changelog::encode_change_record(&stale_tombstone).unwrap();
        assert!(
            address
                .validate_existing_mutable_value(&stale_tombstone, &canonical)
                .is_err()
        );
    }

    #[test]
    fn captured_missing_read_exports_recipe_shape_without_row_payloads() {
        let capture = ReadInterestRegistry::new(16, 4096);
        capture
            .register(LogicalReadInterest::FilesystemPaths {
                scope: crate::filesystem::FilesystemPathIndexScope::All,
                branch_ids: vec!["branch".into()],
                include_blob_refs: false,
                cache_small_blob_data: false,
            })
            .unwrap();
        let error = annotate_capture(
            LixError::new(LixError::CODE_INTERNAL_ERROR, "missing native dependency"),
            Some(&capture),
        );
        let details = error.details.unwrap();
        assert_eq!(details["nativeReadRecipeCount"], serde_json::json!(1));
        assert_eq!(details["nativeReadRecipeMask"], serde_json::json!(16));
        assert!(details.get("readFulfillment").is_some());
    }

    #[test]
    fn selected_change_payload_wire_identity_preserves_uuid_component_types() {
        let id = uuid::Uuid::now_v7().to_string();
        let uuid_row_pk = crate::row_pk::RowPk::uuid_from_canonical(&id).unwrap();
        let string_row_pk = crate::row_pk::RowPk::single(id);
        assert_ne!(uuid_row_pk, string_row_pk);
        let address = ReadInputAddress::ChangeRecord {
            change_id: uuid::Uuid::now_v7().to_string(),
            source_commit_id: uuid::Uuid::now_v7().to_string(),
            branch_id: "branch".to_owned(),
            schema_key: "lix_account".to_owned(),
            file_id: None,
            row_pk: uuid_row_pk.clone(),
            updated_at: "2026-09-30T00:00:00.000Z".to_owned(),
            payload_digest: [0; 32],
        };

        let encoded = serde_json::to_vec(&address).unwrap();
        let wire: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            wire.pointer("/address/row_pk/0/type"),
            Some(&serde_json::json!("uuid"))
        );
        let decoded: ReadInputAddress = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, address);
        let ReadInputAddress::ChangeRecord { row_pk, .. } = decoded else {
            unreachable!("the typed row address round-trips as a ChangeRecord")
        };
        assert_eq!(row_pk, uuid_row_pk);
        assert_ne!(row_pk, string_row_pk);
    }

    #[test]
    fn filesystem_path_recipe_selects_only_its_native_scope() {
        assert_eq!(payload_schema_kind("private_custom_schema"), "other");
        let id = "00000000-0000-7000-8000-000000000031";
        let other = "00000000-0000-7000-8000-000000000032";
        let key = crate::row_pk::RowPk::uuid_from_canonical(id).unwrap();
        let other_key = crate::row_pk::RowPk::uuid_from_canonical(other).unwrap();
        let mut interest = LogicalReadInterest::FilesystemPaths {
            scope: crate::filesystem::FilesystemPathIndexScope::FileIds(vec![id.into()]),
            branch_ids: vec!["branch".into()],
            include_blob_refs: true,
            cache_small_blob_data: false,
        };
        assert_eq!(payload_recipe_mask(std::slice::from_ref(&interest)), 16);
        assert!(scan_recipe_selects_change_identity(
            &interest,
            "branch",
            "lix_file_descriptor",
            Some(id),
            &key
        ));
        assert!(scan_recipe_selects_change_identity(
            &interest,
            "branch",
            "lix_binary_blob_ref",
            Some(id),
            &key
        ));
        assert!(scan_recipe_selects_change_identity(
            &interest,
            "branch",
            "lix_directory_descriptor",
            None,
            &other_key
        ));
        assert!(!scan_recipe_selects_change_identity(
            &interest,
            "branch",
            "lix_file_descriptor",
            Some(other),
            &other_key
        ));
        assert!(!scan_recipe_selects_change_identity(
            &interest,
            "other-branch",
            "lix_file_descriptor",
            Some(id),
            &key
        ));
        assert!(!scan_recipe_selects_change_identity(
            &interest,
            "branch",
            "lix_account",
            None,
            &key
        ));
        if let LogicalReadInterest::FilesystemPaths {
            include_blob_refs, ..
        } = &mut interest
        {
            *include_blob_refs = false;
        }
        assert!(!scan_recipe_selects_change_identity(
            &interest,
            "branch",
            "lix_binary_blob_ref",
            Some(id),
            &key
        ));
        let metadata = LogicalReadInterest::FilesystemMetadata {
            directory: false,
            branch_ids: vec!["branch".into()],
            file_ids: Some(vec![id.into()]),
            directory_ids: None,
            root_directory: false,
            path_predicate: crate::hot_state::FilePathInterest::All,
        };
        for (branch, schema, file, row, expected) in [
            ("branch", "lix_directory_descriptor", None, &other_key, true),
            ("branch", "lix_file_descriptor", Some(id), &key, true),
            ("branch", "lix_file_descriptor", Some(other), &other_key, false),
            ("other-branch", "lix_directory_descriptor", None, &other_key, false),
            ("branch", "lix_binary_blob_ref", Some(id), &key, false),
            ("branch", "lix_account", None, &key, false),
        ] {
            assert_eq!(
                scan_recipe_selects_change_identity(&metadata, branch, schema, file, row),
                expected,
            );
        }
    }

    #[tokio::test]
    async fn cold_file_path_recipe_payloads_validate_after_wire_roundtrip() {
        let authority = crate::open_lix().await.unwrap();
        authority
            .set_sync_role(crate::sync::SyncRole::Authority)
            .unwrap();
        authority
            .execute(
                "INSERT INTO lix_file(path, content) VALUES('/cold.txt', CAST('text' AS BYTEA))",
                &[],
            )
            .await
            .unwrap();
        let rows = authority
            .execute(
                "SELECT id, lixcol_change_id AS change_id FROM lix_file WHERE path='/cold.txt'",
                &[],
            )
            .await
            .unwrap();
        let file_id = rows.rows()[0].get::<String>("id").unwrap();
        let change_id = rows.rows()[0].get::<String>("change_id").unwrap();
        let adapter = authority.storage_adapter();
        let mut writes = adapter.new_write_set();
        writes.delete(
            crate::changelog::CHANGE_SPACE,
            StorageKey(Bytes::copy_from_slice(
                canonical_change_id(&change_id)
                    .unwrap()
                    .as_uuid()
                    .as_bytes(),
            )),
        );
        adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let leased = authority
            .leased_partial_replica_descriptor(None)
            .await
            .unwrap();
        let request = ReadFulfillmentRequest {
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor: leased.descriptor.clone(),
            interests: vec![
                LogicalReadInterest::FileContent {
                    request: crate::hot_state::HotStateScanRequest {
                        filter: crate::hot_state::HotStateFilter {
                            schema_keys: vec![
                                "lix_file_descriptor".to_owned(),
                                "lix_binary_blob_ref".to_owned(),
                                "lix_directory_descriptor".to_owned(),
                            ],
                            branch_ids: vec![leased.descriptor.selected_branch.branch_id.clone()],
                            ..Default::default()
                        },
                        projection: crate::hot_state::HotStateProjection {
                            columns: vec!["snapshot_content".to_owned()],
                        },
                        limit: None,
                    },
                    file_ids: Some(vec![file_id.clone()]),
                    directory_ids: None,
                    root_directory: false,
                    indexed: true,
                    path_predicate: crate::hot_state::FilePathInterest::All,
                    byte_range: None,
                },
                LogicalReadInterest::FilesystemPaths {
                    scope: crate::filesystem::FilesystemPathIndexScope::FileIds(vec![
                        file_id.clone(),
                    ]),
                    branch_ids: vec![leased.descriptor.selected_branch.branch_id.clone()],
                    include_blob_refs: true,
                    cache_small_blob_data: false,
                },
            ],
            required: vec![
                ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(change_id)),
                ReadInputAddress::Metadata(NativeMetadataRef::CommitGraphRecord(
                    leased.descriptor.selected_branch.head.commit_id.clone(),
                )),
            ],
            continuation: None,
        };
        let request: ReadFulfillmentRequest =
            serde_json::from_slice(&serde_json::to_vec(&request).unwrap()).unwrap();
        let response = authority
            .read_sync_fulfillment(&request, &leased.lease.lease_id)
            .await
            .unwrap();
        assert!(response.inputs.iter().any(|input| matches!(&input.address, ReadInputAddress::ChangeRecord { schema_key, .. } if schema_key == "lix_file_descriptor" || schema_key == "lix_binary_blob_ref")), "the test must exercise a selected mutable payload");
        let response: ReadFulfillmentResponse =
            serde_json::from_slice(&serde_json::to_vec(&response).unwrap()).unwrap();
        validate_complete(&request, &response).unwrap();
        let other =
            crate::row_pk::RowPk::uuid_from_canonical("00000000-0000-7000-8000-000000000032")
                .unwrap();
        for input in &response.inputs {
            if let ReadInputAddress::ChangeRecord {
                branch_id,
                schema_key,
                file_id,
                ..
            } = &input.address
            {
                if schema_key != "lix_directory_descriptor" {
                    assert!(!request.interests.iter().any(|interest| {
                        scan_recipe_selects_change_identity(
                            interest,
                            branch_id,
                            schema_key,
                            file_id.as_deref(),
                            &other,
                        )
                    }));
                }
            }
        }
        authority.close().await.unwrap();
    }

    #[test]
    fn client_install_failure_annotation_exports_only_bounded_reason_and_phase() {
        let prefixed = LixError::new(
            "LIX_READ_FULFILLMENT_INVALID",
            "fulfill native read: private context",
        )
        .with_details(
            serde_json::json!({"payloadFailureReason": "selected_change_payload_recipe_mismatch"}),
        );
        let annotated = annotate_client_failure(prefixed, ClientFailurePhase::Validation);
        assert_eq!(
            annotated
                .details
                .as_ref()
                .unwrap()
                .get("payloadFailureReason"),
            Some(&serde_json::json!(
                "selected_change_payload_recipe_mismatch"
            ))
        );
        let error = LixError::new(
            "LIX_READ_FULFILLMENT_INVALID",
            "canonical selected change payload identity or lifetime mismatch",
        )
        .with_details(serde_json::json!({"privateAddress": "secret row value"}));
        let error = annotate_client_failure(error, ClientFailurePhase::Installation);
        let details = error.details.unwrap();
        assert_eq!(
            details.get("payloadPhase"),
            Some(&serde_json::json!("read_fulfillment_install"))
        );
        assert_eq!(
            details.get("payloadFailureReason"),
            Some(&serde_json::json!(
                "selected_change_payload_identity_or_lifetime_mismatch"
            ))
        );

        let generic = LixError::new("LIX_READ_FULFILLMENT_INVALID", "private validation detail");
        let generic = annotate_client_failure(generic, ClientFailurePhase::Validation);
        let details = generic.details.unwrap();
        assert_eq!(
            details.get("payloadPhase"),
            Some(&serde_json::json!("read_fulfillment_validation"))
        );
        assert_eq!(
            details.get("payloadFailureReason"),
            Some(&serde_json::json!("read_fulfillment_validation_failed"))
        );

        let unrelated = LixError::new("LIX_STORAGE_IO", "private storage failure");
        let unrelated = annotate_client_failure(unrelated, ClientFailurePhase::Installation);
        assert!(unrelated.details.is_none());
    }

    #[tokio::test]
    async fn authority_fulfills_direct_change_locator_without_physical_locator_row() {
        let authority = crate::open_lix().await.unwrap();
        authority
            .set_sync_role(crate::sync::SyncRole::Authority)
            .unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('direct-locator', 'value')",
                &[],
            )
            .await
            .unwrap();
        let rows = authority
            .execute(
                "SELECT lixcol_change_id AS id FROM lix_key_value WHERE key = 'direct-locator'",
                &[],
            )
            .await
            .unwrap();
        let change = rows.rows()[0].get::<String>("id").unwrap();
        let address = NativeMetadataRef::ChangeLocator(change.clone());
        let adapter = authority.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let key = crate::sync::native_metadata::key(&address).unwrap();
        let values = read
            .get_many(&[StorageGetManyRequest {
                space: crate::sync::native_metadata::space(&address),
                keys: std::slice::from_ref(&key),
                opts: Default::default(),
            }])
            .await
            .unwrap()
            .values;
        assert!(values[0].is_none(), "direct changes have no locator row");
        drop(read);

        let leased = authority
            .leased_partial_replica_descriptor(None)
            .await
            .unwrap();
        let request = ReadFulfillmentRequest {
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor: leased.descriptor.clone(),
            interests: vec![LogicalReadInterest::FilesystemMetadata {
                directory: false,
                branch_ids: vec![leased.descriptor.selected_branch.branch_id.clone()],
                file_ids: None,
                directory_ids: None,
                root_directory: false,
                path_predicate: crate::hot_state::FilePathInterest::All,
            }],
            required: vec![ReadInputAddress::Metadata(address.clone())],
            continuation: None,
        };
        let response = authority
            .read_sync_fulfillment(&request, &leased.lease.lease_id)
            .await
            .unwrap();
        let input = response
            .inputs
            .iter()
            .find(|input| input.address == ReadInputAddress::Metadata(address.clone()))
            .expect("required direct locator should be returned");
        crate::sync::native_metadata::validate_bytes(&address, &input.bytes).unwrap();
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn selected_scan_row_closes_its_canonical_change_payload() {
        let authority = crate::open_lix().await.unwrap();
        authority
            .set_sync_role(crate::sync::SyncRole::Authority)
            .unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('closure-payload-row', 'value')",
                &[],
            )
            .await
            .unwrap();
        let rows = authority
            .execute(
                "SELECT lixcol_change_id AS id FROM lix_key_value WHERE key = 'closure-payload-row'",
                &[],
            )
            .await
            .unwrap();
        let change_id = rows.rows()[0]
            .get::<String>("id")
            .unwrap()
            .parse::<crate::changelog::ChangeId>()
            .unwrap();
        // Exercise the historical cold case: the mutable standalone projection
        // is missing, but the exact row's canonical physical owner remains.
        let storage = authority.storage_adapter();
        let mut writes = storage.new_write_set();
        writes.delete(
            crate::changelog::CHANGE_SPACE,
            StorageKey(Bytes::copy_from_slice(change_id.as_uuid().as_bytes())),
        );
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let leased = authority
            .leased_partial_replica_descriptor(None)
            .await
            .unwrap();
        let change = change_id.to_string();
        let request = ReadFulfillmentRequest {
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor: leased.descriptor.clone(),
            interests: vec![LogicalReadInterest::Scan {
                request: crate::hot_state::HotStateScanRequest {
                    filter: crate::hot_state::HotStateFilter {
                        schema_keys: vec!["lix_key_value".to_owned()],
                        row_pks: vec![crate::row_pk::RowPk::single("closure-payload-row")],
                        branch_ids: vec![leased.descriptor.selected_branch.branch_id.clone()],
                        file_ids: vec![crate::NullableKeyFilter::Null],
                        untracked: None,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                domain: InterestDomain::Combined,
            }],
            required: vec![ReadInputAddress::Metadata(
                NativeMetadataRef::ChangeLocator(change.clone()),
            )],
            continuation: None,
        };
        let response = authority
            .read_sync_fulfillment(&request, &leased.lease.lease_id)
            .await
            .unwrap();
        validate_complete(&request, &response)
            .expect("an exact row identity selected by a scan is a valid payload recipe");

        // Re-sign a response against a recipe that differs only in its row
        // identity. This reaches the recipe authorization check rather than
        // failing earlier on the request or closure digest.
        let mut wrong_recipe = request.clone();
        let LogicalReadInterest::Scan { request: scan, .. } = &mut wrong_recipe.interests[0] else {
            unreachable!("the fixture uses one Scan recipe")
        };
        scan.filter.row_pks[0] = crate::row_pk::RowPk::single("different-row");
        let mut wrong_response = response.clone();
        wrong_response.request_digest = wrong_recipe.digest().unwrap();
        let wrong_closure_digest = input_digest(&wrong_recipe, &wrong_response.inputs).unwrap();
        wrong_response.closure_digest = wrong_closure_digest;
        let error = validate_complete(&wrong_recipe, &wrong_response)
            .expect_err("a payload for a different row must not be authorized by the scan");
        assert_eq!(error.code, "LIX_READ_FULFILLMENT_INVALID");
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("payloadFailureReason")),
            Some(&serde_json::json!(
                "selected_change_payload_recipe_mismatch"
            )),
        );

        let expected_key = StorageKey(Bytes::copy_from_slice(change_id.as_uuid().as_bytes()));
        assert!(
            response.inputs.iter().any(|input| {
                matches!(input.address, ReadInputAddress::ChangeRecord { .. })
                    && input.address.coordinate().is_ok_and(|(space, key)| {
                        space == crate::changelog::CHANGE_SPACE && key == expected_key
                    })
            }),
            "the selected scan-row recipe must transfer its canonical physical payload even when CHANGE_SPACE is absent"
        );
        let canonical_input = response
            .inputs
            .iter()
            .find(|input| matches!(input.address, ReadInputAddress::ChangeRecord { .. }))
            .expect("selected row must include its canonical change payload");
        let canonical_record =
            crate::changelog::decode_change_record(&canonical_input.bytes, change_id).unwrap();
        let locator_address = NativeMetadataRef::ChangeLocator(change.clone());
        let canonical_locator_input = response
            .inputs
            .iter()
            .find(|input| input.address == ReadInputAddress::Metadata(locator_address.clone()))
            .expect("selected row must include its canonical change locator");
        let canonical_locator =
            crate::tracked_state::decode_change_locator(change_id, &canonical_locator_input.bytes)
                .unwrap();
        let canonical_source_commit_id = match &canonical_input.address {
            ReadInputAddress::ChangeRecord {
                source_commit_id, ..
            } => source_commit_id,
            _ => unreachable!("canonical input was selected as a ChangeRecord"),
        };
        assert_eq!(
            canonical_locator.commit_id.to_string(),
            *canonical_source_commit_id,
            "selected locator and payload must share their exact physical source"
        );
        let stale_locator = crate::tracked_state::CommitDeltaChangeLocator {
            change_id,
            commit_id: crate::changelog::CommitId::for_test_label(
                "stale-but-decodable-selected-locator-owner",
            ),
            segment_index: 0,
            ordinal: 0,
        };
        assert_ne!(stale_locator.commit_id, canonical_locator.commit_id);
        let stale_locator_bytes = crate::tracked_state::encode_change_locator(stale_locator);
        crate::sync::native_metadata::validate_bytes(&locator_address, &stale_locator_bytes)
            .expect("stale fixture locator is still structurally valid");
        let mut stale_record = canonical_record.clone();
        stale_record.schema_key = "lix_file_descriptor".to_owned();
        let stale_bytes = crate::changelog::encode_change_record(&stale_record).unwrap();
        let partial_state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            authority.active_account_id().to_owned(),
            request.epoch_id.clone(),
            request.descriptor.clone(),
        )
        .unwrap();
        let partial_storage = StorageAdapter::new(Memory::new());
        let locator_key = crate::sync::native_metadata::key(&locator_address).unwrap();
        let mut admission_writes = partial_storage.new_write_set();
        let admission = crate::sync::partial_state::stage_partial_replica_state(
            &mut admission_writes,
            &partial_state,
            None,
        )
        .unwrap();
        let mut migration = partial_storage
            .begin_migration_write(StorageWriteOptions {
                preconditions: vec![admission],
                ..Default::default()
            })
            .await
            .unwrap();
        admission_writes.lower_into(&mut migration).await.unwrap();
        migration.commit().await.unwrap();
        partial_storage
            .admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
        let change_key = StorageKey(Bytes::copy_from_slice(change_id.as_uuid().as_bytes()));
        let pending_id =
            crate::changelog::ChangeId::for_test_label("unrelated-local-pending-change");
        assert_ne!(pending_id, change_id);
        let pending_record = crate::changelog::ChangeRecord {
            format_version: 2,
            change_id: pending_id,
            account_id: authority.active_account_id().to_owned(),
            schema_key: "lix_key_value".to_owned(),
            row_pk: crate::row_pk::RowPk::single("unrelated-local-pending-row"),
            file_id: None,
            snapshot: Some(vec![4, 5, 6]),
            metadata: None,
            created_at: crate::common::LixTimestamp::from_unix_millis_utc_lossy(9),
            origin_key: None,
        };
        let pending_bytes = crate::changelog::encode_change_record(&pending_record).unwrap();
        let pending_key = StorageKey(Bytes::copy_from_slice(pending_id.as_uuid().as_bytes()));
        let mut stale_writes = partial_storage.new_write_set();
        stale_writes.put(
            crate::changelog::CHANGE_SPACE,
            change_key.clone(),
            StorageValue {
                bytes: Bytes::from(stale_bytes),
            },
        );
        stale_writes.put(
            crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
            locator_key.clone(),
            StorageValue {
                bytes: Bytes::from(stale_locator_bytes.clone()),
            },
        );
        stale_writes.put(
            crate::changelog::CHANGE_SPACE,
            pending_key.clone(),
            StorageValue {
                bytes: Bytes::from(pending_bytes.clone()),
            },
        );
        partial_storage
            .commit_partial_replica_write_set(
                crate::sync::partial_replica_write_capability(),
                stale_writes,
                StorageWriteOptions {
                    preconditions: vec![
                        StoragePrecondition::KeyAbsent {
                            space: crate::changelog::CHANGE_SPACE,
                            key: change_key.clone(),
                        },
                        StoragePrecondition::KeyAbsent {
                            space: crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
                            key: locator_key.clone(),
                        },
                        StoragePrecondition::KeyAbsent {
                            space: crate::changelog::CHANGE_SPACE,
                            key: pending_key.clone(),
                        },
                    ],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        install(&partial_storage, &partial_state, &request, &response)
            .await
            .expect("canonical selected payload must replace its stale projection");
        let read = partial_storage
            .begin_read(Default::default())
            .await
            .unwrap();
        let installed = PointReadPlan::new(
            crate::changelog::CHANGE_SPACE,
            std::slice::from_ref(&change_key),
        )
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten()
        .and_then(|value| match value {
            StorageProjectedValue::FullValue(bytes) => Some(bytes),
            StorageProjectedValue::KeyOnly => None,
        })
        .expect("canonical payload should be installed");
        assert_eq!(installed.as_ref(), canonical_input.bytes.as_slice());
        let installed_locator = PointReadPlan::new(
            crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
            std::slice::from_ref(&locator_key),
        )
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten()
        .and_then(|value| match value {
            StorageProjectedValue::FullValue(bytes) => Some(bytes),
            StorageProjectedValue::KeyOnly => None,
        })
        .expect("canonical locator should replace its stale projection");
        assert_eq!(
            installed_locator.as_ref(),
            canonical_locator_input.bytes.as_slice()
        );
        assert_eq!(
            crate::tracked_state::decode_change_locator(change_id, &installed_locator).unwrap(),
            canonical_locator,
            "next locator resolution must use the exact selected source"
        );
        let pending = PointReadPlan::new(
            crate::changelog::CHANGE_SPACE,
            std::slice::from_ref(&pending_key),
        )
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten()
        .and_then(|value| match value {
            StorageProjectedValue::FullValue(bytes) => Some(bytes),
            StorageProjectedValue::KeyOnly => None,
        })
        .expect("unrelated local pending payload should remain installed");
        assert_eq!(pending.as_ref(), pending_bytes.as_slice());

        // Repeat with an optional locator response. A valid resident locator
        // is mutable state too, but this exact payload pairing proves the
        // canonical address strongly enough to repair it with CAS.
        let optional_request = ReadFulfillmentRequest {
            required: vec![ReadInputAddress::Metadata(
                NativeMetadataRef::CommitGraphRecord(
                    request.descriptor.selected_branch.head.commit_id.clone(),
                ),
            )],
            ..request.clone()
        };
        let optional_response = authority
            .read_sync_fulfillment(&optional_request, &leased.lease.lease_id)
            .await
            .unwrap();
        partial_storage
            .commit_partial_replica_write_set(
                crate::sync::partial_replica_write_capability(),
                {
                    let mut writes = partial_storage.new_write_set();
                    writes.put(
                        crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
                        locator_key.clone(),
                        StorageValue {
                            bytes: Bytes::from(stale_locator_bytes),
                        },
                    );
                    writes
                },
                StorageWriteOptions {
                    preconditions: vec![StoragePrecondition::KeyValueEquals {
                        space: crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
                        key: locator_key.clone(),
                        expected: installed_locator,
                    }],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        install(
            &partial_storage,
            &partial_state,
            &optional_request,
            &optional_response,
        )
        .await
        .expect("paired optional canonical locator should repair a stale projection");
        let read = partial_storage
            .begin_read(Default::default())
            .await
            .unwrap();
        let repaired_optional_locator = PointReadPlan::new(
            crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
            std::slice::from_ref(&locator_key),
        )
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten()
        .and_then(|value| match value {
            StorageProjectedValue::FullValue(bytes) => Some(bytes),
            StorageProjectedValue::KeyOnly => None,
        })
        .expect("optional canonical locator should remain installed");
        assert_eq!(
            repaired_optional_locator.as_ref(),
            canonical_locator_input.bytes.as_slice()
        );
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn broad_account_scan_closes_only_its_scoped_canonical_payloads() {
        let authority = crate::open_lix().await.unwrap();
        authority
            .set_sync_role(crate::sync::SyncRole::Authority)
            .unwrap();
        let account_id = uuid::Uuid::now_v7().to_string();
        authority
            .ensure_account(&account_id, "Read fulfillment fixture", "human")
            .await
            .unwrap();
        let global = authority
            .open_another_session()
            .with_branch(crate::GLOBAL_BRANCH_ID)
            .await
            .unwrap();
        let rows = global
            .execute(
                "SELECT lixcol_change_id AS id FROM lix_account WHERE id = $1 AND lixcol_untracked = false LIMIT 10",
                &[crate::Value::Text(account_id.clone())],
            )
            .await
            .unwrap();
        let change_id = rows.rows()[0]
            .get::<String>("id")
            .unwrap()
            .parse::<crate::changelog::ChangeId>()
            .unwrap();

        // Exercise the same broad, row-key-free recipe emitted for the
        // production `SELECT id FROM lix_account ... LIMIT 10` scan. LIMIT is
        // applied above this native scan, so its recipe intentionally carries
        // no row keys or native limit.
        let leased = authority
            .leased_partial_replica_descriptor(Some(crate::GLOBAL_BRANCH_ID))
            .await
            .unwrap();
        let change = change_id.to_string();
        let request = ReadFulfillmentRequest {
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor: leased.descriptor.clone(),
            interests: vec![LogicalReadInterest::Scan {
                request: crate::hot_state::HotStateScanRequest {
                    filter: crate::hot_state::HotStateFilter {
                        schema_keys: vec!["lix_account".to_owned()],
                        branch_ids: vec![crate::GLOBAL_BRANCH_ID.to_owned()],
                        // Empty row_pks and file_ids mean wildcard, as in the
                        // production native scan. The schema and branch remain
                        // explicit and descriptor-scoped.
                        untracked: None,
                        ..Default::default()
                    },
                    projection: crate::hot_state::HotStateProjection {
                        columns: vec!["id".to_owned()],
                    },
                    limit: None,
                },
                domain: InterestDomain::Combined,
            }],
            required: vec![ReadInputAddress::Metadata(
                NativeMetadataRef::ChangeLocator(change.clone()),
            )],
            continuation: None,
        };

        // Simulate an old physical repository where the mutable standalone
        // changelog projection is missing while the selected row's owner is
        // still present in its canonical commit delta.
        let storage = authority.storage_adapter();
        let mut writes = storage.new_write_set();
        writes.delete(
            crate::changelog::CHANGE_SPACE,
            StorageKey(Bytes::copy_from_slice(change_id.as_uuid().as_bytes())),
        );
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let response = authority
            .read_sync_fulfillment(&request, &leased.lease.lease_id)
            .await
            .unwrap();
        // Exercise the HTTP serde boundary that otherwise collapses an
        // account UUID row key into an indistinguishable string component.
        let response: ReadFulfillmentResponse =
            serde_json::from_slice(&serde_json::to_vec(&response).unwrap()).unwrap();
        validate_complete(&request, &response)
            .expect("row-key-free account scan must authorize its exact returned rows");

        let expected_row_pk = crate::row_pk::RowPk::uuid_from_canonical(&account_id).unwrap();
        let mut selected_record = None;
        for input in &response.inputs {
            let ReadInputAddress::ChangeRecord {
                change_id: selected_change_id,
                source_commit_id,
                branch_id,
                schema_key,
                file_id,
                row_pk,
                ..
            } = &input.address
            else {
                continue;
            };
            assert_eq!(schema_key, "lix_account");
            assert_eq!(branch_id, crate::GLOBAL_BRANCH_ID);
            assert_eq!(file_id, &None);

            let locator_address = ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(
                selected_change_id.clone(),
            ));
            let locator_input = response
                .inputs
                .iter()
                .find(|candidate| candidate.address == locator_address)
                .expect("every selected payload must retain its canonical locator");
            let parsed_change = canonical_change_id(selected_change_id).unwrap();
            let locator =
                crate::tracked_state::decode_change_locator(parsed_change, &locator_input.bytes)
                    .unwrap();
            assert_eq!(locator.commit_id.to_string(), *source_commit_id);

            if row_pk == &expected_row_pk {
                selected_record = Some(selected_change_id.clone());
            }
        }
        assert_eq!(
            selected_record.as_deref(),
            Some(change.as_str()),
            "the account row selected by SQL must have its canonical payload in the closure"
        );

        global.close().await.unwrap();
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn install_coalesces_chunk_and_manifest_demand_mutations() {
        let authority = crate::open_lix().await.unwrap();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            authority.active_account_id().to_owned(),
            uuid::Uuid::now_v7().to_string(),
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap();
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        let admission =
            crate::sync::partial_state::stage_partial_replica_state(&mut writes, &state, None)
                .unwrap();
        let mut raw = storage
            .begin_migration_write(StorageWriteOptions {
                preconditions: vec![admission],
                ..Default::default()
            })
            .await
            .unwrap();
        writes.lower_into(&mut raw).await.unwrap();
        raw.commit().await.unwrap();
        storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());

        let bytes = b"chunk and manifest arrive together".to_vec();
        let canonical = crate::binary_cas::CanonicalBlobManifest::from_bytes(&bytes);
        let chunk = &canonical.chunks[0];
        let wire = crate::sync::SyncBlobManifest {
            blob_id: canonical.blob_id.to_hex(),
            size_bytes: canonical.size_bytes,
            chunks: canonical
                .chunks
                .iter()
                .map(|chunk| crate::sync::SyncBlobChunk {
                    chunk_id: chunk.hash.to_hex(),
                    size_bytes: chunk.size_bytes,
                })
                .collect(),
            inline_bytes_base64: None,
        };
        let chunk_input = ReadInput {
            address: ReadInputAddress::BlobChunk(*chunk.hash.as_bytes()),
            bytes: bytes.clone(),
        };
        let manifest_input = ReadInput {
            address: ReadInputAddress::BlobManifest(*canonical.blob_id.as_bytes()),
            bytes: serde_json::to_vec(&wire).unwrap(),
        };
        // The chunk deliberately precedes the manifest. A per-input installer
        // would stage a demand after the chunk's delete and hit duplicate
        // mutation validation; the batch CAS lowerer emits one final demand
        // mutation per chunk key.
        let request = ReadFulfillmentRequest {
            epoch_id: state.epoch_id().to_owned(),
            descriptor: state.descriptor().clone(),
            interests: vec![LogicalReadInterest::FilesystemMetadata {
                directory: false,
                branch_ids: vec![state.descriptor().selected_branch.branch_id.clone()],
                file_ids: None,
                directory_ids: None,
                root_directory: false,
                path_predicate: crate::hot_state::FilePathInterest::All,
            }],
            required: vec![chunk_input.address.clone()],
            continuation: None,
        };
        let inputs = vec![chunk_input, manifest_input];
        let response = ReadFulfillmentResponse {
            lix_id: request.descriptor.lix_id.clone(),
            epoch_id: request.epoch_id.clone(),
            request_digest: request.digest().unwrap(),
            closure_digest: input_digest(&request, &inputs).unwrap(),
            inputs,
            profile: Default::default(),
            continuation: None,
        };

        install(&storage, &state, &request, &response)
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        assert!(
            crate::binary_cas::load_verified_chunk(&read, chunk.hash)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            crate::binary_cas::load_metadata_many(&read, &[canonical.blob_id])
                .await
                .unwrap()
                .into_vec()[0]
                .is_some()
        );
        let demand = PointReadPlan::new(
            crate::binary_cas::BINARY_CAS_CHUNK_DEMAND_SPACE,
            &[StorageKey(Bytes::copy_from_slice(chunk.hash.as_bytes()))],
        )
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten();
        assert!(demand.is_none(), "resident chunk retained a demand marker");
        authority.close().await.unwrap();
    }
}
