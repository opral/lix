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
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

mod spool;
pub(crate) mod staging;

const DISCOVERY_READ_BUDGET: ReadBudget = ReadBudget {
    max_result_bytes: 8 * 1024 * 1024,
    max_single_value_bytes: 64 * 1024 * 1024,
};

const MAX_INPUT_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_RESPONSE_BYTES: usize = PAGE_PAYLOAD_BYTES.div_ceil(3) * 4 + 2 * 1024 * 1024;
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

pub(crate) fn history_recipes_within_budget<'a>(
    interests: impl IntoIterator<Item = &'a LogicalReadInterest>,
) -> bool {
    let mut recipes = 0usize;
    let mut selected_ids = 0usize;
    for interest in interests {
        if let LogicalReadInterest::History { commit_ids, .. } = interest {
            let Some(next_recipes) = recipes.checked_add(1) else {
                return false;
            };
            let Some(next_selected_ids) = selected_ids.checked_add(commit_ids.len()) else {
                return false;
            };
            recipes = next_recipes;
            selected_ids = next_selected_ids;
            if recipes > crate::hot_state::MAX_HISTORY_RECIPE_COUNT
                || selected_ids > crate::hot_state::MAX_HISTORY_RECIPE_SELECTED_IDS
            {
                return false;
            }
        }
    }
    true
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
    if error.automatic_retry_is_forbidden() {
        return error;
    }
    let Some(capture) = capture else {
        return error;
    };
    let snapshot = match capture.snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => return error,
    };
    let has_bounded_history = snapshot
        .interests
        .iter()
        .any(|interest| is_bounded_native_recipe(interest.as_ref()));
    if error
        .details
        .as_ref()
        .is_some_and(|d| d.get("nativeHistoryDemand").is_some())
        && !has_bounded_history
        && selected_change_payload_locator(&error).is_none()
    {
        return error;
    }
    if snapshot.interests.is_empty() {
        return error;
    }
    let has_diff = snapshot.interests.iter().any(|interest| {
        matches!(interest.as_ref(), LogicalReadInterest::Diff { .. })
            && !super::working_diff_recipe::is_supported_bounded_diff_recipe(interest.as_ref())
    });
    let history_over_budget =
        !history_recipes_within_budget(snapshot.interests.iter().map(|interest| interest.as_ref()));
    let bounded_diff_over_budget = snapshot
        .interests
        .iter()
        .filter(|interest| matches!(interest.as_ref(), LogicalReadInterest::Diff { .. }))
        .count()
        > super::working_diff_recipe::MAX_BOUNDED_DIFF_RECIPE_COUNT;
    let salvage_current_recipes = has_diff || history_over_budget || bounded_diff_over_budget;
    if salvage_current_recipes && selected_change_payload_locator(&error).is_none() {
        // Historical diffs may name local pending commits. Their specialized
        // demand path owns those endpoints; over-budget History has the same
        // native-demand behavior. Neither can be replayed as an admitted
        // current-state recipe without a selected payload locator.
        return error;
    }
    let interests: Vec<_> = if salvage_current_recipes {
        // Historical recipes cannot authorize a current mutable payload.
        // When the native miss identifies that payload exactly, retain only
        // independent current recipes; the historical miss stays native.
        snapshot
            .interests
            .iter()
            .filter(|interest| {
                !matches!(
                    interest.as_ref(),
                    LogicalReadInterest::History { .. } | LogicalReadInterest::Diff { .. }
                )
            })
            .map(|interest| interest.as_ref())
            .collect()
    } else {
        snapshot
            .interests
            .iter()
            .map(|interest| interest.as_ref())
            .collect()
    };
    if interests.is_empty() || interests.len() > 4096 {
        return error;
    }
    let serialized_interests = match serde_json::to_vec(&interests) {
        Ok(serialized) => serialized,
        Err(_) => return invalid("invalid read operation recipes"),
    };
    if serialized_interests.len() > MAX_RECIPE_BYTES {
        return invalid("read operation recipe byte limit exceeded");
    }
    // Keep the native missing diagnostic intact for corruption handling and
    // pinned transaction admission. Failed captures are never published.
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

/// Narrow opportunistic fulfillment recipes against the actual partial lease
/// before any authority request is sent. Unsupported historical scopes stay
/// on the native-demand path unless the native error independently identifies
/// the selected live change payload; in that case only independent current
/// recipes may accompany its canonical locator.
pub(super) struct PartialDemandFulfillmentPlan {
    pub(super) interests: Vec<LogicalReadInterest>,
    /// Present only when unsupported historical recipes were removed under a
    /// selected-payload diagnostic. That replay must require the locator alone.
    pub(super) selected_payload_locator: Option<NativeMetadataRef>,
}

pub(super) fn partial_demand_fulfillment_plan(
    error: &LixError,
    interests: &[LogicalReadInterest],
    selected_branch_id: &str,
) -> Option<PartialDemandFulfillmentPlan> {
    let history_shape_is_supported = |interest: &LogicalReadInterest| {
        let LogicalReadInterest::History {
            branch_id,
            commit_ids,
            relation,
            filter,
            retain_payloads,
            projected_columns,
            limit,
        } = interest
        else {
            return true;
        };
        if branch_id != selected_branch_id
            || limit.is_some()
            || commit_ids.is_empty()
            || commit_ids.len() > crate::hot_state::MAX_HISTORY_RECIPE_COMMIT_IDS
            || !matches!(relation.as_str(), "lix_file" | "lix_directory")
        {
            return false;
        }
        let mut seen = BTreeSet::new();
        if commit_ids.iter().any(|id| {
            canonical_commit_id(id).map_or(true, |parsed| {
                !parsed.has_canonical_text(id) || !seen.insert(parsed)
            })
        }) {
            return false;
        }
        crate::sql2::validate_bounded_history_recipe_shape(
            relation,
            filter,
            projected_columns,
            *retain_payloads,
        )
        .is_ok()
    };
    let bounded_diff_count = interests
        .iter()
        .filter(|interest| matches!(interest, LogicalReadInterest::Diff { .. }))
        .count();
    let historical_scope_is_unsupported = !history_recipes_within_budget(interests)
        || bounded_diff_count > super::working_diff_recipe::MAX_BOUNDED_DIFF_RECIPE_COUNT
        || interests.iter().any(|interest| {
            matches!(interest, LogicalReadInterest::History { .. })
                && !history_shape_is_supported(interest)
        })
        || super::working_diff_recipe::validate_bounded_diff_recipes(interests, selected_branch_id)
            .is_err();
    if !historical_scope_is_unsupported {
        return Some(PartialDemandFulfillmentPlan {
            interests: interests.to_vec(),
            selected_payload_locator: None,
        });
    }
    let selected_payload_locator = selected_change_payload_locator(error)?;
    let current = interests
        .iter()
        .filter(|interest| {
            !matches!(
                interest,
                LogicalReadInterest::History { .. } | LogicalReadInterest::Diff { .. }
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    (!current.is_empty()).then_some(PartialDemandFulfillmentPlan {
        interests: current,
        selected_payload_locator: Some(selected_payload_locator),
    })
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
    /// Client-known idempotency key for this logical read operation. This is
    /// separate from the server's sealed-spool cursor so the client can clean
    /// up even when the first response never arrives.
    pub(crate) operation_id: String,
    pub(crate) release: bool,
    /// Fixed operation validity copied from the server-issued lease at request
    /// creation. Lease renewal must not extend a delayed operation or its
    /// cancellation/retirement identity.
    pub(crate) operation_expires_at_ms: u64,
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
    pub(crate) next_offset: usize,
    pub(crate) spool_id: String,
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
    /// A canonical changelog dependency selected by a descriptor-scoped
    /// candidate identity, before local result filtering. Its CHANGE_SPACE
    /// value is mutable, so its exact row lifetime and source owner travel
    /// with the typed input.
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
/// A bounded range of one canonical member. Whole-member identity and codec
/// validation happens against private staged bytes before any publication.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ReadInputFrame {
    pub(crate) address: ReadInputAddress,
    pub(crate) total_bytes: usize,
    pub(crate) offset: usize,
    pub(crate) digest: [u8; 32],
    #[serde(with = "payload")]
    pub(crate) bytes: Vec<u8>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DiscoveryProfile {
    pub(crate) storage_calls: usize,
    pub(crate) storage_keys: usize,
    pub(crate) peak_provider_bytes: usize,
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
    pub(crate) frame: Option<ReadInputFrame>,
    pub(crate) profile: DiscoveryProfile,
    pub(crate) closure_digest: String,
    pub(crate) continuation: Option<ReadContinuation>,
    pub(crate) outcome: ReadFulfillmentOutcome,
}

/// A successful, authenticated indication that bounded discovery declined.
/// Neither refusal grants coverage; the client still performs its original
/// native-demand read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReadFulfillmentOutcome {
    Complete,
    NativeFallback,
    OperationFallback,
}

const MAX_REQUEST_REFUSAL_MEMO: usize = 128;
static NATIVE_PROOF_REFUSAL_MEMO: OnceLock<Mutex<VecDeque<String>>> = OnceLock::new();
static OPERATION_REFUSAL_MEMO: OnceLock<Mutex<VecDeque<String>>> = OnceLock::new();

fn is_bounded_native_recipe(interest: &LogicalReadInterest) -> bool {
    matches!(interest, LogicalReadInterest::History { .. })
        || super::working_diff_recipe::is_supported_bounded_diff_recipe(interest)
}

fn native_proof_refusal_basis(
    request: &ReadFulfillmentRequest,
) -> Result<Option<String>, LixError> {
    if request.continuation.is_some() || !request.interests.iter().any(is_bounded_native_recipe) {
        return Ok(None);
    }
    let native_recipes = request
        .interests
        .iter()
        .filter(|interest| is_bounded_native_recipe(interest))
        .collect::<Vec<_>>();
    let basis = serde_json::json!({
        "epochId": &request.epoch_id,
        "descriptor": &request.descriptor,
        "nativeRecipes": native_recipes,
    });
    Ok(Some(
        blake3::hash(
            &serde_json::to_vec(&basis).map_err(|_| invalid("invalid native proof basis"))?,
        )
        .to_hex()
        .to_string(),
    ))
}

fn discovery_work_fallback_outcome(
    error_code: &str,
    observations_exhausted: bool,
) -> ReadFulfillmentOutcome {
    if !observations_exhausted
        && matches!(
            error_code,
            "LIX_HISTORY_RECIPE_FALLBACK"
                | super::working_diff_recipe::WORKING_DIFF_RECIPE_FALLBACK_CODE
                | crate::tracked_state::NATIVE_DIFF_RECIPE_WORK_BOUND_CODE
        )
    {
        ReadFulfillmentOutcome::NativeFallback
    } else {
        ReadFulfillmentOutcome::OperationFallback
    }
}

fn operation_refusal_basis(request: &ReadFulfillmentRequest) -> Result<Option<String>, LixError> {
    if request.continuation.is_some() || !request.interests.iter().any(is_bounded_native_recipe) {
        return Ok(None);
    }
    // Discovery bounds include current recipes and explicitly required inputs.
    // A declined operation must not poison a different closure.
    Ok(Some(request.digest()?))
}

/// Whether native proof or an exact request operation was declined. These are
/// work-avoidance memos, never evidence that a recipe is covered.
pub(crate) fn request_closure_is_ineligible(
    request: &ReadFulfillmentRequest,
) -> Result<bool, LixError> {
    let Some(operation_basis) = operation_refusal_basis(request)? else {
        return Ok(false);
    };
    let operation_memo = OPERATION_REFUSAL_MEMO.get_or_init(|| Mutex::new(VecDeque::new()));
    let operation_declined = operation_memo
        .lock()
        .map_err(|_| invalid("native recipe eligibility memo poisoned"))?
        .contains(&operation_basis);
    if operation_declined {
        return Ok(true);
    }
    let Some(proof_basis) = native_proof_refusal_basis(request)? else {
        return Ok(false);
    };
    let proof_memo = NATIVE_PROOF_REFUSAL_MEMO.get_or_init(|| Mutex::new(VecDeque::new()));
    Ok(proof_memo
        .lock()
        .map_err(|_| invalid("native recipe eligibility memo poisoned"))?
        .contains(&proof_basis))
}

/// Build the narrow current-row recovery request after bounded discovery has
/// declined. Only operation-local current
/// recipes remain, and the selected canonical locator is the sole required
/// input; private historical frontiers are never forwarded to authority.
pub(crate) fn current_payload_request_after_native_fallback(
    request: &ReadFulfillmentRequest,
    locator: &NativeMetadataRef,
) -> Option<ReadFulfillmentRequest> {
    if !matches!(locator, NativeMetadataRef::ChangeLocator(_))
        || !request.interests.iter().any(is_bounded_native_recipe)
    {
        return None;
    }
    let mut current = request.clone();
    current.interests.retain(|interest| {
        !matches!(
            interest,
            LogicalReadInterest::History { .. } | LogicalReadInterest::Diff { .. }
        )
    });
    if current.interests.is_empty() {
        return None;
    }
    current.required = vec![ReadInputAddress::Metadata(locator.clone())];
    current.continuation = None;
    current.operation_id = uuid::Uuid::now_v7().to_string();
    Some(current)
}

pub(crate) fn remember_request_closure_ineligible(
    request: &ReadFulfillmentRequest,
    outcome: ReadFulfillmentOutcome,
) -> Result<(), LixError> {
    let basis = match outcome {
        ReadFulfillmentOutcome::NativeFallback => native_proof_refusal_basis(request)?,
        ReadFulfillmentOutcome::OperationFallback => operation_refusal_basis(request)?,
        ReadFulfillmentOutcome::Complete => None,
    };
    let Some(basis) = basis else {
        return Ok(());
    };
    let memo = match outcome {
        ReadFulfillmentOutcome::NativeFallback => {
            NATIVE_PROOF_REFUSAL_MEMO.get_or_init(|| Mutex::new(VecDeque::new()))
        }
        ReadFulfillmentOutcome::OperationFallback => {
            OPERATION_REFUSAL_MEMO.get_or_init(|| Mutex::new(VecDeque::new()))
        }
        ReadFulfillmentOutcome::Complete => return Ok(()),
    };
    let mut memo = memo
        .lock()
        .map_err(|_| invalid("native recipe eligibility memo poisoned"))?;
    memo.retain(|existing| existing != &basis);
    memo.push_back(basis);
    while memo.len() > MAX_REQUEST_REFUSAL_MEMO {
        memo.pop_front();
    }
    Ok(())
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
        if uuid::Uuid::parse_str(&self.operation_id).is_err()
            || uuid::Uuid::parse_str(&self.epoch_id).is_err()
            || self.operation_expires_at_ms == 0
            || (self.release && self.continuation.is_some())
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
        super::working_diff_recipe::validate_bounded_diff_recipes(
            &self.interests,
            &self.descriptor.selected_branch.branch_id,
        )?;
        if !history_recipes_within_budget(&self.interests) {
            return Err(invalid("aggregate bounded-history recipe limit exceeded"));
        }
        for interest in &self.interests {
            if let LogicalReadInterest::History {
                branch_id,
                commit_ids,
                relation,
                filter,
                retain_payloads,
                projected_columns,
                limit,
                ..
            } = interest
            {
                if branch_id != &self.descriptor.selected_branch.branch_id
                    || limit.is_some()
                    || commit_ids.is_empty()
                    || commit_ids.len() > crate::hot_state::MAX_HISTORY_RECIPE_COMMIT_IDS
                    || !matches!(relation.as_str(), "lix_file" | "lix_directory")
                {
                    return Err(invalid("invalid bounded history recipe scope"));
                }
                let mut seen = BTreeSet::new();
                for commit_id in commit_ids {
                    let parsed = canonical_commit_id(commit_id)?;
                    if !parsed.has_canonical_text(commit_id) || !seen.insert(parsed) {
                        return Err(invalid("invalid bounded history commit selection"));
                    }
                }
                crate::sql2::validate_bounded_history_recipe_shape(
                    relation,
                    filter,
                    projected_columns,
                    *retain_payloads,
                )?;
            }
        }
        for interest in &self.interests {
            let selected_branch_id = self.descriptor.selected_branch.branch_id.as_str();
            let global_branch_id = self.descriptor.global_branch.branch_id.as_str();
            let branch_is_in_descriptor =
                |branch_id: &str| branch_id == selected_branch_id || branch_id == global_branch_id;
            let branch_scope_is_in_descriptor = match interest {
                LogicalReadInterest::FilesystemMetadata { branch_ids, .. }
                | LogicalReadInterest::FilesystemPaths { branch_ids, .. } => {
                    branch_ids.iter().all(|id| branch_is_in_descriptor(id))
                }
                LogicalReadInterest::FileContent { request, .. }
                | LogicalReadInterest::Scan { request, .. } => request
                    .filter
                    .branch_ids
                    .iter()
                    .all(|id| branch_is_in_descriptor(id)),
                LogicalReadInterest::CollectionGeneration { branch_id, .. }
                | LogicalReadInterest::PackedIdentityMembership { branch_id, .. } => {
                    branch_is_in_descriptor(branch_id)
                }
                LogicalReadInterest::Exact { rows, .. } => rows
                    .iter()
                    .all(|row| branch_is_in_descriptor(&row.branch_id)),
                LogicalReadInterest::Diff { branch_id, .. } => {
                    branch_id.as_deref().is_none_or(branch_is_in_descriptor)
                }
                LogicalReadInterest::History { branch_id, .. } => branch_id == selected_branch_id,
            };
            if !branch_scope_is_in_descriptor {
                return Err(invalid(
                    "read interest branch scope is outside the descriptor",
                ));
            }
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
            if (cursor.next_input == 0 && cursor.next_offset == 0)
                || cursor.next_offset >= MAX_INPUT_BYTES
                || cursor.next_input >= MAX_RECORDS
                || !valid_digest(&cursor.closure_digest)
                || uuid::Uuid::parse_str(&cursor.spool_id).is_err()
            {
                return Err(invalid("invalid read continuation"));
            }
        }
        Ok(())
    }
    fn digest(&self) -> Result<String, LixError> {
        let mut basis = self.clone();
        basis.continuation = None;
        basis.release = false;
        basis.operation_expires_at_ms = 0;
        // The operation id identifies an attempt, not the immutable recipe.
        // Keeping it out of this digest preserves semantic refusal memoization
        // and lets a bounded restart use a fresh operation id.
        basis.operation_id.clear();
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
    if id != value {
        return Err(invalid("change payload ID must be a canonical UUID"));
    }
    Ok(id)
}

fn canonical_commit_id(value: &str) -> Result<crate::changelog::CommitId, LixError> {
    let id = crate::changelog::CommitId::parse(value)
        .map_err(|_| invalid("change payload source must be a canonical UUID"))?;
    if id != value {
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
    values: BTreeMap<(StorageSpace, StorageKey), spool::PayloadRef>,
    payloads: Arc<Mutex<spool::PayloadSpool>>,
    profile: DiscoveryProfile,
    exhausted: bool,
}
impl Observations {
    fn charge_call(&mut self) -> Result<(), StorageError> {
        if self.profile.storage_calls >= MAX_READ_CALLS {
            self.exhausted = true;
            return Err(StorageError::Io(
                "read discovery work limit exceeded".into(),
            ));
        }
        self.profile.storage_calls += 1;
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
            self.exhausted = true;
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
                self.exhausted = true;
                return Err(StorageError::Io(
                    "read discovery payload limit exceeded".into(),
                ));
            }
            let payload = self
                .payloads
                .lock()
                .map_err(|_| StorageError::Io("operation spool poisoned".into()))?
                .append(bytes)
                .map_err(|error| {
                    self.exhausted = true;
                    StorageError::Io(error.to_string())
                })?;
            self.values.insert((space, key.clone()), payload);
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
        {
            let mut observations = self.observations.lock().unwrap();
            observations.charge_call()?;
            let keys = requests
                .iter()
                .map(|request| request.keys.len())
                .sum::<usize>();
            observations.profile.storage_keys =
                observations.profile.storage_keys.saturating_add(keys);
            if observations.profile.storage_keys > 4 * MAX_READ_CALLS {
                observations.exhausted = true;
                return Err(StorageError::Io(
                    "read discovery physical key budget exceeded".into(),
                ));
            }
        }
        let (result, peak_provider_bytes, pages) = collect_bounded_point_pages(
            &self.base,
            requests,
            DISCOVERY_READ_BUDGET,
            MAX_INPUT_BYTES,
            32,
        )
        .await
        .map_err(|error| {
            if matches!(error, StorageError::ReadBudgetExceeded { .. }) {
                self.observations.lock().unwrap().exhausted = true;
            }
            error
        })?;
        let mut values = result.values.iter();
        let mut observations = self.observations.lock().unwrap();
        for _ in 1..pages {
            observations.charge_call()?;
        }
        observations.profile.peak_provider_bytes = observations
            .profile
            .peak_provider_bytes
            .max(peak_provider_bytes);
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
    async fn get_many_bounded_prefix(
        &self,
        requests: &[StorageGetManyRequest<'_>],
        offset: usize,
        max_slots: usize,
        budget: ReadBudget,
    ) -> Result<GetManyPrefixResult, StorageError> {
        self.observations.lock().unwrap().charge_call()?;
        let budget = ReadBudget {
            max_result_bytes: budget
                .max_result_bytes
                .min(DISCOVERY_READ_BUDGET.max_result_bytes),
            max_single_value_bytes: budget
                .max_single_value_bytes
                .min(DISCOVERY_READ_BUDGET.max_single_value_bytes),
        };
        let result = self
            .base
            .get_many_bounded_prefix(requests, offset, max_slots.min(32), budget)
            .await?;
        budget.validate_result(&result.values)?;
        let (window, total) =
            bounded_prefix_requests(requests, offset, result.values.len().max(1))?;
        let next = offset
            .checked_add(result.values.len())
            .ok_or(StorageError::InvalidCursor)?;
        if result.values.len() > max_slots.min(32).min(MAX_SCAN_PAGE_ROWS)
            || next > total
            || result.next_offset != (next < total).then_some(next)
            || (result.values.is_empty() && next < total)
        {
            return Err(StorageError::InvalidCursor);
        }
        let mut observations = self.observations.lock().unwrap();
        observations.profile.storage_keys = observations
            .profile
            .storage_keys
            .saturating_add(result.values.len());
        if observations.profile.storage_keys > 4 * MAX_READ_CALLS {
            observations.exhausted = true;
            return Err(StorageError::ReadBudgetExceeded { singleton: false });
        }
        let bytes = result
            .values
            .iter()
            .flatten()
            .map(|value| match value {
                StorageProjectedValue::FullValue(bytes) => bytes.len(),
                StorageProjectedValue::KeyOnly => 0,
            })
            .sum();
        observations.profile.peak_provider_bytes =
            observations.profile.peak_provider_bytes.max(bytes);
        for ((space, key), value) in window
            .iter()
            .flat_map(|request| request.keys.iter().map(move |key| (request.space, key)))
            .zip(&result.values)
        {
            if let Some(value) = value {
                observations.observe(space, key, value)?;
            }
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
                .next_page_bounded(limit.min(MAX_SCAN_PAGE_ROWS), DISCOVERY_READ_BUDGET)
                .await
                .map_err(|error| {
                    if matches!(error, StorageError::ReadBudgetExceeded { .. }) {
                        self.observations.lock().unwrap().exhausted = true;
                    }
                    error
                })?
                .into_parts();
            let mut observations = self.observations.lock().unwrap();
            observations.profile.storage_keys =
                observations.profile.storage_keys.saturating_add(rows.len());
            let provider_bytes = rows
                .iter()
                .map(|row| match &row.value {
                    StorageProjectedValue::FullValue(bytes) => bytes.len(),
                    StorageProjectedValue::KeyOnly => 0,
                })
                .sum::<usize>();
            observations.profile.peak_provider_bytes =
                observations.profile.peak_provider_bytes.max(provider_bytes);
            if observations.profile.storage_keys > 4 * MAX_READ_CALLS {
                observations.exhausted = true;
                return Err(StorageError::Io(
                    "read discovery physical row budget exceeded".into(),
                ));
            }
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
    fn get_many_bounded<'a>(
        &'a self,
        requests: &'a [StorageGetManyRequest<'a>],
        budget: ReadBudget,
    ) -> futures_util::future::BoxFuture<'a, Result<StorageGetManyResult, StorageError>>;
    fn get_many_bounded_prefix<'a>(
        &'a self,
        requests: &'a [StorageGetManyRequest<'a>],
        offset: usize,
        max_slots: usize,
        budget: ReadBudget,
    ) -> futures_util::future::BoxFuture<'a, Result<GetManyPrefixResult, StorageError>>;
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
    fn get_many_bounded<'a>(
        &'a self,
        requests: &'a [StorageGetManyRequest<'a>],
        budget: ReadBudget,
    ) -> futures_util::future::BoxFuture<'a, Result<StorageGetManyResult, StorageError>> {
        Box::pin(StorageAdapterRead::get_many_bounded(
            &self.0, requests, budget,
        ))
    }
    fn get_many_bounded_prefix<'a>(
        &'a self,
        requests: &'a [StorageGetManyRequest<'a>],
        offset: usize,
        max_slots: usize,
        budget: ReadBudget,
    ) -> futures_util::future::BoxFuture<'a, Result<GetManyPrefixResult, StorageError>> {
        Box::pin(StorageAdapterRead::get_many_bounded_prefix(
            &self.0, requests, offset, max_slots, budget,
        ))
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
    async fn get_many_bounded(
        &self,
        requests: &[StorageGetManyRequest<'_>],
        budget: ReadBudget,
    ) -> Result<StorageGetManyResult, StorageError> {
        self.0.get_many_bounded(requests, budget).await
    }
    async fn get_many_bounded_prefix(
        &self,
        requests: &[StorageGetManyRequest<'_>],
        offset: usize,
        max_slots: usize,
        budget: ReadBudget,
    ) -> Result<GetManyPrefixResult, StorageError> {
        self.0
            .get_many_bounded_prefix(requests, offset, max_slots, budget)
            .await
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
    let observations = Arc::new(Mutex::new(Observations::default()));
    let result = discover_bounded_with_read(
        base,
        repository,
        account,
        lease_id,
        request,
        hot,
        observations.clone(),
    )
    .await;
    let (exhausted, profile) = {
        let observations = observations
            .lock()
            .map_err(|_| invalid("dependency observations poisoned"))?;
        (observations.exhausted, observations.profile.clone())
    };
    match result {
        Err(error)
            if request.continuation.is_none()
                && request.interests.iter().any(is_bounded_native_recipe)
                && (exhausted
                    || error.code == "LIX_NATIVE_RECIPE_WORK_BOUND"
                    || error.code == crate::tracked_state::NATIVE_DIFF_RECIPE_WORK_BOUND_CODE) =>
        {
            let outcome = discovery_work_fallback_outcome(&error.code, exhausted);
            fallback_response(repository, request, outcome, profile)
        }
        result => result,
    }
}

fn fallback_response(
    repository: &str,
    request: &ReadFulfillmentRequest,
    outcome: ReadFulfillmentOutcome,
    profile: DiscoveryProfile,
) -> Result<ReadFulfillmentResponse, LixError> {
    if !matches!(
        outcome,
        ReadFulfillmentOutcome::NativeFallback | ReadFulfillmentOutcome::OperationFallback
    ) {
        return Err(invalid("invalid read fulfillment fallback outcome"));
    }
    if request.continuation.is_some() {
        return Err(invalid(
            "fallback is unavailable for paged read fulfillment",
        ));
    }
    let inputs = Vec::new();
    Ok(ReadFulfillmentResponse {
        frame: None,
        lix_id: repository.into(),
        epoch_id: request.epoch_id.clone(),
        request_digest: request.digest()?,
        closure_digest: input_digest(request, &inputs)?,
        inputs,
        profile,
        continuation: None,
        outcome,
    })
}

#[cfg(test)]
fn native_fallback_response(
    repository: &str,
    request: &ReadFulfillmentRequest,
    profile: DiscoveryProfile,
) -> Result<ReadFulfillmentResponse, LixError> {
    fallback_response(
        repository,
        request,
        ReadFulfillmentOutcome::NativeFallback,
        profile,
    )
}

async fn discover_bounded_with_read(
    base: RecipeRead,
    repository: &str,
    account: &str,
    lease_id: &str,
    request: &ReadFulfillmentRequest,
    hot: crate::hot_state::HotStateContext,
    observations: Arc<Mutex<Observations>>,
) -> Result<ReadFulfillmentResponse, LixError> {
    request.validate(repository)?;
    let lease = crate::gc::require_native_baseline_lease(
        &base,
        lease_id,
        account,
        crate::telemetry::unix_time_ms(),
    )
    .await?;
    let now_ms = crate::telemetry::unix_time_ms();
    if request.operation_expires_at_ms <= now_ms {
        return Err(LixError::new(
            "LIX_READ_FULFILLMENT_RESTART",
            "read operation validity expired",
        ));
    }
    if request.operation_expires_at_ms > lease.expires_at_ms {
        return Err(invalid(
            "read operation expiry exceeds authenticated lease expiry",
        ));
    }
    lease.validate_for_roots(
        account,
        &super::leased_descriptor::descriptor_roots(&request.descriptor)?,
    )?;
    if request.release {
        spool::release(repository, account, lease_id, lease.expires_at_ms, request)?;
        return Ok(ReadFulfillmentResponse {
            lix_id: repository.into(),
            epoch_id: request.epoch_id.clone(),
            request_digest: request.digest()?,
            inputs: Vec::new(),
            frame: None,
            profile: DiscoveryProfile::default(),
            closure_digest: input_digest(request, &[])?,
            continuation: None,
            outcome: ReadFulfillmentOutcome::Complete,
        });
    }
    if request.continuation.is_some() {
        return spool::continuation_page(repository, account, lease_id, request);
    }
    let mut operation =
        match spool::begin(repository, account, lease_id, lease.expires_at_ms, request).await? {
            spool::BeginOperation::Owner(operation) => operation,
            spool::BeginOperation::Replay(response) => return Ok(response),
        };
    let read = DependencyRead {
        base: base.clone(),
        observations: observations.clone(),
    };
    let blobs = Arc::new(BlobReadCapture::default());
    let payloads = observations
        .lock()
        .map_err(|_| invalid("dependency observations poisoned"))?
        .payloads
        .clone();
    operation.bind_payload_spool(&payloads)?;
    let logical_spool = Arc::new(Mutex::new(spool::InputSpool::new(payloads.clone())));
    let mut required_locator_ids = Vec::new();
    for address in &request.required {
        if let ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(id)) = address {
            let commit = crate::changelog::CommitId::parse(id)
                .map_err(|_| invalid("native metadata ID must be a canonical UUID"))?;
            required_locator_ids.push(crate::changelog::ChangeId::new(*commit.as_uuid()));
        }
    }
    let required_locators = crate::tracked_state::load_canonical_change_locators(
        &read,
        &required_locator_ids,
    )
    .await?;
    let mut required_locators = required_locators.into_iter();
    let mut required_index = 0usize;
    while required_index < request.required.len() {
        if matches!(
            &request.required[required_index],
            ReadInputAddress::BlobChunk(_)
        ) {
            let run_start = required_index;
            while required_index < request.required.len()
                && matches!(
                    &request.required[required_index],
                    ReadInputAddress::BlobChunk(_)
                )
            {
                required_index += 1;
            }
            let addresses = &request.required[run_start..required_index];
            append_required_blob_chunk_run(&read, addresses, &logical_spool).await?;
            continue;
        }
        let address = &request.required[required_index];
        required_index += 1;
        let (space, key) = address.coordinate()?;
        // Change locators are logical metadata addresses. Directly authored
        // changes intentionally have no locator row at this key; use the same
        // canonical authority resolver as the native-metadata endpoint.
        let is_change_locator = matches!(
            address,
            ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(_))
        );
        let value = if matches!(
            address,
            ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(_))
        ) {
            required_locators
                .next()
                .flatten()
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
    let logical_inputs =
        match super::read_interest_prepare::prepare_native_read_interests_authority(
            scoped,
            &request.descriptor,
            &interests,
            account,
            hot,
            blobs.clone(),
            {
                let logical_spool = logical_spool.clone();
                Arc::new(move |input| {
                    logical_spool
                        .lock()
                        .map_err(|_| invalid("logical spool poisoned"))?
                        .append(input)
                })
            },
        )
        .await
        {
            Ok(inputs) => inputs,
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "LIX_HISTORY_RECIPE_FALLBACK"
                        | super::working_diff_recipe::WORKING_DIFF_RECIPE_FALLBACK_CODE
                        | crate::tracked_state::NATIVE_DIFF_RECIPE_WORK_BOUND_CODE
                ) && request.interests.iter().any(is_bounded_native_recipe) =>
            {
                let (exhausted, profile) = {
                    let observations = observations
                        .lock()
                        .map_err(|_| invalid("dependency observations poisoned"))?;
                    (observations.exhausted, observations.profile.clone())
                };
                let outcome = discovery_work_fallback_outcome(&error.code, exhausted);
                return fallback_response(repository, request, outcome, profile);
            }
            Err(error) => return Err(error),
        };
    // Logical selected rows were emitted incrementally, while physical
    // observations retain only compact references into the same payload file.
    debug_assert!(logical_inputs.is_empty());
    let mut inputs = Arc::try_unwrap(logical_spool)
        .map_err(|_| invalid("logical operation sink still retained"))?
        .into_inner()
        .map_err(|_| invalid("logical spool poisoned"))?;
    let mut typed = BTreeSet::new();
    loop {
        let pending = observations
            .lock()
            .map_err(|_| invalid("dependency observations poisoned"))?
            .values
            .iter()
            .filter(|(coordinate, _)| !typed.contains(*coordinate))
            .map(|(coordinate, payload)| (coordinate.clone(), *payload))
            .collect::<Vec<_>>();
        if pending.is_empty() {
            break;
        }
        for ((space, key), payload) in pending {
            let bytes = payloads
                .lock()
                .map_err(|_| invalid("operation spool poisoned"))?
                .read(payload)?;
            let input = typed_input(&read, space, &key.0, Bytes::from(bytes)).await?;
            inputs.append(input)?;
            typed.insert((space, key));
        }
    }
    export_blob_inputs(&read, &blobs, &mut inputs).await?;
    validate_spooled_complete(request, &inputs)?;
    let mut profile = observations
        .lock()
        .map_err(|_| invalid("read discovery accounting poisoned"))?
        .profile
        .clone();
    profile.payload_bytes = inputs.payload_bytes;
    let sealing_lease = crate::gc::require_native_baseline_lease(
        &base,
        lease_id,
        account,
        crate::telemetry::unix_time_ms(),
    )
    .await?;
    sealing_lease.validate_for_roots(
        account,
        &super::leased_descriptor::descriptor_roots(&request.descriptor)?,
    )?;
    let response = spool::seal_and_page(
        inputs,
        repository,
        account,
        lease_id,
        request.operation_expires_at_ms,
        request,
        profile,
        &mut operation,
    )?;
    validate_response(request, &response)?;
    Ok(response)
}

async fn append_required_blob_chunk_run(
    read: &impl StorageAdapterRead,
    addresses: &[ReadInputAddress],
    logical_spool: &Arc<Mutex<spool::InputSpool>>,
) -> Result<(), LixError> {
    if addresses.is_empty() || addresses.len() > 32 {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "required blob chunk run exceeds its validated bounds",
        ));
    }
    let chunk_ids = addresses
        .iter()
        .map(|address| match address {
            ReadInputAddress::BlobChunk(hash) => {
                Ok(crate::binary_cas::ChunkHash::from_bytes(*hash))
            }
            _ => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "required blob chunk run contains another input kind",
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let logical_spool = logical_spool.clone();
    crate::binary_cas::visit_verified_raw_chunks(read, &chunk_ids, |index, chunk_id, payload| {
        let address = addresses.get(index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "verified chunk visitor returned an invalid request index",
            )
        })?;
        let ReadInputAddress::BlobChunk(expected_hash) = address else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "verified chunk visitor crossed a required-input boundary",
            ));
        };
        if expected_hash != chunk_id.as_bytes() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "verified chunk visitor changed request ordering",
            ));
        }
        let Some(payload) = payload else {
            return Err(invalid("authority lacks required chunk"));
        };
        logical_spool
            .lock()
            .map_err(|_| invalid("logical spool poisoned"))?
            .append_borrowed(address.clone(), payload)
    })
    .await
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
    if request.release {
        if request.continuation.is_some()
            || !response.inputs.is_empty()
            || response.frame.is_some()
            || response.continuation.is_some()
            || response.outcome != ReadFulfillmentOutcome::Complete
            || response.closure_digest != input_digest(request, &[])?
        {
            return Err(invalid(
                "invalid read operation cancellation acknowledgement",
            ));
        }
        return Ok(());
    }
    if response.outcome != ReadFulfillmentOutcome::Complete {
        if !request.interests.iter().any(is_bounded_native_recipe)
            || request.continuation.is_some()
            || !response.inputs.is_empty()
            || response.frame.is_some()
            || response.continuation.is_some()
            || input_digest(request, &response.inputs)? != response.closure_digest
        {
            return Err(invalid("invalid bounded-native fallback page"));
        }
        return Ok(());
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
    if bytes > PAGE_PAYLOAD_BYTES {
        return Err(invalid("read page exceeds byte bound"));
    }
    let (start, offset) = request
        .continuation
        .as_ref()
        .map_or((0, 0), |cursor| (cursor.next_input, cursor.next_offset));
    let (next_input, next_offset) = if let Some(frame) = &response.frame {
        if !response.inputs.is_empty()
            || frame.total_bytes <= PAGE_PAYLOAD_BYTES
            || frame.total_bytes > MAX_INPUT_BYTES
            || frame.offset != offset
            || frame.bytes.is_empty()
            || frame.bytes.len() > PAGE_PAYLOAD_BYTES
            || frame.offset.saturating_add(frame.bytes.len()) > frame.total_bytes
        {
            return Err(invalid("invalid bounded read frame"));
        }
        frame.address.coordinate()?;
        let end = frame.offset + frame.bytes.len();
        if end == frame.total_bytes {
            (start + 1, 0)
        } else {
            (start, end)
        }
    } else {
        if offset != 0 {
            return Err(invalid(
                "read frame sequence ended before member completion",
            ));
        }
        (start + response.inputs.len(), 0)
    };
    if request
        .continuation
        .as_ref()
        .is_some_and(|cursor| cursor.closure_digest != response.closure_digest)
    {
        return Err(invalid("read continuation changed its closure"));
    }
    if let Some(next) = &response.continuation {
        if (next_input, next_offset) <= (start, offset)
            || (next.next_input, next.next_offset) != (next_input, next_offset)
            || next.next_input >= MAX_RECORDS
            || next.closure_digest != response.closure_digest
            || uuid::Uuid::parse_str(&next.spool_id).is_err()
            || request
                .continuation
                .as_ref()
                .is_some_and(|cursor| cursor.spool_id != next.spool_id)
        {
            return Err(invalid("read continuation did not advance exactly"));
        }
    } else if next_offset != 0 {
        return Err(invalid("terminal read page contains an incomplete member"));
    }
    Ok(())
}
/// Authorize canonical dependencies within a scan's physical candidate scope.
/// A read recipe prepares native inputs, not SQL result rows: indexed equality
/// and range probes may return stale candidates, or fall back to a schema scan
/// when their index is incomplete. Visibility also reads global rows and
/// tombstones before applying its output filters and limit. Local evaluation
/// must retain those predicates; applying them here would reject the inputs it
/// needs to decide the result.
///
/// An empty `row_pks` list is a wildcard, but schema, branch/global overlay,
/// file, and primary-key bounds still fence every canonical candidate. The
/// descriptor and closure byte/count bounds are validated separately. A
/// matching identity never proves membership in a limited or filtered result.
fn scan_selects_change_candidate_identity(
    scan: &crate::hot_state::HotStateScanRequest,
    domain: InterestDomain,
    branch_id: &str,
    schema_key: &str,
    file_id: Option<&str>,
    row_pk: &crate::row_pk::RowPk,
) -> bool {
    let filter = &scan.filter;
    if domain == InterestDomain::Untracked
        || filter.untracked == Some(true)
        || filter.rows != crate::hot_state::HotStateRowFilter::All
        || !filter
            .schema_keys
            .iter()
            .any(|candidate| candidate == schema_key)
        || !branch_is_selected_by_scan(&filter.branch_ids, branch_id)
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

    // These inputs can be needed before residual predicates, branch/global
    // visibility, tombstone removal, or an output limit are evaluated. Their
    // predicates do not narrow the physical candidate dependency scope.
    true
}

/// Branch-scoped scans physically read global rows as candidates for the
/// branch/global visibility overlay. A canonical payload may therefore name
/// the global source branch even though the logical recipe names only the
/// selected branch. Keep the exception tied to that implicit global read.
fn branch_is_selected_by_scan(branch_ids: &[String], branch_id: &str) -> bool {
    branch_ids.iter().any(|candidate| candidate == branch_id)
        || (branch_id == crate::GLOBAL_BRANCH_ID
            && branch_ids
                .iter()
                .any(|candidate| candidate != crate::GLOBAL_BRANCH_ID))
}

fn scan_recipe_selects_change_candidate_identity(
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
        LogicalReadInterest::Scan { request, domain } => scan_selects_change_candidate_identity(
            request, *domain, branch_id, schema_key, file_id, row_pk,
        ),
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
                crate::filesystem::FilesystemPathIndexScope::DirectoriesOnly
            } else {
                file_ids.clone().map_or(
                    crate::filesystem::FilesystemPathIndexScope::All,
                    crate::filesystem::FilesystemPathIndexScope::FileIds,
                )
            };
            scan_recipe_selects_change_candidate_identity(
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
            if !scan_selects_change_candidate_identity(
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
        // Historical recipes currently authorize immutable native inputs
        // only. They do not mint mutable CHANGE_SPACE row-selection proofs.
        LogicalReadInterest::History { .. } => false,
        _ => false,
    }
}

/// Current tracked-row reads prepare the branch plugin registry as a native
/// executable dependency. The registry itself is not part of a custom schema
/// recipe's filter, so authorize only its reserved fileless identity when the
/// same recipe also selected another canonical row in that branch.
fn plugin_registry_dependency_matches(
    interest: &LogicalReadInterest,
    dependency: &ReadInput,
    inputs: &[ReadInput],
) -> bool {
    use crate::plugin::runtime::PLUGIN_REGISTRY_KEY;

    let ReadInputAddress::ChangeRecord {
        branch_id,
        schema_key,
        file_id,
        row_pk,
        ..
    } = &dependency.address
    else {
        return false;
    };
    if schema_key != "lix_key_value"
        || file_id.is_some()
        || row_pk.as_single_string().ok() != Some(PLUGIN_REGISTRY_KEY)
        || !matches!(
            interest,
            LogicalReadInterest::Scan { .. }
                | LogicalReadInterest::Exact { .. }
                | LogicalReadInterest::FilesystemMetadata { .. }
                | LogicalReadInterest::FilesystemPaths { .. }
        )
    {
        return false;
    }

    inputs.iter().any(|input| {
        let ReadInputAddress::ChangeRecord {
            branch_id: selected_branch_id,
            schema_key: selected_schema_key,
            file_id: selected_file_id,
            row_pk: selected_row_pk,
            ..
        } = &input.address
        else {
            return false;
        };
        if selected_branch_id != branch_id
            || (selected_schema_key == "lix_key_value"
                && selected_row_pk.as_single_string().ok() == Some(PLUGIN_REGISTRY_KEY))
        {
            return false;
        }
        scan_recipe_selects_change_candidate_identity(
            interest,
            selected_branch_id,
            selected_schema_key,
            selected_file_id.as_deref(),
            selected_row_pk,
        )
    })
}

/// Exact executable-owner reads accompany native file rows, including the
/// file descriptors used by metadata/path replay. Admit only the reserved,
/// canonically decoded owner of a separately recipe-selected same-branch file.
fn plugin_owner_dependency_matches(
    interest: &LogicalReadInterest,
    dependency: &ReadInput,
    inputs: &[ReadInput],
) -> bool {
    use crate::plugin::runtime::{PLUGIN_OWNER_KEY, PluginFileOwner};
    let ReadInputAddress::ChangeRecord {
        branch_id,
        schema_key,
        file_id: Some(file_id),
        row_pk,
        ..
    } = &dependency.address
    else {
        return false;
    };
    if schema_key != "lix_key_value" || row_pk.as_single_string().ok() != Some(PLUGIN_OWNER_KEY) {
        return false;
    }
    let Some(row) = change_payload_row(dependency) else {
        return false;
    };
    if !row.deleted {
        let Some(snapshot) = row
            .snapshot_content
            .as_deref()
            .and_then(|snapshot| serde_json::from_str::<serde_json::Value>(snapshot).ok())
        else {
            return false;
        };
        if PluginFileOwner::from_snapshot(file_id, &snapshot).is_err() {
            return false;
        }
    }
    let Ok(file_pk) = crate::row_pk::RowPk::uuid_from_canonical(file_id) else {
        return false;
    };
    inputs.iter().any(|input| {
        let ReadInputAddress::ChangeRecord {
            branch_id: selected_branch,
            schema_key: selected_schema,
            file_id: selected_file,
            row_pk: selected_pk,
            ..
        } = &input.address
        else {
            return false;
        };
        if selected_branch != branch_id
            || (selected_schema == "lix_key_value"
                && selected_pk.as_single_string().ok() == Some(PLUGIN_OWNER_KEY))
        {
            return false;
        }
        let same_file = match selected_schema.as_str() {
            "lix_file_descriptor" | "lix_binary_blob_ref" => {
                selected_pk == &file_pk
                    && selected_file.as_deref().is_none_or(|file| file == file_id)
            }
            "lix_directory_descriptor" => false,
            _ => selected_file.as_deref() == Some(file_id.as_str()),
        };
        same_file
            && scan_recipe_selects_change_candidate_identity(
                interest,
                selected_branch,
                selected_schema,
                selected_file.as_deref(),
                selected_pk,
            )
    })
}

#[derive(Default)]
struct ReadFulfillmentPayloadContext {
    filesystem_path_rows: Vec<crate::hot_state::MaterializedHotStateRow>,
    plugin_owner_schemas: BTreeMap<(String, String), BTreeSet<String>>,
}

#[derive(Default)]
struct FileContentPayloadSelection {
    file_ids: BTreeSet<String>,
    path_change_ids: BTreeSet<crate::changelog::ChangeId>,
}

impl ReadFulfillmentPayloadContext {
    fn new(inputs: &[ReadInput]) -> Result<Self, LixError> {
        use crate::plugin::runtime::PLUGIN_OWNER_KEY;

        let mut path_rows = Vec::new();
        let mut plugin_owner_schemas = BTreeMap::new();
        let mut path_row_identities = BTreeSet::new();
        let mut plugin_owner_identities = BTreeSet::new();
        for input in inputs {
            let ReadInputAddress::ChangeRecord {
                branch_id,
                schema_key,
                file_id,
                row_pk,
                ..
            } = &input.address
            else {
                continue;
            };
            if !matches!(
                schema_key.as_str(),
                "lix_file_descriptor" | "lix_directory_descriptor" | "lix_binary_blob_ref"
            ) && !(schema_key == "lix_key_value"
                && row_pk.as_single_string().ok() == Some(PLUGIN_OWNER_KEY))
            {
                continue;
            }
            let row = change_payload_row(input).ok_or_else(|| {
                invalid("read fulfillment contains an invalid filesystem or plugin owner row")
            })?;
            if matches!(
                schema_key.as_str(),
                "lix_file_descriptor" | "lix_directory_descriptor" | "lix_binary_blob_ref"
            ) {
                let identity = (
                    branch_id.clone(),
                    schema_key.clone(),
                    row_pk.clone(),
                    file_id.clone(),
                );
                if !path_row_identities.insert(identity) {
                    return Err(invalid(
                        "read fulfillment contains duplicate filesystem row identities",
                    ));
                }
                path_rows.push(row.clone());
            }
            if schema_key == "lix_key_value"
                && row_pk.as_single_string().ok() == Some(PLUGIN_OWNER_KEY)
                && !row.deleted
            {
                let file_id = file_id
                    .as_deref()
                    .ok_or_else(|| invalid("plugin owner row is missing its file identity"))?;
                let snapshot = row
                    .snapshot_content
                    .as_deref()
                    .and_then(|snapshot| serde_json::from_str::<serde_json::Value>(snapshot).ok())
                    .ok_or_else(|| invalid("plugin owner row has an invalid snapshot"))?;
                let owner =
                    crate::plugin::runtime::PluginFileOwner::from_snapshot(file_id, &snapshot)
                        .map_err(|_| invalid("plugin owner row failed canonical validation"))?;
                if !plugin_owner_identities.insert((branch_id.clone(), owner.file_id().to_owned()))
                {
                    return Err(invalid(
                        "read fulfillment contains duplicate plugin owner identities",
                    ));
                }
                plugin_owner_schemas.insert(
                    (branch_id.clone(), owner.file_id().to_owned()),
                    owner.schema_keys().iter().cloned().collect(),
                );
            }
        }
        Ok(Self {
            filesystem_path_rows: path_rows,
            plugin_owner_schemas,
        })
    }

    fn selected_files(&self, interest: &LogicalReadInterest) -> FileContentPayloadSelection {
        let LogicalReadInterest::FileContent {
            request,
            file_ids,
            directory_ids,
            root_directory,
            path_predicate,
            ..
        } = interest
        else {
            return FileContentPayloadSelection::default();
        };
        if file_ids.as_ref().is_some_and(Vec::is_empty)
            || directory_ids.as_ref().is_some_and(Vec::is_empty)
        {
            return FileContentPayloadSelection::default();
        }
        let mut selected = FileContentPayloadSelection::default();
        if !self.filesystem_path_rows.is_empty() {
            let visible_path_rows = crate::hot_state::resolve_visible_batch(
                crate::hot_state::MaterializedHotStateBatch::from_rows(
                    self.filesystem_path_rows.clone(),
                ),
                crate::hot_state::MaterializedHotStateBatch::default(),
                &crate::hot_state::VisibilityRequest {
                    branch_scope: crate::hot_state::VisibilityBranchScope::BranchIds {
                        branch_ids: request.filter.branch_ids.clone(),
                    },
                    include_tombstones: false,
                    limit: None,
                },
            );
            let index =
                crate::filesystem::FilesystemPathIndex::from_live_batch(&visible_path_rows).ok();
            if let Some(index) = index {
                let entries = index.entries();
                let directories = entries
                    .iter()
                    .filter(|entry| entry.kind == crate::filesystem::FilesystemPathKind::Directory)
                    .map(|entry| {
                        (
                            (entry.live_row().branch_id.clone(), entry.id().to_owned()),
                            entry,
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                for entry in &entries {
                    if entry.kind != crate::filesystem::FilesystemPathKind::File
                        || !file_ids
                            .as_ref()
                            .is_none_or(|ids| ids.iter().any(|id| id == entry.id()))
                        || (directory_ids.as_ref().is_some_and(|ids| {
                            entry
                                .parent_id
                                .as_ref()
                                .is_none_or(|parent| !ids.iter().any(|id| id == parent))
                        }))
                        || (*root_directory && entry.parent_id.is_some())
                        || !file_path_interest_matches(path_predicate, &entry.path)
                    {
                        continue;
                    }
                    let row = entry.live_row();
                    if scan_selects_change_candidate_identity(
                        request,
                        InterestDomain::Combined,
                        &row.branch_id,
                        &row.schema_key,
                        row.file_id.as_deref(),
                        &row.row_pk,
                    ) {
                        selected.file_ids.insert(entry.id().to_owned());
                        let selected_row = entry.live_row();
                        if let Some(change_id) = selected_row.change_id {
                            selected.path_change_ids.insert(change_id);
                        }
                        if let Some(blob_row) = entry.blob_ref_live_row()
                            && let Some(change_id) = blob_row.change_id
                        {
                            selected.path_change_ids.insert(change_id);
                        }
                        let mut parent_id = entry.parent_id.clone();
                        while let Some(directory_id) = parent_id {
                            let Some(directory) = directories
                                .get(&(selected_row.branch_id.clone(), directory_id.clone()))
                            else {
                                break;
                            };
                            if let Some(change_id) = directory.live_row().change_id {
                                selected.path_change_ids.insert(change_id);
                            }
                            parent_id = directory.parent_id.clone();
                        }
                    }
                }
                return selected;
            }
        }

        // An explicit file-id recipe can still prove the identity without
        // reconstructing a path index. Path and directory predicates require
        // descriptor evidence, so they fail closed when that evidence is not
        // in this closure.
        if *root_directory
            || directory_ids.is_some()
            || !matches!(path_predicate, crate::hot_state::FilePathInterest::All)
        {
            return selected;
        }
        if let Some(file_ids) = file_ids {
            selected.file_ids.extend(file_ids.iter().cloned());
        } else {
            selected
                .file_ids
                .extend(
                    request
                        .filter
                        .file_ids
                        .iter()
                        .filter_map(|candidate| match candidate {
                            crate::NullableKeyFilter::Value(file_id) => Some(file_id.clone()),
                            crate::NullableKeyFilter::Any | crate::NullableKeyFilter::Null => None,
                        }),
                );
        }
        selected
    }
}

fn change_payload_row(input: &ReadInput) -> Option<crate::hot_state::MaterializedHotStateRow> {
    let ReadInputAddress::ChangeRecord {
        change_id,
        source_commit_id,
        branch_id,
        schema_key,
        file_id,
        row_pk,
        updated_at,
        ..
    } = &input.address
    else {
        return None;
    };
    let change_id = canonical_change_id(change_id).ok()?;
    let source_commit_id = canonical_commit_id(source_commit_id).ok()?;
    let updated_at = canonical_timestamp(updated_at).ok()?;
    let record = crate::changelog::decode_change_record(&input.bytes, change_id).ok()?;
    let deleted = record.snapshot.is_none();
    let snapshot_content = match record.snapshot {
        Some(snapshot) => Some(
            crate::row_payload::TypedRow::decode_durable_payload(
                Arc::from(snapshot),
                schema_key,
                row_pk,
            )
            .ok()?
            .to_json_shared()
            .ok()?,
        ),
        None => None,
    };
    Some(crate::hot_state::MaterializedHotStateRow {
        row_pk: row_pk.clone(),
        schema_key: schema_key.clone(),
        file_id: file_id.clone(),
        snapshot_content,
        metadata: record.metadata.map(|metadata| metadata.to_string().into()),
        deleted,
        created_at: record.created_at,
        updated_at,
        global: branch_id == crate::GLOBAL_BRANCH_ID,
        change_id: Some(change_id),
        author_id: record.account_id,
        commit_id: Some(source_commit_id),
        untracked: false,
        branch_id: branch_id.clone().into(),
    })
}

fn file_path_interest_matches(predicate: &crate::hot_state::FilePathInterest, path: &str) -> bool {
    use crate::hot_state::{
        FilePathInterest as Interest, FilePathInterestComparison as Comparison,
    };
    match predicate {
        Interest::All => true,
        Interest::Comparison { operation, value } => match operation {
            Comparison::Equal => path == value,
            Comparison::LessThan => path < value.as_str(),
            Comparison::LessThanOrEqual => path <= value.as_str(),
            Comparison::GreaterThan => path > value.as_str(),
            Comparison::GreaterThanOrEqual => path >= value.as_str(),
        },
        Interest::In { values } => values.iter().any(|value| value == path),
        Interest::LowercaseContains { value } => path.to_lowercase().contains(value),
        Interest::And { left, right } => {
            file_path_interest_matches(left, path) && file_path_interest_matches(right, path)
        }
        Interest::Or { left, right } => {
            file_path_interest_matches(left, path) || file_path_interest_matches(right, path)
        }
    }
}

/// File-content reads also prepare the selected file's plugin owner and
/// plugin-declared row schemas. The owner record is the authority for which
/// non-native schemas belong to a file; a shared branch scan alone cannot
/// admit unrelated rows.
#[cfg(test)]
fn file_content_recipe_selects_change_identity(
    interest: &LogicalReadInterest,
    inputs: &[ReadInput],
    branch_id: &str,
    schema_key: &str,
    file_id: Option<&str>,
    row_pk: &crate::row_pk::RowPk,
) -> bool {
    let Ok(context) = ReadFulfillmentPayloadContext::new(inputs) else {
        return false;
    };
    let selected_files = context.selected_files(interest);
    file_content_recipe_selects_change_identity_with_context(
        interest,
        &context,
        &selected_files,
        None,
        branch_id,
        schema_key,
        file_id,
        row_pk,
    )
}

fn file_content_recipe_selects_change_identity_with_context(
    interest: &LogicalReadInterest,
    context: &ReadFulfillmentPayloadContext,
    selected_files: &FileContentPayloadSelection,
    change_id: Option<crate::changelog::ChangeId>,
    branch_id: &str,
    schema_key: &str,
    file_id: Option<&str>,
    row_pk: &crate::row_pk::RowPk,
) -> bool {
    use crate::plugin::runtime::{PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY};

    let LogicalReadInterest::FileContent {
        request,
        directory_ids,
        root_directory,
        path_predicate,
        ..
    } = interest
    else {
        return false;
    };
    if !branch_is_selected_by_scan(&request.filter.branch_ids, branch_id) {
        return false;
    }

    if schema_key == "lix_key_value" {
        match row_pk.as_single_string().ok() {
            Some(PLUGIN_REGISTRY_KEY) => {
                return file_id.is_none()
                    && request
                        .filter
                        .schema_keys
                        .iter()
                        .any(|schema| schema == "lix_file_descriptor");
            }
            Some(PLUGIN_OWNER_KEY) => {
                return file_id.is_some_and(|file_id| selected_files.file_ids.contains(file_id));
            }
            _ => return false,
        }
    }

    if request
        .filter
        .schema_keys
        .iter()
        .any(|schema| schema == schema_key)
        && scan_selects_change_candidate_identity(
            request,
            InterestDomain::Combined,
            branch_id,
            schema_key,
            file_id,
            row_pk,
        )
    {
        let path_scoped = *path_predicate != crate::hot_state::FilePathInterest::All
            || *root_directory
            || directory_ids.is_some();
        let path_identity_selected = !path_scoped
            || change_id
                .is_some_and(|change_id| selected_files.path_change_ids.contains(&change_id));
        if path_identity_selected {
            return true;
        }
    }

    file_id.is_some_and(|file_id| {
        selected_files.file_ids.contains(file_id)
            && context
                .plugin_owner_schemas
                .get(&(branch_id.to_owned(), file_id.to_owned()))
                .is_some_and(|schemas| schemas.contains(schema_key))
    })
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
            LogicalReadInterest::History { .. } => 256,
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
        || response.frame.is_some()
        || response.inputs.len() > MAX_RECORDS
        || input_digest(request, &response.inputs)? != response.closure_digest
    {
        return Err(invalid(
            "read fulfillment closure is incomplete or mismatched",
        ));
    }
    if response.outcome != ReadFulfillmentOutcome::Complete {
        if request.continuation.is_none()
            && response.inputs.is_empty()
            && request.interests.iter().any(is_bounded_native_recipe)
        {
            return Ok(());
        }
        return Err(invalid("invalid bounded-native fallback response"));
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
    validate_payload_membership(request, &response.inputs)
}

// Membership checks need identity facts for every selected row, but payload
// bytes only for filesystem descriptors and the reserved executable-owner row.
// The input's address/digest and canonical lifetime are validated separately.
fn proof_requires_payload(address: &ReadInputAddress) -> bool {
    match address {
        ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(_)) => true,
        ReadInputAddress::ChangeRecord {
            schema_key, row_pk, ..
        } => {
            matches!(
                schema_key.as_str(),
                "lix_file_descriptor" | "lix_directory_descriptor" | "lix_binary_blob_ref"
            ) || (schema_key == "lix_key_value"
                && row_pk.as_single_string().ok() == Some(crate::plugin::runtime::PLUGIN_OWNER_KEY))
        }
        _ => false,
    }
}
fn proof_fact(address: ReadInputAddress, bytes: Vec<u8>) -> Result<(ReadInput, usize), LixError> {
    let index_bytes = serde_json::to_vec(&address)
        .map_err(|_| invalid("invalid proof address"))?
        .len()
        .saturating_mul(2)
        .saturating_add(256)
        .saturating_add(bytes.len());
    Ok((ReadInput { address, bytes }, index_bytes))
}

fn validate_spooled_complete(
    request: &ReadFulfillmentRequest,
    spool: &spool::InputSpool,
) -> Result<(), LixError> {
    for required in &request.required {
        if !spool.contains(required)? {
            return Err(invalid("read fulfillment omitted a required input"));
        }
    }
    let mut proof_inputs = Vec::new();
    let mut proof_bytes = 0usize;
    for (index, entry) in spool.inputs.iter().enumerate() {
        if matches!(
            &entry.address,
            ReadInputAddress::ChangeRecord { .. }
                | ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(_))
        ) {
            let bytes = if proof_requires_payload(&entry.address) {
                spool.read(index)?.bytes
            } else {
                Vec::new()
            };
            let (fact, resident_bytes) = proof_fact(entry.address.clone(), bytes)?;
            proof_bytes = proof_bytes.saturating_add(resident_bytes);
            if proof_bytes > 4 * 1024 * 1024 {
                return Err(LixError::new(
                    "LIX_NATIVE_RECIPE_WORK_BOUND",
                    "read closure proof facts exceed byte budget",
                ));
            }
            proof_inputs.push(fact);
        }
    }
    validate_payload_membership(request, &proof_inputs)
}

fn validate_payload_membership(
    request: &ReadFulfillmentRequest,
    inputs: &[ReadInput],
) -> Result<(), LixError> {
    let payload_context = if request
        .interests
        .iter()
        .any(|interest| matches!(interest, LogicalReadInterest::FileContent { .. }))
    {
        ReadFulfillmentPayloadContext::new(inputs)?
    } else {
        ReadFulfillmentPayloadContext::default()
    };
    let selected_file_ids = request
        .interests
        .iter()
        .map(|interest| payload_context.selected_files(interest))
        .collect::<Vec<_>>();
    for input in inputs {
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
        let selected_by_recipe = request
            .interests
            .iter()
            .enumerate()
            .any(|(index, interest)| {
                scan_recipe_selects_change_candidate_identity(
                    interest,
                    branch_id,
                    schema_key,
                    file_id.as_deref(),
                    row_pk,
                ) || file_content_recipe_selects_change_identity_with_context(
                    interest,
                    &payload_context,
                    &selected_file_ids[index],
                    canonical_change_id(change_id).ok(),
                    branch_id,
                    schema_key,
                    file_id.as_deref(),
                    row_pk,
                ) || plugin_registry_dependency_matches(interest, input, inputs)
                    || plugin_owner_dependency_matches(interest, input, inputs)
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
        let locator = inputs
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

#[cfg(test)]
pub(super) async fn install<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    request: &ReadFulfillmentRequest,
    response: &ReadFulfillmentResponse,
) -> Result<super::runtime::HydratedInputs, LixError> {
    if response.outcome != ReadFulfillmentOutcome::Complete {
        return Err(invalid(
            "bounded-native fallback cannot be installed as read coverage",
        ));
    }
    validate_complete(request, response)?;
    if request.epoch_id != state.epoch_id() || request.descriptor != *state.descriptor() {
        return Err(invalid("read fulfillment basis changed"));
    }
    install_inputs(storage, state, request, response, false, None).await
}

/// Warm a moving working-diff candidate's dependency closure without publishing
/// candidate admission or serving state. Absent validated metadata may be
/// seeded; resident mutable overlays are retained by the shared CAS installer.
#[cfg(test)]
pub(super) async fn install_candidate_immutable<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    previous: &PartialReplicaState,
    next: &PartialReplicaState,
    request: &ReadFulfillmentRequest,
    response: &ReadFulfillmentResponse,
) -> Result<(), LixError> {
    validate_candidate_immutable_basis(previous, next, request, response)?;
    install_inputs(storage, previous, request, response, true, None)
        .await
        .map(|_| ())
}

pub(super) fn validate_candidate_basis(
    previous: &PartialReplicaState,
    next: &PartialReplicaState,
    request: &ReadFulfillmentRequest,
) -> Result<(), LixError> {
    let previous_descriptor = previous.descriptor();
    let next_descriptor = next.descriptor();
    if previous.repository_id() != next.repository_id()
        || previous.remote_id() != next.remote_id()
        || previous.active_account_id() != next.active_account_id()
        || previous.epoch_id() != next.epoch_id()
        || previous_descriptor.default_branch_id != next_descriptor.default_branch_id
        || previous_descriptor.selected_branch.branch_id
            != next_descriptor.selected_branch.branch_id
        || previous_descriptor.global_branch.branch_id != next_descriptor.global_branch.branch_id
        || next_descriptor.cursor < previous_descriptor.cursor
    {
        return Err(invalid(
            "working-diff candidate changed its admission identity",
        ));
    }
    next_descriptor.validate(
        next.repository_id(),
        Some(&next_descriptor.selected_branch.branch_id),
    )?;
    if request.epoch_id != next.epoch_id()
        || request.descriptor != *next_descriptor
        || request.continuation.is_some()
        || request.interests.is_empty()
        || request
            .interests
            .iter()
            .any(|interest| !matches!(interest, LogicalReadInterest::Diff { .. }))
    {
        return Err(invalid(
            "candidate request is outside the working-diff basis",
        ));
    }
    super::working_diff_recipe::validate_working_diff_recipes(
        &request.interests,
        &next_descriptor.selected_branch.branch_id,
    )?;
    let head_header = ReadInputAddress::Metadata(NativeMetadataRef::CommitStateHeader(
        next_descriptor.selected_branch.head.commit_id.clone(),
    ));
    if request.required.len() != 1 || request.required.first() != Some(&head_header) {
        return Err(invalid(
            "candidate request must require the exact next selected-head header",
        ));
    }
    Ok(())
}

fn validate_candidate_immutable_basis(
    previous: &PartialReplicaState,
    next: &PartialReplicaState,
    request: &ReadFulfillmentRequest,
    response: &ReadFulfillmentResponse,
) -> Result<(), LixError> {
    validate_candidate_basis(previous, next, request)?;
    if response
        .inputs
        .iter()
        .any(|input| matches!(input.address, ReadInputAddress::ChangeRecord { .. }))
    {
        return Err(invalid(
            "candidate immutable closure cannot contain change records",
        ));
    }
    if response.outcome != ReadFulfillmentOutcome::Complete {
        return Err(invalid("candidate immutable closure is not complete"));
    }
    validate_complete(request, response)
}

async fn install_inputs<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    request: &ReadFulfillmentRequest,
    response: &ReadFulfillmentResponse,
    immutable_only: bool,
    finalize_scratch: Option<&staging::ScratchOwnerFinalizeCapability>,
) -> Result<super::runtime::HydratedInputs, LixError> {
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
            // The candidate closure may seed absent mutable-key metadata for
            // later evaluation, but it must never select or replace a local
            // overlay. Required mutable inputs are outside this route; the
            // ordinary candidate evaluator remains the authority for them.
            if immutable_only
                && preserves_local_mutable_native_overlay(&input.address)
                && request.required.contains(&input.address)
            {
                return Err(invalid(
                    "candidate immutable closure cannot require a mutable overlay",
                ));
            }
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
        if let Some(capability) = finalize_scratch {
            staging::stage_owner_reaping_fence(
                &read,
                state,
                capability,
                &mut writes,
                &mut preconditions,
            )
            .await?;
        }
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
pub(crate) struct BlobReadCapture(Mutex<BlobSelections>);
#[derive(Default)]
struct BlobSelections {
    values: BTreeMap<crate::binary_cas::BlobId, BlobSelection>,
    range_count: usize,
    range_bytes: u64,
}
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
    pub(crate) fn record(
        &self,
        blob: crate::binary_cas::BlobId,
        full: bool,
        range: Option<std::ops::Range<u64>>,
    ) -> Result<(), LixError> {
        let mut requests = self
            .0
            .lock()
            .map_err(|_| invalid("blob read capture poisoned"))?;
        if requests.values.len() >= 4096 && !requests.values.contains_key(&blob) {
            return Err(LixError::new(
                "LIX_NATIVE_RECIPE_WORK_BOUND",
                "blob discovery count limit exceeded",
            ));
        }
        let selection = requests.values.entry(blob).or_default();
        if full || selection.full {
            let removed_count = selection.ranges.len();
            let removed_bytes = selection
                .ranges
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u64>();
            selection.full = true;
            selection.ranges.clear();
            requests.range_count -= removed_count;
            requests.range_bytes -= removed_bytes;
        } else if let Some(range) = range
            && !selection.ranges.contains(&range)
        {
            let len = range
                .end
                .checked_sub(range.start)
                .ok_or_else(|| invalid("invalid blob read range"))?;
            if requests.range_count >= MAX_RECORDS
                || requests.range_bytes.saturating_add(len) > MAX_PAYLOAD_BYTES as u64
            {
                return Err(LixError::new(
                    "LIX_NATIVE_RECIPE_WORK_BOUND",
                    "aggregate blob range budget exceeded",
                ));
            }
            requests
                .values
                .get_mut(&blob)
                .expect("selected blob exists")
                .ranges
                .push(range);
            requests.range_count += 1;
            requests.range_bytes += len;
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
    inputs: &mut spool::InputSpool,
) -> Result<(), LixError> {
    use crate::binary_cas::*;
    // Discovery is complete; consume the bounded selection index rather than
    // cloning all blob/range identities during export.
    let selections = std::mem::take(
        &mut capture
            .0
            .lock()
            .map_err(|_| invalid("blob read capture poisoned"))?
            .values,
    )
    .into_iter()
    .collect::<Vec<_>>();
    let mut chunks = inputs
        .inputs
        .iter()
        .filter_map(|input| {
            if let ReadInputAddress::BlobChunk(hash) = input.address {
                Some(ChunkHash::from_bytes(hash))
            } else {
                None
            }
        })
        .collect::<BTreeSet<_>>();
    for group in selections.chunks(32) {
        let ids = group.iter().map(|(blob, _)| *blob).collect::<Vec<_>>();
        let metadata = load_metadata_many(read, &ids).await?.into_vec();
        for ((blob, selection), metadata) in group.iter().zip(metadata) {
            let metadata = metadata.ok_or_else(|| invalid("authority lacks selected blob"))?;
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
            let bytes =
                serde_json::to_vec(&wire).map_err(|_| invalid("invalid canonical manifest"))?;
            inputs.append(ReadInput {
                address: ReadInputAddress::BlobManifest(*blob.as_bytes()),
                bytes,
            })?;
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
                    anchors
                        .insert(offset / (CHUNK_ANCHOR_BYTES as u64) * (CHUNK_ANCHOR_BYTES as u64));
                }
                offset = end;
            }
            for offset in anchors {
                for chunk in load_canonical_blob_anchor(read, &metadata, offset).await? {
                    if selected.contains(&chunk.receipt.hash) && chunks.insert(chunk.receipt.hash) {
                        inputs.append(ReadInput {
                            address: ReadInputAddress::BlobChunk(*chunk.receipt.hash.as_bytes()),
                            bytes: chunk.bytes,
                        })?;
                    }
                }
            }
        }
    }
    Ok(())
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
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms: crate::telemetry::unix_time_ms() + 60_000,
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
            frame: None,
            lix_id: request.descriptor.lix_id.clone(),
            epoch_id: request.epoch_id.clone(),
            request_digest: request.digest().unwrap(),
            closure_digest: input_digest(&request, &inputs).unwrap(),
            inputs,
            profile: Default::default(),
            continuation: None,
            outcome: ReadFulfillmentOutcome::Complete,
        };
        authority.close().await.unwrap();
        (request, response)
    }

    #[tokio::test]
    async fn native_proof_fallback_is_typed_and_reused_across_frontiers() {
        let (mut request, _) = fixture().await;
        let branch = &request.descriptor.selected_branch;
        request.interests = vec![LogicalReadInterest::History {
            branch_id: branch.branch_id.clone(),
            commit_ids: vec![branch.head.commit_id.clone()],
            relation: "lix_file".into(),
            filter: crate::tracked_state::TrackedStateFilter {
                file_ids: vec![crate::NullableKeyFilter::Null],
                include_tombstones: true,
                ..Default::default()
            },
            retain_payloads: false,
            projected_columns: Vec::new(),
            limit: None,
        }];
        request.validate(&request.descriptor.lix_id).unwrap();

        let empty = Vec::new();
        let mut response = ReadFulfillmentResponse {
            frame: None,
            lix_id: request.descriptor.lix_id.clone(),
            epoch_id: request.epoch_id.clone(),
            request_digest: request.digest().unwrap(),
            closure_digest: input_digest(&request, &empty).unwrap(),
            inputs: empty,
            profile: Default::default(),
            continuation: None,
            outcome: ReadFulfillmentOutcome::NativeFallback,
        };
        validate_complete(&request, &response).unwrap();
        validate_response(&request, &response).unwrap();
        assert!(!request_closure_is_ineligible(&request).unwrap());
        remember_request_closure_ineligible(&request, ReadFulfillmentOutcome::NativeFallback)
            .unwrap();
        assert!(request_closure_is_ineligible(&request).unwrap());

        let mut same_recipe_different_frontier = request.clone();
        same_recipe_different_frontier
            .required
            .push(ReadInputAddress::Metadata(
                NativeMetadataRef::CommitGraphRecord(
                    request.descriptor.selected_branch.head.commit_id.clone(),
                ),
            ));
        assert!(request_closure_is_ineligible(&same_recipe_different_frontier).unwrap());

        let mut changed_basis = request.clone();
        changed_basis.descriptor.cursor += 1;
        assert!(!request_closure_is_ineligible(&changed_basis).unwrap());

        let current_interest = LogicalReadInterest::FilesystemMetadata {
            directory: false,
            branch_ids: vec![request.descriptor.selected_branch.branch_id.clone()],
            file_ids: None,
            directory_ids: None,
            root_directory: false,
            path_predicate: crate::hot_state::FilePathInterest::All,
        };
        let locator = NativeMetadataRef::ChangeLocator(uuid::Uuid::now_v7().to_string());
        let mut mixed = request.clone();
        mixed.interests.push(current_interest.clone());
        let current_only = current_payload_request_after_native_fallback(&mixed, &locator)
            .expect("current recipe remains eligible after private History fallback");
        assert_eq!(current_only.descriptor, mixed.descriptor);
        assert_eq!(current_only.epoch_id, mixed.epoch_id);
        assert_eq!(current_only.interests, vec![current_interest]);
        assert_eq!(
            current_only.required,
            vec![ReadInputAddress::Metadata(locator.clone())]
        );
        assert!(current_only.continuation.is_none());
        assert!(current_payload_request_after_native_fallback(&request, &locator).is_none());

        response.inputs.push(ReadInput {
            address: ReadInputAddress::BlobChunk([7; 32]),
            bytes: b"not coverage".to_vec(),
        });
        response.closure_digest = input_digest(&request, &response.inputs).unwrap();
        assert!(validate_complete(&request, &response).is_err());
        assert!(validate_response(&request, &response).is_err());
        response.inputs.clear();
        response.closure_digest = input_digest(&request, &response.inputs).unwrap();
        response.continuation = Some(ReadContinuation {
            next_offset: 0,
            next_input: 1,
            spool_id: uuid::Uuid::now_v7().to_string(),
            closure_digest: response.closure_digest.clone(),
        });
        assert!(validate_complete(&request, &response).is_err());
        assert!(validate_response(&request, &response).is_err());
        response.continuation = None;
        let mut constructor_paged_request = request.clone();
        constructor_paged_request.continuation = Some(ReadContinuation {
            next_offset: 0,
            next_input: 1,
            spool_id: uuid::Uuid::now_v7().to_string(),
            closure_digest: response.closure_digest.clone(),
        });
        assert!(
            native_fallback_response(
                &constructor_paged_request.descriptor.lix_id,
                &constructor_paged_request,
                DiscoveryProfile::default(),
            )
            .is_err()
        );

        let mut paged_request = request.clone();
        paged_request.continuation = Some(ReadContinuation {
            next_offset: 0,
            next_input: 1,
            spool_id: uuid::Uuid::now_v7().to_string(),
            closure_digest: response.closure_digest.clone(),
        });
        response.request_digest = paged_request.digest().unwrap();
        assert!(validate_response(&paged_request, &response).is_err());

        let mut non_history_request = request.clone();
        non_history_request.interests = vec![LogicalReadInterest::FilesystemMetadata {
            directory: false,
            branch_ids: vec![request.descriptor.selected_branch.branch_id.clone()],
            file_ids: None,
            directory_ids: None,
            root_directory: false,
            path_predicate: crate::hot_state::FilePathInterest::All,
        }];
        let non_native_fallback = ReadFulfillmentResponse {
            frame: None,
            lix_id: non_history_request.descriptor.lix_id.clone(),
            epoch_id: non_history_request.epoch_id.clone(),
            request_digest: non_history_request.digest().unwrap(),
            closure_digest: input_digest(&non_history_request, &[]).unwrap(),
            inputs: Vec::new(),
            profile: Default::default(),
            continuation: None,
            outcome: ReadFulfillmentOutcome::NativeFallback,
        };
        assert!(validate_response(&non_history_request, &non_native_fallback).is_err());
    }

    #[test]
    fn scan_change_payload_recipe_preserves_candidate_identity_scope() {
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
            scan_selects_change_candidate_identity(scan, domain, "branch", "schema", None, &row_pk)
        };

        assert!(selects(&make_scan(), InterestDomain::Combined));
        assert!(selects(&make_scan(), InterestDomain::Tracked));
        assert!(!selects(&make_scan(), InterestDomain::Untracked));

        assert!(
            scan_selects_change_candidate_identity(
                &make_scan(),
                InterestDomain::Combined,
                crate::GLOBAL_BRANCH_ID,
                "schema",
                None,
                &row_pk,
            ),
            "a branch scan includes global candidates for visibility overlay"
        );

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

        let mut no_rows = make_scan();
        no_rows.filter.rows = crate::hot_state::HotStateRowFilter::None;
        assert!(!selects(&no_rows, InterestDomain::Combined));

        for inclusive in [false, true] {
            let bound = crate::tracked_state::RowPkRangeBound {
                row_pk: row_pk.clone(),
                inclusive,
            };
            let mut lower = make_scan();
            lower.filter.row_pk_lower = Some(bound.clone());
            assert_eq!(selects(&lower, InterestDomain::Combined), inclusive);
            let mut upper = make_scan();
            upper.filter.row_pk_upper = Some(bound);
            assert_eq!(selects(&upper, InterestDomain::Combined), inclusive);
        }
        let mut below_lower = make_scan();
        below_lower.filter.row_pk_lower = Some(crate::tracked_state::RowPkRangeBound {
            row_pk: crate::row_pk::RowPk::single("zzz"),
            inclusive: true,
        });
        assert!(!selects(&below_lower, InterestDomain::Combined));
        let mut above_upper = make_scan();
        above_upper.filter.row_pk_upper = Some(crate::tracked_state::RowPkRangeBound {
            row_pk: crate::row_pk::RowPk::single("aaa"),
            inclusive: true,
        });
        assert!(!selects(&above_upper, InterestDomain::Combined));

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
        assert!(selects(&content_predicate, InterestDomain::Combined));

        let mut global_scope = make_scan();
        global_scope.filter.global = Some(false);
        assert!(selects(&global_scope, InterestDomain::Combined));

        let mut tombstones = make_scan();
        tombstones.filter.include_tombstones = true;
        assert!(selects(&tombstones, InterestDomain::Combined));

        let mut limited = make_scan();
        limited.limit = Some(1);
        assert!(selects(&limited, InterestDomain::Combined));

        let mut indexed_candidates = make_scan();
        indexed_candidates.filter.declared_column_eq = Some(crate::hot_state::DeclaredColumnEq {
            schema_key: "schema".to_owned(),
            ordinal: 0,
            values: vec![crate::hot_state::HotIndexValue::String("target".to_owned())],
        });
        assert!(selects(&indexed_candidates, InterestDomain::Combined));
        indexed_candidates.filter.declared_column_eq = None;
        indexed_candidates.filter.declared_column_range =
            Some(Box::new(crate::hot_state::DeclaredColumnRange {
                schema_key: "schema".to_owned(),
                ordinal: 0,
                lower: Some((
                    crate::hot_state::HotIndexValue::String("a".to_owned()),
                    true,
                )),
                upper: Some((
                    crate::hot_state::HotIndexValue::String("z".to_owned()),
                    false,
                )),
            }));
        assert!(selects(&indexed_candidates, InterestDomain::Combined));
        indexed_candidates.filter.row_pks = vec![crate::row_pk::RowPk::single("other_row")];
        assert!(
            !selects(&indexed_candidates, InterestDomain::Combined),
            "an index predicate cannot widen the explicit primary-key scope"
        );
    }

    #[test]
    fn plugin_registry_dependency_requires_a_selected_tracked_row() {
        use crate::plugin::runtime::PLUGIN_REGISTRY_KEY;

        let branch = "branch";
        let row_pk = crate::row_pk::RowPk::single("row");
        let make_input = |label: &str,
                          branch_id: &str,
                          schema_key: &str,
                          file_id: Option<&str>,
                          row_pk: crate::row_pk::RowPk| {
            ReadInput {
                address: ReadInputAddress::ChangeRecord {
                    change_id: format!("change-{label}"),
                    source_commit_id: format!("source-{label}"),
                    branch_id: branch_id.to_owned(),
                    schema_key: schema_key.to_owned(),
                    file_id: file_id.map(str::to_owned),
                    row_pk,
                    updated_at: "2025-01-01T00:00:00.000Z".to_owned(),
                    payload_digest: [0; 32],
                },
                bytes: Vec::new(),
            }
        };
        let row_input = make_input("row", branch, "merge_test_row", None, row_pk.clone());
        let registry_input = make_input(
            "registry",
            branch,
            "lix_key_value",
            None,
            crate::row_pk::RowPk::single(PLUGIN_REGISTRY_KEY),
        );
        let scan = LogicalReadInterest::Scan {
            request: crate::hot_state::HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
                    schema_keys: vec!["merge_test_row".to_owned()],
                    branch_ids: vec![branch.to_owned()],
                    row_pks: vec![row_pk.clone()],
                    ..Default::default()
                },
                ..Default::default()
            },
            domain: InterestDomain::Tracked,
        };
        let inputs = vec![row_input.clone(), registry_input.clone()];

        assert!(plugin_registry_dependency_matches(
            &scan,
            &registry_input,
            &inputs,
        ));
        assert!(
            !plugin_registry_dependency_matches(
                &scan,
                &registry_input,
                std::slice::from_ref(&registry_input),
            ),
            "a registry row alone cannot prove a returned-row dependency"
        );

        let unrelated_key = make_input(
            "unrelated",
            branch,
            "lix_key_value",
            None,
            crate::row_pk::RowPk::single("ordinary-setting"),
        );
        assert!(!plugin_registry_dependency_matches(
            &scan,
            &unrelated_key,
            &inputs,
        ));

        let wrong_branch_registry = make_input(
            "wrong-branch-registry",
            "other-branch",
            "lix_key_value",
            None,
            crate::row_pk::RowPk::single(PLUGIN_REGISTRY_KEY),
        );
        assert!(!plugin_registry_dependency_matches(
            &scan,
            &wrong_branch_registry,
            &inputs,
        ));

        let file_scoped_registry = make_input(
            "file-scoped-registry",
            branch,
            "lix_key_value",
            Some("file"),
            crate::row_pk::RowPk::single(PLUGIN_REGISTRY_KEY),
        );
        assert!(!plugin_registry_dependency_matches(
            &scan,
            &file_scoped_registry,
            &inputs,
        ));

        let untracked_scan = LogicalReadInterest::Scan {
            request: match &scan {
                LogicalReadInterest::Scan { request, .. } => request.clone(),
                _ => unreachable!(),
            },
            domain: InterestDomain::Untracked,
        };
        assert!(!plugin_registry_dependency_matches(
            &untracked_scan,
            &registry_input,
            &inputs,
        ));

        let exact = LogicalReadInterest::Exact {
            rows: vec![crate::hot_state::ExactReadIdentity {
                branch_id: branch.to_owned(),
                schema_key: "merge_test_row".to_owned(),
                file_id: None,
                row_pk: row_pk.clone(),
            }],
            projection: crate::hot_state::HotStateProjection::default(),
            untracked: Some(false),
            include_tombstones: false,
        };
        assert!(plugin_registry_dependency_matches(
            &exact,
            &registry_input,
            &inputs,
        ));

        let directory = make_input(
            "directory",
            branch,
            "lix_directory_descriptor",
            None,
            row_pk.clone(),
        );
        let directory_inputs = vec![directory, registry_input.clone()];
        let directory_paths = LogicalReadInterest::FilesystemPaths {
            scope: crate::filesystem::FilesystemPathIndexScope::DirectoriesOnly,
            branch_ids: vec![branch.to_owned()],
            include_blob_refs: false,
            cache_small_blob_data: false,
        };
        assert!(plugin_registry_dependency_matches(
            &directory_paths,
            &registry_input,
            &directory_inputs,
        ));
        assert!(!plugin_registry_dependency_matches(
            &directory_paths,
            &registry_input,
            &inputs,
        ));

        // Branch scans implicitly read global candidates for overlay
        // resolution, so the registry dependency must be accepted from the
        // global branch when the selected tracked row is also global.
        let global_row = make_input(
            "global-row",
            crate::GLOBAL_BRANCH_ID,
            "merge_test_row",
            None,
            row_pk.clone(),
        );
        let global_registry = make_input(
            "global-registry",
            crate::GLOBAL_BRANCH_ID,
            "lix_key_value",
            None,
            crate::row_pk::RowPk::single(PLUGIN_REGISTRY_KEY),
        );
        let global_inputs = vec![global_row, global_registry.clone()];
        assert!(plugin_registry_dependency_matches(
            &scan,
            &global_registry,
            &global_inputs,
        ));
    }

    #[test]
    fn file_content_recipe_authorizes_only_selected_plugin_inputs() {
        use crate::plugin::runtime::{PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginFileOwner};

        let branch = "01920000-0000-7000-8000-0000000000b1";
        let file_id = "01920000-0000-7000-8000-0000000000d2";
        let file_row_pk = crate::row_pk::RowPk::uuid_from_canonical(file_id).unwrap();
        let file_snapshot = serde_json::json!({
            "id": file_id,
            "directory_id": null,
            "name": "target.csv",
        });
        let file_payload = crate::row_payload::TypedRow::from_builtin_json(
            "lix_file_descriptor",
            &file_row_pk,
            &file_snapshot,
        )
        .unwrap()
        .durable_payload()
        .unwrap()
        .to_vec();
        let file_change_id = crate::changelog::ChangeId::for_test_label("file-content-file");
        let created_at = crate::common::LixTimestamp::from_unix_millis_utc_lossy(6);
        let file_record = crate::changelog::ChangeRecord {
            format_version: 2,
            change_id: file_change_id,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            schema_key: "lix_file_descriptor".to_owned(),
            row_pk: file_row_pk.clone(),
            file_id: Some(file_id.to_owned()),
            snapshot: Some(file_payload),
            metadata: None,
            created_at,
            origin_key: None,
        };
        let file_input = ReadInput {
            address: ReadInputAddress::ChangeRecord {
                change_id: file_change_id.to_string(),
                source_commit_id: crate::changelog::CommitId::for_test_label(
                    "file-content-file-source",
                )
                .to_string(),
                branch_id: branch.to_owned(),
                schema_key: file_record.schema_key.clone(),
                file_id: file_record.file_id.clone(),
                row_pk: file_row_pk,
                updated_at: created_at.to_string(),
                payload_digest: [0; 32],
            },
            bytes: crate::changelog::encode_change_record(&file_record).unwrap(),
        };
        let owner_row_pk = crate::row_pk::RowPk::single(PLUGIN_OWNER_KEY);
        let owner = PluginFileOwner::new(file_id, "plugin_csv", vec!["csv_row".into()]).unwrap();
        let owner_snapshot = owner.to_snapshot().unwrap();
        let owner_payload = crate::row_payload::TypedRow::from_builtin_json(
            "lix_key_value",
            &owner_row_pk,
            &owner_snapshot,
        )
        .unwrap()
        .durable_payload()
        .unwrap()
        .to_vec();
        let change_id = crate::changelog::ChangeId::for_test_label("file-content-owner");
        let created_at = crate::common::LixTimestamp::from_unix_millis_utc_lossy(7);
        let record = crate::changelog::ChangeRecord {
            format_version: 2,
            change_id,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            schema_key: "lix_key_value".to_owned(),
            row_pk: owner_row_pk.clone(),
            file_id: Some(file_id.to_owned()),
            snapshot: Some(owner_payload),
            metadata: None,
            created_at,
            origin_key: None,
        };
        let owner_input = ReadInput {
            address: ReadInputAddress::ChangeRecord {
                change_id: change_id.to_string(),
                source_commit_id: crate::changelog::CommitId::for_test_label(
                    "file-content-owner-source",
                )
                .to_string(),
                branch_id: branch.to_owned(),
                schema_key: record.schema_key.clone(),
                file_id: record.file_id.clone(),
                row_pk: owner_row_pk.clone(),
                updated_at: created_at.to_string(),
                payload_digest: [0; 32],
            },
            bytes: crate::changelog::encode_change_record(&record).unwrap(),
        };
        let metadata = LogicalReadInterest::FilesystemMetadata {
            directory: false,
            branch_ids: vec![branch.into()],
            file_ids: None,
            directory_ids: None,
            root_directory: false,
            path_predicate: crate::hot_state::FilePathInterest::All,
        };
        let owner_inputs = vec![file_input.clone(), owner_input.clone()];
        assert!(plugin_owner_dependency_matches(
            &metadata,
            &owner_input,
            &owner_inputs
        ));
        assert!(!plugin_owner_dependency_matches(
            &metadata,
            &owner_input,
            std::slice::from_ref(&owner_input)
        ));
        let mut directory_metadata = metadata.clone();
        if let LogicalReadInterest::FilesystemMetadata { directory, .. } = &mut directory_metadata {
            *directory = true;
        }
        assert!(!plugin_owner_dependency_matches(
            &directory_metadata,
            &owner_input,
            &owner_inputs
        ));
        let directories_only = LogicalReadInterest::FilesystemPaths {
            scope: crate::filesystem::FilesystemPathIndexScope::DirectoriesOnly,
            branch_ids: vec![branch.into()],
            include_blob_refs: false,
            cache_small_blob_data: false,
        };
        assert!(!plugin_owner_dependency_matches(
            &directories_only,
            &owner_input,
            &owner_inputs
        ));
        let exact_file = LogicalReadInterest::Exact {
            rows: vec![crate::hot_state::ExactReadIdentity {
                schema_key: "lix_file_descriptor".into(),
                branch_id: branch.into(),
                file_id: Some(file_id.into()),
                row_pk: file_record.row_pk.clone(),
            }],
            projection: crate::hot_state::HotStateProjection {
                columns: vec!["snapshot_content".into()],
            },
            untracked: Some(false),
            include_tombstones: false,
        };
        assert!(plugin_owner_dependency_matches(
            &exact_file,
            &owner_input,
            &owner_inputs
        ));
        let scanned_file = LogicalReadInterest::Scan {
            request: crate::hot_state::HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
                    schema_keys: vec!["lix_file_descriptor".into()],
                    branch_ids: vec![branch.into()],
                    file_ids: vec![crate::NullableKeyFilter::Value(file_id.into())],
                    ..Default::default()
                },
                ..Default::default()
            },
            domain: InterestDomain::Tracked,
        };
        assert!(plugin_owner_dependency_matches(
            &scanned_file,
            &owner_input,
            &owner_inputs
        ));
        let mut directory_witness = file_input.clone();
        if let ReadInputAddress::ChangeRecord { schema_key, .. } = &mut directory_witness.address {
            *schema_key = "lix_directory_descriptor".into();
        }
        assert!(!plugin_owner_dependency_matches(
            &metadata,
            &owner_input,
            &[directory_witness, owner_input.clone()]
        ));
        let mut wrong_file_pk = file_input.clone();
        if let ReadInputAddress::ChangeRecord { row_pk, .. } = &mut wrong_file_pk.address {
            *row_pk =
                crate::row_pk::RowPk::uuid_from_canonical("01920000-0000-7000-8000-0000000000d3")
                    .unwrap();
        }
        assert!(!plugin_owner_dependency_matches(
            &metadata,
            &owner_input,
            &[wrong_file_pk, owner_input.clone()]
        ));
        let mut wrong_branch = file_input.clone();
        if let ReadInputAddress::ChangeRecord { branch_id, .. } = &mut wrong_branch.address {
            *branch_id = crate::GLOBAL_BRANCH_ID.into();
        }
        assert!(!plugin_owner_dependency_matches(
            &metadata,
            &owner_input,
            &[wrong_branch, owner_input.clone()]
        ));
        let mut wrong_file = file_input.clone();
        if let ReadInputAddress::ChangeRecord {
            file_id, row_pk, ..
        } = &mut wrong_file.address
        {
            *file_id = Some("01920000-0000-7000-8000-0000000000d3".into());
            *row_pk =
                crate::row_pk::RowPk::uuid_from_canonical(file_id.as_deref().unwrap()).unwrap();
        }
        assert!(!plugin_owner_dependency_matches(
            &metadata,
            &owner_input,
            &[wrong_file, owner_input.clone()]
        ));
        let mut tombstone_record = record.clone();
        tombstone_record.snapshot = None;
        let mut tombstone = owner_input.clone();
        tombstone.bytes = crate::changelog::encode_change_record(&tombstone_record).unwrap();
        assert!(plugin_owner_dependency_matches(
            &metadata,
            &tombstone,
            &owner_inputs
        ));
        assert!(!plugin_owner_dependency_matches(
            &metadata,
            &tombstone,
            std::slice::from_ref(&tombstone)
        ));
        let mut fileless_record = file_record.clone();
        fileless_record.file_id = None;
        let mut fileless_descriptor = file_input.clone();
        if let ReadInputAddress::ChangeRecord { file_id, .. } = &mut fileless_descriptor.address {
            *file_id = None;
        }
        fileless_descriptor.bytes =
            crate::changelog::encode_change_record(&fileless_record).unwrap();
        assert!(plugin_owner_dependency_matches(
            &metadata,
            &owner_input,
            &[fileless_descriptor, owner_input.clone()]
        ));
        let mut malformed_owner = owner_input.clone();
        malformed_owner.bytes.clear();
        assert!(!plugin_owner_dependency_matches(
            &metadata,
            &malformed_owner,
            &owner_inputs
        ));
        let mut ordinary_key = owner_input.clone();
        if let ReadInputAddress::ChangeRecord { row_pk, .. } = &mut ordinary_key.address {
            *row_pk = crate::row_pk::RowPk::single("unrelated-setting");
        }
        assert!(!plugin_owner_dependency_matches(
            &metadata,
            &ordinary_key,
            &owner_inputs
        ));

        let inputs = vec![file_input, owner_input];
        let interest = LogicalReadInterest::FileContent {
            request: crate::hot_state::HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
                    schema_keys: vec![
                        "lix_file_descriptor".into(),
                        "lix_binary_blob_ref".into(),
                        "lix_directory_descriptor".into(),
                    ],
                    branch_ids: vec![branch.into()],
                    ..Default::default()
                },
                ..Default::default()
            },
            file_ids: None,
            directory_ids: None,
            root_directory: false,
            indexed: true,
            path_predicate: crate::hot_state::FilePathInterest::In {
                values: vec!["/target.csv".into()],
            },
            byte_range: None,
        };

        assert!(file_content_recipe_selects_change_identity(
            &interest,
            &inputs,
            branch,
            "lix_key_value",
            Some(file_id),
            &owner_row_pk,
        ));
        assert!(file_content_recipe_selects_change_identity(
            &interest,
            &inputs,
            branch,
            "csv_row",
            Some(file_id),
            &crate::row_pk::RowPk::single("row"),
        ));
        assert!(!file_content_recipe_selects_change_identity(
            &interest,
            &inputs,
            branch,
            "other_plugin_row",
            Some(file_id),
            &crate::row_pk::RowPk::single("row"),
        ));
        assert!(!file_content_recipe_selects_change_identity(
            &interest,
            &inputs,
            branch,
            "csv_row",
            Some("01920000-0000-7000-8000-0000000000d3"),
            &crate::row_pk::RowPk::single("foreign-row"),
        ));

        let registry_row_pk = crate::row_pk::RowPk::single(PLUGIN_REGISTRY_KEY);
        assert!(
            file_content_recipe_selects_change_identity(
                &interest,
                &[],
                branch,
                "lix_key_value",
                None,
                &registry_row_pk,
            ),
            "file content needs the exact branch plugin registry row"
        );
        assert!(!file_content_recipe_selects_change_identity(
            &interest,
            &[],
            branch,
            "lix_key_value",
            None,
            &crate::row_pk::RowPk::single("unrelated-setting"),
        ));
    }

    #[test]
    fn file_content_path_selection_uses_the_effective_branch_global_overlay() {
        let selected_branch = "01920000-0000-7000-8000-0000000000b1";
        let file_id = "01920000-0000-7000-8000-0000000000d2";
        let directory_id = "01920000-0000-7000-8000-0000000000d3";
        let make_input = |branch_id: &str,
                          schema_key: &str,
                          id: &str,
                          file_id: Option<&str>,
                          name: &str,
                          directory_id: Option<&str>,
                          label: &str| {
            let row_pk = crate::row_pk::RowPk::uuid_from_canonical(id).unwrap();
            let snapshot = if schema_key == "lix_file_descriptor" {
                serde_json::json!({
                    "id": id,
                    "directory_id": directory_id,
                    "name": name,
                })
            } else {
                serde_json::json!({
                    "id": id,
                    "parent_id": directory_id,
                    "name": name,
                })
            };
            let payload =
                crate::row_payload::TypedRow::from_builtin_json(schema_key, &row_pk, &snapshot)
                    .unwrap()
                    .durable_payload()
                    .unwrap()
                    .to_vec();
            let change_id = crate::changelog::ChangeId::for_test_label(label);
            let created_at = crate::common::LixTimestamp::from_unix_millis_utc_lossy(8);
            let record = crate::changelog::ChangeRecord {
                format_version: 2,
                change_id,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
                schema_key: schema_key.to_owned(),
                row_pk: row_pk.clone(),
                file_id: file_id.map(str::to_owned),
                snapshot: Some(payload),
                metadata: None,
                created_at,
                origin_key: None,
            };
            ReadInput {
                address: ReadInputAddress::ChangeRecord {
                    change_id: change_id.to_string(),
                    source_commit_id: crate::changelog::CommitId::for_test_label(&format!(
                        "{label}-source"
                    ))
                    .to_string(),
                    branch_id: branch_id.to_owned(),
                    schema_key: schema_key.to_owned(),
                    file_id: file_id.map(str::to_owned),
                    row_pk,
                    updated_at: created_at.to_string(),
                    payload_digest: [0; 32],
                },
                bytes: crate::changelog::encode_change_record(&record).unwrap(),
            }
        };
        // Filesystem parents retain their global scope. The global file
        // lives under a global directory; its selected-branch override moves
        // to the selected branch's root rather than referencing that separate
        // global parent scope.
        let inputs = vec![
            make_input(
                crate::GLOBAL_BRANCH_ID,
                "lix_directory_descriptor",
                directory_id,
                None,
                "global-dir",
                None,
                "overlay-global-directory",
            ),
            make_input(
                crate::GLOBAL_BRANCH_ID,
                "lix_file_descriptor",
                file_id,
                Some(file_id),
                "old.csv",
                Some(directory_id),
                "overlay-global-file",
            ),
            make_input(
                selected_branch,
                "lix_file_descriptor",
                file_id,
                Some(file_id),
                "new.csv",
                None,
                "overlay-selected-file",
            ),
        ];
        let make_interest = |path: &str| LogicalReadInterest::FileContent {
            request: crate::hot_state::HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
                    schema_keys: vec![
                        "lix_file_descriptor".into(),
                        "lix_directory_descriptor".into(),
                        "lix_binary_blob_ref".into(),
                    ],
                    branch_ids: vec![selected_branch.into()],
                    ..Default::default()
                },
                ..Default::default()
            },
            file_ids: None,
            directory_ids: None,
            root_directory: false,
            indexed: true,
            path_predicate: crate::hot_state::FilePathInterest::In {
                values: vec![path.into()],
            },
            byte_range: None,
        };
        let context = ReadFulfillmentPayloadContext::new(&inputs).unwrap();
        assert!(
            !context
                .selected_files(&make_interest("/global-dir/old.csv"))
                .file_ids
                .contains(file_id)
        );
        assert!(
            context
                .selected_files(&make_interest("/new.csv"))
                .file_ids
                .contains(file_id)
        );

        let global_only_context = ReadFulfillmentPayloadContext::new(&inputs[..2]).unwrap();
        assert!(
            global_only_context
                .selected_files(&make_interest("/global-dir/old.csv"))
                .file_ids
                .contains(file_id)
        );
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
    async fn operation_fallback_is_exact_and_working_diff_proof_fallback_is_basis_bound() {
        let (mut request, _) = fixture().await;
        request.interests = vec![LogicalReadInterest::Diff {
            branch_id: Some(request.descriptor.selected_branch.branch_id.clone()),
            relation: "lix_file".into(),
            from: crate::hot_state::DiffInterestEndpoint::WorkingCheckpoint,
            to: crate::hot_state::DiffInterestEndpoint::ActiveHead,
            filter: crate::tracked_state::TrackedStateFilter {
                include_tombstones: true,
                ..Default::default()
            },
            retain_payloads: false,
            projected_columns: vec![],
            limit: None,
        }];
        request.validate(&request.descriptor.lix_id).unwrap();
        let identity_budget = crate::tracked_state::NativeDiffIdentityBudget::new(0);
        let identity_error = identity_budget
            .charge(0)
            .expect_err("native identity budget refusal has a distinct source code");
        assert_eq!(
            identity_error.code,
            crate::tracked_state::NATIVE_DIFF_RECIPE_WORK_BOUND_CODE
        );
        assert_ne!(
            identity_error.code, "LIX_NATIVE_RECIPE_WORK_BOUND",
            "payload/blob operation work limits must retain exact-request memo scope"
        );
        let identity_outcome = discovery_work_fallback_outcome(&identity_error.code, false);
        assert_eq!(identity_outcome, ReadFulfillmentOutcome::NativeFallback);
        assert_eq!(
            discovery_work_fallback_outcome("LIX_NATIVE_RECIPE_WORK_BOUND", false),
            ReadFulfillmentOutcome::OperationFallback
        );
        assert_eq!(
            discovery_work_fallback_outcome(&identity_error.code, true),
            ReadFulfillmentOutcome::OperationFallback,
            "a global observation cap takes exact-operation memo scope even if native work also refused"
        );
        let response = native_fallback_response(
            &request.descriptor.lix_id,
            &request,
            DiscoveryProfile {
                storage_calls: 9,
                storage_bytes: 1_234,
                payload_bytes: 678,
                ..Default::default()
            },
        )
        .unwrap();
        validate_response(&request, &response).unwrap();
        validate_complete(&request, &response).unwrap();
        assert!(response.inputs.is_empty());
        assert_eq!(response.profile.storage_calls, 9);
        assert_eq!(response.profile.storage_bytes, 1_234);
        assert_eq!(response.profile.payload_bytes, 678);

        let mut new_frontier = request.clone();
        new_frontier.required = vec![ReadInputAddress::Metadata(
            NativeMetadataRef::CommitStateHeader(
                request.descriptor.selected_branch.head.commit_id.clone(),
            ),
        )];
        remember_request_closure_ineligible(&request, identity_outcome).unwrap();
        assert!(request_closure_is_ineligible(&new_frontier).unwrap());
        let mut mixed_native_proof = new_frontier.clone();
        mixed_native_proof
            .interests
            .push(LogicalReadInterest::FilesystemMetadata {
                directory: false,
                branch_ids: vec![request.descriptor.selected_branch.branch_id.clone()],
                file_ids: None,
                directory_ids: None,
                root_directory: false,
                path_predicate: crate::hot_state::FilePathInterest::All,
            });
        assert!(request_closure_is_ineligible(&mixed_native_proof).unwrap());

        let mut operation = request.clone();
        operation.epoch_id = uuid::Uuid::now_v7().to_string();
        let mut mixed = operation.clone();
        mixed
            .interests
            .push(LogicalReadInterest::FilesystemMetadata {
                directory: false,
                branch_ids: vec![request.descriptor.selected_branch.branch_id.clone()],
                file_ids: None,
                directory_ids: None,
                root_directory: false,
                path_predicate: crate::hot_state::FilePathInterest::All,
            });
        operation.interests = mixed.interests.clone();
        let operation_outcome = discovery_work_fallback_outcome(&identity_error.code, true);
        assert_eq!(operation_outcome, ReadFulfillmentOutcome::OperationFallback);
        let refused = fallback_response(
            &operation.descriptor.lix_id,
            &operation,
            operation_outcome,
            DiscoveryProfile {
                storage_calls: MAX_READ_CALLS,
                storage_bytes: MAX_READ_BYTES + 1,
                payload_bytes: MAX_PAYLOAD_BYTES + 2,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(refused.outcome, ReadFulfillmentOutcome::OperationFallback);
        assert!(refused.inputs.is_empty());
        assert_eq!(refused.profile.storage_calls, MAX_READ_CALLS);
        assert_eq!(refused.profile.storage_bytes, MAX_READ_BYTES + 1);
        assert_eq!(refused.profile.payload_bytes, MAX_PAYLOAD_BYTES + 2);
        validate_response(&operation, &refused).unwrap();
        validate_complete(&operation, &refused).unwrap();
        remember_request_closure_ineligible(&operation, ReadFulfillmentOutcome::OperationFallback)
            .unwrap();
        assert!(request_closure_is_ineligible(&operation).unwrap());
        let mut narrower = operation.clone();
        narrower.interests = request.interests.clone();
        assert!(
            !request_closure_is_ineligible(&narrower).unwrap(),
            "a mixed operation refusal must not poison a narrower native proof request"
        );
        let mut changed_frontier = operation.clone();
        changed_frontier.required.push(ReadInputAddress::Metadata(
            NativeMetadataRef::CommitGraphRecord(
                request.descriptor.selected_branch.head.commit_id.clone(),
            ),
        ));
        assert!(
            !request_closure_is_ineligible(&changed_frontier).unwrap(),
            "operation work refusal is scoped to the exact required frontier"
        );

        let mut changed = request.clone();
        changed.epoch_id = uuid::Uuid::now_v7().to_string();
        assert!(!request_closure_is_ineligible(&changed).unwrap());
        assert!(validate_complete(&changed, &response).is_err());
        request.continuation = Some(ReadContinuation {
            next_offset: 0,
            next_input: 1,
            spool_id: uuid::Uuid::now_v7().to_string(),
            closure_digest: response.closure_digest.clone(),
        });
        assert!(validate_complete(&request, &response).is_err());
    }

    #[tokio::test]
    async fn fulfillment_rejects_fixed_history_endpoints_outside_its_recipe_scope() {
        let (mut request, _) = fixture().await;
        request.required = vec![ReadInputAddress::Metadata(
            NativeMetadataRef::CommitStateHeader(
                request.descriptor.selected_branch.head.commit_id.clone(),
            ),
        )];
        request.interests = vec![LogicalReadInterest::Diff {
            branch_id: Some(request.descriptor.selected_branch.branch_id.clone()),
            relation: "lix_file".into(),
            from: crate::hot_state::DiffInterestEndpoint::Fixed(uuid::Uuid::now_v7().to_string()),
            to: crate::hot_state::DiffInterestEndpoint::ActiveHead,
            filter: crate::tracked_state::TrackedStateFilter {
                include_tombstones: true,
                ..Default::default()
            },
            retain_payloads: false,
            projected_columns: vec!["id".into()],
            limit: None,
        }];
        let error = request.validate(&request.descriptor.lix_id).unwrap_err();
        assert_eq!(error.code, "LIX_READ_FULFILLMENT_INVALID");
        let LogicalReadInterest::Diff { from, .. } = &mut request.interests[0] else {
            unreachable!();
        };
        *from = crate::hot_state::DiffInterestEndpoint::WorkingCheckpoint;
        request.validate(&request.descriptor.lix_id).unwrap();
    }

    #[tokio::test]
    async fn fulfillment_accepts_only_bounded_leased_history_recipes() {
        let (mut request, _) = fixture().await;
        let selected_branch = request.descriptor.selected_branch.branch_id.clone();
        let recipe = || LogicalReadInterest::History {
            branch_id: selected_branch.clone(),
            commit_ids: vec![uuid::Uuid::now_v7().to_string()],
            relation: "lix_file".into(),
            filter: crate::tracked_state::TrackedStateFilter {
                include_tombstones: true,
                ..Default::default()
            },
            retain_payloads: false,
            projected_columns: vec!["id".into()],
            limit: None,
        };

        request.interests = vec![recipe()];
        request.validate(&request.descriptor.lix_id).unwrap();

        let mut full_page = request.clone();
        let LogicalReadInterest::History { commit_ids, .. } = &mut full_page.interests[0] else {
            unreachable!();
        };
        *commit_ids = (0..crate::hot_state::MAX_HISTORY_RECIPE_COMMIT_IDS)
            .map(|_| uuid::Uuid::now_v7().to_string())
            .collect();
        full_page.validate(&full_page.descriptor.lix_id).unwrap();

        let mut too_many_recipes = request.clone();
        too_many_recipes.interests = (0..=crate::hot_state::MAX_HISTORY_RECIPE_COUNT)
            .map(|_| recipe())
            .collect();
        assert!(
            too_many_recipes
                .validate(&too_many_recipes.descriptor.lix_id)
                .is_err()
        );

        let mut too_many_selected_ids = request.clone();
        too_many_selected_ids.interests = (0..5)
            .map(|_| {
                let mut interest = recipe();
                let LogicalReadInterest::History { commit_ids, .. } = &mut interest else {
                    unreachable!();
                };
                *commit_ids = (0..13).map(|_| uuid::Uuid::now_v7().to_string()).collect();
                interest
            })
            .collect();
        assert!(
            too_many_selected_ids
                .validate(&too_many_selected_ids.descriptor.lix_id)
                .is_err()
        );

        let mut invalid = request.clone();
        let LogicalReadInterest::History { commit_ids, .. } = &mut invalid.interests[0] else {
            unreachable!();
        };
        commit_ids.clear();
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History { commit_ids, .. } = &mut invalid.interests[0] else {
            unreachable!();
        };
        *commit_ids = (0..=crate::hot_state::MAX_HISTORY_RECIPE_COMMIT_IDS)
            .map(|_| uuid::Uuid::now_v7().to_string())
            .collect();
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History { commit_ids, .. } = &mut invalid.interests[0] else {
            unreachable!();
        };
        let selected_id = commit_ids[0].clone();
        commit_ids.push(selected_id);
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History { commit_ids, .. } = &mut invalid.interests[0] else {
            unreachable!();
        };
        commit_ids[0] = "not-a-commit".into();
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History { branch_id, .. } = &mut invalid.interests[0] else {
            unreachable!();
        };
        *branch_id = request.descriptor.global_branch.branch_id.clone();
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History { limit, .. } = &mut invalid.interests[0] else {
            unreachable!();
        };
        *limit = Some(1);
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History {
            projected_columns, ..
        } = &mut invalid.interests[0]
        else {
            unreachable!();
        };
        projected_columns.push("private_unknown_column".into());
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History { filter, .. } = &mut invalid.interests[0] else {
            unreachable!();
        };
        filter.file_ids.push(crate::NullableKeyFilter::Any);
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History {
            projected_columns, ..
        } = &mut invalid.interests[0]
        else {
            unreachable!();
        };
        let column = projected_columns[0].clone();
        projected_columns.push(column);
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History {
            retain_payloads, ..
        } = &mut invalid.interests[0]
        else {
            unreachable!();
        };
        *retain_payloads = true;
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History { filter, .. } = &mut invalid.interests[0] else {
            unreachable!();
        };
        filter.include_tombstones = false;
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History { filter, .. } = &mut invalid.interests[0] else {
            unreachable!();
        };
        filter.row_pk_lower = Some(crate::tracked_state::RowPkRangeBound {
            row_pk: crate::row_pk::RowPk::uuid_from_canonical(&uuid::Uuid::now_v7().to_string())
                .unwrap(),
            inclusive: true,
        });
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History { filter, .. } = &mut invalid.interests[0] else {
            unreachable!();
        };
        filter
            .row_pks
            .push(crate::row_pk::RowPk::single("not-a-uuid"));
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History { filter, .. } = &mut invalid.interests[0] else {
            unreachable!();
        };
        filter
            .file_ids
            .push(crate::NullableKeyFilter::Value("not-a-uuid".into()));
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History { filter, .. } = &mut invalid.interests[0] else {
            unreachable!();
        };
        filter.file_ids = (0..=crate::hot_state::MAX_HISTORY_RECIPE_IDENTITIES)
            .map(|_| crate::NullableKeyFilter::Value(uuid::Uuid::now_v7().to_string()))
            .collect();
        filter.file_ids.push(crate::NullableKeyFilter::Null);
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());

        let mut invalid = request.clone();
        let LogicalReadInterest::History { relation, .. } = &mut invalid.interests[0] else {
            unreachable!();
        };
        *relation = "lix_key_value".into();
        assert!(invalid.validate(&invalid.descriptor.lix_id).is_err());
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
            next_offset: 0,
            next_input: 1,
            spool_id: uuid::Uuid::now_v7().to_string(),
            closure_digest: response.closure_digest.clone(),
        });
        response.continuation = Some(ReadContinuation {
            next_offset: 0,
            next_input: 2,
            spool_id: request.continuation.as_ref().unwrap().spool_id.clone(),
            closure_digest: response.closure_digest.clone(),
        });
        validate_response(&request, &response).unwrap();
        let token = response.continuation.as_ref().unwrap().spool_id.clone();
        response.continuation.as_mut().unwrap().spool_id = uuid::Uuid::now_v7().to_string();
        assert!(validate_response(&request, &response).is_err());
        response.continuation.as_mut().unwrap().spool_id = token;
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
    fn overbudget_history_capture_keeps_the_original_native_demand() {
        let make_history = || LogicalReadInterest::History {
            branch_id: "branch".into(),
            commit_ids: vec![uuid::Uuid::now_v7().to_string()],
            relation: "lix_file".into(),
            filter: crate::tracked_state::TrackedStateFilter {
                include_tombstones: true,
                ..Default::default()
            },
            retain_payloads: false,
            projected_columns: vec!["id".into()],
            limit: None,
        };

        let capture = ReadInterestRegistry::new(16, 4096);
        for _ in 0..=crate::hot_state::MAX_HISTORY_RECIPE_COUNT {
            capture.register(make_history()).unwrap();
        }
        let error = LixError::new(LixError::CODE_INTERNAL_ERROR, "native history is missing")
            .with_details(serde_json::json!({
                "nativeHistoryDemand": {"kind": "history"},
                "kept": "original diagnostic"
            }));
        let annotated = annotate_capture(error, Some(&capture));
        assert_eq!(annotated.code, LixError::CODE_INTERNAL_ERROR);
        let details = annotated.details.as_ref().unwrap();
        assert_eq!(details["kept"], "original diagnostic");
        assert!(details.get("nativeHistoryDemand").is_some());
        assert!(details.get(MARKER).is_none());
        assert!(interests_for_error(&annotated).unwrap().is_none());

        let boundary = (0..crate::hot_state::MAX_HISTORY_RECIPE_COUNT)
            .map(|_| {
                let mut interest = make_history();
                let LogicalReadInterest::History { commit_ids, .. } = &mut interest else {
                    unreachable!();
                };
                *commit_ids = (0..crate::hot_state::MAX_HISTORY_RECIPE_SELECTED_IDS
                    / crate::hot_state::MAX_HISTORY_RECIPE_COUNT)
                    .map(|_| uuid::Uuid::now_v7().to_string())
                    .collect();
                interest
            })
            .collect::<Vec<_>>();
        assert!(history_recipes_within_budget(&boundary));
    }

    #[test]
    fn overbudget_history_keeps_current_recipe_for_selected_payload_recovery() {
        let capture = ReadInterestRegistry::new(32, 64 * 1024);
        for _ in 0..=crate::hot_state::MAX_HISTORY_RECIPE_COUNT {
            capture
                .register(LogicalReadInterest::History {
                    branch_id: "branch".into(),
                    commit_ids: vec![uuid::Uuid::now_v7().to_string()],
                    relation: "lix_file".into(),
                    filter: crate::tracked_state::TrackedStateFilter {
                        include_tombstones: true,
                        ..Default::default()
                    },
                    retain_payloads: false,
                    projected_columns: vec!["id".into()],
                    limit: None,
                })
                .unwrap();
        }
        capture
            .register(LogicalReadInterest::Scan {
                request: crate::hot_state::HotStateScanRequest {
                    filter: crate::hot_state::HotStateFilter {
                        schema_keys: vec!["lix_key_value".into()],
                        branch_ids: vec!["branch".into()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                domain: InterestDomain::Tracked,
            })
            .unwrap();

        let change_id = uuid::Uuid::now_v7().to_string();
        let error = LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "selected change payload is unavailable",
        )
        .with_details(serde_json::json!({
            "payloadFailureReason": "selected_change_payload_unavailable",
            "changeId": change_id,
            "nativeHistoryDemand": {"version": 1}
        }));
        let annotated = annotate_capture(error, Some(&capture));
        let interests = interests_for_error(&annotated)
            .unwrap()
            .expect("current recipe should be retained for payload recovery");

        assert_eq!(interests.len(), 1);
        assert!(matches!(interests[0], LogicalReadInterest::Scan { .. }));
        let details = annotated.details.as_ref().unwrap();
        assert!(details.get("nativeHistoryDemand").is_some());
        assert_eq!(details["nativeReadRecipeCount"], serde_json::json!(1));
    }

    #[test]
    fn diff_capture_keeps_current_recipe_only_for_selected_payload_recovery() {
        let scan = || LogicalReadInterest::Scan {
            request: crate::hot_state::HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
                    schema_keys: vec!["lix_key_value".into()],
                    branch_ids: vec!["branch".into()],
                    ..Default::default()
                },
                ..Default::default()
            },
            domain: InterestDomain::Tracked,
        };
        let diff = || LogicalReadInterest::Diff {
            branch_id: Some("branch".into()),
            relation: "lix_key_value".into(),
            from: crate::hot_state::DiffInterestEndpoint::Fixed(uuid::Uuid::now_v7().to_string()),
            to: crate::hot_state::DiffInterestEndpoint::Fixed(uuid::Uuid::now_v7().to_string()),
            filter: crate::tracked_state::TrackedStateFilter::default(),
            retain_payloads: false,
            projected_columns: vec!["key".into()],
            limit: None,
        };
        let missing_payload = || {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "selected change payload is unavailable",
            )
            .with_details(serde_json::json!({
                "payloadFailureReason": "selected_change_payload_unavailable",
                "changeId": uuid::Uuid::now_v7().to_string(),
                "nativeHistoryDemand": {"version": 1},
            }))
        };

        let mixed = ReadInterestRegistry::new(8, 16 * 1024);
        mixed.register(diff()).unwrap();
        mixed.register(scan()).unwrap();
        let annotated = annotate_capture(missing_payload(), Some(&mixed));
        let interests = interests_for_error(&annotated)
            .unwrap()
            .expect("current recipe should survive a selected payload miss");
        assert_eq!(interests.len(), 1);
        assert!(matches!(interests[0], LogicalReadInterest::Scan { .. }));
        assert!(
            annotated
                .details
                .as_ref()
                .is_some_and(|details| details.get("nativeHistoryDemand").is_some())
        );

        let diff_only = ReadInterestRegistry::new(8, 16 * 1024);
        diff_only.register(diff()).unwrap();
        let annotated = annotate_capture(missing_payload(), Some(&diff_only));
        assert!(
            annotated
                .details
                .as_ref()
                .is_none_or(|details| details.get(MARKER).is_none())
        );
        assert!(interests_for_error(&annotated).unwrap().is_none());
    }

    #[test]
    fn partial_demand_narrowing_declines_unrelated_diff_or_uses_locator_only() {
        let selected_branch = "00000000-0000-7000-8000-000000000123";
        let global_branch = crate::GLOBAL_BRANCH_ID;
        let working_diff = |branch_id: &str| LogicalReadInterest::Diff {
            branch_id: Some(branch_id.to_owned()),
            relation: "lix_file".into(),
            from: crate::hot_state::DiffInterestEndpoint::WorkingCheckpoint,
            to: crate::hot_state::DiffInterestEndpoint::ActiveHead,
            filter: crate::tracked_state::TrackedStateFilter {
                include_tombstones: true,
                ..Default::default()
            },
            retain_payloads: false,
            projected_columns: vec!["id".into()],
            limit: None,
        };
        let current = LogicalReadInterest::FilesystemMetadata {
            directory: false,
            branch_ids: vec![selected_branch.to_owned()],
            file_ids: None,
            directory_ids: None,
            root_directory: false,
            path_predicate: crate::hot_state::FilePathInterest::All,
        };
        let interests = vec![
            working_diff(selected_branch),
            working_diff(global_branch),
            current.clone(),
        ];
        let native_miss =
            || LixError::new(LixError::CODE_INTERNAL_ERROR, "missing native diff input");
        assert!(
            partial_demand_fulfillment_plan(&native_miss(), &interests, selected_branch,).is_none(),
            "off-lease Diff must stay on the original native path"
        );

        let change_id = "00000000-0000-7000-8000-000000000456";
        let selected_payload_miss = LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "selected change payload is unavailable",
        )
        .with_details(serde_json::json!({
            "payloadFailureReason": "selected_change_payload_unavailable",
            "changeId": change_id,
        }));
        let plan =
            partial_demand_fulfillment_plan(&selected_payload_miss, &interests, selected_branch)
                .expect("exact selected payload locator permits current-only salvage");
        assert_eq!(plan.interests, vec![current]);
        assert_eq!(
            plan.selected_payload_locator,
            Some(NativeMetadataRef::ChangeLocator(change_id.to_owned())),
        );
        let required = vec![ReadInputAddress::Metadata(
            plan.selected_payload_locator.clone().unwrap(),
        )];
        assert_eq!(
            required,
            vec![ReadInputAddress::Metadata(
                NativeMetadataRef::ChangeLocator(change_id.to_owned(),)
            )],
        );
    }

    #[test]
    fn selected_nonancestor_history_is_left_for_authenticated_authority_fallback() {
        let selected_branch = "00000000-0000-7000-8000-000000000123";
        let interest = LogicalReadInterest::History {
            branch_id: selected_branch.into(),
            commit_ids: vec!["00000000-0000-7000-8000-000000000999".into()],
            relation: "lix_file".into(),
            filter: crate::tracked_state::TrackedStateFilter {
                include_tombstones: true,
                ..Default::default()
            },
            retain_payloads: false,
            projected_columns: vec!["id".into()],
            limit: None,
        };
        let native_miss = LixError::new(LixError::CODE_INTERNAL_ERROR, "missing history");
        let plan = partial_demand_fulfillment_plan(
            &native_miss,
            std::slice::from_ref(&interest),
            selected_branch,
        )
        .expect("client must not preempt the authority's ancestry proof");
        assert_eq!(plan.interests, vec![interest]);
        assert!(plan.selected_payload_locator.is_none());
    }

    #[test]
    fn discovery_work_bounds_are_explicit_and_fail_closed() {
        let mut calls = Observations::default();
        calls.profile.storage_calls = MAX_READ_CALLS;
        assert!(calls.charge_call().is_err());
        assert!(calls.exhausted);
        assert_eq!(calls.profile.storage_calls, MAX_READ_CALLS);
        let key = StorageKey(vec![1].into());
        let value = StorageProjectedValue::FullValue(Bytes::from_static(&[1]));
        let mut bytes = Observations::default();
        bytes.profile.storage_bytes = MAX_READ_BYTES;
        assert!(
            bytes
                .observe(
                    crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
                    &key,
                    &value
                )
                .is_err()
        );
        assert!(bytes.exhausted);
        assert_eq!(bytes.profile.storage_bytes, MAX_READ_BYTES + 1);
        let mut payload = Observations::default();
        payload.profile.payload_bytes = MAX_PAYLOAD_BYTES;
        assert!(
            payload
                .observe(
                    crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
                    &key,
                    &value
                )
                .is_err()
        );
        assert!(payload.exhausted);
        assert_eq!(payload.profile.payload_bytes, MAX_PAYLOAD_BYTES + 1);
        assert!(
            payload.values.is_empty(),
            "bounded failure must not expose partial installable coverage"
        );
    }

    #[test]
    fn bounded_working_diff_capture_preserves_immutable_native_demand() {
        let capture = ReadInterestRegistry::new(8, 16 * 1024);
        capture
            .register(LogicalReadInterest::Diff {
                branch_id: Some("branch".into()),
                relation: "lix_file".into(),
                from: crate::hot_state::DiffInterestEndpoint::WorkingCheckpoint,
                to: crate::hot_state::DiffInterestEndpoint::ActiveHead,
                filter: crate::tracked_state::TrackedStateFilter {
                    include_tombstones: true,
                    ..Default::default()
                },
                retain_payloads: false,
                projected_columns: vec![],
                limit: None,
            })
            .unwrap();
        let error = LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "working diff dependency unavailable",
        )
        .with_details(serde_json::json!({"nativeHistoryDemand": {"version": 1}}));
        let annotated = annotate_capture(error, Some(&capture));
        let recipes = interests_for_error(&annotated)
            .unwrap()
            .expect("bounded working diff is replayable");
        assert_eq!(recipes.len(), 1);
        assert!(super::super::working_diff_recipe::is_supported_working_diff_recipe(&recipes[0]));
        assert!(
            annotated
                .details
                .as_ref()
                .unwrap()
                .get("nativeHistoryDemand")
                .is_some()
        );
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
        assert!(scan_recipe_selects_change_candidate_identity(
            &interest,
            "branch",
            "lix_file_descriptor",
            Some(id),
            &key
        ));
        assert!(scan_recipe_selects_change_candidate_identity(
            &interest,
            "branch",
            "lix_binary_blob_ref",
            Some(id),
            &key
        ));
        assert!(scan_recipe_selects_change_candidate_identity(
            &interest,
            "branch",
            "lix_directory_descriptor",
            None,
            &other_key
        ));
        assert!(!scan_recipe_selects_change_candidate_identity(
            &interest,
            "branch",
            "lix_file_descriptor",
            Some(other),
            &other_key
        ));
        assert!(!scan_recipe_selects_change_candidate_identity(
            &interest,
            "other-branch",
            "lix_file_descriptor",
            Some(id),
            &key
        ));
        assert!(!scan_recipe_selects_change_candidate_identity(
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
        assert!(!scan_recipe_selects_change_candidate_identity(
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
            (
                "branch",
                "lix_file_descriptor",
                Some(other),
                &other_key,
                false,
            ),
            (
                "other-branch",
                "lix_directory_descriptor",
                None,
                &other_key,
                false,
            ),
            ("branch", "lix_binary_blob_ref", Some(id), &key, false),
            ("branch", "lix_account", None, &key, false),
        ] {
            assert_eq!(
                scan_recipe_selects_change_candidate_identity(&metadata, branch, schema, file, row),
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
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms: leased.lease.expires_at_ms,
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
                        scan_recipe_selects_change_candidate_identity(
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
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms: leased.lease.expires_at_ms,
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
    async fn selected_medium_payloads_stream_through_byte_bounded_provider_pages() {
        let authority = crate::open_lix().await.unwrap();
        authority
            .set_sync_role(crate::sync::SyncRole::Authority)
            .unwrap();
        let mut content = String::with_capacity(450 * 1024);
        for index in 0..(450 * 1024 / 32) {
            let hash = blake3::hash(&(index as u64).to_be_bytes());
            content.extend(
                hash.as_bytes()
                    .iter()
                    .map(|byte| char::from(33 + byte % 94)),
            );
        }
        let mut row_pks = Vec::new();
        for index in 0..32 {
            let key = format!("bounded-payload-{index}");
            row_pks.push(crate::row_pk::RowPk::single(&key));
            authority
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
                    &[crate::Value::Text(key), crate::Value::Text(content.clone())],
                )
                .await
                .unwrap();
        }
        let leased = authority
            .leased_partial_replica_descriptor(None)
            .await
            .unwrap();
        let mut request = ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms: leased.lease.expires_at_ms,
            epoch_id: uuid::Uuid::now_v7().to_string(),
            descriptor: leased.descriptor.clone(),
            interests: vec![LogicalReadInterest::Scan {
                request: crate::hot_state::HotStateScanRequest {
                    filter: crate::hot_state::HotStateFilter {
                        schema_keys: vec!["lix_key_value".into()],
                        row_pks,
                        branch_ids: vec![leased.descriptor.selected_branch.branch_id.clone()],
                        file_ids: vec![crate::NullableKeyFilter::Null],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                domain: InterestDomain::Combined,
            }],
            required: vec![ReadInputAddress::Metadata(
                NativeMetadataRef::CommitGraphRecord(
                    leased.descriptor.selected_branch.head.commit_id.clone(),
                ),
            )],
            continuation: None,
        };
        let mut inputs = Vec::new();
        let mut pages = 0usize;
        loop {
            let response = authority
                .read_sync_fulfillment(&request, &leased.lease.lease_id)
                .await
                .unwrap();
            assert_eq!(response.outcome, ReadFulfillmentOutcome::Complete);
            assert!(
                response.profile.peak_provider_bytes <= DISCOVERY_READ_BUDGET.max_result_bytes,
                "medium selected rows must use ordinary provider pages"
            );
            validate_response(&request, &response).unwrap();
            assert!(
                response.frame.is_none(),
                "medium codec members should fit ordinary transport pages"
            );
            inputs.extend(response.inputs);
            pages += 1;
            match response.continuation {
                Some(cursor) => request.continuation = Some(cursor),
                None => break,
            }
        }
        assert!(pages > 1, "the closure exceeds one transport page");
        let selected = inputs.iter().filter(|input| matches!(&input.address, ReadInputAddress::ChangeRecord { schema_key, .. } if schema_key == "lix_key_value")).collect::<Vec<_>>();
        assert_eq!(selected.len(), 32);
        for input in selected {
            let ReadInputAddress::ChangeRecord { change_id, .. } = &input.address else {
                unreachable!()
            };
            let record =
                crate::changelog::decode_change_record(&input.bytes, change_id.parse().unwrap())
                    .unwrap();
            assert!(record.snapshot.unwrap().len() > 300 * 1024);
        }
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
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms: leased.lease.expires_at_ms,
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

        // A valid History recipe authenticates only its immutable native
        // dependency closure. It cannot be used as a wildcard authority for
        // otherwise valid mutable ChangeRecord payloads.
        let mut history_only = request.clone();
        history_only.interests = vec![LogicalReadInterest::History {
            branch_id: leased.descriptor.selected_branch.branch_id.clone(),
            commit_ids: vec![leased.descriptor.selected_branch.head.commit_id.clone()],
            relation: "lix_file".into(),
            filter: crate::tracked_state::TrackedStateFilter {
                include_tombstones: true,
                ..Default::default()
            },
            retain_payloads: false,
            projected_columns: vec!["id".into()],
            limit: None,
        }];
        let mut history_response = response.clone();
        history_response.request_digest = history_only.digest().unwrap();
        history_response.closure_digest =
            input_digest(&history_only, &history_response.inputs).unwrap();
        let error = validate_complete(&history_only, &history_response)
            .expect_err("History must not authorize mutable row payloads");
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
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms: leased.lease.expires_at_ms,
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
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms: state.baseline_lease().expires_at_ms,
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
            frame: None,
            lix_id: request.descriptor.lix_id.clone(),
            epoch_id: request.epoch_id.clone(),
            request_digest: request.digest().unwrap(),
            closure_digest: input_digest(&request, &inputs).unwrap(),
            inputs,
            profile: Default::default(),
            continuation: None,
            outcome: ReadFulfillmentOutcome::Complete,
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

    #[tokio::test]
    async fn required_chunk_page_keeps_order_and_fails_without_a_closure_on_missing_input() {
        let storage = StorageAdapter::new(Memory::new());
        let payloads = [b"first required chunk".as_slice(), b"second required chunk"];
        let hashes = payloads
            .iter()
            .map(|payload| crate::binary_cas::ChunkHash::from_content(payload))
            .collect::<Vec<_>>();
        let mut writes = storage.new_write_set();
        for (hash, payload) in hashes.iter().copied().zip(payloads) {
            crate::binary_cas::stage_verified_raw_chunk(&mut writes, hash, payload).unwrap();
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();

        let ordered_addresses = hashes
            .iter()
            .rev()
            .map(|hash| ReadInputAddress::BlobChunk(*hash.as_bytes()))
            .collect::<Vec<_>>();
        let payload_store = Arc::new(Mutex::new(spool::PayloadSpool::default()));
        let ordered_spool = Arc::new(Mutex::new(spool::InputSpool::new(payload_store)));
        append_required_blob_chunk_run(&read, &ordered_addresses, &ordered_spool)
            .await
            .unwrap();
        let ordered_spool = ordered_spool.lock().unwrap();
        assert_eq!(ordered_spool.inputs.len(), 2);
        for (index, expected_hash) in hashes.iter().rev().enumerate() {
            let input = ordered_spool.read(index).unwrap();
            assert_eq!(
                input.address,
                ReadInputAddress::BlobChunk(*expected_hash.as_bytes())
            );
            assert_eq!(input.bytes, payloads[1 - index]);
        }
        drop(ordered_spool);

        let missing_hash = crate::binary_cas::ChunkHash::from_content(b"missing required chunk");
        let missing_addresses = vec![
            ReadInputAddress::BlobChunk(*hashes[0].as_bytes()),
            ReadInputAddress::BlobChunk(*missing_hash.as_bytes()),
            ReadInputAddress::BlobChunk(*hashes[1].as_bytes()),
        ];
        let failed_payload_store = Arc::new(Mutex::new(spool::PayloadSpool::default()));
        let failed_spool = Arc::new(Mutex::new(spool::InputSpool::new(failed_payload_store)));
        let error = append_required_blob_chunk_run(&read, &missing_addresses, &failed_spool)
            .await
            .expect_err("a missing required chunk must prevent closure completion");
        assert_eq!(error.message, "authority lacks required chunk");
        assert_eq!(
            failed_spool.lock().unwrap().inputs.len(),
            1,
            "only the prefix before the missing required item can enter the private spool"
        );
        drop(failed_spool);
    }

    #[tokio::test]
    async fn fulfillment_endpoint_returns_no_closure_for_a_missing_required_chunk() {
        let authority = crate::open_lix().await.unwrap();
        authority
            .set_sync_role(crate::sync::SyncRole::Authority)
            .unwrap();
        let leased = authority
            .leased_partial_replica_descriptor(None)
            .await
            .unwrap();
        let missing = crate::binary_cas::ChunkHash::from_content(b"absent required chunk");
        let request = ReadFulfillmentRequest {
            operation_id: uuid::Uuid::now_v7().to_string(),
            release: false,
            operation_expires_at_ms: leased.lease.expires_at_ms,
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
            required: vec![ReadInputAddress::BlobChunk(*missing.as_bytes())],
            continuation: None,
        };

        let error = authority
            .read_sync_fulfillment(&request, &leased.lease.lease_id)
            .await
            .expect_err("a missing required chunk must not produce a closure response");
        assert_eq!(error.message, "authority lacks required chunk");
        authority.close().await.unwrap();
    }

}

#[cfg(test)]
#[path = "read_fulfillment_candidate_tests.rs"]
mod candidate_tests;
