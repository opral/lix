//! Bounded immutable dependency selection. This is demand-driven
//! prefetch, never an opening inventory or a claim that history is complete.
use super::native_metadata::{
    MAX_NATIVE_METADATA_PAYLOAD_BYTES, MAX_NATIVE_METADATA_WALK_RECORDS, NativeMetadata,
    NativeMetadataRequest, NativeMetadataResponse, key, space, validate_bytes,
    validate_native_metadata_request,
};
use crate::changelog::{CommitId, CommitRecord};
use crate::storage_adapter::{
    Storage, StorageAdapterRead, StorageGetManyRequest, StorageProjectedValue,
};
use crate::tracked_state::NativeMetadataRef;
use crate::{Lix, LixError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

// Walks can carry up to one graph record and one optional state header per
// commit. Their encoded records remain bounded independently of this count.
pub(crate) const MAX_METADATA_WALK_COMMITS: u8 = 64;
const MAX_METADATA_WALK_RECORDS: usize = MAX_NATIVE_METADATA_WALK_RECORDS;
const MAX_METADATA_READ_PAGE_SLOTS: usize = 32;
/// Selection supplies optional native inputs, never a server-side proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum MetadataSelection {
    History,
    Causal,
    Incorporation,
    JumpSpine,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeMetadataWalkRequest {
    pub(crate) epoch_id: String,
    pub(crate) anchor: String,
    pub(crate) max_commits: u8,
    pub(crate) include_state_headers: bool,
    pub(crate) selection: MetadataSelection,
    pub(crate) stops: Vec<String>,
    pub(crate) minimum_generation: Option<u64>,
    pub(crate) require_state_header: bool,
}
const HISTORY_MARKER: &str = "nativeHistoryDemand";

pub(super) fn request_for_missing(
    epoch_id: &str,
    addresses: &[NativeMetadataRef],
    error: &LixError,
) -> Result<Option<NativeMetadataWalkRequest>, LixError> {
    let Some(marker) = error
        .details
        .as_ref()
        .and_then(|details| details.get(HISTORY_MARKER))
    else {
        return Ok(None);
    };
    let Some(anchor) = addresses.iter().find_map(|address| match address {
        NativeMetadataRef::CommitGraphRecord(id) => Some(id),
        _ => None,
    }) else {
        return Ok(None);
    };
    if marker.get("version").and_then(serde_json::Value::as_u64) != Some(1) {
        return Err(invalid("invalid native history walk diagnostic"));
    }
    let include_state_headers = marker
        .get("includeStateHeaders")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| invalid("invalid native history walk diagnostic"))?;
    let request = NativeMetadataWalkRequest {
        epoch_id: epoch_id.to_owned(),
        anchor: anchor.clone(),
        max_commits: MAX_METADATA_WALK_COMMITS,
        include_state_headers,
        selection: MetadataSelection::History,
        stops: Vec::new(),
        minimum_generation: None,
        require_state_header: false,
    };
    validate_request(&request)?;
    Ok(Some(request))
}

fn invalid(message: &str) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}
pub(crate) fn validate_request(request: &NativeMetadataWalkRequest) -> Result<(), LixError> {
    if request.max_commits == 0 || request.max_commits > MAX_METADATA_WALK_COMMITS {
        return Err(invalid(
            "native metadata walk requires between 1 and 64 commits",
        ));
    }
    if request.stops.len() > 2 || (request.require_state_header && !request.include_state_headers) {
        return Err(invalid("invalid metadata selection bounds"));
    }
    for stop in &request.stops {
        key(&NativeMetadataRef::CommitGraphRecord(stop.clone()))?;
    }
    validate_native_metadata_request(&NativeMetadataRequest {
        epoch_id: request.epoch_id.clone(),
        objects: vec![NativeMetadataRef::CommitGraphRecord(request.anchor.clone())],
    })
}
fn graph(object: &NativeMetadata) -> Result<CommitRecord, LixError> {
    // Payload validation precedes traversal on both the server and client.
    crate::storage_codec::decode("commit record", &object.bytes)
}

/// Derive an exact install request only after proving every returned address is
/// on the requested parent path (or is that commit's optional state header).
/// Server-selected arbitrary addresses must never become trusted install input.
pub(crate) fn validate_response(
    repository_id: &str,
    request: &NativeMetadataWalkRequest,
    response: &NativeMetadataResponse,
) -> Result<NativeMetadataRequest, LixError> {
    validate_request(request)?;
    if response.objects.is_empty()
        || response.objects.len()
            > usize::from(request.max_commits)
                .saturating_mul(2)
                .min(MAX_METADATA_WALK_RECORDS)
    {
        return Err(invalid(
            "native metadata walk response exceeds its record bound",
        ));
    }
    let exact = NativeMetadataRequest {
        epoch_id: request.epoch_id.clone(),
        objects: response
            .objects
            .iter()
            .map(|object| object.address.clone())
            .collect(),
    };
    super::native_metadata::validate_native_metadata_response_for_addresses(
        repository_id,
        &request.epoch_id,
        &exact.objects,
        MAX_METADATA_WALK_RECORDS,
        response,
    )?;
    if request.selection != MetadataSelection::History {
        return validate_selected_response(request, response, exact);
    }
    let mut expected = Some(request.anchor.clone());
    let mut seen = BTreeSet::new();
    let mut previous_generation = None;
    let mut current = None;
    let mut header_seen = false;
    for object in &response.objects {
        match &object.address {
            NativeMetadataRef::CommitGraphRecord(id) => {
                if expected.as_ref() != Some(id)
                    || !seen.insert(id.clone())
                    || seen.len() > usize::from(request.max_commits)
                {
                    return Err(invalid(
                        "native metadata walk is not the requested parent prefix",
                    ));
                }
                let record = graph(object)?;
                if previous_generation.is_some_and(|generation| record.generation >= generation) {
                    return Err(invalid(
                        "native metadata walk parent generation must decrease",
                    ));
                }
                expected = record.parent_commit_ids.first().map(ToString::to_string);
                if expected.as_ref().is_some_and(|id| seen.contains(id)) {
                    return Err(invalid("native metadata walk contains a parent cycle"));
                }
                previous_generation = Some(record.generation);
                current = Some(id);
                header_seen = false;
            }
            NativeMetadataRef::CommitStateHeader(id)
                if request.include_state_headers && current == Some(id) && !header_seen =>
            {
                header_seen = true;
            }
            _ => return Err(invalid("native metadata walk contains an unrelated record")),
        }
    }
    Ok(exact)
}

fn selected_edges(request: &NativeMetadataWalkRequest, record: &CommitRecord) -> Vec<String> {
    if request
        .stops
        .iter()
        .any(|stop| stop == &record.commit_id.to_string())
    {
        return Vec::new();
    }
    match request.selection {
        MetadataSelection::History => record
            .parent_commit_ids
            .first()
            .map(ToString::to_string)
            .into_iter()
            .collect(),
        MetadataSelection::JumpSpine => {
            if record.first_parent_jump_commit_id == record.commit_id {
                Vec::new()
            } else {
                vec![record.first_parent_jump_commit_id.to_string()]
            }
        }
        MetadataSelection::Causal => {
            if request
                .minimum_generation
                .is_some_and(|minimum| record.generation <= minimum)
            {
                return Vec::new();
            }
            if record.parent_commit_ids.len() == 1
                && record.first_parent_jump_span > 1
                && record
                    .generation
                    .checked_sub(record.first_parent_jump_span)
                    .is_some_and(|generation| {
                        request
                            .minimum_generation
                            .is_some_and(|minimum| generation >= minimum)
                    })
            {
                return vec![record.first_parent_jump_commit_id.to_string()];
            }
            record
                .parent_commit_ids
                .iter()
                .map(ToString::to_string)
                .collect()
        }
        MetadataSelection::Incorporation => record
            .parent_commit_ids
            .iter()
            .map(ToString::to_string)
            .collect(),
    }
}

fn header_source(object: &NativeMetadata) -> Result<Option<String>, LixError> {
    let topology = crate::tracked_state::decode_published_commit_state_topology(
        CommitId::parse_lix(object.address.id(), "metadata selection")?,
        &object.bytes,
    )?;
    Ok(match topology.incorporation() {
        crate::tracked_state::CommitStateIncorporation::Complete(source) => {
            Some(source.to_string())
        }
        _ => topology
            .complete_state_source_commit_id()
            .map(|id| id.to_string()),
    })
}

fn validate_selected_response(
    request: &NativeMetadataWalkRequest,
    response: &NativeMetadataResponse,
    exact: NativeMetadataRequest,
) -> Result<NativeMetadataRequest, LixError> {
    let mut reachable = BTreeSet::from([request.anchor.clone()]);
    let mut seen = BTreeSet::new();
    let mut current = None;
    let mut header_seen = false;
    for object in &response.objects {
        match &object.address {
            NativeMetadataRef::CommitGraphRecord(id) => {
                if !reachable.contains(id)
                    || !seen.insert(id.clone())
                    || seen.len() > usize::from(request.max_commits)
                {
                    return Err(invalid(
                        "metadata selection contains an unrelated or repeated graph record",
                    ));
                }
                reachable.extend(selected_edges(request, &graph(object)?));
                current = Some(id);
                header_seen = false;
            }
            NativeMetadataRef::CommitStateHeader(id)
                if request.include_state_headers && current == Some(id) && !header_seen =>
            {
                header_seen = true;
                if request.selection == MetadataSelection::Incorporation
                    && !request.stops.contains(id)
                {
                    reachable.extend(header_source(object)?);
                }
            }
            _ => return Err(invalid("metadata selection contains an unrelated header")),
        }
    }
    if request.require_state_header
        && !exact
            .objects
            .contains(&NativeMetadataRef::CommitStateHeader(
                request.anchor.clone(),
            ))
    {
        return Err(invalid(
            "metadata selection omitted the required anchor header",
        ));
    }
    Ok(exact)
}

/// Batch independently addressed graph/header records through bounded provider
/// pages. Individual decode failures are retained separately: optional
/// lookahead must not turn an unused sibling into a required corruption failure.
struct BoundedMetadataInputs {
    values: Vec<Result<Option<NativeMetadata>, LixError>>,
    budget_truncated: bool,
    raw_bytes: usize,
}

async fn read_many(
    read: &(impl StorageAdapterRead + ?Sized),
    addresses: &[NativeMetadataRef],
    remaining_response_bytes: usize,
) -> Result<BoundedMetadataInputs, LixError> {
    let keys = addresses
        .iter()
        .map(|address| key(address).map(|key| [key]))
        .collect::<Result<Vec<_>, _>>()?;
    let requests = addresses
        .iter()
        .zip(&keys)
        .map(|(address, keys)| StorageGetManyRequest {
            space: space(address),
            keys,
            opts: Default::default(),
        })
        .collect::<Vec<_>>();
    let mut values = Vec::with_capacity(addresses.len());
    let mut offset = 0usize;
    let mut retained_bytes = 0usize;
    let mut budget_truncated = false;
    while offset < requests.len() {
        let remaining = remaining_response_bytes
            .min(MAX_NATIVE_METADATA_PAYLOAD_BYTES)
            .saturating_sub(retained_bytes);
        if remaining == 0 {
            budget_truncated = true;
            break;
        }
        let budget = crate::storage_adapter::ReadBudget {
            max_result_bytes: remaining,
            max_single_value_bytes: remaining,
        };
        let (page, page_bytes) = match crate::storage_adapter::read_bounded_point_page(
            read,
            &requests,
            offset,
            MAX_METADATA_READ_PAGE_SLOTS,
            budget,
        )
        .await
        {
            Ok(page) => page,
            Err(crate::storage_adapter::StorageError::ReadBudgetExceeded { .. }) => {
                budget_truncated = true;
                break;
            }
            Err(error) => return Err(error.into()),
        };
        retained_bytes = retained_bytes
            .checked_add(page_bytes)
            .filter(|bytes| *bytes <= MAX_NATIVE_METADATA_PAYLOAD_BYTES)
            .ok_or_else(|| invalid("metadata selection payload exceeds byte bound"))?;
        let count = page.values.len();
        for (address, value) in addresses[offset..offset + count].iter().zip(page.values) {
            let value = match value {
                None => Ok(None),
                Some(StorageProjectedValue::FullValue(bytes)) => {
                    if bytes.len() > MAX_NATIVE_METADATA_PAYLOAD_BYTES {
                        Err(invalid("metadata selection record exceeds byte bound"))
                    } else {
                        validate_bytes(address, &bytes).map(|()| {
                            Some(NativeMetadata {
                                address: address.clone(),
                                bytes: bytes.to_vec(),
                                checkpoint_conversation: None,
                            })
                        })
                    }
                }
                Some(StorageProjectedValue::KeyOnly) => {
                    Err(LixError::unknown("metadata selection omitted payload"))
                }
            };
            values.push(value);
        }
        offset += count;
    }
    Ok(BoundedMetadataInputs {
        values,
        budget_truncated,
        raw_bytes: retained_bytes,
    })
}

async fn read_walk(
    read: &(impl StorageAdapterRead + ?Sized),
    repository_id: &str,
    request: &NativeMetadataWalkRequest,
) -> Result<NativeMetadataResponse, LixError> {
    read_walk_with_response_budget(
        read,
        repository_id,
        request,
        MAX_NATIVE_METADATA_PAYLOAD_BYTES,
    )
    .await
}

async fn read_walk_with_response_budget(
    read: &(impl StorageAdapterRead + ?Sized),
    repository_id: &str,
    request: &NativeMetadataWalkRequest,
    response_budget_bytes: usize,
) -> Result<NativeMetadataResponse, LixError> {
    validate_request(request)?;
    let mut pending = vec![request.anchor.clone()];
    let mut objects = Vec::new();
    let mut total = 0usize;
    let mut seen = BTreeSet::new();
    let mut previous_generation = None;
    let mut fetched = std::collections::VecDeque::new();
    let mut stop_after_fetched_prefix = false;
    let mut fetched_raw_bytes = 0usize;
    loop {
        if fetched.is_empty() {
            let mut group = Vec::new();
            let mut group_ids = BTreeSet::new();
            while let Some(id) = pending.pop() {
                if !seen.contains(&id) && group_ids.insert(id.clone()) {
                    group.push(id);
                    if group.len() >= usize::from(request.max_commits).saturating_sub(seen.len()) {
                        break;
                    }
                }
            }
            if group.is_empty() {
                break;
            }
            let addresses = group
                .iter()
                .flat_map(|id| {
                    let mut addresses = vec![NativeMetadataRef::CommitGraphRecord(id.clone())];
                    if request.include_state_headers {
                        addresses.push(NativeMetadataRef::CommitStateHeader(id.clone()));
                    }
                    addresses
                })
                .collect::<Vec<_>>();
            let remaining = response_budget_bytes.saturating_sub(fetched_raw_bytes);
            let batch = read_many(read, &addresses, remaining).await?;
            fetched_raw_bytes = fetched_raw_bytes.saturating_add(batch.raw_bytes);
            stop_after_fetched_prefix = batch.budget_truncated;
            if batch.values.is_empty() && batch.budget_truncated {
                if objects.is_empty() && group.iter().any(|id| id == &request.anchor) {
                    return Err(invalid(
                        "required metadata anchor exceeds response byte bound",
                    ));
                }
                break;
            }
            let mut values = batch.values.into_iter();
            for id in group {
                let Some(graph) = values.next() else {
                    break;
                };
                let mut inputs = vec![graph];
                if request.include_state_headers {
                    if let Some(header) = values.next() {
                        inputs.push(header);
                    } else {
                        // The graph slot precedes its optional header on the
                        // wire. An omitted header is a suffix boundary, not a
                        // claim that the authority had no header.
                        inputs.push(Ok(None));
                    }
                }
                fetched.push_back((id, inputs));
            }
        }
        let Some((id, inputs)) = fetched.pop_front() else {
            break;
        };
        if seen.len() >= usize::from(request.max_commits) {
            break;
        }
        if !seen.insert(id.clone()) {
            continue;
        }
        let required = id == request.anchor;
        let address = NativeMetadataRef::CommitGraphRecord(id.clone());
        let mut inputs = inputs.into_iter();
        let object = match inputs.next().expect("required graph input") {
            Ok(Some(object)) => object,
            Ok(None) if required => {
                return Err(address.annotate_missing(LixError::new(
                    "LIX_NATIVE_METADATA_UNAVAILABLE",
                    "requested native metadata is unavailable",
                )));
            }
            Err(error) if required => return Err(error),
            Ok(None) | Err(_) => {
                if request.selection == MetadataSelection::History {
                    break;
                }
                continue; // Speculative siblings cannot fail a successful proof.
            }
        };
        if total.saturating_add(object.bytes.len()) > MAX_NATIVE_METADATA_PAYLOAD_BYTES {
            break;
        }
        let record = graph(&object)?;
        if request.selection == MetadataSelection::History {
            let next = record.parent_commit_ids.first();
            if previous_generation.is_some_and(|generation| record.generation >= generation)
                || next.is_some_and(|parent| seen.contains(&parent.to_string()))
            {
                if required {
                    return Err(invalid(
                        "native metadata walk anchor has invalid parent topology",
                    ));
                }
                break;
            }
            previous_generation = Some(record.generation);
        }
        pending.extend(
            selected_edges(request, &record)
                .into_iter()
                .take(32usize.saturating_sub(pending.len())),
        );
        total += object.bytes.len();
        objects.push(object);
        if request.include_state_headers {
            let address = NativeMetadataRef::CommitStateHeader(id.clone());
            match inputs.next().expect("requested optional header") {
                Ok(Some(header)) => {
                    if total.saturating_add(header.bytes.len()) > MAX_NATIVE_METADATA_PAYLOAD_BYTES
                    {
                        if required && request.require_state_header {
                            return Err(invalid(
                                "required graph/header group exceeds metadata budget",
                            ));
                        }
                        break;
                    }
                    if request.selection == MetadataSelection::Incorporation
                        && !request.stops.contains(&id)
                    {
                        pending.extend(
                            header_source(&header)?
                                .into_iter()
                                .take(32usize.saturating_sub(pending.len())),
                        );
                    }
                    total += header.bytes.len();
                    objects.push(header);
                }
                Ok(None) if required && request.require_state_header => {
                    return Err(address.annotate_missing(LixError::new(
                        "LIX_NATIVE_METADATA_UNAVAILABLE",
                        "required topology header is unavailable",
                    )));
                }
                Err(error) if required && request.require_state_header => return Err(error),
                Ok(None) => {}
                Err(_) => {
                    if request.selection == MetadataSelection::History {
                        break;
                    }
                }
            }
        }
        if stop_after_fetched_prefix && fetched.is_empty() {
            // A typed byte-budget refusal ends this optional read prefix only
            // after all records already admitted in the final provider page
            // have had the existing topology/error checks applied.
            break;
        }
    }
    let mut checkpoint_indexes = Vec::new();
    let mut checkpoint_ids = Vec::new();
    for (index, object) in objects.iter().enumerate() {
        if let NativeMetadataRef::CommitGraphRecord(_) = object.address {
            let record: CommitRecord =
                crate::storage_codec::decode("commit record", &object.bytes)?;
            if record.is_checkpoint {
                checkpoint_indexes.push(index);
                checkpoint_ids.push(record.commit_id);
            }
        }
    }
    let conversation_ids =
        match crate::checkpoint_conversation::load_checkpoint_conversations(read, &checkpoint_ids)
            .await
        {
            Ok(values) => values,
            Err(_) if checkpoint_indexes.first().is_some_and(|index| *index > 0) => {
                // A companion of unused lookahead cannot poison the anchor. Keep
                // the valid prefix; its client walk will request any needed edge.
                objects.truncate(checkpoint_indexes[0]);
                checkpoint_indexes.clear();
                checkpoint_ids.clear();
                Vec::new()
            }
            Err(_) if checkpoint_ids.len() > 1 => {
                // The anchor's own companion is required. Recheck just that fact;
                // speculative companion corruption may only truncate the suffix.
                let values = crate::checkpoint_conversation::load_checkpoint_conversations(
                    read,
                    &checkpoint_ids[..1],
                )
                .await?;
                objects.truncate(checkpoint_indexes[1]);
                checkpoint_indexes.truncate(1);
                checkpoint_ids.truncate(1);
                values
            }
            Err(error) => return Err(error),
        };
    for ((index, id), conversation_id) in checkpoint_indexes
        .into_iter()
        .zip(checkpoint_ids)
        .zip(conversation_ids)
    {
        objects[index].checkpoint_conversation =
            Some(super::native_metadata::CheckpointConversationEnvelope {
                commit_id: id.to_string(),
                conversation_id: super::native_metadata::RequiredNullable(conversation_id),
            });
    }
    let response = NativeMetadataResponse {
        dependencies: Default::default(),
        lix_id: repository_id.to_owned(),
        epoch_id: request.epoch_id.clone(),
        objects,
    };
    validate_response(repository_id, request, &response)?;
    Ok(response)
}

impl<S: Storage + Clone + Send + Sync + 'static> Lix<S> {
    #[cfg(test)]
    pub(crate) async fn read_sync_native_metadata_walk(
        &self,
        request: &NativeMetadataWalkRequest,
    ) -> Result<NativeMetadataResponse, LixError> {
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(Default::default()).await?;
        read_walk(&read, self.lix_id(), request).await
    }

    pub(crate) async fn read_sync_native_metadata_walk_leased(
        &self,
        request: &NativeMetadataWalkRequest,
        lease_id: &str,
    ) -> Result<NativeMetadataResponse, LixError> {
        validate_request(request)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(Default::default()).await?;
        crate::gc::require_native_baseline_lease(
            &read,
            lease_id,
            self.active_account_id(),
            crate::telemetry::unix_time_ms(),
        )
        .await?;
        read_walk(&read, self.lix_id(), request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Memory, Value, open_lix};

    async fn fixture() -> (Lix<Memory>, NativeMetadataWalkRequest) {
        fixture_with_revisions(12).await
    }

    async fn fixture_with_revisions(revisions: usize) -> (Lix<Memory>, NativeMetadataWalkRequest) {
        let lix = open_lix().await.unwrap();
        lix.execute(
            "INSERT INTO lix_file (path,content) VALUES ('/walk.txt',$1)",
            &[Value::Blob(b"initial".to_vec().into())],
        )
        .await
        .unwrap();
        for index in 0..revisions {
            lix.execute(
                "UPDATE lix_file SET content=$1 WHERE path='/walk.txt'",
                &[Value::Blob(format!("revision {index}").into_bytes().into())],
            )
            .await
            .unwrap();
            lix.execute(
                "SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)",
                &[],
            )
            .await
            .unwrap();
        }
        let anchor = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        (
            lix,
            NativeMetadataWalkRequest {
                epoch_id: uuid::Uuid::now_v7().to_string(),
                anchor,
                max_commits: MAX_METADATA_WALK_COMMITS,
                include_state_headers: true,
                selection: MetadataSelection::History,
                stops: Vec::new(),
                minimum_generation: None,
                require_state_header: false,
            },
        )
    }

    #[tokio::test]
    async fn walk_is_bounded_and_headers_are_optional() {
        let (lix, mut request) = fixture().await;
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        for headers in [false, true] {
            request.include_state_headers = headers;
            request.max_commits = 3;
            let response = read_walk(&read, lix.lix_id(), &request).await.unwrap();
            assert_eq!(
                response
                    .objects
                    .iter()
                    .filter(|o| matches!(o.address, NativeMetadataRef::CommitGraphRecord(_)))
                    .count(),
                3
            );
            assert!(response.objects.len() <= if headers { 6 } else { 3 });
            let exact = validate_response(lix.lix_id(), &request, &response).unwrap();
            assert_eq!(exact.objects.len(), response.objects.len());
        }
        drop(read);
        lix.close().await.unwrap();
    }

    #[tokio::test]
    async fn expanded_walk_crosses_old_commit_cap_and_keeps_global_byte_prefix() {
        let (lix, mut request) = fixture_with_revisions(40).await;
        request.max_commits = 40;
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let full = read_walk(&read, lix.lix_id(), &request).await.unwrap();
        assert_eq!(
            full.objects
                .iter()
                .filter(|object| matches!(object.address, NativeMetadataRef::CommitGraphRecord(_)))
                .count(),
            40
        );
        assert!(full.objects.len() > 32);
        validate_response(lix.lix_id(), &request, &full).unwrap();
        assert!(
            super::super::native_metadata::validate_native_metadata_response_for_addresses(
                lix.lix_id(),
                &request.epoch_id,
                &full
                    .objects
                    .iter()
                    .map(|object| object.address.clone())
                    .collect::<Vec<_>>(),
                super::super::native_metadata::MAX_NATIVE_METADATA_BATCH,
                &full,
            )
            .is_err()
        );

        let first_graph = full
            .objects
            .iter()
            .find(|object| matches!(object.address, NativeMetadataRef::CommitGraphRecord(_)))
            .unwrap();
        request.include_state_headers = false;
        request.max_commits = 4;
        let exact_first_record =
            read_walk_with_response_budget(&read, lix.lix_id(), &request, first_graph.bytes.len())
                .await
                .unwrap();
        assert_eq!(exact_first_record.objects.len(), 1);
        assert_eq!(exact_first_record.objects[0].address, first_graph.address);
        validate_response(lix.lix_id(), &request, &exact_first_record).unwrap();

        request.include_state_headers = true;
        request.require_state_header = false;
        let first_header = full
            .objects
            .iter()
            .find(|object| matches!(object.address, NativeMetadataRef::CommitStateHeader(_)))
            .unwrap();
        let graph_then_partial_header = read_walk_with_response_budget(
            &read,
            lix.lix_id(),
            &request,
            first_graph.bytes.len() + first_header.bytes.len() - 1,
        )
        .await
        .unwrap();
        assert_eq!(graph_then_partial_header.objects.len(), 1);
        assert_eq!(
            graph_then_partial_header.objects[0].address,
            first_graph.address
        );
        validate_response(lix.lix_id(), &request, &graph_then_partial_header).unwrap();

        request.require_state_header = true;
        let required_header_does_not_silently_truncate = read_walk_with_response_budget(
            &read,
            lix.lix_id(),
            &request,
            first_graph.bytes.len() + first_header.bytes.len() - 1,
        )
        .await;
        assert!(required_header_does_not_silently_truncate.is_err());
        drop(read);
        lix.close().await.unwrap();
    }

    #[tokio::test]
    async fn response_rejects_unrelated_reordered_and_duplicate_records() {
        let (lix, request) = fixture().await;
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let response = read_walk(&read, lix.lix_id(), &request).await.unwrap();
        assert!(response.objects.len() >= 6);
        let mut wrong = response.clone();
        wrong.objects.swap(0, 2);
        assert!(validate_response(lix.lix_id(), &request, &wrong).is_err());
        let mut wrong = response.clone();
        wrong.objects[2] = wrong.objects[0].clone();
        assert!(validate_response(lix.lix_id(), &request, &wrong).is_err());
        let mut wrong = response.clone();
        wrong.objects[1] = response.objects[3].clone();
        assert!(validate_response(lix.lix_id(), &request, &wrong).is_err());
        let mut wrong = response.clone();
        wrong.epoch_id = uuid::Uuid::now_v7().to_string();
        assert!(validate_response(lix.lix_id(), &request, &wrong).is_err());
        assert!(validate_response("another-repository", &request, &response).is_err());
        let mut topology_only = request.clone();
        topology_only.include_state_headers = false;
        assert!(validate_response(lix.lix_id(), &topology_only, &response).is_err());
        let mut shorter = request.clone();
        shorter.max_commits = 1;
        assert!(validate_response(lix.lix_id(), &shorter, &response).is_err());
        drop(read);
        lix.close().await.unwrap();
    }

    #[tokio::test]
    async fn retained_gap_returns_prefix_but_missing_anchor_fails() {
        let (lix, mut request) = fixture().await;
        request.include_state_headers = false;
        request.max_commits = 4;
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let response = read_walk(&read, lix.lix_id(), &request).await.unwrap();
        assert_eq!(response.objects.len(), 4);
        drop(read);
        let removed = response.objects[2].address.clone();
        let mut writes = adapter.new_write_set();
        writes.delete(space(&removed), key(&removed).unwrap());
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let prefix = read_walk(&read, lix.lix_id(), &request).await.unwrap();
        assert_eq!(prefix.objects.len(), 2);
        request.anchor = removed.id().to_owned();
        assert_eq!(
            read_walk(&read, lix.lix_id(), &request)
                .await
                .unwrap_err()
                .code,
            "LIX_NATIVE_METADATA_UNAVAILABLE"
        );
        drop(read);
        lix.close().await.unwrap();
    }

    #[tokio::test]
    async fn speculative_corruption_truncates_but_required_corruption_is_reported() {
        for corrupt_header in [false, true] {
            let (lix, mut request) = fixture().await;
            request.max_commits = 4;
            let adapter = lix.storage_adapter();
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let response = read_walk(&read, lix.lix_id(), &request).await.unwrap();
            let index = if corrupt_header { 5 } else { 4 };
            let damaged = response.objects[index].address.clone();
            drop(read);
            // Immutable storage correctly rejects overwriting an existing
            // header. Remove it first, then seed malformed bytes deliberately.
            let mut removal = adapter.new_write_set();
            removal.delete(space(&damaged), key(&damaged).unwrap());
            adapter
                .commit_write_set(removal, Default::default())
                .await
                .unwrap();
            let mut writes = adapter.new_write_set();
            writes.put(
                space(&damaged),
                key(&damaged).unwrap(),
                crate::storage_adapter::StorageValue {
                    bytes: bytes::Bytes::from_static(b"corrupt"),
                },
            );
            adapter
                .commit_write_set(writes, Default::default())
                .await
                .unwrap();
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let prefix = read_walk(&read, lix.lix_id(), &request).await.unwrap();
            assert_eq!(prefix.objects.len(), index);
            validate_response(lix.lix_id(), &request, &prefix).unwrap();
            // Optional selection does not turn corrupt data into absence or a
            // successful required read, including the omitted state header.
            assert!(
                read_many(
                    &read,
                    std::slice::from_ref(&damaged),
                    MAX_NATIVE_METADATA_PAYLOAD_BYTES,
                )
                    .await
                    .unwrap()
                    .values
                    .remove(0)
                    .is_err()
            );
            if !corrupt_header {
                request.anchor = damaged.id().to_owned();
                assert!(read_walk(&read, lix.lix_id(), &request).await.is_err());
            }
            drop(read);
            lix.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn incorporation_pages_follow_sources_and_require_only_the_anchor() {
        let (lix, mut request) = fixture().await;
        request.selection = MetadataSelection::Incorporation;
        request.require_state_header = true;
        request.max_commits = 4;
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let page = read_walk(&read, lix.lix_id(), &request).await.unwrap();
        assert!(page.objects.len() >= 4);
        assert!(page.objects.len() <= 8);
        validate_response(lix.lix_id(), &request, &page).unwrap();
        let anchor_header = NativeMetadataRef::CommitStateHeader(request.anchor.clone());
        assert!(
            page.objects
                .iter()
                .any(|object| object.address == anchor_header)
        );
        let speculative = page
            .objects
            .iter()
            .filter(|object| matches!(object.address, NativeMetadataRef::CommitStateHeader(_)))
            .find(|object| object.address != anchor_header)
            .unwrap()
            .address
            .clone();
        drop(read);
        let mut removal = adapter.new_write_set();
        removal.delete(space(&speculative), key(&speculative).unwrap());
        adapter
            .commit_write_set(removal, Default::default())
            .await
            .unwrap();
        let mut damage = adapter.new_write_set();
        damage.put(
            space(&speculative),
            key(&speculative).unwrap(),
            crate::storage_adapter::StorageValue {
                bytes: bytes::Bytes::from_static(b"corrupt"),
            },
        );
        adapter
            .commit_write_set(damage, Default::default())
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let page = read_walk(&read, lix.lix_id(), &request).await.unwrap();
        assert!(
            !page
                .objects
                .iter()
                .any(|object| object.address == speculative)
        );
        validate_response(lix.lix_id(), &request, &page).unwrap();
        request.anchor = speculative.id().to_owned();
        assert!(read_walk(&read, lix.lix_id(), &request).await.is_err());
        drop(read);
        lix.close().await.unwrap();
    }

    #[tokio::test]
    async fn selected_pages_cannot_invent_edges_or_cross_a_stop() {
        let (lix, mut request) = fixture().await;
        request.selection = MetadataSelection::Incorporation;
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let page = read_walk(&read, lix.lix_id(), &request).await.unwrap();
        assert!(page.objects.len() > 2);
        let mut reordered = page.clone();
        reordered.objects.swap(0, 2);
        assert!(validate_response(lix.lix_id(), &request, &reordered).is_err());
        request.stops = vec![request.anchor.clone()];
        let stopped = read_walk(&read, lix.lix_id(), &request).await.unwrap();
        assert_eq!(stopped.objects.len(), 2);
        assert!(validate_response(lix.lix_id(), &request, &page).is_err());
        drop(read);
        lix.close().await.unwrap();
    }

    #[test]
    fn only_typed_retryable_history_graph_misses_select_a_walk() {
        let epoch = "00000000-0000-7000-8000-000000000001";
        let graph =
            NativeMetadataRef::CommitGraphRecord("00000000-0000-7000-8000-000000000002".into());
        let point_error = graph.clone().annotate_missing(LixError::unknown("missing"));
        assert!(
            request_for_missing(epoch, std::slice::from_ref(&graph), &point_error)
                .unwrap()
                .is_none()
        );
        let history_error = NativeMetadataRef::annotate_history_demand(point_error.clone(), true);
        let walk = request_for_missing(epoch, std::slice::from_ref(&graph), &history_error)
            .unwrap()
            .unwrap();
        assert_eq!(walk.anchor, graph.id());
        assert_eq!(walk.max_commits, MAX_METADATA_WALK_COMMITS);
        assert!(walk.include_state_headers);
        let mut forbidden = point_error;
        forbidden.details.as_mut().unwrap()["nonRetryableAfterExecution"] = serde_json::json!(true);
        let expected = forbidden.details.clone();
        assert_eq!(
            NativeMetadataRef::annotate_history_demand(forbidden, true).details,
            expected
        );
        let ordinary = LixError::unknown("unrelated");
        assert_eq!(
            NativeMetadataRef::annotate_history_demand(ordinary, true).details,
            None
        );
    }

    #[test]
    fn request_bounds_are_enforced_before_storage_access() {
        let mut request = NativeMetadataWalkRequest {
            epoch_id: "00000000-0000-7000-8000-000000000001".into(),
            anchor: "00000000-0000-7000-8000-000000000002".into(),
            max_commits: 0,
            include_state_headers: true,
            selection: MetadataSelection::History,
            stops: Vec::new(),
            minimum_generation: None,
            require_state_header: false,
        };
        assert!(validate_request(&request).is_err());
        request.max_commits = MAX_METADATA_WALK_COMMITS + 1;
        assert!(validate_request(&request).is_err());
        request.max_commits = 1;
        assert!(validate_request(&request).is_ok());
        request.anchor = "noncanonical".into();
        assert!(validate_request(&request).is_err());
    }
}
