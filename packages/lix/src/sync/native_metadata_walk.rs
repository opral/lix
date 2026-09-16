//! Bounded immutable first-parent metadata selection. This is demand-driven
//! prefetch, never an opening inventory or a claim that history is complete.
use super::native_metadata::{
    MAX_NATIVE_METADATA_PAYLOAD_BYTES, NativeMetadata, NativeMetadataRequest,
    NativeMetadataResponse, key, space, validate_bytes, validate_native_metadata_request,
    validate_native_metadata_response,
};
use crate::changelog::CommitRecord;
use crate::storage_adapter::{
    Storage, StorageAdapterRead, StorageGetManyRequest, StorageProjectedValue,
};
use crate::tracked_state::NativeMetadataRef;
use crate::{Lix, LixError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

// Two possible records per commit remain within the existing 32-record cap.
pub(crate) const MAX_METADATA_WALK_COMMITS: u8 = 16;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeMetadataWalkRequest {
    pub(crate) epoch_id: String,
    pub(crate) anchor: String,
    pub(crate) max_commits: u8,
    pub(crate) include_state_headers: bool,
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
            "native metadata walk requires between 1 and 16 commits",
        ));
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
    if response.objects.is_empty() || response.objects.len() > usize::from(request.max_commits) * 2
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
    validate_native_metadata_response(repository_id, &exact, response)?;
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

async fn read_one(
    read: &(impl StorageAdapterRead + ?Sized),
    address: NativeMetadataRef,
) -> Result<Option<NativeMetadata>, LixError> {
    let keys = [key(&address)?];
    let values = read
        .get_many(&[StorageGetManyRequest {
            space: space(&address),
            keys: &keys,
            opts: Default::default(),
        }])
        .await?
        .values;
    if values.len() != 1 {
        return Err(LixError::unknown(
            "native metadata walk storage cardinality mismatch",
        ));
    }
    match values.into_iter().next().unwrap() {
        None => Ok(None),
        Some(StorageProjectedValue::FullValue(bytes)) => {
            if bytes.len() > MAX_NATIVE_METADATA_PAYLOAD_BYTES {
                return Err(invalid("native metadata walk record exceeds byte bound"));
            }
            validate_bytes(&address, &bytes)?;
            Ok(Some(NativeMetadata {
                address,
                bytes: bytes.to_vec(),
            }))
        }
        Some(StorageProjectedValue::KeyOnly) => {
            Err(LixError::unknown("native metadata walk omitted payload"))
        }
    }
}

async fn read_walk(
    read: &(impl StorageAdapterRead + ?Sized),
    repository_id: &str,
    request: &NativeMetadataWalkRequest,
) -> Result<NativeMetadataResponse, LixError> {
    validate_request(request)?;
    let mut next = Some(request.anchor.clone());
    let mut objects = Vec::new();
    let mut total = 0usize;
    let mut seen = BTreeSet::new();
    let mut previous_generation = None;
    for _ in 0..request.max_commits {
        let Some(id) = next else {
            break;
        };
        let address = NativeMetadataRef::CommitGraphRecord(id.clone());
        let object = match read_one(read, address.clone()).await {
            Ok(Some(object)) => object,
            Ok(None) if objects.is_empty() => {
                return Err(address.annotate_missing(LixError::new(
                    "LIX_NATIVE_METADATA_UNAVAILABLE",
                    "requested native metadata is unavailable",
                )));
            }
            Err(error) if objects.is_empty() => return Err(error),
            // Older nodes are speculative. A short query must not depend on
            // their availability or integrity. Return the validated prefix;
            // a later required read still exposes the original failure.
            Ok(None) | Err(_) => break,
        };
        if total.saturating_add(object.bytes.len()) > MAX_NATIVE_METADATA_PAYLOAD_BYTES {
            if objects.is_empty() {
                return Err(invalid("native metadata walk payload exceeds bound"));
            }
            break;
        }
        let record = graph(&object)?;
        next = record.parent_commit_ids.first().map(ToString::to_string);
        if previous_generation.is_some_and(|generation| record.generation >= generation)
            || seen.contains(&id)
            || next
                .as_ref()
                .is_some_and(|parent| parent == &id || seen.contains(parent))
        {
            if objects.is_empty() {
                return Err(invalid(
                    "native metadata walk anchor has invalid parent topology",
                ));
            }
            break;
        }
        previous_generation = Some(record.generation);
        seen.insert(id.clone());
        total += object.bytes.len();
        objects.push(object);
        if request.include_state_headers {
            match read_one(read, NativeMetadataRef::CommitStateHeader(id)).await {
                Ok(Some(header)) => {
                    if total.saturating_add(header.bytes.len()) > MAX_NATIVE_METADATA_PAYLOAD_BYTES
                    {
                        break;
                    }
                    total += header.bytes.len();
                    objects.push(header);
                }
                Ok(None) => {}
                Err(_) => break, // Optional headers must not invalidate the required graph read.
            }
        }
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
        let lix = open_lix().await.unwrap();
        lix.execute(
            "INSERT INTO lix_file (path,content) VALUES ('/walk.txt',$1)",
            &[Value::Blob(b"initial".to_vec().into())],
        )
        .await
        .unwrap();
        for index in 0..12 {
            lix.execute(
                "UPDATE lix_file SET content=$1 WHERE path='/walk.txt'",
                &[Value::Blob(format!("revision {index}").into_bytes().into())],
            )
            .await
            .unwrap();
            lix.execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
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
            assert!(read_one(&read, damaged.clone()).await.is_err());
            if !corrupt_header {
                request.anchor = damaged.id().to_owned();
                assert!(read_walk(&read, lix.lix_id(), &request).await.is_err());
            }
            drop(read);
            lix.close().await.unwrap();
        }
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
