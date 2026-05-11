use crate::tracked_state::codec::{
    encode_key_ref as encode_tracked_key_ref, encode_value_ref as encode_tracked_value_ref,
};
use crate::tracked_state::types::{
    TrackedStateIndexValueRef, TrackedStateKey, TrackedStateKeyRef, TrackedStateTreeScanRequest,
};
use crate::tracked_state::TrackedStateScanRequest;
use crate::NullableKeyFilter;

const NULL_COMPONENT: &str = "\0";
const VALUE_PREFIX: &str = "\u{1}";

pub(crate) struct ByFileIndex;

impl ByFileIndex {
    pub(crate) fn should_use(request: &TrackedStateScanRequest) -> bool {
        !request.filter.file_ids.is_empty()
            && request
                .filter
                .file_ids
                .iter()
                .all(|filter| !matches!(filter, NullableKeyFilter::Any))
    }

    pub(crate) fn scan_request_from_tracked(
        request: &TrackedStateScanRequest,
    ) -> TrackedStateTreeScanRequest {
        let schema_keys = request
            .filter
            .file_ids
            .iter()
            .filter_map(|filter| match filter {
                NullableKeyFilter::Any => None,
                NullableKeyFilter::Null => Some(NULL_COMPONENT.to_string()),
                NullableKeyFilter::Value(file_id) => Some(value_component(file_id)),
            })
            .collect();
        let file_ids = request
            .filter
            .schema_keys
            .iter()
            .cloned()
            .map(NullableKeyFilter::Value)
            .collect();
        TrackedStateTreeScanRequest {
            schema_keys,
            entity_ids: request.filter.entity_ids.clone(),
            file_ids,
            limit: None,
        }
    }

    pub(crate) fn encode_key_ref(row: TrackedStateKeyRef<'_>) -> Vec<u8> {
        let schema_key = component(row.file_id);
        encode_tracked_key_ref(TrackedStateKeyRef {
            schema_key: &schema_key,
            file_id: Some(row.schema_key),
            entity_id: row.entity_id,
        })
    }

    pub(crate) fn primary_key_from_index_key(
        index_key: TrackedStateKey,
    ) -> Option<TrackedStateKey> {
        let schema_key = index_key.file_id?;
        Some(TrackedStateKey {
            schema_key,
            file_id: file_id_from_component(&index_key.schema_key)?,
            entity_id: index_key.entity_id,
        })
    }

    pub(crate) fn encode_header_value_ref(value: TrackedStateIndexValueRef<'_>) -> Vec<u8> {
        encode_tracked_value_ref(value)
    }
}

fn component(file_id: Option<&str>) -> String {
    match file_id {
        Some(file_id) => value_component(file_id),
        None => NULL_COMPONENT.to_string(),
    }
}

fn value_component(file_id: &str) -> String {
    format!("{VALUE_PREFIX}{file_id}")
}

fn file_id_from_component(component: &str) -> Option<Option<String>> {
    if component == NULL_COMPONENT {
        return Some(None);
    }
    component
        .strip_prefix(VALUE_PREFIX)
        .map(|file_id| Some(file_id.to_string()))
}
