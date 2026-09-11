//! Diagnostic addresses for missing immutable inputs of a partial replica.
//!
//! An address does not authorize fetching or certify reachability. Ordinary
//! repositories retain their existing corruption errors; a future partial
//! runtime must separately establish the baseline and permitted object set.

use crate::LixError;
use crate::storage_adapter::StorageSpace;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", content = "key", rename_all = "snake_case")]
pub(crate) enum NativeObjectRef {
    TrackedStateTreeChunk([u8; 32]),
    ScopedRangeNode([u8; 32]),
    MutationDirectoryNode([u8; 32]),
    MutationCatalog {
        commit_id: [u8; 16],
        expected_digest: [u8; 32],
    },
    CommitDeltaPart {
        commit_id: [u8; 16],
        part_index: u32,
        expected_digest: [u8; 32],
        replacement: bool,
    },
}

impl NativeObjectRef {
    pub(crate) const MAX_MISSING_BATCH: usize = 32;

    /// Report only addresses already selected by the native query's point-read
    /// plan. The first address retains the ordinary single-miss diagnostic;
    /// partial runtimes can use the bounded additional frontier in one fetch.
    pub(crate) fn annotate_missing_batch(
        addresses: impl IntoIterator<Item = Self>,
        error: LixError,
    ) -> LixError {
        let mut selected = Vec::new();
        for address in addresses {
            if !selected.contains(&address) {
                selected.push(address);
            }
            if selected.len() == Self::MAX_MISSING_BATCH {
                break;
            }
        }
        let Some(first) = selected.first().copied() else {
            return error;
        };
        let mut error = first.annotate_missing(error);
        if selected.len() > 1 {
            let details = error.details.get_or_insert_with(|| serde_json::json!({}));
            details
                .as_object_mut()
                .expect("native diagnostic details are object")
                .insert(
                    "missingNativeObjects".into(),
                    serde_json::json!({"version":1,"addresses":selected}),
                );
        }
        error
    }

    /// Validate a selected point-read frontier before reporting its absent
    /// members. Resident corruption is never converted into a missing demand.
    pub(crate) fn check_selected_read_batch(
        addresses: impl IntoIterator<Item = Self>,
        values: &[Option<crate::storage_adapter::StorageProjectedValue>],
        missing_error: LixError,
    ) -> Result<(), LixError> {
        let addresses = addresses.into_iter().collect::<Vec<_>>();
        if addresses.len() != values.len() {
            return Err(LixError::unknown(
                "native frontier response cardinality mismatch",
            ));
        }
        if values.iter().all(Option::is_some) {
            return Ok(());
        }
        for (address, value) in addresses.iter().zip(values) {
            if let Some(value) = value {
                let crate::storage_adapter::StorageProjectedValue::FullValue(bytes) = value else {
                    return Err(LixError::unknown(
                        "native frontier omitted resident payload",
                    ));
                };
                address.validate(bytes)?;
            }
        }
        Err(Self::annotate_missing_batch(
            addresses
                .into_iter()
                .zip(values)
                .filter_map(|(address, value)| value.is_none().then_some(address)),
            missing_error,
        ))
    }

    pub(crate) fn batch_from_missing_error(
        error: &LixError,
    ) -> Result<Option<Vec<Self>>, LixError> {
        let Some(marker) = error
            .details
            .as_ref()
            .and_then(|value| value.get("missingNativeObjects"))
        else {
            return Ok(None);
        };
        let invalid = || {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "invalid native missing-object batch diagnostic",
            )
        };
        if marker.get("version").and_then(|v| v.as_u64()) != Some(1) {
            return Err(invalid());
        }
        let values = marker
            .get("addresses")
            .and_then(|v| v.as_array())
            .ok_or_else(invalid)?;
        if values.is_empty() || values.len() > Self::MAX_MISSING_BATCH {
            return Err(invalid());
        }
        let mut result = Vec::with_capacity(values.len());
        for value in values {
            let address: Self = serde_json::from_value(value.clone()).map_err(|_| invalid())?;
            if result.contains(&address) {
                return Err(invalid());
            }
            result.push(address);
        }
        if Self::from_missing_error(error)?.is_some_and(|first| Some(&first) != result.first()) {
            return Err(invalid());
        }
        Ok(Some(result))
    }

    /// Extract a diagnostic address without making absence retryable. The
    /// caller must independently establish partial-mode baseline authority.
    pub(crate) fn from_missing_error(error: &LixError) -> Result<Option<Self>, LixError> {
        let Some(marker) = error
            .details
            .as_ref()
            .and_then(|details| details.get("missingNativeObject"))
        else {
            return Ok(None);
        };
        let malformed = || {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "invalid native object missing diagnostic",
            )
        };
        let marker = marker.as_object().ok_or_else(malformed)?;
        if marker.get("version").and_then(serde_json::Value::as_u64) != Some(1) {
            return Err(malformed());
        }
        if let Some(address) = marker.get("address") {
            return serde_json::from_value(address.clone())
                .map(Some)
                .map_err(|_| malformed());
        }
        let kind = marker
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(malformed)?;
        let key = marker
            .get("key")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(malformed)?;
        let digest = *blake3::Hash::from_hex(key)
            .map_err(|_| malformed())?
            .as_bytes();
        let address = match kind {
            "tracked_state_tree_chunk" => Self::TrackedStateTreeChunk(digest),
            "scoped_range_node" => Self::ScopedRangeNode(digest),
            "mutation_directory_node" => Self::MutationDirectoryNode(digest),
            _ => return Err(malformed()),
        };
        Ok(Some(address))
    }

    pub(crate) fn digest(self) -> [u8; 32] {
        match self {
            Self::TrackedStateTreeChunk(key)
            | Self::ScopedRangeNode(key)
            | Self::MutationDirectoryNode(key) => key,
            Self::MutationCatalog {
                expected_digest, ..
            }
            | Self::CommitDeltaPart {
                expected_digest, ..
            } => expected_digest,
        }
    }

    pub(crate) fn storage_key(self) -> Vec<u8> {
        match self {
            Self::MutationCatalog { commit_id, .. } => commit_id.to_vec(),
            Self::CommitDeltaPart {
                commit_id,
                part_index,
                expected_digest,
                replacement,
            } => {
                let mut key = commit_id.to_vec();
                key.extend_from_slice(&part_index.to_be_bytes());
                if replacement {
                    key.extend_from_slice(&expected_digest);
                }
                key
            }
            _ => self.digest().to_vec(),
        }
    }

    pub(crate) fn space(self) -> StorageSpace {
        match self {
            Self::TrackedStateTreeChunk(_) => super::storage::TRACKED_STATE_TREE_CHUNK_SPACE,
            Self::ScopedRangeNode(_) => super::scoped_range::SCOPED_RANGE_NODE_SPACE,
            Self::MutationDirectoryNode(_) => {
                super::mutation_directory::MUTATION_DIRECTORY_NODE_SPACE
            }
            Self::MutationCatalog { .. } => {
                super::storage::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE
            }
            Self::CommitDeltaPart { .. } => {
                super::storage::TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE
            }
        }
    }

    pub(crate) fn validate(self, bytes: &[u8]) -> Result<(), LixError> {
        match self {
            Self::TrackedStateTreeChunk(key) => super::storage::verify_chunk_hash(&key, bytes),
            Self::ScopedRangeNode(key) => super::scoped_range::validate_node_digest(&key, bytes),
            Self::MutationDirectoryNode(key) => {
                super::mutation_directory::validate_node_digest(&key, bytes)
            }
            Self::MutationCatalog {
                expected_digest, ..
            }
            | Self::CommitDeltaPart {
                expected_digest,
                replacement: false,
                ..
            } => super::storage::verify_chunk_hash(&expected_digest, bytes),
            Self::CommitDeltaPart {
                expected_digest,
                replacement: true,
                ..
            } => super::replacement_part::decode_replacement_part(&expected_digest, bytes)
                .map(|_| ()),
        }
    }

    pub(crate) fn annotate_missing(self, error: LixError) -> LixError {
        if matches!(
            self,
            Self::MutationCatalog { .. } | Self::CommitDeltaPart { .. }
        ) {
            let mut details = error
                .details
                .clone()
                .unwrap_or_else(|| serde_json::json!({}));
            if let Some(object) = details.as_object_mut() {
                object.insert(
                    "missingNativeObject".to_owned(),
                    serde_json::json!({"version": 1, "address": self}),
                );
            }
            return error.with_details(details);
        }
        let (kind, digest) = match self {
            Self::TrackedStateTreeChunk(digest) => ("tracked_state_tree_chunk", digest),
            Self::ScopedRangeNode(digest) => ("scoped_range_node", digest),
            Self::MutationDirectoryNode(digest) => ("mutation_directory_node", digest),
            _ => unreachable!("structured addresses handled above"),
        };
        // Keep original code/message so a missing object in an ordinary full
        // repository remains a corruption failure, never an automatic retry.
        let mut details = error
            .details
            .clone()
            .unwrap_or_else(|| serde_json::json!({}));
        if let Some(object) = details.as_object_mut() {
            object.insert(
                "missingNativeObject".to_owned(),
                serde_json::json!({
                    "version": 1,
                    "kind": kind,
                    "key": blake3::Hash::from(digest).to_hex().to_string(),
                }),
            );
        }
        error.with_details(details)
    }
}

/// UUID-addressed metadata needs authenticated authority provenance; unlike
/// NativeObjectRef, this address supplies no content hash or completeness proof.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(from = "NativeMetadataWire", into = "NativeMetadataWire")]
pub(crate) enum NativeMetadataRef {
    CommitStateHeader(String),
    CommitGraphRecord(String),
    ChangeLocator(String),
}

// Preserve existing commit metadata wire fields while naming change IDs accurately.
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum NativeMetadataWire {
    CommitStateHeader {
        #[serde(rename = "commitId")]
        commit_id: String,
    },
    CommitGraphRecord {
        #[serde(rename = "commitId")]
        commit_id: String,
    },
    ChangeLocator {
        #[serde(rename = "changeId")]
        change_id: String,
    },
}
impl From<NativeMetadataWire> for NativeMetadataRef {
    fn from(value: NativeMetadataWire) -> Self {
        match value {
            NativeMetadataWire::CommitStateHeader { commit_id } => {
                Self::CommitStateHeader(commit_id)
            }
            NativeMetadataWire::CommitGraphRecord { commit_id } => {
                Self::CommitGraphRecord(commit_id)
            }
            NativeMetadataWire::ChangeLocator { change_id } => Self::ChangeLocator(change_id),
        }
    }
}
impl From<NativeMetadataRef> for NativeMetadataWire {
    fn from(value: NativeMetadataRef) -> Self {
        match value {
            NativeMetadataRef::CommitStateHeader(commit_id) => {
                Self::CommitStateHeader { commit_id }
            }
            NativeMetadataRef::CommitGraphRecord(commit_id) => {
                Self::CommitGraphRecord { commit_id }
            }
            NativeMetadataRef::ChangeLocator(change_id) => Self::ChangeLocator { change_id },
        }
    }
}

impl NativeMetadataRef {
    pub(crate) fn id(&self) -> &str {
        match self {
            Self::CommitStateHeader(id) | Self::CommitGraphRecord(id) | Self::ChangeLocator(id) => {
                id
            }
        }
    }

    pub(crate) fn validate_address(&self) -> Result<(), LixError> {
        if crate::storage_codec::id_string::uuid_bytes_from_canonical(self.id()).is_none() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "native metadata ID must be a canonical UUID",
            ));
        }
        Ok(())
    }

    pub(crate) fn annotate_missing(self, error: LixError) -> LixError {
        let mut details = error
            .details
            .clone()
            .unwrap_or_else(|| serde_json::json!({}));
        if let Some(object) = details.as_object_mut() {
            object.insert(
                "missingNativeMetadata".to_owned(),
                serde_json::json!({"version": 1, "address": self}),
            );
        }
        error.with_details(details)
    }

    pub(crate) fn from_missing_error(error: &LixError) -> Result<Option<Self>, LixError> {
        let Some(marker) = error
            .details
            .as_ref()
            .and_then(|details| details.get("missingNativeMetadata"))
        else {
            return Ok(None);
        };
        let invalid = || {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "invalid native metadata missing diagnostic",
            )
        };
        if marker.get("version").and_then(serde_json::Value::as_u64) != Some(1) {
            return Err(invalid());
        }
        let address: Self =
            serde_json::from_value(marker.get("address").ok_or_else(invalid)?.clone())
                .map_err(|_| invalid())?;
        address.validate_address()?;
        Ok(Some(address))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn change_locator_wire_uses_change_id_and_preserves_commit_wire() {
        let id = "00000000-0000-7000-8000-000000000001";
        let value = serde_json::to_value(NativeMetadataRef::ChangeLocator(id.into())).unwrap();
        assert_eq!(value, json!({"kind":"change_locator", "changeId":id}));
        assert!(
            serde_json::from_value::<NativeMetadataRef>(
                json!({"kind":"change_locator", "commitId":id})
            )
            .is_err()
        );
        assert_eq!(
            serde_json::to_value(NativeMetadataRef::CommitGraphRecord(id.into())).unwrap(),
            json!({"kind":"commit_graph_record", "commitId":id})
        );
    }

    #[test]
    fn metadata_missing_diagnostic_preserves_errors_and_rejects_untyped_addresses() {
        for address in [
            NativeMetadataRef::CommitStateHeader("00000000-0000-0000-0000-000000000001".into()),
            NativeMetadataRef::CommitGraphRecord("00000000-0000-0000-0000-000000000002".into()),
            NativeMetadataRef::ChangeLocator("00000000-0000-0000-0000-000000000003".into()),
        ] {
            let error = address
                .clone()
                .annotate_missing(LixError::new("original", "missing authority"));
            assert_eq!(error.code, "original");
            assert_eq!(error.message, "missing authority");
            assert_eq!(
                NativeMetadataRef::from_missing_error(&error).unwrap(),
                Some(address)
            );
        }
        for address in [
            json!({"kind":"commit_state_header", "commitId":"invalid"}),
            json!({"kind":"arbitrary_namespace", "commitId":"00000000-0000-0000-0000-000000000001"}),
            json!({"kind":"commit_graph_record", "commitId":"00000000-0000-0000-0000-000000000001", "space":123}),
        ] {
            let error = LixError::new("original", "missing")
                .with_details(json!({"missingNativeMetadata":{"version":1,"address":address}}));
            assert!(NativeMetadataRef::from_missing_error(&error).is_err());
        }
    }

    #[test]
    fn structured_addresses_bind_physical_key_and_expected_digest() {
        let bytes = b"catalog-or-direct-part";
        let digest = *blake3::hash(bytes).as_bytes();
        let commit_id = [0x42; 16];
        let catalog = NativeObjectRef::MutationCatalog {
            commit_id,
            expected_digest: digest,
        };
        let direct = NativeObjectRef::CommitDeltaPart {
            commit_id,
            part_index: 257,
            expected_digest: digest,
            replacement: false,
        };
        let replacement = NativeObjectRef::CommitDeltaPart {
            commit_id,
            part_index: 257,
            expected_digest: digest,
            replacement: true,
        };
        assert_eq!(catalog.storage_key(), commit_id);
        assert_eq!(&direct.storage_key()[16..], &[0, 0, 1, 1]);
        assert_eq!(&replacement.storage_key()[20..], &digest);
        for address in [
            catalog,
            direct,
            replacement,
            NativeObjectRef::MutationDirectoryNode(digest),
        ] {
            let error = address.annotate_missing(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "missing input",
            ));
            assert_eq!(
                NativeObjectRef::from_missing_error(&error).unwrap(),
                Some(address)
            );
            assert_eq!(error.message, "missing input");
        }
        catalog.validate(bytes).unwrap();
        direct.validate(bytes).unwrap();
        assert!(catalog.validate(b"corruption").is_err());
        assert!(
            replacement.validate(bytes).is_err(),
            "replacement uses its native domain hash and codec"
        );
        assert!(
            NativeObjectRef::MutationDirectoryNode(digest)
                .validate(bytes)
                .is_err()
        );
    }

    #[test]
    fn missing_diagnostics_roundtrip_both_native_families_without_changing_errors() {
        for address in [
            NativeObjectRef::TrackedStateTreeChunk([0xab; 32]),
            NativeObjectRef::ScopedRangeNode([0x12; 32]),
        ] {
            let original =
                LixError::new(LixError::CODE_INTERNAL_ERROR, "missing referenced object");
            let annotated = address.annotate_missing(original.clone());
            assert_eq!(annotated.code, original.code);
            assert_eq!(annotated.message, original.message);
            assert_eq!(
                NativeObjectRef::from_missing_error(&annotated).unwrap(),
                Some(address)
            );
            let wire = serde_json::to_value(address).unwrap();
            let extracted = NativeObjectRef::from_missing_error(&annotated)
                .unwrap()
                .unwrap();
            assert_eq!(serde_json::to_value(extracted).unwrap(), wire);
        }
    }

    #[test]
    fn unrelated_and_corruption_errors_have_no_native_absence_address() {
        for details in [
            None,
            Some(json!({})),
            Some(json!({"unrelated": 1})),
            Some(json!("unrelated")),
        ] {
            let mut error =
                LixError::new(LixError::CODE_UNKNOWN, "tracked-state chunk hash mismatch");
            error.details = details;
            assert_eq!(NativeObjectRef::from_missing_error(&error).unwrap(), None);
        }
    }

    #[test]
    fn malformed_missing_diagnostic_is_rejected_instead_of_ignored() {
        let valid =
            json!({"version": 1, "kind": "tracked_state_tree_chunk", "key": "01".repeat(32)});
        let mut malformed = vec![json!(null), json!([]), json!("address"), json!({})];
        for (field, value) in [
            ("version", json!(2)),
            ("version", json!(0)),
            ("version", json!(-1)),
            ("version", json!("1")),
            ("version", json!(1.5)),
            ("kind", json!("unknown_native_family")),
            ("kind", json!(null)),
            ("key", json!("01".repeat(31))),
            ("key", json!("01".repeat(33))),
            ("key", json!("gg".repeat(32))),
            ("key", json!([1, 2, 3])),
        ] {
            let mut marker = valid.clone();
            marker[field] = value;
            malformed.push(marker);
        }
        for field in ["version", "kind", "key"] {
            let mut marker = valid.clone();
            marker.as_object_mut().unwrap().remove(field);
            malformed.push(marker);
        }
        for marker in malformed {
            let error = LixError::new(LixError::CODE_UNKNOWN, "missing object")
                .with_details(json!({"missingNativeObject": marker}));
            assert!(
                NativeObjectRef::from_missing_error(&error).is_err(),
                "malformed marker: {:?}",
                error.details
            );
        }
    }
}

#[cfg(test)]
mod missing_batch_tests {
    use super::*;
    #[test]
    fn diagnostic_frontier_is_bounded_deduplicated_and_keeps_original_failure() {
        let refs = (0u8..64).flat_map(|i| [NativeObjectRef::ScopedRangeNode([i; 32]); 2]);
        let error = NativeObjectRef::annotate_missing_batch(
            refs,
            LixError::new("CORRUPTION", "missing selected nodes"),
        );
        assert_eq!(error.code, "CORRUPTION");
        let batch = NativeObjectRef::batch_from_missing_error(&error)
            .unwrap()
            .unwrap();
        assert_eq!(batch.len(), 32);
        assert_eq!(batch[31], NativeObjectRef::ScopedRangeNode([31; 32]));
        assert_eq!(
            NativeObjectRef::from_missing_error(&error).unwrap(),
            Some(batch[0])
        );
    }
    #[test]
    fn selected_batch_reports_only_absences_and_refuses_corrupt_resident_member() {
        use crate::storage_adapter::StorageProjectedValue;
        let bytes = b"selected immutable payload";
        let resident = NativeObjectRef::TrackedStateTreeChunk(*blake3::hash(bytes).as_bytes());
        let missing_a = NativeObjectRef::TrackedStateTreeChunk([31; 32]);
        let missing_b = NativeObjectRef::TrackedStateTreeChunk([32; 32]);
        let values = [
            None,
            Some(StorageProjectedValue::FullValue(
                bytes::Bytes::copy_from_slice(bytes),
            )),
            None,
        ];
        let error = NativeObjectRef::check_selected_read_batch(
            [missing_a, resident, missing_b],
            &values,
            LixError::unknown("missing selected parts"),
        )
        .unwrap_err();
        assert_eq!(
            NativeObjectRef::batch_from_missing_error(&error)
                .unwrap()
                .unwrap(),
            vec![missing_a, missing_b]
        );
        let error = NativeObjectRef::check_selected_read_batch(
            [
                missing_a,
                NativeObjectRef::TrackedStateTreeChunk([99; 32]),
                missing_b,
            ],
            &values,
            LixError::unknown("missing selected parts"),
        )
        .unwrap_err();
        assert!(
            NativeObjectRef::batch_from_missing_error(&error)
                .unwrap()
                .is_none()
        );
        assert!(
            NativeObjectRef::from_missing_error(&error)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn truncated_all_present_frontier_never_means_complete() {
        use crate::storage_adapter::StorageProjectedValue;
        let address = NativeObjectRef::TrackedStateTreeChunk([1; 32]);
        for values in [
            vec![],
            vec![Some(StorageProjectedValue::FullValue(bytes::Bytes::new()))],
        ] {
            let error = NativeObjectRef::check_selected_read_batch(
                [address, address],
                &values,
                LixError::unknown("missing"),
            )
            .unwrap_err();
            assert!(error.message.contains("cardinality"));
            assert!(
                NativeObjectRef::batch_from_missing_error(&error)
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn malformed_batch_is_not_retry_authority() {
        let address = NativeObjectRef::ScopedRangeNode([1; 32]);
        for marker in [
            serde_json::json!({"version":2,"addresses":[address]}),
            serde_json::json!({"version":1,"addresses":[]}),
            serde_json::json!({"version":1,"addresses":[address,address]}),
            serde_json::json!({"version":1,"addresses":vec![address;33]}),
        ] {
            let error = LixError::unknown("missing")
                .with_details(serde_json::json!({"missingNativeObjects":marker}));
            assert!(NativeObjectRef::batch_from_missing_error(&error).is_err());
        }
    }
}
