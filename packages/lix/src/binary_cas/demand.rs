//! Typed demands for blobs referenced by visible repository data. Generic CAS
//! probes retain their ordinary absent result, and corrupt metadata still fails.
use std::collections::BTreeSet;

use super::BlobId;
use crate::LixError;

/// Bounds one shared referenced-content preparation operation. This matches
/// the operation-scoped blob selection cap used by read fulfillment.
pub(crate) const MAX_REFERENCED_BLOB_HASHES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BlobManifestsRequired(pub(crate) Vec<BlobId>);

impl BlobManifestsRequired {
    pub(crate) fn new(mut ids: Vec<BlobId>) -> Result<Self, LixError> {
        if ids.is_empty() || ids.len() > MAX_REFERENCED_BLOB_HASHES {
            return Err(work_bound_error());
        }
        ids.sort_unstable();
        ids.dedup();
        if ids.is_empty() {
            return Err(work_bound_error());
        }
        Ok(Self(ids))
    }

    pub(crate) fn into_error(self) -> LixError {
        LixError::new(
            "LIX_PARTIAL_BLOB_MANIFESTS_REQUIRED",
            "referenced blob manifests are not resident",
        )
        .with_details(serde_json::json!({
            "missingBlobManifests": {
                "version": 1,
                "blobIds": self.0.iter().map(|id| id.to_hex()).collect::<Vec<_>>(),
            }
        }))
    }

    pub(crate) fn from_error(error: &LixError) -> Result<Option<Self>, LixError> {
        if error.code != "LIX_PARTIAL_BLOB_MANIFESTS_REQUIRED" {
            return Ok(None);
        }
        let invalid = || {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "invalid referenced blob manifest demand",
            )
        };
        let marker = error
            .details
            .as_ref()
            .and_then(|details| details.get("missingBlobManifests"))
            .ok_or_else(invalid)?;
        if marker.get("version").and_then(serde_json::Value::as_u64) != Some(1) {
            return Err(invalid());
        }
        let values = marker
            .get("blobIds")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(invalid)?;
        if values.is_empty() || values.len() > MAX_REFERENCED_BLOB_HASHES {
            return Err(invalid());
        }
        let mut ids = Vec::with_capacity(values.len());
        for value in values {
            let text = value.as_str().ok_or_else(invalid)?;
            let id = BlobId::from_hex(text)?;
            if id.to_hex() != text {
                return Err(invalid());
            }
            ids.push(id);
        }
        if ids.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(invalid());
        }
        Ok(Some(Self(ids)))
    }
}

pub(crate) fn normalize_referenced_blob_hashes(
    hashes: &[BlobId],
) -> Result<Vec<BlobId>, LixError> {
    if hashes.len() > MAX_REFERENCED_BLOB_HASHES {
        return Err(work_bound_error());
    }
    Ok(hashes.iter().copied().collect::<BTreeSet<_>>().into_iter().collect())
}

pub(crate) fn work_bound_error() -> LixError {
    LixError::new(
        "LIX_NATIVE_RECIPE_WORK_BOUND",
        "referenced blob dependency count limit exceeded",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_manifest_batch_demand_is_sorted_deduped_and_bounded() {
        let a = BlobId::from_content(b"a");
        let b = BlobId::from_content(b"b");
        let demand = BlobManifestsRequired::new(vec![b, a, b]).unwrap();
        assert_eq!(demand.0, vec![a.min(b), a.max(b)]);
        assert_eq!(
            BlobManifestsRequired::from_error(&demand.clone().into_error()).unwrap(),
            Some(demand)
        );

        let error = LixError::new("LIX_PARTIAL_BLOB_MANIFESTS_REQUIRED", "bad")
            .with_details(serde_json::json!({
                "missingBlobManifests": {
                    "version": 1,
                    "blobIds": [a.max(b).to_hex(), a.min(b).to_hex()],
                }
            }));
        assert!(BlobManifestsRequired::from_error(&error).is_err());

        let error = LixError::new("LIX_PARTIAL_BLOB_MANIFESTS_REQUIRED", "bad")
            .with_details(serde_json::json!({
                "missingBlobManifests": {
                    "version": 1,
                    "blobIds": vec!["a"; MAX_REFERENCED_BLOB_HASHES + 1],
                }
            }));
        assert!(BlobManifestsRequired::from_error(&error).is_err());
    }

    #[test]
    fn referenced_blob_hashes_are_bounded_before_deduplication() {
        let id = BlobId::from_content(b"same");
        assert_eq!(normalize_referenced_blob_hashes(&[id, id]).unwrap(), vec![id]);
        assert!(normalize_referenced_blob_hashes(
            &vec![id; MAX_REFERENCED_BLOB_HASHES + 1]
        )
        .is_err());
    }
}
