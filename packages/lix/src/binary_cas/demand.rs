//! Typed demand for a blob referenced by visible repository data. Generic CAS
//! probes retain their ordinary absent result, and corrupt metadata still fails.
use super::BlobId;
use crate::LixError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BlobManifestRequired(pub(crate) BlobId);

impl BlobManifestRequired {
    pub(crate) fn into_error(self) -> LixError {
        LixError::new(
            "LIX_PARTIAL_BLOB_MANIFEST_REQUIRED",
            "referenced blob manifest is not resident",
        )
        .with_details(
            serde_json::json!({"missingBlobManifest":{"version":1,"blobId":self.0.to_hex()}}),
        )
    }

    pub(crate) fn from_error(error: &LixError) -> Result<Option<Self>, LixError> {
        if error.code != "LIX_PARTIAL_BLOB_MANIFEST_REQUIRED" {
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
            .and_then(|details| details.get("missingBlobManifest"))
            .ok_or_else(invalid)?;
        if marker.get("version").and_then(serde_json::Value::as_u64) != Some(1) {
            return Err(invalid());
        }
        let id = marker
            .get("blobId")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(invalid)?;
        let hash = BlobId::from_hex(id)?;
        if hash.to_hex() != id {
            return Err(invalid());
        }
        Ok(Some(Self(hash)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn typed_manifest_demand_rejects_missing_and_malformed_details() {
        let demand = BlobManifestRequired(BlobId::from_content(b"content"));
        assert_eq!(
            BlobManifestRequired::from_error(&demand.into_error()).unwrap(),
            Some(demand)
        );
        assert!(
            BlobManifestRequired::from_error(&LixError::new("OTHER", "absent"))
                .unwrap()
                .is_none()
        );
        for marker in [
            serde_json::json!(null),
            serde_json::json!({"version":2,"blobId":demand.0.to_hex()}),
            serde_json::json!({"version":1,"blobId":"invalid"}),
        ] {
            let error = LixError::new("LIX_PARTIAL_BLOB_MANIFEST_REQUIRED", "bad")
                .with_details(serde_json::json!({"missingBlobManifest":marker}));
            assert!(BlobManifestRequired::from_error(&error).is_err());
        }
    }
}
