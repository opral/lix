//! Neutral metadata shared by SQL execution and session orchestration.

use sha2::{Digest as _, Sha256};

use crate::{Blob, LixError};

/// An immutable request blob whose complete SHA-256 proof has been established.
///
/// This is intentionally an opaque transport primitive. It lets a remote
/// protocol retain a verified full base and derive a verified splice successor
/// without exposing an unchecked provenance constructor to callers.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedRequestBlob {
    blob: Blob,
    sha256: String,
}

impl VerifiedRequestBlob {
    /// Establishes the full-content proof for an inbound blob once.
    pub fn verify(blob: Blob) -> Self {
        let sha256 = sha256_lower_hex(&blob);
        Self { blob, sha256 }
    }

    /// Returns the immutable bytes protected by this proof.
    pub fn blob(&self) -> &Blob {
        &self.blob
    }

    /// Returns the lowercase SHA-256 digest established for [`Self::blob`].
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Reconstructs, hashes, and proves a localized successor in one pass over
    /// the necessary output. The returned `Blob` is shared by SQL, transport
    /// provenance, and a possible successor cache entry.
    pub fn reconstruct_splice(
        &self,
        base_sha256: &str,
        result_sha256: &str,
        prefix_bytes: usize,
        suffix_bytes: usize,
        insert: Blob,
    ) -> Result<(Self, RequestBlobSpliceProvenance), LixError> {
        if base_sha256 != self.sha256 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "blob splice baseSha256 does not match the verified base bytes",
            ));
        }
        let reconstructed_len =
            splice_result_len(self.blob.len(), prefix_bytes, suffix_bytes, insert.len())?;
        let mut reconstructed = Vec::with_capacity(reconstructed_len);
        let mut hasher = Sha256::new();
        append_and_hash(&mut reconstructed, &mut hasher, &self.blob[..prefix_bytes]);
        append_and_hash(&mut reconstructed, &mut hasher, &insert);
        append_and_hash(
            &mut reconstructed,
            &mut hasher,
            &self.blob[self.blob.len() - suffix_bytes..],
        );
        let actual_result_sha256 = sha256_lower_hex_from_digest(&hasher.finalize());
        if result_sha256 != actual_result_sha256 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "blob splice resultSha256 does not match the reconstructed bytes",
            ));
        }

        let result = Self {
            blob: reconstructed.into(),
            sha256: actual_result_sha256,
        };
        let provenance = RequestBlobSpliceProvenance {
            base_sha256: self.sha256.clone(),
            result_sha256: result.sha256.clone(),
            prefix_bytes,
            suffix_bytes,
            insert,
            validated_result: result.blob.clone(),
        };
        Ok((result, provenance))
    }
}

/// Transport-side description of a blob parameter reconstructed from a
/// localized splice.
///
/// This is execution metadata, not a second public file-write API. Callers
/// continue to bind the fully reconstructed blob through ordinary SQL. The
/// remote protocol uses this sidecar so the file write path can hand an
/// incremental runtime the original localized edit without guessing it again
/// from two complete blobs.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestBlobSpliceProvenance {
    base_sha256: String,
    result_sha256: String,
    prefix_bytes: usize,
    suffix_bytes: usize,
    insert: Blob,
    /// Cheaply cloned identity of the exact immutable SQL blob validated by
    /// the constructor. Metadata cannot be transplanted onto different result
    /// bytes without forcing the ordinary full-diff path.
    validated_result: Blob,
}

impl RequestBlobSpliceProvenance {
    /// Constructs provenance only after proving both named byte versions and
    /// the exact splice reconstruction.
    ///
    /// This validation is intentionally paid once at the transport boundary.
    /// The engine can then retain an O(changed-region) hot path without
    /// trusting caller-populated digest or prefix/suffix fields.
    pub fn new_validated(
        base: &[u8],
        result: &Blob,
        base_sha256: &str,
        result_sha256: &str,
        prefix_bytes: usize,
        suffix_bytes: usize,
        insert: Vec<u8>,
    ) -> Result<Self, LixError> {
        let insert: Blob = insert.into();
        let actual_base_sha256 = sha256_lower_hex(base);
        if base_sha256 != actual_base_sha256 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "blob splice baseSha256 does not match the validated base bytes",
            ));
        }
        let actual_result_sha256 = sha256_lower_hex(result);
        if result_sha256 != actual_result_sha256 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "blob splice resultSha256 does not match the validated result bytes",
            ));
        }
        let expected_result_len =
            splice_result_len(base.len(), prefix_bytes, suffix_bytes, insert.len())?;
        let insert_end = prefix_bytes + insert.len();
        if expected_result_len != result.len()
            || result.get(..prefix_bytes) != base.get(..prefix_bytes)
            || result.get(prefix_bytes..insert_end) != Some(&insert[..])
            || result.get(insert_end..) != base.get(base.len().saturating_sub(suffix_bytes)..)
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "blob splice does not reconstruct the validated result bytes",
            ));
        }
        Ok(Self {
            base_sha256: actual_base_sha256,
            result_sha256: actual_result_sha256,
            prefix_bytes,
            suffix_bytes,
            insert,
            validated_result: result.clone(),
        })
    }

    pub(crate) fn base_sha256(&self) -> &str {
        &self.base_sha256
    }

    pub(crate) fn result_sha256(&self) -> &str {
        &self.result_sha256
    }

    pub(crate) fn prefix_bytes(&self) -> usize {
        self.prefix_bytes
    }

    pub(crate) fn suffix_bytes(&self) -> usize {
        self.suffix_bytes
    }

    pub(crate) fn insert(&self) -> &[u8] {
        &self.insert
    }

    pub(crate) fn matches_result(&self, result: &[u8]) -> bool {
        self.validated_result.len() == result.len()
            && self.validated_result.as_ptr() == result.as_ptr()
    }

    #[cfg(test)]
    pub(crate) fn new_validated_for_test(
        base: &[u8],
        result: &Blob,
        prefix_bytes: usize,
        suffix_bytes: usize,
        insert: Vec<u8>,
    ) -> Self {
        Self::new_validated(
            base,
            result,
            &sha256_lower_hex(base),
            &sha256_lower_hex(result),
            prefix_bytes,
            suffix_bytes,
            insert,
        )
        .expect("test blob splice should validate")
    }
}

fn sha256_lower_hex(bytes: &[u8]) -> String {
    sha256_lower_hex_from_digest(&Sha256::digest(bytes))
}

fn sha256_lower_hex_from_digest(digest: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(digest.len() * 2);
    for &byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn append_and_hash(output: &mut Vec<u8>, hasher: &mut Sha256, bytes: &[u8]) {
    let start = output.len();
    output.extend_from_slice(bytes);
    hasher.update(&output[start..]);
}

fn splice_result_len(
    base_len: usize,
    prefix_bytes: usize,
    suffix_bytes: usize,
    insert_len: usize,
) -> Result<usize, LixError> {
    if prefix_bytes > base_len
        || suffix_bytes > base_len
        || prefix_bytes
            .checked_add(suffix_bytes)
            .is_none_or(|covered| covered > base_len)
    {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "blob splice prefix and suffix overlap or exceed the validated base",
        ));
    }
    prefix_bytes
        .checked_add(insert_len)
        .and_then(|length| length.checked_add(suffix_bytes))
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "blob splice result length overflowed",
            )
        })
}

/// Optional caller-supplied identity for one mutation statement.
///
/// Keeping the full operation proof alongside the compact namespace seed is
/// important: truncating the proof would make two colliding 128-bit seeds
/// indistinguishable when the engine reserves a generated-ID namespace
/// durably. In-process integrations may supply this identity when they own a
/// retry contract. The current HTTP protocol does not expose mutation replay
/// identity.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutationIdentity {
    pub namespace_seed: [u8; 16],
    pub operation_proof: [u8; 32],
}

/// Per-statement metadata aligned with the statement's SQL parameter vector.
///
/// An entry is `Some` only when that exact parameter arrived over the remote
/// protocol as a validated blob splice. Locally supplied and full-blob
/// parameters use `None`.
#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecuteStatementMetadata {
    pub parameter_blob_splices: Vec<Option<RequestBlobSpliceProvenance>>,
    /// Optional retry-stable identity for generated IDs in this statement.
    /// When absent, the engine generates a fresh local identity for the
    /// operation. The transaction binds both fields to the branch, file
    /// incarnation, plugin, and component generation before exposing the
    /// 128-bit namespace to a component. The full proof is retained for
    /// durable collision checks and is never part of the public plugin ABI.
    pub mutation_identity: Option<MutationIdentity>,
}

impl ExecuteStatementMetadata {
    pub(crate) fn blob_splice_for_parameter(
        &self,
        one_based_parameter_index: usize,
    ) -> Option<&RequestBlobSpliceProvenance> {
        one_based_parameter_index
            .checked_sub(1)
            .and_then(|index| self.parameter_blob_splices.get(index))
            .and_then(Option::as_ref)
    }

    pub(crate) fn mutation_identity(&self) -> Option<MutationIdentity> {
        self.mutation_identity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verified_request_blob_reconstructs_once_and_shares_result_with_provenance() {
        let base: Blob = b"alpha,beta,omega".as_slice().into();
        let verified = VerifiedRequestBlob::verify(base.clone());
        let insert: Blob = b"BETA".as_slice().into();
        let insert_ptr = insert.as_ptr();
        let expected: Blob = b"alpha,BETA,omega".as_slice().into();

        let (result, provenance) = verified
            .reconstruct_splice(
                verified.sha256(),
                &sha256_lower_hex(&expected),
                6,
                6,
                insert,
            )
            .expect("verified splice should reconstruct");

        assert_eq!(result.blob(), &expected);
        assert!(provenance.matches_result(result.blob()));
        assert_eq!(provenance.validated_result.as_ptr(), result.blob().as_ptr());
        assert_eq!(provenance.insert.as_ptr(), insert_ptr);
    }

    #[test]
    fn verified_request_blob_rejects_forged_names_and_overlapping_bounds() {
        let base: Blob = b"alpha,beta,omega".as_slice().into();
        let verified = VerifiedRequestBlob::verify(base);
        let expected: Blob = b"alpha,BETA,omega".as_slice().into();

        let wrong_base = verified
            .reconstruct_splice(
                "0".repeat(64).as_str(),
                &sha256_lower_hex(&expected),
                6,
                6,
                b"BETA".as_slice().into(),
            )
            .expect_err("a forged base digest must be rejected");
        assert_eq!(wrong_base.code, LixError::CODE_INVALID_PARAM);

        let overlapping = verified
            .reconstruct_splice(
                verified.sha256(),
                &sha256_lower_hex(&expected),
                12,
                12,
                b"BETA".as_slice().into(),
            )
            .expect_err("overlapping splice bounds must be rejected");
        assert_eq!(overlapping.code, LixError::CODE_INVALID_PARAM);
    }
}
