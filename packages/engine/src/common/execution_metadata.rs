//! Neutral metadata shared by SQL execution and session orchestration.

use sha2::{Digest as _, Sha256};

use crate::{Blob, LixError};

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
    insert: Vec<u8>,
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
        if prefix_bytes > base.len()
            || suffix_bytes > base.len()
            || prefix_bytes
                .checked_add(suffix_bytes)
                .is_none_or(|covered| covered > base.len())
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "blob splice prefix and suffix overlap or exceed the validated base",
            ));
        }
        let insert_end = prefix_bytes.checked_add(insert.len()).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "blob splice result length overflowed",
            )
        })?;
        let expected_result_len = insert_end.checked_add(suffix_bytes).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "blob splice result length overflowed",
            )
        })?;
        if expected_result_len != result.len()
            || result.get(..prefix_bytes) != base.get(..prefix_bytes)
            || result.get(prefix_bytes..insert_end) != Some(insert.as_slice())
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
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
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
