//! Neutral metadata shared by SQL execution and session orchestration.

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
    pub base_sha256: String,
    pub result_sha256: String,
    pub prefix_bytes: usize,
    pub suffix_bytes: usize,
    pub insert: Vec<u8>,
}

/// Transport-authored identity for one mutation statement.
///
/// Keeping the full operation proof alongside the compact namespace seed is
/// important: truncating the proof at the transport boundary would make two
/// colliding 128-bit seeds indistinguishable when the engine reserves a
/// generated-ID namespace durably.
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
    /// Retry-stable identity for generated IDs in this statement. The
    /// transaction binds both fields to the branch, file incarnation, plugin,
    /// and component generation before exposing the 128-bit namespace to a
    /// component. The full proof is retained for durable collision checks and
    /// is never part of the public plugin ABI.
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
