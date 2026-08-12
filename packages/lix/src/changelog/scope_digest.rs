//! Per-commit touched-scope digest carried on the immutable commit record.
//!
//! # Why this exists
//!
//! History traversal is cheap; the per-commit *membership test* is not. To
//! decide whether a reached commit contributed anything a history query asked
//! for, the reader used to load that commit's replay-state authority (header +
//! mutation inventory) — one extra point-read pair per reached commit, which
//! made `lix_file_history(...) WHERE path = ?` grow with history depth even
//! when the answer did not.
//!
//! Git does not pay this because a commit's tree hash *is already* the answer:
//! comparing it to the parent's tree is pointer equality against data the
//! traversal already holds. Lix has no tree, but it does already load a
//! [`CommitGraphNode`](crate::commit_graph::CommitGraphNode) per commit to find
//! parents. Publishing the touched-scope set on that record makes the
//! membership test ride on bytes already in hand.
//!
//! # What it is
//!
//! A fixed-size Bloom filter over the *collection scopes* a commit's delta
//! actually has members in. Two token families are inserted per scope:
//!
//! * `schema_key` alone, and
//! * `(schema_key, file_id)`,
//!
//! so a reader constrained by schema key alone, or by schema key **and** file
//! id, can both get an exact-absence proof. This is deliberately per-*scope*,
//! not per-*path*: a per-path artifact is a tree, and lix has no tree.
//!
//! # Why a negative is exact
//!
//! Bloom false positives only cost a load that would have happened anyway. A
//! negative is an absence proof **only** when the digest is [`Exact`], meaning
//! every member of the commit delta contributed its scope. Deltas whose member
//! scopes cannot be enumerated from the inventory alone publish [`Opaque`] and
//! are always loaded.
//!
//! [`Exact`]: CommitScopeDigestState::Exact
//! [`Opaque`]: CommitScopeDigestState::Opaque

use crate::LixError;

/// Filter width. Per-commit scope cardinality is small (a commit touches a
/// handful of collections, not a handful of thousands), so 256 bits keeps the
/// false-positive rate negligible while costing 32 B per commit — the low end
/// of the node-growth budget, because *every* commit-topology consumer pays
/// this, including merge-base, GC and branch operations that gain nothing.
pub(crate) const COMMIT_SCOPE_DIGEST_BYTES: usize = 32;
const COMMIT_SCOPE_DIGEST_HASHES: usize = 4;
const COMMIT_SCOPE_DIGEST_CONTEXT: &str = "lix per-commit touched collection scope v1";

/// Digest state discriminant stored on the record.
///
/// Three states rather than two: an *absent* digest (a commit record produced
/// by a writer that does not derive one) must stay distinguishable from an
/// *opaque* one (derived, but not exactly enumerable), because they have
/// different operational meanings — the first says "this repository is not
/// getting the optimization at all", the second says "this particular commit
/// shape cannot be proven".
pub(crate) mod state {
    pub(crate) const ABSENT: u8 = 0;
    pub(crate) const OPAQUE: u8 = 1;
    pub(crate) const EXACT: u8 = 2;
}

/// Scope the digest is keyed on.
///
/// Mirrors `CommitDeltaReplacementScope` without depending on the tracked-state
/// module, so the changelog record type stays free of storage-layer types.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CommitScopeKey {
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
}

/// Per-commit touched-scope digest.
///
/// Encoding idiom deliberately mirrors `CommitStateTouchedScopeFilter`
/// (`complete` + `bits`), the existing acceleration structure on the
/// commit-state manifest, rather than inventing a second one. The difference is
/// scope: that filter is *cumulative over ancestry* and lives on the manifest
/// this digest exists to avoid loading; this one is *local to one commit* and
/// lives on the commit record itself.
#[derive(Debug, Clone, Default, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitTouchedScopeDigest {
    /// One of [`state::ABSENT`], [`state::OPAQUE`], [`state::EXACT`].
    pub(crate) state: u8,
    #[musli(bytes)]
    pub(crate) bits: Vec<u8>,
}

impl CommitTouchedScopeDigest {
    /// No digest was derived by the writer of this record.
    pub(crate) fn absent() -> Self {
        Self {
            state: state::ABSENT,
            bits: Vec::new(),
        }
    }

    /// A digest was attempted but the delta's member scopes are not exactly
    /// enumerable, so no absence can be proven from it.
    pub(crate) fn opaque() -> Self {
        Self {
            state: state::OPAQUE,
            bits: Vec::new(),
        }
    }

    /// Builds an exact digest from the complete set of scopes this commit's
    /// delta has members in.
    pub(crate) fn exact<'a>(scopes: impl IntoIterator<Item = &'a CommitScopeKey>) -> Self {
        let mut bits = vec![0u8; COMMIT_SCOPE_DIGEST_BYTES];
        for scope in scopes {
            for token in scope_tokens(scope) {
                for bit in digest_bits(&token) {
                    bits[bit / 8] |= 1 << (bit % 8);
                }
            }
        }
        Self {
            state: state::EXACT,
            bits,
        }
    }

    pub(crate) fn is_absent(&self) -> bool {
        self.state == state::ABSENT
    }

    pub(crate) fn is_exact(&self) -> bool {
        self.state == state::EXACT
    }

    /// Proves that this commit's delta has **no** member in `scope`.
    ///
    /// `false` means "load the delta" — either because the digest cannot prove
    /// anything (absent/opaque) or because a bit collision made the scope look
    /// present. It never means "the scope is definitely there".
    pub(crate) fn proves_absent(&self, scope: &CommitScopeKey) -> bool {
        if !self.is_exact() || self.bits.len() != COMMIT_SCOPE_DIGEST_BYTES {
            return false;
        }
        let token = scope_token(scope);
        digest_bits(&token)
            .into_iter()
            .any(|bit| self.bits[bit / 8] & (1 << (bit % 8)) == 0)
    }

    pub(crate) fn validate(&self) -> Result<(), LixError> {
        match self.state {
            state::EXACT => {
                if self.bits.len() != COMMIT_SCOPE_DIGEST_BYTES {
                    return Err(LixError::unknown(
                        "exact commit touched-scope digest has the wrong length",
                    ));
                }
            }
            state::ABSENT | state::OPAQUE => {
                if !self.bits.is_empty() {
                    return Err(LixError::unknown(
                        "unauthoritative commit touched-scope digest carries bits",
                    ));
                }
            }
            other => {
                return Err(LixError::unknown(format!(
                    "commit touched-scope digest has unknown state {other}"
                )));
            }
        }
        Ok(())
    }
}

/// The two tokens one scope contributes: the schema family, and the
/// schema-family/file pair.
fn scope_tokens(scope: &CommitScopeKey) -> [Vec<u8>; 2] {
    [
        scope_token(&CommitScopeKey {
            schema_key: scope.schema_key.clone(),
            file_id: None,
        }),
        scope_token(scope),
    ]
}

fn scope_token(scope: &CommitScopeKey) -> Vec<u8> {
    let mut token = Vec::with_capacity(
        scope.schema_key.len() + scope.file_id.as_ref().map_or(0, String::len) + 2,
    );
    token.extend_from_slice(scope.schema_key.as_bytes());
    token.push(0);
    if let Some(file_id) = scope.file_id.as_ref() {
        token.push(1);
        token.extend_from_slice(file_id.as_bytes());
    }
    token
}

fn digest_bits(token: &[u8]) -> [usize; COMMIT_SCOPE_DIGEST_HASHES] {
    let digest = blake3::Hasher::new_derive_key(COMMIT_SCOPE_DIGEST_CONTEXT)
        .update(&(token.len() as u64).to_be_bytes())
        .update(token)
        .finalize();
    let bytes = digest.as_bytes();
    std::array::from_fn(|index| {
        let offset = index * 8;
        let hash = u64::from_be_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .expect("BLAKE3 supplies four u64 digest hashes"),
        );
        hash as usize % (COMMIT_SCOPE_DIGEST_BYTES * 8)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(schema_key: &str, file_id: Option<&str>) -> CommitScopeKey {
        CommitScopeKey {
            schema_key: schema_key.to_string(),
            file_id: file_id.map(str::to_string),
        }
    }

    #[test]
    fn exact_digest_never_proves_a_present_scope_absent() {
        let scopes = vec![
            scope("lix_file_descriptor", None),
            scope("lix_binary_blob_ref", Some("file-a")),
            scope("lix_key_value", None),
        ];
        let digest = CommitTouchedScopeDigest::exact(scopes.iter());
        for scope in &scopes {
            assert!(!digest.proves_absent(scope));
            // The schema-family token must also be present for every scope.
            assert!(!digest.proves_absent(&CommitScopeKey {
                schema_key: scope.schema_key.clone(),
                file_id: None,
            }));
        }
    }

    #[test]
    fn exact_digest_proves_untouched_schema_family_absent() {
        let digest =
            CommitTouchedScopeDigest::exact([&scope("lix_binary_blob_ref", Some("file-a"))]);
        assert!(digest.proves_absent(&scope("lix_directory_descriptor", None)));
        assert!(digest.proves_absent(&scope("lix_key_value", None)));
    }

    #[test]
    fn exact_digest_proves_untouched_file_scope_absent() {
        let digest =
            CommitTouchedScopeDigest::exact([&scope("lix_binary_blob_ref", Some("file-a"))]);
        assert!(digest.proves_absent(&scope("lix_binary_blob_ref", Some("file-b"))));
        assert!(!digest.proves_absent(&scope("lix_binary_blob_ref", None)));
    }

    #[test]
    fn absent_and_opaque_digests_prove_nothing() {
        for digest in [
            CommitTouchedScopeDigest::absent(),
            CommitTouchedScopeDigest::opaque(),
        ] {
            assert!(!digest.proves_absent(&scope("lix_key_value", None)));
            digest.validate().expect("state is valid");
        }
        assert!(CommitTouchedScopeDigest::absent().is_absent());
        assert!(!CommitTouchedScopeDigest::opaque().is_absent());
    }

    #[test]
    fn empty_commit_digest_proves_every_scope_absent() {
        let digest = CommitTouchedScopeDigest::exact(std::iter::empty());
        assert!(digest.is_exact());
        assert!(digest.proves_absent(&scope("lix_file_descriptor", None)));
        assert_eq!(digest.bits.len(), COMMIT_SCOPE_DIGEST_BYTES);
    }

    #[test]
    fn digest_round_trips_through_the_record_codec() {
        let digest = CommitTouchedScopeDigest::exact([&scope("lix_file_descriptor", None)]);
        let encoded =
            crate::storage_codec::encode("commit scope digest", &digest).expect("encode digest");
        let decoded: CommitTouchedScopeDigest =
            crate::storage_codec::decode("commit scope digest", &encoded).expect("decode digest");
        assert_eq!(decoded, digest);
    }
}
