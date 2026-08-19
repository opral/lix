//! Portable, repository-scoped sync protocol values.
//!
//! These values describe immutable Lix commits and compare-and-swap ref
//! updates. They deliberately contain no query scopes, file projections,
//! admission metadata, or per-branch cursors.

use serde::{Deserialize, Serialize};

use super::commit::SyncCommit;

/// One atomic compare-and-swap update to a repository branch ref.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncRefUpdate {
    pub branch_id: String,
    pub expected_head_commit_id: Option<String>,
    pub head_commit_id: Option<String>,
}

/// Publishes complete immutable commits and their ref updates atomically.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPushRequest {
    pub commits: Vec<SyncCommit>,
    pub ref_updates: Vec<SyncRefUpdate>,
}

/// Acknowledges the repository cursor assigned to a successful push.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPushResponse {
    pub cursor: u64,
}

/// A compact immutable commit header used by hot-state bootstrap.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncCommitHeader {
    pub commit_id: String,
    pub parent_commit_ids: Vec<String>,
    pub account_id: String,
    pub created_at: String,
    /// Monotonic authenticated generation used by commit topology checks.
    pub generation: u64,
    /// Optional logarithmic first-parent jump target.
    pub first_parent_jump_commit_id: Option<String>,
    /// Number of first-parent edges covered by the jump target.
    pub first_parent_jump_span: Option<u64>,
    /// Canonical state-root digest when the authority can certify it.
    ///
    /// A snapshot must include this certificate for every direct first parent
    /// of an advertised branch head so the head remains topology-valid while
    /// historical commit bodies stay lazy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_root_id: Option<String>,
}

/// Current row state transferred during bootstrap.
///
/// Historical membership is intentionally absent. It is fetched through the
/// history endpoint as complete [`SyncCommit`] objects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncSnapshotRow {
    pub branch_id: String,
    pub schema_key: String,
    pub file_id: Option<String>,
    pub row_pk: serde_json::Value,
    pub snapshot: Option<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
    pub change_id: String,
    pub commit_id: String,
    pub created_at: String,
    pub updated_at: String,
    pub change_account_id: String,
    pub change_created_at: String,
    pub origin_key: Option<String>,
}

/// One stateless page of hot rows at an immutable branch head.
///
/// `continuation` is opaque to the client. Repeating the request is safe
/// because `(branch_id, head_commit_id)` pins an immutable state snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncSnapshotRowPage {
    pub branch_id: String,
    pub head_commit_id: String,
    pub rows: Vec<SyncSnapshotRow>,
    pub continuation: Option<String>,
}

/// Current server branch ref included in a hot-state bootstrap.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncBranchHead {
    pub branch_id: String,
    pub head_commit_id: Option<String>,
    /// BLAKE3 root of the live, tombstone-filtered row stream at this head.
    /// This is distinct from a commit header's physical state root.
    pub hot_state_root_id: String,
}

/// One item in the repository-wide live stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncEvent {
    pub cursor: u64,
    pub commits: Vec<SyncCommit>,
    pub ref_updates: Vec<SyncRefUpdate>,
}

/// Bootstrap is hot state plus lightweight topology; delta pages contain
/// complete commits. Both advance the same repository cursor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "kind"
)]
pub enum SyncRepositoryPullResponse {
    Snapshot {
        cursor: u64,
        default_branch_id: String,
        branches: Vec<SyncBranchHead>,
    },
    Delta {
        cursor: u64,
        events: Vec<SyncEvent>,
    },
}

/// Explicit immutable history fetch. It never changes refs or the live cursor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncHistoryResponse {
    pub commits: Vec<SyncCommit>,
    /// Bounded topology certificates for requested commits and the direct
    /// parent/jump boundaries needed to validate them without eager history.
    pub commit_headers: Vec<SyncCommitHeader>,
    pub missing_commit_ids: Vec<String>,
}

/// One BLAKE3-addressed FastCDC chunk in a canonical flat blob manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncBlobChunk {
    pub chunk_id: String,
    pub size_bytes: u64,
}

/// Storage-layout-independent description of one Lix binary blob.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncBlobManifest {
    pub blob_id: String,
    pub size_bytes: u64,
    pub chunks: Vec<SyncBlobChunk>,
}

/// Result of registering a manifest. Upload the missing chunks and retry the
/// same registration; the manifest becomes visible only once it verifies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncBlobRegistration {
    pub missing_chunk_ids: Vec<String>,
    pub complete: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pull_wire_is_explicitly_snapshot_or_delta() {
        let value = serde_json::to_value(SyncRepositoryPullResponse::Delta {
            cursor: 7,
            events: Vec::new(),
        })
        .expect("serialize pull response");
        assert_eq!(value["kind"], "delta");
        assert_eq!(value["cursor"], 7);
        assert!(value.get("branchId").is_none());
        assert!(value.get("schemas").is_none());
    }

    #[test]
    fn snapshot_row_page_pins_an_immutable_branch_head() {
        let page = SyncSnapshotRowPage {
            branch_id: "branch".to_owned(),
            head_commit_id: "head".to_owned(),
            rows: Vec::new(),
            continuation: Some("opaque".to_owned()),
        };
        let value = serde_json::to_value(page).expect("serialize snapshot row page");
        assert_eq!(value["branchId"], "branch");
        assert_eq!(value["headCommitId"], "head");
        assert_eq!(value["continuation"], "opaque");
        assert!(value.get("cursor").is_none());
    }

    #[test]
    fn snapshot_metadata_cannot_inline_large_head_bodies_or_rows() {
        let branches = (0..10_000)
            .map(|index| SyncBranchHead {
                branch_id: format!("branch-{index}"),
                head_commit_id: Some(format!("head-{index}")),
                hot_state_root_id: format!("{:064x}", index),
            })
            .collect();
        let value = serde_json::to_value(SyncRepositoryPullResponse::Snapshot {
            cursor: 9,
            default_branch_id: "branch-0".to_owned(),
            branches,
        })
        .expect("serialize bounded snapshot metadata");
        let object = value.as_object().expect("snapshot wire object");
        assert_eq!(
            object
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            ["branches", "cursor", "defaultBranchId", "kind"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        );
        assert!(serde_json::to_vec(&value).unwrap().len() < 2 * 1024 * 1024);
    }
}
