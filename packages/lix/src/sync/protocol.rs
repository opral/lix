//! Portable, repository-scoped sync protocol values.
//!
//! These values describe immutable Lix commits and compare-and-swap ref
//! updates. They deliberately contain no query scopes, file projections,
//! admission metadata, or per-branch cursors.

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

use super::commit::SyncCommit;

/// Returns the exact JSON size of a one-event delta response without cloning
/// commit payloads. Both HTTP admission and ordinary Authority transactions
/// use this single wire projection so an accepted event is always pullable.
pub(crate) fn encoded_delta_event_len(
    cursor: u64,
    commits: &[&SyncCommit],
    ref_updates: &[SyncRefUpdate],
) -> Result<usize, crate::LixError> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BorrowedEvent<'a> {
        cursor: u64,
        commits: &'a [&'a SyncCommit],
        ref_updates: &'a [SyncRefUpdate],
        inline_blobs: &'a [SyncBlobManifest],
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct BorrowedDelta<'a> {
        kind: &'static str,
        cursor: u64,
        events: [BorrowedEvent<'a>; 1],
    }

    serde_json::to_vec(&BorrowedDelta {
        kind: "delta",
        cursor,
        events: [BorrowedEvent {
            cursor,
            commits,
            ref_updates,
            inline_blobs: &[],
        }],
    })
    .map(|encoded| encoded.len())
    .map_err(|error| {
        crate::LixError::new(
            crate::LixError::CODE_INTERNAL_ERROR,
            format!("encode sync delta event: {error}"),
        )
    })
}

/// One atomic compare-and-swap update to a repository branch ref.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncRefUpdate {
    pub branch_id: String,
    pub expected_head_commit_id: Option<String>,
    /// Checkpoint coordinate paired with `expected_head_commit_id` for CAS.
    pub expected_checkpoint_commit_id: Option<String>,
    pub head_commit_id: Option<String>,
    /// The branch-specific checkpoint against which working changes are read.
    /// It is null only when `head_commit_id` is null for a ref deletion.
    pub checkpoint_commit_id: Option<String>,
    /// Authority-certified BLAKE3 root of the live, tombstone-filtered row
    /// stream at `head_commit_id`. Authority delta events must carry this for
    /// every headed ref; client push requests leave it null.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_state_root_id: Option<String>,
    /// Authority-certified live-row root at `checkpoint_commit_id`. It follows
    /// the same event-only rule as `head_state_root_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_state_root_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyncRefUpdateWire {
    branch_id: String,
    expected_head_commit_id: Option<String>,
    expected_checkpoint_commit_id: RequiredOption<String>,
    head_commit_id: Option<String>,
    checkpoint_commit_id: RequiredOption<String>,
    #[serde(default)]
    head_state_root_id: Option<String>,
    #[serde(default)]
    checkpoint_state_root_id: Option<String>,
}

impl<'de> Deserialize<'de> for SyncRefUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SyncRefUpdateWire::deserialize(deserializer)?;
        validate_checkpoint_coordinate(
            wire.expected_head_commit_id.as_deref(),
            wire.expected_checkpoint_commit_id.0.as_deref(),
        )
        .map_err(D::Error::custom)?;
        validate_checkpoint_coordinate(
            wire.head_commit_id.as_deref(),
            wire.checkpoint_commit_id.0.as_deref(),
        )
        .map_err(D::Error::custom)?;
        Ok(Self {
            branch_id: wire.branch_id,
            expected_head_commit_id: wire.expected_head_commit_id,
            expected_checkpoint_commit_id: wire.expected_checkpoint_commit_id.0,
            head_commit_id: wire.head_commit_id,
            checkpoint_commit_id: wire.checkpoint_commit_id.0,
            head_state_root_id: wire.head_state_root_id,
            checkpoint_state_root_id: wire.checkpoint_state_root_id,
        })
    }
}

/// Publishes complete immutable commits and their ref updates atomically.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPushRequest {
    pub commits: Vec<SyncCommit>,
    pub ref_updates: Vec<SyncRefUpdate>,
    /// Self-contained small blobs referenced by `commits`. Larger blobs keep
    /// using the manifest/chunk transfer lane.
    pub inline_blobs: Vec<SyncBlobManifest>,
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
    pub base_commit_id: Option<String>,
    pub account_id: String,
    pub created_at: String,
    #[serde(default)]
    pub global_scope: bool,
    /// Monotonic authenticated generation used by commit topology checks.
    pub generation: u64,
    /// Optional logarithmic first-parent jump target.
    pub first_parent_jump_commit_id: Option<String>,
    /// Number of first-parent edges covered by the jump target.
    pub first_parent_jump_span: Option<u64>,
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncBranchHead {
    pub branch_id: String,
    pub head_commit_id: Option<String>,
    /// The branch-specific checkpoint against which working changes are read.
    /// It is null only when `head_commit_id` is null.
    pub checkpoint_commit_id: Option<String>,
    /// BLAKE3 root of the live checkpoint row stream.
    pub checkpoint_state_root_id: String,
    /// BLAKE3 root of the live, tombstone-filtered row stream at this head.
    /// This is distinct from a commit header's physical state root.
    pub hot_state_root_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyncBranchHeadWire {
    branch_id: String,
    head_commit_id: Option<String>,
    checkpoint_commit_id: RequiredOption<String>,
    checkpoint_state_root_id: String,
    hot_state_root_id: String,
}

impl<'de> Deserialize<'de> for SyncBranchHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SyncBranchHeadWire::deserialize(deserializer)?;
        validate_checkpoint_coordinate(
            wire.head_commit_id.as_deref(),
            wire.checkpoint_commit_id.0.as_deref(),
        )
        .map_err(D::Error::custom)?;
        Ok(Self {
            branch_id: wire.branch_id,
            head_commit_id: wire.head_commit_id,
            checkpoint_commit_id: wire.checkpoint_commit_id.0,
            checkpoint_state_root_id: wire.checkpoint_state_root_id,
            hot_state_root_id: wire.hot_state_root_id,
        })
    }
}

struct RequiredOption<T>(Option<T>);

impl<'de, T> Deserialize<'de> for RequiredOption<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<T>::deserialize(deserializer).map(Self)
    }
}

fn validate_checkpoint_coordinate(
    head_commit_id: Option<&str>,
    checkpoint_commit_id: Option<&str>,
) -> Result<(), &'static str> {
    if head_commit_id.is_some() == checkpoint_commit_id.is_some() {
        Ok(())
    } else {
        Err("sync branch head and checkpoint must either both be present or both be null")
    }
}

/// One item in the repository-wide live stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncEvent {
    pub cursor: u64,
    pub commits: Vec<SyncCommit>,
    pub ref_updates: Vec<SyncRefUpdate>,
    /// Self-contained small blobs referenced by `commits`.
    pub inline_blobs: Vec<SyncBlobManifest>,
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
        lix_id: String,
        default_branch_id: String,
        branches: Vec<SyncBranchHead>,
    },
    Delta {
        cursor: u64,
        events: Vec<SyncEvent>,
    },
}

/// One complete-state fence in a bounded history page.
///
/// The digest covers the live (tombstone-filtered) row stream at `commit_id`.
/// It deliberately does not reuse the commit's physical state-root digest:
/// physical roots include tombstones and storage ancestry, while a detached
/// history fence is a self-contained live snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncHistoryBoundary {
    pub commit_id: String,
    pub live_state_root_id: String,
}

/// One bounded, first-parent history page. It never changes refs or the live
/// cursor. Commits are ordered oldest-to-newest so import is one forward pass.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncHistoryResponse {
    pub commits: Vec<SyncCommit>,
    /// Lightweight topology certificates needed to install the requested
    /// commits without loading historical member payloads.
    pub commit_headers: Vec<SyncCommitHeader>,
    /// Commits in this page with at least one parent outside the page.
    pub boundaries: Vec<SyncHistoryBoundary>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_bytes_base64: Option<String>,
}

/// Result of registering a manifest. Upload the missing chunks and retry the
/// same registration; the manifest becomes visible only once it verifies.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncBlobRegistration {
    pub missing_chunk_ids: Vec<String>,
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
                checkpoint_commit_id: Some(format!("checkpoint-{index}")),
                checkpoint_state_root_id: format!("{:064x}", index),
                hot_state_root_id: format!("{:064x}", index),
            })
            .collect();
        let value = serde_json::to_value(SyncRepositoryPullResponse::Snapshot {
            cursor: 9,
            lix_id: "00000000-0000-7000-8000-000000000001".to_owned(),
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
            ["branches", "cursor", "defaultBranchId", "kind", "lixId"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        );
        assert!(serde_json::to_vec(&value).unwrap().len() < 3 * 1024 * 1024);
    }

    #[test]
    fn checkpoint_coordinate_is_required_and_matches_ref_presence() {
        let update = SyncRefUpdate {
            branch_id: "branch".to_owned(),
            expected_head_commit_id: Some("old-head".to_owned()),
            expected_checkpoint_commit_id: Some("old-checkpoint".to_owned()),
            head_commit_id: Some("head".to_owned()),
            checkpoint_commit_id: Some("checkpoint".to_owned()),
            head_state_root_id: Some("0".repeat(64)),
            checkpoint_state_root_id: Some("1".repeat(64)),
        };
        let value = serde_json::to_value(&update).expect("serialize ref update");
        assert_eq!(value["checkpointCommitId"], "checkpoint");
        assert!(
            serde_json::from_value::<SyncRefUpdate>(serde_json::json!({
                "branchId": "branch",
                "expectedHeadCommitId": null,
                "expectedCheckpointCommitId": null,
                "headCommitId": "head"
            }))
            .is_err(),
            "omitting the checkpoint coordinate must be rejected"
        );
        assert!(
            serde_json::from_value::<SyncRefUpdate>(serde_json::json!({
                "branchId": "branch",
                "expectedHeadCommitId": null,
                "expectedCheckpointCommitId": null,
                "headCommitId": "head",
                "checkpointCommitId": null
            }))
            .is_err(),
            "a live ref must carry a live checkpoint coordinate"
        );
        serde_json::from_value::<SyncRefUpdate>(serde_json::json!({
            "branchId": "branch",
            "expectedHeadCommitId": "head",
            "expectedCheckpointCommitId": "checkpoint",
            "headCommitId": null,
            "checkpointCommitId": null
        }))
        .expect("ref deletion carries an explicit null checkpoint coordinate");

        let branch = SyncBranchHead {
            branch_id: "branch".to_owned(),
            head_commit_id: Some("head".to_owned()),
            checkpoint_commit_id: Some("checkpoint".to_owned()),
            checkpoint_state_root_id: "1".repeat(64),
            hot_state_root_id: "0".repeat(64),
        };
        let value = serde_json::to_value(&branch).expect("serialize branch head");
        assert_eq!(value["checkpointCommitId"], "checkpoint");
        assert!(
            serde_json::from_value::<SyncBranchHead>(serde_json::json!({
                "branchId": "branch",
                "headCommitId": "head",
                "checkpointStateRootId": "1".repeat(64),
                "hotStateRootId": "0".repeat(64)
            }))
            .is_err(),
            "snapshot metadata must not omit the checkpoint coordinate"
        );
    }

    #[test]
    fn encoded_delta_size_includes_checkpoint_coordinate() {
        let ref_updates = vec![SyncRefUpdate {
            branch_id: "branch".to_owned(),
            expected_head_commit_id: Some("old-head".to_owned()),
            expected_checkpoint_commit_id: Some("old-checkpoint".to_owned()),
            head_commit_id: Some("head".to_owned()),
            checkpoint_commit_id: Some("checkpoint".to_owned()),
            head_state_root_id: Some("0".repeat(64)),
            checkpoint_state_root_id: Some("1".repeat(64)),
        }];
        let expected = serde_json::to_vec(&SyncRepositoryPullResponse::Delta {
            cursor: 3,
            events: vec![SyncEvent {
                cursor: 3,
                commits: Vec::new(),
                ref_updates: ref_updates.clone(),
                inline_blobs: Vec::new(),
            }],
        })
        .expect("serialize delta response")
        .len();
        assert_eq!(
            encoded_delta_event_len(3, &[], &ref_updates).expect("measure delta response"),
            expected
        );
    }
}
