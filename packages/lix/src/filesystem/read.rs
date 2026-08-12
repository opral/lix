use std::collections::BTreeSet;

use serde::Deserialize;

use crate::LixError;

use super::keys::BLOB_REF_SCHEMA_KEY;

/// Collects every file payload root selected by the authenticated serving
/// controls and retained commit/checkpoint roots. Tracked history is read from
/// commit state; current-only untracked rows are read from each control's
/// untracked selector through the live-state owner.
pub(crate) async fn collect_gc_binary_blob_roots<S>(
    store: &S,
    controls: &[(String, crate::branch::BranchHeadControl)],
    retained_commits: &BTreeSet<crate::changelog::CommitId>,
) -> Result<BTreeSet<crate::binary_cas::BlobId>, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    let request = crate::tracked_state::TrackedStateScanRequest {
        filter: crate::tracked_state::TrackedStateFilter {
            schema_keys: vec![BLOB_REF_SCHEMA_KEY.to_owned()],
            ..crate::tracked_state::TrackedStateFilter::default()
        },
        read_columns: crate::tracked_state::TrackedStateReadColumns {
            columns: vec!["snapshot_content".to_owned()],
        },
        limit: None,
    };
    let mut roots = BTreeSet::new();
    let current = crate::hot_state::TrackedHeadContext::new()
        .reader(store)
        .scan_live_batches_for_controls(controls, &request, Some(true))
        .await
        .map_err(|error| {
            LixError::new(
                error.code,
                format!("collect current binary blob roots: {}", error.message),
            )
        })?;
    for (_, rows) in current {
        for row in rows.iter() {
            let snapshot = row.snapshot_content().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "current binary blob reference has no snapshot",
                )
            })?;
            roots.insert(blob_id_from_snapshot(snapshot.as_str())?);
        }
    }

    let retained_schema_keys = [BLOB_REF_SCHEMA_KEY.to_owned()];
    for commit_id in retained_commits {
        for row in crate::tracked_state::load_retained_commit_snapshots_for_schemas(
            store,
            *commit_id,
            &retained_schema_keys,
        )
        .await?
        {
            if row.deleted {
                continue;
            }
            let snapshot = row.snapshot.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!("live binary blob reference in commit '{commit_id}' has no snapshot"),
                )
            })?;
            roots.insert(blob_id_from_snapshot(&snapshot)?);
        }
    }
    Ok(roots)
}

fn blob_id_from_snapshot(snapshot: &str) -> Result<crate::binary_cas::BlobId, LixError> {
    let snapshot: BlobRefSnapshot = serde_json::from_str(snapshot).map_err(|error| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("invalid live binary blob reference snapshot: {error}"),
        )
    })?;
    crate::binary_cas::BlobId::from_hex(&snapshot.blob_hash)
}

#[derive(Debug, Deserialize)]
struct BlobRefSnapshot {
    blob_hash: String,
}
