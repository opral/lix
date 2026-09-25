//! Recover semantic omissions only from durably certified materialized snapshots.
use super::*;
use crate::migration::MigrationBoundedRead as BoundedRead;

pub(crate) struct CollectedSnapshotOmissions {
    pub(crate) owners: BTreeMap<CommitId, Option<bool>>,
    pub(crate) entries: usize,
    pub(crate) bytes: usize,
}
pub(crate) async fn collect_certified_snapshot_omitted_owners(
    read: &(impl StorageAdapterRead + ?Sized),
    max_entries: usize,
    max_bytes: usize,
) -> Result<CollectedSnapshotOmissions, LixError> {
    let bounded = BoundedRead::new(read, max_entries, max_bytes);
    let result = collect(&bounded).await;
    let (entries, bytes) = bounded.usage()?;
    Ok(CollectedSnapshotOmissions {
        owners: result?,
        entries,
        bytes,
    })
}

async fn collect<R: StorageAdapterRead + ?Sized>(
    read: &BoundedRead<'_, R>,
) -> Result<BTreeMap<CommitId, Option<bool>>, LixError> {
    let Some(state) = load_replica_state(read).await?.0 else {
        return Ok(BTreeMap::new());
    };
    let mut roots = BTreeSet::new();
    for (branch, coordinate) in state.authoritative_branches {
        let Some(certificate) = state.certified_branch_roots.get(&branch) else {
            continue;
        };
        if let AuthoritativeBranchCoordinate::Headed {
            head_commit_id,
            checkpoint_commit_id,
        } = coordinate
        {
            // The installer checked these logical certificates before recording
            // the immutable coordinates. Physical tree hashes use another codec.
            parse_sync_live_value_root_id(&certificate.head_state_root_id)?;
            parse_sync_live_value_root_id(&certificate.checkpoint_state_root_id)?;
            roots.insert(CommitId::parse_lix(
                &head_commit_id,
                "certified snapshot head",
            )?);
            roots.insert(CommitId::parse_lix(
                &checkpoint_commit_id,
                "certified snapshot checkpoint",
            )?);
        }
    }
    let mut candidates = BTreeSet::new();
    let tree = crate::tracked_state::TrackedStateTree::new();
    for id in roots {
        let Some(topology) = load_published_commit_state_topology(read, id).await? else {
            continue;
        };
        let Some(root) = topology
            .snapshot_root()
            .filter(|root| root.complete_state_fence && root.parent_roots.is_empty())
        else {
            continue;
        };
        let mut after = None;
        loop {
            let page = tree
                .scan_after(
                    read,
                    &root.root_id,
                    &crate::tracked_state::TrackedStateTreeScanRequest {
                        limit: Some(256),
                        ..Default::default()
                    },
                    after.as_ref(),
                )
                .await?;
            if page.is_empty() {
                break;
            }
            read.charge(page.len(), 0)?;
            after = page.last().map(|(key, _)| key.clone());
            candidates.extend(page.into_iter().map(|(_, value)| value.commit_id));
        }
        candidates.extend(crate::sync::load_complete_state_alias_source(read, id, None).await?);
        if let crate::tracked_state::CommitStateIncorporation::Complete(source) =
            topology.incorporation()
        {
            candidates.insert(source);
        }
    }
    let mut owners = BTreeMap::new();
    for id in candidates {
        if load_published_commit_state_topology(read, id)
            .await?
            .is_some()
            || commit_history_is_deferred(read, id).await?
        {
            continue;
        }
        // A materialized selected root may contain inherited GLOBAL rows.
        // Its containing lane cannot establish the omitted author's lane.
        owners.insert(id, None);
        read.charge(1, 16)?;
    }
    Ok(owners)
}
