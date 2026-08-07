use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;

use bytes::Bytes;

use crate::storage::{
    CoreProjection, Key, KeyRange, MAX_SCAN_PAGE_ROWS, ProjectedValue, ScanOptions, StorageError,
};
use crate::storage_adapter::StorageAdapterRead;

use super::codec::corruption;
use super::model::{
    BlobChunkV1, BlobManifestV1, BranchSelectorV1, BranchSnapshotV1, ChangeCatalogEntry,
    ChangeCatalogOwner, ChangeId, ChangeObjectV1, CommitCatalogEntry, CommitId, CommitObjectV1,
    GcMarkPackV1, GcProgressSelectorV1, GcProgressV1, GlobalSelectorV1, RepositoryRootV1,
    SnapshotSelectorV1, SnapshotTargetV1, UploadPartV1, UploadProgressV1, UploadSelectorV1,
    branch_selector_key, gc_progress_selector_key, global_selector_key, snapshot_selector_key,
    upload_selector_key,
};
use super::object::{OBJECT_SPACE, ObjectDomain, ObjectId, authenticate_object_domain};
use super::state::{UNTRACKED_ROW_SPACE, decode_untracked_key, decode_untracked_value};
use super::tree::ordered_tree_edges;
use super::view::{CoherentView, SELECTOR_SPACE, load_object_bytes};

/// A deletion capability produced only by a complete authenticated mark over
/// one coherent selector epoch. Callers cannot construct or add arbitrary IDs.
#[derive(Debug)]
pub(crate) struct SweepPlan {
    pub(super) expected_global: Bytes,
    pub(super) orphan_object_ids: BTreeSet<ObjectId>,
}

#[derive(Clone, Debug)]
struct ChunkExpectation {
    declared_len: u64,
    claim: &'static str,
}

#[derive(Clone, Debug)]
struct SequenceExpectation {
    chunks: Vec<super::model::BlobChunkRefV1>,
    digest: [u8; 32],
    claim: &'static str,
}

/// Discovers an authenticated sweep under the exact raw global selector held
/// by `view`. Selector and untracked-root discovery is page-streamed with
/// `O(page)` transient memory. Graph marking is `O(reachable objects + edges)`
/// and retains only unique object IDs plus bounded chunk-validation claims;
/// object-space retirement is page-streamed. The final publication must exact
/// compare-and-swap the unchanged raw global selector and rotates the epoch.
pub(crate) async fn discover_sweep_plan<R>(
    view: &CoherentView<R>,
) -> Result<SweepPlan, StorageError>
where
    R: StorageAdapterRead,
{
    let roots = stream_selector_roots(view).await?;
    let marked = mark_reachable(view.read(), roots).await?;
    let mut orphan_object_ids = BTreeSet::new();
    let mut resume_after = None;
    loop {
        let page = view
            .read()
            .scan(
                OBJECT_SPACE,
                unbounded_range(),
                ScanOptions {
                    projection: CoreProjection::FullValue,
                    limit_rows: MAX_SCAN_PAGE_ROWS,
                    resume_after: resume_after.clone(),
                },
            )
            .await?;
        for entry in &page.entries {
            let id = object_id_from_key(&entry.key)?;
            let bytes = full_value(&entry.value, "object scan returned key-only data")?;
            let _ = authenticate_object_domain(id, bytes)?;
            if !marked.contains(&id) {
                orphan_object_ids.insert(id);
            }
        }
        resume_after = page.entries.last().map(|entry| entry.key.clone());
        if !page.has_more {
            break;
        }
        if resume_after.is_none() {
            return Err(corruption(
                "object scan claims more rows after an empty page",
            ));
        }
    }
    Ok(SweepPlan {
        expected_global: view.raw_global_selector().clone(),
        orphan_object_ids,
    })
}

/// Streams every authenticated selector and current untracked row. This is the
/// sole root decoder for branch, upload, checkpoint/recovery/undo/redo,
/// branch-tombstone, and GC-progress selectors. Unknown or mismatched selector
/// keys fail closed. Work is `O(selectors + current untracked rows)` and page
/// memory is bounded by the storage scan page.
async fn stream_selector_roots<R>(view: &CoherentView<R>) -> Result<Vec<ObjectId>, StorageError>
where
    R: StorageAdapterRead,
{
    let mut roots = Vec::new();
    let mut live_branches = BTreeSet::new();
    let mut saw_global = false;
    let mut resume_after = None;
    loop {
        let page = view
            .read()
            .scan(
                SELECTOR_SPACE,
                unbounded_range(),
                ScanOptions {
                    projection: CoreProjection::FullValue,
                    limit_rows: MAX_SCAN_PAGE_ROWS,
                    resume_after: resume_after.clone(),
                },
            )
            .await?;
        for entry in &page.entries {
            let key = entry.key.0.as_ref();
            let bytes = full_value(&entry.value, "selector scan returned key-only data")?;
            if key == global_selector_key().as_ref() {
                let selector = GlobalSelectorV1::decode(bytes)?;
                if bytes.as_ref() != view.raw_global_selector().as_ref()
                    || selector != view.global_selector()
                    || saw_global
                {
                    return Err(corruption(
                        "global selector scan does not match coherent view",
                    ));
                }
                saw_global = true;
                roots.push(selector.repository_root);
            } else if key.starts_with(b"branch/") {
                let selector = BranchSelectorV1::decode(bytes)?;
                if entry.key.0.as_ref() != branch_selector_key(selector.branch_id).as_ref() {
                    return Err(corruption("branch selector key/authenticated ID mismatch"));
                }
                let snapshot_bytes =
                    load_object_bytes(view.read(), selector.branch_snapshot_object_id).await?;
                let snapshot =
                    BranchSnapshotV1::decode(selector.branch_snapshot_object_id, &snapshot_bytes)?;
                if snapshot.branch_id != selector.branch_id {
                    return Err(corruption(
                        "branch selector does not authenticate its branch snapshot identity",
                    ));
                }
                validate_branch_snapshot_root(view.read(), &snapshot).await?;
                live_branches.insert(selector.branch_id);
                roots.push(selector.branch_snapshot_object_id);
            } else if key.starts_with(b"upload/") {
                let selector = UploadSelectorV1::decode(bytes)?;
                if entry.key.0.as_ref() != upload_selector_key(&selector.upload_id)?.as_ref() {
                    return Err(corruption("upload selector key/authenticated ID mismatch"));
                }
                let progress_bytes =
                    load_object_bytes(view.read(), selector.progress_object_id).await?;
                let progress =
                    UploadProgressV1::decode(selector.progress_object_id, &progress_bytes)?;
                if progress.upload_id != selector.upload_id
                    || progress.binding_digest != selector.binding_digest
                {
                    return Err(corruption(
                        "upload selector does not authenticate its progress binding",
                    ));
                }
                roots.push(selector.progress_object_id);
            } else if key == gc_progress_selector_key().as_ref() {
                let selector = GcProgressSelectorV1::decode(bytes)?;
                let progress_bytes =
                    load_object_bytes(view.read(), selector.progress_object_id).await?;
                let _ = GcProgressV1::decode(selector.progress_object_id, &progress_bytes)?;
                roots.push(selector.progress_object_id);
            } else {
                let selector = SnapshotSelectorV1::decode(bytes)?;
                if entry.key.0.as_ref()
                    != snapshot_selector_key(selector.role, selector.selector_id).as_ref()
                {
                    return Err(corruption(
                        "snapshot selector key/authenticated ID mismatch",
                    ));
                }
                let target_bytes =
                    load_object_bytes(view.read(), selector.target_object_id).await?;
                let target = SnapshotTargetV1::decode(selector.target_object_id, &target_bytes)?;
                if target.role != selector.role || target.selector_id != selector.selector_id {
                    return Err(corruption(
                        "snapshot selector does not authenticate its target identity",
                    ));
                }
                let snapshot_bytes =
                    load_object_bytes(view.read(), target.branch_snapshot_object_id).await?;
                let snapshot =
                    BranchSnapshotV1::decode(target.branch_snapshot_object_id, &snapshot_bytes)?;
                if snapshot.branch_id != target.branch_id
                    || snapshot.semantic_head_commit_object_id != target.semantic_commit_object_id
                {
                    return Err(corruption(
                        "snapshot target does not authenticate its branch/semantic head",
                    ));
                }
                validate_branch_snapshot_root(view.read(), &snapshot).await?;
                roots.push(selector.target_object_id);
            }
        }
        resume_after = page.entries.last().map(|entry| entry.key.clone());
        if !page.has_more {
            break;
        }
        if resume_after.is_none() {
            return Err(corruption(
                "selector scan claims more rows after an empty page",
            ));
        }
    }
    if !saw_global {
        return Err(corruption(
            "selector root scan did not find the global selector",
        ));
    }

    let mut resume_after = None;
    loop {
        let page = view
            .read()
            .scan(
                UNTRACKED_ROW_SPACE,
                unbounded_range(),
                ScanOptions {
                    projection: CoreProjection::FullValue,
                    limit_rows: MAX_SCAN_PAGE_ROWS,
                    resume_after: resume_after.clone(),
                },
            )
            .await?;
        for entry in &page.entries {
            let (branch, _) = decode_untracked_key(&entry.key.0)
                .map_err(|error| corruption(error.to_string()))?;
            let value = decode_untracked_value(full_value(
                &entry.value,
                "untracked scan returned key-only data",
            )?)
            .map_err(|error| corruption(error.to_string()))?;
            if live_branches.contains(&branch) {
                roots.extend(value.blob_manifest_object_ids);
            }
        }
        resume_after = page.entries.last().map(|entry| entry.key.clone());
        if !page.has_more {
            break;
        }
        if resume_after.is_none() {
            return Err(corruption(
                "untracked-root scan claims more rows after an empty page",
            ));
        }
    }
    if roots.contains(&ObjectId::ZERO) {
        return Err(corruption("root universe contains a zero object ID"));
    }
    roots.sort_unstable();
    roots.dedup();
    Ok(roots)
}

async fn validate_branch_snapshot_root<R>(
    read: &R,
    snapshot: &BranchSnapshotV1,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let commit_bytes = load_object_bytes(read, snapshot.semantic_head_commit_object_id).await?;
    let commit = CommitObjectV1::decode(snapshot.semantic_head_commit_object_id, &commit_bytes)?;
    if commit.global_state_root != snapshot.historical_global_state_root
        || commit.local_state_root != snapshot.local_state_root
    {
        return Err(corruption(
            "branch snapshot semantic head does not authenticate its state roots",
        ));
    }
    if let Some(ref_id) = snapshot.latest_ref_change_object_id {
        let bytes = load_object_bytes(read, ref_id).await?;
        match ChangeObjectV1::decode(ref_id, &bytes)? {
            ChangeObjectV1::BranchRef {
                branch_id,
                after_semantic_head_commit_object_id,
                ..
            } if branch_id == snapshot.branch_id
                && after_semantic_head_commit_object_id
                    == Some(snapshot.semantic_head_commit_object_id) => {}
            _ => {
                return Err(corruption(
                    "branch snapshot latest RefChange does not authenticate its head",
                ));
            }
        }
    }
    Ok(())
}

pub(super) async fn mark_reachable<R>(
    read: &R,
    roots: Vec<ObjectId>,
) -> Result<BTreeSet<ObjectId>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mut marked = BTreeSet::new();
    let mut pending = VecDeque::from(roots);
    let mut chunk_expectations: BTreeMap<ObjectId, ChunkExpectation> = BTreeMap::new();
    let mut sequences = Vec::new();
    let mut commit_claims = Vec::new();
    let mut change_claims = Vec::new();
    let mut upload_progress_claims = Vec::new();
    while let Some(id) = pending.pop_front() {
        if !marked.insert(id) {
            continue;
        }
        let bytes = load_object_bytes(read, id).await?;
        let domain = authenticate_object_domain(id, &bytes)?;
        let edges = object_edges(
            id,
            &bytes,
            domain,
            &mut chunk_expectations,
            &mut sequences,
            &mut commit_claims,
            &mut change_claims,
            &mut upload_progress_claims,
        )?;
        pending.extend(edges.into_iter().filter(|edge| !marked.contains(edge)));
    }
    validate_chunk_claims(read, &chunk_expectations, &sequences).await?;
    validate_catalog_claims(read, &commit_claims, &change_claims).await?;
    for progress in &upload_progress_claims {
        super::blob::authenticate_open_upload_progress(read, progress).await?;
    }
    Ok(marked)
}

fn object_edges(
    id: ObjectId,
    bytes: &[u8],
    domain: ObjectDomain,
    chunk_expectations: &mut BTreeMap<ObjectId, ChunkExpectation>,
    sequences: &mut Vec<SequenceExpectation>,
    commit_claims: &mut Vec<(CommitId, CommitCatalogEntry)>,
    change_claims: &mut Vec<(ChangeId, ChangeCatalogEntry)>,
    upload_progress_claims: &mut Vec<UploadProgressV1>,
) -> Result<Vec<ObjectId>, StorageError> {
    let mut edges = Vec::new();
    match domain {
        ObjectDomain::RepositoryRoot => {
            let value = RepositoryRootV1::decode(id, bytes)?;
            edges.extend([
                value.global_state_root,
                value.commit_catalog_root,
                value.change_catalog_root,
                value.retention_policy_root,
            ]);
        }
        ObjectDomain::BranchSnapshot => {
            let value = BranchSnapshotV1::decode(id, bytes)?;
            edges.extend([
                value.local_state_root,
                value.semantic_head_commit_object_id,
                value.historical_global_state_root,
            ]);
            edges.extend(value.latest_ref_change_object_id);
        }
        ObjectDomain::Commit => {
            let value = CommitObjectV1::decode(id, bytes)?;
            edges.extend(value.parent_commit_object_ids);
            edges.extend(value.member_change_object_ids);
            edges.extend([value.global_state_root, value.local_state_root]);
        }
        ObjectDomain::SemanticChange => {
            if !matches!(
                ChangeObjectV1::decode(id, bytes)?,
                ChangeObjectV1::Semantic { .. }
            ) {
                return Err(corruption(
                    "semantic Change object decoded as another domain",
                ));
            }
        }
        ObjectDomain::BranchRefChange => {
            let ChangeObjectV1::BranchRef {
                before_semantic_head_commit_object_id,
                after_semantic_head_commit_object_id,
                previous_ref_change_object_id,
                ..
            } = ChangeObjectV1::decode(id, bytes)?
            else {
                return Err(corruption("RefChange object decoded as a semantic Change"));
            };
            edges.extend(before_semantic_head_commit_object_id);
            edges.extend(after_semantic_head_commit_object_id);
            edges.extend(previous_ref_change_object_id);
        }
        ObjectDomain::OrderedTreeNode => {
            let tree = ordered_tree_edges(id, bytes)?;
            edges.extend(tree.object_ids);
            commit_claims.extend(tree.commit_entries);
            change_claims.extend(tree.change_entries);
        }
        ObjectDomain::UploadPart => {
            let value = UploadPartV1::decode(id, bytes)?;
            add_chunk_claims(chunk_expectations, &value.ordered_chunks, "upload part")?;
            edges.extend(
                value
                    .ordered_chunks
                    .iter()
                    .map(|chunk| chunk.chunk_object_id),
            );
            sequences.push(SequenceExpectation {
                chunks: value.ordered_chunks,
                digest: value.part_digest,
                claim: "upload part",
            });
        }
        ObjectDomain::UploadProgress => {
            let value = UploadProgressV1::decode(id, bytes)?;
            edges.push(value.receipt_tree_root);
            upload_progress_claims.push(value);
        }
        ObjectDomain::BlobChunk => {
            let _ = BlobChunkV1::decode(id, bytes)?;
        }
        ObjectDomain::BlobManifest => {
            let value = BlobManifestV1::decode(id, bytes)?;
            add_chunk_claims(chunk_expectations, &value.ordered_chunks, "blob manifest")?;
            edges.extend(
                value
                    .ordered_chunks
                    .iter()
                    .map(|chunk| chunk.chunk_object_id),
            );
            sequences.push(SequenceExpectation {
                chunks: value.ordered_chunks,
                digest: value.content_digest,
                claim: "blob manifest",
            });
        }
        ObjectDomain::SnapshotTarget => {
            let value = SnapshotTargetV1::decode(id, bytes)?;
            edges.extend([
                value.branch_snapshot_object_id,
                value.semantic_commit_object_id,
            ]);
        }
        ObjectDomain::GcMarkPack => {
            let value = GcMarkPackV1::decode(id, bytes)?;
            edges.extend(value.object_ids);
            edges.extend(value.next_pack_object_id);
        }
        ObjectDomain::GcProgress => {
            edges.push(GcProgressV1::decode(id, bytes)?.mark_pack_object_id);
        }
    }
    if edges.contains(&ObjectId::ZERO) {
        return Err(corruption("authenticated object contains a zero edge"));
    }
    Ok(edges)
}

async fn validate_catalog_claims<R>(
    read: &R,
    commits: &[(CommitId, CommitCatalogEntry)],
    changes: &[(ChangeId, ChangeCatalogEntry)],
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    for (key, entry) in commits {
        let bytes = load_object_bytes(read, entry.commit_object_id).await?;
        if CommitObjectV1::decode(entry.commit_object_id, &bytes)?.commit_id != *key {
            return Err(corruption(
                "CommitCatalog key does not match its authenticated Commit object",
            ));
        }
    }
    for (key, entry) in changes {
        let bytes = load_object_bytes(read, entry.change_object_id).await?;
        let change = ChangeObjectV1::decode(entry.change_object_id, &bytes)?;
        if change.change_id() != *key {
            return Err(corruption(
                "ChangeCatalog key does not match its authenticated Change object",
            ));
        }
        match (entry.owner, change) {
            (
                ChangeCatalogOwner::CommitMember {
                    commit_object_id,
                    ordinal,
                },
                ChangeObjectV1::Semantic { .. },
            ) => {
                let bytes = load_object_bytes(read, commit_object_id).await?;
                let commit = CommitObjectV1::decode(commit_object_id, &bytes)?;
                if commit.member_change_object_ids.get(ordinal as usize)
                    != Some(&entry.change_object_id)
                {
                    return Err(corruption(
                        "ChangeCatalog commit owner does not point back from its ordinal",
                    ));
                }
            }
            (
                ChangeCatalogOwner::BranchRef {
                    ref_change_object_id,
                    branch_id,
                },
                ChangeObjectV1::BranchRef {
                    branch_id: object_branch,
                    before_semantic_head_commit_object_id,
                    after_semantic_head_commit_object_id,
                    ..
                },
            ) if ref_change_object_id == entry.change_object_id && branch_id == object_branch => {
                for target in [
                    before_semantic_head_commit_object_id,
                    after_semantic_head_commit_object_id,
                ]
                .into_iter()
                .flatten()
                {
                    let bytes = load_object_bytes(read, target).await?;
                    let _ = CommitObjectV1::decode(target, &bytes)?;
                }
            }
            _ => {
                return Err(corruption(
                    "ChangeCatalog owner kind or authenticated back-edge is invalid",
                ));
            }
        }
    }
    Ok(())
}

fn add_chunk_claims(
    expectations: &mut BTreeMap<ObjectId, ChunkExpectation>,
    chunks: &[super::model::BlobChunkRefV1],
    claim: &'static str,
) -> Result<(), StorageError> {
    for chunk in chunks {
        match expectations.get(&chunk.chunk_object_id) {
            Some(existing) if existing.declared_len != chunk.declared_len => {
                return Err(corruption(format!(
                    "conflicting live chunk lengths from {} and {claim}",
                    existing.claim
                )));
            }
            Some(_) => {}
            None => {
                expectations.insert(
                    chunk.chunk_object_id,
                    ChunkExpectation {
                        declared_len: chunk.declared_len,
                        claim,
                    },
                );
            }
        }
    }
    Ok(())
}

async fn validate_chunk_claims<R>(
    read: &R,
    expectations: &BTreeMap<ObjectId, ChunkExpectation>,
    sequences: &[SequenceExpectation],
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    for (id, expected) in expectations {
        let bytes = load_object_bytes(read, *id).await?;
        let chunk = BlobChunkV1::decode(*id, &bytes)?;
        if chunk.bytes.len() as u64 != expected.declared_len {
            return Err(corruption(format!(
                "{} chunk bytes do not match declared length",
                expected.claim
            )));
        }
    }
    for sequence in sequences {
        let mut digest = blake3::Hasher::new();
        for chunk in &sequence.chunks {
            let bytes = load_object_bytes(read, chunk.chunk_object_id).await?;
            let chunk = BlobChunkV1::decode(chunk.chunk_object_id, &bytes)?;
            digest.update(&chunk.bytes);
        }
        if digest.finalize().as_bytes() != &sequence.digest {
            return Err(corruption(format!(
                "{} digest does not match authenticated chunk bytes",
                sequence.claim
            )));
        }
    }
    Ok(())
}

fn unbounded_range() -> KeyRange {
    KeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    }
}

fn object_id_from_key(key: &Key) -> Result<ObjectId, StorageError> {
    Ok(ObjectId::from_bytes(key.0.as_ref().try_into().map_err(
        |_| corruption("object-space key is not a 32-byte object ID"),
    )?))
}

fn full_value<'a>(value: &'a ProjectedValue, key_only: &str) -> Result<&'a Bytes, StorageError> {
    match value {
        ProjectedValue::FullValue(bytes) => Ok(bytes),
        ProjectedValue::KeyOnly => Err(corruption(key_only)),
    }
}
