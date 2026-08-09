use std::collections::{BTreeMap, BTreeSet};

use crate::storage::{
    BeginScanOptions, CoreProjection, GetManyRequest, GetManyResult, GetOptions, Key, KeyRange,
    Prefix, ProjectedValue, ScanCursor, ScanOrder, StorageError, StorageSpace,
};
use crate::storage_adapter::StorageAdapterRead;

use super::codec::corruption;
use super::model::{
    BlobChunkV1, BranchSelectorV1, CanonicalBranchId, ChangeCatalogEntry, ChangeCatalogOwner,
    ChangeId, ChangeObjectV1, CommitCatalogEntry, CommitId, CommitMemberV1, CommitObjectV1,
    GlobalSelectorV1, RepositoryRootV1, branch_selector_key, global_selector_key,
};
use super::object::ObjectId;
use super::state::{
    HistoricalStateDiffEntry, HistoricalStateRow, StateCell, StateValue, decode_state_key,
    decode_state_value,
};
use super::tree::{
    ImmutableObjectSet, OrderedTreeMutation, apply_ordered_mutations,
    apply_ordered_mutations_idempotent_inserts, diff_ordered_tree_on_read, lookup_on_read,
    scan_bounded_page_on_read, scan_page_on_read, validate_root_on_read,
};
use super::view::{CoherentView, SELECTOR_SPACE, open_coherent_view_on_read};

const BRANCH_SELECTOR_PREFIX: &[u8] = b"branch/";
const BRANCH_SCAN_PAGE_ROWS: usize = 256;
const CATALOG_SCAN_PAGE_ROWS: usize = 1024;

/// Loads a commit's complete authenticated member closure. Small commits keep
/// the original inline representation; larger commits resolve the immutable
/// CommitMemberPageV1 chain while checking commit identity, contiguous
/// ordinals, cycles, and duplicate Change object identities.
pub(crate) async fn load_commit_members<R>(
    read: &R,
    commit: &CommitObjectV1,
) -> Result<Vec<CommitMemberV1>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let Some(mut page_id) = commit.member_page_root else {
        return Ok(commit.members.clone());
    };
    if !commit.members.is_empty() {
        return Err(corruption("paged commit carries an inline member closure"));
    }
    let mut members = Vec::new();
    let mut visited = BTreeSet::new();
    loop {
        if !visited.insert(page_id) {
            return Err(corruption("commit member page chain contains a cycle"));
        }
        let bytes = super::view::load_object_bytes(read, page_id).await?;
        let page = super::model::CommitMemberPageV1::decode(page_id, &bytes)?;
        if page.commit_id != commit.commit_id
            || page.start_ordinal
                != u32::try_from(members.len())
                    .map_err(|_| corruption("commit member page ordinal exceeds u32"))?
        {
            return Err(corruption(
                "commit member page chain has a mismatched commit or ordinal",
            ));
        }
        members.extend(page.members);
        match page.next_page_object_id {
            Some(next) => page_id = next,
            None => break,
        }
    }
    let mut unique_changes = BTreeSet::new();
    for member in &members {
        if !unique_changes.insert(member.change_object_id()) {
            return Err(corruption(
                "commit member page chain repeats a change object",
            ));
        }
    }
    Ok(members)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StateSource {
    Global,
    Branch,
}

#[derive(Clone, Debug)]
pub(crate) struct VisibleStateRow {
    pub(crate) encoded_key: Vec<u8>,
    pub(crate) value: StateValue,
    pub(crate) source: StateSource,
    pub(super) view_instance_id: u64,
}

impl PartialEq for VisibleStateRow {
    fn eq(&self, other: &Self) -> bool {
        self.encoded_key == other.encoded_key
            && self.value == other.value
            && self.source == other.source
    }
}

impl Eq for VisibleStateRow {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StateTreeMutation {
    Insert { key: Vec<u8>, value: Vec<u8> },
    Update { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
}

impl StateTreeMutation {
    pub(crate) fn insert(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self::Insert { key, value }
    }

    pub(crate) fn update(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self::Update { key, value }
    }

    pub(crate) fn remove(key: Vec<u8>) -> Self {
        Self::Remove { key }
    }

    fn into_ordered(self) -> OrderedTreeMutation {
        match self {
            Self::Insert { key, value } => OrderedTreeMutation::Insert { key, value },
            Self::Update { key, value } => OrderedTreeMutation::Update { key, value },
            Self::Remove { key } => OrderedTreeMutation::Delete { key },
        }
    }
}

#[derive(Debug)]
pub(crate) struct StateTreeEdit {
    pub(super) base_root: ObjectId,
    pub(crate) root: ObjectId,
    entry_count: u64,
    copied_nodes: usize,
    pub(crate) added_blob_roots: BTreeMap<ObjectId, ()>,
    pub(super) wrote_tombstone: bool,
    pub(super) written_commit_ids: BTreeSet<[u8; 16]>,
    pub(super) objects: ImmutableObjectSet,
}

impl StateTreeEdit {
    pub(crate) fn entry_count(&self) -> u64 {
        self.entry_count
    }

    pub(crate) fn copied_nodes(&self) -> usize {
        self.copied_nodes
    }
}

#[derive(Debug)]
pub(crate) struct CatalogTreeEdit {
    pub(super) base_root: ObjectId,
    pub(crate) root: ObjectId,
    entry_count: u64,
    copied_nodes: usize,
    pub(crate) commit_entries: BTreeMap<CommitId, CommitCatalogEntry>,
    pub(crate) change_entries: BTreeMap<ChangeId, ChangeCatalogEntry>,
    pub(super) objects: ImmutableObjectSet,
}

impl CatalogTreeEdit {
    pub(crate) fn entry_count(&self) -> u64 {
        self.entry_count
    }

    pub(crate) fn copied_nodes(&self) -> usize {
        self.copied_nodes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CatalogPage<T> {
    pub(crate) entries: Vec<T>,
    pub(crate) resume_token: Option<Vec<u8>>,
}

/// The complete authenticated input required by commit-DAG algorithms. It
/// deliberately contains no semantic Change identity or payload metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitTopology {
    pub(crate) commit_id: crate::changelog::CommitId,
    pub(crate) parent_commit_ids: Vec<crate::changelog::CommitId>,
    pub(crate) generation: u64,
}

#[derive(Clone, Debug)]
struct CachedCommitTopologyEnvelope {
    object_id: ObjectId,
    commit: CommitObjectV1,
}

/// Reader-local authenticated topology working set. This cache is bound to the
/// caller's retained StorageRead and is never persisted or used as authority.
#[derive(Default)]
struct CommitTopologyReadCache {
    by_commit_id: BTreeMap<crate::changelog::CommitId, CachedCommitTopologyEnvelope>,
    by_object_id: BTreeMap<ObjectId, crate::changelog::CommitId>,
    resolved: BTreeMap<crate::changelog::CommitId, CommitTopology>,
}

pub(crate) struct CommitTopologyBatch {
    pub(crate) requested: Vec<Option<CommitTopology>>,
    pub(crate) cache_seeded: Vec<CommitTopology>,
}

/// One snapshot-bound topology reader. The authenticated positive cache cannot
/// be named, constructed, detached, or paired with a different StorageRead.
pub(crate) struct CommitTopologyReader<R> {
    read: R,
    cache: CommitTopologyReadCache,
}

impl<R> CommitTopologyReader<R>
where
    R: StorageAdapterRead,
{
    pub(crate) fn new(read: R) -> Self {
        Self {
            read,
            cache: CommitTopologyReadCache::default(),
        }
    }

    pub(crate) fn read(&self) -> &R {
        &self.read
    }

    pub(crate) async fn load(
        &mut self,
        ids: &[crate::changelog::CommitId],
    ) -> Result<CommitTopologyBatch, crate::LixError> {
        load_commit_topology_batch(&self.read, ids, &mut self.cache).await
    }
}

/// Loads one authenticated moving branch head through the ForkTree selector
/// owner. Missing selectors are ordinary branch absence; malformed selectors,
/// snapshots, and selected commit edges fail closed.
pub(crate) async fn load_branch_head<R>(
    read: &R,
    branch_id: &str,
) -> Result<Option<crate::changelog::CommitId>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let branch_id = canonical_branch_id(branch_id)?;
    let selector_key = branch_selector_key(branch_id);
    let keys = [Key(selector_key.clone())];
    let loaded = read
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    let Some(value) = loaded.values.into_iter().next().flatten() else {
        return Ok(None);
    };
    let bytes = match value {
        ProjectedValue::FullValue(bytes) => bytes,
        ProjectedValue::KeyOnly => {
            return Err(crate::LixError::new(
                crate::LixError::CODE_INTERNAL_ERROR,
                "ForkTree branch selector point read returned key-only data",
            ));
        }
    };
    let selector = BranchSelectorV1::decode(&bytes)?;
    if selector.branch_id != branch_id || branch_selector_key(selector.branch_id) != selector_key {
        return Err(
            corruption("ForkTree branch selector key and embedded branch ID differ").into(),
        );
    }
    selected_head_commit_id(read, branch_id).await.map(Some)
}

/// Loads the authenticated semantic identity of a moving branch selector.
/// The identity is the latest RefChange object named by the selected snapshot;
/// no head-only live-state projection can manufacture it.
pub(crate) async fn load_branch_ref_metadata<R>(
    read: &R,
    branch_id: &str,
) -> Result<crate::branch::BranchRefMetadata, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let branch_id = canonical_branch_id(branch_id)?;
    let view = open_coherent_view_on_read(read, branch_id).await?;
    let ref_object_id = view
        .branch_snapshot()
        .latest_ref_change_object_id
        .ok_or_else(|| corruption("branch snapshot has no authenticated latest RefChange edge"))?;
    let bytes = view.load_object_bytes(ref_object_id).await?;
    let change = ChangeObjectV1::decode(ref_object_id, &bytes)?;
    let ChangeObjectV1::BranchRef {
        change_id,
        updated_at,
        branch_id: change_branch_id,
        after_semantic_head_commit_object_id,
        ..
    } = change
    else {
        return Err(
            corruption("branch snapshot latest ref-change edge names a semantic Change").into(),
        );
    };
    if change_branch_id != branch_id
        || after_semantic_head_commit_object_id
            != Some(view.branch_snapshot().semantic_head_commit_object_id)
    {
        return Err(
            corruption("branch snapshot latest ref-change does not match its branch/head").into(),
        );
    }
    Ok(crate::branch::BranchRefMetadata {
        change_id: crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(*change_id.as_bytes())),
        updated_at,
    })
}

pub(crate) async fn load_branch_ref_change_id<R>(
    read: &R,
    branch_id: &str,
) -> Result<Option<crate::changelog::ChangeId>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    Ok(Some(
        load_branch_ref_metadata(read, branch_id).await?.change_id,
    ))
}

/// Scans every authenticated branch selector in one coherent read view.
/// Selector enumeration is storage-streaming and retains only one page plus
/// the output branch-head list.
pub(crate) async fn scan_branch_heads<R>(
    read: &R,
) -> Result<Vec<(String, crate::changelog::CommitId)>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let range = Prefix {
        bytes: bytes::Bytes::from_static(BRANCH_SELECTOR_PREFIX),
    }
    .to_range()?;
    let mut cursor = read
        .begin_scan(
            SELECTOR_SPACE,
            range,
            BeginScanOptions {
                projection: CoreProjection::FullValue,
                order: ScanOrder::Ascending,
            },
        )
        .await?;
    let mut heads = Vec::new();
    loop {
        let page = cursor.next_page(BRANCH_SCAN_PAGE_ROWS).await?;
        for entry in page.entries {
            let bytes = match entry.value {
                ProjectedValue::FullValue(bytes) => bytes,
                ProjectedValue::KeyOnly => {
                    return Err(crate::LixError::new(
                        crate::LixError::CODE_INTERNAL_ERROR,
                        "ForkTree branch selector scan returned key-only data",
                    ));
                }
            };
            let selector = BranchSelectorV1::decode(&bytes)?;
            if entry.key.0 != branch_selector_key(selector.branch_id) {
                return Err(corruption(
                    "ForkTree branch selector scan key and embedded branch ID differ",
                )
                .into());
            }
            let branch_text = uuid::Uuid::from_bytes(*selector.branch_id.as_bytes()).to_string();
            let commit_id = selected_head_commit_id(read, selector.branch_id).await?;
            heads.push((branch_text, commit_id));
        }
        if !page.has_more {
            break;
        }
    }
    Ok(heads)
}

fn canonical_branch_id(branch_id: &str) -> Result<CanonicalBranchId, crate::LixError> {
    let id = uuid::Uuid::parse_str(branch_id).map_err(|error| {
        crate::LixError::new(
            crate::LixError::CODE_INVALID_PARAM,
            format!("branch ID must be a UUID: {error}"),
        )
    })?;
    Ok(CanonicalBranchId::from_bytes(*id.as_bytes()))
}

async fn selected_head_commit_id<R>(
    read: &R,
    branch_id: CanonicalBranchId,
) -> Result<crate::changelog::CommitId, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let view = open_coherent_view_on_read(read, branch_id).await?;
    let selected_id = view.branch_snapshot().semantic_head_commit_object_id;
    let bytes = view.load_object_bytes(selected_id).await?;
    let commit = CommitObjectV1::decode(selected_id, &bytes)?;
    Ok(crate::changelog::CommitId::new(uuid::Uuid::from_bytes(
        *commit.commit_id.as_bytes(),
    )))
}

/// Loads exact public commit facts from the single authenticated
/// CommitCatalog through the caller's coherent StorageRead.
pub(crate) async fn load_commit_records<R>(
    read: &R,
    ids: &[crate::changelog::CommitId],
) -> Result<Vec<Option<crate::changelog::CommitRecord>>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let repository = load_repository_root(read).await?;
    let mut records = Vec::with_capacity(ids.len());
    for id in ids {
        let id = CommitId::from_bytes(*id.as_uuid().as_bytes());
        let Some(value) = lookup_on_read(
            repository.commit_catalog_root,
            "commit",
            id.as_bytes(),
            read,
        )
        .await?
        else {
            records.push(None);
            continue;
        };
        let entry = CommitCatalogEntry::decode(&value)?;
        let bytes = super::view::load_object_bytes(read, entry.commit_object_id).await?;
        let commit = CommitObjectV1::decode(entry.commit_object_id, &bytes)?;
        let topology =
            validate_commit_topology(read, repository.commit_catalog_root, id, &commit).await?;
        records.push(Some(semantic_commit_record(commit, &topology)?));
    }
    Ok(records)
}

/// Loads one required semantic commit record from the authenticated
/// CommitCatalog. Unlike the historical compatibility readers, an absent
/// catalog entry is corruption here; only an authenticated state-key absence
/// is a valid empty result.
pub(crate) async fn load_required_commit_record<R>(
    read: &R,
    id: crate::changelog::CommitId,
) -> Result<crate::changelog::CommitRecord, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let repository = load_repository_root(read).await?;
    let catalog_id = CommitId::from_bytes(*id.as_uuid().as_bytes());
    let entry =
        load_required_commit_catalog_entry(read, repository.commit_catalog_root, catalog_id)
            .await?;
    let bytes = super::view::load_object_bytes(read, entry.commit_object_id).await?;
    let commit = CommitObjectV1::decode(entry.commit_object_id, &bytes)?;
    if commit.commit_id != catalog_id {
        return Err(corruption("CommitCatalog key does not match Commit object").into());
    }
    validate_commit_catalog_identity(
        read,
        repository.commit_catalog_root,
        entry.commit_object_id,
        &commit,
    )
    .await?;
    validate_retained_commit(
        read,
        repository.commit_catalog_root,
        repository.change_catalog_root,
        entry.commit_object_id,
        &commit,
    )
    .await?;
    let topology =
        validate_commit_topology(read, repository.commit_catalog_root, catalog_id, &commit).await?;
    semantic_commit_record(commit, &topology)
}

/// Reads one bounded ordered CommitCatalog page. `start_after` is exclusive
/// and interpreted inside the caller's retained read view.
pub(crate) async fn scan_commit_records<R>(
    read: &R,
    start_after: Option<crate::changelog::CommitId>,
    limit: usize,
) -> Result<Vec<crate::changelog::CommitRecord>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    if limit == 0 {
        return Ok(Vec::new());
    }
    let repository = load_repository_root(read).await?;
    let start_after = start_after.map(|id| id.as_uuid().as_bytes().to_vec());
    let rows = scan_page_on_read(
        repository.commit_catalog_root,
        "commit",
        start_after.as_deref(),
        limit.min(CATALOG_SCAN_PAGE_ROWS),
        read,
    )
    .await?;
    let mut records = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        let id = CommitId::from_bytes(
            key.as_slice()
                .try_into()
                .map_err(|_| corruption("CommitCatalog key is not a raw UUID"))?,
        );
        let entry = CommitCatalogEntry::decode(&value)?;
        let bytes = super::view::load_object_bytes(read, entry.commit_object_id).await?;
        let commit = CommitObjectV1::decode(entry.commit_object_id, &bytes)?;
        if commit.commit_id != id {
            return Err(corruption("CommitCatalog key does not match Commit object").into());
        }
        let topology =
            validate_commit_topology(read, repository.commit_catalog_root, id, &commit).await?;
        records.push(semantic_commit_record(commit, &topology)?);
    }
    Ok(records)
}

/// Loads exact authenticated commit-DAG topology and nothing else. No Change
/// object, ChangeCatalog entry, member payload, or semantic Commit metadata is
/// decoded by this path.
pub(crate) async fn load_commit_topologies<R>(
    read: &R,
    ids: &[crate::changelog::CommitId],
) -> Result<Vec<Option<CommitTopology>>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mut reader = CommitTopologyReader::new(read);
    Ok(reader.load(ids).await?.requested)
}

/// Loads one exact topology batch through one retained StorageRead. Requested
/// Commit objects and the union of their immediate parent Commit objects are
/// each authenticated at most once. Decoded parent envelopes remain in the
/// reader-local cache so a later graph step never reloads the parent object.
async fn load_commit_topology_batch<R>(
    read: &R,
    ids: &[crate::changelog::CommitId],
    cache: &mut CommitTopologyReadCache,
) -> Result<CommitTopologyBatch, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let repository = load_repository_root(read).await?;
    let mut requested_objects = Vec::with_capacity(ids.len());
    let mut missing_requested_objects = BTreeSet::new();
    for id in ids {
        let catalog_id = CommitId::from_bytes(*id.as_uuid().as_bytes());
        let Some(value) = lookup_on_read(
            repository.commit_catalog_root,
            "commit",
            catalog_id.as_bytes(),
            read,
        )
        .await?
        else {
            requested_objects.push(None);
            continue;
        };
        let entry = CommitCatalogEntry::decode(&value)?;
        if let Some(cached) = cache.by_commit_id.get(id) {
            if cached.object_id != entry.commit_object_id {
                return Err(corruption("CommitCatalog changed within one topology read").into());
            }
        } else {
            missing_requested_objects.insert(entry.commit_object_id);
        }
        requested_objects.push(Some((*id, entry.commit_object_id)));
    }

    load_topology_envelopes(read, missing_requested_objects, cache).await?;

    let mut missing_parent_objects = BTreeSet::new();
    for requested in requested_objects.iter().flatten() {
        let envelope = cache.by_commit_id.get(&requested.0).ok_or_else(|| {
            corruption("requested Commit object was not decoded into the topology batch")
        })?;
        validate_commit_identity(requested.0, requested.1, envelope)?;
        let mut unique_parent_objects = BTreeSet::new();
        for parent_object_id in &envelope.commit.parent_commit_object_ids {
            if !unique_parent_objects.insert(*parent_object_id) {
                return Err(
                    corruption("Commit topology contains duplicate parent object edges").into(),
                );
            }
            if !cache.by_object_id.contains_key(parent_object_id) {
                missing_parent_objects.insert(*parent_object_id);
            }
        }
    }
    load_topology_envelopes(read, missing_parent_objects, cache).await?;

    let mut unique_parent_back_edges = BTreeMap::new();
    for requested in requested_objects.iter().flatten() {
        let envelope = cache
            .by_commit_id
            .get(&requested.0)
            .expect("requested topology envelope was checked above");
        for parent_object_id in &envelope.commit.parent_commit_object_ids {
            let parent_id = *cache.by_object_id.get(parent_object_id).ok_or_else(|| {
                corruption(format!("Commit parent object {parent_object_id} is absent"))
            })?;
            unique_parent_back_edges
                .entry(parent_id)
                .or_insert(*parent_object_id);
        }
    }
    for (parent_id, parent_object_id) in &unique_parent_back_edges {
        validate_commit_catalog_back_edge(
            read,
            repository.commit_catalog_root,
            *parent_id,
            *parent_object_id,
        )
        .await?;
    }

    let mut requested = Vec::with_capacity(ids.len());
    for requested_object in requested_objects {
        let Some((id, _)) = requested_object else {
            requested.push(None);
            continue;
        };
        let topology = resolve_cached_topology(id, cache)?;
        cache.resolved.insert(id, topology.clone());
        requested.push(Some(topology));
    }

    // Any decoded parent whose own parent envelopes are already present can be
    // seeded directly into the graph node cache. Roots are the important
    // shared-parent case; deeper nodes remain as decoded envelopes and are
    // completed without reloading themselves when traversal reaches them.
    let candidate_ids = cache.by_commit_id.keys().copied().collect::<Vec<_>>();
    for id in candidate_ids {
        if cache.resolved.contains_key(&id) {
            continue;
        }
        let can_resolve = cache
            .by_commit_id
            .get(&id)
            .expect("candidate came from topology cache")
            .commit
            .parent_commit_object_ids
            .iter()
            .all(|parent| cache.by_object_id.contains_key(parent));
        if can_resolve {
            let topology = resolve_cached_topology(id, cache)?;
            cache.resolved.insert(id, topology);
        }
    }

    Ok(CommitTopologyBatch {
        requested,
        cache_seeded: cache.resolved.values().cloned().collect(),
    })
}

pub(crate) async fn scan_commit_topologies<R>(
    read: &R,
    start_after: Option<crate::changelog::CommitId>,
    limit: usize,
) -> Result<Vec<CommitTopology>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    if limit == 0 {
        return Ok(Vec::new());
    }
    let repository = load_repository_root(read).await?;
    let start_after = start_after.map(|id| id.as_uuid().as_bytes().to_vec());
    let rows = scan_page_on_read(
        repository.commit_catalog_root,
        "commit",
        start_after.as_deref(),
        limit.min(CATALOG_SCAN_PAGE_ROWS),
        read,
    )
    .await?;
    let mut topologies = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        let id = CommitId::from_bytes(
            key.as_slice()
                .try_into()
                .map_err(|_| corruption("CommitCatalog key is not a raw UUID"))?,
        );
        let entry = CommitCatalogEntry::decode(&value)?;
        let bytes = super::view::load_object_bytes(read, entry.commit_object_id).await?;
        let commit = CommitObjectV1::decode(entry.commit_object_id, &bytes)?;
        topologies.push(
            validate_commit_topology(read, repository.commit_catalog_root, id, &commit).await?,
        );
    }
    Ok(topologies)
}

pub(crate) async fn load_change_records<R>(
    read: &R,
    ids: &[crate::changelog::ChangeId],
) -> Result<Vec<Option<crate::changelog::ChangeRecord>>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let repository = load_repository_root(read).await?;
    let mut records = Vec::with_capacity(ids.len());
    for id in ids {
        let id = ChangeId::from_bytes(*id.as_uuid().as_bytes());
        let Some(value) = lookup_on_read(
            repository.change_catalog_root,
            "change",
            id.as_bytes(),
            read,
        )
        .await?
        else {
            records.push(None);
            continue;
        };
        let entry = ChangeCatalogEntry::decode(&value)?;
        records
            .push(semantic_change_record(read, repository.change_catalog_root, id, entry).await?);
    }
    Ok(records)
}

/// Loads the authenticated semantic Change members owned by one commit. The
/// Commit object supplies ordered membership; the unified ChangeCatalog must
/// supply the exact reverse owner/ordinal edge for every returned payload.
async fn load_required_commit_catalog_entry<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    commit_id: CommitId,
) -> Result<CommitCatalogEntry, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let value = lookup_on_read(commit_catalog_root, "commit", commit_id.as_bytes(), read)
        .await?
        .ok_or_else(|| corruption("selected CommitCatalog entry is absent"))?;
    Ok(CommitCatalogEntry::decode(&value)?)
}

pub(crate) async fn load_commit_member_records<R>(
    read: &R,
    commit_id: crate::changelog::CommitId,
) -> Result<Option<Vec<crate::changelog::ChangeRecord>>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    Ok(load_commit_member_sources(read, commit_id)
        .await?
        .map(|members| members.into_iter().map(|(_, record)| record).collect()))
}

/// Loads authenticated commit members together with the commit that
/// introduced each Change.  A selected member is a legitimate semantic
/// dependency even when that source commit is not a first-parent ancestor of
/// the compacting checkpoint.  The member/catalog/source back-edges are
/// validated before this relation is exposed to history consumers.
pub(crate) async fn load_commit_member_sources<R>(
    read: &R,
    commit_id: crate::changelog::CommitId,
) -> Result<
    Option<Vec<(crate::changelog::CommitId, crate::changelog::ChangeRecord)>>,
    crate::LixError,
>
where
    R: StorageAdapterRead + ?Sized,
{
    let repository = load_repository_root(read).await?;
    let commit_id = CommitId::from_bytes(*commit_id.as_uuid().as_bytes());
    let entry =
        load_required_commit_catalog_entry(read, repository.commit_catalog_root, commit_id).await?;
    let commit_object_id = entry.commit_object_id;
    let bytes = super::view::load_object_bytes(read, commit_object_id).await?;
    let commit = CommitObjectV1::decode(commit_object_id, &bytes)?;
    if commit.commit_id != commit_id {
        return Err(corruption("CommitCatalog key does not match Commit object").into());
    }
    validate_retained_commit(
        read,
        repository.commit_catalog_root,
        repository.change_catalog_root,
        commit_object_id,
        &commit,
    )
    .await?;
    let members = load_commit_members(read, &commit).await?;
    let mut records = Vec::with_capacity(members.len());
    for (ordinal, member) in members.iter().copied().enumerate() {
        let change_object_id = member.change_object_id();
        let bytes = super::view::load_object_bytes(read, change_object_id).await?;
        let change = ChangeObjectV1::decode(change_object_id, &bytes)?;
        let change_id = change.change_id();
        let value = lookup_on_read(
            repository.change_catalog_root,
            "change",
            change_id.as_bytes(),
            read,
        )
        .await?
        .ok_or_else(|| corruption("Commit member has no ChangeCatalog owner"))?;
        let entry = ChangeCatalogEntry::decode(&value)?;
        validate_member_catalog_owner(
            read,
            repository.commit_catalog_root,
            commit_object_id,
            commit.generation,
            ordinal,
            member,
            entry,
        )
        .await?;
        let source_commit_id = match member.source() {
            None => commit_id,
            Some((source_commit_object_id, _)) => {
                let bytes = super::view::load_object_bytes(read, source_commit_object_id).await?;
                let source = CommitObjectV1::decode(source_commit_object_id, &bytes)?;
                source.commit_id
            }
        };
        let record = semantic_change_record(read, repository.change_catalog_root, change_id, entry)
            .await?
            .ok_or_else(|| corruption("Commit member has no semantic Change payload"))?;
        records.push((
            crate::changelog::CommitId::new(uuid::Uuid::from_bytes(*source_commit_id.as_bytes())),
            record,
        ));
    }
    Ok(Some(records))
}

/// Loads one visible state value from an authenticated immutable commit root.
/// The commit envelope, catalog identity, and retained member/back-edge closure
/// are validated before the state tree is consulted.
pub(crate) async fn load_state_value_at_commit<R>(
    read: &R,
    commit_id: crate::changelog::CommitId,
    key: &[u8],
    include_tombstone: bool,
) -> Result<Option<(StateValue, StateSource)>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let repository = load_repository_root(read).await?;
    let commit_id = CommitId::from_bytes(*commit_id.as_uuid().as_bytes());
    let entry =
        load_required_commit_catalog_entry(read, repository.commit_catalog_root, commit_id).await?;
    let bytes = super::view::load_object_bytes(read, entry.commit_object_id).await?;
    let commit = CommitObjectV1::decode(entry.commit_object_id, &bytes)?;
    if commit.commit_id != commit_id {
        return Err(corruption("CommitCatalog key does not match Commit object").into());
    }
    validate_commit_catalog_identity(
        read,
        repository.commit_catalog_root,
        entry.commit_object_id,
        &commit,
    )
    .await?;
    validate_retained_commit(
        read,
        repository.commit_catalog_root,
        repository.change_catalog_root,
        entry.commit_object_id,
        &commit,
    )
    .await?;
    state_point_on_read(
        commit.global_state_root,
        commit.local_state_root,
        key,
        include_tombstone,
        read,
    )
    .await
    .map_err(Into::into)
}

pub(crate) async fn scan_change_records<R>(
    read: &R,
    start_after: Option<crate::changelog::ChangeId>,
    limit: usize,
) -> Result<Vec<crate::changelog::ChangeRecord>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    if limit == 0 {
        return Ok(Vec::new());
    }
    let repository = load_repository_root(read).await?;
    let start_after = start_after.map(|id| id.as_uuid().as_bytes().to_vec());
    let rows = scan_page_on_read(
        repository.change_catalog_root,
        "change",
        start_after.as_deref(),
        limit.min(CATALOG_SCAN_PAGE_ROWS),
        read,
    )
    .await?;
    let mut records = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        let id = ChangeId::from_bytes(
            key.as_slice()
                .try_into()
                .map_err(|_| corruption("ChangeCatalog key is not a raw UUID"))?,
        );
        let entry = ChangeCatalogEntry::decode(&value)?;
        if let Some(record) =
            semantic_change_record(read, repository.change_catalog_root, id, entry).await?
        {
            records.push(record);
        }
    }
    Ok(records)
}

async fn load_repository_root<R>(read: &R) -> Result<RepositoryRootV1, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let keys = [Key(global_selector_key())];
    let loaded = read
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    let raw = required_full_value(
        loaded.values.into_iter().next().flatten(),
        "ForkTree global selector is absent",
    )?;
    let selector = GlobalSelectorV1::decode(&raw)?;
    let bytes = super::view::load_object_bytes(read, selector.repository_root).await?;
    Ok(RepositoryRootV1::decode(selector.repository_root, &bytes)?)
}

fn semantic_commit_record(
    commit: CommitObjectV1,
    topology: &CommitTopology,
) -> Result<crate::changelog::CommitRecord, crate::LixError> {
    let record = crate::changelog::decode_forktree_commit_payload(&commit.metadata)?;
    if record.commit_id != topology.commit_id || record.generation != topology.generation {
        return Err(corruption("Commit semantic payload disagrees with its envelope").into());
    }
    if record.parent_commit_ids != topology.parent_commit_ids {
        return Err(corruption("Commit semantic parents disagree with topology edges").into());
    }
    Ok(record)
}

pub(crate) async fn validate_commit_topology<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    catalog_id: CommitId,
    commit: &CommitObjectV1,
) -> Result<CommitTopology, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    if commit.commit_id != catalog_id {
        return Err(corruption("CommitCatalog key does not match Commit object").into());
    }
    let mut unique_parent_objects = BTreeSet::new();
    if commit
        .parent_commit_object_ids
        .iter()
        .any(|id| !unique_parent_objects.insert(*id))
    {
        return Err(corruption("Commit topology contains duplicate parent object edges").into());
    }
    let parent_objects =
        super::view::load_object_map(read, commit.parent_commit_object_ids.iter().copied()).await?;
    let mut parent_commit_ids = Vec::with_capacity(commit.parent_commit_object_ids.len());
    let mut unique_parent_ids = BTreeSet::new();
    for parent_object_id in &commit.parent_commit_object_ids {
        let bytes = parent_objects.get(parent_object_id).ok_or_else(|| {
            corruption(format!("Commit parent object {parent_object_id} is absent"))
        })?;
        let parent = CommitObjectV1::decode(*parent_object_id, bytes)?;
        if parent.generation >= commit.generation {
            return Err(corruption(
                "Commit parent generation is not strictly earlier than its child",
            )
            .into());
        }
        if !unique_parent_ids.insert(parent.commit_id) {
            return Err(corruption("Commit topology contains duplicate parent CommitIds").into());
        }
        let catalog_value = lookup_on_read(
            commit_catalog_root,
            "commit",
            parent.commit_id.as_bytes(),
            read,
        )
        .await?
        .ok_or_else(|| corruption("Commit parent has no CommitCatalog back-edge"))?;
        let catalog_entry = CommitCatalogEntry::decode(&catalog_value)?;
        if catalog_entry.commit_object_id != *parent_object_id {
            return Err(corruption("Commit parent CommitCatalog back-edge is invalid").into());
        }
        parent_commit_ids.push(crate::changelog::CommitId::new(uuid::Uuid::from_bytes(
            *parent.commit_id.as_bytes(),
        )));
    }
    Ok(CommitTopology {
        commit_id: crate::changelog::CommitId::new(uuid::Uuid::from_bytes(
            *commit.commit_id.as_bytes(),
        )),
        parent_commit_ids,
        generation: commit.generation,
    })
}

async fn load_topology_envelopes<R>(
    read: &R,
    object_ids: BTreeSet<ObjectId>,
    cache: &mut CommitTopologyReadCache,
) -> Result<(), crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    if object_ids.is_empty() {
        return Ok(());
    }
    let objects = super::view::load_object_map(read, object_ids.iter().copied()).await?;
    for object_id in object_ids {
        let bytes = objects
            .get(&object_id)
            .ok_or_else(|| corruption(format!("Commit object {object_id} is absent")))?;
        let commit = CommitObjectV1::decode(object_id, bytes)?;
        let public_id =
            crate::changelog::CommitId::new(uuid::Uuid::from_bytes(*commit.commit_id.as_bytes()));
        if let Some(existing_id) = cache.by_object_id.insert(object_id, public_id)
            && existing_id != public_id
        {
            return Err(
                corruption("Commit object identity changed within one topology read").into(),
            );
        }
        let envelope = CachedCommitTopologyEnvelope { object_id, commit };
        if let Some(existing) = cache.by_commit_id.insert(public_id, envelope.clone())
            && existing.object_id != object_id
        {
            return Err(corruption("multiple Commit objects claim one CommitId").into());
        }
    }
    Ok(())
}

fn validate_commit_identity(
    expected_id: crate::changelog::CommitId,
    expected_object_id: ObjectId,
    envelope: &CachedCommitTopologyEnvelope,
) -> Result<(), crate::LixError> {
    if envelope.object_id != expected_object_id
        || envelope.commit.commit_id.as_bytes() != expected_id.as_uuid().as_bytes()
    {
        return Err(corruption("CommitCatalog key does not match Commit object").into());
    }
    Ok(())
}

async fn validate_commit_catalog_back_edge<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    parent_id: crate::changelog::CommitId,
    parent_object_id: ObjectId,
) -> Result<(), crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let catalog_value = lookup_on_read(
        commit_catalog_root,
        "commit",
        parent_id.as_uuid().as_bytes(),
        read,
    )
    .await?
    .ok_or_else(|| corruption("Commit parent has no CommitCatalog back-edge"))?;
    let catalog_entry = CommitCatalogEntry::decode(&catalog_value)?;
    if catalog_entry.commit_object_id != parent_object_id {
        return Err(corruption("Commit parent CommitCatalog back-edge is invalid").into());
    }
    Ok(())
}

fn resolve_cached_topology(
    id: crate::changelog::CommitId,
    cache: &CommitTopologyReadCache,
) -> Result<CommitTopology, crate::LixError> {
    let envelope = cache
        .by_commit_id
        .get(&id)
        .ok_or_else(|| corruption("Commit topology envelope is absent from its exact batch"))?;
    let mut parent_commit_ids = Vec::with_capacity(envelope.commit.parent_commit_object_ids.len());
    let mut unique_parent_ids = BTreeSet::new();
    for parent_object_id in &envelope.commit.parent_commit_object_ids {
        let parent_id = *cache.by_object_id.get(parent_object_id).ok_or_else(|| {
            corruption(format!("Commit parent object {parent_object_id} is absent"))
        })?;
        let parent = cache
            .by_commit_id
            .get(&parent_id)
            .ok_or_else(|| corruption("decoded parent Commit identity is absent"))?;
        if parent.commit.generation >= envelope.commit.generation {
            return Err(corruption(
                "Commit parent generation is not strictly earlier than its child",
            )
            .into());
        }
        if !unique_parent_ids.insert(parent_id) {
            return Err(corruption("Commit topology contains duplicate parent CommitIds").into());
        }
        parent_commit_ids.push(parent_id);
    }
    Ok(CommitTopology {
        commit_id: id,
        parent_commit_ids,
        generation: envelope.commit.generation,
    })
}

async fn semantic_change_record<R>(
    read: &R,
    change_catalog_root: ObjectId,
    id: ChangeId,
    entry: ChangeCatalogEntry,
) -> Result<Option<crate::changelog::ChangeRecord>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let bytes = super::view::load_object_bytes(read, entry.change_object_id).await?;
    let change = ChangeObjectV1::decode(entry.change_object_id, &bytes)?;
    if change.change_id() != id {
        return Err(corruption("ChangeCatalog key does not match Change object").into());
    }
    match (entry.owner, &change) {
        (
            ChangeCatalogOwner::CommitMember {
                commit_object_id,
                ordinal,
            },
            ChangeObjectV1::Semantic { .. },
        ) => {
            let bytes = super::view::load_object_bytes(read, commit_object_id).await?;
            let commit = CommitObjectV1::decode(commit_object_id, &bytes)?;
            let members = load_commit_members(read, &commit).await?;
            if members.get(ordinal as usize)
                != Some(&CommitMemberV1::introduced(entry.change_object_id))
            {
                return Err(corruption("ChangeCatalog owner/ordinal back-edge is invalid").into());
            }
        }
        (
            ChangeCatalogOwner::BranchRef {
                ref_change_object_id,
                branch_id,
            },
            ChangeObjectV1::BranchRef {
                branch_id: object_branch,
                ..
            },
        ) if ref_change_object_id == entry.change_object_id && branch_id == *object_branch => {
            validate_retained_ref_change(
                read,
                change_catalog_root,
                entry.change_object_id,
                &change,
            )
            .await?;
        }
        _ => return Err(corruption("ChangeCatalog owner kind/back-edge is invalid").into()),
    }
    let (payload, json_payload_object_ids, is_empty_ref_payload) = match change {
        ChangeObjectV1::Semantic {
            payload,
            json_payload_object_ids,
            ..
        } => (payload, json_payload_object_ids, false),
        ChangeObjectV1::BranchRef {
            payload,
            json_payload_object_ids,
            ..
        } => {
            let is_empty = payload.is_empty();
            (payload, json_payload_object_ids, is_empty)
        }
    };
    if is_empty_ref_payload {
        // Creation RefChanges carry authenticated branch/head edges but no
        // public lix_change payload. Keep the object/catalog validation above,
        // while leaving this control-plane fact out of the semantic changelog.
        return Ok(None);
    }
    let record = crate::changelog::decode_forktree_change_payload(
        &payload,
        crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(*id.as_bytes())),
    )?;
    let expected = crate::changelog::forktree_change_json_payload_ids(&record)
        .into_iter()
        .map(ObjectId::from_bytes)
        .collect::<Vec<_>>();
    if expected != json_payload_object_ids {
        return Err(corruption(
            "Change object JSON payload edges do not match its semantic payload",
        )
        .into());
    }
    Ok(Some(record))
}

async fn validate_commit_catalog_identity<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    commit_object_id: ObjectId,
    commit: &CommitObjectV1,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let catalog_value = lookup_on_read(
        commit_catalog_root,
        "commit",
        commit.commit_id.as_bytes(),
        read,
    )
    .await?
    .ok_or_else(|| corruption("Commit object has no authoritative CommitCatalog entry"))?;
    let catalog_entry = CommitCatalogEntry::decode(&catalog_value)?;
    if catalog_entry.commit_object_id != commit_object_id {
        return Err(corruption(
            "Commit object disagrees with its authoritative CommitCatalog entry",
        ));
    }
    Ok(())
}

pub(super) async fn validate_member_catalog_owner<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    target_commit_object_id: ObjectId,
    target_generation: u64,
    target_ordinal: usize,
    member: CommitMemberV1,
    entry: ChangeCatalogEntry,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if entry.change_object_id != member.change_object_id() {
        return Err(corruption(
            "commit membership edge disagrees with ChangeCatalog object identity",
        ));
    }
    let canonical_owner = match entry.owner {
        ChangeCatalogOwner::CommitMember {
            commit_object_id,
            ordinal,
        } => {
            let bytes = super::view::load_object_bytes(read, commit_object_id).await?;
            let introduction = CommitObjectV1::decode(commit_object_id, &bytes)?;
            validate_commit_catalog_identity(
                read,
                commit_catalog_root,
                commit_object_id,
                &introduction,
            )
            .await?;
            let introduction_members = load_commit_members(read, &introduction).await?;
            if introduction_members.get(ordinal as usize)
                != Some(&CommitMemberV1::introduced(member.change_object_id()))
            {
                return Err(corruption(
                    "ChangeCatalog canonical introduction owner/ordinal is invalid",
                ));
            }
            (commit_object_id, ordinal)
        }
        ChangeCatalogOwner::BranchRef { .. } => {
            return Err(corruption(
                "semantic commit member resolves to a branch-ref catalog owner",
            ));
        }
    };
    match member.source() {
        None => {
            let target_ordinal = u32::try_from(target_ordinal)
                .map_err(|_| corruption("commit member ordinal exceeds u32"))?;
            if canonical_owner != (target_commit_object_id, target_ordinal) {
                return Err(corruption(
                    "introduced membership is not the canonical ChangeCatalog owner",
                ));
            }
        }
        Some((source_commit_object_id, source_ordinal)) => {
            let bytes = super::view::load_object_bytes(read, source_commit_object_id).await?;
            let source_commit = CommitObjectV1::decode(source_commit_object_id, &bytes)?;
            validate_commit_catalog_identity(
                read,
                commit_catalog_root,
                source_commit_object_id,
                &source_commit,
            )
            .await?;
            if source_commit.generation >= target_generation {
                return Err(corruption(
                    "selected membership source generation is not earlier than its target",
                ));
            }
            let source_members = load_commit_members(read, &source_commit).await?;
            if source_members
                .get(source_ordinal as usize)
                .map(|source| source.change_object_id())
                != Some(member.change_object_id())
            {
                return Err(corruption(
                    "selected membership source commit/ordinal back-edge is invalid",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) async fn select_historical_commit_member<R>(
    view: &CoherentView<R>,
    source_commit_id: CommitId,
    change_id: ChangeId,
) -> Result<(CommitMemberV1, CommitObjectV1, ChangeObjectV1), StorageError>
where
    R: StorageAdapterRead,
{
    let source = load_commit(view, source_commit_id)
        .await?
        .ok_or_else(|| corruption("selected source commit is absent from CommitCatalog"))?;
    let (source_commit_object_id, _) = source.encode()?;
    let source_members = view.load_commit_members(&source).await?;
    for (source_ordinal, source_member) in source_members.iter().copied().enumerate() {
        let change_object_id = source_member.change_object_id();
        let bytes = view.load_object_bytes(change_object_id).await?;
        let change = ChangeObjectV1::decode(change_object_id, &bytes)?;
        if change.change_id() != change_id {
            continue;
        }
        let value = view
            .lookup_tree_value(
                view.repository_root().change_catalog_root,
                "change",
                change_id.as_bytes(),
            )
            .await?
            .ok_or_else(|| corruption("selected Change has no ChangeCatalog introduction owner"))?;
        let entry = ChangeCatalogEntry::decode(&value)?;
        view.validate_member_catalog_owner(
            view.repository_root().commit_catalog_root,
            source_commit_object_id,
            source.generation,
            source_ordinal,
            source_member,
            entry,
        )
        .await?;
        return Ok((
            CommitMemberV1::selected(
                change_object_id,
                source_commit_object_id,
                u32::try_from(source_ordinal)
                    .map_err(|_| corruption("selected source ordinal exceeds u32"))?,
            ),
            source,
            change,
        ));
    }
    Err(corruption(
        "selected ChangeId is absent from its authenticated source commit membership",
    ))
}

fn required_full_value(
    value: Option<ProjectedValue>,
    missing: &'static str,
) -> Result<bytes::Bytes, crate::LixError> {
    match value {
        Some(ProjectedValue::FullValue(bytes)) => Ok(bytes),
        Some(ProjectedValue::KeyOnly) => {
            Err(corruption("ForkTree full-value read returned key-only data").into())
        }
        None => Err(corruption(missing).into()),
    }
}

pub(crate) async fn state_point<R>(
    view: &CoherentView<R>,
    key: &[u8],
    include_tombstone: bool,
) -> Result<Option<VisibleStateRow>, StorageError>
where
    R: StorageAdapterRead,
{
    let (global_root, local_root) = current_state_roots(view);
    let value_source = match local_root {
        Some(local_root) => {
            view.state_point_at_roots(global_root, local_root, key, include_tombstone)
                .await?
        }
        None => view
            .state_point_at_roots(global_root, global_root, key, include_tombstone)
            .await?
            .map(|(value, _)| (value, StateSource::Global)),
    };
    let Some((value, source)) = value_source else {
        return Ok(None);
    };
    Ok(Some(VisibleStateRow {
        encoded_key: key.to_vec(),
        value,
        source,
        view_instance_id: view.view_instance_id(),
    }))
}

/// Resolves one state identity against explicitly authenticated commit roots
/// on the caller's retained read. This is the ordered-history source-state
/// seam; it never opens or refreshes a storage snapshot.
pub(crate) async fn state_point_on_read<R>(
    global_state_root: ObjectId,
    local_state_root: ObjectId,
    key: &[u8],
    include_tombstone: bool,
    read: &R,
) -> Result<Option<(StateValue, StateSource)>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if let Some(encoded) = lookup_on_read(local_state_root, "state", key, read).await? {
        let value = decode_state_value_storage(&encoded)?;
        return if value.cell.deleted() && !include_tombstone {
            Ok(None)
        } else {
            Ok(Some((value, StateSource::Branch)))
        };
    }
    let Some(encoded) = lookup_on_read(global_state_root, "state", key, read).await? else {
        return Ok(None);
    };
    let value = decode_state_value_storage(&encoded)?;
    if matches!(value.cell, StateCell::Tombstone) {
        return Err(corruption("global state tree contains a tombstone"));
    }
    Ok(Some((value, StateSource::Global)))
}

pub(crate) async fn state_range<R>(
    view: &CoherentView<R>,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    limit: Option<usize>,
    include_tombstones: bool,
) -> Result<Vec<VisibleStateRow>, StorageError>
where
    R: StorageAdapterRead,
{
    let (global_root, local_root) = current_state_roots(view);
    let rows = view
        .state_range_at_roots(
            global_root,
            local_root,
            lower,
            upper,
            limit,
            include_tombstones,
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|(encoded_key, value, source)| VisibleStateRow {
            encoded_key,
            value,
            source,
            view_instance_id: view.view_instance_id(),
        })
        .collect())
}

/// The global branch has no branch-local overlay. Bootstrap intentionally
/// retains the same authenticated state root in the selected global snapshot,
/// so treating that root as a local branch would relabel global rows and make
/// the SQL schema catalog disappear from the global write domain. Current
/// global reads therefore resolve only the repository global root; ordinary
/// branches continue to resolve their local root over that global root.
fn current_state_roots<R>(view: &CoherentView<R>) -> (ObjectId, Option<ObjectId>)
where
    R: StorageAdapterRead,
{
    let global_root = view.repository_root().global_state_root;
    if view.branch_id().as_bytes()
        == uuid::Uuid::parse_str(crate::GLOBAL_BRANCH_ID)
            .expect("GLOBAL_BRANCH_ID must be a UUID")
            .as_bytes()
    {
        (global_root, None)
    } else {
        (global_root, Some(view.branch_snapshot().local_state_root))
    }
}

/// Scans the authenticated global/local state overlay for explicit historical
/// roots. The roots and every leaf are read through the caller's retained
/// StorageRead; no current selector or legacy tracked-state reader is opened.
pub(crate) async fn state_range_on_roots<R>(
    global_state_root: ObjectId,
    local_state_root: Option<ObjectId>,
    read: &R,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    limit: Option<usize>,
    include_tombstones: bool,
) -> Result<Vec<(Vec<u8>, StateValue, StateSource)>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let page_size = limit.unwrap_or(64).clamp(1, 64);
    let mut output = Vec::new();
    let mut global_cursor = None;
    let mut local_cursor = None;
    let mut global = std::collections::VecDeque::new();
    let mut local = std::collections::VecDeque::new();
    let mut global_done = false;
    let mut local_done = local_state_root.is_none();
    loop {
        if limit.is_some_and(|limit| output.len() >= limit) {
            break;
        }
        if global.is_empty() && !global_done {
            let page = scan_bounded_page_on_read(
                global_state_root,
                "state",
                lower,
                upper,
                global_cursor.as_deref(),
                page_size,
                read,
            )
            .await?;
            global_done = page.len() < page_size;
            global_cursor = page.last().map(|(key, _)| key.clone());
            global.extend(page);
        }
        if local.is_empty() && !local_done {
            let page = scan_bounded_page_on_read(
                local_state_root.expect("local state root is present while scanning"),
                "state",
                lower,
                upper,
                local_cursor.as_deref(),
                page_size,
                read,
            )
            .await?;
            local_done = page.len() < page_size;
            local_cursor = page.last().map(|(key, _)| key.clone());
            local.extend(page);
        }
        if global.is_empty() && local.is_empty() {
            break;
        }
        let take_local = match (global.front(), local.front()) {
            (None, Some(_)) => true,
            (Some(_), None) => false,
            (Some((global_key, _)), Some((local_key, _))) => local_key <= global_key,
            (None, None) => break,
        };
        let (key, encoded, source) = if take_local {
            let (key, value) = local.pop_front().expect("front local state row");
            if global
                .front()
                .is_some_and(|(global_key, _)| *global_key == key)
            {
                global.pop_front();
            }
            (key, value, StateSource::Branch)
        } else {
            let (key, value) = global.pop_front().expect("front global state row");
            (key, value, StateSource::Global)
        };
        let value = decode_state_value_storage(&encoded)?;
        if source == StateSource::Global && matches!(value.cell, StateCell::Tombstone) {
            return Err(corruption("global state tree contains a tombstone"));
        }
        if value.cell.deleted() && !include_tombstones {
            continue;
        }
        output.push((key, value, source));
    }
    Ok(output)
}

/// Loads the complete authenticated state overlay for one historical commit.
/// A missing commit/catalog/root is an error; an absent key is represented by
/// the absence of a row in the returned ordered stream.
pub(crate) async fn scan_state_rows_at_commit<R>(
    read: &R,
    commit_id: crate::changelog::CommitId,
) -> Result<Vec<HistoricalStateRow>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let repository = load_repository_root(read).await?;
    let catalog_id = CommitId::from_bytes(*commit_id.as_uuid().as_bytes());
    let entry =
        load_required_commit_catalog_entry(read, repository.commit_catalog_root, catalog_id)
            .await?;
    let bytes = super::view::load_object_bytes(read, entry.commit_object_id).await?;
    let commit = CommitObjectV1::decode(entry.commit_object_id, &bytes)?;
    if commit.commit_id != catalog_id {
        return Err(corruption("CommitCatalog key does not match Commit object").into());
    }
    validate_commit_catalog_identity(
        read,
        repository.commit_catalog_root,
        entry.commit_object_id,
        &commit,
    )
    .await?;
    validate_retained_commit(
        read,
        repository.commit_catalog_root,
        repository.change_catalog_root,
        entry.commit_object_id,
        &commit,
    )
    .await?;
    let rows = state_range_on_roots(
        commit.global_state_root,
        Some(commit.local_state_root),
        read,
        None,
        None,
        None,
        true,
    )
    .await?;
    rows.into_iter()
        .map(|(encoded_key, value, source)| {
            let key = decode_state_key(&encoded_key)?;
            let (snapshot_content, deleted) = match value.cell {
                StateCell::Value(snapshot) => (Some(snapshot), false),
                StateCell::Null => (None, false),
                StateCell::Tombstone => (None, true),
            };
            Ok(HistoricalStateRow {
                key,
                global: source == StateSource::Global,
                change_id: value.change_id,
                commit_id: value.commit_id,
                created_at: value.created_at,
                updated_at: value.updated_at,
                snapshot_content,
                metadata: value.metadata,
                deleted,
                blob_manifest_object_ids: value.blob_manifest_object_ids,
            })
        })
        .collect()
}

/// Diffs two authenticated historical state overlays without scanning either
/// endpoint. Ordered-tree object IDs prune equal subtrees; only changed leaf
/// keys are decoded and projected. Commit envelopes/catalog/topology and every
/// changed row's ChangeCatalog identity are still authenticated, while
/// unrelated commit members are not eagerly traversed.
pub(crate) async fn diff_state_rows_between_commits<R>(
    read: &R,
    before: crate::changelog::CommitId,
    after: crate::changelog::CommitId,
    include_global: bool,
) -> Result<Vec<HistoricalStateDiffEntry>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    diff_state_rows_between_commits_impl(read, before, after, include_global, true).await
}

/// Diffs the same authenticated roots for the working-diff projection. The
/// result deliberately ignores commit/change provenance when the materialized
/// payload is equal, so a value reverted to its checkpoint payload disappears
/// from the user-facing working diff.
pub(crate) async fn diff_state_rows_for_working_diff<R>(
    read: &R,
    before: crate::changelog::CommitId,
    after: crate::changelog::CommitId,
    include_global: bool,
) -> Result<Vec<HistoricalStateDiffEntry>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    diff_state_rows_between_commits_impl(read, before, after, include_global, false).await
}

async fn diff_state_rows_between_commits_impl<R>(
    read: &R,
    before: crate::changelog::CommitId,
    after: crate::changelog::CommitId,
    include_global: bool,
    identity_aware: bool,
) -> Result<Vec<HistoricalStateDiffEntry>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let before_commit = load_diff_commit(read, before).await?;
    let after_commit = load_diff_commit(read, after).await?;
    let mut roots = BTreeMap::<
        Vec<u8>,
        (
            Option<StateValue>,
            Option<StateValue>,
            Option<StateValue>,
            Option<StateValue>,
        ),
    >::new();

    for (before_root, after_root, is_global) in [
        (
            before_commit.global_state_root,
            after_commit.global_state_root,
            true,
        ),
        (
            before_commit.local_state_root,
            after_commit.local_state_root,
            false,
        ),
    ] {
        for (key, before_value, after_value) in
            diff_ordered_tree_on_read(before_root, after_root, "state", read).await?
        {
            let before_value = before_value
                .as_deref()
                .map(decode_state_value_storage)
                .transpose()?;
            let after_value = after_value
                .as_deref()
                .map(decode_state_value_storage)
                .transpose()?;
            if is_global
                && [before_value.as_ref(), after_value.as_ref()]
                    .into_iter()
                    .flatten()
                    .any(|value| matches!(value.cell, StateCell::Tombstone))
            {
                return Err(corruption("global state tree contains a tombstone").into());
            }
            let entry = roots.entry(key).or_insert((None, None, None, None));
            if is_global {
                entry.0 = before_value;
                entry.1 = after_value;
            } else {
                entry.2 = before_value;
                entry.3 = after_value;
            }
        }
    }

    // A changed key may come from only one overlay root. The unchanged root
    // therefore contributes no diff tuple, but it can still provide the
    // visible counterpart (for example, a global row hidden by a new local
    // override). Resolve only those missing endpoint/root cells by point
    // lookup on this same retained read; never rescan either endpoint.
    for (encoded_key, (global_before, global_after, local_before, local_after)) in roots.iter_mut()
    {
        if global_before.is_none() {
            *global_before = load_state_value_from_root(
                before_commit.global_state_root,
                encoded_key,
                true,
                read,
            )
            .await?;
        }
        if global_after.is_none() {
            *global_after =
                load_state_value_from_root(after_commit.global_state_root, encoded_key, true, read)
                    .await?;
        }
        if local_before.is_none() {
            *local_before = load_state_value_from_root(
                before_commit.local_state_root,
                encoded_key,
                false,
                read,
            )
            .await?;
        }
        if local_after.is_none() {
            *local_after =
                load_state_value_from_root(after_commit.local_state_root, encoded_key, false, read)
                    .await?;
        }
    }

    let mut rows =
        BTreeMap::<Vec<u8>, (Option<HistoricalStateRow>, Option<HistoricalStateRow>)>::new();
    for (encoded_key, (global_before, global_after, local_before, local_after)) in roots {
        let key = decode_state_key(&encoded_key)?;
        let before = local_before
            .map(|value| (value, StateSource::Branch))
            .or_else(|| global_before.map(|value| (value, StateSource::Global)));
        let after = local_after
            .map(|value| (value, StateSource::Branch))
            .or_else(|| global_after.map(|value| (value, StateSource::Global)));
        rows.insert(
            encoded_key,
            (
                before.map(|(value, source)| historical_state_row(key.clone(), value, source)),
                after.map(|(value, source)| historical_state_row(key.clone(), value, source)),
            ),
        );
    }

    let mut change_ids = BTreeSet::new();
    for (before, after) in rows.values() {
        for row in [before.as_ref(), after.as_ref()].into_iter().flatten() {
            change_ids.insert(row.change_id);
        }
    }
    if !change_ids.is_empty() {
        let ids = change_ids.into_iter().collect::<Vec<_>>();
        let records = load_change_records(read, &ids).await?;
        let mut authenticated = BTreeMap::new();
        for (id, record) in ids.into_iter().zip(records) {
            let record = record.ok_or_else(|| {
                corruption("changed historical state row has no authenticated Change")
            })?;
            authenticated.insert(id, record);
        }
        for (before, after) in rows.values() {
            for row in [before.as_ref(), after.as_ref()].into_iter().flatten() {
                let record = authenticated.get(&row.change_id).ok_or_else(|| {
                    corruption("changed historical state row lost its authenticated Change")
                })?;
                validate_historical_state_row(read, row, record).await?;
            }
        }
    }

    Ok(rows
        .into_values()
        .filter_map(|(before, after)| {
            let changed = if identity_aware {
                historical_state_payloads_differ(before.as_ref(), after.as_ref())
            } else {
                historical_state_content_differ(before.as_ref(), after.as_ref())
            };
            if !changed {
                return None;
            }
            let before = before.filter(|row| include_global || !row.global);
            let after = after.filter(|row| include_global || !row.global);
            (before.is_some() || after.is_some())
                .then_some(HistoricalStateDiffEntry { before, after })
        })
        .collect())
}

async fn load_diff_commit<R>(
    read: &R,
    id: crate::changelog::CommitId,
) -> Result<CommitObjectV1, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let repository = load_repository_root(read).await?;
    let catalog_id = CommitId::from_bytes(*id.as_uuid().as_bytes());
    let entry =
        load_required_commit_catalog_entry(read, repository.commit_catalog_root, catalog_id)
            .await?;
    let bytes = super::view::load_object_bytes(read, entry.commit_object_id).await?;
    let commit = CommitObjectV1::decode(entry.commit_object_id, &bytes)?;
    validate_commit_catalog_identity(
        read,
        repository.commit_catalog_root,
        entry.commit_object_id,
        &commit,
    )
    .await?;
    validate_commit_topology(read, repository.commit_catalog_root, catalog_id, &commit).await?;
    Ok(commit)
}

fn historical_state_row(
    key: super::state::StateKey,
    value: StateValue,
    source: StateSource,
) -> HistoricalStateRow {
    let (snapshot_content, deleted) = match value.cell {
        StateCell::Value(snapshot) => (Some(snapshot), false),
        StateCell::Null => (None, false),
        StateCell::Tombstone => (None, true),
    };
    HistoricalStateRow {
        key,
        global: source == StateSource::Global,
        change_id: value.change_id,
        commit_id: value.commit_id,
        created_at: value.created_at,
        updated_at: value.updated_at,
        snapshot_content,
        metadata: value.metadata,
        deleted,
        blob_manifest_object_ids: value.blob_manifest_object_ids,
    }
}

async fn load_state_value_from_root<R>(
    root: ObjectId,
    encoded_key: &[u8],
    is_global: bool,
    read: &R,
) -> Result<Option<StateValue>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let Some(encoded_value) = lookup_on_read(root, "state", encoded_key, read).await? else {
        return Ok(None);
    };
    let value = decode_state_value_storage(&encoded_value)?;
    if is_global && matches!(value.cell, StateCell::Tombstone) {
        return Err(corruption("global state tree contains a tombstone"));
    }
    Ok(Some(value))
}

fn historical_state_payloads_differ(
    before: Option<&HistoricalStateRow>,
    after: Option<&HistoricalStateRow>,
) -> bool {
    match (before, after) {
        (Some(left), Some(right)) => {
            left.key != right.key
                || left.change_id != right.change_id
                || left.commit_id != right.commit_id
                || left.deleted != right.deleted
                || left.snapshot_content != right.snapshot_content
                || left.metadata != right.metadata
        }
        (Some(_), None) | (None, Some(_)) => true,
        (None, None) => false,
    }
}

fn historical_state_content_differ(
    before: Option<&HistoricalStateRow>,
    after: Option<&HistoricalStateRow>,
) -> bool {
    match (before, after) {
        (Some(left), Some(right)) => {
            left.key != right.key
                || left.deleted != right.deleted
                || left.snapshot_content != right.snapshot_content
                || left.metadata != right.metadata
        }
        (Some(_), None) | (None, Some(_)) => true,
        (None, None) => false,
    }
}

async fn json_slot_matches_state_content<R>(
    read: &R,
    content: Option<&str>,
    expected: &crate::json_store::JsonSlot,
) -> Result<bool, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    match expected {
        crate::json_store::JsonSlot::None => Ok(content.is_none()),
        crate::json_store::JsonSlot::Inline(expected) => Ok(content == Some(expected.as_ref())),
        crate::json_store::JsonSlot::Ref(_) => Ok(content
            .map(crate::json_store::JsonSlot::from_json)
            .is_some_and(|actual| actual == *expected)),
        crate::json_store::JsonSlot::ForkTreeObject(object_id) => {
            let object_id = ObjectId::from_bytes(*object_id);
            let bytes = super::view::load_object_bytes(read, object_id).await?;
            let chunk = BlobChunkV1::decode(object_id, &bytes)?;
            Ok(content.is_some_and(|content| chunk.bytes.as_ref() == content.as_bytes()))
        }
    }
}

pub(crate) async fn validate_historical_state_row<R>(
    read: &R,
    row: &HistoricalStateRow,
    record: &crate::changelog::ChangeRecord,
) -> Result<(), crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    if record.schema_key != row.key.schema_key
        || record.file_id != row.key.file_id
        || record.entity_pk != row.key.entity_pk
        || record.created_at != row.created_at
        || !json_slot_matches_state_content(read, row.snapshot_content.as_deref(), &record.snapshot)
            .await?
        || !json_slot_matches_state_content(read, row.metadata.as_deref(), &record.metadata).await?
    {
        return Err(corruption(
            "changed historical state row does not authenticate its Change payload",
        )
        .into());
    }
    Ok(())
}

pub(crate) async fn edit_state_tree<R>(
    root: ObjectId,
    mutations: Vec<StateTreeMutation>,
    read: &R,
) -> Result<StateTreeEdit, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let root = validate_root_on_read(root, "state", read).await?;
    let mut added_blob_roots = BTreeMap::new();
    let mut wrote_tombstone = false;
    let mut written_commit_ids = BTreeSet::new();
    for mutation in &mutations {
        let (key, value) = match mutation {
            StateTreeMutation::Insert { key, value } | StateTreeMutation::Update { key, value } => {
                (key, Some(value))
            }
            StateTreeMutation::Remove { key } => (key, None),
        };
        decode_state_key(key).map_err(|error| corruption(error.to_string()))?;
        if let Some(value) = value {
            let value = decode_state_value_storage(value)?;
            wrote_tombstone |= value.cell.deleted();
            written_commit_ids.insert(*value.commit_id.as_uuid().as_bytes());
            added_blob_roots.extend(
                value
                    .blob_manifest_object_ids
                    .into_iter()
                    .map(|object_id| (object_id, ())),
            );
        }
    }
    let mutations = mutations
        .into_iter()
        .map(StateTreeMutation::into_ordered)
        .collect::<Vec<_>>();
    let edit = apply_ordered_mutations(root, "state", &mutations, read).await?;
    Ok(StateTreeEdit {
        base_root: root.object_id,
        root: edit.root.object_id,
        entry_count: edit.root.entry_count,
        copied_nodes: edit.copied_nodes,
        added_blob_roots,
        wrote_tombstone,
        written_commit_ids,
        objects: edit.objects,
    })
}

/// Applies an ordered sequence of commit-local state mutations while keeping
/// every newly authenticated root available through the caller's one retained
/// read. Intermediate roots remain immutable commit authority, so their
/// operation-local nodes are accumulated rather than pruned as transient
/// publication scratch.
pub(crate) async fn edit_state_tree_sequence<R>(
    mut root: ObjectId,
    mutation_batches: Vec<Vec<StateTreeMutation>>,
    read: &R,
) -> Result<Vec<StateTreeEdit>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mut accumulated = ImmutableObjectSet::default();
    let mut edits = Vec::with_capacity(mutation_batches.len());
    for mutations in mutation_batches {
        let overlay = ObjectOverlayRead {
            read,
            objects: &accumulated,
        };
        let edit = edit_state_tree(root, mutations, &overlay).await?;
        root = edit.root;
        accumulated.extend(edit.objects.clone())?;
        edits.push(edit);
    }
    Ok(edits)
}

pub(super) struct ObjectOverlayRead<'a, R: ?Sized> {
    read: &'a R,
    objects: &'a ImmutableObjectSet,
}

impl<'a, R: ?Sized> ObjectOverlayRead<'a, R> {
    pub(super) fn new(read: &'a R, objects: &'a ImmutableObjectSet) -> Self {
        Self { read, objects }
    }
}

impl<R> StorageAdapterRead for ObjectOverlayRead<'_, R>
where
    R: StorageAdapterRead + ?Sized,
{
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.read.snapshot_cache_key()
    }

    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        let mut loaded = self.read.get_many(requests).await?;
        let mut value_index = 0usize;
        for request in requests {
            for key in request.keys {
                if request.space == super::object::OBJECT_SPACE {
                    if let Ok(raw_id) = <[u8; 32]>::try_from(key.0.as_ref()) {
                        let id = ObjectId::from_bytes(raw_id);
                        if let Some(bytes) = self.objects.get(id) {
                            loaded.values[value_index] = Some(match request.opts.projection {
                                CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
                                CoreProjection::FullValue => {
                                    ProjectedValue::FullValue(bytes.clone())
                                }
                            });
                        }
                    }
                }
                value_index += 1;
            }
        }
        Ok(loaded)
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        self.read.begin_scan(space, range, opts)
    }
}

pub(crate) async fn put_commit_catalog_entries<R>(
    root: ObjectId,
    entries: &[(CommitId, CommitCatalogEntry)],
    read: &R,
) -> Result<CatalogTreeEdit, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if entries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(corruption(
            "CommitCatalog entries are not strictly ordered and distinct",
        ));
    }
    let mut mutations = Vec::with_capacity(entries.len());
    for (id, entry) in entries {
        let value = entry.encode()?;
        mutations.push(OrderedTreeMutation::Insert {
            key: id.as_bytes().to_vec(),
            value,
        });
    }
    let mut edit = edit_catalog(root, "commit", &mutations, read, true).await?;
    edit.commit_entries.extend(entries.iter().copied());
    Ok(edit)
}

pub(crate) async fn put_change_catalog_entries<R>(
    root: ObjectId,
    entries: &[(ChangeId, ChangeCatalogEntry)],
    read: &R,
) -> Result<CatalogTreeEdit, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if entries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(corruption(
            "ChangeCatalog entries are not strictly ordered and distinct",
        ));
    }
    let mut mutations = Vec::with_capacity(entries.len());
    for (id, entry) in entries {
        let value = entry.encode()?;
        mutations.push(OrderedTreeMutation::Insert {
            key: id.as_bytes().to_vec(),
            value,
        });
    }
    let mut edit = edit_catalog(root, "change", &mutations, read, true).await?;
    edit.change_entries.extend(entries.iter().copied());
    Ok(edit)
}

pub(crate) async fn retire_commit_catalog_entries<R>(
    root: ObjectId,
    ids: &[CommitId],
    read: &R,
) -> Result<CatalogTreeEdit, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mutations = ids
        .iter()
        .map(|id| OrderedTreeMutation::Delete {
            key: id.as_bytes().to_vec(),
        })
        .collect::<Vec<_>>();
    edit_catalog(root, "commit", &mutations, read, false).await
}

pub(crate) async fn retire_change_catalog_entries<R>(
    root: ObjectId,
    ids: &[ChangeId],
    read: &R,
) -> Result<CatalogTreeEdit, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mutations = ids
        .iter()
        .map(|id| OrderedTreeMutation::Delete {
            key: id.as_bytes().to_vec(),
        })
        .collect::<Vec<_>>();
    edit_catalog(root, "change", &mutations, read, false).await
}

pub(crate) async fn load_commit<R>(
    view: &CoherentView<R>,
    id: CommitId,
) -> Result<Option<CommitObjectV1>, StorageError>
where
    R: StorageAdapterRead,
{
    let Some(value) = view
        .lookup_tree_value(
            view.repository_root().commit_catalog_root,
            "commit",
            id.as_bytes(),
        )
        .await?
    else {
        return Ok(None);
    };
    let entry = CommitCatalogEntry::decode(&value)?;
    let bytes = view.load_object_bytes(entry.commit_object_id).await?;
    let commit = CommitObjectV1::decode(entry.commit_object_id, &bytes)?;
    if commit.commit_id != id {
        return Err(corruption(
            "CommitCatalog key does not match embedded CommitId",
        ));
    }
    view.validate_retained_commit(
        view.repository_root().commit_catalog_root,
        view.repository_root().change_catalog_root,
        entry.commit_object_id,
        &commit,
    )
    .await?;
    Ok(Some(commit))
}

/// Loads the authenticated commit envelope and immediate topology needed to
/// publish a child. Member payloads remain lazy: callers that enumerate or
/// consume members must use the eager path above, which keeps every member and
/// ChangeCatalog owner check fail-closed at the point of access.
pub(crate) async fn load_commit_summary<R>(
    view: &CoherentView<R>,
    id: CommitId,
) -> Result<Option<CommitObjectV1>, crate::LixError>
where
    R: StorageAdapterRead,
{
    let Some(value) = view
        .lookup_tree_value(
            view.repository_root().commit_catalog_root,
            "commit",
            id.as_bytes(),
        )
        .await?
    else {
        return Ok(None);
    };
    let entry = CommitCatalogEntry::decode(&value)?;
    let bytes = view.load_object_bytes(entry.commit_object_id).await?;
    let commit = CommitObjectV1::decode(entry.commit_object_id, &bytes)?;
    if commit.commit_id != id {
        return Err(corruption("CommitCatalog key does not match Commit object").into());
    }
    view.validate_commit_topology(view.repository_root().commit_catalog_root, id, &commit)
        .await?;
    Ok(Some(commit))
}
pub(crate) async fn load_change<R>(
    view: &CoherentView<R>,
    id: ChangeId,
) -> Result<Option<ChangeObjectV1>, StorageError>
where
    R: StorageAdapterRead,
{
    let Some(value) = view
        .lookup_tree_value(
            view.repository_root().change_catalog_root,
            "change",
            id.as_bytes(),
        )
        .await?
    else {
        return Ok(None);
    };
    let entry = ChangeCatalogEntry::decode(&value)?;
    validate_change_entry(view, id, entry).await.map(Some)
}

pub(crate) async fn page_commits<R>(
    view: &CoherentView<R>,
    resume_token: Option<&[u8]>,
    page_size: usize,
) -> Result<CatalogPage<(CommitId, CommitObjectV1)>, StorageError>
where
    R: StorageAdapterRead,
{
    let root = view.repository_root().commit_catalog_root;
    let start_after = resume_token
        .map(|token| view.validate_resume_key(root, token))
        .transpose()?;
    let rows = view
        .scan_tree_page(root, "commit", start_after.as_deref(), page_size)
        .await?;
    let mut entries = Vec::with_capacity(rows.len());
    for (key, value) in &rows {
        let id = CommitId::from_bytes(
            key.as_slice()
                .try_into()
                .map_err(|_| corruption("CommitCatalog key is not a raw UUID"))?,
        );
        let entry = CommitCatalogEntry::decode(value)?;
        let bytes = view.load_object_bytes(entry.commit_object_id).await?;
        let commit = CommitObjectV1::decode(entry.commit_object_id, &bytes)?;
        if commit.commit_id != id {
            return Err(corruption(
                "CommitCatalog page has a mismatched key/object ID",
            ));
        }
        view.validate_retained_commit(
            view.repository_root().commit_catalog_root,
            view.repository_root().change_catalog_root,
            entry.commit_object_id,
            &commit,
        )
        .await?;
        entries.push((id, commit));
    }
    Ok(CatalogPage {
        resume_token: (rows.len() == page_size)
            .then(|| view.bind_resume_key(root, &rows[rows.len() - 1].0)),
        entries,
    })
}

pub(crate) async fn page_changes<R>(
    view: &CoherentView<R>,
    resume_token: Option<&[u8]>,
    page_size: usize,
) -> Result<CatalogPage<(ChangeId, ChangeObjectV1)>, StorageError>
where
    R: StorageAdapterRead,
{
    let root = view.repository_root().change_catalog_root;
    let start_after = resume_token
        .map(|token| view.validate_resume_key(root, token))
        .transpose()?;
    let rows = view
        .scan_tree_page(root, "change", start_after.as_deref(), page_size)
        .await?;
    let mut entries = Vec::with_capacity(rows.len());
    for (key, value) in &rows {
        let id = ChangeId::from_bytes(
            key.as_slice()
                .try_into()
                .map_err(|_| corruption("ChangeCatalog key is not a raw UUID"))?,
        );
        let entry = ChangeCatalogEntry::decode(value)?;
        entries.push((id, validate_change_entry(view, id, entry).await?));
    }
    Ok(CatalogPage {
        resume_token: (rows.len() == page_size)
            .then(|| view.bind_resume_key(root, &rows[rows.len() - 1].0)),
        entries,
    })
}

async fn edit_catalog<R>(
    root: ObjectId,
    kind: &'static str,
    mutations: &[OrderedTreeMutation],
    read: &R,
    idempotent_inserts: bool,
) -> Result<CatalogTreeEdit, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let root = validate_root_on_read(root, kind, read).await?;
    let edit = if idempotent_inserts {
        apply_ordered_mutations_idempotent_inserts(root, kind, mutations, read).await?
    } else {
        apply_ordered_mutations(root, kind, mutations, read).await?
    };
    Ok(CatalogTreeEdit {
        base_root: root.object_id,
        root: edit.root.object_id,
        entry_count: edit.root.entry_count,
        copied_nodes: edit.copied_nodes,
        commit_entries: BTreeMap::new(),
        change_entries: BTreeMap::new(),
        objects: edit.objects,
    })
}

async fn validate_change_entry<R>(
    view: &CoherentView<R>,
    id: ChangeId,
    entry: ChangeCatalogEntry,
) -> Result<ChangeObjectV1, StorageError>
where
    R: StorageAdapterRead,
{
    let bytes = view.load_object_bytes(entry.change_object_id).await?;
    let change = ChangeObjectV1::decode(entry.change_object_id, &bytes)?;
    if change.change_id() != id {
        return Err(corruption(
            "ChangeCatalog key does not match embedded ChangeId",
        ));
    }
    match (entry.owner, &change) {
        (
            ChangeCatalogOwner::CommitMember {
                commit_object_id,
                ordinal,
            },
            ChangeObjectV1::Semantic { .. },
        ) => {
            let bytes = view.load_object_bytes(commit_object_id).await?;
            let commit = CommitObjectV1::decode(commit_object_id, &bytes)?;
            let members = view.load_commit_members(&commit).await?;
            if members.get(ordinal as usize)
                != Some(&CommitMemberV1::introduced(entry.change_object_id))
            {
                return Err(corruption(
                    "ChangeCatalog commit owner does not point back at its ordinal member",
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
                ..
            },
        ) if ref_change_object_id == entry.change_object_id && branch_id == *object_branch => {
            view.validate_retained_ref_change(
                view.repository_root().change_catalog_root,
                entry.change_object_id,
                &change,
            )
            .await?;
        }
        _ => return Err(corruption("ChangeCatalog owner kind/back-edge is invalid")),
    }
    Ok(change)
}

/// Authenticates the immediate retained-history edges of one visited commit.
/// Callers page deeper history separately; unrelated immutable corruption is
/// intentionally latent until its edge is visited.
pub(super) async fn validate_retained_commit<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    change_catalog_root: ObjectId,
    commit_object_id: ObjectId,
    commit: &CommitObjectV1,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    for parent_id in &commit.parent_commit_object_ids {
        let bytes = super::view::load_object_bytes(read, *parent_id).await?;
        let parent = CommitObjectV1::decode(*parent_id, &bytes)?;
        if parent.generation >= commit.generation {
            return Err(corruption(
                "retained commit parent generation is not strictly earlier",
            ));
        }
    }
    let members = load_commit_members(read, commit).await?;
    for (ordinal, member) in members.iter().copied().enumerate() {
        let change_object_id = member.change_object_id();
        let bytes = super::view::load_object_bytes(read, change_object_id).await?;
        let change = ChangeObjectV1::decode(change_object_id, &bytes)?;
        if !matches!(change, ChangeObjectV1::Semantic { .. }) {
            return Err(corruption("commit member edge names a RefChange object"));
        }
        let value = lookup_on_read(
            change_catalog_root,
            "change",
            change.change_id().as_bytes(),
            read,
        )
        .await?
        .ok_or_else(|| corruption("retained Change object has no ChangeCatalog owner"))?;
        let entry = ChangeCatalogEntry::decode(&value)?;
        validate_member_catalog_owner(
            read,
            commit_catalog_root,
            commit_object_id,
            commit.generation,
            ordinal,
            member,
            entry,
        )
        .await?;
    }
    Ok(())
}

/// Authenticates one visited standalone branch-ref fact and its immediate
/// predecessor edge without eagerly materializing the full chronology.
pub(super) async fn validate_retained_ref_change<R>(
    read: &R,
    change_catalog_root: ObjectId,
    ref_object_id: ObjectId,
    change: &ChangeObjectV1,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let ChangeObjectV1::BranchRef {
        change_id,
        branch_id,
        before_semantic_head_commit_object_id,
        after_semantic_head_commit_object_id,
        previous_ref_change_object_id,
        ..
    } = change
    else {
        return Err(corruption("RefChange chronology reached a semantic Change"));
    };
    let value = lookup_on_read(change_catalog_root, "change", change_id.as_bytes(), read)
        .await?
        .ok_or_else(|| corruption("retained RefChange has no ChangeCatalog owner"))?;
    let entry = ChangeCatalogEntry::decode(&value)?;
    if entry.change_object_id != ref_object_id
        || entry.owner
            != (ChangeCatalogOwner::BranchRef {
                ref_change_object_id: ref_object_id,
                branch_id: *branch_id,
            })
    {
        return Err(corruption(
            "retained RefChange disagrees with its ChangeCatalog owner/back-edge",
        ));
    }
    for target in [
        *before_semantic_head_commit_object_id,
        *after_semantic_head_commit_object_id,
    ]
    .into_iter()
    .flatten()
    {
        let bytes = super::view::load_object_bytes(read, target).await?;
        let _ = CommitObjectV1::decode(target, &bytes)?;
    }
    match previous_ref_change_object_id {
        Some(previous_id) => {
            let bytes = super::view::load_object_bytes(read, *previous_id).await?;
            let previous = ChangeObjectV1::decode(*previous_id, &bytes)?;
            let ChangeObjectV1::BranchRef {
                branch_id: previous_branch_id,
                after_semantic_head_commit_object_id: previous_after,
                ..
            } = previous
            else {
                return Err(corruption("RefChange predecessor is a semantic Change"));
            };
            if previous_branch_id != *branch_id
                || previous_after != *before_semantic_head_commit_object_id
            {
                return Err(corruption(
                    "RefChange predecessor branch binding is invalid",
                ));
            }
        }
        None if before_semantic_head_commit_object_id.is_some() => {
            return Err(corruption(
                "non-creation RefChange is missing its authenticated predecessor",
            ));
        }
        None => {}
    }
    Ok(())
}

fn decode_state_value_storage(bytes: &[u8]) -> Result<StateValue, StorageError> {
    decode_state_value(bytes).map_err(|error| corruption(error.to_string()))
}
