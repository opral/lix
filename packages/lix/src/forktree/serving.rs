use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::common::LixTimestamp;
use crate::storage::{
    BeginScanOptions, CoreProjection, GetManyRequest, GetManyResult, GetOptions, Key, KeyRange,
    Prefix, ProjectedValue, ScanCursor, ScanOrder, StorageError, StorageSpace,
};
use crate::storage_adapter::StorageAdapterRead;

use super::codec::corruption;
use super::model::{
    BranchSelectorV1, CanonicalBranchId, ChangeCatalogEntry, ChangeCatalogOwner, ChangeId,
    ChangeObjectV1, CommitCatalogEntry, CommitId, CommitMemberV1, CommitObjectV1, GlobalSelectorV1,
    RepositoryRootV1, branch_selector_key, global_selector_key,
};
use super::object::ObjectId;
use super::state::{
    HistoricalStateRow, StateCell, StateValue, decode_state_key, decode_state_value,
    encode_state_key,
};
use super::tree::{
    ImmutableObjectSet, OrderedTreeMutation, apply_ordered_mutations,
    apply_ordered_mutations_idempotent_inserts, delete_ordered_range, lookup_many_on_read,
    lookup_on_read, scan_bounded_page_on_read, scan_page_on_read, scan_range_on_read,
    scan_ranges_on_read, validate_root_on_read,
};
use super::view::{CoherentView, SELECTOR_SPACE};

const BRANCH_SELECTOR_PREFIX: &[u8] = b"branch/";
const BRANCH_SCAN_PAGE_ROWS: usize = 256;
const CATALOG_SCAN_PAGE_ROWS: usize = 1024;

/// Loads a commit's complete authenticated member closure from its mandatory
/// byte-bounded change-page chain.
pub(crate) async fn load_commit_members<R>(
    read: &R,
    commit: &CommitObjectV1,
) -> Result<Vec<CommitMemberV1>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if commit.member_page_object_ids.is_empty() {
        return Ok(commit.members.clone());
    }
    if !commit.members.is_empty() {
        return Err(corruption("paged commit carries an inline member closure"));
    }
    let mut members = Vec::new();
    let pages =
        super::view::load_object_map(read, commit.member_page_object_ids.iter().copied()).await?;
    for page_id in &commit.member_page_object_ids {
        let bytes = pages
            .get(page_id)
            .ok_or_else(|| corruption("commit change page is absent"))?;
        let page = super::model::CommitChangePageV2::decode(*page_id, bytes)?;
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
    }
    let mut unique_changes = BTreeSet::new();
    for member in &members {
        if !unique_changes.insert(member.change_id()) {
            return Err(corruption("commit member page chain repeats a ChangeId"));
        }
    }
    Ok(members)
}

#[derive(Clone, Debug)]
pub(super) struct ResolvedSemanticMember {
    change_id: ChangeId,
    payload: Vec<u8>,
    global: bool,
    updated_at: LixTimestamp,
    selected_created_at: Option<LixTimestamp>,
    blob_manifest_object_ids: Vec<ObjectId>,
}

async fn resolve_semantic_member<R>(
    read: &R,
    member: &CommitMemberV1,
) -> Result<ResolvedSemanticMember, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    resolve_semantic_member_with_cache(read, member, &mut BTreeMap::new()).await
}

async fn resolve_semantic_member_with_cache<R>(
    read: &R,
    member: &CommitMemberV1,
    member_closures: &mut BTreeMap<ObjectId, Arc<[CommitMemberV1]>>,
) -> Result<ResolvedSemanticMember, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let expected_change_id = member.change_id();
    let mut current = member.clone();
    let mut visited = BTreeSet::new();
    let mut selected_created_at = None;
    loop {
        match current {
            CommitMemberV1::Introduced {
                change_id,
                payload,
                global,
                updated_at,
                blob_manifest_object_ids,
            } => {
                if change_id != expected_change_id {
                    return Err(corruption(
                        "selected member source changes its authenticated ChangeId",
                    ));
                }
                return Ok(ResolvedSemanticMember {
                    change_id,
                    payload,
                    global,
                    updated_at,
                    selected_created_at,
                    blob_manifest_object_ids,
                });
            }
            CommitMemberV1::Selected {
                change_id,
                source_commit_object_id,
                source_ordinal,
                created_at,
            } => {
                if change_id != expected_change_id
                    || !visited.insert((source_commit_object_id, source_ordinal))
                {
                    return Err(corruption(
                        "selected member source is cyclic or changes its ChangeId",
                    ));
                }
                selected_created_at.get_or_insert(created_at);
                let source_members =
                    if let Some(members) = member_closures.get(&source_commit_object_id) {
                        Arc::clone(members)
                    } else {
                        let bytes =
                            super::view::load_object_bytes(read, source_commit_object_id).await?;
                        let source = CommitObjectV1::decode(source_commit_object_id, &bytes)?;
                        let members: Arc<[CommitMemberV1]> =
                            load_commit_members(read, &source).await?.into();
                        member_closures.insert(source_commit_object_id, Arc::clone(&members));
                        members
                    };
                current = source_members
                    .get(source_ordinal as usize)
                    .cloned()
                    .ok_or_else(|| corruption("selected member source ordinal is absent"))?;
            }
        }
    }
}

async fn semantic_change_for_member<R>(
    read: &R,
    member: &CommitMemberV1,
) -> Result<ChangeObjectV1, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let resolved = resolve_semantic_member(read, member).await?;
    let payload = if let Some(created_at) = resolved.selected_created_at {
        let public_change_id =
            crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(*resolved.change_id.as_bytes()));
        let mut record =
            crate::changelog::decode_forktree_change_payload(&resolved.payload, public_change_id)
                .map_err(|error| corruption(error.to_string()))?;
        record.created_at = created_at;
        crate::changelog::encode_forktree_change_payload(&record)
            .map_err(|error| corruption(error.to_string()))?
    } else {
        resolved.payload
    };
    Ok(ChangeObjectV1::Semantic {
        change_id: resolved.change_id,
        payload,
        json_payload_object_ids: Vec::new(),
    })
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
    Insert {
        key: Vec<u8>,
        value: Vec<u8>,
        audit: Option<StateMutationAudit>,
    },
    Update {
        key: Vec<u8>,
        value: Vec<u8>,
        audit: Option<StateMutationAudit>,
    },
    Remove {
        key: Vec<u8>,
    },
    RemoveRange {
        lower: Vec<u8>,
        upper: Option<Vec<u8>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StateMutationAudit {
    pub(crate) commit_id: [u8; 16],
    pub(crate) tombstone: bool,
    pub(crate) blob_manifest_object_ids: Vec<ObjectId>,
}

impl StateTreeMutation {
    pub(crate) fn insert(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self::Insert {
            key,
            value,
            audit: None,
        }
    }

    pub(crate) fn update(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self::Update {
            key,
            value,
            audit: None,
        }
    }

    pub(crate) fn insert_bound(key: Vec<u8>, value: Vec<u8>, audit: StateMutationAudit) -> Self {
        Self::Insert {
            key,
            value,
            audit: Some(audit),
        }
    }

    pub(crate) fn update_bound(key: Vec<u8>, value: Vec<u8>, audit: StateMutationAudit) -> Self {
        Self::Update {
            key,
            value,
            audit: Some(audit),
        }
    }

    pub(crate) fn remove(key: Vec<u8>) -> Self {
        Self::Remove { key }
    }

    pub(crate) fn remove_range(lower: Vec<u8>, upper: Option<Vec<u8>>) -> Self {
        Self::RemoveRange { lower, upper }
    }

    fn into_ordered(self) -> OrderedTreeMutation {
        match self {
            Self::Insert { key, value, .. } => OrderedTreeMutation::Insert { key, value },
            Self::Update { key, value, .. } => OrderedTreeMutation::Update { key, value },
            Self::Remove { key } => OrderedTreeMutation::Delete { key },
            Self::RemoveRange { .. } => {
                unreachable!("range deletion is lowered directly by edit_state_tree")
            }
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

    pub(crate) fn stage_objects(
        &mut self,
        objects: impl IntoIterator<Item = (ObjectId, bytes::Bytes)>,
    ) -> Result<(), StorageError> {
        for (id, bytes) in objects {
            self.objects.insert(id, bytes)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct CatalogTreeEdit {
    pub(super) base_root: ObjectId,
    pub(crate) root: ObjectId,
    pub(crate) commit_entries: BTreeMap<CommitId, CommitCatalogEntry>,
    pub(crate) change_entries: BTreeMap<ChangeId, ChangeCatalogEntry>,
    pub(super) objects: ImmutableObjectSet,
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

/// One authenticated branch result returned by the selector/control-plane
/// batch. The head, RefChange identity, and timestamp are all derived from
/// the same retained read and validated against the same branch snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AuthenticatedBranchHead {
    pub(crate) branch_id: String,
    pub(crate) head_commit_id: crate::changelog::CommitId,
    pub(crate) change_id: crate::changelog::ChangeId,
    pub(crate) updated_at: LixTimestamp,
}

/// Loads selected branch heads and their authenticated RefChange metadata in
/// one retained read. `None` selects every branch selector; `Some` preserves
/// requested order and omits absent selectors. Selector, snapshot, root,
/// head, RefChange, catalog, predecessor, and target objects are acquired in
/// batches. No per-branch coherent view or retained-ref validator is opened.
pub(crate) async fn load_branch_heads_with_metadata<R>(
    read: &R,
    requested_branch_ids: Option<&[String]>,
) -> Result<Vec<AuthenticatedBranchHead>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let selectors = load_branch_selectors(read, requested_branch_ids).await?;
    if selectors.is_empty() {
        return Ok(Vec::new());
    }

    let global_key = Key(global_selector_key());
    let global = read
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: std::slice::from_ref(&global_key),
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    let raw_global = match global.values.into_iter().next().flatten() {
        Some(ProjectedValue::FullValue(bytes)) => bytes,
        Some(ProjectedValue::KeyOnly) => {
            return Err(corruption("ForkTree global selector returned key-only data").into());
        }
        None => return Err(corruption("ForkTree global selector is absent").into()),
    };
    let global_selector = GlobalSelectorV1::decode(&raw_global)?;

    let mut root_ids = Vec::with_capacity(selectors.len() + 1);
    root_ids.push(global_selector.repository_root);
    root_ids.extend(
        selectors
            .iter()
            .map(|selector| selector.branch_snapshot_object_id),
    );
    root_ids.sort_unstable();
    root_ids.dedup();
    let roots = super::view::load_object_map(read, root_ids).await?;
    let repository_root = RepositoryRootV1::decode(
        global_selector.repository_root,
        required_scan_object(&roots, global_selector.repository_root)?,
    )?;
    let mut snapshots = Vec::with_capacity(selectors.len());
    let mut selected_ids = Vec::with_capacity(selectors.len() * 2);
    for selector in &selectors {
        let snapshot = super::model::BranchSnapshotV1::decode(
            selector.branch_snapshot_object_id,
            required_scan_object(&roots, selector.branch_snapshot_object_id)?,
        )?;
        if snapshot.branch_id != selector.branch_id {
            return Err(corruption("ForkTree branch selector and snapshot IDs differ").into());
        }
        selected_ids.push(snapshot.semantic_head_commit_object_id);
        selected_ids.push(snapshot.latest_ref_change_object_id.ok_or_else(|| {
            corruption("branch snapshot has no authenticated latest RefChange edge")
        })?);
        snapshots.push(snapshot);
    }

    let mut authenticated_root_ids = vec![
        repository_root.global_state_root,
        repository_root.commit_catalog_root,
        repository_root.change_catalog_root,
    ];
    authenticated_root_ids.extend(snapshots.iter().flat_map(|snapshot| {
        [
            snapshot.local_state_root,
            snapshot.historical_global_state_root,
        ]
    }));
    authenticated_root_ids.sort_unstable();
    authenticated_root_ids.dedup();
    let authenticated_roots = super::view::load_object_map(read, authenticated_root_ids).await?;
    for (id, kind) in [
        (repository_root.global_state_root, "state"),
        (repository_root.commit_catalog_root, "commit"),
        (repository_root.change_catalog_root, "change"),
    ] {
        super::tree::validate_root_bytes(
            id,
            kind,
            required_scan_object(&authenticated_roots, id)?,
        )?;
    }
    for snapshot in &snapshots {
        for (id, kind) in [
            (snapshot.local_state_root, "state"),
            (snapshot.historical_global_state_root, "state"),
        ] {
            super::tree::validate_root_bytes(
                id,
                kind,
                required_scan_object(&authenticated_roots, id)?,
            )?;
        }
    }

    selected_ids.sort_unstable();
    selected_ids.dedup();
    let selected_objects = super::view::load_object_map(read, selected_ids).await?;
    let mut heads = Vec::with_capacity(snapshots.len());
    let mut refs = Vec::with_capacity(snapshots.len());
    for snapshot in &snapshots {
        let head = CommitObjectV1::decode(
            snapshot.semantic_head_commit_object_id,
            required_scan_object(&selected_objects, snapshot.semantic_head_commit_object_id)?,
        )?;
        if head.global_state_root != snapshot.historical_global_state_root
            || head.local_state_root != snapshot.local_state_root
        {
            return Err(
                corruption("selected semantic head does not authenticate branch roots").into(),
            );
        }
        let ref_id = snapshot.latest_ref_change_object_id.ok_or_else(|| {
            corruption("branch snapshot has no authenticated latest RefChange edge")
        })?;
        let change =
            ChangeObjectV1::decode(ref_id, required_scan_object(&selected_objects, ref_id)?)?;
        let ChangeObjectV1::BranchRef {
            branch_id,
            after_semantic_head_commit_object_id,
            ..
        } = &change
        else {
            return Err(corruption(
                "branch snapshot latest RefChange edge names a semantic Change",
            )
            .into());
        };
        if *branch_id != snapshot.branch_id
            || *after_semantic_head_commit_object_id
                != Some(snapshot.semantic_head_commit_object_id)
        {
            return Err(corruption(
                "branch snapshot latest RefChange does not match its branch/head",
            )
            .into());
        }
        heads.push(head);
        refs.push((ref_id, change));
    }

    let change_keys = refs
        .iter()
        .map(|(_, change)| change.change_id().as_bytes().to_vec())
        .collect::<Vec<_>>();
    let catalog_values = lookup_many_on_read(
        repository_root.change_catalog_root,
        "change",
        &change_keys,
        read,
    )
    .await?;
    if catalog_values.len() != refs.len() {
        return Err(
            corruption("batched ChangeCatalog lookup returned the wrong number of values").into(),
        );
    }
    for ((ref_id, change), value) in refs.iter().zip(catalog_values) {
        let value =
            value.ok_or_else(|| corruption("retained RefChange has no ChangeCatalog owner"))?;
        let entry = ChangeCatalogEntry::decode(&value)?;
        if entry.owner
            != (ChangeCatalogOwner::BranchRef {
                ref_change_object_id: *ref_id,
                branch_id: match change {
                    ChangeObjectV1::BranchRef { branch_id, .. } => *branch_id,
                    ChangeObjectV1::Semantic { .. } => {
                        return Err(
                            corruption("RefChange chronology reached a semantic Change").into()
                        );
                    }
                },
            })
        {
            return Err(corruption(
                "retained RefChange disagrees with its ChangeCatalog owner/back-edge",
            )
            .into());
        }
    }

    let mut provenance_ids = Vec::new();
    for (_, change) in &refs {
        let ChangeObjectV1::BranchRef {
            before_semantic_head_commit_object_id,
            after_semantic_head_commit_object_id,
            previous_ref_change_object_id,
            ..
        } = change
        else {
            return Err(corruption("RefChange chronology reached a semantic Change").into());
        };
        provenance_ids.extend(
            before_semantic_head_commit_object_id
                .into_iter()
                .chain(after_semantic_head_commit_object_id)
                .copied(),
        );
        if let Some(previous_id) = previous_ref_change_object_id {
            provenance_ids.push(*previous_id);
        } else if before_semantic_head_commit_object_id.is_some() {
            return Err(corruption(
                "non-creation RefChange is missing its authenticated predecessor",
            )
            .into());
        }
    }
    provenance_ids.sort_unstable();
    provenance_ids.dedup();
    let provenance_objects = super::view::load_object_map(read, provenance_ids).await?;
    for (_, change) in &refs {
        let ChangeObjectV1::BranchRef {
            branch_id,
            before_semantic_head_commit_object_id,
            after_semantic_head_commit_object_id,
            previous_ref_change_object_id,
            ..
        } = change
        else {
            return Err(corruption("RefChange chronology reached a semantic Change").into());
        };
        if let Some(previous_id) = previous_ref_change_object_id {
            let previous = ChangeObjectV1::decode(
                *previous_id,
                required_scan_object(&provenance_objects, *previous_id)?,
            )?;
            let ChangeObjectV1::BranchRef {
                branch_id: previous_branch_id,
                after_semantic_head_commit_object_id: previous_after,
                ..
            } = previous
            else {
                return Err(corruption("RefChange predecessor is a semantic Change").into());
            };
            if previous_branch_id != *branch_id
                || previous_after != *before_semantic_head_commit_object_id
            {
                return Err(corruption("RefChange predecessor branch binding is invalid").into());
            }
        }
        for target in [
            *before_semantic_head_commit_object_id,
            *after_semantic_head_commit_object_id,
        ]
        .into_iter()
        .flatten()
        {
            let _ =
                CommitObjectV1::decode(target, required_scan_object(&provenance_objects, target)?)?;
        }
    }

    Ok(snapshots
        .into_iter()
        .zip(heads)
        .zip(refs)
        .map(|((snapshot, head), (_, change))| {
            let ChangeObjectV1::BranchRef {
                change_id,
                updated_at,
                ..
            } = change
            else {
                unreachable!("validated branch ref changes are BranchRef");
            };
            AuthenticatedBranchHead {
                branch_id: uuid::Uuid::from_bytes(*snapshot.branch_id.as_bytes()).to_string(),
                head_commit_id: crate::changelog::CommitId::new(uuid::Uuid::from_bytes(
                    *head.commit_id.as_bytes(),
                )),
                change_id: crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(
                    *change_id.as_bytes(),
                )),
                updated_at,
            }
        })
        .collect())
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

async fn load_branch_selectors<R>(
    read: &R,
    requested_branch_ids: Option<&[String]>,
) -> Result<Vec<BranchSelectorV1>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let Some(requested_branch_ids) = requested_branch_ids else {
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
        let mut selectors = Vec::new();
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
                selectors.push(selector);
            }
            if !page.has_more {
                break;
            }
        }
        return Ok(selectors);
    };

    if requested_branch_ids.is_empty() {
        return Ok(Vec::new());
    }
    let canonical_ids = requested_branch_ids
        .iter()
        .map(|branch_id| canonical_branch_id(branch_id))
        .collect::<Result<Vec<_>, _>>()?;
    let keys = canonical_ids
        .iter()
        .map(|branch_id| Key(branch_selector_key(*branch_id)))
        .collect::<Vec<_>>();
    let loaded = read
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    if loaded.values.len() != keys.len() {
        return Err(corruption("branch selector batch returned the wrong number of values").into());
    }
    let mut selectors = Vec::new();
    for ((canonical_id, key), value) in canonical_ids.into_iter().zip(keys).zip(loaded.values) {
        let Some(value) = value else {
            continue;
        };
        let bytes = match value {
            ProjectedValue::FullValue(bytes) => bytes,
            ProjectedValue::KeyOnly => {
                return Err(corruption(
                    "ForkTree branch selector point read returned key-only data",
                )
                .into());
            }
        };
        let selector = BranchSelectorV1::decode(&bytes)?;
        if selector.branch_id != canonical_id || branch_selector_key(selector.branch_id) != key.0 {
            return Err(
                corruption("ForkTree branch selector key and embedded branch ID differ").into(),
            );
        }
        selectors.push(selector);
    }
    Ok(selectors)
}

fn required_scan_object(
    objects: &BTreeMap<ObjectId, bytes::Bytes>,
    id: ObjectId,
) -> Result<&bytes::Bytes, StorageError> {
    objects
        .get(&id)
        .ok_or_else(|| corruption(format!("selected branch object {id} is absent")))
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
/// CommitCatalog. An absent catalog entry is corruption here; only an
/// authenticated state-key absence is a valid empty result.
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

/// Reader-local semantic history cache. It is created for one
/// `load_change_records` invocation and is never persisted or shared with a
/// different retained read. The existing decoders and page-chain validation
/// remain authoritative; this only avoids fetching and decoding the same
/// immutable commit/member closure once per ChangeCatalog entry.
#[derive(Default)]
struct SemanticChangeReadCache {
    closures: BTreeMap<ObjectId, AuthenticatedCommitMemberClosure>,
}

fn packed_change_address(id: ChangeId) -> Option<(ChangeId, u32)> {
    let mut base = *id.as_bytes();
    let ordinal = u32::from_be_bytes(base[12..].try_into().expect("four-byte suffix"));
    if ordinal == 0 {
        return None;
    }
    base[12..].fill(0);
    Some((ChangeId::from_bytes(base), ordinal))
}

async fn packed_change_catalog_entry<R>(
    read: &R,
    change_catalog_root: ObjectId,
    id: ChangeId,
) -> Result<Option<ChangeCatalogEntry>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let Some((base, ordinal)) = packed_change_address(id) else {
        return Ok(None);
    };
    let Some(value) = lookup_on_read(change_catalog_root, "change", base.as_bytes(), read).await?
    else {
        return Ok(None);
    };
    let marker = ChangeCatalogEntry::decode(&value)?;
    let ChangeCatalogOwner::PackedCommit {
        commit_object_id,
        member_count,
    } = marker.owner
    else {
        return Ok(None);
    };
    if ordinal > member_count {
        return Ok(None);
    }
    Ok(Some(ChangeCatalogEntry {
        owner: ChangeCatalogOwner::CommitMember {
            commit_object_id,
            ordinal: ordinal - 1,
        },
    }))
}

async fn lookup_change_catalog_entries<R>(
    read: &R,
    change_catalog_root: ObjectId,
    ids: &[ChangeId],
) -> Result<Vec<Option<ChangeCatalogEntry>>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let keys = ids
        .iter()
        .map(|id| id.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let direct = lookup_many_on_read(change_catalog_root, "change", &keys, read).await?;
    let mut marker_keys = BTreeSet::<ChangeId>::new();
    for (id, value) in ids.iter().zip(&direct) {
        if value.is_none()
            && let Some((base, _)) = packed_change_address(*id)
        {
            marker_keys.insert(base);
        }
    }
    let marker_ids = marker_keys.into_iter().collect::<Vec<_>>();
    let marker_values = if marker_ids.is_empty() {
        Vec::new()
    } else {
        let keys = marker_ids
            .iter()
            .map(|id| id.as_bytes().to_vec())
            .collect::<Vec<_>>();
        lookup_many_on_read(change_catalog_root, "change", &keys, read).await?
    };
    let mut marker_entries = BTreeMap::new();
    for (id, value) in marker_ids.into_iter().zip(marker_values) {
        if let Some(value) = value {
            marker_entries.insert(id, ChangeCatalogEntry::decode(&value)?);
        }
    }
    ids.iter()
        .zip(direct)
        .map(|(id, value)| {
            if let Some(value) = value {
                let entry = ChangeCatalogEntry::decode(&value)?;
                return Ok(
                    (!matches!(entry.owner, ChangeCatalogOwner::PackedCommit { .. }))
                        .then_some(entry),
                );
            }
            let Some((base, ordinal)) = packed_change_address(*id) else {
                return Ok(None);
            };
            let Some(marker) = marker_entries.get(&base) else {
                return Ok(None);
            };
            let ChangeCatalogOwner::PackedCommit {
                commit_object_id,
                member_count,
            } = marker.owner
            else {
                return Ok(None);
            };
            Ok((ordinal <= member_count).then_some(ChangeCatalogEntry {
                owner: ChangeCatalogOwner::CommitMember {
                    commit_object_id,
                    ordinal: ordinal - 1,
                },
            }))
        })
        .collect()
}

pub(crate) async fn load_change_records<R>(
    read: &R,
    ids: &[crate::changelog::ChangeId],
) -> Result<Vec<Option<crate::changelog::ChangeRecord>>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let repository = load_repository_root(read).await?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let model_ids = ids
        .iter()
        .map(|id| ChangeId::from_bytes(*id.as_uuid().as_bytes()))
        .collect::<Vec<_>>();
    let entries =
        lookup_change_catalog_entries(read, repository.change_catalog_root, &model_ids).await?;
    let mut records = Vec::with_capacity(ids.len());
    let mut cache = SemanticChangeReadCache::default();
    for (id, entry) in model_ids.into_iter().zip(entries) {
        let Some(entry) = entry else {
            records.push(None);
            continue;
        };
        records.push(
            semantic_change_record_cached(
                read,
                repository.commit_catalog_root,
                repository.change_catalog_root,
                id,
                entry,
                &mut cache,
            )
            .await?,
        );
    }
    tracing::debug!(
        target: "lix_perf",
        requested_change_ids = ids.len(),
        unique_commit_member_closures = cache.closures.len(),
        "lix.perf.merge_change_records_batch"
    );
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
    let member_ids = members
        .iter()
        .map(CommitMemberV1::change_id)
        .collect::<Vec<_>>();
    let catalog_entries =
        lookup_change_catalog_entries(read, repository.change_catalog_root, &member_ids).await?;
    let mut records = Vec::with_capacity(members.len());
    for (ordinal, (member, entry)) in members.iter().zip(catalog_entries).enumerate() {
        let change_id = member.change_id();
        let entry = entry.ok_or_else(|| corruption("Commit member has no ChangeCatalog owner"))?;
        validate_member_catalog_owner(
            read,
            repository.commit_catalog_root,
            commit_object_id,
            commit.generation,
            ordinal,
            member.clone(),
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
        let ChangeObjectV1::Semantic { payload, .. } =
            semantic_change_for_member(read, member).await?
        else {
            return Err(corruption("Commit member has no semantic Change payload").into());
        };
        let record = crate::changelog::decode_forktree_change_payload(
            &payload,
            crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(*change_id.as_bytes())),
        )?;
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
    let mut values =
        load_state_values_at_commit(read, commit_id, &[key.to_vec()], include_tombstone).await?;
    Ok(values.pop().flatten())
}

/// Loads exact historical state identities after authenticating the selected
/// commit envelope and retained member closure once. All tree lookups remain
/// bound to the caller's retained read and preserve input order/duplicates.
pub(crate) async fn load_state_values_at_commit<R>(
    read: &R,
    commit_id: crate::changelog::CommitId,
    keys: &[Vec<u8>],
    include_tombstone: bool,
) -> Result<Vec<Option<(StateValue, StateSource)>>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let (
        commit_catalog_root,
        change_catalog_root,
        endpoint_commit_object_id,
        global_state_root,
        local_state_root,
    ) = authenticate_historical_state_roots_for_diff(read, commit_id).await?;
    state_points_on_read_with_historical_auth(
        global_state_root,
        Some(local_state_root),
        keys,
        include_tombstone,
        commit_catalog_root,
        change_catalog_root,
        endpoint_commit_object_id,
        read,
    )
    .await
    .map_err(Into::into)
}

pub(crate) async fn authenticate_historical_state_roots<R>(
    read: &R,
    commit_id: crate::changelog::CommitId,
) -> Result<(ObjectId, ObjectId), crate::LixError>
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
    Ok((commit.global_state_root, commit.local_state_root))
}

/// Authenticates the bounded envelope/topology needed to diff two historical
/// roots without eagerly resolving every unrelated commit member. The state
/// point path remains responsible for authenticating each changed state page,
/// selected member, source ordinal, ChangeCatalog owner, and semantic payload
/// before it is returned. This is deliberately a distinct ForkTree-owned
/// operation: callers cannot supply or seed an attestation, and no raw or
/// compatibility reader is introduced.
pub(crate) async fn authenticate_historical_state_roots_for_diff<R>(
    read: &R,
    commit_id: crate::changelog::CommitId,
) -> Result<(ObjectId, ObjectId, ObjectId, ObjectId, ObjectId), crate::LixError>
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
    validate_commit_catalog_identity(
        read,
        repository.commit_catalog_root,
        entry.commit_object_id,
        &commit,
    )
    .await?;
    validate_commit_topology(read, repository.commit_catalog_root, catalog_id, &commit).await?;
    Ok((
        repository.commit_catalog_root,
        repository.change_catalog_root,
        entry.commit_object_id,
        commit.global_state_root,
        commit.local_state_root,
    ))
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
    let public_start = start_after.map(|id| ChangeId::from_bytes(*id.as_uuid().as_bytes()));
    let mut cursor = public_start.map(|id| id.as_bytes().to_vec());
    let mut records = Vec::with_capacity(limit);
    let mut cache = SemanticChangeReadCache::default();

    // A page token may point inside one packed commit. Resume that marker
    // before advancing the physical catalog cursor beyond its base key.
    if let Some(start) = public_start
        && let Some((base, ordinal)) = packed_change_address(start)
        && let Some(value) = lookup_on_read(
            repository.change_catalog_root,
            "change",
            base.as_bytes(),
            read,
        )
        .await?
    {
        let marker = ChangeCatalogEntry::decode(&value)?;
        append_packed_change_records(
            read,
            repository.commit_catalog_root,
            repository.change_catalog_root,
            base,
            marker,
            ordinal,
            limit,
            &mut records,
            &mut cache,
        )
        .await?;
        if records.len() == limit {
            return Ok(records);
        }
        cursor = Some(base.as_bytes().to_vec());
    }

    while records.len() < limit {
        let rows = scan_page_on_read(
            repository.change_catalog_root,
            "change",
            cursor.as_deref(),
            CATALOG_SCAN_PAGE_ROWS,
            read,
        )
        .await?;
        if rows.is_empty() {
            break;
        }
        let row_count = rows.len();
        for (key, value) in rows {
            cursor = Some(key.clone());
            let id = ChangeId::from_bytes(
                key.as_slice()
                    .try_into()
                    .map_err(|_| corruption("ChangeCatalog key is not a raw UUID"))?,
            );
            let entry = ChangeCatalogEntry::decode(&value)?;
            if matches!(entry.owner, ChangeCatalogOwner::PackedCommit { .. }) {
                append_packed_change_records(
                    read,
                    repository.commit_catalog_root,
                    repository.change_catalog_root,
                    id,
                    entry,
                    0,
                    limit,
                    &mut records,
                    &mut cache,
                )
                .await?;
            } else if let Some(record) =
                semantic_change_record(read, repository.change_catalog_root, id, entry).await?
            {
                records.push(record);
            }
            if records.len() == limit {
                break;
            }
        }
        if row_count < CATALOG_SCAN_PAGE_ROWS {
            break;
        }
    }
    Ok(records)
}

#[expect(clippy::too_many_arguments)]
async fn append_packed_change_records<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    change_catalog_root: ObjectId,
    base: ChangeId,
    marker: ChangeCatalogEntry,
    start_ordinal: u32,
    limit: usize,
    records: &mut Vec<crate::changelog::ChangeRecord>,
    cache: &mut SemanticChangeReadCache,
) -> Result<(), crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let ChangeCatalogOwner::PackedCommit {
        commit_object_id,
        member_count,
    } = marker.owner
    else {
        return Ok(());
    };
    let (commit, members) = load_authenticated_commit_member_closure(
        read,
        commit_catalog_root,
        commit_object_id,
        &mut cache.closures,
    )
    .await?;
    if commit.commit_id.as_bytes() != base.as_bytes()
        || usize::try_from(member_count).ok() != Some(members.len())
    {
        return Err(corruption("packed ChangeCatalog marker disagrees with its commit").into());
    }
    for ordinal in start_ordinal.saturating_add(1)..=member_count {
        if records.len() == limit {
            break;
        }
        let mut bytes = *base.as_bytes();
        bytes[12..].copy_from_slice(&ordinal.to_be_bytes());
        let id = ChangeId::from_bytes(bytes);
        let ordinal_index = ordinal - 1;
        let member = members
            .get(ordinal_index as usize)
            .ok_or_else(|| corruption("packed commit ordinal is absent"))?;
        if member.change_id() != id || member.source().is_some() {
            return Err(corruption("packed commit ordinal identity is invalid").into());
        }
        let entry = ChangeCatalogEntry {
            owner: ChangeCatalogOwner::CommitMember {
                commit_object_id,
                ordinal: ordinal_index,
            },
        };
        if let Some(record) = semantic_change_record_cached(
            read,
            commit_catalog_root,
            change_catalog_root,
            id,
            entry,
            cache,
        )
        .await?
        {
            records.push(record);
        }
    }
    Ok(())
}

pub(crate) async fn load_repository_root<R>(read: &R) -> Result<RepositoryRootV1, crate::LixError>
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

async fn semantic_change_record_cached<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    change_catalog_root: ObjectId,
    id: ChangeId,
    entry: ChangeCatalogEntry,
    cache: &mut SemanticChangeReadCache,
) -> Result<Option<crate::changelog::ChangeRecord>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let change = match entry.owner {
        ChangeCatalogOwner::CommitMember {
            commit_object_id,
            ordinal,
        } => {
            let (_, members) = load_authenticated_commit_member_closure(
                read,
                commit_catalog_root,
                commit_object_id,
                &mut cache.closures,
            )
            .await?;
            let member = members
                .get(ordinal as usize)
                .ok_or_else(|| corruption("ChangeCatalog owner ordinal is absent"))?;
            if member.change_id() != id || member.source().is_some() {
                return Err(corruption("ChangeCatalog owner/ordinal back-edge is invalid").into());
            }
            semantic_change_for_member_with_commit_cache(
                read,
                commit_catalog_root,
                member,
                &mut cache.closures,
            )
            .await?
        }
        ChangeCatalogOwner::BranchRef {
            ref_change_object_id,
            branch_id,
        } => {
            let bytes = super::view::load_object_bytes(read, ref_change_object_id).await?;
            let change = ChangeObjectV1::decode(ref_change_object_id, &bytes)?;
            let ChangeObjectV1::BranchRef {
                branch_id: object_branch,
                ..
            } = &change
            else {
                return Err(corruption("branch-ref catalog owner names semantic payload").into());
            };
            if branch_id != *object_branch {
                return Err(corruption("ChangeCatalog branch owner/back-edge is invalid").into());
            }
            validate_retained_ref_change(read, change_catalog_root, ref_change_object_id, &change)
                .await?;
            change
        }
        ChangeCatalogOwner::PackedCommit { .. } => {
            return Err(corruption("packed commit marker is not a semantic ChangeId").into());
        }
    };
    decode_semantic_change_record(id, change)
}

fn decode_semantic_change_record(
    id: ChangeId,
    change: ChangeObjectV1,
) -> Result<Option<crate::changelog::ChangeRecord>, crate::LixError> {
    if change.change_id() != id {
        return Err(corruption("ChangeCatalog key does not match embedded ChangeId").into());
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

async fn semantic_change_record<R>(
    read: &R,
    change_catalog_root: ObjectId,
    id: ChangeId,
    entry: ChangeCatalogEntry,
) -> Result<Option<crate::changelog::ChangeRecord>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let change = match entry.owner {
        ChangeCatalogOwner::CommitMember {
            commit_object_id,
            ordinal,
        } => {
            let bytes = super::view::load_object_bytes(read, commit_object_id).await?;
            let commit = CommitObjectV1::decode(commit_object_id, &bytes)?;
            let members = load_commit_members(read, &commit).await?;
            let member = members
                .get(ordinal as usize)
                .ok_or_else(|| corruption("ChangeCatalog owner ordinal is absent"))?;
            if member.change_id() != id || member.source().is_some() {
                return Err(corruption("ChangeCatalog owner/ordinal back-edge is invalid").into());
            }
            semantic_change_for_member(read, member).await?
        }
        ChangeCatalogOwner::BranchRef {
            ref_change_object_id,
            branch_id,
        } => {
            let bytes = super::view::load_object_bytes(read, ref_change_object_id).await?;
            let change = ChangeObjectV1::decode(ref_change_object_id, &bytes)?;
            let ChangeObjectV1::BranchRef {
                branch_id: object_branch,
                ..
            } = &change
            else {
                return Err(corruption("branch-ref catalog owner names semantic payload").into());
            };
            if branch_id != *object_branch {
                return Err(corruption("ChangeCatalog branch owner/back-edge is invalid").into());
            }
            validate_retained_ref_change(read, change_catalog_root, ref_change_object_id, &change)
                .await?;
            change
        }
        ChangeCatalogOwner::PackedCommit { .. } => {
            return Err(corruption("packed commit marker is not a semantic ChangeId").into());
        }
    };
    decode_semantic_change_record(id, change)
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

/// Loads one authenticated Commit envelope for checkpoint chronology without
/// materializing its Change-member closure. The CommitCatalog back-edge is the
/// sole public-ID authority; the chronology cursor and timestamp live in the
/// same immutable object.
async fn load_checkpoint_commit_envelope_raw<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    commit_object_id: ObjectId,
) -> Result<(CommitObjectV1, crate::changelog::CommitRecord), crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let bytes = super::view::load_object_bytes(read, commit_object_id).await?;
    let commit = CommitObjectV1::decode(commit_object_id, &bytes)?;
    validate_commit_catalog_identity(read, commit_catalog_root, commit_object_id, &commit).await?;
    let record = crate::changelog::decode_forktree_commit_payload(&commit.metadata)?;
    let expected_commit_id =
        crate::changelog::CommitId::new(uuid::Uuid::from_bytes(*commit.commit_id.as_bytes()));
    if record.commit_id != expected_commit_id
        || record.generation != commit.generation
        || record.parent_commit_ids.len() != commit.parent_commit_object_ids.len()
    {
        return Err(corruption(
            "checkpoint chronology Commit payload disagrees with its authenticated envelope",
        )
        .into());
    }
    Ok((commit, record))
}

/// Loads and re-derives one checkpoint cursor from the authenticated first
/// parent and this commit's complete member closure. Publication performs the
/// same derivation before commit; repeating it on a cold read prevents a
/// coherently encoded but unrelated checkpoint/root edge from becoming a
/// serving authority.
pub(super) async fn load_checkpoint_commit_envelope<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    commit_object_id: ObjectId,
) -> Result<(CommitObjectV1, crate::changelog::CommitRecord), crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let (commit, record) =
        load_checkpoint_commit_envelope_raw(read, commit_catalog_root, commit_object_id).await?;
    let Some(first_parent_object_id) = commit.parent_commit_object_ids.first().copied() else {
        return Ok((commit, record));
    };
    let (first_parent, first_parent_record) =
        load_checkpoint_commit_envelope_raw(read, commit_catalog_root, first_parent_object_id)
            .await?;
    if record.parent_commit_ids.first().copied() != Some(first_parent_record.commit_id) {
        return Err(corruption(
            "checkpoint chronology first-parent object disagrees with Commit payload",
        )
        .into());
    }
    let members = load_commit_members(read, &commit).await?;
    let owner_branch_id = commit
        .checkpoint_cursor
        .owner_branch_id()
        .ok_or_else(|| corruption("non-root checkpoint cursor has no branch owner"))?;
    let expected = super::model::CheckpointCursorV1::after_first_parent(
        first_parent_object_id,
        &first_parent,
        owner_branch_id,
        super::publication::introduced_checkpoint_marker(&members, owner_branch_id)?,
    )?;
    if commit.checkpoint_cursor != expected {
        return Err(corruption(
            "checkpoint chronology cursor does not derive from its authenticated first parent and marker",
        )
        .into());
    }
    Ok((commit, record))
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
            let introduction_member = introduction_members
                .get(ordinal as usize)
                .ok_or_else(|| corruption("ChangeCatalog introduction ordinal is absent"))?;
            if introduction_member.change_id() != member.change_id()
                || introduction_member.source().is_some()
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
        ChangeCatalogOwner::PackedCommit { .. } => {
            return Err(corruption("semantic member resolved to a packed marker"));
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
                .map(CommitMemberV1::change_id)
                != Some(member.change_id())
            {
                return Err(corruption(
                    "selected membership source commit/ordinal back-edge is invalid",
                ));
            }
        }
    }
    Ok(())
}

type AuthenticatedCommitMemberClosure = (CommitObjectV1, Arc<[CommitMemberV1]>);

async fn load_authenticated_commit_member_closure<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    commit_object_id: ObjectId,
    closures: &mut BTreeMap<ObjectId, AuthenticatedCommitMemberClosure>,
) -> Result<AuthenticatedCommitMemberClosure, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if let Some(closure) = closures.get(&commit_object_id) {
        return Ok(closure.clone());
    }
    let bytes = super::view::load_object_bytes(read, commit_object_id).await?;
    let commit = CommitObjectV1::decode(commit_object_id, &bytes)?;
    validate_commit_catalog_identity(read, commit_catalog_root, commit_object_id, &commit).await?;
    let members: Arc<[CommitMemberV1]> = load_commit_members(read, &commit).await?.into();
    validate_commit_topology(read, commit_catalog_root, commit.commit_id, &commit)
        .await
        .map_err(|error| corruption(error.to_string()))?;
    let closure = (commit, members);
    closures.insert(commit_object_id, closure.clone());
    Ok(closure)
}

async fn semantic_change_for_member_with_commit_cache<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    member: &CommitMemberV1,
    closures: &mut BTreeMap<ObjectId, AuthenticatedCommitMemberClosure>,
) -> Result<ChangeObjectV1, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let expected_change_id = member.change_id();
    let mut current = member.clone();
    let mut visited = BTreeSet::new();
    let mut selected_created_at = None;
    let resolved = loop {
        match current {
            CommitMemberV1::Introduced {
                change_id,
                payload,
                global,
                updated_at,
                blob_manifest_object_ids,
            } => {
                if change_id != expected_change_id {
                    return Err(corruption(
                        "selected member source changes its authenticated ChangeId",
                    ));
                }
                break ResolvedSemanticMember {
                    change_id,
                    payload,
                    global,
                    updated_at,
                    selected_created_at,
                    blob_manifest_object_ids,
                };
            }
            CommitMemberV1::Selected {
                change_id,
                source_commit_object_id,
                source_ordinal,
                created_at,
            } => {
                if change_id != expected_change_id
                    || !visited.insert((source_commit_object_id, source_ordinal))
                {
                    return Err(corruption(
                        "selected member source is cyclic or changes its ChangeId",
                    ));
                }
                selected_created_at.get_or_insert(created_at);
                let (_, source_members) = load_authenticated_commit_member_closure(
                    read,
                    commit_catalog_root,
                    source_commit_object_id,
                    closures,
                )
                .await?;
                current = source_members
                    .get(source_ordinal as usize)
                    .cloned()
                    .ok_or_else(|| corruption("selected member source ordinal is absent"))?;
            }
        }
    };
    let payload = if let Some(created_at) = resolved.selected_created_at {
        let public_change_id =
            crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(*resolved.change_id.as_bytes()));
        let mut record =
            crate::changelog::decode_forktree_change_payload(&resolved.payload, public_change_id)
                .map_err(|error| corruption(error.to_string()))?;
        record.created_at = created_at;
        crate::changelog::encode_forktree_change_payload(&record)
            .map_err(|error| corruption(error.to_string()))?
    } else {
        resolved.payload
    };
    Ok(ChangeObjectV1::Semantic {
        change_id: resolved.change_id,
        payload,
        json_payload_object_ids: Vec::new(),
    })
}

async fn resolve_semantic_member_with_authenticated_cache<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    change_catalog_root: ObjectId,
    member: &CommitMemberV1,
    binding: HistoricalMemberBinding,
    closures: &mut BTreeMap<ObjectId, AuthenticatedCommitMemberClosure>,
) -> Result<ResolvedSemanticMember, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let expected_change_id = member.change_id();
    let mut current = member.clone();
    let mut current_commit_object_id = binding.target_commit_object_id;
    let mut current_generation = binding.target_generation;
    let mut current_ordinal = binding.target_ordinal;
    let mut visited = BTreeSet::new();
    let mut selected_created_at = None;
    loop {
        if current.change_id() != expected_change_id
            || !visited.insert((current_commit_object_id, current_ordinal))
        {
            return Err(corruption(
                "selected member source is cyclic or changes its ChangeId",
            ));
        }
        let catalog_value = lookup_on_read(
            change_catalog_root,
            "change",
            current.change_id().as_bytes(),
            read,
        )
        .await?;
        let catalog_entry = match catalog_value {
            Some(value) => ChangeCatalogEntry::decode(&value)?,
            None => packed_change_catalog_entry(read, change_catalog_root, current.change_id())
                .await?
                .ok_or_else(|| corruption("historical member has no ChangeCatalog owner"))?,
        };
        validate_member_catalog_owner_with_commit_cache(
            read,
            commit_catalog_root,
            current_commit_object_id,
            current_generation,
            current_ordinal,
            &current,
            catalog_entry,
            closures,
        )
        .await?;
        match current {
            CommitMemberV1::Introduced {
                change_id,
                payload,
                global,
                updated_at,
                blob_manifest_object_ids,
            } => {
                return Ok(ResolvedSemanticMember {
                    change_id,
                    payload,
                    global,
                    updated_at,
                    selected_created_at,
                    blob_manifest_object_ids,
                });
            }
            CommitMemberV1::Selected {
                source_commit_object_id,
                source_ordinal,
                created_at,
                ..
            } => {
                selected_created_at.get_or_insert(created_at);
                let (source_commit, source_members) = load_authenticated_commit_member_closure(
                    read,
                    commit_catalog_root,
                    source_commit_object_id,
                    closures,
                )
                .await?;
                current_commit_object_id = source_commit_object_id;
                current_generation = source_commit.generation;
                current_ordinal = source_ordinal as usize;
                current = source_members
                    .get(current_ordinal)
                    .cloned()
                    .ok_or_else(|| corruption("selected member source ordinal is absent"))?;
            }
        }
    }
}

async fn validate_member_catalog_owner_with_commit_cache<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    target_commit_object_id: ObjectId,
    target_generation: u64,
    target_ordinal: usize,
    member: &CommitMemberV1,
    entry: ChangeCatalogEntry,
    closures: &mut BTreeMap<ObjectId, AuthenticatedCommitMemberClosure>,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let canonical_owner = match entry.owner {
        ChangeCatalogOwner::CommitMember {
            commit_object_id,
            ordinal,
        } => {
            let (_, introduction_members) = load_authenticated_commit_member_closure(
                read,
                commit_catalog_root,
                commit_object_id,
                closures,
            )
            .await?;
            let introduction_member = introduction_members
                .get(ordinal as usize)
                .ok_or_else(|| corruption("ChangeCatalog introduction ordinal is absent"))?;
            if introduction_member.change_id() != member.change_id()
                || introduction_member.source().is_some()
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
        ChangeCatalogOwner::PackedCommit { .. } => {
            return Err(corruption("semantic member resolved to a packed marker"));
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
            let (source_commit, source_members) = load_authenticated_commit_member_closure(
                read,
                commit_catalog_root,
                source_commit_object_id,
                closures,
            )
            .await?;
            if source_commit.generation >= target_generation {
                return Err(corruption(
                    "selected membership source generation is not earlier than its target",
                ));
            }
            if source_members
                .get(source_ordinal as usize)
                .map(CommitMemberV1::change_id)
                != Some(member.change_id())
            {
                return Err(corruption(
                    "selected membership source commit/ordinal back-edge is invalid",
                ));
            }
        }
    }
    Ok(())
}

pub(super) struct StaleMemberAuthCache {
    binding_id: u64,
    commits: BTreeMap<ObjectId, CommitObjectV1>,
    members: BTreeMap<(ObjectId, Vec<u8>, Vec<u8>), (CommitMemberV1, u32)>,
    pages: BTreeMap<(ObjectId, ObjectId), super::model::CommitChangePageV2>,
    change_catalog_entries: BTreeMap<ChangeId, ChangeCatalogEntry>,
}

impl StaleMemberAuthCache {
    pub(super) fn new(binding_id: u64) -> Self {
        Self {
            binding_id,
            commits: BTreeMap::new(),
            members: BTreeMap::new(),
            pages: BTreeMap::new(),
            change_catalog_entries: BTreeMap::new(),
        }
    }

    fn assert_binding(&self, binding_id: u64) -> Result<(), StorageError> {
        if self.binding_id != binding_id {
            return Err(corruption(
                "stale member cache is bound to a different retained read",
            ));
        }
        Ok(())
    }
}

/// Authenticated stale summaries are produced only by the retained-view
/// loader.  The cache has no public map surface, so a caller cannot seed a
/// summary or token that bypasses endpoint validation.
pub(super) struct StaleCommitSummaryCache {
    binding_id: u64,
    entries: BTreeMap<crate::changelog::CommitId, StaleCommitSummary>,
}

impl StaleCommitSummaryCache {
    pub(super) fn new(binding_id: u64) -> Self {
        Self {
            binding_id,
            entries: BTreeMap::new(),
        }
    }

    fn assert_binding(&self, binding_id: u64) -> Result<(), StorageError> {
        if self.binding_id != binding_id {
            return Err(corruption(
                "stale summary cache is bound to a different retained read",
            ));
        }
        Ok(())
    }
}

async fn load_stale_page_for_position<R>(
    read: &R,
    binding_id: u64,
    commit_object_id: ObjectId,
    commit_id: CommitId,
    page_object_id: ObjectId,
    cache: &mut StaleMemberAuthCache,
) -> Result<super::model::CommitChangePageV2, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let key = (commit_object_id, page_object_id);
    if let Some(page) = cache.pages.get(&key) {
        return Ok(page.clone());
    }
    let bytes = super::view::load_object_bytes(read, page_object_id).await?;
    let page = super::model::CommitChangePageV2::decode(page_object_id, &bytes)?;
    if page.commit_id != commit_id {
        return Err(corruption(
            "commit change page belongs to a different Commit",
        ));
    }
    cache.pages.insert(key, page.clone());
    Ok(page)
}

/// Proves the selected page's position in the commit's authenticated ordered
/// page vector without loading the unrelated member closure.  The selected
/// page and its immediate neighbors establish ordinal adjacency; duplicate
/// vector entries are rejected globally because they make the vector
/// ambiguous even when the duplicate is not selected by this request.
pub(super) async fn validate_stale_page_position<R>(
    read: &R,
    binding_id: u64,
    commit_object_id: ObjectId,
    commit_id: CommitId,
    page_object_ids: &[ObjectId],
    page_object_id: ObjectId,
    page: &super::model::CommitChangePageV2,
    cache: &mut StaleMemberAuthCache,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    cache.assert_binding(binding_id)?;
    let mut seen = BTreeSet::new();
    for id in page_object_ids {
        if !seen.insert(*id) {
            return Err(corruption("commit change-page vector repeats an object"));
        }
    }
    let index = page_object_ids
        .iter()
        .position(|id| *id == page_object_id)
        .ok_or_else(|| corruption("selected page is absent from the Commit page vector"))?;
    if page.commit_id != commit_id {
        return Err(corruption("selected page belongs to a different Commit"));
    }
    let mut expected_start = 0_u32;
    for prefix_page_object_id in page_object_ids.iter().take(index + 1) {
        let prefix_page = load_stale_page_for_position(
            read,
            binding_id,
            commit_object_id,
            commit_id,
            *prefix_page_object_id,
            cache,
        )
        .await?;
        if prefix_page.start_ordinal != expected_start {
            return Err(corruption(
                "commit change-page vector has a gap or overlap in the selected prefix",
            ));
        }
        expected_start = prefix_page
            .start_ordinal
            .checked_add(
                u32::try_from(prefix_page.members.len())
                    .map_err(|_| corruption("commit member page count overflows u32"))?,
            )
            .ok_or_else(|| corruption("commit member page ordinal overflows u32"))?;
    }
    if let Some(next_id) = page_object_ids.get(index + 1) {
        let next = load_stale_page_for_position(
            read,
            binding_id,
            commit_object_id,
            commit_id,
            *next_id,
            cache,
        )
        .await?;
        let expected = page
            .start_ordinal
            .checked_add(
                u32::try_from(page.members.len())
                    .map_err(|_| corruption("selected page member count overflows u32"))?,
            )
            .ok_or_else(|| corruption("selected page ordinal overflows u32"))?;
        if next.start_ordinal != expected {
            return Err(corruption(
                "commit change-page vector has a gap or overlap after selected page",
            ));
        }
    }
    Ok(())
}

async fn load_stale_commit<R>(
    read: &R,
    binding_id: u64,
    commit_catalog_root: ObjectId,
    commit_object_id: ObjectId,
    cache: &mut StaleMemberAuthCache,
) -> Result<CommitObjectV1, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    cache.assert_binding(binding_id)?;
    if let Some(commit) = cache.commits.get(&commit_object_id) {
        return Ok(commit.clone());
    }
    let bytes = super::view::load_object_bytes(read, commit_object_id).await?;
    let commit = CommitObjectV1::decode(commit_object_id, &bytes)?;
    validate_commit_catalog_identity(read, commit_catalog_root, commit_object_id, &commit).await?;
    validate_commit_topology(read, commit_catalog_root, commit.commit_id, &commit)
        .await
        .map_err(|error| corruption(error.to_string()))?;
    validate_root_on_read(commit.global_state_root, "state", read).await?;
    validate_root_on_read(commit.local_state_root, "state", read).await?;
    cache.commits.insert(commit_object_id, commit.clone());
    Ok(commit)
}

/// Resolves one authenticated member through a commit's visible state root.
/// The commit envelope/catalog/topology bind both roots, the exact state key
/// selects the authenticated page, and that page binds ordinal and ChangeId.
/// No complete historical member closure is loaded.
async fn load_stale_member_at_key<R>(
    read: &R,
    binding_id: u64,
    commit_catalog_root: ObjectId,
    commit_object_id: ObjectId,
    state_key: &[u8],
    expected_change_id: ChangeId,
    cache: &mut StaleMemberAuthCache,
) -> Result<(CommitObjectV1, CommitMemberV1, u32), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    cache.assert_binding(binding_id)?;
    let cache_key = (
        commit_object_id,
        state_key.to_vec(),
        expected_change_id.as_bytes().to_vec(),
    );
    let commit = load_stale_commit(
        read,
        binding_id,
        commit_catalog_root,
        commit_object_id,
        cache,
    )
    .await?;
    if let Some(member) = cache.members.get(&cache_key) {
        return Ok((commit, member.0.clone(), member.1));
    }
    for (root, expected_global) in [
        (commit.local_state_root, false),
        (commit.global_state_root, true),
    ] {
        let Some(encoded) = lookup_on_read(root, "state", state_key, read).await? else {
            continue;
        };
        let source = if expected_global {
            StateSource::Global
        } else {
            StateSource::Branch
        };
        let pack_row = load_current_pack_rows(read, &[Some((state_key.to_vec(), encoded, source))])
            .await?
            .pop()
            .flatten()
            .map(|(row, _)| row)
            .ok_or_else(|| corruption("authenticated source state pack row is absent"))?;
        let page_bytes =
            super::view::load_object_bytes(read, pack_row.history_page_object_id).await?;
        let page =
            super::model::CommitChangePageV2::decode(pack_row.history_page_object_id, &page_bytes)?;
        if page.commit_id != commit.commit_id
            || !commit
                .member_page_object_ids
                .contains(&pack_row.history_page_object_id)
        {
            return Err(corruption(
                "authenticated source state page is not owned by its Commit",
            ));
        }
        cache.pages.insert(
            (commit_object_id, pack_row.history_page_object_id),
            page.clone(),
        );
        validate_stale_page_position(
            read,
            binding_id,
            commit_object_id,
            commit.commit_id,
            &commit.member_page_object_ids,
            pack_row.history_page_object_id,
            &page,
            cache,
        )
        .await?;
        let page_ordinal = usize::try_from(pack_row.history_page_ordinal)
            .map_err(|_| corruption("authenticated source page ordinal overflows usize"))?;
        let member = page
            .members
            .get(page_ordinal)
            .cloned()
            .ok_or_else(|| corruption("authenticated source state page ordinal is absent"))?;
        if member.change_id() != expected_change_id {
            return Err(corruption(
                "authenticated source state key resolves to a different ChangeId",
            ));
        }
        if let CommitMemberV1::Introduced { global, .. } = &member
            && *global != expected_global
        {
            return Err(corruption(
                "authenticated source state scope differs from its member",
            ));
        }
        let member_ordinal = page
            .start_ordinal
            .checked_add(pack_row.history_page_ordinal)
            .ok_or_else(|| corruption("authenticated source member ordinal overflows u32"))?;
        cache
            .members
            .insert(cache_key, (member.clone(), member_ordinal));
        return Ok((commit, member, member_ordinal));
    }
    Err(corruption(
        "authenticated source state key does not contain its expected ChangeId",
    ))
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StaleMemberLookup {
    commit_object_id: ObjectId,
    state_key: Vec<u8>,
    expected_change_id: ChangeId,
}

impl StaleMemberLookup {
    fn cache_key(&self) -> (ObjectId, Vec<u8>, Vec<u8>) {
        (
            self.commit_object_id,
            self.state_key.clone(),
            self.expected_change_id.as_bytes().to_vec(),
        )
    }
}

/// Resolves a breadth of source-member StateKeys with one retained read.
///
/// Requests are grouped by source Commit. Each source local/global root is
/// traversed once for the complete key batch, and every selected page prefix
/// needed to prove member position is loaded in one object batch. Results are
/// installed only in the operation-local stale cache after the Commit, root,
/// page domain, page position, ChangeId, and state scope all validate.
async fn prime_stale_member_lookups<R>(
    read: &R,
    binding_id: u64,
    commit_catalog_root: ObjectId,
    requests: &[StaleMemberLookup],
    cache: &mut StaleMemberAuthCache,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    cache.assert_binding(binding_id)?;
    let mut requests_by_commit = BTreeMap::<ObjectId, Vec<StaleMemberLookup>>::new();
    for request in requests {
        if !cache.members.contains_key(&request.cache_key()) {
            requests_by_commit
                .entry(request.commit_object_id)
                .or_default()
                .push(request.clone());
        }
    }
    if requests_by_commit.is_empty() {
        return Ok(());
    }

    struct PendingLookup {
        request: StaleMemberLookup,
        commit: CommitObjectV1,
        pack_row: ResolvedCurrentPackRow,
        expected_global: bool,
    }

    let mut pending = Vec::new();
    for (commit_object_id, commit_requests) in requests_by_commit {
        let commit = load_stale_commit(
            read,
            binding_id,
            commit_catalog_root,
            commit_object_id,
            cache,
        )
        .await?;
        let keys = commit_requests
            .iter()
            .map(|request| request.state_key.clone())
            .collect::<Vec<_>>();
        let local = lookup_many_on_read(commit.local_state_root, "state", &keys, read).await?;
        if local.len() != commit_requests.len() {
            return Err(corruption(
                "batched stale local-state lookup returned the wrong number of values",
            ));
        }
        let missing_slots = local
            .iter()
            .enumerate()
            .filter_map(|(slot, value)| value.is_none().then_some(slot))
            .collect::<Vec<_>>();
        let global_keys = missing_slots
            .iter()
            .map(|slot| keys[*slot].clone())
            .collect::<Vec<_>>();
        let global =
            lookup_many_on_read(commit.global_state_root, "state", &global_keys, read).await?;
        if global.len() != missing_slots.len() {
            return Err(corruption(
                "batched stale global-state lookup returned the wrong number of values",
            ));
        }
        let mut global_by_slot = missing_slots
            .into_iter()
            .zip(global)
            .collect::<BTreeMap<_, _>>();
        let mut selected_rows = Vec::with_capacity(commit_requests.len());
        let mut pending_requests = Vec::with_capacity(commit_requests.len());
        for (slot, (request, local)) in commit_requests.into_iter().zip(local).enumerate() {
            let (encoded, expected_global) = match local {
                Some(encoded) => (encoded, false),
                None => (
                    global_by_slot.remove(&slot).flatten().ok_or_else(|| {
                        corruption(
                            "authenticated source state key does not contain its expected ChangeId",
                        )
                    })?,
                    true,
                ),
            };
            selected_rows.push(Some((
                request.state_key.clone(),
                encoded,
                if expected_global {
                    StateSource::Global
                } else {
                    StateSource::Branch
                },
            )));
            pending_requests.push((request, expected_global));
        }
        let resolved_rows = load_current_pack_rows(read, &selected_rows).await?;
        for ((request, expected_global), resolved) in
            pending_requests.into_iter().zip(resolved_rows)
        {
            let (pack_row, _) = resolved.ok_or_else(|| {
                corruption("authenticated source current-state pack row is absent")
            })?;
            pending.push(PendingLookup {
                request,
                commit: commit.clone(),
                pack_row,
                expected_global,
            });
        }
    }

    let mut page_owners = BTreeMap::<ObjectId, (ObjectId, CommitId)>::new();
    for lookup in &pending {
        let selected_index = lookup
            .commit
            .member_page_object_ids
            .iter()
            .position(|id| *id == lookup.pack_row.history_page_object_id)
            .ok_or_else(|| {
                corruption("authenticated source state page is not owned by its Commit")
            })?;
        for page_object_id in lookup
            .commit
            .member_page_object_ids
            .iter()
            .take(selected_index.saturating_add(2))
        {
            match page_owners.entry(*page_object_id) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert((lookup.request.commit_object_id, lookup.commit.commit_id));
                }
                std::collections::btree_map::Entry::Occupied(entry)
                    if *entry.get()
                        != (lookup.request.commit_object_id, lookup.commit.commit_id) =>
                {
                    return Err(corruption(
                        "commit change page is claimed by more than one Commit",
                    ));
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }
    }
    let missing_page_ids = page_owners
        .keys()
        .filter(|page_object_id| {
            let (commit_object_id, _) = page_owners
                .get(page_object_id)
                .expect("page owner key came from the same map");
            !cache
                .pages
                .contains_key(&(*commit_object_id, **page_object_id))
        })
        .copied()
        .collect::<Vec<_>>();
    let page_bytes = super::view::load_object_map(read, missing_page_ids).await?;
    for (page_object_id, bytes) in page_bytes {
        let (commit_object_id, commit_id) = page_owners
            .get(&page_object_id)
            .copied()
            .ok_or_else(|| corruption("batched stale page has no expected owner"))?;
        let page = super::model::CommitChangePageV2::decode(page_object_id, &bytes)?;
        if page.commit_id != commit_id {
            return Err(corruption(
                "commit change page belongs to a different Commit",
            ));
        }
        cache.pages.insert((commit_object_id, page_object_id), page);
    }

    for lookup in pending {
        let page = cache
            .pages
            .get(&(
                lookup.request.commit_object_id,
                lookup.pack_row.history_page_object_id,
            ))
            .cloned()
            .ok_or_else(|| corruption("authenticated source state page is absent"))?;
        validate_stale_page_position(
            read,
            binding_id,
            lookup.request.commit_object_id,
            lookup.commit.commit_id,
            &lookup.commit.member_page_object_ids,
            lookup.pack_row.history_page_object_id,
            &page,
            cache,
        )
        .await?;
        let page_ordinal = usize::try_from(lookup.pack_row.history_page_ordinal)
            .map_err(|_| corruption("authenticated source page ordinal overflows usize"))?;
        let member = page
            .members
            .get(page_ordinal)
            .cloned()
            .ok_or_else(|| corruption("authenticated source state page ordinal is absent"))?;
        if member.change_id() != lookup.request.expected_change_id {
            return Err(corruption(
                "authenticated source state key resolves to a different ChangeId",
            ));
        }
        if let CommitMemberV1::Introduced { global, .. } = &member
            && *global != lookup.expected_global
        {
            return Err(corruption(
                "authenticated source state scope differs from its member",
            ));
        }
        let member_ordinal = page
            .start_ordinal
            .checked_add(lookup.pack_row.history_page_ordinal)
            .ok_or_else(|| corruption("authenticated source member ordinal overflows u32"))?;
        cache
            .members
            .insert(lookup.request.cache_key(), (member, member_ordinal));
    }
    Ok(())
}

/// Expands selected-member owner/source edges breadth-first, batching each
/// newly discovered proof layer. ChangeCatalog entries are authenticated by
/// the caller before this runs; no caller can seed members, roots, pages, or
/// proof results into this private cache.
async fn prime_stale_member_chains<R>(
    read: &R,
    binding_id: u64,
    commit_catalog_root: ObjectId,
    seeds: &[(CommitMemberV1, Vec<u8>)],
    cache: &mut StaleMemberAuthCache,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mut frontier = seeds.to_vec();
    let mut expanded = BTreeSet::<StaleMemberLookup>::new();
    while !frontier.is_empty() {
        let mut requests = BTreeSet::new();
        for (member, state_key) in frontier {
            let entry = cache
                .change_catalog_entries
                .get(&member.change_id())
                .ok_or_else(|| corruption("selected stale member has no ChangeCatalog owner"))?;
            let owner = match entry.owner {
                ChangeCatalogOwner::CommitMember {
                    commit_object_id, ..
                } => commit_object_id,
                ChangeCatalogOwner::BranchRef { .. } => {
                    return Err(corruption(
                        "semantic commit member resolves to a branch-ref catalog owner",
                    ));
                }
                ChangeCatalogOwner::PackedCommit { .. } => {
                    return Err(corruption(
                        "stale semantic member resolved to an unexpanded packed marker",
                    ));
                }
            };
            requests.insert(StaleMemberLookup {
                commit_object_id: owner,
                state_key: state_key.clone(),
                expected_change_id: member.change_id(),
            });
            if let Some((source_commit_object_id, _)) = member.source() {
                requests.insert(StaleMemberLookup {
                    commit_object_id: source_commit_object_id,
                    state_key,
                    expected_change_id: member.change_id(),
                });
            }
        }
        let requests = requests
            .into_iter()
            .filter(|request| expanded.insert(request.clone()))
            .collect::<Vec<_>>();
        if requests.is_empty() {
            break;
        }
        prime_stale_member_lookups(read, binding_id, commit_catalog_root, &requests, cache).await?;
        frontier = requests
            .into_iter()
            .map(|request| {
                let member = cache
                    .members
                    .get(&request.cache_key())
                    .map(|(member, _)| member.clone())
                    .ok_or_else(|| corruption("batched stale member proof is incomplete"))?;
                Ok((member, request.state_key))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
    }
    Ok(())
}
async fn validate_member_catalog_owner_with_stale_cache<R>(
    read: &R,
    binding_id: u64,
    commit_catalog_root: ObjectId,
    target_commit_object_id: ObjectId,
    target_generation: u64,
    target_ordinal: usize,
    state_key: &[u8],
    member: &CommitMemberV1,
    entry: ChangeCatalogEntry,
    cache: &mut StaleMemberAuthCache,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    cache.assert_binding(binding_id)?;
    let canonical_owner = match entry.owner {
        ChangeCatalogOwner::CommitMember {
            commit_object_id,
            ordinal,
        } => {
            let (_introduction_commit, introduction_member, introduction_ordinal) =
                load_stale_member_at_key(
                    read,
                    binding_id,
                    commit_catalog_root,
                    commit_object_id,
                    state_key,
                    member.change_id(),
                    cache,
                )
                .await?;
            if introduction_member.change_id() != member.change_id()
                || introduction_member.source().is_some()
                || introduction_ordinal != ordinal
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
        ChangeCatalogOwner::PackedCommit { .. } => {
            return Err(corruption("semantic member resolved to a packed marker"));
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
            let (source_commit, source_member, source_member_ordinal) = load_stale_member_at_key(
                read,
                binding_id,
                commit_catalog_root,
                source_commit_object_id,
                state_key,
                member.change_id(),
                cache,
            )
            .await?;
            if source_commit.generation >= target_generation {
                return Err(corruption(
                    "selected membership source generation is not earlier than its target",
                ));
            }
            if source_member.change_id() != member.change_id()
                || source_member_ordinal != source_ordinal
            {
                return Err(corruption(
                    "selected membership source commit/ordinal back-edge is invalid",
                ));
            }
        }
    }
    Ok(())
}

pub(super) async fn resolve_semantic_member_with_stale_auth<R>(
    read: &R,
    binding_id: u64,
    member: &CommitMemberV1,
    state_key: &[u8],
    target_commit_object_id: ObjectId,
    target_generation: u64,
    target_ordinal: usize,
    commit_catalog_root: ObjectId,
    change_catalog_root: ObjectId,
    cache: &mut StaleMemberAuthCache,
) -> Result<ResolvedSemanticMember, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    cache.assert_binding(binding_id)?;
    let expected_change_id = member.change_id();
    let mut current = member.clone();
    let mut owner = target_commit_object_id;
    let mut generation = target_generation;
    let mut ordinal = target_ordinal;
    let mut visited = BTreeSet::new();
    let mut selected_created_at = None;

    loop {
        if !visited.insert((owner, ordinal as u32)) {
            return Err(corruption("selected member source ownership is cyclic"));
        }
        let entry = if let Some(entry) = cache.change_catalog_entries.get(&current.change_id()) {
            entry.clone()
        } else {
            let entry = match lookup_on_read(
                change_catalog_root,
                "change",
                current.change_id().as_bytes(),
                read,
            )
            .await?
            {
                Some(value) => ChangeCatalogEntry::decode(&value)?,
                None => packed_change_catalog_entry(read, change_catalog_root, current.change_id())
                    .await?
                    .ok_or_else(|| {
                        corruption("selected stale member has no ChangeCatalog owner")
                    })?,
            };
            cache
                .change_catalog_entries
                .insert(current.change_id(), entry.clone());
            entry
        };
        validate_member_catalog_owner_with_stale_cache(
            read,
            binding_id,
            commit_catalog_root,
            owner,
            generation,
            ordinal,
            state_key,
            &current,
            entry,
            cache,
        )
        .await?;

        match current {
            CommitMemberV1::Introduced {
                change_id,
                payload,
                global,
                updated_at,
                blob_manifest_object_ids,
            } => {
                if change_id != expected_change_id {
                    return Err(corruption(
                        "selected member source changes its authenticated ChangeId",
                    ));
                }
                return Ok(ResolvedSemanticMember {
                    change_id,
                    payload,
                    global,
                    updated_at,
                    selected_created_at,
                    blob_manifest_object_ids,
                });
            }
            CommitMemberV1::Selected {
                change_id,
                source_commit_object_id,
                source_ordinal,
                created_at,
            } => {
                if change_id != expected_change_id {
                    return Err(corruption(
                        "selected member source changes its authenticated ChangeId",
                    ));
                }
                selected_created_at.get_or_insert(created_at);
                let (source_commit, source_member, source_member_ordinal) =
                    load_stale_member_at_key(
                        read,
                        binding_id,
                        commit_catalog_root,
                        source_commit_object_id,
                        state_key,
                        expected_change_id,
                        cache,
                    )
                    .await?;
                if source_member_ordinal != source_ordinal {
                    return Err(corruption(
                        "selected member source state ordinal is inconsistent",
                    ));
                }
                current = source_member;
                if current.change_id() != expected_change_id {
                    return Err(corruption(
                        "selected member source changes its authenticated ChangeId",
                    ));
                }
                owner = source_commit_object_id;
                generation = source_commit.generation;
                ordinal = source_ordinal as usize;
            }
        }
    }
}
/// One ordered-history identity selected from an already published commit.
///
/// Callers identify the semantic member and state row only. Catalog roots,
/// object addresses, member ordinals, source-state roots, and sequence-state
/// roots are derived and authenticated inside [`select_historical_commit_members`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HistoricalMemberSelection {
    source_commit_id: CommitId,
    change_id: ChangeId,
    state_key: Vec<u8>,
}

impl HistoricalMemberSelection {
    pub(crate) fn new(source_commit_id: CommitId, change_id: ChangeId, state_key: Vec<u8>) -> Self {
        Self {
            source_commit_id,
            change_id,
            state_key,
        }
    }
}

/// Authenticated resolution of one ordered historical member.
#[derive(Clone, Debug)]
pub(crate) struct SelectedHistoricalMember {
    pub(crate) member: CommitMemberV1,
    pub(crate) source_commit: CommitObjectV1,
    pub(crate) source_change: ChangeObjectV1,
    pub(crate) source_state: StateValue,
    pub(crate) source_domain: StateSource,
    pub(crate) sequence_state: Option<(StateValue, StateSource)>,
}

/// One operation-owned selected-member result. The authenticated sequence
/// parent is returned so ordered publication can reuse its roots for fresh
/// rows without reopening or accepting caller-supplied tree targets.
#[derive(Debug)]
pub(crate) struct SelectedHistoricalMemberBatch {
    sequence_parent: CommitObjectV1,
    selected: Vec<SelectedHistoricalMember>,
    proof: HistoricalMemberBatchProof,
}

impl SelectedHistoricalMemberBatch {
    pub(crate) fn sequence_parent(&self) -> &CommitObjectV1 {
        &self.sequence_parent
    }

    pub(crate) fn take_selected(&mut self) -> Vec<SelectedHistoricalMember> {
        std::mem::take(&mut self.selected)
    }

    pub(super) fn consume_proof(
        &mut self,
        view_instance_id: u64,
        target_generation: u64,
        member: &CommitMemberV1,
    ) -> Result<(), StorageError> {
        self.proof
            .consume(view_instance_id, target_generation, member)
    }

    pub(super) fn finish_proof(self, view_instance_id: u64) -> Result<(), StorageError> {
        if !self.selected.is_empty() {
            return Err(corruption(
                "ordered history publication received unconsumed selected rows",
            ));
        }
        self.proof.finish(view_instance_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct HistoricalMemberProofKey {
    source_commit_object_id: ObjectId,
    source_ordinal: u32,
    change_id: ChangeId,
}

#[derive(Clone, Copy, Debug)]
struct HistoricalMemberProofEntry {
    source_generation: u64,
    remaining: usize,
}

/// Ephemeral evidence produced only by the authenticated selected-member
/// resolver. Ordered publication consumes it on the exact originating view;
/// callers cannot construct roots, owner entries, ordinals, or generations.
#[derive(Debug)]
struct HistoricalMemberBatchProof {
    view_instance_id: u64,
    members: BTreeMap<HistoricalMemberProofKey, HistoricalMemberProofEntry>,
}

impl HistoricalMemberBatchProof {
    pub(super) fn consume(
        &mut self,
        view_instance_id: u64,
        target_generation: u64,
        member: &CommitMemberV1,
    ) -> Result<(), StorageError> {
        if self.view_instance_id != view_instance_id {
            return Err(corruption(
                "selected-member proof belongs to another retained view",
            ));
        }
        let (source_commit_object_id, source_ordinal) = member
            .source()
            .ok_or_else(|| corruption("selected-member proof received an introduced member"))?;
        let key = HistoricalMemberProofKey {
            source_commit_object_id,
            source_ordinal,
            change_id: member.change_id(),
        };
        let entry = self
            .members
            .get_mut(&key)
            .ok_or_else(|| corruption("ordered history member has no authenticated batch proof"))?;
        if entry.remaining == 0 || entry.source_generation >= target_generation {
            return Err(corruption(
                "ordered history member proof is exhausted or has an invalid generation",
            ));
        }
        entry.remaining -= 1;
        Ok(())
    }

    pub(super) fn finish(self, view_instance_id: u64) -> Result<(), StorageError> {
        if self.view_instance_id != view_instance_id
            || self.members.values().any(|entry| entry.remaining != 0)
        {
            return Err(corruption(
                "ordered history did not consume its exact authenticated member batch",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct HistoricalSourceClosure {
    object_id: ObjectId,
    commit: CommitObjectV1,
    members: Arc<[CommitMemberV1]>,
    ordinal_by_change: BTreeMap<ChangeId, usize>,
}

/// Resolves an ordered merge/checkpoint selection with one retained view.
///
/// Work is grouped by source commit. Each source CommitCatalog/object/member
/// closure is authenticated once, each closure receives one ChangeId index,
/// source-state points are batched per source root pair, and sequence-state
/// points are resolved in one batch. The returned vector preserves request
/// order and repeated requests; malformed duplicate closure membership remains
/// fail-closed in the canonical commit decoder.
pub(crate) async fn select_historical_commit_members<R>(
    view: &CoherentView<R>,
    sequence_parent_id: CommitId,
    selections: &[HistoricalMemberSelection],
) -> Result<SelectedHistoricalMemberBatch, StorageError>
where
    R: StorageAdapterRead,
{
    let sequence_parent = load_commit(view, sequence_parent_id)
        .await?
        .ok_or_else(|| corruption("ordered history sequence parent is absent"))?;
    if selections.is_empty() {
        return Ok(SelectedHistoricalMemberBatch {
            sequence_parent,
            selected: Vec::new(),
            proof: HistoricalMemberBatchProof {
                view_instance_id: view.view_instance_id(),
                members: BTreeMap::new(),
            },
        });
    }

    let unique_source_ids = selections
        .iter()
        .map(|selection| selection.source_commit_id)
        .collect::<BTreeSet<_>>();
    let mut sources = BTreeMap::<CommitId, HistoricalSourceClosure>::new();
    let mut authenticated_closures = BTreeMap::<ObjectId, AuthenticatedCommitMemberClosure>::new();
    for source_commit_id in unique_source_ids {
        let (source_object_id, source, members) = load_commit_with_members(view, source_commit_id)
            .await?
            .ok_or_else(|| corruption("selected source commit is absent from CommitCatalog"))?;
        let mut ordinal_by_change = BTreeMap::new();
        for (ordinal, member) in members.iter().enumerate() {
            if ordinal_by_change
                .insert(member.change_id(), ordinal)
                .is_some()
            {
                return Err(corruption(
                    "selected source commit repeats one authenticated ChangeId",
                ));
            }
        }
        authenticated_closures.insert(source_object_id, (source.clone(), Arc::clone(&members)));
        sources.insert(
            source_commit_id,
            HistoricalSourceClosure {
                object_id: source_object_id,
                commit: source,
                members,
                ordinal_by_change,
            },
        );
    }

    let mut resolved_members = Vec::with_capacity(selections.len());
    let mut proof_members = BTreeMap::<HistoricalMemberProofKey, HistoricalMemberProofEntry>::new();
    for selection in selections {
        let source = sources
            .get(&selection.source_commit_id)
            .ok_or_else(|| corruption("selected source commit batch is incomplete"))?;
        let source_ordinal = *source
            .ordinal_by_change
            .get(&selection.change_id)
            .ok_or_else(|| {
                corruption(
                    "selected ChangeId is absent from its authenticated source commit membership",
                )
            })?;
        let source_member = source
            .members
            .get(source_ordinal)
            .ok_or_else(|| corruption("selected source ordinal is absent"))?;
        let source_change = semantic_change_for_member_with_commit_cache(
            view.retained_read(),
            view.repository_root().commit_catalog_root,
            source_member,
            &mut authenticated_closures,
        )
        .await?;
        if source_change.change_id() != selection.change_id {
            return Err(corruption(
                "selected source Change disagrees with requested ChangeId",
            ));
        }
        let proof_key = HistoricalMemberProofKey {
            source_commit_object_id: source.object_id,
            source_ordinal: u32::try_from(source_ordinal)
                .map_err(|_| corruption("selected source ordinal exceeds u32"))?,
            change_id: selection.change_id,
        };
        match proof_members.entry(proof_key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(HistoricalMemberProofEntry {
                    source_generation: source.commit.generation,
                    remaining: 1,
                });
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if entry.get().source_generation != source.commit.generation {
                    return Err(corruption(
                        "selected-member proof repeats an owner with another generation",
                    ));
                }
                entry.get_mut().remaining = entry
                    .get()
                    .remaining
                    .checked_add(1)
                    .ok_or_else(|| corruption("selected-member proof count overflows usize"))?;
            }
        }
        resolved_members.push((
            CommitMemberV1::selected(
                selection.change_id,
                source.object_id,
                u32::try_from(source_ordinal)
                    .map_err(|_| corruption("selected source ordinal exceeds u32"))?,
                crate::changelog::decode_forktree_change_payload(
                    match &source_change {
                        ChangeObjectV1::Semantic { payload, .. } => payload,
                        ChangeObjectV1::BranchRef { .. } => {
                            return Err(corruption(
                                "selected semantic member resolved to a branch-ref change",
                            ));
                        }
                    },
                    crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(
                        *selection.change_id.as_bytes(),
                    )),
                )
                .map_err(|error| corruption(error.to_string()))?
                .created_at,
            ),
            source.commit.clone(),
            source_change,
        ));
    }

    let mut source_state = vec![None; selections.len()];
    let mut requests_by_source = BTreeMap::<CommitId, Vec<usize>>::new();
    for (slot, selection) in selections.iter().enumerate() {
        requests_by_source
            .entry(selection.source_commit_id)
            .or_default()
            .push(slot);
    }
    for (source_commit_id, slots) in requests_by_source {
        let source = sources
            .get(&source_commit_id)
            .ok_or_else(|| corruption("selected source state batch is incomplete"))?;
        let keys = slots
            .iter()
            .map(|slot| selections[*slot].state_key.clone())
            .collect::<Vec<_>>();
        let values = state_points_on_read(
            source.commit.global_state_root,
            Some(source.commit.local_state_root),
            &keys,
            true,
            view.retained_read(),
        )
        .await?;
        for (slot, value) in slots.into_iter().zip(values) {
            source_state[slot] = value;
        }
    }

    let sequence_keys = selections
        .iter()
        .map(|selection| selection.state_key.clone())
        .collect::<Vec<_>>();
    let sequence_state = state_points_on_read(
        sequence_parent.global_state_root,
        Some(sequence_parent.local_state_root),
        &sequence_keys,
        true,
        view.retained_read(),
    )
    .await?;

    let mut selected = Vec::with_capacity(selections.len());
    for (((member, source_commit, source_change), source_state), sequence_state) in resolved_members
        .into_iter()
        .zip(source_state)
        .zip(sequence_state)
    {
        let (source_state, source_domain) = source_state
            .ok_or_else(|| corruption("selected history source state row is absent"))?;
        selected.push(SelectedHistoricalMember {
            member,
            source_commit,
            source_change,
            source_state,
            source_domain,
            sequence_state,
        });
    }
    Ok(SelectedHistoricalMemberBatch {
        sequence_parent,
        selected,
        proof: HistoricalMemberBatchProof {
            view_instance_id: view.view_instance_id(),
            members: proof_members,
        },
    })
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
    let mut rows = state_points(view, &[key.to_vec()], include_tombstone).await?;
    Ok(rows.pop().flatten())
}

/// Resolves one row from exactly one authenticated state root. Unlike the
/// public overlay point helper this never consults the sibling root, so an
/// owner selected by a filesystem projection cannot silently bind to a
/// same-key row from another scope.
pub(crate) async fn state_point_at_root_on_read<R>(
    root: ObjectId,
    key: &[u8],
    global: bool,
    include_tombstone: bool,
    read: &R,
) -> Result<Option<StateValue>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let encoded = lookup_on_read(root, "state", key, read).await?;
    let selected = encoded.map(|encoded| {
        (
            key.to_vec(),
            encoded,
            if global {
                StateSource::Global
            } else {
                StateSource::Branch
            },
        )
    });
    let value = resolve_state_values_on_read(read, &[selected], None)
        .await?
        .pop()
        .flatten()
        .map(|(value, _)| value);
    match value {
        None => Ok(None),
        Some(value) if value.cell.deleted() && !include_tombstone => Ok(None),
        Some(value) => Ok(Some(value)),
    }
}

/// Resolves an exact batch through the canonical ordered-tree index on one
/// retained view. Each internal node and requested leaf is decoded at most
/// once per root, and result slots preserve caller order and duplicates.
pub(crate) async fn state_points<R>(
    view: &CoherentView<R>,
    keys: &[Vec<u8>],
    include_tombstone: bool,
) -> Result<Vec<Option<VisibleStateRow>>, StorageError>
where
    R: StorageAdapterRead,
{
    let (global_root, local_root) = current_state_roots(view);
    let values = state_points_on_read(
        global_root,
        local_root,
        keys,
        include_tombstone,
        view.retained_read(),
    )
    .await?;
    Ok(keys
        .iter()
        .zip(values)
        .map(|(key, value)| {
            value.map(|(value, source)| VisibleStateRow {
                encoded_key: key.clone(),
                value,
                source,
                view_instance_id: view.view_instance_id(),
            })
        })
        .collect())
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
    let mut rows = state_points_on_read(
        global_state_root,
        Some(local_state_root),
        &[key.to_vec()],
        include_tombstone,
        read,
    )
    .await?;
    Ok(rows.pop().flatten())
}

pub(crate) async fn state_points_on_read<R>(
    global_state_root: ObjectId,
    local_state_root: Option<ObjectId>,
    keys: &[Vec<u8>],
    include_tombstone: bool,
    read: &R,
) -> Result<Vec<Option<(StateValue, StateSource)>>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    state_points_on_read_impl(
        global_state_root,
        local_state_root,
        keys,
        include_tombstone,
        None,
        read,
    )
    .await
}

/// Resolves historical working-diff points with the catalog roots of the
/// selected commits. Unlike the general current-state point helper, this
/// path authenticates each selected state-page member against its exact
/// CommitCatalog/ChangeCatalog owner before exposing the row.
pub(crate) async fn state_points_on_read_with_historical_auth<R>(
    global_state_root: ObjectId,
    local_state_root: Option<ObjectId>,
    keys: &[Vec<u8>],
    include_tombstone: bool,
    commit_catalog_root: ObjectId,
    change_catalog_root: ObjectId,
    endpoint_commit_object_id: ObjectId,
    read: &R,
) -> Result<Vec<Option<(StateValue, StateSource)>>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    state_points_on_read_impl(
        global_state_root,
        local_state_root,
        keys,
        include_tombstone,
        Some(HistoricalStateAuth {
            endpoint_global_state_root: global_state_root,
            endpoint_local_state_root: local_state_root,
            commit_catalog_root,
            change_catalog_root,
            endpoint_commit_object_id,
        }),
        read,
    )
    .await
}

#[derive(Clone, Copy)]
struct HistoricalStateAuth {
    endpoint_global_state_root: ObjectId,
    endpoint_local_state_root: Option<ObjectId>,
    commit_catalog_root: ObjectId,
    change_catalog_root: ObjectId,
    endpoint_commit_object_id: ObjectId,
}

async fn state_points_on_read_impl<R>(
    global_state_root: ObjectId,
    local_state_root: Option<ObjectId>,
    keys: &[Vec<u8>],
    include_tombstone: bool,
    historical_auth: Option<HistoricalStateAuth>,
    read: &R,
) -> Result<Vec<Option<(StateValue, StateSource)>>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let local_encoded = match local_state_root {
        Some(root) => lookup_many_on_read(root, "state", keys, read).await?,
        None => vec![None; keys.len()],
    };
    let mut missing_by_key = BTreeMap::<Vec<u8>, Vec<usize>>::new();
    for (slot, encoded) in local_encoded.iter().enumerate() {
        if encoded.is_none() {
            missing_by_key
                .entry(keys[slot].clone())
                .or_default()
                .push(slot);
        }
    }
    let global_keys = missing_by_key.keys().cloned().collect::<Vec<_>>();
    let global = lookup_many_on_read(global_state_root, "state", &global_keys, read).await?;
    let mut global_by_slot = BTreeMap::new();
    for ((_, slots), value) in missing_by_key.into_iter().zip(global) {
        for slot in slots {
            global_by_slot.insert(slot, value.clone());
        }
    }

    let selected = local_encoded
        .into_iter()
        .enumerate()
        .map(|(slot, local)| {
            local
                .map(|encoded| (keys[slot].clone(), encoded, StateSource::Branch))
                .or_else(|| {
                    global_by_slot
                        .remove(&slot)
                        .flatten()
                        .map(|encoded| (keys[slot].clone(), encoded, StateSource::Global))
                })
        })
        .collect::<Vec<_>>();
    let resolved = resolve_state_values_on_read(read, &selected, historical_auth).await?;
    resolved
        .into_iter()
        .map(|value| match value {
            None => Ok(None),
            Some((value, StateSource::Global)) if matches!(value.cell, StateCell::Tombstone) => {
                Err(corruption("global state tree contains a tombstone"))
            }
            Some((value, _)) if value.cell.deleted() && !include_tombstone => Ok(None),
            value => Ok(value),
        })
        .collect()
}

/// Resolves the changed leaves used by stale reconciliation with the
/// endpoint commit's authenticated catalog identity.  This deliberately
/// shares the ordinary state-tree point selection, but the selected semantic
/// member chain is checked against CommitCatalog and ChangeCatalog at every
/// owner edge before its payload is exposed.
pub(super) async fn state_points_on_read_for_stale<R>(
    repository: &RepositoryRootV1,
    summary: StaleCommitSummary,
    keys: &[Vec<u8>],
    include_tombstone: bool,
    binding_id: u64,
    read: &R,
) -> Result<Vec<Option<(StateValue, StateSource)>>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let local_encoded = lookup_many_on_read(summary.local_state_root, "state", keys, read).await?;
    let mut missing_by_key = BTreeMap::<Vec<u8>, Vec<usize>>::new();
    for (slot, encoded) in local_encoded.iter().enumerate() {
        if encoded.is_none() {
            missing_by_key
                .entry(keys[slot].clone())
                .or_default()
                .push(slot);
        }
    }
    let global_keys = missing_by_key.keys().cloned().collect::<Vec<_>>();
    let global =
        lookup_many_on_read(summary.global_state_root, "state", &global_keys, read).await?;
    let mut global_by_slot = BTreeMap::new();
    for ((_, slots), value) in missing_by_key.into_iter().zip(global) {
        for slot in slots {
            global_by_slot.insert(slot, value.clone());
        }
    }
    let selected = local_encoded
        .into_iter()
        .enumerate()
        .map(|(slot, local)| {
            local
                .map(|encoded| (keys[slot].clone(), encoded, StateSource::Branch))
                .or_else(|| {
                    global_by_slot
                        .remove(&slot)
                        .flatten()
                        .map(|encoded| (keys[slot].clone(), encoded, StateSource::Global))
                })
        })
        .collect::<Vec<_>>();

    let pack_rows = load_current_pack_rows(read, &selected).await?;
    let mut page_ids = BTreeSet::new();
    for row in pack_rows.iter().flatten() {
        page_ids.insert(row.0.history_page_object_id);
    }
    let pages = super::view::load_object_map(read, page_ids).await?;
    let mut decoded_pages = BTreeMap::new();
    for (id, bytes) in pages {
        decoded_pages.insert(id, super::model::CommitChangePageV2::decode(id, &bytes)?);
    }

    let mut summary_cache = StaleCommitSummaryCache::new(binding_id);
    let mut stale_member_cache = StaleMemberAuthCache::new(binding_id);
    let change_ids = selected
        .iter()
        .zip(&pack_rows)
        .filter_map(|(row, pack_row)| {
            let (Some(_), Some((pack_row, _))) = (row, pack_row) else {
                return None;
            };
            let page = decoded_pages.get(&pack_row.history_page_object_id)?;
            page.members
                .get(pack_row.history_page_ordinal as usize)
                .map(CommitMemberV1::change_id)
        })
        .collect::<BTreeSet<_>>();
    let change_ids = change_ids.into_iter().collect::<Vec<_>>();
    let change_entries =
        lookup_change_catalog_entries(read, repository.change_catalog_root, &change_ids).await?;
    if change_entries.len() != change_ids.len() {
        return Err(corruption(
            "batched stale ChangeCatalog lookup returned the wrong number of values",
        ));
    }
    for (change_id, value) in change_ids.into_iter().zip(change_entries) {
        let value =
            value.ok_or_else(|| corruption("selected stale member has no ChangeCatalog owner"))?;
        stale_member_cache
            .change_catalog_entries
            .insert(change_id, value);
    }

    // Validate the endpoint page ownership before following any source edge,
    // then expand every canonical-owner/source proof as operation-local
    // batches. The ordinary resolver below remains the final semantic
    // authority, but all of its source StateKey and page-prefix reads are now
    // satisfied by this retained-read cache rather than repeated per row.
    let mut proof_seeds = Vec::new();
    for (row, pack_row) in selected.iter().zip(&pack_rows) {
        let (Some((encoded_key, _, _)), Some((pack_row, _))) = (row, pack_row) else {
            continue;
        };
        let page = decoded_pages
            .get(&pack_row.history_page_object_id)
            .ok_or_else(|| corruption("state value page is absent"))?;
        let page_changelog_id =
            crate::changelog::CommitId::new(uuid::Uuid::from_bytes(*page.commit_id.as_bytes()));
        let page_summary = load_historical_commit_state_roots_for_stale(
            read,
            binding_id,
            repository,
            page_changelog_id,
            &mut summary_cache,
        )
        .await
        .map_err(|error| corruption(error.to_string()))?;
        if !page_summary
            .member_page_object_ids
            .contains(&pack_row.history_page_object_id)
        {
            return Err(corruption(
                "stale state value page is not owned by its authenticated commit",
            ));
        }
        validate_stale_page_position(
            read,
            binding_id,
            page_summary.commit_object_id,
            page_summary.commit_id,
            &page_summary.member_page_object_ids,
            pack_row.history_page_object_id,
            page,
            &mut stale_member_cache,
        )
        .await?;
        let member = page
            .members
            .get(pack_row.history_page_ordinal as usize)
            .cloned()
            .ok_or_else(|| corruption("state value page ordinal is absent"))?;
        proof_seeds.push((member, encoded_key.clone()));
    }
    prime_stale_member_chains(
        read,
        binding_id,
        repository.commit_catalog_root,
        &proof_seeds,
        &mut stale_member_cache,
    )
    .await?;

    let mut output = Vec::with_capacity(selected.len());
    for (row, pack_row) in selected.iter().zip(pack_rows) {
        let (Some((encoded_key, _, source)), Some((pack_row, pack_source))) = (row, pack_row)
        else {
            output.push(None);
            continue;
        };
        if *source != pack_source {
            return Err(corruption(
                "current-state pack source slot changed during stale resolution",
            ));
        }
        let page = decoded_pages
            .get(&pack_row.history_page_object_id)
            .ok_or_else(|| corruption("state value page is absent"))?;
        let page_commit_id = page.commit_id;
        let page_changelog_id =
            crate::changelog::CommitId::new(uuid::Uuid::from_bytes(*page_commit_id.as_bytes()));
        let page_summary = load_historical_commit_state_roots_for_stale(
            read,
            binding_id,
            repository,
            page_changelog_id,
            &mut summary_cache,
        )
        .await
        .map_err(|error| corruption(error.to_string()))?;
        if !page_summary
            .member_page_object_ids
            .contains(&pack_row.history_page_object_id)
        {
            return Err(corruption(
                "stale state value page is not owned by its authenticated commit",
            ));
        }
        validate_stale_page_position(
            read,
            binding_id,
            page_summary.commit_object_id,
            page_summary.commit_id,
            &page_summary.member_page_object_ids,
            pack_row.history_page_object_id,
            page,
            &mut stale_member_cache,
        )
        .await?;
        let page_ordinal = pack_row.history_page_ordinal as usize;
        let member = page
            .members
            .get(page_ordinal)
            .ok_or_else(|| corruption("state value page ordinal is absent"))?;
        let target_ordinal = usize::try_from(page.start_ordinal)
            .ok()
            .and_then(|start| start.checked_add(page_ordinal))
            .ok_or_else(|| corruption("stale state member ordinal overflows usize"))?;
        let resolved = resolve_semantic_member_with_stale_auth(
            read,
            binding_id,
            member,
            encoded_key,
            page_summary.commit_object_id,
            page_summary.generation,
            target_ordinal,
            repository.commit_catalog_root,
            repository.change_catalog_root,
            &mut stale_member_cache,
        )
        .await?;
        let expected_global = *source == StateSource::Global;
        if resolved.global != expected_global {
            return Err(corruption(
                "state value page domain differs from its state root",
            ));
        }
        let public_change_id =
            crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(*resolved.change_id.as_bytes()));
        let record =
            crate::changelog::decode_forktree_change_payload(&resolved.payload, public_change_id)
                .map_err(|error| corruption(error.to_string()))?;
        let created_at = resolved.selected_created_at.unwrap_or(record.created_at);
        if encode_state_key(super::state::StateKeyRef {
            schema_key: &record.schema_key,
            file_id: record.file_id.as_deref(),
            entity_pk: &record.entity_pk,
        }) != *encoded_key
        {
            return Err(corruption(
                "state value page identity differs from its state key",
            ));
        }
        let cell = authenticated_current_cell_for_history(
            &record.snapshot,
            &pack_row.value.cell,
        )?;
        let logical_cell = logical_history_cell(&record.snapshot)?;
        let metadata = match record.metadata {
            crate::json_store::JsonSlot::None => None,
            crate::json_store::JsonSlot::Inline(value) => Some(value),
            crate::json_store::JsonSlot::ForkTreeObject(_) => {
                return Err(corruption("state value page contains out-of-page metadata"));
            }
        };
        let historical_value = StateValue {
            change_id: public_change_id,
            commit_id: crate::changelog::CommitId::new(uuid::Uuid::from_bytes(
                *page.commit_id.as_bytes(),
            )),
            created_at,
            updated_at: resolved.updated_at,
            cell,
            metadata: metadata.map(Into::into),
            origin_key: record.origin_key,
            blob_manifest_object_ids: resolved.blob_manifest_object_ids,
        };
        if historical_value != pack_row.value {
            return Err(corruption(
                "current-state pack value differs from its stale-authenticated history member",
            ));
        }
        let mut output_value = historical_value;
        output_value.cell = logical_cell;
        output.push(Some((output_value, *source)));
    }

    output
        .into_iter()
        .map(|value| match value {
            None => Ok(None),
            Some((value, StateSource::Global)) if matches!(value.cell, StateCell::Tombstone) => {
                Err(corruption("global state tree contains a tombstone"))
            }
            Some((value, _)) if value.cell.deleted() && !include_tombstone => Ok(None),
            value => Ok(value),
        })
        .collect()
}

fn authenticated_current_cell_for_history(
    historical: &crate::json_store::JsonSlot,
    current: &StateCell,
) -> Result<StateCell, StorageError> {
    match (historical, current) {
        (crate::json_store::JsonSlot::None, StateCell::Tombstone) => Ok(StateCell::Tombstone),
        (crate::json_store::JsonSlot::Inline(value), StateCell::Null)
            if value.as_ref() == "null" =>
        {
            Ok(StateCell::Null)
        }
        (crate::json_store::JsonSlot::Inline(value), StateCell::NativeRow(native)) => {
            let digest = crate::native_row::semantic_digest_text(value)
                .map_err(|error| corruption(error.to_string()))?;
            if digest != native.semantic_digest {
                return Err(corruption(
                    "native current-state payload differs from its authenticated history member",
                ));
            }
            Ok(current.clone())
        }
        (crate::json_store::JsonSlot::ForkTreeObject(_), _) => Err(corruption(
            "state value page contains an out-of-page JSON reference",
        )),
        (_, StateCell::Value(_)) => Err(corruption(
            "state value page uses the removed JSON current-state representation",
        )),
        _ => Err(corruption(
            "current-state cell kind differs from its authenticated history member",
        )),
    }
}

fn logical_history_cell(
    historical: &crate::json_store::JsonSlot,
) -> Result<StateCell, StorageError> {
    match historical {
        crate::json_store::JsonSlot::None => Ok(StateCell::Tombstone),
        crate::json_store::JsonSlot::Inline(value) if value.as_ref() == "null" => {
            Ok(StateCell::Null)
        }
        crate::json_store::JsonSlot::Inline(value) => Ok(StateCell::Value(value.clone().into())),
        crate::json_store::JsonSlot::ForkTreeObject(_) => Err(corruption(
            "state value page contains an out-of-page JSON reference",
        )),
    }
}
#[derive(Clone, Debug)]
struct ResolvedCurrentPackRow {
    value: StateValue,
    history_page_object_id: ObjectId,
    history_page_ordinal: u32,
}

async fn load_current_pack_rows<R>(
    read: &R,
    selected: &[Option<(Vec<u8>, Vec<u8>, StateSource)>],
) -> Result<Vec<Option<(ResolvedCurrentPackRow, StateSource)>>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mut refs = Vec::with_capacity(selected.len());
    let mut pack_ids = BTreeSet::new();
    for row in selected {
        let value_ref = row
            .as_ref()
            .map(|(_, encoded, _)| decode_state_value_storage(encoded))
            .transpose()?;
        if let Some(value_ref) = value_ref {
            pack_ids.insert(value_ref.pack_object_id);
        }
        refs.push(value_ref);
    }
    let encoded_packs = super::view::load_object_map(read, pack_ids).await?;
    let mut packs = BTreeMap::new();
    for (id, bytes) in encoded_packs {
        packs.insert(
            id,
            super::current_pack::CurrentStatePackV1::decode(id, &bytes)?,
        );
    }

    let mut output = Vec::with_capacity(selected.len());
    for (row, value_ref) in selected.iter().zip(refs) {
        let (Some((encoded_key, _, source)), Some(value_ref)) = (row, value_ref) else {
            output.push(None);
            continue;
        };
        let pack = packs
            .get(&value_ref.pack_object_id)
            .ok_or_else(|| corruption("current-state pack is absent"))?;
        if pack.global != (*source == StateSource::Global) {
            return Err(corruption(
                "current-state pack domain differs from its selected state root",
            ));
        }
        let pack_ordinal = usize::try_from(value_ref.pack_ordinal)
            .map_err(|_| corruption("current-state pack ordinal exceeds usize"))?;
        let pack_row = pack
            .rows
            .get(pack_ordinal)
            .ok_or_else(|| corruption("current-state pack ordinal is absent"))?;
        if pack_row.encoded_key != *encoded_key {
            return Err(corruption(
                "current-state pack row identity differs from its state key",
            ));
        }
        output.push(Some((
            ResolvedCurrentPackRow {
                value: pack_row.value.clone(),
                history_page_object_id: pack_row.history_page_object_id,
                history_page_ordinal: pack_row.history_page_ordinal,
            },
            *source,
        )));
    }
    Ok(output)
}

async fn resolve_state_values_on_read<R>(
    read: &R,
    selected: &[Option<(Vec<u8>, Vec<u8>, StateSource)>],
    historical_auth: Option<HistoricalStateAuth>,
) -> Result<Vec<Option<(StateValue, StateSource)>>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let pack_rows = load_current_pack_rows(read, selected).await?;
    if historical_auth.is_none() {
        return Ok(pack_rows
            .into_iter()
            .map(|row| row.map(|(row, source)| (row.value, source)))
            .collect());
    }
    let mut page_ids = BTreeSet::new();
    for row in pack_rows.iter().flatten() {
        page_ids.insert(row.0.history_page_object_id);
    }
    let pages = super::view::load_object_map(read, page_ids).await?;
    let mut decoded_pages = BTreeMap::new();
    for (id, bytes) in pages {
        decoded_pages.insert(id, super::model::CommitChangePageV2::decode(id, &bytes)?);
    }

    let mut output = Vec::with_capacity(selected.len());
    let mut authenticated_member_closures = BTreeMap::new();
    let mut page_commit_cache = BTreeMap::new();
    let (endpoint_commit, endpoint_members) = if let Some(auth) = historical_auth {
        let closure = load_authenticated_commit_member_closure(
            read,
            auth.commit_catalog_root,
            auth.endpoint_commit_object_id,
            &mut authenticated_member_closures,
        )
        .await?;
        (Some(closure.0), Some(closure.1))
    } else {
        (None, None)
    };
    for (row, pack_row) in selected.iter().zip(pack_rows) {
        let (Some((encoded_key, _, source)), Some((pack_row, pack_source))) = (row, pack_row)
        else {
            output.push(None);
            continue;
        };
        if *source != pack_source {
            return Err(corruption(
                "current-state pack source slot changed during resolution",
            ));
        }
        let page = decoded_pages
            .get(&pack_row.history_page_object_id)
            .ok_or_else(|| corruption("state value page is absent"))?;
        let member = page
            .members
            .get(pack_row.history_page_ordinal as usize)
            .ok_or_else(|| corruption("state value page ordinal is absent"))?;
        let historical_binding = if let Some(auth) = historical_auth {
            Some(
                validate_historical_state_page_member(
                    read,
                    auth,
                    pack_row.history_page_object_id,
                    page,
                    pack_row.history_page_ordinal,
                    member,
                    encoded_key,
                    *source,
                    endpoint_members.as_deref(),
                    endpoint_commit.as_ref(),
                    &mut page_commit_cache,
                    &mut authenticated_member_closures,
                )
                .await?,
            )
        } else {
            None
        };
        let resolved = if let (Some(auth), Some(binding)) = (historical_auth, historical_binding) {
            resolve_semantic_member_with_authenticated_cache(
                read,
                auth.commit_catalog_root,
                auth.change_catalog_root,
                member,
                binding,
                &mut authenticated_member_closures,
            )
            .await?
        } else {
            unreachable!("ordinary current-state reads return before historical resolution")
        };
        let expected_global = *source == StateSource::Global;
        if resolved.global != expected_global {
            return Err(corruption(
                "state value page domain differs from its state root",
            ));
        }
        let public_change_id =
            crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(*resolved.change_id.as_bytes()));
        let record =
            crate::changelog::decode_forktree_change_payload(&resolved.payload, public_change_id)
                .map_err(|error| corruption(error.to_string()))?;
        let created_at = resolved.selected_created_at.unwrap_or(record.created_at);
        if encode_state_key(super::state::StateKeyRef {
            schema_key: &record.schema_key,
            file_id: record.file_id.as_deref(),
            entity_pk: &record.entity_pk,
        }) != *encoded_key
        {
            return Err(corruption(
                "state value page identity differs from its state key",
            ));
        }
        let cell = authenticated_current_cell_for_history(
            &record.snapshot,
            &pack_row.value.cell,
        )?;
        let logical_cell = logical_history_cell(&record.snapshot)?;
        let metadata = match record.metadata {
            crate::json_store::JsonSlot::None => None,
            crate::json_store::JsonSlot::Inline(value) => Some(value),
            crate::json_store::JsonSlot::ForkTreeObject(_) => {
                return Err(corruption("state value page contains out-of-page metadata"));
            }
        };
        let historical_value = StateValue {
            change_id: public_change_id,
            commit_id: crate::changelog::CommitId::new(uuid::Uuid::from_bytes(
                *page.commit_id.as_bytes(),
            )),
            created_at,
            updated_at: resolved.updated_at,
            cell,
            metadata: metadata.map(Into::into),
            origin_key: record.origin_key,
            blob_manifest_object_ids: resolved.blob_manifest_object_ids,
        };
        if historical_value != pack_row.value {
            return Err(corruption(
                "current-state pack value differs from its authenticated history member",
            ));
        }
        let mut output_value = historical_value;
        output_value.cell = logical_cell;
        output.push(Some((output_value, *source)));
    }
    Ok(output)
}

#[derive(Clone, Copy)]
struct HistoricalMemberBinding {
    target_commit_object_id: ObjectId,
    target_generation: u64,
    target_ordinal: usize,
}

async fn validate_historical_state_page_member<R>(
    read: &R,
    auth: HistoricalStateAuth,
    page_object_id: ObjectId,
    page: &super::model::CommitChangePageV2,
    page_ordinal: u32,
    member: &CommitMemberV1,
    encoded_key: &[u8],
    source: StateSource,
    endpoint_members: Option<&[CommitMemberV1]>,
    endpoint_commit: Option<&CommitObjectV1>,
    page_commits: &mut BTreeMap<CommitId, (ObjectId, CommitObjectV1)>,
    member_closures: &mut BTreeMap<ObjectId, AuthenticatedCommitMemberClosure>,
) -> Result<HistoricalMemberBinding, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let (target_commit_object_id, target_commit) =
        if let Some(cached) = page_commits.get(&page.commit_id) {
            cached.clone()
        } else {
            let value = lookup_on_read(
                auth.commit_catalog_root,
                "commit",
                page.commit_id.as_bytes(),
                read,
            )
            .await?
            .ok_or_else(|| corruption("historical state page commit has no CommitCatalog entry"))?;
            let entry = CommitCatalogEntry::decode(&value)?;
            let bytes = super::view::load_object_bytes(read, entry.commit_object_id).await?;
            let commit = CommitObjectV1::decode(entry.commit_object_id, &bytes)?;
            validate_commit_catalog_identity(
                read,
                auth.commit_catalog_root,
                entry.commit_object_id,
                &commit,
            )
            .await?;
            validate_commit_topology(read, auth.commit_catalog_root, page.commit_id, &commit)
                .await
                .map_err(|error| corruption(error.to_string()))?;
            let cached = (entry.commit_object_id, commit);
            page_commits.insert(page.commit_id, cached.clone());
            cached
        };
    if !target_commit
        .member_page_object_ids
        .contains(&page_object_id)
    {
        return Err(corruption(
            "historical state page is not authenticated by its Commit object",
        ));
    }
    let target_ordinal = page
        .start_ordinal
        .checked_add(page_ordinal)
        .ok_or_else(|| corruption("historical state member ordinal overflows"))?;
    let (_, target_members) = load_authenticated_commit_member_closure(
        read,
        auth.commit_catalog_root,
        target_commit_object_id,
        member_closures,
    )
    .await?;
    if target_members.get(target_ordinal as usize) != Some(member) {
        return Err(corruption(
            "historical state page member is not bound to its authenticated commit ordinal",
        ));
    }
    let endpoint_members = endpoint_members
        .ok_or_else(|| corruption("historical state endpoint membership is absent"))?;
    let endpoint_commit =
        endpoint_commit.ok_or_else(|| corruption("historical state endpoint commit is absent"))?;
    let mut endpoint_matches = endpoint_members
        .iter()
        .filter(|candidate| candidate.change_id() == member.change_id());
    let endpoint_member = endpoint_matches.next();
    if endpoint_matches.next().is_some() {
        return Err(corruption(
            "endpoint commit contains duplicate members for a historical state ChangeId",
        ));
    }
    if target_commit_object_id == auth.endpoint_commit_object_id {
        endpoint_member.ok_or_else(|| {
            corruption("historical state page member is absent from endpoint commit")
        })?;
        if endpoint_members.get(target_ordinal as usize) != Some(member) {
            return Err(corruption(
                "historical state page member is not bound to the endpoint commit ordinal",
            ));
        }
    } else if let Some(endpoint_member) = endpoint_member {
        if !matches!(
            endpoint_member,
            CommitMemberV1::Selected {
                source_commit_object_id,
                source_ordinal,
                ..
            } if *source_commit_object_id == target_commit_object_id
                && *source_ordinal == target_ordinal
        ) {
            return Err(corruption(
                "historical state page member is not an authenticated endpoint selection",
            ));
        }
    } else {
        let endpoint_root = match source {
            StateSource::Global => Some(auth.endpoint_global_state_root),
            StateSource::Branch => auth.endpoint_local_state_root,
        };
        let target_root = match source {
            StateSource::Global => target_commit.global_state_root,
            StateSource::Branch => target_commit.local_state_root,
        };
        let Some(endpoint_root) = endpoint_root else {
            return Err(corruption(
                "historical state page member is neither endpoint-selected nor endpoint-root-bound",
            ));
        };
        if endpoint_root != target_root {
            // An ancestor member is acceptable only when the endpoint's
            // authenticated state root still points at this exact page and
            // ordinal. Parent ancestry alone does not bind a state value to
            // the selected endpoint and would permit a valid ancestor page
            // to be grafted into a changed root.
            let endpoint_value = lookup_on_read(endpoint_root, "state", encoded_key, read)
                .await?
                .ok_or_else(|| {
                    corruption("historical state member is absent from endpoint state root")
                })?;
            let endpoint_pack_row = load_current_pack_rows(
                read,
                &[Some((encoded_key.to_vec(), endpoint_value, source))],
            )
            .await?
            .pop()
            .flatten()
            .map(|(row, _)| row)
            .ok_or_else(|| corruption("historical endpoint current-state pack row is absent"))?;
            if endpoint_pack_row.history_page_object_id != page_object_id
                || endpoint_pack_row.history_page_ordinal != page_ordinal
            {
                return Err(corruption(
                    "historical state page member is not bound to endpoint state root",
                ));
            }
        }
    }
    let value = lookup_on_read(
        auth.change_catalog_root,
        "change",
        member.change_id().as_bytes(),
        read,
    )
    .await?;
    let entry = match value {
        Some(value) => ChangeCatalogEntry::decode(&value)?,
        None => packed_change_catalog_entry(read, auth.change_catalog_root, member.change_id())
            .await?
            .ok_or_else(|| corruption("historical state member has no ChangeCatalog owner"))?,
    };
    if target_commit_object_id != auth.endpoint_commit_object_id {
        if let Some(endpoint_member) = endpoint_members
            .iter()
            .find(|candidate| candidate.change_id() == member.change_id())
        {
            let endpoint_ordinal = endpoint_members
                .iter()
                .position(|candidate| candidate.change_id() == member.change_id())
                .ok_or_else(|| corruption("endpoint member ordinal is absent"))?;
            validate_member_catalog_owner_with_commit_cache(
                read,
                auth.commit_catalog_root,
                auth.endpoint_commit_object_id,
                endpoint_commit.generation,
                endpoint_ordinal,
                endpoint_member,
                entry,
                member_closures,
            )
            .await?;
        }
    }
    match member {
        CommitMemberV1::Introduced { .. } => match entry.owner {
            ChangeCatalogOwner::CommitMember {
                commit_object_id,
                ordinal,
            } if commit_object_id == target_commit_object_id && ordinal == target_ordinal => Ok(()),
            ChangeCatalogOwner::CommitMember { .. } => Err(corruption(
                "introduced historical state member has the wrong ChangeCatalog owner",
            )),
            ChangeCatalogOwner::BranchRef { .. } => Err(corruption(
                "historical semantic state member has a branch-ref ChangeCatalog owner",
            )),
            ChangeCatalogOwner::PackedCommit { .. } => Err(corruption(
                "historical semantic state member resolved to a packed marker",
            )),
        },
        CommitMemberV1::Selected { .. } => {
            validate_member_catalog_owner_with_commit_cache(
                read,
                auth.commit_catalog_root,
                target_commit_object_id,
                target_commit.generation,
                target_ordinal as usize,
                member,
                entry,
                member_closures,
            )
            .await
        }
    }?;
    Ok(HistoricalMemberBinding {
        target_commit_object_id,
        target_generation: target_commit.generation,
        target_ordinal: target_ordinal as usize,
    })
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

/// Resolves several disjoint canonical state ranges through one retained
/// authenticated view. Shared ordered-tree paths and semantic member pages
/// are loaded in batches, while each output slot retains its requested range
/// and canonical key order.
pub(crate) async fn state_ranges<R>(
    view: &CoherentView<R>,
    ranges: &[(Vec<u8>, Option<Vec<u8>>)],
    include_tombstones: bool,
) -> Result<Vec<Vec<VisibleStateRow>>, StorageError>
where
    R: StorageAdapterRead,
{
    let (global_root, local_root) = current_state_roots(view);
    let rows = state_ranges_on_roots(
        global_root,
        local_root,
        view.retained_read(),
        ranges,
        include_tombstones,
    )
    .await?;
    Ok(rows
        .into_iter()
        .map(|rows| {
            rows.into_iter()
                .map(|(encoded_key, value, source)| VisibleStateRow {
                    encoded_key,
                    value,
                    source,
                    view_instance_id: view.view_instance_id(),
                })
                .collect()
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
    if limit == Some(0) {
        return Ok(Vec::new());
    }
    // This is an internal authenticated merge, not a public pagination
    // boundary.  A 64-row page repeatedly descended the same immutable tree
    // (sixteen times for the common 1K entity scan), dominating broad reads
    // after semantic payloads moved into compact change pages.  Keep the
    // working set bounded while amortizing one tree proof across a useful
    // OLTP batch.
    const STATE_RANGE_PAGE_SIZE: usize = 4_096;
    let page_size = limit.map_or(STATE_RANGE_PAGE_SIZE, |limit| {
        limit.max(1).min(STATE_RANGE_PAGE_SIZE)
    });
    let mut global_cursor = None;
    let mut local_cursor = None;
    let mut global = std::collections::VecDeque::new();
    let mut local = std::collections::VecDeque::new();
    let mut global_done = false;
    let mut local_done = local_state_root.is_none();
    let mut output = Vec::new();
    loop {
        let mut selected = Vec::with_capacity(page_size);
        while selected.len() < page_size {
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
            selected.push(Some((key, encoded, source)));
            if include_tombstones
                && limit.is_some_and(|limit| output.len() + selected.len() >= limit)
            {
                break;
            }
        }
        if selected.is_empty() {
            break;
        }
        let values = resolve_state_values_on_read(read, &selected, None).await?;
        for (selected, value) in selected.into_iter().zip(values) {
            let Some((key, _, source)) = selected else {
                continue;
            };
            let Some((value, resolved_source)) = value else {
                return Err(corruption("selected state leaf did not resolve"));
            };
            if source != resolved_source {
                return Err(corruption("state range source changed during resolution"));
            }
            if source == StateSource::Global && matches!(value.cell, StateCell::Tombstone) {
                return Err(corruption("global state tree contains a tombstone"));
            }
            if value.cell.deleted() && !include_tombstones {
                continue;
            }
            output.push((key, value, source));
            if limit.is_some_and(|limit| output.len() >= limit) {
                return Ok(output);
            }
        }
    }
    Ok(output)
}

/// Batched sibling of [`state_range_on_roots`] for disjoint exact-prefix
/// ranges. The tree walk and semantic page resolution are each shared across
/// the complete request rather than repeated once per primary key.
pub(crate) async fn state_ranges_on_roots<R>(
    global_state_root: ObjectId,
    local_state_root: Option<ObjectId>,
    read: &R,
    ranges: &[(Vec<u8>, Option<Vec<u8>>)],
    include_tombstones: bool,
) -> Result<Vec<Vec<(Vec<u8>, StateValue, StateSource)>>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
    let global = scan_ranges_on_read(global_state_root, "state", ranges, read).await?;
    let local = match local_state_root {
        Some(root) => scan_ranges_on_read(root, "state", ranges, read).await?,
        None => vec![Vec::new(); ranges.len()],
    };

    let mut selected_groups = Vec::with_capacity(ranges.len());
    let mut selected_flat = Vec::new();
    for (global, local) in global.into_iter().zip(local) {
        let mut global = global.into_iter().peekable();
        let mut local = local.into_iter().peekable();
        let start = selected_flat.len();
        while global.peek().is_some() || local.peek().is_some() {
            let take_local = match (global.peek(), local.peek()) {
                (None, Some(_)) => true,
                (Some(_), None) => false,
                (Some((global_key, _)), Some((local_key, _))) => local_key <= global_key,
                (None, None) => break,
            };
            let selected = if take_local {
                let (key, value) = local.next().expect("peeked local state row");
                if global
                    .peek()
                    .is_some_and(|(global_key, _)| *global_key == key)
                {
                    global.next();
                }
                Some((key, value, StateSource::Branch))
            } else {
                let (key, value) = global.next().expect("peeked global state row");
                Some((key, value, StateSource::Global))
            };
            selected_flat.push(selected);
        }
        selected_groups.push(start..selected_flat.len());
    }

    let values = resolve_state_values_on_read(read, &selected_flat, None).await?;
    let mut output = Vec::with_capacity(selected_groups.len());
    for group in selected_groups {
        let mut rows = Vec::with_capacity(group.len());
        for index in group {
            let (key, _, source) = selected_flat[index]
                .as_ref()
                .ok_or_else(|| corruption("selected state leaf is absent"))?;
            let (value, resolved_source) = values[index]
                .clone()
                .ok_or_else(|| corruption("selected state leaf did not resolve"))?;
            if *source != resolved_source {
                return Err(corruption(
                    "state batch range source changed during resolution",
                ));
            }
            if *source == StateSource::Global && matches!(value.cell, StateCell::Tombstone) {
                return Err(corruption("global state tree contains a tombstone"));
            }
            if value.cell.deleted() && !include_tombstones {
                continue;
            }
            rows.push((key.clone(), value, *source));
        }
        output.push(rows);
    }
    Ok(output)
}

/// Loads the complete authenticated state overlay for one historical commit.
/// A missing commit/catalog/root is an error; an absent key is represented by
/// the absence of a row in the returned ordered stream.
pub(crate) async fn load_historical_commit_state_roots<R>(
    read: &R,
    commit_id: crate::changelog::CommitId,
) -> Result<(ObjectId, ObjectId), crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let repository = load_repository_root(read).await?;
    load_historical_commit_state_roots_from_repository(read, &repository, commit_id).await
}

pub(crate) async fn load_historical_commit_state_roots_from_repository<R>(
    read: &R,
    repository: &RepositoryRootV1,
    commit_id: crate::changelog::CommitId,
) -> Result<(ObjectId, ObjectId), crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
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
    Ok((commit.global_state_root, commit.local_state_root))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StaleCommitSummary {
    commit_id: CommitId,
    commit_object_id: ObjectId,
    generation: u64,
    global_state_root: ObjectId,
    local_state_root: ObjectId,
    member_page_object_ids: Vec<ObjectId>,
}

impl StaleCommitSummary {
    pub(super) fn global_state_root(&self) -> ObjectId {
        self.global_state_root
    }

    pub(super) fn local_state_root(&self) -> ObjectId {
        self.local_state_root
    }
}

/// Authenticates only the commit envelope and root/topology summary required
/// by stale reconciliation. Semantic member closure remains lazy and is
/// authenticated by the changed state leaves that stale classification reads.
/// The successful result is cached only in the caller's retained-read
/// operation; failures are never inserted.
pub(super) async fn load_historical_commit_state_roots_for_stale<R>(
    read: &R,
    binding_id: u64,
    repository: &RepositoryRootV1,
    commit_id: crate::changelog::CommitId,
    cache: &mut StaleCommitSummaryCache,
) -> Result<StaleCommitSummary, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    cache.assert_binding(binding_id)?;
    if let Some(summary) = cache.entries.get(&commit_id) {
        return Ok(summary.clone());
    }
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
    validate_commit_topology(read, repository.commit_catalog_root, catalog_id, &commit).await?;
    validate_root_on_read(commit.global_state_root, "state", read).await?;
    validate_root_on_read(commit.local_state_root, "state", read).await?;
    let summary = StaleCommitSummary {
        commit_id: catalog_id,
        commit_object_id: entry.commit_object_id,
        generation: commit.generation,
        global_state_root: commit.global_state_root,
        local_state_root: commit.local_state_root,
        member_page_object_ids: commit.member_page_object_ids.clone(),
    };
    cache.entries.insert(commit_id, summary);
    Ok(cache
        .entries
        .get(&commit_id)
        .expect("stale summary was just inserted")
        .clone())
}

fn historical_state_rows_from_range(
    rows: Vec<(Vec<u8>, StateValue, StateSource)>,
) -> Result<Vec<HistoricalStateRow>, crate::LixError> {
    rows.into_iter()
        .map(|(encoded_key, value, source)| {
            let key = decode_state_key(&encoded_key)?;
            let (snapshot_content, deleted) = match value.cell {
                StateCell::Value(snapshot) => (Some(snapshot), false),
                StateCell::NativeRow(_) => {
                    return Err(corruption(
                        "historical native state range requires its authenticated branch owner",
                    )
                    .into());
                }
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

pub(crate) async fn scan_state_rows_at_commit<R>(
    read: &R,
    commit_id: crate::changelog::CommitId,
) -> Result<Vec<HistoricalStateRow>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    scan_state_rows_at_commit_bounds(read, commit_id, None, None).await
}

/// Loads only one authenticated state-key range for a historical commit. The
/// commit envelope, catalog back-edge, retained roots, and every returned leaf
/// are validated identically to the complete scan; the range merely prevents
/// unrelated state subtrees from being visited.
pub(crate) async fn scan_state_rows_at_commit_range<R>(
    read: &R,
    commit_id: crate::changelog::CommitId,
    lower: &[u8],
    upper: Option<&[u8]>,
) -> Result<Vec<HistoricalStateRow>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    scan_state_rows_at_commit_bounds(read, commit_id, Some(lower), upper).await
}

async fn scan_state_rows_at_commit_bounds<R>(
    read: &R,
    commit_id: crate::changelog::CommitId,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
) -> Result<Vec<HistoricalStateRow>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let (
        commit_catalog_root,
        change_catalog_root,
        endpoint_commit_object_id,
        global_state_root,
        local_state_root,
    ) = authenticate_historical_state_roots_for_diff(read, commit_id).await?;
    let discovered = state_range_on_roots(
        global_state_root,
        Some(local_state_root),
        read,
        lower,
        upper,
        None,
        true,
    )
    .await?;
    let keys = discovered
        .into_iter()
        .map(|(key, _, _)| key)
        .collect::<Vec<_>>();
    let values = state_points_on_read_with_historical_auth(
        global_state_root,
        Some(local_state_root),
        &keys,
        true,
        commit_catalog_root,
        change_catalog_root,
        endpoint_commit_object_id,
        read,
    )
    .await?;
    let rows = keys
        .into_iter()
        .zip(values)
        .map(|(key, value)| {
            value
                .map(|(value, source)| (key, value, source))
                .ok_or_else(|| corruption("historical range key disappeared during exact authentication"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    historical_state_rows_from_range(rows)
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
        let (key, value, audit) = match mutation {
            StateTreeMutation::Insert { key, value, audit }
            | StateTreeMutation::Update { key, value, audit } => (key, Some(value), audit.as_ref()),
            StateTreeMutation::Remove { key } => (key, None, None),
            StateTreeMutation::RemoveRange { lower, upper } => {
                super::state::validate_state_entity_prefix(lower)
                    .map_err(|error| corruption(error.to_string()))?;
                if upper.as_ref().is_some_and(|upper| lower >= upper) {
                    return Err(corruption("state-tree delete range is empty or reversed"));
                }
                wrote_tombstone = true;
                continue;
            }
        };
        decode_state_key(key).map_err(|error| corruption(error.to_string()))?;
        if let Some(value) = value {
            let _ = decode_state_value_storage(value)?;
        }
        if let Some(audit) = audit {
            wrote_tombstone |= audit.tombstone;
            written_commit_ids.insert(audit.commit_id);
            added_blob_roots.extend(
                audit
                    .blob_manifest_object_ids
                    .iter()
                    .copied()
                    .map(|object_id| (object_id, ())),
            );
        }
    }
    let mut point_mutations = Vec::new();
    let mut range_deletes = Vec::new();
    for mutation in mutations {
        match mutation {
            StateTreeMutation::RemoveRange { lower, upper } => {
                range_deletes.push((lower, upper));
            }
            mutation => point_mutations.push(mutation.into_ordered()),
        }
    }
    range_deletes.sort_by(|left, right| left.0.cmp(&right.0));
    if range_deletes.windows(2).any(|pair| {
        pair[0]
            .1
            .as_ref()
            .is_none_or(|upper| pair[1].0.as_slice() < upper.as_slice())
    }) {
        return Err(corruption("state-tree delete ranges overlap"));
    }
    let edit = apply_ordered_mutations(root, "state", &point_mutations, read).await?;
    let mut next_root = edit.root;
    let mut objects = edit.objects;
    let mut copied_nodes = edit.copied_nodes;
    for (lower, upper) in range_deletes {
        let overlay = ObjectOverlayRead::new(read, &objects);
        let range_edit =
            delete_ordered_range(next_root, "state", &lower, upper.as_deref(), &overlay).await?;
        next_root = range_edit.root;
        copied_nodes = copied_nodes.saturating_add(range_edit.copied_nodes);
        objects.extend(range_edit.objects)?;
    }
    Ok(StateTreeEdit {
        base_root: root.object_id,
        root: next_root.object_id,
        entry_count: next_root.entry_count,
        copied_nodes,
        added_blob_roots,
        wrote_tombstone,
        written_commit_ids,
        objects,
    })
}

/// Replaces one complete authenticated state-key range in a single canonical
/// tree build.
///
/// The caller supplies the complete, strictly ordered post-image for the
/// range.  Entries outside the range retain their existing authenticated
/// state-page references; entries inside it are never read or materialized.
/// Consequently a collection replacement is `O(outside + replacement)` and
/// does not perform one root-to-leaf path copy per row.  The returned root is
/// the sole state authority and is authenticated by the semantic commit.
pub(crate) async fn replace_state_tree_range<R>(
    root: ObjectId,
    lower: Vec<u8>,
    upper: Option<Vec<u8>>,
    replacement: Vec<(Vec<u8>, Vec<u8>, StateMutationAudit)>,
    read: &R,
) -> Result<StateTreeEdit, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    super::state::validate_state_entity_prefix(&lower)
        .map_err(|error| corruption(error.to_string()))?;
    if upper.as_ref().is_some_and(|upper| lower >= *upper) {
        return Err(corruption(
            "state-tree replacement range is empty or reversed",
        ));
    }
    if replacement.windows(2).any(|pair| pair[0].0 >= pair[1].0)
        || replacement.iter().any(|(key, value, _)| {
            key.as_slice() < lower.as_slice()
                || upper
                    .as_deref()
                    .is_some_and(|upper| key.as_slice() >= upper)
                || super::state::decode_state_key(key).is_err()
                || decode_state_value(value).is_err()
        })
    {
        return Err(corruption(
            "state-tree replacement entries are invalid, unordered, or outside their range",
        ));
    }

    let root = validate_root_on_read(root, "state", read).await?;
    let mut entries =
        scan_range_on_read(root.object_id, "state", None, Some(&lower), None, read).await?;
    entries.reserve(replacement.len());
    entries.extend(
        replacement
            .iter()
            .map(|(key, value, _)| (key.clone(), value.clone())),
    );
    if let Some(upper) = upper.as_deref() {
        entries.extend(
            scan_range_on_read(root.object_id, "state", Some(upper), None, None, read).await?,
        );
    }
    if entries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(corruption(
            "state-tree root replacement produced duplicate or unordered keys",
        ));
    }
    let build = super::tree::build_state_tree(&entries)?;
    let copied_nodes = build.objects.iter().count();
    let mut added_blob_roots = BTreeMap::new();
    let mut written_commit_ids = BTreeSet::new();
    let mut wrote_tombstone = false;
    for (_, _, audit) in replacement {
        wrote_tombstone |= audit.tombstone;
        written_commit_ids.insert(audit.commit_id);
        added_blob_roots.extend(
            audit
                .blob_manifest_object_ids
                .into_iter()
                .map(|object_id| (object_id, ())),
        );
    }
    Ok(StateTreeEdit {
        base_root: root.object_id,
        root: build.root.object_id,
        entry_count: build.root.entry_count,
        copied_nodes,
        added_blob_roots,
        wrote_tombstone,
        written_commit_ids,
        objects: build.objects,
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

#[cfg(test)]
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

#[cfg(test)]
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
    Ok(load_commit_with_members(view, id)
        .await?
        .map(|(_, commit, _)| commit))
}

async fn load_commit_with_members<R>(
    view: &CoherentView<R>,
    id: CommitId,
) -> Result<Option<(ObjectId, CommitObjectV1, Arc<[CommitMemberV1]>)>, StorageError>
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
    let members = validate_retained_commit_members(
        view.retained_read(),
        view.repository_root().commit_catalog_root,
        view.repository_root().change_catalog_root,
        entry.commit_object_id,
        &commit,
    )
    .await?;
    Ok(Some((entry.commit_object_id, commit, members)))
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
        commit_entries: BTreeMap::new(),
        change_entries: BTreeMap::new(),
        objects: edit.objects,
    })
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
    validate_retained_commit_members(
        read,
        commit_catalog_root,
        change_catalog_root,
        commit_object_id,
        commit,
    )
    .await?;
    Ok(())
}

async fn validate_retained_commit_members<R>(
    read: &R,
    commit_catalog_root: ObjectId,
    change_catalog_root: ObjectId,
    commit_object_id: ObjectId,
    commit: &CommitObjectV1,
) -> Result<Arc<[CommitMemberV1]>, StorageError>
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
    validate_commit_catalog_identity(read, commit_catalog_root, commit_object_id, commit).await?;
    let members: Arc<[CommitMemberV1]> = load_commit_members(read, commit).await?.into();
    let mut closures = BTreeMap::from([(commit_object_id, (commit.clone(), Arc::clone(&members)))]);
    let change_ids = members
        .iter()
        .map(CommitMemberV1::change_id)
        .collect::<Vec<_>>();
    let catalog_values =
        lookup_change_catalog_entries(read, change_catalog_root, &change_ids).await?;
    if catalog_values.len() != members.len() {
        return Err(corruption(
            "batched retained ChangeCatalog lookup returned the wrong number of values",
        ));
    }
    for (ordinal, (member, value)) in members.iter().zip(catalog_values).enumerate() {
        let change = semantic_change_for_member_with_commit_cache(
            read,
            commit_catalog_root,
            member,
            &mut closures,
        )
        .await?;
        if change.change_id() != member.change_id() {
            return Err(corruption(
                "resolved retained Change disagrees with its commit member",
            ));
        }
        let entry =
            value.ok_or_else(|| corruption("retained Change object has no ChangeCatalog owner"))?;
        validate_member_catalog_owner_with_commit_cache(
            read,
            commit_catalog_root,
            commit_object_id,
            commit.generation,
            ordinal,
            member,
            entry,
            &mut closures,
        )
        .await?;
    }
    Ok(members)
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
    if entry.owner
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

fn decode_state_value_storage(bytes: &[u8]) -> Result<super::state::StateValueRef, StorageError> {
    decode_state_value(bytes).map_err(|error| corruption(error.to_string()))
}
