use std::collections::{BTreeMap, BTreeSet};

use crate::storage::StorageError;
use crate::storage_adapter::StorageAdapterRead;

use super::codec::corruption;
use super::model::{
    ChangeCatalogEntry, ChangeCatalogOwner, ChangeId, ChangeObjectV1, CommitCatalogEntry, CommitId,
    CommitObjectV1,
};
use super::object::ObjectId;
use super::state::{StateCell, StateValue, decode_state_key, decode_state_value};
use super::tree::{
    ImmutableObjectSet, OrderedTreeMutation, apply_ordered_mutations, lookup_on_read,
    scan_bounded_page_on_read, scan_page_on_read, validate_root_on_read,
};
use super::view::CoherentView;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StateSource {
    Global,
    Branch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VisibleStateRow {
    pub(crate) encoded_key: Vec<u8>,
    pub(crate) value: StateValue,
    pub(crate) source: StateSource,
}

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

pub(crate) async fn state_point<R>(
    view: &CoherentView<R>,
    key: &[u8],
    include_tombstone: bool,
) -> Result<Option<VisibleStateRow>, StorageError>
where
    R: StorageAdapterRead,
{
    if let Some(encoded) = lookup_on_read(
        view.branch_snapshot().local_state_root,
        "state",
        key,
        view.read(),
    )
    .await?
    {
        let value = decode_state_value_storage(&encoded)?;
        return if value.cell.deleted() && !include_tombstone {
            Ok(None)
        } else {
            Ok(Some(VisibleStateRow {
                encoded_key: key.to_vec(),
                value,
                source: StateSource::Branch,
            }))
        };
    }
    let Some(encoded) = lookup_on_read(
        view.repository_root().global_state_root,
        "state",
        key,
        view.read(),
    )
    .await?
    else {
        return Ok(None);
    };
    let value = decode_state_value_storage(&encoded)?;
    if matches!(value.cell, StateCell::Tombstone) {
        return Err(corruption("global state tree contains a tombstone"));
    }
    Ok(Some(VisibleStateRow {
        encoded_key: key.to_vec(),
        value,
        source: StateSource::Global,
    }))
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
    let page_size = limit.unwrap_or(64).clamp(1, 64);
    let mut output = Vec::new();
    let mut global_cursor = None;
    let mut local_cursor = None;
    let mut global = std::collections::VecDeque::new();
    let mut local = std::collections::VecDeque::new();
    let mut global_done = false;
    let mut local_done = false;
    loop {
        if limit.is_some_and(|limit| output.len() >= limit) {
            break;
        }
        if global.is_empty() && !global_done {
            let page = scan_bounded_page_on_read(
                view.repository_root().global_state_root,
                "state",
                lower,
                upper,
                global_cursor.as_deref(),
                page_size,
                view.read(),
            )
            .await?;
            global_done = page.len() < page_size;
            global_cursor = page.last().map(|(key, _)| key.clone());
            global.extend(page);
        }
        if local.is_empty() && !local_done {
            let page = scan_bounded_page_on_read(
                view.branch_snapshot().local_state_root,
                "state",
                lower,
                upper,
                local_cursor.as_deref(),
                page_size,
                view.read(),
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
        output.push(VisibleStateRow {
            encoded_key: key,
            value,
            source,
        });
    }
    Ok(output)
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
    let mut mutations = Vec::new();
    for (id, entry) in entries {
        let value = entry.encode()?;
        match lookup_on_read(root, "commit", id.as_bytes(), read).await? {
            None => mutations.push(OrderedTreeMutation::Insert {
                key: id.as_bytes().to_vec(),
                value,
            }),
            Some(existing) if existing == value => {}
            Some(_) => {
                return Err(corruption("CommitCatalog cannot remap one stable CommitId"));
            }
        }
    }
    let mut edit = edit_catalog(root, "commit", &mutations, read).await?;
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
    let mut mutations = Vec::new();
    for (id, entry) in entries {
        let value = entry.encode()?;
        match lookup_on_read(root, "change", id.as_bytes(), read).await? {
            None => mutations.push(OrderedTreeMutation::Insert {
                key: id.as_bytes().to_vec(),
                value,
            }),
            Some(existing) if existing == value => {}
            Some(_) => {
                return Err(corruption(
                    "ChangeCatalog cannot remap one stable ChangeId or owner",
                ));
            }
        }
    }
    let mut edit = edit_catalog(root, "change", &mutations, read).await?;
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
    edit_catalog(root, "commit", &mutations, read).await
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
    edit_catalog(root, "change", &mutations, read).await
}

pub(crate) async fn load_commit<R>(
    view: &CoherentView<R>,
    id: CommitId,
) -> Result<Option<CommitObjectV1>, StorageError>
where
    R: StorageAdapterRead,
{
    let Some(value) = lookup_on_read(
        view.repository_root().commit_catalog_root,
        "commit",
        id.as_bytes(),
        view.read(),
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
    validate_retained_commit(
        view.read(),
        view.repository_root().change_catalog_root,
        entry.commit_object_id,
        &commit,
    )
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
    let Some(value) = lookup_on_read(
        view.repository_root().change_catalog_root,
        "change",
        id.as_bytes(),
        view.read(),
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
    let rows = scan_page_on_read(
        root,
        "commit",
        start_after.as_deref(),
        page_size,
        view.read(),
    )
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
        validate_retained_commit(
            view.read(),
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
    let rows = scan_page_on_read(
        root,
        "change",
        start_after.as_deref(),
        page_size,
        view.read(),
    )
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
) -> Result<CatalogTreeEdit, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let root = validate_root_on_read(root, kind, read).await?;
    let edit = apply_ordered_mutations(root, kind, mutations, read).await?;
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
            if commit.member_change_object_ids.get(ordinal as usize)
                != Some(&entry.change_object_id)
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
            validate_retained_ref_change(
                view.read(),
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
    for (ordinal, change_object_id) in commit.member_change_object_ids.iter().enumerate() {
        let bytes = super::view::load_object_bytes(read, *change_object_id).await?;
        let change = ChangeObjectV1::decode(*change_object_id, &bytes)?;
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
        let ordinal =
            u32::try_from(ordinal).map_err(|_| corruption("commit member ordinal exceeds u32"))?;
        if entry.change_object_id != *change_object_id
            || entry.owner
                != (ChangeCatalogOwner::CommitMember {
                    commit_object_id,
                    ordinal,
                })
        {
            return Err(corruption(
                "retained Change object disagrees with its ChangeCatalog owner/back-edge",
            ));
        }
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
                change_id: previous_change_id,
                branch_id: previous_branch_id,
                after_semantic_head_commit_object_id: previous_after,
                ..
            } = previous
            else {
                return Err(corruption("RefChange predecessor is a semantic Change"));
            };
            if previous_branch_id != *branch_id
                || previous_after != *before_semantic_head_commit_object_id
                || previous_change_id.as_bytes() >= change_id.as_bytes()
            {
                return Err(corruption(
                    "RefChange predecessor chronology or branch binding is invalid",
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
