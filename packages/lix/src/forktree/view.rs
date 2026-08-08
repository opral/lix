use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::storage::{
    BeginScanOptions, CoreProjection, GetManyRequest, GetOptions, Key, KeyRange, ProjectedValue,
    ReadOptions, ScanOrder, Storage, StorageError,
};
use crate::storage_adapter::{StorageAdapterRead, StorageAdapterReadScope};

use super::codec::{Encoder, corruption, keyed_hash};
use super::model::{
    BranchSelectorV1, BranchSnapshotV1, CanonicalBranchId, ChangeCatalogEntry, ChangeId,
    ChangeObjectV1, CommitCatalogEntry, CommitId, CommitObjectV1, GlobalSelectorV1,
    RepositoryRootV1, branch_selector_key, global_selector_key,
};
use super::object::{OBJECT_SPACE, ObjectId};

pub(super) const SELECTOR_SPACE: crate::storage::StorageSpace =
    crate::storage::StorageSpace::engine_declared(
        0x0009_0002,
        "forktree.selector.v1",
        crate::storage::ValueSemantics::Mutable,
    );

const VIEW_ID_DOMAIN: &str = "lix forktree coherent selector view v1";
static NEXT_VIEW_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

/// One authenticated branch/global state pair acquired from one immutable
/// storage read. The owned read handle is retained for every later object and
/// catalog traversal, so a caller cannot silently refresh either selector.
pub(crate) struct CoherentView<R> {
    read: R,
    branch_id: CanonicalBranchId,
    raw_global_selector: Bytes,
    raw_branch_selector: Bytes,
    global_selector: GlobalSelectorV1,
    branch_selector: BranchSelectorV1,
    repository_root: RepositoryRootV1,
    branch_snapshot: BranchSnapshotV1,
    view_id: [u8; 32],
    view_instance_id: u64,
}

impl<R> CoherentView<R>
where
    R: StorageAdapterRead,
{
    pub(crate) fn branch_id(&self) -> CanonicalBranchId {
        self.branch_id
    }

    pub(crate) fn view_id(&self) -> [u8; 32] {
        self.view_id
    }

    pub(super) fn view_instance_id(&self) -> u64 {
        self.view_instance_id
    }

    pub(crate) fn raw_global_selector(&self) -> &Bytes {
        &self.raw_global_selector
    }

    pub(crate) fn raw_branch_selector(&self) -> &Bytes {
        &self.raw_branch_selector
    }

    pub(crate) fn global_selector(&self) -> GlobalSelectorV1 {
        self.global_selector
    }

    pub(crate) fn branch_selector(&self) -> BranchSelectorV1 {
        self.branch_selector
    }

    pub(crate) fn repository_root(&self) -> RepositoryRootV1 {
        self.repository_root
    }

    pub(crate) fn branch_snapshot(&self) -> BranchSnapshotV1 {
        self.branch_snapshot
    }

    pub(super) fn storage_read(&self) -> &R {
        &self.read
    }

    /// Applies an authenticated state-tree edit using this view's retained
    /// read. The storage handle never leaves the ForkTree owner.
    pub(crate) async fn edit_state_tree(
        &self,
        root: ObjectId,
        mutations: Vec<super::serving::StateTreeMutation>,
    ) -> Result<super::serving::StateTreeEdit, StorageError> {
        super::serving::edit_state_tree(root, mutations, &self.read).await
    }

    /// Applies ordered intermediate state edits while retaining the same
    /// authenticated operation read for every path-copy lookup.
    pub(crate) async fn edit_state_tree_sequence(
        &self,
        root: ObjectId,
        mutation_batches: Vec<Vec<super::serving::StateTreeMutation>>,
    ) -> Result<Vec<super::serving::StateTreeEdit>, StorageError> {
        super::serving::edit_state_tree_sequence(root, mutation_batches, &self.read).await
    }

    pub(crate) async fn put_commit_catalog_entries(
        &self,
        root: ObjectId,
        entries: &[(CommitId, CommitCatalogEntry)],
    ) -> Result<super::serving::CatalogTreeEdit, StorageError> {
        super::serving::put_commit_catalog_entries(root, entries, &self.read).await
    }

    pub(crate) async fn put_change_catalog_entries(
        &self,
        root: ObjectId,
        entries: &[(ChangeId, ChangeCatalogEntry)],
    ) -> Result<super::serving::CatalogTreeEdit, StorageError> {
        super::serving::put_change_catalog_entries(root, entries, &self.read).await
    }

    pub(crate) async fn state_point_at_roots(
        &self,
        global_root: ObjectId,
        local_root: ObjectId,
        key: &[u8],
        include_tombstone: bool,
    ) -> Result<Option<(super::state::StateValue, super::serving::StateSource)>, StorageError> {
        super::serving::state_point_on_read(
            global_root,
            local_root,
            key,
            include_tombstone,
            &self.read,
        )
        .await
    }

    /// Scans authenticated untracked rows for this branch without exposing
    /// the underlying storage read to callers outside ForkTree.
    pub(crate) async fn scan_untracked_rows(
        &self,
    ) -> Result<Vec<(super::state::StateKey, super::state::UntrackedValue)>, crate::LixError> {
        let mut cursor = self
            .read
            .begin_scan(
                super::state::UNTRACKED_ROW_SPACE,
                KeyRange {
                    lower: std::ops::Bound::Unbounded,
                    upper: std::ops::Bound::Unbounded,
                },
                BeginScanOptions {
                    projection: CoreProjection::FullValue,
                    order: ScanOrder::Ascending,
                },
            )
            .await?;
        let mut rows = Vec::new();
        loop {
            let page = cursor.next_page(256).await?;
            for entry in page.entries {
                let (branch_id, key) = super::state::decode_untracked_key(&entry.key.0)?;
                if branch_id != self.branch_id {
                    continue;
                }
                let value = match entry.value {
                    ProjectedValue::FullValue(bytes) => {
                        super::state::decode_untracked_value(&bytes)?
                    }
                    ProjectedValue::KeyOnly => {
                        return Err(crate::LixError::new(
                            crate::LixError::CODE_STORAGE_ERROR,
                            "ForkTree untracked scan returned key-only data",
                        ));
                    }
                };
                rows.push((key, value));
            }
            if !page.has_more {
                break;
            }
        }
        Ok(rows)
    }

    pub(crate) fn bind_resume_key(&self, catalog_root: ObjectId, last_key: &[u8]) -> Vec<u8> {
        let mut encoder = Encoder::with_prefix(b"LIXFTR\0\x01");
        encoder.fixed(&self.view_id);
        encoder.fixed(self.global_selector.repository_root.as_bytes());
        encoder.fixed(catalog_root.as_bytes());
        encoder
            .bytes(last_key)
            .expect("storage keys necessarily fit the canonical u32 envelope");
        let mut token = encoder.into_vec();
        token.extend_from_slice(&keyed_hash("lix forktree resume token v1", &token));
        token
    }

    pub(crate) fn validate_resume_key(
        &self,
        catalog_root: ObjectId,
        token: &[u8],
    ) -> Result<Vec<u8>, StorageError> {
        let checksum_offset = token
            .len()
            .checked_sub(32)
            .ok_or_else(|| corruption("resume token is shorter than its checksum"))?;
        let (body, checksum) = token.split_at(checksum_offset);
        if keyed_hash("lix forktree resume token v1", body).as_slice() != checksum {
            return Err(StorageError::InvalidCursor);
        }
        let mut decoder = super::codec::Decoder::after_prefix(body, b"LIXFTR\0\x01")?;
        if decoder.fixed::<32>()? != self.view_id
            || decoder.fixed::<32>()? != *self.global_selector.repository_root.as_bytes()
            || decoder.fixed::<32>()? != *catalog_root.as_bytes()
        {
            return Err(StorageError::InvalidCursor);
        }
        let last_key = decoder.bytes("resume key")?;
        decoder.finish()?;
        Ok(last_key)
    }
    pub(crate) async fn load_object_bytes(&self, id: ObjectId) -> Result<Bytes, StorageError> {
        load_object_bytes(&self.read, id).await
    }
}

/// One operation-scoped ForkTree read facade. Branch views borrow the same
/// retained read identity; no branch or untracked traversal can refresh it.
pub(crate) struct ForkTreeReadFacade<R> {
    read: R,
}

impl<R> ForkTreeReadFacade<R>
where
    R: StorageAdapterRead,
{
    pub(crate) fn new(read: R) -> Self {
        Self { read }
    }

    pub(crate) async fn branch(
        &self,
        branch_id: &str,
    ) -> Result<CoherentView<&R>, crate::LixError> {
        let uuid = uuid::Uuid::parse_str(branch_id).map_err(|error| {
            crate::LixError::new(
                crate::LixError::CODE_INVALID_PARAM,
                format!("branch ID must be a UUID: {error}"),
            )
        })?;
        let view =
            open_coherent_view_on_read(&self.read, CanonicalBranchId::from_bytes(*uuid.as_bytes()))
                .await
                .map_err(crate::LixError::from)?;
        let _view_identity = view.view_id();
        let _resume_identity_validator = CoherentView::<&R>::validate_resume_key;
        let _ = _resume_identity_validator;
        Ok(view)
    }

    pub(crate) async fn load_commit_member_records(
        &self,
        commit_id: crate::changelog::CommitId,
    ) -> Result<Option<Vec<crate::changelog::ChangeRecord>>, crate::LixError> {
        super::serving::load_commit_member_records(&self.read, commit_id).await
    }

    pub(crate) async fn load_state_value_at_commit(
        &self,
        commit_id: crate::changelog::CommitId,
        key: &[u8],
        include_tombstone: bool,
    ) -> Result<Option<(super::state::StateValue, super::serving::StateSource)>, crate::LixError>
    {
        super::serving::load_state_value_at_commit(&self.read, commit_id, key, include_tombstone)
            .await
    }

    /// Loads exact historical rows from the authenticated ForkTree state
    /// owner. The transaction supplies the key identities; the returned
    /// ForkTree-owned row shape is a terminal merge-consumer value and never
    /// opens another reader or consults the superseded tracked-state reader.
    pub(crate) async fn load_state_rows_at_commit(
        &self,
        commit_id: &str,
        keys: &[super::state::StateKey],
    ) -> Result<Vec<Option<super::state::HistoricalStateRow>>, crate::LixError> {
        let commit_id = crate::changelog::CommitId::parse_lix(commit_id, "historical commit")?;
        let mut rows = Vec::with_capacity(keys.len());
        for key in keys {
            let encoded_key = super::state::encode_state_key(super::state::StateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            });
            let value = self
                .load_state_value_at_commit(commit_id, &encoded_key, true)
                .await?;
            rows.push(value.map(|(value, _source)| {
                let (snapshot_content, deleted) = match value.cell {
                    super::state::StateCell::Value(snapshot) => (Some(snapshot), false),
                    super::state::StateCell::Null => (None, false),
                    super::state::StateCell::Tombstone => (None, true),
                };
                super::state::HistoricalStateRow {
                    key: key.clone(),
                    snapshot_content,
                    metadata: value.metadata,
                    deleted,
                    created_at: value.created_at,
                    updated_at: value.updated_at,
                    change_id: value.change_id,
                    commit_id: value.commit_id,
                }
            }));
        }
        Ok(rows)
    }

    pub(crate) async fn load_json_slot(
        &self,
        slot: &crate::json_store::JsonSlot,
    ) -> Result<Option<String>, crate::LixError> {
        match slot {
            crate::json_store::JsonSlot::None => Ok(None),
            crate::json_store::JsonSlot::Inline(value) => Ok(Some(value.to_string())),
            crate::json_store::JsonSlot::Ref(json_ref) => {
                let refs = [*json_ref];
                let values = crate::json_store::JsonStoreContext::new()
                    .load_bytes_many(
                        &self.read,
                        crate::json_store::JsonLoadRequestRef {
                            refs: &refs,
                            scope: crate::json_store::JsonReadScopeRef::OutOfBand,
                        },
                    )
                    .await?
                    .into_values();
                let Some(Some(bytes)) = values.into_iter().next() else {
                    return Err(crate::LixError::new(
                        crate::LixError::CODE_STORAGE_ERROR,
                        "authenticated change snapshot payload is missing",
                    ));
                };
                String::from_utf8(bytes.to_vec())
                    .map(Some)
                    .map_err(|error| {
                        crate::LixError::new(
                            crate::LixError::CODE_STORAGE_ERROR,
                            format!("authenticated change snapshot payload is not UTF-8: {error}"),
                        )
                    })
            }
        }
    }
}

pub(crate) async fn open_coherent_view<S>(
    storage: &S,
    branch_id: CanonicalBranchId,
) -> Result<CoherentView<StorageAdapterReadScope<S::Read<'_>>>, StorageError>
where
    S: Storage,
{
    // This is intentionally the one and only begin_read in the acquisition
    // protocol. Every later object load receives this owned handle.
    let read = StorageAdapterReadScope::new(storage.begin_read(ReadOptions::default()).await?);
    open_coherent_view_on_read(read, branch_id).await
}

/// Acquires the exact selector pair and all root objects through a caller-owned
/// adapter read. Transaction/session open calls `begin_read` once, passes that
/// handle here, and must retain the resulting view for all traversal,
/// pagination, and publication preconditions.
pub(crate) async fn open_coherent_view_on_read<R>(
    read: R,
    branch_id: CanonicalBranchId,
) -> Result<CoherentView<R>, StorageError>
where
    R: StorageAdapterRead,
{
    let selector_keys = [
        Key(global_selector_key()),
        Key(branch_selector_key(branch_id)),
    ];
    let loaded = read
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &selector_keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    if loaded.values.len() != selector_keys.len() {
        return Err(corruption(
            "selector get_many returned the wrong number of values",
        ));
    }
    let mut values = loaded.values.into_iter();
    let raw_global_selector =
        projected_required(values.next().flatten(), "global selector is absent")?;
    let raw_branch_selector = projected_required(
        values.next().flatten(),
        "requested branch selector is absent",
    )?;
    let global_selector = GlobalSelectorV1::decode(&raw_global_selector)?;
    let branch_selector = BranchSelectorV1::decode(&raw_branch_selector)?;
    if branch_selector.branch_id != branch_id {
        return Err(corruption(
            "branch selector key does not match its authenticated branch id",
        ));
    }

    let object_ids = [
        Key(Bytes::copy_from_slice(
            global_selector.repository_root.as_bytes(),
        )),
        Key(Bytes::copy_from_slice(
            branch_selector.branch_snapshot_object_id.as_bytes(),
        )),
    ];
    let objects = read
        .get_many(&[GetManyRequest {
            space: OBJECT_SPACE,
            keys: &object_ids,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    if objects.values.len() != object_ids.len() {
        return Err(corruption(
            "root object get_many returned the wrong number of values",
        ));
    }
    let mut objects = objects.values.into_iter();
    let raw_repository_root = projected_required(
        objects.next().flatten(),
        "global selector repository root is absent",
    )?;
    let raw_branch_snapshot = projected_required(
        objects.next().flatten(),
        "branch selector snapshot is absent",
    )?;
    let repository_root =
        RepositoryRootV1::decode(global_selector.repository_root, &raw_repository_root)?;
    let branch_snapshot = BranchSnapshotV1::decode(
        branch_selector.branch_snapshot_object_id,
        &raw_branch_snapshot,
    )?;
    if branch_snapshot.branch_id != branch_id {
        return Err(corruption(
            "branch snapshot does not match the selected branch id",
        ));
    }
    authenticate_selected_graph(
        &read,
        global_selector.repository_root,
        branch_selector.branch_snapshot_object_id,
        repository_root,
        branch_snapshot,
    )
    .await?;
    let view_id = derive_view_id(&raw_global_selector, &raw_branch_selector);
    let view_instance_id = NEXT_VIEW_INSTANCE_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| corruption("coherent view instance identifier space is exhausted"))?;
    Ok(CoherentView {
        read,
        branch_id,
        raw_global_selector,
        raw_branch_selector,
        global_selector,
        branch_selector,
        repository_root,
        branch_snapshot,
        view_id,
        view_instance_id,
    })
}

pub(super) async fn load_object_bytes(
    read: &(impl StorageAdapterRead + ?Sized),
    id: ObjectId,
) -> Result<Bytes, StorageError> {
    let keys = [Key(Bytes::copy_from_slice(id.as_bytes()))];
    let loaded = read
        .get_many(&[GetManyRequest {
            space: OBJECT_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    projected_required(
        loaded.values.into_iter().next().flatten(),
        format!("object {id} is absent"),
    )
}

async fn authenticate_selected_graph<R>(
    read: &R,
    _repository_id: ObjectId,
    _branch_snapshot_id: ObjectId,
    repository: RepositoryRootV1,
    branch: BranchSnapshotV1,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    // Open authenticates only the selector pair and directly referenced root
    // envelopes. Every deeper point/range/history traversal uses the same
    // retained StorageRead and validates each child edge before output. This
    // keeps transaction open O(1) in repository size without weakening the
    // fail-closed boundary for any visited object.
    let mut ids = vec![
        repository.global_state_root,
        repository.commit_catalog_root,
        repository.change_catalog_root,
        repository.retention_policy_root,
        branch.local_state_root,
        branch.historical_global_state_root,
        branch.semantic_head_commit_object_id,
    ];
    if let Some(id) = branch.latest_ref_change_object_id {
        ids.push(id);
    }
    ids.sort_unstable();
    ids.dedup();
    let objects = load_object_map(read, ids).await?;
    for (id, kind) in [
        (repository.global_state_root, "state"),
        (branch.local_state_root, "state"),
        (branch.historical_global_state_root, "state"),
        (repository.commit_catalog_root, "commit"),
        (repository.change_catalog_root, "change"),
        (repository.retention_policy_root, "retention"),
    ] {
        super::tree::validate_root_bytes(id, kind, required_object(&objects, id)?)?;
    }
    let head = CommitObjectV1::decode(
        branch.semantic_head_commit_object_id,
        required_object(&objects, branch.semantic_head_commit_object_id)?,
    )?;
    if head.global_state_root != branch.historical_global_state_root
        || head.local_state_root != branch.local_state_root
    {
        return Err(corruption(
            "selected semantic head does not authenticate the branch/global state pair",
        ));
    }
    let Some(ref_id) = branch.latest_ref_change_object_id else {
        return Ok(());
    };
    let change = ChangeObjectV1::decode(ref_id, required_object(&objects, ref_id)?)?;
    let ChangeObjectV1::BranchRef {
        change_id,
        branch_id,
        before_semantic_head_commit_object_id: _,
        after_semantic_head_commit_object_id,
        previous_ref_change_object_id: _,
        ..
    } = change
    else {
        return Err(corruption(
            "branch snapshot latest ref-change edge names a semantic Change",
        ));
    };
    if branch_id != branch.branch_id
        || after_semantic_head_commit_object_id != Some(branch.semantic_head_commit_object_id)
    {
        return Err(corruption(
            "branch snapshot latest ref-change does not match its branch/head",
        ));
    }
    let _ = change_id;
    Ok(())
}

pub(super) async fn load_object_map<R>(
    read: &R,
    ids: impl IntoIterator<Item = ObjectId>,
) -> Result<BTreeMap<ObjectId, Bytes>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let ids = ids.into_iter().collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let keys = ids
        .iter()
        .map(|id| Key(Bytes::copy_from_slice(id.as_bytes())))
        .collect::<Vec<_>>();
    let loaded = read
        .get_many(&[GetManyRequest {
            space: OBJECT_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    if loaded.values.len() != ids.len() {
        return Err(corruption(
            "selected graph object read returned the wrong number of values",
        ));
    }
    ids.into_iter()
        .zip(loaded.values)
        .map(|(id, value)| {
            projected_required(value, format!("selected object {id} is absent"))
                .map(|bytes| (id, bytes))
        })
        .collect()
}

fn required_object(
    objects: &BTreeMap<ObjectId, Bytes>,
    id: ObjectId,
) -> Result<&Bytes, StorageError> {
    objects
        .get(&id)
        .ok_or_else(|| corruption(format!("selected object {id} is absent")))
}

fn derive_view_id(raw_global: &[u8], raw_branch: &[u8]) -> [u8; 32] {
    let mut encoder = Encoder::default();
    encoder
        .bytes(raw_global)
        .expect("selector value necessarily fits canonical u32 length");
    encoder
        .bytes(raw_branch)
        .expect("selector value necessarily fits canonical u32 length");
    keyed_hash(VIEW_ID_DOMAIN, &encoder.into_vec())
}

fn projected_required(
    value: Option<ProjectedValue>,
    missing: impl Into<String>,
) -> Result<Bytes, StorageError> {
    match value {
        Some(ProjectedValue::FullValue(bytes)) => Ok(bytes),
        Some(ProjectedValue::KeyOnly) => Err(corruption(
            "full-value projection returned a key-only value",
        )),
        None => Err(corruption(missing)),
    }
}
