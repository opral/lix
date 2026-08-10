use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::future::Future;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::entity_pk::EntityPk;
use crate::storage::{
    BeginScanOptions, CoreProjection, GetManyRequest, GetOptions, Key, KeyRange, ProjectedValue,
    ReadOptions, ScanCursor, ScanOrder, Storage, StorageError,
};
use crate::storage_adapter::{StorageAdapterRead, StorageAdapterReadScope};

use super::codec::{Encoder, corruption, keyed_hash};
use super::model::{
    BlobChunkV1, BranchSelectorV1, BranchSnapshotV1, CanonicalBranchId, ChangeCatalogEntry,
    ChangeId, ChangeObjectV1, CommitCatalogEntry, CommitId, CommitObjectV1, GlobalSelectorV1,
    RepositoryRootV1, branch_selector_key, global_selector_key,
};
use super::object::{OBJECT_SPACE, ObjectId};

pub(crate) const SELECTOR_SPACE: crate::storage::StorageSpace =
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

    #[cfg(test)]
    pub(crate) fn test_storage_read(&self) -> &R {
        &self.read
    }

    pub(crate) fn retained_read(&self) -> &R {
        &self.read
    }

    /// Builds the publication's temporary object overlay without exposing
    /// this view's retained storage handle to callers. Staged objects must
    /// still be read through the same authenticated view identity.
    pub(super) fn object_overlay<'a>(
        &'a self,
        objects: &'a super::tree::ImmutableObjectSet,
    ) -> super::serving::ObjectOverlayRead<'a, R> {
        super::serving::ObjectOverlayRead::new(&self.read, objects)
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

    pub(crate) async fn replace_state_tree_range(
        &self,
        root: ObjectId,
        lower: Vec<u8>,
        upper: Option<Vec<u8>>,
        replacement: Vec<(Vec<u8>, Vec<u8>, super::serving::StateMutationAudit)>,
    ) -> Result<super::serving::StateTreeEdit, StorageError> {
        super::serving::replace_state_tree_range(root, lower, upper, replacement, &self.read).await
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

    pub(crate) async fn load_object_bytes(&self, id: ObjectId) -> Result<Bytes, StorageError> {
        load_object_bytes(&self.read, id).await
    }

    pub(crate) async fn load_selector_value(
        &self,
        key: &[u8],
    ) -> Result<Option<Bytes>, StorageError> {
        let loaded = self
            .read
            .get_many(&[GetManyRequest {
                space: SELECTOR_SPACE,
                keys: &[Key(key.to_vec().into())],
                opts: GetOptions {
                    projection: CoreProjection::FullValue,
                },
            }])
            .await?;
        match loaded.values.as_slice() {
            [None] => Ok(None),
            [Some(ProjectedValue::FullValue(bytes))] => Ok(Some(bytes.clone())),
            [Some(ProjectedValue::KeyOnly)] => {
                Err(corruption("ForkTree selector read returned key-only data"))
            }
            _ => Err(corruption("ForkTree selector read cardinality is invalid")),
        }
    }

    pub(crate) async fn validate_receipt_root(
        &self,
        root: super::tree::ReceiptTreeRoot,
    ) -> Result<(), StorageError> {
        super::tree::validate_receipt_root_on_read(root, &self.read).await
    }

    pub(crate) async fn insert_receipt_part(
        &self,
        root: super::tree::ReceiptTreeRoot,
        part_object_id: ObjectId,
        part: &super::model::UploadPartV1,
        overlay: &super::tree::ImmutableObjectSet,
    ) -> Result<super::tree::ReceiptTreeEdit, StorageError> {
        super::tree::insert_receipt_part_on_read(root, part_object_id, part, &self.read, overlay)
            .await
    }

    pub(super) async fn authenticate_chunk(
        &self,
        chunk_ref: &super::model::BlobChunkRefV1,
        part_hasher: &mut blake3::Hasher,
        final_hasher: &mut blake3::Hasher,
    ) -> Result<[u8; 32], StorageError> {
        super::blob::authenticate_chunk(&self.read, chunk_ref, part_hasher, final_hasher).await
    }

    pub(super) async fn load_blob_bytes_many_on_view(
        &self,
        refs: &[super::blob::AuthenticatedBlobRef],
    ) -> Result<crate::binary_cas::BlobBytesBatch, crate::LixError> {
        super::blob::load_blob_bytes_many_on_read(
            &self.read,
            self.branch_id(),
            self.view_id(),
            self.view_instance_id(),
            refs,
        )
        .await
    }

    pub(super) async fn load_blob_merkle_proof(
        &self,
        manifest: super::model::BlobManifestV1,
        state_key: &super::state::StateKey,
        leaf_range: std::ops::Range<u64>,
    ) -> Result<super::merkle::BlobMerkleProofV1, StorageError> {
        super::merkle::load_blob_merkle_range_proof(&self.read, manifest, state_key, leaf_range)
            .await
    }

    pub(super) async fn build_blob_merkle_edit_successor(
        &self,
        manifest: super::model::BlobManifestV1,
        payload: &[u8],
        offset: usize,
        delete_len: usize,
        insert_len: usize,
    ) -> Result<super::merkle::BlobMerkleTreeBuild, StorageError> {
        super::merkle::build_blob_merkle_edit_successor(
            &self.read, manifest, payload, offset, delete_len, insert_len,
        )
        .await
    }

    pub(super) async fn load_blob_ranges_many_on_view(
        &self,
        requests: &[(super::blob::AuthenticatedBlobRef, std::ops::Range<u64>)],
    ) -> Result<crate::binary_cas::BlobRangeBytesBatch, crate::LixError> {
        super::blob::load_blob_ranges_many_on_read(
            &self.read,
            self.branch_id(),
            self.view_id(),
            self.view_instance_id(),
            requests,
        )
        .await
    }

    pub(crate) async fn lookup_tree_value(
        &self,
        root: ObjectId,
        expected_kind: &'static str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        super::tree::lookup_on_read(root, expected_kind, key, &self.read).await
    }

    pub(crate) async fn load_commit_members(
        &self,
        commit: &CommitObjectV1,
    ) -> Result<Vec<super::model::CommitMemberV1>, StorageError> {
        super::serving::load_commit_members(&self.read, commit).await
    }

    pub(crate) async fn scan_tree_page(
        &self,
        root: ObjectId,
        expected_kind: &'static str,
        start_after: Option<&[u8]>,
        page_size: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        super::tree::scan_page_on_read(root, expected_kind, start_after, page_size, &self.read)
            .await
    }

    pub(crate) async fn validate_member_catalog_owner(
        &self,
        commit_catalog_root: ObjectId,
        target_commit_object_id: ObjectId,
        target_generation: u64,
        target_ordinal: usize,
        member: super::model::CommitMemberV1,
        entry: ChangeCatalogEntry,
    ) -> Result<(), StorageError> {
        super::serving::validate_member_catalog_owner(
            &self.read,
            commit_catalog_root,
            target_commit_object_id,
            target_generation,
            target_ordinal,
            member,
            entry,
        )
        .await
    }

    pub(crate) async fn validate_retained_commit(
        &self,
        commit_catalog_root: ObjectId,
        change_catalog_root: ObjectId,
        commit_object_id: ObjectId,
        commit: &CommitObjectV1,
    ) -> Result<(), StorageError> {
        super::serving::validate_retained_commit(
            &self.read,
            commit_catalog_root,
            change_catalog_root,
            commit_object_id,
            commit,
        )
        .await
    }

    pub(crate) async fn validate_commit_topology(
        &self,
        commit_catalog_root: ObjectId,
        commit_id: CommitId,
        commit: &CommitObjectV1,
    ) -> Result<super::serving::CommitTopology, crate::LixError> {
        super::serving::validate_commit_topology(&self.read, commit_catalog_root, commit_id, commit)
            .await
    }

    pub(crate) async fn state_range_at_roots(
        &self,
        global_root: ObjectId,
        local_root: Option<ObjectId>,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
        include_tombstones: bool,
    ) -> Result<
        Vec<(
            Vec<u8>,
            super::state::StateValue,
            super::serving::StateSource,
        )>,
        StorageError,
    > {
        super::serving::state_range_on_roots(
            global_root,
            local_root,
            &self.read,
            lower,
            upper,
            limit,
            include_tombstones,
        )
        .await
    }

    /// Resolves exact canonical state keys through this view's retained read.
    /// The ordered-tree lookup batches shared authenticated paths, giving
    /// `O(K log_F N)` work for `K` requested keys without a repository scan.
    pub(crate) async fn points(
        &self,
        keys: &[Vec<u8>],
        include_tombstone: bool,
    ) -> Result<Vec<Option<super::serving::VisibleStateRow>>, StorageError> {
        super::serving::state_points(self, keys, include_tombstone).await
    }

    /// Resolves a canonical half-open state-key range through this view's
    /// retained read. The ordered-tree descent skips unrelated subtrees and
    /// returns at most `limit` rows.
    pub(crate) async fn range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: Option<usize>,
        include_tombstones: bool,
    ) -> Result<Vec<super::serving::VisibleStateRow>, StorageError> {
        super::serving::state_range(self, lower, upper, limit, include_tombstones).await
    }

    /// Resolves disjoint canonical ranges through one retained authenticated
    /// tree walk. Output slots correspond to input ranges and each slot is in
    /// intrinsic state-key order.
    pub(crate) async fn ranges(
        &self,
        ranges: &[(Vec<u8>, Option<Vec<u8>>)],
        include_tombstones: bool,
    ) -> Result<Vec<Vec<super::serving::VisibleStateRow>>, StorageError> {
        super::serving::state_ranges(self, ranges, include_tombstones).await
    }

    pub(crate) async fn branch_view(
        &self,
        branch_id: CanonicalBranchId,
    ) -> Result<CoherentView<&R>, StorageError> {
        open_coherent_view_on_read(&self.read, branch_id).await
    }

    async fn checkpoint_history(
        &self,
        min_depth: Option<u32>,
        max_depth: Option<u32>,
        limit: Option<usize>,
    ) -> Result<CheckpointBranchHistory, crate::LixError> {
        let head_id = self.branch_snapshot.semantic_head_commit_object_id;
        let commit_catalog_root = self.repository_root.commit_catalog_root;
        let (head, head_record) = super::serving::load_checkpoint_commit_envelope(
            &self.read,
            commit_catalog_root,
            head_id,
        )
        .await?;
        let (root_id, head_distance_to_root) = head.checkpoint_cursor.root_edge(head_id);
        let head_commit_id = head_record.commit_id;
        let (mut current_id, mut depth) = head
            .checkpoint_cursor
            .latest_for_branch(head_id, self.branch_id);
        if depth > head_distance_to_root {
            return Err(corruption(
                "checkpoint chronology target exceeds the authenticated root distance",
            )
            .into());
        }

        let mut current = if current_id == head_id {
            (head, head_record)
        } else {
            super::serving::load_checkpoint_commit_envelope(
                &self.read,
                commit_catalog_root,
                current_id,
            )
            .await?
        };
        let mut visited = HashSet::new();
        let mut history = Vec::new();
        loop {
            if !visited.insert(current_id) {
                return Err(corruption("checkpoint chronology contains a cycle").into());
            }
            let (current_root_id, current_distance_to_root) =
                current.0.checkpoint_cursor.root_edge(current_id);
            if current_root_id != root_id
                || current_distance_to_root
                    .checked_add(depth)
                    .is_none_or(|distance| distance != head_distance_to_root)
            {
                return Err(corruption(
                    "checkpoint chronology edge disagrees with the authenticated root distance",
                )
                .into());
            }
            match current.0.checkpoint_cursor {
                super::model::CheckpointCursorV1::Root if current_id == root_id => {}
                super::model::CheckpointCursorV1::Checkpoint {
                    owner_branch_id, ..
                } if owner_branch_id == self.branch_id => {}
                super::model::CheckpointCursorV1::Root => {
                    return Err(corruption(
                        "checkpoint chronology reached a substituted repository root",
                    )
                    .into());
                }
                super::model::CheckpointCursorV1::Ordinary { .. }
                | super::model::CheckpointCursorV1::Checkpoint { .. } => {
                    return Err(corruption(
                        "checkpoint chronology target is not owned by the selected branch",
                    )
                    .into());
                }
            }

            if max_depth.is_some_and(|maximum| depth > maximum) {
                break;
            }
            if min_depth.is_none_or(|minimum| depth >= minimum) {
                history.push(CheckpointHistoryEntry {
                    commit_id: current.1.commit_id,
                    created_at: current.1.created_at.to_string(),
                    depth,
                });
                if limit.is_some_and(|limit| history.len() >= limit) {
                    break;
                }
            }

            let Some((next_id, distance)) = current.0.checkpoint_cursor.previous_checkpoint()
            else {
                break;
            };
            depth = depth
                .checked_add(distance)
                .ok_or_else(|| corruption("checkpoint chronology depth overflowed"))?;
            if depth > head_distance_to_root {
                return Err(corruption(
                    "checkpoint chronology previous edge exceeds the repository root",
                )
                .into());
            }
            current_id = next_id;
            current = super::serving::load_checkpoint_commit_envelope(
                &self.read,
                commit_catalog_root,
                current_id,
            )
            .await?;
        }
        Ok(CheckpointBranchHistory {
            head_commit_id,
            entries: history,
        })
    }
}

/// One authenticated historical commit on an operation-owned retained read.
/// Construction validates the commit/catalog/member closure exactly once;
/// later exact point batches can only traverse the two bound state roots.
pub(crate) struct AuthenticatedHistoricalStateView<'a, R: ?Sized> {
    read: &'a R,
    global_state_root: ObjectId,
    local_state_root: ObjectId,
}

impl<R> AuthenticatedHistoricalStateView<'_, R>
where
    R: StorageAdapterRead + ?Sized,
{
    pub(crate) async fn load_state_value(
        &self,
        key: &[u8],
        include_tombstone: bool,
    ) -> Result<Option<(super::state::StateValue, super::serving::StateSource)>, crate::LixError>
    {
        let mut values = super::serving::state_points_on_read(
            self.global_state_root,
            Some(self.local_state_root),
            &[key.to_vec()],
            include_tombstone,
            self.read,
        )
        .await?;
        Ok(values.pop().flatten())
    }

    pub(crate) async fn load_state_rows(
        &self,
        keys: &[super::state::StateKey],
    ) -> Result<Vec<Option<super::state::HistoricalStateRow>>, crate::LixError> {
        let encoded_keys = keys
            .iter()
            .map(|key| {
                super::state::encode_state_key(super::state::StateKeyRef {
                    schema_key: &key.schema_key,
                    file_id: key.file_id.as_deref(),
                    entity_pk: &key.entity_pk,
                })
            })
            .collect::<Vec<_>>();
        let values = super::serving::state_points_on_read(
            self.global_state_root,
            Some(self.local_state_root),
            &encoded_keys,
            true,
            self.read,
        )
        .await?;
        Ok(keys
            .iter()
            .zip(values)
            .map(|(key, value)| {
                value.map(|(value, source)| {
                    let (snapshot_content, deleted) = match value.cell {
                        super::state::StateCell::Value(snapshot) => (Some(snapshot), false),
                        super::state::StateCell::Null => (None, false),
                        super::state::StateCell::Tombstone => (None, true),
                    };
                    super::state::HistoricalStateRow {
                        key: key.clone(),
                        global: source == super::serving::StateSource::Global,
                        snapshot_content,
                        metadata: value.metadata,
                        deleted,
                        blob_manifest_object_ids: value.blob_manifest_object_ids,
                        created_at: value.created_at,
                        updated_at: value.updated_at,
                        change_id: value.change_id,
                        commit_id: value.commit_id,
                    }
                })
            })
            .collect())
    }
}

/// One operation-scoped ForkTree read facade. Branch views borrow the same
/// retained read identity; no branch or untracked traversal can refresh it.
#[derive(Clone)]
pub(crate) struct ForkTreeReadFacade<R> {
    read: R,
    operation_id: u64,
}

/// Let snapshot-bound ForkTree serving primitives consume the operation-owned
/// facade without exposing or reacquiring its underlying read handle.
impl<R> StorageAdapterRead for ForkTreeReadFacade<R>
where
    R: StorageAdapterRead,
{
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.read.snapshot_cache_key()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<crate::storage::GetManyResult, StorageError>> + Send {
        self.read.get_many(requests)
    }

    fn begin_scan(
        &self,
        space: crate::storage::StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        self.read.begin_scan(space, range, opts)
    }
}

/// ForkTree-owned first-parent checkpoint chronology. The state marker is
/// authenticated from the same retained read as the commit envelope; an
/// inherited marker never reclassifies a descendant ordinary commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointHistoryEntry {
    pub(crate) commit_id: crate::changelog::CommitId,
    pub(crate) created_at: String,
    pub(crate) depth: u32,
}

pub(crate) struct CheckpointBranchHistory {
    pub(crate) head_commit_id: crate::changelog::CommitId,
    pub(crate) entries: Vec<CheckpointHistoryEntry>,
}

pub(crate) struct CheckpointBranchBaseline {
    pub(crate) head_commit_id: crate::changelog::CommitId,
    pub(crate) checkpoint_commit_id: crate::changelog::CommitId,
}

impl<R> ForkTreeReadFacade<R>
where
    R: StorageAdapterRead,
{
    pub(crate) fn new(read: R) -> Self {
        let operation_id = NEXT_VIEW_INSTANCE_ID.fetch_add(1, Ordering::Relaxed);
        Self { read, operation_id }
    }

    pub(super) fn operation_id(&self) -> u64 {
        self.operation_id
    }

    pub(crate) async fn historical_state_view(
        &self,
        commit_id: &str,
    ) -> Result<AuthenticatedHistoricalStateView<'_, R>, crate::LixError> {
        let commit_id = crate::changelog::CommitId::parse_lix(commit_id, "historical commit")?;
        let (global_state_root, local_state_root) =
            super::serving::authenticate_historical_state_roots(&self.read, commit_id).await?;
        Ok(AuthenticatedHistoricalStateView {
            read: &self.read,
            global_state_root,
            local_state_root,
        })
    }

    /// Consumes this operation-owned facade into one retained authenticated
    /// branch view. The read handle is moved, not reacquired, so the state
    /// boundary can retain selector/root identity for every later point or
    /// range operation.
    pub(crate) async fn into_branch(
        self,
        branch_id: &str,
    ) -> Result<CoherentView<R>, crate::LixError> {
        let uuid = uuid::Uuid::parse_str(branch_id).map_err(|error| {
            crate::LixError::branch_not_found(
                branch_id,
                "open ForkTree branch view",
                format!("branch ID must be a UUID: {error}"),
            )
        })?;
        open_coherent_view_on_read(self.read, CanonicalBranchId::from_bytes(*uuid.as_bytes()))
            .await
            .map_err(crate::LixError::from)
    }

    /// Loads one authenticated collection-generation marker from the
    /// operation-owned branch view. This is a native marker lookup, not a
    /// compatibility scan or a second reader acquisition.
    pub(crate) async fn collection_generation(
        &self,
        branch_id: &str,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<crate::collection_generation::CollectionGeneration>, crate::LixError> {
        let expected_scope = crate::collection_generation::collection_scope_key(scope);
        let entity_pk = EntityPk::single(&expected_scope);
        let key = super::state::encode_state_key(super::state::StateKeyRef {
            schema_key: crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY,
            file_id: None,
            entity_pk: &entity_pk,
        });
        let row = self
            .branch(branch_id)
            .await?
            .points(&[key], true)
            .await
            .map_err(crate::LixError::from)?
            .pop()
            .flatten();
        let Some(row) = row else {
            return Ok(None);
        };
        let decoded = super::state::decode_state_key(&row.encoded_key)?;
        if decoded.schema_key != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
            || decoded.file_id.is_some()
            || decoded.entity_pk != entity_pk
        {
            return Err(crate::LixError::new(
                crate::LixError::CODE_STORAGE_ERROR,
                "collection generation row identity does not match its requested scope",
            ));
        }
        let snapshot = match row.value.cell {
            super::state::StateCell::Value(value) => value,
            super::state::StateCell::Null | super::state::StateCell::Tombstone => return Ok(None),
        };
        let snapshot =
            serde_json::from_str::<serde_json::Value>(snapshot.as_str()).map_err(|error| {
                crate::LixError::new(
                    crate::LixError::CODE_STORAGE_ERROR,
                    format!("collection generation row is malformed: {error}"),
                )
            })?;
        if snapshot
            .get("scope_key")
            .and_then(serde_json::Value::as_str)
            != Some(expected_scope.as_str())
            || snapshot
                .get("schema_key")
                .and_then(serde_json::Value::as_str)
                != Some(scope.schema_key)
            || snapshot.get("file_id").and_then(serde_json::Value::as_str) != scope.file_id
        {
            return Err(crate::LixError::new(
                crate::LixError::CODE_STORAGE_ERROR,
                "collection generation row identity does not match its requested scope",
            ));
        }
        let live_count = snapshot
            .get("live_count")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                crate::LixError::new(
                    crate::LixError::CODE_STORAGE_ERROR,
                    "collection generation row is missing live_count",
                )
            })?;
        Ok(Some(crate::collection_generation::CollectionGeneration {
            active_generation: row.value.commit_id,
            live_count,
            ordered_identity_digest: None,
        }))
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
                .map_err(|error| match error {
                    StorageError::Corruption(message)
                        if message.ends_with("requested branch selector is absent") =>
                    {
                        crate::LixError::branch_not_found(
                            branch_id.to_owned(),
                            "open ForkTree branch view",
                            "branch selector",
                        )
                    }
                    error => error.into(),
                })?;
        let _view_identity = view.view_id();
        Ok(view)
    }

    pub(crate) async fn load_commit_member_records(
        &self,
        commit_id: crate::changelog::CommitId,
    ) -> Result<Option<Vec<crate::changelog::ChangeRecord>>, crate::LixError> {
        super::serving::load_commit_member_records(&self.read, commit_id).await
    }

    pub(crate) async fn load_commit_member_sources(
        &self,
        commit_id: crate::changelog::CommitId,
    ) -> Result<
        Option<Vec<(crate::changelog::CommitId, crate::changelog::ChangeRecord)>>,
        crate::LixError,
    > {
        super::serving::load_commit_member_sources(&self.read, commit_id).await
    }

    pub(crate) async fn load_change_records(
        &self,
        ids: &[crate::changelog::ChangeId],
    ) -> Result<Vec<Option<crate::changelog::ChangeRecord>>, crate::LixError> {
        super::serving::load_change_records(&self.read, ids).await
    }

    pub(crate) async fn scan_change_records(
        &self,
        start_after: Option<crate::changelog::ChangeId>,
        limit: usize,
    ) -> Result<Vec<crate::changelog::ChangeRecord>, crate::LixError> {
        super::serving::scan_change_records(&self.read, start_after, limit).await
    }

    pub(crate) async fn scan_commit_records(
        &self,
        start_after: Option<crate::changelog::CommitId>,
        limit: usize,
    ) -> Result<Vec<crate::changelog::CommitRecord>, crate::LixError> {
        super::serving::scan_commit_records(&self.read, start_after, limit).await
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
        self.historical_state_view(commit_id)
            .await?
            .load_state_rows(keys)
            .await
    }

    /// Loads historical file payloads from exact state keys through this
    /// facade's retained read. The state value, manifest edge, and payload
    /// are authenticated together; no BlobId-only reader is involved.
    pub(crate) async fn load_historical_blob_bytes_for_rows(
        &self,
        requests: &[(String, super::state::StateKey)],
    ) -> Result<crate::binary_cas::BlobBytesBatch, crate::LixError> {
        if requests.is_empty() {
            return Ok(crate::binary_cas::BlobBytesBatch::new(Vec::new()));
        }
        let mut values = Vec::with_capacity(requests.len());
        for (commit_id, key) in requests {
            let commit_id =
                crate::changelog::CommitId::parse_lix(commit_id, "historical blob commit")?;
            let encoded_key = super::state::encode_state_key(super::state::StateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            });
            let value = self
                .load_state_value_at_commit(commit_id, &encoded_key, true)
                .await?
                .map(|(value, _source)| value)
                .ok_or_else(|| {
                    crate::LixError::new(
                        crate::LixError::CODE_STORAGE_ERROR,
                        "historical BlobRef state row is absent",
                    )
                })?;
            values.push((key.clone(), value));
        }
        super::blob::load_historical_blob_bytes_for_state_values(&self.read, &values).await
    }

    /// Loads the state rows authored by one authenticated semantic commit.
    /// Commit membership and the final state row are authenticated together:
    /// a missing row, substituted key, or row owned by another change/commit
    /// is corruption rather than an absent value. `schema_keys` is only a
    /// projection filter; it never changes the ownership checks.
    pub(crate) async fn load_commit_delta_rows(
        &self,
        commit_id: crate::changelog::CommitId,
        schema_keys: Option<&[&str]>,
    ) -> Result<Vec<super::state::HistoricalStateRow>, crate::LixError> {
        let members = self
            .load_commit_member_records(commit_id)
            .await?
            .ok_or_else(|| {
                crate::LixError::new(
                    crate::LixError::CODE_COMMIT_NOT_FOUND,
                    format!("commit '{commit_id}' has no authenticated member records"),
                )
            })?;
        let mut keys = Vec::with_capacity(members.len());
        let mut seen = BTreeMap::new();
        for member in members.iter().filter(|member| {
            schema_keys.is_none_or(|schemas| {
                schemas
                    .iter()
                    .any(|schema_key| *schema_key == member.schema_key)
            })
        }) {
            let key = super::state::StateKey {
                schema_key: member.schema_key.clone(),
                file_id: member.file_id.clone(),
                entity_pk: member.entity_pk.clone(),
            };
            if seen.insert(key.clone(), ()).is_some() {
                return Err(crate::LixError::new(
                    crate::LixError::CODE_INTERNAL_ERROR,
                    format!("commit '{commit_id}' has duplicate state member identity"),
                ));
            }
            keys.push(key);
        }
        let rows = self
            .load_state_rows_at_commit(&commit_id.to_string(), &keys)
            .await?;
        if rows.len() != keys.len() {
            return Err(crate::LixError::new(
                crate::LixError::CODE_INTERNAL_ERROR,
                format!("commit '{commit_id}' returned an incomplete state delta"),
            ));
        }
        let mut delta = Vec::with_capacity(keys.len());
        for ((member, key), row) in members
            .iter()
            .filter(|member| {
                schema_keys.is_none_or(|schemas| {
                    schemas
                        .iter()
                        .any(|schema_key| *schema_key == member.schema_key)
                })
            })
            .zip(keys)
            .zip(rows)
        {
            let row = row.ok_or_else(|| {
                crate::LixError::new(
                    crate::LixError::CODE_INTERNAL_ERROR,
                    format!("commit '{commit_id}' is missing an authenticated state member"),
                )
            })?;
            if row.key != key || row.change_id != member.change_id || row.commit_id != commit_id {
                return Err(crate::LixError::new(
                    crate::LixError::CODE_INTERNAL_ERROR,
                    format!("commit '{commit_id}' has a substituted state member"),
                ));
            }
            delta.push(row);
        }
        Ok(delta)
    }

    /// Loads the complete authenticated historical state overlay through this
    /// facade's retained read. Callers may project the returned ForkTree-owned
    /// rows into their public DTOs, but may not acquire a second state reader for
    /// the same commit.
    pub(crate) async fn scan_state_rows_at_commit(
        &self,
        commit_id: crate::changelog::CommitId,
    ) -> Result<Vec<super::state::HistoricalStateRow>, crate::LixError> {
        super::serving::scan_state_rows_at_commit(&self.read, commit_id).await
    }

    pub(crate) async fn scan_state_rows_at_commit_range(
        &self,
        commit_id: crate::changelog::CommitId,
        lower: &[u8],
        upper: Option<&[u8]>,
    ) -> Result<Vec<super::state::HistoricalStateRow>, crate::LixError> {
        super::serving::scan_state_rows_at_commit_range(&self.read, commit_id, lower, upper).await
    }

    /// Diffs two authenticated historical state roots through this facade's
    /// retained read. The neutral result is the sole source for checkpoint
    /// and working-diff projections; callers may filter and project it but
    /// may not reopen a parallel state reader for the same interval.
    pub(crate) async fn diff_state_rows_between_commits(
        &self,
        before: crate::changelog::CommitId,
        after: crate::changelog::CommitId,
    ) -> Result<Vec<super::state::HistoricalStateDiffEntry>, crate::LixError> {
        Ok(
            stale_state_changes_between_commits_on_read(self, before, after, true)
                .await?
                .complete,
        )
    }

    /// Diffs only the branch-local state domain for checkpoint and working
    /// diff projections. Global rows remain visible through the complete
    /// historical API above, but are not re-staged into a branch checkpoint.
    pub(crate) async fn diff_branch_state_rows_between_commits(
        &self,
        before: crate::changelog::CommitId,
        after: crate::changelog::CommitId,
    ) -> Result<Vec<super::state::HistoricalStateDiffEntry>, crate::LixError> {
        diff_branch_state_rows_between_commits_on_read(&self.read, before, after).await
    }

    /// Returns authenticated state-write identity changes separately from
    /// endpoint payload differences. Both scans use this facade's retained
    /// read and therefore inherit the same catalog, root, and member
    /// fail-closed validation as the payload path.
    pub(crate) async fn touched_state_identities_between_commits(
        &self,
        before: crate::changelog::CommitId,
        after: crate::changelog::CommitId,
    ) -> Result<Vec<super::state::HistoricalStateIdentityChange>, crate::LixError> {
        Ok(
            stale_state_changes_between_commits_on_read(self, before, after, true)
                .await?
                .identities,
        )
    }

    /// Returns payload and write-identity changes from one authenticated root
    /// diff. Stale reconciliation consumes both projections from this single
    /// traversal so equal payloads with a new ChangeId remain visible without
    /// rescanning either endpoint.
    pub(crate) async fn stale_state_changes_between_commits(
        &self,
        before: crate::changelog::CommitId,
        after: crate::changelog::CommitId,
    ) -> Result<StaleStateChanges, crate::LixError> {
        stale_state_changes_between_commits_on_read(self, before, after, true).await
    }

    /// Loads the authenticated commit summary used by stale reconciliation.
    /// The cache is scoped by the caller to this exact retained view/read.
    pub(super) async fn load_stale_commit_state_roots(
        &self,
        repository: &RepositoryRootV1,
        commit_id: crate::changelog::CommitId,
        cache: &mut super::serving::StaleCommitSummaryCache,
    ) -> Result<super::serving::StaleCommitSummary, crate::LixError> {
        super::serving::load_historical_commit_state_roots_for_stale(
            &self.read,
            self.operation_id,
            repository,
            commit_id,
            cache,
        )
        .await
    }

    /// Resolves one required semantic commit record from the authenticated
    /// catalog on this facade's retained read.
    pub(crate) async fn load_required_commit_record(
        &self,
        commit_id: crate::changelog::CommitId,
    ) -> Result<crate::changelog::CommitRecord, crate::LixError> {
        super::serving::load_required_commit_record(&self.read, commit_id).await
    }

    /// Returns authenticated first-parent checkpoint history and its head from
    /// one selector-bound branch view. The caller cannot supply a detached head
    /// or cursor. Unfiltered LIMIT 1 follows one sealed chronology edge,
    /// independent of ordinary history height. Filtered history visits only
    /// authenticated checkpoint edges, but may skip entries before satisfying
    /// a minimum-depth predicate.
    pub(crate) async fn checkpoint_history_for_branch(
        &self,
        branch_id: &str,
        min_depth: Option<u32>,
        max_depth: Option<u32>,
        limit: Option<usize>,
    ) -> Result<CheckpointBranchHistory, crate::LixError> {
        if limit == Some(0) {
            let view = self.branch(branch_id).await?;
            let head_id = view.branch_snapshot.semantic_head_commit_object_id;
            let (_, head_record) = super::serving::load_checkpoint_commit_envelope(
                &view.read,
                view.repository_root.commit_catalog_root,
                head_id,
            )
            .await?;
            return Ok(CheckpointBranchHistory {
                head_commit_id: head_record.commit_id,
                entries: Vec::new(),
            });
        }
        self.branch(branch_id)
            .await?
            .checkpoint_history(min_depth, max_depth, limit)
            .await
    }

    pub(crate) async fn checkpoint_baseline_for_branch(
        &self,
        branch_id: &str,
    ) -> Result<CheckpointBranchBaseline, crate::LixError> {
        let history = self
            .checkpoint_history_for_branch(branch_id, None, None, Some(1))
            .await?;
        let checkpoint_commit_id = history
            .entries
            .into_iter()
            .next()
            .ok_or_else(|| corruption("selected branch has no checkpoint baseline"))?
            .commit_id;
        Ok(CheckpointBranchBaseline {
            head_commit_id: history.head_commit_id,
            checkpoint_commit_id,
        })
    }

    pub(crate) async fn load_json_slot(
        &self,
        slot: &crate::json_store::JsonSlot,
    ) -> Result<Option<String>, crate::LixError> {
        match slot {
            crate::json_store::JsonSlot::None => Ok(None),
            crate::json_store::JsonSlot::Inline(value) => Ok(Some(value.to_string())),
            crate::json_store::JsonSlot::ForkTreeObject(object_id) => {
                let id = ObjectId::from_bytes(*object_id);
                let bytes = load_object_bytes(&self.read, id).await?;
                let chunk = BlobChunkV1::decode(id, &bytes)?;
                String::from_utf8(chunk.bytes.to_vec())
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

#[derive(Debug)]
pub(crate) struct StaleStateChanges {
    pub(crate) payload: Vec<super::state::HistoricalStateDiffEntry>,
    /// Complete public historical changes, including authenticated write
    /// identity-only changes whose endpoint payloads are equal.
    pub(crate) complete: Vec<super::state::HistoricalStateDiffEntry>,
    pub(crate) identities: Vec<super::state::HistoricalStateIdentityChange>,
}

async fn stale_state_changes_between_commits_on_read<R>(
    view: &ForkTreeReadFacade<R>,
    before: crate::changelog::CommitId,
    after: crate::changelog::CommitId,
    include_global: bool,
) -> Result<StaleStateChanges, crate::LixError>
where
    R: StorageAdapterRead,
{
    let repository = super::serving::load_repository_root(&view.read).await?;
    let mut summary_cache = super::serving::StaleCommitSummaryCache::new(view.operation_id());
    let before_roots = view
        .load_stale_commit_state_roots(&repository, before, &mut summary_cache)
        .await?;
    if before == after {
        return Ok(StaleStateChanges {
            payload: Vec::new(),
            complete: Vec::new(),
            identities: Vec::new(),
        });
    }
    let after_roots = view
        .load_stale_commit_state_roots(&repository, after, &mut summary_cache)
        .await?;
    let local_changes = super::tree::diff_roots(
        Some(before_roots.local_state_root()),
        Some(after_roots.local_state_root()),
        &view.read,
    )
    .await?;
    let global_changes = super::tree::diff_roots(
        Some(before_roots.global_state_root()),
        Some(after_roots.global_state_root()),
        &view.read,
    )
    .await?;
    let keys = merge_sorted_state_keys(local_changes, global_changes);
    if keys.is_empty() {
        return Ok(StaleStateChanges {
            payload: Vec::new(),
            complete: Vec::new(),
            identities: Vec::new(),
        });
    }

    let encoded = keys
        .iter()
        .map(|key| {
            super::state::encode_state_key(super::state::StateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            })
        })
        .collect::<Vec<_>>();
    let before_rows = super::serving::state_points_on_read_for_stale(
        &repository,
        before_roots,
        &encoded,
        true,
        view.operation_id(),
        &view.read,
    )
    .await?;
    let after_rows = super::serving::state_points_on_read_for_stale(
        &repository,
        after_roots,
        &encoded,
        true,
        view.operation_id(),
        &view.read,
    )
    .await?;

    let mut payload = Vec::new();
    let mut complete = Vec::new();
    let mut identities = Vec::new();
    for ((key, before), after) in keys.into_iter().zip(before_rows).zip(after_rows) {
        let before = historical_state_row_from_point(key.clone(), before, include_global)?;
        let after = historical_state_row_from_point(key, after, include_global)?;
        let payload_changed = historical_state_payloads_differ(before.as_ref(), after.as_ref());
        let identity_changed = historical_state_identity_changed(before.as_ref(), after.as_ref());
        let entry = super::state::HistoricalStateDiffEntry { before, after };
        if payload_changed {
            payload.push(entry.clone());
        }
        if historical_state_change_is_public(payload_changed, identity_changed) {
            complete.push(entry.clone());
        }
        if identity_changed {
            let row = entry
                .after
                .as_ref()
                .or(entry.before.as_ref())
                .expect("identity change has an endpoint");
            let identity = |row: &super::state::HistoricalStateRow| {
                super::state::HistoricalStateWriteIdentity {
                    change_id: row.change_id,
                    commit_id: row.commit_id,
                }
            };
            identities.push(super::state::HistoricalStateIdentityChange {
                key: row.key.clone(),
                before: entry.before.as_ref().map(identity),
                after: entry.after.as_ref().map(identity),
            });
        }
    }
    Ok(StaleStateChanges {
        payload,
        complete,
        identities,
    })
}

fn historical_state_row_from_point(
    key: super::state::StateKey,
    point: Option<(super::state::StateValue, super::serving::StateSource)>,
    include_global: bool,
) -> Result<Option<super::state::HistoricalStateRow>, crate::LixError> {
    let Some((value, source)) = point else {
        return Ok(None);
    };
    if !include_global && source == super::serving::StateSource::Global {
        return Ok(None);
    }
    let (snapshot_content, deleted) = match value.cell {
        super::state::StateCell::Value(snapshot) => (Some(snapshot), false),
        super::state::StateCell::Null => (None, false),
        super::state::StateCell::Tombstone => (None, true),
    };
    Ok(Some(super::state::HistoricalStateRow {
        key,
        global: source == super::serving::StateSource::Global,
        snapshot_content,
        metadata: value.metadata,
        deleted,
        blob_manifest_object_ids: value.blob_manifest_object_ids,
        created_at: value.created_at,
        updated_at: value.updated_at,
        change_id: value.change_id,
        commit_id: value.commit_id,
    }))
}

pub(super) fn merge_sorted_state_keys(
    left: Vec<super::state::StateKey>,
    right: Vec<super::state::StateKey>,
) -> Vec<super::state::StateKey> {
    // `diff_roots` returns keys in their canonical encoded byte order
    // (schema, entity_pk, file_id), while `StateKey::Ord` follows the Rust
    // field order (schema, file_id, entity_pk).  Merge the canonical bytes,
    // not the incidental struct order, so local/global overlays stay ordered
    // and duplicate physical acquisition of one key is collapsed.
    let mut merged = left.into_iter().chain(right).collect::<Vec<_>>();
    merged.sort_unstable_by(|left, right| {
        super::state::encode_state_key(super::state::StateKeyRef {
            schema_key: &left.schema_key,
            file_id: left.file_id.as_deref(),
            entity_pk: &left.entity_pk,
        })
        .cmp(&super::state::encode_state_key(super::state::StateKeyRef {
            schema_key: &right.schema_key,
            file_id: right.file_id.as_deref(),
            entity_pk: &right.entity_pk,
        }))
    });
    merged.dedup_by(|left, right| {
        super::state::encode_state_key(super::state::StateKeyRef {
            schema_key: &left.schema_key,
            file_id: left.file_id.as_deref(),
            entity_pk: &left.entity_pk,
        }) == super::state::encode_state_key(super::state::StateKeyRef {
            schema_key: &right.schema_key,
            file_id: right.file_id.as_deref(),
            entity_pk: &right.entity_pk,
        })
    });
    merged
}

/// Diffs the authenticated branch-local roots and resolves only the changed
/// keys against both complete global/local overlays. The structural diff is
/// the discovery authority; exact point batches provide the endpoint values,
/// so a local add/remove still reveals or re-masks its global fallback without
/// scanning either historical state tree.
async fn diff_branch_state_rows_between_commits_on_read<R>(
    read: &R,
    before: crate::changelog::CommitId,
    after: crate::changelog::CommitId,
) -> Result<Vec<super::state::HistoricalStateDiffEntry>, crate::LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let (
        before_commit_catalog_root,
        before_change_catalog_root,
        before_endpoint_commit_object_id,
        before_global_root,
        before_local_root,
    ) = super::serving::authenticate_historical_state_roots_for_diff(read, before).await?;
    let (
        after_commit_catalog_root,
        after_change_catalog_root,
        after_endpoint_commit_object_id,
        after_global_root,
        after_local_root,
    ) = super::serving::authenticate_historical_state_roots_for_diff(read, after).await?;

    // Authenticate every selected state root before local-root pruning. The
    // point resolver may legitimately skip a global lookup when a local row
    // masks that key; that optimization must not turn a missing/corrupt
    // global root into a successful working diff.
    let mut authenticated_roots = BTreeSet::new();
    for root in [
        before_global_root,
        before_local_root,
        after_global_root,
        after_local_root,
    ] {
        if authenticated_roots.insert(root) {
            super::tree::validate_root_on_read(root, "state", read).await?;
        }
    }
    let changed_keys =
        super::tree::diff_roots(Some(before_local_root), Some(after_local_root), read).await?;
    if changed_keys.is_empty() {
        return Ok(Vec::new());
    }
    let encoded_keys = changed_keys
        .iter()
        .map(|key| {
            super::state::encode_state_key(super::state::StateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            })
        })
        .collect::<Vec<_>>();

    let before_values = super::serving::state_points_on_read_with_historical_auth(
        before_global_root,
        Some(before_local_root),
        &encoded_keys,
        true,
        before_commit_catalog_root,
        before_change_catalog_root,
        before_endpoint_commit_object_id,
        read,
    )
    .await?;
    let after_values = super::serving::state_points_on_read_with_historical_auth(
        after_global_root,
        Some(after_local_root),
        &encoded_keys,
        true,
        after_commit_catalog_root,
        after_change_catalog_root,
        after_endpoint_commit_object_id,
        read,
    )
    .await?;
    let before_rows = historical_state_rows_from_points(&encoded_keys, before_values)?;
    let after_rows = historical_state_rows_from_points(&encoded_keys, after_values)?;
    Ok(before_rows
        .into_iter()
        .zip(after_rows)
        .filter_map(|(before, after)| {
            historical_state_payloads_differ(before.as_ref(), after.as_ref())
                .then_some(super::state::HistoricalStateDiffEntry { before, after })
        })
        .collect())
}

fn historical_state_rows_from_points(
    encoded_keys: &[Vec<u8>],
    values: Vec<Option<(super::state::StateValue, super::serving::StateSource)>>,
) -> Result<Vec<Option<super::state::HistoricalStateRow>>, crate::LixError> {
    if encoded_keys.len() != values.len() {
        return Err(crate::LixError::new(
            crate::LixError::CODE_INTERNAL_ERROR,
            "state point batch returned a different number of slots than requested",
        ));
    }
    encoded_keys
        .iter()
        .zip(values)
        .map(|(encoded_key, value)| {
            let Some((value, source)) = value else {
                return Ok(None);
            };
            let key = super::state::decode_state_key(encoded_key)?;
            let (snapshot_content, deleted) = match value.cell {
                super::state::StateCell::Value(snapshot) => (Some(snapshot), false),
                super::state::StateCell::Null => (None, false),
                super::state::StateCell::Tombstone => (None, true),
            };
            Ok(Some(super::state::HistoricalStateRow {
                key,
                global: source == super::serving::StateSource::Global,
                snapshot_content,
                metadata: value.metadata,
                deleted,
                blob_manifest_object_ids: value.blob_manifest_object_ids,
                created_at: value.created_at,
                updated_at: value.updated_at,
                change_id: value.change_id,
                commit_id: value.commit_id,
            }))
        })
        .collect()
}

fn historical_state_payloads_differ(
    before: Option<&super::state::HistoricalStateRow>,
    after: Option<&super::state::HistoricalStateRow>,
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

fn historical_state_identity_changed(
    before: Option<&super::state::HistoricalStateRow>,
    after: Option<&super::state::HistoricalStateRow>,
) -> bool {
    match (before, after) {
        (Some(left), Some(right)) => {
            left.change_id != right.change_id || left.commit_id != right.commit_id
        }
        (Some(_), None) | (None, Some(_)) => true,
        (None, None) => false,
    }
}

fn historical_state_change_is_public(payload_changed: bool, identity_changed: bool) -> bool {
    payload_changed || identity_changed
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

pub(crate) async fn load_object_bytes(
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
        return Err(corruption(
            "branch snapshot has no authenticated latest RefChange edge",
        ));
    };
    let change = ChangeObjectV1::decode(ref_id, required_object(&objects, ref_id)?)?;
    let ChangeObjectV1::BranchRef {
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
    super::serving::validate_retained_ref_change(
        read,
        repository.change_catalog_root,
        ref_id,
        &change,
    )
    .await?;
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

#[cfg(test)]
mod tests {
    use super::{
        historical_state_change_is_public, historical_state_identity_changed,
        historical_state_payloads_differ,
    };
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::forktree::state::{HistoricalStateRow, StateKey};
    use crate::forktree::{CanonicalBranchId, CheckpointCursorV1, ObjectId};

    fn historical_row(
        change_id: ChangeId,
        commit_id: CommitId,
        deleted: bool,
        snapshot_content: Option<&str>,
    ) -> HistoricalStateRow {
        HistoricalStateRow {
            key: StateKey {
                schema_key: "plugin_entity".to_owned(),
                file_id: Some("file-a".to_owned()),
                entity_pk: EntityPk::single("row-a"),
            },
            global: false,
            change_id,
            commit_id,
            created_at: LixTimestamp::from_unix_millis_utc_lossy(0),
            updated_at: LixTimestamp::from_unix_millis_utc_lossy(0),
            snapshot_content: snapshot_content.map(Into::into),
            metadata: None,
            deleted,
            blob_manifest_object_ids: Vec::new(),
        }
    }

    #[test]
    fn inherited_marker_does_not_classify_descendant_commit() {
        let owner = CanonicalBranchId::from_bytes([0x11; 16]);
        let inheriting_branch = CanonicalBranchId::from_bytes([0x12; 16]);
        let root = ObjectId::from_bytes([0x21; 32]);
        let checkpoint = ObjectId::from_bytes([0x22; 32]);
        let ordinary = ObjectId::from_bytes([0x23; 32]);
        let cursor = CheckpointCursorV1::Ordinary {
            owner_branch_id: owner,
            root_commit_object_id: root,
            distance_to_root: 3,
            latest_checkpoint_object_id: checkpoint,
            distance_to_latest: 1,
        };

        assert_eq!(cursor.latest_for_branch(ordinary, owner), (checkpoint, 1));
        assert_eq!(
            cursor.latest_for_branch(ordinary, inheriting_branch),
            (ordinary, 0),
        );
    }

    #[test]
    fn same_payload_with_new_authenticated_change_identity_is_a_touched_identity() {
        let before = historical_row(
            ChangeId::for_test_label("change-before"),
            CommitId::for_test_label("commit-before"),
            false,
            Some(r#"{"id":"row-a","value":"same"}"#),
        );
        let same_payload_new_change = historical_row(
            ChangeId::for_test_label("change-after"),
            CommitId::for_test_label("commit-after"),
            false,
            Some(r#"{"id":"row-a","value":"same"}"#),
        );

        assert!(historical_state_identity_changed(
            Some(&before),
            Some(&same_payload_new_change),
        ));
        assert!(historical_state_change_is_public(true, true));
        assert!(historical_state_change_is_public(false, true));
        assert!(!historical_state_identity_changed(
            Some(&before),
            Some(&before)
        ));
    }

    #[test]
    fn null_tombstone_and_absence_remain_distinct_diff_states() {
        let change = ChangeId::for_test_label("change");
        let commit = CommitId::for_test_label("commit");
        let null = historical_row(change, commit, false, None);
        let tombstone = historical_row(change, commit, true, None);

        assert!(historical_state_payloads_differ(
            Some(&null),
            Some(&tombstone)
        ));
        assert!(historical_state_payloads_differ(Some(&tombstone), None));
        assert!(historical_state_payloads_differ(None, Some(&null)));
    }
}
