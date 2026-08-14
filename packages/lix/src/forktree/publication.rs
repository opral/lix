use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::RequestBlobSpliceProvenance;
use crate::binary_cas::{BlobEditSplice, BlobPayload, BlobSameLengthSplice};
use crate::storage::{Key, Precondition, StorageError};
use crate::storage_adapter::StorageAdapterRead;
use crate::storage_adapter::{StoragePrecondition, StorageWriteSet};

use super::blob::{AuthenticatedBlobRef, CompletedUpload, PreparedUploadPart};
use super::codec::corruption;
use super::model::{
    BlobChunkV1, BlobManifestV1, BranchSelectorV1, BranchSnapshotV1, ChangeCatalogEntry,
    ChangeCatalogOwner, ChangeObjectV1, CheckpointCursorV1, CommitObjectV1, GlobalSelectorV1,
    RepositoryRootV1, SnapshotSelectorV1, SnapshotTargetV1, UploadPartV1, UploadProgressV1,
    UploadSelectorV1, branch_selector_key, gc_progress_selector_key, global_selector_key,
    snapshot_selector_key, upload_selector_key,
};

pub(crate) fn introduced_checkpoint_marker(
    members: &[super::model::CommitMemberV3],
    branch_id: super::model::CanonicalBranchId,
) -> Result<bool, StorageError> {
    let expected_branch = uuid::Uuid::from_bytes(*branch_id.as_bytes()).to_string();
    let expected_pk = crate::entity_pk::EntityPk::uuid_from_canonical(&expected_branch)
        .map_err(|error| corruption(error.to_string()))?;
    let expected_key = super::state::encode_state_key(super::state::StateKeyRef {
        schema_key: crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY,
        file_id: None,
        entity_pk: &expected_pk,
    });
    let mut found = false;
    for member in members {
        let Some((encoded_key, _, global, _, _, deleted)) = member.introduced_identity() else {
            continue;
        };
        if encoded_key != expected_key {
            continue;
        }
        if found || global || deleted {
            return Err(corruption(
                "checkpoint commit contains an invalid or duplicate marker",
            ));
        }
        found = true;
    }
    Ok(found)
}
use super::object::{OBJECT_SPACE, ObjectId};
use super::serving::{CatalogTreeEdit, SelectedHistoricalMemberBatch, StateTreeEdit};
use super::state::StateKey;
use super::tree::{ImmutableObjectSet, ReceiptTreeEdit};
use super::view::{CoherentView, SELECTOR_SPACE};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SelectorExpectation {
    Absent,
    Equals(Bytes),
}

/// All typed objects required to move one selected branch to a new semantic
/// commit/state pair. Construction is not authority: `publish_state_transition`
/// validates every root, catalog, member, and owner edge before staging bytes.
#[derive(Debug)]
pub(crate) struct BranchStateTransition {
    pub(crate) state_edit: StateTreeEdit,
    pub(crate) commit_catalog_edit: CatalogTreeEdit,
    pub(crate) change_catalog_edit: CatalogTreeEdit,
    pub(crate) semantic_commit: CommitObjectV1,
    pub(crate) changes: Vec<ChangeObjectV1>,
    pub(crate) branch_snapshot: BranchSnapshotV1,
    pub(crate) repository_root: RepositoryRootV1,
}

/// One ordered single-branch history publication. Intermediate commits and
/// their immutable state roots are cataloged together; only the final commit
/// advances the branch selector/ref fact.
#[derive(Debug)]
pub(crate) struct OrderedBranchHistoryTransition {
    pub(crate) state_edits: Vec<StateTreeEdit>,
    pub(crate) state_domain_global: bool,
    pub(crate) commit_catalog_edit: CatalogTreeEdit,
    pub(crate) change_catalog_edit: CatalogTreeEdit,
    pub(crate) semantic_commits: Vec<CommitObjectV1>,
    pub(crate) fresh_changes: Vec<ChangeObjectV1>,
    pub(crate) branch_ref_change: ChangeObjectV1,
    pub(crate) branch_snapshot: BranchSnapshotV1,
    pub(crate) repository_root: RepositoryRootV1,
    pub(crate) selected_history: SelectedHistoricalMemberBatch,
}

/// One prepared atomic publication. It always exact-CASes and rotates the
/// global epoch, including fully deduplicated and root-only publications.
/// Object and selector puts are staged into the same storage write; no extra
/// flush or round trip exists at this boundary.
#[derive(Debug)]
pub(crate) struct PreparedPublication {
    expected_global: Bytes,
    next_global: GlobalSelectorV1,
    next_repository_root: Option<RepositoryRootV1>,
    selector_expectations: BTreeMap<Bytes, SelectorExpectation>,
    selector_puts: BTreeMap<Bytes, Bytes>,
    selector_deletes: BTreeSet<Bytes>,
    object_puts: ImmutableObjectSet,
    object_deletes: BTreeSet<ObjectId>,
}

impl PreparedPublication {
    /// Starts a branch/state publication and fences both raw selectors from
    /// the one coherent view used to derive it.
    pub(crate) fn from_branch_view<R>(view: &CoherentView<R>) -> Result<Self, StorageError>
    where
        R: StorageAdapterRead,
    {
        let mut publication = Self::from_global_epoch(view)?;
        publication.fence_branch_view(view)?;
        Ok(publication)
    }

    /// Adds the exact selector fence for another branch participating in the
    /// same transaction. All branch views are still derived from the one
    /// retained read/global epoch; this only widens the atomic CAS set and
    /// never creates a second publication or state authority.
    pub(crate) fn fence_branch_view<R>(
        &mut self,
        view: &CoherentView<R>,
    ) -> Result<(), StorageError>
    where
        R: StorageAdapterRead,
    {
        if self.expected_global.as_ref() != view.raw_global_selector().as_ref() {
            return Err(corruption(
                "branch selector fence was derived from another global view",
            ));
        }
        self.expect_selector(
            branch_selector_key(view.branch_id()),
            SelectorExpectation::Equals(view.raw_branch_selector().clone()),
        )
    }

    pub(crate) fn current_repository_root(&self) -> RepositoryRootV1 {
        self.next_repository_root
            .expect("prepared publication has an authenticated repository root")
    }

    pub(crate) async fn put_commit_catalog_entries<R>(
        &self,
        view: &CoherentView<R>,
        root: ObjectId,
        entries: &[(super::model::CommitId, super::model::CommitCatalogEntry)],
    ) -> Result<super::serving::CatalogTreeEdit, StorageError>
    where
        R: StorageAdapterRead,
    {
        let overlay = view.object_overlay(&self.object_puts);
        super::serving::put_commit_catalog_entries(root, entries, &overlay).await
    }

    pub(crate) async fn put_change_catalog_entries<R>(
        &self,
        view: &CoherentView<R>,
        root: ObjectId,
        entries: &[(super::model::ChangeId, super::model::ChangeCatalogEntry)],
    ) -> Result<super::serving::CatalogTreeEdit, StorageError>
    where
        R: StorageAdapterRead,
    {
        let overlay = view.object_overlay(&self.object_puts);
        super::serving::put_change_catalog_entries(root, entries, &overlay).await
    }

    /// Starts a receipt/GC publication whose only repository-wide read fence
    /// is the exact authenticated global selector. Receipt-specific expected
    /// bytes or absence must be added before commit.
    pub(crate) fn from_global_epoch<R>(view: &CoherentView<R>) -> Result<Self, StorageError>
    where
        R: StorageAdapterRead,
    {
        Ok(Self {
            expected_global: view.raw_global_selector().clone(),
            next_global: view.global_selector().rotated()?,
            next_repository_root: Some(view.repository_root()),
            selector_expectations: BTreeMap::new(),
            selector_puts: BTreeMap::new(),
            selector_deletes: BTreeSet::new(),
            object_puts: ImmutableObjectSet::default(),
            object_deletes: BTreeSet::new(),
        })
    }

    /// Starts a repository-global publication from an already authenticated
    /// selector claim. GC keeps the claim and its retained read in its
    /// operation-owned snapshot; this constructor only makes that exact
    /// claim usable by the same publication lowering boundary.
    pub(super) fn from_global_selector_claim(
        raw_global: Bytes,
        global: GlobalSelectorV1,
    ) -> Result<Self, StorageError> {
        if global.encode()?.as_ref() != raw_global.as_ref() {
            return Err(corruption(
                "global selector claim does not match its authenticated bytes",
            ));
        }
        Ok(Self {
            expected_global: raw_global,
            next_global: global.rotated()?,
            next_repository_root: None,
            selector_expectations: BTreeMap::new(),
            selector_puts: BTreeMap::new(),
            selector_deletes: BTreeSet::new(),
            object_puts: ImmutableObjectSet::default(),
            object_deletes: BTreeSet::new(),
        })
    }

    /// Adds one authenticated maintenance object mutation to this publication.
    /// Reachability owns the typed bytes; publication owns the single object
    /// namespace and rejects put/delete ambiguity before lowering.
    pub(super) fn stage_gc_object_put(
        &mut self,
        id: ObjectId,
        bytes: Bytes,
    ) -> Result<(), StorageError> {
        if self.object_deletes.contains(&id) {
            return Err(corruption("GC publication puts and deletes one object"));
        }
        self.stage_encoded_object(id, bytes)
    }

    pub(super) fn stage_gc_object_delete(&mut self, id: ObjectId) -> Result<(), StorageError> {
        if self.object_puts.get(id).is_some() {
            return Err(corruption("GC publication puts and deletes one object"));
        }
        self.object_deletes.insert(id);
        Ok(())
    }

    /// Atomically advances or retires the rebuildable GC progress selector.
    pub(super) fn stage_gc_progress_selector(
        &mut self,
        expected: Option<Bytes>,
        next: Option<Bytes>,
    ) -> Result<(), StorageError> {
        let key = gc_progress_selector_key();
        match next {
            Some(value) => self.put_selector(
                key,
                value,
                expected
                    .map(SelectorExpectation::Equals)
                    .unwrap_or(SelectorExpectation::Absent),
            ),
            None => self.delete_selector(
                key,
                expected.ok_or_else(|| corruption("GC selector retirement is not present"))?,
            ),
        }
    }

    /// Merges two plans derived from the same coherent global selector. This
    /// is an in-memory composition step only: the caller still performs one
    /// `into_storage_plan`, one prepare, and one backend commit. Independent
    /// selector expectations or incompatible repository-root rotations are
    /// rejected rather than silently choosing one authority.
    pub(crate) fn merge_from(&mut self, other: Self) -> Result<(), StorageError> {
        if self.expected_global != other.expected_global
            || self.next_global.epoch != other.next_global.epoch
            || self.next_global.selector_generation != other.next_global.selector_generation
        {
            return Err(corruption(
                "publications were prepared from different global epochs",
            ));
        }
        if self.next_global.repository_root != other.next_global.repository_root {
            let original_root = GlobalSelectorV1::decode(&self.expected_global)?.repository_root;
            let self_changed = self.next_global.repository_root != original_root;
            let other_changed = other.next_global.repository_root != original_root;
            if self_changed && other_changed {
                return Err(corruption(
                    "publications assign conflicting repository roots",
                ));
            }
            if !self_changed {
                self.next_global.repository_root = other.next_global.repository_root;
                self.next_repository_root = other.next_repository_root;
            }
        }
        for (key, expected) in &other.selector_expectations {
            self.expect_selector(key.clone(), expected.clone())?;
        }
        for (key, value) in other.selector_puts {
            let expected = self
                .selector_expectations
                .get(&key)
                .cloned()
                .ok_or_else(|| corruption("upload publication lost selector expectation"))?;
            self.put_selector(key, value, expected)?;
        }
        for key in other.selector_deletes {
            let expected = match other.selector_expectations.get(&key) {
                Some(SelectorExpectation::Equals(bytes)) => bytes.clone(),
                _ => {
                    return Err(corruption(
                        "publication deletes a selector without an exact expectation",
                    ));
                }
            };
            self.delete_selector(key, expected)?;
        }
        self.object_puts.extend(other.object_puts)?;
        Ok(())
    }

    /// Stages one authenticated upload part in this publication. Open parts
    /// retain typed ReceiptTree state; a completed part stages the manifest
    /// and lets the ordinary file-row lowerer publish the visible BlobRef in
    /// the same transaction. No separate CAS writer or second commit is
    /// reachable from this operation.
    pub(crate) fn publish_upload_part(
        &mut self,
        prepared: PreparedUploadPart,
    ) -> Result<(), StorageError> {
        if prepared.already_present {
            return Ok(());
        }
        for chunk in &prepared.chunks {
            self.stage_blob_chunk(chunk)?;
        }
        if let Some(manifest) = &prepared.complete_manifest {
            let merkle_objects = prepared.complete_merkle_objects.as_ref().ok_or_else(|| {
                corruption("completed upload manifest has no Merkle object closure")
            })?;
            self.stage_immutable_objects(merkle_objects)?;
            self.stage_blob_manifest(manifest)?;
            if let Some(raw_selector) = prepared.raw_selector {
                let selector = UploadSelectorV1::decode(&raw_selector)?;
                self.delete_upload_selector(&selector, raw_selector)?;
            }
            return Ok(());
        }

        self.stage_upload_part(&prepared.part)?;
        self.stage_receipt_tree_edit(prepared.receipt)?;
        let (progress_id, _) = prepared.progress.encode()?;
        if progress_id != prepared.selector.progress_object_id {
            return Err(corruption(
                "upload selector does not name the staged progress object",
            ));
        }
        self.stage_upload_progress(&prepared.progress)?;
        self.put_upload_selector(
            &prepared.selector,
            match prepared.raw_selector {
                Some(raw) => SelectorExpectation::Equals(raw),
                None => SelectorExpectation::Absent,
            },
        )
    }

    pub(super) fn stage_repository_root(
        &mut self,
        root: RepositoryRootV1,
    ) -> Result<ObjectId, StorageError> {
        let (id, bytes) = root.encode()?;
        self.stage_encoded_object(id, bytes)?;
        self.next_global.repository_root = id;
        self.next_repository_root = Some(root);
        Ok(id)
    }

    fn expect_selector(
        &mut self,
        key: Bytes,
        expected: SelectorExpectation,
    ) -> Result<(), StorageError> {
        if key == global_selector_key() {
            return Err(corruption(
                "global selector expectation is owned by the epoch fence",
            ));
        }
        match self.selector_expectations.get(&key) {
            Some(existing) if existing != &expected => Err(corruption(
                "publication assigns conflicting expectations to one selector",
            )),
            Some(_) => Ok(()),
            None => {
                self.selector_expectations.insert(key, expected);
                Ok(())
            }
        }
    }

    fn put_selector(
        &mut self,
        key: Bytes,
        value: Bytes,
        expected: SelectorExpectation,
    ) -> Result<(), StorageError> {
        if key == global_selector_key() || self.selector_deletes.contains(&key) {
            return Err(corruption("publication has an invalid selector put"));
        }
        self.expect_selector(key.clone(), expected)?;
        match self.selector_puts.get(&key) {
            Some(existing) if existing != &value => {
                Err(corruption("publication assigns two values to one selector"))
            }
            Some(_) => Ok(()),
            None => {
                self.selector_puts.insert(key, value);
                Ok(())
            }
        }
    }

    fn delete_selector(&mut self, key: Bytes, expected: Bytes) -> Result<(), StorageError> {
        if key == global_selector_key() || self.selector_puts.contains_key(&key) {
            return Err(corruption("publication has an invalid selector delete"));
        }
        self.expect_selector(key.clone(), SelectorExpectation::Equals(expected))?;
        self.selector_deletes.insert(key);
        Ok(())
    }

    fn stage_encoded_object(&mut self, id: ObjectId, bytes: Bytes) -> Result<(), StorageError> {
        self.object_puts.insert(id, bytes)
    }

    fn stage_immutable_objects(
        &mut self,
        objects: &ImmutableObjectSet,
    ) -> Result<(), StorageError> {
        for (id, bytes) in objects.iter() {
            self.stage_encoded_object(id, bytes.clone())?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn stage_blob_merkle_build_for_test(
        &mut self,
        build: &super::merkle::BlobMerkleTreeBuild,
    ) -> Result<ObjectId, StorageError> {
        self.stage_immutable_objects(&build.objects)?;
        self.stage_blob_manifest(&build.manifest)
    }

    fn stage_object_set(&mut self, objects: ImmutableObjectSet) -> Result<(), StorageError> {
        for (id, bytes) in objects.iter() {
            self.stage_encoded_object(id, bytes.clone())?;
        }
        Ok(())
    }

    pub(super) fn stage_branch_snapshot(
        &mut self,
        value: BranchSnapshotV1,
    ) -> Result<ObjectId, StorageError> {
        let (id, bytes) = value.encode()?;
        self.stage_encoded_object(id, bytes)?;
        Ok(id)
    }

    /// Publishes the selector for a newly-created branch into this same
    /// transaction-owned publication. The source commit is already
    /// authenticated through the caller's retained view; its state roots are
    /// copied into the new branch snapshot and the selector is fenced with an
    /// absent-key CAS, so creation cannot overwrite an existing branch.
    pub(crate) async fn publish_new_branch_selector<R>(
        &mut self,
        view: &CoherentView<R>,
        branch_id: super::model::CanonicalBranchId,
        source_commit: &CommitObjectV1,
        change_id: super::model::ChangeId,
        updated_at: crate::common::LixTimestamp,
    ) -> Result<ObjectId, StorageError>
    where
        R: StorageAdapterRead,
    {
        if branch_id == view.branch_id() {
            return Err(corruption(
                "branch creation selector targets the selected branch",
            ));
        }
        let (source_commit_object_id, _) = source_commit.encode()?;
        let branch_ref = ChangeObjectV1::BranchRef {
            change_id,
            updated_at,
            branch_id,
            before_semantic_head_commit_object_id: None,
            after_semantic_head_commit_object_id: Some(source_commit_object_id),
            previous_ref_change_object_id: None,
            payload: Vec::new(),
            json_payload_object_ids: Vec::new(),
        };
        let (ref_object_id, ref_bytes) = branch_ref.encode()?;
        let base_repository_root = self.next_repository_root.unwrap_or(view.repository_root());
        let overlay = view.object_overlay(&self.object_puts);
        let change_catalog_edit = super::serving::put_change_catalog_entries(
            base_repository_root.change_catalog_root,
            &[(
                change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::BranchRef {
                        ref_change_object_id: ref_object_id,
                        branch_id,
                    },
                },
            )],
            &overlay,
        )
        .await?;
        let next_change_catalog_root = change_catalog_edit.root;
        let branch_snapshot = BranchSnapshotV1 {
            branch_id,
            local_state_root: source_commit.local_state_root,
            semantic_head_commit_object_id: source_commit_object_id,
            latest_ref_change_object_id: Some(ref_object_id),
            historical_global_state_root: source_commit.global_state_root,
        };
        let snapshot_id = self.stage_branch_snapshot(branch_snapshot)?;
        self.stage_encoded_object(ref_object_id, ref_bytes)?;
        self.stage_catalog_edit(change_catalog_edit)?;
        self.stage_repository_root(RepositoryRootV1 {
            global_state_root: base_repository_root.global_state_root,
            commit_catalog_root: base_repository_root.commit_catalog_root,
            change_catalog_root: next_change_catalog_root,
        })?;
        self.put_branch_selector(
            BranchSelectorV1 {
                branch_id,
                branch_snapshot_object_id: snapshot_id,
                selector_generation: 1,
            },
            SelectorExpectation::Absent,
        )?;
        Ok(snapshot_id)
    }

    /// Publishes an existing branch's moving head or retires its selector in
    /// this same transaction-owned publication. The target view is opened
    /// from the caller's retained read, so selector generation, repository
    /// roots, and RefChange ancestry all use one coherent identity.
    pub(crate) async fn publish_branch_selector_intent<R>(
        &mut self,
        view: &CoherentView<R>,
        next_commit: Option<CommitObjectV1>,
        change_id: super::model::ChangeId,
        updated_at: crate::common::LixTimestamp,
    ) -> Result<(), StorageError>
    where
        R: StorageAdapterRead,
    {
        let Some(next_commit) = next_commit else {
            return self.delete_branch_selector(
                view.branch_selector(),
                view.raw_branch_selector().clone(),
            );
        };
        let (next_commit_object_id, _) = next_commit.encode()?;
        if next_commit_object_id == view.branch_snapshot().semantic_head_commit_object_id {
            return Ok(());
        }
        let branch_ref = ChangeObjectV1::BranchRef {
            change_id,
            updated_at,
            branch_id: view.branch_id(),
            before_semantic_head_commit_object_id: Some(
                view.branch_snapshot().semantic_head_commit_object_id,
            ),
            after_semantic_head_commit_object_id: Some(next_commit_object_id),
            previous_ref_change_object_id: view.branch_snapshot().latest_ref_change_object_id,
            payload: Vec::new(),
            json_payload_object_ids: Vec::new(),
        };
        let (ref_object_id, ref_bytes) = branch_ref.encode()?;
        let base_repository_root = self.next_repository_root.unwrap_or(view.repository_root());
        // A preceding semantic publication may have produced the catalog root
        // in this same PreparedPublication. Read those immutable path-copy
        // nodes through the publication overlay; the retained operation read
        // alone cannot see staged objects.
        let overlay = view.object_overlay(&self.object_puts);
        let change_catalog_edit = super::serving::put_change_catalog_entries(
            base_repository_root.change_catalog_root,
            &[(
                change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::BranchRef {
                        ref_change_object_id: ref_object_id,
                        branch_id: view.branch_id(),
                    },
                },
            )],
            &overlay,
        )
        .await?;
        let branch_snapshot = BranchSnapshotV1 {
            branch_id: view.branch_id(),
            local_state_root: next_commit.local_state_root,
            semantic_head_commit_object_id: next_commit_object_id,
            latest_ref_change_object_id: Some(ref_object_id),
            historical_global_state_root: next_commit.global_state_root,
        };
        let snapshot_id = self.stage_branch_snapshot(branch_snapshot)?;
        let next_change_catalog_root = change_catalog_edit.root;
        self.stage_catalog_edit(change_catalog_edit)?;
        self.stage_encoded_object(ref_object_id, ref_bytes)?;
        self.stage_repository_root(RepositoryRootV1 {
            global_state_root: base_repository_root.global_state_root,
            commit_catalog_root: base_repository_root.commit_catalog_root,
            change_catalog_root: next_change_catalog_root,
        })?;
        self.put_branch_selector(
            BranchSelectorV1 {
                branch_id: view.branch_id(),
                branch_snapshot_object_id: snapshot_id,
                selector_generation: view
                    .branch_selector()
                    .selector_generation
                    .checked_add(1)
                    .ok_or_else(|| corruption("branch selector generation overflowed"))?,
            },
            SelectorExpectation::Equals(view.raw_branch_selector().clone()),
        )?;
        Ok(())
    }

    pub(super) fn stage_blob_chunk(
        &mut self,
        value: &BlobChunkV1,
    ) -> Result<ObjectId, StorageError> {
        let (id, bytes) = value.encode()?;
        self.stage_encoded_object(id, bytes)?;
        Ok(id)
    }

    pub(super) fn stage_blob_manifest(
        &mut self,
        value: &BlobManifestV1,
    ) -> Result<ObjectId, StorageError> {
        let (id, bytes) = value.encode()?;
        self.stage_encoded_object(id, bytes)?;
        Ok(id)
    }

    pub(crate) fn staged_blob_manifest(
        &self,
        id: ObjectId,
    ) -> Result<Option<BlobManifestV1>, StorageError> {
        self.object_puts
            .get(id)
            .map(|bytes| BlobManifestV1::decode(id, bytes))
            .transpose()
    }

    /// Lowers an inline file payload into the same authenticated object set as
    /// every other ForkTree blob publication. The returned manifest identity
    /// is later attached to the exact `lix_binary_blob_ref` state row; no
    /// BlobId-only reader or separate CAS commit is involved.
    pub(crate) fn stage_inline_blob_payload(
        &mut self,
        bytes: &[u8],
    ) -> Result<ObjectId, StorageError> {
        let chunks = if bytes.is_empty() {
            vec![BlobChunkV1 {
                bytes: Bytes::new(),
            }]
        } else {
            bytes
                .chunks(super::blob::CANONICAL_BLOB_CHUNK_BYTES)
                .map(|chunk| BlobChunkV1 {
                    bytes: Bytes::copy_from_slice(chunk),
                })
                .collect::<Vec<_>>()
        };
        let build = super::merkle::build_blob_merkle_tree(&chunks)?;
        self.stage_immutable_objects(&build.objects)?;
        self.stage_blob_manifest(&build.manifest)
    }

    /// Authenticates a fixed-width successor against the exact BlobRef
    /// StateKey selected by `view`, then stages only chunks intersecting the
    /// verified edit. Unchanged chunk references are copied from the
    /// authenticated base manifest; no payload or second storage read is used
    /// for those chunks.
    pub(crate) async fn stage_verified_inline_blob_splice<R>(
        &mut self,
        view: &CoherentView<R>,
        state_key: &StateKey,
        payload: &BlobPayload,
        splice: BlobSameLengthSplice,
    ) -> Result<ObjectId, StorageError>
    where
        R: StorageAdapterRead + Sync,
    {
        let reference = view
            .bind_blob_at_state_key(state_key)
            .await
            .map_err(|error| StorageError::Corruption(error.to_string()))?
            .ok_or_else(|| corruption("verified blob splice base owner is absent"))?;
        self.stage_verified_inline_blob_splice_bound(view, state_key, payload, splice, reference)
            .await
    }

    /// Re-binds a transaction-verified variable-width edit to the exact
    /// StateKey-selected BlobRef and path-copies its Merkle successor. The
    /// edit hint is never an authority by itself: the selected base identity,
    /// manifest root, geometry, and complete retained node closure are
    /// authenticated on this coherent view before successor objects stage.
    pub(crate) async fn stage_verified_inline_blob_edit<R>(
        &mut self,
        view: &CoherentView<R>,
        state_key: &StateKey,
        payload: &BlobPayload,
        splice: BlobEditSplice,
    ) -> Result<ObjectId, StorageError>
    where
        R: StorageAdapterRead + Sync,
    {
        let reference = view
            .bind_blob_at_state_key(state_key)
            .await
            .map_err(|error| StorageError::Corruption(error.to_string()))?
            .ok_or_else(|| corruption("verified blob edit base owner is absent"))?;
        self.stage_verified_inline_blob_edit_bound(view, state_key, payload, splice, reference)
            .await
    }

    /// Promotes SQL's transport-side splice proof at the publication owner.
    /// The proof is re-bound to the exact BlobRef StateKey on this retained
    /// coherent view, and the authenticated base payload is checked against
    /// the transport digest and exact prefix/insert/suffix bytes before the
    /// fixed-chunk writer is allowed to copy unchanged manifest edges.
    pub(crate) async fn stage_verified_request_blob_splice<R>(
        &mut self,
        view: &CoherentView<R>,
        state_key: &StateKey,
        payload: &BlobPayload,
        provenance: &RequestBlobSpliceProvenance,
    ) -> Result<ObjectId, StorageError>
    where
        R: StorageAdapterRead + Sync,
    {
        let reference = view
            .bind_blob_at_state_key(state_key)
            .await
            .map_err(|error| StorageError::Corruption(error.to_string()))?
            .ok_or_else(|| corruption("request blob splice base owner is absent"))?;
        if provenance.base_blob_id() != reference.semantic_id() {
            return Err(corruption(
                "request blob splice base identity does not match its StateKey owner",
            ));
        }
        let base_len = usize::try_from(reference.expected_size())
            .map_err(|_| corruption("request blob splice base length is invalid"))?;
        let prefix = provenance.prefix_bytes();
        let suffix = provenance.suffix_bytes();
        if prefix > base_len || suffix > base_len.saturating_sub(prefix) {
            return Err(corruption("request blob splice bounds are invalid"));
        }
        let replacement_len = base_len - prefix - suffix;
        let insert = provenance.insert();
        let expected_len = prefix
            .checked_add(insert.len())
            .and_then(|len| len.checked_add(suffix))
            .ok_or_else(|| corruption("request blob splice result length overflows"))?;
        if payload.len() != expected_len || (replacement_len == 0 && insert.is_empty()) {
            return Err(corruption(
                "request blob splice result length or changed range is invalid",
            ));
        }
        let insert_end = prefix + insert.len();
        if payload.bytes().get(prefix..insert_end) != Some(insert)
            || insert_end != payload.len() - suffix
        {
            return Err(corruption(
                "request blob splice bytes do not match its authenticated base",
            ));
        }
        // SHA-256 remains transport metadata. The canonical Merkle identity
        // derived from those verified bytes was matched to the exact
        // StateKey-selected BlobRef above; the proof below binds that owner to
        // its manifest root without a backend whole-file witness pass.
        if replacement_len == insert.len() {
            let splice =
                BlobSameLengthSplice::new(reference.semantic_id(), prefix, replacement_len);
            self.stage_verified_inline_blob_splice_bound(
                view, state_key, payload, splice, reference,
            )
            .await
        } else {
            let splice = BlobEditSplice {
                base_blob_hash: reference.semantic_id(),
                offset: prefix,
                delete_len: replacement_len,
                insert_len: insert.len(),
            };
            self.stage_verified_inline_blob_edit_bound(view, state_key, payload, splice, reference)
                .await
        }
    }

    async fn stage_verified_inline_blob_edit_bound<R>(
        &mut self,
        view: &CoherentView<R>,
        _state_key: &StateKey,
        payload: &BlobPayload,
        splice: BlobEditSplice,
        reference: AuthenticatedBlobRef,
    ) -> Result<ObjectId, StorageError>
    where
        R: StorageAdapterRead + Sync,
    {
        if reference.semantic_id() != splice.base_blob_hash {
            return Err(corruption(
                "verified blob edit base identity does not match its StateKey owner",
            ));
        }
        let base_len = usize::try_from(reference.expected_size())
            .map_err(|_| corruption("verified blob edit base length is invalid"))?;
        let delete_end = splice
            .offset
            .checked_add(splice.delete_len)
            .filter(|end| *end <= base_len)
            .ok_or_else(|| corruption("verified blob edit delete range is invalid"))?;
        let expected_len = base_len
            .checked_sub(splice.delete_len)
            .and_then(|len| len.checked_add(splice.insert_len))
            .ok_or_else(|| corruption("verified blob edit successor length overflows"))?;
        if expected_len != payload.len()
            || splice.offset.checked_add(splice.insert_len).is_none()
            || (splice.delete_len == 0 && splice.insert_len == 0)
        {
            return Err(corruption(
                "verified blob edit payload length or changed range is invalid",
            ));
        }
        let base_manifest_object_id = reference.manifest_object_id();
        let base_manifest_bytes = view.load_object_bytes(base_manifest_object_id).await?;
        let base_manifest = BlobManifestV1::decode(base_manifest_object_id, &base_manifest_bytes)?;
        if base_manifest.logical_bytes != reference.expected_size()
            || base_manifest.canonical_blob_id != splice.base_blob_hash
        {
            return Err(corruption(
                "verified blob edit base manifest is not bound to its BlobRef owner",
            ));
        }
        let successor = view
            .build_blob_merkle_edit_successor(
                base_manifest,
                payload.bytes(),
                splice.offset,
                delete_end - splice.offset,
                splice.insert_len,
            )
            .await?;
        let expected_successor_id = payload
            .hash()
            .unwrap_or_else(|| crate::binary_cas::BlobId::from_canonical_content(payload.bytes()));
        if successor.manifest.canonical_blob_id != expected_successor_id
            || successor.manifest.logical_bytes != payload.len() as u64
        {
            return Err(corruption(
                "verified blob edit hint does not reproduce the requested payload",
            ));
        }
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_verified_inline_blob_splice(
            successor
                .objects
                .iter()
                .filter(|(id, bytes)| {
                    super::object::authenticate_object_domain(*id, bytes)
                        == Ok(super::object::ObjectDomain::BlobChunk)
                })
                .count(),
            successor.manifest.leaf_count as usize,
        );
        self.stage_immutable_objects(&successor.objects)?;
        self.stage_blob_manifest(&successor.manifest)
    }

    async fn stage_verified_inline_blob_splice_bound<R>(
        &mut self,
        view: &CoherentView<R>,
        state_key: &StateKey,
        payload: &BlobPayload,
        splice: BlobSameLengthSplice,
        reference: AuthenticatedBlobRef,
    ) -> Result<ObjectId, StorageError>
    where
        R: StorageAdapterRead + Sync,
    {
        if reference.semantic_id() != splice.base_blob_hash {
            return Err(corruption(
                "verified blob splice base identity does not match its StateKey owner",
            ));
        }
        if reference.expected_size() != payload.len() as u64 {
            return Err(corruption(
                "verified blob splice changes the authenticated base length",
            ));
        }
        let splice_end = splice
            .end()
            .filter(|end| splice.length != 0 && *end <= payload.len())
            .ok_or_else(|| corruption("verified blob splice range is invalid"))?;
        let base_manifest_object_id = reference.manifest_object_id();
        let base_manifest_bytes = view.load_object_bytes(base_manifest_object_id).await?;
        let base_manifest = BlobManifestV1::decode(base_manifest_object_id, &base_manifest_bytes)?;
        if base_manifest.logical_bytes != payload.len() as u64
            || base_manifest.canonical_blob_id != splice.base_blob_hash
        {
            return Err(corruption(
                "verified blob splice base manifest is not bound to its BlobRef owner",
            ));
        }
        let byte_range = splice.offset as u64..splice_end as u64;
        let leaf_range = super::merkle::leaf_range_for_bytes(&base_manifest, byte_range)?;
        let proof = view
            .load_blob_merkle_proof(base_manifest, state_key, leaf_range.clone())
            .await?;
        let proof_start = leaf_range.start * super::model::BLOB_MERKLE_CHUNK_BYTES;
        let proof_end = (leaf_range.end * super::model::BLOB_MERKLE_CHUNK_BYTES)
            .min(base_manifest.logical_bytes);
        let authenticated = super::merkle::materialize_blob_merkle_range(
            &proof,
            state_key,
            base_manifest,
            proof_start..proof_end,
        )?;
        let prefix_len = splice.offset - proof_start as usize;
        let suffix_start = splice_end - proof_start as usize;
        let payload_start = proof_start as usize;
        let payload_end = proof_end as usize;
        if authenticated[..prefix_len] != payload.bytes()[payload_start..splice.offset]
            || authenticated[suffix_start..] != payload.bytes()[splice_end..payload_end]
        {
            return Err(corruption(
                "verified blob splice unchanged bytes do not match its authenticated proof",
            ));
        }
        let mut replacements = BTreeMap::new();
        for ordinal in leaf_range.clone() {
            let start = ordinal as usize * super::blob::CANONICAL_BLOB_CHUNK_BYTES;
            let end = start
                .saturating_add(super::blob::CANONICAL_BLOB_CHUNK_BYTES)
                .min(payload.len());
            replacements.insert(
                ordinal,
                BlobChunkV1 {
                    bytes: Bytes::copy_from_slice(&payload.bytes()[start..end]),
                },
            );
        }
        let successor = super::merkle::build_blob_merkle_successor(
            &proof,
            state_key,
            base_manifest,
            leaf_range,
            &replacements,
        )?;
        self.stage_immutable_objects(&successor.objects)?;
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_verified_inline_blob_splice(
            replacements.len(),
            base_manifest.leaf_count as usize,
        );
        self.stage_blob_manifest(&successor.manifest)
    }

    /// Lowers one large JSON value into an authenticated ForkTree payload
    /// object. The object is deliberately a typed BlobChunk: its envelope
    /// authenticates the domain and complete bytes, while the owning Change
    /// object carries the edge used by reachability and GC. JSON never uses
    /// the legacy JSON_SPACE on this path.
    pub(crate) fn stage_json_payload(&mut self, json: &str) -> Result<ObjectId, StorageError> {
        if json.len() <= crate::json_store::JSON_INLINE_MAX_BYTES {
            return Err(corruption(
                "inline JSON must remain inline rather than becoming an object",
            ));
        }
        self.stage_blob_chunk(&BlobChunkV1 {
            bytes: Bytes::copy_from_slice(json.as_bytes()),
        })
    }

    pub(super) fn stage_upload_part(
        &mut self,
        value: &UploadPartV1,
    ) -> Result<ObjectId, StorageError> {
        let (id, bytes) = value.encode()?;
        self.stage_encoded_object(id, bytes)?;
        Ok(id)
    }

    pub(super) fn stage_upload_progress(
        &mut self,
        value: &UploadProgressV1,
    ) -> Result<ObjectId, StorageError> {
        let (id, bytes) = value.encode()?;
        self.stage_encoded_object(id, bytes)?;
        Ok(id)
    }

    pub(super) fn stage_receipt_tree_edit(
        &mut self,
        edit: ReceiptTreeEdit,
    ) -> Result<(), StorageError> {
        self.stage_object_set(edit.objects)
    }

    pub(super) fn stage_snapshot_target(
        &mut self,
        value: SnapshotTargetV1,
    ) -> Result<ObjectId, StorageError> {
        let (id, bytes) = value.encode()?;
        self.stage_encoded_object(id, bytes)?;
        Ok(id)
    }

    pub(super) fn stage_state_edit(&mut self, edit: StateTreeEdit) -> Result<(), StorageError> {
        self.stage_object_set(edit.objects)
    }

    pub(super) fn stage_catalog_edit(&mut self, edit: CatalogTreeEdit) -> Result<(), StorageError> {
        self.stage_object_set(edit.objects)
    }

    /// Creates one open multipart receipt from a fully typed closure. The
    /// ReceiptTree, parts, chunks, progress aggregates, and selector binding
    /// are validated as one authority before any bytes are staged.
    pub(crate) fn publish_new_upload(
        &mut self,
        chunks: &[BlobChunkV1],
        parts: &[UploadPartV1],
        receipt: ReceiptTreeEdit,
        progress: &UploadProgressV1,
        selector: &UploadSelectorV1,
    ) -> Result<(), StorageError> {
        if selector.upload_id != progress.upload_id
            || selector.binding_digest != progress.binding_digest
        {
            return Err(corruption(
                "upload selector/progress binding is inconsistent",
            ));
        }
        let mut objects = receipt.objects;
        for chunk in chunks {
            let (id, bytes) = chunk.encode()?;
            objects.insert(id, bytes)?;
        }
        for part in parts {
            let (id, bytes) = part.encode()?;
            objects.insert(id, bytes)?;
        }
        let (progress_id, progress_bytes) = progress.encode()?;
        if selector.progress_object_id != progress_id {
            return Err(corruption("upload selector names another progress object"));
        }
        objects.insert(progress_id, progress_bytes)?;
        let loaded_parts = super::tree::validate_upload_progress_tree(progress, |id| {
            objects
                .get(id)
                .cloned()
                .ok_or_else(|| corruption(format!("new upload object {id} is absent")))
        })?;
        if loaded_parts.len() != parts.len()
            || loaded_parts
                .iter()
                .zip(parts)
                .any(|(left, right)| left != right)
        {
            return Err(corruption(
                "new upload typed parts do not equal its ReceiptTree closure",
            ));
        }
        self.stage_object_set(objects)?;
        self.put_upload_selector(selector, SelectorExpectation::Absent)
    }

    pub(crate) fn abort_upload(
        &mut self,
        selector: &UploadSelectorV1,
        raw_selector: Bytes,
    ) -> Result<(), StorageError> {
        if UploadSelectorV1::decode(&raw_selector)? != *selector {
            return Err(corruption(
                "upload abort raw selector does not match typed selector",
            ));
        }
        self.delete_upload_selector(selector, raw_selector)
    }

    /// Pins the exact selected branch snapshot/head under any retained-root
    /// role without letting a caller supply an unrelated object edge.
    pub(crate) fn publish_current_snapshot_pin<R>(
        &mut self,
        view: &CoherentView<R>,
        role: super::model::SnapshotRole,
        selector_id: super::model::SnapshotSelectorId,
        expected: SelectorExpectation,
    ) -> Result<ObjectId, StorageError>
    where
        R: StorageAdapterRead,
    {
        let selector_generation = match &expected {
            SelectorExpectation::Absent => 1,
            SelectorExpectation::Equals(raw_selector) => {
                let previous = SnapshotSelectorV1::decode(raw_selector)?;
                if previous.role != role || previous.selector_id != selector_id {
                    return Err(corruption(
                        "snapshot selector expectation identity does not match its replacement",
                    ));
                }
                previous
                    .selector_generation
                    .checked_add(1)
                    .ok_or_else(|| corruption("snapshot selector generation overflowed"))?
            }
        };
        let target = SnapshotTargetV1 {
            role,
            selector_id,
            branch_id: view.branch_id(),
            branch_snapshot_object_id: view.branch_selector().branch_snapshot_object_id,
            semantic_commit_object_id: view.branch_snapshot().semantic_head_commit_object_id,
        };
        let target_id = self.stage_snapshot_target(target)?;
        self.put_snapshot_selector(
            SnapshotSelectorV1 {
                role,
                selector_id,
                target_object_id: target_id,
                selector_generation,
            },
            expected,
        )?;
        Ok(target_id)
    }

    #[cfg(test)]
    fn release_snapshot_pin(
        &mut self,
        selector: SnapshotSelectorV1,
        raw_selector: Bytes,
    ) -> Result<(), StorageError> {
        if SnapshotSelectorV1::decode(&raw_selector)? != selector {
            return Err(corruption(
                "snapshot release raw selector does not match typed selector",
            ));
        }
        self.delete_snapshot_selector(selector, raw_selector)
    }

    /// Atomically releases the final retained selector and removes the exact
    /// catalog edges whose objects were retained only by that selector.
    ///
    /// The caller derives both path-copy edits from the same coherent view;
    /// moving the epoch, selector, RepositoryRoot, and catalog roots in one
    /// commit prevents either a dangling back-edge or an early reclamation
    /// window.
    #[cfg(test)]
    pub(crate) fn release_snapshot_pin_with_catalog_retirement<R>(
        &mut self,
        view: &CoherentView<R>,
        selector: SnapshotSelectorV1,
        raw_selector: Bytes,
        commit_catalog_edit: CatalogTreeEdit,
        change_catalog_edit: CatalogTreeEdit,
        repository_root: RepositoryRootV1,
    ) -> Result<(), StorageError>
    where
        R: StorageAdapterRead,
    {
        if self.expected_global.as_ref() != view.raw_global_selector().as_ref()
            || commit_catalog_edit.base_root != view.repository_root().commit_catalog_root
            || change_catalog_edit.base_root != view.repository_root().change_catalog_root
            || repository_root.commit_catalog_root != commit_catalog_edit.root
            || repository_root.change_catalog_root != change_catalog_edit.root
            || repository_root.global_state_root != view.repository_root().global_state_root
        {
            return Err(corruption(
                "snapshot release catalog retirement was derived from another coherent view",
            ));
        }
        self.stage_catalog_edit(commit_catalog_edit)?;
        self.stage_catalog_edit(change_catalog_edit)?;
        self.stage_repository_root(repository_root)?;
        self.release_snapshot_pin(selector, raw_selector)
    }

    /// Publishes one fully authenticated branch/state transition. This owner
    /// operation is the only route from path-copied state/catalog objects to
    /// serving selectors; mismatched graph edges cannot be staged.
    pub(crate) async fn publish_state_transition<R>(
        &mut self,
        view: &CoherentView<R>,
        transition: BranchStateTransition,
    ) -> Result<ObjectId, StorageError>
    where
        R: StorageAdapterRead,
    {
        if self.expected_global.as_ref() != view.raw_global_selector().as_ref() {
            return Err(corruption(
                "state transition was derived from another global view",
            ));
        }
        let BranchStateTransition {
            state_edit,
            commit_catalog_edit,
            change_catalog_edit,
            mut semantic_commit,
            changes,
            branch_snapshot,
            repository_root,
        } = transition;
        if branch_snapshot.branch_id != view.branch_id() {
            return Err(corruption(
                "state transition branch snapshot has another branch ID",
            ));
        }
        let current_repository_root = self.current_repository_root();
        let is_global = repository_root.global_state_root == state_edit.root
            && branch_snapshot.local_state_root == view.branch_snapshot().local_state_root
            && branch_snapshot.historical_global_state_root == state_edit.root;
        let is_branch_local = branch_snapshot.local_state_root == state_edit.root
            && repository_root.global_state_root == current_repository_root.global_state_root
            && branch_snapshot.historical_global_state_root
                == current_repository_root.global_state_root;
        if is_global == is_branch_local {
            return Err(corruption(
                "state edit must install at exactly one global or branch-local root",
            ));
        }
        let expected_state_base = if is_global {
            current_repository_root.global_state_root
        } else {
            view.branch_snapshot().local_state_root
        };
        if state_edit.base_root != expected_state_base {
            return Err(corruption(
                "state edit base does not match the selected authenticated root",
            ));
        }
        if is_global && state_edit.wrote_tombstone {
            return Err(corruption("global state publication contains a tombstone"));
        }
        if semantic_commit.parent_commit_object_ids.first()
            != Some(&view.branch_snapshot().semantic_head_commit_object_id)
        {
            return Err(corruption(
                "semantic commit first parent is not the selected branch head",
            ));
        }
        let member_pages = semantic_commit.prepare_member_pages()?;
        let (commit_id, commit_bytes) = semantic_commit.encode()?;
        if state_edit
            .written_commit_ids
            .iter()
            .any(|id| id != semantic_commit.commit_id.as_bytes())
        {
            return Err(corruption(
                "state mutation commit ID does not match the semantic commit",
            ));
        }
        if branch_snapshot.semantic_head_commit_object_id != commit_id
            || semantic_commit.global_state_root != branch_snapshot.historical_global_state_root
            || semantic_commit.local_state_root != branch_snapshot.local_state_root
        {
            return Err(corruption(
                "semantic commit does not authenticate the selected state roots",
            ));
        }
        if commit_catalog_edit.base_root != current_repository_root.commit_catalog_root
            || change_catalog_edit.base_root != current_repository_root.change_catalog_root
            || repository_root.commit_catalog_root != commit_catalog_edit.root
            || repository_root.change_catalog_root != change_catalog_edit.root
            || commit_catalog_edit
                .commit_entries
                .get(&semantic_commit.commit_id)
                .map(|entry| entry.commit_object_id)
                != Some(commit_id)
        {
            return Err(corruption(
                "repository catalogs do not authenticate the semantic commit",
            ));
        }
        let mut parent_ids = BTreeSet::new();
        let mut first_parent = None;
        for (parent_index, parent_id) in semantic_commit.parent_commit_object_ids.iter().enumerate()
        {
            if !parent_ids.insert(*parent_id) {
                return Err(corruption("semantic commit repeats one parent"));
            }
            let bytes = view.load_object_bytes(*parent_id).await?;
            let parent = CommitObjectV1::decode(*parent_id, &bytes)?;
            if parent_index == 0 {
                first_parent = Some((*parent_id, parent.clone()));
            }
            if parent.generation >= semantic_commit.generation {
                return Err(corruption(
                    "semantic commit generation does not follow every parent",
                ));
            }
        }
        let (first_parent_id, first_parent) =
            first_parent.ok_or_else(|| corruption("semantic commit has no first parent"))?;
        let expected_checkpoint_cursor = CheckpointCursorV1::after_first_parent(
            first_parent_id,
            &first_parent,
            view.branch_id(),
            introduced_checkpoint_marker(&semantic_commit.members, view.branch_id())?,
        )?;
        if semantic_commit.checkpoint_cursor != expected_checkpoint_cursor {
            return Err(corruption(
                "semantic commit checkpoint cursor does not derive from its first parent and marker",
            ));
        }

        let mut encoded_changes = BTreeMap::new();
        for change in &changes {
            let (id, bytes) = change.encode()?;
            if encoded_changes.insert(id, (change, bytes)).is_some() {
                return Err(corruption("state transition repeats one Change object"));
            }
        }
        let mut expected_change_ids = BTreeSet::new();
        for (ordinal, member) in semantic_commit.members.iter().enumerate() {
            if member.source().is_some() {
                return Err(corruption(
                    "single-transition publication cannot introduce selected history",
                ));
            }
            let change_id = member.change_id();
            if !expected_change_ids.insert(change_id) {
                return Err(corruption("semantic commit repeats one member ChangeId"));
            }
            let Some((_encoded_key, _layout, _global, _owner, _semantic, _deleted)) =
                member.introduced_identity()
            else {
                return Err(corruption(
                    "single-transition semantic member is not introduced",
                ));
            };
            let expected = super::model::ChangeCatalogEntry {
                owner: super::model::ChangeCatalogOwner::CommitMember {
                    commit_object_id: commit_id,
                    ordinal: u32::try_from(ordinal)
                        .map_err(|_| corruption("semantic commit ordinal exceeds u32"))?,
                },
            };
            if change_catalog_edit.change_entries.get(&change_id) != Some(&expected) {
                return Err(corruption(
                    "semantic ChangeCatalog owner does not match commit ordinal",
                ));
            }
        }
        let ref_id = branch_snapshot
            .latest_ref_change_object_id
            .ok_or_else(|| corruption("state transition has no branch RefChange edge"))?;
        let Some((
            ChangeObjectV1::BranchRef {
                change_id,
                branch_id,
                before_semantic_head_commit_object_id,
                after_semantic_head_commit_object_id,
                previous_ref_change_object_id,
                ..
            },
            _,
        )) = encoded_changes.get(&ref_id)
        else {
            return Err(corruption(
                "new branch RefChange edge has no typed Change object",
            ));
        };
        let expected = super::model::ChangeCatalogEntry {
            owner: super::model::ChangeCatalogOwner::BranchRef {
                ref_change_object_id: ref_id,
                branch_id: *branch_id,
            },
        };
        if *branch_id != view.branch_id()
            || *before_semantic_head_commit_object_id
                != Some(view.branch_snapshot().semantic_head_commit_object_id)
            || *after_semantic_head_commit_object_id != Some(commit_id)
            || *previous_ref_change_object_id != view.branch_snapshot().latest_ref_change_object_id
            || change_catalog_edit.change_entries.get(change_id) != Some(&expected)
        {
            return Err(corruption(
                "branch RefChange/catalog/head edge is inconsistent",
            ));
        }
        if encoded_changes.len() != 1 || !encoded_changes.contains_key(&ref_id) {
            return Err(corruption(
                "state transition standalone Change set is not exactly its RefChange",
            ));
        }

        self.stage_state_edit(state_edit)?;
        self.stage_catalog_edit(commit_catalog_edit)?;
        self.stage_catalog_edit(change_catalog_edit)?;
        for (page_id, page_bytes) in member_pages {
            self.stage_encoded_object(page_id, page_bytes)?;
        }
        self.stage_encoded_object(commit_id, commit_bytes)?;
        for (id, (_, bytes)) in encoded_changes {
            self.stage_encoded_object(id, bytes)?;
        }
        let snapshot_id = self.stage_branch_snapshot(branch_snapshot)?;
        self.stage_repository_root(repository_root)?;
        self.install_branch_selector(view, snapshot_id)
    }

    pub(crate) async fn publish_ordered_branch_history<R>(
        &mut self,
        view: &CoherentView<R>,
        transition: OrderedBranchHistoryTransition,
    ) -> Result<ObjectId, StorageError>
    where
        R: StorageAdapterRead,
    {
        if self.expected_global.as_ref() != view.raw_global_selector().as_ref() {
            return Err(corruption(
                "ordered history was derived from another coherent global view",
            ));
        }
        let OrderedBranchHistoryTransition {
            state_edits,
            state_domain_global,
            commit_catalog_edit,
            change_catalog_edit,
            mut semantic_commits,
            fresh_changes,
            branch_ref_change,
            branch_snapshot,
            repository_root,
            mut selected_history,
        } = transition;
        let mut member_pages = Vec::new();
        for commit in &mut semantic_commits {
            member_pages.extend(commit.prepare_member_pages()?);
        }
        if semantic_commits.is_empty() || semantic_commits.len() != state_edits.len() {
            return Err(corruption(
                "ordered history has mismatched commit/state-root cardinality",
            ));
        }
        if branch_snapshot.branch_id != view.branch_id()
            || commit_catalog_edit.base_root != view.repository_root().commit_catalog_root
            || change_catalog_edit.base_root != view.repository_root().change_catalog_root
            || repository_root.commit_catalog_root != commit_catalog_edit.root
            || repository_root.change_catalog_root != change_catalog_edit.root
        {
            return Err(corruption(
                "ordered history roots/catalogs do not derive from the selected view",
            ));
        }

        let mut encoded_commits = BTreeMap::new();
        for commit in &semantic_commits {
            let (object_id, bytes) = commit.encode()?;
            if encoded_commits.insert(object_id, (commit, bytes)).is_some()
                || commit_catalog_edit
                    .commit_entries
                    .get(&commit.commit_id)
                    .map(|entry| entry.commit_object_id)
                    != Some(object_id)
            {
                return Err(corruption(
                    "ordered history repeats or miscatalogs one Commit object",
                ));
            }
        }
        if encoded_commits.len() != commit_catalog_edit.commit_entries.len() {
            return Err(corruption(
                "ordered history CommitCatalog edit is not exactly the staged commits",
            ));
        }

        if !fresh_changes.is_empty() {
            return Err(corruption(
                "ordered history cannot publish standalone semantic Change objects",
            ));
        }

        let is_global = state_domain_global;
        let first_parent_object_id = semantic_commits
            .first()
            .and_then(|commit| commit.parent_commit_object_ids.first())
            .copied()
            .ok_or_else(|| corruption("ordered history first commit has no parent"))?;
        let first_parent_bytes = view.load_object_bytes(first_parent_object_id).await?;
        let first_parent = CommitObjectV1::decode(first_parent_object_id, &first_parent_bytes)?;
        view.validate_retained_commit(
            view.repository_root().commit_catalog_root,
            view.repository_root().change_catalog_root,
            first_parent_object_id,
            &first_parent,
        )
        .await?;
        let mut expected_state_base = if is_global {
            first_parent.global_state_root
        } else {
            first_parent.local_state_root
        };
        let mut seen_commit_ids = BTreeSet::new();
        for (commit, state_edit) in semantic_commits.iter().zip(state_edits.iter()) {
            let (commit_object_id, _) = commit.encode()?;
            if !seen_commit_ids.insert(commit.commit_id)
                || state_edit.base_root != expected_state_base
                || state_edit
                    .written_commit_ids
                    .iter()
                    .any(|id| id != commit.commit_id.as_bytes())
                || (is_global && state_edit.wrote_tombstone)
            {
                return Err(corruption(
                    "ordered history state edit/commit chronology is inconsistent",
                ));
            }
            let expected_global_root = if is_global {
                state_edit.root
            } else {
                first_parent.global_state_root
            };
            let expected_local_root = if is_global {
                first_parent.local_state_root
            } else {
                state_edit.root
            };
            if commit.global_state_root != expected_global_root
                || commit.local_state_root != expected_local_root
            {
                return Err(corruption(
                    "ordered Commit object does not authenticate its state edit root",
                ));
            }
            expected_state_base = state_edit.root;

            let mut parent_objects = BTreeSet::new();
            let mut first_parent = None;
            for (parent_index, parent_id) in commit.parent_commit_object_ids.iter().enumerate() {
                if !parent_objects.insert(*parent_id) {
                    return Err(corruption("ordered Commit repeats one parent edge"));
                }
                let parent = if let Some((parent, _)) = encoded_commits.get(parent_id) {
                    (*parent).clone()
                } else {
                    let bytes = view.load_object_bytes(*parent_id).await?;
                    CommitObjectV1::decode(*parent_id, &bytes)?
                };
                if parent_index == 0 {
                    first_parent = Some((*parent_id, parent.clone()));
                }
                if parent.generation >= commit.generation {
                    return Err(corruption(
                        "ordered Commit generation does not strictly follow every parent",
                    ));
                }
            }
            let (first_parent_id, first_parent) =
                first_parent.ok_or_else(|| corruption("ordered Commit has no first parent"))?;
            let expected_checkpoint_cursor = CheckpointCursorV1::after_first_parent(
                first_parent_id,
                &first_parent,
                view.branch_id(),
                introduced_checkpoint_marker(&commit.members, view.branch_id())?,
            )?;
            if commit.checkpoint_cursor != expected_checkpoint_cursor {
                return Err(corruption(
                    "ordered Commit checkpoint cursor does not derive from its first parent and marker",
                ));
            }

            let mut member_ids = BTreeSet::new();
            for (ordinal, member) in commit.members.iter().enumerate() {
                let change_id = member.change_id();
                if !member_ids.insert(change_id) {
                    return Err(corruption("ordered Commit repeats one ChangeId member"));
                }
                match member.source() {
                    None => {
                        let Some((_encoded_key, _layout, _global, _owner, _semantic, _deleted)) =
                            member.introduced_identity()
                        else {
                            return Err(corruption(
                                "introduced member has no embedded semantic payload",
                            ));
                        };
                        let expected = super::model::ChangeCatalogEntry {
                            owner: super::model::ChangeCatalogOwner::CommitMember {
                                commit_object_id,
                                ordinal: u32::try_from(ordinal).map_err(|_| {
                                    corruption("ordered Commit member ordinal exceeds u32")
                                })?,
                            },
                        };
                        if change_catalog_edit.change_entries.get(&change_id) != Some(&expected) {
                            return Err(corruption(
                                "fresh ChangeCatalog introduction owner is inconsistent",
                            ));
                        }
                    }
                    Some(_) => {
                        selected_history.consume_proof(
                            view.view_instance_id(),
                            commit.generation,
                            member,
                        )?;
                    }
                }
            }
        }
        selected_history.finish_proof(view.view_instance_id())?;

        let final_commit = semantic_commits.last().expect("nonempty checked");
        let (final_commit_object_id, _) = final_commit.encode()?;
        if branch_snapshot.semantic_head_commit_object_id != final_commit_object_id
            || branch_snapshot.local_state_root != final_commit.local_state_root
            || branch_snapshot.historical_global_state_root != final_commit.global_state_root
            || repository_root.global_state_root != final_commit.global_state_root
        {
            return Err(corruption(
                "final ordered Commit does not authenticate selected branch/repository roots",
            ));
        }
        let (ref_object_id, ref_bytes) = branch_ref_change.encode()?;
        let ChangeObjectV1::BranchRef {
            change_id,
            branch_id,
            before_semantic_head_commit_object_id,
            after_semantic_head_commit_object_id,
            previous_ref_change_object_id,
            ..
        } = &branch_ref_change
        else {
            return Err(corruption(
                "ordered history final ref fact has wrong domain",
            ));
        };
        let expected_ref_entry = super::model::ChangeCatalogEntry {
            owner: super::model::ChangeCatalogOwner::BranchRef {
                ref_change_object_id: ref_object_id,
                branch_id: *branch_id,
            },
        };
        if *branch_id != view.branch_id()
            || *before_semantic_head_commit_object_id
                != Some(view.branch_snapshot().semantic_head_commit_object_id)
            || *after_semantic_head_commit_object_id != Some(final_commit_object_id)
            || *previous_ref_change_object_id != view.branch_snapshot().latest_ref_change_object_id
            || branch_snapshot.latest_ref_change_object_id != Some(ref_object_id)
            || change_catalog_edit.change_entries.get(change_id) != Some(&expected_ref_entry)
        {
            return Err(corruption(
                "ordered history final branch ref/catalog/selector edge is inconsistent",
            ));
        }

        for edit in state_edits {
            self.stage_state_edit(edit)?;
        }
        self.stage_catalog_edit(commit_catalog_edit)?;
        self.stage_catalog_edit(change_catalog_edit)?;
        for (page_id, page_bytes) in member_pages {
            self.stage_encoded_object(page_id, page_bytes)?;
        }
        for (object_id, (_, bytes)) in encoded_commits {
            self.stage_encoded_object(object_id, bytes)?;
        }
        self.stage_encoded_object(ref_object_id, ref_bytes)?;
        let snapshot_id = self.stage_branch_snapshot(branch_snapshot)?;
        self.stage_repository_root(repository_root)?;
        self.install_branch_selector(view, snapshot_id)
    }

    /// Atomically moves reachability from one open receipt to a state-rooted
    /// blob manifest. A completion proof from another view, a state edit that
    /// omits the manifest, or a receipt changed after validation is rejected.
    pub(crate) async fn publish_completed_upload<R>(
        &mut self,
        view: &CoherentView<R>,
        completion: CompletedUpload,
        transition: BranchStateTransition,
    ) -> Result<ObjectId, StorageError>
    where
        R: StorageAdapterRead,
    {
        if completion.view_id != view.view_id() {
            return Err(corruption(
                "upload completion was derived from another coherent view",
            ));
        }
        let (manifest_id, _) = completion.manifest.encode()?;
        if !transition
            .state_edit
            .added_blob_roots
            .contains_key(&manifest_id)
        {
            return Err(corruption(
                "completed blob manifest is absent from the published state edit",
            ));
        }
        self.stage_immutable_objects(&completion.merkle_objects)?;
        self.stage_blob_manifest(&completion.manifest)?;
        self.delete_upload_selector(&completion.selector, completion.raw_upload_selector)?;
        self.publish_state_transition(view, transition).await?;
        Ok(manifest_id)
    }

    fn install_branch_selector<R>(
        &mut self,
        view: &CoherentView<R>,
        snapshot_id: ObjectId,
    ) -> Result<ObjectId, StorageError>
    where
        R: StorageAdapterRead,
    {
        let selector_generation = view
            .branch_selector()
            .selector_generation
            .checked_add(1)
            .ok_or_else(|| corruption("branch selector generation overflowed"))?;
        self.put_branch_selector(
            BranchSelectorV1 {
                branch_id: view.branch_id(),
                branch_snapshot_object_id: snapshot_id,
                selector_generation,
            },
            SelectorExpectation::Equals(view.raw_branch_selector().clone()),
        )?;
        Ok(snapshot_id)
    }

    pub(super) fn put_branch_selector(
        &mut self,
        value: BranchSelectorV1,
        expected: SelectorExpectation,
    ) -> Result<(), StorageError> {
        self.put_selector(
            branch_selector_key(value.branch_id),
            value.encode()?,
            expected,
        )
    }

    pub(super) fn put_upload_selector(
        &mut self,
        value: &UploadSelectorV1,
        expected: SelectorExpectation,
    ) -> Result<(), StorageError> {
        self.put_selector(
            upload_selector_key(&value.upload_id)?,
            value.encode()?,
            expected,
        )
    }

    pub(super) fn put_snapshot_selector(
        &mut self,
        value: SnapshotSelectorV1,
        expected: SelectorExpectation,
    ) -> Result<(), StorageError> {
        self.put_selector(
            snapshot_selector_key(value.role, value.selector_id),
            value.encode()?,
            expected,
        )
    }

    pub(super) fn delete_upload_selector(
        &mut self,
        value: &UploadSelectorV1,
        expected: Bytes,
    ) -> Result<(), StorageError> {
        self.delete_selector(upload_selector_key(&value.upload_id)?, expected)
    }

    pub(super) fn delete_branch_selector(
        &mut self,
        value: BranchSelectorV1,
        expected: Bytes,
    ) -> Result<(), StorageError> {
        self.delete_selector(branch_selector_key(value.branch_id), expected)
    }

    pub(super) fn delete_snapshot_selector(
        &mut self,
        value: SnapshotSelectorV1,
        expected: Bytes,
    ) -> Result<(), StorageError> {
        self.delete_selector(
            snapshot_selector_key(value.role, value.selector_id),
            expected,
        )
    }

    /// Retires one selected branch and atomically installs owner-produced
    /// catalog pruning under the same epoch. The path-copied catalog edits are
    /// the retirement proof; immutable branch/commit/state objects become
    /// sweep candidates only after this selector/catalog move commits.
    #[cfg(test)]
    pub(crate) fn publish_branch_retirement<R>(
        &mut self,
        view: &CoherentView<R>,
        commit_catalog_edit: CatalogTreeEdit,
        change_catalog_edit: CatalogTreeEdit,
        repository_root: RepositoryRootV1,
    ) -> Result<(), StorageError>
    where
        R: StorageAdapterRead,
    {
        if self.expected_global.as_ref() != view.raw_global_selector().as_ref()
            || repository_root.commit_catalog_root != commit_catalog_edit.root
            || repository_root.change_catalog_root != change_catalog_edit.root
        {
            return Err(corruption(
                "branch retirement catalogs or global view are inconsistent",
            ));
        }
        self.stage_catalog_edit(commit_catalog_edit)?;
        self.stage_catalog_edit(change_catalog_edit)?;
        self.stage_repository_root(repository_root)?;
        self.delete_branch_selector(view.branch_selector(), view.raw_branch_selector().clone())
    }

    /// Lowers one authenticated publication into the engine's canonical
    /// in-memory write/precondition plan.
    ///
    /// Transaction commit extends this plan with runtime, idempotency, and
    /// orthogonal metadata before issuing the existing single backend commit.
    /// This function performs no I/O and cannot publish a partial ForkTree
    /// transition independently.
    pub(crate) fn into_storage_plan(
        self,
    ) -> Result<(StorageWriteSet, Vec<StoragePrecondition>), StorageError> {
        let mut preconditions = Vec::with_capacity(1 + self.selector_expectations.len());
        preconditions.push(Precondition::KeyValueEquals {
            space: SELECTOR_SPACE,
            key: Key(global_selector_key()),
            expected: self.expected_global.clone(),
        });
        // The exact global bytes are also the GC publication fence: every GC
        // checkpoint rotates them atomically with its progress selector. A
        // logical publication that wins first discards that now-stale,
        // rebuildable selector in this same commit. A GC checkpoint that wins
        // first changes the expected global bytes and rejects this writer.
        for (key, expected) in &self.selector_expectations {
            preconditions.push(match expected {
                SelectorExpectation::Absent => Precondition::KeyAbsent {
                    space: SELECTOR_SPACE,
                    key: Key(key.clone()),
                },
                SelectorExpectation::Equals(expected) => Precondition::KeyValueEquals {
                    space: SELECTOR_SPACE,
                    key: Key(key.clone()),
                    expected: expected.clone(),
                },
            });
        }
        let next_global = self.next_global.encode()?;
        let preserves_gc_progress_selector =
            self.selector_puts.contains_key(&gc_progress_selector_key())
                || self.selector_deletes.contains(&gc_progress_selector_key());
        let mut writes = StorageWriteSet::with_capacity(
            self.object_puts
                .iter()
                .count()
                .saturating_add(self.selector_puts.len())
                .saturating_add(self.selector_deletes.len())
                .saturating_add(self.object_deletes.len())
                .saturating_add(2),
            3,
        );
        for (id, bytes) in self.object_puts.iter() {
            writes.put(OBJECT_SPACE, id.as_bytes().to_vec(), bytes.to_vec());
        }
        for id in self.object_deletes {
            writes.delete(OBJECT_SPACE, id.as_bytes().to_vec());
        }
        writes.put(
            SELECTOR_SPACE,
            global_selector_key().to_vec(),
            next_global.to_vec(),
        );
        for (key, value) in self.selector_puts {
            writes.put(SELECTOR_SPACE, key.to_vec(), value.to_vec());
        }
        let gc_key = gc_progress_selector_key();
        if !preserves_gc_progress_selector {
            writes.delete(SELECTOR_SPACE, gc_key.to_vec());
        }
        for key in self.selector_deletes {
            writes.delete(SELECTOR_SPACE, key.to_vec());
        }
        Ok((writes, preconditions))
    }
}
