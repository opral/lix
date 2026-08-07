use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::storage::{
    Key, Precondition, PutBatch, PutEntry, Storage, StorageError, StorageWrite, StoredValue,
    WriteOptions,
};
use crate::storage_adapter::StorageAdapterRead;

use super::blob::CompletedUpload;
use super::codec::corruption;
use super::model::{
    BlobChunkV1, BlobManifestV1, BranchSelectorV1, BranchSnapshotV1, ChangeObjectV1,
    CommitObjectV1, GcMarkPackV1, GcProgressSelectorV1, GcProgressV1, GlobalSelectorV1,
    SnapshotSelectorV1, SnapshotTargetV1, UploadPartV1, UploadProgressV1, UploadSelectorV1,
    branch_selector_key, gc_progress_selector_key, global_selector_key, snapshot_selector_key,
    upload_selector_key,
};
use super::object::{OBJECT_SPACE, ObjectId};
use super::reachability::SweepPlan;
use super::serving::{CatalogTreeEdit, StateTreeEdit};
use super::state::{
    StateKeyRef, UNTRACKED_ROW_SPACE, UntrackedValueRef, encode_untracked_key,
    encode_untracked_value,
};
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
    pub(crate) repository_root: super::model::RepositoryRootV1,
}

/// One prepared atomic publication. It always exact-CASes and rotates the
/// global epoch, including fully deduplicated and root-only publications.
/// Object and selector puts are staged into the same storage write; no extra
/// flush or round trip exists at this boundary.
#[derive(Debug)]
pub(crate) struct PreparedPublication {
    expected_global: Bytes,
    next_global: GlobalSelectorV1,
    selector_expectations: BTreeMap<Bytes, SelectorExpectation>,
    selector_puts: BTreeMap<Bytes, Bytes>,
    selector_deletes: BTreeSet<Bytes>,
    object_puts: ImmutableObjectSet,
    object_deletes: BTreeSet<ObjectId>,
    untracked_puts: BTreeMap<Bytes, Bytes>,
    untracked_deletes: BTreeSet<Bytes>,
}

impl PreparedPublication {
    /// Starts a branch/state publication and fences both raw selectors from
    /// the one coherent view used to derive it.
    pub(crate) fn from_branch_view<R>(view: &CoherentView<R>) -> Result<Self, StorageError>
    where
        R: StorageAdapterRead,
    {
        let mut publication = Self::from_global_epoch(view)?;
        publication.expect_selector(
            branch_selector_key(view.branch_id()),
            SelectorExpectation::Equals(view.raw_branch_selector().clone()),
        )?;
        Ok(publication)
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
            selector_expectations: BTreeMap::new(),
            selector_puts: BTreeMap::new(),
            selector_deletes: BTreeSet::new(),
            object_puts: ImmutableObjectSet::default(),
            object_deletes: BTreeSet::new(),
            untracked_puts: BTreeMap::new(),
            untracked_deletes: BTreeSet::new(),
        })
    }

    pub(super) fn stage_repository_root(
        &mut self,
        root: super::model::RepositoryRootV1,
    ) -> Result<ObjectId, StorageError> {
        let (id, bytes) = root.encode()?;
        self.stage_encoded_object(id, bytes)?;
        self.next_global.repository_root = id;
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
        if self.object_deletes.contains(&id) {
            return Err(corruption("publication both puts and deletes one object"));
        }
        self.object_puts.insert(id, bytes)
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

    pub(super) fn stage_gc_mark_pack(
        &mut self,
        value: &GcMarkPackV1,
    ) -> Result<ObjectId, StorageError> {
        let (id, bytes) = value.encode()?;
        self.stage_encoded_object(id, bytes)?;
        Ok(id)
    }

    pub(super) fn stage_gc_progress(
        &mut self,
        value: &GcProgressV1,
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
                selector_generation: 1,
            },
            expected,
        )?;
        Ok(target_id)
    }

    pub(crate) fn release_snapshot_pin(
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

    pub(crate) fn publish_gc_progress(
        &mut self,
        mark_pack: &GcMarkPackV1,
        object_scan_after: Option<Vec<u8>>,
        discovered_epoch: u64,
        selector_generation: u64,
        expected: SelectorExpectation,
    ) -> Result<ObjectId, StorageError> {
        let mark_pack_object_id = self.stage_gc_mark_pack(mark_pack)?;
        let progress = GcProgressV1 {
            mark_pack_object_id,
            object_scan_after,
            discovered_epoch,
        };
        let progress_object_id = self.stage_gc_progress(&progress)?;
        self.put_gc_progress_selector(
            GcProgressSelectorV1 {
                progress_object_id,
                selector_generation,
            },
            expected,
        )?;
        Ok(progress_object_id)
    }

    pub(crate) fn release_gc_progress(
        &mut self,
        selector: GcProgressSelectorV1,
        raw_selector: Bytes,
    ) -> Result<(), StorageError> {
        if GcProgressSelectorV1::decode(&raw_selector)? != selector {
            return Err(corruption(
                "GC progress release raw selector does not match typed selector",
            ));
        }
        self.delete_gc_progress_selector(raw_selector)
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
            semantic_commit,
            changes,
            branch_snapshot,
            repository_root,
        } = transition;
        if branch_snapshot.branch_id != view.branch_id() {
            return Err(corruption(
                "state transition branch snapshot has another branch ID",
            ));
        }
        let is_global = repository_root.global_state_root == state_edit.root
            && branch_snapshot.local_state_root == view.branch_snapshot().local_state_root
            && branch_snapshot.historical_global_state_root == state_edit.root;
        let is_branch_local = branch_snapshot.local_state_root == state_edit.root
            && repository_root.global_state_root == view.repository_root().global_state_root
            && branch_snapshot.historical_global_state_root
                == view.repository_root().global_state_root;
        if is_global == is_branch_local {
            return Err(corruption(
                "state edit must install at exactly one global or branch-local root",
            ));
        }
        let expected_state_base = if is_global {
            view.repository_root().global_state_root
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
        if commit_catalog_edit.base_root != view.repository_root().commit_catalog_root
            || change_catalog_edit.base_root != view.repository_root().change_catalog_root
            || repository_root.commit_catalog_root != commit_catalog_edit.root
            || repository_root.change_catalog_root != change_catalog_edit.root
            || repository_root.retention_policy_root != view.repository_root().retention_policy_root
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
        if semantic_commit.parent_commit_object_ids.is_empty()
            || !semantic_commit
                .parent_commit_object_ids
                .contains(&view.branch_snapshot().semantic_head_commit_object_id)
        {
            return Err(corruption(
                "semantic commit does not descend from the selected branch head",
            ));
        }
        let mut parent_ids = BTreeSet::new();
        for parent_id in &semantic_commit.parent_commit_object_ids {
            if !parent_ids.insert(*parent_id) {
                return Err(corruption("semantic commit repeats one parent"));
            }
            let bytes = view.load_object_bytes(*parent_id).await?;
            let parent = CommitObjectV1::decode(*parent_id, &bytes)?;
            if parent.generation >= semantic_commit.generation {
                return Err(corruption(
                    "semantic commit generation does not follow every parent",
                ));
            }
        }

        let mut encoded_changes = BTreeMap::new();
        for change in &changes {
            let (id, bytes) = change.encode()?;
            if encoded_changes.insert(id, (change, bytes)).is_some() {
                return Err(corruption("state transition repeats one Change object"));
            }
        }
        let mut expected_changes = BTreeSet::new();
        for (ordinal, member_id) in semantic_commit.member_change_object_ids.iter().enumerate() {
            if !expected_changes.insert(*member_id) {
                return Err(corruption("semantic commit repeats one member Change"));
            }
            let Some((ChangeObjectV1::Semantic { change_id, .. }, _)) =
                encoded_changes.get(member_id)
            else {
                return Err(corruption(
                    "semantic commit member is absent or is a branch RefChange",
                ));
            };
            let expected = super::model::ChangeCatalogEntry {
                change_object_id: *member_id,
                owner: super::model::ChangeCatalogOwner::CommitMember {
                    commit_object_id: commit_id,
                    ordinal: u32::try_from(ordinal)
                        .map_err(|_| corruption("semantic commit ordinal exceeds u32"))?,
                },
            };
            if change_catalog_edit.change_entries.get(change_id) != Some(&expected) {
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
            change_object_id: ref_id,
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
        expected_changes.insert(ref_id);
        if encoded_changes.keys().copied().collect::<BTreeSet<_>>() != expected_changes {
            return Err(corruption(
                "state transition Change set is not exactly the commit members and RefChange",
            ));
        }

        self.stage_state_edit(state_edit)?;
        self.stage_catalog_edit(commit_catalog_edit)?;
        self.stage_catalog_edit(change_catalog_edit)?;
        self.stage_encoded_object(commit_id, commit_bytes)?;
        for (id, (_, bytes)) in encoded_changes {
            self.stage_encoded_object(id, bytes)?;
        }
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
            .contains(&manifest_id)
        {
            return Err(corruption(
                "completed blob manifest is absent from the published state edit",
            ));
        }
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

    pub(super) fn put_gc_progress_selector(
        &mut self,
        value: GcProgressSelectorV1,
        expected: SelectorExpectation,
    ) -> Result<(), StorageError> {
        self.put_selector(gc_progress_selector_key(), value.encode()?, expected)
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

    pub(super) fn delete_gc_progress_selector(
        &mut self,
        expected: Bytes,
    ) -> Result<(), StorageError> {
        self.delete_selector(gc_progress_selector_key(), expected)
    }

    /// Stages one current untracked row under the same global epoch as every
    /// CAS/root mutation. This prevents an untracked blob reference from
    /// reviving a payload concurrently with sweep.
    pub(crate) fn put_untracked_row(
        &mut self,
        branch_id: super::model::CanonicalBranchId,
        key: StateKeyRef<'_>,
        value: UntrackedValueRef<'_>,
    ) -> Result<(), StorageError> {
        let key = Bytes::from(encode_untracked_key(branch_id, key));
        if self.untracked_deletes.contains(&key) {
            return Err(corruption(
                "publication both puts and deletes one untracked row",
            ));
        }
        let value = Bytes::from(
            encode_untracked_value(value).map_err(|error| corruption(error.to_string()))?,
        );
        match self.untracked_puts.get(&key) {
            Some(existing) if existing != &value => Err(corruption(
                "publication assigns two values to one untracked row",
            )),
            Some(_) => Ok(()),
            None => {
                self.untracked_puts.insert(key, value);
                Ok(())
            }
        }
    }

    pub(crate) fn delete_untracked_row(
        &mut self,
        branch_id: super::model::CanonicalBranchId,
        key: StateKeyRef<'_>,
    ) -> Result<(), StorageError> {
        let key = Bytes::from(encode_untracked_key(branch_id, key));
        if self.untracked_puts.contains_key(&key) {
            return Err(corruption(
                "publication both puts and deletes one untracked row",
            ));
        }
        self.untracked_deletes.insert(key);
        Ok(())
    }

    pub(crate) fn apply_sweep_plan(&mut self, plan: SweepPlan) -> Result<(), StorageError> {
        if plan.expected_global != self.expected_global {
            return Err(corruption(
                "sweep proof was discovered under another global selector",
            ));
        }
        for id in plan.orphan_object_ids {
            if id == ObjectId::ZERO || self.object_puts.get(id).is_some() {
                return Err(corruption(
                    "sweep proof contains an invalid object deletion",
                ));
            }
            self.object_deletes.insert(id);
        }
        Ok(())
    }

    /// Retires one selected branch and atomically installs owner-produced
    /// catalog pruning under the same epoch. The path-copied catalog edits are
    /// the retirement proof; immutable branch/commit/state objects become
    /// sweep candidates only after this selector/catalog move commits.
    pub(crate) fn publish_branch_retirement<R>(
        &mut self,
        view: &CoherentView<R>,
        commit_catalog_edit: CatalogTreeEdit,
        change_catalog_edit: CatalogTreeEdit,
        repository_root: super::model::RepositoryRootV1,
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

    pub(crate) async fn commit<S>(self, storage: &S) -> Result<(), StorageError>
    where
        S: Storage,
    {
        let mut preconditions = Vec::with_capacity(1 + self.selector_expectations.len());
        preconditions.push(Precondition::KeyValueEquals {
            space: SELECTOR_SPACE,
            key: Key(global_selector_key()),
            expected: self.expected_global.clone(),
        });
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
        let capacity = self
            .object_puts
            .iter()
            .map(|(_, bytes)| bytes.len())
            .sum::<usize>()
            .saturating_add(
                self.selector_puts
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>(),
            )
            .saturating_add(
                self.untracked_puts
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>(),
            )
            .saturating_add(next_global.len());
        let mut write = storage
            .begin_write(WriteOptions {
                preconditions,
                batch_capacity_hint_bytes: capacity,
                ..WriteOptions::default()
            })
            .await?;
        if !self.object_puts.is_empty() {
            write
                .put_many(
                    OBJECT_SPACE,
                    PutBatch {
                        entries: self
                            .object_puts
                            .iter()
                            .map(|(id, bytes)| PutEntry {
                                key: Key(Bytes::copy_from_slice(id.as_bytes())),
                                value: StoredValue {
                                    bytes: bytes.clone(),
                                },
                            })
                            .collect(),
                    },
                )
                .await?;
        }
        let mut selector_entries = Vec::with_capacity(self.selector_puts.len() + 1);
        selector_entries.push(PutEntry {
            key: Key(global_selector_key()),
            value: StoredValue { bytes: next_global },
        });
        selector_entries.extend(self.selector_puts.into_iter().map(|(key, value)| PutEntry {
            key: Key(key),
            value: StoredValue { bytes: value },
        }));
        write
            .put_many(
                SELECTOR_SPACE,
                PutBatch {
                    entries: selector_entries,
                },
            )
            .await?;
        if !self.selector_deletes.is_empty() {
            let keys = self
                .selector_deletes
                .into_iter()
                .map(Key)
                .collect::<Vec<_>>();
            write.delete_many(SELECTOR_SPACE, &keys).await?;
        }
        if !self.untracked_puts.is_empty() {
            write
                .put_many(
                    UNTRACKED_ROW_SPACE,
                    PutBatch {
                        entries: self
                            .untracked_puts
                            .into_iter()
                            .map(|(key, value)| PutEntry {
                                key: Key(key),
                                value: StoredValue { bytes: value },
                            })
                            .collect(),
                    },
                )
                .await?;
        }
        if !self.untracked_deletes.is_empty() {
            let keys = self
                .untracked_deletes
                .into_iter()
                .map(Key)
                .collect::<Vec<_>>();
            write.delete_many(UNTRACKED_ROW_SPACE, &keys).await?;
        }
        if !self.object_deletes.is_empty() {
            let keys = self
                .object_deletes
                .into_iter()
                .map(|id| Key(Bytes::copy_from_slice(id.as_bytes())))
                .collect::<Vec<_>>();
            write.delete_many(OBJECT_SPACE, &keys).await?;
        }
        write.commit().await?;
        Ok(())
    }
}
