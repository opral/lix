use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::storage::{Key, Precondition, StorageError};
use crate::storage_adapter::StorageAdapterRead;
use crate::storage_adapter::{StoragePrecondition, StorageWriteSet};

use super::blob::{CompletedUpload, PreparedUploadPart};
use super::codec::corruption;
use super::model::{
    BlobChunkRefV1, BlobChunkV1, BlobManifestV1, BranchSelectorV1, BranchSnapshotV1,
    ChangeCatalogEntry, ChangeCatalogOwner, ChangeObjectV1, CommitObjectV1, GlobalSelectorV1,
    RepositoryRootV1, SnapshotSelectorV1, SnapshotTargetV1, UploadPartV1, UploadProgressV1,
    UploadSelectorV1, branch_selector_key, gc_progress_selector_key, global_selector_key,
    snapshot_selector_key, upload_selector_key,
};
use super::object::{OBJECT_SPACE, ObjectId};
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
            next_repository_root: Some(view.repository_root()),
            selector_expectations: BTreeMap::new(),
            selector_puts: BTreeMap::new(),
            selector_deletes: BTreeSet::new(),
            object_puts: ImmutableObjectSet::default(),
            object_deletes: BTreeSet::new(),
            untracked_puts: BTreeMap::new(),
            untracked_deletes: BTreeSet::new(),
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
            untracked_puts: BTreeMap::new(),
            untracked_deletes: BTreeSet::new(),
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
        for (key, value) in other.untracked_puts {
            if self.untracked_deletes.contains(&key) {
                return Err(corruption(
                    "publication both puts and deletes one untracked row",
                ));
            }
            match self.untracked_puts.get(&key) {
                Some(existing) if existing != &value => {
                    return Err(corruption("publications assign conflicting untracked rows"));
                }
                Some(_) => {}
                None => {
                    self.untracked_puts.insert(key, value);
                }
            }
        }
        for key in other.untracked_deletes {
            if self.untracked_puts.contains_key(&key) {
                return Err(corruption(
                    "publication both puts and deletes one untracked row",
                ));
            }
            self.untracked_deletes.insert(key);
        }
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
                    change_object_id: ref_object_id,
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
            retention_policy_root: base_repository_root.retention_policy_root,
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
                    change_object_id: ref_object_id,
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
            retention_policy_root: base_repository_root.retention_policy_root,
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

    /// Lowers an inline file payload into the same authenticated object set as
    /// every other ForkTree blob publication. The returned manifest identity
    /// is later attached to the exact `lix_binary_blob_ref` state row; no
    /// BlobId-only reader or separate CAS commit is involved.
    pub(crate) fn stage_inline_blob_payload(
        &mut self,
        bytes: &[u8],
    ) -> Result<ObjectId, StorageError> {
        if bytes.is_empty() {
            return Err(corruption(
                "empty inline payload has no blob manifest; omit its BlobRef row",
            ));
        }
        let mut ordered_chunks = Vec::with_capacity(bytes.len().div_ceil(1024 * 1024));
        for chunk_bytes in bytes.chunks(1024 * 1024) {
            let chunk = BlobChunkV1 {
                bytes: Bytes::copy_from_slice(chunk_bytes),
            };
            let (chunk_object_id, encoded) = chunk.encode()?;
            self.stage_encoded_object(chunk_object_id, encoded)?;
            ordered_chunks.push(BlobChunkRefV1 {
                chunk_object_id,
                declared_len: chunk_bytes.len() as u64,
            });
        }
        let manifest = BlobManifestV1::from_authenticated_chunks(
            bytes.len() as u64,
            ordered_chunks,
            crate::binary_cas::BlobId::from_content(bytes),
            *blake3::hash(bytes).as_bytes(),
        );
        self.stage_blob_manifest(&manifest)
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
            || repository_root.retention_policy_root != view.repository_root().retention_policy_root
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
        for (ordinal, member) in semantic_commit.members.iter().copied().enumerate() {
            let member_id = member.change_object_id();
            if member.source().is_some() {
                return Err(corruption(
                    "single-transition publication cannot introduce selected history",
                ));
            }
            if !expected_changes.insert(member_id) {
                return Err(corruption("semantic commit repeats one member Change"));
            }
            let Some((ChangeObjectV1::Semantic { change_id, .. }, _)) =
                encoded_changes.get(&member_id)
            else {
                return Err(corruption(
                    "semantic commit member is absent or is a branch RefChange",
                ));
            };
            let expected = ChangeCatalogEntry {
                change_object_id: member_id,
                owner: ChangeCatalogOwner::CommitMember {
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
        let expected = ChangeCatalogEntry {
            change_object_id: ref_id,
            owner: ChangeCatalogOwner::BranchRef {
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
            || repository_root.retention_policy_root != view.repository_root().retention_policy_root
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

        let mut encoded_fresh_changes = BTreeMap::new();
        for change in &fresh_changes {
            let (object_id, bytes) = change.encode()?;
            if !matches!(change, ChangeObjectV1::Semantic { .. })
                || encoded_fresh_changes
                    .insert(object_id, (change, bytes))
                    .is_some()
            {
                return Err(corruption(
                    "ordered history repeats or misclassifies one fresh Change object",
                ));
            }
        }

        let is_global = state_domain_global;
        let mut expected_state_base = if is_global {
            view.repository_root().global_state_root
        } else {
            view.branch_snapshot().local_state_root
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
                view.repository_root().global_state_root
            };
            let expected_local_root = if is_global {
                view.branch_snapshot().local_state_root
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
            for parent_id in &commit.parent_commit_object_ids {
                if !parent_objects.insert(*parent_id) {
                    return Err(corruption("ordered Commit repeats one parent edge"));
                }
                let parent = if let Some((parent, _)) = encoded_commits.get(parent_id) {
                    (*parent).clone()
                } else {
                    let bytes = view.load_object_bytes(*parent_id).await?;
                    CommitObjectV1::decode(*parent_id, &bytes)?
                };
                if parent.generation >= commit.generation {
                    return Err(corruption(
                        "ordered Commit generation does not strictly follow every parent",
                    ));
                }
            }

            let mut member_ids = BTreeSet::new();
            for (ordinal, member) in commit.members.iter().copied().enumerate() {
                let change_object_id = member.change_object_id();
                if !member_ids.insert(change_object_id) {
                    return Err(corruption("ordered Commit repeats one Change member"));
                }
                match member.source() {
                    None => {
                        let Some((ChangeObjectV1::Semantic { change_id, .. }, _)) =
                            encoded_fresh_changes.get(&change_object_id)
                        else {
                            return Err(corruption(
                                "introduced member is absent from fresh semantic Changes",
                            ));
                        };
                        let expected = ChangeCatalogEntry {
                            change_object_id,
                            owner: ChangeCatalogOwner::CommitMember {
                                commit_object_id,
                                ordinal: u32::try_from(ordinal).map_err(|_| {
                                    corruption("ordered Commit member ordinal exceeds u32")
                                })?,
                            },
                        };
                        if change_catalog_edit.change_entries.get(change_id) != Some(&expected) {
                            return Err(corruption(
                                "fresh ChangeCatalog introduction owner is inconsistent",
                            ));
                        }
                    }
                    Some(_) => {
                        let bytes = view.load_object_bytes(change_object_id).await?;
                        let change = ChangeObjectV1::decode(change_object_id, &bytes)?;
                        if !matches!(change, ChangeObjectV1::Semantic { .. }) {
                            return Err(corruption(
                                "selected history member names a non-semantic Change",
                            ));
                        }
                        let raw_entry = view
                            .lookup_tree_value(
                                view.repository_root().change_catalog_root,
                                "change",
                                change.change_id().as_bytes(),
                            )
                            .await?
                            .ok_or_else(|| {
                                corruption("selected history member has no ChangeCatalog owner")
                            })?;
                        view.validate_member_catalog_owner(
                            view.repository_root().commit_catalog_root,
                            commit_object_id,
                            commit.generation,
                            ordinal,
                            member,
                            ChangeCatalogEntry::decode(&raw_entry)?,
                        )
                        .await?;
                    }
                }
            }
        }

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
        let expected_ref_entry = ChangeCatalogEntry {
            change_object_id: ref_object_id,
            owner: ChangeCatalogOwner::BranchRef {
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
        for (object_id, (_, bytes)) in encoded_fresh_changes {
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

    /// Retires one selected branch and atomically installs owner-produced
    /// catalog pruning under the same epoch. The path-copied catalog edits are
    /// the retirement proof; immutable branch/commit/state objects become
    /// sweep candidates only after this selector/catalog move commits.
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
                .saturating_add(self.untracked_puts.len())
                .saturating_add(self.untracked_deletes.len())
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
        for (key, value) in self.untracked_puts {
            writes.put(UNTRACKED_ROW_SPACE, key.to_vec(), value.to_vec());
        }
        for key in self.untracked_deletes {
            writes.delete(UNTRACKED_ROW_SPACE, key.to_vec());
        }
        Ok((writes, preconditions))
    }
}
