//! A finite upload wave containing immutable IDs and captured branch intents.
//!
//! Storage and transport stay outside this module. A page is only a proposal:
//! callers load its payloads from a fresh read and acknowledge it only after
//! the server's receipt has been imported durably. Appended local work belongs
//! to the next wave, so a continuous writer cannot postpone these refs forever.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::LixError;
use crate::changelog::CommitId;
use crate::storage_adapter::{
    StorageAdapterRead, StorageGetManyRequest, StorageGetOptions, StorageKey, StoragePrecondition,
    StorageProjectedValue, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
    ValueSemantics, exact_get_many,
};

use super::protocol::SyncRefUpdate;

/// An opaque invalidation token, not a monotonic counter or an outbox.
/// Destructive ref changes publish a replacement atomically with their state;
/// ordinary appends and acknowledgments leave an existing wave usable.
pub(crate) const SYNC_UPLOAD_GENERATION_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0017),
    "sync.upload_generation.v1",
    ValueSemantics::Mutable,
);

fn generation_key() -> StorageKey {
    StorageKey(Bytes::new())
}

pub(crate) async fn load_generation(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<u128, LixError> {
    let key = generation_key();
    let values = exact_get_many(
        read,
        &[StorageGetManyRequest {
            space: SYNC_UPLOAD_GENERATION_SPACE,
            keys: std::slice::from_ref(&key),
            opts: StorageGetOptions::default(),
        }],
    )
    .await?;
    let Some(value) = values.values.into_iter().next().flatten() else {
        return Ok(0);
    };
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync upload generation read omitted its value",
        ));
    };
    let bytes: [u8; 16] = bytes.as_ref().try_into().map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync upload generation must contain sixteen bytes",
        )
    })?;
    Ok(u128::from_le_bytes(bytes))
}

/// Bind preparation to the same destructive-intent generation as its read.
/// A restore cannot race between this check and publication of a request proof.
pub(crate) fn generation_precondition(generation: u128) -> StoragePrecondition {
    if generation == 0 {
        StoragePrecondition::KeyAbsent {
            space: SYNC_UPLOAD_GENERATION_SPACE,
            key: generation_key(),
        }
    } else {
        StoragePrecondition::KeyValueEquals {
            space: SYNC_UPLOAD_GENERATION_SPACE,
            key: generation_key(),
            expected: Bytes::copy_from_slice(&generation.to_le_bytes()),
        }
    }
}

/// A fresh token avoids an extra read and read-modify-write contention between
/// engines. Equality is the only operation callers may perform on generations.
pub(crate) fn stage_invalidate(writes: &mut StorageWriteSet) {
    // A transaction can replace several branches or stage restore intent too.
    // One replacement token fences the whole atomic publication.
    if writes.contains_put(SYNC_UPLOAD_GENERATION_SPACE, generation_key().0.as_ref()) {
        return;
    }
    let generation = uuid::Uuid::new_v4().as_u128();
    writes.put(
        SYNC_UPLOAD_GENERATION_SPACE,
        generation_key(),
        StorageValue {
            bytes: Bytes::copy_from_slice(&generation.to_le_bytes()),
        },
    );
}

#[derive(Debug)]
pub(crate) struct UploadPlan {
    generation: u128,
    commits: Vec<CommitId>,
    refs: Vec<SyncRefUpdate>,
    acknowledged_commits: usize,
    acknowledged_refs: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UploadPlanPage {
    pub(crate) commit_ids: Vec<CommitId>,
    pub(crate) ref_updates: Vec<SyncRefUpdate>,
    generation: u128,
    commit_offset: usize,
    ref_offset: usize,
}

impl UploadPlan {
    /// Orders each dependency once, then drops the graph. Only O(P + B) IDs
    /// and refs remain resident; no commit members, blobs, or storage read do.
    /// `known` must include every authority-known reset/dependency boundary.
    pub(crate) fn new(
        generation: u128,
        mut pending: BTreeMap<CommitId, BTreeSet<CommitId>>,
        known: &BTreeSet<CommitId>,
        captured_refs: Vec<SyncRefUpdate>,
    ) -> Result<Self, LixError> {
        pending.retain(|id, _| !known.contains(id));
        let mut branches = BTreeSet::new();
        for update in &captured_refs {
            if !branches.insert(&update.branch_id) {
                return Err(invalid_plan("upload wave contains duplicate branch refs"));
            }
            if update.head_commit_id.is_some() != update.checkpoint_commit_id.is_some() {
                return Err(invalid_plan(
                    "upload wave has an incomplete target coordinate",
                ));
            }
            for id in [
                update.head_commit_id.as_deref(),
                update.checkpoint_commit_id.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                let id = CommitId::parse_lix(id, "upload wave ref target")?;
                if !pending.contains_key(&id) && !known.contains(&id) {
                    return Err(invalid_plan(
                        "upload wave ref target is not dependency-complete",
                    ));
                }
            }
        }

        let mut counts = BTreeMap::new();
        let mut dependents = BTreeMap::<CommitId, Vec<CommitId>>::new();
        let mut ready = BTreeSet::new();
        for (id, dependencies) in &pending {
            let mut count = 0;
            for dependency in dependencies {
                if known.contains(dependency) {
                    continue;
                }
                if !pending.contains_key(dependency) {
                    return Err(invalid_plan("upload wave has an unavailable dependency"));
                }
                dependents.entry(*dependency).or_default().push(*id);
                count += 1;
            }
            counts.insert(*id, count);
            if count == 0 {
                ready.insert(*id);
            }
        }
        let mut commits = Vec::with_capacity(pending.len());
        while let Some(id) = ready.pop_first() {
            commits.push(id);
            for dependent in dependents.remove(&id).unwrap_or_default() {
                let count = counts.get_mut(&dependent).expect("indexed dependent");
                *count -= 1;
                if *count == 0 {
                    ready.insert(dependent);
                }
            }
        }
        if commits.len() != pending.len() {
            return Err(invalid_plan("upload wave commit graph has a cycle"));
        }
        Ok(Self {
            generation,
            commits,
            refs: captured_refs,
            acknowledged_commits: 0,
            acknowledged_refs: 0,
        })
    }

    pub(crate) fn generation(&self) -> u128 {
        self.generation
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.acknowledged_commits == self.commits.len() && self.acknowledged_refs == self.refs.len()
    }

    /// Repeating this call does not consume anything. A smaller limit after a
    /// 413 selects a shorter prefix of the same unacknowledged wave.
    pub(crate) fn page(&self, max_items: usize) -> Result<Option<UploadPlanPage>, LixError> {
        if max_items == 0 || max_items > super::MAX_SYNC_REQUEST_ITEMS {
            return Err(invalid_plan(
                "upload page item limit is outside protocol bounds",
            ));
        }
        if self.is_complete() {
            return Ok(None);
        }
        let commit_end = self
            .commits
            .len()
            .min(self.acknowledged_commits.saturating_add(max_items));
        let commit_ids = self.commits[self.acknowledged_commits..commit_end].to_vec();
        let remaining_capacity = max_items - commit_ids.len();
        // Publish the captured refs once this entire finite graph is ready.
        // This avoids rescanning all branches for readiness on every page.
        let ref_end = if commit_end == self.commits.len() {
            self.refs
                .len()
                .min(self.acknowledged_refs.saturating_add(remaining_capacity))
        } else {
            self.acknowledged_refs
        };
        Ok(Some(UploadPlanPage {
            commit_ids,
            ref_updates: self.refs[self.acknowledged_refs..ref_end].to_vec(),
            generation: self.generation,
            commit_offset: self.acknowledged_commits,
            ref_offset: self.acknowledged_refs,
        }))
    }

    /// Call only after the corresponding receipt has been imported durably
    /// The caller must revalidate its generation before constructing another page.
    pub(crate) fn acknowledge(&mut self, page: &UploadPlanPage) -> Result<(), LixError> {
        let item_count = page.commit_ids.len() + page.ref_updates.len();
        if page.generation != self.generation
            || page.commit_offset != self.acknowledged_commits
            || page.ref_offset != self.acknowledged_refs
            || self.page(item_count)?.as_ref() != Some(page)
        {
            return Err(invalid_plan(
                "upload acknowledgment does not match the pending page",
            ));
        }
        self.acknowledged_commits += page.commit_ids.len();
        self.acknowledged_refs += page.ref_updates.len();
        Ok(())
    }
}

fn invalid_plan(message: &str) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalidation_is_visible_only_in_the_committed_storage_snapshot() {
        use crate::storage_adapter::{
            Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions,
        };

        let storage = StorageAdapter::new(Memory::new());
        let before = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        assert_eq!(load_generation(&before).await.unwrap(), 0);
        let mut writes = storage.new_write_set();
        stage_invalidate(&mut writes);
        assert_eq!(load_generation(&before).await.unwrap(), 0);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let after = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        assert_ne!(load_generation(&after).await.unwrap(), 0);
        assert_eq!(load_generation(&before).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn multiple_invalidations_share_one_atomic_generation_write() {
        use crate::storage_adapter::{
            Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions,
        };

        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        stage_invalidate(&mut writes);
        stage_invalidate(&mut writes);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("restoring multiple branches must not stage duplicate generation keys");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        assert_ne!(load_generation(&read).await.unwrap(), 0);
    }

    fn id(label: &str) -> CommitId {
        CommitId::for_test_label(label)
    }

    fn branch(head: CommitId, checkpoint: CommitId) -> SyncRefUpdate {
        SyncRefUpdate {
            branch_id: "main".to_owned(),
            expected_head_commit_id: None,
            expected_checkpoint_commit_id: None,
            head_commit_id: Some(head.to_string()),
            checkpoint_commit_id: Some(checkpoint.to_string()),
        }
    }

    #[test]
    fn retries_and_smaller_pages_do_not_advance_the_wave() {
        let (base, first, second, third) = (id("base"), id("first"), id("second"), id("third"));
        let graph = BTreeMap::from([
            (first, BTreeSet::from([base])),
            (second, BTreeSet::from([first])),
            (third, BTreeSet::from([second])),
        ]);
        let mut plan =
            UploadPlan::new(4, graph, &BTreeSet::from([base]), vec![branch(third, base)])
                .expect("closed graph");
        assert_eq!(plan.generation(), 4);
        let original = plan.page(3).unwrap().unwrap();
        assert_eq!(plan.page(3).unwrap().unwrap(), original);
        let small = plan.page(1).unwrap().unwrap();
        assert_eq!(small.commit_ids, vec![first]);
        plan.acknowledge(&small).unwrap();
        assert!(
            plan.acknowledge(&small).is_err(),
            "duplicate receipt cannot skip a page"
        );
        assert!(
            plan.acknowledge(&original).is_err(),
            "superseded page is not an acknowledgment"
        );
        let remainder = plan.page(3).unwrap().unwrap();
        assert_eq!(remainder.commit_ids, vec![second, third]);
        assert_eq!(remainder.ref_updates, vec![branch(third, base)]);
        plan.acknowledge(&remainder).unwrap();
        assert!(plan.is_complete());
        assert!(plan.page(1).unwrap().is_none());
    }

    #[test]
    fn multiple_dependencies_precede_their_ref_and_metadata_is_page_bounded() {
        let (parent, base, alias, head) = (id("parent"), id("base"), id("alias"), id("head"));
        let mut plan = UploadPlan::new(
            0,
            BTreeMap::from([
                (head, BTreeSet::from([parent, base, alias])),
                (parent, BTreeSet::new()),
                (base, BTreeSet::new()),
                (alias, BTreeSet::new()),
            ]),
            &BTreeSet::new(),
            vec![branch(head, parent)],
        )
        .unwrap();
        let mut acknowledged = BTreeSet::new();
        let mut total_items = 0;
        while let Some(page) = plan.page(2).unwrap() {
            assert!(page.commit_ids.len() + page.ref_updates.len() <= 2);
            for commit in &page.commit_ids {
                if *commit == head {
                    assert!(
                        [parent, base, alias]
                            .iter()
                            .all(|id| acknowledged.contains(id))
                    );
                }
                acknowledged.insert(*commit);
            }
            if !page.ref_updates.is_empty() {
                assert_eq!(acknowledged.len(), 4);
            }
            total_items += page.commit_ids.len() + page.ref_updates.len();
            plan.acknowledge(&page).unwrap();
        }
        assert_eq!(total_items, 5);
    }

    #[test]
    fn known_objects_and_checkpoint_only_refs_need_no_reupload() {
        let (head, checkpoint) = (id("known-head"), id("known-checkpoint"));
        let mut plan = UploadPlan::new(
            0,
            BTreeMap::from([(head, BTreeSet::from([checkpoint]))]),
            &BTreeSet::from([head, checkpoint]),
            vec![branch(head, checkpoint)],
        )
        .unwrap();
        let page = plan.page(1).unwrap().unwrap();
        assert!(page.commit_ids.is_empty());
        assert_eq!(page.ref_updates.len(), 1);
        plan.acknowledge(&page).unwrap();
        assert!(plan.is_complete());
    }

    #[test]
    fn invalid_graphs_and_unavailable_checkpoint_targets_are_rejected() {
        let (first, second) = (id("first"), id("second"));
        assert!(
            UploadPlan::new(
                0,
                BTreeMap::from([(first, BTreeSet::from([second]))]),
                &BTreeSet::new(),
                vec![]
            )
            .is_err()
        );
        assert!(
            UploadPlan::new(
                0,
                BTreeMap::from([
                    (first, BTreeSet::from([second])),
                    (second, BTreeSet::from([first])),
                ]),
                &BTreeSet::new(),
                vec![]
            )
            .is_err()
        );
        assert!(
            UploadPlan::new(
                0,
                BTreeMap::from([(first, BTreeSet::new())]),
                &BTreeSet::new(),
                vec![branch(first, second)]
            )
            .is_err()
        );
    }

    #[test]
    fn modified_or_different_generation_pages_cannot_advance_a_plan() {
        let head = id("head");
        let graph = BTreeMap::from([(head, BTreeSet::new())]);
        let mut plan = UploadPlan::new(1, graph.clone(), &BTreeSet::new(), vec![]).unwrap();
        let other = UploadPlan::new(2, graph, &BTreeSet::new(), vec![]).unwrap();
        assert!(plan.acknowledge(&other.page(1).unwrap().unwrap()).is_err());
        let mut page = plan.page(1).unwrap().unwrap();
        page.commit_ids[0] = id("unrelated");
        assert!(plan.acknowledge(&page).is_err());
        assert!(!plan.is_complete());
        assert!(plan.page(0).is_err());
        assert!(plan.page(super::super::MAX_SYNC_REQUEST_ITEMS + 1).is_err());
    }
}
