//! Dependency-free model for the BranchHeadControl hard cut.
//!
//! This is intentionally not linked into Lix. It models only the accepted
//! GlobalSelectorV1/BranchSelectorV1 plus one epoch/CAS plane and is compiled
//! as a standalone test artifact by the report's exact command.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
struct GlobalSelector {
    repository_root: u64,
    epoch: u64,
    selector_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BranchSelector {
    branch_id: u8,
    snapshot_object: u64,
    selector_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BranchSnapshot {
    branch_id: u8,
    local_state_root: u64,
    historical_global_root: u64,
    semantic_head: u64,
    latest_ref_change: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SelectorBytes {
    Global(GlobalSelector),
    Branch(BranchSelector),
    Malformed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoreImage {
    global: SelectorBytes,
    branches: BTreeMap<u8, SelectorBytes>,
    snapshots: BTreeMap<u64, BranchSnapshot>,
    sequence: u64,
    next_object: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct View {
    branch_id: u8,
    raw_global: Vec<u8>,
    raw_branch: Vec<u8>,
    global: GlobalSelector,
    branch: BranchSelector,
    snapshot: BranchSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GcObservation {
    raw_global: Vec<u8>,
    reachable_snapshots: BTreeSet<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ModelError {
    Stale,
    Missing,
    Malformed,
    WrongKind,
    MissingObject,
    InvalidBranch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Store {
    global: SelectorBytes,
    branches: BTreeMap<u8, SelectorBytes>,
    snapshots: BTreeMap<u64, BranchSnapshot>,
    sequence: u64,
    next_object: u64,
}

impl Store {
    fn initialized() -> Self {
        let main_snapshot = BranchSnapshot {
            branch_id: 1,
            local_state_root: 10,
            historical_global_root: 10,
            semantic_head: 11,
            latest_ref_change: 12,
        };
        Self {
            global: SelectorBytes::Global(GlobalSelector {
                repository_root: 10,
                epoch: 1,
                selector_generation: 1,
            }),
            branches: BTreeMap::from([(
                1,
                SelectorBytes::Branch(BranchSelector {
                    branch_id: 1,
                    snapshot_object: 2,
                    selector_generation: 1,
                }),
            )]),
            snapshots: BTreeMap::from([(2, main_snapshot)]),
            sequence: 0,
            next_object: 20,
        }
    }

    fn image(&self) -> StoreImage {
        StoreImage {
            global: self.global.clone(),
            branches: self.branches.clone(),
            snapshots: self.snapshots.clone(),
            sequence: self.sequence,
            next_object: self.next_object,
        }
    }

    fn reopen(image: StoreImage) -> Self {
        Self {
            global: image.global,
            branches: image.branches,
            snapshots: image.snapshots,
            sequence: image.sequence,
            next_object: image.next_object,
        }
    }

    fn open(&self, branch_id: u8) -> Result<View, ModelError> {
        let global = match &self.global {
            SelectorBytes::Global(selector) => selector.clone(),
            SelectorBytes::Branch(_) => return Err(ModelError::WrongKind),
            SelectorBytes::Malformed => return Err(ModelError::Malformed),
        };
        let branch = match self.branches.get(&branch_id) {
            None => return Err(ModelError::Missing),
            Some(SelectorBytes::Global(_)) => return Err(ModelError::WrongKind),
            Some(SelectorBytes::Malformed) => return Err(ModelError::Malformed),
            Some(SelectorBytes::Branch(selector)) => selector.clone(),
        };
        if branch.branch_id != branch_id {
            return Err(ModelError::Malformed);
        }
        let snapshot = self
            .snapshots
            .get(&branch.snapshot_object)
            .cloned()
            .ok_or(ModelError::MissingObject)?;
        if snapshot.branch_id != branch_id {
            return Err(ModelError::Malformed);
        }
        Ok(View {
            branch_id,
            raw_global: encode_global(&global),
            raw_branch: encode_branch(&branch),
            global,
            branch,
            snapshot,
        })
    }

    fn snapshot(&self, branch_id: u8) -> Result<BranchSnapshot, ModelError> {
        Ok(self.open(branch_id)?.snapshot)
    }

    fn no_op(&self, view: &View) -> Result<(), ModelError> {
        self.check_view(view)
    }

    fn publish_branch(
        &mut self,
        view: &View,
        next_head: u64,
        next_local_root: u64,
    ) -> Result<(), ModelError> {
        self.check_view(view)?;
        let next_object = self.next_object;
        let next = BranchSnapshot {
            branch_id: view.branch_id,
            local_state_root: next_local_root,
            historical_global_root: view.snapshot.historical_global_root,
            semantic_head: next_head,
            latest_ref_change: next_object + 1,
        };
        let next_global = rotate(&view.global)?;
        let next_selector = BranchSelector {
            branch_id: view.branch_id,
            snapshot_object: next_object,
            selector_generation: view.branch.selector_generation + 1,
        };
        self.snapshots.insert(next_object, next);
        self.branches
            .insert(view.branch_id, SelectorBytes::Branch(next_selector));
        self.global = SelectorBytes::Global(next_global);
        self.next_object += 2;
        Ok(())
    }

    fn publish_sequence(&mut self, view: &View, next_sequence: u64) -> Result<bool, ModelError> {
        self.check_global(view)?;
        if next_sequence == self.sequence {
            return Ok(false);
        }
        self.global = SelectorBytes::Global(rotate(&view.global)?);
        self.sequence = next_sequence;
        Ok(true)
    }

    fn create_branch(&mut self, view: &View, branch_id: u8) -> Result<(), ModelError> {
        self.check_view(view)?;
        if self.branches.contains_key(&branch_id) {
            return Err(ModelError::Stale);
        }
        let snapshot_object = self.next_object;
        let snapshot = BranchSnapshot {
            branch_id,
            local_state_root: view.snapshot.local_state_root,
            historical_global_root: view.global.repository_root,
            semantic_head: view.snapshot.semantic_head,
            latest_ref_change: snapshot_object + 1,
        };
        self.snapshots.insert(snapshot_object, snapshot);
        self.branches.insert(
            branch_id,
            SelectorBytes::Branch(BranchSelector {
                branch_id,
                snapshot_object,
                selector_generation: 1,
            }),
        );
        self.global = SelectorBytes::Global(rotate(&view.global)?);
        self.next_object += 2;
        Ok(())
    }

    fn delete_branch(&mut self, view: &View) -> Result<(), ModelError> {
        self.check_view(view)?;
        if view.branch_id == 1 {
            return Err(ModelError::InvalidBranch);
        }
        self.branches.remove(&view.branch_id);
        self.global = SelectorBytes::Global(rotate(&view.global)?);
        Ok(())
    }

    fn switch_session(&self, active_branch: &mut u8, target: u8) -> Result<(), ModelError> {
        self.open(target)?;
        *active_branch = target;
        Ok(())
    }

    fn observe_gc(&self) -> Result<GcObservation, ModelError> {
        let global = match &self.global {
            SelectorBytes::Global(selector) => selector,
            SelectorBytes::Branch(_) => return Err(ModelError::WrongKind),
            SelectorBytes::Malformed => return Err(ModelError::Malformed),
        };
        let mut reachable = BTreeSet::new();
        for selector in self.branches.values() {
            let SelectorBytes::Branch(selector) = selector else {
                return Err(ModelError::WrongKind);
            };
            reachable.insert(selector.snapshot_object);
        }
        let _ = global;
        Ok(GcObservation {
            raw_global: encode_global(global),
            reachable_snapshots: reachable,
        })
    }

    fn sweep_gc(&mut self, observation: &GcObservation) -> Result<(), ModelError> {
        let current = self.raw_global();
        if current != observation.raw_global {
            return Err(ModelError::Stale);
        }
        let next_global = match &self.global {
            SelectorBytes::Global(selector) => rotate(selector)?,
            SelectorBytes::Branch(_) => return Err(ModelError::WrongKind),
            SelectorBytes::Malformed => return Err(ModelError::Malformed),
        };
        let unreachable: Vec<u64> = self
            .snapshots
            .keys()
            .copied()
            .filter(|id| !observation.reachable_snapshots.contains(id))
            .collect();
        self.global = SelectorBytes::Global(next_global);
        for id in unreachable {
            self.snapshots.remove(&id);
        }
        Ok(())
    }

    fn raw_global(&self) -> Vec<u8> {
        match &self.global {
            SelectorBytes::Global(selector) => encode_global(selector),
            SelectorBytes::Branch(selector) => encode_branch(selector),
            SelectorBytes::Malformed => b"malformed".to_vec(),
        }
    }

    fn check_global(&self, view: &View) -> Result<(), ModelError> {
        if self.raw_global() != view.raw_global {
            return Err(ModelError::Stale);
        }
        Ok(())
    }

    fn check_view(&self, view: &View) -> Result<(), ModelError> {
        self.check_global(view)?;
        let current = self
            .branches
            .get(&view.branch_id)
            .ok_or(ModelError::Missing)?;
        if current != &SelectorBytes::Branch(view.branch.clone()) {
            return Err(ModelError::Stale);
        }
        Ok(())
    }
}

fn rotate(selector: &GlobalSelector) -> Result<GlobalSelector, ModelError> {
    Ok(GlobalSelector {
        repository_root: selector.repository_root,
        epoch: selector.epoch.checked_add(1).ok_or(ModelError::Malformed)?,
        selector_generation: selector
            .selector_generation
            .checked_add(1)
            .ok_or(ModelError::Malformed)?,
    })
}

fn encode_global(selector: &GlobalSelector) -> Vec<u8> {
    format!(
        "G:{}:{}:{}",
        selector.repository_root, selector.epoch, selector.selector_generation
    )
    .into_bytes()
}

fn encode_branch(selector: &BranchSelector) -> Vec<u8> {
    format!(
        "B:{}:{}:{}",
        selector.branch_id, selector.snapshot_object, selector.selector_generation
    )
    .into_bytes()
}

fn assert_unchanged<F>(store: &mut Store, operation: F)
where
    F: FnOnce(&mut Store) -> Result<(), ModelError>,
{
    let before = store.image();
    assert_eq!(operation(store), Err(ModelError::Stale));
    assert_eq!(store.image(), before);
}

fn test_init_switch_noop_reopen() {
    let store = Store::initialized();
    let initial = store.image();
    let mut active = 1;
    store.switch_session(&mut active, 1).unwrap();
    assert_eq!(active, 1);
    let view = store.open(1).unwrap();
    store.no_op(&view).unwrap();
    assert_eq!(store.image(), initial);
    let reopened = Store::reopen(store.image());
    assert_eq!(reopened, store);
}

fn test_create_delete_and_unrelated_owner_success() {
    let mut store = Store::initialized();
    let main = store.open(1).unwrap();
    store.create_branch(&main, 2).unwrap();
    let branch_a_before = store.snapshot(1).unwrap();
    let branch_b = store.open(2).unwrap();
    store.publish_branch(&branch_b, 22, 220).unwrap();
    assert_eq!(store.snapshot(1).unwrap(), branch_a_before);
    let branch_b_after = store.open(2).unwrap();
    store.delete_branch(&branch_b_after).unwrap();
    assert_eq!(store.open(2), Err(ModelError::Missing));
}

fn test_global_sequence_publication_and_noop() {
    let mut store = Store::initialized();
    let view = store.open(1).unwrap();
    let before = store.image();
    assert!(!store.publish_sequence(&view, 0).unwrap());
    assert_eq!(store.image(), before);
    assert!(store.publish_sequence(&view, 7).unwrap());
    assert_eq!(store.sequence, 7);
    assert_eq!(store.open(1).unwrap().branch.snapshot_object, view.branch.snapshot_object);
    assert_eq!(store.open(1).unwrap().global.epoch, view.global.epoch + 1);
}

fn test_same_owner_stale_is_atomic() {
    let mut store = Store::initialized();
    let first = store.open(1).unwrap();
    let second = store.open(1).unwrap();
    store.publish_branch(&first, 31, 310).unwrap();
    assert_unchanged(&mut store, |candidate| {
        candidate.publish_branch(&second, 32, 320)
    });
}

fn test_branch_first_and_gc_first_races() {
    let mut branch_first = Store::initialized();
    let branch_view = branch_first.open(1).unwrap();
    let gc_observation = branch_first.observe_gc().unwrap();
    branch_first.publish_branch(&branch_view, 41, 410).unwrap();
    let before_stale_gc = branch_first.image();
    assert_eq!(branch_first.sweep_gc(&gc_observation), Err(ModelError::Stale));
    assert_eq!(branch_first.image(), before_stale_gc);

    let mut gc_first = Store::initialized();
    let branch_view = gc_first.open(1).unwrap();
    let gc_observation = gc_first.observe_gc().unwrap();
    gc_first.sweep_gc(&gc_observation).unwrap();
    let before_stale_branch = gc_first.image();
    assert_eq!(
        gc_first.publish_branch(&branch_view, 42, 420),
        Err(ModelError::Stale)
    );
    assert_eq!(gc_first.image(), before_stale_branch);
}

fn test_malformed_missing_and_wrong_kind_fail_closed() {
    let mut malformed = Store::initialized();
    malformed.global = SelectorBytes::Malformed;
    assert_eq!(malformed.open(1), Err(ModelError::Malformed));

    let mut missing_branch = Store::initialized();
    missing_branch.branches.remove(&1);
    assert_eq!(missing_branch.open(1), Err(ModelError::Missing));

    let mut wrong_global = Store::initialized();
    wrong_global.global = SelectorBytes::Branch(BranchSelector {
        branch_id: 1,
        snapshot_object: 2,
        selector_generation: 1,
    });
    assert_eq!(wrong_global.open(1), Err(ModelError::WrongKind));

    let mut wrong_branch = Store::initialized();
    wrong_branch.branches.insert(
        1,
        SelectorBytes::Global(GlobalSelector {
            repository_root: 10,
            epoch: 1,
            selector_generation: 1,
        }),
    );
    assert_eq!(wrong_branch.open(1), Err(ModelError::WrongKind));

    let mut missing_object = Store::initialized();
    missing_object.snapshots.remove(&2);
    assert_eq!(missing_object.open(1), Err(ModelError::MissingObject));
}

fn test_gc_observation_and_final_release() {
    let mut store = Store::initialized();
    let main = store.open(1).unwrap();
    store.create_branch(&main, 2).unwrap();
    let branch = store.open(2).unwrap();
    let retained_snapshot = branch.branch.snapshot_object;
    store.delete_branch(&branch).unwrap();
    let observation = store.observe_gc().unwrap();
    assert!(!observation.reachable_snapshots.contains(&retained_snapshot));
    store.sweep_gc(&observation).unwrap();
    assert!(!store.snapshots.contains_key(&retained_snapshot));
    assert!(store.open(1).is_ok());
}

fn main() {
    test_init_switch_noop_reopen();
    test_create_delete_and_unrelated_owner_success();
    test_global_sequence_publication_and_noop();
    test_same_owner_stale_is_atomic();
    test_branch_first_and_gc_first_races();
    test_malformed_missing_and_wrong_kind_fail_closed();
    test_gc_observation_and_final_release();
    println!("MODEL=GREEN cases=7");
}
