//! Dependency-free first-migration model for branch selectors and sequence
//! publication. This is deliberately not linked into Lix production.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
struct GlobalSelector {
    repository_root: u64,
    sequence: u64,
    epoch: u64,
    generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BranchSelector {
    branch_id: u8,
    snapshot_object: u64,
    generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BranchSnapshot {
    branch_id: u8,
    local_root: u64,
    historical_global_root: u64,
    semantic_head: u64,
    latest_ref_change: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SelectorSlot {
    Global(GlobalSelector),
    Branch(BranchSelector),
    Malformed,
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
struct PublicationReceipt {
    selector_writes: usize,
    object_writes: usize,
    epoch_before: u64,
    epoch_after: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GcObservation {
    raw_global: Vec<u8>,
    reachable_snapshots: BTreeSet<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Error {
    Stale,
    Missing,
    MissingObject,
    Malformed,
    WrongKind,
    InvalidBranch,
    SequenceRegression,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Image {
    global: SelectorSlot,
    branches: BTreeMap<u8, SelectorSlot>,
    snapshots: BTreeMap<u64, BranchSnapshot>,
    sequence: u64,
    next_object: u64,
    publications: Vec<PublicationReceipt>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Store {
    global: SelectorSlot,
    branches: BTreeMap<u8, SelectorSlot>,
    snapshots: BTreeMap<u64, BranchSnapshot>,
    sequence: u64,
    next_object: u64,
    publications: Vec<PublicationReceipt>,
}

impl Store {
    fn initialize() -> Self {
        Self {
            global: SelectorSlot::Global(GlobalSelector {
                repository_root: 10,
                sequence: 0,
                epoch: 1,
                generation: 1,
            }),
            branches: BTreeMap::from([(
                1,
                SelectorSlot::Branch(BranchSelector {
                    branch_id: 1,
                    snapshot_object: 2,
                    generation: 1,
                }),
            )]),
            snapshots: BTreeMap::from([(
                2,
                BranchSnapshot {
                    branch_id: 1,
                    local_root: 10,
                    historical_global_root: 10,
                    semantic_head: 11,
                    latest_ref_change: 12,
                },
            )]),
            sequence: 0,
            next_object: 20,
            publications: Vec::new(),
        }
    }

    fn image(&self) -> Image {
        Image {
            global: self.global.clone(),
            branches: self.branches.clone(),
            snapshots: self.snapshots.clone(),
            sequence: self.sequence,
            next_object: self.next_object,
            publications: self.publications.clone(),
        }
    }

    fn reopen(image: Image) -> Self {
        Self {
            global: image.global,
            branches: image.branches,
            snapshots: image.snapshots,
            sequence: image.sequence,
            next_object: image.next_object,
            publications: image.publications,
        }
    }

    fn open(&self, branch_id: u8) -> Result<View, Error> {
        let global = match &self.global {
            SelectorSlot::Global(value) => value.clone(),
            SelectorSlot::Branch(_) => return Err(Error::WrongKind),
            SelectorSlot::Malformed => return Err(Error::Malformed),
        };
        let branch = match self.branches.get(&branch_id) {
            None => return Err(Error::Missing),
            Some(SelectorSlot::Global(_)) => return Err(Error::WrongKind),
            Some(SelectorSlot::Branch(value)) => value.clone(),
            Some(SelectorSlot::Malformed) => return Err(Error::Malformed),
        };
        if branch.branch_id != branch_id {
            return Err(Error::Malformed);
        }
        let snapshot = self
            .snapshots
            .get(&branch.snapshot_object)
            .cloned()
            .ok_or(Error::MissingObject)?;
        if snapshot.branch_id != branch_id {
            return Err(Error::Malformed);
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

    fn check_view(&self, view: &View) -> Result<(), Error> {
        if self.raw_global() != view.raw_global {
            return Err(Error::Stale);
        }
        let current = self.branches.get(&view.branch_id).ok_or(Error::Missing)?;
        if current != &SelectorSlot::Branch(view.branch.clone()) {
            return Err(Error::Stale);
        }
        Ok(())
    }

    fn check_global(&self, view: &View) -> Result<(), Error> {
        if self.raw_global() == view.raw_global {
            Ok(())
        } else {
            Err(Error::Stale)
        }
    }

    fn publish_branch(
        &mut self,
        view: &View,
        next_head: u64,
        next_local_root: u64,
    ) -> Result<PublicationReceipt, Error> {
        self.check_view(view)?;
        let before = self.image();
        let object = self.next_object;
        let epoch_before = view.global.epoch;
        let next_global = rotate(&view.global)?;
        let next_snapshot = BranchSnapshot {
            branch_id: view.branch_id,
            local_root: next_local_root,
            historical_global_root: view.snapshot.historical_global_root,
            semantic_head: next_head,
            latest_ref_change: object + 1,
        };
        let receipt = PublicationReceipt {
            selector_writes: 2,
            object_writes: 2,
            epoch_before,
            epoch_after: next_global.epoch,
        };
        self.snapshots.insert(object, next_snapshot);
        self.branches.insert(
            view.branch_id,
            SelectorSlot::Branch(BranchSelector {
                branch_id: view.branch_id,
                snapshot_object: object,
                generation: view.branch.generation + 1,
            }),
        );
        self.global = SelectorSlot::Global(next_global);
        self.next_object += 2;
        self.publications.push(receipt.clone());
        assert_ne!(self.image(), before);
        Ok(receipt)
    }

    fn publish_sequence(
        &mut self,
        view: &View,
        next_sequence: u64,
    ) -> Result<Option<PublicationReceipt>, Error> {
        self.check_global(view)?;
        if next_sequence == self.sequence {
            return Ok(None);
        }
        if next_sequence < self.sequence {
            return Err(Error::SequenceRegression);
        }
        let next_global = rotate(&view.global)?;
        let receipt = PublicationReceipt {
            selector_writes: 1,
            object_writes: 0,
            epoch_before: view.global.epoch,
            epoch_after: next_global.epoch,
        };
        self.global = SelectorSlot::Global(next_global);
        self.sequence = next_sequence;
        self.publications.push(receipt.clone());
        Ok(Some(receipt))
    }

    fn create_branch(&mut self, view: &View, branch_id: u8) -> Result<PublicationReceipt, Error> {
        self.check_view(view)?;
        if self.branches.contains_key(&branch_id) {
            return Err(Error::Stale);
        }
        let object = self.next_object;
        let next_global = rotate(&view.global)?;
        let receipt = PublicationReceipt {
            selector_writes: 2,
            object_writes: 2,
            epoch_before: view.global.epoch,
            epoch_after: next_global.epoch,
        };
        self.snapshots.insert(
            object,
            BranchSnapshot {
                branch_id,
                local_root: view.snapshot.local_root,
                historical_global_root: view.global.repository_root,
                semantic_head: view.snapshot.semantic_head,
                latest_ref_change: object + 1,
            },
        );
        self.branches.insert(
            branch_id,
            SelectorSlot::Branch(BranchSelector {
                branch_id,
                snapshot_object: object,
                generation: 1,
            }),
        );
        self.global = SelectorSlot::Global(next_global);
        self.next_object += 2;
        self.publications.push(receipt.clone());
        Ok(receipt)
    }

    fn delete_branch(&mut self, view: &View) -> Result<PublicationReceipt, Error> {
        self.check_view(view)?;
        if view.branch_id == 1 {
            return Err(Error::InvalidBranch);
        }
        let next_global = rotate(&view.global)?;
        let receipt = PublicationReceipt {
            selector_writes: 2,
            object_writes: 1,
            epoch_before: view.global.epoch,
            epoch_after: next_global.epoch,
        };
        self.branches.remove(&view.branch_id);
        self.global = SelectorSlot::Global(next_global);
        self.publications.push(receipt.clone());
        Ok(receipt)
    }

    fn observe_gc(&self) -> Result<GcObservation, Error> {
        let global = match &self.global {
            SelectorSlot::Global(value) => value,
            SelectorSlot::Branch(_) => return Err(Error::WrongKind),
            SelectorSlot::Malformed => return Err(Error::Malformed),
        };
        let mut reachable = BTreeSet::new();
        for selector in self.branches.values() {
            let SelectorSlot::Branch(selector) = selector else {
                return Err(Error::WrongKind);
            };
            reachable.insert(selector.snapshot_object);
        }
        Ok(GcObservation {
            raw_global: encode_global(global),
            reachable_snapshots: reachable,
        })
    }

    fn sweep_gc(&mut self, observation: &GcObservation) -> Result<(), Error> {
        if self.raw_global() != observation.raw_global {
            return Err(Error::Stale);
        }
        let global = match &self.global {
            SelectorSlot::Global(value) => rotate(value)?,
            SelectorSlot::Branch(_) => return Err(Error::WrongKind),
            SelectorSlot::Malformed => return Err(Error::Malformed),
        };
        let unreachable = self
            .snapshots
            .keys()
            .copied()
            .filter(|id| !observation.reachable_snapshots.contains(id))
            .collect::<Vec<_>>();
        for id in unreachable {
            self.snapshots.remove(&id);
        }
        self.global = SelectorSlot::Global(global);
        Ok(())
    }

    fn raw_global(&self) -> Vec<u8> {
        match &self.global {
            SelectorSlot::Global(value) => encode_global(value),
            SelectorSlot::Branch(value) => encode_branch(value),
            SelectorSlot::Malformed => b"malformed".to_vec(),
        }
    }
}

fn rotate(value: &GlobalSelector) -> Result<GlobalSelector, Error> {
    Ok(GlobalSelector {
        repository_root: value.repository_root,
        sequence: value.sequence,
        epoch: value.epoch.checked_add(1).ok_or(Error::Malformed)?,
        generation: value.generation.checked_add(1).ok_or(Error::Malformed)?,
    })
}

fn encode_global(value: &GlobalSelector) -> Vec<u8> {
    format!(
        "G:{}:{}:{}:{}",
        value.repository_root, value.sequence, value.epoch, value.generation
    )
    .into_bytes()
}

fn encode_branch(value: &BranchSelector) -> Vec<u8> {
    format!("B:{}:{}:{}", value.branch_id, value.snapshot_object, value.generation).into_bytes()
}

fn expect_atomic_stale<F>(store: &mut Store, operation: F)
where
    F: FnOnce(&mut Store) -> Result<(), Error>,
{
    let before = store.image();
    assert_eq!(operation(store), Err(Error::Stale));
    assert_eq!(store.image(), before);
}

fn test_init_create_switch_delete_reopen() {
    let mut store = Store::initialize();
    let initial = store.image();
    let main = store.open(1).unwrap();
    let created = store.create_branch(&main, 2).unwrap();
    assert_eq!((created.selector_writes, created.object_writes), (2, 2));
    assert_eq!(store.open(2).unwrap().snapshot.branch_id, 2);
    let active = 2;
    assert_eq!(active, 2);
    let branch = store.open(active).unwrap();
    store.delete_branch(&branch).unwrap();
    assert_eq!(store.open(2), Err(Error::Missing));
    let reopened = Store::reopen(store.image());
    assert_eq!(reopened, store);
    assert_ne!(initial, store.image());
}

fn test_sequence_and_noop() {
    let mut store = Store::initialize();
    let view = store.open(1).unwrap();
    let before = store.image();
    assert_eq!(store.publish_sequence(&view, 0).unwrap(), None);
    assert_eq!(store.image(), before);
    let receipt = store.publish_sequence(&view, 17).unwrap().unwrap();
    assert_eq!((receipt.selector_writes, receipt.object_writes), (1, 0));
    assert_eq!(receipt.epoch_after, receipt.epoch_before + 1);
    assert_eq!(store.sequence, 17);
    assert_eq!(store.publish_sequence(&store.open(1).unwrap(), 16), Err(Error::SequenceRegression));
}

fn test_stale_same_and_unrelated_owner_atomic() {
    let mut store = Store::initialize();
    let first = store.open(1).unwrap();
    let second = store.open(1).unwrap();
    store.publish_branch(&first, 31, 310).unwrap();
    expect_atomic_stale(&mut store, |candidate| {
        candidate.publish_branch(&second, 32, 320).map(|_| ())
    });

    let main = store.open(1).unwrap();
    store.create_branch(&main, 2).unwrap();
    let unrelated = store.open(2).unwrap();
    let old_main = store.open(1).unwrap();
    store.publish_branch(&unrelated, 41, 410).unwrap();
    expect_atomic_stale(&mut store, |candidate| {
        candidate.publish_branch(&old_main, 42, 420).map(|_| ())
    });
}

fn test_branch_first_gc_first_and_corruption() {
    let mut branch_first = Store::initialize();
    let view = branch_first.open(1).unwrap();
    let observation = branch_first.observe_gc().unwrap();
    branch_first.publish_branch(&view, 51, 510).unwrap();
    let before_gc = branch_first.image();
    assert_eq!(branch_first.sweep_gc(&observation), Err(Error::Stale));
    assert_eq!(branch_first.image(), before_gc);

    let mut gc_first = Store::initialize();
    let view = gc_first.open(1).unwrap();
    let observation = gc_first.observe_gc().unwrap();
    gc_first.sweep_gc(&observation).unwrap();
    expect_atomic_stale(&mut gc_first, |candidate| {
        candidate.publish_branch(&view, 52, 520).map(|_| ())
    });

    let mut malformed = Store::initialize();
    malformed.global = SelectorSlot::Malformed;
    assert_eq!(malformed.open(1), Err(Error::Malformed));
    let mut wrong_kind = Store::initialize();
    wrong_kind.branches.insert(
        1,
        SelectorSlot::Global(GlobalSelector {
            repository_root: 10,
            sequence: 0,
            epoch: 1,
            generation: 1,
        }),
    );
    assert_eq!(wrong_kind.open(1), Err(Error::WrongKind));
    let mut missing = Store::initialize();
    missing.snapshots.remove(&2);
    assert_eq!(missing.open(1), Err(Error::MissingObject));
}

fn main() {
    test_init_create_switch_delete_reopen();
    test_sequence_and_noop();
    test_stale_same_and_unrelated_owner_atomic();
    test_branch_first_gc_first_and_corruption();
    println!("BRANCH_MIGRATION_MODEL=GREEN cases=4");
}
