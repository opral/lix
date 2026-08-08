#![allow(dead_code)]

//! Pure, dependency-free BranchHeadControl replacement oracle.
//!
//! This file is intentionally outside the Lix workspace.  It is a model of
//! the required selector/owner semantics, not an implementation or a format
//! proposal.  The production acceptance target must use authenticated
//! GlobalSelectorV1/BranchSelectorV1 bytes and the existing ForkTree owner.

use std::collections::{BTreeMap, BTreeSet};

pub type BranchId = u64;
pub type ObjectId = u64;
pub type CommitId = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct GlobalSelector {
    pub repository_root: ObjectId,
    pub epoch: u64,
    pub generation: u64,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct BranchSelector {
    pub branch_id: BranchId,
    pub snapshot: ObjectId,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawSelector {
    Missing,
    Global {
        repository_root: ObjectId,
        epoch: u64,
        generation: u64,
        sequence: u64,
    },
    Branch {
        branch_id: BranchId,
        snapshot: ObjectId,
        generation: u64,
    },
    WrongKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reject {
    MissingSelector,
    WrongSelectorKind,
    CorruptSelector,
    MissingBranch,
    MainBranchImmutable,
    ActiveBranch,
    DuplicateBranch,
    StaleOwner,
    StaleGc,
    CheckpointFloor,
    EmptyUndo,
    EmptyRedo,
    InvalidOperation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Owner {
    Branch(BranchId),
    Global,
    Gc,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BranchTxn {
    pub branch_id: BranchId,
    pub observed_global_epoch: u64,
    pub observed_global_sequence: u64,
    pub observed_generation: u64,
    pub observed_selector: BranchSelector,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcTxn {
    pub observed_global_epoch: u64,
    pub unreachable: BTreeSet<ObjectId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BranchState {
    selector: BranchSelector,
    history: Vec<(CommitId, ObjectId)>,
    cursor: usize,
    checkpoint_floor: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Model {
    pub global: Option<GlobalSelector>,
    branches: BTreeMap<BranchId, BranchState>,
    pub active_branch: Option<BranchId>,
    objects: BTreeSet<ObjectId>,
    pub live_objects: BTreeSet<ObjectId>,
    next_object: ObjectId,
}

impl Model {
    pub fn empty() -> Self {
        Self {
            global: None,
            branches: BTreeMap::new(),
            active_branch: None,
            objects: BTreeSet::new(),
            live_objects: BTreeSet::new(),
            next_object: 1,
        }
    }

    pub fn init() -> Result<Self, Reject> {
        let mut model = Self::empty();
        let root = model.allocate();
        let main_snapshot = model.allocate();
        model.global = Some(GlobalSelector {
            repository_root: root,
            epoch: 1,
            generation: 1,
            sequence: 1,
        });
        let main = BranchState {
            selector: BranchSelector {
                branch_id: 0,
                snapshot: main_snapshot,
                generation: 1,
            },
            history: vec![(1, main_snapshot)],
            cursor: 0,
            checkpoint_floor: 0,
        };
        model.branches.insert(0, main);
        model.active_branch = Some(0);
        model.refresh_live_roots();
        model.validate()?;
        Ok(model)
    }

    pub fn open(raw_global: RawSelector, raw_branch: RawSelector) -> Result<(), Reject> {
        let global = match raw_global {
            RawSelector::Global {
                repository_root,
                epoch,
                generation,
                sequence,
            } if repository_root != 0 && epoch != 0 && generation != 0 && sequence != 0 => {
                GlobalSelector {
                    repository_root,
                    epoch,
                    generation,
                    sequence,
                }
            }
            RawSelector::Missing => return Err(Reject::MissingSelector),
            RawSelector::WrongKind | RawSelector::Branch { .. } => {
                return Err(Reject::WrongSelectorKind)
            }
            RawSelector::Global { .. } => return Err(Reject::CorruptSelector),
        };
        match raw_branch {
            RawSelector::Branch {
                branch_id,
                snapshot,
                generation,
            } if snapshot != 0 && generation != 0 => {
                if branch_id == 0 && global.repository_root == 0 {
                    return Err(Reject::CorruptSelector);
                }
                Ok(())
            }
            RawSelector::Missing => Err(Reject::MissingSelector),
            RawSelector::WrongKind | RawSelector::Global { .. } => {
                Err(Reject::WrongSelectorKind)
            }
            RawSelector::Branch { .. } => Err(Reject::CorruptSelector),
        }
    }

    pub fn begin_branch(&self, branch_id: BranchId) -> Result<BranchTxn, Reject> {
        let global = self.global.ok_or(Reject::MissingSelector)?;
        let branch = self.branches.get(&branch_id).ok_or(Reject::MissingBranch)?;
        Ok(BranchTxn {
            branch_id,
            observed_global_epoch: global.epoch,
            observed_global_sequence: global.sequence,
            observed_generation: branch.selector.generation,
            observed_selector: branch.selector,
        })
    }

    pub fn switch(&mut self, branch_id: BranchId) -> Result<(), Reject> {
        if !self.branches.contains_key(&branch_id) {
            return Err(Reject::MissingBranch);
        }
        self.active_branch = Some(branch_id);
        Ok(())
    }

    pub fn create_branch(
        &mut self,
        branch_id: BranchId,
        from: BranchTxn,
    ) -> Result<BranchSelector, Reject> {
        if self.branches.contains_key(&branch_id) {
            return Err(Reject::DuplicateBranch);
        }
        let source = self
            .branches
            .get(&from.branch_id)
            .ok_or(Reject::MissingBranch)?;
        self.require_owner(from)?;
        let snapshot = source.selector.snapshot;
        let selector = BranchSelector {
            branch_id,
            snapshot,
            generation: 1,
        };
        let state = BranchState {
            selector,
            history: source.history.clone(),
            cursor: source.cursor,
            checkpoint_floor: source.checkpoint_floor,
        };
        self.branches.insert(branch_id, state);
        self.advance_global();
        self.refresh_live_roots();
        Ok(selector)
    }

    pub fn publish_branch(
        &mut self,
        tx: BranchTxn,
        commit: CommitId,
    ) -> Result<BranchSelector, Reject> {
        self.require_owner(tx)?;
        let snapshot = self.allocate();
        let selector = {
            let branch = self
                .branches
                .get_mut(&tx.branch_id)
                .ok_or(Reject::MissingBranch)?;
            branch.history.truncate(branch.cursor + 1);
            branch.history.push((commit, snapshot));
            branch.cursor += 1;
            branch.selector = BranchSelector {
                branch_id: tx.branch_id,
                snapshot,
                generation: tx.observed_generation + 1,
            };
            branch.selector
        };
        self.advance_global();
        self.refresh_live_roots();
        Ok(selector)
    }

    pub fn undo(&mut self, tx: BranchTxn) -> Result<BranchSelector, Reject> {
        self.require_owner(tx)?;
        let selector = {
            let branch = self
                .branches
                .get_mut(&tx.branch_id)
                .ok_or(Reject::MissingBranch)?;
            if branch.cursor == branch.checkpoint_floor {
                return Err(Reject::CheckpointFloor);
            }
            branch.cursor -= 1;
            branch.selector.snapshot = branch.history[branch.cursor].1;
            branch.selector.generation += 1;
            branch.selector
        };
        self.advance_global();
        self.refresh_live_roots();
        Ok(selector)
    }

    pub fn redo(&mut self, tx: BranchTxn) -> Result<BranchSelector, Reject> {
        self.require_owner(tx)?;
        let selector = {
            let branch = self
                .branches
                .get_mut(&tx.branch_id)
                .ok_or(Reject::MissingBranch)?;
            if branch.cursor + 1 >= branch.history.len() {
                return Err(Reject::EmptyRedo);
            }
            branch.cursor += 1;
            branch.selector.snapshot = branch.history[branch.cursor].1;
            branch.selector.generation += 1;
            branch.selector
        };
        self.advance_global();
        self.refresh_live_roots();
        Ok(selector)
    }

    pub fn checkpoint(&mut self, tx: BranchTxn) -> Result<(), Reject> {
        self.require_owner(tx)?;
        {
            let branch = self
                .branches
                .get_mut(&tx.branch_id)
                .ok_or(Reject::MissingBranch)?;
            branch.checkpoint_floor = branch.cursor;
            branch.selector.generation += 1;
        }
        self.advance_global();
        self.refresh_live_roots();
        Ok(())
    }

    pub fn delete_branch(&mut self, tx: BranchTxn) -> Result<(), Reject> {
        self.require_owner(tx)?;
        if tx.branch_id == 0 {
            return Err(Reject::MainBranchImmutable);
        }
        if self.active_branch == Some(tx.branch_id) {
            return Err(Reject::ActiveBranch);
        }
        self.branches.remove(&tx.branch_id).ok_or(Reject::MissingBranch)?;
        self.advance_global();
        self.refresh_live_roots();
        Ok(())
    }

    pub fn begin_gc(&self, unreachable: BTreeSet<ObjectId>) -> Result<GcTxn, Reject> {
        Ok(GcTxn {
            observed_global_epoch: self.global.ok_or(Reject::MissingSelector)?.epoch,
            unreachable,
        })
    }

    pub fn commit_gc(&mut self, gc: GcTxn) -> Result<usize, Reject> {
        let global = self.global.ok_or(Reject::MissingSelector)?;
        if global.epoch != gc.observed_global_epoch {
            return Err(Reject::StaleGc);
        }
        let before = self.objects.len();
        for object in gc.unreachable {
            if !self.live_objects.contains(&object) {
                self.objects.remove(&object);
            }
        }
        self.advance_global();
        Ok(before - self.objects.len())
    }

    pub fn reopen(&self) -> Result<(), Reject> {
        let global = self.global.ok_or(Reject::MissingSelector)?;
        if global.repository_root == 0 || global.epoch == 0 || global.generation == 0 {
            return Err(Reject::CorruptSelector);
        }
        for (id, branch) in &self.branches {
            if *id != branch.selector.branch_id
                || branch.selector.snapshot == 0
                || branch.selector.generation == 0
            {
                return Err(Reject::CorruptSelector);
            }
        }
        Ok(())
    }

    pub fn state_fingerprint(&self) -> u64 {
        let mut hash = 0xcbf29ce484222325u64;
        if let Some(global) = self.global {
            hash = mix(hash, global.repository_root);
            hash = mix(hash, global.epoch);
            hash = mix(hash, global.generation);
            hash = mix(hash, global.sequence);
        }
        for (branch_id, branch) in &self.branches {
            hash = mix(hash, *branch_id);
            hash = mix(hash, branch.selector.snapshot);
            hash = mix(hash, branch.selector.generation);
            hash = mix(hash, branch.cursor as u64);
            hash = mix(hash, branch.checkpoint_floor as u64);
        }
        hash
    }

    fn require_owner(&self, tx: BranchTxn) -> Result<(), Reject> {
        let global = self.global.ok_or(Reject::MissingSelector)?;
        let branch = self.branches.get(&tx.branch_id).ok_or(Reject::MissingBranch)?;
        if branch.selector != tx.observed_selector
            || branch.selector.generation != tx.observed_generation
            || global.generation == 0
        {
            return Err(Reject::StaleOwner);
        }
        Ok(())
    }

    fn allocate(&mut self) -> ObjectId {
        let object = self.next_object;
        self.next_object += 1;
        self.objects.insert(object);
        object
    }

    fn advance_global(&mut self) {
        let global = self.global.as_mut().expect("initialized model");
        global.epoch += 1;
        global.generation += 1;
        global.sequence += 1;
    }

    fn refresh_live_roots(&mut self) {
        self.live_objects.clear();
        if let Some(global) = self.global {
            self.live_objects.insert(global.repository_root);
        }
        self.live_objects.extend(
            self.branches
                .values()
                .map(|branch| branch.selector.snapshot),
        );
    }

    fn validate(&self) -> Result<(), Reject> {
        self.reopen()
    }
}

fn mix(mut value: u64, next: u64) -> u64 {
    value ^= next;
    value = value.wrapping_mul(0x100000001b3);
    value.rotate_left(13)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn branch_tx(model: &Model, branch: BranchId) -> BranchTxn {
        model.begin_branch(branch).expect("branch exists")
    }

    #[test]
    fn init_create_switch_delete_and_owner_conflicts_are_atomic() {
        let mut model = Model::init().unwrap();
        let main = branch_tx(&model, 0);
        model.create_branch(1, main).unwrap();
        model.switch(1).unwrap();

        let a = branch_tx(&model, 1);
        let b = branch_tx(&model, 1);
        model.publish_branch(a, 2).unwrap();
        let before = model.state_fingerprint();
        assert_eq!(model.publish_branch(b, 3), Err(Reject::StaleOwner));
        assert_eq!(model.state_fingerprint(), before);

        let unrelated = branch_tx(&model, 0);
        model.publish_branch(unrelated, 4).unwrap();
        let delete = branch_tx(&model, 1);
        model.switch(0).unwrap();
        model.delete_branch(delete).unwrap();
        assert_eq!(model.begin_branch(1), Err(Reject::MissingBranch));
    }

    #[test]
    fn undo_redo_checkpoint_and_sequence_are_deterministic() {
        let mut model = Model::init().unwrap();
        let first = branch_tx(&model, 0);
        model.publish_branch(first, 2).unwrap();
        let checkpoint = branch_tx(&model, 0);
        model.checkpoint(checkpoint).unwrap();
        let second = branch_tx(&model, 0);
        model.publish_branch(second, 3).unwrap();
        let undo = branch_tx(&model, 0);
        model.undo(undo).unwrap();
        let redo = branch_tx(&model, 0);
        model.redo(redo).unwrap();
        let floor = branch_tx(&model, 0);
        model.undo(floor).unwrap();
        let floor_again = branch_tx(&model, 0);
        assert_eq!(model.undo(floor_again), Err(Reject::CheckpointFloor));
        assert_eq!(model.global.unwrap().sequence, 8);
    }

    #[test]
    fn gc_and_publication_both_orders_preserve_roots_and_reject_stale_gc() {
        let mut model = Model::init().unwrap();
        let mut unreachable = BTreeSet::new();
        unreachable.insert(999);
        let gc_before_write = model.begin_gc(unreachable.clone()).unwrap();
        let write = branch_tx(&model, 0);
        model.publish_branch(write, 2).unwrap();
        assert_eq!(model.commit_gc(gc_before_write), Err(Reject::StaleGc));

        let gc_first = model.begin_gc(unreachable).unwrap();
        model.commit_gc(gc_first).unwrap();
        let write_after_gc = branch_tx(&model, 0);
        let new_root = model.publish_branch(write_after_gc, 3).unwrap();
        assert!(model.live_objects.contains(&new_root.snapshot));
        model.reopen().unwrap();
    }

    #[test]
    fn malformed_missing_and_wrong_kind_selectors_fail_closed() {
        let valid_global = RawSelector::Global {
            repository_root: 1,
            epoch: 1,
            generation: 1,
            sequence: 1,
        };
        let valid_branch = RawSelector::Branch {
            branch_id: 0,
            snapshot: 2,
            generation: 1,
        };
        assert_eq!(Model::open(valid_global, valid_branch), Ok(()));
        assert_eq!(
            Model::open(RawSelector::Missing, valid_branch),
            Err(Reject::MissingSelector)
        );
        assert_eq!(
            Model::open(RawSelector::WrongKind, valid_branch),
            Err(Reject::WrongSelectorKind)
        );
        assert_eq!(
            Model::open(
                RawSelector::Global {
                    repository_root: 0,
                    epoch: 1,
                    generation: 1,
                    sequence: 1,
                },
                valid_branch,
            ),
            Err(Reject::CorruptSelector)
        );
    }
}
