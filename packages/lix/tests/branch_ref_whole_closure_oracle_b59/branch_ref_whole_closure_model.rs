use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
struct GlobalSelector {
    root: String,
    epoch: u64,
    generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BranchSelector {
    branch: String,
    snapshot: String,
    generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CoherentView {
    global: GlobalSelector,
    branch: BranchSelector,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StateFingerprint {
    active_branch: Option<String>,
    histories: BTreeMap<String, Vec<String>>,
    objects: BTreeSet<String>,
    live_objects: BTreeSet<String>,
    allocations: BTreeSet<String>,
    global_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedPublication {
    expected_global: GlobalSelector,
    expected_branch: Option<BranchSelector>,
    next_global: GlobalSelector,
    next_branch: Option<BranchSelector>,
    staged_objects: BTreeSet<String>,
    next_active_branch: Option<String>,
    view_count: u8,
    commit_count: u8,
    selector_cas_count: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Failure {
    InvalidBranchIdentity,
    StaleSelector,
    CorruptSelector,
    MissingRoot,
    Cycle,
    DualAuthority,
    InvalidGlobalSequence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OperationResult {
    Published,
    NoOp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Repository {
    global: Option<GlobalSelector>,
    branches: BTreeMap<String, BranchSelector>,
    histories: BTreeMap<String, Vec<String>>,
    objects: BTreeSet<String>,
    live_objects: BTreeSet<String>,
    allocations: BTreeSet<String>,
    derived_branch_refs: BTreeMap<String, String>,
    epoch_history: Vec<u64>,
    active_branch: Option<String>,
    cycles: BTreeSet<String>,
    views: u8,
    writes: u8,
    commits: u8,
}

impl Repository {
    fn bootstrap() -> Self {
        let mut objects = BTreeSet::new();
        objects.insert("root-global".into());
        let mut live_objects = BTreeSet::new();
        live_objects.insert("root-global".into());
        Self {
            global: Some(GlobalSelector {
                root: "root-global".into(),
                epoch: 1,
                generation: 1,
            }),
            branches: BTreeMap::new(),
            histories: BTreeMap::new(),
            objects,
            live_objects,
            allocations: BTreeSet::new(),
            derived_branch_refs: BTreeMap::new(),
            epoch_history: vec![1],
            active_branch: None,
            cycles: BTreeSet::new(),
            views: 0,
            writes: 0,
            commits: 0,
        }
    }

    fn fingerprint(&self) -> StateFingerprint {
        StateFingerprint {
            active_branch: self.active_branch.clone(),
            histories: self.histories.clone(),
            objects: self.objects.clone(),
            live_objects: self.live_objects.clone(),
            allocations: self.allocations.clone(),
            global_epoch: self.global.as_ref().map_or(0, |selector| selector.epoch),
        }
    }

    fn create_branch(&mut self, branch: &str, snapshot: &str) -> Result<OperationResult, Failure> {
        validate_branch(branch)?;
        if self.branches.contains_key(branch) {
            return Err(Failure::StaleSelector);
        }
        self.stage_object(snapshot);
        let prepared = self.prepare_create_branch(branch, snapshot)?;
        self.publish(prepared)
    }

    fn open_view(&mut self, branch: &str) -> Result<CoherentView, Failure> {
        validate_branch(branch)?;
        self.views += 1;
        let global = self.global.clone().ok_or(Failure::MissingRoot)?;
        let selector = self
            .branches
            .get(branch)
            .cloned()
            .ok_or(Failure::MissingRoot)?;
        if self.cycles.contains(branch)
            || selector.branch != branch
            || !self.objects.contains(&selector.snapshot)
            || !self.live_objects.contains(&selector.snapshot)
        {
            return Err(if self.cycles.contains(branch) {
                Failure::Cycle
            } else {
                Failure::CorruptSelector
            });
        }
        Ok(CoherentView {
            global,
            branch: selector,
        })
    }

    fn stage_object(&mut self, object: &str) {
        self.allocations.insert(object.into());
    }

    fn prepare_create_branch(
        &self,
        branch: &str,
        snapshot: &str,
    ) -> Result<PreparedPublication, Failure> {
        validate_branch(branch)?;
        if self.branches.contains_key(branch) {
            return Err(Failure::StaleSelector);
        }
        if !self.allocations.contains(snapshot) {
            return Err(Failure::MissingRoot);
        }
        let global = self.global.clone().ok_or(Failure::MissingRoot)?;
        Ok(PreparedPublication {
            expected_global: global.clone(),
            expected_branch: None,
            next_global: GlobalSelector {
                root: global.root,
                epoch: global.epoch + 1,
                generation: global.generation + 1,
            },
            next_branch: Some(BranchSelector {
                branch: branch.into(),
                snapshot: snapshot.into(),
                generation: 1,
            }),
            staged_objects: std::iter::once(snapshot.into()).collect(),
            next_active_branch: Some(branch.into()),
            view_count: 1,
            commit_count: 1,
            selector_cas_count: 2,
        })
    }

    fn prepare_branch(
        &self,
        view: &CoherentView,
        next_snapshot: &str,
    ) -> Result<PreparedPublication, Failure> {
        if !self.objects.contains(next_snapshot) && !self.allocations.contains(next_snapshot) {
            return Err(Failure::MissingRoot);
        }
        let next_global = GlobalSelector {
            root: view.global.root.clone(),
            epoch: view.global.epoch + 1,
            generation: view.global.generation + 1,
        };
        let next_branch = BranchSelector {
            branch: view.branch.branch.clone(),
            snapshot: next_snapshot.into(),
            generation: view.branch.generation + 1,
        };
        Ok(PreparedPublication {
            expected_global: view.global.clone(),
            expected_branch: Some(view.branch.clone()),
            next_global,
            next_branch: Some(next_branch),
            staged_objects: self
                .allocations
                .iter()
                .filter(|object| object.as_str() == next_snapshot)
                .cloned()
                .collect(),
            next_active_branch: self.active_branch.clone(),
            view_count: 1,
            commit_count: 1,
            selector_cas_count: 2,
        })
    }

    fn prepare_delete(&self, view: &CoherentView) -> PreparedPublication {
        PreparedPublication {
            expected_global: view.global.clone(),
            expected_branch: Some(view.branch.clone()),
            next_global: GlobalSelector {
                root: view.global.root.clone(),
                epoch: view.global.epoch + 1,
                generation: view.global.generation + 1,
            },
            next_branch: None,
            staged_objects: BTreeSet::new(),
            next_active_branch: self
                .active_branch
                .as_deref()
                .filter(|active| *active != view.branch.branch)
                .map(str::to_owned),
            view_count: 1,
            commit_count: 1,
            selector_cas_count: 2,
        }
    }

    fn publish(&mut self, prepared: PreparedPublication) -> Result<OperationResult, Failure> {
        if prepared.view_count != 1
            || prepared.commit_count != 1
            || prepared.selector_cas_count != 2
        {
            return Err(Failure::DualAuthority);
        }
        if self.global.as_ref() != Some(&prepared.expected_global) {
            return Err(Failure::StaleSelector);
        }
        if prepared.expected_branch.is_none() {
            if let Some(next_branch) = prepared.next_branch.as_ref() {
                if self.branches.contains_key(&next_branch.branch) {
                    return Err(Failure::StaleSelector);
                }
            }
        }
        if let Some(expected_branch) = prepared.expected_branch.as_ref() {
            if self.branches.get(&expected_branch.branch) != Some(expected_branch) {
                return Err(Failure::StaleSelector);
            }
        }

        // Compute every mutation before assigning any durable field. This is
        // the model's no-partial-commit boundary.
        let mut next_branches = self.branches.clone();
        let mut next_histories = self.histories.clone();
        let mut next_objects = self.objects.clone();
        let mut next_live = self.live_objects.clone();
        let mut next_allocations = self.allocations.clone();
        let next_global = prepared.next_global.clone();
        for object in &prepared.staged_objects {
            next_objects.insert(object.clone());
            next_live.insert(object.clone());
            next_allocations.remove(object);
        }
        if let Some(next_branch) = prepared.next_branch {
            next_live.insert(next_branch.snapshot.clone());
            if prepared.expected_branch.is_none() {
                next_histories.insert(next_branch.branch.clone(), Vec::new());
            }
            next_branches.insert(next_branch.branch.clone(), next_branch);
        } else if let Some(expected_branch) = prepared.expected_branch {
            next_branches.remove(&expected_branch.branch);
            next_histories.remove(&expected_branch.branch);
            next_live.remove(&expected_branch.snapshot);
        }

        self.global = Some(next_global);
        self.branches = next_branches;
        self.histories = next_histories;
        self.objects = next_objects;
        self.live_objects = next_live;
        self.allocations = next_allocations;
        self.active_branch = prepared.next_active_branch;
        self.epoch_history.push(self.global.as_ref().unwrap().epoch);
        self.writes += 1;
        self.commits += 1;
        Ok(OperationResult::Published)
    }

    fn empty_undo(&mut self, branch: &str) -> Result<OperationResult, Failure> {
        validate_branch(branch)?;
        if self.histories.get(branch).map_or(true, Vec::is_empty) {
            return Ok(OperationResult::NoOp);
        }
        Err(Failure::DualAuthority)
    }

    fn empty_redo(&mut self, branch: &str) -> Result<OperationResult, Failure> {
        self.empty_undo(branch)
    }

    fn gc(&mut self) {
        let live = self.live_objects.clone();
        self.objects.retain(|object| live.contains(object));
        self.writes += 1;
    }

    fn reopen(&self) -> Result<Self, Failure> {
        if self
            .epoch_history
            .windows(2)
            .any(|window| window[1] != window[0] + 1)
        {
            return Err(Failure::InvalidGlobalSequence);
        }
        let Some(global) = &self.global else {
            return Err(Failure::MissingRoot);
        };
        if self.epoch_history.last() != Some(&global.epoch) {
            return Err(Failure::InvalidGlobalSequence);
        }
        for (branch, selector) in &self.branches {
            if branch != &selector.branch || !self.objects.contains(&selector.snapshot) {
                return Err(Failure::CorruptSelector);
            }
        }
        Ok(self.clone())
    }
}

fn validate_branch(branch: &str) -> Result<(), Failure> {
    let bytes = branch.as_bytes();
    if bytes.len() != 36
        || !matches!(bytes[8], b'-')
        || !matches!(bytes[13], b'-')
        || !matches!(bytes[18], b'-')
        || !matches!(bytes[23], b'-')
    {
        return Err(Failure::InvalidBranchIdentity);
    }
    for (index, byte) in bytes.iter().copied().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            continue;
        }
        if !matches!(byte, b'0'..=b'9' | b'a'..=b'f') {
            return Err(Failure::InvalidBranchIdentity);
        }
    }
    Ok(())
}

const BRANCH_A: &str = "01920000-0000-7000-8000-0000000000a1";
const BRANCH_B: &str = "01920000-0000-7000-8000-0000000000b1";

fn repository_with_branch() -> Repository {
    let mut repository = Repository::bootstrap();
    repository.create_branch(BRANCH_A, "root-a").unwrap();
    repository
}

#[test]
fn fingerprint_covers_active_branch_history_objects_liveness_and_allocations() {
    let mut repository = repository_with_branch();
    repository.stage_object("staged-a");
    let fingerprint = repository.fingerprint();
    assert_eq!(fingerprint.active_branch.as_deref(), Some(BRANCH_A));
    assert!(fingerprint.histories.contains_key(BRANCH_A));
    assert!(fingerprint.objects.contains("root-global"));
    assert!(fingerprint.live_objects.contains("root-a"));
    assert!(fingerprint.allocations.contains("staged-a"));
}

#[test]
fn one_view_one_prepared_publication_one_commit() {
    let mut repository = repository_with_branch();
    repository.stage_object("root-next");
    let view = repository.open_view(BRANCH_A).unwrap();
    let prepared = repository.prepare_branch(&view, "root-next").unwrap();
    assert_eq!(repository.publish(prepared), Ok(OperationResult::Published));
    assert_eq!(repository.views, 1);
    assert_eq!(repository.writes, 2); // create plus advance
    assert_eq!(repository.commits, 2);
    assert!(repository.allocations.is_empty());
}

#[test]
fn stale_or_corrupt_publication_has_no_partial_state() {
    let mut repository = repository_with_branch();
    repository.stage_object("root-next");
    let view = repository.open_view(BRANCH_A).unwrap();
    let prepared = repository.prepare_branch(&view, "root-next").unwrap();
    let before = repository.fingerprint();
    repository.branches.get_mut(BRANCH_A).unwrap().generation += 1;
    let changed = repository.fingerprint();
    assert_ne!(before, changed);
    let writes = repository.writes;
    assert_eq!(repository.publish(prepared), Err(Failure::StaleSelector));
    assert_eq!(repository.writes, writes);
    assert!(repository.allocations.contains("root-next"));
}

#[test]
fn malformed_identity_missing_root_and_cycle_fail_closed() {
    let mut repository = Repository::bootstrap();
    assert_eq!(
        repository.open_view("not-a-branch"),
        Err(Failure::InvalidBranchIdentity)
    );
    repository.branches.insert(
        BRANCH_A.into(),
        BranchSelector {
            branch: BRANCH_A.into(),
            snapshot: "missing-root".into(),
            generation: 1,
        },
    );
    assert_eq!(
        repository.open_view(BRANCH_A),
        Err(Failure::CorruptSelector)
    );
    repository.objects.insert("root-cycle".into());
    repository.live_objects.insert("root-cycle".into());
    repository.branches.insert(
        BRANCH_B.into(),
        BranchSelector {
            branch: BRANCH_B.into(),
            snapshot: "root-cycle".into(),
            generation: 1,
        },
    );
    repository.cycles.insert(BRANCH_B.into());
    assert_eq!(repository.open_view(BRANCH_B), Err(Failure::Cycle));
    assert_eq!(repository.writes, 0); // direct corruption setup is not a publication
}

#[test]
fn empty_undo_redo_are_true_no_ops() {
    let mut repository = repository_with_branch();
    let before = repository.fingerprint();
    assert_eq!(repository.empty_undo(BRANCH_A), Ok(OperationResult::NoOp));
    assert_eq!(repository.empty_redo(BRANCH_A), Ok(OperationResult::NoOp));
    assert_eq!(repository.fingerprint(), before);
    assert_eq!(repository.writes, 1);
    assert_eq!(repository.commits, 1);
}

#[test]
fn derived_projection_never_becomes_selector_authority() {
    let mut repository = repository_with_branch();
    repository
        .derived_branch_refs
        .insert(BRANCH_A.into(), "fake-root".into());
    let view = repository.open_view(BRANCH_A).unwrap();
    assert_eq!(view.branch.snapshot, "root-a");
    assert_eq!(repository.global.as_ref().unwrap().root, "root-global");
}

#[test]
fn delete_and_gc_reclaim_final_branch_reference_only() {
    let mut repository = repository_with_branch();
    let view = repository.open_view(BRANCH_A).unwrap();
    let before = repository.fingerprint();
    assert_eq!(
        repository.publish(repository.prepare_delete(&view)),
        Ok(OperationResult::Published)
    );
    assert_ne!(repository.fingerprint(), before);
    repository.gc();
    assert!(!repository.objects.contains("root-a"));
    assert!(repository.objects.contains("root-global"));
}

#[test]
fn old_view_survives_switch_checkpoint_like_rotation_and_reopen() {
    let mut repository = repository_with_branch();
    repository.stage_object("root-next");
    let old_view = repository.open_view(BRANCH_A).unwrap();
    repository
        .publish(repository.prepare_branch(&old_view, "root-next").unwrap())
        .unwrap();
    assert_eq!(old_view.branch.snapshot, "root-a");
    let reopened = repository.reopen().unwrap();
    assert_eq!(reopened.fingerprint(), repository.fingerprint());
    assert_eq!(reopened.branches[BRANCH_A].snapshot, "root-next");
}

#[test]
fn reopen_rejects_global_epoch_gap() {
    let mut repository = repository_with_branch();
    repository.epoch_history.push(99);
    repository.global.as_mut().unwrap().epoch = 99;
    assert_eq!(repository.reopen(), Err(Failure::InvalidGlobalSequence));
}

#[test]
fn invalid_multi_authority_publication_rejects_before_write() {
    let mut repository = repository_with_branch();
    let view = repository.open_view(BRANCH_A).unwrap();
    let prepared = repository.prepare_delete(&view);
    prepared.view_count = 2;
    let writes = repository.writes;
    assert_eq!(repository.publish(prepared), Err(Failure::DualAuthority));
    assert_eq!(repository.writes, writes);
}
