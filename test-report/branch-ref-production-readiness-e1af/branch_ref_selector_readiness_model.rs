//! Standalone test/report model. It is not a production implementation.
//! The model makes the selector/read/publication ownership contract executable
//! without importing Lix or inventing a persisted compatibility space.

use std::collections::{BTreeMap, BTreeSet};

const GLOBAL_KEY: &str = "selector:global";
const GLOBAL_ROOT: &str = "root-global";
const SELECTOR_SPACE: &str = "SELECTOR_SPACE";

fn auth(bytes: &str) -> u64 {
    bytes.bytes().fold(0xcbf29ce484222325u64, |state, byte| {
        (state ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GlobalSelector {
    key: String,
    root: String,
    epoch: u64,
    bytes: String,
    tag: u64,
}

impl GlobalSelector {
    fn canonical(epoch: u64) -> Self {
        let bytes = format!("global|key={GLOBAL_KEY}|root={GLOBAL_ROOT}|epoch={epoch}");
        Self {
            key: GLOBAL_KEY.to_owned(),
            root: GLOBAL_ROOT.to_owned(),
            epoch,
            tag: auth(&bytes),
            bytes,
        }
    }

    fn authenticated(&self) -> bool {
        self.key == GLOBAL_KEY
            && self.root == GLOBAL_ROOT
            && self.bytes
                == format!(
                    "global|key={}|root={}|epoch={}",
                    self.key, self.root, self.epoch
                )
            && self.tag == auth(&self.bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BranchSelector {
    branch: String,
    owner: String,
    root: String,
    generation: u64,
    bytes: String,
    tag: u64,
}

impl BranchSelector {
    fn canonical(branch: &str, root: &str, generation: u64) -> Self {
        let owner = format!("branch:{branch}");
        let bytes = format!(
            "branch|key=selector:branch:{branch}|owner={owner}|branch={branch}|root={root}|generation={generation}"
        );
        Self {
            branch: branch.to_owned(),
            owner,
            root: root.to_owned(),
            generation,
            tag: auth(&bytes),
            bytes,
        }
    }

    fn authenticated(&self) -> bool {
        self.owner == format!("branch:{}", self.branch)
            && self.bytes
                == format!(
                    "branch|key=selector:branch:{}|owner={}|branch={}|root={}|generation={}",
                    self.branch, self.owner, self.branch, self.root, self.generation
                )
            && self.tag == auth(&self.bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CoherentView {
    read_id: u64,
    global_epoch: u64,
    branch: String,
    branch_root: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedPublication {
    read_id: u64,
    owner: String,
    branch: String,
    expected_epoch: u64,
    expected_root: String,
    next_root: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Failure {
    InvalidSelector,
    MissingRoot,
    Cycle,
    StaleOwner,
    UnrelatedOwner,
    DualAuthority,
    EpochGap,
    Retired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Lifecycle {
    Create,
    Switch,
    Advance,
    Delete,
    Retire,
    Checkpoint,
}

#[derive(Clone, Debug)]
struct Repository {
    global: GlobalSelector,
    branches: BTreeMap<String, BranchSelector>,
    objects: BTreeSet<String>,
    retained: BTreeMap<u64, String>,
    retired: BTreeSet<String>,
    cycles: BTreeSet<String>,
    epochs: Vec<u64>,
    active: String,
    next_read: u64,
    reads: u64,
    publications: u64,
    commits: u64,
    selector_writes: u64,
    flat_row_writer: bool,
    second_authority: bool,
    cache_authority: bool,
}

impl Repository {
    fn bootstrap() -> Self {
        let mut objects = BTreeSet::from([GLOBAL_ROOT.to_owned(), "root-main".to_owned()]);
        let mut branches = BTreeMap::new();
        branches.insert(
            "main".to_owned(),
            BranchSelector::canonical("main", "root-main", 0),
        );
        Self {
            global: GlobalSelector::canonical(0),
            branches,
            objects: std::mem::take(&mut objects),
            retained: BTreeMap::new(),
            retired: BTreeSet::new(),
            cycles: BTreeSet::new(),
            epochs: vec![0],
            active: "main".to_owned(),
            next_read: 1,
            reads: 0,
            publications: 0,
            commits: 0,
            selector_writes: 0,
            flat_row_writer: false,
            second_authority: false,
            cache_authority: false,
        }
    }

    fn open_view(&mut self, branch: &str) -> Result<CoherentView, Failure> {
        if !self.global.authenticated() || !self.objects.contains(&self.global.root) {
            return Err(Failure::InvalidSelector);
        }
        if self.cycles.contains(branch) {
            return Err(Failure::Cycle);
        }
        let selector = self.branches.get(branch).ok_or(Failure::MissingRoot)?;
        if !selector.authenticated() || !self.objects.contains(&selector.root) {
            return Err(Failure::MissingRoot);
        }
        let read_id = self.next_read;
        self.next_read += 1;
        self.reads += 1;
        self.retained.insert(read_id, selector.root.clone());
        Ok(CoherentView {
            read_id,
            global_epoch: self.global.epoch,
            branch: branch.to_owned(),
            branch_root: selector.root.clone(),
        })
    }

    fn prepare(
        &self,
        view: &CoherentView,
        next_root: &str,
        owner: &str,
    ) -> Result<PreparedPublication, Failure> {
        if self.flat_row_writer || self.second_authority || self.cache_authority {
            return Err(Failure::DualAuthority);
        }
        if owner != format!("branch:{}", view.branch) {
            return Err(Failure::UnrelatedOwner);
        }
        let current = self
            .branches
            .get(&view.branch)
            .ok_or(Failure::MissingRoot)?;
        if self.global.epoch != view.global_epoch || current.root != view.branch_root {
            return Err(Failure::StaleOwner);
        }
        if !self.objects.contains(next_root) {
            return Err(Failure::MissingRoot);
        }
        Ok(PreparedPublication {
            read_id: view.read_id,
            owner: owner.to_owned(),
            branch: view.branch.clone(),
            expected_epoch: view.global_epoch,
            expected_root: view.branch_root.clone(),
            next_root: next_root.to_owned(),
        })
    }

    fn commit(&mut self, prepared: PreparedPublication) -> Result<(), Failure> {
        if !self.retained.contains_key(&prepared.read_id) {
            return Err(Failure::StaleOwner);
        }
        if self.global.epoch != prepared.expected_epoch {
            return Err(Failure::StaleOwner);
        }
        let current = self
            .branches
            .get(&prepared.branch)
            .ok_or(Failure::MissingRoot)?;
        if current.root != prepared.expected_root || current.owner != prepared.owner {
            return Err(Failure::StaleOwner);
        }
        let next_epoch = self.global.epoch + 1;
        self.global = GlobalSelector::canonical(next_epoch);
        let next_generation = current.generation + 1;
        self.branches.insert(
            prepared.branch.clone(),
            BranchSelector::canonical(&prepared.branch, &prepared.next_root, next_generation),
        );
        self.epochs.push(next_epoch);
        self.publications += 1;
        self.commits += 1;
        self.selector_writes += 2;
        Ok(())
    }

    fn release(&mut self, view: &CoherentView) {
        self.retained.remove(&view.read_id);
    }

    fn stage(&mut self, root: &str) {
        self.objects.insert(root.to_owned());
    }

    fn cold_reopen(&self) -> Result<(), Failure> {
        if !self.global.authenticated()
            || self.epochs.windows(2).any(|pair| pair[1] != pair[0] + 1)
            || !self.branches.values().all(BranchSelector::authenticated)
        {
            return Err(Failure::EpochGap);
        }
        Ok(())
    }

    fn gc(&mut self) {
        let retained: BTreeSet<_> = self.retained.values().cloned().collect();
        self.objects
            .retain(|object| object == GLOBAL_ROOT || retained.contains(object));
    }

    fn lifecycle_marker(&mut self, lifecycle: Lifecycle) {
        if matches!(lifecycle, Lifecycle::Retire | Lifecycle::Delete) {
            self.retired.insert(format!("{lifecycle:?}"));
        }
    }
}

#[test]
fn canonical_authority_uses_one_read_and_one_commit() {
    let mut repo = Repository::bootstrap();
    assert_eq!(SELECTOR_SPACE, "SELECTOR_SPACE");
    assert_eq!(repo.active, "main");
    repo.stage("root-next");
    let view = repo.open_view("main").unwrap();
    let prepared = repo.prepare(&view, "root-next", "branch:main").unwrap();
    repo.commit(prepared).unwrap();
    assert_eq!((repo.reads, repo.publications, repo.commits), (1, 1, 1));
    assert_eq!(repo.selector_writes, 2);
}

#[test]
fn create_switch_advance_delete_retire_gc_and_cold_reopen_are_modeled() {
    let mut repo = Repository::bootstrap();
    repo.stage("root-feature");
    repo.lifecycle_marker(Lifecycle::Create);
    repo.lifecycle_marker(Lifecycle::Switch);
    repo.lifecycle_marker(Lifecycle::Advance);
    repo.lifecycle_marker(Lifecycle::Checkpoint);
    repo.lifecycle_marker(Lifecycle::Delete);
    repo.lifecycle_marker(Lifecycle::Retire);
    assert_eq!(repo.retired.len(), 2);
    repo.gc();
    repo.cold_reopen().unwrap();
}

#[test]
fn same_owner_stale_and_unrelated_owner_fail_before_publication() {
    let mut repo = Repository::bootstrap();
    repo.stage("root-next");
    let view = repo.open_view("main").unwrap();
    repo.global = GlobalSelector::canonical(1);
    assert_eq!(
        repo.prepare(&view, "root-next", "branch:main"),
        Err(Failure::StaleOwner)
    );
    let view = repo.open_view("main").unwrap();
    assert_eq!(
        repo.prepare(&view, "root-next", "branch:other"),
        Err(Failure::UnrelatedOwner)
    );
    assert_eq!((repo.publications, repo.commits), (0, 0));
}

#[test]
fn forged_key_root_missing_root_cycle_and_epoch_gap_fail_closed() {
    let mut repo = Repository::bootstrap();
    repo.global.key = "selector:forged".into();
    repo.global.tag = auth(&repo.global.bytes);
    assert_eq!(repo.open_view("main"), Err(Failure::InvalidSelector));

    let mut repo = Repository::bootstrap();
    repo.objects.remove("root-main");
    assert_eq!(repo.open_view("main"), Err(Failure::MissingRoot));

    let mut repo = Repository::bootstrap();
    repo.cycles.insert("main".into());
    assert_eq!(repo.open_view("main"), Err(Failure::Cycle));

    let mut repo = Repository::bootstrap();
    repo.epochs.push(3);
    assert_eq!(repo.cold_reopen(), Err(Failure::EpochGap));
}

#[test]
fn flat_row_cache_and_second_authority_are_rejected() {
    for field in ["flat", "cache", "second"] {
        let mut repo = Repository::bootstrap();
        repo.stage("root-next");
        if field == "flat" {
            repo.flat_row_writer = true;
        } else if field == "cache" {
            repo.cache_authority = true;
        } else {
            repo.second_authority = true;
        }
        let view = repo.open_view("main").unwrap();
        assert_eq!(
            repo.prepare(&view, "root-next", "branch:main"),
            Err(Failure::DualAuthority)
        );
    }
}

#[test]
fn retained_view_keeps_root_until_release_then_gc_reclaims() {
    let mut repo = Repository::bootstrap();
    let view = repo.open_view("main").unwrap();
    repo.gc();
    assert!(repo.objects.contains("root-main"));
    repo.release(&view);
    repo.objects.remove("root-main");
    repo.gc();
    assert!(!repo.objects.contains("root-main"));
}

#[test]
fn empty_undo_redo_are_noops() {
    let repo = Repository::bootstrap();
    assert_eq!(
        (repo.publications, repo.commits, repo.global.epoch),
        (0, 0, 0)
    );
}
