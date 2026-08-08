//! Dependency-free TEST/REPORT-ONLY selector authority model.
//!
//! This is not Cargo-wired and is not run against compiler-red 705. It makes
//! the required old/new, stale, chronology, and fail-closed distinctions
//! executable for the later landing wave without adding a production owner.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Selector {
    owner: String,
    root: u64,
    generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Commit {
    id: u64,
    parents: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct View {
    global: Selector,
    branch: Selector,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Error {
    Corrupt,
    Stale,
    WrongOwner,
    Cycle,
    FloorMissing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Authority {
    global: Selector,
    branches: BTreeMap<String, Selector>,
    commits: BTreeMap<u64, Commit>,
    checkpoint_floor: BTreeSet<u64>,
}

impl Authority {
    fn view(&self, branch: &str) -> Result<View, Error> {
        let branch_id = branch.to_owned();
        let branch = self.branches.get(branch).cloned().ok_or(Error::Corrupt)?;
        if branch.owner != branch_id || self.global.root == 0 || branch.root == 0 {
            return Err(Error::Corrupt);
        }
        Ok(View {
            global: self.global.clone(),
            branch,
        })
    }

    fn publish(&mut self, view: &View, owner: &str, new_root: u64) -> Result<(), Error> {
        let current = self.branches.get(owner).ok_or(Error::WrongOwner)?;
        if current != &view.branch || self.global != view.global || new_root == 0 {
            return Err(Error::Stale);
        }
        let mut next = self.clone();
        next.global.generation += 1;
        next.global.root = new_root;
        let branch = next.branches.get_mut(owner).ok_or(Error::WrongOwner)?;
        branch.root = new_root;
        branch.generation += 1;
        *self = next;
        Ok(())
    }

    fn first_parent_to_floor(&self, head: u64, floor: u64) -> Result<Vec<u64>, Error> {
        let mut result = Vec::new();
        let mut seen = BTreeSet::new();
        let mut current = head;
        loop {
            if !seen.insert(current) {
                return Err(Error::Cycle);
            }
            let node = self.commits.get(&current).ok_or(Error::Corrupt)?;
            result.push(current);
            if current == floor {
                return Ok(result);
            }
            current = *node.parents.first().ok_or(Error::FloorMissing)?;
        }
    }
}

#[test]
fn stale_same_owner_and_wrong_owner_are_not_accepted() {
    let mut authority = fixture();
    let view = authority.view("main").unwrap();
    authority.publish(&view, "main", 2).unwrap();
    assert_eq!(authority.publish(&view, "main", 3), Err(Error::Stale));
    assert_eq!(authority.publish(&view, "other", 3), Err(Error::Stale));
}

#[test]
fn checkpoint_floor_is_not_a_selector_parent() {
    let authority = fixture();
    let path = authority.first_parent_to_floor(3, 1).unwrap();
    assert_eq!(path, vec![3, 2, 1]);
    assert!(!authority.commits[&3].parents.contains(&authority.global.root));
}

#[test]
fn malformed_missing_and_cycle_inputs_fail_closed() {
    let mut authority = fixture();
    authority.global.root = 0;
    assert_eq!(authority.view("main"), Err(Error::Corrupt));

    let mut authority = fixture();
    authority.commits.insert(
        3,
        Commit {
            id: 3,
            parents: vec![3],
        },
    );
    assert_eq!(authority.first_parent_to_floor(3, 1), Err(Error::Cycle));
}

#[test]
fn publication_is_all_or_nothing() {
    let mut authority = fixture();
    let before = authority.clone();
    let view = authority.view("main").unwrap();
    assert_eq!(authority.publish(&view, "main", 0), Err(Error::Stale));
    assert_eq!(authority, before);
}

fn fixture() -> Authority {
    let global = Selector {
        owner: "global".into(),
        root: 1,
        generation: 1,
    };
    let branch = |owner: &str, root| Selector {
        owner: owner.into(),
        root,
        generation: 1,
    };
    Authority {
        global,
        branches: BTreeMap::from([
            ("main".into(), branch("main", 1)),
            ("other".into(), branch("other", 1)),
        ]),
        commits: BTreeMap::from([
            (
                1,
                Commit {
                    id: 1,
                    parents: Vec::new(),
                },
            ),
            (
                2,
                Commit {
                    id: 2,
                    parents: vec![1],
                },
            ),
            (
                3,
                Commit {
                    id: 3,
                    parents: vec![2],
                },
            ),
        ]),
        checkpoint_floor: BTreeSet::from([1]),
    }
}
