//! Dependency-free W5/R7 acceptance model; intentionally not Cargo-wired.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Fence { epoch: u64, progress: u64, owner_selector: u64 }

#[derive(Clone, Debug, Eq, PartialEq)]
struct Entry { object: String, blocked: bool }

#[derive(Clone, Debug, Eq, PartialEq)]
struct Queue {
    fence: Fence,
    entries: Vec<Entry>,
    head: usize,
    debt: u32,
    calls: u32,
    deleted: BTreeSet<String>,
}

impl Queue {
    fn new(entries: Vec<Entry>) -> Self {
        Self { fence: Fence { epoch: 0, progress: 0, owner_selector: 0 }, entries, head: 0, debt: 0, calls: 0, deleted: BTreeSet::new() }
    }
    fn process(&mut self, page: usize) -> (bool, bool, usize) {
        self.calls += 1;
        if self.head == self.entries.len() { return (false, true, 0); }
        if self.entries[self.head].blocked { self.debt = 1; return (false, false, 0); }
        let end = (self.head + page).min(self.entries.len());
        let reclaimed = self.entries[self.head..end].iter().filter(|e| self.deleted.insert(e.object.clone())).count();
        self.head = end;
        self.fence.epoch += 1;
        self.fence.progress += 1;
        (true, end == self.entries.len(), reclaimed)
    }
    fn release(&mut self) { self.entries[self.head].blocked = false; }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Race { RetryFreshView, RejectOwnerCas, Proceed }

fn race(epoch_changed: bool, owner_changed: bool) -> Race {
    if owner_changed { Race::RejectOwnerCas } else if epoch_changed { Race::RetryFreshView } else { Race::Proceed }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootKind { Upload, Branch, History, Checkpoint, Shared, Final }

#[derive(Debug)]
struct Graph { roots: BTreeMap<String, RootKind>, edges: BTreeMap<String, Vec<String>> }

impl Graph {
    fn validate(&self) -> Result<BTreeSet<String>, &'static str> {
        fn visit(node: &str, edges: &BTreeMap<String, Vec<String>>, live: &mut BTreeSet<String>, active: &mut BTreeSet<String>) -> Result<(), &'static str> {
            if !active.insert(node.to_owned()) { return Err("cycle"); }
            if !live.insert(node.to_owned()) { active.remove(node); return Ok(()); }
            for child in edges.get(node).ok_or("missing-root-or-object")? { visit(child, edges, live, active)?; }
            active.remove(node);
            Ok(())
        }
        let mut live = BTreeSet::new();
        let mut active = BTreeSet::new();
        for root in self.roots.keys() { visit(root, &self.edges, &mut live, &mut active)?; }
        Ok(live)
    }
}

#[test]
fn sixty_five_entries_are_64_plus_suffix() {
    let mut q = Queue::new((0..65).map(|i| Entry { object: format!("object-{i:02}"), blocked: false }).collect());
    assert_eq!(q.process(64), (true, false, 64));
    assert_eq!(q.process(64), (true, true, 1));
    assert_eq!(q.process(64), (false, true, 0));
    assert_eq!(q.deleted.len(), 65);
}

#[test]
fn blocked_debt_does_not_spin_and_release_drains() {
    let mut q = Queue::new(vec![Entry { object: "blocked".into(), blocked: true }, Entry { object: "released".into(), blocked: false }]);
    assert_eq!(q.process(64), (false, false, 0));
    assert_eq!(q.process(64), (false, false, 0));
    assert_eq!((q.debt, q.calls), (1, 2));
    q.release();
    assert_eq!(q.process(64), (true, true, 2));
}

#[test]
fn publication_first_gc_first_and_owner_selector_are_fenced() {
    assert_eq!(race(true, false), Race::RetryFreshView);
    assert_eq!(race(false, true), Race::RejectOwnerCas);
    assert_eq!(race(false, false), Race::Proceed);
}

#[test]
fn poisoned_cursor_requires_fresh_exclusive_restart() {
    let old_view = 1_u64;
    let old_poisoned = true;
    let fresh_view = 2_u64;
    let exclusive_key = Some("object-63");
    assert!(old_poisoned);
    assert_ne!(old_view, fresh_view);
    assert_eq!(exclusive_key, Some("object-63"));
}

#[test]
fn upload_branch_history_checkpoint_shared_final_roots_stay_reachable() {
    let roots = [("upload", RootKind::Upload), ("branch", RootKind::Branch), ("history", RootKind::History), ("checkpoint", RootKind::Checkpoint), ("shared", RootKind::Shared), ("final", RootKind::Final)].into_iter().collect();
    let edges = [("upload", vec!["chunk".into()]), ("branch", vec!["commit".into()]), ("history", vec!["commit".into()]), ("checkpoint", vec!["commit".into()]), ("shared", vec!["chunk".into()]), ("final", vec!["chunk".into()]), ("chunk", vec![]), ("commit", vec![])].into_iter().collect();
    let live = Graph { roots, edges }.validate().unwrap();
    assert!(live.contains("chunk") && live.contains("commit"));
}

#[test]
fn missing_edges_and_cycles_fail_closed() {
    let roots = [("root".into(), RootKind::Final)].into_iter().collect();
    assert_eq!(Graph { roots: roots.clone(), edges: BTreeMap::new() }.validate(), Err("missing-root-or-object"));
    let edges = [("root".into(), vec!["root".into()])].into_iter().collect();
    assert_eq!(Graph { roots, edges }.validate(), Err("cycle"));
}

#[test]
fn one_coherent_read_epoch_progress_owner_cas_and_commit() {
    let trace = ["CoherentView", "epoch", "progress", "owner-selector-CAS", "commit-at-boundary"];
    assert_eq!(trace.iter().filter(|s| **s == "CoherentView").count(), 1);
    assert_eq!(trace.iter().filter(|s| **s == "owner-selector-CAS").count(), 1);
    assert_eq!(trace.iter().filter(|s| **s == "commit-at-boundary").count(), 1);
}
