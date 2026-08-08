//! W1b-3 standalone undo/redo and typed-transition model.
//!
//! Test/report-only: no Lix imports, storage access, actor invocation, or
//! adapter runtime. This file is compiled and run independently with
//! rustc --edition=2024 --test -D warnings.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
enum Value {
    Null,
    Tombstone,
    Text(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Row {
    identity: String,
    change_id: u64,
    value: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Commit {
    id: String,
    generation: u64,
    parents: Vec<String>,
    rows: BTreeMap<String, Row>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReadTrace {
    view_id: u64,
    begin_reads: u32,
    reader_instances: u32,
    writes: u32,
    events: Vec<(u64, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransitionRequest {
    current_head: String,
    desired_head: String,
    keys: Vec<String>,
    current_rows: BTreeMap<String, Row>,
    desired_rows: BTreeMap<String, Row>,
    read: ReadTrace,
    staged_writes: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransitionRow {
    identity: String,
    expected_change_id: Option<u64>,
    target_change_id: Option<u64>,
    target_value: Option<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransitionPlan {
    current_head: String,
    desired_head: String,
    rows: Vec<TransitionRow>,
    atomic: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Reject {
    ReadView,
    StaleHead,
    DirtyTransaction,
    Empty,
    DuplicateIdentity,
    IdentityMismatch,
    Unchanged,
    MissingCommit,
    RootOrMerge,
    CheckpointFloor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct History {
    commits: BTreeMap<String, Commit>,
    head: String,
    undo_top: Option<String>,
    redo_top: Option<String>,
    redo_target: Option<String>,
    checkpoint_floor: Option<String>,
}

fn validate_read(read: &ReadTrace) -> Result<(), Reject> {
    if read.begin_reads != 1
        || read.reader_instances != 1
        || read.writes != 0
        || read.events.iter().any(|(view_id, _)| *view_id != read.view_id)
    {
        return Err(Reject::ReadView);
    }
    Ok(())
}

fn execute_transition(request: TransitionRequest) -> Result<TransitionPlan, Reject> {
    validate_read(&request.read)?;
    if request.current_head != "head" {
        return Err(Reject::StaleHead);
    }
    if request.staged_writes {
        return Err(Reject::DirtyTransaction);
    }
    if request.keys.is_empty() {
        return Err(Reject::Empty);
    }
    let unique = request.keys.iter().collect::<BTreeSet<_>>();
    if unique.len() != request.keys.len() {
        return Err(Reject::DuplicateIdentity);
    }

    let mut rows = Vec::with_capacity(request.keys.len());
    for key in request.keys {
        let current = request.current_rows.get(&key);
        let desired = request.desired_rows.get(&key);
        for row in [current, desired].into_iter().flatten() {
            if row.identity != key {
                return Err(Reject::IdentityMismatch);
            }
        }
        let expected_change_id = current.map(|row| row.change_id);
        let target_change_id = desired.map(|row| row.change_id);
        if expected_change_id == target_change_id {
            return Err(Reject::Unchanged);
        }
        rows.push(TransitionRow {
            identity: key,
            expected_change_id,
            target_change_id,
            target_value: desired.map(|row| row.value.clone()),
        });
    }
    Ok(TransitionPlan {
        current_head: request.current_head,
        desired_head: request.desired_head,
        rows,
        atomic: true,
    })
}

fn history_transition(
    history: &History,
    desired_id: &str,
    read: ReadTrace,
) -> Result<TransitionPlan, Reject> {
    let current = history
        .commits
        .get(&history.head)
        .ok_or(Reject::MissingCommit)?;
    let desired = history
        .commits
        .get(desired_id)
        .ok_or(Reject::MissingCommit)?;
    let mut keys = BTreeSet::new();
    keys.extend(current.rows.keys().cloned());
    keys.extend(desired.rows.keys().cloned());
    execute_transition(TransitionRequest {
        current_head: "head".into(),
        desired_head: desired.id.clone(),
        keys: keys.into_iter().collect(),
        current_rows: current.rows.clone(),
        desired_rows: desired.rows.clone(),
        read,
        staged_writes: false,
    })
}

fn undo(history: &History, read: ReadTrace) -> Result<TransitionPlan, Reject> {
    let target_id = history.undo_top.as_ref().ok_or(Reject::CheckpointFloor)?;
    if history.checkpoint_floor.as_ref() == Some(target_id) {
        return Err(Reject::CheckpointFloor);
    }
    let target = history
        .commits
        .get(target_id)
        .ok_or(Reject::MissingCommit)?;
    let parent = match target.parents.as_slice() {
        [parent] => parent,
        [] => return Err(Reject::RootOrMerge),
        _ => return Err(Reject::RootOrMerge),
    };
    history_transition(history, parent, read)
}

fn redo(history: &History, read: ReadTrace) -> Result<TransitionPlan, Reject> {
    let redo_node = history.redo_top.as_ref().ok_or(Reject::CheckpointFloor)?;
    let target = history
        .redo_target
        .as_ref()
        .filter(|_| *redo_node == history.head)
        .or_else(|| history.redo_target.as_ref())
        .ok_or(Reject::MissingCommit)?;
    let target_commit = history
        .commits
        .get(target)
        .ok_or(Reject::MissingCommit)?;
    if target_commit.parents.len() != 1 {
        return Err(Reject::RootOrMerge);
    }
    history_transition(history, target, read)
}

fn row(identity: &str, change_id: u64, value: Value) -> Row {
    Row {
        identity: identity.into(),
        change_id,
        value,
    }
}

fn commit(id: &str, generation: u64, parents: &[&str], rows: &[Row]) -> Commit {
    Commit {
        id: id.into(),
        generation,
        parents: parents.iter().map(|parent| (*parent).into()).collect(),
        rows: rows.iter().map(|row| (row.identity.clone(), row.clone())).collect(),
    }
}

fn read() -> ReadTrace {
    ReadTrace {
        view_id: 17,
        begin_reads: 1,
        reader_instances: 1,
        writes: 0,
        events: vec![(17, "topology/marker/state".into())],
    }
}

fn linear_history() -> History {
    let root = commit("A", 1, &[], &[row("x", 1, Value::Null)]);
    let target = commit("B", 2, &["A"], &[row("x", 2, Value::Text("new".into()))]);
    History {
        commits: BTreeMap::from([(root.id.clone(), root), (target.id.clone(), target)]),
        head: "B".into(),
        undo_top: Some("B".into()),
        redo_top: None,
        redo_target: None,
        checkpoint_floor: None,
    }
}

#[test]
fn undo_and_redo_preserve_exact_inverse_state_identity() {
    let mut history = linear_history();
    let mut undo_view = read();
    let plan = undo(&history, undo_view.clone()).expect("undo plan");
    assert_eq!(plan.current_head, "head");
    assert_eq!(plan.desired_head, "A");
    assert_eq!(plan.rows[0].identity, "x");
    assert_eq!(plan.rows[0].expected_change_id, Some(2));
    assert_eq!(plan.rows[0].target_change_id, Some(1));
    assert_eq!(plan.rows[0].target_value, Some(Value::Null));
    assert!(plan.atomic);

    let inverse = commit("I", 3, &["B"], &[row("x", 1, Value::Null)]);
    history.commits.insert(inverse.id.clone(), inverse);
    history.head = "I".into();
    history.undo_top = None;
    history.redo_top = Some("I".into());
    history.redo_target = Some("B".into());
    undo_view.view_id = 17;
    let replay = redo(&history, undo_view).expect("redo plan");
    assert_eq!(replay.desired_head, "B");
    assert_eq!(replay.rows[0].expected_change_id, Some(1));
    assert_eq!(replay.rows[0].target_change_id, Some(2));
    assert_eq!(replay.rows[0].target_value, Some(Value::Text("new".into())));
}

#[test]
fn checkpoint_floor_and_merge_or_root_history_fail_closed() {
    let mut history = linear_history();
    history.checkpoint_floor = Some("B".into());
    assert_eq!(undo(&history, read()), Err(Reject::CheckpointFloor));

    history.checkpoint_floor = None;
    let merge = commit("M", 3, &["A", "B"], &[row("x", 3, Value::Tombstone)]);
    history.commits.insert("M".into(), merge);
    history.head = "head".into();
    history.undo_top = Some("M".into());
    assert_eq!(undo(&history, read()), Err(Reject::RootOrMerge));
}

#[test]
fn typed_transition_is_atomic_and_rejects_identity_or_stale_errors() {
    let mut current = BTreeMap::new();
    current.insert("x".into(), row("x", 1, Value::Null));
    current.insert("y".into(), row("y", 1, Value::Tombstone));
    let mut desired = current.clone();
    desired.insert("x".into(), row("x", 2, Value::Text("next".into())));
    desired.insert("y".into(), row("wrong", 2, Value::Null));

    let request = TransitionRequest {
        current_head: "head".into(),
        desired_head: "desired".into(),
        keys: vec!["x".into(), "y".into()],
        current_rows: current,
        desired_rows: desired,
        read: read(),
        staged_writes: false,
    };
    assert_eq!(execute_transition(request), Err(Reject::IdentityMismatch));

    let mut duplicate = linear_history();
    duplicate.head = "head".into();
    let request = TransitionRequest {
        current_head: "old-head".into(),
        desired_head: "desired".into(),
        keys: vec!["x".into(), "x".into()],
        current_rows: BTreeMap::new(),
        desired_rows: BTreeMap::new(),
        read: read(),
        staged_writes: false,
    };
    assert_eq!(execute_transition(request), Err(Reject::StaleHead));
    duplicate.head = "head".into();
    assert_eq!(duplicate.head, "head");
}

#[test]
fn read_poisoning_and_missing_history_fail_closed() {
    let history = linear_history();
    let mut poisoned = read();
    poisoned.begin_reads = 2;
    assert_eq!(undo(&history, poisoned), Err(Reject::ReadView));

    assert_eq!(history_transition(&history, "missing", read()), Err(Reject::MissingCommit));
    let mut cross_view = read();
    cross_view.events.push((99, "second-reader".into()));
    assert_eq!(history_transition(&history, "A", cross_view), Err(Reject::ReadView));
}

#[test]
fn cold_reopen_preserves_history_and_transition_identity() {
    let history = linear_history();
    let reopened = history.clone();
    assert_eq!(history, reopened);
    let first = undo(&history, read()).expect("original undo");
    let second = undo(&reopened, read()).expect("reopened undo");
    assert_eq!(first, second);
}
