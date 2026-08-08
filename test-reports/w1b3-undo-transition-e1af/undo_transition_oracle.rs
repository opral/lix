//! W1b-3 stateful undo/redo and typed-transition model.
//!
//! Test/report-only: no Lix imports, storage access, actor invocation, or
//! adapter runtime. The model is compiled independently with Rust 2024 and
//! warnings denied. It validates topology, generations, marker floors,
//! inverse/replay identity, and atomic selector/cursor updates.

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
struct ReadCall {
    facade_id: u64,
    view_id: u64,
    operation: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReadTrace {
    view_id: u64,
    facade_id: u64,
    begin_reads: u32,
    reader_instances: u32,
    writes: u32,
    raw_store_reads: u32,
    fallback_reads: u32,
    cache_authority_reads: u32,
    calls: Vec<ReadCall>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransitionRequest {
    observed_head: String,
    expected_head: String,
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
    Chronology,
    RootOrMerge,
    CheckpointFloor,
    NoRedo,
    CursorMismatch,
    SelectorMismatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct History {
    commits: BTreeMap<String, Commit>,
    head: String,
    undo_target: Option<String>,
    redo_cursor: Option<String>,
    redo_target: Option<String>,
    checkpoint_floor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Repository {
    history: History,
    selector_head: String,
    selector_generation: u64,
}

fn validate_read(read: &ReadTrace) -> Result<(), Reject> {
    if read.begin_reads != 1
        || read.reader_instances != 1
        || read.writes != 0
        || read.raw_store_reads != 0
        || read.fallback_reads != 0
        || read.cache_authority_reads != 0
        || read.calls.is_empty()
        || read.calls.iter().any(|call| {
            call.facade_id != read.facade_id
                || call.view_id != read.view_id
                || call.operation.is_empty()
        })
    {
        return Err(Reject::ReadView);
    }
    Ok(())
}

fn validate_commit_graph(history: &History) -> Result<(), Reject> {
    if !history.commits.contains_key(&history.head) {
        return Err(Reject::MissingCommit);
    }
    for commit in history.commits.values() {
        let mut parent_ids = BTreeSet::new();
        let mut maximum_parent_generation: Option<u64> = None;
        for parent_id in &commit.parents {
            if !parent_ids.insert(parent_id) {
                return Err(Reject::Chronology);
            }
            let parent = history
                .commits
                .get(parent_id)
                .ok_or(Reject::MissingCommit)?;
            maximum_parent_generation = Some(
                maximum_parent_generation
                    .map_or(parent.generation, |current| current.max(parent.generation)),
            );
        }
        match maximum_parent_generation {
            None if commit.generation != 0 => return Err(Reject::Chronology),
            Some(parent_generation)
                if commit.parents.len() == 1
                    && commit.generation != parent_generation.saturating_add(1) =>
            {
                return Err(Reject::Chronology);
            }
            Some(parent_generation) if commit.generation <= parent_generation => {
                return Err(Reject::Chronology);
            }
            _ => {}
        }
    }
    Ok(())
}

fn execute_transition(request: TransitionRequest) -> Result<TransitionPlan, Reject> {
    validate_read(&request.read)?;
    if request.observed_head != request.expected_head {
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
        if current == desired {
            return Err(Reject::Unchanged);
        }
        let expected_change_id = current.map(|row| row.change_id);
        let target_change_id = desired.map(|row| row.change_id);
        if expected_change_id == target_change_id
            && current.map(|row| &row.value) != desired.map(|row| &row.value)
        {
            return Err(Reject::IdentityMismatch);
        }
        rows.push(TransitionRow {
            identity: key,
            expected_change_id,
            target_change_id,
            target_value: desired.map(|row| row.value.clone()),
        });
    }
    Ok(TransitionPlan {
        current_head: request.observed_head,
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
    validate_commit_graph(history)?;
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
        observed_head: history.head.clone(),
        expected_head: history.head.clone(),
        desired_head: desired.id.clone(),
        keys: keys.into_iter().collect(),
        current_rows: current.rows.clone(),
        desired_rows: desired.rows.clone(),
        read,
        staged_writes: false,
    })
}

fn first_parent(history: &History, target_id: &str) -> Result<String, Reject> {
    validate_commit_graph(history)?;
    let target = history
        .commits
        .get(target_id)
        .ok_or(Reject::MissingCommit)?;
    match target.parents.as_slice() {
        [parent] => Ok(parent.clone()),
        [] | [_, ..] => Err(Reject::RootOrMerge),
    }
}

fn undo_plan(history: &History, read: ReadTrace) -> Result<TransitionPlan, Reject> {
    let target_id = history
        .undo_target
        .as_ref()
        .ok_or(Reject::CheckpointFloor)?;
    if target_id != &history.head {
        return Err(Reject::CursorMismatch);
    }
    let parent_id = first_parent(history, target_id)?;
    if let Some(floor_id) = &history.checkpoint_floor {
        let floor = history.commits.get(floor_id).ok_or(Reject::MissingCommit)?;
        let parent = history
            .commits
            .get(&parent_id)
            .ok_or(Reject::MissingCommit)?;
        if parent.generation < floor.generation {
            return Err(Reject::CheckpointFloor);
        }
    }
    history_transition(history, &parent_id, read)
}

fn redo_plan(history: &History, read: ReadTrace) -> Result<TransitionPlan, Reject> {
    let cursor = history.redo_cursor.as_ref().ok_or(Reject::NoRedo)?;
    if cursor != &history.head {
        return Err(Reject::CursorMismatch);
    }
    let target_id = history.redo_target.as_ref().ok_or(Reject::NoRedo)?;
    let target = history
        .commits
        .get(target_id)
        .ok_or(Reject::MissingCommit)?;
    if target.parents.as_slice() != [history.head.clone()] {
        return Err(Reject::CursorMismatch);
    }
    history_transition(history, target_id, read)
}

impl Repository {
    fn new(history: History) -> Self {
        Self {
            selector_head: history.head.clone(),
            selector_generation: 0,
            history,
        }
    }

    fn apply_undo(&mut self, read: ReadTrace) -> Result<TransitionPlan, Reject> {
        if self.selector_head != self.history.head {
            return Err(Reject::SelectorMismatch);
        }
        let plan = undo_plan(&self.history, read)?;
        let old_head = self.history.head.clone();
        self.history.head = plan.desired_head.clone();
        self.history.undo_target = Some(self.history.head.clone());
        self.history.redo_cursor = Some(self.history.head.clone());
        self.history.redo_target = Some(old_head);
        self.selector_head = self.history.head.clone();
        self.selector_generation += 1;
        Ok(plan)
    }

    fn apply_redo(&mut self, read: ReadTrace) -> Result<TransitionPlan, Reject> {
        if self.selector_head != self.history.head {
            return Err(Reject::SelectorMismatch);
        }
        let plan = redo_plan(&self.history, read)?;
        self.history.head = plan.desired_head.clone();
        self.history.undo_target = Some(self.history.head.clone());
        self.history.redo_cursor = None;
        self.history.redo_target = None;
        self.selector_head = self.history.head.clone();
        self.selector_generation += 1;
        Ok(plan)
    }

    fn ordinary_commit(&mut self, id: &str, rows: &[Row]) -> Result<(), Reject> {
        if self.selector_head != self.history.head {
            return Err(Reject::SelectorMismatch);
        }
        let parent = self
            .history
            .commits
            .get(&self.history.head)
            .ok_or(Reject::MissingCommit)?;
        let commit = commit(id, parent.generation + 1, &[&parent.id], rows);
        self.history.commits.insert(commit.id.clone(), commit);
        self.history.head = id.into();
        self.history.undo_target = Some(id.into());
        self.history.redo_cursor = None;
        self.history.redo_target = None;
        self.selector_head = id.into();
        self.selector_generation += 1;
        Ok(())
    }

    fn cold_reopen(&self) -> Result<Self, Reject> {
        validate_commit_graph(&self.history)?;
        if self.selector_head != self.history.head {
            return Err(Reject::SelectorMismatch);
        }
        Ok(self.clone())
    }
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
        rows: rows
            .iter()
            .map(|row| (row.identity.clone(), row.clone()))
            .collect(),
    }
}

fn read() -> ReadTrace {
    ReadTrace {
        view_id: 17,
        facade_id: 23,
        begin_reads: 1,
        reader_instances: 1,
        writes: 0,
        raw_store_reads: 0,
        fallback_reads: 0,
        cache_authority_reads: 0,
        calls: vec![
            ReadCall {
                facade_id: 23,
                view_id: 17,
                operation: "first_parent/marker/exact_rows".into(),
            },
            ReadCall {
                facade_id: 23,
                view_id: 17,
                operation: "inverse_or_replay_transition".into(),
            },
        ],
    }
}

fn linear_history() -> History {
    let root = commit("A", 0, &[], &[row("x", 1, Value::Null)]);
    let target = commit("B", 1, &["A"], &[row("x", 2, Value::Text("new".into()))]);
    History {
        commits: BTreeMap::from([(root.id.clone(), root), (target.id.clone(), target)]),
        head: "B".into(),
        undo_target: Some("B".into()),
        redo_cursor: None,
        redo_target: None,
        checkpoint_floor: None,
    }
}

fn three_commit_history() -> History {
    let mut history = linear_history();
    let target = commit("C", 2, &["B"], &[row("x", 3, Value::Text("third".into()))]);
    history.commits.insert(target.id.clone(), target);
    history.head = "C".into();
    history.undo_target = Some("C".into());
    history
}

#[test]
fn undo_and_redo_preserve_exact_inverse_state_identity() {
    let mut repository = Repository::new(linear_history());
    let undo = repository.apply_undo(read()).expect("undo plan");
    assert_eq!(undo.current_head, "B");
    assert_eq!(undo.desired_head, "A");
    assert_eq!(undo.rows[0].identity, "x");
    assert_eq!(undo.rows[0].expected_change_id, Some(2));
    assert_eq!(undo.rows[0].target_change_id, Some(1));
    assert_eq!(undo.rows[0].target_value, Some(Value::Null));
    assert!(undo.atomic);

    let redo = repository.apply_redo(read()).expect("redo plan");
    assert_eq!(redo.current_head, "A");
    assert_eq!(redo.desired_head, "B");
    assert_eq!(redo.rows[0].expected_change_id, Some(1));
    assert_eq!(redo.rows[0].target_change_id, Some(2));
    assert_eq!(redo.rows[0].target_value, Some(Value::Text("new".into())));
}

#[test]
fn generation_first_parent_root_and_merge_fail_closed() {
    let mut invalid = linear_history();
    invalid.commits.get_mut("B").expect("B").generation = 0;
    assert_eq!(validate_commit_graph(&invalid), Err(Reject::Chronology));

    let mut root = linear_history();
    root.head = "A".into();
    root.undo_target = Some("A".into());
    assert_eq!(
        Repository::new(root).apply_undo(read()),
        Err(Reject::RootOrMerge)
    );

    let mut merge = linear_history();
    let merge_commit = commit("M", 2, &["A", "B"], &[row("x", 3, Value::Tombstone)]);
    merge.commits.insert("M".into(), merge_commit);
    merge.head = "M".into();
    merge.undo_target = Some("M".into());
    assert_eq!(
        Repository::new(merge).apply_undo(read()),
        Err(Reject::RootOrMerge)
    );
}

#[test]
fn ordered_checkpoint_floor_allows_to_floor_but_not_below() {
    let mut repository = Repository::new(three_commit_history());
    repository.history.checkpoint_floor = Some("B".into());
    repository.apply_undo(read()).expect("C -> B is allowed");
    let before = repository.clone();
    assert_eq!(repository.apply_undo(read()), Err(Reject::CheckpointFloor));
    assert_eq!(repository, before);
}

#[test]
fn ordinary_commit_after_undo_discards_redo_cursor() {
    let mut repository = Repository::new(linear_history());
    repository.apply_undo(read()).expect("undo");
    repository
        .ordinary_commit("N", &[row("x", 4, Value::Text("new branch".into()))])
        .expect("ordinary commit");
    assert_eq!(repository.apply_redo(read()), Err(Reject::NoRedo));
    assert_eq!(repository.history.redo_cursor, None);
    assert_eq!(repository.history.redo_target, None);
}

#[test]
fn redo_cursor_mismatch_fails_closed_without_selector_update() {
    let mut repository = Repository::new(linear_history());
    repository.apply_undo(read()).expect("undo");
    repository.history.redo_cursor = Some("B".into());
    let before = repository.clone();
    assert_eq!(repository.apply_redo(read()), Err(Reject::CursorMismatch));
    assert_eq!(repository, before);
}

#[test]
fn absence_null_and_tombstone_are_distinct_valid_transition_values() {
    let mut current = BTreeMap::new();
    current.insert("null".into(), row("null", 1, Value::Null));
    current.insert("tombstone".into(), row("tombstone", 1, Value::Tombstone));
    let mut desired = BTreeMap::new();
    desired.insert("null".into(), row("null", 2, Value::Text("value".into())));
    desired.insert("new".into(), row("new", 1, Value::Text("created".into())));
    let plan = execute_transition(TransitionRequest {
        observed_head: "B".into(),
        expected_head: "B".into(),
        desired_head: "A".into(),
        keys: vec!["new".into(), "null".into(), "tombstone".into()],
        current_rows: current,
        desired_rows: desired,
        read: read(),
        staged_writes: false,
    })
    .expect("distinct absence/null/tombstone transition");
    assert_eq!(
        plan.rows[0].target_value,
        Some(Value::Text("created".into()))
    );
    assert_eq!(plan.rows[1].target_value, Some(Value::Text("value".into())));
    assert_eq!(plan.rows[2].target_value, None);
}

#[test]
fn atomic_failure_leaves_selector_cursor_and_rows_unchanged() {
    let mut repository = Repository::new(linear_history());
    let before = repository.clone();
    let mut poisoned = read();
    poisoned.begin_reads = 2;
    assert_eq!(repository.apply_undo(poisoned), Err(Reject::ReadView));
    assert_eq!(repository, before);

    let mut current = BTreeMap::new();
    current.insert("x".into(), row("x", 1, Value::Null));
    let mut desired = current.clone();
    desired.insert("x".into(), row("wrong", 2, Value::Text("bad".into())));
    let request = TransitionRequest {
        observed_head: "B".into(),
        expected_head: "B".into(),
        desired_head: "A".into(),
        keys: vec!["x".into()],
        current_rows: current,
        desired_rows: desired,
        read: read(),
        staged_writes: false,
    };
    assert_eq!(execute_transition(request), Err(Reject::IdentityMismatch));
}

#[test]
fn fresh_alias_raw_store_fallback_and_cache_reads_fail_closed() {
    let history = linear_history();
    let mut alias = read();
    alias.calls[1].facade_id = 99;
    assert_eq!(undo_plan(&history, alias), Err(Reject::ReadView));

    let mut raw_store = read();
    raw_store.raw_store_reads = 1;
    assert_eq!(undo_plan(&history, raw_store), Err(Reject::ReadView));

    let mut fallback = read();
    fallback.fallback_reads = 1;
    assert_eq!(undo_plan(&history, fallback), Err(Reject::ReadView));

    let mut cache = read();
    cache.cache_authority_reads = 1;
    assert_eq!(undo_plan(&history, cache), Err(Reject::ReadView));
}

#[test]
fn duplicate_empty_missing_and_cold_reopen_fail_closed_or_preserve_state() {
    let repository = Repository::new(linear_history());
    let before = repository.clone();
    let empty = execute_transition(TransitionRequest {
        observed_head: "B".into(),
        expected_head: "B".into(),
        desired_head: "A".into(),
        keys: Vec::new(),
        current_rows: BTreeMap::new(),
        desired_rows: BTreeMap::new(),
        read: read(),
        staged_writes: false,
    });
    assert_eq!(empty, Err(Reject::Empty));
    assert_eq!(repository, before);

    let duplicate = execute_transition(TransitionRequest {
        observed_head: "B".into(),
        expected_head: "B".into(),
        desired_head: "A".into(),
        keys: vec!["x".into(), "x".into()],
        current_rows: BTreeMap::new(),
        desired_rows: BTreeMap::from([(String::from("x"), row("x", 1, Value::Text("x".into())))]),
        read: read(),
        staged_writes: false,
    });
    assert_eq!(duplicate, Err(Reject::DuplicateIdentity));

    assert_eq!(
        history_transition(&repository.history, "missing", read()),
        Err(Reject::MissingCommit)
    );
    assert_eq!(repository.cold_reopen(), Ok(repository.clone()));
}
