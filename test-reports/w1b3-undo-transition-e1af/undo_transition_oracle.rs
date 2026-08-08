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
    CorruptEnvelope,
    InjectedFailure,
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
        || !read
            .calls
            .iter()
            .any(|call| call.operation == "first_parent/marker/exact_rows")
        || !read
            .calls
            .iter()
            .any(|call| call.operation == "inverse_or_replay_transition")
    {
        return Err(Reject::ReadView);
    }
    Ok(())
}

fn validate_commit_graph(history: &History) -> Result<(), Reject> {
    if !history.commits.contains_key(&history.head) {
        return Err(Reject::MissingCommit);
    }
    for (map_id, commit) in &history.commits {
        if map_id.is_empty() || commit.id != map_id.as_str() {
            return Err(Reject::IdentityMismatch);
        }
        for (row_id, row) in &commit.rows {
            if row_id.is_empty() || row.identity != row_id.as_str() {
                return Err(Reject::IdentityMismatch);
            }
        }
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
            Some(parent_generation) if commit.generation <= parent_generation => {
                return Err(Reject::Chronology);
            }
            Some(parent_generation) if commit.parents.len() == 1 => {
                if commit.generation
                    != parent_generation.checked_add(1).ok_or(Reject::Chronology)?
                {
                    return Err(Reject::Chronology);
                }
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

fn first_parent_contains(
    history: &History,
    descendant_id: &str,
    ancestor_id: &str,
) -> Result<bool, Reject> {
    let mut current_id = descendant_id.to_string();
    loop {
        if current_id == ancestor_id {
            return Ok(true);
        }
        let current = history
            .commits
            .get(&current_id)
            .ok_or(Reject::MissingCommit)?;
        match current.parents.as_slice() {
            [parent] => current_id = parent.clone(),
            [] | [_, ..] => return Ok(false),
        }
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
        history.commits.get(floor_id).ok_or(Reject::MissingCommit)?;
        if !first_parent_contains(history, target_id, floor_id)?
            || !first_parent_contains(history, &parent_id, floor_id)?
        {
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
        if let Some(undo_target) = &self.history.undo_target {
            self.history
                .commits
                .get(undo_target)
                .ok_or(Reject::MissingCommit)?;
            if undo_target != &self.history.head {
                return Err(Reject::CursorMismatch);
            }
        }
        if self.history.redo_cursor.is_some() != self.history.redo_target.is_some() {
            return Err(Reject::CursorMismatch);
        }
        if let Some(redo_cursor) = &self.history.redo_cursor {
            if redo_cursor != &self.history.head {
                return Err(Reject::CursorMismatch);
            }
        }
        if let Some(redo_target) = &self.history.redo_target {
            let target = self
                .history
                .commits
                .get(redo_target)
                .ok_or(Reject::MissingCommit)?;
            if target.parents.as_slice() != [self.history.head.clone()] {
                return Err(Reject::CursorMismatch);
            }
        }
        if let Some(floor_id) = &self.history.checkpoint_floor {
            self.history
                .commits
                .get(floor_id)
                .ok_or(Reject::MissingCommit)?;
            if !first_parent_contains(&self.history, &self.history.head, floor_id)? {
                return Err(Reject::CheckpointFloor);
            }
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

fn forked_floor_history() -> History {
    let root = commit("A", 0, &[], &[row("x", 1, Value::Null)]);
    let active_parent = commit("B", 1, &["A"], &[row("x", 2, Value::Text("b".into()))]);
    let sibling_floor = commit("F", 1, &["A"], &[row("x", 2, Value::Text("f".into()))]);
    let active_head = commit("D", 2, &["B"], &[row("x", 3, Value::Text("d".into()))]);
    History {
        commits: BTreeMap::from([
            (root.id.clone(), root),
            (active_parent.id.clone(), active_parent),
            (sibling_floor.id.clone(), sibling_floor),
            (active_head.id.clone(), active_head),
        ]),
        head: "D".into(),
        undo_target: Some("D".into()),
        redo_cursor: None,
        redo_target: None,
        checkpoint_floor: Some("F".into()),
    }
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
fn sibling_and_non_ancestor_checkpoint_floors_fail_closed() {
    let mut sibling = forked_floor_history();
    assert_eq!(
        Repository::new(sibling.clone()).apply_undo(read()),
        Err(Reject::CheckpointFloor)
    );

    let non_ancestor = commit("G", 2, &["F"], &[row("x", 3, Value::Text("g".into()))]);
    sibling
        .commits
        .insert(non_ancestor.id.clone(), non_ancestor);
    sibling.checkpoint_floor = Some("G".into());
    assert_eq!(
        Repository::new(sibling).apply_undo(read()),
        Err(Reject::CheckpointFloor)
    );
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
fn cold_reopen_reauthenticates_cursor_floor_and_commit_identity() {
    let mut missing_redo = Repository::new(linear_history());
    missing_redo.apply_undo(read()).expect("undo");
    missing_redo.history.redo_target = Some("missing".into());
    assert_eq!(missing_redo.cold_reopen(), Err(Reject::MissingCommit));

    let mut missing_floor = Repository::new(linear_history());
    missing_floor.history.checkpoint_floor = Some("missing".into());
    assert_eq!(missing_floor.cold_reopen(), Err(Reject::MissingCommit));

    let mut mismatched_cursor = Repository::new(linear_history());
    mismatched_cursor.apply_undo(read()).expect("undo");
    mismatched_cursor.history.redo_cursor = Some("B".into());
    assert_eq!(mismatched_cursor.cold_reopen(), Err(Reject::CursorMismatch));

    let mut forged_commit = Repository::new(linear_history());
    forged_commit.history.commits.get_mut("B").expect("B").id = "forged".into();
    assert_eq!(forged_commit.cold_reopen(), Err(Reject::IdentityMismatch));
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

    let mut fake = read();
    fake.calls[0].operation = "unrelated_operation".into();
    assert_eq!(undo_plan(&history, fake), Err(Reject::ReadView));
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

// The following model is deliberately separate from the compact history model
// above. It represents the authenticated object envelope and the mutable
// publication boundary that a production candidate must preserve. Object IDs
// and content hashes are a deterministic model hash, not a production codec.
const AUTH_DOMAIN: u64 = 0x5741_4243_3341_5554;

#[derive(Clone, Debug, Eq, PartialEq)]
enum AuthKind {
    Commit,
    Marker,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthCommitPayload {
    logical_id: String,
    generation: u64,
    parents: Vec<u64>,
    rows: BTreeMap<String, Row>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthMarkerPayload {
    owner: String,
    head: u64,
    undo_target: u64,
    redo_cursor: Option<u64>,
    redo_target: Option<u64>,
    checkpoint_floor: Option<u64>,
    generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AuthPayload {
    Commit(AuthCommitPayload),
    Marker(AuthMarkerPayload),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthObject {
    object_id: u64,
    domain: u64,
    kind: AuthKind,
    payload: AuthPayload,
    content_hash: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthStore {
    objects: BTreeMap<u64, AuthObject>,
    selector_head: u64,
    marker: u64,
    selector_generation: u64,
}

fn model_digest<T: std::fmt::Debug>(value: &T) -> u64 {
    let bytes = format!("{value:?}").into_bytes();
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        hash.rotate_left(5) ^ u64::from(*byte).wrapping_add(0x9e3779b97f4a7c15)
    })
}

fn seal_auth_object(domain: u64, kind: AuthKind, payload: AuthPayload) -> AuthObject {
    let object_id = model_digest(&(domain, &kind, &payload));
    let content_hash = model_digest(&(domain, &kind, &payload, object_id));
    AuthObject {
        object_id,
        domain,
        kind,
        payload,
        content_hash,
    }
}

fn auth_commit(object: &AuthObject) -> Result<&AuthCommitPayload, Reject> {
    if object.kind != AuthKind::Commit {
        return Err(Reject::CorruptEnvelope);
    }
    match &object.payload {
        AuthPayload::Commit(payload) => Ok(payload),
        AuthPayload::Marker(_) => Err(Reject::CorruptEnvelope),
    }
}

fn auth_marker(object: &AuthObject) -> Result<&AuthMarkerPayload, Reject> {
    if object.kind != AuthKind::Marker {
        return Err(Reject::CorruptEnvelope);
    }
    match &object.payload {
        AuthPayload::Marker(payload) => Ok(payload),
        AuthPayload::Commit(_) => Err(Reject::CorruptEnvelope),
    }
}

fn validate_auth_object(map_id: u64, object: &AuthObject) -> Result<(), Reject> {
    if map_id != object.object_id
        || object.domain != AUTH_DOMAIN
        || object.content_hash
            != model_digest(&(
                object.domain,
                &object.kind,
                &object.payload,
                object.object_id,
            ))
    {
        return Err(Reject::CorruptEnvelope);
    }
    match (&object.kind, &object.payload) {
        (AuthKind::Commit, AuthPayload::Commit(_)) | (AuthKind::Marker, AuthPayload::Marker(_)) => {
            Ok(())
        }
        _ => Err(Reject::CorruptEnvelope),
    }
}

fn auth_parent(store: &AuthStore, id: u64) -> Result<&AuthCommitPayload, Reject> {
    let object = store.objects.get(&id).ok_or(Reject::MissingCommit)?;
    auth_commit(object)
}

fn validate_first_parent_chain(
    store: &AuthStore,
    descendant: u64,
    ancestor: u64,
    seen: &mut BTreeSet<u64>,
) -> Result<bool, Reject> {
    if descendant == ancestor {
        return Ok(true);
    }
    if !seen.insert(descendant) {
        return Err(Reject::Chronology);
    }
    let commit = auth_parent(store, descendant)?;
    match commit.parents.as_slice() {
        [parent] => validate_first_parent_chain(store, *parent, ancestor, seen),
        [] | [_, ..] => Ok(false),
    }
}

fn validate_auth_store(store: &AuthStore) -> Result<(), Reject> {
    let head = auth_parent(store, store.selector_head)?;
    for (map_id, object) in &store.objects {
        validate_auth_object(*map_id, object)?;
        if let AuthPayload::Commit(payload) = &object.payload {
            let mut parents = BTreeSet::new();
            let mut maximum_generation = None;
            for parent_id in &payload.parents {
                if !parents.insert(*parent_id) {
                    return Err(Reject::Chronology);
                }
                let parent = auth_parent(store, *parent_id)?;
                maximum_generation = Some(
                    maximum_generation.map_or(parent.generation, |current: u64| {
                        current.max(parent.generation)
                    }),
                );
            }
            match maximum_generation {
                None if payload.generation != 0 => return Err(Reject::Chronology),
                Some(parent_generation) if payload.generation <= parent_generation => {
                    return Err(Reject::Chronology);
                }
                Some(parent_generation) if payload.parents.len() == 1 => {
                    if payload.generation != parent_generation + 1 {
                        return Err(Reject::Chronology);
                    }
                }
                _ => {}
            }
        }
    }
    let marker_object = store
        .objects
        .get(&store.marker)
        .ok_or(Reject::MissingCommit)?;
    let marker = auth_marker(marker_object)?;
    if marker.owner != "undo-redo"
        || marker.head != store.selector_head
        || marker.undo_target != store.selector_head
        || marker.generation != head.generation
        || marker.generation != store.selector_generation
    {
        return Err(Reject::CorruptEnvelope);
    }
    for id in [
        marker.redo_cursor,
        marker.redo_target,
        marker.checkpoint_floor,
    ]
    .into_iter()
    .flatten()
    {
        auth_parent(store, id)?;
    }
    if let Some(cursor) = marker.redo_cursor {
        if cursor != store.selector_head {
            return Err(Reject::CursorMismatch);
        }
    }
    if let Some(target) = marker.redo_target {
        let target_payload = auth_parent(store, target)?;
        if target_payload.parents.as_slice() != [store.selector_head] {
            return Err(Reject::CursorMismatch);
        }
    }
    if let Some(floor) = marker.checkpoint_floor {
        if !validate_first_parent_chain(store, store.selector_head, floor, &mut BTreeSet::new())? {
            return Err(Reject::CheckpointFloor);
        }
    }
    Ok(())
}

fn authenticated_linear_store() -> AuthStore {
    let root = seal_auth_object(
        AUTH_DOMAIN,
        AuthKind::Commit,
        AuthPayload::Commit(AuthCommitPayload {
            logical_id: "A".into(),
            generation: 0,
            parents: Vec::new(),
            rows: BTreeMap::from([(String::from("x"), row("x", 1, Value::Null))]),
        }),
    );
    let target = seal_auth_object(
        AUTH_DOMAIN,
        AuthKind::Commit,
        AuthPayload::Commit(AuthCommitPayload {
            logical_id: "B".into(),
            generation: 1,
            parents: vec![root.object_id],
            rows: BTreeMap::from([(String::from("x"), row("x", 2, Value::Text("new".into())))]),
        }),
    );
    let marker = seal_auth_object(
        AUTH_DOMAIN,
        AuthKind::Marker,
        AuthPayload::Marker(AuthMarkerPayload {
            owner: "undo-redo".into(),
            head: target.object_id,
            undo_target: target.object_id,
            redo_cursor: None,
            redo_target: None,
            checkpoint_floor: Some(root.object_id),
            generation: 1,
        }),
    );
    AuthStore {
        objects: BTreeMap::from([
            (root.object_id, root),
            (target.object_id, target),
            (marker.object_id, marker.clone()),
        ]),
        selector_head: marker
            .payload
            .clone()
            .into_marker_head()
            .expect("marker payload"),
        marker: marker.object_id,
        selector_generation: 1,
    }
}

trait MarkerHead {
    fn into_marker_head(self) -> Result<u64, Reject>;
}

impl MarkerHead for AuthPayload {
    fn into_marker_head(self) -> Result<u64, Reject> {
        match self {
            AuthPayload::Marker(payload) => Ok(payload.head),
            AuthPayload::Commit(_) => Err(Reject::CorruptEnvelope),
        }
    }
}

fn cold_reopen_authenticated(store: &AuthStore) -> Result<AuthStore, Reject> {
    validate_auth_store(store)?;
    Ok(store.clone())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedPublication {
    expected_head: String,
    desired_head: String,
    expected_receipt: u64,
    plan: TransitionPlan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AtomicTransitionState {
    head: String,
    rows: BTreeMap<String, Row>,
    receipt: u64,
    prepared_publications: u64,
    plans: u64,
    atomic_commits: u64,
}

fn prepare_publication(
    state: &AtomicTransitionState,
    request: TransitionRequest,
) -> Result<PreparedPublication, Reject> {
    let plan = execute_transition(request)?;
    if plan.current_head != state.head {
        return Err(Reject::StaleHead);
    }
    Ok(PreparedPublication {
        expected_head: state.head.clone(),
        desired_head: plan.desired_head.clone(),
        expected_receipt: state.receipt,
        plan,
    })
}

fn apply_prepared_publication(
    state: &mut AtomicTransitionState,
    prepared: PreparedPublication,
    fail_after_row: Option<usize>,
) -> Result<(), Reject> {
    if state.head != prepared.expected_head || state.receipt != prepared.expected_receipt {
        return Err(Reject::StaleHead);
    }
    let mut staged_rows = state.rows.clone();
    for (index, row) in prepared.plan.rows.iter().enumerate() {
        if fail_after_row == Some(index) {
            return Err(Reject::InjectedFailure);
        }
        match &row.target_value {
            Some(value) => {
                staged_rows.insert(
                    row.identity.clone(),
                    Row {
                        identity: row.identity.clone(),
                        change_id: row.target_change_id.ok_or(Reject::CorruptEnvelope)?,
                        value: value.clone(),
                    },
                );
            }
            None => {
                staged_rows.remove(&row.identity);
            }
        }
    }
    state.rows = staged_rows;
    state.head = prepared.desired_head;
    state.receipt += 1;
    state.prepared_publications += 1;
    state.plans += 1;
    state.atomic_commits += 1;
    Ok(())
}

#[test]
fn authenticated_envelopes_reject_substitution_kind_and_topology_corruption() {
    let baseline = authenticated_linear_store();
    assert_eq!(cold_reopen_authenticated(&baseline), Ok(baseline.clone()));

    let mut same_key = baseline.clone();
    let target_id = same_key
        .objects
        .values()
        .find_map(|object| match &object.payload {
            AuthPayload::Commit(payload) if payload.logical_id == "B" => Some(object.object_id),
            _ => None,
        })
        .expect("target");
    let target = same_key.objects.get_mut(&target_id).expect("target object");
    if let AuthPayload::Commit(payload) = &mut target.payload {
        payload.rows.insert(
            "x".into(),
            row("x", 2, Value::Text("same-size forged".into())),
        );
    }
    assert_eq!(
        cold_reopen_authenticated(&same_key),
        Err(Reject::CorruptEnvelope)
    );

    let mut wrong_kind = baseline.clone();
    let marker_id = wrong_kind.marker;
    wrong_kind.objects.get_mut(&marker_id).expect("marker").kind = AuthKind::Commit;
    assert_eq!(
        cold_reopen_authenticated(&wrong_kind),
        Err(Reject::CorruptEnvelope)
    );

    let mut missing = baseline.clone();
    missing.objects.remove(&missing.marker);
    assert_eq!(
        cold_reopen_authenticated(&missing),
        Err(Reject::MissingCommit)
    );

    let mut reordered = baseline.clone();
    let commit_id = reordered.selector_head;
    let commit = reordered.objects.get_mut(&commit_id).expect("head");
    if let AuthPayload::Commit(payload) = &mut commit.payload {
        let parent = payload.parents[0];
        payload.parents.push(parent);
    }
    assert_eq!(
        cold_reopen_authenticated(&reordered),
        Err(Reject::CorruptEnvelope)
    );
}

#[test]
fn marker_cursor_head_floor_and_generation_are_authenticated_on_reopen() {
    let baseline = authenticated_linear_store();
    let mut forged_marker = baseline.clone();
    let marker = forged_marker
        .objects
        .get_mut(&forged_marker.marker)
        .expect("marker");
    if let AuthPayload::Marker(payload) = &mut marker.payload {
        payload.redo_cursor = Some(payload.head);
        payload.redo_target = Some(payload.head);
    }
    assert_eq!(
        cold_reopen_authenticated(&forged_marker),
        Err(Reject::CorruptEnvelope)
    );

    let mut forged_generation = baseline.clone();
    forged_generation.selector_generation = 9;
    assert_eq!(
        cold_reopen_authenticated(&forged_generation),
        Err(Reject::CorruptEnvelope)
    );
}

fn atomic_request(state: &AtomicTransitionState) -> TransitionRequest {
    let mut desired = state.rows.clone();
    desired.insert("x".into(), row("x", 2, Value::Text("updated".into())));
    desired.remove("y");
    TransitionRequest {
        observed_head: state.head.clone(),
        expected_head: state.head.clone(),
        desired_head: "next".into(),
        keys: vec!["x".into(), "y".into()],
        current_rows: state.rows.clone(),
        desired_rows: desired,
        read: read(),
        staged_writes: false,
    }
}

#[test]
fn typed_transition_failure_after_partial_staging_rolls_back_everything() {
    let state = AtomicTransitionState {
        head: "current".into(),
        rows: BTreeMap::from([
            (String::from("x"), row("x", 1, Value::Null)),
            (String::from("y"), row("y", 2, Value::Text("old".into()))),
        ]),
        receipt: 41,
        prepared_publications: 0,
        plans: 0,
        atomic_commits: 0,
    };
    let before = state.clone();
    let prepared = prepare_publication(&state, atomic_request(&state)).expect("prepare");
    let mut failed = state.clone();
    assert_eq!(
        apply_prepared_publication(&mut failed, prepared, Some(1)),
        Err(Reject::InjectedFailure)
    );
    assert_eq!(failed, before);
}

#[test]
fn typed_transition_success_is_one_prepared_plan_and_atomic_commit() {
    let mut state = AtomicTransitionState {
        head: "current".into(),
        rows: BTreeMap::from([
            (String::from("x"), row("x", 1, Value::Null)),
            (String::from("y"), row("y", 2, Value::Text("old".into()))),
        ]),
        receipt: 41,
        prepared_publications: 0,
        plans: 0,
        atomic_commits: 0,
    };
    let prepared = prepare_publication(&state, atomic_request(&state)).expect("prepare");
    apply_prepared_publication(&mut state, prepared, None).expect("one commit");
    assert_eq!(state.head, "next");
    assert_eq!(state.receipt, 42);
    assert_eq!(state.rows["x"].value, Value::Text("updated".into()));
    assert!(!state.rows.contains_key("y"));
    assert_eq!(state.prepared_publications, 1);
    assert_eq!(state.plans, 1);
    assert_eq!(state.atomic_commits, 1);
}
