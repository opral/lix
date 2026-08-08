//! W1b-1 standalone semantic model.
//!
//! Test/report-only: it does not import Lix, open storage, or claim runtime
//! qualification. Future review may compile it with rustc --edition=2024 --test.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Cell {
    Absent,
    Null,
    Tombstone,
    Blob { owner: String, bytes: Vec<u8> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Authority {
    Authenticated { commit: String, generation: u64 },
    Missing,
    Malformed,
    WrongKind,
    IdentitySubstituted { claimed: String, actual: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Commit {
    id: String,
    generation: u64,
    parent: Option<String>,
    authority: Authority,
    rows: BTreeMap<String, Cell>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReadTrace {
    view_id: u64,
    begin_reads: u32,
    events: Vec<(u64, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Request {
    base: Commit,
    source: Commit,
    target: Commit,
    trace: ReadTrace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Decision {
    Unchanged,
    TakeSource,
    TakeTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PlannedRow {
    entity: String,
    decision: Decision,
    value: Cell,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Reject {
    Authority,
    Generation,
    Parent,
    ReadView,
    Conflict(String),
}

fn validate_authority(commit: &Commit) -> Result<(), Reject> {
    match &commit.authority {
        Authority::Authenticated { commit: id, generation }
            if id == &commit.id && *generation == commit.generation => Ok(()),
        _ => Err(Reject::Authority),
    }
}

fn merge(request: &Request) -> Result<Vec<PlannedRow>, Reject> {
    for commit in [&request.base, &request.source, &request.target] {
        validate_authority(commit)?;
    }
    if request.source.generation <= request.base.generation
        || request.target.generation <= request.base.generation
    {
        return Err(Reject::Generation);
    }
    if request.source.parent.as_deref() != Some(request.base.id.as_str())
        || request.target.parent.as_deref() != Some(request.base.id.as_str())
    {
        return Err(Reject::Parent);
    }
    if request.trace.begin_reads != 1
        || request.trace.events.iter().any(|(view, _)| *view != request.trace.view_id)
    {
        return Err(Reject::ReadView);
    }

    let mut entities = BTreeSet::new();
    entities.extend(request.base.rows.keys().cloned());
    entities.extend(request.source.rows.keys().cloned());
    entities.extend(request.target.rows.keys().cloned());

    let mut plan = Vec::new();
    for entity in entities {
        let base = request.base.rows.get(&entity).cloned().unwrap_or(Cell::Absent);
        let source = request.source.rows.get(&entity).cloned().unwrap_or(Cell::Absent);
        let target = request.target.rows.get(&entity).cloned().unwrap_or(Cell::Absent);
        let source_changed = source != base;
        let target_changed = target != base;
        let (decision, value) = match (source_changed, target_changed, source == target) {
            (false, false, _) => (Decision::Unchanged, base),
            (true, false, _) => (Decision::TakeSource, source),
            (false, true, _) => (Decision::TakeTarget, target),
            (true, true, true) => (Decision::TakeSource, source),
            (true, true, false) => return Err(Reject::Conflict(entity)),
        };
        plan.push(PlannedRow { entity, decision, value });
    }
    Ok(plan)
}

fn commit(id: &str, generation: u64, parent: &str, rows: &[(&str, Cell)]) -> Commit {
    Commit {
        id: id.to_owned(),
        generation,
        parent: Some(parent.to_owned()),
        authority: Authority::Authenticated { commit: id.to_owned(), generation },
        rows: rows.iter().map(|(key, value)| ((*key).to_owned(), value.clone())).collect(),
    }
}

fn request(base: Commit, source: Commit, target: Commit) -> Request {
    Request {
        base,
        source,
        target,
        trace: ReadTrace {
            view_id: 7,
            begin_reads: 1,
            events: vec![(7, "catalog/topology/state/member".to_owned())],
        },
    }
}

#[test]
fn disjoint_changes_are_sorted_and_successful() {
    let base = commit("B", 1, "ROOT", &[("a", Cell::Null), ("b", Cell::Absent)]);
    let source = commit(
        "S",
        2,
        "B",
        &[
            ("a", Cell::Blob { owner: "plugin-a".into(), bytes: vec![1] }),
            ("b", Cell::Absent),
        ],
    );
    let target = commit("T", 2, "B", &[("a", Cell::Null), ("b", Cell::Tombstone)]);
    let plan = merge(&request(base, source, target)).expect("disjoint merge");
    assert_eq!(
        plan.iter().map(|row| row.entity.as_str()).collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert_eq!(plan[0].decision, Decision::TakeSource);
    assert_eq!(plan[1].decision, Decision::TakeTarget);
}

#[test]
fn convergent_equal_values_are_not_conflicts_and_states_are_distinct() {
    let base = commit("B", 1, "ROOT", &[("x", Cell::Null)]);
    let source = commit("S", 2, "B", &[("x", Cell::Tombstone)]);
    let target = commit("T", 2, "B", &[("x", Cell::Tombstone)]);
    let plan = merge(&request(base, source, target)).expect("convergent update");
    assert_eq!(plan[0].value, Cell::Tombstone);
    assert_ne!(Cell::Absent, Cell::Null);
    assert_ne!(Cell::Null, Cell::Tombstone);
    assert_ne!(
        Cell::Tombstone,
        Cell::Blob { owner: "plugin-a".into(), bytes: vec![] }
    );
}

#[test]
fn conflicting_same_entity_fails_before_projection() {
    let base = commit("B", 1, "ROOT", &[("x", Cell::Null)]);
    let source = commit(
        "S",
        2,
        "B",
        &[("x", Cell::Blob { owner: "plugin-a".into(), bytes: vec![1] })],
    );
    let target = commit(
        "T",
        2,
        "B",
        &[("x", Cell::Blob { owner: "plugin-b".into(), bytes: vec![2] })],
    );
    assert_eq!(
        merge(&request(base, source, target)),
        Err(Reject::Conflict("x".into()))
    );
}

#[test]
fn authority_substitution_and_corrupt_read_views_fail_closed() {
    let base = commit("B", 1, "ROOT", &[]);
    let mut source = commit("S", 2, "B", &[("x", Cell::Null)]);
    source.authority = Authority::IdentitySubstituted {
        claimed: "S".into(),
        actual: "OTHER".into(),
    };
    let target = commit("T", 2, "B", &[]);
    assert_eq!(
        merge(&request(base.clone(), source, target.clone())),
        Err(Reject::Authority)
    );

    let mut bad_view = request(base, commit("S", 2, "B", &[]), target);
    bad_view.trace.begin_reads = 2;
    assert_eq!(merge(&bad_view), Err(Reject::ReadView));
}

#[test]
fn missing_malformed_and_wrong_kind_authority_fail_closed() {
    for authority in [Authority::Missing, Authority::Malformed, Authority::WrongKind] {
        let base = commit("B", 1, "ROOT", &[]);
        let mut source = commit("S", 2, "B", &[]);
        source.authority = authority;
        let target = commit("T", 2, "B", &[]);
        assert_eq!(
            merge(&request(base, source, target)),
            Err(Reject::Authority)
        );
    }
}
