//! W1b-1 standalone correction model.
//!
//! Test/report-only. It does not import Lix, open storage, or qualify an
//! adapter. Compile with rustc --edition=2024 --test -D warnings.

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
struct OwnerRow {
    entity: String,
    owner_commit: String,
    value: Cell,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Commit {
    id: String,
    generation: u64,
    parent: Option<String>,
    authority: Authority,
    rows: Vec<OwnerRow>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ForkTreeReadFacade {
    storage_read_id: u64,
    coherent_view_id: u64,
    operation_id: u64,
    begin_reads: u32,
    events: Vec<(u64, &'static str)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ConflictGroup {
    entity: String,
    source: Cell,
    target: Cell,
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
    owner_commit: String,
    decision: Decision,
    value: Cell,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MergeResult {
    base_commit_id: String,
    source_commit_id: String,
    target_commit_id: String,
    rows: Vec<PlannedRow>,
    conflict_groups: Vec<ConflictGroup>,
    result_digest: u64,
    publication_steps: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Reject {
    Authority,
    Generation,
    Parent,
    ReadView,
    DuplicateOwner(String),
    Conflict(String),
    Publication,
}

fn validate_authority(commit: &Commit) -> Result<(), Reject> {
    match &commit.authority {
        Authority::Authenticated {
            commit: id,
            generation,
        } if id == &commit.id && *generation == commit.generation => Ok(()),
        _ => Err(Reject::Authority),
    }
}

fn owner_map(commit: &Commit) -> Result<BTreeMap<String, Cell>, Reject> {
    let mut rows = BTreeMap::new();
    for row in &commit.rows {
        if row.owner_commit != commit.id {
            return Err(Reject::Authority);
        }
        if rows.insert(row.entity.clone(), row.value.clone()).is_some() {
            return Err(Reject::DuplicateOwner(row.entity.clone()));
        }
    }
    Ok(rows)
}

fn digest(result: &MergeResult) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    let mut feed = |bytes: &[u8]| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    };
    feed(result.base_commit_id.as_bytes());
    feed(result.source_commit_id.as_bytes());
    feed(result.target_commit_id.as_bytes());
    for row in &result.rows {
        feed(row.entity.as_bytes());
        feed(row.owner_commit.as_bytes());
        feed(match row.decision {
            Decision::Unchanged => b"unchanged",
            Decision::TakeSource => b"source",
            Decision::TakeTarget => b"target",
        });
        feed(format!("{:?}", row.value).as_bytes());
    }
    hash
}

fn merge(
    view: &ForkTreeReadFacade,
    base: &Commit,
    source: &Commit,
    target: &Commit,
) -> Result<MergeResult, Reject> {
    if view.begin_reads != 1
        || view.storage_read_id != view.coherent_view_id
        || view.coherent_view_id != view.operation_id
        || view
            .events
            .iter()
            .any(|(id, _)| *id != view.coherent_view_id)
    {
        return Err(Reject::ReadView);
    }
    for commit in [base, source, target] {
        validate_authority(commit)?;
    }
    if source.generation <= base.generation || target.generation <= base.generation {
        return Err(Reject::Generation);
    }
    if source.parent.as_deref() != Some(base.id.as_str())
        || target.parent.as_deref() != Some(base.id.as_str())
    {
        return Err(Reject::Parent);
    }
    let base_rows = owner_map(base)?;
    let source_rows = owner_map(source)?;
    let target_rows = owner_map(target)?;
    let mut entities = BTreeSet::new();
    entities.extend(base_rows.keys().cloned());
    entities.extend(source_rows.keys().cloned());
    entities.extend(target_rows.keys().cloned());

    let mut rows = Vec::new();
    let mut conflict_groups = Vec::new();
    for entity in entities {
        let base_value = base_rows.get(&entity).cloned().unwrap_or(Cell::Absent);
        let source_value = source_rows.get(&entity).cloned().unwrap_or(Cell::Absent);
        let target_value = target_rows.get(&entity).cloned().unwrap_or(Cell::Absent);
        let source_changed = source_value != base_value;
        let target_changed = target_value != base_value;
        let (decision, value) = match (source_changed, target_changed, source_value == target_value)
        {
            (false, false, _) => (Decision::Unchanged, base_value),
            (true, false, _) => (Decision::TakeSource, source_value),
            (false, true, _) => (Decision::TakeTarget, target_value),
            (true, true, true) => (Decision::TakeSource, source_value),
            (true, true, false) => {
                conflict_groups.push(ConflictGroup {
                    entity: entity.clone(),
                    source: source_value,
                    target: target_value,
                });
                return Err(Reject::Conflict(entity));
            }
        };
        rows.push(PlannedRow {
            entity,
            owner_commit: match decision {
                Decision::TakeSource => source.id.clone(),
                Decision::TakeTarget => target.id.clone(),
                Decision::Unchanged => base.id.clone(),
            },
            decision,
            value,
        });
    }
    let mut result = MergeResult {
        base_commit_id: base.id.clone(),
        source_commit_id: source.id.clone(),
        target_commit_id: target.id.clone(),
        rows,
        conflict_groups,
        result_digest: 0,
        publication_steps: 0,
    };
    result.result_digest = digest(&result);
    if result.publication_steps != 0 {
        return Err(Reject::Publication);
    }
    Ok(result)
}

fn commit(id: &str, generation: u64, parent: &str, rows: &[(&str, Cell)]) -> Commit {
    Commit {
        id: id.to_owned(),
        generation,
        parent: Some(parent.to_owned()),
        authority: Authority::Authenticated {
            commit: id.to_owned(),
            generation,
        },
        rows: rows
            .iter()
            .map(|(entity, value)| OwnerRow {
                entity: (*entity).to_owned(),
                owner_commit: id.to_owned(),
                value: value.clone(),
            })
            .collect(),
    }
}

fn view() -> ForkTreeReadFacade {
    ForkTreeReadFacade {
        storage_read_id: 7,
        coherent_view_id: 7,
        operation_id: 7,
        begin_reads: 1,
        events: vec![(7, "catalog/topology/state/owner-row")],
    }
}

#[test]
fn disjoint_owner_rows_are_sorted_and_digest_is_stable() {
    let base = commit("B", 1, "ROOT", &[("a", Cell::Null), ("b", Cell::Absent)]);
    let source = commit(
        "S",
        2,
        "B",
        &[
            (
                "a",
                Cell::Blob {
                    owner: "plugin-a".into(),
                    bytes: vec![1],
                },
            ),
            ("b", Cell::Absent),
        ],
    );
    let target = commit("T", 2, "B", &[("a", Cell::Null), ("b", Cell::Tombstone)]);
    let result = merge(&view(), &base, &source, &target).expect("disjoint merge");
    assert_eq!(result.base_commit_id, "B");
    assert_eq!(result.source_commit_id, "S");
    assert_eq!(result.target_commit_id, "T");
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.entity.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert!(result.conflict_groups.is_empty());
    assert_eq!(result.publication_steps, 0);
    assert_eq!(result.result_digest, digest(&result));
    assert_ne!(result.result_digest, 0);
}

#[test]
fn convergent_equal_values_preserve_distinct_cell_states() {
    let base = commit("B", 1, "ROOT", &[("x", Cell::Null)]);
    let source = commit("S", 2, "B", &[("x", Cell::Tombstone)]);
    let target = commit("T", 2, "B", &[("x", Cell::Tombstone)]);
    let result = merge(&view(), &base, &source, &target).expect("convergent update");
    assert_eq!(result.rows[0].value, Cell::Tombstone);
    assert_ne!(Cell::Absent, Cell::Null);
    assert_ne!(Cell::Null, Cell::Tombstone);
    assert_ne!(
        Cell::Tombstone,
        Cell::Blob {
            owner: "plugin-a".into(),
            bytes: vec![]
        }
    );
}

#[test]
fn conflict_group_rejects_before_partial_output_or_publication() {
    let base = commit("B", 1, "ROOT", &[("x", Cell::Null)]);
    let source = commit(
        "S",
        2,
        "B",
        &[(
            "x",
            Cell::Blob {
                owner: "a".into(),
                bytes: vec![1],
            },
        )],
    );
    let target = commit(
        "T",
        2,
        "B",
        &[(
            "x",
            Cell::Blob {
                owner: "b".into(),
                bytes: vec![2],
            },
        )],
    );
    assert_eq!(
        merge(&view(), &base, &source, &target),
        Err(Reject::Conflict("x".into()))
    );
}

#[test]
fn missing_malformed_wrong_kind_and_identity_fail_closed() {
    for authority in [
        Authority::Missing,
        Authority::Malformed,
        Authority::WrongKind,
        Authority::IdentitySubstituted {
            claimed: "S".into(),
            actual: "OTHER".into(),
        },
    ] {
        let base = commit("B", 1, "ROOT", &[]);
        let mut source = commit("S", 2, "B", &[]);
        source.authority = authority;
        let target = commit("T", 2, "B", &[]);
        assert_eq!(
            merge(&view(), &base, &source, &target),
            Err(Reject::Authority)
        );
    }
}

#[test]
fn duplicate_owner_and_bad_view_fail_before_result() {
    let base = commit("B", 1, "ROOT", &[]);
    let mut source = commit("S", 2, "B", &[("x", Cell::Null)]);
    source.rows.push(OwnerRow {
        entity: "x".into(),
        owner_commit: "S".into(),
        value: Cell::Null,
    });
    let target = commit("T", 2, "B", &[]);
    assert_eq!(
        merge(&view(), &base, &source, &target),
        Err(Reject::DuplicateOwner("x".into()))
    );

    let mut bad_view = view();
    bad_view.begin_reads = 2;
    let source = commit("S", 2, "B", &[]);
    assert_eq!(
        merge(&bad_view, &base, &source, &target),
        Err(Reject::ReadView)
    );
}

#[test]
fn publication_chain_is_outside_reader_slice() {
    let base = commit("B", 1, "ROOT", &[]);
    let source = commit("S", 2, "B", &[]);
    let target = commit("T", 2, "B", &[]);
    let result = merge(&view(), &base, &source, &target).expect("reader result");
    assert_eq!(result.publication_steps, 0);
}
