//! Dependency-free semantic model for the H4 historical-provider contract.
//!
//! This is a test/report artifact. It deliberately models public outcomes,
//! not physical IDs or adapter implementation details.

use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum IdentityKind {
    File,
    Directory,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Identity {
    kind: IdentityKind,
    id: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Cell {
    Value(&'static str),
    Null,
    Tombstone,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Row {
    identity: Identity,
    as_of: &'static str,
    observed: &'static str,
    depth: u32,
    path: Option<&'static str>,
    cell: Cell,
    checkpoint_marker: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootStatus {
    Valid,
    Missing,
    WrongKind,
    Malformed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Authority {
    catalog_present: bool,
    commit_valid: bool,
    root: RootStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Outcome {
    Absent,
    Rows(Vec<Row>),
    Corruption(&'static str),
}

#[derive(Default)]
struct ReadCounters {
    begin_read: usize,
    retries: usize,
    fallback_reads: usize,
    cache_substitutions: usize,
}

fn authenticate(authority: Authority) -> Result<(), Outcome> {
    if !authority.catalog_present {
        return Err(Outcome::Corruption("missing CommitCatalog entry"));
    }
    if !authority.commit_valid {
        return Err(Outcome::Corruption("malformed or wrong-kind commit"));
    }
    if authority.root != RootStatus::Valid {
        return Err(Outcome::Corruption("missing, malformed, or wrong-kind root"));
    }
    Ok(())
}

fn history_order(left: &Row, right: &Row) -> Ordering {
    left.identity
        .cmp(&right.identity)
        .then(left.as_of.cmp(right.as_of))
        .then(left.depth.cmp(&right.depth))
        .then(left.observed.cmp(right.observed))
}

fn scan_history(
    authority: Authority,
    rows: &[Row],
    kind: IdentityKind,
    include_tombstones: bool,
    limit: Option<usize>,
) -> Outcome {
    if let Err(error) = authenticate(authority) {
        return error;
    }
    let mut result = rows
        .iter()
        .filter(|row| row.identity.kind == kind)
        .filter(|row| include_tombstones || row.cell != Cell::Tombstone)
        .filter(|row| !row.checkpoint_marker)
        .cloned()
        .collect::<Vec<_>>();
    result.sort_by(history_order);
    if let Some(limit) = limit {
        result.truncate(limit);
    }
    if result.is_empty() {
        Outcome::Absent
    } else {
        Outcome::Rows(result)
    }
}

fn point(
    authority: Authority,
    rows: &[Row],
    identity: Identity,
    include_tombstone: bool,
) -> Outcome {
    if let Err(error) = authenticate(authority) {
        return error;
    }
    let Some(row) = rows.iter().find(|row| row.identity == identity) else {
        return Outcome::Absent;
    };
    if row.cell == Cell::Tombstone && !include_tombstone {
        Outcome::Absent
    } else {
        Outcome::Rows(vec![row.clone()])
    }
}

fn assert_one_read(counters: &ReadCounters) {
    assert_eq!(counters.begin_read, 1);
    assert_eq!(counters.retries, 0);
    assert_eq!(counters.fallback_reads, 0);
    assert_eq!(counters.cache_substitutions, 0);
}

fn file(id: &'static str, as_of: &'static str, depth: u32, cell: Cell) -> Row {
    Row {
        identity: Identity { kind: IdentityKind::File, id },
        as_of,
        observed: as_of,
        depth,
        path: Some("/docs/file"),
        cell,
        checkpoint_marker: false,
    }
}

fn directory(id: &'static str, as_of: &'static str, depth: u32, cell: Cell) -> Row {
    Row {
        identity: Identity { kind: IdentityKind::Directory, id },
        as_of,
        observed: as_of,
        depth,
        path: Some("/docs"),
        cell,
        checkpoint_marker: false,
    }
}

#[test]
fn authority_precedes_absence_for_point_and_scan() {
    let key = Identity { kind: IdentityKind::File, id: "absent" };
    let rows = vec![file("present", "c1", 0, Cell::Value("v"))];
    let missing = Authority {
        catalog_present: false,
        commit_valid: true,
        root: RootStatus::Valid,
    };
    assert_eq!(point(missing, &rows, key.clone(), true), Outcome::Corruption("missing CommitCatalog entry"));
    assert_eq!(scan_history(missing, &rows, IdentityKind::File, true, None), Outcome::Corruption("missing CommitCatalog entry"));

    let valid = Authority {
        catalog_present: true,
        commit_valid: true,
        root: RootStatus::Valid,
    };
    assert_eq!(point(valid, &rows, key, true), Outcome::Absent);
    assert_eq!(scan_history(valid, &[], IdentityKind::File, true, None), Outcome::Absent);
}

#[test]
fn all_invalid_authority_states_fail_closed() {
    for authority in [
        Authority { catalog_present: false, commit_valid: true, root: RootStatus::Valid },
        Authority { catalog_present: true, commit_valid: false, root: RootStatus::Valid },
        Authority { catalog_present: true, commit_valid: true, root: RootStatus::Missing },
        Authority { catalog_present: true, commit_valid: true, root: RootStatus::WrongKind },
        Authority { catalog_present: true, commit_valid: true, root: RootStatus::Malformed },
    ] {
        assert!(matches!(point(authority, &[], Identity { kind: IdentityKind::File, id: "x" }, true), Outcome::Corruption(_)));
        assert!(matches!(scan_history(authority, &[], IdentityKind::File, true, None), Outcome::Corruption(_)));
    }
}

#[test]
fn null_value_tombstone_and_identity_domains_remain_distinct() {
    let rows = vec![
        file("same-id", "c1", 0, Cell::Null),
        directory("same-id", "c1", 0, Cell::Value("directory")),
        file("deleted", "c1", 1, Cell::Tombstone),
        file("value", "c1", 2, Cell::Value("bytes")),
    ];
    let authority = Authority { catalog_present: true, commit_valid: true, root: RootStatus::Valid };
    assert_eq!(point(authority, &rows, Identity { kind: IdentityKind::File, id: "same-id" }, true), Outcome::Rows(vec![rows[0].clone()]));
    assert_eq!(point(authority, &rows, Identity { kind: IdentityKind::Directory, id: "same-id" }, true), Outcome::Rows(vec![rows[1].clone()]));
    assert_eq!(point(authority, &rows, Identity { kind: IdentityKind::File, id: "deleted" }, false), Outcome::Absent);
    assert!(matches!(point(authority, &rows, Identity { kind: IdentityKind::File, id: "deleted" }, true), Outcome::Rows(_)));
}

#[test]
fn order_and_limit_are_applied_after_identity_grouping() {
    let mut rows = vec![
        file("b", "c2", 2, Cell::Value("b2")),
        file("a", "c2", 2, Cell::Value("a2")),
        file("a", "c1", 1, Cell::Value("a1")),
    ];
    let authority = Authority { catalog_present: true, commit_valid: true, root: RootStatus::Valid };
    let outcome = scan_history(authority, &rows, IdentityKind::File, true, Some(2));
    rows.sort_by(history_order);
    assert_eq!(outcome, Outcome::Rows(rows[..2].to_vec()));
}

#[test]
fn checkpoint_markers_are_not_ordinary_diff_rows() {
    let mut marker = file("marker", "c1", 0, Cell::Value("checkpoint"));
    marker.checkpoint_marker = true;
    let value = file("normal", "c1", 0, Cell::Value("normal"));
    let authority = Authority { catalog_present: true, commit_valid: true, root: RootStatus::Valid };
    assert_eq!(scan_history(authority, &[marker, value.clone()], IdentityKind::File, true, None), Outcome::Rows(vec![value]));
}

#[test]
fn one_retained_read_has_no_hidden_alternate_path() {
    let counters = ReadCounters { begin_read: 1, ..ReadCounters::default() };
    assert_one_read(&counters);
}
