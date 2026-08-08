//! Test/report-only discriminator for the rejected 11442 direct entity reader.
//!
//! The model deliberately keeps the canonical read as one operation. Snapshot
//! and primary-key projections are terminal derivations from that one result;
//! neither projection is allowed to acquire a second view or raw storage read.

use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum TypedPk {
    Integer(i64),
    Text(String),
    Uuid(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Cell {
    Value(String),
    Null,
    Tombstone,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Domain {
    Tracked,
    Untracked,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Branch {
    Global,
    Local,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Row {
    pk: TypedPk,
    branch: Branch,
    domain: Domain,
    cell: Cell,
    sequence: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Request {
    exact_pks: Option<Vec<TypedPk>>,
    include_tombstones: bool,
    limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PublicRow {
    pk: TypedPk,
    cell: Cell,
}

fn fixture() -> Vec<Row> {
    vec![
        Row {
            pk: TypedPk::Text("a".into()),
            branch: Branch::Global,
            domain: Domain::Tracked,
            cell: Cell::Value("global-a".into()),
            sequence: 0,
        },
        Row {
            pk: TypedPk::Text("a".into()),
            branch: Branch::Local,
            domain: Domain::Tracked,
            cell: Cell::Value("branch-a".into()),
            sequence: 1,
        },
        // The canonical combined reader returns the untracked replacement
        // after the tracked row; the visibility resolver keeps that winner.
        Row {
            pk: TypedPk::Text("a".into()),
            branch: Branch::Local,
            domain: Domain::Untracked,
            cell: Cell::Value("untracked-a".into()),
            sequence: 2,
        },
        Row {
            pk: TypedPk::Integer(2),
            branch: Branch::Global,
            domain: Domain::Tracked,
            cell: Cell::Value("global-2".into()),
            sequence: 3,
        },
        Row {
            pk: TypedPk::Integer(2),
            branch: Branch::Local,
            domain: Domain::Untracked,
            cell: Cell::Value("untracked-2".into()),
            sequence: 4,
        },
        Row {
            pk: TypedPk::Uuid("01920000-0000-7000-8000-000000000001".into()),
            branch: Branch::Global,
            domain: Domain::Tracked,
            cell: Cell::Value("global-uuid".into()),
            sequence: 5,
        },
        Row {
            pk: TypedPk::Uuid("01920000-0000-7000-8000-000000000001".into()),
            branch: Branch::Local,
            domain: Domain::Tracked,
            cell: Cell::Null,
            sequence: 6,
        },
        Row {
            pk: TypedPk::Text("d".into()),
            branch: Branch::Global,
            domain: Domain::Tracked,
            cell: Cell::Value("global-d".into()),
            sequence: 7,
        },
        Row {
            pk: TypedPk::Text("d".into()),
            branch: Branch::Local,
            domain: Domain::Tracked,
            cell: Cell::Tombstone,
            sequence: 8,
        },
        Row {
            pk: TypedPk::Text("e".into()),
            branch: Branch::Global,
            domain: Domain::Untracked,
            cell: Cell::Value("global-untracked-e".into()),
            sequence: 9,
        },
        Row {
            pk: TypedPk::Text("f".into()),
            branch: Branch::Local,
            domain: Domain::Untracked,
            cell: Cell::Value("local-untracked-f".into()),
            sequence: 10,
        },
    ]
}

fn requested_branch(row: &Row) -> bool {
    matches!(row.branch, Branch::Global | Branch::Local)
}

fn canonical_rows(rows: &[Row], request: &Request) -> Vec<PublicRow> {
    let mut candidates = BTreeMap::<TypedPk, Vec<&Row>>::new();
    for row in rows.iter().filter(|row| requested_branch(row)) {
        if request
            .exact_pks
            .as_ref()
            .is_some_and(|pks| !pks.iter().any(|pk| pk == &row.pk))
        {
            continue;
        }
        candidates.entry(row.pk.clone()).or_default().push(row);
    }

    let mut output = Vec::new();
    for (pk, rows) in candidates {
        let local = rows
            .iter()
            .copied()
            .filter(|row| row.branch == Branch::Local)
            .collect::<Vec<_>>();
        let visible = if local.is_empty() {
            rows.iter()
                .copied()
                .filter(|row| row.branch == Branch::Global)
                .collect::<Vec<_>>()
        } else {
            local
        };
        let Some(row) = visible.into_iter().max_by_key(|row| row.sequence) else {
            continue;
        };
        if !request.include_tombstones && row.cell == Cell::Tombstone {
            continue;
        }
        output.push(PublicRow {
            pk,
            cell: row.cell.clone(),
        });
    }
    if let Some(limit) = request.limit {
        output.truncate(limit);
    }
    output
}

/// The rejected 11442 route models `ForkTreeReadFacade::scan_entity_rows`:
/// it scans only tracked global/local state roots and therefore cannot return
/// an untracked row or replacement.
fn rejected_direct_rows(rows: &[Row], request: &Request) -> Vec<PublicRow> {
    let tracked = rows
        .iter()
        .filter(|row| row.domain == Domain::Tracked)
        .cloned()
        .collect::<Vec<_>>();
    canonical_rows(&tracked, request)
}

trait LiveStateReader {
    fn scan_batch(&mut self, request: &Request) -> Vec<Row>;
}

struct CountingCanonicalReader {
    rows: Vec<Row>,
    calls: usize,
}

impl CountingCanonicalReader {
    fn new(rows: Vec<Row>) -> Self {
        Self { rows, calls: 0 }
    }
}

impl LiveStateReader for CountingCanonicalReader {
    fn scan_batch(&mut self, _request: &Request) -> Vec<Row> {
        self.calls += 1;
        self.rows.clone()
    }
}

fn terminal_snapshots<R: LiveStateReader>(reader: &mut R, request: &Request) -> Vec<PublicRow> {
    let rows = reader.scan_batch(request);
    canonical_rows(&rows, request)
}

fn terminal_primary_keys<R: LiveStateReader>(reader: &mut R, request: &Request) -> Vec<TypedPk> {
    let rows = reader.scan_batch(request);
    canonical_rows(&rows, request)
        .into_iter()
        .map(|row| row.pk)
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DomainSelector {
    Combined,
    Tracked,
    Untracked,
}

#[derive(Debug, Eq, PartialEq)]
enum ModelError {
    DuplicateAuthority,
    MalformedDomain,
}

fn decode_domain(byte: u8) -> Result<DomainSelector, ModelError> {
    match byte {
        0 => Ok(DomainSelector::Combined),
        1 => Ok(DomainSelector::Tracked),
        2 => Ok(DomainSelector::Untracked),
        _ => Err(ModelError::MalformedDomain),
    }
}

fn canonical_scan(
    rows: &[Row],
    selector: DomainSelector,
    request: &Request,
) -> Result<Vec<PublicRow>, ModelError> {
    let selected = rows
        .iter()
        .filter(|row| match selector {
            DomainSelector::Combined => true,
            DomainSelector::Tracked => row.domain == Domain::Tracked,
            DomainSelector::Untracked => row.domain == Domain::Untracked,
        })
        .cloned()
        .collect::<Vec<_>>();

    // A current scan must contain at most one row for each logical identity
    // within one authenticated stream. The same logical key in the tracked
    // and untracked streams is deliberately allowed: that is the replacement
    // and tombstone overlay tested below.
    let mut authorities = BTreeMap::new();
    for row in &selected {
        let key = (&row.pk, row.branch, row.domain);
        if authorities.insert(key, &row.cell).is_some() {
            return Err(ModelError::DuplicateAuthority);
        }
    }
    Ok(canonical_rows(&selected, request))
}

#[test]
fn rejected_direct_route_omits_mixed_domain_rows_and_replacements() {
    let rows = fixture();
    let request = Request {
        exact_pks: None,
        include_tombstones: false,
        limit: None,
    };

    let canonical = canonical_rows(&rows, &request);
    let rejected = rejected_direct_rows(&rows, &request);

    assert_eq!(
        canonical,
        vec![
            PublicRow {
                pk: TypedPk::Integer(2),
                cell: Cell::Value("untracked-2".into()),
            },
            PublicRow {
                pk: TypedPk::Text("a".into()),
                cell: Cell::Value("untracked-a".into()),
            },
            PublicRow {
                pk: TypedPk::Text("e".into()),
                cell: Cell::Value("global-untracked-e".into()),
            },
            PublicRow {
                pk: TypedPk::Text("f".into()),
                cell: Cell::Value("local-untracked-f".into()),
            },
            PublicRow {
                pk: TypedPk::Uuid("01920000-0000-7000-8000-000000000001".into()),
                cell: Cell::Null,
            },
        ]
    );
    assert_eq!(
        rejected,
        vec![
            PublicRow {
                pk: TypedPk::Integer(2),
                cell: Cell::Value("global-2".into()),
            },
            PublicRow {
                pk: TypedPk::Text("a".into()),
                cell: Cell::Value("branch-a".into()),
            },
            PublicRow {
                pk: TypedPk::Uuid("01920000-0000-7000-8000-000000000001".into()),
                cell: Cell::Null,
            },
        ]
    );
    assert_ne!(canonical, rejected);
}

#[test]
fn canonical_reader_preserves_typed_filter_order_and_limit_after_overlay() {
    let request = Request {
        exact_pks: Some(vec![
            TypedPk::Text("a".into()),
            TypedPk::Integer(2),
            TypedPk::Uuid("01920000-0000-7000-8000-000000000001".into()),
        ]),
        include_tombstones: false,
        limit: Some(2),
    };
    let mut reader = CountingCanonicalReader::new(fixture());
    let snapshots = terminal_snapshots(&mut reader, &request);
    assert_eq!(
        reader.calls, 1,
        "snapshot projection must use one canonical scan"
    );
    assert_eq!(
        snapshots,
        vec![
            PublicRow {
                pk: TypedPk::Integer(2),
                cell: Cell::Value("untracked-2".into()),
            },
            PublicRow {
                pk: TypedPk::Text("a".into()),
                cell: Cell::Value("untracked-a".into()),
            },
        ]
    );

    let mut reader = CountingCanonicalReader::new(fixture());
    let primary_keys = terminal_primary_keys(&mut reader, &request);
    assert_eq!(reader.calls, 1, "PK projection must use one canonical scan");
    assert_eq!(
        primary_keys,
        vec![TypedPk::Integer(2), TypedPk::Text("a".into())]
    );
}

#[test]
fn canonical_limit_is_applied_after_typed_order_and_tombstone_resolution() {
    let request = Request {
        exact_pks: None,
        include_tombstones: false,
        limit: Some(4),
    };
    let canonical = canonical_rows(&fixture(), &request);
    assert_eq!(canonical.len(), 4);
    assert!(canonical.iter().all(|row| row.cell != Cell::Tombstone));
    assert_eq!(canonical[0].pk, TypedPk::Integer(2));
    assert_eq!(canonical[3].pk, TypedPk::Text("f".into()));

    let with_tombstone = canonical_rows(
        &fixture(),
        &Request {
            include_tombstones: true,
            ..request
        },
    );
    assert!(
        with_tombstone
            .iter()
            .any(|row| { row.pk == TypedPk::Text("d".into()) && row.cell == Cell::Tombstone })
    );
}

#[test]
fn corrected_terminal_derivation_does_not_open_an_alternate_view() {
    let request = Request {
        exact_pks: None,
        include_tombstones: false,
        limit: Some(4),
    };
    let mut reader = CountingCanonicalReader::new(fixture());
    let expected = canonical_rows(&fixture(), &request);
    let actual = terminal_snapshots(&mut reader, &request);
    assert_eq!(actual, expected);
    assert_eq!(reader.calls, 1);
}

#[test]
fn domain_selector_contract_preserves_combined_overlay_and_explicit_narrow_modes() {
    let request = Request {
        exact_pks: None,
        include_tombstones: true,
        limit: None,
    };
    let combined = canonical_scan(&fixture(), DomainSelector::Combined, &request).unwrap();
    let tracked = canonical_scan(&fixture(), DomainSelector::Tracked, &request).unwrap();
    let untracked = canonical_scan(&fixture(), DomainSelector::Untracked, &request).unwrap();

    assert!(combined.iter().any(|row| {
        row.pk == TypedPk::Text("a".into()) && row.cell == Cell::Value("untracked-a".into())
    }));
    assert!(tracked.iter().any(|row| {
        row.pk == TypedPk::Text("a".into()) && row.cell == Cell::Value("branch-a".into())
    }));
    assert!(untracked.iter().any(|row| {
        row.pk == TypedPk::Text("a".into()) && row.cell == Cell::Value("untracked-a".into())
    }));
    assert!(
        combined
            .iter()
            .any(|row| { row.pk == TypedPk::Text("d".into()) && row.cell == Cell::Tombstone })
    );
}

#[test]
fn duplicate_authority_and_malformed_domain_fail_closed() {
    let mut rows = fixture();
    rows.push(rows[0].clone());
    assert_eq!(
        canonical_scan(
            &rows,
            DomainSelector::Combined,
            &Request {
                exact_pks: None,
                include_tombstones: true,
                limit: None,
            },
        ),
        Err(ModelError::DuplicateAuthority)
    );
    assert_eq!(decode_domain(9), Err(ModelError::MalformedDomain));
}

#[test]
fn same_stream_duplicates_fail_but_cross_stream_replacement_is_valid() {
    let mut tracked_duplicate = fixture();
    tracked_duplicate.push(Row {
        pk: TypedPk::Text("a".into()),
        branch: Branch::Local,
        domain: Domain::Tracked,
        cell: Cell::Value("conflicting-tracked-a".into()),
        sequence: 99,
    });
    assert_eq!(
        canonical_scan(
            &tracked_duplicate,
            DomainSelector::Combined,
            &Request {
                exact_pks: None,
                include_tombstones: true,
                limit: None,
            },
        ),
        Err(ModelError::DuplicateAuthority)
    );

    let mut untracked_duplicate = fixture();
    untracked_duplicate.push(Row {
        pk: TypedPk::Text("a".into()),
        branch: Branch::Local,
        domain: Domain::Untracked,
        cell: Cell::Tombstone,
        sequence: 99,
    });
    assert_eq!(
        canonical_scan(
            &untracked_duplicate,
            DomainSelector::Combined,
            &Request {
                exact_pks: None,
                include_tombstones: true,
                limit: None,
            },
        ),
        Err(ModelError::DuplicateAuthority)
    );

    let cross_stream = canonical_scan(
        &fixture(),
        DomainSelector::Combined,
        &Request {
            exact_pks: Some(vec![TypedPk::Text("a".into())]),
            include_tombstones: false,
            limit: None,
        },
    )
    .unwrap();
    assert_eq!(
        cross_stream,
        vec![PublicRow {
            pk: TypedPk::Text("a".into()),
            cell: Cell::Value("untracked-a".into()),
        }]
    );
}
