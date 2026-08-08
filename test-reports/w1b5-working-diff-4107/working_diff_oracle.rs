//! W1b-5 standalone working-diff provider model.
//!
//! Test/report-only. No Lix import, storage adapter, runtime, or publication.
//! Compile with rustc --edition=2024 --test -D warnings.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct RowKey {
    schema: String,
    file_id: Option<String>,
    entity: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Cell {
    Absent,
    Null,
    Tombstone,
    Scalar(String),
    Blob {
        blob_id: String,
        bytes: Vec<u8>,
        authenticated: bool,
        references: u8,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Snapshot {
    global: BTreeMap<RowKey, Cell>,
    local: BTreeMap<RowKey, Cell>,
    untracked: BTreeMap<RowKey, Cell>,
}

impl Snapshot {
    fn visible(&self, key: &RowKey) -> Cell {
        match self.local.get(key) {
            Some(Cell::Tombstone) => Cell::Tombstone,
            Some(value) if *value != Cell::Absent => value.clone(),
            _ => self.global.get(key).cloned().unwrap_or(Cell::Absent),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Authority {
    Authenticated { commit_id: String, generation: u64 },
    Missing,
    Malformed,
    WrongKind,
    IdentitySubstituted { claimed: String, actual: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Marker {
    None,
    Checkpoint {
        branch_id: String,
        walked_commit_id: String,
    },
    Malformed,
    WrongKind,
    IdentitySubstituted {
        claimed: String,
        actual: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Commit {
    id: String,
    parent: Option<String>,
    generation: u64,
    authority: Authority,
    marker: Marker,
    snapshot: Snapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OperationView {
    storage_read_id: u64,
    facade_id: u64,
    graph_id: u64,
    begin_reads: u32,
    events: Vec<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Order {
    Ascending,
    Descending,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Column {
    Key,
    Before,
    After,
    Kind,
    Branch,
    Blob,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Query {
    projection: Vec<Column>,
    order: Order,
    limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DiffKind {
    Added,
    Modified,
    Removed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DiffRow {
    key: RowKey,
    before: Cell,
    after: Cell,
    kind: DiffKind,
    base_commit_id: String,
    head_commit_id: String,
    branch_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WorkingDiffResult {
    base_commit_id: String,
    head_commit_id: String,
    branch_id: String,
    rows: Vec<DiffRow>,
    digest: u64,
    writes: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Reject {
    ReadView,
    MissingCommit(String),
    Authority,
    Generation,
    Cycle,
    Marker,
    NoCheckpointBaseline,
    BlobReference,
    DuplicateIdentity,
    Projection,
    PartialOutput,
}

fn authenticated(commit: &Commit) -> Result<(), Reject> {
    match &commit.authority {
        Authority::Authenticated {
            commit_id,
            generation,
        } if commit_id == &commit.id && *generation == commit.generation => Ok(()),
        _ => Err(Reject::Authority),
    }
}

fn marker_for_commit(commit: &Commit, branch_id: &str) -> Result<bool, Reject> {
    match &commit.marker {
        Marker::None => Ok(false),
        Marker::Checkpoint {
            branch_id: marker_branch,
            walked_commit_id,
        } if marker_branch == branch_id && walked_commit_id == &commit.id => Ok(true),
        Marker::Checkpoint { .. }
        | Marker::Malformed
        | Marker::WrongKind
        | Marker::IdentitySubstituted { .. } => Err(Reject::Marker),
    }
}

fn checkpoint_history(
    graph: &BTreeMap<String, Commit>,
    head_id: &str,
    branch_id: &str,
) -> Result<Vec<String>, Reject> {
    let mut current = Some(head_id.to_owned());
    let mut visited = BTreeSet::new();
    let mut history = Vec::new();
    while let Some(id) = current {
        if !visited.insert(id.clone()) {
            return Err(Reject::Cycle);
        }
        let commit = graph
            .get(&id)
            .ok_or_else(|| Reject::MissingCommit(id.clone()))?;
        authenticated(commit)?;
        if commit.parent.is_none() || marker_for_commit(commit, branch_id)? {
            history.push(id.clone());
        }
        if commit.generation > 0 {
            if let Some(parent_id) = &commit.parent {
                let parent = graph
                    .get(parent_id)
                    .ok_or_else(|| Reject::MissingCommit(parent_id.clone()))?;
                if parent.generation >= commit.generation {
                    return Err(Reject::Generation);
                }
            }
        }
        current = commit.parent.clone();
    }
    Ok(history)
}

fn validate_snapshot(snapshot: &Snapshot) -> Result<(), Reject> {
    for (key, cell) in snapshot.global.iter().chain(snapshot.local.iter()) {
        if key.schema == "lix_file" {
            match cell {
                Cell::Tombstone | Cell::Null | Cell::Absent => {}
                Cell::Blob {
                    authenticated: true,
                    references: 1,
                    ..
                } => {}
                Cell::Blob { .. } => return Err(Reject::BlobReference),
                Cell::Scalar(_) => return Err(Reject::BlobReference),
            }
        }
    }
    Ok(())
}

fn digest(result: &WorkingDiffResult) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    let mut feed = |bytes: &[u8]| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    };
    feed(result.base_commit_id.as_bytes());
    feed(result.head_commit_id.as_bytes());
    feed(result.branch_id.as_bytes());
    for row in &result.rows {
        feed(row.key.schema.as_bytes());
        feed(row.key.file_id.as_deref().unwrap_or("<none>").as_bytes());
        feed(row.key.entity.as_bytes());
        feed(format!("{:?}", row.before).as_bytes());
        feed(format!("{:?}", row.after).as_bytes());
        feed(format!("{:?}", row.kind).as_bytes());
    }
    hash
}

fn working_diff(
    view: &OperationView,
    graph: &BTreeMap<String, Commit>,
    base_id: &str,
    head_id: &str,
    branch_id: &str,
    query: &Query,
) -> Result<WorkingDiffResult, Reject> {
    if view.begin_reads != 1
        || view.storage_read_id != view.facade_id
        || view.facade_id != view.graph_id
        || view.events.iter().any(|id| *id != view.storage_read_id)
    {
        return Err(Reject::ReadView);
    }
    if query.projection.is_empty() || query.order != Order::Ascending {
        return Err(Reject::Projection);
    }
    if query.projection.iter().any(|column| {
        matches!(column, Column::Key)
            && query
                .projection
                .iter()
                .filter(|item| **item == Column::Key)
                .count()
                > 1
    }) {
        return Err(Reject::DuplicateIdentity);
    }
    let base = graph
        .get(base_id)
        .ok_or_else(|| Reject::MissingCommit(base_id.to_owned()))?;
    let head = graph
        .get(head_id)
        .ok_or_else(|| Reject::MissingCommit(head_id.to_owned()))?;
    authenticated(base)?;
    authenticated(head)?;
    validate_snapshot(&base.snapshot)?;
    validate_snapshot(&head.snapshot)?;
    let history = checkpoint_history(graph, head_id, branch_id)?;
    if history.first().map(String::as_str) != Some(base_id) {
        return Err(Reject::NoCheckpointBaseline);
    }
    let mut keys = BTreeSet::new();
    keys.extend(base.snapshot.global.keys().cloned());
    keys.extend(head.snapshot.global.keys().cloned());
    keys.extend(base.snapshot.local.keys().cloned());
    keys.extend(head.snapshot.local.keys().cloned());
    let mut rows = Vec::new();
    for key in keys {
        let before = base.snapshot.visible(&key);
        let after = head.snapshot.visible(&key);
        if before == after {
            continue;
        }
        let kind = match (&before, &after) {
            (Cell::Absent, _) => DiffKind::Added,
            (_, Cell::Absent) | (_, Cell::Tombstone) => DiffKind::Removed,
            _ => DiffKind::Modified,
        };
        rows.push(DiffRow {
            key,
            before,
            after,
            kind,
            base_commit_id: base_id.to_owned(),
            head_commit_id: head_id.to_owned(),
            branch_id: branch_id.to_owned(),
        });
        if query.limit.is_some_and(|limit| rows.len() >= limit) {
            break;
        }
    }
    let mut result = WorkingDiffResult {
        base_commit_id: base_id.to_owned(),
        head_commit_id: head_id.to_owned(),
        branch_id: branch_id.to_owned(),
        rows,
        digest: 0,
        writes: 0,
    };
    result.digest = digest(&result);
    if result.writes != 0 {
        return Err(Reject::PartialOutput);
    }
    Ok(result)
}

fn base_snapshot() -> Snapshot {
    let file = RowKey {
        schema: "lix_file".into(),
        file_id: Some("file-a".into()),
        entity: "[\"file-a\"]".into(),
    };
    let row = RowKey {
        schema: "entity".into(),
        file_id: None,
        entity: "[\"1\"]".into(),
    };
    Snapshot {
        global: BTreeMap::from([
            (
                file.clone(),
                Cell::Blob {
                    blob_id: "blob-a".into(),
                    bytes: vec![1, 2],
                    authenticated: true,
                    references: 1,
                },
            ),
            (row.clone(), Cell::Null),
        ]),
        local: BTreeMap::new(),
        untracked: BTreeMap::from([(
            RowKey {
                schema: "untracked".into(),
                file_id: None,
                entity: "u".into(),
            },
            Cell::Scalar("outside-history".into()),
        )]),
    }
}

fn view() -> OperationView {
    OperationView {
        storage_read_id: 9,
        facade_id: 9,
        graph_id: 9,
        begin_reads: 1,
        events: vec![9, 9, 9],
    }
}

fn query(limit: Option<usize>) -> Query {
    Query {
        projection: vec![
            Column::Key,
            Column::Before,
            Column::After,
            Column::Kind,
            Column::Branch,
            Column::Blob,
        ],
        order: Order::Ascending,
        limit,
    }
}

fn graph() -> BTreeMap<String, Commit> {
    let root = Commit {
        id: "R".into(),
        parent: None,
        generation: 0,
        authority: Authority::Authenticated {
            commit_id: "R".into(),
            generation: 0,
        },
        marker: Marker::None,
        snapshot: base_snapshot(),
    };
    let checkpoint = Commit {
        id: "C".into(),
        parent: Some("R".into()),
        generation: 1,
        authority: Authority::Authenticated {
            commit_id: "C".into(),
            generation: 1,
        },
        marker: Marker::Checkpoint {
            branch_id: "main".into(),
            walked_commit_id: "C".into(),
        },
        snapshot: base_snapshot(),
    };
    let mut ordinary_snapshot = base_snapshot();
    ordinary_snapshot.local.insert(
        RowKey {
            schema: "entity".into(),
            file_id: None,
            entity: "[\"1\"]".into(),
        },
        Cell::Tombstone,
    );
    ordinary_snapshot.local.insert(
        RowKey {
            schema: "entity".into(),
            file_id: None,
            entity: "[\"2\"]".into(),
        },
        Cell::Scalar("branch-local".into()),
    );
    let ordinary = Commit {
        id: "T".into(),
        parent: Some("C".into()),
        generation: 2,
        authority: Authority::Authenticated {
            commit_id: "T".into(),
            generation: 2,
        },
        marker: Marker::None,
        snapshot: ordinary_snapshot,
    };
    BTreeMap::from([
        ("R".into(), root),
        ("C".into(), checkpoint),
        ("T".into(), ordinary),
    ])
}

#[test]
fn checkpoint_to_ordinary_preserves_base_head_and_digest() {
    let graph = graph();
    let result = working_diff(&view(), &graph, "C", "T", "main", &query(None)).expect("diff");
    assert_eq!(result.base_commit_id, "C");
    assert_eq!(result.head_commit_id, "T");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].key.entity, "[\"1\"]");
    assert_eq!(result.rows[1].key.entity, "[\"2\"]");
    assert_eq!(result.writes, 0);
    assert_eq!(result.digest, digest(&result));
    assert_eq!(result.digest, 0xbb5a5128633cb198);
}

#[test]
fn branch_global_overlay_null_tombstone_and_untracked_are_distinct() {
    let mut snapshot = base_snapshot();
    let key = RowKey {
        schema: "entity".into(),
        file_id: None,
        entity: "[\"1\"]".into(),
    };
    assert_eq!(snapshot.visible(&key), Cell::Null);
    snapshot.local.insert(key.clone(), Cell::Tombstone);
    assert_eq!(snapshot.visible(&key), Cell::Tombstone);
    snapshot.local.insert(key, Cell::Null);
    assert_eq!(
        snapshot.visible(&RowKey {
            schema: "entity".into(),
            file_id: None,
            entity: "[\"1\"]".into(),
        }),
        Cell::Null
    );
    assert_eq!(snapshot.untracked.len(), 1);
}

#[test]
fn projection_order_and_limit_are_deterministic() {
    let graph = graph();
    let result =
        working_diff(&view(), &graph, "C", "T", "main", &query(Some(1))).expect("limited diff");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].base_commit_id, "C");
    assert_eq!(result.rows[0].head_commit_id, "T");
}

#[test]
fn missing_malformed_wrong_kind_and_identity_fail_closed() {
    let mut missing = graph();
    missing.remove("C");
    assert_eq!(
        working_diff(&view(), &missing, "C", "T", "main", &query(None)),
        Err(Reject::MissingCommit("C".into()))
    );

    let mut malformed = graph();
    malformed.get_mut("C").unwrap().authority = Authority::Malformed;
    assert_eq!(
        working_diff(&view(), &malformed, "C", "T", "main", &query(None)),
        Err(Reject::Authority)
    );

    let mut wrong_kind = graph();
    wrong_kind.get_mut("C").unwrap().marker = Marker::WrongKind;
    assert_eq!(
        working_diff(&view(), &wrong_kind, "C", "T", "main", &query(None)),
        Err(Reject::Marker)
    );

    let mut substituted = graph();
    substituted.get_mut("C").unwrap().authority = Authority::IdentitySubstituted {
        claimed: "C".into(),
        actual: "OTHER".into(),
    };
    assert_eq!(
        working_diff(&view(), &substituted, "C", "T", "main", &query(None)),
        Err(Reject::Authority)
    );

    for authority in [Authority::Missing, Authority::WrongKind] {
        let mut invalid = graph();
        invalid.get_mut("C").unwrap().authority = authority;
        assert_eq!(
            working_diff(&view(), &invalid, "C", "T", "main", &query(None)),
            Err(Reject::Authority)
        );
    }
}

#[test]
fn marker_mismatch_cycle_and_blob_substitution_fail_closed() {
    let mut mismatched_marker = graph();
    mismatched_marker.get_mut("C").unwrap().marker = Marker::Checkpoint {
        branch_id: "main".into(),
        walked_commit_id: "OTHER".into(),
    };
    assert_eq!(
        working_diff(&view(), &mismatched_marker, "C", "T", "main", &query(None)),
        Err(Reject::Marker)
    );

    for marker in [
        Marker::Malformed,
        Marker::IdentitySubstituted {
            claimed: "C".into(),
            actual: "OTHER".into(),
        },
    ] {
        let mut invalid = graph();
        invalid.get_mut("C").unwrap().marker = marker;
        assert_eq!(
            working_diff(&view(), &invalid, "C", "T", "main", &query(None)),
            Err(Reject::Marker)
        );
    }

    let mut cycle = graph();
    cycle.get_mut("R").unwrap().parent = Some("T".into());
    assert_eq!(
        working_diff(&view(), &cycle, "C", "T", "main", &query(None)),
        Err(Reject::Cycle)
    );

    let mut blob_substitution = graph();
    let file_key = RowKey {
        schema: "lix_file".into(),
        file_id: Some("file-a".into()),
        entity: "[\"file-a\"]".into(),
    };
    blob_substitution
        .get_mut("T")
        .unwrap()
        .snapshot
        .local
        .insert(
            file_key,
            Cell::Blob {
                blob_id: "substituted".into(),
                bytes: vec![9],
                authenticated: false,
                references: 1,
            },
        );
    assert_eq!(
        working_diff(&view(), &blob_substitution, "C", "T", "main", &query(None)),
        Err(Reject::BlobReference)
    );
}

#[test]
fn bad_view_wrong_order_and_no_baseline_produce_no_result() {
    let graph = graph();
    let mut bad_view = view();
    bad_view.begin_reads = 2;
    assert_eq!(
        working_diff(&bad_view, &graph, "C", "T", "main", &query(None)),
        Err(Reject::ReadView)
    );

    let mut descending = query(None);
    descending.order = Order::Descending;
    assert_eq!(
        working_diff(&view(), &graph, "C", "T", "main", &descending),
        Err(Reject::Projection)
    );

    assert_eq!(
        working_diff(&view(), &graph, "R", "T", "main", &query(None)),
        Err(Reject::NoCheckpointBaseline)
    );
}

#[test]
fn reopen_reproduces_exact_result_without_writes() {
    let graph = graph();
    let first = working_diff(&view(), &graph, "C", "T", "main", &query(None)).expect("first");
    let reopened = graph.clone();
    let second = working_diff(&view(), &reopened, "C", "T", "main", &query(None)).expect("reopen");
    assert_eq!(first, second);
    assert_eq!(second.writes, 0);
}
