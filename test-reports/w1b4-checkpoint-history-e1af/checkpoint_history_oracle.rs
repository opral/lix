#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, HashSet};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Domain {
    CommitCatalog,
    CommitRecord,
    CommitTopology,
    CheckpointMarker,
    StateRoot,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ObjectId(String);

fn object_id(domain: Domain, commit: &str) -> ObjectId {
    ObjectId(format!("{:?}:{commit}", domain))
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Authority<T> {
    Valid(T),
    Missing,
    Malformed,
    WrongKind,
    IdentitySubstituted(T),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParentEdge {
    object: ObjectId,
    commit: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CommitCatalogEntry {
    key: ObjectId,
    record_object: ObjectId,
    domain: Domain,
    commit: String,
    generation: u64,
    parents: Vec<ParentEdge>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CommitRecord {
    object: ObjectId,
    domain: Domain,
    commit: String,
    generation: u64,
    parents: Vec<ParentEdge>,
    payload_digest: String,
    payload_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CommitTopology {
    object: ObjectId,
    domain: Domain,
    commit: String,
    generation: u64,
    parents: Vec<ParentEdge>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StateRoot {
    object: ObjectId,
    domain: Domain,
    commit: String,
    digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Marker {
    Absent,
    Deleted,
    Checkpoint {
        object: ObjectId,
        branch: String,
        commit: String,
        root: ObjectId,
        bytes: Vec<u8>,
    },
    Null,
    Malformed,
    WrongBranch,
    IdentitySubstituted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Commit {
    id: String,
    generation: u64,
    parents: Vec<String>,
    catalog: Authority<CommitCatalogEntry>,
    record: Authority<CommitRecord>,
    topology: Authority<CommitTopology>,
    root: Authority<StateRoot>,
    marker: Marker,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StorageRead {
    id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CoherentView {
    read_id: u64,
    view_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReadLease {
    read: StorageRead,
    view: CoherentView,
    begin_reads: u32,
    reader_instances: u32,
    provider_reads: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Graph {
    commits: BTreeMap<String, Commit>,
    head: String,
    branch: String,
    checkpoint_floor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Entry {
    commit: String,
    generation: u64,
    depth: usize,
    implicit_root: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Reconstruction {
    entries: Vec<Entry>,
    checkpoint_floor: Option<String>,
    retained_for_history_undo: BTreeSet<String>,
    view_id: u64,
    read_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Reject {
    ReadOwnership,
    MissingCommit(String),
    Catalog(String),
    Record(String),
    Topology(String),
    RootAuthority(String),
    RootIdentity(String),
    MarkerNull(String),
    MarkerMalformed(String),
    MarkerBranch(String),
    MarkerIdentity(String),
    Generation(String),
    DuplicateParent(String),
    ParentOrder(String),
    Cycle(String),
    FloorMissing(String),
    Depth,
}

fn parent_edges(parents: &[String]) -> Vec<ParentEdge> {
    parents
        .iter()
        .map(|parent| ParentEdge {
            object: object_id(Domain::CommitRecord, parent),
            commit: parent.clone(),
        })
        .collect()
}

fn marker_bytes(branch: &str, commit: &str, root: &ObjectId) -> Vec<u8> {
    format!("marker:{branch}:{commit}:{}", root.0).into_bytes()
}

fn record_payload_bytes(commit: &str) -> Vec<u8> {
    format!("record:{commit}").into_bytes()
}

fn checkpoint_marker(branch: &str, commit: &str) -> Marker {
    let root = object_id(Domain::StateRoot, commit);
    Marker::Checkpoint {
        object: object_id(Domain::CheckpointMarker, commit),
        branch: branch.to_owned(),
        commit: commit.to_owned(),
        root: root.clone(),
        bytes: marker_bytes(branch, commit, &root),
    }
}

fn valid_commit(id: &str, generation: u64, parents: Vec<String>, marker: Marker) -> Commit {
    let edges = parent_edges(&parents);
    Commit {
        id: id.to_owned(),
        generation,
        parents,
        catalog: Authority::Valid(CommitCatalogEntry {
            key: object_id(Domain::CommitCatalog, id),
            record_object: object_id(Domain::CommitRecord, id),
            domain: Domain::CommitCatalog,
            commit: id.to_owned(),
            generation,
            parents: edges.clone(),
        }),
        record: Authority::Valid(CommitRecord {
            object: object_id(Domain::CommitRecord, id),
            domain: Domain::CommitRecord,
            commit: id.to_owned(),
            generation,
            parents: edges.clone(),
            payload_digest: format!("payload:{id}"),
            payload_bytes: record_payload_bytes(id),
        }),
        topology: Authority::Valid(CommitTopology {
            object: object_id(Domain::CommitTopology, id),
            domain: Domain::CommitTopology,
            commit: id.to_owned(),
            generation,
            parents: edges,
        }),
        root: Authority::Valid(StateRoot {
            object: object_id(Domain::StateRoot, id),
            domain: Domain::StateRoot,
            commit: id.to_owned(),
            digest: format!("root:{id}"),
        }),
        marker,
    }
}

fn validate_read(lease: &ReadLease) -> Result<(), Reject> {
    if lease.begin_reads != 1
        || lease.reader_instances != 1
        || lease.provider_reads != 0
        || lease.view.read_id != lease.read.id
    {
        return Err(Reject::ReadOwnership);
    }
    Ok(())
}

fn validate_edges(commit: &Commit) -> Result<(), Reject> {
    let expected = parent_edges(&commit.parents);
    let authorities = [
        match &commit.catalog {
            Authority::Valid(value) => &value.parents,
            _ => return Err(Reject::Catalog(commit.id.clone())),
        },
        match &commit.record {
            Authority::Valid(value) => &value.parents,
            _ => return Err(Reject::Record(commit.id.clone())),
        },
        match &commit.topology {
            Authority::Valid(value) => &value.parents,
            _ => return Err(Reject::Topology(commit.id.clone())),
        },
    ];
    let mut unique = BTreeSet::new();
    for edge in &expected {
        if !unique.insert(edge.commit.clone()) {
            return Err(Reject::DuplicateParent(commit.id.clone()));
        }
    }
    for actual in authorities {
        if actual.len() != expected.len() {
            return Err(Reject::ParentOrder(commit.id.clone()));
        }
        if actual != &expected {
            return Err(Reject::ParentOrder(commit.id.clone()));
        }
    }
    Ok(())
}

fn validate_commit(map_key: &str, commit: &Commit) -> Result<(), Reject> {
    if map_key != commit.id {
        return Err(Reject::Catalog(commit.id.clone()));
    }
    let expected_catalog = object_id(Domain::CommitCatalog, &commit.id);
    let expected_record = object_id(Domain::CommitRecord, &commit.id);
    let expected_topology = object_id(Domain::CommitTopology, &commit.id);
    let expected_root = object_id(Domain::StateRoot, &commit.id);
    match &commit.catalog {
        Authority::Valid(value)
            if value.key == expected_catalog
                && value.record_object == expected_record
                && value.domain == Domain::CommitCatalog
                && value.commit == commit.id
                && value.generation == commit.generation => {}
        _ => return Err(Reject::Catalog(commit.id.clone())),
    }
    match &commit.record {
        Authority::Valid(value)
            if value.object == expected_record
                && value.domain == Domain::CommitRecord
                && value.commit == commit.id
                && value.generation == commit.generation
                && value.payload_digest == format!("payload:{}", commit.id)
                && value.payload_bytes == record_payload_bytes(&commit.id) => {}
        _ => return Err(Reject::Record(commit.id.clone())),
    }
    match &commit.topology {
        Authority::Valid(value)
            if value.object == expected_topology
                && value.domain == Domain::CommitTopology
                && value.commit == commit.id
                && value.generation == commit.generation => {}
        _ => return Err(Reject::Topology(commit.id.clone())),
    }
    match &commit.root {
        Authority::Valid(value)
            if value.object == expected_root
                && value.domain == Domain::StateRoot
                && value.commit == commit.id
                && value.digest == format!("root:{}", commit.id) => {}
        Authority::Valid(_) | Authority::IdentitySubstituted(_) => {
            return Err(Reject::RootIdentity(commit.id.clone()));
        }
        Authority::Missing | Authority::Malformed | Authority::WrongKind => {
            return Err(Reject::RootAuthority(commit.id.clone()));
        }
    }
    validate_edges(commit)
}

fn validate_marker(commit: &Commit, branch: &str) -> Result<bool, Reject> {
    let expected_root = object_id(Domain::StateRoot, &commit.id);
    let expected_marker = object_id(Domain::CheckpointMarker, &commit.id);
    match &commit.marker {
        Marker::Absent | Marker::Deleted if commit.parents.is_empty() => Ok(false),
        Marker::Absent | Marker::Deleted => Ok(false),
        Marker::Checkpoint {
            object,
            branch: marker_branch,
            commit: marker_commit,
            root,
            bytes,
        } => {
            if object != &expected_marker
                || marker_commit != &commit.id
                || root != &expected_root
                || bytes != &marker_bytes(branch, &commit.id, &expected_root)
            {
                return Err(Reject::MarkerIdentity(commit.id.clone()));
            }
            if marker_branch != branch {
                return Err(Reject::MarkerBranch(commit.id.clone()));
            }
            if commit.parents.is_empty() {
                return Err(Reject::MarkerIdentity(commit.id.clone()));
            }
            Ok(true)
        }
        Marker::WrongBranch => Err(Reject::MarkerBranch(commit.id.clone())),
        Marker::IdentitySubstituted => Err(Reject::MarkerIdentity(commit.id.clone())),
        Marker::Null => Err(Reject::MarkerNull(commit.id.clone())),
        Marker::Malformed => Err(Reject::MarkerMalformed(commit.id.clone())),
    }
}

fn reconstruct(graph: &Graph, lease: &ReadLease) -> Result<Reconstruction, Reject> {
    validate_read(lease)?;
    let mut current = Some(graph.head.clone());
    let mut visited = HashSet::new();
    let mut entries = Vec::new();
    let mut retained = BTreeSet::new();
    let mut floor_seen = graph.checkpoint_floor.is_none();

    while let Some(commit_id) = current {
        if !visited.insert(commit_id.clone()) {
            return Err(Reject::Cycle(commit_id));
        }
        if entries.len() > 4096 {
            return Err(Reject::Depth);
        }
        let commit = graph
            .commits
            .get(&commit_id)
            .ok_or_else(|| Reject::MissingCommit(commit_id.clone()))?;
        validate_commit(&commit_id, commit)?;
        if commit.parents.is_empty() {
            if commit.generation != 0 {
                return Err(Reject::Generation(commit.id.clone()));
            }
        } else {
            let parent_id = &commit.parents[0];
            let parent = graph
                .commits
                .get(parent_id)
                .ok_or_else(|| Reject::MissingCommit(parent_id.clone()))?;
            validate_commit(parent_id, parent)?;
            if commit.generation != parent.generation.checked_add(1).unwrap_or(u64::MAX) {
                return Err(Reject::Generation(commit.id.clone()));
            }
        }
        let _is_checkpoint = validate_marker(commit, &graph.branch)?;
        if graph.checkpoint_floor.as_deref() == Some(commit.id.as_str()) {
            floor_seen = true;
        }
        retained.insert(commit.id.clone());
        entries.push(Entry {
            commit: commit.id.clone(),
            generation: commit.generation,
            depth: entries.len(),
            implicit_root: commit.parents.is_empty(),
        });
        current = commit.parents.first().cloned();
    }

    if !floor_seen {
        return Err(Reject::FloorMissing(
            graph.checkpoint_floor.clone().unwrap_or_default(),
        ));
    }
    Ok(Reconstruction {
        entries,
        checkpoint_floor: graph.checkpoint_floor.clone(),
        retained_for_history_undo: retained,
        view_id: lease.view.view_id,
        read_id: lease.read.id,
    })
}

fn operation_graph<'a>(
    graph: &'a Graph,
    lease: &'a ReadLease,
) -> Result<OperationGraph<'a>, Reject> {
    validate_read(lease)?;
    Ok(OperationGraph { graph, lease })
}

struct OperationGraph<'a> {
    graph: &'a Graph,
    lease: &'a ReadLease,
}

impl OperationGraph<'_> {
    fn history(&self) -> Result<Reconstruction, Reject> {
        reconstruct(self.graph, self.lease)
    }

    fn undo_redo_retention(&self) -> Result<BTreeSet<String>, Reject> {
        Ok(self.history()?.retained_for_history_undo)
    }
}

fn rotations(count: usize) -> Graph {
    let mut commits = BTreeMap::new();
    let root = "c000".to_owned();
    commits.insert(
        root.clone(),
        valid_commit("c000", 0, Vec::new(), Marker::Absent),
    );
    let mut parent = root;
    for index in 1..count {
        let id = format!("c{index:03}");
        let marker = checkpoint_marker("main", &id);
        commits.insert(
            id.clone(),
            valid_commit(&id, index as u64, vec![parent.clone()], marker),
        );
        parent = id;
    }
    Graph {
        commits,
        head: parent,
        branch: "main".to_owned(),
        checkpoint_floor: Some("c032".to_owned()),
    }
}

fn two_parent_graph() -> Graph {
    let mut commits = BTreeMap::new();
    commits.insert(
        "c000".to_owned(),
        valid_commit("c000", 0, Vec::new(), Marker::Absent),
    );
    commits.insert(
        "s000".to_owned(),
        valid_commit("s000", 0, Vec::new(), Marker::Absent),
    );
    commits.insert(
        "c001".to_owned(),
        valid_commit(
            "c001",
            1,
            vec!["c000".to_owned(), "s000".to_owned()],
            checkpoint_marker("main", "c001"),
        ),
    );
    Graph {
        commits,
        head: "c001".to_owned(),
        branch: "main".to_owned(),
        checkpoint_floor: None,
    }
}

fn set_authority_generation(commit: &mut Commit, generation: u64) {
    commit.generation = generation;
    if let Authority::Valid(value) = &mut commit.catalog {
        value.generation = generation;
    }
    if let Authority::Valid(value) = &mut commit.record {
        value.generation = generation;
    }
    if let Authority::Valid(value) = &mut commit.topology {
        value.generation = generation;
    }
}

fn lease(view_id: u64) -> ReadLease {
    ReadLease {
        read: StorageRead { id: 1 },
        view: CoherentView {
            read_id: 1,
            view_id,
        },
        begin_reads: 1,
        reader_instances: 1,
        provider_reads: 0,
    }
}

#[test]
fn sixty_five_rotations_keep_floor_separate_and_retain_history_undo() {
    let graph = rotations(65);
    let read_lease = lease(7);
    let operation = operation_graph(&graph, &read_lease).expect("one operation view");
    let reconstruction = operation.history().expect("valid 65-rotation history");

    assert_eq!(reconstruction.entries.len(), 65);
    assert_eq!(reconstruction.entries[0].commit, "c064");
    assert_eq!(reconstruction.entries[64].commit, "c000");
    assert!(reconstruction.entries[64].implicit_root);
    assert_eq!(reconstruction.entries[32].commit, "c032");
    assert_eq!(reconstruction.entries[32].generation, 32);
    assert_eq!(reconstruction.entries[32].depth, 32);
    assert_eq!(reconstruction.checkpoint_floor.as_deref(), Some("c032"));
    assert_eq!(reconstruction.retained_for_history_undo.len(), 65);
    assert_eq!(reconstruction.view_id, 7);
    assert_eq!(reconstruction.read_id, 1);
    assert_eq!(operation.undo_redo_retention().unwrap().len(), 65);
}

#[test]
fn root_is_implicit_and_absent_or_deleted_non_root_markers_are_not_checkpoints() {
    let mut graph = rotations(3);
    graph.checkpoint_floor = None;
    graph.commits.get_mut("c002").unwrap().marker = Marker::Absent;
    graph.commits.get_mut("c001").unwrap().marker = Marker::Deleted;
    let reconstruction = reconstruct(&graph, &lease(8)).expect("valid non-marker history");
    assert!(reconstruction.entries.last().unwrap().implicit_root);
    assert_eq!(reconstruction.entries.len(), 3);
}

#[test]
fn marker_root_catalog_record_and_topology_corruption_fail_closed_before_output() {
    let marker_cases = [
        Marker::Null,
        Marker::Malformed,
        Marker::WrongBranch,
        Marker::IdentitySubstituted,
    ];
    for marker in marker_cases {
        let mut graph = rotations(2);
        graph.checkpoint_floor = None;
        graph.commits.get_mut("c001").unwrap().marker = marker;
        assert!(reconstruct(&graph, &lease(9)).is_err());
    }
    let mut marker_bytes = rotations(2);
    marker_bytes.checkpoint_floor = None;
    if let Marker::Checkpoint { bytes, .. } =
        &mut marker_bytes.commits.get_mut("c001").unwrap().marker
    {
        *bytes = b"tampered-marker".to_vec();
    }
    assert!(matches!(
        reconstruct(&marker_bytes, &lease(9)),
        Err(Reject::MarkerIdentity(_))
    ));
    for authority in [
        Authority::Missing,
        Authority::Malformed,
        Authority::WrongKind,
        Authority::IdentitySubstituted(StateRoot {
            object: object_id(Domain::StateRoot, "substituted"),
            domain: Domain::StateRoot,
            commit: "substituted".to_owned(),
            digest: "root:substituted".to_owned(),
        }),
    ] {
        let mut graph = rotations(2);
        graph.checkpoint_floor = None;
        graph.commits.get_mut("c001").unwrap().root = authority;
        assert!(reconstruct(&graph, &lease(10)).is_err());
    }
    let mut graph = rotations(2);
    graph.checkpoint_floor = None;
    graph.commits.get_mut("c001").unwrap().record = Authority::WrongKind;
    assert!(matches!(
        reconstruct(&graph, &lease(10)),
        Err(Reject::Record(_))
    ));
    let mut graph = rotations(2);
    graph.checkpoint_floor = None;
    if let Authority::Valid(record) = &mut graph.commits.get_mut("c001").unwrap().record {
        record.payload_bytes = b"tampered-record".to_vec();
    }
    assert!(matches!(
        reconstruct(&graph, &lease(10)),
        Err(Reject::Record(_))
    ));
    let mut graph = rotations(2);
    graph.checkpoint_floor = None;
    graph.commits.get_mut("c001").unwrap().catalog = Authority::Missing;
    assert!(matches!(
        reconstruct(&graph, &lease(10)),
        Err(Reject::Catalog(_))
    ));
    let mut graph = rotations(2);
    graph.checkpoint_floor = None;
    graph.commits.get_mut("c001").unwrap().topology = Authority::Malformed;
    assert!(matches!(
        reconstruct(&graph, &lease(10)),
        Err(Reject::Topology(_))
    ));
}

#[test]
fn missing_parent_generation_duplicate_order_and_cycle_fail_closed() {
    let mut missing = rotations(2);
    missing.checkpoint_floor = None;
    missing.commits.remove("c000");
    assert_eq!(
        reconstruct(&missing, &lease(11)),
        Err(Reject::MissingCommit("c000".to_owned()))
    );

    let mut generation = rotations(2);
    generation.checkpoint_floor = None;
    set_authority_generation(generation.commits.get_mut("c001").unwrap(), 9);
    assert!(matches!(
        reconstruct(&generation, &lease(12)),
        Err(Reject::Generation(_))
    ));

    let mut duplicate = rotations(2);
    duplicate.checkpoint_floor = None;
    duplicate.commits.get_mut("c001").unwrap().parents = vec!["c000".to_owned(), "c000".to_owned()];
    assert!(matches!(
        reconstruct(&duplicate, &lease(12)),
        Err(Reject::DuplicateParent(_))
    ));

    let mut cycle = rotations(3);
    cycle.checkpoint_floor = None;
    cycle.commits.get_mut("c000").unwrap().parents = vec!["c002".to_owned()];
    assert!(reconstruct(&cycle, &lease(13)).is_err());

    let mut reordered = two_parent_graph();
    let record = reordered.commits.get_mut("c001").unwrap();
    if let Authority::Valid(value) = &mut record.record {
        value.parents.reverse();
    }
    assert!(matches!(
        reconstruct(&reordered, &lease(13)),
        Err(Reject::ParentOrder(_))
    ));
}

#[test]
fn cold_reopen_is_identical_and_second_reader_or_provider_read_is_rejected() {
    let graph = rotations(65);
    let first = operation_graph(&graph, &lease(14))
        .unwrap()
        .history()
        .unwrap();
    let reopened = graph.clone();
    let second = operation_graph(&reopened, &lease(14))
        .unwrap()
        .history()
        .unwrap();
    assert_eq!(first, second);

    let duplicate_reader = ReadLease {
        read: StorageRead { id: 2 },
        view: CoherentView {
            read_id: 2,
            view_id: 15,
        },
        begin_reads: 2,
        reader_instances: 2,
        provider_reads: 0,
    };
    assert!(matches!(
        operation_graph(&graph, &duplicate_reader),
        Err(Reject::ReadOwnership)
    ));

    let provider_refresh = ReadLease {
        read: StorageRead { id: 1 },
        view: CoherentView {
            read_id: 1,
            view_id: 16,
        },
        begin_reads: 1,
        reader_instances: 1,
        provider_reads: 1,
    };
    assert!(matches!(
        operation_graph(&graph, &provider_refresh),
        Err(Reject::ReadOwnership)
    ));
}
