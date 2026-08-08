#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, HashSet};

#[derive(Clone, Debug, Eq, PartialEq)]
enum Authority {
    Valid(String),
    Missing,
    Malformed,
    WrongKind,
    IdentitySubstituted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Marker {
    Absent,
    Deleted,
    Checkpoint { branch: String, commit: String },
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
    root: Authority,
    marker: Marker,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReadLease {
    view_id: u64,
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Reject {
    ReadOwnership,
    MissingCommit(String),
    RootAuthority(String),
    RootIdentity(String),
    MarkerNull(String),
    MarkerMalformed(String),
    MarkerBranch(String),
    MarkerIdentity(String),
    Generation(String),
    Cycle(String),
    FloorMissing(String),
    Depth,
}

fn valid_commit(id: &str, generation: u64, parents: Vec<String>, marker: Marker) -> Commit {
    Commit {
        id: id.to_owned(),
        generation,
        parents,
        root: Authority::Valid(format!("root:{id}")),
        marker,
    }
}

fn validate_read(lease: &ReadLease) -> Result<(), Reject> {
    if lease.begin_reads != 1 || lease.reader_instances != 1 || lease.provider_reads != 0 {
        return Err(Reject::ReadOwnership);
    }
    Ok(())
}

fn validate_root(commit: &Commit) -> Result<(), Reject> {
    match &commit.root {
        Authority::Valid(root) if root == &format!("root:{}", commit.id) => Ok(()),
        Authority::Valid(_) | Authority::IdentitySubstituted => {
            Err(Reject::RootIdentity(commit.id.clone()))
        }
        Authority::Missing | Authority::Malformed | Authority::WrongKind => {
            Err(Reject::RootAuthority(commit.id.clone()))
        }
    }
}

fn validate_marker(commit: &Commit, branch: &str) -> Result<bool, Reject> {
    match &commit.marker {
        Marker::Absent | Marker::Deleted => Ok(false),
        Marker::Checkpoint {
            branch: marker_branch,
            commit: marker_commit,
        } if marker_branch == branch && marker_commit == &commit.id => Ok(true),
        Marker::Checkpoint { .. } | Marker::WrongBranch => {
            Err(Reject::MarkerBranch(commit.id.clone()))
        }
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
        validate_root(commit)?;
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
            if commit.generation != parent.generation.saturating_add(1) {
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
        view_id: lease.view_id,
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
        let marker = Marker::Checkpoint {
            branch: "main".to_owned(),
            commit: id.clone(),
        };
        commits.insert(
            id.clone(),
            valid_commit(&id, index as u64, vec![parent], marker),
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

fn lease(view_id: u64) -> ReadLease {
    ReadLease {
        view_id,
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
fn marker_and_root_corruption_fail_closed_before_history_output() {
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
    let root_cases = [
        Authority::Missing,
        Authority::Malformed,
        Authority::WrongKind,
        Authority::IdentitySubstituted,
    ];
    for authority in root_cases {
        let mut graph = rotations(2);
        graph.checkpoint_floor = None;
        graph.commits.get_mut("c001").unwrap().root = authority;
        assert!(reconstruct(&graph, &lease(10)).is_err());
    }
}

#[test]
fn missing_parent_generation_gap_and_cycle_fail_closed() {
    let mut missing = rotations(2);
    missing.checkpoint_floor = None;
    missing.commits.get_mut("c001").unwrap().parents = vec!["missing".to_owned()];
    assert_eq!(
        reconstruct(&missing, &lease(11)),
        Err(Reject::MissingCommit("missing".to_owned()))
    );

    let mut generation = rotations(2);
    generation.checkpoint_floor = None;
    generation.commits.get_mut("c001").unwrap().generation = 9;
    assert!(matches!(
        reconstruct(&generation, &lease(12)),
        Err(Reject::Generation(_))
    ));

    let mut cycle = rotations(3);
    cycle.checkpoint_floor = None;
    cycle.commits.get_mut("c000").unwrap().parents = vec!["c002".to_owned()];
    cycle.commits.get_mut("c000").unwrap().generation = 3;
    assert!(matches!(
        reconstruct(&cycle, &lease(13)),
        Err(Reject::Cycle(_) | Reject::Generation(_))
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
        view_id: 15,
        begin_reads: 2,
        reader_instances: 2,
        provider_reads: 0,
    };
    assert!(matches!(
        operation_graph(&graph, &duplicate_reader),
        Err(Reject::ReadOwnership)
    ));

    let provider_refresh = ReadLease {
        view_id: 16,
        begin_reads: 1,
        reader_instances: 1,
        provider_reads: 1,
    };
    assert!(matches!(
        operation_graph(&graph, &provider_refresh),
        Err(Reject::ReadOwnership)
    ));
}
