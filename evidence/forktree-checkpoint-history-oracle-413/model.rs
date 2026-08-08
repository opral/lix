use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Commit {
    id: &'static str,
    parents: Vec<&'static str>,
    checkpoint_marker: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecoveryRef {
    recovered_head: &'static str,
    checkpoint: &'static str,
    interval_has_commits: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum HistoryError {
    MissingCommit(&'static str),
    Cycle(&'static str),
}

fn checkpoint_history(
    commits: &HashMap<&'static str, Commit>,
    head: &'static str,
) -> Result<Vec<&'static str>, HistoryError> {
    let mut current = Some(head);
    let mut visited = HashSet::new();
    let mut result = Vec::new();
    while let Some(id) = current {
        if !visited.insert(id) {
            return Err(HistoryError::Cycle(id));
        }
        let commit = commits.get(id).ok_or(HistoryError::MissingCommit(id))?;
        if commit.parents.is_empty() || commit.checkpoint_marker {
            result.push(id);
        }
        current = commit.parents.first().copied();
    }
    Ok(result)
}

fn merge_base(
    commits: &HashMap<&'static str, Commit>,
    left: &'static str,
    right: &'static str,
) -> Result<&'static str, HistoryError> {
    fn ancestors(
        commits: &HashMap<&'static str, Commit>,
        start: &'static str,
        out: &mut HashSet<&'static str>,
    ) -> Result<(), HistoryError> {
        if !out.insert(start) {
            return Ok(());
        }
        let commit = commits.get(start).ok_or(HistoryError::MissingCommit(start))?;
        for parent in &commit.parents {
            ancestors(commits, parent, out)?;
        }
        Ok(())
    }

    let mut left_ancestors = HashSet::new();
    ancestors(commits, left, &mut left_ancestors)?;
    let mut queue = vec![right];
    let mut seen = HashSet::new();
    while let Some(id) = queue.pop() {
        if !seen.insert(id) {
            continue;
        }
        if left_ancestors.contains(id) {
            return Ok(id);
        }
        let commit = commits.get(id).ok_or(HistoryError::MissingCommit(id))?;
        queue.extend(commit.parents.iter().copied());
    }
    Err(HistoryError::MissingCommit("no common ancestor"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistoricalCell {
    Absent,
    Null,
    Tombstone,
    Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistoricalInput {
    Valid(HistoricalCell),
    MissingCommit,
    MissingRoot,
    WrongKindRoot,
    MalformedRoot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistoricalResult {
    Cell(HistoricalCell),
    Corruption,
}

fn historical_point(input: HistoricalInput) -> HistoricalResult {
    match input {
        HistoricalInput::Valid(cell) => HistoricalResult::Cell(cell),
        HistoricalInput::MissingCommit
        | HistoricalInput::MissingRoot
        | HistoricalInput::WrongKindRoot
        | HistoricalInput::MalformedRoot => HistoricalResult::Corruption,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct UndoState {
    undo_top: Option<&'static str>,
    redo_top: Option<&'static str>,
}

fn undo(state: UndoState, checkpoint_floor: &'static str) -> Result<UndoState, &'static str> {
    match state.undo_top {
        None => Err("checkpoint floor"),
        Some(id) if id == checkpoint_floor => Err("checkpoint floor"),
        Some(id) => Ok(UndoState {
            undo_top: Some(checkpoint_floor),
            redo_top: Some(id),
        }),
    }
}

fn redo(state: UndoState) -> Result<UndoState, &'static str> {
    let target = state.redo_top.ok_or("nothing to redo")?;
    Ok(UndoState {
        undo_top: Some(target),
        redo_top: None,
    })
}

fn retention_roots(
    active_branch_heads: &[&'static str],
    recovery_refs: &[RecoveryRef],
    checkpoint_roots: &[&'static str],
) -> HashSet<&'static str> {
    let mut roots = active_branch_heads.iter().copied().collect::<HashSet<_>>();
    roots.extend(
        recovery_refs
            .iter()
            .flat_map(|r| [r.recovered_head, r.checkpoint]),
    );
    roots.extend(checkpoint_roots.iter().copied());
    roots
}

#[test]
fn history_uses_first_parent_and_fails_closed() {
    let commits = HashMap::from([
        (
            "R",
            Commit {
                id: "R",
                parents: vec![],
                checkpoint_marker: false,
            },
        ),
        (
            "C1",
            Commit {
                id: "C1",
                parents: vec!["R"],
                checkpoint_marker: true,
            },
        ),
        (
            "A",
            Commit {
                id: "A",
                parents: vec!["C1"],
                checkpoint_marker: false,
            },
        ),
        (
            "C2",
            Commit {
                id: "C2",
                parents: vec!["C1"],
                checkpoint_marker: true,
            },
        ),
    ]);
    assert_eq!(checkpoint_history(&commits, "C2"), Ok(vec!["C2", "C1", "R"]));

    let mut missing = commits.clone();
    missing.remove("C1");
    assert_eq!(
        checkpoint_history(&missing, "C2"),
        Err(HistoryError::MissingCommit("C1"))
    );

    let mut cycle = commits;
    cycle.get_mut("C1").unwrap().parents = vec!["C2"];
    assert_eq!(
        checkpoint_history(&cycle, "C2"),
        Err(HistoryError::Cycle("C2"))
    );
}

#[test]
fn recovery_ref_is_retention_floor_not_chronology() {
    let commits = HashMap::from([
        (
            "H",
            Commit {
                id: "H",
                parents: vec!["R"],
                checkpoint_marker: false,
            },
        ),
        (
            "C",
            Commit {
                id: "C",
                parents: vec!["R"],
                checkpoint_marker: true,
            },
        ),
        (
            "S",
            Commit {
                id: "S",
                parents: vec!["H", "C"],
                checkpoint_marker: false,
            },
        ),
        (
            "R",
            Commit {
                id: "R",
                parents: vec![],
                checkpoint_marker: false,
            },
        ),
    ]);
    let recovery = RecoveryRef {
        recovered_head: "H",
        checkpoint: "C",
        interval_has_commits: true,
    };
    assert!(recovery.interval_has_commits);
    assert_eq!(commits["S"].parents[0], "H");
    assert_eq!(commits["S"].parents[1], "C");
    assert_eq!(merge_base(&commits, "S", "C"), Ok("C"));
    assert_eq!(
        retention_roots(&["S"], &[recovery.clone()], &["C"]),
        HashSet::from(["S", "H", "C"])
    );
    assert_eq!(
        retention_roots(&[], &[], &["C"]),
        HashSet::from(["C"])
    );
}

#[test]
fn sixty_five_rotations_keep_chronology_and_current_recovery_pair() {
    let mut commits = HashMap::from([(
        "R",
        Commit {
            id: "R",
            parents: vec![],
            checkpoint_marker: false,
        },
    )]);
    let mut previous = "R";
    let mut current_recovery = None;
    for index in 1..=65 {
        let id = Box::leak(format!("C{index}").into_boxed_str());
        commits.insert(
            id,
            Commit {
                id,
                parents: vec![previous],
                checkpoint_marker: true,
            },
        );
        current_recovery = Some(RecoveryRef {
            recovered_head: previous,
            checkpoint: id,
            interval_has_commits: index != 64,
        });
        previous = id;
    }
    let history = checkpoint_history(&commits, previous).unwrap();
    assert_eq!(history.len(), 66);
    assert_eq!(history.first().copied(), Some("C65"));
    assert_eq!(history.last().copied(), Some("R"));
    let recovery = current_recovery.unwrap();
    assert_eq!(recovery.recovered_head, "C64");
    assert_eq!(recovery.checkpoint, "C65");
}

#[test]
fn historical_cells_distinguish_absence_from_corruption() {
    assert_eq!(
        historical_point(HistoricalInput::Valid(HistoricalCell::Absent)),
        HistoricalResult::Cell(HistoricalCell::Absent)
    );
    for input in [
        HistoricalInput::MissingCommit,
        HistoricalInput::MissingRoot,
        HistoricalInput::WrongKindRoot,
        HistoricalInput::MalformedRoot,
    ] {
        assert_eq!(historical_point(input), HistoricalResult::Corruption);
    }
    assert_eq!(
        historical_point(HistoricalInput::Valid(HistoricalCell::Null)),
        HistoricalResult::Cell(HistoricalCell::Null)
    );
    assert_eq!(
        historical_point(HistoricalInput::Valid(HistoricalCell::Tombstone)),
        HistoricalResult::Cell(HistoricalCell::Tombstone)
    );
    assert_eq!(
        historical_point(HistoricalInput::Valid(HistoricalCell::Value)),
        HistoricalResult::Cell(HistoricalCell::Value)
    );
}

#[test]
fn undo_redo_stops_at_checkpoint_floor() {
    let after_undo = undo(
        UndoState {
            undo_top: Some("C1"),
            redo_top: None,
        },
        "C1",
    )
    .unwrap_err();
    assert_eq!(after_undo, "checkpoint floor");
    let after_undo = undo(
        UndoState {
            undo_top: Some("C2"),
            redo_top: None,
        },
        "C1",
    )
    .unwrap();
    assert_eq!(after_undo.redo_top, Some("C2"));
    assert_eq!(redo(after_undo).unwrap().undo_top, Some("C2"));
}
