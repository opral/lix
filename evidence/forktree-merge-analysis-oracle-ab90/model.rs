use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Identity(&'static str);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cell {
    Null,
    Tombstone,
    Value(&'static str),
}

type Snapshot = HashMap<Identity, Cell>;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Change {
    identity: Identity,
    before: Option<Cell>,
    after: Option<Cell>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum MergeError {
    Conflict(Identity),
    MissingCommit,
    Corruption,
    StalePublication,
    RegistryMismatch,
}

fn cell(snapshot: &Snapshot, identity: Identity) -> Option<Cell> {
    snapshot.get(&identity).copied()
}

fn diff(base: &Snapshot, side: &Snapshot) -> Vec<Change> {
    let identities = base
        .keys()
        .chain(side.keys())
        .copied()
        .collect::<HashSet<_>>();
    let mut changes = identities
        .into_iter()
        .filter_map(|identity| {
            let before = cell(base, identity);
            let after = cell(side, identity);
            (before != after).then_some(Change {
                identity,
                before,
                after,
            })
        })
        .collect::<Vec<_>>();
    changes.sort_by_key(|change| change.identity.0);
    changes
}

fn merge_plan(
    base: &Snapshot,
    target: &Snapshot,
    source: &Snapshot,
) -> Result<Snapshot, MergeError> {
    let target_diff = diff(base, target);
    let source_diff = diff(base, source);
    let mut result = base.clone();
    for identity in target_diff
        .iter()
        .chain(source_diff.iter())
        .map(|change| change.identity)
    {
        let target_after = cell(target, identity);
        let source_after = cell(source, identity);
        let target_changed = target_after != cell(base, identity);
        let source_changed = source_after != cell(base, identity);
        if target_changed && source_changed && target_after != source_after {
            return Err(MergeError::Conflict(identity));
        }
        let chosen = if source_changed { source_after } else { target_after };
        match chosen {
            Some(value) => {
                result.insert(identity, value);
            }
            None => {
                result.remove(&identity);
            }
        }
    }
    Ok(result)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Commit {
    id: &'static str,
    parents: Vec<&'static str>,
}

fn ancestors(commits: &HashMap<&'static str, Commit>, start: &'static str) -> Result<HashSet<&'static str>, MergeError> {
    let mut result = HashSet::new();
    let mut stack = vec![start];
    while let Some(id) = stack.pop() {
        if !result.insert(id) {
            continue;
        }
        let commit = commits.get(id).ok_or(MergeError::MissingCommit)?;
        stack.extend(commit.parents.iter().copied());
    }
    Ok(result)
}

fn merge_base(
    commits: &HashMap<&'static str, Commit>,
    target: &'static str,
    source: &'static str,
) -> Result<&'static str, MergeError> {
    let target_ancestors = ancestors(commits, target)?;
    let mut queue = vec![source];
    let mut seen = HashSet::new();
    while let Some(id) = queue.pop() {
        if !seen.insert(id) {
            continue;
        }
        if target_ancestors.contains(id) {
            return Ok(id);
        }
        let commit = commits.get(id).ok_or(MergeError::MissingCommit)?;
        queue.extend(commit.parents.iter().copied());
    }
    Err(MergeError::MissingCommit)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PluginRegistry {
    generation: &'static str,
    schema_keys: &'static [&'static str],
}

fn require_common_registry<'a>(
    base: Option<&'a PluginRegistry>,
    target: Option<&'a PluginRegistry>,
    source: Option<&'a PluginRegistry>,
) -> Result<&'a PluginRegistry, MergeError> {
    let (Some(base), Some(target), Some(source)) = (base, target, source) else {
        return Err(MergeError::RegistryMismatch);
    };
    if base != target || base != source {
        return Err(MergeError::RegistryMismatch);
    }
    Ok(base)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistoricalInput {
    ValidAbsent,
    ValidNull,
    ValidTombstone,
    ValidValue,
    MissingCommit,
    MissingRoot,
    WrongKindRoot,
    MalformedRoot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistoricalResult {
    Absent,
    Null,
    Tombstone,
    Value,
    Corruption,
}

fn historical_point(input: HistoricalInput) -> HistoricalResult {
    match input {
        HistoricalInput::ValidAbsent => HistoricalResult::Absent,
        HistoricalInput::ValidNull => HistoricalResult::Null,
        HistoricalInput::ValidTombstone => HistoricalResult::Tombstone,
        HistoricalInput::ValidValue => HistoricalResult::Value,
        HistoricalInput::MissingCommit
        | HistoricalInput::MissingRoot
        | HistoricalInput::WrongKindRoot
        | HistoricalInput::MalformedRoot => HistoricalResult::Corruption,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReadTrace {
    read_id: u8,
    begin_reads: u8,
    fallback_reads: u8,
    cache_reads: u8,
}

fn require_one_retained_read(trace: ReadTrace) -> Result<(), MergeError> {
    (trace.read_id == 1
        && trace.begin_reads == 0
        && trace.fallback_reads == 0
        && trace.cache_reads == 0)
        .then_some(())
        .ok_or(MergeError::Corruption)
}

#[test]
fn disjoint_merge_and_same_identity_conflict_are_distinct() {
    let base = Snapshot::from([
        (Identity("a"), Cell::Value("base-a")),
        (Identity("b"), Cell::Value("base-b")),
    ]);
    let target = Snapshot::from([
        (Identity("a"), Cell::Value("target-a")),
        (Identity("b"), Cell::Value("base-b")),
    ]);
    let source = Snapshot::from([
        (Identity("a"), Cell::Value("base-a")),
        (Identity("b"), Cell::Value("source-b")),
    ]);
    let merged = merge_plan(&base, &target, &source).unwrap();
    assert_eq!(merged[&Identity("a")], Cell::Value("target-a"));
    assert_eq!(merged[&Identity("b")], Cell::Value("source-b"));

    let conflict = Snapshot::from([
        (Identity("a"), Cell::Value("source-a")),
        (Identity("b"), Cell::Value("base-b")),
    ]);
    assert_eq!(
        merge_plan(&base, &target, &conflict),
        Err(MergeError::Conflict(Identity("a")))
    );
}

#[test]
fn null_and_tombstone_are_not_absence_or_each_other() {
    let base = Snapshot::new();
    let target = Snapshot::from([(Identity("null"), Cell::Null)]);
    let source = Snapshot::from([(Identity("deleted"), Cell::Tombstone)]);
    let merged = merge_plan(&base, &target, &source).unwrap();
    assert_eq!(merged[&Identity("null")], Cell::Null);
    assert_eq!(merged[&Identity("deleted")], Cell::Tombstone);
    assert_eq!(historical_point(HistoricalInput::ValidAbsent), HistoricalResult::Absent);
    assert_eq!(historical_point(HistoricalInput::ValidNull), HistoricalResult::Null);
    assert_eq!(
        historical_point(HistoricalInput::ValidTombstone),
        HistoricalResult::Tombstone
    );
}

#[test]
fn branch_parent_chronology_and_checkpoint_floor_are_separate() {
    let commits = HashMap::from([
        ("R", Commit { id: "R", parents: vec![] }),
        ("H", Commit { id: "H", parents: vec!["R"] }),
        ("C", Commit { id: "C", parents: vec!["R"] }),
        ("S", Commit { id: "S", parents: vec!["H", "C"] }),
        ("T", Commit { id: "T", parents: vec!["C"] }),
    ]);
    assert_eq!(commits["S"].id, "S");
    assert_eq!(commits["S"].parents, vec!["H", "C"]);
    assert_eq!(merge_base(&commits, "S", "T"), Ok("C"));
    assert_eq!(merge_base(&commits, "S", "missing"), Err(MergeError::MissingCommit));
}

#[test]
fn plugin_registry_metadata_must_be_common_and_authenticated() {
    let base = PluginRegistry {
        generation: "blob-a",
        schema_keys: &["plugin_entity"],
    };
    let target = base.clone();
    let source = base.clone();
    assert_eq!(
        require_common_registry(Some(&base), Some(&target), Some(&source)),
        Ok(&base)
    );
    let changed = PluginRegistry {
        generation: "blob-b",
        schema_keys: &["plugin_entity"],
    };
    assert_eq!(
        require_common_registry(Some(&base), Some(&changed), Some(&source)),
        Err(MergeError::RegistryMismatch)
    );
    assert_eq!(
        require_common_registry(Some(&base), None, Some(&source)),
        Err(MergeError::RegistryMismatch)
    );
}

#[test]
fn corruption_and_stale_publication_fail_closed() {
    for input in [
        HistoricalInput::MissingCommit,
        HistoricalInput::MissingRoot,
        HistoricalInput::WrongKindRoot,
        HistoricalInput::MalformedRoot,
    ] {
        assert_eq!(historical_point(input), HistoricalResult::Corruption);
    }
    assert_eq!(
        historical_point(HistoricalInput::ValidValue),
        HistoricalResult::Value
    );
    assert_eq!(
        publish_if_fresh("T1", "T1"),
        Ok(())
    );
    assert_eq!(
        publish_if_fresh("T1", "T2"),
        Err(MergeError::StalePublication)
    );
}

fn publish_if_fresh(expected_target_head: &'static str, actual_target_head: &'static str) -> Result<(), MergeError> {
    (expected_target_head == actual_target_head)
        .then_some(())
        .ok_or(MergeError::StalePublication)
}

#[test]
fn cold_reopen_preserves_one_read_contract() {
    let trace = ReadTrace {
        read_id: 1,
        begin_reads: 0,
        fallback_reads: 0,
        cache_reads: 0,
    };
    assert_eq!(require_one_retained_read(trace), Ok(()));
    assert_eq!(
        require_one_retained_read(ReadTrace {
            read_id: 2,
            ..trace
        }),
        Err(MergeError::Corruption)
    );
    assert_eq!(
        require_one_retained_read(ReadTrace {
            fallback_reads: 1,
            ..trace
        }),
        Err(MergeError::Corruption)
    );
}
