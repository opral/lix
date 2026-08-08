// TEST/REPORT-ONLY pure merge model for the exact b59 migration contract.
// It is intentionally independent of Lix production modules and is not run in
// this source-only package.

use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
enum State {
    Null,
    Value(&'static str),
    Tombstone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Row {
    state: State,
    metadata: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Added,
    Modified,
    Removed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Change {
    key: String,
    kind: Kind,
    before: Option<Row>,
    after: Row,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Conflict {
    key: String,
    target: Change,
    source: Change,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Plan {
    source_picks: Vec<Change>,
    conflicts: Vec<Conflict>,
}

fn valid_key(key: &str) -> bool {
    !key.is_empty() && !key.contains('\0')
}

fn equal_state(left: &Row, right: &Row) -> bool {
    left == right
}

fn classify(base: Option<&Row>, side: Option<&Row>) -> Option<Kind> {
    match (base, side) {
        (None, Some(_)) => Some(Kind::Added),
        (Some(_), None) => Some(Kind::Removed),
        (Some(before), Some(after)) if !equal_state(before, after) => {
            if after.state == State::Tombstone {
                Some(Kind::Removed)
            } else {
                Some(Kind::Modified)
            }
        }
        _ => None,
    }
}

fn analyze(
    base: &BTreeMap<String, Row>,
    target: &BTreeMap<String, Row>,
    source: &BTreeMap<String, Row>,
) -> Result<Plan, &'static str> {
    for key in base.keys().chain(target.keys()).chain(source.keys()) {
        if !valid_key(key) {
            return Err("malformed identity");
        }
    }

    let mut keys = base.keys().cloned().collect::<Vec<_>>();
    keys.extend(target.keys().cloned());
    keys.extend(source.keys().cloned());
    keys.sort();
    keys.dedup();

    let mut source_picks = Vec::new();
    let mut conflicts = Vec::new();
    for key in keys {
        let base_row = base.get(&key);
        let target_row = target.get(&key);
        let source_row = source.get(&key);
        let target_kind = classify(base_row, target_row);
        let source_kind = classify(base_row, source_row);

        let Some(source_kind) = source_kind else {
            continue;
        };
        let source_change = Change {
            key: key.clone(),
            kind: source_kind,
            before: base_row.cloned(),
            after: source_row.cloned().ok_or("source removal lacks tombstone")?,
        };

        match target_kind {
            None => source_picks.push(source_change),
            Some(_) => {
                let target_change = Change {
                    key: key.clone(),
                    kind: target_kind.expect("target kind exists"),
                    before: base_row.cloned(),
                    after: target_row.cloned().ok_or("target removal lacks tombstone")?,
                };
                if !equal_state(&target_change.after, &source_change.after) {
                    conflicts.push(Conflict {
                        key,
                        target: target_change,
                        source: source_change,
                    });
                }
            }
        }
    }

    Ok(Plan {
        source_picks,
        conflicts,
    })
}

fn row(state: State, metadata: Option<&'static str>) -> Row {
    Row { state, metadata }
}

fn main() {
    let mut base = BTreeMap::new();
    base.insert("a".to_owned(), row(State::Value("base"), Some("plugin-v1")));
    base.insert("b".to_owned(), row(State::Value("base"), None));
    base.insert("n".to_owned(), row(State::Null, None));

    let mut target = base.clone();
    target.insert("a".to_owned(), row(State::Value("target"), Some("plugin-v2")));
    target.insert("b".to_owned(), row(State::Tombstone, None));

    let mut source = base.clone();
    source.insert("a".to_owned(), row(State::Value("source"), Some("plugin-v3")));
    source.insert("b".to_owned(), row(State::Value("source"), None));
    source.insert("c".to_owned(), row(State::Value("added"), Some("plugin-new")));

    let plan = analyze(&base, &target, &source).expect("valid model");
    assert_eq!(plan.source_picks.iter().map(|change| change.key.as_str()).collect::<Vec<_>>(), vec!["c"]);
    assert_eq!(plan.conflicts.iter().map(|conflict| conflict.key.as_str()).collect::<Vec<_>>(), vec!["a", "b"]);
    assert_eq!(plan.conflicts[0].target.kind, Kind::Modified);
    assert_eq!(plan.conflicts[0].source.kind, Kind::Modified);
    assert_eq!(plan.conflicts[1].target.kind, Kind::Removed);
    assert_eq!(plan.conflicts[1].source.kind, Kind::Modified);

    let mut disjoint_source = BTreeMap::new();
    disjoint_source.insert("c".to_owned(), row(State::Value("added"), None));
    let disjoint = analyze(&base, &target, &disjoint_source).expect("valid disjoint model");
    assert_eq!(disjoint.source_picks.len(), 1);
    assert!(disjoint.conflicts.is_empty());

    let mut tombstone_source = base.clone();
    tombstone_source.insert("n".to_owned(), row(State::Tombstone, None));
    let tombstone = analyze(&base, &base, &tombstone_source).expect("tombstone model");
    assert_eq!(tombstone.source_picks[0].kind, Kind::Removed);
    assert_eq!(tombstone.source_picks[0].before.as_ref().unwrap().state, State::Null);

    let mut malformed = BTreeMap::new();
    malformed.insert(String::new(), row(State::Value("bad"), None));
    assert_eq!(analyze(&base, &base, &malformed), Err("malformed identity"));
}
