#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Commit<'a> {
    id: &'a str,
    parent: Option<&'a str>,
    marker_capable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Marker<'a> {
    commit_id: &'a str,
    branch: &'a str,
}

fn select_checkpoint_ids<'a>(
    chain: &[Commit<'a>],
    markers: &[Marker<'a>],
    branch: &str,
) -> Result<Vec<&'a str>, &'static str> {
    let mut selected = Vec::new();
    for marker in markers {
        if marker.branch != branch {
            return Err("checkpoint marker branch mismatch");
        }
        if !chain
            .iter()
            .any(|commit| commit.id == marker.commit_id && commit.marker_capable)
        {
            return Err("checkpoint marker commit mismatch");
        }
        if selected.contains(&marker.commit_id) {
            return Err("duplicate checkpoint marker");
        }
        selected.push(marker.commit_id);
    }
    for commit in chain {
        if commit.parent.is_none() {
            selected.push(commit.id);
        }
    }
    Ok(selected)
}

#[test]
fn exact_marker_and_implicit_root_exclude_ordinary_commit() {
    let chain = [
        Commit { id: "ordinary", parent: Some("checkpoint"), marker_capable: false },
        Commit { id: "checkpoint", parent: Some("root"), marker_capable: true },
        Commit { id: "root", parent: None, marker_capable: false },
    ];
    assert_eq!(
        select_checkpoint_ids(&chain, &[Marker { commit_id: "checkpoint", branch: "A" }], "A"),
        Ok(vec!["checkpoint", "root"])
    );
}

#[test]
fn wrong_commit_is_not_selected_as_a_marker() {
    let chain = [
        Commit { id: "ordinary", parent: Some("checkpoint"), marker_capable: false },
        Commit { id: "checkpoint", parent: Some("root"), marker_capable: true },
        Commit { id: "root", parent: None, marker_capable: false },
    ];
    assert_eq!(
        select_checkpoint_ids(&chain, &[Marker { commit_id: "ordinary", branch: "A" }], "A"),
        Err("checkpoint marker commit mismatch")
    );
}

#[test]
fn ordinary_commit_after_checkpoint_is_not_selected() {
    let chain = [
        Commit { id: "ordinary", parent: Some("checkpoint"), marker_capable: false },
        Commit { id: "checkpoint", parent: Some("root"), marker_capable: true },
        Commit { id: "root", parent: None, marker_capable: false },
    ];
    assert!(!select_checkpoint_ids(
        &chain,
        &[Marker { commit_id: "checkpoint", branch: "A" }],
        "A",
    )
        .expect("valid marker")
        .contains(&"ordinary"));
}

#[test]
fn wrong_branch_marker_fails_closed() {
    let chain = [Commit { id: "checkpoint", parent: None, marker_capable: true }];
    assert_eq!(
        select_checkpoint_ids(&chain, &[Marker { commit_id: "checkpoint", branch: "B" }], "A"),
        Err("checkpoint marker branch mismatch")
    );
}

#[test]
fn duplicate_marker_fails_closed() {
    let chain = [
        Commit { id: "checkpoint-a", parent: Some("root"), marker_capable: true },
        Commit { id: "checkpoint-b", parent: Some("checkpoint-a"), marker_capable: true },
        Commit { id: "root", parent: None, marker_capable: false },
    ];
    assert_eq!(
        select_checkpoint_ids(
            &chain,
            &[
                Marker { commit_id: "checkpoint-a", branch: "A" },
                Marker { commit_id: "checkpoint-a", branch: "A" },
            ],
            "A",
        ),
        Err("duplicate checkpoint marker")
    );
}
