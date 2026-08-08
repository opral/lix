//! Standalone semantic oracle for the checkpoint chronology discriminator.
//!
//! This file intentionally has no crate dependencies. It is compiled directly
//! by the report-only gate, so it remains runnable while the production
//! ForkTree landing is compiler-red. The successor review must bind the
//! production implementation to these exact expectations.

#[derive(Debug, Clone, PartialEq, Eq)]
struct Marker {
    branch_id: &'static str,
    commit_id: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Commit {
    id: &'static str,
    parent: Option<&'static str>,
    marker: Option<Marker>,
}

fn classify(
    commits: &[Commit],
    selected_branch: &str,
) -> Result<(Vec<&'static str>, Option<&'static str>), &'static str> {
    let implicit_root = commits
        .iter()
        .find(|commit| commit.parent.is_none())
        .map(|commit| commit.id);
    let mut marker_ids = Vec::new();
    for commit in commits {
        let Some(marker) = &commit.marker else {
            continue;
        };
        if marker.branch_id != selected_branch {
            return Err("marker_branch_must_equal_selected_branch");
        }
        if marker.commit_id != commit.id {
            return Err("marker_commit_must_equal_observed_commit");
        }
        if marker_ids.contains(&commit.id) {
            return Err("duplicate_marker_for_observed_commit");
        }
        marker_ids.push(commit.id);
    }
    Ok((marker_ids, implicit_root))
}

#[test]
fn checkpoint_to_ordinary_commit_classifies_only_exact_markers() {
    let commits = [
        Commit {
            id: "commit-R",
            parent: None,
            marker: None,
        },
        Commit {
            id: "commit-C",
            parent: Some("commit-R"),
            marker: Some(Marker {
                branch_id: "branch-A",
                commit_id: "commit-C",
            }),
        },
        Commit {
            id: "commit-O",
            parent: Some("commit-C"),
            marker: None,
        },
        Commit {
            id: "commit-D",
            parent: Some("commit-O"),
            marker: Some(Marker {
                branch_id: "branch-A",
                commit_id: "commit-D",
            }),
        },
    ];
    let (markers, implicit_root) = classify(&commits, "branch-A").expect("valid chronology");
    assert_eq!(markers, ["commit-C", "commit-D"]);
    assert_eq!(implicit_root, Some("commit-R"));
    assert!(!markers.contains(&"commit-O"));
}

#[test]
fn marker_from_another_commit_fails_closed() {
    let commits = [
        Commit {
            id: "commit-R",
            parent: None,
            marker: None,
        },
        Commit {
            id: "commit-C",
            parent: Some("commit-R"),
            marker: Some(Marker {
                branch_id: "branch-A",
                commit_id: "commit-O",
            }),
        },
    ];
    assert_eq!(
        classify(&commits, "branch-A"),
        Err("marker_commit_must_equal_observed_commit")
    );
}

#[test]
fn marker_from_another_branch_fails_closed() {
    let commits = [
        Commit {
            id: "commit-R",
            parent: None,
            marker: None,
        },
        Commit {
            id: "commit-C",
            parent: Some("commit-R"),
            marker: Some(Marker {
                branch_id: "branch-B",
                commit_id: "commit-C",
            }),
        },
    ];
    assert_eq!(
        classify(&commits, "branch-A"),
        Err("marker_branch_must_equal_selected_branch")
    );
}
