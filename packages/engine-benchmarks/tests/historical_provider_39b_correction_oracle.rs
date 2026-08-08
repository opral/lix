//! Pure, dependency-free acceptance model for the blocked 39b historical-provider head.
//!
//! This file deliberately models the required correction contract instead of importing
//! production types. It is safe to compile with `rustc --test` without building or
//! executing the production workspace.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
struct CommitNode {
    id: &'static str,
    parent: Option<&'static str>,
    marker: Option<&'static str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AuthenticatedRow {
    commit_id: &'static str,
    row_id: &'static str,
    payload_id: Option<&'static str>,
    certified: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Projection {
    commit_created_at: bool,
}

#[derive(Clone, Debug)]
struct ForkTreeView {
    commits: BTreeMap<&'static str, CommitNode>,
    rows: Vec<AuthenticatedRow>,
}

impl ForkTreeView {
    fn reachable(&self, head: &'static str) -> Result<Vec<&'static str>, String> {
        let mut current = Some(head);
        let mut seen = BTreeSet::new();
        let mut ordered = Vec::new();
        while let Some(id) = current {
            if !seen.insert(id) {
                return Err(format!("cycle in authenticated commit topology at {id}"));
            }
            let node = self
                .commits
                .get(id)
                .ok_or_else(|| format!("missing authenticated commit topology node {id}"))?;
            if node.id.is_empty() || node.id != id {
                return Err(format!("malformed authenticated commit node {id}"));
            }
            if let Some(marker) = node.marker {
                if !self.commits.contains_key(marker) {
                    return Err(format!("marker references missing commit {marker}"));
                }
            }
            if let Some(parent) = node.parent {
                if !self.commits.contains_key(parent) {
                    return Err(format!("commit {id} references missing parent {parent}"));
                }
            }
            ordered.push(id);
            current = node.parent;
        }
        Ok(ordered)
    }

    fn authenticated_history(
        &self,
        head: &'static str,
        _projection: Projection,
    ) -> Result<Vec<AuthenticatedRow>, String> {
        let reachable = self.reachable(head)?.into_iter().collect::<BTreeSet<_>>();
        let mut result = Vec::new();
        for row in &self.rows {
            if !reachable.contains(row.commit_id) {
                continue;
            }
            if row.payload_id.is_some_and(|payload| payload != row.row_id) {
                return Err(format!(
                    "payload identity mismatch for authenticated row {}",
                    row.row_id
                ));
            }
            result.push(row.clone());
        }
        result.sort_by_key(|row| (row.commit_id, row.row_id));
        Ok(result)
    }

    fn blocked_47957_history(
        &self,
        head: &'static str,
        projection: Projection,
    ) -> Result<Vec<AuthenticatedRow>, String> {
        let reachable = if projection.commit_created_at {
            self.reachable(head)?.into_iter().collect::<BTreeSet<_>>()
        } else {
            BTreeSet::new()
        };
        let mut result = self
            .rows
            .iter()
            .filter(|row| !row.certified || reachable.contains(row.commit_id))
            .cloned()
            .collect::<Vec<_>>();
        result.sort_by_key(|row| (row.commit_id, row.row_id));
        Ok(result)
    }

    fn checkpoint_history(&self, head: &'static str) -> Result<Vec<&'static str>, String> {
        let reachable = self.reachable(head)?;
        let mut selected = Vec::new();
        for id in reachable {
            let node = &self.commits[id];
            if node.parent.is_none() || node.marker == Some(node.id) {
                selected.push(id);
            }
        }
        Ok(selected)
    }
}

#[derive(Default, Debug, PartialEq, Eq)]
struct ChronologyOwner {
    retained_views: usize,
    chronology_walks: usize,
}

fn run_checkpoint_and_working_diff(
    view: &ForkTreeView,
    head: &'static str,
    owner: &mut ChronologyOwner,
) -> Result<(Vec<&'static str>, &'static str), String> {
    owner.retained_views += 1;
    owner.chronology_walks += 1;
    let checkpoints = view.checkpoint_history(head)?;
    let baseline = checkpoints
        .first()
        .copied()
        .ok_or_else(|| "no authenticated checkpoint baseline".to_owned())?;
    Ok((checkpoints, baseline))
}

fn fixture() -> ForkTreeView {
    ForkTreeView {
        commits: BTreeMap::from([
            (
                "root",
                CommitNode {
                    id: "root",
                    parent: None,
                    marker: None,
                },
            ),
            (
                "checkpoint",
                CommitNode {
                    id: "checkpoint",
                    parent: Some("root"),
                    marker: Some("checkpoint"),
                },
            ),
            (
                "ordinary",
                CommitNode {
                    id: "ordinary",
                    parent: Some("checkpoint"),
                    marker: Some("checkpoint"),
                },
            ),
        ]),
        rows: vec![
            AuthenticatedRow {
                commit_id: "ordinary",
                row_id: "plugin-event",
                payload_id: Some("plugin-event"),
                certified: false,
            },
            AuthenticatedRow {
                commit_id: "checkpoint",
                row_id: "certified-plugin-event",
                payload_id: Some("certified-plugin-event"),
                certified: true,
            },
        ],
    }
}

#[test]
fn metadata_omission_keeps_authenticated_certified_rows() {
    let view = fixture();
    let projection = Projection {
        commit_created_at: false,
    };
    let expected = view.authenticated_history("ordinary", projection).unwrap();
    let blocked = view.blocked_47957_history("ordinary", projection).unwrap();
    assert_eq!(expected.len(), 2);
    assert_eq!(expected.iter().filter(|row| row.certified).count(), 1);
    assert_ne!(
        blocked, expected,
        "47957 omission must be detected by the oracle"
    );
}

#[test]
fn projecting_commit_metadata_keeps_same_authenticated_rows() {
    let view = fixture();
    let without = view
        .authenticated_history(
            "ordinary",
            Projection {
                commit_created_at: false,
            },
        )
        .unwrap();
    let with = view
        .authenticated_history(
            "ordinary",
            Projection {
                commit_created_at: true,
            },
        )
        .unwrap();
    assert_eq!(without, with);
}

#[test]
fn missing_or_malformed_topology_fails_closed() {
    let mut missing = fixture();
    missing.commits.get_mut("ordinary").unwrap().parent = Some("gone");
    assert!(missing
        .authenticated_history(
            "ordinary",
            Projection {
                commit_created_at: false,
            },
        )
        .is_err());

    let mut malformed = fixture();
    malformed.commits.get_mut("ordinary").unwrap().marker = Some("gone");
    assert!(malformed.checkpoint_history("ordinary").is_err());
}

#[test]
fn payload_identity_is_bound_to_authenticated_row_key() {
    let mut view = fixture();
    view.rows[1].payload_id = Some("substituted-payload");
    assert!(view
        .authenticated_history(
            "ordinary",
            Projection {
                commit_created_at: false,
            },
        )
        .is_err());
}

#[test]
fn marker_must_equal_walked_commit_and_root_is_implicit() {
    let view = fixture();
    assert_eq!(
        view.checkpoint_history("ordinary").unwrap(),
        vec!["checkpoint", "root"]
    );

    let mut wrong = fixture();
    wrong.commits.get_mut("ordinary").unwrap().marker = Some("checkpoint");
    assert!(!wrong
        .checkpoint_history("ordinary")
        .unwrap()
        .contains(&"ordinary"));
}

#[test]
fn checkpoint_and_working_diff_share_one_retained_chronology_view() {
    let view = fixture();
    let mut owner = ChronologyOwner::default();
    let (checkpoints, baseline) =
        run_checkpoint_and_working_diff(&view, "ordinary", &mut owner).unwrap();
    assert_eq!(checkpoints, vec!["checkpoint", "root"]);
    assert_eq!(baseline, "checkpoint");
    assert_eq!(
        owner,
        ChronologyOwner {
            retained_views: 1,
            chronology_walks: 1
        }
    );
}

fn main() {
    println!("historical_provider_39b_correction_oracle=MODEL_ONLY");
    println!("metadata_omission_certified_rows=REQUIRED");
    println!("missing_malformed_topology=FAIL_CLOSED");
    println!("checkpoint_marker_identity_and_root=REQUIRED");
    println!("checkpoint_working_diff_view_reads=1");
}
