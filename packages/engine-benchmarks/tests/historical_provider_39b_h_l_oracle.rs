//! Comprehensive H-L model: import the frozen H/I tests and add R2 J-L.
//!
//! This is pure std-only test code. It deliberately does not import production
//! types or execute the workspace.

#[path = "historical_provider_39b_correction_oracle.rs"]
#[allow(dead_code)]
mod frozen_h_i;

use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Payload {
    Missing,
    Null,
    Tombstone,
    Value(&'static str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequiredRowKind {
    File,
    Directory,
    Blob,
}

fn require_authenticated_payload(
    kind: RequiredRowKind,
    payload: Payload,
) -> Result<&'static str, String> {
    match payload {
        Payload::Value(value) => Ok(value),
        Payload::Missing => Err(format!("{kind:?} required payload is missing")),
        Payload::Null => Err(format!("{kind:?} required payload is NULL")),
        Payload::Tombstone => Err(format!("{kind:?} required payload is a tombstone")),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CertifiedRow {
    commit_id: &'static str,
    row_id: &'static str,
}

fn require_certified_commit(
    expected: &'static str,
    row: CertifiedRow,
) -> Result<CertifiedRow, String> {
    if row.commit_id != expected {
        return Err(format!(
            "certified row commit identity mismatch: expected {expected}, got {}",
            row.commit_id
        ));
    }
    Ok(row)
}

fn blocked_47957_certified_row(expected: &'static str, row: CertifiedRow) -> Option<CertifiedRow> {
    (row.commit_id == expected).then_some(row)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PathNode {
    parent: Option<&'static str>,
    name: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PathError {
    MissingDirectory(&'static str),
    MissingParent(&'static str),
    Cycle(&'static str),
}

fn resolve_typed_path(
    directory_id: &'static str,
    directories: &BTreeMap<&'static str, PathNode>,
    visiting: &mut Vec<&'static str>,
) -> Result<String, PathError> {
    if visiting.contains(&directory_id) {
        return Err(PathError::Cycle(directory_id));
    }
    let directory = directories
        .get(directory_id)
        .ok_or(PathError::MissingDirectory(directory_id))?;
    visiting.push(directory_id);
    let result = match directory.parent {
        Some(parent_id) if !directories.contains_key(parent_id) => {
            Err(PathError::MissingParent(parent_id))
        }
        Some(parent_id) => {
            let parent = resolve_typed_path(parent_id, directories, visiting)?;
            Ok(format!("{parent}/{}", directory.name))
        }
        None => Ok(directory.name.to_owned()),
    };
    visiting.pop();
    result
}

#[test]
fn required_file_directory_blob_payload_states_fail_closed() {
    let required_kinds = [
        RequiredRowKind::File,
        RequiredRowKind::Directory,
        RequiredRowKind::Blob,
    ];
    let invalid_payloads = [Payload::Missing, Payload::Null, Payload::Tombstone];
    for kind in required_kinds {
        for payload in invalid_payloads {
            assert!(
                require_authenticated_payload(kind, payload).is_err(),
                "{kind:?} must reject {payload:?}"
            );
        }
    }
    assert_eq!(
        require_authenticated_payload(RequiredRowKind::Blob, Payload::Value("blob-ref")),
        Ok("blob-ref")
    );
}

#[test]
fn certified_commit_identity_mismatch_errors_instead_of_skipping() {
    let row = CertifiedRow {
        commit_id: "wrong-commit",
        row_id: "event",
    };
    assert!(require_certified_commit("expected-commit", row).is_err());
    assert_eq!(blocked_47957_certified_row("expected-commit", row), None);
}

#[test]
fn missing_filesystem_parent_is_a_typed_error() {
    let directories = BTreeMap::from([(
        "file-dir",
        PathNode {
            parent: Some("missing-parent"),
            name: "file",
        },
    )]);
    assert_eq!(
        resolve_typed_path("file-dir", &directories, &mut Vec::new()),
        Err(PathError::MissingParent("missing-parent"))
    );
}

#[test]
fn filesystem_parent_cycle_is_a_typed_error() {
    let directories = BTreeMap::from([
        (
            "a",
            PathNode {
                parent: Some("b"),
                name: "a",
            },
        ),
        (
            "b",
            PathNode {
                parent: Some("a"),
                name: "b",
            },
        ),
    ]);
    assert_eq!(
        resolve_typed_path("a", &directories, &mut Vec::new()),
        Err(PathError::Cycle("a"))
    );
}
