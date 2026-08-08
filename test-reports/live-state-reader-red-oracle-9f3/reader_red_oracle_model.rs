//! Dependency-free, intentionally red model for the 9f3 reader boundary.
//!
//! This file is report/test-only and is not included by Cargo. It models the
//! observable distinction that the old reader loses: an unsupported lane must
//! be an error, not an empty current-state batch.

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Rows(&'static [&'static str]),
    EmptySuccess,
    TypedError,
}
#[derive(Debug, PartialEq, Eq)]
struct Request {
    schema_keys: &'static [&'static str],
    untracked: bool,
    complex: bool,
}

fn old_9f3_scan(request: &Request) -> Outcome {
    if request.untracked || request.complex {
        return Outcome::TypedError;
    }
    let has_current_row = request
        .schema_keys
        .iter()
        .any(|schema| *schema == "app.row");
    if has_current_row {
        Outcome::Rows(&["app.row"])
    } else {
        // Mirrors forktree_reader.rs:50-57 + :94: schema filtering followed
        // by a successful MaterializedLiveStateBatch::from_rows(empty).
        Outcome::EmptySuccess
    }
}

fn corrected_scan(request: &Request) -> Outcome {
    let derived_or_history = request.schema_keys.iter().any(|schema| {
        matches!(*schema, "lix_commit" | "lix_commit_edge" | "lix_branch")
    });
    if request.untracked || request.complex || derived_or_history {
        Outcome::TypedError
    } else if request.schema_keys == ["app.row"] {
        Outcome::Rows(&["app.row"])
    } else {
        Outcome::TypedError
    }
}

fn old_9f3_exact(request: &Request) -> Option<&'static str> {
    match old_9f3_scan(request) {
        Outcome::Rows(rows) => rows.first().copied(),
        Outcome::EmptySuccess => None,
        Outcome::TypedError => None,
    }
}

#[test]
fn red_derived_schema_is_not_a_legitimate_empty_exact_slot() {
    let request = Request {
        schema_keys: &["lix_commit"],
        untracked: false,
        complex: false,
    };
    assert_eq!(corrected_scan(&request), Outcome::TypedError);
    assert_ne!(old_9f3_scan(&request), Outcome::TypedError);
    assert!(old_9f3_exact(&request).is_some(), "RED: old exact path becomes None");
}

#[test]
fn red_mixed_schema_cannot_return_current_rows_or_empty() {
    let request = Request {
        schema_keys: &["app.row", "lix_commit"],
        untracked: false,
        complex: false,
    };
    assert_eq!(corrected_scan(&request), Outcome::TypedError);
    assert_ne!(old_9f3_scan(&request), Outcome::TypedError);
}

#[test]
fn explicit_untracked_and_complex_controls_remain_errors() {
    for request in [
        Request {
            schema_keys: &["app.row"],
            untracked: true,
            complex: false,
        },
        Request {
            schema_keys: &["app.row"],
            untracked: false,
            complex: true,
        },
    ] {
        assert_eq!(old_9f3_scan(&request), Outcome::TypedError);
        assert_eq!(corrected_scan(&request), Outcome::TypedError);
    }
}
