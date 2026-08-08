#![allow(clippy::needless_pass_by_value)]

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Descriptor {
    id: String,
    path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DescriptorCell {
    Live(Descriptor),
    Tombstone { id: String },
    Missing,
    Malformed,
    WrongKind { id: String },
    IdentitySubstitution { key: String, payload_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DiffEvent {
    Removed(String),
    Present(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ModelError {
    MissingDescriptor,
    MalformedDescriptor,
    WrongKind,
    IdentitySubstitution,
    BlobCardinality,
    BlobIdentity,
    BlobReference,
    PayloadMissing,
    PayloadDigest,
    MissingParent,
    ParentCycle,
    MarkerMismatch,
}

fn logical_descriptor(cell: DescriptorCell, expected_id: &str) -> Result<Option<Descriptor>, ModelError> {
    match cell {
        DescriptorCell::Live(descriptor) if descriptor.id == expected_id => Ok(Some(descriptor)),
        DescriptorCell::Live(_) | DescriptorCell::IdentitySubstitution { .. } => {
            Err(ModelError::IdentitySubstitution)
        }
        DescriptorCell::Tombstone { id } if id == expected_id => Ok(None),
        DescriptorCell::Tombstone { .. } => Err(ModelError::IdentitySubstitution),
        DescriptorCell::Missing => Err(ModelError::MissingDescriptor),
        DescriptorCell::Malformed => Err(ModelError::MalformedDescriptor),
        DescriptorCell::WrongKind { .. } => Err(ModelError::WrongKind),
    }
}

fn logical_diff(
    before: DescriptorCell,
    after: DescriptorCell,
    expected_id: &str,
) -> Result<Vec<DiffEvent>, ModelError> {
    let before = logical_descriptor(before, expected_id)?;
    let after = logical_descriptor(after, expected_id)?;
    Ok(match (before, after) {
        (Some(_), None) => vec![DiffEvent::Removed(expected_id.to_string())],
        (Some(_), Some(descriptor)) | (None, Some(descriptor)) => {
            vec![DiffEvent::Present(descriptor.path)]
        }
        (None, None) => Vec::new(),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BlobRef {
    file_id: String,
    digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BlobPayload {
    digest: String,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BlobCell {
    Live {
        refs: Vec<BlobRef>,
        payload: Option<BlobPayload>,
    },
    Tombstone,
}

fn validate_live_blob(
    cell: BlobCell,
    file_id: &str,
    metadata_only: bool,
) -> Result<Option<Vec<u8>>, ModelError> {
    let BlobCell::Live { refs, payload } = cell else {
        return Ok(None);
    };
    if refs.len() != 1 {
        return Err(ModelError::BlobCardinality);
    }
    let blob_ref = &refs[0];
    if blob_ref.file_id != file_id {
        return Err(ModelError::BlobIdentity);
    }
    if blob_ref.digest.is_empty() {
        return Err(ModelError::BlobReference);
    }
    let payload = payload.ok_or(ModelError::PayloadMissing)?;
    if payload.digest != blob_ref.digest {
        return Err(ModelError::PayloadDigest);
    }
    Ok((!metadata_only).then_some(payload.bytes))
}

fn validate_chronology(
    marker_commit: &str,
    walked_commit: &str,
    parents: &BTreeMap<String, Vec<String>>,
) -> Result<(), ModelError> {
    if marker_commit != walked_commit {
        return Err(ModelError::MarkerMismatch);
    }
    let mut seen = BTreeSet::new();
    let mut current = walked_commit.to_string();
    loop {
        if !seen.insert(current.clone()) {
            return Err(ModelError::ParentCycle);
        }
        let parent = parents
            .get(&current)
            .ok_or(ModelError::MissingParent)?
            .first()
            .cloned();
        let Some(parent) = parent else { return Ok(()); };
        current = parent;
    }
}

#[test]
fn tombstone_is_removal_and_descriptor_corruption_fails_closed() {
    let live = DescriptorCell::Live(Descriptor {
        id: "file-1".into(),
        path: "/a".into(),
    });
    assert_eq!(
        logical_diff(
            live.clone(),
            DescriptorCell::Tombstone { id: "file-1".into() },
            "file-1",
        ),
        Ok(vec![DiffEvent::Removed("file-1".into())])
    );
    for bad in [
        DescriptorCell::Missing,
        DescriptorCell::Malformed,
        DescriptorCell::WrongKind { id: "file-1".into() },
        DescriptorCell::IdentitySubstitution {
            key: "file-1".into(),
            payload_id: "file-2".into(),
        },
    ] {
        assert!(logical_diff(live.clone(), bad, "file-1").is_err());
    }
}

#[test]
fn live_blob_requires_one_authenticated_ref_and_payload() {
    let valid = BlobCell::Live {
        refs: vec![BlobRef {
            file_id: "file-1".into(),
            digest: "digest-1".into(),
        }],
        payload: Some(BlobPayload {
            digest: "digest-1".into(),
            bytes: b"content".to_vec(),
        }),
    };
    assert_eq!(validate_live_blob(valid, "file-1", false), Ok(Some(b"content".to_vec())));
    for bad in [
        BlobCell::Live {
            refs: Vec::new(),
            payload: None,
        },
        BlobCell::Live {
            refs: vec![
                BlobRef { file_id: "file-1".into(), digest: "d1".into() },
                BlobRef { file_id: "file-1".into(), digest: "d2".into() },
            ],
            payload: None,
        },
        BlobCell::Live {
            refs: vec![BlobRef { file_id: "file-2".into(), digest: "d1".into() }],
            payload: Some(BlobPayload { digest: "d1".into(), bytes: vec![1] }),
        },
        BlobCell::Live {
            refs: vec![BlobRef { file_id: "file-1".into(), digest: "d1".into() }],
            payload: None,
        },
        BlobCell::Live {
            refs: vec![BlobRef { file_id: "file-1".into(), digest: "d1".into() }],
            payload: Some(BlobPayload { digest: "d2".into(), bytes: vec![1] }),
        },
    ] {
        assert!(validate_live_blob(bad, "file-1", true).is_err());
    }
}

#[test]
fn metadata_projection_cannot_bypass_blob_authentication() {
    let malformed_live = BlobCell::Live {
        refs: vec![BlobRef { file_id: "file-1".into(), digest: "d".into() }],
        payload: None,
    };
    assert_eq!(
        validate_live_blob(malformed_live, "file-1", true),
        Err(ModelError::PayloadMissing)
    );
    let tombstone = validate_live_blob(BlobCell::Tombstone, "file-1", true);
    assert_eq!(tombstone, Ok(None));
}

#[test]
fn chronology_requires_exact_marker_and_fail_closed_ancestry() {
    let mut parents = BTreeMap::new();
    parents.insert("C".into(), vec!["H".into()]);
    parents.insert("H".into(), Vec::new());
    assert_eq!(validate_chronology("C", "C", &parents), Ok(()));
    assert_eq!(
        validate_chronology("H", "C", &parents),
        Err(ModelError::MarkerMismatch)
    );
    let mut missing = parents.clone();
    missing.remove("H");
    assert_eq!(
        validate_chronology("C", "C", &missing),
        Err(ModelError::MissingParent)
    );
    parents.insert("H".into(), vec!["C".into()]);
    assert_eq!(
        validate_chronology("C", "C", &parents),
        Err(ModelError::ParentCycle)
    );
}

#[test]
fn corrected_path_has_one_view_and_no_publication_side_effect() {
    // The source correction is read-only: validation and logical diffing happen
    // on the caller-owned retained history view. This fixture makes accidental
    // fallback publication observable as a failed pure assertion.
    let before = DescriptorCell::Live(Descriptor { id: "f".into(), path: "/f".into() });
    let after = DescriptorCell::Tombstone { id: "f".into() };
    let events = logical_diff(before, after, "f").expect("authenticated view is valid");
    assert_eq!(events, vec![DiffEvent::Removed("f".into())]);
}
