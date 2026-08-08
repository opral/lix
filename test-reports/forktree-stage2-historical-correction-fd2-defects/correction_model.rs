use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
struct DescriptorExpectation {
    row_key: String,
    snapshot_id: String,
    descriptor_id: String,
    file_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Descriptor {
    row_key: String,
    snapshot_id: String,
    id: String,
    path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DescriptorCell {
    Live(Descriptor),
    Tombstone {
        row_key: String,
        snapshot_id: String,
        id: String,
    },
    Missing,
    Malformed,
    WrongKind {
        id: String,
    },
    IdentitySubstitution {
        key: String,
        payload_id: String,
    },
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
    BlobRowKey,
    BlobSnapshot,
    BlobDescriptor,
    BlobFileId,
    BlobReference,
    BlobKind,
    BlobSize,
    BlobId,
    PayloadMissing,
    MissingParent,
    ParentCycle,
    MarkerMismatch,
}

fn logical_descriptor(
    cell: DescriptorCell,
    expected: &DescriptorExpectation,
) -> Result<Option<Descriptor>, ModelError> {
    match cell {
        DescriptorCell::Live(descriptor)
            if descriptor.row_key == expected.row_key
                && descriptor.snapshot_id == expected.snapshot_id
                && descriptor.id == expected.descriptor_id
                && expected.descriptor_id == expected.file_id =>
        {
            Ok(Some(descriptor))
        }
        DescriptorCell::Live(_) | DescriptorCell::IdentitySubstitution { .. } => {
            Err(ModelError::IdentitySubstitution)
        }
        DescriptorCell::Tombstone {
            row_key,
            snapshot_id,
            id,
        } if row_key == expected.row_key
            && snapshot_id == expected.snapshot_id
            && id == expected.descriptor_id
            && expected.descriptor_id == expected.file_id =>
        {
            Ok(None)
        }
        DescriptorCell::Tombstone { .. } => Err(ModelError::IdentitySubstitution),
        DescriptorCell::Missing => Err(ModelError::MissingDescriptor),
        DescriptorCell::Malformed => Err(ModelError::MalformedDescriptor),
        DescriptorCell::WrongKind { .. } => Err(ModelError::WrongKind),
    }
}

fn logical_diff(
    before: DescriptorCell,
    after: DescriptorCell,
    expected: &DescriptorExpectation,
) -> Result<Vec<DiffEvent>, ModelError> {
    let before = logical_descriptor(before, expected)?;
    let after = logical_descriptor(after, expected)?;
    Ok(match (before, after) {
        (Some(_), None) => vec![DiffEvent::Removed(expected.file_id.clone())],
        (Some(_), Some(descriptor)) | (None, Some(descriptor)) => {
            vec![DiffEvent::Present(descriptor.path)]
        }
        (None, None) => Vec::new(),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BlobExpectation {
    row_key: String,
    snapshot_id: String,
    descriptor_id: String,
    file_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BlobRef {
    row_key: String,
    snapshot_id: String,
    descriptor_id: String,
    file_id: String,
    blob_id: String,
    declared_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BlobPayload {
    blob_id: String,
    declared_size: usize,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BlobCell {
    Live {
        refs: Vec<BlobRef>,
        payload: Option<BlobPayload>,
    },
    Tombstone,
    Missing,
    Malformed,
    WrongKind,
    IdentitySubstitution,
}

fn blob_id_for(bytes: &[u8]) -> String {
    let encoded = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("blob:{encoded}")
}

fn authenticated_ref(expectation: &BlobExpectation, bytes: &[u8]) -> (BlobRef, BlobPayload) {
    let blob_id = blob_id_for(bytes);
    let declared_size = bytes.len();
    (
        BlobRef {
            row_key: expectation.row_key.clone(),
            snapshot_id: expectation.snapshot_id.clone(),
            descriptor_id: expectation.descriptor_id.clone(),
            file_id: expectation.file_id.clone(),
            blob_id: blob_id.clone(),
            declared_size,
        },
        BlobPayload {
            blob_id,
            declared_size,
            bytes: bytes.to_vec(),
        },
    )
}

fn valid_blob(expectation: &BlobExpectation, bytes: &[u8]) -> BlobCell {
    let (blob_ref, payload) = authenticated_ref(expectation, bytes);
    BlobCell::Live {
        refs: vec![blob_ref],
        payload: Some(payload),
    }
}

fn validate_live_blob(
    cell: BlobCell,
    expected: &BlobExpectation,
    metadata_only: bool,
) -> Result<Option<Vec<u8>>, ModelError> {
    let BlobCell::Live { refs, payload } = cell else {
        return match cell {
            BlobCell::Tombstone => Ok(None),
            BlobCell::Missing => Err(ModelError::BlobReference),
            BlobCell::Malformed => Err(ModelError::BlobReference),
            BlobCell::WrongKind => Err(ModelError::BlobKind),
            BlobCell::IdentitySubstitution => Err(ModelError::BlobId),
            BlobCell::Live { .. } => unreachable!(),
        };
    };
    if refs.len() != 1 {
        return Err(ModelError::BlobCardinality);
    }
    let blob_ref = &refs[0];
    if blob_ref.row_key != expected.row_key {
        return Err(ModelError::BlobRowKey);
    }
    if blob_ref.snapshot_id != expected.snapshot_id {
        return Err(ModelError::BlobSnapshot);
    }
    if blob_ref.descriptor_id != expected.descriptor_id {
        return Err(ModelError::BlobDescriptor);
    }
    if blob_ref.file_id != expected.file_id {
        return Err(ModelError::BlobFileId);
    }
    if blob_ref.blob_id.is_empty() {
        return Err(ModelError::BlobReference);
    }
    let payload = payload.ok_or(ModelError::PayloadMissing)?;
    if blob_ref.declared_size != payload.declared_size
        || payload.declared_size != payload.bytes.len()
    {
        return Err(ModelError::BlobSize);
    }
    if payload.blob_id != blob_ref.blob_id || blob_id_for(&payload.bytes) != blob_ref.blob_id {
        return Err(ModelError::BlobId);
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
        let Some(parent) = parent else {
            return Ok(());
        };
        current = parent;
    }
}

fn descriptor_expectation() -> DescriptorExpectation {
    DescriptorExpectation {
        row_key: "row:file-1".into(),
        snapshot_id: "snapshot-1".into(),
        descriptor_id: "file-1".into(),
        file_id: "file-1".into(),
    }
}

fn blob_expectation() -> BlobExpectation {
    BlobExpectation {
        row_key: "blob-row:file-1".into(),
        snapshot_id: "snapshot-1".into(),
        descriptor_id: "file-1".into(),
        file_id: "file-1".into(),
    }
}

#[test]
fn tombstone_is_removal_and_descriptor_corruption_fails_closed() {
    let expected = descriptor_expectation();
    let live = DescriptorCell::Live(Descriptor {
        row_key: expected.row_key.clone(),
        snapshot_id: expected.snapshot_id.clone(),
        id: expected.descriptor_id.clone(),
        path: "/a".into(),
    });
    let tombstone = DescriptorCell::Tombstone {
        row_key: expected.row_key.clone(),
        snapshot_id: expected.snapshot_id.clone(),
        id: expected.descriptor_id.clone(),
    };
    assert_eq!(
        logical_diff(live.clone(), tombstone, &expected),
        Ok(vec![DiffEvent::Removed("file-1".into())])
    );
    for bad in [
        DescriptorCell::Missing,
        DescriptorCell::Malformed,
        DescriptorCell::WrongKind {
            id: "file-1".into(),
        },
        DescriptorCell::IdentitySubstitution {
            key: "row:file-1".into(),
            payload_id: "file-2".into(),
        },
        DescriptorCell::Live(Descriptor {
            row_key: "row:wrong".into(),
            snapshot_id: "snapshot-1".into(),
            id: "file-1".into(),
            path: "/a".into(),
        }),
    ] {
        assert!(logical_diff(live.clone(), bad, &expected).is_err());
    }
}

#[test]
fn live_blob_binds_every_identity_size_and_payload_field() {
    let expected = blob_expectation();
    let valid = valid_blob(&expected, b"content");
    assert_eq!(
        validate_live_blob(valid, &expected, false),
        Ok(Some(b"content".to_vec()))
    );

    let (valid_ref, valid_payload) = authenticated_ref(&expected, b"content");
    let mut wrong_row = valid_ref.clone();
    wrong_row.row_key = "blob-row:other".into();
    let mut wrong_snapshot = valid_ref.clone();
    wrong_snapshot.snapshot_id = "snapshot-other".into();
    let mut wrong_descriptor = valid_ref.clone();
    wrong_descriptor.descriptor_id = "file-other".into();
    let mut wrong_file = valid_ref.clone();
    wrong_file.file_id = "file-other".into();
    let mut wrong_blob_id = valid_ref.clone();
    wrong_blob_id.blob_id = "blob:substituted".into();
    let mut wrong_size = valid_ref.clone();
    wrong_size.declared_size += 1;
    let mut wrong_payload_size = valid_payload.clone();
    wrong_payload_size.declared_size += 1;
    let mut wrong_payload = valid_payload.clone();
    wrong_payload.bytes = b"different".to_vec();

    for bad in [
        BlobCell::Live {
            refs: Vec::new(),
            payload: None,
        },
        BlobCell::Live {
            refs: vec![valid_ref.clone(), valid_ref.clone()],
            payload: Some(valid_payload.clone()),
        },
        BlobCell::Live {
            refs: vec![wrong_row],
            payload: Some(valid_payload.clone()),
        },
        BlobCell::Live {
            refs: vec![wrong_snapshot],
            payload: Some(valid_payload.clone()),
        },
        BlobCell::Live {
            refs: vec![wrong_descriptor],
            payload: Some(valid_payload.clone()),
        },
        BlobCell::Live {
            refs: vec![wrong_file],
            payload: Some(valid_payload.clone()),
        },
        BlobCell::Live {
            refs: vec![wrong_blob_id],
            payload: Some(valid_payload.clone()),
        },
        BlobCell::Live {
            refs: vec![wrong_size],
            payload: Some(valid_payload.clone()),
        },
        BlobCell::Live {
            refs: vec![valid_ref.clone()],
            payload: Some(wrong_payload_size),
        },
        BlobCell::Live {
            refs: vec![valid_ref.clone()],
            payload: Some(wrong_payload),
        },
        BlobCell::Live {
            refs: vec![valid_ref.clone()],
            payload: None,
        },
        BlobCell::Missing,
        BlobCell::Malformed,
        BlobCell::WrongKind,
        BlobCell::IdentitySubstitution,
    ] {
        assert!(validate_live_blob(bad, &expected, true).is_err());
    }
}

#[test]
fn metadata_projection_cannot_bypass_blob_authentication() {
    let expected = blob_expectation();
    let (mut blob_ref, payload) = authenticated_ref(&expected, b"content");
    blob_ref.declared_size += 1;
    assert_eq!(
        validate_live_blob(
            BlobCell::Live {
                refs: vec![blob_ref],
                payload: Some(payload),
            },
            &expected,
            true,
        ),
        Err(ModelError::BlobSize)
    );
    assert_eq!(
        validate_live_blob(
            BlobCell::Live {
                refs: vec![authenticated_ref(&expected, b"content").0],
                payload: None,
            },
            &expected,
            true,
        ),
        Err(ModelError::PayloadMissing)
    );
    assert_eq!(
        validate_live_blob(BlobCell::Tombstone, &expected, true),
        Ok(None)
    );
}

#[test]
fn valid_empty_file_is_authenticated_and_transitions_to_tombstone() {
    let descriptor = descriptor_expectation();
    let blob = blob_expectation();
    assert_eq!(
        validate_live_blob(valid_blob(&blob, b""), &blob, false),
        Ok(Some(Vec::new()))
    );
    let live = DescriptorCell::Live(Descriptor {
        row_key: descriptor.row_key.clone(),
        snapshot_id: descriptor.snapshot_id.clone(),
        id: descriptor.descriptor_id.clone(),
        path: "/empty".into(),
    });
    let tombstone = DescriptorCell::Tombstone {
        row_key: descriptor.row_key.clone(),
        snapshot_id: descriptor.snapshot_id.clone(),
        id: descriptor.descriptor_id.clone(),
    };
    assert_eq!(
        logical_diff(live, tombstone, &descriptor),
        Ok(vec![DiffEvent::Removed("file-1".into())])
    );
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
    let expected = descriptor_expectation();
    let before = DescriptorCell::Live(Descriptor {
        row_key: expected.row_key.clone(),
        snapshot_id: expected.snapshot_id.clone(),
        id: expected.descriptor_id.clone(),
        path: "/f".into(),
    });
    let after = DescriptorCell::Tombstone {
        row_key: expected.row_key.clone(),
        snapshot_id: expected.snapshot_id.clone(),
        id: expected.descriptor_id.clone(),
    };
    let events = logical_diff(before, after, &expected).expect("authenticated view is valid");
    assert_eq!(events, vec![DiffEvent::Removed("file-1".into())]);
}
