//! Standalone source model for the Cut B correction oracle.
//! This file is intentionally not compiled or run by the report-only gate.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReadId(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DescriptorId(u64);

struct OperationOwner {
    retained_read: ReadId,
}

struct BranchDescriptor<'a> {
    owner: &'a OperationOwner,
    descriptor: DescriptorId,
}

impl OperationOwner {
    fn branch(&self, descriptor: DescriptorId) -> BranchDescriptor<'_> {
        BranchDescriptor {
            owner: self,
            descriptor,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum CursorError {
    CrossDescriptor,
}

struct Cursor {
    descriptor: DescriptorId,
}

fn resume(descriptor: &BranchDescriptor<'_>, cursor: &Cursor) -> Result<(), CursorError> {
    if descriptor.descriptor != cursor.descriptor {
        return Err(CursorError::CrossDescriptor);
    }
    let _same_read = descriptor.owner.retained_read;
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
enum RegistryRow {
    Missing,
    ExplicitBootstrapEmpty,
    Present,
}

#[derive(Debug, Eq, PartialEq)]
enum Corruption {
    MissingSelectedRegistry,
    InvalidBlobRefIdentity,
}

fn load_registry(row: RegistryRow) -> Result<bool, Corruption> {
    match row {
        RegistryRow::Missing => Err(Corruption::MissingSelectedRegistry),
        RegistryRow::ExplicitBootstrapEmpty => Ok(false),
        RegistryRow::Present => Ok(true),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SemanticBlobRow {
    id: String,
    blob_hash: String,
    size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BlobRefJson {
    id: String,
    blob_hash: String,
    size: u64,
}

fn authenticate_blob_ref(row: &SemanticBlobRow, json: &BlobRefJson) -> Result<(), Corruption> {
    if row.id != json.id || row.blob_hash != json.blob_hash || row.size != json.size {
        return Err(Corruption::InvalidBlobRefIdentity);
    }
    Ok(())
}

#[cfg(test)]
mod discriminators {
    use super::*;

    #[test]
    fn branch_descriptors_share_one_read_but_cursors_cannot_cross() {
        let owner = OperationOwner {
            retained_read: ReadId(7),
        };
        let left = owner.branch(DescriptorId(1));
        let right = owner.branch(DescriptorId(2));
        assert_eq!(left.owner.retained_read, right.owner.retained_read);
        assert_eq!(resume(&left, &Cursor { descriptor: DescriptorId(2) }), Err(CursorError::CrossDescriptor));
    }

    #[test]
    fn missing_registry_is_not_bootstrap_empty() {
        assert_eq!(load_registry(RegistryRow::Missing), Err(Corruption::MissingSelectedRegistry));
        assert_eq!(load_registry(RegistryRow::ExplicitBootstrapEmpty), Ok(false));
    }

    #[test]
    fn same_size_remapped_blob_ref_fails() {
        let row = SemanticBlobRow {
            id: "row-a".to_owned(),
            blob_hash: "blob-a".to_owned(),
            size: 4096,
        };
        let remapped = BlobRefJson {
            id: "row-b".to_owned(),
            blob_hash: "blob-b".to_owned(),
            size: 4096,
        };
        assert_eq!(authenticate_blob_ref(&row, &remapped), Err(Corruption::InvalidBlobRefIdentity));
    }
}
