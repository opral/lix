//! Row-specific contract layered over generic immutable columnar row groups.

use std::collections::BTreeMap;

use crate::changelog::CommitId;
use crate::columnar_row_group::{RowGroupRowLocation, RowGroupSetId};

pub(crate) const ROW_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY: &str =
    "lix.row_columnar.lossless_snapshot.v1";
// Persisted in immutable v1 row-group manifests. This is a private physical
// field name, not a column in any SQL surface, and cannot be renamed without a
// storage-format migration.
pub(crate) const ROW_COLUMNAR_IDENTITY_FIELD: &str = "lixcol_row_pk";

pub(crate) fn row_identity_column_index(
    manifest: &crate::columnar_row_group::RowGroupManifest,
) -> Option<usize> {
    (manifest
        .metadata
        .get(ROW_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY)
        .map(String::as_str)
        == Some("true"))
    .then(|| manifest.fields.len().checked_sub(1))
    .flatten()
    .filter(|&index| manifest.fields[index].name == ROW_COLUMNAR_IDENTITY_FIELD)
}

pub(crate) struct RowColumnarWriteSets {
    sets: BTreeMap<(CommitId, String), crate::columnar_row_group::EncodedRowGroupSet>,
    state_row_locations: StateRowLocations,
}

enum StateRowLocations {
    None,
    Dense { row_count: usize },
    Explicit(Vec<Option<RowGroupRowLocation>>),
}

impl RowColumnarWriteSets {
    pub(crate) fn new() -> Self {
        Self {
            sets: BTreeMap::new(),
            state_row_locations: StateRowLocations::None,
        }
    }

    pub(crate) fn with_state_row_count(row_count: usize) -> Self {
        Self {
            sets: BTreeMap::new(),
            state_row_locations: StateRowLocations::Explicit(vec![None; row_count]),
        }
    }

    pub(crate) fn with_dense_state_rows(row_count: usize) -> Self {
        Self {
            sets: BTreeMap::new(),
            state_row_locations: StateRowLocations::Dense { row_count },
        }
    }

    pub(crate) fn dense_state_row_count(&self) -> Option<usize> {
        match &self.state_row_locations {
            StateRowLocations::Dense { row_count } => Some(*row_count),
            StateRowLocations::None | StateRowLocations::Explicit(_) => None,
        }
    }

    pub(crate) fn get(
        &self,
        key: &(CommitId, String),
    ) -> Option<&crate::columnar_row_group::EncodedRowGroupSet> {
        self.sets.get(key)
    }

    pub(crate) fn take(
        &mut self,
        key: &(CommitId, String),
    ) -> Option<crate::columnar_row_group::EncodedRowGroupSet> {
        self.sets.remove(key)
    }

    pub(crate) fn insert(
        &mut self,
        key: (CommitId, String),
        value: crate::columnar_row_group::EncodedRowGroupSet,
    ) {
        self.sets.insert(key, value);
    }

    pub(crate) fn set_state_row_location(
        &mut self,
        state_row_index: usize,
        location: RowGroupRowLocation,
    ) {
        match &mut self.state_row_locations {
            StateRowLocations::Explicit(locations) => {
                locations[state_row_index] = Some(location);
            }
            StateRowLocations::None | StateRowLocations::Dense { .. } => {
                panic!("explicit row location requires an explicit location column")
            }
        }
    }

    pub(crate) fn state_row_location(&self, state_row_index: usize) -> Option<RowGroupRowLocation> {
        match &self.state_row_locations {
            StateRowLocations::None => None,
            StateRowLocations::Dense { row_count } if state_row_index < *row_count => {
                Some(RowGroupRowLocation {
                    group_index: u32::try_from(
                        state_row_index / crate::columnar_row_group::ROW_GROUP_MAX_ROWS,
                    )
                    .ok()?,
                    row_index: u32::try_from(
                        state_row_index % crate::columnar_row_group::ROW_GROUP_MAX_ROWS,
                    )
                    .ok()?,
                })
            }
            StateRowLocations::Dense { .. } => None,
            StateRowLocations::Explicit(locations) => {
                locations.get(state_row_index).copied().flatten()
            }
        }
    }
}

pub(crate) fn row_group_set_id(commit_id: CommitId, schema_key: &str) -> RowGroupSetId {
    let mut digest = blake3::Hasher::new();
    digest.update(b"lix.row_columnar.v1");
    digest.update(commit_id.as_uuid().as_bytes());
    digest.update(&(schema_key.len() as u64).to_be_bytes());
    digest.update(schema_key.as_bytes());
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest.finalize().as_bytes()[..16]);
    RowGroupSetId::new(id)
}
