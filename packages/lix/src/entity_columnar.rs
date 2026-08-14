//! Entity-specific contract layered over generic immutable columnar row groups.

use std::collections::BTreeMap;

use crate::changelog::CommitId;
use crate::columnar_row_group::{RowGroupRowLocation, RowGroupSetId};

pub(crate) const ENTITY_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY: &str =
    "lix.entity_columnar.lossless_snapshot.v1";
pub(crate) const ENTITY_COLUMNAR_TYPED_HISTORY_METADATA_KEY: &str =
    "lix.entity_columnar.typed_history.v1";
pub(crate) const ENTITY_COLUMNAR_ENTITY_PK_FIELD: &str = "lixcol_entity_pk";
pub(crate) const ENTITY_COLUMNAR_DELETED_FIELD: &str = "lixcol_is_deleted";

pub(crate) fn entity_identity_column_index(
    manifest: &crate::columnar_row_group::RowGroupManifest,
) -> Option<usize> {
    (manifest
        .metadata
        .get(ENTITY_COLUMNAR_TYPED_HISTORY_METADATA_KEY)
        .map(String::as_str)
        == Some("true"))
    .then(|| manifest.fields.len().checked_sub(1))
    .flatten()
    .filter(|&index| manifest.fields[index].name == ENTITY_COLUMNAR_ENTITY_PK_FIELD)
}

pub(crate) struct EntityColumnarWriteSets {
    sets: BTreeMap<(CommitId, String), crate::columnar_row_group::EncodedRowGroupSet>,
    state_row_locations: StateRowLocations,
    typed_history_snapshots: Vec<Option<crate::changelog::TypedHistorySnapshot>>,
}

enum StateRowLocations {
    None,
    Dense { row_count: usize },
    Explicit(Vec<Option<RowGroupRowLocation>>),
}

impl EntityColumnarWriteSets {
    pub(crate) fn new() -> Self {
        Self {
            sets: BTreeMap::new(),
            state_row_locations: StateRowLocations::None,
            typed_history_snapshots: Vec::new(),
        }
    }

    pub(crate) fn with_state_row_count(row_count: usize) -> Self {
        Self {
            sets: BTreeMap::new(),
            state_row_locations: StateRowLocations::Explicit(vec![None; row_count]),
            typed_history_snapshots: vec![None; row_count],
        }
    }

    pub(crate) fn with_dense_state_rows(row_count: usize) -> Self {
        Self {
            sets: BTreeMap::new(),
            state_row_locations: StateRowLocations::Dense { row_count },
            typed_history_snapshots: vec![None; row_count],
        }
    }

    pub(crate) fn covers_state_rows(&self, state_row_indices: &[usize]) -> bool {
        state_row_indices
            .iter()
            .all(|&index| self.state_row_location(index).is_some())
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
                panic!("explicit entity row location requires an explicit location column")
            }
        }
    }

    pub(crate) fn set_typed_history_snapshot(
        &mut self,
        state_row_index: usize,
        snapshot: crate::changelog::TypedHistorySnapshot,
    ) {
        self.typed_history_snapshots[state_row_index] = Some(snapshot);
    }

    pub(crate) fn typed_history_snapshot(
        &self,
        state_row_index: usize,
    ) -> Option<&crate::changelog::TypedHistorySnapshot> {
        self.typed_history_snapshots
            .get(state_row_index)
            .and_then(Option::as_ref)
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

pub(crate) fn entity_row_group_set_id(commit_id: CommitId, schema_key: &str) -> RowGroupSetId {
    let mut digest = blake3::Hasher::new();
    digest.update(b"lix.entity_columnar.v1");
    digest.update(commit_id.as_uuid().as_bytes());
    digest.update(&(schema_key.len() as u64).to_be_bytes());
    digest.update(schema_key.as_bytes());
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest.finalize().as_bytes()[..16]);
    RowGroupSetId::new(id)
}
