//! Storage identities for derived entity columnar row groups.

use std::collections::BTreeMap;

use crate::changelog::CommitId;
use crate::columnar_row_group::{RowGroupRowLocation, RowGroupSetId};

pub(crate) struct EntityColumnarWriteSets {
    sets: BTreeMap<(CommitId, String), crate::columnar_row_group::EncodedRowGroupSet>,
    state_row_locations: Vec<Option<RowGroupRowLocation>>,
}

impl EntityColumnarWriteSets {
    pub(crate) fn new() -> Self {
        Self {
            sets: BTreeMap::new(),
            state_row_locations: Vec::new(),
        }
    }

    pub(crate) fn with_state_row_count(row_count: usize) -> Self {
        Self {
            sets: BTreeMap::new(),
            state_row_locations: vec![None; row_count],
        }
    }

    pub(crate) fn get(
        &self,
        key: &(CommitId, String),
    ) -> Option<&crate::columnar_row_group::EncodedRowGroupSet> {
        self.sets.get(key)
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
        self.state_row_locations[state_row_index] = Some(location);
    }

    pub(crate) fn state_row_location(&self, state_row_index: usize) -> Option<RowGroupRowLocation> {
        self.state_row_locations
            .get(state_row_index)
            .copied()
            .flatten()
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
