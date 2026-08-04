//! Transaction-local ownership and coordinates for canonical Arrow state sets.

use std::collections::BTreeMap;

use crate::changelog::CommitId;
use crate::columnar_row_group::{ArrowStateSetId, RowGroupRowLocation};
use crate::tracked_state::{CommitDeltaReplacementScope, TrackedStateBaseCoordinate};

pub(crate) struct EntityColumnarWriteSets {
    sets: BTreeMap<
        (CommitId, CommitDeltaReplacementScope),
        crate::columnar_row_group::EncodedRowGroupSet,
    >,
    state_row_locations: Vec<Option<TrackedStateBaseCoordinate>>,
    replacement_row_locations: BTreeMap<(CommitId, String), Vec<TrackedStateBaseCoordinate>>,
    addressed_row_locations:
        BTreeMap<(CommitId, CommitDeltaReplacementScope, Vec<u8>), TrackedStateBaseCoordinate>,
}

impl EntityColumnarWriteSets {
    pub(crate) fn new() -> Self {
        Self {
            sets: BTreeMap::new(),
            state_row_locations: Vec::new(),
            replacement_row_locations: BTreeMap::new(),
            addressed_row_locations: BTreeMap::new(),
        }
    }

    pub(crate) fn with_state_row_count(row_count: usize) -> Self {
        Self {
            sets: BTreeMap::new(),
            state_row_locations: vec![None; row_count],
            replacement_row_locations: BTreeMap::new(),
            addressed_row_locations: BTreeMap::new(),
        }
    }

    pub(crate) fn get_scope(
        &self,
        commit_id: CommitId,
        scope: &CommitDeltaReplacementScope,
    ) -> Option<&crate::columnar_row_group::EncodedRowGroupSet> {
        self.sets.get(&(commit_id, scope.clone()))
    }

    pub(crate) fn get_unfiled(
        &self,
        commit_id: CommitId,
        schema_key: &str,
    ) -> Option<&crate::columnar_row_group::EncodedRowGroupSet> {
        self.get_scope(
            commit_id,
            &CommitDeltaReplacementScope {
                schema_key: schema_key.to_owned(),
                file_id: None,
            },
        )
    }

    pub(crate) fn insert_scope(
        &mut self,
        commit_id: CommitId,
        scope: CommitDeltaReplacementScope,
        value: crate::columnar_row_group::EncodedRowGroupSet,
    ) {
        self.sets.insert((commit_id, scope), value);
    }

    pub(crate) fn insert_unfiled(
        &mut self,
        commit_id: CommitId,
        schema_key: impl Into<String>,
        value: crate::columnar_row_group::EncodedRowGroupSet,
    ) {
        self.insert_scope(
            commit_id,
            CommitDeltaReplacementScope {
                schema_key: schema_key.into(),
                file_id: None,
            },
            value,
        );
    }

    pub(crate) fn insert_replacement(
        &mut self,
        key: (CommitId, String),
        value: crate::columnar_row_group::EncodedRowGroupSet,
        input_locations: Vec<RowGroupRowLocation>,
    ) {
        let state_set_id = value.id();
        let coordinates = input_locations
            .into_iter()
            .map(|location| TrackedStateBaseCoordinate {
                state_set_id,
                group_index: location.group_index,
                row_index: location.row_index,
            })
            .collect();
        self.replacement_row_locations
            .insert(key.clone(), coordinates);
        self.insert_unfiled(key.0, key.1, value);
    }

    pub(crate) fn replacement_row_location(
        &self,
        key: &(CommitId, String),
        row_index: usize,
    ) -> Option<TrackedStateBaseCoordinate> {
        self.replacement_row_locations
            .get(key)
            .and_then(|locations| locations.get(row_index))
            .copied()
    }

    pub(crate) fn set_state_row_location(
        &mut self,
        state_row_index: usize,
        state_set_id: ArrowStateSetId,
        location: RowGroupRowLocation,
    ) {
        self.state_row_locations[state_row_index] = Some(TrackedStateBaseCoordinate {
            state_set_id,
            group_index: location.group_index,
            row_index: location.row_index,
        });
    }

    pub(crate) fn state_row_location(
        &self,
        state_row_index: usize,
    ) -> Option<TrackedStateBaseCoordinate> {
        self.state_row_locations
            .get(state_row_index)
            .copied()
            .flatten()
    }

    pub(crate) fn set_addressed_row_location(
        &mut self,
        commit_id: CommitId,
        scope: CommitDeltaReplacementScope,
        encoded_key: Vec<u8>,
        state_set_id: ArrowStateSetId,
        location: RowGroupRowLocation,
    ) {
        self.addressed_row_locations.insert(
            (commit_id, scope, encoded_key),
            TrackedStateBaseCoordinate {
                state_set_id,
                group_index: location.group_index,
                row_index: location.row_index,
            },
        );
    }

    pub(crate) fn addressed_row_location(
        &self,
        commit_id: CommitId,
        scope: &CommitDeltaReplacementScope,
        encoded_key: &[u8],
    ) -> Option<TrackedStateBaseCoordinate> {
        self.addressed_row_locations
            .get(&(commit_id, scope.clone(), encoded_key.to_vec()))
            .copied()
    }
}
