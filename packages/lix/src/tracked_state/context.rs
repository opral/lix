//! Neutral tracked-state context retained only for callers that still carry
//! the historical dependency-cascade helper. The deleted implementation was
//! a second tree/storage authority; current reads and writes belong to
//! ForkTree/transaction publication and are intentionally not reintroduced.

use std::collections::BTreeSet;

use crate::LixError;
use crate::tracked_state::{TrackedStateIndexValue, TrackedStateKey};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";

/// Returns file identities whose descriptor tombstone requires dependent
/// payload rows to be retired. This is semantic transaction planning logic;
/// it does not read or write a tracked-state physical owner.
pub(crate) fn descriptor_dependency_cascade_file_ids(
    target_delta: &[(TrackedStateKey, TrackedStateIndexValue)],
) -> Result<Vec<String>, LixError> {
    let mut file_ids = BTreeSet::new();
    for (key, value) in target_delta {
        if key.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY || !value.deleted {
            continue;
        }
        let file_id = key.entity_pk.as_single_string_owned().map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("file descriptor tombstone has invalid identity: {error}"),
            )
        })?;
        file_ids.insert(file_id);
    }
    Ok(file_ids.into_iter().collect())
}
