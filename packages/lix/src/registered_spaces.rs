//! Public handles for every registered storage space.
//!
//! Benchmarks, qualification harnesses and adapter suites live outside this
//! crate but must still name the spaces the engine writes. Before this module
//! existed the only way to do that was to repeat the space *name* as a string
//! literal, and a space name carries the record encoding version, so it is
//! *expected* to churn: `branch.head_control.v10` became `v11` in `76834a1c9`
//! when `untracked_generation` left the packed record.
//!
//! A repeated literal turns that ordinary bump into a runtime break in
//! whichever code path happens to run, and the break is not always loud.
//! Measured against the 31 pinned literals that existed when this module was
//! added, a stale name had three outcomes:
//!
//! - it panicked (`storage_space_by_name`, `packed_history_scale::find_layout`);
//! - it silently disarmed a predicate, so a test passed vacuously or timed out
//!   somewhere unrelated (the server-protocol branch-control gate, which read
//!   as "the branch switch never issued its authoritative read");
//! - **it silently reported zero.** `small_blob_cas` looked its spaces up with
//!   `.find(..).map_or(0, ..)`, so a renamed space did not fail — the bench
//!   reported `0 rows / 0 bytes` as a measurement.
//!
//! Reading the handle instead of repeating the name removes the failure rather
//! than detecting it: there is no longer a second copy of the name to go stale,
//! so a rename propagates on rebuild and there is nothing to update.
//!
//! Prefer the handle's `.id` wherever the caller only needs *identity* — "is
//! this the branch-control space?". The id is the first four bytes of every
//! physical key and does not move across an encoding bump (`0x0004_0020`
//! survived `v10` -> `v11` untouched), whereas the name is versioned by design.
//!
//! This module is a *view* of [`crate::storage_spaces::ALL_STORAGE_SPACES`],
//! never a second authority for it:
//! [`tests::the_published_handles_are_exactly_the_registry`] pins the two to
//! each other, so a space added to the registry is unpublished until it is
//! added here, and a handle cannot drift from the row it re-exports.

use crate::storage_adapter::StorageSpace;

pub const JSON_SPACE: StorageSpace = crate::json_store::JSON_SPACE;
pub const TRACKED_STATE_TREE_CHUNK_SPACE: StorageSpace =
    crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE;
pub const REPOSITORY_PROTOCOL_SPACE: StorageSpace = crate::init::REPOSITORY_PROTOCOL_SPACE;
pub const TRACKED_STATE_CHANGE_LOCATOR_SPACE: StorageSpace =
    crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE;
pub const TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE: StorageSpace =
    crate::tracked_state::TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE;
pub const HOT_ROW_SPACE: StorageSpace = crate::hot_state::ROW_SPACE;
pub const HOT_FILE_SPACE: StorageSpace = crate::hot_state::FILE_SPACE;
pub const HOT_DIFF_SPACE: StorageSpace = crate::hot_state::DIFF_SPACE;
pub const TRACKED_WORKING_DIFF_MARKER_SPACE: StorageSpace =
    crate::hot_state::TRACKED_WORKING_DIFF_MARKER_SPACE;
pub const CERTIFIED_ROW_BATCH_SPACE: StorageSpace = crate::hot_state::CERTIFIED_ROW_BATCH_SPACE;
pub const BRANCH_HEAD_CONTROL_SPACE: StorageSpace = crate::branch::BRANCH_HEAD_CONTROL_SPACE;
pub const CERTIFIED_ROW_BATCH_MANIFEST_SPACE: StorageSpace =
    crate::hot_state::CERTIFIED_ROW_BATCH_MANIFEST_SPACE;
pub const CERTIFIED_ROW_BATCH_PAGE_SPACE: StorageSpace =
    crate::hot_state::CERTIFIED_ROW_BATCH_PAGE_SPACE;
pub const HOT_COLLECTION_CONTROL_SPACE: StorageSpace = crate::hot_state::COLLECTION_CONTROL_SPACE;
pub const PACKED_CURRENT_BASE_SPACE: StorageSpace = crate::hot_state::PACKED_CURRENT_BASE_SPACE;
pub const PACKED_CURRENT_BASE_CONTROL_SPACE: StorageSpace =
    crate::hot_state::PACKED_CURRENT_BASE_CONTROL_SPACE;
pub const PLUGIN_CHECKPOINT_SPACE: StorageSpace = crate::transaction::PLUGIN_CHECKPOINT_SPACE;
pub const PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE: StorageSpace =
    crate::hot_state::PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE;
pub const ROOT_CURRENT_BASE_SPACE: StorageSpace = crate::hot_state::ROOT_CURRENT_BASE_SPACE;
pub const ROW_GROUP_MANIFEST_SPACE: StorageSpace =
    crate::columnar_row_group::ROW_GROUP_MANIFEST_SPACE;
pub const ROW_GROUP_COLUMN_SPACE: StorageSpace = crate::columnar_row_group::ROW_GROUP_COLUMN_SPACE;
pub const TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE: StorageSpace =
    crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE;
pub const TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE: StorageSpace =
    crate::tracked_state::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE;
pub const MUTATION_DIRECTORY_NODE_SPACE: StorageSpace =
    crate::tracked_state::MUTATION_DIRECTORY_NODE_SPACE;
pub const TRACKED_STATE_COMMIT_HISTORY_DEFERRED_SPACE: StorageSpace =
    crate::tracked_state::TRACKED_STATE_COMMIT_HISTORY_DEFERRED_SPACE;
pub const CURRENT_STATE_DATA_PART_SPACE: StorageSpace =
    crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE;
pub const CURRENT_STATE_DATA_PART_REFS_SPACE: StorageSpace =
    crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE;
pub const SCOPED_RANGE_NODE_SPACE: StorageSpace = crate::tracked_state::SCOPED_RANGE_NODE_SPACE;
/// Declared-column access path over the hot rows. Disposable, and genuinely
/// reclaimed with its generation: `INDEX_SPACE` is the first entry in
/// `GENERATION_SCOPED_SPACES` (`hot_state/tracked_head/hot.rs`), so
/// `stage_retire_hot_generation` deletes every entry *and* witness under the
/// retired `(branch_id, generation)` prefix, and the sole writer of this plane
/// (`stage_hot_index_entries`, called from `transaction/commit.rs`) is not
/// reached on any lifecycle republication route. Tombstones do not change this:
/// a deleted row never mints an index entry at all, because the extractor runs
/// only in the live-snapshot arm of transaction validation.
///
/// "Derived from the hot rows" is the one imprecise part, so do not rely on it:
/// nothing reconstructs this plane *from* `ROW_SPACE`. Entries are extracted
/// from snapshot JSON during transaction validation and carried on the prepared
/// batch, so the plane is rebuildable from canonical records but not from the
/// hot rows it indexes. Once a generation is retired, the surviving hot rows
/// cannot rebuild it.
pub const HOT_INDEX_SPACE: StorageSpace = crate::hot_state::INDEX_SPACE;
pub const BINARY_CAS_MANIFEST_SPACE: StorageSpace = crate::binary_cas::BINARY_CAS_MANIFEST_SPACE;
pub const BINARY_CAS_MANIFEST_CHUNK_SPACE: StorageSpace =
    crate::binary_cas::BINARY_CAS_MANIFEST_CHUNK_SPACE;
pub const BINARY_CAS_CHUNK_SPACE: StorageSpace = crate::binary_cas::BINARY_CAS_CHUNK_SPACE;
pub const BINARY_CAS_CHUNK_PRESENCE_SPACE: StorageSpace =
    crate::binary_cas::BINARY_CAS_CHUNK_PRESENCE_SPACE;
pub const BINARY_CAS_CHUNK_DEMAND_SPACE: StorageSpace =
    crate::binary_cas::BINARY_CAS_CHUNK_DEMAND_SPACE;
pub const COMMIT_SPACE: StorageSpace = crate::changelog::COMMIT_SPACE;
pub const CHANGE_SPACE: StorageSpace = crate::changelog::CHANGE_SPACE;
pub const REVISION_SPACE: StorageSpace = crate::storage_adapter::REVISION_SPACE;
pub const EXECUTE_IDEMPOTENCY_RECEIPT_SPACE: StorageSpace =
    crate::session::EXECUTE_IDEMPOTENCY_RECEIPT_SPACE;
pub const UPLOAD_STATE_SPACE: StorageSpace = crate::session::UPLOAD_STATE_SPACE;
pub const UPLOAD_MANIFEST_LEAF_SPACE: StorageSpace = crate::session::UPLOAD_MANIFEST_LEAF_SPACE;
pub const SYNC_SEQUENCE_SPACE: StorageSpace = crate::sync::repository::SYNC_SEQUENCE_SPACE;
pub const SYNC_REPOSITORY_EVENT_SPACE: StorageSpace =
    crate::sync::repository::SYNC_REPOSITORY_EVENT_SPACE;
pub const SYNC_REPLICA_STATE_SPACE: StorageSpace =
    crate::sync::repository::SYNC_REPLICA_STATE_SPACE;
pub const CHECKPOINT_RECOVERY_REF_SPACE: StorageSpace = crate::gc::CHECKPOINT_RECOVERY_REF_SPACE;
pub const CHECKPOINT_GC_STATE_SPACE: StorageSpace = crate::gc::CHECKPOINT_GC_STATE_SPACE;

#[cfg(test)]
mod tests {
    use super::*;

    /// Every handle above, in registry order.
    ///
    /// Stated a second time on purpose: this is the list the test compares
    /// against the registry, and writing it out is what makes "the module
    /// publishes the whole registry" a checked claim rather than a comment.
    const PUBLISHED: &[StorageSpace] = &[
        JSON_SPACE,
        TRACKED_STATE_TREE_CHUNK_SPACE,
        REPOSITORY_PROTOCOL_SPACE,
        TRACKED_STATE_CHANGE_LOCATOR_SPACE,
        TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
        HOT_ROW_SPACE,
        HOT_FILE_SPACE,
        HOT_DIFF_SPACE,
        TRACKED_WORKING_DIFF_MARKER_SPACE,
        CERTIFIED_ROW_BATCH_SPACE,
        BRANCH_HEAD_CONTROL_SPACE,
        CERTIFIED_ROW_BATCH_MANIFEST_SPACE,
        CERTIFIED_ROW_BATCH_PAGE_SPACE,
        HOT_COLLECTION_CONTROL_SPACE,
        PACKED_CURRENT_BASE_SPACE,
        PACKED_CURRENT_BASE_CONTROL_SPACE,
        PLUGIN_CHECKPOINT_SPACE,
        PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
        ROOT_CURRENT_BASE_SPACE,
        ROW_GROUP_MANIFEST_SPACE,
        ROW_GROUP_COLUMN_SPACE,
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
        MUTATION_DIRECTORY_NODE_SPACE,
        TRACKED_STATE_COMMIT_HISTORY_DEFERRED_SPACE,
        CURRENT_STATE_DATA_PART_SPACE,
        CURRENT_STATE_DATA_PART_REFS_SPACE,
        SCOPED_RANGE_NODE_SPACE,
        HOT_INDEX_SPACE,
        BINARY_CAS_MANIFEST_SPACE,
        BINARY_CAS_MANIFEST_CHUNK_SPACE,
        BINARY_CAS_CHUNK_SPACE,
        BINARY_CAS_CHUNK_PRESENCE_SPACE,
        BINARY_CAS_CHUNK_DEMAND_SPACE,
        COMMIT_SPACE,
        CHANGE_SPACE,
        REVISION_SPACE,
        EXECUTE_IDEMPOTENCY_RECEIPT_SPACE,
        UPLOAD_STATE_SPACE,
        UPLOAD_MANIFEST_LEAF_SPACE,
        SYNC_SEQUENCE_SPACE,
        SYNC_REPOSITORY_EVENT_SPACE,
        SYNC_REPLICA_STATE_SPACE,
        CHECKPOINT_RECOVERY_REF_SPACE,
        CHECKPOINT_GC_STATE_SPACE,
    ];

    /// The published handles must be the registry, exactly and in order.
    ///
    /// A partial re-export would recreate the failure `storage_spaces.rs` was
    /// written to end: a hand-maintained subset that drifts from the registry
    /// on its own schedule. Comparing the whole slice also catches a handle
    /// pointed at the wrong constant, because the row carries the id, name and
    /// value semantics together.
    #[test]
    fn the_published_handles_are_exactly_the_registry() {
        assert_eq!(
            PUBLISHED,
            crate::storage_spaces::ALL_STORAGE_SPACES,
            "the published space handles drifted from ALL_STORAGE_SPACES; \
             add the new space here, or point the stale handle back at its \
             registry row",
        );
    }
}
