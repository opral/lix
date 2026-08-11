//! The authoritative registry of every physical storage space.
//!
//! A space id is the first four bytes of every physical key
//! (`storage_adapter/spaces.rs`), so the set of space ids *is* the on-disk key
//! layout: ids must be unique, and their numeric order is the order backends
//! sort and group the data.
//!
//! Before this registry existed, three hand-maintained subsets stood in for it
//! — the id-uniqueness test, the packed-history ordering test, and the bench
//! layout catalog — and each drifted independently as spaces were added. Every
//! layout invariant now iterates [`ALL_STORAGE_SPACES`], so a new space is
//! covered by all of them the moment it is registered, and an unregistered
//! space is caught by [`tests::every_declared_space_is_registered`].

use crate::storage_adapter::StorageSpace;
#[cfg(test)]
use crate::storage_adapter::StorageSpaceId;

/// Every storage space a repository can physically contain, in space-id order.
///
/// Adding a space to the engine without adding it here is a layout bug: the
/// registry is what the uniqueness, ordering, and bench-layout invariants are
/// checked against.
pub(crate) const ALL_STORAGE_SPACES: &[StorageSpace] = &[
    crate::json_store::JSON_SPACE,
    crate::json_store::UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE,
    crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
    crate::init::REPOSITORY_PROTOCOL_SPACE,
    crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
    crate::tracked_state::TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
    crate::live_state::HOT_ROW_SPACE,
    crate::live_state::HOT_FILE_SPACE,
    crate::live_state::HOT_DIFF_SPACE,
    crate::live_state::TRACKED_WORKING_DIFF_MARKER_SPACE,
    crate::live_state::CERTIFIED_ENTITY_BATCH_SPACE,
    crate::branch::BRANCH_HEAD_CONTROL_SPACE,
    crate::live_state::CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE,
    crate::live_state::CERTIFIED_ENTITY_BATCH_PAGE_SPACE,
    crate::live_state::HOT_COLLECTION_CONTROL_SPACE,
    crate::live_state::PACKED_CURRENT_BASE_SPACE,
    crate::live_state::PACKED_CURRENT_BASE_CONTROL_SPACE,
    crate::transaction::PLUGIN_CHECKPOINT_SPACE,
    crate::live_state::PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
    crate::live_state::ROOT_CURRENT_BASE_SPACE,
    crate::columnar_row_group::ROW_GROUP_MANIFEST_SPACE,
    crate::columnar_row_group::ROW_GROUP_COLUMN_SPACE,
    crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
    crate::tracked_state::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
    crate::tracked_state::MUTATION_DIRECTORY_NODE_SPACE,
    crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
    crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE,
    crate::tracked_state::SCOPED_RANGE_NODE_SPACE,
    crate::binary_cas::BINARY_CAS_MANIFEST_SPACE,
    crate::binary_cas::BINARY_CAS_MANIFEST_CHUNK_SPACE,
    crate::binary_cas::BINARY_CAS_CHUNK_SPACE,
    crate::binary_cas::BINARY_CAS_CHUNK_PRESENCE_SPACE,
    crate::changelog::COMMIT_SPACE,
    crate::changelog::CHANGE_SPACE,
    crate::changelog::COMMIT_CHANGE_ID_SPACE,
    crate::storage_adapter::REVISION_SPACE,
    crate::session::EXECUTE_IDEMPOTENCY_RECEIPT_SPACE,
    crate::session::UPLOAD_STATE_SPACE,
    crate::session::UPLOAD_MANIFEST_LEAF_SPACE,
    crate::gc::CHECKPOINT_RECOVERY_REF_SPACE,
    crate::gc::CHECKPOINT_GC_STATE_SPACE,
    crate::gc::GC_REACHABILITY_DELTA_SPACE,
    crate::gc::GC_REACHABILITY_QUEUE_SPACE,
    crate::gc::GC_TREE_SWEEP_EPOCH_SPACE,
    crate::gc::GC_TREE_SWEEP_MARK_SPACE,
    crate::gc::GC_TREE_SWEEP_CURSOR_SPACE,
];

/// Space ids that belonged to spaces this protocol has cut.
///
/// There is no compatibility reader for them; they are listed so the registry
/// tests refuse to hand a retired id to a new space, which would silently
/// reinterpret predecessor bytes left in an existing repository.
#[cfg(test)]
pub(crate) const RETIRED_STORAGE_SPACE_IDS: &[StorageSpaceId] = &[
    // untracked_state.row.v1
    StorageSpaceId(0x0001_0002),
    // live_state.index.branch_root.v1
    StorageSpaceId(0x0004_0005),
];

/// The first live-row space id.
///
/// Live-row spaces are the point-read surface for current state. The ordering
/// invariant below is expressed relative to this boundary.
#[cfg(test)]
pub(crate) const FIRST_LIVE_ROW_SPACE_ID: StorageSpaceId = StorageSpaceId(0x0004_001b);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::ValueSemantics;
    use std::collections::BTreeMap;

    /// The registry must list spaces in ascending id order, because that order
    /// is the physical key order every layout argument reasons about. Reading
    /// the registry top to bottom is reading the disk left to right.
    #[test]
    fn registry_is_in_physical_key_order() {
        for pair in ALL_STORAGE_SPACES.windows(2) {
            assert!(
                pair[0].id.0 < pair[1].id.0,
                "registry is out of physical key order: {} (0x{:08x}) precedes {} (0x{:08x})",
                pair[0].name,
                pair[0].id.0,
                pair[1].name,
                pair[1].id.0,
            );
        }
    }

    /// Two spaces sharing an id interleave their keys in one physical range,
    /// so each would scan and range-delete the other's rows.
    #[test]
    fn every_space_id_is_unique_and_not_retired() {
        let mut seen = BTreeMap::new();
        for space in ALL_STORAGE_SPACES {
            if let Some(existing) = seen.insert(space.id.0, space.name) {
                panic!(
                    "storage space id 0x{:08x} is used by both {existing} and {}",
                    space.id.0, space.name,
                );
            }
            assert!(
                !RETIRED_STORAGE_SPACE_IDS.contains(&space.id),
                "{} reuses retired storage space id 0x{:08x}",
                space.name,
                space.id.0,
            );
        }
    }

    /// Space names are what layout accounting, diagnostics, and the storage
    /// layout tools key on, so a duplicate name silently merges two planes in
    /// every report.
    #[test]
    fn every_space_name_is_unique() {
        let mut seen = BTreeMap::new();
        for space in ALL_STORAGE_SPACES {
            assert_eq!(
                seen.insert(space.name, space.id.0),
                None,
                "storage space name {} is used by more than one space",
                space.name,
            );
        }
    }

    /// The one ordering rule that is physically load-bearing: a mutable space
    /// that grows without bound on every commit must sort below the live-row
    /// spaces, so ordinary current-state point reads are never wedged between
    /// two unbounded planes in the same RocksDB column family.
    ///
    /// This is deliberately scoped to mutable spaces. RocksDB routes
    /// `ValueSemantics::Immutable` to a separate column family
    /// (`rocksdb.rs:253-265`), so an immutable space cannot share an SST with
    /// the live rows whatever its id; asserting an ordering across that
    /// boundary asserts nothing.
    #[test]
    fn unbounded_mutable_planes_sort_below_the_live_row_spaces() {
        for space in [
            crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
            crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
        ] {
            assert_eq!(
                space.value_semantics,
                ValueSemantics::Mutable,
                "{space} is no longer a mutable plane; revisit this invariant",
            );
            assert!(
                space.id.0 < FIRST_LIVE_ROW_SPACE_ID.0,
                "{space} would put an unbounded plane above the live-row spaces",
            );
        }
        for space in [
            crate::live_state::HOT_ROW_SPACE,
            crate::live_state::HOT_FILE_SPACE,
            crate::live_state::HOT_DIFF_SPACE,
        ] {
            assert!(
                space.id.0 >= FIRST_LIVE_ROW_SPACE_ID.0,
                "{space} is a live-row space but sorts below the live-row boundary",
            );
        }
    }

    /// Catches the failure mode this registry exists to prevent: a space
    /// declared somewhere in the engine but never registered, and therefore
    /// invisible to every invariant above and to layout accounting.
    ///
    /// The source scan is intentionally crude — it only has to notice that a
    /// `StorageSpace::mutable`/`::immutable` constructor exists with an id the
    /// registry does not know.
    #[test]
    fn every_declared_space_is_registered() {
        // `storage/conformance/model_based.rs` declares one space for the
        // storage model oracle. It never reaches a repository.
        const TEST_ONLY_SPACE_IDS: &[u32] = &[0x0102_0304];
        let registered = ALL_STORAGE_SPACES
            .iter()
            .map(|space| space.id.0)
            .chain(RETIRED_STORAGE_SPACE_IDS.iter().map(|id| id.0))
            .chain(TEST_ONLY_SPACE_IDS.iter().copied())
            .collect::<std::collections::BTreeSet<_>>();
        let mut unregistered = Vec::new();
        for (path, source) in engine_sources() {
            for id in declared_space_ids(&source) {
                if !registered.contains(&id) {
                    unregistered.push(format!("0x{id:08x} in {path}"));
                }
            }
        }
        assert!(
            unregistered.is_empty(),
            "storage spaces are declared but missing from ALL_STORAGE_SPACES: {}",
            unregistered.join(", "),
        );
    }

    fn engine_sources() -> Vec<(String, String)> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut sources = Vec::new();
        let mut pending = vec![root];
        while let Some(directory) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&directory) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|extension| extension == "rs")
                    && let Ok(source) = std::fs::read_to_string(&path)
                {
                    sources.push((path.display().to_string(), source));
                }
            }
        }
        assert!(!sources.is_empty(), "engine sources should be readable");
        sources
    }

    /// Extracts the id of every `StorageSpace::mutable(StorageSpaceId(0x..)`
    /// or `::immutable(...)` constructor in one source file.
    fn declared_space_ids(source: &str) -> Vec<u32> {
        let mut ids = Vec::new();
        for constructor in ["StorageSpace::mutable(", "StorageSpace::immutable("] {
            let mut rest = source;
            while let Some(offset) = rest.find(constructor) {
                rest = &rest[offset + constructor.len()..];
                // Every declaration writes its id literal immediately after
                // the constructor. Bounding the window keeps a constructor
                // that does not carry one from stealing an unrelated id.
                let window = &rest[..rest.len().min(96)];
                let Some(literal) = window
                    .split_once("SpaceId(0x")
                    .map(|(_, tail)| tail)
                    .and_then(|tail| tail.split(')').next())
                else {
                    continue;
                };
                if let Ok(id) = u32::from_str_radix(&literal.replace('_', ""), 16) {
                    ids.push(id);
                }
            }
        }
        ids
    }
}
