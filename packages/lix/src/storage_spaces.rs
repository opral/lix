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
//!
//! This module is compiled in **every** configuration, not just test and
//! bench ones. `StorageSpace::mutable`/`::immutable` check the id they are
//! handed against [`may_declare`], and those constructors exist in every
//! build, so a registry that came and went with a feature flag would be a
//! guard on the test harness rather than on the shipped crate. Nothing here
//! costs anything at run time: it is two `const` tables and two `const fn`
//! predicates over crate constants that already exist unconditionally.

use crate::storage_adapter::{StorageSpace, StorageSpaceId, ValueSemantics};

/// Every storage space a repository can physically contain, in space-id order.
///
/// Adding a space to the engine without adding it here is a layout bug: the
/// registry is what the uniqueness, ordering, and bench-layout invariants are
/// checked against.
pub(crate) const ALL_STORAGE_SPACES: &[StorageSpace] = &[
    crate::json_store::JSON_SPACE,
    crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
    crate::init::REPOSITORY_PROTOCOL_SPACE,
    crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
    crate::tracked_state::TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
    crate::hot_state::ROW_SPACE,
    crate::hot_state::FILE_SPACE,
    crate::hot_state::DIFF_SPACE,
    crate::hot_state::TRACKED_WORKING_DIFF_MARKER_SPACE,
    crate::branch::BRANCH_HEAD_CONTROL_SPACE,
    crate::hot_state::COLLECTION_CONTROL_SPACE,
    crate::hot_state::PACKED_CURRENT_BASE_SPACE,
    crate::hot_state::PACKED_CURRENT_BASE_CONTROL_SPACE,
    crate::hot_state::PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
    crate::hot_state::ROOT_CURRENT_BASE_SPACE,
    crate::columnar_row_group::ROW_GROUP_MANIFEST_SPACE,
    crate::columnar_row_group::ROW_GROUP_COLUMN_SPACE,
    crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
    crate::tracked_state::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
    crate::tracked_state::MUTATION_DIRECTORY_NODE_SPACE,
    crate::tracked_state::TRACKED_STATE_COMMIT_HISTORY_DEFERRED_SPACE,
    crate::tracked_state::CURRENT_STATE_DATA_PART_SPACE,
    crate::tracked_state::SCOPED_RANGE_NODE_SPACE,
    crate::hot_state::INDEX_SPACE,
    crate::binary_cas::BINARY_CAS_MANIFEST_SPACE,
    crate::binary_cas::BINARY_CAS_MANIFEST_CHUNK_SPACE,
    crate::binary_cas::BINARY_CAS_CHUNK_SPACE,
    crate::binary_cas::BINARY_CAS_CHUNK_PRESENCE_SPACE,
    crate::binary_cas::BINARY_CAS_CHUNK_DEMAND_SPACE,
    crate::changelog::COMMIT_SPACE,
    crate::changelog::CHANGE_SPACE,
    crate::storage_adapter::REVISION_SPACE,
    crate::session::EXECUTE_IDEMPOTENCY_RECEIPT_SPACE,
    crate::session::UPLOAD_STATE_SPACE,
    crate::session::UPLOAD_MANIFEST_LEAF_SPACE,
    crate::sync::SYNC_SEQUENCE_SPACE,
    crate::sync::SYNC_REPOSITORY_EVENT_SPACE,
    crate::sync::SYNC_REPLICA_STATE_SPACE,
    crate::sync::SYNC_MATERIALIZED_STATE_ALIAS_SPACE,
    // `gc.rs` declares these through the checked constructors rather than
    // `StorageSpace::declare`, so referencing its constants here would make
    // `may_declare` read a registry it is in the middle of evaluating. The
    // rows are stated here instead and `tests::gc_spaces_match_the_registry`
    // pins the module's constants to them.
    StorageSpace::declare(
        StorageSpaceId(0x0008_0001),
        "checkpoint.recovery_ref.v3",
        ValueSemantics::Mutable,
    ),
    StorageSpace::declare(
        StorageSpaceId(0x0008_0002),
        "checkpoint.gc_state.v1",
        ValueSemantics::Mutable,
    ),
    StorageSpace::declare(
        StorageSpaceId(0x0008_0008),
        "gc.commit_retirement_intent.v1",
        ValueSemantics::Mutable,
    ),
    crate::storage_adapter::REPOSITORY_EPOCH_SPACE,
];

/// Space ids the constructor check cannot reject yet.
///
/// Empty, and it should stay that way. Every engine site that needs to place
/// chosen bytes under a key the engine publishes write-once states that need
/// through `StorageSpace::mutable_view_for_corruption_test`, which does not go
/// through the checked constructors at all.
///
/// This list held `0x0004_002b` (`tracked_state.commit_state_manifest.v7`) for
/// one raw re-declaration in a `gc.rs` retention test, justified by `gc.rs`
/// being owned by an in-flight redesign and not editable that cycle. The
/// ownership claim was round-4 residue; the site has now been converted to
/// `mutable_view_for_corruption_test` and the hole is closed.
///
/// Note what the guard could and could not do. `tests::the_unchecked_ids_are_
/// exactly_the_known_disagreements` compares this list to
/// `tests::KNOWN_DISAGREEMENTS` -- two hand-maintained lists, checked against
/// each other and never against the code -- so it would not have noticed the
/// call site disappearing, only a disagreement between the two lists. What
/// actually enforces this is the `const` assertion in
/// `StorageSpace::mutable`: with the list empty, a re-declaration is a compile
/// error naming the exact site. Widening a safety check for one call site needs
/// a guard that watches the call site, not a second list describing it.
const UNCHECKED_SPACE_IDS: &[u32] = &[];

/// Whether a space id may be declared with `semantics`.
///
/// Unregistered ids are unconstrained: adapter and conformance suites reuse
/// small ids such as `SpaceId(7)` for both semantics, and bench-owned spaces
/// live in `0x00ff_....`, which the registry never allocates.
pub(crate) const fn may_declare(id: StorageSpaceId, semantics: ValueSemantics) -> bool {
    let mut index = 0;
    while index < UNCHECKED_SPACE_IDS.len() {
        if UNCHECKED_SPACE_IDS[index] == id.0 {
            return true;
        }
        index += 1;
    }
    let mut index = 0;
    while index < ALL_STORAGE_SPACES.len() {
        let space = ALL_STORAGE_SPACES[index];
        if space.id.0 == id.0 {
            return same_semantics(space.value_semantics, semantics);
        }
        index += 1;
    }
    true
}

/// `ValueSemantics` derives `PartialEq`, which is not usable in const context.
const fn same_semantics(left: ValueSemantics, right: ValueSemantics) -> bool {
    matches!(
        (left, right),
        (ValueSemantics::Mutable, ValueSemantics::Mutable)
            | (ValueSemantics::Immutable, ValueSemantics::Immutable)
    )
}

/// Space ids that belonged to spaces this protocol has cut.
///
/// There is no compatibility reader for them; they are listed so the registry
/// tests refuse to hand a retired id to a new space, which would silently
/// reinterpret predecessor bytes left in an existing repository.
#[cfg(test)]
pub(crate) const RETIRED_STORAGE_SPACE_IDS: &[StorageSpaceId] = &[
    // untracked_state.row.v1
    StorageSpaceId(0x0001_0002),
    // json_store.untracked_reclaim_candidate.v1
    StorageSpaceId(0x0002_0002),
    // live_state.index.branch_root.v1
    StorageSpaceId(0x0004_0005),
    // hot_state.certified_row_batch.v1
    StorageSpaceId(0x0004_001f),
    // hot_state.certified_row_batch_manifest.v2
    StorageSpaceId(0x0004_0021),
    // hot_state.certified_row_batch_page.v1
    StorageSpaceId(0x0004_0022),
    // plugin.current_checkpoint.v2
    StorageSpaceId(0x0004_0026),
    // gc.reachability_delta.v1
    StorageSpaceId(0x0008_0003),
    // gc.reachability_queue.v1
    StorageSpaceId(0x0008_0004),
    // gc.tree_sweep_epoch.v1
    StorageSpaceId(0x0008_0005),
    // gc.tree_sweep_mark.v1
    StorageSpaceId(0x0008_0006),
    // gc.tree_sweep_cursor.v1
    StorageSpaceId(0x0008_0007),
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
    use crate::storage_adapter::{ValueIntegrity, ValueSemantics};
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

    /// Opting a space out of the backend's value checksum is only sound when
    /// the engine authenticates that space's values itself, and exactly one
    /// space does: `binary_cas.chunk`, whose key *is* the BLAKE3-256 digest of
    /// its value and which `decode_and_verify_payload` re-checks on every
    /// full-value read in every build.
    ///
    /// This reads the opt-in set **back out of the registry** rather than
    /// comparing two hand-written lists, so a space that opts in tomorrow
    /// fails here rather than silently losing its corruption detection. That
    /// is the whole point: the guard has to be able to observe the thing it
    /// guards.
    #[test]
    fn exactly_one_space_declares_content_addressed_values() {
        let opted_out: Vec<&str> = ALL_STORAGE_SPACES
            .iter()
            .filter(|space| space.value_integrity == ValueIntegrity::ContentAddressed)
            .map(|space| space.name)
            .collect();

        assert_eq!(
            opted_out,
            vec![crate::binary_cas::BINARY_CAS_CHUNK_SPACE.name],
            "a space declared ValueIntegrity::ContentAddressed. That tells every backend it may \
             skip its own value checksum, which is only true if the engine recomputes the value's \
             BLAKE3-256 digest from its key on EVERY full-value read, in release builds too. If \
             the new space really does that, add it here deliberately; otherwise use \
             StorageSpace::declare and keep the backend's checksum."
        );
    }

    /// The manifest planes name chunk hashes but are not addressed by their
    /// own content, and the whole-blob digest check that would catch a
    /// corrupted manifest (`assemble_blob_bytes`) is `cfg!(debug_assertions)`
    /// only. They are the most tempting spaces to opt out by analogy and the
    /// ones where it would be silently wrong, so they are pinned explicitly.
    #[test]
    fn binary_cas_manifest_planes_keep_the_backend_checksum() {
        for space in [
            crate::binary_cas::BINARY_CAS_MANIFEST_SPACE,
            crate::binary_cas::BINARY_CAS_MANIFEST_CHUNK_SPACE,
            crate::binary_cas::BINARY_CAS_CHUNK_PRESENCE_SPACE,
            crate::binary_cas::BINARY_CAS_CHUNK_DEMAND_SPACE,
        ] {
            assert_eq!(
                space.value_integrity,
                ValueIntegrity::BackendVerified,
                "{} is not content-addressed: a corrupted row yields wrong chunk hashes and the \
                 whole-blob guard in assemble_blob_bytes is debug-only",
                space.name
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
            crate::hot_state::ROW_SPACE,
            crate::hot_state::FILE_SPACE,
            crate::hot_state::DIFF_SPACE,
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
            if path.ends_with("storage_spaces.rs") {
                // This file quotes the constructors it is scanning for.
                continue;
            }
            for site in construction_sites(&source) {
                let Some(id) = literal_space_id(&site.id_expression) else {
                    continue;
                };
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

    /// A space id has exactly one value semantics, and this registry is where
    /// it is decided.
    ///
    /// This is the invariant with the sharpest physical consequences, because
    /// both adapters *place* data by the declaration rather than merely
    /// annotating it. RocksDB routes immutable spaces to a separate column
    /// family (`rocksdb.rs:253-265`). SlateDB moves immutable values out of
    /// the LSM entirely into per-publication object segments and leaves only a
    /// locator behind (`slatedb.rs:3395-3460`). A space presented as mutable
    /// on one call path and immutable on another therefore writes one physical
    /// location and reads another: on RocksDB the write lands in a column
    /// family the read never opens, and on SlateDB the raw value overwrites
    /// the locator at the same key and the next read hands those bytes to
    /// `decode_immutable_locator`.
    ///
    /// That last case is why this guard exists rather than a comment. A
    /// benchmark wrote plain bytes to `0x0005_0003` as a mutable space — the
    /// id this registry declares immutable as `binary_cas.chunk` — and the
    /// accounting scan failed with
    /// `Corruption("immutable segment locator is invalid")`. It presented as
    /// data corruption and was a semantics mismatch.
    ///
    /// `StorageSpace::mutable` and `::immutable` reject a registered id with
    /// the other semantics at compile time, so what remains for this scan is
    /// the ids in [`UNCHECKED_SPACE_IDS`], which the constructors let through,
    /// and calls whose id is only known at run time.
    ///
    /// The scan covers the whole workspace, not just this crate, because that
    /// benchmark is not in this crate. Ids the registry does not own are
    /// ignored: adapter and conformance suites legitimately reuse small ids
    /// such as `SpaceId(7)` for both semantics, and bench-owned spaces live in
    /// `0x00ff_....`, which the registry never allocates.
    /// Sites the checked constructors cannot reject, because their id is in
    /// [`UNCHECKED_SPACE_IDS`].
    ///
    /// `gc.rs` re-declares the immutable commit-state manifest space as
    /// mutable so a retention test can delete and overwrite an authority the
    /// engine publishes write-once. Every other engine test states that need
    /// through `StorageSpace::mutable_view_for_corruption_test`; this file is
    /// owned by the in-flight GC ledger redesign and is left alone this cycle.
    /// The list is exact, so this also fails once the redesign removes the
    /// site and the entry goes stale.
    const KNOWN_DISAGREEMENTS: &[(&str, u32)] = &[];

    #[test]
    fn no_registered_space_id_is_declared_with_two_value_semantics() {
        let registry = ALL_STORAGE_SPACES
            .iter()
            .map(|space| (space.id.0, *space))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            registry.len(),
            ALL_STORAGE_SPACES.len(),
            "the registry itself must map each space id to exactly one space",
        );

        let sources = workspace_sources();
        let constants = space_id_constants(&sources);
        let mut disagreements = Vec::new();
        let mut agreeing_registered_ids = std::collections::BTreeSet::new();
        for (path, source) in &sources {
            if path.ends_with("lix/src/storage_spaces.rs") {
                // This file quotes the constructors it is scanning for.
                continue;
            }
            for site in construction_sites(source) {
                let Some(id) = resolve_space_id(&site.id_expression, &constants) else {
                    continue;
                };
                let Some(space) = registry.get(&id) else {
                    continue;
                };
                if space.value_semantics == site.semantics {
                    agreeing_registered_ids.insert(id);
                } else {
                    disagreements.push((
                        path.clone(),
                        id,
                        format!(
                            "{path}:{} declares 0x{id:08x} ({}) as {:?}; the registry declares it {:?}",
                            site.line, space.name, site.semantics, space.value_semantics,
                        ),
                    ));
                }
            }
        }

        assert_eq!(
            agreeing_registered_ids.len(),
            ALL_STORAGE_SPACES.len(),
            "the scan resolved only {} of {} registered spaces, so it is not \
             reading the sources it claims to check",
            agreeing_registered_ids.len(),
            ALL_STORAGE_SPACES.len(),
        );

        let observed = disagreements
            .iter()
            .map(|(path, id, _)| (path.as_str(), *id))
            .collect::<std::collections::BTreeSet<_>>();
        let known = KNOWN_DISAGREEMENTS
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            observed,
            known,
            "value-semantics disagreements changed.\n  found:\n    {}\n  \
             a new entry is a defect: declare the space once here and read it \
             back (`storage_bench::storage_space_by_id`, \
             `storage_bench::storage_space_by_name`) or, for a corruption test \
             inside this crate, use \
             `StorageSpace::mutable_view_for_corruption_test`.\n  a missing \
             entry means KNOWN_DISAGREEMENTS is stale and must shrink.",
            disagreements
                .iter()
                .map(|(_, _, message)| message.as_str())
                .collect::<Vec<_>>()
                .join("\n    "),
        );
    }

    /// The compile-time check and the source scan must cover the same gap.
    ///
    /// Every id exempted from [`may_declare`] is a place where a disagreement
    /// compiles, so it must be a place the scan reports. Pinning both lists to
    /// each other means neither can be widened quietly, and both go away in
    /// the same commit.
    #[test]
    fn the_unchecked_ids_are_exactly_the_known_disagreements() {
        assert_eq!(
            UNCHECKED_SPACE_IDS.iter().copied().collect::<Vec<_>>(),
            KNOWN_DISAGREEMENTS
                .iter()
                .map(|(_, id)| *id)
                .collect::<Vec<_>>(),
            "UNCHECKED_SPACE_IDS holes and KNOWN_DISAGREEMENTS must describe \
             the same sites",
        );
    }

    /// `gc.rs` declares its four spaces through the checked constructors, so
    /// the registry states their rows itself rather than referencing them.
    ///
    /// `may_declare` proves the semantics agree at compile time. Names are not
    /// part of that check, so they are pinned here: a space name is what
    /// layout accounting and every storage report key on.
    #[test]
    fn gc_spaces_match_the_registry() {
        for space in [
            crate::gc::CHECKPOINT_RECOVERY_REF_SPACE,
            crate::gc::CHECKPOINT_GC_STATE_SPACE,
            crate::gc::COMMIT_RETIREMENT_INTENT_SPACE,
        ] {
            let row = ALL_STORAGE_SPACES
                .iter()
                .find(|candidate| candidate.id == space.id)
                .unwrap_or_else(|| panic!("{space} is not registered"));
            assert_eq!(space, *row, "{space} disagrees with its registry row");
        }
    }

    /// One `StorageSpace` constructor call found in a source file.
    struct ConstructionSite {
        offset: usize,
        line: usize,
        /// The first argument, verbatim minus whitespace. It is only sometimes
        /// a literal — `SOME_SPACE.id` and named `SpaceId` constants are the
        /// two indirections real declarations use.
        id_expression: String,
        semantics: ValueSemantics,
    }

    fn engine_sources() -> Vec<(String, String)> {
        let sources = rust_sources(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"));
        assert!(!sources.is_empty(), "engine sources should be readable");
        sources
    }

    /// Every workspace Rust source, keyed by its path below `packages/`.
    ///
    /// Benchmarks, qualification harnesses and adapter suites all construct
    /// storage spaces, and the failure this guard exists for happened in a
    /// benchmark, so a scan scoped to this crate would have missed it.
    fn workspace_sources() -> Vec<(String, String)> {
        let packages = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("the lix crate lives below the packages directory")
            .to_path_buf();
        let mut sources = Vec::new();
        for entry in std::fs::read_dir(&packages)
            .expect("the packages directory should be readable")
            .flatten()
        {
            for subdirectory in ["src", "tests", "benches", "examples"] {
                for (path, source) in rust_sources(&entry.path().join(subdirectory)) {
                    let relative = std::path::Path::new(&path)
                        .strip_prefix(&packages)
                        .ok()
                        .map(|path| path.display().to_string());
                    sources.push((relative.unwrap_or(path), source));
                }
            }
        }
        assert!(!sources.is_empty(), "workspace sources should be readable");
        sources
    }

    fn rust_sources(root: &std::path::Path) -> Vec<(String, String)> {
        let mut sources = Vec::new();
        let mut pending = vec![root.to_path_buf()];
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
        sources
    }

    /// Every `StorageSpace` constructor call in one source file.
    ///
    /// `declare` and `declare_content_addressed` state a pairing and carry
    /// their semantics as a third argument; `mutable` and `immutable` carry it
    /// in the name. All four are collected, so the scan can both find the
    /// registry's own declarations and check every use against them.
    ///
    /// **Every constructor must be listed here.** These names are matched as
    /// literal substrings ending in `(`, so a constructor that is added to
    /// `StorageSpace` and not added to this list does not fail anything — its
    /// call sites simply stop being scanned, and both
    /// [`tests::every_declared_space_is_registered`] and
    /// [`tests::no_registered_space_id_is_declared_with_two_value_semantics`]
    /// go quiet about the space it declares. That is the failure mode this
    /// module exists to prevent, reintroduced one level up. Note also that
    /// `StorageSpace::declare(` does **not** match
    /// `StorageSpace::declare_content_addressed(`, which is why the latter is
    /// its own row rather than covered by a prefix.
    const SPACE_CONSTRUCTORS: &[(&str, Option<ValueSemantics>)] = &[
        ("StorageSpace::mutable(", Some(ValueSemantics::Mutable)),
        ("StorageSpace::immutable(", Some(ValueSemantics::Immutable)),
        ("StorageSpace::declare(", None),
        ("StorageSpace::declare_content_addressed(", None),
    ];

    /// [`SPACE_CONSTRUCTORS`] must name every constructor that takes a space id.
    ///
    /// Read back out of `storage/types.rs` rather than compared to a second
    /// hand-written list, because a list checked against another list is what
    /// `UNCHECKED_SPACE_IDS` above already learned the hard way. A constructor
    /// added to `StorageSpace` and not added to the scanner does not fail
    /// anything by itself — it silently removes its own call sites from both
    /// registry guards — so the scanner's completeness has to be an assertion
    /// against the type, and this is it.
    #[test]
    fn the_scanner_knows_every_space_constructor() {
        let (_, types_source) = engine_sources()
            .into_iter()
            .find(|(path, _)| path.replace('\\', "/").ends_with("storage/types.rs"))
            .expect("storage/types.rs should be readable");
        let implementation = types_source
            .split_once("impl StorageSpace {")
            .expect("StorageSpace should have an inherent impl block")
            .1;
        let mut declared = Vec::new();
        for line in implementation.lines() {
            if line.starts_with('}') {
                break;
            }
            let Some((_, tail)) = line.trim_start().split_once("const fn ") else {
                continue;
            };
            let Some((name, _)) = tail.split_once('(') else {
                continue;
            };
            // `mutable_view_for_corruption_test` takes `self`, not an id, so
            // it can neither introduce a space nor pick a value integrity.
            if implementation
                .split_once(&format!("const fn {name}("))
                .is_some_and(|(_, arguments)| !arguments.trim_start().starts_with("id: SpaceId"))
            {
                continue;
            }
            declared.push(format!("StorageSpace::{name}("));
        }
        declared.sort();
        let mut scanned = SPACE_CONSTRUCTORS
            .iter()
            .map(|(constructor, _)| (*constructor).to_string())
            .collect::<Vec<_>>();
        scanned.sort();
        assert_eq!(
            scanned, declared,
            "storage/types.rs declares a StorageSpace constructor the registry scanner does not \
             look for. Add it to SPACE_CONSTRUCTORS, or every space declared through it becomes \
             invisible to every_declared_space_is_registered and to \
             no_registered_space_id_is_declared_with_two_value_semantics."
        );
    }

    fn construction_sites(source: &str) -> Vec<ConstructionSite> {
        let mut sites = Vec::new();
        for (constructor, declared) in SPACE_CONSTRUCTORS.iter().copied() {
            let mut cursor = 0;
            while let Some(offset) = source[cursor..].find(constructor) {
                let start = cursor + offset;
                let arguments_start = start + constructor.len();
                cursor = arguments_start;
                let Some(arguments) = call_arguments(&source[arguments_start..]) else {
                    continue;
                };
                let Some(id_expression) = arguments.first() else {
                    continue;
                };
                let semantics = match declared {
                    Some(semantics) => semantics,
                    // `declare(id, name, ValueSemantics::…)`.
                    None => match arguments.get(2).map(String::as_str) {
                        Some(argument) if argument.ends_with("ValueSemantics::Mutable") => {
                            ValueSemantics::Mutable
                        }
                        Some(argument) if argument.ends_with("ValueSemantics::Immutable") => {
                            ValueSemantics::Immutable
                        }
                        _ => continue,
                    },
                };
                sites.push(ConstructionSite {
                    offset: start,
                    line: source[..start].matches('\n').count() + 1,
                    id_expression: id_expression.clone(),
                    semantics,
                });
            }
        }
        // Each constructor is scanned in its own pass, so restore source
        // order: callers that take the first site mean the first one written.
        sites.sort_by_key(|site| site.offset);
        sites
    }

    /// The top-level arguments of a call, given the text after its `(`.
    ///
    /// Whitespace is squeezed out so a declaration wrapped across lines reads
    /// the same as one that fits on a single line.
    fn call_arguments(source: &str) -> Option<Vec<String>> {
        let mut arguments = Vec::new();
        let mut current = String::new();
        let mut depth = 0_usize;
        for byte in source.bytes() {
            match byte {
                b'(' | b'[' => depth += 1,
                b')' | b']' if depth > 0 => depth -= 1,
                b')' | b']' => {
                    if !current.is_empty() {
                        arguments.push(current);
                    }
                    return Some(arguments);
                }
                b',' if depth == 0 => {
                    arguments.push(std::mem::take(&mut current));
                    continue;
                }
                _ => {}
            }
            if !byte.is_ascii_whitespace() {
                current.push(char::from(byte));
            }
        }
        None
    }

    /// Space ids declared as named constants, so `SOME_SPACE.id` resolves.
    ///
    /// A name declared twice with different ids is dropped rather than
    /// guessed at; the site that used it is then simply not checked.
    fn space_id_constants(sources: &[(String, String)]) -> BTreeMap<String, u32> {
        let mut resolved: BTreeMap<String, Option<u32>> = BTreeMap::new();
        for (_, source) in sources {
            let mut rest = source.as_str();
            while let Some(offset) = rest.find("const ") {
                rest = &rest[offset + "const ".len()..];
                let Some((declaration, value)) = rest.split_once('=') else {
                    break;
                };
                let Some((name, declared_type)) = declaration.split_once(':') else {
                    continue;
                };
                let name = name.trim();
                if name.is_empty()
                    || !name.bytes().all(|byte| {
                        byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'
                    })
                {
                    continue;
                }
                let Some((expression, _)) = value.split_once(';') else {
                    continue;
                };
                let id = match declared_type.trim() {
                    "SpaceId" | "StorageSpaceId" => {
                        literal_space_id(&expression.split_whitespace().collect::<String>())
                    }
                    "StorageSpace" => construction_sites(expression)
                        .first()
                        .and_then(|site| literal_space_id(&site.id_expression)),
                    _ => continue,
                };
                let Some(id) = id else {
                    continue;
                };
                resolved
                    .entry(name.to_string())
                    .and_modify(|existing| {
                        if *existing != Some(id) {
                            *existing = None;
                        }
                    })
                    .or_insert(Some(id));
            }
        }
        resolved
            .into_iter()
            .filter_map(|(name, id)| id.map(|id| (name, id)))
            .collect()
    }

    /// `SpaceId(0x…)` written out in full. Decimal ids belong to adapter and
    /// conformance scaffolding, which the registry does not own.
    fn literal_space_id(expression: &str) -> Option<u32> {
        let expression = expression.trim();
        let open = expression.find('(')?;
        let constructor = expression[..open].rsplit("::").next()?;
        if constructor != "SpaceId" && constructor != "StorageSpaceId" {
            return None;
        }
        let digits = expression
            .strip_suffix(')')?
            .get(open + 1..)?
            .trim()
            .strip_prefix("0x")?;
        u32::from_str_radix(&digits.replace('_', ""), 16).ok()
    }

    /// Resolves the id argument of one construction site, through the two
    /// indirections real declarations use: `SOME_SPACE.id`, and a named
    /// `SpaceId` constant.
    fn resolve_space_id(expression: &str, constants: &BTreeMap<String, u32>) -> Option<u32> {
        if let Some(id) = literal_space_id(expression) {
            return Some(id);
        }
        let name = expression
            .strip_suffix(".id")
            .unwrap_or(expression)
            .rsplit("::")
            .next()?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return None;
        }
        constants.get(name).copied()
    }
}
