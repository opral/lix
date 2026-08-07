//! Standalone source-residue oracle for the first runnable ForkTree hard cut.
//!
//! Compile this file directly with `rustc`. It reads production Rust source;
//! it never opens or mutates a repository.

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const LEGACY_SPACES: &[&str] = &[
    "BINARY_CAS_MANIFEST_SPACE",
    "BINARY_CAS_MANIFEST_CHUNK_SPACE",
    "BINARY_CAS_CHUNK_SPACE",
    "BINARY_CAS_CHUNK_PRESENCE_SPACE",
    "BRANCH_HEAD_CONTROL_SPACE",
    "CERTIFIED_ENTITY_BATCH_SPACE",
    "CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE",
    "CERTIFIED_ENTITY_BATCH_PAGE_SPACE",
    "CHANGE_SPACE",
    "CHECKPOINT_GC_STATE_SPACE",
    "CHECKPOINT_RECOVERY_REF_SPACE",
    "COMMIT_CHANGE_ID_SPACE",
    "COMMIT_SPACE",
    "CURRENT_STATE_DATA_PART_SPACE",
    "CURRENT_STATE_DATA_PART_REFS_SPACE",
    "GC_REACHABILITY_DELTA_SPACE",
    "GC_REACHABILITY_QUEUE_SPACE",
    "GC_TREE_SWEEP_CURSOR_SPACE",
    "GC_TREE_SWEEP_EPOCH_SPACE",
    "GC_TREE_SWEEP_MARK_SPACE",
    "HOT_COLLECTION_CONTROL_SPACE",
    "HOT_DIFF_SPACE",
    "HOT_FILE_SPACE",
    "HOT_ROW_SPACE",
    "MUTATION_DIRECTORY_NODE_SPACE",
    "PACKED_CURRENT_BASE_CONTROL_SPACE",
    "PACKED_CURRENT_BASE_SPACE",
    "PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE",
    "PLUGIN_CHECKPOINT_SPACE",
    "ROOT_CURRENT_BASE_SPACE",
    "ROW_GROUP_COLUMN_SPACE",
    "ROW_GROUP_MANIFEST_SPACE",
    "SCOPED_RANGE_NODE_SPACE",
    "TRACKED_STATE_CHANGE_LOCATOR_SPACE",
    "TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE",
    "TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE",
    "TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE",
    "TRACKED_STATE_TREE_CHUNK_SPACE",
    "TRACKED_WORKING_DIFF_MARKER_SPACE",
    "UPLOAD_MANIFEST_LEAF_SPACE",
    "UPLOAD_STATE_SPACE",
];

const LEGACY_OWNER_TOKENS: &[&str] = &[
    // Tracked/current physical owners.
    "TrackedStateStoreReader",
    "TrackedStateTree",
    "TrackedHeadContext",
    "HotStateStoreReader",
    "HotStateWriter",
    "HotTrackedSnapshot",
    "TrackedWorkingDiff",
    "WorkingDiffIndexCoverage",
    "CommitStateManifest",
    "CommitStateMutationInventory",
    "build_mutation_directory",
    "load_mutation_part_read_plan",
    "load_commit_state_manifest",
    "load_published_commit_state_manifest",
    "stage_commit_state_manifest",
    "stage_addressable_commit_deltas",
    "stage_ordered_addressable_commit_deltas",
    "stage_change_locators",
    "load_change_record_by_id",
    "scan_commit_delta_inventory",
    "scan_commit_delta_values",
    "scan_change_records_from_commit_deltas",
    "stage_certified_entity_batches",
    "scan_certified_history_rows",
    "stage_current_state_with_working_diff",
    "stage_commit_with_working_diff",
    "working_diff_for_control",
    "stage_tracked_working_diff_epoch",
    "stage_delete_tracked_working_diff_epoch",
    // Changelog and branch control authorities.
    "ChangelogStoreReader",
    "ChangelogStoreWriter",
    "ChangelogStorageRead",
    "stage_transaction_append",
    "commit_change_id_key",
    "encode_commit_record",
    "encode_change_record",
    "BranchHeadControl",
    "BranchHeadControlReader",
    "BranchHeadControlContext",
    "stage_branch_head_control",
    "stage_delete_branch_head_control",
    "branch_head_control_precondition",
    // Legacy binary CAS and multipart owners/codecs.
    "ExistingChunkAwareBinaryCasWriter",
    "StorageBinaryCasDeltaBaseLayout",
    "BinaryCasManifest",
    "encode_binary_cas_manifest",
    "decode_binary_cas_manifest",
    "stage_manifest_chunk",
    "scan_manifest_chunks",
    "stage_blob_write_skipping_existing_chunks",
    "load_upload_state",
    "stage_upload_state",
    "load_upload_manifest_leaf",
    "stage_upload_manifest_leaf",
    // Legacy GC/checkpoint/plugin authority.
    "CheckpointRecoveryRef",
    "CheckpointGcState",
    "TreeSweepEpochSession",
    "RootReachabilityDelta",
    "stage_checkpoint_gc_state",
    "load_checkpoint_gc_state",
    "stage_reachability_queue_seed",
    "stage_reachability_delta_batch",
    "begin_tree_sweep_epoch",
    "stage_tree_sweep_epoch_page",
    "discover_sweep_plan",
    "GcMarkPackV1",
    "GcProgressV1",
    "stage_plugin_checkpoint",
    // #1258 current-main physical CAS/retention implementation. Its semantics
    // move into typed ForkTree edges and bounded owner progress; these names
    // must not survive as a parallel authority.
    "BinaryCasGcSweep",
    "stage_gc_reclamation",
    "stage_reclaim_unreachable_binary_cas",
    "stage_mutation_epoch",
    "AuthenticatedServingDependencyClosure",
    "load_authenticated_repository_retention",
    "load_authenticated_serving_dependency_closure",
    "collect_active_point_replay_dependencies",
    "fold_reachability_batches",
    "collect_all_reachability_checkpoint_roots",
    "RetainedCommitSnapshot",
    "load_retained_commit_snapshots_for_schemas",
    "load_local_selected_change_owner_commit_ids",
    "collect_gc_binary_blob_roots",
    "collect_gc_wasm_blob_roots",
    "stage_reclaimable_upload_receipts",
];

const DELETE_MODULES: &[&str] = &[
    "tracked_state/storage.rs",
    "tracked_state/tree.rs",
    "tracked_state/codec.rs",
    "tracked_state/mutation_directory.rs",
    "tracked_state/scoped_range.rs",
    "tracked_state/scoped_current_state.rs",
    "tracked_state/current_state_data_part.rs",
    "tracked_state/current_state_envelope.rs",
    "tracked_state/commit_root_rebuild.rs",
    "tracked_state/replacement_part.rs",
    "live_state/tracked_head.rs",
    "live_state/tracked_head/hot.rs",
    "columnar_row_group.rs",
    "changelog/store.rs",
    "changelog/codec.rs",
    "branch/control.rs",
    "commit_graph/walker.rs",
    "binary_cas/kv.rs",
    "binary_cas/codec.rs",
    "binary_cas/chunking.rs",
    "binary_cas/stats.rs",
    "transaction/plugin_checkpoint.rs",
    "storage_adapter/scan.rs",
];

// The foundational cursor PR owns deletion of this API and its wrappers. The
// Stage-2 candidate must consume the landed cursor and may not recreate any
// alias or compatibility plan around page reconstruction.
const OLD_SCAN_TOKENS: &[&str] = &[
    "StorageScanOptions",
    "ScanPlan",
    "pub struct ScanOptions",
    "pub struct ScanChunk",
    "scan_resume_after",
];

const UNSEALED_TOKENS: &[&str] = &[
    "pub struct SpaceId(pub u32)",
    "pub const fn mutable(id: SpaceId",
    "pub const fn immutable(id: SpaceId",
];

const REQUIRED_OWNER_TOKENS: &[&str] = &[
    "OBJECT_SPACE",
    "SELECTOR_SPACE",
    "UNTRACKED_ROW_SPACE",
    "RepositoryRootV1",
    "CommitCatalog",
    "ChangeCatalog",
    "PreparedPublication",
    "StateCell",
    "UploadProgressV1",
    "ReceiptTree",
    "GcMarkPackV2",
    "GcProgressV2",
    "GcRadixNodeV1",
    "GcQueuePackV1",
    "GcLiveBranchPackV1",
    "GcProgressSelectorV2",
    "GcEdgeCursorV1",
];

#[derive(Debug, Eq, PartialEq)]
struct Finding {
    class: &'static str,
    item: String,
    count: usize,
}

fn collect_rs(root: &Path, current: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in
        fs::read_dir(current).map_err(|error| format!("read_dir {}: {error}", current.display()))?
    {
        let entry = entry.map_err(|error| format!("directory entry: {error}"))?;
        let path = entry.path();
        let kind = entry
            .file_type()
            .map_err(|error| format!("file_type {}: {error}", path.display()))?;
        if kind.is_dir() {
            collect_rs(root, &path, files)?;
        } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            files.push(path.strip_prefix(root).unwrap_or(&path).to_path_buf());
        }
    }
    Ok(())
}

fn count(source: &str, needle: &str) -> usize {
    source.match_indices(needle).count()
}

fn inspect(source: &str, files: &BTreeSet<String>) -> Vec<Finding> {
    let mut findings = Vec::new();
    for (class, tokens) in [
        ("legacy-space", LEGACY_SPACES),
        ("legacy-owner-or-codec", LEGACY_OWNER_TOKENS),
        ("old-paginated-scan", OLD_SCAN_TOKENS),
        ("unsealed-owner", UNSEALED_TOKENS),
    ] {
        for token in tokens {
            let occurrences = count(source, token);
            if occurrences != 0 {
                findings.push(Finding {
                    class,
                    item: (*token).to_owned(),
                    count: occurrences,
                });
            }
        }
    }
    for module in DELETE_MODULES {
        if files.contains(*module) {
            findings.push(Finding {
                class: "superseded-module",
                item: (*module).to_owned(),
                count: 1,
            });
        }
    }
    for token in REQUIRED_OWNER_TOKENS {
        if !source.contains(token) {
            findings.push(Finding {
                class: "missing-required-owner",
                item: (*token).to_owned(),
                count: 0,
            });
        }
    }
    findings.sort_by(|left, right| {
        (left.class, left.item.as_str()).cmp(&(right.class, right.item.as_str()))
    });
    findings
}

fn audit(root: &Path) -> Result<Vec<Finding>, String> {
    let source_root = root.join("packages/lix/src");
    let mut paths = Vec::new();
    collect_rs(&source_root, &source_root, &mut paths)?;
    paths.sort();
    let mut source = String::new();
    let mut names = BTreeSet::new();
    for relative in paths {
        let path = source_root.join(&relative);
        source.push_str(
            &fs::read_to_string(&path)
                .map_err(|error| format!("read {}: {error}", path.display()))?,
        );
        source.push('\n');
        names.insert(relative.to_string_lossy().replace('\\', "/"));
    }
    Ok(inspect(&source, &names))
}

fn self_test() -> Result<(), String> {
    let required = REQUIRED_OWNER_TOKENS.join("\n");
    let clean = inspect(&required, &BTreeSet::new());
    if !clean.is_empty() {
        return Err(format!("synthetic clean source rejected: {clean:?}"));
    }
    let dirty_source = format!(
        "{required}\n{}\n{}\n{}\n{}",
        LEGACY_SPACES[0], LEGACY_OWNER_TOKENS[0], OLD_SCAN_TOKENS[0], UNSEALED_TOKENS[0]
    );
    let mut dirty_files = BTreeSet::new();
    dirty_files.insert(DELETE_MODULES[0].to_owned());
    let dirty = inspect(&dirty_source, &dirty_files);
    for class in [
        "legacy-space",
        "legacy-owner-or-codec",
        "old-paginated-scan",
        "unsealed-owner",
        "superseded-module",
    ] {
        if !dirty.iter().any(|finding| finding.class == class) {
            return Err(format!("synthetic dirty source missed {class}"));
        }
    }
    println!("forktree-stage2-execution-oracle self-test PASS");
    Ok(())
}

fn print_findings(findings: &[Finding]) {
    for finding in findings {
        println!("{}\t{}\t{}", finding.class, finding.item, finding.count);
    }
    println!("finding_count={}", findings.len());
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("self-test") => self_test(),
        Some("baseline") => {
            let root = PathBuf::from(args.next().ok_or("baseline requires repository root")?);
            let findings = audit(&root)?;
            if findings.is_empty() {
                return Err("baseline unexpectedly has zero residue".to_owned());
            }
            print_findings(&findings);
            Ok(())
        }
        Some("audit") => {
            let root = PathBuf::from(args.next().ok_or("audit requires repository root")?);
            let findings = audit(&root)?;
            if findings.is_empty() {
                println!("forktree-stage2-execution-oracle audit PASS");
                Ok(())
            } else {
                print_findings(&findings);
                Err("first runnable candidate retains forbidden residue".to_owned())
            }
        }
        _ => Err("usage: oracle <self-test|baseline REPO|audit REPO>".to_owned()),
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
