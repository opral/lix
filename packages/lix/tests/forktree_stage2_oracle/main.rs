//! Standalone source-residue oracle for the first runnable ForkTree hard cut.
//!
//! This file deliberately lives below an integration-test subdirectory so it
//! is not linked into Lix. Compile it directly with `rustc`; it only reads
//! production source and never opens a repository.

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

const RETAINED_INDEPENDENT_SPACES: &[&str] = &[
    "JSON_SPACE",
    "UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE",
    "TRACKED_MUTATION_REVISION_SPACE",
    "EXECUTE_IDEMPOTENCY_RECEIPT_SPACE",
    "FILESYSTEM_PATH_REVISION_SPACE",
    "REPOSITORY_PROTOCOL_SPACE",
];

const REBUILDABLE_SPACES: &[&str] = &["MUTATION_REVISION_SPACE", "CATALOG_REVISION_SPACE"];

const DELETE_MODULES: &[&str] = &[
    "tracked_state/storage.rs",
    "tracked_state/tree.rs",
    "tracked_state/codec.rs",
    "tracked_state/mutation_directory.rs",
    "tracked_state/scoped_range.rs",
    "tracked_state/current_state_data_part.rs",
    "tracked_state/current_state_envelope.rs",
    "tracked_state/commit_root_rebuild.rs",
    "tracked_state/replacement_part.rs",
    "changelog/store.rs",
    "changelog/codec.rs",
    "branch/control.rs",
    "binary_cas/kv.rs",
    "transaction/plugin_checkpoint.rs",
];

// Exact legacy APIs that can otherwise survive after constants are renamed.
const LEGACY_OWNER_TOKENS: &[&str] = &[
    "HotStateStoreReader",
    "HotStateWriter",
    "HotTrackedSnapshot",
    "TrackedWorkingDiff",
    "TrackedHeadContext",
    "BranchHeadControlReader",
    "stage_branch_head_control",
    "stage_delete_branch_head_control",
    "stage_transaction_append",
    "load_commit_state_manifest",
    "load_published_commit_state_manifest",
    "stage_commit_state_manifest",
    "stage_addressable_commit_deltas",
    "stage_change_locators",
    "load_change_record_by_id",
    "build_mutation_directory",
    "load_mutation_part_read_plan",
    "stage_certified_entity_batches",
    "scan_certified_history_rows",
    "scan_working_diff",
    "stage_manifest_chunk",
    "scan_manifest_chunks",
    "load_upload_state",
    "stage_upload_state",
    "load_upload_manifest_leaf",
    "stage_upload_manifest_leaf",
    "stage_checkpoint_gc_state",
    "load_checkpoint_gc_state",
    "stage_reachability_queue",
    "stage_tree_sweep_epoch",
    "stage_plugin_checkpoint",
];

// The accepted bounded-GC owner contract deletes the V1 in-memory discovery
// model. These spellings are intentionally exact, not broad words such as
// "legacy" or "fallback" that would reject fail-closed tests and comments.
const FORBIDDEN_OWNER_SHAPES: &[&str] = &[
    "GcMarkPackV1",
    "GcProgressV1",
    "discover_sweep_plan",
    "pub struct SpaceId(pub u32)",
    "pub id: SpaceId",
    "pub name: &'static str",
    "pub value_semantics: ValueSemantics",
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
    "EdgePager",
];

#[derive(Debug)]
struct Finding {
    class: &'static str,
    item: String,
    count: usize,
}

fn collect_rs(root: &Path, current: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = fs::read_dir(current)
        .map_err(|error| format!("read_dir {}: {error}", current.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("directory entry: {error}"))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("file_type {}: {error}", path.display()))?;
        if file_type.is_dir() {
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

    for token in LEGACY_SPACES {
        let occurrences = count(source, token);
        if occurrences != 0 {
            findings.push(Finding {
                class: "legacy-space",
                item: (*token).to_owned(),
                count: occurrences,
            });
        }
    }
    for token in LEGACY_OWNER_TOKENS {
        let occurrences = count(source, token);
        if occurrences != 0 {
            findings.push(Finding {
                class: "legacy-owner-api",
                item: (*token).to_owned(),
                count: occurrences,
            });
        }
    }
    for token in FORBIDDEN_OWNER_SHAPES {
        let occurrences = count(source, token);
        if occurrences != 0 {
            findings.push(Finding {
                class: "unsealed-or-unbounded-owner",
                item: (*token).to_owned(),
                count: occurrences,
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
    for module in DELETE_MODULES {
        if files.contains(*module) {
            findings.push(Finding {
                class: "superseded-module",
                item: (*module).to_owned(),
                count: 1,
            });
        }
    }

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
        "{required}\n{}\n{}\n{}",
        LEGACY_SPACES[0], LEGACY_OWNER_TOKENS[0], FORBIDDEN_OWNER_SHAPES[0]
    );
    let mut dirty_files = BTreeSet::new();
    dirty_files.insert(DELETE_MODULES[0].to_owned());
    let dirty = inspect(&dirty_source, &dirty_files);
    for class in [
        "legacy-space",
        "legacy-owner-api",
        "unsealed-or-unbounded-owner",
        "superseded-module",
    ] {
        if !dirty.iter().any(|finding| finding.class == class) {
            return Err(format!("synthetic dirty source missed {class}"));
        }
    }

    println!(
        "SELF_TEST_OK legacy_spaces={} retained={} rebuildable={} delete_modules={} required_owner_tokens={}",
        LEGACY_SPACES.len(),
        RETAINED_INDEPENDENT_SPACES.len(),
        REBUILDABLE_SPACES.len(),
        DELETE_MODULES.len(),
        REQUIRED_OWNER_TOKENS.len()
    );
    Ok(())
}

fn run() -> Result<i32, String> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("self-test") if args.next().is_none() => {
            self_test()?;
            Ok(0)
        }
        Some("audit") => {
            let root = args
                .next()
                .ok_or_else(|| "usage: oracle audit CHECKOUT".to_owned())?;
            if args.next().is_some() {
                return Err("usage: oracle audit CHECKOUT".to_owned());
            }
            let findings = audit(Path::new(&root))?;
            if findings.is_empty() {
                println!(
                    "STAGE2_RESIDUE_OK legacy_spaces={} retained={} rebuildable={}",
                    LEGACY_SPACES.len(),
                    RETAINED_INDEPENDENT_SPACES.len(),
                    REBUILDABLE_SPACES.len()
                );
                Ok(0)
            } else {
                println!("STAGE2_RESIDUE_BLOCKED findings={}", findings.len());
                for finding in findings {
                    println!("{}\t{}\t{}", finding.class, finding.count, finding.item);
                }
                Ok(1)
            }
        }
        _ => Err("usage: oracle self-test | oracle audit CHECKOUT".to_owned()),
    }
}

fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    }
}
