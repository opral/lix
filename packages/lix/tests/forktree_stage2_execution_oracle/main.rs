//! Standalone source-residue oracle for the first runnable ForkTree hard cut.
//!
//! Compile this file directly with `rustc`. It reads production Rust source;
//! it never opens or mutates a repository.

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const LEGACY_SPACES: &[&str] = &[
    "BINARY_CAS_MUTATION_EPOCH_SPACE",
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
    "load_snapshot_commit_root",
    "load_published_commit_state_topology",
    "load_commit_state_manifests",
    "load_commit_state_authority_ids",
    "load_commit_mutation_directory_roots",
    "load_published_commit_state_manifest",
    "stage_commit_state_manifest",
    "stage_addressable_commit_deltas",
    "stage_ordered_addressable_commit_deltas",
    "stage_change_locators",
    "stage_ordered_addressable_replacement_parts",
    "stage_ordered_columnar_mutations",
    "stage_current_state_scoped_ranges_from_published_parent",
    "stage_current_state_scoped_ranges_from_published_topology_parent",
    "stage_current_state_scoped_ranges_from_staged_parent",
    "stage_current_state_scoped_ranges_from_topology",
    "stage_delete_commit_state_manifest_for_gc",
    "load_change_record_by_id",
    "load_commit_delta_change_records",
    "load_commit_delta_members_with_payloads",
    "load_commit_delta_replay_metadata",
    "load_owned_commit_delta_entries",
    "scan_commit_delta_inventory",
    "scan_commit_delta_values",
    "scan_change_records_from_commit_deltas",
    "stage_certified_entity_batches",
    "scan_certified_history_rows",
    "stage_current_state_with_working_diff",
    "stage_complete_current_state_with_working_diff",
    "stage_commit_with_working_diff",
    "working_diff_for_control",
    "working_diff_epoch",
    "hot_working_diff_entries",
    "choose_hot_or_packed_working_diff",
    "stage_active_working_diff_scopes",
    "stage_checkpoint_working_diff_epochs",
    "stage_tracked_working_diff_epoch",
    "stage_delete_tracked_working_diff_epoch",
    // Canonical branch/HOT deterministic-sequence closure. Stage 2 must move
    // this semantic proof into the authenticated untracked owner and delete
    // the mixed HOT/control implementation rather than preserve a side index.
    "validate_exact_collection_closure",
    "exact_collection_live_count",
    "ordered_identity_digest",
    "canonicalize_hot_scan_rows",
    "DEFERRED_ROOT_LIVE_COUNT",
    // Changelog and branch control authorities.
    "ChangelogStoreReader",
    "ChangelogStoreWriter",
    "ChangelogReader",
    "ChangelogWriter",
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
    "BinaryCasChunking",
    "BinaryCasStorageStats",
    "BinaryCasChunkRef",
    "BinaryChunkCodec",
    "StorageBinaryCasManifestChunk",
    "StorageBinaryCasDeltaSegment",
    "PreparedChunk",
    "BlobWritePlan",
    "encode_binary_cas_manifest",
    "decode_binary_cas_manifest",
    "stage_manifest_chunk",
    "scan_manifest_chunks",
    "stage_blob_write_skipping_existing_chunks",
    "UploadState",
    "UploadManifestLeaf",
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
    "CurrentPluginCheckpoint",
    "load_current_plugin_checkpoint",
    "stage_current_plugin_checkpoint",
    "stage_delete_branch_plugin_checkpoints",
    "stage_delete_current_plugin_checkpoints",
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
    "load_mutation_epoch",
    "mark_live_blob",
    "validate_live_manifest_identity",
    "mark_live_chunk_expectation",
    "decode_manifest_chunk_key",
    "verify_live_chunk_presence",
    "BranchHeadTrackedReachability",
    "tracked_reachability",
    "AuthenticatedControlCommitReachability",
    "authenticated_control_commit_reachability",
    "decode_reachability_batch",
    "blob_id_from_snapshot",
    "scan_packed_current_base_provenance_rows",
    "tracked_serving_commit_dependencies",
    "extend_registry_wasm_roots",
    "decode_upload_manifest_leaf_upload_id",
    "validate_upload_id_for_storage",
    "invalid_upload_storage",
    "decode_current_state_data_part_commit_ids",
    "validate_selected_owner_record",
    "may_contain_finite_selected_members",
    "RepositoryGcCommitBenchResult",
    "collect_repository_gc_for_bench",
];

// These names describe retained public semantics or facades, never legacy
// physical ownership. They are intentionally not residue tokens. Their bodies
// must be replaced by the ForkTree owner before the first runnable compile.
const SEMANTIC_ALLOWLIST: &[&str] = &[
    "BlobDataReader",
    "BranchContext",
    "BranchLifecycle",
    "BranchOperation",
    "ChangeId",
    "ChangelogContext",
    "CommitId",
    "Engine",
    "LiveStateContext",
    "LiveStateReader",
    "SessionContext",
    "load_plugin_registry_at_commit",
    "stage_repository_gc",
    "stage_repository_gc_with_preconditions",
    "upsert_file_content_part",
    "lix_branch",
    "lix_branch_ref",
    "lix_change",
    "lix_commit",
];

const SEMANTIC_ALLOWLIST_TSV: &str = include_str!("PUBLIC_SEMANTIC_ALLOWLIST.tsv");

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

// The foundational cursor PR owns deletion of this API and every wrapper that
// reconstructs a backend iterator from a continuation key. `ScanChunk` is the
// one valid owned page shape and therefore is deliberately not listed here.
const OLD_SCAN_IDENTIFIERS: &[&str] = &[
    "ScanOptions",
    "StorageScanOptions",
    "ScanPlan",
    "ScanPlanCursor",
    "resume_after",
    "scan_resume_after",
    "expected_resume_after",
];

const OLD_SCAN_PATTERNS: &[&str] = &[
    ".first_page(",
    ".page(",
    "fn first_page(",
    "async fn first_page(",
    "StorageRead::scan",
    "read.scan(",
    "older.scan(",
    "snapshot_scan_cursor",
    "stateless fallback",
    "slate_scan_resume",
    "scan_resume_cache",
    "resume_cache",
    "scan_cursor: Mutex<Option<ScanCursor>>",
];

const REQUIRED_CURSOR_TOKENS: &[&str] = &[
    "pub struct BeginScanOptions",
    "pub enum ScanOrder",
    "pub struct ScanChunk",
    "pub struct ScanCursor<'",
    "pub async fn next_page",
    "#[doc(hidden)]\npub trait StorageScanSource",
    "fn begin_scan(",
    "Capability::ReverseScan",
];

// Every retained Rust caller is scanned, including CLI, local-filesystem,
// support crates, benches, examples, and tests. Only the scanner's own
// compile-probe sources are excluded because they intentionally spell the
// forbidden API. SQLite is excluded from conformance and required to be
// physically deleted instead.
const WORKSPACE_RUST_ROOT: &str = "packages";
const EXCLUDED_SOURCE_PREFIXES: &[&str] = &[
    "packages/lix/tests/forktree_stage2_execution_oracle/",
    "packages/sqlite-storage/",
];

const SQLITE_DELETION_TOKENS: &[&str] = &[
    "lix_storage_sqlite",
    "StorageProfile::SQLite",
    "LixStorageProfile::SQLite",
    "SQLiteTempBenchStorage",
    "ChangelogScoreStorage::SQLite",
];

const SQLITE_DELETION_PATH: &str = "packages/sqlite-storage";

const REMOVED_STORAGE_OWNER_TOKENS: &[&str] = &[
    "FileLix",
    "FileStorage",
    "FileStorageRead",
    "FileStorageWrite",
];

const REQUIRED_CLI_ROCKS_TOKENS: &[&str] =
    &["lix_storage_rocksdb", "RocksDB::open", "Lix<RocksDB>"];

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

#[derive(Clone, Copy)]
enum SemanticKind {
    Value,
    Facade,
    Relation,
}

#[derive(Clone, Copy)]
struct SemanticRule {
    symbol: &'static str,
    kind: SemanticKind,
}

const SEMANTIC_RULES: &[SemanticRule] = &[
    SemanticRule {
        symbol: "BlobDataReader",
        kind: SemanticKind::Facade,
    },
    SemanticRule {
        symbol: "BranchContext",
        kind: SemanticKind::Facade,
    },
    SemanticRule {
        symbol: "BranchLifecycle",
        kind: SemanticKind::Facade,
    },
    SemanticRule {
        symbol: "BranchOperation",
        kind: SemanticKind::Value,
    },
    SemanticRule {
        symbol: "ChangeId",
        kind: SemanticKind::Value,
    },
    SemanticRule {
        symbol: "ChangelogContext",
        kind: SemanticKind::Facade,
    },
    SemanticRule {
        symbol: "CommitId",
        kind: SemanticKind::Value,
    },
    SemanticRule {
        symbol: "Engine",
        kind: SemanticKind::Facade,
    },
    SemanticRule {
        symbol: "LiveStateContext",
        kind: SemanticKind::Facade,
    },
    SemanticRule {
        symbol: "LiveStateReader",
        kind: SemanticKind::Facade,
    },
    SemanticRule {
        symbol: "SessionContext",
        kind: SemanticKind::Facade,
    },
    SemanticRule {
        symbol: "load_plugin_registry_at_commit",
        kind: SemanticKind::Facade,
    },
    SemanticRule {
        symbol: "stage_repository_gc",
        kind: SemanticKind::Facade,
    },
    SemanticRule {
        symbol: "stage_repository_gc_with_preconditions",
        kind: SemanticKind::Facade,
    },
    SemanticRule {
        symbol: "upsert_file_content_part",
        kind: SemanticKind::Facade,
    },
    SemanticRule {
        symbol: "lix_branch",
        kind: SemanticKind::Relation,
    },
    SemanticRule {
        symbol: "lix_branch_ref",
        kind: SemanticKind::Relation,
    },
    SemanticRule {
        symbol: "lix_change",
        kind: SemanticKind::Relation,
    },
    SemanticRule {
        symbol: "lix_commit",
        kind: SemanticKind::Relation,
    },
];

const RAW_STORAGE_TOKENS: &[&str] = &[
    "StorageSpace",
    ".begin_write(",
    ".put_many(",
    ".delete_many(",
    ".delete_range(",
    "StorageWriteSet",
];

#[derive(Debug, Eq, PartialEq)]
struct Finding {
    class: &'static str,
    item: String,
    count: usize,
}

#[derive(Debug)]
struct SourceFile {
    path: String,
    source: String,
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

fn collect_named(
    root: &Path,
    current: &Path,
    name: &str,
    files: &mut Vec<PathBuf>,
) -> Result<(), String> {
    for entry in
        fs::read_dir(current).map_err(|error| format!("read_dir {}: {error}", current.display()))?
    {
        let entry = entry.map_err(|error| format!("directory entry: {error}"))?;
        let path = entry.path();
        let kind = entry
            .file_type()
            .map_err(|error| format!("file_type {}: {error}", path.display()))?;
        if kind.is_dir() {
            collect_named(root, &path, name, files)?;
        } else if path.file_name().and_then(|value| value.to_str()) == Some(name) {
            files.push(path.strip_prefix(root).unwrap_or(&path).to_path_buf());
        }
    }
    Ok(())
}

fn count(source: &str, needle: &str) -> usize {
    source.match_indices(needle).count()
}

fn count_identifier(source: &str, needle: &str) -> usize {
    source
        .match_indices(needle)
        .filter(|(offset, _)| {
            let before = source[..*offset].chars().next_back();
            let after = source[*offset + needle.len()..].chars().next();
            let is_ident = |character: char| character == '_' || character.is_alphanumeric();
            before.is_none_or(|character| !is_ident(character))
                && after.is_none_or(|character| !is_ident(character))
        })
        .count()
}

/// Masks comments and literals while preserving byte offsets and newlines.
/// This is sufficient for item-boundary analysis without accepting a
/// first-`#[cfg(test)]` truncation that can hide later production items.
fn rust_code_mask(source: &str) -> Vec<u8> {
    let bytes = source.as_bytes();
    let mut mask = bytes.to_vec();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index..].starts_with(b"//") {
            let end = bytes[index..]
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(bytes.len(), |offset| index + offset);
            for byte in &mut mask[index..end] {
                *byte = b' ';
            }
            index = end;
            continue;
        }
        if bytes[index..].starts_with(b"/*") {
            let start = index;
            index += 2;
            let mut depth = 1_usize;
            while index < bytes.len() && depth != 0 {
                if bytes[index..].starts_with(b"/*") {
                    depth += 1;
                    index += 2;
                } else if bytes[index..].starts_with(b"*/") {
                    depth -= 1;
                    index += 2;
                } else {
                    index += 1;
                }
            }
            for (offset, byte) in mask[start..index].iter_mut().enumerate() {
                if bytes[start + offset] != b'\n' {
                    *byte = b' ';
                }
            }
            continue;
        }
        if bytes[index] == b'r' {
            let mut hashes = 0_usize;
            let mut cursor = index + 1;
            while cursor < bytes.len() && bytes[cursor] == b'#' {
                hashes += 1;
                cursor += 1;
            }
            if cursor < bytes.len() && bytes[cursor] == b'"' {
                let start = index;
                cursor += 1;
                loop {
                    let Some(relative) = bytes[cursor..].iter().position(|byte| *byte == b'"')
                    else {
                        cursor = bytes.len();
                        break;
                    };
                    cursor += relative + 1;
                    if bytes.get(cursor..cursor + hashes) == Some(&vec![b'#'; hashes][..]) {
                        cursor += hashes;
                        break;
                    }
                }
                for (offset, byte) in mask[start..cursor].iter_mut().enumerate() {
                    if bytes[start + offset] != b'\n' {
                        *byte = b' ';
                    }
                }
                index = cursor;
                continue;
            }
        }
        let character_literal = bytes[index] == b'\''
            && ((bytes.get(index + 1) == Some(&b'\\')
                && bytes
                    .get(index + 2..(index + 8).min(bytes.len()))
                    .is_some_and(|tail| tail.contains(&b'\'')))
                || bytes.get(index + 2) == Some(&b'\''));
        if bytes[index] == b'"' || character_literal {
            let quote = bytes[index];
            let start = index;
            index += 1;
            while index < bytes.len() {
                if bytes[index] == b'\\' {
                    index = (index + 2).min(bytes.len());
                } else if bytes[index] == quote {
                    index += 1;
                    break;
                } else {
                    index += 1;
                }
            }
            if index > start + 1 {
                for (offset, byte) in mask[start..index].iter_mut().enumerate() {
                    if bytes[start + offset] != b'\n' {
                        *byte = b' ';
                    }
                }
            }
            continue;
        }
        index += 1;
    }
    mask
}

fn matching_delimiter(mask: &[u8], start: usize, open: u8, close: u8) -> Option<usize> {
    let mut depth = 0_usize;
    for (offset, byte) in mask[start..].iter().copied().enumerate() {
        if byte == open {
            depth += 1;
        } else if byte == close {
            depth = depth.checked_sub(1)?;
            if depth == 0 {
                return Some(start + offset + 1);
            }
        }
    }
    None
}

fn skip_space(mask: &[u8], mut index: usize) -> usize {
    while index < mask.len() && mask[index].is_ascii_whitespace() {
        index += 1;
    }
    index
}

fn item_end(mask: &[u8], start: usize) -> Option<usize> {
    let mut paren = 0_i32;
    let mut bracket = 0_i32;
    let mut index = start;
    while index < mask.len() {
        match mask[index] {
            b'(' => paren += 1,
            b')' => paren -= 1,
            b'[' => bracket += 1,
            b']' => bracket -= 1,
            b'{' if paren == 0 && bracket == 0 => {
                return matching_delimiter(mask, index, b'{', b'}');
            }
            b';' if paren == 0 && bracket == 0 => return Some(index + 1),
            _ => {}
        }
        index += 1;
    }
    None
}

fn is_test_cfg(attribute: &[u8]) -> bool {
    let compact = attribute
        .iter()
        .copied()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    // Only the exact test-only item may be removed. In particular,
    // `#[cfg(not(test))]` is production and an `any(test, feature = ...)` item
    // can be production, so substring matching would create a residue blind
    // spot.
    compact == b"#[cfg(test)]"
}

fn production_source(source: &str) -> String {
    let mask = rust_code_mask(source);
    let mut output = source.as_bytes().to_vec();
    let mut index = 0_usize;
    while index + 1 < mask.len() {
        if mask[index] != b'#' || mask[index + 1] != b'[' {
            index += 1;
            continue;
        }
        let Some(attribute_end) = matching_delimiter(&mask, index + 1, b'[', b']') else {
            break;
        };
        if !is_test_cfg(&mask[index..attribute_end]) {
            index = attribute_end;
            continue;
        }
        let mut item_start = skip_space(&mask, attribute_end);
        while item_start + 1 < mask.len()
            && mask[item_start] == b'#'
            && mask[item_start + 1] == b'['
        {
            let Some(end) = matching_delimiter(&mask, item_start + 1, b'[', b']') else {
                break;
            };
            item_start = skip_space(&mask, end);
        }
        let Some(end) = item_end(&mask, item_start) else {
            break;
        };
        for (offset, byte) in output[index..end].iter_mut().enumerate() {
            if source.as_bytes()[index + offset] != b'\n' {
                *byte = b' ';
            }
        }
        index = end;
    }
    String::from_utf8(output).expect("production source remains UTF-8")
}

fn identifier_at(mask: &[u8], index: usize, token: &[u8]) -> bool {
    if mask.get(index..index + token.len()) != Some(token) {
        return false;
    }
    let identifier = |byte: u8| byte == b'_' || byte.is_ascii_alphanumeric();
    mask.get(index.wrapping_sub(1))
        .is_none_or(|byte| !identifier(*byte))
        && mask
            .get(index + token.len())
            .is_none_or(|byte| !identifier(*byte))
}

fn all_declaration_items(source: &str) -> Vec<(String, String)> {
    let production = production_source(source);
    let mask = rust_code_mask(&production);
    let mut items = Vec::new();
    let mut index = 0_usize;
    const ITEM_KEYWORDS: &[&[u8]] = &[
        b"fn", b"struct", b"enum", b"trait", b"type", b"const", b"static", b"impl",
    ];
    while index < mask.len() {
        let Some(keyword) = ITEM_KEYWORDS
            .iter()
            .find(|keyword| identifier_at(&mask, index, keyword))
        else {
            index += 1;
            continue;
        };
        let Some(end) = item_end(&mask, index) else {
            index += keyword.len();
            continue;
        };
        let header_end = mask[index..end]
            .iter()
            .position(|byte| *byte == b'{' || *byte == b';')
            .map_or(end, |offset| index + offset);
        let header = &production[index..header_end];
        items.push((header.to_owned(), production[index..end].to_owned()));
        index += keyword.len();
    }
    items
}

fn inspect_semantic_facades(files: &[SourceFile]) -> Vec<Finding> {
    let mut findings = Vec::new();
    let items = files
        .iter()
        .filter(|file| file.path.ends_with(".rs"))
        .flat_map(|file| all_declaration_items(&file.source))
        .collect::<Vec<_>>();
    for rule in SEMANTIC_RULES {
        let mut existence = 0_usize;
        let mut fork_delegations = 0_usize;
        let mut raw_storage = 0_usize;
        match rule.kind {
            SemanticKind::Relation => {
                let quoted = format!("\"{}\"", rule.symbol);
                for (_, item) in items.iter().filter(|(_, item)| item.contains(&quoted)) {
                    existence += count(&item, &quoted);
                    if item.to_ascii_lowercase().contains("forktree") {
                        fork_delegations += 1;
                    }
                    raw_storage += RAW_STORAGE_TOKENS
                        .iter()
                        .map(|token| count(&item, token))
                        .sum::<usize>();
                }
            }
            SemanticKind::Value | SemanticKind::Facade => {
                for (_header, item) in items
                    .iter()
                    .filter(|(header, _)| count_identifier(header, rule.symbol) != 0)
                {
                    existence += 1;
                    if item.to_ascii_lowercase().contains("forktree") {
                        fork_delegations += 1;
                    }
                    raw_storage += RAW_STORAGE_TOKENS
                        .iter()
                        .map(|token| count(&item, token))
                        .sum::<usize>();
                    raw_storage += LEGACY_SPACES
                        .iter()
                        .map(|token| count(&item, token))
                        .sum::<usize>();
                }
            }
        }
        if existence == 0 {
            findings.push(Finding {
                class: "missing-public-semantic-facade",
                item: rule.symbol.to_owned(),
                count: 0,
            });
            continue;
        }
        if matches!(rule.kind, SemanticKind::Facade | SemanticKind::Relation)
            && fork_delegations == 0
        {
            findings.push(Finding {
                class: "semantic-facade-not-forktree",
                item: rule.symbol.to_owned(),
                count: 0,
            });
        }
        if raw_storage != 0 {
            findings.push(Finding {
                class: "semantic-facade-raw-storage",
                item: rule.symbol.to_owned(),
                count: raw_storage,
            });
        }
    }
    findings
}

// This deliberately small structural check is scoped to adapter implementations.
// A storage adapter may construct one backend iterator in `begin_scan`, but it
// must not reconstruct a new cursor from a continuation key inside a loop.
fn count_adapter_scan_reconstruction_loops(source: &str) -> usize {
    let production = production_source(source);
    let mut brace_depth = 0_i32;
    let mut loop_depths = Vec::new();
    let mut findings = 0;

    for raw_line in production.lines() {
        let line = raw_line.split("//").next().unwrap_or(raw_line);
        let trimmed = line.trim_start();
        let starts_loop = trimmed.starts_with("loop {")
            || trimmed.starts_with("while ")
            || trimmed.starts_with("for ");
        let opens = line.matches('{').count() as i32;
        let closes = line.matches('}').count() as i32;

        if starts_loop && opens != 0 {
            loop_depths.push(brace_depth + 1);
        }
        if !loop_depths.is_empty() {
            findings += count(line, ".begin_scan(");
        }

        brace_depth += opens - closes;
        loop_depths.retain(|loop_depth| *loop_depth <= brace_depth);
    }

    findings
}

fn inspect(source: &str, files: &BTreeSet<String>) -> Vec<Finding> {
    let mut findings = Vec::new();
    for (class, tokens) in [
        ("legacy-space", LEGACY_SPACES),
        ("legacy-owner-or-codec", LEGACY_OWNER_TOKENS),
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
        if files.iter().any(|path| path.ends_with(module)) {
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

fn is_production_source(file: &SourceFile) -> bool {
    file.path.ends_with(".rs") && file.path.contains("/src/")
}

fn inspect_sqlite_deletion(source: &str, files: &[SourceFile]) -> Vec<Finding> {
    let mut findings = Vec::new();
    for token in SQLITE_DELETION_TOKENS {
        let occurrences = count(source, token);
        if occurrences != 0 {
            findings.push(Finding {
                class: "sqlite-adapter-residue",
                item: (*token).to_owned(),
                count: occurrences,
            });
        }
    }
    if files
        .iter()
        .any(|file| file.path.starts_with(&format!("{SQLITE_DELETION_PATH}/")))
    {
        findings.push(Finding {
            class: "sqlite-adapter-package",
            item: SQLITE_DELETION_PATH.to_owned(),
            count: 1,
        });
    }
    findings
}

fn inspect_removed_storage_boundaries(files: &[SourceFile]) -> Vec<Finding> {
    let mut findings = Vec::new();
    let retained_source = files
        .iter()
        .filter(|file| !file.path.starts_with(&format!("{SQLITE_DELETION_PATH}/")))
        .map(|file| file.source.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    for token in REMOVED_STORAGE_OWNER_TOKENS {
        let occurrences = count_identifier(&retained_source, token);
        if occurrences != 0 {
            findings.push(Finding {
                class: "removed-storage-owner",
                item: (*token).to_owned(),
                count: occurrences,
            });
        }
    }
    let cli_boundary = files
        .iter()
        .filter(|file| {
            file.path == "packages/cli/Cargo.toml" || file.path == "packages/cli/src/db/mod.rs"
        })
        .map(|file| file.source.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    for token in REQUIRED_CLI_ROCKS_TOKENS {
        if !cli_boundary.contains(token) {
            findings.push(Finding {
                class: "missing-cli-rocksdb-default",
                item: (*token).to_owned(),
                count: 0,
            });
        }
    }
    findings
}

fn cursor_sources(files: &[SourceFile]) -> impl Iterator<Item = &SourceFile> {
    files.iter().filter(|file| file.path.ends_with(".rs"))
}

fn inspect_cursor(files: &[SourceFile]) -> Vec<Finding> {
    let cursor_files = cursor_sources(files).collect::<Vec<_>>();
    let cursor_source = cursor_files
        .iter()
        .map(|file| file.source.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let all_source = files
        .iter()
        .map(|file| file.source.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let mut findings = Vec::new();

    for identifier in OLD_SCAN_IDENTIFIERS {
        let occurrences = cursor_files
            .iter()
            .map(|file| count_identifier(&file.source, identifier))
            .sum();
        if occurrences != 0 {
            findings.push(Finding {
                class: "old-paginated-scan",
                item: (*identifier).to_owned(),
                count: occurrences,
            });
        }
    }
    for pattern in OLD_SCAN_PATTERNS {
        let occurrences = count(&cursor_source, pattern);
        if occurrences != 0 {
            findings.push(Finding {
                class: "old-paginated-scan",
                item: (*pattern).to_owned(),
                count: occurrences,
            });
        }
    }

    for token in REQUIRED_CURSOR_TOKENS {
        if !all_source.contains(token) {
            findings.push(Finding {
                class: "missing-streaming-cursor",
                item: (*token).to_owned(),
                count: 0,
            });
        }
    }

    for path in [
        "packages/lix/src/storage/in_memory.rs",
        "packages/rocksdb-storage/src/rocksdb.rs",
        "packages/slatedb-storage/src/slatedb.rs",
    ] {
        let Some(file) = files.iter().find(|file| file.path == path) else {
            findings.push(Finding {
                class: "missing-streaming-cursor",
                item: path.to_owned(),
                count: 0,
            });
            continue;
        };
        for token in ["fn begin_scan(", "Capability::ReverseScan"] {
            if !file.source.contains(token) {
                findings.push(Finding {
                    class: "missing-streaming-cursor",
                    item: format!("{path}:{token}"),
                    count: 0,
                });
            }
        }
        let reconstruction_loops = count_adapter_scan_reconstruction_loops(&file.source);
        if reconstruction_loops != 0 {
            findings.push(Finding {
                class: "adapter-scan-reconstruction-loop",
                item: path.to_owned(),
                count: reconstruction_loops,
            });
        }
    }

    let traits = files
        .iter()
        .find(|file| file.path == "packages/lix/src/storage/traits.rs");
    if traits.is_none_or(|file| !file.source.contains("fn begin_scan(")) {
        findings.push(Finding {
            class: "missing-streaming-cursor",
            item: "StorageRead::begin_scan".to_owned(),
            count: 0,
        });
    }
    let adapter_source = files
        .iter()
        .filter(|file| file.path.starts_with("packages/lix/src/storage_adapter/"))
        .map(|file| file.source.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    if !adapter_source.contains("trait StorageAdapterRead")
        || !adapter_source.contains("fn begin_scan(")
    {
        findings.push(Finding {
            class: "missing-streaming-cursor",
            item: "StorageAdapterRead::begin_scan".to_owned(),
            count: 0,
        });
    }

    findings.sort_by(|left, right| {
        (left.class, left.item.as_str()).cmp(&(right.class, right.item.as_str()))
    });
    findings
}

fn load_sources(root: &Path) -> Result<Vec<SourceFile>, String> {
    let mut files = Vec::new();
    let mut paths = Vec::new();
    collect_rs(root, &root.join(WORKSPACE_RUST_ROOT), &mut paths)?;
    for relative in paths {
        let relative_string = relative.to_string_lossy().replace('\\', "/");
        if EXCLUDED_SOURCE_PREFIXES
            .iter()
            .any(|prefix| relative_string.starts_with(prefix))
        {
            continue;
        }
        let path = root.join(&relative);
        files.push(SourceFile {
            path: relative_string,
            source: fs::read_to_string(&path)
                .map_err(|error| format!("read {}: {error}", path.display()))?,
        });
    }
    // Package manifests are part of the SQLite adapter deletion proof. They
    // are not cursor sources, but stale dependencies/features are blockers.
    let mut manifest_paths = Vec::new();
    collect_named(
        root,
        &root.join("packages"),
        "Cargo.toml",
        &mut manifest_paths,
    )?;
    if root.join("Cargo.toml").is_file() {
        manifest_paths.push(PathBuf::from("Cargo.toml"));
    }
    for relative in manifest_paths {
        let relative_string = relative.to_string_lossy().replace('\\', "/");
        let path = root.join(&relative);
        files.push(SourceFile {
            path: relative_string,
            source: fs::read_to_string(&path)
                .map_err(|error| format!("read {}: {error}", path.display()))?,
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn audit(root: &Path) -> Result<Vec<Finding>, String> {
    let files = load_sources(root)?;
    let mut authority_source = String::new();
    let mut all_source = String::new();
    let mut names = BTreeSet::new();
    for file in &files {
        all_source.push_str(&file.source);
        all_source.push('\n');
        if is_production_source(file) {
            authority_source.push_str(&file.source);
            authority_source.push('\n');
            names.insert(file.path.clone());
        }
    }
    let production_files = files
        .iter()
        .filter(|file| is_production_source(file))
        .map(|file| SourceFile {
            path: file.path.clone(),
            source: file.source.clone(),
        })
        .collect::<Vec<_>>();
    let mut findings = inspect(&authority_source, &names);
    findings.extend(inspect_cursor(&files));
    findings.extend(inspect_semantic_facades(&production_files));
    findings.extend(inspect_sqlite_deletion(&all_source, &files));
    findings.extend(inspect_removed_storage_boundaries(&files));
    findings.sort_by(|left, right| {
        (left.class, left.item.as_str()).cmp(&(right.class, right.item.as_str()))
    });
    Ok(findings)
}

fn cursor_audit(root: &Path) -> Result<Vec<Finding>, String> {
    Ok(inspect_cursor(&load_sources(root)?))
}

fn semantic_audit(root: &Path) -> Result<Vec<Finding>, String> {
    let files = load_sources(root)?;
    Ok(inspect_semantic_facades(
        &files
            .into_iter()
            .filter(is_production_source)
            .collect::<Vec<_>>(),
    ))
}

fn escaped(value: &str) -> String {
    value.chars().flat_map(char::escape_default).collect()
}

fn print_budget(root: &Path) -> Result<(), String> {
    let files = load_sources(root)?;
    let authority_source = files
        .iter()
        .filter(|file| is_production_source(file))
        .map(|file| file.source.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let all_source = files
        .iter()
        .map(|file| file.source.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let cursor_files = cursor_sources(&files).collect::<Vec<_>>();
    let cursor_source = cursor_files
        .iter()
        .map(|file| file.source.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    println!("disposition\tclass\titem\toccurrences");
    for token in LEGACY_SPACES {
        println!(
            "zero\tlegacy-space\t{}\t{}",
            escaped(token),
            count(&authority_source, token)
        );
    }
    for token in LEGACY_OWNER_TOKENS {
        println!(
            "zero\tlegacy-owner-or-codec\t{}\t{}",
            escaped(token),
            count(&authority_source, token)
        );
    }
    for module in DELETE_MODULES {
        let occurrences = usize::from(files.iter().any(|file| file.path.ends_with(module)));
        println!(
            "absent\tsuperseded-module\t{}\t{}",
            escaped(module),
            occurrences
        );
    }
    for token in SQLITE_DELETION_TOKENS {
        println!(
            "zero\tsqlite-adapter-residue\t{}\t{}",
            escaped(token),
            count(&all_source, token)
        );
    }
    println!(
        "absent\tsqlite-adapter-package\t{}\t{}",
        SQLITE_DELETION_PATH,
        usize::from(root.join(SQLITE_DELETION_PATH).exists())
    );
    for token in REMOVED_STORAGE_OWNER_TOKENS {
        println!(
            "zero\tremoved-storage-owner\t{}\t{}",
            escaped(token),
            files
                .iter()
                .filter(|file| !file.path.starts_with(&format!("{SQLITE_DELETION_PATH}/")))
                .map(|file| count_identifier(&file.source, token))
                .sum::<usize>()
        );
    }
    let cli_boundary = files
        .iter()
        .filter(|file| {
            file.path == "packages/cli/Cargo.toml" || file.path == "packages/cli/src/db/mod.rs"
        })
        .map(|file| file.source.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    for token in REQUIRED_CLI_ROCKS_TOKENS {
        println!(
            "present\tcli-rocksdb-default\t{}\t{}",
            escaped(token),
            count(&cli_boundary, token)
        );
    }
    for token in OLD_SCAN_IDENTIFIERS {
        let occurrences = cursor_files
            .iter()
            .map(|file| count_identifier(&file.source, token))
            .sum::<usize>();
        println!(
            "zero\told-scan-identifier\t{}\t{}",
            escaped(token),
            occurrences
        );
    }
    for token in OLD_SCAN_PATTERNS {
        println!(
            "zero\told-scan-pattern\t{}\t{}",
            escaped(token),
            count(&cursor_source, token)
        );
    }
    for token in UNSEALED_TOKENS {
        println!(
            "zero\tunsealed-owner\t{}\t{}",
            escaped(token),
            count(&authority_source, token)
        );
    }
    for token in REQUIRED_OWNER_TOKENS {
        println!(
            "present\trequired-owner\t{}\t{}",
            escaped(token),
            count(&all_source, token)
        );
    }
    for token in REQUIRED_CURSOR_TOKENS {
        println!(
            "present\trequired-cursor\t{}\t{}",
            escaped(token),
            count(&all_source, token)
        );
    }
    for token in SEMANTIC_ALLOWLIST {
        println!(
            "allow\tpublic-semantic\t{}\t{}",
            escaped(token),
            count_identifier(&authority_source, token)
        );
    }
    let semantic_findings = inspect_semantic_facades(
        &files
            .iter()
            .filter(|file| is_production_source(file))
            .map(|file| SourceFile {
                path: file.path.clone(),
                source: file.source.clone(),
            })
            .collect::<Vec<_>>(),
    );
    for rule in SEMANTIC_RULES {
        println!(
            "pass\tsemantic-facade-contract\t{}\t{}",
            escaped(rule.symbol),
            usize::from(
                !semantic_findings
                    .iter()
                    .any(|finding| finding.item == rule.symbol)
            )
        );
    }
    Ok(())
}

fn declaration(line: &str) -> Option<(&'static str, String)> {
    const KINDS: &[&str] = &["fn", "struct", "enum", "trait", "type", "const", "static"];
    let words = line
        .split(|character: char| !(character == '_' || character.is_alphanumeric()))
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    for pair in words.windows(2) {
        if let Some(kind) = KINDS.iter().find(|kind| **kind == pair[0]) {
            return Some((kind, pair[1].to_owned()));
        }
    }
    None
}

fn print_deleted_module_definitions(root: &Path) -> Result<(), String> {
    let files = load_sources(root)?;
    let all_source = files
        .iter()
        .map(|file| file.source.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    println!("module\tkind\tsymbol\tproduction_occurrences");
    for module in DELETE_MODULES {
        let file = files.iter().find(|file| file.path.ends_with(module));
        let Some(file) = file else {
            continue;
        };
        let production = production_source(&file.source);
        let mut definitions = BTreeSet::new();
        for raw_line in production.lines() {
            let line = raw_line.split("//").next().unwrap_or(raw_line);
            if let Some((kind, symbol)) = declaration(line) {
                definitions.insert((kind, symbol));
            }
        }
        for (kind, symbol) in definitions {
            println!(
                "{}\t{}\t{}\t{}",
                escaped(module),
                kind,
                escaped(&symbol),
                count_identifier(&all_source, &symbol)
            );
        }
    }
    Ok(())
}

fn self_test() -> Result<(), String> {
    let forbidden = LEGACY_SPACES
        .iter()
        .chain(LEGACY_OWNER_TOKENS)
        .chain(OLD_SCAN_IDENTIFIERS)
        .copied()
        .collect::<BTreeSet<_>>();
    if let Some(overlap) = SEMANTIC_ALLOWLIST
        .iter()
        .find(|token| forbidden.contains(**token))
    {
        return Err(format!(
            "semantic allowlist overlaps forbidden token {overlap}"
        ));
    }
    let rule_symbols = SEMANTIC_RULES
        .iter()
        .map(|rule| rule.symbol)
        .collect::<BTreeSet<_>>();
    let allowlist_symbols = SEMANTIC_ALLOWLIST.iter().copied().collect::<BTreeSet<_>>();
    if rule_symbols != allowlist_symbols {
        return Err("semantic allowlist and executable rules differ".to_owned());
    }
    let tsv_symbols = SEMANTIC_ALLOWLIST_TSV
        .lines()
        .skip(1)
        .filter_map(|line| {
            let mut fields = line.split('\t');
            let class = fields.next()?;
            let symbol = fields.next()?;
            (class == "semantic").then_some(symbol)
        })
        .collect::<BTreeSet<_>>();
    if rule_symbols != tsv_symbols {
        return Err("semantic TSV and executable rules differ".to_owned());
    }
    let required = REQUIRED_OWNER_TOKENS.join("\n");
    let clean = inspect(&required, &BTreeSet::new());
    if !clean.is_empty() {
        return Err(format!("synthetic clean source rejected: {clean:?}"));
    }
    let dirty_source = format!(
        "{required}\n{}\n{}\n{}",
        LEGACY_SPACES[0], LEGACY_OWNER_TOKENS[0], UNSEALED_TOKENS[0]
    );
    let mut dirty_files = BTreeSet::new();
    dirty_files.insert(DELETE_MODULES[0].to_owned());
    let dirty = inspect(&dirty_source, &dirty_files);
    for class in [
        "legacy-space",
        "legacy-owner-or-codec",
        "unsealed-owner",
        "superseded-module",
    ] {
        if !dirty.iter().any(|finding| finding.class == class) {
            return Err(format!("synthetic dirty source missed {class}"));
        }
    }
    let clean_cursor = REQUIRED_CURSOR_TOKENS.join("\n");
    let clean_cursor_files = vec![
        SourceFile {
            path: "packages/lix/src/storage/traits.rs".to_owned(),
            source: format!("{clean_cursor}\nfn begin_scan("),
        },
        SourceFile {
            path: "packages/lix/src/storage_adapter/context.rs".to_owned(),
            source: "trait StorageAdapterRead {} fn begin_scan(".to_owned(),
        },
        SourceFile {
            path: "packages/lix/src/storage/in_memory.rs".to_owned(),
            source: "fn begin_scan( Capability::ReverseScan".to_owned(),
        },
        SourceFile {
            path: "packages/rocksdb-storage/src/rocksdb.rs".to_owned(),
            source: "fn begin_scan( Capability::ReverseScan".to_owned(),
        },
        SourceFile {
            path: "packages/slatedb-storage/src/slatedb.rs".to_owned(),
            source: "fn begin_scan( Capability::ReverseScan".to_owned(),
        },
    ];
    if !inspect_cursor(&clean_cursor_files).is_empty() {
        return Err("synthetic clean cursor source rejected".to_owned());
    }
    let mut dirty_cursor_files = clean_cursor_files;
    dirty_cursor_files[0]
        .source
        .push_str("\nScanPlan resume_after");
    let dirty_cursor = inspect_cursor(&dirty_cursor_files);
    if !dirty_cursor
        .iter()
        .any(|finding| finding.class == "old-paginated-scan")
    {
        return Err("synthetic dirty cursor source missed residue".to_owned());
    }
    let reconstruction = "fn adapter() {\nloop {\nread.begin_scan(space, range, options);\n}\n}";
    if count_adapter_scan_reconstruction_loops(reconstruction) != 1 {
        return Err("synthetic adapter reconstruction loop was not rejected".to_owned());
    }
    let test_only_reconstruction =
        "fn begin_scan() {}\n#[cfg(test)]\nfn fixture() { loop { read.begin_scan(); } }";
    if count_adapter_scan_reconstruction_loops(test_only_reconstruction) != 0 {
        return Err("test-only adapter reconstruction loop was treated as production".to_owned());
    }
    let interleaved = r#"
fn production_before() {}
#[cfg(test)]
mod tests { fn hidden() { loop { read.begin_scan(); } } }
#[cfg(not(test))]
fn production_not_test() {
    loop { read.begin_scan(); }
}
#[cfg(any(test, feature = "retained"))]
fn potentially_production() {
    loop { read.begin_scan(); }
}
fn production_after() {
    loop {
        read.begin_scan();
    }
}
impl Owner {
    #[cfg(test)]
    fn test_only(&self) { read.begin_scan(); }
    fn production_method(&self) {
        loop {
            read.begin_scan();
        }
    }
}
"#;
    let stripped = production_source(interleaved);
    if stripped.contains("hidden") || stripped.contains("test_only") {
        return Err("item-scoped cfg(test) extraction retained test declarations".to_owned());
    }
    if !stripped.contains("production_before")
        || !stripped.contains("production_after")
        || !stripped.contains("production_not_test")
        || !stripped.contains("potentially_production")
        || !stripped.contains("production_method")
    {
        return Err("item-scoped cfg(test) extraction hid production declarations".to_owned());
    }
    if count_adapter_scan_reconstruction_loops(interleaved) != 4 {
        return Err("item-scoped loop scan did not preserve all production items".to_owned());
    }
    println!("forktree-stage2-execution-oracle self-test PASS");
    Ok(())
}

fn print_findings(findings: &[Finding]) {
    for finding in findings {
        println!(
            "{}\t{}\t{}",
            finding.class,
            escaped(&finding.item),
            finding.count
        );
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
        Some("cursor-baseline") => {
            let root = PathBuf::from(args.next().ok_or("cursor-baseline requires repository root")?);
            let findings = cursor_audit(&root)?;
            if findings.is_empty() {
                return Err("cursor baseline unexpectedly has zero residue".to_owned());
            }
            print_findings(&findings);
            Ok(())
        }
        Some("cursor-audit") => {
            let root = PathBuf::from(args.next().ok_or("cursor-audit requires repository root")?);
            let findings = cursor_audit(&root)?;
            if findings.is_empty() {
                println!("forktree-stage2-execution-oracle cursor-audit PASS");
                Ok(())
            } else {
                print_findings(&findings);
                Err("cursor hard cut retains forbidden residue".to_owned())
            }
        }
        Some("semantic-audit") => {
            let root = PathBuf::from(args.next().ok_or("semantic-audit requires repository root")?);
            let findings = semantic_audit(&root)?;
            if findings.is_empty() {
                println!("forktree-stage2-execution-oracle semantic-audit PASS");
                Ok(())
            } else {
                print_findings(&findings);
                Err("semantic facades are absent, non-ForkTree, or own raw storage".to_owned())
            }
        }
        Some("budget") => {
            let root = PathBuf::from(args.next().ok_or("budget requires repository root")?);
            print_budget(&root)
        }
        Some("definitions") => {
            let root = PathBuf::from(args.next().ok_or("definitions requires repository root")?);
            print_deleted_module_definitions(&root)
        }
        _ => Err(
            "usage: oracle <self-test|baseline REPO|audit REPO|cursor-baseline REPO|cursor-audit REPO|semantic-audit REPO|budget REPO|definitions REPO>".to_owned(),
        ),
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
