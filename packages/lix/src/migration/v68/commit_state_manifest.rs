//! Frozen v68 (`LXCS10` / `LXMI1`) commit-state authority reader.

use bytes::Bytes;

use crate::changelog::CommitId;
use crate::common::LixTimestamp;
use crate::storage_adapter::{
    StorageAdapterRead, StorageGetManyRequest, StorageGetOptions, StorageKey,
    StorageProjectedValue, exact_get_many,
};
use crate::tracked_state::{
    LAYOUT_BOUNDED_DIRECT, LAYOUT_BOUNDED_INDIRECT, LAYOUT_COMPACT_REPLACEMENT,
    LAYOUT_DIRECT_ROWS_ONLY, MutationDirectoryEntry, MutationDirectoryFullTraversalContext,
    MutationDirectoryReadSelection, MutationDirectoryRoot, load_mutation_part_read_plan,
    validate_mutation_directory_root,
    ScopedRangeRoot,
    COMMIT_STATE_MAX_REPLAY_BYTES, COMMIT_STATE_MAX_REPLAY_DEPTH, ColumnarMutationPartSet,
    CommitDeltaLifecycleSummary, CommitDeltaReplacementScope, CommitStateManifest,
    CommitStateReplayDebt, TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
    TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE, TrackedStateCommitRoot,
};
use crate::{LixError, storage_codec};

const HEADER_MAGIC: &[u8] = b"LXCS10";
const INVENTORY_MAGIC: &[u8] = b"LXMI1";
const TOUCHED_SCOPE_FILTER_BYTES: usize = 128;
const DIRECT_PART_MAX_ROWS: usize = 512;

#[derive(Clone, musli::Encode, musli::Decode)]
#[musli(packed)]
struct HeaderV68 {
    commit_id: CommitId,
    change_account_id: String,
    replay_debt: CommitStateReplayDebt,
    #[musli(with = storage_codec::option)]
    selected_source_commit_id: Option<[u8; 16]>,
    mutation_inventory_digest: [u8; 32],
    mutation_transition_digest: [u8; 32],
    mutation_member_count: u32,
    mutation_part_count: u32,
    #[musli(with = storage_codec::option)]
    mutation_directory_root: Option<MutationDirectoryRoot>,
    touched_scope_filter: TouchedScopeFilterWire,
    #[musli(with = storage_codec::option)]
    current_state_scoped_ranges: Option<Box<ScopedRangeAuthorityWire>>,
    #[musli(with = storage_codec::option)]
    snapshot_root: Option<Box<TrackedStateCommitRoot>>,
}

#[derive(Clone, musli::Encode, musli::Decode)]
#[musli(packed)]
struct InventoryV68 {
    member_count: u32,
    selection_fingerprint: [u8; 32],
    #[musli(with = storage_codec::option)]
    single_partition: Option<CommitDeltaReplacementScope>,
    #[musli(with = storage_codec::option)]
    lifecycle_summary: Option<CommitDeltaLifecycleSummary>,
    #[musli(with = storage_codec::option)]
    replacement_generation: Option<ReplacementGenerationWire>,
    #[musli(with = storage_codec::option)]
    replacement_parts: Option<ReplacementPartsAuthorityWire>,
    #[musli(with = storage_codec::option)]
    columnar_parts: Option<ColumnarMutationPartSet>,
    #[musli(bytes)]
    inline_part: Vec<u8>,
    inline_direct: bool,
    #[musli(with = storage_codec::option)]
    directory_root: Option<MutationDirectoryRoot>,
}

// Packed wire mirrors let migration retain the old layout without widening
// the live tracked-state API for its private nested types.
#[derive(Clone, musli::Encode, musli::Decode)]
#[musli(packed)]
struct TouchedScopeFilterWire {
    complete: bool,
    #[musli(bytes)]
    bits: Vec<u8>,
}

#[derive(Clone, musli::Encode, musli::Decode)]
#[musli(packed)]
struct ScopedRangeAuthorityWire {
    tree: ScopedRangeRoot,
    #[musli(with = storage_codec::option)]
    serving_base_commit_id: Option<CommitId>,
    #[musli(with = storage_codec::option)]
    serving_base_root_id: Option<[u8; 32]>,
    transition_digest: [u8; 32],
}

#[derive(Clone, musli::Encode, musli::Decode)]
#[musli(packed)]
struct ReplacementGenerationWire {
    owner_commit_id: [u8; 16],
    scope: CommitDeltaReplacementScope,
    #[musli(with = storage_codec::option)]
    fallback_commit_id: Option<[u8; 16]>,
    integrity_digest: [u8; 32],
}

#[derive(Clone, musli::Encode, musli::Decode)]
#[musli(packed)]
struct ReplacementPartsAuthorityWire {
    directory_digest: [u8; 32],
    uniform_updated_at: LixTimestamp,
}

#[derive(Clone, musli::Encode, musli::Decode)]
#[musli(packed)]
struct ReplacementPartWire {
    content_digest: [u8; 32],
    owner_commit_id: [u8; 16],
    first_address: u32,
    uniform_created_at: LixTimestamp,
    uniform_updated_at: LixTimestamp,
}

#[derive(Clone, musli::Encode, musli::Decode)]
#[musli(packed)]
struct MutationPartWire {
    #[musli(bytes)]
    first_key: Vec<u8>,
    #[musli(bytes)]
    last_key: Vec<u8>,
    #[musli(with = storage_codec::option)]
    replacement_part: Option<ReplacementPartWire>,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct CurrentInventoryWire {
    #[musli(with = storage_codec::option)]
    selected_source_commit_id: Option<[u8; 16]>,
    member_count: u32,
    selection_fingerprint: [u8; 32],
    direct_part_row_counts: Vec<u16>,
    direct_part_ownership: Vec<Vec<u8>>,
    replacement_part_digests: Vec<[u8; 32]>,
    #[musli(with = storage_codec::option)]
    single_partition: Option<CommitDeltaReplacementScope>,
    #[musli(with = storage_codec::option)]
    lifecycle_summary: Option<CommitDeltaLifecycleSummary>,
    #[musli(with = storage_codec::option)]
    replacement_generation: Option<ReplacementGenerationWire>,
    #[musli(with = storage_codec::option)]
    replacement_parts: Option<ReplacementPartsAuthorityWire>,
    #[musli(with = storage_codec::option)]
    columnar_parts: Option<ColumnarMutationPartSet>,
    #[musli(bytes)]
    inline_part: Vec<u8>,
    parts: Vec<MutationPartWire>,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct CurrentManifestWire {
    commit_id: CommitId,
    change_account_id: String,
    replay_debt: CommitStateReplayDebt,
    mutations: CurrentInventoryWire,
    touched_scope_filter: TouchedScopeFilterWire,
    #[musli(with = storage_codec::option)]
    current_state_scoped_ranges: Option<Box<ScopedRangeAuthorityWire>>,
    #[musli(with = storage_codec::option)]
    snapshot_root: Option<Box<TrackedStateCommitRoot>>,
}

/// Loads and authenticates one v68 split authority, then upgrades implicit
/// direct-coordinate ownership to the explicit current manifest shape.
pub(in crate::migration) async fn load_commit_state_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<CommitStateManifest>, LixError> {
    let key = StorageKey(Bytes::copy_from_slice(commit_id.as_uuid().as_bytes()));
    let header_keys = [key.clone()];
    let inventory_keys = [key];
    let requests = [
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            keys: &header_keys,
            opts: StorageGetOptions::default(),
        },
        StorageGetManyRequest {
            space: TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
            keys: &inventory_keys,
            opts: StorageGetOptions::default(),
        },
    ];
    let mut values = exact_get_many(store, &requests).await?.values.into_iter();
    let header = full_value(values.next().flatten());
    let inventory = full_value(values.next().flatten());
    let (header, inventory) = match (header, inventory) {
        (None, None) => return Ok(None),
        (Some(header), Some(inventory)) => (header, inventory),
        _ => return Err(authority_error("has incomplete split physical authority")),
    };
    let manifest = decode_manifest(store, &header, &inventory).await?;
    if manifest.commit_id != commit_id {
        return Err(authority_error(
            "key contains authority for a different commit",
        ));
    }
    Ok(Some(manifest))
}

async fn decode_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    header: &[u8],
    inventory: &[u8],
) -> Result<CommitStateManifest, LixError> {
    let header_payload = header
        .strip_prefix(HEADER_MAGIC)
        .ok_or_else(|| authority_error("header has an unsupported format"))?;
    let stored: HeaderV68 =
        storage_codec::decode("v68 tracked_state commit_state_manifest", header_payload)?;
    validate_header(&stored)?;
    if stored.mutation_inventory_digest != *blake3::hash(inventory).as_bytes() {
        return Err(authority_error(
            "inventory disagrees with its authority digest",
        ));
    }
    let inventory_payload = inventory
        .strip_prefix(INVENTORY_MAGIC)
        .ok_or_else(|| authority_error("inventory has an unsupported format"))?;
    let catalog: InventoryV68 = storage_codec::decode(
        "v68 tracked_state commit mutation inventory",
        inventory_payload,
    )?;
    if stored.mutation_member_count != catalog.member_count
        || stored.mutation_directory_root != catalog.directory_root
    {
        return Err(authority_error(
            "inventory disagrees with its authority header",
        ));
    }
    let entries = match catalog.directory_root.as_ref() {
        Some(root) => load_mutation_part_read_plan(
            store,
            root,
            MutationDirectoryReadSelection::All(
                MutationDirectoryFullTraversalContext::FullManifestExpansion,
            ),
        )
        .await?
        .into_runs()
        .into_iter()
        .map(|run| run.entry)
        .collect(),
        None => Vec::new(),
    };
    assemble_manifest(stored, catalog, entries)
}

fn assemble_manifest(
    stored: HeaderV68,
    catalog: InventoryV68,
    entries: Vec<MutationDirectoryEntry>,
) -> Result<CommitStateManifest, LixError> {
    let mut parts = Vec::new();
    let mut direct_rows = Vec::new();
    let mut replacement_digests = Vec::new();
    match catalog.directory_root.as_ref().map(|root| root.layout) {
        Some(LAYOUT_BOUNDED_INDIRECT | LAYOUT_BOUNDED_DIRECT) => {
            let direct = catalog
                .directory_root
                .as_ref()
                .is_some_and(|root| root.layout == LAYOUT_BOUNDED_DIRECT);
            for entry in entries {
                let MutationDirectoryEntry::Bounded {
                    part,
                    direct_row_count,
                } = entry
                else {
                    return Err(authority_error(
                        "bounded directory contains a non-bounded entry",
                    ));
                };
                parts.push(transcode_part(&part)?);
                if direct {
                    direct_rows.push(direct_row_count);
                }
            }
        }
        Some(LAYOUT_COMPACT_REPLACEMENT) => {
            for entry in entries {
                let MutationDirectoryEntry::CompactReplacement {
                    content_digest,
                    direct_row_count,
                } = entry
                else {
                    return Err(authority_error(
                        "compact directory contains a non-compact entry",
                    ));
                };
                replacement_digests.push(content_digest);
                direct_rows.push(direct_row_count);
            }
        }
        Some(LAYOUT_DIRECT_ROWS_ONLY) => {
            for entry in entries {
                let MutationDirectoryEntry::DirectAddress { direct_row_count } = entry else {
                    return Err(authority_error(
                        "direct directory contains a physical part entry",
                    ));
                };
                direct_rows.push(direct_row_count);
            }
        }
        None if entries.is_empty() => {}
        _ => {
            return Err(authority_error(
                "directory has an unsupported authority layout",
            ));
        }
    }
    if catalog.inline_direct {
        let rows = u16::try_from(catalog.member_count)
            .map_err(|_| authority_error("inline direct row count exceeds u16"))?;
        if rows == 0 || !direct_rows.is_empty() {
            return Err(authority_error(
                "inline direct authority has an invalid directory",
            ));
        }
        direct_rows.push(rows);
    }

    let part_count = catalog
        .columnar_parts
        .as_ref()
        .map_or(0, |columnar| columnar.group_row_counts.len())
        + usize::from(!catalog.inline_part.is_empty())
        + if replacement_digests.is_empty() {
            parts.len()
        } else {
            replacement_digests.len()
        };
    if u32::try_from(part_count).ok() != Some(stored.mutation_part_count) {
        return Err(authority_error(
            "part closure disagrees with its authority header",
        ));
    }
    validate_inventory_shape(
        catalog.member_count,
        part_count,
        &direct_rows,
        &parts,
        &replacement_digests,
        catalog.inline_part.is_empty(),
        catalog.columnar_parts.is_some(),
        stored.selected_source_commit_id.is_some(),
    )?;
    if old_transition_digest(
        stored.selected_source_commit_id,
        &catalog,
        part_count,
        direct_rows.len(),
        parts.len(),
        replacement_digests.len(),
    ) != stored.mutation_transition_digest
    {
        return Err(authority_error(
            "inventory disagrees with its v68 transition digest",
        ));
    }

    let direct_part_ownership = direct_rows
        .iter()
        .copied()
        .map(full_ownership)
        .collect::<Vec<_>>();
    let current_transition_digest = current_transition_digest(
        stored.selected_source_commit_id,
        &catalog,
        part_count,
        parts.len(),
        replacement_digests.len(),
        &direct_part_ownership,
    );
    let mut current_state_scoped_ranges = stored.current_state_scoped_ranges;
    if let Some(root) = current_state_scoped_ranges.as_mut() {
        root.transition_digest = scoped_range_transition_digest(
            stored.commit_id,
            root.serving_base_commit_id,
            root.serving_base_root_id,
            current_transition_digest,
            &root.tree,
        );
    }
    let wire = CurrentManifestWire {
        commit_id: stored.commit_id,
        change_account_id: stored.change_account_id,
        replay_debt: stored.replay_debt,
        mutations: CurrentInventoryWire {
            selected_source_commit_id: stored.selected_source_commit_id,
            member_count: catalog.member_count,
            selection_fingerprint: catalog.selection_fingerprint,
            direct_part_row_counts: direct_rows,
            direct_part_ownership,
            replacement_part_digests: replacement_digests,
            single_partition: catalog.single_partition,
            lifecycle_summary: catalog.lifecycle_summary,
            replacement_generation: catalog.replacement_generation,
            replacement_parts: catalog.replacement_parts,
            columnar_parts: catalog.columnar_parts,
            inline_part: catalog.inline_part,
            parts,
        },
        touched_scope_filter: stored.touched_scope_filter,
        current_state_scoped_ranges,
        snapshot_root: stored.snapshot_root,
    };
    let encoded = storage_codec::encode("upgraded v68 commit-state manifest", &wire)?;
    storage_codec::decode("upgraded v68 commit-state manifest", &encoded)
}

fn validate_header(stored: &HeaderV68) -> Result<(), LixError> {
    if stored.mutation_inventory_digest == [0; 32]
        || stored.mutation_transition_digest == [0; 32]
        || stored.selected_source_commit_id == Some(*stored.commit_id.as_uuid().as_bytes())
    {
        return Err(authority_error("header has invalid mutation authority"));
    }
    if let Some(root) = stored.mutation_directory_root.as_ref() {
        validate_mutation_directory_root(root)?;
        if root.layout != LAYOUT_DIRECT_ROWS_ONLY && root.entry_count != stored.mutation_part_count
        {
            return Err(authority_error("directory count disagrees with its header"));
        }
    }
    if (stored.replay_debt.depth == 0
        && (stored.replay_debt.rows != 0 || stored.replay_debt.bytes != 0))
        || stored.replay_debt.depth > COMMIT_STATE_MAX_REPLAY_DEPTH
        || stored.replay_debt.bytes > COMMIT_STATE_MAX_REPLAY_BYTES
    {
        return Err(authority_error("header has invalid replay debt"));
    }
    if (stored.replay_debt.depth == 0 && stored.snapshot_root.is_none())
        || stored
            .snapshot_root
            .as_ref()
            .is_some_and(|root| root.commit_id != stored.commit_id || stored.replay_debt.depth != 0)
    {
        return Err(authority_error("header has invalid snapshot authority"));
    }
    let filter = &stored.touched_scope_filter;
    if (filter.complete && filter.bits.len() != TOUCHED_SCOPE_FILTER_BYTES)
        || (!filter.complete && !filter.bits.is_empty())
    {
        return Err(authority_error(
            "header has an invalid touched-scope filter",
        ));
    }
    if let Some(root) = stored.current_state_scoped_ranges.as_ref()
        && (root.tree.root_id == [0; 32]
            || root.tree.root_digest == [0; 32]
            || root.tree.tree_height == 0
            || root.tree.marker_count == 0
            || root.transition_digest == [0; 32]
            || root.serving_base_commit_id.is_some() != root.serving_base_root_id.is_some()
            || (stored.selected_source_commit_id.is_some()
                && root.serving_base_commit_id
                    != stored
                        .selected_source_commit_id
                        .map(|bytes| CommitId::new(uuid::Uuid::from_bytes(bytes))))
            || root.transition_digest
                != scoped_range_transition_digest(
                    stored.commit_id,
                    root.serving_base_commit_id,
                    root.serving_base_root_id,
                    stored.mutation_transition_digest,
                    &root.tree,
                ))
    {
        return Err(authority_error(
            "header has an invalid scoped-range authority",
        ));
    }
    Ok(())
}

fn validate_inventory_shape(
    member_count: u32,
    part_count: usize,
    direct_rows: &[u16],
    parts: &[MutationPartWire],
    replacement_digests: &[[u8; 32]],
    inline_is_empty: bool,
    has_columnar: bool,
    has_selected_source: bool,
) -> Result<(), LixError> {
    if parts.iter().any(|part| {
        part.first_key.is_empty() || part.last_key.is_empty() || part.first_key > part.last_key
    }) || parts
        .windows(2)
        .any(|pair| pair[0].last_key >= pair[1].first_key)
    {
        return Err(authority_error(
            "inventory has invalid or overlapping part bounds",
        ));
    }
    if replacement_digests.contains(&[0; 32]) {
        return Err(authority_error(
            "inventory has a zero replacement-part digest",
        ));
    }
    if member_count > 0 && part_count == 0 && !has_selected_source {
        return Err(authority_error(
            "inventory has members without mutation parts",
        ));
    }
    if !direct_rows.is_empty()
        && ((!has_columnar && direct_rows.len() != part_count)
            || direct_rows.iter().any(|&rows| rows == 0)
            || direct_rows
                .iter()
                .any(|&rows| usize::from(rows) > DIRECT_PART_MAX_ROWS)
            || direct_rows.iter().map(|&rows| u64::from(rows)).sum::<u64>()
                != u64::from(member_count))
    {
        return Err(authority_error("inventory has invalid direct addresses"));
    }
    if !inline_is_empty && (!parts.is_empty() || !replacement_digests.is_empty()) {
        return Err(authority_error("inventory mixes inline and external parts"));
    }
    Ok(())
}

fn old_transition_digest(
    selected_source: Option<[u8; 16]>,
    inventory: &InventoryV68,
    part_count: usize,
    direct_count: usize,
    parts_len: usize,
    replacement_len: usize,
) -> [u8; 32] {
    let mut digest = blake3::Hasher::new_derive_key("lix current-state transition authority v2");
    digest.update(&inventory.member_count.to_be_bytes());
    digest.update(&inventory.selection_fingerprint);
    digest.update(&[u8::from(selected_source.is_some())]);
    if let Some(source) = selected_source {
        digest.update(&source);
    }
    digest.update(&(part_count as u64).to_be_bytes());
    digest.update(&(direct_count as u64).to_be_bytes());
    let generic_count = if inventory.replacement_generation.is_some() {
        0
    } else {
        parts_len
    };
    digest.update(&(generic_count as u64).to_be_bytes());
    digest.update(&(replacement_len as u64).to_be_bytes());
    digest.update(&[u8::from(!inventory.inline_part.is_empty())]);
    if !inventory.inline_part.is_empty() {
        digest.update(blake3::hash(&inventory.inline_part).as_bytes());
    }
    digest.update(&[u8::from(inventory.single_partition.is_some())]);
    if let Some(scope) = inventory.single_partition.as_ref() {
        digest.update(&(scope.schema_key.len() as u64).to_be_bytes());
        digest.update(scope.schema_key.as_bytes());
        digest.update(&[u8::from(scope.file_id.is_some())]);
        if let Some(file_id) = scope.file_id.as_ref() {
            digest.update(&(file_id.len() as u64).to_be_bytes());
            digest.update(file_id.as_bytes());
        }
    }
    if let Some(generation) = inventory.replacement_generation.as_ref() {
        digest.update(&generation.owner_commit_id);
        digest.update(&generation.integrity_digest);
    }
    if let Some(authority) = inventory.replacement_parts.as_ref() {
        digest.update(&authority.directory_digest);
    }
    if let Some(columnar) = inventory.columnar_parts.as_ref() {
        digest.update(&columnar.owner_commit_id);
        digest.update(&columnar.row_group_set_id);
        digest.update(&columnar.manifest_digest);
        digest.update(&columnar.row_count.to_be_bytes());
    }
    *digest.finalize().as_bytes()
}

fn current_transition_digest(
    selected_source: Option<[u8; 16]>,
    inventory: &InventoryV68,
    part_count: usize,
    parts_len: usize,
    replacement_len: usize,
    ownership: &[Vec<u8>],
) -> [u8; 32] {
    let mut digest = blake3::Hasher::new_derive_key("lix current-state transition authority v2");
    digest.update(&inventory.member_count.to_be_bytes());
    digest.update(&inventory.selection_fingerprint);
    digest.update(&[u8::from(selected_source.is_some())]);
    if let Some(source) = selected_source {
        digest.update(&source);
    }
    digest.update(&(part_count as u64).to_be_bytes());
    digest.update(&(ownership.len() as u64).to_be_bytes());
    digest.update(&(ownership.len() as u64).to_be_bytes());
    for bitmap in ownership {
        digest.update(&(bitmap.len() as u64).to_be_bytes());
        digest.update(bitmap);
    }
    let generic_count = if inventory.replacement_generation.is_some() {
        0
    } else {
        parts_len
    };
    digest.update(&(generic_count as u64).to_be_bytes());
    digest.update(&(replacement_len as u64).to_be_bytes());
    digest.update(&[u8::from(!inventory.inline_part.is_empty())]);
    if !inventory.inline_part.is_empty() {
        digest.update(blake3::hash(&inventory.inline_part).as_bytes());
    }
    digest.update(&[u8::from(inventory.single_partition.is_some())]);
    if let Some(scope) = inventory.single_partition.as_ref() {
        digest.update(&(scope.schema_key.len() as u64).to_be_bytes());
        digest.update(scope.schema_key.as_bytes());
        digest.update(&[u8::from(scope.file_id.is_some())]);
        if let Some(file_id) = scope.file_id.as_ref() {
            digest.update(&(file_id.len() as u64).to_be_bytes());
            digest.update(file_id.as_bytes());
        }
    }
    if let Some(generation) = inventory.replacement_generation.as_ref() {
        digest.update(&generation.owner_commit_id);
        digest.update(&generation.integrity_digest);
    }
    if let Some(authority) = inventory.replacement_parts.as_ref() {
        digest.update(&authority.directory_digest);
    }
    if let Some(columnar) = inventory.columnar_parts.as_ref() {
        digest.update(&columnar.owner_commit_id);
        digest.update(&columnar.row_group_set_id);
        digest.update(&columnar.manifest_digest);
        digest.update(&columnar.row_count.to_be_bytes());
    }
    *digest.finalize().as_bytes()
}

fn scoped_range_transition_digest(
    commit_id: CommitId,
    base_commit_id: Option<CommitId>,
    base_root_id: Option<[u8; 32]>,
    mutation_digest: [u8; 32],
    tree: &ScopedRangeRoot,
) -> [u8; 32] {
    let mut digest = blake3::Hasher::new_derive_key("lix current-state scoped-range transition v2");
    digest.update(commit_id.as_uuid().as_bytes());
    digest.update(&[u8::from(base_commit_id.is_some())]);
    if let Some(base) = base_commit_id {
        digest.update(base.as_uuid().as_bytes());
    }
    digest.update(&[u8::from(base_root_id.is_some())]);
    if let Some(root_id) = base_root_id {
        digest.update(&root_id);
    }
    digest.update(&mutation_digest);
    digest.update(&tree.root_id);
    digest.update(&tree.root_digest);
    digest.update(&tree.marker_count.to_be_bytes());
    digest.update(&tree.part_count.to_be_bytes());
    digest.update(&tree.row_count.to_be_bytes());
    digest.update(&tree.tree_height.to_be_bytes());
    *digest.finalize().as_bytes()
}

fn full_ownership(row_count: u16) -> Vec<u8> {
    let row_count = usize::from(row_count);
    let mut bits = vec![u8::MAX; row_count.div_ceil(8)];
    if row_count % 8 != 0 {
        *bits.last_mut().expect("nonzero direct part") = (1u8 << (row_count % 8)) - 1;
    }
    bits
}

fn transcode_part<T: musli::Encode<musli::mode::Binary> + ?Sized>(
    part: &T,
) -> Result<MutationPartWire, LixError> {
    let bytes = storage_codec::encode("v68 mutation-directory part", part)?;
    storage_codec::decode("v68 mutation-directory part", &bytes)
}

fn full_value(value: Option<StorageProjectedValue>) -> Option<Bytes> {
    match value {
        Some(StorageProjectedValue::FullValue(bytes)) => Some(bytes),
        Some(StorageProjectedValue::KeyOnly) | None => None,
    }
}

fn authority_error(message: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("v68 tracked_state commit-state authority {message}"),
    )
}

#[cfg(test)]
mod tests {
    use crate::changelog::CommitId;
    use crate::tracked_state::CommitStateReplayDebt;

    use super::{
        HeaderV68, InventoryV68, TouchedScopeFilterWire, assemble_manifest, full_ownership,
        old_transition_digest,
    };

    #[test]
    fn full_ownership_is_canonical_little_endian_bitmap() {
        assert_eq!(full_ownership(1), vec![0b0000_0001]);
        assert_eq!(full_ownership(8), vec![0xff]);
        assert_eq!(full_ownership(10), vec![0xff, 0b0000_0011]);
    }

    #[test]
    fn inline_v68_authority_validates_before_ownership_upgrade() {
        let commit_id = CommitId::for_test_label("v68-manifest-upgrade");
        let catalog = InventoryV68 {
            member_count: 1,
            selection_fingerprint: [7; 32],
            single_partition: None,
            lifecycle_summary: None,
            replacement_generation: None,
            replacement_parts: None,
            columnar_parts: None,
            inline_part: vec![42],
            inline_direct: true,
            directory_root: None,
        };
        let transition_digest = old_transition_digest(None, &catalog, 1, 1, 0, 0);
        let header = HeaderV68 {
            commit_id,
            change_account_id: "account".to_string(),
            replay_debt: CommitStateReplayDebt {
                depth: 1,
                rows: 1,
                bytes: 1,
            },
            selected_source_commit_id: None,
            mutation_inventory_digest: [1; 32],
            mutation_transition_digest: transition_digest,
            mutation_member_count: 1,
            mutation_part_count: 1,
            mutation_directory_root: None,
            touched_scope_filter: TouchedScopeFilterWire {
                complete: false,
                bits: Vec::new(),
            },
            current_state_scoped_ranges: None,
            snapshot_root: None,
        };

        let manifest = assemble_manifest(header.clone(), catalog.clone(), Vec::new()).unwrap();
        assert_eq!(manifest.mutations.direct_part_row_counts, vec![1]);
        assert_eq!(manifest.mutations.direct_part_ownership, vec![vec![1]]);

        let mut tampered = header;
        tampered.mutation_transition_digest[0] ^= 1;
        assert!(assemble_manifest(tampered, catalog, Vec::new()).is_err());
    }
}
