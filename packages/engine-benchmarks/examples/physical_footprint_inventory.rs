//! Read-only physical-footprint inventory for an already populated repository.
//!
//! This is deliberately an example, rather than an engine API.  It opens one
//! coherent read view, scans known storage spaces without staging a write, and
//! emits tab-separated accounting suitable for hashing and later comparison
//! with a preserved large fixture.  The command never seeds, compacts,
//! deletes, or rewrites the supplied path.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use bytes::Bytes;
use lix::storage_adapter::{
    ScanPlan, Storage, StorageAdapter, StorageAdapterRead, StorageCoreProjection, StoragePrefix,
    StorageReadOptions, StorageScanOptions, StorageSpace, StorageSpaceId,
};
use lix::storage_bench::{
    CommitGraphBenchMode, content_authority_accounting_for_bench,
    current_image_cas_oracle_accounting, layout_space_catalog, plan_repository_gc_for_bench,
    read_commit_graph_for_bench, semantic_payload_accounting_for_bench,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Category {
    UserLogicalValue,
    CurrentAuthority,
    RetainedHistoryAuthority,
    GraphManifestDirectoryControl,
    RebuildableMaterialization,
    Unknown,
}

impl Category {
    const ALL: [Self; 6] = [
        Self::UserLogicalValue,
        Self::CurrentAuthority,
        Self::RetainedHistoryAuthority,
        Self::GraphManifestDirectoryControl,
        Self::RebuildableMaterialization,
        Self::Unknown,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::UserLogicalValue => "user_logical_value",
            Self::CurrentAuthority => "current_authority",
            Self::RetainedHistoryAuthority => "retained_history_authority",
            Self::GraphManifestDirectoryControl => "graph_manifest_directory_control",
            Self::RebuildableMaterialization => "rebuildable_materialization",
            Self::Unknown => "unknown_unclassified",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct SpaceAccounting {
    id: u32,
    rows: u64,
    key_bytes: u64,
    value_bytes: u64,
}

impl SpaceAccounting {
    fn logical_bytes(self) -> u64 {
        self.key_bytes.saturating_add(self.value_bytes)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Totals {
    rows: u64,
    key_bytes: u64,
    value_bytes: u64,
}

impl Totals {
    fn add(&mut self, row: SpaceAccounting) {
        self.rows = self.rows.saturating_add(row.rows);
        self.key_bytes = self.key_bytes.saturating_add(row.key_bytes);
        self.value_bytes = self.value_bytes.saturating_add(row.value_bytes);
    }
    fn logical_bytes(self) -> u64 {
        self.key_bytes.saturating_add(self.value_bytes)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct FilesystemTotals {
    files: u64,
    logical_bytes: u64,
    allocated_bytes: u64,
    sst_files: u64,
    sst_bytes: u64,
    wal_bytes: u64,
    metadata_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct DeleteAccounting {
    rows: u64,
    key_bytes: u64,
    value_bytes: u64,
}

#[derive(Debug, Default)]
struct DeleteMapping {
    present: Vec<(u32, DeleteAccounting)>,
}

impl DeleteAccounting {
    fn logical_bytes(self) -> u64 {
        self.key_bytes.saturating_add(self.value_bytes)
    }
    fn add(&mut self, other: Self) {
        self.rows = self.rows.saturating_add(other.rows);
        self.key_bytes = self.key_bytes.saturating_add(other.key_bytes);
        self.value_bytes = self.value_bytes.saturating_add(other.value_bytes);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    let backend = args
        .next()
        .ok_or("usage: physical_footprint_inventory <rocksdb|slatedb> <path>")?;
    let path = PathBuf::from(
        args.next()
            .ok_or("usage: physical_footprint_inventory <rocksdb|slatedb> <path>")?,
    );
    if args.next().is_some() {
        return Err("unexpected argument; output is stdout and is safe to redirect".into());
    }
    let backend = backend.to_string_lossy();
    let mut output = String::new();
    writeln!(output, "INVENTORY_FORMAT\tphysical_footprint_inventory.v2")?;
    writeln!(output, "BACKEND\t{backend}")?;
    writeln!(output, "PATH\t{}", path.display())?;
    writeln!(
        output,
        "READ_ONLY\tlogical_writes=0\tcompaction_requested=0\tdeletions=0\treseed=0\tadapter_open_may_update_backend_metadata=true\tpreserved_source_requires_snapshot_or_reflink_copy=true"
    )?;
    match backend.as_ref() {
        "rocksdb" => run_backend(RocksDB::open(&path)?, &path, &mut output).await?,
        "slatedb" => run_backend(SlateDB::open(&path)?, &path, &mut output).await?,
        other => return Err(format!("unsupported backend '{other}'").into()),
    }
    print!("{output}");
    Ok(())
}

async fn run_backend<S>(backend: S, path: &Path, output: &mut String) -> Result<(), Box<dyn Error>>
where
    S: Storage,
{
    let storage = StorageAdapter::new(backend);
    let read = storage.begin_read(StorageReadOptions::default()).await?;
    let spaces = inventory_spaces();
    let inventory_started = Instant::now();
    let mut rows = Vec::with_capacity(spaces.len());
    for space in spaces {
        rows.push((space, scan_space(&read, space).await?));
    }
    let mut category_totals = BTreeMap::<Category, Totals>::new();
    let mut total = Totals::default();
    for (space, accounting) in &rows {
        let category = classify(space.name);
        let category_total = category_totals.entry(category).or_default();
        category_total.add(*accounting);
        total.add(*accounting);
        writeln!(
            output,
            "SPACE\tid={}\tname={}\tcategory={}\trows={}\tkey_bytes={}\tvalue_bytes={}\tlogical_bytes={}\tunique_digest_count={}",
            accounting.id,
            space.name,
            category.name(),
            accounting.rows,
            accounting.key_bytes,
            accounting.value_bytes,
            accounting.logical_bytes(),
            unique_digest_count(space.name, *accounting),
        )?;
    }
    let inventory_ms = inventory_started.elapsed().as_millis();
    writeln!(
        output,
        "TOTAL\trows={}\tkey_bytes={}\tvalue_bytes={}\tlogical_bytes={}",
        total.rows,
        total.key_bytes,
        total.value_bytes,
        total.logical_bytes(),
    )?;
    let category_sum = category_totals
        .values()
        .fold(Totals::default(), |mut sum, value| {
            sum.rows = sum.rows.saturating_add(value.rows);
            sum.key_bytes = sum.key_bytes.saturating_add(value.key_bytes);
            sum.value_bytes = sum.value_bytes.saturating_add(value.value_bytes);
            sum
        });
    if category_sum.rows != total.rows
        || category_sum.key_bytes != total.key_bytes
        || category_sum.value_bytes != total.value_bytes
    {
        return Err("accounting identity failed: category totals do not reconcile".into());
    }
    for category in Category::ALL {
        let value = category_totals.get(&category).copied().unwrap_or_default();
        writeln!(
            output,
            "CATEGORY\tname={}\trows={}\tkey_bytes={}\tvalue_bytes={}\tlogical_bytes={}",
            category.name(),
            value.rows,
            value.key_bytes,
            value.value_bytes,
            value.logical_bytes(),
        )?;
    }
    writeln!(output, "ACCOUNTING_CHECK\tcategory_totals_equal_total=true")?;

    let semantic = semantic_payload_accounting_for_bench(&read).await?;
    if semantic.live_semantic_rows != semantic.covered_live_rows
        || semantic.covered_live_rows != semantic.decoded_rows
    {
        return Err("semantic payload coverage failed: live tracked rows were not decoded".into());
    }
    writeln!(
        output,
        "SEMANTIC_PAYLOAD\tlive_semantic_rows={}\tdecoded_rows={}\tcanonical_value_bytes={}\tidentity_bytes={}\tschema_bytes={}\tcovered_live_rows={}\tscanned_rows={}\tderived_projection_rows_excluded={}\tcoverage=visible_stored_tracked_entities_untracked_false\torthogonal_to_physical_categories=true",
        semantic.live_semantic_rows,
        semantic.decoded_rows,
        semantic.canonical_value_bytes,
        semantic.identity_bytes,
        semantic.schema_bytes,
        semantic.covered_live_rows,
        semantic.scanned_rows,
        semantic.derived_projection_rows_excluded,
    )?;

    let content = content_authority_accounting_for_bench(&read, &inventory_spaces()).await?;
    let mut content_unique_rows = 0_u64;
    let mut content_unique_bytes = 0_u64;
    for entry in &content {
        content_unique_rows = content_unique_rows.saturating_add(entry.unique_content_digest_rows);
        content_unique_bytes = content_unique_bytes.saturating_add(entry.unique_content_bytes);
        writeln!(
            output,
            "CONTENT_AUTHORITY\tauthority={}\tdigest_codec={}\treference_count={}\treference_bytes={}\tunique_content_digest_rows={}\treference_fanout={}/{}\tunique_content_bytes={}\tduplicated_reference_bytes={}",
            entry.authority,
            entry.digest_codec,
            entry.reference_count,
            entry.reference_bytes,
            entry.unique_content_digest_rows,
            entry.reference_count,
            entry.unique_content_digest_rows,
            entry.unique_content_bytes,
            entry.duplicated_reference_bytes,
        )?;
    }
    if content_unique_rows == 0 || content_unique_bytes == 0 {
        return Err("content authority accounting found no decoded digest-backed content".into());
    }
    writeln!(
        output,
        "CONTENT_SUMMARY\tunique_content_digest_rows={}\tunique_content_bytes={}\tcontent_reference_fanout_is_separate=true",
        content_unique_rows, content_unique_bytes,
    )?;

    // Keep the legacy summary label, but source it from the decoded
    // key/digest accounting above rather than counting only binary-CAS rows.
    let fanout_rows = rows
        .iter()
        .filter(|(space, _)| {
            matches!(
                space.name,
                "live_state.hot_file_schema.v18"
                    | "live_state.hot_diff.v17"
                    | "binary_cas.manifest_chunk"
                    | "binary_cas.chunk_presence"
            )
        })
        .map(|(_, accounting)| accounting.rows)
        .sum::<u64>();
    writeln!(
        output,
        "DIGESTS\tunique_content_digest_rows={}\treference_fanout_rows={}\tcontent_and_fanout_are_separate=true",
        content_unique_rows, fanout_rows
    )?;

    // The production GC planner is read-only here: it drops its write set
    // rather than committing it.  Its live set is the authenticated union of
    // branch/recovery roots and retained semantic history.  The branch-control
    // row count is the exact number of current-head roots; the planner's live
    // commit count is the exact retained-history union count.
    let branch_rows = rows
        .iter()
        .find(|(space, _)| space.name.starts_with("branch.head_control"))
        .map_or(0, |(_, accounting)| accounting.rows);
    let branch_space = inventory_spaces()
        .into_iter()
        .find(|space| space.name.starts_with("branch.head_control"))
        .ok_or("branch control space is absent from inventory catalog")?;
    let branch_values = scan_full_values(&read, branch_space).await?;
    if branch_values.len() != branch_rows as usize {
        return Err("branch control scan disagrees with space row count".into());
    }
    let mut current_head_transitive_commits = 0_u64;
    for value in branch_values {
        let bytes: [u8; 16] = value
            .get(..16)
            .ok_or("branch control value is shorter than a commit id")?
            .try_into()
            .map_err(|_| "branch control head commit id has invalid width")?;
        let commit_id = Uuid::from_bytes(bytes).to_string();
        let graph =
            read_commit_graph_for_bench(&storage, &commit_id, CommitGraphBenchMode::ReachableNodes)
                .await?;
        current_head_transitive_commits = current_head_transitive_commits
            .checked_add(graph.nodes as u64)
            .ok_or("current-head commit count overflow")?;
    }
    let reachability_started = Instant::now();
    let gc = plan_repository_gc_for_bench(&storage).await?;
    let reachability_ms = reachability_started.elapsed().as_millis();
    writeln!(
        output,
        "REACHABILITY\tcurrent_head_root_rows={}\tcurrent_head_transitive_commits={}\tretained_history_union_commits={}\tswept_commits={}\tswept_payloads={}\tgc_planner_writes_staged={}\tgc_planner_is_read_only=true",
        branch_rows,
        current_head_transitive_commits,
        gc.live_commits,
        gc.swept_commits,
        gc.swept_payloads,
        gc.staged_puts + gc.staged_deletes
    )?;
    let delete_mapping = match map_gc_delete_entries(&read, &gc, &rows).await {
        Ok(mapping) => mapping,
        Err(error) => {
            writeln!(
                output,
                "DELETE_MAPPING_FAILURE\tstatus=fail_closed\terror={error}\taccounting_check=false"
            )?;
            print!("{output}");
            return Err(error);
        }
    };
    let mapped_by_space = delete_mapping
        .present
        .iter()
        .copied()
        .collect::<BTreeMap<_, _>>();
    let mut planned_deleted_rows = 0_u64;
    for (space_id, count) in &gc.delete_counts_by_space {
        let Some((space, accounting)) = rows.iter().find(|(_, value)| value.id == *space_id) else {
            return Err(format!("GC planned deletion in unknown space id {space_id:#x}").into());
        };
        let count = *count as u64;
        let mapped = mapped_by_space.get(space_id).copied().unwrap_or_default();
        if mapped.rows != count
            || mapped.rows > accounting.rows
            || mapped.key_bytes > accounting.key_bytes
            || mapped.value_bytes > accounting.value_bytes
        {
            return Err(format!(
                "GC delete mapping does not exactly cover {}: planned={}, mapped={}, inventory_rows={}",
                space.name, count, mapped.rows, accounting.rows
            )
            .into());
        }
        planned_deleted_rows = planned_deleted_rows.saturating_add(count);
        writeln!(
            output,
            "RETENTION\tspace={}\tall_rows={}\tunion_retained_rows={}\tplanned_delete_intents={}\tplanned_unreachable_or_superseded_rows={}\tplanned_key_bytes={}\tplanned_value_bytes={}\tplanned_logical_bytes={}\tmapping=exact_key_to_one_inventory_row",
            space.name,
            accounting.rows,
            accounting.rows.saturating_sub(mapped.rows),
            count,
            mapped.rows,
            mapped.key_bytes,
            mapped.value_bytes,
            mapped.logical_bytes(),
        )?;
    }
    if planned_deleted_rows != gc.staged_deletes {
        return Err("GC planned delete identities do not reconcile with staged deletes".into());
    }
    let mut planned_delete_totals = DeleteAccounting::default();
    let mut retained_history_only = category_totals
        .get(&Category::RetainedHistoryAuthority)
        .copied()
        .unwrap_or_default();
    for (space_id, mapped) in &delete_mapping.present {
        let space = inventory_spaces()
            .into_iter()
            .find(|candidate| candidate.id.0 == *space_id)
            .ok_or_else(|| format!("GC mapped delete space {space_id:#x} is not cataloged"))?;
        planned_delete_totals.add(*mapped);
        writeln!(
            output,
            "DELETE_ACCOUNTING\tspace={}\tclassification=unreachable_or_superseded\trows={}\tkey_bytes={}\tvalue_bytes={}\tlogical_bytes={}\tmapping=exact_key_to_one_inventory_row",
            space.name,
            mapped.rows,
            mapped.key_bytes,
            mapped.value_bytes,
            mapped.logical_bytes(),
        )?;
        if classify(space.name) == Category::RetainedHistoryAuthority {
            retained_history_only.rows = retained_history_only.rows.saturating_sub(mapped.rows);
            retained_history_only.key_bytes = retained_history_only
                .key_bytes
                .saturating_sub(mapped.key_bytes);
            retained_history_only.value_bytes = retained_history_only
                .value_bytes
                .saturating_sub(mapped.value_bytes);
        }
    }
    writeln!(
        output,
        "RETENTION_SUMMARY\tplanned_unreachable_or_superseded_rows={}\tplanned_key_bytes={}\tplanned_value_bytes={}\tplanned_logical_bytes={}\tretained_history_only_rows={}\tretained_history_only_key_bytes={}\tretained_history_only_value_bytes={}\tretained_history_only_logical_bytes={}\thistorical_only_content_is_not_labeled_obsolete=true\tdelete_mapping_exact=true\tmissing_or_duplicate_mapping_fail_closed=true",
        planned_deleted_rows,
        planned_delete_totals.key_bytes,
        planned_delete_totals.value_bytes,
        planned_delete_totals.logical_bytes(),
        retained_history_only.rows,
        retained_history_only.key_bytes,
        retained_history_only.value_bytes,
        retained_history_only.logical_bytes(),
    )?;

    // This oracle is exact for the binary-CAS current-image subset.  It is
    // intentionally reported separately from the full retained-history set;
    // no current-head-only heuristic is allowed to delete historical bytes.
    let cas_started = Instant::now();
    let cas = current_image_cas_oracle_accounting(&read).await?;
    let cas_oracle_ms = cas_started.elapsed().as_millis();
    writeln!(
        output,
        "CURRENT_HEAD_CAS\tcurrent_file_images={}\tcurrent_cas_row_bytes={}\tretained_cas_row_bytes={}\tcurrent_head_only_reclaimable_bytes={}\tlabel=CAS_subset_only",
        cas.current_file_images,
        cas.current_cas_row_bytes,
        cas.retained_cas_row_bytes,
        cas.reclaimable_cas_row_bytes,
    )?;
    let filesystem = filesystem_totals(path);
    writeln!(
        output,
        "FILESYSTEM\tfiles={}\tlogical_file_bytes={}\tallocated_bytes={}\tsst_files={}\tsst_bytes={}\twal_bytes={}\tmetadata_bytes={}\tallocation_distinct_from_logical=true",
        filesystem.files,
        filesystem.logical_bytes,
        filesystem.allocated_bytes,
        filesystem.sst_files,
        filesystem.sst_bytes,
        filesystem.wal_bytes,
        filesystem.metadata_bytes,
    )?;
    let backend_name = if path.join("CURRENT").exists() || path.join("MANIFEST-000000").exists() {
        "rocksdb"
    } else {
        "slatedb"
    };
    if backend_name == "rocksdb" {
        writeln!(
            output,
            "BACKEND_PROPERTIES\tbackend=rocksdb\tsst_by_level=unavailable_without_native_property_handle\tnative_live_data_bytes=unavailable_without_native_property_handle\tlogical_inventory_bytes={}\ttotal_sst_bytes={}\tsst_files={}\twal_bytes={}\tmetadata_bytes={}\tpending_compaction=unavailable_without_native_property_handle",
            total.logical_bytes(),
            filesystem.sst_bytes,
            filesystem.sst_files,
            filesystem.wal_bytes,
            filesystem.metadata_bytes,
        )?;
    } else {
        writeln!(
            output,
            "BACKEND_PROPERTIES\tbackend=slatedb\tobject_store_logical_bytes={}\tobject_store_allocated_bytes={}\tcompaction_pending=unavailable\twal_bytes={}\tmetadata_bytes={}",
            filesystem.logical_bytes,
            filesystem.allocated_bytes,
            filesystem.wal_bytes,
            filesystem.metadata_bytes,
        )?;
    }
    writeln!(
        output,
        "NORMALIZATION\trows={}\tsemantic_commits={}\tretained_history_entries={}\tlive_semantic_rows={}\tcanonical_value_bytes={}\tidentity_bytes={}\tschema_bytes={}\tsemantic_commits_source=gc_live_set\tretained_history_entries_source=gc_live_set\tsemantic_payload_source=decoded_tracked_live_entities\tsemantic_payload_is_orthogonal=true",
        total.rows,
        gc.live_commits,
        gc.live_commits,
        semantic.live_semantic_rows,
        semantic.canonical_value_bytes,
        semantic.identity_bytes,
        semantic.schema_bytes,
    )?;
    let semantic_commit_count = u64::try_from(gc.live_commits)?;
    writeln!(
        output,
        "NORMALIZED\tlogical_bytes_per_physical_row={}\tlogical_bytes_per_semantic_commit={}\tlogical_bytes_per_retained_history_entry={}\tlogical_bytes_per_live_semantic_row={}\tlogical_bytes_per_canonical_value_byte={}\tfilesystem_logical_bytes_per_live_semantic_row={}\tfilesystem_logical_bytes_per_semantic_commit={}\tfilesystem_allocated_bytes_per_semantic_commit={}\tfilesystem_logical_bytes_per_canonical_value_byte={}",
        ratio(total.logical_bytes(), total.rows),
        ratio(total.logical_bytes(), semantic_commit_count),
        ratio(total.logical_bytes(), semantic_commit_count),
        ratio(total.logical_bytes(), semantic.live_semantic_rows),
        ratio(total.logical_bytes(), semantic.canonical_value_bytes),
        ratio(filesystem.logical_bytes, semantic.live_semantic_rows),
        ratio(filesystem.logical_bytes, semantic_commit_count),
        ratio(filesystem.allocated_bytes, semantic_commit_count),
        ratio(filesystem.logical_bytes, semantic.canonical_value_bytes),
    )?;
    writeln!(
        output,
        "TIMING\tread_inventory_ms={}\treachability_ms={}\tcas_oracle_ms={}\tproduction_cut_claimed=false",
        inventory_ms, reachability_ms, cas_oracle_ms
    )?;
    Ok(())
}

fn ratio(numerator: u64, denominator: u64) -> String {
    if denominator == 0 {
        "unavailable".to_owned()
    } else {
        format!("{:.6}", numerator as f64 / denominator as f64)
    }
}

async fn scan_full_values<R>(read: &R, space: StorageSpace) -> Result<Vec<Vec<u8>>, Box<dyn Error>>
where
    R: StorageAdapterRead,
{
    let plan = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut values = Vec::new();
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                read,
                StorageScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        let last = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            let lix::storage_adapter::StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(format!("space {} returned a key-only value", space.name).into());
            };
            values.push(value.to_vec());
        }
        if !page.value.has_more {
            return Ok(values);
        }
        let Some(last) = last else {
            return Err(
                format!("space {} returned has_more with an empty page", space.name).into(),
            );
        };
        if resume_after
            .as_ref()
            .is_some_and(|previous| last <= *previous)
        {
            return Err(format!("space {} returned a non-advancing cursor", space.name).into());
        }
        resume_after = Some(last);
    }
}

async fn scan_entries<R>(
    read: &R,
    space: StorageSpace,
) -> Result<Vec<lix::storage_adapter::StorageReadEntry>, Box<dyn Error>>
where
    R: StorageAdapterRead,
{
    let plan = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut entries = Vec::new();
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                read,
                StorageScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        let last = page.value.entries.last().map(|entry| entry.key.clone());
        entries.extend(page.value.entries);
        if !page.value.has_more {
            return Ok(entries);
        }
        let Some(last) = last else {
            return Err(
                format!("space {} returned has_more with an empty page", space.name).into(),
            );
        };
        if resume_after
            .as_ref()
            .is_some_and(|previous| last <= *previous)
        {
            return Err(format!("space {} returned a non-advancing cursor", space.name).into());
        }
        resume_after = Some(last);
    }
}

async fn map_gc_delete_entries<R>(
    read: &R,
    gc: &lix::storage_bench::RepositoryGcBenchResult,
    rows: &[(StorageSpace, SpaceAccounting)],
) -> Result<DeleteMapping, Box<dyn Error>>
where
    R: StorageAdapterRead,
{
    let mut seen_spaces = BTreeSet::new();
    let mut mapped = DeleteMapping {
        present: Vec::with_capacity(gc.delete_entries_by_space.len()),
    };
    for (space_id, keys) in &gc.delete_entries_by_space {
        if !seen_spaces.insert(*space_id) {
            return Err(format!("GC delete mapping repeats storage space {space_id:#x}").into());
        }
        let (space, inventory) = rows
            .iter()
            .find(|(_, accounting)| accounting.id == *space_id)
            .ok_or_else(|| format!("GC planned deletion in unknown space id {space_id:#x}"))?;
        let expected = keys.iter().cloned().collect::<BTreeSet<_>>();
        if expected.len() != keys.len() {
            return Err(format!("GC planned duplicate delete key in {}", space.name).into());
        }
        let mut found = BTreeSet::new();
        let mut accounting = DeleteAccounting::default();
        for entry in scan_entries(read, *space).await? {
            let key = entry.key.0.to_vec();
            if !expected.contains(&key) {
                continue;
            }
            if !found.insert(key.clone()) {
                return Err(format!("GC delete key appears twice in {}", space.name).into());
            }
            let value = match entry.value {
                lix::storage_adapter::StorageProjectedValue::FullValue(value) => value,
                lix::storage_adapter::StorageProjectedValue::KeyOnly => {
                    return Err(
                        format!("GC delete mapping saw key-only value in {}", space.name).into(),
                    );
                }
            };
            accounting.rows = accounting.rows.saturating_add(1);
            accounting.key_bytes = accounting
                .key_bytes
                .saturating_add(4_u64.saturating_add(key.len() as u64));
            accounting.value_bytes = accounting.value_bytes.saturating_add(value.len() as u64);
        }
        if accounting.rows != found.len() as u64 || found.len() != expected.len() {
            return Err(format!(
                "GC delete mapping missing {} rows in {} (expected={}, found={}, inventory had {})",
                expected.len().saturating_sub(found.len()),
                space.name,
                expected.len(),
                found.len(),
                inventory.rows,
            )
            .into());
        }
        mapped.present.push((*space_id, accounting));
    }
    Ok(mapped)
}

fn inventory_spaces() -> Vec<StorageSpace> {
    let mut spaces = layout_space_catalog()
        .into_iter()
        .map(|(id, name)| catalog_space(id, name))
        .collect::<Vec<_>>();
    // The benchmark catalog predates a few current immutable serving spaces
    // and maintenance namespaces. Keep them here so the inventory itself
    // exposes rows the older accounting helper would omit.
    spaces.extend([
        StorageSpace::immutable(
            StorageSpaceId(0x0004_002d),
            "tracked_state.commit_mutation_directory_node.v1",
        ),
        StorageSpace::immutable(
            StorageSpaceId(0x0004_002f),
            "tracked_state.current_state_data_part.v1",
        ),
        StorageSpace::immutable(
            StorageSpaceId(0x0004_0030),
            "tracked_state.current_state_data_part_refs.v1",
        ),
        StorageSpace::immutable(StorageSpaceId(0x0004_0032), "tracked_state.scoped_range.v3"),
        StorageSpace::immutable(
            StorageSpaceId(0x0004_0029),
            "entity.columnar_row_group_manifest.v1",
        ),
        StorageSpace::immutable(
            StorageSpaceId(0x0004_002a),
            "entity.columnar_row_group_column.v1",
        ),
        StorageSpace::mutable(StorageSpaceId(0x0007_0001), "observe.mutation_revision"),
        StorageSpace::mutable(
            StorageSpaceId(0x0007_0002),
            "filesystem.path_index_revision",
        ),
        StorageSpace::mutable(StorageSpaceId(0x0007_0003), "catalog.schema_revision"),
        StorageSpace::mutable(StorageSpaceId(0x0007_0004), "transaction.tracked_revision"),
        StorageSpace::mutable(
            StorageSpaceId(0x0007_0005),
            "session.execute_idempotency_receipt.v1",
        ),
        StorageSpace::mutable(StorageSpaceId(0x0007_0006), "session.file_upload.v2"),
        StorageSpace::mutable(
            StorageSpaceId(0x0007_0007),
            "session.file_upload_manifest_leaf.v2",
        ),
        StorageSpace::mutable(StorageSpaceId(0x0008_0001), "checkpoint.recovery_ref.v3"),
        StorageSpace::mutable(StorageSpaceId(0x0008_0002), "checkpoint.gc_state.v1"),
        StorageSpace::mutable(StorageSpaceId(0x0008_0003), "gc.reachability_delta.v1"),
        StorageSpace::mutable(StorageSpaceId(0x0008_0004), "gc.reachability_queue.v1"),
        StorageSpace::mutable(StorageSpaceId(0x0001_0002), "untracked_state.row.v1"),
        StorageSpace::mutable(
            StorageSpaceId(0x0004_0005),
            "live_state.index.branch_root.v1",
        ),
    ]);
    spaces.sort_unstable_by_key(|space| space.id.0);
    spaces.dedup_by_key(|space| space.id.0);
    spaces
}

fn catalog_space(id: u32, name: &'static str) -> StorageSpace {
    if matches!(
        name,
        "tracked_state.commit_delta_segment.v6"
            | "tracked_state.commit_state_manifest.v7"
            | "tracked_state.commit_mutation_catalog.v1"
            | "tracked_state.commit_mutation_directory_node.v1"
            | "binary_cas.chunk"
            | "tracked_state.current_state_data_part.v1"
            | "tracked_state.current_state_data_part_refs.v1"
            | "tracked_state.scoped_range.v3"
            | "entity.columnar_row_group_manifest.v1"
            | "entity.columnar_row_group_column.v1"
    ) {
        StorageSpace::immutable(StorageSpaceId(id), name)
    } else {
        StorageSpace::mutable(StorageSpaceId(id), name)
    }
}

fn classify(name: &str) -> Category {
    if matches!(
        name,
        "json_store.json" | "binary_cas.chunk" | "binary_cas.chunk_presence"
    ) {
        return Category::UserLogicalValue;
    }
    if name.starts_with("live_state.hot_row")
        || name.starts_with("live_state.packed_current")
        || name.starts_with("live_state.root_current")
        || name.starts_with("tracked_state.scoped_range")
    {
        return Category::CurrentAuthority;
    }
    if name.starts_with("tracked_state.commit_")
        || name.starts_with("tracked_state.tree_chunk")
        || name.starts_with("tracked_state.change_locator")
        || name.starts_with("changelog.")
        || name.starts_with("binary_cas.manifest")
    {
        return Category::RetainedHistoryAuthority;
    }
    if name.starts_with("repository.")
        || name.starts_with("branch.")
        || name.starts_with("checkpoint.")
        || name.starts_with("gc.")
        || name.starts_with("session.execute_idempotency")
        || name.ends_with("revision")
    {
        return Category::GraphManifestDirectoryControl;
    }
    if name.starts_with("plugin.")
        || name.starts_with("entity.columnar")
        || name.starts_with("tracked_state.current_state_data_part")
        || name.starts_with("live_state.hot_file")
        || name.starts_with("live_state.hot_diff")
        || name.starts_with("live_state.certified_entity_batch")
        || name.starts_with("json_store.untracked_reclaim")
        || name.starts_with("session.file_upload")
    {
        return Category::RebuildableMaterialization;
    }
    Category::Unknown
}

fn unique_digest_count(name: &str, accounting: SpaceAccounting) -> u64 {
    if name.starts_with("binary_cas.")
        || matches!(
            name,
            "tracked_state.tree_chunk"
                | "tracked_state.commit_mutation_directory_node.v1"
                | "tracked_state.current_state_data_part.v1"
                | "tracked_state.current_state_data_part_refs.v1"
        )
    {
        accounting.rows
    } else {
        0
    }
}

async fn scan_space<R>(read: &R, space: StorageSpace) -> Result<SpaceAccounting, Box<dyn Error>>
where
    R: StorageAdapterRead,
{
    let plan = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut result = SpaceAccounting {
        id: space.id.0,
        ..Default::default()
    };
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                read,
                StorageScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        let has_more = page.value.has_more;
        let last = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in &page.value.entries {
            result.rows = result.rows.checked_add(1).ok_or("row count overflow")?;
            result.key_bytes = result
                .key_bytes
                .checked_add(entry.key.0.len() as u64 + 4)
                .ok_or("key byte count overflow")?;
            let lix::storage_adapter::StorageProjectedValue::FullValue(value) = &entry.value else {
                return Err(format!(
                    "space {} returned a key-only row for a full-value inventory",
                    space.name
                )
                .into());
            };
            result.value_bytes = result
                .value_bytes
                .checked_add(value.len() as u64)
                .ok_or("value byte count overflow")?;
        }
        if !has_more {
            break;
        }
        let Some(last) = last else {
            return Err(
                format!("space {} reported has_more with an empty page", space.name).into(),
            );
        };
        if resume_after
            .as_ref()
            .is_some_and(|previous| last <= *previous)
        {
            return Err(format!("space {} returned a non-advancing cursor", space.name).into());
        }
        resume_after = Some(last);
    }
    Ok(result)
}

fn filesystem_totals(path: &Path) -> FilesystemTotals {
    fn visit(path: &Path, total: &mut FilesystemTotals) {
        let Ok(metadata) = fs::symlink_metadata(path) else {
            return;
        };
        if metadata.is_dir() {
            let Ok(entries) = fs::read_dir(path) else {
                return;
            };
            for entry in entries.flatten() {
                visit(&entry.path(), total);
            }
            return;
        }
        if !metadata.is_file() {
            return;
        }
        total.files = total.files.saturating_add(1);
        total.logical_bytes = total.logical_bytes.saturating_add(metadata.len());
        #[cfg(unix)]
        {
            total.allocated_bytes = total.allocated_bytes.saturating_add(
                std::os::unix::fs::MetadataExt::blocks(&metadata).saturating_mul(512),
            );
        }
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        if name.ends_with(".sst") {
            total.sst_files = total.sst_files.saturating_add(1);
            total.sst_bytes = total.sst_bytes.saturating_add(metadata.len());
        }
        if name.ends_with(".log") || name == "LOG" {
            total.wal_bytes = total.wal_bytes.saturating_add(metadata.len());
        }
        if name == "CURRENT"
            || name.starts_with("MANIFEST")
            || name.starts_with("OPTIONS")
            || name == "IDENTITY"
        {
            total.metadata_bytes = total.metadata_bytes.saturating_add(metadata.len());
        }
    }
    let mut total = FilesystemTotals::default();
    visit(path, &mut total);
    total
}
