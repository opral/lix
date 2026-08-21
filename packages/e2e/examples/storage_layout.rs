use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use lix::registered_spaces::HOT_ROW_SPACE;
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::{
    binary_cas_owner_layout_accounting, binary_manifest_layout_accounting,
    commit_delta_layout_accounting, layout_accounting, layout_space_catalog, space_inventory,
};
use lix_storage_slatedb::SlateDB;
use object_store::local::LocalFileSystem;
use slatedb::{SstReader, ValueDeletable};

const IMMUTABLE_BINARY_CAS_CHUNK_DIR: &str = "db/lix-immutable-binary-cas-segment-v4";

#[derive(Default)]
struct HotGroup {
    schema: String,
    file_id: Option<String>,
    prefix_len: usize,
    rows: Vec<HotRow>,
}

struct HotRow {
    key_len: usize,
    suffix_len: usize,
    value_len: usize,
}

#[derive(Default)]
struct PageEstimate {
    pages: usize,
    key_bytes: usize,
    value_bytes: usize,
}

#[tokio::main]
async fn main() {
    let path = std::env::args_os()
        .nth(1)
        .expect("usage: storage_layout <slatedb-path>");
    let mode = std::env::args_os().nth(2);
    let logical_only = mode.as_deref().is_some_and(|arg| arg == "--logical-only");
    let physical_only = mode.as_deref().is_some_and(|arg| arg == "--physical-only");
    if physical_only {
        let space_names = layout_space_catalog().into_iter().collect();
        let sst_bytes = inspect_ssts_fast(Path::new(&path), &space_names).await;
        let immutable_bytes = inspect_immutable_chunks(Path::new(&path));
        println!(
            "PHYSICAL_TOTAL_FAST\tbytes={}",
            sst_bytes.saturating_add(immutable_bytes)
        );
        return;
    }
    let storage = StorageAdapter::new(SlateDB::open(&path).expect("open SlateDB"));
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open storage snapshot");
    if mode
        .as_deref()
        .is_some_and(|arg| arg == "--binary-manifest-only")
    {
        print_binary_manifest_layout(&read).await;
        return;
    }
    if mode
        .as_deref()
        .is_some_and(|arg| arg == "--commit-delta-only")
    {
        print_commit_delta_inventory(&read).await;
        return;
    }
    let mut total_rows = 0_u64;
    let mut total_keys = 0_u64;
    let mut total_values = 0_u64;
    let accounting = layout_accounting(&read).await;
    let space_names = accounting
        .iter()
        .map(|entry| (entry.space_id, entry.space))
        .collect::<BTreeMap<_, _>>();
    for entry in accounting {
        println!(
            "SPACE\t{}\t{}\t{}\t{}\t{}",
            entry.space_id, entry.space, entry.rows, entry.key_bytes, entry.value_bytes
        );
        total_rows += entry.rows;
        total_keys += entry.key_bytes;
        total_values += entry.value_bytes;
    }
    println!("SPACE\tTOTAL\tTOTAL\t{total_rows}\t{total_keys}\t{total_values}");
    print_commit_delta_inventory(&read).await;
    print_binary_manifest_layout(&read).await;
    print_binary_cas_owners(&read).await;

    let inventory = space_inventory(&read, HOT_ROW_SPACE.name).await;
    let mut groups = BTreeMap::<Vec<u8>, HotGroup>::new();
    for (key, value) in inventory {
        let decoded = decode_hot_group(&key).expect("valid HOT V19 key");
        let group = groups
            .entry(key[..decoded.prefix_len].to_vec())
            .or_insert_with(|| HotGroup {
                schema: decoded.schema,
                file_id: decoded.file_id,
                prefix_len: decoded.prefix_len,
                rows: Vec::new(),
            });
        group.rows.push(HotRow {
            key_len: key.len(),
            suffix_len: key.len() - decoded.prefix_len,
            value_len: value.len(),
        });
    }

    let mut cardinalities = groups
        .values()
        .map(|group| group.rows.len())
        .collect::<Vec<_>>();
    cardinalities.sort_unstable();
    let file_groups = groups
        .values()
        .filter(|group| group.file_id.is_some())
        .count();
    let rows = groups.values().map(|group| group.rows.len()).sum::<usize>();
    let key_bytes = groups
        .values()
        .flat_map(|group| &group.rows)
        .map(|row| row.key_len)
        .sum::<usize>();
    let suffix_bytes = groups
        .values()
        .flat_map(|group| &group.rows)
        .map(|row| row.suffix_len)
        .sum::<usize>();
    let value_bytes = groups
        .values()
        .flat_map(|group| &group.rows)
        .map(|row| row.value_len)
        .sum::<usize>();
    println!(
        "HOT_SUMMARY\trows={rows}\tgroups={}\tfile_groups={file_groups}\tkey_bytes={key_bytes}\tgroup_prefix_bytes={}\tsuffix_bytes={suffix_bytes}\tvalue_bytes={value_bytes}\tp50={}\tp90={}\tp95={}\tp99={}\tmax={}",
        groups.len(),
        key_bytes - suffix_bytes,
        percentile(&cardinalities, 50),
        percentile(&cardinalities, 90),
        percentile(&cardinalities, 95),
        percentile(&cardinalities, 99),
        cardinalities.last().copied().unwrap_or(0),
    );
    for page_rows in [64, 128, 256, 512] {
        let estimate = estimate_pages(groups.values(), page_rows);
        let packed_total = estimate.key_bytes + estimate.value_bytes;
        let current_total = key_bytes + value_bytes;
        println!(
            "HOT_PAGE_ESTIMATE\trows_per_page={page_rows}\tpages={}\tkey_bytes={}\tvalue_bytes={}\ttotal_bytes={packed_total}\tsaved_bytes={}\tsaved_pct={:.2}",
            estimate.pages,
            estimate.key_bytes,
            estimate.value_bytes,
            current_total.saturating_sub(packed_total),
            100.0 * (current_total.saturating_sub(packed_total) as f64) / current_total as f64,
        );
    }

    let mut largest = groups.values().collect::<Vec<_>>();
    largest.sort_unstable_by(|left, right| right.rows.len().cmp(&left.rows.len()));
    for group in largest.into_iter().take(30) {
        let group_key_bytes = group.rows.iter().map(|row| row.key_len).sum::<usize>();
        let group_value_bytes = group.rows.iter().map(|row| row.value_len).sum::<usize>();
        println!(
            "HOT_GROUP\trows={}\tprefix_len={}\tkey_bytes={group_key_bytes}\tvalue_bytes={group_value_bytes}\tschema={}\tfile={}",
            group.rows.len(),
            group.prefix_len,
            group.schema,
            group.file_id.as_deref().unwrap_or("<null>"),
        );
    }

    if !logical_only {
        inspect_ssts(Path::new(&path), &space_names).await;
    }
}

async fn print_binary_cas_owners(read: &impl lix::storage_adapter::StorageAdapterRead) {
    for owner in binary_cas_owner_layout_accounting(read)
        .await
        .expect("decode binary CAS owners")
    {
        println!(
            "BINARY_OWNER\towner={}\treferences={}\tmanifests={}\tlogical_bytes={}\tencoded_manifest_bytes={}\tempty={}\tsingle_chunk={}\tchunked={}\tdelta={}\tchunk_values={}\tencoded_chunk_bytes={}",
            owner.owner,
            owner.references,
            owner.manifests,
            owner.logical_bytes,
            owner.encoded_manifest_bytes,
            owner.empty_manifests,
            owner.single_chunk_manifests,
            owner.chunked_manifests,
            owner.delta_manifests,
            owner.chunk_values,
            owner.encoded_chunk_bytes,
        );
    }
}

async fn print_binary_manifest_layout(read: &impl lix::storage_adapter::StorageAdapterRead) {
    let layout = binary_manifest_layout_accounting(read)
        .await
        .expect("decode binary manifest layout");
    println!(
        "BINARY_MANIFEST\tmanifests={}\tencoded_bytes={}\tempty={}\tsingle_chunk={}\tchunked={}\tdelta={}",
        layout.manifests,
        layout.encoded_bytes,
        layout.empty_manifests,
        layout.single_chunk_manifests,
        layout.chunked_manifests,
        layout.delta_manifests,
    );
}

async fn print_commit_delta_inventory(read: &impl lix::storage_adapter::StorageAdapterRead) {
    for entry in commit_delta_layout_accounting(read)
        .await
        .expect("decode commit-delta inventory")
    {
        println!(
            "COMMIT_DELTA\tcommit={}\tphysical_key_bytes={}\tphysical_value_bytes={}\tsegments={}\tmembers={}\tauthored={}\tselected={}\tselected_tombstones={}\tselected_direct_addresses={}\tselected_source_commits={}\tdominant_selected_source_members={}",
            entry.commit_id,
            entry.physical_key_bytes,
            entry.physical_value_bytes,
            entry.segment_count,
            entry.members,
            entry.authored_members,
            entry.selected_members,
            entry.selected_tombstones,
            entry.selected_direct_addresses,
            entry.selected_source_commits,
            entry.dominant_selected_source_members,
        );
    }
}

#[derive(Default)]
struct PhysicalSpace {
    blocks: usize,
    compressed_bytes: u64,
    raw_key_bytes: u64,
    raw_value_bytes: u64,
    rows: u64,
    puts: u64,
    tombstones: u64,
    merges: u64,
    unique_keys: BTreeSet<Vec<u8>>,
    min_seq: u64,
    max_seq: u64,
    seq_rows: BTreeMap<u64, (u64, u64, u64)>,
    hot_versions: BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>)>>,
}

async fn inspect_ssts_fast(path: &Path, space_names: &BTreeMap<u32, &'static str>) -> u64 {
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(path.join("db")).expect("local object store"));
    let reader = SstReader::new("lix-space-segments-v2", object_store, None, None);
    let compacted = path.join("db/lix-space-segments-v2/compacted");
    let mut files = std::fs::read_dir(&compacted)
        .expect("read compacted directory")
        .map(|entry| entry.expect("read compacted entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "sst"))
        .collect::<Vec<_>>();
    files.sort();
    let mut spaces = BTreeMap::<u32, (usize, u64)>::new();
    let mut total_file_bytes = 0_u64;
    let mut total_block_bytes = 0_u64;
    for path in files {
        let id = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .expect("UTF-8 SST stem")
            .parse::<ulid::Ulid>()
            .expect("ULID SST stem");
        let file = reader.open(id).await.expect("open SST");
        total_file_bytes += path.metadata().expect("SST metadata").len();
        let index = file.index().await.expect("read SST index");
        let data_end = file.info().filter_offset;
        for (block_index, (offset, first_key)) in index.iter().enumerate() {
            let end = index
                .get(block_index + 1)
                .map_or(data_end, |(next_offset, _)| *next_offset);
            let compressed_bytes = end.saturating_sub(*offset);
            total_block_bytes += compressed_bytes;
            let space_id = if first_key.len() >= 4 {
                physical_space_id(first_key)
            } else {
                let rows = file.read_block(block_index).await.expect("read SST block");
                physical_space_id(&rows.first().expect("indexed SST block is not empty").key)
            };
            let entry = spaces.entry(space_id).or_default();
            entry.0 += 1;
            entry.1 += compressed_bytes;
        }
    }
    println!(
        "SST_SUMMARY_FAST\tfile_bytes={total_file_bytes}\tdata_block_bytes={total_block_bytes}\toverhead_bytes={}\tspace_attribution=first_key",
        total_file_bytes.saturating_sub(total_block_bytes),
    );
    let mut spaces = spaces.into_iter().collect::<Vec<_>>();
    spaces.sort_unstable_by(|left, right| right.1.1.cmp(&left.1.1));
    for (space_id, (blocks, compressed_bytes)) in spaces {
        println!(
            "SST_SPACE_FAST\tspace_id={space_id}\tspace={}\tblocks={blocks}\tcompressed_bytes={compressed_bytes}",
            space_names.get(&space_id).copied().unwrap_or("<unknown>"),
        );
    }
    total_file_bytes
}

fn inspect_immutable_chunks(path: &Path) -> u64 {
    let directory = path.join(IMMUTABLE_BINARY_CAS_CHUNK_DIR);
    let Ok(entries) = std::fs::read_dir(directory) else {
        println!("IMMUTABLE_CHUNKS_FAST\tobjects=0\tbytes=0");
        return 0;
    };
    let mut objects = 0_u64;
    let mut bytes = 0_u64;
    for entry in entries {
        let entry = entry.expect("read immutable chunk entry");
        let metadata = entry.metadata().expect("read immutable chunk metadata");
        if metadata.is_file() {
            objects += 1;
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    println!("IMMUTABLE_CHUNKS_FAST\tobjects={objects}\tbytes={bytes}");
    bytes
}

fn physical_space_id(key: &[u8]) -> u32 {
    u32::from_be_bytes(
        key.get(..4)
            .expect("physical key has space prefix")
            .try_into()
            .expect("space prefix has fixed length"),
    )
}

async fn inspect_ssts(path: &Path, space_names: &BTreeMap<u32, &'static str>) {
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(path.join("db")).expect("local object store"));
    let reader = SstReader::new("lix-space-segments-v2", object_store, None, None);
    let compacted = path.join("db/lix-space-segments-v2/compacted");
    let mut files = std::fs::read_dir(&compacted)
        .expect("read compacted directory")
        .map(|entry| entry.expect("read compacted entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "sst"))
        .collect::<Vec<_>>();
    files.sort();
    let mut spaces = BTreeMap::<u32, PhysicalSpace>::new();
    let mut total_file_bytes = 0_u64;
    let mut total_block_bytes = 0_u64;
    let mut total_overhead_bytes = 0_u64;
    let mut cross_space_blocks = 0_usize;
    for path in files {
        let id = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .expect("UTF-8 SST stem")
            .parse::<ulid::Ulid>()
            .expect("ULID SST stem");
        let file = reader.open(id).await.expect("open SST");
        let file_bytes = path.metadata().expect("SST metadata").len();
        total_file_bytes += file_bytes;
        let index = file.index().await.expect("read SST index");
        let data_end = file.info().filter_offset;
        let mut sst_block_bytes = 0_u64;
        for (block_index, (offset, _)) in index.iter().enumerate() {
            let end = index
                .get(block_index + 1)
                .map_or(data_end, |(next_offset, _)| *next_offset);
            let compressed_bytes = end.saturating_sub(*offset);
            let rows = file.read_block(block_index).await.expect("read SST block");
            let mut raw_by_space =
                BTreeMap::<u32, (u64, u64, u64, u64, u64, u64, BTreeSet<Vec<u8>>, u64, u64)>::new();
            for row in rows {
                let space_id = u32::from_be_bytes(
                    row.key
                        .get(..4)
                        .expect("physical key has space prefix")
                        .try_into()
                        .expect("space prefix has fixed length"),
                );
                let raw_key = row.key.len() as u64;
                let (raw_value, puts, tombstones, merges) = match row.value {
                    ValueDeletable::Value(value) => (value.len() as u64, 1, 0, 0),
                    ValueDeletable::Merge(value) => (value.len() as u64, 0, 0, 1),
                    ValueDeletable::Tombstone => (0, 0, 1, 0),
                };
                let entry = raw_by_space.entry(space_id).or_default();
                entry.0 += raw_key;
                entry.1 += raw_value;
                entry.2 += 1;
                entry.3 += puts;
                entry.4 += tombstones;
                entry.5 += merges;
                entry.6.insert(row.key.to_vec());
                if entry.7 == 0 || row.seq < entry.7 {
                    entry.7 = row.seq;
                }
                entry.8 = entry.8.max(row.seq);
            }
            if raw_by_space.len() > 1 {
                cross_space_blocks += 1;
            }
            let raw_total = raw_by_space
                .values()
                .map(|(keys, values, ..)| keys + values)
                .sum::<u64>();
            let mut remaining = compressed_bytes;
            let space_count = raw_by_space.len();
            for (
                index,
                (
                    space_id,
                    (keys, values, rows, puts, tombstones, merges, unique_keys, min_seq, max_seq),
                ),
            ) in raw_by_space.into_iter().enumerate()
            {
                let share = if index + 1 == space_count {
                    remaining
                } else if raw_total == 0 {
                    compressed_bytes / space_count as u64
                } else {
                    compressed_bytes.saturating_mul(keys + values) / raw_total
                };
                remaining = remaining.saturating_sub(share);
                let entry = spaces.entry(space_id).or_default();
                entry.blocks += 1;
                entry.compressed_bytes += share;
                entry.raw_key_bytes += keys;
                entry.raw_value_bytes += values;
                entry.rows += rows;
                entry.puts += puts;
                entry.tombstones += tombstones;
                entry.merges += merges;
                entry.unique_keys.extend(unique_keys);
                if entry.min_seq == 0 || min_seq < entry.min_seq {
                    entry.min_seq = min_seq;
                }
                entry.max_seq = entry.max_seq.max(max_seq);
                for row in file
                    .read_block(block_index)
                    .await
                    .expect("reread SST block for sequence accounting")
                {
                    let row_space_id = u32::from_be_bytes(
                        row.key
                            .get(..4)
                            .expect("physical key has space prefix")
                            .try_into()
                            .expect("space prefix has fixed length"),
                    );
                    if row_space_id != space_id {
                        continue;
                    }
                    let value_len = row.value.len() as u64;
                    let seq = entry.seq_rows.entry(row.seq).or_default();
                    seq.0 += 1;
                    seq.1 += row.key.len() as u64;
                    seq.2 += value_len;
                    if space_id == 0x0004_001b {
                        entry
                            .hot_versions
                            .entry(row.key.to_vec())
                            .or_default()
                            .push((
                                row.seq,
                                row.value
                                    .as_bytes()
                                    .map_or_else(Vec::new, |value| value.to_vec()),
                            ));
                    }
                }
            }
            sst_block_bytes += compressed_bytes;
        }
        total_block_bytes += sst_block_bytes;
        total_overhead_bytes += file_bytes.saturating_sub(sst_block_bytes);
    }
    let mut spaces = spaces.into_iter().collect::<Vec<_>>();
    spaces.sort_unstable_by(|left, right| right.1.compressed_bytes.cmp(&left.1.compressed_bytes));
    println!(
        "SST_SUMMARY\tfile_bytes={total_file_bytes}\tdata_block_bytes={total_block_bytes}\toverhead_bytes={total_overhead_bytes}\tcross_space_blocks={cross_space_blocks}"
    );
    for (space_id, entry) in spaces {
        println!(
            "SST_SPACE\tspace_id={space_id}\tspace={}\tblocks={}\tcompressed_bytes={}\traw_key_bytes={}\traw_value_bytes={}\trows={}\tputs={}\ttombstones={}\tmerges={}\tunique_keys={}\tversions_per_key={:.3}\tmin_seq={}\tmax_seq={}\tcompression_ratio={:.3}",
            space_names.get(&space_id).copied().unwrap_or("<unknown>"),
            entry.blocks,
            entry.compressed_bytes,
            entry.raw_key_bytes,
            entry.raw_value_bytes,
            entry.rows,
            entry.puts,
            entry.tombstones,
            entry.merges,
            entry.unique_keys.len(),
            entry.rows as f64 / entry.unique_keys.len() as f64,
            entry.min_seq,
            entry.max_seq,
            (entry.raw_key_bytes + entry.raw_value_bytes) as f64 / entry.compressed_bytes as f64,
        );
        if matches!(
            space_id,
            0x0004_001a | 0x0004_001b | 0x0004_0022 | 0x0004_0023
        ) {
            for (seq, (rows, keys, values)) in &entry.seq_rows {
                println!(
                    "SST_SPACE_SEQ\tspace_id={space_id}\tseq={seq}\trows={rows}\traw_key_bytes={keys}\traw_value_bytes={values}"
                );
            }
        }
        if space_id == 0x0004_001b {
            compare_hot_sequences(&entry.hot_versions, 7, 9);
        }
    }
}

fn compare_hot_sequences(
    versions: &BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>)>>,
    left_seq: u64,
    right_seq: u64,
) {
    let mut both = 0_u64;
    let mut equal = 0_u64;
    let mut same_len = 0_u64;
    let mut left_bytes = 0_u64;
    let mut right_bytes = 0_u64;
    let mut first_difference = None;
    for values in versions.values() {
        let left = values
            .iter()
            .find_map(|(seq, value)| (*seq == left_seq).then_some(value));
        let right = values
            .iter()
            .find_map(|(seq, value)| (*seq == right_seq).then_some(value));
        let (Some(left), Some(right)) = (left, right) else {
            continue;
        };
        both += 1;
        equal += u64::from(left == right);
        same_len += u64::from(left.len() == right.len());
        left_bytes += left.len() as u64;
        right_bytes += right.len() as u64;
        if first_difference.is_none() && left != right {
            first_difference = Some((left.clone(), right.clone()));
        }
    }
    println!(
        "HOT_SEQ_COMPARE\tleft_seq={left_seq}\tright_seq={right_seq}\tboth={both}\tequal={equal}\tsame_len={same_len}\tleft_bytes={left_bytes}\tright_bytes={right_bytes}"
    );
    if let Some((left, right)) = first_difference {
        println!(
            "HOT_SEQ_SAMPLE\tleft_len={}\tright_len={}\tleft={}\tright={}",
            left.len(),
            right.len(),
            hex_prefix(&left),
            hex_prefix(&right),
        );
    }
}

fn hex_prefix(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(96)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn estimate_pages<'a>(
    groups: impl Iterator<Item = &'a HotGroup>,
    rows_per_page: usize,
) -> PageEstimate {
    let mut estimate = PageEstimate::default();
    for group in groups {
        for rows in group.rows.chunks(rows_per_page) {
            estimate.pages += 1;
            // Page key: the complete group prefix followed by a big-endian page ordinal.
            estimate.key_bytes += group.prefix_len + 4;
            // Conservative payload: version + row count, followed by fixed-width
            // suffix/value lengths and the exact row suffix/value bytes.
            estimate.value_bytes += 1 + 4;
            estimate.value_bytes += rows
                .iter()
                .map(|row| 8 + row.suffix_len + row.value_len)
                .sum::<usize>();
        }
    }
    estimate
}

struct DecodedHotGroup {
    prefix_len: usize,
    schema: String,
    file_id: Option<String>,
}

fn decode_hot_group(bytes: &[u8]) -> Result<DecodedHotGroup, String> {
    let mut offset = 0;
    let _branch = read_key_string(bytes, &mut offset, "branch")?;
    offset = offset
        .checked_add(16)
        .filter(|offset| *offset <= bytes.len())
        .ok_or_else(|| "truncated generation".to_string())?;
    let schema = read_key_string(bytes, &mut offset, "schema")?;
    let file_tag = *bytes
        .get(offset)
        .ok_or_else(|| "truncated file tag".to_string())?;
    offset += 1;
    let file_id = match file_tag {
        0 => None,
        1 => Some(read_key_string(bytes, &mut offset, "file")?),
        _ => return Err(format!("invalid file tag {file_tag}")),
    };
    Ok(DecodedHotGroup {
        prefix_len: offset,
        schema,
        file_id,
    })
}

fn read_key_string(bytes: &[u8], offset: &mut usize, field: &str) -> Result<String, String> {
    let mut value = Vec::new();
    loop {
        let byte = *bytes
            .get(*offset)
            .ok_or_else(|| format!("truncated {field}"))?;
        *offset += 1;
        if byte != 0 {
            value.push(byte);
            continue;
        }
        let terminator = *bytes
            .get(*offset)
            .ok_or_else(|| format!("truncated {field} terminator"))?;
        *offset += 1;
        if terminator == 0xff {
            value.push(0);
            continue;
        }
        if terminator != 0 {
            return Err(format!("invalid {field} terminator {terminator}"));
        }
        return String::from_utf8(value).map_err(|error| format!("invalid {field} UTF-8: {error}"));
    }
}

fn percentile(sorted: &[usize], percentile: usize) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
}
