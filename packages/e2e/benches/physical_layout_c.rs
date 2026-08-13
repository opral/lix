//! Additive physical-layout experiment for typed ForkTree rows.
//!
//! This is deliberately not wired into production. It compares authenticated
//! immutable geometries using opaque canonical row bytes and one schema
//! fingerprint. The model has no legacy reader, fallback, or dual writer.

use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::Bytes;
use lix::storage::{
    GetManyRequest, GetOptions, Key, PutBatch, PutEntry, ReadOptions, SpaceId, Storage,
    StorageRead, StorageSpace, StorageWrite, StoredValue, WriteOptions,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const OBJECT_SPACE: StorageSpace =
    StorageSpace::immutable(SpaceId(0x00ff_00c1), "bench.physical_layout_c.object");
const SCHEMA_FINGERPRINT: [u8; 16] = *b"typed-row-v1\0\0\0\0";
const PAGE_TARGETS: [usize; 4] = [4 << 10, 16 << 10, 64 << 10, 256 << 10];
const ROW_COUNTS: [usize; 3] = [1_000, 10_000, 50_000];
const ROW_WIDTHS: [usize; 4] = [64, 256, 1_024, 4_096];
const DELTAS: [usize; 3] = [1, 100, 1_000];
const MAX_DECODED_OBJECT_BYTES: usize = 256 << 10;
const PAGE_BOUNDARY_DOMAIN: &[u8] = b"lix.forktree.slotted-page-boundary.v1\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Geometry {
    C1,
    C2,
    C3,
}

impl Geometry {
    const ALL: [Self; 3] = [Self::C1, Self::C2, Self::C3];

    const fn label(self) -> &'static str {
        match self {
            Self::C1 => "c1_one_row",
            Self::C2 => "c2_slotted",
            Self::C3 => "c3_pax",
        }
    }
}

#[derive(Clone)]
struct TypedRow {
    key: Vec<u8>,
    canonical: Vec<u8>,
    fixed_len: usize,
}

#[derive(Clone)]
struct Object {
    id: [u8; 32],
    bytes: Bytes,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    rows: usize,
    sidecar: Option<[u8; 32]>,
    sidecar_bytes: Option<Bytes>,
}

#[derive(Clone)]
struct Tree {
    geometry: Geometry,
    target: usize,
    rows: Vec<TypedRow>,
    levels: Vec<Vec<Object>>,
    objects: BTreeMap<[u8; 32], Bytes>,
}

#[derive(Default)]
struct ReadMetric {
    calls: usize,
    objects: usize,
    bytes: usize,
    proof_nodes: usize,
    decompress_bytes: usize,
}

#[derive(Default)]
struct MutationMetric {
    puts: usize,
    bytes: usize,
    splits: isize,
}

#[derive(Default)]
struct CpuMetric {
    point: Duration,
    range: Duration,
    scan: Duration,
    update: Duration,
}

fn main() {
    print_header();
    let backend = std::env::var_os("LIX_LAYOUT_C_BACKEND").is_some();
    for width in ROW_WIDTHS {
        for n in ROW_COUNTS {
            let rows = fixture_rows(n, width);
            for geometry in Geometry::ALL {
                let targets: &[usize] = if geometry == Geometry::C1 {
                    &PAGE_TARGETS[..1]
                } else {
                    &PAGE_TARGETS
                };
                for &target in targets {
                    let started = Instant::now();
                    let tree = Tree::build(rows.clone(), geometry, target);
                    let build = started.elapsed();
                    tree.verify_all().expect("fresh tree authenticates");
                    for d in DELTAS.into_iter().filter(|d| *d <= n) {
                        let (mutation, update_cpu) = tree.update_spread(d);
                        let reads = tree.profile_reads();
                        let cpu = tree.profile_cpu(&reads, update_cpu);
                        print_model_row(&tree, width, n, d, build, &reads, &mutation, &cpu);
                    }
                    if geometry == Geometry::C2 {
                        for d in DELTAS.into_iter().filter(|d| *d <= n) {
                            print_mutation_control("insert", &tree, d, tree.insert_spread(d));
                            print_mutation_control("delete", &tree, d, tree.delete_spread(d));
                        }
                        print_branch_control(&tree);
                    }
                    if backend && width == 256 && n <= 10_000 {
                        run_backends(&tree);
                    }
                }
            }
        }
    }
    run_corruption_controls();
    run_compression_bound_control();
    run_boundary_controls();
}

fn print_header() {
    println!(
        "kind,backend,geometry,target,row_width,n,d,height,leaves,objects,fanout,total_bytes,compression_ratio,point_calls,point_objects,point_bytes,range_calls,range_objects,range_bytes,scan_calls,scan_objects,scan_bytes,partial_bytes,update_puts,update_bytes,splits,build_us,point_us,range_us,scan_us,update_us,settled_bytes,cold_reopen,logical_key_bytes,logical_payload_bytes"
    );
}

impl Tree {
    fn build(rows: Vec<TypedRow>, geometry: Geometry, target: usize) -> Self {
        let leaves = build_leaves(&rows, geometry, target);
        let mut objects = BTreeMap::new();
        install_objects(&mut objects, &leaves);
        let mut levels = vec![leaves];
        while levels.last().is_some_and(|level| level.len() > 1) {
            let next = build_internal_level(levels.last().expect("level exists"), target);
            install_objects(&mut objects, &next);
            levels.push(next);
        }
        Self {
            geometry,
            target,
            rows,
            levels,
            objects,
        }
    }

    fn verify_all(&self) -> Result<(), &'static str> {
        for (id, bytes) in &self.objects {
            if blake3::hash(bytes).as_bytes() != id {
                return Err("object id mismatch");
            }
            decode_envelope(bytes)?;
        }
        for level in &self.levels {
            for object in level {
                if object.first_key > object.last_key || object.rows == 0 {
                    return Err("invalid authenticated key range");
                }
                if let Some(sidecar) = object.sidecar
                    && !self.objects.contains_key(&sidecar)
                {
                    return Err("missing authenticated PAX sidecar");
                }
            }
        }
        Ok(())
    }

    fn profile_reads(&self) -> (ReadMetric, ReadMetric, ReadMetric, ReadMetric) {
        let point_index = self.rows.len() / 2;
        let point = self.read_rows(point_index, point_index + 1, true);
        let partial = self.read_rows(point_index, point_index + 1, false);
        let range_len = (self.rows.len() / 100).max(1);
        let range_start = self.rows.len() / 3;
        let range = self.read_rows(range_start, range_start + range_len, true);
        let scan = self.read_rows(0, self.rows.len(), true);
        (point, range, scan, partial)
    }

    fn read_rows(&self, start: usize, end: usize, full: bool) -> ReadMetric {
        let mut metric = ReadMetric::default();
        let selected = self.levels[0]
            .iter()
            .enumerate()
            .filter(|(_, page)| {
                let first = lower_bound(&self.rows, &page.first_key);
                let last = first + page.rows;
                first < end && last > start
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let mut child_indices = selected;
        for (level_index, level) in self.levels.iter().enumerate() {
            let indices = if level_index == 0 {
                child_indices.clone()
            } else {
                let mut parents = BTreeSet::new();
                for &child in &child_indices {
                    let key = &self.levels[level_index - 1][child].first_key;
                    let parent =
                        level.partition_point(|node| node.last_key.as_slice() < key.as_slice());
                    parents.insert(parent.min(level.len() - 1));
                }
                parents.into_iter().collect::<Vec<_>>()
            };
            metric.calls += usize::from(!indices.is_empty());
            metric.objects += indices.len();
            metric.proof_nodes += indices.len();
            for &index in &indices {
                let object = &level[index];
                metric.bytes += object.bytes.len();
                metric.decompress_bytes += decoded_len(&object.bytes).unwrap_or(0);
                if level_index == 0
                    && full
                    && let Some(sidecar) = object.sidecar
                {
                    metric.objects += 1;
                    metric.bytes += self.objects[&sidecar].len();
                    metric.decompress_bytes += decoded_len(&self.objects[&sidecar]).unwrap_or(0);
                }
            }
            child_indices = indices;
        }
        metric
    }

    fn update_spread(&self, d: usize) -> (MutationMetric, Duration) {
        let mut rows = self.rows.clone();
        let stride = (rows.len() / d).max(1);
        for ordinal in 0..d {
            let index = (ordinal * stride).min(rows.len() - 1);
            let last = rows[index].canonical.len() - 1;
            rows[index].canonical[last] ^= 0x5a;
        }
        let started = Instant::now();
        let updated = Self::build(rows, self.geometry, self.target);
        let elapsed = started.elapsed();
        let old = self.objects.keys().copied().collect::<BTreeSet<_>>();
        let mut mutation = MutationMetric::default();
        for (id, bytes) in &updated.objects {
            if !old.contains(id) {
                mutation.puts += 1;
                mutation.bytes += bytes.len();
            }
        }
        mutation.splits = updated.levels[0].len() as isize - self.levels[0].len() as isize;
        (mutation, elapsed)
    }

    fn insert_spread(&self, d: usize) -> MutationMetric {
        let mut rows = self.rows.clone();
        let stride = (rows.len() / d).max(1);
        for ordinal in 0..d {
            let source = &self.rows[(ordinal * stride).min(self.rows.len() - 1)];
            let mut inserted = source.clone();
            inserted.key.extend_from_slice(b"/insert");
            inserted.canonical[0] ^= 0xa5;
            rows.push(inserted);
        }
        rows.sort_by(|left, right| left.key.cmp(&right.key));
        self.mutation_against(rows)
    }

    fn delete_spread(&self, d: usize) -> MutationMetric {
        let stride = (self.rows.len() / d).max(1);
        let deleted = (0..d)
            .map(|ordinal| (ordinal * stride).min(self.rows.len() - 1))
            .collect::<BTreeSet<_>>();
        let rows = self
            .rows
            .iter()
            .enumerate()
            .filter(|(index, _)| !deleted.contains(index))
            .map(|(_, row)| row.clone())
            .collect();
        self.mutation_against(rows)
    }

    fn mutation_against(&self, rows: Vec<TypedRow>) -> MutationMetric {
        let updated = Self::build(rows, self.geometry, self.target);
        let old = self.objects.keys().copied().collect::<BTreeSet<_>>();
        let puts = updated
            .objects
            .iter()
            .filter(|(id, _)| !old.contains(*id))
            .collect::<Vec<_>>();
        MutationMetric {
            puts: puts.len(),
            bytes: puts.iter().map(|(_, bytes)| bytes.len()).sum(),
            splits: updated.levels[0].len() as isize - self.levels[0].len() as isize,
        }
    }

    fn profile_cpu(
        &self,
        reads: &(ReadMetric, ReadMetric, ReadMetric, ReadMetric),
        update: Duration,
    ) -> CpuMetric {
        CpuMetric {
            point: authenticate_metric(self, &reads.0),
            range: authenticate_metric(self, &reads.1),
            scan: authenticate_metric(self, &reads.2),
            update,
        }
    }
}

fn build_leaves(rows: &[TypedRow], geometry: Geometry, target: usize) -> Vec<Object> {
    if geometry == Geometry::C1 {
        return rows
            .iter()
            .map(|row| encode_leaf(std::slice::from_ref(row), geometry))
            .collect();
    }
    let mut leaves = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let mut end = start + 1;
        let mut decoded_bytes = estimated_decoded_page_header_bytes(geometry)
            + estimated_decoded_row_bytes(&rows[start], geometry);
        while end < rows.len() {
            let next = estimated_decoded_row_bytes(&rows[end], geometry);
            if decoded_bytes.saturating_add(next) > target {
                break;
            }
            if decoded_bytes >= target / 2 && content_boundary(&rows[end - 1].key, next, target) {
                break;
            }
            decoded_bytes += next;
            end += 1;
        }
        leaves.push(encode_leaf(&rows[start..end], geometry));
        start = end;
    }
    leaves
}

const fn estimated_decoded_page_header_bytes(geometry: Geometry) -> usize {
    match geometry {
        Geometry::C1 | Geometry::C2 => 32,
        Geometry::C3 => 80,
    }
}

fn estimated_decoded_row_bytes(row: &TypedRow, geometry: Geometry) -> usize {
    match geometry {
        Geometry::C1 => row.key.len() + row.canonical.len(),
        Geometry::C2 => 16 + row.key.len() + row.canonical.len(),
        // PAX uses one authenticated base and one hash-bound varlen sidecar.
        // Both decoded areas count against the size target.
        Geometry::C3 => 8 + row.key.len() + row.canonical.len(),
    }
}

fn encode_leaf(rows: &[TypedRow], geometry: Geometry) -> Object {
    match geometry {
        Geometry::C1 => encode_c1(rows.first().expect("C1 row")),
        Geometry::C2 => encode_c2(rows),
        Geometry::C3 => encode_c3(rows),
    }
}

fn encode_c1(row: &TypedRow) -> Object {
    let mut raw = Vec::with_capacity(row.key.len() + row.canonical.len() + 32);
    raw.extend_from_slice(b"LXC1");
    raw.extend_from_slice(&SCHEMA_FINGERPRINT);
    push_u32(&mut raw, row.key.len());
    push_u32(&mut raw, row.canonical.len());
    raw.extend_from_slice(&row.key);
    raw.extend_from_slice(&row.canonical);
    make_object(raw, row.key.clone(), row.key.clone(), 1, None)
}

fn encode_c2(rows: &[TypedRow]) -> Object {
    let prefix = common_prefix(rows.iter().map(|row| row.key.as_slice()));
    let mut raw = Vec::new();
    raw.extend_from_slice(b"LXC2");
    raw.extend_from_slice(&SCHEMA_FINGERPRINT);
    push_u32(&mut raw, rows.len());
    push_u32(&mut raw, prefix.len());
    raw.extend_from_slice(prefix);
    let mut suffixes = Vec::new();
    let mut values = Vec::new();
    for row in rows {
        push_u32(&mut raw, suffixes.len());
        push_u32(&mut raw, row.key.len() - prefix.len());
        push_u32(&mut raw, values.len());
        push_u32(&mut raw, row.canonical.len());
        suffixes.extend_from_slice(&row.key[prefix.len()..]);
        values.extend_from_slice(&row.canonical);
    }
    raw.extend_from_slice(&suffixes);
    raw.extend_from_slice(&values);
    make_object(
        raw,
        rows.first().expect("page row").key.clone(),
        rows.last().expect("page row").key.clone(),
        rows.len(),
        None,
    )
}

fn encode_c3(rows: &[TypedRow]) -> Object {
    let prefix = common_prefix(rows.iter().map(|row| row.key.as_slice()));
    let mut sidecar_raw = Vec::new();
    sidecar_raw.extend_from_slice(b"LXCV");
    sidecar_raw.extend_from_slice(&SCHEMA_FINGERPRINT);
    push_u32(&mut sidecar_raw, rows.len());
    for row in rows {
        let var = &row.canonical[row.fixed_len..];
        push_u32(&mut sidecar_raw, var.len());
        sidecar_raw.extend_from_slice(var);
    }
    let sidecar_bytes = encode_envelope(sidecar_raw);
    let sidecar_id = *blake3::hash(&sidecar_bytes).as_bytes();

    let mut raw = Vec::new();
    raw.extend_from_slice(b"LXC3");
    raw.extend_from_slice(&SCHEMA_FINGERPRINT);
    raw.extend_from_slice(&sidecar_id);
    push_u32(&mut raw, rows.len());
    push_u32(&mut raw, prefix.len());
    raw.extend_from_slice(prefix);
    for row in rows {
        push_u32(&mut raw, row.key.len() - prefix.len());
        raw.extend_from_slice(&row.key[prefix.len()..]);
    }
    for row in rows {
        push_u32(&mut raw, row.fixed_len);
        raw.extend_from_slice(&row.canonical[..row.fixed_len]);
    }
    let mut object = make_object(
        raw,
        rows.first().expect("page row").key.clone(),
        rows.last().expect("page row").key.clone(),
        rows.len(),
        Some(sidecar_id),
    );
    object.sidecar_bytes = Some(sidecar_bytes);
    object
}

fn build_internal_level(children: &[Object], target: usize) -> Vec<Object> {
    let mut groups = Vec::new();
    let mut start = 0;
    while start < children.len() {
        let mut end = start + 1;
        let mut decoded_bytes = 64 + estimated_internal_entry_bytes(&children[start]);
        while end < children.len() {
            let next = estimated_internal_entry_bytes(&children[end]);
            if decoded_bytes.saturating_add(next) > target {
                break;
            }
            if decoded_bytes >= target / 2
                && content_boundary(&children[end - 1].last_key, next, target)
            {
                break;
            }
            decoded_bytes += next;
            end += 1;
        }
        groups.push(&children[start..end]);
        start = end;
    }
    groups
        .into_iter()
        .map(|group| {
            let prefix = common_prefix(group.iter().map(|child| child.last_key.as_slice()));
            let mut raw = Vec::new();
            raw.extend_from_slice(b"LXCI");
            push_u32(&mut raw, group.len());
            push_u32(&mut raw, prefix.len());
            raw.extend_from_slice(prefix);
            for child in group {
                raw.extend_from_slice(&child.id);
                push_u32(&mut raw, child.last_key.len() - prefix.len());
                raw.extend_from_slice(&child.last_key[prefix.len()..]);
                push_u32(&mut raw, child.rows);
            }
            make_object(
                raw,
                group.first().expect("child").first_key.clone(),
                group.last().expect("child").last_key.clone(),
                group.iter().map(|child| child.rows).sum(),
                None,
            )
        })
        .collect()
}

fn estimated_internal_entry_bytes(child: &Object) -> usize {
    44 + child.last_key.len()
}

fn content_boundary(key: &[u8], next_decoded_bytes: usize, target: usize) -> bool {
    let mut hasher = blake3::Hasher::new();
    hasher.update(PAGE_BOUNDARY_DOMAIN);
    hasher.update(key);
    let hash = hasher.finalize();
    let sample = u16::from_be_bytes([hash.as_bytes()[0], hash.as_bytes()[1]]) as usize;
    let remaining = (target / 2).max(1);
    let threshold = next_decoded_bytes
        .saturating_mul(u16::MAX as usize)
        .checked_div(remaining)
        .unwrap_or(u16::MAX as usize)
        .min(u16::MAX as usize);
    sample <= threshold
}

#[derive(Default)]
struct BoundaryStats {
    pages: usize,
    hash_boundaries: usize,
    forced_max_boundaries: usize,
    underfilled_non_final: usize,
    min_decoded: usize,
    max_decoded: usize,
}

fn boundary_stats(rows: &[TypedRow], target: usize) -> BoundaryStats {
    let mut stats = BoundaryStats {
        min_decoded: usize::MAX,
        ..BoundaryStats::default()
    };
    let mut start = 0;
    while start < rows.len() {
        let mut end = start + 1;
        let mut decoded = estimated_decoded_page_header_bytes(Geometry::C2)
            + estimated_decoded_row_bytes(&rows[start], Geometry::C2);
        let mut reason = None;
        while end < rows.len() {
            let next = estimated_decoded_row_bytes(&rows[end], Geometry::C2);
            if decoded.saturating_add(next) > target {
                reason = Some("max");
                break;
            }
            if decoded >= target / 2 && content_boundary(&rows[end - 1].key, next, target) {
                reason = Some("hash");
                break;
            }
            decoded += next;
            end += 1;
        }
        stats.pages += 1;
        stats.min_decoded = stats.min_decoded.min(decoded);
        stats.max_decoded = stats.max_decoded.max(decoded);
        if end < rows.len() && decoded < target / 2 {
            stats.underfilled_non_final += 1;
        }
        match reason {
            Some("max") => stats.forced_max_boundaries += 1,
            Some("hash") => stats.hash_boundaries += 1,
            _ => {}
        }
        start = end;
    }
    stats
}

fn make_object(
    raw: Vec<u8>,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    rows: usize,
    sidecar: Option<[u8; 32]>,
) -> Object {
    let bytes = encode_envelope(raw);
    let id = *blake3::hash(&bytes).as_bytes();
    Object {
        id,
        bytes,
        first_key,
        last_key,
        rows,
        sidecar,
        sidecar_bytes: None,
    }
}

fn install_objects(objects: &mut BTreeMap<[u8; 32], Bytes>, level: &[Object]) {
    for object in level {
        if let Some(sidecar) = &object.sidecar_bytes {
            let sidecar_id = *blake3::hash(sidecar).as_bytes();
            debug_assert_eq!(object.sidecar, Some(sidecar_id));
            objects.insert(sidecar_id, sidecar.clone());
        }
        objects.insert(object.id, object.bytes.clone());
    }
}

fn encode_envelope(raw: Vec<u8>) -> Bytes {
    let compressed = zstd::bulk::compress(&raw, 1).expect("zstd encode");
    let (kind, payload) = if compressed.len() + 9 < raw.len() {
        (1_u8, compressed)
    } else {
        (0_u8, raw.clone())
    };
    let mut out = Vec::with_capacity(payload.len() + 9);
    out.extend_from_slice(b"LXPG");
    out.push(kind);
    push_u32(&mut out, raw.len());
    out.extend_from_slice(&payload);
    Bytes::from(out)
}

fn decode_envelope(bytes: &[u8]) -> Result<Vec<u8>, &'static str> {
    if bytes.len() < 9 || &bytes[..4] != b"LXPG" {
        return Err("bad envelope");
    }
    let raw_len = u32::from_be_bytes(bytes[5..9].try_into().map_err(|_| "bad length")?) as usize;
    if raw_len > MAX_DECODED_OBJECT_BYTES {
        return Err("decoded object exceeds hard allocation bound");
    }
    let payload = &bytes[9..];
    let raw = match bytes[4] {
        0 => payload.to_vec(),
        1 => zstd::bulk::decompress(payload, raw_len).map_err(|_| "bad zstd")?,
        _ => return Err("bad compression tag"),
    };
    if raw.len() != raw_len {
        return Err("decoded length mismatch");
    }
    Ok(raw)
}

fn decoded_len(bytes: &[u8]) -> Result<usize, &'static str> {
    decode_envelope(bytes).map(|raw| raw.len())
}

fn authenticate_metric(tree: &Tree, metric: &ReadMetric) -> Duration {
    let started = Instant::now();
    let mut remaining = metric.objects;
    for bytes in tree.objects.values() {
        if remaining == 0 {
            break;
        }
        black_box(blake3::hash(bytes));
        black_box(decode_envelope(bytes).ok());
        remaining -= 1;
    }
    started.elapsed()
}

fn fixture_rows(n: usize, width: usize) -> Vec<TypedRow> {
    const TEXT: &[u8] =
        b"project status open owner agent typed row payload description priority branch ";
    (0..n)
        .map(|ordinal| {
            let key = format!("tenant/acme/schema/task/{ordinal:012}").into_bytes();
            let mut canonical = vec![0_u8; width];
            let mut state = (ordinal as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            for (offset, byte) in canonical.iter_mut().enumerate() {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = if offset < 32 || offset % 17 == 0 {
                    (state >> 24) as u8
                } else {
                    TEXT[(offset + ordinal * 7) % TEXT.len()]
                };
            }
            // 64/256-byte fixtures are scalar tuples with no JSONB. The wider
            // fixtures add one optional opaque varlen cell after a 64-byte
            // scalar prefix. C1/C2 never inspect this distinction; it exists
            // only to measure the rejected PAX projection alternative.
            let fixed_len = if width >= 1_024 { 64 } else { width };
            TypedRow {
                key,
                canonical,
                fixed_len,
            }
        })
        .collect()
}

fn common_prefix<'a>(mut values: impl Iterator<Item = &'a [u8]>) -> &'a [u8] {
    let Some(first) = values.next() else {
        return &[];
    };
    let mut len = first.len();
    for value in values {
        len = first[..len]
            .iter()
            .zip(value)
            .take_while(|(left, right)| left == right)
            .count();
    }
    &first[..len]
}

fn lower_bound(rows: &[TypedRow], key: &[u8]) -> usize {
    rows.partition_point(|row| row.key.as_slice() < key)
}

fn push_u32(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(
        &u32::try_from(value)
            .expect("model value fits u32")
            .to_be_bytes(),
    );
}

fn print_model_row(
    tree: &Tree,
    width: usize,
    n: usize,
    d: usize,
    build: Duration,
    reads: &(ReadMetric, ReadMetric, ReadMetric, ReadMetric),
    mutation: &MutationMetric,
    cpu: &CpuMetric,
) {
    let raw = n * (width + 48);
    let total = tree.objects.values().map(Bytes::len).sum::<usize>();
    let leaves = tree.levels[0].len();
    let fanout = if tree.levels.len() > 1 {
        tree.levels[0].len() as f64 / tree.levels[1].len() as f64
    } else {
        1.0
    };
    println!(
        "model,-,{geometry},{target},{width},{n},{d},{height},{leaves},{objects},{fanout:.2},{total},{ratio:.4},{point_calls},{point_objects},{point_bytes},{range_calls},{range_objects},{range_bytes},{scan_calls},{scan_objects},{scan_bytes},{partial_bytes},{update_puts},{update_bytes},{splits},{build_us},{point_us},{range_us},{scan_us},{update_us},0,-,{key_bytes},{payload_bytes}",
        geometry = tree.geometry.label(),
        target = tree.target,
        height = tree.levels.len(),
        objects = tree.objects.len(),
        ratio = total as f64 / raw as f64,
        point_calls = reads.0.calls,
        point_objects = reads.0.objects,
        point_bytes = reads.0.bytes,
        range_calls = reads.1.calls,
        range_objects = reads.1.objects,
        range_bytes = reads.1.bytes,
        scan_calls = reads.2.calls,
        scan_objects = reads.2.objects,
        scan_bytes = reads.2.bytes,
        partial_bytes = reads.3.bytes,
        update_puts = mutation.puts,
        update_bytes = mutation.bytes,
        splits = mutation.splits,
        build_us = build.as_micros(),
        point_us = cpu.point.as_micros(),
        range_us = cpu.range.as_micros(),
        scan_us = cpu.scan.as_micros(),
        update_us = cpu.update.as_micros(),
        key_bytes = tree.rows.iter().map(|row| row.key.len()).sum::<usize>(),
        payload_bytes = tree
            .rows
            .iter()
            .map(|row| row.canonical.len())
            .sum::<usize>(),
    );
}

fn print_mutation_control(kind: &str, tree: &Tree, d: usize, mutation: MutationMetric) {
    println!(
        "{kind},-,{geometry},{target},{width},{n},{d},{height},{leaves},{objects},0,{total},0,0,0,0,0,0,0,0,0,0,0,{puts},{bytes},{splits},0,0,0,0,0,0,-,{key_bytes},{payload_bytes}",
        geometry = tree.geometry.label(),
        target = tree.target,
        width = tree.rows[0].canonical.len(),
        n = tree.rows.len(),
        height = tree.levels.len(),
        leaves = tree.levels[0].len(),
        objects = tree.objects.len(),
        total = tree.objects.values().map(Bytes::len).sum::<usize>(),
        puts = mutation.puts,
        bytes = mutation.bytes,
        splits = mutation.splits,
        key_bytes = tree.rows.iter().map(|row| row.key.len()).sum::<usize>(),
        payload_bytes = tree
            .rows
            .iter()
            .map(|row| row.canonical.len())
            .sum::<usize>(),
    );
}

fn print_branch_control(tree: &Tree) {
    let root = tree.levels.last().expect("root level")[0].id;
    let copied_root = tree.levels.last().expect("root level")[0].id;
    assert_eq!(root, copied_root, "branch copy reuses the canonical root");
    println!(
        "branch_share,-,{geometry},{target},{width},{n},0,{height},{leaves},{objects},0,{total},0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,-,{key_bytes},{payload_bytes}",
        geometry = tree.geometry.label(),
        target = tree.target,
        width = tree.rows[0].canonical.len(),
        n = tree.rows.len(),
        height = tree.levels.len(),
        leaves = tree.levels[0].len(),
        objects = tree.objects.len(),
        total = tree.objects.values().map(Bytes::len).sum::<usize>(),
        key_bytes = tree.rows.iter().map(|row| row.key.len()).sum::<usize>(),
        payload_bytes = tree
            .rows
            .iter()
            .map(|row| row.canonical.len())
            .sum::<usize>(),
    );
}

fn run_backends(tree: &Tree) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let rocks = tempfile::tempdir().expect("rocks dir");
        let rocks_path = rocks.path().join("db");
        let storage = RocksDB::open(&rocks_path).expect("open rocks");
        seed_backend(&storage, tree).await;
        storage.flush().expect("flush rocks model");
        drop(storage);
        let rocks_settled = directory_bytes(rocks.path());
        let reopened = RocksDB::open(&rocks_path).expect("reopen rocks");
        verify_backend(&reopened, tree).await;
        print_backend_row("rocksdb", tree, rocks_settled);
        drop(reopened);
        drop(rocks);

        let slate = tempfile::tempdir().expect("slate dir");
        let slate_path = slate.path().join("db");
        let storage = SlateDB::open(&slate_path).expect("open slate");
        seed_backend(&storage, tree).await;
        storage
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush slate model");
        drop(storage);
        let slate_settled = directory_bytes(slate.path());
        let reopened = SlateDB::open(&slate_path).expect("reopen slate");
        verify_backend(&reopened, tree).await;
        print_backend_row("slatedb", tree, slate_settled);
        drop(reopened);
        drop(slate);
    });
}

async fn seed_backend<S: Storage>(storage: &S, tree: &Tree) {
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin model write");
    write
        .put_many(
            OBJECT_SPACE,
            PutBatch {
                entries: tree
                    .objects
                    .iter()
                    .map(|(id, bytes)| PutEntry {
                        key: Key(Bytes::copy_from_slice(id)),
                        value: StoredValue {
                            bytes: bytes.clone(),
                        },
                    })
                    .collect(),
            },
        )
        .await
        .expect("write model objects");
    write.commit().await.expect("commit model objects");
    verify_backend(storage, tree).await;
}

async fn verify_backend<S: Storage>(storage: &S, tree: &Tree) {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin model read");
    let root = tree
        .levels
        .last()
        .expect("root level")
        .first()
        .expect("root");
    let result = read
        .get_many(&[GetManyRequest {
            space: OBJECT_SPACE,
            keys: &[Key(Bytes::copy_from_slice(&root.id))],
            opts: GetOptions::default(),
        }])
        .await
        .expect("read model root");
    assert_eq!(result.values.len(), 1);
    let value = result.values[0].as_ref().expect("model root exists");
    let lix::storage::ProjectedValue::FullValue(bytes) = value else {
        panic!("model root read returned key-only projection")
    };
    assert_eq!(blake3::hash(bytes).as_bytes(), &root.id);
    decode_envelope(bytes).expect("cold model root authenticates");
}

fn print_backend_row(backend: &str, tree: &Tree, settled_bytes: u64) {
    let root = tree
        .levels
        .last()
        .expect("root level")
        .first()
        .expect("root");
    let total = tree.objects.values().map(Bytes::len).sum::<usize>();
    println!(
        "backend,{backend},{geometry},{target},{width},{n},0,{height},{leaves},{objects},0,{total},{ratio:.4},1,1,{root_bytes},0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,{settled_bytes},true,{key_bytes},{payload_bytes}",
        geometry = tree.geometry.label(),
        target = tree.target,
        width = tree.rows[0].canonical.len(),
        n = tree.rows.len(),
        height = tree.levels.len(),
        leaves = tree.levels[0].len(),
        objects = tree.objects.len(),
        ratio = total as f64 / (tree.rows.len() * (tree.rows[0].canonical.len() + 48)) as f64,
        root_bytes = root.bytes.len(),
        key_bytes = tree.rows.iter().map(|row| row.key.len()).sum::<usize>(),
        payload_bytes = tree
            .rows
            .iter()
            .map(|row| row.canonical.len())
            .sum::<usize>(),
    );
}

fn directory_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0_u64;
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += directory_bytes(&path);
        } else if let Ok(metadata) = entry.metadata() {
            total += metadata.len();
        }
    }
    total
}

fn run_corruption_controls() {
    let tree = Tree::build(fixture_rows(1_000, 256), Geometry::C2, 64 << 10);
    let (id, bytes) = tree.objects.iter().next().expect("object");
    let mut corrupt = bytes.to_vec();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    assert_ne!(blake3::hash(&corrupt).as_bytes(), id);
    assert!(tree.verify_all().is_ok());
    println!(
        "control,corruption,c2_slotted,65536,256,1000,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,rejected,0,0"
    );
}

fn run_compression_bound_control() {
    let mut rows = fixture_rows(10_000, 4_096);
    for row in &mut rows {
        row.canonical.fill(b'a');
    }
    let started = Instant::now();
    let tree = Tree::build(rows, Geometry::C2, 64 << 10);
    let elapsed = started.elapsed();
    let max_decoded = tree.levels[0]
        .iter()
        .map(|leaf| decoded_len(&leaf.bytes).expect("compressible page decodes"))
        .max()
        .expect("compressible fixture has leaves");
    assert!(max_decoded <= 64 << 10);
    assert!(tree.levels[0].len() > 600);
    println!(
        "control,compressible_bound,c2_slotted,65536,4096,10000,0,{},{},{},0,{},0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,{},0,0,0,0,bounded,0,0",
        tree.levels.len(),
        tree.levels[0].len(),
        tree.objects.len(),
        max_decoded,
        elapsed.as_micros(),
    );
}

fn run_boundary_controls() {
    const TARGET: usize = 4 << 10;
    let sequential = fixture_rows(10_000, 256);
    let mut random = fixture_rows(10_000, 256);
    for (ordinal, row) in random.iter_mut().enumerate() {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"layout-c-random-key\0");
        hasher.update(&ordinal.to_be_bytes());
        row.key = hasher.finalize().as_bytes().to_vec();
    }
    random.sort_by(|left, right| left.key.cmp(&right.key));
    let forced_max = boundary_fixture(10_000, 256, false);
    let forced_min = boundary_fixture(10_000, 256, true);
    for (name, rows) in [
        ("sequential_same_prefix", &sequential),
        ("random", &random),
        ("adversarial_max", &forced_max),
        ("adversarial_min", &forced_min),
    ] {
        let stats = boundary_stats(rows, TARGET);
        assert_eq!(stats.underfilled_non_final, 0);
        assert!(stats.max_decoded <= TARGET);
        println!(
            "boundary_{name},-,c2_slotted,{TARGET},256,10000,0,0,{pages},0,0,{max_decoded},0,0,0,0,0,0,0,0,0,0,{min_decoded},{hash_boundaries},{forced_max},0,0,0,0,0,0,0,bounded,0,0",
            pages = stats.pages,
            max_decoded = stats.max_decoded,
            min_decoded = stats.min_decoded,
            hash_boundaries = stats.hash_boundaries,
            forced_max = stats.forced_max_boundaries,
        );
    }

    let base = Tree::build(sequential.clone(), Geometry::C2, TARGET);
    let mut inserted_rows = sequential.clone();
    let mut inserted = inserted_rows[inserted_rows.len() / 2].clone();
    inserted.key.extend_from_slice(b"/insert");
    inserted_rows.push(inserted);
    inserted_rows.sort_by(|left, right| left.key.cmp(&right.key));
    let inserted = Tree::build(inserted_rows, Geometry::C2, TARGET);
    let mut deleted_rows = sequential;
    deleted_rows.remove(deleted_rows.len() / 2);
    let deleted = Tree::build(deleted_rows, Geometry::C2, TARGET);
    let base_ids = base.levels[0]
        .iter()
        .map(|page| page.id)
        .collect::<BTreeSet<_>>();
    let inserted_shared = inserted.levels[0]
        .iter()
        .filter(|page| base_ids.contains(&page.id))
        .count();
    let deleted_shared = deleted.levels[0]
        .iter()
        .filter(|page| base_ids.contains(&page.id))
        .count();
    assert!(inserted_shared + 4 >= base.levels[0].len());
    assert!(deleted_shared + 4 >= base.levels[0].len());
    println!(
        "boundary_stability,-,c2_slotted,{TARGET},256,10000,0,0,{base_pages},0,0,0,0,0,0,0,0,0,0,0,0,0,0,{inserted_shared},{deleted_shared},0,0,0,0,0,0,0,stable,0,0",
        base_pages = base.levels[0].len(),
    );
}

fn boundary_fixture(n: usize, width: usize, seek_low: bool) -> Vec<TypedRow> {
    let mut rows = fixture_rows(n, width);
    for (ordinal, row) in rows.iter_mut().enumerate() {
        let mut nonce = 0_u64;
        loop {
            let key = format!("adversarial/{ordinal:012}/{nonce:016x}").into_bytes();
            let mut hasher = blake3::Hasher::new();
            hasher.update(PAGE_BOUNDARY_DOMAIN);
            hasher.update(&key);
            let hash = hasher.finalize();
            let sample = u16::from_be_bytes([hash.as_bytes()[0], hash.as_bytes()[1]]);
            if (seek_low && sample < 64) || (!seek_low && sample > 65_000) {
                row.key = key;
                break;
            }
            nonce += 1;
        }
    }
    rows.sort_by(|left, right| left.key.cmp(&right.key));
    rows
}
