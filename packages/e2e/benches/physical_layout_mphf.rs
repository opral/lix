//! EXP-MPHF-18: authenticated page-local minimal-perfect-hash experiment.
//!
//! This is deliberately not wired into production. C2 is the approved control.
//! The candidates retain C2's canonical sorted key/slot directory as the sole
//! scan and ordering representation, and add an authenticated deterministic CHD
//! displacement table that may only propose a slot. Every hit reconstructs and
//! compares the complete canonical key before returning the value. The model has
//! no fallback reader, second object authority, cache, or dual writer.

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
const ROW_COUNTS: [usize; 4] = [1_000, 10_000, 50_000, 100_000];
const DELTAS: [usize; 3] = [1, 100, 1_000];
const MAX_DECODED_OBJECT_BYTES: usize = 256 << 10;
const PAGE_BOUNDARY_DOMAIN: &[u8] = b"lix.forktree.slotted-page-boundary.v1\0";

#[derive(Clone, Copy, Debug)]
enum PkKind {
    Integer,
    Uuid,
    Text,
    Composite,
}

impl PkKind {
    const ALL: [Self; 4] = [Self::Integer, Self::Uuid, Self::Text, Self::Composite];

    const fn label(self) -> &'static str {
        match self {
            Self::Integer => "integer",
            Self::Uuid => "uuid",
            Self::Text => "text",
            Self::Composite => "composite",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Shape {
    Narrow,
    Wide,
}

impl Shape {
    const ALL: [Self; 2] = [Self::Narrow, Self::Wide];

    const fn label(self) -> &'static str {
        match self {
            Self::Narrow => "narrow",
            Self::Wide => "wide",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum JsonDensity {
    Absent,
    Sparse,
    Dense,
}

impl JsonDensity {
    const ALL: [Self; 3] = [Self::Absent, Self::Sparse, Self::Dense];

    const fn label(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Sparse => "sparse",
            Self::Dense => "dense",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Geometry {
    C2,
    Mphf8,
    Mphf16,
}

impl Geometry {
    const ALL: [Self; 3] = [Self::C2, Self::Mphf8, Self::Mphf16];

    const fn label(self) -> &'static str {
        match self {
            Self::C2 => "c2_slotted",
            Self::Mphf8 => "mphf8_chd",
            Self::Mphf16 => "mphf16_chd",
        }
    }

    const fn fingerprint_bytes(self) -> Option<usize> {
        match self {
            Self::C2 => None,
            Self::Mphf8 => Some(1),
            Self::Mphf16 => Some(2),
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
    ids: Vec<[u8; 32]>,
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
    let selected_n = std::env::var("LIX_MPHF_N")
        .ok()
        .and_then(|value| value.parse::<usize>().ok());
    let selected_pk = std::env::var("LIX_MPHF_PK").ok();
    let selected_shape = std::env::var("LIX_MPHF_SHAPE").ok();
    let selected_json = std::env::var("LIX_MPHF_JSON").ok();
    let selected_target = std::env::var("LIX_MPHF_TARGET")
        .ok()
        .and_then(|value| value.parse::<usize>().ok());
    let selected_geometry = std::env::var("LIX_MPHF_GEOMETRY").ok();
    for pk in PkKind::ALL.into_iter().filter(|kind| {
        selected_pk
            .as_deref()
            .is_none_or(|value| value == kind.label())
    }) {
        for shape in Shape::ALL.into_iter().filter(|shape| {
            selected_shape
                .as_deref()
                .is_none_or(|value| value == shape.label())
        }) {
            for json in JsonDensity::ALL.into_iter().filter(|density| {
                selected_json
                    .as_deref()
                    .is_none_or(|value| value == density.label())
            }) {
                for n in ROW_COUNTS
                    .into_iter()
                    .filter(|n| selected_n.is_none_or(|value| value == *n))
                {
                    let rows = fixture_rows_for_schema(n, pk, shape, json);
                    let width = rows[0].canonical.len();
                    println!(
                        "scenario,pk={},shape={},json={},n={},tuple_bytes={}",
                        pk.label(),
                        shape.label(),
                        json.label(),
                        n,
                        width
                    );
                    for geometry in Geometry::ALL.into_iter().filter(|geometry| {
                        selected_geometry
                            .as_deref()
                            .is_none_or(|value| value == geometry.label())
                    }) {
                        for target in PAGE_TARGETS
                            .into_iter()
                            .filter(|target| selected_target.is_none_or(|value| value == *target))
                        {
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
                                    print_mutation_control(
                                        "insert",
                                        &tree,
                                        d,
                                        tree.insert_spread(d),
                                    );
                                    print_mutation_control(
                                        "delete",
                                        &tree,
                                        d,
                                        tree.delete_spread(d),
                                    );
                                }
                            }
                            print_branch_control(&tree);
                            if backend && n <= 10_000 {
                                run_backends(&tree);
                            }
                        }
                    }
                }
            }
        }
    }
    if std::env::var_os("LIX_MPHF_CONTROLS").is_some() {
        run_corruption_controls();
        run_compression_bound_control();
        run_boundary_controls();
    }
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
        let (store, branch_ref_id) = self.authenticated_store_for_branch(b"main");
        let verified = verify_branch(&store, branch_ref_id)?;
        if verified.branch_id != b"main"
            || verified.root_id != self.root_id()
            || verified.rows != self.rows.len()
        {
            return Err("authenticated branch/root summary mismatch");
        }
        if verified.visited != self.objects.keys().copied().collect() {
            return Err("unreachable or missing authenticated page object");
        }
        Ok(())
    }

    fn root_id(&self) -> [u8; 32] {
        self.levels.last().expect("root level")[0].id
    }

    fn authenticated_store_for_branch(
        &self,
        branch_id: &[u8],
    ) -> (BTreeMap<[u8; 32], Bytes>, [u8; 32]) {
        let mut store = self.objects.clone();
        let branch_ref = encode_branch_ref(branch_id, self.root_id());
        let branch_ref_id = *blake3::hash(&branch_ref).as_bytes();
        store.insert(branch_ref_id, branch_ref);
        (store, branch_ref_id)
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
                metric.ids.push(object.id);
                metric.bytes += object.bytes.len();
                metric.decompress_bytes += decoded_len(&object.bytes).unwrap_or(0);
                let _ = full;
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
            point: point_lookup_cpu(self),
            range: authenticate_metric(self, &reads.1),
            scan: authenticate_metric(self, &reads.2),
            update,
        }
    }
}

fn build_leaves(rows: &[TypedRow], geometry: Geometry, target: usize) -> Vec<Object> {
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
        Geometry::C2 | Geometry::Mphf8 | Geometry::Mphf16 => 32,
    }
}

fn estimated_decoded_row_bytes(row: &TypedRow, geometry: Geometry) -> usize {
    match geometry {
        Geometry::C2 | Geometry::Mphf8 | Geometry::Mphf16 => {
            16 + row.key.len() + row.canonical.len()
        }
    }
}

fn encode_leaf(rows: &[TypedRow], geometry: Geometry) -> Object {
    match geometry {
        Geometry::C2 => encode_c2(rows),
        Geometry::Mphf8 | Geometry::Mphf16 => {
            encode_mphf(rows, geometry.fingerprint_bytes().expect("MPHF width"))
        }
    }
}

fn encode_c2(rows: &[TypedRow]) -> Object {
    let prefix = common_prefix(rows.iter().map(|row| row.key.as_slice()));
    let mut raw = Vec::new();
    raw.extend_from_slice(b"LXC2");
    raw.extend_from_slice(&SCHEMA_FINGERPRINT);
    push_bytes(&mut raw, &rows.first().expect("page row").key);
    push_bytes(&mut raw, &rows.last().expect("page row").key);
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
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MphfTable {
    seed: u32,
    displacements: Vec<u16>,
    slot_ordinals: Vec<u16>,
    fingerprints: Vec<u16>,
}

fn encode_mphf(rows: &[TypedRow], fingerprint_bytes: usize) -> Object {
    let prefix = common_prefix(rows.iter().map(|row| row.key.as_slice()));
    let suffix_bytes = rows.iter().map(|row| row.key.len() - prefix.len()).sum();
    let table = build_mphf(rows, fingerprint_bytes)
        .filter(|table| mphf_encoded_len(table, fingerprint_bytes) < suffix_bytes);
    let mut raw = Vec::new();
    raw.extend_from_slice(b"LXMH");
    raw.extend_from_slice(&SCHEMA_FINGERPRINT);
    push_bytes(&mut raw, &rows.first().expect("page row").key);
    push_bytes(&mut raw, &rows.last().expect("page row").key);
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
    raw.push(u8::try_from(fingerprint_bytes).expect("fingerprint width"));
    raw.push(u8::from(table.is_some()));
    if let Some(table) = &table {
        raw.extend_from_slice(&table.seed.to_be_bytes());
        push_u32(&mut raw, table.displacements.len());
        for displacement in &table.displacements {
            raw.extend_from_slice(&displacement.to_be_bytes());
        }
        for ordinal in &table.slot_ordinals {
            raw.extend_from_slice(&ordinal.to_be_bytes());
        }
        for fingerprint in &table.fingerprints {
            if fingerprint_bytes == 1 {
                raw.push(*fingerprint as u8);
            } else {
                raw.extend_from_slice(&fingerprint.to_be_bytes());
            }
        }
    }
    raw.extend_from_slice(&suffixes);
    raw.extend_from_slice(&values);
    make_object(
        raw,
        rows.first().expect("page row").key.clone(),
        rows.last().expect("page row").key.clone(),
        rows.len(),
    )
}

fn build_mphf(rows: &[TypedRow], fingerprint_bytes: usize) -> Option<MphfTable> {
    let n = rows.len();
    if n == 0 || n > u16::MAX as usize || !matches!(fingerprint_bytes, 1 | 2) {
        return None;
    }
    for seed in 0..=255_u32 {
        let hashes = rows
            .iter()
            .map(|row| mphf_hash(&row.key, seed, n))
            .collect::<Vec<_>>();
        let mut buckets = vec![Vec::new(); n];
        for (ordinal, &(bucket, _, _)) in hashes.iter().enumerate() {
            buckets[bucket].push(ordinal);
        }
        let mut order = (0..n).collect::<Vec<_>>();
        order.sort_by_key(|bucket| (std::cmp::Reverse(buckets[*bucket].len()), *bucket));
        let mut occupied = vec![false; n];
        let mut displacements = vec![0_u16; n];
        let mut slot_ordinals = vec![u16::MAX; n];
        let mut valid = true;
        for bucket in order {
            if buckets[bucket].is_empty() {
                continue;
            }
            let found = (0..=u16::MAX).find(|displacement| {
                let mut proposed = BTreeSet::new();
                buckets[bucket].iter().all(|ordinal| {
                    let slot = (hashes[*ordinal].1 + usize::from(*displacement)) % n;
                    !occupied[slot] && proposed.insert(slot)
                })
            });
            let Some(displacement) = found else {
                valid = false;
                break;
            };
            displacements[bucket] = displacement;
            for ordinal in &buckets[bucket] {
                let slot = (hashes[*ordinal].1 + usize::from(displacement)) % n;
                occupied[slot] = true;
                slot_ordinals[slot] = u16::try_from(*ordinal).ok()?;
            }
        }
        if valid && occupied.iter().all(|occupied| *occupied) {
            let mut fingerprints = vec![0_u16; n];
            for (ordinal, &(_, _, fingerprint)) in hashes.iter().enumerate() {
                let bucket = hashes[ordinal].0;
                let slot = (hashes[ordinal].1 + usize::from(displacements[bucket])) % n;
                fingerprints[slot] = if fingerprint_bytes == 1 {
                    fingerprint & 0xff
                } else {
                    fingerprint
                };
            }
            return Some(MphfTable {
                seed,
                displacements,
                slot_ordinals,
                fingerprints,
            });
        }
    }
    None
}

fn mphf_hash(key: &[u8], seed: u32, n: usize) -> (usize, usize, u16) {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"lix.forktree.c2.mphf.v1\0");
    hasher.update(&seed.to_be_bytes());
    hasher.update(key);
    let hash = hasher.finalize();
    let bytes = hash.as_bytes();
    let first = u64::from_be_bytes(bytes[0..8].try_into().expect("hash width"));
    let second = u64::from_be_bytes(bytes[8..16].try_into().expect("hash width"));
    (
        first as usize % n,
        second as usize % n,
        u16::from_be_bytes([bytes[16], bytes[17]]),
    )
}

fn mphf_encoded_len(table: &MphfTable, fingerprint_bytes: usize) -> usize {
    8 + table.displacements.len() * 2
        + table.slot_ordinals.len() * 2
        + table.fingerprints.len() * fingerprint_bytes
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
            raw.extend_from_slice(&SCHEMA_FINGERPRINT);
            push_bytes(&mut raw, &group.first().expect("child").first_key);
            push_bytes(&mut raw, &group.last().expect("child").last_key);
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

fn make_object(raw: Vec<u8>, first_key: Vec<u8>, last_key: Vec<u8>, rows: usize) -> Object {
    let bytes = encode_envelope(raw);
    let id = *blake3::hash(&bytes).as_bytes();
    Object {
        id,
        bytes,
        first_key,
        last_key,
        rows,
    }
}

fn install_objects(objects: &mut BTreeMap<[u8; 32], Bytes>, level: &[Object]) {
    for object in level {
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

fn encode_branch_ref(branch_id: &[u8], root_id: [u8; 32]) -> Bytes {
    let mut raw = Vec::new();
    raw.extend_from_slice(b"LXBR");
    raw.extend_from_slice(&SCHEMA_FINGERPRINT);
    push_bytes(&mut raw, branch_id);
    raw.extend_from_slice(&root_id);
    encode_envelope(raw)
}

struct VerifiedBranch {
    branch_id: Vec<u8>,
    root_id: [u8; 32],
    rows: usize,
    visited: BTreeSet<[u8; 32]>,
}

#[derive(Clone)]
struct NodeSummary {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    rows: usize,
}

fn verify_branch(
    store: &BTreeMap<[u8; 32], Bytes>,
    branch_ref_id: [u8; 32],
) -> Result<VerifiedBranch, &'static str> {
    let branch_bytes = authenticated_object(store, branch_ref_id)?;
    let raw = decode_envelope(branch_bytes)?;
    let mut cursor = Cursor::new(&raw);
    cursor.expect_magic(b"LXBR")?;
    cursor.expect_fingerprint()?;
    let branch_id = cursor.bytes()?.to_vec();
    if branch_id.is_empty() {
        return Err("empty authenticated branch identity");
    }
    let root_id = cursor.object_id()?;
    cursor.finish()?;
    let mut visited = BTreeSet::new();
    let mut visiting = BTreeSet::new();
    let summary = verify_node(store, root_id, &mut visited, &mut visiting)?;
    Ok(VerifiedBranch {
        branch_id,
        root_id,
        rows: summary.rows,
        visited,
    })
}

fn authenticated_object(
    store: &BTreeMap<[u8; 32], Bytes>,
    id: [u8; 32],
) -> Result<&Bytes, &'static str> {
    let bytes = store.get(&id).ok_or("missing authenticated object")?;
    if blake3::hash(bytes).as_bytes() != &id {
        return Err("object id mismatch");
    }
    Ok(bytes)
}

fn verify_node(
    store: &BTreeMap<[u8; 32], Bytes>,
    id: [u8; 32],
    visited: &mut BTreeSet<[u8; 32]>,
    visiting: &mut BTreeSet<[u8; 32]>,
) -> Result<NodeSummary, &'static str> {
    if !visiting.insert(id) {
        return Err("authenticated object cycle");
    }
    let bytes = authenticated_object(store, id)?;
    let raw = decode_envelope(bytes)?;
    let summary = match raw.get(..4) {
        Some(b"LXC2") => verify_c2(&raw)?,
        Some(b"LXMH") => verify_mphf(&raw)?,
        Some(b"LXCI") => verify_internal(store, &raw, visited, visiting)?,
        _ => return Err("wrong authenticated object domain"),
    };
    visiting.remove(&id);
    visited.insert(id);
    Ok(summary)
}

fn read_page_header(cursor: &mut Cursor<'_>) -> Result<(Vec<u8>, Vec<u8>), &'static str> {
    cursor.expect_fingerprint()?;
    let first = cursor.bytes()?.to_vec();
    let last = cursor.bytes()?.to_vec();
    if first.is_empty() || first > last {
        return Err("invalid embedded page key bounds");
    }
    Ok((first, last))
}

fn verify_c2(raw: &[u8]) -> Result<NodeSummary, &'static str> {
    let mut cursor = Cursor::new(raw);
    cursor.expect_magic(b"LXC2")?;
    let (first, last) = read_page_header(&mut cursor)?;
    let rows = cursor.u32()?;
    if rows == 0 {
        return Err("empty slotted page");
    }
    let prefix = cursor.bytes()?.to_vec();
    let mut directory = Vec::with_capacity(rows);
    for _ in 0..rows {
        directory.push((cursor.u32()?, cursor.u32()?, cursor.u32()?, cursor.u32()?));
    }
    let suffix_total = directory
        .iter()
        .map(|(offset, len, _, _)| offset.checked_add(*len).ok_or("suffix overflow"))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(0);
    let suffixes = cursor.take(suffix_total)?;
    let values = cursor.remaining();
    let mut expected_suffix = 0;
    let mut expected_value = 0;
    let mut keys = Vec::with_capacity(rows);
    for (suffix_offset, suffix_len, value_offset, value_len) in directory {
        if suffix_offset != expected_suffix || value_offset != expected_value {
            return Err("non-canonical slot directory offsets");
        }
        let suffix_end = suffix_offset
            .checked_add(suffix_len)
            .ok_or("suffix overflow")?;
        let value_end = value_offset
            .checked_add(value_len)
            .ok_or("value overflow")?;
        let suffix = suffixes
            .get(suffix_offset..suffix_end)
            .ok_or("slot suffix out of bounds")?;
        values
            .get(value_offset..value_end)
            .ok_or("slot value out of bounds")?;
        let mut key = prefix.clone();
        key.extend_from_slice(suffix);
        keys.push(key);
        expected_suffix = suffix_end;
        expected_value = value_end;
    }
    if expected_value != values.len()
        || keys.windows(2).any(|pair| pair[0] >= pair[1])
        || keys.first() != Some(&first)
        || keys.last() != Some(&last)
    {
        return Err("slotted page ordering/bounds mismatch");
    }
    Ok(NodeSummary {
        first_key: first,
        last_key: last,
        rows,
    })
}

fn verify_mphf(raw: &[u8]) -> Result<NodeSummary, &'static str> {
    let mut cursor = Cursor::new(raw);
    cursor.expect_magic(b"LXMH")?;
    let (first, last) = read_page_header(&mut cursor)?;
    let rows = cursor.u32()?;
    if rows == 0 {
        return Err("empty MPHF page");
    }
    let prefix = cursor.bytes()?.to_vec();
    let mut directory = Vec::with_capacity(rows);
    for _ in 0..rows {
        directory.push((cursor.u32()?, cursor.u32()?, cursor.u32()?, cursor.u32()?));
    }
    let fingerprint_bytes = cursor.u8()? as usize;
    if !matches!(fingerprint_bytes, 1 | 2) {
        return Err("invalid MPHF fingerprint width");
    }
    let present = cursor.u8()?;
    if present > 1 {
        return Err("invalid MPHF presence tag");
    }
    let table = if present == 1 {
        let seed = cursor.u32()? as u32;
        let buckets = cursor.u32()?;
        if buckets != rows {
            return Err("MPHF bucket count mismatch");
        }
        let mut displacements = Vec::with_capacity(rows);
        let mut slot_ordinals = Vec::with_capacity(rows);
        let mut fingerprints = Vec::with_capacity(rows);
        for _ in 0..rows {
            displacements.push(cursor.u16()?);
        }
        for _ in 0..rows {
            slot_ordinals.push(cursor.u16()?);
        }
        for _ in 0..rows {
            fingerprints.push(if fingerprint_bytes == 1 {
                cursor.u8()? as u16
            } else {
                cursor.u16()?
            });
        }
        Some(MphfTable {
            seed,
            displacements,
            slot_ordinals,
            fingerprints,
        })
    } else {
        None
    };
    let suffix_total = directory
        .iter()
        .map(|(offset, len, _, _)| offset.checked_add(*len).ok_or("suffix overflow"))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(0);
    let suffixes = cursor.take(suffix_total)?;
    let values = cursor.remaining();
    let mut keys = Vec::with_capacity(rows);
    let mut expected_suffix = 0;
    let mut expected_value = 0;
    for (suffix_offset, suffix_len, value_offset, value_len) in &directory {
        if *suffix_offset != expected_suffix || *value_offset != expected_value {
            return Err("non-canonical MPHF slot offsets");
        }
        let end = suffix_offset
            .checked_add(*suffix_len)
            .ok_or("suffix overflow")?;
        let suffix = suffixes
            .get(*suffix_offset..end)
            .ok_or("suffix out of bounds")?;
        let mut key = prefix.clone();
        key.extend_from_slice(suffix);
        keys.push(key);
        expected_suffix = end;
        let value_end = value_offset
            .checked_add(*value_len)
            .ok_or("value overflow")?;
        values
            .get(*value_offset..value_end)
            .ok_or("slot value out of bounds")?;
        expected_value = value_end;
    }
    if expected_value != values.len()
        || keys.windows(2).any(|pair| pair[0] >= pair[1])
        || keys.first() != Some(&first)
        || keys.last() != Some(&last)
    {
        return Err("MPHF key ordering/bounds mismatch");
    }
    let canonical = build_mphf_for_keys(&keys, fingerprint_bytes)
        .filter(|candidate| mphf_encoded_len(candidate, fingerprint_bytes) < suffix_total);
    if table != canonical {
        return Err("non-canonical MPHF construction or byte-cost choice");
    }
    if let Some(table) = &table {
        for (ordinal, key) in keys.iter().enumerate() {
            let (bucket, base, fingerprint) = mphf_hash(key, table.seed, rows);
            let slot = (base + usize::from(table.displacements[bucket])) % rows;
            if usize::from(table.slot_ordinals[slot]) != ordinal {
                return Err("MPHF slot proposes wrong key ordinal");
            }
            let expected = if fingerprint_bytes == 1 {
                fingerprint & 0xff
            } else {
                fingerprint
            };
            if table.fingerprints[slot] != expected || keys[ordinal] != *key {
                return Err("MPHF fingerprint/key binding mismatch");
            }
        }
    }
    Ok(NodeSummary {
        first_key: first,
        last_key: last,
        rows,
    })
}

fn build_mphf_for_keys(keys: &[Vec<u8>], fingerprint_bytes: usize) -> Option<MphfTable> {
    let rows = keys
        .iter()
        .map(|key| TypedRow {
            key: key.clone(),
            canonical: Vec::new(),
            fixed_len: 0,
        })
        .collect::<Vec<_>>();
    build_mphf(&rows, fingerprint_bytes)
}

fn verify_internal(
    store: &BTreeMap<[u8; 32], Bytes>,
    raw: &[u8],
    visited: &mut BTreeSet<[u8; 32]>,
    visiting: &mut BTreeSet<[u8; 32]>,
) -> Result<NodeSummary, &'static str> {
    let mut cursor = Cursor::new(raw);
    cursor.expect_magic(b"LXCI")?;
    let (first, last) = read_page_header(&mut cursor)?;
    let children = cursor.u32()?;
    if children == 0 {
        return Err("empty internal page");
    }
    let prefix = cursor.bytes()?.to_vec();
    let mut entries = Vec::with_capacity(children);
    for _ in 0..children {
        let child_id = cursor.object_id()?;
        let suffix = cursor.bytes()?;
        let mut separator = prefix.clone();
        separator.extend_from_slice(suffix);
        entries.push((child_id, separator, cursor.u32()?));
    }
    cursor.finish()?;
    if entries.windows(2).any(|pair| pair[0].1 >= pair[1].1) {
        return Err("internal separator order mismatch");
    }
    let mut summaries = Vec::with_capacity(children);
    for (child_id, separator, rows) in entries {
        let child = verify_node(store, child_id, visited, visiting)?;
        if child.last_key != separator || child.rows != rows {
            return Err("authenticated parent edge mismatch");
        }
        summaries.push(child);
    }
    if summaries
        .windows(2)
        .any(|pair| pair[0].last_key >= pair[1].first_key)
        || summaries.first().map(|child| &child.first_key) != Some(&first)
        || summaries.last().map(|child| &child.last_key) != Some(&last)
    {
        return Err("authenticated child range mismatch");
    }
    Ok(NodeSummary {
        first_key: first,
        last_key: last,
        rows: summaries.iter().map(|child| child.rows).sum(),
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], &'static str> {
        let end = self.offset.checked_add(len).ok_or("cursor overflow")?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or("truncated authenticated object")?;
        self.offset = end;
        Ok(value)
    }

    fn expect_magic(&mut self, magic: &[u8; 4]) -> Result<(), &'static str> {
        if self.take(4)? != magic {
            return Err("wrong authenticated object domain");
        }
        Ok(())
    }

    fn expect_fingerprint(&mut self) -> Result<(), &'static str> {
        if self.take(SCHEMA_FINGERPRINT.len())? != SCHEMA_FINGERPRINT {
            return Err("schema/layout fingerprint mismatch");
        }
        Ok(())
    }

    fn u32(&mut self) -> Result<usize, &'static str> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().map_err(|_| "truncated u32")?) as usize)
    }

    fn u16(&mut self) -> Result<u16, &'static str> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().map_err(|_| "truncated u16")?,
        ))
    }

    fn u8(&mut self) -> Result<u8, &'static str> {
        Ok(self.take(1)?[0])
    }

    fn bytes(&mut self) -> Result<&'a [u8], &'static str> {
        let len = self.u32()?;
        self.take(len)
    }

    fn object_id(&mut self) -> Result<[u8; 32], &'static str> {
        self.take(32)?.try_into().map_err(|_| "truncated object id")
    }

    fn remaining(&mut self) -> &'a [u8] {
        let remaining = &self.bytes[self.offset..];
        self.offset = self.bytes.len();
        remaining
    }

    fn finish(self) -> Result<(), &'static str> {
        if self.offset != self.bytes.len() {
            return Err("trailing authenticated object bytes");
        }
        Ok(())
    }
}

fn authenticate_metric(tree: &Tree, metric: &ReadMetric) -> Duration {
    let started = Instant::now();
    for id in &metric.ids {
        let bytes = &tree.objects[id];
        black_box(blake3::hash(bytes));
        black_box(decode_envelope(bytes).ok());
    }
    started.elapsed()
}

fn point_lookup_cpu(tree: &Tree) -> Duration {
    let key = &tree.rows[tree.rows.len() / 2].key;
    let mut missing = key.clone();
    missing.push(0);
    let leaf = tree.levels[0]
        .iter()
        .find(|leaf| {
            leaf.first_key.as_slice() <= key.as_slice()
                && key.as_slice() <= leaf.last_key.as_slice()
        })
        .expect("point leaf");
    let started = Instant::now();
    for ordinal in 0..256 {
        black_box(blake3::hash(&leaf.bytes));
        let raw = decode_envelope(&leaf.bytes).expect("point page envelope");
        let query = if ordinal % 2 == 0 {
            key.as_slice()
        } else {
            missing.as_slice()
        };
        let hit = match tree.geometry {
            Geometry::C2 => c2_point_lookup(&raw, query),
            Geometry::Mphf8 | Geometry::Mphf16 => mphf_point_lookup(&raw, query),
        }
        .expect("authenticated point lookup");
        black_box(hit);
    }
    started.elapsed()
}

fn c2_point_lookup(raw: &[u8], key: &[u8]) -> Result<bool, &'static str> {
    let mut cursor = Cursor::new(raw);
    cursor.expect_magic(b"LXC2")?;
    read_page_header(&mut cursor)?;
    let rows = cursor.u32()?;
    let prefix = cursor.bytes()?.to_vec();
    let mut directory = Vec::with_capacity(rows);
    let mut suffix_total = 0;
    for _ in 0..rows {
        let suffix_offset = cursor.u32()?;
        let suffix_len = cursor.u32()?;
        cursor.u32()?;
        cursor.u32()?;
        suffix_total = suffix_total.max(suffix_offset.checked_add(suffix_len).ok_or("suffix")?);
        directory.push((suffix_offset, suffix_len));
    }
    let suffixes = cursor.take(suffix_total)?;
    sorted_directory_lookup(&prefix, &directory, suffixes, key)
}

fn mphf_point_lookup(raw: &[u8], key: &[u8]) -> Result<bool, &'static str> {
    let mut cursor = Cursor::new(raw);
    cursor.expect_magic(b"LXMH")?;
    read_page_header(&mut cursor)?;
    let rows = cursor.u32()?;
    let prefix = cursor.bytes()?.to_vec();
    let mut directory = Vec::with_capacity(rows);
    let mut suffix_total = 0;
    for _ in 0..rows {
        let suffix_offset = cursor.u32()?;
        let suffix_len = cursor.u32()?;
        cursor.u32()?;
        cursor.u32()?;
        suffix_total = suffix_total.max(suffix_offset.checked_add(suffix_len).ok_or("suffix")?);
        directory.push((suffix_offset, suffix_len));
    }
    let fingerprint_bytes = cursor.u8()? as usize;
    let present = cursor.u8()?;
    if present == 0 {
        let suffixes = cursor.take(suffix_total)?;
        return sorted_directory_lookup(&prefix, &directory, suffixes, key);
    }
    if present != 1 || !matches!(fingerprint_bytes, 1 | 2) {
        return Err("malformed MPHF point table");
    }
    let seed = cursor.u32()? as u32;
    if cursor.u32()? != rows {
        return Err("MPHF point bucket count");
    }
    let displacements = (0..rows)
        .map(|_| cursor.u16())
        .collect::<Result<Vec<_>, _>>()?;
    let ordinals = (0..rows)
        .map(|_| cursor.u16())
        .collect::<Result<Vec<_>, _>>()?;
    let fingerprints = (0..rows)
        .map(|_| {
            if fingerprint_bytes == 1 {
                cursor.u8().map(u16::from)
            } else {
                cursor.u16()
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let suffixes = cursor.take(suffix_total)?;
    let (bucket, base, fingerprint) = mphf_hash(key, seed, rows);
    let slot = (base + usize::from(displacements[bucket])) % rows;
    let expected = if fingerprint_bytes == 1 {
        fingerprint & 0xff
    } else {
        fingerprint
    };
    if fingerprints[slot] != expected {
        return Ok(false);
    }
    let ordinal = usize::from(ordinals[slot]);
    let Some(&(offset, len)) = directory.get(ordinal) else {
        return Err("MPHF point ordinal out of bounds");
    };
    let suffix = suffixes
        .get(offset..offset.checked_add(len).ok_or("suffix")?)
        .ok_or("MPHF point suffix out of bounds")?;
    Ok(key == [prefix.as_slice(), suffix].concat())
}

fn sorted_directory_lookup(
    prefix: &[u8],
    directory: &[(usize, usize)],
    suffixes: &[u8],
    key: &[u8],
) -> Result<bool, &'static str> {
    let mut low = 0;
    let mut high = directory.len();
    while low < high {
        let middle = (low + high) / 2;
        let (offset, len) = directory[middle];
        let suffix = suffixes
            .get(offset..offset.checked_add(len).ok_or("suffix")?)
            .ok_or("sorted point suffix out of bounds")?;
        let candidate = [prefix, suffix].concat();
        match candidate.as_slice().cmp(key) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(true),
        }
    }
    Ok(false)
}

fn fixture_rows(n: usize, width: usize) -> Vec<TypedRow> {
    let shape = if width >= 1_024 {
        Shape::Wide
    } else {
        Shape::Narrow
    };
    let mut rows = fixture_rows_for_schema(n, PkKind::Text, shape, JsonDensity::Absent);
    for row in &mut rows {
        row.canonical.resize(width, b'x');
        row.fixed_len = row.fixed_len.min(width);
    }
    rows
}

fn fixture_rows_for_schema(
    n: usize,
    pk_kind: PkKind,
    shape: Shape,
    json_density: JsonDensity,
) -> Vec<TypedRow> {
    const TEXT: &[u8] = b"project-status-open-owner-agent-native-scalar-description-priority";
    let mut rows = (0..n)
        .map(|ordinal| {
            let key = match pk_kind {
                PkKind::Integer => (ordinal as u64).to_be_bytes().to_vec(),
                PkKind::Uuid => {
                    let mut bytes = [0_u8; 16];
                    bytes[..8].copy_from_slice(&(ordinal as u64).to_be_bytes());
                    bytes[8..]
                        .copy_from_slice(&((ordinal as u64) ^ 0xa5a5_5a5a_33cc_cc33).to_be_bytes());
                    bytes.to_vec()
                }
                PkKind::Text => format!("task/{ordinal:012}").into_bytes(),
                PkKind::Composite => {
                    let mut key = format!("tenant/{:04}/", ordinal % 97).into_bytes();
                    key.extend_from_slice(&(ordinal as u64).to_be_bytes());
                    key
                }
            };
            // Canonical Schema-v1 tuple bytes. C2 stores this byte string
            // verbatim; Mphf8 only changes its representation inside
            // the same authenticated page.
            let mut canonical = Vec::new();
            canonical.extend_from_slice(b"SVT1");
            canonical.extend_from_slice(&[0, 0]); // canonical null bitmap
            canonical.extend_from_slice(&(ordinal as i64).to_be_bytes());
            canonical.extend_from_slice(&((ordinal as f64) * 0.25).to_bits().to_be_bytes());
            canonical.push(u8::from(ordinal % 2 == 0));
            canonical.extend_from_slice(&(1_700_000_000_000_i64 + ordinal as i64).to_be_bytes());
            canonical.extend_from_slice(blake3::hash(&key).as_bytes().get(..16).expect("uuid"));
            let fixed_len = canonical.len();
            let mut state = (ordinal as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let text_columns = match shape {
                Shape::Narrow => 1,
                Shape::Wide => 12,
            };
            for column in 0..text_columns {
                let len = match shape {
                    Shape::Narrow => 24,
                    Shape::Wide => 96 + column * 7,
                };
                let mut value = Vec::with_capacity(len);
                for offset in 0..len {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    value.push(if offset % 17 == 0 {
                        (state >> 24) as u8
                    } else {
                        TEXT[(offset + ordinal * 7 + column) % TEXT.len()]
                    });
                }
                push_bytes(&mut canonical, &value);
            }
            let json_present = match json_density {
                JsonDensity::Absent => false,
                JsonDensity::Sparse => ordinal % 20 == 0,
                JsonDensity::Dense => true,
            };
            if json_present {
                let json = format!(
                    "{{\"ordinal\":{ordinal},\"status\":\"{}\",\"nested\":{{\"active\":{}}}}}",
                    if ordinal % 2 == 0 { "open" } else { "closed" },
                    ordinal % 3 == 0
                );
                push_bytes(&mut canonical, json.as_bytes());
            } else {
                push_u32(&mut canonical, 0);
            }
            TypedRow {
                key,
                canonical,
                fixed_len,
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.key.cmp(&right.key));
    rows
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

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    push_u32(out, bytes.len());
    out.extend_from_slice(bytes);
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
    let (main_store, main_ref) = tree.authenticated_store_for_branch(b"main");
    let (branch_store, branch_ref) = tree.authenticated_store_for_branch(b"feature");
    let main = verify_branch(&main_store, main_ref).expect("main branch authenticates");
    let branch = verify_branch(&branch_store, branch_ref).expect("copied branch authenticates");
    assert_eq!(main.root_id, branch.root_id);
    assert_eq!(main.visited, branch.visited);

    let mut modified_rows = tree.rows.clone();
    let changed = &mut modified_rows[tree.rows.len() / 2].canonical;
    let last = changed.len() - 1;
    changed[last] ^= 0x5a;
    let modified = Tree::build(modified_rows, tree.geometry, tree.target);
    let old_ids = tree.objects.keys().copied().collect::<BTreeSet<_>>();
    let changed_objects = modified
        .objects
        .keys()
        .filter(|id| !old_ids.contains(*id))
        .count();
    // C2 and Mphf8 both have exactly one leaf object authority, so a
    // single-row change rewrites only the leaf-to-root authenticated path.
    let expected_path_objects = tree.levels.len();
    assert_eq!(changed_objects, expected_path_objects);
    println!(
        "branch_share,-,{geometry},{target},{width},{n},0,{height},{leaves},{objects},0,{total},0,0,0,0,0,0,0,0,0,0,0,{changed_objects},0,0,0,0,0,0,0,0,-,{key_bytes},{payload_bytes}",
        geometry = tree.geometry.label(),
        target = tree.target,
        width = tree.rows[0].canonical.len(),
        n = tree.rows.len(),
        height = tree.levels.len(),
        leaves = tree.levels[0].len(),
        objects = tree.objects.len(),
        total = tree.objects.values().map(Bytes::len).sum::<usize>(),
        changed_objects = changed_objects,
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
    let (store, _) = tree.authenticated_store_for_branch(b"main");
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin model write");
    write
        .put_many(
            OBJECT_SPACE,
            PutBatch {
                entries: store
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
    let (expected, branch_ref_id) = tree.authenticated_store_for_branch(b"main");
    let keys = expected
        .keys()
        .map(|id| Key(Bytes::copy_from_slice(id)))
        .collect::<Vec<_>>();
    let result = read
        .get_many(&[GetManyRequest {
            space: OBJECT_SPACE,
            keys: &keys,
            opts: GetOptions::default(),
        }])
        .await
        .expect("read model closure");
    assert_eq!(result.values.len(), keys.len());
    let mut reopened = BTreeMap::new();
    for (key, value) in keys.into_iter().zip(result.values) {
        let value = value.as_ref().expect("model object exists");
        let lix::storage::ProjectedValue::FullValue(bytes) = value else {
            panic!("model object returned key-only projection")
        };
        reopened.insert(
            key.0.as_ref().try_into().expect("ObjectId key"),
            bytes.clone(),
        );
    }
    let verified = verify_branch(&reopened, branch_ref_id).expect("cold closure authenticates");
    assert_eq!(verified.branch_id, b"main");
    assert_eq!(verified.root_id, tree.root_id());
    assert_eq!(verified.rows, tree.rows.len());
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
    let tree = Tree::build(fixture_rows(10, 256), Geometry::C2, 64 << 10);
    tree.verify_all().expect("control tree authenticates");
    let (store, branch_ref_id) = tree.authenticated_store_for_branch(b"main");
    let root_id = tree.root_id();
    let root = store.get(&root_id).expect("root object");

    let mut envelope = store.clone();
    let mut bytes = root.to_vec();
    bytes[4] = 0xff;
    envelope.insert(root_id, Bytes::from(bytes));
    assert!(verify_branch(&envelope, branch_ref_id).is_err());

    let raw = decode_envelope(root).expect("root envelope");
    for mutation in ["domain", "fingerprint", "bounds", "directory"] {
        let mut changed = raw.clone();
        match mutation {
            "domain" => changed[0] ^= 1,
            "fingerprint" => changed[4] ^= 1,
            "bounds" => changed[24] ^= 1,
            "directory" => {
                let directory = c2_directory_offset(&changed).expect("directory offset");
                changed[directory + 3] = 1;
            }
            _ => unreachable!(),
        }
        assert_rehashed_root_rejected(&tree, changed);
    }

    let mut payload = store.clone();
    let mut bytes = root.to_vec();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    payload.insert(root_id, Bytes::from(bytes));
    assert!(verify_branch(&payload, branch_ref_id).is_err());

    let mut root_link = store;
    let branch = root_link.remove(&branch_ref_id).expect("branch ref");
    let mut branch_raw = decode_envelope(&branch).expect("branch envelope");
    let last = branch_raw.len() - 1;
    branch_raw[last] ^= 1;
    let branch = encode_envelope(branch_raw);
    let bad_branch_id = *blake3::hash(&branch).as_bytes();
    root_link.insert(bad_branch_id, branch);
    assert!(verify_branch(&root_link, bad_branch_id).is_err());
    println!(
        "control,corruption,c2_slotted,65536,256,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,rejected,0,0"
    );

    let codec = Tree::build(
        fixture_rows_for_schema(32, PkKind::Composite, Shape::Wide, JsonDensity::Dense),
        Geometry::Mphf8,
        64 << 10,
    );
    codec.verify_all().expect("MPHF control authenticates");
    let (store, branch_ref_id) = codec.authenticated_store_for_branch(b"main");
    let root = codec.levels[0].first().expect("MPHF root leaf");
    assert_eq!(codec.root_id(), root.id);
    let raw = decode_envelope(&root.bytes).expect("MPHF page");
    let offsets = mphf_offsets(&raw).expect("MPHF offsets");
    assert_eq!(offsets.rows, 32);
    assert!(offsets.present);

    let mut malformed_offset = raw.clone();
    malformed_offset[offsets.directory + 3] ^= 1;
    assert_rehashed_root_rejected(&codec, malformed_offset);

    for offset in [
        offsets.width,
        offsets.seed,
        offsets.bucket_count,
        offsets.displacements,
        offsets.slot_ordinals,
        offsets.fingerprints,
    ] {
        let mut malformed = raw.clone();
        malformed[offset] ^= 1;
        assert_rehashed_root_rejected(&codec, malformed);
    }

    let mut duplicate_rows = codec.rows.clone();
    duplicate_rows[1].key = duplicate_rows[0].key.clone();
    duplicate_rows.sort_by(|left, right| left.key.cmp(&right.key));
    let duplicate = Tree::build(duplicate_rows, Geometry::Mphf8, 64 << 10);
    assert!(duplicate.verify_all().is_err());

    let mut truncated = store.clone();
    let mut truncated_page = root.bytes.to_vec();
    truncated_page.pop();
    truncated.insert(root.id, Bytes::from(truncated_page));
    assert!(verify_branch(&truncated, branch_ref_id).is_err());

    let mut bomb = root.bytes.to_vec();
    bomb[5..9].copy_from_slice(&((MAX_DECODED_OBJECT_BYTES + 1) as u32).to_be_bytes());
    let bomb_id = *blake3::hash(&bomb).as_bytes();
    let mut bomb_store = store.clone();
    bomb_store.remove(&root.id);
    bomb_store.insert(bomb_id, Bytes::from(bomb));
    assert!(
        verify_node(
            &bomb_store,
            bomb_id,
            &mut BTreeSet::new(),
            &mut BTreeSet::new()
        )
        .is_err()
    );

    let mut shuffled = codec.rows.clone();
    shuffled.reverse();
    shuffled.sort_by(|left, right| left.key.cmp(&right.key));
    assert_eq!(
        encode_mphf(&codec.rows, 1).id,
        encode_mphf(&shuffled, 1).id,
        "insertion order must not affect canonical page bytes after key ordering"
    );
    println!(
        "control,corruption,mphf8_chd,65536,0,32,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,rejected,0,0"
    );
}

struct MphfOffsets {
    directory: usize,
    width: usize,
    present: bool,
    seed: usize,
    bucket_count: usize,
    displacements: usize,
    slot_ordinals: usize,
    fingerprints: usize,
    rows: usize,
}

fn mphf_offsets(raw: &[u8]) -> Result<MphfOffsets, &'static str> {
    let mut cursor = Cursor::new(raw);
    cursor.expect_magic(b"LXMH")?;
    read_page_header(&mut cursor)?;
    let rows = cursor.u32()?;
    cursor.bytes()?;
    let directory = cursor.offset;
    for _ in 0..rows {
        cursor.u32()?;
        cursor.u32()?;
        cursor.u32()?;
        cursor.u32()?;
    }
    let width = cursor.offset;
    let fingerprint_bytes = cursor.u8()? as usize;
    let present_offset = cursor.offset;
    let present = cursor.u8()? == 1;
    if !present {
        return Err("control fixture must select canonical MPHF");
    }
    let seed = cursor.offset;
    cursor.u32()?;
    let bucket_count = cursor.offset;
    cursor.u32()?;
    let displacements = cursor.offset;
    cursor.take(rows * 2)?;
    let slot_ordinals = cursor.offset;
    cursor.take(rows * 2)?;
    let fingerprints = cursor.offset;
    cursor.take(rows * fingerprint_bytes)?;
    let _ = present_offset;
    Ok(MphfOffsets {
        directory,
        width,
        present,
        seed,
        bucket_count,
        displacements,
        slot_ordinals,
        fingerprints,
        rows,
    })
}

fn c2_directory_offset(raw: &[u8]) -> Result<usize, &'static str> {
    let mut cursor = Cursor::new(raw);
    cursor.expect_magic(b"LXC2")?;
    read_page_header(&mut cursor)?;
    cursor.u32()?;
    cursor.bytes()?;
    Ok(cursor.offset)
}

fn assert_rehashed_root_rejected(tree: &Tree, changed_raw: Vec<u8>) {
    let mut store = tree.objects.clone();
    store.remove(&tree.root_id());
    let changed = encode_envelope(changed_raw);
    let changed_id = *blake3::hash(&changed).as_bytes();
    store.insert(changed_id, changed);
    let branch = encode_branch_ref(b"main", changed_id);
    let branch_id = *blake3::hash(&branch).as_bytes();
    store.insert(branch_id, branch);
    assert!(verify_branch(&store, branch_id).is_err());
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
    let inserted_shared = assert_local_page_interval(&base.levels[0], &inserted.levels[0]);
    let deleted_shared = assert_local_page_interval(&base.levels[0], &deleted.levels[0]);
    println!(
        "boundary_stability,-,c2_slotted,{TARGET},256,10000,0,0,{base_pages},0,0,0,0,0,0,0,0,0,0,0,0,0,0,{inserted_shared},{deleted_shared},0,0,0,0,0,0,0,stable,0,0",
        base_pages = base.levels[0].len(),
    );
}

fn assert_local_page_interval(before: &[Object], after: &[Object]) -> usize {
    let prefix = before
        .iter()
        .zip(after)
        .take_while(|(left, right)| left.id == right.id)
        .count();
    let max_suffix = before.len().min(after.len()).saturating_sub(prefix);
    let suffix = before
        .iter()
        .rev()
        .zip(after.iter().rev())
        .take(max_suffix)
        .take_while(|(left, right)| left.id == right.id)
        .count();
    let changed_before = before.len() - prefix - suffix;
    let changed_after = after.len() - prefix - suffix;
    assert!(changed_before <= 4 && changed_after <= 4);
    for (left, right) in before[..prefix].iter().zip(&after[..prefix]) {
        assert_eq!(left.id, right.id);
    }
    for (left, right) in before[before.len() - suffix..]
        .iter()
        .zip(&after[after.len() - suffix..])
    {
        assert_eq!(left.id, right.id);
    }
    prefix + suffix
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
