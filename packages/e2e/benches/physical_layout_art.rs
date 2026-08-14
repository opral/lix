//! EXP-ART-01: authenticated crit-bit versus schema-partitioned slotted pages.
//!
//! This benchmark is deliberately additive. Both geometries store exactly one
//! Schema-v1 non-PK tuple per logical row and use the same canonical typed PK
//! bytes. Every page/node, branch root, and parent edge is content-addressed and
//! authenticated. There is no compatibility reader, fallback, or second store.

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
    StorageSpace::immutable(SpaceId(0x00ff_00a1), "bench.physical_layout_art.object");
const SCHEMA_FINGERPRINT: [u8; 16] = *b"typed-row-v1\0\0\0\0";
const MAX_OBJECT_BYTES: usize = 256 << 10;
const CSV_VERSION: &str = "exp-art-01-v1";
const PAGE_BOUNDARY_DOMAIN: &[u8] = b"lix.forktree.slotted-page-boundary.v1\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Geometry {
    C2,
    CritBit,
}

impl Geometry {
    const fn label(self) -> &'static str {
        match self {
            Self::C2 => "c2_slotted",
            Self::CritBit => "crit_bit",
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::C2 => 2,
            Self::CritBit => 5,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PkKind {
    Integer,
    Uuid,
    Text,
    Composite,
}

impl PkKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Integer => "integer",
            Self::Uuid => "uuid",
            Self::Text => "text",
            Self::Composite => "composite",
        }
    }
}

#[derive(Clone)]
struct Row {
    key: Vec<u8>,
    tuple: Vec<u8>,
}

#[derive(Clone)]
struct Object {
    id: [u8; 32],
    bytes: Bytes,
    min: Vec<u8>,
    max: Vec<u8>,
    rows: usize,
    children: Vec<[u8; 32]>,
}

#[derive(Clone)]
struct Tree {
    geometry: Geometry,
    page_target: usize,
    rows: Vec<Row>,
    root: Object,
    objects: BTreeMap<[u8; 32], Bytes>,
}

#[derive(Default, Clone, Copy)]
struct IoMetric {
    calls: usize,
    objects: usize,
    bytes: usize,
}

#[derive(Default)]
struct OpMetric {
    wall: Duration,
    cpu_ns: u128,
    io: IoMetric,
    writes: usize,
    write_bytes: usize,
    shared: usize,
    diff_nodes: usize,
    settled_bytes: u64,
    rebuild_model: bool,
    digest: [u8; 32],
}

struct Config {
    ns: Vec<usize>,
    pks: Vec<PkKind>,
    geometries: Vec<Geometry>,
    backends: Vec<String>,
    tuple_width: usize,
    page_target: Option<usize>,
    repeats: usize,
}

fn main() {
    let config = Config::parse();
    print_header();
    for pk in config.pks {
        for &n in &config.ns {
            let rows = fixture_rows(n, config.tuple_width, pk);
            for &geometry in &config.geometries {
                let build_started = Instant::now();
                let cpu_started = cpu_time_ns();
                let page_target = config
                    .page_target
                    .unwrap_or_else(|| derived_page_target(&rows));
                let tree = Tree::build(rows.clone(), geometry, page_target);
                let build_metric = OpMetric {
                    wall: build_started.elapsed(),
                    cpu_ns: cpu_time_ns().saturating_sub(cpu_started),
                    digest: tree.logical_digest(),
                    writes: tree.objects.len(),
                    write_bytes: tree.object_bytes(),
                    ..OpMetric::default()
                };
                tree.verify().expect("fresh authenticated tree verifies");
                print_row(
                    "model",
                    "build",
                    pk,
                    n,
                    &tree,
                    0,
                    &build_metric,
                    false,
                    "ok",
                );
                for _ in 0..config.repeats {
                    run_operations(&tree, pk);
                }
                run_corruption_controls(&tree, pk);
                for backend in &config.backends {
                    run_backend(backend, &tree, pk);
                }
            }
        }
    }
}

impl Config {
    fn parse() -> Self {
        let mut n = env_or("LIX_ART_N", "1000");
        let mut pk = env_or("LIX_ART_PK", "integer");
        let mut geometry = env_or("LIX_ART_GEOMETRY", "c2,art");
        let mut backend = env_or("LIX_ART_BACKENDS", "model");
        let mut width = env_or("LIX_ART_TUPLE_WIDTH", "256");
        let mut target = std::env::var("LIX_ART_PAGE_TARGET").ok();
        let mut repeats = env_or("LIX_ART_REPEATS", "1");
        for argument in std::env::args().skip(1) {
            let Some((name, value)) = argument.split_once('=') else {
                panic!("arguments must use --name=value: {argument}");
            };
            match name {
                "--n" => n = value.to_owned(),
                "--pk" => pk = value.to_owned(),
                "--geometry" => geometry = value.to_owned(),
                "--backends" => backend = value.to_owned(),
                "--tuple-width" => width = value.to_owned(),
                "--page-target" => target = Some(value.to_owned()),
                "--repeats" => repeats = value.to_owned(),
                _ => panic!("unknown argument {name}"),
            }
        }
        Self {
            ns: parse_ns(&n),
            pks: parse_pks(&pk),
            geometries: parse_geometries(&geometry),
            backends: parse_backends(&backend),
            tuple_width: width.parse().expect("tuple width is an integer"),
            page_target: target.map(|value| value.parse().expect("page target is an integer")),
            repeats: repeats.parse().expect("repeats is an integer"),
        }
    }
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn parse_ns(value: &str) -> Vec<usize> {
    let value = if value == "all" {
        "1000,10000,50000,100000"
    } else {
        value
    };
    value
        .split(',')
        .map(|part| part.parse().expect("N must be an integer"))
        .collect()
}

fn parse_pks(value: &str) -> Vec<PkKind> {
    let value = if value == "all" {
        "integer,uuid,text,composite"
    } else {
        value
    };
    value
        .split(',')
        .map(|part| match part {
            "integer" => PkKind::Integer,
            "uuid" => PkKind::Uuid,
            "text" => PkKind::Text,
            "composite" => PkKind::Composite,
            _ => panic!("unknown PK distribution {part}"),
        })
        .collect()
}

fn parse_geometries(value: &str) -> Vec<Geometry> {
    value
        .split(',')
        .map(|part| match part {
            "c2" => Geometry::C2,
            "art" | "critbit" => Geometry::CritBit,
            _ => panic!("unknown geometry {part}"),
        })
        .collect()
}

fn parse_backends(value: &str) -> Vec<String> {
    if value == "model" || value == "none" {
        return Vec::new();
    }
    let value = if value == "all" {
        "rocksdb,slatedb"
    } else {
        value
    };
    value
        .split(',')
        .map(|part| match part {
            "rocksdb" | "slatedb" => part.to_owned(),
            _ => panic!("unknown backend {part}"),
        })
        .collect()
}

impl Tree {
    fn build(rows: Vec<Row>, geometry: Geometry, page_target: usize) -> Self {
        assert!(!rows.is_empty());
        assert!(rows.windows(2).all(|pair| pair[0].key < pair[1].key));
        let mut objects = BTreeMap::new();
        let root = match geometry {
            Geometry::C2 => build_c2(&rows, page_target, &mut objects),
            Geometry::CritBit => build_critbit(&rows, &mut objects),
        };
        Self {
            geometry,
            page_target,
            rows,
            root,
            objects,
        }
    }

    fn branch_store(&self, branch: &[u8]) -> (BTreeMap<[u8; 32], Bytes>, [u8; 32]) {
        let mut store = self.objects.clone();
        let branch_bytes = encode_branch(branch, self.geometry, &self.root);
        let branch_id = object_id(&branch_bytes);
        store.insert(branch_id, branch_bytes);
        (store, branch_id)
    }

    fn verify(&self) -> Result<(), String> {
        let (store, branch_id) = self.branch_store(b"main");
        let verified = verify_branch(&store, branch_id)?;
        if verified.geometry != self.geometry
            || verified.root != self.root.id
            || verified.rows != self.rows.len()
            || verified.visited != self.objects.keys().copied().collect()
        {
            return Err("root/closure summary mismatch".into());
        }
        Ok(())
    }

    fn object_bytes(&self) -> usize {
        self.objects.values().map(Bytes::len).sum()
    }

    fn logical_digest(&self) -> [u8; 32] {
        digest_rows(self.rows.iter())
    }

    fn point(&self, key: &[u8]) -> Result<(Option<Row>, IoMetric), String> {
        match self.geometry {
            Geometry::C2 => self.point_c2(key),
            Geometry::CritBit => self.point_critbit(key),
        }
    }

    fn point_c2(&self, key: &[u8]) -> Result<(Option<Row>, IoMetric), String> {
        let mut current = self.root.id;
        let mut io = IoMetric::default();
        loop {
            let raw = self.load(current, &mut io)?;
            match raw.get(..4) {
                Some(b"LXCI") => {
                    let node = parse_c2_internal(&raw)?;
                    let index = node
                        .edges
                        .iter()
                        .position(|edge| edge.max.as_slice() >= key)
                        .unwrap_or(node.edges.len() - 1);
                    current = node.edges[index].id;
                }
                Some(b"LXC2") => {
                    let rows = parse_c2_leaf(&raw)?;
                    return Ok((rows.into_iter().find(|row| row.key == key), io));
                }
                _ => return Err("wrong C2 object domain".into()),
            }
        }
    }

    fn point_critbit(&self, key: &[u8]) -> Result<(Option<Row>, IoMetric), String> {
        let mut current = self.root.id;
        let mut io = IoMetric::default();
        loop {
            let raw = self.load(current, &mut io)?;
            match raw.get(..4) {
                Some(b"CBIN") => {
                    let node = parse_crit_internal(&raw)?;
                    current = if bit_at(key, node.bit) {
                        node.right
                    } else {
                        node.left
                    };
                }
                Some(b"CBLF") => {
                    let row = parse_crit_leaf(&raw)?;
                    return Ok(((row.key.as_slice() == key).then_some(row), io));
                }
                _ => return Err("wrong crit-bit object domain".into()),
            }
        }
    }

    fn load(&self, id: [u8; 32], io: &mut IoMetric) -> Result<Vec<u8>, String> {
        io.calls += 1;
        io.objects += 1;
        let bytes = authenticated_object(&self.objects, id)?;
        io.bytes += bytes.len();
        let raw = decode_envelope(bytes)?;
        require_domain(bytes, domain_for_raw(&raw)?)?;
        Ok(raw)
    }

    fn range(&self, start: &[u8], count: usize) -> Result<(Vec<Row>, IoMetric), String> {
        let mut rows = Vec::with_capacity(count.min(self.rows.len()));
        let mut io = IoMetric::default();
        collect_range(
            &self.objects,
            self.root.id,
            start,
            count,
            &mut rows,
            &mut io,
        )?;
        Ok((rows, io))
    }

    fn scan(&self) -> Result<(Vec<Row>, IoMetric), String> {
        self.range(&[], usize::MAX)
    }

    fn mutate(&self, kind: &str, amount: usize, pk: PkKind) -> (Self, OpMetric) {
        let mut rows = self.rows.clone();
        let mut updates = BTreeMap::new();
        let mut inserted = None;
        match kind {
            "insert" => {
                let mut row = rows[rows.len() / 2].clone();
                row.key = absent_key(pk, &rows, rows.len().saturating_mul(2).saturating_add(1));
                row.tuple[0] ^= 0xa5;
                inserted = Some(row.clone());
                rows.push(row);
                rows.sort_by(|left, right| left.key.cmp(&right.key));
            }
            "update_one" => {
                let middle = rows.len() / 2;
                rows[middle].tuple[0] ^= 0x5a;
                updates.insert(rows[middle].key.clone(), rows[middle].tuple.clone());
            }
            "mutate_1pct" => {
                for index in deterministic_indices(rows.len(), amount) {
                    rows[index].tuple[0] ^= 0x5a;
                    updates.insert(rows[index].key.clone(), rows[index].tuple.clone());
                }
            }
            _ => panic!("unknown mutation {kind}"),
        }
        let (updated, mutation_io, rebuild_model, wall, cpu_ns) =
            if inserted.is_some() && self.geometry == Geometry::C2 {
                let started = Instant::now();
                let cpu_started = cpu_time_ns();
                let updated = Self::build(rows.clone(), self.geometry, self.page_target);
                (
                    updated,
                    IoMetric::default(),
                    true,
                    started.elapsed(),
                    cpu_time_ns().saturating_sub(cpu_started),
                )
            } else {
                let mut builder = MutationBuilder::new(self);
                let started = Instant::now();
                let cpu_started = cpu_time_ns();
                let root = if let Some(row) = inserted {
                    builder.insert(row).expect("persistent insertion succeeds")
                } else {
                    builder
                        .update(&updates)
                        .expect("persistent path update succeeds")
                };
                let wall = started.elapsed();
                let cpu_ns = cpu_time_ns().saturating_sub(cpu_started);
                let mutation_io = builder.io;
                (
                    builder.finish(rows.clone(), root),
                    mutation_io,
                    false,
                    wall,
                    cpu_ns,
                )
            };
        updated.verify().expect("mutated tree verifies");
        let canonical = Self::build(updated.rows.clone(), updated.geometry, updated.page_target);
        canonical
            .verify()
            .expect("canonical mutation oracle verifies");
        assert_eq!(
            updated.logical_digest(),
            canonical.logical_digest(),
            "persistent mutation changed the logical result"
        );
        assert_eq!(
            updated.root.id, canonical.root.id,
            "persistent mutation diverged from the canonical authenticated root"
        );
        let old = self.objects.keys().copied().collect::<BTreeSet<_>>();
        let writes = updated
            .objects
            .iter()
            .filter(|(id, _)| !old.contains(*id))
            .collect::<Vec<_>>();
        let shared = updated
            .objects
            .keys()
            .filter(|id| old.contains(*id))
            .count();
        let metric = OpMetric {
            wall,
            cpu_ns,
            io: mutation_io,
            rebuild_model,
            writes: writes.len(),
            write_bytes: writes.iter().map(|(_, bytes)| bytes.len()).sum(),
            shared,
            digest: updated.logical_digest(),
            ..OpMetric::default()
        };
        (updated, metric)
    }

    fn diff(&self, other: &Self) -> Result<OpMetric, String> {
        let started = Instant::now();
        let cpu_started = cpu_time_ns();
        let mut metric = OpMetric::default();
        let mut changed = Vec::new();
        diff_nodes(
            self,
            self.root.id,
            other,
            other.root.id,
            &mut metric,
            &mut changed,
        )?;
        changed.sort_by(|left, right| left.key.cmp(&right.key));
        metric.digest = digest_rows(changed.iter());
        metric.wall = started.elapsed();
        metric.cpu_ns = cpu_time_ns().saturating_sub(cpu_started);
        Ok(metric)
    }
}

struct MutationBuilder<'a> {
    base: &'a Tree,
    added_objects: BTreeMap<[u8; 32], Bytes>,
    io: IoMetric,
}

impl<'a> MutationBuilder<'a> {
    fn new(base: &'a Tree) -> Self {
        Self {
            base,
            added_objects: BTreeMap::new(),
            io: IoMetric::default(),
        }
    }

    fn object(&self, id: [u8; 32]) -> Result<Object, String> {
        decode_object(id, self.bytes(id)?)
    }

    fn bytes(&self, id: [u8; 32]) -> Result<&Bytes, String> {
        self.added_objects
            .get(&id)
            .or_else(|| self.base.objects.get(&id))
            .ok_or_else(|| "missing mutation object bytes".into())
    }

    fn load_raw(&mut self, id: [u8; 32]) -> Result<Vec<u8>, String> {
        let bytes = self.bytes(id)?.clone();
        self.io.calls += 1;
        self.io.objects += 1;
        self.io.bytes += bytes.len();
        if object_id(&bytes) != id {
            return Err("mutation ObjectId mismatch".into());
        }
        let raw = decode_envelope(&bytes)?;
        require_domain(&bytes, domain_for_raw(&raw)?)?;
        Ok(raw)
    }

    fn add(&mut self, object: Object) -> Object {
        self.added_objects.insert(object.id, object.bytes.clone());
        object
    }

    fn update(&mut self, updates: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<Object, String> {
        if updates.is_empty() {
            return Err("empty persistent update".into());
        }
        match self.base.geometry {
            Geometry::C2 => self.update_c2(self.base.root.id, updates),
            Geometry::CritBit => self.update_crit(self.base.root.id, updates),
        }
    }

    fn update_c2(
        &mut self,
        id: [u8; 32],
        updates: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<Object, String> {
        let raw = self.load_raw(id)?;
        match raw.get(..4) {
            Some(b"LXC2") => {
                let mut rows = parse_c2_leaf(&raw)?;
                let mut changed = false;
                for row in &mut rows {
                    if let Some(tuple) = updates.get(&row.key) {
                        row.tuple.clone_from(tuple);
                        changed = true;
                    }
                }
                if !changed {
                    return self.object(id);
                }
                Ok(self.add(encode_c2_leaf(&rows)))
            }
            Some(b"LXCI") => {
                let old = self.object(id)?;
                let mut changed = false;
                let mut children = Vec::with_capacity(old.children.len());
                for child_id in old.children {
                    let child = self.object(child_id)?;
                    if has_update_in_range(updates, &child.min, &child.max) {
                        let updated = self.update_c2(child_id, updates)?;
                        changed |= updated.id != child_id;
                        children.push(updated);
                    } else {
                        children.push(child);
                    }
                }
                if !changed {
                    return self.object(id);
                }
                Ok(self.add(encode_c2_internal(&children)))
            }
            _ => Err("wrong C2 mutation object".into()),
        }
    }

    fn update_crit(
        &mut self,
        id: [u8; 32],
        updates: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<Object, String> {
        let raw = self.load_raw(id)?;
        match raw.get(..4) {
            Some(b"CBLF") => {
                let mut row = parse_crit_leaf(&raw)?;
                let Some(tuple) = updates.get(&row.key) else {
                    return self.object(id);
                };
                row.tuple.clone_from(tuple);
                Ok(self.add(encode_crit_leaf(&row)))
            }
            Some(b"CBIN") => {
                let node = parse_crit_internal(&raw)?;
                let old_left = self.object(node.left)?;
                let old_right = self.object(node.right)?;
                let left = if has_update_in_range(updates, &old_left.min, &old_left.max) {
                    self.update_crit(node.left, updates)?
                } else {
                    old_left
                };
                let right = if has_update_in_range(updates, &old_right.min, &old_right.max) {
                    self.update_crit(node.right, updates)?
                } else {
                    old_right
                };
                if left.id == node.left && right.id == node.right {
                    return self.object(id);
                }
                Ok(self.add(encode_crit_internal(node.bit, &left, &right)))
            }
            _ => Err("wrong crit-bit mutation object".into()),
        }
    }

    fn insert(&mut self, row: Row) -> Result<Object, String> {
        if self.base.geometry != Geometry::CritBit {
            return Err("C2 insertion is a non-OLTP canonical rebuild".into());
        }
        self.insert_crit(self.base.root.id, row)
    }

    fn insert_crit(&mut self, id: [u8; 32], row: Row) -> Result<Object, String> {
        let raw = self.load_raw(id)?;
        let current = self.object(id)?;
        let new_bit = first_different_bit(&current.min, &row.key)
            .ok_or_else(|| "duplicate crit-bit insertion key".to_owned())?;
        match raw.get(..4) {
            Some(b"CBLF") => {
                let existing = parse_crit_leaf(&raw)?;
                if existing.key != current.min {
                    return Err("crit-bit leaf identity mismatch".into());
                }
                let old_leaf = current;
                let new_leaf = self.add(encode_crit_leaf(&row));
                Ok(self.add(if bit_at(&row.key, new_bit) {
                    encode_crit_internal(new_bit, &old_leaf, &new_leaf)
                } else {
                    encode_crit_internal(new_bit, &new_leaf, &old_leaf)
                }))
            }
            Some(b"CBIN") => {
                let node = parse_crit_internal(&raw)?;
                if new_bit < node.bit {
                    let new_leaf = self.add(encode_crit_leaf(&row));
                    return Ok(self.add(if bit_at(&row.key, new_bit) {
                        encode_crit_internal(new_bit, &current, &new_leaf)
                    } else {
                        encode_crit_internal(new_bit, &new_leaf, &current)
                    }));
                }
                let mut left = self.object(node.left)?;
                let mut right = self.object(node.right)?;
                if bit_at(&row.key, node.bit) {
                    right = self.insert_crit(node.right, row)?;
                } else {
                    left = self.insert_crit(node.left, row)?;
                }
                Ok(self.add(encode_crit_internal(node.bit, &left, &right)))
            }
            _ => Err("wrong crit-bit insertion object".into()),
        }
    }

    fn finish(self, rows: Vec<Row>, root: Object) -> Tree {
        let mut objects = BTreeMap::new();
        self.collect_closure(root.id, &mut objects)
            .expect("persistent closure exists");
        Tree {
            geometry: self.base.geometry,
            page_target: self.base.page_target,
            rows,
            root,
            objects,
        }
    }

    fn collect_closure(
        &self,
        id: [u8; 32],
        objects: &mut BTreeMap<[u8; 32], Bytes>,
    ) -> Result<(), String> {
        if objects.contains_key(&id) {
            return Ok(());
        }
        let object = self.object(id)?;
        let bytes = self.bytes(id)?.clone();
        for child in &object.children {
            self.collect_closure(*child, objects)?;
        }
        objects.insert(id, bytes);
        Ok(())
    }
}

fn has_update_in_range(updates: &BTreeMap<Vec<u8>, Vec<u8>>, min: &[u8], max: &[u8]) -> bool {
    updates.range(min.to_vec()..=max.to_vec()).next().is_some()
}

fn build_c2(rows: &[Row], target: usize, objects: &mut BTreeMap<[u8; 32], Bytes>) -> Object {
    let mut level = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let mut end = start + 1;
        let mut size = 32 + encoded_row_size(&rows[start]);
        while end < rows.len() {
            let next = encoded_row_size(&rows[end]);
            if size.saturating_add(next) > target {
                break;
            }
            if size >= target / 2 && content_boundary(&rows[end - 1].key, next, target) {
                break;
            }
            size = size.saturating_add(next);
            end += 1;
        }
        level.push(encode_c2_leaf(&rows[start..end]));
        start = end;
    }
    install(&level, objects);
    while level.len() > 1 {
        let mut next = Vec::new();
        let mut start = 0;
        while start < level.len() {
            let mut end = start + 1;
            let mut size = 64 + encoded_internal_entry_size(&level[start]);
            while end < level.len() {
                let entry = encoded_internal_entry_size(&level[end]);
                if size.saturating_add(entry) > target {
                    break;
                }
                if size >= target / 2 && content_boundary(&level[end - 1].max, entry, target) {
                    break;
                }
                size = size.saturating_add(entry);
                end += 1;
            }
            next.push(encode_c2_internal(&level[start..end]));
            start = end;
        }
        install(&next, objects);
        level = next;
    }
    level.pop().expect("C2 root")
}

fn encoded_row_size(row: &Row) -> usize {
    16 + row.key.len() + row.tuple.len()
}

fn encoded_internal_entry_size(child: &Object) -> usize {
    44 + child.max.len()
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

fn derived_page_target(rows: &[Row]) -> usize {
    let max_tuple = rows
        .iter()
        .map(|row| row.tuple.len())
        .max()
        .expect("non-empty fixture");
    let needed = max_tuple.saturating_mul(4).max(4 << 10);
    [4 << 10, 16 << 10, 64 << 10, 256 << 10]
        .into_iter()
        .find(|target| *target >= needed)
        .expect("canonical tuple exceeds the approved 256 KiB page class")
}

fn encode_c2_leaf(rows: &[Row]) -> Object {
    let prefix = common_prefix(rows.iter().map(|row| row.key.as_slice()));
    let mut raw = Vec::new();
    raw.extend_from_slice(b"LXC2");
    raw.extend_from_slice(&SCHEMA_FINGERPRINT);
    push_bytes(&mut raw, &rows[0].key);
    push_bytes(&mut raw, &rows.last().expect("leaf row").key);
    push_u32(&mut raw, rows.len());
    push_u32(&mut raw, prefix.len());
    raw.extend_from_slice(prefix);
    let mut suffixes = Vec::new();
    let mut values = Vec::new();
    for row in rows {
        push_u32(&mut raw, suffixes.len());
        push_u32(&mut raw, row.key.len() - prefix.len());
        push_u32(&mut raw, values.len());
        push_u32(&mut raw, row.tuple.len());
        suffixes.extend_from_slice(&row.key[prefix.len()..]);
        values.extend_from_slice(&row.tuple);
    }
    raw.extend_from_slice(&suffixes);
    raw.extend_from_slice(&values);
    make_object(
        2,
        raw,
        rows[0].key.clone(),
        rows.last().expect("leaf row").key.clone(),
        rows.len(),
        Vec::new(),
    )
}

fn encode_c2_internal(children: &[Object]) -> Object {
    let prefix = common_prefix(children.iter().map(|child| child.max.as_slice()));
    let mut raw = Vec::new();
    raw.extend_from_slice(b"LXCI");
    raw.extend_from_slice(&SCHEMA_FINGERPRINT);
    push_bytes(&mut raw, &children[0].min);
    push_bytes(&mut raw, &children.last().expect("internal child").max);
    push_u32(&mut raw, children.len());
    push_u32(&mut raw, prefix.len());
    raw.extend_from_slice(prefix);
    for child in children {
        raw.extend_from_slice(&child.id);
        push_u32(&mut raw, child.max.len() - prefix.len());
        raw.extend_from_slice(&child.max[prefix.len()..]);
        push_u32(&mut raw, child.rows);
    }
    make_object(
        3,
        raw,
        children[0].min.clone(),
        children.last().expect("internal child").max.clone(),
        children.iter().map(|child| child.rows).sum(),
        children.iter().map(|child| child.id).collect(),
    )
}

fn build_critbit(rows: &[Row], objects: &mut BTreeMap<[u8; 32], Bytes>) -> Object {
    if rows.len() == 1 {
        let object = encode_crit_leaf(&rows[0]);
        install(std::slice::from_ref(&object), objects);
        return object;
    }
    let bit = first_different_bit(&rows[0].key, &rows.last().expect("row").key)
        .expect("distinct canonical keys have a differing bit");
    let split = rows.partition_point(|row| !bit_at(&row.key, bit));
    assert!(split > 0 && split < rows.len());
    let left = build_critbit(&rows[..split], objects);
    let right = build_critbit(&rows[split..], objects);
    let object = encode_crit_internal(bit, &left, &right);
    install(std::slice::from_ref(&object), objects);
    object
}

fn encode_crit_leaf(row: &Row) -> Object {
    let mut raw = Vec::new();
    raw.extend_from_slice(b"CBLF");
    raw.extend_from_slice(&SCHEMA_FINGERPRINT);
    push_bytes(&mut raw, &row.key);
    push_bytes(&mut raw, &row.tuple);
    make_object(4, raw, row.key.clone(), row.key.clone(), 1, Vec::new())
}

fn encode_crit_internal(bit: usize, left: &Object, right: &Object) -> Object {
    let mut raw = Vec::new();
    raw.extend_from_slice(b"CBIN");
    raw.extend_from_slice(&SCHEMA_FINGERPRINT);
    push_u32(&mut raw, bit);
    raw.extend_from_slice(&left.id);
    raw.extend_from_slice(&right.id);
    push_bytes(&mut raw, &left.min);
    push_bytes(&mut raw, &left.max);
    push_bytes(&mut raw, &right.min);
    push_bytes(&mut raw, &right.max);
    push_u32(&mut raw, left.rows);
    push_u32(&mut raw, right.rows);
    make_object(
        5,
        raw,
        left.min.clone(),
        right.max.clone(),
        left.rows + right.rows,
        vec![left.id, right.id],
    )
}

fn make_object(
    domain: u8,
    raw: Vec<u8>,
    min: Vec<u8>,
    max: Vec<u8>,
    rows: usize,
    children: Vec<[u8; 32]>,
) -> Object {
    let bytes = encode_envelope(domain, raw);
    Object {
        id: object_id(&bytes),
        bytes,
        min,
        max,
        rows,
        children,
    }
}

fn install(values: &[Object], objects: &mut BTreeMap<[u8; 32], Bytes>) {
    for value in values {
        objects.insert(value.id, value.bytes.clone());
    }
}

fn encode_envelope(domain: u8, raw: Vec<u8>) -> Bytes {
    assert!(raw.len() <= MAX_OBJECT_BYTES);
    let compressed = zstd::bulk::compress(&raw, 1).expect("zstd encode");
    let (kind, payload) = if compressed.len() + 9 < raw.len() {
        (1_u8, compressed)
    } else {
        (0_u8, raw.clone())
    };
    let mut bytes = Vec::with_capacity(payload.len() + 10);
    bytes.extend_from_slice(b"LXAO");
    bytes.push(domain);
    bytes.push(kind);
    push_u32(&mut bytes, raw.len());
    bytes.extend_from_slice(&payload);
    Bytes::from(bytes)
}

fn decode_envelope(bytes: &[u8]) -> Result<Vec<u8>, String> {
    if bytes.len() < 10 || &bytes[..4] != b"LXAO" {
        return Err("bad authenticated envelope".into());
    }
    let len =
        u32::from_be_bytes(bytes[6..10].try_into().map_err(|_| "bad object length")?) as usize;
    if len > MAX_OBJECT_BYTES {
        return Err("authenticated object exceeds decoded bound".into());
    }
    let raw = match bytes[5] {
        0 => bytes[10..].to_vec(),
        1 => zstd::bulk::decompress(&bytes[10..], len).map_err(|_| "bad zstd payload")?,
        _ => return Err("bad compression tag".into()),
    };
    if raw.len() != len {
        return Err("authenticated object length mismatch".into());
    }
    Ok(raw)
}

fn require_domain(bytes: &[u8], expected: u8) -> Result<(), String> {
    if bytes.get(4).copied() != Some(expected) {
        return Err("wrong authenticated object domain".into());
    }
    Ok(())
}

fn domain_for_raw(raw: &[u8]) -> Result<u8, String> {
    match raw.get(..4) {
        Some(b"ARBR") => Ok(1),
        Some(b"LXC2") => Ok(2),
        Some(b"LXCI") => Ok(3),
        Some(b"CBLF") => Ok(4),
        Some(b"CBIN") => Ok(5),
        _ => Err("wrong authenticated object kind".into()),
    }
}

fn object_id(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

fn authenticated_object(store: &BTreeMap<[u8; 32], Bytes>, id: [u8; 32]) -> Result<&Bytes, String> {
    let bytes = store.get(&id).ok_or("missing authenticated object")?;
    if object_id(bytes) != id {
        return Err("authenticated ObjectId mismatch".into());
    }
    Ok(bytes)
}

fn encode_branch(branch: &[u8], geometry: Geometry, root: &Object) -> Bytes {
    let mut raw = Vec::new();
    raw.extend_from_slice(b"ARBR");
    raw.extend_from_slice(&SCHEMA_FINGERPRINT);
    push_bytes(&mut raw, branch);
    raw.push(geometry.tag());
    raw.extend_from_slice(&root.id);
    push_u32(&mut raw, root.rows);
    encode_envelope(1, raw)
}

struct VerifiedBranch {
    geometry: Geometry,
    root: [u8; 32],
    rows: usize,
    visited: BTreeSet<[u8; 32]>,
}

#[derive(Clone)]
struct Summary {
    min: Vec<u8>,
    max: Vec<u8>,
    rows: usize,
}

fn verify_branch(
    store: &BTreeMap<[u8; 32], Bytes>,
    branch_id: [u8; 32],
) -> Result<VerifiedBranch, String> {
    let branch = authenticated_object(store, branch_id)?;
    require_domain(branch, 1)?;
    let raw = decode_envelope(branch)?;
    let mut cursor = Cursor::new(&raw);
    cursor.magic(b"ARBR")?;
    cursor.fingerprint()?;
    if cursor.bytes()?.is_empty() {
        return Err("empty branch identity".into());
    }
    let geometry = match cursor.u8()? {
        2 => Geometry::C2,
        5 => Geometry::CritBit,
        _ => return Err("unknown authenticated geometry".into()),
    };
    let root = cursor.id()?;
    let rows = cursor.u32()?;
    cursor.finish()?;
    let mut visited = BTreeSet::new();
    let mut visiting = BTreeSet::new();
    let summary = verify_node(store, root, geometry, &mut visited, &mut visiting, None)?;
    if summary.rows != rows {
        return Err("branch row count mismatch".into());
    }
    Ok(VerifiedBranch {
        geometry,
        root,
        rows,
        visited,
    })
}

fn verify_node(
    store: &BTreeMap<[u8; 32], Bytes>,
    id: [u8; 32],
    geometry: Geometry,
    visited: &mut BTreeSet<[u8; 32]>,
    visiting: &mut BTreeSet<[u8; 32]>,
    parent_bit: Option<usize>,
) -> Result<Summary, String> {
    if !visiting.insert(id) {
        return Err("authenticated object cycle".into());
    }
    let bytes = authenticated_object(store, id)?;
    let raw = decode_envelope(bytes)?;
    require_domain(bytes, domain_for_raw(&raw)?)?;
    let summary = match (geometry, raw.get(..4)) {
        (Geometry::C2, Some(b"LXC2")) => summary_rows(&parse_c2_leaf(&raw)?),
        (Geometry::C2, Some(b"LXCI")) => {
            let node = parse_c2_internal(&raw)?;
            let mut children = Vec::with_capacity(node.edges.len());
            for edge in &node.edges {
                let child = verify_node(store, edge.id, geometry, visited, visiting, None)?;
                if child.max != edge.max || child.rows != edge.rows {
                    return Err("C2 authenticated parent edge mismatch".into());
                }
                children.push(child);
            }
            let summary = combine_summaries(&children)?;
            if summary.min != node.first || summary.max != node.last {
                return Err("C2 authenticated internal bounds mismatch".into());
            }
            summary
        }
        (Geometry::CritBit, Some(b"CBLF")) => summary_rows(&[parse_crit_leaf(&raw)?]),
        (Geometry::CritBit, Some(b"CBIN")) => {
            let node = parse_crit_internal(&raw)?;
            if first_different_bit(&node.min, &node.max) != Some(node.bit) {
                return Err("non-canonical crit-bit decision bit".into());
            }
            if parent_bit.is_some_and(|parent| node.bit <= parent) {
                return Err("non-canonical crit-bit ordering".into());
            }
            let left = verify_node(
                store,
                node.left,
                geometry,
                visited,
                visiting,
                Some(node.bit),
            )?;
            let right = verify_node(
                store,
                node.right,
                geometry,
                visited,
                visiting,
                Some(node.bit),
            )?;
            if left.min != node.min
                || right.max != node.max
                || left.max != node.left_max
                || right.min != node.right_min
                || left.rows != node.left_rows
                || right.rows != node.right_rows
                || bit_at(&left.max, node.bit)
                || !bit_at(&right.min, node.bit)
            {
                return Err("crit-bit authenticated edge mismatch".into());
            }
            combine_summaries(&[left, right])?
        }
        _ => return Err("wrong authenticated node domain".into()),
    };
    visiting.remove(&id);
    visited.insert(id);
    Ok(summary)
}

fn summary_rows(rows: &[Row]) -> Summary {
    Summary {
        min: rows[0].key.clone(),
        max: rows.last().expect("summary row").key.clone(),
        rows: rows.len(),
    }
}

fn combine_summaries(children: &[Summary]) -> Result<Summary, String> {
    if children.is_empty() || children.windows(2).any(|pair| pair[0].max >= pair[1].min) {
        return Err("authenticated child ordering mismatch".into());
    }
    Ok(Summary {
        min: children[0].min.clone(),
        max: children.last().expect("child").max.clone(),
        rows: children.iter().map(|child| child.rows).sum(),
    })
}

struct C2Edge {
    id: [u8; 32],
    max: Vec<u8>,
    rows: usize,
}

struct C2Internal {
    first: Vec<u8>,
    last: Vec<u8>,
    edges: Vec<C2Edge>,
}

fn parse_c2_leaf(raw: &[u8]) -> Result<Vec<Row>, String> {
    let mut cursor = Cursor::new(raw);
    cursor.magic(b"LXC2")?;
    cursor.fingerprint()?;
    let first = cursor.bytes()?.to_vec();
    let last = cursor.bytes()?.to_vec();
    let count = cursor.u32()?;
    if count == 0 {
        return Err("empty C2 leaf".into());
    }
    let prefix_len = cursor.u32()?;
    let prefix = cursor.take(prefix_len)?.to_vec();
    let mut directory = Vec::with_capacity(count);
    for _ in 0..count {
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
    let mut rows = Vec::with_capacity(count);
    let mut expected_suffix = 0;
    let mut expected_value = 0;
    for (suffix_offset, suffix_len, value_offset, value_len) in directory {
        if suffix_offset != expected_suffix || value_offset != expected_value {
            return Err("non-canonical slot offsets".into());
        }
        let suffix_end = suffix_offset
            .checked_add(suffix_len)
            .ok_or("suffix overflow")?;
        let value_end = value_offset
            .checked_add(value_len)
            .ok_or("value overflow")?;
        let mut key = prefix.clone();
        key.extend_from_slice(
            suffixes
                .get(suffix_offset..suffix_end)
                .ok_or("suffix out of bounds")?,
        );
        rows.push(Row {
            key,
            tuple: values
                .get(value_offset..value_end)
                .ok_or("tuple out of bounds")?
                .to_vec(),
        });
        expected_suffix = suffix_end;
        expected_value = value_end;
    }
    if expected_value != values.len()
        || rows.first().map(|row| &row.key) != Some(&first)
        || rows.last().map(|row| &row.key) != Some(&last)
    {
        return Err("C2 leaf bounds mismatch".into());
    }
    validate_rows(&rows)?;
    Ok(rows)
}

fn parse_c2_internal(raw: &[u8]) -> Result<C2Internal, String> {
    let mut cursor = Cursor::new(raw);
    cursor.magic(b"LXCI")?;
    cursor.fingerprint()?;
    let first = cursor.bytes()?.to_vec();
    let last = cursor.bytes()?.to_vec();
    let count = cursor.u32()?;
    if count == 0 {
        return Err("empty C2 internal node".into());
    }
    let prefix_len = cursor.u32()?;
    let prefix = cursor.take(prefix_len)?.to_vec();
    let mut edges = Vec::with_capacity(count);
    for _ in 0..count {
        let id = cursor.id()?;
        let suffix_len = cursor.u32()?;
        let mut max = prefix.clone();
        max.extend_from_slice(cursor.take(suffix_len)?);
        edges.push(C2Edge {
            id,
            max,
            rows: cursor.u32()?,
        });
    }
    cursor.finish()?;
    if first > last
        || edges.iter().any(|edge| edge.rows == 0)
        || edges.windows(2).any(|pair| pair[0].max >= pair[1].max)
        || edges.last().map(|edge| &edge.max) != Some(&last)
    {
        return Err("invalid C2 authenticated edges".into());
    }
    Ok(C2Internal { first, last, edges })
}

fn parse_crit_leaf(raw: &[u8]) -> Result<Row, String> {
    let mut cursor = Cursor::new(raw);
    cursor.magic(b"CBLF")?;
    cursor.fingerprint()?;
    let row = Row {
        key: cursor.bytes()?.to_vec(),
        tuple: cursor.bytes()?.to_vec(),
    };
    cursor.finish()?;
    if row.key.is_empty() || row.tuple.is_empty() {
        return Err("empty crit-bit key/tuple".into());
    }
    Ok(row)
}

struct CritInternal {
    bit: usize,
    left: [u8; 32],
    right: [u8; 32],
    min: Vec<u8>,
    left_max: Vec<u8>,
    right_min: Vec<u8>,
    max: Vec<u8>,
    left_rows: usize,
    right_rows: usize,
}

fn parse_crit_internal(raw: &[u8]) -> Result<CritInternal, String> {
    let mut cursor = Cursor::new(raw);
    cursor.magic(b"CBIN")?;
    cursor.fingerprint()?;
    let value = CritInternal {
        bit: cursor.u32()?,
        left: cursor.id()?,
        right: cursor.id()?,
        min: cursor.bytes()?.to_vec(),
        left_max: cursor.bytes()?.to_vec(),
        right_min: cursor.bytes()?.to_vec(),
        max: cursor.bytes()?.to_vec(),
        left_rows: cursor.u32()?,
        right_rows: cursor.u32()?,
    };
    cursor.finish()?;
    if value.min >= value.max || value.left_rows == 0 || value.right_rows == 0 {
        return Err("invalid crit-bit node summary".into());
    }
    Ok(value)
}

fn decode_object(id: [u8; 32], bytes: &Bytes) -> Result<Object, String> {
    if object_id(bytes) != id {
        return Err("authenticated ObjectId mismatch".into());
    }
    let raw = decode_envelope(bytes)?;
    require_domain(bytes, domain_for_raw(&raw)?)?;
    match raw.get(..4) {
        Some(b"LXC2") => {
            let rows = parse_c2_leaf(&raw)?;
            Ok(Object {
                id,
                bytes: bytes.clone(),
                min: rows[0].key.clone(),
                max: rows.last().expect("leaf row").key.clone(),
                rows: rows.len(),
                children: Vec::new(),
            })
        }
        Some(b"LXCI") => {
            let node = parse_c2_internal(&raw)?;
            Ok(Object {
                id,
                bytes: bytes.clone(),
                min: node.first,
                max: node.last,
                rows: node.edges.iter().map(|edge| edge.rows).sum(),
                children: node.edges.into_iter().map(|edge| edge.id).collect(),
            })
        }
        Some(b"CBLF") => {
            let row = parse_crit_leaf(&raw)?;
            Ok(Object {
                id,
                bytes: bytes.clone(),
                min: row.key.clone(),
                max: row.key,
                rows: 1,
                children: Vec::new(),
            })
        }
        Some(b"CBIN") => {
            let node = parse_crit_internal(&raw)?;
            Ok(Object {
                id,
                bytes: bytes.clone(),
                min: node.min,
                max: node.max,
                rows: node.left_rows.saturating_add(node.right_rows),
                children: vec![node.left, node.right],
            })
        }
        _ => Err("wrong authenticated object kind".into()),
    }
}

fn validate_rows(rows: &[Row]) -> Result<(), String> {
    if rows
        .iter()
        .any(|row| row.key.is_empty() || row.tuple.is_empty())
        || rows.windows(2).any(|pair| pair[0].key >= pair[1].key)
    {
        return Err("non-canonical leaf rows".into());
    }
    Ok(())
}

fn collect_range(
    store: &BTreeMap<[u8; 32], Bytes>,
    id: [u8; 32],
    start: &[u8],
    limit: usize,
    output: &mut Vec<Row>,
    io: &mut IoMetric,
) -> Result<(), String> {
    if output.len() >= limit {
        return Ok(());
    }
    io.calls += 1;
    io.objects += 1;
    let bytes = authenticated_object(store, id)?;
    io.bytes += bytes.len();
    let raw = decode_envelope(bytes)?;
    require_domain(bytes, domain_for_raw(&raw)?)?;
    match raw.get(..4) {
        Some(b"LXC2") => output.extend(
            parse_c2_leaf(&raw)?
                .into_iter()
                .filter(|row| row.key.as_slice() >= start)
                .take(limit - output.len()),
        ),
        Some(b"LXCI") => {
            for edge in parse_c2_internal(&raw)?.edges {
                if edge.max.as_slice() >= start {
                    collect_range(store, edge.id, start, limit, output, io)?;
                    if output.len() >= limit {
                        break;
                    }
                }
            }
        }
        Some(b"CBLF") => {
            let row = parse_crit_leaf(&raw)?;
            if row.key.as_slice() >= start {
                output.push(row);
            }
        }
        Some(b"CBIN") => {
            let node = parse_crit_internal(&raw)?;
            if node.left_max.as_slice() >= start {
                collect_range(store, node.left, start, limit, output, io)?;
            }
            if output.len() < limit && node.max.as_slice() >= start {
                collect_range(store, node.right, start, limit, output, io)?;
            }
        }
        _ => return Err("wrong range object domain".into()),
    }
    Ok(())
}

fn diff_nodes(
    before: &Tree,
    before_id: [u8; 32],
    after: &Tree,
    after_id: [u8; 32],
    metric: &mut OpMetric,
    changed: &mut Vec<Row>,
) -> Result<(), String> {
    if before_id == after_id {
        return Ok(());
    }
    metric.diff_nodes += 1;
    let before_node = decode_object(before_id, authenticated_object(&before.objects, before_id)?)?;
    let after_node = decode_object(after_id, authenticated_object(&after.objects, after_id)?)?;
    metric.io.objects += 2;
    metric.io.calls += 2;
    metric.io.bytes += before_node.bytes.len() + after_node.bytes.len();
    let mut paired_bounds_match = before_node.children.len() == after_node.children.len()
        && !before_node.children.is_empty()
        && before_node.min == after_node.min
        && before_node.max == after_node.max;
    if paired_bounds_match {
        for (&left, &right) in before_node.children.iter().zip(&after_node.children) {
            let left = decode_object(left, authenticated_object(&before.objects, left)?)?;
            let right = decode_object(right, authenticated_object(&after.objects, right)?)?;
            metric.io.objects += 2;
            metric.io.calls += 2;
            metric.io.bytes += left.bytes.len() + right.bytes.len();
            if left.min != right.min || left.max != right.max {
                paired_bounds_match = false;
                break;
            }
        }
    }
    if paired_bounds_match {
        for (&left, &right) in before_node.children.iter().zip(&after_node.children) {
            diff_nodes(before, left, after, right, metric, changed)?;
        }
        return Ok(());
    }
    let before_rows = rows_under(before, before_id)?;
    let after_rows = rows_under(after, after_id)?;
    let mut before_map = before_rows
        .into_iter()
        .map(|row| (row.key, row.tuple))
        .collect::<BTreeMap<_, _>>();
    for row in after_rows {
        if before_map.get(&row.key) != Some(&row.tuple) {
            changed.push(row.clone());
        }
        before_map.remove(&row.key);
    }
    for (key, _) in before_map {
        changed.push(Row {
            key,
            tuple: Vec::new(),
        });
    }
    Ok(())
}

fn rows_under(tree: &Tree, id: [u8; 32]) -> Result<Vec<Row>, String> {
    let mut rows = Vec::new();
    let mut io = IoMetric::default();
    collect_range(&tree.objects, id, &[], usize::MAX, &mut rows, &mut io)?;
    Ok(rows)
}

fn run_operations(tree: &Tree, pk: PkKind) {
    let point_key = tree.rows[tree.rows.len() / 2].key.clone();
    let missing_key = absent_key(
        pk,
        &tree.rows,
        tree.rows.len().saturating_mul(2).saturating_add(1),
    );
    emit_read("point", tree, pk, &point_key, true);
    emit_read("missing_point", tree, pk, &missing_key, false);

    let range_start = tree.rows[tree.rows.len() / 3].key.clone();
    let started = Instant::now();
    let cpu_started = cpu_time_ns();
    let (range, io) = tree.range(&range_start, 100).expect("range authenticates");
    let metric = OpMetric {
        wall: started.elapsed(),
        cpu_ns: cpu_time_ns().saturating_sub(cpu_started),
        io,
        digest: digest_rows(range.iter()),
        ..OpMetric::default()
    };
    print_row(
        "model",
        "range_100",
        pk,
        tree.rows.len(),
        tree,
        100,
        &metric,
        false,
        "ok",
    );

    let started = Instant::now();
    let cpu_started = cpu_time_ns();
    let (scan, io) = tree.scan().expect("scan authenticates");
    assert_eq!(scan.len(), tree.rows.len());
    let metric = OpMetric {
        wall: started.elapsed(),
        cpu_ns: cpu_time_ns().saturating_sub(cpu_started),
        io,
        digest: digest_rows(scan.iter()),
        ..OpMetric::default()
    };
    print_row(
        "model",
        "full_scan",
        pk,
        tree.rows.len(),
        tree,
        tree.rows.len(),
        &metric,
        false,
        "ok",
    );

    let (inserted, insert) = tree.mutate("insert", 1, pk);
    print_row(
        "model",
        if tree.geometry == Geometry::C2 {
            "insert_rebuild_model"
        } else {
            "insert"
        },
        pk,
        tree.rows.len(),
        &inserted,
        1,
        &insert,
        false,
        "ok",
    );
    let (updated, update) = tree.mutate("update_one", 1, pk);
    print_row(
        "model",
        "update_one",
        pk,
        tree.rows.len(),
        &updated,
        1,
        &update,
        false,
        "ok",
    );
    let random_n = (tree.rows.len() / 100).max(1);
    let (random, mutation) = tree.mutate("mutate_1pct", random_n, pk);
    print_row(
        "model",
        "mutate_1pct",
        pk,
        tree.rows.len(),
        &random,
        random_n,
        &mutation,
        false,
        "ok",
    );

    let (main_store, main_ref) = tree.branch_store(b"main");
    let (branch_store, branch_ref) = tree.branch_store(b"feature");
    let main = verify_branch(&main_store, main_ref).expect("main verifies");
    let branch = verify_branch(&branch_store, branch_ref).expect("branch verifies");
    assert_eq!(main.root, branch.root);
    let branch_metric = OpMetric {
        writes: 1,
        write_bytes: branch_store
            .get(&branch_ref)
            .expect("feature branch reference")
            .len(),
        shared: tree.objects.len(),
        digest: tree.logical_digest(),
        ..OpMetric::default()
    };
    print_row(
        "model",
        "branch_snapshot",
        pk,
        tree.rows.len(),
        tree,
        0,
        &branch_metric,
        false,
        "ok",
    );

    for (operation, d) in [
        ("hash_diff_1", 1),
        ("hash_diff_10", 10),
        ("hash_diff_1pct", random_n),
    ] {
        let (changed, _) = tree.mutate("mutate_1pct", d, pk);
        let diff = tree.diff(&changed).expect("hash-pruned diff authenticates");
        print_row(
            "model",
            operation,
            pk,
            tree.rows.len(),
            tree,
            d,
            &diff,
            false,
            "ok",
        );
    }
}

fn emit_read(kind: &str, tree: &Tree, pk: PkKind, key: &[u8], present: bool) {
    let started = Instant::now();
    let cpu_started = cpu_time_ns();
    let (row, io) = tree.point(key).expect("point authenticates");
    assert_eq!(row.is_some(), present);
    let metric = OpMetric {
        wall: started.elapsed(),
        cpu_ns: cpu_time_ns().saturating_sub(cpu_started),
        io,
        digest: row
            .as_ref()
            .map_or([0; 32], |value| digest_rows(std::iter::once(value))),
        ..OpMetric::default()
    };
    print_row(
        "model",
        kind,
        pk,
        tree.rows.len(),
        tree,
        1,
        &metric,
        false,
        "ok",
    );
}

fn run_corruption_controls(tree: &Tree, pk: PkKind) {
    let (store, branch_id) = tree.branch_store(b"main");
    let mut controls = 0;
    let mut missing = store.clone();
    missing.remove(&tree.root.id);
    assert!(verify_branch(&missing, branch_id).is_err());
    controls += 1;

    if let Some(child) = tree.root.children.first() {
        let mut missing_child = store.clone();
        missing_child.remove(child);
        assert!(verify_branch(&missing_child, branch_id).is_err());
        controls += 1;
    }

    let mut substituted = store.clone();
    let bytes = substituted.get_mut(&tree.root.id).expect("root exists");
    let mut changed = bytes.to_vec();
    let last = changed.len() - 1;
    changed[last] ^= 1;
    *bytes = Bytes::from(changed);
    assert!(verify_branch(&substituted, branch_id).is_err());
    controls += 1;

    let root = store.get(&tree.root.id).expect("root exists");
    let raw = decode_envelope(root).expect("root decodes");
    let mut bad_schema = raw.clone();
    bad_schema[4] ^= 1;
    assert_rehashed_root_rejected(
        tree,
        encode_envelope(
            domain_for_raw(&raw).expect("root has a known object kind"),
            bad_schema,
        ),
    );
    controls += 1;

    let mut bad_envelope = root.to_vec();
    bad_envelope[5] = 0xff;
    assert_rehashed_root_rejected(tree, Bytes::from(bad_envelope));
    controls += 1;

    let mut bad_length = root.to_vec();
    bad_length[6..10].copy_from_slice(&((MAX_OBJECT_BYTES + 1) as u32).to_be_bytes());
    assert_rehashed_root_rejected(tree, Bytes::from(bad_length));
    controls += 1;

    if !tree.root.children.is_empty() {
        let mut bad_edge = raw.clone();
        let last = bad_edge.len() - 1;
        bad_edge[last] ^= 1;
        assert_rehashed_root_rejected(
            tree,
            encode_envelope(
                domain_for_raw(&raw).expect("root has a known object kind"),
                bad_edge,
            ),
        );
        controls += 1;
    }

    let wrong = encode_envelope(0xfe, raw);
    assert_rehashed_root_rejected(tree, wrong);
    controls += 1;

    let mut bad_branch_raw =
        decode_envelope(store.get(&branch_id).expect("branch exists")).expect("branch decodes");
    let branch_name_len = u32::from_be_bytes(
        bad_branch_raw[20..24]
            .try_into()
            .expect("branch length field"),
    ) as usize;
    let root_offset = 24 + branch_name_len + 1;
    bad_branch_raw[root_offset] ^= 1;
    let bad_branch = encode_envelope(1, bad_branch_raw);
    let bad_branch_id = object_id(&bad_branch);
    let mut bad_branch_store = store.clone();
    bad_branch_store.insert(bad_branch_id, bad_branch);
    assert!(verify_branch(&bad_branch_store, bad_branch_id).is_err());
    controls += 1;

    let metric = OpMetric {
        digest: tree.logical_digest(),
        ..OpMetric::default()
    };
    print_row(
        "model",
        "corruption_controls",
        pk,
        tree.rows.len(),
        tree,
        controls,
        &metric,
        false,
        "rejected",
    );
}

fn assert_rehashed_root_rejected(tree: &Tree, replacement: Bytes) {
    let replacement_id = object_id(&replacement);
    let mut store = tree.objects.clone();
    store.remove(&tree.root.id);
    store.insert(replacement_id, replacement);
    let branch = encode_branch(
        b"main",
        tree.geometry,
        &Object {
            id: replacement_id,
            ..tree.root.clone()
        },
    );
    let branch_id = object_id(&branch);
    store.insert(branch_id, branch);
    assert!(verify_branch(&store, branch_id).is_err());
}

fn run_backend(backend: &str, tree: &Tree, pk: PkKind) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("backend runtime");
    runtime.block_on(async {
        match backend {
            "rocksdb" => {
                let dir = tempfile::tempdir().expect("RocksDB tempdir");
                let path = dir.path().join("db");
                let storage = RocksDB::open(&path).expect("open RocksDB");
                let metric = persist_backend(&storage, tree).await;
                print_row(
                    backend,
                    "persist",
                    pk,
                    tree.rows.len(),
                    tree,
                    0,
                    &metric,
                    false,
                    "ok",
                );
                storage.flush().expect("flush RocksDB");
                drop(storage);
                let settled = directory_bytes(dir.path());
                let reopened = RocksDB::open(&path).expect("reopen RocksDB");
                let reopen = verify_backend(&reopened, tree).await;
                emit_backend(backend, tree, pk, reopen, settled);
            }
            "slatedb" => {
                let dir = tempfile::tempdir().expect("SlateDB tempdir");
                let path = dir.path().join("db");
                let storage = SlateDB::open(&path).expect("open SlateDB");
                let metric = persist_backend(&storage, tree).await;
                print_row(
                    backend,
                    "persist",
                    pk,
                    tree.rows.len(),
                    tree,
                    0,
                    &metric,
                    false,
                    "ok",
                );
                storage
                    .flush_memtable_for_diagnostics()
                    .await
                    .expect("flush SlateDB");
                drop(storage);
                let settled = directory_bytes(dir.path());
                let reopened = SlateDB::open(&path).expect("reopen SlateDB");
                let reopen = verify_backend(&reopened, tree).await;
                emit_backend(backend, tree, pk, reopen, settled);
            }
            _ => unreachable!(),
        }
    });
}

async fn persist_backend<S: Storage>(storage: &S, tree: &Tree) -> OpMetric {
    let (store, _) = tree.branch_store(b"main");
    let started = Instant::now();
    let cpu_started = cpu_time_ns();
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin backend write");
    for entries in store.iter().collect::<Vec<_>>().chunks(1024) {
        write
            .put_many(
                OBJECT_SPACE,
                PutBatch {
                    entries: entries
                        .iter()
                        .map(|(id, bytes)| PutEntry {
                            key: Key(Bytes::copy_from_slice(id.as_slice())),
                            value: StoredValue {
                                bytes: (*bytes).clone(),
                            },
                        })
                        .collect(),
                },
            )
            .await
            .expect("write authenticated objects");
    }
    write.commit().await.expect("commit authenticated objects");
    OpMetric {
        wall: started.elapsed(),
        cpu_ns: cpu_time_ns().saturating_sub(cpu_started),
        io: IoMetric {
            calls: store.len().div_ceil(1024),
            ..IoMetric::default()
        },
        writes: store.len(),
        write_bytes: store.values().map(Bytes::len).sum(),
        digest: tree.logical_digest(),
        ..OpMetric::default()
    }
}

async fn verify_backend<S: Storage>(storage: &S, tree: &Tree) -> OpMetric {
    let started = Instant::now();
    let cpu_started = cpu_time_ns();
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin backend read");
    let (expected, branch_id) = tree.branch_store(b"main");
    let keys = expected
        .keys()
        .map(|id| Key(Bytes::copy_from_slice(id)))
        .collect::<Vec<_>>();
    let mut reopened = BTreeMap::new();
    let mut io = IoMetric::default();
    for chunk in keys.chunks(1024) {
        io.calls += 1;
        io.objects += chunk.len();
        let values = read
            .get_many(&[GetManyRequest {
                space: OBJECT_SPACE,
                keys: chunk,
                opts: GetOptions::default(),
            }])
            .await
            .expect("read backend objects");
        for (key, value) in chunk.iter().zip(values.values) {
            let lix::storage::ProjectedValue::FullValue(bytes) =
                value.expect("authenticated backend object exists")
            else {
                panic!("backend returned key-only projection")
            };
            io.bytes += bytes.len();
            reopened.insert(key.0.as_ref().try_into().expect("ObjectId key"), bytes);
        }
    }
    let verified = verify_branch(&reopened, branch_id).expect("cold reopen authenticates");
    assert_eq!(verified.root, tree.root.id);
    assert_eq!(verified.rows, tree.rows.len());
    OpMetric {
        wall: started.elapsed(),
        cpu_ns: cpu_time_ns().saturating_sub(cpu_started),
        io,
        digest: tree.logical_digest(),
        ..OpMetric::default()
    }
}

fn emit_backend(backend: &str, tree: &Tree, pk: PkKind, mut metric: OpMetric, settled: u64) {
    metric.settled_bytes = settled;
    print_row(
        backend,
        "cold_reopen",
        pk,
        tree.rows.len(),
        tree,
        0,
        &metric,
        true,
        "ok",
    );
}

fn fixture_rows(n: usize, width: usize, pk: PkKind) -> Vec<Row> {
    let mut rows = (0..n)
        .map(|ordinal| Row {
            key: canonical_key(pk, ordinal),
            tuple: schema_v1_tuple(width, ordinal),
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.key.cmp(&right.key));
    validate_rows(&rows).expect("fixture keys are canonical and unique");
    rows
}

fn schema_v1_tuple(width: usize, ordinal: usize) -> Vec<u8> {
    const TEXT: &[u8] = b"project status open owner agent typed native tuple priority branch ";
    let mut tuple = vec![0; width];
    let mut state = (ordinal as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    for (offset, byte) in tuple.iter_mut().enumerate() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = if offset < 32 || offset % 17 == 0 {
            (state >> 24) as u8
        } else {
            TEXT[(offset + ordinal * 7) % TEXT.len()]
        };
    }
    tuple
}

fn canonical_key(kind: PkKind, ordinal: usize) -> Vec<u8> {
    let mut key = Vec::new();
    match kind {
        PkKind::Integer => push_i64_component(&mut key, ordinal as i64),
        PkKind::Uuid => push_uuid_component(&mut key, deterministic_uuid(ordinal)),
        PkKind::Text => push_text_component(&mut key, &format!("row-{ordinal:012}")),
        PkKind::Composite => {
            push_text_component(&mut key, &format!("tenant-{:04}", ordinal % 97));
            push_i64_component(&mut key, (ordinal / 97) as i64);
            push_uuid_component(&mut key, deterministic_uuid(ordinal));
        }
    }
    key
}

fn absent_key(kind: PkKind, rows: &[Row], mut ordinal: usize) -> Vec<u8> {
    loop {
        let key = canonical_key(kind, ordinal);
        if rows
            .binary_search_by(|row| row.key.as_slice().cmp(&key))
            .is_err()
        {
            return key;
        }
        ordinal = ordinal.checked_add(1).expect("insert key ordinal");
    }
}

fn push_i64_component(output: &mut Vec<u8>, value: i64) {
    output.push(0x10);
    output.extend_from_slice(&(value as u64 ^ (1_u64 << 63)).to_be_bytes());
}

fn push_uuid_component(output: &mut Vec<u8>, value: [u8; 16]) {
    output.push(0x20);
    output.extend_from_slice(&value);
}

fn push_text_component(output: &mut Vec<u8>, value: &str) {
    output.push(0x30);
    for &byte in value.as_bytes() {
        if byte == 0 {
            output.extend_from_slice(&[0, 0xff]);
        } else {
            output.push(byte);
        }
    }
    output.extend_from_slice(&[0, 0]);
}

fn deterministic_uuid(ordinal: usize) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"exp-art-01-uuid\0");
    hasher.update(&(ordinal as u64).to_be_bytes());
    hasher.finalize().as_bytes()[..16]
        .try_into()
        .expect("UUID prefix")
}

fn first_different_bit(left: &[u8], right: &[u8]) -> Option<usize> {
    let bytes = left.len().max(right.len());
    for index in 0..bytes {
        let difference =
            left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0);
        if difference != 0 {
            return Some(index * 8 + difference.leading_zeros() as usize);
        }
    }
    None
}

fn bit_at(bytes: &[u8], bit: usize) -> bool {
    bytes
        .get(bit / 8)
        .is_some_and(|byte| byte & (0x80 >> (bit % 8)) != 0)
}

fn deterministic_indices(n: usize, count: usize) -> Vec<usize> {
    let mut ranked = (0..n)
        .map(|index| {
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"exp-art-01-mutation-order\0");
            hasher.update(&(index as u64).to_be_bytes());
            (*hasher.finalize().as_bytes(), index)
        })
        .collect::<Vec<_>>();
    ranked.sort_unstable();
    ranked
        .into_iter()
        .take(count.min(n))
        .map(|(_, index)| index)
        .collect()
}

fn digest_rows<'a>(rows: impl Iterator<Item = &'a Row>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"exp-art-01-result-v1\0");
    for row in rows {
        hasher.update(&(row.key.len() as u64).to_be_bytes());
        hasher.update(&row.key);
        hasher.update(&(row.tuple.len() as u64).to_be_bytes());
        hasher.update(&row.tuple);
    }
    *hasher.finalize().as_bytes()
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

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self.offset.checked_add(len).ok_or("cursor overflow")?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or("truncated authenticated object")?;
        self.offset = end;
        Ok(value)
    }

    fn magic(&mut self, expected: &[u8; 4]) -> Result<(), String> {
        if self.take(4)? != expected {
            return Err("wrong authenticated object kind".into());
        }
        Ok(())
    }

    fn fingerprint(&mut self) -> Result<(), String> {
        if self.take(SCHEMA_FINGERPRINT.len())? != SCHEMA_FINGERPRINT {
            return Err("schema fingerprint mismatch".into());
        }
        Ok(())
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<usize, String> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().map_err(|_| "bad u32")?) as usize)
    }

    fn bytes(&mut self) -> Result<&'a [u8], String> {
        let len = self.u32()?;
        self.take(len)
    }

    fn id(&mut self) -> Result<[u8; 32], String> {
        self.take(32)?.try_into().map_err(|_| "bad ObjectId".into())
    }

    fn remaining(&mut self) -> &'a [u8] {
        let value = &self.bytes[self.offset..];
        self.offset = self.bytes.len();
        value
    }

    fn finish(self) -> Result<(), String> {
        if self.offset != self.bytes.len() {
            return Err("trailing authenticated bytes".into());
        }
        Ok(())
    }
}

fn push_u32(output: &mut Vec<u8>, value: usize) {
    output.extend_from_slice(
        &u32::try_from(value)
            .expect("model value fits u32")
            .to_be_bytes(),
    );
}

fn push_bytes(output: &mut Vec<u8>, value: &[u8]) {
    push_u32(output, value.len());
    output.extend_from_slice(value);
}

fn cpu_time_ns() -> u128 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the supplied rusage on success.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return 0;
    }
    // SAFETY: result == 0 means the structure was initialized.
    let usage = unsafe { usage.assume_init() };
    timeval_ns(usage.ru_utime) + timeval_ns(usage.ru_stime)
}

fn timeval_ns(value: libc::timeval) -> u128 {
    (value.tv_sec as u128) * 1_000_000_000 + (value.tv_usec as u128) * 1_000
}

fn rss_kb() -> i64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the supplied rusage on success.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return 0;
    }
    // SAFETY: result == 0 means the structure was initialized.
    unsafe { usage.assume_init() }.ru_maxrss
}

fn directory_bytes(path: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                directory_bytes(&path)
            } else {
                entry.metadata().map_or(0, |metadata| metadata.len())
            }
        })
        .sum()
}

fn print_header() {
    println!(
        "version,backend,operation,geometry,pk,n,d,page_target,tuple_width,height,objects,object_bytes,wall_ns,cpu_ns,rss_kb,calls,read_objects,read_bytes,writes,write_bytes,settled_bytes,shared_objects,diff_nodes,result_digest,timing_scope,cold_reopen,status"
    );
}

#[allow(clippy::too_many_arguments)]
fn print_row(
    backend: &str,
    operation: &str,
    pk: PkKind,
    n: usize,
    tree: &Tree,
    d: usize,
    metric: &OpMetric,
    cold_reopen: bool,
    status: &str,
) {
    black_box(metric.digest);
    println!(
        "{CSV_VERSION},{backend},{operation},{geometry},{pk},{n},{d},{target},{width},{height},{objects},{object_bytes},{wall_ns},{cpu_ns},{rss},{calls},{read_objects},{read_bytes},{writes},{write_bytes},{settled_bytes},{shared},{diff_nodes},{digest},{timing_scope},{cold_reopen},{status}",
        geometry = tree.geometry.label(),
        pk = pk.label(),
        target = tree.page_target,
        width = tree.rows[0].tuple.len(),
        height = tree_height(tree),
        objects = tree.objects.len(),
        object_bytes = tree.object_bytes(),
        wall_ns = metric.wall.as_nanos(),
        cpu_ns = metric.cpu_ns,
        rss = rss_kb(),
        calls = metric.io.calls,
        read_objects = metric.io.objects,
        read_bytes = metric.io.bytes,
        writes = metric.writes,
        write_bytes = metric.write_bytes,
        settled_bytes = metric.settled_bytes,
        shared = metric.shared,
        diff_nodes = metric.diff_nodes,
        digest = hex(&metric.digest),
        timing_scope = if metric.rebuild_model {
            "full_rebuild_model_non_oltp"
        } else {
            "authenticated_operation"
        },
    );
}

fn tree_height(tree: &Tree) -> usize {
    let mut height = 1;
    let mut current = decode_object(
        tree.root.id,
        authenticated_object(&tree.objects, tree.root.id).expect("height root exists"),
    )
    .expect("height root authenticates");
    while let Some(child) = current.children.first().copied() {
        height += 1;
        current = decode_object(
            child,
            authenticated_object(&tree.objects, child).expect("height child exists"),
        )
        .expect("height child authenticates");
    }
    height
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}
