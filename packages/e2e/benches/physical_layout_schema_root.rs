//! EXP-SCHEMA-ROOT-17: global mixed-schema tree versus schema-root directory.
//!
//! Both models have exactly one repository-root authority. The partitioned
//! model has no mirrored global tree: its authenticated directory is the sole
//! map from schema identity to one canonical C2 subtree root.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

const PAGE_TARGET: usize = 4 << 10;
const INTERNAL_FANOUT: usize = 64;
const SAMPLES: usize = 7;
const LOOKUPS: usize = 10_000;
const COUNTS: [usize; 4] = [1_000, 10_000, 50_000, 100_000];
const SCHEMA_COUNTS: [usize; 5] = [1, 4, 16, 64, 256];
const DIRECTORY_BINDING: [u8; 16] = *b"schema-dir-v1\0\0\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeyKind {
    Integer,
    Uuid,
    Text,
    Composite,
}

impl KeyKind {
    const ALL: [Self; 4] = [Self::Integer, Self::Uuid, Self::Text, Self::Composite];

    const fn label(self) -> &'static str {
        match self {
            Self::Integer => "int8",
            Self::Uuid => "uuid",
            Self::Text => "text",
            Self::Composite => "composite",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Distribution {
    Uniform,
    Hot,
}

impl Distribution {
    const ALL: [Self; 2] = [Self::Uniform, Self::Hot];

    const fn label(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::Hot => "hot80",
        }
    }
}

#[derive(Clone)]
struct LogicalRow {
    schema: [u8; 16],
    pk: Vec<u8>,
    owner: [u8; 16],
    value: Vec<u8>,
}

impl LogicalRow {
    fn global_key(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(32 + self.pk.len());
        key.extend_from_slice(&self.schema);
        key.extend_from_slice(&self.pk);
        key.extend_from_slice(&self.owner);
        key
    }

    fn local_key(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(16 + self.pk.len());
        key.extend_from_slice(&self.pk);
        key.extend_from_slice(&self.owner);
        key
    }
}

#[derive(Clone)]
struct Entry {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone)]
struct Page {
    id: [u8; 32],
    bytes: Vec<u8>,
    first: Vec<u8>,
    last: Vec<u8>,
    entries: usize,
}

#[derive(Clone)]
struct AuthTree {
    binding: [u8; 16],
    levels: Vec<Vec<Page>>,
    objects: BTreeMap<[u8; 32], Vec<u8>>,
}

#[derive(Default, Clone, Copy)]
struct ReadMetric {
    calls: usize,
    bytes: usize,
    pages: usize,
}

impl AuthTree {
    fn build(mut entries: Vec<Entry>, binding: [u8; 16]) -> Self {
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        assert!(!entries.is_empty());
        assert!(entries.windows(2).all(|pair| pair[0].key < pair[1].key));
        let leaves = partition_entries(&entries)
            .into_iter()
            .map(|chunk| encode_page(chunk, binding, false))
            .collect::<Vec<_>>();
        let mut levels = vec![leaves];
        while levels.last().is_some_and(|level| level.len() > 1) {
            let children = levels.last().expect("level");
            let next = children
                .chunks(INTERNAL_FANOUT)
                .enumerate()
                .map(|(group, children)| {
                    let first_child = group * INTERNAL_FANOUT;
                    let entries = children
                        .iter()
                        .enumerate()
                        .map(|(offset, child)| {
                            let mut value = Vec::with_capacity(36);
                            value.extend_from_slice(&child.id);
                            push_u32(&mut value, first_child + offset);
                            Entry {
                                key: child.last.clone(),
                                value,
                            }
                        })
                        .collect::<Vec<_>>();
                    encode_page(&entries, binding, true)
                })
                .collect();
            levels.push(next);
        }
        let objects = levels
            .iter()
            .flat_map(|level| level.iter())
            .map(|page| (page.id, page.bytes.clone()))
            .collect();
        let tree = Self {
            binding,
            levels,
            objects,
        };
        tree.verify().expect("fresh authenticated tree verifies");
        tree
    }

    fn root(&self) -> [u8; 32] {
        self.levels.last().expect("root level")[0].id
    }

    fn verify(&self) -> Result<(), &'static str> {
        for (level_index, level) in self.levels.iter().enumerate() {
            let mut previous: Option<&[u8]> = None;
            for page in level {
                if blake3::hash(&page.bytes).as_bytes() != &page.id {
                    return Err("tree page ObjectId mismatch");
                }
                let decoded = decode_page(&page.bytes, self.binding, level_index > 0)?;
                if decoded.len() != page.entries
                    || decoded.first().map(|entry| entry.key.as_slice())
                        != Some(page.first.as_slice())
                    || decoded.last().map(|entry| entry.key.as_slice())
                        != Some(page.last.as_slice())
                    || decoded.windows(2).any(|pair| pair[0].key >= pair[1].key)
                    || previous.is_some_and(|previous| previous >= page.first.as_slice())
                {
                    return Err("tree page order/bounds mismatch");
                }
                if level_index > 0 {
                    for entry in decoded {
                        if entry.value.len() != 36 {
                            return Err("malformed child edge");
                        }
                        let child_id: [u8; 32] =
                            entry.value[..32].try_into().map_err(|_| "child ObjectId")?;
                        let child = read_u32(&entry.value, 32)?;
                        let expected = self.levels[level_index - 1]
                            .get(child)
                            .ok_or("child ordinal out of bounds")?;
                        if expected.id != child_id || expected.last != entry.key {
                            return Err("authenticated child edge mismatch");
                        }
                    }
                }
                previous = Some(page.last.as_slice());
            }
        }
        Ok(())
    }

    fn point(&self, key: &[u8]) -> (Option<Vec<u8>>, ReadMetric) {
        let mut metric = ReadMetric::default();
        let mut page_index = 0;
        for level_index in (0..self.levels.len()).rev() {
            let page = &self.levels[level_index][page_index];
            authenticate(page, &mut metric);
            let entries = decode_page(&page.bytes, self.binding, level_index > 0)
                .expect("verified page decodes");
            if level_index == 0 {
                let result = entries
                    .binary_search_by(|entry| entry.key.as_slice().cmp(key))
                    .ok()
                    .map(|index| entries[index].value.clone());
                return (result, metric);
            }
            let slot = entries.partition_point(|entry| entry.key.as_slice() < key);
            let entry = &entries[slot.min(entries.len() - 1)];
            page_index = read_u32(&entry.value, 32).expect("verified child ordinal");
        }
        unreachable!()
    }

    fn range(&self, start: &[u8], end: &[u8], limit: usize) -> (usize, [u8; 32], ReadMetric) {
        let mut metric = ReadMetric::default();
        let mut rows = 0;
        let mut digest = blake3::Hasher::new();
        for page in &self.levels[0] {
            if page.last.as_slice() < start || page.first.as_slice() >= end {
                continue;
            }
            authenticate(page, &mut metric);
            for entry in decode_page(&page.bytes, self.binding, false).expect("verified leaf") {
                if entry.key.as_slice() >= start && entry.key.as_slice() < end {
                    digest.update(&entry.key);
                    digest.update(&entry.value);
                    rows += 1;
                    if rows == limit {
                        return (rows, *digest.finalize().as_bytes(), metric);
                    }
                }
            }
        }
        (rows, *digest.finalize().as_bytes(), metric)
    }

    fn scan(&self, limit: usize) -> (usize, [u8; 32], ReadMetric) {
        let mut metric = ReadMetric::default();
        let mut rows = 0;
        let mut digest = blake3::Hasher::new();
        for page in &self.levels[0] {
            authenticate(page, &mut metric);
            for entry in decode_page(&page.bytes, self.binding, false).expect("verified leaf") {
                digest.update(&entry.key);
                digest.update(&entry.value);
                rows += 1;
                if rows == limit {
                    return (rows, *digest.finalize().as_bytes(), metric);
                }
            }
        }
        (rows, *digest.finalize().as_bytes(), metric)
    }
}

fn authenticate(page: &Page, metric: &mut ReadMetric) {
    assert_eq!(blake3::hash(&page.bytes).as_bytes(), &page.id);
    metric.calls += 1;
    metric.pages += 1;
    metric.bytes += page.bytes.len();
}

#[derive(Clone)]
struct GlobalRepo {
    root_id: [u8; 32],
    root_bytes: Vec<u8>,
    tree: AuthTree,
    rows: Vec<LogicalRow>,
}

impl GlobalRepo {
    fn build(rows: Vec<LogicalRow>) -> Self {
        let tree = AuthTree::build(
            rows.iter()
                .map(|row| Entry {
                    key: row.global_key(),
                    value: row.value.clone(),
                })
                .collect(),
            [0; 16],
        );
        let mut root_bytes = b"LXRG".to_vec();
        root_bytes.extend_from_slice(&tree.root());
        let root_id = *blake3::hash(&root_bytes).as_bytes();
        Self {
            root_id,
            root_bytes,
            tree,
            rows,
        }
    }

    fn point(&self, row: &LogicalRow, missing: bool) -> (bool, ReadMetric) {
        let mut metric = self.authenticate_root();
        let mut key = row.global_key();
        if missing {
            key.push(0x80);
        }
        let (value, tree_metric) = self.tree.point(&key);
        add_metric(&mut metric, tree_metric);
        (value.is_some(), metric)
    }

    fn typed_range(&self, schema: [u8; 16], limit: usize) -> (usize, [u8; 32], ReadMetric) {
        let end = prefix_successor(&schema).expect("fixture schema is not maximal");
        let mut metric = self.authenticate_root();
        let (rows, digest, tree_metric) = self.tree.range(&schema, &end, limit);
        add_metric(&mut metric, tree_metric);
        (rows, digest, metric)
    }

    fn authenticate_root(&self) -> ReadMetric {
        assert_eq!(blake3::hash(&self.root_bytes).as_bytes(), &self.root_id);
        ReadMetric {
            calls: 1,
            pages: 1,
            bytes: self.root_bytes.len(),
        }
    }

    fn objects(&self) -> BTreeMap<[u8; 32], Vec<u8>> {
        let mut objects = self.tree.objects.clone();
        objects.insert(self.root_id, self.root_bytes.clone());
        objects
    }
}

#[derive(Clone)]
struct SchemaRepo {
    root_id: [u8; 32],
    root_bytes: Vec<u8>,
    directory: AuthTree,
    subtrees: BTreeMap<[u8; 16], AuthTree>,
    rows: Vec<LogicalRow>,
}

impl SchemaRepo {
    fn build(rows: Vec<LogicalRow>) -> Self {
        let mut grouped = BTreeMap::<[u8; 16], Vec<Entry>>::new();
        for row in &rows {
            grouped.entry(row.schema).or_default().push(Entry {
                key: row.local_key(),
                value: row.value.clone(),
            });
        }
        let subtrees = grouped
            .into_iter()
            .map(|(schema, entries)| (schema, AuthTree::build(entries, schema)))
            .collect::<BTreeMap<_, _>>();
        let directory = AuthTree::build(
            subtrees
                .iter()
                .map(|(schema, tree)| {
                    let mut value = tree.root().to_vec();
                    push_u32(
                        &mut value,
                        tree.levels[0].iter().map(|page| page.entries).sum(),
                    );
                    Entry {
                        key: schema.to_vec(),
                        value,
                    }
                })
                .collect(),
            DIRECTORY_BINDING,
        );
        let mut root_bytes = b"LXRS".to_vec();
        root_bytes.extend_from_slice(&directory.root());
        let root_id = *blake3::hash(&root_bytes).as_bytes();
        let repo = Self {
            root_id,
            root_bytes,
            directory,
            subtrees,
            rows,
        };
        repo.verify().expect("schema repository verifies");
        repo
    }

    fn verify(&self) -> Result<(), &'static str> {
        if blake3::hash(&self.root_bytes).as_bytes() != &self.root_id
            || self.root_bytes.get(..4) != Some(b"LXRS")
            || self.root_bytes.get(4..36) != Some(self.directory.root().as_slice())
        {
            return Err("repository/schema-directory root mismatch");
        }
        self.directory.verify()?;
        let expected = self.subtrees.keys().copied().collect::<BTreeSet<_>>();
        let mut actual = BTreeSet::new();
        for page in &self.directory.levels[0] {
            for entry in decode_page(&page.bytes, DIRECTORY_BINDING, false)? {
                let schema: [u8; 16] = entry
                    .key
                    .as_slice()
                    .try_into()
                    .map_err(|_| "malformed schema identity")?;
                if !actual.insert(schema) {
                    return Err("duplicate schema directory member");
                }
                let subtree = self.subtrees.get(&schema).ok_or("missing schema subtree")?;
                if entry.value.get(..32) != Some(subtree.root().as_slice())
                    || subtree.binding != schema
                {
                    return Err("wrong schema-root binding or swapped subtree");
                }
                subtree.verify()?;
            }
        }
        if actual != expected {
            return Err("missing schema directory member");
        }
        Ok(())
    }

    fn point(&self, row: &LogicalRow, missing: bool) -> (bool, ReadMetric) {
        let mut metric = self.authenticate_root();
        let (directory_value, directory_metric) = self.directory.point(&row.schema);
        add_metric(&mut metric, directory_metric);
        let directory_value = directory_value.expect("authenticated schema directory member");
        let subtree = self.subtrees.get(&row.schema).expect("schema subtree");
        assert_eq!(directory_value.get(..32), Some(subtree.root().as_slice()));
        let mut key = row.local_key();
        if missing {
            key.push(0x80);
        }
        let (value, subtree_metric) = subtree.point(&key);
        add_metric(&mut metric, subtree_metric);
        (value.is_some(), metric)
    }

    fn typed_range(&self, schema: [u8; 16], limit: usize) -> (usize, [u8; 32], ReadMetric) {
        let mut metric = self.authenticate_root();
        let (directory_value, directory_metric) = self.directory.point(&schema);
        add_metric(&mut metric, directory_metric);
        let subtree = self.subtrees.get(&schema).expect("schema subtree");
        assert_eq!(
            directory_value.as_deref().and_then(|value| value.get(..32)),
            Some(subtree.root().as_slice())
        );
        let (rows, digest, subtree_metric) = subtree.scan(limit);
        add_metric(&mut metric, subtree_metric);
        (rows, digest, metric)
    }

    fn authenticate_root(&self) -> ReadMetric {
        assert_eq!(blake3::hash(&self.root_bytes).as_bytes(), &self.root_id);
        ReadMetric {
            calls: 1,
            pages: 1,
            bytes: self.root_bytes.len(),
        }
    }

    fn objects(&self) -> BTreeMap<[u8; 32], Vec<u8>> {
        let mut objects = self.directory.objects.clone();
        for subtree in self.subtrees.values() {
            objects.extend(subtree.objects.clone());
        }
        objects.insert(self.root_id, self.root_bytes.clone());
        objects
    }
}

fn add_metric(target: &mut ReadMetric, source: ReadMetric) {
    target.calls += source.calls;
    target.pages += source.pages;
    target.bytes += source.bytes;
}

fn main() {
    let max_n = std::env::var("LIX_SCHEMA_ROOT_MAX_N")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(usize::MAX);
    println!(
        "key_kind,n,schemas,distribution,layout,operation,d,p50_ns,p95_ns,calls,read_bytes,puts,write_bytes,root_bytes,settled_bytes,result"
    );
    for kind in KeyKind::ALL {
        for n in COUNTS.into_iter().filter(|n| *n <= max_n) {
            for schemas in SCHEMA_COUNTS.into_iter().filter(|schemas| *schemas <= n) {
                for distribution in Distribution::ALL {
                    let rows = fixture_rows(kind, n, schemas, distribution);
                    let global = GlobalRepo::build(rows.clone());
                    let schema = SchemaRepo::build(rows);
                    benchmark_points(kind, n, schemas, distribution, &global, &schema);
                    benchmark_ranges(kind, n, schemas, distribution, &global, &schema);
                    benchmark_mutations(kind, n, schemas, distribution, &global, &schema);
                    corruption_controls(&schema);
                }
            }
        }
    }
    insertion_order_control();
}

fn benchmark_points(
    kind: KeyKind,
    n: usize,
    schemas: usize,
    distribution: Distribution,
    global: &GlobalRepo,
    schema: &SchemaRepo,
) {
    for missing in [false, true] {
        for (layout, run) in [("global", 0_u8), ("schema_root", 1_u8)] {
            let mut samples = Vec::new();
            let mut metric = ReadMetric::default();
            let mut found = 0;
            for sample in 0..SAMPLES {
                let began = Instant::now();
                for iteration in 0..LOOKUPS {
                    let index = query_index(iteration + sample * 17, n, schemas, distribution);
                    let (present, read) = if run == 0 {
                        global.point(&global.rows[index], missing)
                    } else {
                        schema.point(&schema.rows[index], missing)
                    };
                    found += usize::from(present);
                    metric = read;
                }
                samples.push(began.elapsed() / LOOKUPS as u32);
            }
            samples.sort_unstable();
            print_result(
                kind,
                n,
                schemas,
                distribution,
                layout,
                if missing {
                    "point_missing"
                } else {
                    "point_present"
                },
                0,
                samples[SAMPLES / 2],
                samples[SAMPLES - 1],
                metric,
                MutationMetric::default(),
                if run == 0 {
                    global.root_bytes.len()
                } else {
                    schema.root_bytes.len()
                },
                if run == 0 {
                    object_bytes(&global.objects())
                } else {
                    object_bytes(&schema.objects())
                },
                found,
            );
        }
    }
}

fn benchmark_ranges(
    kind: KeyKind,
    n: usize,
    schemas: usize,
    distribution: Distribution,
    global: &GlobalRepo,
    schema: &SchemaRepo,
) {
    let selected = schema.rows[query_index(0, n, schemas, distribution)].schema;
    for limit in [100, usize::MAX] {
        let operation = if limit == 100 {
            "range100"
        } else {
            "full_typed_scan"
        };
        for (layout, run) in [("global", 0_u8), ("schema_root", 1_u8)] {
            let mut samples = Vec::new();
            let mut metric = ReadMetric::default();
            let mut rows = 0;
            let mut digest = [0; 32];
            for _ in 0..SAMPLES {
                let began = Instant::now();
                let result = if run == 0 {
                    global.typed_range(selected, limit)
                } else {
                    schema.typed_range(selected, limit)
                };
                samples.push(began.elapsed());
                (rows, digest, metric) = result;
            }
            samples.sort_unstable();
            print_result(
                kind,
                n,
                schemas,
                distribution,
                layout,
                operation,
                rows,
                samples[SAMPLES / 2],
                samples[SAMPLES - 1],
                metric,
                MutationMetric::default(),
                if run == 0 {
                    global.root_bytes.len()
                } else {
                    schema.root_bytes.len()
                },
                if run == 0 {
                    object_bytes(&global.objects())
                } else {
                    object_bytes(&schema.objects())
                },
                u64::from_be_bytes(digest[..8].try_into().expect("digest")) as usize,
            );
        }
    }
}

#[derive(Default, Clone, Copy)]
struct MutationMetric {
    puts: usize,
    bytes: usize,
}

fn benchmark_mutations(
    kind: KeyKind,
    n: usize,
    schemas: usize,
    distribution: Distribution,
    global: &GlobalRepo,
    schema: &SchemaRepo,
) {
    for d in [1, 10, (n / 100).max(1)] {
        for across in [false, true] {
            let operation = if across {
                "update_across"
            } else {
                "update_one_schema"
            };
            let changed = mutate_rows(&global.rows, d, schemas, distribution, across);
            let began = Instant::now();
            let next_global = GlobalRepo::build(changed.clone());
            let global_time = began.elapsed();
            let global_metric = mutation_metric(&global.objects(), &next_global.objects());
            print_result(
                kind,
                n,
                schemas,
                distribution,
                "global",
                operation,
                d,
                global_time,
                global_time,
                ReadMetric::default(),
                global_metric,
                global.root_bytes.len(),
                object_bytes(&global.objects()),
                next_global.root_prefix() as usize,
            );

            let began = Instant::now();
            let next_schema = SchemaRepo::build(changed);
            let schema_time = began.elapsed();
            let schema_metric = mutation_metric(&schema.objects(), &next_schema.objects());
            print_result(
                kind,
                n,
                schemas,
                distribution,
                "schema_root",
                operation,
                d,
                schema_time,
                schema_time,
                ReadMetric::default(),
                schema_metric,
                schema.root_bytes.len(),
                object_bytes(&schema.objects()),
                next_schema.root_prefix() as usize,
            );
        }
    }
}

impl GlobalRepo {
    fn root_prefix(&self) -> u64 {
        u64::from_be_bytes(self.root_id[..8].try_into().expect("root"))
    }
}

impl SchemaRepo {
    fn root_prefix(&self) -> u64 {
        u64::from_be_bytes(self.root_id[..8].try_into().expect("root"))
    }
}

#[allow(clippy::too_many_arguments)]
fn print_result(
    kind: KeyKind,
    n: usize,
    schemas: usize,
    distribution: Distribution,
    layout: &str,
    operation: &str,
    d: usize,
    p50: Duration,
    p95: Duration,
    read: ReadMetric,
    mutation: MutationMetric,
    root_bytes: usize,
    settled_bytes: usize,
    result: usize,
) {
    println!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        kind.label(),
        n,
        schemas,
        distribution.label(),
        layout,
        operation,
        d,
        p50.as_nanos(),
        p95.as_nanos(),
        read.calls,
        read.bytes,
        mutation.puts,
        mutation.bytes,
        root_bytes,
        settled_bytes,
        result,
    );
}

fn partition_entries(entries: &[Entry]) -> Vec<&[Entry]> {
    let mut pages = Vec::new();
    let mut start = 0;
    while start < entries.len() {
        let mut end = start + 1;
        let mut bytes = 64 + entry_size(&entries[start]);
        while end < entries.len() {
            let next = entry_size(&entries[end]);
            if bytes + next > PAGE_TARGET {
                break;
            }
            bytes += next;
            end += 1;
        }
        pages.push(&entries[start..end]);
        start = end;
    }
    pages
}

fn entry_size(entry: &Entry) -> usize {
    8 + entry.key.len() + entry.value.len()
}

fn encode_page(entries: &[Entry], binding: [u8; 16], internal: bool) -> Page {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"LXST");
    bytes.push(u8::from(internal));
    bytes.extend_from_slice(&binding);
    push_u32(&mut bytes, entries.len());
    for entry in entries {
        push_bytes(&mut bytes, &entry.key);
        push_bytes(&mut bytes, &entry.value);
    }
    let id = *blake3::hash(&bytes).as_bytes();
    Page {
        id,
        bytes,
        first: entries[0].key.clone(),
        last: entries[entries.len() - 1].key.clone(),
        entries: entries.len(),
    }
}

fn decode_page(
    bytes: &[u8],
    binding: [u8; 16],
    internal: bool,
) -> Result<Vec<Entry>, &'static str> {
    let mut cursor = Cursor::new(bytes);
    if cursor.take(4)? != b"LXST"
        || cursor.take(1)? != [u8::from(internal)]
        || cursor.take(16)? != binding
    {
        return Err("page domain/binding mismatch");
    }
    let count = cursor.u32()?;
    if count == 0 {
        return Err("empty page");
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(Entry {
            key: cursor.bytes()?.to_vec(),
            value: cursor.bytes()?.to_vec(),
        });
    }
    cursor.finish()?;
    if entries.windows(2).any(|pair| pair[0].key >= pair[1].key) {
        return Err("duplicate or unordered page key");
    }
    Ok(entries)
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
        let value = self.bytes.get(self.offset..end).ok_or("truncated page")?;
        self.offset = end;
        Ok(value)
    }

    fn u32(&mut self) -> Result<usize, &'static str> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().map_err(|_| "u32")?) as usize)
    }

    fn bytes(&mut self) -> Result<&'a [u8], &'static str> {
        let len = self.u32()?;
        self.take(len)
    }

    fn finish(self) -> Result<(), &'static str> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("trailing page bytes")
        }
    }
}

fn fixture_rows(
    kind: KeyKind,
    n: usize,
    schemas: usize,
    distribution: Distribution,
) -> Vec<LogicalRow> {
    let schema_ids = (0..schemas).map(schema_id).collect::<Vec<_>>();
    (0..n)
        .map(|ordinal| {
            let schema_index = match distribution {
                Distribution::Uniform => ordinal % schemas,
                Distribution::Hot if ordinal < n * 4 / 5 => 0,
                Distribution::Hot => 1 + ordinal % schemas.saturating_sub(1).max(1),
            }
            .min(schemas - 1);
            LogicalRow {
                schema: schema_ids[schema_index],
                pk: fixture_pk(kind, ordinal),
                owner: owner_id(ordinal % 4),
                value: fixture_value(ordinal),
            }
        })
        .collect()
}

fn schema_id(index: usize) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"EXP-SCHEMA-ROOT-17.schema\0");
    hasher.update(&(index as u64).to_be_bytes());
    hasher.finalize().as_bytes()[..16]
        .try_into()
        .expect("schema")
}

fn owner_id(index: usize) -> [u8; 16] {
    let mut owner = [0; 16];
    owner[15] = index as u8;
    owner
}

fn fixture_pk(kind: KeyKind, ordinal: usize) -> Vec<u8> {
    match kind {
        KeyKind::Integer => (ordinal as u64).to_be_bytes().to_vec(),
        KeyKind::Uuid => {
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"EXP-SCHEMA-ROOT-17.uuid\0");
            hasher.update(&(ordinal as u64).to_be_bytes());
            hasher.finalize().as_bytes()[..16].to_vec()
        }
        KeyKind::Text => format!("entity/{ordinal:012}").into_bytes(),
        KeyKind::Composite => {
            let mut key = format!("tenant/{:04}/", ordinal % 97).into_bytes();
            key.extend_from_slice(&(ordinal as u64).to_be_bytes());
            key
        }
    }
}

fn fixture_value(ordinal: usize) -> Vec<u8> {
    let mut value = vec![0; 64];
    let mut state = (ordinal as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    for byte in &mut value {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    value
}

fn query_index(value: usize, n: usize, schemas: usize, distribution: Distribution) -> usize {
    match distribution {
        Distribution::Hot => value.wrapping_mul(2_654_435_761) % (n * 4 / 5).max(1),
        Distribution::Uniform => {
            let schema = value % schemas;
            let mut index = value.wrapping_mul(2_654_435_761) % n;
            while index % schemas != schema {
                index = (index + 1) % n;
            }
            index
        }
    }
}

fn mutate_rows(
    rows: &[LogicalRow],
    d: usize,
    schemas: usize,
    distribution: Distribution,
    across: bool,
) -> Vec<LogicalRow> {
    let mut next = rows.to_vec();
    for ordinal in 0..d {
        let index = if across {
            query_index(ordinal, rows.len(), schemas, Distribution::Uniform)
        } else {
            query_index(ordinal, rows.len(), schemas, distribution)
        };
        next[index].value[0] ^= 0x5a;
    }
    next
}

fn mutation_metric(
    before: &BTreeMap<[u8; 32], Vec<u8>>,
    after: &BTreeMap<[u8; 32], Vec<u8>>,
) -> MutationMetric {
    let created = after
        .iter()
        .filter(|(id, _)| !before.contains_key(*id))
        .collect::<Vec<_>>();
    MutationMetric {
        puts: created.len(),
        bytes: created.iter().map(|(_, bytes)| bytes.len()).sum(),
    }
}

fn object_bytes(objects: &BTreeMap<[u8; 32], Vec<u8>>) -> usize {
    objects.values().map(Vec::len).sum()
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    for byte in successor.iter_mut().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            return Some(successor);
        }
        *byte = 0;
    }
    None
}

fn corruption_controls(repo: &SchemaRepo) {
    repo.verify().expect("control schema repo verifies");
    let mut root = repo.clone();
    root.root_bytes[0] ^= 1;
    assert!(root.verify().is_err());

    if repo.subtrees.len() > 1 {
        let mut swapped = repo.clone();
        let schemas = swapped.subtrees.keys().copied().take(2).collect::<Vec<_>>();
        let left = swapped.subtrees[&schemas[0]].clone();
        let right = swapped.subtrees[&schemas[1]].clone();
        swapped.subtrees.insert(schemas[0], right);
        swapped.subtrees.insert(schemas[1], left);
        assert!(swapped.verify().is_err());

        let mut missing = repo.clone();
        missing.subtrees.remove(&schemas[0]);
        assert!(missing.verify().is_err());

        let mut changed_rows = repo.rows.clone();
        for schema in &schemas {
            changed_rows
                .iter_mut()
                .find(|row| row.schema == *schema)
                .expect("schema row")
                .value[0] ^= 0xa5;
        }
        let successor = SchemaRepo::build(changed_rows);
        let mut partial = repo.clone();
        partial
            .subtrees
            .insert(schemas[0], successor.subtrees[&schemas[0]].clone());
        assert!(partial.verify().is_err());
    }

    let malformed_directory = AuthTree::build(
        vec![Entry {
            key: vec![0; 15],
            value: vec![0; 36],
        }],
        DIRECTORY_BINDING,
    );
    let mut malformed_repo = repo.clone();
    malformed_repo.directory = malformed_directory;
    malformed_repo.root_bytes = b"LXRS".to_vec();
    malformed_repo
        .root_bytes
        .extend_from_slice(&malformed_repo.directory.root());
    malformed_repo.root_id = *blake3::hash(&malformed_repo.root_bytes).as_bytes();
    assert!(malformed_repo.verify().is_err());

    let directory_page = &repo.directory.levels[0][0];
    let mut malformed = directory_page.bytes.clone();
    malformed[25] ^= 1;
    assert!(
        decode_page(&malformed, DIRECTORY_BINDING, false).is_err()
            || blake3::hash(&malformed).as_bytes() != &directory_page.id
    );
}

fn insertion_order_control() {
    let rows = fixture_rows(KeyKind::Composite, 1_000, 16, Distribution::Uniform);
    let canonical = SchemaRepo::build(rows.clone());
    let mut reversed = rows;
    reversed.reverse();
    let reordered = SchemaRepo::build(reversed);
    assert_eq!(canonical.root_id, reordered.root_id);

    let duplicate = vec![
        Entry {
            key: vec![1],
            value: vec![1],
        },
        Entry {
            key: vec![1],
            value: vec![2],
        },
    ];
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let rejected = std::panic::catch_unwind(|| AuthTree::build(duplicate, DIRECTORY_BINDING));
    std::panic::set_hook(previous_hook);
    assert!(rejected.is_err());
}

fn push_u32(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&u32::try_from(value).expect("u32 model value").to_be_bytes());
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    push_u32(out, bytes.len());
    out.extend_from_slice(bytes);
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<usize, &'static str> {
    Ok(u32::from_be_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or("truncated u32")?
            .try_into()
            .map_err(|_| "u32")?,
    ) as usize)
}
