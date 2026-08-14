//! EXP-BOUNDARY-14: deterministic C2 leaf-boundary crossover.
//!
//! All policies use byte-identical C2 leaf/internal encodings. Only the
//! canonical partition rule differs. ObjectId sharing therefore measures the
//! physical effect of boundaries rather than a second page format.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

const TARGET: usize = 4 << 10;
const MIN_PAGE: usize = TARGET / 2;
const INTERNAL_FANOUT: usize = 64;
const COUNTS: [usize; 4] = [1_000, 10_000, 50_000, 100_000];
const SAMPLES: usize = 7;
const POINTS: usize = 20_000;
const SCHEMA: [u8; 16] = *b"typed-row-v1\0\0\0\0";

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
enum Policy {
    Fixed,
    Rolling,
    PrefixFence,
}

impl Policy {
    const ALL: [Self; 3] = [Self::Fixed, Self::Rolling, Self::PrefixFence];

    const fn label(self) -> &'static str {
        match self {
            Self::Fixed => "fixed_bytes",
            Self::Rolling => "rolling_full_key",
            Self::PrefixFence => "prefix_fence",
        }
    }
}

#[derive(Clone)]
struct Row {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone)]
struct Page {
    id: [u8; 32],
    bytes: Vec<u8>,
    first: Vec<u8>,
    last: Vec<u8>,
    rows: usize,
}

#[derive(Clone)]
struct Tree {
    policy: Policy,
    rows: Vec<Row>,
    levels: Vec<Vec<Page>>,
    objects: BTreeMap<[u8; 32], Vec<u8>>,
}

#[derive(Default, Clone, Copy)]
struct Mutation {
    puts: usize,
    bytes: usize,
    retained_leaves: usize,
    old_leaves: usize,
    new_leaves: usize,
}

impl Tree {
    fn build(rows: Vec<Row>, policy: Policy) -> Self {
        assert!(!rows.is_empty());
        assert!(rows.windows(2).all(|pair| pair[0].key < pair[1].key));
        let leaves = partition_rows(&rows, policy)
            .into_iter()
            .map(encode_leaf)
            .collect::<Vec<_>>();
        let mut levels = vec![leaves];
        while levels.last().is_some_and(|level| level.len() > 1) {
            levels.push(
                levels
                    .last()
                    .expect("level")
                    .chunks(INTERNAL_FANOUT)
                    .map(encode_internal)
                    .collect(),
            );
        }
        let objects = levels
            .iter()
            .flat_map(|level| level.iter())
            .map(|page| (page.id, page.bytes.clone()))
            .collect();
        let tree = Self {
            policy,
            rows,
            levels,
            objects,
        };
        tree.verify().expect("fresh tree verifies");
        tree
    }

    fn verify(&self) -> Result<(), &'static str> {
        let mut previous: Option<&[u8]> = None;
        for page in &self.levels[0] {
            if blake3::hash(&page.bytes).as_bytes() != &page.id {
                return Err("page ObjectId mismatch");
            }
            let keys = decode_leaf(&page.bytes)?;
            if keys.len() != page.rows
                || keys.first().map(Vec::as_slice) != Some(page.first.as_slice())
                || keys.last().map(Vec::as_slice) != Some(page.last.as_slice())
                || keys.windows(2).any(|pair| pair[0] >= pair[1])
                || previous.is_some_and(|previous| previous >= keys[0].as_slice())
            {
                return Err("leaf order/bounds mismatch");
            }
            previous = Some(page.last.as_slice());
        }
        for level in 1..self.levels.len() {
            for page in &self.levels[level] {
                if blake3::hash(&page.bytes).as_bytes() != &page.id {
                    return Err("internal ObjectId mismatch");
                }
                verify_internal(&page.bytes, &self.objects)?;
            }
        }
        Ok(())
    }

    fn point(&self, key: &[u8]) -> bool {
        let leaf = self.levels[0].partition_point(|page| page.last.as_slice() < key);
        let Some(page) = self.levels[0].get(leaf) else {
            return false;
        };
        let keys = decode_leaf(&page.bytes).expect("verified page");
        keys.binary_search_by(|candidate| candidate.as_slice().cmp(key))
            .is_ok()
    }

    fn mutate(&self, rows: Vec<Row>) -> (Self, Mutation) {
        let next = Self::build(rows, self.policy);
        let old = self.objects.keys().copied().collect::<BTreeSet<_>>();
        let old_leaves = self.levels[0]
            .iter()
            .map(|page| page.id)
            .collect::<BTreeSet<_>>();
        let retained_leaves = next.levels[0]
            .iter()
            .filter(|page| old_leaves.contains(&page.id))
            .count();
        let created = next
            .objects
            .iter()
            .filter(|(id, _)| !old.contains(*id))
            .collect::<Vec<_>>();
        let metric = Mutation {
            puts: created.len(),
            bytes: created.iter().map(|(_, bytes)| bytes.len()).sum(),
            retained_leaves,
            old_leaves: self.levels[0].len(),
            new_leaves: next.levels[0].len(),
        };
        (next, metric)
    }
}

fn main() {
    let max_n = std::env::var("LIX_BOUNDARY_MAX_N")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(usize::MAX);
    println!(
        "key_kind,n,policy,operation,d,p50_ns,p95_ns,height,leaves,total_bytes,puts,write_bytes,retained_leaves,old_leaves,new_leaves,digest"
    );
    for kind in KeyKind::ALL {
        for n in COUNTS.into_iter().filter(|n| *n <= max_n) {
            let rows = fixture_rows(kind, n);
            for policy in Policy::ALL {
                let tree = Tree::build(rows.clone(), policy);
                benchmark_points(kind, n, &tree);
                benchmark_reads(kind, n, &tree);
                for d in [1, 10, (n / 100).max(1)] {
                    benchmark_mutation(kind, n, &tree, "update", d, update_rows(&rows, d));
                    benchmark_mutation(
                        kind,
                        n,
                        &tree,
                        "insert_after",
                        d,
                        insert_rows(&rows, d, true),
                    );
                    benchmark_mutation(
                        kind,
                        n,
                        &tree,
                        "insert_before",
                        d,
                        insert_rows(&rows, d, false),
                    );
                    benchmark_mutation(kind, n, &tree, "key_shift", d, shift_rows(&rows, d));
                }
                benchmark_repeated(kind, n, &tree);
                benchmark_diff_merge(kind, n, &tree);
                corruption_controls(&tree);
            }
        }
    }
    adversarial_controls();
    insertion_order_control();
}

fn benchmark_points(kind: KeyKind, n: usize, tree: &Tree) {
    let missing = tree
        .rows
        .iter()
        .map(|row| {
            let mut key = row.key.clone();
            key.push(0x80);
            key
        })
        .collect::<Vec<_>>();
    for (operation, present) in [("point_present", true), ("point_missing", false)] {
        let mut samples = Vec::new();
        let mut found = 0_u64;
        for sample in 0..SAMPLES {
            let started = Instant::now();
            for iteration in 0..POINTS {
                let ordinal = permute(iteration + sample * 17, n);
                let key = if present {
                    tree.rows[ordinal].key.as_slice()
                } else {
                    missing[ordinal].as_slice()
                };
                found += u64::from(tree.point(key));
            }
            samples.push(started.elapsed() / POINTS as u32);
        }
        samples.sort_unstable();
        print_row(
            kind,
            n,
            tree,
            operation,
            0,
            samples[SAMPLES / 2],
            samples[SAMPLES - 1],
            Mutation::default(),
            found,
        );
    }
}

fn benchmark_reads(kind: KeyKind, n: usize, tree: &Tree) {
    let start = n / 3;
    let began = Instant::now();
    let mut digest = blake3::Hasher::new();
    for row in &tree.rows[start..(start + 100).min(n)] {
        digest.update(&row.key);
        digest.update(&row.value);
    }
    let range = began.elapsed();
    let range_digest = digest.finalize();
    print_row(
        kind,
        n,
        tree,
        "range100",
        100,
        range,
        range,
        Mutation::default(),
        u64::from_be_bytes(range_digest.as_bytes()[..8].try_into().expect("digest")),
    );

    let began = Instant::now();
    let mut digest = blake3::Hasher::new();
    for row in &tree.rows {
        digest.update(&row.key);
        digest.update(&row.value);
    }
    let scan = began.elapsed();
    let scan_digest = digest.finalize();
    print_row(
        kind,
        n,
        tree,
        "full_scan",
        n,
        scan,
        scan,
        Mutation::default(),
        u64::from_be_bytes(scan_digest.as_bytes()[..8].try_into().expect("digest")),
    );
}

fn benchmark_mutation(
    kind: KeyKind,
    n: usize,
    tree: &Tree,
    operation: &str,
    d: usize,
    rows: Vec<Row>,
) {
    let mut samples = Vec::new();
    let mut final_metric = Mutation::default();
    let mut digest = 0;
    for _ in 0..SAMPLES {
        let began = Instant::now();
        let (next, metric) = tree.mutate(rows.clone());
        samples.push(began.elapsed());
        final_metric = metric;
        digest = u64::from_be_bytes(next.root()[..8].try_into().expect("root digest"));
    }
    samples.sort_unstable();
    print_row(
        kind,
        n,
        tree,
        operation,
        d,
        samples[SAMPLES / 2],
        samples[SAMPLES - 1],
        final_metric,
        digest,
    );
}

fn benchmark_repeated(kind: KeyKind, n: usize, tree: &Tree) {
    let began = Instant::now();
    let mut current = tree.clone();
    let mut bytes = 0;
    let mut puts = 0;
    for round in 0..10 {
        let mut rows = current.rows.clone();
        rows[n / 2].value[round % 64] ^= 0x5a;
        let (next, metric) = current.mutate(rows);
        bytes += metric.bytes;
        puts += metric.puts;
        current = next;
    }
    let elapsed = began.elapsed();
    print_row(
        kind,
        n,
        tree,
        "repeated_update10",
        10,
        elapsed,
        elapsed,
        Mutation {
            puts,
            bytes,
            ..Mutation::default()
        },
        u64::from_be_bytes(current.root()[..8].try_into().expect("root")),
    );
}

fn benchmark_diff_merge(kind: KeyKind, n: usize, tree: &Tree) {
    let left_rows = update_rows(&tree.rows, 1);
    let right_rows = update_rows_offset(&tree.rows, 1, n / 3);
    let (left, _) = tree.mutate(left_rows.clone());
    let (_right, _) = tree.mutate(right_rows.clone());
    let began = Instant::now();
    let (diff_objects, diff_bytes) = object_diff(tree, &left);
    let elapsed = began.elapsed();
    print_row(
        kind,
        n,
        tree,
        "sparse_diff",
        1,
        elapsed,
        elapsed,
        Mutation {
            puts: diff_objects,
            bytes: diff_bytes,
            ..Mutation::default()
        },
        left.root_prefix(),
    );

    let dense_rows = update_rows(&tree.rows, (n / 100).max(1));
    let (dense, _) = tree.mutate(dense_rows);
    let began = Instant::now();
    let (diff_objects, diff_bytes) = object_diff(tree, &dense);
    let elapsed = began.elapsed();
    print_row(
        kind,
        n,
        tree,
        "dense_diff_1pct",
        n / 100,
        elapsed,
        elapsed,
        Mutation {
            puts: diff_objects,
            bytes: diff_bytes,
            ..Mutation::default()
        },
        dense.root_prefix(),
    );

    let mut merged_rows = left_rows;
    let changed = n / 3;
    merged_rows[changed] = right_rows[changed].clone();
    let began = Instant::now();
    let (merged, metric) = tree.mutate(merged_rows);
    let elapsed = began.elapsed();
    print_row(
        kind,
        n,
        tree,
        "merge_disjoint",
        2,
        elapsed,
        elapsed,
        metric,
        merged.root_prefix(),
    );

    assert_eq!(tree.objects, tree.clone().objects);
}

impl Tree {
    fn root(&self) -> [u8; 32] {
        self.levels.last().expect("root level")[0].id
    }

    fn root_prefix(&self) -> u64 {
        u64::from_be_bytes(self.root()[..8].try_into().expect("root"))
    }
}

#[allow(clippy::too_many_arguments)]
fn print_row(
    kind: KeyKind,
    n: usize,
    tree: &Tree,
    operation: &str,
    d: usize,
    p50: Duration,
    p95: Duration,
    mutation: Mutation,
    digest: u64,
) {
    println!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:016x}",
        kind.label(),
        n,
        tree.policy.label(),
        operation,
        d,
        p50.as_nanos(),
        p95.as_nanos(),
        tree.levels.len(),
        tree.levels[0].len(),
        tree.objects.values().map(Vec::len).sum::<usize>(),
        mutation.puts,
        mutation.bytes,
        mutation.retained_leaves,
        mutation.old_leaves,
        mutation.new_leaves,
        digest,
    );
}

fn partition_rows(rows: &[Row], policy: Policy) -> Vec<&[Row]> {
    let mut pages = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let mut end = start + 1;
        let mut bytes = leaf_header_bytes(&rows[start]) + row_bytes(&rows[start], &rows[start].key);
        while end < rows.len() {
            let next = row_bytes(&rows[end], &rows[start].key);
            if bytes + next > TARGET {
                break;
            }
            let split = if bytes >= MIN_PAGE {
                match policy {
                    Policy::Fixed => false,
                    Policy::Rolling => rolling_boundary(&rows[end - 1].key, next),
                    Policy::PrefixFence => fence(&rows[end - 1].key) != fence(&rows[end].key),
                }
            } else {
                false
            };
            if split {
                break;
            }
            bytes += next;
            end += 1;
        }
        pages.push(&rows[start..end]);
        start = end;
    }
    pages
}

fn leaf_header_bytes(row: &Row) -> usize {
    4 + SCHEMA.len() + 4 + row.key.len() * 2 + 4
}

fn row_bytes(row: &Row, first_key: &[u8]) -> usize {
    16 + row
        .key
        .len()
        .saturating_sub(shared_prefix(first_key, &row.key))
        + row.value.len()
}

fn roll_key(mut state: u64, key: &[u8]) -> u64 {
    for byte in key {
        state = state.rotate_left(7) ^ gear(*byte);
    }
    state ^ 0x9e37_79b9_7f4a_7c15
}

fn rolling_boundary(key: &[u8], next_bytes: usize) -> bool {
    let sample = (roll_key(0, key) & u64::from(u16::MAX)) as usize;
    let threshold = next_bytes
        .saturating_mul(usize::from(u16::MAX))
        .checked_div(MIN_PAGE)
        .unwrap_or(usize::from(u16::MAX))
        .min(usize::from(u16::MAX));
    sample <= threshold
}

fn gear(byte: u8) -> u64 {
    let mut value = u64::from(byte).wrapping_add(0x9e37_79b9);
    value ^= value >> 16;
    value = value.wrapping_mul(0x21f0_aaad);
    value ^= value >> 15;
    value.wrapping_mul(0xd35a_2d97)
}

fn fence(key: &[u8]) -> &[u8] {
    &key[..key.len().min(4)]
}

fn encode_leaf(rows: &[Row]) -> Page {
    let prefix = common_prefix(rows.iter().map(|row| row.key.as_slice()));
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"LXBL");
    bytes.extend_from_slice(&SCHEMA);
    push_bytes(&mut bytes, &rows[0].key);
    push_bytes(&mut bytes, &rows[rows.len() - 1].key);
    push_u32(&mut bytes, rows.len());
    push_bytes(&mut bytes, prefix);
    let mut suffixes = Vec::new();
    let mut values = Vec::new();
    for row in rows {
        push_u32(&mut bytes, suffixes.len());
        push_u32(&mut bytes, row.key.len() - prefix.len());
        push_u32(&mut bytes, values.len());
        push_u32(&mut bytes, row.value.len());
        suffixes.extend_from_slice(&row.key[prefix.len()..]);
        values.extend_from_slice(&row.value);
    }
    bytes.extend_from_slice(&suffixes);
    bytes.extend_from_slice(&values);
    let id = *blake3::hash(&bytes).as_bytes();
    Page {
        id,
        bytes,
        first: rows[0].key.clone(),
        last: rows[rows.len() - 1].key.clone(),
        rows: rows.len(),
    }
}

fn encode_internal(children: &[Page]) -> Page {
    let prefix = common_prefix(children.iter().map(|page| page.last.as_slice()));
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"LXBI");
    bytes.extend_from_slice(&SCHEMA);
    push_bytes(&mut bytes, &children[0].first);
    push_bytes(&mut bytes, &children[children.len() - 1].last);
    push_u32(&mut bytes, children.len());
    push_bytes(&mut bytes, prefix);
    for child in children {
        bytes.extend_from_slice(&child.id);
        push_bytes(&mut bytes, &child.last[prefix.len()..]);
        push_u32(&mut bytes, child.rows);
    }
    let id = *blake3::hash(&bytes).as_bytes();
    Page {
        id,
        bytes,
        first: children[0].first.clone(),
        last: children[children.len() - 1].last.clone(),
        rows: children.iter().map(|child| child.rows).sum(),
    }
}

fn decode_leaf(bytes: &[u8]) -> Result<Vec<Vec<u8>>, &'static str> {
    let mut cursor = Cursor::new(bytes);
    cursor.magic(b"LXBL")?;
    cursor.take(SCHEMA.len())?;
    let first = cursor.bytes()?.to_vec();
    let last = cursor.bytes()?.to_vec();
    let rows = cursor.u32()?;
    if rows == 0 {
        return Err("empty leaf");
    }
    let prefix = cursor.bytes()?.to_vec();
    let mut slots = Vec::with_capacity(rows);
    for _ in 0..rows {
        slots.push((cursor.u32()?, cursor.u32()?, cursor.u32()?, cursor.u32()?));
    }
    let suffix_len = slots
        .iter()
        .map(|(offset, len, _, _)| offset.checked_add(*len).ok_or("suffix overflow"))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(0);
    let suffixes = cursor.take(suffix_len)?;
    let values = cursor.remaining();
    let mut expected_suffix = 0;
    let mut expected_value = 0;
    let mut keys = Vec::with_capacity(rows);
    for (suffix_offset, suffix_len, value_offset, value_len) in slots {
        if suffix_offset != expected_suffix || value_offset != expected_value {
            return Err("noncanonical slot offsets");
        }
        let mut key = prefix.clone();
        key.extend_from_slice(
            suffixes
                .get(suffix_offset..suffix_offset + suffix_len)
                .ok_or("truncated suffix")?,
        );
        values
            .get(value_offset..value_offset + value_len)
            .ok_or("truncated value")?;
        expected_suffix += suffix_len;
        expected_value += value_len;
        keys.push(key);
    }
    if expected_value != values.len() || keys.first() != Some(&first) || keys.last() != Some(&last)
    {
        return Err("leaf bounds/value mismatch");
    }
    Ok(keys)
}

fn verify_internal(
    bytes: &[u8],
    objects: &BTreeMap<[u8; 32], Vec<u8>>,
) -> Result<(), &'static str> {
    let mut cursor = Cursor::new(bytes);
    cursor.magic(b"LXBI")?;
    cursor.take(SCHEMA.len())?;
    let first = cursor.bytes()?.to_vec();
    let last = cursor.bytes()?.to_vec();
    let children = cursor.u32()?;
    let prefix = cursor.bytes()?.to_vec();
    let mut previous: Option<Vec<u8>> = None;
    for index in 0..children {
        let id: [u8; 32] = cursor.take(32)?.try_into().map_err(|_| "child id")?;
        let mut separator = prefix.clone();
        separator.extend_from_slice(cursor.bytes()?);
        cursor.u32()?;
        if !objects.contains_key(&id)
            || previous
                .as_ref()
                .is_some_and(|previous| previous >= &separator)
            || (index == children - 1 && separator != last)
        {
            return Err("bad authenticated child edge");
        }
        previous = Some(separator);
    }
    cursor.finish()?;
    if first.is_empty() || last.is_empty() {
        return Err("empty internal bounds");
    }
    Ok(())
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

    fn magic(&mut self, expected: &[u8; 4]) -> Result<(), &'static str> {
        if self.take(4)? != expected {
            return Err("wrong page domain");
        }
        Ok(())
    }

    fn u32(&mut self) -> Result<usize, &'static str> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().map_err(|_| "u32")?) as usize)
    }

    fn bytes(&mut self) -> Result<&'a [u8], &'static str> {
        let len = self.u32()?;
        self.take(len)
    }

    fn remaining(&mut self) -> &'a [u8] {
        let remaining = &self.bytes[self.offset..];
        self.offset = self.bytes.len();
        remaining
    }

    fn finish(self) -> Result<(), &'static str> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("trailing page bytes")
        }
    }
}

fn fixture_rows(kind: KeyKind, n: usize) -> Vec<Row> {
    let mut rows = (0..n)
        .map(|ordinal| Row {
            key: fixture_key(kind, ordinal),
            value: fixture_value(ordinal),
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.key.cmp(&right.key));
    assert!(rows.windows(2).all(|pair| pair[0].key < pair[1].key));
    rows
}

fn fixture_key(kind: KeyKind, ordinal: usize) -> Vec<u8> {
    match kind {
        KeyKind::Integer => (ordinal as u64).to_be_bytes().to_vec(),
        KeyKind::Uuid => {
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"EXP-BOUNDARY-14.uuid\0");
            hasher.update(&(ordinal as u64).to_be_bytes());
            hasher.finalize().as_bytes()[..16].to_vec()
        }
        KeyKind::Text => format!("tenant/acme/entity/{ordinal:012}").into_bytes(),
        KeyKind::Composite => {
            let mut key = format!("tenant/{:04}/schema/task/", ordinal % 97).into_bytes();
            key.extend_from_slice(&(ordinal as u64).to_be_bytes());
            key
        }
    }
}

fn fixture_value(ordinal: usize) -> Vec<u8> {
    let mut value = vec![0_u8; 64];
    let mut state = (ordinal as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    for byte in &mut value {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    value
}

fn update_rows_offset(rows: &[Row], d: usize, offset: usize) -> Vec<Row> {
    let mut next = rows.to_vec();
    let stride = (rows.len() / d).max(1);
    for ordinal in 0..d {
        let index = (offset + ordinal * stride) % rows.len();
        next[index].value[0] ^= 0x5a;
    }
    next
}

fn update_rows(rows: &[Row], d: usize) -> Vec<Row> {
    update_rows_offset(rows, d, 0)
}

fn insert_rows(rows: &[Row], d: usize, after: bool) -> Vec<Row> {
    let mut next = rows.to_vec();
    let stride = (rows.len() / d).max(1);
    for ordinal in 0..d {
        let source = &rows[(ordinal * stride).min(rows.len() - 1)];
        let mut key = if after {
            source.key.clone()
        } else if ordinal * stride > 0 {
            rows[ordinal * stride - 1].key.clone()
        } else {
            Vec::new()
        };
        key.push(if after { 0x00 } else { 0xff });
        next.push(Row {
            key,
            value: fixture_value(rows.len() + ordinal),
        });
    }
    next.sort_by(|left, right| left.key.cmp(&right.key));
    assert!(next.windows(2).all(|pair| pair[0].key < pair[1].key));
    next
}

fn shift_rows(rows: &[Row], d: usize) -> Vec<Row> {
    let mut next = rows.to_vec();
    let stride = (rows.len() / d).max(1);
    for ordinal in 0..d {
        let index = (ordinal * stride).min(rows.len() - 1);
        next[index].key.push(0x40);
    }
    next.sort_by(|left, right| left.key.cmp(&right.key));
    assert!(next.windows(2).all(|pair| pair[0].key < pair[1].key));
    next
}

fn object_diff(left: &Tree, right: &Tree) -> (usize, usize) {
    let left_ids = left.objects.keys().copied().collect::<BTreeSet<_>>();
    let changed = right
        .objects
        .iter()
        .filter(|(id, _)| !left_ids.contains(*id))
        .collect::<Vec<_>>();
    (
        changed.len(),
        changed.iter().map(|(_, bytes)| bytes.len()).sum(),
    )
}

fn corruption_controls(tree: &Tree) {
    let page = &tree.levels[0][tree.levels[0].len() / 2];
    let mut corrupt = page.bytes.clone();
    corrupt[0] ^= 1;
    assert!(decode_leaf(&corrupt).is_err());
    let mut truncated = page.bytes.clone();
    truncated.pop();
    assert!(decode_leaf(&truncated).is_err());
    let mut substituted = page.bytes.clone();
    let offset = 4 + SCHEMA.len() + 4;
    substituted[offset] ^= 1;
    assert!(
        decode_leaf(&substituted).is_err() || blake3::hash(&substituted).as_bytes() != &page.id
    );
}

fn adversarial_controls() {
    for policy in Policy::ALL {
        let rows = fixture_rows(KeyKind::Uuid, 10_000);
        let tree = Tree::build(rows, policy);
        for page in &tree.levels[0][..tree.levels[0].len().saturating_sub(1)] {
            assert!(page.bytes.len() <= TARGET + 512);
        }
        tree.verify().expect("adversarial UUID tree verifies");
    }
}

fn insertion_order_control() {
    let canonical = fixture_rows(KeyKind::Composite, 10_000);
    let mut reversed = canonical.clone();
    reversed.reverse();
    reversed.sort_by(|left, right| left.key.cmp(&right.key));
    for policy in Policy::ALL {
        let left = Tree::build(canonical.clone(), policy);
        let right = Tree::build(reversed.clone(), policy);
        assert_eq!(left.root(), right.root());
    }
    let mut duplicate = canonical;
    duplicate.push(duplicate[0].clone());
    duplicate.sort_by(|left, right| left.key.cmp(&right.key));
    assert!(duplicate.windows(2).any(|pair| pair[0].key == pair[1].key));
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

fn shared_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}

fn push_u32(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&u32::try_from(value).expect("u32 model value").to_be_bytes());
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    push_u32(out, bytes.len());
    out.extend_from_slice(bytes);
}

fn permute(value: usize, n: usize) -> usize {
    value.wrapping_mul(2_654_435_761) % n
}
