//! EXP-KEYDIR-10: authenticated in-page key-directory crossover.
//!
//! This is an additive model benchmark. Every alternative is serialized into
//! the same immutable page bytes and therefore covered by the page ObjectId.
//! Fingerprints are negative filters only; a positive lookup always compares
//! the complete reconstructed key.

use std::cmp::Ordering;
use std::hint::black_box;
use std::time::{Duration, Instant};

const RESTART_INTERVAL: usize = 8;
const LEAF_KEYS: usize = 48;
const INTERNAL_KEYS: usize = 64;
const SAMPLES: usize = 7;
const LOOKUPS: usize = 20_000;
const COUNTS: [usize; 4] = [1_000, 10_000, 50_000, 100_000];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Integer,
    Uuid,
    Text,
    Composite,
}

impl Kind {
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
enum Layout {
    Restart,
    Eytzinger,
    Fingerprint8,
    Fingerprint16,
}

impl Layout {
    const ALL: [Self; 4] = [
        Self::Restart,
        Self::Eytzinger,
        Self::Fingerprint8,
        Self::Fingerprint16,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::Restart => "restart_binary",
            Self::Eytzinger => "eytzinger_anchors",
            Self::Fingerprint8 => "fingerprint8",
            Self::Fingerprint16 => "fingerprint16",
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::Restart => 0,
            Self::Eytzinger => 1,
            Self::Fingerprint8 => 2,
            Self::Fingerprint16 => 3,
        }
    }
}

#[derive(Clone)]
struct Directory {
    layout: Layout,
    bytes: Vec<u8>,
    object_id: [u8; 32],
    entries_start: usize,
    restart_offsets: Vec<usize>,
    eytzinger: Vec<usize>,
    fingerprints: Vec<u16>,
    keys: usize,
}

#[derive(Default, Clone, Copy)]
struct SearchStats {
    full_compares: usize,
    reconstructed: usize,
    fingerprint_rejects: usize,
}

impl Directory {
    fn build(keys: &[Vec<u8>], layout: Layout) -> Self {
        assert!(!keys.is_empty());
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        let mut entries = Vec::new();
        let mut restart_offsets = Vec::new();
        let mut previous: &[u8] = &[];
        for (index, key) in keys.iter().enumerate() {
            let shared = if index % RESTART_INTERVAL == 0 {
                restart_offsets.push(entries.len());
                0
            } else {
                shared_prefix(previous, key)
            };
            push_u16(&mut entries, shared);
            push_u16(&mut entries, key.len() - shared);
            entries.extend_from_slice(&key[shared..]);
            previous = key;
        }
        let fingerprints = match layout {
            Layout::Fingerprint8 | Layout::Fingerprint16 => {
                keys.iter().map(|key| fingerprint(key)).collect()
            }
            _ => Vec::new(),
        };
        let mut eytzinger = Vec::new();
        if layout == Layout::Eytzinger {
            eytzinger.resize(restart_offsets.len(), 0);
            let mut next = 0;
            fill_eytzinger(&mut eytzinger, 0, &mut next);
        }

        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"LXKD");
        bytes.push(1);
        bytes.push(layout.tag());
        push_u16(&mut bytes, RESTART_INTERVAL);
        push_u32(&mut bytes, keys.len());
        push_u32(&mut bytes, entries.len());
        push_u32(&mut bytes, restart_offsets.len());
        bytes.extend_from_slice(&entries);
        for offset in &restart_offsets {
            push_u32(&mut bytes, *offset);
        }
        for index in &eytzinger {
            push_u16(&mut bytes, *index);
        }
        match layout {
            Layout::Fingerprint8 => {
                bytes.extend(fingerprints.iter().map(|fingerprint| *fingerprint as u8));
            }
            Layout::Fingerprint16 => {
                for fingerprint in &fingerprints {
                    push_u16(&mut bytes, *fingerprint as usize);
                }
            }
            _ => {}
        }
        let object_id = *blake3::hash(&bytes).as_bytes();
        let directory = Self {
            layout,
            bytes,
            object_id,
            entries_start: 20,
            restart_offsets,
            eytzinger,
            fingerprints,
            keys: keys.len(),
        };
        directory
            .verify(Some(keys))
            .expect("fresh directory verifies");
        directory
    }

    fn verify(&self, expected: Option<&[Vec<u8>]>) -> Result<(), &'static str> {
        if blake3::hash(&self.bytes).as_bytes() != &self.object_id {
            return Err("authenticated directory object id mismatch");
        }
        if self.bytes.get(..4) != Some(b"LXKD") || self.bytes.get(4) != Some(&1) {
            return Err("wrong directory domain/version");
        }
        if self.bytes.get(5) != Some(&self.layout.tag()) {
            return Err("directory layout tag mismatch");
        }
        if read_u16(&self.bytes, 6)? != RESTART_INTERVAL
            || read_u32(&self.bytes, 8)? != self.keys
            || read_u32(&self.bytes, 16)? != self.restart_offsets.len()
        {
            return Err("directory geometry mismatch");
        }
        let entries_len = read_u32(&self.bytes, 12)?;
        if self
            .entries_start
            .checked_add(entries_len)
            .ok_or("directory overflow")?
            > self.bytes.len()
        {
            return Err("truncated directory entries");
        }
        let restart_start = self.entries_start + entries_len;
        let restart_end = restart_start
            .checked_add(self.restart_offsets.len() * 4)
            .ok_or("restart directory overflow")?;
        let eytzinger_end = restart_end
            .checked_add(self.eytzinger.len() * 2)
            .ok_or("Eytzinger directory overflow")?;
        let fingerprint_width = match self.layout {
            Layout::Fingerprint8 => 1,
            Layout::Fingerprint16 => 2,
            _ => 0,
        };
        let expected_end = eytzinger_end
            .checked_add(self.fingerprints.len() * fingerprint_width)
            .ok_or("fingerprint directory overflow")?;
        if expected_end != self.bytes.len() {
            return Err("truncated or trailing directory metadata");
        }
        for (index, expected) in self.restart_offsets.iter().enumerate() {
            if read_u32(&self.bytes, restart_start + index * 4)? != *expected {
                return Err("restart offset bytes mismatch");
            }
        }
        for (index, expected) in self.eytzinger.iter().enumerate() {
            if read_u16(&self.bytes, restart_end + index * 2)? != *expected {
                return Err("Eytzinger bytes mismatch");
            }
        }
        for (index, expected) in self.fingerprints.iter().enumerate() {
            let actual = match self.layout {
                Layout::Fingerprint8 => usize::from(self.bytes[eytzinger_end + index]),
                Layout::Fingerprint16 => read_u16(&self.bytes, eytzinger_end + index * 2)?,
                _ => 0,
            };
            let expected = match self.layout {
                Layout::Fingerprint8 => usize::from(*expected as u8),
                Layout::Fingerprint16 => usize::from(*expected),
                _ => 0,
            };
            if actual != expected {
                return Err("fingerprint bytes mismatch");
            }
        }
        if self.restart_offsets.first() != Some(&0)
            || self
                .restart_offsets
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || self
                .restart_offsets
                .iter()
                .any(|offset| *offset >= entries_len)
        {
            return Err("malformed restart offsets");
        }
        if self.layout == Layout::Eytzinger {
            let mut sorted = self.eytzinger.clone();
            sorted.sort_unstable();
            if sorted != (0..self.restart_offsets.len()).collect::<Vec<_>>() {
                return Err("malformed Eytzinger permutation");
            }
        }
        if matches!(self.layout, Layout::Fingerprint8 | Layout::Fingerprint16)
            && self.fingerprints.len() != self.keys
        {
            return Err("fingerprint count mismatch");
        }
        let mut previous: Option<Vec<u8>> = None;
        let mut scratch = Vec::new();
        for index in 0..self.keys {
            self.reconstruct(index, &mut scratch)?;
            let key = scratch.clone();
            if previous
                .as_ref()
                .is_some_and(|previous| previous.as_slice() >= key.as_slice())
            {
                return Err("duplicate or unordered directory key");
            }
            if let Some(expected) = expected
                && expected.get(index).map(Vec::as_slice) != Some(key.as_slice())
            {
                return Err("directory key substitution");
            }
            if matches!(self.layout, Layout::Fingerprint8 | Layout::Fingerprint16)
                && self.fingerprints[index] != fingerprint(&key)
            {
                return Err("fingerprint/key mismatch");
            }
            previous = Some(key);
        }
        Ok(())
    }

    fn search(
        &self,
        query: &[u8],
        scratch: &mut Vec<u8>,
        stats: &mut SearchStats,
    ) -> Option<usize> {
        let restart = match self.layout {
            Layout::Eytzinger => self.eytzinger_floor(query, scratch, stats),
            _ => self.binary_restart_floor(query, scratch, stats),
        };
        let start = restart * RESTART_INTERVAL;
        let end = (start + RESTART_INTERVAL).min(self.keys);
        let query_fingerprint = match self.layout {
            Layout::Fingerprint8 | Layout::Fingerprint16 => fingerprint(query),
            _ => 0,
        };
        scratch.clear();
        for index in start..end {
            self.reconstruct(index, scratch).ok()?;
            stats.reconstructed += 1;
            let fp_matches = match self.layout {
                Layout::Fingerprint8 => self.fingerprints[index] as u8 == query_fingerprint as u8,
                Layout::Fingerprint16 => self.fingerprints[index] == query_fingerprint,
                _ => true,
            };
            if !fp_matches {
                stats.fingerprint_rejects += 1;
            }
            stats.full_compares += 1;
            match scratch.as_slice().cmp(query) {
                Ordering::Less => {}
                Ordering::Equal => return Some(index),
                Ordering::Greater => return None,
            }
        }
        None
    }

    fn binary_restart_floor(
        &self,
        query: &[u8],
        scratch: &mut Vec<u8>,
        stats: &mut SearchStats,
    ) -> usize {
        let mut low = 0;
        let mut high = self.restart_offsets.len();
        while low < high {
            let middle = (low + high) / 2;
            self.restart_key(middle, scratch).expect("verified restart");
            stats.reconstructed += 1;
            stats.full_compares += 1;
            if scratch.as_slice() <= query {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low.saturating_sub(1)
    }

    fn eytzinger_floor(
        &self,
        query: &[u8],
        scratch: &mut Vec<u8>,
        stats: &mut SearchStats,
    ) -> usize {
        let mut heap = 0;
        let mut floor = 0;
        while heap < self.eytzinger.len() {
            let restart = self.eytzinger[heap];
            self.restart_key(restart, scratch)
                .expect("verified restart");
            stats.reconstructed += 1;
            stats.full_compares += 1;
            if scratch.as_slice() <= query {
                floor = restart;
                heap = heap * 2 + 2;
            } else {
                heap = heap * 2 + 1;
            }
        }
        floor
    }

    fn restart_key(&self, restart: usize, scratch: &mut Vec<u8>) -> Result<(), &'static str> {
        self.reconstruct(restart * RESTART_INTERVAL, scratch)
    }

    fn reconstruct(&self, index: usize, scratch: &mut Vec<u8>) -> Result<(), &'static str> {
        if index >= self.keys {
            return Err("directory index out of bounds");
        }
        let restart = index / RESTART_INTERVAL;
        let mut cursor = self.entries_start + self.restart_offsets[restart];
        scratch.clear();
        for ordinal in restart * RESTART_INTERVAL..=index {
            let shared = read_u16(&self.bytes, cursor)?;
            let suffix = read_u16(&self.bytes, cursor + 2)?;
            cursor += 4;
            if shared > scratch.len() || cursor + suffix > self.entries_end() {
                return Err("malformed prefix-compressed key");
            }
            if ordinal % RESTART_INTERVAL == 0 && shared != 0 {
                return Err("restart key has shared prefix");
            }
            scratch.truncate(shared);
            scratch.extend_from_slice(&self.bytes[cursor..cursor + suffix]);
            cursor += suffix;
        }
        Ok(())
    }

    fn entries_end(&self) -> usize {
        self.entries_start + read_u32(&self.bytes, 12).expect("verified header")
    }
}

#[derive(Clone)]
struct PageTree {
    levels: Vec<Vec<Directory>>,
    keys: Vec<Vec<u8>>,
}

impl PageTree {
    fn build(keys: Vec<Vec<u8>>, layout: Layout) -> Self {
        let leaves = keys
            .chunks(LEAF_KEYS)
            .map(|chunk| Directory::build(chunk, layout))
            .collect::<Vec<_>>();
        let mut levels = vec![leaves];
        let mut separators = keys
            .chunks(LEAF_KEYS)
            .map(|chunk| chunk.last().expect("leaf key").clone())
            .collect::<Vec<_>>();
        while separators.len() > 1 {
            let level = separators
                .chunks(INTERNAL_KEYS)
                .map(|chunk| Directory::build(chunk, layout))
                .collect::<Vec<_>>();
            separators = separators
                .chunks(INTERNAL_KEYS)
                .map(|chunk| chunk.last().expect("separator").clone())
                .collect();
            levels.push(level);
        }
        Self { levels, keys }
    }

    fn point(&self, query: &[u8], scratch: &mut Vec<u8>, stats: &mut SearchStats) -> bool {
        let mut page = 0;
        for level in (1..self.levels.len()).rev() {
            let directory = &self.levels[level][page];
            let slot = directory
                .search(query, scratch, stats)
                .unwrap_or_else(|| insertion_slot(directory, query, scratch, stats));
            page = page * INTERNAL_KEYS + slot.min(directory.keys - 1);
        }
        let leaf = &self.levels[0][page.min(self.levels[0].len() - 1)];
        leaf.search(query, scratch, stats).is_some()
    }

    fn authenticated_bytes(&self) -> usize {
        self.levels
            .iter()
            .flat_map(|level| level.iter())
            .map(|directory| directory.bytes.len())
            .sum()
    }
}

fn insertion_slot(
    directory: &Directory,
    query: &[u8],
    _scratch: &mut Vec<u8>,
    stats: &mut SearchStats,
) -> usize {
    let mut previous = Vec::new();
    for index in 0..directory.keys {
        directory
            .reconstruct(index, &mut previous)
            .expect("verified key");
        stats.reconstructed += 1;
        stats.full_compares += 1;
        if query <= previous.as_slice() {
            return index;
        }
    }
    directory.keys - 1
}

fn main() {
    println!(
        "key_kind,n,layout,mode,operation,p50_ns,p95_ns,auth_bytes,bytes_delta_pct,full_compares,reconstructed,fingerprint_rejects,found"
    );
    for kind in Kind::ALL {
        for n in COUNTS {
            let keys = fixture_keys(kind, n);
            let baseline = PageTree::build(keys.clone(), Layout::Restart);
            let baseline_bytes = baseline.authenticated_bytes();
            for layout in Layout::ALL {
                let tree = PageTree::build(keys.clone(), layout);
                corruption_controls(&tree, layout);
                for cold in [false, true] {
                    benchmark_points(kind, n, layout, &tree, baseline_bytes, cold);
                }
                benchmark_nonpoint(kind, n, layout, &tree, baseline_bytes);
            }
        }
    }
    insertion_order_control();
}

fn benchmark_points(
    kind: Kind,
    n: usize,
    layout: Layout,
    tree: &PageTree,
    baseline_bytes: usize,
    cold: bool,
) {
    let mode = if cold { "cold" } else { "hot" };
    for present in [true, false] {
        let operation = if present {
            "point_present"
        } else {
            "point_missing"
        };
        let mut samples = Vec::new();
        let mut aggregate = SearchStats::default();
        let mut found = 0;
        let mut cold_buffer = vec![0_u8; 16 << 20];
        let missing_queries = (!present).then(|| {
            tree.keys
                .iter()
                .map(|key| missing_key(key))
                .collect::<Vec<_>>()
        });
        for sample in 0..SAMPLES {
            let mut scratch = Vec::with_capacity(128);
            let mut stats = SearchStats::default();
            let started = Instant::now();
            for iteration in 0..LOOKUPS {
                let ordinal = permute(iteration + sample * 17, n);
                let query = if present {
                    tree.keys[ordinal].as_slice()
                } else {
                    missing_queries.as_ref().expect("missing queries")[ordinal].as_slice()
                };
                found += usize::from(tree.point(query, &mut scratch, &mut stats));
                if cold && iteration % 64 == 0 {
                    let offset = (iteration * 4099) % cold_buffer.len();
                    cold_buffer[offset] = cold_buffer[offset].wrapping_add(1);
                    black_box(cold_buffer[offset]);
                }
            }
            samples.push(started.elapsed() / LOOKUPS as u32);
            aggregate.full_compares += stats.full_compares;
            aggregate.reconstructed += stats.reconstructed;
            aggregate.fingerprint_rejects += stats.fingerprint_rejects;
        }
        samples.sort_unstable();
        print_result(
            kind,
            n,
            layout,
            mode,
            operation,
            samples[SAMPLES / 2],
            samples[SAMPLES - 1],
            tree.authenticated_bytes(),
            baseline_bytes,
            aggregate,
            found,
        );
    }
}

fn benchmark_nonpoint(
    kind: Kind,
    n: usize,
    layout: Layout,
    tree: &PageTree,
    baseline_bytes: usize,
) {
    let range_start = n / 3;
    let started = Instant::now();
    let mut digest = blake3::Hasher::new();
    for key in &tree.keys[range_start..(range_start + 100).min(n)] {
        digest.update(key);
    }
    let range = started.elapsed();
    black_box(digest.finalize());

    let started = Instant::now();
    let mut digest = blake3::Hasher::new();
    for key in &tree.keys {
        digest.update(key);
    }
    let scan = started.elapsed();
    black_box(digest.finalize());

    let mut changed = tree.keys.clone();
    changed[n / 2].push(0x7f);
    changed.sort();
    let started = Instant::now();
    let updated = PageTree::build(changed, layout);
    let update = started.elapsed();
    let changed_bytes = changed_page_bytes(tree, &updated);

    for (operation, elapsed, bytes) in [
        ("range100", range, tree.authenticated_bytes()),
        ("full_scan", scan, tree.authenticated_bytes()),
        ("d1_update", update, changed_bytes),
    ] {
        print_result(
            kind,
            n,
            layout,
            "hot",
            operation,
            elapsed,
            elapsed,
            bytes,
            baseline_bytes,
            SearchStats::default(),
            0,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn print_result(
    kind: Kind,
    n: usize,
    layout: Layout,
    mode: &str,
    operation: &str,
    p50: Duration,
    p95: Duration,
    bytes: usize,
    baseline_bytes: usize,
    stats: SearchStats,
    found: usize,
) {
    println!(
        "{},{},{},{},{},{},{},{},{:.3},{},{},{},{}",
        kind.label(),
        n,
        layout.label(),
        mode,
        operation,
        p50.as_nanos(),
        p95.as_nanos(),
        bytes,
        (bytes as f64 / baseline_bytes as f64 - 1.0) * 100.0,
        stats.full_compares,
        stats.reconstructed,
        stats.fingerprint_rejects,
        found,
    );
}

fn changed_page_bytes(before: &PageTree, after: &PageTree) -> usize {
    let old = before
        .levels
        .iter()
        .flat_map(|level| level.iter())
        .map(|directory| directory.object_id)
        .collect::<std::collections::BTreeSet<_>>();
    after
        .levels
        .iter()
        .flat_map(|level| level.iter())
        .filter(|directory| !old.contains(&directory.object_id))
        .map(|directory| directory.bytes.len())
        .sum()
}

fn corruption_controls(tree: &PageTree, layout: Layout) {
    let directory = &tree.levels[0][tree.levels[0].len() / 2];
    directory.verify(None).expect("control directory verifies");
    let mut corrupted = directory.clone();
    corrupted.bytes[corrupted.entries_start + 1] ^= 1;
    assert!(corrupted.verify(None).is_err());

    let mut truncated = directory.clone();
    truncated.bytes.pop();
    truncated.object_id = *blake3::hash(&truncated.bytes).as_bytes();
    assert!(truncated.verify(None).is_err());

    if matches!(layout, Layout::Fingerprint8 | Layout::Fingerprint16) {
        let mut false_positive = directory.clone();
        false_positive.fingerprints[0] = false_positive.fingerprints[1];
        assert!(false_positive.verify(None).is_err());
        let query = missing_key(&tree.keys[tree.keys.len() / 2]);
        let mut scratch = Vec::new();
        let mut stats = SearchStats::default();
        assert!(!tree.point(&query, &mut scratch, &mut stats));
    }
}

fn insertion_order_control() {
    let canonical = fixture_keys(Kind::Composite, 1_000);
    let mut reversed = canonical.clone();
    reversed.reverse();
    reversed.sort();
    for layout in Layout::ALL {
        let left = PageTree::build(canonical.clone(), layout);
        let right = PageTree::build(reversed.clone(), layout);
        let left_ids = left
            .levels
            .iter()
            .flat_map(|level| level.iter().map(|directory| directory.object_id))
            .collect::<Vec<_>>();
        let right_ids = right
            .levels
            .iter()
            .flat_map(|level| level.iter().map(|directory| directory.object_id))
            .collect::<Vec<_>>();
        assert_eq!(left_ids, right_ids);
        let mut duplicate = canonical.clone();
        duplicate.push(canonical[0].clone());
        duplicate.sort();
        assert!(duplicate.windows(2).any(|pair| pair[0] == pair[1]));
    }
}

fn fixture_keys(kind: Kind, n: usize) -> Vec<Vec<u8>> {
    let mut keys = (0..n)
        .map(|ordinal| match kind {
            Kind::Integer => (ordinal as u64).to_be_bytes().to_vec(),
            Kind::Uuid => {
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"EXP-KEYDIR-10.uuid\0");
                hasher.update(&(ordinal as u64).to_be_bytes());
                hasher.finalize().as_bytes()[..16].to_vec()
            }
            Kind::Text => format!("tenant/acme/entity/{ordinal:012}").into_bytes(),
            Kind::Composite => {
                let mut key = format!("tenant/{:04}/schema/task/", ordinal % 97).into_bytes();
                key.extend_from_slice(&(ordinal as u64).to_be_bytes());
                key
            }
        })
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), n);
    keys
}

fn missing_key(key: &[u8]) -> Vec<u8> {
    let mut missing = key.to_vec();
    missing.push(0x80);
    missing
}

fn permute(value: usize, n: usize) -> usize {
    value.wrapping_mul(2_654_435_761) % n
}

fn fingerprint(key: &[u8]) -> u16 {
    let hash = blake3::hash(key);
    u16::from_be_bytes([hash.as_bytes()[0], hash.as_bytes()[1]])
}

fn fill_eytzinger(output: &mut [usize], heap: usize, next: &mut usize) {
    if heap >= output.len() {
        return;
    }
    fill_eytzinger(output, heap * 2 + 1, next);
    output[heap] = *next;
    *next += 1;
    fill_eytzinger(output, heap * 2 + 2, next);
}

fn shared_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}

fn push_u16(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&u16::try_from(value).expect("u16 model value").to_be_bytes());
}

fn push_u32(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&u32::try_from(value).expect("u32 model value").to_be_bytes());
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<usize, &'static str> {
    Ok(u16::from_be_bytes(
        bytes
            .get(offset..offset + 2)
            .ok_or("truncated u16")?
            .try_into()
            .map_err(|_| "truncated u16")?,
    ) as usize)
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<usize, &'static str> {
    Ok(u32::from_be_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or("truncated u32")?
            .try_into()
            .map_err(|_| "truncated u32")?,
    ) as usize)
}
