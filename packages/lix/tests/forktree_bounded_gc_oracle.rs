//! Independent executable model for the accepted ForkTree bounded-GC contract.
//!
//! This source is deliberately standalone: it imports no Lix production code,
//! owns no production storage token, and can be compiled directly with rustc.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Instant;

const ROOT_PAGE: usize = 256;
const UNTRACKED_PAGE: usize = 1;
const EDGE_PAGE: usize = 256;
const TRAVERSAL_BATCH: usize = 128;
const MARK_PACK_MAX: usize = 4096;
const QUEUE_PACK_MAX: usize = 1024;
const SWEEP_PAGE: usize = 256;
const DELETE_BATCH: usize = 256;
const PEAK_ID_LIMIT: usize = 6000;
const PEAK_METADATA_LIMIT: usize = 512 * 1024;
const MAGIC: &[u8; 8] = b"FTGCMOD2";

struct CountingAllocator;

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, AtomicOrdering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, AtomicOrdering::Relaxed);
        // SAFETY: the exact allocation request is forwarded to System.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the pointer and its original layout are forwarded to System.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, AtomicOrdering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, AtomicOrdering::Relaxed);
        // SAFETY: the exact reallocation request is forwarded to System.
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Clone, Copy)]
struct AllocationSnapshot {
    calls: u64,
    bytes: u64,
}

fn allocations() -> AllocationSnapshot {
    AllocationSnapshot {
        calls: ALLOC_CALLS.load(AtomicOrdering::Relaxed),
        bytes: ALLOC_BYTES.load(AtomicOrdering::Relaxed),
    }
}

fn allocation_delta(start: AllocationSnapshot, end: AllocationSnapshot) -> AllocationSnapshot {
    AllocationSnapshot {
        calls: end.calls.saturating_sub(start.calls),
        bytes: end.bytes.saturating_sub(start.bytes),
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ObjectId([u8; 32]);

impl ObjectId {
    fn deterministic(seed: u64, adversarial_prefix: bool) -> Self {
        let mut output = [0_u8; 32];
        let mut value = seed ^ 0x9e37_79b9_7f4a_7c15;
        for chunk in output.chunks_exact_mut(8) {
            value = splitmix64(value);
            chunk.copy_from_slice(&value.to_be_bytes());
        }
        if adversarial_prefix {
            output[..8].fill(0x5a);
            output[24..].copy_from_slice(&seed.to_be_bytes());
        }
        Self(output)
    }

    fn zero() -> Self {
        Self([0; 32])
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
enum Domain {
    State = 1,
    Commit = 2,
    BlobManifest = 3,
    BlobChunk = 4,
    UploadProgress = 5,
    UploadPart = 6,
    Snapshot = 7,
}

impl Domain {
    fn decode(value: u16) -> Result<Self, String> {
        match value {
            1 => Ok(Self::State),
            2 => Ok(Self::Commit),
            3 => Ok(Self::BlobManifest),
            4 => Ok(Self::BlobChunk),
            5 => Ok(Self::UploadProgress),
            6 => Ok(Self::UploadPart),
            7 => Ok(Self::Snapshot),
            _ => Err(format!("unknown object domain {value}")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Object {
    domain: Domain,
    edges: Vec<(ObjectId, Domain)>,
    payload_bytes: u32,
    valid: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootRole {
    Global,
    Branch,
    Checkpoint,
    Recovery,
    Undo,
    Redo,
    Upload,
    Plugin,
}

impl RootRole {
    fn encode(self) -> u8 {
        match self {
            Self::Global => 0,
            Self::Branch => 1,
            Self::Checkpoint => 2,
            Self::Recovery => 3,
            Self::Undo => 4,
            Self::Redo => 5,
            Self::Upload => 6,
            Self::Plugin => 7,
        }
    }

    fn decode(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(Self::Global),
            1 => Ok(Self::Branch),
            2 => Ok(Self::Checkpoint),
            3 => Ok(Self::Recovery),
            4 => Ok(Self::Undo),
            5 => Ok(Self::Redo),
            6 => Ok(Self::Upload),
            7 => Ok(Self::Plugin),
            _ => Err(format!("unknown root role {value}")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Root {
    role: RootRole,
    object: ObjectId,
    domain: Domain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MarkEntry {
    id: ObjectId,
    domain: Domain,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MarkNode {
    Pack {
        depth: u8,
        entries: Vec<MarkEntry>,
    },
    Branch {
        depth: u8,
        children: BTreeMap<u8, Box<MarkNode>>,
    },
    Missing,
}

impl MarkNode {
    fn empty() -> Self {
        Self::Pack {
            depth: 0,
            entries: Vec::new(),
        }
    }

    fn insert(&mut self, entry: MarkEntry, io: &mut IoStats) -> Result<bool, String> {
        self.insert_at(entry, 0, io)
    }

    fn insert_batch(
        &mut self,
        mut entries: Vec<MarkEntry>,
        io: &mut IoStats,
    ) -> Result<Vec<MarkEntry>, String> {
        entries.sort_by_key(|entry| entry.id);
        for pair in entries.windows(2) {
            if pair[0].id == pair[1].id && pair[0].domain != pair[1].domain {
                return Err("batch contains conflicting domains for one ID".into());
            }
        }
        entries.dedup();
        let mut inserted = Vec::new();
        for entry in entries {
            let mut probe_io = IoStats::default();
            if !self.contains(entry.id, entry.domain, &mut probe_io)? {
                inserted.push(entry);
            }
        }
        let mut mutation_io = IoStats::default();
        for entry in &inserted {
            if !self.insert(*entry, &mut mutation_io)? {
                return Err("new mark disappeared during batch insertion".into());
            }
        }
        let mut touched = BTreeMap::<Vec<u8>, usize>::new();
        for entry in &inserted {
            self.collect_path_sizes(entry.id, &mut touched)?;
        }
        for size in touched.values() {
            io.read(*size as u64);
            io.write(*size as u64);
        }
        Ok(inserted)
    }

    fn collect_path_sizes(
        &self,
        id: ObjectId,
        touched: &mut BTreeMap<Vec<u8>, usize>,
    ) -> Result<(), String> {
        let mut node = self;
        let mut path = Vec::new();
        let mut depth = 0_usize;
        loop {
            touched.insert(path.clone(), node.encoded_size());
            match node {
                Self::Missing => return Err("missing radix pack/node".into()),
                Self::Pack { .. } => return Ok(()),
                Self::Branch { children, .. } => {
                    if depth >= 32 {
                        return Err("radix path exceeds ObjectId".into());
                    }
                    let byte = id.0[depth];
                    path.push(byte);
                    node = children.get(&byte).ok_or("inserted mark path is missing")?;
                    depth += 1;
                }
            }
        }
    }

    fn insert_at(
        &mut self,
        entry: MarkEntry,
        expected_depth: usize,
        io: &mut IoStats,
    ) -> Result<bool, String> {
        io.read(self.encoded_size() as u64);
        match self {
            Self::Missing => Err("missing radix pack/node".into()),
            Self::Pack { depth, entries } => {
                if usize::from(*depth) != expected_depth {
                    return Err("mark pack depth mismatch".into());
                }
                match entries.binary_search_by_key(&entry.id, |value| value.id) {
                    Ok(index) => {
                        if entries[index].domain != entry.domain {
                            return Err("duplicate mark has conflicting domain".into());
                        }
                        Ok(false)
                    }
                    Err(index) => {
                        entries.insert(index, entry);
                        if entries.len() > MARK_PACK_MAX {
                            if expected_depth >= 32 {
                                return Err("mark pack overflow at terminal radix depth".into());
                            }
                            let old = std::mem::take(entries);
                            let mut children = BTreeMap::<u8, Box<MarkNode>>::new();
                            for value in old {
                                let key = value.id.0[expected_depth];
                                let child = children.entry(key).or_insert_with(|| {
                                    Box::new(Self::Pack {
                                        depth: u8::try_from(expected_depth + 1)
                                            .expect("ObjectId radix depth fits u8"),
                                        entries: Vec::new(),
                                    })
                                });
                                child.insert_at(value, expected_depth + 1, io)?;
                            }
                            *self = Self::Branch {
                                depth: u8::try_from(expected_depth)
                                    .expect("ObjectId radix depth fits u8"),
                                children,
                            };
                        }
                        io.write(self.encoded_size() as u64);
                        Ok(true)
                    }
                }
            }
            Self::Branch { depth, children } => {
                if usize::from(*depth) != expected_depth || expected_depth >= 32 {
                    return Err("mark radix branch depth mismatch".into());
                }
                let key = entry.id.0[expected_depth];
                let child = children.entry(key).or_insert_with(|| {
                    Box::new(Self::Pack {
                        depth: u8::try_from(expected_depth + 1)
                            .expect("ObjectId radix depth fits u8"),
                        entries: Vec::new(),
                    })
                });
                let inserted = child.insert_at(entry, expected_depth + 1, io)?;
                if inserted {
                    io.write(self.encoded_size() as u64);
                }
                Ok(inserted)
            }
        }
    }

    fn contains(&self, id: ObjectId, expected: Domain, io: &mut IoStats) -> Result<bool, String> {
        let mut node = self;
        let mut depth = 0_usize;
        loop {
            io.read(node.encoded_size() as u64);
            match node {
                Self::Missing => return Err("missing radix pack/node".into()),
                Self::Pack {
                    depth: stored,
                    entries,
                } => {
                    if usize::from(*stored) != depth {
                        return Err("mark pack depth mismatch".into());
                    }
                    return match entries.binary_search_by_key(&id, |value| value.id) {
                        Ok(index) if entries[index].domain == expected => Ok(true),
                        Ok(_) => Err("mark domain mismatch".into()),
                        Err(_) => Ok(false),
                    };
                }
                Self::Branch {
                    depth: stored,
                    children,
                } => {
                    if usize::from(*stored) != depth || depth >= 32 {
                        return Err("mark radix branch depth mismatch".into());
                    }
                    let Some(child) = children.get(&id.0[depth]) else {
                        return Ok(false);
                    };
                    node = child;
                    depth += 1;
                }
            }
        }
    }

    fn validate(&self) -> Result<MarkMetrics, String> {
        fn walk(
            node: &MarkNode,
            expected_depth: usize,
            lower: Option<ObjectId>,
            upper: Option<ObjectId>,
            metrics: &mut MarkMetrics,
        ) -> Result<(), String> {
            metrics.nodes += 1;
            match node {
                MarkNode::Missing => Err("missing radix pack/node".into()),
                MarkNode::Pack { depth, entries } => {
                    if usize::from(*depth) != expected_depth || entries.len() > MARK_PACK_MAX {
                        return Err("invalid mark pack depth/size".into());
                    }
                    if entries.windows(2).any(|pair| pair[0].id >= pair[1].id) {
                        return Err("mark pack IDs are duplicate or misordered".into());
                    }
                    if entries.iter().any(|entry| {
                        entry.id == ObjectId::zero()
                            || lower.is_some_and(|value| entry.id < value)
                            || upper.is_some_and(|value| entry.id >= value)
                    }) {
                        return Err("mark pack entry is outside its radix range".into());
                    }
                    metrics.entries += entries.len();
                    metrics.max_pack = metrics.max_pack.max(entries.len());
                    Ok(())
                }
                MarkNode::Branch { depth, children } => {
                    if usize::from(*depth) != expected_depth
                        || expected_depth >= 32
                        || children.is_empty()
                        || children.len() > 256
                    {
                        return Err("invalid radix branch depth/fanout".into());
                    }
                    metrics.max_children = metrics.max_children.max(children.len());
                    for (byte, child) in children {
                        let mut low = lower.unwrap_or(ObjectId([0; 32]));
                        low.0[expected_depth] = *byte;
                        let high = if *byte == u8::MAX {
                            upper
                        } else {
                            let mut value = low;
                            value.0[expected_depth] = byte.saturating_add(1);
                            for suffix in &mut value.0[expected_depth + 1..] {
                                *suffix = 0;
                            }
                            Some(value)
                        };
                        walk(child, expected_depth + 1, Some(low), high, metrics)?;
                    }
                    Ok(())
                }
            }
        }

        let mut metrics = MarkMetrics::default();
        walk(self, 0, None, None, &mut metrics)?;
        Ok(metrics)
    }

    fn encoded_size(&self) -> usize {
        match self {
            Self::Missing => 1,
            Self::Pack { entries, .. } => 8 + entries.len() * 34,
            Self::Branch { children, .. } => 40 + children.len() * 32,
        }
    }

    fn iter(&self) -> MarkIter<'_> {
        MarkIter::new(self)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct MarkMetrics {
    nodes: usize,
    entries: usize,
    max_pack: usize,
    max_children: usize,
}

enum MarkFrame<'a> {
    Pack {
        entries: &'a [MarkEntry],
        index: usize,
    },
    Branch(std::collections::btree_map::Iter<'a, u8, Box<MarkNode>>),
}

struct MarkIter<'a> {
    stack: Vec<MarkFrame<'a>>,
}

impl<'a> MarkIter<'a> {
    fn new(root: &'a MarkNode) -> Self {
        let mut value = Self { stack: Vec::new() };
        value.push_node(root);
        value
    }

    fn push_node(&mut self, node: &'a MarkNode) {
        match node {
            MarkNode::Pack { entries, .. } => {
                self.stack.push(MarkFrame::Pack { entries, index: 0 });
            }
            MarkNode::Branch { children, .. } => {
                self.stack.push(MarkFrame::Branch(children.iter()));
            }
            MarkNode::Missing => {}
        }
    }
}

impl Iterator for MarkIter<'_> {
    type Item = MarkEntry;

    fn next(&mut self) -> Option<Self::Item> {
        enum Action<'a> {
            Yield(MarkEntry),
            Descend(&'a MarkNode),
            Pop,
        }

        loop {
            let action = match self.stack.last_mut()? {
                MarkFrame::Pack { entries, index } => {
                    if let Some(value) = entries.get(*index) {
                        *index += 1;
                        Action::Yield(*value)
                    } else {
                        Action::Pop
                    }
                }
                MarkFrame::Branch(iter) => match iter.next() {
                    Some((_, child)) => Action::Descend(child),
                    None => Action::Pop,
                },
            };
            match action {
                Action::Yield(value) => return Some(value),
                Action::Descend(child) => self.push_node(child),
                Action::Pop => {
                    self.stack.pop();
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueueClaim {
    sequence: u64,
    id: ObjectId,
    domain: Domain,
    edge_offset: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueuePack {
    first_sequence: u64,
    entries: Vec<QueueClaim>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PersistedQueue {
    packs: BTreeMap<u64, Option<QueuePack>>,
    pop_sequence: u64,
    push_sequence: u64,
}

impl PersistedQueue {
    fn new() -> Self {
        Self {
            packs: BTreeMap::new(),
            pop_sequence: 0,
            push_sequence: 0,
        }
    }

    fn append(
        &mut self,
        id: ObjectId,
        domain: Domain,
        edge_offset: usize,
        io: &mut IoStats,
    ) -> Result<(), String> {
        let sequence = self.push_sequence;
        let pack_number = sequence / QUEUE_PACK_MAX as u64;
        let first_sequence = pack_number * QUEUE_PACK_MAX as u64;
        let slot = self.packs.entry(pack_number).or_insert_with(|| {
            Some(QueuePack {
                first_sequence,
                entries: Vec::new(),
            })
        });
        let pack = slot.as_mut().ok_or("queue pack is missing")?;
        io.read(queue_pack_size(pack) as u64);
        if pack.entries.len() >= QUEUE_PACK_MAX
            || sequence != first_sequence + pack.entries.len() as u64
        {
            return Err("queue append would violate pack bounds/order".into());
        }
        pack.entries.push(QueueClaim {
            sequence,
            id,
            domain,
            edge_offset: u32::try_from(edge_offset)
                .map_err(|_| "edge continuation exceeds u32".to_string())?,
        });
        self.push_sequence += 1;
        io.write(queue_pack_size(pack) as u64);
        Ok(())
    }

    fn append_batch(
        &mut self,
        claims: &[(ObjectId, Domain, usize)],
        io: &mut IoStats,
    ) -> Result<(), String> {
        let first_sequence = self.push_sequence;
        let mut mutation_io = IoStats::default();
        for (id, domain, edge_offset) in claims {
            self.append(*id, *domain, *edge_offset, &mut mutation_io)?;
        }
        let mut pack_number = first_sequence / QUEUE_PACK_MAX as u64;
        let last = self.push_sequence.saturating_sub(1) / QUEUE_PACK_MAX as u64;
        while !claims.is_empty() && pack_number <= last {
            let pack = self
                .packs
                .get(&pack_number)
                .and_then(Option::as_ref)
                .ok_or("appended queue pack is missing")?;
            let size = queue_pack_size(pack) as u64;
            io.read(size);
            io.write(size);
            pack_number += 1;
        }
        Ok(())
    }

    fn pop_batch(&mut self, io: &mut IoStats) -> Result<Vec<QueueClaim>, String> {
        let count = usize::try_from(self.push_sequence - self.pop_sequence)
            .unwrap_or(usize::MAX)
            .min(TRAVERSAL_BATCH);
        let mut output = Vec::with_capacity(count);
        while output.len() < count {
            let pack_number = self.pop_sequence / QUEUE_PACK_MAX as u64;
            let offset = usize::try_from(self.pop_sequence % QUEUE_PACK_MAX as u64)
                .expect("queue offset fits usize");
            let pack = self
                .packs
                .get(&pack_number)
                .ok_or("queue pack index is missing")?
                .as_ref()
                .ok_or("queue pack object is missing")?;
            io.read(queue_pack_size(pack) as u64);
            let take = (count - output.len()).min(pack.entries.len().saturating_sub(offset));
            if take == 0 {
                return Err("queue cursor points outside pack".into());
            }
            for claim in &pack.entries[offset..offset + take] {
                if claim.sequence != self.pop_sequence {
                    return Err("queue sequence is missing or duplicate".into());
                }
                output.push(*claim);
                self.pop_sequence += 1;
            }
        }
        Ok(output)
    }

    fn is_empty(&self) -> bool {
        self.pop_sequence == self.push_sequence
    }

    fn validate(&self) -> Result<QueueMetrics, String> {
        if self.pop_sequence > self.push_sequence {
            return Err("queue pop cursor exceeds push cursor".into());
        }
        let mut expected = 0_u64;
        let mut metrics = QueueMetrics::default();
        for (pack_number, slot) in &self.packs {
            let pack = slot.as_ref().ok_or("queue pack object is missing")?;
            if pack.first_sequence != pack_number * QUEUE_PACK_MAX as u64
                || pack.entries.is_empty()
                || pack.entries.len() > QUEUE_PACK_MAX
            {
                return Err("invalid queue pack header/size".into());
            }
            for claim in &pack.entries {
                if claim.sequence != expected || claim.id == ObjectId::zero() {
                    return Err("queue claim sequence/ID is invalid".into());
                }
                expected += 1;
            }
            metrics.entries += pack.entries.len();
            metrics.max_pack = metrics.max_pack.max(pack.entries.len());
            metrics.packs += 1;
        }
        if expected != self.push_sequence {
            return Err("queue packs do not cover the push cursor".into());
        }
        Ok(metrics)
    }

    fn encoded_size(&self) -> usize {
        24 + self
            .packs
            .values()
            .map(|value| value.as_ref().map_or(1, queue_pack_size))
            .sum::<usize>()
    }
}

fn queue_pack_size(pack: &QueuePack) -> usize {
    16 + pack.entries.len() * (8 + 32 + 2 + 4)
}

#[derive(Clone, Copy, Debug, Default)]
struct QueueMetrics {
    packs: usize,
    entries: usize,
    max_pack: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    RootSelectors,
    RootUntracked,
    Traverse,
    Sweep,
    Cleanup,
}

impl Phase {
    fn encode(self) -> u8 {
        match self {
            Self::RootSelectors => 0,
            Self::RootUntracked => 1,
            Self::Traverse => 2,
            Self::Sweep => 3,
            Self::Cleanup => 4,
        }
    }

    fn decode(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(Self::RootSelectors),
            1 => Ok(Self::RootUntracked),
            2 => Ok(Self::Traverse),
            3 => Ok(Self::Sweep),
            4 => Ok(Self::Cleanup),
            _ => Err(format!("invalid GC phase {value}")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Progress {
    cycle_id: [u8; 16],
    phase: Phase,
    expected_epoch: u64,
    root_cursor: usize,
    untracked_cursor: usize,
    sweep_after: Option<ObjectId>,
    mark: MarkNode,
    queue: PersistedQueue,
    marked_count: u64,
    validated_count: u64,
    reclaimed_count: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct IoStats {
    read_calls: u64,
    read_bytes: u64,
    write_calls: u64,
    write_bytes: u64,
    delete_calls: u64,
    delete_ids: u64,
    checkpoint_bytes: u64,
}

impl IoStats {
    fn read(&mut self, bytes: u64) {
        self.read_calls += 1;
        self.read_bytes += bytes;
    }

    fn write(&mut self, bytes: u64) {
        self.write_calls += 1;
        self.write_bytes += bytes;
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct PeakWorkingSet {
    ids: usize,
    metadata_bytes: usize,
    maintenance_bytes: usize,
}

impl PeakWorkingSet {
    fn observe(
        &mut self,
        page_ids: usize,
        mark: MarkMetrics,
        queue: QueueMetrics,
        maintenance_bytes: usize,
    ) {
        let retained_ids = page_ids + mark.max_pack + mark.max_children + queue.max_pack;
        let bytes = page_ids * 42
            + mark.max_pack * 34
            + mark.max_children * 32
            + queue.max_pack * (8 + 32 + 2 + 4);
        self.ids = self.ids.max(retained_ids);
        self.metadata_bytes = self.metadata_bytes.max(bytes);
        self.maintenance_bytes = self.maintenance_bytes.max(maintenance_bytes);
    }

    fn assert_bounded(self) {
        assert!(
            self.ids <= PEAK_ID_LIMIT,
            "peak retained IDs {} exceed {PEAK_ID_LIMIT}",
            self.ids
        );
        assert!(
            self.metadata_bytes <= PEAK_METADATA_LIMIT,
            "peak metadata {} exceeds {PEAK_METADATA_LIMIT}",
            self.metadata_bytes
        );
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct World {
    epoch: u64,
    next_cycle: u64,
    objects: BTreeMap<ObjectId, Object>,
    roots: Vec<Root>,
    untracked_roots: Vec<Root>,
    progress: Option<Progress>,
}

impl World {
    fn start_gc(&mut self, io: &mut IoStats) -> Result<(), String> {
        if self.progress.is_some() {
            return Err("GC progress already exists".into());
        }
        let previous_epoch = self.epoch;
        self.epoch += 1;
        self.next_cycle += 1;
        let mut cycle_id = [0_u8; 16];
        cycle_id[..8].copy_from_slice(&previous_epoch.to_be_bytes());
        cycle_id[8..].copy_from_slice(&self.next_cycle.to_be_bytes());
        self.progress = Some(Progress {
            cycle_id,
            phase: Phase::RootSelectors,
            expected_epoch: self.epoch,
            root_cursor: 0,
            untracked_cursor: 0,
            sweep_after: None,
            mark: MarkNode::empty(),
            queue: PersistedQueue::new(),
            marked_count: 0,
            validated_count: 0,
            reclaimed_count: 0,
        });
        io.write(128);
        Ok(())
    }

    fn publish_root(&mut self, root: Root, expected_epoch: u64) -> Result<(), String> {
        if expected_epoch != self.epoch {
            return Err("stale publication epoch".into());
        }
        self.roots.push(root);
        self.roots
            .sort_by_key(|value| (value.role.encode(), value.object));
        self.epoch += 1;
        Ok(())
    }

    fn remove_root(
        &mut self,
        role: RootRole,
        object: ObjectId,
        expected_epoch: u64,
    ) -> Result<(), String> {
        if expected_epoch != self.epoch {
            return Err("stale root release epoch".into());
        }
        let before = self.roots.len();
        self.roots
            .retain(|value| value.role != role || value.object != object);
        if self.roots.len() == before {
            return Err("root release target is absent".into());
        }
        self.epoch += 1;
        Ok(())
    }

    fn complete_upload(
        &mut self,
        progress_id: ObjectId,
        manifest_id: ObjectId,
        expected_epoch: u64,
    ) -> Result<(), String> {
        if expected_epoch != self.epoch {
            return Err("stale upload completion epoch".into());
        }
        let Some(root) = self
            .roots
            .iter_mut()
            .find(|value| value.role == RootRole::Upload && value.object == progress_id)
        else {
            return Err("upload selector is absent".into());
        };
        let manifest = self
            .objects
            .get(&manifest_id)
            .ok_or("completion manifest is absent")?;
        if manifest.domain != Domain::BlobManifest || !manifest.valid {
            return Err("completion manifest is invalid".into());
        }
        root.role = RootRole::Branch;
        root.object = manifest_id;
        root.domain = Domain::BlobManifest;
        self.roots
            .sort_by_key(|value| (value.role.encode(), value.object));
        self.epoch += 1;
        Ok(())
    }

    fn advance_gc(
        &mut self,
        io: &mut IoStats,
        peak: &mut PeakWorkingSet,
        events: &mut Vec<String>,
    ) -> Result<bool, String> {
        let mut progress = self.progress.take().ok_or("GC is not active")?;
        if progress.expected_epoch != self.epoch {
            self.progress = Some(progress);
            return Err("stale GC epoch".into());
        }
        progress.mark.validate()?;
        progress.queue.validate()?;

        let phase_before = progress.phase;
        match progress.phase {
            Phase::RootSelectors => {
                let end = (progress.root_cursor + ROOT_PAGE).min(self.roots.len());
                let page = &self.roots[progress.root_cursor..end];
                let inserted = progress.mark.insert_batch(
                    page.iter()
                        .map(|root| MarkEntry {
                            id: root.object,
                            domain: root.domain,
                        })
                        .collect(),
                    io,
                )?;
                let claims: Vec<_> = inserted
                    .iter()
                    .map(|entry| (entry.id, entry.domain, 0))
                    .collect();
                progress.queue.append_batch(&claims, io)?;
                progress.marked_count += inserted.len() as u64;
                progress.root_cursor = end;
                if end == self.roots.len() {
                    progress.phase = Phase::RootUntracked;
                }
                self.observe_progress(&progress, page.len(), peak)?;
            }
            Phase::RootUntracked => {
                let end =
                    (progress.untracked_cursor + UNTRACKED_PAGE).min(self.untracked_roots.len());
                let page = &self.untracked_roots[progress.untracked_cursor..end];
                let inserted = progress.mark.insert_batch(
                    page.iter()
                        .map(|root| MarkEntry {
                            id: root.object,
                            domain: root.domain,
                        })
                        .collect(),
                    io,
                )?;
                let claims: Vec<_> = inserted
                    .iter()
                    .map(|entry| (entry.id, entry.domain, 0))
                    .collect();
                progress.queue.append_batch(&claims, io)?;
                progress.marked_count += inserted.len() as u64;
                progress.untracked_cursor = end;
                if end == self.untracked_roots.len() {
                    progress.phase = Phase::Traverse;
                }
                self.observe_progress(&progress, page.len(), peak)?;
            }
            Phase::Traverse => {
                let claims = progress.queue.pop_batch(io)?;
                let mut edge_page_peak = 0_usize;
                for claim in &claims {
                    let object = self
                        .objects
                        .get(&claim.id)
                        .ok_or("reachable object is missing")?;
                    io.read(object_size(object) as u64);
                    if !object.valid || object.domain != claim.domain {
                        return Err("reachable object authentication/domain failed".into());
                    }
                    let offset = usize::try_from(claim.edge_offset)
                        .map_err(|_| "edge offset does not fit usize".to_string())?;
                    if offset > object.edges.len() {
                        return Err("edge continuation is outside object".into());
                    }
                    let end = (offset + EDGE_PAGE).min(object.edges.len());
                    let edges = &object.edges[offset..end];
                    edge_page_peak = edge_page_peak.max(edges.len());
                    if edges.iter().any(|(id, _)| *id == ObjectId::zero()) {
                        return Err("reachable edge has zero ID".into());
                    }
                    let inserted = progress.mark.insert_batch(
                        edges
                            .iter()
                            .map(|(id, domain)| MarkEntry {
                                id: *id,
                                domain: *domain,
                            })
                            .collect(),
                        io,
                    )?;
                    let mut appended: Vec<_> = inserted
                        .iter()
                        .map(|entry| (entry.id, entry.domain, 0))
                        .collect();
                    progress.marked_count += inserted.len() as u64;
                    if end < object.edges.len() {
                        appended.push((claim.id, claim.domain, end));
                    } else {
                        progress.validated_count += 1;
                    }
                    progress.queue.append_batch(&appended, io)?;
                }
                if progress.queue.is_empty() {
                    progress.phase = Phase::Sweep;
                }
                self.observe_progress(&progress, claims.len() + edge_page_peak, peak)?;
            }
            Phase::Sweep => {
                let page: Vec<_> = self
                    .objects
                    .range((
                        progress
                            .sweep_after
                            .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                        std::ops::Bound::Unbounded,
                    ))
                    .take(SWEEP_PAGE)
                    .map(|(id, object)| (*id, object.domain))
                    .collect();
                let mut deletes = Vec::with_capacity(DELETE_BATCH);
                if !page.is_empty() {
                    io.read((page.len() * 34 + 8_192) as u64);
                }
                for (id, domain) in &page {
                    let mut range_probe = IoStats::default();
                    if !progress.mark.contains(*id, *domain, &mut range_probe)? {
                        let object = self.objects.get(id).ok_or("sweep candidate disappeared")?;
                        io.read(object_size(object) as u64);
                        if !object.valid {
                            return Err("unmarked object is corrupt; sweep fails closed".into());
                        }
                        deletes.push(*id);
                    }
                }
                if deletes.len() > DELETE_BATCH {
                    return Err("sweep delete batch exceeds bound".into());
                }
                for id in &deletes {
                    self.objects.remove(id);
                }
                if !deletes.is_empty() {
                    io.delete_calls += 1;
                    io.delete_ids += deletes.len() as u64;
                    progress.reclaimed_count += deletes.len() as u64;
                }
                progress.sweep_after = page.last().map(|value| value.0);
                if page.len() < SWEEP_PAGE {
                    progress.phase = Phase::Cleanup;
                }
                self.observe_progress(&progress, page.len() + deletes.len(), peak)?;
            }
            Phase::Cleanup => {
                progress.mark.validate()?;
                progress.queue.validate()?;
                self.epoch += 1;
                events.push(format!(
                    "phase={:?}->Done epoch={} root_cursor={} marked={} validated={} reclaimed={}",
                    phase_before,
                    self.epoch,
                    progress.root_cursor,
                    progress.marked_count,
                    progress.validated_count,
                    progress.reclaimed_count
                ));
                io.write(64);
                return Ok(true);
            }
        }

        self.epoch += 1;
        progress.expected_epoch = self.epoch;
        events.push(format!(
            "phase={:?}->{:?} epoch={} root_cursor={} untracked_cursor={} queue={}/{} marked={} validated={} reclaimed={}",
            phase_before,
            progress.phase,
            self.epoch,
            progress.root_cursor,
            progress.untracked_cursor,
            progress.queue.pop_sequence,
            progress.queue.push_sequence,
            progress.marked_count,
            progress.validated_count,
            progress.reclaimed_count
        ));
        // The persisted progress object contains only fixed roots/cursors and
        // counters. Mark/queue packs are separate immutable objects.
        io.write(160);
        self.progress = Some(progress);
        Ok(false)
    }

    fn observe_progress(
        &self,
        progress: &Progress,
        page_ids: usize,
        peak: &mut PeakWorkingSet,
    ) -> Result<(), String> {
        let mark = progress.mark.validate()?;
        let queue = progress.queue.validate()?;
        peak.observe(page_ids, mark, queue, progress_size(progress));
        Ok(())
    }

    fn validate(&self) -> Result<(), String> {
        if self.roots.windows(2).any(|pair| {
            (pair[0].role.encode(), pair[0].object) >= (pair[1].role.encode(), pair[1].object)
        }) {
            return Err("roots are duplicate or misordered".into());
        }
        if self.objects.contains_key(&ObjectId::zero()) {
            return Err("object store contains zero ID".into());
        }
        if let Some(progress) = &self.progress {
            if progress.expected_epoch != self.epoch {
                return Err("progress fence does not match current epoch".into());
            }
            progress.mark.validate()?;
            progress.queue.validate()?;
        }
        Ok(())
    }
}

fn object_size(object: &Object) -> usize {
    16 + object.payload_bytes as usize + object.edges.len() * 34
}

fn progress_size(progress: &Progress) -> usize {
    160 + progress.mark.encoded_size() + progress.queue.encoded_size()
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn id(&mut self, value: ObjectId) {
        self.bytes.extend_from_slice(&value.0);
    }

    fn finish(mut self) -> Vec<u8> {
        let checksum = checksum64(&self.bytes);
        self.u64(checksum);
        self.bytes
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, String> {
        if bytes.len() < MAGIC.len() + 8 {
            return Err("snapshot is truncated".into());
        }
        let body_len = bytes.len() - 8;
        let expected = u64::from_be_bytes(
            bytes[body_len..]
                .try_into()
                .map_err(|_| "snapshot checksum is truncated")?,
        );
        if checksum64(&bytes[..body_len]) != expected {
            return Err("snapshot checksum mismatch".into());
        }
        Ok(Self {
            bytes: &bytes[..body_len],
            cursor: 0,
        })
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], String> {
        let end = self
            .cursor
            .checked_add(count)
            .ok_or("snapshot cursor overflow")?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or("snapshot is truncated")?;
        self.cursor = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().map_err(|_| "invalid u16")?,
        ))
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| "invalid u32")?,
        ))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| "invalid u64")?,
        ))
    }

    fn usize(&mut self, label: &str, maximum: usize) -> Result<usize, String> {
        let value =
            usize::try_from(self.u64()?).map_err(|_| format!("{label} does not fit usize"))?;
        if value > maximum {
            return Err(format!("{label} exceeds bound {maximum}"));
        }
        Ok(value)
    }

    fn id(&mut self) -> Result<ObjectId, String> {
        Ok(ObjectId(
            self.take(32)?.try_into().map_err(|_| "invalid ObjectId")?,
        ))
    }

    fn finish(self) -> Result<(), String> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err("snapshot contains trailing bytes".into())
        }
    }
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut value = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x100_0000_01b3);
    }
    value
}

fn encode_mark(node: &MarkNode, encoder: &mut Encoder) {
    match node {
        MarkNode::Pack { depth, entries } => {
            encoder.u8(0);
            encoder.u8(*depth);
            encoder.u64(entries.len() as u64);
            for entry in entries {
                encoder.id(entry.id);
                encoder.u16(entry.domain as u16);
            }
        }
        MarkNode::Branch { depth, children } => {
            encoder.u8(1);
            encoder.u8(*depth);
            encoder.u64(children.len() as u64);
            for (key, child) in children {
                encoder.u8(*key);
                encode_mark(child, encoder);
            }
        }
        MarkNode::Missing => encoder.u8(2),
    }
}

fn decode_mark(decoder: &mut Decoder<'_>) -> Result<MarkNode, String> {
    match decoder.u8()? {
        0 => {
            let depth = decoder.u8()?;
            let count = decoder.usize("mark pack count", MARK_PACK_MAX)?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                entries.push(MarkEntry {
                    id: decoder.id()?,
                    domain: Domain::decode(decoder.u16()?)?,
                });
            }
            Ok(MarkNode::Pack { depth, entries })
        }
        1 => {
            let depth = decoder.u8()?;
            let count = decoder.usize("radix child count", 256)?;
            let mut children = BTreeMap::new();
            for _ in 0..count {
                let key = decoder.u8()?;
                if children
                    .insert(key, Box::new(decode_mark(decoder)?))
                    .is_some()
                {
                    return Err("duplicate radix child".into());
                }
            }
            Ok(MarkNode::Branch { depth, children })
        }
        2 => Ok(MarkNode::Missing),
        value => Err(format!("invalid mark node tag {value}")),
    }
}

fn encode_queue(queue: &PersistedQueue, encoder: &mut Encoder) {
    encoder.u64(queue.pop_sequence);
    encoder.u64(queue.push_sequence);
    encoder.u64(queue.packs.len() as u64);
    for (number, slot) in &queue.packs {
        encoder.u64(*number);
        match slot {
            Some(pack) => {
                encoder.u8(1);
                encoder.u64(pack.first_sequence);
                encoder.u64(pack.entries.len() as u64);
                for claim in &pack.entries {
                    encoder.u64(claim.sequence);
                    encoder.id(claim.id);
                    encoder.u16(claim.domain as u16);
                    encoder.u32(claim.edge_offset);
                }
            }
            None => encoder.u8(0),
        }
    }
}

fn decode_queue(decoder: &mut Decoder<'_>) -> Result<PersistedQueue, String> {
    let pop_sequence = decoder.u64()?;
    let push_sequence = decoder.u64()?;
    let count = decoder.usize("queue pack count", 10_000_000)?;
    let mut packs = BTreeMap::new();
    for _ in 0..count {
        let number = decoder.u64()?;
        let slot = match decoder.u8()? {
            0 => None,
            1 => {
                let first_sequence = decoder.u64()?;
                let entry_count = decoder.usize("queue pack entries", QUEUE_PACK_MAX)?;
                let mut entries = Vec::with_capacity(entry_count);
                for _ in 0..entry_count {
                    entries.push(QueueClaim {
                        sequence: decoder.u64()?,
                        id: decoder.id()?,
                        domain: Domain::decode(decoder.u16()?)?,
                        edge_offset: decoder.u32()?,
                    });
                }
                Some(QueuePack {
                    first_sequence,
                    entries,
                })
            }
            value => return Err(format!("invalid queue pack option {value}")),
        };
        if packs.insert(number, slot).is_some() {
            return Err("duplicate queue pack number".into());
        }
    }
    Ok(PersistedQueue {
        packs,
        pop_sequence,
        push_sequence,
    })
}

fn encode_world(world: &World) -> Vec<u8> {
    let mut encoder = Encoder::default();
    encoder.bytes.extend_from_slice(MAGIC);
    encoder.u64(world.epoch);
    encoder.u64(world.next_cycle);
    encoder.u64(world.objects.len() as u64);
    for (id, object) in &world.objects {
        encoder.id(*id);
        encoder.u16(object.domain as u16);
        encoder.u32(object.payload_bytes);
        encoder.u8(u8::from(object.valid));
        encoder.u64(object.edges.len() as u64);
        for (edge, domain) in &object.edges {
            encoder.id(*edge);
            encoder.u16(*domain as u16);
        }
    }
    encoder.u64(world.roots.len() as u64);
    for root in &world.roots {
        encoder.u8(root.role.encode());
        encoder.id(root.object);
        encoder.u16(root.domain as u16);
    }
    encoder.u64(world.untracked_roots.len() as u64);
    for root in &world.untracked_roots {
        encoder.u8(root.role.encode());
        encoder.id(root.object);
        encoder.u16(root.domain as u16);
    }
    match &world.progress {
        Some(progress) => {
            encoder.u8(1);
            encoder.bytes.extend_from_slice(&progress.cycle_id);
            encoder.u8(progress.phase.encode());
            encoder.u64(progress.expected_epoch);
            encoder.u64(progress.root_cursor as u64);
            encoder.u64(progress.untracked_cursor as u64);
            match progress.sweep_after {
                Some(value) => {
                    encoder.u8(1);
                    encoder.id(value);
                }
                None => encoder.u8(0),
            }
            encoder.u64(progress.marked_count);
            encoder.u64(progress.validated_count);
            encoder.u64(progress.reclaimed_count);
            encode_mark(&progress.mark, &mut encoder);
            encode_queue(&progress.queue, &mut encoder);
        }
        None => encoder.u8(0),
    }
    encoder.finish()
}

fn decode_world(bytes: &[u8]) -> Result<World, String> {
    let mut decoder = Decoder::new(bytes)?;
    if decoder.take(MAGIC.len())? != MAGIC {
        return Err("snapshot magic/version mismatch".into());
    }
    let epoch = decoder.u64()?;
    let next_cycle = decoder.u64()?;
    let object_count = decoder.usize("object count", 10_000_000)?;
    let mut objects = BTreeMap::new();
    for _ in 0..object_count {
        let id = decoder.id()?;
        let domain = Domain::decode(decoder.u16()?)?;
        let payload_bytes = decoder.u32()?;
        let valid = match decoder.u8()? {
            0 => false,
            1 => true,
            value => return Err(format!("invalid object validity byte {value}")),
        };
        let edge_count = decoder.usize("object edge count", 10_000_000)?;
        let mut edges = Vec::with_capacity(edge_count);
        for _ in 0..edge_count {
            edges.push((decoder.id()?, Domain::decode(decoder.u16()?)?));
        }
        if objects
            .insert(
                id,
                Object {
                    domain,
                    edges,
                    payload_bytes,
                    valid,
                },
            )
            .is_some()
        {
            return Err("duplicate object ID".into());
        }
    }
    let root_count = decoder.usize("root count", 10_000_000)?;
    let mut roots = Vec::with_capacity(root_count);
    for _ in 0..root_count {
        roots.push(Root {
            role: RootRole::decode(decoder.u8()?)?,
            object: decoder.id()?,
            domain: Domain::decode(decoder.u16()?)?,
        });
    }
    let untracked_count = decoder.usize("untracked root count", 10_000_000)?;
    let mut untracked_roots = Vec::with_capacity(untracked_count);
    for _ in 0..untracked_count {
        untracked_roots.push(Root {
            role: RootRole::decode(decoder.u8()?)?,
            object: decoder.id()?,
            domain: Domain::decode(decoder.u16()?)?,
        });
    }
    let progress = match decoder.u8()? {
        0 => None,
        1 => {
            let cycle_id = decoder
                .take(16)?
                .try_into()
                .map_err(|_| "invalid cycle ID")?;
            let phase = Phase::decode(decoder.u8()?)?;
            let expected_epoch = decoder.u64()?;
            let root_cursor = decoder.usize("root cursor", 10_000_000)?;
            let untracked_cursor = decoder.usize("untracked cursor", 10_000_000)?;
            let sweep_after = match decoder.u8()? {
                0 => None,
                1 => Some(decoder.id()?),
                value => return Err(format!("invalid sweep cursor option {value}")),
            };
            let marked_count = decoder.u64()?;
            let validated_count = decoder.u64()?;
            let reclaimed_count = decoder.u64()?;
            let mark = decode_mark(&mut decoder)?;
            let queue = decode_queue(&mut decoder)?;
            Some(Progress {
                cycle_id,
                phase,
                expected_epoch,
                root_cursor,
                untracked_cursor,
                sweep_after,
                mark,
                queue,
                marked_count,
                validated_count,
                reclaimed_count,
            })
        }
        value => return Err(format!("invalid progress option {value}")),
    };
    decoder.finish()?;
    let world = World {
        epoch,
        next_cycle,
        objects,
        roots,
        untracked_roots,
        progress,
    };
    world.validate()?;
    Ok(world)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Persistence {
    Memory,
    File,
}

impl Persistence {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "memory" => Ok(Self::Memory),
            "file" => Ok(Self::File),
            _ => Err(format!("unknown persistence {value}")),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::File => "file",
        }
    }
}

struct RunOutcome {
    world: World,
    events: Vec<String>,
    io: IoStats,
    peak: PeakWorkingSet,
    checkpoint_writes: u64,
    checkpoint_reads: u64,
    checkpoint_path: Option<PathBuf>,
}

fn persist_and_reopen(
    world: &World,
    persistence: Persistence,
    path: Option<&Path>,
    io: &mut IoStats,
) -> Result<World, String> {
    let bytes = encode_world(world);
    io.checkpoint_bytes += bytes.len() as u64;
    match persistence {
        Persistence::Memory => decode_world(&bytes),
        Persistence::File => {
            let path = path.ok_or("file persistence requires a path")?;
            fs::write(path, &bytes).map_err(|error| format!("write checkpoint: {error}"))?;
            let reopened = fs::read(path).map_err(|error| format!("read checkpoint: {error}"))?;
            decode_world(&reopened)
        }
    }
}

fn run_to_completion(
    mut world: World,
    persistence: Persistence,
    reopen_every_checkpoint: bool,
) -> Result<RunOutcome, String> {
    let path = if persistence == Persistence::File {
        let mut path = env::temp_dir();
        path.push(format!(
            "forktree-bounded-gc-model-{}-{}.bin",
            std::process::id(),
            world.next_cycle
        ));
        Some(path)
    } else {
        None
    };
    let mut io = IoStats::default();
    let mut peak = PeakWorkingSet::default();
    let mut events = Vec::new();
    let mut writes = 0_u64;
    let mut reads = 0_u64;
    if world.progress.is_none() {
        world.start_gc(&mut io)?;
    }
    loop {
        let done = world.advance_gc(&mut io, &mut peak, &mut events)?;
        peak.maintenance_bytes = peak
            .maintenance_bytes
            .max(world.progress.as_ref().map_or(0, progress_size));
        if reopen_every_checkpoint || done {
            world = persist_and_reopen(&world, persistence, path.as_deref(), &mut io)?;
            writes += 1;
            reads += 1;
        }
        if done {
            break;
        }
    }
    peak.assert_bounded();
    Ok(RunOutcome {
        world,
        events,
        io,
        peak,
        checkpoint_writes: writes,
        checkpoint_reads: reads,
        checkpoint_path: path,
    })
}

fn build_scale_fixture(root_count: usize, fanout: usize, adversarial: bool) -> World {
    assert!(root_count > 0);
    let shared_count = fanout.max(1);
    let mut objects = BTreeMap::new();
    let mut shared = Vec::with_capacity(shared_count);
    for index in 0..shared_count {
        let id = ObjectId::deterministic(10_000_000 + index as u64, adversarial);
        objects.insert(
            id,
            Object {
                domain: Domain::State,
                edges: Vec::new(),
                payload_bytes: 32,
                valid: true,
            },
        );
        shared.push((id, Domain::State));
    }
    let mut roots = Vec::with_capacity(root_count);
    for index in 0..root_count {
        let id = ObjectId::deterministic(index as u64 + 1, adversarial);
        let edges = if index == 0 {
            shared.clone()
        } else {
            vec![shared[index % shared_count]]
        };
        objects.insert(
            id,
            Object {
                domain: Domain::Commit,
                edges,
                payload_bytes: 48,
                valid: true,
            },
        );
        roots.push(Root {
            role: if index == 0 {
                RootRole::Global
            } else {
                RootRole::Branch
            },
            object: id,
            domain: Domain::Commit,
        });
    }
    roots.sort_by_key(|value| (value.role.encode(), value.object));
    let orphan_count = root_count.div_ceil(10);
    for index in 0..orphan_count {
        let id = ObjectId::deterministic(20_000_000 + index as u64, adversarial);
        objects.insert(
            id,
            Object {
                domain: Domain::State,
                edges: Vec::new(),
                payload_bytes: 64,
                valid: true,
            },
        );
    }
    World {
        epoch: 1,
        next_cycle: 0,
        objects,
        roots,
        untracked_roots: Vec::new(),
        progress: None,
    }
}

fn run_scale(
    root_count: usize,
    fanout: usize,
    adversarial: bool,
    persistence: Persistence,
) -> Result<(), String> {
    let world = build_scale_fixture(root_count, fanout, adversarial);
    let initial_objects = world.objects.len();
    let logical_bytes: usize = world.objects.values().map(object_size).sum();
    let allocation_start = allocations();
    let cpu_start = process_cpu_ns();
    let rss_start = rss_kib("VmRSS:");
    let started = Instant::now();
    let outcome = run_to_completion(world, persistence, persistence == Persistence::File)?;
    let wall = started.elapsed();
    let cpu = process_cpu_ns().saturating_sub(cpu_start);
    let allocation = allocation_delta(allocation_start, allocations());
    let rss_end = rss_kib("VmRSS:");
    let rss_peak = rss_kib("VmHWM:");
    let expected_reachable = root_count + fanout.max(1);
    if outcome.world.objects.len() != expected_reachable {
        return Err(format!(
            "reachable object mismatch: expected {expected_reachable}, got {}",
            outcome.world.objects.len()
        ));
    }
    if outcome.world.progress.is_some() {
        return Err("GC progress survived cleanup".into());
    }
    let mark_order = verify_mark_order(root_count, fanout, adversarial)?;
    let disk_bytes = outcome
        .checkpoint_path
        .as_ref()
        .and_then(|path| fs::metadata(path).ok())
        .map_or(0, |value| value.len());
    println!(
        "RESULT lane=bounded_gc backend={} roots={} fanout={} adversarial={} initial_objects={} reachable={} orphaned={} mark_order={} wall_ns={} cpu_ns={} alloc_calls={} alloc_bytes={} rss_start_kib={} rss_end_kib={} rss_hwm_kib={} peak_ids={} peak_metadata_bytes={} peak_maintenance_bytes={} backend_read_calls={} backend_read_bytes={} backend_write_calls={} backend_write_bytes={} backend_delete_calls={} backend_delete_ids={} checkpoint_writes={} checkpoint_reads={} checkpoint_bytes={} logical_object_bytes={} settled_disk_bytes={} events={}",
        persistence.label(),
        root_count,
        fanout,
        adversarial,
        initial_objects,
        outcome.world.objects.len(),
        initial_objects - outcome.world.objects.len(),
        mark_order,
        wall.as_nanos(),
        cpu,
        allocation.calls,
        allocation.bytes,
        rss_start,
        rss_end,
        rss_peak,
        outcome.peak.ids,
        outcome.peak.metadata_bytes,
        outcome.peak.maintenance_bytes,
        outcome.io.read_calls,
        outcome.io.read_bytes,
        outcome.io.write_calls,
        outcome.io.write_bytes,
        outcome.io.delete_calls,
        outcome.io.delete_ids,
        outcome.checkpoint_writes,
        outcome.checkpoint_reads,
        outcome.io.checkpoint_bytes,
        logical_bytes,
        disk_bytes,
        outcome.events.len(),
    );
    if let Some(path) = outcome.checkpoint_path {
        let _ = fs::remove_file(path);
    }
    Ok(())
}

fn verify_mark_order(root_count: usize, fanout: usize, adversarial: bool) -> Result<usize, String> {
    let world = build_scale_fixture(root_count, fanout, adversarial);
    let mut io = IoStats::default();
    let mut peak = PeakWorkingSet::default();
    let mut events = Vec::new();
    let mut active = world;
    active.start_gc(&mut io)?;
    while active
        .progress
        .as_ref()
        .is_some_and(|value| value.phase != Phase::Sweep)
    {
        active.advance_gc(&mut io, &mut peak, &mut events)?;
    }
    let progress = active
        .progress
        .as_ref()
        .ok_or("progress missing before sweep")?;
    let first: Vec<_> = progress.mark.iter().map(|entry| entry.id).collect();
    let second: Vec<_> = progress.mark.iter().map(|entry| entry.id).collect();
    if first != second || first.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err("mark iteration is nondeterministic or misordered".into());
    }
    let mut resumed = Vec::new();
    let mut cursor = None;
    loop {
        let page: Vec<_> = progress
            .mark
            .iter()
            .filter(|entry| cursor.is_none_or(|value| entry.id > value))
            .take(SWEEP_PAGE)
            .map(|entry| entry.id)
            .collect();
        if page.is_empty() {
            break;
        }
        cursor = page.last().copied();
        resumed.extend(page);
    }
    if resumed != first {
        return Err("start-after pagination changed mark order".into());
    }
    Ok(first.len())
}

fn process_cpu_ns() -> u128 {
    let Ok(stat) = fs::read_to_string("/proc/self/stat") else {
        return 0;
    };
    let Some(end) = stat.rfind(')') else {
        return 0;
    };
    let fields: Vec<_> = stat[end + 1..].split_whitespace().collect();
    let user = fields
        .get(11)
        .and_then(|value| value.parse::<u128>().ok())
        .unwrap_or(0);
    let system = fields
        .get(12)
        .and_then(|value| value.parse::<u128>().ok())
        .unwrap_or(0);
    (user + system) * 10_000_000
}

fn rss_kib(label: &str) -> u64 {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix(label)?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
        })
        .unwrap_or(0)
}

fn conformance() -> Result<(), String> {
    deterministic_crash_reopen()?;
    corruption_fails_closed()?;
    epoch_races()?;
    upload_and_final_reference()?;
    synthetic_source_and_sealing_gate()?;
    println!(
        "RESULT lane=conformance tests=5 status=pass bounds=root:{ROOT_PAGE},untracked:{UNTRACKED_PAGE},edge:{EDGE_PAGE},traversal:{TRAVERSAL_BATCH},mark:{MARK_PACK_MAX},queue:{QUEUE_PACK_MAX},sweep:{SWEEP_PAGE},delete:{DELETE_BATCH}"
    );
    Ok(())
}

fn deterministic_crash_reopen() -> Result<(), String> {
    let fixture = build_scale_fixture(1_000, 4_097, true);
    let uninterrupted = run_to_completion(fixture.clone(), Persistence::Memory, false)?;
    let crash_memory = run_to_completion(fixture.clone(), Persistence::Memory, true)?;
    let crash_file = run_to_completion(fixture, Persistence::File, true)?;
    if uninterrupted.world != crash_memory.world
        || uninterrupted.world != crash_file.world
        || uninterrupted.events != crash_memory.events
        || uninterrupted.events != crash_file.events
    {
        return Err("crash/reopen changed deterministic result or page order".into());
    }
    let phases: BTreeSet<_> = crash_memory
        .events
        .iter()
        .filter_map(|event| event.split_whitespace().next())
        .collect();
    if phases.len() < 5 {
        return Err("crash fixture did not cross every persisted phase".into());
    }
    crash_memory.peak.assert_bounded();
    crash_file.peak.assert_bounded();
    if let Some(path) = crash_file.checkpoint_path {
        let _ = fs::remove_file(path);
    }
    Ok(())
}

fn active_after_roots() -> Result<World, String> {
    let mut world = build_scale_fixture(1_000, 300, true);
    let mut io = IoStats::default();
    let mut peak = PeakWorkingSet::default();
    let mut events = Vec::new();
    world.start_gc(&mut io)?;
    while world
        .progress
        .as_ref()
        .is_some_and(|value| value.phase != Phase::Traverse)
    {
        world.advance_gc(&mut io, &mut peak, &mut events)?;
    }
    Ok(world)
}

fn corruption_fails_closed() -> Result<(), String> {
    let baseline = active_after_roots()?;
    let object_count = baseline.objects.len();

    let mut duplicate = baseline.clone();
    let progress = duplicate.progress.as_mut().ok_or("missing progress")?;
    let mut first_pack = false;
    fn duplicate_first(node: &mut MarkNode, found: &mut bool) {
        if *found {
            return;
        }
        match node {
            MarkNode::Pack { entries, .. } if !entries.is_empty() => {
                entries.insert(0, entries[0]);
                *found = true;
            }
            MarkNode::Branch { children, .. } => {
                for child in children.values_mut() {
                    duplicate_first(child, found);
                    if *found {
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    duplicate_first(&mut progress.mark, &mut first_pack);
    if !first_pack {
        return Err("fixture did not contain a mark pack".into());
    }
    let before_epoch = duplicate.epoch;
    let mut io = IoStats::default();
    let mut peak = PeakWorkingSet::default();
    let mut events = Vec::new();
    if duplicate
        .advance_gc(&mut io, &mut peak, &mut events)
        .is_ok()
        || duplicate.objects.len() != object_count
        || duplicate.epoch != before_epoch
    {
        return Err("duplicate mark pack did not fail closed".into());
    }

    let mut missing = baseline.clone();
    let progress = missing.progress.as_mut().ok_or("missing progress")?;
    let first = *progress
        .queue
        .packs
        .keys()
        .next()
        .ok_or("fixture has no queue pack")?;
    progress.queue.packs.insert(first, None);
    if missing
        .advance_gc(
            &mut IoStats::default(),
            &mut PeakWorkingSet::default(),
            &mut Vec::new(),
        )
        .is_ok()
        || missing.objects.len() != object_count
    {
        return Err("missing queue pack did not fail closed".into());
    }

    let mut duplicate_sequence = baseline.clone();
    let progress = duplicate_sequence
        .progress
        .as_mut()
        .ok_or("missing progress")?;
    let pack = progress
        .queue
        .packs
        .values_mut()
        .find_map(Option::as_mut)
        .ok_or("fixture has no queue pack")?;
    if pack.entries.len() < 2 {
        return Err("fixture queue pack is too small".into());
    }
    pack.entries[1].sequence = pack.entries[0].sequence;
    if duplicate_sequence
        .advance_gc(
            &mut IoStats::default(),
            &mut PeakWorkingSet::default(),
            &mut Vec::new(),
        )
        .is_ok()
    {
        return Err("duplicate queue sequence did not fail closed".into());
    }

    let mut bytes = encode_world(&baseline);
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x80;
    if decode_world(&bytes).is_ok() {
        return Err("malformed persisted snapshot passed authentication".into());
    }

    let mut bad_object = build_scale_fixture(1_000, 1, false);
    let orphan_id = *bad_object
        .objects
        .keys()
        .find(|id| !bad_object.roots.iter().any(|root| root.object == **id))
        .ok_or("fixture has no orphan")?;
    bad_object
        .objects
        .get_mut(&orphan_id)
        .ok_or("orphan disappeared")?
        .valid = false;
    let original_count = bad_object.objects.len();
    if run_to_completion(bad_object, Persistence::Memory, true).is_ok() {
        return Err("corrupt unmarked object was silently deleted".into());
    }
    if original_count == 0 {
        return Err("invalid corruption fixture".into());
    }
    Ok(())
}

fn epoch_races() -> Result<(), String> {
    let mut publication_first = build_scale_fixture(1_000, 1, false);
    let mut io = IoStats::default();
    publication_first.start_gc(&mut io)?;
    let stale_gc_epoch = publication_first.epoch;
    let new_id = ObjectId::deterministic(90_000_000, false);
    publication_first.objects.insert(
        new_id,
        Object {
            domain: Domain::State,
            edges: Vec::new(),
            payload_bytes: 16,
            valid: true,
        },
    );
    publication_first.publish_root(
        Root {
            role: RootRole::Checkpoint,
            object: new_id,
            domain: Domain::State,
        },
        stale_gc_epoch,
    )?;
    if !matches!(
        publication_first.advance_gc(
            &mut io,
            &mut PeakWorkingSet::default(),
            &mut Vec::new()
        ),
        Err(error) if error == "stale GC epoch"
    ) {
        return Err("publication-first did not reject stale GC".into());
    }

    let mut gc_first = build_scale_fixture(1_000, 1, false);
    let writer_epoch = gc_first.epoch;
    gc_first.start_gc(&mut IoStats::default())?;
    gc_first.advance_gc(
        &mut IoStats::default(),
        &mut PeakWorkingSet::default(),
        &mut Vec::new(),
    )?;
    if !matches!(
        gc_first.publish_root(
            Root {
                role: RootRole::Recovery,
                object: gc_first.roots[0].object,
                domain: gc_first.roots[0].domain,
            },
            writer_epoch,
        ),
        Err(error) if error == "stale publication epoch"
    ) {
        return Err("GC-first did not reject stale publication".into());
    }
    Ok(())
}

fn upload_fixture() -> (World, ObjectId, ObjectId, ObjectId, ObjectId, ObjectId) {
    let shared = ObjectId::deterministic(1, false);
    let upload_only = ObjectId::deterministic(2, false);
    let semantic = ObjectId::deterministic(3, false);
    let part = ObjectId::deterministic(4, false);
    let progress = ObjectId::deterministic(5, false);
    let manifest = ObjectId::deterministic(6, false);
    let objects = BTreeMap::from([
        (
            shared,
            Object {
                domain: Domain::BlobChunk,
                edges: Vec::new(),
                payload_bytes: 256,
                valid: true,
            },
        ),
        (
            upload_only,
            Object {
                domain: Domain::BlobChunk,
                edges: Vec::new(),
                payload_bytes: 256,
                valid: true,
            },
        ),
        (
            semantic,
            Object {
                domain: Domain::State,
                edges: vec![(shared, Domain::BlobChunk)],
                payload_bytes: 32,
                valid: true,
            },
        ),
        (
            part,
            Object {
                domain: Domain::UploadPart,
                edges: vec![
                    (shared, Domain::BlobChunk),
                    (upload_only, Domain::BlobChunk),
                ],
                payload_bytes: 32,
                valid: true,
            },
        ),
        (
            progress,
            Object {
                domain: Domain::UploadProgress,
                edges: vec![(part, Domain::UploadPart)],
                payload_bytes: 32,
                valid: true,
            },
        ),
        (
            manifest,
            Object {
                domain: Domain::BlobManifest,
                edges: vec![
                    (shared, Domain::BlobChunk),
                    (upload_only, Domain::BlobChunk),
                ],
                payload_bytes: 32,
                valid: true,
            },
        ),
    ]);
    (
        World {
            epoch: 1,
            next_cycle: 0,
            objects,
            roots: vec![
                Root {
                    role: RootRole::Branch,
                    object: semantic,
                    domain: Domain::State,
                },
                Root {
                    role: RootRole::Upload,
                    object: progress,
                    domain: Domain::UploadProgress,
                },
            ],
            untracked_roots: Vec::new(),
            progress: None,
        },
        shared,
        upload_only,
        semantic,
        progress,
        manifest,
    )
}

fn upload_and_final_reference() -> Result<(), String> {
    let (world, shared, upload_only, semantic, progress, manifest) = upload_fixture();
    let both = run_to_completion(world.clone(), Persistence::Memory, true)?.world;
    if !both.objects.contains_key(&shared) || !both.objects.contains_key(&upload_only) {
        return Err("open upload/shared roots did not retain chunks".into());
    }

    let mut completed = world.clone();
    let epoch = completed.epoch;
    completed.complete_upload(progress, manifest, epoch)?;
    let completed = run_to_completion(completed, Persistence::Memory, true)?.world;
    if !completed.objects.contains_key(&shared) || !completed.objects.contains_key(&upload_only) {
        return Err("receipt-to-file completion had a reachability gap".into());
    }

    let mut aborted = world;
    let epoch = aborted.epoch;
    aborted.remove_root(RootRole::Upload, progress, epoch)?;
    let aborted = run_to_completion(aborted, Persistence::Memory, true)?.world;
    if !aborted.objects.contains_key(&shared) || aborted.objects.contains_key(&upload_only) {
        return Err("abort did not preserve shared/reclaim upload-only chunk".into());
    }

    let mut final_release = aborted;
    let epoch = final_release.epoch;
    final_release.remove_root(RootRole::Branch, semantic, epoch)?;
    let final_release = run_to_completion(final_release, Persistence::Memory, true)?.world;
    if final_release.objects.contains_key(&shared) {
        return Err("shared chunk survived final reference release".into());
    }
    Ok(())
}

fn synthetic_source_and_sealing_gate() -> Result<(), String> {
    let good = r#"
        pub struct SpaceId(u32);
        pub struct StorageSpace { id: SpaceId, _brand: private::EngineDeclared }
        pub(crate) fn engine_declared() {}
        struct GcMarkPackV2;
        struct GcProgressV2;
        struct SweepBatch { private: () }
        fn mark_range_iter() {}
    "#;
    source_text_gate(good)?;
    for bad in [
        format!("{good}\npub struct SpaceId(pub u32);"),
        format!("{good}\npub const OBJECT_SPACE: u32 = 1;"),
        format!("{good}\nstruct GcMarkPackV1;"),
        format!("{good}\nfn discover_sweep_plan() {{}}"),
        format!("{good}\nlet orphan_object_ids = Vec::new();"),
    ] {
        if source_text_gate(&bad).is_ok() {
            return Err("negative source/sealing fixture was accepted".into());
        }
    }
    Ok(())
}

fn source_text_gate(source: &str) -> Result<(), String> {
    for forbidden in [
        "SpaceId(pub",
        "pub const OBJECT_SPACE",
        "pub const SELECTOR_SPACE",
        "GcMarkPackV1",
        "GcProgressV1",
        "discover_sweep_plan",
        "orphan_object_ids",
        "BTreeSet<ObjectId>",
        "VecDeque<ObjectId>",
    ] {
        if source.contains(forbidden) {
            return Err(format!("forbidden source residue: {forbidden}"));
        }
    }
    for required in [
        "pub struct SpaceId(u32)",
        "_brand: private::EngineDeclared",
        "GcMarkPackV2",
        "GcProgressV2",
        "SweepBatch { private:",
        "mark_range_iter",
    ] {
        if !source.contains(required) {
            return Err(format!("required source invariant absent: {required}"));
        }
    }
    Ok(())
}

fn source_gate(checkout: &Path) -> Result<(), String> {
    let storage_types = fs::read_to_string(checkout.join("packages/lix/src/storage/types.rs"))
        .map_err(|error| format!("read storage/types.rs: {error}"))?;
    let forktree = checkout.join("packages/lix/src/forktree");
    let mut combined = storage_types;
    for entry in fs::read_dir(&forktree).map_err(|error| format!("read forktree: {error}"))? {
        let path = entry.map_err(|error| error.to_string())?.path();
        if path.extension().is_some_and(|value| value == "rs") {
            combined.push_str(
                &fs::read_to_string(&path)
                    .map_err(|error| format!("read {}: {error}", path.display()))?,
            );
        }
    }
    source_text_gate(&combined)?;
    println!(
        "RESULT lane=source_gate checkout={} status=pass",
        checkout.display()
    );
    Ok(())
}

fn usage() -> String {
    "usage: forktree_bounded_gc_oracle conformance | scale <roots> <fanout> <normal|adversarial> <memory|file> | source-gate <checkout>".into()
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let result = match args.get(1).map(String::as_str) {
        Some("conformance") if args.len() == 2 => conformance(),
        Some("scale") if args.len() == 6 => {
            let roots = args[2]
                .parse::<usize>()
                .map_err(|error| format!("invalid roots: {error}"));
            let fanout = args[3]
                .parse::<usize>()
                .map_err(|error| format!("invalid fanout: {error}"));
            let adversarial = match args[4].as_str() {
                "normal" => Ok(false),
                "adversarial" => Ok(true),
                value => Err(format!("invalid shape {value}")),
            };
            let persistence = Persistence::parse(&args[5]);
            roots
                .and_then(|roots| fanout.map(|fanout| (roots, fanout)))
                .and_then(|(roots, fanout)| adversarial.map(|shape| (roots, fanout, shape)))
                .and_then(|(roots, fanout, shape)| {
                    persistence.map(|backend| (roots, fanout, shape, backend))
                })
                .and_then(|(roots, fanout, shape, backend)| {
                    run_scale(roots, fanout, shape, backend)
                })
        }
        Some("source-gate") if args.len() == 3 => source_gate(Path::new(&args[2])),
        _ => Err(usage()),
    };
    if let Err(error) = result {
        eprintln!("ERROR {error}");
        std::process::exit(1);
    }
}
