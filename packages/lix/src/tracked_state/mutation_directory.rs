//! Authenticated immutable directory for commit mutation parts.
//!
//! Commit headers authenticate a small catalog, and that catalog authenticates
//! one root from this tree. Part bounds, compact replacement identities, and
//! direct-address row counts live only in leaves. Metadata-only operations can
//! therefore stop at the header while point routing reads one node per level.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Range;

use bytes::Bytes;

use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet, ValueSemantics,
};
use crate::{LixError, storage_codec};

use super::types::CommitStateMutationPart;

pub(crate) const MUTATION_DIRECTORY_NODE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_002d),
    "tracked_state.commit_mutation_directory_node.v2",
    ValueSemantics::Immutable,
);

const NODE_MAGIC: &[u8] = b"LXMD3";
const NODE_HASH_CONTEXT: &str = "lix commit mutation directory node v2";
const ROOT_HASH_CONTEXT: &str = "lix commit mutation directory root v2";
const FANOUT: usize = 128;

// A pass-through counting allocator, test builds only.  It delegates every
// call to `System` (which is what this crate already used, since it declares
// no allocator otherwise) and counts only while a measurement scope is open
// on the *current thread*, so a parallel sibling test cannot contaminate it.
#[cfg(test)]
mod alloc_census {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    thread_local! {
        static ON: Cell<bool> = const { Cell::new(false) };
        static ALLOCS: Cell<u64> = const { Cell::new(0) };
        static BYTES: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) struct Counting;

    fn bump(size: usize) {
        let _ = ON.try_with(|on| {
            if on.get() {
                let _ = ALLOCS.try_with(|counter| counter.set(counter.get().saturating_add(1)));
                let _ = BYTES.try_with(|counter| {
                    counter.set(counter.get().saturating_add(size as u64))
                });
            }
        });
    }

    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            bump(layout.size());
            unsafe { System.alloc(layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            bump(layout.size());
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            bump(new_size);
            unsafe { System.realloc(ptr, layout, new_size) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    /// Whether allocation counting is armed on this thread.  The decode census
    /// consults this so its own bookkeeping never lands in a measured count.
    pub(super) fn armed() -> bool {
        ON.try_with(|on| on.get()).unwrap_or(false)
    }

    /// Runs `body` with allocation counting armed on this thread and returns
    /// `(output, allocations, allocated_bytes)`.
    pub(super) fn measure<T>(body: impl FnOnce() -> T) -> (T, u64, u64) {
        let _ = ALLOCS.try_with(|counter| counter.set(0));
        let _ = BYTES.try_with(|counter| counter.set(0));
        let _ = ON.try_with(|on| on.set(true));
        let output = body();
        let _ = ON.try_with(|on| on.set(false));
        let allocs = ALLOCS.try_with(|counter| counter.get()).unwrap_or(0);
        let bytes = BYTES.try_with(|counter| counter.get()).unwrap_or(0);
        (output, allocs, bytes)
    }
}

#[cfg(test)]
#[global_allocator]
static COUNTING_ALLOCATOR: alloc_census::Counting = alloc_census::Counting;

/// Prototype of the seekable node layout ("LXMD2").
///
/// A v1 node is one `musli(packed)` record: decoding it materializes every
/// entry in the node, including an owned `Vec<u8>` per key bound, before the
/// reader can find the single entry a point read wants.  A v2 node keeps the
/// same information but stores it as fixed-stride tables over the node's own
/// bytes, so a point read binary-searches the key table in place and decodes
/// exactly one entry.
///
/// Layout, all little-endian:
/// ```text
///   0  magic "LXMD2"        (5)
///   5  kind                 (1)   0 = leaf, 1 = internal
///   6  layout               (1)
///   7  level                (2)
///   9  count                (4)   entries/children in this node
///  13  entry_count          (4)   leaf entries under this node
///  17  direct_row_count     (8)
///  25  key_off[2*count + 1] (4 each)  alternating first_key/last_key bounds
///      fixed[count]         (leaf: 2 bytes; internal: 44 bytes)
///      key_region
/// ```
/// The node summary a parent authenticates (`first_key`, `last_key`,
/// `entry_count`, `direct_row_count`) is readable from the header and the two
/// outermost key slots, so validating a loaded node is O(1) rather than a walk
/// over every entry.
///
/// The prototype covers `LAYOUT_BOUNDED_DIRECT` with no replacement part -
/// the shape a cold point read actually traverses.  The other three layouts
/// carry no key bytes at all and route by index, which the same cumulative
/// tables serve.
#[cfg(test)]
pub(crate) mod seekable_prototype {
    use super::*;

    pub(crate) const V2_MAGIC: &[u8] = b"LXMD2";
    const HEADER: usize = 25;
    const LEAF_STRIDE: usize = 2; // direct_row_count u16
    const INTERNAL_STRIDE: usize = 44; // node_id[32] + cum_entry_count u32 + cum_direct_rows u64

    fn put_u32(out: &mut Vec<u8>, value: u32) {
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn header(
        out: &mut Vec<u8>,
        kind: u8,
        layout: u8,
        level: u16,
        count: u32,
        entry_count: u32,
        direct_row_count: u64,
    ) {
        out.extend_from_slice(V2_MAGIC);
        out.push(kind);
        out.push(layout);
        out.extend_from_slice(&level.to_le_bytes());
        put_u32(out, count);
        put_u32(out, entry_count);
        out.extend_from_slice(&direct_row_count.to_le_bytes());
    }

    /// Encodes a bounded leaf in the v2 layout.
    pub(crate) fn encode_leaf(layout: u8, entries: &[(Vec<u8>, Vec<u8>, u16)]) -> Vec<u8> {
        let count = entries.len();
        let direct_row_count: u64 = entries.iter().map(|entry| u64::from(entry.2)).sum();
        let mut out = Vec::new();
        header(
            &mut out,
            0,
            layout,
            0,
            count as u32,
            count as u32,
            direct_row_count,
        );
        let mut key_offsets = Vec::with_capacity(2 * count + 1);
        let mut cursor = 0u32;
        for (first_key, last_key, _) in entries {
            key_offsets.push(cursor);
            cursor += first_key.len() as u32;
            key_offsets.push(cursor);
            cursor += last_key.len() as u32;
        }
        key_offsets.push(cursor);
        for offset in &key_offsets {
            put_u32(&mut out, *offset);
        }
        for (_, _, direct_rows) in entries {
            out.extend_from_slice(&direct_rows.to_le_bytes());
        }
        for (first_key, last_key, _) in entries {
            out.extend_from_slice(first_key);
            out.extend_from_slice(last_key);
        }
        out
    }

    /// Encodes an internal node in the v2 layout.  Children carry cumulative
    /// entry and row counts so index routing is a binary search rather than a
    /// prefix-sum walk over every child.
    pub(crate) fn encode_internal(
        layout: u8,
        level: u16,
        children: &[(Vec<u8>, Vec<u8>, [u8; 32], u32, u64)],
    ) -> Vec<u8> {
        let count = children.len();
        let entry_count: u32 = children.iter().map(|child| child.3).sum();
        let direct_row_count: u64 = children.iter().map(|child| child.4).sum();
        let mut out = Vec::new();
        header(
            &mut out,
            1,
            layout,
            level,
            count as u32,
            entry_count,
            direct_row_count,
        );
        let mut key_offsets = Vec::with_capacity(2 * count + 1);
        let mut cursor = 0u32;
        for (first_key, last_key, ..) in children {
            key_offsets.push(cursor);
            cursor += first_key.len() as u32;
            key_offsets.push(cursor);
            cursor += last_key.len() as u32;
        }
        key_offsets.push(cursor);
        for offset in &key_offsets {
            put_u32(&mut out, *offset);
        }
        let mut cum_entries = 0u32;
        let mut cum_rows = 0u64;
        for (_, _, node_id, child_entries, child_rows) in children {
            cum_entries += child_entries;
            cum_rows += child_rows;
            out.extend_from_slice(node_id);
            put_u32(&mut out, cum_entries);
            out.extend_from_slice(&cum_rows.to_le_bytes());
        }
        for (first_key, last_key, ..) in children {
            out.extend_from_slice(first_key);
            out.extend_from_slice(last_key);
        }
        out
    }

    /// A borrowed view over an encoded node.  Constructing one copies nothing.
    #[derive(Clone, Copy)]
    pub(crate) struct NodeView<'a> {
        bytes: &'a [u8],
        count: usize,
        kind: u8,
    }

    /// Counts the bytes a lookup actually reads out of the node.
    #[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct Touched {
        pub(crate) bytes: usize,
        pub(crate) key_compares: usize,
    }

    impl<'a> NodeView<'a> {
        pub(crate) fn new(bytes: &'a [u8], touched: &mut Touched) -> Result<Self, LixError> {
            if bytes.len() < HEADER || !bytes.starts_with(V2_MAGIC) {
                return Err(directory_error("node has an unsupported format"));
            }
            touched.bytes += HEADER;
            let kind = bytes[5];
            let count = u32::from_le_bytes(bytes[9..13].try_into().unwrap()) as usize;
            if count == 0 || count > FANOUT {
                return Err(directory_error("node shape is invalid"));
            }
            Ok(Self { bytes, count, kind })
        }

        fn stride(&self) -> usize {
            if self.kind == 0 {
                LEAF_STRIDE
            } else {
                INTERNAL_STRIDE
            }
        }

        fn key_region(&self) -> usize {
            HEADER + 4 * (2 * self.count + 1) + self.stride() * self.count
        }

        fn key_offset(&self, slot: usize) -> usize {
            let at = HEADER + 4 * slot;
            u32::from_le_bytes(self.bytes[at..at + 4].try_into().unwrap()) as usize
        }

        /// `bound`: 0 = first_key, 1 = last_key.
        fn key(&self, index: usize, bound: usize, touched: &mut Touched) -> &'a [u8] {
            let slot = 2 * index + bound;
            let start = self.key_offset(slot);
            let end = self.key_offset(slot + 1);
            touched.bytes += 8 + (end - start);
            let base = self.key_region();
            &self.bytes[base + start..base + end]
        }

        /// Returns the index of the entry whose `[first_key, last_key]` range
        /// contains `probe`, reading only the key slots the binary search
        /// visits.  No allocation.
        pub(crate) fn seek(&self, probe: &[u8], touched: &mut Touched) -> Option<usize> {
            let mut low = 0usize;
            let mut high = self.count;
            while low < high {
                let mid = low + (high - low) / 2;
                touched.key_compares += 1;
                if self.key(mid, 0, touched) <= probe {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            if low == 0 {
                return None;
            }
            let index = low - 1;
            touched.key_compares += 1;
            if probe <= self.key(index, 1, touched) {
                Some(index)
            } else {
                None
            }
        }

        /// The node summary a parent authenticates, read in O(1) from the
        /// header and the two outermost key slots.  The v1 reader recomputes
        /// the same four values by walking every entry in the node.
        pub(crate) fn summary(
            &self,
            touched: &mut Touched,
        ) -> (&'a [u8], &'a [u8], u32, u64) {
            touched.bytes += 12;
            let entry_count =
                u32::from_le_bytes(self.bytes[13..17].try_into().unwrap());
            let direct_row_count =
                u64::from_le_bytes(self.bytes[17..25].try_into().unwrap());
            let first_key = self.key(0, 0, touched);
            let last_key = self.key(self.count - 1, 1, touched);
            (first_key, last_key, entry_count, direct_row_count)
        }

        /// Rejects a node whose *touched* key region is not strictly ordered.
        /// Under v1 this is enforced for the whole node at decode time by
        /// `validate_stored_entries`; under v2 the unvisited region is left to
        /// the content digest and the authenticated parent, and what the
        /// reader touches is checked here.
        pub(crate) fn check_local_order(
            &self,
            index: usize,
            touched: &mut Touched,
        ) -> Result<(), LixError> {
            let first_key = self.key(index, 0, touched);
            let last_key = self.key(index, 1, touched);
            if first_key.is_empty() || first_key > last_key {
                return Err(directory_error("bounded entries overlap or are unordered"));
            }
            if index + 1 < self.count {
                let next_first = self.key(index + 1, 0, touched);
                if last_key >= next_first {
                    return Err(directory_error("bounded entries overlap or are unordered"));
                }
            }
            Ok(())
        }

        /// Reads one child summary.  Fixed stride, so this is random access
        /// with no decoding; the only copy is the 32-byte node id, which goes
        /// on the stack.
        pub(crate) fn child(
            &self,
            index: usize,
            touched: &mut Touched,
        ) -> ([u8; 32], u32, u64) {
            let at = HEADER + 4 * (2 * self.count + 1) + INTERNAL_STRIDE * index;
            touched.bytes += INTERNAL_STRIDE;
            let mut node_id = [0u8; 32];
            node_id.copy_from_slice(&self.bytes[at..at + 32]);
            let cum_entries = u32::from_le_bytes(self.bytes[at + 32..at + 36].try_into().unwrap());
            let cum_rows = u64::from_le_bytes(self.bytes[at + 36..at + 44].try_into().unwrap());
            let (prev_entries, prev_rows) = if index == 0 {
                (0, 0)
            } else {
                let prev = at - INTERNAL_STRIDE;
                touched.bytes += 12;
                (
                    u32::from_le_bytes(self.bytes[prev + 32..prev + 36].try_into().unwrap()),
                    u64::from_le_bytes(self.bytes[prev + 36..prev + 44].try_into().unwrap()),
                )
            };
            (node_id, cum_entries - prev_entries, cum_rows - prev_rows)
        }

        /// Routes an entry index to the child that owns it, by binary search
        /// on the cumulative counts.  This is the path the keyless layouts
        /// (compact replacement, direct rows) take; v1 walks every child.
        pub(crate) fn seek_entry_index(
            &self,
            entry_index: u32,
            touched: &mut Touched,
        ) -> Option<usize> {
            let mut low = 0usize;
            let mut high = self.count;
            while low < high {
                let mid = low + (high - low) / 2;
                let at = HEADER + 4 * (2 * self.count + 1) + INTERNAL_STRIDE * mid + 32;
                touched.bytes += 4;
                let cum = u32::from_le_bytes(self.bytes[at..at + 4].try_into().unwrap());
                if cum <= entry_index {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            (low < self.count).then_some(low)
        }

        /// Materializes exactly one leaf entry.
        pub(crate) fn leaf_entry(
            &self,
            index: usize,
            touched: &mut Touched,
        ) -> MutationDirectoryEntry {
            let first_key = self.key(index, 0, touched).to_vec();
            let last_key = self.key(index, 1, touched).to_vec();
            let at = HEADER + 4 * (2 * self.count + 1) + LEAF_STRIDE * index;
            touched.bytes += LEAF_STRIDE;
            let direct_row_count =
                u16::from_le_bytes(self.bytes[at..at + LEAF_STRIDE].try_into().unwrap());
            MutationDirectoryEntry::Bounded {
                part: CommitStateMutationPart {
                    first_key,
                    last_key,
                    content_digest: [1; 32],
                    replacement_part: None,
                },
                direct_row_count,
            }
        }
    }
}
const MAX_NODE_BYTES: usize = 16 * 1024 * 1024;

#[cfg(any(test, feature = "storage-benches"))]
mod read_accounting {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    pub(super) const DIRECT_ROUTE_CALLS: usize = 0;
    pub(super) const SELECTOR_ALL_ROOTS: usize = 1;
    pub(super) const SELECTOR_RANGE_CALLS: usize = 2;
    pub(super) const SELECTOR_POINT_CALLS: usize = 3;
    pub(super) const SELECTOR_DIRECT_CALLS: usize = 4;
    pub(super) const TRAVERSAL_LEVELS: usize = 5;
    pub(super) const NODE_BATCHES: usize = 6;
    pub(super) const UNIQUE_NODE_IDS: usize = 7;
    pub(super) const NODE_GETS: usize = 8;
    pub(super) const VISITED_NODES: usize = 9;
    pub(super) const EMITTED_RUNS: usize = 10;
    pub(super) const BULK_MANIFEST_ROOTS: usize = 11;
    pub(super) const COMPACT_MEMBER_ROOTS: usize = 12;
    pub(super) const EMPTY_SCHEMA_MEMBER_ROOTS: usize = 13;
    pub(super) const COMPACT_VALUE_ROOTS: usize = 14;
    pub(super) const EMPTY_SCHEMA_VALUE_ROOTS: usize = 15;
    pub(super) const REPOSITORY_INVENTORY_ROOTS: usize = 16;
    pub(super) const FULL_MANIFEST_ROOTS: usize = 17;
    pub(super) const GC_REACHABILITY_ROOTS: usize = 18;
    pub(super) const EXTERNAL_PARTS_LOADED: usize = 19;
    pub(super) const PARTS_DECODED: usize = 20;
    pub(super) const DECODED_ROWS: usize = 21;
    pub(super) const RAW_BYTES: usize = 22;
    pub(super) const RESIDENT_BYTES: usize = 23;
    pub(super) const REQUESTED_ROWS: usize = 24;
    pub(super) const UNIQUE_REQUESTED_ROWS: usize = 25;
    pub(super) const CLAIMED_UNIQUE_ROWS: usize = 26;
    pub(super) const SCATTERED_ROWS: usize = 27;
    pub(super) const EXPLICIT_FALLBACK_ROWS: usize = 28;
    pub(super) const NOT_OWNED_MISSING_COMMIT: usize = 29;
    pub(super) const NOT_OWNED_UNSUPPORTED_LAYOUT: usize = 30;
    pub(super) const NOT_OWNED_ABSENT_INLINE: usize = 31;
    pub(super) const NOT_OWNED_PART_INDEX: usize = 32;
    pub(super) const NOT_OWNED_LOCAL_ROW: usize = 33;
    pub(super) const CORRUPTION_OUTCOMES: usize = 34;
    const COUNTER_COUNT: usize = 35;

    static ACTIVE: AtomicBool = AtomicBool::new(false);
    static COUNTERS: [AtomicU64; COUNTER_COUNT] = [const { AtomicU64::new(0) }; COUNTER_COUNT];

    pub(super) fn reset() {
        ACTIVE.store(false, Ordering::Release);
        for counter in &COUNTERS {
            counter.store(0, Ordering::Relaxed);
        }
        ACTIVE.store(true, Ordering::Release);
    }

    pub(super) fn stop() {
        ACTIVE.store(false, Ordering::Release);
    }

    pub(super) fn add(counter: usize, value: usize) {
        if ACTIVE.load(Ordering::Relaxed) {
            COUNTERS[counter].fetch_add(value as u64, Ordering::Relaxed);
        }
    }

    pub(super) fn get(counter: usize) -> u64 {
        COUNTERS[counter].load(Ordering::Relaxed)
    }
}

// The process-wide counters above are useful for benchmark phases, but their
// absolute values are intentionally not suitable for assertions in a
// parallel test suite.  Keep a task-local test observer for the narrow
// per-invocation properties that must not be contaminated by other tests.
#[cfg(test)]
pub(crate) mod test_read_accounting {
    use std::cell::RefCell;
    use std::future::Future;
    use std::rc::Rc;

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub(crate) struct ScopedMutationDirectoryReadAccounting {
        pub(crate) direct_route_calls: u64,
        pub(crate) selector_all_roots: u64,
        pub(crate) selector_direct_calls: u64,
        pub(crate) external_parts_loaded: u64,
        pub(crate) parts_decoded: u64,
        pub(crate) decoded_rows: u64,
        pub(crate) raw_bytes: u64,
        pub(crate) resident_bytes: u64,
        pub(crate) explicit_fallback_rows: u64,
        pub(crate) not_owned_missing_commit: u64,
        pub(crate) not_owned_unsupported_layout: u64,
        pub(crate) not_owned_absent_inline: u64,
        pub(crate) not_owned_part_index: u64,
        pub(crate) not_owned_local_row: u64,
        pub(crate) node_decodes: u64,
        pub(crate) node_decode_bytes: u64,
        pub(crate) node_decode_entries: u64,
        pub(crate) node_decode_key_bytes: u64,
        pub(crate) node_decode_allocs: u64,
    }

    tokio::task_local! {
        static ACTIVE: Rc<RefCell<ScopedMutationDirectoryReadAccounting>>;
    }

    pub(crate) async fn scope<F>(future: F) -> (F::Output, ScopedMutationDirectoryReadAccounting)
    where
        F: Future,
    {
        let state = Rc::new(RefCell::new(
            ScopedMutationDirectoryReadAccounting::default(),
        ));
        let output = ACTIVE.scope(state.clone(), future).await;
        let accounting = *state.borrow();
        (output, accounting)
    }

    fn update(update: impl FnOnce(&mut ScopedMutationDirectoryReadAccounting)) {
        let _ = ACTIVE.try_with(|state| {
            update(&mut state.borrow_mut());
        });
    }

    pub(crate) fn record_direct_route_start() {
        update(|state| state.direct_route_calls = state.direct_route_calls.saturating_add(1));
    }

    pub(crate) fn record_selector_all_roots(roots: usize) {
        update(|state| {
            state.selector_all_roots = state.selector_all_roots.saturating_add(roots as u64);
        });
    }

    pub(crate) fn record_selector_direct() {
        update(|state| {
            state.selector_direct_calls = state.selector_direct_calls.saturating_add(1);
        });
    }

    pub(crate) fn record_external_parts_loaded(parts: usize) {
        update(|state| {
            state.external_parts_loaded = state.external_parts_loaded.saturating_add(parts as u64);
        });
    }

    pub(crate) fn record_part_decoded(rows: usize, raw_bytes: usize, resident_bytes: usize) {
        update(|state| {
            state.parts_decoded = state.parts_decoded.saturating_add(1);
            state.decoded_rows = state.decoded_rows.saturating_add(rows as u64);
            state.raw_bytes = state.raw_bytes.saturating_add(raw_bytes as u64);
            state.resident_bytes = state.resident_bytes.saturating_add(resident_bytes as u64);
        });
    }

    pub(crate) fn record_explicit_fallback(rows: usize) {
        update(|state| {
            state.explicit_fallback_rows = state.explicit_fallback_rows.saturating_add(rows as u64);
        });
    }

    pub(crate) fn record_not_owned_missing_commit(rows: usize) {
        update(|state| {
            state.not_owned_missing_commit =
                state.not_owned_missing_commit.saturating_add(rows as u64);
        });
    }

    pub(crate) fn record_not_owned_unsupported_layout(rows: usize) {
        update(|state| {
            state.not_owned_unsupported_layout = state
                .not_owned_unsupported_layout
                .saturating_add(rows as u64);
        });
    }

    pub(crate) fn record_not_owned_absent_inline(rows: usize) {
        update(|state| {
            state.not_owned_absent_inline =
                state.not_owned_absent_inline.saturating_add(rows as u64);
        });
    }

    pub(crate) fn record_not_owned_part_index(rows: usize) {
        update(|state| {
            state.not_owned_part_index = state.not_owned_part_index.saturating_add(rows as u64);
        });
    }

    pub(crate) fn record_node_decode(
        payload_bytes: usize,
        entries: usize,
        key_bytes: usize,
        allocs: usize,
    ) {
        update(|state| {
            state.node_decodes = state.node_decodes.saturating_add(1);
            state.node_decode_bytes = state.node_decode_bytes.saturating_add(payload_bytes as u64);
            state.node_decode_entries = state.node_decode_entries.saturating_add(entries as u64);
            state.node_decode_key_bytes =
                state.node_decode_key_bytes.saturating_add(key_bytes as u64);
            state.node_decode_allocs = state.node_decode_allocs.saturating_add(allocs as u64);
        });
    }

    pub(crate) fn record_not_owned_local_row(rows: usize) {
        update(|state| {
            state.not_owned_local_row = state.not_owned_local_row.saturating_add(rows as u64);
        });
    }
}

#[cfg(any(test, feature = "storage-benches"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MutationDirectoryReadAccounting {
    pub direct_route_calls: u64,
    pub selector_all_roots: u64,
    pub selector_range_calls: u64,
    pub selector_point_calls: u64,
    pub selector_direct_calls: u64,
    pub traversal_levels: u64,
    pub node_batches: u64,
    pub unique_node_ids: u64,
    pub node_gets: u64,
    pub visited_nodes: u64,
    pub emitted_runs: u64,
    pub bulk_manifest_roots: u64,
    pub compact_member_roots: u64,
    pub empty_schema_member_roots: u64,
    pub compact_value_roots: u64,
    pub empty_schema_value_roots: u64,
    pub repository_inventory_roots: u64,
    pub full_manifest_roots: u64,
    pub gc_reachability_roots: u64,
    pub external_parts_loaded: u64,
    pub parts_decoded: u64,
    pub decoded_rows: u64,
    pub raw_bytes: u64,
    pub resident_bytes: u64,
    pub requested_rows: u64,
    pub unique_requested_rows: u64,
    pub claimed_unique_rows: u64,
    pub scattered_rows: u64,
    pub explicit_fallback_rows: u64,
    pub not_owned_missing_commit: u64,
    pub not_owned_unsupported_layout: u64,
    pub not_owned_absent_inline: u64,
    pub not_owned_part_index: u64,
    pub not_owned_local_row: u64,
    pub corruption_outcomes: u64,
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn reset_mutation_directory_read_accounting() {
    read_accounting::reset();
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn snapshot_mutation_directory_read_accounting() -> MutationDirectoryReadAccounting {
    use read_accounting as counters;
    counters::stop();
    MutationDirectoryReadAccounting {
        direct_route_calls: counters::get(counters::DIRECT_ROUTE_CALLS),
        selector_all_roots: counters::get(counters::SELECTOR_ALL_ROOTS),
        selector_range_calls: counters::get(counters::SELECTOR_RANGE_CALLS),
        selector_point_calls: counters::get(counters::SELECTOR_POINT_CALLS),
        selector_direct_calls: counters::get(counters::SELECTOR_DIRECT_CALLS),
        traversal_levels: counters::get(counters::TRAVERSAL_LEVELS),
        node_batches: counters::get(counters::NODE_BATCHES),
        unique_node_ids: counters::get(counters::UNIQUE_NODE_IDS),
        node_gets: counters::get(counters::NODE_GETS),
        visited_nodes: counters::get(counters::VISITED_NODES),
        emitted_runs: counters::get(counters::EMITTED_RUNS),
        bulk_manifest_roots: counters::get(counters::BULK_MANIFEST_ROOTS),
        compact_member_roots: counters::get(counters::COMPACT_MEMBER_ROOTS),
        empty_schema_member_roots: counters::get(counters::EMPTY_SCHEMA_MEMBER_ROOTS),
        compact_value_roots: counters::get(counters::COMPACT_VALUE_ROOTS),
        empty_schema_value_roots: counters::get(counters::EMPTY_SCHEMA_VALUE_ROOTS),
        repository_inventory_roots: counters::get(counters::REPOSITORY_INVENTORY_ROOTS),
        full_manifest_roots: counters::get(counters::FULL_MANIFEST_ROOTS),
        gc_reachability_roots: counters::get(counters::GC_REACHABILITY_ROOTS),
        external_parts_loaded: counters::get(counters::EXTERNAL_PARTS_LOADED),
        parts_decoded: counters::get(counters::PARTS_DECODED),
        decoded_rows: counters::get(counters::DECODED_ROWS),
        raw_bytes: counters::get(counters::RAW_BYTES),
        resident_bytes: counters::get(counters::RESIDENT_BYTES),
        requested_rows: counters::get(counters::REQUESTED_ROWS),
        unique_requested_rows: counters::get(counters::UNIQUE_REQUESTED_ROWS),
        claimed_unique_rows: counters::get(counters::CLAIMED_UNIQUE_ROWS),
        scattered_rows: counters::get(counters::SCATTERED_ROWS),
        explicit_fallback_rows: counters::get(counters::EXPLICIT_FALLBACK_ROWS),
        not_owned_missing_commit: counters::get(counters::NOT_OWNED_MISSING_COMMIT),
        not_owned_unsupported_layout: counters::get(counters::NOT_OWNED_UNSUPPORTED_LAYOUT),
        not_owned_absent_inline: counters::get(counters::NOT_OWNED_ABSENT_INLINE),
        not_owned_part_index: counters::get(counters::NOT_OWNED_PART_INDEX),
        not_owned_local_row: counters::get(counters::NOT_OWNED_LOCAL_ROW),
        corruption_outcomes: counters::get(counters::CORRUPTION_OUTCOMES),
    }
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn record_direct_route_start(requested_rows: usize) {
    read_accounting::add(read_accounting::DIRECT_ROUTE_CALLS, 1);
    read_accounting::add(read_accounting::REQUESTED_ROWS, requested_rows);
    #[cfg(test)]
    test_read_accounting::record_direct_route_start();
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn record_direct_route_unique_rows(unique_rows: usize) {
    read_accounting::add(read_accounting::UNIQUE_REQUESTED_ROWS, unique_rows);
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn record_direct_route_claimed_rows(claimed_rows: usize) {
    read_accounting::add(read_accounting::CLAIMED_UNIQUE_ROWS, claimed_rows);
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn record_direct_route_scattered_rows(scattered_rows: usize) {
    read_accounting::add(read_accounting::SCATTERED_ROWS, scattered_rows);
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn record_direct_route_explicit_fallback(rows: usize) {
    read_accounting::add(read_accounting::EXPLICIT_FALLBACK_ROWS, rows);
    #[cfg(test)]
    test_read_accounting::record_explicit_fallback(rows);
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn record_direct_route_not_owned(reason: MutationDirectoryNotOwnedReason, rows: usize) {
    let counter = match reason {
        MutationDirectoryNotOwnedReason::UnsupportedLayout => {
            read_accounting::NOT_OWNED_UNSUPPORTED_LAYOUT
        }
        MutationDirectoryNotOwnedReason::PartIndexOutOfRange => {
            read_accounting::NOT_OWNED_PART_INDEX
        }
        MutationDirectoryNotOwnedReason::LocalRowOutOfRange => read_accounting::NOT_OWNED_LOCAL_ROW,
    };
    read_accounting::add(counter, rows);
    #[cfg(test)]
    match reason {
        MutationDirectoryNotOwnedReason::UnsupportedLayout => {
            test_read_accounting::record_not_owned_unsupported_layout(rows)
        }
        MutationDirectoryNotOwnedReason::PartIndexOutOfRange => {
            test_read_accounting::record_not_owned_part_index(rows)
        }
        MutationDirectoryNotOwnedReason::LocalRowOutOfRange => {
            test_read_accounting::record_not_owned_local_row(rows)
        }
    }
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn record_direct_route_missing_commit(rows: usize) {
    read_accounting::add(read_accounting::NOT_OWNED_MISSING_COMMIT, rows);
    #[cfg(test)]
    test_read_accounting::record_not_owned_missing_commit(rows);
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn record_direct_route_absent_inline(rows: usize) {
    read_accounting::add(read_accounting::NOT_OWNED_ABSENT_INLINE, rows);
    #[cfg(test)]
    test_read_accounting::record_not_owned_absent_inline(rows);
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn record_direct_route_corruption() {
    read_accounting::add(read_accounting::CORRUPTION_OUTCOMES, 1);
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn record_direct_external_parts_loaded(parts: usize) {
    read_accounting::add(read_accounting::EXTERNAL_PARTS_LOADED, parts);
    #[cfg(test)]
    test_read_accounting::record_external_parts_loaded(parts);
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn record_direct_part_decoded(rows: usize, raw_bytes: usize, resident_bytes: usize) {
    read_accounting::add(read_accounting::PARTS_DECODED, 1);
    read_accounting::add(read_accounting::DECODED_ROWS, rows);
    read_accounting::add(read_accounting::RAW_BYTES, raw_bytes);
    read_accounting::add(read_accounting::RESIDENT_BYTES, resident_bytes);
    #[cfg(test)]
    test_read_accounting::record_part_decoded(rows, raw_bytes, resident_bytes);
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) struct DirectRouteAccountingGuard {
    complete: bool,
}

#[cfg(any(test, feature = "storage-benches"))]
impl DirectRouteAccountingGuard {
    pub(crate) fn new() -> Self {
        Self { complete: false }
    }

    pub(crate) fn finish(&mut self) {
        self.complete = true;
    }
}

#[cfg(any(test, feature = "storage-benches"))]
impl Drop for DirectRouteAccountingGuard {
    fn drop(&mut self) {
        if !self.complete {
            record_direct_route_corruption();
        }
    }
}

pub(crate) const LAYOUT_BOUNDED_INDIRECT: u8 = 1;
pub(crate) const LAYOUT_BOUNDED_DIRECT: u8 = 2;
pub(crate) const LAYOUT_COMPACT_REPLACEMENT: u8 = 3;
pub(crate) const LAYOUT_DIRECT_ROWS_ONLY: u8 = 4;

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct MutationDirectoryRoot {
    pub(crate) root_id: [u8; 32],
    pub(crate) root_digest: [u8; 32],
    pub(crate) entry_count: u32,
    pub(crate) direct_row_count: u64,
    pub(crate) tree_height: u16,
    pub(crate) layout: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MutationDirectoryEntry {
    Bounded {
        part: CommitStateMutationPart,
        direct_row_count: u16,
    },
    CompactReplacement {
        content_digest: [u8; 32],
        direct_row_count: u16,
    },
    DirectAddress {
        direct_row_count: u16,
    },
}

impl MutationDirectoryEntry {
    fn direct_row_count(&self) -> u16 {
        match self {
            Self::Bounded {
                direct_row_count, ..
            }
            | Self::CompactReplacement {
                direct_row_count, ..
            }
            | Self::DirectAddress { direct_row_count } => *direct_row_count,
        }
    }

    #[cfg(test)]
    fn first_key(&self) -> &[u8] {
        match self {
            Self::Bounded { part, .. } => &part.first_key,
            Self::CompactReplacement { .. } => &[],
            Self::DirectAddress { .. } => &[],
        }
    }

    #[cfg(test)]
    fn last_key(&self) -> &[u8] {
        match self {
            Self::Bounded { part, .. } => &part.last_key,
            Self::CompactReplacement { .. } => &[],
            Self::DirectAddress { .. } => &[],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MutationDirectoryKeyRange {
    pub(crate) start: Bytes,
    pub(crate) end: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MutationDirectoryDirectCoordinate {
    pub(crate) part_index: u32,
    pub(crate) local_row: u16,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum MutationDirectoryReadSelection<'a> {
    All(MutationDirectoryFullTraversalContext),
    SortedRanges(&'a [MutationDirectoryKeyRange]),
    SortedUniquePoints(&'a [Bytes]),
    SortedUniqueDirectCoordinates(&'a [MutationDirectoryDirectCoordinate]),
    /// Selects physical parts by authenticated directory ordinal without
    /// claiming that the ordinal is encoded in the ChangeId. Explicit
    /// locators use this for indirect layouts and validate the row ordinal
    /// against the selected immutable part after it is decoded.
    SortedUniquePartCoordinates(&'a [MutationDirectoryDirectCoordinate]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MutationDirectoryFullTraversalContext {
    BulkCommitStateManifests,
    CompactMemberScan,
    EmptySchemaMemberScan,
    CompactValueScan,
    EmptySchemaValueScan,
    RepositoryInventory,
    FullManifestExpansion,
    #[cfg(test)]
    Test,
}

#[cfg(any(test, feature = "storage-benches"))]
fn record_full_traversal(context: MutationDirectoryFullTraversalContext, roots: usize) {
    use read_accounting as counters;
    #[cfg(test)]
    test_read_accounting::record_selector_all_roots(roots);
    counters::add(counters::SELECTOR_ALL_ROOTS, roots);
    let counter = match context {
        MutationDirectoryFullTraversalContext::BulkCommitStateManifests => {
            counters::BULK_MANIFEST_ROOTS
        }
        MutationDirectoryFullTraversalContext::CompactMemberScan => counters::COMPACT_MEMBER_ROOTS,
        MutationDirectoryFullTraversalContext::EmptySchemaMemberScan => {
            counters::EMPTY_SCHEMA_MEMBER_ROOTS
        }
        MutationDirectoryFullTraversalContext::CompactValueScan => counters::COMPACT_VALUE_ROOTS,
        MutationDirectoryFullTraversalContext::EmptySchemaValueScan => {
            counters::EMPTY_SCHEMA_VALUE_ROOTS
        }
        MutationDirectoryFullTraversalContext::RepositoryInventory => {
            counters::REPOSITORY_INVENTORY_ROOTS
        }
        MutationDirectoryFullTraversalContext::FullManifestExpansion => {
            counters::FULL_MANIFEST_ROOTS
        }
        #[cfg(test)]
        MutationDirectoryFullTraversalContext::Test => return,
    };
    counters::add(counter, roots);
}

#[cfg(any(test, feature = "storage-benches"))]
fn record_selector(selection: MutationDirectoryReadSelection<'_>) {
    use read_accounting as counters;
    match selection {
        MutationDirectoryReadSelection::All(context) => record_full_traversal(context, 1),
        MutationDirectoryReadSelection::SortedRanges(_) => {
            counters::add(counters::SELECTOR_RANGE_CALLS, 1);
        }
        MutationDirectoryReadSelection::SortedUniquePoints(_) => {
            counters::add(counters::SELECTOR_POINT_CALLS, 1);
        }
        MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(_) => {
            counters::add(counters::SELECTOR_DIRECT_CALLS, 1);
            #[cfg(test)]
            test_read_accounting::record_selector_direct();
        }
        MutationDirectoryReadSelection::SortedUniquePartCoordinates(_) => {
            counters::add(counters::SELECTOR_DIRECT_CALLS, 1);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MutationDirectoryNotOwnedReason {
    UnsupportedLayout,
    PartIndexOutOfRange,
    LocalRowOutOfRange,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MutationDirectoryNotOwnedSpan {
    pub(crate) selector_span: Range<usize>,
    pub(crate) reason: MutationDirectoryNotOwnedReason,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MutationDirectoryPartRun {
    pub(crate) entry_index: u32,
    pub(crate) selector_span: Range<usize>,
    pub(crate) entry: MutationDirectoryEntry,
}

#[derive(Debug)]
pub(crate) struct AuthenticatedMutationPartReadPlan {
    runs: Vec<MutationDirectoryPartRun>,
    direct_not_owned: Vec<MutationDirectoryNotOwnedSpan>,
    #[cfg(test)]
    visited_node_count: usize,
    #[cfg(test)]
    node_summary_owner_count: usize,
    #[cfg(test)]
    node_summary_clone_count: usize,
    #[cfg(test)]
    part_clone_count: usize,
}

impl AuthenticatedMutationPartReadPlan {
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.runs.len()
    }

    pub(crate) fn into_runs(self) -> Vec<MutationDirectoryPartRun> {
        debug_assert!(self.direct_not_owned.is_empty());
        self.runs
    }

    pub(crate) fn into_direct_routes(
        self,
    ) -> (
        Vec<MutationDirectoryPartRun>,
        Vec<MutationDirectoryNotOwnedSpan>,
    ) {
        (self.runs, self.direct_not_owned)
    }

    #[cfg(test)]
    pub(crate) fn visited_node_count(&self) -> usize {
        self.visited_node_count
    }

    #[cfg(test)]
    pub(crate) fn node_summary_owner_count(&self) -> usize {
        self.node_summary_owner_count
    }

    #[cfg(test)]
    pub(crate) fn node_summary_clone_count(&self) -> usize {
        self.node_summary_clone_count
    }

    #[cfg(test)]
    pub(crate) fn part_clone_count(&self) -> usize {
        self.part_clone_count
    }
}

#[derive(Debug)]
pub(crate) struct BuiltMutationDirectory {
    pub(crate) root: MutationDirectoryRoot,
    nodes: BTreeMap<[u8; 32], Bytes>,
}

impl BuiltMutationDirectory {
    pub(crate) fn stage(&self, writes: &mut StorageWriteSet) -> Result<(), LixError> {
        for (node_id, bytes) in &self.nodes {
            if let Some(existing) = writes.staged_value(MUTATION_DIRECTORY_NODE_SPACE, node_id) {
                if existing != *bytes {
                    return Err(directory_error("content ID has conflicting staged bytes"));
                }
                continue;
            }
            writes.put(
                MUTATION_DIRECTORY_NODE_SPACE,
                StorageKey(Bytes::copy_from_slice(node_id)),
                StorageValue {
                    bytes: bytes.clone(),
                },
            );
        }
        Ok(())
    }

    pub(crate) fn node_bytes(&self) -> &BTreeMap<[u8; 32], Bytes> {
        &self.nodes
    }
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
enum StoredEntry {
    Bounded {
        #[musli(bytes)]
        first_key: Vec<u8>,
        #[musli(bytes)]
        last_key: Vec<u8>,
        content_digest: [u8; 32],
        #[musli(with = storage_codec::option)]
        replacement_part: Option<super::types::StoredReplacementPart>,
        direct_row_count: u16,
    },
    CompactReplacement {
        content_digest: [u8; 32],
        direct_row_count: u16,
    },
    DirectAddress {
        direct_row_count: u16,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredChild {
    #[musli(bytes)]
    first_key: Vec<u8>,
    #[musli(bytes)]
    last_key: Vec<u8>,
    node_id: [u8; 32],
    entry_count: u32,
    direct_row_count: u64,
    level: u16,
    layout: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
enum StoredNode {
    Leaf {
        layout: u8,
        entries: Vec<StoredEntry>,
    },
    Internal {
        layout: u8,
        level: u16,
        children: Vec<StoredChild>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeSummary {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    node_id: [u8; 32],
    entry_count: u32,
    direct_row_count: u64,
    level: u16,
    layout: u8,
}

impl From<&NodeSummary> for StoredChild {
    fn from(summary: &NodeSummary) -> Self {
        Self {
            first_key: summary.first_key.clone(),
            last_key: summary.last_key.clone(),
            node_id: summary.node_id,
            entry_count: summary.entry_count,
            direct_row_count: summary.direct_row_count,
            level: summary.level,
            layout: summary.layout,
        }
    }
}

#[cfg(test)]
impl From<&StoredChild> for NodeSummary {
    fn from(child: &StoredChild) -> Self {
        Self {
            first_key: child.first_key.clone(),
            last_key: child.last_key.clone(),
            node_id: child.node_id,
            entry_count: child.entry_count,
            direct_row_count: child.direct_row_count,
            level: child.level,
            layout: child.layout,
        }
    }
}

impl From<StoredChild> for NodeSummary {
    fn from(child: StoredChild) -> Self {
        Self {
            first_key: child.first_key,
            last_key: child.last_key,
            node_id: child.node_id,
            entry_count: child.entry_count,
            direct_row_count: child.direct_row_count,
            level: child.level,
            layout: child.layout,
        }
    }
}

#[cfg(test)]
pub(crate) fn build_mutation_directory(
    layout: u8,
    entries: &[MutationDirectoryEntry],
) -> Result<BuiltMutationDirectory, LixError> {
    validate_entries(layout, entries)?;
    build_stored_mutation_directory(layout, entries.iter().map(stored_entry))
}

pub(crate) fn build_bounded_mutation_directory(
    parts: &[CommitStateMutationPart],
    direct_row_counts: Option<&[u16]>,
) -> Result<BuiltMutationDirectory, LixError> {
    if direct_row_counts.is_some_and(|rows| rows.len() != parts.len()) {
        return Err(directory_error(
            "bounded direct-row counts do not match part count",
        ));
    }
    let layout = if direct_row_counts.is_some() {
        LAYOUT_BOUNDED_DIRECT
    } else {
        LAYOUT_BOUNDED_INDIRECT
    };
    if parts.iter().any(|part| !valid_part(part))
        || parts
            .windows(2)
            .any(|pair| pair[0].last_key >= pair[1].first_key)
        || direct_row_counts.is_some_and(|rows| rows.contains(&0))
    {
        return Err(directory_error("bounded entries overlap or are invalid"));
    }
    let entries = parts
        .iter()
        .enumerate()
        .map(|(index, part)| StoredEntry::Bounded {
            first_key: part.first_key.clone(),
            last_key: part.last_key.clone(),
            content_digest: part.content_digest,
            replacement_part: part.replacement_part.clone(),
            direct_row_count: direct_row_counts.map_or(0, |rows| rows[index]),
        });
    build_stored_mutation_directory(layout, entries)
}

pub(crate) fn build_compact_replacement_mutation_directory(
    content_digests: &[[u8; 32]],
    direct_row_counts: &[u16],
) -> Result<BuiltMutationDirectory, LixError> {
    if content_digests.len() != direct_row_counts.len() {
        return Err(directory_error(
            "compact replacement counts do not match digest count",
        ));
    }
    if content_digests.contains(&[0; 32]) || direct_row_counts.contains(&0) {
        return Err(directory_error("compact replacement entry is invalid"));
    }
    build_stored_mutation_directory(
        LAYOUT_COMPACT_REPLACEMENT,
        content_digests
            .iter()
            .copied()
            .zip(direct_row_counts.iter().copied())
            .map(
                |(content_digest, direct_row_count)| StoredEntry::CompactReplacement {
                    content_digest,
                    direct_row_count,
                },
            ),
    )
}

pub(crate) fn build_direct_rows_mutation_directory(
    direct_row_counts: &[u16],
) -> Result<BuiltMutationDirectory, LixError> {
    if direct_row_counts.contains(&0) {
        return Err(directory_error("direct-address entry is invalid"));
    }
    build_stored_mutation_directory(
        LAYOUT_DIRECT_ROWS_ONLY,
        direct_row_counts
            .iter()
            .copied()
            .map(|direct_row_count| StoredEntry::DirectAddress { direct_row_count }),
    )
}

fn build_stored_mutation_directory<I>(
    layout: u8,
    entries: I,
) -> Result<BuiltMutationDirectory, LixError>
where
    I: ExactSizeIterator<Item = StoredEntry>,
{
    validate_layout(layout)?;
    if entries.len() == 0 {
        return Err(directory_error("cannot build an empty directory"));
    }
    let mut nodes = BTreeMap::new();
    let mut entries = entries.into_iter();
    let mut level = balanced_chunk_lengths(entries.len())
        .map(|chunk_len| {
            let stored = StoredNode::Leaf {
                layout,
                entries: entries.by_ref().take(chunk_len).collect(),
            };
            stage_encoded_node(&mut nodes, stored)
        })
        .collect::<Result<Vec<_>, _>>()?;
    debug_assert!(entries.next().is_none());
    let mut tree_height = 1u16;
    while level.len() > 1 {
        level = balanced_chunks(&level)
            .into_iter()
            .map(|chunk| {
                let child_level = chunk[0].level;
                let stored = StoredNode::Internal {
                    layout,
                    level: child_level
                        .checked_add(1)
                        .ok_or_else(|| directory_error("tree height overflows"))?,
                    children: chunk.iter().map(StoredChild::from).collect(),
                };
                stage_encoded_node(&mut nodes, stored)
            })
            .collect::<Result<Vec<_>, _>>()?;
        tree_height = tree_height
            .checked_add(1)
            .ok_or_else(|| directory_error("tree height overflows"))?;
    }
    let summary = level.pop().expect("non-empty directory has a root");
    let root = MutationDirectoryRoot {
        root_id: summary.node_id,
        root_digest: root_digest(
            summary.node_id,
            summary.entry_count,
            summary.direct_row_count,
            tree_height,
            layout,
        ),
        entry_count: summary.entry_count,
        direct_row_count: summary.direct_row_count,
        tree_height,
        layout,
    };
    validate_root(&root)?;
    Ok(BuiltMutationDirectory { root, nodes })
}

/// Loads complete authenticated plans for many roots level-by-level. Shared
/// nodes and same-level frontiers issue one physical point-read batch instead
/// of one request chain per commit. The output remains the same ordered run
/// contract as selective reads; there is no second flat-directory interface.
pub(crate) async fn load_all_mutation_part_read_plans(
    store: &(impl StorageAdapterRead + ?Sized),
    roots: &[MutationDirectoryRoot],
    context: MutationDirectoryFullTraversalContext,
) -> Result<Vec<AuthenticatedMutationPartReadPlan>, LixError> {
    let valid_context = matches!(
        context,
        MutationDirectoryFullTraversalContext::BulkCommitStateManifests
            | MutationDirectoryFullTraversalContext::RepositoryInventory
    );
    #[cfg(test)]
    let valid_context =
        valid_context || matches!(context, MutationDirectoryFullTraversalContext::Test);
    if !valid_context {
        return Err(directory_error(
            "batched full traversal received a single-root context",
        ));
    }
    for root in roots {
        validate_root(root)?;
    }
    #[cfg(any(test, feature = "storage-benches"))]
    record_full_traversal(context, roots.len());
    let mut frontiers = roots
        .iter()
        .map(|root| vec![(root.root_id, 0u32, None::<NodeSummary>)])
        .collect::<Vec<_>>();
    let mut outputs = roots
        .iter()
        .map(|root| Vec::with_capacity(root.entry_count as usize))
        .collect::<Vec<_>>();
    #[cfg(test)]
    let mut visited_node_counts = vec![0usize; roots.len()];
    #[cfg(test)]
    let mut node_summary_owner_counts = vec![0usize; roots.len()];
    #[cfg(test)]
    let mut node_summary_clone_counts = vec![0usize; roots.len()];
    #[cfg(test)]
    let mut part_clone_counts = vec![0usize; roots.len()];
    while frontiers.iter().any(|frontier| !frontier.is_empty()) {
        #[cfg(any(test, feature = "storage-benches"))]
        read_accounting::add(read_accounting::TRAVERSAL_LEVELS, 1);
        let mut node_ids = Vec::new();
        let mut use_counts = HashMap::<[u8; 32], usize>::new();
        for (node_id, _, _) in frontiers.iter().flatten() {
            let node_id = *node_id;
            match use_counts.entry(node_id) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(1);
                    node_ids.push(node_id);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    *entry.get_mut() += 1;
                }
            }
        }
        let loaded_nodes = load_nodes(store, &node_ids, true).await?;
        let mut loaded = node_ids
            .into_iter()
            .zip(loaded_nodes)
            .map(|(node_id, node)| {
                let use_count = use_counts[&node_id];
                (node_id, (node, use_count))
            })
            .collect::<HashMap<_, _>>();
        for (root_index, frontier) in frontiers.iter_mut().enumerate() {
            let mut next = Vec::new();
            for (node_id, base_index, expected) in std::mem::take(frontier) {
                let remaining_uses = loaded
                    .get(&node_id)
                    .map(|(_, use_count)| *use_count)
                    .ok_or_else(|| directory_error("directory batch omitted a node"))?;
                let node = if remaining_uses == 1 {
                    loaded
                        .remove(&node_id)
                        .expect("loaded node use was just observed")
                        .0
                } else {
                    let (node, use_count) = loaded
                        .get_mut(&node_id)
                        .expect("loaded node use was just observed");
                    *use_count -= 1;
                    #[cfg(test)]
                    match node {
                        StoredNode::Leaf { entries, .. } => {
                            part_clone_counts[root_index] += entries.len();
                        }
                        StoredNode::Internal { children, .. } => {
                            node_summary_clone_counts[root_index] += children.len();
                        }
                    }
                    node.clone()
                };
                #[cfg(test)]
                {
                    visited_node_counts[root_index] += 1;
                }
                validate_loaded_node(
                    &node,
                    node_id,
                    expected.as_ref(),
                    &roots[root_index],
                    "directory batch",
                )?;
                match node {
                    StoredNode::Leaf { entries, .. } => {
                        for (index, entry) in entries.into_iter().enumerate() {
                            outputs[root_index].push(MutationDirectoryPartRun {
                                entry_index: base_index
                                    .checked_add(u32::try_from(index).map_err(|_| {
                                        directory_error("leaf entry index overflows")
                                    })?)
                                    .ok_or_else(|| directory_error("entry index overflows"))?,
                                selector_span: 0..0,
                                entry: runtime_entry(entry)?,
                            });
                        }
                    }
                    StoredNode::Internal { children, .. } => {
                        let mut preceding = 0u32;
                        for child in children {
                            let child_base = base_index
                                .checked_add(preceding)
                                .ok_or_else(|| directory_error("entry offset overflows"))?;
                            preceding = preceding
                                .checked_add(child.entry_count)
                                .ok_or_else(|| directory_error("entry offset overflows"))?;
                            let node_id = child.node_id;
                            next.push((node_id, child_base, Some(NodeSummary::from(child))));
                            #[cfg(test)]
                            {
                                node_summary_owner_counts[root_index] += 1;
                            }
                        }
                    }
                }
            }
            *frontier = next;
        }
    }
    for (root, runs) in roots.iter().zip(&outputs) {
        if runs.len() != root.entry_count as usize
            || runs
                .iter()
                .map(|run| run.entry.direct_row_count())
                .map(u64::from)
                .sum::<u64>()
                != root.direct_row_count
            || runs
                .iter()
                .enumerate()
                .any(|(index, run)| run.entry_index as usize != index)
        {
            return Err(directory_error("directory closure disagrees with its root"));
        }
    }
    #[cfg(any(test, feature = "storage-benches"))]
    read_accounting::add(
        read_accounting::EMITTED_RUNS,
        outputs.iter().map(Vec::len).sum(),
    );
    Ok(outputs
        .into_iter()
        .enumerate()
        .map(|(root_index, runs)| {
            #[cfg(not(test))]
            let _ = root_index;
            AuthenticatedMutationPartReadPlan {
                runs,
                direct_not_owned: Vec::new(),
                #[cfg(test)]
                visited_node_count: visited_node_counts[root_index],
                #[cfg(test)]
                node_summary_owner_count: node_summary_owner_counts[root_index],
                #[cfg(test)]
                node_summary_clone_count: node_summary_clone_counts[root_index],
                #[cfg(test)]
                part_clone_count: part_clone_counts[root_index],
            }
        })
        .collect())
}

/// Selects immutable mutation parts through one authenticated, level-batched
/// directory traversal.
///
/// Points and ranges are caller-canonicalized. The directory rejects any
/// unordered or duplicate point column and any unordered, overlapping, empty,
/// or open-ended non-final range. One run owns one selected part and a compact
/// selector span; keys never own directory bounds.
pub(crate) async fn load_mutation_part_read_plan(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &MutationDirectoryRoot,
    selection: MutationDirectoryReadSelection<'_>,
) -> Result<AuthenticatedMutationPartReadPlan, LixError> {
    if matches!(
        selection,
        MutationDirectoryReadSelection::All(
            MutationDirectoryFullTraversalContext::BulkCommitStateManifests
                | MutationDirectoryFullTraversalContext::RepositoryInventory
        )
    ) {
        return Err(directory_error(
            "single-root full traversal received a batched context",
        ));
    }
    validate_root(root)?;
    validate_selection(root, selection)?;
    #[cfg(any(test, feature = "storage-benches"))]
    record_selector(selection);
    let selector_count = selection.len();
    if !matches!(selection, MutationDirectoryReadSelection::All(_)) && selector_count == 0 {
        return Ok(AuthenticatedMutationPartReadPlan {
            runs: Vec::new(),
            direct_not_owned: Vec::new(),
            #[cfg(test)]
            visited_node_count: 0,
            #[cfg(test)]
            node_summary_owner_count: 0,
            #[cfg(test)]
            node_summary_clone_count: 0,
            #[cfg(test)]
            part_clone_count: 0,
        });
    }

    struct PendingNode {
        node_id: [u8; 32],
        base_index: u32,
        selector_span: Range<usize>,
        expected: Option<NodeSummary>,
    }

    let mut direct_not_owned = Vec::new();
    let mut direct_coverage = Vec::<Range<usize>>::new();
    let mut routed_selector_count = selector_count;
    if let MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(coordinates) = selection {
        if root.layout == LAYOUT_BOUNDED_INDIRECT {
            let selector_span = 0..selector_count;
            return Ok(AuthenticatedMutationPartReadPlan {
                runs: Vec::new(),
                direct_not_owned: vec![MutationDirectoryNotOwnedSpan {
                    selector_span,
                    reason: MutationDirectoryNotOwnedReason::UnsupportedLayout,
                }],
                #[cfg(test)]
                visited_node_count: 0,
                #[cfg(test)]
                node_summary_owner_count: 0,
                #[cfg(test)]
                node_summary_clone_count: 0,
                #[cfg(test)]
                part_clone_count: 0,
            });
        }
        routed_selector_count =
            coordinates.partition_point(|coordinate| coordinate.part_index < root.entry_count);
    }

    let mut frontier = vec![PendingNode {
        node_id: root.root_id,
        base_index: 0,
        selector_span: 0..routed_selector_count,
        expected: None,
    }];
    let mut runs = Vec::new();
    #[cfg(test)]
    let mut visited_node_count = 0usize;
    #[cfg(test)]
    let mut node_summary_owner_count = 0usize;

    while !frontier.is_empty() {
        #[cfg(any(test, feature = "storage-benches"))]
        read_accounting::add(read_accounting::TRAVERSAL_LEVELS, 1);
        let node_ids = frontier
            .iter()
            .map(|pending| pending.node_id)
            .collect::<Vec<_>>();
        let loaded = load_nodes(store, &node_ids, is_bounded(root.layout)).await?;
        let mut next = Vec::new();
        for (pending, node) in frontier.into_iter().zip(loaded) {
            #[cfg(test)]
            {
                visited_node_count += 1;
            }
            validate_loaded_node(
                &node,
                pending.node_id,
                pending.expected.as_ref(),
                root,
                "mutation read-plan",
            )?;
            match node {
                StoredNode::Leaf { entries, .. } => {
                    let mut selector_cursor = pending.selector_span.start;
                    for (index, entry) in entries.into_iter().enumerate() {
                        let entry_index = pending
                            .base_index
                            .checked_add(
                                u32::try_from(index)
                                    .map_err(|_| directory_error("leaf entry index overflows"))?,
                            )
                            .ok_or_else(|| directory_error("entry index overflows"))?;
                        let entry_end = entry_index
                            .checked_add(1)
                            .ok_or_else(|| directory_error("entry index overflows"))?;
                        let selector_span = selection_span_for_entry(
                            selection,
                            &mut selector_cursor,
                            pending.selector_span.end,
                            stored_entry_first_key(&entry),
                            stored_entry_last_key(&entry),
                            entry_index,
                            entry_end,
                            Some(stored_entry_direct_rows(&entry)),
                        )?;
                        let Some(mut selector_span) = selector_span else {
                            continue;
                        };
                        if let MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(
                            coordinates,
                        ) = selection
                        {
                            let owned_end = selector_span.start
                                + coordinates[selector_span.clone()].partition_point(
                                    |coordinate| {
                                        coordinate.local_row < stored_entry_direct_rows(&entry)
                                    },
                                );
                            if selector_span.start < owned_end {
                                let owned_span = selector_span.start..owned_end;
                                direct_coverage.push(owned_span.clone());
                                runs.push(MutationDirectoryPartRun {
                                    entry_index,
                                    selector_span: owned_span,
                                    entry: runtime_entry(entry)?,
                                });
                            }
                            if owned_end < selector_span.end {
                                selector_span.start = owned_end;
                                direct_coverage.push(selector_span.clone());
                                direct_not_owned.push(MutationDirectoryNotOwnedSpan {
                                    selector_span,
                                    reason: MutationDirectoryNotOwnedReason::LocalRowOutOfRange,
                                });
                            }
                            continue;
                        }
                        runs.push(MutationDirectoryPartRun {
                            entry_index,
                            selector_span,
                            entry: runtime_entry(entry)?,
                        });
                    }
                }
                StoredNode::Internal { children, .. } => {
                    let mut preceding = 0u32;
                    let mut selector_cursor = pending.selector_span.start;
                    for child in children {
                        let child_base = pending
                            .base_index
                            .checked_add(preceding)
                            .ok_or_else(|| directory_error("entry offset overflows"))?;
                        preceding = preceding
                            .checked_add(child.entry_count)
                            .ok_or_else(|| directory_error("entry offset overflows"))?;
                        let child_end = child_base
                            .checked_add(child.entry_count)
                            .ok_or_else(|| directory_error("entry offset overflows"))?;
                        let selector_span = selection_span_for_entry(
                            selection,
                            &mut selector_cursor,
                            pending.selector_span.end,
                            &child.first_key,
                            &child.last_key,
                            child_base,
                            child_end,
                            None,
                        )?;
                        let Some(selector_span) = selector_span else {
                            continue;
                        };
                        let node_id = child.node_id;
                        next.push(PendingNode {
                            node_id,
                            base_index: child_base,
                            selector_span,
                            expected: Some(NodeSummary::from(child)),
                        });
                        #[cfg(test)]
                        {
                            node_summary_owner_count += 1;
                        }
                    }
                }
            }
        }
        frontier = next;
    }
    if routed_selector_count < selector_count {
        let selector_span = routed_selector_count..selector_count;
        direct_coverage.push(selector_span.clone());
        direct_not_owned.push(MutationDirectoryNotOwnedSpan {
            selector_span,
            reason: MutationDirectoryNotOwnedReason::PartIndexOutOfRange,
        });
    }
    if runs.windows(2).any(|pair| {
        pair[0].entry_index >= pair[1].entry_index
            || pair[0].selector_span.start > pair[1].selector_span.start
    }) {
        return Err(directory_error(
            "mutation read-plan output is not canonical",
        ));
    }
    if matches!(
        selection,
        MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(_)
    ) && (direct_coverage
        .first()
        .is_none_or(|selector_span| selector_span.start != 0)
        || direct_coverage
            .windows(2)
            .any(|pair| pair[0].end != pair[1].start)
        || direct_coverage
            .last()
            .is_none_or(|selector_span| selector_span.end != selector_count))
    {
        return Err(directory_error(
            "direct-coordinate read-plan output does not cover every selector",
        ));
    }
    #[cfg(any(test, feature = "storage-benches"))]
    read_accounting::add(read_accounting::EMITTED_RUNS, runs.len());
    Ok(AuthenticatedMutationPartReadPlan {
        runs,
        direct_not_owned,
        #[cfg(test)]
        visited_node_count,
        #[cfg(test)]
        node_summary_owner_count,
        #[cfg(test)]
        node_summary_clone_count: 0,
        #[cfg(test)]
        part_clone_count: 0,
    })
}

impl MutationDirectoryReadSelection<'_> {
    fn len(self) -> usize {
        match self {
            Self::All(_) => 0,
            Self::SortedRanges(ranges) => ranges.len(),
            Self::SortedUniquePoints(points) => points.len(),
            Self::SortedUniqueDirectCoordinates(coordinates) => coordinates.len(),
            Self::SortedUniquePartCoordinates(coordinates) => coordinates.len(),
        }
    }
}

fn validate_selection(
    root: &MutationDirectoryRoot,
    selection: MutationDirectoryReadSelection<'_>,
) -> Result<(), LixError> {
    match selection {
        MutationDirectoryReadSelection::All(_) => Ok(()),
        MutationDirectoryReadSelection::SortedUniquePoints(points) => {
            if !is_bounded(root.layout) {
                return Err(directory_error(
                    "point selection requires a bounded directory",
                ));
            }
            if points.iter().any(Bytes::is_empty)
                || points.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err(directory_error(
                    "point selection must be strictly sorted and unique",
                ));
            }
            Ok(())
        }
        MutationDirectoryReadSelection::SortedRanges(ranges) => {
            if !is_bounded(root.layout) {
                return Err(directory_error(
                    "range selection requires a bounded directory",
                ));
            }
            for (index, range) in ranges.iter().enumerate() {
                if range.start.is_empty()
                    || range
                        .end
                        .as_ref()
                        .is_some_and(|end| end.as_ref() <= range.start.as_ref())
                    || (range.end.is_none() && index + 1 != ranges.len())
                {
                    return Err(directory_error(
                        "range selection contains an empty or non-final open range",
                    ));
                }
            }
            if ranges.windows(2).any(|pair| {
                pair[0]
                    .end
                    .as_ref()
                    .is_none_or(|end| end.as_ref() >= pair[1].start.as_ref())
            }) {
                return Err(directory_error(
                    "range selection must be sorted, disjoint, and canonical",
                ));
            }
            Ok(())
        }
        MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(coordinates) => {
            if coordinates.is_empty() {
                return Ok(());
            }
            if coordinates.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(directory_error(
                    "direct-coordinate selection must be strictly sorted and unique",
                ));
            }
            Ok(())
        }
        MutationDirectoryReadSelection::SortedUniquePartCoordinates(coordinates) => {
            if coordinates.windows(2).any(|pair| pair[0] >= pair[1])
                || coordinates
                    .last()
                    .is_some_and(|coordinate| coordinate.part_index >= root.entry_count)
            {
                return Err(directory_error(
                    "part-coordinate selection must be in-range, strictly sorted and unique",
                ));
            }
            Ok(())
        }
    }
}

fn selection_span_for_entry(
    selection: MutationDirectoryReadSelection<'_>,
    cursor: &mut usize,
    selector_end: usize,
    first_key: &[u8],
    last_key: &[u8],
    entry_index: u32,
    entry_end: u32,
    direct_row_count: Option<u16>,
) -> Result<Option<Range<usize>>, LixError> {
    match selection {
        MutationDirectoryReadSelection::All(_) => Ok(Some(0..0)),
        MutationDirectoryReadSelection::SortedUniquePoints(points) => {
            while *cursor < selector_end && points[*cursor].as_ref() < first_key {
                *cursor += 1;
            }
            let start = *cursor;
            while *cursor < selector_end && points[*cursor].as_ref() <= last_key {
                *cursor += 1;
            }
            Ok((start < *cursor).then_some(start..*cursor))
        }
        MutationDirectoryReadSelection::SortedRanges(ranges) => {
            while *cursor < selector_end
                && ranges[*cursor]
                    .end
                    .as_ref()
                    .is_some_and(|end| end.as_ref() <= first_key)
            {
                *cursor += 1;
            }
            let start = *cursor;
            let mut end = start;
            while end < selector_end && ranges[end].start.as_ref() <= last_key {
                end += 1;
            }
            Ok((start < end).then_some(start..end))
        }
        MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(coordinates) => {
            while *cursor < selector_end && coordinates[*cursor].part_index < entry_index {
                *cursor += 1;
            }
            let start = *cursor;
            while *cursor < selector_end && coordinates[*cursor].part_index < entry_end {
                *cursor += 1;
            }
            let _ = direct_row_count;
            Ok((start < *cursor).then_some(start..*cursor))
        }
        MutationDirectoryReadSelection::SortedUniquePartCoordinates(coordinates) => {
            while *cursor < selector_end && coordinates[*cursor].part_index < entry_index {
                *cursor += 1;
            }
            let start = *cursor;
            while *cursor < selector_end && coordinates[*cursor].part_index < entry_end {
                *cursor += 1;
            }
            let _ = direct_row_count;
            Ok((start < *cursor).then_some(start..*cursor))
        }
    }
}

pub(crate) async fn collect_mutation_directory_node_ids(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &MutationDirectoryRoot,
) -> Result<BTreeSet<[u8; 32]>, LixError> {
    validate_root(root)?;
    #[cfg(any(test, feature = "storage-benches"))]
    read_accounting::add(read_accounting::GC_REACHABILITY_ROOTS, 1);
    let mut reachable = BTreeSet::new();
    let mut frontier = vec![(root.root_id, None::<NodeSummary>)];
    while !frontier.is_empty() {
        let node_ids = frontier
            .iter()
            .map(|(node_id, _)| *node_id)
            .collect::<Vec<_>>();
        let loaded = load_nodes(store, &node_ids, is_bounded(root.layout)).await?;
        let mut next = Vec::new();
        for ((node_id, expected), node) in frontier.into_iter().zip(loaded) {
            validate_loaded_node(&node, node_id, expected.as_ref(), root, "reachability")?;
            if !reachable.insert(node_id) {
                continue;
            }
            if let StoredNode::Internal { children, .. } = node {
                next.extend(children.into_iter().map(|child| {
                    let node_id = child.node_id;
                    (node_id, Some(NodeSummary::from(child)))
                }));
            }
        }
        frontier = next;
    }
    Ok(reachable)
}

#[cfg(test)]
pub(crate) fn decode_built_mutation_directory(
    built: &BuiltMutationDirectory,
) -> Result<Vec<MutationDirectoryEntry>, LixError> {
    validate_root(&built.root)?;
    let mut frontier = vec![(built.root.root_id, None::<NodeSummary>)];
    let mut entries = Vec::new();
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for (node_id, expected) in frontier {
            let bytes = built
                .nodes
                .get(&node_id)
                .ok_or_else(|| directory_error("built tree omitted a node"))?;
            let node = decode_node(bytes)?;
            let summary = node_summary(&node, node_id)?;
            match expected {
                Some(expected) if expected != summary => {
                    return Err(directory_error("built child summary mismatch"));
                }
                None if !summary_matches_root(&summary, &built.root) => {
                    return Err(directory_error("built root summary mismatch"));
                }
                _ => {}
            }
            match node {
                StoredNode::Leaf { entries: leaf, .. } => entries.extend(
                    leaf.into_iter()
                        .map(runtime_entry)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                StoredNode::Internal { children, .. } => {
                    next.extend(children.into_iter().map(|child| {
                        let summary = NodeSummary::from(&child);
                        (child.node_id, Some(summary))
                    }))
                }
            }
        }
        frontier = next;
    }
    validate_entries(built.root.layout, &entries)?;
    Ok(entries)
}

async fn load_nodes(
    store: &(impl StorageAdapterRead + ?Sized),
    node_ids: &[[u8; 32]],
    unique: bool,
) -> Result<Vec<StoredNode>, LixError> {
    if node_ids.is_empty() {
        return Ok(Vec::new());
    }
    let keys = node_ids
        .iter()
        .map(|node_id| StorageKey(Bytes::copy_from_slice(node_id)))
        .collect::<Vec<_>>();
    let plan = if unique {
        PointReadPlan::from_unique_keys(MUTATION_DIRECTORY_NODE_SPACE, keys)
    } else {
        PointReadPlan::new(MUTATION_DIRECTORY_NODE_SPACE, &keys)
    };
    #[cfg(any(test, feature = "storage-benches"))]
    {
        read_accounting::add(read_accounting::NODE_BATCHES, 1);
        read_accounting::add(
            read_accounting::UNIQUE_NODE_IDS,
            plan.logical_unique_keys.len(),
        );
        read_accounting::add(read_accounting::NODE_GETS, plan.logical_unique_keys.len());
    }
    let values = plan
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    let nodes = node_ids
        .iter()
        .zip(values)
        .map(|(node_id, value)| {
            let value = value.ok_or_else(|| directory_error("tree references a missing node"))?;
            let StorageProjectedValue::FullValue(bytes) = value else {
                return Err(directory_error("node read omitted its value"));
            };
            if node_digest(&bytes) != *node_id {
                return Err(directory_error("node content digest mismatch"));
            }
            decode_node(&bytes)
        })
        .collect::<Result<Vec<_>, _>>()?;
    #[cfg(any(test, feature = "storage-benches"))]
    read_accounting::add(read_accounting::VISITED_NODES, nodes.len());
    Ok(nodes)
}

fn stage_encoded_node(
    nodes: &mut BTreeMap<[u8; 32], Bytes>,
    node: StoredNode,
) -> Result<NodeSummary, LixError> {
    let bytes = encode_node(&node)?;
    let node_id = node_digest(&bytes);
    let summary = node_summary(&node, node_id)?;
    match nodes.entry(node_id) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(bytes);
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &bytes => {}
        std::collections::btree_map::Entry::Occupied(_) => {
            return Err(directory_error("content ID collision"));
        }
    }
    Ok(summary)
}

fn encode_node(node: &StoredNode) -> Result<Bytes, LixError> {
    validate_node(node)?;
    let payload = storage_codec::encode("commit mutation directory node", node)?;
    if payload.len() > MAX_NODE_BYTES {
        return Err(directory_error("node exceeds its size bound"));
    }
    let mut bytes = Vec::with_capacity(NODE_MAGIC.len() + payload.len());
    bytes.extend_from_slice(NODE_MAGIC);
    bytes.extend_from_slice(&payload);
    Ok(Bytes::from(bytes))
}

fn decode_node(bytes: &[u8]) -> Result<StoredNode, LixError> {
    let Some(payload) = bytes.strip_prefix(NODE_MAGIC) else {
        return Err(directory_error("node has an unsupported format"));
    };
    if payload.len() > MAX_NODE_BYTES {
        return Err(directory_error("node exceeds its size bound"));
    }
    let node = storage_codec::decode("commit mutation directory node", payload)?;
    #[cfg(test)]
    census_node_decode(payload.len(), &node);
    validate_node(&node)?;
    Ok(node)
}

/// Records exactly what decoding one directory node materialized.  The
/// allocation count is the number of heap allocations musli performs for the
/// node: one for the entry/child vector plus, per bounded entry or child, one
/// for `first_key` and one for `last_key`.
#[cfg(test)]
fn census_node_decode(payload_bytes: usize, node: &StoredNode) {
    if alloc_census::armed() {
        return;
    }
    let (entries, key_bytes, allocs) = match node {
        StoredNode::Leaf { entries, .. } => {
            let mut key_bytes = 0usize;
            let mut allocs = 1usize;
            for entry in entries {
                if let StoredEntry::Bounded {
                    first_key,
                    last_key,
                    replacement_part,
                    ..
                } = entry
                {
                    key_bytes += first_key.len() + last_key.len();
                    allocs += 2;
                    if let Some(part) = replacement_part.as_ref() {
                        let _ = part;
                        allocs += 1;
                    }
                }
            }
            (entries.len(), key_bytes, allocs)
        }
        StoredNode::Internal { children, .. } => {
            let key_bytes: usize = children
                .iter()
                .map(|child| child.first_key.len() + child.last_key.len())
                .sum();
            let allocs = 1 + children
                .iter()
                .filter(|child| !child.first_key.is_empty() || !child.last_key.is_empty())
                .count()
                * 2;
            (children.len(), key_bytes, allocs)
        }
    };
    test_read_accounting::record_node_decode(payload_bytes, entries, key_bytes, allocs);
}

fn validate_node(node: &StoredNode) -> Result<(), LixError> {
    match node {
        StoredNode::Leaf { layout, entries } => {
            validate_layout(*layout)?;
            if entries.is_empty() || entries.len() > FANOUT {
                return Err(directory_error("leaf shape is invalid"));
            }
            validate_stored_entries(*layout, entries)
        }
        StoredNode::Internal {
            layout,
            level,
            children,
        } => {
            validate_layout(*layout)?;
            if *level == 0 || children.is_empty() || children.len() > FANOUT {
                return Err(directory_error("internal node shape is invalid"));
            }
            let child_level = children[0].level;
            if child_level.checked_add(1) != Some(*level)
                || children.iter().any(|child| {
                    child.layout != *layout
                        || child.level != child_level
                        || child.node_id == [0; 32]
                        || child.entry_count == 0
                        || (is_bounded(*layout)
                            && (child.first_key.is_empty()
                                || child.first_key.as_slice() > child.last_key.as_slice()))
                        || (!is_bounded(*layout)
                            && (!child.first_key.is_empty() || !child.last_key.is_empty()))
                })
                || (is_bounded(*layout)
                    && children
                        .windows(2)
                        .any(|pair| pair[0].last_key >= pair[1].first_key))
            {
                return Err(directory_error("internal child summaries are invalid"));
            }
            Ok(())
        }
    }
}

#[derive(Clone, Copy)]
struct NodeSummaryRef<'a> {
    first_key: &'a [u8],
    last_key: &'a [u8],
    node_id: [u8; 32],
    entry_count: u32,
    direct_row_count: u64,
    level: u16,
    layout: u8,
}

fn node_summary_ref(node: &StoredNode, node_id: [u8; 32]) -> Result<NodeSummaryRef<'_>, LixError> {
    match node {
        StoredNode::Leaf { layout, entries } => {
            let entry_count = u32::try_from(entries.len())
                .map_err(|_| directory_error("entry count overflows"))?;
            let direct_row_count = entries.iter().try_fold(0u64, |sum, entry| {
                sum.checked_add(u64::from(stored_entry_direct_rows(entry)))
                    .ok_or_else(|| directory_error("row count overflows"))
            })?;
            Ok(NodeSummaryRef {
                first_key: stored_entry_first_key(&entries[0]),
                last_key: stored_entry_last_key(&entries[entries.len() - 1]),
                node_id,
                entry_count,
                direct_row_count,
                level: 0,
                layout: *layout,
            })
        }
        StoredNode::Internal {
            layout,
            level,
            children,
        } => {
            let (entry_count, direct_row_count) =
                children
                    .iter()
                    .try_fold((0u32, 0u64), |(entry_sum, row_sum), child| {
                        Ok::<_, LixError>((
                            entry_sum
                                .checked_add(child.entry_count)
                                .ok_or_else(|| directory_error("entry count overflows"))?,
                            row_sum
                                .checked_add(child.direct_row_count)
                                .ok_or_else(|| directory_error("row count overflows"))?,
                        ))
                    })?;
            Ok(NodeSummaryRef {
                first_key: &children[0].first_key,
                last_key: &children[children.len() - 1].last_key,
                node_id,
                entry_count,
                direct_row_count,
                level: *level,
                layout: *layout,
            })
        }
    }
}

fn summary_ref_matches_owned(actual: NodeSummaryRef<'_>, expected: &NodeSummary) -> bool {
    actual.first_key == expected.first_key
        && actual.last_key == expected.last_key
        && actual.node_id == expected.node_id
        && actual.entry_count == expected.entry_count
        && actual.direct_row_count == expected.direct_row_count
        && actual.level == expected.level
        && actual.layout == expected.layout
}

fn summary_ref_matches_root(actual: NodeSummaryRef<'_>, root: &MutationDirectoryRoot) -> bool {
    actual.node_id == root.root_id
        && actual.entry_count == root.entry_count
        && actual.direct_row_count == root.direct_row_count
        && actual.level.checked_add(1) == Some(root.tree_height)
        && actual.layout == root.layout
}

fn validate_loaded_node(
    node: &StoredNode,
    node_id: [u8; 32],
    expected: Option<&NodeSummary>,
    root: &MutationDirectoryRoot,
    operation: &str,
) -> Result<(), LixError> {
    let actual = node_summary_ref(node, node_id)?;
    match expected {
        Some(expected) if !summary_ref_matches_owned(actual, expected) => Err(directory_error(
            format!("{operation} child summary mismatch"),
        )),
        None if !summary_ref_matches_root(actual, root) => Err(directory_error(format!(
            "{operation} root summary mismatch"
        ))),
        _ => Ok(()),
    }
}

fn node_summary(node: &StoredNode, node_id: [u8; 32]) -> Result<NodeSummary, LixError> {
    match node {
        StoredNode::Leaf { layout, entries } => {
            let entry_count = u32::try_from(entries.len())
                .map_err(|_| directory_error("entry count overflows"))?;
            let direct_row_count = entries.iter().try_fold(0u64, |sum, entry| {
                sum.checked_add(u64::from(stored_entry_direct_rows(entry)))
                    .ok_or_else(|| directory_error("row count overflows"))
            })?;
            Ok(NodeSummary {
                first_key: stored_entry_first_key(&entries[0]).to_vec(),
                last_key: stored_entry_last_key(&entries[entries.len() - 1]).to_vec(),
                node_id,
                entry_count,
                direct_row_count,
                level: 0,
                layout: *layout,
            })
        }
        StoredNode::Internal {
            layout,
            level,
            children,
        } => {
            let (entry_count, direct_row_count) =
                children
                    .iter()
                    .try_fold((0u32, 0u64), |(entry_sum, row_sum), child| {
                        Ok::<_, LixError>((
                            entry_sum
                                .checked_add(child.entry_count)
                                .ok_or_else(|| directory_error("entry count overflows"))?,
                            row_sum
                                .checked_add(child.direct_row_count)
                                .ok_or_else(|| directory_error("row count overflows"))?,
                        ))
                    })?;
            Ok(NodeSummary {
                first_key: children[0].first_key.clone(),
                last_key: children[children.len() - 1].last_key.clone(),
                node_id,
                entry_count,
                direct_row_count,
                level: *level,
                layout: *layout,
            })
        }
    }
}

#[cfg(test)]
fn validate_entries(layout: u8, entries: &[MutationDirectoryEntry]) -> Result<(), LixError> {
    validate_layout(layout)?;
    if entries.is_empty() {
        return Ok(());
    }
    for entry in entries {
        match (layout, entry) {
            (
                LAYOUT_BOUNDED_INDIRECT,
                MutationDirectoryEntry::Bounded {
                    part,
                    direct_row_count: 0,
                },
            ) if valid_part(part) => {}
            (
                LAYOUT_BOUNDED_DIRECT,
                MutationDirectoryEntry::Bounded {
                    part,
                    direct_row_count,
                },
            ) if valid_part(part) && *direct_row_count > 0 => {}
            (
                LAYOUT_COMPACT_REPLACEMENT,
                MutationDirectoryEntry::CompactReplacement {
                    content_digest,
                    direct_row_count,
                },
            ) if *content_digest != [0; 32] && *direct_row_count > 0 => {}
            (
                LAYOUT_DIRECT_ROWS_ONLY,
                MutationDirectoryEntry::DirectAddress { direct_row_count },
            ) if *direct_row_count > 0 => {}
            _ => return Err(directory_error("entry disagrees with directory layout")),
        }
    }
    if is_bounded(layout)
        && entries
            .windows(2)
            .any(|pair| pair[0].last_key() >= pair[1].first_key())
    {
        return Err(directory_error("bounded entries overlap or are unordered"));
    }
    Ok(())
}

fn validate_stored_entries(layout: u8, entries: &[StoredEntry]) -> Result<(), LixError> {
    validate_layout(layout)?;
    for entry in entries {
        match (layout, entry) {
            (
                LAYOUT_BOUNDED_INDIRECT,
                StoredEntry::Bounded {
                    first_key,
                    last_key,
                    direct_row_count: 0,
                    ..
                },
            ) if !first_key.is_empty() && first_key <= last_key => {}
            (
                LAYOUT_BOUNDED_DIRECT,
                StoredEntry::Bounded {
                    first_key,
                    last_key,
                    direct_row_count,
                    ..
                },
            ) if !first_key.is_empty() && first_key <= last_key && *direct_row_count > 0 => {}
            (
                LAYOUT_COMPACT_REPLACEMENT,
                StoredEntry::CompactReplacement {
                    content_digest,
                    direct_row_count,
                },
            ) if *content_digest != [0; 32] && *direct_row_count > 0 => {}
            (LAYOUT_DIRECT_ROWS_ONLY, StoredEntry::DirectAddress { direct_row_count })
                if *direct_row_count > 0 => {}
            _ => return Err(directory_error("entry disagrees with directory layout")),
        }
    }
    if is_bounded(layout)
        && entries
            .windows(2)
            .any(|pair| stored_entry_last_key(&pair[0]) >= stored_entry_first_key(&pair[1]))
    {
        return Err(directory_error("bounded entries overlap or are unordered"));
    }
    Ok(())
}

fn validate_root(root: &MutationDirectoryRoot) -> Result<(), LixError> {
    validate_layout(root.layout)?;
    if root.root_id == [0; 32]
        || root.root_digest == [0; 32]
        || root.entry_count == 0
        || root.tree_height == 0
        || (root.layout == LAYOUT_BOUNDED_INDIRECT && root.direct_row_count != 0)
        || ((root.layout == LAYOUT_BOUNDED_DIRECT
            || root.layout == LAYOUT_COMPACT_REPLACEMENT
            || root.layout == LAYOUT_DIRECT_ROWS_ONLY)
            && root.direct_row_count == 0)
        || root.root_digest
            != root_digest(
                root.root_id,
                root.entry_count,
                root.direct_row_count,
                root.tree_height,
                root.layout,
            )
    {
        return Err(directory_error("root is invalid"));
    }
    Ok(())
}

pub(crate) fn validate_mutation_directory_root(
    root: &MutationDirectoryRoot,
) -> Result<(), LixError> {
    validate_root(root)
}

#[cfg(test)]
fn summary_matches_root(summary: &NodeSummary, root: &MutationDirectoryRoot) -> bool {
    summary.node_id == root.root_id
        && summary.entry_count == root.entry_count
        && summary.direct_row_count == root.direct_row_count
        && summary.level.checked_add(1) == Some(root.tree_height)
        && summary.layout == root.layout
}

fn validate_layout(layout: u8) -> Result<(), LixError> {
    if matches!(
        layout,
        LAYOUT_BOUNDED_INDIRECT
            | LAYOUT_BOUNDED_DIRECT
            | LAYOUT_COMPACT_REPLACEMENT
            | LAYOUT_DIRECT_ROWS_ONLY
    ) {
        Ok(())
    } else {
        Err(directory_error("layout is unsupported"))
    }
}

fn is_bounded(layout: u8) -> bool {
    layout == LAYOUT_BOUNDED_INDIRECT || layout == LAYOUT_BOUNDED_DIRECT
}

fn valid_part(part: &CommitStateMutationPart) -> bool {
    !part.first_key.is_empty()
        && part.first_key <= part.last_key
        && part.content_digest != [0; 32]
}

#[cfg(test)]
fn stored_entry(entry: &MutationDirectoryEntry) -> StoredEntry {
    match entry {
        MutationDirectoryEntry::Bounded {
            part,
            direct_row_count,
        } => StoredEntry::Bounded {
            first_key: part.first_key.clone(),
            last_key: part.last_key.clone(),
            content_digest: part.content_digest,
            replacement_part: part.replacement_part.clone(),
            direct_row_count: *direct_row_count,
        },
        MutationDirectoryEntry::CompactReplacement {
            content_digest,
            direct_row_count,
        } => StoredEntry::CompactReplacement {
            content_digest: *content_digest,
            direct_row_count: *direct_row_count,
        },
        MutationDirectoryEntry::DirectAddress { direct_row_count } => StoredEntry::DirectAddress {
            direct_row_count: *direct_row_count,
        },
    }
}

fn runtime_entry(entry: StoredEntry) -> Result<MutationDirectoryEntry, LixError> {
    let entry = match entry {
        StoredEntry::Bounded {
            first_key,
            last_key,
            content_digest,
            replacement_part,
            direct_row_count,
        } => MutationDirectoryEntry::Bounded {
            part: CommitStateMutationPart {
                first_key,
                last_key,
                content_digest,
                replacement_part,
            },
            direct_row_count,
        },
        StoredEntry::CompactReplacement {
            content_digest,
            direct_row_count,
        } => MutationDirectoryEntry::CompactReplacement {
            content_digest,
            direct_row_count,
        },
        StoredEntry::DirectAddress { direct_row_count } => {
            MutationDirectoryEntry::DirectAddress { direct_row_count }
        }
    };
    Ok(entry)
}

fn stored_entry_first_key(entry: &StoredEntry) -> &[u8] {
    match entry {
        StoredEntry::Bounded { first_key, .. } => first_key,
        StoredEntry::CompactReplacement { .. } => &[],
        StoredEntry::DirectAddress { .. } => &[],
    }
}

fn stored_entry_last_key(entry: &StoredEntry) -> &[u8] {
    match entry {
        StoredEntry::Bounded { last_key, .. } => last_key,
        StoredEntry::CompactReplacement { .. } => &[],
        StoredEntry::DirectAddress { .. } => &[],
    }
}

fn stored_entry_direct_rows(entry: &StoredEntry) -> u16 {
    match entry {
        StoredEntry::Bounded {
            direct_row_count, ..
        }
        | StoredEntry::CompactReplacement {
            direct_row_count, ..
        }
        | StoredEntry::DirectAddress { direct_row_count } => *direct_row_count,
    }
}

fn balanced_chunks<T>(values: &[T]) -> Vec<&[T]> {
    debug_assert!(!values.is_empty());
    let chunk_count = values.len().div_ceil(FANOUT);
    let base = values.len() / chunk_count;
    let larger = values.len() % chunk_count;
    let mut chunks = Vec::with_capacity(chunk_count);
    let mut start = 0usize;
    for index in 0..chunk_count {
        let length = base + usize::from(index < larger);
        chunks.push(&values[start..start + length]);
        start += length;
    }
    chunks
}

fn balanced_chunk_lengths(value_count: usize) -> impl Iterator<Item = usize> {
    let chunk_count = value_count.div_ceil(FANOUT);
    let base = value_count / chunk_count;
    let larger = value_count % chunk_count;
    (0..chunk_count).map(move |index| base + usize::from(index < larger))
}

fn node_digest(bytes: &[u8]) -> [u8; 32] {
    let mut digest = blake3::Hasher::new_derive_key(NODE_HASH_CONTEXT);
    digest.update(bytes);
    *digest.finalize().as_bytes()
}

fn root_digest(
    root_id: [u8; 32],
    entry_count: u32,
    direct_row_count: u64,
    tree_height: u16,
    layout: u8,
) -> [u8; 32] {
    let mut digest = blake3::Hasher::new_derive_key(ROOT_HASH_CONTEXT);
    digest.update(&root_id);
    digest.update(&entry_count.to_be_bytes());
    digest.update(&direct_row_count.to_be_bytes());
    digest.update(&tree_height.to_be_bytes());
    digest.update(&[layout]);
    *digest.finalize().as_bytes()
}

fn directory_error(message: impl Into<String>) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state mutation directory: {}", message.into()),
    )
}

#[cfg(test)]
mod tests {
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    use super::*;

    /// Measurement probe: what one single-key point read costs, as a function
    /// of directory entry count.  Deterministic counts only - no timing.
    ///
    /// Both arms of every size run inside one `#[test]` so the task-local
    /// census cannot be contaminated by a sibling test.
    #[tokio::test]
    #[ignore = "measurement probe, not a gate"]
    async fn dirnode_point_read_cost_curve() {
        const KEY_LEN: usize = 27;

        fn wide_entry(index: u32) -> MutationDirectoryEntry {
            let mut first_key = vec![0u8; KEY_LEN];
            let mut last_key = vec![0u8; KEY_LEN];
            first_key[..8].copy_from_slice(&(u64::from(index) * 2).to_be_bytes());
            last_key[..8].copy_from_slice(&(u64::from(index) * 2 + 1).to_be_bytes());
            MutationDirectoryEntry::Bounded {
                part: CommitStateMutationPart {
                    first_key,
                    last_key,
                    content_digest: [1; 32],
                    replacement_part: None,
                },
                direct_row_count: 7,
            }
        }

        println!(
            "DIRNODE_CURVE entries,height,total_node_bytes,decodes,bytes,entries_decoded,key_bytes,allocs,bytes_per_decode,allocs_per_decode"
        );
        for entry_count in [2u32, 8, 32, 64, 128, 256, 512, 2048, 8192, 16384] {
            let entries = (0..entry_count).map(wide_entry).collect::<Vec<_>>();
            let built = build_mutation_directory(LAYOUT_BOUNDED_DIRECT, &entries).unwrap();
            let total_node_bytes: usize =
                built.node_bytes().values().map(|bytes| bytes.len()).sum();
            let (_storage, read) = stored_directory(&built).await;

            // Probe the middle entry so the answer is one run, every level.
            let mut probe = vec![0u8; KEY_LEN];
            probe[..8].copy_from_slice(&(u64::from(entry_count / 2) * 2).to_be_bytes());
            let points = vec![Bytes::from(probe)];

            // Two reps per size: a count lane must be shown to be stable
            // before one rep is treated as enough.
            let mut seen = Vec::new();
            for _ in 0..2 {
                let (plan, accounting) = test_read_accounting::scope(async {
                    load_mutation_part_read_plan(
                        &read,
                        &built.root,
                        MutationDirectoryReadSelection::SortedUniquePoints(&points),
                    )
                    .await
                })
                .await;
                let plan = plan.unwrap();
                assert_eq!(plan.len(), 1, "probe must select exactly one entry");
                seen.push(accounting);
            }
            assert_eq!(
                seen[0], seen[1],
                "point-read census is not deterministic at {entry_count} entries"
            );
            let a = seen[0];
            println!(
                "DIRNODE_CURVE {},{},{},{},{},{},{},{},{:.2},{:.2}",
                entry_count,
                built.root.tree_height,
                total_node_bytes,
                a.node_decodes,
                a.node_decode_bytes,
                a.node_decode_entries,
                a.node_decode_key_bytes,
                a.node_decode_allocs,
                a.node_decode_bytes as f64 / a.node_decodes as f64,
                a.node_decode_allocs as f64 / a.node_decodes as f64,
            );
        }
    }

    /// Measurement probe: one point lookup inside one node, v1 record versus
    /// the v2 seekable layout.  Same entry data, same answer, both arms
    /// measured by the same thread-local allocation counter.
    #[test]
    #[ignore = "measurement probe, not a gate"]
    fn dirnode_point_lookup_v1_vs_v2() {
        use super::seekable_prototype::{NodeView, Touched, encode_leaf};

        const KEY_LEN: usize = 27;

        fn keys(index: u32) -> (Vec<u8>, Vec<u8>) {
            let mut first_key = vec![0u8; KEY_LEN];
            let mut last_key = vec![0u8; KEY_LEN];
            first_key[..8].copy_from_slice(&(u64::from(index) * 2).to_be_bytes());
            last_key[..8].copy_from_slice(&(u64::from(index) * 2 + 1).to_be_bytes());
            (first_key, last_key)
        }

        println!(
            "DIRNODE_AB entries,v1_node_bytes,v2_node_bytes,v1_read_bytes,v1_allocs,v1_alloc_bytes,v2_read_bytes,v2_key_compares,v2_allocs,v2_alloc_bytes"
        );
        println!(
            "DIRNODE_INTERNAL entries,v1_node_bytes,v2_node_bytes,v1_read_bytes,v1_allocs,v1_alloc_bytes,v2_read_bytes,v2_allocs,v2_alloc_bytes"
        );        println!(
            "DIRNODE_WRITE entries,v1_node_bytes,v2_node_bytes,v1_encode_allocs,v1_encode_alloc_bytes,v2_encode_allocs,v2_encode_alloc_bytes"
        );
        for entry_count in [2u32, 8, 32, 64, 128] {
            let raw = (0..entry_count)
                .map(|index| {
                    let (first_key, last_key) = keys(index);
                    (first_key, last_key, 7u16)
                })
                .collect::<Vec<_>>();

            let stored_entries = raw
                .iter()
                .map(|(first_key, last_key, direct_row_count)| StoredEntry::Bounded {
                    first_key: first_key.clone(),
                    last_key: last_key.clone(),
                    content_digest: [1; 32],
                    replacement_part: None,
                    direct_row_count: *direct_row_count,
                })
                .collect::<Vec<_>>();
            let v1_node = StoredNode::Leaf {
                layout: LAYOUT_BOUNDED_DIRECT,
                entries: stored_entries,
            };
            let v1_bytes = encode_node(&v1_node).unwrap();
            let v1_payload = v1_bytes.len() - NODE_MAGIC.len();
            let v2_bytes = encode_leaf(LAYOUT_BOUNDED_DIRECT, &raw);

            let target = entry_count / 2;
            let probe = keys(target).0;

            // Arm v1: decode the whole node, then walk it for the one entry -
            // exactly what `load_mutation_part_read_plan` does at a leaf.
            let ((v1_answer, v1_index), v1_allocs, v1_alloc_bytes) =
                alloc_census::measure(|| {
                    let node = decode_node(&v1_bytes).unwrap();
                    let StoredNode::Leaf { entries, .. } = node else {
                        unreachable!("leaf");
                    };
                    let mut found = None;
                    for (index, entry) in entries.into_iter().enumerate() {
                        if stored_entry_first_key(&entry) <= probe.as_slice()
                            && probe.as_slice() <= stored_entry_last_key(&entry)
                        {
                            found = Some((runtime_entry(entry).unwrap(), index));
                            break;
                        }
                    }
                    found.unwrap()
                });

            // Arm v2: binary-search the key table in place, decode one entry.
            let mut touched = Touched::default();
            let ((v2_answer, v2_index), v2_allocs, v2_alloc_bytes) =
                alloc_census::measure(|| {
                    let view = NodeView::new(&v2_bytes, &mut touched).unwrap();
                    let index = view.seek(&probe, &mut touched).unwrap();
                    (view.leaf_entry(index, &mut touched), index)
                });

            assert_eq!(v1_index, target as usize, "v1 found the wrong entry");
            assert_eq!(v2_index, target as usize, "v2 found the wrong entry");
            assert_eq!(v1_answer, v2_answer, "arms disagree at {entry_count} entries");

            println!(
                "DIRNODE_AB {},{},{},{},{},{},{},{},{},{}",
                entry_count,
                v1_bytes.len(),
                v2_bytes.len(),
                v1_payload,
                v1_allocs,
                v1_alloc_bytes,
                touched.bytes,
                touched.key_compares,
                v2_allocs,
                v2_alloc_bytes,
            );

            // Write side.  Warm reads decode nothing, so the only way this
            // layout can cost steady-state throughput is at commit, where
            // every node is encoded once.  Inputs for both arms are built
            // before the measured region so only the encode is counted.
            let write_node = StoredNode::Leaf {
                layout: LAYOUT_BOUNDED_DIRECT,
                entries: raw
                    .iter()
                    .map(|(first_key, last_key, direct_row_count)| StoredEntry::Bounded {
                        first_key: first_key.clone(),
                        last_key: last_key.clone(),
                        content_digest: [1; 32],
                        replacement_part: None,
                        direct_row_count: *direct_row_count,
                    })
                    .collect(),
            };
            let (v1_out, v1_enc_allocs, v1_enc_alloc_bytes) =
                alloc_census::measure(|| encode_node(&write_node).unwrap());
            let (v2_out, v2_enc_allocs, v2_enc_alloc_bytes) =
                alloc_census::measure(|| encode_leaf(LAYOUT_BOUNDED_DIRECT, &raw));
            assert_eq!(v1_out.len(), v1_bytes.len());
            assert_eq!(v2_out.len(), v2_bytes.len());
            // Internal-node arm: the other half of a height-2 point read.
            let child_ids = (0..entry_count)
                .map(|index| {
                    // Non-zero fill: `validate_node` rejects an all-zero
                    // child node id, which index 0 would otherwise produce.
                    let mut node_id = [0xABu8; 32];
                    node_id[..4].copy_from_slice(&index.to_be_bytes());
                    node_id
                })
                .collect::<Vec<_>>();
            let internal_children = raw
                .iter()
                .zip(&child_ids)
                .map(|((first_key, last_key, _), node_id)| {
                    (first_key.clone(), last_key.clone(), *node_id, 128u32, 128u64)
                })
                .collect::<Vec<_>>();
            let v1_internal = StoredNode::Internal {
                layout: LAYOUT_BOUNDED_DIRECT,
                level: 1,
                children: internal_children
                    .iter()
                    .map(|(first_key, last_key, node_id, entries, rows)| StoredChild {
                        first_key: first_key.clone(),
                        last_key: last_key.clone(),
                        node_id: *node_id,
                        entry_count: *entries,
                        direct_row_count: *rows,
                        level: 0,
                        layout: LAYOUT_BOUNDED_DIRECT,
                    })
                    .collect(),
            };
            let v1_internal_bytes = encode_node(&v1_internal).unwrap();
            let v2_internal_bytes =
                seekable_prototype::encode_internal(LAYOUT_BOUNDED_DIRECT, 1, &internal_children);

            let ((v1_child, v1_child_index), v1_int_allocs, v1_int_alloc_bytes) =
                alloc_census::measure(|| {
                    let node = decode_node(&v1_internal_bytes).unwrap();
                    let StoredNode::Internal { children, .. } = node else {
                        unreachable!("internal");
                    };
                    let mut preceding = 0u32;
                    let mut found = None;
                    for (index, child) in children.into_iter().enumerate() {
                        if child.first_key.as_slice() <= probe.as_slice()
                            && probe.as_slice() <= child.last_key.as_slice()
                        {
                            found = Some(((child.node_id, child.entry_count, preceding), index));
                            break;
                        }
                        preceding += child.entry_count;
                    }
                    found.unwrap()
                });

            let mut int_touched = Touched::default();
            let ((v2_child, v2_child_index), v2_int_allocs, v2_int_alloc_bytes) =
                alloc_census::measure(|| {
                    let view = NodeView::new(&v2_internal_bytes, &mut int_touched).unwrap();
                    let index = view.seek(&probe, &mut int_touched).unwrap();
                    let (node_id, entries, _) = view.child(index, &mut int_touched);
                    ((node_id, entries, 128 * index as u32), index)
                });

            assert_eq!(v1_child_index, v2_child_index, "internal arms disagree on index");
            assert_eq!(v1_child, v2_child, "internal arms disagree on child");

            // The keyless layouts route by entry index instead of by key.
            // Both routings must select the same child.
            let index_view = NodeView::new(&v2_internal_bytes, &mut Touched::default()).unwrap();
            assert_eq!(
                index_view.seek_entry_index(
                    128 * v2_child_index as u32 + 5,
                    &mut Touched::default()
                ),
                Some(v2_child_index),
                "index routing disagrees with key routing"
            );

            println!(
                "DIRNODE_INTERNAL {},{},{},{},{},{},{},{},{}",
                entry_count,
                v1_internal_bytes.len(),
                v2_internal_bytes.len(),
                v1_internal_bytes.len() - NODE_MAGIC.len(),
                v1_int_allocs,
                v1_int_alloc_bytes,
                int_touched.bytes,
                v2_int_allocs,
                v2_int_alloc_bytes,
            );

            println!(
                "DIRNODE_WRITE {},{},{},{},{},{},{}",
                entry_count,
                v1_out.len(),
                v2_out.len(),
                v1_enc_allocs,
                v1_enc_alloc_bytes,
                v2_enc_allocs,
                v2_enc_alloc_bytes,
            );
        }
    }

    /// The v2 reader must vouch for exactly what the v1 reader vouches for.
    ///
    /// This is a real gate, not a probe: it fails if the O(1) header-read
    /// summary ever diverges from the walked summary `node_summary_ref`
    /// computes, and it fails if a malformed key range inside the region the
    /// reader touches is accepted.
    #[test]
    fn dirnode_v2_summary_matches_walked_summary_and_rejects_touched_disorder() {
        use super::seekable_prototype::{NodeView, Touched, encode_leaf};

        const KEY_LEN: usize = 27;

        fn keys(index: u32) -> (Vec<u8>, Vec<u8>) {
            let mut first_key = vec![0u8; KEY_LEN];
            let mut last_key = vec![0u8; KEY_LEN];
            first_key[..8].copy_from_slice(&(u64::from(index) * 2).to_be_bytes());
            last_key[..8].copy_from_slice(&(u64::from(index) * 2 + 1).to_be_bytes());
            (first_key, last_key)
        }

        for entry_count in [1u32, 2, 8, 128] {
            let raw = (0..entry_count)
                .map(|index| {
                    let (first_key, last_key) = keys(index);
                    (first_key, last_key, 7u16)
                })
                .collect::<Vec<_>>();
            let v1_node = StoredNode::Leaf {
                layout: LAYOUT_BOUNDED_DIRECT,
                entries: raw
                    .iter()
                    .map(|(first_key, last_key, direct_row_count)| StoredEntry::Bounded {
                        first_key: first_key.clone(),
                        last_key: last_key.clone(),
                        content_digest: [1; 32],
                        replacement_part: None,
                        direct_row_count: *direct_row_count,
                    })
                    .collect(),
            };
            let node_id = [7u8; 32];
            let walked = node_summary_ref(&v1_node, node_id).unwrap();

            let v2_bytes = encode_leaf(LAYOUT_BOUNDED_DIRECT, &raw);
            let mut touched = Touched::default();
            let view = NodeView::new(&v2_bytes, &mut touched).unwrap();
            let (first_key, last_key, count, direct_rows) = view.summary(&mut touched);

            assert_eq!(first_key, walked.first_key, "first_key at {entry_count}");
            assert_eq!(last_key, walked.last_key, "last_key at {entry_count}");
            assert_eq!(count, walked.entry_count, "entry_count at {entry_count}");
            assert_eq!(
                direct_rows, walked.direct_row_count,
                "direct_row_count at {entry_count}"
            );

            // Reading the summary must stay O(1): it may not grow with the
            // number of entries in the node.
            assert!(
                touched.bytes < 200,
                "summary read touched {} bytes at {entry_count} entries",
                touched.bytes
            );

            // Every entry the reader touches is order-checked.
            for index in 0..entry_count as usize {
                view.check_local_order(index, &mut Touched::default())
                    .unwrap();
            }
        }

        // A node whose touched region is out of order must be refused.
        let (first_key, last_key) = keys(5);
        let (later_first, later_last) = keys(9);
        let disordered = vec![
            (later_first, later_last, 7u16),
            (first_key, last_key, 7u16),
        ];
        let bad = encode_leaf(LAYOUT_BOUNDED_DIRECT, &disordered);
        let mut touched = Touched::default();
        let view = NodeView::new(&bad, &mut touched).unwrap();
        assert!(
            view.check_local_order(0, &mut touched).is_err(),
            "out-of-order touched entries must be rejected"
        );
    }

    fn bounded_entry(index: u32) -> MutationDirectoryEntry {
        let first_key = index.saturating_mul(10).to_be_bytes().to_vec();
        let last_key = index
            .saturating_mul(10)
            .saturating_add(9)
            .to_be_bytes()
            .to_vec();
        MutationDirectoryEntry::Bounded {
            part: CommitStateMutationPart {
                first_key,
                last_key,
                content_digest: [1; 32],
                replacement_part: None,
            },
            direct_row_count: 7,
        }
    }

    async fn stored_directory(
        built: &BuiltMutationDirectory,
    ) -> (StorageAdapter<Memory>, impl StorageAdapterRead) {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        built.stage(&mut writes).unwrap();
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        (storage, read)
    }

    #[tokio::test]
    async fn multi_level_directory_emits_distinct_runs_without_per_key_owners() {
        let entries = (0..(FANOUT as u32 * 2 + 17))
            .map(bounded_entry)
            .collect::<Vec<_>>();
        let built = build_mutation_directory(LAYOUT_BOUNDED_DIRECT, &entries).unwrap();
        assert!(built.root.tree_height >= 2);
        assert!(built.node_bytes().len() > 1);
        let (_storage, read) = stored_directory(&built).await;

        let all = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::All(MutationDirectoryFullTraversalContext::Test),
        )
        .await
        .unwrap();
        assert_eq!(all.len(), entries.len());
        assert_eq!(all.visited_node_count(), built.node_bytes().len());
        assert_eq!(all.node_summary_owner_count() + 1, all.visited_node_count());
        assert_eq!(all.node_summary_clone_count(), 0);
        assert_eq!(all.part_clone_count(), 0);
        assert_eq!(
            all.into_runs()
                .into_iter()
                .map(|run| run.entry)
                .collect::<Vec<_>>(),
            entries
        );

        let requested = [0u32, FANOUT as u32, FANOUT as u32 * 2 + 16];
        let points = requested
            .iter()
            .map(|index| {
                Bytes::copy_from_slice(&index.saturating_mul(10).saturating_add(4).to_be_bytes())
            })
            .collect::<Vec<_>>();
        let point_plan = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniquePoints(&points),
        )
        .await
        .unwrap();
        assert_eq!(point_plan.len(), requested.len());
        assert_eq!(
            point_plan.node_summary_owner_count() + 1,
            point_plan.visited_node_count()
        );
        assert!(
            point_plan.visited_node_count()
                <= 1 + requested.len() * built.root.tree_height as usize
        );
        assert_eq!(point_plan.node_summary_clone_count(), 0);
        assert_eq!(point_plan.part_clone_count(), 0);
        for ((&expected_index, expected_selector), run) in
            requested.iter().zip(0usize..).zip(point_plan.into_runs())
        {
            assert_eq!(run.entry_index, expected_index);
            assert_eq!(run.selector_span, expected_selector..expected_selector + 1);
            assert_eq!(run.entry.direct_row_count(), 7);
            assert_eq!(
                run.entry,
                match &entries[expected_index as usize] {
                    entry @ MutationDirectoryEntry::Bounded { .. } => entry.clone(),
                    _ => unreachable!(),
                }
            );
        }

        let clustered_points = [1u32, 4, 8]
            .into_iter()
            .map(|key| Bytes::copy_from_slice(&key.to_be_bytes()))
            .collect::<Vec<_>>();
        let clustered = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniquePoints(&clustered_points),
        )
        .await
        .unwrap()
        .into_runs();
        assert_eq!(clustered.len(), 1, "one touched part must produce one run");
        assert_eq!(clustered[0].entry_index, 0);
        assert_eq!(clustered[0].selector_span, 0..clustered_points.len());

        let coordinates = [
            MutationDirectoryDirectCoordinate {
                part_index: 0,
                local_row: 0,
            },
            MutationDirectoryDirectCoordinate {
                part_index: FANOUT as u32,
                local_row: 3,
            },
            MutationDirectoryDirectCoordinate {
                part_index: u32::try_from(entries.len() - 1).unwrap(),
                local_row: 6,
            },
        ];
        let coordinate_plan = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&coordinates),
        )
        .await
        .unwrap();
        assert_eq!(coordinate_plan.node_summary_clone_count(), 0);
        assert_eq!(coordinate_plan.part_clone_count(), 0);
        assert!(
            coordinate_plan.visited_node_count()
                <= 1 + coordinates.len() * built.root.tree_height as usize
        );
        let coordinate_runs = coordinate_plan.into_runs();
        assert_eq!(coordinate_runs.len(), coordinates.len());
        assert_eq!(coordinate_runs[0].entry_index, 0);
        assert_eq!(coordinate_runs[0].selector_span, 0..1);
        assert_eq!(coordinate_runs[1].entry_index, FANOUT as u32);
        assert_eq!(coordinate_runs[1].selector_span, 1..2);
        assert_eq!(
            coordinate_runs[2].entry_index,
            u32::try_from(entries.len() - 1).unwrap()
        );
        assert_eq!(coordinate_runs[2].selector_span, 2..3);

        let clustered_coordinates = [
            MutationDirectoryDirectCoordinate {
                part_index: 0,
                local_row: 0,
            },
            MutationDirectoryDirectCoordinate {
                part_index: 0,
                local_row: 1,
            },
            MutationDirectoryDirectCoordinate {
                part_index: 0,
                local_row: 6,
            },
        ];
        let clustered_coordinate_runs = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&clustered_coordinates),
        )
        .await
        .unwrap()
        .into_runs();
        assert_eq!(clustered_coordinate_runs.len(), 1);
        assert_eq!(clustered_coordinate_runs[0].entry_index, 0);
        assert_eq!(clustered_coordinate_runs[0].selector_span, 0..3);

        let ranges = points
            .iter()
            .map(|point| {
                let mut end = point.to_vec();
                *end.last_mut().unwrap() += 1;
                MutationDirectoryKeyRange {
                    start: point.clone(),
                    end: Some(Bytes::from(end)),
                }
            })
            .collect::<Vec<_>>();
        let range_runs = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedRanges(&ranges),
        )
        .await
        .unwrap()
        .into_runs();
        assert_eq!(
            range_runs
                .iter()
                .map(|run| (run.entry_index, run.selector_span.clone()))
                .collect::<Vec<_>>(),
            requested
                .iter()
                .copied()
                .zip((0usize..requested.len()).map(|index| index..index + 1))
                .collect::<Vec<_>>()
        );

        let wide_range = [MutationDirectoryKeyRange {
            start: Bytes::copy_from_slice(&4u32.to_be_bytes()),
            end: Some(Bytes::copy_from_slice(&25u32.to_be_bytes())),
        }];
        let wide_runs = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedRanges(&wide_range),
        )
        .await
        .unwrap()
        .into_runs();
        assert_eq!(
            wide_runs
                .iter()
                .map(|run| (run.entry_index, run.selector_span.clone()))
                .collect::<Vec<_>>(),
            vec![(0, 0..1), (1, 0..1), (2, 0..1)]
        );

        let batched = load_all_mutation_part_read_plans(
            &read,
            &[built.root.clone(), built.root.clone()],
            MutationDirectoryFullTraversalContext::Test,
        )
        .await
        .unwrap();
        assert_eq!(batched.len(), 2);
        assert!(batched.iter().all(|plan| plan.len() == entries.len()));
        assert!(batched.iter().all(|plan| {
            plan.node_summary_clone_count() <= plan.node_summary_owner_count()
                && plan.part_clone_count() <= plan.len()
        }));
        assert_eq!(
            collect_mutation_directory_node_ids(&read, &built.root)
                .await
                .unwrap()
                .len(),
            built.node_bytes().len()
        );
    }

    #[tokio::test]
    async fn direct_coordinates_preserve_physical_holes_across_three_tree_levels() {
        let irregular = [2u16, 7, 1]
            .into_iter()
            .map(|direct_row_count| MutationDirectoryEntry::DirectAddress { direct_row_count })
            .collect::<Vec<_>>();
        let built = build_mutation_directory(LAYOUT_DIRECT_ROWS_ONLY, &irregular).unwrap();
        let (_storage, read) = stored_directory(&built).await;
        let coordinates = [
            MutationDirectoryDirectCoordinate {
                part_index: 0,
                local_row: 1,
            },
            MutationDirectoryDirectCoordinate {
                part_index: 1,
                local_row: 6,
            },
            MutationDirectoryDirectCoordinate {
                part_index: 2,
                local_row: 0,
            },
        ];
        let runs = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&coordinates),
        )
        .await
        .unwrap()
        .into_runs();
        assert_eq!(
            runs.iter().map(|run| run.entry_index).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let (_runs, not_owned) = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&[
                MutationDirectoryDirectCoordinate {
                    part_index: 0,
                    local_row: 2,
                },
            ]),
        )
        .await
        .expect("a physical hole is an authenticated unowned slot")
        .into_direct_routes();
        assert_eq!(
            not_owned,
            vec![MutationDirectoryNotOwnedSpan {
                selector_span: 0..1,
                reason: MutationDirectoryNotOwnedReason::LocalRowOutOfRange,
            }]
        );

        let entries = (0..(FANOUT * FANOUT + 1))
            .map(|_| MutationDirectoryEntry::DirectAddress {
                direct_row_count: 1,
            })
            .collect::<Vec<_>>();
        let built = build_mutation_directory(LAYOUT_DIRECT_ROWS_ONLY, &entries).unwrap();
        assert!(built.root.tree_height >= 3);
        let (_storage, read) = stored_directory(&built).await;
        let coordinates = [
            0u32,
            FANOUT as u32 - 1,
            FANOUT as u32,
            u32::try_from(FANOUT * FANOUT - 1).unwrap(),
            u32::try_from(FANOUT * FANOUT).unwrap(),
        ]
        .map(|part_index| MutationDirectoryDirectCoordinate {
            part_index,
            local_row: 0,
        });
        let plan = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&coordinates),
        )
        .await
        .unwrap();
        assert_eq!(plan.len(), coordinates.len());
        assert_eq!(plan.node_summary_clone_count(), 0);
        assert_eq!(plan.part_clone_count(), 0);
        assert!(
            plan.visited_node_count() <= 1 + coordinates.len() * built.root.tree_height as usize
        );
        assert_eq!(
            plan.into_runs()
                .into_iter()
                .map(|run| run.entry_index)
                .collect::<Vec<_>>(),
            coordinates
                .iter()
                .map(|coordinate| coordinate.part_index)
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn read_plan_rejects_noncanonical_selectors_and_handles_empty_inputs() {
        let entries = (0..4).map(bounded_entry).collect::<Vec<_>>();
        let built = build_mutation_directory(LAYOUT_BOUNDED_DIRECT, &entries).unwrap();
        let (_storage, read) = stored_directory(&built).await;
        let point = Bytes::copy_from_slice(&4u32.to_be_bytes());
        let next = Bytes::copy_from_slice(&14u32.to_be_bytes());

        for points in [
            vec![point.clone(), point.clone()],
            vec![next.clone(), point.clone()],
        ] {
            let error = load_mutation_part_read_plan(
                &read,
                &built.root,
                MutationDirectoryReadSelection::SortedUniquePoints(&points),
            )
            .await
            .expect_err("noncanonical points must fail");
            assert!(error.to_string().contains("strictly sorted and unique"));
        }

        let touching = vec![
            MutationDirectoryKeyRange {
                start: point.clone(),
                end: Some(next.clone()),
            },
            MutationDirectoryKeyRange {
                start: next.clone(),
                end: None,
            },
        ];
        let error = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedRanges(&touching),
        )
        .await
        .expect_err("touching ranges must be canonicalized upstream");
        assert!(
            error
                .to_string()
                .contains("sorted, disjoint, and canonical")
        );

        let empty_points = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniquePoints(&[]),
        )
        .await
        .unwrap();
        assert!(empty_points.is_empty());
        assert_eq!(empty_points.visited_node_count(), 0);
        let empty_ranges = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedRanges(&[]),
        )
        .await
        .unwrap();
        assert!(empty_ranges.is_empty());
        assert_eq!(empty_ranges.visited_node_count(), 0);

        for coordinates in [
            vec![
                MutationDirectoryDirectCoordinate {
                    part_index: 0,
                    local_row: 0,
                },
                MutationDirectoryDirectCoordinate {
                    part_index: 0,
                    local_row: 0,
                },
            ],
            vec![
                MutationDirectoryDirectCoordinate {
                    part_index: 1,
                    local_row: 0,
                },
                MutationDirectoryDirectCoordinate {
                    part_index: 0,
                    local_row: 1,
                },
            ],
        ] {
            let error = load_mutation_part_read_plan(
                &read,
                &built.root,
                MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&coordinates),
            )
            .await
            .expect_err("noncanonical direct coordinates must fail");
            assert!(error.to_string().contains("strictly sorted and unique"));
        }
        let (_runs, not_owned) = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&[
                MutationDirectoryDirectCoordinate {
                    part_index: built.root.entry_count,
                    local_row: 0,
                },
            ]),
        )
        .await
        .expect("out-of-range part coordinates are authenticated unowned slots")
        .into_direct_routes();
        assert_eq!(
            not_owned,
            vec![MutationDirectoryNotOwnedSpan {
                selector_span: 0..1,
                reason: MutationDirectoryNotOwnedReason::PartIndexOutOfRange,
            }]
        );
        let (_runs, not_owned) = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&[
                MutationDirectoryDirectCoordinate {
                    part_index: 0,
                    local_row: 7,
                },
            ]),
        )
        .await
        .expect("out-of-range local rows are authenticated unowned slots")
        .into_direct_routes();
        assert_eq!(
            not_owned,
            vec![MutationDirectoryNotOwnedSpan {
                selector_span: 0..1,
                reason: MutationDirectoryNotOwnedReason::LocalRowOutOfRange,
            }]
        );
        let empty_coordinates = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&[]),
        )
        .await
        .unwrap();
        assert!(empty_coordinates.is_empty());
        assert_eq!(empty_coordinates.visited_node_count(), 0);
    }

    #[tokio::test]
    async fn every_read_plan_mode_fails_closed_on_authenticated_bound_mismatch() {
        let entries = (0..(FANOUT as u32 + 1))
            .map(bounded_entry)
            .collect::<Vec<_>>();
        let built = build_mutation_directory(LAYOUT_BOUNDED_DIRECT, &entries).unwrap();
        let root_bytes = built.node_bytes()[&built.root.root_id].clone();
        let mut root_node = decode_node(&root_bytes).unwrap();
        let StoredNode::Internal { children, .. } = &mut root_node else {
            panic!("fixture must have an internal root");
        };
        *children[0].last_key.last_mut().unwrap() -= 1;
        let tampered_bytes = encode_node(&root_node).unwrap();
        let tampered_id = node_digest(&tampered_bytes);
        let mut tampered_root = built.root.clone();
        tampered_root.root_id = tampered_id;
        tampered_root.root_digest = root_digest(
            tampered_id,
            tampered_root.entry_count,
            tampered_root.direct_row_count,
            tampered_root.tree_height,
            tampered_root.layout,
        );

        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        for (node_id, bytes) in built.node_bytes() {
            writes.put(
                MUTATION_DIRECTORY_NODE_SPACE,
                StorageKey(Bytes::copy_from_slice(node_id)),
                StorageValue {
                    bytes: bytes.clone(),
                },
            );
        }
        writes.put(
            MUTATION_DIRECTORY_NODE_SPACE,
            StorageKey(Bytes::copy_from_slice(&tampered_id)),
            StorageValue {
                bytes: tampered_bytes,
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let point = Bytes::copy_from_slice(&4u32.to_be_bytes());
        let ranges = [MutationDirectoryKeyRange {
            start: point.clone(),
            end: Some(Bytes::copy_from_slice(&5u32.to_be_bytes())),
        }];
        let coordinate = [MutationDirectoryDirectCoordinate {
            part_index: 0,
            local_row: 0,
        }];
        for selection in [
            MutationDirectoryReadSelection::All(MutationDirectoryFullTraversalContext::Test),
            MutationDirectoryReadSelection::SortedRanges(&ranges),
            MutationDirectoryReadSelection::SortedUniquePoints(std::slice::from_ref(&point)),
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&coordinate),
        ] {
            let error = load_mutation_part_read_plan(&read, &tampered_root, selection)
                .await
                .expect_err("every selection must authenticate visited child bounds");
            assert!(error.to_string().contains("child summary mismatch"));
        }

        let mut bad_count = built.root.clone();
        bad_count.entry_count += 1;
        bad_count.root_digest = root_digest(
            bad_count.root_id,
            bad_count.entry_count,
            bad_count.direct_row_count,
            bad_count.tree_height,
            bad_count.layout,
        );
        let error = load_mutation_part_read_plan(
            &read,
            &bad_count,
            MutationDirectoryReadSelection::All(MutationDirectoryFullTraversalContext::Test),
        )
        .await
        .expect_err("root counts must agree with authenticated nodes");
        assert!(error.to_string().contains("root summary mismatch"));
    }

    #[tokio::test]
    async fn read_plan_rejects_missing_nodes_and_content_digest_mismatch() {
        let entries = (0..2).map(bounded_entry).collect::<Vec<_>>();
        let built = build_mutation_directory(LAYOUT_BOUNDED_DIRECT, &entries).unwrap();

        let missing_storage = StorageAdapter::new(Memory::new());
        let missing_read = missing_storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let error = load_mutation_part_read_plan(
            &missing_read,
            &built.root,
            MutationDirectoryReadSelection::All(MutationDirectoryFullTraversalContext::Test),
        )
        .await
        .expect_err("a missing authenticated node must fail");
        assert!(error.to_string().contains("missing node"));

        let digest_storage = StorageAdapter::new(Memory::new());
        let mut corrupt = built.node_bytes()[&built.root.root_id].to_vec();
        *corrupt.last_mut().unwrap() ^= 1;
        let mut writes = digest_storage.new_write_set();
        writes.put(
            MUTATION_DIRECTORY_NODE_SPACE,
            StorageKey(Bytes::copy_from_slice(&built.root.root_id)),
            StorageValue {
                bytes: Bytes::from(corrupt),
            },
        );
        digest_storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let digest_read = digest_storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let error = load_mutation_part_read_plan(
            &digest_read,
            &built.root,
            MutationDirectoryReadSelection::All(MutationDirectoryFullTraversalContext::Test),
        )
        .await
        .expect_err("content bytes must match their immutable node id");
        assert!(error.to_string().contains("content digest mismatch"));
    }
}
