//! Arena-first string interner for the materialized batch owners.
//!
//! Both state planes dictionary-encode the identity columns of a materialized
//! batch — schema keys, file ids, branch ids — into one contiguous UTF-8 arena
//! addressed by `u32` ordinals. Small dictionaries use a linear range scan;
//! larger ones promote to a hash table whose buckets hold compact arena
//! ordinals, with same-hash entries chained through a flat ordinal column.
//! Keys therefore stay stable across arena growth without retaining one heap
//! allocation per distinct string, and finishing transfers the arena into
//! [`Bytes`] without recopying it.
//!
//! This machinery existed twice — once in the canonical plane and once in the
//! derived plane — and the copies had already drifted apart in wording, in
//! constructor shape, and in the seeds handed to the promoted lookup. One
//! implementation here means the next tuning change reaches both planes
//! instead of one.
//!
//! It lives in `common` for the same reason [`super::exact_batch`] does: it is
//! a generic container over borrowed identity strings with no repository
//! semantics of its own. Its only crate dependency is [`SharedStr`], which
//! `common` owns.

use std::collections::HashMap;
use std::ops::Range;

use ahash::RandomState;
use bytes::Bytes;

use super::SharedStr;

/// Hash builder for dictionary lookups.
///
/// `ahash` with fixed seeds. A batch dictionary is process-local, short-lived,
/// and never keyed by adversarial input, so the DoS-resistant random seeding is
/// pure cost here; fixing the seeds also makes bucket layout reproducible
/// across runs.
pub(crate) type FastHashBuilder = RandomState;

/// The one hash builder both planes intern through.
pub(crate) fn fast_hash_builder() -> FastHashBuilder {
    FastHashBuilder::with_seeds(0, 0, 0, 0)
}

/// Entries below this count are found by scanning `ranges` directly, which
/// beats hashing for the batch sizes that dominate.
const SMALL_DICTIONARY_LOOKUP_LIMIT: usize = 32;
/// First arena reservation for a dictionary that is still in linear mode.
const SMALL_DICTIONARY_ARENA_BYTES: usize = 1024;
/// Terminal ordinal, reserved as the end-of-collision-chain sentinel.
const NO_DICTIONARY_ORDINAL: u32 = u32::MAX;
#[cfg(test)]
const LARGE_DICTIONARY_ALLOCATION_BYTES: usize = 32 * 1024;

/// Immutable dictionary storage shared by every identity column in one batch.
///
/// Distinct values occupy one contiguous UTF-8 arena, so repeated batch-wide
/// metadata costs a four-byte ordinal per row instead of another owned
/// allocation.
#[derive(Debug, Clone, Default)]
pub(crate) struct StringDictionary {
    bytes: Bytes,
    ranges: Vec<Range<u32>>,
    #[cfg(test)]
    arena_allocation_count: usize,
    #[cfg(test)]
    arena_large_allocation_count: usize,
}

impl StringDictionary {
    /// Borrows the value at `ordinal`.
    pub(crate) fn get(&self, ordinal: u32) -> &str {
        let range = self
            .ranges
            .get(ordinal as usize)
            .expect("string ordinal belongs to this dictionary");
        let range = range.start as usize..range.end as usize;
        // SAFETY: the builder appends complete `str` values and records their
        // exact boundaries. `Bytes` preserves that immutable allocation.
        unsafe { std::str::from_utf8_unchecked(&self.bytes[range]) }
    }

    /// Shares the value at `ordinal` without copying it out of the arena.
    pub(crate) fn shared(&self, ordinal: u32) -> SharedStr {
        let value = self.get(ordinal);
        SharedStr::from_utf8_slice(self.bytes.clone(), value)
            .expect("dictionary value points into its own byte arena")
    }

    /// Number of distinct interned values.
    pub(crate) fn len(&self) -> usize {
        self.ranges.len()
    }

    /// Size of the UTF-8 arena in bytes.
    #[cfg(test)]
    pub(crate) fn byte_len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the arena holds any bytes at all.
    #[cfg(test)]
    pub(crate) fn is_arena_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Reserved, not occupied, range slots — the allocation the range column
    /// actually holds.
    #[cfg(test)]
    pub(crate) fn ranges_capacity(&self) -> usize {
        self.ranges.capacity()
    }

    /// How many times the arena was (re)reserved while building.
    #[cfg(test)]
    pub(crate) fn arena_allocation_count(&self) -> usize {
        self.arena_allocation_count
    }

    /// How many of those reservations were large enough to matter.
    #[cfg(test)]
    pub(crate) fn arena_large_allocation_count(&self) -> usize {
        self.arena_large_allocation_count
    }
}

enum StringLookup {
    Small,
    Hashed(HashMap<u64, u32, FastHashBuilder>),
}

/// Builder for a [`StringDictionary`].
pub(crate) struct StringDictionaryBuilder {
    bytes: Vec<u8>,
    ranges: Vec<Range<u32>>,
    collision_next: Vec<u32>,
    lookup: StringLookup,
    hash_builder: FastHashBuilder,
    expected_entry_capacity: usize,
    maximum_entry_capacity: usize,
    max_string_len: usize,
    exact_byte_capacity: bool,
    #[cfg(test)]
    arena_allocation_count: usize,
    #[cfg(test)]
    arena_large_allocation_count: usize,
}

impl StringDictionaryBuilder {
    /// Reserves a dictionary.
    ///
    /// `entry_capacity` and `byte_capacity` size the initial allocations.
    /// `projected_entry_capacity` is the caller's upper bound on distinct
    /// entries — typically rows × identity columns per row — and is used only
    /// to project arena growth once the lookup has promoted to hashing; pass
    /// `0` when `entry_capacity` is already that bound.
    ///
    /// `exact_byte_capacity` marks `byte_capacity` as a measurement rather than
    /// an estimate, which suppresses the speculative arena reservation taken at
    /// promotion.
    pub(crate) fn with_capacity(
        projected_entry_capacity: usize,
        entry_capacity: usize,
        byte_capacity: usize,
        exact_byte_capacity: bool,
    ) -> Self {
        let expected_entry_capacity = entry_capacity.max(1);
        Self {
            bytes: Vec::with_capacity(byte_capacity),
            ranges: Vec::with_capacity(entry_capacity),
            collision_next: Vec::with_capacity(entry_capacity),
            lookup: StringLookup::Small,
            hash_builder: fast_hash_builder(),
            expected_entry_capacity,
            maximum_entry_capacity: projected_entry_capacity.max(entry_capacity).max(1),
            max_string_len: 0,
            exact_byte_capacity,
            #[cfg(test)]
            arena_allocation_count: usize::from(byte_capacity != 0),
            #[cfg(test)]
            arena_large_allocation_count: usize::from(
                byte_capacity >= LARGE_DICTIONARY_ALLOCATION_BYTES,
            ),
        }
    }

    /// Interns an owned value; the allocation is dropped, only its bytes are
    /// kept.
    pub(crate) fn intern_owned(&mut self, value: String) -> u32 {
        self.intern(value.as_str())
    }

    pub(crate) fn intern(&mut self, value: &str) -> u32 {
        if !matches!(&self.lookup, StringLookup::Small) {
            return self.intern_hashed(value);
        }
        if let Some(ordinal) = self.find_linear(value) {
            return ordinal;
        }
        if self.ranges.len() == SMALL_DICTIONARY_LOOKUP_LIMIT {
            self.promote_to_hashed(value.len());
            self.intern_hashed(value)
        } else {
            self.append_small(value)
        }
    }

    fn find_linear(&self, value: &str) -> Option<u32> {
        self.ranges
            .iter()
            .position(|range| {
                &self.bytes[range.start as usize..range.end as usize] == value.as_bytes()
            })
            .map(|ordinal| u32::try_from(ordinal).expect("string dictionary ordinal exceeds u32"))
    }

    fn intern_hashed(&mut self, value: &str) -> u32 {
        let hash = self.hash_builder.hash_one(value.as_bytes());
        let mut candidate = match &self.lookup {
            StringLookup::Small => {
                unreachable!("hashed dictionary lookup must be promoted first")
            }
            StringLookup::Hashed(lookup) => lookup.get(&hash).copied(),
        };
        while let Some(ordinal) = candidate {
            if self.value(ordinal) == value {
                return ordinal;
            }
            let next = self.collision_next[ordinal as usize];
            candidate = (next != NO_DICTIONARY_ORDINAL).then_some(next);
        }
        self.append_hashed(value, hash)
    }

    fn value(&self, ordinal: u32) -> &str {
        let range = &self.ranges[ordinal as usize];
        // SAFETY: `append_bytes` receives a `str` and records that complete
        // value's exact boundaries.
        unsafe {
            std::str::from_utf8_unchecked(&self.bytes[range.start as usize..range.end as usize])
        }
    }

    fn append_small(&mut self, value: &str) -> u32 {
        let ordinal = self.append_bytes(value);
        self.collision_next.push(NO_DICTIONARY_ORDINAL);
        ordinal
    }

    fn append_hashed(&mut self, value: &str, hash: u64) -> u32 {
        let previous_head = match &self.lookup {
            StringLookup::Small => {
                unreachable!("hashed dictionary insertion must be promoted first")
            }
            StringLookup::Hashed(lookup) => {
                lookup.get(&hash).copied().unwrap_or(NO_DICTIONARY_ORDINAL)
            }
        };
        let ordinal = self.append_bytes(value);
        self.collision_next.push(previous_head);
        let StringLookup::Hashed(lookup) = &mut self.lookup else {
            unreachable!("hashed dictionary insertion must retain its lookup")
        };
        lookup.insert(hash, ordinal);
        ordinal
    }

    fn append_bytes(&mut self, value: &str) -> u32 {
        self.max_string_len = self.max_string_len.max(value.len());
        let end = self
            .bytes
            .len()
            .checked_add(value.len())
            .expect("string dictionary byte count overflow");
        let end_u32 = u32::try_from(end).expect("string dictionary exceeds u32 bytes");
        self.ensure_arena_capacity(end);
        let start =
            u32::try_from(self.bytes.len()).expect("string dictionary start exceeds u32 bytes");
        self.bytes.extend_from_slice(value.as_bytes());
        let ordinal =
            u32::try_from(self.ranges.len()).expect("string dictionary exceeds u32 entries");
        assert_ne!(
            ordinal, NO_DICTIONARY_ORDINAL,
            "string dictionary reserves the terminal u32 ordinal"
        );
        self.ranges.push(start..end_u32);
        ordinal
    }

    fn ensure_arena_capacity(&mut self, required: usize) {
        if required <= self.bytes.capacity() {
            return;
        }
        let projected = match &self.lookup {
            StringLookup::Small => SMALL_DICTIONARY_ARENA_BYTES,
            StringLookup::Hashed(_) => self
                .maximum_entry_capacity
                .saturating_mul(self.max_string_len),
        };
        let target = required.max(projected);
        self.bytes.reserve_exact(target - self.bytes.len());
        #[cfg(test)]
        {
            self.arena_allocation_count += 1;
            self.arena_large_allocation_count +=
                usize::from(target >= LARGE_DICTIONARY_ALLOCATION_BYTES);
        }
    }

    fn promote_to_hashed(&mut self, incoming_len: usize) {
        self.max_string_len = self.max_string_len.max(incoming_len);
        let projected_entries = self
            .expected_entry_capacity
            .max(self.ranges.len().saturating_add(1));
        let projected_bytes = projected_entries.saturating_mul(self.max_string_len);
        if !self.exact_byte_capacity && projected_bytes > self.bytes.capacity() {
            self.bytes.reserve_exact(projected_bytes - self.bytes.len());
            #[cfg(test)]
            {
                self.arena_allocation_count += 1;
                self.arena_large_allocation_count +=
                    usize::from(projected_bytes >= LARGE_DICTIONARY_ALLOCATION_BYTES);
            }
        }

        let mut lookup = HashMap::with_capacity_and_hasher(projected_entries, fast_hash_builder());
        for ordinal in 0..self.ranges.len() {
            let ordinal = u32::try_from(ordinal).expect("string dictionary ordinal exceeds u32");
            let hash = self.hash_builder.hash_one(self.value(ordinal).as_bytes());
            self.collision_next[ordinal as usize] = lookup
                .insert(hash, ordinal)
                .unwrap_or(NO_DICTIONARY_ORDINAL);
        }
        self.lookup = StringLookup::Hashed(lookup);
    }

    pub(crate) fn finish(self) -> StringDictionary {
        debug_assert!(
            self.ranges
                .iter()
                .all(|range| range.start <= range.end && range.end as usize <= self.bytes.len())
        );
        StringDictionary {
            bytes: Bytes::from(self.bytes),
            ranges: self.ranges,
            #[cfg(test)]
            arena_allocation_count: self.arena_allocation_count,
            #[cfg(test)]
            arena_large_allocation_count: self.arena_large_allocation_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn builder() -> StringDictionaryBuilder {
        StringDictionaryBuilder::with_capacity(0, 0, 0, false)
    }

    #[test]
    fn interning_deduplicates_and_preserves_values() {
        let mut builder = builder();
        assert_eq!(builder.intern("alpha"), 0);
        assert_eq!(builder.intern("beta"), 1);
        assert_eq!(builder.intern("alpha"), 0);
        assert_eq!(builder.intern_owned("gamma".to_owned()), 2);

        let dictionary = builder.finish();
        assert_eq!(dictionary.len(), 3);
        assert_eq!(dictionary.get(0), "alpha");
        assert_eq!(dictionary.get(1), "beta");
        assert_eq!(dictionary.get(2), "gamma");
        assert_eq!(dictionary.shared(1).as_str(), "beta");
        assert_eq!(dictionary.byte_len(), "alphabetagamma".len());
    }

    /// The linear-to-hashed promotion has to keep every ordinal it already
    /// handed out, and keep deduplicating across the boundary.
    #[test]
    fn promotion_to_the_hashed_lookup_preserves_ordinals() {
        let mut builder = builder();
        let values = (0..SMALL_DICTIONARY_LOOKUP_LIMIT * 4)
            .map(|index| format!("value-{index}"))
            .collect::<Vec<_>>();
        let ordinals = values
            .iter()
            .map(|value| builder.intern(value))
            .collect::<Vec<_>>();
        assert_eq!(
            ordinals,
            (0..values.len() as u32).collect::<Vec<_>>(),
            "each distinct value takes the next ordinal"
        );
        for (expected, value) in ordinals.iter().zip(&values) {
            assert_eq!(builder.intern(value), *expected, "re-interning is stable");
        }

        let dictionary = builder.finish();
        assert_eq!(dictionary.len(), values.len());
        for (ordinal, value) in values.iter().enumerate() {
            assert_eq!(dictionary.get(ordinal as u32), value);
        }
    }

    #[test]
    fn an_empty_dictionary_holds_no_arena() {
        let dictionary = builder().finish();
        assert_eq!(dictionary.len(), 0);
        assert!(dictionary.is_arena_empty());
    }
}
