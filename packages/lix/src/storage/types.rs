use std::{fmt, ops::Bound};

use bytes::Bytes;

use crate::storage::{Precondition, StorageError};

/// Maximum number of owned rows returned by one storage scan page.
pub const MAX_SCAN_PAGE_ROWS: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SpaceId(pub u32);

/// Engine-declared mutation semantics for one logical storage space.
///
/// Lix owns and validates these semantics. Storage implementations receive the
/// declaration so they can choose an appropriate physical layout without
/// exposing a second blob or object API.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ValueSemantics {
    Mutable,
    Immutable,
}

/// Who is responsible for detecting corruption of a space's *value* bytes.
///
/// This exists because one space in the engine authenticates its own values
/// more strongly than any backend checksum can, and paying for both is pure
/// duplicated work over the same bytes.
///
/// The default is [`ValueIntegrity::BackendVerified`] and every constructor
/// produces it. Opting out requires naming
/// [`StorageSpace::declare_content_addressed`] deliberately, so a space added
/// tomorrow is protected without anyone having to remember that it should be.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ValueIntegrity {
    /// The backend's own checksum is the only thing standing between a bit
    /// flip on disk and the engine. Backends must verify it on every read.
    #[default]
    BackendVerified,
    /// Every value in this space is a content-addressed blob whose **key is a
    /// BLAKE3-256 digest of its own bytes**, and the engine recomputes that
    /// digest and compares it before the bytes escape the read — in every
    /// build, release included.
    ///
    /// A backend may therefore skip its own value checksum here: a corruption
    /// it would have caught is caught by a strictly stronger check that has
    /// already been paid for. Skipping is an optimisation, never an
    /// obligation; a backend that cannot express it stays correct by doing
    /// nothing.
    ///
    /// **This is a claim about the engine, not a hint.** Declaring a space
    /// content-addressed without an unconditional digest check on every
    /// full-value read of it removes real protection and replaces it with
    /// nothing.
    ContentAddressed,
}

/// A logical ordered-key space and the value semantics Lix guarantees for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StorageSpace {
    pub id: SpaceId,
    pub name: &'static str,
    pub value_semantics: ValueSemantics,
    /// Who detects value corruption in this space. See [`ValueIntegrity`].
    pub value_integrity: ValueIntegrity,
}

impl StorageSpace {
    /// Declares the one value semantics a space id has.
    ///
    /// This is the registry's constructor and nothing else should reach for
    /// it. `mutable` and `immutable` below *check* their argument against
    /// `storage_spaces::ALL_STORAGE_SPACES`, so the declarations that registry
    /// is built from cannot go through them without a const-evaluation cycle.
    /// The split is the point: one constructor states a pairing, the other two
    /// are uses that must agree with it.
    pub(crate) const fn declare(
        id: SpaceId,
        name: &'static str,
        value_semantics: ValueSemantics,
    ) -> Self {
        Self {
            id,
            name,
            value_semantics,
            value_integrity: ValueIntegrity::BackendVerified,
        }
    }

    /// Declares a space whose values are BLAKE3-256 content-addressed by their
    /// own key and verified by the engine on every full-value read.
    ///
    /// Deliberately a **separate constructor** rather than a parameter on
    /// [`StorageSpace::declare`] or a builder method: the safe answer is what
    /// you get by default and by omission, and opting out is something you
    /// have to type. `storage_spaces::tests::exactly_one_space_declares_
    /// content_addressed_values` pins the opt-in set to the single space that
    /// has earned it, reading it back out of `ALL_STORAGE_SPACES` rather than
    /// from a second hand-written list.
    ///
    /// Do not reach for this because a space "holds hashes" or "is immutable".
    /// The requirement is exact: the key must be the digest of the value, and
    /// the engine must recompute and compare it on every read in release
    /// builds. `binary_cas.manifest` fails that test — its rows *contain*
    /// chunk hashes but are not themselves addressed by their content, and the
    /// whole-blob guard that would catch a corrupted manifest
    /// (`assemble_blob_bytes`) is `cfg!(debug_assertions)` only.
    pub(crate) const fn declare_content_addressed(
        id: SpaceId,
        name: &'static str,
        value_semantics: ValueSemantics,
    ) -> Self {
        Self {
            id,
            name,
            value_semantics,
            value_integrity: ValueIntegrity::ContentAddressed,
        }
    }

    /// A mutable space. Registered ids are checked against the registry.
    ///
    /// A space id has exactly one value semantics, and both adapters *place*
    /// data by that declaration rather than merely recording it: RocksDB puts
    /// immutable spaces in a separate column family (`rocksdb.rs:253-265`) and
    /// SlateDB moves immutable values out of the LSM into per-publication
    /// object segments, leaving a locator behind (`slatedb.rs:3395-3460`). A
    /// space handed to a backend as mutable on one path and immutable on
    /// another therefore writes one physical location and reads another.
    ///
    /// So this is an assertion in const context, which is a compile error:
    /// re-declaring a registered space with the other semantics does not
    /// build. Ids the registry does not own are unconstrained, because adapter
    /// and conformance suites legitimately reuse small ids such as
    /// `SpaceId(7)` for both semantics.
    pub const fn mutable(id: SpaceId, name: &'static str) -> Self {
        assert!(
            crate::storage_spaces::may_declare(id, ValueSemantics::Mutable),
            "this space id is registered immutable in ALL_STORAGE_SPACES; read \
             the space back from the registry instead of re-declaring it"
        );
        Self::declare(id, name, ValueSemantics::Mutable)
    }

    /// An immutable space. Registered ids are checked against the registry;
    /// see [`StorageSpace::mutable`] for why that check is worth a panic.
    pub const fn immutable(id: SpaceId, name: &'static str) -> Self {
        assert!(
            crate::storage_spaces::may_declare(id, ValueSemantics::Immutable),
            "this space id is registered mutable in ALL_STORAGE_SPACES; read \
             the space back from the registry instead of re-declaring it"
        );
        Self::declare(id, name, ValueSemantics::Immutable)
    }

    /// The same space id and name, re-declared mutable, for corruption tests.
    ///
    /// A space id has exactly one value semantics
    /// (`storage_spaces::ALL_STORAGE_SPACES`), and both adapters act on that
    /// declaration physically: RocksDB routes immutable spaces to a separate
    /// column family (`rocksdb.rs:253-265`) and SlateDB stores only a locator
    /// in the LSM while the value moves to a per-publication object segment
    /// (`slatedb.rs:3395-3460`). Handing the same id to a backend with two
    /// different semantics therefore writes one physical location and reads
    /// another.
    ///
    /// Corruption tests still need to place chosen bytes under a key the
    /// engine publishes write-once. This constructor exists so that need is
    /// spelled out at the call site instead of being smuggled in as a second
    /// `StorageSpace::mutable(SOME_SPACE.id, ..)` declaration that reads like
    /// a canonical one. It is `cfg(test)`, so no production path can reach it,
    /// and the registry drift guard treats every remaining raw re-declaration
    /// as a defect.
    ///
    /// **The state it fabricates is faithful only on the in-memory backend**,
    /// which stores mutable and immutable values under the same physical key
    /// and differs only in the write-once check (`storage/in_memory.rs:388`).
    /// A test that must corrupt an immutable value on a real backend has to
    /// delete the key in one committed write set and publish the replacement
    /// through the canonical immutable space in a second — see
    /// `engine-benchmarks/tests/corruption_recovery_qualification.rs`'s
    /// `replace_immutable_value_with_corruption`.
    #[cfg(test)]
    pub(crate) const fn mutable_view_for_corruption_test(self) -> Self {
        Self {
            id: self.id,
            name: self.name,
            value_semantics: ValueSemantics::Mutable,
            // A corruption test writes bytes that deliberately do not match
            // the key's digest, so this view must not claim the engine will
            // authenticate them.
            value_integrity: ValueIntegrity::BackendVerified,
        }
    }
}

impl fmt::Display for StorageSpace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}({:?}, {:?})",
            self.name, self.id, self.value_semantics
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key(pub Bytes);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadEntry {
    pub key: Key,
    pub value: ProjectedValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutEntry {
    pub key: Key,
    pub value: StoredValue,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PutBatch {
    pub entries: Vec<PutEntry>,
}

/// A validated slice inside one immutable encoded mutation buffer.
///
/// Storage write planning uses these compact descriptors to keep encoded keys
/// and values in batch-wide buffers until the final backend boundary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BufferRange {
    offset: usize,
    length: usize,
}

impl BufferRange {
    pub const fn new(offset: usize, length: usize) -> Self {
        Self { offset, length }
    }

    pub const fn offset(self) -> usize {
        self.offset
    }

    pub const fn len(self) -> usize {
        self.length
    }

    pub const fn is_empty(self) -> bool {
        self.length == 0
    }

    fn checked_end(self) -> Option<usize> {
        self.offset.checked_add(self.length)
    }
}

/// One put whose key and value are ranges in an [`EncodedMutationBatch`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodedPut {
    pub key: BufferRange,
    pub value: BufferRange,
}

/// Contiguous encoded storage mutations for one storage space.
///
/// This is the zero-copy ingress for domain lowerers which already encode a
/// complete batch. `key_bytes` contains every put and delete key; `value_bytes`
/// contains every put value. Construction validates every descriptor once, so
/// [`crate::storage_adapter::StorageWriteSet`] can retain the two buffers and
/// carry only ranges through sorting and duplicate validation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EncodedMutationBatch {
    key_bytes: Bytes,
    value_bytes: Bytes,
    puts: Vec<EncodedPut>,
    deletes: Vec<BufferRange>,
}

impl EncodedMutationBatch {
    pub fn try_new(
        key_bytes: Bytes,
        value_bytes: Bytes,
        puts: Vec<EncodedPut>,
        deletes: Vec<BufferRange>,
    ) -> Result<Self, EncodedMutationBatchError> {
        for (index, put) in puts.iter().enumerate() {
            validate_buffer_range(put.key, key_bytes.len()).map_err(|()| {
                EncodedMutationBatchError::PutKeyOutOfBounds {
                    index,
                    range: put.key,
                    buffer_len: key_bytes.len(),
                }
            })?;
            validate_buffer_range(put.value, value_bytes.len()).map_err(|()| {
                EncodedMutationBatchError::PutValueOutOfBounds {
                    index,
                    range: put.value,
                    buffer_len: value_bytes.len(),
                }
            })?;
        }
        for (index, range) in deletes.iter().copied().enumerate() {
            validate_buffer_range(range, key_bytes.len()).map_err(|()| {
                EncodedMutationBatchError::DeleteKeyOutOfBounds {
                    index,
                    range,
                    buffer_len: key_bytes.len(),
                }
            })?;
        }
        Ok(Self {
            key_bytes,
            value_bytes,
            puts,
            deletes,
        })
    }

    pub fn put_count(&self) -> usize {
        self.puts.len()
    }

    pub fn delete_count(&self) -> usize {
        self.deletes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.puts.is_empty() && self.deletes.is_empty()
    }

    pub fn key_bytes(&self) -> &Bytes {
        &self.key_bytes
    }

    pub fn value_bytes(&self) -> &Bytes {
        &self.value_bytes
    }

    pub fn puts(&self) -> &[EncodedPut] {
        &self.puts
    }

    pub fn deletes(&self) -> &[BufferRange] {
        &self.deletes
    }

    pub(crate) fn into_parts(self) -> (Bytes, Bytes, Vec<EncodedPut>, Vec<BufferRange>) {
        (self.key_bytes, self.value_bytes, self.puts, self.deletes)
    }
}

fn validate_buffer_range(range: BufferRange, buffer_len: usize) -> Result<(), ()> {
    match range.checked_end() {
        Some(end) if end <= buffer_len => Ok(()),
        Some(_) | None => Err(()),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodedMutationBatchError {
    PutKeyOutOfBounds {
        index: usize,
        range: BufferRange,
        buffer_len: usize,
    },
    PutValueOutOfBounds {
        index: usize,
        range: BufferRange,
        buffer_len: usize,
    },
    DeleteKeyOutOfBounds {
        index: usize,
        range: BufferRange,
        buffer_len: usize,
    },
}

impl fmt::Display for EncodedMutationBatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PutKeyOutOfBounds {
                index,
                range,
                buffer_len,
            } => write!(
                formatter,
                "encoded put {index} key range {range:?} exceeds key buffer length {buffer_len}"
            ),
            Self::PutValueOutOfBounds {
                index,
                range,
                buffer_len,
            } => write!(
                formatter,
                "encoded put {index} value range {range:?} exceeds value buffer length {buffer_len}"
            ),
            Self::DeleteKeyOutOfBounds {
                index,
                range,
                buffer_len,
            } => write!(
                formatter,
                "encoded delete {index} key range {range:?} exceeds key buffer length {buffer_len}"
            ),
        }
    }
}

impl std::error::Error for EncodedMutationBatchError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredValue {
    pub bytes: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRange {
    pub lower: Bound<Key>,
    pub upper: Bound<Key>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Prefix {
    pub bytes: Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GetOptions {
    pub projection: CoreProjection,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ScanOrder {
    #[default]
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BeginScanOptions {
    pub projection: CoreProjection,
    pub order: ScanOrder,
}

impl Default for BeginScanOptions {
    fn default() -> Self {
        Self {
            projection: CoreProjection::FullValue,
            order: ScanOrder::Ascending,
        }
    }
}

/// One page of a storage scan, together with whether the range has more rows.
///
/// Both fields are private on purpose. The only way to reach the rows is
/// [`ScanChunk::into_parts`], which hands back `has_more` in the same
/// expression, so a caller that stops after one page does so visibly at the
/// call site. The previous public `entries` field let
/// `cursor.next_page(MAX_SCAN_PAGE_ROWS).await?.entries` read as a complete
/// scan while silently dropping every row past the page boundary; nothing in
/// the type system, the linter, or CI could see that.
///
/// Callers that want every row in the range must not assemble pages by hand.
/// Use [`crate::storage::ScanCursor::collect_all`] for a materialized vector or
/// [`crate::storage::ScanCursor::next_chunk`] to stream page by page — neither
/// exposes a flag that can be forgotten.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use = "a scan page carries `has_more`; dropping it silently truncates the scan"]
pub struct ScanChunk {
    entries: Vec<ReadEntry>,
    has_more: bool,
}

impl ScanChunk {
    /// Builds one scan page. Storage adapters are the intended callers.
    pub fn new(entries: Vec<ReadEntry>, has_more: bool) -> Self {
        Self { entries, has_more }
    }

    /// Splits the page into its rows and whether the range continues past them.
    ///
    /// Binding both halves is the point: a caller that discards the flag has
    /// written that decision down where a reviewer can see it.
    pub fn into_parts(self) -> (Vec<ReadEntry>, bool) {
        (self.entries, self.has_more)
    }

    /// Rows in this page, not counting anything the range may hold after it.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether this page has no rows. A page can be empty while the range
    /// itself is not yet drained only if the backend says so.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetManyResult {
    /// One slot per key passed to `get_many`, flattened in request order and
    /// then key order within each request.
    ///
    /// Duplicates are preserved. `None` means the requested key was missing.
    pub values: Vec<Option<ProjectedValue>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GetManyRequest<'a> {
    pub space: StorageSpace,
    pub keys: &'a [Key],
    pub opts: GetOptions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreProjection {
    KeyOnly,
    FullValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectedValue {
    KeyOnly,
    FullValue(Bytes),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadOptions {
    pub snapshot: Option<SnapshotRef>,
    pub consistency: ReadConsistency,
    /// Minimum persistence boundary for rows returned by this read.
    ///
    /// Most engine reads use [`ReadDurability::Visible`], which permits a
    /// backend to serve its latest committed in-memory state. Callers that
    /// must distinguish a published write from one that has crossed the
    /// backend's documented durable boundary use [`ReadDurability::Durable`].
    /// Backends that cannot prove that boundary must return an explicit
    /// storage error instead of silently downgrading the read.
    pub durability: ReadDurability,
}

/// Persistence boundary required from a storage read.
///
/// This is deliberately separate from [`ReadConsistency`]: consistency
/// chooses a snapshot/view, while durability controls which persistence tier
/// may satisfy that view.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadDurability {
    /// The backend's normal committed view, which may include data awaiting a
    /// background durability step.
    #[default]
    Visible,
    /// Only data which the backend can prove has crossed its durable
    /// persistence boundary.
    Durable,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadConsistency {
    #[default]
    Snapshot,
    StaleOk,
    Latest,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteOptions {
    pub base_snapshot: Option<SnapshotRef>,
    pub idempotency_key: Option<Bytes>,
    /// Do not acknowledge the commit until the backend has crossed its
    /// durable persistence boundary.
    pub await_durable: bool,
    /// Conditions evaluated atomically against the state immediately before
    /// this write becomes visible.
    pub preconditions: Vec<Precondition>,
    /// Lower bound for the backend's encoded write-batch capacity.
    ///
    /// Correctness never depends on this hint. The storage adapter fills it
    /// from the complete canonical write set so backends with contiguous batch
    /// encodings can allocate their large buffer once.
    pub batch_capacity_hint_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRef(pub Bytes);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteStats {
    pub put_entries: u64,
    pub deleted_entries: u64,
    pub deleted_ranges: u64,
    pub written_bytes: u64,
    pub storage_calls: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitResult {
    pub commit_id: Option<Bytes>,
    pub stats: WriteStats,
}

impl Prefix {
    pub fn to_range(&self) -> Result<KeyRange, StorageError> {
        let lower = Key(self.bytes.clone());
        let mut upper = self.bytes.to_vec();

        while let Some(last) = upper.last_mut() {
            if *last == u8::MAX {
                upper.pop();
            } else {
                *last += 1;
                return Ok(KeyRange {
                    lower: Bound::Included(lower),
                    upper: Bound::Excluded(Key(Bytes::from(upper))),
                });
            }
        }

        Ok(KeyRange {
            lower: Bound::Included(lower),
            upper: Bound::Unbounded,
        })
    }
}

impl Default for GetOptions {
    fn default() -> Self {
        Self {
            projection: CoreProjection::FullValue,
        }
    }
}

impl GetManyResult {
    pub fn new(values: Vec<Option<ProjectedValue>>) -> Self {
        Self { values }
    }

    pub fn entries_for_requested_keys(&self, keys: &[Key]) -> Vec<ReadEntry> {
        keys.iter()
            .cloned()
            .zip(self.values.iter().cloned())
            .filter_map(|(key, value)| value.map(|value| ReadEntry { key, value }))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_ranges_cover_exact_lexicographic_edges() {
        let cases = [
            (
                &[][..],
                Bound::Included(Key(Bytes::new())),
                Bound::Unbounded,
            ),
            (
                &[0x61][..],
                Bound::Included(Key(Bytes::from_static(&[0x61]))),
                Bound::Excluded(Key(Bytes::from_static(&[0x62]))),
            ),
            (
                &[0x61, 0xff][..],
                Bound::Included(Key(Bytes::from_static(&[0x61, 0xff]))),
                Bound::Excluded(Key(Bytes::from_static(&[0x62]))),
            ),
            (
                &[0xff, 0xff][..],
                Bound::Included(Key(Bytes::from_static(&[0xff, 0xff]))),
                Bound::Unbounded,
            ),
        ];
        for (bytes, lower, upper) in cases {
            let actual = Prefix {
                bytes: Bytes::copy_from_slice(bytes),
            }
            .to_range()
            .expect("prefix range should be valid");
            assert_eq!(actual, KeyRange { lower, upper });
        }
    }
}
