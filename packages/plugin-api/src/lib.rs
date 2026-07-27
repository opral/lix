//! The small, author-facing API for Lix Component v2 plugins.
//!
//! Authors implement [`FormatPlugin`]: four semantic transitions over an
//! immutable document plus one stateless conflict hook with a deterministic
//! default. This crate owns the generated WIT bindings, bounded
//! packet cursors, lazy attachment tables, and transport error mapping. It is
//! intentionally *not* a universal document model: JSON, CSV, Markdown, and
//! Excalidraw keep their own parser, identity policy, and persistent state.
//!
//! The two rules that preserve v2's hot path are visible in the API:
//!
//! - [`Source::read_all`] is an explicit cold-path choice; warm code can use
//!   [`Source::read`] and receives only the ranges it asks for.
//! - [`FileUpdate`] contains verified base-relative splices. Returning
//!   [`ByteEdit`] values is likewise sparse and base-relative.

// `wit-bindgen` emits a generated empty vector with matching length/capacity.
// The public reader methods deliberately use `next` because they are fallible
// pull sources (a standard `Iterator` would turn a source failure into an
// item-level error and make the common `reader.next()?` shape unavailable).
#![allow(
    clippy::missing_errors_doc,
    clippy::same_length_and_capacity,
    clippy::should_implement_trait
)]

wit_bindgen::generate!({
    path: "wit",
    world: "plugin",
    // The component crate invokes this generated export macro. Keeping the
    // WIT lowering here lets an author import one Rust crate rather than five
    // generated WIT traits and three resource implementations.
    pub_export_macro: true,
    export_macro_name: "__export_component_v2",
    default_bindings_module: "lix_plugin_api_v2",
});

use exports::lix::plugin::api::{
    ByteOutputs, ChangeCursor, ChangePage, ConflictUpdate as WitConflictUpdate, Document,
    EditCursor, EditPage, EntityTransition, EntityUpdate as WitEntityUpdate, FileTransition,
    FileUpdate as WitFileUpdate, Guest, GuestByteOutputs, GuestChangeCursor, GuestDocument,
    GuestEditCursor, GuestResolutionCursor, InputBytes, OpenEntitiesInput, OpenFileInput,
    OutputBytes, OutputRange, OutputSplice, PluginError, ResolutionCursor, ResolutionPage,
};
use lix::plugin::host::{ByteSource, ByteSources, PacketSource, SourceError, TransitionBudget};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

const PACKET_FORMAT_VERSION: u16 = 1;
const EDIT_METADATA_BYTES: usize = 24;

/// A plugin failure classified for the v2 host contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidInput(String),
    RecordTooLarge(u64),
    LimitExceeded(String),
    DeadlineExceeded,
    Internal(String),
}

impl Error {
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

fn plugin_error(error: Error) -> PluginError {
    match error {
        Error::InvalidInput(message) => PluginError::InvalidInput(message),
        Error::RecordTooLarge(size) => PluginError::RecordTooLarge(size),
        Error::LimitExceeded(message) => PluginError::LimitExceeded(message),
        Error::DeadlineExceeded => PluginError::DeadlineExceeded,
        Error::Internal(message) => PluginError::Internal(message),
    }
}

fn source_error(error: SourceError) -> Error {
    match error {
        SourceError::InvalidRange => Error::invalid_input("invalid byte-source range"),
        SourceError::RecordTooLarge(size) => Error::RecordTooLarge(size),
        SourceError::LimitExceeded(message) => Error::LimitExceeded(message),
        SourceError::DeadlineExceeded => Error::DeadlineExceeded,
        SourceError::Unavailable(message) => Error::Internal(message),
    }
}

fn file_info(descriptor: &exports::lix::plugin::api::FileDescriptor) -> FileInfo {
    FileInfo {
        path: descriptor.path.clone(),
        media_type: descriptor.media_type.clone(),
    }
}

/// The retry-stable mutation namespace supplied by the host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IdNamespace {
    high: u64,
    low: u64,
}

impl IdNamespace {
    /// Returns the canonical primary-key component for a newly allocated
    /// entity ordinal.
    ///
    /// The host supplies a namespace that is stable for one mutation. A
    /// format must preserve acknowledged IDs and use a deterministic ordinal
    /// only for genuinely new entities. This helper implements the normative
    /// `namespace || ordinal` big-endian, unpadded-base64url encoding so a
    /// normal plugin never has to reproduce that wire rule.
    pub fn id(self, ordinal: u64) -> String {
        const BASE64URL: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

        let mut input = [0_u8; 24];
        input[..8].copy_from_slice(&self.high.to_be_bytes());
        input[8..16].copy_from_slice(&self.low.to_be_bytes());
        input[16..].copy_from_slice(&ordinal.to_be_bytes());

        // 24 is divisible by three, so base64url produces exactly 32 bytes
        // and never needs padding. `BASE64URL` contains ASCII only.
        let mut output = [0_u8; 32];
        for (chunk_index, chunk) in input.chunks_exact(3).enumerate() {
            let output_index = chunk_index * 4;
            output[output_index] = BASE64URL[usize::from(chunk[0] >> 2)];
            output[output_index + 1] =
                BASE64URL[usize::from(((chunk[0] & 0b0000_0011) << 4) | (chunk[1] >> 4))];
            output[output_index + 2] =
                BASE64URL[usize::from(((chunk[1] & 0b0000_1111) << 2) | (chunk[2] >> 6))];
            output[output_index + 3] = BASE64URL[usize::from(chunk[2] & 0b0011_1111)];
        }
        String::from_utf8(output.to_vec()).expect("the base64url alphabet is valid UTF-8")
    }
}

/// One complete semantic entity. Parentage, order, and format-specific facts
/// belong in `snapshot`; the API runtime never interprets them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityRecord {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    pub snapshot: Vec<u8>,
}

/// Classification of a complete entity upsert.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeEffect {
    Content,
    FormatOnly,
}

/// A complete upsert or a tombstone (`snapshot: None`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityChange {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    pub snapshot: Option<Vec<u8>>,
    pub effect: ChangeEffect,
}

impl EntityChange {
    pub fn upsert(
        schema_key: impl Into<String>,
        entity_pk: Vec<String>,
        snapshot: Vec<u8>,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            entity_pk,
            snapshot: Some(snapshot),
            effect: ChangeEffect::Content,
        }
    }

    pub fn delete(schema_key: impl Into<String>, entity_pk: Vec<String>) -> Self {
        Self {
            schema_key: schema_key.into(),
            entity_pk,
            snapshot: None,
            effect: ChangeEffect::Content,
        }
    }
}

/// One lazy immutable value in a three-way semantic entity conflict.
///
/// Calling [`Self::read`] is explicit because attachment-backed values can be
/// large. A resolver that returns [`ConflictResolution::TakeA`] or
/// [`ConflictResolution::TakeB`] does not copy the selected snapshot through
/// guest memory.
pub struct ConflictValue<'a> {
    value: &'a EncodedConflictValue,
    attachments: Option<&'a ByteSources>,
    budget: &'a TransitionBudget,
}

impl std::fmt::Debug for ConflictValue<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConflictValue")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl ConflictValue<'_> {
    pub fn len(&self) -> u64 {
        match self.value {
            EncodedConflictValue::Inline(bytes) => {
                u64::try_from(bytes.len()).expect("usize fits u64")
            }
            EncodedConflictValue::Attachment { length, .. } => *length,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn read(&self) -> Result<Vec<u8>> {
        match self.value {
            EncodedConflictValue::Inline(bytes) => Ok(bytes.clone()),
            EncodedConflictValue::Attachment {
                index,
                offset,
                length,
            } => read_attachment(self.attachments, self.budget, *index, *offset, *length),
        }
    }
}

fn conflict_value<'a>(
    value: &'a EncodedConflictValue,
    attachments: Option<&'a ByteSources>,
    budget: &'a TransitionBudget,
) -> ConflictValue<'a> {
    ConflictValue {
        value,
        attachments,
        budget,
    }
}

/// One same-key, canonically ordered three-way semantic conflict.
///
/// `a` and `b` are ordered by durable `(updated_at, change_id)`, independent
/// of which branch initiated the merge. That makes every resolver
/// deterministic in both merge directions.
pub struct EntityConflict<'a> {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    pub base: Option<ConflictValue<'a>>,
    pub a: Option<ConflictValue<'a>>,
    pub b: Option<ConflictValue<'a>>,
}

impl std::fmt::Debug for EntityConflict<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EntityConflict")
            .field("schema_key", &self.schema_key)
            .field("entity_pk", &self.entity_pk)
            .field("base", &self.base)
            .field("a", &self.a)
            .field("b", &self.b)
            .finish()
    }
}

impl EntityConflict<'_> {
    /// The deterministic safe fallback for a format-specific resolver.
    pub fn take_b_or_delete(&self) -> ConflictResolution {
        if self.b.is_some() {
            ConflictResolution::TakeB
        } else {
            ConflictResolution::Delete
        }
    }
}

/// The deterministic resolution of one [`EntityConflict`].
///
/// `Take*` retains a host-owned immutable value without copying it through
/// Wasm. `Replace` supplies one newly composed complete semantic snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConflictResolution {
    TakeBase,
    TakeA,
    TakeB,
    Replace(Vec<u8>),
    Delete,
}

/// A sparse base-relative renderer edit. The API runtime transparently uses a lazy
/// output attachment when an insert does not fit an inline packet page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ByteEdit {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: Arc<Vec<u8>>,
}

impl ByteEdit {
    pub fn new(offset: u64, delete_len: u64, insert: impl Into<Vec<u8>>) -> Self {
        Self {
            offset,
            delete_len,
            insert: Arc::new(insert.into()),
        }
    }
}

/// Describes the inserted bytes for one verified file splice.
///
/// Most local edits are inline. For a large replacement, the host preserves a
/// range into [`FileUpdate::after_source`] so a format can choose exactly what
/// to read instead of materializing the complete replacement at the boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InputInsert {
    Inline(Vec<u8>),
    AfterRange { offset: u64, length: u64 },
}

/// A verified base-relative incoming file splice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputSplice {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: InputInsert,
}

/// Lazy immutable source bytes. `read_all` is useful for cold parsing; warm
/// paths should use `read` for the affected range/window.
#[derive(Clone, Copy)]
pub struct Source<'a> {
    source: &'a ByteSource,
    budget: &'a TransitionBudget,
}

impl std::fmt::Debug for Source<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Source")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl<'a> Source<'a> {
    fn new(source: &'a ByteSource, budget: &'a TransitionBudget) -> Self {
        Self { source, budget }
    }

    pub fn len(&self) -> u64 {
        self.source.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn read(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        read_source_range(self.source, self.budget, offset, length)
    }

    pub fn read_all(&self) -> Result<Vec<u8>> {
        self.read(0, self.len())
    }
}

fn read_source_range(
    source: &ByteSource,
    budget: &TransitionBudget,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| Error::invalid_input("byte-source range overflow"))?;
    if end > source.len() {
        return Err(Error::invalid_input("byte-source range exceeds source"));
    }
    let capacity = usize::try_from(length)
        .map_err(|_| Error::LimitExceeded("source is too large".to_owned()))?;
    let mut output = Vec::with_capacity(capacity);
    let page_cap = budget.limits().max_page_bytes.max(1);
    let mut cursor = offset;
    while cursor < end {
        let request =
            u32::try_from((end - cursor).min(u64::from(page_cap))).expect("bounded by u32");
        let page = source.read(budget, cursor, request).map_err(source_error)?;
        if page.is_empty() {
            return Err(Error::Internal(
                "byte source returned an empty page before EOF".to_owned(),
            ));
        }
        if page.len() > usize::try_from(request).expect("u32 fits usize") {
            return Err(Error::Internal(
                "byte source returned more bytes than requested".to_owned(),
            ));
        }
        cursor += u64::try_from(page.len()).expect("usize fits u64");
        output.extend_from_slice(&page);
    }
    Ok(output)
}

/// Bounded, permanent-EOF source of complete durable semantic entities.
pub struct EntityReader<'a> {
    source: &'a PacketSource,
    budget: &'a TransitionBudget,
    pending: VecDeque<EntityRecord>,
    eof: bool,
}

impl std::fmt::Debug for EntityReader<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EntityReader")
            .finish_non_exhaustive()
    }
}

impl<'a> EntityReader<'a> {
    fn new(source: &'a PacketSource, budget: &'a TransitionBudget) -> Self {
        Self {
            source,
            budget,
            pending: VecDeque::new(),
            eof: false,
        }
    }

    pub fn next(&mut self) -> Result<Option<EntityRecord>> {
        loop {
            if let Some(record) = self.pending.pop_front() {
                return Ok(Some(record));
            }
            if self.eof {
                return Ok(None);
            }
            let max_bytes = self.budget.limits().max_page_bytes.max(1);
            let Some(page) = self
                .source
                .next(self.budget, max_bytes)
                .map_err(source_error)?
            else {
                self.eof = true;
                return Ok(None);
            };
            if page.format_version != PACKET_FORMAT_VERSION {
                return Err(Error::invalid_input(format!(
                    "unsupported packet version {}",
                    page.format_version
                )));
            }
            let mut attachment_error = None;
            let records =
                decode_entity_page(&page.payload, page.record_count, |index, offset, length| {
                    read_attachment(
                        page.attachments.as_ref(),
                        self.budget,
                        index,
                        offset,
                        length,
                    )
                    .map_err(|error| {
                        attachment_error = Some(error);
                        "attachment read failed".to_owned()
                    })
                })
                .map_err(|error| attachment_error.unwrap_or_else(|| Error::invalid_input(error)))?;
            self.pending.extend(records);
        }
    }
}

/// Bounded, permanent-EOF source of merge-resolved entity changes.
pub struct EntityChangeReader<'a> {
    source: &'a PacketSource,
    budget: &'a TransitionBudget,
    pending: VecDeque<EntityChange>,
    eof: bool,
}

impl std::fmt::Debug for EntityChangeReader<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EntityChangeReader")
            .finish_non_exhaustive()
    }
}

impl<'a> EntityChangeReader<'a> {
    fn new(source: &'a PacketSource, budget: &'a TransitionBudget) -> Self {
        Self {
            source,
            budget,
            pending: VecDeque::new(),
            eof: false,
        }
    }

    pub fn next(&mut self) -> Result<Option<EntityChange>> {
        loop {
            if let Some(change) = self.pending.pop_front() {
                return Ok(Some(change));
            }
            if self.eof {
                return Ok(None);
            }
            let max_bytes = self.budget.limits().max_page_bytes.max(1);
            let Some(page) = self
                .source
                .next(self.budget, max_bytes)
                .map_err(source_error)?
            else {
                self.eof = true;
                return Ok(None);
            };
            if page.format_version != PACKET_FORMAT_VERSION {
                return Err(Error::invalid_input(format!(
                    "unsupported packet version {}",
                    page.format_version
                )));
            }
            let mut attachment_error = None;
            let changes =
                decode_change_page(&page.payload, page.record_count, |index, offset, length| {
                    read_attachment(
                        page.attachments.as_ref(),
                        self.budget,
                        index,
                        offset,
                        length,
                    )
                    .map_err(|error| {
                        attachment_error = Some(error);
                        "attachment read failed".to_owned()
                    })
                })
                .map_err(|error| attachment_error.unwrap_or_else(|| Error::invalid_input(error)))?;
            self.pending.extend(changes);
        }
    }
}

fn read_attachment(
    attachments: Option<&ByteSources>,
    budget: &TransitionBudget,
    index: u32,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>> {
    let attachments =
        attachments.ok_or_else(|| Error::invalid_input("packet attachment table is missing"))?;
    let source_len = attachments.len(index).map_err(source_error)?;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| Error::invalid_input("attachment range overflow"))?;
    if end > source_len {
        return Err(Error::invalid_input("attachment range exceeds source"));
    }
    let capacity = usize::try_from(length)
        .map_err(|_| Error::LimitExceeded("attachment is too large".to_owned()))?;
    let mut output = Vec::with_capacity(capacity);
    let page_cap = budget.limits().max_page_bytes.max(1);
    let mut cursor = offset;
    while cursor < end {
        let request =
            u32::try_from((end - cursor).min(u64::from(page_cap))).expect("bounded by u32");
        let page = attachments
            .read(budget, index, cursor, request)
            .map_err(source_error)?;
        if page.is_empty() {
            return Err(Error::Internal(
                "attachment returned an empty page before EOF".to_owned(),
            ));
        }
        if page.len() > usize::try_from(request).expect("u32 fits usize") {
            return Err(Error::Internal(
                "attachment returned more bytes than requested".to_owned(),
            ));
        }
        cursor += u64::try_from(page.len()).expect("usize fits u64");
        output.extend_from_slice(&page);
    }
    Ok(output)
}

/// File facts that can affect parsing or rendering.
///
/// The runtime has already selected this plugin and generation, so those
/// internal selection facts are deliberately not part of the author surface.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileInfo {
    pub path: Option<String>,
    pub media_type: Option<String>,
}

/// Input for the initial byte-to-entity transition.
#[derive(Debug)]
pub struct OpenFile<'a> {
    pub file: FileInfo,
    pub source: Source<'a>,
    pub ids: IdNamespace,
}

/// Input for a cold entity-to-byte transition.
#[derive(Debug)]
pub struct OpenEntities<'a> {
    pub file: FileInfo,
    pub entities: EntityReader<'a>,
    /// Optional host-verified materialization for the same semantic root.
    /// Returned edits are relative to this checkpoint when present, and to an
    /// empty file when absent. Read it only when the format can use it as a
    /// restore/render checkpoint.
    pub accepted: Option<Source<'a>>,
}

/// Input for a warm byte-to-entity transition.
#[derive(Debug)]
pub struct FileUpdate<'a> {
    /// Semantic file facts before and after this transition. A rename-only
    /// update has an empty splice list but still changes these values.
    pub before: FileInfo,
    pub after: FileInfo,
    /// Lazy accepted bytes for a format that needs local lexical context.
    /// Ordinary hot paths should use [`Self::edits`] instead.
    pub before_source: Source<'a>,
    /// Lazy complete-result bytes. Use [`Self::read_insert`] for an individual
    /// large replacement rather than materializing this source by default.
    pub after_source: Source<'a>,
    pub edits: Vec<InputSplice>,
    pub ids: IdNamespace,
}

impl FileUpdate<'_> {
    /// Reads the inserted bytes for one member of [`Self::edits`].
    pub fn read_insert(&self, splice: &InputSplice) -> Result<Vec<u8>> {
        match &splice.insert {
            InputInsert::Inline(bytes) => Ok(bytes.clone()),
            InputInsert::AfterRange { offset, length } => self.after_source.read(*offset, *length),
        }
    }
}

/// Input for a warm entity-to-byte transition.
#[derive(Debug)]
pub struct EntityUpdate<'a> {
    /// Semantic file facts before and after this transition.
    pub before: FileInfo,
    pub after: FileInfo,
    /// Lazy accepted render bytes for a format that needs local lexical
    /// context while applying semantic changes.
    pub before_source: Source<'a>,
    pub changes: EntityChangeReader<'a>,
}

/// A lazy, potentially fallible stream of complete semantic changes.
pub type Changes = Box<dyn Iterator<Item = Result<EntityChange>>>;

/// Turns ordinary semantic changes into a lazy output stream without buffering
/// a whole file. This is the common authoring path.
pub fn changes<I>(iterator: I) -> Changes
where
    I: Iterator<Item = EntityChange> + 'static,
{
    Box::new(iterator.map(Ok))
}

/// Turns a fallible lazy semantic stream into an output stream. Use this only
/// when producing a later record can genuinely fail.
pub fn try_changes<I>(iterator: I) -> Changes
where
    I: Iterator<Item = Result<EntityChange>> + 'static,
{
    Box::new(iterator)
}

/// A lazy, potentially fallible stream of sparse renderer edits.
pub type Edits = Box<dyn Iterator<Item = Result<ByteEdit>>>;

/// Turns ordinary sparse renderer edits into a lazy output stream. This is the
/// common authoring path.
pub fn edits<I>(iterator: I) -> Edits
where
    I: Iterator<Item = ByteEdit> + 'static,
{
    Box::new(iterator.map(Ok))
}

/// Turns a fallible lazy renderer stream into an output stream.
pub fn try_edits<I>(iterator: I) -> Edits
where
    I: Iterator<Item = Result<ByteEdit>> + 'static,
{
    Box::new(iterator)
}

/// The complete authoring contract. The API runtime owns all Component/WIT plumbing;
/// an author only supplies parser/state behavior and chooses when a source is
/// read. Documents are immutable so `fork` is inexpensive and obvious.
pub trait FormatPlugin: 'static {
    type Document: Clone + 'static;

    /// Resolves one colliding semantic entity without hydrating a document.
    ///
    /// The default is deterministic canonical `b`, or deletion when `b` is a
    /// tombstone. Formats override this only when they have a safe
    /// format-specific composition rule, such as independent CSV cells or
    /// disjoint Markdown text spans.
    fn resolve_conflict(conflict: EntityConflict<'_>) -> Result<ConflictResolution> {
        Ok(conflict.take_b_or_delete())
    }

    fn open_file(input: OpenFile<'_>) -> Result<(Self::Document, Changes)>;

    fn open_entities(input: OpenEntities<'_>) -> Result<(Self::Document, Edits)>;

    fn file_changed(
        document: &Self::Document,
        update: FileUpdate<'_>,
    ) -> Result<(Self::Document, Changes)>;

    fn entities_changed(
        document: &Self::Document,
        update: EntityUpdate<'_>,
    ) -> Result<(Self::Document, Edits)>;
}

/// The generated Component-v2 implementation for a [`FormatPlugin`].
#[doc(hidden)]
#[derive(Debug)]
pub struct Component<P>(PhantomData<P>);

/// Exports one [`FormatPlugin`] as a Lix Component v2 plugin.
///
/// An ordinary plugin needs only this macro after its `FormatPlugin`
/// implementation; it never names the generated Component wrapper.
#[macro_export]
macro_rules! export_v2 {
    ($plugin:ty) => {
        type __LixPluginApiComponent = $crate::Component<$plugin>;
        $crate::__export_component_v2!(__LixPluginApiComponent);
    };
}

#[doc(hidden)]
pub struct AuthorDocument<P: FormatPlugin>(P::Document);

impl<P: FormatPlugin> std::fmt::Debug for AuthorDocument<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_tuple("AuthorDocument").finish()
    }
}

#[doc(hidden)]
pub struct AuthorChangeCursor {
    state: RefCell<ChangeCursorState>,
}

struct ChangeCursorState {
    changes: Changes,
    pending: Option<EntityChange>,
    eof: bool,
}

impl std::fmt::Debug for AuthorChangeCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorChangeCursor")
            .finish_non_exhaustive()
    }
}

#[doc(hidden)]
pub struct AuthorEditCursor {
    state: RefCell<EditCursorState>,
}

struct EditCursorState {
    edits: Edits,
    pending: Option<ByteEdit>,
    eof: bool,
}

impl std::fmt::Debug for AuthorEditCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorEditCursor")
            .finish_non_exhaustive()
    }
}

#[doc(hidden)]
pub struct AuthorResolutionCursor<P: FormatPlugin> {
    state: RefCell<ResolutionCursorState>,
    plugin: PhantomData<P>,
}

struct ResolutionCursorState {
    source: PacketSource,
    pending: VecDeque<(u32, ConflictResolution)>,
    source_eof: bool,
    eof: bool,
}

impl<P: FormatPlugin> std::fmt::Debug for AuthorResolutionCursor<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorResolutionCursor")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
#[doc(hidden)]
pub struct AuthorByteOutputs {
    values: Vec<Arc<Vec<u8>>>,
}

fn file_transition<P: FormatPlugin>(document: P::Document, changes: Changes) -> FileTransition {
    FileTransition {
        document: Document::new(AuthorDocument::<P>(document)),
        changes: ChangeCursor::new(AuthorChangeCursor {
            state: RefCell::new(ChangeCursorState {
                changes,
                pending: None,
                eof: false,
            }),
        }),
    }
}

fn entity_transition<P: FormatPlugin>(document: P::Document, edits: Edits) -> EntityTransition {
    EntityTransition {
        document: Document::new(AuthorDocument::<P>(document)),
        edits: EditCursor::new(AuthorEditCursor {
            state: RefCell::new(EditCursorState {
                edits,
                pending: None,
                eof: false,
            }),
        }),
    }
}

impl<P: FormatPlugin> Guest for Component<P> {
    type ByteOutputs = AuthorByteOutputs;
    type ChangeCursor = AuthorChangeCursor;
    type EditCursor = AuthorEditCursor;
    type Document = AuthorDocument<P>;
    type ResolutionCursor = AuthorResolutionCursor<P>;

    fn open_file(
        budget: &TransitionBudget,
        input: OpenFileInput,
    ) -> std::result::Result<FileTransition, PluginError> {
        let author_input = OpenFile {
            file: file_info(&input.descriptor),
            source: Source::new(&input.file, budget),
            ids: IdNamespace {
                high: input.ids.high,
                low: input.ids.low,
            },
        };
        P::open_file(author_input)
            .map(|(document, changes)| file_transition::<P>(document, changes))
            .map_err(plugin_error)
    }

    fn open_entities(
        budget: &TransitionBudget,
        input: OpenEntitiesInput,
    ) -> std::result::Result<EntityTransition, PluginError> {
        let author_input = OpenEntities {
            file: file_info(&input.descriptor),
            entities: EntityReader::new(&input.entities, budget),
            accepted: input
                .accepted
                .as_ref()
                .map(|accepted| Source::new(accepted, budget)),
        };
        P::open_entities(author_input)
            .map(|(document, edits)| entity_transition::<P>(document, edits))
            .map_err(plugin_error)
    }

    fn resolve_conflicts(
        _budget: &TransitionBudget,
        input: WitConflictUpdate,
    ) -> std::result::Result<ResolutionCursor, PluginError> {
        Ok(ResolutionCursor::new(AuthorResolutionCursor::<P> {
            state: RefCell::new(ResolutionCursorState {
                source: input.conflicts,
                pending: VecDeque::new(),
                source_eof: false,
                eof: false,
            }),
            plugin: PhantomData,
        }))
    }
}

impl<P: FormatPlugin> GuestDocument for AuthorDocument<P> {
    fn fork(&self) -> Document {
        Document::new(Self(self.0.clone()))
    }

    fn file_changed(
        &self,
        budget: &TransitionBudget,
        update: WitFileUpdate,
    ) -> std::result::Result<FileTransition, PluginError> {
        let edits = update
            .edits
            .into_iter()
            .map(|edit| InputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: match edit.insert {
                    InputBytes::Inline(bytes) => InputInsert::Inline(bytes),
                    InputBytes::AfterRange(range) => InputInsert::AfterRange {
                        offset: range.offset,
                        length: range.length,
                    },
                },
            })
            .collect();
        let author_update = FileUpdate {
            before: file_info(&update.before_descriptor),
            after: file_info(&update.after_descriptor),
            before_source: Source::new(&update.before, budget),
            after_source: Source::new(&update.after, budget),
            edits,
            ids: IdNamespace {
                high: update.ids.high,
                low: update.ids.low,
            },
        };
        P::file_changed(&self.0, author_update)
            .map(|(document, changes)| file_transition::<P>(document, changes))
            .map_err(plugin_error)
    }

    fn entities_changed(
        &self,
        budget: &TransitionBudget,
        update: WitEntityUpdate,
    ) -> std::result::Result<EntityTransition, PluginError> {
        let author_update = EntityUpdate {
            before: file_info(&update.before_descriptor),
            after: file_info(&update.after_descriptor),
            before_source: Source::new(&update.before, budget),
            changes: EntityChangeReader::new(&update.changes, budget),
        };
        P::entities_changed(&self.0, author_update)
            .map(|(document, edits)| entity_transition::<P>(document, edits))
            .map_err(plugin_error)
    }
}

impl GuestChangeCursor for AuthorChangeCursor {
    fn next(
        &self,
        budget: &TransitionBudget,
        max_bytes: u32,
    ) -> std::result::Result<Option<ChangePage>, PluginError> {
        let max_record_bytes = budget.limits().max_record_bytes;
        let mut state = self.state.borrow_mut();
        if state.eof {
            return Ok(None);
        }
        let page = {
            let ChangeCursorState {
                changes, pending, ..
            } = &mut *state;
            next_change_page(changes, pending, max_bytes, max_record_bytes)
        }
        .map_err(plugin_error)?;
        let Some(page) = page else {
            state.eof = true;
            return Ok(None);
        };
        let attachments = if page.attachments.is_empty() {
            None
        } else {
            Some(ByteOutputs::new(AuthorByteOutputs {
                values: page.attachments,
            }))
        };
        Ok(Some(ChangePage {
            format_version: PACKET_FORMAT_VERSION,
            record_count: page.record_count,
            payload: page.payload,
            attachments,
        }))
    }
}

impl<P: FormatPlugin> GuestResolutionCursor for AuthorResolutionCursor<P> {
    fn next(
        &self,
        budget: &TransitionBudget,
        max_bytes: u32,
    ) -> std::result::Result<Option<ResolutionPage>, PluginError> {
        let max_record_bytes = budget.limits().max_record_bytes;
        let mut state = self.state.borrow_mut();
        if state.eof {
            return Ok(None);
        }

        while state.pending.is_empty() && !state.source_eof {
            let input_max_bytes = budget.limits().max_page_bytes.max(1);
            let Some(page) = state
                .source
                .next(budget, input_max_bytes)
                .map_err(|error| plugin_error(source_error(error)))?
            else {
                state.source_eof = true;
                break;
            };
            if page.format_version != PACKET_FORMAT_VERSION {
                return Err(plugin_error(Error::invalid_input(format!(
                    "unsupported packet version {}",
                    page.format_version
                ))));
            }
            let conflicts = decode_conflict_page(&page.payload, page.record_count)
                .map_err(|error| plugin_error(Error::invalid_input(error)))?;
            for conflict in conflicts {
                let author_conflict = EntityConflict {
                    schema_key: conflict.schema_key,
                    entity_pk: conflict.entity_pk,
                    base: conflict
                        .base
                        .as_ref()
                        .map(|value| conflict_value(value, page.attachments.as_ref(), budget)),
                    a: conflict
                        .a
                        .as_ref()
                        .map(|value| conflict_value(value, page.attachments.as_ref(), budget)),
                    b: conflict
                        .b
                        .as_ref()
                        .map(|value| conflict_value(value, page.attachments.as_ref(), budget)),
                };
                let resolution = P::resolve_conflict(author_conflict).map_err(plugin_error)?;
                state.pending.push_back((conflict.ordinal, resolution));
            }
        }

        if state.pending.is_empty() {
            state.eof = true;
            return Ok(None);
        }

        let page = next_resolution_page(&mut state.pending, max_bytes, max_record_bytes)
            .map_err(plugin_error)?;
        let Some(page) = page else {
            state.eof = true;
            return Ok(None);
        };
        let attachments = if page.attachments.is_empty() {
            None
        } else {
            Some(ByteOutputs::new(AuthorByteOutputs {
                values: page.attachments,
            }))
        };
        Ok(Some(ResolutionPage {
            format_version: PACKET_FORMAT_VERSION,
            record_count: page.record_count,
            payload: page.payload,
            attachments,
        }))
    }
}

impl GuestEditCursor for AuthorEditCursor {
    fn next(
        &self,
        budget: &TransitionBudget,
        max_edits: u32,
        max_inline_bytes: u32,
    ) -> std::result::Result<Option<EditPage>, PluginError> {
        let mut state = self.state.borrow_mut();
        if state.eof {
            return Ok(None);
        }
        let page = {
            let EditCursorState { edits, pending, .. } = &mut *state;
            next_edit_page(edits, pending, budget, max_edits, max_inline_bytes)
        }
        .map_err(plugin_error)?;
        let Some(page) = page else {
            state.eof = true;
            return Ok(None);
        };
        let outputs = if page.outputs.is_empty() {
            None
        } else {
            Some(ByteOutputs::new(AuthorByteOutputs {
                values: page.outputs,
            }))
        };
        Ok(Some(EditPage {
            edits: page.edits,
            outputs,
        }))
    }
}

impl GuestByteOutputs for AuthorByteOutputs {
    fn len(&self, index: u32) -> std::result::Result<u64, PluginError> {
        self.values
            .get(usize::try_from(index).expect("u32 fits usize"))
            .map(|value| u64::try_from(value.len()).expect("usize fits u64"))
            .ok_or_else(|| PluginError::InvalidInput("invalid byte-output index".to_owned()))
    }

    fn read(
        &self,
        _budget: &TransitionBudget,
        index: u32,
        offset: u64,
        length: u32,
    ) -> std::result::Result<Vec<u8>, PluginError> {
        let value = self
            .values
            .get(usize::try_from(index).expect("u32 fits usize"))
            .ok_or_else(|| PluginError::InvalidInput("invalid byte-output index".to_owned()))?;
        let start = usize::try_from(offset)
            .map_err(|_| PluginError::InvalidInput("output offset overflow".to_owned()))?;
        let end = start
            .checked_add(usize::try_from(length).expect("u32 fits usize"))
            .ok_or_else(|| PluginError::InvalidInput("output range overflow".to_owned()))?;
        value
            .get(start..end)
            .map(ToOwned::to_owned)
            .ok_or_else(|| PluginError::InvalidInput("output range exceeds value".to_owned()))
    }
}

struct EncodedChangePage {
    record_count: u32,
    payload: Vec<u8>,
    attachments: Vec<Arc<Vec<u8>>>,
}

fn next_change_page(
    changes: &mut Changes,
    pending: &mut Option<EntityChange>,
    max_bytes: u32,
    max_record_bytes: u32,
) -> Result<Option<EncodedChangePage>> {
    if max_bytes == 0 {
        return Err(Error::LimitExceeded(
            "change cursor max-bytes must be positive".to_owned(),
        ));
    }
    if max_record_bytes == 0 {
        return Err(Error::LimitExceeded(
            "change cursor max-record-bytes must be positive".to_owned(),
        ));
    }
    let limit = usize::try_from(max_bytes).expect("u32 fits usize");
    let record_limit = usize::try_from(max_record_bytes).expect("u32 fits usize");
    let mut payload = Vec::with_capacity(limit.min(64 * 1024));
    let mut attachments = Vec::new();
    let mut record_count = 0u32;
    loop {
        let change = match pending.take() {
            Some(change) => Some(change),
            None => changes.next().transpose()?,
        };
        let Some(mut change) = change else {
            break;
        };
        let inline_record_len = encoded_change_len(&change, None)?;
        let inline_framed_len = inline_record_len
            .checked_add(4)
            .ok_or_else(|| Error::internal("change record length overflow"))?;
        let inline_fits = inline_record_len <= record_limit && inline_framed_len <= limit;
        let (record_len, attach_snapshot) = if inline_fits {
            if payload
                .len()
                .checked_add(inline_framed_len)
                .is_none_or(|next_len| next_len > limit)
            {
                *pending = Some(change);
                break;
            }
            (inline_record_len, false)
        } else if change.snapshot.is_some() {
            let attachment_index = u32::try_from(attachments.len()).map_err(|_| {
                Error::LimitExceeded("change page has too many attachments".to_owned())
            })?;
            let record_len = encoded_change_len(&change, Some(attachment_index))?;
            let framed_len = record_len
                .checked_add(4)
                .ok_or_else(|| Error::internal("change record length overflow"))?;
            if record_len > record_limit {
                return Err(Error::RecordTooLarge(
                    u64::try_from(record_len).expect("usize fits u64"),
                ));
            }
            if framed_len > limit {
                return Err(Error::RecordTooLarge(
                    u64::try_from(framed_len).expect("usize fits u64"),
                ));
            }
            if payload
                .len()
                .checked_add(framed_len)
                .is_none_or(|next_len| next_len > limit)
            {
                *pending = Some(change);
                break;
            }
            (record_len, true)
        } else {
            return Err(Error::RecordTooLarge(
                u64::try_from(inline_record_len).expect("usize fits u64"),
            ));
        };
        put_u32(
            &mut payload,
            u32::try_from(record_len)
                .map_err(|_| Error::LimitExceeded("change record exceeds 4GiB".to_owned()))?,
        );
        let record_start = payload.len();
        encode_change_into(
            &mut payload,
            &change,
            attach_snapshot.then(|| u32::try_from(attachments.len()).expect("checked above")),
        )?;
        debug_assert_eq!(payload.len() - record_start, record_len);
        if attach_snapshot {
            attachments.push(Arc::new(
                change
                    .snapshot
                    .take()
                    .expect("attached change must have a snapshot"),
            ));
        }
        record_count += 1;
    }
    if record_count == 0 {
        Ok(None)
    } else {
        Ok(Some(EncodedChangePage {
            record_count,
            payload,
            attachments,
        }))
    }
}

struct EncodedResolutionPage {
    record_count: u32,
    payload: Vec<u8>,
    attachments: Vec<Arc<Vec<u8>>>,
}

fn next_resolution_page(
    resolutions: &mut VecDeque<(u32, ConflictResolution)>,
    max_bytes: u32,
    max_record_bytes: u32,
) -> Result<Option<EncodedResolutionPage>> {
    if max_bytes == 0 {
        return Err(Error::LimitExceeded(
            "resolution cursor max-bytes must be positive".to_owned(),
        ));
    }
    if max_record_bytes == 0 {
        return Err(Error::LimitExceeded(
            "resolution cursor max-record-bytes must be positive".to_owned(),
        ));
    }
    let limit = usize::try_from(max_bytes).expect("u32 fits usize");
    let record_limit = usize::try_from(max_record_bytes).expect("u32 fits usize");
    let mut payload = Vec::with_capacity(limit.min(64 * 1024));
    let mut attachments = Vec::new();
    let mut record_count = 0u32;

    while let Some((ordinal, resolution)) = resolutions.front() {
        let snapshot = match resolution {
            ConflictResolution::Replace(snapshot) => Some(snapshot),
            ConflictResolution::TakeBase
            | ConflictResolution::TakeA
            | ConflictResolution::TakeB
            | ConflictResolution::Delete => None,
        };
        let needs_attachment = snapshot.is_some_and(|snapshot| {
            snapshot.len().checked_add(11).is_none_or(|record_len| {
                record_len > record_limit
                    || record_len
                        .checked_add(4)
                        .is_none_or(|framed_len| framed_len > limit)
            })
        });
        let inline_record = if needs_attachment {
            None
        } else {
            Some(encode_resolution(*ordinal, resolution, None)?)
        };
        let inline_framed_len = inline_record
            .as_ref()
            .map(|record| {
                record
                    .len()
                    .checked_add(4)
                    .ok_or_else(|| Error::internal("resolution record length overflow"))
            })
            .transpose()?;
        let inline_fits =
            inline_record
                .as_ref()
                .zip(inline_framed_len)
                .is_some_and(|(record, framed_len)| {
                    record.len() <= record_limit && framed_len <= limit
                });

        let (record, attach_snapshot) = if inline_fits {
            let record = inline_record.expect("checked inline resolution");
            let framed_len = inline_framed_len.expect("checked inline resolution frame");
            if payload
                .len()
                .checked_add(framed_len)
                .is_none_or(|next_len| next_len > limit)
            {
                break;
            }
            (record, false)
        } else if snapshot.is_some() {
            let attachment_index = u32::try_from(attachments.len()).map_err(|_| {
                Error::LimitExceeded("resolution page has too many attachments".to_owned())
            })?;
            let record = encode_resolution(*ordinal, resolution, Some(attachment_index))?;
            let framed_len = record
                .len()
                .checked_add(4)
                .ok_or_else(|| Error::internal("resolution record length overflow"))?;
            if record.len() > record_limit {
                return Err(Error::RecordTooLarge(
                    u64::try_from(record.len()).expect("usize fits u64"),
                ));
            }
            if framed_len > limit {
                return Err(Error::RecordTooLarge(
                    u64::try_from(framed_len).expect("usize fits u64"),
                ));
            }
            if payload
                .len()
                .checked_add(framed_len)
                .is_none_or(|next_len| next_len > limit)
            {
                break;
            }
            (record, true)
        } else {
            let required = inline_framed_len
                .or_else(|| inline_record.as_ref().map(|record| record.len() + 4))
                .expect("snapshot-free resolution has an inline encoding");
            return Err(Error::RecordTooLarge(
                u64::try_from(required).expect("usize fits u64"),
            ));
        };

        put_u32(
            &mut payload,
            u32::try_from(record.len())
                .map_err(|_| Error::LimitExceeded("resolution record exceeds 4GiB".to_owned()))?,
        );
        payload.extend_from_slice(&record);
        let (_, resolution) = resolutions
            .pop_front()
            .expect("front resolution was checked before selection");
        if attach_snapshot {
            let ConflictResolution::Replace(snapshot) = resolution else {
                unreachable!("only replacement resolutions use attachments");
            };
            attachments.push(Arc::new(snapshot));
        }
        record_count = record_count
            .checked_add(1)
            .ok_or_else(|| Error::internal("resolution record count overflow"))?;
    }

    if record_count == 0 {
        Ok(None)
    } else {
        Ok(Some(EncodedResolutionPage {
            record_count,
            payload,
            attachments,
        }))
    }
}

struct EncodedEditPage {
    edits: Vec<OutputSplice>,
    outputs: Vec<Arc<Vec<u8>>>,
}

fn next_edit_page(
    edits: &mut Edits,
    pending: &mut Option<ByteEdit>,
    budget: &TransitionBudget,
    max_edits: u32,
    max_inline_bytes: u32,
) -> Result<Option<EncodedEditPage>> {
    if max_edits == 0 {
        return Err(Error::LimitExceeded(
            "edit cursor max-edits must be positive".to_owned(),
        ));
    }
    let limits = budget.limits();
    let record_limit = usize::try_from(limits.max_record_bytes).expect("u32 fits usize");
    let page_limit = usize::try_from(limits.max_page_bytes).expect("u32 fits usize");
    if record_limit < EDIT_METADATA_BYTES || page_limit < EDIT_METADATA_BYTES {
        return Err(Error::RecordTooLarge(EDIT_METADATA_BYTES as u64));
    }
    let inline_limit = usize::try_from(max_inline_bytes).expect("u32 fits usize");
    let mut page = Vec::new();
    let mut outputs = Vec::new();
    let mut inline_used = 0usize;
    let mut page_used = 0usize;
    for _ in 0..max_edits {
        let edit = match pending.take() {
            Some(edit) => Some(edit),
            None => edits.next().transpose()?,
        };
        let Some(edit) = edit else {
            break;
        };
        if page_used + EDIT_METADATA_BYTES > page_limit {
            *pending = Some(edit);
            break;
        }
        let inline_record_len = EDIT_METADATA_BYTES
            .checked_add(edit.insert.len())
            .ok_or_else(|| Error::LimitExceeded("edit record length overflow".to_owned()))?;
        let next_inline_used = inline_used
            .checked_add(edit.insert.len())
            .ok_or_else(|| Error::LimitExceeded("edit inline-byte counter overflow".to_owned()))?;
        let inline = next_inline_used <= inline_limit
            && inline_record_len <= record_limit
            && page_used + inline_record_len <= page_limit;
        let insert = if inline {
            inline_used = next_inline_used;
            page_used += inline_record_len;
            OutputBytes::Inline(edit.insert.as_ref().clone())
        } else {
            page_used += EDIT_METADATA_BYTES;
            let index = u32::try_from(outputs.len())
                .map_err(|_| Error::LimitExceeded("too many edit outputs".to_owned()))?;
            let length = u64::try_from(edit.insert.len()).expect("usize fits u64");
            outputs.push(edit.insert);
            OutputBytes::Output(OutputRange {
                index,
                offset: 0,
                length,
            })
        };
        page.push(OutputSplice {
            offset: edit.offset,
            delete_len: edit.delete_len,
            insert,
        });
    }
    if page.is_empty() {
        Ok(None)
    } else {
        Ok(Some(EncodedEditPage {
            edits: page,
            outputs,
        }))
    }
}

fn encoded_change_len(change: &EntityChange, attachment_index: Option<u32>) -> Result<usize> {
    let key_len = encoded_key_len(&change.schema_key, &change.entity_pk)?;
    let mut len = 1usize
        .checked_add(key_len)
        .ok_or_else(|| Error::LimitExceeded("change record length overflow".to_owned()))?;
    if let Some(snapshot) = &change.snapshot {
        let snapshot_len = if attachment_index.is_some() {
            1 + 4 + 8 + 8
        } else {
            let _ = u32::try_from(snapshot.len())
                .map_err(|_| Error::LimitExceeded("snapshot exceeds 4GiB".to_owned()))?;
            1usize
                .checked_add(4)
                .and_then(|len| len.checked_add(snapshot.len()))
                .ok_or_else(|| Error::LimitExceeded("change record length overflow".to_owned()))?
        };
        len = len
            .checked_add(1)
            .and_then(|len| len.checked_add(snapshot_len))
            .ok_or_else(|| Error::LimitExceeded("change record length overflow".to_owned()))?;
    }
    Ok(len)
}

fn encoded_key_len(schema_key: &str, pk: &[String]) -> Result<usize> {
    let _ = u32::try_from(pk.len()).map_err(|_| {
        Error::LimitExceeded("entity primary key has too many components".to_owned())
    })?;
    let mut len = encoded_text_len(schema_key)?
        .checked_add(4)
        .ok_or_else(|| Error::LimitExceeded("entity key length overflow".to_owned()))?;
    for component in pk {
        len = len
            .checked_add(encoded_text_len(component)?)
            .ok_or_else(|| Error::LimitExceeded("entity key length overflow".to_owned()))?;
    }
    Ok(len)
}

fn encoded_text_len(value: &str) -> Result<usize> {
    let _ = u32::try_from(value.len())
        .map_err(|_| Error::LimitExceeded("packet text exceeds 4GiB".to_owned()))?;
    value
        .len()
        .checked_add(4)
        .ok_or_else(|| Error::LimitExceeded("packet text length overflow".to_owned()))
}

fn encode_change_into(
    output: &mut Vec<u8>,
    change: &EntityChange,
    attachment_index: Option<u32>,
) -> Result<()> {
    output.push(u8::from(change.snapshot.is_none()));
    encode_key(output, &change.schema_key, &change.entity_pk)?;
    if let Some(snapshot) = &change.snapshot {
        output.push(match change.effect {
            ChangeEffect::Content => 0,
            ChangeEffect::FormatOnly => 1,
        });
        match attachment_index {
            Some(index) => {
                output.push(1);
                put_u32(output, index);
                output.extend_from_slice(&0_u64.to_le_bytes());
                output.extend_from_slice(
                    &u64::try_from(snapshot.len())
                        .expect("usize fits u64")
                        .to_le_bytes(),
                );
            }
            None => {
                output.push(0);
                put_u32(
                    output,
                    u32::try_from(snapshot.len())
                        .map_err(|_| Error::LimitExceeded("snapshot exceeds 4GiB".to_owned()))?,
                );
                output.extend_from_slice(snapshot);
            }
        }
    }
    Ok(())
}

fn encode_resolution(
    ordinal: u32,
    resolution: &ConflictResolution,
    attachment_index: Option<u32>,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    match resolution {
        ConflictResolution::TakeBase => {
            output.push(0);
            put_u32(&mut output, ordinal);
            output.push(0);
        }
        ConflictResolution::TakeA => {
            output.push(0);
            put_u32(&mut output, ordinal);
            output.push(1);
        }
        ConflictResolution::TakeB => {
            output.push(0);
            put_u32(&mut output, ordinal);
            output.push(2);
        }
        ConflictResolution::Replace(snapshot) => {
            output.push(1);
            put_u32(&mut output, ordinal);
            // Composed replacements are semantic content changes.
            output.push(0);
            match attachment_index {
                Some(index) => {
                    output.push(1);
                    put_u32(&mut output, index);
                    output.extend_from_slice(&0_u64.to_le_bytes());
                    output.extend_from_slice(
                        &u64::try_from(snapshot.len())
                            .expect("usize fits u64")
                            .to_le_bytes(),
                    );
                }
                None => {
                    output.push(0);
                    put_u32(
                        &mut output,
                        u32::try_from(snapshot.len()).map_err(|_| {
                            Error::LimitExceeded("snapshot exceeds 4GiB".to_owned())
                        })?,
                    );
                    output.extend_from_slice(snapshot);
                }
            }
        }
        ConflictResolution::Delete => {
            output.push(2);
            put_u32(&mut output, ordinal);
        }
    }
    Ok(output)
}

fn encode_key(output: &mut Vec<u8>, schema_key: &str, pk: &[String]) -> Result<()> {
    put_text(output, schema_key)?;
    put_u32(
        output,
        u32::try_from(pk.len()).map_err(|_| {
            Error::LimitExceeded("entity primary key has too many components".to_owned())
        })?,
    );
    for component in pk {
        put_text(output, component)?;
    }
    Ok(())
}

fn put_text(output: &mut Vec<u8>, value: &str) -> Result<()> {
    put_u32(
        output,
        u32::try_from(value.len())
            .map_err(|_| Error::LimitExceeded("text value exceeds 4GiB".to_owned()))?,
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EncodedConflictValue {
    Inline(Vec<u8>),
    Attachment {
        index: u32,
        offset: u64,
        length: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EncodedConflict {
    schema_key: String,
    entity_pk: Vec<String>,
    ordinal: u32,
    base: Option<EncodedConflictValue>,
    a: Option<EncodedConflictValue>,
    b: Option<EncodedConflictValue>,
}

struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn take(&mut self, len: usize) -> std::result::Result<&'a [u8], String> {
        let end = self
            .cursor
            .checked_add(len)
            .ok_or_else(|| "packet length overflow".to_owned())?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or_else(|| "truncated packet".to_owned())?;
        self.cursor = end;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.cursor)
    }

    fn u8(&mut self) -> std::result::Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> std::result::Result<u32, String> {
        let bytes: [u8; 4] = self.take(4)?.try_into().expect("four bytes");
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> std::result::Result<u64, String> {
        let bytes: [u8; 8] = self.take(8)?.try_into().expect("eight bytes");
        Ok(u64::from_le_bytes(bytes))
    }

    fn text(&mut self) -> std::result::Result<String, String> {
        let len = usize::try_from(self.u32()?).expect("u32 fits usize");
        std::str::from_utf8(self.take(len)?)
            .map(ToOwned::to_owned)
            .map_err(|error| format!("packet text is not UTF-8: {error}"))
    }

    fn key(&mut self) -> std::result::Result<(String, Vec<String>), String> {
        let schema_key = self.text()?;
        let count = usize::try_from(self.u32()?).expect("u32 fits usize");
        if count > self.remaining() / 4 {
            return Err("entity primary-key component count exceeds packet bounds".to_owned());
        }
        let mut entity_pk = Vec::with_capacity(count);
        for _ in 0..count {
            entity_pk.push(self.text()?);
        }
        Ok((schema_key, entity_pk))
    }

    fn blob(
        &mut self,
        attachment: &mut impl FnMut(u32, u64, u64) -> std::result::Result<Vec<u8>, String>,
    ) -> std::result::Result<Vec<u8>, String> {
        match self.u8()? {
            0 => {
                let len = usize::try_from(self.u32()?).expect("u32 fits usize");
                Ok(self.take(len)?.to_vec())
            }
            1 => {
                let index = self.u32()?;
                let offset = self.u64()?;
                let length = self.u64()?;
                attachment(index, offset, length)
            }
            tag => Err(format!("unknown packet blob-ref tag {tag}")),
        }
    }

    fn conflict_value(&mut self) -> std::result::Result<EncodedConflictValue, String> {
        match self.u8()? {
            0 => {
                let len = usize::try_from(self.u32()?).expect("u32 fits usize");
                Ok(EncodedConflictValue::Inline(self.take(len)?.to_vec()))
            }
            1 => Ok(EncodedConflictValue::Attachment {
                index: self.u32()?,
                offset: self.u64()?,
                length: self.u64()?,
            }),
            tag => Err(format!("unknown packet blob-ref tag {tag}")),
        }
    }

    fn conflict_state(&mut self) -> std::result::Result<Option<EncodedConflictValue>, String> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.conflict_value().map(Some),
            tag => Err(format!("unknown packet conflict-state tag {tag}")),
        }
    }

    fn finish(self) -> std::result::Result<(), String> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err("packet record has trailing bytes".to_owned())
        }
    }
}

fn framed_records(payload: &[u8], count: u32) -> std::result::Result<Vec<&[u8]>, String> {
    if count == 0 {
        return Err("packet page must contain at least one record".to_owned());
    }
    let count = usize::try_from(count).expect("u32 fits usize");
    if count > payload.len() / 4 {
        return Err("packet record count exceeds payload bounds".to_owned());
    }
    let mut decoder = Decoder::new(payload);
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let len = usize::try_from(decoder.u32()?).expect("u32 fits usize");
        records.push(decoder.take(len)?);
    }
    decoder.finish()?;
    Ok(records)
}

fn decode_entity_page(
    payload: &[u8],
    count: u32,
    mut attachment: impl FnMut(u32, u64, u64) -> std::result::Result<Vec<u8>, String>,
) -> std::result::Result<Vec<EntityRecord>, String> {
    let records = framed_records(payload, count)?;
    let mut output = Vec::with_capacity(records.len());
    for record in records {
        let mut decoder = Decoder::new(record);
        let (schema_key, entity_pk) = decoder.key()?;
        let snapshot = decoder.blob(&mut attachment)?;
        decoder.finish()?;
        output.push(EntityRecord {
            schema_key,
            entity_pk,
            snapshot,
        });
    }
    Ok(output)
}

fn decode_change_page(
    payload: &[u8],
    count: u32,
    mut attachment: impl FnMut(u32, u64, u64) -> std::result::Result<Vec<u8>, String>,
) -> std::result::Result<Vec<EntityChange>, String> {
    let records = framed_records(payload, count)?;
    let mut output = Vec::with_capacity(records.len());
    for record in records {
        let mut decoder = Decoder::new(record);
        let tag = decoder.u8()?;
        let (schema_key, entity_pk) = decoder.key()?;
        let (snapshot, effect) = match tag {
            0 => {
                let effect = match decoder.u8()? {
                    0 => ChangeEffect::Content,
                    1 => ChangeEffect::FormatOnly,
                    value => return Err(format!("unknown packet effect tag {value}")),
                };
                (Some(decoder.blob(&mut attachment)?), effect)
            }
            1 => (None, ChangeEffect::Content),
            value => return Err(format!("unknown packet change tag {value}")),
        };
        decoder.finish()?;
        output.push(EntityChange {
            schema_key,
            entity_pk,
            snapshot,
            effect,
        });
    }
    Ok(output)
}

fn decode_conflict_page(
    payload: &[u8],
    count: u32,
) -> std::result::Result<Vec<EncodedConflict>, String> {
    let records = framed_records(payload, count)?;
    let mut output = Vec::with_capacity(records.len());
    for record in records {
        let mut decoder = Decoder::new(record);
        let (schema_key, entity_pk) = decoder.key()?;
        let ordinal = decoder.u32()?;
        let base = decoder.conflict_state()?;
        let a = decoder.conflict_state()?;
        let b = decoder.conflict_state()?;
        decoder.finish()?;
        output.push(EncodedConflict {
            schema_key,
            entity_pk,
            ordinal,
            base,
            a,
            b,
        });
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_changes_round_trip_through_one_packet_page() {
        let expected = EntityChange::upsert(
            "test",
            vec!["entity".to_owned()],
            br#"{\"value\":true}"#.to_vec(),
        );
        let mut changes = changes(std::iter::once(expected.clone()));
        let mut pending = None;
        let page = next_change_page(&mut changes, &mut pending, 4096, 4096)
            .unwrap()
            .unwrap();
        let decoded = decode_change_page(&page.payload, page.record_count, |_, _, _| {
            Err("unexpected attachment".to_owned())
        })
        .unwrap();
        assert_eq!(decoded, [expected]);
    }

    #[test]
    fn oversized_snapshots_use_a_page_local_attachment() {
        let expected = EntityChange::upsert(
            "test",
            vec!["large".to_owned()],
            vec![b'x'; 2 * 1024 * 1024],
        );
        let mut changes = changes(std::iter::once(expected.clone()));
        let mut pending = None;
        let page = next_change_page(&mut changes, &mut pending, 4096, 256)
            .unwrap()
            .unwrap();
        assert_eq!(page.attachments.len(), 1);
        let decoded =
            decode_change_page(&page.payload, page.record_count, |index, offset, length| {
                let value = page
                    .attachments
                    .get(usize::try_from(index).expect("u32 fits usize"))
                    .ok_or_else(|| "missing attachment".to_owned())?;
                let start = usize::try_from(offset).map_err(|_| "offset overflow".to_owned())?;
                let end = start
                    .checked_add(usize::try_from(length).map_err(|_| "length overflow".to_owned())?)
                    .ok_or_else(|| "range overflow".to_owned())?;
                value
                    .get(start..end)
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| "attachment range exceeds value".to_owned())
            })
            .unwrap();
        assert_eq!(decoded, [expected]);
    }

    #[test]
    fn impossible_packet_count_is_rejected_before_allocation() {
        let error = decode_entity_page(&[], u32::MAX, |_, _, _| {
            Err("unexpected attachment".to_owned())
        })
        .unwrap_err();
        assert!(
            error.contains("record count exceeds payload bounds"),
            "{error}"
        );
    }

    #[test]
    fn generated_ids_are_canonical_unpadded_base64url() {
        assert_eq!(
            IdNamespace { high: 0, low: 0 }.id(0),
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        );
        assert_eq!(
            IdNamespace {
                high: u64::MAX,
                low: u64::MAX,
            }
            .id(u64::MAX),
            "________________________________"
        );
        assert_eq!(
            IdNamespace {
                high: 0x0011_2233_4455_6677,
                low: 0x8899_aabb_ccdd_eeff,
            }
            .id(0x0102_0304_0506_0708),
            "ABEiM0RVZneImaq7zN3u_wECAwQFBgcI"
        );
    }
}
