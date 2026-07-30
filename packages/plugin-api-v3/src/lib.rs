//! Author-facing hard-cut API for Lix Component v3 plugins.
//!
//! Plugins retain no document object. Every call receives host capabilities
//! for immutable accepted and prospective roots, and returns only a bounded
//! cursor plus the same staged transaction capability.

#![allow(clippy::missing_errors_doc)]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;

wit_bindgen::generate!({
    path: "../rs-sdk/wit/v3",
    world: "plugin",
    pub_export_macro: true,
    export_macro_name: "__export_component_v3",
    default_bindings_module: "lix_plugin_api_v3",
});

pub use exports::lix::plugin::api::{
    ByteEdit, ChangedEntity, ConflictUpdate, CreateContext, EntityUpdate, FileDescriptor,
    FileUpdate, InputBytes, InputSplice, OpenEntitiesInput, OpenFileInput,
};
use exports::lix::plugin::api::{
    ChangeCursor, ConflictChoice as WitConflictChoice,
    ConflictReplacement as WitConflictReplacement, ConflictResolution as WitConflictResolution,
    ConflictTransition, EditCursor, EntityTransition, FileTransition, Guest, GuestChangeCursor,
    GuestEditCursor, GuestResolutionCursor, PluginError, ResolutionCursor,
};
pub use lix::plugin::arena::{Budget, ConflictSet, ConflictSide, Root, Transaction};

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityChange {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    pub snapshot: Option<Vec<u8>>,
    pub format_only: bool,
}

pub fn entity_key(schema_key: &str, entity_pk: &[String]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    append_key_part(&mut output, schema_key.as_bytes())?;
    for part in entity_pk {
        append_key_part(&mut output, part.as_bytes())?;
    }
    Ok(output)
}

pub fn decode_entity_key(key: &[u8]) -> Result<(String, Vec<String>)> {
    let mut offset = 0usize;
    let schema_key = take_key_part(key, &mut offset)?;
    let mut entity_pk = Vec::new();
    while offset < key.len() {
        entity_pk.push(take_key_part(key, &mut offset)?);
    }
    if entity_pk.is_empty() {
        return Err(Error::invalid_input(
            "v3 entity arena key has no primary-key components",
        ));
    }
    Ok((schema_key, entity_pk))
}

fn append_key_part(output: &mut Vec<u8>, part: &[u8]) -> Result<()> {
    let len = u32::try_from(part.len())
        .map_err(|_| Error::invalid_input("v3 entity key component exceeds u32"))?;
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(part);
    Ok(())
}

fn take_key_part(key: &[u8], offset: &mut usize) -> Result<String> {
    let len_end = offset
        .checked_add(4)
        .ok_or_else(|| Error::invalid_input("v3 entity arena key overflowed"))?;
    let len = key
        .get(*offset..len_end)
        .ok_or_else(|| Error::invalid_input("truncated v3 entity arena key length"))?;
    let len = u32::from_le_bytes(len.try_into().expect("length slice is exactly four bytes"));
    let value_end = len_end
        .checked_add(len as usize)
        .ok_or_else(|| Error::invalid_input("v3 entity arena key overflowed"))?;
    let value = key
        .get(len_end..value_end)
        .ok_or_else(|| Error::invalid_input("truncated v3 entity arena key value"))?;
    *offset = value_end;
    String::from_utf8(value.to_vec())
        .map_err(|_| Error::invalid_input("v3 entity arena key is not UTF-8"))
}

pub struct FileResult {
    pub successor: Transaction,
    pub changes: EntityChanges,
}

impl std::fmt::Debug for FileResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileResult")
            .field("successor", &self.successor)
            .field("changes", &self.changes)
            .finish()
    }
}

/// Lazy semantic output owned by the exported WIT cursor.
///
/// In particular, cold imports must not collect every snapshot in guest
/// memory before the host begins draining pages.
pub struct EntityChanges {
    source: EntityChangeStream,
}

pub trait EntityChangeSource {
    fn next(&mut self, budget: &Budget) -> Result<Option<EntityChange>>;
}

pub trait EntityChangePacketSource {
    fn next_packet(&mut self, budget: &Budget, max_bytes: u32) -> Result<Option<Vec<u8>>>;
}

enum EntityChangeStream {
    Changes(Box<dyn EntityChangeSource>),
    Packets(Box<dyn EntityChangePacketSource>),
}

struct IteratorChangeSource<I> {
    iterator: I,
}

impl<I> EntityChangeSource for IteratorChangeSource<I>
where
    I: Iterator<Item = Result<EntityChange>>,
{
    fn next(&mut self, _budget: &Budget) -> Result<Option<EntityChange>> {
        self.iterator.next().transpose()
    }
}

impl EntityChanges {
    pub fn from_results(iterator: impl Iterator<Item = Result<EntityChange>> + 'static) -> Self {
        Self {
            source: EntityChangeStream::Changes(Box::new(IteratorChangeSource { iterator })),
        }
    }

    pub fn from_source(source: impl EntityChangeSource + 'static) -> Self {
        Self {
            source: EntityChangeStream::Changes(Box::new(source)),
        }
    }

    pub fn from_packet_source(source: impl EntityChangePacketSource + 'static) -> Self {
        Self {
            source: EntityChangeStream::Packets(Box::new(source)),
        }
    }
}

impl std::fmt::Debug for EntityChanges {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("EntityChanges(<lazy>)")
    }
}

impl From<Vec<EntityChange>> for EntityChanges {
    fn from(changes: Vec<EntityChange>) -> Self {
        Self::from_results(changes.into_iter().map(Ok))
    }
}

#[derive(Debug)]
pub struct EntityResult {
    pub successor: Transaction,
    pub edits: Vec<ByteEdit>,
}

/// Lazy view of one conflict snapshot. Merely choosing a side does not lower
/// its bytes into guest memory.
pub struct ConflictValue<'a> {
    conflicts: &'a ConflictSet,
    budget: &'a Budget,
    ordinal: u64,
    side: ConflictSide,
    len: u64,
}

impl std::fmt::Debug for ConflictValue<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConflictValue")
            .field("ordinal", &self.ordinal)
            .field("side", &self.side)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl ConflictValue<'_> {
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn read(&self) -> Result<Vec<u8>> {
        let capacity = usize::try_from(self.len).map_err(|_| Error::RecordTooLarge(self.len))?;
        let mut output = Vec::with_capacity(capacity);
        let max_page = self.budget.limits().max_page_bytes.max(1);
        let mut offset = 0_u64;
        while offset < self.len {
            let requested = u32::try_from((self.len - offset).min(u64::from(max_page)))
                .expect("conflict read is bounded by max-page-bytes");
            let page = self
                .conflicts
                .read_value(self.budget, self.ordinal, self.side, offset, requested)
                .map_err(arena_error)?;
            if page.len() != requested as usize {
                return Err(Error::invalid_input(
                    "host conflict arena returned a short value page",
                ));
            }
            output.extend_from_slice(&page);
            offset += u64::from(requested);
        }
        Ok(output)
    }
}

#[derive(Debug)]
pub struct EntityConflict<'a> {
    pub ordinal: u64,
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    pub base: Option<ConflictValue<'a>>,
    pub a: Option<ConflictValue<'a>>,
    pub b: Option<ConflictValue<'a>>,
}

impl EntityConflict<'_> {
    pub fn take_b_or_delete(&self) -> ConflictResolution {
        if self.b.is_some() {
            ConflictResolution::TakeB
        } else {
            ConflictResolution::Delete
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConflictResolution {
    TakeBase,
    TakeA,
    TakeB,
    Replace(Vec<u8>),
    ReplaceFormatOnly(Vec<u8>),
    Delete,
}

/// Stateless format behavior over host-owned roots. Implementations may cache
/// call-local decoded pages, but no value survives a transition in guest
/// memory.
pub trait FormatPlugin {
    fn open_file(budget: &Budget, input: OpenFileInput) -> Result<FileResult>;
    fn file_changed(budget: &Budget, input: FileUpdate) -> Result<FileResult>;
    fn open_entities(budget: &Budget, input: OpenEntitiesInput) -> Result<EntityResult>;
    fn entities_changed(budget: &Budget, input: EntityUpdate) -> Result<EntityResult>;

    fn resolve_conflict(conflict: EntityConflict<'_>) -> Result<ConflictResolution> {
        Ok(conflict.take_b_or_delete())
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct Component<P>(PhantomData<P>);

#[doc(hidden)]
pub struct AuthorChangeCursor {
    state: RefCell<ChangeCursorState>,
}

impl std::fmt::Debug for AuthorChangeCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthorChangeCursor(<lazy>)")
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct AuthorEditCursor {
    state: RefCell<CursorState<ByteEdit>>,
}

#[doc(hidden)]
pub struct AuthorResolutionCursor<P> {
    state: RefCell<ResolutionCursorState>,
    plugin: PhantomData<P>,
}

impl<P> std::fmt::Debug for AuthorResolutionCursor<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthorResolutionCursor(<lazy>)")
    }
}

#[derive(Debug)]
struct CursorState<T> {
    pending: VecDeque<T>,
    eof: bool,
}

struct ChangeCursorState {
    source: EntityChangeStream,
    pending: Option<EntityChange>,
    eof: bool,
}

struct ResolutionCursorState {
    conflicts: ConflictSet,
    after_ordinal: Option<u64>,
    eof: bool,
}

impl<P: FormatPlugin + 'static> Guest for Component<P> {
    type ChangeCursor = AuthorChangeCursor;
    type EditCursor = AuthorEditCursor;
    type ResolutionCursor = AuthorResolutionCursor<P>;

    fn open_file(
        budget: &Budget,
        input: OpenFileInput,
    ) -> std::result::Result<FileTransition, PluginError> {
        P::open_file(budget, input)
            .and_then(|result| file_transition(result, budget, budget.limits().max_page_bytes))
            .map_err(plugin_error)
    }

    fn file_changed(
        budget: &Budget,
        input: FileUpdate,
    ) -> std::result::Result<FileTransition, PluginError> {
        P::file_changed(budget, input)
            .and_then(|result| file_transition(result, budget, budget.limits().max_page_bytes))
            .map_err(plugin_error)
    }

    fn open_entities(
        budget: &Budget,
        input: OpenEntitiesInput,
    ) -> std::result::Result<EntityTransition, PluginError> {
        P::open_entities(budget, input)
            .and_then(|result| entity_transition(result, budget.limits().max_page_bytes))
            .map_err(plugin_error)
    }

    fn entities_changed(
        budget: &Budget,
        input: EntityUpdate,
    ) -> std::result::Result<EntityTransition, PluginError> {
        P::entities_changed(budget, input)
            .and_then(|result| entity_transition(result, budget.limits().max_page_bytes))
            .map_err(plugin_error)
    }

    fn resolve_conflicts(
        budget: &Budget,
        input: ConflictUpdate,
    ) -> std::result::Result<ConflictTransition, PluginError> {
        let mut state = ResolutionCursorState {
            conflicts: input.conflicts,
            after_ordinal: None,
            eof: false,
        };
        let first_resolutions =
            next_resolution_page::<P>(&mut state, budget, budget.limits().max_page_bytes)
                .map_err(plugin_error)?
                .unwrap_or_default();
        Ok(ConflictTransition {
            first_resolutions,
            resolutions: (!state.eof).then(|| {
                ResolutionCursor::new(AuthorResolutionCursor {
                    state: RefCell::new(state),
                    plugin: PhantomData::<P>,
                })
            }),
        })
    }
}

impl GuestChangeCursor for AuthorChangeCursor {
    fn next(
        &self,
        budget: &Budget,
        max_bytes: u32,
    ) -> std::result::Result<Option<Vec<u8>>, PluginError> {
        next_change_packet(&mut self.state.borrow_mut(), budget, max_bytes).map_err(plugin_error)
    }
}

impl GuestEditCursor for AuthorEditCursor {
    fn next(
        &self,
        _budget: &Budget,
        max_bytes: u32,
    ) -> std::result::Result<Option<Vec<ByteEdit>>, PluginError> {
        next_page(&mut self.state.borrow_mut(), max_bytes, byte_edit_bytes).map_err(plugin_error)
    }
}

impl<P: FormatPlugin + 'static> GuestResolutionCursor for AuthorResolutionCursor<P> {
    fn next(
        &self,
        budget: &Budget,
        max_bytes: u32,
    ) -> std::result::Result<Option<Vec<WitConflictResolution>>, PluginError> {
        next_resolution_page::<P>(&mut self.state.borrow_mut(), budget, max_bytes)
            .map_err(plugin_error)
    }
}

fn next_resolution_page<P: FormatPlugin>(
    state: &mut ResolutionCursorState,
    budget: &Budget,
    max_bytes: u32,
) -> Result<Option<Vec<WitConflictResolution>>> {
    if state.eof {
        return Ok(None);
    }
    let page = state
        .conflicts
        .scan(budget, state.after_ordinal, max_bytes)
        .map_err(arena_error)?;
    if page.entries.is_empty() {
        if page.next_ordinal.is_some() {
            return Err(Error::invalid_input(
                "host conflict arena returned an empty non-EOF page",
            ));
        }
        state.eof = true;
        return Ok(None);
    }
    let mut output = Vec::with_capacity(page.entries.len());
    for entry in page.entries {
        let (schema_key, entity_pk) = decode_entity_key(&entry.key)?;
        let value = |side: ConflictSide, len: Option<u64>| {
            len.map(|len| ConflictValue {
                conflicts: &state.conflicts,
                budget,
                ordinal: entry.ordinal,
                side,
                len,
            })
        };
        let resolution = P::resolve_conflict(EntityConflict {
            ordinal: entry.ordinal,
            schema_key,
            entity_pk,
            base: value(ConflictSide::Base, entry.base_length),
            a: value(ConflictSide::A, entry.a_length),
            b: value(ConflictSide::B, entry.b_length),
        })?;
        output.push(WitConflictResolution {
            ordinal: entry.ordinal,
            choice: match resolution {
                ConflictResolution::TakeBase => WitConflictChoice::TakeBase,
                ConflictResolution::TakeA => WitConflictChoice::TakeA,
                ConflictResolution::TakeB => WitConflictChoice::TakeB,
                ConflictResolution::Replace(snapshot) => {
                    WitConflictChoice::Replace(WitConflictReplacement {
                        snapshot,
                        format_only: false,
                    })
                }
                ConflictResolution::ReplaceFormatOnly(snapshot) => {
                    WitConflictChoice::Replace(WitConflictReplacement {
                        snapshot,
                        format_only: true,
                    })
                }
                ConflictResolution::Delete => WitConflictChoice::Delete,
            },
        });
    }
    state.after_ordinal = page.next_ordinal;
    state.eof = state.after_ordinal.is_none();
    Ok(Some(output))
}

fn file_transition(result: FileResult, budget: &Budget, max_bytes: u32) -> Result<FileTransition> {
    let mut state = ChangeCursorState {
        source: result.changes.source,
        pending: None,
        eof: false,
    };
    let first_change_packet =
        next_change_packet(&mut state, budget, max_bytes)?.unwrap_or_default();
    Ok(FileTransition {
        successor: result.successor,
        first_change_packet,
        changes: (!state.eof).then(|| {
            ChangeCursor::new(AuthorChangeCursor {
                state: RefCell::new(state),
            })
        }),
    })
}

const CHANGE_PACKET_MAGIC: [u8; 4] = *b"L3C1";
const COMPRESSED_CHANGE_PACKET_MAGIC: [u8; 4] = *b"L3Z1";
const CHANGE_PACKET_HEADER_BYTES: usize = 8;
const CHANGE_PACKET_COMPRESSION_THRESHOLD: usize = 4 * 1024;

fn next_change_packet(
    state: &mut ChangeCursorState,
    budget: &Budget,
    max_bytes: u32,
) -> Result<Option<Vec<u8>>> {
    if state.eof {
        return Ok(None);
    }
    if let EntityChangeStream::Packets(source) = &mut state.source {
        let packet = source.next_packet(budget, max_bytes)?;
        match &packet {
            Some(packet) if packet.is_empty() => {
                return Err(Error::invalid_input(
                    "v3 packet source returned an empty non-EOF packet",
                ));
            }
            Some(packet) if packet.len() > max_bytes as usize => {
                return Err(Error::RecordTooLarge(packet.len() as u64));
            }
            None => state.eof = true,
            _ => {}
        }
        return Ok(packet);
    }
    let max_bytes = max_bytes as usize;
    if max_bytes < CHANGE_PACKET_HEADER_BYTES {
        return Err(Error::RecordTooLarge(CHANGE_PACKET_HEADER_BYTES as u64));
    }
    let mut output = Vec::with_capacity(max_bytes.min(64 * 1024));
    output.extend_from_slice(&CHANGE_PACKET_MAGIC);
    output.extend_from_slice(&0_u32.to_le_bytes());
    let mut count = 0_u32;
    loop {
        let item = match state.pending.take() {
            Some(item) => Some(item),
            None => match &mut state.source {
                EntityChangeStream::Changes(source) => source.next(budget)?,
                EntityChangeStream::Packets(_) => unreachable!("packet source returned above"),
            },
        };
        let Some(item) = item else {
            state.eof = true;
            break;
        };
        let encoded_len = encoded_entity_change_len(&item)?;
        let packet_bytes = CHANGE_PACKET_HEADER_BYTES.saturating_add(encoded_len);
        if count == 0 && packet_bytes > max_bytes {
            return Err(Error::RecordTooLarge(packet_bytes as u64));
        }
        if output.len().saturating_add(encoded_len) > max_bytes {
            state.pending = Some(item);
            break;
        }
        encode_entity_change(&item, &mut output)?;
        count = count
            .checked_add(1)
            .ok_or_else(|| Error::internal("v3 change packet count overflowed"))?;
    }
    if count == 0 && state.eof {
        Ok(None)
    } else {
        output[4..8].copy_from_slice(&count.to_le_bytes());
        Ok(Some(output))
    }
}

#[derive(Debug)]
pub struct EntityChangePacketBuilder {
    max_bytes: usize,
    output: Vec<u8>,
    count: u32,
}

#[derive(Debug)]
pub enum DirectPacketPush {
    Pushed,
    Pending(Vec<u8>),
    NoRecord,
}

impl EntityChangePacketBuilder {
    pub fn new(max_bytes: u32) -> Result<Self> {
        let max_bytes = max_bytes as usize;
        if max_bytes < CHANGE_PACKET_HEADER_BYTES {
            return Err(Error::RecordTooLarge(CHANGE_PACKET_HEADER_BYTES as u64));
        }
        let mut output = Vec::with_capacity(max_bytes);
        output.extend_from_slice(&CHANGE_PACKET_MAGIC);
        output.extend_from_slice(&0_u32.to_le_bytes());
        Ok(Self {
            max_bytes,
            output,
            count: 0,
        })
    }

    /// Appends a change, or returns it unchanged when the current packet is
    /// full. A single record larger than the configured packet is rejected.
    pub fn try_push(&mut self, change: EntityChange) -> Result<Option<EntityChange>> {
        let encoded_len = encoded_entity_change_len(&change)?;
        let packet_bytes = CHANGE_PACKET_HEADER_BYTES.saturating_add(encoded_len);
        if self.count == 0 && packet_bytes > self.max_bytes {
            return Err(Error::RecordTooLarge(packet_bytes as u64));
        }
        if self.output.len().saturating_add(encoded_len) > self.max_bytes {
            return Ok(Some(change));
        }
        encode_entity_change(&change, &mut self.output)?;
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| Error::internal("v3 change packet count overflowed"))?;
        Ok(None)
    }

    /// Writes one upsert directly into the packet, avoiding an intermediate
    /// entity-change allocation and snapshot recopy. If the current packet is
    /// full, the encoded record is returned for the next packet.
    pub fn try_push_with_snapshot(
        &mut self,
        schema_key: &str,
        entity_pk: &[&str],
        format_only: bool,
        write_snapshot: impl FnOnce(&mut Vec<u8>) -> Result<bool>,
    ) -> Result<DirectPacketPush> {
        let start = self.output.len();
        let key_len = 4usize
            .checked_add(schema_key.len())
            .and_then(|len| {
                entity_pk
                    .iter()
                    .try_fold(len, |len, part| len.checked_add(4)?.checked_add(part.len()))
            })
            .ok_or_else(|| Error::RecordTooLarge(u64::MAX))?;
        append_packet_len(&mut self.output, key_len)?;
        append_packet_bytes(&mut self.output, schema_key.as_bytes())?;
        for part in entity_pk {
            append_packet_bytes(&mut self.output, part.as_bytes())?;
        }
        self.output.push(1 | (u8::from(format_only) << 1));
        let snapshot_len_offset = self.output.len();
        self.output.extend_from_slice(&0_u32.to_le_bytes());
        let snapshot_start = self.output.len();
        match write_snapshot(&mut self.output) {
            Ok(true) => {}
            Ok(false) => {
                self.output.truncate(start);
                return Ok(DirectPacketPush::NoRecord);
            }
            Err(error) => {
                self.output.truncate(start);
                return Err(error);
            }
        }
        let snapshot_len = self.output.len().saturating_sub(snapshot_start);
        self.output[snapshot_len_offset..snapshot_start].copy_from_slice(
            &u32::try_from(snapshot_len)
                .map_err(|_| Error::RecordTooLarge(snapshot_len as u64))?
                .to_le_bytes(),
        );
        let encoded_len = self.output.len().saturating_sub(start);
        if self.output.len() > self.max_bytes {
            let record = self.output.split_off(start);
            if self.count == 0 {
                return Err(Error::RecordTooLarge(
                    CHANGE_PACKET_HEADER_BYTES.saturating_add(encoded_len) as u64,
                ));
            }
            return Ok(DirectPacketPush::Pending(record));
        }
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| Error::internal("v3 change packet count overflowed"))?;
        Ok(DirectPacketPush::Pushed)
    }

    /// Appends a record returned by `try_push_with_snapshot`.
    pub fn try_push_encoded_record(&mut self, record: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let packet_bytes = CHANGE_PACKET_HEADER_BYTES.saturating_add(record.len());
        if self.count == 0 && packet_bytes > self.max_bytes {
            return Err(Error::RecordTooLarge(packet_bytes as u64));
        }
        if self.output.len().saturating_add(record.len()) > self.max_bytes {
            return Ok(Some(record));
        }
        self.output.extend_from_slice(&record);
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| Error::internal("v3 change packet count overflowed"))?;
        Ok(None)
    }

    pub fn finish(mut self) -> Option<Vec<u8>> {
        if self.count == 0 {
            return None;
        }
        self.output[4..8].copy_from_slice(&self.count.to_le_bytes());
        if self.output.len() < CHANGE_PACKET_COMPRESSION_THRESHOLD {
            return Some(self.output);
        }
        let compressed = lz4_flex::block::compress_prepend_size(&self.output);
        if compressed.len().saturating_add(4) >= self.output.len() {
            return Some(self.output);
        }
        let mut packet = Vec::with_capacity(4 + compressed.len());
        packet.extend_from_slice(&COMPRESSED_CHANGE_PACKET_MAGIC);
        packet.extend_from_slice(&compressed);
        Some(packet)
    }
}

fn encode_entity_change(change: &EntityChange, output: &mut Vec<u8>) -> Result<()> {
    let key_len = encoded_entity_key_len(change)?;
    append_packet_len(output, key_len)?;
    append_packet_bytes(output, change.schema_key.as_bytes())?;
    for part in &change.entity_pk {
        append_packet_bytes(output, part.as_bytes())?;
    }
    let mut flags = u8::from(change.snapshot.is_some());
    if change.format_only {
        flags |= 1 << 1;
    }
    output.push(flags);
    if let Some(snapshot) = &change.snapshot {
        append_packet_bytes(output, snapshot)?;
    }
    Ok(())
}

fn encoded_entity_change_len(change: &EntityChange) -> Result<usize> {
    let key_len = encoded_entity_key_len(change)?;
    4usize
        .checked_add(key_len)
        .and_then(|len| len.checked_add(1))
        .and_then(|len| {
            change.snapshot.as_ref().map_or(Some(len), |snapshot| {
                len.checked_add(4)
                    .and_then(|len| len.checked_add(snapshot.len()))
            })
        })
        .ok_or_else(|| Error::RecordTooLarge(u64::MAX))
}

fn encoded_entity_key_len(change: &EntityChange) -> Result<usize> {
    let mut len = 4usize
        .checked_add(change.schema_key.len())
        .ok_or_else(|| Error::RecordTooLarge(u64::MAX))?;
    for part in &change.entity_pk {
        len = len
            .checked_add(4)
            .and_then(|len| len.checked_add(part.len()))
            .ok_or_else(|| Error::RecordTooLarge(u64::MAX))?;
    }
    u32::try_from(len).map_err(|_| Error::RecordTooLarge(len as u64))?;
    Ok(len)
}

fn append_packet_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    append_packet_len(output, bytes.len())?;
    output.extend_from_slice(bytes);
    Ok(())
}

fn append_packet_len(output: &mut Vec<u8>, len: usize) -> Result<()> {
    let len = u32::try_from(len).map_err(|_| Error::RecordTooLarge(len as u64))?;
    output.extend_from_slice(&len.to_le_bytes());
    Ok(())
}

fn entity_transition(result: EntityResult, max_bytes: u32) -> Result<EntityTransition> {
    let (first_edits, remaining) = split_first_page(result.edits, max_bytes, byte_edit_bytes)?;
    Ok(EntityTransition {
        successor: result.successor,
        first_edits,
        edits: (!remaining.is_empty()).then(|| {
            EditCursor::new(AuthorEditCursor {
                state: RefCell::new(CursorState {
                    pending: remaining,
                    eof: false,
                }),
            })
        }),
    })
}

fn split_first_page<T>(
    values: Vec<T>,
    max_bytes: u32,
    measure: impl Copy + Fn(&T) -> u64,
) -> Result<(Vec<T>, VecDeque<T>)> {
    let mut state = CursorState {
        pending: values.into(),
        eof: false,
    };
    let first = next_page(&mut state, max_bytes, measure)?.unwrap_or_default();
    Ok((first, state.pending))
}

fn next_page<T>(
    state: &mut CursorState<T>,
    max_bytes: u32,
    measure: impl Fn(&T) -> u64,
) -> Result<Option<Vec<T>>> {
    if state.eof {
        return Ok(None);
    }
    if state.pending.is_empty() {
        state.eof = true;
        return Ok(None);
    }
    let mut bytes = 0_u64;
    let mut output = Vec::new();
    while let Some(item) = state.pending.front() {
        let item_bytes = measure(item);
        if output.is_empty() && item_bytes > u64::from(max_bytes) {
            return Err(Error::RecordTooLarge(item_bytes));
        }
        if bytes.saturating_add(item_bytes) > u64::from(max_bytes) {
            break;
        }
        bytes = bytes.saturating_add(item_bytes);
        output.push(
            state
                .pending
                .pop_front()
                .expect("front item must still be present"),
        );
    }
    Ok(Some(output))
}

fn byte_edit_bytes(edit: &ByteEdit) -> u64 {
    24_u64.saturating_add(edit.insert.len() as u64)
}

fn plugin_error(error: Error) -> PluginError {
    match error {
        Error::InvalidInput(message) => PluginError::InvalidInput(message),
        Error::RecordTooLarge(bytes) => PluginError::RecordTooLarge(bytes),
        Error::LimitExceeded(message) => PluginError::LimitExceeded(message),
        Error::DeadlineExceeded => PluginError::DeadlineExceeded,
        Error::Internal(message) => PluginError::Internal(message),
    }
}

fn arena_error(error: lix::plugin::arena::ArenaError) -> Error {
    use lix::plugin::arena::ArenaError;
    match error {
        ArenaError::InvalidRange => Error::invalid_input("host arena range is invalid"),
        ArenaError::RecordTooLarge(bytes) => Error::RecordTooLarge(bytes),
        ArenaError::LimitExceeded(message) => Error::LimitExceeded(message),
        ArenaError::DeadlineExceeded => Error::DeadlineExceeded,
        ArenaError::Unavailable(message) => Error::internal(message),
    }
}

#[macro_export]
macro_rules! export_v3 {
    ($plugin:ty) => {
        type __LixPluginApiV3Component = $crate::Component<$plugin>;
        $crate::__export_component_v3!(__LixPluginApiV3Component);
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_never_emits_an_empty_non_eof_page() {
        let mut state = CursorState {
            pending: VecDeque::from([ByteEdit {
                offset: 0,
                delete_len: 0,
                insert: b"x".to_vec(),
            }]),
            eof: false,
        };
        assert_eq!(
            next_page(&mut state, 25, byte_edit_bytes)
                .unwrap()
                .unwrap()
                .len(),
            1
        );
        assert!(
            next_page(&mut state, 25, byte_edit_bytes)
                .unwrap()
                .is_none()
        );
        assert!(
            next_page(&mut state, 25, byte_edit_bytes)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn entity_arena_key_round_trips_without_delimiter_ambiguity() {
        let expected = (
            "json_object_member".to_owned(),
            vec!["a/b".to_owned(), "c".to_owned()],
        );
        let encoded = entity_key(&expected.0, &expected.1).unwrap();
        assert_eq!(decode_entity_key(&encoded).unwrap(), expected);
        assert_ne!(
            entity_key("a", &["bc".to_owned()]).unwrap(),
            entity_key("ab", &["c".to_owned()]).unwrap()
        );
    }
}
