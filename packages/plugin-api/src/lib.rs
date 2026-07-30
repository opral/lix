//! Authoring layer for Lix's fused, host-owned Component API v3.

#![allow(clippy::missing_errors_doc)]

wit_bindgen::generate!({
    path: "wit",
    world: "plugin",
    pub_export_macro: true,
    export_macro_name: "__export_plugin_component",
    default_bindings_module: "lix_plugin_api",
});

use exports::lix::plugin::api::{
    ConflictUpdate as WitConflictUpdate, EntityUpdate as WitEntityUpdate, Guest, PluginError,
    TransitionRequest,
};
use lix::plugin::host::{
    ChangeEffect as WitChangeEffect, ChangePage, ConflictSide as WitConflictSide, ConflictSource,
    EntityChangeSource, HostError, MapSpace, PackedPage, ResolutionEffect, ResolutionSink,
    Snapshot as WitSnapshot, Transition as WitTransition,
};
use std::marker::PhantomData;

pub const PACKET_FORMAT_V1: u16 = 1;
pub const PACKED_CSV_ROWS_CODEC: &str = "lix.csv.rows";
pub const PACKED_CSV_ROWS_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidInput(String),
    LimitExceeded(String),
    Internal(String),
}

impl Error {
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput(message.into())
    }

    pub fn limit_exceeded(message: impl Into<String>) -> Self {
        Self::LimitExceeded(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

fn plugin_error(error: Error) -> PluginError {
    match error {
        Error::InvalidInput(message) => PluginError::InvalidInput(message),
        Error::LimitExceeded(message) => PluginError::LimitExceeded(message),
        Error::Internal(message) => PluginError::Internal(message),
    }
}

fn host_error(context: &str, error: HostError) -> Error {
    match error {
        HostError::InvalidRange => Error::invalid_input(format!("{context}: invalid range")),
        HostError::LimitExceeded(message) => Error::limit_exceeded(format!("{context}: {message}")),
        HostError::Rejected(message) => Error::invalid_input(format!("{context}: {message}")),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateContext {
    pub high: u64,
    pub low: u32,
}

impl CreateContext {
    pub fn namespace_bytes(self) -> [u8; 12] {
        let mut bytes = [0_u8; 12];
        bytes[..8].copy_from_slice(&self.high.to_be_bytes());
        bytes[8..].copy_from_slice(&self.low.to_be_bytes());
        bytes
    }

    pub fn id(self, local_ref: u32) -> String {
        let mut bytes = [0_u8; 16];
        bytes[..12].copy_from_slice(&self.namespace_bytes());
        bytes[12..].copy_from_slice(&local_ref.to_be_bytes());
        uuid::Uuid::from_bytes(bytes).to_string()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileInfo {
    pub path: Option<String>,
    pub media_type: Option<String>,
}

pub struct Root<'a> {
    inner: &'a WitSnapshot,
}

impl std::fmt::Debug for Root<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Root")
            .field("file_len", &self.len())
            .finish()
    }
}

impl Root<'_> {
    pub fn len(&self) -> u64 {
        self.inner.file_len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn read_all(&self) -> Result<Vec<u8>> {
        self.read_range(0, self.len())
    }

    pub fn read_range(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        const READ_BYTES: u32 = 1024 * 1024;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Error::invalid_input("snapshot byte range overflowed"))?;
        if end > self.len() {
            return Err(Error::invalid_input("snapshot byte range exceeds the file"));
        }
        let capacity = usize::try_from(length)
            .map_err(|_| Error::limit_exceeded("snapshot range exceeds guest address space"))?;
        let mut output = Vec::with_capacity(capacity);
        let mut cursor = offset;
        while cursor < end {
            let chunk = u32::try_from((end - cursor).min(u64::from(READ_BYTES)))
                .expect("bounded snapshot read fits u32");
            let bytes = self
                .inner
                .read_file(cursor, chunk)
                .map_err(|error| host_error("host snapshot read failed", error))?;
            if bytes.len() != chunk as usize {
                return Err(Error::invalid_input("host snapshot returned a short read"));
            }
            output.extend_from_slice(&bytes);
            cursor += u64::from(chunk);
        }
        Ok(output)
    }

    pub fn get_entity(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.read_record_all(MapSpace::Entity, key)
    }

    pub fn get_state(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.read_record_all(MapSpace::State, key)
    }

    pub fn state_len(&self, key: &[u8]) -> Result<Option<u64>> {
        self.inner
            .read_record(MapSpace::State, key, 0, 0)
            .map(|chunk| chunk.map(|chunk| chunk.total_len))
            .map_err(|error| host_error("host state length read failed", error))
    }

    pub fn read_state_range(
        &self,
        key: &[u8],
        offset: u64,
        length: u32,
    ) -> Result<Option<Vec<u8>>> {
        self.inner
            .read_record(MapSpace::State, key, offset, length)
            .map(|chunk| chunk.map(|chunk| chunk.bytes))
            .map_err(|error| host_error("host state range read failed", error))
    }

    fn read_record_all(&self, space: MapSpace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        const READ_BYTES: u32 = 1024 * 1024;
        let Some(first) = self
            .inner
            .read_record(space, key, 0, READ_BYTES)
            .map_err(|error| host_error("host record read failed", error))?
        else {
            return Ok(None);
        };
        let capacity = usize::try_from(first.total_len)
            .map_err(|_| Error::limit_exceeded("host record exceeds guest address space"))?;
        let mut output = Vec::with_capacity(capacity);
        output.extend_from_slice(&first.bytes);
        while output.len() < capacity {
            let offset = output.len() as u64;
            let remaining = capacity - output.len();
            let chunk_len = u32::try_from(remaining.min(READ_BYTES as usize))
                .expect("bounded record read fits u32");
            let chunk = self
                .inner
                .read_record(space, key, offset, chunk_len)
                .map_err(|error| host_error("host record read failed", error))?
                .ok_or_else(|| Error::invalid_input("host record disappeared during read"))?;
            if chunk.total_len != first.total_len || chunk.bytes.is_empty() {
                return Err(Error::invalid_input(
                    "host record changed or returned a short read",
                ));
            }
            output.extend_from_slice(&chunk.bytes);
        }
        Ok(Some(output))
    }
}

pub struct Transaction<'a> {
    inner: &'a WitTransition,
}

impl std::fmt::Debug for Transaction<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Transaction")
            .finish_non_exhaustive()
    }
}

impl Transaction<'_> {
    pub fn put_state(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner
            .put_state(key, value)
            .map_err(|error| host_error("host rejected state page", error))
    }

    pub fn delete_state(&self, key: &[u8]) -> Result<()> {
        self.inner
            .delete_state(key)
            .map_err(|error| host_error("host rejected state deletion", error))
    }
}

pub struct Sink<'a> {
    inner: &'a WitTransition,
    max_batch_bytes: u32,
}

impl std::fmt::Debug for Sink<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Sink")
            .field("max_batch_bytes", &self.max_batch_bytes)
            .finish_non_exhaustive()
    }
}

impl Sink<'_> {
    pub fn max_batch_bytes(&self) -> u32 {
        self.max_batch_bytes
    }

    pub fn emit_changes(&mut self, record_count: u32, payload: Vec<u8>) -> Result<()> {
        self.validate_page(record_count, payload.len(), "change")?;
        self.inner
            .emit_changes(&ChangePage {
                record_count,
                payload,
            })
            .map_err(|error| host_error("host rejected entity batch", error))
    }

    pub fn emit_csv_rows(&mut self, row_count: u32, payload: Vec<u8>) -> Result<()> {
        self.emit_packed(
            PACKED_CSV_ROWS_CODEC,
            PACKED_CSV_ROWS_VERSION,
            row_count,
            payload,
        )
    }

    pub fn emit_packed(
        &mut self,
        codec: &str,
        format_version: u16,
        record_count: u32,
        payload: Vec<u8>,
    ) -> Result<()> {
        self.validate_page(record_count, payload.len(), "packed")?;
        self.inner
            .emit_packed(&PackedPage {
                codec: codec.to_owned(),
                format_version,
                record_count,
                payload,
            })
            .map_err(|error| host_error("host rejected packed batch", error))
    }

    pub fn replace_file(&mut self, bytes: &[u8]) -> Result<()> {
        self.inner
            .begin_file_replacement(bytes.len() as u64)
            .map_err(|error| host_error("host rejected file replacement", error))?;
        for chunk in bytes.chunks(self.max_batch_bytes as usize) {
            self.inner
                .write_file_replacement(chunk)
                .map_err(|error| host_error("host rejected file replacement chunk", error))?;
        }
        self.inner
            .finish_file_replacement()
            .map_err(|error| host_error("host rejected file replacement finish", error))
    }

    fn validate_page(&self, record_count: u32, payload_len: usize, kind: &str) -> Result<()> {
        if record_count == 0 {
            return Err(Error::invalid_input(format!(
                "v3 {kind} pages cannot be empty"
            )));
        }
        if payload_len > self.max_batch_bytes as usize {
            return Err(Error::limit_exceeded(format!(
                "v3 {kind} page exceeds max-batch-bytes"
            )));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct OpenFile<'a> {
    pub file: FileInfo,
    pub accepted: Root<'a>,
    pub successor: Transaction<'a>,
    pub creates: CreateContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputSplice {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: Vec<u8>,
}

#[derive(Debug)]
pub struct FileUpdate<'a> {
    pub before_file: FileInfo,
    pub after_file: FileInfo,
    pub before: Root<'a>,
    pub edits: Vec<InputSplice>,
    pub successor: Transaction<'a>,
    pub creates: CreateContext,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConflictSide {
    Base,
    A,
    B,
}

impl ConflictSide {
    fn wit(self) -> WitConflictSide {
        match self {
            Self::Base => WitConflictSide::Base,
            Self::A => WitConflictSide::A,
            Self::B => WitConflictSide::B,
        }
    }
}

pub struct ConflictValue<'a> {
    source: &'a ConflictSource,
    index: u32,
    side: ConflictSide,
    len: u64,
}

impl std::fmt::Debug for ConflictValue<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConflictValue")
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
        const READ_BYTES: u32 = 1024 * 1024;
        let capacity = usize::try_from(self.len)
            .map_err(|_| Error::limit_exceeded("conflict value exceeds guest address space"))?;
        let mut output = Vec::with_capacity(capacity);
        while output.len() < capacity {
            let offset = output.len() as u64;
            let length = u32::try_from((capacity - output.len()).min(READ_BYTES as usize))
                .expect("bounded conflict read fits u32");
            let bytes = self
                .source
                .read_value(self.index, self.side.wit(), offset, length)
                .map_err(|error| host_error("host conflict read failed", error))?
                .ok_or_else(|| Error::invalid_input("host conflict value disappeared"))?;
            if bytes.is_empty() {
                return Err(Error::invalid_input(
                    "host conflict value returned a short read",
                ));
            }
            output.extend_from_slice(&bytes);
        }
        Ok(output)
    }
}

#[derive(Debug)]
pub struct EntityConflict<'a> {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeEffect {
    Content,
    FormatOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityChange {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    pub snapshot: Option<Vec<u8>>,
    pub effect: ChangeEffect,
}

pub struct EntityChangeReader<'a> {
    source: &'a EntityChangeSource,
    next: u32,
    len: u32,
}

impl std::fmt::Debug for EntityChangeReader<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EntityChangeReader")
            .field("next", &self.next)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl EntityChangeReader<'_> {
    pub fn next(&mut self) -> Result<Option<EntityChange>> {
        if self.next == self.len {
            return Ok(None);
        }
        let index = self.next;
        self.next += 1;
        let meta = self
            .source
            .get(index)
            .map_err(|error| host_error("host entity-change metadata read failed", error))?;
        if meta.ordinal != index {
            return Err(Error::invalid_input(format!(
                "entity-change ordinal {}, expected {index}",
                meta.ordinal
            )));
        }
        let snapshot = meta
            .snapshot_len
            .map(|length| read_entity_snapshot(self.source, index, length))
            .transpose()?;
        Ok(Some(EntityChange {
            schema_key: meta.schema_key,
            entity_pk: meta.entity_pk,
            snapshot,
            effect: match meta.effect {
                WitChangeEffect::Content => ChangeEffect::Content,
                WitChangeEffect::FormatOnly => ChangeEffect::FormatOnly,
            },
        }))
    }
}

fn read_entity_snapshot(source: &EntityChangeSource, index: u32, length: u64) -> Result<Vec<u8>> {
    const READ_BYTES: u32 = 1024 * 1024;
    let capacity = usize::try_from(length)
        .map_err(|_| Error::limit_exceeded("entity snapshot exceeds guest address space"))?;
    let mut output = Vec::with_capacity(capacity);
    while output.len() < capacity {
        let offset = output.len() as u64;
        let chunk = u32::try_from((capacity - output.len()).min(READ_BYTES as usize))
            .expect("bounded entity snapshot read fits u32");
        let bytes = source
            .read_snapshot(index, offset, chunk)
            .map_err(|error| host_error("host entity snapshot read failed", error))?
            .ok_or_else(|| Error::invalid_input("host entity snapshot disappeared"))?;
        if bytes.is_empty() {
            return Err(Error::invalid_input(
                "host entity snapshot returned a short read",
            ));
        }
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}

#[derive(Debug)]
pub struct EntityUpdate<'a> {
    pub before_file: FileInfo,
    pub after_file: FileInfo,
    pub before: Root<'a>,
    pub changes: EntityChangeReader<'a>,
}

pub trait FormatPlugin: 'static {
    fn open_file(input: &OpenFile<'_>, sink: &mut Sink<'_>) -> Result<()>;

    fn file_changed(update: &FileUpdate<'_>, sink: &mut Sink<'_>) -> Result<()>;

    fn resolve_conflict(conflict: EntityConflict<'_>) -> Result<ConflictResolution> {
        Ok(conflict.take_b_or_delete())
    }

    fn entities_changed(_update: &mut EntityUpdate<'_>, _sink: &mut Sink<'_>) -> Result<()> {
        Err(Error::internal(
            "this format does not implement semantic entity changes",
        ))
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct Component<P>(PhantomData<P>);

impl<P: FormatPlugin> Guest for Component<P> {
    fn apply(
        request: TransitionRequest,
        output: &WitTransition,
    ) -> std::result::Result<(), PluginError> {
        let max_batch_bytes = output.max_batch_bytes();
        if max_batch_bytes == 0 {
            return Err(PluginError::LimitExceeded(
                "max-batch-bytes must be positive".to_owned(),
            ));
        }
        let mut sink = Sink {
            inner: output,
            max_batch_bytes,
        };
        match request {
            TransitionRequest::Open(request) => {
                let input = OpenFile {
                    file: FileInfo {
                        path: request.descriptor.path,
                        media_type: request.descriptor.media_type,
                    },
                    accepted: Root {
                        inner: &request.accepted,
                    },
                    successor: Transaction { inner: output },
                    creates: CreateContext {
                        high: request.creates.high,
                        low: request.creates.low,
                    },
                };
                P::open_file(&input, &mut sink).map_err(plugin_error)
            }
            TransitionRequest::Update(request) => {
                let input = FileUpdate {
                    before_file: FileInfo {
                        path: request.before_descriptor.path,
                        media_type: request.before_descriptor.media_type,
                    },
                    after_file: FileInfo {
                        path: request.after_descriptor.path,
                        media_type: request.after_descriptor.media_type,
                    },
                    before: Root {
                        inner: &request.before,
                    },
                    edits: request
                        .edits
                        .into_iter()
                        .map(|edit| InputSplice {
                            offset: edit.offset,
                            delete_len: edit.delete_len,
                            insert: edit.insert,
                        })
                        .collect(),
                    successor: Transaction { inner: output },
                    creates: CreateContext {
                        high: request.creates.high,
                        low: request.creates.low,
                    },
                };
                P::file_changed(&input, &mut sink).map_err(plugin_error)
            }
        }
    }

    fn resolve_conflicts(
        input: WitConflictUpdate,
        output: &ResolutionSink,
    ) -> std::result::Result<(), PluginError> {
        let count = input.conflicts.len();
        let max_batch_bytes = output.max_batch_bytes();
        if max_batch_bytes == 0 {
            return Err(PluginError::LimitExceeded(
                "max-batch-bytes must be positive".to_owned(),
            ));
        }
        for index in 0..count {
            let meta = input.conflicts.get(index).map_err(|error| {
                plugin_error(host_error("host conflict metadata read failed", error))
            })?;
            let value = |side, len: Option<u64>| {
                len.map(|len| ConflictValue {
                    source: &input.conflicts,
                    index,
                    side,
                    len,
                })
            };
            let conflict = EntityConflict {
                schema_key: meta.schema_key,
                entity_pk: meta.entity_pk,
                base: value(ConflictSide::Base, meta.base_len),
                a: value(ConflictSide::A, meta.a_len),
                b: value(ConflictSide::B, meta.b_len),
            };
            let resolution = P::resolve_conflict(conflict).map_err(plugin_error)?;
            match resolution {
                ConflictResolution::TakeBase => output
                    .take(meta.ordinal, WitConflictSide::Base)
                    .map_err(|error| {
                        plugin_error(host_error("host rejected conflict take", error))
                    })?,
                ConflictResolution::TakeA => output
                    .take(meta.ordinal, WitConflictSide::A)
                    .map_err(|error| {
                        plugin_error(host_error("host rejected conflict take", error))
                    })?,
                ConflictResolution::TakeB => output
                    .take(meta.ordinal, WitConflictSide::B)
                    .map_err(|error| {
                        plugin_error(host_error("host rejected conflict take", error))
                    })?,
                ConflictResolution::Delete => output.delete(meta.ordinal).map_err(|error| {
                    plugin_error(host_error("host rejected conflict delete", error))
                })?,
                ConflictResolution::Replace(snapshot) => {
                    output
                        .begin_replace(
                            meta.ordinal,
                            ResolutionEffect::Content,
                            snapshot.len() as u64,
                        )
                        .map_err(|error| {
                            plugin_error(host_error("host rejected conflict replacement", error))
                        })?;
                    for chunk in snapshot.chunks(max_batch_bytes as usize) {
                        output.write_replacement(chunk).map_err(|error| {
                            plugin_error(host_error("host rejected replacement chunk", error))
                        })?;
                    }
                    output.finish_replace().map_err(|error| {
                        plugin_error(host_error("host rejected replacement finish", error))
                    })?;
                }
                ConflictResolution::ReplaceFormatOnly(snapshot) => {
                    output
                        .begin_replace(
                            meta.ordinal,
                            ResolutionEffect::FormatOnly,
                            snapshot.len() as u64,
                        )
                        .map_err(|error| {
                            plugin_error(host_error("host rejected conflict replacement", error))
                        })?;
                    for chunk in snapshot.chunks(max_batch_bytes as usize) {
                        output.write_replacement(chunk).map_err(|error| {
                            plugin_error(host_error("host rejected replacement chunk", error))
                        })?;
                    }
                    output.finish_replace().map_err(|error| {
                        plugin_error(host_error("host rejected replacement finish", error))
                    })?;
                }
            }
        }
        Ok(())
    }

    fn entities_changed(
        input: WitEntityUpdate,
        output: &WitTransition,
    ) -> std::result::Result<(), PluginError> {
        let max_batch_bytes = output.max_batch_bytes();
        if max_batch_bytes == 0 {
            return Err(PluginError::LimitExceeded(
                "max-batch-bytes must be positive".to_owned(),
            ));
        }
        let mut update = EntityUpdate {
            before_file: FileInfo {
                path: input.before_descriptor.path,
                media_type: input.before_descriptor.media_type,
            },
            after_file: FileInfo {
                path: input.after_descriptor.path,
                media_type: input.after_descriptor.media_type,
            },
            before: Root {
                inner: &input.before,
            },
            changes: EntityChangeReader {
                len: input.changes.len(),
                source: &input.changes,
                next: 0,
            },
        };
        let mut sink = Sink {
            inner: output,
            max_batch_bytes,
        };
        P::entities_changed(&mut update, &mut sink).map_err(plugin_error)
    }
}

#[macro_export]
macro_rules! export_plugin {
    ($plugin:ty) => {
        type __LixPluginComponent = $crate::Component<$plugin>;
        $crate::__export_plugin_component!(__LixPluginComponent);
    };
}
