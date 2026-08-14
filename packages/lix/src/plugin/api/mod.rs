//! Authoring layer for Lix's fused, host-owned Component API v1.

#![allow(clippy::missing_errors_doc)]

wit_bindgen::generate!({
    path: "wit",
    world: "plugin",
    pub_export_macro: true,
    export_macro_name: "__export_plugin_component",
});

use self::lix::plugin::host::{
    ConflictSide as WitConflictSide, ConflictSource, HostError, ResolutionSink,
    RowPage as WitRowPage, RowSource, Snapshot as WitSnapshot, Transition as WitTransition,
};
use super::wire::{Operation, Page as WirePage, Representation, encode_single_section};
use exports::lix::plugin::api::{
    ColdFileChangedRequest as WitColdFileChangedRequest, ConflictUpdate as WitConflictUpdate,
    Guest, PluginError, RestoreRequest as WitRestoreRequest,
    RowsChangedRequest as WitRowsChangedRequest, TransitionRequest,
};
use std::marker::PhantomData;

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
    high: u64,
    low: u32,
}

impl CreateContext {
    pub fn from_namespace_bytes(bytes: [u8; 12]) -> Self {
        Self {
            high: u64::from_be_bytes(bytes[..8].try_into().expect("eight-byte namespace prefix")),
            low: u32::from_be_bytes(bytes[8..].try_into().expect("four-byte namespace suffix")),
        }
    }

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

pub struct Snapshot<'a> {
    inner: &'a WitSnapshot,
}

impl std::fmt::Debug for Snapshot<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Snapshot")
            .field("file_len", &self.len())
            .finish()
    }
}

impl Snapshot<'_> {
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

    pub fn get_state(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.read_state_all(key)
    }

    pub fn state_len(&self, key: &[u8]) -> Result<Option<u64>> {
        self.inner
            .read_state(key, 0, 0)
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
            .read_state(key, offset, length)
            .map(|chunk| chunk.map(|chunk| chunk.bytes))
            .map_err(|error| host_error("host state range read failed", error))
    }

    fn read_state_all(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        const READ_BYTES: u32 = 1024 * 1024;
        let Some(first) = self
            .inner
            .read_state(key, 0, READ_BYTES)
            .map_err(|error| host_error("host state read failed", error))?
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
                .read_state(key, offset, chunk_len)
                .map_err(|error| host_error("host state read failed", error))?
                .ok_or_else(|| Error::invalid_input("host state disappeared during read"))?;
            if chunk.total_len != first.total_len || chunk.bytes.is_empty() {
                return Err(Error::invalid_input(
                    "host state changed or returned a short read",
                ));
            }
            output.extend_from_slice(&chunk.bytes);
        }
        Ok(Some(output))
    }
}

pub struct Output<'a> {
    inner: &'a WitTransition,
    max_page_bytes: u32,
    max_batch_bytes: u32,
    row_payload: Vec<u8>,
    row_records: u32,
    row_creates_only: Option<bool>,
}

impl std::fmt::Debug for Output<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Output")
            .field("max_batch_bytes", &self.max_batch_bytes)
            .finish_non_exhaustive()
    }
}

impl<'a> Output<'a> {
    fn new(inner: &'a WitTransition) -> std::result::Result<Self, PluginError> {
        let max_page_bytes = inner.max_batch_bytes();
        let snapshot_overhead = 24;
        if max_page_bytes <= snapshot_overhead {
            return Err(PluginError::LimitExceeded(
                "max-batch-bytes cannot hold a row page".to_owned(),
            ));
        }
        Ok(Output {
            inner,
            max_page_bytes,
            max_batch_bytes: max_page_bytes - snapshot_overhead,
            row_payload: Vec::with_capacity(
                usize::try_from(max_page_bytes - snapshot_overhead)
                    .expect("u32 fits usize")
                    .min(64 * 1024),
            ),
            row_records: 0,
            row_creates_only: None,
        })
    }

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

    pub fn replace_file(&mut self, bytes: &[u8]) -> Result<()> {
        self.flush_rows()?;
        self.inner
            .begin_file_replacement(bytes.len() as u64)
            .map_err(|error| host_error("host rejected file replacement", error))?;
        for chunk in bytes.chunks(self.max_page_bytes as usize) {
            self.inner
                .write_file_replacement(chunk)
                .map_err(|error| host_error("host rejected file replacement chunk", error))?;
        }
        self.inner
            .finish_file_replacement()
            .map_err(|error| host_error("host rejected file replacement finish", error))
    }

    /// Emits one typed mutation. The SDK owns record framing, bounded batching,
    /// create-page separation, record counts, and final flushing.
    pub fn row(&mut self, mutation: RowMutation<'_>) -> Result<()> {
        let max_batch_bytes = self.max_batch_bytes as usize;
        // Smaller durable pages keep later point reads bounded without exposing
        // paging policy to authors. A single large record may still use the
        // full host limit; only multi-record batching uses this target.
        let target_page_bytes = max_batch_bytes.min(256 * 1024);
        let oversized_snapshot = match mutation {
            RowMutation::Create { snapshot, .. } | RowMutation::Upsert { snapshot, .. } => {
                snapshot.len() > max_batch_bytes
            }
            RowMutation::Delete { .. } => false,
        };
        if oversized_snapshot {
            return self.emit_attached_row(mutation);
        }

        let start = self.row_payload.len();
        let creates_only = encode_row_mutation(&mut self.row_payload, mutation, None)?;
        if self.row_records > 0
            && (self.row_payload.len() > target_page_bytes
                || self.row_creates_only != Some(creates_only))
        {
            self.row_payload.truncate(start);
            self.flush_rows()?;
            let repeated = encode_row_mutation(&mut self.row_payload, mutation, None)?;
            debug_assert_eq!(repeated, creates_only);
        }
        if self.row_payload.len() > max_batch_bytes {
            self.row_payload.clear();
            return self.emit_attached_row(mutation);
        }
        self.row_records = self
            .row_records
            .checked_add(1)
            .ok_or_else(|| Error::limit_exceeded("row mutation count overflowed"))?;
        self.row_creates_only = Some(creates_only);
        Ok(())
    }

    fn emit_attached_row(&mut self, mutation: RowMutation<'_>) -> Result<()> {
        self.flush_rows()?;
        let snapshot = match mutation {
            RowMutation::Create { snapshot, .. } | RowMutation::Upsert { snapshot, .. } => snapshot,
            RowMutation::Delete { .. } => {
                return Err(Error::limit_exceeded(
                    "one row key exceeds the host page limit",
                ));
            }
        };
        let length = u64::try_from(snapshot.len())
            .map_err(|_| Error::limit_exceeded("row snapshot exceeds u64"))?;
        let ordinal = self
            .inner
            .begin_row_attachment(length)
            .map_err(|error| host_error("host rejected row attachment", error))?;
        for chunk in snapshot.chunks(self.max_page_bytes as usize) {
            self.inner
                .write_row_attachment(chunk)
                .map_err(|error| host_error("host rejected row attachment chunk", error))?;
        }
        self.inner
            .finish_row_attachment()
            .map_err(|error| host_error("host rejected row attachment finish", error))?;
        let creates_only =
            encode_row_mutation(&mut self.row_payload, mutation, Some((ordinal, length)))?;
        if self.row_payload.len() > self.max_batch_bytes as usize {
            self.row_payload.clear();
            return Err(Error::limit_exceeded(
                "one row mutation metadata exceeds the host page limit",
            ));
        }
        self.row_records = 1;
        self.row_creates_only = Some(creates_only);
        Ok(())
    }

    fn flush_rows(&mut self) -> Result<()> {
        if self.row_records == 0 {
            return Ok(());
        }
        let payload = std::mem::replace(
            &mut self.row_payload,
            Vec::with_capacity((self.max_batch_bytes as usize).min(64 * 1024)),
        );
        let records = std::mem::take(&mut self.row_records);
        self.row_creates_only = None;
        let page = RowPage::snapshots(records, payload)?;
        self.inner
            .emit_rows(&WitRowPage {
                payload: page.payload,
            })
            .map_err(|error| host_error("host rejected row page", error))
    }

    fn finish(&mut self) -> Result<()> {
        self.flush_rows()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowMutation<'a> {
    Create {
        schema_key: &'a str,
        local_ref: u32,
        snapshot: &'a [u8],
    },
    Upsert {
        schema_key: &'a str,
        row_pk: &'a [String],
        snapshot: &'a [u8],
        effect: ChangeEffect,
    },
    Delete {
        schema_key: &'a str,
        row_pk: &'a [String],
    },
}

fn encode_row_mutation(
    output: &mut Vec<u8>,
    mutation: RowMutation<'_>,
    attachment: Option<(u32, u64)>,
) -> Result<bool> {
    let start = output.len();
    output.extend_from_slice(&0_u32.to_le_bytes());
    let is_create = match mutation {
        RowMutation::Create {
            schema_key,
            local_ref,
            snapshot,
        } => {
            output.push(2);
            encode_text(output, schema_key)?;
            output.extend_from_slice(&u64::from(local_ref).to_le_bytes());
            encode_snapshot(output, snapshot, attachment)?;
            true
        }
        RowMutation::Upsert {
            schema_key,
            row_pk,
            snapshot,
            effect,
        } => {
            output.push(0);
            encode_key(output, schema_key, row_pk)?;
            output.push(match effect {
                ChangeEffect::Content => 0,
                ChangeEffect::FormatOnly => 1,
            });
            encode_snapshot(output, snapshot, attachment)?;
            false
        }
        RowMutation::Delete { schema_key, row_pk } => {
            output.push(1);
            encode_key(output, schema_key, row_pk)?;
            false
        }
    };
    let length = u32::try_from(output.len() - start - 4)
        .map_err(|_| Error::limit_exceeded("row mutation exceeds u32 framing"))?;
    output[start..start + 4].copy_from_slice(&length.to_le_bytes());
    Ok(is_create)
}

fn encode_key(output: &mut Vec<u8>, schema_key: &str, row_pk: &[String]) -> Result<()> {
    encode_text(output, schema_key)?;
    output.extend_from_slice(
        &u32::try_from(row_pk.len())
            .map_err(|_| Error::limit_exceeded("row primary key has too many components"))?
            .to_le_bytes(),
    );
    for component in row_pk {
        encode_text(output, component)?;
    }
    Ok(())
}

fn encode_text(output: &mut Vec<u8>, value: &str) -> Result<()> {
    output.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| Error::limit_exceeded("row text exceeds u32 framing"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode_snapshot(
    output: &mut Vec<u8>,
    snapshot: &[u8],
    attachment: Option<(u32, u64)>,
) -> Result<()> {
    if let Some((ordinal, length)) = attachment {
        output.push(1);
        output.extend_from_slice(&ordinal.to_le_bytes());
        output.extend_from_slice(&length.to_le_bytes());
    } else {
        output.push(0);
        output.extend_from_slice(
            &u32::try_from(snapshot.len())
                .map_err(|_| Error::limit_exceeded("row snapshot exceeds u32 framing"))?
                .to_le_bytes(),
        );
        output.extend_from_slice(snapshot);
    }
    Ok(())
}

/// One universal row output page.
#[derive(Clone, Debug, Eq, PartialEq)]
struct RowPage {
    payload: Vec<u8>,
}

impl RowPage {
    fn snapshots(record_count: u32, payload: Vec<u8>) -> Result<Self> {
        encode_single_section(
            Representation::Snapshots,
            Operation::Mixed,
            "",
            &[],
            record_count,
            payload,
        )
        .map(|payload| Self { payload })
        .map_err(|error| Error::invalid_input(format!("invalid row page: {error:?}")))
    }
}

#[derive(Debug)]
pub struct OpenFile<'a> {
    pub path: String,
    pub accepted: Snapshot<'a>,
    pub creates: CreateContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileEdit {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: Vec<u8>,
}

#[derive(Debug)]
pub struct FileUpdate<'a> {
    pub before_path: String,
    pub after_path: String,
    pub before: Snapshot<'a>,
    pub edits: Vec<FileEdit>,
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
pub struct RowConflict<'a> {
    pub schema_key: String,
    pub row_pk: Vec<String>,
    pub base: Option<ConflictValue<'a>>,
    pub a: Option<ConflictValue<'a>>,
    pub b: Option<ConflictValue<'a>>,
}

impl RowConflict<'_> {
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
    Delete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeEffect {
    Content,
    FormatOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowChange {
    pub schema_key: String,
    pub row_pk: Vec<String>,
    pub snapshot: Option<Vec<u8>>,
    pub effect: ChangeEffect,
}

pub struct RowChangeReader<'a> {
    source: &'a RowSource,
    page: Option<InputPage>,
    max_page_bytes: u32,
    next: u32,
    eof: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Row {
    pub schema_key: String,
    pub row_pk: Vec<String>,
    pub snapshot: Vec<u8>,
}

/// Validated current rows used by restore and cold transitions. Durable
/// current state cannot contain tombstones, so plugins never branch on them.
pub struct RowReader<'a> {
    changes: RowChangeReader<'a>,
}

impl std::fmt::Debug for RowReader<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RowReader")
            .field("changes", &self.changes)
            .finish()
    }
}

impl RowReader<'_> {
    fn new(source: &RowSource, max_page_bytes: u32) -> RowReader<'_> {
        RowReader {
            changes: RowChangeReader::new(source, max_page_bytes),
        }
    }

    pub fn next(&mut self) -> Result<Option<Row>> {
        let Some(change) = self.changes.next()? else {
            return Ok(None);
        };
        let snapshot = change.snapshot.ok_or_else(|| {
            Error::invalid_input("durable current rows cannot contain tombstones")
        })?;
        Ok(Some(Row {
            schema_key: change.schema_key,
            row_pk: change.row_pk,
            snapshot,
        }))
    }
}

impl std::fmt::Debug for RowChangeReader<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RowChangeReader")
            .field("next", &self.next)
            .field(
                "buffered",
                &self.page.as_ref().map_or(0, |page| page.remaining),
            )
            .field("max_page_bytes", &self.max_page_bytes)
            .field("eof", &self.eof)
            .finish_non_exhaustive()
    }
}

impl RowChangeReader<'_> {
    fn new(source: &RowSource, max_page_bytes: u32) -> RowChangeReader<'_> {
        RowChangeReader {
            source,
            page: None,
            max_page_bytes,
            next: 0,
            eof: false,
        }
    }

    pub fn next(&mut self) -> Result<Option<RowChange>> {
        if self.page.as_ref().is_none_or(|page| page.remaining == 0) {
            if self.eof {
                return Ok(None);
            }
            let Some(page) = self
                .source
                .next_page(self.max_page_bytes)
                .map_err(|error| host_error("host row-change page read failed", error))?
            else {
                self.eof = true;
                return Ok(None);
            };
            self.page = Some(decode_input_page(page.payload)?);
        }
        let index = self.next;
        self.next += 1;
        let change = self
            .page
            .as_mut()
            .expect("a non-empty row-change page has a first record");
        let change = change.next(index)?;
        let snapshot = match change.snapshot {
            Some(InputSnapshot::Inline(bytes)) => Some(bytes),
            Some(InputSnapshot::Attachment { ordinal, length }) => Some(read_row_attachment(
                self.source,
                ordinal,
                length,
                self.max_page_bytes,
            )?),
            None => None,
        };
        debug_assert_eq!(index, change.ordinal);
        Ok(Some(RowChange {
            schema_key: change.schema_key,
            row_pk: change.row_pk,
            snapshot,
            effect: change.effect,
        }))
    }
}

#[derive(Debug)]
struct InputChange {
    ordinal: u32,
    schema_key: String,
    row_pk: Vec<String>,
    snapshot: Option<InputSnapshot>,
    effect: ChangeEffect,
}

#[derive(Debug)]
struct InputPage {
    payload: Vec<u8>,
    offset: usize,
    remaining: u32,
}

impl InputPage {
    fn next(&mut self, ordinal: u32) -> Result<InputChange> {
        if self.remaining == 0 {
            return Err(Error::invalid_input("row input page is exhausted"));
        }
        let mut framed = InputReader::new(&self.payload[self.offset..]);
        let record_len = framed.u32()? as usize;
        let mut record = framed.reader(record_len)?;
        let tag = record.u8()?;
        let (schema_key, row_pk, snapshot, effect) = match tag {
            0 => {
                let (schema_key, row_pk) = record.key()?;
                let effect = match record.u8()? {
                    0 => ChangeEffect::Content,
                    1 => ChangeEffect::FormatOnly,
                    _ => return Err(Error::invalid_input("unknown row change effect")),
                };
                (schema_key, row_pk, Some(record.snapshot()?), effect)
            }
            1 => {
                let (schema_key, row_pk) = record.key()?;
                (schema_key, row_pk, None, ChangeEffect::Content)
            }
            _ => {
                return Err(Error::invalid_input(
                    "row input page contains an unresolved create",
                ));
            }
        };
        record.finish()?;
        self.offset = self
            .offset
            .checked_add(4 + record_len)
            .ok_or_else(|| Error::limit_exceeded("row input page offset overflowed"))?;
        self.remaining -= 1;
        if self.remaining == 0 && self.offset != self.payload.len() {
            return Err(Error::invalid_input("row input page has trailing bytes"));
        }
        Ok(InputChange {
            ordinal,
            schema_key,
            row_pk,
            snapshot,
            effect,
        })
    }
}

#[derive(Debug)]
enum InputSnapshot {
    Inline(Vec<u8>),
    Attachment { ordinal: u32, length: u64 },
}

fn decode_input_page(mut bytes: Vec<u8>) -> Result<InputPage> {
    let page = WirePage::decode(&bytes)
        .map_err(|error| Error::invalid_input(format!("invalid row page: {error:?}")))?;
    let section = page
        .section()
        .map_err(|error| Error::invalid_input(format!("invalid row page: {error:?}")))?;
    if section.representation != Representation::Snapshots
        || section.operation != Operation::Mixed
        || !section.schema_key.is_empty()
        || !section.layout.is_empty()
    {
        return Err(Error::invalid_input(
            "row input page must use the mixed snapshot representation",
        ));
    }
    let payload_len = section.payload.len();
    let remaining = section.record_count;
    bytes.truncate(payload_len);
    Ok(InputPage {
        payload: bytes,
        offset: 0,
        remaining,
    })
}

struct InputReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> InputReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| Error::invalid_input("truncated row input page"))?;
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.exact(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.exact(4)?.try_into().expect("four bytes"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.exact(8)?.try_into().expect("eight bytes"),
        ))
    }

    fn text(&mut self) -> Result<String> {
        let length = self.u32()? as usize;
        String::from_utf8(self.exact(length)?.to_vec())
            .map_err(|_| Error::invalid_input("row input text is not UTF-8"))
    }

    fn key(&mut self) -> Result<(String, Vec<String>)> {
        let schema_key = self.text()?;
        let count = self.u32()? as usize;
        if count > self.bytes.len().saturating_sub(self.offset) / 4 {
            return Err(Error::invalid_input(
                "row primary key count exceeds page bounds",
            ));
        }
        let mut row_pk = Vec::with_capacity(count);
        for _ in 0..count {
            row_pk.push(self.text()?);
        }
        Ok((schema_key, row_pk))
    }

    fn snapshot(&mut self) -> Result<InputSnapshot> {
        match self.u8()? {
            0 => {
                let length = self.u32()? as usize;
                Ok(InputSnapshot::Inline(self.exact(length)?.to_vec()))
            }
            1 => Ok(InputSnapshot::Attachment {
                ordinal: self.u32()?,
                length: self.u64()?,
            }),
            _ => Err(Error::invalid_input("unknown row attachment tag")),
        }
    }

    fn reader(&mut self, length: usize) -> Result<InputReader<'a>> {
        Ok(InputReader::new(self.exact(length)?))
    }

    fn finish(self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(Error::invalid_input("row input page has trailing bytes"));
        }
        Ok(())
    }
}

fn read_row_attachment(
    source: &RowSource,
    ordinal: u32,
    length: u64,
    max_page_bytes: u32,
) -> Result<Vec<u8>> {
    const READ_BYTES: u32 = 1024 * 1024;
    let capacity = usize::try_from(length)
        .map_err(|_| Error::limit_exceeded("row snapshot exceeds guest address space"))?;
    let mut output = Vec::with_capacity(capacity);
    while output.len() < capacity {
        let offset = output.len() as u64;
        let chunk =
            u32::try_from((capacity - output.len()).min(READ_BYTES.min(max_page_bytes) as usize))
                .expect("bounded row snapshot read fits u32");
        let bytes = source
            .read_attachment(ordinal, offset, chunk)
            .map_err(|error| host_error("host row attachment read failed", error))?
            .ok_or_else(|| Error::invalid_input("host row attachment disappeared"))?;
        if bytes.is_empty() {
            return Err(Error::invalid_input(
                "host row attachment returned a short read",
            ));
        }
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}

#[derive(Debug)]
pub struct RowUpdate<'a> {
    pub before_path: String,
    pub before: Snapshot<'a>,
    pub changes: RowChangeReader<'a>,
}

#[derive(Debug)]
pub struct RestoreFile<'a> {
    pub accepted: Option<Snapshot<'a>>,
    pub rows: RowReader<'a>,
}

#[derive(Debug)]
pub struct ColdUpdate<'a> {
    pub before_path: String,
    pub after_path: String,
    pub before: Snapshot<'a>,
    pub edits: Vec<FileEdit>,
    pub rows: RowReader<'a>,
    pub creates: CreateContext,
}

pub trait Plugin: 'static {
    fn open(input: &OpenFile<'_>, output: &mut Output<'_>) -> Result<()>;

    fn file_changed(update: &FileUpdate<'_>, output: &mut Output<'_>) -> Result<()>;

    fn resolve_conflict(conflict: RowConflict<'_>) -> Result<ConflictResolution> {
        Ok(conflict.take_b_or_delete())
    }

    fn rows_changed(update: &mut RowUpdate<'_>, output: &mut Output<'_>) -> Result<()>;

    fn restore(input: &mut RestoreFile<'_>, output: &mut Output<'_>) -> Result<()>;

    /// Reconciles a successor directly from durable rows and accepted
    /// bytes when no warm guest document is available.
    ///
    /// This is mandatory: the host may choose the cold route after eviction,
    /// process restart, or cache pressure. A format must therefore preserve
    /// durable identitys without requiring hydrate followed by a second
    /// guest transition.
    fn cold_file_changed(update: &mut ColdUpdate<'_>, output: &mut Output<'_>) -> Result<()>;
}

#[doc(hidden)]
#[derive(Debug)]
pub struct Component<P>(PhantomData<P>);

impl<P: Plugin> Guest for Component<P> {
    fn apply(
        request: TransitionRequest,
        output: &WitTransition,
    ) -> std::result::Result<(), PluginError> {
        let mut sink = Output::new(output)?;
        match request {
            TransitionRequest::Open(request) => {
                let input = OpenFile {
                    path: request.path,
                    accepted: Snapshot {
                        inner: &request.accepted,
                    },
                    creates: CreateContext {
                        high: request.creates.high,
                        low: request.creates.low,
                    },
                };
                P::open(&input, &mut sink).map_err(plugin_error)?;
                sink.finish().map_err(plugin_error)
            }
            TransitionRequest::FileChanged(request) => {
                let input = FileUpdate {
                    before_path: request.before_path,
                    after_path: request.after_path,
                    before: Snapshot {
                        inner: &request.before,
                    },
                    edits: request
                        .edits
                        .into_iter()
                        .map(|edit| FileEdit {
                            offset: edit.offset,
                            delete_len: edit.delete_len,
                            insert: edit.insert,
                        })
                        .collect(),
                    creates: CreateContext {
                        high: request.creates.high,
                        low: request.creates.low,
                    },
                };
                P::file_changed(&input, &mut sink).map_err(plugin_error)?;
                sink.finish().map_err(plugin_error)
            }
            TransitionRequest::RowsChanged(input) => apply_rows::<P>(input, output),
            TransitionRequest::Restore(input) => apply_restore::<P>(input, output),
            TransitionRequest::ColdFileChanged(input) => apply_cold_update::<P>(input, output),
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
            let conflict = RowConflict {
                schema_key: meta.schema_key,
                row_pk: meta.row_pk,
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
                        .begin_replace(meta.ordinal, snapshot.len() as u64)
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
}

fn apply_rows<P: Plugin>(
    input: WitRowsChangedRequest,
    output: &WitTransition,
) -> std::result::Result<(), PluginError> {
    let max_batch_bytes = output.max_batch_bytes();
    let mut update = RowUpdate {
        before_path: input.before_path,
        before: Snapshot {
            inner: &input.before,
        },
        changes: RowChangeReader::new(&input.changes, max_batch_bytes),
    };
    let mut sink = Output::new(output)?;
    P::rows_changed(&mut update, &mut sink).map_err(plugin_error)?;
    sink.finish().map_err(plugin_error)
}

fn apply_restore<P: Plugin>(
    input: WitRestoreRequest,
    output: &WitTransition,
) -> std::result::Result<(), PluginError> {
    let max_batch_bytes = output.max_batch_bytes();
    let accepted = input
        .accepted
        .as_ref()
        .map(|accepted| Snapshot { inner: accepted });
    let mut input = RestoreFile {
        accepted,
        rows: RowReader::new(&input.rows, max_batch_bytes),
    };
    let mut sink = Output::new(output)?;
    P::restore(&mut input, &mut sink).map_err(plugin_error)?;
    sink.finish().map_err(plugin_error)
}

fn apply_cold_update<P: Plugin>(
    input: WitColdFileChangedRequest,
    output: &WitTransition,
) -> std::result::Result<(), PluginError> {
    let max_batch_bytes = output.max_batch_bytes();
    let mut input = ColdUpdate {
        before_path: input.before_path,
        after_path: input.after_path,
        before: Snapshot {
            inner: &input.before,
        },
        edits: input
            .edits
            .iter()
            .map(|edit| FileEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: edit.insert.clone(),
            })
            .collect(),
        rows: RowReader::new(&input.rows, max_batch_bytes),
        creates: CreateContext {
            high: input.creates.high,
            low: input.creates.low,
        },
    };
    let mut sink = Output::new(output)?;
    P::cold_file_changed(&mut input, &mut sink).map_err(plugin_error)?;
    sink.finish().map_err(plugin_error)
}

#[doc(hidden)]
#[macro_export]
macro_rules! __lix_export_plugin {
    ($plugin:ty) => {
        type __LixPluginComponent = $crate::plugin::Component<$plugin>;
        $crate::plugin::api::__export_plugin_component!(
            __LixPluginComponent with_types_in $crate::plugin::api
        );
    };
}
