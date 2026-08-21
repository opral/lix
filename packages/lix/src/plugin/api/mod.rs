//! Row-first authoring layer for Lix's Component API v2.

#![allow(
    clippy::missing_errors_doc,
    trivial_numeric_casts,
    unused_qualifications
)]

#[doc(hidden)]
pub mod column_merger_bindings {
    include!(concat!(env!("OUT_DIR"), "/column_merger_bindings.rs"));

    pub use super::combined_bindings::lix;
}

#[doc(hidden)]
pub mod combined_bindings {
    include!(concat!(env!("OUT_DIR"), "/combined_bindings.rs"));
}

#[doc(hidden)]
pub mod file_projection_bindings {
    include!(concat!(env!("OUT_DIR"), "/file_projection_bindings.rs"));

    pub use super::combined_bindings::lix;
}

use self::combined_bindings::exports::lix::plugin::column_merger::Guest as CombinedColumnMergerGuest;
use self::combined_bindings::exports::lix::plugin::file_projection::Guest as CombinedFileProjectionGuest;
use self::combined_bindings::lix::plugin::host::{
    ColumnMergeSink, ColumnMergeSource, Transition as WitTransition,
};
use self::combined_bindings::lix::plugin::types::{
    ParseChangesRequest as WitParseChangesRequest, ParseRequest as WitParseRequest, PluginError,
    SerializeChangesRequest as WitSerializeChangesRequest, SerializeRequest as WitSerializeRequest,
};
use super::wire::typed::{self, ChangeEffect as TypedChangeEffect, Mutation as TypedMutation};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

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

/// Schema v1 typed values used at the plugin boundary. JSONB values remain
/// JSON values, but the surrounding row never becomes a JSON object.
pub type TypedRow = lix_schema::Row;
pub type TypedValue = lix_schema::Value;

/// Computes the fingerprint carried by every typed-row page for one Schema v1
/// definition. Plugins normally pass an embedded schema document here.
pub fn schema_fingerprint(schema_json: &str) -> Result<[u8; 32]> {
    let schema = lix_schema::from_json(schema_json)
        .map_err(|error| Error::invalid_input(format!("invalid Schema v1 definition: {error}")))?;
    Ok(*schema
        .wire_fingerprint()
        .map_err(|error| Error::invalid_input(format!("failed to fingerprint schema: {error}")))?
        .as_bytes())
}

fn plugin_error(error: Error) -> PluginError {
    match error {
        Error::InvalidInput(message) => PluginError::InvalidInput(message),
        Error::LimitExceeded(message) => PluginError::LimitExceeded(message),
        Error::Internal(message) => PluginError::Internal(message),
    }
}

fn column_plugin_error(error: Error) -> column_merger_bindings::lix::plugin::types::PluginError {
    use column_merger_bindings::lix::plugin::types::PluginError as Target;
    match error {
        Error::InvalidInput(message) => Target::InvalidInput(message),
        Error::LimitExceeded(message) => Target::LimitExceeded(message),
        Error::Internal(message) => Target::Internal(message),
    }
}

fn projection_plugin_error(
    error: Error,
) -> file_projection_bindings::lix::plugin::types::PluginError {
    use file_projection_bindings::lix::plugin::types::PluginError as Target;
    match error {
        Error::InvalidInput(message) => Target::InvalidInput(message),
        Error::LimitExceeded(message) => Target::LimitExceeded(message),
        Error::Internal(message) => Target::Internal(message),
    }
}

#[derive(Debug)]
enum CommonHostError {
    InvalidRange,
    LimitExceeded(String),
    Rejected(String),
}

fn host_error(context: &str, error: CommonHostError) -> Error {
    match error {
        CommonHostError::InvalidRange => Error::invalid_input(format!("{context}: invalid range")),
        CommonHostError::LimitExceeded(message) => {
            Error::limit_exceeded(format!("{context}: {message}"))
        }
        CommonHostError::Rejected(message) => Error::invalid_input(format!("{context}: {message}")),
    }
}

#[derive(Debug)]
struct CommonRecordChunk {
    total_len: u64,
    bytes: Vec<u8>,
}

trait SnapshotHost {
    fn file_len(&self) -> u64;
    fn read_file(&self, offset: u64, length: u32) -> std::result::Result<Vec<u8>, CommonHostError>;
    fn read_state(
        &self,
        key: &[u8],
        offset: u64,
        max_bytes: u32,
    ) -> std::result::Result<Option<CommonRecordChunk>, CommonHostError>;
}

trait RowSourceHost {
    fn next_page(
        &self,
        max_bytes: u32,
    ) -> std::result::Result<Option<(Vec<u8>, Vec<Vec<u8>>)>, CommonHostError>;
}

trait TransitionHost {
    fn max_batch_bytes(&self) -> u32;
    fn put_state(&self, key: &[u8], value: &[u8]) -> std::result::Result<(), CommonHostError>;
    fn delete_state(&self, key: &[u8]) -> std::result::Result<(), CommonHostError>;
    fn emit_rows(
        &self,
        payload: Vec<u8>,
        attachments: Vec<Vec<u8>>,
    ) -> std::result::Result<(), CommonHostError>;
    fn replace_all_rows(&self) -> std::result::Result<(), CommonHostError>;
    fn emit_file_edit(
        &self,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
    ) -> std::result::Result<(), CommonHostError>;
    fn begin_file_replacement(&self, length: u64) -> std::result::Result<(), CommonHostError>;
    fn write_file_replacement(&self, chunk: &[u8]) -> std::result::Result<(), CommonHostError>;
    fn finish_file_replacement(&self) -> std::result::Result<(), CommonHostError>;
}

#[derive(Debug)]
struct CommonColumnMergeMeta {
    ordinal: u32,
    schema_key: String,
    primary_key: Vec<Vec<u8>>,
    schema_fingerprint: Vec<u8>,
    file_id: Option<String>,
    column: String,
    base_len: Option<u64>,
    a_len: Option<u64>,
    b_len: Option<u64>,
    base_row_len: u64,
    a_row_len: u64,
    b_row_len: u64,
}

trait ColumnMergeSourceHost {
    fn len(&self) -> u32;
    fn get(&self, index: u32) -> std::result::Result<CommonColumnMergeMeta, CommonHostError>;
    fn read_value(
        &self,
        index: u32,
        side: MergeSide,
        offset: u64,
        length: u32,
    ) -> std::result::Result<Option<Vec<u8>>, CommonHostError>;
    fn read_row(
        &self,
        index: u32,
        side: MergeSide,
        offset: u64,
        length: u32,
    ) -> std::result::Result<Vec<u8>, CommonHostError>;
}

trait ColumnMergeSinkHost {
    fn max_batch_bytes(&self) -> u32;
    fn use_lww(&self, ordinal: u32) -> std::result::Result<(), CommonHostError>;
    fn begin_replace(
        &self,
        ordinal: u32,
        length: Option<u64>,
    ) -> std::result::Result<(), CommonHostError>;
    fn write_replacement(&self, chunk: &[u8]) -> std::result::Result<(), CommonHostError>;
    fn finish_replace(&self) -> std::result::Result<(), CommonHostError>;
}

macro_rules! impl_projection_hosts {
    ($bindings:ident) => {
        fn map_host_error(error: $bindings::lix::plugin::host::HostError) -> CommonHostError {
            match error {
                $bindings::lix::plugin::host::HostError::InvalidRange => {
                    CommonHostError::InvalidRange
                }
                $bindings::lix::plugin::host::HostError::LimitExceeded(message) => {
                    CommonHostError::LimitExceeded(message)
                }
                $bindings::lix::plugin::host::HostError::Rejected(message) => {
                    CommonHostError::Rejected(message)
                }
            }
        }

        impl SnapshotHost for $bindings::lix::plugin::host::Snapshot {
            fn file_len(&self) -> u64 {
                self.file_len()
            }
            fn read_file(
                &self,
                offset: u64,
                length: u32,
            ) -> std::result::Result<Vec<u8>, CommonHostError> {
                self.read_file(offset, length).map_err(map_host_error)
            }
            fn read_state(
                &self,
                key: &[u8],
                offset: u64,
                max_bytes: u32,
            ) -> std::result::Result<Option<CommonRecordChunk>, CommonHostError> {
                self.read_state(key, offset, max_bytes)
                    .map(|chunk| {
                        chunk.map(|chunk| CommonRecordChunk {
                            total_len: chunk.total_len,
                            bytes: chunk.bytes,
                        })
                    })
                    .map_err(map_host_error)
            }
        }

        impl RowSourceHost for $bindings::lix::plugin::host::RowSource {
            fn next_page(
                &self,
                max_bytes: u32,
            ) -> std::result::Result<Option<(Vec<u8>, Vec<Vec<u8>>)>, CommonHostError> {
                self.next_page(max_bytes)
                    .map(|page| page.map(|page| (page.payload, page.attachments)))
                    .map_err(map_host_error)
            }
        }

        impl TransitionHost for $bindings::lix::plugin::host::Transition {
            fn max_batch_bytes(&self) -> u32 {
                self.max_batch_bytes()
            }
            fn put_state(
                &self,
                key: &[u8],
                value: &[u8],
            ) -> std::result::Result<(), CommonHostError> {
                self.put_state(key, value).map_err(map_host_error)
            }
            fn delete_state(&self, key: &[u8]) -> std::result::Result<(), CommonHostError> {
                self.delete_state(key).map_err(map_host_error)
            }
            fn emit_rows(
                &self,
                payload: Vec<u8>,
                attachments: Vec<Vec<u8>>,
            ) -> std::result::Result<(), CommonHostError> {
                self.emit_rows(&$bindings::lix::plugin::host::RowPage {
                    payload,
                    attachments,
                })
                .map_err(map_host_error)
            }
            fn replace_all_rows(&self) -> std::result::Result<(), CommonHostError> {
                self.replace_all_rows().map_err(map_host_error)
            }
            fn emit_file_edit(
                &self,
                offset: u64,
                delete_len: u64,
                insert: &[u8],
            ) -> std::result::Result<(), CommonHostError> {
                self.emit_file_edit(&$bindings::lix::plugin::host::FileEdit {
                    offset,
                    delete_len,
                    insert: insert.to_vec(),
                })
                .map_err(map_host_error)
            }
            fn begin_file_replacement(
                &self,
                length: u64,
            ) -> std::result::Result<(), CommonHostError> {
                self.begin_file_replacement(length).map_err(map_host_error)
            }
            fn write_file_replacement(
                &self,
                chunk: &[u8],
            ) -> std::result::Result<(), CommonHostError> {
                self.write_file_replacement(chunk).map_err(map_host_error)
            }
            fn finish_file_replacement(&self) -> std::result::Result<(), CommonHostError> {
                self.finish_file_replacement().map_err(map_host_error)
            }
        }
    };
}

mod combined_hosts {
    use super::*;
    impl_projection_hosts!(combined_bindings);
}

macro_rules! impl_column_hosts {
    ($bindings:ident) => {
        fn map_host_error(error: $bindings::lix::plugin::host::HostError) -> CommonHostError {
            match error {
                $bindings::lix::plugin::host::HostError::InvalidRange => {
                    CommonHostError::InvalidRange
                }
                $bindings::lix::plugin::host::HostError::LimitExceeded(message) => {
                    CommonHostError::LimitExceeded(message)
                }
                $bindings::lix::plugin::host::HostError::Rejected(message) => {
                    CommonHostError::Rejected(message)
                }
            }
        }
        fn side(side: MergeSide) -> $bindings::lix::plugin::host::MergeSide {
            match side {
                MergeSide::Base => $bindings::lix::plugin::host::MergeSide::Base,
                MergeSide::A => $bindings::lix::plugin::host::MergeSide::A,
                MergeSide::B => $bindings::lix::plugin::host::MergeSide::B,
            }
        }
        impl ColumnMergeSourceHost for $bindings::lix::plugin::host::ColumnMergeSource {
            fn len(&self) -> u32 {
                self.len()
            }
            fn get(
                &self,
                index: u32,
            ) -> std::result::Result<CommonColumnMergeMeta, CommonHostError> {
                self.get(index)
                    .map(|meta| CommonColumnMergeMeta {
                        ordinal: meta.ordinal,
                        schema_key: meta.schema_key,
                        primary_key: meta.primary_key,
                        schema_fingerprint: meta.schema_fingerprint,
                        file_id: meta.file_id,
                        column: meta.column,
                        base_len: meta.base_len,
                        a_len: meta.a_len,
                        b_len: meta.b_len,
                        base_row_len: meta.base_row_len,
                        a_row_len: meta.a_row_len,
                        b_row_len: meta.b_row_len,
                    })
                    .map_err(map_host_error)
            }
            fn read_value(
                &self,
                index: u32,
                merge_side: MergeSide,
                offset: u64,
                length: u32,
            ) -> std::result::Result<Option<Vec<u8>>, CommonHostError> {
                self.read_value(index, side(merge_side), offset, length)
                    .map_err(map_host_error)
            }
            fn read_row(
                &self,
                index: u32,
                merge_side: MergeSide,
                offset: u64,
                length: u32,
            ) -> std::result::Result<Vec<u8>, CommonHostError> {
                self.read_row(index, side(merge_side), offset, length)
                    .map_err(map_host_error)
            }
        }
        impl ColumnMergeSinkHost for $bindings::lix::plugin::host::ColumnMergeSink {
            fn max_batch_bytes(&self) -> u32 {
                self.max_batch_bytes()
            }
            fn use_lww(&self, ordinal: u32) -> std::result::Result<(), CommonHostError> {
                self.use_lww(ordinal).map_err(map_host_error)
            }
            fn begin_replace(
                &self,
                ordinal: u32,
                length: Option<u64>,
            ) -> std::result::Result<(), CommonHostError> {
                self.begin_replace(ordinal, length).map_err(map_host_error)
            }
            fn write_replacement(&self, chunk: &[u8]) -> std::result::Result<(), CommonHostError> {
                self.write_replacement(chunk).map_err(map_host_error)
            }
            fn finish_replace(&self) -> std::result::Result<(), CommonHostError> {
                self.finish_replace().map_err(map_host_error)
            }
        }
    };
}

mod combined_column_hosts {
    use super::*;
    impl_column_hosts!(combined_bindings);
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

    /// Returns the deterministic UUID reserved for a transition-local create.
    pub fn id(self, local_ref: u32) -> uuid::Uuid {
        let mut bytes = [0_u8; 16];
        bytes[..12].copy_from_slice(&self.namespace_bytes());
        bytes[12..].copy_from_slice(&local_ref.to_be_bytes());
        uuid::Uuid::from_bytes(bytes)
    }
}

/// Immutable source for accepted file bytes and private plugin state.
pub struct Snapshot<'a> {
    inner: &'a dyn SnapshotHost,
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

struct TypedPageBuffer {
    schema_key: String,
    schema_fingerprint: [u8; 32],
    columns: Vec<std::sync::Arc<str>>,
    payload: Vec<u8>,
    attachments: Vec<Vec<u8>>,
    attachment_bytes: usize,
    record_count: u32,
}

impl TypedPageBuffer {
    fn new(schema_key: &str, schema_fingerprint: [u8; 32]) -> Self {
        Self {
            schema_key: schema_key.to_owned(),
            schema_fingerprint,
            columns: Vec::new(),
            payload: Vec::new(),
            attachments: Vec::new(),
            attachment_bytes: 0,
            record_count: 0,
        }
    }

    fn begin(&mut self, row: Option<&TypedRow>) -> Result<()> {
        self.columns = row.map_or_else(Vec::new, |row| row.shared_keys().cloned().collect());
        self.payload = typed::begin_page_payload(&self.columns)
            .map_err(|error| Error::invalid_input(format!("invalid typed row page: {error:?}")))?;
        Ok(())
    }

    fn buffered_bytes(&self) -> Option<usize> {
        super::wire::typed_page_overhead(&self.schema_key)
            .ok()?
            .checked_add(self.payload.len())?
            .checked_add(self.attachment_bytes)?
            .checked_add(
                self.attachments
                    .len()
                    .checked_mul(typed::ATTACHMENT_TABLE_ENTRY_BYTES)?,
            )
    }
}

struct TransitionOutput<'a> {
    inner: &'a dyn TransitionHost,
    max_page_bytes: u32,
    max_batch_bytes: u32,
    typed_page_target_bytes: u32,
    typed_pages: Vec<TypedPageBuffer>,
    typed_active_page: Option<usize>,
    coalesce_typed_schemas: bool,
}

impl std::fmt::Debug for TransitionOutput<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransitionOutput")
            .field("max_batch_bytes", &self.max_batch_bytes)
            .finish_non_exhaustive()
    }
}

impl<'a> TransitionOutput<'a> {
    fn buffered_typed_bytes(&self) -> Option<usize> {
        self.typed_pages.iter().try_fold(0usize, |total, page| {
            total.checked_add(page.buffered_bytes()?)
        })
    }

    fn new(inner: &'a dyn TransitionHost) -> std::result::Result<Self, Error> {
        let max_page_bytes = inner.max_batch_bytes();
        let page_overhead = super::wire::typed_page_overhead("")
            .map_err(|error| Error::internal(format!("invalid typed page overhead: {error:?}")))?
            as u32;
        if max_page_bytes <= page_overhead {
            return Err(Error::limit_exceeded(
                "max-batch-bytes cannot hold a row page".to_owned(),
            ));
        }
        Ok(TransitionOutput {
            inner,
            max_page_bytes,
            max_batch_bytes: max_page_bytes,
            // Cold-file admission may raise the hard batch ceiling to carry
            // one legitimately large record. Keep ordinary typed rows on the
            // normal 1 MiB schedule so a large source file does not make the
            // guest retain multi-megabyte aggregate page buffers.
            typed_page_target_bytes: max_page_bytes.min(1024 * 1024),
            typed_pages: Vec::new(),
            typed_active_page: None,
            coalesce_typed_schemas: false,
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

    fn replace_all_rows(&mut self) -> Result<()> {
        self.flush_typed_rows()?;
        self.inner
            .replace_all_rows()
            .map_err(|error| host_error("host rejected complete row replacement", error))
    }

    fn file_edit(&mut self, offset: u64, delete_len: u64, insert: &[u8]) -> Result<()> {
        self.flush_typed_rows()?;
        self.inner
            .emit_file_edit(offset, delete_len, insert)
            .map_err(|error| host_error("host rejected file edit", error))
    }

    pub fn replace_file(&mut self, bytes: &[u8]) -> Result<()> {
        self.flush_typed_rows()?;
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

    fn typed_row(
        &mut self,
        schema_key: &str,
        schema_fingerprint: [u8; 32],
        mutation: TypedMutation<'_>,
    ) -> Result<()> {
        let row = match &mutation {
            TypedMutation::Create { row, .. } | TypedMutation::Upsert { row, .. } => Some(*row),
            TypedMutation::Delete { .. } => None,
        };
        let page_index = if let Some(index) = self.typed_pages.iter().position(|page| {
            page.schema_key == schema_key && page.schema_fingerprint == schema_fingerprint
        }) {
            index
        } else {
            if self.coalesce_typed_schemas && self.typed_pages.len() >= 16 {
                return Err(Error::limit_exceeded(
                    "schema-coalesced output exceeds 16 page buffers",
                ));
            }
            self.typed_pages
                .push(TypedPageBuffer::new(schema_key, schema_fingerprint));
            self.typed_pages.len() - 1
        };
        if let Some(active_page) = self.typed_active_page
            && active_page != page_index
            && !self.coalesce_typed_schemas
        {
            self.flush_typed_page(active_page)?;
        }
        self.typed_active_page = Some(page_index);
        // A delete-only page has no row from which to derive its canonical
        // column layout. Finish it before the first complete row rather than
        // retaining names on every record.
        let must_flush = {
            let page = &self.typed_pages[page_index];
            let layout_changed = row.is_some_and(|row| {
                row.len() != page.columns.len()
                    || row
                        .shared_keys()
                        .zip(&page.columns)
                        .any(|(actual, expected)| {
                            !std::sync::Arc::ptr_eq(actual, expected) && actual != expected
                        })
            });
            page.record_count == typed::MAX_RECORDS_PER_PAGE
                || (page.record_count > 0 && layout_changed)
        };
        if must_flush {
            self.flush_typed_page(page_index)?;
            self.typed_active_page = Some(page_index);
        }
        if self.typed_pages[page_index].record_count == 0 {
            self.typed_pages[page_index].begin(row)?;
        }

        let buffered_records_before = if self.coalesce_typed_schemas {
            self.typed_pages
                .iter()
                .map(|page| u64::from(page.record_count))
                .sum::<u64>()
        } else {
            0
        };
        let (payload_checkpoint, attachment_checkpoint, attachment_bytes_checkpoint) = {
            let page = &mut self.typed_pages[page_index];
            let payload_checkpoint = page.payload.len();
            let attachment_checkpoint = page.attachments.len();
            let attachment_bytes_checkpoint = page.attachment_bytes;
            if let Err(error) = typed::append_mutation(
                &mut page.payload,
                &mut page.attachments,
                &mutation,
                &page.columns,
            ) {
                page.payload.truncate(payload_checkpoint);
                page.attachments.truncate(attachment_checkpoint);
                return Err(Error::invalid_input(format!(
                    "invalid typed row page: {error:?}"
                )));
            }
            page.attachment_bytes = page.attachments[attachment_checkpoint..]
                .iter()
                .try_fold(attachment_bytes_checkpoint, |total, attachment| {
                    total.checked_add(attachment.len())
                })
                .unwrap_or(usize::MAX);
            page.record_count += 1;
            (
                payload_checkpoint,
                attachment_checkpoint,
                attachment_bytes_checkpoint,
            )
        };
        let page_overfull = self.typed_pages[page_index]
            .buffered_bytes()
            .is_none_or(|bytes| bytes > self.typed_page_target_bytes as usize)
            && self.typed_pages[page_index].record_count > 1;
        let aggregate_overfull = self.coalesce_typed_schemas
            && buffered_records_before > 0
            && self
                .buffered_typed_bytes()
                .is_none_or(|bytes| bytes > self.typed_page_target_bytes as usize);
        if page_overfull || aggregate_overfull {
            let page = &mut self.typed_pages[page_index];
            page.payload.truncate(payload_checkpoint);
            page.attachments.truncate(attachment_checkpoint);
            page.attachment_bytes = attachment_bytes_checkpoint;
            page.record_count -= 1;
            if aggregate_overfull {
                self.flush_typed_rows()?;
            } else {
                self.flush_typed_page(page_index)?;
            }
            self.typed_active_page = Some(page_index);
            let page = &mut self.typed_pages[page_index];
            page.begin(row)?;
            typed::append_mutation(
                &mut page.payload,
                &mut page.attachments,
                &mutation,
                &page.columns,
            )
            .map_err(|error| Error::invalid_input(format!("invalid typed row page: {error:?}")))?;
            page.attachment_bytes = page
                .attachments
                .iter()
                .try_fold(0usize, |total, attachment| {
                    total.checked_add(attachment.len())
                })
                .ok_or_else(|| Error::limit_exceeded("typed row attachment bytes overflowed"))?;
            page.record_count = 1;
        }
        Ok(())
    }

    fn flush_typed_page(&mut self, page_index: usize) -> Result<()> {
        let page = &mut self.typed_pages[page_index];
        if page.record_count == 0 {
            return Ok(());
        }
        let payload = std::mem::take(&mut page.payload);
        page.columns.clear();
        let attachments = std::mem::take(&mut page.attachments);
        page.attachment_bytes = 0;
        let record_count = std::mem::take(&mut page.record_count);
        let (page, attachments) = typed::finish_page_parts(
            &page.schema_key,
            &page.schema_fingerprint,
            record_count,
            payload,
            attachments,
        )
        .map_err(|error| Error::invalid_input(format!("invalid typed row page: {error:?}")))?;
        self.inner
            .emit_rows(page, attachments)
            .map_err(|error| host_error("host rejected typed row page", error))?;
        if self.typed_active_page == Some(page_index) {
            self.typed_active_page = None;
        }
        Ok(())
    }

    fn flush_typed_rows(&mut self) -> Result<()> {
        for page_index in 0..self.typed_pages.len() {
            self.flush_typed_page(page_index)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.flush_typed_rows()
    }
}

/// Complete rows produced by [`FileProjection::parse`] or cold reconciliation
/// in [`FileProjection::parse_changes`].
#[derive(Debug)]
pub struct RowOutput<'output, 'host> {
    inner: &'output mut TransitionOutput<'host>,
}

impl RowOutput<'_, '_> {
    /// Groups initial complete rows into one bounded page stream per schema.
    ///
    /// This explicitly changes emission order from call order to first-seen
    /// schema order and is therefore available only for initial complete-row
    /// projections, whose output is a set. Sparse change streams retain exact
    /// guest order. At most sixteen schema buffers may be active.
    pub fn coalesce_schema_pages(&mut self) -> Result<()> {
        if self
            .inner
            .typed_pages
            .iter()
            .any(|page| page.record_count > 0)
        {
            return Err(Error::invalid_input(
                "schema page coalescing must be enabled before emitting rows",
            ));
        }
        self.inner.coalesce_typed_schemas = true;
        Ok(())
    }

    pub fn create(
        &mut self,
        schema_key: &str,
        schema_fingerprint: [u8; 32],
        local_ref: u32,
        row: &TypedRow,
    ) -> Result<()> {
        self.inner.typed_row(
            schema_key,
            schema_fingerprint,
            TypedMutation::Create { local_ref, row },
        )
    }

    pub fn upsert(
        &mut self,
        schema_key: &str,
        schema_fingerprint: [u8; 32],
        primary_key: Vec<TypedValue>,
        row: &TypedRow,
    ) -> Result<()> {
        self.inner.typed_row(
            schema_key,
            schema_fingerprint,
            TypedMutation::Upsert {
                row_pk: &primary_key,
                row,
                effect: TypedChangeEffect::Content,
            },
        )
    }

    /// Emits the currently buffered typed rows as one page. This is useful for
    /// large, single-schema outputs that need a tighter guest-memory bound than
    /// the host's maximum page size.
    pub fn flush_page(&mut self) -> Result<()> {
        self.inner.flush_typed_rows()
    }

    pub fn put_state(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put_state(key, value)
    }

    pub fn delete_state(&mut self, key: &[u8]) -> Result<()> {
        self.inner.delete_state(key)
    }
}

/// Sparse row mutations produced from sparse file edits.
#[derive(Debug)]
pub struct RowChangeOutput<'output, 'host> {
    inner: &'output mut TransitionOutput<'host>,
}

impl<'host> RowChangeOutput<'_, 'host> {
    pub fn create(
        &mut self,
        schema_key: &str,
        schema_fingerprint: [u8; 32],
        local_ref: u32,
        row: &TypedRow,
    ) -> Result<()> {
        self.inner.typed_row(
            schema_key,
            schema_fingerprint,
            TypedMutation::Create { local_ref, row },
        )
    }

    pub fn upsert(
        &mut self,
        schema_key: &str,
        schema_fingerprint: [u8; 32],
        primary_key: Vec<TypedValue>,
        row: &TypedRow,
        effect: ChangeEffect,
    ) -> Result<()> {
        self.inner.typed_row(
            schema_key,
            schema_fingerprint,
            TypedMutation::Upsert {
                row_pk: &primary_key,
                row,
                effect: match effect {
                    ChangeEffect::Content => TypedChangeEffect::Content,
                    ChangeEffect::FormatOnly => TypedChangeEffect::FormatOnly,
                },
            },
        )
    }

    pub fn delete(
        &mut self,
        schema_key: &str,
        schema_fingerprint: [u8; 32],
        primary_key: Vec<TypedValue>,
    ) -> Result<()> {
        self.inner.typed_row(
            schema_key,
            schema_fingerprint,
            TypedMutation::Delete {
                row_pk: &primary_key,
            },
        )
    }

    /// Emits the currently buffered typed mutations as one page.
    pub fn flush_page(&mut self) -> Result<()> {
        self.inner.flush_typed_rows()
    }

    pub fn replace_all_rows(&mut self) -> Result<RowOutput<'_, 'host>> {
        self.inner.replace_all_rows()?;
        Ok(RowOutput { inner: self.inner })
    }

    pub fn put_state(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put_state(key, value)
    }

    pub fn delete_state(&mut self, key: &[u8]) -> Result<()> {
        self.inner.delete_state(key)
    }
}

/// Complete file bytes produced from complete rows.
#[derive(Debug)]
pub struct FileOutput<'output, 'host> {
    inner: &'output mut TransitionOutput<'host>,
}

impl FileOutput<'_, '_> {
    pub fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.inner.replace_file(bytes)
    }

    pub fn put_state(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put_state(key, value)
    }

    pub fn delete_state(&mut self, key: &[u8]) -> Result<()> {
        self.inner.delete_state(key)
    }
}

/// Sparse byte edits produced from sparse row mutations.
#[derive(Debug)]
pub struct FileEditOutput<'output, 'host> {
    inner: &'output mut TransitionOutput<'host>,
}

impl FileEditOutput<'_, '_> {
    pub fn replace(&mut self, offset: u64, delete_len: u64, insert: &[u8]) -> Result<()> {
        self.inner.file_edit(offset, delete_len, insert)
    }

    pub fn replace_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.inner.replace_file(bytes)
    }

    pub fn put_state(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put_state(key, value)
    }

    pub fn delete_state(&mut self, key: &[u8]) -> Result<()> {
        self.inner.delete_state(key)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileEdit {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MergeSide {
    Base,
    A,
    B,
}

pub struct ColumnValue<'a> {
    source: &'a dyn ColumnMergeSourceHost,
    index: u32,
    side: MergeSide,
    len: Option<u64>,
}

impl std::fmt::Debug for ColumnValue<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ColumnValue")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl ColumnValue<'_> {
    pub fn len(&self) -> Option<u64> {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == Some(0)
    }

    pub fn is_missing(&self) -> bool {
        self.len.is_none()
    }

    fn read_bytes(&self) -> Result<Option<Vec<u8>>> {
        const READ_BYTES: u32 = 1024 * 1024;
        let Some(len) = self.len else {
            return Ok(None);
        };
        let capacity = usize::try_from(len)
            .map_err(|_| Error::limit_exceeded("column value exceeds guest address space"))?;
        let mut output = Vec::with_capacity(capacity);
        while output.len() < capacity {
            let offset = output.len() as u64;
            let length = u32::try_from((capacity - output.len()).min(READ_BYTES as usize))
                .expect("bounded conflict read fits u32");
            let bytes = self
                .source
                .read_value(self.index, self.side, offset, length)
                .map_err(|error| host_error("host conflict read failed", error))?
                .ok_or_else(|| Error::invalid_input("host column value disappeared"))?;
            if bytes.is_empty() {
                return Err(Error::invalid_input(
                    "host column value returned a short read",
                ));
            }
            output.extend_from_slice(&bytes);
        }
        Ok(Some(output))
    }

    /// Reads the native Schema v1 value supplied by the host. A missing
    /// optional column is `None`; an explicit null value is
    /// `Some(TypedValue::Null)`.
    pub fn value(&self) -> Result<Option<TypedValue>> {
        let Some(bytes) = self.read_bytes()? else {
            return Ok(None);
        };
        typed::decode_value_bytes(&bytes)
            .map(Some)
            .map_err(|error| Error::invalid_input(format!("invalid typed column value: {error:?}")))
    }
}

#[derive(Debug)]
pub struct RowIdentity {
    pub schema_key: String,
    pub schema_fingerprint: [u8; 32],
    /// Native Schema v1 primary-key components in schema declaration order.
    pub primary_key: Vec<TypedValue>,
    pub file_id: Option<String>,
}

/// One complete native row payload supplied as merge context.
pub struct RowPayload<'a> {
    source: &'a dyn ColumnMergeSourceHost,
    index: u32,
    side: MergeSide,
    len: u64,
}

impl std::fmt::Debug for RowPayload<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RowPayload")
            .field("len", &self.len)
            .finish()
    }
}

impl RowPayload<'_> {
    fn read_bytes(&self) -> Result<Vec<u8>> {
        const READ_BYTES: u32 = 1024 * 1024;
        let capacity = usize::try_from(self.len)
            .map_err(|_| Error::limit_exceeded("row payload exceeds guest address space"))?;
        let mut output = Vec::with_capacity(capacity);
        while output.len() < capacity {
            let length = u32::try_from((capacity - output.len()).min(READ_BYTES as usize))
                .expect("bounded row read fits u32");
            let bytes = self
                .source
                .read_row(self.index, self.side, output.len() as u64, length)
                .map_err(|error| host_error("host row context read failed", error))?;
            if bytes.is_empty() {
                return Err(Error::invalid_input(
                    "host row context returned a short read",
                ));
            }
            output.extend_from_slice(&bytes);
        }
        Ok(output)
    }

    pub fn typed(&self) -> Result<TypedRow> {
        typed::decode_row_bytes(&self.read_bytes()?)
            .map_err(|error| Error::invalid_input(format!("invalid typed row context: {error:?}")))
    }
}

#[derive(Debug)]
pub struct RowPayloadVersions<'a> {
    pub base: RowPayload<'a>,
    pub a: RowPayload<'a>,
    pub b: RowPayload<'a>,
}

#[derive(Debug)]
pub struct ColumnMerge<'a> {
    pub row: RowIdentity,
    pub column: String,
    pub base: ColumnValue<'a>,
    pub a: ColumnValue<'a>,
    pub b: ColumnValue<'a>,
    pub row_payloads: RowPayloadVersions<'a>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OwnedColumnValue {
    Typed(TypedValue),
    Missing,
}

impl OwnedColumnValue {
    pub fn typed(value: &TypedValue) -> Result<Self> {
        Ok(Self::Typed(value.clone()))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnMergeResult {
    UseLww,
    Replace(OwnedColumnValue),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeEffect {
    Content,
    FormatOnly,
}

/// One complete Schema v1 row delivered to a plugin without an outer JSON
/// representation. The fingerprint lets a plugin select the exact schema
/// plan before interpreting the typed values.
#[derive(Clone, Debug, PartialEq)]
pub struct TypedRowRecord {
    pub schema_key: Arc<str>,
    pub schema_fingerprint: [u8; 32],
    /// Native Schema v1 primary-key components in schema declaration order.
    pub primary_key: Vec<TypedValue>,
    pub row: TypedRow,
}

/// One typed sparse row change. Creates carry their host-local reference and
/// the complete row; upserts carry the typed identity and row; deletes carry
/// only the typed identity.
#[derive(Clone, Debug, PartialEq)]
pub struct TypedRowChange {
    pub schema_key: Arc<str>,
    pub schema_fingerprint: [u8; 32],
    /// Native Schema v1 primary-key components in schema declaration order.
    /// Creates leave this empty until the host resolves generated identities.
    pub primary_key: Vec<TypedValue>,
    pub row: Option<TypedRow>,
    pub local_ref: Option<u32>,
    pub effect: ChangeEffect,
}

#[derive(Debug)]
struct TypedInputPage {
    schema_key: Arc<str>,
    schema_fingerprint: [u8; 32],
    mutations: VecDeque<typed::OwnedMutation>,
}

/// Reads complete typed rows from host pages and intentionally accepts only
/// upserts, since durable current state cannot contain creates or tombstones.
pub struct TypedRowReader<'a> {
    changes: TypedRowChangeReader<'a>,
}

impl std::fmt::Debug for TypedRowReader<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TypedRowReader")
            .field("changes", &self.changes)
            .finish()
    }
}

impl TypedRowReader<'_> {
    fn new(source: &dyn RowSourceHost, max_page_bytes: u32) -> TypedRowReader<'_> {
        TypedRowReader {
            changes: TypedRowChangeReader::new(source, max_page_bytes),
        }
    }

    pub fn next(&mut self) -> Result<Option<TypedRowRecord>> {
        let Some(change) = self.changes.next()? else {
            return Ok(None);
        };
        let row = change.row.ok_or_else(|| {
            Error::invalid_input("durable typed rows cannot contain creates or tombstones")
        })?;
        if change.local_ref.is_some() {
            return Err(Error::invalid_input(
                "durable typed rows cannot contain create references",
            ));
        }
        Ok(Some(TypedRowRecord {
            schema_key: change.schema_key,
            schema_fingerprint: change.schema_fingerprint,
            primary_key: change.primary_key,
            row,
        }))
    }
}

/// Reads typed sparse changes from host pages. A page is decoded once and its
/// owned values are retained only until the plugin consumes the page.
pub struct TypedRowChangeReader<'a> {
    source: &'a dyn RowSourceHost,
    page: Option<TypedInputPage>,
    max_page_bytes: u32,
    eof: bool,
}

impl std::fmt::Debug for TypedRowChangeReader<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TypedRowChangeReader")
            .field(
                "buffered",
                &self.page.as_ref().map_or(0, |page| page.mutations.len()),
            )
            .field("max_page_bytes", &self.max_page_bytes)
            .field("eof", &self.eof)
            .finish_non_exhaustive()
    }
}

impl TypedRowChangeReader<'_> {
    fn new(source: &dyn RowSourceHost, max_page_bytes: u32) -> TypedRowChangeReader<'_> {
        TypedRowChangeReader {
            source,
            page: None,
            max_page_bytes,
            eof: false,
        }
    }

    pub fn next(&mut self) -> Result<Option<TypedRowChange>> {
        loop {
            if self
                .page
                .as_ref()
                .is_some_and(|page| !page.mutations.is_empty())
            {
                break;
            }
            if self.eof {
                return Ok(None);
            }
            let Some((bytes, attachments)) = self
                .source
                .next_page(self.max_page_bytes)
                .map_err(|error| host_error("host typed-row page read failed", error))?
            else {
                self.eof = true;
                return Ok(None);
            };
            let (schema_key, schema_fingerprint, mutations) =
                typed::decode_page_parts(&bytes, attachments).map_err(|error| {
                    Error::invalid_input(format!("invalid typed-row page: {error:?}"))
                })?;
            if mutations.is_empty() {
                return Err(Error::invalid_input("typed-row page must not be empty"));
            }
            self.page = Some(TypedInputPage {
                schema_key: schema_key.into(),
                schema_fingerprint,
                mutations: mutations.into(),
            });
        }

        let page = self.page.as_mut().expect("checked typed page is non-empty");
        let mutation = page
            .mutations
            .pop_front()
            .expect("checked typed page has a next mutation");
        let (primary_key, row, local_ref, effect) = match mutation {
            typed::OwnedMutation::Create { local_ref, row } => (
                Vec::new(),
                Some(row),
                Some(local_ref),
                ChangeEffect::Content,
            ),
            typed::OwnedMutation::Upsert {
                row_pk,
                row,
                effect,
            } => (
                row_pk,
                Some(row),
                None,
                match effect {
                    TypedChangeEffect::Content => ChangeEffect::Content,
                    TypedChangeEffect::FormatOnly => ChangeEffect::FormatOnly,
                },
            ),
            typed::OwnedMutation::Delete { row_pk } => (row_pk, None, None, ChangeEffect::Content),
        };
        let page = self.page.as_ref().expect("typed page remains buffered");
        Ok(Some(TypedRowChange {
            schema_key: page.schema_key.clone(),
            schema_fingerprint: page.schema_fingerprint,
            primary_key,
            row,
            local_ref,
            effect,
        }))
    }
}

#[derive(Debug)]
pub struct ParseInput<'a> {
    pub file_id: &'a str,
    pub path: &'a str,
    pub file: Snapshot<'a>,
    pub creates: CreateContext,
}

#[derive(Debug)]
pub struct FileEditReader<'a> {
    edits: &'a [FileEdit],
}

impl<'a> FileEditReader<'a> {
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &'a FileEdit> + Clone {
        self.edits.iter()
    }
}

#[derive(Debug)]
pub struct ParseChangesInput<'a> {
    pub file_id: &'a str,
    pub before_path: &'a str,
    pub after_path: &'a str,
    pub before: Snapshot<'a>,
    pub file_edits: FileEditReader<'a>,
    /// Complete accepted typed rows when this is a cold transition. Warm
    /// incremental transitions omit them so the host does not hydrate
    /// untouched rows.
    pub typed_rows: Option<TypedRowReader<'a>>,
    pub creates: CreateContext,
}

#[derive(Debug)]
pub struct SerializeInput<'a> {
    pub file_id: &'a str,
    pub path: &'a str,
    pub typed_rows: TypedRowReader<'a>,
    pub before: Option<Snapshot<'a>>,
}

#[derive(Debug)]
pub struct SerializeChangesInput<'a> {
    pub file_id: &'a str,
    pub path: &'a str,
    pub before: Snapshot<'a>,
    pub typed_row_changes: TypedRowChangeReader<'a>,
}

/// Optional capability that improves the host's column-based LWW result.
pub trait ColumnMerger: 'static {
    fn merge(input: ColumnMerge<'_>) -> Result<ColumnMergeResult>;
}

/// Optional capability that maintains a bytes-to-rows projection.
pub trait FileProjection: 'static {
    fn parse(input: ParseInput<'_>, output: &mut RowOutput<'_, '_>) -> Result<()>;

    fn parse_changes(
        input: ParseChangesInput<'_>,
        output: &mut RowChangeOutput<'_, '_>,
    ) -> Result<()>;

    fn serialize(input: SerializeInput<'_>, output: &mut FileOutput<'_, '_>) -> Result<()>;

    fn serialize_changes(
        input: SerializeChangesInput<'_>,
        output: &mut FileEditOutput<'_, '_>,
    ) -> Result<()>;
}

#[doc(hidden)]
#[derive(Debug)]
pub struct Component<C, F>(PhantomData<(C, F)>);

#[doc(hidden)]
#[derive(Debug)]
pub struct ColumnMergerComponent<C>(PhantomData<C>);

#[doc(hidden)]
#[derive(Debug)]
pub struct FileProjectionComponent<F>(PhantomData<F>);

fn apply_column_merges<C: ColumnMerger>(
    input: &dyn ColumnMergeSourceHost,
    output: &dyn ColumnMergeSinkHost,
) -> Result<()> {
    let count = input.len();
    let max_batch_bytes = output.max_batch_bytes();
    if max_batch_bytes == 0 {
        return Err(Error::limit_exceeded("max-batch-bytes must be positive"));
    }
    for index in 0..count {
        let meta = input
            .get(index)
            .map_err(|error| host_error("host column merge metadata read failed", error))?;
        let value = |side, len: Option<u64>| ColumnValue {
            source: input,
            index,
            side,
            len,
        };
        let row_payload = |side, len| RowPayload {
            source: input,
            index,
            side,
            len,
        };
        let schema_fingerprint: [u8; 32] =
            meta.schema_fingerprint.as_slice().try_into().map_err(|_| {
                Error::invalid_input("host column merge metadata has an invalid schema fingerprint")
            })?;
        let primary_key = meta
            .primary_key
            .iter()
            .map(|component| {
                typed::decode_value_bytes(component).map_err(|error| {
                    Error::invalid_input(format!("invalid typed row identity: {error:?}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let merge = ColumnMerge {
            row: RowIdentity {
                schema_key: meta.schema_key,
                schema_fingerprint,
                primary_key,
                file_id: meta.file_id,
            },
            column: meta.column,
            base: value(MergeSide::Base, meta.base_len),
            a: value(MergeSide::A, meta.a_len),
            b: value(MergeSide::B, meta.b_len),
            row_payloads: RowPayloadVersions {
                base: row_payload(MergeSide::Base, meta.base_row_len),
                a: row_payload(MergeSide::A, meta.a_row_len),
                b: row_payload(MergeSide::B, meta.b_row_len),
            },
        };
        match C::merge(merge)? {
            ColumnMergeResult::UseLww => output
                .use_lww(meta.ordinal)
                .map_err(|error| host_error("host rejected column LWW result", error))?,
            ColumnMergeResult::Replace(replacement) => {
                let (length, bytes) = match replacement {
                    OwnedColumnValue::Typed(value) => {
                        let bytes = typed::encode_value_bytes(&value).map_err(|error| {
                            Error::invalid_input(format!("invalid typed replacement: {error:?}"))
                        })?;
                        (Some(bytes.len() as u64), Some(bytes))
                    }
                    OwnedColumnValue::Missing => (None, None),
                };
                output
                    .begin_replace(meta.ordinal, length)
                    .map_err(|error| host_error("host rejected column replacement", error))?;
                if let Some(bytes) = bytes {
                    for chunk in bytes.chunks(max_batch_bytes as usize) {
                        output.write_replacement(chunk).map_err(|error| {
                            host_error("host rejected replacement chunk", error)
                        })?;
                    }
                }
                output
                    .finish_replace()
                    .map_err(|error| host_error("host rejected replacement finish", error))?;
            }
        }
    }
    Ok(())
}

impl<C: ColumnMerger, F: 'static> CombinedColumnMergerGuest for Component<C, F> {
    fn merge(
        input: ColumnMergeSource,
        output: &ColumnMergeSink,
    ) -> std::result::Result<(), PluginError> {
        apply_column_merges::<C>(&input, output).map_err(plugin_error)
    }
}

impl<C: ColumnMerger> column_merger_bindings::exports::lix::plugin::column_merger::Guest
    for ColumnMergerComponent<C>
{
    fn merge(
        input: column_merger_bindings::lix::plugin::host::ColumnMergeSource,
        output: &column_merger_bindings::lix::plugin::host::ColumnMergeSink,
    ) -> std::result::Result<(), column_merger_bindings::lix::plugin::types::PluginError> {
        apply_column_merges::<C>(&input, output).map_err(column_plugin_error)
    }
}

impl<C: 'static, F: FileProjection> CombinedFileProjectionGuest for Component<C, F> {
    fn parse(
        input: WitParseRequest,
        output: &WitTransition,
    ) -> std::result::Result<(), PluginError> {
        let mut transition = TransitionOutput::new(output).map_err(plugin_error)?;
        let file = Snapshot { inner: &input.file };
        let input = ParseInput {
            file_id: &input.file_id,
            path: &input.path,
            file,
            creates: CreateContext {
                high: input.creates.high,
                low: input.creates.low,
            },
        };
        let mut rows = RowOutput {
            inner: &mut transition,
        };
        F::parse(input, &mut rows).map_err(plugin_error)?;
        transition.finish().map_err(plugin_error)
    }

    fn parse_changes(
        input: WitParseChangesRequest,
        output: &WitTransition,
    ) -> std::result::Result<(), PluginError> {
        let max_batch_bytes = output.max_batch_bytes();
        let edits: Vec<FileEdit> = input
            .file_edits
            .iter()
            .map(|edit| FileEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: edit.insert.clone(),
            })
            .collect();
        let mut transition = TransitionOutput::new(output).map_err(plugin_error)?;
        let projection = Snapshot {
            inner: &input.before,
        };
        let typed_rows = input
            .rows
            .as_ref()
            .map(|rows| TypedRowReader::new(rows, max_batch_bytes));
        let update = ParseChangesInput {
            file_id: &input.file_id,
            before_path: &input.before_path,
            after_path: &input.after_path,
            before: projection,
            file_edits: FileEditReader { edits: &edits },
            typed_rows,
            creates: CreateContext {
                high: input.creates.high,
                low: input.creates.low,
            },
        };
        let mut changes = RowChangeOutput {
            inner: &mut transition,
        };
        F::parse_changes(update, &mut changes).map_err(plugin_error)?;
        transition.finish().map_err(plugin_error)
    }

    fn serialize(
        input: WitSerializeRequest,
        output: &WitTransition,
    ) -> std::result::Result<(), PluginError> {
        let max_batch_bytes = output.max_batch_bytes();
        let before = input
            .before
            .as_ref()
            .map(|before| Snapshot { inner: before });
        let typed_rows = TypedRowReader::new(&input.rows, max_batch_bytes);
        let serialize = SerializeInput {
            file_id: &input.file_id,
            path: &input.path,
            typed_rows,
            before,
        };
        let mut transition = TransitionOutput::new(output).map_err(plugin_error)?;
        let mut file = FileOutput {
            inner: &mut transition,
        };
        F::serialize(serialize, &mut file).map_err(plugin_error)?;
        transition.finish().map_err(plugin_error)
    }

    fn serialize_changes(
        input: WitSerializeChangesRequest,
        output: &WitTransition,
    ) -> std::result::Result<(), PluginError> {
        let max_batch_bytes = output.max_batch_bytes();
        let update = SerializeChangesInput {
            file_id: &input.file_id,
            path: &input.path,
            before: Snapshot {
                inner: &input.before,
            },
            typed_row_changes: TypedRowChangeReader::new(&input.row_changes, max_batch_bytes),
        };
        let mut transition = TransitionOutput::new(output).map_err(plugin_error)?;
        let mut edits = FileEditOutput {
            inner: &mut transition,
        };
        F::serialize_changes(update, &mut edits).map_err(plugin_error)?;
        transition.finish().map_err(plugin_error)
    }
}

impl<F: FileProjection> file_projection_bindings::exports::lix::plugin::file_projection::Guest
    for FileProjectionComponent<F>
{
    fn parse(
        input: file_projection_bindings::lix::plugin::types::ParseRequest,
        output: &file_projection_bindings::lix::plugin::host::Transition,
    ) -> std::result::Result<(), file_projection_bindings::lix::plugin::types::PluginError> {
        let mut transition = TransitionOutput::new(output).map_err(projection_plugin_error)?;
        let file = Snapshot { inner: &input.file };
        let input = ParseInput {
            file_id: &input.file_id,
            path: &input.path,
            file,
            creates: CreateContext {
                high: input.creates.high,
                low: input.creates.low,
            },
        };
        let mut rows = RowOutput {
            inner: &mut transition,
        };
        F::parse(input, &mut rows).map_err(projection_plugin_error)?;
        transition.finish().map_err(projection_plugin_error)
    }

    fn parse_changes(
        input: file_projection_bindings::lix::plugin::types::ParseChangesRequest,
        output: &file_projection_bindings::lix::plugin::host::Transition,
    ) -> std::result::Result<(), file_projection_bindings::lix::plugin::types::PluginError> {
        let max_batch_bytes = output.max_batch_bytes();
        let edits: Vec<FileEdit> = input
            .file_edits
            .iter()
            .map(|edit| FileEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: edit.insert.clone(),
            })
            .collect();
        let mut transition = TransitionOutput::new(output).map_err(projection_plugin_error)?;
        let typed_rows = input
            .rows
            .as_ref()
            .map(|rows| TypedRowReader::new(rows, max_batch_bytes));
        let update = ParseChangesInput {
            file_id: &input.file_id,
            before_path: &input.before_path,
            after_path: &input.after_path,
            before: Snapshot {
                inner: &input.before,
            },
            file_edits: FileEditReader { edits: &edits },
            typed_rows,
            creates: CreateContext {
                high: input.creates.high,
                low: input.creates.low,
            },
        };
        let mut changes = RowChangeOutput {
            inner: &mut transition,
        };
        F::parse_changes(update, &mut changes).map_err(projection_plugin_error)?;
        transition.finish().map_err(projection_plugin_error)
    }

    fn serialize(
        input: file_projection_bindings::lix::plugin::types::SerializeRequest,
        output: &file_projection_bindings::lix::plugin::host::Transition,
    ) -> std::result::Result<(), file_projection_bindings::lix::plugin::types::PluginError> {
        let max_batch_bytes = output.max_batch_bytes();
        let before = input
            .before
            .as_ref()
            .map(|before| Snapshot { inner: before });
        let serialize = SerializeInput {
            file_id: &input.file_id,
            path: &input.path,
            typed_rows: TypedRowReader::new(&input.rows, max_batch_bytes),
            before,
        };
        let mut transition = TransitionOutput::new(output).map_err(projection_plugin_error)?;
        let mut file = FileOutput {
            inner: &mut transition,
        };
        F::serialize(serialize, &mut file).map_err(projection_plugin_error)?;
        transition.finish().map_err(projection_plugin_error)
    }

    fn serialize_changes(
        input: file_projection_bindings::lix::plugin::types::SerializeChangesRequest,
        output: &file_projection_bindings::lix::plugin::host::Transition,
    ) -> std::result::Result<(), file_projection_bindings::lix::plugin::types::PluginError> {
        let max_batch_bytes = output.max_batch_bytes();
        let update = SerializeChangesInput {
            file_id: &input.file_id,
            path: &input.path,
            before: Snapshot {
                inner: &input.before,
            },
            typed_row_changes: TypedRowChangeReader::new(&input.row_changes, max_batch_bytes),
        };
        let mut transition = TransitionOutput::new(output).map_err(projection_plugin_error)?;
        let mut edits = FileEditOutput {
            inner: &mut transition,
        };
        F::serialize_changes(update, &mut edits).map_err(projection_plugin_error)?;
        transition.finish().map_err(projection_plugin_error)
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! __lix_export_capabilities {
    (column_merger: $column_merger:ty, file_projection: $file_projection:ty $(,)?) => {
        #[cfg(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2"))]
        type __LixPluginComponent =
            $crate::plugin::api::Component<$column_merger, $file_projection>;
        #[cfg(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2"))]
        $crate::plugin::api::combined_bindings::export_combined_component!(
            __LixPluginComponent with_types_in $crate::plugin::api::combined_bindings
        );
    };
    (file_projection: $file_projection:ty, column_merger: $column_merger:ty $(,)?) => {
        $crate::__lix_export_capabilities!(
            column_merger: $column_merger,
            file_projection: $file_projection,
        );
    };
    (column_merger: $column_merger:ty $(,)?) => {
        #[cfg(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2"))]
        type __LixPluginComponent =
            $crate::plugin::api::ColumnMergerComponent<$column_merger>;
        #[cfg(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2"))]
        $crate::plugin::api::column_merger_bindings::export_column_merger_component!(
            __LixPluginComponent with_types_in $crate::plugin::api::column_merger_bindings
        );
    };
    (file_projection: $file_projection:ty $(,)?) => {
        #[cfg(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2"))]
        type __LixPluginComponent =
            $crate::plugin::api::FileProjectionComponent<$file_projection>;
        #[cfg(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2"))]
        $crate::plugin::api::file_projection_bindings::export_file_projection_component!(
            __LixPluginComponent with_types_in $crate::plugin::api::file_projection_bindings
        );
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct RecordingTransitionHost {
        pages: Mutex<Vec<(Vec<u8>, Vec<Vec<u8>>)>>,
        max_batch_bytes: u32,
    }

    impl Default for RecordingTransitionHost {
        fn default() -> Self {
            Self {
                pages: Mutex::new(Vec::new()),
                max_batch_bytes: 64 * 1024,
            }
        }
    }

    impl RecordingTransitionHost {
        fn with_max_batch_bytes(max_batch_bytes: u32) -> Self {
            Self {
                pages: Mutex::new(Vec::new()),
                max_batch_bytes,
            }
        }
    }

    impl TransitionHost for RecordingTransitionHost {
        fn max_batch_bytes(&self) -> u32 {
            self.max_batch_bytes
        }
        fn put_state(&self, _: &[u8], _: &[u8]) -> std::result::Result<(), CommonHostError> {
            Ok(())
        }
        fn delete_state(&self, _: &[u8]) -> std::result::Result<(), CommonHostError> {
            Ok(())
        }
        fn emit_rows(
            &self,
            payload: Vec<u8>,
            attachments: Vec<Vec<u8>>,
        ) -> std::result::Result<(), CommonHostError> {
            self.pages.lock().unwrap().push((payload, attachments));
            Ok(())
        }
        fn replace_all_rows(&self) -> std::result::Result<(), CommonHostError> {
            Ok(())
        }
        fn emit_file_edit(
            &self,
            _: u64,
            _: u64,
            _: &[u8],
        ) -> std::result::Result<(), CommonHostError> {
            Ok(())
        }
        fn begin_file_replacement(&self, _: u64) -> std::result::Result<(), CommonHostError> {
            Ok(())
        }
        fn write_file_replacement(&self, _: &[u8]) -> std::result::Result<(), CommonHostError> {
            Ok(())
        }
        fn finish_file_replacement(&self) -> std::result::Result<(), CommonHostError> {
            Ok(())
        }
    }

    fn recorded_schemas(host: &RecordingTransitionHost) -> Vec<(String, usize)> {
        host.pages
            .lock()
            .unwrap()
            .iter()
            .map(|(page, attachments)| {
                let (schema, _, mutations) = typed::decode_page_parts(page, attachments.clone())
                    .expect("valid recorded page");
                (schema, mutations.len())
            })
            .collect()
    }

    fn resident_page_bytes(payload: &[u8], attachments: &[Vec<u8>]) -> usize {
        payload.len()
            + attachments.iter().map(Vec::len).sum::<usize>()
            + attachments.len() * typed::ATTACHMENT_TABLE_ENTRY_BYTES
    }

    fn recorded_typed_totals(host: &RecordingTransitionHost) -> (usize, usize) {
        host.pages
            .lock()
            .unwrap()
            .iter()
            .map(|(page, attachments)| {
                let (_, _, mutations) = typed::decode_page_parts(page, attachments.clone())
                    .expect("valid recorded page");
                let text_bytes = mutations
                    .iter()
                    .filter_map(|mutation| match mutation {
                        typed::OwnedMutation::Create { row, .. }
                        | typed::OwnedMutation::Upsert { row, .. } => Some(row),
                        typed::OwnedMutation::Delete { .. } => None,
                    })
                    .flat_map(TypedRow::values)
                    .filter_map(|value| match value {
                        TypedValue::Text(value) => Some(value.len()),
                        _ => None,
                    })
                    .sum::<usize>();
                (mutations.len(), text_bytes)
            })
            .fold((0, 0), |(records, bytes), page| {
                (records + page.0, bytes + page.1)
            })
    }

    #[test]
    fn typed_output_preserves_schema_transition_order_by_default() {
        let host = RecordingTransitionHost::default();
        let mut output = TransitionOutput::new(&host).unwrap();
        let row = TypedRow::from([("id".to_owned(), TypedValue::Text("x".to_owned()))]);
        output
            .typed_row(
                "a",
                [1; 32],
                TypedMutation::Create {
                    local_ref: 0,
                    row: &row,
                },
            )
            .unwrap();
        output
            .typed_row(
                "b",
                [2; 32],
                TypedMutation::Create {
                    local_ref: 1,
                    row: &row,
                },
            )
            .unwrap();
        output
            .typed_row(
                "a",
                [1; 32],
                TypedMutation::Create {
                    local_ref: 2,
                    row: &row,
                },
            )
            .unwrap();
        output.finish().unwrap();
        assert_eq!(
            recorded_schemas(&host),
            vec![("a".into(), 1), ("b".into(), 1), ("a".into(), 1)]
        );
    }

    #[test]
    fn initial_output_may_explicitly_coalesce_schema_pages() {
        let host = RecordingTransitionHost::default();
        let mut output = TransitionOutput::new(&host).unwrap();
        output.coalesce_typed_schemas = true;
        let row = TypedRow::from([("id".to_owned(), TypedValue::Text("x".to_owned()))]);
        for (schema, fingerprint, local_ref) in
            [("a", [1; 32], 0), ("b", [2; 32], 1), ("a", [1; 32], 2)]
        {
            output
                .typed_row(
                    schema,
                    fingerprint,
                    TypedMutation::Create {
                        local_ref,
                        row: &row,
                    },
                )
                .unwrap();
        }
        output.finish().unwrap();
        assert_eq!(
            recorded_schemas(&host),
            vec![("a".into(), 2), ("b".into(), 1)]
        );
    }

    #[test]
    fn typed_output_splits_mixed_create_and_complete_layouts() {
        let host = RecordingTransitionHost::default();
        let mut output = TransitionOutput::new(&host).unwrap();
        let create = TypedRow::from([("body".to_owned(), TypedValue::Text("a".to_owned()))]);
        let complete = TypedRow::from([
            ("body".to_owned(), TypedValue::Text("b".to_owned())),
            ("id".to_owned(), TypedValue::Uuid(uuid::Uuid::nil())),
        ]);
        output
            .typed_row(
                "row",
                [3; 32],
                TypedMutation::Create {
                    local_ref: 0,
                    row: &create,
                },
            )
            .unwrap();
        output
            .typed_row(
                "row",
                [3; 32],
                TypedMutation::Upsert {
                    row_pk: &[TypedValue::Uuid(uuid::Uuid::nil())],
                    row: &complete,
                    effect: TypedChangeEffect::Content,
                },
            )
            .unwrap();
        output.finish().unwrap();
        assert_eq!(
            recorded_schemas(&host),
            vec![("row".into(), 1), ("row".into(), 1)]
        );
    }

    #[test]
    fn typed_output_flushes_at_the_codec_record_limit() {
        let host = RecordingTransitionHost::with_max_batch_bytes(u32::MAX);
        let mut output = TransitionOutput::new(&host).unwrap();
        let row = TypedRow::default();
        for local_ref in 0..=typed::MAX_RECORDS_PER_PAGE {
            output
                .typed_row(
                    "row",
                    [4; 32],
                    TypedMutation::Create {
                        local_ref,
                        row: &row,
                    },
                )
                .unwrap();
        }
        output.finish().unwrap();

        assert_eq!(
            recorded_schemas(&host),
            vec![
                ("row".into(), typed::MAX_RECORDS_PER_PAGE as usize),
                ("row".into(), 1),
            ]
        );
    }

    #[test]
    fn typed_output_counts_attachment_storage_when_splitting_pages() {
        let max_batch_bytes = 32 * 1024;
        let host = RecordingTransitionHost::with_max_batch_bytes(max_batch_bytes);
        let mut output = TransitionOutput::new(&host).unwrap();
        let row = TypedRow::from([("body".to_owned(), TypedValue::Text("x".repeat(9 * 1024)))]);
        for local_ref in 0..10 {
            output
                .typed_row(
                    "row",
                    [5; 32],
                    TypedMutation::Create {
                        local_ref,
                        row: &row,
                    },
                )
                .unwrap();
        }
        output.finish().unwrap();

        let pages = host.pages.lock().unwrap();
        assert_eq!(pages.len(), 4);
        for (payload, attachments) in pages.iter() {
            let guest_page_bytes = payload.len()
                + attachments.iter().map(Vec::len).sum::<usize>()
                + attachments.len() * typed::ATTACHMENT_TABLE_ENTRY_BYTES;
            assert!(guest_page_bytes <= max_batch_bytes as usize);
        }
    }

    #[test]
    fn typed_output_bounds_ordinary_pages_at_one_mib_under_a_larger_hard_max() {
        const TARGET_BYTES: usize = 1024 * 1024;
        const RECORD_BYTES: usize = 4 * 1024;
        const RECORDS: usize = 600;

        let host = RecordingTransitionHost::with_max_batch_bytes(4 * TARGET_BYTES as u32);
        let mut output = TransitionOutput::new(&host).unwrap();
        let row = TypedRow::from([(
            "body".to_owned(),
            TypedValue::Text("x".repeat(RECORD_BYTES)),
        )]);
        for local_ref in 0..RECORDS as u32 {
            output
                .typed_row(
                    "row",
                    [6; 32],
                    TypedMutation::Create {
                        local_ref,
                        row: &row,
                    },
                )
                .unwrap();
        }
        output.finish().unwrap();

        let pages = host.pages.lock().unwrap();
        assert_eq!(pages.len(), 3, "one host callback per bounded page");
        assert!(pages.iter().all(|(payload, attachments)| {
            attachments.is_empty() && resident_page_bytes(payload, attachments) <= TARGET_BYTES
        }));
        drop(pages);
        assert_eq!(
            recorded_typed_totals(&host),
            (RECORDS, RECORDS * RECORD_BYTES)
        );
    }

    #[test]
    fn typed_output_counts_attachments_against_the_one_mib_target() {
        const TARGET_BYTES: usize = 1024 * 1024;
        const RECORD_BYTES: usize = TARGET_BYTES / 2;

        let host = RecordingTransitionHost::with_max_batch_bytes(4 * TARGET_BYTES as u32);
        let mut output = TransitionOutput::new(&host).unwrap();
        let row = TypedRow::from([(
            "body".to_owned(),
            TypedValue::Text("x".repeat(RECORD_BYTES)),
        )]);
        for local_ref in 0..2 {
            output
                .typed_row(
                    "row",
                    [7; 32],
                    TypedMutation::Create {
                        local_ref,
                        row: &row,
                    },
                )
                .unwrap();
        }
        output.finish().unwrap();

        let pages = host.pages.lock().unwrap();
        assert_eq!(pages.len(), 2, "attachment bytes force a second callback");
        assert!(pages.iter().all(|(payload, attachments)| {
            attachments.len() == 1 && resident_page_bytes(payload, attachments) <= TARGET_BYTES
        }));
        drop(pages);
        assert_eq!(recorded_typed_totals(&host), (2, 2 * RECORD_BYTES));
    }

    #[test]
    fn typed_output_allows_one_oversized_record_up_to_the_host_hard_max() {
        const TARGET_BYTES: usize = 1024 * 1024;
        const OVERSIZED_BYTES: usize = TARGET_BYTES + 256 * 1024;
        const ORDINARY_BYTES: usize = 64 * 1024;

        let host = RecordingTransitionHost::with_max_batch_bytes(4 * TARGET_BYTES as u32);
        let mut output = TransitionOutput::new(&host).unwrap();
        for (local_ref, bytes) in [(0, OVERSIZED_BYTES), (1, ORDINARY_BYTES)] {
            let row = TypedRow::from([("body".to_owned(), TypedValue::Text("x".repeat(bytes)))]);
            output
                .typed_row(
                    "row",
                    [8; 32],
                    TypedMutation::Create {
                        local_ref,
                        row: &row,
                    },
                )
                .unwrap();
        }
        output.finish().unwrap();

        let pages = host.pages.lock().unwrap();
        assert_eq!(pages.len(), 2, "the oversized singleton is one callback");
        let page_bytes = pages
            .iter()
            .map(|(payload, attachments)| resident_page_bytes(payload, attachments))
            .collect::<Vec<_>>();
        assert!(page_bytes[0] > TARGET_BYTES);
        assert!(page_bytes[0] <= host.max_batch_bytes as usize);
        assert!(page_bytes[1] <= TARGET_BYTES);
        drop(pages);
        assert_eq!(
            recorded_typed_totals(&host),
            (2, OVERSIZED_BYTES + ORDINARY_BYTES)
        );
    }

    #[test]
    fn schema_coalescing_uses_the_one_mib_aggregate_target() {
        const TARGET_BYTES: usize = 1024 * 1024;
        const RECORD_BYTES: usize = 96 * 1024;
        const SCHEMAS: usize = 16;

        let host = RecordingTransitionHost::with_max_batch_bytes(4 * TARGET_BYTES as u32);
        let mut output = TransitionOutput::new(&host).unwrap();
        output.coalesce_typed_schemas = true;
        for index in 0..SCHEMAS as u8 {
            let row = TypedRow::from([(
                "body".to_owned(),
                TypedValue::Text(char::from(b'a' + index).to_string().repeat(RECORD_BYTES)),
            )]);
            output
                .typed_row(
                    &format!("schema-{index}"),
                    [index; 32],
                    TypedMutation::Create {
                        local_ref: u32::from(index),
                        row: &row,
                    },
                )
                .unwrap();
            assert!(output.buffered_typed_bytes().unwrap() <= TARGET_BYTES);
        }
        output.finish().unwrap();

        let pages = host.pages.lock().unwrap();
        assert_eq!(pages.len(), SCHEMAS, "every schema page reaches the host");
        assert!(pages.iter().all(|(payload, attachments)| {
            resident_page_bytes(payload, attachments) <= TARGET_BYTES
        }));
        drop(pages);
        assert_eq!(
            recorded_typed_totals(&host),
            (SCHEMAS, SCHEMAS * RECORD_BYTES)
        );
    }

    #[test]
    fn schema_coalescing_keeps_total_guest_page_buffers_bounded() {
        let max_batch_bytes = 32 * 1024;
        let host = RecordingTransitionHost::with_max_batch_bytes(max_batch_bytes);
        let mut output = TransitionOutput::new(&host).unwrap();
        output.coalesce_typed_schemas = true;
        for index in 0..16_u8 {
            let row = TypedRow::from([(
                "body".to_owned(),
                TypedValue::Text(char::from(b'a' + index).to_string().repeat(9 * 1024)),
            )]);
            output
                .typed_row(
                    &format!("schema-{index}"),
                    [index; 32],
                    TypedMutation::Create {
                        local_ref: u32::from(index),
                        row: &row,
                    },
                )
                .unwrap();
            assert!(
                output.buffered_typed_bytes().unwrap() <= max_batch_bytes as usize,
                "coalesced schema buffers exceeded the guest-side page budget"
            );
        }
        output.finish().unwrap();
        assert_eq!(
            host.pages
                .lock()
                .unwrap()
                .iter()
                .map(|(_, attachments)| attachments.len())
                .sum::<usize>(),
            16
        );
    }

    #[test]
    fn create_context_returns_native_uuid() {
        let namespace = [
            0x01, 0x8f, 0x0f, 0x31, 0x8f, 0x50, 0x7e, 0xb0, 0x9b, 0x5b, 0x7f, 0xc8,
        ];
        let context = CreateContext::from_namespace_bytes(namespace);
        let id = context.id(0xfe9f_5250);

        assert_eq!(id.as_bytes()[..12], namespace);
        assert_eq!(&id.as_bytes()[12..], &0xfe9f_5250_u32.to_be_bytes());
    }

    #[test]
    fn public_row_identities_preserve_native_schema_values() {
        let primary_key = vec![
            TypedValue::Uuid(
                uuid::Uuid::parse_str("018f0f31-8f50-7eb0-9b5b-7fc8fe9f5250").unwrap(),
            ),
            TypedValue::Int8(i64::MIN),
            TypedValue::Boolean(true),
            TypedValue::Jsonb(serde_json::json!({ "scope": [1, null, false] }).into()),
        ];

        let record = TypedRowRecord {
            schema_key: "example".into(),
            schema_fingerprint: [7; 32],
            primary_key: primary_key.clone(),
            row: TypedRow::default(),
        };
        let change = TypedRowChange {
            schema_key: "example".into(),
            schema_fingerprint: [7; 32],
            primary_key: primary_key.clone(),
            row: None,
            local_ref: None,
            effect: ChangeEffect::Content,
        };
        let merge_identity = RowIdentity {
            schema_key: "example".to_owned(),
            schema_fingerprint: [7; 32],
            primary_key: primary_key.clone(),
            file_id: None,
        };

        assert_eq!(record.primary_key, primary_key);
        assert_eq!(change.primary_key, primary_key);
        assert_eq!(merge_identity.primary_key, primary_key);
    }
}
