//! Row-first authoring layer for Lix's Component API v1.

#![allow(clippy::missing_errors_doc)]

#[doc(hidden)]
pub use lix_plugin_bindings_column_merger as column_merger_bindings;
#[doc(hidden)]
pub use lix_plugin_bindings_combined as combined_bindings;
#[doc(hidden)]
pub use lix_plugin_bindings_file_projection as file_projection_bindings;

use self::combined_bindings::exports::lix::plugin::column_merger::Guest as CombinedColumnMergerGuest;
use self::combined_bindings::exports::lix::plugin::file_projection::Guest as CombinedFileProjectionGuest;
use self::combined_bindings::lix::plugin::host::{
    ColumnMergeSink, ColumnMergeSource, Transition as WitTransition,
};
use self::combined_bindings::lix::plugin::types::{
    ParseChangesRequest as WitParseChangesRequest, ParseRequest as WitParseRequest, PluginError,
    SerializeChangesRequest as WitSerializeChangesRequest, SerializeRequest as WitSerializeRequest,
};
use super::wire::{Operation, Page as WirePage, Representation, encode_single_section};
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
    fn next_page(&self, max_bytes: u32) -> std::result::Result<Option<Vec<u8>>, CommonHostError>;
    fn read_attachment(
        &self,
        ordinal: u32,
        offset: u64,
        length: u32,
    ) -> std::result::Result<Option<Vec<u8>>, CommonHostError>;
}

trait TransitionHost {
    fn max_batch_bytes(&self) -> u32;
    fn put_state(&self, key: &[u8], value: &[u8]) -> std::result::Result<(), CommonHostError>;
    fn delete_state(&self, key: &[u8]) -> std::result::Result<(), CommonHostError>;
    fn emit_rows(&self, payload: Vec<u8>) -> std::result::Result<(), CommonHostError>;
    fn replace_all_rows(&self) -> std::result::Result<(), CommonHostError>;
    fn begin_row_attachment(&self, length: u64) -> std::result::Result<u32, CommonHostError>;
    fn write_row_attachment(&self, chunk: &[u8]) -> std::result::Result<(), CommonHostError>;
    fn finish_row_attachment(&self) -> std::result::Result<(), CommonHostError>;
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
    row_pk: Vec<String>,
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
            ) -> std::result::Result<Option<Vec<u8>>, CommonHostError> {
                self.next_page(max_bytes)
                    .map(|page| page.map(|page| page.payload))
                    .map_err(map_host_error)
            }
            fn read_attachment(
                &self,
                ordinal: u32,
                offset: u64,
                length: u32,
            ) -> std::result::Result<Option<Vec<u8>>, CommonHostError> {
                self.read_attachment(ordinal, offset, length)
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
            fn emit_rows(&self, payload: Vec<u8>) -> std::result::Result<(), CommonHostError> {
                self.emit_rows(&$bindings::lix::plugin::host::RowPage { payload })
                    .map_err(map_host_error)
            }
            fn replace_all_rows(&self) -> std::result::Result<(), CommonHostError> {
                self.replace_all_rows().map_err(map_host_error)
            }
            fn begin_row_attachment(
                &self,
                length: u64,
            ) -> std::result::Result<u32, CommonHostError> {
                self.begin_row_attachment(length).map_err(map_host_error)
            }
            fn write_row_attachment(
                &self,
                chunk: &[u8],
            ) -> std::result::Result<(), CommonHostError> {
                self.write_row_attachment(chunk).map_err(map_host_error)
            }
            fn finish_row_attachment(&self) -> std::result::Result<(), CommonHostError> {
                self.finish_row_attachment().map_err(map_host_error)
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

mod file_projection_hosts {
    use super::*;
    impl_projection_hosts!(file_projection_bindings);
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
                        row_pk: meta.row_pk,
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

mod column_merger_hosts {
    use super::*;
    impl_column_hosts!(column_merger_bindings);
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

struct TransitionOutput<'a> {
    inner: &'a dyn TransitionHost,
    max_page_bytes: u32,
    max_batch_bytes: u32,
    row_payload: Vec<u8>,
    row_records: u32,
    row_creates_only: Option<bool>,
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
    fn new(inner: &'a dyn TransitionHost) -> std::result::Result<Self, Error> {
        let max_page_bytes = inner.max_batch_bytes();
        let snapshot_overhead = 24;
        if max_page_bytes <= snapshot_overhead {
            return Err(Error::limit_exceeded(
                "max-batch-bytes cannot hold a row page".to_owned(),
            ));
        }
        Ok(TransitionOutput {
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

    fn replace_all_rows(&mut self) -> Result<()> {
        self.flush_rows()?;
        self.inner
            .replace_all_rows()
            .map_err(|error| host_error("host rejected complete row replacement", error))
    }

    fn file_edit(&mut self, offset: u64, delete_len: u64, insert: &[u8]) -> Result<()> {
        self.flush_rows()?;
        self.inner
            .emit_file_edit(offset, delete_len, insert)
            .map_err(|error| host_error("host rejected file edit", error))
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
            .emit_rows(page.payload)
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

/// Complete rows produced by [`FileProjection::parse`] or the full-reparse
/// fallback of [`FileProjection::parse_changes`].
#[derive(Debug)]
pub struct RowOutput<'output, 'host> {
    inner: &'output mut TransitionOutput<'host>,
}

impl RowOutput<'_, '_> {
    pub fn create(&mut self, schema_key: &str, local_ref: u32, snapshot: &[u8]) -> Result<()> {
        self.inner.row(RowMutation::Create {
            schema_key,
            local_ref,
            snapshot,
        })
    }

    pub fn upsert(&mut self, schema_key: &str, row_pk: &[String], snapshot: &[u8]) -> Result<()> {
        self.inner.row(RowMutation::Upsert {
            schema_key,
            row_pk,
            snapshot,
            effect: ChangeEffect::Content,
        })
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
    pub fn create(&mut self, schema_key: &str, local_ref: u32, snapshot: &[u8]) -> Result<()> {
        self.inner.row(RowMutation::Create {
            schema_key,
            local_ref,
            snapshot,
        })
    }

    pub fn upsert(
        &mut self,
        schema_key: &str,
        row_pk: &[String],
        snapshot: &[u8],
        effect: ChangeEffect,
    ) -> Result<()> {
        self.inner.row(RowMutation::Upsert {
            schema_key,
            row_pk,
            snapshot,
            effect,
        })
    }

    pub fn delete(&mut self, schema_key: &str, row_pk: &[String]) -> Result<()> {
        self.inner.row(RowMutation::Delete { schema_key, row_pk })
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

    pub fn read(&self) -> Result<Option<Vec<u8>>> {
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

    /// Decodes the canonical JSON representation of this column. A missing
    /// optional column remains `None`; an explicit JSON `null` is decoded by
    /// `T` in the ordinary serde way.
    pub fn decode_json<T: serde::de::DeserializeOwned>(&self) -> Result<Option<T>> {
        self.read()?
            .map(|bytes| {
                serde_json::from_slice(&bytes).map_err(|error| {
                    Error::invalid_input(format!("invalid column JSON value: {error}"))
                })
            })
            .transpose()
    }

    /// Convenience for the dominant custom-merge case: a text column.
    pub fn text(&self) -> Result<Option<String>> {
        self.decode_json::<String>()
    }
}

#[derive(Debug)]
pub struct RowIdentity {
    pub schema_key: String,
    pub row_pk: Vec<String>,
    pub file_id: Option<String>,
}

pub struct RowSnapshot<'a> {
    source: &'a dyn ColumnMergeSourceHost,
    index: u32,
    side: MergeSide,
    len: u64,
}

impl std::fmt::Debug for RowSnapshot<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RowSnapshot")
            .field("len", &self.len)
            .finish()
    }
}

impl RowSnapshot<'_> {
    pub fn read(&self) -> Result<Vec<u8>> {
        const READ_BYTES: u32 = 1024 * 1024;
        let capacity = usize::try_from(self.len)
            .map_err(|_| Error::limit_exceeded("row snapshot exceeds guest address space"))?;
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
}

#[derive(Debug)]
pub struct RowVersions<'a> {
    pub base: RowSnapshot<'a>,
    pub a: RowSnapshot<'a>,
    pub b: RowSnapshot<'a>,
}

#[derive(Debug)]
pub struct ColumnMerge<'a> {
    pub row: RowIdentity,
    pub column: String,
    pub base: ColumnValue<'a>,
    pub a: ColumnValue<'a>,
    pub b: ColumnValue<'a>,
    pub rows: RowVersions<'a>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OwnedColumnValue {
    Value(Vec<u8>),
    Missing,
}

impl OwnedColumnValue {
    /// Uses an already encoded canonical JSON column value.
    pub fn json(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Value(bytes.into())
    }

    pub fn text(value: impl AsRef<str>) -> Self {
        let mut json = String::with_capacity(value.as_ref().len() + 2);
        json.push('"');
        for character in value.as_ref().chars() {
            match character {
                '"' => json.push_str("\\\""),
                '\\' => json.push_str("\\\\"),
                '\u{08}' => json.push_str("\\b"),
                '\u{0c}' => json.push_str("\\f"),
                '\n' => json.push_str("\\n"),
                '\r' => json.push_str("\\r"),
                '\t' => json.push_str("\\t"),
                character if character <= '\u{1f}' => {
                    use std::fmt::Write as _;
                    write!(json, "\\u{:04x}", character as u32)
                        .expect("writing to a String cannot fail");
                }
                character => json.push(character),
            }
        }
        json.push('"');
        Self::Value(json.into_bytes())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnMergeResult {
    UseLww,
    Replace(OwnedColumnValue),
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
    source: &'a dyn RowSourceHost,
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
    fn new(source: &dyn RowSourceHost, max_page_bytes: u32) -> RowReader<'_> {
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
    fn new(source: &dyn RowSourceHost, max_page_bytes: u32) -> RowChangeReader<'_> {
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
            self.page = Some(decode_input_page(page)?);
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
    source: &dyn RowSourceHost,
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

pub type FileSnapshot<'a> = Snapshot<'a>;
pub type ProjectionSnapshot<'a> = Snapshot<'a>;

#[derive(Debug)]
pub struct ParseInput<'a> {
    pub file_id: &'a str,
    pub path: &'a str,
    pub file: FileSnapshot<'a>,
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
    pub before: ProjectionSnapshot<'a>,
    pub file_edits: FileEditReader<'a>,
    /// Complete accepted rows when this is a cold transition. Warm incremental
    /// transitions omit them so the host does not hydrate untouched rows.
    pub rows: Option<RowReader<'a>>,
    pub creates: CreateContext,
}

#[derive(Debug)]
pub struct SerializeInput<'a> {
    pub file_id: &'a str,
    pub path: &'a str,
    pub rows: RowReader<'a>,
    pub before: Option<ProjectionSnapshot<'a>>,
}

#[derive(Debug)]
pub struct SerializeChangesInput<'a> {
    pub file_id: &'a str,
    pub path: &'a str,
    pub before: ProjectionSnapshot<'a>,
    pub row_changes: RowChangeReader<'a>,
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
        let row = |side, len| RowSnapshot {
            source: input,
            index,
            side,
            len,
        };
        let merge = ColumnMerge {
            row: RowIdentity {
                schema_key: meta.schema_key,
                row_pk: meta.row_pk,
                file_id: meta.file_id,
            },
            column: meta.column,
            base: value(MergeSide::Base, meta.base_len),
            a: value(MergeSide::A, meta.a_len),
            b: value(MergeSide::B, meta.b_len),
            rows: RowVersions {
                base: row(MergeSide::Base, meta.base_row_len),
                a: row(MergeSide::A, meta.a_row_len),
                b: row(MergeSide::B, meta.b_row_len),
            },
        };
        match C::merge(merge)? {
            ColumnMergeResult::UseLww => output
                .use_lww(meta.ordinal)
                .map_err(|error| host_error("host rejected column LWW result", error))?,
            ColumnMergeResult::Replace(replacement) => {
                let (length, bytes) = match replacement {
                    OwnedColumnValue::Value(bytes) => (Some(bytes.len() as u64), Some(bytes)),
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
        let rows = input
            .rows
            .as_ref()
            .map(|rows| RowReader::new(rows, max_batch_bytes));
        let update = ParseChangesInput {
            file_id: &input.file_id,
            before_path: &input.before_path,
            after_path: &input.after_path,
            before: projection,
            file_edits: FileEditReader { edits: &edits },
            rows,
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
        let rows = RowReader::new(&input.rows, max_batch_bytes);
        let serialize = SerializeInput {
            file_id: &input.file_id,
            path: &input.path,
            rows,
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
            row_changes: RowChangeReader::new(&input.row_changes, max_batch_bytes),
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
        let rows = input
            .rows
            .as_ref()
            .map(|rows| RowReader::new(rows, max_batch_bytes));
        let update = ParseChangesInput {
            file_id: &input.file_id,
            before_path: &input.before_path,
            after_path: &input.after_path,
            before: Snapshot {
                inner: &input.before,
            },
            file_edits: FileEditReader { edits: &edits },
            rows,
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
            rows: RowReader::new(&input.rows, max_batch_bytes),
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
            row_changes: RowChangeReader::new(&input.row_changes, max_batch_bytes),
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
