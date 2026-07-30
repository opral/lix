//! Authoring layer for the host-owned arena Component API v3 experiment.

#![allow(clippy::missing_errors_doc)]

wit_bindgen::generate!({
    path: "wit",
    world: "plugin",
    pub_export_macro: true,
    export_macro_name: "__export_component_v3_prototype",
    default_bindings_module: "lix_plugin_api_v3_prototype",
});

use exports::lix::plugin::api::{
    FileTransition, FileUpdate as WitFileUpdate, Guest, InputBytes, OpenFileInput, PluginError,
    TransitionSummary as WitTransitionSummary,
};
use lix::plugin::host::{
    ChangePage, CsvRowBatch, Root as WitRoot, Transaction as WitTransaction, TransitionSink,
};
use std::marker::PhantomData;

pub const PACKET_FORMAT_V1: u16 = 1;

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
    inner: &'a WitRoot,
}

impl std::fmt::Debug for Root<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Root")
            .field("generation", &self.generation())
            .field("file_len", &self.len())
            .finish()
    }
}

impl Root<'_> {
    pub fn generation(&self) -> String {
        self.inner.generation()
    }

    pub fn len(&self) -> u64 {
        self.inner.file_len()
    }

    pub fn read_all(&self) -> Result<Vec<u8>> {
        self.read_range(0, self.len())
    }

    pub fn read_range(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        const READ_BYTES: u32 = 1024 * 1024;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Error::invalid_input("root byte range overflowed"))?;
        if end > self.len() {
            return Err(Error::invalid_input("root byte range exceeds the file"));
        }
        let capacity = usize::try_from(length)
            .map_err(|_| Error::limit_exceeded("root range exceeds guest address space"))?;
        let mut output = Vec::with_capacity(capacity);
        let mut cursor = offset;
        while cursor < end {
            let chunk = u32::try_from((end - cursor).min(u64::from(READ_BYTES)))
                .expect("bounded root read fits u32");
            let bytes = self.inner.read_file(cursor, chunk).map_err(|error| {
                Error::invalid_input(format!("host root read failed: {error:?}"))
            })?;
            if bytes.len() != chunk as usize {
                return Err(Error::invalid_input("host root returned a short read"));
            }
            output.extend_from_slice(&bytes);
            cursor += u64::from(chunk);
        }
        Ok(output)
    }

    pub fn get_entity(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner
            .get_entity(key)
            .map_err(|error| Error::invalid_input(format!("host entity read failed: {error:?}")))
    }

    pub fn get_state(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner
            .get_state(key)
            .map_err(|error| Error::invalid_input(format!("host state read failed: {error:?}")))
    }

    pub fn state_len(&self, key: &[u8]) -> Option<u64> {
        self.inner.state_len(key)
    }

    pub fn read_state_range(
        &self,
        key: &[u8],
        offset: u64,
        length: u32,
    ) -> Result<Option<Vec<u8>>> {
        self.inner.read_state(key, offset, length).map_err(|error| {
            Error::invalid_input(format!("host state range read failed: {error:?}"))
        })
    }
}

pub struct Transaction {
    inner: WitTransaction,
}

impl std::fmt::Debug for Transaction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Transaction")
            .finish_non_exhaustive()
    }
}

impl Transaction {
    pub fn put_state(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner
            .put_state(key, value)
            .map_err(|error| Error::invalid_input(format!("host rejected state page: {error:?}")))
    }

    pub fn delete_state(&self, key: &[u8]) -> Result<()> {
        self.inner.delete_state(key).map_err(|error| {
            Error::invalid_input(format!("host rejected state deletion: {error:?}"))
        })
    }
}

pub struct Sink<'a> {
    inner: &'a TransitionSink,
    max_batch_bytes: u32,
    entity_count: u64,
    batch_count: u32,
    payload_bytes: u64,
}

impl std::fmt::Debug for Sink<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Sink")
            .field("max_batch_bytes", &self.max_batch_bytes)
            .field("entity_count", &self.entity_count)
            .field("batch_count", &self.batch_count)
            .field("payload_bytes", &self.payload_bytes)
            .finish_non_exhaustive()
    }
}

impl Sink<'_> {
    pub fn max_batch_bytes(&self) -> u32 {
        self.max_batch_bytes
    }

    pub fn emit_changes(&mut self, record_count: u32, payload: Vec<u8>) -> Result<()> {
        if record_count == 0 {
            return Err(Error::invalid_input("v3 change pages cannot be empty"));
        }
        if payload.len() > self.max_batch_bytes as usize {
            return Err(Error::limit_exceeded(
                "v3 change page exceeds max-batch-bytes",
            ));
        }
        let payload_len = payload.len() as u64;
        self.inner
            .emit_changes(&ChangePage {
                format_version: PACKET_FORMAT_V1,
                record_count,
                payload,
            })
            .map_err(|error| {
                Error::invalid_input(format!("host rejected entity batch: {error:?}"))
            })?;
        self.entity_count = self.entity_count.saturating_add(u64::from(record_count));
        self.batch_count = self.batch_count.saturating_add(1);
        self.payload_bytes = self.payload_bytes.saturating_add(payload_len);
        Ok(())
    }

    pub fn emit_csv_rows(&mut self, row_count: u32, payload: Vec<u8>) -> Result<()> {
        if row_count == 0 {
            return Err(Error::invalid_input("v3 CSV batches cannot be empty"));
        }
        if payload.len() > self.max_batch_bytes as usize {
            return Err(Error::limit_exceeded(
                "v3 CSV batch exceeds max-batch-bytes",
            ));
        }
        let payload_len = payload.len() as u64;
        self.inner
            .emit_csv_rows(&CsvRowBatch { row_count, payload })
            .map_err(|error| Error::invalid_input(format!("host rejected CSV batch: {error:?}")))?;
        self.entity_count = self.entity_count.saturating_add(u64::from(row_count));
        self.batch_count = self.batch_count.saturating_add(1);
        self.payload_bytes = self.payload_bytes.saturating_add(payload_len);
        Ok(())
    }

    fn summary(&self) -> TransitionSummary {
        TransitionSummary {
            entity_count: self.entity_count,
            batch_count: self.batch_count,
            payload_bytes: self.payload_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransitionSummary {
    pub entity_count: u64,
    pub batch_count: u32,
    pub payload_bytes: u64,
}

#[derive(Debug)]
pub struct OpenFile<'a> {
    pub file: FileInfo,
    pub accepted: Root<'a>,
    pub successor: Transaction,
    pub creates: CreateContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SpliceInsert {
    Inline(Vec<u8>),
    AfterRange { offset: u64, length: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputSplice {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: SpliceInsert,
}

#[derive(Debug)]
pub struct FileUpdate<'a> {
    pub before_file: FileInfo,
    pub after_file: FileInfo,
    pub before: Root<'a>,
    pub edits: Vec<InputSplice>,
    pub successor: Transaction,
    pub creates: CreateContext,
}

pub trait FormatPlugin: 'static {
    fn open_file(input: &OpenFile<'_>, sink: &mut Sink<'_>) -> Result<()>;

    fn file_changed(update: &FileUpdate<'_>, sink: &mut Sink<'_>) -> Result<()>;
}

#[doc(hidden)]
#[derive(Debug)]
pub struct Component<P>(PhantomData<P>);

impl<P: FormatPlugin> Guest for Component<P> {
    fn open_file(
        input: OpenFileInput,
        sink: &TransitionSink,
    ) -> std::result::Result<FileTransition, PluginError> {
        let OpenFileInput {
            descriptor,
            accepted,
            successor,
            creates,
            max_batch_bytes,
        } = input;
        if max_batch_bytes == 0 {
            return Err(PluginError::LimitExceeded(
                "max-batch-bytes must be positive".to_owned(),
            ));
        }
        let input = OpenFile {
            file: FileInfo {
                path: descriptor.path,
                media_type: descriptor.media_type,
            },
            accepted: Root { inner: &accepted },
            successor: Transaction { inner: successor },
            creates: CreateContext {
                high: creates.high,
                low: creates.low,
            },
        };
        let mut sink = Sink {
            inner: sink,
            max_batch_bytes,
            entity_count: 0,
            batch_count: 0,
            payload_bytes: 0,
        };
        P::open_file(&input, &mut sink).map_err(plugin_error)?;
        let summary = sink.summary();
        Ok(FileTransition {
            successor: input.successor.inner,
            summary: WitTransitionSummary {
                entity_count: summary.entity_count,
                batch_count: summary.batch_count,
                payload_bytes: summary.payload_bytes,
            },
        })
    }

    fn file_changed(
        input: WitFileUpdate,
        sink: &TransitionSink,
    ) -> std::result::Result<FileTransition, PluginError> {
        let WitFileUpdate {
            before_descriptor,
            after_descriptor,
            before,
            edits,
            successor,
            creates,
            max_batch_bytes,
        } = input;
        if max_batch_bytes == 0 {
            return Err(PluginError::LimitExceeded(
                "max-batch-bytes must be positive".to_owned(),
            ));
        }
        let edits = edits
            .into_iter()
            .map(|edit| InputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: match edit.insert {
                    InputBytes::Inline(bytes) => SpliceInsert::Inline(bytes),
                    InputBytes::AfterRange(range) => SpliceInsert::AfterRange {
                        offset: range.offset,
                        length: range.length,
                    },
                },
            })
            .collect();
        let input = FileUpdate {
            before_file: FileInfo {
                path: before_descriptor.path,
                media_type: before_descriptor.media_type,
            },
            after_file: FileInfo {
                path: after_descriptor.path,
                media_type: after_descriptor.media_type,
            },
            before: Root { inner: &before },
            edits,
            successor: Transaction { inner: successor },
            creates: CreateContext {
                high: creates.high,
                low: creates.low,
            },
        };
        let mut sink = Sink {
            inner: sink,
            max_batch_bytes,
            entity_count: 0,
            batch_count: 0,
            payload_bytes: 0,
        };
        P::file_changed(&input, &mut sink).map_err(plugin_error)?;
        let summary = sink.summary();
        Ok(FileTransition {
            successor: input.successor.inner,
            summary: WitTransitionSummary {
                entity_count: summary.entity_count,
                batch_count: summary.batch_count,
                payload_bytes: summary.payload_bytes,
            },
        })
    }
}

#[macro_export]
macro_rules! export_v3_prototype {
    ($plugin:ty) => {
        type __LixPluginApiV3PrototypeComponent = $crate::Component<$plugin>;
        $crate::__export_component_v3_prototype!(__LixPluginApiV3PrototypeComponent);
    };
}
