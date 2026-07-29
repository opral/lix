//! Minimal authoring layer for the fused Component API v3 experiment.

#![allow(clippy::missing_errors_doc)]

wit_bindgen::generate!({
    path: "wit",
    world: "plugin",
    pub_export_macro: true,
    export_macro_name: "__export_component_v3_prototype",
    default_bindings_module: "lix_plugin_api_v3_prototype",
});

use exports::lix::plugin::api::{
    Document, FileTransition, FileUpdate as WitFileUpdate, Guest, GuestDocument, InputBytes,
    OpenFileInput, PluginError, TransitionSummary as WitTransitionSummary,
};
use lix::plugin::host::{ChangePage, CsvRowBatch, Source as WitSource, TransitionSink};
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileInfo {
    pub path: Option<String>,
    pub media_type: Option<String>,
}

pub struct Source<'a> {
    inner: &'a WitSource,
}

impl std::fmt::Debug for Source<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Source")
            .field("len", &self.len())
            .finish()
    }
}

impl Source<'_> {
    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    pub fn read_all(&self) -> Result<Vec<u8>> {
        const READ_BYTES: u32 = 1024 * 1024;
        let length = self.len();
        let capacity = usize::try_from(length)
            .map_err(|_| Error::limit_exceeded("source length exceeds guest address space"))?;
        if let Ok(length) = u32::try_from(length) {
            let bytes = self
                .inner
                .read(0, length)
                .map_err(|error| Error::invalid_input(format!("source read failed: {error:?}")))?;
            if bytes.len() != capacity {
                return Err(Error::invalid_input("source returned a short read"));
            }
            return Ok(bytes);
        }
        let mut output = Vec::with_capacity(capacity);
        let mut offset = 0_u64;
        while offset < length {
            let remaining = length - offset;
            let chunk = u32::try_from(remaining.min(u64::from(READ_BYTES)))
                .expect("bounded source read fits u32");
            let bytes = self
                .inner
                .read(offset, chunk)
                .map_err(|error| Error::invalid_input(format!("source read failed: {error:?}")))?;
            if bytes.len() != chunk as usize {
                return Err(Error::invalid_input("source returned a short read"));
            }
            output.extend_from_slice(&bytes);
            offset += u64::from(chunk);
        }
        Ok(output)
    }

    pub fn read_range(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        const READ_BYTES: u32 = 1024 * 1024;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Error::invalid_input("source range overflowed"))?;
        if end > self.len() {
            return Err(Error::invalid_input("source range exceeds its input"));
        }
        let capacity = usize::try_from(length)
            .map_err(|_| Error::limit_exceeded("source range exceeds guest address space"))?;
        let mut output = Vec::with_capacity(capacity);
        let mut cursor = offset;
        while cursor < end {
            let chunk = u32::try_from((end - cursor).min(u64::from(READ_BYTES)))
                .expect("bounded source read fits u32");
            let bytes = self
                .inner
                .read(cursor, chunk)
                .map_err(|error| Error::invalid_input(format!("source read failed: {error:?}")))?;
            if bytes.len() != chunk as usize {
                return Err(Error::invalid_input("source returned a short read"));
            }
            output.extend_from_slice(&bytes);
            cursor += u64::from(chunk);
        }
        Ok(output)
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
            .finish()
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

pub struct OpenFile<'a> {
    pub file: FileInfo,
    pub source: Source<'a>,
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
    pub before: Source<'a>,
    pub edits: Vec<InputSplice>,
    pub after: Source<'a>,
    pub creates: CreateContext,
}

pub trait FormatPlugin: 'static {
    type Document: Clone + 'static;

    fn open_file(input: OpenFile<'_>, sink: &mut Sink<'_>) -> Result<Self::Document>;

    fn file_changed(
        document: &Self::Document,
        update: FileUpdate<'_>,
        sink: &mut Sink<'_>,
    ) -> Result<Self::Document>;
}

#[doc(hidden)]
pub struct Component<P>(PhantomData<P>);

impl<P: FormatPlugin> Guest for Component<P> {
    type Document = AuthorDocument<P>;

    fn open_file(
        input: OpenFileInput,
        sink: &TransitionSink,
    ) -> std::result::Result<FileTransition, PluginError> {
        if input.max_batch_bytes == 0 {
            return Err(PluginError::LimitExceeded(
                "max-batch-bytes must be positive".to_owned(),
            ));
        }
        let author_input = OpenFile {
            file: FileInfo {
                path: input.descriptor.path,
                media_type: input.descriptor.media_type,
            },
            source: Source { inner: &input.file },
            creates: CreateContext {
                high: input.creates.high,
                low: input.creates.low,
            },
        };
        let mut author_sink = Sink {
            inner: sink,
            max_batch_bytes: input.max_batch_bytes,
            entity_count: 0,
            batch_count: 0,
            payload_bytes: 0,
        };
        let document = P::open_file(author_input, &mut author_sink).map_err(plugin_error)?;
        let summary = author_sink.summary();
        Ok(FileTransition {
            document: Document::new(AuthorDocument::<P>(document)),
            summary: WitTransitionSummary {
                entity_count: summary.entity_count,
                batch_count: summary.batch_count,
                payload_bytes: summary.payload_bytes,
            },
        })
    }
}

#[doc(hidden)]
pub struct AuthorDocument<P: FormatPlugin>(P::Document);

impl<P: FormatPlugin> std::fmt::Debug for AuthorDocument<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_tuple("AuthorDocument").finish()
    }
}

impl<P: FormatPlugin> GuestDocument for AuthorDocument<P> {
    fn fork(&self) -> Document {
        Document::new(Self(self.0.clone()))
    }

    fn file_changed(
        &self,
        input: WitFileUpdate,
        sink: &TransitionSink,
    ) -> std::result::Result<FileTransition, PluginError> {
        if input.max_batch_bytes == 0 {
            return Err(PluginError::LimitExceeded(
                "max-batch-bytes must be positive".to_owned(),
            ));
        }
        let edits = input
            .edits
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
        let update = FileUpdate {
            before_file: FileInfo {
                path: input.before_descriptor.path,
                media_type: input.before_descriptor.media_type,
            },
            after_file: FileInfo {
                path: input.after_descriptor.path,
                media_type: input.after_descriptor.media_type,
            },
            before: Source {
                inner: &input.before,
            },
            edits,
            after: Source {
                inner: &input.after,
            },
            creates: CreateContext {
                high: input.creates.high,
                low: input.creates.low,
            },
        };
        let mut author_sink = Sink {
            inner: sink,
            max_batch_bytes: input.max_batch_bytes,
            entity_count: 0,
            batch_count: 0,
            payload_bytes: 0,
        };
        let document = P::file_changed(&self.0, update, &mut author_sink).map_err(plugin_error)?;
        let summary = author_sink.summary();
        Ok(FileTransition {
            document: Document::new(Self(document)),
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
