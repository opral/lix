#![allow(clippy::same_length_and_capacity)]

wit_bindgen::generate!({
    path: "../../packages/engine/wit/v2",
    world: "plugin",
});

use crate::core::{
    ByteEdit as CoreByteEdit, ChangeEffect, Document as CoreDocument,
    EntityChange as CoreEntityChange, EntityImportBuilder, EntityRecord as CoreEntityRecord,
    IdNamespace as CoreIdNamespace, InputSplice as CoreInputSplice,
};
use crate::packet::{ChangeStream, FORMAT_VERSION, decode_change_page, decode_entity_page};
use exports::lix::plugin::api::{
    ByteOutputs, ChangeCursor, ChangePage, Document, EditCursor, EditPage, EntityTransition,
    EntityUpdate, FileTransition, FileUpdate, Guest, GuestByteOutputs, GuestChangeCursor,
    GuestDocument, GuestEditCursor, InputBytes, OpenEntitiesInput, OpenFileInput, OutputBytes,
    OutputRange, OutputSplice, PluginError,
};
use lix::plugin::host::{ByteSource, ByteSources, PacketSource, SourceError, TransitionBudget};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;

struct CsvGuest;

#[derive(Debug)]
struct CsvDocument(CoreDocument);

#[derive(Debug)]
struct CsvChangeCursor {
    state: RefCell<ChangeCursorState>,
}

#[derive(Debug)]
struct ChangeCursorState {
    stream: ChangeStream,
    pending: Option<CoreEntityChange>,
    eof: bool,
}

#[derive(Debug)]
struct CsvEditCursor {
    state: RefCell<EditCursorState>,
}

#[derive(Debug)]
struct EditCursorState {
    edits: VecDeque<CoreByteEdit>,
    eof: bool,
}

#[derive(Debug)]
struct CsvByteOutputs {
    values: Vec<Arc<Vec<u8>>>,
}

fn plugin_error(error: impl Into<String>) -> PluginError {
    PluginError::InvalidInput(error.into())
}

fn source_error(error: SourceError) -> PluginError {
    match error {
        SourceError::InvalidRange => {
            PluginError::InvalidInput("invalid byte-source range".to_owned())
        }
        SourceError::RecordTooLarge(size) => PluginError::RecordTooLarge(size),
        SourceError::LimitExceeded(message) => PluginError::LimitExceeded(message),
        SourceError::DeadlineExceeded => PluginError::DeadlineExceeded,
        SourceError::Unavailable(message) => PluginError::Internal(message),
    }
}

fn read_source(source: &ByteSource, budget: &TransitionBudget) -> Result<Vec<u8>, PluginError> {
    read_source_range(source, budget, 0, source.len())
}

fn read_source_range(
    source: &ByteSource,
    budget: &TransitionBudget,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>, PluginError> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| plugin_error("byte-source range overflow"))?;
    if end > source.len() {
        return Err(plugin_error("byte-source range exceeds source"));
    }
    let mut output = Vec::with_capacity(
        usize::try_from(length)
            .map_err(|_| PluginError::LimitExceeded("source is too large".to_owned()))?,
    );
    let page_cap = budget.limits().max_page_bytes.max(1);
    let mut cursor = offset;
    while cursor < end {
        let remaining = end - cursor;
        let request = u32::try_from(remaining.min(u64::from(page_cap))).expect("bounded by u32");
        let page = source.read(budget, cursor, request).map_err(source_error)?;
        if page.is_empty() {
            return Err(PluginError::Internal(
                "byte source returned an empty page before EOF".to_owned(),
            ));
        }
        if page.len() > usize::try_from(request).expect("u32 fits usize") {
            return Err(PluginError::Internal(
                "byte source returned more bytes than requested".to_owned(),
            ));
        }
        cursor += u64::try_from(page.len()).expect("usize fits u64");
        output.extend_from_slice(&page);
    }
    Ok(output)
}

fn read_attachment(
    attachments: Option<&ByteSources>,
    budget: &TransitionBudget,
    index: u32,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>, String> {
    let attachments = attachments.ok_or_else(|| "packet attachment table is missing".to_owned())?;
    let source_len = attachments
        .len(index)
        .map_err(|error| format!("attachment len failed: {error:?}"))?;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| "attachment range overflow".to_owned())?;
    if end > source_len {
        return Err("attachment range exceeds source".to_owned());
    }
    let page_cap = budget.limits().max_page_bytes.max(1);
    let mut output = Vec::with_capacity(
        usize::try_from(length).map_err(|_| "attachment is too large".to_owned())?,
    );
    let mut cursor = offset;
    while cursor < end {
        let request =
            u32::try_from((end - cursor).min(u64::from(page_cap))).expect("bounded by u32");
        let page = attachments
            .read(budget, index, cursor, request)
            .map_err(|error| format!("attachment read failed: {error:?}"))?;
        if page.is_empty() {
            return Err("attachment returned an empty page before EOF".to_owned());
        }
        cursor += u64::try_from(page.len()).expect("usize fits u64");
        output.extend_from_slice(&page);
    }
    Ok(output)
}

fn drain_entities(
    source: &PacketSource,
    budget: &TransitionBudget,
) -> Result<Vec<CoreEntityRecord>, PluginError> {
    let mut output = Vec::new();
    let max_bytes = budget.limits().max_page_bytes.max(1);
    loop {
        let Some(page) = source.next(budget, max_bytes).map_err(source_error)? else {
            break;
        };
        if page.format_version != FORMAT_VERSION {
            return Err(plugin_error(format!(
                "unsupported packet version {}",
                page.format_version
            )));
        }
        let records =
            decode_entity_page(&page.payload, page.record_count, |index, offset, length| {
                read_attachment(page.attachments.as_ref(), budget, index, offset, length)
            })
            .map_err(plugin_error)?;
        output.extend(records);
    }
    Ok(output)
}

fn drain_entities_into_builder(
    source: &PacketSource,
    budget: &TransitionBudget,
    builder: &mut EntityImportBuilder,
) -> Result<(), PluginError> {
    let max_bytes = budget.limits().max_page_bytes.max(1);
    loop {
        let Some(page) = source.next(budget, max_bytes).map_err(source_error)? else {
            break;
        };
        if page.format_version != FORMAT_VERSION {
            return Err(plugin_error(format!(
                "unsupported packet version {}",
                page.format_version
            )));
        }
        let records =
            decode_entity_page(&page.payload, page.record_count, |index, offset, length| {
                read_attachment(page.attachments.as_ref(), budget, index, offset, length)
            })
            .map_err(plugin_error)?;
        for record in records {
            builder.push(record).map_err(plugin_error)?;
        }
    }
    Ok(())
}

fn drain_changes(
    source: &PacketSource,
    budget: &TransitionBudget,
) -> Result<Vec<CoreEntityChange>, PluginError> {
    let mut output = Vec::new();
    let max_bytes = budget.limits().max_page_bytes.max(1);
    loop {
        let Some(page) = source.next(budget, max_bytes).map_err(source_error)? else {
            break;
        };
        if page.format_version != FORMAT_VERSION {
            return Err(plugin_error(format!(
                "unsupported packet version {}",
                page.format_version
            )));
        }
        let changes =
            decode_change_page(&page.payload, page.record_count, |index, offset, length| {
                read_attachment(page.attachments.as_ref(), budget, index, offset, length)
            })
            .map_err(plugin_error)?;
        output.extend(changes);
    }
    Ok(output)
}

fn file_transition(document: CoreDocument, stream: ChangeStream) -> FileTransition {
    FileTransition {
        document: Document::new(CsvDocument(document)),
        changes: ChangeCursor::new(CsvChangeCursor {
            state: RefCell::new(ChangeCursorState {
                stream,
                pending: None,
                eof: false,
            }),
        }),
    }
}

fn entity_transition(document: CoreDocument, edits: Vec<CoreByteEdit>) -> EntityTransition {
    EntityTransition {
        document: Document::new(CsvDocument(document)),
        edits: EditCursor::new(CsvEditCursor {
            state: RefCell::new(EditCursorState {
                edits: edits.into(),
                eof: false,
            }),
        }),
    }
}

impl Guest for CsvGuest {
    type ByteOutputs = CsvByteOutputs;
    type ChangeCursor = CsvChangeCursor;
    type EditCursor = CsvEditCursor;
    type Document = CsvDocument;

    fn open_file(
        budget: &TransitionBudget,
        input: OpenFileInput,
    ) -> Result<FileTransition, PluginError> {
        let bytes = read_source(&input.file, budget)?;
        let namespace = CoreIdNamespace::from_halves(input.ids.high, input.ids.low);
        let (document, changes) =
            CoreDocument::open_file(bytes, input.descriptor.path.as_deref(), namespace)
                .map_err(plugin_error)?;
        Ok(file_transition(document, ChangeStream::Initial(changes)))
    }

    fn open_entities(
        budget: &TransitionBudget,
        input: OpenEntitiesInput,
    ) -> Result<EntityTransition, PluginError> {
        let mut builder = EntityImportBuilder::new();
        drain_entities_into_builder(&input.entities, budget, &mut builder)?;
        let (document, edit) = builder.finish().map_err(plugin_error)?;
        let edits = if edit.insert.is_empty() {
            Vec::new()
        } else {
            vec![edit]
        };
        Ok(entity_transition(document, edits))
    }
}

impl GuestDocument for CsvDocument {
    fn fork(&self) -> Document {
        Document::new(Self(self.0.fork()))
    }

    fn file_changed(
        &self,
        budget: &TransitionBudget,
        update: FileUpdate,
    ) -> Result<FileTransition, PluginError> {
        let mut owned = Vec::with_capacity(update.edits.len());
        for edit in update.edits {
            let insert = match edit.insert {
                InputBytes::Inline(bytes) => bytes,
                InputBytes::AfterRange(range) => {
                    read_source_range(&update.after, budget, range.offset, range.length)?
                }
            };
            owned.push((edit.offset, edit.delete_len, insert));
        }
        let splices = owned
            .iter()
            .map(|(offset, delete_len, insert)| CoreInputSplice {
                offset: *offset,
                delete_len: *delete_len,
                insert,
            })
            .collect::<Vec<_>>();
        let namespace = CoreIdNamespace::from_halves(update.ids.high, update.ids.low);
        let (document, changes) = self
            .0
            .file_changed(&splices, namespace)
            .map_err(plugin_error)?;
        Ok(file_transition(document, ChangeStream::ready(changes)))
    }

    fn entities_changed(
        &self,
        budget: &TransitionBudget,
        update: EntityUpdate,
    ) -> Result<EntityTransition, PluginError> {
        let mut changes = drain_changes(&update.changes, budget)?;
        for activated in drain_entities(&update.activated_entities, budget)? {
            changes.push(CoreEntityChange {
                schema_key: activated.schema_key,
                entity_pk: activated.entity_pk,
                snapshot: Some(activated.snapshot),
                effect: ChangeEffect::Content,
            });
        }
        let (document, edits) = self.0.entities_changed(&changes).map_err(plugin_error)?;
        Ok(entity_transition(document, edits))
    }
}

impl GuestChangeCursor for CsvChangeCursor {
    fn next(
        &self,
        _budget: &TransitionBudget,
        max_bytes: u32,
    ) -> Result<Option<ChangePage>, PluginError> {
        let mut state = self.state.borrow_mut();
        if state.eof {
            return Ok(None);
        }
        let page = {
            let ChangeCursorState {
                stream, pending, ..
            } = &mut *state;
            stream.next_page(pending, max_bytes)
        }
        .map_err(|error| {
            if error.contains("page cap") {
                PluginError::RecordTooLarge(u64::from(max_bytes) + 1)
            } else {
                plugin_error(error)
            }
        })?;
        let Some(page) = page else {
            state.eof = true;
            return Ok(None);
        };
        Ok(Some(ChangePage {
            format_version: FORMAT_VERSION,
            record_count: page.record_count,
            payload: page.payload,
            attachments: None,
        }))
    }
}

impl GuestEditCursor for CsvEditCursor {
    fn next(
        &self,
        _budget: &TransitionBudget,
        max_edits: u32,
        max_inline_bytes: u32,
    ) -> Result<Option<EditPage>, PluginError> {
        if max_edits == 0 {
            return Err(PluginError::LimitExceeded(
                "edit cursor max-edits must be positive".to_owned(),
            ));
        }
        let mut state = self.state.borrow_mut();
        if state.eof {
            return Ok(None);
        }
        let mut edits = Vec::new();
        let mut outputs = Vec::<Arc<Vec<u8>>>::new();
        let mut inline_remaining = usize::try_from(max_inline_bytes).expect("u32 fits usize");
        for _ in 0..max_edits {
            let Some(edit) = state.edits.pop_front() else {
                break;
            };
            let insert = if edit.insert.len() <= inline_remaining {
                inline_remaining -= edit.insert.len();
                OutputBytes::Inline(edit.insert.as_ref().clone())
            } else {
                let index = u32::try_from(outputs.len())
                    .map_err(|_| PluginError::LimitExceeded("too many edit outputs".to_owned()))?;
                let length = u64::try_from(edit.insert.len()).expect("usize fits u64");
                outputs.push(edit.insert);
                OutputBytes::Output(OutputRange {
                    index,
                    offset: 0,
                    length,
                })
            };
            edits.push(OutputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert,
            });
        }
        if edits.is_empty() {
            state.eof = true;
            return Ok(None);
        }
        let outputs = if outputs.is_empty() {
            None
        } else {
            Some(ByteOutputs::new(CsvByteOutputs { values: outputs }))
        };
        Ok(Some(EditPage { edits, outputs }))
    }
}

impl GuestByteOutputs for CsvByteOutputs {
    fn len(&self, index: u32) -> Result<u64, PluginError> {
        self.values
            .get(usize::try_from(index).expect("u32 fits usize"))
            .map(|value| u64::try_from(value.len()).expect("usize fits u64"))
            .ok_or_else(|| plugin_error("invalid byte-output index"))
    }

    fn read(
        &self,
        _budget: &TransitionBudget,
        index: u32,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, PluginError> {
        let value = self
            .values
            .get(usize::try_from(index).expect("u32 fits usize"))
            .ok_or_else(|| plugin_error("invalid byte-output index"))?;
        let start = usize::try_from(offset).map_err(|_| plugin_error("output offset overflow"))?;
        let end = start
            .checked_add(usize::try_from(length).expect("u32 fits usize"))
            .ok_or_else(|| plugin_error("output range overflow"))?;
        value
            .get(start..end)
            .map(ToOwned::to_owned)
            .ok_or_else(|| plugin_error("output range exceeds value"))
    }
}

#[cfg(target_family = "wasm")]
export!(CsvGuest);
