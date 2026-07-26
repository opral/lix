#![allow(clippy::same_length_and_capacity)]

wit_bindgen::generate!({
    path: "../../packages/engine/wit/v2",
    world: "plugin",
});

use crate::core::{
    ByteEdit as CoreByteEdit, Document as CoreDocument, EntityChange as CoreEntityChange,
    EntityImportBuilder, IdNamespace as CoreIdNamespace, InputSplice as CoreInputSplice,
    ROW_SCHEMA_KEY, RowConflictResolution, resolve_row_conflict,
};
use crate::packet::{
    ChangeStream, ConflictRecord, ConflictResolution, ConflictSnapshot, FORMAT_VERSION,
    ResolutionStream, decode_change_page, decode_conflict_page, decode_entity_page,
};
use exports::lix::plugin::api::{
    ByteOutputs, ChangeCursor, ChangePage, Document, EditCursor, EditPage, EntityTransition,
    EntityUpdate, FileTransition, FileUpdate, Guest, GuestByteOutputs, GuestChangeCursor,
    GuestDocument, GuestEditCursor, GuestResolutionCursor, InputBytes, OpenEntitiesInput,
    OpenFileInput, OutputBytes, OutputRange, OutputSplice, PluginError, ResolutionCursor,
    ResolutionPage,
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

/// The static resolver owns the lazily supplied conflict source.  It consumes
/// one bounded input page at a time when the host drains its output cursor, so
/// a large file with many independent conflicts never creates a guest-sized
/// list of snapshots or resolutions.
struct CsvResolutionCursor {
    state: RefCell<ResolutionCursorState>,
}

struct ResolutionCursorState {
    source: PacketSource,
    pending: ResolutionStream,
    source_eof: bool,
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

fn materialize_conflict_snapshot(
    snapshot: &ConflictSnapshot,
    attachments: Option<&ByteSources>,
    budget: &TransitionBudget,
) -> Result<Vec<u8>, PluginError> {
    snapshot
        .materialize(&mut |index, offset, length| {
            read_attachment(attachments, budget, index, offset, length)
        })
        .map_err(plugin_error)
}

/// Conflict values are normally one compact CSV row. A pathological giant row
/// is an edge case rather than a reason to copy three multi-megabyte values
/// through the guest; retain the deterministic lazy b-wins path there.
const MAX_HEURISTIC_CONFLICT_SNAPSHOT_BYTES: u64 = 64 * 1024;

fn resolve_conflict_record(
    conflict: ConflictRecord,
    attachments: Option<&ByteSources>,
    budget: &TransitionBudget,
) -> Result<ConflictResolution, PluginError> {
    // Non-row schema records (including the CSV table/dialect entity) have no
    // safe field-level merge. Their canonical b version wins without
    // touching any lazy snapshot attachment.
    if conflict.schema_key != ROW_SCHEMA_KEY {
        return Ok(if conflict.b.is_some() {
            ConflictResolution::TakeB
        } else {
            ConflictResolution::Delete
        });
    }

    let (Some(base), Some(a), Some(b)) = (&conflict.base, &conflict.a, &conflict.b) else {
        return Ok(if conflict.b.is_some() {
            ConflictResolution::TakeB
        } else {
            ConflictResolution::Delete
        });
    };
    if [base, a, b]
        .into_iter()
        .any(|snapshot| snapshot.len() > MAX_HEURISTIC_CONFLICT_SNAPSHOT_BYTES)
    {
        return Ok(ConflictResolution::TakeB);
    }

    // The row-specific algorithm is the only path which reads all three
    // snapshots. A take result remains source-backed on the host; a composed
    // row becomes the one small replacement attachment when necessary.
    let base = materialize_conflict_snapshot(base, attachments, budget)?;
    let a = materialize_conflict_snapshot(a, attachments, budget)?;
    let b = materialize_conflict_snapshot(b, attachments, budget)?;
    Ok(
        match resolve_row_conflict(Some(&base), Some(&a), Some(&b)) {
            RowConflictResolution::TakeA => ConflictResolution::TakeA,
            RowConflictResolution::TakeB => ConflictResolution::TakeB,
            RowConflictResolution::Replace(snapshot) => ConflictResolution::Replace(snapshot),
            RowConflictResolution::Delete => ConflictResolution::Delete,
        },
    )
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
    type ResolutionCursor = CsvResolutionCursor;

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
        let accepted = input
            .accepted
            .as_ref()
            .map(|source| read_source(source, budget))
            .transpose()?;
        let mut builder = EntityImportBuilder::new();
        drain_entities_into_builder(&input.entities, budget, &mut builder)?;
        let (document, mut edit) = builder.finish().map_err(plugin_error)?;
        let edits = match accepted {
            Some(accepted) if edit.insert.as_ref() == &accepted => Vec::new(),
            Some(accepted) => {
                edit.delete_len = u64::try_from(accepted.len())
                    .map_err(|_| plugin_error("accepted CSV is too large"))?;
                vec![edit]
            }
            None if edit.insert.is_empty() => Vec::new(),
            None => vec![edit],
        };
        Ok(entity_transition(document, edits))
    }

    fn resolve_conflicts(
        _budget: &TransitionBudget,
        input: exports::lix::plugin::api::ConflictUpdate,
    ) -> Result<ResolutionCursor, PluginError> {
        // Keep the packet source owned by the returned cursor.  The host's
        // transition budget spans both this call and cursor draining, so this
        // is still one bounded transition while avoiding eager conflict input
        // materialization in guest memory.
        Ok(ResolutionCursor::new(CsvResolutionCursor {
            state: RefCell::new(ResolutionCursorState {
                source: input.conflicts,
                pending: ResolutionStream::default(),
                source_eof: false,
                eof: false,
            }),
        }))
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
        let before_path = update.before_descriptor.path.clone();
        let after_path = update.after_descriptor.path.clone();
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
            .file_changed_with_paths(
                &splices,
                before_path.as_deref(),
                after_path.as_deref(),
                namespace,
            )
            .map_err(plugin_error)?;
        Ok(file_transition(document, ChangeStream::ready(changes)))
    }

    fn entities_changed(
        &self,
        budget: &TransitionBudget,
        update: EntityUpdate,
    ) -> Result<EntityTransition, PluginError> {
        let changes = drain_changes(&update.changes, budget)?;
        let (document, edits) = self.0.entities_changed(&changes).map_err(plugin_error)?;
        Ok(entity_transition(document, edits))
    }
}

impl GuestChangeCursor for CsvChangeCursor {
    fn next(
        &self,
        budget: &TransitionBudget,
        max_bytes: u32,
    ) -> Result<Option<ChangePage>, PluginError> {
        let max_record_bytes = budget.limits().max_record_bytes;
        let mut state = self.state.borrow_mut();
        if state.eof {
            return Ok(None);
        }
        let page = {
            let ChangeCursorState {
                stream, pending, ..
            } = &mut *state;
            stream.next_page(pending, max_bytes, max_record_bytes)
        }
        .map_err(|error| {
            if error.contains("record cap") {
                PluginError::RecordTooLarge(u64::from(max_record_bytes) + 1)
            } else if error.contains("page cap") {
                PluginError::RecordTooLarge(u64::from(max_bytes) + 1)
            } else {
                plugin_error(error)
            }
        })?;
        let Some(page) = page else {
            state.eof = true;
            return Ok(None);
        };
        let attachments = if page.attachments.is_empty() {
            None
        } else {
            Some(ByteOutputs::new(CsvByteOutputs {
                values: page.attachments,
            }))
        };
        Ok(Some(ChangePage {
            format_version: FORMAT_VERSION,
            record_count: page.record_count,
            payload: page.payload,
            attachments,
        }))
    }
}

impl GuestResolutionCursor for CsvResolutionCursor {
    fn next(
        &self,
        budget: &TransitionBudget,
        max_bytes: u32,
    ) -> Result<Option<ResolutionPage>, PluginError> {
        let mut state = self.state.borrow_mut();
        if state.eof {
            return Ok(None);
        }
        let max_record_bytes = budget.limits().max_record_bytes;
        loop {
            if let Some(page) = state
                .pending
                .next_page(max_bytes, max_record_bytes)
                .map_err(|error| {
                    if error.contains("record cap") {
                        PluginError::RecordTooLarge(u64::from(max_record_bytes) + 1)
                    } else if error.contains("page cap") {
                        PluginError::RecordTooLarge(u64::from(max_bytes) + 1)
                    } else {
                        plugin_error(error)
                    }
                })?
            {
                let attachments = if page.attachments.is_empty() {
                    None
                } else {
                    Some(ByteOutputs::new(CsvByteOutputs {
                        values: page.attachments,
                    }))
                };
                return Ok(Some(ResolutionPage {
                    format_version: FORMAT_VERSION,
                    record_count: page.record_count,
                    payload: page.payload,
                    attachments,
                }));
            }
            if state.source_eof {
                state.eof = true;
                return Ok(None);
            }

            let source_page_cap = budget.limits().max_page_bytes.max(1);
            let Some(page) = state
                .source
                .next(budget, source_page_cap)
                .map_err(source_error)?
            else {
                state.source_eof = true;
                continue;
            };
            if page.format_version != FORMAT_VERSION {
                return Err(plugin_error(format!(
                    "unsupported packet version {}",
                    page.format_version
                )));
            }
            let conflicts =
                decode_conflict_page(&page.payload, page.record_count).map_err(plugin_error)?;
            let mut resolutions = Vec::with_capacity(conflicts.len());
            for conflict in conflicts {
                let ordinal = conflict.ordinal;
                let resolution =
                    resolve_conflict_record(conflict, page.attachments.as_ref(), budget)?;
                resolutions.push((ordinal, resolution));
            }
            state.pending.extend(resolutions);
        }
    }
}

impl GuestEditCursor for CsvEditCursor {
    fn next(
        &self,
        budget: &TransitionBudget,
        max_edits: u32,
        max_inline_bytes: u32,
    ) -> Result<Option<EditPage>, PluginError> {
        const EDIT_METADATA_BYTES: usize = 24;
        let mut state = self.state.borrow_mut();
        if state.eof {
            return Ok(None);
        }
        if max_edits == 0 {
            return Err(PluginError::LimitExceeded(
                "edit cursor max-edits must be positive".to_owned(),
            ));
        }
        let limits = budget.limits();
        let record_limit = usize::try_from(limits.max_record_bytes).expect("u32 fits usize");
        let page_limit = usize::try_from(limits.max_page_bytes).expect("u32 fits usize");
        if record_limit < EDIT_METADATA_BYTES || page_limit < EDIT_METADATA_BYTES {
            return Err(PluginError::RecordTooLarge(EDIT_METADATA_BYTES as u64));
        }
        let mut edits = Vec::new();
        let mut outputs = Vec::<Arc<Vec<u8>>>::new();
        let inline_limit = usize::try_from(max_inline_bytes).expect("u32 fits usize");
        let mut inline_used = 0usize;
        let mut page_used = 0usize;
        for _ in 0..max_edits {
            let Some(edit) = state.edits.pop_front() else {
                break;
            };
            if page_used + EDIT_METADATA_BYTES > page_limit {
                state.edits.push_front(edit);
                break;
            }
            let inline_record_len = EDIT_METADATA_BYTES
                .checked_add(edit.insert.len())
                .ok_or_else(|| {
                    PluginError::LimitExceeded("edit record length overflow".to_owned())
                })?;
            let next_inline_used = inline_used.checked_add(edit.insert.len()).ok_or_else(|| {
                PluginError::LimitExceeded("edit inline-byte counter overflow".to_owned())
            })?;
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
