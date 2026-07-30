//! Functional CSV port to Plugin API v3.
//!
//! This first profile lane deliberately reconstructs the document from durable
//! rows in a cold guest. The measured result is the baseline for replacing it
//! with affected row-span and identity pages.

#![cfg_attr(not(target_family = "wasm"), allow(dead_code))]

use lix_plugin_api_v3 as sdk;
use plugin_csv_core::{
    ByteEdit as CoreByteEdit, ChangeEffect as CoreChangeEffect, Document as CoreDocument,
    EntityChange as CoreEntityChange, EntityRecord as CoreEntityRecord, IdNamespace,
    InputSplice as CoreInputSplice, ROW_SCHEMA_KEY, RowConflictResolution, V3ColdIndex,
    V3ColdMetadata, V3RowFramer, V3RowWindowCheckpoint, V3StreamAnalyzer, encode_row_snapshot,
    parse_row_snapshot, resolve_row_conflict, v3_stream_table_change,
};

const INDEX_METADATA_KEY: &[u8] = b"csv/v3/index-metadata";
const INDEX_VERSION: &[u8] = b"row-window-checkpoints-v3";
const INDEX_KEY_PREFIX: &[u8] = b"csv/v3/rows/";
const INDEX_WINDOW_BYTES: u64 = 16 * 1024;

struct CsvV3Plugin;

struct CsvColdSource {
    root: sdk::Root,
    framer: V3RowFramer,
    metadata: V3ColdMetadata,
    file_len: u64,
    read_offset: u64,
    ordinal: usize,
    table_pending: bool,
    pending_record: Option<Vec<u8>>,
}

impl CsvColdSource {
    fn load_page(&mut self, budget: &sdk::Budget) -> sdk::Result<()> {
        if self.read_offset == self.file_len {
            return Err(sdk::Error::invalid_input(
                "CSV page source ended before the analyzed row count",
            ));
        }
        let requested = u32::try_from(
            (self.file_len - self.read_offset)
                .min(u64::from(budget.limits().max_page_bytes.max(1))),
        )
        .expect("CSV page request is bounded by max-page-bytes");
        let page = self
            .root
            .read_file(budget, self.read_offset, requested)
            .map_err(arena_error)?;
        self.read_offset += page.len() as u64;
        self.framer.push(&page).map_err(sdk::Error::invalid_input)
    }
}

impl sdk::EntityChangePacketSource for CsvColdSource {
    fn next_packet(
        &mut self,
        budget: &sdk::Budget,
        max_bytes: u32,
    ) -> sdk::Result<Option<Vec<u8>>> {
        let mut packet = sdk::EntityChangePacketBuilder::new(max_bytes)?;
        if let Some(record) = self.pending_record.take() {
            if let Some(record) = packet.try_push_encoded_record(record)? {
                self.pending_record = Some(record);
                return Ok(packet.finish());
            }
        }
        loop {
            if self.ordinal < self.metadata.row_count() {
                let id = self
                    .metadata
                    .row_id_ascii(self.ordinal)
                    .map_err(sdk::Error::invalid_input)?;
                let id = std::str::from_utf8(&id)
                    .map_err(|_| sdk::Error::internal("generated CSV row ID is not ASCII"))?;
                let eof = self.read_offset == self.file_len;
                match packet.try_push_with_snapshot(ROW_SCHEMA_KEY, &[&id], false, |output| {
                    self.framer
                        .next_snapshot_into_with_id(eof, self.ordinal, self.metadata, id, output)
                        .map(|snapshot| snapshot.is_some())
                        .map_err(sdk::Error::invalid_input)
                })? {
                    sdk::DirectPacketPush::Pushed => {
                        self.ordinal += 1;
                    }
                    sdk::DirectPacketPush::Pending(record) => {
                        self.ordinal += 1;
                        self.pending_record = Some(record);
                        return Ok(packet.finish());
                    }
                    sdk::DirectPacketPush::NoRecord => self.load_page(budget)?,
                }
                continue;
            }
            if self.table_pending {
                let table = core_change_to_sdk(v3_stream_table_change(self.metadata));
                if packet.try_push(table)?.is_some() {
                    return Ok(packet.finish());
                }
                self.table_pending = false;
            }
            return Ok(packet.finish());
        }
    }
}

impl sdk::FormatPlugin for CsvV3Plugin {
    fn resolve_conflict(conflict: sdk::EntityConflict<'_>) -> sdk::Result<sdk::ConflictResolution> {
        const MAX_HEURISTIC_SNAPSHOT_BYTES: u64 = 64 * 1024;

        if conflict.schema_key != ROW_SCHEMA_KEY {
            return Ok(conflict.take_b_or_delete());
        }
        let (Some(base), Some(a), Some(b)) = (&conflict.base, &conflict.a, &conflict.b) else {
            return Ok(conflict.take_b_or_delete());
        };
        if [base, a, b]
            .into_iter()
            .any(|snapshot| snapshot.len() > MAX_HEURISTIC_SNAPSHOT_BYTES)
        {
            return Ok(sdk::ConflictResolution::TakeB);
        }
        let base = base.read()?;
        let a = a.read()?;
        let b = b.read()?;
        Ok(
            match resolve_row_conflict(Some(&base), Some(&a), Some(&b)) {
                RowConflictResolution::TakeA => sdk::ConflictResolution::TakeA,
                RowConflictResolution::TakeB => sdk::ConflictResolution::TakeB,
                RowConflictResolution::Replace(snapshot) => {
                    sdk::ConflictResolution::Replace(snapshot)
                }
                RowConflictResolution::Delete => sdk::ConflictResolution::Delete,
            },
        )
    }

    fn open_file(budget: &sdk::Budget, input: sdk::OpenFileInput) -> sdk::Result<sdk::FileResult> {
        let file_len = input.accepted.file_len();
        let mut analyzer = V3StreamAnalyzer::new(
            input.descriptor.path.as_deref(),
            namespace(input.creates),
            INDEX_WINDOW_BYTES,
        )
        .map_err(sdk::Error::invalid_input)?;
        let mut offset = 0_u64;
        while offset < file_len {
            let requested = u32::try_from(
                (file_len - offset).min(u64::from(budget.limits().max_page_bytes.max(1))),
            )
            .expect("CSV page request is bounded by max-page-bytes");
            let page = input
                .accepted
                .read_file(budget, offset, requested)
                .map_err(arena_error)?;
            offset += page.len() as u64;
            analyzer.push(&page).map_err(sdk::Error::invalid_input)?;
        }
        let (index, metadata) = analyzer.finish().map_err(sdk::Error::invalid_input)?;
        write_cold_row_index(
            &input.successor,
            budget,
            &index,
            metadata.row_count() as u64,
        )?;
        input
            .successor
            .declare_ordered_entity_output()
            .map_err(arena_error)?;
        Ok(sdk::FileResult {
            successor: input.successor,
            changes: sdk::EntityChanges::from_packet_source(CsvColdSource {
                root: input.accepted,
                framer: V3RowFramer::new(input.descriptor.path.as_deref()),
                metadata,
                file_len,
                read_offset: 0,
                ordinal: 0,
                table_pending: true,
                pending_record: None,
            }),
        })
    }

    fn file_changed(budget: &sdk::Budget, input: sdk::FileUpdate) -> sdk::Result<sdk::FileResult> {
        if let Some(changes) = sparse_row_file_changed(&input, budget)? {
            return Ok(sdk::FileResult {
                successor: input.successor,
                changes: changes.into(),
            });
        }
        let document = document_from_root(&input.before, budget)?;
        let inserts = input
            .edits
            .iter()
            .map(|edit| match &edit.insert {
                sdk::InputBytes::Inline(bytes) => Ok(bytes.clone()),
                sdk::InputBytes::AfterRange(range) => input
                    .successor
                    .read_file(
                        budget,
                        range.offset,
                        u32::try_from(range.length)
                            .map_err(|_| sdk::Error::RecordTooLarge(range.length))?,
                    )
                    .map_err(arena_error),
            })
            .collect::<sdk::Result<Vec<_>>>()?;
        let splices = input
            .edits
            .iter()
            .zip(&inserts)
            .map(|(edit, insert)| CoreInputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert,
            })
            .collect::<Vec<_>>();
        let (after, changes) = document
            .file_changed_with_paths(
                &splices,
                input.before_descriptor.path.as_deref(),
                input.after_descriptor.path.as_deref(),
                namespace(input.creates),
            )
            .map_err(sdk::Error::invalid_input)?;
        write_row_index(&input.successor, budget, &after)?;
        Ok(sdk::FileResult {
            successor: input.successor,
            changes: changes
                .into_iter()
                .map(core_change_to_sdk)
                .collect::<Vec<_>>()
                .into(),
        })
    }

    fn open_entities(
        budget: &sdk::Budget,
        input: sdk::OpenEntitiesInput,
    ) -> sdk::Result<sdk::EntityResult> {
        let entities = read_entities_from_root(&input.durable, budget)?;
        let (document, mut edit) =
            CoreDocument::open_entities(entities).map_err(sdk::Error::invalid_input)?;
        let accepted = read_root_bytes(&input.durable, budget)?;
        let edits = if edit.insert.as_slice() == accepted {
            Vec::new()
        } else {
            edit.delete_len = accepted.len() as u64;
            vec![core_edit_to_sdk(edit)]
        };
        write_row_index(&input.successor, budget, &document)?;
        Ok(sdk::EntityResult {
            successor: input.successor,
            edits,
        })
    }

    fn entities_changed(
        budget: &sdk::Budget,
        input: sdk::EntityUpdate,
    ) -> sdk::Result<sdk::EntityResult> {
        let document = document_from_root(&input.before, budget)?;
        let mut changes = Vec::with_capacity(input.changed_entities.len());
        for changed in &input.changed_entities {
            let (schema_key, entity_pk) = sdk::decode_entity_key(&changed.key)?;
            changes.push(CoreEntityChange {
                schema_key,
                entity_pk,
                snapshot: input
                    .successor
                    .get_entity(budget, &changed.key)
                    .map_err(arena_error)?,
                effect: if changed.format_only {
                    CoreChangeEffect::FormatOnly
                } else {
                    CoreChangeEffect::Content
                },
            });
        }
        let (after, edits) = document
            .entities_changed(&changes)
            .map_err(sdk::Error::invalid_input)?;
        write_row_index(&input.successor, budget, &after)?;
        Ok(sdk::EntityResult {
            successor: input.successor,
            edits: edits.into_iter().map(core_edit_to_sdk).collect(),
        })
    }
}

fn document_from_root(root: &sdk::Root, budget: &sdk::Budget) -> sdk::Result<CoreDocument> {
    CoreDocument::open_entities(read_entities_from_root(root, budget)?)
        .map(|(document, _)| document)
        .map_err(sdk::Error::invalid_input)
}

fn read_root_bytes(root: &sdk::Root, budget: &sdk::Budget) -> sdk::Result<Vec<u8>> {
    let len = root.file_len();
    let max_page = budget.limits().max_page_bytes.max(1);
    let mut bytes =
        Vec::with_capacity(usize::try_from(len).map_err(|_| sdk::Error::RecordTooLarge(len))?);
    let mut offset = 0_u64;
    while offset < len {
        let requested = u32::try_from((len - offset).min(u64::from(max_page)))
            .expect("request is bounded by max-page-bytes");
        bytes.extend(
            root.read_file(budget, offset, requested)
                .map_err(arena_error)?,
        );
        offset += u64::from(requested);
    }
    Ok(bytes)
}

fn read_entities_from_root(
    root: &sdk::Root,
    budget: &sdk::Budget,
) -> sdk::Result<Vec<CoreEntityRecord>> {
    let mut entities = Vec::new();
    let mut after_key = None;
    loop {
        let page = root
            .scan_entities(
                budget,
                after_key.as_deref(),
                budget.limits().max_page_bytes.max(1),
            )
            .map_err(arena_error)?;
        for (key, snapshot) in page.entries {
            let (schema_key, entity_pk) = sdk::decode_entity_key(&key)?;
            entities.push(CoreEntityRecord {
                schema_key,
                entity_pk,
                snapshot,
            });
        }
        let Some(next_key) = page.next_key else {
            break;
        };
        after_key = Some(next_key);
    }
    Ok(entities)
}

fn write_cold_row_index(
    transaction: &sdk::Transaction,
    budget: &sdk::Budget,
    index: &V3ColdIndex,
    row_count: u64,
) -> sdk::Result<()> {
    for checkpoint in &index.windows {
        transaction
            .put_state(
                budget,
                &row_index_key(checkpoint.page),
                &encode_row_checkpoint(*checkpoint),
            )
            .map_err(arena_error)?;
    }
    transaction
        .put_state(
            budget,
            INDEX_METADATA_KEY,
            &encode_index_metadata(index.namespace, row_count),
        )
        .map_err(arena_error)
}

fn write_row_index(
    transaction: &sdk::Transaction,
    budget: &sdk::Budget,
    document: &CoreDocument,
) -> sdk::Result<()> {
    let records = document.v3_row_index_records();
    let row_count = records.len() as u64;
    let Some(first_record) = records.first() else {
        transaction
            .delete_state(budget, INDEX_METADATA_KEY)
            .map_err(arena_error)?;
        return Ok(());
    };
    let namespace =
        IdNamespace::from_generated_id(&first_record.id).map_err(sdk::Error::invalid_input)?;
    if records
        .iter()
        .enumerate()
        .any(|(ordinal, record)| record.id != namespace.encode(ordinal as u64))
    {
        // One compact namespace can represent the normal import sequence.
        // Mixed imported/generated identities deliberately take the complete
        // durable-entity fallback instead of accepting a lossy locator.
        transaction
            .delete_state(budget, INDEX_METADATA_KEY)
            .map_err(arena_error)?;
        return Ok(());
    }

    let mut pages = std::collections::BTreeMap::<u64, V3RowWindowCheckpoint>::new();
    for (ordinal, record) in records.into_iter().enumerate() {
        let first = u64::from(record.row_start) / INDEX_WINDOW_BYTES;
        let inclusive_end =
            u64::from(record.row_start).saturating_add(u64::from(record.row_len).saturating_sub(1));
        let last = inclusive_end / INDEX_WINDOW_BYTES;
        for page in first..=last {
            pages
                .entry(page)
                .and_modify(|checkpoint| {
                    checkpoint.read_end = checkpoint
                        .read_end
                        .max(record.row_start.saturating_add(record.row_len));
                })
                .or_insert(V3RowWindowCheckpoint {
                    page,
                    row_start: record.row_start,
                    read_end: record.row_start.saturating_add(record.row_len),
                    first_ordinal: ordinal as u32,
                });
        }
    }
    for (page, checkpoint) in pages {
        transaction
            .put_state(
                budget,
                &row_index_key(page),
                &encode_row_checkpoint(checkpoint),
            )
            .map_err(arena_error)?;
    }
    transaction
        .put_state(
            budget,
            INDEX_METADATA_KEY,
            &encode_index_metadata(namespace.0, row_count),
        )
        .map_err(arena_error)
}

fn sparse_row_file_changed(
    input: &sdk::FileUpdate,
    budget: &sdk::Budget,
) -> sdk::Result<Option<Vec<sdk::EntityChange>>> {
    if input.edits.is_empty() {
        return Ok(Some(Vec::new()));
    }
    if input.before_descriptor.path != input.after_descriptor.path {
        return Ok(None);
    }
    let deleted = input.edits.iter().try_fold(0_u64, |total, edit| {
        total
            .checked_add(edit.delete_len)
            .ok_or_else(|| sdk::Error::invalid_input("CSV edit size overflowed"))
    })?;
    let inserted = input.edits.iter().try_fold(0_u64, |total, edit| {
        let len = match &edit.insert {
            sdk::InputBytes::Inline(bytes) => bytes.len() as u64,
            sdk::InputBytes::AfterRange(range) => range.length,
        };
        total
            .checked_add(len)
            .ok_or_else(|| sdk::Error::invalid_input("CSV edit size overflowed"))
    })?;
    if deleted != inserted {
        return Ok(None);
    }
    let Some(metadata) = input
        .before
        .get_state(budget, INDEX_METADATA_KEY)
        .map_err(arena_error)?
    else {
        return Ok(None);
    };
    let (indexed_namespace, indexed_row_count) = decode_index_metadata(&metadata)?;
    let first_offset = input.edits[0].offset;
    let Some(index) = input
        .before
        .get_state(budget, &row_index_key(first_offset / INDEX_WINDOW_BYTES))
        .map_err(arena_error)?
    else {
        return Ok(None);
    };
    let checkpoint = decode_row_checkpoint(&index)?;
    let fast_row = read_unquoted_lf_row(input, budget, checkpoint)?;
    let used_fast_row = fast_row.is_some();
    let (window_bytes, window_start, indexed_local_ordinal) =
        if let Some((row, row_start, local_ordinal)) = fast_row {
            (row, row_start, Some(local_ordinal))
        } else {
            (
                input
                    .successor
                    .read_file(
                        budget,
                        u64::from(checkpoint.row_start),
                        checkpoint.read_end - checkpoint.row_start,
                    )
                    .map_err(arena_error)?,
                u64::from(checkpoint.row_start),
                None,
            )
        };
    let (window_document, changes) = CoreDocument::open_file(
        window_bytes,
        input.after_descriptor.path.as_deref(),
        indexed_namespace,
    )
    .map_err(sdk::Error::invalid_input)?;
    let relative_edits = input
        .edits
        .iter()
        .map(|edit| {
            edit.offset
                .checked_sub(window_start)
                .map(|offset| (offset, edit.delete_len))
        })
        .collect::<Option<Vec<_>>>();
    let Some(relative_edits) = relative_edits else {
        return Ok(None);
    };
    let Some((document_local_ordinal, _record)) = window_document
        .v3_row_index_records()
        .into_iter()
        .enumerate()
        .find(|(_, record)| {
            let start = u64::from(record.row_start);
            let end = start.saturating_add(u64::from(record.row_len));
            relative_edits.iter().all(|(offset, delete_len)| {
                offset
                    .checked_add(*delete_len)
                    .is_some_and(|edit_end| *offset >= start && edit_end <= end)
            })
        })
    else {
        return Ok(None);
    };
    let local_ordinal = indexed_local_ordinal.unwrap_or(document_local_ordinal as u64);
    let actual_ordinal = u64::from(checkpoint.first_ordinal)
        .checked_add(local_ordinal)
        .ok_or_else(|| sdk::Error::internal("CSV row index ordinal overflowed"))?;
    let actual_id = indexed_namespace.encode(actual_ordinal);
    let generated = changes
        .filter_map(Result::ok)
        .filter(|change| change.schema_key == ROW_SCHEMA_KEY)
        .nth(document_local_ordinal)
        .and_then(|change| change.snapshot)
        .ok_or_else(|| sdk::Error::invalid_input("edited CSV range is not exactly one row"))?;
    let mut after = parse_row_snapshot(&generated).map_err(sdk::Error::invalid_input)?;
    let format_only = if used_fast_row {
        after.id.clone_from(&actual_id);
        after.order_key = initial_order_key(actual_ordinal, indexed_row_count)?;
        false
    } else {
        let key = sdk::entity_key(ROW_SCHEMA_KEY, std::slice::from_ref(&actual_id))?;
        let Some(before_snapshot) = input.before.get_entity(budget, &key).map_err(arena_error)?
        else {
            return Ok(None);
        };
        let before = parse_row_snapshot(&before_snapshot).map_err(sdk::Error::invalid_input)?;
        if before.id != actual_id {
            return Err(sdk::Error::internal("CSV row locator identity drifted"));
        }
        after.id.clone_from(&before.id);
        after.order_key.clone_from(&before.order_key);
        let format_only = before.cells == after.cells;
        let snapshot = encode_row_snapshot(&after).map_err(sdk::Error::invalid_input)?;
        if snapshot == before_snapshot {
            return Ok(Some(Vec::new()));
        }
        format_only
    };
    let snapshot = encode_row_snapshot(&after).map_err(sdk::Error::invalid_input)?;
    Ok(Some(vec![sdk::EntityChange {
        schema_key: ROW_SCHEMA_KEY.to_owned(),
        entity_pk: vec![actual_id],
        snapshot: Some(snapshot),
        format_only,
    }]))
}

fn read_unquoted_lf_row(
    input: &sdk::FileUpdate,
    budget: &sdk::Budget,
    checkpoint: V3RowWindowCheckpoint,
) -> sdk::Result<Option<(Vec<u8>, u64, u64)>> {
    if input.edits.iter().any(|edit| match &edit.insert {
        sdk::InputBytes::Inline(bytes) => bytes
            .iter()
            .any(|byte| matches!(byte, b'\n' | b'\r' | b'"')),
        sdk::InputBytes::AfterRange(_) => true,
    }) {
        return Ok(None);
    }
    let checkpoint_start = u64::from(checkpoint.row_start);
    let checkpoint_end = u64::from(checkpoint.read_end);
    let checkpoint_len = checkpoint_end
        .checked_sub(checkpoint_start)
        .ok_or_else(|| sdk::Error::internal("CSV checkpoint range is invalid"))?;
    let edit_start = input
        .edits
        .iter()
        .map(|edit| edit.offset)
        .min()
        .ok_or_else(|| sdk::Error::internal("CSV sparse edit unexpectedly has no splice"))?;
    let edit_end = input.edits.iter().try_fold(edit_start, |end, edit| {
        edit.offset
            .checked_add(edit.delete_len)
            .map(|candidate| end.max(candidate))
            .ok_or_else(|| sdk::Error::invalid_input("CSV edit range overflowed"))
    })?;
    if edit_start < checkpoint_start || edit_end > checkpoint_end {
        return Ok(None);
    }
    let Some(locator) = input
        .successor
        .locate_file_record(
            budget,
            checkpoint_start,
            checkpoint_len,
            edit_start,
            b'\n',
            &[b'"', b'\r'],
        )
        .map_err(arena_error)?
    else {
        return Ok(None);
    };
    if edit_end > locator.offset.saturating_add(locator.length) {
        return Ok(None);
    }
    Ok(Some((locator.content, locator.offset, locator.ordinal)))
}

fn row_index_key(page: u64) -> Vec<u8> {
    let mut key = INDEX_KEY_PREFIX.to_vec();
    key.extend_from_slice(&page.to_be_bytes());
    key
}

fn encode_index_metadata(namespace: [u8; 16], row_count: u64) -> Vec<u8> {
    let mut output = Vec::with_capacity(INDEX_VERSION.len() + namespace.len() + 8);
    output.extend_from_slice(INDEX_VERSION);
    output.extend_from_slice(&namespace);
    output.extend_from_slice(&row_count.to_le_bytes());
    output
}

fn decode_index_metadata(bytes: &[u8]) -> sdk::Result<(IdNamespace, u64)> {
    let payload = bytes
        .strip_prefix(INDEX_VERSION)
        .ok_or_else(|| sdk::Error::internal("CSV row index metadata is invalid"))?;
    if payload.len() != 24 {
        return Err(sdk::Error::internal("CSV row index metadata is invalid"));
    }
    let namespace = <[u8; 16]>::try_from(&payload[..16]).unwrap();
    let row_count = u64::from_le_bytes(payload[16..].try_into().unwrap());
    Ok((IdNamespace(namespace), row_count))
}

fn initial_order_key(ordinal: u64, row_count: u64) -> sdk::Result<String> {
    let denominator = u128::from(row_count)
        .checked_add(1)
        .ok_or_else(|| sdk::Error::internal("CSV row count overflowed"))?;
    let numerator = u128::from(ordinal)
        .checked_add(1)
        .and_then(|ordinal| ordinal.checked_mul(u128::from(u64::MAX)))
        .ok_or_else(|| sdk::Error::internal("CSV row ordinal overflowed"))?;
    let rank = u64::try_from(numerator / denominator)
        .map_err(|_| sdk::Error::internal("CSV row rank overflowed"))?
        | 1;
    Ok(format!("{rank:016x}"))
}

fn encode_row_checkpoint(checkpoint: V3RowWindowCheckpoint) -> [u8; 12] {
    let mut output = [0; 12];
    output[..4].copy_from_slice(&checkpoint.row_start.to_le_bytes());
    output[4..8].copy_from_slice(&checkpoint.read_end.to_le_bytes());
    output[8..].copy_from_slice(&checkpoint.first_ordinal.to_le_bytes());
    output
}

fn decode_row_checkpoint(bytes: &[u8]) -> sdk::Result<V3RowWindowCheckpoint> {
    if bytes.len() != 12 {
        return Err(sdk::Error::internal("CSV row checkpoint is invalid"));
    }
    Ok(V3RowWindowCheckpoint {
        page: 0,
        row_start: u32::from_le_bytes(bytes[..4].try_into().unwrap()),
        read_end: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
        first_ordinal: u32::from_le_bytes(bytes[8..].try_into().unwrap()),
    })
}

fn namespace(creates: sdk::CreateContext) -> IdNamespace {
    IdNamespace::from_halves(creates.high, creates.low)
}

fn core_change_to_sdk(change: CoreEntityChange) -> sdk::EntityChange {
    sdk::EntityChange {
        schema_key: change.schema_key,
        entity_pk: change.entity_pk,
        snapshot: change.snapshot,
        format_only: change.effect == CoreChangeEffect::FormatOnly,
    }
}

fn core_edit_to_sdk(edit: CoreByteEdit) -> sdk::ByteEdit {
    sdk::ByteEdit {
        offset: edit.offset,
        delete_len: edit.delete_len,
        insert: edit.insert.as_ref().clone(),
    }
}

fn arena_error(error: sdk::lix::plugin::arena::ArenaError) -> sdk::Error {
    use sdk::lix::plugin::arena::ArenaError;
    match error {
        ArenaError::InvalidRange => sdk::Error::invalid_input("invalid arena range"),
        ArenaError::RecordTooLarge(bytes) => sdk::Error::RecordTooLarge(bytes),
        ArenaError::LimitExceeded(message) => sdk::Error::LimitExceeded(message),
        ArenaError::DeadlineExceeded => sdk::Error::DeadlineExceeded,
        ArenaError::Unavailable(message) => sdk::Error::internal(message),
    }
}

#[cfg(target_family = "wasm")]
lix_plugin_api_v3::export_v3!(CsvV3Plugin);
