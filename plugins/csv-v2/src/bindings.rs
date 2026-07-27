#![allow(dead_code, unused_qualifications)]

//! CSV's author-facing Component-v2 implementation.
//!
//! Delimiter detection, row identity, dialect-preserving rendering, and
//! path-sensitive parsing remain CSV concerns. The API package owns only
//! transport.

use crate::core::{
    ByteEdit as CoreByteEdit, ChangeEffect as CoreChangeEffect, Document as CoreDocument,
    EntityChange as CoreEntityChange, EntityImportBuilder, EntityRecord as CoreEntityRecord,
    IdNamespace as CoreIdNamespace, InputSplice as CoreInputSplice, ROW_SCHEMA_KEY,
    RowConflictResolution, resolve_row_conflict,
};
use lix_plugin_api_v2 as sdk;

struct CsvPlugin;

impl sdk::FormatPlugin for CsvPlugin {
    type Document = CoreDocument;

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

    fn open_file(input: sdk::OpenFile<'_>) -> sdk::Result<(Self::Document, sdk::Changes)> {
        let bytes = input.source.read_all()?;
        let namespace = core_namespace(input.ids)?;
        let (document, changes) =
            CoreDocument::open_file(bytes, input.file.path.as_deref(), namespace)
                .map_err(sdk::Error::invalid_input)?;
        Ok((document, core_changes(changes)))
    }

    fn open_entities(
        mut input: sdk::OpenEntities<'_>,
    ) -> sdk::Result<(Self::Document, sdk::Edits)> {
        let accepted = input
            .accepted
            .as_ref()
            .map(sdk::Source::read_all)
            .transpose()?;
        let mut builder = EntityImportBuilder::new();
        while let Some(record) = input.entities.next()? {
            builder
                .push(core_record(record))
                .map_err(sdk::Error::invalid_input)?;
        }
        let (document, mut edit) = builder.finish().map_err(sdk::Error::invalid_input)?;
        let edits = match accepted {
            Some(accepted) if edit.insert.as_ref() == &accepted => Vec::new(),
            Some(accepted) => {
                edit.delete_len = u64::try_from(accepted.len())
                    .map_err(|_| sdk::Error::invalid_input("accepted CSV is too large"))?;
                vec![edit]
            }
            None if edit.insert.is_empty() => Vec::new(),
            None => vec![edit],
        };
        Ok((document, core_edits(edits)))
    }

    fn file_changed(
        document: &Self::Document,
        update: sdk::FileUpdate<'_>,
    ) -> sdk::Result<(Self::Document, sdk::Changes)> {
        let inserts = update
            .edits
            .iter()
            .map(|edit| update.read_insert(edit))
            .collect::<sdk::Result<Vec<_>>>()?;
        let splices = update
            .edits
            .iter()
            .zip(&inserts)
            .map(|(edit, insert)| CoreInputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert,
            })
            .collect::<Vec<_>>();
        let namespace = core_namespace(update.ids)?;
        let (document, changes) = document
            .file_changed_with_paths(
                &splices,
                update.before.path.as_deref(),
                update.after.path.as_deref(),
                namespace,
            )
            .map_err(sdk::Error::invalid_input)?;
        Ok((document, core_changes(changes.into_iter().map(Ok))))
    }

    fn entities_changed(
        document: &Self::Document,
        mut update: sdk::EntityUpdate<'_>,
    ) -> sdk::Result<(Self::Document, sdk::Edits)> {
        let mut changes = Vec::new();
        while let Some(change) = update.changes.next()? {
            changes.push(core_change(change));
        }
        let (document, edits) = document
            .entities_changed(&changes)
            .map_err(sdk::Error::invalid_input)?;
        Ok((document, core_edits(edits)))
    }
}

fn core_namespace(ids: sdk::IdNamespace) -> sdk::Result<CoreIdNamespace> {
    CoreIdNamespace::from_generated_id(&ids.id(0)).map_err(sdk::Error::internal)
}

fn core_record(record: sdk::EntityRecord) -> CoreEntityRecord {
    CoreEntityRecord {
        schema_key: record.schema_key,
        entity_pk: record.entity_pk,
        snapshot: record.snapshot,
    }
}

fn sdk_change(change: CoreEntityChange) -> sdk::EntityChange {
    sdk::EntityChange {
        schema_key: change.schema_key,
        entity_pk: change.entity_pk,
        snapshot: change.snapshot,
        effect: match change.effect {
            CoreChangeEffect::Content => sdk::ChangeEffect::Content,
            CoreChangeEffect::FormatOnly => sdk::ChangeEffect::FormatOnly,
        },
    }
}

fn core_change(change: sdk::EntityChange) -> CoreEntityChange {
    CoreEntityChange {
        schema_key: change.schema_key,
        entity_pk: change.entity_pk,
        snapshot: change.snapshot,
        effect: match change.effect {
            sdk::ChangeEffect::Content => CoreChangeEffect::Content,
            sdk::ChangeEffect::FormatOnly => CoreChangeEffect::FormatOnly,
        },
    }
}

fn sdk_edit(edit: CoreByteEdit) -> sdk::ByteEdit {
    sdk::ByteEdit {
        offset: edit.offset,
        delete_len: edit.delete_len,
        insert: edit.insert,
    }
}

fn core_changes<I>(changes: I) -> sdk::Changes
where
    I: Iterator<Item = std::result::Result<CoreEntityChange, String>> + 'static,
{
    sdk::try_changes(
        changes.map(|change| change.map(sdk_change).map_err(sdk::Error::invalid_input)),
    )
}

fn core_edits(edits: Vec<CoreByteEdit>) -> sdk::Edits {
    sdk::edits(edits.into_iter().map(sdk_edit))
}

#[cfg(target_family = "wasm")]
lix_plugin_api_v2::export_v2!(CsvPlugin);
