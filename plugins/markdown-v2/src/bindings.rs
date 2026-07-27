#![allow(dead_code, unused_qualifications)]

//! Markdown's author-facing Component-v2 implementation.
//!
//! Markdown keeps its GFM parser, lexical representation, and persistent tree.
//! The shared API package lowers its semantic transitions and deterministic
//! entity conflict resolver to Component v2.

use crate::core::{
    ByteEdit as CoreByteEdit, ChangeEffect as CoreChangeEffect, Document as CoreDocument,
    EntityChange as CoreEntityChange, EntityRecord as CoreEntityRecord,
    IdNamespace as CoreIdNamespace, InputSplice as CoreInputSplice, PluginError as CorePluginError,
};
use lix_plugin_api_v2 as sdk;

struct MarkdownPlugin;

impl sdk::FormatPlugin for MarkdownPlugin {
    type Document = CoreDocument;

    fn resolve_conflict(conflict: sdk::EntityConflict<'_>) -> sdk::Result<sdk::ConflictResolution> {
        const MAX_HEURISTIC_SNAPSHOT_BYTES: u64 = 64 * 1024;

        let Some(b) = conflict.b.as_ref() else {
            return Ok(sdk::ConflictResolution::Delete);
        };
        if conflict.schema_key != crate::NODE_SCHEMA_KEY {
            return Ok(sdk::ConflictResolution::TakeB);
        }
        let (Some(base), Some(a)) = (&conflict.base, &conflict.a) else {
            return Ok(sdk::ConflictResolution::TakeB);
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
        let resolved = CoreDocument::resolve_entity_conflict(
            Some(base.clone()),
            Some(a.clone()),
            Some(b.clone()),
        );
        Ok(match resolved {
            None => sdk::ConflictResolution::Delete,
            Some(resolved) if resolved == b => sdk::ConflictResolution::TakeB,
            Some(resolved) if resolved == a => sdk::ConflictResolution::TakeA,
            Some(resolved) if resolved == base => sdk::ConflictResolution::TakeBase,
            Some(resolved) => sdk::ConflictResolution::Replace(resolved),
        })
    }

    fn open_file(input: sdk::OpenFile<'_>) -> sdk::Result<(Self::Document, sdk::Changes)> {
        let bytes = input.source.read_all()?;
        let namespace = core_namespace(input.ids)?;
        let (document, changes) =
            CoreDocument::open_file(bytes, input.file.path.as_deref(), namespace)
                .map_err(core_error)?;
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
        let mut records = Vec::new();
        while let Some(record) = input.entities.next()? {
            records.push(core_record(record));
        }
        let (document, edits) =
            CoreDocument::open_entities(records, accepted).map_err(core_error)?;
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
            .file_changed(&splices, namespace)
            .map_err(core_error)?;
        Ok((document, core_changes(changes)))
    }

    fn entities_changed(
        document: &Self::Document,
        mut update: sdk::EntityUpdate<'_>,
    ) -> sdk::Result<(Self::Document, sdk::Edits)> {
        let mut changes = Vec::new();
        while let Some(change) = update.changes.next()? {
            changes.push(core_change(change));
        }
        let (document, edits) = document.entities_changed(changes).map_err(core_error)?;
        Ok((document, core_edits(edits)))
    }
}

fn core_namespace(ids: sdk::IdNamespace) -> sdk::Result<CoreIdNamespace> {
    CoreIdNamespace::from_generated_id(&ids.id(0)).map_err(sdk::Error::internal)
}

fn core_error(error: CorePluginError) -> sdk::Error {
    match error {
        CorePluginError::InvalidInput(message) => sdk::Error::invalid_input(message),
        CorePluginError::Internal(message) => sdk::Error::internal(message),
    }
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

fn core_changes(changes: Vec<CoreEntityChange>) -> sdk::Changes {
    sdk::changes(changes.into_iter().map(sdk_change))
}

fn core_edits(edits: Vec<CoreByteEdit>) -> sdk::Edits {
    sdk::edits(edits.into_iter().map(sdk_edit))
}

#[cfg(target_family = "wasm")]
lix_plugin_api_v2::export_v2!(MarkdownPlugin);
