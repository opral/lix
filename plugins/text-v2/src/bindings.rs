//! Public Component-v2 adapter for the Git text line model.
#![allow(dead_code, unused_qualifications)]

use lix_plugin_api_v2 as lix;

use crate::core::{Document, InputSplice};

struct GitTextPlugin;

impl lix::FormatPlugin for GitTextPlugin {
    type Document = Document;

    fn open_file(input: lix::OpenFile<'_>) -> lix::Result<(Self::Document, lix::Changes)> {
        let bytes = input.source.read_all()?;
        let (document, changes) = Document::open_file(bytes, |ordinal| input.ids.id(ordinal))
            .map_err(lix::Error::invalid_input)?;
        Ok((document, lix::changes(changes.into_iter())))
    }

    fn open_entities(
        mut input: lix::OpenEntities<'_>,
    ) -> lix::Result<(Self::Document, lix::Edits)> {
        // `accepted` is a host-verified materialization for this exact
        // semantic root. The document rows are sufficient to recreate it, so
        // do not read a potentially huge raw checkpoint merely to rediscover
        // that the renderer agrees. A missing checkpoint instead means the
        // host needs one insertion relative to an empty file.
        let has_accepted = input.accepted.is_some();
        let mut records = Vec::new();
        while let Some(record) = input.entities.next()? {
            records.push(record);
        }
        let document = Document::open_entities(records).map_err(lix::Error::invalid_input)?;
        let edits = restore_edits(has_accepted, document.bytes());
        Ok((document, lix::edits(edits.into_iter())))
    }

    fn file_changed(
        document: &Self::Document,
        update: lix::FileUpdate<'_>,
    ) -> lix::Result<(Self::Document, lix::Changes)> {
        let mut splices = Vec::with_capacity(update.edits.len());
        for edit in &update.edits {
            splices.push(InputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: update.read_insert(edit)?,
            });
        }
        let (document, changes) = document
            .file_changed(&splices, |ordinal| update.ids.id(ordinal))
            .map_err(lix::Error::invalid_input)?;
        Ok((document, lix::changes(changes.into_iter())))
    }

    fn entities_changed(
        document: &Self::Document,
        mut update: lix::EntityUpdate<'_>,
    ) -> lix::Result<(Self::Document, lix::Edits)> {
        let mut changes = Vec::new();
        while let Some(change) = update.changes.next()? {
            changes.push(change);
        }
        let (document, edits) = document
            .entities_changed(changes)
            .map_err(lix::Error::invalid_input)?;
        Ok((document, lix::edits(edits.into_iter())))
    }
}

fn restore_edits(has_accepted: bool, rendered: &[u8]) -> Vec<lix::ByteEdit> {
    if !has_accepted {
        return (!rendered.is_empty())
            .then(|| lix::ByteEdit::new(0, 0, rendered.to_vec()))
            .into_iter()
            .collect();
    }
    Vec::new()
}

#[cfg(target_family = "wasm")]
lix_plugin_api_v2::export_v2!(GitTextPlugin);
