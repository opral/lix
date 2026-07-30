use lix_plugin_api_v2 as lix;
use std::sync::Arc;

/// This is intentionally a separate integration crate: it may use only the
/// public author-facing API package, as a new plugin would.
#[allow(dead_code)]
struct MinimalFormat;

impl lix::FormatPlugin for MinimalFormat {
    type Document = Arc<()>;

    fn open_file(_input: lix::OpenFile<'_>) -> lix::Result<(Self::Document, lix::Changes)> {
        Ok((
            Arc::new(()),
            lix::changes(std::iter::empty::<lix::EntityChange>()),
        ))
    }

    fn open_entities(_input: lix::OpenEntities<'_>) -> lix::Result<(Self::Document, lix::Edits)> {
        Ok((
            Arc::new(()),
            lix::edits(std::iter::empty::<lix::ByteEdit>()),
        ))
    }

    fn file_changed(
        _document: &Self::Document,
        _update: lix::FileUpdate<'_>,
    ) -> lix::Result<(Self::Document, lix::Changes)> {
        Ok((
            Arc::new(()),
            lix::changes(std::iter::empty::<lix::EntityChange>()),
        ))
    }

    fn entities_changed(
        _document: &Self::Document,
        _update: lix::EntityUpdate<'_>,
    ) -> lix::Result<(Self::Document, lix::Edits)> {
        Ok((
            Arc::new(()),
            lix::edits(std::iter::empty::<lix::ByteEdit>()),
        ))
    }
}

#[test]
fn a_new_plugin_needs_only_the_public_four_transition_surface() {
    let splice = lix::InputSplice {
        offset: 3,
        delete_len: 1,
        insert: lix::InputInsert::AfterRange {
            offset: 100,
            length: 2,
        },
    };
    assert_eq!(
        splice.insert,
        lix::InputInsert::AfterRange {
            offset: 100,
            length: 2,
        }
    );
}
