use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChangeEffect {
    Content,
    FormatOnly,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RowChange {
    pub(crate) schema_key: Arc<str>,
    pub(crate) row_pk: Vec<lix::plugin::TypedValue>,
    pub(crate) row: Option<lix::plugin::TypedRow>,
    pub(crate) effect: ChangeEffect,
}

impl RowChange {
    pub(crate) fn upsert(
        schema_key: impl Into<Arc<str>>,
        row_pk: Vec<lix::plugin::TypedValue>,
        row: lix::plugin::TypedRow,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            row_pk,
            row: Some(row),
            effect: ChangeEffect::Content,
        }
    }

    pub(crate) fn delete(
        schema_key: impl Into<Arc<str>>,
        row_pk: Vec<lix::plugin::TypedValue>,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            row_pk,
            row: None,
            effect: ChangeEffect::Content,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RowRecord {
    pub(crate) schema_key: Arc<str>,
    pub(crate) row_pk: Vec<lix::plugin::TypedValue>,
    pub(crate) row: lix::plugin::TypedRow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ByteEdit {
    pub(crate) offset: u64,
    pub(crate) delete_len: u64,
    pub(crate) insert: Vec<u8>,
}

impl ByteEdit {
    pub(crate) fn new(offset: u64, delete_len: u64, insert: Vec<u8>) -> Self {
        Self {
            offset,
            delete_len,
            insert,
        }
    }
}
