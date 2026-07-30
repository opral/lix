#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChangeEffect {
    Content,
    FormatOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EntityChange {
    pub(crate) schema_key: String,
    pub(crate) entity_pk: Vec<String>,
    pub(crate) snapshot: Option<Vec<u8>>,
    pub(crate) effect: ChangeEffect,
}

impl EntityChange {
    pub(crate) fn upsert(
        schema_key: impl Into<String>,
        entity_pk: Vec<String>,
        snapshot: Vec<u8>,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            entity_pk,
            snapshot: Some(snapshot),
            effect: ChangeEffect::Content,
        }
    }

    pub(crate) fn delete(schema_key: impl Into<String>, entity_pk: Vec<String>) -> Self {
        Self {
            schema_key: schema_key.into(),
            entity_pk,
            snapshot: None,
            effect: ChangeEffect::Content,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EntityRecord {
    pub(crate) schema_key: String,
    pub(crate) entity_pk: Vec<String>,
    pub(crate) snapshot: Vec<u8>,
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
