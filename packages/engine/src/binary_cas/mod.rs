pub(crate) mod chunking;
pub(crate) mod codec;
pub(crate) mod gc;
pub(crate) mod init;
pub(crate) mod read;
pub(crate) mod schema;
pub(crate) mod support;
pub(crate) mod write;

pub(crate) use init::init;
pub(crate) use schema::INTERNAL_BINARY_BLOB_STORE;

pub(crate) fn internal_exact_relation_names() -> &'static [&'static str] {
    &[
        schema::INTERNAL_BINARY_BLOB_MANIFEST,
        schema::INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        schema::INTERNAL_BINARY_BLOB_STORE,
        schema::INTERNAL_BINARY_CHUNK_STORE,
        schema::INTERNAL_BINARY_FILE_VERSION_REF,
    ]
}
