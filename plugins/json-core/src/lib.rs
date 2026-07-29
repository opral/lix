//! ABI-neutral recursive stable-identity JSON semantic core.

#[path = "../../json-v2/src/core.rs"]
mod core;

pub use core::{
    ARRAY_ITEM_SCHEMA_KEY, ByteEdit, ChangeEffect, Document, EntityChange, EntityRecord,
    IdNamespace, InitialChanges, InputSplice, OBJECT_MEMBER_SCHEMA_KEY, ROOT_SCHEMA_KEY,
    V3ScalarIndexRecord,
};
