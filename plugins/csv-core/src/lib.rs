//! ABI-neutral incremental CSV semantic core.

#![cfg_attr(test, allow(dead_code))]

#[path = "../../csv-v2/src/core.rs"]
mod core;

pub use core::{
    ByteEdit, ChangeEffect, Dialect, Document, EntityChange, EntityRecord, IdNamespace,
    InitialChanges, InputSplice, ROOT_ENTITY_PK, ROW_SCHEMA_KEY, RowConflictResolution,
    RowSnapshot, TABLE_SCHEMA_KEY, Terminator, V3ColdIndex, V3ColdMetadata, V3InitialChanges,
    V3RowFramer, V3RowIndexRecord, V3RowWindowCheckpoint, V3StreamAnalyzer, describe_memory,
    encode_row_snapshot, parse_row_snapshot, render_row, resolve_row_conflict, v3_open_file_stream,
    v3_stream_row_change, v3_stream_table_change,
};
