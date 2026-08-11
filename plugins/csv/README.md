# CSV plugin

The CSV plugin implements the `FileProjection` and `ColumnMerger` capabilities
described in [the universal plugin API](../../rfcs/universal-plugin-api.md). It preserves
table and row identity, exact source bytes, dialect metadata, sparse edits,
cold successors, reopen behavior, and disjoint edits to cells in the same row.

All row mutations, including initial import, use complete snapshots through
the universal row output method. The SDK owns page framing, batching,
generated primary keys, final flush, and oversized attachments. CSV streams
rows into that method without constructing a persistent document first.

Plugin state stores paged row spans and identity indexes. Those pages are
rebuildable acceleration data rather than merge authority, and their keys and
encoding are private implementation details.
