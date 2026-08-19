# CSV plugin

The CSV plugin implements the `FileProjection` and `ColumnMerger` capabilities
described in [the universal plugin API](../../rfcs/universal-plugin-api.md). It preserves
table and row identity, exact source bytes, dialect metadata, sparse edits,
cold successors, reopen behavior, and disjoint edits to cells in the same row.

All row mutations, including initial import, use Schema v1 typed rows and
typed primary-key values through the SDK. The SDK owns page framing, batching,
generated primary keys, and final flush. CSV streams typed rows into that path
without constructing a persistent document first.

Plugin state stores paged row spans and identity indexes, never whole-row
copies. Those pages are rebuildable acceleration data rather than merge
authority, and their keys and encoding are private implementation details.
