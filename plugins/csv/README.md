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

## CSV rows in SQL

Each physical CSV record maps to one `csv_row`. Its `cells` column is a JSONB
array of strings in file-column order. The first record is ordinary data: the
plugin does not guess whether it is a header. Duplicate or blank headers,
headerless files, ragged rows, empty strings, and values such as `001` therefore
remain distinguishable. Quoted newlines stay inside their cell.

`id` is the stable row UUID; `order_key` controls its position. Query with
`ORDER BY order_key, id` to obtain file order. Scope queries and edits with
`lixcol_file_id`, because one SQL table contains rows from multiple CSV files.
For example, with `$1` bound to the file ID:

```sql
SELECT id, cells, order_key
FROM csv_row
WHERE lixcol_file_id = $1
ORDER BY order_key, id;
```

To edit a record, bind a JSONB array of strings to `$1`, its UUID to `$2`, and
its file ID to `$3`. Keep `layout` unchanged to retain existing quoting and
line-ending choices where possible:

```sql
UPDATE csv_row SET cells = $1
WHERE id = $2 AND lixcol_file_id = $3;

DELETE FROM csv_row
WHERE id = $1 AND lixcol_file_id = $2;
```

Rows accept 1–65,535 cells. Values remain strings; SQL numeric/null cell values
are rejected rather than implicitly converted. NUL is not supported. Files
must be UTF-8 and smaller than 4 GiB.

An inserted record needs a position. Bind a valid order key to `$1`, JSONB
cells to `$2`, and the file ID to `$3`; the UUID defaults to `uuidv7()` and
omitted `layout` defaults to SQL NULL:

```sql
INSERT INTO csv_row (order_key, cells, lixcol_file_id)
VALUES ($1, $2, $3);
```

Order keys are nonempty, lowercase hexadecimal strings with an even number of
characters and cannot end in `00`. They sort lexicographically, with UUID as a
tie-breaker. Choose a key between the intended neighboring rows; a fixed key
such as `ff` is only an append position when it sorts after the existing last
key. The API does not currently provide a SQL order-key allocator. See
[the API improvement report](PLUGIN_API_IMPROVEMENTS.md) for that proposal.

## Dialect and formatting

Each file has one `csv_table` record with `id = 'root'`. Its `dialect` JSONB
object describes `delimiter`, `quote` (or null), `terminator`, and optional
`bom`. CSV defaults to comma; a `.tsv` path defaults to tab. Quoting defaults to
double quotes. The plugin preserves the detected encoding BOM and newline
style, including row-specific exceptions. Dialect changes through SQL rerender
the file; choose a quote character when cells contain delimiters or newlines.

`csv_row.layout` stores lexical exceptions such as unnecessary quotes and an
exceptional or missing row terminator. SQL NULL requests canonical quoting and
the table's terminator. A missing terminator belongs to its row: if that row is
moved into the middle, the renderer adds a separator without removing the
new final row's requested ending. The renderer also quotes cells when necessary
to prevent a leading U+FEFF becoming an encoding BOM or two records collapsing
into a CRLF terminator.

## Performance qualification

[The scaling probes](qa_scale/README.md) exercise 100k and 1M rows through both
the native core and the actual SQL/Wasm interface. Equal-size edits to imported
rows retain the compact index and read only affected records. Length-changing
edits update paged offsets without reading unchanged file content. Structural
changes and dialect changes can require broader reconstruction; measurements
must include repeated edits and the next edit after a fallback.

See the [QA results and remaining limits](QA_REPORT.md) and the separate
[plugin API improvement report](PLUGIN_API_IMPROVEMENTS.md).
