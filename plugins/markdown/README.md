# Markdown rows and SQL editing

The plugin projects each file into `markdown_node`. One row represents a block or
structural element; a paragraph, heading, or table cell embeds its inline content
in `payload_json.inline`. Inline text is not a separate SQL row. This keeps a
paragraph edit local while giving lists, quotations, and tables explicit structure.

## Row fields

- `id`: stable UUID identity. Preserve it when editing an existing node.
- `kind`: one of the kinds below.
- `parent_id`: structural parent UUID. Only the document root has no parent.
- `order_key`: sibling position, encoded as an even-length hexadecimal byte string
  with a nonzero final byte. For example, `40`, `60`, and `80` are ordered positions.
  Only the root has a null key. Equal keys are ordered deterministically by UUID.
  Prefer lowercase keys when querying their textual ordering.
- `payload_json`: semantic content, as JSONB.
- `format_json`: spelling preferences, as JSONB. Keep existing fields when editing
  content. Stale cached spellings cannot override new semantic values.
- `lixcol_file_id`: the Lix system column that associates every row with its file.
  Include it when querying or inserting rows for a particular file.

Every file has exactly one `document`. Structural changes must leave all nodes
reachable from it, without cycles or missing parents. Deleting a container requires
deleting or moving its descendants in the same transaction. Invalid child kinds
return an error rather than disappearing from the rendered file.

## Block and structural kinds

The examples show payload and format shapes; UUIDs and inline arrays are abbreviated.

| Kind | `payload_json` | `format_json` | Children |
| --- | --- | --- | --- |
| `document` | `{"dialect":"gfm"}` | `{"line_ending":"lf","final_newline":true}` | Blocks |
| `paragraph` | `{"inline":[...]}` | `{}` | None |
| `heading` | `{"depth":2,"inline":[...]}` | `{"style":"atx"}` or `{"style":"setext"}` | None |
| `thematic_break` | `{}` | `{"marker":"dash"}`; also `asterisk`, `underscore` | None |
| `block_quote` | `{}` | `{}` | Blocks |
| `list` | `{"ordered":false,"start":null,"tight":true}` | `{"delimiter":"dash"}` | `list_item` |
| `list_item` | `{"checked":null}`; task items use `true` or `false` | `{}` | Blocks |
| `code_block` | `{"value":"code\n","info":null}` | `{"style":"fenced","marker":"backtick","fence_length":3}` or `{"style":"indented"}` | None |
| `html_block` | `{"value":"<div>raw HTML</div>\n"}` | `{}` | None |
| `definition` | `{"identifier":"label","destination":"/url","title":null}` | `{"label":"Label","destination":"bare","title":null}` | None |
| `footnote_definition` | `{"identifier":"note"}` | `{"label":"note"}` | Blocks |
| `frontmatter` | `{"kind":"yaml","value":"key: value\n"}`; also `toml` | `{}` | None |
| `table` | `{}` | `{}` | `table_column`, `table_row` |
| `table_column` | `{"alignment":"none"}`; also `left`, `center`, `right` | `{}` | None |
| `table_row` | `{"role":"header"}` or `{"role":"body"}` | `{}` | `table_cell` |
| `table_cell` | `{"column_id":"column UUID","inline":[...]}` | `{}` | None |

A table has explicit column rows so changing column order moves all its cells
without rewriting their content. Each cell references a column UUID, and each row
must contain exactly one cell per column. The first table row in sibling order is
the header. Its role must be `header`, and subsequent rows must use `body`.
Reorder rows and their header/body roles together. Add or remove a
column and its cells in one transaction.

Unordered list delimiters are `dash`, `plus`, and `asterisk`. Ordered lists use
`period` or `paren` and an integer `start`. When switching list type, update
`ordered`, `start` (integer for ordered, null for unordered), and the delimiter
together. Code fences use `backtick` or `tilde`.
Reference identifiers use the parser's whitespace-collapsed, case-folded label.
Rename definitions and their referencing inlines in the same transaction.
The `label` format field preserves original spelling while it identifies the same
reference. Link destinations use `bare` or `angle`; title styles are `double_quote`,
`single_quote`, or `paren`.

## Inline payloads

Plain text: `{"type":"text","value":"Hello"}`. Formatting nodes wrap a `children`
array, for example:

```json
{
  "inline": [
    {"type":"text","value":"Hello "},
    {"type":"strong","children":[{"type":"text","value":"world"}],"format":{"marker":"**"}}
  ]
}
```

Supported inline types are `text`, `escape`, `character_reference`, `emphasis`,
`strong`, `delete`, `code`, `link`, `image`, `link_reference`, `image_reference`,
`autolink`, `html`, `soft_break`, `line_break`, and `footnote_reference`.
Non-text inline nodes may carry stable `id` UUIDs; preserve existing IDs during
edits. Images use `alt` inline arrays instead of `children`.

- Emphasis/strong use `format.marker`: `*`/`_` or `**`/`__`.
- Deletion uses `~` or `~~`.
- Inline code uses `value` and `format: {"raw":"code","fence_length":1}`.
  Edit `value`; the old raw spelling is reused only when still valid.
- Links/images use `destination`, nullable `title`, and
  `format: {"destination":"bare","title":null}`. Adding a title automatically
  chooses double quotes when no previous style exists.
- Reference links/images use `identifier` and
  `format: {"label":"label","kind":"full"}`; also `collapsed`, `shortcut`.
- Autolinks use `destination` and `format.kind` (`angle` or `literal`); literal
  links retain `format.original` while it still represents the destination.
- Character references use decoded `value` and `format.reference`, e.g. `&amp;`.
- `soft_break` has no other fields. `line_break` uses
  `format: {"kind":"spaces"}` or `{"kind":"backslash"}`.

## SQL examples

Read blocks for one file, binding `$1` to its file ID:

```sql
SELECT id, kind, parent_id, order_key, payload_json, format_json
FROM markdown_node
WHERE lixcol_file_id = $1
ORDER BY parent_id, order_key, id;
```

Replace a paragraph's inline content, binding its node ID as `$1` and file ID as `$2`:

```sql
UPDATE markdown_node
SET payload_json = '{"inline":[{"type":"text","value":"Updated paragraph."}]}'
WHERE id = $1 AND lixcol_file_id = $2 AND kind = 'paragraph';
```

Insert a paragraph between siblings with keys `40` and `80`, binding its parent
UUID as `$1` and file ID as `$2`. The schema supplies the new node UUID:

```sql
INSERT INTO markdown_node
  (kind, parent_id, order_key, payload_json, format_json, lixcol_file_id)
VALUES
  ('paragraph', $1, '60', '{"inline":[{"type":"text","value":"New paragraph."}]}', '{}', $2);
```

For a move, update `parent_id` and `order_key` while retaining `id`. For a formatting
change, update the appropriate format property while preserving the other fields.
For edits to an inline subtree, read the JSON, modify the intended subtree, and
write it back while retaining unrelated inline IDs and content.

## Roundtrip contract and limits

Unedited files retain their accepted bytes, including whitespace, line endings,
BOMs, and detected encoding. SQL edits preserve unrelated source spelling where
its block boundaries and meaning remain valid. Changed constructs may receive
safe canonical spelling: fences can grow, indented code can become fenced code,
and literal punctuation can become escapes or character references.

Some noncanonical spellings require the accepted file bytes in addition to rows.
Keep Lix's file snapshot when reopening; exported semantic rows alone are not a
standalone archive of every lexical detail. Root lexical cache fields are internal
and should not be authored through SQL.

Rendered row edits are reparsed and compared semantically before acceptance.
Unrepresentable structures, HTML block values that become ordinary paragraphs,
and inconsistent table roles return an error. A loose single-item list needs
multiple block children to express its loose layout. Code block values use LF.
Nonempty fenced code values end in LF; terminal indented code may omit it when
the document has no final newline. Frontmatter values exclude the newline
separating the value from its closing fence; additional boundary blank lines are content.

The parser and row graph enforce a nesting budget of 64 to return an error before
exhausting the host stack. Inline code values must be nonempty and cannot contain
line endings: Markdown code spans cannot represent those values exactly. Invalid
row graphs and unrepresentable inline code edits are rejected.
