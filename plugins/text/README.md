# Text line plugin

Text is Lix's baseline, Git-style LF-delimited representation. Markdown can
provide finer semantic diffs on top. The matcher selects files with no NUL
among their first **8,000 bytes** (Git's bounded binary heuristic). Empty files
have zero rows. Invalid UTF-8, mixed endings, BOMs, and a missing final newline
roundtrip byte for byte. A NUL after the scan window is preserved too.

Each `text_line` row has:

| Column | Meaning |
| --- | --- |
| `id` | Stable UUID, generated on insert. |
| `content` | Readable UTF-8 line body, excluding LF. |
| `content_base64` | Unpadded base64url body for invalid UTF-8 only; otherwise NULL. |
| `line_ending` | LF (`'\n'`) or the empty string for an unterminated final line. |
| `order_key` | Lowercase hexadecimal fractional position. Sort by `order_key, id`. |

Exactly one of `content` and `content_base64` is non-NULL. CR remains in the
body, including the CR of CRLF; this mirrors Git's LF segmentation and avoids
normalizing mixed endings. Content cannot contain LF. Empty content with LF
represents a blank line; empty content without LF is invalid. Every nonfinal
row must end in LF. Invalid SQL mutations fail atomically.

```sql
SELECT id, content, content_base64, line_ending, order_key
FROM text_line WHERE lixcol_file_id = $1 ORDER BY order_key, id;

UPDATE text_line SET content = 'new text'
WHERE lixcol_file_id = $1 AND id = $2;

-- Explicit position between '40' and '80'; bind $2 to LF. id defaults automatically.
INSERT INTO text_line (content, order_key, line_ending, lixcol_file_id)
VALUES ('inserted line', '60', $2, $1);

DELETE FROM text_line WHERE lixcol_file_id = $1 AND id = $2;
```

When changing from invalid UTF-8 to text, set `content_base64 = NULL` in the
same update. To change the other way, set `content = NULL` and provide canonical
base64url. An encoding switch wins over a concurrent edit in the old encoding;
conflicting edits in the same representation use Lix's default resolution.

Order keys must be nonempty, lowercase hex with an even number of digits and
must not end in `00`. Equal keys are ordered by UUID, supporting independent
inserts into the same gap. File edits preserve line identities and use
fractional positions to avoid renumbering unrelated lines. A later edit may
separate tied positions. SQL clients currently allocate intermediate keys;
there is no SQL order-key allocation function.

Appending after an unterminated final row requires setting its `line_ending`
to LF in the same transaction. Moving an unterminated row into the middle is
rejected. Merges that combine removal of the final LF with an append can also
conflict on this document invariant; the plugin does not silently insert bytes.

Build the Wasm component with:

```sh
cargo build --release -p plugin_text --target wasm32-wasip2
```

Package `manifest.json`, `schema/text_line.json`, and `plugin.wasm` in a stored
`.lixplugin` ZIP and install it at `/.lix/plugins/plugin_text.lixplugin`.
The schema change is intentional and does not provide backward compatibility.
