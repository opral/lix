# Lix plugin packet encoding v1

This file is the normative encoding for `packet-page.format-version = 1` and
`change-page.format-version = 1` in `lix-plugin-v2.wit`. The binary packet is
an SDK/runtime detail rather than the intended format-authoring surface. There
is no reusable production v2 SDK yet: the CSV vertical slice keeps its checked
codec in [`plugins/csv-v2/src/packet.rs`](../../../../plugins/csv-v2/src/packet.rs)
and its typed adapter in
[`plugins/csv-v2/src/bindings.rs`](../../../../plugins/csv-v2/src/bindings.rs).
Those files are the current reference implementation, not a frozen general SDK
facade. Format logic should use typed entity, entity-change, and merge-group
values behind such an adapter.

The packet is a transient Component-boundary arena. It is not a RocksDB or
SlateDB storage format and must not be persisted as one.

## Primitive encodings

All integer fields are unsigned little-endian. Decoders use checked arithmetic
and reject overflow, truncation, invalid UTF-8, an unknown tag/version, an
out-of-range attachment, or trailing bytes.

| Name | Encoding |
|---|---|
| `u8`, `u16`, `u32`, `u64` | Fixed-width unsigned integer |
| `text` | `u32 byte_length` followed by exactly that many UTF-8 bytes |
| `entity-key` | `text schema_key`, `u32 pk_count`, then `pk_count` `text` values |
| `blob-ref` inline | tag `0:u8`, `u32 byte_length`, then the bytes |
| `blob-ref` attachment | tag `1:u8`, `u32 attachment_index`, `u64 offset`, `u64 length` |

An attachment reference addresses an entry in the page's single optional
attachment table. For a host `packet-page` it names an index in `byte-sources`;
for a guest `change-page` it names an index in `byte-outputs`. `offset +
length` must not overflow and must be within the length reported for the named
index. Snapshot bytes must be valid UTF-8 Snapshot JSON v1 for the entity's
schema.

## Snapshot JSON semantic model v1

Snapshot JSON v1 defines one transport semantic model rather than delegating
JSON equality to an SDK's native number or object implementation. Input may
use any valid RFC 8259 lexical spelling, but the decoder normalizes it before
schema validation, comparison, hashing, conflict resolution, or conversion to
Lix's versioned durable JSON representation. The packet does not preserve
lexical spelling.

Decoders apply these rules recursively:

- Invalid UTF-8 and duplicate decoded object keys are rejected. Duplicate
  rejection applies at every object depth, including keys whose escape
  spellings differ (for example, `"a"` and `"\u0061"`).
- A string is an exact sequence of Unicode scalar values. A valid escaped
  surrogate pair decodes to its scalar value; an unpaired surrogate is
  rejected. No NFC, NFD, case, newline, or other Unicode normalization occurs.
- Arrays are ordered sequences. Objects are unordered maps for semantic
  equality; their canonical member order is the lexicographic order of the
  unsigned raw UTF-8 bytes of each decoded key.
- `null` and booleans have their JSON meanings. Numbers are finite decimal
  values of arbitrary precision; binary floating point, implementation integer
  ranges, NaN, and infinities are not part of this model.

### Number normalization and equality

After validating the JSON number grammar, concatenate its integer and fraction
digits into a non-negative decimal coefficient and set the effective base-10
exponent to the explicit exponent (or zero) minus the number of fraction
digits. Both the coefficient and exponent are arbitrary precision. Remove
leading coefficient zeroes. If the coefficient is zero, normalize the number
to `(positive, 0, 0)`, so `-0` equals `0`. Otherwise remove all trailing
coefficient zeroes and increment the effective exponent once per removed zero.
The normalized value is the triple `(sign, coefficient, effective_exponent)`;
two numbers are equal exactly when these normalized triples are equal.

For example, `1`, `1.0`, and `1e0` all normalize to `(positive, 1, 0)`;
`100.0` normalizes to `(positive, 1, 2)`; and `-0`, `0.0`, and `0e999` all
normalize to `(positive, 0, 0)`. Implementations must not reject or round a
valid value merely because its coefficient or exponent does not fit a machine
integer. Transition byte limits still bound the lexical input.

### Deterministic encoding

The canonical encoder emits UTF-8 with no insignificant whitespace:

- `null`, `true`, and `false` use those lowercase spellings.
- Zero is `0`. A nonzero number is an optional `-`, its normalized coefficient,
  and, only when the effective exponent is nonzero, `e` followed by the
  exponent in base 10. The exponent has `-` only when negative, no `+`, and no
  leading zeroes. Thus canonical `100` is `1e2` and canonical `0.01` is
  `1e-2`.
- Strings are enclosed in `"`. Quotation mark and reverse solidus are encoded
  as `\"` and `\\`. Scalars U+0000 through U+001F are encoded as `\u` plus
  four uppercase hexadecimal digits. Every other scalar is emitted directly
  as UTF-8; solidus is not escaped.
- Arrays use `[` and `]` with comma-separated canonical elements. Objects use
  `{` and `}` with comma-separated members in the raw-UTF-8 key order above;
  each canonical string key is followed by `:` and its canonical value.

Normalized recursive values define semantic equality. A `format-only` upsert
still carries a complete changed durable snapshot. The host rejects it as a
no-op when that normalized snapshot equals the normalized complete snapshot in
the accepted base; comparison of packet bytes is never sufficient. A
`format-only` upsert with a genuinely changed normalized snapshot remains a
typed conflict/notification/rendering hint, not ephemeral state.

### Durable representation gate

Current Lix snapshots use `serde_json::Value` without its arbitrary-precision
number feature and therefore cannot faithfully store every Snapshot JSON v1
number. The initial production `wasm-component-v2` slice is consequently
schema-gated to snapshots in which JSON number nodes are unreachable. The CSV
table/row schemas satisfy that profile: their values are objects, arrays,
strings, booleans, or null. A host must reject a number-bearing v2 snapshot
before durable mutation rather than round it or coerce it through binary
floating point. Plugins whose schemas admit numbers are not production-v2
eligible yet.

Lifting this gate requires a versioned durable JSON representation with a
tagged normalized-decimal node (sign, arbitrary-precision coefficient,
arbitrary-precision exponent), migration/hash/comparison rules matching the
section above, and shared golden vectors across storage, conflict resolution,
and every SDK. Choosing bounded numeric semantics instead requires a new packet
format version with explicit range/equality rules; it must not silently weaken
format version 1. This gate keeps the CSV vertical slice faithful without
pretending the general five-format numeric contract is already solved.

## Page framing

`payload` is exactly this sequence, repeated `record-count` times:

```text
u32 record_byte_length
record_byte_length bytes
```

The length excludes its own four-byte prefix. `record-count` must be positive,
records never cross pages, the decoder must consume the payload exactly, and a
source/cursor must return permanent EOF after its first `none`.

The call site fixes the record kind:

- `open-entities.entities`, `entity-update.activated-entities`, and
  `entity-update.current-entities` contain `entity-record` values.
- `entity-update.changes` and guest `change-cursor` pages contain
  `merge-group-record` values.

A page with the wrong record kind is invalid input. The runtime never guesses
the kind from bytes.

## Entity record

```text
entity-key key
blob-ref   snapshot_content
```

This is a complete entity snapshot, not a partial patch. Host-produced complete
entity streams are strictly increasing by entity-key order across page
boundaries and contain no duplicate key. Entity-key order compares the UTF-8
bytes of `schema_key`, then compares primary-key components pairwise by their
UTF-8 bytes; after an equal component prefix, the shorter PK tuple sorts first.
Semantic file order is part of the schema snapshot, not packet order.
`entity-update.activated-entities` contains complete prospective snapshots for
durable keys that were inactive before and active after the transition,
excluding keys already present as complete upserts in `changes`.

## Merge-group record

```text
u32 member_count                 // must be greater than zero
member members[member_count]
```

Members are strictly increasing by the same entity-key ordering within the
group:

```text
u8         change_tag            // 0 = upsert, 1 = delete
entity-key key

// Present only for change_tag = 0:
u8         effect                // 0 = content, 1 = format-only
blob-ref   complete_snapshot_content
```

Groups may arrive in any cursor order because transport order is never merge
rank authority. The canonical group key is the exact byte sequence `u32
member_count || entity-key[member_count]`, using little-endian `member_count`,
the self-delimiting entity-key encoding above, and keys sorted by the defined
entity-key order. The host rejects a key repeated anywhere in the complete
transition and validates each complete upsert against its schema before
conflict resolution.

## Limits and attachment tables

The four-byte frame prefix and all inline record bytes count toward the page
limit. Attachment payload bytes do not count toward the inline record-size cap,
but every table read counts toward the same transition-wide byte, page, and
deadline budget. Every attachment-tagged `blob-ref` occurrence, including
repeated references to the same index/range, counts toward
`max-attachment-refs`; inline `blob-ref` values do not. An indivisible inline
record that cannot fit returns `record-too-large`; SDKs must move a large
snapshot to an attachment rather than splitting a record or group.

Each page owns at most one multiplexed table resource, preventing untrusted
input from allocating one Component resource handle per record. The table must
be `some` if at least one reference occurs and `none` if no reference occurs;
indices are page-local and need not be dense. Attachments are immutable for the
lifetime of the transition. A decoder may read only referenced ranges and must
not concatenate the table merely to decode a page.

The receiver must first validate bounded inline framing and count all attachment
references without invoking `byte-sources` or `byte-outputs`. It rejects the
page if the transition-wide reference cap would be exceeded. Only then may it
validate referenced indices and ranges with `len(index)` and drain bytes with
`read(index, ...)`. An invalid index, overflowed range, unused table, or missing
table is invalid input. The same order applies in an SDK receiving host pages.

`edit-page` uses the same defense without binary packet framing. Its bounded
`edits` list contains inline bytes or an
`output-range { index, offset, length }`. The page owns at most one optional
`byte-outputs` table supplying all such ranges. The host validates edit count,
inline byte count, checked ranges, ordering, and the aggregate output-reference
count before invoking the table; the table is `some` iff an output reference
occurs.

## Canonical validation order

Before exposing typed values, an SDK/runtime decoder must:

1. reject any format version other than `1`;
2. validate frame count, checked lengths, and exact payload consumption;
3. count attachment references and enforce `max-attachment-refs` before
   invoking the page's optional attachment table;
4. validate every tag, UTF-8 string, attachment index, and attachment range;
5. normalize Snapshot JSON v1, validate record-kind-specific structure and
   complete snapshots;
6. enforce entity ordering or group-local ordering as applicable; and
7. enforce transition-wide duplicate-key, page-count, byte, and permanent-EOF
   rules in the host drain validator.

Golden encode/decode vectors for recursive duplicate rejection, Unicode scalar
handling, arbitrary-precision numbers, canonical bytes, normalized equality,
both attachment ownership directions, and generated IDs are required before
the production ABI is frozen. ID vectors cover all-zero/all-`0xff` namespace
halves, ordinal zero/`u64::MAX`, exact unpadded base64url, canonical decode and
rejection, and retry equality. Generated bindings in every supported SDK must
pass the same vectors.
