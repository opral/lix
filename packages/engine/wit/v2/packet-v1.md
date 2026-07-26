# Lix plugin packet encoding v1

This file is the normative encoding for `packet-page.format-version = 1`,
`change-page.format-version = 1`, and `resolution-page.format-version = 1` in
`lix-plugin-v2.wit`. The binary packet is an API/runtime detail rather than the
format-authoring surface. The public
[`lix_plugin_api_v2`](../../../plugin-api/README.md) package owns its checked
codec and typed Component adapter. Format logic should use its typed entity,
entity-change, conflict/resolution, and byte-edit values rather than this
packet encoding.

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
for a guest `change-page` or `resolution-page` it names an index in
`byte-outputs`. `offset + length` must not overflow and must be within the
length reported for the named index. Snapshot bytes must be valid UTF-8
Snapshot JSON v1 for the entity's schema.

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
schema-gated to snapshots in which JSON number nodes are unreachable. All four
reference schema sets satisfy that profile: CSV uses strings for cell values,
JSON stores a scalar's exact JSON spelling as text, and Markdown/Excalidraw
encode their mature number-bearing payloads as validated JSON strings. A host
must reject a number-bearing v2 snapshot before durable mutation rather than
round it or coerce it through binary floating point. Plugins whose schemas
admit numbers are not production-v2 eligible yet.

Lifting this gate requires a versioned durable JSON representation with a
tagged normalized-decimal node (sign, arbitrary-precision coefficient,
arbitrary-precision exponent), migration/hash/comparison rules matching the
section above, and shared golden vectors across storage, conflict resolution,
and every SDK. Choosing bounded numeric semantics instead requires a new packet
format version with explicit range/equality rules; it must not silently weaken
format version 1. This gate keeps the four reference implementations faithful
without pretending the general numeric contract is already solved.

## Page framing

`payload` is exactly this sequence, repeated `record-count` times:

```text
u32 record_byte_length
record_byte_length bytes
```

The length excludes its own four-byte prefix. `record-count` must be positive,
records never cross pages, the decoder must consume the payload exactly, and a
source/cursor must return permanent EOF after its first `none`.
Before allocating from it, a receiver must prove
`record-count <= payload.byte_length / 4`; each record needs at least its
four-byte frame prefix. Nested counts receive an equivalent remaining-byte
bound before allocation.

The call site fixes the record kind:

- `open-entities.entities` contains `entity-record` values.
- `entity-update.changes` and guest `change-cursor` pages contain
  `entity-change-record` values.
- `conflict-update.conflicts` contains `entity-conflict-record` values.
- Guest `resolution-cursor` pages contain `conflict-resolution-record` values.

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

## Entity-change record

```text
u8         change_tag            // 0 = upsert, 1 = delete
entity-key key

// Present only for change_tag = 0:
u8         effect                // 0 = content, 1 = format-only
blob-ref   complete_snapshot_content
```

Changes may arrive in any cursor order because transport order is never merge
rank authority. The host rejects a key repeated anywhere in the complete
transition and validates each complete upsert against its schema before
conflict resolution. Conflict resolution is entity-granular; packet format 1
does not encode cross-entity atomic groups.

## Static conflict-resolution packets

`resolve-conflicts` is a static, file-scoped operation. It deliberately takes
no `document` resource: resolving one colliding paragraph or CSV row must not
cold-hydrate an unrelated multi-megabyte file or rebuild a plugin's complete
index. The engine invokes it once for a compatible file/plugin generation
group, supplies a lazy paged conflict source, drains one aligned resolution
cursor, then feeds the resolved semantic changes through the ordinary
`entities-changed` render path once.

The engine owns merge authority and canonical side ordering. `base` is the
common ancestor. `a` and `b` are the two divergent versions ordered by
their durable `(updated_at, change_id)` tuple, never by target/source branch
direction or packet arrival order. Reversing the branch merge therefore gives
the plugin the same triple and the same deterministic decision. `b` is the
higher-ranked fallback candidate; it is not a claim that a client wall clock
is authoritative. Transport-level arrival-order LWW requires a future
host-issued write rank.

### Entity-conflict record

```text
entity-key key
u32       ordinal              // host-assigned, zero-based in this batch

u8         base_state           // 0 = absent/tombstone, 1 = present
blob-ref   base_snapshot         // present only when base_state = 1
u8         a_state              // 0 = absent/tombstone, 1 = present
blob-ref   a_snapshot            // present only when a_state = 1
u8         b_state              // 0 = absent/tombstone, 1 = present
blob-ref   b_snapshot            // present only when b_state = 1
```

The three snapshot fields are independent lazy `blob-ref` values. A resolver
that selects a side does not read its bytes. In particular, a deterministic
canonical fallback emits `take(b)` without copying a large value
through guest linear memory. A plugin reads only the snapshots needed for a
format-specific heuristic.

### Conflict-resolution record

Resolution records are positional, not keyed. A resolution cursor must emit
exactly one record for every input conflict record, in precisely the consumed
input order, with no skipped, repeated, inserted, or reordered result. It
echoes the host-assigned `ordinal`, so the host proves that property rather than
trusting the guest's ordering claim. The host owns entity keys and rejects a
cursor whose ordinal sequence or total cardinality differs from its source.
This prevents a plugin from changing merge scope while still allowing its
result to be streamed in bounded pages.

```text
u8         resolution_tag       // 0 = take, 1 = replace, 2 = delete
u32        ordinal              // echoes the matching input record

// Present only for resolution_tag = 0:
u8         take_side            // 0 = base, 1 = a, 2 = b

// Present only for resolution_tag = 1:
u8         effect               // 0 = content, 1 = format-only
blob-ref   complete_snapshot_content
```

`take` retains the selected immutable input version and therefore has no
guest-output snapshot. `replace` supplies one complete newly merged snapshot;
large replacements use the `resolution-page`'s `byte-outputs` attachment table
under the ordinary attachment rules. `delete` resolves the entity to a
tombstone. A plugin must always return a deterministic result in this release.
For an unsupported, overlapping, malformed, or structural edit, the normal
fallback is `take(b)` (or `delete` when `b` is absent).

### Granularity and heuristic guidance

Conflict resolution is scoped to the plugin's durable entity granularity, not
to a universal file AST. A CSV plugin can represent a stable row as one entity
and safely compose changes to different same-index cells when the base, a,
and b values retain the same row identity, order, field count, and layout.
Concurrent edits to the same cell, row reordering, field-shape changes, or
layout changes should deterministically take `b` rather than inventing a
row-structure CRDT. A Markdown plugin can similarly attempt a bounded
three-way text heuristic inside one paragraph/block entity and take `b`
for overlapping or unsupported syntax. Entity IDs remain stable identifiers;
row positions, array indices, and byte offsets are not identities.

This API intentionally resolves a merge now. It does not persist both sides as
first-class conflict rows, and plugins must not encode a private durable
conflict object inside an ordinary entity snapshot. JJ-style persisted conflict
values, explicit user resolution, and transport of unresolved alternatives are
deferred to a later data-model and protocol increment.

## Limits and attachment tables

The four-byte frame prefix and all inline record bytes count toward the page
limit. The record-size cap applies to the record bytes after that prefix.
Attachment payload bytes do not count toward the inline record-size cap, but
every table read counts toward the same transition-wide byte, page, and deadline
budget. Every attachment-tagged `blob-ref` occurrence, including
repeated references to the same index/range, counts toward
`max-attachment-refs`; inline `blob-ref` values do not. An indivisible inline
record that cannot fit returns `record-too-large`; SDKs must move a large
snapshot to an attachment rather than splitting a record.

Each page owns at most one multiplexed table resource, preventing untrusted
input from allocating one Component resource handle per record. The table must
be `some` if at least one reference occurs and `none` if no reference occurs;
indices are page-local and need not be dense. Attachments are immutable for the
lifetime of the transition. A decoder may read only referenced ranges and must
not concatenate the table merely to decode a page.

At the untrusted guest-to-host boundary, the host first validates bounded inline
framing and counts all attachment references without invoking `byte-outputs`.
It rejects the page if the transition-wide reference cap would be exceeded.
Only then may it validate referenced indices and ranges with `len(index)` and
drain bytes with `read(index, ...)`. An invalid index, overflowed range, unused
table, or missing table is invalid plugin output.

The production host constructs host-to-guest `byte-sources`; generated guest
adapters may resolve those trusted references while parsing the already bounded
page. Reads still use the same transition budget. An SDK that treats its host as
untrusted should use the stricter two-pass order above in that direction too.

`edit-page` uses the same defense without binary packet framing. Its bounded
`edits` list contains inline bytes or an
`output-range { index, offset, length }`. Boundary accounting assigns 24 bytes
of metadata to every `output-splice`, plus the length of an inline insert. That
same sum is the edit's record size; output attachment payload bytes are
excluded. A guest stops the page before the aggregate exceeds
`max-page-bytes`, keeps aggregate inline insert bytes within the call's
`max-inline-bytes`, and moves an insert to an output range when the inline edit
would exceed `max-record-bytes`. The page owns at most one optional
`byte-outputs` table supplying all such ranges. The host validates those limits,
checked ranges, ordering, and the aggregate output-reference count before
invoking the table; the table is `some` iff an output reference occurs.

## Canonical validation order

Before exposing guest-produced typed values at an untrusted boundary, the host
runtime must:

1. reject any format version other than `1`;
2. validate frame count, checked lengths, and exact payload consumption;
3. count attachment references and enforce `max-attachment-refs` before
   invoking the page's optional attachment table;
4. validate every tag, UTF-8 string, attachment index, and attachment range;
5. normalize Snapshot JSON v1, validate record-kind-specific structure and
   complete snapshots;
6. enforce entity ordering for complete entity streams; and
7. enforce transition-wide duplicate-key, page-count, byte, and permanent-EOF
   rules in the host drain validator.

Before the binary packet format is declared frozen for independent third-party
SDK implementations, it needs one shared golden-vector corpus covering
recursive duplicate rejection, Unicode scalar handling, arbitrary-precision
numbers, canonical bytes, normalized equality, both attachment ownership
directions, and generated IDs. ID vectors cover all-zero/all-`0xff` namespace
halves, ordinal zero/`u64::MAX`, exact unpadded base64url, canonical decode and
rejection, and equality under the same explicit mutation identity. This draft
ships one Rust host and four Rust reference plugins with local conformance
tests; extracting that shared cross-SDK corpus is a rollout gate, not an
unverified claim of this change.
