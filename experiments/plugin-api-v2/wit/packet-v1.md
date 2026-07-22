# Lix plugin packet encoding v1

This file is the normative encoding for `packet-page.format-version = 1` and
`change-page.format-version = 1` in `lix-plugin-v2.wit`. The binary packet is
an SDK/runtime detail. Plugin authors use typed `Entity`, `EntityChange`, and
`MergeGroup` values; generated SDK code is responsible for this encoding.

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

An attachment reference addresses the page-local `attachments` list. For a
host `packet-page` it names a `byte-source`; for a guest `change-page` it names
a `byte-output`. `offset + length` must not overflow and must be within the
named attachment. Snapshot bytes must be valid UTF-8 JSON for the entity's
schema.

Snapshot JSON is an opaque transport payload, not a second canonical JSON
format. The host parses it with duplicate-object-key rejection, validates the
schema and primary-key correspondence, and converts it to Lix's versioned
durable JSON representation before comparison, hashing, conflict resolution,
or storage. Consequently semantically equal JSON spellings from different SDKs
interoperate. A `format-only` upsert is rejected as unchanged after this
semantic normalization, not by comparing packet bytes. The packet does not
preserve JSON lexical spelling.

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

- `open-entities.entities` and `entity-update.current-entities` contain
  `entity-record` values.
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
entity streams are strictly increasing by the bytewise tuple
`(schema_key, entity_pk[0], ...)` across page boundaries and contain no
duplicate key. Semantic file order is part of the schema snapshot, not packet
order.

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
rank authority. The host derives the canonical group key from the sorted
member keys, rejects a key repeated anywhere in the complete transition, and
validates the complete upsert against its schema before conflict resolution.

## Limits and attachments

The four-byte frame prefix and all inline record bytes count toward the page
limit. Attachment payload bytes do not count toward the inline record-size cap,
but every attachment read counts toward the same transition-wide byte, page,
and deadline budget. An indivisible inline record that cannot fit returns
`record-too-large`; SDKs must move a large snapshot to an attachment rather
than splitting a record or group.

Attachments are immutable for the lifetime of the transition. A decoder may
read only the referenced range. It must not concatenate all attachments merely
to decode a page.

## Canonical validation order

Before exposing typed values, an SDK/runtime decoder must:

1. reject any format version other than `1`;
2. validate frame count, checked lengths, and exact payload consumption;
3. validate every tag, UTF-8 string, attachment index, and attachment range;
4. validate record-kind-specific structure and complete snapshots;
5. enforce entity ordering or group-local ordering as applicable; and
6. enforce transition-wide duplicate-key, page-count, byte, and permanent-EOF
   rules in the host drain validator.

Golden encode/decode vectors and generated bindings for both ownership
directions are required before the production ABI is frozen.
