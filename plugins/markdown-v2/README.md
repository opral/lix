# Markdown Component v2 plugin

This crate implements the production Lix Wasm Component plugin API v2 for
GitHub Flavored Markdown. It retains the existing block/tree semantic model:
one `markdown_node_v2` entity per document, block, list/table structure, and
text-bearing leaf. Inline syntax remains an embedded typed AST so independently
addressable blocks stay small without duplicating descendant text.

The v2 document resource is immutable. `fork` is an `Arc` clone,
`file-changed` emits only complete entities whose semantic or render-effective
state changed, and `entities-changed` returns one minimal byte splice. Parsing
is currently whole-document; sparse output and stable identity keep unrelated
durable state unchanged while a later regional parser can optimize the CPU
path without changing the API.

Canonical UTF-8 documents keep that sparse path. Every accepted source whose
canonical render differs from its input bytes carries a base64 lexical
fallback on the document entity; examples include extra blank lines, CRLF,
BOMs, and UTF-16. A cold actor can therefore reproduce the committed bytes
exactly. The renderer reparses and compares that fallback with the current
entity projection before using it; a direct entity edit that changes the
projection renders canonical bytes instead of reviving stale source.

Entity IDs, including the document node, come from the host transition
namespace using the v2 32-character base64url encoding. Parsing may use
temporary identities internally, but none cross the component boundary.

Packet-v1's current durable gate rejects JSON number nodes. Markdown needs
numeric values such as heading depth, ordered-list start, and fence length.
The schema therefore stores the mature `payload` and `format` objects as
explicit `payload_json` and `format_json` strings at the transport boundary.
The plugin validates and decodes them before rendering. This is a wire-format
constraint, not a loss of Markdown semantics.

Focused checks:

```sh
cargo test -p plugin_markdown_incremental_v2
cargo build --release -p plugin_markdown_incremental_v2 --target wasm32-wasip2
```
