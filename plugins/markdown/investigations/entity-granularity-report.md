# Markdown entity granularity investigation

Date: 2026-07-12

Baseline: `origin/main` at `023077621f177b1f8369fff8182a6e7e0867bc76`

Target syntax: GitHub Flavored Markdown (GFM)

> **Historical exploration.** This report records the pre-implementation
> candidate analysis against `origin/main`. The implemented v2 deliberately
> differs in two places: it includes a bounded typed format layer and tolerates
> equal sibling order keys by sorting on `(order_key, id)`. See
> [v2-implementation-results.md](./v2-implementation-results.md) for the final
> schema, guarantees, and executable result matrix.

## Decision

Ship Markdown plugin v2 with **state-authoritative Hybrid-canonical**:

1. `lix_state` is the sole durable content authority.
2. Store stable, opaque IDs on structural nodes using child-owned
   `parent_id + order_key` relationships.
3. Store one inline AST payload on each text-bearing leaf (paragraph, heading,
   table cell), with embedded persistent IDs only for inline atoms that need
   independent alignment or attribute review, such as links, images, inline
   code, hard breaks, and footnote references.
4. Store literal content such as code and raw GFM HTML once in the owning node's
   payload. Reject unsupported non-GFM input or represent it once as an opaque
   literal node; never silently drop or coerce it.
5. Validate the complete state graph before rendering. Invalid parents, cycles,
   malformed payloads, incompatible kinds, and duplicate rendered diff keys
   fail closed. Equal sibling order keys render deterministically by
   `(order_key, id)`. There is no byte fallback.

`lix_file.data` remains writable, but it is an input command rather than a
second authority:

```text
write lix_file.data
  -> parse GFM
  -> reconcile persistent IDs against current state
  -> atomically update lix_state
  -> discard submitted bytes

read lix_file.data or lix_file_history.data
  -> validate lix_state
  -> render canonical GFM
```

A generated entity-table write changes that same authoritative state, and the
file view immediately renders the change. Original choices such as CRLF,
additional blank lines, bullet markers, fence spelling, and equivalent emphasis
syntax are intentionally normalized. Exact input-byte preservation is not the
v2 contract.

The two write surfaces need deterministic transaction semantics. Sequential SQL
statements are coherent because plugin reconciliation reads transaction-visible
staged state. A single prepared write that contains both `lix_file.data` and
`markdown_node` mutations for the same file is ambiguous: current reconciliation
reads active state before the sibling node rows are staged. V2 should reject
that mixed batch. Callers can issue ordered statements in one transaction; the
later command then operates on the earlier staged result.

Alec's upward-tree idea is useful as a derived index or Automerge-like rendering
view, but not as the authoritative Lix representation. A move of an 11-leaf
subtree changed 11 upward-path rows versus one hybrid row; changing a 100-item
list from unordered to ordered changed all 100 upward-path rows versus one
hybrid container row.

## Single-authority implications

The supplied feedback correctly identified an authority inversion in the
consumer, but retaining a second raw document is not required to fix it. The
fix is to respect the existing Lix contract: when a plugin owns a file, the
plugin state is the file.

On this baseline, plugin-owned raw file data is removed after
`detect_changes`; historical `lix_file.data` is reconstructed by calling the
plugin renderer. See the reconciliation in
[`context.rs`](../../../packages/engine/src/transaction/context.rs) and history
materialization in
[`file_history.rs`](../../../packages/engine/src/sql2/providers/file_history.rs).
Discarding the submitted raw bytes is therefore expected, not a storage defect.

Measured current behavior:

| Operation or input | Current result | v2 interpretation |
| --- | --- | --- |
| Delete one valid paragraph row | Render omits it | Expected content deletion: state is authoritative |
| Corrupt one block row | Renderer rejects it unless a caller filters it first | Fail closed; filtering is forbidden |
| Opaque top-level HTML | Exact literal round-trip | Keep literal HTML as its node payload |
| CRLF plus extra inter-block blank line | Bytes differ; parsed AST remains equal | Expected canonicalization |
| Missing document row | Valid blocks still render today | v2 must require exactly one document root |

A consumer such as Flashtype must request a complete state snapshot at the
chosen commit/change context. “At least one parseable row” is not evidence of a
complete snapshot. Malformed rows must not be filtered into a different valid
document. A valid row subset can be a legitimate document in a
state-authoritative system, so completeness of a history query is an API/query
contract, not something that can be inferred by comparing state with hidden
source bytes.

Do not add a full raw blob, source hash, root projection hash, or editable AST
overlay. Those introduce duplicate content or a hot derived row. If exact byte
preservation becomes a requirement later, the single-authority design is a
lossless CST/token state in which every byte has exactly one owner. That is a
substantially different model and performed poorly against the current goals:
Markdown trivia ownership is complex, token row counts increase full-state
cost, and content editing becomes syntax-oriented. A root source string plus
read-only identity metadata is also single-authority, but every text edit and
undo is document-granular, defeating this investigation's goal.

| State representation | One content authority | Exact submitted bytes | Granular state edit/undo | Decision |
| --- | --- | --- | --- | --- |
| Hybrid canonical AST | Yes: semantic node payloads | No; canonical output | Good at structural/leaf level | **Use for v2** |
| Lossless CST/token tree | Yes: non-overlapping syntax/token payloads | Yes | Syntax-fragile; likely many rows | Defer unless exact bytes become a hard requirement |
| One root source string + identity metadata | Yes: root source string | Yes | Content remains document-granular | Reject for this goal |
| Full bytes plus independently editable semantic rows | No | Yes | Requires bidirectional synchronization | Reject: two authorities |

## Current Markdown plugin architecture

The current plugin has two schemas:

- `markdown_document`: a fixed `root` row without a manifest;
- `markdown_block`: one UUIDv7 entity per top-level mdast child, containing
  `{id, order_key, block}` where `block` is normalized raw Markdown.

`markdown-rs` parses with `ParseOptions::gfm()`, but the AST is used only to
obtain top-level source ranges. A whole list, blockquote, or table is therefore
one entity. Inline nodes do not have identities.

Reconciliation first diffs complete block strings, retains exact equal blocks,
finds exact-content moves inside replace runs, and then pairs remaining blocks
positionally. New blocks receive UUIDv7 IDs. Order keys are changed outside the
longest retained ordered subsequence. This is effective for top-level Markdown
and deliberately low-noise, but cannot express a list-item or table-cell edit as
an independently reviewable change.

The engine passes every active plugin row to both `detect_changes` and
`render`; there is no incremental state query in the plugin API. Every emitted
entity update becomes a durable Lix change record even though all updates from a
file write share one transaction.

Writing semantic state is mechanically simple: generated entity tables are
directly writable. A probe using
`UPDATE markdown_block SET block = ... WHERE id = ...` changed exactly one
entity row, retained its ID, and made `lix_file.data` immediately render the new
Markdown. Updates are complete entity snapshots rather than JSON patches.
Single-row edits are the native path. Split/merge, subtree deletion, and other
multi-row structural operations must be one transaction; rendering validates
the resulting graph and emits canonical GFM.

There is also a syntax-contract mismatch: the manifest matches `.mdx`, while
the implementation only promises the GFM parser path. For the stated target,
remove `.mdx` from this plugin's glob.

## Candidate implementations

| ID | Representation | Useful property | Primary cost/failure |
| --- | --- | --- | --- |
| C0 | Current top-level raw block | Few rows; source spelling survives inside each block | Lists/tables are one review and undo unit |
| C1 | Top-level block containing raw source plus persistent keyed AST | Stable nested HTML keys without increasing row count | Raw source and editable AST duplicate content; rewrites a large snapshot; undo remains block-granular |
| C2 | One normalized row for every AST node | Maximum semantic addressability and small changed payloads | Many active rows; inline formatting churn; full-state scan/transfer cost |
| C3 | Text/leaf rows containing complete upward ancestor paths | AST can be assembled from leaves | Ancestor edits and subtree moves update every descendant; duplicated invariants |
| C4 | Structural rows plus inline-AST leaf rows | One content representation, local edits/moves, per-leaf undo, moderate row count | Requires a complete canonical GFM serializer |

C4 is the v2 design. There is no compatibility representation or migration
layer. C1 remains useful experimental evidence about visual alignment, but it
should not ship because it duplicates source and AST content inside state and
still cannot undo two independent table-cell edits separately.

## Prototype methodology

Temporary exploration harnesses parsed real GFM, assigned persistent IDs,
reconciled exact subtrees plus typed sibling context, and projected the same
edits into C0-C4. Their byte figures below are deterministic serialized JSON
estimates with 36-byte IDs, not database storage bytes. The final implementation
keeps a reproducible native benchmark in
[`v2_write_amplification.rs`](../examples/v2_write_amplification.rs).

The Flashtype evaluation used commit
[`ec491692`](https://github.com/opral/flashtype/tree/ec4916925a5e856ab6a8bd1933414f18545dc94d),
its checked-in Markdown diff fixtures, and the published `@lix-js/html-diff`
0.1.0 implementation. “Persisted” results substituted edit-history IDs for the
current label/first-cell-derived keys while leaving the renderer unchanged.

Results marked “representation pass” below mean that the candidate exposes a
stable review entity. They do not imply that current `html-diff` can render all
inline GFM changes; that is tested separately.

## Current-plugin regression baseline

At exploration time, two executable suites kept content round-tripping separate
from the proposed v2 entity contract:

- [`roundtrip_corpus.rs`](../tests/roundtrip_corpus.rs) records byte, semantic,
  and first-render-idempotence results.
- [`v2_contract.rs`](../tests/v2_contract.rs) began as red target assertions and
  now runs as an active passing contract suite for the implemented v2.

Measured current results:

| Corpus/contract | Pass | Fail | Interpretation |
| --- | ---: | ---: | --- |
| 41-case exact-byte round-trip | 25 | 16 | Most failures are documented decoding/EOL/block-separator normalization; exact bytes are not the v2 oracle |
| 41-case semantic GFM round-trip | 37 | **4** | Four real content regressions |
| 41-case first-render idempotence | 41 | 0 | Necessary but insufficient: the lossy first render becomes stable |
| 33 repository Markdown files, semantic | 33 | 0 | All real repository documents preserve rendered GFM semantics |
| 33 repository Markdown files, exact bytes | 11 | 22 | Current top-level block joining normalizes many real files |
| Formatting-only edit emits zero state changes | 3 | **12** | Current raw blocks record semantic no-ops as changes; 13 total block upserts |
| V2 nested identity/granularity/graph contract | 0 | **14** | Current top-level model exposes none of the target nested guarantees |

The four semantic round-trip failures are minimal, valid editing states:

1. A link definition immediately followed by `---`: adding the block separator
   changes paragraph text into a thematic break.
2. A link definition immediately followed by indented text: adding the separator
   changes paragraph text into an indented code block.
3. A link definition immediately followed by a custom HTML tag: adding the
   separator changes escaped paragraph text into a raw HTML block.
4. An unclosed fenced block containing a blank content line: trimming terminal
   newlines removes that code content. Unclosed fences are normal while a user
   is actively editing.

The first three come from reconstructing top-level source ranges with an
unconditional blank line. The fourth comes from `trim_matches('\n')` treating a
meaningful newline as boundary trivia. An exhaustive 24,389-document adjacency
probe found 564 semantic mismatches around the same root causes, so these are
not isolated fixture tricks.

The 15 formatting-equivalence tests cover blank lines, EOL/final-newline,
emphasis/strong, heading, bullet/ordered-list, fence, hard-break, character
reference, table delimiter, autolink, and reference-label spellings. Only blank
lines, line endings, and final newline are no-ops today; the other 12 generate
unnecessary state changes.

## Implementation test matrix

| Scenario | C0 current | C1 embedded | C2 every node | C3 upward paths | C4 hybrid v2 |
| --- | --- | --- | --- | --- | --- |
| One-character paragraph edit | Pass, 1 coarse row | Pass, 1 | Pass, 1 | Pass, 1 | Pass, **1 leaf row** |
| Bold text edit | Block-only | Representation pass | Pass | Pass | Leaf pass |
| Link URL and text edit | Block-only | Representation pass | Pass, 2 rows | Pass, 1 | Pass, 1 leaf row |
| Paragraph split/merge | 2 writes | 2 | 3 | 2 | 2; one operation group |
| Move list item | Whole list rewrite | Whole embedded snapshot | 1 order update | 1 leaf/path update in simple case | **1 parent/order update** |
| Move subtree with 10 child leaves | Whole list, 1 | Whole snapshot, 1 | 1 | **11** | **1** |
| Change 100-item list container style | Whole list, 1 | Whole snapshot, 1 | 1 | **100** | **1** |
| Two table-cell edits, undo one entity | Fail | Fail | Pass | Pass for leaf edits | Pass |
| Duplicate/non-Latin list labels | No nested identity | Persisted IDs pass | Pass | Pass | Pass |
| Flashtype #224 list replacements | 0/5 current nested keys | 5/5 with persisted IDs | 5/5 | 5/5 | 5/5 |
| Delete one valid leaf row | Deletes coarse block | Deletes coarse block | Deletes that leaf | Deletes terminal leaf | Deletes that authoritative leaf; container deletion is a subtree transaction |
| Malformed/orphaned/cyclic state | Current row parser rejects malformed input; callers can wrongly filter | Must reject | Must reject | Must reject | **Must reject; no fallback** |
| CRLF/trivia-only file write | Canonicalizes | Raw and AST can disagree | Canonicalizes | Canonicalizes | Canonicalizes by contract |
| Raw GFM HTML | Top-level literal passes | Needs one chosen authority | Literal node | Literal leaf | Authoritative literal node payload |
| Targeted sentence in repository README | 1 changed row | 1 | 1 | 1 | 1 |
| Move README demo section | 3 rows | 3 | 3 | 4 | 3 |

## Quantitative schema results

### One edit inside a 100-item list

| Candidate | Active rows | State size | Writes | Changed snapshot |
| --- | ---: | ---: | ---: | ---: |
| C0 current | 1 | 1.3 KiB | 1 | 1.3 KiB |
| C1 embedded AST + raw block | 1 | 39.9 KiB | 1 | 39.9 KiB |
| C2 every node | 301 | 49.4 KiB | 1 | 0.2 KiB |
| C3 upward leaf | 100 | 49.7 KiB | 1 | 0.5 KiB |
| C4 hybrid | 201 | 42.9 KiB | 1 | 0.2 KiB |

### One edit inside a 100-row, two-column table

| Candidate | Active rows | State size | Writes | Changed snapshot |
| --- | ---: | ---: | ---: | ---: |
| C0 current | 1 | 2.6 KiB | 1 | 2.6 KiB |
| C1 embedded AST + raw block | 1 | 63.3 KiB | 1 | 63.4 KiB |
| C2 every node | 506 | 78.8 KiB | 1 | 0.2 KiB |
| C3 upward leaf | 202 | 91.3 KiB | 1 | 0.5 KiB |
| C4 hybrid | 304 | 63.5 KiB | 1 | 0.2 KiB |

### Repository README

The checked-in README parsed to 175 AST nodes and 59 current top-level blocks.

| Candidate | Active rows | State size | Sentence-edit writes | Section-move writes |
| --- | ---: | ---: | ---: | ---: |
| C0 current | 59 | 16.0 KiB | 1 | 3 |
| C1 embedded | 59 | 48.8 KiB | 1 | 3 |
| C2 every node | 175 | 36.7 KiB | 1 | 3 |
| C3 upward leaf | 93 | 39.2 KiB | 1 | 4 |
| C4 hybrid | 79 | 28.8 KiB | 1 | 3 |

The hybrid is notably smaller than both fine-grained alternatives on this real
document while retaining all 175 matchable AST identities in both edits.

## Flashtype stable-key results

Current Flashtype derives list-item identity from a label or the first three
words, table-row identity from the first cell, and column identity from the
header. That performs well when content already behaves like a primary key, but
content is not a durable identifier.

| Real/fixture scenario | Current safe stable matches | Persisted IDs |
| --- | ---: | ---: |
| Issue #224, five rewritten ordered-list items | **0/5** | 5/5 |
| Quick-facts list edit/add | 4/4 | 4/4 |
| Nested release checklist | 8/8 | 8/8 |
| Unique list reorder | 4/4 | 4/4 |
| Duplicate-task fixture | **5/9** | 9/9 |
| Insert identical duplicate before two identical items | **0/2** | 2/2 |
| Plan table cell edits | 20/20 | 20/20 |
| Table row reorder/add/remove with distinguishable labels | 20/20 | 20/20 |
| Edit the first cell of one row | **0/4 cells** | 4/4 |
| Rename one column in a 100-row table | **0/101 cells** | 101/101 |
| Edit a duplicate-row disambiguator cell | **0/3 cells** | 3/3 |

For issue #224, actual `html-diff` output with current keys was five added
blocks plus five removed paragraphs outside the ordered list and zero word
spans. Persisted list, list-item, and leaf IDs produced no block churn and 12
added plus 13 removed word spans inside the five items.

An exact-duplicate table test found a correctness bug rather than merely noisy
output: duplicate keys caused the edit to the second row to produce zero diff
statuses. `html-diff` indexes the first occurrence of a key. A plugin/renderer
must reject duplicate keys before diffing.

### What stable IDs do not fix

Tests against the actual renderer found:

| Inline edit under paragraph word mode | Actual result |
| --- | --- |
| `<strong>same text</strong>` to `<em>same text</em>` | Zero statuses |
| Same link text, changed `href` | Zero statuses |
| Linked `Ada` to `Ada Lovelace` | Word status appears, but output drops the `<a>` element |

`html-diff` word mode replaces a leaf's children with text/status spans. Stable
leaf IDs therefore solve alignment, not inline-mark or attribute semantics.
Use an inline-AST-aware diff that emits Tiptap/DOM decorations, or extend
`html-diff` with mark/attribute handling. Do not create one Lix entity per word
just to work around this renderer limitation.

`html-diff` also has no moved status. Stable order keys retain alignment for a
move, but a separate move annotation is needed if moves must be visible.

## Lix write amplification

The end-to-end benchmark ran on an Apple M5 Pro, arm64, with a durable SQLite
Lix. Timed spans include the JS worker round trip, WASM plugin invocation,
complete active-state handoff, detection, validation, history/tree staging, and
commit.

The important result is that 100 changed rows are affordable in isolation, but
not free in history: each creates its own Lix change. Complete active-state
transfer and parsing becomes the larger latency risk as row count grows.

Representative measured medians:

| Active top-level blocks | Changed entity rows | Median write |
| ---: | ---: | ---: |
| 1,000 | 1 | about 16 ms |
| 1,000 | 10 | about 16 ms |
| 1,000 | 100 | about 17-18 ms |
| 1,000 | 500 | about 21-24 ms |
| 1,000 | 1,000 | about 25-27 ms |

| Active top-level blocks | Plugin rows | One changed row, median |
| ---: | ---: | ---: |
| 10 | 11 | about 2.3 ms |
| 100 | 101 | about 4 ms |
| 1,000 | 1,001 | about 16-21 ms |
| 5,000 | 5,001 | about 88-89 ms |

Native-only timing varied noticeably with machine load, especially at 5,000
rows, while the same growth curve remained. This report treats the durable
end-to-end measurement as the decision metric.

The implication is not “100 rows is prohibitively slow.” It is:

- avoid ancestor-path designs that turn one logical operation into O(n)
  durable history changes;
- avoid entity-per-inline-token designs that inflate the complete state handed
  to the plugin;
- keep a leaf edit to exactly one authoritative leaf row;
- consider an incremental plugin-state API separately if very large documents
  are an important workload.

## Stable identifier policy

Persistent opaque IDs are the identity. Content hashes, text labels, tree paths,
and indices are matching evidence only.

This agrees with editor/UI practice: React warns against positional or
generated render-time keys, Tiptap's UniqueID extension and BlockNote persist
node/block IDs, while ProseMirror and Slate expose transient position/path
mapping for selections rather than treating a path as durable identity.

Recommended file-write reconciliation (the submitted bytes are transient):

1. Reuse IDs from the current authoritative state.
2. Match unique exact typed-subtree hashes.
3. Anchor siblings with LCS/order context.
4. Score unmatched candidates by node kind, normalized attributes, ancestor
   kinds, neighboring anchors, and text similarity.
5. Reuse an ID only for a clear, one-to-one winner; otherwise mint UUIDv7.
6. For a copy, retain the old ID only in the best contextual continuation and
   mint IDs for additional copies.
7. Prefer a false remove/add over a false match. Record match confidence and
   provenance (`exact`, `contextual`, `fuzzy`, `new`) in change metadata.

For ordering, store the relationship on the child:

```json
{
  "id": "019...",
  "parent_id": "root-or-node-id",
  "order_key": "...",
  "kind": "list_item",
  "payload_kind": "structural",
  "payload": { "checked": false, "spread": false }
}
```

This makes a subtree move an update to the moved root's parent/order fields. A
parent-owned child-ID array would rewrite the old and new parents and create a
hot root row.

## Proposed schemas and validation

This is a clean-break v2, so replace the current schemas directly. Keep one
polymorphic node schema so an entity can retain its ID if its kind changes from
a paragraph to a heading.

```json
// markdown_document: exactly one per file
{
  "id": "root",
  "format_profile": "gfm-canonical-v1"
}
```

```json
// markdown_node
{
  "id": "019...",
  "parent_id": "root-or-node-id",
  "order_key": "...",
  "kind": "list_item",
  "payload": {
    "checked": false,
    "spread": false
  }
}
```

Structural nodes carry only their kind-specific attributes. Paragraph,
heading, and table-cell payloads contain their inline AST. Code and raw HTML
carry their literal value. All content exists once in node payloads; there is no
whole-document source field, duplicated embedded AST, changing manifest, or
identity overlay.

Before rendering or completing a multi-row edit, validate:

- exactly one document root with the supported GFM canonical profile;
- node snapshot `id` equals its entity primary key;
- every `parent_id` exists and every node's parent chain reaches the root;
- the graph is acyclic;
- valid parent kinds and unique sibling order keys;
- structural and leaf kind/payload constraints;
- leaves have no structural children;
- all embedded inline IDs and rendered `data-diff-key` values are unique.

A missing leaf row is a deletion by definition, not corruption relative to a
hidden document. Deleting a container with descendants, however, must tombstone
the complete subtree in one transaction or validation will find orphaned rows.
Malformed rows are never filtered. Post-merge state must pass the same graph and
order validation.

Direct state edits are authoritative operations: update the affected row, or
apply a split/merge/move/subtree operation as one transaction, validate, and
render. File writes follow the reverse path: parse the transient bytes,
reconcile IDs, emit one atomic state delta, validate, then discard the input.

## GFM materialization risks

Canonical serialization is now the main implementation dependency, not an
optional exact-mode concern. The current `markdown-rs` 1.0 API parses to mdast
and renders HTML but does not serialize Markdown.

The obvious companion, `mdast_util_to_markdown` 0.0.2, resolves against the
current `markdown` 1.0 types but is **not GFM-complete**. A local executable
smoke test produced:

| Parsed node/input | `mdast_util_to_markdown` result |
| --- | --- |
| Strong emphasis | Pass |
| GFM strikethrough (`Delete`) | `unexpected-node` error |
| GFM table | `unexpected-node` error |
| GFM footnote reference | `unexpected-node` error |
| Checked task item | Serialized as an ordinary item; checkbox lost |

It cannot be adopted unchanged. Either implement/fork the missing GFM handlers
or select one parser/serializer that owns both directions.

`markdown-syntax` 0.2.0 is now the leading parser/serializer candidate. Its GFM
preset passed the same strikethrough, table, checked-task, footnote, raw HTML,
and strong-emphasis smoke cases and canonicalized the table delimiter as
expected. Further checks against
[repository commit `58910b9`](https://github.com/plimeor/markdown-syntax/tree/58910b9483a0aab6fd9aef5d6be124d3b947f528)
found:

- all 146 normal all-target tests passed;
- its 2,300-case semantic input corpus passed parse/serialize semantic stability
  and serializer idempotence;
- 33 Markdown files in this repository and all 24 Flashtype diff fixtures had no
  parse/serialize errors, no second-render instability, and no HTML-semantic
  change after canonicalization;
- its separate 2,236-case HTML conformance report passed 2,229 cases (99.69%):
  1,968/1,971 CommonMark and 261/265 GFM.

The seven known HTML-conformance misses are two BOM cases, two NUL/link cases,
and three GFM bracketed-autolink cases. BOM handling already exists at the Lix
file-ingress boundary; the bracketed-autolink behavior and NUL policy need to be
fixed or explicitly resolved before adoption. The crate is also a new 0.2 AST,
so pin its commit/version and keep the conformance corpus in the plugin suite.

An indicative release-mode comparison on the same generated paragraph corpus
favored `markdown-syntax`; the numbers are parser-library timings, not complete
Lix writes:

| Paragraphs | Bytes | `markdown-rs` parse | `markdown-syntax` parse | `markdown-syntax` parse + serialize |
| ---: | ---: | ---: | ---: | ---: |
| 10 | 788 | 143-167 us | 53-58 us | 68-73 us |
| 100 | 7,988 | 1.55-1.74 ms | 0.53-0.56 ms | 0.72-0.77 ms |
| 1,000 | 80,888 | 18.0-29.0 ms | 6.6-7.2 ms | 8.8-8.9 ms |
| 5,000 | 408,888 | 135-144 ms | 26.8-38.6 ms | 36.1-40.4 ms |

Based on coverage and performance, prefer a `markdown-syntax`-based v2 spike
over extending `mdast_util_to_markdown`. Adoption still requires closing the
seven conformance gaps and mapping its AST cleanly to the proposed node schema.

The v2 correctness oracles are:

```text
semantic round-trip:  semantic(parse(render(state))) == semantic(state)
canonical idempotence: render(project(render(state))) == render(state)
identity continuity:  file-write reconciliation retains every unambiguously
                      matched entity ID
```

The corpus must still cover BOM/encoding, CRLF/CR/LF, trailing and inter-block
newlines, tabs/indentation, hard-break spaces, alternative emphasis/heading
markers, ordered-list marker numbers, fence character/length, reference casing,
escapes, autolinks, tables, task lists, footnotes, and raw HTML. These cases now
assert documented canonical output and semantic preservation rather than exact
input bytes. A formatting-only file write should emit zero semantic entity
changes, and the subsequent file read returns the existing canonical rendering.

## Historical rollout plan

1. Prototype v2 on pinned `markdown-syntax` 0.2, fix or disposition its seven
   known conformance misses, and retain the 2,300-case semantic corpus plus Lix
   and Flashtype fixtures in CI. The current `mdast_util_to_markdown` is
   insufficient unchanged.
2. Replace the plugin schemas with the v2 document plus polymorphic hybrid-node
   model and implement strict graph/payload validation.
3. Implement file-write reconciliation against current state. Compare identity
   retention, false matches, writes, snapshot bytes, and p50/p95 latency on the
   existing scenario matrix.
4. Add transactional direct-state edit/undo operations and test multi-step
   histories (edit A, edit B, undo A), subtree move plus later child edit,
   delete/restore, and split/merge operation groups.
5. Update Flashtype to request complete historical state and use persisted
   structural/leaf IDs. Reject duplicate keys,
   place removals at the nearest keyed ancestor, and add inline-AST-aware
   decorations plus move annotations.
6. Remove `.mdx` from the GFM manifest.

This is a breaking v2 implementation. There is no compatibility schema,
dual-projection period, raw-source migration, or versioned fallback path.

## Reproduction

```sh
cargo run --release -p plugin_md_v2 --example v2_write_amplification
cargo test -p plugin_md_v2 --test roundtrip_corpus -- --nocapture
cargo test -p plugin_md_v2 --test v2_contract
```

The leading serializer candidate was checked at the pinned commit with:

```sh
git clone https://github.com/plimeor/markdown-syntax.git
git -C markdown-syntax checkout 58910b9483a0aab6fd9aef5d6be124d3b947f528
cd markdown-syntax
cargo test --all-targets
cargo test --features html --test html_conformance -- --nocapture
```

Validation used for this investigation:

```sh
cargo fmt --all -- --check
cargo test -p plugin_md_v2 --all-targets
cargo clippy -p plugin_md_v2 --all-targets -- -D warnings
git diff --check
```

## Primary references

- [`html-diff` guide: durable and fallback keys](https://html-diff.lix.dev/guide/)
- [`html-diff` duplicate-key indexing and matching source](https://github.com/opral/html-diff/blob/88c7e9da025c4fd33bfc61157a4f20b5a4ee9a01/src/render-html-diff.ts#L68-L74)
- [`html-diff` word-mode subtree replacement](https://github.com/opral/html-diff/blob/88c7e9da025c4fd33bfc61157a4f20b5a4ee9a01/src/render-html-diff.ts#L177-L194)
- [Flashtype issue #224](https://github.com/opral/flashtype/issues/224)
- [Flashtype nested key derivation](https://github.com/opral/flashtype/blob/ec4916925a5e856ab6a8bd1933414f18545dc94d/src/extensions/markdown/render-review-diff-html.ts#L293-L400)
- [Flashtype partial projection acceptance](https://github.com/opral/flashtype/blob/ec4916925a5e856ab6a8bd1933414f18545dc94d/src/extensions/markdown/index.tsx#L610-L752)
- [GFM 0.29 specification and conformance examples](https://github.github.com/gfm/)
- [Automerge rich-text schema and upward `parents` path](https://automerge.org/docs/reference/under-the-hood/rich-text-schema/)
- [React list key guidance](https://react.dev/learn/rendering-lists)
- [Tiptap UniqueID](https://tiptap.dev/docs/editor/extensions/functionality/uniqueid)
- [BlockNote stable block IDs](https://www.blocknotejs.org/docs/foundations/document-structure)
- [ProseMirror positions and mappings](https://prosemirror.net/docs/guide/)
- [Slate `PathRef`](https://docs.slatejs.org/api/locations/path-ref)
- [GumTree AST matching](https://www.labri.fr/perso/xblanc/data/papers/ASE14.pdf)
- [`markdown-rs` API](https://docs.rs/markdown/)
- [`mdast_util_to_markdown` API](https://docs.rs/mdast_util_to_markdown/latest/mdast_util_to_markdown/)
- [`markdown-syntax` 0.2 parser/serializer API](https://docs.rs/markdown-syntax/0.2.0/markdown_syntax/)
