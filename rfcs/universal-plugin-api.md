# Universal plugin API v1 experiment

## Decision target

Find the smallest complete API that lets an agent author a correct and
performant plugin in one pass across CSV, JSON, Markdown, Excalidraw, and future
formats. The canonical Component package remains exactly `lix:plugin@1.0.0`.
This is an intentional hard cut: there is no compatibility adapter.

A small measured regression is acceptable when it removes author-facing or
engine surface. Per-record Component calls, unbounded buffering, format-key
branches in the engine, or incomplete cold/reopen behavior are not acceptable.

## Selected author surface

Capabilities are independent. Ordinary schemas need no executable component:
Lix merges row snapshots column by column, using deterministic LWW only for a
column changed differently on both sides. A schema-owning plugin may opt into
`ColumnMerger` for those overlaps:

```rust
pub trait ColumnMerger: 'static {
    fn merge(input: ColumnMerge<'_>) -> Result<ColumnMergeResult>;
}

// `b` is the host's canonically later value. Most mergers return UseLww for
// columns they do not understand and Replace only the domain-specific value.
```

Files are only a projection of rows. A file format additionally implements all
four directions so incremental behavior is explicit at compile time:

```rust
pub trait FileProjection: 'static {
    fn parse(input: ParseInput<'_>, output: &mut RowOutput<'_, '_>) -> Result<()>;
    fn parse_changes(
        input: ParseChangesInput<'_>,
        output: &mut RowChangeOutput<'_, '_>,
    ) -> Result<()>;
    fn serialize(input: SerializeInput<'_>, output: &mut FileOutput<'_, '_>) -> Result<()>;
    fn serialize_changes(
        input: SerializeChangesInput<'_>,
        output: &mut FileEditOutput<'_, '_>,
    ) -> Result<()>;
}

lix::plugin::export_capabilities! {
    column_merger: MarkdownMerger,
    file_projection: MarkdownProjection,
}
```

Export presence is the capability declaration. The Component has three valid
shapes: column merger only, file projection only, or both. There are no
manifest capability flags and no disabled placeholder exports. A schemas-only
plugin has neither `entry` nor Wasm.

Normal row output remains one typed call:

```rust
let id = input.creates.id(0);
let snapshot = format!(r#"{{"body":"hello","id":"{id}"}}"#);
output.create("note", 0, snapshot.as_bytes())?;
```

`RowOutput` owns record framing, page limits, create-page separation,
record counts, automatic flushing, and host errors. Authors do not implement a
packet codec. Creates carry the complete canonical snapshot; the host validates
its generated primary key against `local_ref`. This is the only row output
path for every format, including dense CSV imports.

An ordinary snapshot is a duplicate-free, number-free JSON object in canonical
form. Object keys are lexicographically sorted at every nesting level; arrays
retain their order; the encoding has no insignificant whitespace; and strings
use the shortest JSON escapes (`\b`, `\t`, `\n`, `\f`, `\r`, `\"`, `\\`, or
lowercase `\u00xx` for the remaining controls). Values may be objects, arrays,
strings, booleans, or null. Encode numeric domain values as schema-approved
strings. In Rust, serialize recursively ordered maps (for example `BTreeMap`)
with `serde_json`, or declare every struct's serialized fields in lexical
order; do not hand-build JSON unless you also preserve these rules. For
example, `{"body":"hello","id":"..."}` is canonical while
`{"id":"...", "body":"hello"}` is not.

`CreateContext` is opaque. It exposes deterministic `id(local_ref)`, the
12-byte namespace for persisted plugin state, and reconstruction from those
bytes. Its host representation is not author API.

Incremental file parsing receives the accepted byte/state snapshot, sparse byte
edits, and a create context. Cold execution additionally receives complete
current rows for identity-preserving recovery; the warm 90% path leaves them
absent:

```rust
pub struct ParseChangesInput<'a> {
    pub file_id: &'a str,
    pub before_path: &'a str,
    pub after_path: &'a str,
    pub before: ProjectionSnapshot<'a>,
    pub file_edits: FileEditReader<'a>,
    pub rows: Option<RowReader<'a>>,
    pub creates: CreateContext,
}
```

Every file projection path has a stable file ID and resolved path. Column
merging does not: `file_id` is optional, so the same merger works for ordinary
application rows (for example a conversation body) and rows projected from a
file. Creation/deletion races remain whole-row LWW; plugins cannot change row
identity or raise cross-row conflicts in v1.

## One row page in both directions

Every row input and output crosses the Component boundary as the same
bounded `row-page` byte envelope. A page has exactly one snapshot section;
the codec does not expose representations, layouts, or manual page sizing.
The typed SDK batches `RowOutput` mutations and flushes pages automatically.

Large snapshots use a bounded attachment referenced by the page. Inputs and
outputs remain paged; neither side requires a complete row collection or a
per-row ABI call.

The SDK targets 256 KiB of records per normal output page. This is an internal
batching choice, not author-facing API: a single record may grow to the host
page limit, and a larger snapshot automatically uses an attachment. Profiling
showed that this target preserves sparse point reads while amortizing Component
calls on dense imports.

`serialize` and cold `parse_changes` expose `RowReader`, whose items always
contain a snapshot. Only `serialize_changes` uses `RowChangeReader` and can yield a
tombstone, so plugin authors do not repeat impossible-state checks.

The engine validates complete typed creates generically; it does not branch on
CSV, Markdown, text, or any plugin/schema key.

### CSV A/B result

The 10.68 MB / 220,001-row RocksDB import benchmark compared the removed dense
row encoding with streaming `RowOutput` snapshots. The universal path
kept guest high-water memory unchanged at 28,639,232 bytes and changed median
latency from 1,234.95 ms to 1,272.17 ms (+3.01%). P95 changed from 1,830.85 ms
to 1,880.19 ms (+2.69%). Allocated bytes increased 0.13%, allocation count fell
18.48%, and peak live bytes increased 7.49%.
Boundary bytes increased 69.44%, but remained bounded and all exact hash,
cardinality, and reopen checks passed. This measured trade buys one row API
for every plugin, so the dense authoring lane is removed.

## Manifest

The author manifest keeps `entry` only for executable capabilities and
`file_match` only for a file projection. A row-only merger has `entry` but no
matcher; a schemas-only plugin has neither:

```json
{
  "key": "plugin_notes",
  "file_match": {
    "path_glob": "*.notes",
    "content": "text"
  },
  "entry": "plugin.wasm",
  "schemas": ["schema/note.json"]
}
```

File bytes always use blob durability. `materialization`, `runtime`, and
`api_version` are removed and rejected. The installed component and durable
registry are validated against `lix:plugin@1.0.0` instead.

Content matching is generic: UTF-8 text, binary, or a bounded
`prefix_excludes { byte, bytes }` predicate. The engine contains no MIME catalog
and no format-specific selection rules.

## Structural acceptance gates

- one guest export per stateful transition;
- no per-row Component calls;
- one universal, bounded row page in both directions;
- no complete row collection before output;
- direct cold update after eviction or restart;
- lazy ranged file/state/attachment reads;
- streamed file replacement;
- default column-based LWW makes no Wasm call;
- a custom merger receives only same-column overlaps, with lazy complete rows
  as context, and can replace only that column;
- branch merges, stale commits, and commit cohorts share the same row merge;
- row-only merging requires no file path, projection state, or document actor;
- no engine/runtime branch on plugin keys, schema keys, or format names;
- exact bytes, complete semantic rows, history, reopen, and cold successor
  remain correct.

## Performance protocol

Use separate process-cold and warm-transition lanes. Each reported lane uses at
least five unrecorded warmups and 21 recorded samples, with deterministic
generated fixtures or fixed source paths shared by both worktrees. Record
p50/p95 latency, Rust cumulative and peak live
allocation, process maximum RSS, guest linear-memory high-water, Component
calls and charged boundary bytes, pages/records, source/state reads, reparses,
renders, durable changes, and hashes of the complete output/row/history
result.

| Format | Required workloads |
| --- | --- |
| CSV | 10.68 MiB / 220,000-row import and sparse row edit |
| JSON | 10 MiB import, sparse scalar edit, and cold successor |
| Markdown | pinned VS Code API exact transition; cold/reopen correctness |
| Excalidraw | 20,000-element localized edit; cold/reopen correctness |

Candidate gates are p50 at most 1.10x baseline, p95 at most 1.15x, cumulative
and peak Rust allocation at most 1.10x, allocation count at most 1.20x only
when bytes improve, guest high-water and process RSS at most 1.10x, and exact
envelope boundary allowance. A slightly larger result may still be selected
only when the surface reduction is explicit and structural scaling remains
bounded.

## Row-first hard-cut profile

The public SQL workflow benchmark compares this cut with exact `origin/main`
`d2c634b2aeb780aff46013ec04902fcbb5c6f846`. Both worktrees use byte-identical
fixtures and benchmark code, optimized builds, and 51 recorded samples per
lane. Projection lanes verify exact bytes. Merge lanes verify the intended
semantic result: both disjoint Markdown edits survive, and the two CSV column
edits compose. Ratios below are candidate divided by baseline.

This is a supplemental in-process regression check, not the comprehensive
protocol above: it records elapsed time and Rust cumulative/peak allocation,
but does not claim RSS, guest-memory, Component-call, or boundary-byte results.

| Lane | p50 | p95 | allocated bytes | peak live bytes |
| --- | ---: | ---: | ---: | ---: |
| CSV file roundtrip | 1.016 | 1.084 | 0.993 | 1.031 |
| CSV sparse file update | 1.026 | 1.120 | 1.033 | 1.002 |
| JSON file roundtrip | 1.008 | 0.997 | 1.025 | 1.001 |
| JSON sparse file update | 0.975 | 0.982 | 0.959 | 0.973 |
| Markdown file roundtrip | 0.965 | 0.944 | 0.931 | 0.928 |
| Markdown sparse file update | 0.971 | 0.949 | 1.021 | 1.000 |
| Text file roundtrip | 0.914 | 0.914 | 0.887 | 0.888 |
| Text sparse file update | 0.970 | 0.955 | 0.928 | 0.872 |
| Excalidraw file roundtrip | 1.050 | 1.036 | 1.017 | 1.019 |
| Excalidraw sparse file update | 0.961 | 0.831 | 1.006 | 1.004 |
| Markdown same-row text merge | 1.002 | 0.955 | 1.039 | 0.989 |
| CSV same-row column merge | 1.067 | 1.106 | 0.992 | 0.981 |

Every reported ratio passes its corresponding in-process threshold. The sparse
result depends on a deliberate API property: warm `parse_changes` receives file
edits and plugin state without hydrating complete durable rows. If it elects to
replace all rows, the host lazily materializes predecessor identities only;
cold execution receives complete rows when identity recovery is required.

## Historical projection baseline

The measurements below selected the universal page and incremental projection
surface before the row-first merge cut. They compare that earlier projection
candidate with its own frozen predecessor; they do not measure `ColumnMerger`,
capability discovery, or the removal of file-scoped conflict resolution. Each
lane used five discarded warmups followed by 21 release samples pinned to CPUs
0-3.

| Lane | Candidate / baseline p50 | p95 | allocated bytes | peak live bytes | process RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| CSV 10.68 MiB import | 0.708 | 0.722 | 0.129 | 0.130 | 0.923 |
| CSV sparse edit | 1.015 | 1.066 | 0.988 | 0.999 | 0.925 |
| CSV cold successor | 0.764 | 0.700 | 0.999 | 1.000 | 0.950 |
| JSON 10 MiB import | 0.952 | 0.989 | 1.012 | 1.000 | 0.928 |
| JSON sparse edit | 0.984 | 0.938 | 0.996 | 1.000 | 0.858 |
| JSON cold successor | 1.013 | 1.101 | 1.000 | 1.000 | 0.838 |
| Markdown VS Code API edit | 0.824 | 0.841 | 0.570 | 1.000 | 0.990 |
| Excalidraw localized edit | 1.031 | 0.904 | 0.999 | 1.000 | 0.979 |

All scored ratios passed the projection gates above. Process RSS is an external maximum-RSS
measurement around the complete 21-sample process; it is reported separately
from the in-process allocation scorecard. Correctness tests cover exact output,
row cardinality and history, reopen, direct cold successor, generated-ID
stability, and oversized attachment streaming for all applicable formats.

This historical timing matrix measures import, sparse, and cold behavior for CSV and JSON,
and representative warm sparse transitions for Markdown and Excalidraw. Cold
and reopen behavior for Markdown and Excalidraw is correctness-tested but is not
a dedicated timed lane. Universal row pages are exercised in both ABI
directions, but there is no isolated row-to-file microbenchmark. Those are
explicit coverage limits, not inferred performance claims.

For this historical projection matrix, machine-readable summaries, captured
sample logs, external RSS records, and the passing scorecard are stored outside
the worktree in
`/root/projects/lix-profile-results/final` so Cargo cleanup does not remove the
evidence. The scorecard executable enforces the guest high-water and RSS gates
from those logs in addition to its in-process metrics.
