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

One required trait makes every file lifecycle visible at compile time:

```rust
pub trait Plugin: 'static {
    fn open(input: &OpenFile<'_>, output: &mut Output<'_>) -> Result<()>;
    fn file_changed(input: &FileUpdate<'_>, output: &mut Output<'_>) -> Result<()>;
    fn rows_changed(input: &mut RowUpdate<'_>, output: &mut Output<'_>) -> Result<()>;
    fn restore(input: &mut RestoreFile<'_>, output: &mut Output<'_>) -> Result<()>;
    fn cold_file_changed(input: &mut ColdUpdate<'_>, output: &mut Output<'_>) -> Result<()>;

    fn resolve_conflict(conflict: RowConflict<'_>) -> Result<ConflictResolution> {
        Ok(conflict.take_b_or_delete())
    }
}

struct NotePlugin;

// After implementing every callback above, export the Component entry point.
lix::plugin::export!(NotePlugin);
```

The export macro is required in the plugin crate. Without it, the Rust code may
compile but the resulting Wasm component has no Lix plugin export.

The WIT has one stateful export, `apply(transition-request, transition)`, whose
request variant uses the same five names. Conflict resolution is the only
separate stateless export.

Normal row output is one typed call:

```rust
let id = input.creates.id(0);
let snapshot = format!(r#"{{"body":"hello","id":"{id}"}}"#);
output.row(RowMutation::Create {
    schema_key: "note",
    local_ref: 0,
    snapshot: snapshot.as_bytes(),
})?;
```

`Output::row` owns record framing, page limits, create-page separation,
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

Cold input has one byte-authority shape:

```rust
pub struct ColdUpdate<'a> {
    pub before_path: String,
    pub after_path: String,
    pub before: Snapshot<'a>,
    pub edits: Vec<FileEdit>,
    pub rows: RowReader<'a>,
    pub creates: CreateContext,
}
```

Every plugin lifecycle path is a required resolved string. The engine may use
an absent path internally for unowned file states, but it rejects that state at
the Component boundary instead of exposing an impossible `Option` branch to
plugin authors. Restore remains descriptor-free by design.

## One row page in both directions

Every row input and output crosses the Component boundary as the same
bounded `row-page` byte envelope. A page has exactly one snapshot section;
the codec does not expose representations, layouts, or manual page sizing.
The typed SDK batches `Output::row` calls and flushes pages automatically.

Large snapshots use a bounded attachment referenced by the page. Inputs and
outputs remain paged; neither side requires a complete row collection or a
per-row ABI call.

The SDK targets 256 KiB of records per normal output page. This is an internal
batching choice, not author-facing API: a single record may grow to the host
page limit, and a larger snapshot automatically uses an attachment. Profiling
showed that this target preserves sparse point reads while amortizing Component
calls on dense imports.

Restore and cold transitions expose `RowReader`, whose items always contain
a snapshot. Only `rows_changed` uses `RowChangeReader` and can yield a
tombstone, so plugin authors do not repeat impossible-state checks.

The engine validates complete typed creates generically; it does not branch on
CSV, Markdown, text, or any plugin/schema key.

### CSV A/B result

The 10.68 MB / 220,001-row RocksDB import benchmark compared the removed dense
row encoding with streaming `Output::row` snapshots. The universal path
kept guest high-water memory unchanged at 28,639,232 bytes and changed median
latency from 1,234.95 ms to 1,272.17 ms (+3.01%). P95 changed from 1,830.85 ms
to 1,880.19 ms (+2.69%). Allocated bytes increased 0.13%, allocation count fell
18.48%, and peak live bytes increased 7.49%.
Boundary bytes increased 69.44%, but remained bounded and all exact hash,
cardinality, and reopen checks passed. This measured trade buys one row API
for every plugin, so the dense authoring lane is removed.

## Manifest

The author manifest keeps `entry` and omits constants already fixed by the
component contract:

```json
{
  "key": "plugin_notes",
  "match": {
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
- conflict `take` does not copy selected snapshots through Wasm;
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

## Measured decision

Selected: the universal page and author surface above. Measurements compare the
candidate with a frozen pre-change worktree on the same machine. Each lane used
five discarded warmups followed by 21 release samples pinned to CPUs 0-3.

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

All scored ratios pass the gates above. Process RSS is an external maximum-RSS
measurement around the complete 21-sample process; it is reported separately
from the in-process allocation scorecard. Correctness tests cover exact output,
row cardinality and history, reopen, direct cold successor, generated-ID
stability, and oversized attachment streaming for all applicable formats.

The timing matrix measures import, sparse, and cold behavior for CSV and JSON,
and representative warm sparse transitions for Markdown and Excalidraw. Cold
and reopen behavior for Markdown and Excalidraw is correctness-tested but is not
a dedicated timed lane. Universal row pages are exercised in both ABI
directions, but there is no isolated row-to-file microbenchmark. Those are
explicit coverage limits, not inferred performance claims.

Machine-readable summaries, captured sample logs, external RSS records, and the
passing scorecard are stored outside the worktree in
`/root/projects/lix-profile-results/final` so Cargo cleanup does not remove the
evidence. The scorecard executable enforces the guest high-water and RSS gates
from those logs in addition to its in-process metrics.
