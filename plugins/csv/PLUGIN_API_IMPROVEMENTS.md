# Plugin API improvements from CSV QA

This report separates improvements possible inside the current CSV plugin from
changes to the shared plugin API. Measurements use the CSV QA probes in this
folder with baseline engine `d99c3f37a` and the fixes in this branch. Timings are
single-run observations on the development machine, not portable guarantees.

## Evidence

The native release core parsed/indexed 1,000,000 rows (45 MB) in 93.5 ms and
updated one row in about 0.107 ms. The adapter nevertheless reopened an identity
checkpoint in 1,454 ms; changing two rows took 867 ms and replaced the entire
45 MB file. The actual SQL/Wasm path at 100,000 rows (4.5 MB) took 134 ms for the
first cell edit, 1,031 ms for the second, and 1,744 ms for a two-row update.
The engine was a development build and the Wasm plugin a release build;
component compilation was warmed separately. A 1,000,000-row import failed
with `component transition exceeds max-total-bytes`.

These observations reveal both plugin implementation problems and shared API
limitations. The existing API already supports file/state range reads and
bounded file splices. A full-file read for a point SQL update is not required by
the API and should be fixed in the plugin before expanding the contract.

## 1. Give cold imports budgets proportional to semantic output

The default aggregate transition limit is 128 MiB. Cold-file admission scales
record/page capacity and execution time with file size, but not aggregate
output capacity. CSV expands short source records into typed rows with UUIDs,
order keys, cells, and page metadata. A 45 MB file can therefore exceed the
output limit while all individual pages and guest memory remain bounded.

Scale aggregate traffic for cold imports with a documented expansion allowance
and a hard cap. Keep sparse transition limits separate. Longer term, admit work
using source bytes, estimated row count, and estimated semantic output rather
than one byte-size heuristic. Report the exhausted resource, observed value,
limit, and transition kind in errors. Do not silently grant unlimited memory or
time. Test both legitimate large imports and exact rejection at each bound.

## 2. Make bounded row lookup straightforward

`serialize_changes` receives changed typed rows, accepted bytes, and private
state. It has no direct accepted-row lookup by primary key or row locator.
CSV must build and persist its own relationship between UUID, order, and byte
span. Reconstructing this relationship on every edit overwhelms the actual
cell-edit cost.

The current plugin can use generated UUID ordinals and paged offsets for the
common case. A more general API should offer batched accepted-row lookup and an
optional plugin-owned locator index with point/range reads and atomic updates.
Keep locators rebuildable: semantic rows and accepted bytes remain authoritative.
Exercise arbitrary UUIDs, inserts, reorders, deletes, missing state, and reopen.

## 3. Support efficient changes to indexed state

Private state supports range reads, but writes replace a key's value. Changing
a row's byte length shifts every subsequent absolute byte offset. Plugins can
implement page summaries and relative offsets, but each plugin then owns a
storage tree, encoding, validation, and recovery protocol.

Provide an SDK collection for ordered records with subtree byte-length sums,
UUID lookup, and copy-on-write page updates. A point edit should touch the
record and a logarithmic number of summaries, independent of file size.
The host should commit byte splices, row changes, and index updates atomically.
This can start as an SDK library using existing state operations; a new ABI is
justified only by measured host-call or copying overhead.

## 4. Define normalization feedback for SQL writes

Lossless rendering carries lexical state as well as cell values. CSV examples
include forced quotes, literal quotes in unquoted fields, mixed line endings,
and the missing final terminator. Moving an unterminated row into the middle
requires a separator. A literal U+FEFF at byte zero must be quoted to avoid
becoming an encoding BOM. An empty LF row following CR must be quoted to avoid
becoming one CRLF terminator.

Specify whether serialization can return normalized lexical columns, and how
those updates become durable without recursively invoking serialization. A
normalized row should retain its semantic values and requested identity/order.
Test file-to-rows-to-file and rows-to-file-to-rows separately: byte equality
alone does not prove SQL editability, and value equality alone does not prove
losslessness. Make normalization observable and deterministic across warm,
cold, merged, restored, and reopened paths.

## 5. Give row ordering a usable shared interface

CSV's positional string arrays preserve headerless files, duplicate/blank
headers, empty strings, leading zeroes, and ragged records. Promoting a guessed
header or inferring numeric types would lose those distinctions.

The awkward part of SQL insertion is allocating a valid order key. Add a
shared SQL/SDK operation for a key between two neighbors and document the
UUID tie-break rule for concurrent insertion. Avoid a constant default key:
it would silently order appended rows by UUID rather than insertion intent.
A host-owned append/before/after operation could handle concurrent allocation
without exposing the encoding in everyday SQL.

## 6. Measure the adapter, not just the parser

The core benchmark hid the second-edit checkpoint cliff. Add host counters for
file bytes read, state bytes read/written, typed rows consumed/emitted, output
bytes, Wasm peak memory, and host calls per transition. Expose these through
the native testing harness and the real SQL/Wasm qualification harness.

Require 100k/1M-row scenarios for import, first and repeated point edits,
variable-length edits, multi-row edits, inserts/deletes, reorder, cold state
recovery, and byte-exact output. Assert structural costs as well as recording
time, so regressions do not depend on machine speed. Include tiny malformed
inputs and mixed formatting alongside uniform performance fixtures.

## Suggested sequence

1. Fix the plugin's full-file/checkpoint fallbacks using current range APIs.
2. Correct cold-import aggregate admission and add real 1M-row qualification.
3. Define normalization feedback and ordering helpers with SQL examples.
4. Prototype the indexed-state SDK collection and compare it against CSV's
   implementation using host-call and byte counters.
5. Change the ABI only where the prototype demonstrates a remaining limit.

## Implemented in this branch

The current API now supports repeated sparse SQL edits, bounded grouped batches,
streamed identity checkpoints, and memory-bounded million-row structural edits.
Cold imports receive a source-proportional aggregate output budget (16 times
source bytes, a 128 MiB floor, and a 2 GiB cap). Other resource limits remain.
The host also awaits WASI calls asynchronously: a guest diagnostic flush during
a trap previously could panic the host by nesting Tokio runtimes. Regression
tests now verify ordinary error handling and subsequent instantiation on both
Tokio runtime flavors.

Final SQL/Wasm qualification passes at 100k and 1M rows, including exact reopen.
At 100k, the second edit fell from 1,031 ms to 16.3 ms and the two-row edit from
1,744 ms to 16.4 ms. At 1M, import now succeeds in 43.63 s; repeated point edits
take 88.7 ms and 4,097 alternating updates take 4.39 s. See the complete
[QA report](QA_REPORT.md) for methodology and remaining costs.

## Remaining host query work

Selecting the first two records with `ORDER BY order_key, id` still takes
0.75 s at 100k and 7.95 s at 1M in the development engine. Bounded plugin I/O
does not provide bounded SQL execution. Add query-plan visibility and an ordered
index access path with LIMIT pushdown before claiming file-size-independent
editing end to end. The qualification also encountered `LIX_UNSUPPORTED_SQL`
for an `order_key <= $2` predicate. Its bulk operation selects ordered IDs and
updates using bound IDs instead. Extending comparison support belongs with the
SQL planner, rather than an ad hoc CSV API. Treat these as separate measured
host improvements alongside the proposed plugin API work above.
