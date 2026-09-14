# Plugin API improvements informed by text QA

These are proposals, not changes to the engine API in this PR. The text plugin
is the baseline for arbitrary Git-style text; format plugins such as Markdown
should add finer diffs without weakening byte preservation. Evidence and
reproduction commands are in [QA.md](QA.md).

## 1. Streaming large row values across bounded pages

**Evidence.** The real Wasm test now successfully roundtrips a 4 MiB line and
applies a large after-range edit. The host currently caps component batch pages
at 16 MiB (`packages/lix/src/plugin/runtime/component_shared.rs`), and its
transition limit also caps complete records to the page size. Typed readers
consume complete mutations from one decoded page. A single line's payload
therefore eventually exceeds the transport budget even when total file bytes
are otherwise supported. The byte fallback expands the body by 4/3 before
row overhead, so it reaches that bound sooner than readable UTF-8.

**Proposal.** Keep bounded metadata pages but transport large cell values via
length-addressed attachments with range reads/writes. Add typed stream value
handles to create/upsert and row input; validate total logical value size and
content hash at the host. Persist the row atomically only after all attachments
are complete. Existing file range APIs already solve the analogous file case.
Simply raising the cap moves the failure point and amplifies peak copies.

**Qualification.** Roundtrip/edit 4, 16, 64 and 128 MiB single lines in both
representations; interrupt an attachment write and verify rollback; count peak
guest memory, copied bytes and host calls. Preserve one intuitive SQL row per
line rather than leaking transport chunks into the schema.

## 2. Atomic document validation and optional row normalization on merge

**Evidence.** Base `a\n`; one branch removes the final LF while another appends
`b\n`. Both inputs are valid, but merged rows put an unterminated row before
another row. The plugin correctly rejects the merge and the test proves the
target remains unchanged. A per-column callback cannot inspect the additional
row. `serialize_changes` can emit file edits and state, but cannot publish a
corresponding normalized row change in the same transition.

**Proposal.** Add a document-level merge validation result that can report a
structured conflict with involved row IDs, or return explicit row mutations
and byte edits atomically. Require a deterministic policy and avoid recursive
serialization. The default text policy should report the conflict; automatic
newline insertion should be a deliberate caller choice because it creates
bytes absent from the selected row content.

**Qualification.** Test append/final-LF, reorder/final-LF, delete/edit and
concurrent inserts in both merge orientations; assert rollback and cold history
reconstruction. The current rejection is safe, but a structured conflict would
be more useful than a generic plugin serialization error.

## 3. Shared SQL ordering operations

**Evidence.** `order_key` needs an explicit value, and SQL callers currently
need knowledge of private hexadecimal fractional ordering. Repeated midpoint
end allocations previously produced 50,020,000 key characters for 20,000
appends; the text-local stride fix reduces that to at most 680,000. Literal
defaults already exist through Schema v1 `default_value`; the plugin now uses
that API to default `line_ending` to LF. No new default-expression API is needed.

**Proposal.** Add a shared SQL function such as
`lix_order_between(previous, next)` plus documented batch allocation. Specify
canonical encoding, deterministic tie ordering, append/prepend growth and
bounds behavior. Reuse one tested ordering contract across text and Markdown
rather than duplicating SQL-side algorithms.

**Qualification.** Insert ordinary text with content and file ID plus a clear
placement operation; verify exact LF default and ID. Exercise 20,000 end and
interior insertions, concurrent same-gap allocation, ties and cold reopen.

## 4. Atomic column groups for coupled representations

**Evidence.** `content` and `content_base64` are mutually exclusive. Generic
column resolution can choose values independently. The text plugin's bounded
workaround uses the existing ColumnMerger to prefer NULL when an encoding
switch conflicts with editing the old representation. Exhaustive review of 64
base/left/right combinations and compiled merge tests prove the XOR invariant.
It deliberately makes the encoding switch win; it cannot express arbitrary
whole-row winner policies. Callbacks are invoked only for conflicting columns,
so reading whole rows inside a callback alone does not solve general grouping.

**Proposal.** Permit schema-declared atomic merge groups, or invoke a row-level
merger after provisional column resolution. Supply base/left/right/provisional
rows and require a complete validated group replacement. This supports coupled
units, coordinate pairs, tagged payloads and format-specific structures without
forcing plugins to invent NULL preference rules.

**Qualification.** Retain text's canonical XOR invariant for all representation
combinations, conflict orientations and cold history reads. Compare callback
counts and transferred bytes against the current bounded workaround.

## 5. Measure persistent incremental projections before adding new APIs

**Evidence.** Current native file edits are about 7.4 ms at 100,000 lines in the
controlled core comparison; they still scale with the whole document. The
adapter reads accepted bytes and identity pages, constructs a document, applies
an edit and serializes identities every call. Skipping identical identity pages
now reduces content-only state writes to zero. `Snapshot::read_range` and
`read_state_range` already exist, so range access is not a missing API.

**Proposal.** First prototype a persistent line-offset index using existing
range/state APIs and measure scanned lines, bytes read, host calls, allocated
bytes and cache invalidations. Only add an API if the experiment identifies a
specific bottleneck: candidates are indexed row lookup by primary key, typed
sparse row input on warm transitions, or host-owned validated projection caches
with explicit snapshot identities and invalidation rules.

**Qualification.** Use the committed 1k/10k/100k probes, then 1M lines, measuring
warm and cold file/SQL edits separately. Keep byte-for-byte replay and history
checks. Report preprocessing and index-write costs; do not merely move full-file
work out of the timed region. The richer readable rows increased row emission
about 30% in the controlled shim, so measure the real SDK and transport before
choosing a row-format optimization.
