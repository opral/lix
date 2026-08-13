---
date: "2026-07-23"
---

# Incremental Wasm file plugins

> Historical design note: the v2 contract described below has been superseded
> by the hard-cut v1 contract in `docs/plugin-api-v1.md`. No prototype runtime or
> compatibility adapter remains in the workspace.

## Summary

Lix file plugins remain sandboxed WebAssembly Components, but API v2 replaces
the stateless whole-file/whole-state calls with persistent, immutable document
resources. A localized byte edit can therefore produce sparse entity changes,
and a localized entity change can produce sparse byte edits, without crossing
the component boundary with the rest of the file.

The production contract is WIT package `lix:plugin@2.1.0` and packet format
`packet-v1`. The public Rust authoring surface and canonical WIT copy live in
[`lix::plugin`](../../packages/lix/PLUGIN.md). The engine and
Rust SDK retain package-local WIT mirrors for generated host bindings; CI
enforces byte-for-byte parity so every published crate remains self-contained.

## Motivation

The predecessor stateless API reparses a complete file and materializes every
plugin-owned entity for each edit and render. That is easy to understand, but
work and memory scale with document size even when one CSV cell or one JSON
leaf changes.

WebAssembly itself is not the bottleneck. The expensive path is repeatedly
materializing rich state across the component boundary. API v2 keeps the
sandbox and typed WIT boundary while allowing format-specific indexes to stay
inside a file actor.

## Decision

Each installed v2 plugin generation is compiled once. The engine creates an
isolated actor for each owned file and branch. An actor contains a Wasm
instance and one accepted immutable document handle.

The lifecycle has two cold constructors, two warm transitions, and one static
merge-resolution phase:

- `open-file` parses initial bytes and emits complete entity upserts.
- `open-entities` reconstructs a document and canonical bytes from durable
  entities after restart or eviction.
- `file-changed` consumes byte splices and emits sparse entity changes.
- `entities-changed` consumes merge-resolved entity changes and emits sparse
  byte edits.
- `resolve-conflicts` consumes only colliding semantic entity triples and
  returns aligned deterministic resolutions before the ordinary
  `entities-changed` render phase.

Every document transition returns a new document. `resolve-conflicts` returns
only a resolution cursor and cannot mutate an actor. The engine retains the old
document until it has drained and validated all output and committed the
transaction. A trap, timeout, invalid packet, failed constraint, or rollback
therefore cannot corrupt the accepted actor.

File-scoped semantic SQL writes take the reverse path through
`entities-changed` in the same database transaction. Multiple statements chain
from a private pending document and publish that document only once, at commit;
rollback discards the complete chain. Writing both blob bytes and semantic
entities for the same file in one transaction is rejected because neither side
has unambiguous authority.

## Boundary model

WIT defines typed capabilities and resource lifetimes:

- immutable byte sources with bounded random reads;
- bounded cursors for entities, changes, and edits;
- a lazy conflict source and aligned resolution cursor for static merge
  resolution;
- immutable documents and explicit `fork`;
- lazy output attachments for large replacement bytes; and
- transition budgets and descriptor metadata.

The packet format carries entity snapshots through a flat checked arena. It is
a transient component-boundary representation, not a storage format and not a
generic author-facing AST.

Inputs and outputs are paged only to bound individual calls. All pages and
source reads share one non-renewing transition budget. Paging cannot reset the
deadline or evade total-byte and record-count limits.

## Semantic changes

An entity upsert contains a complete schema entity. A deletion is a distinct
operation. Render-effective facts such as order, parentage, and native
references must be durable in the schema snapshot rather than hidden in
ephemeral plugin metadata.

A transition may mention an entity key only once. Each upsert is a complete
schema entity and each deletion is explicit. Conflict resolution remains
entity-granular; the API does not promise unsupported cross-entity atomic merge
groups.

Plugins choose their own semantic granularity. For example:

- CSV uses table and stable row entities.
- JSON uses recursive object-member entities and stable array-item entities;
  JSON Pointer is a locator derived from that graph, not entity identity.
- Markdown can use block/container entities and keep inline syntax inside a
  block snapshot.
- Excalidraw can use scene, element, and file-asset entities.

The API does not require one entity per top-level property or one universal
syntax tree.

The JSON reference deliberately narrows direct semantic mutation to one
existing scalar value. JSON byte writes remain authoritative for structure and
lossless layout. This is a format policy, not a limitation of the V2 WIT
contract: it retains the sparse scalar hot path, gives direct scalar writes an
engine-local serialized overwrite rule, and fails stale structural rebases
closed instead of recreating detached JSON nodes. First-class JSON conflict
objects are deferred.

## Plugin conflict resolution

When merge analysis finds the same plugin-owned entity changed on both sides,
the engine first proves a common live file incarnation (owner payload and
incarnation ID), one identical descriptor and complete path, and one pinned
plugin registry entry in all three roots, then invokes the plugin's static
`resolve-conflicts` operation before any merge-specific document hydration or
rendering. A divergent file rename, extension, ancestor-directory path, or
plugin generation remains an ordinary merge conflict: a resolver must never
render using target CSV descriptor/component facts while the merge selects
source TSV metadata or a source plugin generation. The operation is batched per compatible
file/plugin generation and receives a bounded lazy packet source.
Each record contains the common `base` plus two divergent values named `a`
and `b`. The engine assigns those names by durable `(updated_at,
change_id)` order, not by target/source branch labels or transport arrival
order. A merge therefore reaches the same plugin decision regardless of merge
direction. `b` is the higher-ranked deterministic fallback, not an assertion
that a client wall clock is authoritative; true arrival-order LWW belongs to a
future host-issued transport rank.

For ordinary local public-SQL writes today, the engine persists the timestamp
and change ID used for this deterministic tuple. That tuple is auditable and
total, but is deliberately neither commit order nor a distributed LWW protocol:
physical clocks can tie or move backward. A future sync layer can replace the
host comparison with an immutable HLC-style `write_rank` (with actor and
mutation tie-breakers) without changing the plugin ABI: it still receives only
`base`, lower-ranked `a`, and higher-ranked `b`.

The resolver returns one result for each input record, echoing the host-assigned
ordinal in the exact same order. It may `take(base|a|b)` without
copying the selected snapshot through Wasm memory, `replace` it with one
complete merged snapshot, or `delete` it. The host validates exact cardinality
and ordinal alignment, applies the result to the semantic merge plan, then uses the existing
`entities-changed` path to materialize the file once. There is intentionally no
`document` parameter: resolving two small collisions must not cold-hydrate a
large Markdown, CSV, or JSON file merely to access an actor-local index.

The current contract assumes every *eligible semantic* conflict returns a
result. A safe default for an unsupported, overlapping, malformed, or
structural value conflict is the canonical `take(b)` choice; when `b` is
absent, `delete` is the equivalent deterministic result. A divergent file
lifecycle, descriptor, or plugin generation—delete-vs-edit, delete/recreate
with a reused file ID, divergent rename/path, or any plugin generation
change—is deliberately not handed to this value resolver: it
remains an ordinary merge conflict rather than silently pairing live semantic
rows with a deleted owner. Persisted JJ-style conflict rows containing multiple
alternatives, explicit user resolution, and replication of unresolved values
are intentionally deferred to a later data-model and protocol increment.

### Granularity guidance

Resolution quality comes from choosing stable entities that match a format's
independently editable units. A CSV row can be one entity and can compose two
changes that modify different same-index cells, provided identity, order,
field count, quoting, and terminator layout still agree. Concurrent writes to
the same cell, row moves, shape changes, and layout changes take `b` rather
than pretending to implement a structural spreadsheet CRDT. A Markdown
paragraph/block can apply a bounded three-way text heuristic for disjoint
edits, then take `b` for overlap or syntax it cannot preserve. A giant
value stored as one entity remains one merge unit; a plugin should take the
lazy fallback rather than materialize arbitrary large triples for a marginal
heuristic.

Entity keys stay stable identity, not current byte offsets, row positions, or
array indices. The API permits a future JSON plugin to use recursive member
entities, but it does not require every format to use that granularity.

### API-shape decision

Three shapes were considered:

1. A document-bound `document.resolve-conflicts` method would make an existing
   actor index available, but forces cold hydration or actor acquisition for a
   merge whose inputs may be only a few entities.
2. One top-level Component call per conflict is direct to describe, but makes
   boundary, resource, and trap overhead scale with the number of collisions
   and prevents a bounded multi-record packet/attachment strategy.
3. The chosen static batched lazy operation uses one call and one output cursor
   per file/plugin generation. It keeps input snapshots as attachments until a
   format heuristic actually needs them, permits zero-copy `take` results, and
   reuses the existing single render path after semantic resolution.

The choice is an end-to-end mechanism decision, not a claim that a lower-level
ABI alone solves merge performance. Future measurements may refine page sizes,
heuristics, or a small authoring helper without changing the semantic ordering
and alignment contract.

## Identity

Schemas whose `/id` primary-key string property declares both
`"format": "uuid"` and `"x-lix-default": "lix_uuid_v7()"` permit keyless
creates. The plugin emits a transition-local `u32` reference and an ID-free
snapshot. The host derives the canonical UUIDv7, completes and validates the
snapshot, and converts the create to an ordinary keyed entity before storage.
Plugins preserve acknowledged IDs for existing entities.

The create context is bound to the mutation, file incarnation, plugin, and
generation. The engine durably reserves it before accepting creates. Remote
transport retry and exactly-once replay are separate protocol concerns and are
not introduced by this API.

An array position, row number, or current byte offset is not an entity
identity.

## Concurrency and authority

The shared actor represents merged canonical state. A client may also hold an
opaque observation of an exact private document version it previously read or
successfully wrote. A later sparse byte splice must present a still-current
observation; the engine does not infer authority merely because bytes happen
to hash equally.

Transitions are serialized per actor, while unrelated files can proceed in
parallel. Plugin replacement takes an exclusive generation fence through
preflight and commit. Existing owned files permit only a compatible v2
generation replacement: API version, matcher, schema set, and create-default
contract must remain stable.

## Host responsibilities

The engine, not the plugin, owns:

- transaction acceptance, rollback, retry, and durable merge;
- schema and packet validation;
- stable create-context reservation;
- observation authority and stale-view rejection;
- actor scheduling, generation fencing, and eviction;
- source/read/output limits, fuel, deadlines, and linear-memory limits; and
- storage of plugin archives, schemas, entities, and component generations.

Plugins never commit directly and receive no ambient filesystem or network
capability from this API.

## Authoring model

Format logic should be independent from the generated WIT adapter and packet
codec. A basic implementation may use bounded read-all and full-reparse
helpers while preserving the same lifecycle. An optimized implementation can
read only affected source ranges, update its syntax/identity index, and emit
local deltas. There is no separate fast API.

The first production plugins are executable references and consumers of the
shared public `lix_plugin_api_v2` package. It exposes the four irreducible
cold/warm × byte/entity transitions while retaining WIT resources, packet
codecs, paging, attachments, and bounds as runtime internals.

## Limits

The runtime enforces a configurable linear-memory ceiling and a hard
workspace-wide live-Store admission limit. The integrated host defaults each
v2 actor to 128 MiB and permits at most four live Stores, bounding guest linear
memory to 512 MiB before host-side document state. Cached actors, active
transaction leases, pending publications, cold-open candidates, and upgrade
preflight all consume that same budget. When it is full, an idle LRU actor may
be evicted; otherwise the request fails deterministically and can be retried
after commit or with a higher deployment limit. Both values are configurable
through `EngineOptions`; they are deployment policy, not protocol guarantees.
Correct plugins must also obey per-transition record, page, attachment, byte,
fuel, and time budgets. The integrated host keeps the five-second default for
warm edits and static conflict resolution. A cold `open-file` parse gets one
additional second per started MiB of submitted input, capped at one minute, so
the bounded 10 MiB import path is admissible on slower hosts without turning a
hot transition into an unbounded one.

Malformed or globally coupled syntax may require a larger invalidation region
or a bounded full reparse. API v2 optimizes the common localized path; it does
not promise sublinear work for every possible edit.

## Contract scope

`wasm-component-v2` at API version `2.1.0` is the Component plugin contract.
A plugin declares it with:

```json
{
  "runtime": "wasm-component-v2",
  "api_version": "2.1.0"
}
```

The exact API version is checked at installation. CSV/TSV, JSON, Markdown, and
Excalidraw are the in-tree production references. Replacing an owned plugin is
a compatible generation update: API version, matcher, schema set, and
create-default contract remain stable.

The rollout gate is end-to-end: format round-trip and stable-identity tests,
rollback and multiplayer authority tests, bounded-host validation, and
large-file benchmarks on production storage backends. A boundary redesign is
accepted for measured improvements, not merely lower-level ABI microbenchmarks.

## Measured evidence

The full-engine CSV campaign used a 10,680,000-byte, 220,000-row file and one
localized row edit. On RocksDB, edit p50 fell from 6,507.439 ms in the
predecessor implementation to 63.610 ms and exact-render p50 fell from
2,317.470 ms to 18.013 ms. On
cached SlateDB, edit p50 fell from 9,659.544 ms to 80.184 ms and exact-render
p50 from 7,600.187 ms to 6.397 ms. The candidate emitted one durable entity
change, performed no warm source reads, full semantic materialization, reparse,
or full render, and observed 58.3125 MiB guest high-water.

The recursive JSON reference has a real-Component, end-to-end acceptance gate:
an exact 10 MiB flat fixture with 39,870 properties and one byte changed in one
property. It installs the production Wasm component, verifies the materialized
bytes and affected semantic member, and requires one sparse semantic change,
zero warm source reads, and less than 64 KiB across the warm component
boundary.

The remote blob-splice transport keeps a complete cache base as an opaque
SHA-256-verified immutable blob. It reconstructs and hashes a successor once,
then shares that payload with SQL, splice provenance, and the successor cache;
it never accepts caller-supplied digest or splice metadata without this proof.
An isolated 10 MiB JSON / one-byte-edit release benchmark measured the prior
reconstruct + rehash + validation + cache-copy path at 32.250 ms median and
the verified shared-payload path at 7.494 ms median (4.30× faster). It excludes
network, client-side splice discovery, SQL, and CAS persistence. A later real
Wasm gate execution measured 2,339.552 ms cold hydration, 7.543 ms verified
transport reconstruction, 18.252 ms warm engine transition, 25.796 ms total
warm request work, 26,673,152-byte guest high-water, and 418 warm boundary
bytes. These are single-run acceptance measurements, not latency percentiles.

An N=10 authorship evaluation of the immediately preceding WIT surface
completed successfully for every participant, with median final score 76 (p25
72.75, p75 82.75). The final contract keeps that lifecycle but removes two
unused entity streams and the unsupported merge-group wrapper, so the result
is conservative directional evidence rather than an exact final-surface rerun.
It supports the raw interface as implementable across formats, while repeated
packet/binding glue in the four references remains evidence for a small future
helper layer rather than a reason to add an unproven broad SDK now.

The paired latency campaign identifies the accepted mechanism and WIT
lifecycle; it predates the final lossless-format and transaction-hardening
patches in this implementation. The final revision reruns deterministic work
invariants, large-file memory/correctness gates, and end-to-end behavior, but
does not claim a fresh 12-block paired timing campaign.

## Alternatives considered

### Keep the stateless API

This preserves the smallest surface but necessarily rematerializes complete
files and entity sets. It cannot make localized large-file work proportional
to the affected region.

### Core Wasm with a custom ABI

A custom allocator/call ABI can reduce adapter surface, but it gives up WIT's
versioned interface definition, generated bindings, resource typing, and
component composition. It should replace the Component contract only if
end-to-end measurements show a material benefit after equivalent semantics
and validation.

An equivalent recursive-JSON Core Wasm screening prototype did not clear that
gate. On the 10 MB screen it was 1.7–3.0% slower at cold p50 and was 10.5%
slower in an exact cold-reopen spot check. Flat-edit guest memory rose 6.3%;
nested-edit memory fell only 1.8%. Core's hot edit p50 was 0.3–9.2% faster and
its exact-render p50 was 2.6–40.3% faster, but the preregistered replacement
gate required a greater-than-20% edit win across both shapes and backends, or a
greater-than-30% memory win, with hot-path non-regression. Neither alternative
was close on point estimates. These two-block smoke measurements are
disqualification screening evidence, not an acceptance benchmark campaign.


### A universal engine-owned AST

CSV, JSON, Markdown, and Excalidraw have different identity, ordering, syntax,
and conflict requirements. Standardizing one AST would move format policy into
the engine and still not eliminate parsing. V2 standardizes lifecycle and
checked deltas instead.
