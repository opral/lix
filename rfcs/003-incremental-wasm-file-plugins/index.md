---
date: "2026-07-23"
---

# Incremental Wasm file plugins

## Summary

Lix file plugins remain sandboxed WebAssembly Components, but API v2 replaces
the stateless whole-file/whole-state calls with persistent, immutable document
resources. A localized byte edit can therefore produce sparse entity changes,
and a localized entity change can produce sparse byte edits, without crossing
the component boundary with the rest of the file.

The production contract is WIT package `lix:plugin@2.0.0` and packet format
`packet-v1`. Their normative definitions live in
[`packages/engine/wit/v2`](../../packages/engine/wit/v2).

## Motivation

API v1 reparses a complete file and materializes every plugin-owned entity for
each edit and render. That is easy to understand, but work and memory scale
with document size even when one CSV cell or one JSON leaf changes.

WebAssembly itself is not the bottleneck. The expensive path is repeatedly
materializing rich state across the component boundary. API v2 keeps the
sandbox and typed WIT boundary while allowing format-specific indexes to stay
inside a file actor.

## Decision

Each installed v2 plugin generation is compiled once. The engine creates an
isolated actor for each owned file and branch. An actor contains a Wasm
instance and one accepted immutable document handle.

The lifecycle has two cold constructors and two warm transitions:

- `open-file` parses initial bytes and emits complete entity upserts.
- `open-entities` reconstructs a document and canonical bytes from durable
  entities after restart or eviction.
- `file-changed` consumes byte splices and emits sparse entity changes.
- `entities-changed` consumes merge-resolved entity changes and emits sparse
  byte edits.

Every transition returns a new document. The engine retains the old document
until it has drained and validated all output and committed the transaction.
A trap, timeout, invalid packet, failed constraint, or rollback therefore
cannot corrupt the accepted actor.

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

## Identity

Schemas that declare `x-lix-id-allocation: "host-allocated"` receive a
mutation-scoped namespace for new IDs. The plugin derives IDs from
deterministic ordinals in that namespace and preserves acknowledged IDs for
existing entities.

The namespace is bound to the mutation, file incarnation, plugin, and
generation. The engine durably reserves it before accepting new IDs. Remote
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
generation replacement: API version, matcher, schema set, and ID-allocation
contract must remain stable.

## Host responsibilities

The engine, not the plugin, owns:

- transaction acceptance, rollback, retry, and durable merge;
- schema and packet validation;
- stable namespace reservation;
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

The first production plugins are executable references. A shared public SDK is
deliberately deferred until repeated implementations show which adapter
helpers are stable.

## Limits

The runtime enforces a configurable linear-memory ceiling. The integrated host
preserves the existing v1 default at 64 MiB and defaults v2 actors to 128 MiB.
It retains at most four idle/warm v2 file actors, bounding cached guest
capacity to 512 MiB per workspace before host-side document state. Actors held
by live transactions and cold-open candidates remain individually capped but
are not covered by a workspace-wide concurrency limit. Both v2 values are
configurable through `EngineOptions`; they are deployment policy, not protocol
guarantees. Correct plugins must also obey per-transition record, page,
attachment, byte, fuel, and time budgets.

Malformed or globally coupled syntax may require a larger invalidation region
or a bounded full reparse. API v2 optimizes the common localized path; it does
not promise sublinear work for every possible edit.

## Compatibility and rollout

API v1 and v2 runtimes coexist. A plugin opts into v2 with:

```json
{
  "runtime": "wasm-component-v2",
  "api_version": "2.0.0"
}
```

The exact v2 API version is checked at installation. Existing v1 manifests and
plugins retain their current behavior.

The v2 format implementations are opt-in references rather than bundled
replacements. Their CSV, JSON, and Markdown matchers overlap v1 plugins.
Evaluation must use a blank registry or remove the overlapping v1 plugin;
equal-specificity registry-key ordering is deterministic but is not a rollout
priority contract. Default-plugin migration is follow-up work.

The rollout gate is end-to-end: format round-trip and stable-identity tests,
rollback and multiplayer authority tests, bounded-host validation, and
large-file benchmarks on production storage backends. A boundary redesign is
accepted for measured improvements, not merely lower-level ABI microbenchmarks.

## Measured evidence

The full-engine CSV campaign used a 10,680,000-byte, 220,000-row file and one
localized row edit. On RocksDB, edit p50 fell from 6,507.439 ms on v1 to
63.610 ms on v2 and exact-render p50 fell from 2,317.470 ms to 18.013 ms. On
cached SlateDB, edit p50 fell from 9,659.544 ms to 80.184 ms and exact-render
p50 from 7,600.187 ms to 6.397 ms. The candidate emitted one durable entity
change, performed no warm source reads, full semantic materialization, reparse,
or full render, and observed 58.3125 MiB guest high-water.

The recursive JSON reference gates both flat and nested 10 MB fixtures with one
changed property and one sparse entity change. Direct cold hydration reduced
guest high-water from 160,169,984 to 101,056,512 bytes for the flat fixture and
from 202,833,920 to 101,515,264 bytes for the nested fixture. Warm p50 remained
about 65–72 ms because the end-to-end result is dominated by durable storage;
the deterministic guest work stays local.

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

### WASI Preview 3 streams

Async streams can reduce peak memory during cold hydration. They do not by
themselves accelerate synchronous random reads or sparse warm output, which
are the hot-edit path. Preview 3 can be adopted behind compatible lifecycle
semantics when its implementation is production-ready.

Experimental screening measured cold-stream guest memory falling from
15.625 MiB to 6.625 MiB (57.6%) at neutral cold p50. Ready async hot reads were
10.2% slower and one-item async output was 84.2% slower. The resulting
candidate is cold-only future adoption; the production hot path remains
synchronous.

### A universal engine-owned AST

CSV, JSON, Markdown, and Excalidraw have different identity, ordering, syntax,
and conflict requirements. Standardizing one AST would move format policy into
the engine and still not eliminate parsing. V2 standardizes lifecycle and
checked deltas instead.
