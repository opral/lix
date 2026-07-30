# Plugin API v3: host-owned persistent arenas

## Status

Accepted hard cut. API v3 is the only plugin ABI and runtime path. Total-memory
measurements include host-owned live bytes, guest linear-memory high water, and
transition materialization. The implementation retains no v2 adapter, runtime,
WIT, SDK, or format plugin.

## Evidence for the cut

On the v2 baseline at `57d619aad`, the existing 220,000-row / 10.68 MB CSV
acceptance workload measured:

| transition | v2 guest linear-memory high water |
| --- | ---: |
| initial import | 53,018,624 bytes |
| cold entity restore | 61,997,056 bytes |
| one-row warm byte/entity transition after restore | 61,997,056 bytes |

The production `vscode-docs` Markdown replay at commit `d5badf95` previously
measured 102,367,232 bytes after its borrowed-view optimization. Both profiles
show that paged ABI transport alone is insufficient: the accepted document and
its indexes remain persistent inside the guest Store.

## Ownership model

Each accepted version is one atomic root over three separate immutable arenas.
File bytes use a rope of immutable page slices. Durable entities and opaque
state use content-addressed keyed maps whose values are independently paged.
The separation is intentional:

- byte coordinates and splice sharing are unrelated to entity identity;
- durable entities are merge authority and retain the existing schemas and
  semantic granularity;
- opaque state is generation-specific, rebuildable, and never merge authority.

A transition receives the old root and a host-created transaction already
containing the verified candidate byte rope. The plugin reads affected ranges
and keys, replaces affected state pages, and returns the transaction with a
sparse cursor. The host validates the complete cursor before atomically
publishing the new root. Dropping the transaction is rollback.

## Format page layouts

The schemas and semantic entity boundaries do not change.

| plugin | durable entity page key | opaque state page |
| --- | --- | --- |
| Markdown | existing `markdown_node_v2` identity | top-level source range, compact tree node/index |
| CSV | existing table/row identity | 512-row span/dialect chunk and 64-row identity chunk |
| JSON | existing root/member/item identity | 512-node lexical span/parent index chunk |
| Excalidraw | existing scene/element/file identity | scene template plus element/file span chunks |

File edits identify byte pages first; those pages identify the minimum state
keys to load. Entity edits arrive with changed durable keys. A plugin may read a
neighbor page for delimiter, syntax, or ordering context, but whole-root scans
are cold-path behavior and are counted.

## Correctness gates

The host-arena implementation covers exact byte reconstruction, stable unchanged
entity value identities, deterministic successor roots, constant-time
branching, rollback on invalid output, cache-independent roots, generation
upgrades without arena rewrites, and sparse three-arena commits. End-to-end
plugin tests must additionally retain the existing v2 corpus, identity,
conflict, and merge assertions after the four format adapters move to v3.

## Benchmark gates

Report all of the following for v2 and v3:

- host unique page bytes plus guest high-water bytes;
- source, entity, state, attachment, and cursor bytes crossing the Component
  boundary;
- pages/keys read and replaced;
- cold import, warm byte edit, warm entity edit, branch switch, eviction
  restore, and merge p50/p95 latency;
- exact output hashes and semantic row counts.

For each Markdown, CSV, JSON, and Excalidraw fixture, cold import, warm byte
edit, warm entity edit, eviction restore, and merge must independently satisfy
`v3 p95 <= v2 p95 / 2` and `v3 peak total bytes <= v2 peak total bytes / 3`.
Medians are reported but cannot hide a failing p95. The deterministic arena
micro-scorecard is a design check, not the acceptance decision. The decision
comes from the same RocksDB/public-SQL workloads used by the v2 Markdown and
CSV profiles, expanded to JSON and Excalidraw.
