# Plugin API v1

## Status

Accepted hard cut. `lix:plugin@1.0.0` is the only plugin ABI and runtime path;
there is no compatibility adapter. The selected author API, universal row
page, manifest contract, correctness gates, and performance protocol are
specified in [Universal plugin API v1](universal-plugin-api.md).

## Architecture

Each accepted file version is an atomic snapshot over three host-owned stores:

- immutable file-byte pages;
- durable semantic rows, which remain merge authority;
- opaque, rebuildable plugin state used for indexes and lexical metadata.

A transition reads bounded ranges and pages from the accepted snapshot and
emits a streamed whole-file replacement, row mutations, and state updates. The host validates the
complete result before publishing it atomically; dropping a failed transition
is rollback. Plugins can reconstruct a successor directly after eviction or a
restart without retaining a guest-side document.

Every row input and output uses the same bounded row-page envelope.
Every plugin emits complete typed snapshots through the same SDK method. The
engine never selects a codec or execution path by plugin key, schema key, or
file format.

## Acceptance

The implementation is accepted only when the cross-format protocol in
[Universal plugin API v1](universal-plugin-api.md#performance-protocol) proves:

- exact byte reconstruction and complete semantic rows;
- stable identity, history, conflict, reopen, and cold-successor behavior;
- bounded Component traffic and guest memory;
- matched latency, allocation, peak-memory, and process-RSS measurements for
  CSV, JSON, Markdown, and Excalidraw.

Small measured regressions are acceptable only when they buy a materially
smaller universal author surface and preserve bounded scaling.
