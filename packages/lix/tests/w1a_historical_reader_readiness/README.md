# W1a historical-reader readiness package

Test/report-only package anchored to accepted e1af. It converts the frozen W1
map into a source-boundary verifier and a deterministic readiness contract for
the first one-retained-ForkTree-view historical-reader cut.

The current e1af source is intentionally RED: the common SQL history helper
still owns CommitGraphReader/JSON/changelog reads while file and directory
providers separately use ForkTree observed-state rows. The expected RED is
recorded in `EXPECTED_RED.tsv` and `SOURCE_GATE_RESULT.md`.

The future corrected head must make one operation-owned ForkTree historical
reader the sole reachability-aware authority, while preserving public route,
projection, ordering, LIMIT, tombstone/NULL, source-change, and typed
fail-closed semantics. This package does not authorize migration of merge,
undo/redo, transition, changelog, writer, selector, GC, CAS, or W3-W5 code.

Run the source verifier only as a static check:

```sh
bash packages/lix/tests/w1a_historical_reader_readiness/verify_w1a_source_boundary.sh \
  "$PWD" e1af471b9ab0f598dafa7c2ddec7867667c81740
```

It is expected to exit `1` on e1af. No compiler, adapter runtime, or production
source was run or changed while creating this package.
