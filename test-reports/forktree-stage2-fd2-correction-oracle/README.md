# fd2 historical-correction source oracle

Test/report-only acceptance package anchored to immutable production head
`fd2be256d763f17e9f127d4c984e36fba191cb82`.

This package is separate from the H3 tombstone/BlobRef-cardinality oracle. It
covers only the two additional fd2 review seams:

1. balanced source parsing proves that each destructured provider scan closure
   passes the tuple element derived from the caller-owned
   `forktree_reader.clone()` to the chronology receiver and `load_rows` reader
   argument; local/fresh/mismatched/legacy readers are rejected;
2. the plugin-history registry contract rejects missing, wrong-kind, malformed,
   and substituted authenticated registry entries without falling back to
   `owner.schema_keys()`, while an authenticated present-empty registry remains
   valid.

The gate is source-only. It does not build, execute adapters, load data, alter
Cargo metadata, change production paths, or create a runtime/cache oracle.

Run from the frozen worktree:

```text
bash test-reports/forktree-stage2-fd2-correction-oracle/source_gate.sh .
```

The exact fd2 baseline is intentionally RED because its plugin-history helper
still has the `owner.schema_keys()` fallback. The new balanced parser proves
the actual destructured provider closure identity on fd2; the package records
that green identity result, the RED registry result, and the complete file
manifest.
