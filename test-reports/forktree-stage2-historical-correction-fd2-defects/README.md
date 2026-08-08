# ForkTree historical correction oracle (fd2)

This package is test/report-only. It is pinned to the blocked production
commit `fd2be256d763f17e9f127d4c984e36fba191cb82` (tree
`20110ca5e3c33d34217630fff0a2b784b545317a`) and does not alter production
sources.

It freezes two independent corrections for the historical filesystem/file
read path:

1. a valid authenticated descriptor tombstone is absence in the logical
   snapshot and therefore emits a removal when diffed against a live prior
   descriptor; missing, malformed, wrong-kind, and identity-substituted
   descriptor authority fails closed;
2. every live content-bearing historical file validates exactly one
   authenticated BlobRef and its payload before projection gating. Metadata-only
   projection may omit byte materialization, but may not bypass that
   authentication. Zero, multiple, substituted, or missing-payload references
   fail closed. A valid descriptor tombstone has no live-payload obligation.

The structural verifier uses balanced, function-scoped Rust extraction. The
pure model supplies negative fixtures for each failure class and preserves the
fd2 chronology contract: one retained ForkTree history view, exact checkpoint
marker ancestry, and fail-closed missing/cyclic parent evidence.

The exact fd2 source is expected to fail the correction audit in two places;
that RED result is the calibration, not a production test pass. The model is
the green executable specification for the successor.

## Frozen commands

From this package directory:

```text
bash verify_source_contract.sh audit       # expected exit 1 on exact fd2
rustc --edition=2021 --test correction_model.rs -o /tmp/forktree-fd2-correction-model
/tmp/forktree-fd2-correction-model
```

Future candidate-only adapter gates remain dormant until the production
successor compiles. Run the model and source gate first, then one fresh,
single-threaded Memory, RocksDB, and SlateDB cell in that order; cap every
cell at 20 minutes. Each adapter must exercise filesystem working diff,
metadata-only and content projections, tombstone removal, reopen, and
corruption fail-closed behavior. This package does not claim those runtime
cells.
