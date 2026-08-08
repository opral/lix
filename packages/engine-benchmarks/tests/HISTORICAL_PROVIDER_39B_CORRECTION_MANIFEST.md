# Historical provider 39b correction oracle

This is a test/report-only package anchored at blocked production head
`47957d30ae7c16c89c3c523feea23e2f98461fed` (tree
`b2e0c8a355fcee64d24cd5fcf77d2351d6fe4170`). It contains no production
implementation and must not be used as a Cargo/runtime acceptance claim.

## Required correction gates

1. A history query that does not project or filter
   `lixcol_commit_created_at` still walks authenticated commit topology and
   returns the exact certified event/plugin rows. Missing parents, malformed
   topology, missing authenticated rows, and payload-key mismatches fail closed.
2. Checkpoint chronology and filesystem working-diff baseline use one retained
   ForkTree chronology view. A parentless root is implicit; a marker selects a
   commit only when `marker == walked_commit_id`. A checkpoint followed by an
   ordinary commit must select the checkpoint, never the ordinary head.
3. Static source gates reject projection-dependent `reachable_nodes`, explicit
   typed deferral, duplicate chronology/TrackedState fallback, raw/second read
   acquisition, and missing ForkTree ownership in the two deferred providers.

## Commands

From the repository root, without Cargo or production runtime:

```text
rustc --edition=2021 --test packages/engine-benchmarks/tests/historical_provider_39b_correction_oracle.rs -o /tmp/historical-provider-39b-correction-oracle-test
/tmp/historical-provider-39b-correction-oracle-test --test-threads=1
bash packages/engine-benchmarks/tests/historical_provider_39b_source_gate.sh
```

The pure model is expected GREEN. The source gate is expected RED on the
blocked 47957 anchor because both candidate findings are still present. No
production Cargo build, adapter runtime, or benchmark is claimed.
