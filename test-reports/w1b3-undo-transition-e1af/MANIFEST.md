# W1b-3 undo/redo and typed-transition corrected readiness package v2

This package is test/report-only. It is anchored to exact e1af and contains
no production source, Cargo manifest, adapter, benchmark, or Lix runtime
change.

## Immutable source anchor

- Commit: e1af471b9ab0f598dafa7c2ddec7867667c81740
- Tree: bfa0d271a723da8250ab76ada16fda90926f1099
- Parent: b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
- Direct correction predecessor: fc01cd1ddb4a2b5e3ca26a55290a511be83f11c8
- Successor package ref: origin/w1b3-undo-transition-oracle-e1af-correction-v2

## Package files

- W1B3_UNDO_TRANSITION_READINESS.md: call graph, authority contract, semantic
  gates, deletion order, corrected RED calibration, and bounded future commands.
- SOURCE_ALLOWLIST.md: exact candidate production path allowlist and forbidden
  widening.
- undo_transition_oracle.rs: standalone warnings-denied stateful model and
  positive/discriminating negative fixtures for ancestry, identity, and
  reopen corruption.
- MODEL_RUN.txt: standalone model command, corrected binary hash, and 11/11
  result.
- verify_source_contract.sh: source-only structural/argument-aware,
  closure-following candidate-parametric verifier; exact e1af is intentionally
  RED because undo/redo and transitions still use legacy readers.
- EXPECTED_RED.txt: exact source-only calibration from e1af.
- SHA256SUMS: hashes of all package files except this checksum file.

## Non-goals

W1a/W1b-1/W1b-2, checkpoint reconstruction, working-diff, changelog,
selectors/BranchRef, writer/publication, GC, CAS/blob layout, and W3-W5 are
excluded. No production build or adapter runtime is claimed.
