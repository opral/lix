# W1b-4 checkpoint/history reconstruction readiness package

This is a TEST/REPORT-only readiness package anchored to exact source head
`e1af471b9ab0f598dafa7c2ddec7867667c81740` (tree
`bfa0d271a723da8250ab76ada16fda90926f1099`). It contains no production
changes, adapter runtime, or Cargo build output.

The package is deliberately RED against the anchor. The source verifier
records the remaining checkpoint-selection ownership gap: the current
transaction path constructs more than one `ForkTreeReadFacade` while planning
one checkpoint operation. The future correction must bind chronology, state
diff, undo/history retention, and checkpoint-floor decisions to one
operation-owned facade/read.

Files:

- `W1B4_CHECKPOINT_HISTORY_READINESS.md`: contract, scope, call graph, and
  future gates.
- `SOURCE_ALLOWLIST.md`: exact future production slice and forbidden paths.
- `checkpoint_history_oracle.rs`: standalone model and five tests.
- `verify_source_contract.sh`: source-only RED/positive calibration.
- `EXPECTED_RED.txt`: captured verifier output for the immutable anchor.
- `MODEL_RUN.txt`: standalone `rustc -D warnings` run and digest.
- `SHA256SUMS`: package artifact hashes.

The model treats the checkpoint marker as an authenticated claim about the
commit currently being walked. A root with no parent is an implicit
checkpoint; it is not inferred from an unrelated marker. A configured
checkpoint floor is a retention boundary, not a truncation boundary: all
walked commits remain available for history/undo replay.
