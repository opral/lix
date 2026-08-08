# Independent review: TrackedHead transaction migration oracle

Verdict: **BLOCKER**. This is a read-only TEST/REPORT-ONLY review of the
immutable transaction migration oracle. No production source, adapter, build,
benchmark, or runtime matrix was changed or run.

## Immutable subject

- oracle ref: `origin/codex/tracked-head-transaction-oracle-413e`
- head/tree: `3e365c3a9c184ca6a870deddeb9ab84908d604f5` /
  `625df3c22b712dd3abb3c39c18cd898862978a27`
- parent/source anchor: `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d` /
  `820fe560da3bbd2b00b788b0b1759c409048cd6e`
- parent→head full-index binary diff:
  `1d657cb835bf1e420fad1b2c22af695fe4a6e0a77a1602e78504c119cec64b35`
- parent→head ordinary diff:
  `61ebdaa3186f75c419c18c21f5454d65832a13e31d3fa10247e52a7bae97f908`
- stable patch ID: `7c14a58b46d42eb876cd2c4fb1365ea3ede4c79c`
- required whole-module gate object:
  `0b9ab47f7ac7da600b215d0b8aa2ab04db6bd36d`

Subject file hashes:

```text
ACCEPTANCE_ORACLE.md             6ef7b2a1d3896c51383b15cbb8d523ef0667567ebe145b19d7ce76884e6aadd3
FUTURE_GATE_COMMANDS.md          e882809d66e4fd38b4ba2077ddaa939f726d6579d0dc155b2529c3595592d77d
MANIFEST.md                       5fd82915b844f6c6d67b506b5b6b49bca8fbcf970f68c61a64cf24a3810ecb24
transaction_migration_model.rs   69e1906e66cfa5cb1ce51916fb5037f47f323dc67213fc1e61bba3c4dc00dd0e
verify_transaction_migration_source.sh
                                  d84c505c6c8dff88ee904240dab72ce2f770d440d5ff083ef46ad508e7595e00
```

## Static review

The declared contract requires one retained coherent view, ForkTree-only
working-diff/current-generation derivation, one prepared publication/plan/
write-set/backend commit, exact selector+epoch CAS, zero writes for no-op,
unsupported, stale, and corruption cases, and cold-reopen verification.

The supplied model does not exercise those semantics. `OpeningView` contains
four scalar integers; it has no selector bytes, branch/global selector roots,
repository/catalog roots, checkpoint ancestry, or retained read identity.
`classify_race` compares only `owner` and `selector_epoch`; it ignores both
`state_root` and `checkpoint_root`. `Publication` validates caller-supplied
counters rather than modeling a state transition or persisted publication.

The model has no working-diff contents or identity digest, generation
derivation, checkpoint floor, staged overlay, savepoint rollback, idempotency
key, or cold-reopen state. It has no malformed/missing/wrong-kind/root
identity/cycle/duplicate corruption fixtures. Consequently the advertised
rollback/savepoint/idempotency/stale/no-op/current-generation contract is not
executable evidence.

The source verifier is limited to `packages/lix/src/transaction` and two
obsolete file paths. It does not scan the complete production caller closure,
does not enforce the declared exact production allowlist, and does not reject
`TrackedHeadContext`/`TrackedHead` fallback outside the selected transaction
paths. Its prerequisite check accepts only the existence of a gate commit
object; it does not verify the gate ref, tree, or its source-result identity.
It also does not prove one `PreparedPublication`, one storage plan, one
prepared write set, one backend commit, or one selector/epoch CAS in source.

The future commands reference a Cargo test target that is not present in the
immutable package and claim Memory/RocksDB/SlateDB evidence only for a future
candidate. No adapter or runtime qualification is claimed here, as required
by the no-build review boundary.

## Blocking conditions

1. Current-generation and working-diff authority is not proven to be solely
   selector/catalog/root-derived; scalar owner/epoch equality is insufficient.
2. Savepoint rollback, idempotency, no-op behavior, and staged-overlay digest
   semantics are absent from the executable model.
3. Corruption and cold-reopen behavior is specified but not modeled or tested.
4. The path verifier cannot establish zero TrackedHead/legacy fallback across
   the full transaction caller closure or enforce the allowlist.
5. The one-publication/one-commit shape is asserted by fabricated counters,
   not verified against a plan or source call graph.

Smallest correction: replace the scalar model with a stateful test-only model
containing authenticated selector/view bytes, branch/global/state/catalog/
checkpoint roots, staged overlay and persisted publication state. Add
rollback, idempotency, no-op, corruption, reopen, and exact one-plan/one-commit
transition tests. Make the verifier validate the exact prerequisite ref/tree/
result and scan the complete transaction caller closure for legacy readers,
writers, spaces, and independent publication points.

Until those corrections and the whole-module source gate are green, this
oracle cannot approve the TrackedHead transaction migration.
