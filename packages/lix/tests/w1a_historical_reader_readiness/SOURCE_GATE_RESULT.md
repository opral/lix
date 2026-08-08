# W1a source-gate calibration

This is the frozen source-only calibration for the W1a readiness package. It
is deliberately anchored to the unmodified accepted source commit `e1af` and
therefore must be RED until the W1a production slice is implemented. It is not
a compiler, adapter, or runtime result.

## Immutable anchor

- Commit: `e1af471b9ab0f598dafa7c2ddec7867667c81740`
- Tree: `bfa0d271a723da8250ab76ada16fda90926f1099`
- Parent: `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- Production source diff at calibration: empty

## Exact command and result

From the detached e1af worktree:

```sh
bash packages/lix/tests/w1a_historical_reader_readiness/verify_w1a_source_boundary.sh \
  /tmp/lix-w1-e1af e1af471b9ab0f598dafa7c2ddec7867667c81740
```

Observed exit status: `1` (expected RED). The verifier shell syntax, JSON
manifest parse, and `git diff --check` all passed. No Cargo command, build,
adapter test, or production mutation was performed.

The captured output is recorded in `EXPECTED_RED_OUTPUT.txt`. Its original
absolute-path capture was `/tmp/w1a-e1af-source-red.log` with SHA-256
`82445b50046a79d90badd2cec0663617addee0e399fb10b17e387609534ae132`.
The package copy is intentionally path-normalized only by documenting the
anchor-relative findings; the verifier itself emits the active repository
path.

## Findings

The calibration found 13 expected RED findings:

1. `HistoryQuerySource.store` remains exposed.
2. `HistoryQuerySource.json_reader` remains exposed.
3. The storage-bearing `load_history_entries` common helper remains.
4. The common route retains `CommitGraphReader`.
5–10. Entity, directory, and file history providers retain the legacy graph
   reader and common history helper reachability.
11–12. File and directory providers retain the legacy parent-graph helper.
13. The entity-history provider has no ForkTree owner reference.

The two direct raw field probes in `file_history.rs` are PASS because the
legacy fields are reached through the common helper rather than accessed by
those exact names in that provider; this is still represented by findings
1–4 and is not a false GREEN. The expected corrected gate must eliminate the
storage-bearing helper and make every W1a provider consume the same
operation-owned ForkTree reader.

The package is therefore a readiness oracle, not an acceptance claim.
