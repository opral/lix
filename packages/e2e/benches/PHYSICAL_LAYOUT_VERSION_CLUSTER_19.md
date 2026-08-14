# EXP-VERSION-CLUSTER-19

## Result

Qualified **NO-WIN**. This is global physical-layout rejection **#19/20**.

The clustered representation materially regresses current-state work as version
depth grows. At `H=10`, median full-scan bytes increased by 282.5% for narrow
rows and 84.0% for wide rows. Point bytes increased by 8.0% and 30.1%,
respectively. These exceed the 5% OLTP guardrail, so the experiment stopped
after the prescribed small crossover rather than running the full matrix.

## Provenance and authority

- Approved C2 parent: `b384d051b4ae888ba84cadcd5f9da719deb0f5f8`
- Parent tree: `f1f525a39ff17287f476b0337cfa326be4f09bd9`
- Model executable: `physical_layout_version_cluster`
- Control: C2 current-state pages plus separate authenticated per-row history
  runs.
- Candidate: one C2-sorted key directory points to one per-identity version
  record. Its first slot is the current typed tuple and its tail is the
  commit-ordered typed history. No separate current tuple or history object is
  emitted for the candidate.

Both geometries use the same Schema-v1 typed tuple/key generator, page policy,
content addressing, branch reference, mutation schedule, and backend adapter.

## Small crossover

Three isolated-process samples were run for integer/text primary keys, narrow
and wide rows, JSONB absent, `N=1000`, `H=1/10/100`, and both geometries.
Numbers below are medians pooled across the two primary-key distributions.

| H | Shape | Metric | Separate history | Version cluster | Delta |
|---:|---|---|---:|---:|---:|
| 1 | narrow | point bytes | 2,719 | 2,878 | +5.8% |
| 1 | narrow | scan bytes | 47,645 | 72,673 | +52.5% |
| 1 | wide | point bytes | 7,820 | 8,047 | +2.9% |
| 1 | wide | scan bytes | 346,981 | 370,102 | +6.7% |
| 10 | narrow | point bytes | 2,719 | 2,937 | +8.0% |
| 10 | narrow | scan bytes | 47,645 | 182,237 | +282.5% |
| 10 | wide | point bytes | 7,820 | 10,178 | +30.1% |
| 10 | wide | scan bytes | 346,981 | 638,454 | +84.0% |
| 100 | narrow | point bytes | 2,719 | 4,098 | +50.7% |
| 100 | narrow | scan bytes | 47,645 | 714,853 | +1,400.4% |
| 100 | wide | point bytes | 7,820 | 10,502 | +34.3% |
| 100 | wide | scan bytes | 346,981 | 1,023,027 | +194.8% |

The candidate does reduce total authenticated bytes at shallow histories
because it removes one object envelope per row, and some point CPU medians are
lower. Those wins do not offset the critical page-read amplification: every
current-state scan and every selected point page carries version tails that the
operation does not need. The H=100 narrow layout also loses the total-byte win.

## Correctness gates

- Fresh-tree and branch-root authentication pass.
- RocksDB cold reopen passes for control and candidate.
- SlateDB cold reopen passes for control and candidate.
- Sparse branch update shares unchanged authenticated records/pages.
- Candidate lookup validates owner/key binding before returning the head slot.
- Rejected corruption: owner/key substitution, malformed count, zero or
  duplicate/nonmonotonic commit, wrong predecessor, malformed tombstone,
  tombstone/head mismatch, malformed directory offset, duplicate key,
  truncation, decoded-length bomb, branch/root substitution.
- Canonical insertion order produces the same candidate page ObjectId.
- Failed validation returns no partial authenticated result.

The full `N=10K/50K`, `H=1000`, JSONB, range-history, diff, and merge matrix was
not run because the small crossover already violated the primary OLTP
qualification gate by a wide margin. No production composition, PR, or
independent reviewer was requested for this rejection.

## Commands and evidence

```text
cargo check --manifest-path packages/e2e/Cargo.toml \
  --bench physical_layout_version_cluster \
  --features storage-benches,slatedb

cargo bench --manifest-path packages/e2e/Cargo.toml \
  --bench physical_layout_version_cluster \
  --features storage-benches,slatedb --no-run

packages/e2e/benches/run_physical_layout_version_cluster.sh \
  target/release/deps/physical_layout_version_cluster-f1171fe8c8d3ba6a \
  /root/repos/evidence/experiment-version-cluster-19/quick-v2 quick 3
```

- Quick manifest SHA-256:
  `e29715efe3e1184fbd1dce35207f83bc102579ffcb264d45309ff42ea291017f`
- Quick checksum ledger SHA-256:
  `e2c22bc2091f30c355eca1f72f6e56998878fc08897c65491370ae11877cc6e5`
- Candidate corruption/backend/reopen log SHA-256:
  `198104fd9a274e1bb16620e8d65b29d16278964239eff51590763b0f9f8a1557`
- Control backend/reopen log SHA-256:
  `3d0defb8f066bbf0840116d780d03b6aea3bea2c3b2830d0ea6eb96bf358f240`
- Empty stderr SHA-256:
  `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
