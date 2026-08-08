# Frozen package manifest

package: test-reports/forktree-historical-correction-cd91
base/head: cd91b9b90f7f468158b4df154adbed9551eb5d60
tree: 5ad2a0c8399971d6803e096fd228c5a6149e06ee
parent: 47957d30ae7c16c89c3c523feea23e2f98461fed
scope: package-only test/report/model/source-gate files
production edits: none
Cargo edits: none
runtime/adapter execution: none

Files:
- README.md: package scope and future order.
- ACCEPTANCE_ORACLE.md: source ownership and semantic contract.
- FIXTURES.tsv: deterministic row/corruption cases and zero-publication counters.
- history_view_model.rs: dependency-free pure model; intentionally not Cargo-registered.
- source_gate.sh: path-aware wrapper pinned to cd91b9b9.
- structural_source_gate.py: balanced function/call-argument verifier and
  production-shaped fixture runner; source-only and dependency-free.
- source_negative_fixtures/: positive alias and five negative source cases.
- production_history_fixtures.tsv: exact persisted-row/BlobRef/plugin/tombstone
  discrimination cases.
- CD91_RED_CALIBRATION.log: exact baseline stdout and exit status.
- CD91_V2_RED_CALIBRATION.log: exact corrected-gate baseline stdout and exit
  status.
- MANIFEST.md: this manifest.
- SHA256SUMS: file identities excluding the checksum file itself.

Required successor scope:
The production successor may change only the direct consumer implementation needed
to satisfy the gate. It must not widen into SQL entity/PK/columnar authority,
new format, adapter, benchmark, or compatibility code. The gate must turn GREEN
only when all four consumers use the caller-owned ForkTree view and all required
historical/file failure checks remain present. No production file is changed by
this package.
