# Frozen package manifest

package: test-reports/forktree-historical-correction-cd91
base/head: cd91b9b90f7f468158b4df154adbed9551eb5d60
tree: 5ad2a0c8399971d6803e096fd228c5a6149e06ee
parent: 47957d30ae7c16c89c3c523feea23e2f98461fed
direct successor of v2: 884870554e1efb020a4501824c99b90ef2d3d6e4
v2 tree: 7d0a92056cf1341eb963a0c6c3fed936b95b8284
v2 parent: d23faca4340fc69151c57a65f4e3329adefb109d
v3 scope: add exact cd91 v2 RED calibration bytes and rustfmt-only model normalization
v3 calibration command: bash source_gate.sh <immutable-cd91-root> plus exit_status capture
v3 calibration SHA256: cc0d5dc36a609ab5ce4d5200089aa18d6fadf5bd5a95ada57d586df09bccbbc7
v3 model source SHA256: 38cbe459b0810f55ae3d9c0b12dbe72a86274a876e2605ad41d47f7bb35ece89
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
