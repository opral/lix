# Corrected b59-bound DuckDB OLAP package

Status: `TEST/REPORT-ONLY`; no production source, adapter, SQL, or benchmark
was edited or executed. This package is a direct successor to the blocked
DuckDB package at `287c7bef0cceb63d991de83b0659d3f030986f2f`.

## Immutable binding

- Exact semantic anchor: `b59e1f11a51153e0a787a81f0f25bf104d150aaf`
- Anchor tree: `700fd04d21bc40c05425c9fc9e10d65c9e1eda24`
- Blocked package tree: `5e2974f60d043df0c7ef26427bd9e4934e0a201a`
- Blocked package full-index diff: `d8fd439e71aeb3a9d6a246779a0f2832711731b89438a9e370989f2ded6c3e4e`
- Blocked package patch ID: `e99d9124876d8dcad7a886f391a386e40a2f7706`
- Historical timing input: `origin/codex/olap-duckdb-comparator-2a0`
- Historical input head: `cd76d29406ed7e00711a5b5ba9c40da537524dd3`
- Historical source/results hash: `20f6b010fa770b3a24e69cf7e13a44cda4977d0b3ee3b705dcc49c95e56b3f99`

The historical input is retained only as timing provenance. Its 27 rows are
not b59 observations and every b59 digest, reopen, verification, resource,
backend, publication, and mutation field is `UNRUN`.

## Correction contents

1. `CORRUPTION_MATRIX.md` names malformed, missing, wrong-kind, and identity-
   substitution cases for global selector, branch selector, state root,
   catalog root, and checkpoint root. It distinguishes valid optional absence
   from missing required authority and requires one retained read, unchanged
   authority fingerprint, typed failure, and zero durable work.
2. `corruption_matrix_model.rs` is a pure Rust model for those 20 cases. It
   passes the typed fail-closed/atomicity tests and the valid-absence test.
3. `RESULTS.csv` has a 37-column schema with source provenance, setup boundary,
   exact digest/reopen fields, backend/resource counters, and per-row source
   provenance. Its 27 historical rows remain setup-excluded and `UNRUN` for
   every future b59 field.
4. `source_verifier.sh` validates exact b59 ancestry, package-only scope, the
   five-domain corruption contract, the full RESULTS schema, all 27 rows, all
   nine query labels at each size, and every artifact hash.

## Checks performed

- `bash -n source_verifier.sh`: PASS
- `rustfmt --edition 2021 --check corruption_matrix_model.rs`: PASS
- standalone `rustc --edition=2021 --test` model build: PASS
- model tests: 2/2 PASS
- `source_verifier.sh`: PASS
- `sha256sum -c SHA256SUMS`: PASS
- `git diff --check`: PASS

No b59 adapter runtime, DuckDB runtime, current-main performance, Rust
production build, or broad benchmark claim is made here. The future Memory →
RocksDB → SlateDB command order, 10K → 50K → 500K cells, exact 27-shape
contract, `>=10%` target, `<=5%` guardrails, setup exclusion, and 20-minute
cell cap remain in `FUTURE_GATE_COMMANDS.md` and `QUERY_CONTRACT.md`.
