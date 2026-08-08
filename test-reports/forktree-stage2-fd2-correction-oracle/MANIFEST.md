# Immutable package manifest

Anchor: `fd2be256d763f17e9f127d4c984e36fba191cb82`

All files below are test/report-only and are covered by `SHA256SUMS`:

```text
ACCEPTANCE_ORACLE.md
MANIFEST.md
README.md
SHA256SUMS
FD2_RED_CALIBRATION.log
fixtures/readers/distinct_view.rs
fixtures/readers/fresh_read.rs
fixtures/readers/legacy_reader.rs
fixtures/readers/mismatched_argument.rs
fixtures/readers/valid.rs
fixtures/registry_cases.tsv
source_gate.py
source_gate.sh
```

`FD2_RED_CALIBRATION.log` is added after the first exact baseline run and is
also included in the final checksum manifest. No path under `packages/lix/src`
or any Cargo manifest is part of the package.
