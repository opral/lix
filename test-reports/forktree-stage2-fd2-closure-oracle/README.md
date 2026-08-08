# FD2 whole-closure correction oracle

This additive package is a deterministic TEST/REPORT-only oracle for the
immutable file-history candidate:

`b484e20d845aee3f8137bfa3496f9b3cd0e8cd35` / tree
`4477c83b246bddac09cd972564bd4ccd67f90f7b`.

It binds the prior blocker report SHA
`83871d2d7c1e8faa0231f77aae75a3f2811debfaeaebd5fb6c18aa83d74d5e96` and
does not modify either production provider or any pre-existing oracle path.

The package contains:

- `source_gate.py`: function-scoped source proof. It must reproduce RED on
  b484 and records the already-correct working-diff path as a positive
  control.
- `model_oracle.py`: executable deterministic semantic fixtures for all nine
  closure seams plus valid live/tombstone/explicit-empty controls.
- `run_oracle.sh`: bounded package-only runner. It expects the source gate to
  reject b484 and the pure model to pass.
- `CLOSURE_ORACLE.md`: exact contract, matrix, and correction requirements.
- `REPORT.md`: immutable handoff scope and expected calibration.

Run without compiling or executing production code:

```text
bash test-reports/forktree-stage2-fd2-closure-oracle/run_oracle.sh \
  /tmp/lix-fd2-review-b484
```

The source argument must be a clean checkout at the exact b484 commit. No
adapter, Memory, RocksDB, SlateDB, or production runtime is part of this
oracle.
