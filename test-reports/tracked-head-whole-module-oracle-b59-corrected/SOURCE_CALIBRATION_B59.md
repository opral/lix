# Reproducible source-gate calibration

This is a test/report-only b59 calibration. No production source, Cargo
manifest, adapter, build target, or runtime fixture was changed or executed.

Exact command:

```text
bash test-reports/tracked-head-whole-module-oracle-b59-corrected/verify_whole_module_source.sh \
  /root/repos/lix-tracked-head-whole-module-b59 \
  b59e1f11a51153e0a787a81f0f25bf104d150aaf
```

The output is path-normalized, sorted, and deterministic. It exits `1` by
design because exact b59 is the RED deletion baseline: the old callers and
spaces remain while the defining `tracked_head` files are already absent.

```text
exit_status=1
output_sha256=f8e3c11af5fa5fe3c35973a727ad31bbfed9e27b4908b23d907ebbdc71d12867
```

The corrected verifier explicitly reports the two
`session/merge/branch.rs` wrapper sites, scans source plus compiled-test and
engine-benchmark roots, and confirms the direct public-SQL entity/PK/columnar
paths are untouched. Future candidate output must be GREEN with the same
command and an exact new output hash.
