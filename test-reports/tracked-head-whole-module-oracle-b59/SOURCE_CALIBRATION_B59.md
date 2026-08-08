# Source calibration record, exact b59

This is a TEST/REPORT-ONLY calibration. No production source, Cargo manifest,
adapter, build target, or runtime fixture was changed or executed.

## Exact invocation

```text
cwd=/root/repos/lix-tracked-head-whole-module-b59
command=bash test-reports/tracked-head-whole-module-oracle-b59/verify_whole_module_source.sh /root/repos/lix-tracked-head-whole-module-b59 b59e1f11a51153e0a787a81f0f25bf104d150aaf
stdout+stderr=single merged stream
path policy=absolute paths in diagnostic lines are intentional; semantic checks are path-normalized by the verifier
exit=1 (expected RED calibration)
```

The exact captured merged stream from the final pre-freeze b59 invocation has
SHA-256:

```text
1f2f6ff00ac3dc37bcbd71979d4ac870341cf84393dd190f4578a77e907df78b
```

It proves both `live_state/tracked_head.rs` and `live_state/tracked_head/hot.rs`
are absent in b59, while direct callers still name `TrackedHeadContext`,
`HotStateTransactionCache`, `TrackedWorkingDiff*`, the marker space, staging
methods, GC cleanup methods, the SQL tracked-state fallback, and the
transaction reader wrapper. Compiled-test residue is absent in the b59 tree,
and every required cohort path exists. This intentional mixed result is the
compiler-frontier baseline; it is not a production acceptance result.

The verifier's future GREEN condition is strict: all obsolete production and
compiled-test symbols, wrappers, old module paths, and the marker space must be
absent; the intentional obsolete consumer must still fail to compile; then the
future Memory → RocksDB → SlateDB sequence in `FUTURE_GATE_COMMANDS.md` must
prove one coherent read and one transaction publication for supported cohorts,
and zero writes for no-op, unsupported, or corrupt input.
