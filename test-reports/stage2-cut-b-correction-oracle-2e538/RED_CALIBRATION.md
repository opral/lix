# Red calibration

The exact blocked `2e5389265d0495728325efe43d7eb6d9ad715aa0` source was checked
with `base=head`, so the result cannot depend on a diff presentation or an
unrelated parent. The expected result is nonzero with findings for the old
historical plugin reader, its merge callsites, raw `scan_branch`, arbitrary raw
facade construction/detached branch view, raw-owner root collectors, the
unqualified zero-row plugin registry case, and the unbound filesystem BlobRef
identity case.

Command and output are frozen by the adjacent verifier. After running it, fill
the measured output hash below without changing the source package.

```text
command: test-reports/stage2-cut-b-correction-oracle-2e538/verify_source_contract.sh "$PWD" 2e5389265d0495728325efe43d7eb6d9ad715aa0 2e5389265d0495728325efe43d7eb6d9ad715aa0
expected exit: 1
output sha256: c37e8cf3cfc206a64896c572df4bac77ab73e091c221e14174e1693ee725b55a
output bytes/lines: 3541 / 57
```

This is a deliberate red control. No runtime/build evidence is implied.
