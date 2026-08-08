# Baseline calibration

Anchor: `fd2be256d763f17e9f127d4c984e36fba191cb82`

Command:

```text
review/stage2-semantic-replay-fd2/verify_source.sh --baseline /tmp/lix-stage2-semantic-replay-oracle-fd2
```

Expected result: exit status `1`, because fd2 intentionally still contains
the legacy replay loader, legacy replay metadata/scope, undo/redo
`tracked_state_reader()`, a raw `StorageReadOptions` acquisition in the
replay caller, and the raw `StorageAdapterRead` helper signature. This is a
source RED calibration, not a runtime claim.

The exact captured output is in `BASELINE_SOURCE_GATE.log`; the successor-mode
red control is in `SUCCESSOR_RED_CONTROL.log`.

The corrected successor must be tested with `--successor`; it must satisfy the
positive ForkTree semantic-owner checks and remove the forbidden symbols from
the exact caller paths. Deferred GC/init/replacement/current-serving/
reachability cohorts are not silently accepted by this package and remain
outside the narrow bridge.
