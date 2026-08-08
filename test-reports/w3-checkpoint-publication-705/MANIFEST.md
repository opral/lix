# W3 checkpoint publication oracle manifest

Target/base:

```text
head 705440f55eccba9e2d55c0951d6a684737005d76
tree 2b8dcb45a2d06bdda86d0fa5add5ea8c12d18c2d
parent 9f3c703e953440cde1d60b1511467c4337648c8f
```

The package is test/report-only and contains no production source, Cargo
wiring, storage write, runtime, or benchmark. `verify_source_contract.sh`
requires the exact head/tree, runs `git diff --check`, confirms the existing
one-view/plan/prepare/commit ordinary route and checkpoint staging symbols,
then deliberately returns RED while the old checkpoint-publication rejection
is present.

Exact source command:

```text
test-reports/w3-checkpoint-publication-705/verify_source_contract.sh \
  "$PWD" 705440f55eccba9e2d55c0951d6a684737005d76
```

Expected exit: `1`. No cargo, runtime, adapter, or benchmark command is part
of this calibration.
