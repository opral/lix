# Red calibration

The exact blocked production anchor `2e5389265d0495728325efe43d7eb6d9ad715aa0`
was checked with `base=head`, eliminating parent/diff ambiguity. The v2 oracle
returned exit 1 with 14 findings. It catches the legacy historical plugin and
merge readers, raw scan/view acquisition, arbitrary raw facade ownership,
missing cursor descriptor binding, raw root owners, unqualified empty registry,
and unbound BlobRef identity/size.

```text
command: test-reports/stage2-cut-b-correction-oracle-2e538-v2/verify_source_contract.sh "$PWD" 2e5389265d0495728325efe43d7eb6d9ad715aa0 2e5389265d0495728325efe43d7eb6d9ad715aa0
expected exit: 1
output sha256: dfb9d9d975f340c9df9f97ac493a33e729f791350214e7e223289b35c04be0ed
output bytes/lines: 4436 / 72
```

The standalone model is source evidence only; it was not compiled or run.
