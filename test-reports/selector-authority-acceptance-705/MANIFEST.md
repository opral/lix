# Selector acceptance package manifest

Package status: immutable TEST/REPORT-ONLY, source RED calibration.

```text
anchor head: 705440f55eccba9e2d55c0951d6a684737005d76
anchor tree: 2b8dcb45a2d06bdda86d0fa5add5ea8c12d18c2d
anchor parent: 9f3c703e953440cde1d60b1511467c4337648c8f
SELECTOR_CONTRACT.md supplied SHA256: ff784043429f563fb01a29c42eecc90a939f7ce8ac7926d9db07a0f13313da24
```

The named contract file was not found locally; the package records that
provenance limitation and binds to the supplied digest.

Files:

- `SELECTOR_ACCEPTANCE.md`: authority map, source RED findings, A1-A15
  discriminators, and future Memory/RocksDB/SlateDB commands.
- `selector_authority_model.rs`: dependency-free unwired model tests.
- `verify_selector_source_contract.sh`: exact-anchor source verifier.
- `SOURCE_RED_OUTPUT.txt`: captured expected verifier output.

Commands actually run:

```text
git show 705440f55eccba9e2d55c0951d6a684737005d76:<selector sources>
bash -n test-reports/selector-authority-acceptance-705/verify_selector_source_contract.sh
test-reports/selector-authority-acceptance-705/verify_selector_source_contract.sh \
  "$PWD" 705440f55eccba9e2d55c0951d6a684737005d76
git diff --check
```

The verifier is expected to exit `1` with source RED. No Cargo, runtime,
adapter, benchmark, or production command is part of this package.
