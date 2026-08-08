# TrackedHeadContext hard-cut acceptance oracle

This directory is test/report-only. It is anchored at immutable e166 and is
not wired into the production crate, workspace manifests, adapters, or PR
automation.

Anchor:

- commit `e1666edd0b4d814a88d985086ecc5a477b5d32e6`
- tree `c680bd7e7f7b70cd784676515839af2dcbbc7917`
- parent `3def82e48ed74ab3d914867767e3bf06def3ffc2`

Files:

- `ACCEPTANCE_ORACLE.md`: frozen semantic contract and case matrix.
- `tracked_head_contract_model.rs`: dependency-free ownership/atomicity model.
- `verify_source_contract.sh`: path-aware deletion/residue verifier.
- `FUTURE_GATE_COMMANDS.md`: exact future Memory -> RocksDB -> SlateDB order.

Expected baseline behavior on e166: source verification is RED because the
implementation files are deleted but residual production/test call sites still
name the obsolete owner. The RED output is a required calibration, not a
candidate rejection; a future migrated candidate must make the same verifier
GREEN without adding a compatibility path.

The package intentionally contains no `Cargo.toml` change, Rust module wiring,
production source, adapter implementation, benchmark, or current-main
comparison.
