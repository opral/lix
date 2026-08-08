# Whole-module deletion acceptance oracle

Test/report-only package anchored at:

- head `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`
- tree `820fe560da3bbd2b00b788b0b1759c409048cd6e`
- parent `11442c1e0023e20307a7231d88cd557bc704fd13`
- exact e166 ancestor `e1666edd0b4d814a88d985086ecc5a477b5d32e6`

The package is not wired into Cargo, production modules, adapters, current
main, or benchmarks. It contains the semantic model, source/residue verifier,
intentional obsolete-consumer compile-fail source, and exact future adapter
command order.

Expected calibration: source verification against 413e is RED because legacy
TrackedHead call sites, marker space, and SQL fallback remain. This is an
intentional baseline calibration. A future candidate is accepted only when
the same verifier is GREEN and the negative consumer still fails for the
obsolete module, reexports, and space.
