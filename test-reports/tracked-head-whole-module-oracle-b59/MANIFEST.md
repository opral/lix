# Whole-module deletion acceptance oracle, exact b59

Test/report-only package anchored at:

- head `b59e1f11a51153e0a787a81f0f25bf104d150aaf`
- tree `700fd04d21bc40c05425c9fc9e10d65c9e1eda24`
- parent `713455a3557907ce705d06f720fcdc4486bddd4a`
- exact e166 ancestor `e1666edd0b4d814a88d985086ecc5a477b5d32e6`
- parent..head full-index SHA-256 `4b2885709ba09034068b321be2fe5f27348d6681b1060133af1df0b7d76bb8d4`
- parent..head stable patch ID `63dcb8dcecba8a25dea0ce8be19d26cdac264729`

The package is not wired into Cargo, production modules, adapters, current
main, or benchmarks. It contains the semantic model, source/residue verifier,
intentional obsolete-consumer compile-fail source, and exact future adapter
command order.

Expected calibration: source verification against b59 is RED because legacy
TrackedHead call sites, marker space, and SQL fallback remain. This is an
intentional baseline calibration. A future candidate is accepted only when
the same verifier is GREEN and the negative consumer still fails for the
obsolete module, reexports, and space.

Scope excludes the already frozen public-SQL direct entity snapshot, typed
primary-key, and columnar reader slice. SQL working_diff remains in scope.
