# Retire obsolete private APIs

Baseline: `4d1c219e5f08a6b342e7a013f72f9097ad7261e4`.

The private typed-slot codec, atomic-script parser, prepared-DML parameter-page
frontend, and plugin row-authority checkpoint serialization have no callers from
the supported runtime entry points. Their only constructors/callers are their
own adapters and tests. This change removes those complete private paths.
Public parameterized SQL, execute batches, the Arrow/value batch fast paths,
plugin actor checkpoints, live row-authority ownership, storage migrations and
historical format decoding remain supported.

The clustered registered-row-group encoder remains an independent fixture oracle
for projection, corruption and layout-equivalence tests. It and its input type
are now explicitly test-only; the unclustered runtime encoder is unchanged.
This gating receives no repository deletion credit. Tracked-state rebuild and
corruption oracles remain unchanged.

## Evidence and accounting

Source-wide searches (`rg --hidden`) covered `packages`, `plugins`, and `tooling`,
including hidden-directory integration fixtures and platform-gated code. Removed identifiers include `TypedSlotsRef`,
`TypedSlotLayout`, `PreparedDmlParameterBatch`, `PreparedDmlValueRef`, and
`execute_prepared_dml_batch`. The `LIXAUT02` codec was referenced only by its
own authority serialization methods and two codec tests; it was not wired into
checkpoint persistence or migration. Tests of live range/delta authority
membership and actor publication remain. The independent stale-publication
transaction test formerly using private prepared DML now exercises public cached
parameterized execution, preserving its conflict and winning-value assertions.

Using the audit's baseline test/support line classification and an exact line
diff: 1,810 production-classified lines and 702 test/support lines are deleted;
49 physical source lines are added (mostly explicit fixture gating and import
formatting). Charging every added line to production gives a conservative
**1,761 net production-line reduction**; total source reduction is **2,463 lines**.
Comments and whitespace are included. Removing orphan codec/frontend tests is
reported separately, rather than counted as production simplification.

No throughput, allocation or binary-size improvement is claimed. These paths
have no runtime callers, so a timing benchmark cannot meaningfully exercise
their removal. Validation targets compilation across configurations and
preservation of live engine behavior. Profiling of subsequent live-path changes
must use their own baseline/candidate workloads.

## Validation

- `cargo nextest run -p lix --features all-simulations,server-protocol --no-fail-fast --test-threads 8`: 3,702 passed, 69 existing skips; 63.450 seconds execution. The 27 removed tests cover the retired private codecs/parser/frontend.
- `cargo test -p lix --doc --features all-simulations,server-protocol`: 10 passed.
- Initial native `cargo check -p lix --all-features` passed; final full suite also compiles native library and test configurations after fixture import corrections.
- Independent subagent review: no correctness findings.
- Browser no-default-feature build queued for combined platform verification.

Logs: `/root/repos/lix-cut-a-tests.log`, `/root/repos/lix-cut-a-docs.log`.
No CI/CD runs requested; implementation commit carries `[skip ci]`.

## Final review — 2026-09-11

Independent review found no remaining production callers of the retired APIs and no weakened surviving public-path regression. The refreshed integration tree passed 3,711 engine tests with all-simulations and server-protocol enabled (69 existing skips), plus 10 doctests. An initial release-profile run hit an unchanged test that expects debug-only content-address verification; the normal test-profile rerun passed in full. Full PR CI is required before merging into #1729.

CI exposed one transitive orphan: `append_key_value_bytes` lost its last caller when typed slots were removed. Deleted the five-line private helper; adjacent active key encoders/decoders remain. Final all-features validation passed 3,740 engine tests (72 existing skips), 10 doctests, and strict Clippy across all lix targets. The first CI E2E run exceeded the unchanged 100 ms concurrent-commit p95 gate in its first wave; isolated repeats and a fresh full CI run validate that separately.
