# W1b-2 stale reconciliation correction — test/report-only readiness

## Immutable boundary

This direct successor is based directly on blocked W1b-2 correction head
`7d71c5c381a2ab1eb049d955258d20291bc3a611` (tree
`c1d688ecff0be0e68d41436b54db21eadd45cd38`), whose parent is
`8b44e8cbd226e8820498e7c5c8e02d291c34abb8`, and remains TEST/REPORT-only.
It changes no `packages/lix/src` file, Cargo manifest, adapter, PR, or main
branch. The production semantic anchor is exact e1af:

```text
e1af471b9ab0f598dafa7c2ddec7867667c81740
tree bfa0d271a723da8250ab76ada16fda90926f1099
```

The exact five-RED e1af control in `EXPECTED_RED.txt` is preserved byte for
byte. The package does not claim production approval; the GREEN fixture only
proves that the verifier can distinguish a structurally corrected source
shape from the exact baseline.

## Candidate-parametric source verifier

`verify_source_contract.sh` has two modes:

```sh
# Exact production-anchor RED calibration.
timeout 1200s test-reports/w1b2-stale-reconciliation-e1af/verify_source_contract.sh \
  "$PWD" e1af471b9ab0f598dafa7c2ddec7867667c81740 \
  e1af471b9ab0f598dafa7c2ddec7867667c81740

# Hermetic structural GREEN calibration.
timeout 1200s test-reports/w1b2-stale-reconciliation-e1af/verify_source_contract.sh \
  --fixture test-reports/w1b2-stale-reconciliation-e1af/fixtures/green
```

Production mode resolves the target commit, checks exact e1af ancestry, and
compares every changed workspace path (including Cargo and report paths) to
the six-path production/package allowlist. Any production or Cargo escape is
RED. Each legacy RED is scoped to the named function body; an empty diff is
not treated as GREEN. The exact anchor still reports the five original
legacy-reader REDs.

Fixture mode scans an actual six-path source tree plus a separately compiled
runtime fixture; it is not a list of self-declared booleans. Its GREEN
predicates are argument-aware and function-scoped:

- `commit_prepared` constructs exactly one facade from the opening read and
  contains no nested `begin_read`, raw `read_store`, or facade clone.
- stale-disjoint, stale-plugin, cohort, and cohort-owner functions receive a
  `facade` argument and perform direct `facade.` operations; legacy reader and
  projected-batch calls are rejected inside those bodies.
- owner/registry authentication is present before the idempotency-key check
  and `Outcome::Idempotent` terminal return.
- plugin reconciliation sorts by write rank, validates the complete plan, and
  retains the pure stale classifier.
- the ForkTree facade exposes owner-proof, registry-proof, and semantic-row
  operations; all six allowed paths are scanned for legacy reader, raw read,
  fallback/retry, cache, and alternate-authority identifiers.
- `runtime.rs` binds `OpeningStorageRead` by pointer identity, rejects a
  swapped view, validates complete plan count/digest/order, authenticates all
  writes, walks every idempotency operation, and records exactly one atomic
  commit. It runs nine executable controls under `rustc -D warnings`.

This fixture is a verifier calibration, not a production candidate. A future
production head must pass the same structural predicates against its actual
source and retain the exact e1af RED control.

## Stateful model contract

`stale_reconciliation_oracle.rs` now binds:

- file owner, plugin key, generation, revision, and change ID;
- registry plugin/generation, revision, and change ID;
- exactly one opening read/view (`begin_reads=1`, current observations zero)
  with one reader instance and one view identity;
- same-owner stale overlap versus unrelated-owner success;
- exact idempotency fingerprint replay versus idempotency mismatch conflict,
  with owner/registry authentication before replay;
- missing, malformed, wrong-kind, file-identity, registry-identity,
  revision, and change-ID corruption;
- deterministic multi-write rank ordering and immutable current state, proving
  no partial reconciliation/publication;
- NULL, tombstone, absent, and JSON values.
- selector and commit identity substitution, cross-bound registry/plugin
  identity, mixed replay mismatch, duplicate operation, second commit, and
  second facade/read controls.

`negative_reconciliation_fixtures.rs` includes the model and adds explicit
second-read, forged revision/change-ID, idempotency mismatch, and reversed
input-order controls.

## Exact scope

Future production changes remain limited to the six paths in
`SOURCE_ALLOWLIST.md`. W1a/W1b-1, W1b-3/W1b-4, merge analysis,
undo/redo/checkpoint/history, working-diff, changelog, selectors/BranchRef,
writer/publication, GC, CAS/blob, storage adapters, W3-W5, compatibility,
fallback, retry, cache, second reader, and alternate authority remain outside
this package.

## Reproduced results

The exact baseline source verifier exits 1 with the committed five-RED output;
both streams hash `9afe7f764ef6bea1d914329e3ad0fded3bc59207d20696480dc76f457b22f7d0`.

The GREEN fixture exits 0 with:

```text
FIXTURE SCOPE PASS allowlist=6+runtime
PASS_FUNCTION=owner/registry authentication
PASS_FUNCTION=deterministic multi-write ordering
PASS_FUNCTION=write rank binding
PASS_FUNCTION=complete-plan validation
PASS_FUNCTION=owner proof operation
PASS_FUNCTION=registry proof operation
PASS_FUNCTION=semantic row operation
RESULT=GREEN candidate-parametric structural predicates pass
RUNTIME GREEN tests=9
```

Rust model commands, all bounded to 1200 seconds:

```sh
timeout 1200s rustfmt --edition 2024 --check \
  test-reports/w1b2-stale-reconciliation-e1af/stale_reconciliation_oracle.rs \
  test-reports/w1b2-stale-reconciliation-e1af/negative_reconciliation_fixtures.rs
timeout 1200s rustc --edition=2024 --test -D warnings \
  test-reports/w1b2-stale-reconciliation-e1af/stale_reconciliation_oracle.rs \
  -o /tmp/w1b2-v3-model
timeout 1200s /tmp/w1b2-v3-model --nocapture
timeout 1200s rustc --edition=2024 --test -D warnings \
  test-reports/w1b2-stale-reconciliation-e1af/negative_reconciliation_fixtures.rs \
  -o /tmp/w1b2-v3-negative
timeout 1200s /tmp/w1b2-v3-negative --nocapture
timeout 1200s rustc --edition=2024 --test -D warnings \
  test-reports/w1b2-stale-reconciliation-e1af/fixtures/green/runtime.rs \
  -o /tmp/w1b2-v3-green
timeout 1200s /tmp/w1b2-v3-green --nocapture
```

The stateful model has 11 passing tests, the negative runner has 15, and the
executable plan/commit fixture has 9. All compile with warnings denied. No
production Cargo, adapter, or runtime test is run by this package.
