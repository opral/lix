# Transaction working-diff/generation migration oracle

Test/report-only package anchored at exact source
`413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`.

Required whole-module prerequisite:

- ref `origin/codex/tracked-head-whole-module-oracle-413e`
- gate head `0b9ab47f7ac7da600b215d0b8aa2ab04db6bd36d`
- gate tree `029a89195741920a7ff50a6a79bdefe0ec35f927`

The package is not wired into Cargo or production. It contains a pure
transaction model, path-aware source verifier, and exact future adapter order.
No production edit, build matrix, current-main benchmark, or adapter runtime is
part of this package.
