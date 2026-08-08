# W0 storage-boundary / b484 implementability map

This is a test/report-only package. It maps exact blocked lineage
`b484e20d845aee3f8137bfa3496f9b3cd0e8cd35` against the accepted W0 binding
`846981ead666eda465d358368f73cf93e2c9339f`. It contains no production edit,
Cargo metadata change, build, adapter runtime, PR operation, or main-branch
operation.

Read `IMPLEMENTABILITY_MAP.md` first, then run:

```bash
python3 test-reports/forktree-w0-storage-boundary-b484-map/verify_map.py \
  --repo "$PWD" \
  --w0 846981ead666eda465d358368f73cf93e2c9339f \
  --candidate b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
```

The verifier is source/provenance-only. Runtime and compiler commands in the
map are future acceptance commands and are explicitly UNRUN here.
