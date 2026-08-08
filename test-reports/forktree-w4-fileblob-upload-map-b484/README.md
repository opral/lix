# W4 file/blob/upload map for exact b484

TEST/REPORT-only source evidence. This package maps the blocked b484 lineage
to the locally frozen W4/file-blob contracts and records the smallest
dependency-ordered publication cut. It contains no production changes and
does not run Cargo, adapters, benchmarks, or runtime tests.

Run the static verifier only:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 \
  test-reports/forktree-w4-fileblob-upload-map-b484/verify_map.py \
  --repo "$PWD"
```
