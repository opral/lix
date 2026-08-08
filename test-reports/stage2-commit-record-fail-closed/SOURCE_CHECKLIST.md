# CommitRecord fail-closed source checklist

This is a test/report-only contract anchored to immutable Stage2 5C:

- Base: `1f742a382c755399b8a49ab536c4f6dc55fffdd8`
- Base tree: `860a047b98eaa38368a3d889497628e244c2e0ec`
- The successor must not include the parked current-state reader slice.
- Production source must be unchanged except the narrow
  `packages/lix/src/sql2/providers/change.rs` correction and its focused test.

Required source properties:

1. `CommitCatalog`/commit-graph enumeration remains the sole set of requested
   commit identities; no changelog scan, reverse index, cache, or fallback may
   fill a missing semantic record.
2. Every returned `CommitRecord` is present, decodable, and exactly matches
   its requested `CommitId`; missing, substituted, reordered, duplicated,
   wrong-domain, malformed, or cyclic authority fails closed.
3. Validation completes before any `LixChangeRow` is appended and before SQL
   `limit` truncation. A missing later record must still error with `limit=1`.
4. Exact lookup retains its existing explicit missing-record error.
5. The correction uses the existing retained read and transaction semantics;
   it adds no write, selector, epoch, receipt, retry, compatibility decoder,
   second authority, or reader mode.
6. The parked `live_state`/`tracked_state` reader changes are absent. No
   production paths outside the narrow SQL provider/test seam are changed.

Required focused cases:

- valid all-route scan preserves exact ordering/deduplication/result digest;
- missing one `CommitRecord` while its authenticated graph/catalog node stays
  live returns an error and no partial rows;
- substituted, reordered, duplicate, wrong-kind, and malformed records fail;
- exact lookup still fails closed when its indexed record is absent;
- `limit=1` does not hide a missing later record;
- cold reopen on RocksDB and SlateDB preserves the same result/error and makes
  no selector/epoch/write mutation.

## Bounded review commands

```text
git diff --check 1f742a382c755399b8a49ab536c4f6dc55fffdd8 HEAD
bash test-reports/stage2-commit-record-fail-closed/verify_commit_record_fail_closed.sh "$PWD"
cargo check -p lix --lib
cargo test -p lix --lib <focused-commit-record-test> --no-run
```

Stop on the first source/authority blocker. Runtime acceptance requires an
immutable successor and both adapter results; this package itself is the red
control and does not claim runtime acceptance.
