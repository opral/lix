# File-history identity/tombstone correction oracle

This is a test/report-only oracle anchored at immutable production commit
`b484e20d845aee3f8137bfa3496f9b3cd0e8cd35` (tree
`4477c83b246bddac09cd972564bd4ccd67f90f7b`). It does not import, compile, or
execute Lix production code.

Run from the repository root:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 test-report/forktree-stage2-fd2-file-history-correction-oracle/oracle.py --mode all

# Full frozen package gate; b484's production fmt check is expected RED.
sh test-report/forktree-stage2-fd2-file-history-correction-oracle/verify.sh
```

The model cases are executable identity and mutation negatives. They require:

- historical file descriptors to bind `file_id == entity_pk == snapshot.id`;
- historical directory descriptors to bind `file_id == NULL == directory row scope`;
- exact BlobRef cardinality, file identity, BlobId/payload hash, and size;
- deleted file, directory, and plugin-owner rows with payload to fail before projection;
- payload-less tombstones to remain logical absence;
- explicit live empty state to remain distinct from tombstone state.

The source gate is deliberately function-scoped. On b484 it must report exactly
the five known RED findings in the report: the two historical descriptor key
bindings and the three observed tombstone-payload guards. It also checks that
the corrected working-diff identity/tombstone pattern is present and that the
two changed production paths contain no raw `begin_read`, legacy reader, or
`owner.schema_keys()` fallback.

This package intentionally does not claim runtime or compiler-green status.
