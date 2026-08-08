# File-history identity/tombstone correction oracle v3

This is a test/report-only v3 successor to the immutable v2 oracle commit
`1b8134f7bc02802c203853a3f71dbbee639b6932`, anchored at immutable production commit
`b484e20d845aee3f8137bfa3496f9b3cd0e8cd35` (tree
`4477c83b246bddac09cd972564bd4ccd67f90f7b`). It does not import, compile, or
execute Lix production code.

The retained blocker bindings are H3 report
`fabefc265ec09b225eaac2ff4daeefd6baef35971497b077547c6d60dccaf39c` and R1
whole-closure report
`83871d2d7c1e8faa0231f77aae75a3f2811debfaeaebd5fb6c18aa83d74d5e96`.

Run from the repository root:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 test-report/forktree-stage2-fd2-file-history-correction-oracle/oracle.py --mode all

# Full frozen package gate; b484's production fmt check is expected RED.
sh test-report/forktree-stage2-fd2-file-history-correction-oracle/verify.sh
```

The model retains all 25 v2 cases and adds three executable discriminators,
for 28 cases total. They require:

- historical file descriptors to bind `file_id == entity_pk == snapshot.id`;
- historical directory descriptors to bind `file_id == NULL == directory row scope`;
- exact BlobRef cardinality, file identity, BlobId/payload hash, and size;
- missing, NULL, and tombstoned selected BlobRefs to fail closed;
- deleted file, directory, and plugin-owner rows with payload to fail before projection;
- payload-less tombstones to remain logical absence;
- explicit live empty state to remain distinct from tombstone state.
- a complete composite EntityPk binding (selecting only its first component
  fails);
- conflicting authenticated source-change IDs to fail before grouping or
  deduplication;
- metadata-only and data projections to expose an authenticated empty BlobRef
  as `live-empty`, while missing/NULL/tombstoned references fail and a
  payload-less tombstone is `absent`.

The source gate is deliberately function-scoped. On b484 it must report exactly
nine RED findings: the two historical descriptor key bindings, the three
observed tombstone-payload guards, the permissive selected-BlobRef/projection
fallback, first-component composite EntityPk selection, silent conflicting
source-change deduplication, and the zero-BlobRef projection conflation. It
also checks that
the corrected working-diff identity/tombstone pattern is present and that the
two changed production paths contain no raw `begin_read`, legacy reader, or
`owner.schema_keys()` fallback.

The six v2 RED IDs remain unchanged as a subset of this nine-element b484
calibration. The package does not claim that any production defect is fixed.
`ADDITIVE_CLOSURE_BINDING.md` binds the independent immutable all-nine oracle
at `9cd14f684205f21f76f0504871fd00ed2d5eea07`; its exact replay is recorded
there with calibration stdout SHA-256
`6f74dbaa54574e6e94dec6f758c1a6d2047225d7f7bfe31e1f50582f2426e832`.
The current immutable `--mode all` stdout is 2,284 bytes with SHA-256
`4b933f0da4a1fd0c62e70d811976bd5144e4772a34e2445b22743b7f0ece5f98`; the
source-only calibration stdout is 850 bytes with SHA-256
`767e567390fe54f43fb503393ef412989ec40254062ab88b3c04f63775989ec2`.

`verify.sh` requires its direct parent to be the exact v2 package commit,
checks that the successor diff contains only this report directory, verifies
every package checksum, and records b484's expected RED format result. This
package intentionally does not claim runtime or compiler-green status.
