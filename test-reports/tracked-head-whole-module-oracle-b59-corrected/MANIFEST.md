# Corrected whole-module TrackedHead deletion oracle

This successor is based on immutable 422319cca0dad82525ab840d157aba5be49b09f
and remains anchored to exact b59. It adds a stateful authenticated
selector/root corruption model and an explicit reader-first deletion order.

TEST/REPORT-ONLY, directly anchored to exact b59:

```text
head   b59e1f11a51153e0a787a81f0f25bf104d150aaf
tree   700fd04d21bc40c05425c9fc9e10d65c9e1eda24
parent 713455a3557907ce705d06f720fcdc4486bddd4a
e166 ancestor e1666edd0b4d814a88d985086ecc5a477b5d32e6
713..b59 full-index SHA-256 4b2885709ba09034068b321be2fe5f27348d6681b1060133af1df0b7d76bb8d4
713..b59 stable patch ID 63dcb8dcecba8a25dea0ce8be19d26cdac264729
```

The corrected package closes the prior oracle blockers:

- source-gate output is path-normalized and reproducible;
- `session/merge/branch.rs` wrapper call sites are explicit and checked;
- old paths/reexports/factories are scanned in `packages/lix/src`,
  `packages/lix/tests`, and `packages/engine-benchmarks`;
- the obsolete consumer is compiled by direct `rustc` after a future
  `cargo build --lib`, not by an unwired Cargo test target; and
- fail-closed corruption/unsupported reads model one coherent read and zero
  plans, writes, commits, or selector publication.

The package is not wired into Cargo or production. It does not touch the
direct public-SQL entity, typed-PK, or columnar reader slice. Those paths are
an explicit separate lane and are forbidden in this oracle's candidate diff.

The ordered cut is:

1. Keep the direct SQL entity/typed-PK/columnar paths unchanged and fail the
   candidate if any of those four paths enters the diff.
2. Move transaction opening, reconciliation, stale-writer checks,
   savepoint/rollback, and one-publication lowering to one retained
   CoherentView; delete reader factories and callback wrappers only after all
   direct consumers are moved.
3. Move init, schema resolution, and SQL working-diff consumers to ForkTree
   state/catalog/selector reads with no TrackedHead or tracked-state fallback.
4. Move GC roots and current-generation observation to authenticated
   ForkTree roots and one epoch/progress fence; no old generation/cache owner.
5. Move production fixtures and engine benchmarks to public ForkTree behavior,
   then remove old test-only factory calls that would mask a production
   residue.
6. Delete live-state TrackedHead modules, reexports, marker spaces, factories,
   and old symbols before the first accepted compile. The direct obsolete
   consumer must fail for deleted API/space names.

The source gate is RED if any step is incomplete, if a wrapper/cache/fallback
remains, if an independent commit survives, or if a direct SQL
entity/typed-PK/columnar path is changed.

The exact b59 source-gate calibration exits `1` as the intentional RED
baseline. Its path-normalized output SHA-256 is
`f8e3c11af5fa5fe3c35973a727ad31bbfed9e27b4908b23d907ebbdc71d12867`; see
`SOURCE_CALIBRATION_B59.md` for the reproducible command.
