# Corrected whole-module TrackedHead deletion oracle

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

The exact b59 source-gate calibration exits `1` as the intentional RED
baseline. Its path-normalized output SHA-256 is
`f8e3c11af5fa5fe3c35973a727ad31bbfed9e27b4908b23d907ebbdc71d12867`; see
`SOURCE_CALIBRATION_B59.md` for the reproducible command.
