# LocalFilesystem hard-cut executable oracle

## Provenance and scope

- Baseline: `c8c7899912a661b7bbd802eaced3c076f52876e5`
- Baseline tree: `e857186ae74c0e537ec3eff04f284590b86c636e`
- Scope: tests and this report only. No production source is changed.
- Contract: positional path-only Rust and JavaScript construction, one synchronization owner, automatic bidirectional synchronization, physical `.lix` exclusion, terminal close drain/join, and predecessor API unnameability.

## Exact c8 calibration

The Rust oracle built warnings-denied in 9m02s. Its exact executable SHA-256 was
`a6865a2b0dac38a82eb6a600ce380479a1805ab5caf37775ed35ca3b51570287`.

- RED as intended: `positional_open_imports_workspace_but_never_physical_lix_metadata`.
  Exact failure: physical `.lix/oracle-sentinel.bin` was imported as the user row
  `/.lix/oracle-sentinel.bin`.
- GREEN: background disk create/modify/delete/rename/nested/binary synchronization and
  two-debounce self-loop stability, 3.31s.
- GREEN: acknowledged Lix-to-disk write, terminal close/join ownership check, and exact
  cold reopen, 0.05s.
- GREEN: the Rust positional `LocalFilesystem::open(path)` compile probe.
- RED as intended: the deleted-Rust-API probe still compiles on c8, proving
  `LocalFilesystemOpenOptions`, `open_with_options`,
  `open_with_options_and_wasm_runtime`, `import_paths`, and
  `sync_disk_to_lix` remain nameable on the baseline.
- RED source gate: 17 predecessor findings. Canonical finding count/hash:
  `17` /
  `6211c906a661a772a550dc179508eb9d1df5b77ef18646e01ba7f895b9467ab0`.
  Ownership counts were watcher/supervisor/worker/JS-watcher = `1/1/1/0`.
- The c8 JavaScript hard-cut suite is intentionally not run: its path-only constructor
  and negative TypeScript probes are compile-red against the old options/manual-sync
  surface. Candidate qualification must typecheck and run it after the API cut.

No oracle uses an explicit flush or manual synchronization method. Eventual disk-to-Lix
checks poll public state; Lix-to-disk acknowledgement and close are tested independently.

## Candidate acceptance

Run `run_acceptance.sh <candidate-root> <cargo-target-dir>`. Acceptance requires:

1. source gate zero findings and owner counts `1/1/1/0`;
2. positive Rust and TypeScript path-only probes compile;
3. every negative Rust/TypeScript/native predecessor probe fails to name the removed API;
4. all three Rust lifecycle tests pass;
5. focused JavaScript lifecycle test passes;
6. fmt and diff checks pass.

The runtime discriminator covers create, modify, delete, rename, nested and binary files,
physical `.lix` exclusion, no duplicate/self-loop publication, accepted FIFO drain before
close returns, sole worker join, and exact cold reopen.

## Source hashes

- Rust lifecycle: `e1d4bd354874ef43700343ab99df3e741e88a99fef9db92afe664da45c6a5f8d`
- Rust positive probe: `cab66d8a2afc64d4f652fbdbb1bbad0921e683a25a81b023d08aaad29038ad67`
- Rust negative probe: `e1f70ad3dd4c5cedbe778e89db0554336ecbd3c03a33190f784b0247b432b971`
- JavaScript lifecycle: `083343553326b4d7f5bd1eecb7c363d9cf83e714db63b0b92bf00d50dd0c4b96`
- TypeScript negative probe: `d7330bb22dcd369f1d752a94c3b34c73f7855fdf25fae542874c971848d7b89b`
- Source gate: `6dc2e12fc9b683bcf792c49a8221ea608a7215f71794b2fc6e7de9acdf0581d8`
- Runner: `5c7600d2fe7e68526a7d40f0ecf00341bf1d854aac5cbf91ccea48772dbe8362`
