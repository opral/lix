# ForkTree Stage 2 authoritative acceptance matrix

Status: test/report-only integration package. It changes no production source,
persisted format, PR, or runnable candidate. Every command is for a disposable
exact-head qualification worktree with an isolated Cargo target.

## Provenance and interpretation

The matrix is rooted at exact current-layout comparator
`a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`, tree
`9a705d36392e88d8f5f363b2b23d373deec3321d`, and accepted unwired Stage 1
`138b55e1de90806c380ad27b2b349f4c66a1387f`, tree
`26a3e6ead4d690bf1fe2ebca1e2da7d597256b84`.

`FORKTREE_STAGE2_ACCEPTANCE_MATRIX.tsv` is the machine-readable authority.
Commands are shell fragments with `<candidate>`, `<target>`, and `<fresh-db>`
placeholders. Each command is independently capped at 20 minutes. A timeout
without test execution is a host compile boundary, never a candidate pass.

The refs have different historical bases and must not be merged wholesale.
For qualification, materialize only each row's named test/report artifacts in
a disposable worktree rooted at the immutable candidate, verify their SHA-256
values, and record the resulting prospective tree. Production blobs must stay
identical to the candidate. If an artifact needs a candidate-facing facade,
the facade must already be present in the candidate; the reviewer must not
adapt the test, widen visibility, or restore a legacy API to make it compile.

Frozen artifact identities and independently reproduced canonical full-index
binary diff SHA-256 values are:

| Gate | Exact ref/head/tree | Comparator and canonical diff SHA-256 |
|---|---|---|
| SQL | `origin/agent/forktree-stage2-sql-dml-oracle` / `cb834007768205d5e9fb83919ca2915c77acca2d` / `8826a0a404a39bf4f932ad5140e0dfd1657f48fb` | `a12..cb834`: `be976527a15ec049be6465c3cf91020b3f58d0788792d7a5f0b1e00165a8b8ff` |
| Version control | `origin/agent/forktree-stage2-version-control-oracle` / `3cb6aa56804642efbe703f5e36bdc1788b51a4e7` / `911e0d6138b760a1c63e0e2c16b00e8f4b95c7dd` | `a12..3cb6`: `9348633179a5991dacf6bba85510e4f0cb1d391eeaae0042ab1956a0b08348b4` |
| Checkpoint | `origin/codex/checkpoint-stage2-acceptance-oracle` / `9bace2186664fc77877aa24abae6e516855313a1` / `e006aa4a5a3c6443e13d2c746fe81d9f97c30761` | `c3a58..9bace`: `7525ac6d2dd2b11e7b69709c341fe14a8bfc1b6bbfb525abe995a398e3ef8841`; complete `a12..9bace`: `34724dac23108ad3e65d2245ee926e702a88e778efc0459b98254bb09518d159` |
| 65-row delete | `origin/agent/forktree-bc823-oltp-delete-repro` / `9713361663df727af88dcf88aa05bd4b998c4149` / `a1b1ef1bed7f2a48b9f11a1a6288f325b3f64590` | `bc823..971336`: `6d633a6d61b33700f12b05b5f38486a16941eb556c40bba7a5e3c42004ebf065` |
| Deletion/residue | `origin/codex/forktree-stage2-deletion-oracle-v2` / `1dbbf3d206540d36f5912eab8372a42819778b47` / `7fe3b3c83133344dff4025b558dbdd63bb1be21f` | `d00584e..1dbbf`: `0a6edac94dd03cd287e134bd873962bc841c0d2d5aebb9f92b1de45d5e359da5` |
| No-lease reference | `origin/codex/stage2-no-lease-read-view-boundary-ee402` / `89c73a24b97ce8dedee5e6c9a85e67c481b29090` / `6b90abcc440a3c13a6e95c641426629593536012` | complete `138b..89c`: `e93a1d78d01f3c7d29d4038627c691047fa8953c55bd61e77c6351e714114796` |
| GC/publication | `origin/codex/stage2-gc-publication-acceptance-oracle` / `0b4e5042b6a79b8be80dbfe4e4cdbff3b28d9a9c` / `0ac1ab8e74b85a92a8044cb4280adf8cf66ba387` | `cbe488..0b4e`: `bb6a70454484b9bba9e29929656a205a0706d1a0a2e60e495ea52fc19e567224` |
| Multimedia | `origin/codex/forktree-stage2-multimedia-oracle-a12` / `61fc367988190b3438672743331a81d83d450fae` / `1600e8ce54d9f52f6ee3546068362ae298d4d243` | `a12..61fc`: `65cda6ee906b6986bf70b636dfaadda5f8f89a2f8f4af407852687c474472660`; patch ID `30b6a50ac8730e382d2034b704082bab4fe41b7b`; final report `0dd241e1d6bd8fa32d84751972bd96fed666f2dafe742b447ca496f06aadc5bb`; package sums `ccc755a1cc70a28bc08145aeb61bec940f3db9b01b49c7e89931c9bd9218d0e8`; pre-normalization local report `6691dc30ce4f6eb0bd0a413aa060eb80614d4447bd05fd30c268a3e878044274`; predecessor `10bb5f41...` and object `66912a3d...` are excluded |

The no-lease package and 65-row delete reproduction are causal/reference
discriminators. They do not become candidate acceptance merely because their
detached model code passes. The first runnable candidate must execute the
delete sequence against its production owner, and must expose the sealed
candidate-facing GC/publication facade. This prevents a copied model from
qualifying unwired production.

## Minimal first-runnable sequence

1. Verify the candidate SHA/tree and apply only exact test/report artifacts in
   a disposable prospective tree. Run `cargo fmt --all -- --check`,
   `git diff --check`, the residue scanner, semantic delegation audit, CLI
   routing audit, cursor compile probes, and the 21-path/39-symbol #1258 map.
   Any nonzero legacy authority stops all runtime work.
2. Run the production-owner 65-row, batch-1 delete on RocksDB. Only after it
   cold-reopens empty, run SlateDB. Retain 64-row/batch-1 and
   65-row/batch-100 controls. A detached benchmark-model pass is insufficient.
3. Run the 18-statement SQL RocksDB smoke, then SlateDB. Both public-result and
   cold-state digests must equal the frozen current-layout values.
4. Run the three-row checkpoint/merge RocksDB test, then SlateDB. This single
   focused gate includes 64 rotations, true conflict, missing-parent,
   undo/redo, cold reopen, and final release.
5. Run the no-lease discriminator and then the sealed GC/publication RocksDB
   gate. Only after both are green, run their SlateDB counterparts. Exact
   one-view transport, progress rotation, persisted bounded packs, upload and
   final-reference checks are mandatory.
6. Run broader version-control RocksDB/SlateDB. With H4's exact normalized
   multimedia transport bound, run 64 MiB/1% image RocksDB first, then SlateDB, then
   the remaining 64 MiB shape and only then 512 MiB/10% archive/video. No
   broader workspace or performance matrix precedes these focused gates.

## Blocker routing

- Source residue, alternate authority, dual writer, compatibility reader, or
  missing facade delegation: stop and return to R5's compiler-deletion wave.
- `finish_root`/path-copy 65-row failure: R5's tree owner; do not enter SQL.
- SQL-only mismatch: SQL transaction/binder owner plus R5. Never add a second
  layout selector or provider hook.
- Checkpoint ancestry/base/reclamation mismatch: checkpoint graph owner; queue
  internals remain unreadable to public tests.
- One-view, cursor poison, epoch/progress CAS, mark/queue, upload, or final
  release mismatch: GC/publication owner; no lease or raw-space escape.
- Multimedia transport/provenance mismatch: H4. Runtime/accounting facade
  absence routes to R5; a 64 MiB failure stops 512 MiB qualification.
- Version-control-only mismatch: branch/history/merge owner, after isolating
  whether the failure is actually the already-gated checkpoint or GC seam.

The immutable candidate must remain unchanged while these artifacts are
applied. Passing tests never authorizes retaining their cfg-only SPI in normal
production builds, and no test facade may expose `StorageSpace`, raw selector
bytes, object IDs, queue keys, codecs, or direct mutation callbacks.
