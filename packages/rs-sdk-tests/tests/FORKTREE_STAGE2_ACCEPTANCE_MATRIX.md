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

## First-runnable readiness

The approved topology milestone
`origin/codex/forktree-stage2-milestone3e-topology-owned-reader` is frozen at
`af7899f41c489fe763ce1a64c5468083570979e2`, tree
`da097bd739b50629ea39b155d4fa9efc870654e0`, parent
`2e0cea1b91558179e6ed90847bc8b04b23de246f`. Its parent delta has canonical
full-index SHA-256
`942d05f6c92f89e6c32c3b706c82c4e506e498263b5798c92eb2af607a219587`
and patch ID `6ed511438fe08387ea40a5b6861f7db9f3544764`. This is an approved
topology-ownership frontier remains valid in the approved lineage.

The latest approved readiness base is the source-only BlobRef identity
successor `origin/codex/forktree-stage2-milestone4b-blob-manifest-identity`,
head `54e90dbf2bcf55c74de0be6ea4b217dc02cec89c`, tree
`5a8da9f8b11d83bf8216e266beaf4042cee84068`, parent `08f8dd5c...`.
Its focused full-index SHA-256 is
`c507282c79b8de8b9cdec3960157276efef2769e2866a895b4c0d015b77fa8f1`,
its `a12..head` SHA-256 is
`d5adb4a322dbf98a590d765c9ee2179a3a1e583211cb6a41c3fe2bf2cd786bae`,
and its stable patch ID is `242302af3d9db6ecb81f258570b1ed0ec99cde3c`.
Two independent source reviews approved this exact immutable object. It remains
non-runnable and receives no artifact application or build.

The next eligible candidate must be an explicitly compile-green immutable
writer/SPI head descending from `54e90dbf...`, unless the coordinator names a
replacement lineage. Compile-green ordinary commit lowering is not sufficient:
the same immutable head must have independent R2 atomicity approval, H2
deletion/residue approval, and zero independent ForkTree publication points
across transaction, upload, and GC families. The disposable
preflight/application procedure is frozen in
`FORKTREE_STAGE2_FIRST_RUNNABLE_RECIPE.md`.

Immutable writer Milestone 5A
`origin/codex/forktree-stage2-milestone5a-ordinary-atomic-writer` is held at
`5c4cae810324a34c0adbbb5a1a0be5fba5348054`, tree
`16741cdf6efce6bccdcf469406be1e1bce9b5f37`, parent `54e90dbf...`.
Its focused full-index SHA-256 is
`80f48db60e9205b2d3f242ee966f8d61d539a9dff65030d002a71c7f82bfaf2d`,
its `a12..head` SHA-256 is
`ad49aa816f4cece010f361f6fadf7f7b59f2003d6e905bd8f44341d613d08f56`,
and its stable patch ID is `2b57e2e9e23bd79343068a3b237ce20581c56526`.
It remains non-runnable at 201 errors/8 warnings and 170 residue findings.
Upload, checkpoint, history, multi-branch, and reachability publication families
remain independent, so this is a held identity frontier, not readiness
promotion. The latest approved readiness base remains `54e90dbf...`.

The superseded BlobRef predecessor
`origin/codex/forktree-stage2-milestone4-blobref-owned-view` is pinned at
`08f8dd5cf20842f79996fae9eb7b0924f074a084`, tree
`19c8706d6bc3d1dbe9217b4f8386b19c66f027a8`, parent `af7899f...`, but is
was **BLOCKED**. Its range path authenticates a manifest and intersecting chunks
while checking only owner size, so a same-size valid manifest can be
substituted under a different row `BlobId`. This object remains immutable
blocker evidence, not an executable candidate. The approved `54e90dbf...`
successor closes that exact blocker by authenticating an owner-private canonical
BlobId in the manifest and comparing it with the state-row identity before any
full or range payload chunk read.

Frozen artifact identities and independently reproduced canonical full-index
binary diff SHA-256 values are:

| Gate | Exact ref/head/tree | Comparator and canonical diff SHA-256 |
|---|---|---|
| SQL | `origin/agent/forktree-stage2-sql-dml-oracle` / `cb834007768205d5e9fb83919ca2915c77acca2d` / `8826a0a404a39bf4f932ad5140e0dfd1657f48fb` | `a12..cb834`: `be976527a15ec049be6465c3cf91020b3f58d0788792d7a5f0b1e00165a8b8ff` |
| Version control | `origin/agent/forktree-stage2-version-control-oracle` / `3cb6aa56804642efbe703f5e36bdc1788b51a4e7` / `911e0d6138b760a1c63e0e2c16b00e8f4b95c7dd` | `a12..3cb6`: `9348633179a5991dacf6bba85510e4f0cb1d391eeaae0042ab1956a0b08348b4` |
| Checkpoint | `origin/codex/checkpoint-stage2-acceptance-oracle` / `9bace2186664fc77877aa24abae6e516855313a1` / `e006aa4a5a3c6443e13d2c746fe81d9f97c30761` | `c3a58..9bace`: `7525ac6d2dd2b11e7b69709c341fe14a8bfc1b6bbfb525abe995a398e3ef8841`; complete `a12..9bace`: `34724dac23108ad3e65d2245ee926e702a88e778efc0459b98254bb09518d159` |
| 65-row delete | `origin/agent/forktree-bc823-oltp-delete-repro` / `9713361663df727af88dcf88aa05bd4b998c4149` / `a1b1ef1bed7f2a48b9f11a1a6288f325b3f64590` | `bc823..971336`: `6d633a6d61b33700f12b05b5f38486a16941eb556c40bba7a5e3c42004ebf065` |
| 1K point read | `origin/agent/stage2-point-read-oracle-a12` / `33117e1c128b038a5bbe486db126b3cb303c0f20` / `098afc641a8219390dd16e17b99598c928af0760` | `a12..33117`: `255127d4faf169f0127c7c254dc6df33c5be20959f4bfc17655d9bfe6a432b55`; patch ID `008ae479bb7d4a73bc125f18d1d5406340de5059`; source `8e9f6aa4e1e3d085c7b1bf6315ff0d8aa94912d5266133594a97961ee2d3e756`; source gate `af8897916e9d3dacbd54f3e48245766d163804b6a9004afa9905b8bb01eb5033`; frozen binary `ead3dae2ad74b349ef116b1e3ff9265a20a09f90f24dbdb2e32542c4cd5c8c1a`; external report `b86db402ec0bf9b25ca619564edb82a420fcdb34f68de2f972f6c774e863dbf5`; external manifest `cacbcee8f7b80a627f96dd8b7d6d55beef0fe2a2f7228f0290b488ae7717a888` (author-reported 3/3 verification; files are not mounted on this reviewer host) |
| Deletion/residue | `origin/codex/forktree-stage2-deletion-oracle-v2` / `1dbbf3d206540d36f5912eab8372a42819778b47` / `7fe3b3c83133344dff4025b558dbdd63bb1be21f` | `d00584e..1dbbf`: `0a6edac94dd03cd287e134bd873962bc841c0d2d5aebb9f92b1de45d5e359da5` |
| No-lease reference | `origin/codex/stage2-no-lease-read-view-boundary-ee402` / `89c73a24b97ce8dedee5e6c9a85e67c481b29090` / `6b90abcc440a3c13a6e95c641426629593536012` | complete `138b..89c`: `e93a1d78d01f3c7d29d4038627c691047fa8953c55bd61e77c6351e714114796` |
| GC/publication | `origin/codex/stage2-gc-publication-acceptance-oracle` / `0b4e5042b6a79b8be80dbfe4e4cdbff3b28d9a9c` / `0ac1ab8e74b85a92a8044cb4280adf8cf66ba387` | `cbe488..0b4e`: `bb6a70454484b9bba9e29929656a205a0706d1a0a2e60e495ea52fc19e567224` |
| Ordered OLAP | `origin/codex/forktree-stage2-olap-acceptance-oracle-a12` / `b9055810dff42c9eb2a096a83ab2207024dce1c6` / `231939d8a8d0f2a46803264184eea8171fa05f90` | `a12..b905` canonical committed-attribute stream: `545ff4e7c74b6bc19223d4977fd4dbba11914d46e3e5efadcae8407741e44b42` (42,863 bytes), patch ID `f88e0ea04bef0e93b0280c19f7c89d59c678223e`; alternate same-object text rendering: `0490631c2cc76da541e84c10c0e208c02d7a6626fa52a34b2c8aabaa1c9aaad9`, patch ID `d6bb816da4117a4b1781cda19450d6a32e2e0099`; report `005646c33cda54a363ab3c81f0b7ed0a5891e24de20639f05d8ca00a0f66009f`; SPI `c54c50ee301338ef3c04a3364eaff06e075a3f0212099cbc9a4290e8c5197193`; manifest `8e8b616f39bd37fc7eb3dda7aa19da7ce2011c16561c1687caa2cb6942d1a7c2`; external provenance report `e78821631888e8a8810df78e9bdffbe31c8a8124227c5ad0c3b549a6e60795a4` |
| Multimedia | `origin/codex/forktree-stage2-multimedia-oracle-a12` / `61fc367988190b3438672743331a81d83d450fae` / `1600e8ce54d9f52f6ee3546068362ae298d4d243` | `a12..61fc`: `65cda6ee906b6986bf70b636dfaadda5f8f89a2f8f4af407852687c474472660`; patch ID `30b6a50ac8730e382d2034b704082bab4fe41b7b`; final report `0dd241e1d6bd8fa32d84751972bd96fed666f2dafe742b447ca496f06aadc5bb`; package sums `ccc755a1cc70a28bc08145aeb61bec940f3db9b01b49c7e89931c9bd9218d0e8`; pre-normalization local report `6691dc30ce4f6eb0bd0a413aa060eb80614d4447bd05fd30c268a3e878044274`; predecessor `10bb5f41...` and object `66912a3d...` are excluded |

The no-lease package, 65-row delete reproduction, and point-read reference are
causal/reference discriminators. They do not become candidate acceptance merely
because their detached model code passes. The point-read reference uses its own
test-only authenticated encoding, so its frozen latency and resource medians are
not candidate A/B acceptance evidence. The first runnable candidate must execute
the delete and point-read sequences against its production owner, source-map the
actual public point/BlobRef transitive closure, and expose the sealed
candidate-facing GC/publication facade. This prevents a copied model from
qualifying unwired production.

The exact point-read build invocation is:

```sh
cd /root/repos/lix-stage2-point-read-oracle-a12 && env CARGO_TARGET_DIR=/root/repos/lix-stage2-point-read-oracle-a12/target RUST_TEST_THREADS=1 timeout 1200 cargo bench --profile bench -p lix_benchmarks --bench stage2_point_read_oracle --features 'storage-benches slatedb' --no-run
```

It produced
`target/release/deps/stage2_point_read_oracle-025c3b394ec8760c` with SHA-256
`ead3dae2ad74b349ef116b1e3ff9265a20a09f90f24dbdb2e32542c4cd5c8c1a`.
The frozen RocksDB and SlateDB invocations are the matrix commands with,
respectively,
`/tmp/stage2-point-read-oracle-rocks-a12-1k` and
`/tmp/stage2-point-read-oracle-slate-a12-1k` substituted for the path
placeholder. Those paths are consumed evidence and must not be reused or
deleted; candidate runs substitute only fresh nonexistent paths. The optional
source gate is:

```sh
cd /root/repos/lix-stage2-point-read-oracle-a12 && packages/engine-benchmarks/tests/stage2_point_read_source_gate.sh packages/engine-benchmarks/tests/stage2_point_read_source_gate_fixture/entry.rs packages/engine-benchmarks/tests/stage2_point_read_source_gate_fixture/helper.rs
```

## Minimal first-runnable sequence

1. Verify the candidate SHA/tree and apply only exact test/report artifacts in
   a disposable prospective tree. Run `cargo fmt --all -- --check`,
   `git diff --check`, the residue scanner, semantic delegation audit, CLI
   routing audit, cursor compile probes, and the 21-path/39-symbol #1258 map.
   Any nonzero legacy authority stops all runtime work.
2. Run the production-owner 65-row, batch-1 delete on RocksDB. Only after it
   cold-reopens empty, run SlateDB. Retain 64-row/batch-1 and
   65-row/batch-100 controls. A detached benchmark-model pass is insufficient.
3. Bind the actual public point plus BlobRef seam and enumerate every transitive
   helper. Run the 1K point-read gate on RocksDB and then SlateDB with at least
   five samples and setup excluded. Each sample must perform exactly 1,000
   `begin_read` calls, 6,000 authenticated gets, and 3,922,880 logical read
   bytes; perform zero scans, writes, or commits; leave disk unchanged; return
   digest `0f28fc4645fef236d6332733a943b5b43ab35034c2b1d365d928bda0718295a1`
   hot and cold; and fail closed for malformed selector/catalog and kind/ID
   substitution. Require a meaningful paired improvement greater than 10% on
   both adapters and no critical regression greater than 5%. Any failure stops
   before SQL and before 10K/50K scaling.
4. Run the 18-statement SQL RocksDB smoke, then SlateDB. Both public-result and
   cold-state digests must equal the frozen current-layout values.
5. Run the three-row checkpoint/merge RocksDB test, then SlateDB. This single
   focused gate includes 64 rotations, true conflict, missing-parent,
   undo/redo, cold reopen, and final release.
6. Run the no-lease discriminator and then the sealed GC/publication RocksDB
   gate. Only after both are green, run their SlateDB counterparts. Exact
   one-view transport, progress rotation, persisted bounded packs, upload and
   final-reference checks are mandatory.
7. Build the sealed OLAP oracle once, then run its ordered gate into a new empty
   evidence directory: 10K RocksDB plus both corruption cells, 10K SlateDB plus
   both corruption cells, 50K RocksDB then SlateDB, and finally 500K RocksDB
   then SlateDB. Every process cell is capped at 20 minutes. Query digests and
   row counts must match hot and cold; query writes remain zero; range and
   projection improve by more than 10%; all critical regressions remain within
   5%. SlateDB's six-versus-five range reads or twelve-versus-ten join reads
   hard-block broad closeout unless a checked-in manager waiver binds the exact
   candidate, scale, query, values, report hash, and at least 20% aggregate
   improvement. No waiver can cover any other metric.
8. Run broader version-control RocksDB/SlateDB. With H4's exact normalized
   multimedia transport bound, run 64 MiB/1% image RocksDB first, then SlateDB, then
   the remaining 64 MiB shape and only then 512 MiB/10% archive/video. No
   broader workspace or performance matrix precedes these focused gates.

## Blocker routing

- Source residue, alternate authority, dual writer, compatibility reader, or
  missing facade delegation: stop and return to R5's compiler-deletion wave.
- `finish_root`/path-copy 65-row failure: R5's tree owner; do not enter SQL.
- Point-read source-map/SPI absence, count/digest/cold/corruption mismatch:
  R5's read owner. A paired improvement of 10% or less, or a critical
  regression above 5%, freezes a no-scale verdict and stops before SQL.
- SQL-only mismatch: SQL transaction/binder owner plus R5. Never add a second
  layout selector or provider hook.
- Checkpoint ancestry/base/reclamation mismatch: checkpoint graph owner; queue
  internals remain unreadable to public tests.
- One-view, cursor poison, epoch/progress CAS, mark/queue, upload, or final
  release mismatch: GC/publication owner; no lease or raw-space escape.
- OLAP SPI/owner identity, digest/reopen/corruption, coherent read, batching,
  projection order, writes, latency, resource, or physical-read mismatch: R5's
  OLAP reader owner. Stop at the first failing scale. The SlateDB object-count
  residual requires the exact narrow manager waiver or remains a hard blocker.
- Multimedia transport/provenance mismatch: H4. Runtime/accounting facade
  absence routes to R5; a 64 MiB failure stops 512 MiB qualification.
- Version-control-only mismatch: branch/history/merge owner, after isolating
  whether the failure is actually the already-gated checkpoint or GC seam.

The immutable candidate must remain unchanged while these artifacts are
applied. Passing tests never authorizes retaining their cfg-only SPI in normal
production builds, and no test facade may expose `StorageSpace`, raw selector
bytes, object IDs, queue keys, codecs, or direct mutation callbacks.
