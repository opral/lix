# ForkTree Stage 2 authoritative acceptance matrix

Status: test/report-only integration package. It changes no production source,
persisted format, PR, or runnable candidate. Every command is for a disposable
exact-head qualification worktree with an isolated Cargo target.

## Provenance and interpretation

The current qualification target is exact main
`822c204ce0670969ca71045bc74f9ca25fde8093`, tree
`fac3f2b713683be17c34515062dd72edc8feed95`. The prior immutable
non-runnable ForkTree frontier is `34f2dacad4a0126a58d015f27ed75c2142547dd5`,
tree `2e68fb8becd480f97364dbc2cc70416e66e765c1`, parent
`a1cf8f7fd55ac21ef7e5bfe7f385c49d99140737`, with parent-to-head full-index
SHA-256 `6fadece2bdb9cbd3d36d52ba738834ee6172cae5031d4fac4dd87099a77661ac`.
The latest held catalog-boundary frontier is
`7c9b1060bc396dfa54efcc6c888e37894a7cfb04`, tree
`ee96c5b64912b8fa8bb15fb7c31916244a255523`, parent
`34f2dacad4a0126a58d015f27ed75c2142547dd5`, with parent-to-head full-index
SHA-256 `109ae9bc8eb4e24487bde7c50da28b020a23da4f7ebcf744d25bdc5787f3d779`.
Its stable patch ID is `1e9b50c12a8db7a3db9024e28ff2d06b5a0dbb0d` and the
source report SHA-256 is
`8fe40b5fe63895a132293151e126394161552bf95a4106e02c01bb068c2b17ff`.
Both are non-runnable; the latest held frontier remains blocked until an
immutable descendant is explicitly compile-green and independently clears R2
atomicity and H2 residue approval.

The seven-stage landing overlay records the exact blocked topology frontier
`1f742a382c755399b8a49ab536c4f6dc55fffdd8`, tree
`860a047b98eaa38368a3d889497628e244c2e0ec`, parent
`7c9b1060bc396dfa54efcc6c888e37894a7cfb04`, with parent-to-head full-index
SHA-256 `18a7df6d37fce9809b2214f5b1530204b1a2dd4cf19760aa876ec7856249dbc7`.
Its exact remote ref is
`origin/codex/forktree-stage2-milestone5c-topology-semantic-bridge`. The
disposable overlay and machine-readable seven-stage order are
`FORKTREE_STAGE2_SEVEN_STAGE_OVERLAY.md`,
`FORKTREE_STAGE2_SEVEN_STAGE_OVERLAY.tsv`,
`R1_CHECKPOINT_GC_BINDING.tsv`, `R5_CORRECTED_FRONTIER_BINDING.tsv`,
`W5_R7_GC_REACHABILITY_CONTRACT.tsv`, `READER_FRONTIER_BINDING.tsv`, and
`forktree_stage2_seven_stage_overlay.sh`. They are dormant and test/report
only: no runtime cell may start until a candidate has a reviewed correction
frontier replacing blocked `1f742...`, is explicitly compile-green, and the R1
binding contains the immutable checkpoint/GC ref/head/tree/report identity.

The historical exact current-layout comparator for the frozen test/report
artifacts is `a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`, tree
`9a705d36392e88d8f5f363b2b23d373deec3321d`; it is a control identity, not a
replacement for current main. Accepted unwired Stage 1 remains
`138b55e1de90806c380ad27b2b349f4c66a1387f`, tree
`26a3e6ead4d690bf1fe2ebca1e2da7d597256b84`.

`FORKTREE_STAGE2_ACCEPTANCE_MATRIX.tsv` is the machine-readable authority.
Commands are shell fragments with `<candidate>`, `<target>`, and `<fresh-db>`
placeholders. Each command is independently capped at 20 minutes. A timeout
without test execution is a host compile boundary, never a candidate pass.
The latest read-only verifier pass is frozen with stdout SHA-256
`96e6e33bd8b550ad0c0a1bb758e3281cd0455b468ec78d4868e5e27c24712e5e` and empty
stderr SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

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

The latest reader-only readiness base remains the source-only BlobRef identity
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

The latest source/static-approved readiness base is the non-runnable 5A2
successor `origin/codex/forktree-stage2-milestone5a2-runtime-intent`, head
`a1cf8f7fd55ac21ef7e5bfe7f385c49d99140737`, tree
`d8326da2b1d38bd51b8ac7229d00684a6865bce2`, parent `5c4cae81...`.
Its focused full-index SHA-256 is
`a81dd0af7154b86f663ca786bcd0470c6cd4af01a1fada0eea3ac6696a709e8c`,
its `a12..head` SHA-256 is
`8192c4f2409f11fedbab14e3553f98fa7f09887afd7745ad351aa72e5c87b87c`,
and its patch ID is `f903241b622507aa637e09fa2362b976def580e2`.
The exact two changed production blobs are transaction `commit.rs`
`cfc40fa496ddcdc9ea920b3b6c17d19978e1ea0c` and `context.rs`
`3a7c27fa922cda832d2bf89f5942b0981e444126`. Static review approved complete
intent classification before view/plan creation, unsupported-cohort zero-write
rejection, true empty no-op behavior, and the advanced runtime row in the sole
ForkTree plan/prepare/commit. This is source/static readiness only: it remains
compile-red/non-runnable and has no runtime, broad, 5B, merge, or artifact result.

The next eligible candidate must be an explicitly compile-green immutable
writer/SPI head descending from `a1cf8f7f...`, unless the coordinator names a
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
It remains non-runnable at 201 errors/8 warnings and is
**BLOCKED**: the ordinary lowerer discards deterministic `runtime_functions`,
so runtime sequence writes and preconditions do not enter the sole transaction
batch. Its `tracked_rows.is_empty()` branch can also discard ref-only or
selected-history intent while still publishing untracked/global-epoch work, and
true empty commits error instead of retaining no-op behavior. Upload,
checkpoint, history, multi-branch, and reachability publication families also
remain independent. This is blocked identity evidence, not readiness promotion.
This rejected object remains blocker evidence; it is superseded for readiness
by the narrowly corrected `a1cf8f7f...` source/static milestone.

The 5A2 scanner replay is bound to exact residue ref
`1dbbf3d206540d36f5912eab8372a42819778b47`, source SHA-256
`f71e91fcbccbb7d6df676a95e9d747725856b77f7e3177ec42f12ca8b28736cc`,
and owner-frozen binary SHA-256
`40d02e20dd2cbd1334a8c0eddccce9c16e012200707d8488e136415a89483066`.
Baseline and candidate each contain 166 identical semantic finding records.
Their sorted unique normalized record set hashes to
`86010e7dad821c8cc89858dcbf1a55cb9a234ea2eeab6d43ef08247e4ede61aa`.
The previously conflicting raw hashes are presentation variants: baseline
stdout, including `finding_count=166` and its final newline, hashes to
`6f4013daca11867c9e07fab14b741c1650515eed473f87c12377e3421db8c42b`;
audit mode writes that same stdout plus `first runnable candidate retains
forbidden residue` to stderr, and combining streams with `2>&1` hashes to
`3891a48613e5d6ebd3d0ab2780aed13c6dd0236f1c2ff343320dd73fb2158a0d`.
Removing that terminal stderr line makes the streams byte-identical. The
independent reconciliation report SHA-256 is
`1f90f530b02743ffda50b56646499759119e69590a11f0b3eabe4a71b9b3a251`.
Acceptance
binds scanner source/binary identity and the normalized semantic set, not a
cwd/path/redirection-dependent raw presentation.

The immutable transport ref for the P0+W1a source-gate package is
`origin/codex/forktree-stage2-p0-w1a-acceptance-a1cf`, head
`d03d03a4925b51c7d43801bf256b9c9b37f53f67`, tree
`d5016a33f069c5151944eff1f11eca650d8fd872`, parent `a1cf8f7f...`, and
parent-to-head full-index SHA-256
`e568ad37ff4531958780b6530124c91dccb9df4f572c0c41d4e75918605447d9`.
Its stable patch ID is `ef8f5f2bac0a7bd5f8d10f356fddc706e30803c4`. The exact
four-file package manifest is SHA-256
`73cd9f5d4de76b618d3f483e957755271f81cfb503d48a63c4d4cdddbbfc2dc6`.
The contract, case table, verifier, freeze report, and transport report
respectively hash to
`cfd25a6064aa1c5fd3ad06558c43f79c2169ac88f7b80bd9dab05a90f739d249`,
`77af0924a86cf023a2924075507545b52035739e8c5bfc33accc080e8f4a9b17`,
`35dfbedc0373f5292d96d9e0ab2feafbc11b3f35618adcaa2d5c921514304550`,
`77a0762582364b3c77ca78720e8feca9c2b44c3cbdf40b4a91037ca704064e8e`, and
`db5a29e0bac9f09da6defa6af4be0b3c3a79d06890f386392e124169d1fb1a8e`.
The five transport-tree files are independently checked by the verifier. The
package remains test/report-only and is never applied to production. Every first-runnable
candidate must pass it before residue or runtime artifact application: P0
removes direct `PreparedPublication::commit`/independent write entry points;
W1a lowers ordered single-branch history and selected members through exactly
one read, plan, prepare, and commit, with unsupported families failing before
any plan or epoch rotation.

## External target-discovery package

The updated goal includes OLTP, version control, parsed files, large
multimedia, both RocksDB and SlateDB, corruption/recovery/GC, and external
target-discovery artifacts. The immutable discovery package is
`origin/codex/forktree-post-stage2-acceptance-manifest`, head
`1ad4c879b1d8339bca7fdc414fdee36305ce9a69`, tree
`bc9728cfa0b761d98c0639479c80c65cbad5e4a9`, parent
`a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`, and exact `a12..head`
full-index binary SHA-256
`1bf97fb4037add7d5ecf3d046359e6ab92188f3740532c155e5bd885a0fa841d`.
Its stable patch ID is `7389880b6907b20c3d97971883b352105e15a816`, and its
canonical diff stream is 28,494 bytes. The three test-only files hash to:

- `FORKTREE_POST_STAGE2_ACCEPTANCE_MANIFEST.md`:
  `456dd164a7a1742c917ad69acc806c3eebf1205125c44dccb5561c9def778a06`;
- `forktree_post_stage2_acceptance/SOURCE_REFS.tsv`:
  `349ca7019d7e4db03c57ee0f2e8123f7e1f85666a945221ad5a8997b33c9b39d`;
- `forktree_post_stage2_acceptance/run.sh`:
  `8b1a6ef66e32ba795c415b02486ead7a030fe5aa1ecbe33f65246661348de8ac`.

This is a discovery/recipe row, not a current-main result and not a candidate
artifact. It supplies parsed-file/public-file lifecycle, DataFusion
range/projection, OLTP/SQL, version-control, multimedia, and bounded-GC target
families. Its a12 controls must be replayed or re-bound to exact main
`822c204c...` and the first compile-green ForkTree descendant before any result
is accepted. The runner is dry-run by default; individual cells, never a
whole-matrix command, retain the 20-minute cap.

The superseded BlobRef predecessor
`origin/codex/forktree-stage2-milestone4-blobref-owned-view` is pinned at
`08f8dd5cf20842f79996fae9eb7b0924f074a084`, tree
`19c8706d6bc3d1dbe9217b4f8386b19c66f027a8`, parent `af7899f...`, and is
**BLOCKED**. Its range path authenticates a manifest and intersecting chunks
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

## Minimum first-runnable landing gate

The reduced landing package is seven stages, with P0 and residue folded into
stage 1 and checkpoint/recovery plus GC/publication retained as separate
RocksDB-then-SlateDB stages. The former broad/comparator prerequisite is
removed. The exact order is static owner/residue, 65-row delete, SQL DML,
branch/diff/merge/history, parsed files plus BlobRef, checkpoint/recovery, and
GC/publication. The final two stages are now source-mapped to the exact R1
oracle at `f01b08a2db1bd71650eec11123adec26b5222dcc`; older checkpoint and GC
refs remain historical provenance. The package remains held because `1f742...`
is blocked for silent missing-CommitRecord omission; R5 must publish a narrow
reviewed correction before any descendant is executable. R2/R4 have approved
the source correction at `d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768` (tree
`641654079f60fcd1c9ff9ccbbd06d3edcabe4096`, parent `1f742...`, diff
`be940f41...`, patch `1902f4c9`); its immutable transport ref and report are
still unbound, so it remains non-runnable.

Stage 7 also binds the external W5/R7 reachability contract: contract SHA
`9b0aa1f080a082685df1cdbd905bbf90064840b9858159f099d394d7ecf1afb8` and
companion sums SHA
`cea56dd052eb8d64a41bd52feebf5a39623a233d3c8037e0bc5b792e76190e88`.
The immutable W5 package is report-only and no-run blocked by inherited d6b
symbols: head `6487170dfa11b24411dbbd73e3c003439072df09`, tree
`94eefb7de3260a8c8a3217805a5372cb8670157c`, report
`fd47899844bafc72fb47c254f77c74b91d4d40f43d0bb2a54d043823892b6a35`, and
manifest `ea5a278b81d23136e276b29e350752b8c25ce656ba375864362fc2ab0d60ee4c`.
Its one authority, epoch/race, reader-pin, root-universe, corruption,
cold-reopen, and final-reference requirements are bound without enabling
runtime.

The pending reader frontier above approved d6b is head
`9f3c703e953440cde1d60b1511467c4337648c8f`, tree
`51a0026c0c3eced6fdaa5e5ed4824111377f086c`, parent d6b, diff prefix
`6000f34f`, patch prefix `3890dad2`, expected compile frontier 185/7. It is
blocked by derived/history empty-success and legacy TrackedHead/control
acquisition in `load_exact_batch`; d6 remains the last approved base and the
frontier cannot be promoted or run.

This is the reduced non-negotiable landing gate for the first explicitly
compile-green immutable candidate. It is test/report-only and does not authorize
running current main, any compiler-red frontier, or any artifact branch. Every
build, adapter, corruption, and recovery cell is independently wrapped in
`timeout 20m`; stop on the first focused blocker.

1. Verify the candidate exact head/tree against current main
   `822c204c...` and require the immutable descendant to carry the accepted
   ForkTree lineage, R2 atomicity approval, H2 deletion/residue approval, and
   zero independent transaction/upload/GC publication points.
2. Run the immutable P0+W1a source gate, then the deletion/residue, semantic
   delegation, CLI-routing, cursor, `cargo check`, `cargo fmt`, `git diff --check`,
   and warnings-denied Clippy gates. Any legacy authority, compatibility route,
   dual writer, facade gap, or lint/compile failure stops the sequence.
3. Run the production-owner 65-row batch-1 delete on RocksDB, then SlateDB;
   require cold reopen with zero remaining logical rows and no missing-object
   failure. This is the focused OLTP/path-copy publication gate.
4. Run the public SQL/transaction smoke on RocksDB, then SlateDB; require
   atomic batch, rollback, savepoint, `RETURNING`/`ON CONFLICT`,
   idempotency, exact result metadata, and cold-reopen digests.
5. Run the core branch/diff/merge/history/undo-redo gate on RocksDB, then
   SlateDB; require exact graph chronology, branch semantics, public file and
   parsed-file callers, corruption fail-closed, and cold reopen.
6. Run the parsed-file plus large-BlobRef identity gate on RocksDB, then
   SlateDB. It uses only the immutable discovery runner's `vc-*-1k` and
   `blob-*-64` cells: exact parsed/file bytes, authenticated BlobId and
   declared size/domain/hash/range identity, corruption fail-closed, cold
   reopen, and final-reference behavior. It is not the broad multimedia matrix.
7. Run the three-row checkpoint/recovery gate on RocksDB, then SlateDB;
   require 64 rotations, exact merge base, true conflict, missing-parent
   fail-closed, undo/redo, recovery, cold reopen, and bounded final release.
8. Run the sealed GC/publication gate on RocksDB, then SlateDB; require one
   coherent view, epoch/progress fencing in both race orders, bounded persisted
   mark/queue state, upload/ordinary publication, corruption fail-closed,
   reader-pin safety, cold reopen, and final-reference reclamation.

No point-read performance threshold, OLAP scale, broad retained-history matrix,
large-shape multimedia scale, 512 MiB cell, or detached comparator result is a
landing prerequisite. The candidate remains blocked if any minimum gate fails.

## Post-landing follow-up queue

Preserve these rows and their provenance, but do not make them prerequisites:

- 1K point-read A/B and its >10% paired-win / <=5% critical-regression rule;
- 10K/50K/500K ordered OLAP and the SlateDB physical-object residual rule;
- broad retained-history/version-control qualification;
- 64 MiB shape families beyond the identity gate and 512 MiB blobs; and
- external comparator, scaling, and target-discovery measurements.

These follow-ups may run only after the landing gate is green and must retain
the same exact candidate, fresh paths, source map, corruption contract, and
20-minute per-cell cap.

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
