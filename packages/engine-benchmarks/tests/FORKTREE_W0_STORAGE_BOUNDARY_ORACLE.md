# ForkTree W0 storage-boundary correction oracle

Status: TEST/REPORT-ONLY. This direct successor is based on the immutable W0
correction `4abf60b0115c114d3e3784fb0fb8a9ea2e559dfc` (tree
`30aaecc6a2bff719c44499ad933a42437e42c2da`), whose original blocked anchor is
`465786fccbf55decd92e169d646670e3351d077a` (tree
`0f553a521c91983e6d0ea1db98bc7397793aa449`). It changes only benchmark tests,
probes, reports, and verifier scripts. It does not change production source,
start an adapter, add a compatibility path, or open a PR.

## Contract being frozen

The accepted physical boundary remains one engine-owned descriptor/object
plane:

* `StorageSpace` has private fields and a private `SpaceId` field. Only the
  crate-visible `engine_declared` constructor exists; no public `new`,
  `mutable`, or `immutable` forge path is accepted.
* ForkTree owns authenticated `ObjectId` and non-public `ObjectDomain` values.
  `CoherentView<R>` is crate-visible, has a private `read: R`, and does not
  expose a public read/object-domain constructor or getter.
* `OBJECT_SPACE` is immutable object storage, while `SELECTOR_SPACE` and
  `UNTRACKED_ROW_SPACE` are the retained mutable control/data spaces. No raw
  adapter registry, alternate object index, second writer, or fallback reader
  is introduced.
* A view authenticates the root object ID and domain before point/range work.
  Cursors bind both view instance and root identity. Reopen authenticates the
  exact encoded root bytes and domain. Missing roots, wrong domains, identity
  substitution, tampered bytes, foreign cursors, and expired cursors fail
  closed; an authenticated missing row remains `Ok(None)` and is distinct from
  a tombstone or explicit NULL cell.

## What the predecessor missed

The blocked verifier searched only `packages/lix/src`, accepted required names
by token presence, and did not execute its probes. Its TypeScript negative
probe declared the removed methods itself, so it could type-check even when
those methods existed. Its Rust model compared locally manufactured identical
errors and did not authenticate a root/object domain or bind reopen identity.

This successor corrects those gaps:

* The verifier obtains the complete tracked source list with `git ls-files` and
  scans Lix, JS SDK, native bindings, engine examples/benches/tests, RocksDB,
  and SlateDB. Generic storage-adapter test/implementation constructors are
  printed as an explicit allowlist, never silently omitted. Binary-CAS
  `context.rs` and all predecessor binary-CAS owner files are explicit residue
  candidates. The W0 probe/report files are excluded only to avoid the oracle
  auditing its own fixture strings.
* Structural checks inspect declarations, not token presence: private
  `SpaceId`/`StorageSpace` fields and engine brand, crate-visible constructor,
  crate-visible `ObjectId`, `super`-visible `ObjectDomain` and authenticator,
  crate-visible `CoherentView<R>`, private read field, and absence of public
  forge/read accessors.
* `negative_native_exports.ts` imports `LocalFilesystem` and `LixBinding` from
  the actual `packages/js-sdk/src` sources. It does not declare the removed
  properties or methods. The runner separately scans the actual N-API Rust
  registration for the old exports.
* `forktree_w0_compile_probes.sh` runs an actual package compile-pass gate,
  an actual positive descriptor crate, four external Rust compile-fail probes,
  the real TypeScript negative probe, and the native-export absence check. A
  future candidate must make the positive gates pass and the forbidden API
  gates fail with the expected compiler diagnostic code and symbol token; an
  arbitrary nonzero exit is not accepted. The Rust negatives import only
  actual public crate-root `lix::storage`/`lix::storage_adapter` APIs, and the
  runner enables the public `storage-benches` feature rather than depending on
  private or fabricated modules. The runner is bounded by
  `W0_TIMEOUT_SECONDS` (default 1200s) per command and never runs a storage
  adapter.

## Model oracle

`forktree_w0_storage_boundary_oracle.rs` is a standalone deterministic model,
not a claim about an adapter. It now has six executable tests:

1. descriptor identity plus attempted raw-space, raw-domain, and deleted
   columnar-owner operations;
2. authenticated root, point/range order, cursor continuation, reopen, and
   zero read-side writes under Memory/RocksDB/SlateDB labels;
3. missing root, wrong domain, same-size root substitution, malformed bytes,
   and tampered object identity rejection;
4. foreign-view and expired-cursor rejection without writes;
5. authenticated absent, NULL, and tombstone distinction; and
6. retained descriptor parity across all three adapter labels.

The model intentionally uses a deterministic identity stand-in and makes no
cryptographic claim. Future production qualification must use the real
authenticated objects and separate Memory, RocksDB, and SlateDB lifecycle
oracles only after the production crate compiles.

## Exact gates

From an isolated checkout of the candidate:

```sh
node scripts/forktree_w0_storage_boundary_residue_verify.mjs --root "$PWD"
cargo fmt --all -- --check
git diff --check
W0_TIMEOUT_SECONDS=1200 scripts/forktree_w0_compile_probes.sh "$PWD" "${CARGO_TARGET_DIR:-/tmp/forktree-w0-probes}"
cargo clippy -p lix_benchmarks --test forktree_w0_storage_boundary_oracle -- -D warnings
cargo test -p lix_benchmarks --test forktree_w0_storage_boundary_oracle --no-run
```

The standalone calibration command, which does not build or run production,
is:

```sh
rustc --edition=2024 --test \
  packages/engine-benchmarks/tests/forktree_w0_storage_boundary_oracle.rs \
  -o /tmp/forktree-w0-model
/tmp/forktree-w0-model --nocapture --test-threads=1
```

No Memory/RocksDB/SlateDB runtime cell is claimed by this package. A future
compile-green candidate must add those adapter commands, flush/drop/cold
reopen, exact root/object corruption, point/range ordering, cursor binding,
and zero-read-write controls. Every future cell is capped at 20 minutes.

## Immutable baseline calibration

The original predecessor verifier, run unchanged against the exact blocked
anchor, exited 1 with 565 residues and no missing boundary tokens:

```text
node scripts/forktree_w0_storage_boundary_residue_verify.mjs --root <465-review-worktree>
571 lines, 69865 bytes
SHA-256 4cecc96ae9569e5a8c3db0c6860e903b6d114aaa94ef3436fb35be94211fa271
```

The v1 corrected verifier against its own exact correction worktree was
intentionally still red with 956 residues. The corrected verifier against
this direct successor worktree is intentionally still
red on the blocked production source because it scans the full tracked
workspace and finds the existing binary-CAS/columnar/tracked/changelog/raw
owner residues and old JS/native filesystem exports. This successor extends
the existing public tracked/changelog probe to cover the legacy branch-owner
diagnostic and fixes the required `SpaceId(u32)` declaration allowance; it does
not hide any production residue:

```text
598 scanned source files / 606 tracked source files
995 lines, 130025 bytes
SHA-256 6e054be650935553b8efc894c38afd5158e0416fb3cc58fe2681f029602d4749
exit 1; missing retained boundary none; structural findings 0; residues 955
```

The hash above is for the exact detached review root
`/tmp/lix-w0-correction-v2`; the scanner's first line includes its root path.
The canonical count/result is therefore the 606 tracked / 598 scanned source
files and 955-residue RED outcome, with the recorded root-bound hash preserved
for replay.

The explicit generic-storage allowlist is printed separately and is not
counted as residue. This is a diagnostic blocker for a future production W0
cut, not a claim that this test-only successor should delete production code.

The corrected model compiles and passes 6/6 with no warnings:

```text
binary SHA-256 e33005d7653e17a1d8acbf13c323ba195ebbe7fad7b66cd8afec800cd0b9985e
run log SHA-256 d63dc63486f4cef75e6bb0625ce70adb7bf3ab366e9dfca9f7ed51e9333e603f
```

The executable probe runner is wired against the exact blocked source with
per-probe diagnostic validation. Each negative Rust probe now requires its
expected error code and the attempted removed symbol in the captured log;
the raw-space probe additionally retains the `SpaceId` constructor error. The
four probes are `raw_space`, `columnar_owner`, `tracked_changelog`, and
`binary_cas_owner`; the tracked/changelog probe asserts all three removed
reader/selector symbols. A full rerun was deliberately not
claimed after the review host hit the inherited compile frontier: the bounded
attempt was stopped before completion and is non-evidence, not a candidate
acceptance result. An earlier partial attempt used pre-final five-probe
wiring and is intentionally not evidence for this frozen command. The exact
replay command remains the one above; its positive/negative results must be
regenerated on a compile-capable successor.

No adapter runtime, production build matrix, or performance result was
started. The accepted next step is a compile-green production candidate that
must rerun this unchanged verifier and runner before any adapter qualification.
