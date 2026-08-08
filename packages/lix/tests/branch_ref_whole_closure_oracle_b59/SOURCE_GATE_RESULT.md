# b59 source-gate calibration and BranchRef v5 correction

This is a static calibration result on the immutable b59 anchor. It is not a
candidate acceptance result and does not claim production compilation or
runtime. The corrected successor keeps the source gate intentionally RED on
b59 while strengthening the pure model and source policy.

Command:

```sh
bash packages/lix/tests/branch_ref_whole_closure_oracle_b59/verify_branch_ref_whole_closure.sh \
  /root/repos/lix-branch-ref-oracle-b59 \
  b59e1f11a51153e0a787a81f0f25bf104d150aaf
```

Result: exit status `1`, expected `RED`.

The corrected successor replayed the same source verifier from its immutable
482e descendant. It still reports `required-missing=0` and the expected
compiler-red closure (`legacy-residue=460`, `old-closure-paths=4`); no
production file changed in the correction. The normalized source-gate capture
is `/tmp/branch-ref-selector-correction-b59-source.log`, SHA-256
`6517c8ef1f25af1cc875b7f13ac0f7bb46786b6cc16ad3d3dab800aa6cb2b7f3`.

Summary:

```text
required-missing=0
legacy-residue=460
old-closure-paths=4
lix-branch-ref-occurrence-files=15
non-derived-lix-branch-ref-files=4
authority-use-lines=331
RED BranchHead/BranchRef whole-closure deletion boundary
```

The complete path-normalized stdout/stderr capture is identified by:

```text
22218ff0895f667e533bc942254f234cf543e399640223f89f0cca70c34469fb
```

The four non-derived projection files are intentionally reported rather than
whitelisted: `branch/stage_rows.rs`, `gc.rs`, `init.rs`, and
`live_state/context.rs`. The branch descriptor schema, public test/benchmark/
SDK surfaces, and explicitly listed schema/catalog projection files are
classified as derived-only. Any successor must make the former files derived
or remove their legacy ownership before the gate can turn GREEN.

Static checks on this package:

* `bash -n verify_branch_ref_whole_closure.sh`: PASS
* `rustfmt --edition 2021 --check branch_ref_whole_closure_model.rs`: PASS
* `git diff --check`: PASS
* inherited 482e standalone model `rustc --edition=2021 --test -D warnings`: PASS
* inherited 482e standalone model runtime: 13/13 PASS, binary SHA-256
  `c8599f55163dd03ea17a480df49bf342a2778c04c0e35d1dafca269773ae023a`,
  log SHA-256 `42d98c0fb0bbc875bf4ea85649d9cd7ce305bcdf674bb307ba682cf9bd6f3f17`
* direct successor adds create-read ownership and catalog-object negatives;
  its standalone model compile/runtime is intentionally not claimed here
* production compilation, adapter tests, and current-main runtime: deliberately
  not run

## H1 correction coverage

The successor model adds deterministic selector construction and validation.
Global and branch selector bytes contain the exact root, epoch/generations,
catalog root, canonical branch identity, and owner identity; each has a stable
authentication fingerprint. `SelectorFingerprint` exposes all of those fields
for equality across reopen and CAS outcomes.

The model has separate `StaleSelector`, `UnrelatedOwner`, and `DualAuthority`
outcomes. Same-owner stale bytes fail before writes; a publication relabeled
to another branch owner fails before writes; and a forged derived
`lix_branch_ref` authority cannot publish. The lifecycle tests exercise create,
switch, advance, delete, retire, retained-view release/GC, and cold reopen,
including malformed/same-size substitutions, missing roots, cycles, and epoch
gaps. Future adapter commands require the same fingerprints, one retained view,
and zero backend writes for read/CAS rejection.

## Direct v2 successor correction coverage

The create path now acquires a retained read for the staged snapshot and
publication checks the exact nonzero read ID and snapshot binding for both
creation and update. The correction oracle rejects a zero read ID and a
released create read without changing selector state or write counters.
`open_view` and `reopen` now require the selector-catalog object to exist and
to authenticate its canonical object ID, `selector_catalog` kind, and
`selector:global` back-edge. The correction oracle rejects missing physical
catalog objects, missing catalog records, wrong object IDs, wrong kinds, and
wrong back-edges. These are model/source gates only; no production or adapter
runtime was run for this direct successor.

## Direct v3 read-closure correction

This immutable child of `882b13e5c6fb3b0f2ff3d79e7ede7665f7ee0d41` adds a
retained-read fingerprint covering branch identity, branch selector key and
snapshot, plus global selector key, root, epoch, and generation. A read from a
different branch sharing a snapshot, or a branch/global root substitution,
fails before any write. `open_view` and `reopen` require live global roots;
reopen also requires live selected branch snapshots and the authenticated live
catalog closure. `prepare_branch` rejects absent, dead, and staged-only
objects, while update publication rejects forged non-live/staged replacements
before mutation and cannot resurrect them. Four discriminating model cases
cover branch-sharing, root mismatch, global liveness, non-live preparation/
publication, and reopen closure. The v3 model is source-only here; no compile
or runtime claim is made.

## Direct v4 error-taxonomy correction

This immutable test/report-only child of v3 commit
`3a3c687f30c5190ac5eb9dc397745aec5d3a18d2` changes only the model's failure
classification. `open_view` and `reopen` share a branch-snapshot validator:
an absent or non-live global root, branch snapshot, or selector-catalog
closure is `MissingRoot`; malformed selector authentication retained v4's
prior `InvalidFingerprint` outcome, while an embedded branch identity mismatch
remained `CorruptSelector`.
No production source, adapter, format,
compatibility path, or authority changed. The exact standalone v4 model
compile and 19-test runtime are recorded below; production and adapter
runtime remain unclaimed.

* direct v4 `rustc --edition=2021 --test -D warnings`: PASS
* direct v4 model runtime: 19/19 PASS, binary SHA-256
  `cf49f6b83d088f0a642ef8434fc9b59802bf67dfc7147bb5cad0c3c1f178953f`,
  log SHA-256
  `2efcf07abc35e28bd258ed9592f66da89333bc917e96f322693794c579fb457b`
* direct v4 source-verifier capture: expected RED, log SHA-256
  `c1c39f02df2f99116cf4aa9ef5a30e8719774a54d7d9ffb9e943483c046e522d`
* `rustfmt --edition 2021 --check`, `bash -n`, and `git diff --check`: PASS

## Direct v5 selector-authentication correction

This immutable child of v4 commit
`3a553f5cf59745651cf7df026a7a10de6f8639aa` removes the stale
`InvalidFingerprint` model outcome. Both malformed global and malformed branch
selector authentication now fail closed as `CorruptSelector`, with an
explicit two-selector regression. `MissingRoot` and stale/unrelated CAS
classification are unchanged.

* direct v5 `rustc --edition=2021 --test -D warnings`: PASS
* direct v5 model runtime: 20/20 PASS, binary SHA-256
  `8b60cbc43e3e070e30de68c1dff308b72574ddbeef00c5a252d8aefe18cbc11a`,
  log SHA-256
  `3e54bef653af9ce56e7f08eface6036f80b25b7908e9e696d209cc716bb7b8d9`
* direct v5 source-verifier capture: expected RED, log SHA-256
  `c1c39f02df2f99116cf4aa9ef5a30e8719774a54d7d9ffb9e943483c046e522d`
* direct v5 rustfmt, shell syntax, diff, and package checksum gates: PASS
