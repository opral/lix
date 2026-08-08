# b59 source-gate calibration and 482e H1 correction

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

## Direct successor correction coverage

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
