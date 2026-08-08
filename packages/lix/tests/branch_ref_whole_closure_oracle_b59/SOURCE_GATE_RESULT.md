# b59 source-gate calibration and 6eba H1 correction oracle

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
482e descendant. The exact candidate run reports `required-missing=0` and the
compiler-red closure (`legacy-residue=481`, `old-closure-paths=4`); no
production file changed in the correction. The path-normalized source-gate
capture is `/root/repos/lix-evidence/branch-ref-selector-correction-oracle-6eba/source-successor-normalized.log`,
SHA-256 `18f9ac8be382be047445a6c52448d9d4f77785f23b8383dc4846a4d23defecd4`.

Summary:

```text
required-missing=0
legacy-residue=481
old-closure-paths=4
lix-branch-ref-occurrence-files=15
non-derived-lix-branch-ref-files=4
authority-use-lines=343
RED BranchHead/BranchRef whole-closure deletion boundary
```

The complete path-normalized stdout/stderr capture is identified by:

```text
18f9ac8be382be047445a6c52448d9d4f77785f23b8383dc4846a4d23defecd4
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
* standalone model `rustc --edition=2021 --test -D warnings`: PASS
* standalone model runtime: 15/15 PASS; binary and log hashes are recorded
  in `SHA256SUMS`.
* production compilation, adapter tests, and current-main runtime: deliberately
  not run

## H1 correction coverage

The successor model adds deterministic selector construction and validation.
Global and branch selector bytes contain the exact root, epoch/generations,
catalog root, canonical branch identity, and owner identity; each has a stable
authentication fingerprint. Global validation additionally requires the
canonical global selector key `selector:global` and repository root
`root-global`; a recomputed fingerprint over same-size forged key/root bytes
is not sufficient. `SelectorFingerprint` exposes all of these fields for
equality across reopen and CAS outcomes.

The model has separate `StaleSelector`, `UnrelatedOwner`, and `DualAuthority`
outcomes. Same-owner stale bytes fail before writes; a publication relabeled
to another branch owner fails before writes; and a forged derived
`lix_branch_ref` authority cannot publish. The lifecycle tests exercise create,
switch, advance, delete, retire, retained-view release/GC, and cold reopen,
including malformed/same-size substitutions, missing roots, cycles, and epoch
gaps. Future adapter commands require the same fingerprints, one retained view,
and zero backend writes for read/CAS rejection. The correction's positive
control accepts the canonical selector; its negative control requires
same-size forged key/root rejection as `InvalidFingerprint` before view,
write, commit, or rotation.
