# b59 source-gate calibration and 6eba H1 correction oracle v3

This is a static calibration result on the immutable b59 anchor. It is not a
candidate acceptance result and does not claim production compilation or
runtime. The direct successor keeps the source gate intentionally RED on b59
while preserving the approved pure model and correcting the scanner's input
boundary.

## Exact command and input

```sh
bash packages/lix/tests/branch_ref_whole_closure_oracle_b59/verify_branch_ref_whole_closure.sh \
  /tmp/branch-ref-selector-correction-v2-review \
  b59e1f11a51153e0a787a81f0f25bf104d150aaf \
  > /root/repos/lix-evidence/branch-ref-selector-correction-oracle-6eba-v3/source-canonical.raw.log 2>&1
```

The exact input is the detached v2 successor checkout at
`/tmp/branch-ref-selector-correction-v2-review`, whose HEAD is
`9f45f77955317b8dd64fadb049d85c33ca109bf4`. The command scans the tracked
`packages` tree and excludes only the oracle package using the search-root
relative glob `!**/lix/tests/branch_ref_whole_closure_oracle_b59/**`. The
verifier records raw output first; path normalization replaces the absolute
input root with `<ROOT>` for the normalized digest.

Result: exit status `1`, expected `RED`.

The raw capture is
`/root/repos/lix-evidence/branch-ref-selector-correction-oracle-6eba-v3/source-canonical.raw.log`,
SHA-256 `aa50ca96ffe94bc3917f2ba065edce9de2aa1843c442ac72fa37aeaf230b7232`.
The normalized capture is
`/root/repos/lix-evidence/branch-ref-selector-correction-oracle-6eba-v3/source-canonical.normalized.log`,
SHA-256 `34d516017699c2a8cdc39d74ed52a037aa5787fca688d5bac7f4955b8fc0698b`.

## Canonical result

```text
required-missing=0
legacy-residue=460
old-closure-paths=4
lix-branch-ref-occurrence-files=15
non-derived-lix-branch-ref-files=4
authority-use-lines=331
RED BranchHead/BranchRef whole-closure deletion boundary
```

The prior v2 capture's `481/343` was noncanonical because its ripgrep glob was
evaluated from the absolute `packages` search root and did not exclude the
oracle package. The corrected scanner commits the explicit
`ORACLE_RG_EXCLUDE="!**/$ORACLE_RG_REL/**"` input boundary. The 460/331
calibration is therefore the canonical value for this package.

The four non-derived projection files are intentionally reported rather than
whitelisted: `branch/stage_rows.rs`, `gc.rs`, `init.rs`, and
`live_state/context.rs`. The branch descriptor schema, public test/benchmark/
SDK surfaces, and explicitly listed schema/catalog projection files are
classified as derived-only. Any future production successor must make the
former files derived or remove their legacy ownership before the gate can turn
GREEN.

## Prior-control binding

The R1 v5 blocker control evidence remains bound exactly by source log
`a1fe150858a8f85af1e24e04f1bbc367182b1b5652bd904187b21dec1411d830`, model log
`c481827a36dacf8168d35791f9ac44ec4992b86604eb2af117165d6713d75044`, and
model binary
`36f96fbac86acfaf6a783ac608c528f5467814304d8d184cbb15c62caf3987b0`.
These are prior-control identities, not production/runtime claims.

## Static/model checks

* `bash -n verify_branch_ref_whole_closure.sh`: PASS
* `rustfmt --edition 2021 --check branch_ref_whole_closure_model.rs`: PASS
* `git diff --check`: PASS
* `rustc --edition=2021 --test -D warnings branch_ref_whole_closure_model.rs`: PASS
* standalone model runtime: 15/15 PASS
* model compile log SHA-256: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
* model log SHA-256: `f3988dddb6163e4da6160c67fee6ae31a7c21541f8c421abefea9955d6c06fc2`
* model binary SHA-256: `26c829ad33572ccc59ad8aecf0be29361220be1c2ead7568de90a00d22176328`
* production compilation, adapter tests, and current-main runtime: deliberately not run

## H1 correction coverage

The approved model binds global and branch selector bytes to exact roots,
epoch/generations, catalog root, canonical branch identity, owner identity,
and authentication fingerprints. Global validation requires the canonical
global selector key `selector:global` and repository root `root-global`; a
recomputed fingerprint over same-size forged key/root bytes is rejected before
view, write, commit, or rotation. Separate stale same-owner, unrelated-owner,
and derived-BranchRef outcomes, lifecycle/GC/cold-reopen, cycle/epoch-gap, and
missing/malformed controls remain unchanged from the approved 15/15 model.

## Handoff binding

This direct successor is descended from v2 commit
`9f45f77955317b8dd64fadb049d85c33ca109bf4`. `MANIFEST.json` binds the prior
immutable handoff SHA-256
`288926d43355526489908c84845ba2d30343e97117f04652f0d58754862c128b`, prior
blocker report SHA-256
`69ddb8b7024ac436af29ae7efe19fb2bfc2a74a03d4f206f45ceb1cb8e6d35a3`, and
prior blocker manifest SHA-256
`8af37ef5dcf992d43e9afa65cdfbc76578763d9db0c33b3fb6f6ee3813ef2072`.
