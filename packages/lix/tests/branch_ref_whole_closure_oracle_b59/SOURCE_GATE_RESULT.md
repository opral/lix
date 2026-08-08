# b59 source-gate calibration

This is a static calibration result on the immutable b59 anchor. It is not a
candidate acceptance result and does not claim compilation or runtime.

Command:

```sh
bash packages/lix/tests/branch_ref_whole_closure_oracle_b59/verify_branch_ref_whole_closure.sh \
  /root/repos/lix-branch-ref-oracle-b59 \
  b59e1f11a51153e0a787a81f0f25bf104d150aaf
```

Result: exit status `1`, expected `RED`.

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
* Rust compilation, adapter tests, and runtime: deliberately not run
