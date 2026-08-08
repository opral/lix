# BranchRef successor independent review checklist

Test/report-only checklist anchored to the accepted calibration contract:

- accepted calibration ref:
  origin/codex/review/branch-ref-calibration-contract-9f45
- accepted calibration head:
  32d7a024d8d060f9084a45116fbbfca294fdb454
- accepted normalized calibration SHA-256:
  026fcd6b7aaa9afd8341fdca6451962d4addd5aedef63724b6f90d50b8b573bb

No production, adapter, or runtime work is permitted.

## Immutable inputs and provenance

The next candidate must provide immutable commit/tree/parent identities and a
remote ref. The known review inputs are:

~~~text
b59 base commit b59e1f11a51153e0a787a81f0f25bf104d150aaf
b59 base tree   700fd04d21bc40c05425c9fc9e10d65c9e1eda24
9f45 candidate  9f45f77955317b8dd64fadb049d85c33ca109bf4
9f45 tree       c38c4d60c74bf70994378029ad9e286a83cf2d69
9f45 parent     ee00381fd95148cd85a4c0940c3c17ee6805aa25
9f45 parent diff b77a739ac6231e3fac859bb80a4d38b968f5cb911aaca1f88644e20996953b37
9f45 patch-id   872cb7d3d4e7756ca895119ec0ebdee13aa1717a
HANDOFF SHA    288926d43355526489908c84845ba2d30343e97117f04652f0d58754862c128b
~~~

Required commands, with exact candidate SHA substituted after handoff:

~~~sh
git ls-remote origin <ref>
git rev-parse HEAD HEAD^ HEAD^{tree}
git merge-base --is-ancestor b59e1f11a51153e0a787a81f0f25bf104d150aaf HEAD
git diff --name-status HEAD^ HEAD
git diff --full-index --binary HEAD^ HEAD | sha256sum
git diff --binary HEAD^ HEAD | git patch-id --stable
git diff --check
git status --porcelain=v1
~~~

## Scope and artifact gates

The corrected BranchRef package is report/test-only. Its complete package
directory is:

~~~text
packages/lix/tests/branch_ref_whole_closure_oracle_b59/
  FUTURE_GATE_COMMANDS.md
  MANIFEST.json
  README.md
  SHA256SUMS
  SOURCE_GATE_RESULT.md
  branch_ref_whole_closure_model.rs
  verify_branch_ref_whole_closure.sh
~~~

For the accepted 9f45 parent delta, the exact changed path set is only
MANIFEST.json, SHA256SUMS, and SOURCE_GATE_RESULT.md below that directory. Any
production path, Cargo manifest, selector implementation, reader/writer, or
adapter change is an immediate BLOCKER.

Run:

~~~sh
cd packages/lix/tests/branch_ref_whole_closure_oracle_b59
sha256sum -c SHA256SUMS
bash -n verify_branch_ref_whole_closure.sh
rustfmt --edition 2021 --check branch_ref_whole_closure_model.rs
~~~

The manifest must embed the exact HANDOFF digest, R1 blocker hashes, candidate
identity, and the canonical source-calibration result. Stale 460/331 prose is
not itself a blocker only when the committed normalizer reproduces the
canonical result; unbound alternative counts are rejected.

## Calibration replay and normalization

Run the committed calibration verifier with immutable b59 and candidate
worktrees. It must capture stdout and stderr separately, require scanner exit
status 1, and preserve the candidate-owned scanner.

Normalization may only:

1. convert CRLF to LF;
2. remove trailing horizontal whitespace;
3. replace the absolute first root=... value with root=<ROOT>;
4. wrap the unchanged streams as [stdout] and [stderr].

It must not sort, reorder, rewrite counts, drop paths, merge streams, or filter
inventory lines. Both trees must produce:

~~~text
required-missing=0
legacy-residue=460
old-closure-paths=4
lix-branch-ref-occurrence-files=15
non-derived-lix-branch-ref-files=4
authority-use-lines=331
normalized bytes=78700, lines=26
normalized SHA-256=026fcd6b7aaa9afd8341fdca6451962d4addd5aedef63724b6f90d50b8b573bb
stderr SHA-256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
~~~

Any 481/343 claim must commit its normalizer/source, raw captures, normalized
output/hash, exact base/candidate identities, and HANDOFF-bound manifest, then
reproduce from these same trees.

## Semantic model gate

The only model compilation allowed is standalone, outside Cargo/production:

~~~sh
rustc --edition=2021 --test -D warnings \
  packages/lix/tests/branch_ref_whole_closure_oracle_b59/branch_ref_whole_closure_model.rs \
  -o /tmp/branch-ref-model
/tmp/branch-ref-model --nocapture --test-threads=1
~~~

Require all 15 tests, including:

~~~text
authenticated_fingerprint_covers_every_selector_authority_field
canonical_global_selector_is_accepted
create_switch_delete_retire_gc_and_cold_reopen_are_one_authority
delete_and_gc_reclaim_final_branch_reference_only
empty_undo_redo_are_true_no_ops
fingerprint_covers_state_and_in_flight_allocations
invalid_multi_authority_publication_rejects_before_write
malformed_identity_missing_root_and_cycle_fail_closed
old_view_survives_rotation_and_reopen_until_released
one_retained_view_and_one_prepared_publication_one_commit
reopen_rejects_global_epoch_gap
same_owner_stale_cas_and_unrelated_owner_are_distinct_failures
same_size_forged_global_key_and_root_fail_before_view_or_write
second_authority_negative_cannot_publish_or_change_selected_root
selector_bytes_bind_exact_root_catalog_generation_and_owner
~~~

The forged global control must use equal-size forged canonical key/root bytes,
recompute authentication, and fail before view return, write, commit, or
selector rotation. Canonical global acceptance must remain green.

## Tamper negatives

The review is BLOCKED if any of these tamper cases passes:

- wrong base/candidate commit or tree;
- wrong anchor or parent/diff/patch identity;
- missing, altered, or unbound HANDOFF digest;
- changed normalizer that strips counts, paths, inventory, or stream identity;
- merged or swapped stdout/stderr;
- root replacement omitted, overbroad, or applied to non-root content;
- any canonical count changed from 460/331/4 without committed reproducible
  transformation;
- missing/malformed SHA256SUMS or package file outside the approved path set;
- model warning, failed test, forged selector acceptance, or write counter
  change before rejection;
- production source, Cargo, adapter, runtime, selector, cache, fallback, or
  second-authority changes.

Terminal approval requires every provenance, package, calibration, model, and
tamper gate to pass on the immutable handoff.
