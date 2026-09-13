# Partial replica convergence — engineer handoff (2026-09-13)

Historical handoff for the starting revision `f6136a19c`. The continuing QA work
is recorded in [docs/partial-replica-qa-1772.md](docs/partial-replica-qa-1772.md).
The status, workspace directions, and validation statements below describe that
earlier revision, not the current working tree.

## Objective and status

Finish server-authoritative convergence for Lixray's **partial replica with on-demand sync**. Use server acceptance order and existing native row/plugin merge semantics; no user-facing conflicts or new public preparation API. Preserve bounded opening and fast local warm reads/writes. User allows breaking changes, with old repositories remaining readable/migratable.

**This branch is an unvalidated work-in-progress, pushed at the user's request. Do not deploy or merge yet.** Current native acceptance changes have not been compiled or run through full tests. Storage regression reproduced the live error before the fix; green validation is outstanding. No claim that the reported preview is fixed.

## Three findings

1. **Live preview storage failure, reproduced locally.** A new disposable file in launch-q3 fails `/sync/push` with HTTP400: `LIX_STORAGE_ERROR: storage corruption: immutable segment identity was assigned different bytes`. Segment IDs previously hashed ordered keys and lengths, not values. Upload happens before transaction commit; rollback can leave an orphan object that collides with a later legitimate same-length upload. This branch hashes contents in the common segment writer (v2 domain), removes SlateDB's duplicate replacement-only hash, and adds a rollback/reopen/immutability regression. Existing locator/envelope formats are unchanged; old segment IDs are opaque addresses and remain readable. Published per-key immutability checks remain.
2. **Frozen authority head prevents progress.** Previous PR1768 only restarted an attempt when initial retention saw selected-head drift. Drift after retention could still strand an immutable attempt. This branch accepts against the current selected authority head inside the authority transaction, keeps original request/digest and receipt-first deduplication, and verifies the actual merge parent during settlement/rollover. Local edits made after the submitted L remain pending. An already-included L receives an empty native acknowledgment merge, without replaying rows or old checkpoint intent. Already-included acknowledgments preserve current authority checkpoint; genuine new checkpoint intent still uses normal merge validation.
3. **Editor observation/save-completion race, already merged separately.** An older save promise could reset the editor's acknowledged baseline after a newer authoritative observation, leaving it falsely dirty and suppressing future remote updates. Atelier PR168 adds a private observation-generation guard and a native-backed regression. Merged main: `5f1cc8074898204aa46e70f3a1703e3dd212c76e`. Relevant 93 tests, typecheck, formatting and CI passed. https://github.com/opral/atelier/pull/168

## Blocking review / protocol decisions

- **Protocol hard cut unresolved.** Old clients require the receipt merge's first parent to equal captured R. New server may use actual current R. Unchanged DTO bytes do not imply compatibility. Bump the appropriate sync protocol and verify old live tabs fail with an actionable reload/version mismatch instead of getting stranded. Paired SDK/server deployment alone does not solve already-open tabs. No compatibility shim requested.
- GLOBAL drift during a pending selected attempt remains explicitly unsupported. Do not describe this patch as universal convergence across arbitrary global changes; assess whether further work is required for the user's contract.
- Independent final review of checkpoint/included-head handling is pending. In particular, never replay already-accepted checkpoint selection over a later authority checkpoint.
- Storage fix must pass real adapter tests; only pre-fix red evidence exists so far.

## Workspaces and branches

- Combined native fixes: `/root/repos/lix-current-authority-acceptance`, branch `fix/current-authority-acceptance`, based on main `792bc4b47` (includes newer migration/plugin changes; preserve these).
- Isolated storage reproduction: `/root/repos/lix-immutable-segment-content`; its two changed files were copied to the combined branch. Continue in combined workspace.
- Lixray integration: `/root/repos/lixray-convergence-latency`, branch `fix/convergence-latency`; PR381 remains OPEN, do not merge it: https://github.com/opral/lixray/pull/381
- Lixray currently pins Lix `c8fb003d78d8ffb2e1b694676827229fb4006b0c` and Atelier `3c0c7a5c18b6e9235a4711c1ddaeba0d82815583`; neither new fix is deployed there.
- Do not modify unrelated dirty original `/root/repos/lixray` workspace.

## Validation to run

No Cargo process is running from this task at handoff. Shared target was used to reduce rebuild cost; run builds sequentially:

```sh
cd /root/repos/lix-current-authority-acceptance
export CARGO_TARGET_DIR=/root/repos/lixray/vendor/lix/target
export CARGO_BUILD_JOBS=2
export CARGO_INCREMENTAL=0
cargo nextest run -p lix-storage-slatedb --no-fail-fast
cargo nextest run -p lix-storage-rocksdb --no-fail-fast
cargo nextest run -p lix --features all-simulations,server-protocol --no-fail-fast
cargo test -p lix --features all-simulations,server-protocol --doc
cargo clippy -p lix --all-features --all-targets -- -D warnings
node scripts/validate-changes.mjs
node scripts/validate-server-protocol-docs.mjs
git diff --check
```

Package names on latest main use hyphens (older AGENTS examples use underscores). Pre-fix storage regression log: `/tmp/immutable-orphan-red.log`, test `rolled_back_immutable_upload_does_not_reserve_unpublished_identity`; it fails with the exact live error. Formatting and diff checks were run on the draft; unrelated formatting changes were excluded.

After review and green CI, merge upstream Lix, upgrade BOTH Lix and Atelier submodule pointers in Lixray, then install/build the exact main SDK artifact:

```sh
LIXRAY_LIX_ARTIFACTS=only pnpm --dir web-app lix:build
pnpm vendor:check
pnpm --dir web-app ci:prepare:web
pnpm --dir web-app ci:lint
pnpm --dir web-app ci:check
pnpm --dir web-app ci:build
```

Wait for matching server image/artifact readiness. Push PR381, get CI green and confirm preview health deployment changed. Never stamp an artifact as a revision it was not built from.

## Live QA and recovery evidence

Preview: https://lixray-web-lixray-pr-381.up.railway.app
Latest observed web deployment: `78fdb477-ebec-463a-9d2a-b89f1fa070a2`.
User file (do not edit): `/@acme/launch-q3/file/2ESap92eQqKDRMU6gE16ew/hello-world.md`.
Disposable test file: `/@acme/launch-q3/file/pL4JPPtVRj-09chC8exy3w/untitled.md`.

Saved independent OPFS browser profiles and failure events:
`/root/repos/research/lazy-browser-sync/preview-divergence-heading-qa/`.
This run failed before the second editor opened: 20 push400 responses; first browser still owns a pending creation. **Preserve profiles, reopen after deployment and prove pending creation recovers without clearing storage.**

Scripts (Playwright dependencies available in this directory):
- `/tmp/partial-final-profile/qa-preview-heading.mjs`: latest reproduction; intended alternating heading/body edits but upload blocked first. Add a finite default action timeout.
- `/tmp/partial-final-profile/qa-preview-divergence-fixed.mjs`: fresh two-context edits and final/sentinel assertions; adapt output directory.
- `/tmp/partial-final-profile/qa-preview-upgrade.mjs`: prior saved-profile recovery; adapt to new profiles/file.
- `/tmp/partial-final-profile/qa-preview-final-authority.mjs`: independent server SQL comparison; update deployment/file.

Required QA: saved pending-data recovery; two fresh independent contexts continuously editing heading and body; wait for queues to settle; both editor texts and independent server SQL must match; then confirm later edits propagate in BOTH directions and a fresh context sees the same result. Record convergence latency and request counts. Do not mistake temporarily matching screenshots or a single successful writer for convergence. Prior tests passed after typing stopped while repeatedly restarting during typing, which was insufficient.

Demo launch-q3 repo UUID: `00000000-0000-4000-8000-000000000002`. Server SQL uses `$1` placeholders; demo auth/session setup is in the scripts. Raw traces contain session material: keep local, do not attach publicly. Earlier evidence/report: `/root/repos/research/lazy-browser-sync/working-set-delivery.md`; prior recovery profiles also remain under that research directory.

## Next engineer's sequence

1. Review combined changes, settle protocol hard-cut and remaining correctness concerns.
2. Run adapter/native gates; fix failures; obtain independent review.
3. Merge upstream only when CI is green and feedback addressed.
4. Upgrade both vendor dependencies in PR381 and deploy matching SDK/server.
5. Run saved-profile recovery and fresh two-client QA against the actual deployment, comparing server SQL; fix any remaining divergence before declaring success.
