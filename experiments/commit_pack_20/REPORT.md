# EXP-COMMIT-PACK-20 — qualified no-cut

## Identity and decision

- Base/ref: `origin/codex/schema-forktree-unified-dc4-347`
- Base commit: `5089b964d5e9b0143656c5278e525db9100e2b61`
- Base tree: `4e95f72208bb702698483cb261ebab53c301f064`
- Verdict: **NO-CUT**. A canonical CommitPack does not clear the OLTP-first `>5%`
  acceptance rule without a critical topology/history regression.
- Global ledger: rejection **20/20** (PAGE-SIZE-06 was 19/20).

The experiment stopped at the required smallest crossover. It did not modify the
production ForkTree format or introduce a second reader/writer.

## Existing geometry

The pinned base already packs commit metadata aggressively:

- `CommitObjectV1` is the compact topology envelope (parents, roots, checkpoint
  cursor, member-page object IDs).
- `CommitChangePageV2` stores ordered introduced payloads inline, is compressed,
  byte-bounded, and enforces the authenticated edge bound.
- Commit-member closure loading batches all page IDs in one storage `get_many`.

Consequently, the proposed pack can remove only one logical metadata request for
small one-page commits. For commits exceeding the canonical 64-KiB page target,
both layouts use the same envelope plus deterministic fixed pages, so object count
and bytes are unchanged. Inlining the one page also makes topology-only history
reads fetch and hash payload bytes that the current envelope avoids.

## Canonical model

`src/main.rs` models the pinned format's 64-KiB target, 4-MiB maximum page,
256-edge bound, zstd level 1, BLAKE3 object identities, introduced and selected
members, and 1/2/8-parent envelopes. The candidate is a versioned, deterministic
pack containing the envelope and at most one canonical member page. Larger commits
remain envelope plus fixed authenticated pages under the same candidate format.

Representative encoded geometry:

| Shape | Current objects/bytes | Pack objects/bytes | Topology bytes current→pack | Hash cost current→pack |
|---|---:|---:|---:|---:|
| introduced D=1, payload=128 | 2 / 562 | 1 / 586 | 361→586 | 389→643 ns |
| introduced D=10, payload=128 | 2 / 1,768 | 1 / 1,792 | 361→1,792 | 399→1,776 ns |
| introduced D=100, payload=128 | 2 / 13,498 | 1 / 13,522 | 361→13,522 | 385→3,673 ns |
| selected D=100 | 2 / 750 | 1 / 774 | 361→774 | topology inflation |
| introduced D=500, payload=128 | 3 / unchanged | 3 / unchanged | unchanged | unchanged |

The model rejects truncation, duplicate members, ordinal gaps, count mismatch,
root/owner substitution, cross-commit members, and missing large pages before use.

## Real adapter crossover

Command:

```text
CARGO_TARGET_DIR=/root/repos/lix-exp-page-size-06/target \
  cargo bench -p lix_e2e --bench commit_pack_20 \
  --features 'storage-benches slatedb' -- --nocapture
```

Three warmups and twenty measured samples per cell, retained read, 1,000-commit
history. Times below are p50. Slate physical counters stayed at zero after warmup,
so Slate numbers qualify only as hot retained-adapter wall time, not cold physical
I/O evidence.

| Shape | Backend | topology one | closure one | topology H=1000 | closure H=1000 |
|---|---|---:|---:|---:|---:|
| introduced D=1 | Rocks | 518→593 ns (+14.5%) | 1,108→600 (-45.8%) | 440,098→431,011 ns (-2.1%) | 847,936→430,550 (-49.2%) |
| introduced D=1 | Slate | 148→173 ns (+16.9%) | 286→185 (-35.3%) | 804,474→828,008 (+2.9%) | 3,100,443→819,412 (-73.6%) |
| introduced D=10 | Rocks | 500→644 ns (+28.8%) | 1,223→640 (-47.7%) | 409,150→551,949 (+34.9%) | 903,600→542,190 (-40.0%) |
| introduced D=10 | Slate | 147→174 ns (+18.4%) | 282→174 (-38.3%) | 806,036→830,834 (+3.1%) | 3,107,286→823,269 (-73.5%) |
| introduced D=100 | Rocks | 494→945 ns (+91.3%) | 1,483→948 (-36.1%) | 409,941→1,717,451 (+319%) | 2,173,299→1,700,670 (-21.7%) |
| introduced D=100 | Slate | 161→169 ns (+5.0%) | 278→351 (+26.3%) | 805,876→834,290 (+3.5%) | 3,104,712→821,746 (-73.5%) |
| selected D=100 | Rocks | 496→604 ns (+21.8%) | 1,170→612 (-47.7%) | 441,230→441,912 (neutral) | 886,829→438,595 (-50.5%) |
| selected D=100 | Slate | 147→173 ns (+17.7%) | 310→1,010 (regression) | 807,469→833,509 (+3.2%) | 3,103,369→824,201 (-73.4%) |

The positive result is real but narrow: a consumer that always needs the complete
member closure saves one logical request. The same format materially regresses the
topology-only path, especially as the inlined page grows. History, branch ancestry,
merge-base, GC topology, and checkpoint discovery are topology consumers. The
candidate therefore violates the no-critical-`>5%`-regression rule.

## Public harness qualification boundary

The exact-base `branch_merge_benchmark` is stale relative to the pinned public API.
A warm release build first failed at its three removed `open_another_session()`
calls. A temporary benchmark-only rebind reached schema registration, where the
harness still used removed `lix_registered_schema.lixcol_untracked`. Updating that
temporary statement reached branch setup, which failed because the stale harness
does not supply the required `lix_branch_descriptor` entity identity. No VCS timing
was produced and none is claimed. All temporary harness edits were removed.

The test-aware base also has unrelated pre-existing test-source diagnostics
(missing hydrated CSV fixtures and stale test APIs); the standalone model and real
adapter benchmark compile and run successfully.

## Conclusion

CommitPack shifts cost rather than removing an asymptotic owner. Current ForkTree
already has one compact envelope, compressed fixed member pages, and batched page
loads. Packing one page into the envelope trades a request on closure-heavy reads
for larger hashing/read amplification on topology-heavy VCS paths; large commits are
physically identical. There is no production cut to carry forward.

No reviewer was spawned because the experiment is a rejection, as required by the
ledger contract. The global sequence reaches 20 consecutive qualified no-wins and
should pause pending a new assignment.
