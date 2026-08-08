# W3 v2 structural-oracle correction

Status: `TEST/REPORT-ONLY`; production, adapter, PR, and main are untouched.

Direct correction base: `85e087f436867f514341fc730cc88729547ad45c` (the blocked
structural-oracle head), parent of this package successor. The package remains
bound to the exact e1af source calibration:

```text
e1af head  e1af471b9ab0f598dafa7c2ddec7867667c81740
e1af tree  bfa0d271a723da8250ab76ada16fda90926f1099
e1af parent b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
clusters   W3-01..W3-14
calibration 58 / 1139 / 16 / 770 / 24
```

The exact e1af control remains RED in all 14 diagnostic clusters. The v2
successor changes only this report package. No source crate is compiled and no
adapter runtime is claimed.

## Corrections

The structural verifier now binds an owned `read` argument to exactly one
`open_coherent_view_on_read(read, selector)` call, a typed `CoherentView`, one
typed `PreparedPublication::from_view(&view, selector, owner, epoch)`, one
explicit `bind_selector_epoch_owner_cas(selector, owner, epoch)`, complete
`publication.into_storage_plan(metadata, idempotency)`, one
`prepare_write_set(plan)`, and one `prepared.commit()`. It rejects copied or
swapped aliases, fresh facade reads, wrong selector/owner/epoch, incomplete
plans, raw stores, second reads/publications/commits, cache, fallback, and
compatibility seams. The accepted GREEN path is a structural fixture; it is
not a production compile claim.

The source graph checks the named publication, lowering, and transaction
functions separately. Candidate paths are limited to this package or the
diagnostic path allowlist, legacy counts cannot increase, and each surviving
cluster must be absent or fail closed before any plan/I/O. The e1af RED control
is intentionally exempt from extra operation-graph reclassification so its
14-cluster output remains the baseline.

The pure model now covers:

- one atomic publication across branch-first and GC-first races, exact epoch,
  same-owner stale, unrelated-owner current, no-op, savepoint, rollback, and
  zero partial state on rejection;
- first-parent generations/floors and missing, duplicate, cycle,
  non-increasing, wrong-kind, substituted, and absent roots;
- authenticated checkpoint/recovery/upload/final transitive roots, shared
  retention, owner+view pins, wrong-owner release, and final-reference
  reclamation;
- poisoned cancellation/malformed cursors with explicit exclusive restart;
- authenticated reopen; and
- 64+suffix progress, one debt, no spin, release cadence, and idempotent drain.

The standalone model gate is `6/6`; the structural self-test has one positive
and twelve discriminating negatives. Future execution is strictly:

```sh
timeout 1200 python3 -W error \
  test-report/forktree-w3-e1af-structural-oracle/w3_65_gc_model.py
PYTHONWARNINGS=error python3 -W error \
  test-report/forktree-w3-e1af-structural-oracle/w3_structural_gate.py --self-test
```

Only after those package gates and a first compile-green candidate may the
documented Memory -> RocksDB -> SlateDB cells run, each under `timeout 1200`,
with immediate stop on the first failure. This correction does not widen W4,
W5, or adapter qualification.
