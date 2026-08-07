# ForkTree history-independence decision gate

## Decision

Do **not** require online canonical/history-independent packing before the
production hard cut. Keep the current content-addressed path-copy layout and an
explicit future canonicalization seam.

The focused 1K gate proves a real divergent-history limitation: equal logical
states built through different split and value-pack histories do not generally
have equal roots or subtree boundaries, and cold diff can fall back to
`O(N + M)`. Canonical rebuilding has a large perfect-elimination ceiling for
that case. It does not clear the acceptance policy, however, because making the
rebuild part of every publication replaces changed-path work with `O(N)`
publication and causes critical write, allocation, and CPU regressions well
above 5%. Ordinary same-parent changes remain changed-path/output proportional.

No production, Stage2, cursor, Storage, or SQL source is changed by this
package. The only model change is a read-only authenticated state inspection
surface; it cannot publish, route, or authorize objects.

## Provenance

- Approved model base: `bc82385ec42b1789018fbd1213f637c19104a02c`
- Base tree: `abfaa70faf12c3cdcbe3f990dbf8b4e01340af4a`
- Physical model: fixed 64-row leaves and fanout 32, one immutable object space,
  one selector/epoch plane, mutation-local value packs, authenticated ObjectIds
- Current-main context at freeze time: `803d19ec0b67fb4b759aceab7ceb74650d9d894f`
  (not merged into or used to mutate this benchmark-only branch)

The fixture has one sentinel plus 1,000 deterministic rows. It constructs the
same ordered state by bulk sorted construction, one sorted transaction,
deterministically shuffled 32-row transactions, reverse transaction order,
single-row split-boundary-adversarial insertion, and delete/reinsert around
16/64-row boundaries. A one-row update from the sorted-insert root is the real
diff control. Exact ordered rows are checked before flush/drop/reopen and all
diff cardinalities are asserted.

## Current and proposed complexity

Current layout:

- publication: `O(U log_F N + copied blocks)`, bounded by the mutation working
  set and value pack;
- equal-root diff: `O(1)`;
- related sparse diff: `O(changed paths + output)`;
- independently reconstructed, boundary-divergent diff: worst-case
  `O(N + M)`;
- synchronization: `O(nonshared authenticated bytes)`.

Rejected mandatory online canonical packing:

- publication: `O(N)` reads/CPU/allocation/object writes for each publication;
- equal logical state converges to one root and equal-state diff becomes
  `O(1)`;
- it destroys the accepted localized path-copy coefficient.

Preserved seam: an optional future offline canonical snapshot may perform an
explicit `O(N)` rebuild and atomically move the sole selector. It must use the
same object encoding/authority, cannot serve beside the live layout, and needs
new product demand plus a fresh greater-than-10% gate.

## Focused 1K results

All values below are one deterministic release run after setup exclusion where
the row says so. Every cell completed in under one second; the release build and
Clippy cells completed in under 20 minutes.

### Equal logical states

Bulk-sorted compared with independently constructed states:

| history | roots/boundaries equal | shared authenticated bytes | bidirectional sync bytes | Rocks cold diff | Slate cold diff |
|---|---:|---:|---:|---:|---:|
| sorted insert | no/no | 2.6782% | 6,061 / 8,358 | 2,038 gets, 1,326,870 B, 32.564 ms | 2,038 gets, 2,104 physical objects, 1,513,453 B, 31.777 ms |
| random 32-row batches | no/no | 0.7594% | 30,057 / 8,358 | 2,043 gets, 441,050 B, 7.771 ms | 2,043 gets, 2,074 physical objects, 561,572 B, 12.310 ms |
| reverse transactions | no/no | 1.4245% | 15,916 / 8,358 | 2,053 gets, 412,307 B, 7.511 ms | 2,053 gets, 2,080 physical objects, 521,937 B, 12.311 ms |
| adversarial splits | no/no | 0.2310% | 99,351 / 8,358 | 2,050 gets, 332,753 B, 6.302 ms | 2,050 gets, 2,104 physical objects, 519,259 B, 17.424 ms |
| delete/reinsert | no/no | 2.6782% | 7,783 / 8,358 | 2,038 gets, 1,263,516 B, 24.311 ms | 2,038 gets, 2,038 physical objects, 1,296,124 B, 29.407 ms |

Every equal-state diff returned zero rows. Root inequality therefore reflects
physical history, not a semantic mismatch.

### Ordinary real diff

The one-row update from the sorted-insert parent preserved identical leaf and
internal boundaries, shared 83.4901% of authenticated state bytes, and required
1,053 / 966 synchronization bytes. Cold diff returned exactly one row:

| adapter | wall | CPU | allocation | logical reads/bytes | physical reads |
|---|---:|---:|---:|---:|---:|
| RocksDB | 52 us | 52 us | 213,804 B / 1,553 calls | 10 gets / 4,716 B | n/a |
| SlateDB | 103 us | 103 us | 369,553 B / 2,374 calls | 10 gets / 4,716 B | 10 objects / 4,876 B |

This focused case is not generalized to a 95% sharing claim. The independently
accepted broader oracle measured ordinary whole-state sharing at about 40.427%
for its 1K histories and about 85% in prior 50K controls. Those lower figures
remain the honest architecture disclosure. They still preserve changed-path
diff behavior; they do not establish history-independent roots.

### Publication and canonical-rebuild proxy

| history | Rocks wall / alloc / adapter writes | Slate wall / alloc / physical writes |
|---|---:|---:|
| bulk whole-state | 455 us / 676,876 B / 37 puts, 9,882 B | 1.058 ms / 3,100,635 B / 1 object, 9,262 B |
| sorted one transaction | 392 us / 907,805 B / 22 puts, 23,917 B | 460 us / 1,087,139 B / 1 object, 23,528 B |
| random 32-row batches | 5.621 ms / 9,618,905 B / 514 puts, 343,360 B | 7.583 ms / 14,473,045 B / 32 objects, 333,920 B |
| adversarial single-row history | 36.695 ms / 47,410,349 B / 6,963 puts, 2,801,256 B | 79.281 ms / 115,038,589 B / 1,000 objects, 2,647,848 B |
| ordinary one-row path copy | 61 us / 46,902 B / 7 puts, 1,443 B | 1.030 ms / 917,578 B / 1 object, 1,297 B |

Using bulk whole-state construction as the most favorable online canonical
rebuild proxy, the ordinary one-row Rocks publication regresses 7.46x in wall,
14.43x in allocation, 5.29x in put count, and 6.85x in bytes. Slate wall happens
to regress only 2.7% in this single run, but CPU is 2.90x, allocation is 3.38x, and
logical put bytes are 6.85x. Thus a mandatory canonical rebuild fails the
both-adapter no-critical-regression gate even though it could eliminate almost
all cold-diff work for independently reconstructed equal states.

Post-flush database size for the complete multi-history fixture was 3,372,139 B
on RocksDB and 3,898,380 B on SlateDB. These totals include all retained fixture
histories and are not presented as a production disk comparison.

## Widening decision

No new 50K cell was run. The 1K gate already isolates the two causal regimes on
both adapters, and the frozen independent 50K evidence supplies the scaling
point without duplicating work: normal related histories retain materially more
state (about 85%), while adversarial independently reconstructed equal states
can still degrade to full traversal. Earlier key-anchored canonical rebuild
research converged roots but incurred 122x--9,600x publication slowdown and only
3.2% bulk-byte reduction. That corroborates, rather than contradicts, the 1K
rejection.

## Reproduction

From the repository root:

```text
CARGO_TARGET_DIR=/root/repos/target-forktree-history-gate cargo build --release --manifest-path packages/engine-benchmarks/Cargo.toml --bench forktree_history_independence --features 'storage-benches slatedb' -j2
/root/repos/target-forktree-history-gate/release/deps/forktree_history_independence-d088ef373c9fb63e rocksdb 1000
/root/repos/target-forktree-history-gate/release/deps/forktree_history_independence-d088ef373c9fb63e slatedb 1000
CARGO_TARGET_DIR=/root/repos/target-forktree-history-gate cargo clippy --release --manifest-path packages/engine-benchmarks/Cargo.toml --bench forktree_history_independence --features 'storage-benches slatedb' -- -D warnings
cargo fmt --all -- --check
git diff --check
```

Frozen local evidence before commit:

- release binary SHA-256:
  `716782a1f073de6c0b7baf7625eac702aeb14fb7ed7cd9c9352f3af15bd6afc6`
- RocksDB log SHA-256:
  `3879acf61e4f83fdc3839e470a0ddfe942098810b4805fc43b7462f5ad99fdbd`
- SlateDB log SHA-256:
  `e09aec52c1f254552bfaccad29f7d043aa5976e3cfa5db553d458ad1828f2f4b`
