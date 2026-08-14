# EXP-COMMIT-PAGE-ROUTING-87F — NO-CUT

## Identity and decision

- Parent: `87f6059a2aa243bc371c035701b61ee7f95369a4`
- Parent tree: `93910f6fb8bfe0bbf2f4702f192d7b1f2108751e`
- Decision: **NO-CUT / DO NOT COMPOSE**

The requested canonical StateKey-range directory cannot index the unchanged
CommitChangePage V3 representation. V3 pages preserve member/result-slot
ordinal order, not StateKey order. At 50,000 rows with 500 changed members the
modeled ranges overlap; at 100,000/1,000 they overlap as well. Making them
non-overlapping would reorder or duplicate the page payload, which is outside
the experiment contract.

The frozen source also contains a stronger count-directory prototype. It adds
one authenticated `u32` cardinality beside each existing page ObjectId. An
exact current-pack page back-edge can then prove the selected page's global
ordinal without decoding every preceding page. This preserves page bytes,
member order, one commit authority, and full-key/page authentication.

The count variant projects a large read-count reduction but is rejected because
that projection could not be qualified on the pinned parent's real RocksDB and
SlateDB VCS path. It must not be promoted from model evidence.

## Smallest crossover

Fixed geometry: 270 members/page and 64 KiB/page. Dimensions were
`N=1K/10K/50K/100K`, `D=1/10/1%`, `H=10/100/1K/10K`, and 32/64-byte keys.

| N | D | old prefix pages | count-directory pages | projected reduction |
|---:|---:|---:|---:|---:|
| 1K | 1 | 4 | 1 | 75.00% |
| 10K | 1 | 21 | 1 | 95.24% |
| 10K | 10 | 38 | 10 | 73.68% |
| 50K | 1 | 95 | 1 | 98.95% |
| 50K | 10 | 178 | 10 | 94.38% |
| 100K | 1 | 187 | 1 | 99.47% |
| 100K | 10 | 354 | 10 | 97.18% |
| 50K | 1% | 186 | 186 | 0.00% |
| 100K | 1% | 371 | 371 | 0.00% |

Authenticated root overhead is 4 bytes/page, or 0.0061% relative to 64 KiB
pages. Full history still loads and validates every page and its aligned count.
The selected-page path fails closed on missing/malformed/wrong-commit pages,
ordinal or count substitution, truncated count vectors, zero counts, overflow,
and duplicate page ObjectIds.

## Gates and qualification boundary

- `cargo fmt --all -- --check`: PASS
- `git diff --check`: PASS
- `cargo check -p lix --lib --all-features`: PASS
- Model assertions and CSV generation: PASS
- Test-aware compile: inherited RED; none of its final 16 errors references the
  new count field. The pinned parent has missing CSV `include_str!` fixtures and
  unrelated stale plugin/SQL/schema/history test APIs.
- Real VCS benchmark: infrastructure/source-frontier RED. The parent's
  `tracked_working_diff` harness imports deleted `lix::tracked_state`, deleted
  `diff_tracked_commits_for_bench`, and old `open_another_session` APIs. Repairing
  it would widen this experiment.
- RocksDB/SlateDB end-to-end p50/p95: **UNQUALIFIED / UNRUN**.

Evidence hashes:

- model source: `12779e3e68a685421f3a2e40e2d83b823ed36c308f9b40396441e6aa0b25d280`
- model executable: `724d154f4333006a6b6eadfa3590126d7e529ce1ccb34b4a7b8f2e1e8ae6094a`
- model CSV: `3b4bfaa1753ab354f8a2cbe0b397b0ea9eb4acc583aa23a8aeee253073ee271a`
- all-features library check log: `775c45fe60cbb8888a4ba2a45d59cd3862adfb48cf364e60fa307e95fd927f6b`
- test-aware compile log: `3a1728159b09863f634f3a183e8db60f030c93205e9f42d8f7e5a794d3ad9347`
- real-path harness build log: `5816e8f52ef3fb35147ac5dff388cfe95739d648eec17306314510d144a05ff7`

No reviewer was spawned because the experiment did not meet the required
RocksDB and SlateDB end-to-end win threshold.
