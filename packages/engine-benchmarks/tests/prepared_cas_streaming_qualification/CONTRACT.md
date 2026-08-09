# Prepared-CAS streaming qualification contract

This is a test/report-only contract for Hetzner-I's engine-level prepared-CAS
streaming correction. It is based on immutable
`8ce131acf3690eaf48dc5722cd2f141b53c62572`. The direct successor registers a
public adapter suite and adds only feature-gated scalar observability hooks at
the existing owner boundaries; default production builds do not compile those
hooks and no runtime authority is changed.

This successor exposes scalar counters at the private prepared-receipt owner
boundary behind the `prepared-cas-observability` feature. The module is absent
from default builds, and the counters retain no payload, object ID, selector,
root, or cache state:

| Counter | Required bound/meaning |
| --- | --- |
| `file_content_writes_payload_bytes` | zero payload bytes after receipt lowering |
| `peak_file_content_writes_bytes` | receipt/object metadata plus one bounded chunk/page |
| `peak_transaction_retained_payload_bytes` | `O(page + receipt metadata)`, independent of total payload |
| prepared receipt/object totals | exact accounting, not a second authority |
| semantic marker count | exactly one |
| semantic commit count | exactly one on success, zero on rollback |

The model uses 65 one-MiB logical files and page sizes 1, 8, 32, and 64.
Inputs are reversed while authenticated publication order is canonical. It
checks identical tree/plugin/semantic digests, rollback, orphan reclamation,
owner/manifest/chunk/size/digest/view corruption, duplicate receipts, and
simulated cold reopen for Memory, RocksDB, and SlateDB.

Required durable sequence, each bounded to 20 minutes and stopped on the first
failure:

1. Run the source contract and standalone model.
2. Run the public receipt semantic suite on Memory, including cold reopen.
3. Run the same fixture on RocksDB, flush/drop/reopen, then SlateDB,
   flush/drop/reopen.
4. Inject every authenticated corruption case after preparation and verify no
   visible row, selector, marker, or commit; reclaim all orphan objects.
5. Compare exact tree/plugin/semantic digests and backend counters.

A candidate that reports growing
`file_content_writes` payload bytes across pages is a hard BLOCKER even when
the marker and publication remain atomic.

The adapter runner is
`run_adapter_qualification.sh CANDIDATE_ROOT TARGET_DIR RESULTS_DIR`. It runs
exactly three 1,200-second cells, in order: Memory, RocksDB, and SlateDB. Each
implementation test emits an observable semantic TSV. The public validator
rejects missing adapters, wrong row digests, missing marker/commit, visible
rollback state, unbounded retained payload, nonzero final transaction payload,
missing reachability/orphan/reclamation accounting, and missing corruption
rejection accounting. The standalone model remains available as an
independent semantic control.
