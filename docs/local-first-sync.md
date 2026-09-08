# Local-first sync

Sync mode serves a durable local replica. `execute` and explicit transaction
commit resolve after local durability, and observations see that commit before
server acceptance. Remote mode retains server-acknowledged writes.

## State and reconciliation

The durable replica receipt records confirmed server heads, checkpoints, roots,
and cursor. Visible branch controls may advance beyond that receipt. Native
immutable commits and those controls form the upload outbox; there is no second
application mutation language or copy of the write logic.

The worker uploads dependency-closed bounded batches. Immutable commit identity
and expected head/checkpoint coordinates make a lost response retryable. A
server acknowledgment advances the receipt without overwriting a newer local
descendant, including compact checkpoint source dependencies.

Scoped checkpoints also record their captured local head in a private,
fixed-size record written atomically with the checkpoint. This lets upload
admission recognize local progress when checkpoint compaction removes that
head from canonical ancestry. The record is not a history parent or wire state
alias, and garbage collection removes it with its checkpoint commit.

An incompatible server ref move discards all pending state in that replica and
restores its confirmed server coordinates atomically. This includes pending
work on other branches: checkpoints also write a global catalog, and schema
changes can be dependencies of multiple branches. The conservative reset avoids
uploading an orphan checkpoint or a discarded dependency. Own acknowledgments
preserve newer local descendants. Permanent write rejections also reset pending
state; temporary transport errors and credential-refresh failures retain it.
Sync never automatically merges divergent heads. This is server-wins, not
wall-clock last-write-wins.

A reset publishes corrected refs and receipt atomically and preserves immutable
cached history. Garbage collection retains both the confirmed baseline and
pending dependency closure so offline checkpoints cannot erase recovery inputs.

## Reads, history, and exports

Current rows and the working checkpoint baseline are installed at bootstrap.
Ordinary writes and reads execute against the embedded engine. Historical reads
hydrate missing immutable boundaries and blobs lazily into durable storage.
Coherent mixed current/history batches retry the whole captured read after
hydration. Explicit transactions require historical inputs to be prefetched
before opening their fixed snapshot.

Current working-diff reads preserve storage snapshot-expiration errors through
the HOT index provider, so concurrent publication retries the complete read
batch instead of reporting a falsely unavailable index.

Warm reopen uses local state and reconnects in the background. Close cancels
network work; it does not await delivery. Canonical snapshot export opens a
short-lived authority session on demand and exports accepted server state,
which may not yet contain pending local commits.

## Complexity and profiling

Let N be working-set rows, H cold history depth, P pending commits, and E pending
dependency edges. Foreground operations pay their existing local engine cost,
without a server round trip or a foreground certification scan. Full checkpoint
publication aliases captured state; partial checkpoints still process the
selected dependency closure. Folder moves retain filesystem validation costs.
Checkpoint sync provenance adds one constant-size metadata write per checkpoint,
without rewriting the pending checkpoint queue. Background pending walks load
these source records by commit ID as needed.

The worker captures a finite upload wave using commit IDs and dependency
metadata, orders it once, and decodes payloads only for the selected page. It
retains the disposable in-memory plan across pages and advances it only after
the server acknowledgment has been imported durably. Ordinary appends and
checkpoints join the next wave, so continuous editing cannot indefinitely delay
publication of the captured refs. Restarting the worker reconstructs a plan from
the durable replica; there is no additional durable outbox.

Dependency ordering costs O((P + E) log P); the retained plan holds O(P + B)
IDs and refs, where B is branch count. Payload loading is proportional to the
commits sent rather than the sum of the remaining queues across pages. Initial
ancestry checks can still revisit overlapping branch closures, costing up to
O(B * (P + E)); each page also decodes the receipt and compares authority
coordinates. Thus this removes repeated queue planning, not every branch-count
or acknowledgment-frontier cost. Traversal stops at confirmed boundaries rather
than loading cold history. At most four upload batches run before incoming
demands and a finite pull receive service.

Destructive ref changes rotate an atomically written invalidation token. A
fresh read checks that token and server coordinates before loading each page;
ref preparation additionally guards current reset intent and persists exact
request proofs with a compare-and-swap. Restores, incompatible server updates,
and missing garbage-collected bodies discard the cache. No storage snapshot is
held across the network. Lost responses retry unacknowledged work, and a request
size rejection selects a smaller prefix without advancing the plan.

Prepared-ref proofs retain at most 64 distinct targets per branch and source
coordinate. Repeated ordinary retries reuse the same target. Exceptionally,
64 distinct preparations without an authority advance can exhaust that bound;
a further distinct target returns an error rather than forgetting a possibly
in-flight acknowledgment. Automatic recovery for this exceptional case is not
implemented.

Checkpoint source acknowledgments are cleared once every branch has converged
and no restore is pending. This uses an O(B) control scan guarded against
concurrent branch creation and mutation. Body-only acknowledgments retain source
boundaries needed by other pending branches. A wide pending graph can still
require a wide acknowledgment frontier before convergence.

Ordinary ref deltas load only their affected branch controls and perform no
branch scans. Incompatible-state resets inspect O(B) branch metadata to guard
atomic client resets, where B is the local branch count. An exceptional reset
with removed local rows rebuilds the affected current-state generation in O(N).

Certified changed-root publication retains the current O(N log N) bound. Warm
cached history queries perform no repeated history transfer; cold reads pay for
the required immutable closure, not an unconditional repository-history load.

The ignored `local_first_foreground_profile_scorecard` in `sync_mode.rs` uses
matched 32/256-row fixtures with 0/100 ms injected network RTT, recording latency,
allocations for reads, writes, folder moves, and checkpoints.
The profile isolates foreground completion; background convergence is checked
separately. Timings are diagnostics, while offline correctness and request-count
assertions are regression gates.

### Measured foreground comparison

Same-host measurements against main `56bd1a061` and this implementation, using
identical filesystem-replica/in-memory-authority fixtures with 100 ms injected
HTTP RTT:

| Operation | Main, 32 rows | Local-first, 32 rows | Main, 256 rows | Local-first, 256 rows |
| --- | ---: | ---: | ---: | ---: |
| Current point read | 0.91 ms | 0.82 ms | 0.86 ms | 0.88 ms |
| Current write | 313.80 ms | 1.16 ms | 353.82 ms | 1.40 ms |
| Partial checkpoint | 333.09 ms | 2.18 ms | 391.13 ms | 2.44 ms |
| Folder move | 329.79 ms | 2.19 ms | 405.84 ms | 4.46 ms |
| Full checkpoint | 332.05 ms | 1.01 ms | 390.21 ms | 1.06 ms |

Main pays approximately three network round trips plus publication work.
Local-first completion does not wait for those requests. At 256 rows, measured
full-checkpoint allocation fell from 274.55 MB to 1.18 MB; current-write
allocation fell from 248.55 MB to 1.44 MB. Folder-move allocation still grows
with the filesystem working set (3.25 MB at 32 rows, 7.57 MB at 256 rows), so
these results do not claim constant-time folder validation.

These are single-operation diagnostic samples from Cargo's test profile, not
release-build percentile promises. The allocator scope is process-global and
can include the background worker and in-process authority. Compare latency
and allocation only with these matched fixtures; offline tests establish the
absence of a required successful foreground network request independently of
machine timing. The measured profile does not claim background drain or
server reconciliation has constant cost.

Reproduce from the repository root:

```sh
LIX_LOCAL_FIRST_PROFILE_OUTPUT=/tmp/local-first-profile.json \
  cargo +nightly-2026-05-21 test --manifest-path tooling/Cargo.toml \
  -p lix_e2e --features sdk-tests,server-protocol --test sync_mode \
  local_first_foreground_profile_scorecard -- --ignored --exact --nocapture
```

The JSON artifact uses `lix.local-first-foreground-profile.v1` and records all
32/256-row and 0/100 ms RTT cases. Run the same harness on the baseline checkout
for a comparison; do not compare unrelated older benchmark binaries.


### Large offline queues

The ignored `upload_plan_scaling_profile` compares three implementations using
512, 2,048, and 8,192 pending commits, 1 KiB values written to one key, and a
512-item upload limit. The baseline is main `37cd4904a`; page-only mode rebuilds
metadata each page while decoding only selected payloads; retained mode is the
production worker behavior in this change.

Results use Cargo's unoptimized test profile and in-memory replica/authority
storage with no injected network latency. Planning excludes fixture writes,
authority processing, and receipt import; catch-up includes those upload and
import steps and harness bookkeeping/frontier sampling, but excludes fixture
construction. These are single-run diagnostics,
not production percentile guarantees. The [recorded counters](benchmarks/sync-upload-planning.json)
include all three modes and checkpoint summaries.

| Pending commits | Main planning | Page-only planning | Retained planning | Main catch-up | Retained catch-up |
| --- | ---: | ---: | ---: | ---: | ---: |
| 512 | 94 ms | 88 ms | 81 ms | 493 ms | 464 ms |
| 2,048 | 828 ms | 493 ms | 310 ms | 2,436 ms | 1,802 ms |
| 8,192 | 11,328 ms | 4,594 ms | 1,281 ms | 18,031 ms | 7,380 ms |

At 8,192 commits, payload-load calls fall from 69,632 to 8,192; commit-header
point reads fall from 278,528 to 24,576; encoded storage bytes returned during
planning fall from 282.6 MB to 27.7 MB. Payload loads and header-read counts have
non-timing regression assertions. Returned storage bytes measure backend reads,
not a census of decoded bytes or wire traffic. The counters show why retaining
the plan earns its extra complexity beyond loading only the selected page.

A separate 32-wave checkpoint profile uses four edits and one full/scoped
checkpoint per wave, with two-item pages. On main, full checkpoints leave 32
source acknowledgment entries after convergence (maximum 34 while uploading).
The retained implementation finishes each wave with zero entries (maximum 3).
Scoped checkpoints finish with zero in both versions (maximum 2). The cleanup
therefore addresses full-checkpoint aliases without removing boundaries that a
pending sibling branch still needs.

The harness also records whole-process Linux `VmHWM`. That includes fixtures,
the in-process authority, and earlier cases in the same process; it does not
isolate planner peak memory. The retained-plan memory bound is established by
its ID/ref-only representation, not by interpreting process RSS as cache size.

Reproduce the candidate from the repository root:

```sh
LIX_UPLOAD_PROFILE_SIZES=512,2048,8192 LIX_UPLOAD_PROFILE_CACHED=1 \
  cargo test -p lix --all-features --lib upload_ -- \
  --ignored --nocapture --test-threads=1
```

Set `LIX_UPLOAD_PROFILE_CACHED=0` for the page-only comparator. To reproduce
main, port `upload_plan_profile_tests.rs` and `upload_metrics.rs` to that
checkout, wiring their test-only module/include and the counter at the start of
`load_sync_commit`. Adapt the harness drain to call only the original
`build_sync_push`: remove retained-mode/cleanup calls and the candidate-only
linear-read regression. Keep the baseline production planner unchanged.
Run builds and profiles sequentially
when sharing a Cargo target directory. Frontier and timing JSON is printed as
`UPLOAD_FRONTIER_PROFILE` and `UPLOAD_PLAN_PROFILE`, respectively.
