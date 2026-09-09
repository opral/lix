# Certified HOT state performance contract

Sync clients serve current state locally, including durable pending writes.
The authority-certified receipt remains separate from visible local heads.
Certification and synchronization run in the background; local write completion
does not wait for either. The bounds below describe certified bootstrap and
background publication. See [local-first sync](./local-first-sync.md) for the
foreground contract and the matched performance profile.

Historical reads hydrate immutable data on demand and retain it in local
storage. They are separate from the current-state interaction path.

## Cost model

Let:

- `N` be the live rows in the published branch head;
- `C` be distinct live rows in its latest-checkpoint baseline;
- `D` be identities changed since that checkpoint;
- `E` be repository events in one received delta;
- `U` be the unapplied suffix of an overlapping delta (`U <= E`);
- `B` be published branch descriptors;
- `H` be cold commit-history depth; and
- `P` be transferred snapshot and metadata bytes;
- `M` be the distinct rows transferred by bootstrap; and
- `Q` be distinct checkpoint snapshots certified during bootstrap.
- `S` be the number of file-backed schemas inspected by an exact file read;
- `A_f` be the live tracked atoms belonging to the selected file; and
- `P_f` be the selected file's before/after payload bytes.

The intended bounds are:

| Operation | Time / transfer | Peak owned memory |
| --- | --- | --- |
| Certified bootstrap | `O(M log M + (B + Q)M)`, independent of `H` | `O(P + M)` |
| Certified live delta | `O(E)` cursor admission plus `O(N log N)` per distinct head/checkpoint value root in the unapplied suffix today | `O(N)` transient keys/digests; overlapping-prefix trimming is a borrowed slice and adds `O(1)` memory |
| Working-diff identity scan | `O(D log D)` after the certified index is installed | `O(D)` |
| Selected working-file payload | `O(S + A_f log A_f + P_f)` via exact HOT file-ID pushdown | `O(A_f + P_f)` transient rows and payload copies |
| Working file rendering | `O(D log F + F * h)` for `F` changed files and directory depth `h` | `O(D + F + directories)` |
| Historical point/diff read | Cold: transfer the missing dependency closure; warm: local query cost | Fetched immutable history persists locally |
| Connected freshness barrier | One finite authority round trip and `O(1)` metadata when its private cursor is certified. When behind: `O(B + Δ)` transfer, authority `O((B + Q)M log M)` snapshot-root recertification, and client `O(N log N)` per distinct changed root | Authority `O(max branch live set)` plus client `O(B + page + N)` during changed-root certification |

Payload bytes are part of these bounds. A result containing `K` one-megabyte
rows cannot use `O(K)` bytes; it uses `O(K MiB)`. Content-addressed sharing may
reduce the constant but is not required for correctness.

Bootstrap may page network responses, but paging alone is not a memory bound.
A conforming implementation must avoid retaining multiple full decoded copies
of all pages or rescanning all rows once per branch. Branch/checkpoint roots,
the installed rows, the working-diff epoch, and the repository cursor are one
atomic publication: the old certified generation remains visible until all of
the new generation is verified and durable.

The `O(N log N)` live-value-root build is the main remaining optimization
target. It is correctness-complete and bounded independently of cold history
`H`, but an incrementally maintained authenticated root would reduce
steady-state delta certification to the changed-row frontier.

Overlapping pulls from two handles sharing one durable browser store never
replay already-published events. Admission validates all `E` cursors, borrows
the `U`-event unapplied suffix without copying it, and retries only after the
durable receipt CAS proves that another publisher made progress. The normal
single-worker path therefore adds one linear cursor scan and no prefix-sized
allocation; contention work is proportional to successful receipt advances.

## Regression evidence

`sync_mode::certified_hot_state_profile_scorecard` runs real in-process HTTP
sync against shallow-history, deep-history, and wider-row fixtures. It asserts:

- identical bootstrap page and topology-request counts at equal `N`, `C`, `D`,
  and `B` when only `H` changes;
- exact replica current-row and working-diff cardinality;
- zero cold-history requests from the no-endpoint working-diff query; and
- a measured one-megabyte exact current-file content read, including its allocation scope;
- bounded allocation before and after checkpoint retirement of retained
  net-zero working tombstones; and
- generous allocation high-water envelopes that catch super-linear growth
  without treating allocator/RSS noise as a latency benchmark.

The regular connected-API test separately asserts zero finite publication
pulls for certified current reads and verifies server-first coherent reads.

Run the focused scorecard with:

```sh
LIX_HOT_STATE_PROFILE_OUTPUT="$PWD/target/hot-state-profile.json" \
  cargo +nightly-2026-05-21 -Z bindeps test \
  --manifest-path tooling/Cargo.toml \
  -p lix_e2e --features sdk-tests,server-protocol \
  --test sync_mode certified_hot_state_profile_scorecard \
  -- --ignored --exact --nocapture
```

The 2026-08-31 reference run against this implementation passed every
cardinality, request-count, and allocator-growth assertion:

| Case | Live / dirty / history rows | Bootstrap allocated / peak-live bytes | Working diff allocated / peak-live bytes | History requests from working diff |
| --- | ---: | ---: | ---: | ---: |
| Shallow history | 256 / 32 / 2 | 198,205,063 / 10,988,033 | 864,162 / 212,656 | 0 |
| Deep history | 256 / 32 / 64 | 198,219,881 / 11,052,505 | 840,979 / 212,656 | 0 |
| Wide rows | 768 / 96 / 2 | 551,364,548 / 28,820,964 | 1,141,113 / 229,151 | 0 |

Shallow and deep history used the same six snapshot-row pulls and four
topology/history endpoint calls during bootstrap despite a 32x increase in
cold history depth. Timings and RSS are intentionally omitted from the
contract because they are machine- and allocator-dependent.

The exact selected-file probe returned a 1,048,576-byte current `content` value in
all three cases. It allocated 18,491,287 / 18,240,350 / 28,992,135 bytes with
3,726,576 / 3,508,440 / 4,885,244 peak-live bytes for shallow / deep / wide,
respectively. Certified current reads and one-argument working diffs use
payloads installed before publication, so they issue no foreground chunk or
history requests. Explicit historical reads use local `lix_as_of` and multi-argument
`lix_diff` surfaces, hydrating missing history and retaining it for reuse.
The reference measurements below predate local-first sync and describe the
former authority-routing implementation.

For 128 retained net-zero tombstones, checkpoint retirement kept the working
diff at zero rows and reduced the probe from 1,268,863 to 1,058,794 allocated
bytes and from 334,458 to 250,675 peak-live bytes.

The artifact schema is `lix.certified-hot-state-profile-artifact.v1`. Each case
records its dimensions, elapsed time, allocation count/bytes, peak live bytes,
RSS at the scope boundaries, snapshot page requests, and history requests.
Elapsed time and RSS are diagnostic. Exact request/cardinality assertions and
the allocation growth envelopes are the portable regression gates.

For storage-adapter latency profiles, use the existing
`tracked_working_diff`, `working_diff_file_scope`, and
`checkpoint_history_scale` benches. Compare matched closed fixtures and
backend configurations; do not compare a warmed candidate with a cold
baseline.
