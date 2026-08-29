# Certified HOT state performance contract

Connected replicas have one serving plane: the authority-certified current
state and the latest-checkpoint baseline needed for Working Changes. Arbitrary
commit history is a server read and is not allowed to enter the working-diff
critical path.

## Cost model

Let:

- `N` be the live rows in the published branch head;
- `C` be distinct live rows in its latest-checkpoint baseline;
- `D` be identities changed since that checkpoint;
- `B` be published branch descriptors;
- `H` be cold commit-history depth; and
- `P` be transferred snapshot and metadata bytes.

The intended bounds are:

| Operation | Time / transfer | Peak owned memory |
| --- | --- | --- |
| Certified bootstrap | `O(N + C + B)`, independent of `H` | `O(P + D)` |
| Certified live delta | `O(N)` per distinct head/checkpoint root today | `O(N)` transient rows |
| Working-diff identity scan | `O(D)` after the certified index is installed | `O(D)` |
| Working file rendering | `O(D log F + F * h)` for `F` changed files and directory depth `h` | `O(D + F + directories)` |
| Historical point/diff read | `O(server result/page)` | `O(server result/page)` with no unbounded replica growth |
| Authority mutation catch-up | `O(R)` coordinate probes over `R` pull/poll attempts | `O(1)` excluding transport buffers |

Payload bytes are part of these bounds. A result containing `K` one-megabyte
rows cannot use `O(K)` bytes; it uses `O(K MiB)`. Content-addressed sharing may
reduce the constant but is not required for correctness.

Bootstrap may page network responses, but paging alone is not a memory bound.
A conforming implementation must avoid retaining multiple full decoded copies
of all pages or rescanning all rows once per branch. Branch/checkpoint roots,
the installed rows, the working-diff epoch, and the repository cursor are one
atomic publication: the old certified generation remains visible until all of
the new generation is verified and durable.

The `O(N)` live-root row scan is the main remaining optimization target. It is
correctness-complete and bounded independently of cold history `H`, but an
incrementally maintained authenticated root would reduce steady-state delta
certification to the changed-row frontier.

## Regression evidence

`sync_mode::certified_hot_state_profile_scorecard` runs real in-process HTTP
sync against shallow-history, deep-history, and wider-row fixtures. It asserts:

- identical bootstrap page and topology-request counts at equal `N`, `C`, `D`,
  and `B` when only `H` changes;
- exact replica current-row and working-diff cardinality;
- zero cold-history requests from the no-endpoint working-diff query; and
- generous allocation high-water envelopes that catch super-linear growth
  without treating allocator/RSS noise as a latency benchmark.

Run the focused scorecard with:

```sh
LIX_HOT_STATE_PROFILE_OUTPUT=target/hot-state-profile.json \
  cargo +nightly-2026-05-21 -Z bindeps test \
  --manifest-path tooling/Cargo.toml \
  -p lix_e2e --features sdk-tests,server-protocol \
  --test sync_mode certified_hot_state_profile_scorecard \
  -- --ignored --exact --nocapture
```

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
