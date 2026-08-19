# Typed-row plugin benchmark contract

This benchmark is the reproducible before/after qualification for the plugin
typed-row hard cut. The baseline is the immutable commit
`89aea5d55773586ea60f77c1d9dddcfc8b394dd1`; the candidate is the current
worktree. The runner rejects any other resolved baseline revision.

## Pinned workload

The corpus manifest is
[`benchmarks/plugin_api_corpus.json`](benchmarks/plugin_api_corpus.json). It
pins the generator revision and seed, fixture bytes and SHA-256 digests, exact
lane inventory, 5 default warmups, declared lane exceptions, and 61 measured
samples. [`benchmarks/plugin_api_qualification.json`](benchmarks/plugin_api_qualification.json)
pins the timed harness, normalized contract, runner and runner tests, plan,
Rust toolchain, lockfiles, baseline, exact adapter patch digests, and the
qualification host architecture, CPU model/governor, and rustc commit. A run
outside those environment pins is rejected before either revision executes. Both
revisions run a byte-identical timed harness. A narrow baseline adapter exposes
existing counters, omits candidate-only counter fields after the timed scope,
derives the historical row-page callback count from its one callback per input
or output page, enables Wasmtime perf-map symbols only during profiling, and
removes the post-scope typed-attachment assertion that cannot hold on the JSON
baseline. The report records every adapter edit and resulting digest.

The 19 lanes cover:

| Plugin | Parse/round trip | Incremental update | Direct row mutation | Merge | Scale/special case |
| --- | ---: | ---: | ---: | ---: | --- |
| CSV | yes | yes | generated-id create/update/delete | yes | quoted multiline rows |
| JSON | yes | yes | existing-scalar update | n/a | nested values + 10 MiB paged JSONB |
| Markdown | yes | yes | create/update/delete | yes | structured rich-inline document |
| Text | yes | yes | create/update/delete | n/a | Unicode + 336 KiB attachment |
| Excalidraw | yes | yes | create/update/delete | n/a | scene/elements/app state/files |

Every workflow checks exact bytes or semantic output. The CSV, Markdown, Text,
and Excalidraw direct-mutation lanes create and semantically verify a row,
commit an update and verify it, delete it, and verify both the rendered file
and row absence. JSON's schema models structural identity from the document
tree and intentionally rejects SQL-created/deleted members, so its direct lane
updates an existing native JSONB scalar and verifies the rendered value. The large
JSON lane asserts multiple typed output pages; the text attachment lane
asserts attachment-table engagement. CSV direct mutation omits its UUID primary
key on create and proves generated identities remain native through SQL and
transaction staging.

## Measurements and process control

Each lane/sample pair runs in a fresh process pinned to one CPU. Pair order
alternates baseline/candidate and candidate/baseline (AB/BA), preventing a
fixed revision-order or process-high-water bias. Release binaries are built
before sampling. `sched_getaffinity` and `taskset` are mandatory rather than
best-effort, and the paired schedule aborts both revisions and all remaining
lanes immediately after the first build or measurement failure.

The report contains p50 and p95 elapsed time, allocated bytes, allocation
count, peak live bytes, scope-boundary current RSS and RSS delta (never the
process-lifetime `VmHWM`), guest linear-memory high-water reset by each guest invocation,
component boundary bytes, typed wire bytes, callback/page counts, attachment
counts, and phase timings. Exact lane/sample pairs and correctness hashes are
required on both revisions. Candidate transition profiles are required for
all 19 lanes and all eight outer-row JSON call/byte counters must be present,
nonnegative, and zero in every sample. Required metric/counter matrices reject
missing or malformed evidence; phases must account for total measured time.
Live-byte delta, large-allocation count, RSS end, and RSS delta are regression
gates rather than presence-only diagnostics. Input pages, output pages, and
terminal input-source callbacks must exactly equal the recorded callback count,
including merge lanes. Every plugin-owned transaction row passes one ingress
guard before certified or scalar normalization. The guard rejects JSON payloads
and records the actual forbidden DOM-fallback bytes; the native wire codec is
also audited to contain no outer snapshot JSON codec. Positive-control tests
exercise this real rejection path, all four counter classes, and the report's
non-zero rejection, so producer-free counters cannot qualify as evidence.

Performance gates preserve baseline/candidate pairing and use deterministic
5,000-resample bootstrap confidence intervals around p50 and p95 ratios. A
comparison passes only when the upper 95% bound is inside the material
regression envelope. Tail gates require all 61 paired observations; zero
baselines pass only when both revisions are exactly zero.

CPU profiles are collected with pinned `samply 0.13.1` at 1000 Hz for every
lane on both revisions (38 presymbolicated artifacts), so any gated regression
has a directly corresponding profile. An
explicit post-warmup barrier brackets one measured workload. The runner trims
the profile to the recorded go/done monotonic-clock interval, excluding attach
polling, hashing, record emission, summaries, and teardown; artifacts must
retain at least 25 in-scope profiler samples, including at least five samples
with a guest frame for sustained guest-transition lanes. Direct row mutation
requires at least three guest-bearing samples. Normal lanes execute 101 profile
iterations; both short merge lanes execute 255.
Baseline/candidate profile order
alternates by plugin. Qualification validates the compressed profile JSON,
symbol sidecar, their SHA-256 digests, sidecar-resolved sampled RVAs, sampled
guest coverage, sample records,
exact command, affinity, and pinned environment. Report replay reopens and
rehashes all 38 artifacts, while resumed collection requires the candidate
HEAD and complete working-tree digest to match the measured checkpoint.
Profile failures still produce a failed final report. The machine and Markdown
reports identify both the largest measured candidate phase and the top sampled
candidate CPU leaf for every plugin.

## Reproducible qualification

```text
PYTHONDONTWRITEBYTECODE=1 python3 \
  packages/e2e/benchmarks/plugin_api_benchmark.py run \
  --root . \
  --output /tmp/lix-plugin-api-ab \
  --baseline 89aea5d55773586ea60f77c1d9dddcfc8b394dd1 \
  --samples 61 \
  --warmups 5 \
  --require-baseline
```

Artifacts include metadata, raw baseline/candidate logs, JSONL records,
`report.json`, `PLUGIN_TYPED_ROW_BENCHMARK_REPORT.md`, and baseline/candidate
CPU profiles under `cpu-profiles/`. The `report` subcommand reads only the
digest-verified JSONL records and digest-verified CPU profiles; raw logs are
diagnostic and cannot change a replayed result.
The run fails for an unavailable or mismatched baseline, missing pairs,
correctness drift, incomplete CPU profiles, non-zero outer-row JSON activity,
or a material metric/phase regression.

The focused runner tests are:

```text
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest \
  packages/e2e/benchmarks/test_plugin_api_benchmark.py
```

CI publishes the generated Markdown to the job summary and uploads the full
artifact with missing files treated as an error. Final measured results can be
checked in from that generated report after qualification succeeds.
