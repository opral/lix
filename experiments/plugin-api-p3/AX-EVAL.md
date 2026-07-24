# Preview 3 raw-WIT authorship evaluation

This exploratory AX cohort asks whether an agent can implement the JSON plugin
directly against [`wit/plugin.wit`](wit/plugin.wit), without an SDK facade or
authorship documentation.

The retained cohort result is
[`ax-eval/2026-07-23-p3-raw-n3-result.json`](ax-eval/2026-07-23-p3-raw-n3-result.json).
Full runner transcripts remain in the machine-local AX result directory.

## Cohort

- Date: July 23, 2026
- Model: `gpt-5.4`
- Agents: 3
- Documentation: none
- Starter: isolated WIT plus minimal Rust guest scaffold
- Task: parse top-level JSON properties, stream cold-open entities, and handle
  one equal-length property-value edit

All three implementations passed the compile-oriented independent success
judge and built for `wasm32-wasip2`.

| Agent | Duration | Tool calls | Errors | Final score |
|---|---:|---:|---:|---:|
| 1 | 288.6 s | 40 | 3 | 64 |
| 2 | 282.3 s | 41 | 3 | 64 |
| 3 | 288.0 s | 51 | 8 | 55 |

The success rate was 100%. Median final score was 64, with p25/p75 of
59.5/64. Median duration was 288.0 seconds and median tool count was 41. All
three efficiency scores were zero. The rubric's 100 friction score records
zero human interruptions; it does not mean the bindings caused no authorship
friction.

## What the cohort says

The raw API is implementable, but this cohort does not support calling it
intuitive yet. Before their first code edit, the agents made 35, 18, and 43
shell calls. Of their later shell commands, 23/38, 21/38, and 33/49 inspected
`wit-bindgen`, generated output, Cargo target artifacts, macro expansion, or
registry/runtime source.

All three had to discover generated module and trait names and learn the
concrete `wit_stream`, `wit_future`, producer-task, and terminal-future APIs.
One first implementation produced 14 compiler errors involving the host
namespace, resource handles, stream construction, and future result shape.
The stream-plus-terminal-future contract was the most visible source of
boilerplate and recovery work.

WIT itself still provides the valuable stable boundary: typed resources,
ownership, async functions, streams, futures, and generated host/guest
bindings. The friction is primarily in the current Rust binding surface and
in protocol semantics that WIT types alone do not specify, such as entity
identity, hash/range rules, duplicate keys, revisions, and immutable snapshot
behavior. Successful agents selected different hash contracts, demonstrating
that compile success alone does not establish semantic interoperability.

## Recommendation

Do not add a broad SDK solely from this N=3 result. First add:

1. one canonical guest example showing imports, `export!`, stream/future
   construction, producer spawning, backpressure, cancellation, and terminal
   completion;
2. a small generated transport helper for producing the
   `entity-summary-stream`; and
3. explicit semantic documentation for identity, ranges, hashes, duplicates,
   revisions, and snapshot ownership, backed by host contract tests for
   malformed/truncated streams and JSON semantics.

Then run a paired N=10 cohort using the same model, scaffold, prompt, and
rubric across raw v2, raw P3, and P3 plus the helper. Compare build success,
duration, tool calls, errors, and final score before deciding whether a larger
SDK earns its maintenance cost.

## Limitations

This was intentionally an early N=3 signal, not the default N=10 gate. The
runner used Codex `gpt-5.4`, built-in system instructions, and shell plus
`apply_patch`; the preferred Claude model and exact tool/system controls were
not available. The starter, prompt, and model also differ from the earlier
production-v2 evaluation, so their numeric scores are not directly
comparable. Cargo artifacts were shared between otherwise isolated worktrees,
and the judge checked transcript evidence plus successful builds rather than
running a host-side semantic oracle.
