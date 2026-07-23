# Codex AX-eval harness

`codex_ax_eval.py` runs the supplied ax-eval v2 protocol through
`codex exec --json`; it never requires the Claude CLI. Every tested agent gets
one detached Git worktree at the exact requested commit and receives exactly
this one-line prompt:

```text
{task} using {tool}
```

After all tested agents finish, an independent Codex judge reads each retained
raw transcript using the ax-eval judge prompt. A timeout can leave that log
partial; its process outcome remains explicit in the adjacent sidecar. Tested
agents and judges run in separate bounded parallel batches.

## Run

```sh
python3 codex_ax_eval.py \
  --repo /absolute/path/to/lix \
  --revision 0123456789abcdef \
  --model gpt-5.4 \
  --task 'Implement and test the CSV plugin' \
  --tool 'the Lix plugin API in this repository' \
  --round baseline
```

The cohort defaults to 10 agents with batches of two. Use `--agent-count` and
`--parallelism` to change those independently. Each Codex process has a
one-hour default bound; change it with `--timeout-seconds`.

All tested and judge subprocesses inherit one shared `CARGO_TARGET_DIR`,
defaulting to `<repo>/target/ax-eval`, so worktrees do not produce duplicate
Cargo target trees. `--cargo-target-dir` may select another Git-ignored,
untracked location under the main repository's `target/` directory.

## Safety and artifacts

- Worktrees live under a harness-owned `mkdtemp` root, are detached, and are
  removed with `git worktree remove --force` only after ownership checks.
- The main worktree is never checked out, reset, cleaned, or used as an agent
  working directory. Tested agents receive only two additional writable roots:
  the shared ignored Cargo target and an agent-private temporary directory.
- Agents use `workspace-write`; judges use `read-only`. Web search, apps, MCP,
  hooks, goals, remote plugins, and subagents are disabled for each invocation.
- Raw Codex stdout remains byte-for-byte JSONL in
  `transcripts/agent-N.jsonl`; stderr, final messages, and monotonic timing
  sidecars sit next to it. Raw judge JSONL is retained under `judges/`.
- The round directory is created before work starts. On handled interruption
  or error, partial transcripts and a `failure.json` remain in place; cleanup
  never removes the output directory. Cleanup warnings are durable in
  `invocation.json`.

Completed rounds use the supplied schemas:

```text
~/.ax-eval/<tool-slug>/
  index.json
  <timestamp>_<round>/
    result.json
    invocation.json
    transcripts/agent-1.jsonl
    judges/agent-1.jsonl
```

`result.json` contains the deterministic ax-eval formulas and the separate
judge verdicts. `index.json` is updated under a file lock by writing
`index.json.tmp` and atomically renaming it. The config block records every
Codex substitution for the pinned Claude model, tool surface, system prompt,
permission mode, maximum turns, temperature, and log format.

## Utilities and validation

The retained compatibility utilities support `extract`, `validate`, `list`,
and `compare`; see `--help` for the full list.

```sh
python3 -m py_compile codex_ax_eval.py
python3 codex_ax_eval.py --help
python3 -m unittest discover -s tests -v
```

## Production v2 authoring task

[`tasks/production-v2-tsv.md`](tasks/production-v2-tsv.md) defines a narrow
task for measuring whether an agent can discover and use the production v2
contract without being led to the CSV implementation. Pass its `task` and
`tool` values verbatim to the harness so the tested-agent prompt remains the
canonical one-line `{task} using {tool}` form.
