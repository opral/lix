# Plugin API ax-eval harness

This directory adapts the supplied ax-eval v2 rubric to Codex rollout logs. It
does not ask agents to self-report metrics: tool calls,
duration, interruptions, command discovery, and tool errors come directly from
the JSONL transcript. A separate judge rollout supplies only task success.
Forked Codex rollouts can embed compacted parent history; extraction begins at
the `task_started` event immediately preceding the subagent's `NEW_TASK`
envelope, so inherited commands and elapsed time do not pollute the score.

The vendored schemas are byte-for-byte copies of ax-eval's `result.schema.json`
and `index.schema.json`. The harness validates both result and index documents
without requiring a third-party Python package.

## Controlled evaluation procedure

1. Use one isolated starter workspace per tested agent. Only that workspace may
   be visible under `runs/` while the agent executes; archive it before starting
   another agent so repository search cannot expose a sibling implementation.
   `prepare-workspaces` copies the starter tree and refuses to overwrite any
   existing output.

   ```sh
   python3 codex_ax_eval.py prepare-workspaces \
     --template /absolute/path/to/candidate-a-starter \
     --output /private/tmp/lix-ax-candidate-a \
     --count 10
   ```
2. Spawn each agent with `fork_turns="none"` and the canonical one-line prompt:

   ```text
   {task} using {tool}
   ```

3. Do not send hints or follow-ups. Record every unavailable pinned setting in
   `config.overrides` (Codex model/tool surface, temperature control, maximum
   turns, and permission mode).
4. Find the raw rollout by exact collaboration path, then extract metrics:

   ```sh
   python3 codex_ax_eval.py find --agent-path /root/ax_a_01
   python3 codex_ax_eval.py extract /absolute/path/to/rollout.jsonl
   ```

5. Spawn one independent judge per tested agent using the exact text printed by:

   ```sh
   python3 codex_ax_eval.py judge-prompt \
     --task 'Implement and test the CSV plugin' \
     --transcript /absolute/path/to/rollout.jsonl
   ```

6. Persist a completed round from a manifest:

   ```sh
   python3 codex_ax_eval.py persist manifest.json
   ```

The manifest has this shape:

```json
{
  "tool": {
    "slug": "lix-plugin-api-a",
    "name": "Lix plugin API candidate A",
    "version": "research-1",
    "install": "included in isolated workspace"
  },
  "task": "Implement and test the assigned file-format plugin",
  "round": "candidate-a",
  "ts": "2026-07-22T12:00:00Z",
  "config": {
    "agent_model": "gpt-5.6-terra",
    "agent_count": 1,
    "temperature": 0,
    "tools": ["Codex functions.exec", "Read", "apply_patch", "rg"],
    "mcp_servers": [],
    "system_prompt": null,
    "max_turns": 40,
    "prompt_template": "{task} using {tool}",
    "docs_included": false,
    "overrides": {
      "agent_model": "claude-opus-4-7 unavailable; used gpt-5.6-terra",
      "temperature": "Codex collaboration agents do not expose temperature",
      "tools": "Codex tool surface replaces the pinned Claude Code tools",
      "system_prompt": "Codex collaboration agents use the Codex system prompt",
      "mode": "Codex sandbox policy replaces bypassPermissions",
      "max_turns": "Codex collaboration agents do not expose a hard turn cap"
    }
  },
  "agents": [
    {
      "id": 1,
      "transcript": "/absolute/path/to/tested-agent-rollout.jsonl",
      "judge": "/absolute/path/to/judge-rollout-or-result.jsonl"
    }
  ]
}
```

The example is a schema-valid one-agent smoke round. The adaptive tournament in
this research uses five-agent format-complete screens, then the ax-eval default
of ten independent agents for the selected candidate. Screening results are
not used to claim sub-ten-point score differences.

The harness creates the required `~/.ax-eval/{tool-slug}/` layout, archives raw
tested-agent transcripts, writes `result.json`, and atomically updates
`index.json`. It never overwrites an existing round.

## Compare, list, and validate

```sh
python3 codex_ax_eval.py list lix-plugin-api-a
python3 codex_ax_eval.py compare lix-plugin-api-a baseline refined
python3 codex_ax_eval.py validate ~/.ax-eval/lix-plugin-api-a/index.json
```

Quartiles use linear interpolation at `(N - 1) × q`. Ties for the common first
command are broken by the lowest agent ID, making repeated persistence fully
deterministic.

## Codex adapter boundary

One Codex `custom_tool_call` or `function_call` event counts as one tool call,
matching ax-eval's event-block definition. The `exec` wrapper can hide a nested
command's exit status when an agent emits only `result.output`; the adapter
counts a command failure whenever the rollout exposes a nonzero `exit_code`,
and counts explicit `isError`, `is_error`, failed call status, or `Script failed`
markers. This limitation applies equally to every candidate and must be listed
in the research report's evaluation overrides.

Run the harness tests with:

```sh
python3 -m unittest discover -s tests -v
```
