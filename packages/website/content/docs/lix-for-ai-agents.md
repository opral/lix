# Lix for AI Agents

![AI agent changes need to be visible and controllable](/blame-what-did-you-change.svg)

AI agents can generate more changes than a team can review in real time. Those changes still need to be **visible, attributable, and reversible**.

**Lix provides the version control layer that lets agents move fast while keeping every edit transparent and reviewable.**

## Terminology

- A **version** is an **isolated branch of state**. Use versions to run agents without touching production data.
- A **change proposal** is a review unit (diff + discussion + approval) for deciding what ships.

## Every change is reviewable

- Use [attribution](/docs/attribution) to see which agent (or human) made each edit.
- Use [diffs](/docs/diffs) to review what changed before you merge or publish it.
- Query history to answer questions like: "Which agent changed this configuration last?"

## Humans stay in control

Agents can draft changes, but humans decide what ships.

- Use [change proposals](/docs/change-proposals) to review, discuss, and approve a set of changes.
- Use [conversations](/docs/conversations) to leave comments, request revisions, and loop in stakeholders.
- Merge a proposal when it meets your requirements, or request another iteration.

## Isolated branches for AI agents

Use [versions](/docs/versions) to give each agent an isolated branch of state.

- If you run agents in parallel, create one version per agent task.
- Compare outcomes, merge the best result, or discard versions you don't want.
- If something goes wrong, use [restore](/docs/restore) to roll back to a known-good state.

## Typical workflow

1. Open a fresh version for an agent task.
2. Run the agent; Lix records changes and attribution.
3. Review the diff and open a change proposal.
4. Comment, request revisions, and approve when ready.
5. Merge the proposal or discard it; restore a previous state if needed.

## Coming soon: automated guardrails

> [!NOTE]
> [Validation rules](/docs/validation-rules) are an upcoming feature. Define automated checks agents can run before opening a proposal (for example: schema constraints, required fields, invariants). Follow the issue for progress and demos.

![Validation rules for AI agents](/validation-rules-agent.svg)

## Next steps

- Walk through the [Getting Started guide](/docs/getting-started) to wire Lix into your agent pipeline.
- Learn how to diff, merge, and experiment with [versions](/docs/versions).
- See change proposals in action in the [live example](https://prosemirror-example.onrender.com/).
