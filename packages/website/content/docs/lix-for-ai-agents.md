# Lix for AI Agents

![AI agent changes need to be visible and controllable](/blame-what-did-you-change.svg)

AI agents edit your product's **state** (files, documents, configs, database-backed content). Those edits need the same guarantees teams expect from Git—**diff, review, rollback**—as a library you can import.

**Lix is a version control library.**

Import Lix, route agent writes through it, and your UI can show **semantic diffs**, **attribution**, **proposals**, and **rollback**—all as queryable data.

## Key concepts

- **Version**: an isolated branch of your application state. Run agents safely without touching production.
- **Change proposal**: a review unit (diff + discussion + approval). Humans decide what ships.

## Review every agent edit

- **Attribution**: see which agent (or human) changed what. → [/docs/attribution](/docs/attribution)
- **Diffs**: review what changed before it's merged or published. → [/docs/diffs](/docs/diffs)
- **Queryable history**: answer "Which agent changed this setting last week?" via SQL.

## Keep humans in control

Agents draft. Humans approve.

- Use **change proposals** to bundle edits into a reviewable unit. → [/docs/change-proposals](/docs/change-proposals)
- Use **conversations** to comment, request revisions, loop in stakeholders. → [/docs/conversations](/docs/conversations)
- Merge when ready—or reject and iterate.

## Run agents in isolated versions

Use **versions** to give each agent task its own branch of state. → [/docs/versions](/docs/versions)

- Run agents in parallel: one version per task.
- Compare outcomes, merge the best, discard the rest.
- Something went wrong? **Restore** a known-good state. → [/docs/restore](/docs/restore)

## Typical workflow

1. Create a new **version** for an agent task.
2. Run the agent—Lix records semantic changes + attribution.
3. Open a **change proposal** from the version diff.
4. Review, comment, request revisions, approve.
5. Merge the proposal (or discard it). Restore if needed.

## Coming soon: automated guardrails

> [!NOTE]
> [Validation rules](/docs/validation-rules) are upcoming. Define checks agents can run before opening a proposal—schema constraints, required fields, invariants. Follow the issue for progress.

![Validation rules for AI agents](/validation-rules-agent.svg)

## Next steps

- Wire Lix into your agent pipeline: [/docs/getting-started](/docs/getting-started)
- Learn diff, merge, and experimentation with versions: [/docs/versions](/docs/versions)
- See proposals in action: [prosemirror-example.onrender.com](https://prosemirror-example.onrender.com/)
