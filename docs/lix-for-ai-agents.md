# Lix for AI Agents

AI agents can make large, fast, and useful changes. They can also make changes that need review.

Lix gives agentic applications a place to put those changes before they become the main state.

## The problem

An agent might edit:

- Files
- Documents
- Configuration
- Structured records
- Database-backed application state

Without version control inside the app, those edits are hard to inspect. The user sees the result, but not the path that produced it.

For agent workflows, that is not enough. Users need to review, compare, approve, reject, and recover.

## The Lix model

Route agent writes through Lix.

Then each agent task can have its own isolated version of state:

1. Create a version for the agent task.
2. Switch the agent into that version.
3. Let the agent make changes.
4. Preview what changed.
5. Ask a human or policy to approve the result.
6. Merge or discard the version.

The app stays in control of the workflow.

## Why versions matter for agents

Versions let agents work without immediately changing the main state.

That unlocks safer product experiences:

- Run multiple agents in parallel.
- Compare different proposed outcomes.
- Keep the main state stable while work is in progress.
- Merge only the changes that pass review.
- Discard a bad attempt without manual cleanup.

## What users should see

Lix is infrastructure. Your product still decides the UI.

A good agent review UI usually shows:

- The task the agent was asked to complete.
- The files or records that changed.
- A human-readable diff.
- Any validation or policy checks.
- Buttons to approve, request changes, or discard.

The important part is that the product can present agent work as a reviewable change, not as an invisible mutation.

## Minimal flow

```ts
const mainVersionId = await lix.activeVersionId();

const agentVersion = await lix.createVersion({
  id: "agent-task-123",
  name: "Agent task 123",
});

await lix.switchVersion({ versionId: agentVersion.id });

// Run the agent here. Any writes routed through Lix are isolated.

await lix.switchVersion({ versionId: mainVersionId });

const preview = await lix.mergeVersionPreview({
  sourceVersionId: agentVersion.id,
});

console.log(preview.changeStats);
```

Once the user approves the change:

```ts
await lix.mergeVersion({
  sourceVersionId: agentVersion.id,
});
```

## Next

- Learn the basics in [Getting Started](/docs/getting-started)
- Compare the mental model in [Comparison to Git](/docs/comparison-to-git)
