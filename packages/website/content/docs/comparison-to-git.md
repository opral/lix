---
description: See when to use Git, when to use Lix, and how semantic change tracking differs from snapshot-based source control.
---

# Comparison to Git

Lix and Git are both version-control systems, but they are built for different places in the stack.

Use Git for source code. Use Lix when version control needs to live inside your app.

## The short version

| Question | Git | Lix |
| :-- | :-- | :-- |
| Where does it run? | Outside your app | Inside your app |
| Main interface | CLI and hosting platforms | JavaScript/TypeScript SDK |
| Best for | Source-code repositories | Product state, files, and agent workflows |
| Review flow | Pull requests | App-defined review flows |
| History | Git commits | Queryable application data |
| Diff model | Mostly text/snapshot oriented | Designed for semantic changes |

## Git is a developer tool

Git is excellent for source code:

- Branches
- Commits
- Pull requests
- CI integration
- Collaboration through GitHub, GitLab, or similar platforms

That workflow is perfect when the user is a developer and the artifact is a repository.

## Lix is an application primitive

Lix is for products that need version control as part of the user experience.

For example:

- An AI agent edits a document and a human reviews the result.
- A product lets users draft changes before publishing them.
- A workflow needs history, rollback, and auditability.
- A structured file or app state needs semantic diffs instead of plain text diffs.

In these cases, asking users to open Git is the wrong abstraction. The version-control workflow belongs in the app.

## Snapshots vs changes

Git stores snapshots and computes diffs between them.

Lix is designed around changes as data. That makes it easier for an app to ask product-level questions:

- Which fields changed?
- Which agent made this edit?
- What would happen if we merge this version?
- What changed since the user opened this review?

## They can be used together

Lix does not need to replace Git.

A product can use Git to version its source code while using Lix to version the state that users and agents edit at runtime.

That separation is the point: Git remains the developer workflow, while Lix becomes the product workflow.
