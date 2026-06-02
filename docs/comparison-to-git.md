---
description: Git is great for source code. Lix brings Git-like workflows to non-code files like PDF, DOCX, XLSX, CSV, JSON, and agent-generated work.
---

# How Lix compares to Git

> **Git is great for source code. Lix brings Git-like branching, merging, history, and review to non-code files and AI agent workspaces.**

Use Git when developers are versioning source code. Use Lix when agents, tools, or teams need Git-like workflows for **non-code files** like PDF, DOCX, XLSX, CSV, JSON, CAD, datasets, generated reports, and other artifacts where semantic diffs and merges matter.

The difference is the unit of change: Git is strongest when a line-oriented text diff explains the work. Lix is strongest when the thing you need to review is a cell, clause, property, row, record, prompt output, or parser-defined entity.

|                   | Git                                             | Lix                                            |
| :---------------- | :---------------------------------------------- | :--------------------------------------------- |
| Primary fit       | Source-code repositories and developer workflows | Non-code repositories, structured artifacts, and AI agent workspaces |
| Integration model | External VCS, usually operated around a repo     | Library/runtime layer for agents, services, CLIs, and apps |
| Artifact model    | Files and snapshots                              | Format-aware entities across files and data    |
| Diff model        | Text-oriented by default; custom drivers possible | Semantic per-entity changes via format support |
| Merge model       | Line merge for text; binary fallback for many formats | Entity-aware merge for supported formats       |
| History surface   | Commit history and Git tooling                   | SQL-queryable change graph                     |
| Driven by         | Developers, CI, and source-code tools            | Agents, services, automation, products, and users |

Both can coexist: keep source code in Git, and use Lix for the non-code files or agent workspaces where review, merge, and rollback need to happen at the level of cells, clauses, properties, records, or generated changes.

## Source-code history vs semantic file history

Git stores snapshots and commonly presents changes as text diffs. That works extremely well for source code, where line-oriented review is natural. For many non-code files, such as spreadsheets, rich documents, CSV datasets, CAD files, PDFs, and agent-generated outputs, a line or binary diff often loses the domain meaning users care about.

Lix stores changes as data. For supported formats, parsers can map files into domain entities such as XLSX cells, DOCX clauses, CSV rows, JSON properties, PDF sections, CAD parts, or agent outputs. The plugin API is on the [roadmap](https://github.com/opral/lix#roadmap); as it matures, format experts will be able to add semantic versioning for more file types.

Agent- and tool-level questions become direct queries:

- Which cells / clauses / parts changed?
- Which prompt, tool call, user action, or service made this edit?
- What would happen if we merged this version?

Lix exposes history as queryable change data rather than only as repository history. That lets an agent ask which entities changed, who or what changed them, whether two branches touch the same entity, and what needs review before merge. See [Change History](./history.md).

## What this looks like

Git can be extended with custom diff drivers and textconv filters. The difference is that those semantic views usually sit beside Git, while Lix is designed to store and query structured changes as part of the version-control model.

### Excel

**Before:**

| order_id | product  | status  |
| -------- | -------- | ------- |
| 1001     | Widget A | shipped |
| 1002     | Widget B | pending |

**After:**

| order_id | product  | status  |
| -------- | -------- | ------- |
| 1001     | Widget A | shipped |
| 1002     | Widget B | shipped |

**Git default diff sees:**

```diff
Binary files differ
```

**With an XLSX plugin, Lix can expose:**

```diff
order_id 1002 status:
- pending
+ shipped
```

### JSON

Formatted JSON works reasonably well in Git. Semantic diffing helps when formatting, ordering, minification, or generated output obscures the actual field-level change.

**Before:**

```json
{ "theme": "light", "notifications": true, "language": "en" }
```

**After:**

```json
{ "theme": "dark", "notifications": true, "language": "en" }
```

**Git default diff on minified JSON sees:**

```diff
-{ "theme": "light", "notifications": true, "language": "en" }
+{ "theme": "dark", "notifications": true, "language": "en" }
```

**Lix sees:**

```diff
property theme:
- light
+ dark
```

If your agents need version history, review, rollback, or merge for non-code files, Lix gives them semantic primitives instead of opaque file diffs. Start with [Change History](./history.md).
