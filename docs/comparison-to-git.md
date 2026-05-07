---
description: Git versions text files line-by-line. Lix versions any file format (DOCX, XLSX, CAD, etc.) semantically per entity.
---

# Comparison to Git

> **Git versions text files line-by-line. Lix versions any file format, semantically per entity.**

Use Git for source code: text in a working tree, edited by developers, reviewed via pull requests. Use Lix when the artifacts you're versioning are anything else (DOCX, XLSX, CAD, PDF, structured app data) and the diff needs to be semantic to be useful.

|                  | Git                | Lix                                   |
| :--------------- | :----------------- | :------------------------------------ |
| Where it runs    | Separate process   | In-process, as a library              |
| What it versions | Text files         | Any file format, plus structured data |
| Diff model       | Line-by-line text  | Per-entity semantic                   |
| History          | `git log`          | `SELECT * FROM lix_change`            |
| Driven by        | Developer at a CLI | Code: app, service, agent, CLI        |

Both can coexist: Git for source code, Lix for the files and data your product, service, or tool versions at runtime.

## Snapshots vs changes

Git stores snapshots and computes text diffs between them. That works for code, where lines are the unit of change. For spreadsheets, documents, CAD, and PDFs, the line-based diff doesn't surface meaningful changes, which is exactly the kind of file where end users want version control.

Lix stores changes as data, parsed into entities by format-specific plugins (XLSX → cells, DOCX → clauses, CAD → parts). The plugin API itself is on the [roadmap](https://github.com/opral/lix#roadmap); once it lands, plugins are written by the people who know each format. Product- and tool-level questions become direct queries:

- Which cells / clauses / parts changed?
- Who or what made this edit?
- What would happen if we merged this version?

That's why Lix's history surface is a SQL table, not a `git log` parser. See [Change History](./history.md).

## What this looks like

### JSON

**Before:**

```json
{ "theme": "light", "notifications": true, "language": "en" }
```

**After:**

```json
{ "theme": "dark", "notifications": true, "language": "en" }
```

**Git sees:**

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

**Git sees:**

```diff
-Binary files differ
```

**Lix sees:**

```diff
order_id 1002 status:
- pending
+ shipped
```
