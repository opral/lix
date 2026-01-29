# Comparison to Git

> [!TIP]
>
> Lix does not replace Git. They solve different problems.

**Use Git for source code. Use Lix when you need version control inside your product.**

Git is typically used as an external developer toolchain (CLI + hosting workflows). Lix is designed to be embedded in your product. Agents can propose changes and users can review/approve in your UI. Git stores snapshots and derives diffs by comparing versions. Lix stores semantic changes (deltas) as data, so diffs, audit trails, and rollback are native and queryable.

- **Git**: "line 5 changed"
- **Lix**: "price changed from $10 to $12"

|               | Git                                      | Lix                                      |
| :------------ | :--------------------------------------- | :--------------------------------------- |
| Architecture  | Snapshot-first                           | Change-first (semantic deltas)           |
| Primary use   | Code repositories                        | Embedded in applications                 |
| Interface     | CLI + external services                  | SDK (JS, soon Rust/Python)               |
| Diffs         | Computed from snapshots                  | Semantic change records                  |
| History       | git log                                  | SQL queries                              |
| Metadata      | Review workflow usually external (PRs, comments) | Workflow data lives with the repo (queryable) |

## When to Use Git

- Source code repositories
- Developer workflows (branches, PRs, CI/CD)
- Text-based config files
- Collaboration via GitHub/GitLab

Git excels here. Don't replace it with Lix for these use cases.

## When to Use Lix

- **In-app version control**: Users review and approve changes without leaving your product
- **AI agent workflows**: Agents propose changes, humans review before merging
- **Queryable history**: Audit trails, blame, and rollback via SQL
- **Non-code formats**: Structured diffs for JSON, CSV, and other formats via plugins
- **Portable repository**: Self-contained repos (often a single SQLite file), designed to integrate with SQL backends

## Technical Differences

### 1. Change-First Architecture

In Git, diffs are derived by comparing snapshots. In Lix, semantic changes are stored as first-class records at write time, so diffs, audit trails, and rollback are native and queryable.

Lix can track:

- **JSON**: Individual properties (price changed from $10 to $12)
- **CSV**: Specific cells or rows
- **Excel**: Individual cells with row/column context (with plugin)

This enables:

- **Precise diffs**: "price field changed from $10 to $12" instead of line numbers
- **Granular queries**: SQL queries like "show all price changes in the last week"
- **Smarter conflict resolution**: Semantic merging reduces conflicts

Because changes are stored as data, you can query history directly:

```sql
SELECT
  change_id,
  snapshot_content,
  account.display_name
FROM state_history
JOIN change_author ON change_author.change_id = state_history.change_id
JOIN account ON account.id = change_author.account_id
WHERE entity_id = '/product/price'
ORDER BY lixcol_depth ASC;
```

### 2. Plugin System for Any File Format

Format support depends on plugins. Plugins teach Lix what a "change" means for each format:

- **What to track**: A cell, a row, a JSON property
- **What changed**: The semantic delta (not just bytes)
- **How to reconstruct**: Rebuild files from change history

Once a plugin exists, that format gets queryable, diffable, mergeable changes.

[Read more about plugins →](https://lix.dev/docs/plugins)

### 3. Runs on SQL Databases

Lix uses SQL databases as query engine and persistence layer.

```
┌─────────────────────────────────────────────────┐
│                      Lix                        │
│                                                 │
│ ┌────────────┐ ┌──────────┐ ┌─────────┐ ┌─────┐ │
│ │ Filesystem │ │ Branches │ │ History │ │ ... │ │
│ └────────────┘ └──────────┘ └─────────┘ └─────┘ │
└────────────────────────┬────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────┐
│                  SQL database                   │
│            (SQLite, Postgres, etc.)             │
└─────────────────────────────────────────────────┘
```

[Read more about Lix architecture →](https://lix.dev/docs/architecture)

## FAQ

### Why not Git + diff drivers?

Git diff drivers can improve display for some formats, but Git remains snapshot-first and toolchain-oriented. Lix is designed for in-product workflows: semantic deltas, embedded approvals, and queryable history as data.

