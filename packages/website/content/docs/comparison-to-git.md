# Comparison to Git

Git was built for text files. It can't meaningfully diff spreadsheets, PDFs, or binary formats. Lix can—via plugins that understand file structure.

- **Git**: "line 5 changed"
- **Lix**: "price changed from $10 to $12"

|              | Git                       | Lix             |
| :----------- | :------------------------ | :-------------- |
| Diffs        | Line-based                | Schema-aware    |
| File formats | Text                      | Any via plugins |
| Metadata     | External (GitHub, GitLab) | In the repo     |
| Interface    | CLI                       | SDK             |

## Key Differences

### 1. Schema-Aware Change Tracking

**Git** tracks changes line-by-line without understanding data structure. A JSON property change appears as "line 5 modified" with no semantic context.

**Lix** is schema-aware. It can track:

- **JSON**: Individual properties (`/product/price` changed from $10 to $12)
- **CSV**: Specific cells or rows
- **Excel**: Individual cells with row/column context

This enables:

- **Precise diffs**: "price field changed from $10 to $12" instead of line numbers
- **Granular queries**: SQL queries like "show all email changes in the last week"
- **Smarter conflict resolution**: Schema-aware merging reduces conflicts

```typescript
// Query history of a single JSON property
const priceHistory = await lix.db
  .selectFrom("state_history")
  .innerJoin(
    "change_author",
    "change_author.change_id",
    "state_history.change_id",
  )
  .innerJoin("account", "account.id", "change_author.account_id")
  .where("entity_id", "=", "/sku_124/price")
  .where("schema_key", "=", "plugin_json_pointer_value")
  .orderBy("lixcol_depth", "asc") // depth 0 = current
  .select([
    "state_history.change_id",
    "state_history.snapshot_content", // { value: 250 }
    "account.display_name",
  ])
  .execute();
```

### 2. Plugin System for Any File Format

**Git** treats binary files as opaque blobs. You can't query "what changed in cell C45?" for Excel or "which layer was modified?" for design files.

**Lix** uses plugins to understand file formats. Each plugin defines:

- What constitutes a trackable unit (a cell, a row, a JSON property)
- How to detect changes between versions
- How to reconstruct files from changes

Plugins can handle JSON, CSV, Excel, PDFs, design files, or proprietary formats.

### 3. SDK, Not CLI

**Git** is an external CLI tool. Integrating it into applications requires shelling out to commands and parsing text output.

**Lix** is an embeddable JavaScript library that runs directly in your application:

- **Runs anywhere**: Browsers, Node.js, edge functions, Web Workers
- **SQL queries**: Query change history programmatically instead of parsing CLI output
- **Portable storage**: `.lix` files (SQLite) can be stored in OPFS, S3, database columns, or in-memory

### 4. Metadata Lives in the Repo

**Git** stores only file content and commits. Everything else—pull requests, code reviews, discussions, CI rules—lives in external services like GitHub or GitLab.

**Lix** stores all metadata directly in the repo:

- **Change proposals**: The equivalent of pull requests, stored as data in the repo
- **Discussions**: Conversations and comments attached to changes
- **Automation rules**: Validation, formatting, and workflow rules travel with the repo

All data is co-located and queryable via SQL—making it directly accessible to agents and automation without requiring API integrations to external services.

### 5. SQL-Queryable History

```typescript
// Time-travel: query file history from a specific commit
const history = await lix.db
  .selectFrom("file_history")
  .where("path", "=", "/catalog.json")
  .where("lixcol_root_commit_id", "=", versionCommit)
  .orderBy("lixcol_depth", "asc")
  .execute();

// Cross-version file comparison
const diff = await lix.db
  .selectFrom("file_history as v1")
  .innerJoin("file_history as v2", "v1.id", "v2.id")
  .where("v1.lixcol_root_commit_id", "=", versionACommit)
  .where("v2.lixcol_root_commit_id", "=", versionBCommit)
  .where("v1.lixcol_depth", "=", 0)
  .where("v2.lixcol_depth", "=", 0)
  .select(["v1.path", "v1.data as versionAData", "v2.data as versionBData"])
  .execute();
```
