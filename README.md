<p align="center">
  <img src="https://raw.githubusercontent.com/opral/lix/main/assets/logo.svg" alt="Lix" height="60">
</p>

<h3 align="center">Embeddable version control system</h3>

<p align="center">
  <a href="https://www.npmjs.com/package/@lix-js/sdk"><img src="https://img.shields.io/npm/dw/%40lix-js%2Fsdk?logo=npm&logoColor=red&label=npm%20downloads" alt="97k weekly downloads on NPM"></a>
  <a href="https://discord.gg/gdMPPWy57R"><img src="https://img.shields.io/discord/897438559458430986?style=flat&logo=discord&labelColor=white" alt="Discord"></a>
  <a href="https://github.com/opral/lix"><img src="https://img.shields.io/github/stars/opral/lix?style=flat&logo=github&color=brightgreen" alt="GitHub Stars"></a>
  <a href="https://x.com/lixCCS"><img src="https://img.shields.io/badge/Follow-@lixCCS-black?logo=x&logoColor=white" alt="X (Twitter)"></a>
</p>

> [!NOTE]
>
> **Lix is in alpha** · [Follow progress to v1.0 →](https://github.com/opral/lix/issues/374)

---

Lix is an **embeddable version control system** that can be imported as a library. Use lix, for example, to enable human-in-the-loop workflows for AI agents like diffs and reviews.

- **It's just a library** — Lix is a library you import. Get branching, diff, rollback in your existing stack
- **Tracks semantic changes** — diffs, blame, and history are queryable via SQL
- **Approval workflows for agents** — agents propose changes in isolated versions, humans review and merge

[How does Lix compare to Git? →](https://lix.dev/docs/comparison-to-git)

## Getting started

<p>
  <img src="https://cdn.simpleicons.org/javascript/F7DF1E" alt="JavaScript" width="18" height="18" /> JavaScript ·
  <a href="https://github.com/opral/lix/issues/370"><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" alt="Python" width="18" height="18" /> Python</a> ·
  <a href="https://github.com/opral/lix/issues/371"><img src="https://cdn.simpleicons.org/rust/CE422B" alt="Rust" width="18" height="18" /> Rust</a> ·
  <a href="https://github.com/opral/lix/issues/373"><img src="https://cdn.simpleicons.org/go/00ADD8" alt="Go" width="18" height="18" /> Go</a>
</p>

```bash
npm install @lix-js/sdk
```

```ts
import { openLix, selectWorkingDiff } from "@lix-js/sdk";

const lix = await openLix({
  environment: new InMemorySQLite()
});

await lix.db.insertInto("file").values({ path: "/hello.txt", data: ... }).execute();

const diff = await selectWorkingDiff({ lix }).selectAll().execute();
```

## Semantic change (delta) tracking

Unlike Git's line-based diffs, Lix understands file structure through plugins. Lix sees `price: 10 → 12` or `cell B4: pending → shipped`, not "line 4 changed" or "binary files differ".

### JSON file example

**Before:**
```json
{"theme":"light","notifications":true,"language":"en"}
```

**After:**
```json
{"theme":"dark","notifications":true,"language":"en"}
```

**Git sees:**
```diff
-{"theme":"light","notifications":true,"language":"en"}
+{"theme":"dark","notifications":true,"language":"en"}
```

**Lix sees:**

```diff
property theme:
- light
+ dark
```

### Excel file example

The same approach works for binary formats. With an XLSX plugin, Lix shows cell-level changes:

**Before:**
```diff
  | order_id | product  | status   |
  | -------- | -------- | -------- |
  | 1001     | Widget A | shipped  |
  | 1002     | Widget B | pending |
```

**After:**
```diff
  | order_id | product  | status   |
  | -------- | -------- | -------- |
  | 1001     | Widget A | shipped  |
  | 1002     | Widget B | shipped |
```

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

## How Lix Works

Lix is **change-first**: it stores semantic changes as queryable data, not snapshots.

Plugins parse files and app state into structured entities. Lix stores **what changed** semantically — not just which bytes differ. Audit trails, rollbacks, and history become simple SQL queries:

```sql
SELECT * FROM change_history
WHERE entity_id = 'order.1002.status'
ORDER BY created_at DESC;
```

- **Doesn't reinvent databases** — uses SQLite, Postgres, etc.
- **SQL API for changes** — query diffs, history, and audit trails directly
- **Branching & merging** — isolate agent work, compare, and merge

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

## Learn More

- **[Getting Started Guide](https://lix.dev/docs/getting-started)** - Build your first app with Lix
- **[Documentation](https://lix.dev/docs)** - Full API reference and guides
- **[Discord](https://discord.gg/gdMPPWy57R)** - Get help and join the community
- **[GitHub](https://github.com/opral/lix)** - Report issues and contribute

## Blog posts

- [Introducing Lix: An embeddable version control system](https://lix.dev/blog/introducing-lix)
- [What if a Git SDK to build apps exists?](https://samuelstroschein.com/blog/what-if-a-git-sdk-exists)
- [Git is unsuited for applications](https://samuelstroschein.com/blog/git-limitations)
- [Does a git-based architecture make sense?](https://samuelstroschein.com/blog/git-based-architecture)

## License

[MIT](https://github.com/opral/lix/blob/main/packages/lix-sdk/LICENSE)
