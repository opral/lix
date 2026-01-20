<p align="center">
  <img src="https://raw.githubusercontent.com/opral/lix/main/assets/logo.svg" alt="Lix" height="60">
</p>

<h3 align="center">The version control system for AI agents</h3>

<p align="center">
  <a href="https://www.npmjs.com/package/@lix-js/sdk"><img src="https://img.shields.io/npm/dw/%40lix-js%2Fsdk?logo=npm&logoColor=red&label=npm%20downloads" alt="NPM Downloads"></a>
  <a href="https://discord.gg/gdMPPWy57R"><img src="https://img.shields.io/discord/897438559458430986?style=flat&logo=discord&labelColor=white" alt="Discord"></a>
  <a href="https://x.com/lixCCS"><img src="https://img.shields.io/badge/Follow-@lixCCS-black?logo=x&logoColor=white" alt="X (Twitter)"></a>
</p>

> [!NOTE]
>
> **Lix is in alpha** · [Follow progress to v1.0 →](https://github.com/opral/lix/issues/374)

---

Lix is a **universal version control system** that can diff any file format (`.xlsx`, `.pdf`, `.docx`, etc).

Unlike Git's line-based diffs, Lix understands file structure through plugins. Lix sees `price: 10 → 12` or `cell B4: pending → shipped`, not "line 4 changed" or "binary files differ". 

This makes Lix the ideal version control layer for AI agents operating on non-code formats.

### Example: Excel

An AI agent updates an order status in `orders.xlsx`.

**Before:**
```diff
  | order_id | product  | status  |
  | -------- | -------- | ------- |
  | 1001     | Widget A | shipped |
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

### Example: JSON

Even for structured text file formats like `.json`, Lix tracks semantics rather than line-by-line diffs.

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

## Quick Start

<p>
  <img src="https://cdn.simpleicons.org/javascript/F7DF1E" alt="JavaScript" width="18" height="18" /> JavaScript ·
  <a href="https://github.com/opral/lix/issues/370"><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" alt="Python" width="18" height="18" /> Python</a> ·
  <a href="https://github.com/opral/lix/issues/371"><img src="https://cdn.simpleicons.org/rust/CE422B" alt="Rust" width="18" height="18" /> Rust</a> ·
  <a href="https://github.com/opral/lix/issues/373"><img src="https://cdn.simpleicons.org/go/00ADD8" alt="Go" width="18" height="18" /> Go</a>
</p>

```bash
npm install @lix-js/sdk @lix-js/plugin-json
```

```ts
import { openLix, selectWorkingDiff, InMemoryEnvironment } from "@lix-js/sdk";
import { plugin as json } from "@lix-js/plugin-json";

// 1) Open a lix with plugins
const lix = await openLix({
  environment: new InMemoryEnvironment(),
  providePlugins: [json],
});

// 2) Write a file via SQL
await lix.db
  .insertInto("file")
  .values({
    path: "/settings.json",
    data: new TextEncoder().encode(JSON.stringify({ theme: "light" })),
  })
  .execute();

// 3) Query the changes
const diff = await selectWorkingDiff({ lix }).execute();
console.log(diff);
```

[Full getting started →](https://lix.dev/docs/getting-started)

## How Lix Works

Lix is a version control system that runs on top of SQL databases:

- **Filesystem**: A virtual filesystem for files and directories
- **Branching**: Isolate work in branches, compare, and merge
- **History**: Full change history with commits and diffs
- **Change proposals**: Built-in pull request-like workflows

```
┌─────────────────────────────────────────────────┐
│                      Lix                        │
│           (version control system)              │
│                                                 │
│ ┌────────────┐ ┌──────────┐ ┌─────────┐ ┌─────┐ │
│ │ Filesystem │ │ Branches │ │ History │ │ ... │ │
│ └────────────┘ └──────────┘ └─────────┘ └─────┘ │
└────────────────────────┬────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────┐
│                  SQL database                   │
└─────────────────────────────────────────────────┘
```


> [!NOTE]
> Lix targets SQLite at the moment. [Upvote issue #372 for Postgres support →](https://github.com/opral/lix/issues/372)

[Read more about Lix architecture →](https://lix.dev/docs/architecture)

## Learn More

- **[Getting Started Guide](https://lix.dev/docs/getting-started)** - Build your first app with Lix
- **[Documentation](https://lix.dev/docs)** - Full API reference and guides
- **[Discord](https://discord.gg/gdMPPWy57R)** - Get help and join the community
- **[GitHub](https://github.com/opral/lix)** - Report issues and contribute

## License

[MIT](https://github.com/opral/lix/blob/main/packages/lix-sdk/LICENSE)
