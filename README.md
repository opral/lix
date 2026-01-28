<p align="center">
  <img src="https://raw.githubusercontent.com/opral/lix/main/assets/logo.svg" alt="Lix" height="60">
</p>

<h3 align="center">The version control system for AI agents</h3>

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

Lix is a **universal version control system** that can diff any file format (`.xlsx`, `.pdf`, `.docx`, etc) on top of existing SQL databases.

Unlike Git's line-based diffs, Lix understands file structure through plugins. Lix sees `price: 10 → 12` or `cell B4: pending → shipped`, not "line 4 changed" or "binary files differ". 

This makes Lix the ideal version control layer for AI agents operating on non-code formats.

![Lix features: diff any file format, human-in-the-loop review, and branch isolation for agents](https://raw.githubusercontent.com/opral/lix/main/assets/value-prop.png)

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

## Excel file example

An AI agent updates an order status in `orders.xlsx`.


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

## JSON file example

Even for structured text file formats like `.json` lix is tracking semantics rather than line by line diffs.

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

[Read more about Lix architecture →](https://lix.dev/docs/architecture)

## Learn More

- **[Getting Started Guide](https://lix.dev/docs/getting-started)** - Build your first app with Lix
- **[Documentation](https://lix.dev/docs)** - Full API reference and guides
- **[Discord](https://discord.gg/gdMPPWy57R)** - Get help and join the community
- **[GitHub](https://github.com/opral/lix)** - Report issues and contribute

## Blog posts

- [Introducing Lix: A universal version control system](https://lix.dev/blog/introducing-lix)
- [What if a Git SDK to build apps exists?](https://samuelstroschein.com/blog/what-if-a-git-sdk-exists)
- [Git is unsuited for applications](https://samuelstroschein.com/blog/git-limitations)
- [Does a git-based architecture make sense?](https://samuelstroschein.com/blog/git-based-architecture)

## License

[MIT](https://github.com/opral/lix/blob/main/packages/lix-sdk/LICENSE)
