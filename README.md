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

Git works for code. Lix works for any file format: spreadsheets, documents, PDFs, and more. Track file edits, review diffs, and merge approved changes from branches.

## Why Lix

Git provides guardrails for AI coding assistants. AI agents working beyond code need the same guardrails. Lix provides them.

- **Track agent actions** - See exactly what an agent did and when.
- **Review meaningful diffs** - See what actually changed, not noisy line-by-line text.
- **Isolate tasks in branches** - Propose changes for human review and merge only what's approved.

Plugins handle any file format. History is SQL-queryable. Everything lives in a single portable SQLite file.

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

Lix is a version control system that runs on top of an existing SQL(ite) database:

- **Filesystem**: A virtual filesystem for files and directories
- **Branching**: Isolate work in branches, compare, and merge
- **History**: Full change history with commits and diffs
- **Change proposals**: Built-in pull request-like workflows

```
┌─────────────────────────────────────────────────┐
│                    Lix SDK                      │
│           (version control system)              │
│                                                 │
│ ┌────────────┐ ┌──────────┐ ┌─────────┐ ┌─────┐ │
│ │ Filesystem │ │ Branching│ │ History │ │ ... │ │
│ └────────────┘ └──────────┘ └─────────┘ └─────┘ │
│                                                 │
└────────────────────────┬────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────┐
│               SQL(ite) database                 │
└─────────────────────────────────────────────────┘
```

Everything lives in a single SQLite database file. Persist anywhere (S3, filesystem, sandbox, etc.).

[Upvote issue #372 for Postgres support →](https://github.com/opral/lix/issues/372)

## Comparison to Git

Git was built for text files. It can't meaningfully diff spreadsheets, PDFs, or binary formats. Lix can—via plugins that understand file structure.

**Example**

- **Git**: "line 5 changed"
- **Lix**: "price changed from $10 to $12"

|              | Git                       | Lix             |
| ------------ | ------------------------- | --------------- |
| Diffs        | Line-based                | Schema-aware    |
| File formats | Text                      | Any via plugins |
| Metadata     | External (GitHub, GitLab) | In the repo     |
| Interface    | CLI                       | SDK             |

[Full comparison to Git →](https://lix.dev/docs/comparison-to-git)

## Learn More

- **[Getting Started Guide](https://lix.dev/docs/getting-started)** - Build your first app with Lix
- **[Documentation](https://lix.dev/docs)** - Full API reference and guides
- **[Discord](https://discord.gg/gdMPPWy57R)** - Get help and join the community
- **[GitHub](https://github.com/opral/lix)** - Report issues and contribute

## License

[MIT](https://github.com/opral/lix/blob/main/packages/lix-sdk/LICENSE)
