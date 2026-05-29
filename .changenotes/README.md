# Changenotes

Changenotes are release-note fragments for user-facing changes in core Lix packages.

Add one Markdown file per change to this folder. Use a short descriptive filename, for example:

```text
.changenotes/fix-sqlite-backend-reads.md
```

Use this format:

```md
---
type: patch
scope: lix-sdk, engine
---

Improved SQLite backend read performance and native backend snapshot support.

The SQLite backend now avoids loading values for key-only point reads and uses native backend read snapshots more directly.
```

## Fields

- `type` must be exactly one of `major`, `minor`, or `patch`.
- `scope` must be one or more of `engine`, `lix-sdk`, `js-sdk`, or `cli`.
- Use comma-separated scopes for changes affecting multiple packages, for example `scope: lix-sdk, engine`.
- The body should be changelog-ready prose. Start with one clear summary sentence, then optionally add a short explainer paragraph.
- Write for the generated `CHANGELOG.md`: clear, intuitive, user-facing, and free of implementation noise unless it helps users understand the impact.

## When To Add One

Add a changenote for user-facing changes in the core packages above.

- Use `major` for breaking user-facing API or behavior changes.
- Use `minor` for backward-compatible user-facing capability additions.
- Use `patch` for user-facing fixes, compatibility fixes, and performance improvements.

Do not add a changenote for repo-only, documentation-only, CI-only, test-only, or chore-only changes.
