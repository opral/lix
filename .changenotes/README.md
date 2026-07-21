# Changenotes

Changenotes are release-note fragments for user-facing changes in core Lix packages.

Add one Markdown file per change to this folder. Use a short descriptive filename, for example:

```text
.changenotes/fix-sqlite-storage-reads.md
```

Use this format:

```md
---
type: patch
---

Improved SQLite read performance and native storage snapshot support.

SQLite now avoids loading values for key-only point reads and uses native storage snapshots more directly.
```

## Fields

- `type` must be exactly one of `minor` or `patch`.
- The body should be changelog-ready prose. Start with one clear summary sentence, then optionally add a short explainer paragraph.
- Write for the generated `CHANGELOG.md`: clear, intuitive, user-facing, and free of implementation noise unless it helps users understand the impact.

## When To Add One

Add a changenote for user-facing changes in the core packages above.

- Use `minor` for backward-compatible user-facing capability additions.
- Use `patch` for user-facing fixes, compatibility fixes, and performance improvements.

Lix does not publish major releases from changenotes. Coordinate breaking changes so they can ship in a minor release before adding a changenote.

Do not add a changenote for repo-only, documentation-only, CI-only, test-only, or chore-only changes.
