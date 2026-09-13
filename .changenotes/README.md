# Changenotes

Changenotes are release-note fragments for significant user-facing changes in Lix and independently released plugins.

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
- `target` is optional and defaults to `lix`. Plugin notes use one of `plugin_csv`, `plugin_excalidraw`, `plugin_json`, `plugin_markdown`, or `plugin_text`.
- Each note targets one release. For a change affecting Lix and a plugin, write one note for each target; they can have different release types.
- The body should be changelog-ready prose. Start with one clear summary sentence, then optionally add a short explainer paragraph.
- Write for the generated `CHANGELOG.md`: clear, intuitive, user-facing, and free of implementation noise unless it helps users understand the impact.

## When To Add One

Add a changenote only when users would reasonably need to know about the change
when deciding whether to upgrade or when adapting their application. Consolidate
related capabilities and fixes into one release-level theme.

- Use `minor` for backward-compatible user-facing capability additions.
- Use `patch` for user-facing fixes, compatibility fixes, and performance improvements.

Lix does not publish major releases from changenotes. Coordinate breaking changes so they can ship in a minor release before adding a changenote.

Do not add a changenote for repo-only, documentation-only, CI-only, test-only,
chore-only, implementation-detail, or narrowly scoped optimization changes.
Avoid separate notes for internal hard cuts, storage mechanics, retry paths, or
telemetry subspans unless they materially change the public contract.

## Plugin releases

For example:

```md
---
type: patch
target: plugin_json
---

Fixed JSON number serialization.
```

A plugin note opens that plugin's draft release PR. It bumps the plugin's own
Cargo version, updates `plugins/<name>/CHANGELOG.md`, and consumes only that
plugin's notes. Lix notes continue to update the root changelog and lockstep Lix
packages. Ready release candidates freeze only their own target.

Plugin releases publish compiled `.lixplugin` ZIP archives on GitHub Releases
under tags such as `plugin_json/v0.16.2`. Plugin release versions are independent
of both Lix versions and the plugin API identity embedded in the Component.
